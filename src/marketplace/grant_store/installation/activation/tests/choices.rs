//! Actual SQL/store writes and signed source material, synthetic discovery.
//! Counter snapshots are DATA, never native readiness/disposal authority.
use super::*;
use crate::{
    extensions::store::ExtensionStore,
    marketplace::grant_store::installation::{InstallTarget, choices as revisions},
};
use std::time::Duration;

async fn stage_with(f: &Fixture, mutation: Uuid) -> Result<StagedInstallation> {
    f.stage_configured(
        InstallTarget::New {
            name: format!("Choice {mutation}"),
        },
        mutation,
        0,
        limit(),
    )
    .await
}
async fn stage(f: &Fixture) -> Result<StagedInstallation> {
    stage_with(f, Uuid::new_v4()).await
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
async fn capture(f: &Fixture, owner: Uuid) -> Result<revisions::Snapshot> {
    let mut tx = f.db.pool.begin().await?;
    let snapshot = revisions::capture(
        &mut tx,
        f.db.driver,
        f.actor.home,
        &f.descriptor.release.payload.extension_id,
        owner,
    )
    .await?;
    tx.rollback().await?;
    Ok(snapshot)
}
async fn prepare(f: &Fixture, staged: &StagedInstallation) -> Result<PreparedOrRecovery> {
    let plan = plan(f)?;
    prepare_checked(step(f, staged, Uuid::new_v4(), &plan)?).await
}

#[tokio::test]
async fn candidate_sdk_effect_cannot_write_after_a_real_same_value_disable() -> Result<()> {
    use crate::marketplace::grant_store::{
        installation::runtime::{InstalledController, tests::configuration},
        sdk,
    };
    use elixir_marketplace::source_execution::RuntimeJson;
    let f = Fixture::new().await?;
    let manager = crate::secrets::SecretsManager::from_key_bytes([57; 32], false);
    let config = configuration(&f, IMAGE, "choice-effects")?;
    let mut controller =
        InstalledController::open(f.db.clone(), manager.clone(), config.clone()).await?;
    let staged = stage(&f).await?;
    let owner = staged.configured_instance().context("owner")?;
    // The source probe/native-effect boundary is synthetic, as in the existing
    // candidate SDK tests. This does not claim native readiness or retirement.
    let claim = super::configured_runtime::claim_with_credentials(
        &f,
        &mut controller,
        &config,
        &staged,
        None,
    )
    .await?;
    let request = RuntimeJson::from_value(
        serde_json::json!({"operation":"state_set","key":"cursor","value":17}),
    )?;
    let scope = sdk::CandidateScope {
        database: &f.db,
        secrets: &manager,
        binding: claim.binding(),
    };
    scope.fixture_execute(&request, 65536, || Ok(())).await?;
    let before:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
        .bind(staged.instance().to_string()).fetch_one(&f.db.pool).await?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(owner, false)
        .await?;
    let changed = RuntimeJson::from_value(
        serde_json::json!({"operation":"state_set","key":"cursor","value":99}),
    )?;
    let error = scope
        .fixture_execute(&changed, 65536, || Ok(()))
        .await
        .err()
        .context("superseded candidate wrote state")?;
    assert!(
        format!("{error:#}").contains("choices changed"),
        "{error:#}"
    );
    let after:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
        .bind(staged.instance().to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(after, before);
    Ok(())
}

#[tokio::test]
async fn same_value_real_instance_disable_supersedes_staging_without_blocking_cleanup() -> Result<()>
{
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let owner = staged.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(owner, false)
        .await?;
    assert!(prepare(&f, &staged).await.is_err());
    assert_eq!(record(&f, owner).await?, before);
    let p = f.actor.fixture_principal();
    cancellation::cancel(
        &f.db,
        cancellation::OwnerCancelIntent::new(&p, &staged, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE instance_id=$1")
            .bind(owner.to_string())
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn same_value_real_extension_disable_supersedes_all_older_instance_snapshots() -> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let b = stage(&f).await?;
    ExtensionStore::new(&f.db.pool)
        .set_extension_enabled(a.extension().as_str(), false)
        .await?;
    assert!(prepare(&f, &a).await.is_err());
    assert!(prepare(&f, &b).await.is_err());
    Ok(())
}

#[tokio::test]
async fn config_and_name_aba_changes_are_not_hidden_by_equal_final_settings() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let owner = staged.configured_instance().context("owner")?;
    let store = ExtensionStore::new(&f.db.pool);
    store
        .update_instance_config(owner, Some(&serde_json::json!({"language":"changed"})))
        .await?;
    store.update_instance_config(owner, None).await?;
    let name: String =
        sqlx::query_scalar("SELECT instance_name FROM extension_instances WHERE instance_id=$1")
            .bind(owner.to_string())
            .fetch_one(&f.db.pool)
            .await?;
    store.rename_instance(owner, "Temporary rename").await?;
    store.rename_instance(owner, &name).await?;
    assert!(prepare(&f, &staged).await.is_err());
    Ok(())
}

#[tokio::test]
async fn another_instance_control_write_does_not_supersede_this_instance() -> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let b = stage(&f).await?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(b.configured_instance().context("B")?, false)
        .await?;
    assert!(matches!(
        prepare(&f, &a).await?,
        PreparedOrRecovery::Prepared(_)
    ));
    assert!(prepare(&f, &b).await.is_err());
    Ok(())
}

#[tokio::test]
async fn exact_staging_retry_keeps_its_original_choices_after_later_disable() -> Result<()> {
    let f = Fixture::new().await?;
    let mutation = Uuid::new_v4();
    let a = stage_with(&f, mutation).await?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(a.configured_instance().context("owner")?, false)
        .await?;
    let retry = stage_with(&f, mutation).await?;
    assert!(retry.choices == a.choices);
    assert!(prepare(&f, &retry).await.is_err());
    Ok(())
}

#[tokio::test]
async fn choice_saturation_never_blocks_turning_off_or_rearms_pending_work() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let owner = staged.configured_instance().context("owner")?;
    sqlx::query("UPDATE marketplace_instance_choices SET revision=$1 WHERE instance_id=$2")
        .bind(MAX_REVISION - 1)
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    capture(&f, owner).await?;
    let store = ExtensionStore::new(&f.db.pool);
    store.set_instance_enabled(owner, false).await?;
    store.set_instance_enabled(owner, true).await?;
    store.set_instance_enabled(owner, false).await?;
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM marketplace_instance_choices WHERE instance_id=$1",
    )
    .bind(owner.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(revision, MAX_REVISION);
    assert!(capture(&f, owner).await.is_err());
    assert!(prepare(&f, &staged).await.is_err());
    let enabled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(enabled, 0);
    Ok(())
}

#[tokio::test]
async fn missing_choice_counter_is_fenced_not_reset_by_a_real_control_write() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let owner = staged.configured_instance().context("owner")?;
    sqlx::query("DELETE FROM marketplace_instance_choices WHERE instance_id=$1")
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    assert!(capture(&f, owner).await.is_err());
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(owner, false)
        .await?;
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM marketplace_instance_choices WHERE instance_id=$1",
    )
    .bind(owner.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(revision, MAX_REVISION);
    assert!(prepare(&f, &staged).await.is_err());
    Ok(())
}

#[tokio::test]
async fn ordinary_legacy_controls_do_not_allocate_marketplace_choice_rows() -> Result<()> {
    let f = Fixture::new().await?;
    let _managed = stage(&f).await?;
    sqlx::query("INSERT INTO extensions(extension_id,name,version,kind,trust_level,manifest_json,enabled) VALUES('legacy.choice-test','Legacy','1.0.0','module','community','{}',TRUE)")
        .execute(&f.db.pool).await?;
    let instance = Uuid::new_v4();
    sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,'legacy.choice-test','Legacy',TRUE)")
        .bind(instance.to_string()).execute(&f.db.pool).await?;
    let store = ExtensionStore::new(&f.db.pool);
    store
        .set_extension_enabled("legacy.choice-test", false)
        .await?;
    store.set_instance_enabled(instance, false).await?;
    store.rename_instance(instance, "Renamed legacy").await?;
    store
        .update_instance_config(instance, Some(&serde_json::json!({"preserve":true})))
        .await?;
    let counts:(i64,i64)=sqlx::query_as("SELECT CAST((SELECT COUNT(*) FROM marketplace_extension_choices) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_instance_choices) AS BIGINT)")
        .fetch_one(&f.db.pool).await?;
    assert_eq!(counts, (1, 1));
    store.delete_instance(instance).await?;
    store.delete_extension("legacy.choice-test").await?;
    Ok(())
}

#[tokio::test]
async fn old_configured_journal_bytes_are_preserved_but_cannot_adopt_new_choice_authority()
-> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let owner = staged.configured_instance().context("owner")?;
    let mut old: serde_json::Value = serde_json::from_str(&record(&f, owner).await?)?;
    old.as_object_mut()
        .context("journal object")?
        .remove("choices");
    let bytes = canonical::to_vec(&old)?;
    let journal: Journal = canonical::decode(&bytes, record::MAX_RECORD_BYTES)?;
    journal.validate()?;
    assert_eq!(canonical::to_vec(&journal)?, bytes);
    let stored = record::Stored {
        journal,
        floor: 1,
        encoded: 131072 + bytes.len() as i64,
    };
    let old_stage = stored.receipt()?;
    assert!(intent(&f, &old_stage, Uuid::new_v4()).is_err());
    Ok(())
}

#[tokio::test]
async fn choice_migration_is_additive_backfills_only_managed_rows_and_preserves_old_bytes()
-> Result<()> {
    let root = tempfile::tempdir()?;
    let db = Database::connect(&crate::config::DatabaseConfig {
        url: format!(
            "sqlite://{}",
            root.path().join("choice-upgrade.sqlite").display()
        ),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    // The relevant pre-72 schema only: opaque old bytes/ciphertext are never
    // interpreted or promoted by this additive migration.
    sqlx::query("CREATE TABLE extensions(extension_id TEXT PRIMARY KEY,enabled BOOLEAN); CREATE TABLE extension_instances(instance_id TEXT PRIMARY KEY,extension_id TEXT REFERENCES extensions(extension_id),instance_name TEXT,config_json TEXT,enabled BOOLEAN); CREATE TABLE marketplace_installation_metadata(extension_id TEXT PRIMARY KEY REFERENCES extensions(extension_id),home_id TEXT,metadata_sha256 TEXT); CREATE TABLE old_private_bytes(record_json TEXT,ciphertext TEXT,updated_at TEXT); INSERT INTO extensions VALUES('managed',FALSE),('legacy',TRUE); INSERT INTO extension_instances VALUES('managed-instance','managed','Original',NULL,FALSE),('legacy-instance','legacy','Legacy',NULL,TRUE); INSERT INTO marketplace_installation_metadata VALUES('managed','home','unchanged'); INSERT INTO old_private_bytes VALUES(' {\"old\":true} ','opaque-encrypted-bytes','2000-01-01');")
        .execute(&db.pool).await?;
    let before: (String, String, String) =
        sqlx::query_as("SELECT record_json,ciphertext,updated_at FROM old_private_bytes")
            .fetch_one(&db.pool)
            .await?;
    sqlx::query(include_str!(
        "../../../../../../migrations/0072_marketplace_choice_revisions.sql"
    ))
    .execute(&db.pool)
    .await?;
    let after: (String, String, String) =
        sqlx::query_as("SELECT record_json,ciphertext,updated_at FROM old_private_bytes")
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(before, after);
    let ext: Vec<(String, i64)> =
        sqlx::query_as("SELECT extension_id,revision FROM marketplace_extension_choices")
            .fetch_all(&db.pool)
            .await?;
    assert_eq!(ext, vec![("managed".to_string(), 1)]);
    let instance: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT instance_id,extension_id,revision FROM marketplace_instance_choices",
    )
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        instance,
        vec![("managed-instance".to_string(), "managed".to_string(), 1)]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL actual control/activation lock ordering and counter bounds; no native proof"]
async fn postgres_choice_fences_serialize_with_real_operational_writes_without_lost_disable()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_CHOICES_POSTGRES_URL")?;
    let db = super::fresh_postgres(&url).await?;
    let f = Fixture::with_database(tempfile::tempdir()?, db).await?;
    let a = stage(&f).await?;
    let owner = a.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    // Actual control UPDATE owns its parent and trigger counter. The original
    // preparation future is polled, retained through the wait, and joined after
    // that real transaction commits. No inference from a free lock/status.
    let mut control = f.db.pool.begin().await?;
    sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1")
        .bind(owner.to_string())
        .execute(&mut *control)
        .await?;
    let preparing = prepare(&f, &a);
    tokio::pin!(preparing);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), preparing.as_mut())
            .await
            .is_err()
    );
    control.commit().await?;
    let error = tokio::time::timeout(Duration::from_secs(10), preparing.as_mut())
        .await?
        .err()
        .context("later disable was lost")?;
    assert!(
        format!("{error:#}").contains("choices changed"),
        "{error:#}"
    );
    assert_eq!(record(&f, owner).await?, before);
    // Reverse order: a snapshot transaction locks parents before counters.
    // A real ExtensionStore control must wait, then succeed after its release.
    let b = stage(&f).await?;
    let other = b.configured_instance().context("B")?;
    let mut transaction = f.db.pool.begin().await?;
    revisions::capture(
        &mut transaction,
        f.db.driver,
        f.actor.home,
        b.extension(),
        other,
    )
    .await?;
    let store = ExtensionStore::new(&f.db.pool);
    let disabling = store.set_extension_enabled(b.extension().as_str(), false);
    tokio::pin!(disabling);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), disabling.as_mut())
            .await
            .is_err()
    );
    transaction.commit().await?;
    tokio::time::timeout(Duration::from_secs(10), disabling.as_mut()).await??;
    assert!(prepare(&f, &b).await.is_err());
    sqlx::query("UPDATE marketplace_instance_choices SET revision=$1 WHERE instance_id=$2")
        .bind(MAX_REVISION - 1)
        .bind(other.to_string())
        .execute(&f.db.pool)
        .await?;
    capture(&f, other).await?;
    store.set_instance_enabled(other, true).await?;
    store.set_instance_enabled(other, false).await?;
    let revision: String = sqlx::query_scalar(
        "SELECT CAST(revision AS TEXT) FROM marketplace_instance_choices WHERE instance_id=$1",
    )
    .bind(other.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(revision, MAX_REVISION.to_string());
    assert!(capture(&f, other).await.is_err());
    let p = f.actor.fixture_principal();
    cancellation::cancel(
        &f.db,
        cancellation::OwnerCancelIntent::new(&p, &b, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    f.db.pool.close().await;
    Ok(())
}
