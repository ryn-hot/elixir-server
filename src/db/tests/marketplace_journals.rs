//! Actual 70->71 structural upgrades. Raw fixture records are deliberately not
//! runtime authority; migration must preserve even unadopted legacy bytes.
use super::marketplace_namespace;
use super::*;
use elixir_marketplace::identity::Digest;

async fn seed(db: &Database) -> Result<(Uuid, Uuid)> {
    match db.driver {
        DatabaseDriver::Sqlite => migrator_through(68).run(&db.pool).await?,
        DatabaseDriver::Postgres => postgres_migrator_through(68)?.run(&db.pool).await?,
    }
    let owners = marketplace_namespace::seed(db).await?;
    match db.driver {
        DatabaseDriver::Sqlite => migrator_through(70).run(&db.pool).await?,
        DatabaseDriver::Postgres => postgres_migrator_through(70)?.run(&db.pool).await?,
    }
    let json = r#"{"legacy":"preserved structurally, never adopted as authority"}"#;
    sqlx::query("INSERT INTO marketplace_installations(extension_id,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes) SELECT extension_id,home_id,9007199254740000,$1,$2,$3,$4 FROM marketplace_instance_sdk_state WHERE instance_id=$5")
        .bind(json).bind(json.len() as i64).bind(Digest::of(json.as_bytes()).to_string()).bind(json.len() as i64+131072).bind(owners.0.to_string()).execute(&db.pool).await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=9007199254740000,entries=1,encoded_bytes=$1")
        .bind(json.len() as i64+131072).execute(&db.pool).await?;
    Ok(owners)
}
type JournalSnapshot = (String, String, i64, String, i64, String, i64, String);

async fn snapshot(db: &Database) -> Result<String> {
    let journals: Vec<JournalSnapshot> = sqlx::query_as("SELECT extension_id,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes,CAST(updated_at AS TEXT) FROM marketplace_installations ORDER BY extension_id,record_json")
        .fetch_all(&db.pool).await?;
    let budget: (i64,i64,i64) = sqlx::query_as("SELECT revision,entries,encoded_bytes FROM marketplace_installation_budget WHERE singleton=1")
        .fetch_one(&db.pool).await?;
    Ok(serde_json::to_string(&(
        marketplace_namespace::snapshot(db).await?,
        journals,
        budget,
    ))?)
}
async fn upgrade(db: &Database) -> Result<()> {
    let (first, second) = seed(db).await?;
    let before = snapshot(db).await?;
    db.run_migrations().await?;
    assert_eq!(
        snapshot(db).await?,
        before,
        "preserve exact journal hash/bytes/53bit counters/timestamps and operational references"
    );
    let owner: Option<String> =
        sqlx::query_scalar("SELECT instance_id FROM marketplace_installations")
            .fetch_one(&db.pool)
            .await?;
    assert!(
        owner.is_none(),
        "migration never infers a configured owner from a capsule"
    );
    let markers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installation_metadata")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(markers, 0, "no metadata adoption");
    db.run_migrations().await?;
    assert_eq!(snapshot(db).await?, before);
    for owner in [first, second] {
        sqlx::query("INSERT INTO marketplace_installations(extension_id,instance_id,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes) SELECT extension_id,$1,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes FROM marketplace_installations WHERE instance_id IS NULL")
            .bind(owner.to_string()).execute(&db.pool).await?;
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installations")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(
        count, 3,
        "multiple configured IDs and one untouched legacy record can be represented; application adoption stays separate"
    );
    for owner in [None, Some(first), Some(second), Some(Uuid::new_v4())] {
        let mut tx = db.pool.begin().await?;
        assert!(sqlx::query("INSERT INTO marketplace_installations(extension_id,instance_id,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes) SELECT extension_id,$1,home_id,revision,record_json,record_bytes,record_sha256,encoded_bytes FROM marketplace_installations WHERE instance_id IS NULL")
            .bind(owner.map(|id|id.to_string())).execute(&mut *tx).await.is_err(),"unique identity or stable-owner FK must reject this row");
        tx.rollback().await?;
    }
    let mut tx = db.pool.begin().await?;
    assert!(
        sqlx::query("DELETE FROM extension_instances WHERE instance_id=$1")
            .bind(first.to_string())
            .execute(&mut *tx)
            .await
            .is_err(),
        "journal must protect, not cascade-delete, the stable operational owner"
    );
    tx.rollback().await?;
    // These are raw schema fixtures, not native/runtime cleanup operations.
    sqlx::query("DELETE FROM marketplace_installations WHERE instance_id IS NOT NULL")
        .execute(&db.pool)
        .await?;
    assert_eq!(snapshot(db).await?, before);
    Ok(())
}
async fn sqlite() -> Result<Database> {
    Database::connect(&DatabaseConfig {
        url: "sqlite::memory:".into(),
        max_connections: 1,
        ..Default::default()
    })
    .await
}

#[tokio::test]
async fn sqlite_instance_journal_upgrade_preserves_legacy_bytes_and_protects_stable_owners()
-> Result<()> {
    let db = sqlite().await?;
    upgrade(&db).await?;
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&db.pool)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_late_instance_journal_upgrade_failure_preserves_original_primary_key_and_can_retry()
-> Result<()> {
    let db = sqlite().await?;
    seed(&db).await?;
    let before = snapshot(&db).await?;
    sqlx::query("CREATE TABLE marketplace_installation_metadata(sentinel TEXT)")
        .execute(&db.pool)
        .await?;
    assert!(db.run_migrations().await.is_err());
    assert_eq!(snapshot(&db).await?, before);
    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version=71")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(applied, 0);
    let columns = sqlx::query("PRAGMA table_info(marketplace_installations)")
        .fetch_all(&db.pool)
        .await?;
    assert!(
        !columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "instance_id")
    );
    assert!(
        columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "extension_id"
                && row.get::<i64, _>("pk") == 1)
    );
    let partial: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='marketplace_installations_per_instance'").fetch_one(&db.pool).await?;
    assert_eq!(partial, 0);
    sqlx::query("DROP TABLE marketplace_installation_metadata")
        .execute(&db.pool)
        .await?;
    db.run_migrations().await?;
    assert_eq!(snapshot(&db).await?, before);
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh empty loopback PostgreSQL journal upgrade; preserve legacy records, no adoption/reset"]
async fn postgres_instance_journal_upgrade_preserves_legacy_bytes_and_protects_stable_owners()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_JOURNALS_POSTGRES_URL")?;
    let parsed = reqwest::Url::parse(&url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && matches!(
                parsed.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
            )
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path().starts_with("/elixir_marketplace_journals_"),
        "dedicated empty loopback fixture required"
    );
    let db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 4,
        ..Default::default()
    })
    .await?;
    anyhow::ensure!(db.driver == DatabaseDriver::Postgres, "PostgreSQL required");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')").fetch_one(&db.pool).await?;
    anyhow::ensure!(count == 0, "fixture must be empty; no reset");
    upgrade(&db).await
}
