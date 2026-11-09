//! Upgrade the actual pre-69 schema, including encrypted rows and operational
//! references. No namespace owner is inferred as runtime/activation authority.
use super::*;

pub(super) async fn seed(db: &Database) -> Result<(Uuid, Uuid)> {
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO users(id,email,password_hash) VALUES($1,$2,'fixture')")
        .bind(user.to_string())
        .bind(format!("{user}@namespace.example.test"))
        .execute(&db.pool)
        .await?;
    let home = crate::auth::home_profiles::HomeProfileRepository::new(&db.pool)
        .ensure_owner_home(user)
        .await?
        .home
        .id;
    sqlx::query("INSERT INTO extensions(extension_id,name,version,kind,manifest_json,enabled) VALUES('namespace-upgrade.fixture','preserved','1.0.0','module','{}',FALSE)")
        .execute(&db.pool).await?;
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    let manager = crate::secrets::SecretsManager::from_key_bytes([107; 32], false);
    for (id, purpose) in ids.iter().zip(["installed", "foreground"]) {
        sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,config_json,enabled) VALUES($1,'namespace-upgrade.fixture',$2,$3,FALSE)")
            .bind(id.to_string()).bind(format!("configured-{id}")).bind(r#"{"preserved":true}"#).execute(&db.pool).await?;
        let cipher = manager.encrypt(r#"{"private":"unchanged encrypted bytes"}"#)?;
        sqlx::query("INSERT INTO marketplace_instance_sdk_state(instance_id,extension_id,home_id,revision,key_count,encoded_bytes,purpose) VALUES($1,'namespace-upgrade.fixture',$2,9007199254740990,1,$3,$4)")
            .bind(id.to_string()).bind(home.to_string()).bind(cipher.len() as i64).bind(purpose).execute(&db.pool).await?;
        sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,'cursor',7,$2,$3)")
            .bind(id.to_string()).bind(&cipher).bind(cipher.len() as i64).execute(&db.pool).await?;
        sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'api_key',$3,FALSE)")
            .bind(Uuid::new_v4().to_string()).bind(id.to_string()).bind(&cipher).execute(&db.pool).await?;
    }
    let provider = Uuid::new_v4();
    let media = Uuid::new_v4();
    sqlx::query("INSERT INTO providers(provider_id,instance_id,capability,cardinality) VALUES($1,$2,'media.fixture','many')")
        .bind(provider.to_string()).bind(ids[0].to_string()).execute(&db.pool).await?;
    sqlx::query("INSERT INTO media_items(id,type,title) VALUES($1,'movie','preserved')")
        .bind(media.to_string())
        .execute(&db.pool)
        .await?;
    sqlx::query("INSERT INTO media_ownerships(ownership_id,media_item_id,owner_type,owner_instance_id,owner_provider_id) VALUES($1,$2,'extension',$3,$4)")
        .bind(Uuid::new_v4().to_string()).bind(media.to_string()).bind(ids[0].to_string()).bind(provider.to_string()).execute(&db.pool).await?;
    Ok((ids[0], ids[1]))
}

type RootSnapshot = (String, String, String, i64, i64, i64, String, String);

pub(super) async fn snapshot(db: &Database) -> Result<String> {
    let roots:Vec<RootSnapshot>=sqlx::query_as("SELECT instance_id,extension_id,home_id,revision,key_count,encoded_bytes,CAST(updated_at AS TEXT),purpose FROM marketplace_instance_sdk_state ORDER BY instance_id")
        .fetch_all(&db.pool).await?;
    let values:Vec<(String,String,i64,String,i64)>=sqlx::query_as("SELECT instance_id,key,revision,value_encrypted,encoded_bytes FROM marketplace_instance_sdk_values ORDER BY instance_id,key")
        .fetch_all(&db.pool).await?;
    let instances:Vec<(String,String,String,String,i64)>=sqlx::query_as("SELECT instance_id,extension_id,instance_name,config_json,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances ORDER BY instance_id")
        .fetch_all(&db.pool).await?;
    let providers: Vec<(String, String)> =
        sqlx::query_as("SELECT provider_id,instance_id FROM providers ORDER BY provider_id")
            .fetch_all(&db.pool)
            .await?;
    let ownerships:Vec<(String,String,String,String)>=sqlx::query_as("SELECT ownership_id,media_item_id,owner_instance_id,owner_provider_id FROM media_ownerships ORDER BY ownership_id").fetch_all(&db.pool).await?;
    let secrets: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT secret_id,scope,scope_id,key,value_encrypted FROM secrets ORDER BY secret_id",
    )
    .fetch_all(&db.pool)
    .await?;
    Ok(serde_json::to_string(&(
        roots, values, instances, providers, ownerships, secrets,
    ))?)
}

async fn upgrade(db: &Database) -> Result<()> {
    let (first, second) = seed(db).await?;
    let before = snapshot(db).await?;
    db.run_migrations().await?;
    assert_eq!(
        snapshot(db).await?,
        before,
        "upgrade must preserve exact ciphertext/counters and operational references"
    );
    let mappings:Vec<(String,String,String)>=sqlx::query_as("SELECT namespace_id,instance_id,purpose FROM marketplace_sdk_namespaces ORDER BY namespace_id")
        .fetch_all(&db.pool).await?;
    let mut expected = vec![
        (
            first.to_string(),
            first.to_string(),
            "installed".to_string(),
        ),
        (
            second.to_string(),
            second.to_string(),
            "foreground".to_string(),
        ),
    ];
    expected.sort();
    assert_eq!(mappings, expected);
    db.run_migrations().await?;
    assert_eq!(
        snapshot(db).await?,
        before,
        "migration rerun must not rewrite data"
    );
    let journals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installations")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(
        journals, 0,
        "migration does not adopt or activate existing roots"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_upgrade_preserves_exact_ciphertexts_settings_and_operational_ownership()
-> Result<()> {
    let db = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:".into(),
        max_connections: 1,
        ..DatabaseConfig::default()
    })
    .await?;
    migrator_through(68).run(&db.pool).await?;
    upgrade(&db).await?;
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&db.pool)
        .await?;
    assert!(violations.is_empty());
    Ok(())
}

#[tokio::test]
async fn sqlite_late_upgrade_failure_rolls_back_all_private_table_changes_and_can_retry()
-> Result<()> {
    let db = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:".into(),
        max_connections: 1,
        ..DatabaseConfig::default()
    })
    .await?;
    migrator_through(68).run(&db.pool).await?;
    seed(&db).await?;
    let before = snapshot(&db).await?;
    sqlx::query("CREATE TABLE marketplace_instance_sdk_values_v2(sentinel TEXT)")
        .execute(&db.pool)
        .await?;
    assert!(db.run_migrations().await.is_err());
    assert_eq!(snapshot(&db).await?, before);
    let partial:i64=sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('marketplace_sdk_namespaces','marketplace_instance_sdk_state_v2')")
        .fetch_one(&db.pool).await?;
    assert_eq!(partial, 0);
    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version=69")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(applied, 0);
    sqlx::query("DROP TABLE marketplace_instance_sdk_values_v2")
        .execute(&db.pool)
        .await?;
    db.run_migrations().await?;
    assert_eq!(snapshot(&db).await?, before);
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh empty loopback PostgreSQL namespace-upgrade database; no reset"]
async fn postgres_upgrade_preserves_exact_ciphertexts_settings_and_operational_ownership()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_NAMESPACE_POSTGRES_URL")?;
    let parsed = reqwest::Url::parse(&url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && matches!(
                parsed.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
            )
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path().starts_with("/elixir_marketplace_namespace_"),
        "fresh dedicated loopback namespace fixture required"
    );
    let db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 4,
        ..DatabaseConfig::default()
    })
    .await?;
    anyhow::ensure!(db.driver == DatabaseDriver::Postgres, "PostgreSQL required");
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')").fetch_one(&db.pool).await?;
    anyhow::ensure!(count == 0, "fixture database must be empty; no reset");
    postgres_migrator_through(68)?.run(&db.pool).await?;
    upgrade(&db).await
}

/// Schema/FK checks only. These raw rows provide no SDK or runtime authority.
async fn generation_secret_upgrade(db: &Database) -> Result<()> {
    match db.driver {
        DatabaseDriver::Sqlite => migrator_through(68).run(&db.pool).await?,
        DatabaseDriver::Postgres => postgres_migrator_through(68)?.run(&db.pool).await?,
    }
    let (owner, _) = seed(db).await?;
    match db.driver {
        DatabaseDriver::Sqlite => migrator_through(69).run(&db.pool).await?,
        DatabaseDriver::Postgres => postgres_migrator_through(69)?.run(&db.pool).await?,
    }
    let before = snapshot(db).await?;
    let mappings: Vec<(String,String,String,String,String)> = sqlx::query_as("SELECT namespace_id,instance_id,extension_id,home_id,purpose FROM marketplace_sdk_namespaces ORDER BY namespace_id")
        .fetch_all(&db.pool).await?;
    db.run_migrations().await?;
    assert_eq!(snapshot(db).await?, before);
    let after: Vec<(String,String,String,String,String)> = sqlx::query_as("SELECT namespace_id,instance_id,extension_id,home_id,purpose FROM marketplace_sdk_namespaces ORDER BY namespace_id")
        .fetch_all(&db.pool).await?;
    assert_eq!(after, mappings, "no automatic namespace adoption");
    let empty: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(empty, 0, "no legacy-secret backfill");
    db.run_migrations().await?;
    assert_eq!(snapshot(db).await?, before);

    let namespace = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO marketplace_sdk_namespaces(namespace_id,instance_id,extension_id,home_id,purpose) SELECT $1,instance_id,extension_id,home_id,purpose FROM marketplace_sdk_namespaces WHERE namespace_id=$2")
        .bind(&namespace).bind(owner.to_string()).execute(&db.pool).await?;
    let cipher = crate::secrets::SecretsManager::from_key_bytes([109; 32], false)
        .encrypt_bound("schema fixture only", b"not SDK or runtime authority")?;
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,'api_key',$2,$3,$4,FALSE)")
        .bind(&namespace).bind(&id).bind(&cipher).bind(cipher.len() as i64).execute(&db.pool).await?;
    let fresh_id = Uuid::new_v4().to_string();
    for (ns, key, secret_id, value, bytes) in [
        (
            Uuid::new_v4().to_string(),
            "other".to_string(),
            fresh_id.clone(),
            cipher.clone(),
            cipher.len() as i64,
        ),
        (
            namespace.clone(),
            "".to_string(),
            fresh_id.clone(),
            cipher.clone(),
            cipher.len() as i64,
        ),
        (
            namespace.clone(),
            "k".repeat(65),
            fresh_id.clone(),
            cipher.clone(),
            cipher.len() as i64,
        ),
        (
            namespace.clone(),
            "other".to_string(),
            "short".to_string(),
            cipher.clone(),
            cipher.len() as i64,
        ),
        (
            namespace.clone(),
            "other".to_string(),
            fresh_id.clone(),
            "".to_string(),
            0,
        ),
        (
            namespace.clone(),
            "other".to_string(),
            fresh_id.clone(),
            "x".repeat(98305),
            98305,
        ),
        (
            namespace.clone(),
            "other".to_string(),
            fresh_id.clone(),
            cipher.clone(),
            cipher.len() as i64 + 1,
        ),
        (
            namespace.clone(),
            "api_key".to_string(),
            fresh_id,
            cipher.clone(),
            cipher.len() as i64,
        ),
        (
            namespace.clone(),
            "other".to_string(),
            id,
            cipher.clone(),
            cipher.len() as i64,
        ),
    ] {
        let mut tx = db.pool.begin().await?;
        assert!(sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,$3,$4,$5,FALSE)")
            .bind(ns).bind(key).bind(secret_id).bind(value).bind(bytes).execute(&mut *tx).await.is_err());
        tx.rollback().await?;
    }
    // Namespace cleanup cascades only private data; never operational owners
    // or their legacy instance/provider/global secret address space.
    sqlx::query("DELETE FROM marketplace_sdk_namespaces WHERE namespace_id=$1")
        .bind(namespace)
        .execute(&db.pool)
        .await?;
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_generation_secrets")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(remaining, 0);
    assert_eq!(snapshot(db).await?, before);
    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version=70")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(applied, 1);
    Ok(())
}

#[tokio::test]
async fn sqlite_generation_secret_upgrade_is_additive_private_bounded_and_idempotent() -> Result<()>
{
    let db = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:".into(),
        max_connections: 1,
        ..DatabaseConfig::default()
    })
    .await?;
    generation_secret_upgrade(&db).await?;
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&db.pool)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh empty loopback PostgreSQL generation-secret upgrade database; no reset"]
async fn postgres_generation_secret_upgrade_is_additive_private_bounded_and_idempotent()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_SECRETS_POSTGRES_URL")?;
    let parsed = reqwest::Url::parse(&url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && matches!(
                parsed.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
            )
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path().starts_with("/elixir_marketplace_secrets_"),
        "fresh dedicated loopback secret fixture required"
    );
    let db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 4,
        ..DatabaseConfig::default()
    })
    .await?;
    anyhow::ensure!(db.driver == DatabaseDriver::Postgres, "PostgreSQL required");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')")
        .fetch_one(&db.pool).await?;
    anyhow::ensure!(count == 0, "fixture database must be empty; no reset");
    generation_secret_upgrade(&db).await
}
