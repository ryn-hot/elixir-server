//! Real migrated databases and actual child processes, not process-local locks.
//! PostgreSQL requires an explicitly supplied EMPTY disposable database.
use super::*;
use crate::{
    config::DatabaseConfig,
    db::{Database, DatabaseDriver},
    secrets::SecretsManager,
};
use std::{path::Path, sync::Arc};
use tokio::{
    task::JoinSet,
    time::{sleep, timeout},
};

fn input(key: &str, value: &str) -> Result<NewSecret> {
    Ok(NewSecret {
        secret_id: Uuid::new_v4(),
        scope: SecretScope::Global,
        scope_id: None,
        key: key.into(),
        value_encrypted: SecretsManager::from_key_bytes([105; 32], false).encrypt(value)?,
        rotatable: true,
    })
}
fn plaintext(secret: &Secret) -> Result<String> {
    SecretsManager::from_key_bytes([105; 32], false).decrypt(&secret.value_encrypted)
}
async fn sqlite() -> Result<(tempfile::TempDir, Database, String)> {
    let root = tempfile::tempdir()?;
    let url = format!(
        "sqlite:{}?mode=rwc",
        root.path().join("secrets.sqlite").display()
    );
    let db = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 8,
        connect_timeout_seconds: 5,
    })
    .await?;
    db.run_migrations().await?;
    Ok((root, db, url))
}
async fn records(db: &Database, key: &str) -> Result<Vec<(String, String, String, i64)>> {
    let rows = sqlx::query("SELECT secret_id,value_encrypted,CAST(created_at AS TEXT) AS created,CAST(CASE WHEN rotatable THEN 1 ELSE 0 END AS BIGINT) AS rotatable FROM secrets WHERE scope='global' AND scope_id IS NULL AND key=$1 ORDER BY secret_id")
        .bind(key).fetch_all(&db.pool).await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get("secret_id")?,
                row.try_get("value_encrypted")?,
                row.try_get("created")?,
                row.try_get("rotatable")?,
            ))
        })
        .collect()
}
async fn contract(db: &Database) -> Result<()> {
    let store = ExtensionStore::new(&db.pool);
    let original = input("contract-vpn", "original")?;
    assert!(store.insert_secret_if_absent(&original).await?);
    let before = store
        .get_secret(SecretScope::Global, None, &original.key)
        .await?
        .context("created key")?;
    assert!(
        !store
            .insert_secret_if_absent(&input(&original.key, "must-not-replace")?)
            .await?
    );
    assert_eq!(records(db, &original.key).await?.len(), 1);
    for index in 0..12 {
        let mut change = input(&original.key, &format!("rotation-{index}"))?;
        change.rotatable = index % 2 == 0;
        store.upsert_secret(&change).await?;
        let current = store
            .get_secret(SecretScope::Global, None, &original.key)
            .await?
            .context("rotated key")?;
        assert_eq!(current.secret_id, before.secret_id);
        assert_eq!(current.created_at, before.created_at);
        assert_eq!(current.rotatable, change.rotatable);
        assert_eq!(plaintext(&current)?, format!("rotation-{index}"));
        assert!(store.get_secret_by_id(change.secret_id).await?.is_none());
    }
    assert_eq!(records(db, &original.key).await?.len(), 1);
    store
        .update_secret(
            before.secret_id,
            &input("unused", "by-id-rotation")?.value_encrypted,
            true,
        )
        .await?;
    assert_eq!(
        plaintext(
            &store
                .get_secret_by_id(before.secret_id)
                .await?
                .context("retained API ID")?
        )?,
        "by-id-rotation"
    );

    // Scoped writes keep their existing unique-key/guard behavior and cannot
    // collide with a global name or a different provider's same name.
    for provider in [Uuid::new_v4(), Uuid::new_v4()] {
        let mut scoped = input(&original.key, "scoped-original")?;
        scoped.scope = SecretScope::Provider;
        scoped.scope_id = Some(provider);
        assert!(store.insert_secret_if_absent(&scoped).await?);
        scoped.secret_id = Uuid::new_v4();
        scoped.value_encrypted = input("unused", "scoped-new")?.value_encrypted;
        assert!(!store.insert_secret_if_absent(&scoped).await?);
        store.upsert_secret(&scoped).await?;
        assert_eq!(
            plaintext(
                &store
                    .get_secret(SecretScope::Provider, Some(provider), &original.key)
                    .await?
                    .context("provider key")?
            )?,
            "scoped-new"
        );
    }
    assert_eq!(records(db, &original.key).await?.len(), 1);

    let first = input("legacy-ambiguous", "private-old-value")?;
    store.upsert_secret(&first).await?;
    let duplicate = input(&first.key, "private-other-value")?;
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'global',NULL,$2,$3,TRUE)")
        .bind(duplicate.secret_id.to_string()).bind(&duplicate.key).bind(&duplicate.value_encrypted).execute(&db.pool).await?;
    let before = records(db, &first.key).await?;
    let error = store
        .get_secret(SecretScope::Global, None, &first.key)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), AMBIGUOUS);
    assert!(
        store
            .upsert_secret(&input(&first.key, "no-implicit-repair")?)
            .await
            .is_err()
    );
    assert!(
        store
            .insert_secret_if_absent(&input(&first.key, "no-implicit-repair")?)
            .await
            .is_err()
    );
    assert_eq!(
        before,
        records(db, &first.key).await?,
        "ambiguous values, IDs and times stay unchanged"
    );
    assert!(store.get_secret_by_id(first.secret_id).await?.is_some());
    assert!(store.get_secret_by_id(duplicate.secret_id).await?.is_some());
    // Existing explicit-ID review/removal remains available to the owner.
    store.delete_secret(duplicate.secret_id).await?;
    assert_eq!(
        plaintext(
            &store
                .get_secret(SecretScope::Global, None, &first.key)
                .await?
                .context("explicitly reviewed survivor")?
        )?,
        "private-old-value"
    );
    Ok(())
}
async fn lock_and_cancellation(db: &Database) -> Result<()> {
    let mut held = begin(&db.pool, "absent-lock-test").await?;
    if db.driver == DatabaseDriver::Postgres {
        let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&mut *held)
            .await?;
        assert_eq!(isolation, "read committed");
    }
    let pool = db.pool.clone();
    let mut writer = tokio::spawn(async move {
        ExtensionStore::new(&pool)
            .insert_secret_if_absent(&input("absent-lock-test", "after-lock")?)
            .await
    });
    sleep(Duration::from_millis(100)).await;
    assert!(
        !writer.is_finished(),
        "absent-key writer must wait for the original database lock"
    );
    // Commit a creator while the other process/connection waits. Its subsequent
    // read must see this committed key, not a pre-lock repeatable-read snapshot.
    let original = input("absent-lock-test", "held-created")?;
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'global',NULL,$2,$3,TRUE)")
        .bind(original.secret_id.to_string()).bind(&original.key).bind(&original.value_encrypted)
        .execute(&mut *held).await?;
    held.commit().await?;
    let result = timeout(Duration::from_secs(5), &mut writer).await;
    if result.is_err() {
        writer.abort();
        let _ = writer.await;
        anyhow::bail!("original database writer did not join");
    }
    let joined = result.context("original database writer deadline")?;
    assert!(
        !joined??,
        "a waited lock must observe the committed creator"
    );
    assert_eq!(records(db, "absent-lock-test").await?.len(), 1);
    assert_eq!(
        ExtensionStore::new(&db.pool)
            .get_secret(SecretScope::Global, None, "absent-lock-test")
            .await?
            .context("original creator")?
            .secret_id,
        original.secret_id
    );

    let data = input("cancelled-transaction", "original")?;
    ExtensionStore::new(&db.pool).upsert_secret(&data).await?;
    let before = records(db, &data.key).await?;
    let pool = db.pool.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let task = tokio::spawn(async move {
        let mut tx = begin(&pool, "cancelled-transaction").await?;
        sqlx::query("UPDATE secrets SET value_encrypted='uncommitted' WHERE scope='global' AND scope_id IS NULL AND key='cancelled-transaction'")
            .execute(&mut *tx).await?;
        signal.notify_one();
        std::future::pending::<()>().await;
        tx.commit().await?;
        Ok::<_, anyhow::Error>(())
    });
    if let Err(error) = timeout(Duration::from_secs(5), entered.notified()).await {
        task.abort();
        let _ = task.await;
        return Err(error.into());
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(before, records(db, &data.key).await?);
    // This new writer must acquire the same real DB lock after rollback.
    ExtensionStore::new(&db.pool)
        .upsert_secret(&input(&data.key, "after-cancellation")?)
        .await?;
    let current = ExtensionStore::new(&db.pool)
        .get_secret(SecretScope::Global, None, &data.key)
        .await?
        .context("post-cancel key")?;
    assert_eq!(current.secret_id, data.secret_id);
    assert_eq!(plaintext(&current)?, "after-cancellation");
    Ok(())
}

#[tokio::test]
async fn sqlite_global_rotation_create_only_ambiguity_and_cancellation_contract() -> Result<()> {
    let (_root, db, _url) = sqlite().await?;
    contract(&db).await?;
    lock_and_cancellation(&db).await
}

const CHILD: &str = "extensions::store::global_secrets::tests::global_secret_child_process";
async fn process_contract(db: &Database, root: &Path, url: &str) -> Result<()> {
    let token = Uuid::new_v4().to_string();
    sqlx::query("CREATE TABLE elixir_secret_writer_test_owner (token TEXT PRIMARY KEY)")
        .execute(&db.pool)
        .await?;
    sqlx::query("INSERT INTO elixir_secret_writer_test_owner(token) VALUES($1)")
        .bind(&token)
        .execute(&db.pool)
        .await?;
    std::fs::write(root.join("owner"), &token)?;
    let mut retained = None;
    for mode in ["create", "upsert"] {
        let mut children = JoinSet::new();
        for index in 0..4 {
            let mut command = tokio::process::Command::new(std::env::current_exe()?);
            command
                .args([CHILD, "--exact", "--ignored", "--nocapture"])
                .kill_on_drop(true)
                .env("ELIXIR_SECRET_TEST_URL", url)
                .env("ELIXIR_SECRET_TEST_TOKEN", &token)
                .env("ELIXIR_SECRET_TEST_ROOT", root)
                .env("ELIXIR_SECRET_TEST_MODE", mode)
                .env("ELIXIR_SECRET_TEST_INDEX", index.to_string());
            children.spawn(async move {
                let output = timeout(Duration::from_secs(45), command.output()).await??;
                anyhow::ensure!(
                    output.status.success(),
                    "child writer failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                Ok::<_, anyhow::Error>(())
            });
        }
        while let Some(result) = children.join_next().await {
            result??;
        }
        let current = ExtensionStore::new(&db.pool)
            .get_secret(SecretScope::Global, None, "cross-process-secret")
            .await?
            .context("child-created key")?;
        assert_eq!(records(db, "cross-process-secret").await?.len(), 1);
        if mode == "create" {
            let results = (0..4)
                .map(|index| std::fs::read_to_string(root.join(format!("{mode}-{index}.result"))))
                .collect::<std::io::Result<Vec<_>>>()?;
            assert_eq!(
                results
                    .iter()
                    .filter(|result| result.as_str() == "created")
                    .count(),
                1
            );
            assert_eq!(
                results
                    .iter()
                    .filter(|result| result.as_str() == "exists")
                    .count(),
                3
            );
            let winner = results
                .iter()
                .position(|result| result == "created")
                .context("one creator")?;
            assert_eq!(plaintext(&current)?, format!("fixture-create-{winner}"));
            retained = Some(current.secret_id);
        } else {
            assert_eq!(Some(current.secret_id), retained);
            assert!(plaintext(&current)?.starts_with("fixture-upsert-"));
            for index in 0..4 {
                assert_eq!(
                    std::fs::read_to_string(root.join(format!("{mode}-{index}.result")))?,
                    "8"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "actual SQLite child-process qualification; run exclusively after compiler joins"]
async fn sqlite_global_secret_cross_process_create_and_rotation() -> Result<()> {
    let (root, db, url) = sqlite().await?;
    process_contract(&db, root.path(), &url).await
}

#[tokio::test]
#[ignore = "actual PostgreSQL contract; ELIXIR_TEST_SECRET_STORE_POSTGRES_EMPTY_URL must name an EMPTY disposable database"]
async fn postgres_global_secret_contract_and_cross_process_concurrency() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_SECRET_STORE_POSTGRES_EMPTY_URL")?;
    let db = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 8,
        connect_timeout_seconds: 5,
    })
    .await?;
    anyhow::ensure!(db.driver == DatabaseDriver::Postgres, "PostgreSQL required");
    if let Ok(expected) = std::env::var("ELIXIR_TEST_SECRET_STORE_EXPECT_DEFAULT_ISOLATION") {
        anyhow::ensure!(
            matches!(expected.as_str(), "read committed" | "repeatable read"),
            "test isolation expectation bounds"
        );
        let actual: String = sqlx::query_scalar("SHOW default_transaction_isolation")
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(actual, expected);
    }
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=CURRENT_SCHEMA()",
    )
    .fetch_one(&db.pool)
    .await?;
    anyhow::ensure!(
        tables == 0,
        "refusing to modify a nonempty PostgreSQL database"
    );
    db.run_migrations().await?;
    contract(&db).await?;
    lock_and_cancellation(&db).await?;
    let root = tempfile::tempdir()?;
    process_contract(&db, root.path(), &url).await
}

#[tokio::test]
#[ignore = "invoked only by the owned parent contract with a database and filesystem ownership marker"]
async fn global_secret_child_process() -> Result<()> {
    let url = std::env::var("ELIXIR_SECRET_TEST_URL")?;
    let token = std::env::var("ELIXIR_SECRET_TEST_TOKEN")?;
    Uuid::parse_str(&token)?;
    let root = std::path::PathBuf::from(std::env::var("ELIXIR_SECRET_TEST_ROOT")?);
    anyhow::ensure!(
        std::fs::read_to_string(root.join("owner"))? == token,
        "fixture directory owner mismatch"
    );
    let index: usize = std::env::var("ELIXIR_SECRET_TEST_INDEX")?.parse()?;
    anyhow::ensure!(index < 4, "child index bounds");
    let mode = std::env::var("ELIXIR_SECRET_TEST_MODE")?;
    anyhow::ensure!(
        matches!(mode.as_str(), "create" | "upsert"),
        "child operation bounds"
    );
    let db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 2,
        connect_timeout_seconds: 5,
    })
    .await?;
    let owners: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM elixir_secret_writer_test_owner WHERE token=$1")
            .bind(token)
            .fetch_one(&db.pool)
            .await?;
    anyhow::ensure!(owners == 1, "fixture database owner mismatch");
    let store = ExtensionStore::new(&db.pool);
    let result = if mode == "create" {
        if store
            .insert_secret_if_absent(&input(
                "cross-process-secret",
                &format!("fixture-create-{index}"),
            )?)
            .await?
        {
            "created"
        } else {
            "exists"
        }
    } else {
        for iteration in 0..8 {
            store
                .upsert_secret(&input(
                    "cross-process-secret",
                    &format!("fixture-upsert-{index}-{iteration}"),
                )?)
                .await?;
        }
        "8"
    };
    use std::io::Write;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{mode}-{index}.result")))?;
    output.write_all(result.as_bytes())?;
    Ok(())
}
