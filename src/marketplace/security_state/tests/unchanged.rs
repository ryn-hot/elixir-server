use super::*;

async fn revision(db: &Database) -> Result<u64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM marketplace_security_budget WHERE singleton=1",
    )
    .fetch_one(&db.pool)
    .await? as u64)
}

#[tokio::test]
async fn warm_catalog_admit_uses_no_write_and_never_allocates_a_cold_identity() -> Result<()> {
    let (_directory, db) = database().await?;
    let (descriptor, policy) = fixture()?;
    let initial = revision(&db).await?;
    assert!(
        super::super::unchanged::check(&db, &incoming(&descriptor)?, initial)
            .await?
            .is_none()
    );
    assert_eq!(revision(&db).await?, initial);
    let (metadata, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let enrolled = catalog_admit(&db, &metadata, initial).await?;
    let current = enrolled.revision();
    assert!(enrolled.into_batch().is_some());
    // The old implementation writes even for an exact warm no-op. A real SQL
    // trigger proves this optimization does not enter that write path.
    sqlx::query("CREATE TRIGGER forbid_security_write BEFORE UPDATE ON marketplace_security_budget BEGIN SELECT RAISE(FAIL,'unexpected security write'); END")
        .execute(&db.pool).await?;
    for _ in 0..3 {
        let mutation = catalog_admit(&db, &metadata, current).await?;
        assert!(mutation.tracked());
        assert_eq!(mutation.revision(), current);
        assert!(mutation.into_batch().is_none());
        require_current(&db, &metadata).await?;
    }
    assert_eq!(revision(&db).await?, current);
    Ok(())
}

#[tokio::test]
async fn same_head_with_new_withdrawal_and_new_head_still_use_durable_merge() -> Result<()> {
    let (_directory, db) = database().await?;
    let (mut descriptor, policy) = fixture()?;
    let (metadata, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let current = catalog_admit(&db, &metadata, revision(&db).await?)
        .await?
        .revision();
    // Even a withdrawal unrelated to this admitted release must be retained.
    let withdrawn = Digest::of(b"a different release");
    descriptor.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: descriptor.publisher.payload.root_key,
            extension_id: descriptor.release.payload.extension_id.clone(),
            release_ids: vec![withdrawn],
            release_keys: vec![],
            reason: "withdrawn".into(),
        },
        None,
        &key(1),
    )?);
    assert!(
        super::super::unchanged::check(&db, &incoming(&descriptor)?, current)
            .await?
            .is_none()
    );
    let (updated, _updated_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let mutation = catalog_admit(&db, &updated, current).await?;
    let next = mutation.revision();
    assert_eq!(next, current + 1);
    assert!(mutation.into_batch().is_some());
    let stored: String = sqlx::query_scalar(
        "SELECT state_json FROM marketplace_extension_security WHERE extension_id=$1",
    )
    .bind(descriptor.release.payload.extension_id.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert!(
        canonical::decode::<ExtensionState>(stored.as_bytes(), 16384)?
            .revoked_releases
            .contains(&withdrawn)
    );
    descriptor.head.payload.sequence += 1;
    resign(&mut descriptor.head, &key(2))?;
    assert!(
        super::super::unchanged::check(&db, &incoming(&descriptor)?, next)
            .await?
            .is_none()
    );
    let (new_head, _new_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    assert!(
        catalog_admit(&db, &new_head, next)
            .await?
            .into_batch()
            .is_some()
    );
    assert!(require_current(&db, &metadata).await.is_err());
    Ok(())
}

#[tokio::test]
async fn warm_no_change_cannot_hide_withdrawal_fork_revision_or_protection_loss() -> Result<()> {
    let (_directory, db) = database().await?;
    let (descriptor, policy) = fixture()?;
    let (metadata, protection) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let current = catalog_admit(&db, &metadata, revision(&db).await?)
        .await?
        .revision();
    assert!(catalog_admit(&db, &metadata, current - 1).await.is_err());
    let mut withdrawn = descriptor.clone();
    withdrawn.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: descriptor.publisher.payload.root_key,
            extension_id: descriptor.release.payload.extension_id.clone(),
            release_ids: vec![Digest::of(canonical::to_vec(&descriptor.release.payload)?)],
            release_keys: vec![],
            reason: "withdrawn".into(),
        },
        None,
        &key(1),
    )?);
    mutate(&db, incoming(&withdrawn)?, false).await?;
    // Omission cannot erase retained withdrawal. A no-delta response is still
    // NOT a currentness grant; the caller's mandatory check must reject it.
    assert!(
        catalog_admit(&db, &metadata, revision(&db).await?)
            .await?
            .into_batch()
            .is_none()
    );
    assert!(require_current(&db, &metadata).await.is_err());
    let mut fork = descriptor;
    fork.head.payload.timestamp -= 1;
    resign(&mut fork.head, &key(2))?;
    assert!(
        super::super::unchanged::check(&db, &incoming(&fork)?, revision(&db).await?)
            .await?
            .is_none()
    );
    protection(false);
    assert!(
        catalog_admit(&db, &metadata, revision(&db).await?)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn warm_read_fails_closed_on_budget_row_loss_and_integrity_corruption() -> Result<()> {
    for mutation in [
        "UPDATE marketplace_security_budget SET blocked=1",
        "UPDATE marketplace_security_budget SET publishers=publishers+1",
        "UPDATE marketplace_security_budget SET extensions=extensions+1",
        "UPDATE marketplace_security_budget SET encoded_bytes=encoded_bytes+1",
        "DELETE FROM marketplace_publisher_security",
        "DELETE FROM marketplace_extension_security",
        "DELETE FROM marketplace_security_budget",
        "UPDATE marketplace_extension_security SET state_sha256='sha256:0000000000000000000000000000000000000000000000000000000000000000'",
        "UPDATE marketplace_publisher_security SET state_sha256='sha256:0000000000000000000000000000000000000000000000000000000000000000'",
    ] {
        let (_directory, db) = database().await?;
        let (descriptor, policy) = fixture()?;
        let (metadata, _owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
        let current = catalog_admit(&db, &metadata, revision(&db).await?)
            .await?
            .revision();
        if mutation == "DELETE FROM marketplace_publisher_security" {
            // Simulate storage corruption on this private fixture connection;
            // normal FK enforcement correctly forbids creating the orphan.
            // Restore enforcement even if fault injection itself fails.
            let mut connection = db.pool.acquire().await?;
            sqlx::query("PRAGMA foreign_keys=OFF")
                .execute(&mut *connection)
                .await?;
            let injected = sqlx::query(mutation).execute(&mut *connection).await;
            sqlx::query("PRAGMA foreign_keys=ON")
                .execute(&mut *connection)
                .await?;
            injected?;
        } else {
            sqlx::query(mutation).execute(&db.pool).await?;
        }
        assert!(
            catalog_admit(&db, &metadata, current).await.is_err(),
            "{mutation}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL read-only warm-admission qualification"]
async fn warm_postgres_admission_reads_under_shared_lock_and_new_heads_still_commit() -> Result<()>
{
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_UNCHANGED_POSTGRES_URL")
        .context("supply a fresh dedicated PostgreSQL test database")?;
    let db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    if db.driver != DatabaseDriver::Postgres {
        bail!("explicit warm-admission fixture requires PostgreSQL");
    }
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'",
    )
    .fetch_one(&db.pool)
    .await?;
    if tables != 0 {
        bail!("dedicated warm-admission database must be empty; no reset performed");
    }
    db.run_migrations().await?;
    let (mut descriptor, policy) = fixture()?;
    let (metadata, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let current = catalog_admit(&db, &metadata, revision(&db).await?)
        .await?
        .revision();
    let mut locked = db.pool.begin().await?;
    sqlx::query("SELECT revision FROM marketplace_security_budget WHERE singleton=1 FOR SHARE")
        .fetch_one(&mut *locked)
        .await?;
    // An UPDATE, even revision=revision, conflicts with this actual shared
    // lock. The warm read must finish while it is still held by another conn.
    let mutation = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        catalog_admit(&db, &metadata, current),
    )
    .await
    .context("warm admission attempted a conflicting write")??;
    assert_eq!(mutation.revision(), current);
    assert!(mutation.into_batch().is_none());
    require_current(&db, &metadata).await?;
    locked.rollback().await?;
    descriptor.head.payload.sequence += 1;
    resign(&mut descriptor.head, &key(2))?;
    let (new_head, _new_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let mutation = catalog_admit(&db, &new_head, current).await?;
    assert_eq!(mutation.revision(), current + 1);
    assert!(mutation.into_batch().is_some());
    require_current(&db, &new_head).await?;
    assert!(require_current(&db, &metadata).await.is_err());
    Ok(())
}
