use super::*;
use crate::marketplace::security_state::retained::require_retained;
use elixir_marketplace::retained_authority::AuthorityRecord;

#[tokio::test]
async fn retained_authority_survives_new_heads_key_rotation_receipt_expiry_and_restart()
-> Result<()> {
    let (directory, db) = database().await?;
    let (d, policy) = fixture()?;
    let authority = AuthorityRecord::from_descriptor(&d)?.verify()?;
    assert!(
        require_retained(&db, &authority).await.is_err(),
        "no untracked enrollment"
    );
    let (receipt, owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    require_retained(&db, &authority).await?;
    let mut future = d.clone();
    future.head.payload.sequence += 1;
    resign(&mut future.head, &key(2))?;
    let future = AuthorityRecord::from_descriptor(&future)?.verify()?;
    assert!(
        require_retained(&db, &future).await.is_err(),
        "do not repair a rolled-back or behind security store"
    );
    let mut newer = d.clone();
    newer.head.payload.sequence += 1;
    newer.head.payload.stable_release_id = Digest::of(b"a newer release");
    resign(&mut newer.head, &key(2))?;
    mutate(&db, incoming(&newer)?, false).await?;
    assert!(require_current(&db, &receipt).await.is_err());
    require_retained(&db, &authority).await?;
    // Release key 2 is retired normally, not root-revoked. An already enrolled
    // release can continue; this does not authorize installing it again.
    newer.publisher.payload.sequence += 1;
    newer.publisher.payload.release_keys =
        vec![PublicKey::from_bytes(key(4).verifying_key().to_bytes())?];
    resign(&mut newer.publisher, &key(1))?;
    let publisher = Digest::of(newer.publisher.to_bytes()?);
    newer.head.payload.publisher_state_id = publisher;
    newer.head.payload.sequence = 0;
    newer.head.authority_record_id = Some(publisher);
    resign(&mut newer.head, &key(4))?;
    mutate(&db, incoming(&newer)?, false).await?;
    owner(false);
    assert!(!receipt.is_current());
    require_retained(&db, &authority).await?;
    let bytes = authority.record().to_bytes()?;
    db.pool.close().await;
    let reopened = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            directory.path().join("security.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    let restored = AuthorityRecord::parse(&bytes)?.verify()?;
    require_retained(&reopened, &restored).await?;
    Ok(())
}

#[tokio::test]
async fn retained_authority_obeys_root_withdrawals_conflicts_and_store_integrity() -> Result<()> {
    for fault in [
        "release",
        "release_key",
        "head_key",
        "head_fork",
        "publisher_fork",
        "missing",
        "corrupt",
        "blocked",
        "accounting",
    ] {
        let (_directory, db) = database().await?;
        let (mut d, policy) = fixture()?;
        if fault == "head_key" {
            d.release_authority = Some(d.publisher.clone());
            d.publisher.payload.sequence += 1;
            d.publisher
                .payload
                .release_keys
                .push(PublicKey::from_bytes(key(4).verifying_key().to_bytes())?);
            resign(&mut d.publisher, &key(1))?;
            let publisher = Digest::of(d.publisher.to_bytes()?);
            d.head.payload.publisher_state_id = publisher;
            d.head.payload.sequence = 0;
            d.head.authority_record_id = Some(publisher);
            resign(&mut d.head, &key(4))?;
        }
        let authority = AuthorityRecord::from_descriptor(&d)?.verify()?;
        let (receipt, _owner) =
            MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
        admit(&db, &receipt).await?;
        match fault {
            "release" | "release_key" | "head_key" => {
                let mut revoked = d.clone();
                revoked.revocations.push(SignedRecord::sign(
                    Revocation {
                        root_key: authority.root_key(),
                        extension_id: authority.extension_id().clone(),
                        release_ids: if fault == "release" {
                            vec![authority.release().release_id()]
                        } else {
                            vec![]
                        },
                        release_keys: match fault {
                            "release_key" => vec![authority.release().signer_key()],
                            "head_key" => vec![authority.head_key()],
                            _ => vec![],
                        },
                        reason: "withdrawn".into(),
                    },
                    None,
                    &key(1),
                )?);
                mutate(&db, incoming(&revoked)?, false).await?;
            }
            "head_fork" => {
                let mut fork = d.clone();
                fork.head.payload.timestamp -= 1;
                resign(&mut fork.head, &key(2))?;
                mutate(&db, incoming(&fork)?, false).await?;
            }
            "publisher_fork" => {
                let mut fork = d.clone();
                fork.publisher
                    .payload
                    .release_keys
                    .push(PublicKey::from_bytes(key(9).verifying_key().to_bytes())?);
                resign(&mut fork.publisher, &key(1))?;
                mutate(&db, incoming(&fork)?, false).await?;
            }
            "missing" => {
                sqlx::query("DELETE FROM marketplace_extension_security")
                    .execute(&db.pool)
                    .await?;
            }
            "corrupt" => {
                sqlx::query("UPDATE marketplace_extension_security SET state_sha256=$1")
                    .bind(Digest::of(b"corrupt").to_string())
                    .execute(&db.pool)
                    .await?;
            }
            "blocked" => {
                sqlx::query("UPDATE marketplace_security_budget SET blocked=1")
                    .execute(&db.pool)
                    .await?;
            }
            "accounting" => {
                sqlx::query("UPDATE marketplace_security_budget SET encoded_bytes=1")
                    .execute(&db.pool)
                    .await?;
            }
            _ => unreachable!(),
        }
        assert!(require_retained(&db, &authority).await.is_err(), "{fault}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL retained-authority transaction qualification"]
async fn retained_postgres_readers_share_fences_and_hold_a_root_withdrawal_until_commit()
-> Result<()> {
    use std::time::Duration;
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_RETAINED_AUTHORITY_POSTGRES_URL")
        .context("supply a fresh dedicated PostgreSQL test database")?;
    let config = DatabaseConfig {
        url,
        max_connections: 6,
        connect_timeout_seconds: 5,
    };
    let db = Database::connect(&config).await?;
    if db.driver != DatabaseDriver::Postgres {
        bail!("explicit retained-authority fixture requires PostgreSQL");
    }
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'",
    )
    .fetch_one(&db.pool)
    .await?;
    if tables != 0 {
        bail!("dedicated retained-authority fixture database must be empty; no reset performed");
    }
    db.run_migrations().await?;
    let (d, policy) = fixture()?;
    let authority = AuthorityRecord::from_descriptor(&d)?.verify()?;
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    let mut first = db.pool.begin().await?;
    let mut second = db.pool.begin().await?;
    super::super::retained::require_in_transaction(&mut first, db.driver, &authority).await?;
    tokio::time::timeout(
        Duration::from_secs(2),
        super::super::retained::require_in_transaction(&mut second, db.driver, &authority),
    )
    .await??;
    let mut revoked = d.clone();
    revoked.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: authority.root_key(),
            extension_id: authority.extension_id().clone(),
            release_ids: vec![authority.release().release_id()],
            release_keys: vec![],
            reason: "withdrawn".into(),
        },
        None,
        &key(1),
    )?);
    let input = incoming(&revoked)?;
    let writer_db = db.clone();
    let mut writer = tokio::spawn(async move { mutate(&writer_db, input, false).await });
    // Observe an ACTUAL blocked PG backend, not merely a slow task or sleep.
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let waiting:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND state='active' AND query LIKE '%marketplace_security_budget%'")
                .fetch_one(&db.pool).await?;
            if waiting>0 {return Ok::<_,anyhow::Error>(());}
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.context("withdrawal did not reach the actual retained security lock")??;
    first.rollback().await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut writer)
            .await
            .is_err(),
        "second reader must still hold the writer"
    );
    second.rollback().await?;
    assert!(tokio::time::timeout(Duration::from_secs(3), &mut writer).await???);
    assert!(require_retained(&db, &authority).await.is_err());
    db.pool.close().await;
    let reopened = Database::connect(&config).await?;
    assert!(
        require_retained(&reopened, &authority).await.is_err(),
        "withdrawal persists across connection replacement"
    );
    let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT COUNT(*) FROM marketplace_extension_security),(SELECT COUNT(*) FROM marketplace_publisher_security)")
        .fetch_one(&reopened.pool).await?;
    assert_eq!(counts, (1, 1), "checks create no identity/history rows");
    reopened.pool.close().await;
    Ok(())
}
