//! Actual isolated DB reads and locally signed receipts, not protected-network
//! or release-mode performance qualification.
use super::*;

fn renamed(base: &CandidateDescriptor, slug: &str) -> Result<CandidateDescriptor> {
    let mut descriptor = base.clone();
    let id = ExtensionId::new(&descriptor.publisher.payload.root_key, slug)?;
    descriptor.release.payload.extension_id = id.clone();
    resign(&mut descriptor.release, &key(2))?;
    let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
    descriptor.head.payload.extension_id = id.clone();
    descriptor.head.payload.stable_release_id = release;
    resign(&mut descriptor.head, &key(2))?;
    for (index, pair) in descriptor.gate_evidence.iter_mut().enumerate() {
        pair.credential.payload.extension_id = id.clone();
        resign(&mut pair.credential, &key(3))?;
        pair.endorsement.payload.extension_id = id.clone();
        pair.endorsement.payload.release_id = release;
        pair.endorsement.payload.credential_id = Digest::of(pair.credential.to_bytes()?);
        resign(&mut pair.endorsement, &key(10 + index as u8))?;
    }
    Ok(descriptor)
}

pub(super) async fn full_page(
    db: &Database,
) -> Result<(Vec<MetadataObservation>, Vec<impl Fn(bool) + use<>>)> {
    let (base, policy) = fixture()?;
    let mut values = Vec::new();
    let mut owners = Vec::new();
    for index in 0..elixir_marketplace::MAX_SEARCH_RESULTS {
        let descriptor = renamed(&base, &format!("batch-{index:02}"))?;
        let (value, owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
        admit(db, &value).await?;
        values.push(value);
        owners.push(owner);
    }
    Ok((values, owners))
}

#[tokio::test]
async fn full_current_page_is_read_only_ordered_and_matches_all_single_checks() -> Result<()> {
    let (_directory, db) = database().await?;
    let (values, _owners) = full_page(&db).await?;
    sqlx::query("CREATE TRIGGER forbid_batch_security_write BEFORE UPDATE ON marketplace_security_budget BEGIN SELECT RAISE(FAIL,'unexpected security write'); END")
        .execute(&db.pool).await?;
    let references = values.iter().rev().collect::<Vec<_>>();
    assert_eq!(
        require_current_batch(&db, &references).await?,
        vec![true; 20]
    );
    for value in &references {
        require_current(&db, value).await?;
    }
    assert_eq!(
        require_current_batch(&db, &references).await?,
        vec![true; 20]
    );
    Ok(())
}

#[tokio::test]
async fn batch_rechecks_head_withdrawal_and_protection_without_hiding_unrelated_rows() -> Result<()>
{
    let (_directory, db) = database().await?;
    let (values, owners) = full_page(&db).await?;
    let references = values.iter().take(4).collect::<Vec<_>>();
    assert_eq!(
        require_current_batch(&db, &references).await?,
        vec![true; 4]
    );
    let mut newer = values[0].descriptor().clone();
    newer.head.payload.sequence += 1;
    resign(&mut newer.head, &key(2))?;
    mutate(&db, incoming(&newer)?, false).await?;
    let mut withdrawn = values[1].descriptor().clone();
    withdrawn.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: withdrawn.publisher.payload.root_key,
            extension_id: withdrawn.release.payload.extension_id.clone(),
            release_ids: vec![Digest::of(canonical::to_vec(&withdrawn.release.payload)?)],
            release_keys: vec![],
            reason: "withdrawn".into(),
        },
        None,
        &key(1),
    )?);
    mutate(&db, incoming(&withdrawn)?, false).await?;
    owners[2](false);
    let expected = vec![false, false, false, true];
    assert_eq!(require_current_batch(&db, &references).await?, expected);
    for (value, allowed) in references.iter().zip(expected) {
        assert_eq!(require_current(&db, value).await.is_ok(), allowed);
    }
    Ok(())
}

#[tokio::test]
async fn missing_blocked_corrupt_and_oversized_batch_authority_fail_closed() -> Result<()> {
    for statement in [
        "UPDATE marketplace_security_budget SET blocked=1",
        "DELETE FROM marketplace_security_budget",
        "DELETE FROM marketplace_extension_security",
        "UPDATE marketplace_extension_security SET state_sha256='sha256:0000000000000000000000000000000000000000000000000000000000000000'",
        "UPDATE marketplace_publisher_security SET state_sha256='sha256:0000000000000000000000000000000000000000000000000000000000000000'",
        "UPDATE marketplace_extension_security SET state_json=printf('%020000d',0)",
    ] {
        let (_directory, db) = database().await?;
        let (descriptor, policy) = fixture()?;
        let (metadata, _owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
        admit(&db, &metadata).await?;
        let mut connection = db.pool.acquire().await?;
        let oversized = statement.contains("printf");
        if oversized {
            // Simulate damaged on-disk state beyond schema CHECK bounds on
            // this one isolated SQLite connection. Restore checks BEFORE any
            // readback or returning the connection; production never does this.
            sqlx::query("PRAGMA ignore_check_constraints=ON")
                .execute(&mut *connection)
                .await?;
        }
        sqlx::query(statement).execute(&mut *connection).await?;
        if oversized {
            sqlx::query("PRAGMA ignore_check_constraints=OFF")
                .execute(&mut *connection)
                .await?;
        }
        drop(connection);
        let error = require_current_batch(&db, &[&metadata]).await.unwrap_err();
        assert!(
            !is_expected_rejection(&error),
            "damaged authority must fail-stop its owner"
        );
        assert!(require_current(&db, &metadata).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn batch_bounds_duplicates_and_expired_owner_do_not_need_database_access() -> Result<()> {
    let (_directory, db) = database().await?;
    let (descriptor, policy) = fixture()?;
    let (metadata, owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    db.pool.close().await;
    assert!(
        require_current_batch(&db, &[&metadata, &metadata])
            .await
            .is_err()
    );
    assert!(
        require_current_batch(&db, &vec![&metadata; 21])
            .await
            .is_err()
    );
    assert!(require_current_batch(&db, &[]).await?.is_empty());
    owner(false);
    assert_eq!(require_current_batch(&db, &[&metadata]).await?, vec![false]);
    Ok(())
}

#[tokio::test]
#[ignore = "explicit unthrottled local DB component benchmark; not protected HTTP/UI, playback or release qualification"]
async fn actual_twenty_row_currentness_batch_component_timing() -> Result<()> {
    let (_directory, db) = database().await?;
    let (values, _owners) = full_page(&db).await?;
    let references = values.iter().collect::<Vec<_>>();
    let mut single = Vec::new();
    let mut batch = Vec::new();
    for sample in 0..36 {
        for grouped in if sample % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let began = std::time::Instant::now();
            // The production view checks before AND after its worker read.
            for _ in 0..2 {
                if grouped {
                    assert_eq!(
                        require_current_batch(&db, &references).await?,
                        vec![true; 20]
                    );
                } else {
                    for value in &references {
                        require_current(&db, value).await?;
                    }
                }
            }
            if sample >= 4 {
                let samples = if grouped { &mut batch } else { &mut single };
                samples.push(began.elapsed().as_micros());
            }
        }
    }
    single.sort_unstable();
    batch.sort_unstable();
    eprintln!(
        "{}",
        serde_json::json!({"stage":"currentness_batch_component_result",
        "rows":20,"samples":32,"single_statements":40,"batch_statements":2,
        "single_p50_us":single[15],"single_p95_us":single[30],
        "batch_p50_us":batch[15],"batch_p95_us":batch[30],
        "release_mode":!cfg!(debug_assertions),"protected_network":false,"http_ui":false})
    );
    Ok(())
}
