//! A preview can skip duplicate work, never authenticate or persist new input.
use super::*;

#[tokio::test]
async fn duplicate_preview_is_read_only_and_a_bad_signature_cannot_gain_authority() -> Result<()> {
    let (_directory, db) = database().await?;
    let (d, policy) = fixture()?;
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    let mut records = incoming(&d)?.records;
    let before: i64 =
        sqlx::query_scalar("SELECT revision FROM marketplace_security_budget WHERE singleton=1")
            .fetch_one(&db.pool)
            .await?;
    assert!(!observation_needed(&db, &records).await?);
    // Matching already-retained payloads need no helper, even if untrusted
    // repeated bytes have a bad signature. This produces no security receipt.
    records.publisher.signature = "A".repeat(86);
    assert!(!observation_needed(&db, &records).await?);
    assert!(
        records
            .verify(chrono::Utc::now().timestamp() as u64)
            .is_err()
    );
    assert_eq!(
        before,
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM marketplace_security_budget WHERE singleton=1"
        )
        .fetch_one(&db.pool)
        .await?
    );
    require_current(&db, &receipt).await?;
    Ok(())
}

#[tokio::test]
async fn changed_preview_cannot_write_and_only_authenticated_merge_can_add_a_withdrawal()
-> Result<()> {
    let (_directory, db) = database().await?;
    let (mut d, policy) = fixture()?;
    let (receipt, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(d.clone(), &policy)?;
    admit(&db, &receipt).await?;
    d.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: d.publisher.payload.root_key,
            extension_id: d.release.payload.extension_id.clone(),
            release_ids: vec![Digest::of(canonical::to_vec(&d.release.payload)?)],
            release_keys: vec![],
            reason: "preview test".into(),
        },
        None,
        &key(1),
    )?);
    let records = incoming(&d)?.records;
    let before: i64 =
        sqlx::query_scalar("SELECT revision FROM marketplace_security_budget WHERE singleton=1")
            .fetch_one(&db.pool)
            .await?;
    assert!(observation_needed(&db, &records).await?);
    require_current(&db, &receipt).await?;
    assert_eq!(
        before,
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM marketplace_security_budget WHERE singleton=1"
        )
        .fetch_one(&db.pool)
        .await?
    );
    // This unit uses local cryptography, not a claim of private native/VPN
    // qualification. The real service must obtain its SecurityObservation.
    mutate(&db, incoming(&d)?, false).await?;
    assert!(!observation_needed(&db, &records).await?);
    assert!(require_current(&db, &receipt).await.is_err());
    let mut omitted = records.clone();
    omitted.revocations.clear();
    assert!(!observation_needed(&db, &omitted).await?);
    assert!(require_current(&db, &receipt).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unknown_preview_does_not_allocate_and_corrupt_accounting_does_not_become_a_noop()
-> Result<()> {
    let (_directory, db) = database().await?;
    let (d, _) = fixture()?;
    let records = incoming(&d)?.records;
    assert!(observation_needed(&db, &records).await?);
    assert_eq!(
        0,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_extension_security")
            .fetch_one(&db.pool)
            .await?
    );
    sqlx::query("UPDATE marketplace_security_budget SET blocked=1 WHERE singleton=1")
        .execute(&db.pool)
        .await?;
    assert!(observation_needed(&db, &records).await.is_err());
    Ok(())
}
