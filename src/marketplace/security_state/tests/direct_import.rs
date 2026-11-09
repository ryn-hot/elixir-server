//! Actual SQLite state and signatures; receipt/protection owners are synthetic.
//! No source availability, installed code, native UID or public-path claim.
use super::*;
use elixir_marketplace::publication::PreparedPublication;

pub(super) fn receipt(
    descriptor: &CandidateDescriptor,
) -> Result<(PublicationObservation, impl Fn(bool) + use<>)> {
    PublicationObservation::fixture_for_verified_publication(PreparedPublication {
        schema_version: 1,
        publisher: descriptor.publisher.clone(),
        release: descriptor.release.clone(),
        head: descriptor.head.clone(),
    })
}

#[tokio::test]
async fn unknown_direct_references_allocate_nothing_but_known_head_changes_are_revisioned()
-> Result<()> {
    let (_directory, db) = database().await?;
    let (descriptor, policy) = fixture()?;
    let (preview, _owner) = receipt(&descriptor)?;
    let noop = catalog_observe_publication(&db, &preview, 0).await?;
    assert!(!noop.tracked());
    assert_eq!(noop.revision(), 0);
    assert!(noop.into_batch().is_none());
    assert!(!require_publication_current(&db, &preview).await?);
    assert_eq!(catalog_baseline(&db).await?.batches().count(), 0);
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    admit(&db, &metadata).await?;
    assert!(require_publication_current(&db, &preview).await?);
    let mut next = descriptor.clone();
    // This release has no matching paid evidence: a preview can authenticate
    // its signatures, but cannot turn old-release supporters into admission.
    next.release.payload.description = "New signed metadata, not adopted".into();
    resign(&mut next.release, &key(2))?;
    next.head.payload.sequence += 1;
    next.head.payload.stable_release_id = Digest::of(canonical::to_vec(&next.release.payload)?);
    resign(&mut next.head, &key(2))?;
    assert!(
        next.verify_metadata(
            chrono::Utc::now().timestamp() as u64,
            &policy,
            &mut elixir_marketplace::records::SignatureCache::new(128)?
        )
        .is_err()
    );
    let (next_preview, _next_owner) = receipt(&next)?;
    let mutation = catalog_observe_publication(&db, &next_preview, 1).await?;
    assert!(mutation.tracked());
    assert_eq!(mutation.revision(), 2);
    let delta = mutation
        .into_batch()
        .context("known head change missing mirror delta")?;
    assert_eq!(delta.extensions.len(), 1);
    delta.validate()?;
    assert!(require_publication_current(&db, &next_preview).await?);
    assert!(require_publication_current(&db, &preview).await.is_err());
    assert!(require_current(&db, &metadata).await.is_err());
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_security")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(rows, 1);
    Ok(())
}

#[tokio::test]
async fn direct_previews_cannot_clear_withdrawals_or_same_sequence_conflicts() -> Result<()> {
    for withdrawn in ["release", "key", "fork"] {
        let (_directory, db) = database().await?;
        let (mut descriptor, policy) = fixture()?;
        let (metadata, _owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
        admit(&db, &metadata).await?;
        if withdrawn == "fork" {
            descriptor.head.payload.timestamp -= 1;
            resign(&mut descriptor.head, &key(2))?;
        } else {
            let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
            descriptor.revocations.push(SignedRecord::sign(
                Revocation {
                    root_key: descriptor.publisher.payload.root_key,
                    extension_id: descriptor.release.payload.extension_id.clone(),
                    release_ids: if withdrawn == "release" {
                        vec![release]
                    } else {
                        vec![]
                    },
                    release_keys: if withdrawn == "key" {
                        vec![descriptor.release.signer_key]
                    } else {
                        vec![]
                    },
                    reason: "direct-preview test withdrawal".into(),
                },
                None,
                &key(1),
            )?);
            assert!(mutate(&db, incoming(&descriptor)?, false).await?);
        }
        let (preview, _preview_owner) = receipt(&descriptor)?;
        let revision = catalog_baseline(&db).await?.revision();
        let mutation = catalog_observe_publication(&db, &preview, revision).await?;
        if withdrawn == "fork" {
            assert!(mutation.into_batch().is_some());
        }
        let error = require_publication_current(&db, &preview)
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<Rejection>(),
            Some(Rejection::Withdrawn | Rejection::Conflicted)
        ));
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_security")
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(rows, 1);
    }
    Ok(())
}

#[tokio::test]
async fn untracked_siblings_check_known_publisher_state_without_consuming_storage() -> Result<()> {
    let (_directory, db) = database().await?;
    let (mut descriptor, policy) = fixture()?;
    let (metadata, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    admit(&db, &metadata).await?;
    let other = ExtensionId::new(&descriptor.publisher.payload.root_key, "untracked-sibling")?;
    descriptor.release.payload.extension_id = other.clone();
    descriptor.head.payload.extension_id = other;
    resign(&mut descriptor.release, &key(2))?;
    descriptor.head.payload.stable_release_id =
        Digest::of(canonical::to_vec(&descriptor.release.payload)?);
    resign(&mut descriptor.head, &key(2))?;
    let (preview, _preview_owner) = receipt(&descriptor)?;
    assert!(
        !catalog_observe_publication(&db, &preview, 1)
            .await?
            .tracked()
    );
    assert!(!require_publication_current(&db, &preview).await?);
    assert_eq!(catalog_baseline(&db).await?.revision(), 1);
    descriptor.publisher.payload.sequence = 1;
    resign(&mut descriptor.publisher, &key(1))?;
    let authority = Digest::of(descriptor.publisher.to_bytes()?);
    descriptor.release.authority_record_id = Some(authority);
    resign(&mut descriptor.release, &key(2))?;
    descriptor.head.authority_record_id = Some(authority);
    descriptor.head.payload.publisher_state_id = authority;
    resign(&mut descriptor.head, &key(2))?;
    let (preview, _preview_owner) = receipt(&descriptor)?;
    assert!(!require_publication_current(&db, &preview).await?);
    assert_eq!(catalog_baseline(&db).await?.revision(), 1);
    Ok(())
}

#[tokio::test]
async fn preview_receipt_expiry_and_corrupt_storage_fail_without_repair_or_allocation() -> Result<()>
{
    for fault in ["expired", "blocked", "accounting", "missing"] {
        let (_directory, db) = database().await?;
        let (descriptor, _) = fixture()?;
        let (preview, owner) = receipt(&descriptor)?;
        match fault {
            "expired" => owner(false),
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
            _ => {
                sqlx::query("DELETE FROM marketplace_security_budget")
                    .execute(&db.pool)
                    .await?;
            }
        }
        assert!(
            require_publication_current(&db, &preview).await.is_err(),
            "{fault}"
        );
        assert!(
            catalog_observe_publication(&db, &preview, 0).await.is_err(),
            "{fault}"
        );
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_security")
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(rows, 0);
    }
    Ok(())
}
