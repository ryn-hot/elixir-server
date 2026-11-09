//! Cache accounting/deduplication tests inject synthetic native facts. They do
//! not establish native origin; separate process fixtures verify that boundary.
use super::*;
use elixir_marketplace::evidence::EvidencePair;
use std::collections::BTreeSet;

fn synthetic_batch(
    metadata: &MetadataObservation,
    indices: std::ops::Range<usize>,
    weight: u8,
    expiry: u64,
) -> Result<(EvidenceBatch, EvidenceObservation, watch::Sender<Snapshot>)> {
    let template = &metadata.descriptor().gate_evidence[0];
    let mut pairs = vec![];
    let mut verified = vec![];
    for index in indices {
        let mut pair: EvidencePair = template.clone();
        pair.credential.payload.subject = Digest::of(format!("synthetic cache subject {index}"));
        pair.credential.payload.tenure_weight = weight;
        pair.credential.payload.expires_at = expiry;
        let credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement.payload.credential_id = credential_id;
        verified.push(VerifiedWitness {
            pair_index: pairs.len(),
            subject: pair.credential.payload.subject,
            weight,
            expires_at: expiry,
            issuer: pair.credential.signer_key,
            credential_id,
        });
        pairs.push(pair);
    }
    let release = Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?);
    let batch = EvidenceBatch {
        schema_version: 1,
        release_id: release,
        snapshot_id: Digest::of(b"synthetic snapshot"),
        pairs,
        next_cursor: None,
    };
    let (owner, protection) = watch::channel(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(3600)),
    });
    let observation = EvidenceObservation {
        extension_id: metadata.descriptor().release.payload.extension_id.clone(),
        release_id: release,
        batch_id: Digest::of(canonical::to_vec(&batch)?),
        issuer_policy: metadata.issuer_policy(),
        snapshot_id: batch.snapshot_id,
        next_cursor: None,
        examined: batch.pairs.len(),
        verified,
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection,
    };
    Ok((batch, observation, owner))
}

#[tokio::test]
async fn retains_strongest_4096_deduplicates_renewals_and_never_inflates_from_replays() -> Result<()>
{
    let (descriptor, _, policy) = super::super::tests::fixture()?;
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let now = observation_utc()?;
    let mut cache = ScoreCache::default();
    let mut expected = metadata
        .descriptor()
        .gate_evidence
        .iter()
        .map(|p| p.credential.payload.subject)
        .collect::<BTreeSet<_>>();
    for start in (0..4096).step_by(64) {
        let (batch, observation, _owner) =
            synthetic_batch(&metadata, start..start + 64, 4, now + 3600)?;
        expected.extend(observation.verified.iter().map(|v| v.subject));
        cache.merge(&metadata, &batch, &observation, &policy, now)?;
        cache.merge(&metadata, &batch, &observation, &policy, now)?;
        assert!(cache.bytes <= MAX_BYTES);
    }
    let summary = cache.summary(&metadata, &policy, now)?;
    assert_eq!(summary.verified_supporters, 4096);
    assert_eq!(summary.weighted_score, 16384);
    assert!(summary.capped);
    assert!(!summary.is_global_total);
    let actual = cache
        .entries
        .back()
        .unwrap()
        .subjects
        .iter()
        .map(|fact| fact.subject)
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected.into_iter().take(4096).collect());
    // A renewal has a new credential digest but still counts only one subject.
    let first = *actual.first().unwrap();
    let (mut batch, mut observation, _owner) = synthetic_batch(&metadata, 0..1, 4, now + 4000)?;
    batch.pairs[0].credential.payload.subject = first;
    batch.pairs[0].endorsement.payload.credential_id =
        Digest::of(batch.pairs[0].credential.to_bytes()?);
    observation.verified[0].subject = first;
    observation.verified[0].credential_id = batch.pairs[0].endorsement.payload.credential_id;
    observation.batch_id = Digest::of(canonical::to_vec(&batch)?);
    cache.merge(&metadata, &batch, &observation, &policy, now)?;
    assert_eq!(
        cache.summary(&metadata, &policy, now)?.verified_supporters,
        4096
    );
    assert_eq!(
        cache
            .entries
            .back()
            .unwrap()
            .known(first)
            .unwrap()
            .expires_at,
        now + 4000
    );
    Ok(())
}

#[tokio::test]
async fn expires_individual_facts_rejects_scope_changes_and_clock_rollback() -> Result<()> {
    let (descriptor, gate, policy) = super::super::tests::fixture()?;
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let now = observation_utc()?;
    let mut cache = ScoreCache::default();
    let (batch, observation, _owner) = synthetic_batch(&metadata, 0..64, 1, now + 10)?;
    cache.merge(&metadata, &batch, &observation, &policy, now)?;
    assert_eq!(
        cache.summary(&metadata, &policy, now)?.verified_supporters,
        69
    );
    assert_eq!(
        cache
            .summary(&metadata, &policy, now + 11)?
            .verified_supporters,
        5
    );
    assert_eq!(
        cache.memo.len(),
        5,
        "expired signature memo entries must be pruned too"
    );
    assert!(cache.summary(&metadata, &policy, now + 10).is_err());
    let mut wrong = gate;
    wrong.release_id = Digest::of(b"different release");
    assert!(cache.lookup(&metadata, &wrong, &policy, now + 11).is_err());
    assert!(
        cache
            .summary(
                &metadata,
                &IssuerPolicy::new([elixir_marketplace::identity::PublicKey::from_bytes(
                    ed25519_dalek::SigningKey::from_bytes(&[4; 32])
                        .verifying_key()
                        .to_bytes()
                )?])?,
                now + 11
            )
            .is_err()
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cache_revalidates_immutable_signatures_beyond_receipt_ttl_without_native_process_or_slot()
-> Result<()> {
    let (descriptor, batch, policy) = super::super::tests::fixture()?;
    let (old, _old_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let source = super::super::super::tests::fixture()?;
    let client = Client(source.owner.clone());
    let _permit = source.owner.slots.clone().try_acquire_owned()?;
    assert_eq!(
        client
            .verify_evidence(&old, &batch)
            .await?
            .witnesses()?
            .len(),
        5
    );
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(!old.is_current());
    // The synthetic guard must be live again; cache reuse alone cannot do it.
    source.protection.send_replace(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(3600)),
    });
    let (fresh, _fresh_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let reply = client.verify_evidence(&fresh, &batch).await?;
    assert!(reply.is_current());
    assert_eq!(reply.witnesses()?.len(), 5);
    assert_eq!(reply.retained_batch(&fresh, &batch)?.pairs.len(), 5);
    assert!(
        !old.is_current(),
        "crypto reuse must not renew an older availability/metadata receipt"
    );
    assert_eq!(source.owner.slots.available_permits(), 0);
    Ok(())
}

#[tokio::test]
async fn unverified_dominated_claims_can_only_skip_work_not_become_verified_or_add_influence()
-> Result<()> {
    let (descriptor, mut batch, policy) = super::super::tests::fixture()?;
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let source = super::super::super::tests::fixture()?;
    let client = Client(source.owner.clone());
    let _permit = source.owner.slots.clone().try_acquire_owned()?;
    batch.pairs[0].credential.signature = "A".repeat(86);
    let observation = client.verify_evidence(&metadata, &batch).await?;
    assert_eq!(observation.examined(), 5);
    assert_eq!(
        observation.witnesses()?.len(),
        4,
        "the altered signature must not be reported as verified"
    );
    let retained = observation.retained_batch(&metadata, &batch)?;
    assert_eq!(retained.pairs.len(), 4);
    assert_eq!(
        canonical::to_vec(&retained.pairs)?,
        canonical::to_vec(&&batch.pairs[1..])?
    );
    assert!(retained.next_cursor.is_none());
    let summary = client.ranking_summary(&metadata)?;
    assert_eq!(summary.verified_supporters, 5);
    assert_eq!(summary.weighted_score, 20);
    batch.pairs[0].credential.payload.tenure_weight = 255;
    assert!(
        client.verify_evidence(&metadata, &batch).await.is_err(),
        "a claimed increase must require independent verification"
    );
    assert_eq!(client.ranking_summary(&metadata)?.weighted_score, 20);
    Ok(())
}

#[tokio::test]
async fn cache_population_stays_within_global_byte_and_identity_limits() -> Result<()> {
    let (descriptor, _, policy) = super::super::tests::fixture()?;
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let now = observation_utc()?;
    let mut cache = ScoreCache::default();
    let (batch, observation, _owner) = synthetic_batch(&metadata, 0..64, 4, now + 3600)?;
    for i in 0..MAX_ENTRIES * 4 {
        // Synthetic identities exercise allocation/eviction, not public-source
        // or signature admission. The real metadata scope cannot be mutated.
        let mut entry = cache.take(&metadata, &policy, now)?;
        entry.key.release = Digest::of(format!("synthetic release {i}"));
        for value in &observation.verified {
            entry.insert(
                Digest::of(canonical::to_vec(&batch.pairs[value.pair_index])?),
                value.clone(),
                now,
                &policy,
            );
        }
        cache.retain(entry);
        assert!(cache.entries.len() <= MAX_ENTRIES);
        assert!(cache.bytes <= MAX_BYTES);
        assert_eq!(
            cache.bytes,
            cache.entries.iter().map(Entry::bytes).sum::<usize>()
        );
    }
    // Also exercise byte eviction: entry count alone is not the allocation cap.
    for n in 0..40 {
        let mut entry = Entry {
            key: Key {
                extension: metadata.descriptor().release.payload.extension_id.clone(),
                release: Digest::of(format!("full synthetic release {n}")),
                policy: policy.fingerprint(),
            },
            subjects: vec![],
        };
        entry.merge(
            (0..4096).map(|i| CompactFact {
                subject: Digest::of(format!("quota subject {i}")),
                weight: 4,
                expires_at: now + 3600,
            }),
            now,
        );
        cache.retain(entry);
        assert!(cache.bytes <= MAX_BYTES);
        assert!(cache.bytes + cache.memo.len() * MEMO_CHARGE <= MAX_BYTES);
        assert!(cache.entries.len() <= MAX_ENTRIES);
        if n == 19 {
            assert_eq!(
                cache
                    .entries
                    .iter()
                    .filter(|e| e.subjects.len() == 4096)
                    .count(),
                20,
                "a full20-result hot page must fit without score-set thrashing"
            );
        }
    }
    Ok(())
}
