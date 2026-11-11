//! Parser/admission semantics with real signatures and synthetic owners only.
//! Native process provenance is qualified separately in the container fixtures.
use super::*;
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor, identity::PublicKey, records::SignedRecord, verifier::Verifier,
};
use serde_json::{Value, json};

pub(in crate::marketplace::worker::protected::source_preflight) fn fixture()
-> Result<(CandidateDescriptor, EvidenceBatch, IssuerPolicy)> {
    let data: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let mut descriptor = CandidateDescriptor::parse(&serde_json::to_vec(
        &json!({"schema_version":1,
        "publisher":data["publisher"],"release":data["release"],"head":data["head"],"revocations":[],
        "gate_evidence":data["evidence"]["pairs"]}),
    )?)?;
    let issuer = SigningKey::from_bytes(&[3; 32]);
    let now = observation_utc()?;
    for (index, pair) in descriptor.gate_evidence.iter_mut().enumerate() {
        pair.credential.payload.issued_at = now;
        pair.credential.payload.expires_at = now + 3600;
        pair.credential = SignedRecord::sign(pair.credential.payload.clone(), None, &issuer)?;
        pair.endorsement.payload.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(
            pair.endorsement.payload.clone(),
            None,
            &SigningKey::from_bytes(&[10 + index as u8; 32]),
        )?;
    }
    let batch = EvidenceBatch {
        schema_version: 1,
        release_id: Digest::of(canonical::to_vec(&descriptor.release.payload)?),
        snapshot_id: Digest::of(b"fixture snapshot"),
        pairs: descriptor.gate_evidence.clone(),
        next_cursor: None,
    };
    Ok((
        descriptor,
        batch,
        IssuerPolicy::new([PublicKey::from_bytes(issuer.verifying_key().to_bytes())?])?,
    ))
}
fn report(extension: &ExtensionId, batch: &EvidenceBatch, policy: &IssuerPolicy) -> Result<Value> {
    let bytes = canonical::to_vec(&VerificationRequest::VerifyEvidence {
        schema_version: 1,
        request_id: 7,
        extension_id: extension.clone(),
        release_id: batch.release_id,
        batch: batch.clone(),
    })?;
    Ok(serde_json::to_value(
        Verifier::new(policy.clone())?.process(&bytes, observation_utc()?),
    )?)
}

#[test]
fn native_batch_parser_binds_every_fact_and_never_counts_pairs_as_distinct_supporters() -> Result<()>
{
    let (descriptor, mut batch, policy) = fixture()?;
    let extension = &descriptor.release.payload.extension_id;
    batch.pairs = (0..EVIDENCE_BATCH_SIZE)
        .map(|i| batch.pairs[i % 5].clone())
        .collect();
    let report = report(extension, &batch, &policy)?;
    let bytes = canonical::to_vec(&report)?;
    assert!(bytes.len() <= MAX_REPLY);
    let facts = parse(&bytes, 7, extension, &batch, &policy)??;
    assert_eq!(facts.len(), 64);
    assert_eq!(
        facts
            .iter()
            .map(|f| f.subject)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        5
    );
    for pointer in [
        "/schema_version",
        "/request_id",
        "/outcome/verified/0/pair_index",
        "/outcome/verified/0/weight",
        "/outcome/verified/0/expires_at",
        "/outcome/invalid",
    ] {
        let mut changed = report.clone();
        *changed.pointer_mut(pointer).unwrap() =
            json!(changed.pointer(pointer).unwrap().as_u64().unwrap() + 1);
        assert!(
            parse(&canonical::to_vec(&changed)?, 7, extension, &batch, &policy).is_err(),
            "{pointer}"
        );
    }
    for pointer in [
        "/outcome/release_id",
        "/outcome/snapshot_id",
        "/outcome/verified/0/subject",
        "/outcome/verified/0/credential_id",
    ] {
        let mut changed = report.clone();
        *changed.pointer_mut(pointer).unwrap() = json!(Digest::of(b"not checked"));
        assert!(
            parse(&canonical::to_vec(&changed)?, 7, extension, &batch, &policy).is_err(),
            "{pointer}"
        );
    }
    let mut duplicated = report.clone();
    duplicated["outcome"]["verified"][1] = duplicated["outcome"]["verified"][0].clone();
    assert!(
        parse(
            &canonical::to_vec(&duplicated)?,
            7,
            extension,
            &batch,
            &policy
        )
        .is_err()
    );
    let mut extra = report.clone();
    extra["outcome"]["verified"][0]["trusted"] = json!(true);
    assert!(parse(&canonical::to_vec(&extra)?, 7, extension, &batch, &policy).is_err());
    let wrong = IssuerPolicy::new([PublicKey::from_bytes(
        SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes(),
    )?])?;
    assert!(parse(&bytes, 7, extension, &batch, &wrong).is_err());
    assert!(parse(&vec![b' '; MAX_REPLY + 1], 7, extension, &batch, &policy).is_err());
    Ok(())
}

#[test]
fn invalid_evidence_is_omitted_and_request_ids_stay_exact_on_the_wire() -> Result<()> {
    let (descriptor, mut batch, policy) = fixture()?;
    batch.pairs[0].credential.payload.tenure_weight = 1; // break the issuer signature
    let extension = &descriptor.release.payload.extension_id;
    let response = report(extension, &batch, &policy)?;
    assert_eq!(response["outcome"]["invalid"], 1);
    assert_eq!(
        parse(
            &canonical::to_vec(&response)?,
            7,
            extension,
            &batch,
            &policy
        )??
        .len(),
        4
    );
    for _ in 0..128 {
        let (id, bytes) = request(extension.clone(), batch.clone())?;
        assert!((1..=(1u64 << 53) - 1).contains(&id));
        assert_eq!(VerificationRequest::parse(&bytes)?.request_id(), id);
    }
    Ok(())
}

#[tokio::test]
async fn retained_evidence_contains_only_authenticated_bytes_and_no_foreign_cursor() -> Result<()> {
    let (descriptor, mut batch, policy) = fixture()?;
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    batch.next_cursor = Some("foreign-peer-next".into());
    batch.pairs[1].credential.payload.tenure_weight = 1;
    let (observation, _owner) = EvidenceObservation::fixture_for_batch(&metadata, &batch, &policy)?;
    let retained = observation.retained_batch(&metadata, &batch)?;
    assert_eq!(retained.pairs.len(), 4);
    assert_eq!(retained.release_id, batch.release_id);
    assert!(retained.next_cursor.is_none());
    assert_ne!(retained.snapshot_id, batch.snapshot_id);
    let expected = batch
        .pairs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 1)
        .map(|(_, pair)| pair.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        canonical::to_vec(&retained.pairs)?,
        canonical::to_vec(&expected)?
    );
    let extension = &metadata.descriptor().release.payload.extension_id;
    let checked = report(extension, &retained, &policy)?;
    assert_eq!(checked["outcome"]["invalid"], 0);
    assert_eq!(checked["outcome"]["verified"].as_array().unwrap().len(), 4);

    for pair in &mut batch.pairs {
        pair.credential.signature = "A".repeat(86);
    }
    let (invalid, _invalid_owner) =
        EvidenceObservation::fixture_for_batch(&metadata, &batch, &policy)?;
    let empty = invalid.retained_batch(&metadata, &batch)?;
    assert!(empty.pairs.is_empty());
    assert!(empty.next_cursor.is_none());
    Ok(())
}

#[tokio::test]
async fn retained_evidence_cannot_be_rebound_to_different_input_policy_or_facts() -> Result<()> {
    let (descriptor, batch, policy) = fixture()?;
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let (mut observation, _owner) =
        EvidenceObservation::fixture_for_batch(&metadata, &batch, &policy)?;
    for variant in 0..6 {
        let mut changed = batch.clone();
        match variant {
            0 => changed.pairs.swap(0, 1),
            1 => changed.pairs[0].credential.signature = "A".repeat(86),
            2 => changed.pairs[0].endorsement.signature = "A".repeat(86),
            3 => changed.snapshot_id = Digest::of(b"other snapshot"),
            4 => changed.next_cursor = Some("other-cursor".into()),
            _ => changed.release_id = Digest::of(b"other release"),
        }
        assert!(
            observation.retained_batch(&metadata, &changed).is_err(),
            "variant {variant}"
        );
    }
    let extended_policy = IssuerPolicy::new([
        PublicKey::from_bytes(SigningKey::from_bytes(&[3; 32]).verifying_key().to_bytes())?,
        PublicKey::from_bytes(SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes())?,
    ])?;
    let (other_policy, _other_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &extended_policy)?;
    assert!(observation.retained_batch(&other_policy, &batch).is_err());
    // A malformed private receipt is not constructible by public callers, but
    // retention must still fail closed if an internal invariant regresses.
    let original = observation.verified[0].clone();
    observation.verified[0].subject = Digest::of(b"not this pair");
    assert!(observation.retained_batch(&metadata, &batch).is_err());
    observation.verified[0] = original;
    observation.verified.push(observation.verified[0].clone());
    assert!(observation.retained_batch(&metadata, &batch).is_err());
    observation.verified.pop();
    observation.examined -= 1;
    assert!(observation.retained_batch(&metadata, &batch).is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retained_evidence_needs_live_metadata_and_native_receipt_owners() -> Result<()> {
    let (descriptor, batch, policy) = fixture()?;
    let (metadata, metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let (observation, owner) = EvidenceObservation::fixture_for_batch(&metadata, &batch, &policy)?;
    assert_eq!(
        observation.retained_batch(&metadata, &batch)?.pairs.len(),
        5
    );
    owner(false);
    assert!(observation.retained_batch(&metadata, &batch).is_err());
    owner(true);
    metadata_owner(false);
    assert!(observation.retained_batch(&metadata, &batch).is_err());
    metadata_owner(true);
    assert!(observation.retained_batch(&metadata, &batch).is_ok());
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(observation.retained_batch(&metadata, &batch).is_err());
    Ok(())
}

#[tokio::test]
async fn retained_evidence_omits_credentials_that_expired_since_authentication() -> Result<()> {
    let (descriptor, mut batch, policy) = fixture()?;
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    let now = observation_utc()?;
    batch.pairs[0].credential.payload.issued_at = now - 120;
    batch.pairs[0].credential.payload.expires_at = now - 1;
    batch.pairs[0].credential = SignedRecord::sign(
        batch.pairs[0].credential.payload.clone(),
        None,
        &SigningKey::from_bytes(&[3; 32]),
    )?;
    batch.pairs[0].endorsement.payload.credential_id =
        Digest::of(batch.pairs[0].credential.to_bytes()?);
    batch.pairs[0].endorsement = SignedRecord::sign(
        batch.pairs[0].endorsement.payload.clone(),
        None,
        &SigningKey::from_bytes(&[10; 32]),
    )?;
    let extension = metadata.descriptor().release.payload.extension_id.clone();
    let (id, bytes) = request(extension.clone(), batch.clone())?;
    let historical = Verifier::new(policy.clone())?.process(&bytes, now - 60);
    let verified = parse(
        &canonical::to_vec(&historical)?,
        id,
        &extension,
        &batch,
        &policy,
    )??;
    assert_eq!(
        verified.len(),
        5,
        "all signatures were valid at the earlier instant"
    );
    let (mut observation, _owner) =
        EvidenceObservation::fixture_for_batch(&metadata, &batch, &policy)?;
    // Simulate a still-current receipt produced sixty seconds ago using real
    // historical signature verification, not a sleep or altered wall clock.
    observation.verified = verified;
    observation.observed_utc = now - 60;
    let retained = observation.retained_batch(&metadata, &batch)?;
    assert_eq!(retained.pairs.len(), 4);
    assert!(
        retained
            .pairs
            .iter()
            .all(|pair| pair.credential.payload.expires_at > now)
    );
    Ok(())
}

#[tokio::test]
async fn ranking_requires_current_metadata_scope_feature_and_the_existing_single_job_slot()
-> Result<()> {
    let (descriptor, mut batch, policy) = fixture()?;
    let (metadata, owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    // A changed, uncached proof must require the shared native slot. Exact
    // gate proofs now reuse the independently authenticated metadata cache.
    batch.pairs[0].credential.payload.subject = Digest::of(b"not cached");
    let mut source = super::super::tests::fixture()?;
    let client = Client(source.owner.clone());
    let permit = source.owner.slots.clone().try_acquire_owned()?;
    assert!(
        client
            .verify_evidence(&metadata, &batch)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("slot is busy")
    );
    let mut other = batch.clone();
    other.release_id = Digest::of(b"wrong release");
    assert!(
        client
            .verify_evidence(&metadata, &other)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("release differs")
    );
    owner(false);
    assert!(
        client
            .verify_evidence(&metadata, &batch)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("current matching metadata")
    );
    drop(permit);
    drop(client);
    Arc::get_mut(&mut source.owner)
        .unwrap()
        .binding
        .evidence_verifier = false;
    assert!(
        Client(source.owner.clone())
            .verify_evidence(&metadata, &batch)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("verification is unavailable")
    );
    assert!(!source.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn compact_native_facts_expire_and_cannot_outlive_their_protection_owner() -> Result<()> {
    let (descriptor, batch, policy) = fixture()?;
    let extension = descriptor.release.payload.extension_id;
    let verified = parse(
        &canonical::to_vec(&report(&extension, &batch, &policy)?)?,
        7,
        &extension,
        &batch,
        &policy,
    )??;
    let now = observation_utc()?;
    let (owner, protection) = watch::channel(Snapshot::timed(
        ProtectionState::Ready,
        Duration::from_secs(3600),
    ));
    let mut observation = EvidenceObservation {
        extension_id: extension,
        release_id: batch.release_id,
        batch_id: Digest::of(canonical::to_vec(&batch)?),
        issuer_policy: policy.fingerprint(),
        snapshot_id: batch.snapshot_id,
        next_cursor: None,
        examined: 5,
        verified,
        observed: Instant::now(),
        observed_utc: now,
        protection,
    };
    assert_eq!(observation.witnesses()?.len(), 5);
    observation.observed_utc = now + 1;
    assert!(observation.witnesses().is_err());
    observation.observed_utc = now - 301;
    assert!(observation.witnesses().is_err());
    observation.observed_utc = now;
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(observation.witnesses().is_err());
    observation.observed = Instant::now();
    assert!(observation.witnesses().is_ok());
    drop(owner);
    assert!(observation.witnesses().is_err());
    Ok(())
}
