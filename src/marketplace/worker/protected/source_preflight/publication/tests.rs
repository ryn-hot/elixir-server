//! Real local signatures with synthetic private-process/protection fixtures.
//! Actual UID/cgroup provenance is exercised by the isolated image test.
use super::*;
use elixir_marketplace::{evidence::IssuerPolicy, verifier::Verifier};
use serde_json::{Value, json};

fn fixture() -> Result<(PreparedPublication, Value)> {
    let vector: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let publication = PreparedPublication::parse(&serde_json::to_vec(&json!({
        "schema_version":1,"publisher":vector["publisher"],"release":vector["release"],"head":vector["head"]
    }))?)?;
    let mut verifier = Verifier::new(IssuerPolicy::new([vector["issuer_key"]
        .as_str()
        .unwrap()
        .parse()?])?)?;
    let reply = serde_json::to_value(verifier.process(
        &canonical::to_vec(&VerificationRequest::VerifyPublication {
            schema_version: 1,
            request_id: 7,
            publication: Box::new(publication.clone()),
        })?,
        observation_utc()?,
    ))?;
    Ok((publication, reply))
}
#[test]
fn report_binding_rejects_forged_totals_scopes_flags_and_correlation() -> Result<()> {
    let (publication, report) = fixture()?;
    parse_reply(&serde_json::to_vec(&report)?, 7, &publication)??;
    for field in [
        "reference_id",
        "release_id",
        "publisher_state_id",
        "publisher_payload_id",
        "head_record_id",
        "head_payload_id",
    ] {
        let mut bad = report.clone();
        bad["outcome"]["summary"][field] = Digest::of(b"wrong").to_string().into();
        assert!(
            parse_reply(&serde_json::to_vec(&bad)?, 7, &publication).is_err(),
            "{field}"
        );
    }
    for field in ["publisher_sequence", "head_sequence"] {
        let mut bad = report.clone();
        bad["outcome"]["summary"][field] = 99.into();
        assert!(parse_reply(&serde_json::to_vec(&bad)?, 7, &publication).is_err());
    }
    for field in ["metadata_only", "discovery_qualified"] {
        let mut bad = report.clone();
        bad["outcome"][field] = (!bad["outcome"][field].as_bool().unwrap()).into();
        assert!(parse_reply(&serde_json::to_vec(&bad)?, 7, &publication).is_err());
    }
    let mut bad = report.clone();
    bad["outcome"]["supporters"] = 4096.into();
    assert!(parse_reply(&serde_json::to_vec(&bad)?, 7, &publication).is_err());
    assert!(parse_reply(&serde_json::to_vec(&report)?, 8, &publication).is_err());
    let rejection =
        json!({"schema_version":1,"request_id":7,"outcome":{"type":"rejected","code":"signature"}});
    assert!(parse_reply(&serde_json::to_vec(&rejection)?, 7, &publication)?.is_err());
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn receipts_expire_on_owner_loss_monotonic_time_and_clock_rollback() -> Result<()> {
    let (publication, _) = fixture()?;
    let (mut receipt, owner) =
        PublicationObservation::fixture_for_verified_publication(publication)?;
    assert!(receipt.is_current());
    assert!(receipt.valid_for() <= OBSERVATION_TTL);
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(!receipt.is_current());
    assert!(receipt.summary().is_err());
    receipt.observed = Instant::now();
    receipt.observed_utc = observation_utc()? + 1;
    assert!(!receipt.is_current());
    receipt.observed_utc = observation_utc()?;
    assert!(receipt.is_current());
    owner(false);
    assert!(!receipt.is_current());
    assert_eq!(receipt.valid_for(), Duration::ZERO);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn feature_absence_or_shared_slot_pressure_cannot_start_a_helper() -> Result<()> {
    let (publication, _) = fixture()?;
    let fixture = super::super::tests::fixture()?;
    fixture.owner.admit(true)?;
    let permit = fixture.owner.slots.clone().try_acquire_owned()?;
    let client = Client(fixture.owner.clone());
    assert!(client.verify_publication(&publication).await.is_err());
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    drop(permit);
    // Build a fresh, private test owner before changing its captured feature.
    drop(client);
    let mut owner = fixture.owner;
    Arc::get_mut(&mut owner)
        .context("shared fixture owner")?
        .binding
        .publication_verifier = false;
    assert!(
        Client(owner.clone())
            .verify_publication(&publication)
            .await
            .is_err()
    );
    assert!(!owner.failed.load(Ordering::SeqCst));
    Ok(())
}

fn prepared(
    descriptor: &elixir_marketplace::admission::CandidateDescriptor,
) -> PreparedPublication {
    PreparedPublication {
        schema_version: SCHEMA_VERSION,
        publisher: descriptor.publisher.clone(),
        release: descriptor.release.clone(),
        head: descriptor.head.clone(),
    }
}

#[tokio::test]
async fn preview_of_just_verified_records_does_not_compete_with_the_existing_helper() -> Result<()>
{
    let f = super::super::tests::fixture()?;
    let (descriptor, _, policy) = super::super::evidence::tests::fixture()?;
    let publication = prepared(&descriptor);
    let (metadata, _owner) =
        super::super::metadata::MetadataObservation::fixture_for_verified_descriptor(
            descriptor, &policy,
        )?;
    let times = metadata.observation_times();
    f.owner
        .metadata_cache
        .lock()
        .unwrap()
        .fixture_insert(&metadata)?;
    let held = f.owner.slots.clone().try_acquire_owned()?;
    let result = Client(f.owner.clone())
        .verify_publication_for_preview(&publication)
        .await?;
    assert_eq!(result.summary()?, &publication.verify(observation_utc()?)?);
    assert_eq!((result.observed, result.observed_utc), times);
    assert_eq!(
        f.owner.slots.available_permits(),
        0,
        "cache read must not acquire a helper slot"
    );
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    drop(held);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cached_publication_requires_all_exact_records_policy_and_original_lifetime() -> Result<()>
{
    let f = super::super::tests::fixture()?;
    let (descriptor, _, policy) = super::super::evidence::tests::fixture()?;
    let publication = prepared(&descriptor);
    let (metadata, _owner) =
        super::super::metadata::MetadataObservation::fixture_for_verified_descriptor(
            descriptor, &policy,
        )?;
    let times = metadata.observation_times();
    let mut cache = f.owner.metadata_cache.lock().unwrap();
    cache.fixture_insert(&metadata)?;
    assert!(
        cache
            .get_publication(&publication, Digest::of(b"other issuer policy"))?
            .is_none()
    );
    for part in 0..4 {
        let mut changed = publication.clone();
        match part {
            0 => changed.publisher.payload.sequence += 1,
            1 => changed.release.payload.description.push_str("changed"),
            2 => changed.head.payload.sequence += 1,
            _ => {
                changed.release.signer_key = elixir_marketplace::identity::PublicKey::from_bytes(
                    ed25519_dalek::SigningKey::from_bytes(&[99; 32])
                        .verifying_key()
                        .to_bytes(),
                )?
            }
        }
        assert!(
            cache
                .get_publication(&changed, policy.fingerprint())?
                .is_none()
        );
    }
    tokio::time::advance(Duration::from_secs(299)).await;
    let receipt = cache
        .get_publication(&publication, policy.fingerprint())?
        .context("original receipt should still be current")?;
    assert_eq!((receipt.observed, receipt.observed_utc), times);
    assert!(receipt.valid_for() <= Duration::from_secs(1));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(
        cache
            .get_publication(&publication, policy.fingerprint())?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn informational_cache_never_replaces_fresh_install_or_background_verification() -> Result<()>
{
    let f = super::super::tests::fixture()?;
    let (descriptor, _, policy) = super::super::evidence::tests::fixture()?;
    let publication = prepared(&descriptor);
    let (metadata, _owner) =
        super::super::metadata::MetadataObservation::fixture_for_verified_descriptor(
            descriptor, &policy,
        )?;
    f.owner
        .metadata_cache
        .lock()
        .unwrap()
        .fixture_insert(&metadata)?;
    let held = f.owner.slots.clone().try_acquire_owned()?;
    let client = Client(f.owner.clone());
    assert!(
        client
            .verify_publication(&publication)
            .await
            .err()
            .is_some_and(|error| error.to_string().contains("slot is busy"))
    );
    f.settings
        .send_modify(|settings| settings.mode = elixir_marketplace::policy::ContributionMode::On);
    assert!(
        client
            .verify_background_publication(&publication)
            .await
            .err()
            .is_some_and(|error| error.to_string().contains("slot is busy"))
    );
    drop(held);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn near_expiry_preview_requires_new_native_work_not_a_renewed_cache_timestamp() -> Result<()>
{
    let f = super::super::tests::fixture()?;
    let (descriptor, _, policy) = super::super::evidence::tests::fixture()?;
    let publication = prepared(&descriptor);
    let (metadata, _owner) =
        super::super::metadata::MetadataObservation::fixture_for_verified_descriptor(
            descriptor, &policy,
        )?;
    f.owner
        .metadata_cache
        .lock()
        .unwrap()
        .fixture_insert(&metadata)?;
    tokio::time::advance(Duration::from_secs(271)).await;
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(3600)),
    });
    f.facts
        .send_modify(|facts| facts.as_mut().unwrap().observed = Instant::now());
    let held = f.owner.slots.clone().try_acquire_owned()?;
    let client = Client(f.owner.clone());
    let mut preview = Box::pin(client.verify_publication_for_preview(&publication));
    assert!(
        futures_util::poll!(preview.as_mut()).is_pending(),
        "nearly expired native facts must not be silently renewed"
    );
    tokio::time::advance(super::super::super::RENEW_DEADLINE).await;
    assert!(
        preview
            .await
            .err()
            .is_some_and(|error| error.to_string().contains("slot deadline"))
    );
    drop(held);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn cached_publication_cannot_survive_its_native_owner_or_failed_live_admission() -> Result<()>
{
    let f = super::super::tests::fixture()?;
    let (descriptor, _, policy) = super::super::evidence::tests::fixture()?;
    let publication = prepared(&descriptor);
    let (metadata, owner) =
        super::super::metadata::MetadataObservation::fixture_for_verified_descriptor(
            descriptor, &policy,
        )?;
    f.owner
        .metadata_cache
        .lock()
        .unwrap()
        .fixture_insert(&metadata)?;
    owner(false);
    assert!(
        f.owner
            .metadata_cache
            .lock()
            .unwrap()
            .get_publication(&publication, policy.fingerprint())?
            .is_none()
    );
    let (fresh, _fresh_owner) =
        super::super::metadata::MetadataObservation::fixture_for_verified_descriptor(
            metadata.descriptor().clone(),
            &policy,
        )?;
    f.owner
        .metadata_cache
        .lock()
        .unwrap()
        .fixture_insert(&fresh)?;
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Failed);
    assert!(
        Client(f.owner.clone())
            .verify_publication_for_preview(&publication)
            .await
            .is_err()
    );
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn foreground_preview_waits_fairly_for_one_slot_and_cancellation_removes_its_waiter()
-> Result<()> {
    let f = super::super::tests::fixture()?;
    let held = f.owner.slots.clone().try_acquire_owned()?;
    let mut pending = Box::pin(publication_slot(&f.owner, Work::Foreground));
    assert!(futures_util::poll!(pending.as_mut()).is_pending());
    assert_eq!(f.owner.slots.available_permits(), 0);
    drop(held);
    let permit = pending.await?;
    assert_eq!(f.owner.slots.available_permits(), 0);
    drop(permit);
    let held = f.owner.slots.clone().try_acquire_owned()?;
    let mut cancelled = Box::pin(publication_slot(&f.owner, Work::Foreground));
    assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    drop(held);
    assert!(f.owner.slots.clone().try_acquire_owned().is_ok());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn preview_slot_wait_is_finite_and_rechecks_protection_before_any_launch() -> Result<()> {
    let f = super::super::tests::fixture()?;
    let held = f.owner.slots.clone().try_acquire_owned()?;
    let mut pending = Box::pin(publication_slot(&f.owner, Work::Foreground));
    assert!(futures_util::poll!(pending.as_mut()).is_pending());
    tokio::time::advance(super::super::super::RENEW_DEADLINE).await;
    assert!(
        pending
            .await
            .unwrap_err()
            .to_string()
            .contains("slot deadline")
    );
    let mut pending = Box::pin(publication_slot(&f.owner, Work::Foreground));
    assert!(futures_util::poll!(pending.as_mut()).is_pending());
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Failed);
    drop(held);
    assert!(pending.await.is_err());
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn publisher_renewal_is_background_only_and_shares_the_original_native_slot() -> Result<()> {
    let (publication, _) = fixture()?;
    let fixture = super::super::tests::fixture()?;
    let client = Client(fixture.owner.clone());
    // The test runtime is deliberately nonexistent: each denial must happen
    // before native launch, not by trying foreground work after background Off.
    assert!(
        client
            .verify_background_publication(&publication)
            .await
            .is_err()
    );
    fixture
        .settings
        .send_modify(|s| s.mode = elixir_marketplace::policy::ContributionMode::On);
    let permit = fixture.owner.slots.clone().try_acquire_owned()?;
    let error = client
        .verify_background_publication(&publication)
        .await
        .err()
        .context("shared slot was bypassed")?;
    assert!(error.to_string().contains("slot is busy"), "{error:#}");
    fixture
        .facts
        .send_modify(|f| f.as_mut().unwrap().background_allowance = false);
    assert!(
        client
            .verify_background_publication(&publication)
            .await
            .is_err()
    );
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    drop(permit);
    Ok(())
}

#[tokio::test]
async fn direct_preview_reuses_exact_source_cache_but_installation_requires_a_fresh_helper()
-> Result<()> {
    let (publication, _) = fixture()?;
    let (receipt, owner) = PublicationObservation::fixture_for_verified_publication(publication)?;
    let fixture = super::super::tests::fixture()?;
    let plan = elixir_marketplace::source_preflight::SourcePreflightRequest::for_release(
        &receipt.publication().release.payload,
        0,
        Digest::of(b"direct reference source test"),
        ProbeFamily::Ipv4,
    )?
    .plan;
    let source = PublicSourceObservation {
        release_id: plan.release_id,
        location_id: plan.location_id,
        license_sha256: Digest::of(b"SPDX-License-Identifier: MIT\n"),
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection: fixture.owner.protection.clone(),
    };
    fixture.owner.source_cache.lock().unwrap().insert(&source);
    let _permit = fixture.owner.slots.clone().try_acquire_owned()?;
    let client = Client(fixture.owner.clone());
    let cached = client.request_publication(&receipt, 0).await?;
    assert_eq!(cached.release_id(), receipt.summary()?.release_id);
    assert_eq!(
        cached.observed, source.observed,
        "cache access cannot renew source evidence"
    );
    assert_eq!(
        fixture.owner.slots.available_permits(),
        0,
        "no helper was launched"
    );
    let error = client
        .request_fresh_publication(&receipt, 0)
        .await
        .err()
        .context("installation reused a cached direct-source fact")?;
    assert!(error.to_string().contains("slot is busy"), "{error:#}");
    assert_eq!(fixture.owner.slots.available_permits(), 0);
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    assert_eq!(
        client.request_publication(&receipt, 0).await?.observed,
        source.observed,
        "fresh admission failure must not renew or destroy preview evidence"
    );
    assert!(
        client
            .request_publication(&receipt, usize::MAX)
            .await
            .is_err()
    );
    fixture.owner.failed.store(true, Ordering::SeqCst);
    assert!(client.request_publication(&receipt, 0).await.is_err());
    assert!(client.request_fresh_publication(&receipt, 0).await.is_err());
    fixture.owner.failed.store(false, Ordering::SeqCst);
    owner(false);
    assert!(client.request_publication(&receipt, 0).await.is_err());
    assert!(client.request_fresh_publication(&receipt, 0).await.is_err());
    Ok(())
}
