use super::*;
use elixir_marketplace::verifier::Verifier;
use serde_json::{Value, json};

#[tokio::test(start_paused = true)]
async fn sharing_renewal_requires_more_validity_than_an_aging_search_cache_hit() -> Result<()> {
    let (descriptor, report, _) = fixture()?;
    let f = super::super::tests::fixture()?;
    f.settings
        .send_modify(|settings| settings.mode = elixir_marketplace::policy::ContributionMode::On);
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(3600)),
    });
    assert!(SHARING_RENEWAL_LEAD > super::super::super::CHECKING_DEADLINE);
    let mut support = parse_reply(&serde_json::to_vec(&report)?, 7, &descriptor)??;
    support.valid_until = Some(observation_utc()? + 3600);
    let metadata = MetadataObservation {
        descriptor,
        support,
        issuer_policy: IssuerPolicy::new(f.owner.binding.issuer_keys.iter().copied())?
            .fingerprint(),
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection: f.owner.protection.clone(),
    };
    f.owner.metadata_cache.lock().unwrap().insert(&metadata)?;
    let client = Client(f.owner.clone());
    let _held = f.owner.slots.clone().try_acquire_owned()?;
    // An actually fresh receipt is reusable; no extra native helper is needed.
    assert_eq!(
        client
            .verify_background_metadata(&metadata.descriptor)
            .await?
            .observed,
        metadata.observed
    );
    tokio::time::advance(OBSERVATION_TTL - SHARING_RENEWAL_LEAD + Duration::from_secs(1)).await;
    f.facts
        .send_modify(|facts| facts.as_mut().unwrap().observed = Instant::now());
    let error = client
        .verify_background_metadata(&metadata.descriptor)
        .await
        .err()
        .context("aging receipt incorrectly counted as renewal")?;
    assert!(admission::verification_deferred(&error), "{error:#}");
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    assert_eq!(f.owner.slots.available_permits(), 0);
    // Ordinary current search facts remain reusable under their original TTL.
    let cached = client.verify_metadata(&metadata.descriptor).await?;
    assert_eq!(cached.observed, metadata.observed);
    assert_eq!(cached.observed_utc, metadata.observed_utc);
    tokio::time::advance(SHARING_RENEWAL_LEAD).await;
    assert!(
        !cached.is_current(),
        "a failed renewal must not extend authority"
    );
    Ok(())
}

#[tokio::test]
async fn separate_release_authority_never_becomes_a_plain_publication_receipt() -> Result<()> {
    let (descriptor, _, policy) = super::super::evidence::tests::fixture()?;
    let publication = elixir_marketplace::publication::PreparedPublication {
        schema_version: SCHEMA_VERSION,
        publisher: descriptor.publisher.clone(),
        release: descriptor.release.clone(),
        head: descriptor.head.clone(),
    };
    let (mut value, _owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    // Synthetic negative cache-shape input, not a new signature-qualified
    // receipt. Any separate authority must force native publication checking.
    value.descriptor.release_authority = Some(value.descriptor.publisher.clone());
    let mut cache = ReceiptCache::default();
    cache.insert(&value)?;
    assert!(
        cache
            .get_publication(&publication, policy.fingerprint())?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn search_warming_reads_positive_receipts_only_without_launching_neighbor_verifiers()
-> Result<()> {
    let f = super::super::tests::fixture()?;
    let (descriptor, _, policy) = super::super::evidence::tests::fixture()?;
    let (receipt, owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
    let client = Client(f.owner.clone());
    let held = f.owner.slots.clone().try_acquire_owned()?;
    assert!(client.cached_metadata(&descriptor)?.is_none());
    f.owner.metadata_cache.lock().unwrap().insert(&receipt)?;
    let cached = client
        .cached_metadata(&descriptor)?
        .context("positive native receipt missing")?;
    assert_eq!(cached.observed, receipt.observed);
    let mut invalid = descriptor.clone();
    invalid.head.payload.sequence += 1;
    assert!(client.cached_metadata(&invalid)?.is_none());
    owner(false);
    assert!(client.cached_metadata(&descriptor)?.is_none());
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    drop(held);
    assert!(f.owner.slots.clone().try_acquire_owned().is_ok());
    Ok(())
}

fn fixture() -> Result<(CandidateDescriptor, Value, u64)> {
    let data: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let descriptor = CandidateDescriptor::parse(&serde_json::to_vec(&json!({
        "schema_version":1, "publisher":data["publisher"], "release":data["release"],
        "head":data["head"], "revocations":[], "gate_evidence":data["evidence"]["pairs"],
    }))?)?;
    let mut verifier = Verifier::new(IssuerPolicy::new([data["issuer_key"]
        .as_str()
        .unwrap()
        .parse()?])?)?;
    let request = VerificationRequest::VerifyDescriptor {
        schema_version: 1,
        request_id: 7,
        descriptor: Box::new(descriptor.clone()),
    };
    let report = serde_json::to_value(
        verifier.process(&canonical::to_vec(&request)?, data["now"].as_u64().unwrap()),
    )?;
    Ok((descriptor, report, data["now"].as_u64().unwrap()))
}

#[tokio::test(start_paused = true)]
async fn metadata_cache_preserves_origin_time_policy_scope_and_bounded_state() -> Result<()> {
    let (descriptor, report, _) = fixture()?;
    let mut support = parse_reply(&serde_json::to_vec(&report)?, 7, &descriptor)??;
    let now = observation_utc()?;
    support.valid_until = Some(now + 3600);
    let policy = Digest::of(b"synthetic cache policy");
    let (owner, protection) = watch::channel(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(3600)),
    });
    let mut value = MetadataObservation {
        descriptor,
        support,
        issuer_policy: policy,
        observed: Instant::now(),
        observed_utc: now,
        protection,
    };
    let mut cache = ReceiptCache::default();
    cache.insert(&value)?;
    let input = CheckedDescriptor::new(&value.descriptor)?;
    assert!(
        cache
            .get(&input, Digest::of(b"different issuer policy"))
            .is_none()
    );
    tokio::time::advance(Duration::from_secs(299)).await;
    let read = cache.get(&input, policy).unwrap();
    assert_eq!(read.observed, value.observed);
    assert_eq!(read.observed_utc, value.observed_utc);
    assert!(read.valid_for() <= Duration::from_secs(1));
    assert!(
        cache
            .get_with_validity(&input, policy, Duration::from_secs(60))
            .is_none(),
        "long transfers need a new native check, not a re-timestamped receipt"
    );
    assert_eq!(cache.get(&input, policy).unwrap().observed, value.observed);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(cache.get(&input, policy).is_none());
    assert_eq!(cache.entries.len(), 0);
    value.observed = Instant::now();
    // Synthetic changed bodies exercise cache accounting only, not signature
    // admission. Production receipts have no mutation/deserialization path.
    for i in 0..MAX_CACHE_ENTRIES + 64 {
        value.descriptor.release.payload.extension_id = ExtensionId::new(
            &value.descriptor.publisher.payload.root_key,
            &format!("cache-{i}"),
        )?;
        value.descriptor.release.payload.description = format!("synthetic cache body {i}");
        cache.insert(&value)?;
        assert!(cache.entries.len() <= MAX_CACHE_ENTRIES);
        assert!(cache.entries.len() * super::super::receipt_fifo::ENTRY_BYTES <= MAX_CACHE_BYTES);
    }
    assert_eq!(cache.entries.len(), 10_000);
    let old = value.descriptor.clone();
    value.descriptor.release.payload.description = "replacement for the same extension".into();
    cache.insert(&value)?;
    assert_eq!(cache.entries.len(), 10_000);
    assert!(cache.get(&CheckedDescriptor::new(&old)?, policy).is_none());
    // Large bodies do not remain in this cache. Only the exact hash and fixed
    // receipt fields are retained, with independent original TTL/owner checks.
    assert!(
        std::mem::size_of::<CompactReceipt>() + std::mem::size_of::<Digest>() + 256
            <= super::super::receipt_fifo::ENTRY_BYTES
    );
    cache = ReceiptCache::default();
    let mut page = vec![];
    let mut wire = 0;
    for i in 0..elixir_marketplace::MAX_SEARCH_RESULTS {
        value.descriptor.release.payload.extension_id = ExtensionId::new(
            &value.descriptor.publisher.payload.root_key,
            &format!("page-{i}"),
        )?;
        value.descriptor.release.payload.description = format!("synthetic page item {i}");
        let bytes = value.descriptor.to_bytes()?;
        wire += bytes.len();
        page.push(value.descriptor.clone());
        cache.insert(&value)?;
    }
    assert!(wire < MAX_CONTROL_FRAME_BYTES);
    for descriptor in &page {
        let input = CheckedDescriptor::new(descriptor)?;
        assert!(cache.get(&input, policy).is_some());
        let mut changed = descriptor.clone();
        changed.release.payload.description.push('!');
        let changed_input = CheckedDescriptor::new(&changed)?;
        assert_ne!(input.id(), changed_input.id());
        assert!(
            cache.get(&changed_input, policy).is_none(),
            "a changed body cannot reuse a receipt for the same extension"
        );
        changed.gate_evidence.pop();
        assert!(CheckedDescriptor::new(&changed).is_err());
    }
    drop(owner);
    for descriptor in page {
        assert!(
            cache
                .get(&CheckedDescriptor::new(&descriptor)?, policy)
                .is_none()
        );
    }
    assert_eq!(cache.entries.len(), 0);
    Ok(())
}

#[test]
fn randomized_native_requests_stay_in_the_exact_json_integer_range() -> Result<()> {
    let (descriptor, _, _) = fixture()?;
    for _ in 0..128 {
        let (id, bytes) = descriptor_request(&descriptor)?;
        assert!((1..=(1u64 << 53) - 1).contains(&id));
        assert!(bytes.len() <= MAX_CONTROL_FRAME_BYTES);
        let request = VerificationRequest::parse(&bytes)?;
        assert_eq!(request.request_id(), id);
        assert!(matches!(
            request,
            VerificationRequest::VerifyDescriptor { .. }
        ));
    }
    Ok(())
}

#[test]
fn metadata_report_parser_binds_exact_input_and_never_trusts_an_advertised_total() -> Result<()> {
    let (descriptor, report, _) = fixture()?;
    let support = parse_reply(&serde_json::to_vec(&report)?, 7, &descriptor)??;
    assert_eq!(support.verified_supporters, 5);
    assert!(!support.is_global_total);
    for pointer in [
        "/request_id",
        "/schema_version",
        "/outcome/publisher_sequence",
        "/outcome/head_sequence",
        "/outcome/support/verified_supporters",
        "/outcome/support/weighted_score",
        "/outcome/support/valid_until",
    ] {
        let mut changed = report.clone();
        let value = changed.pointer_mut(pointer).unwrap();
        *value = json!(value.as_u64().unwrap() + 1);
        assert!(
            parse_reply(&serde_json::to_vec(&changed)?, 7, &descriptor).is_err(),
            "{pointer}"
        );
    }
    for pointer in [
        "/outcome/metadata_only",
        "/outcome/stable",
        "/outcome/support/discovery_qualified",
        "/outcome/support/capped",
        "/outcome/support/is_global_total",
    ] {
        let mut changed = report.clone();
        let value = changed.pointer_mut(pointer).unwrap();
        *value = json!(!value.as_bool().unwrap());
        assert!(
            parse_reply(&serde_json::to_vec(&changed)?, 7, &descriptor).is_err(),
            "{pointer}"
        );
    }
    for field in ["release_id", "publisher_state_id"] {
        let mut changed = report.clone();
        changed["outcome"][field] = json!(Digest::of(b"wrong scoped input"));
        assert!(parse_reply(&serde_json::to_vec(&changed)?, 7, &descriptor).is_err());
    }
    let mut changed = report.clone();
    changed["unrequested"] = json!(true);
    assert!(parse_reply(&serde_json::to_vec(&changed)?, 7, &descriptor).is_err());
    let duplicate = serde_json::to_string(&report)?.replacen('{', "{\"schema_version\":1,", 1);
    assert!(parse_reply(duplicate.as_bytes(), 7, &descriptor).is_err());
    assert!(parse_reply(&vec![b' '; MAX_REPLY + 1], 7, &descriptor).is_err());
    for outcome in [
        json!({"type":"issuer_policy","fingerprint":Digest::of(b"policy")}),
        json!({"type":"rejected","code":"attacker text"}),
    ] {
        let reply = json!({"schema_version":1,"request_id":7,"outcome":outcome});
        assert!(parse_reply(&serde_json::to_vec(&reply)?, 7, &descriptor).is_err());
    }
    let rejected = json!({"schema_version":1,"request_id":7,
        "outcome":{"type":"rejected","code":"signature"}});
    assert!(parse_reply(&serde_json::to_vec(&rejected)?, 7, &descriptor)?.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn native_origin_receipt_still_expires_and_dies_with_its_protection_owner() -> Result<()> {
    let (descriptor, report, _) = fixture()?;
    let mut support = parse_reply(&serde_json::to_vec(&report)?, 7, &descriptor)??;
    let now = chrono::Utc::now().timestamp() as u64;
    support.valid_until = Some(now + 3600);
    let (owner, protection) = watch::channel(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(600)),
    });
    // Synthetic test owner only, not evidence of real private-process origin.
    let mut observation = MetadataObservation {
        descriptor,
        support,
        issuer_policy: Digest::of(b"test-only-policy"),
        observed: Instant::now(),
        observed_utc: now,
        protection,
    };
    assert!(observation.support().is_ok());
    observation.observed_utc = now + 3600;
    assert!(
        !observation.is_current(),
        "clock rollback must not extend validity"
    );
    observation.observed_utc = now - 301;
    assert!(
        !observation.is_current(),
        "wall-clock age must expire a suspended monotonic clock"
    );
    observation.observed_utc = now;
    observation.support.valid_until = Some(now);
    assert!(observation.support().is_err());
    observation.support.valid_until = Some(now + 3600);
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(!observation.is_current());
    observation.observed = Instant::now();
    owner.send_replace(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(600)),
    });
    assert!(observation.is_current());
    drop(owner);
    assert!(observation.support().is_err());
    Ok(())
}

#[tokio::test]
async fn native_metadata_to_source_bridge_requires_a_current_metadata_owner_even_on_cache_hits()
-> Result<()> {
    let (descriptor, report, _) = fixture()?;
    let mut support = parse_reply(&serde_json::to_vec(&report)?, 7, &descriptor)??;
    let now = chrono::Utc::now().timestamp() as u64;
    support.valid_until = Some(now + 3600);
    let fixture = super::super::tests::fixture()?;
    // Synthetic receipt/availability for this scheduling test only. Actual
    // private-process origin and public fetching are separate native suites.
    let mut metadata = MetadataObservation {
        descriptor,
        support,
        issuer_policy: IssuerPolicy::new(fixture.owner.binding.issuer_keys.iter().copied())?
            .fingerprint(),
        observed: Instant::now(),
        observed_utc: now,
        protection: fixture.owner.protection.clone(),
    };
    let plan = SourcePreflightRequest::for_release(
        &metadata.descriptor.release.payload,
        0,
        Digest::of(b"test metadata source bridge"),
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
    let cached = client.request_metadata(&metadata, 0).await?;
    assert_eq!(cached.release_id(), plan.release_id);
    assert_eq!(cached.observed, source.observed);
    let policy = metadata.issuer_policy;
    metadata.issuer_policy = Digest::of(b"different current issuer policy");
    assert!(client.request_metadata(&metadata, 0).await.is_err());
    metadata.issuer_policy = policy;
    metadata.support.valid_until = Some(now);
    assert!(client.request_metadata(&metadata, 0).await.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn search_fact_hits_do_not_create_an_install_source_capability() -> Result<()> {
    let (descriptor, report, _) = fixture()?;
    let f = super::super::tests::fixture()?;
    let mut support = parse_reply(&serde_json::to_vec(&report)?, 7, &descriptor)??;
    support.valid_until = Some(observation_utc()? + 3600);
    let metadata = MetadataObservation {
        descriptor,
        support,
        issuer_policy: IssuerPolicy::new(f.owner.binding.issuer_keys.iter().copied())?
            .fingerprint(),
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection: f.owner.protection.clone(),
    };
    let plan = SourcePreflightRequest::for_release(
        &metadata.descriptor.release.payload,
        0,
        Digest::of(b"search separation test"),
        ProbeFamily::Ipv4,
    )?
    .plan;
    let native = PublicSourceObservation {
        release_id: plan.release_id,
        location_id: plan.location_id,
        license_sha256: Digest::of(b"MIT"),
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection: f.owner.protection.clone(),
    };
    f.owner.source_cache.lock().unwrap().insert(&native);
    let client = Client(f.owner.clone());
    assert!(client.cached_search_metadata_source(&metadata, 0)?.is_none());
    let fact = client.request_search_metadata(&metadata, 0).await?;
    assert_eq!(fact.release_id(), native.release_id());
    f.owner
        .source_cache
        .lock()
        .unwrap()
        .entries_for_test_clear();
    let permit = f.owner.slots.clone().try_acquire_owned()?;
    assert_eq!(
        client
            .cached_search_metadata_source(&metadata, 0)?
            .unwrap()
            .release_id(),
        native.release_id()
    );
    assert!(
        client
            .cached_search_metadata_source(&metadata, usize::MAX)
            .is_err()
    );
    assert!(
        client.request_search_metadata(&metadata, 0).await.is_ok(),
        "a local fact needs no helper slot"
    );
    assert!(
        client.request_fresh_metadata(&metadata, 0).await.is_err(),
        "an install must acquire its actual private helper even with a warm search fact"
    );
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(native.valid_for().is_zero());
    // Synthetic healthy-owner/metadata renewal only. Neither renewal changes
    // the original source-check timestamps or grants a new native source fact.
    f.protection.send_modify(|snapshot| {
        snapshot.until = Some(Instant::now() + Duration::from_secs(15));
    });
    f.facts.send_modify(|facts| {
        facts.as_mut().unwrap().observed = Instant::now();
    });
    assert!(
        client.request_search_metadata(&metadata, 0).await.is_err(),
        "a source cache hit cannot renew expired metadata or ranking"
    );
    assert!(client.cached_search_metadata_source(&metadata, 0).is_err());
    let mut current = metadata.retained_copy();
    current.observed = Instant::now();
    current.observed_utc = observation_utc()?;
    let cached = client.request_search_metadata(&current, 0).await?;
    assert!(cached.valid_for() > Duration::from_secs(23 * 60 * 60));
    let error = client
        .request_fresh_metadata(&current, 0)
        .await
        .err()
        .context("install unexpectedly reused a search-only fact")?;
    assert!(
        error.to_string().contains("source preflight slot is busy"),
        "install must reacquire a fresh helper, not reuse the day-long search fact: {error:#}"
    );
    drop(permit);
    Ok(())
}

#[tokio::test]
async fn compact_metadata_hit_needs_no_new_process_or_headroom_but_keeps_its_owner() -> Result<()> {
    let (descriptor, report, _) = fixture()?;
    let f = super::super::tests::fixture()?;
    let mut support = parse_reply(&serde_json::to_vec(&report)?, 7, &descriptor)??;
    support.valid_until = Some(observation_utc()? + 3600);
    let metadata = MetadataObservation {
        descriptor,
        support,
        issuer_policy: IssuerPolicy::new(f.owner.binding.issuer_keys.iter().copied())?
            .fingerprint(),
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection: f.owner.protection.clone(),
    };
    f.owner.metadata_cache.lock().unwrap().insert(&metadata)?;
    f.facts.send_modify(|facts| {
        let facts = facts.as_mut().unwrap();
        facts.available_memory = Some(1);
        facts.resource_pressure = true;
        facts.foreground_resource_pressure = true;
    });
    let client = Client(f.owner.clone());
    let cached = client.verify_metadata(&metadata.descriptor).await?;
    assert_eq!(cached.observed, metadata.observed);
    assert_eq!(cached.observed_utc, metadata.observed_utc);
    f.owner.playback.set_test_startup_queue_len(1);
    let cached = client.verify_metadata(&metadata.descriptor).await?;
    assert_eq!(cached.observed, metadata.observed);
    assert_eq!(cached.observed_utc, metadata.observed_utc);
    let mut cold = metadata.descriptor.clone();
    cold.head.payload.sequence += 1;
    let error = client
        .verify_metadata(&cold)
        .await
        .err()
        .context("new helper ignored playback startup")?;
    f.owner.playback.set_test_startup_queue_len(0);
    assert!(error.to_string().contains("playback startup"));
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Failed,
        until: None,
    });
    assert!(client.verify_metadata(&metadata.descriptor).await.is_err());
    Ok(())
}
