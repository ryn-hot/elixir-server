//! Real local signature checks + synthetic source/protection owners. This is
//! not native process, public network/source or installed-runtime evidence.
use super::*;
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    evidence::{IssuerPolicy, SupportSet},
    identity::PublicKey,
    records::{SignatureCache, SignedRecord},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
mod deferral;
mod ranking_ready;
mod selection;
mod view_progress;

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}
pub(super) fn descriptor(index: usize, weight: u8) -> Result<CandidateDescriptor> {
    let value: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let mut d = CandidateDescriptor::parse(&serde_json::to_vec(
        &serde_json::json!({"schema_version":1,
        "publisher":value["publisher"],"release":value["release"],"head":value["head"],"revocations":[],
        "gate_evidence":value["evidence"]["pairs"]}),
    )?)?;
    let id = ExtensionId::new(&d.publisher.payload.root_key, &format!("search-{index}"))?;
    d.release.payload.extension_id = id.clone();
    d.release.payload.display_name = format!("Example {index}");
    d.release = SignedRecord::sign(
        d.release.payload.clone(),
        d.release.authority_record_id,
        &key(2),
    )?;
    let release = Digest::of(canonical::to_vec(&d.release.payload)?);
    d.head.payload.extension_id = id.clone();
    d.head.payload.stable_release_id = release;
    d.head = SignedRecord::sign(d.head.payload.clone(), d.head.authority_record_id, &key(2))?;
    let now = chrono::Utc::now().timestamp() as u64;
    for (index, pair) in d.gate_evidence.iter_mut().enumerate() {
        let mut credential = pair.credential.payload.clone();
        credential.extension_id = id.clone();
        credential.tenure_weight = weight;
        credential.issued_at = now;
        credential.expires_at = now + 3600;
        pair.credential = SignedRecord::sign(credential, None, &key(3))?;
        let mut endorsement = pair.endorsement.payload.clone();
        endorsement.extension_id = id.clone();
        endorsement.release_id = release;
        endorsement.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(endorsement, None, &key(10 + index as u8))?;
    }
    Ok(d)
}
pub(super) struct Fake {
    page: SearchPage,
    owners: Mutex<Vec<Box<dyn Fn(bool) + Send + Sync>>>,
    current: AtomicBool,
    calls: AtomicUsize,
    hold_first: AtomicBool,
    started: Notify,
    resume: Notify,
    queries: Mutex<Vec<String>>,
    current_calls: AtomicUsize,
    hold_current: AtomicBool,
    current_started: Notify,
    current_resume: Notify,
    delay_second_current: AtomicBool,
    panic_page: AtomicBool,
    rank: AtomicBool,
    multi_peer: AtomicBool,
    scores: Mutex<std::collections::BTreeMap<ExtensionId, SupportSet>>,
    evidence_calls: Mutex<Vec<(ExtensionId, Option<String>)>>,
    repeat_cursor: AtomicBool,
    hold_catalog_hints: AtomicBool,
    hints_started: Notify,
    hints_resume: Notify,
    catalog_hints: Mutex<Vec<PeerKey>>,
}
impl Fake {
    pub(super) fn new(descriptors: Vec<CandidateDescriptor>) -> Self {
        Self {
            page: SearchPage {
                schema_version: 1,
                snapshot_id: Digest::of(b"test page"),
                results: descriptors
                    .into_iter()
                    .enumerate()
                    .map(
                        |(index, descriptor)| elixir_marketplace::catalog::SearchResult {
                            descriptor,
                            retained_support: SupportSummary {
                                verified_supporters: if index == 0 { 999_999 } else { 0 },
                                weighted_score: if index == 0 { 999_999 } else { 0 },
                                discovery_qualified: true,
                                capped: false,
                                is_global_total: false,
                                valid_until: None,
                            },
                        },
                    )
                    .collect(),
                next_cursor: None,
                network_complete: true,
                metadata_only: true,
            },
            owners: Mutex::new(vec![]),
            current: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            hold_first: AtomicBool::new(false),
            started: Notify::new(),
            resume: Notify::new(),
            queries: Mutex::new(vec![]),
            current_calls: AtomicUsize::new(0),
            hold_current: AtomicBool::new(false),
            current_started: Notify::new(),
            current_resume: Notify::new(),
            delay_second_current: AtomicBool::new(false),
            panic_page: AtomicBool::new(false),
            rank: AtomicBool::new(false),
            multi_peer: AtomicBool::new(false),
            scores: Mutex::new(std::collections::BTreeMap::new()),
            evidence_calls: Mutex::new(vec![]),
            repeat_cursor: AtomicBool::new(false),
            hold_catalog_hints: AtomicBool::new(false),
            hints_started: Notify::new(),
            hints_resume: Notify::new(),
            catalog_hints: Mutex::new(vec![]),
        }
    }
}
#[async_trait]
impl Backend for Fake {
    async fn page(&self, request: &SearchRequest, _interest: &Interest) -> Result<SearchPage> {
        assert!(
            !self.panic_page.swap(false, Ordering::SeqCst),
            "synthetic page panic"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.queries
            .lock()
            .unwrap()
            .push(request.query.text.clone());
        if self.hold_first.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.resume.notified().await;
        }
        Ok(self.page.clone())
    }
    async fn admit(
        &self,
        descriptor: CandidateDescriptor,
        _interest: &Interest,
    ) -> Result<Arc<Verified>> {
        let policy =
            IssuerPolicy::new([PublicKey::from_bytes(key(3).verifying_key().to_bytes())?])?;
        descriptor.verify_metadata(
            chrono::Utc::now().timestamp() as u64,
            &policy,
            &mut SignatureCache::new(128)?,
        )?;
        let (metadata, owner) =
            MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
        let release = Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?);
        let (source, source_owner) = PublicSourceObservation::fixture_for_source(
            release,
            Digest::of(b"SPDX-License-Identifier: MIT\n"),
        );
        if self.rank.load(Ordering::SeqCst) {
            let mut scores = self.scores.lock().unwrap();
            let id = metadata.descriptor().release.payload.extension_id.clone();
            let set = scores
                .entry(id.clone())
                .or_insert_with(|| SupportSet::new(id.clone(), release));
            for pair in &metadata.descriptor().gate_evidence {
                set.insert(
                    pair.verify(
                        &id,
                        release,
                        chrono::Utc::now().timestamp() as u64,
                        &policy,
                        &mut SignatureCache::new(128)?,
                    )?,
                    chrono::Utc::now().timestamp() as u64,
                    &policy,
                )?;
            }
        }
        self.owners.lock().unwrap().extend([
            Box::new(owner) as Box<dyn Fn(bool) + Send + Sync>,
            Box::new(source_owner),
        ]);
        Ok(Arc::new(Verified {
            metadata,
            source: SearchSourceObservation::from_source(&source)?,
        }))
    }
    async fn current(&self, _: &Verified) -> Result<()> {
        if self.hold_current.swap(false, Ordering::SeqCst) {
            self.current_started.notify_one();
            self.current_resume.notified().await;
        }
        if self.delay_second_current.load(Ordering::SeqCst)
            && self.current_calls.fetch_add(1, Ordering::SeqCst) == 1
        {
            tokio::time::sleep(Duration::from_secs(301)).await;
        }
        if !self.current.load(Ordering::SeqCst) {
            bail!("synthetic retained fence advanced");
        }
        Ok(())
    }
    async fn catalog_peers(&self, _: &Verified) -> Result<Vec<PeerKey>> {
        if self.hold_catalog_hints.swap(false, Ordering::SeqCst) {
            self.hints_started.notify_one();
            self.hints_resume.notified().await;
        }
        Ok(self.catalog_hints.lock().unwrap().clone())
    }
    async fn support(&self, value: &Verified) -> Result<SupportSummary> {
        self.current(value).await?;
        if !self.rank.load(Ordering::SeqCst) {
            return Ok(value.metadata.support()?.clone());
        }
        let policy =
            IssuerPolicy::new([PublicKey::from_bytes(key(3).verifying_key().to_bytes())?])?;
        Ok(self
            .scores
            .lock()
            .unwrap()
            .get_mut(&value.metadata.descriptor().release.payload.extension_id)
            .unwrap()
            .summary(chrono::Utc::now().timestamp() as u64, &policy))
    }
    async fn evidence(
        &self,
        value: &Verified,
        step: &EvidenceStep,
        _interest: &Interest,
    ) -> Result<Option<EvidenceObservation>> {
        if !self.rank.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let id = value
            .metadata
            .descriptor()
            .release
            .payload
            .extension_id
            .clone();
        self.evidence_calls
            .lock()
            .unwrap()
            .push((id.clone(), step.cursor.clone()));
        let policy =
            IssuerPolicy::new([PublicKey::from_bytes(key(3).verifying_key().to_bytes())?])?;
        // Give the initially lower-scored first release enough independently
        // signed distinct subjects to pass the higher gate-only release.
        let mut pairs = vec![];
        if id.to_string().ends_with("search-0") {
            for index in 0..64 {
                let mut pair = value.metadata.descriptor().gate_evidence[0].clone();
                pair.credential.payload.subject =
                    Digest::of(if self.multi_peer.load(Ordering::SeqCst) {
                        format!("search-extra-{:?}-{index}", step.origin)
                    } else {
                        format!("search-extra-{index}")
                    });
                pair.credential =
                    SignedRecord::sign(pair.credential.payload.clone(), None, &key(3))?;
                pair.endorsement.payload.credential_id = Digest::of(pair.credential.to_bytes()?);
                pair.endorsement =
                    SignedRecord::sign(pair.endorsement.payload.clone(), None, &key(10))?;
                pairs.push(pair);
            }
        }
        let batch = EvidenceBatch {
            schema_version: 1,
            release_id: value.source.release_id(),
            snapshot_id: if self.multi_peer.load(Ordering::SeqCst) {
                Digest::of(format!("snapshot-{:?}", step.origin))
            } else {
                Digest::of(b"search ranking snapshot")
            },
            pairs,
            next_cursor: if self.repeat_cursor.load(Ordering::SeqCst) {
                Some("repeat".into())
            } else {
                None
            },
        };
        let (observation, owner) =
            EvidenceObservation::fixture_for_batch(&value.metadata, &batch, &policy)?;
        let now = chrono::Utc::now().timestamp() as u64;
        for pair in &batch.pairs {
            self.scores.lock().unwrap().get_mut(&id).unwrap().insert(
                pair.verify(
                    &id,
                    batch.release_id,
                    now,
                    &policy,
                    &mut SignatureCache::new(128)?,
                )?,
                now,
                &policy,
            )?;
        }
        self.owners.lock().unwrap().push(Box::new(owner));
        Ok(Some(observation))
    }
    async fn evidence_sources(&self, _: &Verified, _: &Interest) -> Result<Vec<EvidenceOrigin>> {
        if self.multi_peer.load(Ordering::SeqCst) {
            Ok(vec![
                EvidenceOrigin::Cache,
                EvidenceOrigin::Peer(PeerKey::try_from("1".repeat(64))?),
                EvidenceOrigin::Peer(PeerKey::try_from("2".repeat(64))?),
            ])
        } else {
            Ok(vec![EvidenceOrigin::Cache])
        }
    }
}
pub(super) fn request(text: &str) -> SearchRequest {
    SearchRequest {
        query: SearchQuery {
            text: text.into(),
            tags: vec![],
            limit: 20,
        },
        cursor: None,
        prepare_catalog: false,
    }
}
pub(super) async fn ready(service: &SearchService, id: Uuid) -> Result<SearchView> {
    timeout(Duration::from_secs(10), async {
        loop {
            let view = service.view(id).await?;
            if view.phase != SearchPhase::Checking {
                return Ok(view);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn only_verified_matching_unique_cards_appear_and_peer_popularity_is_ignored() -> Result<()> {
    let low = descriptor(0, 1)?;
    let high = descriptor(1, 4)?;
    let mut tampered = descriptor(2, 4)?;
    tampered.release.payload.description = "not signed".into();
    let backend = Arc::new(Fake::new(vec![low, high, tampered]));
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    let view = ready(&service, id).await?;
    assert_eq!(view.results.len(), 2);
    assert_eq!(view.results[0].name, "Example 1");
    assert_eq!(view.results[0].support.weighted_score, 20);
    assert_eq!(view.results[1].support.weighted_score, 5);
    assert_eq!(view.omitted, 1);
    assert!(!view.network_complete);
    assert!(view.ranking_partial);
    assert!(
        view.results
            .iter()
            .all(|card| card.metadata_only && !card.support.is_global_total)
    );
    backend.current.store(false, Ordering::SeqCst);
    let view = service.view(id).await?;
    assert!(view.results.is_empty());
    assert!(view.phase == SearchPhase::Stale);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn pending_candidates_never_become_cards_or_selections_without_the_final_current_check()
-> Result<()> {
    let d = descriptor(0, 1)?;
    let extension = d.release.payload.extension_id.clone();
    let release = Digest::of(canonical::to_vec(&d.release.payload)?);
    let backend = Arc::new(Fake::new(vec![d]));
    backend.current.store(false, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    service.join().await?;
    {
        let slot = service.inner.slot.lock().unwrap();
        let query = slot.query.as_ref().unwrap();
        assert_eq!(query.phase, SearchPhase::Ready);
        assert_eq!(
            query.values.len(),
            1,
            "pending native/source fact is not a card"
        );
    }
    let view = service.view(id).await?;
    assert!(view.results.is_empty());
    assert_eq!(view.phase, SearchPhase::Stale);
    assert!(
        service
            .selected(id, &extension, release, |_| true)
            .await
            .is_err()
    );
    assert_eq!(service.selections.available_permits(), 1);
    backend.current.store(true, Ordering::SeqCst);
    assert_eq!(service.view(id).await?.results.len(), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_first_proofs_cannot_hide_an_independently_valid_candidate_for_the_same_id()
-> Result<()> {
    let valid = descriptor(0, 1)?;
    let mut forged = valid.clone();
    forged.gate_evidence[0].credential.payload.tenure_weight = 2;
    let backend = Arc::new(Fake::new(vec![forged, valid]));
    let service = SearchService::default();
    let id = service.start_with(backend, request("example"))?;
    let view = ready(&service, id).await?;
    assert_eq!(view.results.len(), 1);
    assert_eq!(view.omitted, 1);
    assert_eq!(view.results[0].support.verified_supporters, 5);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn rapid_query_changes_keep_only_one_active_and_the_latest_pending_request() -> Result<()> {
    let backend = Arc::new(Fake::new(vec![]));
    backend.hold_first.store(true, Ordering::SeqCst);
    let service = SearchService::default();
    let old = service.start_with(backend.clone(), request("first"))?;
    backend.started.notified().await;
    let mut latest = old;
    for i in 0..100 {
        latest = service.start_with(backend.clone(), request(&format!("latest {i}")))?;
    }
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert!(service.view(old).await.is_err());
    backend.resume.notify_one();
    ready(&service, latest).await?;
    assert_eq!(
        backend.queries.lock().unwrap().as_slice(),
        &["first", "latest 99"]
    );
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn expired_receipts_are_removed_and_stop_cannot_return_a_previous_query() -> Result<()> {
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?]));
    let service = SearchService::default();
    let id = service.start_with(backend, request("example"))?;
    assert_eq!(ready(&service, id).await?.results.len(), 1);
    tokio::time::advance(Duration::from_secs(301)).await;
    let view = service.view(id).await?;
    assert!(view.results.is_empty());
    assert!(view.phase == SearchPhase::Stale);
    service.request_stop();
    assert!(service.view(id).await.is_err());
    service.join().await?;
    Ok(())
}
#[test]
fn search_request_rejects_unknown_fields_mixed_live_filters_and_oversized_cursors() {
    for input in [
        serde_json::json!({"query":{"text":"x","tags":["live","movies"],"limit":20}}),
        serde_json::json!({"query":{"text":"x","tags":[],"limit":21}}),
        serde_json::json!({"query":{"text":"x","tags":[],"limit":20},"trusted":true}),
        serde_json::json!({"query":{"text":"x","tags":[],"limit":20},"cursor":"x".repeat(129)}),
    ] {
        assert!(SearchRequest::parse(&serde_json::to_vec(&input).unwrap()).is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn receipt_expiring_during_later_security_check_never_becomes_a_card() -> Result<()> {
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?, descriptor(1, 1)?]));
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    assert_eq!(ready(&service, id).await?.results.len(), 2);
    backend.current_calls.store(0, Ordering::SeqCst);
    backend.delay_second_current.store(true, Ordering::SeqCst);
    let view = service.view(id).await?;
    assert!(view.results.is_empty());
    assert!(view.phase == SearchPhase::Stale);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn failed_task_does_not_leave_search_stuck_or_restore_any_previous_cards() -> Result<()> {
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?]));
    let service = SearchService::default();
    backend.panic_page.store(true, Ordering::SeqCst);
    let id = service.start_with(backend.clone(), request("example"))?;
    let failed = ready(&service, id).await?;
    assert!(failed.phase == SearchPhase::Unavailable);
    assert!(failed.results.is_empty());
    let next = service.start_with(backend, request("example"))?;
    assert!(service.view(id).await.is_err());
    assert_eq!(ready(&service, next).await?.results.len(), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn independently_verified_extra_support_changes_order_and_repeated_pages_stop() -> Result<()>
{
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?, descriptor(1, 4)?]));
    backend.rank.store(true, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    ready(&service, id).await?;
    service.join().await?; // Page readiness does not mean ranking convergence.
    let view = service.view(id).await?;
    assert_eq!(view.results[0].name, "Example 0");
    assert_eq!(view.results[0].support.verified_supporters, 69);
    assert_eq!(view.results[0].support.weighted_score, 69);
    assert_eq!(backend.evidence_calls.lock().unwrap().len(), 2);
    assert!(
        view.results
            .iter()
            .all(|r| !r.support.is_global_total && r.ranking_partial)
    );
    service.request_stop();
    service.join().await?;
    backend.evidence_calls.lock().unwrap().clear();
    backend.repeat_cursor.store(true, Ordering::SeqCst);
    let id = service.start_with(backend.clone(), request("example 0"))?;
    ready(&service, id).await?;
    service.join().await?;
    assert_eq!(
        backend.evidence_calls.lock().unwrap().len(),
        2,
        "a repeated next cursor must stop, not make64 requests"
    );
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn independent_evidence_origins_merge_verified_subjects_without_trusting_shared_snapshot_ids()
-> Result<()> {
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?]));
    backend.rank.store(true, Ordering::SeqCst);
    backend.multi_peer.store(true, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    ready(&service, id).await?;
    service.join().await?;
    let view = service.view(id).await?;
    assert_eq!(view.results.len(), 1);
    assert_eq!(view.results[0].support.verified_supporters, 5 + 3 * 64);
    assert_eq!(view.results[0].support.weighted_score, 5 + 3 * 64);
    assert_eq!(backend.evidence_calls.lock().unwrap().len(), 3);
    assert!(!view.network_complete && view.ranking_partial);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn ranking_pass_caps_all_work_at_64_batches_and_4096_pairs() -> Result<()> {
    let backend = Arc::new(Fake::new(vec![]));
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("unused"))?;
    ready(&service, id).await?;
    let value = backend
        .admit(
            descriptor(0, 1)?,
            &Interest {
                inner: Arc::downgrade(&service.inner),
                id,
            },
        )
        .await?;
    let policy = IssuerPolicy::new([PublicKey::from_bytes(key(3).verifying_key().to_bytes())?])?;
    let mut pass = RankingPass::new(value.clone());
    for page in 0..64 {
        assert!(pass.step().is_some());
        let batch = EvidenceBatch {
            schema_version: 1,
            release_id: value.source.release_id(),
            snapshot_id: Digest::of(b"one snapshot"),
            pairs: (0..64)
                .map(|i| value.metadata.descriptor().gate_evidence[i % 5].clone())
                .collect(),
            next_cursor: Some(format!("page_{}", page + 1)),
        };
        let (observation, _owner) =
            EvidenceObservation::fixture_for_batch(&value.metadata, &batch, &policy)?;
        pass.accept(&observation)?;
        assert_eq!(pass.done, page == 63);
    }
    assert_eq!(pass.pairs, 4096);
    assert_eq!(pass.batches, 64);
    let batch = EvidenceBatch {
        schema_version: 1,
        release_id: value.source.release_id(),
        snapshot_id: Digest::of(b"one snapshot"),
        pairs: value.metadata.descriptor().gate_evidence.clone(),
        next_cursor: None,
    };
    let (observation, _owner) =
        EvidenceObservation::fixture_for_batch(&value.metadata, &batch, &policy)?;
    assert!(pass.accept(&observation).is_err());
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn ranking_cursors_snapshots_and_failures_are_per_origin_but_attempt_and_pair_caps_are_shared()
-> Result<()> {
    let backend = Arc::new(Fake::new(vec![]));
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("unused"))?;
    ready(&service, id).await?;
    let value = backend
        .admit(
            descriptor(0, 1)?,
            &Interest {
                inner: Arc::downgrade(&service.inner),
                id,
            },
        )
        .await?;
    let policy = IssuerPolicy::new([PublicKey::from_bytes(key(3).verifying_key().to_bytes())?])?;
    let peer1 = EvidenceOrigin::Peer(PeerKey::try_from("1".repeat(64))?);
    let peer2 = EvidenceOrigin::Peer(PeerKey::try_from("2".repeat(64))?);
    let sources = vec![EvidenceOrigin::Cache, peer1.clone(), peer2.clone()];
    let mut pass = RankingPass::with_sources(value.clone(), sources.clone())?;
    for n in 0..4 {
        let step = pass.step().unwrap();
        assert_eq!(
            step.origin,
            if n < 2 {
                EvidenceOrigin::Cache
            } else {
                peer1.clone()
            }
        );
        if n == 0 || n == 2 {
            assert!(step.cursor.is_none() && step.snapshot.is_none());
        } else {
            assert_eq!(step.cursor.as_deref(), Some("same_cursor"));
        }
        assert!(
            pass.step().is_none(),
            "one request may be active for a release"
        );
        let batch = EvidenceBatch {
            schema_version: 1,
            release_id: value.source.release_id(),
            snapshot_id: Digest::of(if n < 2 {
                format!("cache-{n}")
            } else {
                "peer-1".into()
            }),
            pairs: value.metadata.descriptor().gate_evidence.clone(),
            next_cursor: (n != 3).then(|| "same_cursor".into()),
        };
        let (observation, _owner) =
            EvidenceObservation::fixture_for_batch(&value.metadata, &batch, &policy)?;
        if n == 1 {
            assert!(pass.accept(&observation).is_err());
            assert!(!pass.done);
        } else {
            pass.accept(&observation)?;
        }
    }
    let step = pass.step().unwrap();
    assert_eq!(step.origin, peer2);
    assert!(step.cursor.is_none());
    pass.failed();
    assert!(pass.done);
    assert_eq!(pass.batches, 5);
    assert_eq!(
        pass.pairs, 20,
        "a rejected snapshot still consumes its examined pairs"
    );
    assert!(pass.step().is_none());
    let mut capped = RankingPass::with_sources(value.clone(), sources)?;
    for n in 0..64 {
        let step = capped.step().unwrap();
        assert_eq!(
            step.origin,
            if n < 20 {
                EvidenceOrigin::Cache
            } else if n < 40 {
                peer1.clone()
            } else {
                peer2.clone()
            }
        );
        let batch = EvidenceBatch {
            schema_version: 1,
            release_id: value.source.release_id(),
            snapshot_id: Digest::of(format!("{:?}", step.origin)),
            pairs: (0..64)
                .map(|i| value.metadata.descriptor().gate_evidence[i % 5].clone())
                .collect(),
            next_cursor: (!matches!(n, 19 | 39)).then(|| format!("cursor-{n}")),
        };
        let (observation, _owner) =
            EvidenceObservation::fixture_for_batch(&value.metadata, &batch, &policy)?;
        capped.accept(&observation)?;
    }
    assert!(capped.done && capped.step().is_none());
    assert_eq!(capped.batches, 64);
    assert_eq!(capped.pairs, 4096);
    assert!(RankingPass::with_sources(value.clone(), vec![EvidenceOrigin::Cache; 2]).is_err());
    assert!(
        RankingPass::with_sources(
            value,
            (1..=7)
                .map(|n| EvidenceOrigin::Peer(PeerKey::try_from(format!("{n:064x}")).unwrap()))
                .collect()
        )
        .is_err()
    );
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn cancelled_search_join_wait_retains_the_original_task_until_actual_completion() -> Result<()>
{
    let service = SearchService::default();
    service.inner.slot.lock().unwrap().task = Some(tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), service.join())
            .await
            .is_err()
    );
    assert!(service.retiring.lock().await.is_some());
    tokio::time::advance(Duration::from_secs(2)).await;
    service.join().await?;
    assert!(service.retiring.lock().await.is_none());
    Ok(())
}
