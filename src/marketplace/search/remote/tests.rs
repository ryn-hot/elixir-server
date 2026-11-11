//! Pagination/data-bound tests only. Synthetic candidates here are not native
//! source/adoption observations and do not establish display/install authority.
use super::*;
use std::collections::BTreeMap;
mod local_ready;
mod page_pressure;

struct Unused;
#[async_trait]
impl Backend for Unused {
    async fn page(&self, _: &SearchRequest, _: &Interest) -> Result<SearchPage> {
        bail!("unused")
    }
    async fn admit(&self, _: CandidateDescriptor, _: &Interest) -> Result<Arc<Verified>> {
        bail!("unused")
    }
    async fn current(&self, _: &Verified) -> Result<()> {
        bail!("unused")
    }
}
fn context(limit: usize) -> (Arc<Inner>, Interest, SearchRequest) {
    let inner = Arc::new(Inner::default());
    let id = Uuid::new_v4();
    let request = SearchRequest {
        query: SearchQuery {
            text: String::new(),
            tags: vec![],
            limit,
        },
        cursor: None,
        prepare_catalog: false,
    };
    inner.slot.lock().unwrap().query = Some(Query {
        id,
        request: request.clone(),
        backend: Arc::new(Unused),
        last_active: Instant::now(),
        phase: SearchPhase::Checking,
        total: 0,
        checked: 0,
        omitted: 0,
        next_cursor: None,
        values: vec![],
        preparation: None,
        preparation_until: None,
    });
    let interest = Interest {
        inner: Arc::downgrade(&inner),
        id,
    };
    (inner, interest, request)
}
fn candidate(n: usize) -> SearchResult {
    let fixture: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))
    .unwrap();
    let mut descriptor=CandidateDescriptor::parse(&serde_json::to_vec(&serde_json::json!({"schema_version":1,
        "publisher":fixture["publisher"],"release":fixture["release"],"head":fixture["head"],"revocations":[],
        "gate_evidence":fixture["evidence"]["pairs"]})).unwrap()).unwrap();
    // Intentionally unverified data. Real admission must reject the changed
    // signatures; the pagination layer has no authority to admit it.
    descriptor.release.payload.extension_id = ExtensionId::new(
        &descriptor.publisher.payload.root_key,
        &format!("remote-{n}"),
    )
    .unwrap();
    descriptor.release.payload.display_name = format!("Remote {n}");
    SearchResult {
        descriptor,
        retained_support: SupportSummary {
            verified_supporters: 5,
            weighted_score: 5,
            discovery_qualified: true,
            capped: false,
            is_global_total: false,
            valid_until: Some(2_000_000_000),
        },
    }
}
fn peer(n: usize) -> PeerKey {
    PeerKey::try_from(format!("{n:064x}")).unwrap()
}
#[tokio::test]
async fn update_lookup_queries_peers_even_when_the_local_catalog_has_a_usable_card() -> Result<()> {
    let (_inner, interest, mut request) = context(1);
    let old = candidate(1);
    let mut next = old.clone();
    // Raw pagination only: deliberately unverified; admission remains required.
    next.descriptor.release.payload.version = "2.0.0".into();
    request.query.text = old.descriptor.release.payload.extension_id.to_string();
    let mut transport = Mock::new();
    transport.pages.insert("local".into(), vec![vec![old]]);
    transport.catalog(1, vec![vec![next.clone()]]);
    let mut session = Session::for_update(DiscoveryProgress::default());
    let page = session.read(&transport, &request, &interest).await?;
    assert_eq!(
        page.results[0].descriptor.to_bytes()?,
        next.descriptor.to_bytes()?
    );
    assert!(session.discovered);
    assert!(
        session
            .sources
            .iter()
            .all(|s| matches!(s.location, Location::Peer(_)))
    );
    assert!(
        transport
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(name, _)| name != "local")
    );
    assert!(!page.network_complete);
    Ok(())
}
#[tokio::test]
async fn unavailable_update_peers_cannot_fall_back_to_a_local_card_and_claim_a_remote_check()
-> Result<()> {
    for failed in [false, true] {
        let (_inner, interest, request) = context(1);
        let mut transport = Mock::new();
        transport
            .pages
            .insert("local".into(), vec![vec![candidate(1)]]);
        if failed {
            transport.catalog(1, vec![vec![candidate(2)]]);
            transport.fail = true;
        }
        let mut session = Session::for_update(DiscoveryProgress::default());
        assert!(
            session
                .read(&transport, &request, &interest)
                .await?
                .results
                .is_empty()
        );
        assert!(
            transport
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|(name, _)| name != "local")
        );
        assert!(session.admitted.is_empty());
    }
    Ok(())
}
#[test]
fn known_local_queue_pressure_cannot_mark_the_pending_peer_as_invalid() -> Result<()> {
    let id = Digest::of(b"unverified pending hint");
    let mut session = Session::default();
    session
        .sources
        .push(Source::new(Location::Peer(peer(1)), true));
    session.emitted.insert(id, 0);
    let result = Err(ParticipationClient::fixture_queue_busy());
    let feedback = AdmissionFeedback::from_result(&result);
    assert_eq!(feedback, AdmissionFeedback::LocalUnavailable);
    session.admission_result(id, feedback)?;
    assert!(!session.sources[0].rejected);
    assert!(!session.sources[0].done);
    assert!(
        session.admitted.is_empty(),
        "pressure cannot admit a hint either"
    );
    Ok(())
}

#[test]
fn local_pending_is_exact_owned_provenance_not_a_peer_flag_or_retained_permission() -> Result<()> {
    let local = Digest::of(b"exact emitted local descriptor");
    let remote = Digest::of(b"exact emitted peer descriptor");
    let unknown = Digest::of(b"not emitted");
    let mut session = Session::default();
    session.sources = vec![
        Source::new(Location::Local, true),
        Source::new(Location::Peer(peer(1)), true),
    ];
    session.emitted.insert(local, 0);
    session.emitted.insert(remote, 1);
    assert!(session.local_pending(local));
    assert!(!session.local_pending(remote));
    assert!(!session.local_pending(unknown));
    session.sources[0].enabled = false;
    assert!(!session.local_pending(local));
    session.sources[0].enabled = true;
    session.sources[0].rejected = true;
    assert!(!session.local_pending(local));
    session.sources[0].rejected = false;
    session.admission_result(local, AdmissionFeedback::Accepted)?;
    assert!(
        !session.local_pending(local),
        "an admitted origin is not another pending operation"
    );
    assert!(!session.local_pending(remote));
    Ok(())
}
struct DelayedDiscovery {
    inner: Arc<Inner>,
    born: Instant,
    after: Duration,
    calls: std::sync::atomic::AtomicUsize,
    refreshes: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Discovery for DelayedDiscovery {
    async fn status(&self) -> Result<Vec<PeerKey>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Simulates a visible client's status polls, NOT an authority lease.
        if let Some(q) = self.inner.slot.lock().unwrap().query.as_mut() {
            q.last_active = Instant::now();
        }
        Ok(if self.born.elapsed() >= self.after {
            vec![peer(1)]
        } else {
            vec![]
        })
    }
    async fn refresh(&self) -> Result<()> {
        self.refreshes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
#[error("original private refresh rejection")]
struct RefreshRejected;
struct DeclinedDiscovery {
    statuses: std::sync::atomic::AtomicUsize,
    refreshes: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Discovery for DeclinedDiscovery {
    async fn status(&self) -> Result<Vec<PeerKey>> {
        self.statuses
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(vec![])
    }
    async fn refresh(&self) -> Result<()> {
        self.refreshes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(RefreshRejected.into())
    }
}
#[tokio::test(start_paused = true)]
async fn declined_refresh_preserves_original_error_without_polling_an_empty_success() -> Result<()>
{
    use std::sync::atomic::Ordering;
    let (_owner, interest, _) = context(20);
    let state = DeclinedDiscovery {
        statuses: 0.into(),
        refreshes: 0.into(),
    };
    let start = Instant::now();
    let error = discover_from(&state, &interest, None).await.unwrap_err();
    assert!(error.is::<RefreshRejected>());
    assert_eq!(state.statuses.load(Ordering::SeqCst), 1);
    assert_eq!(state.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
    Ok(())
}
struct AdmittedDiscovery {
    state: DelayedDiscovery,
    admission: Duration,
}
#[async_trait]
impl Discovery for AdmittedDiscovery {
    async fn status(&self) -> Result<Vec<PeerKey>> {
        self.state.status().await
    }
    async fn refresh(&self) -> Result<()> {
        self.state.refresh().await?;
        tokio::time::sleep(self.admission).await;
        Ok(())
    }
}
#[tokio::test(start_paused = true)]
async fn known_predispatch_wait_does_not_spend_or_renew_the_one_discovery_window() -> Result<()> {
    use std::sync::atomic::Ordering;
    for after in [110, 300] {
        let (inner, interest, _) = context(20);
        // Explicit setup interest, not status polls extending authority.
        inner
            .slot
            .lock()
            .unwrap()
            .query
            .as_mut()
            .unwrap()
            .preparation_until = Some(Instant::now() + Duration::from_secs(3600));
        let state = AdmittedDiscovery {
            state: DelayedDiscovery {
                inner,
                born: Instant::now(),
                after: Duration::from_secs(after),
                calls: 0.into(),
                refreshes: 0.into(),
            },
            admission: Duration::from_secs(72),
        };
        let peers = discover_from(&state, &interest, None).await?;
        let maximum = state.admission + ParticipationClient::DISCOVERY_WAIT;
        assert!(state.state.born.elapsed() <= maximum);
        assert_eq!(state.state.refreshes.load(Ordering::SeqCst), 1);
        assert!(state.state.calls.load(Ordering::SeqCst) <= 72);
        assert_eq!(peers, if after == 110 { vec![peer(1)] } else { vec![] });
        if after == 300 {
            assert_eq!(state.state.born.elapsed(), maximum);
        }
    }
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn one_discovery_attempt_covers_onion_dial_and_noise_but_still_has_a_fixed_end() -> Result<()>
{
    use std::sync::atomic::Ordering;
    // PeerNetwork: 20s query + 120s onion dial + 15s Noise. A search must not
    // report a terminal empty attempt while that original connection is valid.
    assert_eq!(
        ParticipationClient::DISCOVERY_WAIT,
        Duration::from_secs(165)
    );
    for after in [
        Duration::ZERO,
        Duration::from_secs(45),
        Duration::from_secs(120),
        Duration::from_secs(155),
        Duration::from_secs(300),
    ] {
        let (inner, interest, _) = context(20);
        let state = DelayedDiscovery {
            inner,
            born: Instant::now(),
            after,
            calls: 0.into(),
            refreshes: 0.into(),
        };
        let peers = discover_from(&state, &interest, None).await?;
        if after.is_zero() {
            assert_eq!(state.refreshes.load(Ordering::SeqCst), 0);
            assert_eq!(state.calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(state.refreshes.load(Ordering::SeqCst), 1);
        }
        assert!(state.born.elapsed() <= ParticipationClient::DISCOVERY_WAIT);
        assert!(
            state.calls.load(Ordering::SeqCst) <= 72,
            "status reads must back off, not busy-poll"
        );
        assert_eq!(
            peers,
            if after < ParticipationClient::DISCOVERY_WAIT {
                vec![peer(1)]
            } else {
                vec![]
            }
        );
    }
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn an_alternative_identity_never_completes_a_specific_peers_reconnect() -> Result<()> {
    let (inner, interest, _) = context(20);
    let state = DelayedDiscovery {
        inner,
        born: Instant::now(),
        after: Duration::ZERO,
        calls: 0.into(),
        refreshes: 0.into(),
    };
    let wanted = peer(2);
    let result = discover_from(&state, &interest, Some(&wanted)).await?;
    assert!(!result.contains(&wanted));
    assert_eq!(state.born.elapsed(), ParticipationClient::DISCOVERY_WAIT);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn discovery_replacement_cancels_the_read_waiter_before_another_status_request() -> Result<()>
{
    use std::sync::atomic::Ordering;
    let (inner, interest, _) = context(20);
    let state = Arc::new(DelayedDiscovery {
        inner: inner.clone(),
        born: Instant::now(),
        after: Duration::from_secs(45),
        calls: 0.into(),
        refreshes: 0.into(),
    });
    let copy = state.clone();
    let task = tokio::spawn(async move {
        while_interested(&interest, discover_from(copy.as_ref(), &interest, None)).await
    });
    while state.calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let calls = state.calls.load(Ordering::SeqCst);
    inner.slot.lock().unwrap().query = None;
    tokio::time::advance(Duration::from_millis(251)).await;
    assert!(task.await?.is_err());
    assert_eq!(state.calls.load(Ordering::SeqCst), calls);
    Ok(())
}
#[tokio::test]
async fn metadata_warming_only_copies_the_emitted_sources_remaining_bounded_page() -> Result<()> {
    let (_owner, interest, request) = context(20);
    let mut transport = Mock::new();
    transport
        .pages
        .insert("local".into(), vec![(0..20).map(candidate).collect()]);
    let mut session = Session::default();
    session.begin_page();
    let page = session
        .read_chunk(&transport, &request, &interest, 1)
        .await?;
    assert_eq!(page.results.len(), 1);
    let descriptor = &page.results[0].descriptor;
    let before = (
        session.requests,
        session.examined,
        session.seen.len(),
        session.emitted.len(),
    );
    let batch = session.metadata_batch(descriptor);
    assert_eq!(batch.len(), 20);
    assert_eq!(batch[0].to_bytes()?, descriptor.to_bytes()?);
    assert_eq!(session.sources[0].pending.len(), 19);
    assert!(!session.discovered);
    // The first attempt may have deferred before creating native metadata
    // receipts. Metadata selection must not consume source warming before that
    // awaited operation succeeds. Retry the SAME emitted candidate/page.
    assert!(session.source_warmed.is_empty());
    let retry_metadata = session.metadata_batch(descriptor);
    assert_eq!(retry_metadata.len(), 20);
    assert!(session.source_warmed.is_empty());
    let session = tokio::sync::Mutex::new(session);
    let deferred = prefetch_after_metadata(&session, descriptor, |rows| async move {
        assert_eq!(rows.len(), 20);
        bail!("synthetic native metadata resource deferral")
    })
    .await;
    assert!(deferred.is_err());
    {
        let session = session.lock().await;
        assert!(
            session.source_warmed.is_empty(),
            "failed metadata warming cannot spend the source attempt"
        );
        assert_eq!(session.sources[0].pending.len(), 19);
    }
    let source = prefetch_after_metadata(&session, descriptor, |rows| async move {
        assert_eq!(rows.len(), 20);
        Ok(())
    })
    .await?;
    let mut session = session.lock().await;
    assert_eq!(source.len(), 20);
    assert!(session.source_batch(descriptor)?.is_empty());
    for row in &batch {
        assert!(
            session.source_batch(row)?.is_empty(),
            "no repeated warming for another row from the same page"
        );
    }
    assert_eq!(session.source_warmed.len(), 20);
    assert_eq!(
        before,
        (
            session.requests,
            session.examined,
            session.seen.len(),
            session.emitted.len()
        )
    );
    // Unknown and already-resolved inputs cannot borrow another origin's page.
    assert_eq!(session.metadata_batch(&candidate(99).descriptor).len(), 1);
    session.admission_result(
        Digest::of(canonical::to_vec(descriptor)?),
        AdmissionFeedback::Rejected,
    )?;
    assert_eq!(session.metadata_batch(descriptor).len(), 1);
    Ok(())
}
#[test]
fn local_first_download_fallbacks_preserve_origins_deduplicate_and_cap() {
    assert_eq!(
        download_peers([], [peer(1), peer(2)]),
        vec![peer(1), peer(2)]
    );
    assert_eq!(
        download_peers([peer(3), peer(1)], (1..=10).map(peer)),
        vec![peer(3), peer(1), peer(2), peer(4), peer(5), peer(6)]
    );
}

#[tokio::test]
async fn superseded_query_cancels_only_the_read_only_waiter() -> Result<()> {
    let (inner, interest, _) = context(20);
    let started = Arc::new(tokio::sync::Notify::new());
    let observed = started.clone();
    let read = tokio::spawn(async move {
        while_interested(&interest, async {
            observed.notify_one();
            std::future::pending::<Result<()>>().await
        })
        .await
    });
    started.notified().await;
    inner.slot.lock().unwrap().query.as_mut().unwrap().id = Uuid::new_v4();
    assert!(timeout(Duration::from_secs(2), read).await??.is_err());
    // No interest must not even poll a ready operation with side effects.
    let (_, expired, _) = context(20);
    assert!(
        while_interested(&expired, async {
            panic!("obsolete read polled");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .is_err()
    );
    Ok(())
}
fn name(location: &Location) -> String {
    match location {
        Location::Local => "local".into(),
        Location::Peer(key) => key.to_string(),
    }
}
struct Mock {
    peers: Vec<PeerKey>,
    pages: BTreeMap<String, Vec<Vec<SearchResult>>>,
    calls: Mutex<Vec<(String, Option<String>)>>,
    repeated: bool,
    wrong_snapshot: bool,
    oversized: bool,
    fail: bool,
    discovery_failure: bool,
}
impl Mock {
    fn new() -> Self {
        Self {
            peers: vec![],
            pages: BTreeMap::new(),
            calls: Mutex::new(vec![]),
            repeated: false,
            wrong_snapshot: false,
            oversized: false,
            fail: false,
            discovery_failure: false,
        }
    }
    fn catalog(&mut self, n: usize, pages: Vec<Vec<SearchResult>>) {
        self.peers.push(peer(n));
        self.pages.insert(peer(n).to_string(), pages);
    }
}
#[async_trait]
impl Transport for Mock {
    async fn peers(&self, _: &Interest) -> Result<Vec<PeerKey>> {
        if self.discovery_failure {
            return Err(RefreshRejected.into());
        }
        Ok(self.peers.clone())
    }
    async fn fetch(
        &self,
        location: &Location,
        query: &SearchQuery,
        cursor: Option<String>,
        _: &Interest,
    ) -> Result<SearchPage> {
        let key = name(location);
        self.calls
            .lock()
            .unwrap()
            .push((key.clone(), cursor.clone()));
        if self.fail && matches!(location, Location::Peer(_)) {
            bail!("unavailable peer");
        }
        let index = if let Some(cursor) = &cursor {
            let expected = format!("{key}_");
            assert!(
                cursor.starts_with(&expected),
                "cursor must never be forwarded to a different source"
            );
            cursor[expected.len()..].parse::<usize>()?
        } else {
            0
        };
        let pages = self.pages.get(&key);
        let mut results = pages
            .and_then(|p| p.get(index))
            .cloned()
            .unwrap_or_default();
        assert!(results.len() <= query.limit);
        let mut next_cursor = pages
            .filter(|p| index + 1 < p.len())
            .map(|_| format!("{key}_{}", index + 1));
        if self.repeated && cursor.is_some() {
            next_cursor = cursor.clone();
        }
        if self.oversized {
            results = vec![candidate(0); 21];
        }
        Ok(SearchPage {
            schema_version: 1,
            snapshot_id: Digest::of(if self.wrong_snapshot && index > 0 {
                b"changed".as_slice()
            } else {
                b"stable".as_slice()
            }),
            results,
            next_cursor,
            network_complete: false,
            metadata_only: true,
        })
    }
}

#[tokio::test]
async fn session_keeps_discovery_failure_distinct_from_a_successful_empty_page() -> Result<()> {
    for remote_only in [false, true] {
        let (_owner, interest, request) = context(20);
        let mut transport = Mock::new();
        transport.discovery_failure = true;
        let mut session = if remote_only {
            Session::for_update(DiscoveryProgress::default())
        } else {
            Session::default()
        };
        let error = session
            .read(&transport, &request, &interest)
            .await
            .unwrap_err();
        assert!(error.is::<RefreshRejected>());
        assert!(!session.discovery_progress.waiting());
        assert!(
            session.token.is_none(),
            "failed work cannot acquire a continuation"
        );
        assert!(session.admitted.is_empty());
        assert_eq!(
            transport.calls.lock().unwrap().len(),
            usize::from(!remote_only)
        );
    }
    Ok(())
}

#[tokio::test]
async fn exact_identity_query_filters_copied_names_before_remote_admission() -> Result<()> {
    let (_owner, interest, mut request) = context(1);
    let mut target = candidate(91);
    target.descriptor.release.payload.extension_id = ExtensionId::new(
        &target.descriptor.publisher.payload.root_key,
        &format!("{}z", "a-".repeat(31)),
    )?;
    let identity = target.descriptor.release.payload.extension_id.clone();
    request.query.text = identity.to_string();
    let mut impostor = candidate(92);
    impostor.descriptor.release.payload.display_name = identity.to_string();
    impostor.retained_support.weighted_score = 20;
    let mut transport = Mock::new();
    transport.pages.insert("local".into(), vec![vec![impostor]]);
    transport.catalog(1, vec![vec![target]]);
    let page = Session::default()
        .read(&transport, &request, &interest)
        .await?;
    assert_eq!(page.results.len(), 1);
    assert_eq!(
        page.results[0].descriptor.release.payload.extension_id,
        identity
    );
    assert!(!page.network_complete && page.metadata_only);
    // These are deliberately unsigned pagination fixtures, not an admitted
    // source or proof of native/public-source/paid-account verification.
    Ok(())
}

#[tokio::test]
async fn independent_peer_cursors_and_buffers_survive_combined_pagination_without_global_claims()
-> Result<()> {
    let (_owner, interest, mut request) = context(2);
    let mut transport = Mock::new();
    for n in 1..=3 {
        transport.catalog(
            n,
            vec![
                vec![candidate(n * 10), candidate(n * 10 + 1)],
                vec![candidate(n * 10 + 2)],
            ],
        );
    }
    let mut session = Session::default();
    let mut ids = BTreeSet::new();
    let mut pages = 0;
    loop {
        let page = session.read(&transport, &request, &interest).await?;
        pages += 1;
        assert!(page.metadata_only && !page.network_complete);
        assert!(page.results.len() <= 2);
        for result in page.results {
            assert!(ids.insert(result.descriptor.release.payload.extension_id));
        }
        request.cursor = page.next_cursor;
        if request.cursor.is_none() {
            break;
        }
        assert!(pages < 10);
    }
    assert_eq!(ids.len(), 9);
    assert_eq!(session.seen.len(), 9);
    let calls = transport.calls.lock().unwrap();
    for n in 1..=3 {
        assert_eq!(
            calls
                .iter()
                .filter(|(key, _)| *key == peer(n).to_string())
                .count(),
            2
        );
    }
    assert!(calls.iter().any(|(_, cursor)| cursor.is_some()));
    Ok(())
}

#[tokio::test]
async fn cached_first_cards_do_not_wait_for_or_contact_peer_discovery() -> Result<()> {
    struct LocalOnly;
    #[async_trait]
    impl Transport for LocalOnly {
        async fn peers(&self, _: &Interest) -> Result<Vec<PeerKey>> {
            panic!("cached search must not touch peer discovery");
        }
        async fn fetch(
            &self,
            location: &Location,
            _: &SearchQuery,
            _: Option<String>,
            _: &Interest,
        ) -> Result<SearchPage> {
            assert_eq!(*location, Location::Local);
            Ok(SearchPage {
                schema_version: SCHEMA_VERSION,
                snapshot_id: Digest::of(b"local"),
                results: vec![candidate(1)],
                next_cursor: None,
                network_complete: false,
                metadata_only: true,
            })
        }
    }
    let (_owner, interest, request) = context(20);
    let mut session = Session::default();
    let page = tokio::time::timeout(
        Duration::from_secs(1),
        session.read(&LocalOnly, &request, &interest),
    )
    .await??;
    assert_eq!(page.results.len(), 1);
    assert!(
        page.next_cursor.is_some(),
        "network continuation remains possible, not complete coverage"
    );
    assert!(!session.discovered);
    assert_eq!(
        session.evidence_origins(Digest::of(canonical::to_vec(&page.results[0].descriptor)?)),
        vec![EvidenceOrigin::Cache]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn first_remote_hint_does_not_wait_for_the_next_peer_but_keeps_its_work() -> Result<()> {
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct FastThenSlow {
        fetched: AtomicUsize,
        second_ready: AtomicBool,
    }
    #[async_trait]
    impl Transport for FastThenSlow {
        async fn peers(&self, _: &Interest) -> Result<Vec<PeerKey>> {
            Ok(vec![peer(1), peer(2)])
        }
        async fn fetch(
            &self,
            location: &Location,
            _: &SearchQuery,
            cursor: Option<String>,
            _: &Interest,
        ) -> Result<SearchPage> {
            assert!(cursor.is_none(), "each peer gets its own fresh search");
            let results = if matches!(location, Location::Peer(_)) {
                let n = self.fetched.fetch_add(1, Ordering::SeqCst);
                if n > 0 && !self.second_ready.load(Ordering::SeqCst) {
                    std::future::pending::<()>().await;
                }
                vec![candidate(n + 1)]
            } else {
                vec![]
            };
            Ok(SearchPage {
                schema_version: SCHEMA_VERSION,
                snapshot_id: Digest::of(b"private pagination fixture"),
                results,
                next_cursor: None,
                network_complete: false,
                metadata_only: true,
            })
        }
    }
    let (_owner, interest, request) = context(20);
    let transport = FastThenSlow::default();
    let mut session = Session::default();
    session.begin_page();
    let page = tokio::time::timeout(
        Duration::from_millis(100),
        session.read_chunk(&transport, &request, &interest, 1),
    )
    .await??;
    assert_eq!(transport.fetched.load(Ordering::SeqCst), 1);
    assert_eq!(page.results.len(), 1);
    assert!(!page.network_complete && page.next_cursor.is_some());
    assert!(session.admitted.is_empty(), "a peer hint is not admission");
    let id = Digest::of(canonical::to_vec(&page.results[0].descriptor)?);
    session.admission_result(id, AdmissionFeedback::Accepted)?;
    transport.second_ready.store(true, Ordering::SeqCst);
    let remainder = session
        .more_from(&transport, &request, &interest)
        .await?
        .unwrap();
    assert_eq!(transport.fetched.load(Ordering::SeqCst), 2);
    assert_eq!(remainder.results.len(), 1);
    assert_ne!(
        remainder.results[0].descriptor.to_bytes()?,
        page.results[0].descriptor.to_bytes()?
    );
    assert!(!remainder.network_complete);
    Ok(())
}

#[tokio::test]
async fn local_results_precede_peer_discovery_and_errors_do_not_hide_a_local_page() -> Result<()> {
    let (_owner, interest, request) = context(20);
    let mut transport = Mock::new();
    transport
        .pages
        .insert("local".into(), vec![vec![candidate(99)]]);
    for n in 1..=6 {
        transport.catalog(n, vec![]);
    }
    transport.fail = true;
    let mut session = Session::default();
    let page = session.read(&transport, &request, &interest).await?;
    assert_eq!(page.results.len(), 1);
    assert!(!session.discovered);
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
    let mut next = request.clone();
    next.cursor = page.next_cursor;
    assert!(next.cursor.is_some());
    let remainder = session.read(&transport, &next, &interest).await?;
    assert!(remainder.results.is_empty());
    assert!(remainder.next_cursor.is_none());
    assert_eq!(transport.calls.lock().unwrap().len(), 7);
    assert_eq!(session.sources.len(), 7);
    assert!(session.sources.iter().all(|s| s.enabled));
    Ok(())
}

#[tokio::test]
async fn transport_hints_do_not_defeat_dedup_but_invalid_first_proofs_cannot_suppress_different_proofs()
-> Result<()> {
    let (_owner, interest, request) = context(20);
    let mut transport = Mock::new();
    let valid = candidate(0);
    let mut different_hint = valid.clone();
    different_hint.retained_support.weighted_score = 20;
    let mut different_proof = valid.clone();
    different_proof.descriptor.gate_evidence[0]
        .credential
        .payload
        .tenure_weight = 2;
    transport
        .pages
        .insert("local".into(), vec![vec![valid.clone()]]);
    transport.catalog(1, vec![vec![different_hint, different_proof]]);
    let mut session = Session::default();
    let page = session.read(&transport, &request, &interest).await?;
    assert_eq!(page.results.len(), 1, "local candidates are emitted first");
    let mut continuation = request.clone();
    continuation.cursor = page.next_cursor;
    let next = session.read(&transport, &continuation, &interest).await?;
    let results: Vec<_> = page.results.into_iter().chain(next.results).collect();
    assert_eq!(
        results.len(),
        2,
        "raw candidate identity includes proofs but not peer popularity claims"
    );
    assert!(
        results
            .iter()
            .all(|r| r.descriptor.release.payload.extension_id
                == valid.descriptor.release.payload.extension_id)
    );
    Ok(())
}

#[tokio::test]
async fn replay_mixed_queries_and_unrecognized_tokens_send_no_more_packets() -> Result<()> {
    let (_owner, interest, mut request) = context(1);
    let mut transport = Mock::new();
    transport.catalog(
        1,
        vec![vec![candidate(1)], vec![candidate(2)], vec![candidate(3)]],
    );
    let mut session = Session::default();
    let first = session.read(&transport, &request, &interest).await?;
    let first_token = first.next_cursor.unwrap();
    request.cursor = Some(first_token.clone());
    let second = session.read(&transport, &request, &interest).await?;
    assert!(second.next_cursor.is_some());
    let before = transport.calls.lock().unwrap().len();
    assert!(session.read(&transport, &request, &interest).await.is_err());
    request.cursor = second.next_cursor;
    request.query.text = "different".into();
    assert!(session.read(&transport, &request, &interest).await.is_err());
    request.query.text.clear();
    request.cursor = Some("unknown".into());
    assert!(session.read(&transport, &request, &interest).await.is_err());
    assert_eq!(transport.calls.lock().unwrap().len(), before);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stale_sessions_and_lost_interest_cannot_refresh_or_reuse_saved_buffers() -> Result<()> {
    let (owner, interest, mut request) = context(1);
    let mut transport = Mock::new();
    transport.catalog(1, vec![vec![candidate(1)], vec![candidate(2)]]);
    let mut session = Session::default();
    request.cursor = session
        .read(&transport, &request, &interest)
        .await?
        .next_cursor;
    let before = transport.calls.lock().unwrap().len();
    tokio::time::advance(IDLE + Duration::from_secs(1)).await;
    owner
        .slot
        .lock()
        .unwrap()
        .query
        .as_mut()
        .unwrap()
        .last_active = Instant::now();
    assert!(session.read(&transport, &request, &interest).await.is_err());
    owner.slot.lock().unwrap().query = None;
    assert!(session.read(&transport, &request, &interest).await.is_err());
    assert_eq!(transport.calls.lock().unwrap().len(), before);
    Ok(())
}

#[tokio::test]
async fn changing_snapshots_repeated_cursors_and_oversized_pages_retire_only_the_bad_source()
-> Result<()> {
    for kind in 0..3 {
        let (_owner, interest, mut request) = context(1);
        let mut transport = Mock::new();
        transport.catalog(
            1,
            vec![vec![candidate(1)], vec![candidate(2)], vec![candidate(3)]],
        );
        transport.repeated = kind == 0;
        transport.wrong_snapshot = kind == 1;
        transport.oversized = kind == 2;
        let mut session = Session::default();
        let first = session.read(&transport, &request, &interest).await?;
        if let Some(token) = first.next_cursor {
            request.cursor = Some(token);
            let next = session.read(&transport, &request, &interest).await?;
            assert!(next.results.is_empty());
            assert!(next.next_cursor.is_none());
        } else {
            assert!(first.results.is_empty());
        }
        assert!(transport.calls.lock().unwrap().len() <= 4);
    }
    Ok(())
}

#[tokio::test]
async fn ten_thousand_raw_candidates_from_overlapping_catalogs_paginate_with_fixed_source_buffers()
-> Result<()> {
    // Planner/cursor qualification only: no crypto/source/native/network or
    // hard-budget assertion is made by these deliberately synthetic records.
    struct Generated {
        template: SearchResult,
        calls: std::sync::atomic::AtomicUsize,
    }
    #[async_trait]
    impl Transport for Generated {
        async fn peers(&self, _: &Interest) -> Result<Vec<PeerKey>> {
            Ok((1..=3).map(peer).collect())
        }
        async fn fetch(
            &self,
            location: &Location,
            query: &SearchQuery,
            cursor: Option<String>,
            _: &Interest,
        ) -> Result<SearchPage> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let key = name(location);
            let offset = cursor
                .map(|c| {
                    c.strip_prefix(&format!("{key}_"))
                        .context("foreign cursor")
                        .and_then(|s| Ok(s.parse::<usize>()?))
                })
                .transpose()?
                .unwrap_or(0);
            let mut results = vec![];
            if matches!(location, Location::Peer(_)) {
                for n in offset..(offset + query.limit).min(10_000) {
                    let mut item = self.template.clone();
                    let release = &mut item.descriptor.release.payload;
                    release.extension_id = ExtensionId::new(
                        &item.descriptor.publisher.payload.root_key,
                        &format!("scale-{n:05}"),
                    )?;
                    release.display_name = format!("Scale {n:05}");
                    results.push(item);
                }
            }
            let next_cursor = (!results.is_empty() && offset + results.len() < 10_000)
                .then(|| format!("{key}_{}", offset + results.len()));
            Ok(SearchPage {
                schema_version: 1,
                snapshot_id: Digest::of(b"generated catalog"),
                results,
                next_cursor,
                network_complete: false,
                metadata_only: true,
            })
        }
    }
    let (owner, interest, mut request) = context(20);
    let transport = Generated {
        template: candidate(0),
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let mut session = Session::default();
    let mut ids = BTreeSet::new();
    let mut pages = 0;
    loop {
        owner
            .slot
            .lock()
            .unwrap()
            .query
            .as_mut()
            .unwrap()
            .last_active = Instant::now();
        let page = session.read(&transport, &request, &interest).await?;
        pages += 1;
        for item in page.results {
            assert!(ids.insert(item.descriptor.release.payload.extension_id));
        }
        assert!(
            session.sources.len() <= 7
                && session
                    .sources
                    .iter()
                    .all(|s| s.pending.len() <= 20 && s.cursors.len() <= 512)
        );
        assert!(
            session.seen.len() <= 10_000
                && session.examined <= MAX_EXAMINED
                && session.requests <= MAX_REQUESTS
        );
        request.cursor = page.next_cursor;
        if request.cursor.is_none() {
            break;
        }
        assert!(pages <= 501);
    }
    assert_eq!(ids.len(), 10_000);
    assert_eq!(pages, 500);
    assert!(transport.calls.load(std::sync::atomic::Ordering::SeqCst) <= 1501);
    Ok(())
}

#[tokio::test]
async fn replaced_queries_cancel_only_their_read_waiters_during_discovery_or_a_peer_page()
-> Result<()> {
    struct Hanging {
        discovery: bool,
        started: tokio::sync::Notify,
    }
    #[async_trait]
    impl Transport for Hanging {
        async fn peers(&self, _: &Interest) -> Result<Vec<PeerKey>> {
            if self.discovery {
                self.started.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(vec![peer(1)])
        }
        async fn fetch(
            &self,
            location: &Location,
            _: &SearchQuery,
            _: Option<String>,
            _: &Interest,
        ) -> Result<SearchPage> {
            if matches!(location, Location::Peer(_)) {
                self.started.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(SearchPage {
                schema_version: 1,
                snapshot_id: Digest::of(b"empty"),
                results: vec![],
                next_cursor: None,
                network_complete: false,
                metadata_only: true,
            })
        }
    }
    for discovery in [true, false] {
        let (owner, interest, request) = context(20);
        let transport = Arc::new(Hanging {
            discovery,
            started: tokio::sync::Notify::new(),
        });
        let querying = transport.clone();
        let task = tokio::spawn(async move {
            Session::default()
                .read(querying.as_ref(), &request, &interest)
                .await
        });
        transport.started.notified().await;
        owner.slot.lock().unwrap().query = None;
        assert!(timeout(Duration::from_secs(2), task).await??.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn native_admission_failures_activate_fallback_before_a_bad_peer_can_spend_the_page_budget()
-> Result<()> {
    // Real local signature/adoption checks, synthetic source/protection owners.
    // This does not qualify a protected public-source/native process path.
    use super::super::tests::{Fake, descriptor};
    struct Byzantine {
        good: Vec<CandidateDescriptor>,
        order: Mutex<Vec<PeerKey>>,
    }
    #[async_trait]
    impl Transport for Byzantine {
        async fn peers(&self, _: &Interest) -> Result<Vec<PeerKey>> {
            Ok((1..=6).map(peer).collect())
        }
        async fn fetch(
            &self,
            location: &Location,
            query: &SearchQuery,
            _: Option<String>,
            _: &Interest,
        ) -> Result<SearchPage> {
            let mut results = vec![];
            let mut next_cursor = None;
            if let Location::Peer(key) = location {
                let mut order = self.order.lock().unwrap();
                let ordinal = match order.iter().position(|known| known == key) {
                    Some(n) => n,
                    None => {
                        order.push(key.clone());
                        order.len() - 1
                    }
                };
                if ordinal < 3 {
                    let mut forged = self.good[0].clone();
                    forged.release.payload.description = format!("unsigned bad source {ordinal}");
                    results = vec![
                        SearchResult {
                            descriptor: forged,
                            retained_support: candidate(0).retained_support
                        };
                        query.limit
                    ];
                    next_cursor = Some(format!("bad_{ordinal}_more"));
                } else {
                    results = self
                        .good
                        .iter()
                        .take(query.limit)
                        .cloned()
                        .map(|descriptor| SearchResult {
                            descriptor,
                            retained_support: candidate(0).retained_support,
                        })
                        .collect();
                }
            }
            Ok(SearchPage {
                schema_version: 1,
                snapshot_id: Digest::of(b"source snapshot"),
                results,
                next_cursor,
                network_complete: false,
                metadata_only: true,
            })
        }
    }
    struct Admitted {
        transport: Byzantine,
        session: tokio::sync::Mutex<Session>,
        checks: Fake,
    }
    #[async_trait]
    impl Backend for Admitted {
        async fn page(&self, request: &SearchRequest, interest: &Interest) -> Result<SearchPage> {
            let mut session = self.session.lock().await;
            session.begin_page();
            session
                .read_chunk(&self.transport, request, interest, 1)
                .await
        }
        async fn more(
            &self,
            request: &SearchRequest,
            interest: &Interest,
        ) -> Result<Option<SearchPage>> {
            self.session
                .lock()
                .await
                .more_from(&self.transport, request, interest)
                .await
        }
        async fn admission_result(
            &self,
            descriptor: Digest,
            feedback: AdmissionFeedback,
        ) -> Result<()> {
            self.session
                .lock()
                .await
                .admission_result(descriptor, feedback)
        }
        async fn admit(
            &self,
            descriptor: CandidateDescriptor,
            interest: &Interest,
        ) -> Result<Arc<Verified>> {
            self.checks.admit(descriptor, interest).await
        }
        async fn current(&self, value: &Verified) -> Result<()> {
            self.checks.current(value).await
        }
    }
    for limit in [1, 20] {
        let backend = Arc::new(Admitted {
            transport: Byzantine {
                good: (0..20).map(|n| descriptor(n, 1)).collect::<Result<_>>()?,
                order: Mutex::new(vec![]),
            },
            session: tokio::sync::Mutex::new(Session::default()),
            checks: Fake::new(vec![]),
        });
        let service = SearchService::default();
        let request = SearchRequest {
            query: SearchQuery {
                text: String::new(),
                tags: vec![],
                limit,
            },
            cursor: None,
            prepare_catalog: false,
        };
        let id = service.start_with(backend.clone(), request)?;
        let view = timeout(Duration::from_secs(10), async {
            loop {
                let view = service.view(id).await?;
                if view.phase != SearchPhase::Checking {
                    return Ok::<_, anyhow::Error>(view);
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        assert!(
            !view.results.is_empty(),
            "bad initial peers must not hide the fallback peers"
        );
        assert_eq!(view.omitted, 3);
        assert!(view.checked <= 20 && view.results.len() <= limit);
        assert_eq!(view.results.len(), if limit == 1 { 1 } else { 17 });
        let session = backend.session.lock().await;
        assert_eq!(session.page_requests, 7);
        assert!(session.page_examined <= PAGE_EXAMINED);
        assert_eq!(session.sources.iter().filter(|s| s.enabled).count(), 7);
        let bad: Vec<_> = backend
            .transport
            .order
            .lock()
            .unwrap()
            .iter()
            .take(3)
            .cloned()
            .collect();
        assert!(
            session
                .sources
                .iter()
                .filter(|s| match &s.location {
                    Location::Peer(peer) => bad.contains(peer),
                    _ => false,
                })
                .all(|s| s.done && s.pending.is_empty())
        );
        assert!(session.emitted.is_empty());
        assert_eq!(session.admitted.len(), view.results.len());
        for (descriptor, origin) in &session.admitted {
            let expected = match &session.sources[*origin].location {
                Location::Local => EvidenceOrigin::Cache,
                Location::Peer(peer) => EvidenceOrigin::Peer(peer.clone()),
            };
            let origins = session.evidence_origins(*descriptor);
            assert_eq!(origins.first(), Some(&expected));
            assert!(origins.iter().all(|origin| match origin {
                EvidenceOrigin::Cache => true,
                EvidenceOrigin::Peer(peer) => !bad.contains(peer),
            }));
        }
        drop(session);
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[tokio::test]
async fn source_and_session_work_caps_end_continuations_without_growing_history() -> Result<()> {
    let (_owner, interest, mut request) = context(1);
    let mut transport = Mock::new();
    transport.catalog(
        1,
        vec![vec![candidate(1)], vec![candidate(2)], vec![candidate(3)]],
    );
    let mut session = Session::default();
    request.cursor = session
        .read(&transport, &request, &interest)
        .await?
        .next_cursor;
    let source = session
        .sources
        .iter_mut()
        .find(|s| matches!(s.location, Location::Peer(_)))
        .unwrap();
    source.pages = MAX_SOURCE_PAGES - 1;
    let page = session.read(&transport, &request, &interest).await?;
    assert_eq!(page.results.len(), 1);
    assert!(page.next_cursor.is_none());
    assert!(
        session
            .sources
            .iter()
            .all(|s| s.cursors.len() <= MAX_SOURCE_PAGES && s.pending.len() <= MAX_SEARCH_RESULTS)
    );
    let mut session = Session::default();
    request.cursor = None;
    request.cursor = session
        .read(&transport, &request, &interest)
        .await?
        .next_cursor;
    session.requests = MAX_REQUESTS;
    let before = transport.calls.lock().unwrap().len();
    let page = session.read(&transport, &request, &interest).await?;
    assert!(page.next_cursor.is_none());
    assert_eq!(transport.calls.lock().unwrap().len(), before);
    Ok(())
}
