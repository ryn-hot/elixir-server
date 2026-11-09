//! Exercise the real search service AND its one-candidate Session path. Raw
//! pagination's multi-candidate read alone cannot prove a partial page is Ready.
//! Signatures are real local checks; source/protection owners remain synthetic.
use super::*;
use crate::marketplace::search::tests::{Fake, descriptor, ready, request};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Checked {
    catalog: Mock,
    fail_local: bool,
    discoveries: AtomicUsize,
    session: tokio::sync::Mutex<Session>,
    checks: Fake,
    unavailable: BTreeSet<ExtensionId>,
    discovery_progress: DiscoveryProgress,
    delay_discovery: bool,
    discovery_started: tokio::sync::Notify,
    continue_discovery: tokio::sync::Notify,
}
impl Checked {
    fn new(local: Vec<CandidateDescriptor>) -> Result<Self> {
        let row = |descriptor| SearchResult {
            descriptor,
            retained_support: candidate(0).retained_support,
        };
        let mut catalog = Mock::new();
        catalog
            .pages
            .insert("local".into(), vec![local.into_iter().map(row).collect()]);
        catalog.catalog(1, vec![vec![row(descriptor(100, 1)?)]]);
        let discovery_progress = DiscoveryProgress::default();
        Ok(Self {
            catalog,
            fail_local: false,
            discoveries: 0.into(),
            session: tokio::sync::Mutex::new(Session::with_progress(discovery_progress.clone())),
            checks: Fake::new(vec![]),
            unavailable: BTreeSet::new(),
            discovery_progress,
            delay_discovery: false,
            discovery_started: tokio::sync::Notify::new(),
            continue_discovery: tokio::sync::Notify::new(),
        })
    }
}
#[async_trait]
impl Transport for Checked {
    async fn peers(&self, interest: &Interest) -> Result<Vec<PeerKey>> {
        self.discoveries.fetch_add(1, Ordering::SeqCst);
        if self.delay_discovery {
            self.discovery_started.notify_one();
            self.continue_discovery.notified().await;
        }
        self.catalog.peers(interest).await
    }
    async fn fetch(
        &self,
        location: &Location,
        query: &SearchQuery,
        cursor: Option<String>,
        interest: &Interest,
    ) -> Result<SearchPage> {
        if self.fail_local && matches!(location, Location::Local) {
            bail!("synthetic local catalog read unavailable");
        }
        self.catalog.fetch(location, query, cursor, interest).await
    }
}
#[async_trait]
impl Backend for Checked {
    fn searching_peers(&self) -> bool {
        self.discovery_progress.waiting()
    }
    fn local_page_complete(&self, query: Uuid) -> bool {
        self.discovery_progress.local_page_complete(query)
    }
    async fn page(&self, request: &SearchRequest, interest: &Interest) -> Result<SearchPage> {
        let mut session = self.session.lock().await;
        session.begin_page();
        session.read_chunk(self, request, interest, 1).await
    }
    async fn more(
        &self,
        request: &SearchRequest,
        interest: &Interest,
    ) -> Result<Option<SearchPage>> {
        self.session
            .lock()
            .await
            .more_from(self, request, interest)
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
        let value = self.checks.admit(descriptor, interest).await?;
        if self
            .unavailable
            .contains(&value.metadata.descriptor().release.payload.extension_id)
        {
            // Simulate only the source stage AFTER real metadata verification.
            // No positive source/card escapes this synthetic failed admission.
            return Err(PublicSourceUnavailable.into());
        }
        Ok(value)
    }
    async fn current(&self, value: &Verified) -> Result<()> {
        self.checks.current(value).await
    }
}

#[tokio::test]
async fn update_lookup_keeps_normal_admission_and_never_substitutes_a_trusted_local_card()
-> Result<()> {
    for forged in [false, true] {
        let local = descriptor(100, 1)?;
        let mut backend = Checked::new(vec![local.clone()])?;
        *backend.session.get_mut() = Session::for_update(backend.discovery_progress.clone());
        if forged {
            backend.catalog.pages.get_mut(&peer(1).to_string()).unwrap()[0][0]
                .descriptor
                .release
                .payload
                .description = "unsigned remote mutation".into();
        }
        let backend = Arc::new(backend);
        let service = SearchService::default();
        let mut query = request("");
        query.query.text = local.release.payload.extension_id.to_string();
        query.query.limit = 1;
        let id = service.start_with(backend.clone(), query)?;
        let view = ready(&service, id).await?;
        assert_eq!(view.results.len(), usize::from(!forged));
        assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
        assert!(
            backend
                .catalog
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|(name, _)| name != "local")
        );
        assert!(!view.network_complete);
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[tokio::test]
async fn empty_local_progress_is_readable_during_discovery_without_claiming_ready_or_complete()
-> Result<()> {
    let mut backend = Checked::new(vec![])?;
    backend.delay_discovery = true;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request(""))?;
    timeout(Duration::from_secs(5), backend.discovery_started.notified()).await?;
    // The actual session mutex is held by the blocked peer read. A view must
    // remain available without waiting for that read or issuing a second one.
    let view = timeout(Duration::from_secs(1), service.view(id)).await??;
    assert_eq!(view.phase, SearchPhase::Checking);
    assert!(view.local_page_complete);
    assert!(!backend.discovery_progress.local_page_complete(Uuid::new_v4()));
    assert!(view.searching_peers);
    assert!(view.results.is_empty());
    assert_eq!((view.checked, view.pending, view.omitted), (0, 0, 0));
    assert!(!view.network_complete && view.metadata_only && view.ranking_partial);
    assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
    backend.continue_discovery.notify_one();
    let view = ready(&service, id).await?;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert!(!view.searching_peers);
    assert_eq!(
        view.results.len(),
        1,
        "discovery still supplies independently admitted peer results"
    );
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn local_page_progress_never_substitutes_for_setup_update_or_failed_local_reads()
-> Result<()> {
    for case in ["setup", "update", "failed_local", "invalid_local"] {
        let mut backend = Checked::new(vec![])?;
        backend.delay_discovery = true;
        let mut query = request("");
        match case {
            "setup" => query.prepare_catalog = true,
            "update" => {
                *backend.session.get_mut() =
                    Session::for_update(backend.discovery_progress.clone());
            }
            "failed_local" => backend.fail_local = true,
            "invalid_local" => backend.catalog.oversized = true,
            _ => unreachable!(),
        }
        let backend = Arc::new(backend);
        let service = SearchService::default();
        let id = service.start_with(backend.clone(), query)?;
        timeout(Duration::from_secs(5), backend.discovery_started.notified()).await?;
        let view = timeout(Duration::from_secs(1), service.view(id)).await??;
        assert_eq!(view.phase, SearchPhase::Checking, "{case}");
        assert!(!view.local_page_complete, "{case}");
        assert!(view.results.is_empty() && !view.network_complete, "{case}");
        assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
        backend.continue_discovery.notify_one();
        let _ = ready(&service, id).await?;
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[tokio::test]
async fn replaced_query_cannot_inherit_local_completion_from_the_previous_page() -> Result<()> {
    let mut backend = Checked::new(vec![])?;
    backend.delay_discovery = true;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let original = service.start_with(backend.clone(), request(""))?;
    timeout(Duration::from_secs(5), backend.discovery_started.notified()).await?;
    assert!(service.view(original).await?.local_page_complete);
    // Share the exact progress owner, like a continuation. The old peer read
    // still holds its mutex; no replacement local request has completed.
    let replacement = service.start_with(backend.clone(), request(""))?;
    assert!(!service.view(replacement).await?.local_page_complete);
    assert!(service.view(original).await.is_err());
    backend.continue_discovery.notify_one();
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn replacing_a_peer_wait_clears_its_progress_and_keeps_the_new_query_independent()
-> Result<()> {
    let mut backend = Checked::new(vec![])?;
    backend.delay_discovery = true;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let old = service.start_with(backend.clone(), request(""))?;
    timeout(Duration::from_secs(5), backend.discovery_started.notified()).await?;
    assert!(service.view(old).await?.searching_peers);
    let latest = service.start_with(Arc::new(Fake::new(vec![descriptor(7, 1)?])), request(""))?;
    let view = ready(&service, latest).await?;
    assert_eq!(view.results.len(), 1);
    assert!(!view.searching_peers);
    assert!(
        !backend.discovery_progress.waiting(),
        "cancelled read drops its presentation guard"
    );
    assert!(service.view(old).await.is_err());
    assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn partially_filled_verified_local_pages_are_ready_before_discovery_and_keep_more()
-> Result<()> {
    for (count, reject_last) in [(1, false), (19, false), (1, true)] {
        let mut local = (0..count)
            .map(|n| descriptor(n, 1))
            .collect::<Result<Vec<_>>>()?;
        if reject_last {
            let mut forged = descriptor(count, 1)?;
            forged.release.payload.description = "unsigned".into();
            local.push(forged);
        }
        let backend = Arc::new(Checked::new(local)?);
        let service = SearchService::default();
        let id = service.start_with(backend.clone(), request(""))?;
        let view = ready(&service, id).await?;
        assert_eq!(view.phase, SearchPhase::Ready);
        assert_eq!(view.results.len(), count);
        assert_eq!(view.checked, count + usize::from(reject_last));
        assert_eq!(view.omitted, usize::from(reject_last));
        assert!(!view.network_complete && view.ranking_partial && view.metadata_only);
        assert_eq!(backend.discoveries.load(Ordering::SeqCst), 0);
        assert_eq!(backend.catalog.calls.lock().unwrap().len(), 1);
        {
            let session = backend.session.lock().await;
            assert!(!session.discovered);
            assert!(session.emitted.is_empty());
            assert_eq!(session.admitted.len(), count);
            assert!(view.next_cursor.is_some());
            assert_eq!(view.next_cursor, session.token);
        }
        // The retained token has NOT been consumed by an empty hidden chunk.
        // Explicit More starts a new displayed page and may discover normally.
        let mut next = request("");
        next.cursor = view.next_cursor;
        let id = service.start_with(backend.clone(), next)?;
        let view = ready(&service, id).await?;
        assert_eq!(view.phase, SearchPhase::Ready);
        assert_eq!(view.results.len(), 1);
        assert_eq!(view.results[0].name, "Example 100");
        assert_eq!(view.checked, 1);
        assert_eq!(view.omitted, 0);
        assert!(view.next_cursor.is_none());
        assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[tokio::test]
async fn empty_or_rejected_local_data_still_discovers_before_a_page_is_ready() -> Result<()> {
    for reject_local in [false, true] {
        let mut local = vec![];
        if reject_local {
            let mut forged = descriptor(0, 1)?;
            forged.release.payload.description = "unsigned".into();
            local.push(forged);
        }
        let backend = Arc::new(Checked::new(local)?);
        let service = SearchService::default();
        let id = service.start_with(backend.clone(), request(""))?;
        let view = ready(&service, id).await?;
        assert_eq!(view.phase, SearchPhase::Ready);
        assert_eq!(view.results.len(), 1);
        assert_eq!(view.results[0].name, "Example 100");
        assert_eq!(view.checked, 1 + usize::from(reject_local));
        assert_eq!(view.omitted, usize::from(reject_local));
        assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
        assert!(view.next_cursor.is_none());
        assert!(!view.network_complete);
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[test]
fn source_outage_feedback_is_typed_not_inferred_from_peer_controlled_error_text() {
    let unavailable: Result<Arc<Verified>> = Err(PublicSourceUnavailable.into());
    assert_eq!(
        AdmissionFeedback::from_result(&unavailable),
        AdmissionFeedback::SourceUnavailable
    );
    let contextual = unavailable.context("source stage");
    assert_eq!(
        AdmissionFeedback::from_result(&contextual),
        AdmissionFeedback::SourceUnavailable
    );
    let text: Result<Arc<Verified>> = Err(anyhow::anyhow!(PublicSourceUnavailable.to_string()));
    assert_eq!(
        AdmissionFeedback::from_result(&text),
        AdmissionFeedback::Rejected,
        "untrusted strings must not determine the private admission stage"
    );
}

#[tokio::test]
async fn unavailable_local_rows_do_not_discard_the_rest_of_a_verified_page() -> Result<()> {
    for bad in [0, 9, 19] {
        let local = (0..20)
            .map(|n| descriptor(n, 1))
            .collect::<Result<Vec<_>>>()?;
        let unavailable = local[bad].release.payload.extension_id.clone();
        let mut backend = Checked::new(local)?;
        backend.unavailable.insert(unavailable.clone());
        let backend = Arc::new(backend);
        let service = SearchService::default();
        let id = service.start_with(backend.clone(), request(""))?;
        let view = ready(&service, id).await?;
        assert_eq!(view.phase, SearchPhase::Ready);
        assert_eq!(view.results.len(), 19);
        assert_eq!(view.checked, 20);
        assert_eq!(view.omitted, 1);
        assert!(
            view.results
                .iter()
                .all(|row| row.extension_id != unavailable)
        );
        assert!(!view.network_complete && view.ranking_partial && view.metadata_only);
        assert!(view.next_cursor.is_some());
        assert_eq!(backend.discoveries.load(Ordering::SeqCst), 0);
        assert_eq!(backend.catalog.calls.lock().unwrap().len(), 1);
        {
            let session = backend.session.lock().await;
            assert!(!session.sources[0].rejected);
            assert!(session.sources[0].pending.is_empty());
            assert_eq!(session.page_examined, 20);
            assert_eq!(session.admitted.len(), 19);
            assert!(session.emitted.is_empty());
        }
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[tokio::test]
async fn an_invalid_cached_row_does_not_suppress_unrelated_local_results() -> Result<()> {
    let mut local = (0..4)
        .map(|n| descriptor(n, 1))
        .collect::<Result<Vec<_>>>()?;
    let bad = local[0].release.payload.extension_id.clone();
    local[0].release.payload.description = "unsigned".into();
    let backend = Arc::new(Checked::new(local)?);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request(""))?;
    let view = ready(&service, id).await?;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 3);
    assert_eq!(view.checked, 4);
    assert_eq!(view.omitted, 1);
    assert!(view.results.iter().all(|row| row.extension_id != bad));
    assert_eq!(backend.discoveries.load(Ordering::SeqCst), 0);
    assert!(!backend.session.lock().await.sources[0].rejected);
    service.request_stop();
    service.join().await?;
    Ok(())
}

#[tokio::test]
async fn authenticated_peer_source_outages_omit_rows_without_retiring_the_catalog() -> Result<()> {
    for all_unavailable in [false, true] {
        let rows = (0..20)
            .map(|n| descriptor(n, 1))
            .collect::<Result<Vec<_>>>()?;
        let mut backend = Checked::new(vec![])?;
        backend.unavailable.extend(
            rows.iter()
                .take(if all_unavailable { 20 } else { 1 })
                .map(|row| row.release.payload.extension_id.clone()),
        );
        backend.catalog.pages.insert(
            peer(1).to_string(),
            vec![
                rows.into_iter()
                    .map(|descriptor| SearchResult {
                        descriptor,
                        retained_support: candidate(0).retained_support,
                    })
                    .collect(),
            ],
        );
        let backend = Arc::new(backend);
        let service = SearchService::default();
        let id = service.start_with(backend.clone(), request(""))?;
        let view = ready(&service, id).await?;
        assert_eq!(
            view.phase,
            if all_unavailable {
                SearchPhase::Unavailable
            } else {
                SearchPhase::Ready
            }
        );
        assert_eq!(view.results.len(), if all_unavailable { 0 } else { 19 });
        assert_eq!(view.checked, 20);
        assert_eq!(view.omitted, if all_unavailable { 20 } else { 1 });
        assert!(
            view.results
                .iter()
                .all(|row| !backend.unavailable.contains(&row.extension_id))
        );
        assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
        assert_eq!(backend.catalog.calls.lock().unwrap().len(), 2);
        assert!(view.next_cursor.is_none());
        {
            let session = backend.session.lock().await;
            let source = session
                .sources
                .iter()
                .find(|source| matches!(source.location, Location::Peer(_)))
                .unwrap();
            assert!(!source.rejected && source.pending.is_empty());
            assert_eq!(session.page_examined, 20);
            assert_eq!(session.page_requests, 2);
            assert_eq!(session.admitted.len(), view.results.len());
            assert!(session.emitted.is_empty());
        }
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}

#[tokio::test]
async fn unavailable_only_local_rows_still_allow_verified_peer_fallback() -> Result<()> {
    let local = (0..2)
        .map(|n| descriptor(n, 1))
        .collect::<Result<Vec<_>>>()?;
    let unavailable = local
        .iter()
        .map(|row| row.release.payload.extension_id.clone())
        .collect();
    let mut backend = Checked::new(local)?;
    backend.unavailable = unavailable;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request(""))?;
    let view = ready(&service, id).await?;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 1);
    assert_eq!(view.results[0].name, "Example 100");
    assert_eq!(view.checked, 3);
    assert_eq!(view.omitted, 2);
    assert_eq!(backend.discoveries.load(Ordering::SeqCst), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}
