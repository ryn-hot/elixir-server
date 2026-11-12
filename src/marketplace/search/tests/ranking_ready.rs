//! Scheduling only: real local signatures, synthetic source/protection owners.
//! Held evidence represents an already-owned operation, not live network IO.
use super::*;

struct Later {
    checks: Fake,
    hold: AtomicBool,
    available: AtomicBool,
    invalid_origins: AtomicBool,
    started: Notify,
    resume: Notify,
    calls: AtomicUsize,
    populated: AtomicUsize,
}
impl Later {
    fn new() -> Result<Self> {
        let checks = Fake::new(vec![descriptor(0, 1)?, descriptor(1, 4)?]);
        checks.rank.store(true, Ordering::SeqCst);
        Ok(Self {
            checks,
            hold: AtomicBool::new(true),
            available: AtomicBool::new(true),
            invalid_origins: AtomicBool::new(false),
            started: Notify::new(),
            resume: Notify::new(),
            calls: AtomicUsize::new(0),
            populated: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl Backend for Later {
    async fn page(&self, r: &SearchRequest, i: &Interest) -> Result<SearchPage> {
        self.checks.page(r, i).await
    }
    async fn admit(&self, d: CandidateDescriptor, i: &Interest) -> Result<Arc<Verified>> {
        self.checks.admit(d, i).await
    }
    async fn current(&self, v: &Verified) -> Result<()> {
        self.checks.current(v).await
    }
    async fn support(&self, v: &Verified) -> Result<SupportSummary> {
        self.checks.support(v).await
    }
    fn preparation_ready(&self) -> Result<bool> {
        Ok(self.available.load(Ordering::SeqCst))
    }
    async fn populate(
        &self,
        _: &Interest,
        _: Instant,
        _: &mut catalog::EarlyBrowse<'_>,
    ) -> Result<()> {
        self.populated.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn evidence_sources(&self, v: &Verified, i: &Interest) -> Result<Vec<EvidenceOrigin>> {
        if self.invalid_origins.load(Ordering::SeqCst) {
            Ok(vec![EvidenceOrigin::Cache, EvidenceOrigin::Cache])
        } else {
            self.checks.evidence_sources(v, i).await
        }
    }
    async fn evidence(
        &self,
        v: &Verified,
        s: &EvidenceStep,
        i: &Interest,
    ) -> Result<Option<EvidenceObservation>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.hold.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            self.resume.notified().await;
        }
        self.checks.evidence(v, s, i).await
    }
}
async fn held(service: &SearchService, backend: &Arc<Later>, prepare: bool) -> Result<Uuid> {
    let mut r = request("example");
    if prepare {
        r = request("");
        r.prepare_catalog = true;
    }
    let id = service.start_with(backend.clone(), r)?;
    timeout(Duration::from_secs(3), backend.started.notified()).await?;
    Ok(id)
}

#[tokio::test]
async fn admitted_page_is_ready_before_optional_ranking_and_later_merges_only_verified_scores()
-> Result<()> {
    let backend = Arc::new(Later::new()?);
    let service = SearchService::default();
    let id = held(&service, &backend, false).await?;
    let first = service.view(id).await?;
    assert_eq!(first.phase, SearchPhase::Ready);
    assert_eq!(first.checked, 2);
    assert_eq!(first.pending, 0);
    assert_eq!(first.results.len(), 2);
    assert_eq!(first.results[0].name, "Example 1");
    assert_eq!(first.results[0].support.verified_supporters, 5);
    assert!(first.ranking_partial && first.metadata_only && !first.network_complete);
    assert!(
        service.inner.slot.lock().unwrap().running,
        "ready is not a retirement acknowledgement"
    );
    backend.resume.notify_one();
    service.join().await?;
    let enriched = service.view(id).await?;
    assert_eq!(enriched.phase, SearchPhase::Ready);
    assert_eq!(enriched.query_id, first.query_id);
    assert_eq!(enriched.results[0].name, "Example 0");
    assert_eq!(enriched.results[0].support.verified_supporters, 69);
    assert_eq!(enriched.results[0].support.weighted_score, 69);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    service.request_stop();
    Ok(())
}

#[tokio::test]
async fn replacement_waits_for_original_enrichment_retirement_without_hiding_ready_cards()
-> Result<()> {
    let backend = Arc::new(Later::new()?);
    let service = SearchService::default();
    let old = held(&service, &backend, false).await?;
    assert_eq!(service.view(old).await?.phase, SearchPhase::Ready);
    let next = Arc::new(Fake::new(vec![descriptor(2, 1)?]));
    let latest = service.start_with(next, request("example 2"))?;
    assert!(service.view(old).await.is_err());
    assert_eq!(service.view(latest).await?.phase, SearchPhase::Checking);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    backend.resume.notify_one();
    let page = ready(&service, latest).await?;
    assert_eq!(page.results.len(), 1);
    assert_eq!(page.results[0].name, "Example 2");
    service.request_stop();
    service.join().await?;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn setup_enrichment_and_catalog_share_one_nonrenewable_deadline_even_with_fresh_polls()
-> Result<()> {
    let backend = Arc::new(Later::new()?);
    let service = SearchService::default();
    let id = held(&service, &backend, true).await?;
    let first = service.view(id).await?;
    assert_eq!(first.phase, SearchPhase::Ready);
    assert!(!first.preparation.unwrap().local_pass_complete);
    let until = service
        .inner
        .slot
        .lock()
        .unwrap()
        .query
        .as_ref()
        .unwrap()
        .preparation_until
        .unwrap();
    tokio::time::advance(catalog::LIFETIME - Duration::from_secs(1)).await;
    // Reading may renew interactive interest, never metadata or background work.
    let stale = service.view(id).await?;
    assert_eq!(stale.phase, SearchPhase::Stale);
    assert!(stale.results.is_empty());
    assert_eq!(
        service
            .inner
            .slot
            .lock()
            .unwrap()
            .query
            .as_ref()
            .unwrap()
            .preparation_until,
        Some(until)
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    let interest = Interest {
        inner: Arc::downgrade(&service.inner),
        id,
    };
    assert!(
        !interest.active(),
        "a fresh UI poll cannot defeat the fixed setup deadline"
    );
    backend.resume.notify_one();
    service.join().await?;
    let view = service.view(id).await?;
    assert!(matches!(
        view.preparation.unwrap().phase,
        catalog::Phase::Limited
    ));
    assert_eq!(
        backend.populated.load(Ordering::SeqCst),
        0,
        "catalog work cannot start a second one-hour window"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert!(
        service
            .inner
            .slot
            .lock()
            .unwrap()
            .query
            .as_ref()
            .unwrap()
            .preparation_until
            .is_none()
    );
    service.request_stop();
    Ok(())
}

#[tokio::test]
async fn invalid_optional_origins_cannot_erase_an_independently_verified_page() -> Result<()> {
    let backend = Arc::new(Later::new()?);
    backend.invalid_origins.store(true, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    service.join().await?;
    let view = service.view(id).await?;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 2);
    assert_eq!(view.results[0].support.verified_supporters, 5);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    service.request_stop();
    Ok(())
}

#[tokio::test]
async fn temporary_pressure_skips_new_optional_batches_but_retires_an_already_owned_one()
-> Result<()> {
    let backend = Arc::new(Later::new()?);
    let service = SearchService::default();
    let id = held(&service, &backend, false).await?;
    backend.available.store(false, Ordering::SeqCst);
    assert_eq!(service.view(id).await?.phase, SearchPhase::Ready);
    backend.resume.notify_one();
    service.join().await?;
    assert_eq!(service.view(id).await?.results.len(), 2);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    service.request_stop();
    Ok(())
}

#[tokio::test]
async fn owner_loss_during_optional_work_still_invalidates_cards_and_stop_joins_the_original()
-> Result<()> {
    let backend = Arc::new(Later::new()?);
    let service = SearchService::default();
    let id = held(&service, &backend, false).await?;
    backend.checks.current.store(false, Ordering::SeqCst);
    let lost = service.view(id).await?;
    assert_eq!(lost.phase, SearchPhase::Stale);
    assert!(lost.results.is_empty());
    service.request_stop();
    assert!(service.view(id).await.is_err());
    assert!(
        timeout(Duration::from_millis(20), service.join())
            .await
            .is_err(),
        "ready must not detach or abort the owned evidence operation"
    );
    backend.resume.notify_one();
    service.join().await?;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    Ok(())
}
