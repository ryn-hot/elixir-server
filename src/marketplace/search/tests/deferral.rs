//! Scheduling only: real fixture signatures, synthetic native/source owners.
//! No public source, actual host-pressure or protected network qualification.
use super::*;

#[derive(Clone, Copy)]
enum WaitingFor {
    Helper,
    LocalQueue,
    UntrustedText,
}
struct Deferred {
    checks: Fake,
    available: AtomicBool,
    stopped: AtomicBool,
    cached: bool,
    calls: AtomicUsize,
    rejected: AtomicUsize,
    started: Notify,
    waiting_for: WaitingFor,
}
impl Deferred {
    fn new(cached: bool) -> Result<Self> {
        Ok(Self {
            checks: Fake::new(vec![descriptor(1, 1)?]),
            available: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            cached,
            calls: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
            started: Notify::new(),
            waiting_for: WaitingFor::Helper,
        })
    }
}
#[async_trait]
impl Backend for Deferred {
    async fn page(&self, r: &SearchRequest, i: &Interest) -> Result<SearchPage> {
        self.checks.page(r, i).await
    }
    fn preparation_ready(&self) -> Result<bool> {
        if self.stopped.load(Ordering::SeqCst) {
            bail!("synthetic owner stopped");
        }
        Ok(
            !matches!(self.waiting_for, WaitingFor::Helper)
                || self.available.load(Ordering::SeqCst),
        )
    }
    async fn admit(&self, d: CandidateDescriptor, i: &Interest) -> Result<Arc<Verified>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if !self.cached && !self.available.load(Ordering::SeqCst) {
            match self.waiting_for {
                WaitingFor::Helper => bail!("synthetic helper deferral"),
                WaitingFor::LocalQueue => return Err(ParticipationClient::fixture_queue_busy()),
                WaitingFor::UntrustedText => bail!("marketplace request slot is unavailable"),
            }
        }
        self.checks.admit(d, i).await
    }
    async fn admission_result(&self, _: Digest, feedback: AdmissionFeedback) -> Result<()> {
        if feedback != AdmissionFeedback::Accepted {
            self.rejected.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    async fn current(&self, v: &Verified) -> Result<()> {
        self.checks.current(v).await
    }
}
#[tokio::test(start_paused = true)]
async fn known_local_queue_pressure_waits_without_omitting_a_card_or_retiring_its_peer()
-> Result<()> {
    let mut backend = Deferred::new(false)?;
    backend.waiting_for = WaitingFor::LocalQueue;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    let pending = service.view(id).await?;
    assert_eq!(pending.phase, SearchPhase::Checking);
    assert_eq!(
        (
            pending.checked,
            pending.omitted,
            backend.rejected.load(Ordering::SeqCst)
        ),
        (0, 0, 0)
    );
    backend.available.store(true, Ordering::SeqCst);
    let view = ready(&service, id).await?;
    assert_eq!(view.results.len(), 1);
    assert_eq!(view.omitted, 0);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn queued_pressure_replacement_retires_the_same_waiter_without_a_negative_verdict()
-> Result<()> {
    let mut backend = Deferred::new(false)?;
    backend.waiting_for = WaitingFor::LocalQueue;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let old = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    let latest = service.start_with(
        Arc::new(Fake::new(vec![descriptor(2, 1)?])),
        request("example"),
    )?;
    assert_eq!(ready(&service, latest).await?.results.len(), 1);
    assert!(service.view(old).await.is_err());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn local_queue_wait_has_bounded_polling_and_expires_with_original_interest() -> Result<()> {
    let mut backend = Deferred::new(false)?;
    backend.waiting_for = WaitingFor::LocalQueue;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    for _ in 0..31 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    service.join().await?;
    assert_eq!(service.view(id).await?.phase, SearchPhase::Unavailable);
    assert!(backend.calls.load(Ordering::SeqCst) <= 125);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn matching_peer_text_is_not_local_queue_deferral_provenance() -> Result<()> {
    let mut backend = Deferred::new(false)?;
    backend.waiting_for = WaitingFor::UntrustedText;
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    let view = ready(&service, id).await?;
    assert_eq!(view.phase, SearchPhase::Unavailable);
    assert_eq!(view.omitted, 1);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn temporary_helper_pressure_retries_the_same_candidate_without_a_negative_source_verdict()
-> Result<()> {
    let backend = Arc::new(Deferred::new(false)?);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let pending = service.view(id).await?;
    assert_eq!(pending.phase, SearchPhase::Checking);
    assert_eq!(pending.checked, 0);
    assert_eq!(pending.omitted, 0);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
    backend.available.store(true, Ordering::SeqCst);
    let view = ready(&service, id).await?;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 1);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn replacing_a_resource_wait_neither_retires_its_source_nor_blocks_the_latest_query()
-> Result<()> {
    let backend = Arc::new(Deferred::new(false)?);
    let service = SearchService::default();
    let old = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    let latest = service.start_with(
        Arc::new(Fake::new(vec![descriptor(2, 1)?])),
        request("example"),
    )?;
    let view = ready(&service, latest).await?;
    assert_eq!(view.results.len(), 1);
    assert!(service.view(old).await.is_err());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn current_cached_facts_do_not_wait_for_a_new_helpers_resource_admission() -> Result<()> {
    let backend = Arc::new(Deferred::new(true)?);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    assert_eq!(ready(&service, id).await?.results.len(), 1);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn unattended_resource_wait_expires_with_its_existing_interest_without_new_work() -> Result<()>
{
    let backend = Arc::new(Deferred::new(false)?);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    tokio::time::advance(Duration::from_secs(31)).await;
    service.join().await?; // do not renew interest through view() before retirement
    assert_eq!(service.view(id).await?.phase, SearchPhase::Unavailable);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
    service.request_stop();
    service.join().await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn a_failed_owner_ends_the_wait_without_creating_a_negative_source_verdict() -> Result<()> {
    for waiting_for in [WaitingFor::Helper, WaitingFor::LocalQueue] {
        let mut backend = Deferred::new(false)?;
        backend.waiting_for = waiting_for;
        let backend = Arc::new(backend);
        let service = SearchService::default();
        let id = service.start_with(backend.clone(), request("example"))?;
        backend.started.notified().await;
        backend.stopped.store(true, Ordering::SeqCst);
        assert_eq!(ready(&service, id).await?.phase, SearchPhase::Unavailable);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(backend.rejected.load(Ordering::SeqCst), 0);
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn completed_bad_metadata_still_gets_its_normal_single_negative_verdict() -> Result<()> {
    let mut backend = Deferred::new(false)?;
    backend.available.store(true, Ordering::SeqCst);
    backend.checks.page.results[0].descriptor.head.signature = "A".repeat(86);
    let backend = Arc::new(backend);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    let view = ready(&service, id).await?;
    assert_eq!(view.phase, SearchPhase::Unavailable);
    assert_eq!(view.checked, 1);
    assert_eq!(view.omitted, 1);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.rejected.load(Ordering::SeqCst), 1);
    service.request_stop();
    service.join().await?;
    Ok(())
}
