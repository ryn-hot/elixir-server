//! A page the owner keeps polling stays current: each card's metadata is
//! verified again before its checks expire. Nothing renews an unwatched page,
//! a selection, or an expired card, and replacement is never delayed.
use super::*;

const POLL: Duration = Duration::from_secs(25);

async fn viewed(backend: &Arc<Fake>) -> Result<(SearchService, Uuid)> {
    backend.renewing.store(true, Ordering::SeqCst);
    let service = SearchService::default();
    let id = service.start_with(backend.clone(), request("example"))?;
    assert_eq!(ready(&service, id).await?.results.len(), 2);
    Ok((service, id))
}
impl Fake {
    /// What the live guard does periodically: a new protection window.
    fn protect(&self) {
        for owner in self.owners.lock().unwrap().iter() {
            owner(true);
        }
    }
}
fn two() -> Result<Arc<Fake>> {
    Ok(Arc::new(Fake::new(vec![descriptor(0, 1)?, descriptor(1, 1)?])))
}
async fn stop(service: SearchService) -> Result<()> {
    service.request_stop();
    service.join().await
}

#[tokio::test(start_paused = true)]
async fn a_polled_page_outlives_its_first_checks() -> Result<()> {
    let backend = two()?;
    let (service, id) = viewed(&backend).await?;
    // The renewal loop must leave the paused clock idle between steps; a loop
    // that never sleeps would hold this test until real time ran out.
    let real = std::time::Instant::now();
    // Twice the 300-s check lifetime, polled at the app's ready-page cadence.
    for _ in 0..24 {
        tokio::time::sleep(POLL).await;
        // The live guard keeps renewing protection; the fixture's window is
        // a fixed 600 s, which is not what this test measures.
        backend.protect();
        let view = service.view(id).await?;
        assert_eq!(view.phase, SearchPhase::Ready);
        assert_eq!(view.results.len(), 2);
        assert!(view.valid_for_seconds >= 90, "{}", view.valid_for_seconds);
    }
    // Each card renewed in each of the later lifetimes, never one per poll.
    let renewals = backend.renewals.load(Ordering::SeqCst);
    assert!((4..=8).contains(&renewals), "{renewals}");
    assert!(real.elapsed() < Duration::from_secs(30), "{:?}", real.elapsed());
    stop(service).await
}

#[tokio::test(start_paused = true)]
async fn an_unwatched_page_is_not_renewed_and_still_expires() -> Result<()> {
    let backend = two()?;
    let (service, id) = viewed(&backend).await?;
    tokio::time::sleep(Duration::from_secs(301)).await;
    assert_eq!(backend.renewals.load(Ordering::SeqCst), 0);
    let view = service.view(id).await?;
    assert!(view.results.is_empty());
    assert_eq!(view.phase, SearchPhase::Stale);
    assert!(!service.inner.slot.lock().unwrap().running);
    stop(service).await
}

#[tokio::test(start_paused = true)]
async fn a_returning_viewer_resumes_renewal() -> Result<()> {
    let backend = two()?;
    let (service, id) = viewed(&backend).await?;
    tokio::time::sleep(Duration::from_secs(100)).await;
    assert!(!service.inner.slot.lock().unwrap().running, "lease lapsed");
    for _ in 0..16 {
        let view = service.view(id).await?;
        assert_eq!(view.results.len(), 2);
        tokio::time::sleep(POLL).await;
    }
    assert!(backend.renewals.load(Ordering::SeqCst) >= 2);
    stop(service).await
}

#[tokio::test(start_paused = true)]
async fn a_new_search_is_not_held_by_a_renewal_wait() -> Result<()> {
    let backend = two()?;
    let (service, first) = viewed(&backend).await?;
    service.view(first).await?;
    tokio::task::yield_now().await;
    let began = Instant::now();
    let second = service.start_with(backend.clone(), request("example"))?;
    assert_eq!(ready(&service, second).await?.results.len(), 2);
    assert!(began.elapsed() < Duration::from_secs(1), "{:?}", began.elapsed());
    stop(service).await
}

#[tokio::test(start_paused = true)]
async fn no_renewal_runs_while_a_release_is_selected() -> Result<()> {
    let backend = two()?;
    let (service, id) = viewed(&backend).await?;
    let selected = service.selections.clone().try_acquire_owned()?;
    for _ in 0..10 {
        tokio::time::sleep(POLL).await;
        service.view(id).await?;
    }
    assert_eq!(backend.renewals.load(Ordering::SeqCst), 0);
    drop(selected);
    for _ in 0..4 {
        tokio::time::sleep(POLL).await;
        assert_eq!(service.view(id).await?.results.len(), 2);
    }
    assert!(backend.renewals.load(Ordering::SeqCst) >= 2);
    stop(service).await
}

#[tokio::test(start_paused = true)]
async fn a_failing_renewal_retries_at_a_bounded_rate_and_the_card_still_expires() -> Result<()> {
    let backend = two()?;
    backend.fail_renewal.store(true, Ordering::SeqCst);
    let (service, id) = viewed(&backend).await?;
    for _ in 0..12 {
        tokio::time::sleep(POLL).await;
        service.view(id).await?;
    }
    // 120 s below the lead at one attempt per 5 s, not a busy loop.
    let renewals = backend.renewals.load(Ordering::SeqCst);
    assert!((1..=30).contains(&renewals), "{renewals}");
    let view = service.view(id).await?;
    assert!(view.results.is_empty());
    assert_eq!(view.phase, SearchPhase::Stale);
    stop(service).await
}
