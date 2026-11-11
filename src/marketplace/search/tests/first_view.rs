use super::*;
use std::pin::pin;

#[tokio::test(start_paused = true)]
async fn first_interactive_view_waits_for_the_checked_page_without_waiting_for_a_poll() -> Result<()>
{
    let service = SearchService::default();
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?]));
    backend.hold_first.store(true, Ordering::SeqCst);
    let id = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    let began = Instant::now();
    let mut first = pin!(service.interactive_view(id));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.resume.notify_one();
    let view = first.await?;
    assert_eq!(began.elapsed(), Duration::ZERO);
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 1);
    assert!(!view.network_complete);

    backend.current.store(false, Ordering::SeqCst);
    let view = service.interactive_view(id).await?;
    assert_eq!(view.phase, SearchPhase::Stale);
    assert!(view.results.is_empty());
    service.request_stop();
    service.join().await
}

#[tokio::test(start_paused = true)]
async fn first_interactive_view_wait_is_bounded_and_cannot_be_claimed_twice() -> Result<()> {
    let service = SearchService::default();
    let backend = Arc::new(Fake::new(vec![]));
    backend.hold_first.store(true, Ordering::SeqCst);
    let id = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    let began = Instant::now();
    let mut first = pin!(service.interactive_view(id));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    let second = service.interactive_view(id).await?;
    assert_eq!(second.phase, SearchPhase::Checking);
    assert_eq!(began.elapsed(), Duration::ZERO);
    tokio::time::advance(FIRST_VIEW_WAIT).await;
    let partial = first.await?;
    assert_eq!(partial.phase, SearchPhase::Checking);
    assert_eq!(began.elapsed(), FIRST_VIEW_WAIT);
    service.request_stop();
    backend.resume.notify_one();
    service.join().await
}

#[tokio::test(start_paused = true)]
async fn replacement_wakes_waiter_and_setup_still_returns_immediately() -> Result<()> {
    let service = SearchService::default();
    let backend = Arc::new(Fake::new(vec![]));
    backend.hold_first.store(true, Ordering::SeqCst);
    let old_id = service.start_with(backend.clone(), request("example"))?;
    backend.started.notified().await;
    let began = Instant::now();
    let mut first = pin!(service.interactive_view(old_id));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    let mut setup = request("");
    setup.prepare_catalog = true;
    let new_id = service.start_with(Arc::new(Fake::new(vec![])), setup)?;
    assert!(first.await.is_err());
    let view = service.interactive_view(new_id).await?;
    assert_eq!(view.query_id, new_id);
    assert_eq!(view.phase, SearchPhase::Checking);
    assert!(view.preparation.is_some());
    assert_eq!(began.elapsed(), Duration::ZERO);
    service.request_stop();
    backend.resume.notify_one();
    service.join().await
}
