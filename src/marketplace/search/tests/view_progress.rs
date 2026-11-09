//! Presentation progress can advance after a view's awaited security checks,
//! but only when the original query and exact observation objects are retained.
use super::*;

async fn checking() -> Result<(Arc<SearchService>, Arc<Fake>, Uuid)> {
    let service = Arc::new(SearchService::default());
    let backend = Arc::new(Fake::new(vec![descriptor(0, 1)?]));
    let id = service.start_with(backend.clone(), request("example"))?;
    service.join().await?;
    {
        let mut slot = service.inner.slot.lock().unwrap();
        let query = slot.query.as_mut().unwrap();
        assert_eq!(query.phase, SearchPhase::Ready);
        assert_eq!(query.values.len(), 1);
        // Deterministically pause the final page boundary, not a wall-clock
        // race or synthetic native/source receipt supplied by a wire caller.
        query.phase = SearchPhase::Checking;
        query.total = 2;
        query.checked = 1;
    }
    backend.hold_current.store(true, Ordering::SeqCst);
    Ok((service, backend, id))
}

fn complete(service: &SearchService) {
    let mut slot = service.inner.slot.lock().unwrap();
    let query = slot.query.as_mut().unwrap();
    query.phase = SearchPhase::Ready;
    query.checked = 2;
    query.omitted = 1;
    query.next_cursor = Some("same_page_continuation".into());
}

#[tokio::test]
async fn completion_during_awaited_current_check_needs_no_extra_progress_poll() -> Result<()> {
    let (service, backend, id) = checking().await?;
    let pending = tokio::spawn({
        let service = service.clone();
        async move { service.view(id).await }
    });
    backend.current_started.notified().await;
    complete(&service);
    backend.current_resume.notify_one();
    let view = pending.await??;
    assert_eq!(view.phase, SearchPhase::Ready);
    assert_eq!(view.results.len(), 1);
    assert_eq!((view.checked, view.omitted, view.pending), (2, 1, 0));
    assert_eq!(view.next_cursor.as_deref(), Some("same_page_continuation"));
    assert!(view.local_page_complete);
    assert!(!view.network_complete);
    assert!(view.ranking_partial && view.metadata_only);
    assert!(view.valid_for_seconds <= 299);
    Ok(())
}

#[tokio::test]
async fn appended_or_replaced_observations_cannot_borrow_the_old_views_checks() -> Result<()> {
    for append in [true, false] {
        let (service, backend, id) = checking().await?;
        let pending = tokio::spawn({
            let service = service.clone();
            async move { service.view(id).await }
        });
        backend.current_started.notified().await;
        let candidate = if append {
            descriptor(1, 1)?
        } else {
            service
                .inner
                .slot
                .lock()
                .unwrap()
                .query
                .as_ref()
                .unwrap()
                .values[0]
                .metadata
                .descriptor()
                .clone()
        };
        let observation = backend
            .admit(
                candidate,
                &Interest {
                    inner: Arc::downgrade(&service.inner),
                    id,
                },
            )
            .await?;
        complete(&service);
        {
            let mut slot = service.inner.slot.lock().unwrap();
            let query = slot.query.as_mut().unwrap();
            if append {
                query.values.push(observation);
                query.omitted = 0;
            } else {
                // Even equal signed bytes are a different native/source fact.
                assert!(!Arc::ptr_eq(&query.values[0], &observation));
                query.values[0] = observation;
            }
        }
        backend.current_resume.notify_one();
        let view = pending.await??;
        assert_eq!(view.phase, SearchPhase::Checking);
        assert_eq!(view.results.len(), 1);
        assert_eq!((view.checked, view.omitted, view.pending), (1, 0, 1));
        assert!(view.next_cursor.is_none());
        assert!(!view.local_page_complete);
        let fresh = service.view(id).await?;
        assert_eq!(fresh.phase, SearchPhase::Ready);
        assert_eq!(fresh.results.len(), if append { 2 } else { 1 });
    }
    Ok(())
}

#[tokio::test]
async fn completed_progress_never_overrides_a_failed_current_check() -> Result<()> {
    let (service, backend, id) = checking().await?;
    let pending = tokio::spawn({
        let service = service.clone();
        async move { service.view(id).await }
    });
    backend.current_started.notified().await;
    complete(&service);
    backend.current.store(false, Ordering::SeqCst);
    backend.current_resume.notify_one();
    let view = pending.await??;
    assert_eq!(view.phase, SearchPhase::Stale);
    assert!(view.results.is_empty());
    assert!(!view.network_complete);
    Ok(())
}

#[tokio::test]
async fn replacement_or_stop_during_current_check_rejects_the_original_view() -> Result<()> {
    for replace in [false, true] {
        let (service, backend, id) = checking().await?;
        let pending = tokio::spawn({
            let service = service.clone();
            async move { service.view(id).await }
        });
        backend.current_started.notified().await;
        if replace {
            service.start_with(backend.clone(), request("replacement"))?;
        } else {
            service.request_stop();
        }
        backend.current_resume.notify_one();
        assert!(pending.await?.is_err());
        service.request_stop();
        service.join().await?;
    }
    Ok(())
}
