//! Owned-task/backpressure/negative-boundary tests. No synthetic completion
//! below contains a controller or positive native cleanup proof.
use super::*;
use serde_json::json;
use std::{future::Future, mem::size_of};

fn extension() -> ExtensionId {
    crate::marketplace::grants::fixture_review()
        .unwrap()
        .scope()
        .extension_id()
        .clone()
}
fn uncertain() -> Completion {
    Completion {
        controller: None,
        result: Some(Ok(
            RuntimeJson::from_value(json!({"must_not_escape":true})).unwrap()
        )),
        cleanup: Err(anyhow::anyhow!("synthetic unconfirmed native boundary")),
    }
}
fn fixture(task: JoinHandle<Completion>, stop: watch::Sender<bool>) -> (Arc<Entry>, InstalledCall) {
    let consumer = Arc::new(());
    let entry = Arc::new(Entry {
        stop,
        consumer: Arc::downgrade(&consumer),
        task: AsyncMutex::new(Joined::Running(task)),
    });
    let call = InstalledCall {
        entry: entry.clone(),
        _consumer: consumer,
        turn: None,
    };
    (entry, call)
}

#[tokio::test]
async fn invalid_arguments_and_missing_original_owner_fail_before_any_call_is_reserved()
-> Result<()> {
    let service = Service::default();
    let allocation = super::super::fixture_allocation();
    for (home, instance, arguments) in [
        (
            Uuid::nil(),
            Uuid::new_v4(),
            RuntimeJson::from_value(json!({}))?,
        ),
        (
            Uuid::new_v4(),
            Uuid::nil(),
            RuntimeJson::from_value(json!({}))?,
        ),
        (
            Uuid::new_v4(),
            Uuid::new_v4(),
            RuntimeJson::from_value(json!("x".repeat(MAX_ARGUMENT_BYTES)))?,
        ),
    ] {
        let error = service
            .begin_installed_call(
                home,
                extension(),
                instance,
                arguments,
                allocation.installations,
                allocation.execution.clone(),
            )
            .err()
            .context("invalid call accepted")?;
        assert!(error.to_string().contains("invalid or oversized"));
        assert!(service.0.lock().unwrap().invocation.is_none());
    }
    let error = service
        .begin_installed_call(
            Uuid::new_v4(),
            extension(),
            Uuid::new_v4(),
            RuntimeJson::from_value(json!({}))?,
            allocation.installations,
            allocation.execution,
        )
        .err()
        .context("ownerless call accepted")?;
    assert!(
        error
            .to_string()
            .contains("no original installed controller")
    );
    assert!(service.0.lock().unwrap().invocation.is_none());
    let facade = crate::marketplace::service::MarketplaceService::default();
    assert!(
        facade
            .begin_installed_call(
                Uuid::new_v4(),
                extension(),
                Uuid::new_v4(),
                RuntimeJson::from_value(json!({}))?
            )
            .err()
            .context("unconfigured facade accepted a call")?
            .to_string()
            .contains("trusted startup")
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_call_waiter_retains_the_same_original_task_and_unconfirmed_output() -> Result<()>
{
    let queue = queue::Queue::default();
    let (release, wait) = tokio::sync::oneshot::channel();
    let (stop, _) = watch::channel(false);
    let (entry, mut call) = fixture(
        tokio::spawn(async move {
            wait.await.unwrap();
            uncertain()
        }),
        stop,
    );
    call.turn = Some(queue.acquire().await?);
    let service = Service::default();
    service.0.lock().unwrap().invocation = Some(entry.clone());
    assert!(
        timeout(Duration::from_millis(10), call.finish())
            .await
            .is_err()
    );
    assert!(matches!(&*entry.task.lock().await,Joined::Running(task) if !task.is_finished()));
    assert!(
        queue.try_acquire().is_err(),
        "cancelled waiter cannot release its call's turn"
    );
    assert!(
        !*entry.stop.borrow(),
        "cancelling only a borrow does not replace/stop its retained owner"
    );
    release.send(()).unwrap();
    assert!(
        call.finish().await.is_err(),
        "uncertain cleanup must not expose even a well-formed result"
    );
    assert!(matches!(&*entry.task.lock().await,Joined::Done(done) if done.result.is_some()));
    assert!(
        queue.try_acquire().is_err(),
        "unknown cleanup keeps the live consumer's turn"
    );
    drop(call);
    // Dropping scheduling coordination is NOT successful native retirement.
    // Even with a free turn the same original service slot must stay fenced.
    drop(queue.try_acquire()?);
    assert!(service.reap_invocation().is_err());
    assert!(Arc::ptr_eq(
        service.0.lock().unwrap().invocation.as_ref().unwrap(),
        &entry
    ));
    Ok(())
}

#[tokio::test]
async fn original_call_panic_is_sticky_and_cannot_rearm_an_empty_controller_slot() -> Result<()> {
    let (stop, _) = watch::channel(false);
    let (entry, mut call) = fixture(
        tokio::spawn(async { panic!("synthetic unknown installed call owner") }),
        stop,
    );
    let service = Service::default();
    service.0.lock().unwrap().invocation = Some(entry.clone());
    assert!(call.finish().await.is_err());
    drop(call);
    service.reap_invocation()?;
    assert!(matches!(*entry.task.lock().await, Joined::Failed));
    let allocation = super::super::fixture_allocation();
    let error = service
        .begin_installed_call(
            Uuid::new_v4(),
            extension(),
            Uuid::new_v4(),
            RuntimeJson::from_value(json!({}))?,
            allocation.installations,
            allocation.execution,
        )
        .err()
        .context("failed original slot was replaced")?;
    assert!(error.to_string().contains("still owned"));
    assert!(service.shutdown().await.is_err());
    assert!(Arc::ptr_eq(
        service.0.lock().unwrap().invocation.as_ref().unwrap(),
        &entry
    ));
    Ok(())
}

#[tokio::test]
async fn unconsumed_result_reserves_the_original_slot_and_drop_does_not_prove_cleanup() -> Result<()>
{
    let (stop, _) = watch::channel(false);
    let (entry, call) = fixture(tokio::spawn(async { uncertain() }), stop);
    let service = Service::default();
    service.0.lock().unwrap().invocation = Some(entry.clone());
    entry.task.lock().await.join().await?;
    service.reap_invocation()?;
    assert!(service.0.lock().unwrap().invocation.is_some());
    assert!(entry.consumer.upgrade().is_some());
    drop(call);
    assert!(entry.consumer.upgrade().is_none());
    assert!(service.reap_invocation().is_err());
    assert!(service.0.lock().unwrap().controller.is_none());
    Ok(())
}

#[tokio::test]
async fn dropping_the_last_application_owner_signals_even_a_live_consumer_handle() -> Result<()> {
    let (stop, mut stopped) = watch::channel(false);
    let (entry, mut call) = fixture(
        tokio::spawn(async move {
            super::stopped(&mut stopped).await;
            uncertain()
        }),
        stop,
    );
    let service = Service::default();
    service.0.lock().unwrap().invocation = Some(entry.clone());
    let same = service.clone();
    drop(service);
    assert!(
        !*entry.stop.borrow(),
        "dropping one shared facade must not stop the other"
    );
    drop(same);
    assert!(*entry.stop.borrow());
    assert!(call.finish().await.is_err());
    Ok(())
}

#[tokio::test]
async fn dropping_consumer_signals_and_retains_its_same_original_task_for_shutdown() -> Result<()> {
    let (stop, mut stopped) = watch::channel(false);
    let (entry, call) = fixture(
        tokio::spawn(async move {
            super::stopped(&mut stopped).await;
            uncertain()
        }),
        stop,
    );
    let service = Service::default();
    service.0.lock().unwrap().invocation = Some(entry.clone());
    drop(call);
    assert!(*entry.stop.borrow());
    assert!(
        service.shutdown().await.is_err(),
        "a missing original controller is not joined native cleanup"
    );
    assert!(matches!(&*entry.task.lock().await, Joined::Done(_)));
    assert!(Arc::ptr_eq(
        service.0.lock().unwrap().invocation.as_ref().unwrap(),
        &entry
    ));
    Ok(())
}

#[tokio::test]
async fn facade_shutdown_signals_network_before_waiting_for_installed_retirement() -> Result<()> {
    let facade = crate::marketplace::service::MarketplaceService::default();
    let service = facade.fixture_original_installation();
    let (release, wait) = tokio::sync::oneshot::channel();
    let (stop, mut stopped) = watch::channel(false);
    let (entry, call) = fixture(
        tokio::spawn(async move {
            super::stopped(&mut stopped).await;
            wait.await.unwrap();
            uncertain()
        }),
        stop,
    );
    service.0.lock().unwrap().invocation = Some(entry.clone());
    let mut shutdown = Box::pin(facade.shutdown());
    assert!(
        timeout(Duration::from_millis(10), shutdown.as_mut())
            .await
            .is_err()
    );
    assert!(*entry.stop.borrow());
    assert!(
        facade.stop_requested(),
        "slow installed retirement must not delay the network stop signal"
    );
    release.send(()).unwrap();
    assert!(
        shutdown.await.is_err(),
        "unknown native cleanup is not successful shutdown"
    );
    assert!(matches!(&*entry.task.lock().await, Joined::Done(_)));
    drop(call);
    Ok(())
}

fn future_size<F: Future>(
    _: fn(
        InstalledController,
        Request,
        InstallBudget,
        SourceExecutionLimits,
        watch::Receiver<bool>,
    ) -> F,
) -> usize {
    size_of::<F>()
}
#[test]
fn installed_application_call_keeps_the_existing_frame_and_heap_input_bounds() {
    let bytes = future_size(run);
    assert!(
        bytes <= 128 * 1024,
        "installed application future grew to {bytes} bytes"
    );
    assert!(size_of::<Request>() < 4096);
    assert!(size_of::<Completion>() < 16 * 1024);
    assert!(size_of::<InstalledCall>() < 1024);
}
