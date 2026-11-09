//! Task/OS-lock ownership tests with synthetic values, NOT native retirement
//! evidence. The explicit preparation/native_tests cases exercise real work.
use super::*;
use crate::marketplace::lease::ControllerLease;
use std::time::Duration;
use tokio::{sync::oneshot, time::timeout};

#[tokio::test]
async fn lost_waiter_retains_the_original_task_result_and_actual_os_owner() -> Result<()> {
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("owned-operation");
    let lease = ControllerLease::acquire(&lane)?;
    let (complete, release) = oneshot::channel();
    let mut task = OwnedTask::new(tokio::spawn(async move {
        let _ = release.await;
        drop(lease);
        17
    }));
    assert!(
        timeout(Duration::from_millis(20), task.join())
            .await
            .is_err()
    );
    assert!(task.handle.is_some());
    assert!(task.result.is_none());
    assert!(ControllerLease::acquire(&lane).is_err());
    complete
        .send(())
        .map_err(|_| anyhow::anyhow!("test task lost"))?;
    assert_eq!(*task.join().await?, 17);
    assert!(task.handle.is_none());
    assert_eq!(*task.join().await?, 17);
    drop(ControllerLease::acquire(&lane)?);
    Ok(())
}

#[tokio::test]
async fn a_panicked_original_task_cannot_be_replaced_by_a_free_os_lock() -> Result<()> {
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("panicked-operation");
    let lease = ControllerLease::acquire(&lane)?;
    let mut task: OwnedTask<()> = OwnedTask::new(tokio::spawn(async move {
        let _lease = lease;
        panic!("explicit synthetic operation owner panic");
    }));
    assert!(task.join().await.is_err());
    assert!(task.failed);
    assert!(task.handle.is_none());
    assert!(task.result.is_none());
    drop(ControllerLease::acquire(&lane)?);
    assert!(task.join().await.is_err(), "a free lock is not a result");
    assert!(task.result.is_none());
    Ok(())
}

#[tokio::test]
async fn dropping_a_task_slot_never_aborts_its_original_owned_work() -> Result<()> {
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("dropped-operation");
    let lease = ControllerLease::acquire(&lane)?;
    let (release, complete) = oneshot::channel();
    let (done, finished) = oneshot::channel();
    let task = OwnedTask::new(tokio::spawn(async move {
        let _ = complete.await;
        drop(lease);
        let _ = done.send(());
    }));
    drop(task);
    assert!(ControllerLease::acquire(&lane).is_err());
    release
        .send(())
        .map_err(|_| anyhow::anyhow!("test owner aborted"))?;
    timeout(Duration::from_secs(1), finished).await??;
    drop(ControllerLease::acquire(&lane)?);
    Ok(())
}

#[test]
fn owned_operation_and_its_polling_future_keep_bounded_stack_ownership() {
    fn future_size<F: std::future::Future>(
        _: impl FnOnce(
            PreparedSource,
            InstalledController,
            SourceExecutionLimits,
            watch::Receiver<bool>,
            watch::Sender<Phase>,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    assert!(std::mem::size_of::<OwnedCandidate>() <= 16 * 1024);
    let bytes = future_size(run);
    assert!(
        bytes <= 128 * 1024,
        "owned operation future exceeds bound: {bytes}"
    );
}

#[test]
fn owned_source_continuation_keeps_bounded_stack_ownership() {
    fn future_size<F: std::future::Future>(
        _: impl FnOnce(
            SourceLaunch,
            InstalledController,
            SourceExecutionLimits,
            watch::Receiver<bool>,
            watch::Sender<Phase>,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    assert!(std::mem::size_of::<SourceLaunch>() <= 16 * 1024);
    let bytes = future_size(run_source);
    assert!(
        bytes <= 128 * 1024,
        "owned source continuation future exceeds bound: {bytes}"
    );
}

#[test]
fn owned_activation_and_sql_continuations_keep_bounded_stack_ownership() {
    fn frame<F: std::future::Future>(
        _: impl FnOnce(
            ActivationRequest,
            InstalledController,
            SourceExecutionLimits,
            watch::Receiver<bool>,
            watch::Sender<Phase>,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    fn sql_frame<F: std::future::Future>(
        _: impl FnOnce(&'static mut ActivationRequest) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    // Type-only checks, never fabricated source/controller/claim values.
    assert!(std::mem::size_of::<ActivationRequest>() <= 16 * 1024);
    let owned = frame(run_activation);
    let prepare = sql_frame(|request| request.prepare());
    let claim = sql_frame(|request| request.claim());
    assert!(owned <= 128 * 1024, "owned activation frame: {owned}");
    assert!(prepare <= 128 * 1024, "owned prepare SQL frame: {prepare}");
    assert!(claim <= 128 * 1024, "owned claim SQL frame: {claim}");
}
