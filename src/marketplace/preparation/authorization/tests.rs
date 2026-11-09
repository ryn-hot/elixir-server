//! Structured concurrency tests, not native source/protection evidence.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
struct State {
    calls: AtomicUsize,
    fail: AtomicBool,
    stalled: AtomicBool,
    release: Notify,
    completed: AtomicUsize,
    dropped: AtomicUsize,
}
struct Fake(Arc<State>);
struct Held(Arc<State>);
impl Drop for Held {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Check for Fake {
    async fn check(&self) -> Result<()> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        let _held = Held(self.0.clone());
        if self.0.stalled.load(Ordering::SeqCst) {
            self.0.release.notified().await;
        }
        self.0.completed.fetch_add(1, Ordering::SeqCst);
        if self.0.fail.load(Ordering::SeqCst) {
            bail!("synthetic authorization withdrawal");
        }
        Ok(())
    }
}
async fn until(condition: impl Fn() -> bool) -> Result<()> {
    for _ in 0..100 {
        if condition() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    bail!("original monitor did not reach the expected boundary")
}
#[tokio::test(start_paused = true)]
async fn withdrawal_is_observed_during_an_unfinished_work_step() -> Result<()> {
    let state = Arc::new(State::default());
    let (stop, stopped) = watch::channel(false);
    let (failed, mut failure) = watch::channel(false);
    let owner = state.clone();
    let task = tokio::spawn(monitor(Some(Fake(owner)), stopped, failed));
    until(|| state.completed.load(Ordering::SeqCst) == 1).await?;
    state.fail.store(true, Ordering::SeqCst);
    tokio::time::advance(CHECK_INTERVAL).await;
    until(|| *failure.borrow()).await?;
    assert!(task.await?.is_err());
    assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    drop(stop);
    failure.changed().await.ok();
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn a_deadline_signals_stop_without_dropping_or_replacing_the_original_check() -> Result<()> {
    let state = Arc::new(State::default());
    state.stalled.store(true, Ordering::SeqCst);
    let (stop, stopped) = watch::channel(false);
    let (failed, failure) = watch::channel(false);
    let task = tokio::spawn(monitor(Some(Fake(state.clone())), stopped, failed));
    until(|| state.calls.load(Ordering::SeqCst) == 1).await?;
    tokio::time::advance(CHECK_DEADLINE + Duration::from_secs(1)).await;
    until(|| *failure.borrow()).await?;
    assert!(!task.is_finished());
    assert_eq!(state.dropped.load(Ordering::SeqCst), 0);
    stop.send_replace(true);
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    state.release.notify_one();
    assert!(task.await?.is_err());
    assert_eq!(state.completed.load(Ordering::SeqCst), 1);
    assert_eq!(state.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn normal_completion_joins_an_inflight_check_and_preserves_a_late_withdrawal() -> Result<()> {
    let state = Arc::new(State::default());
    state.stalled.store(true, Ordering::SeqCst);
    let (stop, stopped) = watch::channel(false);
    let (failed, _failure) = watch::channel(false);
    let task = tokio::spawn(monitor(Some(Fake(state.clone())), stopped, failed));
    until(|| state.calls.load(Ordering::SeqCst) == 1).await?;
    state.fail.store(true, Ordering::SeqCst);
    stop.send_replace(true);
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    assert_eq!(state.dropped.load(Ordering::SeqCst), 0);
    state.release.notify_one();
    assert!(task.await?.is_err());
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.completed.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn absent_session_monitor_waits_for_its_owner_without_polling_or_claiming_failure()
-> Result<()> {
    let (stop, stopped) = watch::channel(false);
    let (failed, failure) = watch::channel(false);
    let task = tokio::spawn(monitor::<Fake>(None, stopped, failed));
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(!task.is_finished());
    assert!(!*failure.borrow());
    stop.send_replace(true);
    task.await??;
    Ok(())
}
