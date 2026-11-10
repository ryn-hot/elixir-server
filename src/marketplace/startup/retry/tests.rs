use super::*;
use crate::marketplace::worker::{ParticipationPhase, ParticipationSnapshot};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

struct Shared {
    status: Mutex<RuntimeSnapshot>,
    starts: Mutex<Vec<Instant>>,
    failures: AtomicUsize,
    stall_start: AtomicBool,
    stop_requested: AtomicBool,
    cancelled_starts: AtomicUsize,
    cancelled_preparations: AtomicUsize,
    pause_cancel_preparation: AtomicBool,
    fail_cancel_preparation: AtomicBool,
    retired: AtomicUsize,
    fail_retire: AtomicBool,
    pause_retire: AtomicBool,
    configuration_valid: AtomicBool,
    stall_configuration: AtomicBool,
    hold_invalidation: AtomicBool,
    configuration_checks: AtomicUsize,
    invalidations: AtomicUsize,
    checked: Notify,
    started: Notify,
    retiring: Notify,
    resume: Notify,
    progress: crate::marketplace::service::MarketplaceService,
    _progress: crate::marketplace::service::StartupOwner,
    retries: Mutex<Vec<Option<Duration>>>,
}
impl Shared {
    fn set(&self, phase: RuntimePhase, attached: bool) {
        *self.status.lock().unwrap() = RuntimeSnapshot {
            phase,
            participation: attached.then(|| ParticipationSnapshot {
                phase: if phase == RuntimePhase::Failed {
                    ParticipationPhase::Failed
                } else {
                    ParticipationPhase::Running
                },
                ..Default::default()
            }),
        };
    }
    fn count(&self) -> usize {
        self.starts.lock().unwrap().len()
    }
}
struct Fixture(Arc<Shared>);
struct CancelStart {
    state: Arc<Shared>,
    armed: bool,
}
impl Drop for CancelStart {
    fn drop(&mut self) {
        if self.armed {
            self.state.cancelled_starts.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Starter for Fixture {
    fn status(&self) -> RuntimeSnapshot {
        self.0.status.lock().unwrap().clone()
    }
    fn stop_requested(&self) -> bool {
        self.0.stop_requested.load(Ordering::SeqCst)
    }
    fn entropy(&self) -> u64 {
        0
    }
    async fn start(&mut self) -> Result<()> {
        self.0.starts.lock().unwrap().push(Instant::now());
        self.0.started.notify_one();
        self.0.set(RuntimePhase::Starting, false);
        let mut cancelled = CancelStart {
            state: self.0.clone(),
            armed: true,
        };
        if self.0.stall_start.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        cancelled.armed = false;
        if self
            .0
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            })
            .is_ok()
        {
            self.0.set(RuntimePhase::Failed, false);
            anyhow::bail!("fixture preparation unavailable");
        }
        self.0.set(RuntimePhase::Running, true);
        Ok(())
    }
    async fn retire(&mut self) -> Result<bool> {
        self.0.retired.fetch_add(1, Ordering::SeqCst);
        self.0.retiring.notify_one();
        if self.0.pause_retire.load(Ordering::SeqCst) {
            self.0.resume.notified().await;
        }
        if self.0.fail_retire.load(Ordering::SeqCst) {
            anyhow::bail!("fixture cleanup unknown");
        }
        self.0.set(RuntimePhase::NotConnected, false);
        Ok(true)
    }
    async fn cancel_preparation(&mut self) -> Result<()> {
        let status = self.status();
        if status.participation.is_some()
            || matches!(
                status.phase,
                RuntimePhase::NotConnected | RuntimePhase::Stopped
            )
        {
            return Ok(());
        }
        self.0.cancelled_preparations.fetch_add(1, Ordering::SeqCst);
        self.0.set(RuntimePhase::Failed, false);
        self.0.retiring.notify_one();
        if self.0.pause_cancel_preparation.load(Ordering::SeqCst) {
            self.0.resume.notified().await;
        }
        ensure!(
            !self.0.fail_cancel_preparation.load(Ordering::SeqCst),
            "fixture cancellation cleanup unknown"
        );
        self.0.set(RuntimePhase::Stopped, false);
        Ok(())
    }
    async fn configuration_current(&self) -> Result<()> {
        self.0.configuration_checks.fetch_add(1, Ordering::SeqCst);
        self.0.checked.notify_one();
        if self.0.stall_configuration.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        ensure!(
            self.0.configuration_valid.load(Ordering::SeqCst),
            "fixture connection changed"
        );
        Ok(())
    }
    fn invalidate_connection(&self) {
        self.0.invalidations.fetch_add(1, Ordering::SeqCst);
        if !self.0.hold_invalidation.load(Ordering::SeqCst) {
            self.0.set(RuntimePhase::Failed, true);
        }
    }
    async fn configuration_changed(&self) {
        self.0
            .progress
            .wait_connection_configuration_changed()
            .await;
    }
    fn retry_after(&self, delay: Option<Duration>) {
        self.0.retries.lock().unwrap().push(delay);
    }
}
fn fixture(failures: usize) -> (Fixture, Arc<Shared>) {
    let progress = crate::marketplace::service::MarketplaceService::default();
    progress.select_startup();
    let owner = progress
        .claim_startup_progress()
        .expect("fixture progress ownership");
    let shared = Arc::new(Shared {
        status: Mutex::new(RuntimeSnapshot {
            phase: RuntimePhase::NotConnected,
            participation: None,
        }),
        starts: Mutex::new(vec![]),
        failures: AtomicUsize::new(failures),
        stall_start: AtomicBool::new(false),
        stop_requested: AtomicBool::new(false),
        cancelled_starts: AtomicUsize::new(0),
        cancelled_preparations: AtomicUsize::new(0),
        pause_cancel_preparation: AtomicBool::new(false),
        fail_cancel_preparation: AtomicBool::new(false),
        retired: AtomicUsize::new(0),
        fail_retire: AtomicBool::new(false),
        pause_retire: AtomicBool::new(false),
        configuration_valid: AtomicBool::new(true),
        stall_configuration: AtomicBool::new(false),
        hold_invalidation: AtomicBool::new(false),
        configuration_checks: AtomicUsize::new(0),
        invalidations: AtomicUsize::new(0),
        checked: Notify::new(),
        started: Notify::new(),
        retiring: Notify::new(),
        resume: Notify::new(),
        progress,
        _progress: owner,
        retries: Mutex::new(vec![]),
    });
    (Fixture(shared.clone()), shared)
}

#[tokio::test(start_paused = true)]
async fn configuration_hint_shortens_existing_backoff_without_resetting_failure_escalation()
-> Result<()> {
    let (starter, state) = fixture(4);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    step(5).await;
    step(10).await;
    assert_eq!(state.count(), 3);
    assert_eq!(
        state.retries.lock().unwrap().last(),
        Some(&Some(Duration::from_secs(20)))
    );
    for _ in 0..10_000 {
        state.progress.connection_configuration_changed();
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    step(4).await;
    assert_eq!(
        state.count(),
        3,
        "hints cannot create an immediate second attempt"
    );
    step(1).await;
    assert_eq!(state.count(), 4);
    assert_eq!(
        state.retries.lock().unwrap().last(),
        Some(&Some(Duration::from_secs(40))),
        "explicit retry does not reset failures"
    );
    step(39).await;
    assert_eq!(
        state.count(),
        4,
        "coalesced hints do not accumulate across attempts"
    );
    step(1).await;
    assert_eq!(state.count(), 5);
    shutdown.cancel();
    task.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn configuration_hint_does_not_cancel_or_overlap_original_retirement() -> Result<()> {
    let (starter, state) = fixture(0);
    state.pause_retire.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    state.set(RuntimePhase::Failed, true);
    step(1).await;
    state.retiring.notified().await;
    for _ in 0..10_000 {
        state.progress.connection_configuration_changed();
    }
    step(10).await;
    assert_eq!(state.count(), 1);
    state.resume.notify_one();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    step(4).await;
    assert_eq!(state.count(), 1);
    step(1).await;
    assert_eq!(state.count(), 2);
    assert_eq!(state.retired.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    task.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn changed_saved_connection_waits_for_original_retirement_before_starting_a_replacement()
-> Result<()> {
    let (starter, state) = fixture(0);
    state.pause_retire.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.checked.notified().await;
    state.configuration_valid.store(false, Ordering::SeqCst);
    step(5).await;
    state.retiring.notified().await;
    assert_eq!(state.invalidations.load(Ordering::SeqCst), 1);
    assert_eq!(state.count(), 1);
    step(20).await;
    assert_eq!(
        state.count(),
        1,
        "no replacement overlaps an unfinished original retirement"
    );
    state.configuration_valid.store(true, Ordering::SeqCst);
    state.resume.notify_one();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    step(5).await;
    assert_eq!(state.count(), 2);
    assert_eq!(state.retired.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    task.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stalled_connection_check_invalidates_once_and_unknown_shutdown_never_starts_another_worker()
-> Result<()> {
    let (starter, state) = fixture(0);
    state.stall_configuration.store(true, Ordering::SeqCst);
    state.hold_invalidation.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown));
    state.checked.notified().await;
    step(9).await;
    assert_eq!(state.invalidations.load(Ordering::SeqCst), 0);
    step(1).await;
    assert_eq!(state.invalidations.load(Ordering::SeqCst), 1);
    step(30).await;
    assert_eq!(state.count(), 1);
    assert_eq!(state.configuration_checks.load(Ordering::SeqCst), 1);
    step(45).await;
    assert!(task.await?.is_err());
    assert_eq!(state.count(), 1);
    assert_eq!(state.retired.load(Ordering::SeqCst), 0);
    Ok(())
}
async fn step(seconds: u64) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
#[test]
fn backoff_is_bounded_jittered_and_reset_only_explicitly() {
    let mut low = Backoff::default();
    let mut high = Backoff::default();
    for (index, base) in [5, 10, 20, 40, 80, 160, 240].into_iter().enumerate() {
        assert_eq!(low.next(0), Duration::from_secs(base));
        let delay = high.next(u64::MAX);
        assert!(
            delay >= Duration::from_secs(base) && delay <= Duration::from_secs(base + base / 4)
        );
        assert!(low.step <= 6 && high.step <= 6, "{index}");
    }
    for _ in 0..1000 {
        assert!(high.next(u64::MAX) <= Duration::from_secs(300));
    }
    low.reset();
    assert_eq!(low.next(0), Duration::from_secs(5));
}
#[tokio::test(start_paused = true)]
async fn failed_preparations_wait_instead_of_retrying_or_allocating_a_queue() -> Result<()> {
    let (starter, state) = fixture(2);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    step(4).await;
    assert_eq!(state.count(), 1);
    step(1).await;
    assert_eq!(state.count(), 2);
    step(9).await;
    assert_eq!(state.count(), 2);
    step(1).await;
    assert_eq!(state.count(), 3);
    let times = state.starts.lock().unwrap().clone();
    assert_eq!(times[1] - times[0], Duration::from_secs(5));
    assert_eq!(times[2] - times[1], Duration::from_secs(10));
    shutdown.cancel();
    task.await??;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn an_activation_deadline_cancels_its_attempt_before_backoff_and_retry() -> Result<()> {
    let (starter, state) = fixture(0);
    state.stall_start.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    step(419).await;
    assert_eq!(state.count(), 1);
    assert_eq!(state.cancelled_starts.load(Ordering::SeqCst), 0);
    step(1).await;
    assert_eq!(state.cancelled_starts.load(Ordering::SeqCst), 1);
    step(4).await;
    assert_eq!(state.count(), 1);
    step(1).await;
    assert_eq!(state.count(), 2);
    shutdown.cancel();
    task.await??;
    assert_eq!(state.cancelled_starts.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn transient_failure_cleanup_does_not_terminate_the_retry_owner() -> Result<()> {
    let (starter, state) = fixture(0);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    state.set(RuntimePhase::Stopping, true);
    step(5).await;
    assert!(!task.is_finished());
    assert_eq!(state.count(), 1);
    assert_eq!(state.retired.load(Ordering::SeqCst), 0);
    state.set(RuntimePhase::Failed, true);
    step(1).await;
    state.retiring.notified().await;
    step(5).await;
    assert_eq!(state.count(), 2);
    state.stop_requested.store(true, Ordering::SeqCst);
    step(1).await;
    task.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_preempts_an_inflight_attempt_and_backoff_without_another_start() -> Result<()>
{
    for stall in [false, true] {
        let (starter, state) = fixture(99);
        state.stall_start.store(stall, Ordering::SeqCst);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(drive(starter, shutdown.clone()));
        state.started.notified().await;
        shutdown.cancel();
        task.await??;
        step(1000).await;
        assert_eq!(state.count(), 1);
        assert_eq!(
            state.cancelled_starts.load(Ordering::SeqCst),
            usize::from(stall)
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_joins_preparation_retirement_before_returning() -> Result<()> {
    let (starter, state) = fixture(0);
    state.stall_start.store(true, Ordering::SeqCst);
    state.pause_cancel_preparation.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    shutdown.cancel();
    state.retiring.notified().await;
    assert_eq!(state.cancelled_starts.load(Ordering::SeqCst), 1);
    assert!(!task.is_finished());
    step(74).await;
    assert!(!task.is_finished(), "original cleanup must be joined");
    state.resume.notify_one();
    task.await??;
    assert_eq!(state.cancelled_preparations.load(Ordering::SeqCst), 1);
    assert_eq!(state.count(), 1);
    assert_eq!(state.status.lock().unwrap().phase, RuntimePhase::Stopped);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_failed_backoff_also_joins_preparation() -> Result<()> {
    let (starter, state) = fixture(99);
    state.pause_cancel_preparation.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    shutdown.cancel();
    state.retiring.notified().await;
    assert!(!task.is_finished());
    state.resume.notify_one();
    task.await??;
    assert_eq!(state.cancelled_preparations.load(Ordering::SeqCst), 1);
    assert_eq!(state.count(), 1);
    assert_eq!(state.cancelled_starts.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_cancelled_preparation_is_terminal_without_stopped_or_retry() -> Result<()> {
    for stall in [false, true] {
        let (starter, state) = fixture(0);
        state.stall_start.store(true, Ordering::SeqCst);
        state
            .pause_cancel_preparation
            .store(stall, Ordering::SeqCst);
        state
            .fail_cancel_preparation
            .store(!stall, Ordering::SeqCst);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(drive(starter, shutdown.clone()));
        state.started.notified().await;
        shutdown.cancel();
        state.retiring.notified().await;
        if stall {
            step(75).await;
        }
        assert!(task.await?.is_err());
        assert_eq!(state.count(), 1);
        assert_eq!(state.status.lock().unwrap().phase, RuntimePhase::Failed);
        assert_eq!(state.cancelled_preparations.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn an_attached_cleanup_failure_is_terminal_not_an_ownership_retry() -> Result<()> {
    let (starter, state) = fixture(0);
    state.fail_retire.store(true, Ordering::SeqCst);
    let task = tokio::spawn(drive(starter, CancellationToken::new()));
    state.started.notified().await;
    state.set(RuntimePhase::Failed, true);
    step(1).await;
    assert!(task.await?.is_err());
    assert_eq!(state.count(), 1);
    assert_eq!(state.retired.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn shutdown_joins_retirement_and_never_starts_again_after_its_receipt() -> Result<()> {
    let (starter, state) = fixture(0);
    state.pause_retire.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    state.set(RuntimePhase::Failed, true);
    step(1).await;
    state.retiring.notified().await;
    shutdown.cancel();
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "consumed cleanup is joined, not cancelled"
    );
    state.resume.notify_one();
    task.await??;
    assert_eq!(state.count(), 1);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn stalled_retirement_has_a_deadline_without_a_false_receipt() -> Result<()> {
    let (starter, state) = fixture(0);
    state.pause_retire.store(true, Ordering::SeqCst);
    let task = tokio::spawn(drive(starter, CancellationToken::new()));
    state.started.notified().await;
    state.set(RuntimePhase::Failed, true);
    step(1).await;
    state.retiring.notified().await;
    let since = Instant::now();
    assert!(task.await?.is_err());
    assert_eq!(since.elapsed(), RETIRE_DEADLINE);
    assert_eq!(state.count(), 1);
    assert_eq!(state.status.lock().unwrap().phase, RuntimePhase::Failed);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn ten_continuously_running_minutes_reset_escalated_backoff() -> Result<()> {
    let (starter, state) = fixture(2);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(drive(starter, shutdown.clone()));
    state.started.notified().await;
    step(5).await;
    step(10).await;
    assert_eq!(state.count(), 3);
    step(601).await;
    state.set(RuntimePhase::Failed, true);
    step(1).await;
    state.retiring.notified().await;
    step(4).await;
    assert_eq!(state.count(), 3);
    step(1).await;
    assert_eq!(state.count(), 4);
    shutdown.cancel();
    task.await??;
    Ok(())
}
#[tokio::test]
async fn an_existing_shutdown_state_never_attempts_activation() -> Result<()> {
    let (starter, state) = fixture(0);
    state.set(RuntimePhase::Stopped, false);
    drive(starter, CancellationToken::new()).await?;
    assert_eq!(state.count(), 0);
    Ok(())
}
