use std::{
    future::Future,
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::state::AppState;

use super::anime_repair::{AnimeLibraryRepairTrigger, run_anime_library_repair_for_state};

const LIBRARY_SCAN_PENDING: u8 = 1 << 0;
const METADATA_REFRESH_PENDING: u8 = 1 << 1;
const PROVIDER_CORRECTION_PENDING: u8 = 1 << 2;

static REPAIR_LOOP_ACTIVE: AtomicBool = AtomicBool::new(false);
static REPAIR_TRIGGERS: RepairTriggerQueue = RepairTriggerQueue::new();

struct RepairLoopGuard;

impl Drop for RepairLoopGuard {
    fn drop(&mut self) {
        REPAIR_LOOP_ACTIVE.store(false, Ordering::Release);
    }
}

struct RepairTriggerQueue {
    pending: AtomicU8,
    notify: Notify,
}

impl RepairTriggerQueue {
    const fn new() -> Self {
        Self {
            pending: AtomicU8::new(0),
            notify: Notify::const_new(),
        }
    }

    fn request_scan(&self, force_metadata: bool) {
        let trigger = if force_metadata {
            METADATA_REFRESH_PENDING
        } else {
            LIBRARY_SCAN_PENDING
        };
        self.pending.fetch_or(trigger, Ordering::Release);
        // There is one repair worker. `notify_one` retains a permit when the
        // worker is busy, while the bitset preserves every trigger class.
        self.notify.notify_one();
    }

    fn request_provider_correction(&self) {
        self.pending
            .fetch_or(PROVIDER_CORRECTION_PENDING, Ordering::Release);
        self.notify.notify_one();
    }

    /// Folds an ordinary scan that completed before the repair loop started
    /// into the startup pass. The atomic boundary is deliberately immediately
    /// before that pass: a scan requested after this call, including while the
    /// startup repair is in flight, sets the bit again and remains queued.
    fn consume_pre_start_library_scan(&self) -> bool {
        self.pending
            .fetch_and(!LIBRARY_SCAN_PENDING, Ordering::AcqRel)
            & LIBRARY_SCAN_PENDING
            != 0
    }

    fn take(&self) -> Option<AnimeLibraryRepairTrigger> {
        let pending = self.pending.swap(0, Ordering::AcqRel);
        if pending & METADATA_REFRESH_PENDING != 0 {
            Some(AnimeLibraryRepairTrigger::MetadataRefresh)
        } else if pending & PROVIDER_CORRECTION_PENDING != 0 {
            Some(AnimeLibraryRepairTrigger::ProviderCorrection)
        } else if pending & LIBRARY_SCAN_PENDING != 0 {
            Some(AnimeLibraryRepairTrigger::LibraryScan)
        } else {
            None
        }
    }

    async fn notified(&self) {
        self.notify.notified().await;
    }
}

/// Chooses the next ready repair without coupling trigger ordering to the
/// runner's result. Model publications take priority over scan work, while the
/// scan bit remains queued for the following pass. Advancing the observed
/// generation when the trigger is dispatched also coalesces rapid model
/// updates into one pass against the newest published profile.
fn take_ready_repair_trigger(
    queue: &RepairTriggerQueue,
    observed_activation_generation: &mut u64,
    published_activation_generation: u64,
) -> Option<AnimeLibraryRepairTrigger> {
    if published_activation_generation > *observed_activation_generation {
        *observed_activation_generation = published_activation_generation;
        Some(AnimeLibraryRepairTrigger::ModelActivated)
    } else {
        queue.take()
    }
}

#[cfg(test)]
pub(super) async fn dispatch_published_model_activation_for_test<Run, RunFuture, Output>(
    observed_activation_generation: &mut u64,
    published_activation_generation: u64,
    runner: Run,
) -> Option<Output>
where
    Run: FnOnce(AnimeLibraryRepairTrigger) -> RunFuture,
    RunFuture: Future<Output = Output>,
{
    let queue = RepairTriggerQueue::new();
    let trigger = take_ready_repair_trigger(
        &queue,
        observed_activation_generation,
        published_activation_generation,
    )?;
    Some(runner(trigger).await)
}

async fn run_startup_repair_with<Run, RunFuture>(queue: &RepairTriggerQueue, runner: Run)
where
    Run: FnOnce(AnimeLibraryRepairTrigger) -> RunFuture,
    RunFuture: Future<Output = ()>,
{
    queue.consume_pre_start_library_scan();
    runner(AnimeLibraryRepairTrigger::Startup).await;
}

/// Coalesces a completed library scan into the internal anime repair worker.
/// This function never waits for repair work and is safe on the scan response
/// path. A forced metadata scan supersedes an ordinary scan queued with it.
pub fn request_anime_library_repair_after_scan(force_metadata: bool) {
    REPAIR_TRIGGERS.request_scan(force_metadata);
}

/// Retries unresolved historical rows after canonical provider data changes.
/// The repaired cache entry itself is authoritative for this pass, so the
/// worker does not immediately force another network refresh.
pub fn request_anime_library_repair_after_provider_correction() {
    REPAIR_TRIGGERS.request_provider_correction();
}

/// Runs automatic anime-library repair for startup, model publications, and
/// completed library scans until server shutdown.
pub async fn start_anime_library_repair_loop(state: AppState, shutdown: CancellationToken) {
    if REPAIR_LOOP_ACTIVE
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        tracing::warn!("anime library repair loop is already running");
        return;
    }
    let _guard = RepairLoopGuard;
    if shutdown.is_cancelled() {
        return;
    }

    // Snapshot before startup repair. An activation during that repair is then
    // observed immediately by the generation wait below; an earlier activation
    // is already available to the startup pass.
    let mut activation_generation = state.anime_inference.activation_generation();
    run_startup_repair_with(&REPAIR_TRIGGERS, |trigger| run_repair(&state, trigger)).await;

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let published_generation = state.anime_inference.activation_generation();
        if let Some(trigger) = take_ready_repair_trigger(
            &REPAIR_TRIGGERS,
            &mut activation_generation,
            published_generation,
        ) {
            run_repair(&state, trigger).await;
            continue;
        }

        tokio::select! {
            _ = shutdown.cancelled() => break,
            generation = state
                .anime_inference
                .wait_for_activation_after(activation_generation) =>
            {
                let Some(generation) = generation else {
                    break;
                };
                if let Some(trigger) = take_ready_repair_trigger(
                    &REPAIR_TRIGGERS,
                    &mut activation_generation,
                    generation,
                ) {
                    run_repair(&state, trigger).await;
                }
            }
            _ = REPAIR_TRIGGERS.notified() => {}
        }
    }
}

async fn run_repair(state: &AppState, trigger: AnimeLibraryRepairTrigger) {
    match run_anime_library_repair_for_state(state, trigger).await {
        Ok(snapshot) => {
            tracing::info!(
                trigger = ?trigger,
                status = %snapshot.status,
                completed = snapshot.completed_count,
                retryable = snapshot.retryable_count,
                failures = snapshot.failure_count,
                "automatic anime library repair completed"
            );
        }
        Err(error) => {
            tracing::warn!(
                trigger = ?trigger,
                error = %error,
                "automatic anime library repair failed; it remains retryable"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;

    fn dispatch_ready_for_test<E>(
        queue: &RepairTriggerQueue,
        observed_generation: &mut u64,
        published_generation: u64,
        runner: impl FnOnce(AnimeLibraryRepairTrigger) -> Result<(), E>,
    ) -> Option<Result<(), E>> {
        take_ready_repair_trigger(queue, observed_generation, published_generation).map(runner)
    }

    #[test]
    fn alm8_repair_trigger_queue_coalesces_and_prioritizes_metadata_refresh() {
        let queue = RepairTriggerQueue::new();

        queue.request_scan(false);
        queue.request_scan(false);
        assert_eq!(queue.take(), Some(AnimeLibraryRepairTrigger::LibraryScan));
        assert_eq!(queue.take(), None);

        queue.request_scan(false);
        queue.request_scan(true);
        assert_eq!(
            queue.take(),
            Some(AnimeLibraryRepairTrigger::MetadataRefresh)
        );
        assert_eq!(queue.take(), None);

        queue.request_scan(false);
        queue.request_provider_correction();
        assert_eq!(
            queue.take(),
            Some(AnimeLibraryRepairTrigger::ProviderCorrection)
        );
        assert_eq!(queue.take(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alm8_repair_trigger_queue_retains_pre_wait_notification() {
        let queue = RepairTriggerQueue::new();
        queue.request_scan(false);

        tokio::time::timeout(std::time::Duration::from_millis(100), queue.notified())
            .await
            .expect("queued scan notification was lost");
        assert_eq!(queue.take(), Some(AnimeLibraryRepairTrigger::LibraryScan));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alm8_startup_coalesces_pre_start_scan_but_retains_in_flight_scan() {
        let queue = RepairTriggerQueue::new();
        let observed = RefCell::new(Vec::new());

        // This is the notification emitted by the explicitly sequenced
        // initial extension scan. Startup sees the scan's committed state, so
        // a second pass for this notification would repeat the same retry wave.
        queue.request_scan(false);
        let startup_queue = &queue;
        let startup_observed = &observed;
        run_startup_repair_with(&queue, |trigger| async move {
            startup_observed.borrow_mut().push(trigger);
            assert_eq!(
                startup_queue.take(),
                None,
                "pre-start scan was not coalesced"
            );

            // This notification crosses the boundary after startup repair has
            // begun and therefore must survive for the next pass.
            startup_queue.request_scan(false);
            tokio::task::yield_now().await;
        })
        .await;

        let retained = queue
            .take()
            .expect("scan published during startup repair was lost");
        observed.borrow_mut().push(retained);
        assert_eq!(
            observed.into_inner(),
            vec![
                AnimeLibraryRepairTrigger::Startup,
                AnimeLibraryRepairTrigger::LibraryScan,
            ]
        );
        assert_eq!(queue.take(), None, "startup scan produced a duplicate wave");
    }

    #[test]
    fn alm8_model_activation_and_update_are_not_starved_by_scan_events() {
        let queue = RepairTriggerQueue::new();
        let mut observed_generation = 0;

        // A scan is already waiting when the first model becomes active. The
        // activation must run first without consuming the queued scan.
        queue.request_scan(false);
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, 1),
            Some(AnimeLibraryRepairTrigger::ModelActivated)
        );
        assert_eq!(observed_generation, 1);
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, 1),
            Some(AnimeLibraryRepairTrigger::LibraryScan)
        );

        // A later profile publication has the same priority even when both
        // scan classes arrive around it. Metadata refresh remains queued and
        // supersedes the ordinary scan once the model update has run.
        queue.request_scan(false);
        queue.request_scan(true);
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, 2),
            Some(AnimeLibraryRepairTrigger::ModelActivated)
        );
        assert_eq!(observed_generation, 2);
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, 2),
            Some(AnimeLibraryRepairTrigger::MetadataRefresh)
        );
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, 2),
            None
        );
    }

    #[test]
    fn alm8_in_flight_or_failed_pass_does_not_drop_later_triggers() {
        let queue = RepairTriggerQueue::new();
        let mut observed_generation = 4;
        let published_generation = Cell::new(4);

        // New work arrives from inside the runner, after this scan has already
        // been removed from the queue and while its pass is in flight. The
        // synthetic error exercises the same scheduler boundary as run_repair:
        // a runner result never resets the independently queued trigger state.
        queue.request_scan(false);
        assert_eq!(
            dispatch_ready_for_test(
                &queue,
                &mut observed_generation,
                published_generation.get(),
                |trigger| {
                    assert_eq!(trigger, AnimeLibraryRepairTrigger::LibraryScan);
                    queue.request_scan(true);
                    published_generation.set(6);
                    Err("synthetic runner failure")
                },
            ),
            Some(Err("synthetic runner failure"))
        );

        // After the failed runner returns, the newer model is selected first
        // and the metadata refresh that arrived in flight remains processable.
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, published_generation.get()),
            Some(AnimeLibraryRepairTrigger::ModelActivated)
        );
        assert_eq!(observed_generation, 6);
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, published_generation.get()),
            Some(AnimeLibraryRepairTrigger::MetadataRefresh)
        );

        // The same property holds for work arriving while a model-update pass
        // is in flight.
        published_generation.set(7);
        assert_eq!(
            dispatch_ready_for_test(
                &queue,
                &mut observed_generation,
                published_generation.get(),
                |trigger| {
                    assert_eq!(trigger, AnimeLibraryRepairTrigger::ModelActivated);
                    queue.request_scan(false);
                    Ok::<_, &str>(())
                },
            ),
            Some(Ok(()))
        );
        assert_eq!(
            take_ready_repair_trigger(&queue, &mut observed_generation, published_generation.get()),
            Some(AnimeLibraryRepairTrigger::LibraryScan)
        );
    }
}
