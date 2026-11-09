//! One bounded retry owner, never a second networking/health factory. Failed
//! preparations reconcile under actual controller leases before admission;
//! failed attached owners must produce their joined cleanup receipt first.
use super::*;
use crate::marketplace::service::{RuntimePhase, RuntimeSnapshot};
use std::future::Future;
use tokio::time::{Instant, sleep, sleep_until};

const ATTEMPT_DEADLINE: Duration = Duration::from_secs(420);
const RETIRE_DEADLINE: Duration = Duration::from_secs(75);
const STABLE_WINDOW: Duration = Duration::from_secs(600);
const CONFIGURATION_INTERVAL: Duration = Duration::from_secs(5);
const CONFIGURATION_DEADLINE: Duration = Duration::from_secs(10);
const CONFIGURATION_RETRY_FLOOR: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Backoff {
    step: usize,
}
impl Backoff {
    fn next(&mut self, entropy: u64) -> Duration {
        let steps = [5_u64, 10, 20, 40, 80, 160, 240];
        let base = steps[self.step.min(steps.len() - 1)];
        self.step = (self.step + 1).min(steps.len() - 1);
        Duration::from_secs(base + entropy % (base / 4 + 1))
    }
    fn reset(&mut self) {
        self.step = 0;
    }
}
// Private deterministic test seam. No request can supply this implementation
// or assert protection/cleanup through it.
trait Starter: Send + 'static {
    fn status(&self) -> RuntimeSnapshot;
    fn stop_requested(&self) -> bool;
    fn entropy(&self) -> u64;
    fn start(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn retire(&mut self) -> impl Future<Output = Result<bool>> + Send;
    fn configuration_current(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
    fn invalidate_connection(&self) {}
    fn configuration_changed(&self) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
    fn retry_after(&self, _delay: Option<Duration>) {}
}
struct AppStarter {
    state: AppState,
    path: PathBuf,
    binding: Option<super::ConnectionBinding>,
    _progress: super::super::service::StartupOwner,
}
impl Starter for AppStarter {
    fn status(&self) -> RuntimeSnapshot {
        self.state.marketplace.snapshot()
    }
    fn stop_requested(&self) -> bool {
        self.state.marketplace.stop_requested()
    }
    fn entropy(&self) -> u64 {
        u64::from(uuid::Uuid::new_v4().as_bytes()[0])
    }
    async fn start(&mut self) -> Result<()> {
        self.binding = None;
        self.binding = Some(activate_once(&self.state, &self.path).await?);
        Ok(())
    }
    async fn retire(&mut self) -> Result<bool> {
        self.state.marketplace.recover_failed().await
    }
    async fn configuration_current(&self) -> Result<()> {
        if let Some(binding) = &self.binding {
            binding.current().await?;
        }
        Ok(())
    }
    fn invalidate_connection(&self) {
        if let Ok(client) = self.state.marketplace.client() {
            client.invalidate_connection();
        }
    }
    async fn configuration_changed(&self) {
        self.state
            .marketplace
            .wait_connection_configuration_changed()
            .await;
    }
    fn retry_after(&self, delay: Option<Duration>) {
        self.state.marketplace.startup_retry_after(delay);
    }
}
pub(super) fn spawn(
    state: AppState,
    path: PathBuf,
    shutdown: CancellationToken,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let progress = state.marketplace.claim_startup_progress()?;
        let result = drive(
            AppStarter {
                state,
                path,
                binding: None,
                _progress: progress,
            },
            shutdown,
        )
        .await;
        if let Err(error) = &result {
            tracing::warn!(error=%error,"marketplace recovery stopped without a cleanup acknowledgement; no fallback enabled");
        }
        result
    })
}
async fn drive(mut starter: impl Starter, shutdown: CancellationToken) -> Result<()> {
    let mut backoff = Backoff::default();
    loop {
        if shutdown.is_cancelled()
            || starter.stop_requested()
            || starter.status().phase == RuntimePhase::Stopped
        {
            return Ok(());
        }
        // Stopping can also mean failure cleanup is in flight. It is not
        // owner intent and must not silently terminate future recovery.
        if starter.status().phase == RuntimePhase::Stopping {
            tokio::select! { biased;
                _=shutdown.cancelled()=>return Ok(()),
                _=sleep(Duration::from_secs(1))=>{},
            }
            continue;
        }
        starter.retry_after(None);
        let activation = tokio::select! { biased;
            _=shutdown.cancelled()=>return Ok(()),
            result=timeout(ATTEMPT_DEADLINE,starter.start())=>result.context("marketplace startup deadline").and_then(|result|result),
        };
        match activation {
            Ok(()) => {
                tracing::info!("marketplace attached a freshly verified protected worker");
                let mut stable_since = None;
                let mut next_configuration = Instant::now();
                let mut invalidated: Option<Instant> = None;
                loop {
                    if shutdown.is_cancelled() || starter.stop_requested() {
                        return Ok(());
                    }
                    match starter.status().phase {
                        RuntimePhase::Failed => break,
                        RuntimePhase::Stopped => return Ok(()),
                        RuntimePhase::Running => {
                            let since = stable_since.get_or_insert_with(Instant::now);
                            if invalidated.is_none() && since.elapsed() >= STABLE_WINDOW {
                                backoff.reset();
                            }
                        }
                        _ => stable_since = None,
                    }
                    if invalidated.is_some_and(|at| at.elapsed() >= RETIRE_DEADLINE) {
                        anyhow::bail!(
                            "changed marketplace connection did not confirm protection shutdown; original owner remains fenced"
                        );
                    }
                    if invalidated.is_none() && Instant::now() >= next_configuration {
                        let current = tokio::select! {biased;
                            _=shutdown.cancelled()=>return Ok(()),
                            result=timeout(CONFIGURATION_DEADLINE,starter.configuration_current())=>result.context("marketplace configuration check deadline").and_then(|result|result),
                        };
                        next_configuration = Instant::now() + CONFIGURATION_INTERVAL;
                        if let Err(error) = current {
                            crate::marketplace::lifecycle::trace_failure(
                                "connection_configuration",
                                &error,
                            );
                            starter.invalidate_connection();
                            invalidated = Some(Instant::now());
                            stable_since = None;
                            tracing::info!(
                                "marketplace connection configuration unavailable or changed; waiting for original protected owner shutdown before retry"
                            );
                        }
                    }
                    tokio::select! { biased;
                        _=shutdown.cancelled()=>return Ok(()),
                        _=sleep(Duration::from_secs(1))=>{},
                    }
                }
            }
            Err(error) => {
                tracing::warn!(error=%error,"marketplace activation unavailable; retry will not reset state or bypass protection")
            }
        }
        if shutdown.is_cancelled()
            || starter.stop_requested()
            || starter.status().phase == RuntimePhase::Stopped
        {
            return Ok(());
        }
        let status = starter.status();
        if status.phase == RuntimePhase::Failed && status.participation.is_some() {
            // Do not cancel a consumed owner's cleanup future. Shutdown latches
            // stop in the service and joins this bounded retirement in main.
            if !timeout(RETIRE_DEADLINE, starter.retire())
                .await
                .context("marketplace retry cleanup deadline")??
            {
                return Ok(());
            }
        }
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let delay = backoff.next(starter.entropy());
        let until = Instant::now() + delay;
        starter.retry_after(Some(delay));
        tracing::info!(
            retry_seconds = delay.as_secs(),
            "marketplace waiting before ownership-gated retry"
        );
        tokio::select! { biased;
            _=shutdown.cancelled()=>return Ok(()),
            _=sleep_until(until)=>{},
            _=starter.configuration_changed()=>{
                // Never overlap original retirement or bypass host/credit/
                // protection admission. One hint shortens the existing wait,
                // with a five-second floor, without resetting failure backoff.
                let earlier=(Instant::now()+CONFIGURATION_RETRY_FLOOR).min(until);
                starter.retry_after(Some(earlier.saturating_duration_since(Instant::now())));
                tokio::select! {biased;
                    _=shutdown.cancelled()=>return Ok(()),
                    _=sleep_until(earlier)=>{},
                }
            },
        }
    }
}

#[cfg(test)]
mod tests;
