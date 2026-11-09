//! One application-owned installed call using the SAME retained controller.
//! No public HTTP route, discovery dependency, history or owner ticket.
use super::*;
use elixir_marketplace::{
    identity::ExtensionId,
    source_execution::{MAX_ARGUMENT_BYTES, RuntimeJson},
};
use std::{sync::Weak, time::Duration};
use tokio::time::timeout;

const STOP_GRACE: Duration = Duration::from_secs(45);
pub(super) mod queue;

struct Request {
    home: Uuid,
    extension: ExtensionId,
    instance: Uuid,
    arguments: RuntimeJson,
    provider: Option<installation::providers::Expectation>,
    http: Option<crate::marketplace::worker::SdkHttpClient>,
}
impl Request {
    fn validate(&self) -> Result<()> {
        if self.home.is_nil()
            || self.instance.is_nil()
            || self.arguments.accounted_bytes() > MAX_ARGUMENT_BYTES
            || self.provider.as_ref().is_some_and(|p| p.provider.is_nil())
        {
            bail!("invalid or oversized configured installed call");
        }
        Ok(())
    }
}

// This return contains the ORIGINAL controller and the actual cancel-await
// result. It is neither deserializable nor constructible from status. A successful
// cleanup flag without that same idle, owned controller cannot release anything.
struct Completion {
    controller: Option<InstalledController>,
    result: Option<Result<RuntimeJson>>,
    cleanup: Result<()>,
}
impl Completion {
    fn require_closed(&self) -> Result<()> {
        if self.cleanup.is_err() {
            bail!("original installed call cleanup is unconfirmed");
        }
        self.controller
            .as_ref()
            .context("original installed controller already released")?
            .require_idle()
    }
    async fn cancel(&mut self) -> Result<()> {
        if let Some(controller) = self.controller.as_mut() {
            self.cleanup = controller.cancel().await;
            self.require_closed()?;
        } else if self.cleanup.is_err() {
            bail!("original installed cleanup owner is missing");
        }
        self.result = None;
        Ok(())
    }
}
pub(super) struct Entry {
    stop: watch::Sender<bool>,
    consumer: Weak<()>,
    task: AsyncMutex<Joined<Completion>>,
}
impl Entry {
    pub(super) fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    async fn cancel(&self) -> Result<()> {
        self.request_stop();
        timeout(STOP_GRACE, async {
            let mut original = self.task.lock().await;
            original.join().await?.cancel().await
        })
        .await
        .context("original installed call retirement is still pending")?
    }
    async fn shutdown(&self) -> Result<()> {
        self.cancel().await?;
        let mut original = self.task.lock().await;
        let completed = original.join().await?;
        if let Some(controller) = completed.controller.as_mut() {
            controller.shutdown_cache().await?;
        }
        completed.require_closed()
    }
}

/// A single non-cloneable consumer of an already-owned task. Dropping a waiter
/// signals that task but does not abort or replace its retained cleanup owner.
pub(crate) struct InstalledCall {
    entry: Arc<Entry>,
    _consumer: Arc<()>,
    // Drops AFTER consumer/stop signaling; coordination is never cleanup proof.
    turn: Option<queue::Turn>,
}
impl InstalledCall {
    pub(crate) async fn finish(&mut self) -> Result<RuntimeJson> {
        let mut original = self.entry.task.lock().await;
        let completed = original.join().await?;
        completed.require_closed()?;
        let result = completed
            .result
            .take()
            .context("original installed result already consumed")?;
        // Reaping needs this same original task lock. Release it BEFORE waking
        // the next caller; an unconsumed/uncertain result never gets this far.
        drop(original);
        drop(self.turn.take());
        result
    }
    pub(crate) async fn cancel(&mut self) -> Result<()> {
        self.entry.cancel().await?;
        drop(self.turn.take());
        Ok(())
    }
}
impl Drop for InstalledCall {
    fn drop(&mut self) {
        self.entry.request_stop();
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() || stop.changed().await.is_err() {
            return;
        }
    }
}
async fn run(
    mut controller: InstalledController,
    request: Request,
    budget: InstallBudget,
    limits: SourceExecutionLimits,
    mut stop: watch::Receiver<bool>,
) -> Completion {
    let result = async {
        tokio::select! {
            biased;
            _ = stopped(&mut stop) => bail!("installed call cancelled before start"),
            result = Box::pin(async {
                if let Some(provider)=request.provider {
                    let coordinates=installation::providers::Coordinates{provider:provider.provider,home:request.home,
                        extension:request.extension,instance:request.instance};
                    controller.begin_provider(&coordinates,provider.revision,budget,limits,request.arguments,request.http).await
                } else {
                    controller.begin_configured(request.home,&request.extension,request.instance,budget,limits,request.arguments,request.http).await
                }
            }) => result?,
        }
        tokio::select! {
            biased;
            _ = stopped(&mut stop) => bail!("installed call cancelled"),
            result = Box::pin(controller.finish()) => result,
        }
    }
    .await;
    // Always retire through this SAME controller outside the fallible operation.
    // Failed inner joins remain in it; no status or free-lock inference. An
    // unexpected outer panic propagates into the service's sticky Joined::Failed
    // slot; it cannot be converted to a reusable idle/no-effect observation.
    let cleanup = Box::pin(controller.cancel()).await;
    Completion {
        controller: Some(controller),
        result: Some(result),
        cleanup,
    }
}

impl Service {
    #[cfg(test)]
    pub(in crate::marketplace) fn begin_installed_call(
        &self,
        home: Uuid,
        extension: ExtensionId,
        instance: Uuid,
        arguments: RuntimeJson,
        budget: InstallBudget,
        limits: SourceExecutionLimits,
    ) -> Result<InstalledCall> {
        self.reserve_installed_call(
            Request {
                home,
                extension,
                instance,
                arguments,
                provider: None,
                http: None,
            },
            budget,
            limits,
            None,
        )
    }
    pub(in crate::marketplace) fn begin_installed_http_call(
        &self,
        home: Uuid,
        extension: ExtensionId,
        instance: Uuid,
        arguments: RuntimeJson,
        budget: InstallBudget,
        limits: SourceExecutionLimits,
        http: Option<crate::marketplace::worker::SdkHttpClient>,
        turn: queue::Turn,
    ) -> Result<InstalledCall> {
        let request = Request {
            home,
            extension,
            instance,
            arguments,
            provider: None,
            http,
        };
        self.reserve_installed_call(request, budget, limits, Some(turn))
    }
    pub(in crate::marketplace) fn begin_provider_call(
        &self,
        descriptor: &installation::providers::Descriptor,
        arguments: RuntimeJson,
        budget: InstallBudget,
        limits: SourceExecutionLimits,
        http: Option<crate::marketplace::worker::SdkHttpClient>,
        turn: queue::Turn,
    ) -> Result<InstalledCall> {
        self.reserve_installed_call(
            Request {
                home: descriptor.home,
                extension: descriptor.extension.clone(),
                instance: descriptor.instance,
                arguments,
                provider: Some(installation::providers::Expectation {
                    provider: descriptor.provider,
                    revision: descriptor.revision,
                }),
                http,
            },
            budget,
            limits,
            Some(turn),
        )
    }
    fn reserve_installed_call(
        &self,
        request: Request,
        budget: InstallBudget,
        limits: SourceExecutionLimits,
        turn: Option<queue::Turn>,
    ) -> Result<InstalledCall> {
        request.validate()?;
        budget.validate()?;
        limits.validate()?;
        self.reap_finished()?;
        self.reap_invocation()?;
        let executor =
            tokio::runtime::Handle::try_current().context("installed call executor unavailable")?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed call owner unavailable"))?;
        if state.stopped
            || state.entry.is_some()
            || state.invocation.is_some()
            || state.restoration.is_some()
        {
            bail!("installed execution is stopping or original command/call is still owned");
        }
        state
            .controller
            .as_ref()
            .context("no original installed controller is retained")?
            .require_idle()?;
        let (stop, stopped) = watch::channel(false);
        let consumer = Arc::new(());
        let controller = state
            .controller
            .take()
            .expect("borrowed original installed controller");
        let task = executor.spawn(run(controller, request, budget, limits, stopped));
        let entry = Arc::new(Entry {
            stop,
            consumer: Arc::downgrade(&consumer),
            task: AsyncMutex::new(Joined::Running(task)),
        });
        // No await between moving the controller into its sole task and retaining
        // that task. The consumer cannot detach it from application shutdown.
        state.invocation = Some(entry.clone());
        Ok(InstalledCall {
            entry,
            _consumer: consumer,
            turn,
        })
    }
    pub(super) async fn shutdown_invocation(&self) -> Result<()> {
        let original = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed call owner unavailable"))?
            .invocation
            .clone();
        if let Some(original) = original {
            original.shutdown().await?;
        }
        Ok(())
    }
    pub(super) fn reap_invocation(&self) -> Result<()> {
        let original = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed call owner unavailable"))?
            .invocation
            .clone();
        let Some(original) = original else {
            return Ok(());
        };
        let Ok(mut task) = original.task.try_lock() else {
            return Ok(());
        };
        task.poll_finished()?;
        let Joined::Done(completed) = &mut *task else {
            return Ok(());
        };
        // The single unconsumed result reserves the same slot: retaining many
        // finished consumer handles cannot accumulate 16 MiB outputs/history.
        if original.consumer.upgrade().is_some() && completed.result.is_some() {
            return Ok(());
        }
        completed.require_closed()?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed call owner unavailable"))?;
        if state
            .invocation
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &original))
        {
            if state.controller.is_some() || state.entry.is_some() {
                bail!("original installed controller already retained or replaced");
            }
            state.controller = completed.controller.take();
            completed.result = None;
            state.invocation = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
