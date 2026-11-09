//! One startup-owned restoration, independent of discovery or a browser.
//! Dropping a waiter never drops the original open/reconciliation future.
use super::*;
use std::time::Duration;
use tokio::time::timeout;

const JOIN_GRACE: Duration = Duration::from_secs(45);

struct Completion {
    controller: Option<InstalledController>,
    result: Result<()>,
}
impl Completion {
    fn require_owned(&self) -> Result<()> {
        self.result.as_ref().map_err(|error| {
            anyhow::anyhow!("original installed restoration failed; no implicit retry: {error}")
        })?;
        self.controller
            .as_ref()
            .context("original restored controller missing")?
            .require_idle()
    }
    async fn stop(&mut self) -> Result<()> {
        let controller = self
            .controller
            .as_mut()
            .context("original restored controller missing; no cleanup inference")?;
        controller.shutdown_cache().await?;
        controller.require_idle()?;
        self.result
            .as_ref()
            .map_err(|_| anyhow::anyhow!("original installed restoration was not confirmed"))?;
        Ok(())
    }
}
pub(super) struct Entry {
    stop: watch::Sender<bool>,
    task: AsyncMutex<Joined<Completion>>,
}
impl Entry {
    pub(super) fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    async fn shutdown(&self) -> Result<()> {
        self.request_stop();
        timeout(JOIN_GRACE, async {
            self.task.lock().await.join().await?.stop().await
        })
        .await
        .context("original installed restoration is still pending")?
    }
}

async fn run(host: Host, stopped: watch::Receiver<bool>) -> Completion {
    // Never select/drop this future on cancellation: it may be reconciling the
    // actual old native owner under the original global-lane OS lease.
    let opened = Box::pin(InstalledController::open(
        host.database,
        host.secrets,
        host.runtime,
    ))
    .await;
    match opened {
        Ok(mut controller) => {
            let result = async {
                if *stopped.borrow() {
                    controller.cancel().await?;
                }
                controller.require_idle()
            }
            .await;
            Completion {
                controller: Some(controller),
                result,
            }
        }
        Err(error) => Completion {
            controller: None,
            result: Err(error),
        },
    }
}

impl Service {
    pub(in crate::marketplace) fn begin_restoration(&self, host: Host) -> Result<()> {
        let executor = tokio::runtime::Handle::try_current()
            .context("installed restoration executor unavailable")?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed restoration owner unavailable"))?;
        if state.stopped
            || state.restoration.is_some()
            || state.entry.is_some()
            || state.completed.is_some()
            || state.controller.is_some()
            || state.invocation.is_some()
        {
            bail!("installed restoration requires the vacant original application slot");
        }
        let (stop, stopped) = watch::channel(false);
        let task = executor.spawn(run(host, stopped));
        state.restoration = Some(Arc::new(Entry {
            stop,
            task: AsyncMutex::new(Joined::Running(task)),
        }));
        Ok(())
    }
    pub(in crate::marketplace) async fn finish_restoration(&self) -> Result<()> {
        let original = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed restoration owner unavailable"))?
            .restoration
            .clone()
            .context("no original installed restoration")?;
        {
            let mut task = original.task.lock().await;
            task.join().await?.require_owned()?;
        }
        self.reap_restoration()
    }
    pub(super) fn reap_restoration(&self) -> Result<()> {
        let original = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed restoration owner unavailable"))?
            .restoration
            .clone();
        let Some(original) = original else {
            return Ok(());
        };
        let Ok(mut task) = original.task.try_lock() else {
            return Ok(());
        };
        task.poll_finished()?;
        if matches!(&*task, Joined::Failed) {
            bail!("original installed restoration join is unconfirmed");
        }
        let Joined::Done(completed) = &mut *task else {
            return Ok(());
        };
        completed.require_owned()?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed restoration owner unavailable"))?;
        if state.stopped {
            bail!("installed restoration stopped before handoff");
        }
        if state
            .restoration
            .as_ref()
            .is_some_and(|entry| Arc::ptr_eq(entry, &original))
        {
            if state.controller.is_some() || state.entry.is_some() || state.invocation.is_some() {
                bail!("original restored controller already retained or replaced");
            }
            state.controller = completed.controller.take();
            state.restoration = None;
        }
        Ok(())
    }
    pub(super) async fn shutdown_restoration(&self) -> Result<()> {
        let original = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed restoration owner unavailable"))?
            .restoration
            .clone();
        if let Some(original) = original {
            original.shutdown().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
