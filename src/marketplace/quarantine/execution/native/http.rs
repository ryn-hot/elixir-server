//! One original startup AND lifetime task. No HTTP input, packet credit,
//! resource admission or gateway-health capability is manufactured here.
use super::*;
use crate::marketplace::{
    lease::activity::http::HttpFence, runtime::application_http::ApplicationHttpProfile,
    worker::SdkHttpAuthority,
};
use tokio::{sync::oneshot, task::JoinHandle};

mod io;

const AUTHORITY_TIMEOUT: Duration = Duration::from_secs(5);
const RETIRE_TIMEOUT: Duration = Duration::from_secs(30);

struct Owned {
    native: Arc<NativeInvocation>,
    profile: Arc<ApplicationHttpProfile>,
    fence: Option<HttpFence>,
    io_tracking: Arc<io::Tracking>,
}
impl Owned {
    async fn retire(&mut self) -> Result<()> {
        if let Some(fence) = self.fence.as_mut() {
            if !self.io_tracking.seal_for_retirement() {
                // Stop network/native effects, but never remove the marker
                // while its original attach/pipe owner remains unjoined.
                let (profile, handle) = fence.bound(&self.native.lease)?;
                timeout(
                    RETIRE_TIMEOUT,
                    self.native.runtime.stop_application_http(&handle, &profile),
                )
                .await
                .context("application HTTP stop before pipe join deadline")??;
                bail!("original application HTTP pipes remain unjoined");
            }
            // Keep this exact object on failure or cancellation. A subsequent
            // waiter retries retirement, never CREATE/START or name discovery
            // after the actual immutable CID has been saved.
            timeout(
                RETIRE_TIMEOUT,
                fence.retire(self.native.runtime.as_ref(), &self.native.lease),
            )
            .await
            .context("original application HTTP retirement remains pending")??;
            self.fence = None;
        }
        Ok(())
    }
    async fn current(
        &self,
        authority: &dyn SdkHttpAuthority,
        stop: &mut oneshot::Receiver<()>,
    ) -> Result<()> {
        timeout(AUTHORITY_TIMEOUT, authority.current_native(&self.native))
            .await
            .context("application HTTP authority deadline")??;
        self.native.check()?;
        if !matches!(stop.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
            bail!("application HTTP owner cancelled");
        }
        Ok(())
    }
    async fn start(
        &mut self,
        authority: &dyn SdkHttpAuthority,
        stop: &mut oneshot::Receiver<()>,
    ) -> Result<ContainerHandle> {
        self.current(authority, stop).await?;
        let attempt = self
            .fence
            .as_mut()
            .context("application HTTP startup lost its original fence")?
            .creating(&self.native.lease)?;
        // Do NOT select caller cancellation against CREATE/START: a late reply
        // must return to this same task. Only the original absolute deadline
        // can leave an uncertain operation for durable explicit recovery.
        let created = timeout_at(
            self.native.expires,
            self.native
                .runtime
                .create_application_http(&self.profile, attempt),
        )
        .await
        .context("original application HTTP create deadline")??;
        let handle = match created {
            crate::runtime::http_creation::HttpCreation::Created(handle) => handle,
            crate::runtime::http_creation::HttpCreation::NotAttempted(outcome) => {
                let error = self
                    .fence
                    .as_mut()
                    .context("HTTP rejection lost its original fence")?
                    .not_attempted(&self.native.lease, outcome)?;
                return Err(error);
            }
        };
        if handle.name != self.profile.spec().name {
            bail!("application HTTP create returned a different owner");
        }
        // Save the returned identity even if cancellation/deadline happened
        // during CREATE; no post-await authorization failure may discard it.
        self.fence
            .as_mut()
            .context("application HTTP startup lost its original fence")?
            .bind(&self.native.lease, &handle.id)?;
        self.current(authority, stop).await?;
        timeout_at(
            self.native.expires,
            self.native
                .runtime
                .start_application_http(&handle, &self.profile),
        )
        .await
        .context("original application HTTP start deadline")??;
        self.current(authority, stop).await?;
        Ok(handle)
    }
}

/// A single non-cloneable lifetime owner, retained OUTSIDE the fallible source
/// exchange. Drop signals its original task; it never erases a fence, aborts a
/// runtime operation, replaces that task, or acknowledges successful cleanup.
pub(in crate::marketplace) struct HttpLaunch {
    native: Arc<NativeInvocation>,
    authority: Arc<dyn SdkHttpAuthority>,
    profile: Arc<ApplicationHttpProfile>,
    io: io::Owner,
    stop: Option<oneshot::Sender<()>>,
    ready: Option<oneshot::Receiver<Result<ContainerHandle>>>,
    handle: Option<ContainerHandle>,
    ready_failed: bool,
    task: Option<JoinHandle<Owned>>,
    completed: Option<Owned>,
    join_failed: bool,
}
impl HttpLaunch {
    pub(super) fn begin(
        executor: tokio::runtime::Handle,
        native: Arc<NativeInvocation>,
        profile: ApplicationHttpProfile,
        authority: Arc<dyn SdkHttpAuthority>,
        fence: HttpFence,
    ) -> Self {
        let profile = Arc::new(profile);
        let (stop, mut cancelled) = oneshot::channel();
        let (ready, response) = oneshot::channel();
        let io_tracking = Arc::new(io::Tracking::new());
        let mut owned = Owned {
            native: native.clone(),
            profile: profile.clone(),
            fence: Some(fence),
            io_tracking: io_tracking.clone(),
        };
        let lifetime_authority = authority.clone();
        let task = executor.spawn(async move {
            let result = owned.start(lifetime_authority.as_ref(), &mut cancelled).await;
            let started = result.is_ok();
            let delivered = ready.send(result).is_ok();
            if started && delivered {
                // The native executor deadline also bounds this helper's
                // lifetime. Time passing never becomes retirement proof.
                let expires = owned
                    .native
                    .expires
                    .min(Instant::now() + owned.profile.native_deadline());
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut cancelled => break,
                        _ = tokio::time::sleep_until(expires) => break,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {
                            if owned.current(lifetime_authority.as_ref(), &mut cancelled).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            // This same task owns late creation/start outcomes AND cleanup.
            // Failed cleanup returns the owner for an explicit retained retry.
            // If the outer owner is dropped, the durable marker still blocks
            // all future global work until restart recovery proves retirement.
            let _ = owned.retire().await;
            owned
        });
        Self {
            native,
            authority,
            profile,
            io: io::Owner::new(io_tracking),
            stop: Some(stop),
            ready: Some(response),
            handle: None,
            ready_failed: false,
            task: Some(task),
            completed: None,
            join_failed: false,
        }
    }
    /// Startup only, not permission for sockets or evidence of a current grant,
    /// gateway or spare capacity. The actual HTTP owner checks those at input.
    pub(in crate::marketplace) async fn ready(&mut self) -> Result<&ContainerHandle> {
        if self.stop.is_none()
            || self.ready_failed
            || self.task.as_ref().is_none_or(JoinHandle::is_finished)
        {
            bail!("application HTTP startup is cancelled or failed");
        }
        if let Some(response) = self.ready.as_mut() {
            let result = response.await;
            self.ready = None;
            match result {
                Ok(Ok(handle)) => self.handle = Some(handle),
                Ok(Err(error)) => {
                    self.ready_failed = true;
                    return Err(error);
                }
                Err(_) => {
                    self.ready_failed = true;
                    bail!("original application HTTP startup stopped");
                }
            }
        }
        self.handle
            .as_ref()
            .context("application HTTP has no original started identity")
    }
    pub(in crate::marketplace) fn profile(&self) -> &ApplicationHttpProfile {
        &self.profile
    }
    async fn effect_current(&self) -> Result<()> {
        timeout(
            AUTHORITY_TIMEOUT,
            self.authority.current_native(&self.native),
        )
        .await
        .context("application HTTP pipe authority deadline")??;
        self.native.check()
    }
    /// Retain the ORIGINAL local attach task before its first await. This is
    /// I/O ownership only, not admission, a gateway lease or permission to send.
    pub(in crate::marketplace) async fn attach_io(&mut self) -> Result<()> {
        if self.io.empty() {
            let handle = self.ready().await?.clone();
            self.native.check()?;
            self.io.begin(
                self.native.runtime.clone(),
                handle,
                self.profile.clone(),
                self.native.expires,
            )?;
        }
        self.io.connect().await?;
        // Attachment alone cannot open a socket or release extension bytes.
        // The driver checks the actual helper profile; exchange_io performs
        // the full fresh installed/config/native authority check BEFORE input.
        // Avoid spending the helper's first-input deadline on duplicate checks.
        self.native.check()
    }
    /// Private framed transport. The application owner must still validate
    /// StreamProtocol and actual gateway context before releasing SDK replies.
    pub(in crate::marketplace) async fn exchange_io(
        &mut self,
        bytes: &[u8],
    ) -> Result<zeroize::Zeroizing<Vec<u8>>> {
        self.effect_current().await?;
        let reply = timeout_at(self.native.expires, self.io.exchange(bytes))
            .await
            .context("original application HTTP exchange deadline")??;
        if let Err(error) = self.effect_current().await {
            self.io.poison();
            return Err(error);
        }
        Ok(reply)
    }
    /// Natural transport completion, not permission to expose an HTTP reply.
    /// The same codec/authority owner must additionally validate its Close.
    pub(in crate::marketplace) async fn finish_io(&mut self) -> Result<()> {
        if timeout(RETIRE_TIMEOUT, self.io.retire())
            .await
            .context("original application HTTP pipe join deadline")??
            != Some(true)
        {
            bail!("application HTTP transport did not complete naturally");
        }
        let handle = self
            .handle
            .as_ref()
            .context("original HTTP identity unavailable")?;
        self.native
            .runtime
            .confirm_application_http_success(handle, &self.profile)
            .await?;
        self.effect_current().await?;
        Ok(())
    }
    /// Called only after the same attached HTTP task/private pipes are joined.
    /// Cancellation of this waiter retains its original JoinHandle/Owned state.
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        drop(self.stop.take());
        // A cancelled waiter leaves the original attach task/Child/partial
        // reads in self.io. Only its actual join can release the tracking gate.
        timeout(RETIRE_TIMEOUT, self.io.retire())
            .await
            .context("original application HTTP pipes still require retirement")??;
        if self.join_failed {
            bail!("original application HTTP lifetime join failed");
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            match result {
                Ok(owned) => self.completed = Some(owned),
                Err(_) => {
                    self.join_failed = true;
                    bail!("original application HTTP lifetime join failed");
                }
            }
        }
        self.completed
            .as_mut()
            .context("original application HTTP cleanup owner missing")?
            .retire()
            .await?;
        self.handle = None;
        self.ready = None;
        Ok(())
    }
}
impl Drop for HttpLaunch {
    fn drop(&mut self) {
        drop(self.stop.take());
    }
}

#[cfg(all(test, unix))]
mod native_tests;
#[cfg(test)]
mod tests;
