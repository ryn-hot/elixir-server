//! Scheduling only. The trusted caller selects purpose; it is not serialized
//! and grants no source, network, execution, SDK, credit or cleanup authority.
use super::*;

// A Checking rejection is not a failed protection owner. Only pre-launch
// callers may turn it into a retryable result; launched native work may not.
#[derive(Debug)]
pub(super) struct Checking;
impl std::fmt::Display for Checking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("background source preflight is not admitted")
    }
}
impl std::error::Error for Checking {}

#[derive(Debug)]
struct PrelaunchDeferred(&'static str);
impl std::fmt::Display for PrelaunchDeferred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for PrelaunchDeferred {}

pub(super) fn verification_deferred(error: &anyhow::Error) -> bool {
    error.is::<PrelaunchDeferred>()
}

#[cfg(test)]
pub(super) fn fixture_deferral() -> anyhow::Error {
    anyhow::anyhow!("synthetic positively unsent verification").context(PrelaunchDeferred(
        "background source preflight is not admitted",
    ))
}

// No permit or native process is created on this miss. Foreground callers and
// a closed owner retain ordinary failure semantics; no new queue is added.
pub(super) fn try_slot(
    owner: &Owner,
    work: Work,
    message: &'static str,
) -> Result<tokio::sync::OwnedSemaphorePermit> {
    owner.slots.clone().try_acquire_owned().map_err(|error| {
        let busy = matches!(&error, tokio::sync::TryAcquireError::NoPermits);
        let error = anyhow::Error::new(error).context(message);
        if busy && matches!(work, Work::Background) {
            error.context(PrelaunchDeferred(message))
        } else {
            error
        }
    })
}

#[derive(Clone, Copy)]
pub(super) enum Work {
    Background,
    Foreground,
}
impl Work {
    pub(super) fn admit(self, owner: &Owner, ready: bool) -> Result<()> {
        match self {
            Self::Background => owner.admit(ready),
            Self::Foreground => owner.admit_foreground(ready),
        }
    }

    /// Reserving the existing slot is not permission to start a helper or send
    /// input. Foreground work may retain that slot across routine Checking;
    /// background work continues to defer immediately.
    pub(super) fn admit_start(self, owner: &Owner) -> Result<()> {
        self.admit(owner, matches!(self, Self::Background))
            .map_err(|error| {
                if matches!(self, Self::Background) && error.is::<Checking>() {
                    error.context(PrelaunchDeferred(
                        "background source preflight is not admitted",
                    ))
                } else {
                    error
                }
            })
    }

    /// Before launch, allow the SAME protection owner's bounded health cycle
    /// to complete. No native helper exists yet. Keep one slot and one fixed
    /// deadline; changing snapshots cannot renew it or authorize any input.
    pub(super) async fn ready_before_launch<T>(
        self,
        owner: &Owner,
        completion: &mut oneshot::Sender<Result<T>>,
    ) -> Result<()> {
        self.wait_ready(owner, completion, super::super::CHECKING_DEADLINE, true)
            .await
    }

    /// A launched helper remains under its shorter retirement budget. Never
    /// spend the longer pre-launch allowance while owning a native process.
    pub(super) async fn ready<T>(
        self,
        owner: &Owner,
        completion: &mut oneshot::Sender<Result<T>>,
    ) -> Result<()> {
        self.wait_ready(owner, completion, super::super::RENEW_DEADLINE, false)
            .await
    }

    async fn wait_ready<T>(
        self,
        owner: &Owner,
        completion: &mut oneshot::Sender<Result<T>>,
        maximum: Duration,
        before_launch: bool,
    ) -> Result<()> {
        let mut protection = owner.protection.clone();
        let mut settings = owner.settings.clone();
        let mut facts = owner.facts.clone();
        let until = Instant::now() + maximum;
        loop {
            // Mark before checking so a concurrent update cannot be missed
            // between the admission read and changed(). No guard crosses await.
            let expires = protection.borrow_and_update().until;
            settings.borrow_and_update();
            facts.borrow_and_update();
            if completion.is_closed() {
                bail!("marketplace helper cancelled before protected readiness");
            }
            if Instant::now() >= until {
                bail!("marketplace helper protected readiness deadline");
            }
            if before_launch {
                self.admit_start(owner)?;
            } else {
                // A helper already exists: never label its interruption as
                // positively unsent or permit a replacement before retirement.
                self.admit(owner, matches!(self, Self::Background))?;
            }
            if self.admit(owner, true).is_ok() {
                return Ok(());
            }
            // Only foreground Checking reaches this wait. Revalidate all
            // other gates rather than waiting out revoked consent or pressure.
            self.admit(owner, false)?;
            tokio::select! {biased;
                _ = completion.closed() => bail!("marketplace helper cancelled before protected readiness"),
                result = protection.changed() => result.context("marketplace helper protection owner stopped")?,
                result = settings.changed() => result.context("marketplace helper settings owner stopped")?,
                result = facts.changed() => result.context("marketplace helper host observation owner stopped")?,
                _ = tokio::time::sleep_until(expires.map_or(until, |expiry| expiry.min(until))) => {
                    bail!("marketplace helper protected readiness deadline");
                }
            }
        }
    }
}

impl Owner {
    pub(super) fn admit_foreground(&self, ready: bool) -> Result<()> {
        let phase = observed_state(&self.protection);
        if self.failed.load(Ordering::SeqCst) {
            bail!("foreground helper owner has failed");
        }
        if (ready && phase != ProtectionState::Ready)
            || !matches!(phase, ProtectionState::Ready | ProtectionState::Checking)
        {
            bail!("foreground helper protection is not admitted ({phase:?})");
        }
        if self.settings.has_changed().is_err() || self.facts.has_changed().is_err() {
            bail!("foreground helper settings or host observation owner has stopped");
        }
        // Current server state, not the slower background sample. Stable
        // playback alone is not a veto; startup/seek/fallback still wins.
        if self.playback.startup_queue_len() > 0 {
            bail!("foreground helper playback startup or seek is active");
        }
        let settings = self.settings.borrow();
        let explicitly_off = settings.mode == elixir_marketplace::policy::ContributionMode::Off
            && settings.revision.is_some_and(|revision| revision > 0);
        if !explicitly_off
            && settings.accepted_disclosure_version != Some(PARTICIPATION_DISCLOSURE_VERSION)
        {
            bail!("foreground helper disclosure is not accepted");
        }
        let facts = self.facts.borrow();
        let facts = facts
            .as_ref()
            .context("foreground helper host sample is unavailable")?;
        if facts.observed.elapsed() > Duration::from_secs(30) {
            bail!("foreground helper host sample is stale");
        }
        if facts
            .available_memory
            .is_none_or(|n| n < HOST_HELPER_HEADROOM)
        {
            bail!("foreground helper memory headroom is unavailable");
        }
        if !facts.disk_headroom {
            bail!("foreground helper disk headroom is unavailable");
        }
        if facts.foreground_resource_pressure {
            bail!("foreground helper host resource pressure is active");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
