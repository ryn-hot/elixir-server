//! One coalesced configuration-change hint and bounded retry visibility. This
//! owns no packet/process/provider authority and cannot create a startup task.
use anyhow::{Result, bail};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tokio::{
    sync::Notify,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct StartupView {
    pub configured: bool,
    pub retry_owner_present: bool,
    pub retry_after_seconds: Option<u64>,
}
#[derive(Default)]
struct State {
    configured: bool,
    live: bool,
    retry: Option<Instant>,
    pending: bool,
}
#[derive(Default)]
pub(super) struct Control {
    state: Mutex<State>,
    changed: Notify,
}
pub(in crate::marketplace) struct StartupOwner(Arc<Control>);
impl Control {
    pub(super) fn select(&self) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .configured = true;
    }
    pub(super) fn claim(self: &Arc<Self>) -> Result<StartupOwner> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.configured || state.live {
            bail!("marketplace startup progress already owned or unconfigured");
        }
        state.live = true;
        state.retry = None;
        Ok(StartupOwner(self.clone()))
    }
    pub(super) fn snapshot(&self) -> StartupView {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        StartupView {
            configured: state.configured,
            retry_owner_present: state.live,
            retry_after_seconds: state.retry.map(|until| {
                let remaining = until.saturating_duration_since(Instant::now());
                remaining.as_secs() + u64::from(remaining.subsec_nanos() != 0)
            }),
        }
    }
    pub(super) fn notify(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.configured || !state.live {
            return false;
        }
        // Notify alone can retain a permit in addition to a wake already
        // assigned to a waiter. Gate both with one pending bit so a burst
        // cannot leave an extra retry hint for the following attempt.
        if !state.pending {
            state.pending = true;
            self.changed.notify_one();
        }
        true
    }
    pub(super) async fn changed(&self) {
        self.changed.notified().await;
        self.state.lock().unwrap_or_else(|p| p.into_inner()).pending = false;
    }
    pub(super) fn retry(&self, delay: Option<Duration>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.live {
            state.retry = delay.map(|delay| Instant::now() + delay.min(Duration::from_secs(300)));
        }
    }
}
impl Drop for StartupOwner {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        state.live = false;
        state.retry = None;
    }
}
#[cfg(test)]
mod tests;
