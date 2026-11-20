//! The installed application slot during an owner uninstall. While the slot is
//! held no install, update, restoration or installed call can start; a call
//! already running is cancelled through its own task, and the retained
//! controller stays with the uninstall until it ends.
use super::*;

/// Why the slot cannot be taken now (the uninstall answers 409).
#[derive(Debug)]
pub(in crate::marketplace) struct SlotBusy(pub &'static str);

/// Held for the whole uninstall. Dropping it gives the controller back.
pub(in crate::marketplace) struct UninstallSlot {
    service: Service,
    controller: Option<InstalledController>,
}

impl Drop for UninstallSlot {
    fn drop(&mut self) {
        let mut state = self
            .service
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.uninstalling = false;
        if let Some(controller) = self.controller.take()
            && state.controller.is_none()
        {
            state.controller = Some(controller);
        }
    }
}

impl Service {
    pub(in crate::marketplace) async fn begin_uninstall(&self) -> Result<UninstallSlot, SlotBusy> {
        // Finished work gives its controller back first; failures here only
        // mean the work is still owned, which the checks below report.
        let _ = self.reap_finished();
        let _ = self.reap_invocation();
        let running = {
            let mut state = self
                .0
                .lock()
                .map_err(|_| SlotBusy("the installed extension owner is unavailable"))?;
            if state.stopped {
                return Err(SlotBusy("the server is stopping"));
            }
            if state.uninstalling {
                return Err(SlotBusy("another extension is being uninstalled"));
            }
            if state.restoration.is_some() {
                return Err(SlotBusy("installed extensions are still being restored"));
            }
            if state.entry.is_some() {
                return Err(SlotBusy(
                    "an extension install, update or activation is in progress",
                ));
            }
            state.uninstalling = true;
            state.invocation.clone()
        };
        // From here on, dropping the slot clears the flag again.
        let mut slot = UninstallSlot {
            service: self.clone(),
            controller: None,
        };
        if let Some(running) = running {
            running
                .cancel()
                .await
                .map_err(|_| SlotBusy("a running extension call did not stop"))?;
            self.reap_invocation()
                .map_err(|_| SlotBusy("a running extension call did not stop"))?;
        }
        let mut state = self
            .0
            .lock()
            .map_err(|_| SlotBusy("the installed extension owner is unavailable"))?;
        if state.invocation.is_some() || state.entry.is_some() {
            return Err(SlotBusy("a running extension call did not stop"));
        }
        if let Some(controller) = &state.controller
            && controller.require_idle().is_err()
        {
            return Err(SlotBusy("the installed extension controller is still busy"));
        }
        slot.controller = state.controller.take();
        drop(state);
        Ok(slot)
    }
}
