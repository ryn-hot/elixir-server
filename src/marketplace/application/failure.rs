//! Bounded private diagnostics for the original command, never an HTTP view,
//! cleanup acknowledgement, grant, retry instruction or installation authority.
use super::*;

#[derive(Clone, Default)]
pub(super) struct Failures(Arc<Mutex<State>>);
#[derive(Default)]
struct State {
    original: Option<Arc<anyhow::Error>>,
    cleanup: Option<Arc<anyhow::Error>>,
}
impl Failures {
    pub(super) fn original(&self, error: anyhow::Error) {
        if let Ok(mut state) = self.0.lock() {
            state.original.get_or_insert_with(|| {
                crate::marketplace::lifecycle::trace_failure("installation", &error);
                Arc::new(error)
            });
        }
    }
    pub(super) fn cleanup(&self, error: anyhow::Error) {
        if let Ok(mut state) = self.0.lock() {
            state.cleanup.get_or_insert_with(|| {
                crate::marketplace::lifecycle::trace_failure("installation_cleanup", &error);
                Arc::new(error)
            });
        }
    }
    // Only the existing authenticated RetryStage path clears an attempt.
    pub(super) fn clear(&self) {
        if let Ok(mut state) = self.0.lock() {
            *state = State::default();
        }
    }
    #[cfg(test)]
    pub(super) fn snapshot(
        &self,
    ) -> Result<(Option<Arc<anyhow::Error>>, Option<Arc<anyhow::Error>>)> {
        let state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install diagnostics unavailable"))?;
        Ok((state.original.clone(), state.cleanup.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_and_cleanup_failures_remain_independent_and_bounded() -> Result<()> {
        let failures = Failures::default();
        let same = failures.clone();
        failures.original(anyhow::anyhow!("first original"));
        let first = same.snapshot()?.0.context("original missing")?;
        failures.original(anyhow::anyhow!("later original"));
        failures.cleanup(anyhow::anyhow!("first cleanup"));
        let (original, cleanup) = same.snapshot()?;
        assert!(Arc::ptr_eq(&first, &original.context("original lost")?));
        let cleanup = cleanup.context("cleanup missing")?;
        failures.cleanup(anyhow::anyhow!("later cleanup"));
        assert!(Arc::ptr_eq(
            &cleanup,
            &same.snapshot()?.1.context("cleanup lost")?
        ));
        assert!(format!("{first:#}").contains("first original"));
        assert!(format!("{cleanup:#}").contains("first cleanup"));
        failures.clear();
        assert!(same.snapshot()?.0.is_none());
        assert!(same.snapshot()?.1.is_none());
        Ok(())
    }
}
