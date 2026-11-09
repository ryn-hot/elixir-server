//! Keep one private preflight cause, not a history or an availability receipt.
//! Public summaries stay unchanged; a failed mirror never creates a source.
#[derive(Default)]
pub(in crate::marketplace::search) struct SourceFailures(Option<anyhow::Error>);

impl SourceFailures {
    pub(in crate::marketplace::search) fn record<T>(
        &mut self,
        result: anyhow::Result<T>,
    ) -> Option<T> {
        match result {
            Ok(source) => Some(source),
            Err(error) => {
                if self.0.is_none() {
                    self.0 = Some(error);
                }
                None
            }
        }
    }

    pub(in crate::marketplace::search) fn unavailable(
        self,
        summary: &'static str,
    ) -> anyhow::Error {
        match self.0 {
            Some(error) => error.context(summary),
            None => anyhow::anyhow!(summary),
        }
    }

    pub(in crate::marketplace::search) fn unavailable_error<
        E: std::error::Error + Send + Sync + 'static,
    >(
        self,
        summary: E,
    ) -> anyhow::Error {
        match self.0 {
            Some(error) => error.context(summary),
            None => anyhow::Error::new(summary),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io, sync::Arc};

    #[test]
    fn failed_mirrors_keep_the_first_typed_cause_and_unchanged_public_summary() {
        let mut failures = SourceFailures::default();
        let first = anyhow::Error::new(io::Error::new(
            io::ErrorKind::TimedOut,
            "first private source cause",
        ))
        .context("first native source boundary");
        assert!(failures.record::<()>(Err(first)).is_none());
        assert!(
            failures
                .record::<()>(Err(anyhow::anyhow!("discarded second mirror cause")))
                .is_none()
        );
        let error = failures.unavailable("accepted direct public source is unavailable");
        assert_eq!(
            error.to_string(),
            "accepted direct public source is unavailable"
        );
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::TimedOut
        );
        let private = format!("{error:#}");
        assert!(private.contains("first native source boundary"));
        assert!(private.contains("first private source cause"));
        assert!(!private.contains("discarded second mirror cause"));
    }

    #[test]
    fn successful_source_is_returned_unchanged_and_empty_attempts_keep_the_old_error() {
        let mut failures = SourceFailures::default();
        assert!(
            failures
                .record::<Arc<u8>>(Err(anyhow::anyhow!("first mirror unavailable")))
                .is_none()
        );
        let source = Arc::new(7);
        let observed = failures.record(Ok(source.clone())).unwrap();
        assert!(Arc::ptr_eq(&source, &observed));
        let error = SourceFailures::default().unavailable("selected public source unavailable");
        assert_eq!(error.to_string(), "selected public source unavailable");
        assert_eq!(error.chain().count(), 1);
    }

    #[test]
    fn typed_source_unavailability_keeps_both_feedback_marker_and_original_cause() {
        use crate::marketplace::search::PublicSourceUnavailable;
        for with_cause in [false, true] {
            let mut failures = SourceFailures::default();
            if with_cause {
                failures.record::<()>(Err(anyhow::Error::new(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "original private source cause",
                ))));
            }
            let error = failures.unavailable_error(PublicSourceUnavailable);
            assert!(error.is::<PublicSourceUnavailable>());
            if with_cause {
                assert_eq!(
                    error.downcast_ref::<io::Error>().unwrap().kind(),
                    io::ErrorKind::TimedOut
                );
                assert!(format!("{error:#}").contains("original private source cause"));
            }
        }
    }
}
