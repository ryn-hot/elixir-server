//! Private joined-task outcome. A failed operation does not imply failed
//! physical cleanup, and a failed cleanup must never permit recovery. This is
//! not serializable, clonable or constructible by a peer/settings request.
use anyhow::Result;
use std::fmt::{self, Write};

// Private operator diagnostics only. Never retain a failure history or put
// error text into HTTP status, a cleanup receipt, readiness or authority.
const MAX_DIAGNOSTIC_BYTES: usize = 2048;
const MAX_DIAGNOSTIC_CAUSES: usize = 8;
struct Report(String);
impl Write for Report {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        for character in value.chars() {
            let character = if character.is_control() {
                ' '
            } else {
                character
            };
            if self.0.len() + character.len_utf8() > MAX_DIAGNOSTIC_BYTES {
                return Err(fmt::Error);
            }
            self.0.push(character);
        }
        Ok(())
    }
}
fn private_report(error: &anyhow::Error) -> String {
    let mut report = Report(String::with_capacity(MAX_DIAGNOSTIC_BYTES));
    for (index, cause) in error.chain().take(MAX_DIAGNOSTIC_CAUSES).enumerate() {
        if index > 0 && report.write_str(": ").is_err() {
            break;
        }
        if write!(report, "{cause}").is_err() {
            break;
        }
    }
    report.0
}

/// Preserve the first cause before a fail-closed latch turns later callers
/// into generic "unavailable" errors. No payload text in default logs or HTTP.
pub(super) fn trace_failure(owner: &'static str, error: &anyhow::Error) {
    tracing::warn!(
        owner,
        "marketplace operation failed before protection invalidation"
    );
    tracing::debug!(owner, error = %private_report(error), "private marketplace first failure");
}

pub(super) struct Exit {
    operation: Result<()>,
    cleanup: Result<()>,
}
impl Exit {
    pub fn new(operation: Result<()>, cleanup: Result<()>) -> Self {
        Self { operation, cleanup }
    }
    pub fn with_cleanup(self, next: Result<()>) -> Self {
        Self {
            operation: self.operation,
            cleanup: self.cleanup.and(next),
        }
    }
    /// Consume the original joined dependency, retaining its operational cause
    /// separately from physical cleanup. An unjoined dependency is NOT clean.
    pub fn with_dependency(self, joined: Result<Self>) -> Self {
        let dependency = match joined {
            Ok(dependency) => dependency,
            Err(error) => return self.with_cleanup(Err(error)),
        };
        let operation = match (self.operation, dependency.operation) {
            (Err(parent), Err(cause)) => Err(cause.context(parent)),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        };
        Self {
            operation,
            cleanup: self.cleanup.and(dependency.cleanup),
        }
    }
    pub fn succeeded(&self) -> bool {
        self.operation.is_ok() && self.cleanup.is_ok()
    }
    pub fn cleanup_confirmed(&self) -> bool {
        self.cleanup.is_ok()
    }
    /// Observe the original joined failures BEFORE a recovery caller consumes
    /// cleanup alone. Default warnings contain only closed local fields. Error
    /// chains require explicit debug logging and remain private, finite data.
    pub fn trace_failures(&self, owner: &'static str) {
        if let Err(error) = &self.operation {
            tracing::warn!(
                owner,
                cleanup_confirmed = self.cleanup_confirmed(),
                "marketplace runtime operation failed"
            );
            tracing::debug!(owner, error = %private_report(error), "private marketplace runtime operation failure");
        }
        if let Err(error) = &self.cleanup {
            tracing::warn!(owner, "marketplace runtime cleanup was not confirmed");
            tracing::debug!(owner, error = %private_report(error), "private marketplace runtime cleanup failure");
        }
    }
    /// Preserve the ordinary stop API's original operational failure.
    pub fn result(self) -> Result<()> {
        self.operation.and(self.cleanup)
    }
    /// Only a successfully joined owner may consume this cleanup outcome.
    /// Discarding an operation failure never discards an uncertain cleanup.
    pub fn cleanup(self) -> Result<()> {
        self.cleanup
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    #[test]
    fn private_failure_reports_preserve_causes_without_consuming_cleanup_or_authority() {
        let operation = anyhow!("native original cause").context("participation consequence");
        assert_eq!(
            private_report(&operation),
            "participation consequence: native original cause"
        );
        let exit = Exit::new(Err(operation), Err(anyhow!("separate cleanup cause")));
        exit.trace_failures("test");
        assert!(!exit.succeeded());
        assert!(!exit.cleanup_confirmed());
        assert_eq!(
            exit.cleanup().unwrap_err().to_string(),
            "separate cleanup cause"
        );
        let exit = Exit::new(Err(anyhow!("original failed")), Ok(()));
        exit.trace_failures("test");
        assert!(
            exit.cleanup().is_ok(),
            "logging cannot turn an operation failure into unknown cleanup"
        );
    }
    #[test]
    fn private_failure_reports_bound_bytes_depth_and_control_characters() {
        let error = anyhow!("{}\n\r\t\u{1b}", "é".repeat(MAX_DIAGNOSTIC_BYTES));
        let report = private_report(&error);
        assert!(report.len() <= MAX_DIAGNOSTIC_BYTES);
        assert!(report.is_char_boundary(report.len()));
        assert!(!report.chars().any(char::is_control));
        assert_eq!(
            private_report(&anyhow!("first\nsecond\u{1b}")),
            "first second "
        );
        let mut error = anyhow!("root");
        for _ in 0..100 {
            error = error.context("context");
        }
        let report = private_report(&error);
        assert_eq!(report.split(": ").count(), MAX_DIAGNOSTIC_CAUSES);
        assert!(!report.contains("root"));
    }
    #[test]
    fn operation_failure_and_physical_cleanup_are_independent() {
        assert!(
            Exit::new(Err(anyhow!("network failed")), Ok(()))
                .result()
                .is_err()
        );
        assert!(
            Exit::new(Err(anyhow!("network failed")), Ok(()))
                .cleanup()
                .is_ok()
        );
        assert!(
            Exit::new(Ok(()), Err(anyhow!("unknown cleanup")))
                .cleanup()
                .is_err()
        );
        assert!(
            Exit::new(
                Err(anyhow!("network failed")),
                Err(anyhow!("unknown cleanup"))
            )
            .cleanup()
            .is_err()
        );
        assert!(Exit::new(Ok(()), Ok(())).result().is_ok());
    }
    #[test]
    fn every_cleanup_stage_must_succeed() {
        assert!(
            Exit::new(Err(anyhow!("network failed")), Ok(()))
                .with_cleanup(Ok(()))
                .cleanup()
                .is_ok()
        );
        assert!(
            Exit::new(Ok(()), Err(anyhow!("guard unknown")))
                .with_cleanup(Ok(()))
                .cleanup()
                .is_err()
        );
        assert!(
            Exit::new(Ok(()), Ok(()))
                .with_cleanup(Err(anyhow!("worker unknown")))
                .cleanup()
                .is_err()
        );
    }
    #[test]
    fn a_joined_dependency_retains_both_operations_without_conflating_cleanup() {
        for parent_failed in [false, true] {
            for dependency_failed in [false, true] {
                for parent_cleanup_failed in [false, true] {
                    for dependency_cleanup_failed in [false, true] {
                        let outcome = |failed: bool, message: &'static str| {
                            if failed {
                                Err(anyhow!(message))
                            } else {
                                Ok(())
                            }
                        };
                        let joined = || {
                            Exit::new(
                                outcome(parent_failed, "participation stopped"),
                                outcome(parent_cleanup_failed, "parent cleanup unknown"),
                            )
                            .with_dependency(Ok(Exit::new(
                                outcome(dependency_failed, "original protection failure"),
                                outcome(dependency_cleanup_failed, "protection cleanup unknown"),
                            )))
                        };
                        let clean = !parent_cleanup_failed && !dependency_cleanup_failed;
                        assert_eq!(joined().cleanup_confirmed(), clean);
                        assert_eq!(joined().cleanup().is_ok(), clean);
                        assert_eq!(
                            joined().succeeded(),
                            clean && !parent_failed && !dependency_failed
                        );
                        let result = joined().result();
                        assert_eq!(
                            result.is_ok(),
                            clean && !parent_failed && !dependency_failed
                        );
                        if parent_failed && dependency_failed {
                            let report = format!("{:#}", result.unwrap_err());
                            assert!(report.contains("participation stopped"));
                            assert!(report.contains("original protection failure"));
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn an_unjoined_dependency_never_confirms_cleanup() {
        for parent_failed in [false, true] {
            let joined = || {
                Exit::new(
                    if parent_failed {
                        Err(anyhow!("parent failed"))
                    } else {
                        Ok(())
                    },
                    Ok(()),
                )
                .with_dependency(Err(anyhow!("original dependency join unknown")))
            };
            assert!(!joined().succeeded());
            assert!(!joined().cleanup_confirmed());
            assert!(joined().result().is_err());
            assert!(joined().cleanup().is_err());
        }
    }
}
