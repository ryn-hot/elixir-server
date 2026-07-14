//! Supervised, copy-only Live protocol adaptation.

mod adapter;
mod profile;
mod service;

pub use service::{
    LiveRemuxBuildError, LiveRemuxError, LiveRemuxJobDiagnostics, LiveRemuxPayload,
    LiveRemuxPayloadBody, LiveRemuxReconciliation, LiveRemuxService, LiveRemuxSnapshot,
};

#[cfg(test)]
mod tests;
