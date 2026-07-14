//! Durable, fenced lifecycle for standalone Live playback sessions.

mod descriptor;
mod lifecycle;
mod recovery;
mod repository;
mod types;

pub use lifecycle::{LiveSessionLifecycle, SessionLifecycleError, SessionReconciliationReport};
pub use recovery::{
    RecoveryAction, RecoveryEvent, RecoveryOutcome, RecoveryPolicy, RecoveryPolicyError,
    RecoveryReason, RequestedEgressMode, StoredRecoveryState,
};
pub use repository::{
    CryptoRotationReport, LiveSessionRepository, SessionCleanupReport, SessionRepositoryError,
};
pub use types::{
    DeliveryMode, IdempotencyRequest, LiveTrackPreferenceUpdate, LiveTrackPreferences,
    LiveTrackSelection, NewSession, SessionGrant, SessionMutation, SessionOwner, SessionProtocol,
    SessionRecord, SessionRecoveryFailure, SessionRecoveryReplacement, SessionSecretMaterial,
    SessionState, TerminalReason,
};

#[cfg(test)]
pub(crate) mod tests;
pub use descriptor::{
    StoredAuthority, StoredCookie, StoredDescriptorError, StoredEgressPolicy,
    StoredSessionDescriptor, StoredSource,
};
