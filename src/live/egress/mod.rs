//! Live protected-egress boundary.

mod connector;
mod control;
mod material;
mod policy;
mod repository;
mod service;
mod worker;

pub(crate) use connector::ProtectedEgressTransport;
pub(crate) use policy::validate_effective_policy;
pub use policy::{
    EffectiveEgressPolicy, EgressPolicyMode, EgressPolicySelectionError, EgressPolicySource,
    PolicyCandidate, PolicyScope, SessionEgressPolicyRequest, select_effective_policy,
};
pub use repository::{EgressPolicyRepository, EgressPolicyRepositoryError, StoredPolicyAssignment};
pub use service::{
    LiveEgressError, LiveEgressOutcome, LiveEgressProfileStatus, LiveEgressService,
    LiveEgressStatus,
};
pub use worker::run_live_egress_worker_from_environment;
