//! Separate application-data protection configuration. Never catalog credit,
//! an installed grant, native ownership, VPN readiness or permission to send.
mod gateway;
mod observation;
mod protection;
mod selection;
#[cfg(unix)]
mod warm;
#[cfg(unix)]
pub(in crate::marketplace) use warm::Pool as WarmPool;
#[cfg(unix)]
mod request;
#[cfg(unix)]
mod stream;
pub(in crate::marketplace) use gateway::GatewayInput;
pub(crate) use gateway::MATERIAL_ACCEPTED;
pub(in crate::marketplace) use protection::Protection;
#[cfg(unix)]
pub(in crate::marketplace) use request::Request;
#[cfg(unix)]
pub(in crate::marketplace) use stream::{Configuration, Stream};

// Public type names for RuntimeManager implementations, not public factories,
// mutable specifications, native leases or permission to open network input.
pub use crate::marketplace::lease::activity::gateway::GatewayCreateAttempt;
pub(crate) use gateway::ApplicationGatewayIntent;
pub use gateway::ApplicationGatewayProfile;
pub(crate) use gateway::ENTRYPOINT;
pub use observation::ApplicationGatewayObservation;
pub(crate) use observation::OBSERVE_GUARD;
