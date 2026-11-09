//! DNS-pinned, credential-scoped Live upstream-fetch boundary.

mod configured_lan;
mod connector;
mod credentials;
mod error;
mod fetcher;
mod policy;
mod resolver;

pub(crate) use configured_lan::{ConfiguredLanEndpoint, LanEndpointAccess};
pub use connector::DirectEgressConnector;
pub(crate) use connector::{EgressConnector, PreparedRequest};
pub use credentials::{CredentialSet, SafeRequestHeaders};
pub use error::{UpstreamError, UpstreamErrorCode};
pub use fetcher::{
    FetchRequest, FetchStats, UpstreamBody, UpstreamChunk, UpstreamFetcher, UpstreamLimits,
    UpstreamMethod, UpstreamResponse,
};
pub(crate) use policy::ResolvedTarget;
pub use policy::{
    BlockedNetwork, DestinationPolicy, DestinationRule, LocalDestinationDenylist, NetworkScope,
    PrivateLanGate, ResponseOrigin,
};
pub use resolver::{DnsResolver, HostGatewayDnsResolver, SystemDnsResolver};

#[cfg(test)]
mod tests;
