//! Configured endpoint restrictions, NOT a grant or execution capability.
//! The broker must obtain the endpoint and access mode from current, reviewed
//! owner configuration, and independently retain/recheck its native authority.
//! No discovery, DNS, request, host exception or public fallback happens here.
use std::fmt;

use reqwest::Url;

use super::{
    DestinationPolicy, DestinationRule, LocalDestinationDenylist, NetworkScope, PrivateLanGate,
    error::{Result, UpstreamErrorCode},
};

/// Origin-wide access is an explicit stronger contract, never inferred from a
/// URL's trailing slash. Both modes still prohibit redirects to other URLs in
/// the per-request policy returned below.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LanEndpointAccess {
    ExactUrl,
    Origin,
}

pub(crate) struct ConfiguredLanEndpoint {
    configured: Url,
    access: LanEndpointAccess,
}
impl fmt::Debug for ConfiguredLanEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfiguredLanEndpoint")
            .field("endpoint", &"<redacted>")
            .field("access", &self.access)
            .finish()
    }
}
impl ConfiguredLanEndpoint {
    pub(crate) fn new(configured: &str, access: LanEndpointAccess) -> Result<Self> {
        Ok(Self {
            configured: parse(configured)?,
            access,
        })
    }

    pub(crate) fn accepts(&self, requested: &str) -> Result<bool> {
        let requested = parse(requested)?;
        Ok(match self.access {
            LanEndpointAccess::ExactUrl => requested == self.configured,
            LanEndpointAccess::Origin => requested.origin() == self.configured.origin(),
        })
    }

    /// Construct a restriction for THIS complete request URL, including its
    /// query. A caller cannot validate one URL then use this policy for another
    /// path/query or redirect. Resolution still requires only private addresses
    /// and applies the unchanged local-host/control-plane destination denylist.
    pub(crate) fn policy_for(
        &self,
        requested: &str,
        gate: PrivateLanGate,
        denylist: LocalDestinationDenylist,
    ) -> Result<DestinationPolicy> {
        // Deny before DNS, even when the owner endpoint has a DNS hostname.
        if !gate.permits() {
            return Err(UpstreamErrorCode::PrivateLanUnauthorized.into());
        }
        if !self.accepts(requested)? {
            return Err(UpstreamErrorCode::DestinationForbidden.into());
        }
        let requested = parse(requested)?;
        let rule = DestinationRule::for_exact_lan_url(&requested)?;
        let policy =
            DestinationPolicy::new(vec![rule], gate, requested.scheme() == "http", denylist)?;
        policy.validate_initial(requested.as_str())?;
        Ok(policy)
    }
}

fn parse(raw: &str) -> Result<Url> {
    if raw.is_empty()
        || raw.len() > super::policy::MAX_URL_BYTES
        || raw.chars().any(char::is_control)
        || raw.contains('\\')
    {
        return Err(UpstreamErrorCode::InvalidUrl.into());
    }
    let url = Url::parse(raw).map_err(|_| UpstreamErrorCode::InvalidUrl)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.port_or_known_default().is_none_or(|port| port == 0)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(UpstreamErrorCode::InvalidUrl.into());
    }
    // Reuse the Live URL/hostname validation without fabricating a local-host
    // denylist. This public-scope policy is ONLY a syntax check; it is not
    // returned, never resolves anything and confers no private address access.
    let policy = DestinationPolicy::new(
        vec![DestinationRule::new(
            url.scheme(),
            url.host_str().ok_or(UpstreamErrorCode::HostForbidden)?,
            url.port_or_known_default()
                .ok_or(UpstreamErrorCode::PortForbidden)?,
            url.path(),
            NetworkScope::Public,
            true,
        )?],
        PrivateLanGate::default(),
        url.scheme() == "http",
        LocalDestinationDenylist::empty(),
    )?;
    policy.validate_initial(url.as_str())?;
    Ok(url)
}

#[cfg(test)]
mod tests;
