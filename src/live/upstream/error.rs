use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamErrorCode {
    InvalidUrl,
    SchemeForbidden,
    PortForbidden,
    HostForbidden,
    DestinationForbidden,
    RedirectInvalid,
    RedirectLoop,
    RedirectLimit,
    RedirectDowngrade,
    DnsFailed,
    DnsTimeout,
    DnsEmpty,
    DnsMixedScope,
    AddressForbidden,
    PrivateLanUnauthorized,
    NetworkScopeMismatch,
    HeaderRejected,
    CookieRejected,
    EgressRejected,
    PeerUnverified,
    ConnectTimeout,
    HeaderTimeout,
    IdleTimeout,
    TotalTimeout,
    Cancelled,
    UpstreamConnect,
    UpstreamProtocol,
    ResponseHeadersTooLarge,
    BodyTooLarge,
    SensitiveResponse,
}

impl UpstreamErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidUrl => "LIVE_UPSTREAM_INVALID_URL",
            Self::SchemeForbidden => "LIVE_UPSTREAM_SCHEME_FORBIDDEN",
            Self::PortForbidden => "LIVE_UPSTREAM_PORT_FORBIDDEN",
            Self::HostForbidden => "LIVE_UPSTREAM_HOST_FORBIDDEN",
            Self::DestinationForbidden => "LIVE_UPSTREAM_DESTINATION_FORBIDDEN",
            Self::RedirectInvalid => "LIVE_UPSTREAM_REDIRECT_INVALID",
            Self::RedirectLoop => "LIVE_UPSTREAM_REDIRECT_LOOP",
            Self::RedirectLimit => "LIVE_UPSTREAM_REDIRECT_LIMIT",
            Self::RedirectDowngrade => "LIVE_UPSTREAM_REDIRECT_DOWNGRADE",
            Self::DnsFailed => "LIVE_UPSTREAM_DNS_FAILED",
            Self::DnsTimeout => "LIVE_UPSTREAM_DNS_TIMEOUT",
            Self::DnsEmpty => "LIVE_UPSTREAM_DNS_EMPTY",
            Self::DnsMixedScope => "LIVE_UPSTREAM_DNS_MIXED_SCOPE",
            Self::AddressForbidden => "LIVE_UPSTREAM_ADDRESS_FORBIDDEN",
            Self::PrivateLanUnauthorized => "LIVE_UPSTREAM_PRIVATE_LAN_UNAUTHORIZED",
            Self::NetworkScopeMismatch => "LIVE_UPSTREAM_NETWORK_SCOPE_MISMATCH",
            Self::HeaderRejected => "LIVE_UPSTREAM_HEADER_REJECTED",
            Self::CookieRejected => "LIVE_UPSTREAM_COOKIE_REJECTED",
            Self::EgressRejected => "LIVE_UPSTREAM_EGRESS_REJECTED",
            Self::PeerUnverified => "LIVE_UPSTREAM_PEER_UNVERIFIED",
            Self::ConnectTimeout => "LIVE_UPSTREAM_CONNECT_TIMEOUT",
            Self::HeaderTimeout => "LIVE_UPSTREAM_HEADER_TIMEOUT",
            Self::IdleTimeout => "LIVE_UPSTREAM_IDLE_TIMEOUT",
            Self::TotalTimeout => "LIVE_UPSTREAM_TOTAL_TIMEOUT",
            Self::Cancelled => "LIVE_UPSTREAM_CANCELLED",
            Self::UpstreamConnect => "LIVE_UPSTREAM_CONNECT_FAILED",
            Self::UpstreamProtocol => "LIVE_UPSTREAM_PROTOCOL_FAILED",
            Self::ResponseHeadersTooLarge => "LIVE_UPSTREAM_HEADERS_TOO_LARGE",
            Self::BodyTooLarge => "LIVE_UPSTREAM_BODY_TOO_LARGE",
            Self::SensitiveResponse => "LIVE_UPSTREAM_SENSITIVE_RESPONSE",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct UpstreamError {
    code: UpstreamErrorCode,
}

impl UpstreamError {
    pub const fn new(code: UpstreamErrorCode) -> Self {
        Self { code }
    }

    pub const fn code(self) -> UpstreamErrorCode {
        self.code
    }
}

impl fmt::Debug for UpstreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamError")
            .field("code", &self.code.as_str())
            .finish()
    }
}

impl fmt::Display for UpstreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for UpstreamError {}

impl From<UpstreamErrorCode> for UpstreamError {
    fn from(code: UpstreamErrorCode) -> Self {
        Self::new(code)
    }
}

pub(crate) type Result<T> = std::result::Result<T, UpstreamError>;
