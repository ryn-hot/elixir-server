//! Pure delivery planning for canonical Live source descriptors.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::live::{
    contract::{ClientDisclosure, ResolvedSources, ServerEgress, SourceDescriptor, StreamProtocol},
    egress::EgressPolicyMode,
    session::DeliveryMode,
};

const MAX_DISCLOSURE_RULES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerReason {
    PublicCompatibleDirect,
    DirectDisabledRequiresRelay,
    DisclosureRuleMissingRequiresRelay,
    ProviderServerOnlyRequiresRelay,
    SensitiveHeadersRequireRelay,
    SensitiveCookiesRequireRelay,
    SensitiveOriginRequireRelay,
    SensitiveRefererRequireRelay,
    SensitiveUrlRequireRelay,
    RefreshHandleRequiresRelay,
    ProviderServerEgressRequiresRelay,
    ProviderServerEgressPreferredRelay,
    ProtectedEgressRequiresRelay,
    ProtectedEgressPreferredRelay,
    PrivateNetworkRequiresRelay,
    DirectRecoveryRequiresServer,
    CompatibleServerRelay,
    ClientProtocolRequiresRemux,
    UnsupportedContainerRequiresRemux,
}

impl PlannerReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublicCompatibleDirect => "public_compatible_direct",
            Self::DirectDisabledRequiresRelay => "direct_disabled_requires_relay",
            Self::DisclosureRuleMissingRequiresRelay => "disclosure_rule_missing_requires_relay",
            Self::ProviderServerOnlyRequiresRelay => "provider_server_only_requires_relay",
            Self::SensitiveHeadersRequireRelay => "sensitive_headers_require_relay",
            Self::SensitiveCookiesRequireRelay => "sensitive_cookies_require_relay",
            Self::SensitiveOriginRequireRelay => "sensitive_origin_requires_relay",
            Self::SensitiveRefererRequireRelay => "sensitive_referer_requires_relay",
            Self::SensitiveUrlRequireRelay => "sensitive_url_requires_relay",
            Self::RefreshHandleRequiresRelay => "refresh_handle_requires_relay",
            Self::ProviderServerEgressRequiresRelay => "provider_server_egress_requires_relay",
            Self::ProviderServerEgressPreferredRelay => "provider_server_egress_preferred_relay",
            Self::ProtectedEgressRequiresRelay => "protected_egress_requires_relay",
            Self::ProtectedEgressPreferredRelay => "protected_egress_preferred_relay",
            Self::PrivateNetworkRequiresRelay => "private_network_requires_relay",
            Self::DirectRecoveryRequiresServer => "direct_recovery_requires_server",
            Self::CompatibleServerRelay => "compatible_server_relay",
            Self::ClientProtocolRequiresRemux => "client_protocol_requires_remux",
            Self::UnsupportedContainerRequiresRemux => "unsupported_container_requires_remux",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerRejectionCode {
    InvalidInput,
    MalformedDescriptor,
    ProtocolMismatch,
    DescriptorExpired,
    TimeShiftUnavailable,
    PrivateNetworkForbidden,
    ProtectedEgressUnavailable,
    ClientProtocolUnsupported,
    ClientCodecUnsupported,
    ClientContainerUnsupported,
    RelayDisabled,
    RelayCapacity,
    RemuxDisabled,
    RemuxCapacity,
    RemuxProfileUnsupported,
}

impl PlannerRejectionCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::MalformedDescriptor => "malformed_descriptor",
            Self::ProtocolMismatch => "protocol_mismatch",
            Self::DescriptorExpired => "descriptor_expired",
            Self::TimeShiftUnavailable => "time_shift_unavailable",
            Self::PrivateNetworkForbidden => "private_network_forbidden",
            Self::ProtectedEgressUnavailable => "protected_egress_unavailable",
            Self::ClientProtocolUnsupported => "client_protocol_unsupported",
            Self::ClientCodecUnsupported => "client_codec_unsupported",
            Self::ClientContainerUnsupported => "client_container_unsupported",
            Self::RelayDisabled => "relay_disabled",
            Self::RelayCapacity => "relay_capacity",
            Self::RemuxDisabled => "remux_disabled",
            Self::RemuxCapacity => "remux_capacity",
            Self::RemuxProfileUnsupported => "remux_profile_unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressMode {
    ServerDefault,
    Protected,
    PrivateLan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenPolicy {
    None,
    HeaderBearer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemuxProfile {
    DashToHlsCopy,
    MpegTsToHlsCopy,
    RtmpToHlsCopy,
    SrtToHlsCopy,
}

impl RemuxProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DashToHlsCopy => "dash_to_hls_copy",
            Self::MpegTsToHlsCopy => "mpeg_ts_to_hls_copy",
            Self::RtmpToHlsCopy => "rtmp_to_hls_copy",
            Self::SrtToHlsCopy => "srt_to_hls_copy",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateRejection {
    pub source_index: usize,
    pub code: PlannerRejectionCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryPlan {
    pub mode: DeliveryMode,
    pub reason: PlannerReason,
    pub egress: Option<EgressMode>,
    pub selected_source_index: usize,
    pub remux_profile: Option<RemuxProfile>,
    pub token_policy: TokenPolicy,
    pub fallback_candidates: Vec<usize>,
    pub rejected_candidates: Vec<CandidateRejection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerRejection {
    pub code: PlannerRejectionCode,
    pub rejected_candidates: Vec<CandidateRejection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCapabilities {
    pub protocols: BTreeSet<StreamProtocol>,
    pub containers: BTreeSet<String>,
    pub video_codecs: BTreeSet<String>,
    pub audio_codecs: BTreeSet<String>,
}

impl ClientCapabilities {
    pub fn validate(&self) -> bool {
        !self.protocols.is_empty()
            && self.containers.len() <= 64
            && self.video_codecs.len() <= 64
            && self.audio_codecs.len() <= 64
            && self
                .containers
                .iter()
                .chain(self.video_codecs.iter())
                .chain(self.audio_codecs.iter())
                .all(|value| valid_capability_name(value))
    }

    fn supports_protocol(&self, protocol: StreamProtocol) -> bool {
        self.protocols.contains(&protocol)
    }

    fn supports_container(&self, container: Option<&str>) -> bool {
        container.is_none_or(|value| self.containers.contains(&normalize_name(value)))
    }

    fn supports_codecs(&self, source: &SourceDescriptor) -> bool {
        source.media.as_ref().is_none_or(|media| {
            media
                .video_codec
                .as_deref()
                .is_none_or(|value| self.video_codecs.contains(&normalize_name(value)))
                && media
                    .audio_codec
                    .as_deref()
                    .is_none_or(|value| self.audio_codecs.contains(&normalize_name(value)))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectDisclosureRule {
    scheme: String,
    host: String,
    port: u16,
    exact_path: String,
    public_network: bool,
    allow_client_disclosure: bool,
}

impl DirectDisclosureRule {
    pub fn new(
        scheme: &str,
        host: &str,
        port: u16,
        exact_path: &str,
        public_network: bool,
        allow_client_disclosure: bool,
    ) -> Result<Self, PlannerRejectionCode> {
        Self::new_inner(
            scheme,
            host,
            port,
            exact_path,
            public_network,
            allow_client_disclosure,
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_loopback(
        scheme: &str,
        host: &str,
        port: u16,
        exact_path: &str,
        public_network: bool,
        allow_client_disclosure: bool,
    ) -> Result<Self, PlannerRejectionCode> {
        Self::new_inner(
            scheme,
            host,
            port,
            exact_path,
            public_network,
            allow_client_disclosure,
            true,
        )
    }

    fn new_inner(
        scheme: &str,
        host: &str,
        port: u16,
        exact_path: &str,
        public_network: bool,
        allow_client_disclosure: bool,
        allow_test_loopback: bool,
    ) -> Result<Self, PlannerRejectionCode> {
        let scheme = scheme.to_ascii_lowercase();
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        if scheme != "https"
            || host.is_empty()
            || host.len() > 253
            || host.parse::<std::net::IpAddr>().is_ok()
            || (!allow_test_loopback && (host == "localhost" || host.ends_with(".localhost")))
            || host.ends_with(".local")
            || host.ends_with(".internal")
            || port == 0
            || !valid_exact_path(exact_path)
            || !host.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
            })
        {
            return Err(PlannerRejectionCode::InvalidInput);
        }
        Ok(Self {
            scheme,
            host,
            port,
            exact_path: exact_path.to_string(),
            public_network,
            allow_client_disclosure,
        })
    }

    fn permits(&self, url: &Url) -> bool {
        self.public_network
            && self.allow_client_disclosure
            && self.scheme == url.scheme()
            && url
                .host_str()
                .is_some_and(|host| self.host == host.trim_end_matches('.').to_ascii_lowercase())
            && self.port == url.port_or_known_default().unwrap_or(0)
            && self.exact_path == url.path()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerPolicy {
    pub client_direct_enabled: bool,
    pub relay_enabled: bool,
    pub remux_enabled: bool,
    pub relay_capacity_available: bool,
    pub remux_capacity_available: bool,
    pub protected_egress_mode: EgressPolicyMode,
    pub protected_egress_ready: bool,
    pub allow_private_lan_sources: bool,
    pub provider_private_network_permission: bool,
    pub native_dash_relay_enabled: bool,
    pub rtmp_remux_enabled: bool,
    pub srt_remux_enabled: bool,
    pub disclosure_rules: Vec<DirectDisclosureRule>,
}

impl PlannerPolicy {
    fn validate(&self) -> bool {
        self.disclosure_rules.len() <= MAX_DISCLOSURE_RULES
            && (!self.remux_enabled || self.relay_enabled)
            && (!self.native_dash_relay_enabled || self.relay_enabled)
            && (!self.rtmp_remux_enabled || self.remux_enabled)
            && (!self.srt_remux_enabled || self.remux_enabled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaybackRequirements {
    pub require_time_shift: bool,
    pub require_server_delivery: bool,
}

pub struct PlannerInput<'a> {
    pub sources: &'a ResolvedSources,
    pub client: &'a ClientCapabilities,
    pub policy: &'a PlannerPolicy,
    pub requirements: PlaybackRequirements,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
struct CandidatePlan {
    mode: DeliveryMode,
    reason: PlannerReason,
    egress: Option<EgressMode>,
    remux_profile: Option<RemuxProfile>,
    token_policy: TokenPolicy,
}

pub fn plan_delivery(input: &PlannerInput<'_>) -> Result<DeliveryPlan, PlannerRejection> {
    if !input.client.validate() || !input.policy.validate() || input.sources.alternatives.len() > 15
    {
        return Err(PlannerRejection {
            code: PlannerRejectionCode::InvalidInput,
            rejected_candidates: Vec::new(),
        });
    }
    let sources = std::iter::once(&input.sources.descriptor)
        .chain(input.sources.alternatives.iter())
        .collect::<Vec<_>>();
    let mut selected = None;
    let mut viable = Vec::new();
    let mut rejected = Vec::new();
    for (source_index, source) in sources.iter().enumerate() {
        match evaluate_source(source, input) {
            Ok(candidate) => {
                if selected.is_none() {
                    selected = Some((source_index, candidate));
                } else {
                    viable.push(source_index);
                }
            }
            Err(code) => rejected.push(CandidateRejection { source_index, code }),
        }
    }
    let Some((selected_source_index, selected)) = selected else {
        return Err(PlannerRejection {
            code: rejected
                .first()
                .map_or(PlannerRejectionCode::InvalidInput, |item| item.code),
            rejected_candidates: rejected,
        });
    };
    Ok(DeliveryPlan {
        mode: selected.mode,
        reason: selected.reason,
        egress: selected.egress,
        selected_source_index,
        remux_profile: selected.remux_profile,
        token_policy: selected.token_policy,
        fallback_candidates: viable,
        rejected_candidates: rejected,
    })
}

fn evaluate_source(
    source: &SourceDescriptor,
    input: &PlannerInput<'_>,
) -> Result<CandidatePlan, PlannerRejectionCode> {
    let url =
        Url::parse(source.url.expose()).map_err(|_| PlannerRejectionCode::MalformedDescriptor)?;
    if !source.protocol.expected_scheme().contains(&url.scheme()) {
        return Err(PlannerRejectionCode::ProtocolMismatch);
    }
    if source.expires_at.is_some_and(|expiry| expiry <= input.now) {
        return Err(PlannerRejectionCode::DescriptorExpired);
    }
    if input.requirements.require_time_shift && !source.time_shift.available {
        return Err(PlannerRejectionCode::TimeShiftUnavailable);
    }
    if source.private_network
        && !(input.policy.allow_private_lan_sources
            && input.policy.provider_private_network_permission)
    {
        return Err(PlannerRejectionCode::PrivateNetworkForbidden);
    }
    if !source.private_network
        && input.policy.protected_egress_mode != EgressPolicyMode::Off
        && !input.policy.protected_egress_ready
    {
        return Err(PlannerRejectionCode::ProtectedEgressUnavailable);
    }
    if !input.client.supports_codecs(source) {
        return Err(PlannerRejectionCode::ClientCodecUnsupported);
    }

    let direct_constraint = direct_constraint(source, &url, input.policy, input.requirements);
    let direct_protocol = input.client.supports_protocol(source.protocol);
    let direct_container = input.client.supports_container(
        source
            .media
            .as_ref()
            .and_then(|media| media.container.as_deref()),
    );
    if direct_constraint.is_none() && direct_protocol && direct_container {
        return Ok(CandidatePlan {
            mode: DeliveryMode::ClientDirect,
            reason: PlannerReason::PublicCompatibleDirect,
            egress: None,
            remux_profile: None,
            token_policy: TokenPolicy::None,
        });
    }

    let relay_reason = direct_constraint.unwrap_or(PlannerReason::CompatibleServerRelay);
    if relay_supported(source.protocol, input.policy) && direct_protocol && direct_container {
        if !input.policy.relay_enabled {
            return Err(PlannerRejectionCode::RelayDisabled);
        }
        if !input.policy.relay_capacity_available {
            return Err(PlannerRejectionCode::RelayCapacity);
        }
        return Ok(CandidatePlan {
            mode: DeliveryMode::ServerRelay,
            reason: relay_reason,
            egress: Some(source_egress(source, input.policy)),
            remux_profile: None,
            token_policy: TokenPolicy::HeaderBearer,
        });
    }

    if !direct_protocol && !remux_protocol_possible(source.protocol, input.policy) {
        return Err(PlannerRejectionCode::ClientProtocolUnsupported);
    }
    if direct_protocol
        && !direct_container
        && !remux_protocol_possible(source.protocol, input.policy)
    {
        return Err(PlannerRejectionCode::ClientContainerUnsupported);
    }
    plan_remux(source, input, direct_protocol, direct_container)
}

fn direct_constraint(
    source: &SourceDescriptor,
    url: &Url,
    policy: &PlannerPolicy,
    requirements: PlaybackRequirements,
) -> Option<PlannerReason> {
    if source.private_network {
        return Some(PlannerReason::PrivateNetworkRequiresRelay);
    }
    match policy.protected_egress_mode {
        EgressPolicyMode::RequireProtected => {
            return Some(PlannerReason::ProtectedEgressRequiresRelay);
        }
        EgressPolicyMode::PreferProtected => {
            return Some(PlannerReason::ProtectedEgressPreferredRelay);
        }
        EgressPolicyMode::Off => {}
    }
    if source.server_egress == ServerEgress::Required {
        return Some(PlannerReason::ProviderServerEgressRequiresRelay);
    }
    if source.server_egress == ServerEgress::Preferred {
        return Some(PlannerReason::ProviderServerEgressPreferredRelay);
    }
    if !source.request_headers.is_empty() {
        return Some(PlannerReason::SensitiveHeadersRequireRelay);
    }
    if !source.cookies.is_empty() {
        return Some(PlannerReason::SensitiveCookiesRequireRelay);
    }
    if source.origin.is_some() {
        return Some(PlannerReason::SensitiveOriginRequireRelay);
    }
    if source.referer.is_some() {
        return Some(PlannerReason::SensitiveRefererRequireRelay);
    }
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Some(PlannerReason::SensitiveUrlRequireRelay);
    }
    if source.refresh_handle.is_some() || source.expires_at.is_some() {
        return Some(PlannerReason::RefreshHandleRequiresRelay);
    }
    if source.client_disclosure != ClientDisclosure::Public {
        return Some(PlannerReason::ProviderServerOnlyRequiresRelay);
    }
    if !policy.disclosure_rules.iter().any(|rule| rule.permits(url)) {
        return Some(PlannerReason::DisclosureRuleMissingRequiresRelay);
    }
    if requirements.require_server_delivery {
        return Some(PlannerReason::DirectRecoveryRequiresServer);
    }
    if !policy.client_direct_enabled {
        return Some(PlannerReason::DirectDisabledRequiresRelay);
    }
    None
}

fn relay_supported(protocol: StreamProtocol, policy: &PlannerPolicy) -> bool {
    match protocol {
        StreamProtocol::Hls | StreamProtocol::HttpProgressive | StreamProtocol::MpegTs => true,
        StreamProtocol::Dash => policy.native_dash_relay_enabled,
        StreamProtocol::Rtmp | StreamProtocol::Srt => false,
    }
}

fn remux_protocol_possible(protocol: StreamProtocol, policy: &PlannerPolicy) -> bool {
    match protocol {
        StreamProtocol::Dash | StreamProtocol::MpegTs => true,
        StreamProtocol::Rtmp => policy.rtmp_remux_enabled,
        StreamProtocol::Srt => policy.srt_remux_enabled,
        StreamProtocol::Hls | StreamProtocol::HttpProgressive => false,
    }
}

fn plan_remux(
    source: &SourceDescriptor,
    input: &PlannerInput<'_>,
    direct_protocol: bool,
    direct_container: bool,
) -> Result<CandidatePlan, PlannerRejectionCode> {
    if !input.policy.remux_enabled {
        return Err(PlannerRejectionCode::RemuxDisabled);
    }
    if !input.policy.remux_capacity_available {
        return Err(PlannerRejectionCode::RemuxCapacity);
    }
    if !input.client.supports_protocol(StreamProtocol::Hls) {
        return Err(PlannerRejectionCode::ClientProtocolUnsupported);
    }
    if !input.client.supports_container(Some("mpegts")) {
        return Err(PlannerRejectionCode::ClientContainerUnsupported);
    }
    if !copy_compatible(source) {
        return Err(PlannerRejectionCode::RemuxProfileUnsupported);
    }
    let profile = match source.protocol {
        StreamProtocol::Dash => RemuxProfile::DashToHlsCopy,
        StreamProtocol::MpegTs => RemuxProfile::MpegTsToHlsCopy,
        StreamProtocol::Rtmp if input.policy.rtmp_remux_enabled => RemuxProfile::RtmpToHlsCopy,
        StreamProtocol::Srt if input.policy.srt_remux_enabled => RemuxProfile::SrtToHlsCopy,
        _ => return Err(PlannerRejectionCode::RemuxProfileUnsupported),
    };
    Ok(CandidatePlan {
        mode: DeliveryMode::ServerRemux,
        reason: if direct_protocol && !direct_container {
            PlannerReason::UnsupportedContainerRequiresRemux
        } else {
            PlannerReason::ClientProtocolRequiresRemux
        },
        egress: Some(source_egress(source, input.policy)),
        remux_profile: Some(profile),
        token_policy: TokenPolicy::HeaderBearer,
    })
}

fn source_egress(source: &SourceDescriptor, policy: &PlannerPolicy) -> EgressMode {
    if source.private_network {
        EgressMode::PrivateLan
    } else if policy.protected_egress_mode != EgressPolicyMode::Off {
        EgressMode::Protected
    } else {
        EgressMode::ServerDefault
    }
}

fn copy_compatible(source: &SourceDescriptor) -> bool {
    source.media.as_ref().is_none_or(|media| {
        media
            .video_codec
            .as_deref()
            .is_none_or(|codec| matches!(normalize_name(codec).as_str(), "h264" | "hevc" | "h265"))
            && media.audio_codec.as_deref().is_none_or(|codec| {
                matches!(
                    normalize_name(codec).as_str(),
                    "aac" | "ac3" | "eac3" | "mp3"
                )
            })
    })
}

fn normalize_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn valid_capability_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value == normalize_name(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_exact_path(value: &str) -> bool {
    value.starts_with('/')
        && value.len() <= 2_048
        && !value.contains(['*', '?', '#', ' '])
        && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use chrono::{Duration, TimeZone};

    use crate::live::contract::{
        ClientDisclosure, MediaHints, ProviderCookie, ResolvedSources, SensitiveString,
        ServerEgress, SourceDescriptor, StreamProtocol, TimeShift,
    };

    use super::*;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 12, 20, 0, 0).unwrap()
    }

    fn source(protocol: StreamProtocol, url: &str) -> SourceDescriptor {
        SourceDescriptor {
            stream_id: "stream-1".to_string(),
            label: "Main".to_string(),
            quality: Some("1080p".to_string()),
            language: Some("en".to_string()),
            priority: 100,
            protocol,
            url: SensitiveString::new(url),
            request_headers: BTreeMap::new(),
            cookies: Vec::new(),
            origin: None,
            referer: None,
            credential_authorities: Vec::new(),
            client_disclosure: ClientDisclosure::Public,
            expires_at: None,
            refresh_handle: None,
            server_egress: ServerEgress::NotRequired,
            private_network: false,
            time_shift: TimeShift {
                available: true,
                window_seconds: Some(1_800),
            },
            media: Some(MediaHints {
                container: Some("mpegts".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }),
        }
    }

    fn client() -> ClientCapabilities {
        ClientCapabilities {
            protocols: BTreeSet::from([
                StreamProtocol::Hls,
                StreamProtocol::Dash,
                StreamProtocol::HttpProgressive,
                StreamProtocol::MpegTs,
            ]),
            containers: BTreeSet::from(["mpegts".to_string(), "mp4".to_string()]),
            video_codecs: BTreeSet::from(["h264".to_string(), "hevc".to_string()]),
            audio_codecs: BTreeSet::from(["aac".to_string(), "ac3".to_string()]),
        }
    }

    fn rule() -> DirectDisclosureRule {
        DirectDisclosureRule::new(
            "https",
            "media.example.invalid",
            443,
            "/live/main.m3u8",
            true,
            true,
        )
        .unwrap()
    }

    fn policy() -> PlannerPolicy {
        PlannerPolicy {
            client_direct_enabled: true,
            relay_enabled: true,
            remux_enabled: true,
            relay_capacity_available: true,
            remux_capacity_available: true,
            protected_egress_mode: EgressPolicyMode::Off,
            protected_egress_ready: true,
            allow_private_lan_sources: false,
            provider_private_network_permission: false,
            native_dash_relay_enabled: false,
            rtmp_remux_enabled: false,
            srt_remux_enabled: false,
            disclosure_rules: vec![rule()],
        }
    }

    fn plan(
        source: SourceDescriptor,
        policy: &PlannerPolicy,
        client: &ClientCapabilities,
    ) -> Result<DeliveryPlan, PlannerRejection> {
        let sources = ResolvedSources {
            descriptor: source,
            alternatives: Vec::new(),
        };
        plan_delivery(&PlannerInput {
            sources: &sources,
            client,
            policy,
            requirements: PlaybackRequirements {
                require_time_shift: false,
                require_server_delivery: false,
            },
            now: now(),
        })
    }

    #[test]
    fn p11_direct_requires_every_public_disclosure_proof() {
        let client = client();
        let policy = policy();
        let direct = plan(
            source(
                StreamProtocol::Hls,
                "https://media.example.invalid/live/main.m3u8",
            ),
            &policy,
            &client,
        )
        .unwrap();
        assert_eq!(direct.mode, DeliveryMode::ClientDirect);
        assert_eq!(direct.reason, PlannerReason::PublicCompatibleDirect);
        assert_eq!(direct.token_policy, TokenPolicy::None);

        let cases = [
            (
                "https://media.example.invalid/live/main.m3u8?token=x",
                PlannerReason::SensitiveUrlRequireRelay,
            ),
            (
                "http://media.example.invalid/live/main.m3u8",
                PlannerReason::SensitiveUrlRequireRelay,
            ),
            (
                "https://other.example.invalid/live/main.m3u8",
                PlannerReason::DisclosureRuleMissingRequiresRelay,
            ),
            (
                "https://media.example.invalid/live/other.m3u8",
                PlannerReason::DisclosureRuleMissingRequiresRelay,
            ),
        ];
        for (url, reason) in cases {
            let planned = plan(source(StreamProtocol::Hls, url), &policy, &client).unwrap();
            assert_eq!(planned.mode, DeliveryMode::ServerRelay);
            assert_eq!(planned.reason, reason);
            assert_eq!(planned.token_policy, TokenPolicy::HeaderBearer);
        }
    }

    #[test]
    fn p11_sensitive_and_expiring_sources_never_disclose_to_client() {
        let client = client();
        let policy = policy();
        let mut cases = Vec::new();
        let mut headers = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        headers
            .request_headers
            .insert("Authorization".to_string(), SensitiveString::new("secret"));
        cases.push((headers, PlannerReason::SensitiveHeadersRequireRelay));
        let mut cookies = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        cookies.cookies.push(ProviderCookie {
            name: "session".to_string(),
            value: SensitiveString::new("secret"),
            domain: None,
            path: Some("/live".to_string()),
            secure: true,
            http_only: true,
            expires_at: None,
        });
        cases.push((cookies, PlannerReason::SensitiveCookiesRequireRelay));
        let mut origin = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        origin.origin = Some(SensitiveString::new("https://origin.example.invalid"));
        cases.push((origin, PlannerReason::SensitiveOriginRequireRelay));
        let mut referer = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        referer.referer = Some(SensitiveString::new("https://ref.example.invalid"));
        cases.push((referer, PlannerReason::SensitiveRefererRequireRelay));
        let mut refresh = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        refresh.refresh_handle = Some(SensitiveString::new("refresh-secret"));
        cases.push((refresh, PlannerReason::RefreshHandleRequiresRelay));
        let mut expiring = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        expiring.expires_at = Some(now() + Duration::minutes(10));
        cases.push((expiring, PlannerReason::RefreshHandleRequiresRelay));
        let mut server_only = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        server_only.client_disclosure = ClientDisclosure::ServerOnly;
        cases.push((server_only, PlannerReason::ProviderServerOnlyRequiresRelay));
        for (source, reason) in cases {
            let planned = plan(source, &policy, &client).unwrap();
            assert_eq!(planned.mode, DeliveryMode::ServerRelay);
            assert_eq!(planned.reason, reason);
        }

        let mut direct_disabled = policy.clone();
        direct_disabled.client_direct_enabled = false;
        assert_eq!(
            plan(
                source(
                    StreamProtocol::Hls,
                    "https://media.example.invalid/live/main.m3u8",
                ),
                &direct_disabled,
                &client,
            )
            .unwrap()
            .reason,
            PlannerReason::DirectDisabledRequiresRelay
        );
    }

    #[test]
    fn p11_private_sources_use_the_direct_lan_hop_under_protected_profiles() {
        let client = client();
        let mut policy = policy();
        let mut private = source(StreamProtocol::Hls, "https://10.0.0.10/live/main.m3u8");
        private.private_network = true;
        private.server_egress = ServerEgress::Required;
        assert_eq!(
            plan(private.clone(), &policy, &client).unwrap_err().code,
            PlannerRejectionCode::PrivateNetworkForbidden
        );
        policy.allow_private_lan_sources = true;
        policy.provider_private_network_permission = true;
        let private_plan = plan(private.clone(), &policy, &client).unwrap();
        assert_eq!(private_plan.mode, DeliveryMode::ServerRelay);
        assert_eq!(private_plan.egress, Some(EgressMode::PrivateLan));
        assert_eq!(
            private_plan.reason,
            PlannerReason::PrivateNetworkRequiresRelay
        );

        policy.protected_egress_mode = EgressPolicyMode::RequireProtected;
        policy.protected_egress_ready = false;
        let required_private = plan(private, &policy, &client).unwrap();
        assert_eq!(required_private.mode, DeliveryMode::ServerRelay);
        assert_eq!(required_private.egress, Some(EgressMode::PrivateLan));
        assert_eq!(
            required_private.reason,
            PlannerReason::PrivateNetworkRequiresRelay
        );

        let protected = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        assert_eq!(
            plan(protected.clone(), &policy, &client).unwrap_err().code,
            PlannerRejectionCode::ProtectedEgressUnavailable
        );
        policy.protected_egress_ready = true;
        let protected_plan = plan(protected, &policy, &client).unwrap();
        assert_eq!(protected_plan.egress, Some(EgressMode::Protected));
        assert_eq!(
            protected_plan.reason,
            PlannerReason::ProtectedEgressRequiresRelay
        );

        let preferred = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        policy.protected_egress_mode = EgressPolicyMode::PreferProtected;
        policy.protected_egress_ready = false;
        assert_eq!(
            plan(preferred.clone(), &policy, &client).unwrap_err().code,
            PlannerRejectionCode::ProtectedEgressUnavailable
        );
        policy.protected_egress_ready = true;
        let preferred_plan = plan(preferred, &policy, &client).unwrap();
        assert_eq!(preferred_plan.egress, Some(EgressMode::Protected));
        assert_eq!(
            preferred_plan.reason,
            PlannerReason::ProtectedEgressPreferredRelay
        );
        assert!(
            DirectDisclosureRule::new("https", "10.0.0.10", 443, "/live/main.m3u8", true, true,)
                .is_err()
        );
    }

    #[test]
    fn p11_provider_server_egress_hint_never_selects_protected_policy() {
        let client = client();
        let policy = policy();
        for (hint, reason) in [
            (
                ServerEgress::Required,
                PlannerReason::ProviderServerEgressRequiresRelay,
            ),
            (
                ServerEgress::Preferred,
                PlannerReason::ProviderServerEgressPreferredRelay,
            ),
        ] {
            let mut hinted = source(
                StreamProtocol::Hls,
                "https://media.example.invalid/live/main.m3u8",
            );
            hinted.server_egress = hint;
            let planned = plan(hinted, &policy, &client).unwrap();
            assert_eq!(planned.mode, DeliveryMode::ServerRelay);
            assert_eq!(planned.reason, reason);
            assert_eq!(planned.egress, Some(EgressMode::ServerDefault));
        }
    }

    #[test]
    fn p11_protocol_container_and_codec_matrix_never_transcodes() {
        let mut client = client();
        let policy = policy();
        client.protocols.remove(&StreamProtocol::Dash);
        let dash = plan(
            source(
                StreamProtocol::Dash,
                "https://media.example.invalid/live/manifest.mpd",
            ),
            &policy,
            &client,
        )
        .unwrap();
        assert_eq!(dash.mode, DeliveryMode::ServerRemux);
        assert_eq!(dash.remux_profile, Some(RemuxProfile::DashToHlsCopy));

        let mut unsupported_codec = source(
            StreamProtocol::MpegTs,
            "https://media.example.invalid/live/main.ts",
        );
        unsupported_codec.media.as_mut().unwrap().video_codec = Some("av1".to_string());
        assert_eq!(
            plan(unsupported_codec, &policy, &client).unwrap_err().code,
            PlannerRejectionCode::ClientCodecUnsupported
        );

        let mut unsupported_container = source(
            StreamProtocol::HttpProgressive,
            "https://media.example.invalid/live/main.bin",
        );
        unsupported_container.media.as_mut().unwrap().container = Some("unknown".to_string());
        assert_eq!(
            plan(unsupported_container, &policy, &client)
                .unwrap_err()
                .code,
            PlannerRejectionCode::ClientContainerUnsupported
        );
    }

    #[test]
    fn p11_recovery_replans_the_same_direct_source_through_the_server() {
        let client = client();
        let policy = policy();
        let direct = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        let sources = ResolvedSources {
            descriptor: direct,
            alternatives: Vec::new(),
        };
        let relay = plan_delivery(&PlannerInput {
            sources: &sources,
            client: &client,
            policy: &policy,
            requirements: PlaybackRequirements {
                require_time_shift: false,
                require_server_delivery: true,
            },
            now: now(),
        })
        .unwrap();
        assert_eq!(relay.mode, DeliveryMode::ServerRelay);
        assert_eq!(relay.reason, PlannerReason::DirectRecoveryRequiresServer);

        let mut dash_client = client.clone();
        dash_client.protocols.insert(StreamProtocol::Dash);
        let dash_sources = ResolvedSources {
            descriptor: source(
                StreamProtocol::Dash,
                "https://media.example.invalid/live/manifest.mpd",
            ),
            alternatives: Vec::new(),
        };
        let remux = plan_delivery(&PlannerInput {
            sources: &dash_sources,
            client: &dash_client,
            policy: &policy,
            requirements: PlaybackRequirements {
                require_time_shift: false,
                require_server_delivery: true,
            },
            now: now(),
        })
        .unwrap();
        assert_eq!(remux.mode, DeliveryMode::ServerRemux);
        assert_eq!(remux.remux_profile, Some(RemuxProfile::DashToHlsCopy));
    }

    #[test]
    fn p11_capacity_flags_and_optional_transport_certifications_are_enforced() {
        let client = client();
        let mut policy = policy();
        let mut relay_source = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8?secret=x",
        );
        policy.relay_capacity_available = false;
        assert_eq!(
            plan(relay_source.clone(), &policy, &client)
                .unwrap_err()
                .code,
            PlannerRejectionCode::RelayCapacity
        );
        policy.relay_capacity_available = true;
        policy.relay_enabled = false;
        policy.remux_enabled = false;
        assert_eq!(
            plan(relay_source.clone(), &policy, &client)
                .unwrap_err()
                .code,
            PlannerRejectionCode::RelayDisabled
        );

        policy.relay_enabled = true;
        policy.remux_enabled = true;
        relay_source.protocol = StreamProtocol::Rtmp;
        relay_source.url = SensitiveString::new("rtmp://media.example.invalid/live/main");
        assert_eq!(
            plan(relay_source.clone(), &policy, &client)
                .unwrap_err()
                .code,
            PlannerRejectionCode::ClientProtocolUnsupported
        );
        policy.rtmp_remux_enabled = true;
        let rtmp = plan(relay_source, &policy, &client).unwrap();
        assert_eq!(rtmp.remux_profile, Some(RemuxProfile::RtmpToHlsCopy));
    }

    #[test]
    fn p11_expiry_timeshift_and_fallback_selection_are_deterministic() {
        let client = client();
        let policy = policy();
        let mut expired = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        expired.expires_at = Some(now());
        let fallback = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        let sources = ResolvedSources {
            descriptor: expired,
            alternatives: vec![fallback.clone(), fallback],
        };
        let planned = plan_delivery(&PlannerInput {
            sources: &sources,
            client: &client,
            policy: &policy,
            requirements: PlaybackRequirements {
                require_time_shift: false,
                require_server_delivery: false,
            },
            now: now(),
        })
        .unwrap();
        assert_eq!(planned.selected_source_index, 1);
        assert_eq!(planned.fallback_candidates, vec![2]);
        assert_eq!(
            planned.rejected_candidates,
            vec![CandidateRejection {
                source_index: 0,
                code: PlannerRejectionCode::DescriptorExpired,
            }]
        );

        let mut no_timeshift = source(
            StreamProtocol::Hls,
            "https://media.example.invalid/live/main.m3u8",
        );
        no_timeshift.time_shift.available = false;
        let sources = ResolvedSources {
            descriptor: no_timeshift,
            alternatives: Vec::new(),
        };
        assert_eq!(
            plan_delivery(&PlannerInput {
                sources: &sources,
                client: &client,
                policy: &policy,
                requirements: PlaybackRequirements {
                    require_time_shift: true,
                    require_server_delivery: false,
                },
                now: now(),
            })
            .unwrap_err()
            .code,
            PlannerRejectionCode::TimeShiftUnavailable
        );
    }

    #[test]
    fn p11_reason_and_error_codes_are_stable_and_non_sensitive() {
        let reasons = [
            PlannerReason::PublicCompatibleDirect,
            PlannerReason::SensitiveHeadersRequireRelay,
            PlannerReason::ProtectedEgressRequiresRelay,
            PlannerReason::UnsupportedContainerRequiresRemux,
        ];
        for reason in reasons {
            let value = reason.as_str();
            assert!(!value.is_empty());
            assert!(
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            );
        }
        let errors = [
            PlannerRejectionCode::MalformedDescriptor,
            PlannerRejectionCode::DescriptorExpired,
            PlannerRejectionCode::RelayCapacity,
            PlannerRejectionCode::RemuxProfileUnsupported,
        ];
        for error in errors {
            let value = error.as_str();
            assert!(!value.contains("http"));
            assert!(!value.contains("secret"));
        }
    }

    #[test]
    fn p11_rejection_classes_are_deterministic() {
        let client = client();
        let policy = policy();

        assert_eq!(
            plan(source(StreamProtocol::Hls, "not a url"), &policy, &client)
                .unwrap_err()
                .code,
            PlannerRejectionCode::MalformedDescriptor
        );
        assert_eq!(
            plan(
                source(StreamProtocol::Hls, "rtmp://media.example.invalid/live"),
                &policy,
                &client,
            )
            .unwrap_err()
            .code,
            PlannerRejectionCode::ProtocolMismatch
        );

        let mut invalid_policy = policy.clone();
        invalid_policy.relay_enabled = false;
        assert_eq!(
            plan(
                source(
                    StreamProtocol::Hls,
                    "https://media.example.invalid/live/main.m3u8",
                ),
                &invalid_policy,
                &client,
            )
            .unwrap_err()
            .code,
            PlannerRejectionCode::InvalidInput
        );

        let mut dash_client = client.clone();
        dash_client.protocols.remove(&StreamProtocol::Dash);
        let dash_source = || {
            source(
                StreamProtocol::Dash,
                "https://media.example.invalid/live/manifest.mpd",
            )
        };
        let mut remux_disabled = policy.clone();
        remux_disabled.remux_enabled = false;
        assert_eq!(
            plan(dash_source(), &remux_disabled, &dash_client)
                .unwrap_err()
                .code,
            PlannerRejectionCode::RemuxDisabled
        );
        let mut remux_full = policy.clone();
        remux_full.remux_capacity_available = false;
        assert_eq!(
            plan(dash_source(), &remux_full, &dash_client)
                .unwrap_err()
                .code,
            PlannerRejectionCode::RemuxCapacity
        );
        let mut incompatible = dash_source();
        incompatible.media.as_mut().unwrap().video_codec = Some("vp9".to_string());
        dash_client.video_codecs.insert("vp9".to_string());
        assert_eq!(
            plan(incompatible, &policy, &dash_client).unwrap_err().code,
            PlannerRejectionCode::RemuxProfileUnsupported
        );
    }
}
