use std::{collections::BTreeMap, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::live::{
    contract::{
        ClientDisclosure, CredentialAuthority, MediaHints, ProviderCookie, ResolvedSources,
        SensitiveString, ServerEgress, SourceDescriptor, StreamProtocol, TimeShift,
    },
    egress::{EffectiveEgressPolicy, EgressPolicyMode, EgressPolicySource},
    planner::DeliveryPlan,
};

use super::recovery::StoredRecoveryState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredDescriptorError {
    SelectedSourceMissing,
    InvalidProtocol,
    InvalidClientDisclosure,
    InvalidServerEgress,
    InvalidEgressPolicy,
    DuplicateHeader,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredSessionDescriptor {
    pub provider_revision: String,
    pub decision_reason: String,
    pub playback_url: Option<String>,
    pub selected_source_index: usize,
    pub sources: Vec<StoredSource>,
    #[serde(default)]
    pub egress: StoredEgressPolicy,
    #[serde(default)]
    pub recovery: StoredRecoveryState,
}

impl fmt::Debug for StoredSessionDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredSessionDescriptor")
            .field("provider_revision", &self.provider_revision)
            .field("decision_reason", &self.decision_reason)
            .field("playback_url", &"[REDACTED]")
            .field("selected_source_index", &self.selected_source_index)
            .field("source_count", &self.sources.len())
            .field("egress", &self.egress)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoredEgressPolicy {
    pub mode: String,
    pub policy_id: Option<String>,
    pub allow_fallback: bool,
    pub revision: i64,
    pub source: String,
}

impl Default for StoredEgressPolicy {
    fn default() -> Self {
        Self {
            mode: EgressPolicyMode::Off.as_str().to_string(),
            policy_id: None,
            allow_fallback: false,
            revision: 1,
            source: EgressPolicySource::ServerConfig.as_str().to_string(),
        }
    }
}

impl StoredEgressPolicy {
    pub fn from_effective(policy: &EffectiveEgressPolicy) -> Self {
        Self {
            mode: policy.mode.as_str().to_string(),
            policy_id: policy.policy_id.clone(),
            allow_fallback: policy.allow_fallback,
            revision: policy.revision,
            source: policy.source.as_str().to_string(),
        }
    }

    pub fn to_effective(&self) -> Result<EffectiveEgressPolicy, StoredDescriptorError> {
        let mode = EgressPolicyMode::parse(&self.mode)
            .map_err(|_| StoredDescriptorError::InvalidEgressPolicy)?;
        let source = EgressPolicySource::parse(&self.source)
            .map_err(|_| StoredDescriptorError::InvalidEgressPolicy)?;
        let policy = EffectiveEgressPolicy {
            mode,
            policy_id: self.policy_id.clone(),
            allow_fallback: self.allow_fallback,
            revision: self.revision,
            source,
        };
        super::super::egress::validate_effective_policy(&policy)
            .map_err(|_| StoredDescriptorError::InvalidEgressPolicy)?;
        Ok(policy)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredSource {
    pub stream_id: String,
    pub label: String,
    pub quality: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub priority: i32,
    pub protocol: String,
    pub url: String,
    pub request_headers: Vec<(String, String)>,
    pub cookies: Vec<StoredCookie>,
    pub origin: Option<String>,
    pub referer: Option<String>,
    pub credential_authorities: Vec<StoredAuthority>,
    pub client_disclosure: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub refresh_handle: Option<String>,
    pub server_egress: String,
    pub private_network: bool,
    pub time_shift_available: bool,
    pub time_shift_window_seconds: Option<u32>,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
}

impl fmt::Debug for StoredSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredSource")
            .field("stream_id", &self.stream_id)
            .field("label", &self.label)
            .field("quality", &self.quality)
            .field("protocol", &self.protocol)
            .field("url", &"[REDACTED]")
            .field("request_header_count", &self.request_headers.len())
            .field("cookie_count", &self.cookies.len())
            .field(
                "credential_authority_count",
                &self.credential_authorities.len(),
            )
            .field("client_disclosure", &self.client_disclosure)
            .field("expires_at", &self.expires_at)
            .field("has_refresh_handle", &self.refresh_handle.is_some())
            .field("server_egress", &self.server_egress)
            .field("private_network", &self.private_network)
            .field("time_shift_available", &self.time_shift_available)
            .field("time_shift_window_seconds", &self.time_shift_window_seconds)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredCookie {
    pub name: String,
    pub value: String,
    pub domain: Option<String>,
    pub path: Option<String>,
    pub secure: bool,
    pub http_only: bool,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredAuthority {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub send_request_headers: bool,
    pub send_cookies: bool,
    pub send_origin: bool,
    pub send_referer: bool,
}

impl StoredSessionDescriptor {
    pub fn from_resolved(
        resolved: &ResolvedSources,
        provider_revision: &str,
        plan: &DeliveryPlan,
        playback_url: Option<String>,
    ) -> Result<Self, StoredDescriptorError> {
        let sources = std::iter::once(&resolved.descriptor)
            .chain(resolved.alternatives.iter())
            .map(StoredSource::from_source)
            .collect::<Vec<_>>();
        if sources.get(plan.selected_source_index).is_none() {
            return Err(StoredDescriptorError::SelectedSourceMissing);
        }
        Ok(Self {
            provider_revision: provider_revision.to_string(),
            decision_reason: plan.reason.as_str().to_string(),
            playback_url,
            selected_source_index: plan.selected_source_index,
            sources,
            egress: StoredEgressPolicy::default(),
            recovery: StoredRecoveryState::default(),
        })
    }

    pub fn selected(&self) -> Option<&StoredSource> {
        self.sources.get(self.selected_source_index)
    }
}

impl StoredSource {
    fn from_source(source: &SourceDescriptor) -> Self {
        Self {
            stream_id: source.stream_id.clone(),
            label: source.label.clone(),
            quality: source.quality.clone(),
            language: source.language.clone(),
            priority: source.priority,
            protocol: protocol_name(source.protocol).to_string(),
            url: source.url.expose().to_string(),
            request_headers: source
                .request_headers
                .iter()
                .map(|(name, value)| (name.clone(), value.expose().to_string()))
                .collect(),
            cookies: source
                .cookies
                .iter()
                .map(|cookie| StoredCookie {
                    name: cookie.name.clone(),
                    value: cookie.value.expose().to_string(),
                    domain: cookie.domain.clone(),
                    path: cookie.path.clone(),
                    secure: cookie.secure,
                    http_only: cookie.http_only,
                    expires_at: cookie.expires_at,
                })
                .collect(),
            origin: source
                .origin
                .as_ref()
                .map(|value| value.expose().to_string()),
            referer: source
                .referer
                .as_ref()
                .map(|value| value.expose().to_string()),
            credential_authorities: source
                .credential_authorities
                .iter()
                .map(|authority| StoredAuthority {
                    scheme: authority.scheme.clone(),
                    host: authority.host.clone(),
                    port: authority.port,
                    send_request_headers: authority.send_request_headers,
                    send_cookies: authority.send_cookies,
                    send_origin: authority.send_origin,
                    send_referer: authority.send_referer,
                })
                .collect(),
            client_disclosure: match source.client_disclosure {
                ClientDisclosure::ServerOnly => "server_only",
                ClientDisclosure::Public => "public",
            }
            .to_string(),
            expires_at: source.expires_at,
            refresh_handle: source
                .refresh_handle
                .as_ref()
                .map(|value| value.expose().to_string()),
            server_egress: match source.server_egress {
                ServerEgress::NotRequired => "not_required",
                ServerEgress::Preferred => "preferred",
                ServerEgress::Required => "required",
            }
            .to_string(),
            private_network: source.private_network,
            time_shift_available: source.time_shift.available,
            time_shift_window_seconds: source.time_shift.window_seconds,
            container: source
                .media
                .as_ref()
                .and_then(|media| media.container.clone()),
            video_codec: source
                .media
                .as_ref()
                .and_then(|media| media.video_codec.clone()),
            audio_codec: source
                .media
                .as_ref()
                .and_then(|media| media.audio_codec.clone()),
        }
    }

    pub fn to_source_descriptor(&self) -> Result<SourceDescriptor, StoredDescriptorError> {
        let protocol = match self.protocol.as_str() {
            "hls" => StreamProtocol::Hls,
            "dash" => StreamProtocol::Dash,
            "http_progressive" => StreamProtocol::HttpProgressive,
            "mpeg_ts" => StreamProtocol::MpegTs,
            "rtmp" => StreamProtocol::Rtmp,
            "srt" => StreamProtocol::Srt,
            _ => return Err(StoredDescriptorError::InvalidProtocol),
        };
        let client_disclosure = match self.client_disclosure.as_str() {
            "server_only" => ClientDisclosure::ServerOnly,
            "public" => ClientDisclosure::Public,
            _ => return Err(StoredDescriptorError::InvalidClientDisclosure),
        };
        let server_egress = match self.server_egress.as_str() {
            "not_required" => ServerEgress::NotRequired,
            "preferred" => ServerEgress::Preferred,
            "required" => ServerEgress::Required,
            _ => return Err(StoredDescriptorError::InvalidServerEgress),
        };
        let mut request_headers = BTreeMap::new();
        for (name, value) in &self.request_headers {
            if request_headers
                .insert(name.clone(), SensitiveString::new(value.clone()))
                .is_some()
            {
                return Err(StoredDescriptorError::DuplicateHeader);
            }
        }
        Ok(SourceDescriptor {
            stream_id: self.stream_id.clone(),
            label: self.label.clone(),
            quality: self.quality.clone(),
            language: self.language.clone(),
            priority: self.priority,
            protocol,
            url: SensitiveString::new(self.url.clone()),
            request_headers,
            cookies: self
                .cookies
                .iter()
                .map(|cookie| ProviderCookie {
                    name: cookie.name.clone(),
                    value: SensitiveString::new(cookie.value.clone()),
                    domain: cookie.domain.clone(),
                    path: cookie.path.clone(),
                    secure: cookie.secure,
                    http_only: cookie.http_only,
                    expires_at: cookie.expires_at,
                })
                .collect(),
            origin: self.origin.clone().map(SensitiveString::new),
            referer: self.referer.clone().map(SensitiveString::new),
            credential_authorities: self
                .credential_authorities
                .iter()
                .map(|authority| CredentialAuthority {
                    scheme: authority.scheme.clone(),
                    host: authority.host.clone(),
                    port: authority.port,
                    send_request_headers: authority.send_request_headers,
                    send_cookies: authority.send_cookies,
                    send_origin: authority.send_origin,
                    send_referer: authority.send_referer,
                })
                .collect(),
            client_disclosure,
            expires_at: self.expires_at,
            refresh_handle: self.refresh_handle.clone().map(SensitiveString::new),
            server_egress,
            private_network: self.private_network,
            time_shift: TimeShift {
                available: self.time_shift_available,
                window_seconds: self.time_shift_window_seconds,
            },
            media: (self.container.is_some()
                || self.video_codec.is_some()
                || self.audio_codec.is_some())
            .then(|| MediaHints {
                container: self.container.clone(),
                video_codec: self.video_codec.clone(),
                audio_codec: self.audio_codec.clone(),
            }),
        })
    }
}

const fn protocol_name(protocol: StreamProtocol) -> &'static str {
    match protocol {
        StreamProtocol::Hls => "hls",
        StreamProtocol::Dash => "dash",
        StreamProtocol::HttpProgressive => "http_progressive",
        StreamProtocol::MpegTs => "mpeg_ts",
        StreamProtocol::Rtmp => "rtmp",
        StreamProtocol::Srt => "srt",
    }
}
