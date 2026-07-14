use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::live::crypto::{LiveDeliveryToken, SecretBytes};

use super::recovery::RecoveryAction;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    ClientDirect,
    ServerRelay,
    ServerRemux,
}

impl DeliveryMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientDirect => "client_direct",
            Self::ServerRelay => "server_relay",
            Self::ServerRemux => "server_remux",
        }
    }
}

impl TryFrom<&str> for DeliveryMode {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "client_direct" => Ok(Self::ClientDirect),
            "server_relay" => Ok(Self::ServerRelay),
            "server_remux" => Ok(Self::ServerRemux),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionProtocol {
    Hls,
    Dash,
    HttpProgressive,
    MpegTs,
    Rtmp,
    Srt,
}

impl SessionProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hls => "hls",
            Self::Dash => "dash",
            Self::HttpProgressive => "http_progressive",
            Self::MpegTs => "mpeg_ts",
            Self::Rtmp => "rtmp",
            Self::Srt => "srt",
        }
    }
}

impl TryFrom<&str> for SessionProtocol {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "hls" => Ok(Self::Hls),
            "dash" => Ok(Self::Dash),
            "http_progressive" => Ok(Self::HttpProgressive),
            "mpeg_ts" => Ok(Self::MpegTs),
            "rtmp" => Ok(Self::Rtmp),
            "srt" => Ok(Self::Srt),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Resolving,
    Planning,
    ProvisioningEgress,
    StartingRemux,
    Ready,
    Playing,
    Reconnecting,
    Refreshing,
    FailingOver,
    Ended,
    Expired,
    Failed,
}

impl SessionState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resolving => "resolving",
            Self::Planning => "planning",
            Self::ProvisioningEgress => "provisioning_egress",
            Self::StartingRemux => "starting_remux",
            Self::Ready => "ready",
            Self::Playing => "playing",
            Self::Reconnecting => "reconnecting",
            Self::Refreshing => "refreshing",
            Self::FailingOver => "failing_over",
            Self::Ended => "ended",
            Self::Expired => "expired",
            Self::Failed => "failed",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Ended | Self::Expired | Self::Failed)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        if self.is_terminal() || self == next {
            return false;
        }
        if next.is_terminal() {
            return true;
        }
        matches!(
            (self, next),
            (Self::Resolving, Self::Planning)
                | (Self::Planning, Self::ProvisioningEgress)
                | (Self::Planning, Self::StartingRemux)
                | (Self::Planning, Self::Ready)
                | (Self::ProvisioningEgress, Self::StartingRemux)
                | (Self::ProvisioningEgress, Self::Ready)
                | (Self::StartingRemux, Self::Ready)
                | (Self::Ready, Self::Playing)
                | (Self::Ready, Self::Reconnecting)
                | (Self::Ready, Self::Refreshing)
                | (Self::Ready, Self::FailingOver)
                | (Self::Playing, Self::Reconnecting)
                | (Self::Playing, Self::Refreshing)
                | (Self::Playing, Self::FailingOver)
                | (Self::Reconnecting, Self::Playing)
                | (Self::Reconnecting, Self::Ready)
                | (Self::Reconnecting, Self::Refreshing)
                | (Self::Reconnecting, Self::FailingOver)
                | (Self::Refreshing, Self::Playing)
                | (Self::Refreshing, Self::Ready)
                | (Self::Refreshing, Self::FailingOver)
                | (Self::FailingOver, Self::Planning)
                | (Self::FailingOver, Self::ProvisioningEgress)
                | (Self::FailingOver, Self::StartingRemux)
                | (Self::FailingOver, Self::Ready)
                | (Self::FailingOver, Self::Playing)
                | (Self::FailingOver, Self::Refreshing)
        )
    }
}

impl TryFrom<&str> for SessionState {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "resolving" => Ok(Self::Resolving),
            "planning" => Ok(Self::Planning),
            "provisioning_egress" => Ok(Self::ProvisioningEgress),
            "starting_remux" => Ok(Self::StartingRemux),
            "ready" => Ok(Self::Ready),
            "playing" => Ok(Self::Playing),
            "reconnecting" => Ok(Self::Reconnecting),
            "refreshing" => Ok(Self::Refreshing),
            "failing_over" => Ok(Self::FailingOver),
            "ended" => Ok(Self::Ended),
            "expired" => Ok(Self::Expired),
            "failed" => Ok(Self::Failed),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionOwner {
    pub user_id: Uuid,
    pub home_id: Uuid,
    pub profile_id: Uuid,
    pub account_session_id: Uuid,
    pub provider_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTrackSelection {
    pub track_id: String,
    pub language: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveTrackPreferenceUpdate {
    pub audio: Option<LiveTrackSelection>,
    pub subtitle: Option<LiveTrackSelection>,
}

impl LiveTrackPreferenceUpdate {
    pub fn is_empty(&self) -> bool {
        self.audio.is_none() && self.subtitle.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTrackPreferences {
    pub audio: Option<LiveTrackSelection>,
    pub subtitle: Option<LiveTrackSelection>,
    pub revision: i64,
    pub updated_at: DateTime<Utc>,
}

pub struct NewSession {
    pub owner: SessionOwner,
    pub item_key: SecretBytes,
    pub stream_option_key: SecretBytes,
    pub item_snapshot: SecretBytes,
    pub descriptor: SecretBytes,
    pub delivery_mode: DeliveryMode,
    pub protocol: SessionProtocol,
    pub source_index: i32,
    pub control_fencing_token: i64,
    pub now: DateTime<Utc>,
}

impl fmt::Debug for NewSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewSession")
            .field("owner", &self.owner)
            .field("item_key", &"[REDACTED]")
            .field("stream_option_key", &"[REDACTED]")
            .field("item_snapshot", &"[REDACTED]")
            .field("descriptor", &"[REDACTED]")
            .field("delivery_mode", &self.delivery_mode)
            .field("protocol", &self.protocol)
            .field("source_index", &self.source_index)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("now", &self.now)
            .finish()
    }
}

pub struct IdempotencyRequest {
    pub key: SecretBytes,
    pub request_identity: SecretBytes,
}

impl fmt::Debug for IdempotencyRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyRequest")
            .field("key", &"[REDACTED]")
            .field("request_identity", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub id: Uuid,
    pub owner: SessionOwner,
    pub delivery_mode: DeliveryMode,
    pub protocol: SessionProtocol,
    pub state: SessionState,
    pub revision: i64,
    pub token_revision: i64,
    pub control_fencing_token: i64,
    pub source_index: i32,
    pub failover_count: i32,
    pub refresh_count: i32,
    pub egress_binding_id: Option<Uuid>,
    pub remux_job_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub hard_expires_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub error_code: Option<String>,
    pub error_detail_redacted: Option<String>,
}

impl SessionRecord {
    pub fn needs_rollover(&self, now: DateTime<Utc>, lead_seconds: i64) -> bool {
        !self.state.is_terminal()
            && lead_seconds >= 0
            && now >= self.hard_expires_at - chrono::Duration::seconds(lead_seconds)
    }
}

pub struct SessionSecretMaterial {
    pub item_snapshot: SecretBytes,
    pub descriptor: SecretBytes,
}

impl fmt::Debug for SessionSecretMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionSecretMaterial([REDACTED])")
    }
}

pub struct SessionGrant {
    pub session: SessionRecord,
    pub token: LiveDeliveryToken,
    pub replayed: bool,
}

impl fmt::Debug for SessionGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionGrant")
            .field("session", &self.session)
            .field("token", &"[REDACTED]")
            .field("replayed", &self.replayed)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMutation {
    pub session: SessionRecord,
    pub previous_revision: i64,
}

pub struct SessionRecoveryReplacement {
    pub owner: SessionOwner,
    pub session_id: Uuid,
    pub expected_revision: i64,
    pub control_fencing_token: i64,
    pub descriptor: SecretBytes,
    pub delivery_mode: DeliveryMode,
    pub protocol: SessionProtocol,
    pub source_index: i32,
    pub action: RecoveryAction,
    pub now: DateTime<Utc>,
}

impl fmt::Debug for SessionRecoveryReplacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionRecoveryReplacement")
            .field("owner", &self.owner)
            .field("session_id", &self.session_id)
            .field("expected_revision", &self.expected_revision)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("descriptor", &"[REDACTED]")
            .field("delivery_mode", &self.delivery_mode)
            .field("protocol", &self.protocol)
            .field("source_index", &self.source_index)
            .field("action", &self.action)
            .field("now", &self.now)
            .finish()
    }
}

pub struct SessionRecoveryFailure {
    pub owner: SessionOwner,
    pub session_id: Uuid,
    pub expected_revision: i64,
    pub control_fencing_token: i64,
    pub descriptor: SecretBytes,
    pub action: RecoveryAction,
    pub now: DateTime<Utc>,
}

impl fmt::Debug for SessionRecoveryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionRecoveryFailure")
            .field("owner", &self.owner)
            .field("session_id", &self.session_id)
            .field("expected_revision", &self.expected_revision)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("descriptor", &"[REDACTED]")
            .field("action", &self.action)
            .field("now", &self.now)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalReason {
    pub state: SessionState,
    pub error_code: Option<String>,
    pub error_detail_redacted: Option<String>,
}

impl TerminalReason {
    pub fn ended() -> Self {
        Self {
            state: SessionState::Ended,
            error_code: None,
            error_detail_redacted: None,
        }
    }
}
