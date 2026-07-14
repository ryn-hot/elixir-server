use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::live::{config::LiveRecoveryLimits, planner::ClientCapabilities};

const MAX_STORED_EVENTS: usize = 32;
const MAX_STORED_SOURCE_HEALTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAction {
    Refresh,
    Failover,
}

impl RecoveryAction {
    pub const fn state(self) -> &'static str {
        match self {
            Self::Refresh => "refreshing",
            Self::Failover => "failing_over",
        }
    }

    pub fn success_revision(self, expected_revision: i64) -> Option<i64> {
        expected_revision.checked_add(match self {
            Self::Refresh => 2,
            Self::Failover => 3,
        })
    }

    pub fn failure_revision(self, expected_revision: i64) -> Option<i64> {
        expected_revision.checked_add(2)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryReason {
    ExpiryThreshold,
    UpstreamUnauthorized,
    UpstreamForbidden,
    UpstreamGone,
    Transport,
    Stalled,
    ManualSourceSwitch,
}

impl RecoveryReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExpiryThreshold => "expiry_threshold",
            Self::UpstreamUnauthorized => "upstream_unauthorized",
            Self::UpstreamForbidden => "upstream_forbidden",
            Self::UpstreamGone => "upstream_gone",
            Self::Transport => "transport",
            Self::Stalled => "stalled",
            Self::ManualSourceSwitch => "manual_source_switch",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOutcome {
    Succeeded,
    Failed,
}

impl RecoveryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedEgressMode {
    #[default]
    Inherit,
    Off,
    PreferProtected,
    RequireProtected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryEvent {
    pub at: DateTime<Utc>,
    pub revision: i64,
    pub action: RecoveryAction,
    pub reason: RecoveryReason,
    pub outcome: RecoveryOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceHealth {
    source_id: String,
    failures: u8,
    cooldown_until: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoredRecoveryState {
    pub client: Option<ClientCapabilities>,
    pub requested_egress: RequestedEgressMode,
    initial_source_id: Option<String>,
    attempted_source_ids: Vec<String>,
    source_health: Vec<SourceHealth>,
    pub events: Vec<RecoveryEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPolicy {
    max_sources: usize,
    max_transitions: usize,
    window: Duration,
    source_cooldown: Duration,
}

impl From<&LiveRecoveryLimits> for RecoveryPolicy {
    fn from(limits: &LiveRecoveryLimits) -> Self {
        Self {
            max_sources: limits.max_sources as usize,
            max_transitions: limits.max_transitions as usize,
            window: Duration::seconds(limits.window_seconds as i64),
            source_cooldown: Duration::seconds(limits.source_cooldown_seconds as i64),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPolicyError {
    InvalidState,
    TransitionLimit,
    SourceLimit,
}

impl StoredRecoveryState {
    pub fn new(
        client: ClientCapabilities,
        requested_egress: RequestedEgressMode,
        initial_source_id: &str,
    ) -> Result<Self, RecoveryPolicyError> {
        if !valid_source_id(initial_source_id) {
            return Err(RecoveryPolicyError::InvalidState);
        }
        Ok(Self {
            client: Some(client),
            requested_egress,
            initial_source_id: Some(initial_source_id.to_string()),
            attempted_source_ids: vec![initial_source_id.to_string()],
            source_health: Vec::new(),
            events: Vec::new(),
        })
    }

    pub fn validate(&self) -> Result<(), RecoveryPolicyError> {
        if self.client.as_ref().is_none_or(|client| !client.validate())
            || self
                .initial_source_id
                .as_deref()
                .is_none_or(|source_id| !valid_source_id(source_id))
            || self.attempted_source_ids.len() > MAX_STORED_SOURCE_HEALTH
            || self.source_health.len() > MAX_STORED_SOURCE_HEALTH
            || self.events.len() > MAX_STORED_EVENTS
            || self
                .attempted_source_ids
                .iter()
                .any(|source_id| !valid_source_id(source_id))
            || self
                .source_health
                .iter()
                .any(|health| !valid_source_id(&health.source_id) || health.failures == 0)
            || self.events.iter().any(|event| event.revision < 1)
        {
            return Err(RecoveryPolicyError::InvalidState);
        }
        let attempted = self.attempted_source_ids.iter().collect::<BTreeSet<_>>();
        let health = self
            .source_health
            .iter()
            .map(|entry| &entry.source_id)
            .collect::<BTreeSet<_>>();
        if attempted.len() != self.attempted_source_ids.len()
            || health.len() != self.source_health.len()
        {
            return Err(RecoveryPolicyError::InvalidState);
        }
        Ok(())
    }

    pub fn admit_transition(
        &mut self,
        now: DateTime<Utc>,
        policy: RecoveryPolicy,
    ) -> Result<(), RecoveryPolicyError> {
        self.validate()?;
        let cutoff = now - policy.window;
        self.events.retain(|event| event.at >= cutoff);
        self.source_health
            .retain(|health| health.cooldown_until > now);
        if self.events.len() >= policy.max_transitions {
            return Err(RecoveryPolicyError::TransitionLimit);
        }
        Ok(())
    }

    pub fn may_attempt_source(
        &self,
        source_id: &str,
        now: DateTime<Utc>,
        policy: RecoveryPolicy,
    ) -> Result<bool, RecoveryPolicyError> {
        if !valid_source_id(source_id) {
            return Err(RecoveryPolicyError::InvalidState);
        }
        if self
            .source_health
            .iter()
            .any(|health| health.source_id == source_id && health.cooldown_until > now)
        {
            return Ok(false);
        }
        Ok(self
            .attempted_source_ids
            .iter()
            .any(|value| value == source_id)
            || self.attempted_source_ids.len() < policy.max_sources)
    }

    pub fn mark_source_failed(
        &mut self,
        source_id: &str,
        now: DateTime<Utc>,
        policy: RecoveryPolicy,
    ) -> Result<(), RecoveryPolicyError> {
        self.track_source(source_id, policy)?;
        let cooldown_until = now + policy.source_cooldown;
        if let Some(health) = self
            .source_health
            .iter_mut()
            .find(|health| health.source_id == source_id)
        {
            health.failures = health.failures.saturating_add(1);
            health.cooldown_until = cooldown_until;
        } else {
            self.source_health.push(SourceHealth {
                source_id: source_id.to_string(),
                failures: 1,
                cooldown_until,
            });
        }
        self.validate()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        action: RecoveryAction,
        reason: RecoveryReason,
        outcome: RecoveryOutcome,
        source_id: &str,
        revision: i64,
        now: DateTime<Utc>,
        policy: RecoveryPolicy,
    ) -> Result<(), RecoveryPolicyError> {
        if revision < 1 || !valid_source_id(source_id) {
            return Err(RecoveryPolicyError::InvalidState);
        }
        self.track_source(source_id, policy)?;
        match outcome {
            RecoveryOutcome::Succeeded => self
                .source_health
                .retain(|health| health.source_id != source_id),
            RecoveryOutcome::Failed => self.mark_source_failed(source_id, now, policy)?,
        }
        self.events.push(RecoveryEvent {
            at: now,
            revision,
            action,
            reason,
            outcome,
        });
        if self.events.len() > MAX_STORED_EVENTS {
            let overflow = self.events.len() - MAX_STORED_EVENTS;
            self.events.drain(..overflow);
        }
        self.validate()
    }

    fn track_source(
        &mut self,
        source_id: &str,
        policy: RecoveryPolicy,
    ) -> Result<(), RecoveryPolicyError> {
        if !valid_source_id(source_id) {
            return Err(RecoveryPolicyError::InvalidState);
        }
        if !self
            .attempted_source_ids
            .iter()
            .any(|value| value == source_id)
        {
            if self.attempted_source_ids.len() >= policy.max_sources {
                return Err(RecoveryPolicyError::SourceLimit);
            }
            self.attempted_source_ids.push(source_id.to_string());
        }
        Ok(())
    }
}

fn valid_source_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::live::contract::StreamProtocol;

    fn client() -> ClientCapabilities {
        ClientCapabilities {
            protocols: BTreeSet::from([StreamProtocol::Hls]),
            containers: BTreeSet::from(["mpegts".to_string()]),
            video_codecs: BTreeSet::from(["h264".to_string()]),
            audio_codecs: BTreeSet::from(["aac".to_string()]),
        }
    }

    fn policy() -> RecoveryPolicy {
        RecoveryPolicy::from(&LiveRecoveryLimits::default())
    }

    #[test]
    fn r20_recovery_policy_bounds_transitions_sources_and_cooldown() {
        let now = DateTime::parse_from_rfc3339("2026-07-13T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut state =
            StoredRecoveryState::new(client(), RequestedEgressMode::Inherit, "source-a").unwrap();
        for revision in 2..=7 {
            state.admit_transition(now, policy()).unwrap();
            state
                .record(
                    RecoveryAction::Refresh,
                    RecoveryReason::Transport,
                    RecoveryOutcome::Failed,
                    "source-a",
                    revision,
                    now,
                    policy(),
                )
                .unwrap();
        }
        assert_eq!(
            state.admit_transition(now, policy()),
            Err(RecoveryPolicyError::TransitionLimit)
        );
        assert!(!state.may_attempt_source("source-a", now, policy()).unwrap());
        assert!(
            state
                .may_attempt_source("source-a", now + Duration::seconds(31), policy())
                .unwrap()
        );

        state
            .record(
                RecoveryAction::Failover,
                RecoveryReason::Transport,
                RecoveryOutcome::Succeeded,
                "source-b",
                8,
                now + Duration::seconds(31),
                policy(),
            )
            .unwrap();
        state
            .record(
                RecoveryAction::Failover,
                RecoveryReason::Transport,
                RecoveryOutcome::Succeeded,
                "source-c",
                9,
                now + Duration::seconds(31),
                policy(),
            )
            .unwrap();
        assert!(
            !state
                .may_attempt_source("source-d", now + Duration::seconds(31), policy())
                .unwrap()
        );
    }

    #[test]
    fn r20_recovery_window_prunes_old_transitions_deterministically() {
        let now = DateTime::parse_from_rfc3339("2026-07-13T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut state =
            StoredRecoveryState::new(client(), RequestedEgressMode::Inherit, "source-a").unwrap();
        state
            .record(
                RecoveryAction::Refresh,
                RecoveryReason::ExpiryThreshold,
                RecoveryOutcome::Succeeded,
                "source-a",
                2,
                now,
                policy(),
            )
            .unwrap();
        state
            .admit_transition(now + Duration::seconds(601), policy())
            .unwrap();
        assert!(state.events.is_empty());
    }
}
