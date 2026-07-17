use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressPolicyMode {
    Off,
    PreferProtected,
    RequireProtected,
}

impl EgressPolicyMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::PreferProtected => "prefer_protected",
            Self::RequireProtected => "require_protected",
        }
    }

    pub fn parse(value: &str) -> Result<Self, EgressPolicySelectionError> {
        match value {
            "off" => Ok(Self::Off),
            "prefer_protected" => Ok(Self::PreferProtected),
            "require_protected" => Ok(Self::RequireProtected),
            _ => Err(EgressPolicySelectionError::InvalidMode),
        }
    }

    const fn strength(self) -> u8 {
        match self {
            Self::Off => 0,
            Self::PreferProtected => 1,
            Self::RequireProtected => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressPolicySource {
    ServerConfig,
    ServerAssignment,
    ProviderAssignment,
    ProfileAssignment,
    Session,
}

impl EgressPolicySource {
    const fn precedence(self) -> u8 {
        match self {
            Self::ServerConfig => 0,
            Self::ServerAssignment => 1,
            Self::ProviderAssignment => 2,
            Self::ProfileAssignment => 3,
            Self::Session => 4,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ServerConfig => "server_config",
            Self::ServerAssignment => "server_assignment",
            Self::ProviderAssignment => "provider_assignment",
            Self::ProfileAssignment => "profile_assignment",
            Self::Session => "session",
        }
    }

    pub fn parse(value: &str) -> Result<Self, EgressPolicySelectionError> {
        match value {
            "server_config" => Ok(Self::ServerConfig),
            "server_assignment" => Ok(Self::ServerAssignment),
            "provider_assignment" => Ok(Self::ProviderAssignment),
            "profile_assignment" => Ok(Self::ProfileAssignment),
            "session" => Ok(Self::Session),
            _ => Err(EgressPolicySelectionError::InvalidSelection),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyCandidate {
    pub mode: EgressPolicyMode,
    pub policy_id: Option<String>,
    pub allow_fallback: bool,
    pub revision: i64,
    pub source: EgressPolicySource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveEgressPolicy {
    pub mode: EgressPolicyMode,
    pub policy_id: Option<String>,
    pub allow_fallback: bool,
    pub revision: i64,
    pub source: EgressPolicySource,
}

impl EffectiveEgressPolicy {
    pub fn protected(&self) -> bool {
        self.mode != EgressPolicyMode::Off
    }

    pub fn strict(&self) -> bool {
        self.mode == EgressPolicyMode::RequireProtected
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEgressPolicyRequest {
    pub mode: EgressPolicyMode,
    pub policy_id: Option<String>,
    pub allow_fallback: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyScope {
    ServerDefault,
    Profile(Uuid),
    Provider(Uuid),
}

impl PolicyScope {
    pub const fn scope_type(self) -> &'static str {
        match self {
            Self::ServerDefault => "server_default",
            Self::Profile(_) => "profile",
            Self::Provider(_) => "provider",
        }
    }

    pub fn scope_key(self) -> String {
        match self {
            Self::ServerDefault => "server".to_string(),
            Self::Profile(id) | Self::Provider(id) => id.to_string(),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum EgressPolicySelectionError {
    #[error("invalid Live egress mode")]
    InvalidMode,
    #[error("invalid Live egress policy selection")]
    InvalidSelection,
    #[error("protected Live egress has no selected policy")]
    PolicyRequired,
}

pub fn select_effective_policy(
    candidates: impl IntoIterator<Item = PolicyCandidate>,
) -> Result<EffectiveEgressPolicy, EgressPolicySelectionError> {
    let mut selected: Option<PolicyCandidate> = None;
    for candidate in candidates {
        validate_candidate(&candidate)?;
        let replace = selected.as_ref().is_none_or(|current| {
            candidate.mode.strength() > current.mode.strength()
                || (candidate.mode.strength() == current.mode.strength()
                    && candidate.source.precedence() > current.source.precedence())
        });
        if replace {
            selected = Some(candidate);
        }
    }
    let selected = selected.unwrap_or(PolicyCandidate {
        mode: EgressPolicyMode::Off,
        policy_id: None,
        allow_fallback: false,
        revision: 1,
        source: EgressPolicySource::ServerConfig,
    });
    Ok(EffectiveEgressPolicy {
        mode: selected.mode,
        policy_id: selected.policy_id,
        allow_fallback: selected.allow_fallback,
        revision: selected.revision,
        source: selected.source,
    })
}

pub(crate) fn validate_effective_policy(
    policy: &EffectiveEgressPolicy,
) -> Result<(), EgressPolicySelectionError> {
    validate_candidate(&PolicyCandidate {
        mode: policy.mode,
        policy_id: policy.policy_id.clone(),
        allow_fallback: policy.allow_fallback,
        revision: policy.revision,
        source: policy.source,
    })
}

fn validate_candidate(candidate: &PolicyCandidate) -> Result<(), EgressPolicySelectionError> {
    if candidate.revision < 1
        || candidate
            .policy_id
            .as_deref()
            .is_some_and(|value| !valid_policy_id(value))
    {
        return Err(EgressPolicySelectionError::InvalidSelection);
    }
    match candidate.mode {
        EgressPolicyMode::Off => {
            if candidate.policy_id.is_some() || candidate.allow_fallback {
                return Err(EgressPolicySelectionError::InvalidSelection);
            }
        }
        EgressPolicyMode::PreferProtected => {
            if candidate.policy_id.is_none() {
                return Err(EgressPolicySelectionError::PolicyRequired);
            }
        }
        EgressPolicyMode::RequireProtected => {
            if candidate.policy_id.is_none() || candidate.allow_fallback {
                return Err(EgressPolicySelectionError::InvalidSelection);
            }
        }
    }
    Ok(())
}

pub(crate) fn valid_policy_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        mode: EgressPolicyMode,
        id: Option<&str>,
        fallback: bool,
        source: EgressPolicySource,
    ) -> PolicyCandidate {
        PolicyCandidate {
            mode,
            policy_id: id.map(str::to_string),
            allow_fallback: fallback,
            revision: 1,
            source,
        }
    }

    #[test]
    fn n11_required_wins_and_equal_strength_uses_documented_precedence() {
        let selected = select_effective_policy([
            candidate(
                EgressPolicyMode::RequireProtected,
                Some("server-vpn"),
                false,
                EgressPolicySource::ServerAssignment,
            ),
            candidate(
                EgressPolicyMode::PreferProtected,
                Some("session-vpn"),
                true,
                EgressPolicySource::Session,
            ),
            candidate(
                EgressPolicyMode::RequireProtected,
                Some("profile-vpn"),
                false,
                EgressPolicySource::ProfileAssignment,
            ),
        ])
        .unwrap();
        assert_eq!(selected.policy_id.as_deref(), Some("profile-vpn"));
        assert_eq!(selected.source, EgressPolicySource::ProfileAssignment);
        assert!(selected.strict());
    }

    #[test]
    fn n11_fallback_is_valid_only_for_prefer_protected() {
        for invalid in [
            candidate(
                EgressPolicyMode::Off,
                None,
                true,
                EgressPolicySource::Session,
            ),
            candidate(
                EgressPolicyMode::RequireProtected,
                Some("vpn"),
                true,
                EgressPolicySource::Session,
            ),
        ] {
            assert_eq!(
                select_effective_policy([invalid]).unwrap_err(),
                EgressPolicySelectionError::InvalidSelection
            );
        }
    }
}
