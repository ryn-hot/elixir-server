use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{Any, Row, Transaction};
use uuid::Uuid;

use crate::live::{
    admin::{ActorSnapshot, AdminAction, LiveAuditChain, LiveAuditError},
    session::SessionRecord,
};

use super::policy::{
    EffectiveEgressPolicy, EgressPolicyMode, EgressPolicySelectionError, EgressPolicySource,
    PolicyCandidate, PolicyScope, valid_policy_id, validate_effective_policy,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPolicyAssignment {
    pub id: Uuid,
    pub home_id: Uuid,
    pub scope: PolicyScope,
    pub mode: EgressPolicyMode,
    pub policy_id: Option<String>,
    pub allow_fallback: bool,
    pub revision: i64,
}

impl StoredPolicyAssignment {
    pub fn candidate(&self) -> PolicyCandidate {
        PolicyCandidate {
            mode: self.mode,
            policy_id: self.policy_id.clone(),
            allow_fallback: self.allow_fallback,
            revision: self.revision,
            source: match self.scope {
                PolicyScope::ServerDefault => EgressPolicySource::ServerAssignment,
                PolicyScope::Profile(_) => EgressPolicySource::ProfileAssignment,
                PolicyScope::Provider(_) => EgressPolicySource::ProviderAssignment,
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EgressPolicyRepositoryError {
    #[error("invalid Live egress policy assignment")]
    Invalid,
    #[error("Live egress policy assignment revision changed")]
    RevisionChanged,
    #[error("Live egress policy assignment scope is forbidden")]
    ScopeForbidden,
    #[error("Live egress policy persistence failed")]
    Database(#[from] sqlx::Error),
    #[error("Live egress policy audit failed")]
    Audit(#[from] LiveAuditError),
}

impl From<EgressPolicySelectionError> for EgressPolicyRepositoryError {
    fn from(_: EgressPolicySelectionError) -> Self {
        Self::Invalid
    }
}

#[derive(Clone)]
pub struct EgressPolicyRepository {
    pool: sqlx::AnyPool,
}

impl EgressPolicyRepository {
    pub fn new(pool: sqlx::AnyPool) -> Self {
        Self { pool }
    }

    pub async fn assignments_for(
        &self,
        home_id: Uuid,
        profile_id: Uuid,
        provider_id: Uuid,
    ) -> Result<Vec<StoredPolicyAssignment>, EgressPolicyRepositoryError> {
        let rows = sqlx::query(
            "SELECT id, home_id, scope_type, scope_key, profile_id, provider_id, mode, policy_id,
                    CAST(CASE WHEN allow_fallback THEN 1 ELSE 0 END AS BIGINT) AS allow_fallback,
                    revision
             FROM live_egress_policy_assignments
             WHERE home_id = $1 AND (
                 (scope_type = 'server_default' AND scope_key = 'server')
                 OR (scope_type = 'profile' AND scope_key = $2)
                 OR (scope_type = 'provider' AND scope_key = $3)
             )
             ORDER BY CASE scope_type
                 WHEN 'server_default' THEN 0 WHEN 'provider' THEN 1 ELSE 2 END",
        )
        .bind(home_id.to_string())
        .bind(profile_id.to_string())
        .bind(provider_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > 3 {
            return Err(EgressPolicyRepositoryError::Invalid);
        }
        rows.iter().map(decode_assignment).collect()
    }

    pub async fn assignments_for_home(
        &self,
        home_id: Uuid,
    ) -> Result<Vec<StoredPolicyAssignment>, EgressPolicyRepositoryError> {
        let rows = sqlx::query(
            "SELECT id, home_id, scope_type, scope_key, profile_id, provider_id, mode, policy_id,
                    CAST(CASE WHEN allow_fallback THEN 1 ELSE 0 END AS BIGINT) AS allow_fallback,
                    revision
             FROM live_egress_policy_assignments
             WHERE home_id = $1
             ORDER BY CASE scope_type
                 WHEN 'server_default' THEN 0 WHEN 'provider' THEN 1 ELSE 2 END,
                 scope_key, id",
        )
        .bind(home_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > 256 {
            return Err(EgressPolicyRepositoryError::Invalid);
        }
        rows.iter().map(decode_assignment).collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert(
        &self,
        home_id: Uuid,
        scope: PolicyScope,
        mode: EgressPolicyMode,
        policy_id: Option<&str>,
        allow_fallback: bool,
        expected_revision: i64,
        actor_user_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<StoredPolicyAssignment, EgressPolicyRepositoryError> {
        validate_assignment(mode, policy_id, allow_fallback, expected_revision)?;
        let mut transaction = self.pool.begin().await?;
        validate_scope_in(&mut transaction, home_id, scope).await?;
        let (_, assignment) = mutate_assignment_in(
            &mut transaction,
            home_id,
            scope,
            mode,
            policy_id,
            allow_fallback,
            expected_revision,
            actor_user_id,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(assignment)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_audited(
        &self,
        home_id: Uuid,
        scope: PolicyScope,
        mode: EgressPolicyMode,
        policy_id: Option<&str>,
        allow_fallback: bool,
        expected_revision: i64,
        actor: &ActorSnapshot,
        audit: &LiveAuditChain,
        now: DateTime<Utc>,
    ) -> Result<StoredPolicyAssignment, EgressPolicyRepositoryError> {
        validate_assignment(mode, policy_id, allow_fallback, expected_revision)?;
        let mut transaction = self.pool.begin().await?;
        validate_scope_in(&mut transaction, home_id, scope).await?;
        let (before, assignment) = mutate_assignment_in(
            &mut transaction,
            home_id,
            scope,
            mode,
            policy_id,
            allow_fallback,
            expected_revision,
            actor.actor_user_id,
            now,
        )
        .await?;
        let after = assignment_snapshot(&assignment);
        let before = before.as_ref().map(assignment_snapshot);
        audit
            .append(
                &mut transaction,
                home_id,
                AdminAction::EgressPolicySet,
                "live_egress_policy",
                &format!("{}:{}", scope.scope_type(), scope.scope_key()),
                actor,
                before.as_ref(),
                Some(&after),
                None,
                now,
            )
            .await?;
        transaction.commit().await?;
        Ok(assignment)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn mark_binding_failed(
        &self,
        binding_id: Uuid,
        session: &SessionRecord,
        policy: &EffectiveEgressPolicy,
        actor: &ActorSnapshot,
        audit: &LiveAuditChain,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<(), EgressPolicyRepositoryError> {
        validate_effective_policy(policy)?;
        if binding_id.is_nil()
            || session.control_fencing_token < 1
            || reason.is_empty()
            || reason.len() > 128
            || reason.chars().any(char::is_control)
        {
            return Err(EgressPolicyRepositoryError::Invalid);
        }
        let mut transaction = self.pool.begin().await?;
        let binding_update = sqlx::query(
            "UPDATE live_egress_bindings SET state = 'failed',
                 failure_reason_redacted = $1, released_at = $2
             WHERE id = $3 AND session_id = $4 AND state = 'provisioning'
               AND control_fencing_token = $5 AND policy_revision = $6",
        )
        .bind(reason)
        .bind(now.to_rfc3339())
        .bind(binding_id.to_string())
        .bind(session.id.to_string())
        .bind(session.control_fencing_token)
        .bind(policy.revision)
        .execute(&mut *transaction)
        .await?;
        if binding_update.rows_affected() != 1 {
            return Err(EgressPolicyRepositoryError::RevisionChanged);
        }
        let session_update = sqlx::query(
            "UPDATE live_playback_sessions SET egress_binding_id = NULL
             WHERE id = $1 AND egress_binding_id = $2 AND control_fencing_token = $3
               AND state NOT IN ('ended', 'expired', 'failed')",
        )
        .bind(session.id.to_string())
        .bind(binding_id.to_string())
        .bind(session.control_fencing_token)
        .execute(&mut *transaction)
        .await?;
        if session_update.rows_affected() != 1 {
            return Err(EgressPolicyRepositoryError::RevisionChanged);
        }
        if policy.mode == EgressPolicyMode::PreferProtected && policy.allow_fallback {
            let before = json!({
                "mode": "protected",
                "policyRevision": policy.revision,
                "state": "provisioning",
            });
            let after = json!({
                "fallbackReason": reason,
                "mode": "server_default",
                "policyRevision": policy.revision,
                "state": "direct_fallback",
            });
            audit
                .append(
                    &mut transaction,
                    session.owner.home_id,
                    AdminAction::EgressDirectFallback,
                    "live_playback_session",
                    &session.id.to_string(),
                    actor,
                    Some(&before),
                    Some(&after),
                    None,
                    now,
                )
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

async fn validate_scope_in(
    transaction: &mut Transaction<'_, Any>,
    home_id: Uuid,
    scope: PolicyScope,
) -> Result<(), EgressPolicyRepositoryError> {
    let valid = match scope {
        PolicyScope::ServerDefault => true,
        PolicyScope::Profile(profile_id) => {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM profiles WHERE id = $1 AND home_id = $2",
            )
            .bind(profile_id.to_string())
            .bind(home_id.to_string())
            .fetch_one(&mut **transaction)
            .await?
                == 1
        }
        PolicyScope::Provider(provider_id) => {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM live_provider_admin_state
                 WHERE provider_id = $1 AND home_id = $2",
            )
            .bind(provider_id.to_string())
            .bind(home_id.to_string())
            .fetch_one(&mut **transaction)
            .await?
                == 1
        }
    };
    if !valid {
        return Err(EgressPolicyRepositoryError::ScopeForbidden);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn mutate_assignment_in(
    transaction: &mut Transaction<'_, Any>,
    home_id: Uuid,
    scope: PolicyScope,
    mode: EgressPolicyMode,
    policy_id: Option<&str>,
    allow_fallback: bool,
    expected_revision: i64,
    actor_user_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(Option<StoredPolicyAssignment>, StoredPolicyAssignment), EgressPolicyRepositoryError> {
    let scope_type = scope.scope_type();
    let scope_key = scope.scope_key();
    let existing_row = sqlx::query(
        "SELECT id, home_id, scope_type, scope_key, profile_id, provider_id, mode, policy_id,
                CAST(CASE WHEN allow_fallback THEN 1 ELSE 0 END AS BIGINT) AS allow_fallback,
                revision
         FROM live_egress_policy_assignments
         WHERE home_id = $1 AND scope_type = $2 AND scope_key = $3",
    )
    .bind(home_id.to_string())
    .bind(scope_type)
    .bind(&scope_key)
    .fetch_optional(&mut **transaction)
    .await?;
    let before = existing_row.as_ref().map(decode_assignment).transpose()?;
    let id = if let Some(existing) = before.as_ref() {
        if existing.revision != expected_revision {
            return Err(EgressPolicyRepositoryError::RevisionChanged);
        }
        sqlx::query_scalar::<_, String>(
            "UPDATE live_egress_policy_assignments
             SET mode = $1, policy_id = $2, allow_fallback = $3,
                 revision = revision + 1, updated_by_user_id = $4, updated_at = $5
             WHERE home_id = $6 AND scope_type = $7 AND scope_key = $8 AND revision = $9
             RETURNING id",
        )
        .bind(mode.as_str())
        .bind(policy_id)
        .bind(allow_fallback)
        .bind(actor_user_id.to_string())
        .bind(now.to_rfc3339())
        .bind(home_id.to_string())
        .bind(scope_type)
        .bind(&scope_key)
        .bind(expected_revision)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(EgressPolicyRepositoryError::RevisionChanged)?
    } else {
        if expected_revision != 0 {
            return Err(EgressPolicyRepositoryError::RevisionChanged);
        }
        let id = Uuid::new_v4();
        let (profile_id, provider_id) = match scope {
            PolicyScope::ServerDefault => (None, None),
            PolicyScope::Profile(id) => (Some(id.to_string()), None),
            PolicyScope::Provider(id) => (None, Some(id.to_string())),
        };
        sqlx::query(
            "INSERT INTO live_egress_policy_assignments
             (id, home_id, scope_type, scope_key, profile_id, provider_id, mode, policy_id,
              allow_fallback, revision, updated_by_user_id, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $11, $11)",
        )
        .bind(id.to_string())
        .bind(home_id.to_string())
        .bind(scope_type)
        .bind(&scope_key)
        .bind(profile_id)
        .bind(provider_id)
        .bind(mode.as_str())
        .bind(policy_id)
        .bind(allow_fallback)
        .bind(actor_user_id.to_string())
        .bind(now.to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        id.to_string()
    };
    let row = sqlx::query(
        "SELECT id, home_id, scope_type, scope_key, profile_id, provider_id, mode, policy_id,
                CAST(CASE WHEN allow_fallback THEN 1 ELSE 0 END AS BIGINT) AS allow_fallback,
                revision
         FROM live_egress_policy_assignments WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok((before, decode_assignment(&row)?))
}

fn assignment_snapshot(assignment: &StoredPolicyAssignment) -> Value {
    json!({
        "id": assignment.id,
        "scopeType": assignment.scope.scope_type(),
        "scopeKey": assignment.scope.scope_key(),
        "mode": assignment.mode.as_str(),
        "policyId": assignment.policy_id,
        "allowFallback": assignment.allow_fallback,
        "revision": assignment.revision,
    })
}

fn validate_assignment(
    mode: EgressPolicyMode,
    policy_id: Option<&str>,
    allow_fallback: bool,
    expected_revision: i64,
) -> Result<(), EgressPolicyRepositoryError> {
    if expected_revision < 0 || policy_id.is_some_and(|value| !valid_policy_id(value)) {
        return Err(EgressPolicyRepositoryError::Invalid);
    }
    match mode {
        EgressPolicyMode::Off if policy_id.is_none() && !allow_fallback => Ok(()),
        EgressPolicyMode::PreferProtected if policy_id.is_some() => Ok(()),
        EgressPolicyMode::RequireProtected if policy_id.is_some() && !allow_fallback => Ok(()),
        _ => Err(EgressPolicyRepositoryError::Invalid),
    }
}

fn decode_assignment(
    row: &sqlx::any::AnyRow,
) -> Result<StoredPolicyAssignment, EgressPolicyRepositoryError> {
    let id = parse_uuid(row.try_get::<String, _>("id")?)?;
    let home_id = parse_uuid(row.try_get::<String, _>("home_id")?)?;
    let scope_type: String = row.try_get("scope_type")?;
    let scope_key: String = row.try_get("scope_key")?;
    let scope_id = || parse_uuid(scope_key.clone());
    let scope = match scope_type.as_str() {
        "server_default" if scope_key == "server" => PolicyScope::ServerDefault,
        "profile" => PolicyScope::Profile(scope_id()?),
        "provider" => PolicyScope::Provider(scope_id()?),
        _ => return Err(EgressPolicyRepositoryError::Invalid),
    };
    let mode = EgressPolicyMode::parse(&row.try_get::<String, _>("mode")?)?;
    let policy_id: Option<String> = row.try_get("policy_id")?;
    let allow_fallback = row.try_get::<i64, _>("allow_fallback")? == 1;
    let revision: i64 = row.try_get("revision")?;
    validate_assignment(mode, policy_id.as_deref(), allow_fallback, revision)?;
    Ok(StoredPolicyAssignment {
        id,
        home_id,
        scope,
        mode,
        policy_id,
        allow_fallback,
        revision,
    })
}

fn parse_uuid(value: String) -> Result<Uuid, EgressPolicyRepositoryError> {
    Uuid::parse_str(&value).map_err(|_| EgressPolicyRepositoryError::Invalid)
}
