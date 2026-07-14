use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Serialize;
use sqlx::{AnyPool, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    auth::{
        home_profiles::{HomeRole, ProfileType},
        revocation::{
            AuthorizationRevocationNotifier, NewAuthorizationRevocation, RevocationError,
            append_authorization_revocation_in_transaction,
        },
    },
    authz::{AuthorizationError, bump_profile_authorization_revision_in_transaction},
    live::admin::{ActorSnapshot, AdminAction, AuditReference, LiveAuditChain, LiveAuditError},
};

const MAX_ACTOR_SNAPSHOT_BYTES: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveProviderAccess {
    Browse,
    Play,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveProviderGrant {
    pub id: Uuid,
    pub profile_id: Uuid,
    pub provider_id: Uuid,
    pub can_browse: bool,
    pub can_play: bool,
    pub created_by_user_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityDecision {
    pub allowed: bool,
    pub authorization_revision: i64,
    pub grant: Option<LiveProviderGrant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantMutation {
    pub grant: Option<LiveProviderGrant>,
    pub authorization_revision: i64,
    pub revocation_event_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditedGrantMutation {
    pub provider_id: Uuid,
    pub profile_id: Uuid,
    pub can_browse: bool,
    pub can_play: bool,
    pub revision: i64,
    pub audit: AuditReference,
    #[serde(skip)]
    pub revocation_event_id: Option<Uuid>,
}

struct GrantWrite {
    mutation: GrantMutation,
    audit: Option<AuditReference>,
    admin_revision: Option<i64>,
}

#[derive(Debug, Error)]
pub enum LiveProviderGrantError {
    #[error("invalid Live provider grant input")]
    InvalidInput,
    #[error("Live provider grant target is unavailable")]
    TargetUnavailable,
    #[error("only an active home owner can change Live provider grants")]
    Forbidden,
    #[error("Live provider grant authorization revision changed")]
    RevisionChanged,
    #[error("invalid persisted Live provider grant state")]
    InvalidState,
    #[error("Live provider grant database operation failed")]
    Storage(#[from] sqlx::Error),
    #[error("Live provider grant authorization update failed")]
    Authorization(#[from] AuthorizationError),
    #[error("Live provider grant revocation production failed")]
    Revocation(#[from] RevocationError),
    #[error("Live provider grant audit failed")]
    Audit(#[from] LiveAuditError),
    #[error("Live provider grant serialization failed")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct LiveProviderGrantRepository {
    pool: AnyPool,
}

impl LiveProviderGrantRepository {
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }

    pub async fn visibility(
        &self,
        home_id: Uuid,
        profile_id: Uuid,
        role: HomeRole,
        profile_type: ProfileType,
        provider_id: Uuid,
        access: LiveProviderAccess,
    ) -> Result<VisibilityDecision, LiveProviderGrantError> {
        let row = sqlx::query(
            "SELECT revisions.revision,
                    grants.id, grants.profile_id, grants.provider_id,
                    CAST(CASE WHEN grants.can_browse THEN 1 ELSE 0 END AS BIGINT) AS can_browse,
                    CAST(CASE WHEN grants.can_play THEN 1 ELSE 0 END AS BIGINT) AS can_play,
                    grants.created_by_user_id,
                    CAST(grants.created_at AS TEXT) AS created_at,
                    CAST(grants.updated_at AS TEXT) AS updated_at
             FROM profile_authorization_revisions AS revisions
             LEFT JOIN live_provider_grants AS grants
               ON grants.profile_id = revisions.profile_id
              AND grants.provider_id = $2
             WHERE revisions.profile_id = $1
               AND revisions.home_id = $3
             LIMIT 1",
        )
        .bind(profile_id.to_string())
        .bind(provider_id.to_string())
        .bind(home_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LiveProviderGrantError::TargetUnavailable)?;
        let revision: i64 = row.try_get("revision")?;
        if revision < 1 {
            return Err(LiveProviderGrantError::InvalidState);
        }
        let grant = decode_optional_grant(&row)?;
        let unrestricted = profile_type != ProfileType::Managed
            && matches!(role, HomeRole::Owner | HomeRole::Admin);
        let allowed = unrestricted
            || grant.as_ref().is_some_and(|grant| match access {
                LiveProviderAccess::Browse => grant.can_browse,
                LiveProviderAccess::Play => grant.can_play,
            });
        Ok(VisibilityDecision {
            allowed,
            authorization_revision: revision,
            grant,
        })
    }

    pub async fn list_for_profile(
        &self,
        profile_id: Uuid,
    ) -> Result<Vec<LiveProviderGrant>, LiveProviderGrantError> {
        let rows = sqlx::query(
            "SELECT id, profile_id, provider_id,
                    CAST(CASE WHEN can_browse THEN 1 ELSE 0 END AS BIGINT) AS can_browse,
                    CAST(CASE WHEN can_play THEN 1 ELSE 0 END AS BIGINT) AS can_play,
                    created_by_user_id,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
             FROM live_provider_grants
             WHERE profile_id = $1
             ORDER BY provider_id",
        )
        .bind(profile_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_grant).collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_grant(
        &self,
        actor_user_id: Uuid,
        actor_snapshot: &str,
        profile_id: Uuid,
        provider_id: Uuid,
        can_browse: bool,
        can_play: bool,
        expected_authorization_revision: Option<i64>,
        notifier: Option<&AuthorizationRevocationNotifier>,
    ) -> Result<GrantMutation, LiveProviderGrantError> {
        for attempt in 0..8u64 {
            match self
                .set_grant_once(
                    actor_user_id,
                    actor_snapshot,
                    profile_id,
                    provider_id,
                    can_browse,
                    can_play,
                    expected_authorization_revision,
                    notifier,
                    None,
                )
                .await
            {
                Err(LiveProviderGrantError::Storage(error))
                    if attempt < 7 && transient_database_conflict(&error) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
                }
                result => return result.map(|write| write.mutation),
            }
        }
        unreachable!("bounded grant retry loop always returns")
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_grant_audited(
        &self,
        actor: &ActorSnapshot,
        profile_id: Uuid,
        provider_id: Uuid,
        can_browse: bool,
        can_play: bool,
        expected_admin_revision: i64,
        notifier: Option<&AuthorizationRevocationNotifier>,
        audit: &LiveAuditChain,
    ) -> Result<AuditedGrantMutation, LiveProviderGrantError> {
        let actor_snapshot = serde_json::to_string(actor)?;
        for attempt in 0..8u64 {
            match self
                .set_grant_once(
                    actor.actor_user_id,
                    &actor_snapshot,
                    profile_id,
                    provider_id,
                    can_browse,
                    can_play,
                    None,
                    notifier,
                    Some((audit, actor, expected_admin_revision)),
                )
                .await
            {
                Err(LiveProviderGrantError::Storage(error))
                    if attempt < 7 && transient_database_conflict(&error) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
                }
                Ok(write) => {
                    let audit = write.audit.ok_or(LiveProviderGrantError::InvalidState)?;
                    let grant = write.mutation.grant;
                    return Ok(AuditedGrantMutation {
                        provider_id,
                        profile_id,
                        can_browse: grant.as_ref().is_some_and(|grant| grant.can_browse),
                        can_play: grant.as_ref().is_some_and(|grant| grant.can_play),
                        revision: write
                            .admin_revision
                            .ok_or(LiveProviderGrantError::InvalidState)?,
                        audit,
                        revocation_event_id: write.mutation.revocation_event_id,
                    });
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded audited grant retry loop always returns")
    }

    #[allow(clippy::too_many_arguments)]
    async fn set_grant_once(
        &self,
        actor_user_id: Uuid,
        actor_snapshot: &str,
        profile_id: Uuid,
        provider_id: Uuid,
        can_browse: bool,
        can_play: bool,
        expected_authorization_revision: Option<i64>,
        notifier: Option<&AuthorizationRevocationNotifier>,
        audit: Option<(&LiveAuditChain, &ActorSnapshot, i64)>,
    ) -> Result<GrantWrite, LiveProviderGrantError> {
        validate_actor_snapshot(actor_snapshot)?;
        if can_play && !can_browse
            || expected_authorization_revision.is_some_and(|revision| revision < 1)
        {
            return Err(LiveProviderGrantError::InvalidInput);
        }

        let mut transaction = self.pool.begin().await?;
        // This no-op update is the portable serialization point for all grant writes
        // targeting a profile. The revision is bumped only after a real change.
        let target = sqlx::query(
            "UPDATE profile_authorization_revisions
             SET updated_at = updated_at
             WHERE profile_id = $1
             RETURNING home_id, revision",
        )
        .bind(profile_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(LiveProviderGrantError::TargetUnavailable)?;
        let home_id = parse_uuid(&target.try_get::<String, _>("home_id")?)?;
        let current_revision: i64 = target.try_get("revision")?;
        if expected_authorization_revision.is_some_and(|expected| expected != current_revision) {
            return Err(LiveProviderGrantError::RevisionChanged);
        }

        let required_role = audit.map(|(_, actor, _)| actor.home_role.as_str());
        let authorized: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM home_members
             WHERE home_id = $1 AND user_id = $2
               AND status = 'active'
               AND ($3 IS NULL AND role = 'owner' OR $3 IS NOT NULL AND role = $3)
             LIMIT 1",
        )
        .bind(home_id.to_string())
        .bind(actor_user_id.to_string())
        .bind(required_role)
        .fetch_optional(&mut *transaction)
        .await?;
        if authorized.is_none() {
            return Err(LiveProviderGrantError::Forbidden);
        }
        let provider_exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM providers WHERE provider_id = $1 LIMIT 1")
                .bind(provider_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?;
        if provider_exists.is_none() {
            return Err(LiveProviderGrantError::TargetUnavailable);
        }
        let current_admin_revision = match audit {
            Some((_, _, expected)) => Some(
                lock_admin_grant_revision(&mut transaction, home_id, provider_id, expected).await?,
            ),
            None => None,
        };

        let existing_row = sqlx::query(
            "SELECT id, profile_id, provider_id,
                    CAST(CASE WHEN can_browse THEN 1 ELSE 0 END AS BIGINT) AS can_browse,
                    CAST(CASE WHEN can_play THEN 1 ELSE 0 END AS BIGINT) AS can_play,
                    created_by_user_id,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
             FROM live_provider_grants
             WHERE profile_id = $1 AND provider_id = $2
             LIMIT 1",
        )
        .bind(profile_id.to_string())
        .bind(provider_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let existing = existing_row.as_ref().map(decode_grant).transpose()?;
        let before = existing
            .as_ref()
            .map_or((false, false), |grant| (grant.can_browse, grant.can_play));
        let after = (can_browse, can_play);
        let grant_id = existing
            .as_ref()
            .map_or_else(Uuid::new_v4, |grant| grant.id);
        if before == after {
            let audit = append_grant_audit(
                &mut transaction,
                audit.map(|(chain, actor, _)| (chain, actor)),
                home_id,
                profile_id,
                provider_id,
                grant_id,
                before,
                after,
                current_admin_revision.unwrap_or(current_revision),
            )
            .await?;
            transaction.commit().await?;
            return Ok(GrantWrite {
                mutation: GrantMutation {
                    grant: existing,
                    authorization_revision: current_revision,
                    revocation_event_id: None,
                },
                audit,
                admin_revision: current_admin_revision,
            });
        }

        if can_browse || can_play {
            sqlx::query(
                "INSERT INTO live_provider_grants (
                    id, profile_id, provider_id, can_browse, can_play,
                    created_by_user_id, created_by_actor_snapshot
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT(profile_id, provider_id) DO UPDATE SET
                    can_browse = excluded.can_browse,
                    can_play = excluded.can_play,
                    updated_at = CURRENT_TIMESTAMP",
            )
            .bind(grant_id.to_string())
            .bind(profile_id.to_string())
            .bind(provider_id.to_string())
            .bind(can_browse)
            .bind(can_play)
            .bind(actor_user_id.to_string())
            .bind(actor_snapshot)
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query("DELETE FROM live_provider_grants WHERE id = $1")
                .bind(grant_id.to_string())
                .execute(&mut *transaction)
                .await?;
        }
        let revision =
            bump_profile_authorization_revision_in_transaction(&mut transaction, profile_id)
                .await?;
        let admin_revision = current_admin_revision
            .map(|current| {
                current
                    .checked_add(1)
                    .ok_or(LiveProviderGrantError::InvalidState)
            })
            .transpose()?;
        if let (Some(current), Some(next)) = (current_admin_revision, admin_revision) {
            bump_admin_grant_revision(&mut transaction, home_id, provider_id, current, next)
                .await?;
        }
        let contraction = can_browse < before.0 || can_play < before.1;
        let event = if contraction {
            Some(
                append_authorization_revocation_in_transaction(
                    &mut transaction,
                    &NewAuthorizationRevocation::provider_grant_revoked(
                        home_id,
                        actor_user_id,
                        profile_id,
                        provider_id,
                        grant_id,
                        "live_provider_grant_contracted",
                        before,
                        after,
                    ),
                )
                .await?,
            )
        } else {
            None
        };
        let grant = if can_browse || can_play {
            let row = sqlx::query(
                "SELECT id, profile_id, provider_id,
                        CAST(CASE WHEN can_browse THEN 1 ELSE 0 END AS BIGINT) AS can_browse,
                        CAST(CASE WHEN can_play THEN 1 ELSE 0 END AS BIGINT) AS can_play,
                        created_by_user_id,
                        CAST(created_at AS TEXT) AS created_at,
                        CAST(updated_at AS TEXT) AS updated_at
                 FROM live_provider_grants WHERE id = $1 LIMIT 1",
            )
            .bind(grant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(LiveProviderGrantError::InvalidState)?;
            Some(decode_grant(&row)?)
        } else {
            None
        };
        let audit = append_grant_audit(
            &mut transaction,
            audit.map(|(chain, actor, _)| (chain, actor)),
            home_id,
            profile_id,
            provider_id,
            grant_id,
            before,
            after,
            admin_revision.unwrap_or(revision),
        )
        .await?;
        transaction.commit().await?;
        if let (Some(notifier), Some(event)) = (notifier, event.as_ref()) {
            notifier.publish(event.id);
        }
        Ok(GrantWrite {
            mutation: GrantMutation {
                grant,
                authorization_revision: revision,
                revocation_event_id: event.map(|event| event.id),
            },
            audit,
            admin_revision,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn append_grant_audit(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    audit: Option<(&LiveAuditChain, &ActorSnapshot)>,
    home_id: Uuid,
    profile_id: Uuid,
    provider_id: Uuid,
    grant_id: Uuid,
    before: (bool, bool),
    after: (bool, bool),
    revision: i64,
) -> Result<Option<AuditReference>, LiveProviderGrantError> {
    let Some((audit, actor)) = audit else {
        return Ok(None);
    };
    let before_value = serde_json::json!({
        "providerId": provider_id,
        "profileId": profile_id,
        "canBrowse": before.0,
        "canPlay": before.1,
    });
    let after_value = serde_json::json!({
        "providerId": provider_id,
        "profileId": profile_id,
        "canBrowse": after.0,
        "canPlay": after.1,
        "revision": revision,
    });
    let revoke = !after.0 && !after.1;
    audit
        .append(
            transaction,
            home_id,
            if revoke {
                AdminAction::ProviderGrantRevoke
            } else {
                AdminAction::ProviderGrantSet
            },
            "provider_grant",
            &grant_id.to_string(),
            actor,
            (!revoke).then_some(&before_value),
            (!revoke).then_some(&after_value),
            revoke.then_some(&before_value),
            Utc::now(),
        )
        .await
        .map(Some)
        .map_err(Into::into)
}

async fn lock_admin_grant_revision(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    home_id: Uuid,
    provider_id: Uuid,
    expected: i64,
) -> Result<i64, LiveProviderGrantError> {
    if expected < 1 {
        return Err(LiveProviderGrantError::InvalidInput);
    }
    sqlx::query(
        "INSERT INTO live_provider_admin_state (home_id, provider_id)
         VALUES ($1, $2)
         ON CONFLICT(home_id, provider_id) DO NOTHING",
    )
    .bind(home_id.to_string())
    .bind(provider_id.to_string())
    .execute(&mut **transaction)
    .await?;
    let revision: i64 = sqlx::query_scalar(
        "UPDATE live_provider_admin_state
         SET updated_at = updated_at
         WHERE home_id = $1 AND provider_id = $2
         RETURNING grant_revision",
    )
    .bind(home_id.to_string())
    .bind(provider_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if revision != expected {
        return Err(LiveProviderGrantError::RevisionChanged);
    }
    Ok(revision)
}

async fn bump_admin_grant_revision(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    home_id: Uuid,
    provider_id: Uuid,
    expected: i64,
    revision: i64,
) -> Result<(), LiveProviderGrantError> {
    let result = sqlx::query(
        "UPDATE live_provider_admin_state
         SET grant_revision = $1, updated_at = CURRENT_TIMESTAMP
         WHERE home_id = $2 AND provider_id = $3 AND grant_revision = $4",
    )
    .bind(revision)
    .bind(home_id.to_string())
    .bind(provider_id.to_string())
    .bind(expected)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(LiveProviderGrantError::RevisionChanged);
    }
    Ok(())
}

fn transient_database_conflict(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database) = error else {
        return false;
    };
    matches!(
        database.code().as_deref(),
        Some("5" | "6" | "40001" | "40P01" | "SQLITE_BUSY" | "SQLITE_LOCKED")
    )
}

fn validate_actor_snapshot(value: &str) -> Result<(), LiveProviderGrantError> {
    if value.trim().is_empty() || value.len() > MAX_ACTOR_SNAPSHOT_BYTES {
        return Err(LiveProviderGrantError::InvalidInput);
    }
    let value: serde_json::Value =
        serde_json::from_str(value).map_err(|_| LiveProviderGrantError::InvalidInput)?;
    if !value.is_object() {
        return Err(LiveProviderGrantError::InvalidInput);
    }
    Ok(())
}

fn decode_optional_grant(
    row: &sqlx::any::AnyRow,
) -> Result<Option<LiveProviderGrant>, LiveProviderGrantError> {
    let id: Option<String> = row.try_get("id")?;
    id.map(|_| decode_grant(row)).transpose()
}

fn decode_grant(row: &sqlx::any::AnyRow) -> Result<LiveProviderGrant, LiveProviderGrantError> {
    Ok(LiveProviderGrant {
        id: parse_uuid(&row.try_get::<String, _>("id")?)?,
        profile_id: parse_uuid(&row.try_get::<String, _>("profile_id")?)?,
        provider_id: parse_uuid(&row.try_get::<String, _>("provider_id")?)?,
        can_browse: row.try_get::<i64, _>("can_browse")? != 0,
        can_play: row.try_get::<i64, _>("can_play")? != 0,
        created_by_user_id: row
            .try_get::<Option<String>, _>("created_by_user_id")?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        created_at: parse_timestamp(&row.try_get::<String, _>("created_at")?)?,
        updated_at: parse_timestamp(&row.try_get::<String, _>("updated_at")?)?,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, LiveProviderGrantError> {
    Uuid::parse_str(value).map_err(|_| LiveProviderGrantError::InvalidState)
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, LiveProviderGrantError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc())
        })
        .map_err(|_| LiveProviderGrantError::InvalidState)
}
