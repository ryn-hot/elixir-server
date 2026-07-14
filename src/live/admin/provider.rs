use std::collections::BTreeMap;

use serde::Serialize;
use sqlx::{AnyPool, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    auth::revocation::{
        NewAuthorizationRevocation, RevocationError, append_authorization_revocation_in_transaction,
    },
    live::{
        admin::{ActorSnapshot, AdminAction, AuditReference, LiveAuditChain, LiveAuditError},
        contract::StreamProtocol,
    },
};

const LIVE_PROVIDER_CAPABILITY: &str = "live.catalog_provider/v1";

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminProviderSummary {
    pub provider_id: Uuid,
    pub enabled: bool,
    pub readiness: &'static str,
    pub disabled_reason: Option<&'static str>,
    pub provider_revision: i64,
    pub grant_revision: i64,
    pub active_sessions: u32,
    pub effective_protocols: Vec<StreamProtocol>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderDisableMutation {
    pub operation_id: Uuid,
    pub status: &'static str,
    pub revision: i64,
    pub provider_id: Uuid,
    pub audit: AuditReference,
    #[serde(skip)]
    pub revocation_event_ids: Vec<Uuid>,
}

#[derive(Clone)]
pub struct LiveProviderAdminRepository {
    pool: AnyPool,
}

impl LiveProviderAdminRepository {
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }

    pub async fn list(
        &self,
        home_id: Uuid,
        ready_protocols: &BTreeMap<Uuid, Vec<StreamProtocol>>,
    ) -> Result<Vec<AdminProviderSummary>, LiveProviderAdminError> {
        let rows = sqlx::query(
            "SELECT providers.provider_id,
                    CAST(CASE WHEN extensions.enabled AND instances.enabled
                              THEN 1 ELSE 0 END AS BIGINT) AS enabled,
                    providers.health_state,
                    COALESCE(readiness.readiness_phase, '') AS readiness_phase,
                    COALESCE(admin.provider_revision, 1) AS provider_revision,
                    COALESCE(admin.grant_revision, 1) AS grant_revision,
                    (SELECT COUNT(*) FROM live_playback_sessions AS sessions
                     WHERE sessions.home_id = $1
                       AND sessions.provider_id = providers.provider_id
                       AND sessions.state NOT IN ('ended', 'expired', 'failed')) AS active_sessions,
                    CAST(CASE WHEN extensions.enabled THEN 1 ELSE 0 END AS BIGINT) AS extension_enabled,
                    CAST(CASE WHEN instances.enabled THEN 1 ELSE 0 END AS BIGINT) AS instance_enabled
             FROM providers
             JOIN extension_instances AS instances
               ON instances.instance_id = providers.instance_id
             JOIN extensions
               ON extensions.extension_id = instances.extension_id
             LEFT JOIN provider_readiness AS readiness
               ON readiness.provider_id = providers.provider_id
             LEFT JOIN live_provider_admin_state AS admin
               ON admin.home_id = $1 AND admin.provider_id = providers.provider_id
             WHERE providers.capability = $2
             ORDER BY extensions.extension_id, providers.slot_id, providers.provider_id
             LIMIT 200",
        )
        .bind(home_id.to_string())
        .bind(LIVE_PROVIDER_CAPABILITY)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let provider_id = Uuid::parse_str(&row.try_get::<String, _>("provider_id")?)
                    .map_err(|_| LiveProviderAdminError::InvalidState)?;
                let enabled = row.try_get::<i64, _>("enabled")? != 0;
                let extension_enabled = row.try_get::<i64, _>("extension_enabled")? != 0;
                let instance_enabled = row.try_get::<i64, _>("instance_enabled")? != 0;
                let health_state: String = row.try_get("health_state")?;
                let readiness_phase: String = row.try_get("readiness_phase")?;
                let protocols = ready_protocols
                    .get(&provider_id)
                    .cloned()
                    .unwrap_or_default();
                let (readiness, disabled_reason) = if !extension_enabled {
                    ("disabled", Some("extension_disabled"))
                } else if !instance_enabled {
                    ("disabled", Some("instance_disabled"))
                } else if health_state == "degraded" {
                    ("degraded", Some("provider_degraded"))
                } else if health_state != "healthy" {
                    ("unavailable", Some("provider_unhealthy"))
                } else if readiness_phase != "driver_ready" || protocols.is_empty() {
                    ("unavailable", Some("driver_not_ready"))
                } else {
                    ("ready", None)
                };
                let provider_revision: i64 = row.try_get("provider_revision")?;
                let grant_revision: i64 = row.try_get("grant_revision")?;
                let active_sessions: i64 = row.try_get("active_sessions")?;
                if provider_revision < 1
                    || grant_revision < 1
                    || !(0..=10_000).contains(&active_sessions)
                {
                    return Err(LiveProviderAdminError::InvalidState);
                }
                Ok(AdminProviderSummary {
                    provider_id,
                    enabled,
                    readiness,
                    disabled_reason,
                    provider_revision,
                    grant_revision,
                    active_sessions: u32::try_from(active_sessions)
                        .map_err(|_| LiveProviderAdminError::InvalidState)?,
                    effective_protocols: protocols,
                })
            })
            .collect()
    }

    pub async fn disable(
        &self,
        home_id: Uuid,
        provider_id: Uuid,
        expected_revision: i64,
        actor: &ActorSnapshot,
        audit: &LiveAuditChain,
    ) -> Result<ProviderDisableMutation, LiveProviderAdminError> {
        if expected_revision < 1 {
            return Err(LiveProviderAdminError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        let membership_role: Option<String> = sqlx::query_scalar(
            "SELECT role FROM home_members
             WHERE home_id = $1 AND user_id = $2 AND status = 'active'
             LIMIT 1",
        )
        .bind(home_id.to_string())
        .bind(actor.actor_user_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if membership_role.as_deref() != Some(actor.home_role.as_str()) {
            return Err(LiveProviderAdminError::Forbidden);
        }
        let provider = sqlx::query(
            "SELECT providers.instance_id,
                    CAST(CASE WHEN instances.enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled
             FROM providers
             JOIN extension_instances AS instances
               ON instances.instance_id = providers.instance_id
             WHERE providers.provider_id = $1 AND providers.capability = $2
             LIMIT 1",
        )
        .bind(provider_id.to_string())
        .bind(LIVE_PROVIDER_CAPABILITY)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(LiveProviderAdminError::NotFound)?;
        let instance_id: String = provider.try_get("instance_id")?;
        let was_enabled = provider.try_get::<i64, _>("enabled")? != 0;

        sqlx::query(
            "INSERT INTO live_provider_admin_state (home_id, provider_id)
             VALUES ($1, $2)
             ON CONFLICT(home_id, provider_id) DO NOTHING",
        )
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let current_revision: i64 = sqlx::query_scalar(
            "UPDATE live_provider_admin_state SET updated_at = updated_at
             WHERE home_id = $1 AND provider_id = $2
             RETURNING provider_revision",
        )
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if current_revision != expected_revision {
            return Err(LiveProviderAdminError::RevisionChanged);
        }
        let revision = if was_enabled {
            current_revision
                .checked_add(1)
                .ok_or(LiveProviderAdminError::InvalidState)?
        } else {
            current_revision
        };
        if was_enabled {
            let disabled = sqlx::query(
                "UPDATE extension_instances
                 SET enabled = FALSE, updated_at = CURRENT_TIMESTAMP
                 WHERE instance_id = $1 AND enabled = TRUE",
            )
            .bind(&instance_id)
            .execute(&mut *transaction)
            .await?;
            if disabled.rows_affected() != 1 {
                return Err(LiveProviderAdminError::RevisionChanged);
            }
            let bumped = sqlx::query(
                "UPDATE live_provider_admin_state
                 SET provider_revision = $1, updated_at = CURRENT_TIMESTAMP
                 WHERE home_id = $2 AND provider_id = $3 AND provider_revision = $4",
            )
            .bind(revision)
            .bind(home_id.to_string())
            .bind(provider_id.to_string())
            .bind(current_revision)
            .execute(&mut *transaction)
            .await?;
            if bumped.rows_affected() != 1 {
                return Err(LiveProviderAdminError::RevisionChanged);
            }
        }
        let before = serde_json::json!({
            "providerId": provider_id,
            "enabled": was_enabled,
            "revision": current_revision,
        });
        let after = serde_json::json!({
            "providerId": provider_id,
            "enabled": false,
            "revision": revision,
        });
        let audit = audit
            .append(
                &mut transaction,
                home_id,
                AdminAction::ProviderDisable,
                "provider",
                &provider_id.to_string(),
                actor,
                Some(&before),
                Some(&after),
                None,
                chrono::Utc::now(),
            )
            .await?;
        let affected = sqlx::query_scalar::<_, String>(
            "SELECT provider_id FROM providers
             WHERE instance_id = $1 AND capability = $2
             ORDER BY provider_id LIMIT 200",
        )
        .bind(&instance_id)
        .bind(LIVE_PROVIDER_CAPABILITY)
        .fetch_all(&mut *transaction)
        .await?;
        let mut revocation_event_ids = Vec::with_capacity(affected.len());
        if was_enabled {
            for affected_provider in affected {
                let affected_provider = Uuid::parse_str(&affected_provider)
                    .map_err(|_| LiveProviderAdminError::InvalidState)?;
                let event = append_authorization_revocation_in_transaction(
                    &mut transaction,
                    &NewAuthorizationRevocation::provider_disabled(
                        home_id,
                        actor.actor_user_id,
                        affected_provider,
                        "live_provider_disabled",
                    ),
                )
                .await?;
                revocation_event_ids.push(event.id);
            }
        }
        transaction.commit().await?;
        Ok(ProviderDisableMutation {
            operation_id: Uuid::new_v4(),
            status: "accepted",
            revision,
            provider_id,
            audit,
            revocation_event_ids,
        })
    }
}

#[derive(Debug, Error)]
pub enum LiveProviderAdminError {
    #[error("invalid Live provider administrative input")]
    InvalidInput,
    #[error("Live provider administrative target was not found")]
    NotFound,
    #[error("Live provider administrator is forbidden")]
    Forbidden,
    #[error("Live provider administrative revision changed")]
    RevisionChanged,
    #[error("invalid persisted Live provider administrative state")]
    InvalidState,
    #[error("Live provider administrative storage failed")]
    Storage(#[from] sqlx::Error),
    #[error("Live provider administrative audit failed")]
    Audit(#[from] LiveAuditError),
    #[error("Live provider administrative revocation failed")]
    Revocation(#[from] RevocationError),
}
