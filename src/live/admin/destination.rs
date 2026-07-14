use std::{net::IpAddr, sync::Arc};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Row, any::AnyRow};
use thiserror::Error;
use uuid::Uuid;

use crate::auth::revocation::{
    AuthorizationRevocationEvent, NewAuthorizationRevocation, RevocationError,
    append_authorization_revocation_in_transaction,
};

use super::{ActorSnapshot, AdminAction, AuditReference, LiveAuditChain, LiveAuditError};

const MAX_RULES_PER_PROVIDER: i64 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationNetworkScope {
    Public,
    PrivateLan,
}

impl DestinationNetworkScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::PrivateLan => "private_lan",
        }
    }
}

impl TryFrom<&str> for DestinationNetworkScope {
    type Error = LiveDestinationRuleError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "public" => Ok(Self::Public),
            "private_lan" => Ok(Self::PrivateLan),
            _ => Err(LiveDestinationRuleError::InvalidState),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DestinationRulePolicy {
    pub private_lan_enabled: bool,
    pub rtmp_certified: bool,
    pub srt_certified: bool,
}

impl Default for DestinationRulePolicy {
    fn default() -> Self {
        Self {
            private_lan_enabled: false,
            rtmp_certified: false,
            srt_certified: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DestinationRuleInput {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub network_scope: DestinationNetworkScope,
    pub allow_fetch: bool,
    pub allow_credentials: bool,
    pub allow_client_disclosure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DestinationRule {
    pub rule_id: Uuid,
    pub provider_id: Uuid,
    pub revision: i64,
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub network_scope: DestinationNetworkScope,
    pub allow_fetch: bool,
    pub allow_credentials: bool,
    pub allow_client_disclosure: bool,
    pub created_by: ActorSnapshot,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DestinationRuleMutation {
    pub provider_id: Uuid,
    pub rule_id: Uuid,
    pub revision: i64,
    #[serde(skip)]
    pub provider_revision: i64,
    pub deleted: bool,
    pub rule: Option<DestinationRule>,
    pub audit: AuditReference,
    #[serde(skip)]
    pub terminate_provider_sessions: bool,
    #[serde(skip)]
    pub(crate) revocation_event: Option<AuthorizationRevocationEvent>,
}

#[derive(Clone)]
pub struct LiveDestinationRuleRepository {
    pool: AnyPool,
    audit: Arc<LiveAuditChain>,
    policy: DestinationRulePolicy,
}

impl LiveDestinationRuleRepository {
    pub fn new(pool: AnyPool, audit: Arc<LiveAuditChain>, policy: DestinationRulePolicy) -> Self {
        Self {
            pool,
            audit,
            policy,
        }
    }

    pub async fn list(
        &self,
        home_id: Uuid,
        provider_id: Uuid,
    ) -> Result<Vec<DestinationRule>, LiveDestinationRuleError> {
        let rows = sqlx::query(
            "SELECT id, provider_id, revision, scheme, normalized_host, port, exact_path,
                    network_scope,
                    CAST(CASE WHEN allow_fetch THEN 1 ELSE 0 END AS BIGINT) AS allow_fetch,
                    CAST(CASE WHEN allow_credentials THEN 1 ELSE 0 END AS BIGINT) AS allow_credentials,
                    CAST(CASE WHEN allow_client_disclosure THEN 1 ELSE 0 END AS BIGINT) AS allow_client_disclosure,
                    created_by_actor_snapshot,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
             FROM live_provider_destination_rules
             WHERE home_id = $1 AND provider_id = $2
             ORDER BY scheme, normalized_host, port, exact_path, network_scope, id
             LIMIT 256",
        )
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_rule).collect()
    }

    pub async fn create(
        &self,
        home_id: Uuid,
        provider_id: Uuid,
        expected_provider_revision: i64,
        actor: &ActorSnapshot,
        input: DestinationRuleInput,
        now: DateTime<Utc>,
    ) -> Result<DestinationRuleMutation, LiveDestinationRuleError> {
        if expected_provider_revision < 1 {
            return Err(LiveDestinationRuleError::InvalidInput);
        }
        let normalized = normalize(input, self.policy)?;
        let actor_json = canonical_actor(actor)?;
        let mut transaction = self.pool.begin().await?;
        require_owner_and_provider(&mut transaction, home_id, provider_id, actor).await?;
        let current_provider_revision =
            lock_provider_state(&mut transaction, home_id, provider_id).await?;
        if current_provider_revision != expected_provider_revision {
            return Err(LiveDestinationRuleError::RevisionChanged);
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_provider_destination_rules
             WHERE home_id = $1 AND provider_id = $2",
        )
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if count >= MAX_RULES_PER_PROVIDER {
            return Err(LiveDestinationRuleError::CapacityExceeded);
        }

        let rule_id = Uuid::new_v4();
        let insert = sqlx::query(
            "INSERT INTO live_provider_destination_rules (
                id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
                created_by_user_id, created_by_actor_snapshot, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)",
        )
        .bind(rule_id.to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(&normalized.scheme)
        .bind(&normalized.host)
        .bind(i64::from(normalized.port))
        .bind(&normalized.path)
        .bind(normalized.network_scope.as_str())
        .bind(normalized.allow_fetch)
        .bind(normalized.allow_credentials)
        .bind(normalized.allow_client_disclosure)
        .bind(actor.actor_user_id.to_string())
        .bind(&actor_json)
        .bind(now.to_rfc3339())
        .execute(&mut *transaction)
        .await;
        map_write(insert)?;

        let provider_revision = bump_provider_revision(
            &mut transaction,
            home_id,
            provider_id,
            current_provider_revision,
        )
        .await?;
        let rule = load_rule(&mut transaction, home_id, provider_id, rule_id).await?;
        let after = serde_json::to_value(&rule)?;
        let audit = self
            .audit
            .append(
                &mut transaction,
                home_id,
                AdminAction::DestinationRuleCreate,
                "destination_rule",
                &rule_id.to_string(),
                actor,
                None,
                Some(&after),
                None,
                now,
            )
            .await?;
        transaction.commit().await?;
        Ok(DestinationRuleMutation {
            provider_id,
            rule_id,
            revision: rule.revision,
            provider_revision,
            deleted: false,
            rule: Some(rule),
            audit,
            terminate_provider_sessions: false,
            revocation_event: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update(
        &self,
        home_id: Uuid,
        provider_id: Uuid,
        rule_id: Uuid,
        expected_revision: i64,
        actor: &ActorSnapshot,
        input: DestinationRuleInput,
        now: DateTime<Utc>,
    ) -> Result<DestinationRuleMutation, LiveDestinationRuleError> {
        if expected_revision < 1 {
            return Err(LiveDestinationRuleError::InvalidInput);
        }
        let normalized = normalize(input, self.policy)?;
        let mut transaction = self.pool.begin().await?;
        require_owner_and_provider(&mut transaction, home_id, provider_id, actor).await?;
        let provider_revision = lock_provider_state(&mut transaction, home_id, provider_id).await?;
        let before_rule = load_rule(&mut transaction, home_id, provider_id, rule_id).await?;
        if before_rule.revision != expected_revision {
            return Err(LiveDestinationRuleError::RevisionChanged);
        }
        let update = sqlx::query(
            "UPDATE live_provider_destination_rules
             SET scheme = $1, normalized_host = $2, port = $3, exact_path = $4,
                 network_scope = $5, allow_fetch = $6, allow_credentials = $7,
                 allow_client_disclosure = $8, revision = revision + 1, updated_at = $9
             WHERE id = $10 AND home_id = $11 AND provider_id = $12 AND revision = $13",
        )
        .bind(&normalized.scheme)
        .bind(&normalized.host)
        .bind(i64::from(normalized.port))
        .bind(&normalized.path)
        .bind(normalized.network_scope.as_str())
        .bind(normalized.allow_fetch)
        .bind(normalized.allow_credentials)
        .bind(normalized.allow_client_disclosure)
        .bind(now.to_rfc3339())
        .bind(rule_id.to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(expected_revision)
        .execute(&mut *transaction)
        .await;
        let updated = map_write(update)?;
        if updated.rows_affected() != 1 {
            return Err(LiveDestinationRuleError::RevisionChanged);
        }
        let provider_revision =
            bump_provider_revision(&mut transaction, home_id, provider_id, provider_revision)
                .await?;
        let rule = load_rule(&mut transaction, home_id, provider_id, rule_id).await?;
        let before = serde_json::to_value(&before_rule)?;
        let after = serde_json::to_value(&rule)?;
        let audit = self
            .audit
            .append(
                &mut transaction,
                home_id,
                AdminAction::DestinationRuleUpdate,
                "destination_rule",
                &rule_id.to_string(),
                actor,
                Some(&before),
                Some(&after),
                None,
                now,
            )
            .await?;
        let revocation_event = append_authorization_revocation_in_transaction(
            &mut transaction,
            &NewAuthorizationRevocation::provider_policy_changed(
                home_id,
                actor.actor_user_id,
                provider_id,
                "live_destination_rule_updated",
                provider_revision,
            ),
        )
        .await?;
        transaction.commit().await?;
        Ok(DestinationRuleMutation {
            provider_id,
            rule_id,
            revision: rule.revision,
            provider_revision,
            deleted: false,
            rule: Some(rule),
            audit,
            terminate_provider_sessions: true,
            revocation_event: Some(revocation_event),
        })
    }

    pub async fn delete(
        &self,
        home_id: Uuid,
        provider_id: Uuid,
        rule_id: Uuid,
        expected_revision: i64,
        actor: &ActorSnapshot,
        now: DateTime<Utc>,
    ) -> Result<DestinationRuleMutation, LiveDestinationRuleError> {
        if expected_revision < 1 {
            return Err(LiveDestinationRuleError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        require_owner_and_provider(&mut transaction, home_id, provider_id, actor).await?;
        let provider_revision = lock_provider_state(&mut transaction, home_id, provider_id).await?;
        let before_rule = load_rule(&mut transaction, home_id, provider_id, rule_id).await?;
        if before_rule.revision != expected_revision {
            return Err(LiveDestinationRuleError::RevisionChanged);
        }
        let deleted = sqlx::query(
            "DELETE FROM live_provider_destination_rules
             WHERE id = $1 AND home_id = $2 AND provider_id = $3 AND revision = $4",
        )
        .bind(rule_id.to_string())
        .bind(home_id.to_string())
        .bind(provider_id.to_string())
        .bind(expected_revision)
        .execute(&mut *transaction)
        .await?;
        if deleted.rows_affected() != 1 {
            return Err(LiveDestinationRuleError::RevisionChanged);
        }
        let provider_revision =
            bump_provider_revision(&mut transaction, home_id, provider_id, provider_revision)
                .await?;
        let tombstone = serde_json::to_value(&before_rule)?;
        let audit = self
            .audit
            .append(
                &mut transaction,
                home_id,
                AdminAction::DestinationRuleDelete,
                "destination_rule",
                &rule_id.to_string(),
                actor,
                None,
                None,
                Some(&tombstone),
                now,
            )
            .await?;
        let revocation_event = append_authorization_revocation_in_transaction(
            &mut transaction,
            &NewAuthorizationRevocation::provider_policy_changed(
                home_id,
                actor.actor_user_id,
                provider_id,
                "live_destination_rule_deleted",
                provider_revision,
            ),
        )
        .await?;
        transaction.commit().await?;
        Ok(DestinationRuleMutation {
            provider_id,
            rule_id,
            revision: expected_revision + 1,
            provider_revision,
            deleted: true,
            rule: None,
            audit,
            terminate_provider_sessions: true,
            revocation_event: Some(revocation_event),
        })
    }
}

#[derive(Debug, Error)]
pub enum LiveDestinationRuleError {
    #[error("invalid Live destination rule")]
    InvalidInput,
    #[error("Live destination rule target was not found")]
    NotFound,
    #[error("only an active home owner may manage Live destination rules")]
    Forbidden,
    #[error("Live destination rule revision changed")]
    RevisionChanged,
    #[error("Live destination rule conflicts with an existing normalized rule")]
    Conflict,
    #[error("Live destination rule capacity was reached")]
    CapacityExceeded,
    #[error("invalid persisted Live destination rule state")]
    InvalidState,
    #[error("Live destination rule storage failed")]
    Storage(#[from] sqlx::Error),
    #[error("Live destination rule audit failed")]
    Audit(#[from] LiveAuditError),
    #[error("Live destination rule revocation publication failed")]
    Revocation(#[from] RevocationError),
    #[error("Live destination rule serialization failed")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug)]
struct NormalizedRule {
    scheme: String,
    host: String,
    port: u16,
    path: String,
    network_scope: DestinationNetworkScope,
    allow_fetch: bool,
    allow_credentials: bool,
    allow_client_disclosure: bool,
}

fn normalize(
    input: DestinationRuleInput,
    policy: DestinationRulePolicy,
) -> Result<NormalizedRule, LiveDestinationRuleError> {
    if input.port == 0 {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    let scheme = input.scheme.to_ascii_lowercase();
    if input.scheme.trim() != input.scheme
        || !matches!(scheme.as_str(), "http" | "https" | "rtmp" | "srt")
    {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    if input.network_scope == DestinationNetworkScope::PrivateLan && !policy.private_lan_enabled {
        return Err(LiveDestinationRuleError::Forbidden);
    }
    if scheme == "rtmp" && !policy.rtmp_certified || scheme == "srt" && !policy.srt_certified {
        return Err(LiveDestinationRuleError::Forbidden);
    }
    if matches!(scheme.as_str(), "rtmp" | "srt")
        && (!input.allow_fetch || input.allow_credentials || input.allow_client_disclosure)
    {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    if input.allow_credentials && (scheme != "https" || !input.allow_fetch)
        || input.allow_client_disclosure
            && (scheme != "https"
                || !input.allow_fetch
                || input.allow_credentials
                || input.network_scope != DestinationNetworkScope::Public)
    {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    let host = normalize_host(&input.host)?;
    let path = normalize_path(&input.path)?;
    Ok(NormalizedRule {
        scheme,
        host,
        port: input.port,
        path,
        network_scope: input.network_scope,
        allow_fetch: input.allow_fetch,
        allow_credentials: input.allow_credentials,
        allow_client_disclosure: input.allow_client_disclosure,
    })
}

fn normalize_host(input: &str) -> Result<String, LiveDestinationRuleError> {
    if input.is_empty()
        || input.len() > 255
        || input.trim() != input
        || input.chars().any(char::is_control)
        || input.contains(['/', '\\', '?', '#', '@', '*'])
    {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    let bracketless = input
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(input);
    if let Ok(address) = bracketless.parse::<IpAddr>() {
        return Ok(address.to_string().to_ascii_lowercase());
    }
    if input.contains(':') {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    let host = idna::domain_to_ascii(input.trim_end_matches('.'))
        .map_err(|_| LiveDestinationRuleError::InvalidInput)?
        .to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    for label in host.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(LiveDestinationRuleError::InvalidInput);
        }
    }
    Ok(host)
}

fn normalize_path(input: &str) -> Result<String, LiveDestinationRuleError> {
    if input.is_empty()
        || input.len() > 2_048
        || !input.starts_with('/')
        || input.trim() != input
        || input
            .chars()
            .any(|value| value.is_control() || value == '\\' || value == ' ')
        || input.contains(['?', '#', '*'])
    {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(LiveDestinationRuleError::InvalidInput);
            }
            let high = hex(bytes[index + 1]).ok_or(LiveDestinationRuleError::InvalidInput)?;
            let low = hex(bytes[index + 2]).ok_or(LiveDestinationRuleError::InvalidInput)?;
            let decoded = high * 16 + low;
            if matches!(decoded, b'/' | b'\\' | b'.' | 0..=31 | 127) {
                return Err(LiveDestinationRuleError::InvalidInput);
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    let mut segments = Vec::new();
    for segment in input.split('/').skip(1) {
        match segment {
            "." => {}
            ".." => {
                segments.pop();
            }
            value => segments.push(value),
        }
    }
    let normalized = format!("/{}", segments.join("/"));
    Ok(normalized)
}

fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

async fn require_owner_and_provider(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    home_id: Uuid,
    provider_id: Uuid,
    actor: &ActorSnapshot,
) -> Result<(), LiveDestinationRuleError> {
    if actor.home_role != crate::auth::home_profiles::HomeRole::Owner {
        return Err(LiveDestinationRuleError::Forbidden);
    }
    let owner: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM home_members
         WHERE home_id = $1 AND user_id = $2 AND role = 'owner' AND status = 'active'
         LIMIT 1",
    )
    .bind(home_id.to_string())
    .bind(actor.actor_user_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if owner.is_none() {
        return Err(LiveDestinationRuleError::Forbidden);
    }
    let provider: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM providers
         WHERE provider_id = $1 AND capability = 'live.catalog_provider/v1'
         LIMIT 1",
    )
    .bind(provider_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if provider.is_none() {
        return Err(LiveDestinationRuleError::NotFound);
    }
    Ok(())
}

async fn lock_provider_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    home_id: Uuid,
    provider_id: Uuid,
) -> Result<i64, LiveDestinationRuleError> {
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
         RETURNING provider_revision",
    )
    .bind(home_id.to_string())
    .bind(provider_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if revision < 1 {
        return Err(LiveDestinationRuleError::InvalidState);
    }
    Ok(revision)
}

async fn bump_provider_revision(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    home_id: Uuid,
    provider_id: Uuid,
    expected: i64,
) -> Result<i64, LiveDestinationRuleError> {
    let revision = expected
        .checked_add(1)
        .ok_or(LiveDestinationRuleError::InvalidState)?;
    let result = sqlx::query(
        "UPDATE live_provider_admin_state
         SET provider_revision = $1, updated_at = CURRENT_TIMESTAMP
         WHERE home_id = $2 AND provider_id = $3 AND provider_revision = $4",
    )
    .bind(revision)
    .bind(home_id.to_string())
    .bind(provider_id.to_string())
    .bind(expected)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(LiveDestinationRuleError::RevisionChanged);
    }
    Ok(revision)
}

async fn load_rule(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    home_id: Uuid,
    provider_id: Uuid,
    rule_id: Uuid,
) -> Result<DestinationRule, LiveDestinationRuleError> {
    let row = sqlx::query(
        "SELECT id, provider_id, revision, scheme, normalized_host, port, exact_path,
                network_scope,
                CAST(CASE WHEN allow_fetch THEN 1 ELSE 0 END AS BIGINT) AS allow_fetch,
                CAST(CASE WHEN allow_credentials THEN 1 ELSE 0 END AS BIGINT) AS allow_credentials,
                CAST(CASE WHEN allow_client_disclosure THEN 1 ELSE 0 END AS BIGINT) AS allow_client_disclosure,
                created_by_actor_snapshot,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM live_provider_destination_rules
         WHERE id = $1 AND home_id = $2 AND provider_id = $3
         LIMIT 1",
    )
    .bind(rule_id.to_string())
    .bind(home_id.to_string())
    .bind(provider_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(LiveDestinationRuleError::NotFound)?;
    decode_rule(&row)
}

fn decode_rule(row: &AnyRow) -> Result<DestinationRule, LiveDestinationRuleError> {
    let port: i64 = row.try_get("port")?;
    let revision: i64 = row.try_get("revision")?;
    if revision < 1 {
        return Err(LiveDestinationRuleError::InvalidState);
    }
    Ok(DestinationRule {
        rule_id: parse_uuid(&row.try_get::<String, _>("id")?)?,
        provider_id: parse_uuid(&row.try_get::<String, _>("provider_id")?)?,
        revision,
        scheme: row.try_get("scheme")?,
        host: row.try_get("normalized_host")?,
        port: u16::try_from(port).map_err(|_| LiveDestinationRuleError::InvalidState)?,
        path: row.try_get("exact_path")?,
        network_scope: DestinationNetworkScope::try_from(
            row.try_get::<String, _>("network_scope")?.as_str(),
        )?,
        allow_fetch: row.try_get::<i64, _>("allow_fetch")? != 0,
        allow_credentials: row.try_get::<i64, _>("allow_credentials")? != 0,
        allow_client_disclosure: row.try_get::<i64, _>("allow_client_disclosure")? != 0,
        created_by: serde_json::from_str(&row.try_get::<String, _>("created_by_actor_snapshot")?)
            .map_err(|_| LiveDestinationRuleError::InvalidState)?,
        created_at: parse_timestamp(&row.try_get::<String, _>("created_at")?)?,
        updated_at: parse_timestamp(&row.try_get::<String, _>("updated_at")?)?,
    })
}

fn canonical_actor(actor: &ActorSnapshot) -> Result<String, LiveDestinationRuleError> {
    let value = serde_json::to_string(actor)?;
    if value.len() > 4_096 {
        return Err(LiveDestinationRuleError::InvalidInput);
    }
    Ok(value)
}

fn parse_uuid(value: &str) -> Result<Uuid, LiveDestinationRuleError> {
    Uuid::parse_str(value).map_err(|_| LiveDestinationRuleError::InvalidState)
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, LiveDestinationRuleError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc())
        })
        .map_err(|_| LiveDestinationRuleError::InvalidState)
}

fn map_write(
    result: Result<sqlx::any::AnyQueryResult, sqlx::Error>,
) -> Result<sqlx::any::AnyQueryResult, LiveDestinationRuleError> {
    result.map_err(|error| {
        if unique_violation(&error) {
            LiveDestinationRuleError::Conflict
        } else {
            LiveDestinationRuleError::Storage(error)
        }
    })
}

fn unique_violation(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database) = error else {
        return false;
    };
    matches!(
        database.code().as_deref(),
        Some(
            "2067" | "1555" | "23505" | "SQLITE_CONSTRAINT_UNIQUE" | "SQLITE_CONSTRAINT_PRIMARYKEY"
        )
    )
}
