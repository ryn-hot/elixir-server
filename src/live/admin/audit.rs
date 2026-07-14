use std::{
    collections::BTreeMap,
    fmt,
    fmt::Write as _,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{Any, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::auth::home_profiles::HomeRole;

const AUDIT_DOMAIN: &[u8] = b"elixir.live.audit.v1";
const RETENTION_DAYS: i64 = 365;
const MAX_ACTOR_BYTES: usize = 4_096;
const MAX_SNAPSHOT_BYTES: usize = 65_536;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAction {
    DestinationRuleCreate,
    DestinationRuleUpdate,
    DestinationRuleDelete,
    ProviderDisable,
    ProviderGrantSet,
    ProviderGrantRevoke,
    SessionTerminate,
    EnvelopeKeyRotate,
    AuditKeyRotate,
    TokenHashKeyRotate,
    EgressPolicySet,
    EgressDirectFallback,
}

impl AdminAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DestinationRuleCreate => "destination_rule_create",
            Self::DestinationRuleUpdate => "destination_rule_update",
            Self::DestinationRuleDelete => "destination_rule_delete",
            Self::ProviderDisable => "provider_disable",
            Self::ProviderGrantSet => "provider_grant_set",
            Self::ProviderGrantRevoke => "provider_grant_revoke",
            Self::SessionTerminate => "session_terminate",
            Self::EnvelopeKeyRotate => "envelope_key_rotate",
            Self::AuditKeyRotate => "audit_key_rotate",
            Self::TokenHashKeyRotate => "token_hash_key_rotate",
            Self::EgressPolicySet => "egress_policy_set",
            Self::EgressDirectFallback => "egress_direct_fallback",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorSnapshot {
    pub actor_user_id: Uuid,
    pub display_name: String,
    pub home_role: HomeRole,
}

impl ActorSnapshot {
    pub fn new(
        actor_user_id: Uuid,
        display_name: impl Into<String>,
        home_role: HomeRole,
    ) -> Result<Self, LiveAuditError> {
        let display_name = display_name.into();
        if display_name.trim().is_empty()
            || display_name.len() > 256
            || display_name.chars().any(char::is_control)
        {
            return Err(LiveAuditError::InvalidInput);
        }
        Ok(Self {
            actor_user_id,
            display_name,
            home_role,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditReference {
    pub audit_id: Uuid,
    pub action: &'static str,
    pub target_type: String,
    pub target_id: String,
    pub actor: ActorSnapshot,
    pub occurred_at: DateTime<Utc>,
    pub record_hash: String,
}

pub struct LiveAuditKey {
    key_id: String,
    material: Zeroizing<[u8; 32]>,
}

impl LiveAuditKey {
    pub fn new(key_id: impl Into<String>, material: [u8; 32]) -> Result<Self, LiveAuditError> {
        let key_id = key_id.into();
        if key_id.is_empty()
            || key_id.len() > 32
            || !key_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(LiveAuditError::InvalidKey);
        }
        Ok(Self {
            key_id,
            material: Zeroizing::new(material),
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

impl fmt::Debug for LiveAuditKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveAuditKey")
            .field("key_id", &self.key_id)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug)]
pub struct LiveAuditChain {
    primary: RwLock<Arc<LiveAuditKey>>,
}

impl LiveAuditChain {
    pub fn new(primary: LiveAuditKey) -> Self {
        Self {
            primary: RwLock::new(Arc::new(primary)),
        }
    }

    pub fn primary_key_id(&self) -> Result<String, LiveAuditError> {
        self.primary
            .read()
            .map(|primary| primary.key_id().to_string())
            .map_err(|_| LiveAuditError::InvalidState)
    }

    pub fn rotate_primary(&self, primary: LiveAuditKey) -> Result<(), LiveAuditError> {
        *self
            .primary
            .write()
            .map_err(|_| LiveAuditError::InvalidState)? = Arc::new(primary);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn append(
        &self,
        transaction: &mut Transaction<'_, Any>,
        home_id: Uuid,
        action: AdminAction,
        target_type: &str,
        target_id: &str,
        actor: &ActorSnapshot,
        before: Option<&Value>,
        after: Option<&Value>,
        tombstone: Option<&Value>,
        occurred_at: DateTime<Utc>,
    ) -> Result<AuditReference, LiveAuditError> {
        validate_identifier(target_type, 64)?;
        validate_identifier(target_id, 512)?;
        if before.is_none() && after.is_none() && tombstone.is_none() {
            return Err(LiveAuditError::InvalidInput);
        }
        let actor_json = canonical_json(&serde_json::to_value(actor)?)?;
        if actor_json.len() > MAX_ACTOR_BYTES {
            return Err(LiveAuditError::InvalidInput);
        }
        let before_json = canonical_optional(before)?;
        let after_json = canonical_optional(after)?;
        let tombstone_json = canonical_optional(tombstone)?;

        sqlx::query(
            "INSERT INTO live_admin_audit_chain_heads (home_id)
             VALUES ($1)
             ON CONFLICT(home_id) DO NOTHING",
        )
        .bind(home_id.to_string())
        .execute(&mut **transaction)
        .await?;
        let head = sqlx::query(
            "UPDATE live_admin_audit_chain_heads
             SET updated_at = updated_at
             WHERE home_id = $1
             RETURNING last_record_hash, revision",
        )
        .bind(home_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(LiveAuditError::InvalidState)?;
        let previous_hash: Option<String> = head.try_get("last_record_hash")?;
        let head_revision: i64 = head.try_get("revision")?;
        if head_revision < 0
            || previous_hash
                .as_deref()
                .is_some_and(|hash| !valid_hash(hash))
        {
            return Err(LiveAuditError::InvalidState);
        }

        let audit_id = Uuid::new_v4();
        let retain_until = occurred_at
            .checked_add_signed(Duration::days(RETENTION_DAYS))
            .ok_or(LiveAuditError::InvalidInput)?;
        let event = json!({
            "action": action.as_str(),
            "actorSnapshot": serde_json::from_str::<Value>(&actor_json)?,
            "actorUserId": actor.actor_user_id,
            "after": parse_optional(&after_json)?,
            "auditId": audit_id,
            "before": parse_optional(&before_json)?,
            "homeId": home_id,
            "occurredAt": occurred_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            "retainUntil": retain_until.to_rfc3339_opts(SecondsFormat::Millis, true),
            "targetId": target_id,
            "targetType": target_type,
            "tombstone": parse_optional(&tombstone_json)?,
        });
        let canonical_event = canonical_json(&event)?;
        let primary = self
            .primary
            .read()
            .map_err(|_| LiveAuditError::InvalidState)?
            .clone();
        let record_hash = Self::sign(
            &primary,
            previous_hash.as_deref(),
            canonical_event.as_bytes(),
        )?;

        sqlx::query(
            "INSERT INTO live_admin_audit_events (
                id, home_id, action, target_type, target_id, actor_user_id,
                actor_snapshot_json, before_json, after_json, tombstone_json,
                audit_key_id, previous_hash, record_hash, occurred_at, retain_until
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        )
        .bind(audit_id.to_string())
        .bind(home_id.to_string())
        .bind(action.as_str())
        .bind(target_type)
        .bind(target_id)
        .bind(actor.actor_user_id.to_string())
        .bind(&actor_json)
        .bind(before_json.as_deref())
        .bind(after_json.as_deref())
        .bind(tombstone_json.as_deref())
        .bind(primary.key_id())
        .bind(previous_hash.as_deref())
        .bind(&record_hash)
        .bind(occurred_at.to_rfc3339())
        .bind(retain_until.to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        let updated = sqlx::query(
            "UPDATE live_admin_audit_chain_heads
             SET last_record_hash = $1, revision = revision + 1,
                 updated_at = CURRENT_TIMESTAMP
             WHERE home_id = $2 AND revision = $3",
        )
        .bind(&record_hash)
        .bind(home_id.to_string())
        .bind(head_revision)
        .execute(&mut **transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(LiveAuditError::RevisionChanged);
        }
        Ok(AuditReference {
            audit_id,
            action: action.as_str(),
            target_type: target_type.to_string(),
            target_id: target_id.to_string(),
            actor: actor.clone(),
            occurred_at,
            record_hash,
        })
    }

    fn sign(
        primary: &LiveAuditKey,
        previous_hash: Option<&str>,
        canonical_event: &[u8],
    ) -> Result<String, LiveAuditError> {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(primary.material.as_ref())
            .map_err(|_| LiveAuditError::InvalidKey)?;
        mac.update(AUDIT_DOMAIN);
        update_length_prefixed(&mut mac, primary.key_id.as_bytes());
        update_length_prefixed(&mut mac, previous_hash.unwrap_or_default().as_bytes());
        update_length_prefixed(&mut mac, canonical_event);
        let bytes = mac.finalize().into_bytes();
        let mut output = String::with_capacity(64);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").map_err(|_| LiveAuditError::InvalidState)?;
        }
        Ok(output)
    }
}

#[derive(Debug, Error)]
pub enum LiveAuditError {
    #[error("invalid Live administrative audit input")]
    InvalidInput,
    #[error("invalid Live administrative audit key")]
    InvalidKey,
    #[error("invalid persisted Live administrative audit state")]
    InvalidState,
    #[error("Live administrative audit chain changed concurrently")]
    RevisionChanged,
    #[error("Live administrative audit storage failed")]
    Storage(#[from] sqlx::Error),
    #[error("Live administrative audit serialization failed")]
    Serialization(#[from] serde_json::Error),
}

fn update_length_prefixed(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}

fn canonical_optional(value: Option<&Value>) -> Result<Option<String>, LiveAuditError> {
    value
        .map(canonical_json)
        .transpose()?
        .map(|encoded| {
            if encoded.len() > MAX_SNAPSHOT_BYTES {
                Err(LiveAuditError::InvalidInput)
            } else {
                Ok(encoded)
            }
        })
        .transpose()
}

fn parse_optional(value: &Option<String>) -> Result<Value, serde_json::Error> {
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map(|value| value.unwrap_or(Value::Null))
}

fn canonical_json(value: &Value) -> Result<String, LiveAuditError> {
    let mut output = String::new();
    write_canonical(value, &mut output)?;
    Ok(output)
}

fn write_canonical(value: &Value, output: &mut String) -> Result<(), LiveAuditError> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(&value.to_string()),
        Value::String(value) => output.push_str(&serde_json::to_string(value)?),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_canonical(value, output)?;
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let sorted = values.iter().collect::<BTreeMap<_, _>>();
            for (index, (key, value)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key)?);
                output.push(':');
                write_canonical(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

fn validate_identifier(value: &str, max: usize) -> Result<(), LiveAuditError> {
    if value.trim().is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(LiveAuditError::InvalidInput);
    }
    Ok(())
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
