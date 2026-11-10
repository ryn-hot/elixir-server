//! Durable independent security fences. A compromised/disposable peer catalog
//! cannot delete them or promote a bare score/JSON flag into authority. These
//! fences do not prove global latest state, source equality or install consent.
use super::worker::{MetadataObservation, PublicationObservation, SecurityObservation};
use crate::db::{Database, DatabaseDriver};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    SCHEMA_VERSION, canonical,
    identity::{Digest, ExtensionId, PublicKey},
    security_job::{HeadState, SecurityRecords, SecuritySummary},
};
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Row};
use std::collections::BTreeSet;

mod export;
mod preview;
pub(in crate::marketplace) mod retained;
mod unchanged;
pub(in crate::marketplace) use export::catalog_baseline;
pub(in crate::marketplace) use preview::observation_needed;

const MAX_ROWS: i64 = 10_000;
const MAX_BYTES: i64 = 8 * 1024 * 1024;
const MAX_REVISION: i64 = (1i64 << 53) - 1;

#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    #[error("direct source owner authorization is no longer current")]
    OwnerAuthorization,
    #[error("metadata observation is no longer current")]
    Expired,
    #[error("marketplace candidate is stale against retained security state")]
    Stale,
    #[error("marketplace release or signing key is withdrawn")]
    Withdrawn,
    #[error("marketplace identity is conflicted or saturated")]
    Conflicted,
    #[error("marketplace security state capacity exhausted")]
    Capacity,
}
pub(in crate::marketplace) fn is_expected_rejection(error: &anyhow::Error) -> bool {
    error.downcast_ref::<Rejection>().is_some()
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublisherState {
    schema_version: u32,
    root_key: PublicKey,
    sequence: u64,
    payload_id: Digest,
    conflicted: bool,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadFence {
    generation: u64,
    sequence: u64,
    payload_id: Digest,
    conflicted: bool,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtensionState {
    schema_version: u32,
    extension_id: ExtensionId,
    root_key: PublicKey,
    head: Option<HeadFence>,
    revoked_releases: BTreeSet<Digest>,
    revoked_keys: BTreeSet<PublicKey>,
    saturated: bool,
}
struct Incoming {
    records: SecurityRecords,
    summary: SecuritySummary,
}
/// A transaction-owned, restriction-only delta. No deserializer or public
/// constructor: a caller cannot turn peer JSON into a durable-store update.
pub(in crate::marketplace) struct CatalogMutation {
    revision: u64,
    tracked: bool,
    batch: Option<elixir_marketplace::catalog_fence::FenceBatch>,
}
impl CatalogMutation {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn tracked(&self) -> bool {
        self.tracked
    }
    pub fn into_batch(self) -> Option<elixir_marketplace::catalog_fence::FenceBatch> {
        self.batch
    }
}
pub(in crate::marketplace) async fn catalog_admit(
    database: &Database,
    value: &MetadataObservation,
    expected_revision: u64,
) -> Result<CatalogMutation> {
    if !value.is_current() {
        return Err(Rejection::Expired.into());
    }
    let input = Incoming::metadata(value)?;
    // A warm receipt usually adds no restriction. Compare the complete merge
    // against one fresh SQL statement before opening a write transaction. This
    // is not a cached authority: callers still apply/check the same mirror and
    // require_current, and any change uses the existing durable writer.
    if let Some(mutation) = unchanged::check(database, &input, expected_revision).await? {
        return Ok(mutation);
    }
    mutate_at(database, input, true, Some(expected_revision)).await
}
pub(in crate::marketplace) async fn catalog_observe(
    database: &Database,
    value: &SecurityObservation,
    expected_revision: u64,
) -> Result<CatalogMutation> {
    mutate_at(
        database,
        Incoming {
            records: value.records().clone(),
            summary: value.authenticated_summary().clone(),
        },
        false,
        Some(expected_revision),
    )
    .await
}
/// A direct preview may advance already-tracked signed authority, never
/// allocate unknown identities or manufacture discovery/adoption evidence.
pub(in crate::marketplace) async fn catalog_observe_publication(
    database: &Database,
    value: &PublicationObservation,
    expected_revision: u64,
) -> Result<CatalogMutation> {
    mutate_at(
        database,
        Incoming::publication(value)?,
        false,
        Some(expected_revision),
    )
    .await
}
/// Only an explicitly accepting authenticated owner may allocate security
/// fences for a new direct reference. This creates no catalog/adoption entry.
pub(in crate::marketplace) async fn catalog_accept_publication(
    database: &Database,
    value: &super::selected_source::AcceptedPublication,
    expected_revision: u64,
) -> Result<CatalogMutation> {
    mutate_authorized(
        database,
        Incoming::publication(value.observation())?,
        true,
        Some(expected_revision),
        Some(value),
    )
    .await
}
impl Incoming {
    fn publication(value: &PublicationObservation) -> Result<Self> {
        let summary = value.summary().map_err(|_| Rejection::Expired)?;
        let publication = value.publication();
        Ok(Incoming {
            records: SecurityRecords {
                schema_version: SCHEMA_VERSION,
                extension_id: summary.extension_id.clone(),
                publisher: publication.publisher.clone(),
                head: Some(publication.head.clone()),
                revocations: vec![],
            },
            summary: SecuritySummary {
                extension_id: summary.extension_id.clone(),
                root_key: publication.publisher.payload.root_key,
                publisher_sequence: summary.publisher_sequence,
                publisher_payload_id: summary.publisher_payload_id,
                head: Some(HeadState {
                    sequence: summary.head_sequence,
                    payload_id: summary.head_payload_id,
                }),
                revocation_ids: vec![],
            },
        })
    }
}
impl Incoming {
    fn metadata(value: &MetadataObservation) -> Result<Self> {
        let d = value.descriptor();
        let records = SecurityRecords {
            schema_version: SCHEMA_VERSION,
            extension_id: d.release.payload.extension_id.clone(),
            publisher: d.publisher.clone(),
            head: Some(d.head.clone()),
            revocations: d.revocations.clone(),
        };
        // Only this private-process receipt can take the already-authenticated
        // path. No paid/signature work is repeated in the media-server process.
        let summary = SecuritySummary {
            extension_id: records.extension_id.clone(),
            root_key: records.publisher.payload.root_key,
            publisher_sequence: records.publisher.payload.sequence,
            publisher_payload_id: Digest::of(canonical::to_vec(&records.publisher.payload)?),
            head: Some(HeadState {
                sequence: d.head.payload.sequence,
                payload_id: Digest::of(canonical::to_vec(&d.head.payload)?),
            }),
            revocation_ids: records
                .revocations
                .iter()
                .map(|r| r.to_bytes().map(Digest::of))
                .collect::<elixir_marketplace::Result<_>>()?,
        };
        Ok(Self { records, summary })
    }
}
fn merge(publisher: &mut PublisherState, extension: &mut ExtensionState, input: &Incoming) {
    let summary = &input.summary;
    if summary.publisher_sequence > publisher.sequence {
        publisher.sequence = summary.publisher_sequence;
        publisher.payload_id = summary.publisher_payload_id;
        publisher.conflicted = false;
    } else if summary.publisher_sequence == publisher.sequence
        && summary.publisher_payload_id != publisher.payload_id
    {
        publisher.conflicted = true;
    }
    // Root withdrawals remain effective even with an old publisher/head, no
    // adoption, or an invalid head. Never evict a known revocation to make room.
    for record in &input.records.revocations {
        for id in &record.payload.release_ids {
            if !extension.revoked_releases.contains(id) {
                if extension.revoked_releases.len() == 128 {
                    extension.saturated = true;
                } else {
                    extension.revoked_releases.insert(*id);
                }
            }
        }
        for key in &record.payload.release_keys {
            if !extension.revoked_keys.contains(key) {
                if extension.revoked_keys.len() == 16 {
                    extension.saturated = true;
                } else {
                    extension.revoked_keys.insert(*key);
                }
            }
        }
    }
    if publisher.conflicted
        || summary.publisher_sequence != publisher.sequence
        || summary.publisher_payload_id != publisher.payload_id
    {
        return;
    }
    if let Some(head) = &summary.head {
        let version = (publisher.sequence, head.sequence);
        match &mut extension.head {
            Some(old) if version < (old.generation, old.sequence) => {}
            Some(old) if version == (old.generation, old.sequence) => {
                if old.payload_id != head.payload_id {
                    old.conflicted = true;
                }
            }
            target => {
                *target = Some(HeadFence {
                    generation: publisher.sequence,
                    sequence: head.sequence,
                    payload_id: head.payload_id,
                    conflicted: false,
                })
            }
        }
    }
}
fn check(
    publisher: &PublisherState,
    extension: &ExtensionState,
    value: &MetadataObservation,
) -> Result<()> {
    let d = value.descriptor();
    check_candidate(
        publisher,
        extension,
        &CandidateFence {
            publisher_sequence: d.publisher.payload.sequence,
            publisher_payload_id: Digest::of(canonical::to_vec(&d.publisher.payload)?),
            head_sequence: d.head.payload.sequence,
            head_payload_id: Digest::of(canonical::to_vec(&d.head.payload)?),
            release_id: Digest::of(canonical::to_vec(&d.release.payload)?),
            release_key: d.release.signer_key,
            head_key: d.head.signer_key,
        },
    )
}
struct CandidateFence {
    publisher_sequence: u64,
    publisher_payload_id: Digest,
    head_sequence: u64,
    head_payload_id: Digest,
    release_id: Digest,
    release_key: PublicKey,
    head_key: PublicKey,
}
fn check_candidate(
    publisher: &PublisherState,
    extension: &ExtensionState,
    candidate: &CandidateFence,
) -> Result<()> {
    if publisher.conflicted
        || extension.saturated
        || extension.head.as_ref().is_some_and(|h| h.conflicted)
    {
        return Err(Rejection::Conflicted.into());
    }
    let head = extension
        .head
        .as_ref()
        .context("marketplace head is unknown")?;
    if publisher.sequence != candidate.publisher_sequence
        || publisher.payload_id != candidate.publisher_payload_id
        || (head.generation, head.sequence)
            != (candidate.publisher_sequence, candidate.head_sequence)
        || head.payload_id != candidate.head_payload_id
    {
        return Err(Rejection::Stale.into());
    }
    if extension.revoked_releases.contains(&candidate.release_id)
        || extension.revoked_keys.contains(&candidate.release_key)
        || extension.revoked_keys.contains(&candidate.head_key)
    {
        return Err(Rejection::Withdrawn.into());
    }
    Ok(())
}

/// May allocate at most one bounded identity using actual paid-gate evidence.
/// Still not public-source/search/install authorization. Caller must run the
/// protected source check and recheck this fence immediately before use.
#[cfg(test)]
pub(in crate::marketplace) async fn admit(
    database: &Database,
    value: &MetadataObservation,
) -> Result<()> {
    if !value.is_current() {
        return Err(Rejection::Expired.into());
    }
    mutate(database, Incoming::metadata(value)?, true).await?;
    require_current(database, value).await
}
/// Local-signature/durable-database test seam, not a native observation factory.
/// Verify actual records and update only already tracked identities.
#[cfg(test)]
pub(in crate::marketplace) async fn fixture_observe_signed(
    database: &Database,
    records: SecurityRecords,
) -> Result<bool> {
    let summary = records.verify(chrono::Utc::now().timestamp() as u64)?;
    mutate(database, Incoming { records, summary }, false).await
}
fn decode<T: serde::de::DeserializeOwned + Serialize>(
    json: &str,
    bytes: i64,
    hash: &str,
    max: usize,
) -> Result<T> {
    if bytes != json.len() as i64 || hash != Digest::of(json.as_bytes()).to_string() {
        bail!("marketplace security row integrity differs");
    }
    Ok(canonical::decode(json.as_bytes(), max)?)
}
fn validate(
    publisher: &PublisherState,
    extension: &ExtensionState,
    root: PublicKey,
    id: &ExtensionId,
) -> Result<()> {
    if publisher.schema_version != SCHEMA_VERSION
        || extension.schema_version != SCHEMA_VERSION
        || publisher.root_key != root
        || extension.root_key != root
        || &extension.extension_id != id
        || !id.belongs_to(&root)
        || publisher.sequence > MAX_REVISION as u64
        || extension.revoked_releases.len() > 128
        || extension.revoked_keys.len() > 16
        || extension
            .head
            .as_ref()
            .is_some_and(|h| h.generation > publisher.sequence || h.sequence > MAX_REVISION as u64)
    {
        bail!("marketplace security state scope or bounds differ");
    }
    Ok(())
}
pub async fn require_current(database: &Database, value: &MetadataObservation) -> Result<()> {
    if !value.is_current() {
        return Err(Rejection::Expired.into());
    }
    let id = &value.descriptor().release.payload.extension_id;
    let root = value.descriptor().publisher.payload.root_key;
    let row=sqlx::query("SELECT p.state_json AS p_json,p.state_bytes AS p_bytes,p.state_sha256 AS p_hash, \
        e.state_json AS e_json,e.state_bytes AS e_bytes,e.state_sha256 AS e_hash,b.blocked \
        FROM marketplace_extension_security e JOIN marketplace_publisher_security p ON p.root_key=e.root_key \
        JOIN marketplace_security_budget b ON b.singleton=1 WHERE e.extension_id=$1 AND e.root_key=$2")
        .bind(id.as_str()).bind(root.to_string()).fetch_optional(&database.pool).await?
        .context("marketplace security authority is missing")?;
    if row.try_get::<i64, _>("blocked")? != 0 {
        bail!("marketplace security storage is blocked");
    }
    let p = decode(
        &row.try_get::<String, _>("p_json")?,
        row.try_get("p_bytes")?,
        &row.try_get::<String, _>("p_hash")?,
        1024,
    )?;
    let e = decode(
        &row.try_get::<String, _>("e_json")?,
        row.try_get("e_bytes")?,
        &row.try_get::<String, _>("e_hash")?,
        16384,
    )?;
    validate(&p, &e, root, id)?;
    if !value.is_current() {
        return Err(Rejection::Expired.into());
    }
    check(&p, &e, value)
}

/// Checks known denials without allocating a row for an unqualified preview.
/// False means untracked, NOT "globally latest" or safe to install.
pub(in crate::marketplace) async fn require_publication_current(
    database: &Database,
    value: &PublicationObservation,
) -> Result<bool> {
    let summary = value.summary().map_err(|_| Rejection::Expired)?;
    let publication = value.publication();
    let id = &summary.extension_id;
    let root = publication.publisher.payload.root_key;
    let row = sqlx::query("SELECT b.revision,b.publishers,b.extensions,b.encoded_bytes,b.blocked, \
        SUBSTR(p.state_json,1,1025) AS p_json,p.state_bytes AS p_bytes,p.state_sha256 AS p_hash, \
        SUBSTR(e.state_json,1,16385) AS e_json,e.state_bytes AS e_bytes,e.state_sha256 AS e_hash, \
        (SELECT COUNT(*) FROM marketplace_publisher_security) AS actual_publishers, \
        (SELECT COUNT(*) FROM marketplace_extension_security) AS actual_extensions, \
        CAST((SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_publisher_security) + \
             (SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_extension_security) AS BIGINT) AS actual_bytes \
        FROM marketplace_security_budget b \
        LEFT JOIN marketplace_publisher_security p ON p.root_key=$1 \
        LEFT JOIN marketplace_extension_security e ON e.extension_id=$2 WHERE b.singleton=1")
        .bind(root.to_string()).bind(id.as_str()).fetch_optional(&database.pool).await?
        .context("marketplace security budget is missing")?;
    let publishers: i64 = row.try_get("publishers")?;
    let extensions: i64 = row.try_get("extensions")?;
    let bytes: i64 = row.try_get("encoded_bytes")?;
    if row.try_get::<i64, _>("blocked")? != 0
        || !(0..=MAX_REVISION).contains(&row.try_get::<i64, _>("revision")?)
        || !(0..=MAX_ROWS).contains(&publishers)
        || !(0..=MAX_ROWS).contains(&extensions)
        || !(0..=MAX_BYTES).contains(&bytes)
        || row.try_get::<i64, _>("actual_publishers")? != publishers
        || row.try_get::<i64, _>("actual_extensions")? != extensions
        || row.try_get::<i64, _>("actual_bytes")? != bytes
    {
        bail!("marketplace security storage is blocked or inconsistent");
    }
    let p: Option<PublisherState> = row
        .try_get::<Option<String>, _>("p_json")?
        .map(|json| {
            decode(
                &json,
                row.try_get("p_bytes")?,
                &row.try_get::<String, _>("p_hash")?,
                1024,
            )
        })
        .transpose()?;
    let e: Option<ExtensionState> = row
        .try_get::<Option<String>, _>("e_json")?
        .map(|json| {
            decode(
                &json,
                row.try_get("e_bytes")?,
                &row.try_get::<String, _>("e_hash")?,
                16384,
            )
        })
        .transpose()?;
    let tracked = match (p, e) {
        (None, None) => false,
        (None, Some(_)) => bail!("marketplace publisher security authority missing"),
        (Some(p), None) => {
            if p.schema_version != SCHEMA_VERSION
                || p.root_key != root
                || p.sequence > MAX_REVISION as u64
            {
                bail!("marketplace publisher security scope or bounds differ");
            }
            if p.conflicted {
                return Err(Rejection::Conflicted.into());
            }
            if summary.publisher_sequence < p.sequence
                || (summary.publisher_sequence == p.sequence
                    && summary.publisher_payload_id != p.payload_id)
            {
                return Err(Rejection::Stale.into());
            }
            false
        }
        (Some(p), Some(e)) => {
            validate(&p, &e, root, id)?;
            check_candidate(
                &p,
                &e,
                &CandidateFence {
                    publisher_sequence: summary.publisher_sequence,
                    publisher_payload_id: summary.publisher_payload_id,
                    head_sequence: summary.head_sequence,
                    head_payload_id: summary.head_payload_id,
                    release_id: summary.release_id,
                    release_key: publication.release.signer_key,
                    head_key: publication.head.signer_key,
                },
            )?;
            true
        }
    };
    if !value.is_current() {
        return Err(Rejection::Expired.into());
    }
    Ok(tracked)
}

#[cfg(test)]
async fn mutate(database: &Database, input: Incoming, allocate: bool) -> Result<bool> {
    Ok(mutate_at(database, input, allocate, None).await?.tracked)
}
async fn mutate_at(
    database: &Database,
    input: Incoming,
    allocate: bool,
    expected_revision: Option<u64>,
) -> Result<CatalogMutation> {
    mutate_authorized(database, input, allocate, expected_revision, None).await
}
async fn mutate_authorized(
    database: &Database,
    input: Incoming,
    allocate: bool,
    expected_revision: Option<u64>,
    accepting: Option<&super::selected_source::AcceptedPublication>,
) -> Result<CatalogMutation> {
    let id = &input.summary.extension_id;
    let root = input.summary.root_key;
    let mut connection = super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    if database.driver == DatabaseDriver::Postgres {
        sqlx::query("SET LOCAL synchronous_commit=on")
            .execute(&mut *tx)
            .await?;
    }
    let accepting_expires = match accepting {
        Some(value) => Some(value.authenticate(&mut tx, database.driver).await?),
        None => None,
    };
    // A single durable cross-process lock, not an in-memory mutex. Missing or
    // corrupted authority never initializes a replacement row.
    if sqlx::query("UPDATE marketplace_security_budget SET revision=revision WHERE singleton=1")
        .execute(&mut *tx)
        .await?
        .rows_affected()
        != 1
    {
        bail!("marketplace security budget is missing");
    }
    let budget=sqlx::query("SELECT revision,publishers,extensions,encoded_bytes,blocked FROM marketplace_security_budget WHERE singleton=1")
        .fetch_one(&mut *tx).await?;
    let revision: i64 = budget.try_get("revision")?;
    let publishers: i64 = budget.try_get("publishers")?;
    let extensions: i64 = budget.try_get("extensions")?;
    let total: i64 = budget.try_get("encoded_bytes")?;
    if budget.try_get::<i64, _>("blocked")? != 0
        || !(0..MAX_REVISION).contains(&revision)
        || !(0..=MAX_ROWS).contains(&publishers)
        || !(0..=MAX_ROWS).contains(&extensions)
        || !(0..=MAX_BYTES).contains(&total)
    {
        bail!("marketplace security budget is blocked or invalid");
    }
    if expected_revision.is_some_and(|expected| expected != revision as u64) {
        bail!("marketplace catalog security revision is out of sync");
    }
    let actual=sqlx::query("SELECT \
        (SELECT COUNT(*) FROM marketplace_publisher_security) AS publishers, \
        (SELECT COUNT(*) FROM marketplace_extension_security) AS extensions, \
        CAST((SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_publisher_security) + \
        (SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_extension_security) AS BIGINT) AS encoded_bytes")
        .fetch_one(&mut *tx).await?;
    if actual.try_get::<i64, _>("publishers")? != publishers
        || actual.try_get::<i64, _>("extensions")? != extensions
        || actual.try_get::<i64, _>("encoded_bytes")? != total
    {
        bail!("marketplace security accounting differs; refusing reset");
    }
    let old_p=sqlx::query("SELECT state_json,state_bytes,state_sha256 FROM marketplace_publisher_security WHERE root_key=$1")
        .bind(root.to_string()).fetch_optional(&mut *tx).await?;
    let old_e=sqlx::query("SELECT state_json,state_bytes,state_sha256 FROM marketplace_extension_security WHERE extension_id=$1")
        .bind(id.as_str()).fetch_optional(&mut *tx).await?;
    if !allocate && old_e.is_none() {
        tx.rollback().await?;
        return Ok(CatalogMutation {
            revision: revision as u64,
            tracked: false,
            batch: None,
        });
    }
    if old_e.is_some() && old_p.is_none() {
        bail!("marketplace publisher authority is missing");
    }
    let mut p = match &old_p {
        Some(row) => decode(
            &row.try_get::<String, _>("state_json")?,
            row.try_get("state_bytes")?,
            &row.try_get::<String, _>("state_sha256")?,
            1024,
        )?,
        None => PublisherState {
            schema_version: SCHEMA_VERSION,
            root_key: root,
            sequence: input.summary.publisher_sequence,
            payload_id: input.summary.publisher_payload_id,
            conflicted: false,
        },
    };
    let mut e = match &old_e {
        Some(row) => decode(
            &row.try_get::<String, _>("state_json")?,
            row.try_get("state_bytes")?,
            &row.try_get::<String, _>("state_sha256")?,
            16384,
        )?,
        None => ExtensionState {
            schema_version: SCHEMA_VERSION,
            extension_id: id.clone(),
            root_key: root,
            head: None,
            revoked_releases: BTreeSet::new(),
            revoked_keys: BTreeSet::new(),
            saturated: false,
        },
    };
    validate(&p, &e, root, id)?;
    merge(&mut p, &mut e, &input);
    validate(&p, &e, root, id)?;
    let p_json = String::from_utf8(canonical::to_vec(&p)?)?;
    let e_json = String::from_utf8(canonical::to_vec(&e)?)?;
    let p_changed = old_p
        .as_ref()
        .is_none_or(|row| row.try_get::<String, _>("state_json").ok().as_ref() != Some(&p_json));
    let e_changed = old_e
        .as_ref()
        .is_none_or(|row| row.try_get::<String, _>("state_json").ok().as_ref() != Some(&e_json));
    if !p_changed && !e_changed {
        tx.rollback().await?;
        return Ok(CatalogMutation {
            revision: revision as u64,
            tracked: true,
            batch: None,
        });
    }
    let next_publishers = publishers + i64::from(old_p.is_none());
    let next_extensions = extensions + i64::from(old_e.is_none());
    let previous_bytes = old_p
        .as_ref()
        .map(|r| r.try_get::<i64, _>("state_bytes"))
        .transpose()?
        .unwrap_or(0)
        + old_e
            .as_ref()
            .map(|r| r.try_get::<i64, _>("state_bytes"))
            .transpose()?
            .unwrap_or(0);
    let next_bytes = total - previous_bytes + p_json.len() as i64 + e_json.len() as i64;
    if next_publishers > MAX_ROWS
        || next_extensions > MAX_ROWS
        || next_bytes > MAX_BYTES
        || p_json.len() > 1024
        || e_json.len() > 16384
    {
        // Reject new allocations, but never silently forget authenticated
        // changes to existing authority. Persist one bounded fail-closed bit.
        if (old_p.is_some() && p_changed) || (old_e.is_some() && e_changed) {
            sqlx::query("UPDATE marketplace_security_budget SET blocked=1,revision=revision+1 WHERE singleton=1")
                .execute(&mut *tx).await?;
            if let (Some(value), Some(expires)) = (accepting, accepting_expires) {
                value.before_commit(expires)?;
            }
            tx.commit().await?;
            bail!("marketplace security storage cannot retain authenticated changes");
        } else {
            tx.rollback().await?;
        }
        return Err(Rejection::Capacity.into());
    }
    if p_changed {
        sqlx::query("INSERT INTO marketplace_publisher_security(root_key,state_json,state_bytes,state_sha256) VALUES($1,$2,$3,$4) \
            ON CONFLICT(root_key) DO UPDATE SET state_json=excluded.state_json,state_bytes=excluded.state_bytes,state_sha256=excluded.state_sha256")
            .bind(root.to_string()).bind(&p_json).bind(p_json.len() as i64).bind(Digest::of(p_json.as_bytes()).to_string())
            .execute(&mut *tx).await?;
    }
    if e_changed {
        sqlx::query("INSERT INTO marketplace_extension_security(extension_id,root_key,state_json,state_bytes,state_sha256) VALUES($1,$2,$3,$4,$5) \
            ON CONFLICT(extension_id) DO UPDATE SET state_json=excluded.state_json,state_bytes=excluded.state_bytes,state_sha256=excluded.state_sha256")
            .bind(id.as_str()).bind(root.to_string()).bind(&e_json).bind(e_json.len() as i64).bind(Digest::of(e_json.as_bytes()).to_string())
            .execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE marketplace_security_budget SET revision=$1,publishers=$2,extensions=$3,encoded_bytes=$4 WHERE singleton=1")
        .bind(revision+1).bind(next_publishers).bind(next_extensions).bind(next_bytes).execute(&mut *tx).await?;
    if let (Some(value), Some(expires)) = (accepting, accepting_expires) {
        value.before_commit(expires)?;
    }
    tx.commit().await?;
    Ok(CatalogMutation {
        revision: (revision + 1) as u64,
        tracked: true,
        batch: Some(export::batch((revision + 1) as u64, &p, &e)?),
    })
}

#[cfg(test)]
mod tests;
