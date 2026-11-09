//! Consistent, bounded exports of private durable fences. This is restriction
//! data, not public signature/source/installation authority. Missing or corrupt
//! rows and accounting are errors; never initialize an empty replacement.
use super::*;
use elixir_marketplace::{
    catalog_fence::{
        ExtensionFence, FENCE_BATCH_ROWS, FenceBatch, HeadFence as ExportHead, PublisherFence,
        baseline_digest,
    },
    worker_control::WorkerControlOperation,
};
use futures_util::TryStreamExt;
use std::collections::BTreeMap;

pub(in crate::marketplace) struct CatalogBaseline {
    revision: u64,
    publishers: Vec<PublisherFence>,
    extensions: Vec<ExtensionFence>,
    digest: Digest,
}
impl CatalogBaseline {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn begin(&self, generation: Digest) -> WorkerControlOperation {
        WorkerControlOperation::CatalogSecurityBegin {
            generation,
            revision: self.revision,
            publishers: self.publishers.len(),
            extensions: self.extensions.len(),
            digest: self.digest,
        }
    }
    /// Publishers first, then extensions, each in ASCII key order. Hashes bind
    /// these original rows, not the worker's monotonic merge with older state.
    pub fn batches(&self) -> impl Iterator<Item = FenceBatch> + '_ {
        self.publishers
            .chunks(FENCE_BATCH_ROWS)
            .map(|rows| FenceBatch {
                schema_version: 1,
                revision: self.revision,
                publishers: rows.to_vec(),
                extensions: vec![],
            })
            .chain(
                self.extensions
                    .chunks(FENCE_BATCH_ROWS)
                    .map(|rows| FenceBatch {
                        schema_version: 1,
                        revision: self.revision,
                        publishers: vec![],
                        extensions: rows.to_vec(),
                    }),
            )
    }
    pub fn finish(&self, generation: Digest) -> WorkerControlOperation {
        WorkerControlOperation::CatalogSecurityFinish {
            generation,
            revision: self.revision,
        }
    }
    pub async fn require_current(&self, database: &Database) -> Result<()> {
        let row = sqlx::query(
            "SELECT revision,blocked FROM marketplace_security_budget WHERE singleton=1",
        )
        .fetch_one(&database.pool)
        .await?;
        if row.try_get::<i64, _>("blocked")? != 0
            || row.try_get::<i64, _>("revision")? != self.revision as i64
        {
            bail!("marketplace catalog baseline changed during replay");
        }
        Ok(())
    }
}
fn publisher(value: &PublisherState) -> PublisherFence {
    PublisherFence {
        schema_version: value.schema_version,
        root_key: value.root_key,
        sequence: value.sequence,
        payload_id: value.payload_id,
        conflicted: value.conflicted,
    }
}
fn extension(value: &ExtensionState) -> ExtensionFence {
    ExtensionFence {
        schema_version: value.schema_version,
        extension_id: value.extension_id.clone(),
        root_key: value.root_key,
        head: value.head.as_ref().map(|h| ExportHead {
            generation: h.generation,
            sequence: h.sequence,
            payload_id: h.payload_id,
            conflicted: h.conflicted,
        }),
        revoked_releases: value.revoked_releases.iter().copied().collect(),
        revoked_keys: value.revoked_keys.iter().copied().collect(),
        saturated: value.saturated,
    }
}
pub(super) fn batch(revision: u64, p: &PublisherState, e: &ExtensionState) -> Result<FenceBatch> {
    let batch = FenceBatch {
        schema_version: 1,
        revision,
        publishers: vec![publisher(p)],
        extensions: vec![extension(e)],
    };
    batch.validate()?;
    Ok(batch)
}

pub(in crate::marketplace) async fn catalog_baseline(
    database: &Database,
) -> Result<CatalogBaseline> {
    let mut connection = super::super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    if database.driver == DatabaseDriver::Postgres {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
    }
    let budget=sqlx::query("SELECT revision,publishers,extensions,encoded_bytes,blocked FROM marketplace_security_budget WHERE singleton=1")
        .fetch_one(&mut *tx).await?;
    let revision = budget.try_get::<i64, _>("revision")?;
    let publishers = budget.try_get::<i64, _>("publishers")?;
    let extensions = budget.try_get::<i64, _>("extensions")?;
    let bytes = budget.try_get::<i64, _>("encoded_bytes")?;
    if budget.try_get::<i64, _>("blocked")? != 0
        || !(0..=MAX_REVISION).contains(&revision)
        || !(0..=MAX_ROWS).contains(&publishers)
        || !(0..=MAX_ROWS).contains(&extensions)
        || !(0..=MAX_BYTES).contains(&bytes)
    {
        bail!("marketplace catalog baseline budget is blocked or invalid");
    }
    let actual=sqlx::query("SELECT \
        (SELECT COUNT(*) FROM marketplace_publisher_security) AS publishers, \
        (SELECT COUNT(*) FROM marketplace_extension_security) AS extensions, \
        CAST((SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_publisher_security) + \
        (SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_extension_security) AS BIGINT) AS encoded_bytes")
        .fetch_one(&mut *tx).await?;
    if actual.try_get::<i64, _>("publishers")? != publishers
        || actual.try_get::<i64, _>("extensions")? != extensions
        || actual.try_get::<i64, _>("encoded_bytes")? != bytes
    {
        bail!("marketplace catalog baseline accounting differs");
    }
    // Existing database constraints bound these reads. Substrings and LIMIT
    // also bound allocation if a damaged/foreign schema bypassed constraints.
    let mut state = BTreeMap::new();
    let mut counted = 0i64;
    {
        let mut p_rows=sqlx::query("SELECT SUBSTR(root_key,1,44) AS root_key,SUBSTR(state_json,1,1025) AS state_json,state_bytes,SUBSTR(state_sha256,1,72) AS state_sha256 FROM marketplace_publisher_security LIMIT 10001")
        .fetch(&mut *tx);
        while let Some(row) = p_rows.try_next().await? {
            let key: PublicKey = row.try_get::<String, _>("root_key")?.parse()?;
            let json = row.try_get::<String, _>("state_json")?;
            let value: PublisherState = decode(
                &json,
                row.try_get("state_bytes")?,
                &row.try_get::<String, _>("state_sha256")?,
                1024,
            )?;
            if value.root_key != key
                || value.schema_version != 1
                || value.sequence > MAX_REVISION as u64
            {
                bail!("marketplace catalog publisher scope differs");
            }
            counted += json.len() as i64;
            if counted > bytes || state.insert(key, value).is_some() {
                bail!("marketplace catalog publisher accounting differs");
            }
        }
    }
    if state.len() != publishers as usize {
        bail!("marketplace catalog publisher count differs");
    }
    let mut exported = vec![];
    {
        let mut e_rows=sqlx::query("SELECT SUBSTR(extension_id,1,257) AS extension_id,SUBSTR(root_key,1,44) AS root_key, \
        SUBSTR(state_json,1,16385) AS state_json,state_bytes,SUBSTR(state_sha256,1,72) AS state_sha256 FROM marketplace_extension_security LIMIT 10001")
        .fetch(&mut *tx);
        while let Some(row) = e_rows.try_next().await? {
            let id: ExtensionId = row.try_get::<String, _>("extension_id")?.parse()?;
            let root: PublicKey = row.try_get::<String, _>("root_key")?.parse()?;
            let json = row.try_get::<String, _>("state_json")?;
            let value: ExtensionState = decode(
                &json,
                row.try_get("state_bytes")?,
                &row.try_get::<String, _>("state_sha256")?,
                16384,
            )?;
            let parent = state
                .get(&root)
                .context("marketplace catalog publisher is missing")?;
            validate(parent, &value, root, &id)?;
            counted += json.len() as i64;
            if counted > bytes {
                bail!("marketplace catalog extension accounting differs");
            }
            exported.push(extension(&value));
        }
    }
    if exported.len() != extensions as usize || counted != bytes {
        bail!("marketplace catalog baseline count or bytes differ");
    }
    tx.rollback().await?;
    let mut publishers = state.values().map(publisher).collect::<Vec<_>>();
    publishers.sort_by_cached_key(|p| p.root_key.to_string());
    exported.sort_by(|a, b| a.extension_id.as_str().cmp(b.extension_id.as_str()));
    let digest = baseline_digest(&publishers, &exported)?;
    Ok(CatalogBaseline {
        revision: revision as u64,
        publishers,
        extensions: exported,
        digest,
    })
}
