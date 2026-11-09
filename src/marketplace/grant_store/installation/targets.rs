//! Bounded owner-observed target DATA. Nothing here captures a target, renews
//! original choices, starts source work, or creates execution/cleanup proof.
use super::*;
use serde::de::IntoDeserializer;

const PAGE_SIZE: usize = 20;
const MAX_REQUEST_BYTES: usize = 4096;
#[derive(Debug, thiserror::Error)]
#[error("current server owner authorization is required")]
pub(crate) struct TargetAuthorization;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetCursor {
    instance_id: Uuid,
    extension_revision: i64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetRequest {
    schema_version: u32,
    pub(in crate::marketplace) extension_id: ExtensionId,
    #[serde(default)]
    after: Option<TargetCursor>,
}
impl TargetRequest {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let raw = canonical::parse(bytes, MAX_REQUEST_BYTES)?;
        let mut ignored = false;
        let value: Self = serde_ignored::deserialize(raw.into_deserializer(), |_| ignored = true)?;
        if ignored
            || value.schema_version != 1
            || value.after.as_ref().is_some_and(|cursor| {
                cursor.instance_id.is_nil()
                    || !(1..MAX_REVISION).contains(&cursor.extension_revision)
            })
        {
            bail!("invalid installation target request");
        }
        Ok(value)
    }
}

#[derive(Serialize)]
pub(crate) struct NewTarget {
    expected_revision: i64,
    pub(in crate::marketplace) controls: ExpectedControls,
}
#[derive(Serialize)]
pub(crate) struct ExistingTarget {
    instance_id: Uuid,
    name: String,
    enabled: bool,
    expected_revision: i64,
    controls: Option<ExpectedControls>,
    pending: bool,
}
#[derive(Serialize)]
pub(crate) struct TargetSnapshot {
    schema_version: u32,
    extension_id: ExtensionId,
    pub(in crate::marketplace) new_instance: NewTarget,
    instances: Vec<ExistingTarget>,
    next_cursor: Option<TargetCursor>,
    /// Display-only: the public installation/runtime launch gates are separate.
    installation_ready: bool,
    /// Review can be requested, not permission to install or run anything.
    /// Omitted for old/unconfigured read-only servers.
    #[serde(skip_serializing_if = "Option::is_none")]
    review_available: Option<bool>,
}

impl TargetSnapshot {
    pub(crate) fn advertise_review(&mut self, available: bool) {
        self.review_available = available.then_some(true);
    }
}

/// Actor/home locks protect authorization while this bounded page is read.
/// PostgreSQL uses one repeatable snapshot; SQLite takes its writer position
/// before reads (a no-op, never a settings/counter/timestamp change).
pub(in crate::marketplace) async fn snapshot(
    database: &Database,
    principal: &CurrentPrincipal,
    request: TargetRequest,
) -> Result<TargetSnapshot> {
    let actor = Actor::from_principal(principal, false).map_err(|_| TargetAuthorization)?;
    let now = Utc::now();
    let started = Instant::now();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    if database.driver == DatabaseDriver::Postgres {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
    } else {
        sqlx::query("UPDATE marketplace_preferences SET updated_at=updated_at WHERE singleton=1")
            .execute(&mut *tx)
            .await?;
    }
    let expires = actor
        .authenticate(&mut tx, now)
        .await
        .map_err(|_| TargetAuthorization)?;
    crate::marketplace::preferences::require_actor_binding(&mut tx, &actor)
        .await
        .map_err(|_| TargetAuthorization)?;
    let parent = sqlx::query(store::EXTENSION_METADATA)
        .bind(request.extension_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;
    let marker: Option<(String, String)> = sqlx::query_as("SELECT SUBSTR(home_id,1,37),SUBSTR(metadata_sha256,1,72) FROM marketplace_installation_metadata WHERE extension_id=$1")
        .bind(request.extension_id.as_str()).fetch_optional(&mut *tx).await?;
    let revision: Option<i64> = sqlx::query_scalar(
        "SELECT revision FROM marketplace_extension_choices WHERE extension_id=$1",
    )
    .bind(request.extension_id.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    let journals: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installations WHERE extension_id=$1")
            .bind(request.extension_id.as_str())
            .fetch_one(&mut *tx)
            .await?;
    let Some(parent) = parent else {
        if marker.is_some() || revision.is_some() || journals != 0 || request.after.is_some() {
            bail!("target metadata needs explicit recovery");
        }
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        return Ok(TargetSnapshot {
            schema_version: 1,
            extension_id: request.extension_id,
            new_instance: NewTarget {
                expected_revision: 0,
                controls: ExpectedControls::Absent {},
            },
            instances: vec![],
            next_cursor: None,
            installation_ready: false,
            review_available: None,
        });
    };
    let metadata = store::metadata_from_row(&parent)?;
    let extension_revision = revision.context("managed parent choice counter missing")?;
    if marker != Some((actor.home.to_string(), metadata.id()?.to_string()))
        || !(1..MAX_REVISION).contains(&extension_revision)
        || journals == 0
        || journals > 4096
    {
        bail!("target is unmanaged, foreign, changed or needs control recovery");
    }
    store::lock_grant_binding(&mut tx, database.driver, actor.home, &request.extension_id).await?;
    let legacy: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installations WHERE extension_id=$1 AND instance_id IS NULL")
        .bind(request.extension_id.as_str()).fetch_one(&mut *tx).await?;
    if legacy != 0 {
        bail!("legacy targets require explicit conversion");
    }
    let configured: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE extension_id=$1")
            .bind(request.extension_id.as_str())
            .fetch_one(&mut *tx)
            .await?;
    if configured != journals {
        bail!("unmanaged or missing configured targets require recovery");
    }
    let after = if let Some(cursor) = &request.after {
        let present: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM extension_instances WHERE extension_id=$1 AND instance_id=$2",
        )
        .bind(request.extension_id.as_str())
        .bind(cursor.instance_id.to_string())
        .fetch_one(&mut *tx)
        .await?;
        if cursor.extension_revision != extension_revision || present != 1 {
            bail!("target page changed; refresh instead of silently restamping");
        }
        cursor.instance_id.to_string()
    } else {
        String::new()
    };
    let rows = sqlx::query("SELECT SUBSTR(i.instance_id,1,37) AS instance_id,SUBSTR(i.instance_name,1,129) AS name,CAST(CASE WHEN i.enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,c.revision FROM extension_instances i LEFT JOIN marketplace_instance_choices c ON c.instance_id=i.instance_id AND c.extension_id=i.extension_id WHERE i.extension_id=$1 AND i.instance_id>$2 ORDER BY i.instance_id LIMIT 21")
        .bind(request.extension_id.as_str()).bind(after).fetch_all(&mut *tx).await?;
    let more = rows.len() > PAGE_SIZE;
    let mut instances = Vec::with_capacity(PAGE_SIZE);
    for row in rows.iter().take(PAGE_SIZE) {
        let raw: String = row.try_get("instance_id")?;
        let instance_id: Uuid = raw.parse()?;
        let name: String = row.try_get("name")?;
        let instance_revision: i64 = row.try_get("revision")?;
        if instance_id.is_nil()
            || instance_id.to_string() != raw
            || name.is_empty()
            || name.len() > 128
            || name.chars().any(char::is_control)
            || !(1..MAX_REVISION).contains(&instance_revision)
        {
            bail!("target instance needs control recovery");
        }
        // Bounded original journal parsing validates its checksum, row scope,
        // source records and revision. This is still only a UI snapshot.
        let stored = store::load_configured_shared(
            &mut tx,
            database.driver,
            &request.extension_id,
            Some(instance_id),
        )
        .await?
        .context("unmanaged configured target cannot be adopted")?;
        if stored.journal.home != actor.home
            || stored.journal.configured_instance != Some(instance_id)
        {
            bail!("configured target is foreign");
        }
        let pending = stored.journal.pending.is_some() || stored.journal.activation.is_some();
        let controls = (!pending && stored.journal.revision < MAX_REVISION - 1).then_some(
            ExpectedControls::Existing {
                extension_revision,
                instance_revision,
            },
        );
        instances.push(ExistingTarget {
            instance_id,
            name,
            enabled: row.try_get::<i64, _>("enabled")? != 0,
            expected_revision: stored.journal.revision,
            controls,
            pending,
        });
    }
    let next_cursor = if more {
        instances.last().map(|last| TargetCursor {
            instance_id: last.instance_id,
            extension_revision,
        })
    } else {
        None
    };
    ensure_before_expiry(expires, now, started)?;
    tx.commit().await?;
    Ok(TargetSnapshot {
        schema_version: 1,
        extension_id: request.extension_id,
        new_instance: NewTarget {
            expected_revision: 0,
            controls: ExpectedControls::NewInstance { extension_revision },
        },
        instances,
        next_cursor,
        installation_ready: false,
        review_available: None,
    })
}

#[cfg(test)]
mod tests;
