//! Owner uninstall of a marketplace extension. Every installation journal and
//! its budget, the metadata and choice counters, SDK namespaces with their
//! state and secrets, the extension's instances, providers and secrets, the
//! extension row, the revoked grant and a stack's derived members go in ONE
//! transaction: it either all commits or nothing changes. Nothing native runs
//! here; the caller stops the extension's code first and removes containers
//! and archived releases after the commit.
use super::*;
use crate::marketplace::stack;

/// The extension cannot be uninstalled right now (HTTP 409).
#[derive(Debug)]
pub(in crate::marketplace) struct Busy(pub &'static str);
impl std::fmt::Display for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Busy {}

/// An installed marketplace extension as found before uninstalling it.
pub(in crate::marketplace) struct Inventory {
    pub extension: ExtensionId,
    pub name: String,
    /// Operational instances (also the long-lived executors' keys).
    pub instances: Vec<Uuid>,
    /// `stack.<hash>.`: the derived member rows of this extension's stack,
    /// if it declares one (none match otherwise).
    pub members: String,
    /// A journal is between activation phases.
    pub activating: bool,
}

/// What a committed uninstall removed.
pub(in crate::marketplace) struct Removed {
    pub instances: Vec<Uuid>,
    pub members: stack::RemovedMembers,
    /// Archived releases that only this extension's journals named. Empty
    /// when any remaining journal could not be read (nothing is guessed).
    pub releases: Vec<Digest>,
}

/// The whole installation budget: an uninstall only gives space back, so it
/// must work even when the configured allocation shrank below what is used.
const WHOLE_BUDGET: InstallBudget = InstallBudget {
    entries: 4096,
    bytes: 1024 * 1024 * 1024,
};

fn activation_fenced(record_json: &str) -> bool {
    // Read leniently: an unreadable journal must not hide a fence.
    match serde_json::from_str::<serde_json::Value>(record_json) {
        Ok(value) => value
            .get("activation")
            .is_some_and(|fence| !fence.is_null()),
        Err(_) => record_json.contains("\"activation\""),
    }
}

/// `None` unless `extension` is an installed marketplace extension: an
/// `elx:` row with its installation journal or metadata.
pub(in crate::marketplace) async fn inventory(
    database: &Database,
    extension: &str,
) -> Result<Option<Inventory>> {
    let Ok(extension) = extension.parse::<ExtensionId>() else {
        return Ok(None);
    };
    let row: Option<(String,)> =
        sqlx::query_as("SELECT name FROM extensions WHERE extension_id=$1")
            .bind(extension.as_str())
            .fetch_optional(&database.pool)
            .await?;
    let Some((name,)) = row else { return Ok(None) };
    let journals: Vec<(String,)> =
        sqlx::query_as("SELECT record_json FROM marketplace_installations WHERE extension_id=$1")
            .bind(extension.as_str())
            .fetch_all(&database.pool)
            .await?;
    let metadata: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM marketplace_installation_metadata WHERE extension_id=$1",
    )
    .bind(extension.as_str())
    .fetch_one(&database.pool)
    .await?;
    if journals.is_empty() && metadata == 0 {
        return Ok(None);
    }
    let instances: Vec<(String,)> = sqlx::query_as(
        "SELECT instance_id FROM extension_instances WHERE extension_id=$1 ORDER BY instance_id",
    )
    .bind(extension.as_str())
    .fetch_all(&database.pool)
    .await?;
    Ok(Some(Inventory {
        members: stack::member_prefix(extension.as_str()),
        activating: journals.iter().any(|(json,)| activation_fenced(json)),
        instances: instances
            .into_iter()
            .map(|(id,)| id.trim().parse())
            .collect::<Result<_, _>>()?,
        extension,
        name,
    }))
}

/// Every release a journal names (its generations and a cancelled tombstone).
fn journal_releases(record_json: &str) -> Result<Vec<Digest>> {
    let journal: Journal = canonical::decode(record_json.as_bytes(), record::MAX_RECORD_BYTES)?;
    let mut releases = Vec::new();
    for capsule in journal.slots() {
        releases.push(capsule.facts()?.release().release_id());
    }
    if let Some(cancelled) = &journal.cancelled {
        releases.push(cancelled.release);
    }
    Ok(releases)
}

/// The single uninstall transaction (see the module documentation). The
/// actor is authenticated again inside it, as for every owner decision.
pub(in crate::marketplace) async fn remove(
    database: &Database,
    actor: &Actor,
    inventory: &Inventory,
    fail_before_commit: bool,
) -> Result<Removed> {
    let started = Instant::now();
    let now = Utc::now();
    let id = inventory.extension.as_str();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    // Writer position first (SQLite), then the actual owner session.
    let ledger = store::lock_budget(&mut tx, WHOLE_BUDGET).await?;
    let valid_until = actor
        .authenticate(&mut tx, advancing_now(now, started)?)
        .await?;

    let present: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM extensions WHERE extension_id=$1")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    if present != 1 {
        return Err(Busy("the extension changed while it was being uninstalled").into());
    }
    let journals: Vec<(String, i64)> = sqlx::query_as(
        "SELECT record_json,encoded_bytes FROM marketplace_installations WHERE extension_id=$1",
    )
    .bind(id)
    .fetch_all(&mut *tx)
    .await?;
    if journals.iter().any(|(json, _)| activation_fenced(json)) {
        return Err(Busy("an activation of this extension is in progress").into());
    }
    let instances: Vec<(String,)> = sqlx::query_as(
        "SELECT instance_id FROM extension_instances WHERE extension_id=$1 ORDER BY instance_id",
    )
    .bind(id)
    .fetch_all(&mut *tx)
    .await?;
    let instances: Vec<Uuid> = instances
        .into_iter()
        .map(|(id,)| id.trim().parse())
        .collect::<Result<_, _>>()?;
    if instances != inventory.instances {
        return Err(Busy("the extension's instances changed; try again").into());
    }

    // Archived releases this extension alone names. Any unreadable journal,
    // ours or another extension's, means none are offered for deletion.
    let mut releases = std::collections::BTreeSet::new();
    let mut known = true;
    for (json, _) in &journals {
        match journal_releases(json) {
            Ok(found) => releases.extend(found),
            Err(error) => {
                known = false;
                tracing::warn!(
                    extension = id,
                    "uninstall cannot read a journal's releases: {error:#}"
                );
            }
        }
    }
    let others: Vec<(String,)> =
        sqlx::query_as("SELECT record_json FROM marketplace_installations WHERE extension_id<>$1")
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
    for (json,) in &others {
        match journal_releases(json) {
            Ok(found) => {
                for release in found {
                    releases.remove(&release);
                }
            }
            Err(_) => known = false,
        }
    }

    // Derived lookup hints and publisher-seeding choices.
    sqlx::query("DELETE FROM marketplace_installed_blob_hints WHERE extension_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM marketplace_publisher_seeding WHERE extension_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    // SDK namespaces with their state, values, generation secrets and the
    // instance secrets written under a namespace ID.
    let namespaces: Vec<(String,)> = sqlx::query_as(
        "SELECT namespace_id FROM marketplace_sdk_namespaces WHERE extension_id=$1 \
         UNION SELECT instance_id FROM marketplace_instance_sdk_state WHERE extension_id=$1",
    )
    .bind(id)
    .fetch_all(&mut *tx)
    .await?;
    for (namespace,) in &namespaces {
        for statement in [
            "DELETE FROM marketplace_generation_secrets WHERE namespace_id=$1",
            "DELETE FROM marketplace_instance_sdk_values WHERE instance_id=$1",
            "DELETE FROM marketplace_instance_sdk_state WHERE instance_id=$1",
            "DELETE FROM secrets WHERE scope='instance' AND scope_id=$1",
            "DELETE FROM marketplace_sdk_namespaces WHERE namespace_id=$1",
        ] {
            sqlx::query(statement)
                .bind(namespace)
                .execute(&mut *tx)
                .await?;
        }
    }
    // Journals (their budget is given back below), choice counters, metadata.
    let deleted = sqlx::query("DELETE FROM marketplace_installations WHERE extension_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if deleted != journals.len() as u64 {
        bail!("installation journals changed during uninstall");
    }
    for statement in [
        "DELETE FROM marketplace_instance_choices WHERE extension_id=$1",
        "DELETE FROM marketplace_extension_choices WHERE extension_id=$1",
        "DELETE FROM marketplace_installation_metadata WHERE extension_id=$1",
    ] {
        sqlx::query(statement).bind(id).execute(&mut *tx).await?;
    }
    for instance in &instances {
        stack::delete_instance_rows(&mut tx, *instance).await?;
    }
    let deleted = sqlx::query("DELETE FROM extensions WHERE extension_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if deleted != 1 {
        bail!("extension row changed during uninstall");
    }
    let removed_bytes = journals
        .iter()
        .try_fold(0i64, |sum, (_, bytes)| sum.checked_add(*bytes))
        .context("installation accounting overflow")?;
    store::release(&mut tx, ledger, journals.len() as i64, removed_bytes).await?;
    revoke_grant(&mut tx, actor, id).await?;
    let members = stack::delete_member_rows(&mut tx, &inventory.members).await?;
    if fail_before_commit {
        bail!("uninstall failed before commit (test fault)");
    }
    ensure_before_expiry(valid_until, now, started)?;
    tx.commit().await?;
    Ok(Removed {
        instances,
        members,
        releases: if known {
            releases.into_iter().collect()
        } else {
            Vec::new()
        },
    })
}

/// The same revocation as `grant_store::revoke`, inside the uninstall
/// transaction. The row stays as a tombstone with its revision, so a later
/// reinstall is a fresh owner approval at the next revision and nothing
/// bound to an old revision can come back.
async fn revoke_grant(tx: &mut Transaction<'_, Any>, actor: &Actor, id: &str) -> Result<()> {
    let grant: Option<(String, i64)> = sqlx::query_as(
        "SELECT SUBSTR(home_id,1,37),revision FROM marketplace_extension_grants WHERE extension_id=$1",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((home, revision)) = grant else {
        return Ok(());
    };
    if home != actor.home.to_string() {
        bail!("extension grant belongs to a different home");
    }
    let next = revision
        .checked_add(1)
        .filter(|next| *next <= record::MAX_REVISION)
        .context("grant revision exhausted")?;
    let changed=sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,automatic_updates=FALSE,revision=$1,revocation_revision=CASE WHEN revocation_revision>$1 THEN revocation_revision ELSE $1 END,mutation_id=$2,approved_by_user_id=$3,updated_at=CURRENT_TIMESTAMP WHERE extension_id=$4 AND home_id=$5 AND revision=$6")
        .bind(next).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).bind(id).bind(home).bind(revision)
        .execute(&mut **tx).await?.rows_affected();
    if changed != 1 {
        bail!("extension grant revision changed");
    }
    Ok(())
}
