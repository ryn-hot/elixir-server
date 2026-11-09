//! Disposable, owner-bounded lookup hints. A row is not an ACTIVE binding,
//! current paid admission, source proof, open file or serving permission.
use super::*;
use sqlx::{Any, Transaction};

pub(in crate::marketplace) type Cursor = (String, String);
pub(in crate::marketplace) struct Target {
    pub extension: ExtensionId,
    pub home: Uuid,
    pub instance: Uuid,
    pub blob: Blob,
}
const FROM: &str = "FROM marketplace_installed_blob_hints h JOIN marketplace_installations m ON m.instance_id=h.instance_id AND m.extension_id=h.extension_id AND m.home_id=h.home_id JOIN extensions e ON e.extension_id=h.extension_id JOIN extension_instances i ON i.instance_id=h.instance_id AND i.extension_id=h.extension_id JOIN marketplace_extension_grants g ON g.extension_id=h.extension_id AND g.home_id=h.home_id WHERE e.enabled=TRUE AND i.enabled=TRUE AND g.enabled=TRUE";

pub(in crate::marketplace::grant_store::installation) async fn write(
    tx: &mut Transaction<'_, Any>,
    journal: &Journal,
) -> Result<()> {
    let Some(instance) = journal.configured_instance else {
        return Ok(());
    };
    sqlx::query("DELETE FROM marketplace_installed_blob_hints WHERE instance_id=$1")
        .bind(instance.to_string())
        .execute(&mut **tx)
        .await?;
    if let Some(active) = journal.active.as_ref() {
        let slots = active.descriptor()?.workspace.descriptors()?;
        if slots.len() > 3 {
            bail!("installed archive slot bound exceeded");
        }
        for slot in slots {
            sqlx::query("INSERT INTO marketplace_installed_blob_hints(instance_id,extension_id,home_id,sha256,size) VALUES($1,$2,$3,$4,$5) ON CONFLICT(instance_id,sha256) DO NOTHING")
                .bind(instance.to_string()).bind(journal.extension.as_str()).bind(journal.home.to_string())
                .bind(slot.blob.sha256.to_string()).bind(slot.blob.size as i64).execute(&mut **tx).await?;
        }
    }
    Ok(())
}
pub(in crate::marketplace) async fn lookup(
    database: &Database,
    digest: Digest,
) -> Result<Vec<Target>> {
    // A bounded alternative set lets disabled/quiescing duplicate instances
    // fail independently. The caller must verify each exact active binding.
    let rows:Vec<(String,String,String,i64)>=sqlx::query_as(&format!("SELECT SUBSTR(h.extension_id,1,129),SUBSTR(h.home_id,1,37),SUBSTR(h.instance_id,1,37),h.size {FROM} AND h.sha256=$1 ORDER BY h.instance_id LIMIT 16"))
        .bind(digest.to_string()).fetch_all(&database.pool).await?;
    rows.into_iter()
        .map(|(extension, home, instance, size)| {
            let blob = Blob {
                sha256: digest,
                size: u64::try_from(size)?,
            };
            blob.validate(elixir_marketplace::MAX_COMPILED_ARTIFACT_BYTES)?;
            Ok(Target {
                extension: extension.parse()?,
                home: home.parse()?,
                instance: instance.parse()?,
                blob,
            })
        })
        .collect()
}
pub(in crate::marketplace) async fn page(
    database: &Database,
    after: Option<Digest>,
) -> Result<Vec<Blob>> {
    let rows:Vec<(String,i64)>=sqlx::query_as(&format!("SELECT h.sha256,MIN(h.size) {FROM} AND h.sha256>$1 GROUP BY h.sha256 ORDER BY h.sha256 LIMIT 16"))
        .bind(after.map(|d|d.to_string()).unwrap_or_default()).fetch_all(&database.pool).await?;
    rows.into_iter()
        .map(|(sha, size)| {
            let blob = Blob {
                sha256: sha.parse()?,
                size: u64::try_from(size)?,
            };
            blob.validate(elixir_marketplace::MAX_COMPILED_ARTIFACT_BYTES)?;
            Ok(blob)
        })
        .collect()
}
pub(in crate::marketplace) async fn backfill(
    database: &Database,
    after: Option<&Cursor>,
    budget: InstallBudget,
) -> Result<Option<Cursor>> {
    let (extension, instance) = after
        .map(|v| (v.0.as_str(), v.1.as_str()))
        .unwrap_or(("", ""));
    let row:Option<(String,String,String)>=sqlx::query_as("SELECT SUBSTR(m.extension_id,1,129),SUBSTR(m.instance_id,1,37),SUBSTR(m.home_id,1,37) FROM marketplace_installations m WHERE m.instance_id IS NOT NULL AND (m.extension_id>$1 OR (m.extension_id=$1 AND m.instance_id>$2)) ORDER BY m.extension_id,m.instance_id LIMIT 1")
        .bind(extension).bind(instance).fetch_optional(&database.pool).await?;
    let Some((extension, instance, home)) = row else {
        return Ok(None);
    };
    let cursor = (extension.clone(), instance.clone());
    let extension: ExtensionId = extension.parse()?;
    let instance: Uuid = instance.parse()?;
    let home: Uuid = home.parse()?;
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    crate::marketplace::preferences::require_home_binding(&mut tx, home, database.driver).await?;
    store::lock_grant_binding(&mut tx, database.driver, home, &extension).await?;
    store::read_budget(&mut tx, database.driver, budget).await?;
    let old =
        store::load_configured_shared(&mut tx, database.driver, &extension, Some(instance)).await?;
    if let Some(old) = old {
        write(&mut tx, &old.journal).await?;
    }
    tx.commit().await?;
    Ok(Some(cursor))
}

#[cfg(test)]
mod tests {
    use super::super::super::{
        standing::tests::local_http::configured,
        tests::{Fixture, budget},
    };
    use super::*;
    #[tokio::test]
    async fn hints_follow_only_active_sources_are_indexed_and_never_authorize_a_body() -> Result<()>
    {
        let mut f = Fixture::new().await?;
        let staged = f
            .stage_configured(
                InstallTarget::New {
                    name: "Pending".into(),
                },
                Uuid::new_v4(),
                0,
                budget(),
            )
            .await?;
        assert!(page(&f.db, None).await?.is_empty());
        let (owner, _) = configured(&f, "Active").await?;
        let (other, _) = configured(&f, "Duplicate").await?;
        let before = page(&f.db, None).await?;
        assert_eq!(before.len(), 2);
        let sha = f.descriptor.release.payload.package.sha256;
        let targets = lookup(&f.db, sha).await?;
        assert_eq!(targets.len(), 2);
        assert!(
            targets
                .iter()
                .all(|target| target.instance != staged.configured_instance().unwrap())
        );
        let plan:Vec<sqlx::any::AnyRow>=sqlx::query("EXPLAIN QUERY PLAN SELECT instance_id FROM marketplace_installed_blob_hints WHERE sha256=$1")
            .bind(sha.to_string()).fetch_all(&f.db.pool).await?;
        assert!(plan.iter().any(|row| {
            row.try_get::<String, _>("detail")
                .is_ok_and(|d| d.contains("marketplace_installed_blob_digest"))
        }));
        assert!(
            lookup(&f.db, Digest::of(b"not installed"))
                .await?
                .is_empty()
        );
        sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1")
            .bind(owner.to_string())
            .execute(&f.db.pool)
            .await?;
        let targets = lookup(&f.db, sha).await?;
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].instance, other);
        // A forged derived hint cannot turn a different blob into installed
        // source authority. The subsequent real binding/descriptor rejects it.
        let forged = Digest::of(b"forged hint");
        sqlx::query("INSERT INTO marketplace_installed_blob_hints VALUES($1,$2,$3,$4,1)")
            .bind(other.to_string())
            .bind(f.descriptor.release.payload.extension_id.as_str())
            .bind(f.actor.home.to_string())
            .bind(forged.to_string())
            .execute(&f.db.pool)
            .await?;
        let (observation, _owner) = super::super::tests::metadata(&f).await?;
        let source =
            SharingSource::capture(&f.db, f.actor.home, other, observation, budget()).await?;
        assert!(
            source
                .descriptor(&lookup(&f.db, forged).await?[0].blob)
                .is_err()
        );
        // Repair is one cursor row at a time and deterministically removes
        // stale hints from verified journals, without advancing their revisions.
        let revisions: Vec<(String, i64)> = sqlx::query_as(
            "SELECT instance_id,revision FROM marketplace_installations ORDER BY instance_id",
        )
        .fetch_all(&f.db.pool)
        .await?;
        sqlx::query("DELETE FROM marketplace_installed_blob_hints")
            .execute(&f.db.pool)
            .await?;
        let mut cursor = None;
        let mut count = 0;
        loop {
            cursor = backfill(&f.db, cursor.as_ref(), budget()).await?;
            if cursor.is_none() {
                break;
            }
            count += 1;
            assert!(count <= 3);
        }
        assert_eq!(count, 3);
        assert_eq!(lookup(&f.db, sha).await?.len(), 1);
        assert_eq!(
            revisions,
            sqlx::query_as::<_, (String, i64)>(
                "SELECT instance_id,revision FROM marketplace_installations ORDER BY instance_id"
            )
            .fetch_all(&f.db.pool)
            .await?
        );
        f.upgrade("1.1.0", None).await?;
        f.stage_configured(
            InstallTarget::Existing { instance: other },
            Uuid::new_v4(),
            2,
            budget(),
        )
        .await?;
        assert!(
            lookup(&f.db, f.descriptor.release.payload.package.sha256)
                .await?
                .is_empty(),
            "pending update is not advertised"
        );
        assert_eq!(
            lookup(&f.db, sha).await?.len(),
            1,
            "previous active bytes remain indexed until actual cutover"
        );
        Ok(())
    }
}
