//! Latest operational choices, not native/SDK authority. These fixed counters
//! observe real legacy/store/SQL writes; there is no second settings database.
use super::super::sdk::{RowLock, locked};
use super::*;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Snapshot {
    schema_version: u32,
    extension_revision: i64,
    instance_revision: i64,
}
impl Snapshot {
    pub(super) fn revisions(self) -> (i64, i64) {
        (self.extension_revision, self.instance_revision)
    }
    pub(super) fn validate(self) -> Result<()> {
        if self.schema_version != 1
            || !(1..record::MAX_REVISION).contains(&self.extension_revision)
            || !(1..record::MAX_REVISION).contains(&self.instance_revision)
        {
            bail!("extension choice revisions require explicit recovery");
        }
        Ok(())
    }
    pub(super) async fn require(
        self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
        home: Uuid,
        extension: &ExtensionId,
        instance: Uuid,
    ) -> Result<()> {
        self.validate()?;
        if capture(tx, driver, home, extension, instance).await? != self {
            bail!("extension choices changed; this pending installation is superseded");
        }
        Ok(())
    }
}

pub(super) async fn capture(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    home: Uuid,
    extension: &ExtensionId,
    instance: Uuid,
) -> Result<Snapshot> {
    // Always parent extension -> operational instance -> choice counters.
    // Real UPDATEs lock their parent before the trigger updates its counter.
    let sql = locked(
        "SELECT extension_id FROM extensions WHERE extension_id=$1",
        driver,
        RowLock::Exclusive,
    );
    let parent: Option<String> = sqlx::query_scalar(&sql)
        .bind(extension.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    if parent.is_none() {
        bail!("extension choice parent is missing");
    }
    let sql = locked(
        "SELECT instance_id FROM extension_instances WHERE instance_id=$1 AND extension_id=$2",
        driver,
        RowLock::Exclusive,
    );
    let parent: Option<String> = sqlx::query_scalar(&sql)
        .bind(instance.to_string())
        .bind(extension.as_str())
        .fetch_optional(&mut **tx)
        .await?;
    if parent.is_none() {
        bail!("configured choice parent is missing or foreign");
    }
    let sql = locked(
        "SELECT e.revision,i.revision FROM marketplace_extension_choices e JOIN marketplace_instance_choices i ON i.extension_id=e.extension_id WHERE e.extension_id=$1 AND i.instance_id=$2 AND EXISTS(SELECT 1 FROM marketplace_installation_metadata m WHERE m.extension_id=e.extension_id AND m.home_id=$3)",
        driver,
        RowLock::Shared,
    );
    let (extension_revision, instance_revision): (i64, i64) = sqlx::query_as(&sql)
        .bind(extension.as_str())
        .bind(instance.to_string())
        .bind(home.to_string())
        .fetch_optional(&mut **tx)
        .await?
        .context("configured choice revisions are missing or foreign")?;
    let snapshot = Snapshot {
        schema_version: 1,
        extension_revision,
        instance_revision,
    };
    snapshot.validate()?;
    Ok(snapshot)
}
