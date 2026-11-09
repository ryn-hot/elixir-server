//! Stable configured-instance ownership of private SDK generation namespaces.
//! Coordinates and row creation are DATA, never execution/retirement authority.
//! Callers hold their home/grant/journal/instance locks and admission budget.
use super::*;

// The outer aggregate is restricted ONLY by stable instance_id. Keep the
// scope predicates inside SUM so SQLite cannot choose the extension/purpose
// index and scan every configured instance of a popular extension per effect.
const OWNERSHIP_SQL: &str = "SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM marketplace_sdk_namespaces n JOIN extension_instances i ON i.instance_id=n.instance_id JOIN marketplace_instance_sdk_state s ON s.instance_id=n.namespace_id WHERE n.namespace_id=$1 AND n.instance_id=$2 AND n.extension_id=$3 AND i.extension_id=$3 AND s.extension_id=$3 AND n.home_id=$4 AND s.home_id=$4 AND n.purpose=$5 AND s.purpose=$5 AND NOT EXISTS(SELECT 1 FROM extension_instances other WHERE other.instance_id=n.namespace_id AND other.instance_id<>n.instance_id)) THEN 1 ELSE 0 END AS BIGINT),COUNT(*),CAST(COALESCE(SUM(CASE WHEN extension_id=$3 AND home_id=$4 AND purpose=$5 THEN 1 ELSE 0 END),0) AS BIGINT) FROM marketplace_sdk_namespaces WHERE instance_id=$2";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::marketplace::grant_store) enum Purpose {
    Foreground,
    Installed,
}
impl Purpose {
    fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Installed => "installed",
        }
    }
    fn limit(self) -> i64 {
        match self {
            Self::Foreground => 1,
            Self::Installed => 3,
        }
    }
}

/// The logical instance and namespace are deliberately separate. Existing
/// encrypted envelopes bind the namespace UUID; they are not re-encrypted by
/// a schema migration or interpreted as operational instance authority.
#[derive(Clone, Copy)]
pub(in crate::marketplace::grant_store) struct Coordinates<'a> {
    pub home: Uuid,
    pub extension: &'a ExtensionId,
    pub instance: Uuid,
    pub namespace: Uuid,
}
impl Coordinates<'_> {
    fn validate(self, purpose: Purpose) -> Result<()> {
        if self.home.is_nil()
            || self.instance.is_nil()
            || self.namespace.is_nil()
            || (purpose == Purpose::Foreground && self.instance != self.namespace)
        {
            bail!("invalid SDK namespace ownership coordinates");
        }
        Ok(())
    }
}

/// Fresh creation only: no upsert, missing-root repair, enrollment or enabled
/// flag. Lock the stable owner to serialize its bounded namespace count.
pub(in crate::marketplace::grant_store) async fn initialize(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    coordinates: Coordinates<'_>,
    purpose: Purpose,
) -> Result<()> {
    coordinates.validate(purpose)?;
    let query = locked(
        "SELECT instance_id FROM extension_instances WHERE instance_id=$1 AND extension_id=$2 AND NOT EXISTS(SELECT 1 FROM extension_instances other WHERE other.instance_id=$3 AND other.instance_id<>$1)",
        driver,
        RowLock::Exclusive,
    );
    let owner: Option<String> = sqlx::query_scalar(&query)
        .bind(coordinates.instance.to_string())
        .bind(coordinates.extension.as_str())
        .bind(coordinates.namespace.to_string())
        .fetch_optional(&mut **tx)
        .await?;
    if owner.is_none() {
        bail!("SDK namespace owner is missing or foreign");
    }
    let (count, matching): (i64, i64) = sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(CASE WHEN purpose=$2 AND home_id=$3 AND extension_id=$4 THEN 1 ELSE 0 END),0) AS BIGINT) FROM marketplace_sdk_namespaces WHERE instance_id=$1")
        .bind(coordinates.instance.to_string()).bind(purpose.as_str()).bind(coordinates.home.to_string()).bind(coordinates.extension.as_str())
        .fetch_one(&mut **tx).await?;
    if count >= purpose.limit() || count != matching {
        bail!("SDK namespace capacity or purpose differs");
    }
    sqlx::query("INSERT INTO marketplace_sdk_namespaces(namespace_id,instance_id,extension_id,home_id,purpose) VALUES($1,$2,$3,$4,$5)")
        .bind(coordinates.namespace.to_string()).bind(coordinates.instance.to_string())
        .bind(coordinates.extension.as_str()).bind(coordinates.home.to_string()).bind(purpose.as_str())
        .execute(&mut **tx).await?;
    sqlx::query("INSERT INTO marketplace_instance_sdk_state(instance_id,extension_id,home_id,revision,key_count,encoded_bytes,purpose) VALUES($1,$2,$3,1,0,0,$4)")
        .bind(coordinates.namespace.to_string()).bind(coordinates.extension.as_str())
        .bind(coordinates.home.to_string()).bind(purpose.as_str()).execute(&mut **tx).await?;
    Ok(())
}

/// Parent-held locks provide synchronization. All namespace/owner/root scope
/// fields must agree; an orphan marker or a root alone is never a valid scope.
pub(in crate::marketplace::grant_store) async fn require(
    tx: &mut Transaction<'_, Any>,
    coordinates: Coordinates<'_>,
    purpose: Purpose,
) -> Result<()> {
    checked(tx, coordinates, purpose).await?;
    Ok(())
}

async fn checked(
    tx: &mut Transaction<'_, Any>,
    coordinates: Coordinates<'_>,
    purpose: Purpose,
) -> Result<i64> {
    coordinates.validate(purpose)?;
    let (valid, count, matching): (i64, i64, i64) = sqlx::query_as(OWNERSHIP_SQL)
        .bind(coordinates.namespace.to_string())
        .bind(coordinates.instance.to_string())
        .bind(coordinates.extension.as_str())
        .bind(coordinates.home.to_string())
        .bind(purpose.as_str())
        .fetch_one(&mut **tx)
        .await?;
    if valid != 1 || !(1..=purpose.limit()).contains(&count) || matching != count {
        bail!("SDK namespace ownership or root binding differs");
    }
    Ok(count)
}

/// The old private coordinator still holds identity-mapped IDs. Until its
/// journal is converted, it MUST NOT adopt a new-style generation by accident.
pub(in crate::marketplace::grant_store) async fn require_identity(
    tx: &mut Transaction<'_, Any>,
    home: Uuid,
    extension: &ExtensionId,
    instance: Uuid,
    purpose: Purpose,
) -> Result<()> {
    let count = checked(
        tx,
        Coordinates {
            home,
            extension,
            instance,
            namespace: instance,
        },
        purpose,
    )
    .await?;
    if count != 1 {
        bail!("identity-mapped coordinator has unrecorded SDK generations");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
