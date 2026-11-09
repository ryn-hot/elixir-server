//! Cross-process serialization for NULL-scope global keys. The old nullable
//! unique constraint cannot arbitrate concurrent creates or global rotations.
//! No schema rewrite, implicit key rotation, duplicate winner or data deletion.
use super::*;
use sqlx::{Any, Transaction};

pub(super) const AMBIGUOUS: &str =
    "secret reference is ambiguous; review conflicting records by their explicit IDs";

async fn begin(pool: &AnyPool, key: &str) -> Result<Transaction<'static, Any>> {
    let mut tx = pool.begin().await?;
    match tx.backend_name() {
        "SQLite" => {
            // First statement is a write, even for an absent key: take the
            // writer position before reading, not a deferred read/write upgrade.
            sqlx::query("UPDATE secrets SET secret_id=secret_id WHERE scope='global' AND scope_id IS NULL AND key=$1")
                .bind(key).execute(&mut *tx).await?;
        }
        "PostgreSQL" => {
            // The read after a waited advisory lock must obtain a fresh
            // snapshot even if the database default is REPEATABLE READ.
            sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                .execute(&mut *tx)
                .await?;
            let mut input = blake3::Hasher::new();
            input.update(b"elixir/global-secret-writer/v1\0");
            input.update(&(key.len() as u64).to_be_bytes());
            input.update(key.as_bytes());
            let hash = input.finalize();
            let lock = i64::from_be_bytes(hash.as_bytes()[..8].try_into()?);
            // A rare hash collision only serializes two different names.
            // Transaction ownership releases the lock on commit/cancellation.
            sqlx::query("SET LOCAL lock_timeout = '5s'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(lock)
                .execute(&mut *tx)
                .await?;
        }
        _ => anyhow::bail!("unsupported secret-store database backend"),
    }
    Ok(tx)
}

/// `replace` is a trusted caller operation, not a request-side authority flag.
/// Preserve the unique record's ID and created_at on replacement. Existing
/// ambiguous records remain byte-for-byte unchanged for explicit owner review.
pub(super) async fn write(pool: &AnyPool, data: &NewSecret, replace: bool) -> Result<bool> {
    anyhow::ensure!(
        data.scope == SecretScope::Global && data.scope_id.is_none(),
        "global secret writer requires an unscoped global key"
    );
    let mut tx = begin(pool, &data.key).await?;
    let rows = sqlx::query("SELECT secret_id FROM secrets WHERE scope='global' AND scope_id IS NULL AND key=$1 LIMIT 2")
        .bind(&data.key).fetch_all(&mut *tx).await?;
    if rows.len() > 1 {
        anyhow::bail!(AMBIGUOUS);
    }
    if let Some(row) = rows.first() {
        if !replace {
            tx.commit().await?;
            return Ok(false);
        }
        let id: String = row.try_get("secret_id")?;
        let changed = sqlx::query("UPDATE secrets SET value_encrypted=$1,rotatable=$2 WHERE secret_id=$3 AND scope='global' AND scope_id IS NULL AND key=$4")
            .bind(&data.value_encrypted).bind(data.rotatable).bind(id).bind(&data.key)
            .execute(&mut *tx).await?;
        anyhow::ensure!(
            changed.rows_affected() == 1,
            "global secret changed during replacement"
        );
    } else {
        sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'global',NULL,$2,$3,$4)")
            .bind(data.secret_id.to_string()).bind(&data.key).bind(&data.value_encrypted).bind(data.rotatable)
            .execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests;
