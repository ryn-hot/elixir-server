//! Namespace-owned secret DATA, never grant, native or activation authority.
//! Native callers derive these coordinates from their original held binding;
//! no SDK request can choose an instance, namespace or secret backend.
use super::*;
use elixir_marketplace::canonical;
use namespaces::Coordinates;

pub(in crate::marketplace::grant_store) const MAX_PLAINTEXT: usize = 64 * 1024;
pub(in crate::marketplace::grant_store) const MAX_SETUP_BYTES: usize =
    elixir_marketplace::source_execution::MAX_ARGUMENT_BYTES;

/// Encrypted host-input DATA only. Never accepted as an SDK operation or as
/// source/grant/native authority; the owner copy transaction supplies authority.
pub(in crate::marketplace::grant_store) enum PreparedChange {
    Set {
        key: String,
        cipher: String,
        rotatable: bool,
    },
    Remove {
        key: String,
    },
}
impl PreparedChange {
    fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Remove { key } => key,
        }
    }
}

/// Only during the original disabled-copy transaction, before its first seal
/// is saved. No active/previous/legacy setter, repair or plaintext fallback.
pub(super) async fn apply_prepared(
    tx: &mut Transaction<'_, Any>,
    manager: &SecretsManager,
    scope: Coordinates<'_>,
    review: &GrantReview,
    changes: &[PreparedChange],
) -> Result<()> {
    if changes.is_empty() || changes.len() > 64 || review.scope().extension_id() != scope.extension
    {
        bail!("invalid owner credential change set");
    }
    namespaces::require(tx, scope, namespaces::Purpose::Installed).await?;
    let mut seen = std::collections::BTreeSet::new();
    let mut bytes = 0usize;
    for change in changes {
        let key = change.key();
        associated(scope, key)?;
        if !review
            .scope()
            .secret_keys()
            .iter()
            .any(|declared| declared == key)
            || !seen.insert(key)
        {
            bail!("owner credential key is undeclared or repeated");
        }
        bytes = bytes
            .checked_add(key.len())
            .context("owner credential byte overflow")?;
        if let PreparedChange::Set { cipher, .. } = change {
            bytes = bytes
                .checked_add(cipher.len())
                .context("owner credential byte overflow")?;
            if bytes > MAX_SETUP_BYTES {
                bail!("owner credential setup exceeds its byte bound");
            }
            // Validate every encrypted value before any credential write.
            open(manager, scope, key, cipher)?;
        }
    }
    if bytes > MAX_SETUP_BYTES {
        bail!("owner credential setup exceeds its byte bound");
    }
    for change in changes {
        match change {
            PreparedChange::Set {
                key,
                cipher,
                rotatable,
            } => {
                sqlx::query("INSERT INTO marketplace_generation_secrets(namespace_id,key,secret_id,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(namespace_id,key) DO UPDATE SET value_encrypted=excluded.value_encrypted,encoded_bytes=excluded.encoded_bytes,rotatable=excluded.rotatable")
                    .bind(scope.namespace.to_string()).bind(key).bind(Uuid::new_v4().to_string())
                    .bind(cipher).bind(cipher.len() as i64).bind(*rotatable).execute(&mut **tx).await?;
            }
            PreparedChange::Remove { key } => {
                sqlx::query(
                    "DELETE FROM marketplace_generation_secrets WHERE namespace_id=$1 AND key=$2",
                )
                .bind(scope.namespace.to_string())
                .bind(key)
                .execute(&mut **tx)
                .await?;
            }
        }
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1",
    )
    .bind(scope.namespace.to_string())
    .fetch_one(&mut **tx)
    .await?;
    if count > 64 {
        bail!("owner credential namespace exceeds its key bound");
    }
    Ok(())
}

fn associated(scope: Coordinates<'_>, key: &str) -> Result<Vec<u8>> {
    if scope.home.is_nil()
        || scope.instance.is_nil()
        || scope.namespace.is_nil()
        || scope.instance == scope.namespace
        || !valid_key(key)
    {
        bail!("invalid generation secret coordinates");
    }
    let bytes = canonical::to_vec(&serde_json::json!({
        "domain":"elixir-marketplace/generation-secret/v1", "home":scope.home,
        "extension":scope.extension, "instance":scope.instance,
        "namespace":scope.namespace, "key":key,
    }))?;
    if bytes.len() > 1024 {
        bail!("generation secret context exceeds bounds");
    }
    Ok(bytes)
}
fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

pub(in crate::marketplace::grant_store) fn seal(
    manager: &SecretsManager,
    scope: Coordinates<'_>,
    key: &str,
    value: &str,
) -> Result<String> {
    if value.len() > MAX_PLAINTEXT {
        bail!("generation secret exceeds plaintext bounds");
    }
    let cipher = manager.encrypt_bound(value, &associated(scope, key)?)?;
    if cipher.len() > ENTRY_BYTES as usize {
        bail!("generation secret exceeds ciphertext bounds");
    }
    Ok(cipher)
}

pub(super) fn open(
    manager: &SecretsManager,
    scope: Coordinates<'_>,
    key: &str,
    cipher: &str,
) -> Result<zeroize::Zeroizing<String>> {
    if cipher.len() > ENTRY_BYTES as usize
        || !cipher.is_ascii()
        || !SecretsManager::is_bound(cipher)
    {
        bail!("bounded generation ciphertext required");
    }
    let value = zeroize::Zeroizing::new(
        manager
            .decrypt_bound(cipher, &associated(scope, key)?)
            .context("generation secret context or ciphertext differs")?,
    );
    if value.len() > MAX_PLAINTEXT {
        bail!("generation secret exceeds plaintext bounds");
    }
    Ok(value)
}

/// Read only from this namespace's private store; never a legacy fallback.
/// The outer controller retains all authority/owner locks in this transaction.
pub(super) async fn read(
    tx: &mut Transaction<'_, Any>,
    manager: &SecretsManager,
    scope: Coordinates<'_>,
    key: &str,
) -> Result<Option<zeroize::Zeroizing<String>>> {
    associated(scope, key)?;
    namespaces::require(tx, scope, namespaces::Purpose::Installed).await?;
    let row=sqlx::query("SELECT CASE WHEN LENGTH(value_encrypted)<=98304 THEN value_encrypted ELSE NULL END AS value_encrypted,encoded_bytes FROM marketplace_generation_secrets WHERE namespace_id=$1 AND key=$2")
        .bind(scope.namespace.to_string()).bind(key).fetch_optional(&mut **tx).await?;
    row.map(|row| -> Result<_> {
        let cipher: Option<String> = row.try_get("value_encrypted")?;
        let cipher = cipher.context("generation secret exceeds bounds")?;
        if cipher.len() as i64 != row.try_get::<i64, _>("encoded_bytes")? {
            bail!("generation secret accounting differs");
        }
        open(manager, scope, key, &cipher)
    })
    .transpose()
}

#[cfg(test)]
mod tests;
