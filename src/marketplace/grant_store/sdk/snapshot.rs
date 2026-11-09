//! Bounded encrypted namespace copy. These data/helpers grant NO execution,
//! native-stop, migration or activation authority. The installation transaction
//! must hold its actual runtime guard, owner/grant/journal and instance locks.
use super::*;
use elixir_marketplace::{
    canonical,
    identity::{Digest, ExtensionId},
};
use serde::{Deserialize, Serialize};

const BATCH: usize = 16;
const SECRET_LIMIT: usize = 64;
const COPY_REVISION: i64 = 2;

#[cfg(test)]
mod tests;

/// Namespace coordinates/quota only, not a capability.
#[derive(Clone, Copy)]
pub(in crate::marketplace::grant_store) struct Namespace<'a> {
    pub home: Uuid,
    pub extension: &'a ExtensionId,
    pub instance: Uuid,
    pub owner: Uuid,
    pub review: &'a GrantReview,
}
impl<'a> Namespace<'a> {
    fn coordinates(self) -> namespaces::Coordinates<'a> {
        namespaces::Coordinates {
            home: self.home,
            extension: self.extension,
            instance: self.owner,
            namespace: self.instance,
        }
    }
    fn generation(self) -> bool {
        self.owner != self.instance
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::marketplace::grant_store) struct StateSeal {
    pub revision: i64,
    pub count: i64,
    pub bytes: i64,
    digest: Digest,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::marketplace::grant_store) struct SecretSeal {
    count: usize,
    bytes: u64,
    digest: Digest,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::marketplace::grant_store) struct NamespaceSeal {
    state: StateSeal,
    secrets: SecretSeal,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::marketplace::grant_store) struct CopySeal {
    pub source: Option<NamespaceSeal>,
    pub target: NamespaceSeal,
}
impl CopySeal {
    pub(in crate::marketplace::grant_store) fn validate(&self, has_source: bool) -> Result<()> {
        if self.source.is_some() != has_source || self.target.state.revision != COPY_REVISION {
            bail!("invalid state-copy generation seal");
        }
        for seal in self.source.iter().chain(std::iter::once(&self.target)) {
            State {
                revision: seal.state.revision,
                count: seal.state.count,
                bytes: seal.state.bytes,
            }
            .charge()?;
            if seal.secrets.count > SECRET_LIMIT
                || seal.secrets.bytes > SECRET_LIMIT as u64 * ENTRY_BYTES as u64
            {
                bail!("invalid copied-secret seal");
            }
        }
        Ok(())
    }
}
fn collation(driver: DatabaseDriver) -> &'static str {
    match driver {
        DatabaseDriver::Postgres => "COLLATE \"C\"",
        DatabaseDriver::Sqlite => "COLLATE BINARY",
    }
}
fn key_valid(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}
async fn head(tx: &mut Transaction<'_, Any>, scope: Namespace<'_>) -> Result<State> {
    namespaces::require(tx, scope.coordinates(), namespaces::Purpose::Installed).await?;
    if scope.instance.is_nil()
        || scope.home.is_nil()
        || scope.review.scope().extension_id() != scope.extension
    {
        bail!("invalid copy namespace");
    }
    let row=sqlx::query("SELECT SUBSTR(extension_id,1,129) AS extension_id,SUBSTR(home_id,1,37) AS home_id,SUBSTR(purpose,1,11) AS purpose,revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(scope.instance.to_string()).fetch_optional(&mut **tx).await?.context("copy state root missing; no initialization")?;
    if row.try_get::<String, _>("extension_id")? != scope.extension.as_str()
        || row.try_get::<String, _>("home_id")? != scope.home.to_string()
        || row.try_get::<String, _>("purpose")? != "installed"
    {
        bail!("copy state scope differs");
    }
    let head = State {
        revision: row.try_get("revision")?,
        count: row.try_get("key_count")?,
        bytes: row.try_get("encoded_bytes")?,
    };
    let quota = scope
        .review
        .scope()
        .runtime()
        .context("copy runtime scope missing")?
        .state_bytes;
    if head.charge()? as u64 > quota && (quota != 0 || head.count != 0 || head.bytes != 0) {
        bail!("copy state exceeds its reviewed quota");
    }
    let actual:(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(encoded_bytes),0) AS BIGINT),COALESCE(MAX(revision),0) FROM marketplace_instance_sdk_values WHERE instance_id=$1")
        .bind(scope.instance.to_string()).fetch_one(&mut **tx).await?;
    if actual.0 != head.count || actual.1 != head.bytes || actual.2 > head.revision {
        bail!("copy state counters differ");
    }
    Ok(head)
}

/// Each read batch holds at most 16 bounded ciphertexts, not the whole state.
async fn state_seal(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    scope: Namespace<'_>,
    copy: Option<(&SecretsManager, Namespace<'_>)>,
    audit: Option<&SecretsManager>,
) -> Result<StateSeal> {
    let head = head(tx, scope).await?;
    let mut identity = serde_json::json!({"domain":"elixir-marketplace/sdk-state-copy/v1",
        "home":scope.home,"extension":scope.extension,"instance":scope.instance,"revision":head.revision,"count":head.count,"bytes":head.bytes});
    if scope.owner != scope.instance {
        identity["domain"] = serde_json::json!("elixir-marketplace/sdk-state-copy/v2");
        identity["owner_instance"] = serde_json::json!(scope.owner);
    }
    // Keep identity-mapped v1 fingerprints valid without rewriting old
    // ciphertext/journals; distinct generation IDs also bind their owner.
    let mut digest = Digest::of(canonical::to_vec(&identity)?);
    let mut last: Option<String> = None;
    let mut count = 0i64;
    let mut bytes = 0i64;
    loop {
        let cursor = if last.is_some() {
            format!(" AND key {} > $2", collation(driver))
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT SUBSTR(key,1,65) AS key,revision,encoded_bytes,CASE WHEN LENGTH(value_encrypted)<=98304 THEN value_encrypted ELSE NULL END AS value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1{cursor} ORDER BY key {} LIMIT {BATCH}",
            collation(driver)
        );
        let mut query = sqlx::query(&sql).bind(scope.instance.to_string());
        if let Some(last) = &last {
            query = query.bind(last);
        }
        let rows = query.fetch_all(&mut **tx).await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let key: String = row.try_get("key")?;
            let cipher: Option<String> = row.try_get("value_encrypted")?;
            let cipher = cipher.context("copy state entry exceeds bounds")?;
            let revision: i64 = row.try_get("revision")?;
            let encoded: i64 = row.try_get("encoded_bytes")?;
            if !key_valid(&key)
                || !(1..=head.revision).contains(&revision)
                || encoded <= 0
                || encoded > ENTRY_BYTES
                || cipher.len() as i64 != encoded
                || !cipher.is_ascii()
                || !SecretsManager::is_encrypted(&cipher)
            {
                bail!("invalid copy state entry");
            }
            count += 1;
            bytes += encoded;
            if count > 128 || count > head.count || bytes > head.bytes {
                bail!("copy state scan exceeds its sealed counters");
            }
            digest = Digest::of(canonical::to_vec(
                &serde_json::json!({"domain":"elixir-marketplace/sdk-state-copy-entry/v1","previous":digest,"key":key.as_str(),"revision":revision,"cipher":cipher.as_str()}),
            )?);
            let checked = if let Some(manager) = copy.map(|(manager, _)| manager).or(audit) {
                let plain = zeroize::Zeroizing::new(
                    manager
                        .decrypt(&cipher)
                        .context("copy state decryption failed")?,
                );
                let bounded = RuntimeJson::parse(plain.as_bytes())?;
                let old: Envelope = serde_json::from_str(bounded.as_str())
                    .map_err(|_| anyhow::anyhow!("invalid encrypted SDK state envelope"))?;
                old.require_coordinates(scope.home, scope.extension, scope.instance, scope.owner)?;
                if old.key != key
                    || old.revision != revision
                    || RuntimeJson::serialized_len(&old.value)? > 64 * 1024
                {
                    bail!("copy state envelope identity differs");
                }
                Some(old)
            } else {
                None
            };
            if let Some((manager, target)) = copy {
                let old = checked.context("copy envelope audit missing")?;
                let new = Envelope {
                    schema_version: if target.owner == target.instance {
                        1
                    } else {
                        2
                    },
                    owner_instance_id: (target.owner != target.instance).then_some(target.owner),
                    home_id: target.home,
                    extension_id: target.extension.clone(),
                    instance_id: target.instance,
                    key: key.clone(),
                    revision: COPY_REVISION,
                    value: old.value,
                };
                let serialized = zeroize::Zeroizing::new(serde_json::to_string(&new)?);
                let encrypted = manager.encrypt(&serialized)?;
                if encrypted.len() as i64 > ENTRY_BYTES {
                    bail!("copied state entry exceeds bounds");
                }
                sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,$2,$3,$4,$5)")
                    .bind(target.instance.to_string()).bind(&key).bind(COPY_REVISION).bind(&encrypted).bind(encrypted.len() as i64).execute(&mut **tx).await?;
            }
            last = Some(key);
        }
    }
    if count != head.count || bytes != head.bytes {
        bail!("incomplete state-copy scan");
    }
    Ok(StateSeal {
        revision: head.revision,
        count,
        bytes,
        digest,
    })
}

type SecretRow = (String, String, Option<String>, i64);
async fn secrets(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    scope: Namespace<'_>,
) -> Result<Vec<SecretRow>> {
    // One bounded MVCC read captures the entire secret namespace consistently.
    let base = if scope.generation() {
        "SELECT SUBSTR(secret_id,1,37),SUBSTR(key,1,65),CASE WHEN LENGTH(value_encrypted)<=98304 AND encoded_bytes=LENGTH(value_encrypted) THEN value_encrypted ELSE NULL END,CAST(CASE WHEN rotatable THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_generation_secrets WHERE namespace_id=$1"
    } else {
        "SELECT SUBSTR(secret_id,1,37),SUBSTR(key,1,65),CASE WHEN LENGTH(value_encrypted)<=98304 THEN value_encrypted ELSE NULL END,CAST(CASE WHEN rotatable THEN 1 ELSE 0 END AS BIGINT) FROM secrets WHERE scope='instance' AND scope_id=$1"
    };
    let sql = format!(
        "{base} ORDER BY key {} LIMIT {}",
        collation(driver),
        SECRET_LIMIT + 1
    );
    let rows: Vec<SecretRow> = sqlx::query_as(&sql)
        .bind(scope.instance.to_string())
        .fetch_all(&mut **tx)
        .await?;
    if rows.len() > SECRET_LIMIT {
        bail!("copy secret namespace exceeds its key bound");
    }
    for (id, key, cipher, _) in &rows {
        if id.parse::<Uuid>()?.is_nil() || !key_valid(key) {
            bail!("invalid copy secret identity");
        }
        let cipher = cipher.as_ref().context("copy secret exceeds bounds")?;
        if cipher.len() > ENTRY_BYTES as usize
            || !cipher.is_ascii()
            || !SecretsManager::is_encrypted(cipher)
            || scope.generation() != SecretsManager::is_bound(cipher)
        {
            bail!("copy secret is not bounded encrypted data");
        }
    }
    Ok(rows)
}

fn open_secret(
    manager: &SecretsManager,
    scope: Namespace<'_>,
    key: &str,
    cipher: &str,
) -> Result<zeroize::Zeroizing<String>> {
    if scope.generation() {
        return generation_secrets::open(manager, scope.coordinates(), key, cipher);
    }
    let plain = zeroize::Zeroizing::new(
        manager
            .decrypt(cipher)
            .context("copy secret decryption failed")?,
    );
    if plain.len() > generation_secrets::MAX_PLAINTEXT {
        bail!("copy secret plaintext exceeds bounds");
    }
    Ok(plain)
}

async fn discard_secrets(tx: &mut Transaction<'_, Any>, target: Namespace<'_>) -> Result<()> {
    let sql = if target.generation() {
        "DELETE FROM marketplace_generation_secrets WHERE namespace_id=$1"
    } else {
        "DELETE FROM secrets WHERE scope='instance' AND scope_id=$1"
    };
    sqlx::query(sql)
        .bind(target.instance.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}
fn secret_seal(scope: Namespace<'_>, rows: &[SecretRow]) -> Result<SecretSeal> {
    let mut identity = serde_json::json!({"domain":"elixir-marketplace/sdk-secret-copy/v1","home":scope.home,"extension":scope.extension,"instance":scope.instance});
    if scope.owner != scope.instance {
        identity["domain"] = serde_json::json!("elixir-marketplace/sdk-secret-copy/v2");
        identity["owner_instance"] = serde_json::json!(scope.owner);
    }
    let mut digest = Digest::of(canonical::to_vec(&identity)?);
    let mut bytes = 0u64;
    for (id, key, cipher, rotatable) in rows {
        let cipher = cipher
            .as_ref()
            .context("missing copied secret ciphertext")?;
        bytes += cipher.len() as u64;
        digest = Digest::of(canonical::to_vec(
            &serde_json::json!({"domain":"elixir-marketplace/sdk-secret-copy-entry/v1","previous":digest,"id":id,"key":key,"cipher":cipher,"rotatable":rotatable}),
        )?);
    }
    Ok(SecretSeal {
        count: rows.len(),
        bytes,
        digest,
    })
}
pub(in crate::marketplace::grant_store) async fn fingerprint(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    scope: Namespace<'_>,
) -> Result<NamespaceSeal> {
    let state = state_seal(tx, driver, scope, None, None).await?;
    let secrets = secret_seal(scope, &secrets(tx, driver, scope).await?)?;
    Ok(NamespaceSeal { state, secrets })
}

/// Full local data audit for independently authorized generation collection.
/// This seal is NOT native retirement or permission to delete a namespace.
pub(in crate::marketplace::grant_store) async fn audit_namespace(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    manager: &SecretsManager,
    scope: Namespace<'_>,
) -> Result<NamespaceSeal> {
    let state = state_seal(tx, driver, scope, None, Some(manager)).await?;
    let rows = secrets(tx, driver, scope).await?;
    for (_, key, cipher, _) in &rows {
        open_secret(
            manager,
            scope,
            key,
            cipher.as_ref().context("missing generation secret")?,
        )?;
    }
    Ok(NamespaceSeal {
        state,
        secrets: secret_seal(scope, &rows)?,
    })
}
pub(in crate::marketplace::grant_store) async fn copy(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    manager: &SecretsManager,
    from: Option<Namespace<'_>>,
    target: Namespace<'_>,
) -> Result<CopySeal> {
    copy_with_credentials(tx, driver, manager, from, target, None).await
}

/// Copy plus explicit host-supplied encrypted credentials in the SAME owner
/// transaction, before the first immutable target seal. This is data only;
/// callers still require fresh SecretsManage and original quiescence authority.
pub(in crate::marketplace::grant_store) async fn copy_with_credentials(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    manager: &SecretsManager,
    from: Option<Namespace<'_>>,
    target: Namespace<'_>,
    credentials: Option<&[generation_secrets::PreparedChange]>,
) -> Result<CopySeal> {
    if credentials.is_some() && !target.generation() {
        bail!("owner credential setup requires configured generation storage");
    }
    let empty = fingerprint(tx, driver, target).await?;
    if empty.state.revision != 1
        || empty.state.count != 0
        || empty.state.bytes != 0
        || empty.secrets.count != 0
    {
        bail!("copy target is not an untouched namespace");
    }
    let source = if let Some(source) = from {
        if source.home != target.home
            || source.extension != target.extension
            || source.instance == target.instance
            || (target.generation() && source.owner != target.owner)
            || (source.generation() && !target.generation())
        {
            bail!("copy namespaces are not distinct generations of the same owner");
        }
        let state = state_seal(tx, driver, source, Some((manager, target)), None).await?;
        let rows = secrets(tx, driver, source).await?;
        let secret_fingerprint = secret_seal(source, &rows)?;
        for (_, key, cipher, rotatable) in rows {
            if source.review.scope().secret_keys().contains(&key)
                && target.review.scope().secret_keys().contains(&key)
            {
                let plain = open_secret(
                    manager,
                    source,
                    &key,
                    cipher.as_ref().context("missing source secret")?,
                )?;
                let encrypted = if target.generation() {
                    generation_secrets::seal(manager, target.coordinates(), &key, &plain)?
                } else {
                    manager.encrypt(&plain)?
                };
                if encrypted.len() as i64 > ENTRY_BYTES {
                    bail!("copied secret exceeds bounds");
                }
                if target.generation() {
                    sqlx::query("INSERT INTO marketplace_generation_secrets(secret_id,namespace_id,key,value_encrypted,encoded_bytes,rotatable) VALUES($1,$2,$3,$4,$5,$6)")
                        .bind(Uuid::new_v4().to_string()).bind(target.instance.to_string()).bind(key)
                        .bind(&encrypted).bind(encrypted.len() as i64).bind(rotatable==1).execute(&mut **tx).await?;
                } else {
                    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,$3,$4,$5)")
                    .bind(Uuid::new_v4().to_string()).bind(target.instance.to_string()).bind(key).bind(encrypted).bind(rotatable==1).execute(&mut **tx).await?;
                }
            }
        }
        Some(NamespaceSeal {
            state,
            secrets: secret_fingerprint,
        })
    } else {
        None
    };
    let counts:(i64,i64)=sqlx::query_as("SELECT COUNT(*),CAST(COALESCE(SUM(encoded_bytes),0) AS BIGINT) FROM marketplace_instance_sdk_values WHERE instance_id=$1")
        .bind(target.instance.to_string()).fetch_one(&mut **tx).await?;
    if sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=2,key_count=$1,encoded_bytes=$2 WHERE instance_id=$3 AND revision=1 AND key_count=0 AND encoded_bytes=0")
        .bind(counts.0).bind(counts.1).bind(target.instance.to_string()).execute(&mut **tx).await?.rows_affected()!=1 {bail!("copy target changed");}
    if let Some(changes) = credentials {
        generation_secrets::apply_prepared(
            tx,
            manager,
            target.coordinates(),
            target.review,
            changes,
        )
        .await?;
    }
    let result = CopySeal {
        source,
        target: fingerprint(tx, driver, target).await?,
    };
    result.validate(from.is_some())?;
    verify(tx, driver, from, target, &result).await?;
    Ok(result)
}
pub(in crate::marketplace::grant_store) async fn verify(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    from: Option<Namespace<'_>>,
    target: Namespace<'_>,
    seal: &CopySeal,
) -> Result<()> {
    seal.validate(from.is_some())?;
    if fingerprint(tx, driver, target).await? != seal.target {
        bail!("copied candidate state changed; no overwrite or discard");
    }
    if let Some(source) = from
        && Some(fingerprint(tx, driver, source).await?) != seal.source
    {
        bail!("original snapshot state changed; no overwrite or discard");
    }
    Ok(())
}
pub(in crate::marketplace::grant_store) async fn discard(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    from: Option<Namespace<'_>>,
    target: Namespace<'_>,
    seal: &CopySeal,
) -> Result<()> {
    verify(tx, driver, from, target, seal).await?;
    discard_secrets(tx, target).await?;
    sqlx::query("DELETE FROM marketplace_instance_sdk_values WHERE instance_id=$1")
        .bind(target.instance.to_string())
        .execute(&mut **tx)
        .await?;
    if sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=1,key_count=0,encoded_bytes=0 WHERE instance_id=$1 AND revision=2 AND key_count=$2 AND encoded_bytes=$3")
        .bind(target.instance.to_string()).bind(seal.target.state.count).bind(seal.target.state.bytes).execute(&mut **tx).await?.rows_affected()!=1 {bail!("copied candidate root changed");}
    Ok(())
}

/// Per-effect structural restriction while the original owned candidate may
/// mutate its state. NOT a final snapshot/activation check: final commit must
/// recheck the complete original ciphertext seal and audit candidate entries.
/// Avoid rescanning/hashing 8 MiB of old values on every SDK read/write.
pub(in crate::marketplace::grant_store) async fn verify_candidate(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    from: Option<Namespace<'_>>,
    target: Namespace<'_>,
    seal: &CopySeal,
) -> Result<()> {
    seal.validate(from.is_some())?;
    let target_head = head(tx, target).await?;
    if target_head.revision < COPY_REVISION
        || secret_seal(target, &secrets(tx, driver, target).await?)? != seal.target.secrets
    {
        bail!("candidate copy lineage or read-only secrets changed");
    }
    if let Some(source) = from {
        let actual = head(tx, source).await?;
        let expected = &seal.source.as_ref().context("source seal missing")?.state;
        if actual.revision != expected.revision
            || actual.count != expected.count
            || actual.bytes != expected.bytes
        {
            bail!("original state changed during candidate execution");
        }
    }
    Ok(())
}

/// Complete post-retirement data audit. This is integrity evidence only, not
/// native retirement or installation authority. The caller holds actual row
/// locks and its original joined native owner throughout the transaction.
pub(in crate::marketplace::grant_store) async fn audit_migrated(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    manager: &SecretsManager,
    from: Option<Namespace<'_>>,
    target: Namespace<'_>,
    seal: &CopySeal,
) -> Result<NamespaceSeal> {
    verify_candidate(tx, driver, from, target, seal).await?;
    if let Some(source) = from
        && Some(fingerprint(tx, driver, source).await?) != seal.source
    {
        bail!("original ciphertext or secrets changed during migration");
    }
    // Decrypt one value at a time within the existing 16-ciphertext batch.
    let state = state_seal(tx, driver, target, None, Some(manager)).await?;
    let rows = secrets(tx, driver, target).await?;
    let secrets = secret_seal(target, &rows)?;
    if secrets != seal.target.secrets {
        bail!("copied read-only secrets changed");
    }
    for (_, key, cipher, _) in rows {
        open_secret(
            manager,
            target,
            &key,
            cipher.as_ref().context("missing candidate secret")?,
        )?;
    }
    Ok(NamespaceSeal { state, secrets })
}

/// Deletes ONLY a fully audited disabled candidate's local fork. The parent
/// must additionally require original joined retirement and fresh owner intent.
/// Never call this for ACTIVE/previous data or infer retirement from this seal.
pub(in crate::marketplace::grant_store) async fn discard_migrated(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    manager: &SecretsManager,
    from: Option<Namespace<'_>>,
    target: Namespace<'_>,
    seal: &CopySeal,
) -> Result<()> {
    let audited = audit_migrated(tx, driver, manager, from, target, seal).await?;
    discard_secrets(tx, target).await?;
    sqlx::query("DELETE FROM marketplace_instance_sdk_values WHERE instance_id=$1")
        .bind(target.instance.to_string())
        .execute(&mut **tx)
        .await?;
    if sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=1,key_count=0,encoded_bytes=0 WHERE instance_id=$1 AND revision=$2 AND key_count=$3 AND encoded_bytes=$4")
        .bind(target.instance.to_string()).bind(audited.state.revision).bind(audited.state.count).bind(audited.state.bytes)
        .execute(&mut **tx).await?.rows_affected()!=1 { bail!("candidate root changed during discard"); }
    Ok(())
}
