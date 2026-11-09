//! Restriction checks for a previously enrolled release. This does NOT enroll
//! code or prove an installation; the caller must bind the same historical
//! authority to its durable source/grant/instance receipt and rechecked bytes.
use super::*;
use elixir_marketplace::retained_authority::VerifiedRetainedAuthority;
use sqlx::{Any, Transaction};

/// A point-in-time read only, not an execution capability. Effects must use
/// require_in_transaction in the SAME transaction as their enrollment/grant/
/// instance check and effect. No receipt TTL, issuer call or network fallback.
pub(in crate::marketplace) async fn require_retained(
    database: &Database,
    authority: &VerifiedRetainedAuthority,
) -> Result<()> {
    let mut transaction = database.pool.begin().await?;
    let result = require_in_transaction(&mut transaction, database.driver, authority).await;
    transaction.rollback().await?;
    result
}

pub(in crate::marketplace) async fn require_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    authority: &VerifiedRetainedAuthority,
) -> Result<()> {
    let (publisher, extension) = lock_state(
        transaction,
        driver,
        authority.root_key(),
        authority.extension_id(),
    )
    .await?;
    check(&publisher, &extension, authority)
}

/// New installations use the STRICT current candidate fence, never the
/// historical allowance above. This shares identical transaction lock order.
pub(in crate::marketplace) async fn require_current_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    metadata: &MetadataObservation,
) -> Result<()> {
    if !metadata.is_current() {
        return Err(Rejection::Expired.into());
    }
    metadata.support()?;
    let d = metadata.descriptor();
    let (publisher, extension) = lock_state(
        transaction,
        driver,
        d.publisher.payload.root_key,
        &d.release.payload.extension_id,
    )
    .await?;
    super::check(&publisher, &extension, metadata)?;
    if !metadata.is_current() {
        return Err(Rejection::Expired.into());
    }
    Ok(())
}

/// The same STRICT candidate fence as discovery, without a fabricated paid
/// proof. Only SelectedSource's accepted-owner capability calls this path.
pub(in crate::marketplace) async fn require_publication_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    value: &PublicationObservation,
) -> Result<()> {
    let summary = value.summary()?;
    let publication = value.publication();
    let (publisher, extension) = lock_state(
        transaction,
        driver,
        publication.publisher.payload.root_key,
        &summary.extension_id,
    )
    .await?;
    super::check_candidate(
        &publisher,
        &extension,
        &super::CandidateFence {
            publisher_sequence: summary.publisher_sequence,
            publisher_payload_id: summary.publisher_payload_id,
            head_sequence: summary.head_sequence,
            head_payload_id: summary.head_payload_id,
            release_id: summary.release_id,
            release_key: publication.release.signer_key,
            head_key: publication.head.signer_key,
        },
    )?;
    value.summary()?;
    Ok(())
}

async fn lock_state(
    transaction: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    root: PublicKey,
    id: &ExtensionId,
) -> Result<(PublisherState, ExtensionState)> {
    // PostgreSQL shared locks coexist across independent instances, but block
    // the security writer until the effect commits/rolls back. SQLite callers
    // performing effects already hold their write-first SDK transaction; a
    // stale read-only snapshot cannot later upgrade past a committed writer.
    // Always take the budget first, matching the security writer's order.
    // A joint FOR SHARE query does not guarantee row-lock acquisition order.
    let budget_query = match driver {
        DatabaseDriver::Postgres => {
            "SELECT revision,publishers,extensions,encoded_bytes,blocked FROM marketplace_security_budget WHERE singleton=1 FOR SHARE"
        }
        DatabaseDriver::Sqlite => {
            "SELECT revision,publishers,extensions,encoded_bytes,blocked FROM marketplace_security_budget WHERE singleton=1"
        }
    };
    let budget = sqlx::query(budget_query)
        .fetch_optional(&mut **transaction)
        .await?
        .context("retained marketplace security budget is missing")?;
    if budget.try_get::<i64, _>("blocked")? != 0
        || !(0..=MAX_REVISION).contains(&budget.try_get::<i64, _>("revision")?)
        || !(1..=MAX_ROWS).contains(&budget.try_get::<i64, _>("publishers")?)
        || !(1..=MAX_ROWS).contains(&budget.try_get::<i64, _>("extensions")?)
        || !(1..=MAX_BYTES).contains(&budget.try_get::<i64, _>("encoded_bytes")?)
    {
        bail!("retained marketplace security storage is blocked or inconsistent");
    }
    let base = "SELECT SUBSTR(p.state_json,1,1025) AS p_json,p.state_bytes AS p_bytes,p.state_sha256 AS p_hash, \
        SUBSTR(e.state_json,1,16385) AS e_json,e.state_bytes AS e_bytes,e.state_sha256 AS e_hash \
        FROM marketplace_extension_security e JOIN marketplace_publisher_security p ON p.root_key=e.root_key \
        WHERE e.extension_id=$1 AND e.root_key=$2";
    let query = match driver {
        DatabaseDriver::Postgres => format!("{base} FOR SHARE OF p,e"),
        DatabaseDriver::Sqlite => base.to_owned(),
    };
    let row = sqlx::query(&query)
        .bind(id.as_str())
        .bind(root.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .context("retained marketplace security authority is missing")?;
    let known_bytes = row
        .try_get::<i64, _>("p_bytes")?
        .checked_add(row.try_get("e_bytes")?)
        .context("retained marketplace security accounting overflow")?;
    if known_bytes <= 0 || known_bytes > budget.try_get::<i64, _>("encoded_bytes")? {
        bail!("retained marketplace security accounting differs");
    }
    let publisher = decode(
        &row.try_get::<String, _>("p_json")?,
        row.try_get("p_bytes")?,
        &row.try_get::<String, _>("p_hash")?,
        1024,
    )?;
    let extension = decode(
        &row.try_get::<String, _>("e_json")?,
        row.try_get("e_bytes")?,
        &row.try_get::<String, _>("e_hash")?,
        16384,
    )?;
    validate(&publisher, &extension, root, id)?;
    Ok((publisher, extension))
}

fn check(
    publisher: &PublisherState,
    extension: &ExtensionState,
    authority: &VerifiedRetainedAuthority,
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
        .context("retained marketplace head is unknown")?;
    let original = (authority.publisher_sequence(), authority.head_sequence());
    let retained = (head.generation, head.sequence);
    if publisher.sequence < authority.publisher_sequence() || retained < original {
        return Err(Rejection::Stale.into());
    }
    if (publisher.sequence == authority.publisher_sequence()
        && publisher.payload_id != authority.publisher_payload_id())
        || (retained == original && head.payload_id != authority.head_payload_id())
    {
        return Err(Rejection::Conflicted.into());
    }
    if extension
        .revoked_releases
        .contains(&authority.release().release_id())
        || extension
            .revoked_keys
            .contains(&authority.release().signer_key())
        || extension.revoked_keys.contains(&authority.head_key())
    {
        return Err(Rejection::Withdrawn.into());
    }
    // New heads and ordinary key rotation do not erase previously enrolled
    // code. Only an actual installed-source receipt may use this distinction;
    // a new installation must still pass the STRICT current-candidate fence.
    Ok(())
}
