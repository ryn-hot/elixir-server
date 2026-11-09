//! Read-only no-change optimization, never a security receipt or allocation.
//! Keep the full merge, row integrity and accounting checks. A changed record
//! must use the original transaction/lock/commit/mirror path.
use super::*;

pub(super) async fn check(
    database: &Database,
    input: &Incoming,
    expected_revision: u64,
) -> Result<Option<CatalogMutation>> {
    // One statement observes budget, rows and actual accounting together on
    // both SQLite and PostgreSQL. No prior read, TTL or in-memory positive bit
    // can stand in for this check. Bound text before copying it out of SQL.
    let row = sqlx::query(
        "SELECT b.revision,b.publishers,b.extensions,b.encoded_bytes,b.blocked, \
        SUBSTR(p.state_json,1,1025) AS p_json,p.state_bytes AS p_bytes,p.state_sha256 AS p_hash, \
        SUBSTR(e.state_json,1,16385) AS e_json,e.state_bytes AS e_bytes,e.state_sha256 AS e_hash, \
        (SELECT COUNT(*) FROM marketplace_publisher_security) AS actual_publishers, \
        (SELECT COUNT(*) FROM marketplace_extension_security) AS actual_extensions, \
        CAST((SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_publisher_security) + \
        (SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_extension_security) AS BIGINT) AS actual_bytes \
        FROM marketplace_security_budget b \
        LEFT JOIN marketplace_publisher_security p ON p.root_key=$1 \
        LEFT JOIN marketplace_extension_security e ON e.extension_id=$2 WHERE b.singleton=1",
    )
    .bind(input.summary.root_key.to_string())
    .bind(input.summary.extension_id.as_str())
    .fetch_optional(&database.pool)
    .await?
    .context("marketplace security budget is missing")?;
    let revision: i64 = row.try_get("revision")?;
    let publishers: i64 = row.try_get("publishers")?;
    let extensions: i64 = row.try_get("extensions")?;
    let total: i64 = row.try_get("encoded_bytes")?;
    if row.try_get::<i64, _>("blocked")? != 0
        || !(0..MAX_REVISION).contains(&revision)
        || !(0..=MAX_ROWS).contains(&publishers)
        || !(0..=MAX_ROWS).contains(&extensions)
        || !(0..=MAX_BYTES).contains(&total)
    {
        bail!("marketplace security budget is blocked or invalid");
    }
    if revision as u64 != expected_revision {
        bail!("marketplace catalog security revision is out of sync");
    }
    if row.try_get::<i64, _>("actual_publishers")? != publishers
        || row.try_get::<i64, _>("actual_extensions")? != extensions
        || row.try_get::<i64, _>("actual_bytes")? != total
    {
        bail!("marketplace security accounting differs; refusing reset");
    }
    let old_p: Option<String> = row.try_get("p_json")?;
    let old_e: Option<String> = row.try_get("e_json")?;
    let Some(old_e) = old_e else {
        // Unknown identity: this path must never allocate or approve it.
        return Ok(None);
    };
    let old_p = old_p.context("marketplace publisher authority is missing")?;
    let mut publisher = decode(
        &old_p,
        row.try_get("p_bytes")?,
        &row.try_get::<String, _>("p_hash")?,
        1024,
    )?;
    let mut extension = decode(
        &old_e,
        row.try_get("e_bytes")?,
        &row.try_get::<String, _>("e_hash")?,
        16384,
    )?;
    validate(
        &publisher,
        &extension,
        input.summary.root_key,
        &input.summary.extension_id,
    )?;
    // Compare the COMPLETE merged state, not just the head: a descriptor with
    // the same head can carry newly authenticated withdrawals of other code.
    merge(&mut publisher, &mut extension, input);
    validate(
        &publisher,
        &extension,
        input.summary.root_key,
        &input.summary.extension_id,
    )?;
    if canonical::to_vec(&publisher)? != old_p.as_bytes()
        || canonical::to_vec(&extension)? != old_e.as_bytes()
    {
        return Ok(None);
    }
    // As with the old no-change rollback, this is only data saying no delta
    // was needed. It grants no current/source/display/install authority.
    Ok(Some(CatalogMutation {
        revision: revision as u64,
        tracked: true,
        batch: None,
    }))
}
