//! Read-only no-change optimization, never a security receipt or allocation.
//! Keep the full merge, row integrity and accounting checks. A changed record
//! must use the original transaction/lock/commit/mirror path.
use super::*;

const COLUMNS: &str = "b.revision,b.publishers,b.extensions,b.encoded_bytes,b.blocked, \
    SUBSTR(p.state_json,1,1025) AS p_json,p.state_bytes AS p_bytes,p.state_sha256 AS p_hash, \
    SUBSTR(e.state_json,1,16385) AS e_json,e.state_bytes AS e_bytes,e.state_sha256 AS e_hash, \
    (SELECT COUNT(*) FROM marketplace_publisher_security) AS actual_publishers, \
    (SELECT COUNT(*) FROM marketplace_extension_security) AS actual_extensions, \
    CAST((SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_publisher_security) + \
    (SELECT COALESCE(SUM(state_bytes),0) FROM marketplace_extension_security) AS BIGINT) AS actual_bytes";

pub(super) async fn check(
    database: &Database,
    input: &Incoming,
    expected_revision: u64,
) -> Result<Option<CatalogMutation>> {
    // One statement observes budget, rows and actual accounting together on
    // both SQLite and PostgreSQL. No prior read, TTL or in-memory positive bit
    // can stand in for this check. Bound text before copying it out of SQL.
    let sql = format!(
        "SELECT {COLUMNS} FROM marketplace_security_budget b \
        LEFT JOIN marketplace_publisher_security p ON p.root_key=$1 \
        LEFT JOIN marketplace_extension_security e ON e.extension_id=$2 WHERE b.singleton=1"
    );
    let row = sqlx::query(&sql)
        .bind(input.summary.root_key.to_string())
        .bind(input.summary.extension_id.as_str())
        .fetch_optional(&database.pool)
        .await?
        .context("marketplace security budget is missing")?;
    check_budget(&row, expected_revision)?;
    Ok(merge_is_unchanged(&row, input)?.then_some(CatalogMutation {
        revision: expected_revision,
        tracked: true,
        batch: None,
    }))
}

pub(super) async fn check_page(
    database: &Database,
    inputs: &[Incoming],
    expected_revision: u64,
) -> Result<Vec<bool>> {
    if inputs.len() > elixir_marketplace::MAX_SEARCH_RESULTS
        || inputs
            .iter()
            .map(|v| &v.summary.extension_id)
            .collect::<BTreeSet<_>>()
            .len()
            != inputs.len()
    {
        bail!("warm security page bounds or duplicate identity");
    }
    if inputs.is_empty() {
        return Ok(vec![]);
    }
    use std::fmt::Write;
    let mut sql = String::from("WITH requested(position,root_key,extension_id) AS (VALUES ");
    for position in 0..inputs.len() {
        if position > 0 {
            sql.push(',');
        }
        write!(
            sql,
            "({position},CAST(${} AS TEXT),CAST(${} AS TEXT))",
            2 * position + 1,
            2 * position + 2
        )?;
    }
    // The uncorrelated accounting aggregates execute once in the statement.
    // A left join retains a slot for a cold identity, which cannot be warmed.
    write!(
        sql,
        ") SELECT CAST(requested.position AS BIGINT) AS position,{COLUMNS} \
        FROM requested CROSS JOIN marketplace_security_budget b \
        LEFT JOIN marketplace_publisher_security p ON p.root_key=requested.root_key \
        LEFT JOIN marketplace_extension_security e ON e.extension_id=requested.extension_id \
        WHERE b.singleton=1 ORDER BY requested.position"
    )?;
    let mut query = sqlx::query(&sql);
    for input in inputs {
        query = query
            .bind(input.summary.root_key.to_string())
            .bind(input.summary.extension_id.as_str());
    }
    let rows = query.fetch_all(&database.pool).await?;
    if rows.len() != inputs.len() {
        bail!("marketplace security budget or warm page rows are missing");
    }
    check_budget(&rows[0], expected_revision)?;
    rows.iter()
        .zip(inputs)
        .enumerate()
        .map(|(position, (row, input))| {
            if row.try_get::<i64, _>("position")? != position as i64 {
                bail!("marketplace warm security page row scope differs");
            }
            merge_is_unchanged(row, input)
        })
        .collect()
}

fn check_budget(row: &sqlx::any::AnyRow, expected_revision: u64) -> Result<()> {
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
    Ok(())
}

fn merge_is_unchanged(row: &sqlx::any::AnyRow, input: &Incoming) -> Result<bool> {
    let old_p: Option<String> = row.try_get("p_json")?;
    let old_e: Option<String> = row.try_get("e_json")?;
    let Some(old_e) = old_e else {
        // Unknown identity: this path must never allocate or approve it.
        return Ok(false);
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
    Ok(canonical::to_vec(&publisher)? == old_p.as_bytes()
        && canonical::to_vec(&extension)? == old_e.as_bytes())
}
