//! One fresh, bounded database snapshot per displayed page, never cached trust.
//! The same row-integrity, identity, expiry and restriction checks as singles.
use super::*;
use std::collections::BTreeMap;

pub(in crate::marketplace) async fn require_current_batch(
    database: &Database,
    values: &[&MetadataObservation],
) -> Result<Vec<bool>> {
    if values.len() > elixir_marketplace::MAX_SEARCH_RESULTS
        || values
            .iter()
            .map(|value| &value.descriptor().release.payload.extension_id)
            .collect::<BTreeSet<_>>()
            .len()
            != values.len()
    {
        bail!("marketplace current-state page bounds or duplicate identity");
    }
    let mut result = vec![false; values.len()];
    let indices = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.is_current().then_some(index))
        .collect::<Vec<_>>();
    if indices.is_empty() {
        return Ok(result);
    }
    // Parameters bind BOTH identity and root, not a broad publisher scan. Each
    // indexed identity is unique and this query can return at most twenty rows.
    // Bound text in SQL before copying potentially damaged storage into memory.
    let mut statement = String::from(
        "SELECT e.extension_id AS requested_id, \
         SUBSTR(p.state_json,1,1025) AS p_json,p.state_bytes AS p_bytes,p.state_sha256 AS p_hash, \
         SUBSTR(e.state_json,1,16385) AS e_json,e.state_bytes AS e_bytes,e.state_sha256 AS e_hash,b.blocked \
         FROM marketplace_extension_security e \
         JOIN marketplace_publisher_security p ON p.root_key=e.root_key \
         JOIN marketplace_security_budget b ON b.singleton=1 WHERE ",
    );
    for index in 0..indices.len() {
        if index > 0 {
            statement.push_str(" OR ");
        }
        use std::fmt::Write;
        write!(
            statement,
            "(e.extension_id=${} AND e.root_key=${})",
            2 * index + 1,
            2 * index + 2
        )?;
    }
    let mut query = sqlx::query(&statement);
    for &index in &indices {
        let descriptor = values[index].descriptor();
        query = query
            .bind(descriptor.release.payload.extension_id.as_str())
            .bind(descriptor.publisher.payload.root_key.to_string());
    }
    let fetched = query.fetch_all(&database.pool).await?;
    if fetched.len() != indices.len() {
        // Missing/cross-root authority is a storage failure, not a harmless
        // expired listing. Preserve the original caller's fail-stop behavior.
        bail!("marketplace security authority is missing");
    }
    let mut rows = BTreeMap::new();
    for row in fetched {
        let id: String = row.try_get("requested_id")?;
        if rows.insert(id, row).is_some() {
            bail!("marketplace current-state rows repeat an identity");
        }
    }
    for index in indices {
        let row = rows
            .remove(
                values[index]
                    .descriptor()
                    .release
                    .payload
                    .extension_id
                    .as_str(),
            )
            .context("marketplace current-state row scope differs")?;
        match check_current_row(&row, values[index]) {
            Ok(()) => result[index] = true,
            Err(error) if is_expected_rejection(&error) => (),
            Err(error) => return Err(error),
        }
    }
    Ok(result)
}
