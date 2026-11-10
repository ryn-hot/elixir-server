//! Read-only work avoidance, never authentication or permission. A changed
//! input still needs the private native verifier before the normal fence merge.
use super::*;

pub(in crate::marketplace) async fn observation_needed(
    database: &Database,
    records: &SecurityRecords,
) -> Result<bool> {
    records.validate_shape()?;
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM marketplace_security_budget WHERE singleton=1")
            .fetch_one(&database.pool)
            .await?;
    if !(0..MAX_REVISION).contains(&revision) {
        bail!("marketplace security revision is blocked or invalid");
    }
    // These are explicitly UNAUTHENTICATED hashes used only to decide whether
    // work might add a restriction. No mutation or reusable receipt is exposed.
    let summary = SecuritySummary {
        extension_id: records.extension_id.clone(),
        root_key: records.publisher.payload.root_key,
        publisher_sequence: records.publisher.payload.sequence,
        publisher_payload_id: Digest::of(canonical::to_vec(&records.publisher.payload)?),
        head: records
            .head
            .as_ref()
            .map(|head| -> Result<HeadState> {
                Ok(HeadState {
                    sequence: head.payload.sequence,
                    payload_id: Digest::of(canonical::to_vec(&head.payload)?),
                })
            })
            .transpose()?,
        revocation_ids: records
            .revocations
            .iter()
            .map(|record| record.to_bytes().map(Digest::of))
            .collect::<elixir_marketplace::Result<_>>()?,
    };
    let input = Incoming {
        records: records.clone(),
        summary,
    };
    // Preserve the full merge and row/accounting integrity checks. If nothing
    // could change, no helper or writer is needed. Skipping a duplicate cannot
    // authorize code or remove a previously retained restriction.
    Ok(unchanged::check(database, &input, revision as u64)
        .await?
        .is_none())
}
