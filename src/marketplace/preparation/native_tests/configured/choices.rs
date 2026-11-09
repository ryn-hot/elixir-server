//! Real native readiness/disposal followed by real control writes. Counters
//! never substitute for the original operation's retained retirement proof.
use super::*;
use crate::{
    extensions::store::ExtensionStore,
    marketplace::grant_store::installation::operation::{OwnedCandidate, Phase},
};
use elixir_marketplace::identity::ExtensionId;

async fn durable_snapshot(db: &Database, owner: Uuid) -> Result<(String, String, i64, String)> {
    Ok(sqlx::query_as("SELECT i.record_json,i.record_sha256,i.encoded_bytes,CAST(b.revision AS TEXT) FROM marketplace_installations i CROSS JOIN marketplace_installation_budget b WHERE i.instance_id=$1")
        .bind(owner.to_string()).fetch_one(&db.pool).await?)
}
async fn private_snapshot(db: &Database, namespace: Uuid) -> Result<(i64, i64, i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT revision,key_count,encoded_bytes,CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1) AS BIGINT) FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(namespace.to_string()).fetch_one(&db.pool).await?)
}

pub(super) async fn reject_superseded(
    db: &Database,
    principal: &CurrentPrincipal,
    operation: &mut OwnedCandidate,
    coordinates: (&ExtensionId, Uuid, Uuid),
    mutation: Uuid,
    case: &str,
) -> Result<()> {
    let (extension, owner, namespace) = coordinates;
    assert_eq!(operation.view().await?.phase, Phase::AwaitingCommit);
    let before = durable_snapshot(db, owner).await?;
    let private = private_snapshot(db, namespace).await?;
    assert!(
        private.1 > 0 && private.3 > 0 && private.4 > 0,
        "actual candidate migration and credential copy must have happened"
    );
    let store = ExtensionStore::new(&db.pool);
    if case == "retain_configured_choice_instance" {
        store.set_instance_enabled(owner, false).await?;
    } else {
        store
            .set_extension_enabled(extension.as_str(), false)
            .await?;
    }
    let error = operation
        .activate(principal, mutation)
        .await
        .err()
        .context("stale activation unexpectedly succeeded")?;
    assert!(
        format!("{error:#}").contains("choices changed"),
        "{error:#}"
    );
    assert_eq!(
        durable_snapshot(db, owner).await?,
        before,
        "failed commit cannot rewrite journal or budget"
    );
    assert_eq!(
        private_snapshot(db, namespace).await?,
        private,
        "failed commit cannot delete or rewrite candidate state"
    );
    let enabled:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM extensions WHERE enabled=TRUE)+(SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE) AS BIGINT)")
        .fetch_one(&db.pool).await?;
    assert_eq!(enabled, 0);
    let cancelled = operation.cancel(principal, mutation).await?;
    assert_eq!(cancelled.phase, Phase::Cancelled);
    assert_eq!(
        operation.cancel(principal, mutation).await?.revision,
        cancelled.revision
    );
    let remaining:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)")
        .fetch_one(&db.pool).await?;
    assert_eq!(
        remaining, 0,
        "fresh authenticated cleanup remains available after a later choice"
    );
    eprintln!(
        "{case}: actual native readiness followed by same-value real disable fenced activation; original retired candidate remained safely cancellable"
    );
    Ok(())
}

pub(super) async fn retry_after_disable(
    db: &Database,
    principal: &CurrentPrincipal,
    operation: &mut OwnedCandidate,
    extension: &ExtensionId,
    owner: Uuid,
    mutation: Uuid,
    revision: Option<i64>,
) -> Result<()> {
    let before = durable_snapshot(db, owner).await?;
    ExtensionStore::new(&db.pool)
        .set_instance_enabled(owner, false)
        .await?;
    let retry = operation.activate(principal, mutation).await?;
    assert_eq!(retry.phase, Phase::Activated);
    assert_eq!(retry.revision, revision);
    assert_eq!(durable_snapshot(db, owner).await?, before);
    let enabled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&db.pool).await?;
    assert_eq!(
        enabled, 0,
        "exact retry must acknowledge only, never re-enable"
    );
    assert!(
        InstalledBinding::load_configured(
            db,
            principal.home_id,
            extension,
            owner,
            crate::marketplace::grant_store::installation::InstallBudget {
                entries: 2,
                bytes: 4 * MIB
            }
        )
        .await
        .is_err()
    );
    eprintln!(
        "exact successful activation retry preserved the later real disable and refused installed execution"
    );
    Ok(())
}
