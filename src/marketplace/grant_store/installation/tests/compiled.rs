//! Actual journal staging/readback for an archive carrying an actual native
//! reproduction record. Metadata/public-source inputs are explicit fixtures.
//! This creates only a DISABLED pending generation, never activation authority.
use super::*;
use crate::marketplace::quarantine::local_build::LocalBuildRecord;

pub(in crate::marketplace) async fn stage_and_reload_compiled(
    database: &Database,
    actor: &Actor,
    archive: &ArchivedSource,
    metadata: &MetadataObservation,
    public: &PublicSourceObservation,
    root: &Path,
) -> Result<LocalBuildRecord> {
    let material = Material::from_archive(archive, metadata)?;
    let grant = super::super::super::load(database, material.review.scope().extension_id())
        .await?
        .context("actual compiled grant missing")?;
    let intent = OwnerInstallIntent::for_actor(
        actor.clone(),
        &material.review,
        grant.revision(),
        0,
        Uuid::new_v4(),
    )?;
    let expected = material
        .build
        .as_ref()
        .context("actual build record missing")?
        .to_bytes()?;
    let staged = stage_checked(database, metadata, public, material, intent, budget()).await?;
    database.pool.close().await;
    let reopened =
        crate::marketplace::grant_store::tests::connect(&root.join("retained-build.sqlite"))
            .await?;
    let mut tx = reopened.pool.begin().await?;
    let stored = store::load_shared(&mut tx, reopened.driver, staged.extension())
        .await?
        .context("committed compiled journal disappeared")?;
    assert!(stored.journal.active.is_none() && stored.journal.activation.is_none());
    let pending = stored
        .journal
        .pending
        .as_ref()
        .context("compiled pending capsule missing")?;
    pending.facts()?;
    assert_eq!(pending.instance, staged.instance());
    let record = pending
        .build
        .as_ref()
        .context("durable compiled record missing")?
        .clone();
    assert_eq!(record.to_bytes()?, expected);
    let mut missing = pending.clone();
    missing.build = None;
    assert!(
        missing.facts().is_err(),
        "stored compiled flags cannot replace reproduction history"
    );
    let enabled: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE")
            .fetch_one(&mut *tx)
            .await?;
    assert_eq!(enabled, 0, "staging must not enable a compiled generation");
    tx.commit().await?;
    println!(
        "compiled capsule: actual disabled SQL staging, closed/reopened database and exact encrypted record readback; no activation"
    );
    Ok(record)
}

#[tokio::test]
async fn source_js_capsules_keep_the_legacy_wire_shape_and_do_not_accept_verified_flags()
-> Result<()> {
    // Journal data fixture only; no native execution/activation is claimed.
    let f = Fixture::new().await?;
    let material = f.material()?;
    let capsule = Capsule {
        authority: material.authority,
        source: material.source,
        review: material.review.review_id()?,
        grant_revision: f.revision,
        instance: Uuid::new_v4(),
        build: None,
    };
    capsule.facts()?;
    let bytes = canonical::to_vec(&capsule)?;
    let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert!(
        value.get("build").is_none(),
        "existing SourceJs bytes must not gain a field"
    );
    let reopened: Capsule = serde_json::from_slice(&bytes)?;
    assert_eq!(canonical::to_vec(&reopened)?, bytes);
    value["build"] = serde_json::json!({"verified":true});
    assert!(serde_json::from_value::<Capsule>(value).is_err());
    Ok(())
}
