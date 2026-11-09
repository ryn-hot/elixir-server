//! Actual original-source/image + SQL/session/grant/AES SDK composition.
//! Metadata/public-source observations remain explicit fixtures; no activation.
use super::*;
use crate::marketplace::quarantine::{SourceMatchedQuarantine, image_reference::RuntimeImage};

/// Actual committed owner grant and owned archive copy. No peer or installed
/// activation fixture is substituted for the original native source/image job.
pub(in crate::marketplace) async fn archive_compiled(
    source: SourceMatchedQuarantine,
    root: &Path,
    path: &Path,
    budget: crate::marketplace::archive::ArchiveBudget,
) -> Result<crate::marketplace::archive::ArchivedSource> {
    let runtime = source.runtime();
    let db = grants::connect(&root.join("retained-build.sqlite")).await?;
    let actor = grants::owner(&db).await?;
    let preferences = String::from_utf8(canonical::to_vec(
        &crate::marketplace::preferences::Preferences::default(),
    )?)?;
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string()).bind(preferences).bind(Uuid::new_v4().to_string())
        .bind(actor.user.to_string()).execute(&db.pool).await?;
    let manifest = source.check_manifest()?;
    let review = manifest.grant_review()?;
    let grant = approve_for_actor(&db, &actor, &manifest, &grants::selection(&review, 0)?).await?;
    let checked = grant.check_scope(&db, manifest, Uuid::new_v4()).await?;
    crate::marketplace::archive::persist(&db, runtime.as_ref(), checked, path, budget).await
}

pub(in crate::marketplace) async fn run_compiled(
    source: SourceMatchedQuarantine,
    image: &RuntimeImage,
    value: &serde_json::Value,
    root: &Path,
    package: &[u8],
    proof: &[u8],
    deployment: &str,
) -> Result<SourceMatchedQuarantine> {
    image.require_source(&source)?;
    let runtime = source.runtime();
    let worker = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let db = grants::connect(&root.join("compiled-sdk.sqlite")).await?;
    let actor = grants::owner(&db).await?;
    let preferences = String::from_utf8(canonical::to_vec(
        &crate::marketplace::preferences::Preferences::default(),
    )?)?;
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string()).bind(preferences).bind(Uuid::new_v4().to_string())
        .bind(actor.user.to_string()).execute(&db.pool).await?;
    let files = root.join("compiled-peer-files");
    std::fs::create_dir(&files)?;
    let _restore = ReadOnlyFixtures(files.clone());
    for (name, bytes) in [("package.blob", package), ("proof.blob", proof)] {
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let peer_name = format!(
        "elixir-marketplace-compiled-sdk-peer-{}",
        Uuid::new_v4().simple()
    );
    let mut peer = None;
    let result = std::panic::AssertUnwindSafe(async {
        peer = Some(PeerPipeFixture::start(&worker, &files, &peer_name,
            value["issuer_key"].as_str().context("fixture issuer missing")?, "normal").await?);
        let peer = peer.as_mut().context("original peer missing")?;
        peer.bind_security_database(db.clone());
        let (candidate, policy) = descriptor(value)?;
        let (metadata, _metadata_owner) = MetadataObservation::fixture_for_verified_descriptor(candidate, &policy)?;
        security_state::admit(&db, &metadata).await?;
        // Borrow the ORIGINAL fixture observation; do not mint a replacement
        // TTL or return source evidence tied to this helper's temporary owner.
        let public = source.execution_source()?;
        let mut selected = Selection::fixture_for_native_preparation(peer.client.clone(), metadata, public, vec![peer.peer.clone()])?;
        let manifest = source.check_manifest()?;
        let review = manifest.grant_review()?;
        let grant = approve_for_actor(&db, &actor, &manifest, &grants::selection(&review, 0)?).await?;
        sqlx::query("INSERT INTO extensions (extension_id,name,version,kind,trust_level,manifest_json,enabled) VALUES ($1,'Compiled SDK fixture','1.0.0','module','community','{}',TRUE)")
            .bind(review.scope().extension_id().as_str()).execute(&db.pool).await?;
        let instance = Uuid::new_v4();
        let mut checked = grant.check_scope(&db, manifest, instance).await?;
        checked.initialize_sdk_instance(&db, "compiled SDK").await?;
        sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
            .bind(instance.to_string()).execute(&db.pool).await?;
        let secrets = SecretsManager::from_key_bytes([93; 32], false);
        grants::secret(&db, "global", None, "source_api", "never-global", &secrets).await?;
        grants::secret(&db, "instance", Some(instance), "source_api", "own-native-instance", &secrets).await?;
        let invocation = call(&actor, &checked, json!({}))?;
        let result = checked.invoke_compiled_for_owner(image, &mut selected, &db, &secrets, invocation).await?;
        assert_eq!(result.as_str(), "true");
        assert!(revision(&db, instance).await? > 0);
        image.require_source(checked.source.source())?;
        let writer = format!("elixir-marketplace-source-writer-{deployment}");
        assert!(runtime.get_container_handle(&writer).await?.is_none());

        // Fixture setup through the real scoped SDK, not a native-effect claim.
        // Force a DIFFERENT value so overflow must roll back an actual write,
        // not merely return the existing value's no-op acknowledgement.
        checked.resolve_sdk_local(&db, &secrets, &RuntimeJson::from_value(json!({
            "operation":"state_set","key":"fixture","value":false
        }))?).await?;
        let before = revision(&db, instance).await?;
        let mut overflow = call(&actor, &checked, json!({}))?;
        overflow.limits.sdk.as_mut().unwrap().response_bytes = 3;
        assert!(checked.invoke_compiled_for_owner(image, &mut selected, &db, &secrets, overflow).await.is_err());
        assert_eq!(revision(&db, instance).await?, before, "reply overflow must roll back real state");
        let mut wrong = call(&actor, &checked, json!({}))?;
        wrong.grant_revision += 1;
        assert!(checked.invoke_compiled_for_owner(image, &mut selected, &db, &secrets, wrong).await.is_err());
        assert_eq!(revision(&db, instance).await?, before);
        assert!(runtime.get_container_handle(&writer).await?.is_none());

        let looping = call(&actor, &checked, json!({"revoke":true}))?;
        let revoke = async {
            wait_revision(&db, instance, before).await?;
            sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='compiled SDK fixture' WHERE id=$1")
                .bind(actor.session.to_string()).execute(&db.pool).await?;
            Ok::<_, anyhow::Error>(())
        };
        let (invoked, revoked) = tokio::join!(
            checked.invoke_compiled_for_owner(image, &mut selected, &db, &secrets, looping), revoke);
        revoked?;
        assert!(invoked.is_err(), "revoked actor cannot keep its native effects scope");
        assert!(runtime.get_container_handle(&writer).await?.is_none());
        image.require_source(checked.source.source())?;
        println!("compiled real SDK: own-instance SQL/state/AES, denied secret/network, reply rollback, stale grant and live session revocation passed; no activation");
        Ok::<_, anyhow::Error>(checked.source.into_source())
    }).catch_unwind().await;
    let finished = match peer {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let removed = crate::marketplace::quarantine::peer_tests::remove_peer(&peer_name).await;
    removed?;
    finished?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
