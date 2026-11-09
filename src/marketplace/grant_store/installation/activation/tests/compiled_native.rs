//! Actual fresh build/image -> durable archive -> native probe -> encrypted
//! candidate copy/SDK -> readiness -> owner commit -> offline cold reopen.
//! Public-source/paid/protection observations are explicitly local fixtures.
//! No synthetic SourcePrepared, candidate result, ACTIVE row or commit proof.
use super::*;
use crate::marketplace::grant_store::installation::{
    InstallTarget, OwnerInstallIntent,
    operation::{OwnedCandidate, Phase},
    runtime::{InstalledController, compiled_fixture_configuration},
    standing::InstalledBinding,
};
use crate::marketplace::{
    archive::{self, ArchiveBudget},
    grant_store::{approve_for_actor, invocation::native_tests, tests as grants},
    quarantine::{SourceMatchedQuarantine, build::NativeBuildMatch, image_reference::RuntimeImage},
    search::Selection,
    worker::{MetadataObservation, OfflineWorker, PeerPipeFixture},
};
use elixir_marketplace::source_execution::RuntimeJson;
use futures_util::FutureExt;
use std::{os::unix::fs::PermissionsExt, path::Path};

pub(in crate::marketplace) async fn run_compiled_activation_fixture(
    source: SourceMatchedQuarantine,
    image: &RuntimeImage,
    build: &NativeBuildMatch,
    value: &serde_json::Value,
    root: &Path,
    deployment: &str,
) -> Result<()> {
    let runtime = source.runtime();
    let lane = source.job_lease().directory().to_owned();
    let worker = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let secrets = SecretsManager::from_key_bytes([93; 32], false);
    let public = source.execution_source()?;
    let old_image = image.runtime_view(&source)?;
    let db = grants::connect(&root.join("retained-build.sqlite")).await?;
    let actor = grants::owner(&db).await?;
    let principal = actor.fixture_principal();
    let (descriptor, policy) = native_tests::descriptor(value)?;
    let id = descriptor.release.payload.extension_id.clone();
    let (metadata, _metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    crate::marketplace::security_state::admit(&db, &metadata).await?;
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string()).bind(String::from_utf8(canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?)
        .bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&db.pool).await?;
    let manifest = source.check_manifest()?;
    let review = manifest.grant_review()?;
    let grant = approve_for_actor(&db, &actor, &manifest, &grants::selection(&review, 0)?).await?;
    let revision = grant.revision();
    let mut checked = Some(grant.check_scope(&db, manifest, Uuid::new_v4()).await?);
    let retention = ArchiveBudget {
        bytes: 64 * MIB,
        releases: 4,
    };
    let archive = Box::pin(archive::persist_compiled(
        &db,
        &mut checked,
        build,
        image,
        &secrets,
        &root.join("source-archives"),
        retention,
    ))
    .await?;
    assert!(checked.is_none() && old_image.check().is_err());
    let mut configuration =
        compiled_fixture_configuration(&db, runtime.clone(), root, &lane, &worker, deployment)?;
    configuration.archive = root.join("source-archives");
    let material = Material::from_archive(&archive, &metadata)?;
    let mut absent = configuration.clone();
    absent.compiled_toolchains = None;
    assert!(SourceActivationPlan::for_material(&material, &absent, &secrets).is_err());
    assert!(
        SourceActivationPlan::for_material(
            &material,
            &configuration,
            &SecretsManager::from_key_bytes([94; 32], true)
        )
        .is_err()
    );
    let plan = SourceActivationPlan::for_material(&material, &configuration, &secrets)?;
    assert!(plan.is_compiled());
    assert!(plan.source().workspace().artifact.is_some());
    let files = root.join("activation-peer-files");
    std::fs::create_dir(&files)?;
    use base64::Engine;
    for (name, field) in [
        ("package.blob", "package_base64"),
        ("proof.blob", "source_proof_base64"),
    ] {
        let bytes = base64::engine::general_purpose::STANDARD.decode(
            value[field]
                .as_str()
                .context("fixture source bytes missing")?,
        )?;
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let host_deployment = format!("compiled-activation-{}", Uuid::new_v4().simple());
    let peer_name = format!(
        "elixir-marketplace-compiled-activation-peer-{}",
        Uuid::new_v4().simple()
    );
    let mut host = None;
    let mut peer = None;
    let mut controller = None;
    let mut operation = None;
    let failed = value["fixture_variant"]
        .as_str()
        .is_some_and(|v| v.ends_with("-failure"));
    let outcome=Box::pin(std::panic::AssertUnwindSafe(async {
        controller=Some(InstalledController::open(db.clone(),secrets.clone(),configuration.clone()).await?);
        let limits=crate::marketplace::packet::PacketWorkloadBudget::split(&elixir_marketplace::policy::OrdinaryLimits::default())?.worker_limits().clone();
        let issuer=value["issuer_key"].as_str().context("fixture issuer missing")?;
        host=Some(OfflineWorker::start(runtime.as_ref(),&root.join("activation-host"),&host_deployment,&worker,&[issuer.parse()?],&limits).await?);
        peer=Some(PeerPipeFixture::start(&worker,&files,&peer_name,issuer,"normal").await?);
        let peer=peer.as_mut().context("original peer missing")?;
        peer.bind_security_database(db.clone());
        peer.bind_offline_source_owner(host.as_ref().context("original host missing")?,runtime.clone(),&lane)?;
        let mut selected=Selection::fixture_for_native_preparation(peer.client.clone(),metadata,public,vec![peer.peer.clone()])?;
        let staged=Box::pin(super::super::super::stage(&mut selected,&archive,
            OwnerInstallIntent::new(&principal,&review,revision,0,Uuid::new_v4(),InstallTarget::New {name:"Compiled native fixture".into()})?,limit())).await?;
        let owner=staged.configured_instance().context("configured owner missing")?;
        let namespace=staged.instance();
        let intent=OwnerActivationIntent::new_with_credentials(&principal,&staged,&review,revision,Uuid::new_v4(),&secrets,
            vec![CredentialChange::set("source_api","own-native-instance",false)])?;
        let request=ActivationRequest::new(selected,archive,staged,intent,plan,limit(),retention)?;
        let mut owners=Some((request,controller.take().context("original controller missing")?));
        operation=Some(Box::new(OwnedCandidate::begin_activation(&mut owners,runtime::tests::limits())?));
        let operation=operation.as_mut().context("original activation missing")?;
        let ready=operation.wait().await?;
        let expected=if failed {Phase::AwaitingCleanup}else{Phase::AwaitingCommit};
        if ready.phase!=expected {
            bail!("compiled activation reached {:?}, expected {:?}: {}",ready.phase,expected,
                operation.fixture_failure().map(|e|format!("{e:#}")).unwrap_or_default());
        }
        assert!(operation.take_controller().is_err(),"readiness alone must not release the owner");
        let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='migrated'")
            .bind(namespace.to_string()).fetch_one(&db.pool).await?;
        assert!(!cipher.contains("migrated"));
        let envelope:serde_json::Value=serde_json::from_str(&secrets.decrypt(&cipher)?)?;
        assert_eq!(envelope["owner_instance_id"],serde_json::json!(owner));
        assert_eq!(envelope["instance_id"],serde_json::json!(namespace));
        assert_eq!(envelope["value"],serde_json::json!(true));
        let enabled:i64=sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE").fetch_one(&db.pool).await?;
        assert_eq!(enabled,0,"native readiness is not activation consent");
        if failed {
            assert!(operation.activate(&principal,Uuid::new_v4()).await.is_err());
            assert_eq!(operation.cancel(&principal,Uuid::new_v4()).await?.phase,Phase::Cancelled);
            let retained:i64=sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1")
                .bind(namespace.to_string()).fetch_one(&db.pool).await?;
            assert_eq!(retained,0,"failed candidate state must be discarded by its actual owner");
            let remaining:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT COUNT(*) FROM marketplace_instance_sdk_state WHERE instance_id=$1),(SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE namespace_id=$1),(SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1)")
                .bind(namespace.to_string()).fetch_one(&db.pool).await?;
            assert_eq!(remaining,(0,0,0),"failed candidate must not retain a namespace, state head or credentials");
            assert!(InstalledBinding::load_configured(&db,actor.home,&id,owner,limit()).await.is_err());
        } else {
            let mutation=Uuid::new_v4();
            assert_eq!(operation.activate(&principal,mutation).await?.phase,Phase::Activated);
            assert_eq!(operation.activate(&principal,mutation).await?.phase,Phase::Activated);
        }
        controller=Some(operation.take_controller()?);
        controller.as_mut().unwrap().shutdown_cache().await?;
        drop(controller.take());
        Ok::<_,anyhow::Error>((owner,namespace))
    }).catch_unwind()).await;
    // Always join each original owner before touching exact fixture resources.
    if let Some(operation) = operation.as_mut() {
        operation.shutdown().await?;
    }
    drop(operation);
    if let Some(controller) = controller.as_mut() {
        controller.cancel().await?;
        controller.shutdown_cache().await?;
    }
    drop(controller);
    let peer_stopped = match peer.take() {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let host_stopped = match host.take() {
        Some(host) => host.stop(runtime.as_ref()).await,
        None => Ok(()),
    };
    peer_stopped?;
    host_stopped?;
    crate::marketplace::quarantine::peer_tests::remove_peer(&peer_name).await?;
    crate::marketplace::preparation::remove_native_activation_host(
        runtime.as_ref(),
        &host_deployment,
    )
    .await?;
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))?;
    let (owner, _namespace) = match outcome {
        Ok(result) => result?,
        // The panic hook already recorded the assertion. Return failure after
        // joined cleanup so the outer fixture preserves its DB/archive too.
        Err(_) => {
            bail!("compiled activation fixture assertion failed; see captured panic diagnostics")
        }
    };
    db.pool.close().await;
    if !failed {
        // No discovery worker/peer remains. Reopen the database and original
        // runtime lineage, then use the normal installed controller/broker.
        let reopened = grants::connect(&root.join("retained-build.sqlite")).await?;
        let mut configuration = compiled_fixture_configuration(
            &reopened,
            runtime.clone(),
            root,
            &lane,
            &worker,
            deployment,
        )?;
        configuration.archive = root.join("source-archives");
        let mut controller =
            InstalledController::open(reopened.clone(), secrets, configuration).await?;
        let binding =
            InstalledBinding::load_configured(&reopened, actor.home, &id, owner, limit()).await?;
        controller.begin(
            binding,
            runtime::tests::limits(),
            RuntimeJson::parse(b"null")?,
        )?;
        let result = controller.finish().await;
        controller.cancel().await?;
        controller.shutdown_cache().await?;
        assert_eq!(result?.as_str(), "true");
    }
    println!(
        "compiled activation failed_readiness={failed}: actual archived proof/native probe/state copy/lifecycle SDK/confirmed retirement/owner commit or failed-candidate discard; positive DB reopen/offline invocation; source/protection observations synthetic"
    );
    Ok(())
}
#[test]
fn actual_activation_fixture_keeps_its_continuation_on_the_heap() {
    fn size<F: std::future::Future>(
        _: impl FnOnce(
            SourceMatchedQuarantine,
            &'static RuntimeImage,
            &'static NativeBuildMatch,
            &'static serde_json::Value,
            &'static Path,
            &'static str,
        ) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    // Type-only regression; no source/image/build or activation is fabricated.
    let bytes = size(run_compiled_activation_fixture);
    assert!(
        bytes <= 128 * 1024,
        "compiled activation fixture future too large: {bytes}"
    );
}
