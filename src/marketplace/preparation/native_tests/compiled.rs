//! Ordinary start_install + actual source opening/upload/build/archive/staging.
//! Paid metadata, public-source and protection observations remain synthetic;
//! this is not public discovery, VPN privacy, activation or release approval.
use super::*;
use crate::marketplace::{
    grant_store::{
        self,
        installation::{
            self, CapturedTarget, ExpectedControls, InstallAction, InstallTarget,
            OwnerInstallIntent,
        },
    },
    worker::OfflineWorker,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use elixir_marketplace::reproduction::BuildPlatform;
use tokio::io::AsyncWriteExt;

#[tokio::test]
#[ignore = "private actual ordinary start_install, three signed uploads, source opening/build/import, permission review, original archive handoff and disabled staging; synthetic metadata/public-source/protection"]
async fn ordinary_compiled_preparation_carries_actual_build_history_into_staging() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let toolchain = std::env::var("ELIXIR_MARKETPLACE_TOOLCHAIN_IMAGE")?;
    let mut manager = DockerRuntimeManager::new(None);
    manager = manager.with_marketplace_import_profile(
        crate::runtime::image_import::Profile::from_operator_configuration(
            std::path::PathBuf::from(std::env::var("ELIXIR_TEST_IMAGE_IMPORT_BINARY")?)
                .canonicalize()?,
            std::env::var("ELIXIR_TEST_IMAGE_IMPORT_BINARY_SHA256")?.parse()?,
            std::path::PathBuf::from(std::env::var("ELIXIR_TEST_IMAGE_IMPORT_SOCKET")?)
                .canonicalize()?,
        )?,
    );
    manager = manager.with_marketplace_container_guardian(
        crate::runtime::container_guardian::Profile::from_operator_configuration(
            std::path::PathBuf::from(std::env::var("ELIXIR_TEST_CONTAINER_GUARDIAN_BINARY")?)
                .canonicalize()?,
            std::env::var("ELIXIR_TEST_CONTAINER_GUARDIAN_SHA256")?.parse()?,
            BuildPlatform::LinuxAmd64,
        )?,
    );
    let runtime: Arc<dyn RuntimeManager> = Arc::new(manager);
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?;
    let lane = directory.join("foreground");
    let deployment = format!("compiled-preparation-{}", Uuid::new_v4().simple());
    let host_deployment = format!("activation-host-{}", Uuid::new_v4().simple());
    let name = format!(
        "elixir-marketplace-compiled-preparation-peer-{}",
        Uuid::new_v4().simple()
    );
    let files = directory.join("peer-files");
    std::fs::create_dir(&files)?;
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/sdk/marketplace/test/compiled-build-fixture.mjs");
    let fixture = timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("node")
            .arg(script)
            .args([toolchain.as_str(), "container-image-activation"])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !fixture.status.success() || fixture.stdout.len() > 2 * 1024 * 1024 {
        bail!("compiled preparation fixture failed");
    }
    let value: serde_json::Value = serde_json::from_slice(&fixture.stdout)?;
    let artifact = STANDARD.decode(
        value["artifact_base64"]
            .as_str()
            .context("fixture artifact missing")?,
    )?;
    let (release, package, proof) =
        crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
    // The offline control-channel fixture requires its fixed public files at
    // startup, even though this test supplies quarantine through signed uploads.
    for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let issuer = value["issuer_key"]
        .as_str()
        .context("fixture issuer missing")?;
    let database = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", directory.join("security.sqlite").display()),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let actor = grant_store::tests::owner(&database).await?;
    let principal = actor.fixture_principal();
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string()).bind(String::from_utf8(elixir_marketplace::canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?)
        .bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&database.pool).await?;
    let captured = CapturedTarget::capture(
        &database,
        &principal,
        InstallAction {
            extension: release.payload().extension_id.clone(),
            release: release.release_id(),
            target: InstallTarget::New {
                name: "Actual fresh compiled source".into(),
            },
            expected_revision: 0,
            mutation: Uuid::new_v4(),
            controls: ExpectedControls::Absent {},
        },
    )
    .await?;
    let secrets = crate::secrets::SecretsManager::from_key_bytes([93; 32], false);
    let mut configuration = installation::runtime::compiled_fixture_configuration(
        &database,
        runtime.clone(),
        &directory,
        &lane,
        &image,
        &deployment,
    )?;
    configuration.archive = directory.join("source-archives");
    let service = Service::default();
    let mut peer = None;
    let mut host = None;
    let mut native_cleanup_started = false;
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        let limits = crate::marketplace::packet::PacketWorkloadBudget::split(&elixir_marketplace::policy::OrdinaryLimits::default())?;
        native_cleanup_started = true;
        host = Some(OfflineWorker::start(runtime.as_ref(), &directory.join("admission-host"), &host_deployment, &image, &[issuer.parse()?], limits.worker_limits()).await?);
        peer = Some(PeerPipeFixture::start(&image, &files, &name, issuer, "normal").await.context("offline compiled preparation control-channel startup")?);
        let peer = peer.as_mut().context("original peer missing")?;
        peer.bind_security_database(database.clone());
        peer.bind_offline_source_owner(host.as_ref().context("admission host missing")?, runtime.clone(), &lane)?;
        let (descriptor, policy) = descriptor(&value)?;
        let (metadata, _metadata_owner) = MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
        security_state::admit(&database, &metadata).await?;
        let (source, _source_owner) = PublicSourceObservation::fixture_for_source(release.release_id(), Digest::of(b"SPDX-License-Identifier: MIT\n"));
        let selected = Selection::fixture_for_native_preparation(peer.client.clone(), metadata, source, vec![])?;
        let authorization = grant_store::source_authorization::SourceAuthorization::capture(&database, &principal).await?;
        // No preopened source workspace, direct Build constructor, invented
        // record, alternate compiler or synthesized installation state.
        let id = service.start_install(selected, true, Some(authorization), &configuration, secrets.clone())?;
        let mut supplied = std::collections::BTreeSet::new();
        let review = timeout(Duration::from_secs(180), async {
            loop {
                let view = service.view(id)?;
                if matches!(view.phase, Phase::Failed | Phase::Cancelled | Phase::CleanupFailed) {
                    if let Some(failure) = service.operation_failure(id)? { bail!("ordinary compiled preparation failed: {failure:#}"); }
                    bail!("ordinary compiled preparation stopped at {:?}", view.phase);
                }
                if view.phase == Phase::AwaitingApproval { return view.review.context("original compiled permission review missing"); }
                if let Some(upload) = view.upload.filter(|upload| !upload.claimed) {
                    let bytes = match upload.purpose { QuarantinePurpose::Package => &package, QuarantinePurpose::Proof => &proof, QuarantinePurpose::Artifact => &artifact };
                    assert!(supplied.insert(upload.purpose.name().to_string()));
                    assert_eq!(upload.blob.sha256, Digest::of(bytes));
                    let mut claim = service.claim_upload(id, upload.upload_id, upload.blob.size)?;
                    claim.writer.write_all(bytes).await?;
                    claim.writer.shutdown().await?;
                    assert!(claim.completion.await?, "actual signed upload was rejected");
                }
                sleep(Duration::from_millis(50)).await;
            }
        }).await.context("ordinary compiled preparation observation deadline")??;
        assert_eq!(supplied.len(), 3);
        assert_eq!(counters(&name).await?["starts"], 0, "fixture files cannot substitute for the three actual uploads");
        assert!(ControllerLease::acquire(&lane).is_err(), "review keeps its original native source/image owner");
        let approval = grant_store::ApprovalSelection {
            review_id: review.review_id()?, expected_revision: 0,
            mutation_id: Uuid::new_v4(), automatic_updates: false,
        };
        service.approve_as(id, actor.clone(), approval)?;
        phase(&service, id, Phase::SourceArchived).await?;
        let mut handoff = None;
        service.take_archive(id, &mut handoff).await?;
        let (mut selected, archive, review_id, grant_revision) = handoff.context("actual compiled archive handoff missing")?.into_parts();
        assert_eq!(archive.release_id(), release.release_id());
        assert_eq!(review_id, review.review_id()?);
        assert_eq!(grant_revision, 1);
        let record = archive.build_record().context("owned archive must carry actual local reproduction history")?;
        // Data identifies the image to inspect; it cannot create a native
        // image/source capability or substitute for the actual build above.
        let record_data: serde_json::Value = serde_json::from_slice(&record.to_bytes()?)?;
        let config = record_data["binding"]["config"].as_str().context("actual reproduced image identity missing")?;
        let _: Digest = config.parse()?;
        let inspected = timeout(Duration::from_secs(10), tokio::process::Command::new("docker").args(["image", "inspect", config, "--format", "{{.Id}}"])
            .kill_on_drop(true).output()).await.context("disposed native image inspection deadline")??;
        assert_eq!(inspected.status.code(), Some(1));
        assert!(String::from_utf8(inspected.stderr)?.contains(&format!("No such image: {config}")), "owned archive disposal must retire its passive native image");
        let authority = elixir_marketplace::retained_authority::AuthorityRecord::from_descriptor(&descriptor)?.verify()?;
        let (_, actual_review) = archive.record_for(authority.release())?.review(authority.release())?;
        assert_eq!(actual_review.review_id()?, review_id);
        let allocation = installation::InstallBudget { entries: 2, bytes: 4 * MIB };
        let staged = installation::stage(&mut selected, &archive,
            OwnerInstallIntent::new_captured(&principal, &actual_review, grant_revision, &captured)?, allocation).await?;
        assert_eq!(staged.revision(), 1);
        let enabled: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE").fetch_one(&database.pool).await?;
        assert_eq!(enabled, 0, "reproduction and archive staging alone never activate code");
        installation::cancellation::cancel(&database, installation::cancellation::OwnerCancelIntent::new(&principal, &staged, Uuid::new_v4())?, allocation).await?;
        drop(ControllerLease::acquire(&lane)?);
        println!("ordinary compiled preparation: actual source opening, three signed uploads, approved build/filesystem/export/import, permission review and owned build-record archive/staging passed; no activation or public-network claim");
        Ok::<_, anyhow::Error>(())
    })).catch_unwind().await;
    let outcome =
        outcome.unwrap_or_else(|_| Err(anyhow::anyhow!("ordinary compiled preparation panicked")));
    let cleanup = service.shutdown().await;
    let host_stopped = match host.take() {
        Some(host) => host.stop(runtime.as_ref()).await,
        None => Ok(()),
    };
    let peer_stopped = match peer.take() {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let host_removed = if native_cleanup_started {
        remove_activation_host(runtime.as_ref(), &host_deployment).await
    } else {
        Ok(())
    };
    let peer_removed = remove_peer(&name).await;
    database.pool.close().await;
    let writable = std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))
        .map_err(anyhow::Error::from);
    let cleanup = cleanup
        .and(host_stopped)
        .and(peer_stopped)
        .and(host_removed)
        .and(peer_removed)
        .and(writable);
    let result = match (outcome, cleanup) {
        (Err(original), Err(cleanup)) => Err(cleanup
            .context("original compiled preparation cleanup unconfirmed")
            .context(original)),
        (Err(original), Ok(())) => Err(original),
        (Ok(()), cleanup) => cleanup,
    };
    if result.is_err() {
        let retained = root.keep();
        eprintln!(
            "preserving isolated compiled preparation evidence at {}",
            retained.display()
        );
    }
    result
}
