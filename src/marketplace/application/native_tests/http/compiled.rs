//! Normal protected operator profile -> real authenticated HTTP install/update
//! -> actual source reproduction/activation -> offline cold restoration.
//! Only initial metadata/source availability/protection are synthetic. No
//! production extension, public publication, billing or VPN claim is made.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::{io::Write, os::unix::fs::OpenOptionsExt};

fn write_profile(root: &Path, image: &str, toolchain: &str, a: &Allocation) -> Result<()> {
    let local = |name| -> Result<std::path::PathBuf> {
        Ok(std::path::PathBuf::from(std::env::var(name)?).canonicalize()?)
    };
    let data = json!({"schema_version":1,"worker_image":image,
    "installations":{"entries":a.installations.entries,"bytes":a.installations.bytes},
    "source":{"memory_bytes":a.source.memory_bytes,"cpu_millicores":a.source.cpu_millicores,"disk_bytes":a.source.disk_bytes,"input_bytes":a.source.input_bytes,"output_bytes":a.source.output_bytes,"wall_seconds":a.source.wall_seconds},
    "retention":{"bytes":a.retention.bytes,"releases":a.retention.releases},"execution":a.execution,
    "compiled":{
        "guardian":{"executable":local("ELIXIR_TEST_CONTAINER_GUARDIAN_BINARY")?,"sha256":std::env::var("ELIXIR_TEST_CONTAINER_GUARDIAN_SHA256")?,"platform":"linux/amd64"},
        "importer":{"executable":local("ELIXIR_TEST_IMAGE_IMPORT_BINARY")?,"sha256":std::env::var("ELIXIR_TEST_IMAGE_IMPORT_BINARY_SHA256")?,"local_socket":local("ELIXIR_TEST_IMAGE_IMPORT_SOCKET")?},
        "toolchains":[toolchain],"retain_image_cache":true
    }});
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join("installed-startup.json"))?;
    file.write_all(&serde_json::to_vec(&data)?)?;
    file.sync_all()?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

async fn fixture(toolchain: &str, generation: usize) -> Result<Value> {
    let output = timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("node")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../extensions/sdk/marketplace/test/compiled-build-fixture.mjs"),
            )
            .arg(toolchain)
            .arg(format!("container-image-activation-update-v{generation}"))
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    anyhow::ensure!(
        output.status.success() && output.stdout.len() <= 2 * 1024 * 1024,
        "compiled SDK fixture generation failed"
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

async fn upload(
    app: &Router,
    token: &str,
    operation: Uuid,
    view: &Value,
    bytes: &[u8],
) -> Result<()> {
    assert_eq!(view["blob"]["sha256"], json!(Digest::of(bytes)));
    assert_eq!(view["blob"]["size"], json!(bytes.len()));
    let response = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/marketplace/installations/{operation}/uploads/{}",
                view["upload_id"].as_str().context("original upload id")?
            ))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/octet-stream")
            .header("content-length", bytes.len())
            .body(Body::from(bytes.to_vec()))?,
        )
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "signed HTTP upload failed: {}",
        response.status()
    );
    let result: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), 64 * 1024).await?)?;
    assert_eq!(result["integrity_verified"], true);
    assert_eq!(
        result["source_verified"], false,
        "byte integrity alone is not reproduction"
    );
    assert_eq!(result["installation_ready"], false);
    Ok(())
}

async fn review_with_uploads(
    app: &Router,
    token: &str,
    id: Uuid,
    package: &[u8],
    proof: &[u8],
    artifact: &[u8],
) -> Result<Value> {
    timeout(Duration::from_secs(240), async {
        let mut supplied = std::collections::BTreeSet::new();
        loop {
            let view = call(
                app,
                token,
                "GET",
                &format!("/api/v1/marketplace/installations/{id}"),
                None,
            )
            .await?;
            if view["phase"] == "awaiting_approval" {
                assert_eq!(
                    supplied.len(),
                    3,
                    "all signed bytes must use the original HTTP upload slots"
                );
                return Ok(view);
            }
            if matches!(
                view["phase"].as_str(),
                Some("stopped" | "recovery_required" | "cancelled")
            ) {
                bail!("compiled HTTP preparation stopped at {:?}", view["phase"]);
            }
            let input = &view["preparation"]["upload"];
            if input.is_object() && input["claimed"] == false {
                let purpose = input["purpose"]
                    .as_str()
                    .context("original upload purpose")?;
                let bytes = match purpose {
                    "package" => package,
                    "proof" => proof,
                    "artifact" => artifact,
                    _ => bail!("unexpected upload purpose"),
                };
                assert!(
                    supplied.insert(purpose.to_owned()),
                    "same signed input cannot be replayed"
                );
                upload(app, token, id, input, bytes).await?;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("compiled HTTP preparation observation deadline")?
}

async fn invoke(
    state: &crate::state::AppState,
    home: Uuid,
    extension: &elixir_marketplace::identity::ExtensionId,
    instance: Uuid,
    generation: usize,
) -> Result<()> {
    let mut call = state.marketplace.begin_installed_call(
        home,
        extension.clone(),
        instance,
        RuntimeJson::from_value(json!({}))?,
    )?;
    let value = call.finish().await?;
    assert_eq!(
        serde_json::from_str::<Value>(value.as_str())?,
        json!(generation)
    );
    call.cancel().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "exclusive local native normal operator startup plus authenticated HTTP signed uploads/reproduction/activation/nonempty update/offline cold restore; synthetic metadata/source/protection only"]
async fn normal_startup_owns_compiled_http_install_update_and_offline_restore() -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let toolchain = std::env::var("ELIXIR_MARKETPLACE_TOOLCHAIN_IMAGE")?;
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?;
    let lane = directory.join("marketplace/worker/foreground-source");
    let files = directory.join("source");
    std::fs::create_dir(&files)?;
    let mut configured = allocation();
    configured.source.memory_bytes = 256 * MIB;
    configured.source.cpu_millicores = 200;
    configured.source.disk_bytes = 512 * MIB;
    configured.retention = ArchiveBudget {
        bytes: 192 * MIB,
        releases: 4,
    };
    configured.validate()?;
    write_profile(&directory, &image, &toolchain, &configured)?;
    let first = fixture(&toolchain, 1).await?;
    let (_, package, proof) = crate::marketplace::quarantine::tests::decode_fixture(first.clone())?;
    for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let db = database(&directory, None).await?;
    let url = format!(
        "sqlite://{}",
        directory.join("application.sqlite").display()
    );
    let (mut state, public, principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture(db.clone(), &directory, &url)
            .await?;
    drop(public);
    let mut settings = state.settings.as_ref().clone();
    settings.marketplace.installed_startup_file = Some(directory.join("installed-startup.json"));
    settings.marketplace.installation_management = true;
    // Only initial discovery/protection is synthetic; this path selects the
    // normal router but never reads/starts a network profile or enrolls a VPN.
    settings.marketplace.startup_file = Some(directory.join("unused-network-profile.json"));
    state.settings = Arc::new(settings);
    let app = crate::http::router(state.clone());
    let shutdown = tokio_util::sync::CancellationToken::new();
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let host_deployment = format!("compiled-http-host-{}", Uuid::new_v4().simple());
    let name = format!(
        "elixir-marketplace-compiled-http-peer-{}",
        Uuid::new_v4().simple()
    );
    let mut peer = None;
    let mut host = None;
    let original = state.marketplace.fixture_original_installation();
    let mut installed = None;
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        crate::marketplace::installed_startup::spawn_configured(&state, shutdown.clone()).context("normal compiled startup owner")?.await??;
        assert!(state.marketplace.client().is_err(), "startup does not imply network participation");
        let issuer = first["issuer_key"].as_str().context("fixture issuer")?;
        host = Some(OfflineWorker::start(runtime.as_ref(), &directory.join("admission-host"), &host_deployment, &image, &[issuer.parse()?],
            crate::marketplace::packet::PacketWorkloadBudget::split(&elixir_marketplace::policy::OrdinaryLimits::default())?.worker_limits()).await?);
        peer = Some(PeerPipeFixture::start(&image, &files, &name, issuer, "normal").await?);
        let peer = peer.as_mut().context("original peer")?;
        peer.bind_security_database(db.clone());
        peer.bind_offline_source_owner(host.as_ref().context("original admission host")?, runtime.clone(), &lane)?;
        for generation in 1..=2 {
            let value = if generation == 1 { first.clone() } else { fixture(&toolchain, generation).await? };
            let artifact = STANDARD.decode(value["artifact_base64"].as_str().context("signed artifact")?)?;
            let (release, package, proof) = crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
            let (desc, policy) = descriptor(&value)?;
            let (metadata, _metadata_owner) = MetadataObservation::fixture_for_verified_descriptor(desc, &policy)?;
            security_state::admit(&db, &metadata).await?;
            let (source, _source_owner) = PublicSourceObservation::fixture_for_source(release.release_id(), Digest::of(b"SPDX-License-Identifier: MIT\n"));
            let selected = Selection::fixture_for_native_preparation(peer.client.clone(), metadata, source, vec![])?;
            let query = Uuid::new_v4();
            let publication = elixir_marketplace::publication::PreparedPublication::parse(&serde_json::to_vec(&json!({"schema_version":1,"publisher":value["publisher"],"release":value["release"],"head":value["head"]}))?)?;
            let reference = Digest::of(publication.to_bytes()?);
            state.marketplace.fixture_install_context(InstallContext::fixture_selected(peer.client.clone(), query, selected, false)
                .with_direct(principal.clone(), publication, reference, query).with_upload()?)?;
            let targets = call(&app, &token, "POST", "/api/v1/marketplace/install/targets", Some(json!({"schema_version":1,"extension_id":release.payload().extension_id}))).await?;
            let (target, expected, controls) = if let Some((instance, _)) = &installed {
                let row = targets["instances"].as_array().context("current instances")?.iter().find(|r| r["instance_id"] == json!(instance)).context("same configured target")?;
                (json!({"kind":"existing","instance_id":instance}), row["expected_revision"].clone(), row["controls"].clone())
            } else {
                (json!({"kind":"new","name":"Normal startup compiled fixture"}), targets["new_instance"]["expected_revision"].clone(), targets["new_instance"]["controls"].clone())
            };
            let started = call(&app, &token, "POST", "/api/v1/marketplace/installations", Some(json!({"schema_version":1,"query_id":query,"extension_id":release.payload().extension_id,"release_id":release.release_id(),"target":target,"expected_revision":expected,"mutation_id":Uuid::new_v4(),"controls":controls}))).await?;
            let id: Uuid = serde_json::from_value(started["operation_id"].clone())?;
            let review = review_with_uploads(&app, &token, id, &package, &proof, &artifact).await?;
            action(&app, &token, id, json!({"action":"approve","review_id":review["review_id"],"expected_revision":review["approval_revision"],"mutation_id":Uuid::new_v4(),"automatic_updates":false})).await?;
            observed(&app, &token, id, "awaiting_setup").await?;
            let credentials = if generation == 1 { json!([{"operation":"set","key":"source_api","value":"own-native-instance","rotatable":false}]) } else { json!([]) };
            action(&app, &token, id, json!({"action":"setup","mutation_id":Uuid::new_v4(),"credentials":credentials})).await?;
            observed(&app, &token, id, "awaiting_commit").await?;
            action(&app, &token, id, json!({"action":"activate","mutation_id":Uuid::new_v4()})).await?;
            let active = observed(&app, &token, id, "activated").await?;
            let instance: Uuid = serde_json::from_value(active["configured_instance"].clone())?;
            original.entry(id)?.task.lock().await.join().await?;
            if let Some((previous, _)) = &installed { assert_eq!(instance, *previous, "update keeps its configured instance and inherited state/credential"); }
            invoke(&state, principal.home_id, &release.payload().extension_id, instance, generation).await?;
            installed = Some((instance, release.payload().extension_id.clone()));
            println!("normal compiled startup: actual HTTP generation {generation} reproduction/activation/inherited SDK call passed");
        }
        assert_eq!(crate::marketplace::quarantine::peer_tests::counters(&name).await?["starts"], 0);
        Ok::<_, anyhow::Error>(())
    })).catch_unwind().await;
    shutdown.cancel();
    let stopped = state.marketplace.shutdown().await;
    let peer_stopped = match peer.take() {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let host_stopped = match host.take() {
        Some(worker) => worker.stop(runtime.as_ref()).await,
        None => Ok(()),
    };
    let host_removed = crate::marketplace::preparation::remove_native_activation_host(
        runtime.as_ref(),
        &host_deployment,
    )
    .await;
    let peer_removed = remove_peer(&name).await;
    drop(app);
    drop(original);
    let fresh = crate::state::AppState::new(
        state.settings.as_ref().clone(),
        db.clone(),
        state.auth_service.clone(),
        crate::extensions::ExtensionManager::new(),
        state.metadata.as_ref().clone(),
        crate::library::LinkerService::new(state.settings.classifier.clone())?,
        state.artwork.as_ref().clone(),
        state.secrets.as_ref().clone(),
    );
    // Shutdown joins native cleanup; dropping EVERY old facade releases its
    // original controller's OS lease before the new owner can open it.
    drop(state);
    // A new AppState must restore the existing durable installation through the
    // same real profile, without a peer, rebuild, approval or fabricated ACTIVE.
    let restarted = if outcome.as_ref().is_ok_and(|r| r.is_ok())
        && stopped.is_ok()
        && peer_stopped.is_ok()
        && host_stopped.is_ok()
        && host_removed.is_ok()
        && peer_removed.is_ok()
    {
        let before = sdk_rows(&db).await?;
        let fresh_shutdown = tokio_util::sync::CancellationToken::new();
        let result = async {
            crate::marketplace::installed_startup::spawn_configured(&fresh, fresh_shutdown.clone())
                .context("cold compiled startup owner")?
                .await??;
            assert!(fresh.marketplace.client().is_err());
            let (instance, extension) = installed.as_ref().context("actual installed result")?;
            invoke(&fresh, principal.home_id, extension, *instance, 2).await?;
            assert_eq!(
                sdk_rows(&db).await?,
                before,
                "restoration cannot rerun migration or rewrite SDK state"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fresh_shutdown.cancel();
        let cleanup = fresh.marketplace.shutdown().await;
        result.and(cleanup)
    } else {
        Ok(())
    };
    drop(fresh);
    db.pool.close().await;
    let writable = std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))
        .map_err(anyhow::Error::from);
    let result = outcome.unwrap_or_else(|_| Err(anyhow::anyhow!("compiled HTTP fixture panicked")));
    let cleanup = stopped
        .and(peer_stopped)
        .and(host_stopped)
        .and(host_removed)
        .and(peer_removed)
        .and(writable);
    let result = match (result.and(restarted), cleanup) {
        (Err(original), Err(cleanup)) => Err(cleanup
            .context("compiled HTTP cleanup unconfirmed")
            .context(original)),
        (Err(original), Ok(())) => Err(original),
        (Ok(()), cleanup) => cleanup,
    };
    if result.is_err() {
        eprintln!(
            "preserving original compiled HTTP evidence at {}",
            root.keep().display()
        );
    }
    result
}
