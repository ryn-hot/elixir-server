//! The check before Stage 3 deletes the legacy Arr code: a clean server
//! installs the signed Arr Stack through its own HTTP routes, then sets up the
//! real Sonarr, Radarr and Prowlarr in one click and serves Find Media, the
//! control screens and the web pages, every call going to the stack's own code
//! in the long-lived executor. None of the legacy Arr stack's code may run.
//! Needs Docker, the worker image, and network access for the pinned Arr
//! images and their metadata services.
use super::*;

#[tokio::test]
#[ignore = "explicit clean server: signed Arr Stack install over HTTP, real Arr containers, one-click setup and Find Media through the long-lived executor"]
async fn native_http_arr_stack_one_click_on_a_clean_server() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_test_writer()
        .try_init();
    let configured = allocation();
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable image required")?;
    let image = docker(&["image", "inspect", "--format", "{{.Id}}", &image]).await?;
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?;
    let files = directory.join("source");
    let controller_root = directory.join("marketplace").join("worker");
    for path in [directory.join("marketplace"), controller_root.clone()] {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    let lane = controller_root.join("foreground-source");
    std::fs::create_dir(&files)?;
    let value = sdk_stack_fixture().await?;
    let (release, package, proof) =
        crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
    for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    // Only the library root, as main.rs creates it at startup. The server makes
    // the apps' media folders and the downloads folder itself, and on Linux
    // the apps and the built-in downloaders run as this server's user.
    let media = directory.join("media");
    std::fs::create_dir_all(&media)?;
    let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("extensions/bundled");
    let db = database(&directory, None).await?;
    let sqlite_url = format!(
        "sqlite://{}",
        directory.join("application.sqlite").display()
    );
    let (state, public, _principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture_with_settings(
            db.clone(),
            &directory,
            &sqlite_url,
            |settings| {
                settings.library.local_root = media.display().to_string();
                settings.extensions.bundled_dir = bundled.display().to_string();
                settings.extensions.allow_directory_install = true;
            },
        )
        .await?;
    let app = public
        .merge(crate::http::handlers::marketplace::installation_routes().with_state(state.clone()));
    state
        .marketplace
        .configure_installation(configured.clone())?;
    let original = state.marketplace.fixture_original_installation();
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let name = format!("elixir-marketplace-arr-peer-{}", Uuid::new_v4().simple());
    let host_deployment = format!("arr-host-{}", Uuid::new_v4().simple());
    let deployment = crate::runtime::RuntimePaths::from_roots(
        &state.settings.extensions.storage_root,
        &state.settings.library.local_root,
    )
    .deployment_id();
    let volumes = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut peer = None;
    let mut host = None;
    struct Uninstall;
    impl Drop for Uninstall {
        fn drop(&mut self) {
            crate::orchestrator::stack_setup::install(None);
        }
    }
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        let issuer = value["issuer_key"].as_str().context("fixture issuer")?;
        peer = Some(PeerPipeFixture::start(&image, &files, &name, issuer, "providers").await?);
        check_profile(&name).await?;
        host = Some(
            OfflineWorker::start(
                runtime.as_ref(),
                &directory.join("admission-host"),
                &host_deployment,
                &image,
                &[issuer.parse()?],
                crate::marketplace::packet::PacketWorkloadBudget::split(
                    &elixir_marketplace::policy::OrdinaryLimits::default(),
                )?
                .worker_limits(),
            )
            .await?,
        );
        let p = peer.as_mut().unwrap();
        p.bind_security_database(db.clone());
        p.bind_offline_source_owner(host.as_ref().unwrap(), runtime.clone(), &lane)?;
        let client = p.client.clone();
        let serve_configuration =
            client.installed_configuration(configured.source.clone(), configured.retention)?;
        let (desc, policy) = descriptor(&value)?;
        let (metadata, _metadata_owner) =
            MetadataObservation::fixture_for_verified_descriptor(desc, &policy)?;
        security_state::admit(&db, &metadata).await?;
        let (source, _source_owner) = PublicSourceObservation::fixture_for_source(
            release.release_id(),
            Digest::of(b"SPDX-License-Identifier: MIT\n"),
        );
        let selected = Selection::fixture_for_native_preparation(
            client.clone(),
            metadata,
            source,
            vec![p.peer.clone()],
        )?;
        let query = Uuid::new_v4();
        state
            .marketplace
            .fixture_install_context(InstallContext::fixture_selected(
                client, query, selected, false,
            ))?;

        // Install through the server's own routes: review, approve, set up,
        // activate.
        let extension = release.payload().extension_id.clone();
        let targets = call(
            &app,
            &token,
            "POST",
            "/api/v1/marketplace/install/targets",
            Some(json!({"schema_version": 1, "extension_id": extension})),
        )
        .await?;
        let started = call(
            &app,
            &token,
            "POST",
            "/api/v1/marketplace/installations",
            Some(
                json!({"schema_version": 1, "query_id": query, "extension_id": extension,
                "release_id": release.release_id(), "target": {"kind": "new", "name": "Arr Stack"},
                "expected_revision": targets["new_instance"]["expected_revision"],
                "mutation_id": Uuid::new_v4(), "controls": targets["new_instance"]["controls"]}),
            ),
        )
        .await?;
        let id: Uuid = serde_json::from_value(started["operation_id"].clone())?;
        let review = observed(&app, &token, id, "awaiting_approval").await?;
        action(
            &app,
            &token,
            id,
            json!({"action": "approve", "review_id": review["review_id"],
            "expected_revision": review["approval_revision"], "mutation_id": Uuid::new_v4(),
            "automatic_updates": false}),
        )
        .await?;
        observed(&app, &token, id, "awaiting_setup").await?;
        action(
            &app,
            &token,
            id,
            json!({"action": "setup", "mutation_id": Uuid::new_v4(),
            "credentials": [{"operation": "set", "key": "source_api",
                "value": "credential-private", "rotatable": false}]}),
        )
        .await?;
        observed(&app, &token, id, "awaiting_commit").await?;
        action(
            &app,
            &token,
            id,
            json!({"action": "activate", "mutation_id": Uuid::new_v4()}),
        )
        .await?;
        observed(&app, &token, id, "activated").await?;
        original.entry(id)?.task.lock().await.join().await?;
        peer.take().unwrap().finish().await?;
        eprintln!("clean server installed the signed Arr Stack through its own routes");

        // From here on, exactly as a server started by main.rs: the stack's
        // code runs in its long-lived executor.
        state.marketplace.configure_serve(
            db.clone(),
            state.secrets.as_ref().clone(),
            serve_configuration,
        )?;
        crate::orchestrator::stack_setup::install_marketplace(&state);
        let _uninstall = Uninstall;
        crate::http::live_arr_stack_one_click(crate::http::LiveArrStack {
            app: app.clone(),
            state: state.clone(),
            stack: extension.as_str().to_owned(),
            bundled: bundled.clone(),
            deployment: deployment.clone(),
            volumes: volumes.clone(),
            stack_row: None,
            node_calls: None,
            bearer: Some(format!("Bearer {token}")),
        })
        .await?;
        // qBittorrent's partial downloads go to the server's user as well,
        // even though its first start finds /config still the image's.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt as _;
            let paths = crate::runtime::RuntimePaths::from_roots(
                &state.settings.extensions.storage_root,
                &state.settings.library.local_root,
            );
            let incomplete = Path::new(&paths.downloads_root).join(".incomplete");
            // SAFETY: a plain getter with no arguments.
            let server = unsafe { libc::geteuid() };
            let owner = std::fs::metadata(&incomplete)?.uid();
            assert_eq!(owner, server, "{}", incomplete.display());
        }
        state.marketplace.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }))
    .catch_unwind()
    .await;
    let stopped = state.marketplace.shutdown().await;
    let peer_stopped = match peer.take() {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let host_removed = match host.take() {
        Some(worker) => match worker.stop(runtime.as_ref()).await {
            Ok(()) => {
                crate::marketplace::preparation::remove_native_activation_host(
                    runtime.as_ref(),
                    &host_deployment,
                )
                .await
            }
            Err(e) => Err(e),
        },
        None => Ok(()),
    };
    let peer_removed = remove_peer(&name).await;
    // The stack's containers and settings volumes of this server only.
    let containers = docker(&[
        "ps",
        "-aq",
        "--filter",
        &format!("label=elixir.deployment_id={deployment}"),
    ])
    .await
    .unwrap_or_default();
    for container in containers.split_whitespace() {
        let _ = docker(&["rm", "-f", container]).await;
    }
    for volume in volumes.lock().map(|v| v.clone()).unwrap_or_default() {
        let _ = docker(&["volume", "rm", "-f", &volume]).await;
    }
    db.pool.close().await;
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))?;
    if outcome.as_ref().is_err() || outcome.as_ref().is_ok_and(|r| r.is_err()) {
        eprintln!(
            "failed clean-server Arr Stack fixture preserved at {}",
            root.keep().display()
        );
    }
    stopped?;
    peer_stopped?;
    host_removed?;
    peer_removed?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
