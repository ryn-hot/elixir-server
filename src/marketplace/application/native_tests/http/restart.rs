//! Actual parked-cache crash followed by a clean process through operator startup.
//! Not in-flight native crash, lost-commit, public-network or warm-runtime proof.
use super::*;
use std::io::Write;
mod crash;

pub(super) fn write_profile(root: &Path, image: &str, a: &Allocation) -> Result<()> {
    let data = json!({"schema_version":1,"worker_image":image,
        "installations":{"entries":a.installations.entries,"bytes":a.installations.bytes},
        "source":{"memory_bytes":a.source.memory_bytes,"cpu_millicores":a.source.cpu_millicores,"disk_bytes":a.source.disk_bytes,"input_bytes":a.source.input_bytes,"output_bytes":a.source.output_bytes,"wall_seconds":a.source.wall_seconds},
        "retention":{"bytes":a.retention.bytes,"releases":a.retention.releases},"execution":a.execution});
    use std::os::unix::fs::OpenOptionsExt;
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
pub(super) async fn child(root: &Path, url: &str, image: &str) -> Result<()> {
    crash::qualify(root, url, image).await?;
    // No kill-on-drop or observation timeout: the parent's ORIGINAL child
    // wait owns the whole restart. Never launch a replacement after a slow wait.
    let status = tokio::process::Command::new(std::env::current_exe()?)
        .arg("marketplace::application::native_tests::http::restart::native_offline_restart_child")
        .args(["--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env("ELIXIR_TEST_MARKETPLACE_RESTORE_ROOT", root)
        .env("ELIXIR_TEST_MARKETPLACE_RESTORE_DATABASE_URL", url)
        .env("ELIXIR_MARKETPLACE_WORKER_IMAGE", image)
        .env_remove(crash::NONCE_ENV)
        .kill_on_drop(false)
        .status()
        .await?;
    anyhow::ensure!(status.success(), "original installed restart child failed");
    require_idle_byte_workspace(&DockerRuntimeManager::new(None), &crash::lane(root), false)
        .await?;
    println!("actual parked-cache SIGKILL and subsequent startup eviction/offline restore passed");
    Ok(())
}

#[tokio::test]
#[ignore = "child ONLY of exclusive native HTTP installation fixture, existing DB and private profile, no replacement/native proof factory"]
async fn native_offline_restart_child() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("ELIXIR_TEST_MARKETPLACE_RESTORE_ROOT")?);
    anyhow::ensure!(
        root.is_absolute() && root.canonicalize()? == root,
        "dedicated canonical fixture root required"
    );
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_RESTORE_DATABASE_URL")?;
    anyhow::ensure!(url==format!("sqlite://{}",root.join("application.sqlite").display())
        ||url.starts_with("postgresql://marketplace_fixture@127.0.0.1:54934/elixir_marketplace_activation_app_http_"),"dedicated existing fixture database required");
    let db = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    // Do NOT migrate, seed, sign up, approve, stage, initialize SDK state or use
    // a peer. This is solely the already-existing real installation.
    let before = sdk_rows(&db).await?;
    let mut settings = crate::http::test_settings_with_db();
    settings.database.url = url;
    settings.extensions.storage_root = root.join("extensions").display().to_string();
    settings.extensions.bundled_dir = root.join("bundled").display().to_string();
    settings.marketplace.installed_startup_file = Some(root.join("installed-startup.json"));
    let auth = crate::auth::AuthService::new(settings.auth.clone())?;
    let metadata = crate::metadata::MetadataService::new(settings.metadata.clone())?;
    let linkers = crate::library::LinkerService::new(settings.classifier.clone())?;
    let artwork = crate::http::test_artwork_service(&settings)?;
    let secrets = crate::secrets::SecretsManager::from_settings(&settings)?;
    let state = crate::state::AppState::new(
        settings,
        db.clone(),
        auth,
        crate::extensions::ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let shutdown = tokio_util::sync::CancellationToken::new();
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        let startup =
            crate::marketplace::installed_startup::spawn_configured(&state, shutdown.clone())
                .context("configured actual startup task")?;
        startup.await??;
        // The crash parent observed this cache PRESENT after SIGKILL. Actual
        // operator restoration must evict it BEFORE any new call can recreate it.
        require_idle_byte_workspace(&DockerRuntimeManager::new(None), &crash::lane(&root), false)
            .await?;
        assert!(
            state.marketplace.client().is_err(),
            "retained execution must not need a network participation client"
        );
        let descriptors = state.marketplace.source_providers(&db).await?;
        assert_eq!(descriptors.len(), 1);
        let source = &descriptors[0];
        let mut call = state.marketplace.begin_installed_call(
            source.home,
            source.extension.clone(),
            source.instance,
            RuntimeJson::from_value(json!({}))?,
        )?;
        let value = call.finish().await?;
        assert_eq!(
            serde_json::from_str::<Value>(value.as_str())?,
            json!({"cursor":1,"key":"credential-private"})
        );
        call.cancel().await?;
        drop(call);
        let mut limits = crate::live::config::LiveConfig::default().providers;
        limits.request_timeout_seconds = 60;
        limits.hard_timeout_seconds = 120;
        let client = crate::live::provider::LiveProviderClient::new_with_marketplace(
            db.clone(),
            state.marketplace.clone(),
            limits,
            Arc::new(crate::live::diagnostics::LiveRedactor::default()),
        )?;
        let snapshot = client.directory().get(source.provider).await?;
        client
            .health(&snapshot, &tokio_util::sync::CancellationToken::new())
            .await?;
        assert_eq!(
            sdk_rows(&db).await?,
            before,
            "clean process restart cannot rerun migration or rewrite SDK state"
        );
        drop(client);
        crash::exit_if_requested(&root).await?;
        Ok::<_, anyhow::Error>(())
    }))
    .catch_unwind()
    .await;
    shutdown.cancel();
    let stopped = state.marketplace.shutdown().await;
    drop(state);
    db.pool.close().await;
    stopped?;
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    println!(
        "separate clean process restored actual controller/verified source offline through operator startup, without enrollment or SDK state changes"
    );
    Ok(())
}
