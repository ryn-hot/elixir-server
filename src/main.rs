mod artwork;
mod auth;
mod config;
mod db;
mod drivers;
mod extensions;
mod http;
mod library;
mod media;
mod metadata;
mod metrics;
mod network;
mod orchestrator;
mod playback;
mod runtime;
mod secrets;
mod state;
mod telemetry;

use crate::artwork::ArtworkService;
use crate::auth::AuthService;
use crate::config::Settings;
use crate::db::Database;
use crate::extensions::ExtensionManager;
use crate::extensions::package::{read_manifest_from_dir, unpack_package};
use crate::extensions::registry::start_registry_refresh_loop;
use crate::extensions::store::ExtensionStore;
use crate::http::handlers::extensions::InstallRequest;
use crate::http::router;
use crate::library::LinkerService;
use crate::library::start_periodic_scan;
use crate::metadata::MetadataService;
use crate::network::{start_mdns, wan::start_wan_tasks};
use crate::orchestrator::reconcile::ReconcileConfig;
use crate::playback::start_session_cleanup;
use crate::secrets::SecretsManager;
use crate::state::AppState;
use anyhow::Context;
use axum::{Json, extract::State};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::fs;
use tokio::net::TcpListener;
use uuid::Uuid;

fn load_env() {
    if let Ok(cwd) = std::env::current_dir() {
        for dir in cwd.ancestors() {
            let candidate = dir.join(".env");
            if candidate.is_file() {
                let _ = dotenvy::from_path(&candidate);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from the closest .env, walking up to repo root.
    load_env();

    let settings = Settings::load().context("failed to load configuration")?;
    telemetry::init_tracing(&settings.telemetry).context("failed to initialize tracing")?;
    metrics::init_metrics();
    ensure_runtime_directories(&settings)
        .await
        .context("failed to prepare runtime directories")?;
    log_resolved_paths(&settings);

    let database = Database::connect(&settings.database)
        .await
        .context("failed to initialize database pool")?;
    database
        .run_migrations()
        .await
        .context("database migrations failed")?;

    let auth_service =
        AuthService::new(settings.auth.clone()).context("failed to initialize auth service")?;
    let extension_sources_dir = PathBuf::from(&settings.extensions.bundled_dir)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(&settings.extensions.bundled_dir));
    let extension_sources_dir = extension_sources_dir.to_string_lossy().to_string();
    let extensions = ExtensionManager::load_from_dir(
        &extension_sources_dir,
        &settings.library.local_root,
        settings.library.hash_dedupe_enabled,
    )
    .await
    .unwrap_or_else(|err| {
        tracing::warn!("failed to load extensions directory: {err}");
        ExtensionManager::new()
    });
    let metadata = MetadataService::new(settings.metadata.clone())
        .context("failed to initialize metadata service")?;
    let linkers = LinkerService::new(settings.classifier.clone())
        .context("failed to initialize classifier linkers")?;
    let artwork = ArtworkService::new(
        settings.library.artwork_cache_dir.clone(),
        settings.metadata.request_timeout_seconds,
    )
    .context("failed to initialize artwork cache")?;
    let secrets =
        SecretsManager::from_settings(&settings).context("failed to initialize secrets manager")?;

    let addr = settings
        .server
        .socket_addr()
        .context("invalid server host or port")?;

    let state = AppState::new(
        settings.clone(),
        database,
        auth_service,
        extensions,
        metadata,
        linkers,
        artwork,
        secrets,
    );
    if let Err(err) = bootstrap_core_extensions(&state).await {
        tracing::warn!("core extension bootstrap failed: {err}");
    }
    let app = router(state.clone());

    // Kick off background periodic scan.
    let scan_interval = settings.library.scan_interval_seconds;
    let scan_state = state.clone();
    tokio::spawn(async move { start_periodic_scan(scan_state, scan_interval).await });

    // Start extensions reconcile loop.
    let reconcile_config = ReconcileConfig::from_settings(&settings);
    if let Err(err) = state
        .orchestrator
        .recover_orphaned_state_after_restart(&reconcile_config)
        .await
    {
        tracing::warn!("orchestrator startup recovery failed: {err}");
    }
    state
        .orchestrator
        .clone()
        .start_reconcile_loop(reconcile_config);

    // Refresh extension registries on an interval.
    let registries = settings.extensions.registries.clone();
    let storage_root = settings.extensions.storage_root.clone();
    let registry_interval = settings.extensions.registry_refresh_interval_seconds;
    tokio::spawn(async move {
        start_registry_refresh_loop(
            registries,
            storage_root,
            std::time::Duration::from_secs(registry_interval),
        )
        .await;
    });

    // Announce via mDNS if enabled; keep guard alive for process lifetime.
    let _mdns_guard = if state.settings.network.mdns_enabled {
        match start_mdns(&state.settings.server, &state.settings.network.mdns_name) {
            Ok(handle) => {
                state.mdns_active.store(true, Ordering::Relaxed);
                tracing::info!(
                    "mDNS announced: {}:{} ({})",
                    state.settings.server.host,
                    state.settings.server.port,
                    state.settings.network.mdns_name
                );
                Some(handle)
            }
            Err(err) => {
                tracing::warn!("failed to start mDNS announcer: {err}");
                None
            }
        }
    } else {
        None
    };

    // Attempt WAN mapping/registration asynchronously.
    start_wan_tasks(state.clone());

    // Cleanup stale playback sessions and transcode leftovers.
    let cleanup_state = state.clone();
    let cleanup_interval = settings.playback.cleanup_interval_seconds;
    let session_ttl = settings.playback.session_ttl_seconds;
    tokio::spawn(async move {
        start_session_cleanup(cleanup_state, session_ttl, cleanup_interval).await;
    });

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {}", addr))?;

    tracing::info!("Elixir server listening on http://{}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    // Clean up any lingering transcodes/temp files on shutdown.
    state.transcodes.stop_all().await;

    tracing::info!("Elixir server shutdown complete");
    Ok(())
}

async fn ensure_runtime_directories(settings: &Settings) -> anyhow::Result<()> {
    if let Some(sqlite_path) = sqlite_file_path(&settings.database.url) {
        if let Some(parent) = sqlite_path.parent() {
            fs::create_dir_all(parent).await?;
        }
    }

    fs::create_dir_all(&settings.library.local_root).await?;
    fs::create_dir_all(&settings.library.artwork_cache_dir).await?;
    fs::create_dir_all(&settings.extensions.storage_root).await?;
    fs::create_dir_all(PathBuf::from(&settings.extensions.storage_root).join("packages")).await?;
    fs::create_dir_all(PathBuf::from(&settings.extensions.storage_root).join("unpacked")).await?;
    fs::create_dir_all(PathBuf::from(&settings.extensions.storage_root).join("registry-cache"))
        .await?;
    fs::create_dir_all(PathBuf::from(&settings.extensions.storage_root).join("tmp")).await?;
    fs::create_dir_all(PathBuf::from(&settings.extensions.storage_root).join("probe")).await?;
    fs::create_dir_all(&settings.extensions.bundled_dir).await?;
    Ok(())
}

fn log_resolved_paths(settings: &Settings) {
    tracing::info!(
        "paths: db='{}' media='{}' artwork='{}' extensions_root='{}' bundled='{}'",
        settings.database.url,
        settings.library.local_root,
        settings.library.artwork_cache_dir,
        settings.extensions.storage_root,
        settings.extensions.bundled_dir
    );
}

fn sqlite_file_path(url: &str) -> Option<PathBuf> {
    let rest = if let Some(value) = url.strip_prefix("sqlite://") {
        value
    } else if let Some(value) = url.strip_prefix("sqlite:") {
        value
    } else {
        return None;
    };

    let path = rest.split('?').next().unwrap_or_default();
    if path.is_empty() || path.starts_with(":memory:") {
        return None;
    }
    Some(PathBuf::from(path))
}

async fn bootstrap_core_extensions(state: &AppState) -> anyhow::Result<()> {
    if state.settings.extensions.core_extensions.is_empty() {
        return Ok(());
    }
    let bundled_dir = PathBuf::from(&state.settings.extensions.bundled_dir);
    if !bundled_dir.is_dir() {
        tracing::warn!(
            "bundled extensions dir '{}' does not exist",
            bundled_dir.display()
        );
        return Ok(());
    }
    let tmp_root = PathBuf::from(&state.settings.extensions.storage_root).join("tmp");
    fs::create_dir_all(&tmp_root).await?;

    let package_map = index_bundled_packages(&bundled_dir, &tmp_root).await?;
    if package_map.is_empty() {
        tracing::warn!(
            "no bundled extensions discovered under '{}'",
            bundled_dir.display()
        );
    }

    let store = ExtensionStore::new(&state.db_pool);
    for extension_id in &state.settings.extensions.core_extensions {
        if store.get_extension(extension_id).await?.is_some() {
            continue;
        }
        let Some(path) = package_map.get(extension_id) else {
            tracing::warn!(
                "core extension '{}' package not found in '{}'",
                extension_id,
                bundled_dir.display()
            );
            continue;
        };
        let request = InstallRequest {
            download_url: None,
            package_path: Some(path.to_string_lossy().to_string()),
        };
        match crate::http::handlers::extensions::install_extension(
            State(state.clone()),
            Json(request),
        )
        .await
        {
            Ok(_) => tracing::info!("bootstrapped core extension '{}'", extension_id),
            Err(err) => tracing::warn!(
                "failed to bootstrap core extension '{}': {:?}",
                extension_id,
                err
            ),
        }
    }

    Ok(())
}

async fn index_bundled_packages(
    bundled_dir: &Path,
    tmp_root: &Path,
) -> anyhow::Result<HashMap<String, PathBuf>> {
    let mut map = HashMap::new();
    let mut entries = fs::read_dir(bundled_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() && !file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        if file_type.is_file() {
            let is_elx = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("elx"))
                .unwrap_or(false);
            if !is_elx {
                continue;
            }
        }

        let staging_dir = if file_type.is_dir() {
            None
        } else {
            Some(tmp_root.join(Uuid::new_v4().to_string()))
        };
        let unpacked = match &staging_dir {
            Some(dir) => unpack_package(&path, dir).await?,
            None => path.clone(),
        };
        let package_manifest = match read_manifest_from_dir(&unpacked).await {
            Ok(manifest) => manifest,
            Err(err) => {
                tracing::warn!(
                    "failed to read bundled manifest from '{}': {err}",
                    path.display()
                );
                if let Some(dir) = staging_dir {
                    let _ = fs::remove_dir_all(dir).await;
                }
                continue;
            }
        };
        map.insert(package_manifest.manifest.id.clone(), path.clone());
        if let Some(dir) = staging_dir {
            let _ = fs::remove_dir_all(dir).await;
        }
    }

    Ok(map)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to listen for SIGTERM signal");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, terminating");
}
