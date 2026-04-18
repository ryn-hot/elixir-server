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
use crate::extensions::manifest::{ExtensionManifest, repair_builtin_manifest_json};
use crate::extensions::package::{
    compute_sha256, read_manifest_from_dir, unpack_package, write_manifest_to_dir,
};
use crate::extensions::registry::start_registry_refresh_loop;
use crate::extensions::store::{ExtensionStore, NewExtension};
use crate::http::handlers::extensions::InstallRequest;
use crate::http::router;
use crate::library::LinkerService;
use crate::library::start_periodic_scan;
use crate::metadata::MetadataService;
use crate::network::{start_mdns, wan::start_wan_tasks};
use crate::orchestrator::reconcile::ReconcileConfig;
use crate::playback::start_session_cleanup;
use crate::runtime::docker::{DockerRuntimeManager, DockerStartupConfig};
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
    ensure_docker_runtime_available(&settings)
        .await
        .context("docker runtime is unavailable")?;

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
    if let Err(err) = repair_installed_extension_manifests(&state).await {
        tracing::warn!("installed extension manifest repair failed: {err}");
    }
    if let Err(err) = state.orchestrator.prepare_probe_binary().await {
        tracing::warn!("probe binary preparation failed: {err}");
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
    if let Err(err) = state
        .orchestrator
        .restore_persisted_runtime_health_state()
        .await
    {
        tracing::warn!("orchestrator runtime health restore failed: {err}");
    }
    state
        .orchestrator
        .clone()
        .start_runtime_health_loop(reconcile_config.clone());
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

async fn ensure_docker_runtime_available(settings: &Settings) -> anyhow::Result<()> {
    let runtime = DockerRuntimeManager::new(None);
    let startup = DockerStartupConfig {
        auto_start_runtime: settings.extensions.docker.auto_start_runtime,
        startup_timeout: std::time::Duration::from_secs(
            settings.extensions.docker.startup_timeout_seconds,
        ),
        startup_poll_interval: std::time::Duration::from_millis(
            settings.extensions.docker.startup_poll_interval_millis,
        ),
    };
    let status = runtime.ensure_daemon_available(&startup).await?;
    tracing::info!("docker daemon ready ({status:?})");
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
        let Some(package) = package_map.get(extension_id) else {
            tracing::warn!(
                "core extension '{}' package not found in '{}'",
                extension_id,
                bundled_dir.display()
            );
            continue;
        };
        let request = InstallRequest {
            download_url: None,
            package_path: Some(package.path.to_string_lossy().to_string()),
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

async fn repair_installed_extension_manifests(state: &AppState) -> anyhow::Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    let bundled_dir = PathBuf::from(&state.settings.extensions.bundled_dir);
    let tmp_root = PathBuf::from(&state.settings.extensions.storage_root).join("tmp");
    fs::create_dir_all(&tmp_root).await?;
    let bundled_packages = if bundled_dir.is_dir() {
        index_bundled_packages(&bundled_dir, &tmp_root).await?
    } else {
        HashMap::new()
    };
    let mut resynced = 0_u32;
    let mut repaired = 0_u32;
    let mut rewritten_files = 0_u32;
    let unpacked_root = PathBuf::from(&state.settings.extensions.storage_root).join("unpacked");

    for extension in store.list_extensions().await? {
        if let Some(package) = bundled_packages.get(&extension.extension_id) {
            if bundled_package_drifted(
                &extension.version,
                extension.package_hash.as_deref(),
                &package.version,
                package.package_hash.as_deref(),
            ) {
                let request = InstallRequest {
                    download_url: None,
                    package_path: Some(package.path.to_string_lossy().to_string()),
                };
                match crate::http::handlers::extensions::install_extension(
                    State(state.clone()),
                    Json(request),
                )
                .await
                {
                    Ok(_) => {
                        resynced += 1;
                        tracing::info!(
                            extension_id = %extension.extension_id,
                            installed_version = %extension.version,
                            bundled_version = %package.version,
                            "resynced installed bundled extension from current package"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            extension_id = %extension.extension_id,
                            "failed to resync installed bundled extension: {:?}",
                            err
                        );
                    }
                }
            }
        }
    }

    for extension in store.list_extensions().await? {
        let mut raw_json = extension.manifest_json.clone();
        if !repair_builtin_manifest_json(&mut raw_json) {
            continue;
        }

        let manifest: ExtensionManifest = serde_json::from_value(raw_json.clone())
            .with_context(|| format!("parsing repaired manifest for {}", extension.extension_id))?;
        manifest.validate().with_context(|| {
            format!(
                "validating repaired manifest for {}",
                extension.extension_id
            )
        })?;

        let unpacked_dir = unpacked_root
            .join(&extension.extension_id)
            .join(&extension.version);
        if unpacked_dir.is_dir() {
            write_manifest_to_dir(&unpacked_dir, &raw_json).await?;
            rewritten_files += 1;
        }

        store
            .upsert_extension(&NewExtension {
                extension_id: extension.extension_id.clone(),
                name: extension.name.clone(),
                version: extension.version.clone(),
                kind: extension.kind,
                publisher_name: extension.publisher_name.clone(),
                signing_key_id: extension.signing_key_id.clone(),
                trust_level: extension.trust_level,
                manifest_json: raw_json,
                package_hash: extension.package_hash.clone(),
                enabled: extension.enabled,
            })
            .await?;

        repaired += 1;
        tracing::info!(
            "repaired installed manifest for extension '{}'",
            extension.extension_id
        );
    }

    if resynced > 0 {
        tracing::info!("resynced {resynced} installed bundled extension(s)");
    }
    if repaired > 0 {
        tracing::info!("repaired {repaired} installed extension manifest(s)");
    }
    if rewritten_files > 0 {
        tracing::info!("rewrote {rewritten_files} unpacked installed manifest file(s)");
    }

    Ok(())
}

async fn index_bundled_packages(
    bundled_dir: &Path,
    tmp_root: &Path,
) -> anyhow::Result<HashMap<String, BundledPackage>> {
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
        let package_hash = if file_type.is_file() {
            Some(compute_sha256(&path).await?)
        } else {
            None
        };
        map.insert(
            package_manifest.manifest.id.clone(),
            BundledPackage {
                path: path.clone(),
                version: package_manifest.manifest.version,
                package_hash,
            },
        );
        if let Some(dir) = staging_dir {
            let _ = fs::remove_dir_all(dir).await;
        }
    }

    Ok(map)
}

#[derive(Debug, Clone)]
struct BundledPackage {
    path: PathBuf,
    version: String,
    package_hash: Option<String>,
}

fn bundled_package_drifted(
    installed_version: &str,
    installed_hash: Option<&str>,
    bundled_version: &str,
    bundled_hash: Option<&str>,
) -> bool {
    if installed_version != bundled_version {
        return true;
    }
    match (installed_hash, bundled_hash) {
        (Some(installed), Some(current)) => !installed.eq_ignore_ascii_case(current),
        (None, Some(_)) | (Some(_), None) => true,
        (None, None) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::bundled_package_drifted;

    #[test]
    fn bundled_package_drifted_detects_hash_changes_at_same_version() {
        assert!(bundled_package_drifted(
            "1.0.0",
            Some("aaaa"),
            "1.0.0",
            Some("bbbb"),
        ));
    }

    #[test]
    fn bundled_package_drifted_ignores_identical_hashes_case_insensitively() {
        assert!(!bundled_package_drifted(
            "1.0.0",
            Some("AAAA"),
            "1.0.0",
            Some("aaaa"),
        ));
    }

    #[test]
    fn bundled_package_drifted_detects_version_changes_without_hash_drift() {
        assert!(bundled_package_drifted(
            "1.0.0",
            Some("aaaa"),
            "1.0.1",
            Some("aaaa"),
        ));
    }
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
