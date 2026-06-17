mod acquisition;
mod artwork;
mod auth;
mod config;
mod db;
mod debrid;
mod download_broker;
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
use crate::db::models::{ExtensionInstance, ExtensionKind, SlotCardinality};
use crate::extensions::ExtensionManager;
use crate::extensions::auto_managed::{
    filter_auto_managed_runtime_missing, is_nzbget_extension_id, is_qbittorrent_extension_id,
};
use crate::extensions::cloudstream_registry::{
    CLOUDSTREAM_COMPAT_EXTENSION_ID, CloudStreamRecommendedPackMigrationSummary,
    migrate_cloudstream_recommended_source_pack_for_installed_instances,
    seed_cloudstream_recommended_source_pack_for_instance,
};
use crate::extensions::manifest::{ExtensionManifest, repair_builtin_manifest_json};
use crate::extensions::nuvio_registry::{
    PRISM_EXTENSION_ID, PrismRecommendedPackMigrationSummary,
    migrate_prism_recommended_source_pack_for_installed_instances,
    seed_prism_recommended_source_pack_for_instance,
};
use crate::extensions::package::{
    compute_sha256, read_manifest_from_dir, unpack_package, write_manifest_to_dir,
};
use crate::extensions::registry::start_registry_refresh_loop;
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_manifest,
};
use crate::extensions::store::{
    ExtensionStore, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
};
use crate::extensions::updater::start_proxy_runtime_update_loop;
use crate::http::handlers::extensions::{
    InstallPolicy, install_internal_extension_from_dir, resume_prism_certification_jobs,
};
use crate::http::router;
use crate::library::LinkerService;
use crate::library::start_periodic_scan;
use crate::metadata::MetadataService;
use crate::network::{start_mdns, wan::start_wan_tasks};
use crate::orchestrator::executor::ExecutorAction;
use crate::orchestrator::naming::build_aliases;
use crate::orchestrator::planner::{build_provider_endpoint, stable_provider_id};
use crate::orchestrator::reconcile::ReconcileConfig;
use crate::playback::start_session_cleanup;
use crate::runtime::health::{
    DockerRuntimeHealthSnapshot, DockerRuntimeHealthState, runtime_health_poll_interval,
};
use crate::secrets::SecretsManager;
use crate::state::AppState;
use anyhow::Context;
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
    if let Err(err) = repair_installed_extension_manifests(&state).await {
        tracing::warn!("installed extension manifest repair failed: {err}");
    }
    match ensure_core_extension_instances(&state).await {
        Ok(created) if created > 0 => {
            tracing::info!("bootstrapped {created} core extension default instance(s)");
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!("core extension instance bootstrap failed: {err}");
        }
    };
    match migrate_cloudstream_recommended_pack_for_existing_instances(&state).await {
        Ok(summary) if summary.migrated_instances > 0 => {
            tracing::info!(
                instances = summary.instances_seen,
                migrated = summary.migrated_instances,
                modules = summary.modules,
                versions = summary.versions,
                "migrated existing CloudStream Compat instance(s) to the recommended source pack"
            );
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!("CloudStream recommended source-pack migration failed: {err}");
        }
    }
    match migrate_prism_recommended_pack_for_existing_instances(&state).await {
        Ok(summary) if summary.migrated_instances > 0 => {
            tracing::info!(
                instances = summary.instances_seen,
                migrated = summary.migrated_instances,
                modules = summary.modules,
                versions = summary.versions,
                "migrated existing Prism instance(s) to the recommended source pack"
            );
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!("Prism recommended source-pack migration failed: {err}");
        }
    }
    if let Err(err) = debrid::ensure_debrid_builtin(&state).await {
        tracing::warn!("debrid provider bootstrap failed: {err}");
    }

    let reconcile_config = ReconcileConfig::from_settings(&settings);
    if let Err(err) = state
        .orchestrator
        .recover_orphaned_db_state_after_restart(&reconcile_config)
        .await
    {
        tracing::warn!("orchestrator db-only startup recovery failed: {err}");
    }
    if let Err(err) = state
        .orchestrator
        .restore_persisted_runtime_health_state()
        .await
    {
        tracing::warn!("orchestrator runtime health restore failed: {err}");
    }

    let app = router(state.clone());
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {}", addr))?;

    tracing::info!("Elixir server listening on http://{}", addr);

    tokio::spawn(start_post_listener_background_tasks(
        state.clone(),
        reconcile_config,
    ));

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    // Clean up any lingering transcodes/temp files on shutdown.
    state.transcodes.stop_all().await;

    tracing::info!("Elixir server shutdown complete");
    Ok(())
}

async fn start_post_listener_background_tasks(state: AppState, reconcile_config: ReconcileConfig) {
    // Kick off background periodic scan.
    let scan_interval = state.settings.library.scan_interval_seconds;
    let scan_state = state.clone();
    tokio::spawn(async move { start_periodic_scan(scan_state, scan_interval).await });

    state
        .orchestrator
        .clone()
        .start_runtime_health_loop(reconcile_config.clone());
    state
        .orchestrator
        .clone()
        .start_reconcile_loop(reconcile_config.clone());
    let runtime_bootstrap_state = state.clone();
    tokio::spawn(async move {
        run_docker_dependent_startup_bootstrap(runtime_bootstrap_state, reconcile_config).await;
    });

    let acquisition_recovery_state = state.clone();
    tokio::spawn(async move {
        acquisition::start_acquisition_recovery_loop(acquisition_recovery_state).await;
    });

    let acquisition_automation_state = state.clone();
    tokio::spawn(async move {
        acquisition::automation::start_acquisition_automation_loop(acquisition_automation_state)
            .await;
    });

    let anime_hash_worker_state = state.clone();
    tokio::spawn(async move {
        acquisition::release_resolution::hashing::start_anime_hash_worker_loop(
            anime_hash_worker_state,
        )
        .await;
    });

    let acquisition_import_state = state.clone();
    tokio::spawn(async move {
        acquisition::imports::start_acquisition_import_loop(acquisition_import_state).await;
    });

    let debrid_materializer_state = state.clone();
    tokio::spawn(async move {
        debrid::start_debrid_materializer_loop(debrid_materializer_state).await;
    });

    let http_stream_materializer_state = state.clone();
    tokio::spawn(async move {
        acquisition::stream_materializer::start_http_stream_materializer_loop(
            http_stream_materializer_state,
        )
        .await;
    });

    // Refresh extension registries on an interval.
    let registries = state.settings.extensions.registries.clone();
    let storage_root = state.settings.extensions.storage_root.clone();
    let registry_interval = state.settings.extensions.registry_refresh_interval_seconds;
    tokio::spawn(async move {
        start_registry_refresh_loop(
            registries,
            storage_root,
            std::time::Duration::from_secs(registry_interval),
        )
        .await;
    });

    let proxy_runtime_update_interval = state
        .settings
        .extensions
        .proxy_runtime_update_interval_seconds;
    let proxy_runtime_update_state = state.clone();
    tokio::spawn(async move {
        start_proxy_runtime_update_loop(
            proxy_runtime_update_state,
            std::time::Duration::from_secs(proxy_runtime_update_interval),
        )
        .await;
    });

    let prism_certification_state = state.clone();
    tokio::spawn(async move {
        resume_prism_certification_jobs(prism_certification_state).await;
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
    let cleanup_interval = state.settings.playback.cleanup_interval_seconds;
    let session_ttl = state.settings.playback.session_ttl_seconds;
    tokio::spawn(async move {
        start_session_cleanup(cleanup_state, session_ttl, cleanup_interval).await;
    });

    if _mdns_guard.is_some() {
        std::future::pending::<()>().await;
    }
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

async fn run_docker_dependent_startup_bootstrap(
    state: AppState,
    reconcile_config: ReconcileConfig,
) {
    let retry_interval = runtime_health_poll_interval();
    loop {
        if let Err(err) = state
            .orchestrator
            .run_runtime_health_check_once(&reconcile_config)
            .await
        {
            tracing::warn!("initial docker runtime health check failed: {err}");
        }
        if let Err(err) = state
            .orchestrator
            .recover_orphaned_runtime_state_after_docker_ready()
            .await
        {
            tracing::warn!("orchestrator runtime startup recovery failed: {err}");
        }

        let snapshot = state.orchestrator.docker_runtime_snapshot();
        if let Some(reason) = core_runtime_bootstrap_blocker(&snapshot) {
            tracing::info!(
                "deferring preinstalled core runtime bootstrap until Docker runtime is ready: {reason}"
            );
            tokio::time::sleep(retry_interval).await;
            continue;
        }

        if let Err(err) = state.orchestrator.prepare_probe_binary().await {
            tracing::warn!("probe binary preparation failed: {err}");
            tokio::time::sleep(retry_interval).await;
            continue;
        }

        match bootstrap_preinstalled_core_extension_runtimes(&state).await {
            Ok(result) => {
                if result.bootstrapped > 0 {
                    tracing::info!(
                        "bootstrapped {} preinstalled core runtime(s)",
                        result.bootstrapped
                    );
                }
                if result.blocked_missing_secrets > 0 {
                    tracing::warn!(
                        "preinstalled core runtime bootstrap has {} instance(s) blocked on manual secrets",
                        result.blocked_missing_secrets
                    );
                }
                if result.should_retry() {
                    tracing::warn!(
                        "preinstalled core runtime bootstrap will retry: {} failed runnable instance(s), {} runnable instance(s) still pending",
                        result.failed,
                        result.pending_runnable()
                    );
                    tokio::time::sleep(retry_interval).await;
                    continue;
                }

                match preinstalled_downloader_providers_exist(&state).await {
                    Ok(true) => {
                        if let Err(err) = state
                            .orchestrator
                            .apply_builtin_downloader_profiles_now()
                            .await
                        {
                            tracing::warn!(
                                "preinstalled downloader profile bootstrap failed and will retry: {err}"
                            );
                            tokio::time::sleep(retry_interval).await;
                            continue;
                        }
                    }
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(
                            "checking preinstalled downloader providers failed and will retry: {err}"
                        );
                        tokio::time::sleep(retry_interval).await;
                        continue;
                    }
                }
                return;
            }
            Err(err) => {
                tracing::warn!("preinstalled core runtime bootstrap failed: {err}");
                tokio::time::sleep(retry_interval).await;
            }
        }
    }
}

fn core_runtime_bootstrap_blocker(snapshot: &DockerRuntimeHealthSnapshot) -> Option<String> {
    if snapshot.reboot_recommended {
        return Some(
            snapshot
                .reason
                .clone()
                .unwrap_or_else(|| "Docker runtime requires a host reboot".to_string()),
        );
    }
    if let Some(until) = snapshot.dependency_actions_deferred_until {
        let reason = snapshot.reason.clone().unwrap_or_else(|| {
            "Docker recovered recently and Elixir is waiting for core runtimes.".to_string()
        });
        return Some(format!(
            "{} Dependency work is deferred until {}.",
            reason,
            until.to_rfc3339()
        ));
    }

    match snapshot.state {
        DockerRuntimeHealthState::Degraded => Some(
            snapshot
                .reason
                .clone()
                .unwrap_or_else(|| "Docker runtime is degraded".to_string()),
        ),
        DockerRuntimeHealthState::Healthy | DockerRuntimeHealthState::Recovering => None,
    }
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
        match install_internal_extension_from_dir(
            state,
            &package.path,
            bundled_install_policy(false),
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

async fn ensure_core_extension_instances(state: &AppState) -> anyhow::Result<u32> {
    if state.settings.extensions.core_extensions.is_empty() {
        return Ok(0);
    }

    let store = ExtensionStore::new(&state.db_pool);
    let mut created = 0u32;
    for extension_id in &state.settings.extensions.core_extensions {
        let Some(extension) = store.get_extension(extension_id).await? else {
            continue;
        };
        if !extension.enabled || extension.kind != crate::db::models::ExtensionKind::Module {
            continue;
        }
        if !store.list_instances(Some(extension_id)).await?.is_empty() {
            continue;
        }
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.clone(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        if extension_id == CLOUDSTREAM_COMPAT_EXTENSION_ID {
            let package_dir = PathBuf::from(&state.settings.extensions.storage_root)
                .join("unpacked")
                .join(extension_id)
                .join(&extension.version);
            seed_cloudstream_recommended_source_pack_for_instance(
                &store,
                instance_id,
                Some(&package_dir),
            )
            .await?;
        }
        if extension_id == PRISM_EXTENSION_ID {
            let package_dir = PathBuf::from(&state.settings.extensions.storage_root)
                .join("unpacked")
                .join(extension_id)
                .join(&extension.version);
            seed_prism_recommended_source_pack_for_instance(
                &store,
                instance_id,
                Some(&package_dir),
                Some(&state.settings.extensions.storage_root),
            )
            .await?;
        }
        created += 1;
    }

    Ok(created)
}

async fn migrate_cloudstream_recommended_pack_for_existing_instances(
    state: &AppState,
) -> anyhow::Result<CloudStreamRecommendedPackMigrationSummary> {
    let store = ExtensionStore::new(&state.db_pool);
    let Some(extension) = store.get_extension(CLOUDSTREAM_COMPAT_EXTENSION_ID).await? else {
        return Ok(CloudStreamRecommendedPackMigrationSummary::default());
    };
    let package_dir = PathBuf::from(&state.settings.extensions.storage_root)
        .join("unpacked")
        .join(CLOUDSTREAM_COMPAT_EXTENSION_ID)
        .join(&extension.version);
    migrate_cloudstream_recommended_source_pack_for_installed_instances(&store, Some(&package_dir))
        .await
}

async fn migrate_prism_recommended_pack_for_existing_instances(
    state: &AppState,
) -> anyhow::Result<PrismRecommendedPackMigrationSummary> {
    let store = ExtensionStore::new(&state.db_pool);
    let Some(extension) = store.get_extension(PRISM_EXTENSION_ID).await? else {
        return Ok(PrismRecommendedPackMigrationSummary::default());
    };
    let package_dir = PathBuf::from(&state.settings.extensions.storage_root)
        .join("unpacked")
        .join(PRISM_EXTENSION_ID)
        .join(&extension.version);
    migrate_prism_recommended_source_pack_for_installed_instances(
        &store,
        Some(&package_dir),
        Some(&state.settings.extensions.storage_root),
    )
    .await
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct CoreRuntimeBootstrapResult {
    bootstrapped: u32,
    failed: u32,
    blocked_missing_secrets: u32,
    runnable_candidates: u32,
}

impl CoreRuntimeBootstrapResult {
    fn pending_runnable(&self) -> u32 {
        self.runnable_candidates
            .saturating_sub(self.bootstrapped)
            .saturating_sub(self.failed)
    }

    fn should_retry(&self) -> bool {
        self.failed > 0 || self.pending_runnable() > 0
    }
}

async fn bootstrap_preinstalled_core_extension_runtimes(
    state: &AppState,
) -> anyhow::Result<CoreRuntimeBootstrapResult> {
    if state.settings.extensions.core_extensions.is_empty() {
        return Ok(CoreRuntimeBootstrapResult::default());
    }

    let store = ExtensionStore::new(&state.db_pool);
    let mut result = CoreRuntimeBootstrapResult::default();

    for extension_id in &state.settings.extensions.core_extensions {
        let Some(extension) = store.get_extension(extension_id).await? else {
            continue;
        };
        if !extension.enabled || extension.kind != ExtensionKind::Module {
            continue;
        }

        let manifest: ExtensionManifest = serde_json::from_value(extension.manifest_json.clone())
            .context(format!(
            "parsing core manifest '{}'",
            extension.extension_id
        ))?;
        manifest.validate()?;
        if manifest.runtime.is_none() {
            continue;
        }

        let instances = store.list_instances(Some(extension_id)).await?;
        for instance in instances {
            if !instance.enabled {
                continue;
            }

            let provider_count = store
                .list_providers(Some(instance.instance_id))
                .await?
                .len();
            if !core_instance_needs_startup_bootstrap(&instance, provider_count, &manifest) {
                continue;
            }

            let required = required_secrets_from_manifest(&manifest)?;
            let missing = filter_auto_managed_runtime_missing(
                &extension.extension_id,
                missing_required_secrets_for_instance(&store, instance.instance_id, &required)
                    .await?,
            );
            if !missing.is_empty() {
                result.blocked_missing_secrets += 1;
                tracing::warn!(
                    extension_id = %extension.extension_id,
                    instance_id = %instance.instance_id,
                    instance_name = %instance.instance_name,
                    missing = ?missing,
                    "skipping preinstalled core runtime bootstrap because manual secrets are still missing"
                );
                continue;
            }

            result.runnable_candidates += 1;
            let actions = build_preinstalled_core_bootstrap_actions(&instance, &manifest)?;
            tracing::info!(
                extension_id = %extension.extension_id,
                instance_id = %instance.instance_id,
                instance_name = %instance.instance_name,
                "bootstrapping preinstalled core runtime"
            );
            match state.orchestrator.apply_actions(actions).await {
                Ok(()) => result.bootstrapped += 1,
                Err(err) => {
                    result.failed += 1;
                    tracing::warn!(
                        extension_id = %extension.extension_id,
                        instance_id = %instance.instance_id,
                        instance_name = %instance.instance_name,
                        "preinstalled core runtime bootstrap failed: {err}"
                    );
                }
            }
        }
    }

    Ok(result)
}

async fn preinstalled_downloader_providers_exist(state: &AppState) -> anyhow::Result<bool> {
    let store = ExtensionStore::new(&state.db_pool);
    for extension_id in &state.settings.extensions.core_extensions {
        if !is_qbittorrent_extension_id(extension_id) && !is_nzbget_extension_id(extension_id) {
            continue;
        }
        for instance in store.list_instances(Some(extension_id)).await? {
            if !instance.enabled {
                continue;
            }
            if !store
                .list_providers(Some(instance.instance_id))
                .await?
                .is_empty()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn core_instance_needs_startup_bootstrap(
    instance: &ExtensionInstance,
    provider_count: usize,
    manifest: &ExtensionManifest,
) -> bool {
    if instance.runtime_version.is_none() {
        return true;
    }
    provider_count < manifest.provides.len()
}

fn build_preinstalled_core_bootstrap_actions(
    instance: &ExtensionInstance,
    manifest: &ExtensionManifest,
) -> anyhow::Result<Vec<ExecutorAction>> {
    const PREINSTALLED_BOOTSTRAP_GATE_TIMEOUT_SECONDS: u64 = 120;

    let runtime = manifest
        .runtime
        .clone()
        .ok_or_else(|| anyhow::anyhow!("module runtime is missing"))?;
    let networking = manifest.networking.clone();
    let (aliases, primary_alias) = build_aliases(
        &instance.extension_id,
        &instance.instance_name,
        instance.instance_id,
        runtime.service_name.clone(),
    );

    let mut actions = vec![ExecutorAction::EnsureRuntimeRunning {
        instance_id: instance.instance_id,
        extension_id: instance.extension_id.clone(),
        instance_name: instance.instance_name.clone(),
        runtime,
        networking: networking.clone(),
        aliases,
    }];

    let defer_provider_health_gate = is_qbittorrent_extension_id(&instance.extension_id)
        || is_nzbget_extension_id(&instance.extension_id);
    let mut provider_actions = Vec::new();
    for provide in &manifest.provides {
        let provider_id =
            stable_provider_id(instance.instance_id, &provide.capability, &provide.slot);
        let endpoint = build_provider_endpoint(provide, &networking, &primary_alias)?;
        provider_actions.push(ExecutorAction::CreateOrUpdateProvider {
            provider_id,
            instance_id: instance.instance_id,
            capability: provide.capability.clone(),
            slot_id: provide.slot.clone(),
            cardinality: provide.cardinality.unwrap_or(SlotCardinality::One),
            implementation: provide.implementation.clone(),
            scope_json: provide
                .scope
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .context("serializing provider scope")?,
            endpoint,
        });
        provider_actions.push(ExecutorAction::TransportGate {
            provider_id,
            timeout_seconds: PREINSTALLED_BOOTSTRAP_GATE_TIMEOUT_SECONDS,
        });
        provider_actions.push(ExecutorAction::BootstrapGate {
            provider_id,
            timeout_seconds: PREINSTALLED_BOOTSTRAP_GATE_TIMEOUT_SECONDS,
        });
        if !defer_provider_health_gate {
            provider_actions.push(ExecutorAction::HealthGate {
                provider_id,
                timeout_seconds: PREINSTALLED_BOOTSTRAP_GATE_TIMEOUT_SECONDS,
            });
        }
    }

    if provider_actions.is_empty() {
        anyhow::bail!(
            "preinstalled core module '{}' has no usable providers",
            instance.extension_id
        );
    }

    actions.extend(provider_actions);
    Ok(actions)
}

fn bundled_install_policy(allow_same_version_replace: bool) -> InstallPolicy {
    InstallPolicy {
        allow_internal_directory_install: true,
        allow_internal_unsigned: true,
        allow_downgrade: false,
        allow_same_version_replace,
        suppress_reconcile: true,
    }
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
    let mut refreshed_desired = 0_u32;
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
                match install_internal_extension_from_dir(
                    state,
                    &package.path,
                    bundled_install_policy(true),
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

    let extension_versions: HashMap<String, String> = store
        .list_extensions()
        .await?
        .into_iter()
        .map(|extension| (extension.extension_id, extension.version))
        .collect();
    for desired in store.list_desired_blueprints(None).await? {
        let Some(version) = extension_versions.get(&desired.blueprint_extension_id) else {
            continue;
        };
        if desired.blueprint_version == *version {
            continue;
        }
        store
            .upsert_desired_blueprint(&NewDesiredBlueprint {
                desired_id: desired.desired_id,
                blueprint_extension_id: desired.blueprint_extension_id.clone(),
                blueprint_version: version.clone(),
                params_json: desired.params_json.clone(),
            })
            .await?;
        refreshed_desired += 1;
        tracing::info!(
            desired_id = %desired.desired_id,
            blueprint_extension_id = %desired.blueprint_extension_id,
            previous_version = %desired.blueprint_version,
            refreshed_version = %version,
            "refreshed desired blueprint version to match installed blueprint"
        );
    }

    if resynced > 0 {
        tracing::info!("resynced {resynced} installed bundled extension(s)");
    }
    if repaired > 0 {
        tracing::info!("repaired {repaired} installed extension manifest(s)");
    }
    if refreshed_desired > 0 {
        tracing::info!("refreshed {refreshed_desired} desired blueprint version(s)");
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
    let mut paths = Vec::new();
    let mut entries = fs::read_dir(bundled_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        paths.push(entry.path());
    }
    paths.sort();

    for path in paths {
        let file_type = fs::metadata(&path).await?.file_type();
        if !file_type.is_file() && !file_type.is_dir() {
            continue;
        }
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
        let extension_id = package_manifest.manifest.id.clone();
        let candidate = BundledPackage {
            path: path.clone(),
            version: package_manifest.manifest.version,
            package_hash,
        };
        let should_insert = match map.get(&extension_id) {
            Some(existing) => should_replace_bundled_package(existing, &candidate),
            None => true,
        };
        if should_insert {
            map.insert(extension_id, candidate);
        }
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

fn should_replace_bundled_package(existing: &BundledPackage, candidate: &BundledPackage) -> bool {
    match (
        semver::Version::parse(&existing.version),
        semver::Version::parse(&candidate.version),
    ) {
        (Ok(existing_version), Ok(candidate_version)) => {
            if candidate_version > existing_version {
                return true;
            }
            if candidate_version < existing_version {
                return false;
            }
        }
        _ if candidate.version != existing.version => {
            return candidate.version > existing.version;
        }
        _ => {}
    }

    match (
        existing.package_hash.as_ref(),
        candidate.package_hash.as_ref(),
    ) {
        (None, Some(_)) => true,
        (Some(_), None) => false,
        _ => false,
    }
}

fn bundled_package_drifted(
    installed_version: &str,
    installed_hash: Option<&str>,
    bundled_version: &str,
    bundled_hash: Option<&str>,
) -> bool {
    match (
        semver::Version::parse(installed_version),
        semver::Version::parse(bundled_version),
    ) {
        (Ok(installed), Ok(bundled)) => {
            if bundled < installed {
                return false;
            }
            if bundled > installed {
                return true;
            }
        }
        _ if installed_version != bundled_version => {
            return true;
        }
        _ => {}
    }
    match (installed_hash, bundled_hash) {
        (Some(installed), Some(current)) => !installed.eq_ignore_ascii_case(current),
        (None, Some(_)) | (Some(_), None) => true,
        (None, None) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BundledPackage, CoreRuntimeBootstrapResult, ExecutorAction,
        build_preinstalled_core_bootstrap_actions, bundled_install_policy, bundled_package_drifted,
        core_instance_needs_startup_bootstrap, core_runtime_bootstrap_blocker,
        should_replace_bundled_package,
    };
    use crate::db::models::ExtensionInstance;
    use crate::extensions::manifest::ExtensionManifest;
    use crate::runtime::health::{DockerRuntimeHealthSnapshot, DockerRuntimeHealthState};
    use chrono::Utc;
    use serde_json::json;
    use std::path::PathBuf;
    use uuid::Uuid;

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

    #[test]
    fn bundled_package_drifted_does_not_force_downgrade_from_newer_installed_version() {
        assert!(!bundled_package_drifted("3.4.6", None, "1.0.2", None,));
    }

    #[test]
    fn bundled_install_policy_allows_internal_same_version_resync() {
        let policy = bundled_install_policy(true);
        assert!(policy.allow_internal_directory_install);
        assert!(policy.allow_internal_unsigned);
        assert!(policy.allow_same_version_replace);
        assert!(!policy.allow_downgrade);
    }

    #[test]
    fn bundled_install_policy_keeps_bootstrap_replace_disabled() {
        let policy = bundled_install_policy(false);
        assert!(policy.allow_internal_directory_install);
        assert!(policy.allow_internal_unsigned);
        assert!(!policy.allow_same_version_replace);
        assert!(!policy.allow_downgrade);
    }

    fn runtime_snapshot(state: DockerRuntimeHealthState) -> DockerRuntimeHealthSnapshot {
        DockerRuntimeHealthSnapshot {
            state,
            code: None,
            reason: None,
            until: None,
            host_warning: None,
            quarantined_instances: Vec::new(),
            last_failure_code: None,
            last_failure_reason: None,
            last_failure_at: None,
            last_reset_attempt_at: None,
            auto_reset_attempts_in_window: 0,
            reboot_recommended: false,
            dependency_actions_deferred_until: None,
        }
    }

    #[test]
    fn core_runtime_bootstrap_waits_while_runtime_degraded() {
        let mut snapshot = runtime_snapshot(DockerRuntimeHealthState::Degraded);
        snapshot.reason = Some("Docker daemon is unavailable".to_string());

        let blocker = core_runtime_bootstrap_blocker(&snapshot).expect("blocked");

        assert!(blocker.contains("Docker daemon is unavailable"));
    }

    #[test]
    fn core_runtime_bootstrap_waits_while_recovery_defers_dependencies() {
        let mut snapshot = runtime_snapshot(DockerRuntimeHealthState::Recovering);
        snapshot.dependency_actions_deferred_until = Some(Utc::now());

        let blocker = core_runtime_bootstrap_blocker(&snapshot).expect("blocked");

        assert!(blocker.contains("Dependency work is deferred"));
    }

    #[test]
    fn core_runtime_bootstrap_result_retries_only_runnable_failures() {
        let blocked = CoreRuntimeBootstrapResult {
            blocked_missing_secrets: 1,
            ..Default::default()
        };
        assert!(!blocked.should_retry());

        let failed = CoreRuntimeBootstrapResult {
            runnable_candidates: 1,
            failed: 1,
            ..Default::default()
        };
        assert!(failed.should_retry());

        let complete = CoreRuntimeBootstrapResult {
            runnable_candidates: 2,
            bootstrapped: 2,
            ..Default::default()
        };
        assert!(!complete.should_retry());
    }

    #[test]
    fn packaged_bundled_archive_wins_over_unpacked_manifest_at_same_version() {
        let existing = BundledPackage {
            path: PathBuf::from("flaresolverr-module"),
            version: "1.0.0".to_string(),
            package_hash: None,
        };
        let candidate = BundledPackage {
            path: PathBuf::from("flaresolverr-module.elx"),
            version: "1.0.0".to_string(),
            package_hash: Some("abcd".to_string()),
        };

        assert!(should_replace_bundled_package(&existing, &candidate));
    }

    #[test]
    fn unpacked_manifest_does_not_replace_packaged_archive_at_same_version() {
        let existing = BundledPackage {
            path: PathBuf::from("flaresolverr-module.elx"),
            version: "1.0.0".to_string(),
            package_hash: Some("abcd".to_string()),
        };
        let candidate = BundledPackage {
            path: PathBuf::from("flaresolverr-module"),
            version: "1.0.0".to_string(),
            package_hash: None,
        };

        assert!(!should_replace_bundled_package(&existing, &candidate));
    }

    #[test]
    fn core_runtime_bootstrap_actions_include_runtime_and_provider_upsert() {
        let manifest: ExtensionManifest = serde_json::from_value(json!({
            "id": "elixir.modules.qbittorrent",
            "version": "1.0.0",
            "kind": "module",
            "name": "qBittorrent",
            "provides": [
                {
                    "capability": "downloader.torrent",
                    "slot": "default",
                    "cardinality": "one",
                    "implementation": "qbittorrent"
                }
            ],
            "runtime": {
                "type": "container",
                "image": "lscr.io/linuxserver/qbittorrent:latest",
                "service_name": "svc-elixir-modules-qbittorrent-default"
            },
            "networking": {
                "service_port": { "scheme": "http", "container_port": 8080 }
            }
        }))
        .expect("manifest");

        let instance = ExtensionInstance {
            instance_id: Uuid::new_v4(),
            extension_id: "elixir.modules.qbittorrent".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            runtime_version: None,
            rollback_version: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            enabled: true,
        };

        let actions =
            build_preinstalled_core_bootstrap_actions(&instance, &manifest).expect("actions");

        assert_eq!(actions.len(), 4);
        assert!(matches!(
            actions[0],
            ExecutorAction::EnsureRuntimeRunning { .. }
        ));
        assert!(matches!(
            actions[1],
            ExecutorAction::CreateOrUpdateProvider { .. }
        ));
        assert!(matches!(actions[2], ExecutorAction::TransportGate { .. }));
        assert!(matches!(actions[3], ExecutorAction::BootstrapGate { .. }));
    }

    #[test]
    fn core_runtime_bootstrap_actions_add_full_readiness_sequence_for_non_downloaders() {
        let manifest: ExtensionManifest = serde_json::from_value(json!({
            "id": "elixir.modules.sonarr",
            "version": "1.0.0",
            "kind": "module",
            "name": "Sonarr",
            "provides": [
                {
                    "capability": "media.manager.tv",
                    "slot": "default",
                    "cardinality": "one",
                    "implementation": "sonarr"
                }
            ],
            "runtime": {
                "type": "docker",
                "image": "lscr.io/linuxserver/sonarr:latest"
            },
            "networking": {
                "service_port": { "scheme": "http", "container_port": 8989 }
            }
        }))
        .expect("manifest");

        let instance = ExtensionInstance {
            instance_id: Uuid::new_v4(),
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            runtime_version: None,
            rollback_version: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            enabled: true,
        };

        let actions =
            build_preinstalled_core_bootstrap_actions(&instance, &manifest).expect("actions");

        assert_eq!(actions.len(), 5);
        assert!(matches!(
            actions[0],
            ExecutorAction::EnsureRuntimeRunning { .. }
        ));
        assert!(matches!(
            actions[1],
            ExecutorAction::CreateOrUpdateProvider { .. }
        ));
        assert!(matches!(actions[2], ExecutorAction::TransportGate { .. }));
        assert!(matches!(actions[3], ExecutorAction::BootstrapGate { .. }));
        assert!(matches!(actions[4], ExecutorAction::HealthGate { .. }));
    }

    #[test]
    fn core_runtime_bootstrap_only_runs_for_unbootstrapped_instances() {
        let manifest: ExtensionManifest = serde_json::from_value(json!({
            "id": "elixir.modules.nzbget",
            "version": "1.0.0",
            "kind": "module",
            "name": "NZBGet",
            "provides": [
                {
                    "capability": "downloader.nzb",
                    "slot": "default",
                    "cardinality": "one",
                    "implementation": "nzbget"
                }
            ],
            "runtime": {
                "type": "container",
                "image": "lscr.io/linuxserver/nzbget:latest"
            }
        }))
        .expect("manifest");

        let bootstrapped_instance = ExtensionInstance {
            instance_id: Uuid::new_v4(),
            extension_id: "elixir.modules.nzbget".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            runtime_version: Some("1.0.0".to_string()),
            rollback_version: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            enabled: true,
        };

        assert!(!core_instance_needs_startup_bootstrap(
            &bootstrapped_instance,
            1,
            &manifest
        ));
        assert!(core_instance_needs_startup_bootstrap(
            &bootstrapped_instance,
            0,
            &manifest
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
