mod auth;
mod config;
mod db;
mod extensions;
mod http;
mod library;
mod media;
mod metadata;
mod network;
mod playback;
mod state;
mod telemetry;

use crate::auth::AuthService;
use crate::config::Settings;
use crate::db::Database;
use crate::extensions::ExtensionManager;
use crate::http::router;
use crate::library::start_periodic_scan;
use crate::metadata::MetadataService;
use crate::network::start_mdns;
use crate::playback::start_session_cleanup;
use crate::state::AppState;
use anyhow::Context;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from a local .env file when present for developer convenience.
    dotenvy::dotenv().ok();

    let settings = Settings::load().context("failed to load configuration")?;
    telemetry::init_tracing(&settings.telemetry).context("failed to initialize tracing")?;

    let database = Database::connect(&settings.database)
        .await
        .context("failed to initialize database pool")?;
    database
        .run_migrations()
        .await
        .context("database migrations failed")?;

    let auth_service =
        AuthService::new(settings.auth.clone()).context("failed to initialize auth service")?;
    let extensions = ExtensionManager::load_from_dir("extensions", &settings.library.local_root)
        .await
        .unwrap_or_else(|err| {
            tracing::warn!("failed to load extensions directory: {err}");
            ExtensionManager::new()
        });
    let metadata = MetadataService::new(settings.metadata.clone())
        .context("failed to initialize metadata service")?;

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
    );
    let app = router(state.clone());

    // Kick off background periodic scan.
    let scan_interval = settings.library.scan_interval_seconds;
    let scan_state = state.clone();
    tokio::spawn(async move { start_periodic_scan(scan_state, scan_interval).await });

    // Announce via mDNS if enabled.
    if state.settings.network.mdns_enabled {
        match start_mdns(&state.settings.server, &state.settings.network.mdns_name) {
            Ok(_) => {
                tracing::info!(
                    "mDNS announced: {}:{} ({})",
                    state.settings.server.host,
                    state.settings.server.port,
                    state.settings.network.mdns_name
                );
            }
            Err(err) => {
                tracing::warn!("failed to start mDNS announcer: {err}");
            }
        }
    }

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
