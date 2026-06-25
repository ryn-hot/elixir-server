use std::sync::Arc;

use crate::{
    artwork::ArtworkService,
    auth::AuthService,
    config::Settings,
    db::{Database, DatabaseDriver},
    extensions::ExtensionManager,
    library::LinkerService,
    metadata::MetadataService,
    orchestrator::OrchestratorService,
    playback::{
        PlaybackJobCapacityLimits, PlaybackJobLimits, PlaybackJobManager,
        hardware::HardwareCapabilities,
    },
    runtime::docker::DockerStartupConfig,
    secrets::SecretsManager,
};
use sqlx::AnyPool;
use std::sync::atomic::AtomicBool;
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub db_pool: AnyPool,
    pub db_driver: DatabaseDriver,
    pub auth_service: AuthService,
    pub secrets: Arc<SecretsManager>,
    pub extensions: Arc<ExtensionManager>,
    pub metadata: Arc<MetadataService>,
    pub linkers: Arc<LinkerService>,
    pub artwork: Arc<ArtworkService>,
    pub transcodes: Arc<PlaybackJobManager>,
    pub hardware_capabilities: Arc<OnceCell<HardwareCapabilities>>,
    pub mdns_active: Arc<AtomicBool>,
    pub orchestrator: Arc<OrchestratorService>,
}

impl AppState {
    pub fn new(
        settings: Settings,
        database: Database,
        auth_service: AuthService,
        extensions: ExtensionManager,
        metadata: MetadataService,
        linkers: LinkerService,
        artwork: ArtworkService,
        secrets: SecretsManager,
    ) -> Self {
        let db_pool = database.pool.clone();
        let secrets = Arc::new(secrets);
        let orchestrator = OrchestratorService::new(
            db_pool.clone(),
            settings.extensions.storage_root.clone(),
            settings.extensions.bundled_dir.clone(),
            settings.library.local_root.clone(),
            settings.extensions.core_extensions.clone(),
            settings.network.vpn.wireguard_gateway_image.clone(),
            if settings.network.vpn.enabled
                && (settings.network.vpn.auto_wrap_qbittorrent
                    || settings.network.vpn.auto_wrap_nzbget)
            {
                Some(settings.network.vpn.wireguard_config_secret.clone())
            } else {
                None
            },
            DockerStartupConfig {
                auto_start_runtime: settings.extensions.docker.auto_start_runtime,
                startup_timeout: std::time::Duration::from_secs(
                    settings.extensions.docker.startup_timeout_seconds,
                ),
                startup_poll_interval: std::time::Duration::from_millis(
                    settings.extensions.docker.startup_poll_interval_millis,
                ),
            },
            settings.extensions.downloader_profile,
            secrets.clone(),
        );
        let playback_job_limits = PlaybackJobLimits {
            max_log_bytes: settings
                .playback
                .max_ffmpeg_log_bytes
                .unwrap_or_else(|| PlaybackJobLimits::default().max_log_bytes),
            max_temp_dir_bytes: settings
                .playback
                .max_temp_dir_bytes
                .unwrap_or_else(|| PlaybackJobLimits::default().max_temp_dir_bytes),
            ..PlaybackJobLimits::default()
        };
        let playback_jobs = Arc::new(PlaybackJobManager::with_capacity_limits(
            db_pool.clone(),
            PlaybackJobCapacityLimits {
                max_hls_jobs: settings.playback.max_active_hls_jobs,
                max_direct_streams: settings.playback.max_active_direct_streams,
                max_video_transcodes: settings.playback.video_transcode_capacity_limit(),
                max_hardware_transcodes: settings.playback.max_active_hardware_transcodes,
            },
            playback_job_limits,
        ));
        Self {
            settings: Arc::new(settings),
            db_driver: database.driver,
            db_pool: db_pool.clone(),
            auth_service,
            secrets,
            extensions: Arc::new(extensions),
            metadata: Arc::new(metadata),
            linkers: Arc::new(linkers),
            artwork: Arc::new(artwork),
            transcodes: playback_jobs,
            hardware_capabilities: Arc::new(OnceCell::new()),
            mdns_active: Arc::new(AtomicBool::new(false)),
            orchestrator: Arc::new(orchestrator),
        }
    }
}
