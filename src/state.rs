use std::sync::Arc;

use crate::{
    artwork::ArtworkService,
    auth::AuthService,
    config::Settings,
    db::{Database, DatabaseDriver},
    extensions::ExtensionManager,
    library::LinkerService,
    live::service::LiveService,
    metadata::MetadataService,
    orchestrator::OrchestratorService,
    playback::{
        PlaybackJobCapacityLimits, PlaybackJobLimits, PlaybackJobManager,
        hardware::{
            HardwareCapabilities, HardwareDetectionConfig, HardwarePreference,
            collect_host_hardware_inventory, host_hardware_fingerprint,
            load_or_detect_hardware_capabilities, mark_all_hardware_readiness_stale,
        },
        jobs::HardwareFailureCallback,
        performance::PlaybackPerformanceProbeScheduler,
    },
    runtime::docker::DockerStartupConfig,
    secrets::SecretsManager,
};
use sqlx::AnyPool;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;

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
    pub hardware_capabilities: Arc<RwLock<Option<HardwareCapabilities>>>,
    pub hardware_host_fingerprint: Arc<RwLock<Option<String>>>,
    pub playback_host_fingerprint: Arc<RwLock<Option<String>>>,
    pub playback_performance_probes: Arc<PlaybackPerformanceProbeScheduler>,
    pub mdns_active: Arc<AtomicBool>,
    pub orchestrator: Arc<OrchestratorService>,
    pub live: Arc<LiveService>,
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
        let hardware_capabilities = Arc::new(RwLock::new(None));
        let hardware_host_fingerprint = Arc::new(RwLock::new(None));
        let playback_host_fingerprint = Arc::new(RwLock::new(None));
        let playback_performance_probes = Arc::new(PlaybackPerformanceProbeScheduler::new(
            settings.playback.performance_benchmark_enabled,
            settings.playback.performance_benchmark_timeout_seconds,
        ));
        let hardware_refresh_active = Arc::new(AtomicBool::new(false));
        let hardware_acceleration_enabled = settings.playback.hardware_acceleration_enabled;
        let hardware_acceleration = settings.playback.hardware_acceleration.clone();
        let hardware_failure_callback: HardwareFailureCallback = {
            let db_pool = db_pool.clone();
            let hardware_capabilities = hardware_capabilities.clone();
            let hardware_host_fingerprint = hardware_host_fingerprint.clone();
            let hardware_refresh_active = hardware_refresh_active.clone();
            Arc::new(move || {
                if !hardware_acceleration_enabled {
                    return;
                }
                if hardware_refresh_active
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return;
                }

                let db_pool = db_pool.clone();
                let hardware_capabilities = hardware_capabilities.clone();
                let hardware_host_fingerprint = hardware_host_fingerprint.clone();
                let hardware_refresh_active = hardware_refresh_active.clone();
                let hardware_acceleration = hardware_acceleration.clone();
                tokio::spawn(async move {
                    *hardware_capabilities.write().await = None;
                    *hardware_host_fingerprint.write().await = None;
                    if let Err(err) = mark_all_hardware_readiness_stale(&db_pool).await {
                        tracing::warn!(
                            error = %err,
                            "failed to mark playback hardware readiness stale after live hardware failure"
                        );
                    }
                    let config = HardwareDetectionConfig {
                        preference: HardwarePreference::parse(&hardware_acceleration),
                    };
                    match load_or_detect_hardware_capabilities(&db_pool, &config).await {
                        Ok(capabilities) => {
                            let inventory = collect_host_hardware_inventory().await;
                            let host_fingerprint = host_hardware_fingerprint(&inventory);
                            let available_apis = capabilities.available_apis.clone();
                            *hardware_capabilities.write().await = Some(capabilities);
                            *hardware_host_fingerprint.write().await = Some(host_fingerprint);
                            tracing::info!(
                                available_apis = ?available_apis,
                                "playback hardware readiness refreshed after live hardware failure"
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "playback hardware readiness refresh failed after live hardware failure"
                            );
                        }
                    }
                    hardware_refresh_active.store(false, Ordering::Release);
                });
            })
        };
        let playback_jobs = Arc::new(
            PlaybackJobManager::with_capacity_limits_and_hardware_failure_callback(
                db_pool.clone(),
                PlaybackJobCapacityLimits {
                    max_hls_jobs: settings.playback.max_active_hls_jobs,
                    max_direct_streams: settings.playback.max_active_direct_streams,
                    max_video_transcodes: settings.playback.video_transcode_capacity_limit(),
                    max_hardware_transcodes: settings.playback.max_active_hardware_transcodes,
                },
                playback_job_limits,
                Some(hardware_failure_callback),
            ),
        );
        let live = Arc::new(LiveService::new_with_runtime(
            settings.live.clone(),
            settings.environment.clone(),
            db_pool.clone(),
            secrets.clone(),
            orchestrator.runtime_manager(),
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
            hardware_capabilities,
            hardware_host_fingerprint,
            playback_host_fingerprint,
            playback_performance_probes,
            mdns_active: Arc::new(AtomicBool::new(false)),
            orchestrator: Arc::new(orchestrator),
            live,
        }
    }
}
