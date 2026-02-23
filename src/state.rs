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
    playback::TranscodeManager,
    secrets::SecretsManager,
};
use sqlx::AnyPool;
use std::sync::atomic::AtomicBool;

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
    pub transcodes: Arc<TranscodeManager>,
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
            settings.library.local_root.clone(),
            secrets.clone(),
        );
        Self {
            settings: Arc::new(settings),
            db_driver: database.driver,
            db_pool,
            auth_service,
            secrets,
            extensions: Arc::new(extensions),
            metadata: Arc::new(metadata),
            linkers: Arc::new(linkers),
            artwork: Arc::new(artwork),
            transcodes: Arc::new(TranscodeManager::new()),
            mdns_active: Arc::new(AtomicBool::new(false)),
            orchestrator: Arc::new(orchestrator),
        }
    }
}
