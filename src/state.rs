use std::sync::Arc;

use crate::{
    auth::AuthService,
    artwork::ArtworkService,
    config::Settings,
    db::{Database, DatabaseDriver},
    extensions::ExtensionManager,
    library::LinkerService,
    metadata::MetadataService,
    playback::TranscodeManager,
};
use sqlx::AnyPool;
use std::sync::atomic::AtomicBool;

#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub db_pool: AnyPool,
    pub db_driver: DatabaseDriver,
    pub auth_service: AuthService,
    pub extensions: Arc<ExtensionManager>,
    pub metadata: Arc<MetadataService>,
    pub linkers: Arc<LinkerService>,
    pub artwork: Arc<ArtworkService>,
    pub transcodes: Arc<TranscodeManager>,
    pub mdns_active: Arc<AtomicBool>,
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
    ) -> Self {
        Self {
            settings: Arc::new(settings),
            db_driver: database.driver,
            db_pool: database.pool,
            auth_service,
            extensions: Arc::new(extensions),
            metadata: Arc::new(metadata),
            linkers: Arc::new(linkers),
            artwork: Arc::new(artwork),
            transcodes: Arc::new(TranscodeManager::new()),
            mdns_active: Arc::new(AtomicBool::new(false)),
        }
    }
}
