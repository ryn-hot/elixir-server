use std::sync::Arc;

use crate::{
    auth::AuthService,
    config::Settings,
    db::{Database, DatabaseDriver},
    extensions::ExtensionManager,
    metadata::MetadataService,
    playback::TranscodeManager,
};
use sqlx::AnyPool;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub db_pool: AnyPool,
    pub db_driver: DatabaseDriver,
    pub auth_service: AuthService,
    pub extensions: Arc<ExtensionManager>,
    pub metadata: Arc<MetadataService>,
    pub transcodes: Arc<TranscodeManager>,
    pub server_registry: Arc<RwLock<Vec<RegistryEntry>>>,
}

impl AppState {
    pub fn new(
        settings: Settings,
        database: Database,
        auth_service: AuthService,
        extensions: ExtensionManager,
        metadata: MetadataService,
    ) -> Self {
        Self {
            settings: Arc::new(settings),
            db_driver: database.driver,
            db_pool: database.pool,
            auth_service,
            extensions: Arc::new(extensions),
            metadata: Arc::new(metadata),
            transcodes: Arc::new(TranscodeManager::new()),
            server_registry: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RegistryEntry {
    pub server_id: String,
    pub device_name: String,
    pub lan_addresses: Vec<String>,
    pub wan_direct_endpoint: Option<String>,
    pub overlay_endpoint: Option<String>,
    pub status: &'static str,
}
