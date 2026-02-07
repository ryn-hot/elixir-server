use std::collections::HashMap;
use std::sync::Arc;

use crate::drivers::{
    CapabilityDriver, DownloaderTorrentDriver, IndexerRegistryDriver, MediaManagerMoviesDriver,
    MediaManagerTvDriver,
};

pub struct DriverRegistry {
    drivers: HashMap<String, Arc<dyn CapabilityDriver>>,
}

impl DriverRegistry {
    pub fn new() -> Self {
        Self {
            drivers: HashMap::new(),
        }
    }

    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        registry.register(MediaManagerTvDriver::new());
        registry.register(MediaManagerMoviesDriver::new());
        registry.register(IndexerRegistryDriver::new());
        registry.register(DownloaderTorrentDriver::new());
        registry
    }

    pub fn register<D>(&mut self, driver: D)
    where
        D: CapabilityDriver + 'static,
    {
        self.drivers
            .insert(driver.capability().to_string(), Arc::new(driver));
    }

    pub fn get(&self, capability: &str) -> Option<Arc<dyn CapabilityDriver>> {
        self.drivers.get(capability).cloned()
    }
}

impl Default for DriverRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}
