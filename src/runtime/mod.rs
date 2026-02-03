use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::runtime::model::{ContainerHandle, ContainerSpec, ContainerState};

pub mod docker;
pub mod model;
pub mod probe;

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub data_root: String,
    pub downloads_root: String,
    pub media_root: String,
}

impl RuntimePaths {
    pub fn from_roots(storage_root: &str, media_root: &str) -> Self {
        let storage_path = Path::new(storage_root);
        let data_root_path = storage_path.parent().unwrap_or(storage_path);
        let data_root = data_root_path.to_string_lossy().to_string();
        let downloads_root = data_root_path.join("downloads").to_string_lossy().to_string();
        Self {
            data_root,
            downloads_root,
            media_root: media_root.to_string(),
        }
    }
}

#[async_trait]
pub trait RuntimeManager: Send + Sync {
    async fn ensure_network(&self, name: &str) -> anyhow::Result<()>;
    async fn ensure_container(&self, spec: &ContainerSpec) -> anyhow::Result<ContainerHandle>;
    async fn get_container_handle(&self, name: &str) -> anyhow::Result<Option<ContainerHandle>>;
    async fn start_container(&self, handle: &ContainerHandle) -> anyhow::Result<()>;
    async fn stop_container(&self, handle: &ContainerHandle) -> anyhow::Result<()>;
    async fn rename_container(
        &self,
        handle: &ContainerHandle,
        new_name: &str,
    ) -> anyhow::Result<ContainerHandle>;
    async fn remove_container(&self, handle: &ContainerHandle) -> anyhow::Result<()>;
    async fn container_logs(
        &self,
        handle: &ContainerHandle,
        since: Option<DateTime<Utc>>,
    ) -> anyhow::Result<String>;
    async fn inspect(&self, handle: &ContainerHandle) -> anyhow::Result<ContainerState>;
}
