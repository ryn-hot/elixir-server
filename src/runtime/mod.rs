use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::runtime::model::{
    ContainerHandle, ContainerRuntimeState, ContainerSpec, ContainerState, PrivateFileVolumeSpec,
};

pub mod docker;
pub mod health;
pub mod model;
pub mod probe;

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub data_root: String,
    pub extensions_root: String,
    pub downloads_root: String,
    pub media_root: String,
}

impl RuntimePaths {
    pub fn from_roots(storage_root: &str, media_root: &str) -> Self {
        let storage_path = absolutize_path(storage_root);
        let data_root_path = storage_path.parent().unwrap_or(&storage_path).to_path_buf();
        let data_root = data_root_path.to_string_lossy().to_string();
        let extensions_root = storage_path.to_string_lossy().to_string();
        let downloads_root = data_root_path
            .join("downloads")
            .to_string_lossy()
            .to_string();
        let media_root = absolutize_path(media_root).to_string_lossy().to_string();
        Self {
            data_root,
            extensions_root,
            downloads_root,
            media_root,
        }
    }

    pub fn deployment_id(&self) -> String {
        deployment_id_for_storage_root(&self.extensions_root)
    }
}

fn absolutize_path(raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return path;
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path,
    }
}

pub fn deployment_id_for_storage_root(storage_root: &str) -> String {
    let storage_path = absolutize_path(storage_root);
    let canonical = storage_path.to_string_lossy();
    let hash = blake3::hash(canonical.as_bytes()).to_hex().to_string();
    format!("storage-{}", &hash[..16])
}

#[async_trait]
pub trait RuntimeManager: Send + Sync {
    async fn ensure_network(&self, name: &str) -> anyhow::Result<()>;
    async fn ensure_container(&self, spec: &ContainerSpec) -> anyhow::Result<ContainerHandle>;
    async fn create_private_file_volume(
        &self,
        _spec: &PrivateFileVolumeSpec,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support private file volumes")
    }
    async fn private_file_volume_owned(
        &self,
        _name: &str,
        _required_labels: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<bool> {
        anyhow::bail!("runtime does not support private file volumes")
    }
    async fn remove_private_file_volume(
        &self,
        _name: &str,
        _required_labels: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("runtime does not support private file volumes")
    }
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
    async fn describe_container_runtime_state(
        &self,
        _container_name: &str,
    ) -> anyhow::Result<Option<ContainerRuntimeState>> {
        Ok(None)
    }
    async fn read_container_file(
        &self,
        handle: &ContainerHandle,
        path: &str,
    ) -> anyhow::Result<Option<Vec<u8>>>;
    async fn copy_host_path_to_container(
        &self,
        handle: &ContainerHandle,
        source_path: &Path,
        destination_path: &str,
    ) -> anyhow::Result<()>;
    async fn ensure_container_directories(
        &self,
        handle: &ContainerHandle,
        paths: &[String],
    ) -> anyhow::Result<()>;
    async fn ensure_container_directories_owned_like(
        &self,
        handle: &ContainerHandle,
        reference_path: &str,
        paths: &[String],
    ) -> anyhow::Result<bool>;
}
