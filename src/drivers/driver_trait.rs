use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;

use crate::orchestrator::model::ProviderEndpoint;

#[derive(Debug, Clone)]
pub struct DriverCtx {
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub capability: String,
    pub endpoint: ProviderEndpoint,
    pub transport_base_url: Option<String>,
    pub implementation: Option<String>,
    pub instance_config: Option<serde_json::Value>,
    pub secrets: HashMap<String, String>,
}

impl DriverCtx {
    pub fn new(
        provider_id: Uuid,
        instance_id: Uuid,
        capability: String,
        endpoint: ProviderEndpoint,
        transport_base_url: Option<String>,
        implementation: Option<String>,
        instance_config: Option<serde_json::Value>,
        secrets: HashMap<String, String>,
    ) -> Self {
        Self {
            provider_id,
            instance_id,
            capability,
            endpoint,
            transport_base_url,
            implementation,
            instance_config,
            secrets,
        }
    }

    pub fn canonical_url(&self) -> Result<String> {
        if let Some(url) = self.transport_base_url.as_ref() {
            return Ok(url.clone());
        }
        self.endpoint.canonical_url()
    }

    pub fn secret(&self, key: &str) -> Option<&str> {
        self.secrets.get(key).map(String::as_str)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ActivitySnapshot {
    pub status: Option<String>,
    pub download_rate_bps: Option<u64>,
    pub upload_rate_bps: Option<u64>,
    pub active_items: Option<u64>,
    pub queued_items: Option<u64>,
    pub error_items: Option<u64>,
    pub post_process_items: Option<u64>,
    pub downloaded_bytes: Option<u64>,
    pub uploaded_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StateSnapshot {
    pub summary: Option<String>,
    pub activity: Option<ActivitySnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyStatus {
    Applied,
    Deferred,
}

#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub status: ApplyStatus,
    pub message: Option<String>,
}

impl ApplyResult {
    pub fn applied() -> Self {
        Self {
            status: ApplyStatus::Applied,
            message: None,
        }
    }

    pub fn deferred(message: impl Into<String>) -> Self {
        Self {
            status: ApplyStatus::Deferred,
            message: Some(message.into()),
        }
    }
}

#[async_trait]
pub trait CapabilityDriver: Send + Sync {
    fn capability(&self) -> &'static str;

    async fn read_state(&self, ctx: DriverCtx) -> Result<StateSnapshot>;

    async fn apply_patch(
        &self,
        ctx: DriverCtx,
        patch: crate::drivers::DriverPatch,
    ) -> Result<ApplyResult>;
}
