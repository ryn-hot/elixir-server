use anyhow::{Result, bail};
use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;

use crate::db::models::MediaType;
use crate::extensions::ExternalIds;
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

#[derive(Debug, Clone)]
pub struct AddMediaRequest {
    pub media_type: MediaType,
    pub title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub options: AddMediaOptions,
}

#[derive(Debug, Clone)]
pub struct AddMediaOptions {
    pub monitor: bool,
    pub search: bool,
    pub root_folder_path: Option<String>,
    pub quality_profile_id: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct AddMediaResult {
    pub manager_item_id: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchApplyPolicy {
    PeriodicSafe,
    DesiredChangeOnly,
    ManualRepairOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchSideEffect {
    None,
    LiveApiWrite,
    ReloadService,
    RestartService,
    Destructive,
}

impl PatchSideEffect {
    pub fn is_service_disruptive(self) -> bool {
        matches!(
            self,
            PatchSideEffect::ReloadService
                | PatchSideEffect::RestartService
                | PatchSideEffect::Destructive
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSemantics {
    Comparable,
    NormalizedComparable,
    OpaqueSecret,
    WriteOnly,
    DerivedRuntime,
    ObservedOnly,
}

#[derive(Debug, Clone)]
pub struct DriftField {
    pub name: String,
    pub semantics: FieldSemantics,
}

impl DriftField {
    pub fn new(name: impl Into<String>, semantics: FieldSemantics) -> Self {
        Self {
            name: name.into(),
            semantics,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftStatus {
    InSync,
    Drifted,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct DriftEvaluation {
    pub status: DriftStatus,
    pub message: Option<String>,
    pub non_comparable_fields: Vec<DriftField>,
}

impl DriftEvaluation {
    pub fn in_sync() -> Self {
        Self {
            status: DriftStatus::InSync,
            message: None,
            non_comparable_fields: Vec::new(),
        }
    }

    pub fn drifted(message: impl Into<String>) -> Self {
        Self {
            status: DriftStatus::Drifted,
            message: Some(message.into()),
            non_comparable_fields: Vec::new(),
        }
    }

    pub fn unknown(message: impl Into<String>) -> Self {
        Self {
            status: DriftStatus::Unknown,
            message: Some(message.into()),
            non_comparable_fields: Vec::new(),
        }
    }

    pub fn with_non_comparable_fields(mut self, fields: Vec<DriftField>) -> Self {
        self.non_comparable_fields = fields;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PatchSemantics {
    pub apply_policy: PatchApplyPolicy,
    pub side_effect: PatchSideEffect,
}

impl PatchSemantics {
    pub const fn new(apply_policy: PatchApplyPolicy, side_effect: PatchSideEffect) -> Self {
        Self {
            apply_policy,
            side_effect,
        }
    }

    pub const fn periodic_safe(side_effect: PatchSideEffect) -> Self {
        Self::new(PatchApplyPolicy::PeriodicSafe, side_effect)
    }

    pub const fn desired_change_only(side_effect: PatchSideEffect) -> Self {
        Self::new(PatchApplyPolicy::DesiredChangeOnly, side_effect)
    }

    pub const fn manual_repair_only(side_effect: PatchSideEffect) -> Self {
        Self::new(PatchApplyPolicy::ManualRepairOnly, side_effect)
    }
}

#[async_trait]
pub trait CapabilityDriver: Send + Sync {
    fn capability(&self) -> &'static str;

    async fn read_state(&self, ctx: DriverCtx) -> Result<StateSnapshot>;

    // Legacy fallback: non-disruptive drivers may temporarily inherit
    // `PeriodicSafe + LiveApiWrite`, but disruptive drivers must override this
    // method explicitly before they are considered safe to auto-apply.
    fn patch_semantics(&self, _patch: &crate::drivers::DriverPatch) -> PatchSemantics {
        PatchSemantics::periodic_safe(PatchSideEffect::LiveApiWrite)
    }

    // Legacy fallback: unknown drift evaluation is acceptable only for
    // non-disruptive legacy drivers. The executor now blocks unknown drift for
    // disruptive or non-periodic patches.
    async fn evaluate_patch(
        &self,
        _ctx: DriverCtx,
        _patch: crate::drivers::DriverPatch,
    ) -> Result<DriftEvaluation> {
        Ok(DriftEvaluation::unknown(
            "driver does not implement semantic drift evaluation",
        ))
    }

    async fn apply_patch(
        &self,
        ctx: DriverCtx,
        patch: crate::drivers::DriverPatch,
    ) -> Result<ApplyResult>;

    async fn add_media(
        &self,
        _ctx: DriverCtx,
        _request: AddMediaRequest,
    ) -> Result<AddMediaResult> {
        bail!("driver does not support add_media")
    }
}
