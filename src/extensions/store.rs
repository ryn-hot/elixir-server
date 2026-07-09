use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{AnyPool, QueryBuilder, Row, TypeInfo, Value, ValueRef, any::AnyRow};
use std::time::Duration;
use uuid::Uuid;

use crate::db::models::{
    Binding, BindingStatus, DesiredBlueprint, Extension, ExtensionInstance, ExtensionKind,
    ExtensionTrustLevel, OperationStep, OperationStepStatus, OrchestratorRun,
    OrchestratorRunStatus, Provider, ProviderHealthState, ProviderReadiness,
    ProviderReadinessPhase, RuntimeLog, Secret, SecretScope, SlotCardinality,
};
use crate::extensions::ExternalIds;

#[derive(Debug, Clone)]
pub struct NewExtension {
    pub extension_id: String,
    pub name: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub publisher_name: Option<String>,
    pub signing_key_id: Option<String>,
    pub trust_level: ExtensionTrustLevel,
    pub manifest_json: serde_json::Value,
    pub package_hash: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewExtensionInstance {
    pub instance_id: Uuid,
    pub extension_id: String,
    pub instance_name: String,
    pub config_json: Option<serde_json::Value>,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewProvider {
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub capability: String,
    pub slot_id: String,
    pub cardinality: SlotCardinality,
    pub implementation: Option<String>,
    pub scope_json: Option<serde_json::Value>,
    pub endpoint_json: Option<serde_json::Value>,
    pub health_state: ProviderHealthState,
}

#[derive(Debug, Clone)]
pub struct ProviderDetails {
    pub provider: Provider,
    pub extension_id: String,
    pub trust_level: ExtensionTrustLevel,
}

#[derive(Debug, Clone)]
pub struct ExtensionSettingRecord {
    pub setting_key: String,
    pub value_json: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewBinding {
    pub binding_id: Uuid,
    pub consumer_provider_id: Uuid,
    pub requires_capability: String,
    pub requires_slot_id: String,
    pub target_provider_id: Uuid,
    pub binding_params_json: Option<serde_json::Value>,
    pub status: BindingStatus,
}

#[derive(Debug, Clone)]
pub struct NewDesiredBlueprint {
    pub desired_id: Uuid,
    pub blueprint_extension_id: String,
    pub blueprint_version: String,
    pub params_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct NewManagedIngestIntent {
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Uuid,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct ManagedIngestIntent {
    pub intent_id: Uuid,
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Uuid,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub source: String,
    pub active: bool,
    pub last_matched_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedImportFile {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absolute_episode_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_codec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_codec: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewManagedImportEvent {
    pub event_key: String,
    pub intent_id: Uuid,
    pub media_type: crate::db::models::MediaType,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Uuid,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub imported_files: Vec<ManagedImportFile>,
    pub raw_manager_payload: Option<serde_json::Value>,
    pub imported_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ManagedImportEvent {
    pub event_id: Uuid,
    pub event_key: String,
    pub intent_id: Uuid,
    pub media_type: crate::db::models::MediaType,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Uuid,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub imported_files: Vec<ManagedImportFile>,
    pub raw_manager_payload: Option<serde_json::Value>,
    pub status: String,
    pub linked_media_item_id: Option<Uuid>,
    pub last_error: Option<String>,
    pub imported_at: Option<DateTime<Utc>>,
    pub processed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewManagedLibraryProvenance {
    pub media_item_id: Uuid,
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Uuid,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub intent_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct ManagedLibraryProvenance {
    pub media_item_id: Uuid,
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Uuid,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub intent_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewMediaOwnership {
    pub ownership_id: Uuid,
    pub media_item_id: Uuid,
    pub owner_type: String,
    pub owner_role: String,
    pub owner_label: Option<String>,
    pub owner_implementation: Option<String>,
    pub owner_provider_id: Option<Uuid>,
    pub owner_instance_id: Option<Uuid>,
    pub owner_extension_id: Option<String>,
    pub owner_external_id: Option<String>,
    pub acquisition_subscription_id: Option<Uuid>,
    pub acquisition_target_scope: Option<serde_json::Value>,
    pub release_capability: String,
    pub release_policy: String,
    pub metadata: Option<serde_json::Value>,
    pub active: bool,
}

#[derive(Debug, Clone)]
pub struct MediaOwnership {
    pub ownership_id: Uuid,
    pub media_item_id: Uuid,
    pub owner_type: String,
    pub owner_role: String,
    pub owner_label: Option<String>,
    pub owner_implementation: Option<String>,
    pub owner_provider_id: Option<Uuid>,
    pub owner_instance_id: Option<Uuid>,
    pub owner_extension_id: Option<String>,
    pub owner_external_id: Option<String>,
    pub acquisition_subscription_id: Option<Uuid>,
    pub acquisition_target_scope: Option<serde_json::Value>,
    pub release_capability: String,
    pub release_policy: String,
    pub metadata: Option<serde_json::Value>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewMediaOwnerReleaseEvent {
    pub release_event_id: Uuid,
    pub media_item_id: Option<Uuid>,
    pub ownership_id: Option<Uuid>,
    pub requested_action: String,
    pub owner_type: String,
    pub owner_label: Option<String>,
    pub owner_provider_id: Option<Uuid>,
    pub acquisition_subscription_id: Option<Uuid>,
    pub status: String,
    pub status_reason: Option<String>,
    pub request: Option<serde_json::Value>,
    pub response: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct MediaOwnerReleaseEvent {
    pub release_event_id: Uuid,
    pub media_item_id: Option<Uuid>,
    pub ownership_id: Option<Uuid>,
    pub requested_action: String,
    pub owner_type: String,
    pub owner_label: Option<String>,
    pub owner_provider_id: Option<Uuid>,
    pub acquisition_subscription_id: Option<Uuid>,
    pub status: String,
    pub status_reason: Option<String>,
    pub request: Option<serde_json::Value>,
    pub response: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MediaOwnershipReconcileReport {
    pub external_owners_created: usize,
    pub stale_owners_marked_unsupported: usize,
    pub unsupported_events_created: usize,
}

#[derive(Debug, Clone)]
pub struct NewManagedMediaTombstone {
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Option<Uuid>,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub action: String,
}

#[derive(Debug, Clone)]
pub struct ManagedMediaTombstone {
    pub tombstone_id: Uuid,
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Option<Uuid>,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub action: String,
    pub active: bool,
    pub cleared_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewManagedEpisodeTombstone {
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Option<Uuid>,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub season_number: i32,
    pub episode_number: i32,
    pub absolute_episode_number: Option<i32>,
    pub action: String,
}

#[derive(Debug, Clone)]
pub struct ManagedEpisodeTombstone {
    pub tombstone_id: Uuid,
    pub media_type: crate::db::models::MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Option<Uuid>,
    pub manager_item_id: Option<String>,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub season_number: i32,
    pub episode_number: i32,
    pub absolute_episode_number: Option<i32>,
    pub action: String,
    pub active: bool,
    pub cleared_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewSecret {
    pub secret_id: Uuid,
    pub scope: SecretScope,
    pub scope_id: Option<Uuid>,
    pub key: String,
    pub value_encrypted: String,
    pub rotatable: bool,
}

#[derive(Debug, Clone)]
pub struct NewOrchestratorRun {
    pub run_id: Uuid,
    pub source: String,
    pub status: OrchestratorRunStatus,
    pub phase: Option<String>,
    pub plan_json: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewOperationStep {
    pub step_id: Uuid,
    pub run_id: Uuid,
    pub step_index: i32,
    pub action_type: String,
    pub action_json: Option<serde_json::Value>,
    pub status: OperationStepStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewRuntimeLog {
    pub log_id: Uuid,
    pub instance_id: Uuid,
    pub log_uri: String,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceRegistry {
    pub registry_id: Uuid,
    pub instance_id: Uuid,
    pub registry_key: String,
    pub registry_type: String,
    pub trust_class: String,
    pub display_name: String,
    pub url: Option<String>,
    pub enabled: bool,
    pub auto_refresh: bool,
    pub trusted_for_executable_updates: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ExtensionSourceRegistry {
    pub registry_id: Uuid,
    pub instance_id: Uuid,
    pub registry_key: String,
    pub registry_type: String,
    pub trust_class: String,
    pub display_name: String,
    pub url: Option<String>,
    pub enabled: bool,
    pub auto_refresh: bool,
    pub trusted_for_executable_updates: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub last_fetch_status: String,
    pub last_fetch_error: Option<String>,
    pub last_fetched_at: Option<DateTime<Utc>>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceModule {
    pub source_module_id: Uuid,
    pub instance_id: Uuid,
    pub registry_id: Uuid,
    pub module_key: String,
    pub display_name: String,
    pub ecosystem: String,
    pub plugin_package: Option<String>,
    pub active_version: Option<String>,
    pub rollback_version: Option<String>,
    pub media_types_json: Option<serde_json::Value>,
    pub language_tags_json: Option<serde_json::Value>,
    pub region_tags_json: Option<serde_json::Value>,
    pub source_domains_json: Option<serde_json::Value>,
    pub account_required: bool,
    pub unsupported: bool,
    pub unsupported_reason: Option<String>,
    pub enabled: bool,
    pub installed: bool,
    pub pinned_version: Option<String>,
    pub health_state: String,
    pub replacement_recommendation_key: Option<String>,
    pub last_error: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ExtensionSourceModule {
    pub source_module_id: Uuid,
    pub instance_id: Uuid,
    pub registry_id: Uuid,
    pub module_key: String,
    pub display_name: String,
    pub ecosystem: String,
    pub plugin_package: Option<String>,
    pub active_version: Option<String>,
    pub rollback_version: Option<String>,
    pub media_types_json: Option<serde_json::Value>,
    pub language_tags_json: Option<serde_json::Value>,
    pub region_tags_json: Option<serde_json::Value>,
    pub source_domains_json: Option<serde_json::Value>,
    pub account_required: bool,
    pub unsupported: bool,
    pub unsupported_reason: Option<String>,
    pub enabled: bool,
    pub installed: bool,
    pub pinned_version: Option<String>,
    pub health_state: String,
    pub replacement_recommendation_key: Option<String>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceModuleVersion {
    pub version_id: Uuid,
    pub source_module_id: Uuid,
    pub version: String,
    pub artifact_url: Option<String>,
    pub artifact_sha256: Option<String>,
    pub signature: Option<String>,
    pub install_state: String,
    pub smoke_status: String,
    pub smoke_error: Option<String>,
    pub rollback_of_version_id: Option<Uuid>,
    pub installed_at: Option<DateTime<Utc>>,
    pub activated_at: Option<DateTime<Utc>>,
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ExtensionSourceModuleVersion {
    pub version_id: Uuid,
    pub source_module_id: Uuid,
    pub version: String,
    pub artifact_url: Option<String>,
    pub artifact_sha256: Option<String>,
    pub signature: Option<String>,
    pub install_state: String,
    pub smoke_status: String,
    pub smoke_error: Option<String>,
    pub rollback_of_version_id: Option<Uuid>,
    pub installed_at: Option<DateTime<Utc>>,
    pub activated_at: Option<DateTime<Utc>>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceHealthEvent {
    pub health_event_id: Uuid,
    pub source_module_id: Uuid,
    pub event_type: String,
    pub state: String,
    pub severity: String,
    pub reason: Option<String>,
    pub evidence_json: Option<serde_json::Value>,
    pub observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ExtensionSourceHealthEvent {
    pub health_event_id: Uuid,
    pub source_module_id: Uuid,
    pub event_type: String,
    pub state: String,
    pub severity: String,
    pub reason: Option<String>,
    pub evidence_json: Option<serde_json::Value>,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceModuleCertification {
    pub certification_id: Uuid,
    pub source_module_id: Uuid,
    pub source_module_version_id: Option<Uuid>,
    pub artifact_sha256: Option<String>,
    pub instance_id: Uuid,
    pub adapter: String,
    pub status: String,
    pub failure_class: Option<String>,
    pub summary: Option<String>,
    pub media_type_results_json: serde_json::Value,
    pub materialization_results_json: serde_json::Value,
    pub probe_targets_json: serde_json::Value,
    pub candidate_evidence_json: serde_json::Value,
    pub runtime_version: Option<String>,
    pub policy_version: String,
    pub certified_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ExtensionSourceModuleCertification {
    pub certification_id: Uuid,
    pub source_module_id: Uuid,
    pub source_module_version_id: Option<Uuid>,
    pub artifact_sha256: Option<String>,
    pub instance_id: Uuid,
    pub adapter: String,
    pub status: String,
    pub failure_class: Option<String>,
    pub summary: Option<String>,
    pub media_type_results_json: serde_json::Value,
    pub materialization_results_json: serde_json::Value,
    pub probe_targets_json: serde_json::Value,
    pub candidate_evidence_json: serde_json::Value,
    pub runtime_version: Option<String>,
    pub policy_version: String,
    pub certified_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceCertificationJob {
    pub job_id: Uuid,
    pub instance_id: Uuid,
    pub registry_id: Option<Uuid>,
    pub source_module_id: Option<Uuid>,
    pub requested_by: String,
    pub reason: String,
    pub status: String,
    pub priority: i64,
    pub attempts: i64,
    pub max_attempts: i64,
    pub language_eligibility: Option<String>,
    pub marketplace_state: Option<String>,
    pub summary: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExtensionSourceCertificationJob {
    pub job_id: Uuid,
    pub instance_id: Uuid,
    pub registry_id: Option<Uuid>,
    pub source_module_id: Option<Uuid>,
    pub requested_by: String,
    pub reason: String,
    pub status: String,
    pub priority: i64,
    pub attempts: i64,
    pub max_attempts: i64,
    pub language_eligibility: Option<String>,
    pub marketplace_state: Option<String>,
    pub summary: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceModuleQuarantine {
    pub quarantine_id: Uuid,
    pub source_module_id: Uuid,
    pub source_module_version_id: Option<Uuid>,
    pub instance_id: Uuid,
    pub failure_class: String,
    pub hoster_domain: Option<String>,
    pub candidate_fingerprint: Option<String>,
    pub media_type: Option<String>,
    pub reason: Option<String>,
    pub evidence_json: Option<serde_json::Value>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewExtensionSourceReplacementRecommendation {
    pub recommendation_id: Uuid,
    pub source_module_id: Uuid,
    pub replacement_source_module_id: Option<Uuid>,
    pub replacement_registry_id: Option<Uuid>,
    pub recommendation_key: String,
    pub action: String,
    pub recommended_version: Option<String>,
    pub reason: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
    pub active: bool,
}

#[derive(Debug, Clone)]
pub struct ExtensionSourceReplacementRecommendation {
    pub recommendation_id: Uuid,
    pub source_module_id: Uuid,
    pub replacement_source_module_id: Option<Uuid>,
    pub replacement_registry_id: Option<Uuid>,
    pub recommendation_key: String,
    pub action: String,
    pub recommended_version: Option<String>,
    pub reason: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
    pub active: bool,
    pub applied_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ProviderOwnershipContext {
    instance_id: Option<Uuid>,
    implementation: Option<String>,
    extension_id: Option<String>,
}

#[derive(Debug, Clone)]
struct AcquisitionOwnershipContext {
    subscription_id: Uuid,
    source_provider_id: Option<Uuid>,
    source_extension_id: Option<String>,
}

#[derive(Debug, Clone)]
struct MediaItemOwnershipIdentity {
    media_type: crate::db::models::MediaType,
    title: String,
    year: Option<i32>,
    external_ids: Option<ExternalIds>,
}

pub struct ExtensionStore<'a> {
    pool: &'a AnyPool,
}

impl<'a> ExtensionStore<'a> {
    pub fn new(pool: &'a AnyPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_extension(&self, data: &NewExtension) -> Result<()> {
        let manifest_json =
            serde_json::to_string(&data.manifest_json).context("serializing manifest json")?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extensions (extension_id, name, version, kind, publisher_name, signing_key_id, trust_level, manifest_json, package_hash, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \n             ON CONFLICT(extension_id) DO UPDATE SET name = excluded.name, version = excluded.version, kind = excluded.kind, publisher_name = excluded.publisher_name, signing_key_id = excluded.signing_key_id, trust_level = excluded.trust_level, manifest_json = excluded.manifest_json, package_hash = excluded.package_hash, enabled = excluded.enabled",
        )
        .bind(&data.extension_id)
        .bind(&data.name)
        .bind(&data.version)
        .bind(data.kind.as_str())
        .bind(data.publisher_name.as_deref())
        .bind(data.signing_key_id.as_deref())
        .bind(data.trust_level.as_str())
        .bind(manifest_json)
        .bind(data.package_hash.as_deref())
        .bind(data.enabled)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_extensions(&self) -> Result<Vec<Extension>> {
        let rows = sqlx::query(
            "SELECT extension_id, name, version, kind, CAST(publisher_name AS TEXT) as publisher_name, CAST(signing_key_id AS TEXT) as signing_key_id, trust_level, manifest_json, CAST(package_hash AS TEXT) as package_hash, CAST(installed_at AS TEXT) as installed_at, CAST(enabled AS INTEGER) as enabled FROM extensions ORDER BY installed_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_extension(&row)?);
        }
        Ok(items)
    }

    pub async fn get_extension(&self, extension_id: &str) -> Result<Option<Extension>> {
        let row = sqlx::query(
            "SELECT extension_id, name, version, kind, CAST(publisher_name AS TEXT) as publisher_name, CAST(signing_key_id AS TEXT) as signing_key_id, trust_level, manifest_json, CAST(package_hash AS TEXT) as package_hash, CAST(installed_at AS TEXT) as installed_at, CAST(enabled AS INTEGER) as enabled FROM extensions WHERE extension_id = ? LIMIT 1",
        )
        .bind(extension_id)
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_extension(&row)).transpose()
    }

    pub async fn set_extension_enabled(&self, extension_id: &str, enabled: bool) -> Result<()> {
        sqlx::query::<sqlx::Any>("UPDATE extensions SET enabled = ? WHERE extension_id = ?")
            .bind(enabled)
            .bind(extension_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_extension(&self, extension_id: &str) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM extensions WHERE extension_id = ?")
            .bind(extension_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn create_instance(&self, data: &NewExtensionInstance) -> Result<()> {
        let config_json = json_to_string(data.config_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_instances (instance_id, extension_id, instance_name, config_json, enabled) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(data.instance_id.to_string())
        .bind(&data.extension_id)
        .bind(&data.instance_name)
        .bind(config_json)
        .bind(data.enabled)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_instances(
        &self,
        extension_id: Option<&str>,
    ) -> Result<Vec<ExtensionInstance>> {
        let rows = if let Some(extension_id) = extension_id {
            sqlx::query(
            "SELECT instance_id, extension_id, instance_name, CAST(config_json AS TEXT) as config_json, CAST(runtime_version AS TEXT) as runtime_version, CAST(rollback_version AS TEXT) as rollback_version, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at, CAST(enabled AS INTEGER) as enabled FROM extension_instances WHERE extension_id = ? ORDER BY created_at DESC",
            )
            .bind(extension_id)
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT instance_id, extension_id, instance_name, CAST(config_json AS TEXT) as config_json, CAST(runtime_version AS TEXT) as runtime_version, CAST(rollback_version AS TEXT) as rollback_version, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at, CAST(enabled AS INTEGER) as enabled FROM extension_instances ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_extension_instance(&row)?);
        }
        Ok(items)
    }

    pub async fn get_instance(&self, instance_id: Uuid) -> Result<Option<ExtensionInstance>> {
        let row = sqlx::query(
            "SELECT instance_id, extension_id, instance_name, CAST(config_json AS TEXT) as config_json, CAST(runtime_version AS TEXT) as runtime_version, CAST(rollback_version AS TEXT) as rollback_version, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at, CAST(enabled AS INTEGER) as enabled FROM extension_instances WHERE instance_id = ? LIMIT 1",
        )
        .bind(instance_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_extension_instance(&row)).transpose()
    }

    pub async fn rename_instance(&self, instance_id: Uuid, instance_name: &str) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET instance_name = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(instance_name)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_instance_config(
        &self,
        instance_id: Uuid,
        config_json: Option<&serde_json::Value>,
    ) -> Result<()> {
        let config_json = json_to_string(config_json)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET config_json = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(config_json)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_instance_enabled(&self, instance_id: Uuid, enabled: bool) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(enabled)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_instance_runtime_version(
        &self,
        instance_id: Uuid,
        runtime_version: &str,
        rollback_version: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances SET runtime_version = ?, rollback_version = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?",
        )
        .bind(runtime_version)
        .bind(rollback_version)
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_instance(&self, instance_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM extension_instances WHERE instance_id = ?")
            .bind(instance_id.to_string())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn prune_stale_suffix_instances(
        &self,
        extension_id: &str,
        primary_instance_name: &str,
        stale_before: DateTime<Utc>,
    ) -> Result<u64> {
        let stale_str = stale_before.format("%Y-%m-%d %H:%M:%S").to_string();
        let result = sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_instances
             WHERE extension_id = ?
               AND instance_name LIKE ?
               AND updated_at < ?
               AND COALESCE(NULLIF(TRIM(CAST(config_json AS TEXT)), 'null'), '') = ''
               AND runtime_version IS NULL
               AND rollback_version IS NULL
               AND NOT EXISTS (
                   SELECT 1
                   FROM providers
                   WHERE providers.instance_id = extension_instances.instance_id
               )
               AND NOT EXISTS (
                   SELECT 1
                   FROM secrets
                   WHERE secrets.scope = 'instance'
                     AND secrets.scope_id = extension_instances.instance_id
               )
               AND EXISTS (
                   SELECT 1
                   FROM extension_instances AS primary_instance
                   JOIN providers AS primary_provider
                     ON primary_provider.instance_id = primary_instance.instance_id
                   WHERE primary_instance.extension_id = extension_instances.extension_id
                     AND primary_instance.instance_name = ?
               )",
        )
        .bind(extension_id)
        .bind(format!("{primary_instance_name}-%"))
        .bind(stale_str)
        .bind(primary_instance_name)
        .execute(self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn delete_secrets_by_scope(
        &self,
        scope: SecretScope,
        scope_id: Option<Uuid>,
    ) -> Result<()> {
        match scope_id {
            Some(scope_id) => {
                sqlx::query::<sqlx::Any>("DELETE FROM secrets WHERE scope = ? AND scope_id = ?")
                    .bind(scope.as_str())
                    .bind(scope_id.to_string())
                    .execute(self.pool)
                    .await?;
            }
            None => {
                sqlx::query::<sqlx::Any>(
                    "DELETE FROM secrets WHERE scope = ? AND scope_id IS NULL",
                )
                .bind(scope.as_str())
                .execute(self.pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn prune_orphaned_instance_secrets(&self) -> Result<u64> {
        let result = sqlx::query::<sqlx::Any>(
            "DELETE FROM secrets
             WHERE scope = 'instance'
               AND scope_id IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1
                   FROM extension_instances
                   WHERE extension_instances.instance_id = secrets.scope_id
               )",
        )
        .execute(self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn upsert_provider(&self, data: &NewProvider) -> Result<()> {
        let scope_json = json_to_string(data.scope_json.as_ref())?;
        let endpoint_json = json_to_string(data.endpoint_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO providers (provider_id, instance_id, capability, slot_id, cardinality, implementation, scope_json, endpoint_json, health_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \n             ON CONFLICT(instance_id, capability, slot_id) DO UPDATE SET cardinality = excluded.cardinality, implementation = excluded.implementation, scope_json = excluded.scope_json, endpoint_json = excluded.endpoint_json, health_state = excluded.health_state, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.provider_id.to_string())
        .bind(data.instance_id.to_string())
        .bind(&data.capability)
        .bind(&data.slot_id)
        .bind(data.cardinality.as_str())
        .bind(data.implementation.as_deref())
        .bind(scope_json)
        .bind(endpoint_json)
        .bind(data.health_state.as_str())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO provider_readiness (provider_id, readiness_phase) VALUES (?, ?) \
             ON CONFLICT(provider_id) DO NOTHING",
        )
        .bind(data.provider_id.to_string())
        .bind(ProviderReadinessPhase::Unknown.as_str())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_providers(&self, instance_id: Option<Uuid>) -> Result<Vec<Provider>> {
        let rows = if let Some(instance_id) = instance_id {
            sqlx::query(
                "SELECT provider_id, instance_id, capability, slot_id, cardinality, CAST(implementation AS TEXT) as implementation, CAST(scope_json AS TEXT) as scope_json, CAST(endpoint_json AS TEXT) as endpoint_json, health_state, CAST(last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM providers WHERE instance_id = ? ORDER BY created_at DESC",
            )
            .bind(instance_id.to_string())
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT provider_id, instance_id, capability, slot_id, cardinality, CAST(implementation AS TEXT) as implementation, CAST(scope_json AS TEXT) as scope_json, CAST(endpoint_json AS TEXT) as endpoint_json, health_state, CAST(last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM providers ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_provider(&row)?);
        }
        Ok(items)
    }

    pub async fn get_provider(&self, provider_id: Uuid) -> Result<Option<Provider>> {
        let row = sqlx::query(
            "SELECT provider_id, instance_id, capability, slot_id, cardinality, CAST(implementation AS TEXT) as implementation, CAST(scope_json AS TEXT) as scope_json, CAST(endpoint_json AS TEXT) as endpoint_json, health_state, CAST(last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM providers WHERE provider_id = ? LIMIT 1",
        )
        .bind(provider_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_provider(&row)).transpose()
    }

    pub async fn list_provider_details(&self) -> Result<Vec<ProviderDetails>> {
        let rows = sqlx::query(
            "SELECT p.provider_id, p.instance_id, p.capability, p.slot_id, p.cardinality, CAST(p.implementation AS TEXT) as implementation, CAST(p.scope_json AS TEXT) as scope_json, CAST(p.endpoint_json AS TEXT) as endpoint_json, p.health_state, CAST(p.last_healthcheck_at AS TEXT) as last_healthcheck_at, CAST(p.created_at AS TEXT) as created_at, CAST(p.updated_at AS TEXT) as updated_at, i.extension_id as extension_id, e.trust_level as trust_level FROM providers p JOIN extension_instances i ON p.instance_id = i.instance_id JOIN extensions e ON i.extension_id = e.extension_id ORDER BY p.created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_provider_detail(&row)?);
        }
        Ok(items)
    }

    pub async fn update_provider_health(
        &self,
        provider_id: Uuid,
        health_state: ProviderHealthState,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE providers SET health_state = ?, last_healthcheck_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE provider_id = ?",
        )
        .bind(health_state.as_str())
        .bind(provider_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_provider_readiness(
        &self,
        provider_id: Uuid,
    ) -> Result<Option<ProviderReadiness>> {
        let row = sqlx::query(
            "SELECT provider_id, readiness_phase, CAST(readiness_detail AS TEXT) as readiness_detail, CAST(last_checked_at AS TEXT) as last_checked_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM provider_readiness WHERE provider_id = ? LIMIT 1",
        )
        .bind(provider_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_provider_readiness(&row)).transpose()
    }

    pub async fn list_provider_readiness(&self) -> Result<Vec<ProviderReadiness>> {
        let rows = sqlx::query(
            "SELECT provider_id, readiness_phase, CAST(readiness_detail AS TEXT) as readiness_detail, CAST(last_checked_at AS TEXT) as last_checked_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM provider_readiness ORDER BY created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_provider_readiness(&row)?);
        }
        Ok(items)
    }

    pub async fn upsert_provider_readiness(
        &self,
        provider_id: Uuid,
        readiness_phase: ProviderReadinessPhase,
        readiness_detail: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO provider_readiness (provider_id, readiness_phase, readiness_detail, last_checked_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP) \
             ON CONFLICT(provider_id) DO UPDATE SET readiness_phase = excluded.readiness_phase, readiness_detail = excluded.readiness_detail, last_checked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(provider_id.to_string())
        .bind(readiness_phase.as_str())
        .bind(readiness_detail)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_provider(&self, provider_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM providers WHERE provider_id = ?")
            .bind(provider_id.to_string())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_binding(&self, data: &NewBinding) -> Result<()> {
        let binding_params = json_to_string(data.binding_params_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO bindings (binding_id, consumer_provider_id, requires_capability, requires_slot_id, target_provider_id, binding_params_json, status) VALUES (?, ?, ?, ?, ?, ?, ?) \n             ON CONFLICT(consumer_provider_id, requires_capability, requires_slot_id, target_provider_id) DO UPDATE SET binding_params_json = excluded.binding_params_json, status = excluded.status, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.binding_id.to_string())
        .bind(data.consumer_provider_id.to_string())
        .bind(&data.requires_capability)
        .bind(&data.requires_slot_id)
        .bind(data.target_provider_id.to_string())
        .bind(binding_params)
        .bind(data.status.as_str())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_bindings(&self) -> Result<Vec<Binding>> {
        let rows = sqlx::query(
            "SELECT binding_id, consumer_provider_id, requires_capability, requires_slot_id, target_provider_id, CAST(binding_params_json AS TEXT) as binding_params_json, status, CAST(last_error AS TEXT) as last_error, CAST(last_applied_at AS TEXT) as last_applied_at, CAST(created_at AS TEXT) as created_at, CAST(updated_at AS TEXT) as updated_at FROM bindings ORDER BY created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_binding(&row)?);
        }
        Ok(items)
    }

    pub async fn update_binding_status(
        &self,
        binding_id: Uuid,
        status: BindingStatus,
        last_error: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE bindings SET status = ?, last_error = ?, last_applied_at = CASE WHEN ? = 'applied' THEN CURRENT_TIMESTAMP ELSE last_applied_at END, updated_at = CURRENT_TIMESTAMP WHERE binding_id = ?",
        )
        .bind(status.as_str())
        .bind(last_error)
        .bind(status.as_str())
        .bind(binding_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_desired_blueprint(&self, data: &NewDesiredBlueprint) -> Result<()> {
        let params_json = json_to_string(data.params_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO desired_blueprints (desired_id, blueprint_extension_id, blueprint_version, params_json, applied) VALUES (?, ?, ?, ?, 0)",
        )
        .bind(data.desired_id.to_string())
        .bind(&data.blueprint_extension_id)
        .bind(&data.blueprint_version)
        .bind(params_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_desired_blueprint(&self, data: &NewDesiredBlueprint) -> Result<()> {
        let params_json = json_to_string(data.params_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO desired_blueprints (desired_id, blueprint_extension_id, blueprint_version, params_json, applied)
             VALUES (?, ?, ?, ?, 0)
             ON CONFLICT(desired_id) DO UPDATE SET
                blueprint_extension_id = excluded.blueprint_extension_id,
                blueprint_version = excluded.blueprint_version,
                params_json = excluded.params_json",
        )
        .bind(data.desired_id.to_string())
        .bind(&data.blueprint_extension_id)
        .bind(&data.blueprint_version)
        .bind(params_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_desired_blueprints(
        &self,
        applied: Option<bool>,
    ) -> Result<Vec<DesiredBlueprint>> {
        let rows = if let Some(applied) = applied {
            sqlx::query(
                "SELECT desired_id, blueprint_extension_id, blueprint_version, CAST(params_json AS TEXT) as params_json, CAST(applied AS INTEGER) as applied, CAST(created_at AS TEXT) as created_at, CAST(applied_at AS TEXT) as applied_at FROM desired_blueprints WHERE applied = ? ORDER BY created_at DESC",
            )
            .bind(applied)
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT desired_id, blueprint_extension_id, blueprint_version, CAST(params_json AS TEXT) as params_json, CAST(applied AS INTEGER) as applied, CAST(created_at AS TEXT) as created_at, CAST(applied_at AS TEXT) as applied_at FROM desired_blueprints ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_desired_blueprint(&row)?);
        }
        Ok(items)
    }

    pub async fn mark_desired_applied(&self, desired_id: Uuid, applied: bool) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE desired_blueprints SET applied = ?, applied_at = CURRENT_TIMESTAMP WHERE desired_id = ?",
        )
        .bind(applied)
        .bind(desired_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_desired_blueprints(&self, applied: Option<bool>) -> Result<u64> {
        let result = if let Some(applied) = applied {
            sqlx::query::<sqlx::Any>("DELETE FROM desired_blueprints WHERE applied = ?")
                .bind(applied)
                .execute(self.pool)
                .await?
        } else {
            sqlx::query::<sqlx::Any>("DELETE FROM desired_blueprints")
                .execute(self.pool)
                .await?
        };
        Ok(result.rows_affected())
    }

    pub async fn delete_desired_blueprint(&self, desired_id: Uuid) -> Result<u64> {
        let result =
            sqlx::query::<sqlx::Any>("DELETE FROM desired_blueprints WHERE desired_id = ?")
                .bind(desired_id.to_string())
                .execute(self.pool)
                .await?;
        Ok(result.rows_affected())
    }

    pub async fn delete_desired_blueprints_by_extension(
        &self,
        blueprint_extension_id: &str,
    ) -> Result<u64> {
        let result = sqlx::query::<sqlx::Any>(
            "DELETE FROM desired_blueprints WHERE blueprint_extension_id = ?",
        )
        .bind(blueprint_extension_id)
        .execute(self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn upsert_managed_ingest_intent(
        &self,
        data: &NewManagedIngestIntent,
    ) -> Result<Uuid> {
        let external_ids_json = match data.external_ids.as_ref() {
            Some(ids) => {
                Some(serde_json::to_value(ids).context("serializing managed ingest external ids")?)
            }
            None => None,
        };
        let external_ids_json = json_to_string(external_ids_json.as_ref())?;

        if let Some(manager_item_id) = data.manager_item_id.as_deref() {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO managed_ingest_intents (
                    intent_id, media_type, title, normalized_title, year, external_ids_json,
                    manager_provider_id, manager_item_id, manager_label, source, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                 ON CONFLICT(manager_provider_id, manager_item_id) DO UPDATE SET
                    media_type = excluded.media_type,
                    title = excluded.title,
                    normalized_title = excluded.normalized_title,
                    year = excluded.year,
                    external_ids_json = COALESCE(excluded.external_ids_json, managed_ingest_intents.external_ids_json),
                    manager_label = excluded.manager_label,
                    source = excluded.source,
                    active = 1,
                    updated_at = CURRENT_TIMESTAMP",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(data.media_type.as_str())
            .bind(&data.title)
            .bind(&data.normalized_title)
            .bind(data.year)
            .bind(external_ids_json.as_deref())
            .bind(data.manager_provider_id.to_string())
            .bind(manager_item_id)
            .bind(data.manager_label.as_deref())
            .bind(&data.source)
            .execute(self.pool)
            .await?;

            let intent_id_raw: String = sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT intent_id FROM managed_ingest_intents
                 WHERE manager_provider_id = ? AND manager_item_id = ?
                 LIMIT 1",
            )
            .bind(data.manager_provider_id.to_string())
            .bind(manager_item_id)
            .fetch_one(self.pool)
            .await?;
            return parse_uuid(&intent_id_raw, "managed_ingest_intents.intent_id");
        }

        let existing_intent_id = if let Some(year) = data.year {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT intent_id FROM managed_ingest_intents
                 WHERE manager_provider_id = ?
                   AND manager_item_id IS NULL
                   AND media_type = ?
                   AND normalized_title = ?
                   AND year = ?
                 LIMIT 1",
            )
            .bind(data.manager_provider_id.to_string())
            .bind(data.media_type.as_str())
            .bind(&data.normalized_title)
            .bind(year)
            .fetch_optional(self.pool)
            .await?
        } else {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT intent_id FROM managed_ingest_intents
                 WHERE manager_provider_id = ?
                   AND manager_item_id IS NULL
                   AND media_type = ?
                   AND normalized_title = ?
                   AND year IS NULL
                 LIMIT 1",
            )
            .bind(data.manager_provider_id.to_string())
            .bind(data.media_type.as_str())
            .bind(&data.normalized_title)
            .fetch_optional(self.pool)
            .await?
        };

        if let Some(existing_intent_id) = existing_intent_id {
            if external_ids_json.is_some() {
                sqlx::query::<sqlx::Any>(
                    "UPDATE managed_ingest_intents
                     SET title = ?,
                         normalized_title = ?,
                         year = ?,
                         external_ids_json = ?,
                         manager_label = ?,
                         source = ?,
                         active = 1,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE intent_id = ?",
                )
                .bind(&data.title)
                .bind(&data.normalized_title)
                .bind(data.year)
                .bind(external_ids_json.as_deref())
                .bind(data.manager_label.as_deref())
                .bind(&data.source)
                .bind(&existing_intent_id)
                .execute(self.pool)
                .await?;
            } else {
                sqlx::query::<sqlx::Any>(
                    "UPDATE managed_ingest_intents
                     SET title = ?,
                         normalized_title = ?,
                         year = ?,
                         manager_label = ?,
                         source = ?,
                         active = 1,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE intent_id = ?",
                )
                .bind(&data.title)
                .bind(&data.normalized_title)
                .bind(data.year)
                .bind(data.manager_label.as_deref())
                .bind(&data.source)
                .bind(&existing_intent_id)
                .execute(self.pool)
                .await?;
            }
            return parse_uuid(&existing_intent_id, "managed_ingest_intents.intent_id");
        }

        let intent_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_ingest_intents (
                intent_id, media_type, title, normalized_title, year, external_ids_json,
                manager_provider_id, manager_item_id, manager_label, source, active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, 1)",
        )
        .bind(intent_id.to_string())
        .bind(data.media_type.as_str())
        .bind(&data.title)
        .bind(&data.normalized_title)
        .bind(data.year)
        .bind(external_ids_json.as_deref())
        .bind(data.manager_provider_id.to_string())
        .bind(data.manager_label.as_deref())
        .bind(&data.source)
        .execute(self.pool)
        .await?;
        Ok(intent_id)
    }

    pub async fn list_active_managed_ingest_intents(&self) -> Result<Vec<ManagedIngestIntent>> {
        let rows = sqlx::query(
            "SELECT
                intent_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) as external_ids_json,
                manager_provider_id,
                CAST(manager_item_id AS TEXT) as manager_item_id,
                CAST(manager_label AS TEXT) as manager_label,
                source,
                CAST(active AS INTEGER) as active,
                CAST(last_matched_at AS TEXT) as last_matched_at,
                CAST(created_at AS TEXT) as created_at,
                CAST(updated_at AS TEXT) as updated_at
             FROM managed_ingest_intents
             WHERE active = 1
             ORDER BY created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_managed_ingest_intent(&row)?);
        }
        Ok(items)
    }

    pub async fn mark_managed_ingest_intent_matched(&self, intent_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE managed_ingest_intents
             SET last_matched_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE intent_id = ?",
        )
        .bind(intent_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_managed_import_event(
        &self,
        data: &NewManagedImportEvent,
    ) -> Result<ManagedImportEvent> {
        let external_ids_json = match data.external_ids.as_ref() {
            Some(ids) => Some(
                serde_json::to_value(ids)
                    .context("serializing managed import event external ids")?,
            ),
            None => None,
        };
        let external_ids_json = json_to_string(external_ids_json.as_ref())?;
        let imported_files_json = serde_json::to_string(&data.imported_files)
            .context("serializing managed import event files")?;
        let raw_manager_payload_json = json_to_string(data.raw_manager_payload.as_ref())?;
        let imported_at = data.imported_at.map(db_datetime_string);

        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_import_events (
                event_id,
                event_key,
                intent_id,
                media_type,
                external_ids_json,
                manager_provider_id,
                manager_item_id,
                manager_label,
                manager_implementation,
                imported_files_json,
                raw_manager_payload_json,
                status,
                imported_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            ON CONFLICT(event_key) DO UPDATE SET
                intent_id = excluded.intent_id,
                media_type = excluded.media_type,
                external_ids_json = COALESCE(excluded.external_ids_json, managed_import_events.external_ids_json),
                manager_provider_id = excluded.manager_provider_id,
                manager_item_id = excluded.manager_item_id,
                manager_label = excluded.manager_label,
                manager_implementation = excluded.manager_implementation,
                imported_files_json = excluded.imported_files_json,
                raw_manager_payload_json = COALESCE(excluded.raw_manager_payload_json, managed_import_events.raw_manager_payload_json),
                status = CASE
                    WHEN managed_import_events.status = 'linked' THEN 'linked'
                    ELSE 'pending'
                END,
                last_error = CASE
                    WHEN managed_import_events.status = 'linked' THEN managed_import_events.last_error
                    ELSE NULL
                END,
                imported_at = COALESCE(excluded.imported_at, managed_import_events.imported_at),
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&data.event_key)
        .bind(data.intent_id.to_string())
        .bind(data.media_type.as_str())
        .bind(external_ids_json.as_deref())
        .bind(data.manager_provider_id.to_string())
        .bind(data.manager_item_id.as_deref())
        .bind(data.manager_label.as_deref())
        .bind(data.manager_implementation.as_deref())
        .bind(&imported_files_json)
        .bind(raw_manager_payload_json.as_deref())
        .bind(imported_at.as_deref())
        .execute(self.pool)
        .await?;

        self.get_managed_import_event_by_key(&data.event_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("managed import event was not persisted"))
    }

    pub async fn list_pending_managed_import_events(&self) -> Result<Vec<ManagedImportEvent>> {
        let rows = sqlx::query(
            "SELECT
                event_id,
                event_key,
                intent_id,
                media_type,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                manager_provider_id,
                CAST(manager_item_id AS TEXT) AS manager_item_id,
                CAST(manager_label AS TEXT) AS manager_label,
                CAST(manager_implementation AS TEXT) AS manager_implementation,
                CAST(imported_files_json AS TEXT) AS imported_files_json,
                CAST(raw_manager_payload_json AS TEXT) AS raw_manager_payload_json,
                status,
                CAST(linked_media_item_id AS TEXT) AS linked_media_item_id,
                CAST(last_error AS TEXT) AS last_error,
                CAST(imported_at AS TEXT) AS imported_at,
                CAST(processed_at AS TEXT) AS processed_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM managed_import_events
             WHERE status = 'pending'
             ORDER BY updated_at ASC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_managed_import_event(&row)?);
        }
        Ok(items)
    }

    pub async fn get_managed_import_event_by_key(
        &self,
        event_key: &str,
    ) -> Result<Option<ManagedImportEvent>> {
        let row = sqlx::query(
            "SELECT
                event_id,
                event_key,
                intent_id,
                media_type,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                manager_provider_id,
                CAST(manager_item_id AS TEXT) AS manager_item_id,
                CAST(manager_label AS TEXT) AS manager_label,
                CAST(manager_implementation AS TEXT) AS manager_implementation,
                CAST(imported_files_json AS TEXT) AS imported_files_json,
                CAST(raw_manager_payload_json AS TEXT) AS raw_manager_payload_json,
                status,
                CAST(linked_media_item_id AS TEXT) AS linked_media_item_id,
                CAST(last_error AS TEXT) AS last_error,
                CAST(imported_at AS TEXT) AS imported_at,
                CAST(processed_at AS TEXT) AS processed_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM managed_import_events
             WHERE event_key = ?
             LIMIT 1",
        )
        .bind(event_key)
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_managed_import_event(&row)).transpose()
    }

    pub async fn mark_managed_import_event_linked(
        &self,
        event_id: Uuid,
        linked_media_item_id: Uuid,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE managed_import_events
             SET status = 'linked',
                 linked_media_item_id = ?,
                 last_error = NULL,
                 processed_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE event_id = ?",
        )
        .bind(linked_media_item_id.to_string())
        .bind(event_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_managed_import_event_failed(
        &self,
        event_id: Uuid,
        error: &str,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE managed_import_events
             SET status = 'failed',
                 last_error = ?,
                 processed_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE event_id = ?",
        )
        .bind(error)
        .bind(event_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn deactivate_managed_ingest_intent(&self, intent_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE managed_ingest_intents
             SET active = 0,
                 updated_at = CURRENT_TIMESTAMP
             WHERE intent_id = ?",
        )
        .bind(intent_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_managed_library_provenance(
        &self,
        data: &NewManagedLibraryProvenance,
    ) -> Result<()> {
        let external_ids_json = match data.external_ids.as_ref() {
            Some(ids) => Some(
                serde_json::to_value(ids)
                    .context("serializing managed library provenance external ids")?,
            ),
            None => None,
        };
        let external_ids_json = json_to_string(external_ids_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_library_provenance (
                media_item_id,
                media_type,
                title,
                normalized_title,
                year,
                external_ids_json,
                manager_provider_id,
                manager_item_id,
                manager_label,
                manager_implementation,
                intent_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(media_item_id) DO UPDATE SET
                media_type = excluded.media_type,
                title = excluded.title,
                normalized_title = excluded.normalized_title,
                year = excluded.year,
                external_ids_json = COALESCE(excluded.external_ids_json, managed_library_provenance.external_ids_json),
                manager_provider_id = excluded.manager_provider_id,
                manager_item_id = excluded.manager_item_id,
                manager_label = excluded.manager_label,
                manager_implementation = excluded.manager_implementation,
                intent_id = excluded.intent_id,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.media_item_id.to_string())
        .bind(data.media_type.as_str())
        .bind(&data.title)
        .bind(&data.normalized_title)
        .bind(data.year)
        .bind(external_ids_json.as_deref())
        .bind(data.manager_provider_id.to_string())
        .bind(data.manager_item_id.as_deref())
        .bind(data.manager_label.as_deref())
        .bind(data.manager_implementation.as_deref())
        .bind(data.intent_id.map(|value| value.to_string()))
        .execute(self.pool)
        .await?;
        self.upsert_extension_media_ownership_for_managed(data)
            .await?;
        Ok(())
    }

    pub async fn get_managed_library_provenance(
        &self,
        media_item_id: Uuid,
    ) -> Result<Option<ManagedLibraryProvenance>> {
        let row = sqlx::query(
            "SELECT
                media_item_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                manager_provider_id,
                CAST(manager_item_id AS TEXT) AS manager_item_id,
                CAST(manager_label AS TEXT) AS manager_label,
                CAST(manager_implementation AS TEXT) AS manager_implementation,
                CAST(intent_id AS TEXT) AS intent_id,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM managed_library_provenance
             WHERE media_item_id = ?
             LIMIT 1",
        )
        .bind(media_item_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_managed_library_provenance(&row))
            .transpose()
    }

    pub async fn upsert_media_ownership(&self, data: &NewMediaOwnership) -> Result<()> {
        let acquisition_target_scope_json = json_to_string(data.acquisition_target_scope.as_ref())?;
        let metadata_json = json_to_string(data.metadata.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_ownerships (
                ownership_id,
                media_item_id,
                owner_type,
                owner_role,
                owner_label,
                owner_implementation,
                owner_provider_id,
                owner_instance_id,
                owner_extension_id,
                owner_external_id,
                acquisition_subscription_id,
                acquisition_target_scope_json,
                release_capability,
                release_policy,
                metadata_json,
                active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ownership_id) DO UPDATE SET
                media_item_id = excluded.media_item_id,
                owner_type = excluded.owner_type,
                owner_role = excluded.owner_role,
                owner_label = excluded.owner_label,
                owner_implementation = excluded.owner_implementation,
                owner_provider_id = excluded.owner_provider_id,
                owner_instance_id = excluded.owner_instance_id,
                owner_extension_id = excluded.owner_extension_id,
                owner_external_id = excluded.owner_external_id,
                acquisition_subscription_id = excluded.acquisition_subscription_id,
                acquisition_target_scope_json = excluded.acquisition_target_scope_json,
                release_capability = excluded.release_capability,
                release_policy = excluded.release_policy,
                metadata_json = excluded.metadata_json,
                active = excluded.active,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.ownership_id.to_string())
        .bind(data.media_item_id.to_string())
        .bind(&data.owner_type)
        .bind(&data.owner_role)
        .bind(data.owner_label.as_deref())
        .bind(data.owner_implementation.as_deref())
        .bind(data.owner_provider_id.map(|value| value.to_string()))
        .bind(data.owner_instance_id.map(|value| value.to_string()))
        .bind(data.owner_extension_id.as_deref())
        .bind(data.owner_external_id.as_deref())
        .bind(
            data.acquisition_subscription_id
                .map(|value| value.to_string()),
        )
        .bind(acquisition_target_scope_json.as_deref())
        .bind(&data.release_capability)
        .bind(&data.release_policy)
        .bind(metadata_json.as_deref())
        .bind(data.active)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_external_media_ownership_if_missing(
        &self,
        media_item_id: Uuid,
        media_type: crate::db::models::MediaType,
        title: &str,
        year: Option<i32>,
        external_ids: Option<&ExternalIds>,
    ) -> Result<()> {
        let has_owner = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*)
             FROM media_ownerships
             WHERE media_item_id = ?
               AND owner_role = 'primary'
               AND active = 1",
        )
        .bind(media_item_id.to_string())
        .fetch_one(self.pool)
        .await?
            > 0;
        if has_owner {
            return Ok(());
        }
        self.upsert_media_ownership(&NewMediaOwnership {
            ownership_id: media_item_id,
            media_item_id,
            owner_type: "external".to_string(),
            owner_role: "primary".to_string(),
            owner_label: Some("External import".to_string()),
            owner_implementation: None,
            owner_provider_id: None,
            owner_instance_id: None,
            owner_extension_id: None,
            owner_external_id: None,
            acquisition_subscription_id: None,
            acquisition_target_scope: None,
            release_capability: "none".to_string(),
            release_policy: "unsupported".to_string(),
            metadata: Some(media_ownership_identity_metadata(
                media_type,
                title,
                year,
                external_ids,
            )),
            active: true,
        })
        .await
    }

    pub async fn upsert_acquisition_media_ownership(
        &self,
        media_item_id: Uuid,
        subscription_id: Uuid,
        source_provider_id: Option<Uuid>,
        source_extension_id: Option<&str>,
    ) -> Result<()> {
        let provider_context = match source_provider_id {
            Some(provider_id) => self.provider_ownership_context(provider_id).await?,
            None => None,
        };
        let owner_extension_id = provider_context
            .as_ref()
            .and_then(|context| context.extension_id.clone());
        let owner_instance_id = provider_context
            .as_ref()
            .and_then(|context| context.instance_id);
        let implementation = source_extension_id.map(str::to_string).or_else(|| {
            provider_context
                .as_ref()
                .and_then(|context| context.implementation.clone())
        });
        self.upsert_media_ownership(&NewMediaOwnership {
            ownership_id: media_item_id,
            media_item_id,
            owner_type: "acquisition".to_string(),
            owner_role: "primary".to_string(),
            owner_label: Some("Elixir acquisition".to_string()),
            owner_implementation: implementation,
            owner_provider_id: source_provider_id,
            owner_instance_id,
            owner_extension_id,
            owner_external_id: Some(subscription_id.to_string()),
            acquisition_subscription_id: Some(subscription_id),
            acquisition_target_scope: None,
            release_capability: "acquisition.stop_monitoring".to_string(),
            release_policy: "supported".to_string(),
            metadata: Some(json!({
                "subscriptionId": subscription_id,
                "source": "acquisition_import",
            })),
            active: true,
        })
        .await
    }

    pub async fn list_active_media_ownerships(
        &self,
        media_item_id: Uuid,
    ) -> Result<Vec<MediaOwnership>> {
        let rows = sqlx::query(
            "SELECT
                ownership_id,
                media_item_id,
                owner_type,
                owner_role,
                CAST(owner_label AS TEXT) AS owner_label,
                CAST(owner_implementation AS TEXT) AS owner_implementation,
                CAST(owner_provider_id AS TEXT) AS owner_provider_id,
                CAST(owner_instance_id AS TEXT) AS owner_instance_id,
                CAST(owner_extension_id AS TEXT) AS owner_extension_id,
                CAST(owner_external_id AS TEXT) AS owner_external_id,
                CAST(acquisition_subscription_id AS TEXT) AS acquisition_subscription_id,
                CAST(acquisition_target_scope_json AS TEXT) AS acquisition_target_scope_json,
                release_capability,
                release_policy,
                CAST(metadata_json AS TEXT) AS metadata_json,
                CAST(active AS INTEGER) AS active,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM media_ownerships
             WHERE media_item_id = ?
               AND active = 1
             ORDER BY CASE owner_role WHEN 'primary' THEN 0 ELSE 1 END, created_at ASC",
        )
        .bind(media_item_id.to_string())
        .fetch_all(self.pool)
        .await?;
        rows.into_iter()
            .map(|row| map_media_ownership(&row))
            .collect()
    }

    pub async fn ensure_media_ownerships_for_item(
        &self,
        media_item_id: Uuid,
    ) -> Result<Vec<MediaOwnership>> {
        let existing = self.list_active_media_ownerships(media_item_id).await?;
        if !existing.is_empty() {
            return Ok(existing);
        }

        if let Some(provenance) = self.get_managed_library_provenance(media_item_id).await? {
            self.upsert_extension_media_ownership_for_managed(&NewManagedLibraryProvenance {
                media_item_id: provenance.media_item_id,
                media_type: provenance.media_type,
                title: provenance.title,
                normalized_title: provenance.normalized_title,
                year: provenance.year,
                external_ids: provenance.external_ids,
                manager_provider_id: provenance.manager_provider_id,
                manager_item_id: provenance.manager_item_id,
                manager_label: provenance.manager_label,
                manager_implementation: provenance.manager_implementation,
                intent_id: provenance.intent_id,
            })
            .await?;
            return self.list_active_media_ownerships(media_item_id).await;
        }

        if let Some(acquisition) = self.acquisition_ownership_context(media_item_id).await? {
            self.upsert_acquisition_media_ownership(
                media_item_id,
                acquisition.subscription_id,
                acquisition.source_provider_id,
                acquisition.source_extension_id.as_deref(),
            )
            .await?;
            return self.list_active_media_ownerships(media_item_id).await;
        }

        if let Some(identity) = self.media_item_identity(media_item_id).await? {
            self.upsert_external_media_ownership_if_missing(
                media_item_id,
                identity.media_type,
                &identity.title,
                identity.year,
                identity.external_ids.as_ref(),
            )
            .await?;
        }

        self.list_active_media_ownerships(media_item_id).await
    }

    pub async fn create_media_owner_release_event(
        &self,
        data: &NewMediaOwnerReleaseEvent,
    ) -> Result<()> {
        let request_json = json_to_string(data.request.as_ref())?;
        let response_json = json_to_string(data.response.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_owner_release_events (
                release_event_id,
                media_item_id,
                ownership_id,
                requested_action,
                owner_type,
                owner_label,
                owner_provider_id,
                acquisition_subscription_id,
                status,
                status_reason,
                request_json,
                response_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(data.release_event_id.to_string())
        .bind(data.media_item_id.map(|value| value.to_string()))
        .bind(data.ownership_id.map(|value| value.to_string()))
        .bind(&data.requested_action)
        .bind(&data.owner_type)
        .bind(data.owner_label.as_deref())
        .bind(data.owner_provider_id.map(|value| value.to_string()))
        .bind(
            data.acquisition_subscription_id
                .map(|value| value.to_string()),
        )
        .bind(&data.status)
        .bind(data.status_reason.as_deref())
        .bind(request_json.as_deref())
        .bind(response_json.as_deref())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_media_owner_release_event_status(
        &self,
        release_event_id: Uuid,
        status: &str,
        status_reason: Option<&str>,
        response: Option<&serde_json::Value>,
    ) -> Result<()> {
        let response_json = json_to_string(response)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE media_owner_release_events
             SET status = ?,
                 status_reason = ?,
                 response_json = COALESCE(?, response_json),
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_event_id = ?",
        )
        .bind(status)
        .bind(status_reason)
        .bind(response_json.as_deref())
        .bind(release_event_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_media_owner_release_events_for_item(
        &self,
        media_item_id: Uuid,
    ) -> Result<Vec<MediaOwnerReleaseEvent>> {
        let rows = sqlx::query(
            "SELECT
                release_event_id,
                CAST(media_item_id AS TEXT) AS media_item_id,
                CAST(ownership_id AS TEXT) AS ownership_id,
                requested_action,
                owner_type,
                CAST(owner_label AS TEXT) AS owner_label,
                CAST(owner_provider_id AS TEXT) AS owner_provider_id,
                CAST(acquisition_subscription_id AS TEXT) AS acquisition_subscription_id,
                status,
                CAST(status_reason AS TEXT) AS status_reason,
                CAST(request_json AS TEXT) AS request_json,
                CAST(response_json AS TEXT) AS response_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM media_owner_release_events
             WHERE media_item_id = ?
             ORDER BY created_at DESC",
        )
        .bind(media_item_id.to_string())
        .fetch_all(self.pool)
        .await?;
        rows.into_iter()
            .map(|row| map_media_owner_release_event(&row))
            .collect()
    }

    pub async fn list_media_owner_release_events(
        &self,
        limit: i64,
    ) -> Result<Vec<MediaOwnerReleaseEvent>> {
        let rows = sqlx::query(
            "SELECT
                release_event_id,
                CAST(media_item_id AS TEXT) AS media_item_id,
                CAST(ownership_id AS TEXT) AS ownership_id,
                requested_action,
                owner_type,
                CAST(owner_label AS TEXT) AS owner_label,
                CAST(owner_provider_id AS TEXT) AS owner_provider_id,
                CAST(acquisition_subscription_id AS TEXT) AS acquisition_subscription_id,
                status,
                CAST(status_reason AS TEXT) AS status_reason,
                CAST(request_json AS TEXT) AS request_json,
                CAST(response_json AS TEXT) AS response_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM media_owner_release_events
             ORDER BY created_at DESC
             LIMIT ?",
        )
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        rows.into_iter()
            .map(|row| map_media_owner_release_event(&row))
            .collect()
    }

    pub async fn latest_media_owner_release_event(
        &self,
        media_item_id: Uuid,
        ownership_id: Uuid,
        requested_action: &str,
    ) -> Result<Option<MediaOwnerReleaseEvent>> {
        let row = sqlx::query(
            "SELECT
                release_event_id,
                CAST(media_item_id AS TEXT) AS media_item_id,
                CAST(ownership_id AS TEXT) AS ownership_id,
                requested_action,
                owner_type,
                CAST(owner_label AS TEXT) AS owner_label,
                CAST(owner_provider_id AS TEXT) AS owner_provider_id,
                CAST(acquisition_subscription_id AS TEXT) AS acquisition_subscription_id,
                status,
                CAST(status_reason AS TEXT) AS status_reason,
                CAST(request_json AS TEXT) AS request_json,
                CAST(response_json AS TEXT) AS response_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM media_owner_release_events
             WHERE media_item_id = ?
               AND ownership_id = ?
               AND requested_action = ?
             ORDER BY created_at DESC
             LIMIT 1",
        )
        .bind(media_item_id.to_string())
        .bind(ownership_id.to_string())
        .bind(requested_action)
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_media_owner_release_event(&row))
            .transpose()
    }

    pub async fn reconcile_media_ownerships(&self) -> Result<MediaOwnershipReconcileReport> {
        let missing_owner_rows = sqlx::query(
            "SELECT id
             FROM media_items mi
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM media_ownerships mo
                 WHERE mo.media_item_id = mi.id
                   AND mo.owner_role = 'primary'
                   AND mo.active = 1
             )
             ORDER BY id",
        )
        .fetch_all(self.pool)
        .await?;

        let mut report = MediaOwnershipReconcileReport::default();
        for row in missing_owner_rows {
            let media_item_id_raw: String = row.try_get("id")?;
            let media_item_id = parse_uuid(&media_item_id_raw, "media_items.id")?;
            if let Some(identity) = self.media_item_identity(media_item_id).await? {
                self.upsert_external_media_ownership_if_missing(
                    media_item_id,
                    identity.media_type,
                    &identity.title,
                    identity.year,
                    identity.external_ids.as_ref(),
                )
                .await?;
                report.external_owners_created += 1;
            }
        }

        let stale_owners = self.list_stale_releasable_extension_owners().await?;
        for owner in stale_owners {
            let request_json = json!({
                "action": "reconcile_owner",
                "mediaItemId": owner.media_item_id,
                "ownershipId": owner.ownership_id,
                "ownerType": owner.owner_type.clone(),
                "previousReleaseCapability": owner.release_capability.clone(),
                "previousReleasePolicy": owner.release_policy.clone(),
            });
            let response_json = json!({
                "releaseCapability": "none",
                "releasePolicy": "unsupported",
            });
            sqlx::query::<sqlx::Any>(
                "UPDATE media_ownerships
                 SET release_capability = 'none',
                     release_policy = 'unsupported',
                     updated_at = CURRENT_TIMESTAMP
                 WHERE ownership_id = ?
                   AND active = 1",
            )
            .bind(owner.ownership_id.to_string())
            .execute(self.pool)
            .await?;
            report.stale_owners_marked_unsupported += 1;

            self.create_media_owner_release_event(&NewMediaOwnerReleaseEvent {
                release_event_id: Uuid::new_v4(),
                media_item_id: Some(owner.media_item_id),
                ownership_id: Some(owner.ownership_id),
                requested_action: "reconcile_owner".to_string(),
                owner_type: owner.owner_type.clone(),
                owner_label: owner.owner_label.clone(),
                owner_provider_id: owner.owner_provider_id,
                acquisition_subscription_id: owner.acquisition_subscription_id,
                status: "unsupported".to_string(),
                status_reason: Some(
                    "Owner provider or instance is no longer available; release is unsupported until ownership is repaired."
                        .to_string(),
                ),
                request: Some(request_json),
                response: Some(response_json),
            })
            .await?;
            report.unsupported_events_created += 1;
        }

        Ok(report)
    }

    async fn list_stale_releasable_extension_owners(&self) -> Result<Vec<MediaOwnership>> {
        let rows = sqlx::query(
            "SELECT
                mo.ownership_id,
                mo.media_item_id,
                mo.owner_type,
                mo.owner_role,
                CAST(mo.owner_label AS TEXT) AS owner_label,
                CAST(mo.owner_implementation AS TEXT) AS owner_implementation,
                CAST(mo.owner_provider_id AS TEXT) AS owner_provider_id,
                CAST(mo.owner_instance_id AS TEXT) AS owner_instance_id,
                CAST(mo.owner_extension_id AS TEXT) AS owner_extension_id,
                CAST(mo.owner_external_id AS TEXT) AS owner_external_id,
                CAST(mo.acquisition_subscription_id AS TEXT) AS acquisition_subscription_id,
                CAST(mo.acquisition_target_scope_json AS TEXT) AS acquisition_target_scope_json,
                mo.release_capability,
                mo.release_policy,
                CAST(mo.metadata_json AS TEXT) AS metadata_json,
                CAST(mo.active AS INTEGER) AS active,
                CAST(mo.created_at AS TEXT) AS created_at,
                CAST(mo.updated_at AS TEXT) AS updated_at
             FROM media_ownerships mo
             WHERE mo.active = 1
               AND mo.owner_type = 'extension'
               AND mo.release_capability <> 'none'
               AND (
                   mo.owner_provider_id IS NULL
                   OR NOT EXISTS (
                       SELECT 1
                       FROM providers p
                       WHERE p.provider_id = mo.owner_provider_id
                   )
                   OR (
                       mo.owner_instance_id IS NOT NULL
                       AND NOT EXISTS (
                           SELECT 1
                           FROM extension_instances ei
                           WHERE ei.instance_id = mo.owner_instance_id
                       )
                   )
               )
             ORDER BY mo.updated_at ASC",
        )
        .fetch_all(self.pool)
        .await?;
        rows.into_iter()
            .map(|row| map_media_ownership(&row))
            .collect()
    }

    async fn upsert_extension_media_ownership_for_managed(
        &self,
        data: &NewManagedLibraryProvenance,
    ) -> Result<()> {
        let provider_context = self
            .provider_ownership_context(data.manager_provider_id)
            .await?;
        let provider_implementation = provider_context
            .as_ref()
            .and_then(|context| context.implementation.clone());
        let implementation = data
            .manager_implementation
            .clone()
            .or(provider_implementation);
        let release_supported = implementation
            .as_deref()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "sonarr" | "radarr"
                )
            })
            .unwrap_or(false);
        let owner_label = data
            .manager_label
            .clone()
            .or_else(|| implementation.clone())
            .unwrap_or_else(|| "Managed extension".to_string());
        self.upsert_media_ownership(&NewMediaOwnership {
            ownership_id: data.media_item_id,
            media_item_id: data.media_item_id,
            owner_type: "extension".to_string(),
            owner_role: "primary".to_string(),
            owner_label: Some(owner_label),
            owner_implementation: implementation,
            owner_provider_id: Some(data.manager_provider_id),
            owner_instance_id: provider_context
                .as_ref()
                .and_then(|context| context.instance_id),
            owner_extension_id: provider_context
                .as_ref()
                .and_then(|context| context.extension_id.clone()),
            owner_external_id: data.manager_item_id.clone(),
            acquisition_subscription_id: None,
            acquisition_target_scope: None,
            release_capability: if release_supported {
                "manager.remove_item".to_string()
            } else {
                "none".to_string()
            },
            release_policy: if release_supported {
                "supported".to_string()
            } else {
                "unsupported".to_string()
            },
            metadata: Some(media_ownership_identity_metadata(
                data.media_type,
                &data.title,
                data.year,
                data.external_ids.as_ref(),
            )),
            active: true,
        })
        .await
    }

    async fn provider_ownership_context(
        &self,
        provider_id: Uuid,
    ) -> Result<Option<ProviderOwnershipContext>> {
        let row = sqlx::query(
            "SELECT
                CAST(p.instance_id AS TEXT) AS instance_id,
                CAST(p.implementation AS TEXT) AS implementation,
                CAST(ei.extension_id AS TEXT) AS extension_id
             FROM providers p
             LEFT JOIN extension_instances ei ON ei.instance_id = p.instance_id
             WHERE p.provider_id = ?
             LIMIT 1",
        )
        .bind(provider_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| {
            let instance_id = row_get_opt_string(&row, "instance_id")?
                .as_deref()
                .map(|value| parse_uuid(value, "providers.instance_id"))
                .transpose()?;
            Ok(ProviderOwnershipContext {
                instance_id,
                implementation: row_get_opt_string(&row, "implementation")?,
                extension_id: row_get_opt_string(&row, "extension_id")?,
            })
        })
        .transpose()
    }

    async fn acquisition_ownership_context(
        &self,
        media_item_id: Uuid,
    ) -> Result<Option<AcquisitionOwnershipContext>> {
        let row = sqlx::query(
            "SELECT
                CAST(r.subscription_id AS TEXT) AS subscription_id,
                CAST(r.source_provider_id AS TEXT) AS source_provider_id,
                CAST(r.source_extension_id AS TEXT) AS source_extension_id
             FROM acquisition_import_file_links ail
             JOIN acquisition_releases r ON r.release_id = ail.release_id
             LEFT JOIN media_files mf ON mf.id = ail.media_file_id
             LEFT JOIN episodes e ON e.id = ail.episode_id
             WHERE ail.state = 'imported'
               AND r.subscription_id IS NOT NULL
               AND COALESCE(mf.media_item_id, ail.movie_id, e.series_id) = ?
             ORDER BY ail.updated_at DESC
             LIMIT 1",
        )
        .bind(media_item_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| {
            let subscription_id_raw = row_get_opt_string(&row, "subscription_id")?
                .ok_or_else(|| anyhow::anyhow!("acquisition ownership row missing subscription"))?;
            Ok(AcquisitionOwnershipContext {
                subscription_id: parse_uuid(
                    &subscription_id_raw,
                    "acquisition_releases.subscription_id",
                )?,
                source_provider_id: row_get_opt_string(&row, "source_provider_id")?
                    .as_deref()
                    .map(|value| parse_uuid(value, "acquisition_releases.source_provider_id"))
                    .transpose()?,
                source_extension_id: row_get_opt_string(&row, "source_extension_id")?,
            })
        })
        .transpose()
    }

    async fn media_item_identity(
        &self,
        media_item_id: Uuid,
    ) -> Result<Option<MediaItemOwnershipIdentity>> {
        let row = sqlx::query(
            "SELECT
                type,
                title,
                year,
                CAST(external_ids AS TEXT) AS external_ids
             FROM media_items
             WHERE id = ?
             LIMIT 1",
        )
        .bind(media_item_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| {
            let media_type_raw: String = row.try_get("type")?;
            let external_ids = parse_json_opt(
                row_get_opt_string(&row, "external_ids")?,
                "media_items.external_ids",
            )?
            .map(serde_json::from_value::<ExternalIds>)
            .transpose()
            .context("parsing media item external ids")?;
            Ok(MediaItemOwnershipIdentity {
                media_type: parse_media_type(&media_type_raw, "media_items.type")?,
                title: row.try_get("title")?,
                year: row.try_get::<i64, _>("year").ok().map(|value| value as i32),
                external_ids,
            })
        })
        .transpose()
    }

    pub async fn upsert_managed_media_tombstone(
        &self,
        data: &NewManagedMediaTombstone,
    ) -> Result<Uuid> {
        let external_ids_json = match data.external_ids.as_ref() {
            Some(ids) => {
                Some(serde_json::to_value(ids).context("serializing managed media tombstone ids")?)
            }
            None => None,
        };
        let external_ids_json = json_to_string(external_ids_json.as_ref())?;

        let existing_tombstone_id = if let (Some(provider_id), Some(manager_item_id)) =
            (data.manager_provider_id, data.manager_item_id.as_deref())
        {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT tombstone_id FROM managed_media_tombstones
                 WHERE active = 1
                   AND manager_provider_id = ?
                   AND manager_item_id = ?
                 LIMIT 1",
            )
            .bind(provider_id.to_string())
            .bind(manager_item_id)
            .fetch_optional(self.pool)
            .await?
        } else if let Some(year) = data.year {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT tombstone_id FROM managed_media_tombstones
                 WHERE active = 1
                   AND media_type = ?
                   AND normalized_title = ?
                   AND year = ?
                 LIMIT 1",
            )
            .bind(data.media_type.as_str())
            .bind(&data.normalized_title)
            .bind(year)
            .fetch_optional(self.pool)
            .await?
        } else {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT tombstone_id FROM managed_media_tombstones
                 WHERE active = 1
                   AND media_type = ?
                   AND normalized_title = ?
                   AND year IS NULL
                 LIMIT 1",
            )
            .bind(data.media_type.as_str())
            .bind(&data.normalized_title)
            .fetch_optional(self.pool)
            .await?
        };

        if let Some(existing_tombstone_id) = existing_tombstone_id {
            sqlx::query::<sqlx::Any>(
                "UPDATE managed_media_tombstones
                 SET media_type = ?,
                     title = ?,
                     normalized_title = ?,
                     year = ?,
                     external_ids_json = COALESCE(?, external_ids_json),
                     manager_provider_id = ?,
                     manager_item_id = ?,
                     manager_label = ?,
                     manager_implementation = ?,
                     action = ?,
                     active = 1,
                     cleared_at = NULL,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE tombstone_id = ?",
            )
            .bind(data.media_type.as_str())
            .bind(&data.title)
            .bind(&data.normalized_title)
            .bind(data.year)
            .bind(external_ids_json.as_deref())
            .bind(data.manager_provider_id.map(|value| value.to_string()))
            .bind(data.manager_item_id.as_deref())
            .bind(data.manager_label.as_deref())
            .bind(data.manager_implementation.as_deref())
            .bind(&data.action)
            .bind(&existing_tombstone_id)
            .execute(self.pool)
            .await?;
            return parse_uuid(
                &existing_tombstone_id,
                "managed_media_tombstones.tombstone_id",
            );
        }

        let tombstone_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_media_tombstones (
                tombstone_id,
                media_type,
                title,
                normalized_title,
                year,
                external_ids_json,
                manager_provider_id,
                manager_item_id,
                manager_label,
                manager_implementation,
                action,
                active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        )
        .bind(tombstone_id.to_string())
        .bind(data.media_type.as_str())
        .bind(&data.title)
        .bind(&data.normalized_title)
        .bind(data.year)
        .bind(external_ids_json.as_deref())
        .bind(data.manager_provider_id.map(|value| value.to_string()))
        .bind(data.manager_item_id.as_deref())
        .bind(data.manager_label.as_deref())
        .bind(data.manager_implementation.as_deref())
        .bind(&data.action)
        .execute(self.pool)
        .await?;
        Ok(tombstone_id)
    }

    pub async fn list_active_managed_media_tombstones(&self) -> Result<Vec<ManagedMediaTombstone>> {
        let rows = sqlx::query(
            "SELECT
                tombstone_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                CAST(manager_provider_id AS TEXT) AS manager_provider_id,
                CAST(manager_item_id AS TEXT) AS manager_item_id,
                CAST(manager_label AS TEXT) AS manager_label,
                CAST(manager_implementation AS TEXT) AS manager_implementation,
                action,
                CAST(active AS INTEGER) AS active,
                CAST(cleared_at AS TEXT) AS cleared_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM managed_media_tombstones
             WHERE active = 1
             ORDER BY created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_managed_media_tombstone(&row)?);
        }
        Ok(items)
    }

    pub async fn deactivate_managed_media_tombstone(&self, tombstone_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE managed_media_tombstones
             SET active = 0,
                 cleared_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE tombstone_id = ?",
        )
        .bind(tombstone_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_managed_episode_tombstone(
        &self,
        data: &NewManagedEpisodeTombstone,
    ) -> Result<Uuid> {
        let external_ids_json = match data.external_ids.as_ref() {
            Some(ids) => Some(
                serde_json::to_value(ids)
                    .context("serializing managed episode tombstone external ids")?,
            ),
            None => None,
        };
        let external_ids_json = json_to_string(external_ids_json.as_ref())?;

        let existing_tombstone_id = if let (Some(provider_id), Some(manager_item_id)) =
            (data.manager_provider_id, data.manager_item_id.as_deref())
        {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT tombstone_id FROM managed_episode_tombstones
                 WHERE active = 1
                   AND manager_provider_id = ?
                   AND manager_item_id = ?
                   AND season_number = ?
                   AND episode_number = ?
                 LIMIT 1",
            )
            .bind(provider_id.to_string())
            .bind(manager_item_id)
            .bind(data.season_number)
            .bind(data.episode_number)
            .fetch_optional(self.pool)
            .await?
        } else if let Some(year) = data.year {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT tombstone_id FROM managed_episode_tombstones
                 WHERE active = 1
                   AND media_type = ?
                   AND normalized_title = ?
                   AND year = ?
                   AND season_number = ?
                   AND episode_number = ?
                 LIMIT 1",
            )
            .bind(data.media_type.as_str())
            .bind(&data.normalized_title)
            .bind(year)
            .bind(data.season_number)
            .bind(data.episode_number)
            .fetch_optional(self.pool)
            .await?
        } else {
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT tombstone_id FROM managed_episode_tombstones
                 WHERE active = 1
                   AND media_type = ?
                   AND normalized_title = ?
                   AND year IS NULL
                   AND season_number = ?
                   AND episode_number = ?
                 LIMIT 1",
            )
            .bind(data.media_type.as_str())
            .bind(&data.normalized_title)
            .bind(data.season_number)
            .bind(data.episode_number)
            .fetch_optional(self.pool)
            .await?
        };

        if let Some(existing_tombstone_id) = existing_tombstone_id {
            sqlx::query::<sqlx::Any>(
                "UPDATE managed_episode_tombstones
                 SET media_type = ?,
                     title = ?,
                     normalized_title = ?,
                     year = ?,
                     external_ids_json = COALESCE(?, external_ids_json),
                     manager_provider_id = ?,
                     manager_item_id = ?,
                     manager_label = ?,
                     manager_implementation = ?,
                     season_number = ?,
                     episode_number = ?,
                     absolute_episode_number = COALESCE(?, absolute_episode_number),
                     action = ?,
                     active = 1,
                     cleared_at = NULL,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE tombstone_id = ?",
            )
            .bind(data.media_type.as_str())
            .bind(&data.title)
            .bind(&data.normalized_title)
            .bind(data.year)
            .bind(external_ids_json.as_deref())
            .bind(data.manager_provider_id.map(|value| value.to_string()))
            .bind(data.manager_item_id.as_deref())
            .bind(data.manager_label.as_deref())
            .bind(data.manager_implementation.as_deref())
            .bind(data.season_number)
            .bind(data.episode_number)
            .bind(data.absolute_episode_number)
            .bind(&data.action)
            .bind(&existing_tombstone_id)
            .execute(self.pool)
            .await?;
            return parse_uuid(
                &existing_tombstone_id,
                "managed_episode_tombstones.tombstone_id",
            );
        }

        let tombstone_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_episode_tombstones (
                tombstone_id,
                media_type,
                title,
                normalized_title,
                year,
                external_ids_json,
                manager_provider_id,
                manager_item_id,
                manager_label,
                manager_implementation,
                season_number,
                episode_number,
                absolute_episode_number,
                action,
                active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        )
        .bind(tombstone_id.to_string())
        .bind(data.media_type.as_str())
        .bind(&data.title)
        .bind(&data.normalized_title)
        .bind(data.year)
        .bind(external_ids_json.as_deref())
        .bind(data.manager_provider_id.map(|value| value.to_string()))
        .bind(data.manager_item_id.as_deref())
        .bind(data.manager_label.as_deref())
        .bind(data.manager_implementation.as_deref())
        .bind(data.season_number)
        .bind(data.episode_number)
        .bind(data.absolute_episode_number)
        .bind(&data.action)
        .execute(self.pool)
        .await?;
        Ok(tombstone_id)
    }

    pub async fn list_active_managed_episode_tombstones(
        &self,
    ) -> Result<Vec<ManagedEpisodeTombstone>> {
        let rows = sqlx::query(
            "SELECT
                tombstone_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                CAST(manager_provider_id AS TEXT) AS manager_provider_id,
                CAST(manager_item_id AS TEXT) AS manager_item_id,
                CAST(manager_label AS TEXT) AS manager_label,
                CAST(manager_implementation AS TEXT) AS manager_implementation,
                season_number,
                episode_number,
                absolute_episode_number,
                action,
                CAST(active AS INTEGER) AS active,
                CAST(cleared_at AS TEXT) AS cleared_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM managed_episode_tombstones
             WHERE active = 1
             ORDER BY created_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_managed_episode_tombstone(&row)?);
        }
        Ok(items)
    }

    pub async fn deactivate_managed_episode_tombstone(&self, tombstone_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE managed_episode_tombstones
             SET active = 0,
                 cleared_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE tombstone_id = ?",
        )
        .bind(tombstone_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_secret(&self, data: &NewSecret) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO secrets (secret_id, scope, scope_id, key, value_encrypted, rotatable) VALUES (?, ?, ?, ?, ?, ?) \n             ON CONFLICT(scope, scope_id, key) DO UPDATE SET value_encrypted = excluded.value_encrypted, rotatable = excluded.rotatable",
        )
        .bind(data.secret_id.to_string())
        .bind(data.scope.as_str())
        .bind(data.scope_id.map(|id| id.to_string()))
        .bind(&data.key)
        .bind(&data.value_encrypted)
        .bind(data.rotatable)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_secret(
        &self,
        scope: SecretScope,
        scope_id: Option<Uuid>,
        key: &str,
    ) -> Result<Option<Secret>> {
        let row = if let Some(scope_id) = scope_id {
            sqlx::query(
                "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets WHERE scope = ? AND scope_id = ? AND key = ? LIMIT 1",
            )
            .bind(scope.as_str())
            .bind(scope_id.to_string())
            .bind(key)
            .fetch_optional(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets WHERE scope = ? AND scope_id IS NULL AND key = ? LIMIT 1",
            )
            .bind(scope.as_str())
            .bind(key)
            .fetch_optional(self.pool)
            .await?
        };
        row.map(|row| map_secret(&row)).transpose()
    }

    pub async fn get_secret_by_id(&self, secret_id: Uuid) -> Result<Option<Secret>> {
        let row = sqlx::query(
            "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets WHERE secret_id = ? LIMIT 1",
        )
        .bind(secret_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_secret(&row)).transpose()
    }

    pub async fn list_secrets(
        &self,
        scope: Option<SecretScope>,
        scope_id: Option<Uuid>,
        key: Option<&str>,
    ) -> Result<Vec<Secret>> {
        let mut builder = QueryBuilder::<sqlx::Any>::new(
            "SELECT secret_id, scope, CAST(scope_id AS TEXT) as scope_id, key, value_encrypted, CAST(created_at AS TEXT) as created_at, CAST(rotatable AS INTEGER) as rotatable FROM secrets",
        );
        let mut has_where = false;
        if let Some(scope) = scope {
            builder.push(if has_where { " AND " } else { " WHERE " });
            builder.push("scope = ");
            builder.push_bind(scope.as_str());
            has_where = true;
        }
        if let Some(scope_id) = scope_id {
            builder.push(if has_where { " AND " } else { " WHERE " });
            builder.push("scope_id = ");
            builder.push_bind(scope_id.to_string());
            has_where = true;
        }
        if let Some(key) = key {
            builder.push(if has_where { " AND " } else { " WHERE " });
            builder.push("key = ");
            builder.push_bind(key);
        }

        let rows = builder.build().fetch_all(self.pool).await?;
        let mut secrets = Vec::with_capacity(rows.len());
        for row in rows {
            secrets.push(map_secret(&row)?);
        }
        Ok(secrets)
    }

    pub async fn delete_secret(&self, secret_id: Uuid) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM secrets WHERE secret_id = ?")
            .bind(secret_id.to_string())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_secret(
        &self,
        secret_id: Uuid,
        value_encrypted: &str,
        rotatable: bool,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE secrets SET value_encrypted = ?, rotatable = ? WHERE secret_id = ?",
        )
        .bind(value_encrypted)
        .bind(rotatable)
        .bind(secret_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_run(&self, data: &NewOrchestratorRun) -> Result<()> {
        let plan_json = json_to_string(data.plan_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO orchestrator_runs (run_id, source, status, phase, plan_json, error, started_at) VALUES (?, ?, ?, ?, ?, ?, CASE WHEN ? = 'running' THEN CURRENT_TIMESTAMP ELSE NULL END)",
        )
        .bind(data.run_id.to_string())
        .bind(&data.source)
        .bind(data.status.as_str())
        .bind(data.phase.as_deref())
        .bind(plan_json)
        .bind(data.error.as_deref())
        .bind(data.status.as_str())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn acquire_lock(
        &self,
        lock_name: &str,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let insert = sqlx::query::<sqlx::Any>(
            "INSERT INTO orchestrator_locks (lock_name, owner_id) VALUES (?, ?)",
        )
        .bind(lock_name)
        .bind(owner_id)
        .execute(self.pool)
        .await;

        if insert.is_ok() {
            return Ok(true);
        }

        let err = insert.err().expect("insert error");
        if !is_unique_violation(&err) {
            return Err(err.into());
        }

        let ttl_seconds = ttl.as_secs().max(1);
        let stale_before = Utc::now() - chrono::Duration::seconds(ttl_seconds as i64);
        let stale_str = stale_before.format("%Y-%m-%d %H:%M:%S").to_string();
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_locks SET owner_id = ?, locked_at = CURRENT_TIMESTAMP WHERE lock_name = ? AND locked_at < ?",
        )
        .bind(owner_id)
        .bind(lock_name)
        .bind(stale_str)
        .execute(self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn touch_lock(&self, lock_name: &str, owner_id: &str) -> Result<bool> {
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_locks SET locked_at = CURRENT_TIMESTAMP WHERE lock_name = ? AND owner_id = ?",
        )
        .bind(lock_name)
        .bind(owner_id)
        .execute(self.pool)
        .await?;
        Ok(updated.rows_affected() > 0)
    }

    pub async fn release_lock(&self, lock_name: &str, owner_id: &str) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "DELETE FROM orchestrator_locks WHERE lock_name = ? AND owner_id = ?",
        )
        .bind(lock_name)
        .bind(owner_id)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn force_release_lock(&self, lock_name: &str) -> Result<u64> {
        let result = sqlx::query::<sqlx::Any>("DELETE FROM orchestrator_locks WHERE lock_name = ?")
            .bind(lock_name)
            .execute(self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn update_run_status(
        &self,
        run_id: Uuid,
        status: OrchestratorRunStatus,
        phase: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_runs SET status = ?, phase = ?, error = ?, started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN CURRENT_TIMESTAMP ELSE started_at END, finished_at = CASE WHEN ? IN ('failed', 'completed', 'canceled') THEN CURRENT_TIMESTAMP ELSE finished_at END WHERE run_id = ?",
        )
        .bind(status.as_str())
        .bind(phase)
        .bind(error)
        .bind(status.as_str())
        .bind(status.as_str())
        .bind(run_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_run_plan(&self, run_id: Uuid, plan_json: serde_json::Value) -> Result<()> {
        let plan_json = json_to_string(Some(&plan_json))?;
        sqlx::query::<sqlx::Any>("UPDATE orchestrator_runs SET plan_json = ? WHERE run_id = ?")
            .bind(plan_json)
            .bind(run_id.to_string())
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn cancel_pending_runs_by_source(
        &self,
        source: &str,
        error: Option<&str>,
    ) -> Result<u64> {
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_runs SET status = 'canceled', phase = 'canceled', error = ?, finished_at = CURRENT_TIMESTAMP WHERE source = ? AND status = 'pending'",
        )
        .bind(error)
        .bind(source)
        .execute(self.pool)
        .await?;
        Ok(updated.rows_affected())
    }

    pub async fn list_runs(&self, limit: Option<i64>) -> Result<Vec<OrchestratorRun>> {
        let rows = if let Some(limit) = limit {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs ORDER BY created_at DESC LIMIT ?",
            )
            .bind(limit)
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs ORDER BY created_at DESC",
            )
            .fetch_all(self.pool)
            .await?
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_run(&row)?);
        }
        Ok(items)
    }

    pub async fn delete_run_history(&self) -> Result<u64> {
        let deleted = sqlx::query::<sqlx::Any>(
            "DELETE FROM orchestrator_runs WHERE status IN ('pending', 'failed', 'completed', 'canceled')",
        )
        .execute(self.pool)
        .await?;
        Ok(deleted.rows_affected())
    }

    pub async fn get_latest_run_by_phase(&self, phase: &str) -> Result<Option<OrchestratorRun>> {
        let row = sqlx::query(
            "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE phase = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(phase)
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_run(&row)).transpose()
    }

    pub async fn get_latest_run_by_source(
        &self,
        source: &str,
        status: Option<OrchestratorRunStatus>,
    ) -> Result<Option<OrchestratorRun>> {
        let row = if let Some(status) = status {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE source = ? AND status = ? ORDER BY created_at DESC LIMIT 1",
            )
            .bind(source)
            .bind(status.as_str())
            .fetch_optional(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE source = ? ORDER BY created_at DESC LIMIT 1",
            )
            .bind(source)
            .fetch_optional(self.pool)
            .await?
        };
        row.map(|row| map_run(&row)).transpose()
    }

    pub async fn list_runs_by_source_status(
        &self,
        source: &str,
        status: OrchestratorRunStatus,
    ) -> Result<Vec<OrchestratorRun>> {
        let rows = sqlx::query(
            "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at
             FROM orchestrator_runs
             WHERE source = ? AND status = ?
             ORDER BY created_at DESC",
        )
        .bind(source)
        .bind(status.as_str())
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_run(&row)?);
        }
        Ok(items)
    }

    pub async fn get_run(&self, run_id: Uuid) -> Result<Option<OrchestratorRun>> {
        let row = sqlx::query(
            "SELECT run_id, CAST(source AS TEXT) as source, status, CAST(phase AS TEXT) as phase, CAST(plan_json AS TEXT) as plan_json, CAST(error AS TEXT) as error, CAST(created_at AS TEXT) as created_at, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM orchestrator_runs WHERE run_id = ? LIMIT 1",
        )
        .bind(run_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_run(&row)).transpose()
    }

    pub async fn reap_stale_running_runs(
        &self,
        stale_before: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<u64> {
        let stale_str = stale_before.format("%Y-%m-%d %H:%M:%S").to_string();
        sqlx::query::<sqlx::Any>(
            "UPDATE operation_steps
             SET status = 'failed',
                 error = COALESCE(error, ?),
                 finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)
             WHERE status = 'running'
               AND run_id IN (
                   SELECT run_id
                   FROM orchestrator_runs
                   WHERE status = 'running'
                     AND created_at < ?
               )",
        )
        .bind(reason)
        .bind(&stale_str)
        .execute(self.pool)
        .await?;

        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_runs
             SET status = 'failed',
                 phase = COALESCE(phase, 'reconcile'),
                 error = COALESCE(error, ?),
                 finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)
             WHERE status = 'running'
               AND created_at < ?",
        )
        .bind(reason)
        .bind(&stale_str)
        .execute(self.pool)
        .await?;

        Ok(updated.rows_affected())
    }

    pub async fn create_step(&self, data: &NewOperationStep) -> Result<()> {
        let action_json = json_to_string(data.action_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO operation_steps (step_id, run_id, step_index, action_type, action_json, status, error, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 'running' THEN CURRENT_TIMESTAMP ELSE NULL END)",
        )
        .bind(data.step_id.to_string())
        .bind(data.run_id.to_string())
        .bind(data.step_index)
        .bind(&data.action_type)
        .bind(action_json)
        .bind(data.status.as_str())
        .bind(data.error.as_deref())
        .bind(data.status.as_str())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_step_status(
        &self,
        step_id: Uuid,
        status: OperationStepStatus,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE operation_steps SET status = ?, error = ?, started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN CURRENT_TIMESTAMP ELSE started_at END, finished_at = CASE WHEN ? IN ('failed', 'completed', 'skipped') THEN CURRENT_TIMESTAMP ELSE finished_at END WHERE step_id = ?",
        )
        .bind(status.as_str())
        .bind(error)
        .bind(status.as_str())
        .bind(status.as_str())
        .bind(step_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_steps(&self, run_id: Uuid) -> Result<Vec<OperationStep>> {
        let rows = sqlx::query(
            "SELECT step_id, run_id, step_index, action_type, CAST(action_json AS TEXT) as action_json, status, CAST(error AS TEXT) as error, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at, CAST(created_at AS TEXT) as created_at FROM operation_steps WHERE run_id = ? ORDER BY step_index",
        )
        .bind(run_id.to_string())
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_step(&row)?);
        }
        Ok(items)
    }

    pub async fn create_runtime_log(&self, data: &NewRuntimeLog) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO runtime_logs (log_id, instance_id, log_uri) VALUES (?, ?, ?)",
        )
        .bind(data.log_id.to_string())
        .bind(data.instance_id.to_string())
        .bind(&data.log_uri)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_runtime_logs(&self, instance_id: Uuid) -> Result<Vec<RuntimeLog>> {
        let rows = sqlx::query(
            "SELECT log_id, instance_id, log_uri, CAST(created_at AS TEXT) as created_at FROM runtime_logs WHERE instance_id = ? ORDER BY created_at DESC",
        )
        .bind(instance_id.to_string())
        .fetch_all(self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(map_runtime_log(&row)?);
        }
        Ok(items)
    }

    pub async fn get_extension_setting(&self, key: &str) -> Result<Option<serde_json::Value>> {
        Ok(self
            .get_extension_setting_record(key)
            .await?
            .map(|record| record.value_json))
    }

    pub async fn get_extension_setting_record(
        &self,
        key: &str,
    ) -> Result<Option<ExtensionSettingRecord>> {
        let row = sqlx::query(
            "SELECT setting_key, CAST(value_json AS TEXT) as value_json, CAST(updated_at AS TEXT) as updated_at FROM extension_settings WHERE setting_key = ? LIMIT 1",
        )
        .bind(key)
        .fetch_optional(self.pool)
        .await?;
        row.map(|row| map_extension_setting_record(&row))
            .transpose()
    }

    pub async fn upsert_extension_setting(
        &self,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<()> {
        let value_json = json_to_string(Some(value))?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_settings (setting_key, value_json) VALUES (?, ?) ON CONFLICT(setting_key) DO UPDATE SET value_json = excluded.value_json, updated_at = CURRENT_TIMESTAMP",
        )
        .bind(key)
        .bind(value_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_extension_setting(&self, key: &str) -> Result<()> {
        sqlx::query::<sqlx::Any>("DELETE FROM extension_settings WHERE setting_key = ?")
            .bind(key)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_source_registry(&self, data: &NewExtensionSourceRegistry) -> Result<()> {
        validate_source_registry(data)?;
        let metadata_json = json_to_string(data.metadata_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_registries (
                registry_id,
                instance_id,
                registry_key,
                registry_type,
                trust_class,
                display_name,
                url,
                enabled,
                auto_refresh,
                trusted_for_executable_updates,
                etag,
                last_modified,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(instance_id, registry_key) DO UPDATE SET
                registry_type = excluded.registry_type,
                trust_class = excluded.trust_class,
                display_name = excluded.display_name,
                url = excluded.url,
                enabled = excluded.enabled,
                auto_refresh = excluded.auto_refresh,
                trusted_for_executable_updates = excluded.trusted_for_executable_updates,
                etag = excluded.etag,
                last_modified = excluded.last_modified,
                metadata_json = excluded.metadata_json,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.registry_id.to_string())
        .bind(data.instance_id.to_string())
        .bind(data.registry_key.trim())
        .bind(data.registry_type.trim())
        .bind(data.trust_class.trim())
        .bind(data.display_name.trim())
        .bind(data.url.as_deref().map(str::trim))
        .bind(data.enabled)
        .bind(data.auto_refresh)
        .bind(data.trusted_for_executable_updates)
        .bind(data.etag.as_deref().map(str::trim))
        .bind(data.last_modified.as_deref().map(str::trim))
        .bind(metadata_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_source_registries(
        &self,
        instance_id: Option<Uuid>,
    ) -> Result<Vec<ExtensionSourceRegistry>> {
        let rows = if let Some(instance_id) = instance_id {
            sqlx::query(
                "SELECT
                    registry_id,
                    instance_id,
                    registry_key,
                    registry_type,
                    trust_class,
                    display_name,
                    CAST(url AS TEXT) AS url,
                    CAST(enabled AS INTEGER) AS enabled,
                    CAST(auto_refresh AS INTEGER) AS auto_refresh,
                    CAST(trusted_for_executable_updates AS INTEGER) AS trusted_for_executable_updates,
                    CAST(etag AS TEXT) AS etag,
                    CAST(last_modified AS TEXT) AS last_modified,
                    last_fetch_status,
                    CAST(last_fetch_error AS TEXT) AS last_fetch_error,
                    CAST(last_fetched_at AS TEXT) AS last_fetched_at,
                    CAST(metadata_json AS TEXT) AS metadata_json,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
                 FROM extension_source_registries
                 WHERE instance_id = ?
                 ORDER BY created_at ASC",
            )
            .bind(instance_id.to_string())
            .fetch_all(self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT
                    registry_id,
                    instance_id,
                    registry_key,
                    registry_type,
                    trust_class,
                    display_name,
                    CAST(url AS TEXT) AS url,
                    CAST(enabled AS INTEGER) AS enabled,
                    CAST(auto_refresh AS INTEGER) AS auto_refresh,
                    CAST(trusted_for_executable_updates AS INTEGER) AS trusted_for_executable_updates,
                    CAST(etag AS TEXT) AS etag,
                    CAST(last_modified AS TEXT) AS last_modified,
                    last_fetch_status,
                    CAST(last_fetch_error AS TEXT) AS last_fetch_error,
                    CAST(last_fetched_at AS TEXT) AS last_fetched_at,
                    CAST(metadata_json AS TEXT) AS metadata_json,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(updated_at AS TEXT) AS updated_at
                 FROM extension_source_registries
                 ORDER BY created_at ASC",
            )
            .fetch_all(self.pool)
            .await?
        };
        rows.iter().map(map_source_registry).collect()
    }

    pub async fn record_source_registry_fetch(
        &self,
        registry_id: Uuid,
        status: &str,
        error: Option<&str>,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<()> {
        validate_allowed_value(
            "extension_source_registries.last_fetch_status",
            status,
            SOURCE_REGISTRY_FETCH_STATUSES,
        )?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_registries
             SET last_fetch_status = ?,
                 last_fetch_error = ?,
                 etag = COALESCE(?, etag),
                 last_modified = COALESCE(?, last_modified),
                 last_fetched_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE registry_id = ?",
        )
        .bind(status.trim())
        .bind(error.map(str::trim))
        .bind(etag.map(str::trim))
        .bind(last_modified.map(str::trim))
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_source_registry_enabled_state(
        &self,
        registry_id: Uuid,
        enabled: bool,
        auto_refresh: bool,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_registries
             SET enabled = ?,
                 auto_refresh = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE registry_id = ?",
        )
        .bind(enabled)
        .bind(auto_refresh)
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_source_registry_trust(
        &self,
        registry_id: Uuid,
        trust_class: &str,
        trusted_for_executable_updates: bool,
    ) -> Result<()> {
        validate_allowed_value(
            "extension_source_registries.trust_class",
            trust_class,
            SOURCE_REGISTRY_TRUST_CLASSES,
        )?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_registries
             SET trust_class = ?,
                 trusted_for_executable_updates = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE registry_id = ?",
        )
        .bind(trust_class.trim())
        .bind(trusted_for_executable_updates)
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_source_registry(&self, registry_id: Uuid) -> Result<u64> {
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_certification_jobs
             WHERE registry_id = ?
                OR source_module_id IN (
                    SELECT source_module_id
                    FROM extension_source_modules
                    WHERE registry_id = ?
                )",
        )
        .bind(registry_id.to_string())
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_replacement_recommendations
             WHERE replacement_registry_id = ?
                OR source_module_id IN (
                    SELECT source_module_id
                    FROM extension_source_modules
                    WHERE registry_id = ?
                )
                OR replacement_source_module_id IN (
                    SELECT source_module_id
                    FROM extension_source_modules
                    WHERE registry_id = ?
                )",
        )
        .bind(registry_id.to_string())
        .bind(registry_id.to_string())
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_module_quarantines
             WHERE source_module_id IN (
                SELECT source_module_id
                FROM extension_source_modules
                WHERE registry_id = ?
             )",
        )
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_module_certifications
             WHERE source_module_id IN (
                SELECT source_module_id
                FROM extension_source_modules
                WHERE registry_id = ?
             )",
        )
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_health_events
             WHERE source_module_id IN (
                SELECT source_module_id
                FROM extension_source_modules
                WHERE registry_id = ?
             )",
        )
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_module_versions
             WHERE source_module_id IN (
                SELECT source_module_id
                FROM extension_source_modules
                WHERE registry_id = ?
             )",
        )
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_modules
             WHERE registry_id = ?",
        )
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        let affected = sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_registries
             WHERE registry_id = ?",
        )
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?
        .rows_affected();
        Ok(affected)
    }

    pub async fn delete_orphan_source_modules_for_instance(
        &self,
        instance_id: Uuid,
    ) -> Result<u64> {
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_certification_jobs
             WHERE instance_id = ?
               AND (
                    registry_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM extension_source_registries registry
                        WHERE registry.registry_id = extension_source_certification_jobs.registry_id
                    )
                    OR source_module_id IN (
                        SELECT module.source_module_id
                        FROM extension_source_modules module
                        WHERE module.instance_id = ?
                          AND NOT EXISTS (
                              SELECT 1
                              FROM extension_source_registries registry
                              WHERE registry.registry_id = module.registry_id
                          )
                    )
               )",
        )
        .bind(instance_id.to_string())
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_replacement_recommendations
             WHERE source_module_id IN (
                    SELECT module.source_module_id
                    FROM extension_source_modules module
                    WHERE module.instance_id = ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM extension_source_registries registry
                          WHERE registry.registry_id = module.registry_id
                      )
             )
                OR replacement_source_module_id IN (
                    SELECT module.source_module_id
                    FROM extension_source_modules module
                    WHERE module.instance_id = ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM extension_source_registries registry
                          WHERE registry.registry_id = module.registry_id
                      )
             )",
        )
        .bind(instance_id.to_string())
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_module_quarantines
             WHERE source_module_id IN (
                SELECT module.source_module_id
                FROM extension_source_modules module
                WHERE module.instance_id = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM extension_source_registries registry
                      WHERE registry.registry_id = module.registry_id
                  )
             )",
        )
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_module_certifications
             WHERE source_module_id IN (
                SELECT module.source_module_id
                FROM extension_source_modules module
                WHERE module.instance_id = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM extension_source_registries registry
                      WHERE registry.registry_id = module.registry_id
                  )
             )",
        )
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_health_events
             WHERE source_module_id IN (
                SELECT module.source_module_id
                FROM extension_source_modules module
                WHERE module.instance_id = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM extension_source_registries registry
                      WHERE registry.registry_id = module.registry_id
                  )
             )",
        )
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_module_versions
             WHERE source_module_id IN (
                SELECT module.source_module_id
                FROM extension_source_modules module
                WHERE module.instance_id = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM extension_source_registries registry
                      WHERE registry.registry_id = module.registry_id
                  )
             )",
        )
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?;
        let affected = sqlx::query::<sqlx::Any>(
            "DELETE FROM extension_source_modules
             WHERE instance_id = ?
               AND NOT EXISTS (
                   SELECT 1
                   FROM extension_source_registries registry
                   WHERE registry.registry_id = extension_source_modules.registry_id
               )",
        )
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?
        .rows_affected();
        Ok(affected)
    }

    pub async fn upsert_source_module(&self, data: &NewExtensionSourceModule) -> Result<()> {
        validate_source_module(data)?;
        let media_types_json = json_to_string(data.media_types_json.as_ref())?;
        let language_tags_json = json_to_string(data.language_tags_json.as_ref())?;
        let region_tags_json = json_to_string(data.region_tags_json.as_ref())?;
        let source_domains_json = json_to_string(data.source_domains_json.as_ref())?;
        let metadata_json = json_to_string(data.metadata_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_modules (
                source_module_id,
                instance_id,
                registry_id,
                module_key,
                display_name,
                ecosystem,
                plugin_package,
                active_version,
                rollback_version,
                media_types_json,
                language_tags_json,
                region_tags_json,
                source_domains_json,
                account_required,
                unsupported,
                unsupported_reason,
                enabled,
                installed,
                pinned_version,
                health_state,
                replacement_recommendation_key,
                last_error,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(instance_id, module_key) DO UPDATE SET
                registry_id = excluded.registry_id,
                display_name = excluded.display_name,
                ecosystem = excluded.ecosystem,
                plugin_package = excluded.plugin_package,
                active_version = excluded.active_version,
                rollback_version = excluded.rollback_version,
                media_types_json = excluded.media_types_json,
                language_tags_json = excluded.language_tags_json,
                region_tags_json = excluded.region_tags_json,
                source_domains_json = excluded.source_domains_json,
                account_required = excluded.account_required,
                unsupported = excluded.unsupported,
                unsupported_reason = excluded.unsupported_reason,
                enabled = excluded.enabled,
                installed = excluded.installed,
                pinned_version = excluded.pinned_version,
                health_state = excluded.health_state,
                replacement_recommendation_key = excluded.replacement_recommendation_key,
                last_error = excluded.last_error,
                metadata_json = excluded.metadata_json,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.source_module_id.to_string())
        .bind(data.instance_id.to_string())
        .bind(data.registry_id.to_string())
        .bind(data.module_key.trim())
        .bind(data.display_name.trim())
        .bind(data.ecosystem.trim())
        .bind(data.plugin_package.as_deref().map(str::trim))
        .bind(data.active_version.as_deref().map(str::trim))
        .bind(data.rollback_version.as_deref().map(str::trim))
        .bind(media_types_json)
        .bind(language_tags_json)
        .bind(region_tags_json)
        .bind(source_domains_json)
        .bind(data.account_required)
        .bind(data.unsupported)
        .bind(data.unsupported_reason.as_deref().map(str::trim))
        .bind(data.enabled)
        .bind(data.installed)
        .bind(data.pinned_version.as_deref().map(str::trim))
        .bind(data.health_state.trim())
        .bind(
            data.replacement_recommendation_key
                .as_deref()
                .map(str::trim),
        )
        .bind(data.last_error.as_deref().map(str::trim))
        .bind(metadata_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_source_modules(
        &self,
        instance_id: Option<Uuid>,
        registry_id: Option<Uuid>,
    ) -> Result<Vec<ExtensionSourceModule>> {
        let base = "SELECT
                source_module_id,
                instance_id,
                registry_id,
                module_key,
                display_name,
                ecosystem,
                CAST(plugin_package AS TEXT) AS plugin_package,
                CAST(active_version AS TEXT) AS active_version,
                CAST(rollback_version AS TEXT) AS rollback_version,
                CAST(media_types_json AS TEXT) AS media_types_json,
                CAST(language_tags_json AS TEXT) AS language_tags_json,
                CAST(region_tags_json AS TEXT) AS region_tags_json,
                CAST(source_domains_json AS TEXT) AS source_domains_json,
                CAST(account_required AS INTEGER) AS account_required,
                CAST(unsupported AS INTEGER) AS unsupported,
                CAST(unsupported_reason AS TEXT) AS unsupported_reason,
                CAST(enabled AS INTEGER) AS enabled,
                CAST(installed AS INTEGER) AS installed,
                CAST(pinned_version AS TEXT) AS pinned_version,
                health_state,
                CAST(replacement_recommendation_key AS TEXT) AS replacement_recommendation_key,
                CAST(last_success_at AS TEXT) AS last_success_at,
                CAST(last_failure_at AS TEXT) AS last_failure_at,
                CAST(last_error AS TEXT) AS last_error,
                CAST(metadata_json AS TEXT) AS metadata_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
            FROM extension_source_modules";
        let rows = match (instance_id, registry_id) {
            (Some(instance_id), Some(registry_id)) => {
                sqlx::query(&format!(
                    "{base} WHERE instance_id = ? AND registry_id = ? ORDER BY display_name ASC"
                ))
                .bind(instance_id.to_string())
                .bind(registry_id.to_string())
                .fetch_all(self.pool)
                .await?
            }
            (Some(instance_id), None) => {
                sqlx::query(&format!(
                    "{base} WHERE instance_id = ? ORDER BY display_name ASC"
                ))
                .bind(instance_id.to_string())
                .fetch_all(self.pool)
                .await?
            }
            (None, Some(registry_id)) => {
                sqlx::query(&format!(
                    "{base} WHERE registry_id = ? ORDER BY display_name ASC"
                ))
                .bind(registry_id.to_string())
                .fetch_all(self.pool)
                .await?
            }
            (None, None) => {
                sqlx::query(&format!("{base} ORDER BY display_name ASC"))
                    .fetch_all(self.pool)
                    .await?
            }
        };
        rows.iter().map(map_source_module).collect()
    }

    pub async fn set_source_modules_enabled_for_registry(
        &self,
        registry_id: Uuid,
        enabled: bool,
        health_state: &str,
        last_error: Option<&str>,
    ) -> Result<()> {
        validate_allowed_value(
            "extension_source_modules.health_state",
            health_state,
            SOURCE_MODULE_HEALTH_STATES,
        )?;
        let failure_state = matches!(
            health_state,
            "degraded" | "broken" | "unsupported" | "account_required" | "disabled"
        );
        let clears_last_error = matches!(health_state, "available" | "healthy");
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_modules
             SET enabled = ?,
                 health_state = CASE
                    WHEN unsupported = 1 THEN 'unsupported'
                    WHEN account_required = 1 THEN 'account_required'
                    ELSE ?
                 END,
                 last_failure_at = CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE last_failure_at END,
                 last_error = CASE
                    WHEN ? = 1 THEN COALESCE(?, ?)
                    WHEN ? = 1 THEN NULL
                    ELSE last_error
                 END,
                 updated_at = CURRENT_TIMESTAMP
             WHERE registry_id = ?",
        )
        .bind(enabled)
        .bind(health_state.trim())
        .bind(if failure_state { 1_i64 } else { 0_i64 })
        .bind(if failure_state { 1_i64 } else { 0_i64 })
        .bind(last_error.map(str::trim))
        .bind(health_state.trim())
        .bind(if clears_last_error { 1_i64 } else { 0_i64 })
        .bind(registry_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_source_module_active_version(
        &self,
        source_module_id: Uuid,
        active_version: Option<&str>,
        rollback_version: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_modules
             SET active_version = ?,
                 rollback_version = ?,
                 installed = CASE WHEN ? IS NULL THEN installed ELSE 1 END,
                 updated_at = CURRENT_TIMESTAMP
             WHERE source_module_id = ?",
        )
        .bind(active_version.map(str::trim))
        .bind(rollback_version.map(str::trim))
        .bind(active_version.map(str::trim))
        .bind(source_module_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_source_module_pinned_version(
        &self,
        source_module_id: Uuid,
        pinned_version: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_modules
             SET pinned_version = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE source_module_id = ?",
        )
        .bind(pinned_version.map(str::trim))
        .bind(source_module_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_source_module_enabled_state(
        &self,
        source_module_id: Uuid,
        enabled: bool,
        health_state: &str,
        last_error: Option<&str>,
    ) -> Result<()> {
        validate_allowed_value(
            "extension_source_modules.health_state",
            health_state,
            SOURCE_MODULE_HEALTH_STATES,
        )?;
        let failure_state = matches!(
            health_state,
            "degraded" | "broken" | "unsupported" | "account_required" | "disabled"
        );
        let clears_last_error = matches!(health_state, "available" | "healthy");
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_modules
             SET enabled = ?,
                 health_state = ?,
                 last_failure_at = CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE last_failure_at END,
                 last_error = CASE
                    WHEN ? = 1 THEN COALESCE(?, ?)
                    WHEN ? = 1 THEN NULL
                    ELSE last_error
                 END,
                 updated_at = CURRENT_TIMESTAMP
             WHERE source_module_id = ?",
        )
        .bind(enabled)
        .bind(health_state.trim())
        .bind(if failure_state { 1_i64 } else { 0_i64 })
        .bind(if failure_state { 1_i64 } else { 0_i64 })
        .bind(last_error.map(str::trim))
        .bind(health_state.trim())
        .bind(if clears_last_error { 1_i64 } else { 0_i64 })
        .bind(source_module_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_source_module_installed_state(
        &self,
        source_module_id: Uuid,
        installed: bool,
        active_version: Option<&str>,
        health_state: &str,
        last_error: Option<&str>,
    ) -> Result<()> {
        validate_allowed_value(
            "extension_source_modules.health_state",
            health_state,
            SOURCE_MODULE_HEALTH_STATES,
        )?;
        let failure_state = matches!(
            health_state,
            "degraded" | "broken" | "unsupported" | "account_required" | "disabled"
        );
        let clears_last_error = matches!(health_state, "available" | "healthy");
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_modules
             SET installed = ?,
                 active_version = ?,
                 health_state = ?,
                 last_failure_at = CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE last_failure_at END,
                 last_error = CASE
                    WHEN ? = 1 THEN COALESCE(?, ?)
                    WHEN ? = 1 THEN NULL
                    ELSE last_error
                 END,
                 updated_at = CURRENT_TIMESTAMP
             WHERE source_module_id = ?",
        )
        .bind(installed)
        .bind(active_version.map(str::trim))
        .bind(health_state.trim())
        .bind(if failure_state { 1_i64 } else { 0_i64 })
        .bind(if failure_state { 1_i64 } else { 0_i64 })
        .bind(last_error.map(str::trim))
        .bind(health_state.trim())
        .bind(if clears_last_error { 1_i64 } else { 0_i64 })
        .bind(source_module_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_source_module_replacement_recommendation_key(
        &self,
        source_module_id: Uuid,
        recommendation_key: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_modules
             SET replacement_recommendation_key = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE source_module_id = ?",
        )
        .bind(recommendation_key.map(str::trim))
        .bind(source_module_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_source_module_version(
        &self,
        data: &NewExtensionSourceModuleVersion,
    ) -> Result<()> {
        validate_source_module_version(data)?;
        let metadata_json = json_to_string(data.metadata_json.as_ref())?;
        let installed_at = data.installed_at.map(db_datetime_string);
        let activated_at = data.activated_at.map(db_datetime_string);
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_module_versions (
                version_id,
                source_module_id,
                version,
                artifact_url,
                artifact_sha256,
                signature,
                install_state,
                smoke_status,
                smoke_error,
                rollback_of_version_id,
                installed_at,
                activated_at,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(source_module_id, version) DO UPDATE SET
                artifact_url = excluded.artifact_url,
                artifact_sha256 = excluded.artifact_sha256,
                signature = excluded.signature,
                install_state = excluded.install_state,
                smoke_status = excluded.smoke_status,
                smoke_error = excluded.smoke_error,
                rollback_of_version_id = excluded.rollback_of_version_id,
                installed_at = excluded.installed_at,
                activated_at = excluded.activated_at,
                metadata_json = excluded.metadata_json,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.version_id.to_string())
        .bind(data.source_module_id.to_string())
        .bind(data.version.trim())
        .bind(data.artifact_url.as_deref().map(str::trim))
        .bind(data.artifact_sha256.as_deref().map(str::trim))
        .bind(data.signature.as_deref().map(str::trim))
        .bind(data.install_state.trim())
        .bind(data.smoke_status.trim())
        .bind(data.smoke_error.as_deref().map(str::trim))
        .bind(data.rollback_of_version_id.map(|id| id.to_string()))
        .bind(installed_at)
        .bind(activated_at)
        .bind(metadata_json)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_source_module_versions(
        &self,
        source_module_id: Uuid,
    ) -> Result<Vec<ExtensionSourceModuleVersion>> {
        let rows = sqlx::query(
            "SELECT
                version_id,
                source_module_id,
                version,
                CAST(artifact_url AS TEXT) AS artifact_url,
                CAST(artifact_sha256 AS TEXT) AS artifact_sha256,
                CAST(signature AS TEXT) AS signature,
                install_state,
                smoke_status,
                CAST(smoke_error AS TEXT) AS smoke_error,
                CAST(rollback_of_version_id AS TEXT) AS rollback_of_version_id,
                CAST(installed_at AS TEXT) AS installed_at,
                CAST(activated_at AS TEXT) AS activated_at,
                CAST(metadata_json AS TEXT) AS metadata_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_module_versions
             WHERE source_module_id = ?
             ORDER BY created_at ASC",
        )
        .bind(source_module_id.to_string())
        .fetch_all(self.pool)
        .await?;
        rows.iter().map(map_source_module_version).collect()
    }

    pub async fn set_source_module_version_state(
        &self,
        version_id: Uuid,
        install_state: &str,
        smoke_status: &str,
        smoke_error: Option<&str>,
    ) -> Result<()> {
        validate_allowed_value(
            "extension_source_module_versions.install_state",
            install_state,
            SOURCE_MODULE_VERSION_STATES,
        )?;
        validate_allowed_value(
            "extension_source_module_versions.smoke_status",
            smoke_status,
            SOURCE_MODULE_SMOKE_STATUSES,
        )?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_module_versions
             SET install_state = ?,
                 smoke_status = ?,
                 smoke_error = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE version_id = ?",
        )
        .bind(install_state.trim())
        .bind(smoke_status.trim())
        .bind(smoke_error.map(str::trim))
        .bind(version_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_source_health_event(
        &self,
        data: &NewExtensionSourceHealthEvent,
    ) -> Result<()> {
        validate_source_health_event(data)?;
        let evidence_json = json_to_string(data.evidence_json.as_ref())?;
        let observed_at = data.observed_at.map(db_datetime_string);
        let state = data.state.trim();
        let reason = data
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_health_events (
                health_event_id,
                source_module_id,
                event_type,
                state,
                severity,
                reason,
                evidence_json,
                observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))",
        )
        .bind(data.health_event_id.to_string())
        .bind(data.source_module_id.to_string())
        .bind(data.event_type.trim())
        .bind(state)
        .bind(data.severity.trim())
        .bind(reason)
        .bind(evidence_json)
        .bind(observed_at.as_deref())
        .execute(self.pool)
        .await?;

        let success_event = state == "healthy";
        let failure_event = matches!(
            state,
            "degraded" | "broken" | "unsupported" | "account_required"
        );
        let clears_last_error = matches!(state, "available" | "healthy");
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_modules
             SET health_state = ?,
                 last_success_at = CASE WHEN ? = 1 THEN COALESCE(?, CURRENT_TIMESTAMP) ELSE last_success_at END,
                 last_failure_at = CASE WHEN ? = 1 THEN COALESCE(?, CURRENT_TIMESTAMP) ELSE last_failure_at END,
                 last_error = CASE
                    WHEN ? = 1 THEN COALESCE(?, ?)
                    WHEN ? = 1 THEN NULL
                    ELSE last_error
                 END,
                 updated_at = CURRENT_TIMESTAMP
             WHERE source_module_id = ?",
        )
        .bind(state)
        .bind(if success_event { 1_i64 } else { 0_i64 })
        .bind(observed_at.as_deref())
        .bind(if failure_event { 1_i64 } else { 0_i64 })
        .bind(observed_at.as_deref())
        .bind(if failure_event { 1_i64 } else { 0_i64 })
        .bind(reason)
        .bind(state)
        .bind(if clears_last_error { 1_i64 } else { 0_i64 })
        .bind(data.source_module_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_source_health_events(
        &self,
        source_module_id: Uuid,
        limit: i64,
    ) -> Result<Vec<ExtensionSourceHealthEvent>> {
        let rows = sqlx::query(
            "SELECT
                health_event_id,
                source_module_id,
                event_type,
                state,
                severity,
                CAST(reason AS TEXT) AS reason,
                CAST(evidence_json AS TEXT) AS evidence_json,
                CAST(observed_at AS TEXT) AS observed_at,
                CAST(created_at AS TEXT) AS created_at
             FROM extension_source_health_events
             WHERE source_module_id = ?
             ORDER BY observed_at DESC, created_at DESC
             LIMIT ?",
        )
        .bind(source_module_id.to_string())
        .bind(limit.max(1))
        .fetch_all(self.pool)
        .await?;
        rows.iter().map(map_source_health_event).collect()
    }

    pub async fn upsert_source_module_certification(
        &self,
        data: &NewExtensionSourceModuleCertification,
    ) -> Result<()> {
        validate_source_module_certification(data)?;
        let media_type_results_json = json_to_string(Some(&data.media_type_results_json))?;
        let materialization_results_json =
            json_to_string(Some(&data.materialization_results_json))?;
        let probe_targets_json = json_to_string(Some(&data.probe_targets_json))?;
        let candidate_evidence_json = json_to_string(Some(&data.candidate_evidence_json))?;
        let certified_at = data.certified_at.map(db_datetime_string);
        let expires_at = data.expires_at.map(db_datetime_string);
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_module_certifications (
                certification_id,
                source_module_id,
                source_module_version_id,
                artifact_sha256,
                instance_id,
                adapter,
                status,
                failure_class,
                summary,
                media_type_results_json,
                materialization_results_json,
                probe_targets_json,
                candidate_evidence_json,
                runtime_version,
                policy_version,
                certified_at,
                expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(source_module_id, source_module_version_id, instance_id, adapter) DO UPDATE SET
                certification_id = excluded.certification_id,
                artifact_sha256 = excluded.artifact_sha256,
                status = excluded.status,
                failure_class = excluded.failure_class,
                summary = excluded.summary,
                media_type_results_json = excluded.media_type_results_json,
                materialization_results_json = excluded.materialization_results_json,
                probe_targets_json = excluded.probe_targets_json,
                candidate_evidence_json = excluded.candidate_evidence_json,
                runtime_version = excluded.runtime_version,
                policy_version = excluded.policy_version,
                certified_at = excluded.certified_at,
                expires_at = excluded.expires_at,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.certification_id.to_string())
        .bind(data.source_module_id.to_string())
        .bind(data.source_module_version_id.map(|id| id.to_string()))
        .bind(data.artifact_sha256.as_deref().map(str::trim))
        .bind(data.instance_id.to_string())
        .bind(data.adapter.trim())
        .bind(data.status.trim())
        .bind(data.failure_class.as_deref().map(str::trim))
        .bind(data.summary.as_deref().map(str::trim))
        .bind(media_type_results_json)
        .bind(materialization_results_json)
        .bind(probe_targets_json)
        .bind(candidate_evidence_json)
        .bind(data.runtime_version.as_deref().map(str::trim))
        .bind(data.policy_version.trim())
        .bind(certified_at.as_deref())
        .bind(expires_at.as_deref())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_latest_source_module_certifications(
        &self,
        instance_id: Uuid,
    ) -> Result<Vec<ExtensionSourceModuleCertification>> {
        let rows = sqlx::query(
            "SELECT
                certification_id,
                source_module_id,
                CAST(source_module_version_id AS TEXT) AS source_module_version_id,
                CAST(artifact_sha256 AS TEXT) AS artifact_sha256,
                instance_id,
                adapter,
                status,
                CAST(failure_class AS TEXT) AS failure_class,
                CAST(summary AS TEXT) AS summary,
                media_type_results_json,
                materialization_results_json,
                probe_targets_json,
                candidate_evidence_json,
                CAST(runtime_version AS TEXT) AS runtime_version,
                policy_version,
                CAST(certified_at AS TEXT) AS certified_at,
                CAST(expires_at AS TEXT) AS expires_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_module_certifications
             WHERE instance_id = ?
             ORDER BY updated_at DESC, created_at DESC",
        )
        .bind(instance_id.to_string())
        .fetch_all(self.pool)
        .await?;

        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for row in rows {
            let certification = map_source_module_certification(&row)?;
            if seen.insert(certification.source_module_id) {
                out.push(certification);
            }
        }
        Ok(out)
    }

    pub async fn latest_source_module_certification(
        &self,
        source_module_id: Uuid,
    ) -> Result<Option<ExtensionSourceModuleCertification>> {
        let row = sqlx::query(
            "SELECT
                certification_id,
                source_module_id,
                CAST(source_module_version_id AS TEXT) AS source_module_version_id,
                CAST(artifact_sha256 AS TEXT) AS artifact_sha256,
                instance_id,
                adapter,
                status,
                CAST(failure_class AS TEXT) AS failure_class,
                CAST(summary AS TEXT) AS summary,
                media_type_results_json,
                materialization_results_json,
                probe_targets_json,
                candidate_evidence_json,
                CAST(runtime_version AS TEXT) AS runtime_version,
                policy_version,
                CAST(certified_at AS TEXT) AS certified_at,
                CAST(expires_at AS TEXT) AS expires_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_module_certifications
             WHERE source_module_id = ?
             ORDER BY updated_at DESC, created_at DESC
             LIMIT 1",
        )
        .bind(source_module_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.as_ref()
            .map(map_source_module_certification)
            .transpose()
    }

    pub async fn list_source_module_certifications(
        &self,
        source_module_id: Uuid,
        limit: i64,
    ) -> Result<Vec<ExtensionSourceModuleCertification>> {
        let rows = sqlx::query(
            "SELECT
                certification_id,
                source_module_id,
                CAST(source_module_version_id AS TEXT) AS source_module_version_id,
                CAST(artifact_sha256 AS TEXT) AS artifact_sha256,
                instance_id,
                adapter,
                status,
                CAST(failure_class AS TEXT) AS failure_class,
                CAST(summary AS TEXT) AS summary,
                media_type_results_json,
                materialization_results_json,
                probe_targets_json,
                candidate_evidence_json,
                CAST(runtime_version AS TEXT) AS runtime_version,
                policy_version,
                CAST(certified_at AS TEXT) AS certified_at,
                CAST(expires_at AS TEXT) AS expires_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_module_certifications
             WHERE source_module_id = ?
             ORDER BY updated_at DESC, created_at DESC
             LIMIT ?",
        )
        .bind(source_module_id.to_string())
        .bind(limit.max(1))
        .fetch_all(self.pool)
        .await?;
        rows.iter().map(map_source_module_certification).collect()
    }

    pub async fn create_source_certification_job(
        &self,
        data: &NewExtensionSourceCertificationJob,
    ) -> Result<()> {
        validate_source_certification_job(data)?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_certification_jobs (
                job_id,
                instance_id,
                registry_id,
                source_module_id,
                requested_by,
                reason,
                status,
                priority,
                attempts,
                max_attempts,
                language_eligibility,
                marketplace_state,
                summary,
                last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(data.job_id.to_string())
        .bind(data.instance_id.to_string())
        .bind(data.registry_id.map(|id| id.to_string()))
        .bind(data.source_module_id.map(|id| id.to_string()))
        .bind(data.requested_by.trim())
        .bind(data.reason.trim())
        .bind(data.status.trim())
        .bind(data.priority)
        .bind(data.attempts)
        .bind(data.max_attempts)
        .bind(data.language_eligibility.as_deref().map(str::trim))
        .bind(data.marketplace_state.as_deref().map(str::trim))
        .bind(data.summary.as_deref().map(str::trim))
        .bind(data.last_error.as_deref().map(str::trim))
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn claim_next_source_certification_job(
        &self,
        instance_id: Uuid,
    ) -> Result<Option<ExtensionSourceCertificationJob>> {
        let row = sqlx::query(
            "SELECT
                job_id
             FROM extension_source_certification_jobs
             WHERE instance_id = ?
               AND status = 'queued'
               AND attempts < max_attempts
             ORDER BY priority ASC, created_at ASC, updated_at ASC
             LIMIT 1",
        )
        .bind(instance_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let job_id_raw: String = row.try_get("job_id")?;
        let job_id = parse_uuid(&job_id_raw, "extension_source_certification_jobs.job_id")?;
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_certification_jobs
             SET status = 'running',
                 attempts = attempts + 1,
                 started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                 finished_at = NULL,
                 updated_at = CURRENT_TIMESTAMP
             WHERE job_id = ?
               AND status = 'queued'
               AND attempts < max_attempts",
        )
        .bind(job_id.to_string())
        .execute(self.pool)
        .await?
        .rows_affected();
        if updated == 0 {
            return Ok(None);
        }
        self.get_source_certification_job(job_id).await
    }

    pub async fn get_source_certification_job(
        &self,
        job_id: Uuid,
    ) -> Result<Option<ExtensionSourceCertificationJob>> {
        let row = sqlx::query(
            "SELECT
                job_id,
                instance_id,
                CAST(registry_id AS TEXT) AS registry_id,
                CAST(source_module_id AS TEXT) AS source_module_id,
                requested_by,
                reason,
                status,
                priority,
                attempts,
                max_attempts,
                CAST(language_eligibility AS TEXT) AS language_eligibility,
                CAST(marketplace_state AS TEXT) AS marketplace_state,
                CAST(summary AS TEXT) AS summary,
                CAST(started_at AS TEXT) AS started_at,
                CAST(finished_at AS TEXT) AS finished_at,
                CAST(last_error AS TEXT) AS last_error,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_certification_jobs
             WHERE job_id = ?",
        )
        .bind(job_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        row.as_ref().map(map_source_certification_job).transpose()
    }

    pub async fn requeue_running_source_certification_jobs(
        &self,
        instance_id: Uuid,
        reason: &str,
    ) -> Result<u64> {
        let affected = sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_certification_jobs
             SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
                 summary = ?,
                 last_error = ?,
                 finished_at = CASE WHEN attempts >= max_attempts THEN CURRENT_TIMESTAMP ELSE NULL END,
                 updated_at = CURRENT_TIMESTAMP
             WHERE instance_id = ?
               AND status = 'running'",
        )
        .bind(reason.trim())
        .bind(reason.trim())
        .bind(instance_id.to_string())
        .execute(self.pool)
        .await?
        .rows_affected();
        Ok(affected)
    }

    pub async fn finish_source_certification_job(
        &self,
        job_id: Uuid,
        status: &str,
        marketplace_state: Option<&str>,
        summary: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<()> {
        validate_allowed_value(
            "extension_source_certification_jobs.status",
            status,
            SOURCE_CERTIFICATION_JOB_STATUSES,
        )?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_certification_jobs
             SET status = ?,
                 marketplace_state = COALESCE(?, marketplace_state),
                 summary = ?,
                 last_error = ?,
                 finished_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE job_id = ?",
        )
        .bind(status.trim())
        .bind(marketplace_state.map(str::trim))
        .bind(summary.map(str::trim))
        .bind(last_error.map(str::trim))
        .bind(job_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn cancel_source_certification_jobs(
        &self,
        instance_id: Uuid,
        registry_id: Option<Uuid>,
        source_module_id: Option<Uuid>,
        reason: &str,
    ) -> Result<u64> {
        let affected = match (registry_id, source_module_id) {
            (Some(registry_id), Some(source_module_id)) => sqlx::query::<sqlx::Any>(
                "UPDATE extension_source_certification_jobs
                     SET status = 'cancelled',
                         summary = ?,
                         last_error = ?,
                         finished_at = CURRENT_TIMESTAMP,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE instance_id = ?
                       AND registry_id = ?
                       AND source_module_id = ?
                       AND status IN ('queued', 'running')",
            )
            .bind(reason.trim())
            .bind(reason.trim())
            .bind(instance_id.to_string())
            .bind(registry_id.to_string())
            .bind(source_module_id.to_string())
            .execute(self.pool)
            .await?
            .rows_affected(),
            (Some(registry_id), None) => sqlx::query::<sqlx::Any>(
                "UPDATE extension_source_certification_jobs
                     SET status = 'cancelled',
                         summary = ?,
                         last_error = ?,
                         finished_at = CURRENT_TIMESTAMP,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE instance_id = ?
                       AND registry_id = ?
                       AND status IN ('queued', 'running')",
            )
            .bind(reason.trim())
            .bind(reason.trim())
            .bind(instance_id.to_string())
            .bind(registry_id.to_string())
            .execute(self.pool)
            .await?
            .rows_affected(),
            (None, Some(source_module_id)) => sqlx::query::<sqlx::Any>(
                "UPDATE extension_source_certification_jobs
                     SET status = 'cancelled',
                         summary = ?,
                         last_error = ?,
                         finished_at = CURRENT_TIMESTAMP,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE instance_id = ?
                       AND source_module_id = ?
                       AND status IN ('queued', 'running')",
            )
            .bind(reason.trim())
            .bind(reason.trim())
            .bind(instance_id.to_string())
            .bind(source_module_id.to_string())
            .execute(self.pool)
            .await?
            .rows_affected(),
            (None, None) => sqlx::query::<sqlx::Any>(
                "UPDATE extension_source_certification_jobs
                     SET status = 'cancelled',
                         summary = ?,
                         last_error = ?,
                         finished_at = CURRENT_TIMESTAMP,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE instance_id = ?
                       AND status IN ('queued', 'running')",
            )
            .bind(reason.trim())
            .bind(reason.trim())
            .bind(instance_id.to_string())
            .execute(self.pool)
            .await?
            .rows_affected(),
        };
        Ok(affected)
    }

    pub async fn list_latest_source_certification_jobs(
        &self,
        instance_id: Uuid,
    ) -> Result<Vec<ExtensionSourceCertificationJob>> {
        let rows = sqlx::query(
            "SELECT
                job_id,
                instance_id,
                CAST(registry_id AS TEXT) AS registry_id,
                CAST(source_module_id AS TEXT) AS source_module_id,
                requested_by,
                reason,
                status,
                priority,
                attempts,
                max_attempts,
                CAST(language_eligibility AS TEXT) AS language_eligibility,
                CAST(marketplace_state AS TEXT) AS marketplace_state,
                CAST(summary AS TEXT) AS summary,
                CAST(started_at AS TEXT) AS started_at,
                CAST(finished_at AS TEXT) AS finished_at,
                CAST(last_error AS TEXT) AS last_error,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_certification_jobs
             WHERE instance_id = ?
             ORDER BY updated_at DESC, created_at DESC",
        )
        .bind(instance_id.to_string())
        .fetch_all(self.pool)
        .await?;

        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for row in rows {
            let job = map_source_certification_job(&row)?;
            if let Some(source_module_id) = job.source_module_id {
                if seen.insert(source_module_id) {
                    out.push(job);
                }
            } else {
                out.push(job);
            }
        }
        Ok(out)
    }

    pub async fn list_source_certification_jobs_for_registry(
        &self,
        registry_id: Uuid,
        limit: i64,
    ) -> Result<Vec<ExtensionSourceCertificationJob>> {
        let rows = sqlx::query(
            "SELECT
                job_id,
                instance_id,
                CAST(registry_id AS TEXT) AS registry_id,
                CAST(source_module_id AS TEXT) AS source_module_id,
                requested_by,
                reason,
                status,
                priority,
                attempts,
                max_attempts,
                CAST(language_eligibility AS TEXT) AS language_eligibility,
                CAST(marketplace_state AS TEXT) AS marketplace_state,
                CAST(summary AS TEXT) AS summary,
                CAST(started_at AS TEXT) AS started_at,
                CAST(finished_at AS TEXT) AS finished_at,
                CAST(last_error AS TEXT) AS last_error,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_certification_jobs
             WHERE registry_id = ?
             ORDER BY updated_at DESC, created_at DESC
             LIMIT ?",
        )
        .bind(registry_id.to_string())
        .bind(limit.max(1))
        .fetch_all(self.pool)
        .await?;
        rows.iter().map(map_source_certification_job).collect()
    }

    pub async fn record_source_module_quarantine(
        &self,
        data: &NewExtensionSourceModuleQuarantine,
    ) -> Result<()> {
        validate_source_module_quarantine(data)?;
        let evidence_json = json_to_string(data.evidence_json.as_ref())?;
        let expires_at = data.expires_at.map(db_datetime_string);
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_module_quarantines (
                quarantine_id,
                source_module_id,
                source_module_version_id,
                instance_id,
                failure_class,
                hoster_domain,
                candidate_fingerprint,
                media_type,
                failure_count,
                reason,
                evidence_json,
                expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
             ON CONFLICT(source_module_id, source_module_version_id, failure_class, hoster_domain, candidate_fingerprint, media_type)
             DO UPDATE SET
                failure_count = extension_source_module_quarantines.failure_count + 1,
                reason = excluded.reason,
                evidence_json = excluded.evidence_json,
                last_observed_at = CURRENT_TIMESTAMP,
                expires_at = excluded.expires_at,
                cleared_at = NULL,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.quarantine_id.to_string())
        .bind(data.source_module_id.to_string())
        .bind(data.source_module_version_id.map(|id| id.to_string()))
        .bind(data.instance_id.to_string())
        .bind(data.failure_class.trim())
        .bind(data.hoster_domain.as_deref().map(str::trim))
        .bind(data.candidate_fingerprint.as_deref().map(str::trim))
        .bind(data.media_type.as_deref().map(str::trim))
        .bind(data.reason.as_deref().map(str::trim))
        .bind(evidence_json)
        .bind(expires_at.as_deref())
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_source_replacement_recommendation(
        &self,
        data: &NewExtensionSourceReplacementRecommendation,
    ) -> Result<()> {
        validate_source_replacement_recommendation(data)?;
        let metadata_json = json_to_string(data.metadata_json.as_ref())?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_source_replacement_recommendations (
                recommendation_id,
                source_module_id,
                replacement_source_module_id,
                replacement_registry_id,
                recommendation_key,
                action,
                recommended_version,
                reason,
                metadata_json,
                active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(source_module_id, recommendation_key) DO UPDATE SET
                replacement_source_module_id = excluded.replacement_source_module_id,
                replacement_registry_id = excluded.replacement_registry_id,
                action = excluded.action,
                recommended_version = excluded.recommended_version,
                reason = excluded.reason,
                metadata_json = excluded.metadata_json,
                active = excluded.active,
                applied_at = CASE WHEN excluded.active THEN NULL ELSE applied_at END,
                updated_at = CURRENT_TIMESTAMP",
        )
        .bind(data.recommendation_id.to_string())
        .bind(data.source_module_id.to_string())
        .bind(data.replacement_source_module_id.map(|id| id.to_string()))
        .bind(data.replacement_registry_id.map(|id| id.to_string()))
        .bind(data.recommendation_key.trim())
        .bind(data.action.trim())
        .bind(data.recommended_version.as_deref().map(str::trim))
        .bind(data.reason.as_deref().map(str::trim))
        .bind(metadata_json)
        .bind(data.active)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_source_replacement_recommendations(
        &self,
        source_module_id: Option<Uuid>,
        active_only: bool,
    ) -> Result<Vec<ExtensionSourceReplacementRecommendation>> {
        let base = "SELECT
                recommendation_id,
                source_module_id,
                CAST(replacement_source_module_id AS TEXT) AS replacement_source_module_id,
                CAST(replacement_registry_id AS TEXT) AS replacement_registry_id,
                recommendation_key,
                action,
                CAST(recommended_version AS TEXT) AS recommended_version,
                CAST(reason AS TEXT) AS reason,
                CAST(metadata_json AS TEXT) AS metadata_json,
                CAST(active AS INTEGER) AS active,
                CAST(applied_at AS TEXT) AS applied_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM extension_source_replacement_recommendations";
        let rows = match (source_module_id, active_only) {
            (Some(source_module_id), true) => {
                sqlx::query(&format!(
                    "{base} WHERE source_module_id = ? AND active = 1 ORDER BY created_at DESC"
                ))
                .bind(source_module_id.to_string())
                .fetch_all(self.pool)
                .await?
            }
            (Some(source_module_id), false) => {
                sqlx::query(&format!(
                    "{base} WHERE source_module_id = ? ORDER BY created_at DESC"
                ))
                .bind(source_module_id.to_string())
                .fetch_all(self.pool)
                .await?
            }
            (None, true) => {
                sqlx::query(&format!("{base} WHERE active = 1 ORDER BY created_at DESC"))
                    .fetch_all(self.pool)
                    .await?
            }
            (None, false) => {
                sqlx::query(&format!("{base} ORDER BY created_at DESC"))
                    .fetch_all(self.pool)
                    .await?
            }
        };
        rows.iter()
            .map(map_source_replacement_recommendation)
            .collect()
    }

    pub async fn mark_source_replacement_recommendation_applied(
        &self,
        recommendation_id: Uuid,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_source_replacement_recommendations
             SET active = 0,
                 applied_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE recommendation_id = ?",
        )
        .bind(recommendation_id.to_string())
        .execute(self.pool)
        .await?;
        Ok(())
    }
}

fn json_to_string(value: Option<&serde_json::Value>) -> Result<Option<String>> {
    match value {
        Some(value) => Ok(Some(
            serde_json::to_string(value).context("serializing json")?,
        )),
        None => Ok(None),
    }
}

const SOURCE_REGISTRY_TYPES: &[&str] = &[
    "elixir_curated_cloudstream_pack",
    "cloudstream_repo_json",
    "cloudstream_plugins_json",
    "elixir_curated_nuvio_pack",
    "nuvio_manifest_json",
    "stremio_manifest_json",
];
const SOURCE_REGISTRY_TRUST_CLASSES: &[&str] = &["curated", "maintainer_known", "custom"];
const SOURCE_REGISTRY_FETCH_STATUSES: &[&str] = &["unknown", "success", "failed", "skipped"];
const SOURCE_MODULE_ECOSYSTEMS: &[&str] = &["cloudstream", "aniyomi", "nuvio", "stremio"];
const SOURCE_MODULE_HEALTH_STATES: &[&str] = &[
    "unknown",
    "available",
    "healthy",
    "degraded",
    "broken",
    "unsupported",
    "account_required",
    "disabled",
];
const SOURCE_MODULE_VERSION_STATES: &[&str] = &[
    "available",
    "staged",
    "installed",
    "active",
    "failed",
    "rolled_back",
];
const SOURCE_MODULE_SMOKE_STATUSES: &[&str] = &["unknown", "passed", "failed", "skipped"];
const SOURCE_HEALTH_SEVERITIES: &[&str] = &["info", "warning", "error"];
const SOURCE_MODULE_CERTIFICATION_STATUSES: &[&str] = &[
    "certified",
    "degraded",
    "unsupported",
    "broken",
    "account_required",
    "network_blocked",
    "unknown",
    "probation",
];
const SOURCE_CERTIFICATION_JOB_STATUSES: &[&str] = &[
    "queued",
    "running",
    "succeeded",
    "degraded",
    "blocked",
    "failed",
    "cancelled",
    "skipped",
];
const SOURCE_REPLACEMENT_ACTIONS: &[&str] = &["replace", "disable", "pin", "none"];

fn validate_source_registry(data: &NewExtensionSourceRegistry) -> Result<()> {
    ensure_store_non_empty(
        &data.registry_key,
        "extension_source_registries.registry_key",
    )?;
    ensure_store_non_empty(
        &data.display_name,
        "extension_source_registries.display_name",
    )?;
    validate_allowed_value(
        "extension_source_registries.registry_type",
        &data.registry_type,
        SOURCE_REGISTRY_TYPES,
    )?;
    validate_allowed_value(
        "extension_source_registries.trust_class",
        &data.trust_class,
        SOURCE_REGISTRY_TRUST_CLASSES,
    )?;
    Ok(())
}

fn validate_source_module(data: &NewExtensionSourceModule) -> Result<()> {
    ensure_store_non_empty(&data.module_key, "extension_source_modules.module_key")?;
    ensure_store_non_empty(&data.display_name, "extension_source_modules.display_name")?;
    validate_allowed_value(
        "extension_source_modules.ecosystem",
        &data.ecosystem,
        SOURCE_MODULE_ECOSYSTEMS,
    )?;
    validate_allowed_value(
        "extension_source_modules.health_state",
        &data.health_state,
        SOURCE_MODULE_HEALTH_STATES,
    )?;
    if data.unsupported
        && data
            .unsupported_reason
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
    {
        anyhow::bail!("unsupported source modules must include unsupported_reason");
    }
    Ok(())
}

fn validate_source_module_version(data: &NewExtensionSourceModuleVersion) -> Result<()> {
    ensure_store_non_empty(&data.version, "extension_source_module_versions.version")?;
    validate_allowed_value(
        "extension_source_module_versions.install_state",
        &data.install_state,
        SOURCE_MODULE_VERSION_STATES,
    )?;
    validate_allowed_value(
        "extension_source_module_versions.smoke_status",
        &data.smoke_status,
        SOURCE_MODULE_SMOKE_STATUSES,
    )?;
    Ok(())
}

fn validate_source_health_event(data: &NewExtensionSourceHealthEvent) -> Result<()> {
    ensure_store_non_empty(
        &data.event_type,
        "extension_source_health_events.event_type",
    )?;
    validate_allowed_value(
        "extension_source_health_events.state",
        &data.state,
        SOURCE_MODULE_HEALTH_STATES,
    )?;
    validate_allowed_value(
        "extension_source_health_events.severity",
        &data.severity,
        SOURCE_HEALTH_SEVERITIES,
    )?;
    Ok(())
}

fn validate_source_module_certification(
    data: &NewExtensionSourceModuleCertification,
) -> Result<()> {
    ensure_store_non_empty(
        &data.adapter,
        "extension_source_module_certifications.adapter",
    )?;
    validate_allowed_value(
        "extension_source_module_certifications.status",
        &data.status,
        SOURCE_MODULE_CERTIFICATION_STATUSES,
    )?;
    ensure_store_non_empty(
        &data.policy_version,
        "extension_source_module_certifications.policy_version",
    )?;
    Ok(())
}

fn validate_source_certification_job(data: &NewExtensionSourceCertificationJob) -> Result<()> {
    ensure_store_non_empty(
        &data.requested_by,
        "extension_source_certification_jobs.requested_by",
    )?;
    ensure_store_non_empty(&data.reason, "extension_source_certification_jobs.reason")?;
    validate_allowed_value(
        "extension_source_certification_jobs.status",
        &data.status,
        SOURCE_CERTIFICATION_JOB_STATUSES,
    )?;
    if data.max_attempts < 1 {
        anyhow::bail!("extension_source_certification_jobs.max_attempts must be positive");
    }
    if data.attempts < 0 {
        anyhow::bail!("extension_source_certification_jobs.attempts must not be negative");
    }
    Ok(())
}

fn validate_source_module_quarantine(data: &NewExtensionSourceModuleQuarantine) -> Result<()> {
    ensure_store_non_empty(
        &data.failure_class,
        "extension_source_module_quarantines.failure_class",
    )?;
    Ok(())
}

fn validate_source_replacement_recommendation(
    data: &NewExtensionSourceReplacementRecommendation,
) -> Result<()> {
    ensure_store_non_empty(
        &data.recommendation_key,
        "extension_source_replacement_recommendations.recommendation_key",
    )?;
    validate_allowed_value(
        "extension_source_replacement_recommendations.action",
        &data.action,
        SOURCE_REPLACEMENT_ACTIONS,
    )?;
    if data.action == "replace" && data.replacement_source_module_id.is_none() {
        anyhow::bail!("replace recommendations must include replacement_source_module_id");
    }
    Ok(())
}

fn ensure_store_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("{field} must not be empty");
    }
    Ok(())
}

fn validate_allowed_value(field: &str, value: &str, allowed: &[&str]) -> Result<()> {
    let trimmed = value.trim();
    if allowed.contains(&trimmed) {
        return Ok(());
    }
    anyhow::bail!(
        "{} must be one of [{}], got '{}'",
        field,
        allowed.join(", "),
        value
    )
}

fn media_ownership_identity_metadata(
    media_type: crate::db::models::MediaType,
    title: &str,
    year: Option<i32>,
    external_ids: Option<&ExternalIds>,
) -> serde_json::Value {
    json!({
        "mediaType": media_type.as_str(),
        "title": title,
        "year": year,
        "externalIds": external_ids,
    })
}

fn map_source_registry(row: &AnyRow) -> Result<ExtensionSourceRegistry> {
    let registry_id_raw: String = row.try_get("registry_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let last_fetched_at_raw = row_get_opt_string(row, "last_fetched_at")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    Ok(ExtensionSourceRegistry {
        registry_id: parse_uuid(&registry_id_raw, "extension_source_registries.registry_id")?,
        instance_id: parse_uuid(&instance_id_raw, "extension_source_registries.instance_id")?,
        registry_key: row.try_get("registry_key")?,
        registry_type: row.try_get("registry_type")?,
        trust_class: row.try_get("trust_class")?,
        display_name: row.try_get("display_name")?,
        url: row_get_opt_string(row, "url")?,
        enabled: row_get_bool(row, "enabled")?,
        auto_refresh: row_get_bool(row, "auto_refresh")?,
        trusted_for_executable_updates: row_get_bool(row, "trusted_for_executable_updates")?,
        etag: row_get_opt_string(row, "etag")?,
        last_modified: row_get_opt_string(row, "last_modified")?,
        last_fetch_status: row.try_get("last_fetch_status")?,
        last_fetch_error: row_get_opt_string(row, "last_fetch_error")?,
        last_fetched_at: parse_datetime_opt(
            last_fetched_at_raw,
            "extension_source_registries.last_fetched_at",
        )?,
        metadata_json: parse_json_opt(
            row_get_opt_string(row, "metadata_json")?,
            "extension_source_registries.metadata_json",
        )?,
        created_at: parse_datetime(&created_at_raw, "extension_source_registries.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "extension_source_registries.updated_at")?,
    })
}

fn map_source_module(row: &AnyRow) -> Result<ExtensionSourceModule> {
    let source_module_id_raw: String = row.try_get("source_module_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let registry_id_raw: String = row.try_get("registry_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    Ok(ExtensionSourceModule {
        source_module_id: parse_uuid(
            &source_module_id_raw,
            "extension_source_modules.source_module_id",
        )?,
        instance_id: parse_uuid(&instance_id_raw, "extension_source_modules.instance_id")?,
        registry_id: parse_uuid(&registry_id_raw, "extension_source_modules.registry_id")?,
        module_key: row.try_get("module_key")?,
        display_name: row.try_get("display_name")?,
        ecosystem: row.try_get("ecosystem")?,
        plugin_package: row_get_opt_string(row, "plugin_package")?,
        active_version: row_get_opt_string(row, "active_version")?,
        rollback_version: row_get_opt_string(row, "rollback_version")?,
        media_types_json: parse_json_opt(
            row_get_opt_string(row, "media_types_json")?,
            "extension_source_modules.media_types_json",
        )?,
        language_tags_json: parse_json_opt(
            row_get_opt_string(row, "language_tags_json")?,
            "extension_source_modules.language_tags_json",
        )?,
        region_tags_json: parse_json_opt(
            row_get_opt_string(row, "region_tags_json")?,
            "extension_source_modules.region_tags_json",
        )?,
        source_domains_json: parse_json_opt(
            row_get_opt_string(row, "source_domains_json")?,
            "extension_source_modules.source_domains_json",
        )?,
        account_required: row_get_bool(row, "account_required")?,
        unsupported: row_get_bool(row, "unsupported")?,
        unsupported_reason: row_get_opt_string(row, "unsupported_reason")?,
        enabled: row_get_bool(row, "enabled")?,
        installed: row_get_bool(row, "installed")?,
        pinned_version: row_get_opt_string(row, "pinned_version")?,
        health_state: row.try_get("health_state")?,
        replacement_recommendation_key: row_get_opt_string(row, "replacement_recommendation_key")?,
        last_success_at: parse_datetime_opt(
            row_get_opt_string(row, "last_success_at")?,
            "extension_source_modules.last_success_at",
        )?,
        last_failure_at: parse_datetime_opt(
            row_get_opt_string(row, "last_failure_at")?,
            "extension_source_modules.last_failure_at",
        )?,
        last_error: row_get_opt_string(row, "last_error")?,
        metadata_json: parse_json_opt(
            row_get_opt_string(row, "metadata_json")?,
            "extension_source_modules.metadata_json",
        )?,
        created_at: parse_datetime(&created_at_raw, "extension_source_modules.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "extension_source_modules.updated_at")?,
    })
}

fn map_source_module_version(row: &AnyRow) -> Result<ExtensionSourceModuleVersion> {
    let version_id_raw: String = row.try_get("version_id")?;
    let source_module_id_raw: String = row.try_get("source_module_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    Ok(ExtensionSourceModuleVersion {
        version_id: parse_uuid(
            &version_id_raw,
            "extension_source_module_versions.version_id",
        )?,
        source_module_id: parse_uuid(
            &source_module_id_raw,
            "extension_source_module_versions.source_module_id",
        )?,
        version: row.try_get("version")?,
        artifact_url: row_get_opt_string(row, "artifact_url")?,
        artifact_sha256: row_get_opt_string(row, "artifact_sha256")?,
        signature: row_get_opt_string(row, "signature")?,
        install_state: row.try_get("install_state")?,
        smoke_status: row.try_get("smoke_status")?,
        smoke_error: row_get_opt_string(row, "smoke_error")?,
        rollback_of_version_id: parse_uuid_opt(
            row_get_opt_string(row, "rollback_of_version_id")?,
            "extension_source_module_versions.rollback_of_version_id",
        )?,
        installed_at: parse_datetime_opt(
            row_get_opt_string(row, "installed_at")?,
            "extension_source_module_versions.installed_at",
        )?,
        activated_at: parse_datetime_opt(
            row_get_opt_string(row, "activated_at")?,
            "extension_source_module_versions.activated_at",
        )?,
        metadata_json: parse_json_opt(
            row_get_opt_string(row, "metadata_json")?,
            "extension_source_module_versions.metadata_json",
        )?,
        created_at: parse_datetime(
            &created_at_raw,
            "extension_source_module_versions.created_at",
        )?,
        updated_at: parse_datetime(
            &updated_at_raw,
            "extension_source_module_versions.updated_at",
        )?,
    })
}

fn map_source_health_event(row: &AnyRow) -> Result<ExtensionSourceHealthEvent> {
    let health_event_id_raw: String = row.try_get("health_event_id")?;
    let source_module_id_raw: String = row.try_get("source_module_id")?;
    let observed_at_raw: String = row.try_get("observed_at")?;
    let created_at_raw: String = row.try_get("created_at")?;
    Ok(ExtensionSourceHealthEvent {
        health_event_id: parse_uuid(
            &health_event_id_raw,
            "extension_source_health_events.health_event_id",
        )?,
        source_module_id: parse_uuid(
            &source_module_id_raw,
            "extension_source_health_events.source_module_id",
        )?,
        event_type: row.try_get("event_type")?,
        state: row.try_get("state")?,
        severity: row.try_get("severity")?,
        reason: row_get_opt_string(row, "reason")?,
        evidence_json: parse_json_opt(
            row_get_opt_string(row, "evidence_json")?,
            "extension_source_health_events.evidence_json",
        )?,
        observed_at: parse_datetime(
            &observed_at_raw,
            "extension_source_health_events.observed_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "extension_source_health_events.created_at")?,
    })
}

fn map_source_module_certification(row: &AnyRow) -> Result<ExtensionSourceModuleCertification> {
    let certification_id_raw: String = row.try_get("certification_id")?;
    let source_module_id_raw: String = row.try_get("source_module_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    let media_type_results_raw: String = row.try_get("media_type_results_json")?;
    let materialization_results_raw: String = row.try_get("materialization_results_json")?;
    let probe_targets_raw: String = row.try_get("probe_targets_json")?;
    let candidate_evidence_raw: String = row.try_get("candidate_evidence_json")?;
    Ok(ExtensionSourceModuleCertification {
        certification_id: parse_uuid(
            &certification_id_raw,
            "extension_source_module_certifications.certification_id",
        )?,
        source_module_id: parse_uuid(
            &source_module_id_raw,
            "extension_source_module_certifications.source_module_id",
        )?,
        source_module_version_id: parse_uuid_opt(
            row_get_opt_string(row, "source_module_version_id")?,
            "extension_source_module_certifications.source_module_version_id",
        )?,
        artifact_sha256: row_get_opt_string(row, "artifact_sha256")?,
        instance_id: parse_uuid(
            &instance_id_raw,
            "extension_source_module_certifications.instance_id",
        )?,
        adapter: row.try_get("adapter")?,
        status: row.try_get("status")?,
        failure_class: row_get_opt_string(row, "failure_class")?,
        summary: row_get_opt_string(row, "summary")?,
        media_type_results_json: parse_json(
            &media_type_results_raw,
            "extension_source_module_certifications.media_type_results_json",
        )?,
        materialization_results_json: parse_json(
            &materialization_results_raw,
            "extension_source_module_certifications.materialization_results_json",
        )?,
        probe_targets_json: parse_json(
            &probe_targets_raw,
            "extension_source_module_certifications.probe_targets_json",
        )?,
        candidate_evidence_json: parse_json(
            &candidate_evidence_raw,
            "extension_source_module_certifications.candidate_evidence_json",
        )?,
        runtime_version: row_get_opt_string(row, "runtime_version")?,
        policy_version: row.try_get("policy_version")?,
        certified_at: parse_datetime_opt(
            row_get_opt_string(row, "certified_at")?,
            "extension_source_module_certifications.certified_at",
        )?,
        expires_at: parse_datetime_opt(
            row_get_opt_string(row, "expires_at")?,
            "extension_source_module_certifications.expires_at",
        )?,
        created_at: parse_datetime(
            &created_at_raw,
            "extension_source_module_certifications.created_at",
        )?,
        updated_at: parse_datetime(
            &updated_at_raw,
            "extension_source_module_certifications.updated_at",
        )?,
    })
}

fn map_source_certification_job(row: &AnyRow) -> Result<ExtensionSourceCertificationJob> {
    let job_id_raw: String = row.try_get("job_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    Ok(ExtensionSourceCertificationJob {
        job_id: parse_uuid(&job_id_raw, "extension_source_certification_jobs.job_id")?,
        instance_id: parse_uuid(
            &instance_id_raw,
            "extension_source_certification_jobs.instance_id",
        )?,
        registry_id: parse_uuid_opt(
            row_get_opt_string(row, "registry_id")?,
            "extension_source_certification_jobs.registry_id",
        )?,
        source_module_id: parse_uuid_opt(
            row_get_opt_string(row, "source_module_id")?,
            "extension_source_certification_jobs.source_module_id",
        )?,
        requested_by: row.try_get("requested_by")?,
        reason: row.try_get("reason")?,
        status: row.try_get("status")?,
        priority: row.try_get("priority")?,
        attempts: row.try_get("attempts")?,
        max_attempts: row.try_get("max_attempts")?,
        language_eligibility: row_get_opt_string(row, "language_eligibility")?,
        marketplace_state: row_get_opt_string(row, "marketplace_state")?,
        summary: row_get_opt_string(row, "summary")?,
        started_at: parse_datetime_opt(
            row_get_opt_string(row, "started_at")?,
            "extension_source_certification_jobs.started_at",
        )?,
        finished_at: parse_datetime_opt(
            row_get_opt_string(row, "finished_at")?,
            "extension_source_certification_jobs.finished_at",
        )?,
        last_error: row_get_opt_string(row, "last_error")?,
        created_at: parse_datetime(
            &created_at_raw,
            "extension_source_certification_jobs.created_at",
        )?,
        updated_at: parse_datetime(
            &updated_at_raw,
            "extension_source_certification_jobs.updated_at",
        )?,
    })
}

fn map_source_replacement_recommendation(
    row: &AnyRow,
) -> Result<ExtensionSourceReplacementRecommendation> {
    let recommendation_id_raw: String = row.try_get("recommendation_id")?;
    let source_module_id_raw: String = row.try_get("source_module_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    Ok(ExtensionSourceReplacementRecommendation {
        recommendation_id: parse_uuid(
            &recommendation_id_raw,
            "extension_source_replacement_recommendations.recommendation_id",
        )?,
        source_module_id: parse_uuid(
            &source_module_id_raw,
            "extension_source_replacement_recommendations.source_module_id",
        )?,
        replacement_source_module_id: parse_uuid_opt(
            row_get_opt_string(row, "replacement_source_module_id")?,
            "extension_source_replacement_recommendations.replacement_source_module_id",
        )?,
        replacement_registry_id: parse_uuid_opt(
            row_get_opt_string(row, "replacement_registry_id")?,
            "extension_source_replacement_recommendations.replacement_registry_id",
        )?,
        recommendation_key: row.try_get("recommendation_key")?,
        action: row.try_get("action")?,
        recommended_version: row_get_opt_string(row, "recommended_version")?,
        reason: row_get_opt_string(row, "reason")?,
        metadata_json: parse_json_opt(
            row_get_opt_string(row, "metadata_json")?,
            "extension_source_replacement_recommendations.metadata_json",
        )?,
        active: row_get_bool(row, "active")?,
        applied_at: parse_datetime_opt(
            row_get_opt_string(row, "applied_at")?,
            "extension_source_replacement_recommendations.applied_at",
        )?,
        created_at: parse_datetime(
            &created_at_raw,
            "extension_source_replacement_recommendations.created_at",
        )?,
        updated_at: parse_datetime(
            &updated_at_raw,
            "extension_source_replacement_recommendations.updated_at",
        )?,
    })
}

fn map_extension(row: &AnyRow) -> Result<Extension> {
    let extension_id: String = row.try_get("extension_id")?;
    let name: String = row.try_get("name")?;
    let version: String = row.try_get("version")?;
    let kind_raw: String = row.try_get("kind")?;
    let trust_raw: String = row.try_get("trust_level")?;
    let manifest_raw: String = row.try_get("manifest_json")?;
    let installed_at_raw: String = row.try_get("installed_at")?;

    Ok(Extension {
        extension_id,
        name,
        version,
        kind: parse_enum(&kind_raw, "extensions.kind")?,
        publisher_name: row_get_opt_string(row, "publisher_name")?,
        signing_key_id: row_get_opt_string(row, "signing_key_id")?,
        trust_level: parse_enum(&trust_raw, "extensions.trust_level")?,
        manifest_json: parse_json(&manifest_raw, "extensions.manifest_json")?,
        package_hash: row_get_opt_string(row, "package_hash")?,
        installed_at: parse_datetime(&installed_at_raw, "extensions.installed_at")?,
        enabled: row_get_bool(row, "enabled")?,
    })
}

fn map_extension_setting_record(row: &AnyRow) -> Result<ExtensionSettingRecord> {
    let setting_key: String = row.try_get("setting_key")?;
    let value_json = parse_json_opt(
        row_get_opt_string(row, "value_json")?,
        "extension_settings.value_json",
    )?
    .ok_or_else(|| anyhow::anyhow!("extension_settings.value_json was null"))?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    Ok(ExtensionSettingRecord {
        setting_key,
        value_json,
        updated_at: parse_datetime(&updated_at_raw, "extension_settings.updated_at")?,
    })
}

fn map_extension_instance(row: &AnyRow) -> Result<ExtensionInstance> {
    let instance_id_raw: String = row.try_get("instance_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(ExtensionInstance {
        instance_id: parse_uuid(&instance_id_raw, "extension_instances.instance_id")?,
        extension_id: row.try_get("extension_id")?,
        instance_name: row.try_get("instance_name")?,
        config_json: parse_json_opt(
            row_get_opt_string(row, "config_json")?,
            "extension_instances.config_json",
        )?,
        runtime_version: row_get_opt_string(row, "runtime_version")?,
        rollback_version: row_get_opt_string(row, "rollback_version")?,
        created_at: parse_datetime(&created_at_raw, "extension_instances.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "extension_instances.updated_at")?,
        enabled: row_get_bool(row, "enabled")?,
    })
}

fn map_provider(row: &AnyRow) -> Result<Provider> {
    let provider_id_raw: String = row.try_get("provider_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let cardinality_raw: String = row.try_get("cardinality")?;
    let health_raw: String = row.try_get("health_state")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(Provider {
        provider_id: parse_uuid(&provider_id_raw, "providers.provider_id")?,
        instance_id: parse_uuid(&instance_id_raw, "providers.instance_id")?,
        capability: row.try_get("capability")?,
        slot_id: row.try_get("slot_id")?,
        cardinality: parse_enum(&cardinality_raw, "providers.cardinality")?,
        implementation: row_get_opt_string(row, "implementation")?,
        scope_json: parse_json_opt(
            row_get_opt_string(row, "scope_json")?,
            "providers.scope_json",
        )?,
        endpoint_json: parse_json_opt(
            row_get_opt_string(row, "endpoint_json")?,
            "providers.endpoint_json",
        )?,
        health_state: parse_enum(&health_raw, "providers.health_state")?,
        last_healthcheck_at: parse_datetime_opt(
            row_get_opt_string(row, "last_healthcheck_at")?,
            "providers.last_healthcheck_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "providers.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "providers.updated_at")?,
    })
}

fn map_provider_detail(row: &AnyRow) -> Result<ProviderDetails> {
    let provider = map_provider(row)?;
    let extension_id: String = row.try_get("extension_id")?;
    let trust_raw: String = row.try_get("trust_level")?;
    Ok(ProviderDetails {
        provider,
        extension_id,
        trust_level: parse_enum(&trust_raw, "extensions.trust_level")?,
    })
}

fn map_provider_readiness(row: &AnyRow) -> Result<ProviderReadiness> {
    let provider_id_raw: String = row.try_get("provider_id")?;
    let readiness_phase_raw: String = row.try_get("readiness_phase")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(ProviderReadiness {
        provider_id: parse_uuid(&provider_id_raw, "provider_readiness.provider_id")?,
        readiness_phase: parse_enum(&readiness_phase_raw, "provider_readiness.readiness_phase")?,
        readiness_detail: row_get_opt_string(row, "readiness_detail")?,
        last_checked_at: parse_datetime_opt(
            row_get_opt_string(row, "last_checked_at")?,
            "provider_readiness.last_checked_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "provider_readiness.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "provider_readiness.updated_at")?,
    })
}

fn map_binding(row: &AnyRow) -> Result<Binding> {
    let binding_id_raw: String = row.try_get("binding_id")?;
    let consumer_id_raw: String = row.try_get("consumer_provider_id")?;
    let target_id_raw: String = row.try_get("target_provider_id")?;
    let status_raw: String = row.try_get("status")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(Binding {
        binding_id: parse_uuid(&binding_id_raw, "bindings.binding_id")?,
        consumer_provider_id: parse_uuid(&consumer_id_raw, "bindings.consumer_provider_id")?,
        requires_capability: row.try_get("requires_capability")?,
        requires_slot_id: row.try_get("requires_slot_id")?,
        target_provider_id: parse_uuid(&target_id_raw, "bindings.target_provider_id")?,
        binding_params_json: parse_json_opt(
            row_get_opt_string(row, "binding_params_json")?,
            "bindings.binding_params_json",
        )?,
        status: parse_enum(&status_raw, "bindings.status")?,
        last_error: row_get_opt_string(row, "last_error")?,
        last_applied_at: parse_datetime_opt(
            row_get_opt_string(row, "last_applied_at")?,
            "bindings.last_applied_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "bindings.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "bindings.updated_at")?,
    })
}

fn map_desired_blueprint(row: &AnyRow) -> Result<DesiredBlueprint> {
    let desired_id_raw: String = row.try_get("desired_id")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(DesiredBlueprint {
        desired_id: parse_uuid(&desired_id_raw, "desired_blueprints.desired_id")?,
        blueprint_extension_id: row.try_get("blueprint_extension_id")?,
        blueprint_version: row.try_get("blueprint_version")?,
        params_json: parse_json_opt(
            row_get_opt_string(row, "params_json")?,
            "desired_blueprints.params_json",
        )?,
        applied: row_get_bool(row, "applied")?,
        created_at: parse_datetime(&created_at_raw, "desired_blueprints.created_at")?,
        applied_at: parse_datetime_opt(
            row_get_opt_string(row, "applied_at")?,
            "desired_blueprints.applied_at",
        )?,
    })
}

fn map_secret(row: &AnyRow) -> Result<Secret> {
    let secret_id_raw: String = row.try_get("secret_id")?;
    let scope_raw: String = row.try_get("scope")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(Secret {
        secret_id: parse_uuid(&secret_id_raw, "secrets.secret_id")?,
        scope: parse_enum(&scope_raw, "secrets.scope")?,
        scope_id: parse_uuid_opt(row_get_opt_string(row, "scope_id")?, "secrets.scope_id")?,
        key: row.try_get("key")?,
        value_encrypted: row.try_get("value_encrypted")?,
        created_at: parse_datetime(&created_at_raw, "secrets.created_at")?,
        rotatable: row_get_bool(row, "rotatable")?,
    })
}

fn map_managed_ingest_intent(row: &AnyRow) -> Result<ManagedIngestIntent> {
    let intent_id_raw: String = row.try_get("intent_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let manager_provider_id_raw: String = row.try_get("manager_provider_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    let external_ids = parse_json_opt(
        row_get_opt_string(row, "external_ids_json")?,
        "managed_ingest_intents.external_ids_json",
    )?
    .map(serde_json::from_value::<ExternalIds>)
    .transpose()
    .context("parsing managed ingest external ids")?;

    let year: Option<i64> = row.try_get("year")?;
    let year = year.map(|value| value as i32);

    let media_type = match media_type_raw.trim().to_ascii_lowercase().as_str() {
        "movie" => crate::db::models::MediaType::Movie,
        "series" => crate::db::models::MediaType::Series,
        "anime" => crate::db::models::MediaType::Anime,
        _ => {
            anyhow::bail!(
                "invalid enum value '{}' for field managed_ingest_intents.media_type",
                media_type_raw
            );
        }
    };

    Ok(ManagedIngestIntent {
        intent_id: parse_uuid(&intent_id_raw, "managed_ingest_intents.intent_id")?,
        media_type,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        year,
        external_ids,
        manager_provider_id: parse_uuid(
            &manager_provider_id_raw,
            "managed_ingest_intents.manager_provider_id",
        )?,
        manager_item_id: row_get_opt_string(row, "manager_item_id")?,
        manager_label: row_get_opt_string(row, "manager_label")?,
        source: row.try_get("source")?,
        active: row_get_bool(row, "active")?,
        last_matched_at: parse_datetime_opt(
            row_get_opt_string(row, "last_matched_at")?,
            "managed_ingest_intents.last_matched_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "managed_ingest_intents.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "managed_ingest_intents.updated_at")?,
    })
}

fn map_managed_import_event(row: &AnyRow) -> Result<ManagedImportEvent> {
    let event_id_raw: String = row.try_get("event_id")?;
    let intent_id_raw: String = row.try_get("intent_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let manager_provider_id_raw: String = row.try_get("manager_provider_id")?;
    let linked_media_item_id_raw: Option<String> = row_get_opt_string(row, "linked_media_item_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    let external_ids = parse_json_opt(
        row_get_opt_string(row, "external_ids_json")?,
        "managed_import_events.external_ids_json",
    )?
    .map(serde_json::from_value::<ExternalIds>)
    .transpose()
    .context("parsing managed import event external ids")?;
    let imported_files = parse_json(
        &row.try_get::<String, _>("imported_files_json")?,
        "managed_import_events.imported_files_json",
    )
    .and_then(|value| {
        serde_json::from_value::<Vec<ManagedImportFile>>(value)
            .context("parsing managed import event files")
    })?;
    let raw_manager_payload = parse_json_opt(
        row_get_opt_string(row, "raw_manager_payload_json")?,
        "managed_import_events.raw_manager_payload_json",
    )?;

    Ok(ManagedImportEvent {
        event_id: parse_uuid(&event_id_raw, "managed_import_events.event_id")?,
        event_key: row.try_get("event_key")?,
        intent_id: parse_uuid(&intent_id_raw, "managed_import_events.intent_id")?,
        media_type: parse_media_type(&media_type_raw, "managed_import_events.media_type")?,
        external_ids,
        manager_provider_id: parse_uuid(
            &manager_provider_id_raw,
            "managed_import_events.manager_provider_id",
        )?,
        manager_item_id: row_get_opt_string(row, "manager_item_id")?,
        manager_label: row_get_opt_string(row, "manager_label")?,
        manager_implementation: row_get_opt_string(row, "manager_implementation")?,
        imported_files,
        raw_manager_payload,
        status: row.try_get("status")?,
        linked_media_item_id: linked_media_item_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "managed_import_events.linked_media_item_id"))
            .transpose()?,
        last_error: row_get_opt_string(row, "last_error")?,
        imported_at: parse_datetime_opt(
            row_get_opt_string(row, "imported_at")?,
            "managed_import_events.imported_at",
        )?,
        processed_at: parse_datetime_opt(
            row_get_opt_string(row, "processed_at")?,
            "managed_import_events.processed_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "managed_import_events.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "managed_import_events.updated_at")?,
    })
}

fn map_managed_library_provenance(row: &AnyRow) -> Result<ManagedLibraryProvenance> {
    let media_item_id_raw: String = row.try_get("media_item_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let external_ids_json = parse_json_opt(
        row_get_opt_string(row, "external_ids_json")?,
        "managed_library_provenance.external_ids_json",
    )?;
    let external_ids_json = external_ids_json
        .map(serde_json::from_value::<ExternalIds>)
        .transpose()
        .context("parsing managed library provenance external ids")?;
    let manager_provider_id_raw: String = row.try_get("manager_provider_id")?;
    let intent_id_raw: Option<String> = row.try_get("intent_id").ok().flatten();
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(ManagedLibraryProvenance {
        media_item_id: parse_uuid(
            &media_item_id_raw,
            "managed_library_provenance.media_item_id",
        )?,
        media_type: parse_media_type(&media_type_raw, "managed_library_provenance.media_type")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        year: row.try_get::<i64, _>("year").ok().map(|value| value as i32),
        external_ids: external_ids_json,
        manager_provider_id: parse_uuid(
            &manager_provider_id_raw,
            "managed_library_provenance.manager_provider_id",
        )?,
        manager_item_id: row.try_get("manager_item_id").ok().flatten(),
        manager_label: row.try_get("manager_label").ok().flatten(),
        manager_implementation: row.try_get("manager_implementation").ok().flatten(),
        intent_id: intent_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "managed_library_provenance.intent_id"))
            .transpose()?,
        created_at: parse_datetime(&created_at_raw, "managed_library_provenance.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "managed_library_provenance.updated_at")?,
    })
}

fn map_media_ownership(row: &AnyRow) -> Result<MediaOwnership> {
    let ownership_id_raw: String = row.try_get("ownership_id")?;
    let media_item_id_raw: String = row.try_get("media_item_id")?;
    let owner_provider_id_raw = row_get_opt_string(row, "owner_provider_id")?;
    let owner_instance_id_raw = row_get_opt_string(row, "owner_instance_id")?;
    let acquisition_subscription_id_raw = row_get_opt_string(row, "acquisition_subscription_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(MediaOwnership {
        ownership_id: parse_uuid(&ownership_id_raw, "media_ownerships.ownership_id")?,
        media_item_id: parse_uuid(&media_item_id_raw, "media_ownerships.media_item_id")?,
        owner_type: row.try_get("owner_type")?,
        owner_role: row.try_get("owner_role")?,
        owner_label: row_get_opt_string(row, "owner_label")?,
        owner_implementation: row_get_opt_string(row, "owner_implementation")?,
        owner_provider_id: owner_provider_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "media_ownerships.owner_provider_id"))
            .transpose()?,
        owner_instance_id: owner_instance_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "media_ownerships.owner_instance_id"))
            .transpose()?,
        owner_extension_id: row_get_opt_string(row, "owner_extension_id")?,
        owner_external_id: row_get_opt_string(row, "owner_external_id")?,
        acquisition_subscription_id: acquisition_subscription_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "media_ownerships.acquisition_subscription_id"))
            .transpose()?,
        acquisition_target_scope: parse_json_opt(
            row_get_opt_string(row, "acquisition_target_scope_json")?,
            "media_ownerships.acquisition_target_scope_json",
        )?,
        release_capability: row.try_get("release_capability")?,
        release_policy: row.try_get("release_policy")?,
        metadata: parse_json_opt(
            row_get_opt_string(row, "metadata_json")?,
            "media_ownerships.metadata_json",
        )?,
        active: row_get_bool(row, "active")?,
        created_at: parse_datetime(&created_at_raw, "media_ownerships.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "media_ownerships.updated_at")?,
    })
}

fn map_media_owner_release_event(row: &AnyRow) -> Result<MediaOwnerReleaseEvent> {
    let release_event_id_raw: String = row.try_get("release_event_id")?;
    let media_item_id_raw = row_get_opt_string(row, "media_item_id")?;
    let ownership_id_raw = row_get_opt_string(row, "ownership_id")?;
    let owner_provider_id_raw = row_get_opt_string(row, "owner_provider_id")?;
    let acquisition_subscription_id_raw = row_get_opt_string(row, "acquisition_subscription_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(MediaOwnerReleaseEvent {
        release_event_id: parse_uuid(
            &release_event_id_raw,
            "media_owner_release_events.release_event_id",
        )?,
        media_item_id: media_item_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "media_owner_release_events.media_item_id"))
            .transpose()?,
        ownership_id: ownership_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "media_owner_release_events.ownership_id"))
            .transpose()?,
        requested_action: row.try_get("requested_action")?,
        owner_type: row.try_get("owner_type")?,
        owner_label: row_get_opt_string(row, "owner_label")?,
        owner_provider_id: owner_provider_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "media_owner_release_events.owner_provider_id"))
            .transpose()?,
        acquisition_subscription_id: acquisition_subscription_id_raw
            .as_deref()
            .map(|value| {
                parse_uuid(
                    value,
                    "media_owner_release_events.acquisition_subscription_id",
                )
            })
            .transpose()?,
        status: row.try_get("status")?,
        status_reason: row_get_opt_string(row, "status_reason")?,
        request: parse_json_opt(
            row_get_opt_string(row, "request_json")?,
            "media_owner_release_events.request_json",
        )?,
        response: parse_json_opt(
            row_get_opt_string(row, "response_json")?,
            "media_owner_release_events.response_json",
        )?,
        created_at: parse_datetime(&created_at_raw, "media_owner_release_events.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "media_owner_release_events.updated_at")?,
    })
}

fn map_managed_media_tombstone(row: &AnyRow) -> Result<ManagedMediaTombstone> {
    let tombstone_id_raw: String = row.try_get("tombstone_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let external_ids_json = parse_json_opt(
        row_get_opt_string(row, "external_ids_json")?,
        "managed_media_tombstones.external_ids_json",
    )?;
    let external_ids_json = external_ids_json
        .map(serde_json::from_value::<ExternalIds>)
        .transpose()
        .context("parsing managed media tombstone external ids")?;
    let manager_provider_id_raw: Option<String> = row.try_get("manager_provider_id").ok().flatten();
    let cleared_at_raw: Option<String> = row.try_get("cleared_at").ok().flatten();
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(ManagedMediaTombstone {
        tombstone_id: parse_uuid(&tombstone_id_raw, "managed_media_tombstones.tombstone_id")?,
        media_type: parse_media_type(&media_type_raw, "managed_media_tombstones.media_type")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        year: row.try_get::<i64, _>("year").ok().map(|value| value as i32),
        external_ids: external_ids_json,
        manager_provider_id: manager_provider_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "managed_media_tombstones.manager_provider_id"))
            .transpose()?,
        manager_item_id: row.try_get("manager_item_id").ok().flatten(),
        manager_label: row.try_get("manager_label").ok().flatten(),
        manager_implementation: row.try_get("manager_implementation").ok().flatten(),
        action: row.try_get("action")?,
        active: row
            .try_get::<i64, _>("active")
            .ok()
            .map(|value| value != 0)
            .unwrap_or(false),
        cleared_at: cleared_at_raw
            .as_deref()
            .map(|value| parse_datetime(value, "managed_media_tombstones.cleared_at"))
            .transpose()?,
        created_at: parse_datetime(&created_at_raw, "managed_media_tombstones.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "managed_media_tombstones.updated_at")?,
    })
}

fn map_managed_episode_tombstone(row: &AnyRow) -> Result<ManagedEpisodeTombstone> {
    let tombstone_id_raw: String = row.try_get("tombstone_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let external_ids_json = parse_json_opt(
        row_get_opt_string(row, "external_ids_json")?,
        "managed_episode_tombstones.external_ids_json",
    )?;
    let external_ids_json = external_ids_json
        .map(serde_json::from_value::<ExternalIds>)
        .transpose()
        .context("parsing managed episode tombstone external ids")?;
    let manager_provider_id_raw: Option<String> = row.try_get("manager_provider_id").ok().flatten();
    let cleared_at_raw: Option<String> = row.try_get("cleared_at").ok().flatten();
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    Ok(ManagedEpisodeTombstone {
        tombstone_id: parse_uuid(&tombstone_id_raw, "managed_episode_tombstones.tombstone_id")?,
        media_type: parse_media_type(&media_type_raw, "managed_episode_tombstones.media_type")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        year: row.try_get::<i64, _>("year").ok().map(|value| value as i32),
        external_ids: external_ids_json,
        manager_provider_id: manager_provider_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "managed_episode_tombstones.manager_provider_id"))
            .transpose()?,
        manager_item_id: row.try_get("manager_item_id").ok().flatten(),
        manager_label: row.try_get("manager_label").ok().flatten(),
        manager_implementation: row.try_get("manager_implementation").ok().flatten(),
        season_number: row
            .try_get::<i64, _>("season_number")
            .ok()
            .unwrap_or_default() as i32,
        episode_number: row
            .try_get::<i64, _>("episode_number")
            .ok()
            .unwrap_or_default() as i32,
        absolute_episode_number: row
            .try_get::<i64, _>("absolute_episode_number")
            .ok()
            .map(|value| value as i32),
        action: row.try_get("action")?,
        active: row
            .try_get::<i64, _>("active")
            .ok()
            .map(|value| value != 0)
            .unwrap_or(false),
        cleared_at: cleared_at_raw
            .as_deref()
            .map(|value| parse_datetime(value, "managed_episode_tombstones.cleared_at"))
            .transpose()?,
        created_at: parse_datetime(&created_at_raw, "managed_episode_tombstones.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "managed_episode_tombstones.updated_at")?,
    })
}

fn parse_media_type(raw: &str, field: &str) -> Result<crate::db::models::MediaType> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "movie" => Ok(crate::db::models::MediaType::Movie),
        "series" => Ok(crate::db::models::MediaType::Series),
        "anime" => Ok(crate::db::models::MediaType::Anime),
        _ => anyhow::bail!("invalid enum value '{}' for field {}", raw, field),
    }
}

fn map_run(row: &AnyRow) -> Result<OrchestratorRun> {
    let run_id_raw: String = row.try_get("run_id")?;
    let source_raw: String = row.try_get("source")?;
    let status_raw: String = row.try_get("status")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(OrchestratorRun {
        run_id: parse_uuid(&run_id_raw, "orchestrator_runs.run_id")?,
        source: source_raw,
        status: parse_enum(&status_raw, "orchestrator_runs.status")?,
        phase: row_get_opt_string(row, "phase")?,
        plan_json: parse_json_opt(
            row_get_opt_string(row, "plan_json")?,
            "orchestrator_runs.plan_json",
        )?,
        error: row_get_opt_string(row, "error")?,
        created_at: parse_datetime(&created_at_raw, "orchestrator_runs.created_at")?,
        started_at: parse_datetime_opt(
            row_get_opt_string(row, "started_at")?,
            "orchestrator_runs.started_at",
        )?,
        finished_at: parse_datetime_opt(
            row_get_opt_string(row, "finished_at")?,
            "orchestrator_runs.finished_at",
        )?,
    })
}

fn map_step(row: &AnyRow) -> Result<OperationStep> {
    let step_id_raw: String = row.try_get("step_id")?;
    let run_id_raw: String = row.try_get("run_id")?;
    let status_raw: String = row.try_get("status")?;
    let created_at_raw: String = row.try_get("created_at")?;

    let step_index: i64 = row.try_get("step_index")?;
    let step_index = i32::try_from(step_index).context("operation_steps.step_index overflow")?;

    Ok(OperationStep {
        step_id: parse_uuid(&step_id_raw, "operation_steps.step_id")?,
        run_id: parse_uuid(&run_id_raw, "operation_steps.run_id")?,
        step_index,
        action_type: row.try_get("action_type")?,
        action_json: parse_json_opt(
            row_get_opt_string(row, "action_json")?,
            "operation_steps.action_json",
        )?,
        status: parse_enum(&status_raw, "operation_steps.status")?,
        error: row_get_opt_string(row, "error")?,
        started_at: parse_datetime_opt(
            row_get_opt_string(row, "started_at")?,
            "operation_steps.started_at",
        )?,
        finished_at: parse_datetime_opt(
            row_get_opt_string(row, "finished_at")?,
            "operation_steps.finished_at",
        )?,
        created_at: parse_datetime(&created_at_raw, "operation_steps.created_at")?,
    })
}

fn map_runtime_log(row: &AnyRow) -> Result<RuntimeLog> {
    let log_id_raw: String = row.try_get("log_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let created_at_raw: String = row.try_get("created_at")?;

    Ok(RuntimeLog {
        log_id: parse_uuid(&log_id_raw, "runtime_logs.log_id")?,
        instance_id: parse_uuid(&instance_id_raw, "runtime_logs.instance_id")?,
        log_uri: row.try_get("log_uri")?,
        created_at: parse_datetime(&created_at_raw, "runtime_logs.created_at")?,
    })
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    let details = err.to_string();
    details.contains("UNIQUE") || details.contains("unique")
}

fn parse_enum<T>(value: &str, field: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse::<T>()
        .map_err(|err| anyhow::anyhow!("invalid {field} '{value}': {err}"))
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(value).with_context(|| format!("invalid {field} '{value}'"))
}

fn parse_uuid_opt(value: Option<String>, field: &str) -> Result<Option<Uuid>> {
    match value {
        Some(value) => Ok(Some(parse_uuid(&value, field)?)),
        None => Ok(None),
    }
}

fn parse_datetime(value: &str, field: &str) -> Result<DateTime<Utc>> {
    let value = value.trim();
    let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .with_context(|| format!("invalid {field} '{value}'"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc))
}

fn parse_datetime_opt(value: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    match value {
        Some(value) => Ok(Some(parse_datetime(&value, field)?)),
        None => Ok(None),
    }
}

fn db_datetime_string(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn parse_json(value: &str, field: &str) -> Result<serde_json::Value> {
    serde_json::from_str(value).with_context(|| format!("invalid {field} json"))
}

fn parse_json_opt(value: Option<String>, field: &str) -> Result<Option<serde_json::Value>> {
    match value {
        Some(value) => Ok(Some(parse_json(&value, field)?)),
        None => Ok(None),
    }
}

fn row_get_opt_string(row: &AnyRow, field: &str) -> Result<Option<String>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value))
}

fn row_get_bool(row: &AnyRow, field: &str) -> Result<bool> {
    if let Ok(value) = row.try_get::<bool, _>(field) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(field) {
        return Ok(value != 0);
    }
    if let Ok(value) = row.try_get::<i32, _>(field) {
        return Ok(value != 0);
    }
    let value: String = row
        .try_get(field)
        .with_context(|| format!("missing {field}"))?;
    Ok(matches!(value.as_str(), "1" | "true" | "TRUE"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{Database, models::ExtensionKind},
    };
    use serde_json::json;

    async fn test_store() -> Result<(Database, Uuid)> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            ..DatabaseConfig::default()
        })
        .await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: "elixir.sources.cloudstream_compat".to_string(),
                name: "CloudStream Compat".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({"id": "elixir.sources.cloudstream_compat"}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.sources.cloudstream_compat".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        Ok((database, instance_id))
    }

    #[tokio::test]
    async fn cs1_source_registry_model_round_trips_update_health_and_replacement_state()
    -> Result<()> {
        let (database, instance_id) = test_store().await?;
        let store = ExtensionStore::new(&database.pool);
        let registry_id = Uuid::new_v4();
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "cloudstream.recommended".to_string(),
                registry_type: "elixir_curated_cloudstream_pack".to_string(),
                trust_class: "curated".to_string(),
                display_name: "Recommended CloudStream Sources".to_string(),
                url: None,
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: Some("etag-1".to_string()),
                last_modified: None,
                metadata_json: Some(json!({"channel": "stable"})),
            })
            .await?;
        store
            .record_source_registry_fetch(registry_id, "success", None, Some("etag-2"), None)
            .await?;

        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert_eq!(registries.len(), 1);
        assert_eq!(registries[0].registry_key, "cloudstream.recommended");
        assert_eq!(registries[0].last_fetch_status, "success");
        assert_eq!(registries[0].etag.as_deref(), Some("etag-2"));
        assert!(registries[0].trusted_for_executable_updates);

        let source_module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: "example.module".to_string(),
                display_name: "Example Module".to_string(),
                ecosystem: "cloudstream".to_string(),
                plugin_package: Some("com.example.cloudstream".to_string()),
                active_version: Some("1.0.0".to_string()),
                rollback_version: None,
                media_types_json: Some(json!(["movie", "tv"])),
                language_tags_json: Some(json!(["eng"])),
                region_tags_json: Some(json!(["us"])),
                source_domains_json: Some(json!(["example.invalid"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: true,
                installed: true,
                pinned_version: None,
                health_state: "healthy".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: Some(json!({"sourcePack": "recommended"})),
            })
            .await?;

        let version_100 = Uuid::new_v4();
        let version_110 = Uuid::new_v4();
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id: version_100,
                source_module_id,
                version: "1.0.0".to_string(),
                artifact_url: Some("https://repo.example/plugin-1.0.0.jar".to_string()),
                artifact_sha256: Some("sha256-a".to_string()),
                signature: None,
                install_state: "active".to_string(),
                smoke_status: "passed".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: Some(Utc::now()),
                activated_at: Some(Utc::now()),
                metadata_json: None,
            })
            .await?;
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id: version_110,
                source_module_id,
                version: "1.1.0".to_string(),
                artifact_url: Some("https://repo.example/plugin-1.1.0.jar".to_string()),
                artifact_sha256: Some("sha256-b".to_string()),
                signature: Some("sig-b".to_string()),
                install_state: "staged".to_string(),
                smoke_status: "passed".to_string(),
                smoke_error: None,
                rollback_of_version_id: Some(version_100),
                installed_at: Some(Utc::now()),
                activated_at: None,
                metadata_json: Some(json!({"rollsBackTo": "1.0.0"})),
            })
            .await?;
        store
            .set_source_module_active_version(source_module_id, Some("1.1.0"), Some("1.0.0"))
            .await?;

        store
            .create_source_health_event(&NewExtensionSourceHealthEvent {
                health_event_id: Uuid::new_v4(),
                source_module_id,
                event_type: "smoke_probe".to_string(),
                state: "broken".to_string(),
                severity: "error".to_string(),
                reason: Some("link extraction failed".to_string()),
                evidence_json: Some(json!({"stage": "load_links"})),
                observed_at: Some(Utc::now()),
            })
            .await?;

        let replacement_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id: replacement_id,
                instance_id,
                registry_id,
                module_key: "example.replacement".to_string(),
                display_name: "Example Replacement".to_string(),
                ecosystem: "cloudstream".to_string(),
                plugin_package: Some("com.example.replacement".to_string()),
                active_version: Some("1.0.0".to_string()),
                rollback_version: None,
                media_types_json: Some(json!(["movie", "tv"])),
                language_tags_json: Some(json!(["eng"])),
                region_tags_json: None,
                source_domains_json: Some(json!(["replacement.invalid"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: true,
                installed: true,
                pinned_version: None,
                health_state: "healthy".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: None,
            })
            .await?;
        let recommendation_id = Uuid::new_v4();
        store
            .upsert_source_replacement_recommendation(
                &NewExtensionSourceReplacementRecommendation {
                    recommendation_id,
                    source_module_id,
                    replacement_source_module_id: Some(replacement_id),
                    replacement_registry_id: Some(registry_id),
                    recommendation_key: "replace-example-module".to_string(),
                    action: "replace".to_string(),
                    recommended_version: Some("1.0.0".to_string()),
                    reason: Some("upstream source moved".to_string()),
                    metadata_json: Some(json!({"maintainer": "elixir"})),
                    active: true,
                },
            )
            .await?;

        let modules = store
            .list_source_modules(Some(instance_id), Some(registry_id))
            .await?;
        let source_module = modules
            .iter()
            .find(|module| module.source_module_id == source_module_id)
            .expect("source module");
        assert_eq!(source_module.active_version.as_deref(), Some("1.1.0"));
        assert_eq!(source_module.rollback_version.as_deref(), Some("1.0.0"));
        assert_eq!(source_module.health_state, "broken");
        assert_eq!(
            source_module.last_error.as_deref(),
            Some("link extraction failed")
        );
        assert!(source_module.last_failure_at.is_some());

        let versions = store.list_source_module_versions(source_module_id).await?;
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[1].rollback_of_version_id, Some(version_100));
        assert_eq!(versions[1].signature.as_deref(), Some("sig-b"));

        let events = store
            .list_source_health_events(source_module_id, 10)
            .await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, "broken");
        assert_eq!(events[0].severity, "error");

        let recommendations = store
            .list_source_replacement_recommendations(Some(source_module_id), true)
            .await?;
        assert_eq!(recommendations.len(), 1);
        assert_eq!(
            recommendations[0].replacement_source_module_id,
            Some(replacement_id)
        );
        store
            .mark_source_replacement_recommendation_applied(recommendation_id)
            .await?;
        let active = store
            .list_source_replacement_recommendations(Some(source_module_id), true)
            .await?;
        assert!(active.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn prism_source_certification_and_quarantine_round_trip() -> Result<()> {
        let (database, instance_id) = test_store().await?;
        let store = ExtensionStore::new(&database.pool);
        let registry_id = Uuid::new_v4();
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "prism.fixture".to_string(),
                registry_type: "nuvio_manifest_json".to_string(),
                trust_class: "maintainer_known".to_string(),
                display_name: "Prism Fixture".to_string(),
                url: Some("https://example.test/manifest.json".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;
        let source_module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: "nuvio:fixture:movies".to_string(),
                display_name: "Movies".to_string(),
                ecosystem: "nuvio".to_string(),
                plugin_package: Some("movies".to_string()),
                active_version: Some("1.0.0".to_string()),
                rollback_version: None,
                media_types_json: Some(json!(["movie"])),
                language_tags_json: None,
                region_tags_json: None,
                source_domains_json: Some(json!(["example.test"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: false,
                installed: true,
                pinned_version: None,
                health_state: "available".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: Some(json!({"nuvio": {"moduleId": "movies"}})),
            })
            .await?;
        let version_id = Uuid::new_v4();
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id,
                source_module_id,
                version: "1.0.0".to_string(),
                artifact_url: Some("https://example.test/movies.js".to_string()),
                artifact_sha256: Some("sha256".to_string()),
                signature: None,
                install_state: "active".to_string(),
                smoke_status: "unknown".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: Some(Utc::now()),
                activated_at: Some(Utc::now()),
                metadata_json: Some(
                    json!({"nuvio": {"scriptPath": "/app/source-modules/movies.js"}}),
                ),
            })
            .await?;

        store
            .upsert_source_module_certification(&NewExtensionSourceModuleCertification {
                certification_id: Uuid::new_v4(),
                source_module_id,
                source_module_version_id: Some(version_id),
                artifact_sha256: Some("sha256".to_string()),
                instance_id,
                adapter: "nuvio_js_v1".to_string(),
                status: "certified".to_string(),
                failure_class: None,
                summary: Some("probe passed".to_string()),
                media_type_results_json: json!({"movie": {"status": "certified"}}),
                materialization_results_json: json!({"inspected": []}),
                probe_targets_json: json!([{"mediaType": "movie"}]),
                candidate_evidence_json: json!([]),
                runtime_version: Some("1.0.0".to_string()),
                policy_version: "test-policy".to_string(),
                certified_at: Some(Utc::now()),
                expires_at: Some(Utc::now() + chrono::Duration::days(1)),
            })
            .await?;

        let latest = store
            .latest_source_module_certification(source_module_id)
            .await?
            .expect("certification");
        assert_eq!(latest.status, "certified");
        assert_eq!(latest.source_module_version_id, Some(version_id));
        assert_eq!(
            latest
                .media_type_results_json
                .pointer("/movie/status")
                .and_then(serde_json::Value::as_str),
            Some("certified")
        );

        let skipped_job_id = Uuid::new_v4();
        store
            .create_source_certification_job(&NewExtensionSourceCertificationJob {
                job_id: skipped_job_id,
                instance_id,
                registry_id: Some(registry_id),
                source_module_id: Some(source_module_id),
                requested_by: "test".to_string(),
                reason: "repository_added".to_string(),
                status: "skipped".to_string(),
                priority: 200,
                attempts: 0,
                max_attempts: 1,
                language_eligibility: Some(
                    json!({"state": "skipped_language", "normalizedTags": ["hi"]}).to_string(),
                ),
                marketplace_state: Some("skipped_language".to_string()),
                summary: Some("language mismatch".to_string()),
                last_error: None,
            })
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let job_id = Uuid::new_v4();
        store
            .create_source_certification_job(&NewExtensionSourceCertificationJob {
                job_id,
                instance_id,
                registry_id: Some(registry_id),
                source_module_id: Some(source_module_id),
                requested_by: "test".to_string(),
                reason: "manual_repository_certification".to_string(),
                status: "queued".to_string(),
                priority: 100,
                attempts: 0,
                max_attempts: 2,
                language_eligibility: Some(
                    json!({"state": "preferred_language", "normalizedTags": ["en"]}).to_string(),
                ),
                marketplace_state: Some("certifying".to_string()),
                summary: Some("queued".to_string()),
                last_error: None,
            })
            .await?;
        let claimed = store
            .claim_next_source_certification_job(instance_id)
            .await?
            .expect("queued certification job should be claimed");
        assert_eq!(claimed.job_id, job_id);
        assert_eq!(claimed.status, "running");
        assert_eq!(claimed.attempts, 1);
        let requeued = store
            .requeue_running_source_certification_jobs(instance_id, "server restarted")
            .await?;
        assert_eq!(requeued, 1);
        let requeued_job = store
            .get_source_certification_job(job_id)
            .await?
            .expect("requeued job should remain visible");
        assert_eq!(requeued_job.status, "queued");
        assert_eq!(requeued_job.attempts, 1);
        let claimed = store
            .claim_next_source_certification_job(instance_id)
            .await?
            .expect("requeued certification job should be claimed again");
        assert_eq!(claimed.job_id, job_id);
        assert_eq!(claimed.status, "running");
        assert_eq!(claimed.attempts, 2);
        store
            .finish_source_certification_job(
                job_id,
                "succeeded",
                Some("certified"),
                Some("runtime probe passed"),
                None,
            )
            .await?;

        let latest_jobs = store
            .list_latest_source_certification_jobs(instance_id)
            .await?;
        assert_eq!(latest_jobs.len(), 1);
        assert_eq!(latest_jobs[0].job_id, job_id);
        assert_eq!(
            latest_jobs[0].marketplace_state.as_deref(),
            Some("certified")
        );
        assert_eq!(latest_jobs[0].attempts, 2);
        let registry_jobs = store
            .list_source_certification_jobs_for_registry(registry_id, 10)
            .await?;
        assert_eq!(registry_jobs.len(), 2);

        let cancellable_job_id = Uuid::new_v4();
        store
            .create_source_certification_job(&NewExtensionSourceCertificationJob {
                job_id: cancellable_job_id,
                instance_id,
                registry_id: Some(registry_id),
                source_module_id: Some(source_module_id),
                requested_by: "test".to_string(),
                reason: "repository_refreshed".to_string(),
                status: "queued".to_string(),
                priority: 100,
                attempts: 0,
                max_attempts: 2,
                language_eligibility: None,
                marketplace_state: Some("certifying".to_string()),
                summary: Some("queued".to_string()),
                last_error: None,
            })
            .await?;
        let cancelled = store
            .cancel_source_certification_jobs(
                instance_id,
                Some(registry_id),
                None,
                "cancelled by test",
            )
            .await?;
        assert_eq!(cancelled, 1);

        store
            .record_source_module_quarantine(&NewExtensionSourceModuleQuarantine {
                quarantine_id: Uuid::new_v4(),
                source_module_id,
                source_module_version_id: Some(version_id),
                instance_id,
                failure_class: "source_returned_non_media_response".to_string(),
                hoster_domain: Some("hoster.example".to_string()),
                candidate_fingerprint: Some("candidate-1".to_string()),
                media_type: Some("movie".to_string()),
                reason: Some("returned HTML".to_string()),
                evidence_json: Some(json!({"candidate": "candidate-1"})),
                expires_at: Some(Utc::now() + chrono::Duration::hours(6)),
            })
            .await?;

        let count = sqlx::query_scalar::<_, i64>(
            "SELECT failure_count FROM extension_source_module_quarantines WHERE source_module_id = ?",
        )
        .bind(source_module_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn source_registry_delete_explicitly_removes_modules_versions_and_certification_jobs()
    -> Result<()> {
        let (database, instance_id) = test_store().await?;
        let store = ExtensionStore::new(&database.pool);
        let registry_id = Uuid::new_v4();
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "prism.remove.fixture".to_string(),
                registry_type: "nuvio_manifest_json".to_string(),
                trust_class: "maintainer_known".to_string(),
                display_name: "Remove Fixture".to_string(),
                url: Some("https://example.test/manifest.json".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;
        let source_module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: "nuvio:remove:fixture".to_string(),
                display_name: "Remove Fixture".to_string(),
                ecosystem: "nuvio".to_string(),
                plugin_package: Some("remove_fixture".to_string()),
                active_version: Some("1.0.0".to_string()),
                rollback_version: None,
                media_types_json: Some(json!(["movie"])),
                language_tags_json: Some(json!(["en"])),
                region_tags_json: None,
                source_domains_json: Some(json!(["example.test"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: false,
                installed: true,
                pinned_version: None,
                health_state: "available".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: None,
            })
            .await?;
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id: Uuid::new_v4(),
                source_module_id,
                version: "1.0.0".to_string(),
                artifact_url: Some("https://example.test/remove.js".to_string()),
                artifact_sha256: Some("sha256".to_string()),
                signature: None,
                install_state: "active".to_string(),
                smoke_status: "passed".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: Some(Utc::now()),
                activated_at: Some(Utc::now()),
                metadata_json: None,
            })
            .await?;
        store
            .create_source_certification_job(&NewExtensionSourceCertificationJob {
                job_id: Uuid::new_v4(),
                instance_id,
                registry_id: Some(registry_id),
                source_module_id: Some(source_module_id),
                requested_by: "test".to_string(),
                reason: "repository_added".to_string(),
                status: "queued".to_string(),
                priority: 100,
                attempts: 0,
                max_attempts: 2,
                language_eligibility: None,
                marketplace_state: Some("certifying".to_string()),
                summary: Some("queued".to_string()),
                last_error: None,
            })
            .await?;

        sqlx::query("PRAGMA foreign_keys = OFF;")
            .execute(&database.pool)
            .await?;

        let deleted = store.delete_source_registry(registry_id).await?;
        assert_eq!(deleted, 1);
        assert!(
            store
                .list_source_modules(None, Some(registry_id))
                .await?
                .is_empty()
        );
        assert!(
            store
                .list_source_module_versions(source_module_id)
                .await?
                .is_empty()
        );
        assert!(
            store
                .list_source_certification_jobs_for_registry(registry_id, 10)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn source_orphan_repair_removes_modules_left_after_registry_delete() -> Result<()> {
        let (database, instance_id) = test_store().await?;
        let store = ExtensionStore::new(&database.pool);
        let registry_id = Uuid::new_v4();
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "prism.orphan.fixture".to_string(),
                registry_type: "nuvio_manifest_json".to_string(),
                trust_class: "maintainer_known".to_string(),
                display_name: "Orphan Fixture".to_string(),
                url: Some("https://example.test/manifest.json".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;
        let source_module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: "nuvio:orphan:fixture".to_string(),
                display_name: "Orphan Fixture".to_string(),
                ecosystem: "nuvio".to_string(),
                plugin_package: Some("orphan_fixture".to_string()),
                active_version: Some("1.0.0".to_string()),
                rollback_version: None,
                media_types_json: Some(json!(["movie"])),
                language_tags_json: Some(json!(["en"])),
                region_tags_json: None,
                source_domains_json: Some(json!(["example.test"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: true,
                installed: true,
                pinned_version: None,
                health_state: "available".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: None,
            })
            .await?;
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id: Uuid::new_v4(),
                source_module_id,
                version: "1.0.0".to_string(),
                artifact_url: Some("https://example.test/orphan.js".to_string()),
                artifact_sha256: Some("sha256".to_string()),
                signature: None,
                install_state: "active".to_string(),
                smoke_status: "passed".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: Some(Utc::now()),
                activated_at: Some(Utc::now()),
                metadata_json: None,
            })
            .await?;
        store
            .create_source_certification_job(&NewExtensionSourceCertificationJob {
                job_id: Uuid::new_v4(),
                instance_id,
                registry_id: Some(registry_id),
                source_module_id: Some(source_module_id),
                requested_by: "test".to_string(),
                reason: "repository_added".to_string(),
                status: "queued".to_string(),
                priority: 100,
                attempts: 0,
                max_attempts: 2,
                language_eligibility: None,
                marketplace_state: Some("certifying".to_string()),
                summary: Some("queued".to_string()),
                last_error: None,
            })
            .await?;

        sqlx::query("PRAGMA foreign_keys = OFF;")
            .execute(&database.pool)
            .await?;
        sqlx::query("DELETE FROM extension_source_registries WHERE registry_id = ?")
            .bind(registry_id.to_string())
            .execute(&database.pool)
            .await?;
        assert_eq!(
            store
                .list_source_modules(Some(instance_id), Some(registry_id))
                .await?
                .len(),
            1
        );

        let repaired = store
            .delete_orphan_source_modules_for_instance(instance_id)
            .await?;
        assert_eq!(repaired, 1);
        assert!(
            store
                .list_source_modules(Some(instance_id), Some(registry_id))
                .await?
                .is_empty()
        );
        assert!(
            store
                .list_source_module_versions(source_module_id)
                .await?
                .is_empty()
        );
        assert!(
            store
                .list_source_certification_jobs_for_registry(registry_id, 10)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn cs1_source_registry_model_rejects_invalid_state_before_sql() -> Result<()> {
        let (database, instance_id) = test_store().await?;
        let store = ExtensionStore::new(&database.pool);
        let registry_id = Uuid::new_v4();
        let invalid_registry = store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "bad".to_string(),
                registry_type: "unknown_repo".to_string(),
                trust_class: "custom".to_string(),
                display_name: "Bad Repo".to_string(),
                url: None,
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: false,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await;
        assert!(invalid_registry.is_err());

        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "cloudstream.recommended".to_string(),
                registry_type: "elixir_curated_cloudstream_pack".to_string(),
                trust_class: "curated".to_string(),
                display_name: "Recommended CloudStream Sources".to_string(),
                url: None,
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;
        let unsupported_without_reason = store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id: Uuid::new_v4(),
                instance_id,
                registry_id,
                module_key: "unsupported".to_string(),
                display_name: "Unsupported".to_string(),
                ecosystem: "cloudstream".to_string(),
                plugin_package: None,
                active_version: None,
                rollback_version: None,
                media_types_json: None,
                language_tags_json: None,
                region_tags_json: None,
                source_domains_json: None,
                account_required: false,
                unsupported: true,
                unsupported_reason: None,
                enabled: false,
                installed: false,
                pinned_version: None,
                health_state: "unsupported".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: None,
            })
            .await;
        assert!(unsupported_without_reason.is_err());

        let module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id: module_id,
                instance_id,
                registry_id,
                module_key: "valid".to_string(),
                display_name: "Valid".to_string(),
                ecosystem: "cloudstream".to_string(),
                plugin_package: None,
                active_version: None,
                rollback_version: None,
                media_types_json: None,
                language_tags_json: None,
                region_tags_json: None,
                source_domains_json: None,
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: true,
                installed: false,
                pinned_version: None,
                health_state: "available".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: None,
            })
            .await?;
        let replace_without_target = store
            .upsert_source_replacement_recommendation(
                &NewExtensionSourceReplacementRecommendation {
                    recommendation_id: Uuid::new_v4(),
                    source_module_id: module_id,
                    replacement_source_module_id: None,
                    replacement_registry_id: None,
                    recommendation_key: "missing-target".to_string(),
                    action: "replace".to_string(),
                    recommended_version: None,
                    reason: None,
                    metadata_json: None,
                    active: true,
                },
            )
            .await;
        assert!(replace_without_target.is_err());
        Ok(())
    }
}
