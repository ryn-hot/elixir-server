use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{
        HeaderMap as AxumHeaderMap, HeaderName as AxumHeaderName, HeaderValue as AxumHeaderValue,
        Method, StatusCode, header as axum_header,
    },
    response::Response,
};
use base64::{Engine as _, engine::general_purpose};
use rand::{RngCore, rngs::OsRng};
use reqwest::header::{
    ACCEPT, COOKIE as REQWEST_COOKIE, HOST as REQWEST_HOST, HeaderMap as ReqwestHeaderMap,
    HeaderValue as ReqwestHeaderValue, USER_AGENT,
};
use reqwest::{Client, Method as ReqwestMethod, StatusCode as ReqwestStatusCode, Url};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::fs;
use tokio::net::lookup_host;
use tokio::process::Command;
use uuid::Uuid;

use crate::config::{DownloaderPerformanceProfile, RunEnvironment};
use crate::db::models::{
    Binding, BindingStatus, DesiredBlueprint, Extension, ExtensionInstance, ExtensionKind,
    ExtensionTrustLevel, OperationStep, OperationStepStatus, OrchestratorRun,
    OrchestratorRunStatus, Provider, ProviderHealthState, ProviderReadiness,
    ProviderReadinessPhase, RuntimeLog, Secret, SecretScope,
};
use crate::debrid::{REAL_DEBRID_EXTENSION_ID, REAL_DEBRID_TOKEN_SECRET_KEY};
use crate::drivers::{IndexerRegistryPatch, bootstrap_qbittorrent_session_cookie};
use crate::extensions::auto_managed::filter_auto_managed_runtime_missing;
use crate::extensions::managed_paths::{DOWNLOADS_ROOT, QBITTORRENT_INCOMPLETE_DIR};
use crate::extensions::manifest::{ExtensionManifest, repair_builtin_manifest_json};
use crate::extensions::package::{
    PackageManifest, compute_sha256, read_manifest_from_dir, read_package_signature,
    unpack_package, verify_signature, write_manifest_to_dir,
};
use crate::extensions::permissions::PermissionPolicy;
use crate::extensions::registry::{
    RegistryCacheStore, RegistryClient, RegistryEntry, RegistryFetchError, refresh_registry_cache,
};
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, missing_required_secrets_for_instances,
    required_secrets_from_manifest,
};
use crate::extensions::store::{
    ExtensionStore, ManagedIngestIntent, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
    NewOperationStep, NewOrchestratorRun, NewSecret,
};
use crate::extensions::updater::{ProxyRuntimeUpdateState, load_proxy_runtime_update_state};
use crate::http::auth::CurrentUser;
use crate::http::error::{ApiError, ApiResult};
use crate::orchestrator::executor::ExecutorAction;
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::naming::build_aliases;
use crate::orchestrator::plan_executor::{PlanExecutor, PlannedStep};
use crate::orchestrator::plan_validation::{
    has_unresolved_conflicts, missing_required_secrets_for_plan,
};
use crate::orchestrator::planner::{Plan, PlanAction, PlanBlockedStage, PlanStage, Planner};
use crate::orchestrator::reconcile::ReconcileConfig;
use crate::runtime::health::{DockerRuntimeHealthSnapshot, DockerRuntimeHealthState};
use crate::state::AppState;

mod control;
mod control_contract;

use control_contract::{
    control_notice, control_policy_managed, control_policy_observed, control_policy_seeded,
    repair_managed_invariants_action, section_has_managed_drift,
};

const EXTENSION_UI_TOKEN_TTL_MINUTES: u64 = 60 * 12;

#[derive(Debug, Serialize)]
pub struct CatalogResponse {
    pub installed: Vec<Extension>,
    pub available: Vec<RegistryEntry>,
    pub registry_errors: Vec<RegistryFetchError>,
    pub last_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_refresh_success_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_refresh_error: Option<RegistryFetchError>,
    pub core_extensions: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallRequest {
    pub download_url: Option<String>,
    pub package_path: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InstallResponse {
    pub extension_id: String,
    pub name: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub trust_level: ExtensionTrustLevel,
    pub enabled: bool,
}

struct InstallResult {
    response: InstallResponse,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct InstallPolicy {
    pub allow_internal_directory_install: bool,
    pub allow_internal_unsigned: bool,
    pub allow_downgrade: bool,
    pub allow_same_version_replace: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateInstanceRequest {
    pub instance_name: Option<String>,
    pub config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateInstanceRequest {
    pub instance_name: Option<String>,
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct InstancesQuery {
    pub extension_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GraphResponse {
    pub providers: Vec<Provider>,
    pub bindings: Vec<Binding>,
}

#[derive(Debug, Serialize)]
pub struct ProviderHealthResponse {
    pub provider_id: Uuid,
    pub health_state: ProviderHealthState,
    pub readiness_phase: ProviderReadinessPhase,
    pub readiness_detail: Option<String>,
    pub last_healthcheck_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
pub struct RuntimeLogsResponse {
    pub instance_id: Uuid,
    pub logs: Vec<RuntimeLog>,
}

#[derive(Debug, Deserialize)]
pub struct RunsQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloaderProfileResponse {
    pub profile: DownloaderPerformanceProfile,
    pub default_profile: DownloaderPerformanceProfile,
    pub source: String,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub pending_update_count: usize,
    pub profiles: Vec<DownloaderProfileOption>,
    pub downloaders: Vec<DownloaderTelemetryItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloaderProfileOption {
    pub id: DownloaderPerformanceProfile,
    pub label: &'static str,
    pub description: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloaderTelemetryItem {
    pub name: String,
    pub extension_id: String,
    pub instance_id: Uuid,
    pub instance_name: String,
    pub capability: String,
    pub implementation: Option<String>,
    pub health_state: ProviderHealthState,
    pub readiness_phase: ProviderReadinessPhase,
    pub readiness_detail: Option<String>,
    pub last_healthcheck_at: Option<chrono::DateTime<chrono::Utc>>,
    pub applied_profile: Option<DownloaderPerformanceProfile>,
    pub sync_state: String,
    pub state_summary: Option<String>,
    pub status: Option<String>,
    pub download_rate_bps: Option<u64>,
    pub upload_rate_bps: Option<u64>,
    pub active_items: Option<u64>,
    pub queued_items: Option<u64>,
    pub error_items: Option<u64>,
    pub post_process_items: Option<u64>,
    pub downloaded_bytes: Option<u64>,
    pub uploaded_bytes: Option<u64>,
    pub last_successful_sample_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_error_at: Option<chrono::DateTime<chrono::Utc>>,
    pub telemetry_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDownloaderProfileRequest {
    pub profile: DownloaderPerformanceProfile,
}

#[derive(Debug, Deserialize)]
pub struct DesiredBlueprintsQuery {
    pub applied: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDetailResponse {
    pub run: OrchestratorRun,
    pub steps: Vec<OperationStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_summary: Option<RunStageSummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStageSummary {
    pub current_stage_id: Option<String>,
    pub current_stage_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_stage: Option<PlanBlockedStage>,
    #[serde(default)]
    pub stages: Vec<RunStageProgress>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStageProgress {
    pub stage_id: String,
    pub status: String,
    pub step_count: usize,
    pub completed_step_count: usize,
}

#[derive(Debug, Serialize)]
pub struct ReconcileRunResponse {
    pub run: Option<OrchestratorRun>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeResetResponse {
    pub status: String,
    pub message: String,
    pub docker_restarted: bool,
    pub reboot_recommended: bool,
    pub removed_containers: Vec<String>,
    pub recreated_networks: Vec<String>,
    pub run: Option<OrchestratorRun>,
}

#[derive(Debug, Serialize)]
pub struct DesiredBlueprintsClearResponse {
    pub deleted: u64,
}

#[derive(Debug, Serialize)]
pub struct RunsClearResponse {
    pub deleted: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionStatusSummaryResponse {
    pub needs_attention_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_runtime: Option<DockerRuntimeStatusSummary>,
    pub items: Vec<ExtensionStatusSummaryItem>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DockerRuntimeStatusSummary {
    pub state: String,
    pub severity: String,
    pub code: String,
    pub label: String,
    pub description: String,
    #[serde(default)]
    pub reboot_recommended: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_warning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reset_attempt_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub auto_reset_attempts_in_window: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quarantined_instances: Vec<DockerRuntimeQuarantineSummary>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DockerRuntimeQuarantineSummary {
    pub instance_id: Uuid,
    pub extension_id: String,
    pub extension_name: String,
    pub instance_name: String,
    pub reason: String,
    pub until: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionStatusSummaryItem {
    pub extension_id: String,
    pub name: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub trust_level: ExtensionTrustLevel,
    pub enabled: bool,
    pub severity: String,
    pub status_code: String,
    pub label: String,
    pub description: String,
    pub primary_action: String,
    pub primary_action_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_update: Option<ExtensionAutoUpdateSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub optional_addons: Vec<ExtensionOptionalAddonSummaryItem>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionAutoUpdateSummary {
    pub severity: String,
    pub status_code: String,
    pub label: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_version: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionOptionalAddonSummaryItem {
    pub extension_id: String,
    pub title: String,
    pub description: String,
    pub severity: String,
    pub status_code: String,
    pub label: String,
    pub action: String,
    pub action_label: String,
    #[serde(default)]
    pub required_fields: Vec<String>,
    #[serde(default)]
    pub secret_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_scope_instance_id: Option<Uuid>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlSurface {
    pub extension_id: String,
    pub name: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub trust_level: ExtensionTrustLevel,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation: Option<String>,
    pub status: ExtensionControlStatus,
    #[serde(default)]
    pub sections: Vec<ExtensionControlSection>,
    #[serde(default)]
    pub actions: Vec<ExtensionControlAction>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlStatus {
    pub health: String,
    pub summary: String,
    #[serde(default)]
    pub details: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<ExtensionControlTelemetry>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlTelemetry {
    #[serde(default)]
    pub metrics: Vec<ExtensionControlMetric>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlMetric {
    pub id: String,
    pub label: String,
    pub value: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlSection {
    pub id: String,
    pub title: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<ExtensionControlPolicy>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notices: Vec<ExtensionControlNotice>,
    #[serde(default)]
    pub fields: Vec<ExtensionControlField>,
    #[serde(default)]
    pub entities: Vec<ExtensionControlEntity>,
    #[serde(default)]
    pub actions: Vec<ExtensionControlAction>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlPolicy {
    pub mode: String,
    pub label: String,
    pub description: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlNotice {
    pub severity: String,
    pub code: String,
    pub title: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ExtensionControlAction>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlEntity {
    pub id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    #[serde(default)]
    pub details: Vec<String>,
    #[serde(default)]
    pub actions: Vec<ExtensionControlAction>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlField {
    pub id: String,
    pub label: String,
    pub description: String,
    pub field_type: String,
    pub value: serde_json::Value,
    pub required: bool,
    pub readonly: bool,
    pub secret: bool,
    #[serde(default)]
    pub options: Vec<ExtensionControlOption>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlOption {
    pub value: serde_json::Value,
    pub label: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlAction {
    pub id: String,
    pub label: String,
    pub description: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub navigate_extension_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub navigate_view: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_url: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_scope_instance_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateExtensionControlSettingsRequest {
    #[serde(default)]
    pub values: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunExtensionControlActionRequest {
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionControlActionResponse {
    pub success: bool,
    pub message: String,
    pub control_surface: ExtensionControlSurface,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretsQuery {
    pub scope: Option<String>,
    pub scope_id: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSecretRequest {
    pub scope: String,
    pub scope_id: Option<String>,
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub rotatable: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSecretRequest {
    pub value: Option<String>,
    pub rotatable: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RotateSecretRequest {
    pub value: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretResponse {
    pub secret_id: Uuid,
    pub scope: SecretScope,
    pub scope_id: Option<Uuid>,
    pub key: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub rotatable: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RotateSecretResponse {
    pub secret_id: Uuid,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct ApplyBlueprintRequest {
    pub blueprint_id: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

pub async fn apply_blueprint(
    State(state): State<AppState>,
    Json(payload): Json<ApplyBlueprintRequest>,
) -> ApiResult<Json<Plan>> {
    let db_pool = state.db_pool.clone();
    let store = ExtensionStore::new(&db_pool);
    let blueprint = store
        .get_extension(&payload.blueprint_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("blueprint extension not found"))?;
    let manifest: ExtensionManifest = serde_json::from_value(blueprint.manifest_json.clone())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if manifest.execution.is_some() {
        ensure_execution_blueprint_packages_installed(&state, &manifest)
            .await
            .map_err(ApiError::from)?;
    }
    let planner = Planner::new();
    let mut plan = planner
        .plan_blueprint(&store, payload.blueprint_id, payload.params)
        .await
        .map_err(ApiError::from)?;

    let pending_runs = store
        .list_runs_by_source_status("blueprint", OrchestratorRunStatus::Pending)
        .await
        .map_err(ApiError::from)?;
    let mut reusable_run_id = None;
    for run in pending_runs {
        let Some(plan_json) = run.plan_json.as_ref() else {
            let _ = store
                .update_run_status(
                    run.run_id,
                    OrchestratorRunStatus::Canceled,
                    Some("canceled"),
                    Some("invalid blueprint preview payload"),
                )
                .await;
            continue;
        };
        let Ok(existing_plan) = serde_json::from_value::<Plan>(plan_json.clone()) else {
            let _ = store
                .update_run_status(
                    run.run_id,
                    OrchestratorRunStatus::Canceled,
                    Some("canceled"),
                    Some("invalid blueprint preview payload"),
                )
                .await;
            continue;
        };

        if existing_plan.blueprint_id == blueprint.extension_id
            && existing_plan.params == plan.params
        {
            if reusable_run_id.is_none() {
                reusable_run_id = Some(run.run_id);
                continue;
            }
        }

        let _ = store
            .update_run_status(
                run.run_id,
                OrchestratorRunStatus::Canceled,
                Some("canceled"),
                Some("superseded by newer blueprint preview"),
            )
            .await;
    }
    if let Some(run_id) = reusable_run_id {
        plan.plan_id = run_id;
        let plan_json =
            serde_json::to_value(&plan).map_err(|err| ApiError::internal(err.to_string()))?;
        store
            .update_run_plan(run_id, plan_json)
            .await
            .map_err(ApiError::from)?;
        return Ok(Json(plan));
    }

    let plan_json =
        serde_json::to_value(&plan).map_err(|err| ApiError::internal(err.to_string()))?;
    store
        .create_run(&NewOrchestratorRun {
            run_id: plan.plan_id,
            source: "blueprint".to_string(),
            status: OrchestratorRunStatus::Pending,
            phase: Some("planned".to_string()),
            plan_json: Some(plan_json),
            error: None,
        })
        .await
        .map_err(ApiError::from)?;

    Ok(Json(plan))
}

#[derive(Debug, Serialize)]
pub struct PlanRunResponse {
    pub run_id: Uuid,
    pub status: OrchestratorRunStatus,
}

pub async fn confirm_plan(
    State(state): State<AppState>,
    Path(plan_id): Path<String>,
    _payload: Option<Json<serde_json::Value>>,
) -> ApiResult<Json<PlanRunResponse>> {
    let run_id = Uuid::parse_str(&plan_id).map_err(|_| ApiError::bad_request("invalid plan id"))?;
    let db_pool = state.db_pool.clone();
    let store = ExtensionStore::new(&db_pool);

    let run = store
        .get_run(run_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("plan not found"))?;

    if run.status != OrchestratorRunStatus::Pending {
        return Err(ApiError::conflict("plan is not pending"));
    }

    let plan_json = run
        .plan_json
        .ok_or_else(|| ApiError::bad_request("plan has no payload"))?;
    let plan: Plan = serde_json::from_value(plan_json)
        .map_err(|err| ApiError::bad_request(format!("invalid plan payload: {err}")))?;

    if run.source == "blueprint" {
        let blueprint = store
            .get_extension(&plan.blueprint_id)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::not_found("blueprint extension not found"))?;
        let manifest: ExtensionManifest = serde_json::from_value(blueprint.manifest_json.clone())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        if manifest.execution.is_some() {
            ensure_execution_blueprint_packages_installed(&state, &manifest)
                .await
                .map_err(ApiError::from)?;
        }
    }

    let missing = missing_required_secrets_for_plan(&store, &plan.actions)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if !missing.is_empty() {
        return Err(ApiError::conflict(format!(
            "missing required secrets: {}",
            missing.join(", ")
        )));
    }

    if has_unresolved_conflicts(&plan.conflicts) {
        return Err(ApiError::conflict("plan has unresolved conflicts"));
    }

    if run.source == "blueprint" {
        let blueprint = store
            .get_extension(&plan.blueprint_id)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::not_found("blueprint extension not found"))?;
        store
            .upsert_desired_blueprint(&NewDesiredBlueprint {
                desired_id: run_id,
                blueprint_extension_id: blueprint.extension_id,
                blueprint_version: blueprint.version,
                params_json: plan.params.clone(),
            })
            .await
            .map_err(ApiError::from)?;
        store
            .mark_desired_applied(run_id, true)
            .await
            .map_err(ApiError::from)?;
    }

    let mut steps = Vec::with_capacity(plan.actions.len());
    for (index, action) in plan.actions.iter().enumerate() {
        let step_id = Uuid::new_v4();
        let action_json =
            serde_json::to_value(action).map_err(|err| ApiError::internal(err.to_string()))?;
        store
            .create_step(&NewOperationStep {
                step_id,
                run_id,
                step_index: index as i32,
                action_type: action.action_type().to_string(),
                action_json: Some(action_json),
                status: OperationStepStatus::Pending,
                error: None,
            })
            .await
            .map_err(ApiError::from)?;

        let executor_action = action
            .clone()
            .try_into()
            .map_err(|err: anyhow::Error| ApiError::bad_request(err.to_string()))?;
        steps.push(PlannedStep {
            step_id,
            action: executor_action,
        });
    }

    store
        .update_run_status(run_id, OrchestratorRunStatus::Running, Some("apply"), None)
        .await
        .map_err(ApiError::from)?;

    let executor = PlanExecutor::new(Arc::new(state));

    match executor.execute_steps(steps).await {
        Ok(()) => {
            store
                .update_run_status(
                    run_id,
                    OrchestratorRunStatus::Completed,
                    Some("completed"),
                    None,
                )
                .await
                .map_err(ApiError::from)?;
            Ok(Json(PlanRunResponse {
                run_id,
                status: OrchestratorRunStatus::Completed,
            }))
        }
        Err(err) => {
            let _ = store
                .update_run_status(
                    run_id,
                    OrchestratorRunStatus::Failed,
                    Some("failed"),
                    Some(&err.to_string()),
                )
                .await;
            Err(ApiError::internal(err.to_string()))
        }
    }
}

pub async fn cancel_plan(
    State(state): State<AppState>,
    Path(plan_id): Path<String>,
) -> ApiResult<Json<PlanRunResponse>> {
    let run_id = Uuid::parse_str(&plan_id).map_err(|_| ApiError::bad_request("invalid plan id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let run = store
        .get_run(run_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("plan not found"))?;

    if run.status != OrchestratorRunStatus::Pending {
        return Err(ApiError::conflict(
            "plan cannot be canceled in current state",
        ));
    }

    store
        .update_run_status(
            run_id,
            OrchestratorRunStatus::Canceled,
            Some("canceled"),
            None,
        )
        .await
        .map_err(ApiError::from)?;

    if run.source == "blueprint" {
        let _ = store.delete_desired_blueprint(run_id).await;
    }

    Ok(Json(PlanRunResponse {
        run_id,
        status: OrchestratorRunStatus::Canceled,
    }))
}

pub async fn catalog(State(state): State<AppState>) -> ApiResult<Json<CatalogResponse>> {
    let response = build_catalog(&state, false).await?;
    Ok(Json(response))
}

pub async fn refresh_catalog(State(state): State<AppState>) -> ApiResult<Json<CatalogResponse>> {
    let response = build_catalog(&state, true).await?;
    Ok(Json(response))
}

async fn build_catalog(state: &AppState, force_refresh: bool) -> Result<CatalogResponse, ApiError> {
    let store = ExtensionStore::new(&state.db_pool);
    let installed = store.list_extensions().await.map_err(ApiError::from)?;
    let bundled_available = bundled_catalog_entries(state).await?;

    if state.settings.extensions.registries.is_empty() {
        return Ok(CatalogResponse {
            installed,
            available: bundled_available,
            registry_errors: Vec::new(),
            last_refreshed_at: None,
            last_refresh_success_at: None,
            last_refresh_error: None,
            core_extensions: state.settings.extensions.core_extensions.clone(),
        });
    }

    let storage_paths = ExtensionStoragePaths::new(&state.settings.extensions.storage_root);
    let cache_store = RegistryCacheStore::new(storage_paths.registry_cache_dir.clone());
    let cache = cache_store.load().await.map_err(ApiError::from)?;
    let needs_refresh = force_refresh
        || cache
            .as_ref()
            .map(|cache| cache.registry_urls != state.settings.extensions.registries)
            .unwrap_or(true);

    let cache = if needs_refresh {
        refresh_registry_cache(
            &state.settings.extensions.registries,
            Duration::from_secs(10),
            &cache_store,
        )
        .await
        .map_err(ApiError::from)?
    } else if let Some(cache) = cache {
        cache
    } else {
        refresh_registry_cache(
            &state.settings.extensions.registries,
            Duration::from_secs(10),
            &cache_store,
        )
        .await
        .map_err(ApiError::from)?
    };

    Ok(CatalogResponse {
        installed,
        available: merge_catalog_entries(cache.index.extensions, bundled_available),
        registry_errors: cache.registry_errors,
        last_refreshed_at: Some(cache.fetched_at),
        last_refresh_success_at: cache.last_success_at,
        last_refresh_error: cache.last_error,
        core_extensions: state.settings.extensions.core_extensions.clone(),
    })
}

async fn bundled_catalog_entries(state: &AppState) -> Result<Vec<RegistryEntry>, ApiError> {
    let bundled_dir = PathBuf::from(&state.settings.extensions.bundled_dir);
    if !bundled_dir.is_dir() {
        return Ok(Vec::new());
    }

    let tmp_root = PathBuf::from(&state.settings.extensions.storage_root).join("tmp");
    fs::create_dir_all(&tmp_root)
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?;

    let mut entries = Vec::new();
    let mut dir = fs::read_dir(&bundled_dir)
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?;
    while let Some(entry) = dir
        .next_entry()
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?;
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let is_elx = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("elx"))
            .unwrap_or(false);
        if !is_elx {
            continue;
        }

        let staging_dir = tmp_root.join(Uuid::new_v4().to_string());
        let unpacked = unpack_package(&path, &staging_dir)
            .await
            .map_err(ApiError::from)?;
        let manifest = match read_manifest_from_dir(&unpacked).await {
            Ok(manifest) => manifest.manifest,
            Err(err) => {
                let _ = fs::remove_dir_all(&staging_dir).await;
                tracing::warn!(
                    "failed to read bundled catalog manifest from '{}': {err}",
                    path.display()
                );
                continue;
            }
        };
        let _ = fs::remove_dir_all(&staging_dir).await;

        entries.push(RegistryEntry {
            id: manifest.id,
            version: manifest.version,
            download_url: String::new(),
            package_path: Some(path.to_string_lossy().to_string()),
            sha256: None,
            signature: None,
            publisher_key_id: manifest
                .publisher
                .as_ref()
                .and_then(|publisher| publisher.key_id.clone()),
            trust: manifest.trust,
        });
    }

    Ok(entries)
}

fn merge_catalog_entries(
    mut registry_entries: Vec<RegistryEntry>,
    bundled_entries: Vec<RegistryEntry>,
) -> Vec<RegistryEntry> {
    for entry in bundled_entries {
        if registry_entries
            .iter()
            .any(|existing| existing.id == entry.id)
        {
            continue;
        }
        registry_entries.push(entry);
    }
    registry_entries
}

fn pick_best_catalog_entry(entries: &[RegistryEntry], extension_id: &str) -> Option<RegistryEntry> {
    let mut matches = entries
        .iter()
        .filter(|entry| entry.id == extension_id)
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        let left_version = Version::parse(&left.version).ok();
        let right_version = Version::parse(&right.version).ok();
        right_version
            .cmp(&left_version)
            .then_with(|| left.version.cmp(&right.version))
    });
    matches.into_iter().next()
}

async fn resolve_install_request_for_extension_id(
    state: &AppState,
    extension_id: &str,
) -> anyhow::Result<InstallRequest> {
    let catalog = build_catalog(state, false)
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
    let entry = pick_best_catalog_entry(&catalog.available, extension_id).ok_or_else(|| {
        anyhow::anyhow!(
            "extension '{}' is not available in the catalog",
            extension_id
        )
    })?;
    if let Some(package_path) = entry
        .package_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(InstallRequest {
            download_url: None,
            package_path: Some(package_path.to_string()),
        });
    }
    if !entry.download_url.trim().is_empty() {
        return Ok(InstallRequest {
            download_url: Some(entry.download_url),
            package_path: None,
        });
    }
    anyhow::bail!(
        "extension '{}' catalog entry has no install source",
        extension_id
    );
}

async fn ensure_execution_blueprint_packages_installed(
    state: &AppState,
    manifest: &ExtensionManifest,
) -> anyhow::Result<()> {
    let Some(execution) = manifest.execution.as_ref() else {
        return Ok(());
    };
    let store = ExtensionStore::new(&state.db_pool);
    for extension_id in &execution.packages {
        let desired_extension_id = extension_id.trim();
        if desired_extension_id.is_empty() {
            continue;
        }
        if let Some(existing) = store.get_extension(desired_extension_id).await? {
            if !existing.enabled {
                store
                    .set_extension_enabled(desired_extension_id, true)
                    .await?;
            }
            continue;
        }

        let request = resolve_install_request_for_extension_id(state, desired_extension_id).await?;
        install_extension_internal_with_policy(state, &request, InstallPolicy::default()).await?;
    }
    Ok(())
}

pub async fn get_extension(
    State(state): State<AppState>,
    Path(extension_id): Path<String>,
) -> ApiResult<Json<Extension>> {
    let store = ExtensionStore::new(&state.db_pool);
    let extension = store
        .get_extension(&extension_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("extension not found"))?;
    Ok(Json(extension))
}

pub async fn get_extension_control_surface(
    State(state): State<AppState>,
    Path(extension_id): Path<String>,
) -> ApiResult<Json<ExtensionControlSurface>> {
    let store = ExtensionStore::new(&state.db_pool);
    let surface = build_extension_control_surface(&state, &store, &extension_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(surface))
}

pub async fn update_extension_control_surface_settings(
    State(state): State<AppState>,
    Path(extension_id): Path<String>,
    Json(payload): Json<UpdateExtensionControlSettingsRequest>,
) -> ApiResult<Json<ExtensionControlSurface>> {
    let store = ExtensionStore::new(&state.db_pool);
    if !payload.values.is_empty() {
        let context = load_extension_control_context(&state, &store, &extension_id)
            .await
            .map_err(ApiError::from)?;
        control::update_settings(&state, &store, &context, &payload.values)
            .await
            .map_err(ApiError::from)?;
    }
    let surface = build_extension_control_surface(&state, &store, &extension_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(surface))
}

pub async fn run_extension_control_action(
    State(state): State<AppState>,
    Path((extension_id, action_id)): Path<(String, String)>,
    payload: Option<Json<RunExtensionControlActionRequest>>,
) -> ApiResult<Json<ExtensionControlActionResponse>> {
    let params = payload
        .as_ref()
        .map(|value| value.params.clone())
        .unwrap_or_default();
    let store = ExtensionStore::new(&state.db_pool);
    let message =
        execute_extension_control_action(&state, &store, &extension_id, &action_id, &params)
            .await
            .map_err(ApiError::from)?;
    let surface = build_extension_control_surface(&state, &store, &extension_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ExtensionControlActionResponse {
        success: true,
        message,
        control_surface: surface,
    }))
}

pub async fn start_extension_ui_session(
    State(state): State<AppState>,
    Path(instance_id): Path<Uuid>,
    user: CurrentUser,
) -> ApiResult<Response> {
    let store = ExtensionStore::new(&state.db_pool);
    let instance = store
        .get_instance(instance_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("extension instance not found"))?;
    if !instance.enabled {
        return Err(ApiError::conflict(
            "the selected extension instance is not enabled",
        ));
    }

    let (token, _) = state
        .auth_service
        .sign_access_token_with_ttl_minutes(
            user.user_id,
            user.session_id,
            EXTENSION_UI_TOKEN_TTL_MINUTES,
        )
        .map_err(ApiError::from)?;
    let proxy_prefix = extension_ui_proxy_prefix(instance_id);
    let cookie = format!("elixir_ui_token={token}; Path={proxy_prefix}; HttpOnly; SameSite=Lax");
    let bootstrap_html = build_extension_ui_start_html(&proxy_prefix);
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(axum_header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(axum_header::CACHE_CONTROL, "no-cache, no-store")
        .body(axum::body::Body::from(bootstrap_html))
        .map_err(|err| ApiError::internal(err.to_string()))?;
    let cookie_header =
        AxumHeaderValue::from_str(&cookie).map_err(|err| ApiError::internal(err.to_string()))?;
    response
        .headers_mut()
        .append(axum_header::SET_COOKIE, cookie_header);
    Ok(response)
}

fn build_extension_ui_start_html(proxy_prefix: &str) -> String {
    let proxy_prefix_json =
        serde_json::to_string(proxy_prefix).unwrap_or_else(|_| "\"/\"".to_string());
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <title>Opening extension UI…</title>
</head>
<body>
  <p>Opening extension UI…</p>
  <script>
    (function() {{
      const proxyPrefix = {proxy_prefix_json};
      const scopePrefix = proxyPrefix.endsWith("/") ? proxyPrefix : proxyPrefix + "/";
      const targetUrl = scopePrefix;

      function scopeCookiePaths() {{
        const paths = ["/"];
        let current = scopePrefix.endsWith("/") && scopePrefix.length > 1
          ? scopePrefix.slice(0, -1)
          : scopePrefix;
        while (current && current !== "/") {{
          paths.push(current);
          const nextIndex = current.lastIndexOf("/");
          current = nextIndex > 0 ? current.slice(0, nextIndex) : "/";
        }}
        if (scopePrefix !== "/") {{
          paths.push(scopePrefix);
        }}
        return Array.from(new Set(paths.filter(Boolean)));
      }}

      async function clearScopedIndexedDb() {{
        try {{
          if (!("indexedDB" in window) || typeof indexedDB.databases !== "function") {{
            return;
          }}
          const databases = await indexedDB.databases();
          await Promise.all(databases
            .map((database) => database && database.name)
            .filter(Boolean)
            .map((name) => new Promise((resolve) => {{
              const request = indexedDB.deleteDatabase(name);
              request.onsuccess = () => resolve();
              request.onerror = () => resolve();
              request.onblocked = () => resolve();
            }})));
        }} catch (_error) {{}}
      }}

      function clearScopedCookies() {{
        try {{
          const cookiePairs = String(document.cookie || "")
            .split(";")
            .map((entry) => entry.trim())
            .filter(Boolean)
            .map((entry) => entry.split("=")[0])
            .filter(Boolean);
          const cookiePaths = scopeCookiePaths();
          const expired = "Thu, 01 Jan 1970 00:00:00 GMT";
          cookiePairs.forEach((name) => {{
            cookiePaths.forEach((path) => {{
              document.cookie = name + "=; expires=" + expired + "; path=" + path + "; SameSite=Lax";
            }});
          }});
        }} catch (_error) {{}}
      }}

      async function clearScopedBrowserState() {{
        try {{
          if ("serviceWorker" in navigator) {{
            const registrations = await navigator.serviceWorker.getRegistrations();
            await Promise.all(registrations
              .filter((registration) => String(registration.scope || "").indexOf(scopePrefix) >= 0)
              .map((registration) => registration.unregister()));
          }}
        }} catch (_error) {{}}

        try {{
          if ("caches" in window) {{
            const keys = await caches.keys();
            await Promise.all(keys.map((key) => caches.delete(key)));
          }}
        }} catch (_error) {{}}

        try {{
          if ("localStorage" in window) {{
            localStorage.clear();
          }}
        }} catch (_error) {{}}

        try {{
          if ("sessionStorage" in window) {{
            sessionStorage.clear();
          }}
        }} catch (_error) {{}}

        clearScopedCookies();
        await clearScopedIndexedDb();
      }}

      clearScopedBrowserState().finally(function() {{
        window.location.replace(targetUrl);
      }});
    }})();
  </script>
  <noscript>
    <meta http-equiv="refresh" content="0; url={proxy_prefix}">
  </noscript>
</body>
</html>
"#
    )
}

pub async fn proxy_extension_ui_root(
    State(state): State<AppState>,
    Path(instance_id): Path<Uuid>,
    user: CurrentUser,
    method: Method,
    headers: AxumHeaderMap,
    original_uri: OriginalUri,
    body: Bytes,
) -> ApiResult<Response> {
    if !original_uri.0.path().ends_with('/') {
        let redirect_location = match original_uri.0.query() {
            Some(query) if !query.is_empty() => format!("{}/?{}", original_uri.0.path(), query),
            _ => format!("{}/", original_uri.0.path()),
        };
        return Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header(axum_header::LOCATION, redirect_location)
            .header(axum_header::CACHE_CONTROL, "no-cache, no-store")
            .body(axum::body::Body::empty())
            .map_err(|err| ApiError::internal(err.to_string()));
    }

    proxy_extension_ui_impl(
        &state,
        user,
        instance_id,
        String::new(),
        method,
        headers,
        original_uri.0,
        body,
    )
    .await
}

pub async fn proxy_extension_ui(
    State(state): State<AppState>,
    Path((instance_id, path)): Path<(Uuid, String)>,
    user: CurrentUser,
    method: Method,
    headers: AxumHeaderMap,
    original_uri: OriginalUri,
    body: Bytes,
) -> ApiResult<Response> {
    proxy_extension_ui_impl(
        &state,
        user,
        instance_id,
        path,
        method,
        headers,
        original_uri.0,
        body,
    )
    .await
}

pub async fn install_extension(
    State(state): State<AppState>,
    Json(payload): Json<InstallRequest>,
) -> ApiResult<Json<InstallResponse>> {
    let result = install_extension_internal(&state, &payload)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(result.response))
}

async fn install_extension_internal(
    state: &AppState,
    payload: &InstallRequest,
) -> anyhow::Result<InstallResult> {
    install_extension_internal_with_policy(state, payload, InstallPolicy::default()).await
}

pub(crate) async fn install_internal_extension_from_dir(
    state: &AppState,
    package_dir: &std::path::Path,
    policy: InstallPolicy,
) -> anyhow::Result<()> {
    let request = InstallRequest {
        download_url: None,
        package_path: Some(package_dir.to_string_lossy().to_string()),
    };
    install_extension_internal_with_policy(state, &request, policy).await?;
    Ok(())
}

async fn install_extension_internal_with_policy(
    state: &AppState,
    payload: &InstallRequest,
    policy: InstallPolicy,
) -> anyhow::Result<InstallResult> {
    let storage_paths = ExtensionStoragePaths::new(&state.settings.extensions.storage_root);
    storage_paths.ensure_dirs().await?;

    let is_dev = state.settings.environment == RunEnvironment::Development;
    let allow_unsigned = is_dev && state.settings.extensions.allow_unsigned;
    let allow_directory_install = is_dev && state.settings.extensions.allow_directory_install;

    let package_path = match (&payload.download_url, &payload.package_path) {
        (Some(_), Some(_)) => {
            anyhow::bail!("provide download_url or package_path, not both");
        }
        (Some(url), None) => download_package(url, &storage_paths.packages_dir).await?,
        (None, Some(path)) => PathBuf::from(path),
        (None, None) => {
            anyhow::bail!("download_url or package_path is required");
        }
    };

    if !package_path.exists() {
        anyhow::bail!("package path does not exist");
    }

    let bundled_dir = PathBuf::from(&state.settings.extensions.bundled_dir);
    let is_bundled_source = path_within(&package_path, &bundled_dir);

    let staging_dir = storage_paths.tmp_dir.join(Uuid::new_v4().to_string());
    let mut package_hash = None;
    let staged = if package_path.is_dir() {
        if !allow_directory_install
            && !is_bundled_source
            && !policy.allow_internal_directory_install
        {
            anyhow::bail!(
                "directory installs are only allowed in development with extensions.allow_directory_install=true"
            );
        }
        if !allow_unsigned && !is_bundled_source && !policy.allow_internal_unsigned {
            anyhow::bail!(
                "unsigned installs are disabled; enable extensions.allow_unsigned for development"
            );
        }
        copy_dir_recursive(&package_path, &staging_dir)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        staging_dir.clone()
    } else if package_path.is_file() {
        let hash = compute_sha256(&package_path).await?;
        package_hash = Some(hash);
        unpack_package(&package_path, &staging_dir)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?
    } else {
        anyhow::bail!("package path is not a file or directory");
    };

    let PackageManifest {
        mut manifest,
        mut raw_json,
        ..
    } = read_manifest_from_dir(&staged)
        .await
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    if repair_builtin_manifest_json(&mut raw_json) {
        manifest = serde_json::from_value(raw_json.clone())
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        manifest
            .validate()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        write_manifest_to_dir(&staged, &raw_json)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    }

    let registry_entry = if package_hash.is_some() {
        fetch_registry_entry(
            &state.settings.extensions.registries,
            payload.download_url.as_deref(),
            &manifest,
        )
        .await
    } else {
        None
    };
    if let (Some(entry), Some(hash)) = (&registry_entry, package_hash.as_deref()) {
        if entry.id != manifest.id || entry.version != manifest.version {
            anyhow::bail!("manifest id/version does not match registry entry");
        }
        if let Some(expected_hash) = entry.sha256.as_deref() {
            if !expected_hash.trim().eq_ignore_ascii_case(hash) {
                anyhow::bail!("package hash does not match registry");
            }
        }
        if let (Some(reg_key), Some(manifest_key)) = (
            entry.publisher_key_id.as_deref(),
            manifest
                .publisher
                .as_ref()
                .and_then(|publisher| publisher.key_id.as_deref()),
        ) {
            if !reg_key.trim().eq_ignore_ascii_case(manifest_key.trim()) {
                anyhow::bail!("publisher key mismatch between manifest and registry");
            }
        }
    }

    let mut publisher_key_id = registry_entry
        .as_ref()
        .and_then(|entry| entry.publisher_key_id.as_deref())
        .or_else(|| {
            manifest
                .publisher
                .as_ref()
                .and_then(|publisher| publisher.key_id.as_deref())
        });

    if let Some(hash) = package_hash.as_deref() {
        let package_signature = read_package_signature(&staged)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        let signature = registry_entry
            .as_ref()
            .and_then(|entry| entry.signature.as_deref())
            .or(package_signature.as_deref());
        let has_material = signature.is_some() || publisher_key_id.is_some();
        if has_material {
            if is_bundled_source && signature.is_none() {
                tracing::debug!(
                    extension_id = %manifest.id,
                    "allowing bundled package without signature material"
                );
            } else {
                verify_signature(hash, signature, publisher_key_id)?;
            }
        } else if !allow_unsigned && !is_bundled_source {
            anyhow::bail!("package signature is required");
        }
    }

    let trust_level = registry_entry
        .as_ref()
        .and_then(|entry| entry.trust)
        .or(manifest.trust)
        .unwrap_or(ExtensionTrustLevel::Community);
    let publisher = manifest.publisher.clone();
    let publisher_name = publisher.as_ref().map(|publisher| publisher.name.clone());
    let signing_key_id = publisher_key_id.take().map(|value| value.to_string());

    let permission_policy = PermissionPolicy::new();
    permission_policy.enforce(trust_level, &manifest.permissions, &manifest.id)?;

    let new_version = Version::parse(&manifest.version)
        .map_err(|_| anyhow::anyhow!("extension version is not valid semver"))?;
    let store = ExtensionStore::new(&state.db_pool);
    if let Some(existing) = store.get_extension(&manifest.id).await? {
        validate_semver_upgrade(&existing, &new_version, package_hash.as_deref(), policy)?;
    }
    let required = required_secrets_from_manifest(&manifest)?;
    if !required.is_empty() {
        let instances = store.list_instances(Some(&manifest.id)).await?;
        let instance_ids: Vec<_> = instances
            .into_iter()
            .filter(|instance| instance.enabled)
            .map(|instance| instance.instance_id)
            .collect();
        let missing =
            missing_required_secrets_for_instances(&store, &instance_ids, &required).await?;
        if !missing.is_empty() {
            anyhow::bail!("missing required secrets: {}", missing.join(", "));
        }
    }

    let extension_id = manifest.id.clone();
    let name = manifest.name.clone();
    let version = manifest.version.clone();
    let kind = manifest.kind.clone();

    let extension_root = storage_paths.unpacked_dir.join(&extension_id);
    fs::create_dir_all(&extension_root)
        .await
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    let unpacked_dir = extension_root.join(&version);
    if unpacked_dir.exists() {
        fs::remove_dir_all(&unpacked_dir)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    }
    fs::rename(&staged, &unpacked_dir)
        .await
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.clone(),
            name: name.clone(),
            version: version.clone(),
            kind: kind.clone(),
            publisher_name,
            signing_key_id,
            trust_level,
            manifest_json: raw_json,
            package_hash,
            enabled: true,
        })
        .await?;

    if ensure_default_extension_instance(&store, &manifest, false).await? {
        trigger_extensions_reconcile(state, "extension install default instance create").await;
    }

    Ok(InstallResult {
        response: InstallResponse {
            extension_id,
            name,
            version,
            kind,
            trust_level,
            enabled: true,
        },
    })
}

pub async fn enable_extension(
    State(state): State<AppState>,
    Path(extension_id): Path<String>,
) -> ApiResult<Json<Extension>> {
    let store = ExtensionStore::new(&state.db_pool);
    let extension = store
        .get_extension(&extension_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("extension not found"))?;
    let manifest: crate::extensions::manifest::ExtensionManifest =
        serde_json::from_value(extension.manifest_json.clone())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if let Err(err) = manifest.validate() {
        return Err(ApiError::bad_request(err.to_string()));
    }
    let policy = PermissionPolicy::new();
    policy
        .enforce(
            extension.trust_level,
            &manifest.permissions,
            &extension.extension_id,
        )
        .map_err(|err| ApiError::forbidden(err.to_string()))?;
    let required = required_secrets_from_manifest(&manifest)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if !required.is_empty() {
        let instances = store
            .list_instances(Some(&extension.extension_id))
            .await
            .map_err(ApiError::from)?;
        let instance_ids: Vec<_> = instances
            .into_iter()
            .filter(|instance| instance.enabled)
            .map(|instance| instance.instance_id)
            .collect();
        let missing = missing_required_secrets_for_instances(&store, &instance_ids, &required)
            .await
            .map_err(ApiError::from)?;
        if !missing.is_empty() {
            return Err(ApiError::bad_request(format!(
                "missing required secrets: {}",
                missing.join(", ")
            )));
        }
    }

    store
        .set_extension_enabled(&extension_id, true)
        .await
        .map_err(ApiError::from)?;
    let extension = store
        .get_extension(&extension_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("extension not found"))?;
    Ok(Json(extension))
}

pub async fn disable_extension(
    State(state): State<AppState>,
    Path(extension_id): Path<String>,
) -> ApiResult<Json<Extension>> {
    let store = ExtensionStore::new(&state.db_pool);
    store
        .set_extension_enabled(&extension_id, false)
        .await
        .map_err(ApiError::from)?;
    let extension = store
        .get_extension(&extension_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("extension not found"))?;
    Ok(Json(extension))
}

pub async fn uninstall_extension(
    State(state): State<AppState>,
    Path(extension_id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    if state
        .settings
        .extensions
        .core_extensions
        .iter()
        .any(|id| id == &extension_id)
    {
        return Err(ApiError::forbidden("core extensions cannot be uninstalled"));
    }
    let store = ExtensionStore::new(&state.db_pool);
    let existing = store
        .get_extension(&extension_id)
        .await
        .map_err(ApiError::from)?;
    let Some(existing) = existing else {
        return Err(ApiError::not_found("extension not found"));
    };
    let storage_paths = ExtensionStoragePaths::new(&state.settings.extensions.storage_root);
    let mut deleted_extensions = vec![extension_id.clone()];

    let mut cascade_targets: Vec<Extension> = Vec::new();
    if existing.kind == ExtensionKind::Blueprint {
        if let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(existing.manifest_json.clone())
        {
            let installed = store.list_extensions().await.map_err(ApiError::from)?;
            let installed_index: HashMap<String, Extension> = installed
                .iter()
                .cloned()
                .map(|item| (item.extension_id.clone(), item))
                .collect();
            let dependencies = blueprint_dependency_ids(&manifest, &extension_id);
            for dependency_id in dependencies {
                if state
                    .settings
                    .extensions
                    .core_extensions
                    .iter()
                    .any(|core| core == &dependency_id)
                {
                    continue;
                }
                if dependency_referenced_by_other_blueprints(
                    &installed,
                    &extension_id,
                    &dependency_id,
                ) {
                    continue;
                }
                if let Some(item) = installed_index.get(&dependency_id) {
                    cascade_targets.push(item.clone());
                }
            }
        } else {
            tracing::warn!(
                "failed to parse blueprint manifest during uninstall cascade: {}",
                extension_id
            );
        }
        let _ = store
            .delete_desired_blueprints_by_extension(&extension_id)
            .await
            .map_err(ApiError::from)?;
    }

    cleanup_extension_downstream_state(&state, &store, &existing)
        .await
        .map_err(ApiError::from)?;

    remove_extension_instances(&state, &store, &existing.extension_id)
        .await
        .map_err(ApiError::from)?;

    uninstall_extension_record(&store, &storage_paths, &existing)
        .await
        .map_err(ApiError::from)?;

    for dependency in cascade_targets {
        cleanup_extension_downstream_state(&state, &store, &dependency)
            .await
            .map_err(ApiError::from)?;
        remove_extension_instances(&state, &store, &dependency.extension_id)
            .await
            .map_err(ApiError::from)?;
        uninstall_extension_record(&store, &storage_paths, &dependency)
            .await
            .map_err(ApiError::from)?;
        deleted_extensions.push(dependency.extension_id);
    }

    Ok(Json(serde_json::json!({
        "status": "deleted",
        "deletedExtensions": deleted_extensions
    })))
}

pub async fn list_instances(
    State(state): State<AppState>,
    Query(query): Query<InstancesQuery>,
) -> ApiResult<Json<Vec<ExtensionInstance>>> {
    let store = ExtensionStore::new(&state.db_pool);
    let instances = store
        .list_instances(query.extension_id.as_deref())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(instances))
}

pub async fn create_instance(
    State(state): State<AppState>,
    Path(extension_id): Path<String>,
    Json(payload): Json<CreateInstanceRequest>,
) -> ApiResult<Json<ExtensionInstance>> {
    let store = ExtensionStore::new(&state.db_pool);
    let extension = store
        .get_extension(&extension_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("extension not found"))?;
    if extension.kind == ExtensionKind::Blueprint {
        return Err(ApiError::bad_request(
            "blueprint extensions cannot create instances",
        ));
    }

    let instance_id = Uuid::new_v4();
    let instance_name = payload
        .instance_name
        .unwrap_or_else(|| "default".to_string());
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id,
            instance_name,
            config_json: payload.config,
            enabled: true,
        })
        .await
        .map_err(|err| map_unique_violation(err, "instance already exists"))?;

    let instance = store
        .get_instance(instance_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::internal("instance lookup failed"))?;
    trigger_extensions_reconcile(&state, "manual extension instance create").await;
    Ok(Json(instance))
}

pub(super) async fn create_default_extension_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    manifest: &ExtensionManifest,
) -> anyhow::Result<bool> {
    let created = ensure_default_extension_instance(store, manifest, true).await?;
    if created {
        trigger_extensions_reconcile(state, "manual default instance create").await;
    }
    Ok(created)
}

async fn ensure_default_extension_instance(
    store: &ExtensionStore<'_>,
    manifest: &ExtensionManifest,
    allow_missing_manual_secrets: bool,
) -> anyhow::Result<bool> {
    if manifest.kind != ExtensionKind::Module || manifest.runtime.is_none() {
        return Ok(false);
    }

    let instances = store.list_instances(Some(&manifest.id)).await?;
    if !instances.is_empty() {
        return Ok(false);
    }

    if !allow_missing_manual_secrets {
        let required = required_secrets_from_manifest(manifest)?;
        if !required.is_empty() {
            let probe_instance_id = Uuid::new_v4();
            let missing = filter_auto_managed_runtime_missing(
                &manifest.id,
                missing_required_secrets_for_instance(store, probe_instance_id, &required).await?,
            );
            if !missing.is_empty() {
                return Ok(false);
            }
        }
    }

    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: manifest.id.clone(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;

    Ok(true)
}

async fn trigger_extensions_reconcile(state: &AppState, reason: &str) {
    let config = ReconcileConfig::from_settings(&state.settings);
    if let Err(err) = state.orchestrator.reconcile_once(&config).await {
        tracing::warn!(reason = reason, "extension reconcile trigger failed: {err}");
    }
}

pub async fn update_instance(
    State(state): State<AppState>,
    Path(instance_id): Path<String>,
    Json(payload): Json<UpdateInstanceRequest>,
) -> ApiResult<Json<ExtensionInstance>> {
    let instance_id =
        Uuid::parse_str(&instance_id).map_err(|_| ApiError::bad_request("invalid instance id"))?;
    let store = ExtensionStore::new(&state.db_pool);

    if let Some(name) = payload.instance_name {
        store
            .rename_instance(instance_id, &name)
            .await
            .map_err(|err| map_unique_violation(err, "instance name already exists"))?;
    }
    if let Some(config) = payload.config {
        store
            .update_instance_config(instance_id, Some(&config))
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(enabled) = payload.enabled {
        if enabled {
            let instance = store
                .get_instance(instance_id)
                .await
                .map_err(ApiError::from)?
                .ok_or_else(|| ApiError::not_found("instance not found"))?;
            let extension = store
                .get_extension(&instance.extension_id)
                .await
                .map_err(ApiError::from)?
                .ok_or_else(|| ApiError::not_found("extension not found"))?;
            let manifest: crate::extensions::manifest::ExtensionManifest =
                serde_json::from_value(extension.manifest_json.clone())
                    .map_err(|err| ApiError::bad_request(err.to_string()))?;
            if let Err(err) = manifest.validate() {
                return Err(ApiError::bad_request(err.to_string()));
            }
            let required = required_secrets_from_manifest(&manifest)
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
            if !required.is_empty() {
                let missing = missing_required_secrets_for_instance(&store, instance_id, &required)
                    .await
                    .map_err(ApiError::from)?;
                if !missing.is_empty() {
                    return Err(ApiError::bad_request(format!(
                        "missing required secrets: {}",
                        missing.join(", ")
                    )));
                }
            }
        }
        store
            .set_instance_enabled(instance_id, enabled)
            .await
            .map_err(ApiError::from)?;
    }

    let instance = store
        .get_instance(instance_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("instance not found"))?;
    Ok(Json(instance))
}

pub async fn delete_instance(
    State(state): State<AppState>,
    Path(instance_id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let instance_id =
        Uuid::parse_str(&instance_id).map_err(|_| ApiError::bad_request("invalid instance id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let instance = store
        .get_instance(instance_id)
        .await
        .map_err(ApiError::from)?;
    if instance.is_none() {
        return Err(ApiError::not_found("instance not found"));
    }

    remove_instance_record(&state, &store, instance_id)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(serde_json::json!({ "status": "deleted" })))
}

pub async fn rollback_instance(
    State(state): State<AppState>,
    Path(instance_id): Path<String>,
) -> ApiResult<Json<Plan>> {
    let instance_id =
        Uuid::parse_str(&instance_id).map_err(|_| ApiError::bad_request("invalid instance id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let instance = store
        .get_instance(instance_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("instance not found"))?;

    let blueprint_id = format!(
        "rollback:{}:{}",
        instance.extension_id, instance.instance_name
    );
    let mut plan = Plan::new(blueprint_id, None);
    plan.actions
        .push(PlanAction::RollbackRuntime { instance_id });

    if instance.rollback_version.is_none() {
        plan.conflicts.push(serde_json::json!({
            "code": "rollback_unavailable",
            "extension_id": instance.extension_id,
            "instance_id": instance.instance_id,
            "instance_name": instance.instance_name,
            "detail": "no rollback version recorded for instance"
        }));
    }

    let plan_json =
        serde_json::to_value(&plan).map_err(|err| ApiError::internal(err.to_string()))?;
    store
        .create_run(&NewOrchestratorRun {
            run_id: plan.plan_id,
            source: "rollback".to_string(),
            status: OrchestratorRunStatus::Pending,
            phase: Some("planned".to_string()),
            plan_json: Some(plan_json),
            error: None,
        })
        .await
        .map_err(ApiError::from)?;

    Ok(Json(plan))
}

pub async fn graph(State(state): State<AppState>) -> ApiResult<Json<GraphResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = store.list_providers(None).await.map_err(ApiError::from)?;
    let bindings = store.list_bindings().await.map_err(ApiError::from)?;
    Ok(Json(GraphResponse {
        providers,
        bindings,
    }))
}

pub async fn provider_health(
    State(state): State<AppState>,
    Path(provider_id): Path<String>,
) -> ApiResult<Json<ProviderHealthResponse>> {
    let provider_id =
        Uuid::parse_str(&provider_id).map_err(|_| ApiError::bad_request("invalid provider id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let provider = store
        .get_provider(provider_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("provider not found"))?;
    let readiness = store
        .get_provider_readiness(provider_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ProviderHealthResponse {
        provider_id,
        health_state: provider.health_state,
        readiness_phase: readiness
            .as_ref()
            .map(|value| value.readiness_phase)
            .unwrap_or(ProviderReadinessPhase::Unknown),
        readiness_detail: readiness.and_then(|value| value.readiness_detail),
        last_healthcheck_at: provider.last_healthcheck_at,
    }))
}

#[derive(Debug, Deserialize)]
pub struct RuntimeLogsQuery {
    pub since: Option<String>,
}

pub async fn runtime_logs(
    State(state): State<AppState>,
    Path(instance_id): Path<String>,
    Query(query): Query<RuntimeLogsQuery>,
) -> ApiResult<Json<RuntimeLogsResponse>> {
    let instance_id =
        Uuid::parse_str(&instance_id).map_err(|_| ApiError::bad_request("invalid instance id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let mut logs = store
        .list_runtime_logs(instance_id)
        .await
        .map_err(ApiError::from)?;
    if let Some(since) = query.since {
        let since = chrono::DateTime::parse_from_rfc3339(&since)
            .map_err(|_| ApiError::bad_request("invalid since timestamp"))?
            .with_timezone(&chrono::Utc);
        logs.retain(|log| log.created_at >= since);
    }
    Ok(Json(RuntimeLogsResponse { instance_id, logs }))
}

pub async fn list_secrets(
    State(state): State<AppState>,
    Query(query): Query<SecretsQuery>,
) -> ApiResult<Json<Vec<SecretResponse>>> {
    let store = ExtensionStore::new(&state.db_pool);

    let scope = match query.scope.as_deref() {
        Some(value) => Some(parse_secret_scope(value)?),
        None => None,
    };
    if scope.is_none() && query.scope_id.is_some() {
        return Err(ApiError::bad_request(
            "scope is required when scope_id is provided",
        ));
    }
    let scope_id = match (scope, query.scope_id.as_deref()) {
        (Some(scope), Some(raw)) => parse_scope_id(scope, Some(raw), false)?,
        (Some(scope), None) => parse_scope_id(scope, None, false)?,
        (None, None) => None,
        (None, Some(_)) => None,
    };

    let secrets = store
        .list_secrets(scope, scope_id, query.key.as_deref())
        .await
        .map_err(ApiError::from)?;
    let response = secrets.into_iter().map(secret_response).collect();
    Ok(Json(response))
}

pub async fn get_secret(
    State(state): State<AppState>,
    Path(secret_id): Path<String>,
) -> ApiResult<Json<SecretResponse>> {
    let secret_id =
        Uuid::parse_str(&secret_id).map_err(|_| ApiError::bad_request("invalid secret id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let secret = store
        .get_secret_by_id(secret_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("secret not found"))?;
    Ok(Json(secret_response(secret)))
}

pub async fn create_secret(
    State(state): State<AppState>,
    Json(payload): Json<CreateSecretRequest>,
) -> ApiResult<Json<SecretResponse>> {
    if payload.key.trim().is_empty() {
        return Err(ApiError::bad_request("secret key is required"));
    }
    let (scope, scope_id) = parse_scope_and_id(&payload.scope, payload.scope_id.as_deref(), true)?;

    let store = ExtensionStore::new(&state.db_pool);
    if store
        .get_secret(scope, scope_id, &payload.key)
        .await
        .map_err(ApiError::from)?
        .is_some()
    {
        return Err(ApiError::conflict("secret already exists"));
    }

    let encrypted = state
        .secrets
        .encrypt(&payload.value)
        .map_err(|err| ApiError::internal(err.to_string()))?;

    let secret_id = Uuid::new_v4();
    store
        .upsert_secret(&NewSecret {
            secret_id,
            scope,
            scope_id,
            key: payload.key,
            value_encrypted: encrypted,
            rotatable: payload.rotatable.unwrap_or(false),
        })
        .await
        .map_err(ApiError::from)?;

    let secret = store
        .get_secret_by_id(secret_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::internal("secret lookup failed"))?;
    Ok(Json(secret_response(secret)))
}

pub async fn update_secret(
    State(state): State<AppState>,
    Path(secret_id): Path<String>,
    Json(payload): Json<UpdateSecretRequest>,
) -> ApiResult<Json<SecretResponse>> {
    let secret_id =
        Uuid::parse_str(&secret_id).map_err(|_| ApiError::bad_request("invalid secret id"))?;
    if payload.value.is_none() && payload.rotatable.is_none() {
        return Err(ApiError::bad_request("value or rotatable is required"));
    }
    let store = ExtensionStore::new(&state.db_pool);
    let existing = store
        .get_secret_by_id(secret_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("secret not found"))?;

    let value_encrypted = if let Some(value) = payload.value.as_deref() {
        state
            .secrets
            .encrypt(value)
            .map_err(|err| ApiError::internal(err.to_string()))?
    } else {
        existing.value_encrypted.clone()
    };
    let rotatable = payload.rotatable.unwrap_or(existing.rotatable);

    store
        .update_secret(secret_id, &value_encrypted, rotatable)
        .await
        .map_err(ApiError::from)?;

    let updated = store
        .get_secret_by_id(secret_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::internal("secret lookup failed"))?;
    Ok(Json(secret_response(updated)))
}

pub async fn rotate_secret(
    State(state): State<AppState>,
    Path(secret_id): Path<String>,
    Json(payload): Json<RotateSecretRequest>,
) -> ApiResult<Json<RotateSecretResponse>> {
    let secret_id =
        Uuid::parse_str(&secret_id).map_err(|_| ApiError::bad_request("invalid secret id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let secret = store
        .get_secret_by_id(secret_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("secret not found"))?;
    if !secret.rotatable {
        return Err(ApiError::conflict("secret is not rotatable"));
    }

    let value = match payload.value {
        Some(value) => value,
        None => {
            let mut bytes = [0u8; 32];
            OsRng.fill_bytes(&mut bytes);
            general_purpose::STANDARD.encode(bytes)
        }
    };
    let encrypted = state
        .secrets
        .encrypt(&value)
        .map_err(|err| ApiError::internal(err.to_string()))?;

    store
        .update_secret(secret_id, &encrypted, secret.rotatable)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(RotateSecretResponse { secret_id, value }))
}

pub async fn delete_secret(
    State(state): State<AppState>,
    Path(secret_id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let secret_id =
        Uuid::parse_str(&secret_id).map_err(|_| ApiError::bad_request("invalid secret id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let exists = store
        .get_secret_by_id(secret_id)
        .await
        .map_err(ApiError::from)?
        .is_some();
    if !exists {
        return Err(ApiError::not_found("secret not found"));
    }
    store
        .delete_secret(secret_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(serde_json::json!({ "status": "deleted" })))
}

pub async fn reconcile_now(State(state): State<AppState>) -> ApiResult<Json<ReconcileRunResponse>> {
    let config = ReconcileConfig::from_settings(&state.settings);
    state
        .orchestrator
        .reconcile_once(&config)
        .await
        .map_err(ApiError::from)?;
    let store = ExtensionStore::new(&state.db_pool);
    let run = store
        .get_latest_run_by_phase("reconcile")
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ReconcileRunResponse { run }))
}

async fn run_managed_repair_once(state: &AppState) -> anyhow::Result<()> {
    let config = ReconcileConfig::explicit_repair_from_settings(&state.settings);
    state.orchestrator.reconcile_once(&config).await?;
    state
        .orchestrator
        .apply_builtin_downloader_profiles_now()
        .await?;
    Ok(())
}

async fn run_extension_control_managed_repair(state: &AppState) -> anyhow::Result<String> {
    run_managed_repair_once(state).await?;
    Ok("Ran explicit repair for Elixir-managed settings.".to_string())
}

async fn run_extension_control_targeted_managed_repair(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<String> {
    let Some(provider) = context.selected_provider.as_ref() else {
        return run_extension_control_managed_repair(state).await;
    };

    let extensions = store.list_extensions().await?;
    let mut actions = Vec::new();
    let mut patched_connectors = Vec::new();
    let mut readiness_gates_added = false;

    for extension in extensions {
        if !extension.enabled || extension.kind != ExtensionKind::Connector {
            continue;
        }

        let manifest: ExtensionManifest = serde_json::from_value(extension.manifest_json.clone())
            .with_context(|| {
            format!(
                "parsing manifest for connector '{}'",
                extension.extension_id
            )
        })?;

        for action in manifest.actions {
            if action.r#type != "driver_patch" {
                continue;
            }
            let Some(target) = action.target.as_ref() else {
                continue;
            };
            if target.capability != provider.capability || target.slot != provider.slot_id {
                continue;
            }
            let Some(patch) = action.patch.clone() else {
                continue;
            };

            if !readiness_gates_added {
                actions.push(ExecutorAction::TransportGate {
                    provider_id: provider.provider_id,
                    timeout_seconds: 30,
                });
                actions.push(ExecutorAction::BootstrapGate {
                    provider_id: provider.provider_id,
                    timeout_seconds: 30,
                });
                actions.push(ExecutorAction::HealthGate {
                    provider_id: provider.provider_id,
                    timeout_seconds: 30,
                });
                readiness_gates_added = true;
            }
            actions.push(ExecutorAction::ApplyDriverPatch {
                connector_extension_id: extension.extension_id.clone(),
                target_provider_id: provider.provider_id,
                patch,
            });
            patched_connectors.push(extension.extension_id.clone());
        }
    }

    if actions.is_empty() {
        return run_extension_control_managed_repair(state).await;
    }

    state.orchestrator.apply_actions(actions).await?;
    patched_connectors.sort();
    patched_connectors.dedup();

    Ok(format!(
        "Reapplied {} managed connector patch(es): {}.",
        patched_connectors.len(),
        patched_connectors.join(", ")
    ))
}

pub async fn reconcile_latest(
    State(state): State<AppState>,
) -> ApiResult<Json<ReconcileRunResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let run = store
        .get_latest_run_by_phase("reconcile")
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ReconcileRunResponse { run }))
}

pub async fn reset_runtime(State(state): State<AppState>) -> ApiResult<Json<RuntimeResetResponse>> {
    let config = ReconcileConfig::from_settings(&state.settings);
    let outcome = state
        .orchestrator
        .reset_elixir_runtime(&config)
        .await
        .map_err(ApiError::from)?;
    let store = ExtensionStore::new(&state.db_pool);
    let run = if outcome.reboot_recommended {
        None
    } else {
        store
            .get_latest_run_by_phase("reconcile")
            .await
            .map_err(ApiError::from)?
    };

    Ok(Json(RuntimeResetResponse {
        status: outcome.status,
        message: outcome.message,
        docker_restarted: outcome.docker_restarted,
        reboot_recommended: outcome.reboot_recommended,
        removed_containers: outcome.removed_containers,
        recreated_networks: outcome.recreated_networks,
        run,
    }))
}

pub async fn status_summary(
    State(state): State<AppState>,
) -> ApiResult<Json<ExtensionStatusSummaryResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let response = build_extension_status_summary(&state, &store)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

const DOWNLOADER_PROFILE_SETTING_KEY: &str = "downloader_profile";

pub async fn downloader_profile(
    State(state): State<AppState>,
) -> ApiResult<Json<DownloaderProfileResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let response = build_downloader_profile_response(
        &state,
        &store,
        state.settings.extensions.downloader_profile,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn update_downloader_profile(
    State(state): State<AppState>,
    Json(payload): Json<UpdateDownloaderProfileRequest>,
) -> ApiResult<Json<DownloaderProfileResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    if payload.profile == state.settings.extensions.downloader_profile {
        store
            .delete_extension_setting(DOWNLOADER_PROFILE_SETTING_KEY)
            .await
            .map_err(ApiError::from)?;
    } else {
        store
            .upsert_extension_setting(
                DOWNLOADER_PROFILE_SETTING_KEY,
                &serde_json::json!(payload.profile),
            )
            .await
            .map_err(ApiError::from)?;
    }
    state
        .orchestrator
        .apply_builtin_downloader_profiles_now()
        .await
        .map_err(ApiError::from)?;
    let response = build_downloader_profile_response(
        &state,
        &store,
        state.settings.extensions.downloader_profile,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(Json(response))
}

async fn build_downloader_profile_response(
    state: &AppState,
    store: &ExtensionStore<'_>,
    default_profile: DownloaderPerformanceProfile,
) -> anyhow::Result<DownloaderProfileResponse> {
    let override_record = store
        .get_extension_setting_record(DOWNLOADER_PROFILE_SETTING_KEY)
        .await?;
    let effective_profile = DownloaderPerformanceProfile::from_setting_value(
        override_record.as_ref().map(|record| &record.value_json),
        default_profile,
    );
    let instances = store.list_instances(None).await?;
    let providers = store.list_provider_details().await?;
    let readiness_by_provider: HashMap<Uuid, ProviderReadiness> = store
        .list_provider_readiness()
        .await?
        .into_iter()
        .map(|value| (value.provider_id, value))
        .collect();
    let instance_map: HashMap<Uuid, ExtensionInstance> = instances
        .into_iter()
        .map(|instance| (instance.instance_id, instance))
        .collect();

    let mut downloaders = Vec::new();
    for detail in providers {
        if detail.provider.capability != "downloader.torrent"
            && detail.provider.capability != "downloader.nzb"
        {
            continue;
        }
        let Some(instance) = instance_map.get(&detail.provider.instance_id) else {
            continue;
        };
        let applied_profile = applied_profile_for_provider(
            instance.config_json.as_ref(),
            &detail.provider.capability,
            detail.provider.implementation.as_deref(),
        );
        let sync_state = if applied_profile == Some(effective_profile) {
            "up_to_date"
        } else if applied_profile.is_some() {
            "pending_update"
        } else {
            "pending_bootstrap"
        };
        let mut state_summary = None;
        let mut status = None;
        let mut download_rate_bps = None;
        let mut upload_rate_bps = None;
        let mut active_items = None;
        let mut queued_items = None;
        let mut error_items = None;
        let mut post_process_items = None;
        let mut downloaded_bytes = None;
        let mut uploaded_bytes = None;
        let mut telemetry_status =
            load_downloader_telemetry_status(store, detail.provider.provider_id).await?;
        let mut telemetry_error = None;

        if should_fetch_live_downloader_state(detail.provider.health_state) {
            match state
                .orchestrator
                .read_provider_state(&detail.provider, instance)
                .await
            {
                Ok(snapshot) => {
                    state_summary = snapshot.summary;
                    if let Some(activity) = snapshot.activity {
                        status = activity.status;
                        download_rate_bps = activity.download_rate_bps;
                        upload_rate_bps = activity.upload_rate_bps;
                        active_items = activity.active_items;
                        queued_items = activity.queued_items;
                        error_items = activity.error_items;
                        post_process_items = activity.post_process_items;
                        downloaded_bytes = activity.downloaded_bytes;
                        uploaded_bytes = activity.uploaded_bytes;
                    }
                    telemetry_status =
                        record_downloader_telemetry_success(store, detail.provider.provider_id)
                            .await?;
                }
                Err(err) => {
                    telemetry_status =
                        record_downloader_telemetry_error(store, detail.provider.provider_id)
                            .await?;
                    telemetry_error = Some(err.to_string());
                }
            }
        }
        downloaders.push(DownloaderTelemetryItem {
            name: downloader_display_name(
                &detail.provider.capability,
                detail.provider.implementation.as_deref(),
            ),
            extension_id: detail.extension_id,
            instance_id: instance.instance_id,
            instance_name: instance.instance_name.clone(),
            capability: detail.provider.capability.clone(),
            implementation: detail.provider.implementation.clone(),
            health_state: detail.provider.health_state,
            readiness_phase: readiness_by_provider
                .get(&detail.provider.provider_id)
                .map(|value| value.readiness_phase)
                .unwrap_or(ProviderReadinessPhase::Unknown),
            readiness_detail: readiness_by_provider
                .get(&detail.provider.provider_id)
                .and_then(|value| value.readiness_detail.clone()),
            last_healthcheck_at: detail.provider.last_healthcheck_at,
            applied_profile,
            sync_state: sync_state.to_string(),
            state_summary,
            status,
            download_rate_bps,
            upload_rate_bps,
            active_items,
            queued_items,
            error_items,
            post_process_items,
            downloaded_bytes,
            uploaded_bytes,
            last_successful_sample_at: telemetry_status.last_successful_sample_at,
            last_error_at: telemetry_status.last_error_at,
            telemetry_error,
        });
    }
    downloaders.sort_by(|left, right| left.name.cmp(&right.name));
    let pending_update_count = downloaders
        .iter()
        .filter(|item| item.sync_state != "up_to_date")
        .count();

    Ok(DownloaderProfileResponse {
        profile: effective_profile,
        default_profile,
        source: if override_record.is_some() {
            "override".to_string()
        } else {
            "config".to_string()
        },
        updated_at: override_record.map(|record| record.updated_at),
        pending_update_count,
        profiles: vec![
            DownloaderProfileOption {
                id: DownloaderPerformanceProfile::Balanced,
                label: "Balanced",
                description: "Current default tuning. Strong throughput without pushing CPU, disk, or network limits as hard.",
            },
            DownloaderProfileOption {
                id: DownloaderPerformanceProfile::Aggressive,
                label: "Aggressive",
                description: "Higher concurrency and cache sizes for faster downloading on stronger hardware and networks.",
            },
        ],
        downloaders,
    })
}

async fn build_extension_status_summary(
    state: &AppState,
    store: &ExtensionStore<'_>,
) -> anyhow::Result<ExtensionStatusSummaryResponse> {
    let runtime_snapshot = state.orchestrator.docker_runtime_snapshot();
    let extensions = store.list_extensions().await?;
    ensure_auto_default_instances_for_installed_modules(state, store, &extensions).await?;
    let instances = store.list_instances(None).await?;
    let providers = store.list_providers(None).await?;
    let readiness_by_provider: HashMap<Uuid, ProviderReadiness> = store
        .list_provider_readiness()
        .await?
        .into_iter()
        .map(|value| (value.provider_id, value))
        .collect();
    let bindings = store.list_bindings().await?;
    let desired_blueprints = store.list_desired_blueprints(None).await?;

    let mut instances_by_extension: HashMap<String, Vec<ExtensionInstance>> = HashMap::new();
    for instance in instances {
        instances_by_extension
            .entry(instance.extension_id.clone())
            .or_default()
            .push(instance);
    }

    let mut providers_by_instance: HashMap<Uuid, Vec<Provider>> = HashMap::new();
    let mut provider_instances_by_target: HashMap<(String, String), Vec<Uuid>> = HashMap::new();
    let mut available_targets = HashSet::new();
    for provider in providers {
        available_targets.insert((provider.capability.clone(), provider.slot_id.clone()));
        provider_instances_by_target
            .entry((provider.capability.clone(), provider.slot_id.clone()))
            .or_default()
            .push(provider.instance_id);
        providers_by_instance
            .entry(provider.instance_id)
            .or_default()
            .push(provider);
    }

    let mut failed_bindings_by_consumer: HashMap<Uuid, usize> = HashMap::new();
    for binding in bindings {
        if binding.status == BindingStatus::Failed {
            *failed_bindings_by_consumer
                .entry(binding.consumer_provider_id)
                .or_insert(0) += 1;
        }
    }

    let mut pending_blueprints = HashSet::new();
    for blueprint in desired_blueprints {
        if !blueprint.applied {
            pending_blueprints.insert(blueprint.blueprint_extension_id);
        }
    }

    let extensions_by_id: HashMap<String, Extension> = extensions
        .iter()
        .cloned()
        .map(|extension| (extension.extension_id.clone(), extension))
        .collect();
    let mut manifests_by_id: HashMap<String, ExtensionManifest> = HashMap::new();
    let mut items = Vec::with_capacity(extensions.len());
    for extension in extensions {
        let manifest =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone()).ok();
        if let Some(manifest) = manifest.as_ref() {
            manifests_by_id.insert(extension.extension_id.clone(), manifest.clone());
        }
        let instances = instances_by_extension
            .get(&extension.extension_id)
            .cloned()
            .unwrap_or_default();
        let item = match extension.kind {
            ExtensionKind::Blueprint => {
                summarize_blueprint_extension(&extension, &pending_blueprints)
            }
            ExtensionKind::Connector => {
                summarize_connector_extension(
                    state,
                    store,
                    &extension,
                    manifest.as_ref(),
                    &available_targets,
                    &provider_instances_by_target,
                    &providers_by_instance,
                    &runtime_snapshot,
                )
                .await?
            }
            ExtensionKind::Module => {
                summarize_module_extension(
                    state,
                    store,
                    &extension,
                    manifest.as_ref(),
                    &instances,
                    &providers_by_instance,
                    &readiness_by_provider,
                    &failed_bindings_by_consumer,
                    &runtime_snapshot,
                )
                .await?
            }
        };
        items.push(item);
    }

    let item_summary_by_id: HashMap<String, ExtensionStatusSummaryItem> = items
        .iter()
        .cloned()
        .map(|item| (item.extension_id.clone(), item))
        .collect();
    for item in &mut items {
        let Some(extension) = extensions_by_id.get(&item.extension_id) else {
            continue;
        };
        if extension.kind != ExtensionKind::Blueprint {
            continue;
        }
        let Some(manifest) = manifests_by_id.get(&item.extension_id) else {
            continue;
        };
        if manifest.optional_addons.is_empty() {
            continue;
        }
        item.optional_addons = summarize_blueprint_optional_addons(
            store,
            manifest,
            &extensions_by_id,
            &item_summary_by_id,
            &provider_instances_by_target,
        )
        .await?;
    }

    items.sort_by(|left, right| {
        extension_status_sort_order(&left.severity)
            .cmp(&extension_status_sort_order(&right.severity))
            .then_with(|| {
                left.name
                    .to_ascii_lowercase()
                    .cmp(&right.name.to_ascii_lowercase())
            })
    });

    let needs_attention_count = items
        .iter()
        .filter(|item| item.severity == "attention")
        .count();

    Ok(ExtensionStatusSummaryResponse {
        needs_attention_count,
        docker_runtime: summarize_docker_runtime(&runtime_snapshot),
        items,
    })
}

async fn ensure_auto_default_instances_for_installed_modules(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extensions: &[Extension],
) -> anyhow::Result<bool> {
    let mut created = false;
    for extension in extensions {
        if !extension.enabled || extension.kind != ExtensionKind::Module {
            continue;
        }
        let manifest = match serde_json::from_value::<ExtensionManifest>(
            extension.manifest_json.clone(),
        ) {
            Ok(manifest) => manifest,
            Err(err) => {
                tracing::warn!(
                    extension_id = %extension.extension_id,
                    "skipping default instance auto-provisioning; manifest could not be parsed: {err}"
                );
                continue;
            }
        };
        if let Err(err) = manifest.validate() {
            tracing::warn!(
                extension_id = %extension.extension_id,
                "skipping default instance auto-provisioning; manifest is invalid: {err}"
            );
            continue;
        }
        if ensure_default_extension_instance(store, &manifest, false).await? {
            created = true;
        }
    }
    if created {
        trigger_extensions_reconcile(state, "status default instance auto-provision").await;
    }
    Ok(created)
}

fn summarize_docker_runtime(
    snapshot: &DockerRuntimeHealthSnapshot,
) -> Option<DockerRuntimeStatusSummary> {
    let has_host_warning = snapshot
        .host_warning
        .as_deref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    if snapshot.state == DockerRuntimeHealthState::Healthy
        && snapshot.quarantined_instances.is_empty()
        && !has_host_warning
        && !snapshot.reboot_recommended
    {
        return None;
    }

    let (state, severity, code, label, description) = if snapshot.reboot_recommended {
        (
            "reboot_required".to_string(),
            "attention".to_string(),
            snapshot
                .code
                .clone()
                .unwrap_or_else(|| "docker_runtime_reboot_recommended".to_string()),
            "Host reboot recommended".to_string(),
            snapshot.reason.clone().unwrap_or_else(|| {
                "Elixir exhausted its automatic Docker recovery budget. Reboot the computer, then relaunch Elixir."
                    .to_string()
            }),
        )
    } else {
        match snapshot.state {
            DockerRuntimeHealthState::Healthy => (
                "healthy".to_string(),
                "attention".to_string(),
                "docker_host_warning".to_string(),
                "Docker host warning".to_string(),
                snapshot
                    .host_warning
                    .clone()
                    .unwrap_or_else(|| "Docker host settings need attention.".to_string()),
            ),
            DockerRuntimeHealthState::Recovering => (
                "recovering".to_string(),
                "attention".to_string(),
                snapshot
                    .code
                    .clone()
                    .unwrap_or_else(|| "docker_runtime_recovering".to_string()),
                "Docker runtime recovering".to_string(),
                snapshot.reason.clone().unwrap_or_else(|| {
                    "Docker recovered recently. Elixir is restoring extension runtimes gradually."
                        .to_string()
                }),
            ),
            DockerRuntimeHealthState::Degraded => (
                "degraded".to_string(),
                "attention".to_string(),
                snapshot
                    .code
                    .clone()
                    .unwrap_or_else(|| "docker_runtime_unhealthy".to_string()),
                "Docker runtime unhealthy".to_string(),
                snapshot.reason.clone().unwrap_or_else(|| {
                    "Docker is unhealthy, so extension status is temporarily stale.".to_string()
                }),
            ),
        }
    };

    Some(DockerRuntimeStatusSummary {
        state,
        severity,
        code,
        label,
        description,
        reboot_recommended: snapshot.reboot_recommended,
        until: snapshot.until.clone(),
        host_warning: snapshot.host_warning.clone(),
        last_failure_code: snapshot.last_failure_code.clone(),
        last_failure_reason: snapshot.last_failure_reason.clone(),
        last_failure_at: snapshot.last_failure_at,
        last_reset_attempt_at: snapshot.last_reset_attempt_at,
        auto_reset_attempts_in_window: snapshot.auto_reset_attempts_in_window,
        quarantined_instances: snapshot
            .quarantined_instances
            .iter()
            .cloned()
            .map(|item| DockerRuntimeQuarantineSummary {
                instance_id: item.instance_id,
                extension_id: item.extension_id,
                extension_name: item.extension_name,
                instance_name: item.instance_name,
                reason: item.reason,
                until: item.until,
            })
            .collect(),
    })
}

fn summarize_blueprint_extension(
    extension: &Extension,
    pending_blueprints: &HashSet<String>,
) -> ExtensionStatusSummaryItem {
    if !extension.enabled {
        return disabled_extension_status(
            extension,
            "disabled",
            "Disabled",
            "This stack is installed but turned off.",
            "enable",
            "Enable",
        );
    }
    if pending_blueprints.contains(&extension.extension_id) {
        return attention_extension_status(
            extension,
            "pending_blueprint_setup",
            "Needs setup",
            "This stack is waiting for final setup before it is ready to use.",
            "finish_setup",
            "Finish setup",
        );
    }
    ready_extension_status(
        extension,
        "ready",
        "Ready",
        "This stack is installed and ready to use.",
        "open",
        "Open",
    )
}

async fn summarize_blueprint_optional_addons(
    store: &ExtensionStore<'_>,
    manifest: &ExtensionManifest,
    extensions_by_id: &HashMap<String, Extension>,
    item_summary_by_id: &HashMap<String, ExtensionStatusSummaryItem>,
    provider_instances_by_target: &HashMap<(String, String), Vec<Uuid>>,
) -> anyhow::Result<Vec<ExtensionOptionalAddonSummaryItem>> {
    let mut items = Vec::new();
    for addon in &manifest.optional_addons {
        let installed_extension = extensions_by_id.get(&addon.extension_id);
        let target_instance_id = addon
            .target
            .as_ref()
            .and_then(|target| resolve_addon_target_instance(provider_instances_by_target, target));
        let secret_keys: Vec<String> = addon
            .required_fields
            .iter()
            .map(|field| addon_secret_key(addon, field))
            .collect();

        let mut missing_secret_keys = Vec::new();
        if let Some(instance_id) = target_instance_id {
            for key in &secret_keys {
                let exists = store
                    .get_secret(SecretScope::Instance, Some(instance_id), key)
                    .await?
                    .is_some();
                if !exists {
                    missing_secret_keys.push(key.clone());
                }
            }
        } else {
            missing_secret_keys = secret_keys.clone();
        }

        let mut item = if let Some(summary) = item_summary_by_id.get(&addon.extension_id) {
            let installed_enabled = installed_extension
                .map(|value| value.enabled)
                .unwrap_or(false);
            ExtensionOptionalAddonSummaryItem {
                extension_id: addon.extension_id.clone(),
                title: addon.title.clone().unwrap_or_else(|| summary.name.clone()),
                description: if summary.description.trim().is_empty() {
                    addon
                        .description
                        .clone()
                        .unwrap_or_else(|| "Optional add-on".to_string())
                } else {
                    summary.description.clone()
                },
                severity: summary.severity.clone(),
                status_code: summary.status_code.clone(),
                label: if installed_enabled && summary.severity == "ready" {
                    "Active".to_string()
                } else if !installed_enabled {
                    "Not active".to_string()
                } else {
                    summary.label.clone()
                },
                action: if installed_enabled {
                    "open".to_string()
                } else {
                    "activate".to_string()
                },
                action_label: if installed_enabled {
                    "Open".to_string()
                } else {
                    "Activate".to_string()
                },
                required_fields: addon.required_fields.clone(),
                secret_keys: secret_keys.clone(),
                secret_scope_instance_id: target_instance_id,
            }
        } else {
            ExtensionOptionalAddonSummaryItem {
                extension_id: addon.extension_id.clone(),
                title: addon
                    .title
                    .clone()
                    .unwrap_or_else(|| addon.extension_id.clone()),
                description: addon
                    .description
                    .clone()
                    .unwrap_or_else(|| "Available when you want to add it.".to_string()),
                severity: "available".to_string(),
                status_code: "available".to_string(),
                label: "Available".to_string(),
                action: "activate".to_string(),
                action_label: "Activate".to_string(),
                required_fields: addon.required_fields.clone(),
                secret_keys: secret_keys.clone(),
                secret_scope_instance_id: target_instance_id,
            }
        };

        if !item.required_fields.is_empty()
            && item.action == "activate"
            && !missing_secret_keys.is_empty()
            && target_instance_id.is_some()
        {
            item.description = addon.description.clone().unwrap_or_else(|| {
                "You can add your account details when you activate this source.".to_string()
            });
        }

        items.push(item);
    }

    items.sort_by(|left, right| {
        left.title
            .to_ascii_lowercase()
            .cmp(&right.title.to_ascii_lowercase())
    });
    Ok(items)
}

fn resolve_addon_target_instance(
    provider_instances_by_target: &HashMap<(String, String), Vec<Uuid>>,
    target: &crate::extensions::manifest::ManifestCapabilityRef,
) -> Option<Uuid> {
    let instance_ids =
        provider_instances_by_target.get(&(target.capability.clone(), target.slot.clone()))?;
    let mut unique = instance_ids.clone();
    unique.sort();
    unique.dedup();
    if unique.len() == 1 {
        unique.first().copied()
    } else {
        None
    }
}

fn addon_secret_key(
    addon: &crate::extensions::manifest::ManifestOptionalAddon,
    field: &str,
) -> String {
    match addon.secret_key_prefix.as_deref() {
        Some(prefix) if !prefix.trim().is_empty() => {
            format!("{}.{}", prefix.trim(), field.trim())
        }
        _ => field.trim().to_string(),
    }
}

fn normalize_connector_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn missing_required_connector_targets(
    manifest: &ExtensionManifest,
    available_targets: &HashSet<(String, String)>,
) -> Vec<String> {
    manifest
        .requires
        .iter()
        .filter(|require| !require.optional)
        .filter(|require| {
            !available_targets.contains(&(require.capability.clone(), require.slot.clone()))
        })
        .map(|require| format!("{}/{}", require.capability, require.slot))
        .collect()
}

#[derive(Debug, Clone, Default)]
struct ManagedProwlarrProxyCleanup {
    target_ref: Option<crate::extensions::manifest::ManifestCapabilityRef>,
    proxies: Vec<ManagedProwlarrProxyTarget>,
}

impl ManagedProwlarrProxyCleanup {
    fn is_empty(&self) -> bool {
        self.proxies.is_empty()
    }
}

#[derive(Debug, Clone)]
struct ManagedProwlarrProxyTarget {
    name: String,
    tags: Vec<String>,
}

fn managed_prowlarr_proxy_cleanup_from_manifest(
    manifest: &ExtensionManifest,
) -> ManagedProwlarrProxyCleanup {
    let mut cleanup = ManagedProwlarrProxyCleanup::default();

    for action in &manifest.actions {
        if action.r#type != "driver_patch" {
            continue;
        }
        let Some(target) = action.target.as_ref() else {
            continue;
        };
        if target.capability != "indexer.registry" {
            continue;
        }
        let Some(patch) = action.patch.as_ref() else {
            continue;
        };
        let Ok(parsed) = serde_json::from_value::<IndexerRegistryPatch>(patch.clone()) else {
            continue;
        };
        if let IndexerRegistryPatch::RegisterIndexerProxies { proxies } = parsed {
            cleanup.target_ref = Some(target.clone());
            cleanup
                .proxies
                .extend(proxies.into_iter().map(|proxy| ManagedProwlarrProxyTarget {
                    name: proxy.name,
                    tags: proxy.tags,
                }));
        }
    }

    cleanup
}

async fn cleanup_extension_downstream_state(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension: &Extension,
) -> anyhow::Result<()> {
    if extension.kind != ExtensionKind::Connector {
        return Ok(());
    }

    let Ok(manifest) = serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
    else {
        tracing::warn!(
            "failed to parse connector manifest during uninstall cleanup: {}",
            extension.extension_id
        );
        return Ok(());
    };
    let cleanup = managed_prowlarr_proxy_cleanup_from_manifest(&manifest);
    if cleanup.is_empty() {
        return Ok(());
    }

    let Some(target_ref) = cleanup.target_ref.as_ref() else {
        return Ok(());
    };

    let providers = store.list_providers(None).await?;
    let mut provider_instances_by_target: HashMap<(String, String), Vec<Uuid>> = HashMap::new();
    let mut providers_by_instance: HashMap<Uuid, Vec<Provider>> = HashMap::new();
    for provider in providers {
        provider_instances_by_target
            .entry((provider.capability.clone(), provider.slot_id.clone()))
            .or_default()
            .push(provider.instance_id);
        providers_by_instance
            .entry(provider.instance_id)
            .or_default()
            .push(provider);
    }

    let Some(target_provider) = resolve_target_provider(
        target_ref,
        &provider_instances_by_target,
        &providers_by_instance,
    ) else {
        return Ok(());
    };
    if target_provider.implementation.as_deref() != Some("prowlarr") {
        return Ok(());
    }

    let Some(instance) = store.get_instance(target_provider.instance_id).await? else {
        return Ok(());
    };
    let api_key =
        resolve_control_api_key(state, store, &instance, &["prowlarr_api_key", "api_key"]).await?;
    let endpoint_json = target_provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("target provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url =
        resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?;

    let proxy_names = cleanup
        .proxies
        .iter()
        .map(|proxy| proxy.name.clone())
        .collect::<Vec<_>>();
    delete_prowlarr_entities_by_name(&base_url, &api_key, "api/v1/indexerProxy", &proxy_names)
        .await?;

    let proxy_tags = cleanup
        .proxies
        .iter()
        .flat_map(|proxy| proxy.tags.iter().cloned())
        .collect::<Vec<_>>();
    delete_unused_prowlarr_tags(&base_url, &api_key, &proxy_tags).await?;

    Ok(())
}

async fn delete_prowlarr_entities_by_name(
    base_url: &str,
    api_key: &str,
    list_path: &str,
    names: &[String],
) -> anyhow::Result<()> {
    if names.is_empty() {
        return Ok(());
    }

    let value = request_control_json(base_url, api_key, &[list_path]).await?;
    let items = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("{list_path} response was not an array"))?;
    let expected_names = names
        .iter()
        .map(|name| normalize_connector_name(name))
        .collect::<HashSet<_>>();

    let mut delete_ids = Vec::new();
    for item in items {
        let Some(name) = item.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !expected_names.contains(&normalize_connector_name(name)) {
            continue;
        }
        let Some(id) = prowlarr_entity_id(item) else {
            continue;
        };
        delete_ids.push(id);
    }

    for id in delete_ids {
        let path = format!("{list_path}/{id}");
        request_control_write(
            base_url,
            api_key,
            ReqwestMethod::DELETE,
            &[path.as_str()],
            None,
        )
        .await?;
    }

    Ok(())
}

async fn delete_unused_prowlarr_tags(
    base_url: &str,
    api_key: &str,
    labels: &[String],
) -> anyhow::Result<()> {
    if labels.is_empty() {
        return Ok(());
    }

    let tags_value = request_control_json(base_url, api_key, &["api/v1/tag"]).await?;
    let tags = tags_value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("api/v1/tag response was not an array"))?;
    let expected_labels = labels
        .iter()
        .map(|label| normalize_connector_name(label))
        .collect::<HashSet<_>>();
    let mut candidate_tag_ids = Vec::new();
    for tag in tags {
        let Some(label) = tag.get("label").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !expected_labels.contains(&normalize_connector_name(label)) {
            continue;
        }
        let Some(id) = prowlarr_entity_id(tag) else {
            continue;
        };
        candidate_tag_ids.push(id);
    }
    if candidate_tag_ids.is_empty() {
        return Ok(());
    }

    let mut used_tag_ids = HashSet::new();
    for path in [
        "api/v1/indexer",
        "api/v1/indexerProxy",
        "api/v1/applications",
    ] {
        let value = request_control_json(base_url, api_key, &[path]).await?;
        let items = value
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("{path} response was not an array"))?;
        for item in items {
            for tag_id in prowlarr_entity_tag_ids(item) {
                used_tag_ids.insert(tag_id);
            }
        }
    }

    candidate_tag_ids.sort_unstable();
    candidate_tag_ids.dedup();
    for tag_id in candidate_tag_ids {
        if used_tag_ids.contains(&tag_id) {
            continue;
        }
        let path = format!("api/v1/tag/{tag_id}");
        request_control_write(
            base_url,
            api_key,
            ReqwestMethod::DELETE,
            &[path.as_str()],
            None,
        )
        .await?;
    }

    Ok(())
}

fn prowlarr_entity_id(value: &serde_json::Value) -> Option<i64> {
    value
        .get("id")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            value
                .get("id")
                .and_then(serde_json::Value::as_u64)
                .and_then(|id| i64::try_from(id).ok())
        })
}

fn prowlarr_entity_tag_ids(value: &serde_json::Value) -> Vec<i64> {
    value
        .get("tags")
        .and_then(serde_json::Value::as_array)
        .map(|tags| {
            tags.iter()
                .filter_map(|tag| {
                    tag.as_i64()
                        .or_else(|| tag.as_u64().and_then(|id| i64::try_from(id).ok()))
                })
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Default)]
struct ConnectorDownstreamVerification {
    present: Vec<String>,
    missing: Vec<String>,
}

async fn verify_connector_downstream_state(
    state: &AppState,
    store: &ExtensionStore<'_>,
    manifest: &ExtensionManifest,
    provider_instances_by_target: &HashMap<(String, String), Vec<Uuid>>,
    providers_by_instance: &HashMap<Uuid, Vec<Provider>>,
) -> anyhow::Result<Option<ConnectorDownstreamVerification>> {
    let mut expected_items: Vec<String> = Vec::new();
    let mut target_ref: Option<crate::extensions::manifest::ManifestCapabilityRef> = None;

    for action in &manifest.actions {
        if action.r#type != "driver_patch" {
            continue;
        }
        let Some(target) = action.target.as_ref() else {
            continue;
        };
        if target.capability != "indexer.registry" {
            continue;
        }
        let Some(patch) = action.patch.as_ref() else {
            continue;
        };
        let Ok(parsed) = serde_json::from_value::<IndexerRegistryPatch>(patch.clone()) else {
            continue;
        };
        match parsed {
            IndexerRegistryPatch::RegisterIndexers { indexers } => {
                expected_items.extend(indexers.into_iter().map(|indexer| indexer.name));
                target_ref = Some(target.clone());
            }
            IndexerRegistryPatch::RegisterIndexerProxies { proxies } => {
                expected_items.extend(proxies.into_iter().map(|proxy| proxy.name));
                target_ref = Some(target.clone());
            }
            _ => {}
        }
    }

    if expected_items.is_empty() {
        return Ok(None);
    }

    let Some(target_ref) = target_ref else {
        return Ok(None);
    };
    let Some(target_provider) = resolve_target_provider(
        &target_ref,
        provider_instances_by_target,
        providers_by_instance,
    ) else {
        return Ok(None);
    };
    if target_provider.implementation.as_deref() != Some("prowlarr") {
        return Ok(None);
    }

    let actual_names = list_prowlarr_indexer_names(state, store, target_provider)
        .await?
        .into_iter()
        .chain(list_prowlarr_indexer_proxy_names(state, store, target_provider).await?)
        .collect::<Vec<_>>();
    let actual_normalized: HashSet<String> = actual_names
        .into_iter()
        .map(|value| normalize_connector_name(&value))
        .collect();

    let mut verification = ConnectorDownstreamVerification::default();
    for expected in expected_items {
        if actual_normalized.contains(&normalize_connector_name(&expected)) {
            verification.present.push(expected);
        } else {
            verification.missing.push(expected);
        }
    }
    Ok(Some(verification))
}

fn resolve_target_provider<'a>(
    target: &crate::extensions::manifest::ManifestCapabilityRef,
    provider_instances_by_target: &HashMap<(String, String), Vec<Uuid>>,
    providers_by_instance: &'a HashMap<Uuid, Vec<Provider>>,
) -> Option<&'a Provider> {
    let instance_ids =
        provider_instances_by_target.get(&(target.capability.clone(), target.slot.clone()))?;
    let mut unique = instance_ids.clone();
    unique.sort();
    unique.dedup();
    if unique.len() != 1 {
        return None;
    }
    let instance_id = *unique.first()?;
    providers_by_instance
        .get(&instance_id)?
        .iter()
        .find(|provider| {
            provider.capability == target.capability && provider.slot_id == target.slot
        })
}

async fn list_prowlarr_indexer_names(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &Provider,
) -> anyhow::Result<Vec<String>> {
    let Some(instance) = store.get_instance(provider.instance_id).await? else {
        anyhow::bail!("target provider instance is missing");
    };
    let api_key =
        resolve_control_api_key(state, store, &instance, &["prowlarr_api_key", "api_key"]).await?;
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("target provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url =
        resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?;
    let value = request_control_json(&base_url, &api_key, &["api/v1/indexer"]).await?;
    let items = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("prowlarr indexer response was not an array"))?;
    Ok(items
        .iter()
        .filter_map(|item| item.get("name").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect())
}

pub(crate) async fn list_prowlarr_indexer_proxy_names(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &Provider,
) -> anyhow::Result<Vec<String>> {
    let Some(instance) = store.get_instance(provider.instance_id).await? else {
        anyhow::bail!("target provider instance is missing");
    };
    let api_key =
        resolve_control_api_key(state, store, &instance, &["prowlarr_api_key", "api_key"]).await?;
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("target provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url =
        resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?;
    let value = request_control_json(&base_url, &api_key, &["api/v1/indexerProxy"]).await?;
    let items = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("prowlarr indexer proxy response was not an array"))?;
    Ok(items
        .iter()
        .filter_map(|item| item.get("name").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect())
}

async fn summarize_connector_extension(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension: &Extension,
    manifest: Option<&ExtensionManifest>,
    available_targets: &HashSet<(String, String)>,
    provider_instances_by_target: &HashMap<(String, String), Vec<Uuid>>,
    providers_by_instance: &HashMap<Uuid, Vec<Provider>>,
    runtime_snapshot: &DockerRuntimeHealthSnapshot,
) -> anyhow::Result<ExtensionStatusSummaryItem> {
    if !extension.enabled {
        return Ok(disabled_extension_status(
            extension,
            "disabled",
            "Disabled",
            "This connector is installed but turned off.",
            "enable",
            "Enable",
        ));
    }

    let has_target = manifest
        .map(|manifest| {
            manifest.targets.iter().any(|target| {
                available_targets.contains(&(target.capability.clone(), target.slot.clone()))
            })
        })
        .unwrap_or(true);

    if !has_target {
        return Ok(attention_extension_status(
            extension,
            "waiting_for_app",
            "Needs setup",
            "Install a compatible app to use this connector.",
            "finish_setup",
            "Finish setup",
        ));
    }

    if let Some(manifest) = manifest {
        let missing_requires = missing_required_connector_targets(manifest, available_targets);
        if !missing_requires.is_empty() {
            return Ok(attention_extension_status(
                extension,
                "waiting_for_dependency",
                "Needs setup",
                &format!(
                    "Required dependencies are missing: {}.",
                    missing_requires.join(", ")
                ),
                "finish_setup",
                "Finish setup",
            ));
        }

        let verification = match verify_connector_downstream_state(
            state,
            store,
            manifest,
            provider_instances_by_target,
            providers_by_instance,
        )
        .await
        {
            Ok(value) => value,
            Err(err) => {
                if runtime_snapshot.state != DockerRuntimeHealthState::Healthy {
                    return Ok(runtime_status_stale_extension_status(
                        extension,
                        runtime_snapshot,
                        "open",
                        "Open",
                    ));
                }
                return Ok(attention_extension_status(
                    extension,
                    "downstream_verification_failed",
                    "Needs attention",
                    &format!(
                        "Elixir could not verify this connector's downstream state yet: {err}"
                    ),
                    "open",
                    "Open",
                ));
            }
        };
        if let Some(verification) = verification {
            if !verification.missing.is_empty() {
                return Ok(attention_extension_status(
                    extension,
                    "downstream_incomplete",
                    "Needs attention",
                    &format!(
                        "Expected downstream items are missing: {}.",
                        verification.missing.join(", ")
                    ),
                    "open",
                    "Open",
                ));
            }

            return Ok(ready_extension_status(
                extension,
                "ready",
                "Ready",
                &format!(
                    "This connector is installed and managing {} downstream item{}.",
                    verification.present.len(),
                    if verification.present.len() == 1 {
                        ""
                    } else {
                        "s"
                    }
                ),
                "open",
                "Open",
            ));
        }
    }

    Ok(ready_extension_status(
        extension,
        "ready",
        "Ready",
        "This connector is installed and ready for compatible apps.",
        "open",
        "Open",
    ))
}

async fn summarize_module_extension(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension: &Extension,
    manifest: Option<&ExtensionManifest>,
    instances: &[ExtensionInstance],
    providers_by_instance: &HashMap<Uuid, Vec<Provider>>,
    readiness_by_provider: &HashMap<Uuid, ProviderReadiness>,
    failed_bindings_by_consumer: &HashMap<Uuid, usize>,
    runtime_snapshot: &DockerRuntimeHealthSnapshot,
) -> anyhow::Result<ExtensionStatusSummaryItem> {
    if !extension.enabled {
        return with_module_auto_update_summary(
            store,
            extension,
            disabled_extension_status(
                extension,
                "disabled",
                "Disabled",
                "This extension is installed but turned off.",
                "enable",
                "Enable",
            ),
        )
        .await;
    }

    if instances.is_empty() {
        return with_module_auto_update_summary(
            store,
            extension,
            attention_extension_status(
                extension,
                "missing_instance",
                "Needs setup",
                "Create an instance to start using this extension.",
                "finish_setup",
                "Finish setup",
            ),
        )
        .await;
    }

    let required_secret_keys = manifest
        .and_then(|manifest| required_secrets_from_manifest(manifest).ok())
        .unwrap_or_default();

    let mut enabled_instance_count = 0usize;
    let mut provider_count = 0usize;
    let mut missing_secret_count = 0usize;
    let mut unhealthy_provider_count = 0usize;
    let mut degraded_provider_count = 0usize;
    let mut transport_ready_count = 0usize;
    let mut bootstrap_ready_count = 0usize;
    let mut failed_binding_count = 0usize;

    for instance in instances {
        if !instance.enabled {
            continue;
        }
        enabled_instance_count += 1;

        if !required_secret_keys.is_empty() {
            let missing = missing_required_secrets_for_instance(
                store,
                instance.instance_id,
                &required_secret_keys,
            )
            .await?;
            missing_secret_count +=
                filter_auto_managed_runtime_missing(&extension.extension_id, missing).len();
        }

        if let Some(providers) = providers_by_instance.get(&instance.instance_id) {
            provider_count += providers.len();
            for provider in providers {
                if let Some(readiness) = readiness_by_provider.get(&provider.provider_id) {
                    match readiness.readiness_phase {
                        ProviderReadinessPhase::TransportReady => transport_ready_count += 1,
                        ProviderReadinessPhase::BootstrapReady => bootstrap_ready_count += 1,
                        ProviderReadinessPhase::Unknown | ProviderReadinessPhase::DriverReady => {}
                    }
                }
                match provider.health_state {
                    ProviderHealthState::Unhealthy => unhealthy_provider_count += 1,
                    ProviderHealthState::Degraded => degraded_provider_count += 1,
                    ProviderHealthState::Unknown | ProviderHealthState::Healthy => {}
                }
                failed_binding_count += failed_bindings_by_consumer
                    .get(&provider.provider_id)
                    .copied()
                    .unwrap_or(0);
            }
        }
    }

    if missing_secret_count > 0 {
        let noun = if missing_secret_count == 1 {
            "secret is"
        } else {
            "secrets are"
        };
        return with_module_auto_update_summary(
            store,
            extension,
            attention_extension_status(
                extension,
                "missing_required_secrets",
                "Needs setup",
                &format!(
                    "Finish setup to add the {} required {noun} still missing.",
                    missing_secret_count
                ),
                "finish_setup",
                "Finish setup",
            ),
        )
        .await;
    }

    if enabled_instance_count == 0 {
        return with_module_auto_update_summary(
            store,
            extension,
            disabled_extension_status(
                extension,
                "instances_disabled",
                "Disabled",
                "All instances for this extension are turned off.",
                "finish_setup",
                "Open",
            ),
        )
        .await;
    }

    if runtime_snapshot.state != DockerRuntimeHealthState::Healthy
        && manifest
            .and_then(|value| value.runtime.as_ref())
            .map(|runtime| runtime.r#type.eq_ignore_ascii_case("container"))
            .unwrap_or(false)
    {
        return with_module_auto_update_summary(
            store,
            extension,
            runtime_status_stale_extension_status(extension, runtime_snapshot, "open", "Open"),
        )
        .await;
    }

    if provider_count == 0 {
        return with_module_auto_update_summary(
            store,
            extension,
            attention_extension_status(
                extension,
                "provider_not_ready",
                "Needs setup",
                "This extension is still finishing setup.",
                "finish_setup",
                "Finish setup",
            ),
        )
        .await;
    }

    if extension
        .extension_id
        .eq_ignore_ascii_case(REAL_DEBRID_EXTENSION_ID)
    {
        if let Some(instance) = choose_extension_control_instance(instances) {
            let has_token = store
                .get_secret(
                    SecretScope::Instance,
                    Some(instance.instance_id),
                    REAL_DEBRID_TOKEN_SECRET_KEY,
                )
                .await?
                .is_some();
            if !has_token {
                return with_module_auto_update_summary(
                    store,
                    extension,
                    attention_extension_status(
                        extension,
                        "provider_setup_required",
                        "Add account",
                        "Add a Real-Debrid API token to enable debrid downloads.",
                        "open",
                        "Add account",
                    ),
                )
                .await;
            }
        }
    }

    if unhealthy_provider_count == 0
        && degraded_provider_count == 0
        && (bootstrap_ready_count > 0 || transport_ready_count > 0)
    {
        let (code, title, description) = if bootstrap_ready_count > 0 {
            (
                "bootstrap_in_progress",
                "Finishing setup",
                "This extension is reachable and Elixir is still applying managed bootstrap.",
            )
        } else {
            (
                "runtime_starting",
                "Starting up",
                "This extension runtime is reachable and Elixir is waiting for the app to finish starting.",
            )
        };
        return with_module_auto_update_summary(
            store,
            extension,
            attention_extension_status(extension, code, title, description, "open", "Open"),
        )
        .await;
    }

    if unhealthy_provider_count > 0 || failed_binding_count > 0 {
        let description = if unhealthy_provider_count > 0 && failed_binding_count > 0 {
            "This extension has connection problems and is not working normally."
        } else if unhealthy_provider_count > 0 {
            "This extension is not responding normally right now."
        } else {
            "This extension has a broken connection that needs repair."
        };
        return with_module_auto_update_summary(
            store,
            extension,
            attention_extension_status(
                extension,
                "connection_issue",
                "Connection issue",
                description,
                "fix",
                "Fix",
            ),
        )
        .await;
    }

    if degraded_provider_count > 0 {
        return with_module_auto_update_summary(
            store,
            extension,
            attention_extension_status(
                extension,
                "degraded_runtime",
                "Needs attention",
                "This extension is working, but it needs attention.",
                "fix",
                "Fix",
            ),
        )
        .await;
    }

    if extension
        .extension_id
        .eq_ignore_ascii_case("elixir.modules.nzbget")
    {
        if let Some(instance) = choose_extension_control_instance(instances) {
            match control::load_nzbget_provider_inventory_summary(
                state,
                store,
                instance.instance_id,
            )
            .await
            {
                Ok(summary) if summary.configured_count == 0 => {
                    return with_module_auto_update_summary(
                        store,
                        extension,
                        attention_extension_status(
                            extension,
                            "provider_setup_required",
                            "Add provider",
                            "Add at least one Usenet provider to start NZBGet downloads.",
                            "open",
                            "Add provider",
                        ),
                    )
                    .await;
                }
                Ok(summary) if summary.active_count == 0 => {
                    return with_module_auto_update_summary(
                        store,
                        extension,
                        attention_extension_status(
                            extension,
                            "provider_setup_required",
                            "Activate provider",
                            "Enable or add at least one active Usenet provider before NZBGet can download.",
                            "open",
                            "Manage providers",
                        ),
                    )
                    .await;
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(
                        "extension summary nzbget provider inventory unavailable: {err}"
                    );
                }
            }
        }
    }

    let description = format!(
        "{} instance{} configured and working normally.",
        enabled_instance_count,
        if enabled_instance_count == 1 { "" } else { "s" }
    );
    with_module_auto_update_summary(
        store,
        extension,
        ready_extension_status(extension, "ready", "Ready", &description, "open", "Open"),
    )
    .await
}

fn extension_status_sort_order(severity: &str) -> usize {
    match severity {
        "attention" => 0,
        "ready" => 1,
        "disabled" => 2,
        _ => 3,
    }
}

fn runtime_status_stale_extension_status(
    extension: &Extension,
    runtime_snapshot: &DockerRuntimeHealthSnapshot,
    primary_action: &str,
    primary_action_label: &str,
) -> ExtensionStatusSummaryItem {
    let (status_code, label, description) = match runtime_snapshot.state {
        DockerRuntimeHealthState::Degraded => (
            "runtime_status_stale",
            "Status stale",
            runtime_snapshot.reason.as_deref().unwrap_or(
                "Docker is unhealthy, so Elixir cannot verify this extension's live status yet.",
            ),
        ),
        DockerRuntimeHealthState::Recovering => (
            "runtime_status_recovering",
            "Recovering",
            runtime_snapshot.reason.as_deref().unwrap_or(
                "Docker recovered recently and Elixir is restoring extension runtimes gradually.",
            ),
        ),
        DockerRuntimeHealthState::Healthy => (
            "ready",
            "Ready",
            "This extension is installed and ready to use.",
        ),
    };

    attention_extension_status(
        extension,
        status_code,
        label,
        description,
        primary_action,
        primary_action_label,
    )
}

fn attention_extension_status(
    extension: &Extension,
    status_code: &str,
    label: &str,
    description: &str,
    primary_action: &str,
    primary_action_label: &str,
) -> ExtensionStatusSummaryItem {
    extension_status_item(
        extension,
        "attention",
        status_code,
        label,
        description,
        primary_action,
        primary_action_label,
    )
}

fn ready_extension_status(
    extension: &Extension,
    status_code: &str,
    label: &str,
    description: &str,
    primary_action: &str,
    primary_action_label: &str,
) -> ExtensionStatusSummaryItem {
    extension_status_item(
        extension,
        "ready",
        status_code,
        label,
        description,
        primary_action,
        primary_action_label,
    )
}

fn disabled_extension_status(
    extension: &Extension,
    status_code: &str,
    label: &str,
    description: &str,
    primary_action: &str,
    primary_action_label: &str,
) -> ExtensionStatusSummaryItem {
    extension_status_item(
        extension,
        "disabled",
        status_code,
        label,
        description,
        primary_action,
        primary_action_label,
    )
}

fn extension_status_item(
    extension: &Extension,
    severity: &str,
    status_code: &str,
    label: &str,
    description: &str,
    primary_action: &str,
    primary_action_label: &str,
) -> ExtensionStatusSummaryItem {
    ExtensionStatusSummaryItem {
        extension_id: extension.extension_id.clone(),
        name: extension.name.clone(),
        version: extension.version.clone(),
        kind: extension.kind.clone(),
        trust_level: extension.trust_level.clone(),
        enabled: extension.enabled,
        severity: severity.to_string(),
        status_code: status_code.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        primary_action: primary_action.to_string(),
        primary_action_label: primary_action_label.to_string(),
        auto_update: None,
        optional_addons: Vec::new(),
    }
}

async fn with_module_auto_update_summary(
    store: &ExtensionStore<'_>,
    extension: &Extension,
    mut summary: ExtensionStatusSummaryItem,
) -> anyhow::Result<ExtensionStatusSummaryItem> {
    summary.auto_update = load_proxy_runtime_update_state(store, &extension.extension_id)
        .await?
        .map(extension_auto_update_summary);
    Ok(summary)
}

fn extension_auto_update_summary(state: ProxyRuntimeUpdateState) -> ExtensionAutoUpdateSummary {
    ExtensionAutoUpdateSummary {
        severity: state.severity,
        status_code: state.status_code,
        label: state.label,
        description: state.description,
        checked_at: Some(state.checked_at),
        release_version: state.release_version,
    }
}

const DOWNLOADER_TELEMETRY_SETTING_PREFIX: &str = "extensions.downloaders.telemetry.";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct DownloaderTelemetryStatus {
    #[serde(default)]
    last_successful_sample_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    last_error_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn downloader_telemetry_setting_key(provider_id: Uuid) -> String {
    format!("{DOWNLOADER_TELEMETRY_SETTING_PREFIX}{provider_id}")
}

async fn load_downloader_telemetry_status(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
) -> anyhow::Result<DownloaderTelemetryStatus> {
    let key = downloader_telemetry_setting_key(provider_id);
    match store.get_extension_setting(&key).await? {
        Some(value) => Ok(serde_json::from_value(value)?),
        None => Ok(DownloaderTelemetryStatus::default()),
    }
}

async fn save_downloader_telemetry_status(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    status: &DownloaderTelemetryStatus,
) -> anyhow::Result<()> {
    let key = downloader_telemetry_setting_key(provider_id);
    store.upsert_extension_setting(&key, &json!(status)).await?;
    Ok(())
}

async fn record_downloader_telemetry_success(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
) -> anyhow::Result<DownloaderTelemetryStatus> {
    let mut status = load_downloader_telemetry_status(store, provider_id).await?;
    status.last_successful_sample_at = Some(chrono::Utc::now());
    save_downloader_telemetry_status(store, provider_id, &status).await?;
    Ok(status)
}

async fn record_downloader_telemetry_error(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
) -> anyhow::Result<DownloaderTelemetryStatus> {
    let mut status = load_downloader_telemetry_status(store, provider_id).await?;
    status.last_error_at = Some(chrono::Utc::now());
    save_downloader_telemetry_status(store, provider_id, &status).await?;
    Ok(status)
}

fn applied_profile_for_provider(
    config_json: Option<&serde_json::Value>,
    capability: &str,
    implementation: Option<&str>,
) -> Option<DownloaderPerformanceProfile> {
    let key = match (capability, implementation.unwrap_or_default()) {
        ("downloader.torrent", "qbittorrent") => "qbittorrent_performance_profile_version",
        ("downloader.nzb", "nzbget") => "nzbget_performance_profile_version",
        _ => return None,
    };
    let version = config_json
        .and_then(|value| value.get("managed_defaults"))
        .and_then(|value| value.get(key))
        .and_then(|value| value.as_str())?;
    match version {
        "v1" | "balanced-v1" | "balanced-v2" | "balanced-v4" => {
            Some(DownloaderPerformanceProfile::Balanced)
        }
        "aggressive-v1" | "aggressive-v2" | "aggressive-v4" => {
            Some(DownloaderPerformanceProfile::Aggressive)
        }
        _ => None,
    }
}

fn downloader_display_name(capability: &str, implementation: Option<&str>) -> String {
    match (capability, implementation.unwrap_or_default()) {
        ("downloader.torrent", "qbittorrent") => "qBittorrent".to_string(),
        ("downloader.nzb", "nzbget") => "NZBGet".to_string(),
        (_, implementation) if !implementation.is_empty() => implementation.to_string(),
        _ => capability.to_string(),
    }
}

fn should_fetch_live_downloader_state(health_state: ProviderHealthState) -> bool {
    matches!(
        health_state,
        ProviderHealthState::Healthy | ProviderHealthState::Degraded
    )
}

#[derive(Debug, Clone)]
struct ExtensionControlContext {
    extension: Extension,
    manifest: ExtensionManifest,
    summary: ExtensionStatusSummaryItem,
    instances: Vec<ExtensionInstance>,
    selected_instance: Option<ExtensionInstance>,
    providers: Vec<Provider>,
    selected_provider: Option<Provider>,
    control_binding: ExtensionControlBinding,
}

#[derive(Debug, Default, Clone)]
struct ExtensionControlLiveSnapshot {
    version: Option<String>,
    metrics: Vec<ExtensionControlMetric>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExtensionControlBinding {
    Sonarr,
    Radarr,
    Prowlarr,
    Qbittorrent,
    Nzbget,
    RealDebrid,
    GenericManifest,
    Unsupported,
}

impl ExtensionControlBinding {
    fn from_provider(provider: &Provider) -> Option<Self> {
        Self::from_signature(&provider.capability, provider.implementation.as_deref())
    }

    fn from_manifest(manifest: &ExtensionManifest) -> Option<Self> {
        let mut bindings = Vec::new();
        for provide in &manifest.provides {
            let Some(binding) =
                Self::from_signature(&provide.capability, provide.implementation.as_deref())
            else {
                continue;
            };
            if !bindings.contains(&binding) {
                bindings.push(binding);
            }
        }
        match bindings.as_slice() {
            [binding] => Some(*binding),
            _ => None,
        }
    }

    fn from_signature(capability: &str, implementation: Option<&str>) -> Option<Self> {
        let implementation = implementation.map(str::trim);
        match (capability.trim(), implementation) {
            ("media.manager.tv", Some("sonarr")) => Some(Self::Sonarr),
            ("media.manager.movies", Some("radarr")) => Some(Self::Radarr),
            ("indexer.registry", Some("prowlarr")) => Some(Self::Prowlarr),
            ("downloader.torrent", Some("qbittorrent")) => Some(Self::Qbittorrent),
            ("downloader.nzb", Some("nzbget")) => Some(Self::Nzbget),
            ("debrid.resolver", Some("real_debrid")) => Some(Self::RealDebrid),
            _ => None,
        }
    }

    fn arr_implementation(self) -> Option<&'static str> {
        match self {
            Self::Sonarr => Some("sonarr"),
            Self::Radarr => Some("radarr"),
            _ => None,
        }
    }
}

async fn build_extension_control_surface(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> anyhow::Result<ExtensionControlSurface> {
    let context = load_extension_control_context(state, store, extension_id).await?;
    let live_snapshot = match tokio::time::timeout(
        Duration::from_secs(2),
        control::load_live_snapshot(state, store, &context),
    )
    .await
    {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(err)) => {
            tracing::debug!(
                "control live snapshot unavailable for {}: {err}",
                context.extension.extension_id
            );
            ExtensionControlLiveSnapshot::default()
        }
        Err(_) => {
            tracing::debug!(
                "control live snapshot timed out for {}",
                context.extension.extension_id
            );
            ExtensionControlLiveSnapshot::default()
        }
    };

    let mut details = Vec::new();
    if !context.summary.description.trim().is_empty() {
        details.push(context.summary.description.clone());
    }
    if context.selected_instance.is_none() && context.extension.kind != ExtensionKind::Blueprint {
        details
            .push("Create or enable a default instance to manage this extension here.".to_string());
    }
    let mut status = ExtensionControlStatus {
        health: control_health_for_summary(&context.summary),
        summary: context.summary.label.clone(),
        details,
        telemetry: (!live_snapshot.metrics.is_empty()).then_some(ExtensionControlTelemetry {
            metrics: live_snapshot.metrics.clone(),
        }),
    };

    let implementation = context
        .selected_provider
        .as_ref()
        .and_then(|provider| provider.implementation.clone());
    let mut sections = control::build_sections(state, store, &context).await?;
    if let Some(section) =
        build_extension_control_managed_invariants_section(state, store, &context).await?
    {
        sections.push(section);
    }
    if let Some(section) = build_extension_control_open_web_ui_section(&context).await? {
        sections.push(section);
    }
    if let Some(section) = build_extension_control_service_section(&context, &live_snapshot) {
        sections.push(section);
    }
    sections.push(build_extension_control_overview_section(&context));

    let mut managed_drift_titles = Vec::new();
    for section in &sections {
        if !section_has_managed_drift(section) {
            continue;
        }
        for notice in &section.notices {
            if notice.code.starts_with("managed_") {
                managed_drift_titles.push(notice.title.clone());
            }
        }
    }
    if !managed_drift_titles.is_empty() {
        status.health = "attention".to_string();
        status.summary = "Managed drift detected".to_string();
        status.details.insert(
            0,
            "Elixir detected downstream changes to settings it owns. Repair them in Elixir instead of treating the downstream edits as the new source of truth."
                .to_string(),
        );
        for title in managed_drift_titles.into_iter().take(3) {
            status.details.push(format!("Managed drift: {title}"));
        }
    }

    Ok(ExtensionControlSurface {
        extension_id: context.extension.extension_id.clone(),
        name: context.extension.name.clone(),
        version: context.extension.version.clone(),
        kind: context.extension.kind.clone(),
        trust_level: context.extension.trust_level.clone(),
        enabled: context.extension.enabled,
        instance_id: context
            .selected_instance
            .as_ref()
            .map(|instance| instance.instance_id),
        instance_name: context
            .selected_instance
            .as_ref()
            .map(|instance| instance.instance_name.clone()),
        implementation,
        status,
        sections,
        actions: build_extension_control_actions(&context),
    })
}

fn is_indexer_registry_connector_manifest(manifest: &ExtensionManifest) -> bool {
    manifest.actions.iter().any(|action| {
        if action.r#type != "driver_patch" {
            return false;
        }
        let Some(target) = action.target.as_ref() else {
            return false;
        };
        if target.capability != "indexer.registry" {
            return false;
        }
        let Some(patch) = action.patch.as_ref() else {
            return false;
        };
        matches!(
            serde_json::from_value::<IndexerRegistryPatch>(patch.clone()),
            Ok(IndexerRegistryPatch::RegisterIndexers { .. })
        )
    })
}

fn managed_indexer_names_from_manifest(manifest: &ExtensionManifest) -> Vec<String> {
    let mut names = Vec::new();
    for action in &manifest.actions {
        if action.r#type != "driver_patch" {
            continue;
        }
        let Some(target) = action.target.as_ref() else {
            continue;
        };
        if target.capability != "indexer.registry" {
            continue;
        }
        let Some(patch) = action.patch.as_ref() else {
            continue;
        };
        if let Ok(IndexerRegistryPatch::RegisterIndexers { indexers }) =
            serde_json::from_value::<IndexerRegistryPatch>(patch.clone())
        {
            for indexer in indexers {
                names.push(indexer.name);
            }
        }
    }
    names
}

fn normalized_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

async fn build_extension_control_prowlarr_indexers_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let Some(provider) = context.selected_provider.as_ref() else {
        return Ok(None);
    };
    if provider.implementation.as_deref() != Some("prowlarr") {
        return Ok(None);
    }

    let (base_url, api_key) =
        resolve_extension_control_arr_connection(state, store, context).await?;
    let value = request_control_json(&base_url, &api_key, &["api/v1/indexer"]).await?;
    let Some(indexers) = value.as_array() else {
        return Ok(None);
    };

    let installed = store.list_extensions().await?;
    let mut managed_by_name: HashMap<String, String> = HashMap::new();
    for extension in &installed {
        if !extension.enabled || extension.kind != ExtensionKind::Connector {
            continue;
        }
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        else {
            continue;
        };
        if !is_indexer_registry_connector_manifest(&manifest) {
            continue;
        }
        for name in managed_indexer_names_from_manifest(&manifest) {
            managed_by_name
                .entry(normalized_name(&name))
                .or_insert_with(|| extension.name.clone());
        }
    }

    let mut entities = Vec::new();
    for item in indexers {
        let name = item
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Indexer")
            .to_string();
        let normalized = normalized_name(&name);
        let implementation = item
            .get("implementation")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Unknown")
            .to_string();
        let enabled = item
            .get("enable")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let ownership = managed_by_name.get(&normalized).cloned();
        let subtitle = ownership
            .as_ref()
            .map(|connector| format!("Managed by Elixir via {connector}"))
            .or_else(|| Some("Custom in Prowlarr".to_string()));
        let mut details = vec![format!("Implementation: {implementation}")];
        if let Some(app_profile_id) = item.get("appProfileId").and_then(serde_json::Value::as_i64) {
            details.push(format!("App profile id {app_profile_id}"));
        }
        details.push(if enabled {
            "Enabled".to_string()
        } else {
            "Disabled".to_string()
        });
        if ownership.is_none() {
            details.push(
                "Manual indexers are left alone by Elixir. Use the Prowlarr UI for site-specific login or custom tuning."
                    .to_string(),
            );
        }
        entities.push(ExtensionControlEntity {
            id: name.clone(),
            title: name,
            subtitle,
            details,
            actions: Vec::new(),
        });
    }

    entities.sort_by(|left, right| {
        let left_manual = left
            .subtitle
            .as_deref()
            .map(|value| value.starts_with("Custom"))
            .unwrap_or(false);
        let right_manual = right
            .subtitle
            .as_deref()
            .map(|value| value.starts_with("Custom"))
            .unwrap_or(false);
        left_manual.cmp(&right_manual).then_with(|| {
            left.title
                .to_ascii_lowercase()
                .cmp(&right.title.to_ascii_lowercase())
        })
    });

    Ok(Some(ExtensionControlSection {
        id: "managedIndexers".to_string(),
        title: "Managed indexers".to_string(),
        description:
            "Elixir-managed connectors keep known indexers aligned automatically. Indexers added manually in Prowlarr are shown here too, but Elixir will not overwrite or remove them."
                .to_string(),
        policy: Some(control_policy_observed(
            "Elixir live-reads this downstream state. Manual Prowlarr indexers remain visible here and are not silently overwritten.",
        )),
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: Vec::new(),
    }))
}

async fn build_extension_control_prowlarr_connector_section(
    _state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let Some(provider) = context.selected_provider.as_ref() else {
        return Ok(None);
    };
    if provider.implementation.as_deref() != Some("prowlarr") {
        return Ok(None);
    }

    let selected_instance_id = context
        .selected_instance
        .as_ref()
        .map(|instance| instance.instance_id);
    let installed = store.list_extensions().await?;
    let installed_by_id: HashMap<String, Extension> = installed
        .iter()
        .cloned()
        .map(|extension| (extension.extension_id.clone(), extension))
        .collect();

    let mut entities = Vec::new();
    let mut seen = HashSet::new();

    for extension in &installed {
        if extension.kind != ExtensionKind::Connector {
            continue;
        }
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        else {
            continue;
        };
        if !is_indexer_registry_connector_manifest(&manifest) {
            continue;
        }
        seen.insert(extension.extension_id.clone());
        let managed_names = managed_indexer_names_from_manifest(&manifest);
        let subtitle = if extension.enabled {
            Some("Managed by Elixir".to_string())
        } else {
            Some("Installed but disabled".to_string())
        };
        let details = if managed_names.is_empty() {
            vec!["Indexer rules are defined by this connector.".to_string()]
        } else {
            vec![format!("Manages: {}", managed_names.join(", "))]
        };
        entities.push(ExtensionControlEntity {
            id: extension.extension_id.clone(),
            title: extension.name.clone(),
            subtitle,
            details,
            actions: vec![if extension.enabled {
                ExtensionControlAction {
                    id: "open_connector".to_string(),
                    label: "Open".to_string(),
                    description: "Manage this connector in Elixir.".to_string(),
                    kind: "secondary".to_string(),
                    params: None,
                    confirm_text: None,
                    navigate_extension_id: Some(extension.extension_id.clone()),
                    navigate_view: None,
                    open_url: None,
                    required_fields: Vec::new(),
                    secret_keys: Vec::new(),
                    secret_scope_instance_id: None,
                }
            } else {
                ExtensionControlAction {
                    id: "activate_connector".to_string(),
                    label: "Enable".to_string(),
                    description: "Enable this managed connector and reapply its rules.".to_string(),
                    kind: "primary".to_string(),
                    params: Some(json!({ "extensionId": extension.extension_id })),
                    confirm_text: None,
                    navigate_extension_id: None,
                    navigate_view: None,
                    open_url: None,
                    required_fields: Vec::new(),
                    secret_keys: Vec::new(),
                    secret_scope_instance_id: selected_instance_id,
                }
            }],
        });
    }

    for addon in collect_indexer_registry_optional_addons(&installed) {
        if seen.contains(&addon.extension_id) {
            continue;
        }
        let installed_extension = installed_by_id.get(&addon.extension_id);
        let secret_keys: Vec<String> = addon
            .required_fields
            .iter()
            .map(|field| addon_secret_key(&addon, field))
            .collect();
        let details = if addon.required_fields.is_empty() {
            vec!["No credentials required.".to_string()]
        } else {
            vec![format!(
                "Activation requires: {}",
                addon
                    .required_fields
                    .iter()
                    .map(|field| field.replace('_', " "))
                    .collect::<Vec<_>>()
                    .join(", ")
            )]
        };
        let action = if let Some(extension) = installed_extension {
            if extension.enabled {
                ExtensionControlAction {
                    id: "open_connector".to_string(),
                    label: "Open".to_string(),
                    description: "Open this managed connector in Elixir.".to_string(),
                    kind: "secondary".to_string(),
                    params: None,
                    confirm_text: None,
                    navigate_extension_id: Some(extension.extension_id.clone()),
                    navigate_view: None,
                    open_url: None,
                    required_fields: Vec::new(),
                    secret_keys: Vec::new(),
                    secret_scope_instance_id: None,
                }
            } else {
                ExtensionControlAction {
                    id: "activate_connector".to_string(),
                    label: "Enable".to_string(),
                    description: "Enable this managed connector and wire it into Prowlarr."
                        .to_string(),
                    kind: "primary".to_string(),
                    params: Some(json!({ "extensionId": extension.extension_id })),
                    confirm_text: None,
                    navigate_extension_id: None,
                    navigate_view: None,
                    open_url: None,
                    required_fields: addon.required_fields.clone(),
                    secret_keys,
                    secret_scope_instance_id: selected_instance_id,
                }
            }
        } else {
            ExtensionControlAction {
                id: "activate_connector".to_string(),
                label: "Activate".to_string(),
                description: "Install this managed connector and wire it into Prowlarr."
                    .to_string(),
                kind: "primary".to_string(),
                params: Some(json!({ "extensionId": addon.extension_id })),
                confirm_text: None,
                navigate_extension_id: None,
                navigate_view: None,
                open_url: None,
                required_fields: addon.required_fields.clone(),
                secret_keys,
                secret_scope_instance_id: selected_instance_id,
            }
        };
        entities.push(ExtensionControlEntity {
            id: addon.extension_id.clone(),
            title: addon
                .title
                .clone()
                .unwrap_or_else(|| addon.extension_id.clone()),
            subtitle: Some("Available managed connector".to_string()),
            details: if let Some(description) = addon.description.clone() {
                let mut values = vec![description];
                values.extend(details);
                values
            } else {
                details
            },
            actions: vec![action],
        });
    }

    entities.sort_by(|left, right| {
        left.title
            .to_ascii_lowercase()
            .cmp(&right.title.to_ascii_lowercase())
    });

    Ok(Some(ExtensionControlSection {
        id: "addConnector".to_string(),
        title: "Add connector".to_string(),
        description:
            "Use managed connectors for curated indexer setups. Connectors keep their downstream Prowlarr entries aligned and let Elixir track whether they are actually working."
                .to_string(),
        policy: None,
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: vec![ExtensionControlAction {
            id: "browse_marketplace".to_string(),
            label: "Browse indexer connectors".to_string(),
            description: "Open the Extensions marketplace filtered to managed indexer connectors."
                .to_string(),
            kind: "secondary".to_string(),
            params: Some(json!({
                "marketplaceKind": "connector",
                "marketplaceTargetCapability": "indexer.registry",
                "marketplaceFilterLabel": "Indexer connectors"
            })),
            confirm_text: None,
            navigate_extension_id: None,
            navigate_view: Some("extensions_marketplace".to_string()),
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        }],
    }))
}

fn collect_indexer_registry_optional_addons(
    extensions: &[Extension],
) -> Vec<crate::extensions::manifest::ManifestOptionalAddon> {
    let mut items = Vec::new();
    let mut seen = HashSet::new();
    for extension in extensions {
        if extension.kind != ExtensionKind::Blueprint || !extension.enabled {
            continue;
        }
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        else {
            continue;
        };
        for addon in manifest.optional_addons {
            let Some(target) = addon.target.as_ref() else {
                continue;
            };
            if target.capability != "indexer.registry" {
                continue;
            }
            if seen.insert(addon.extension_id.clone()) {
                items.push(addon);
            }
        }
    }
    items
}

async fn build_extension_control_open_web_ui_section(
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let Some(action) = build_extension_control_open_service_ui_action(context).await else {
        return Ok(None);
    };
    let implementation = context
        .selected_provider
        .as_ref()
        .and_then(|provider| provider.implementation.as_deref())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let description = match implementation.as_str() {
        "prowlarr" => {
            "Use the native Prowlarr UI for trackers that need site-specific login, captcha, cookies, or settings Elixir does not own yet. Manual indexers remain visible in Elixir but are not overwritten."
        }
        "sonarr" | "radarr" | "bazarr" => {
            "Use the native service UI for advanced settings Elixir does not manage yet."
        }
        "qbittorrent" | "nzbget" => {
            "Use the native downloader UI for queue inspection or advanced service options Elixir does not manage yet."
        }
        _ => "Open the native service UI for advanced management.",
    };

    Ok(Some(ExtensionControlSection {
        id: "manualSetup".to_string(),
        title: "Open web UI".to_string(),
        description: description.to_string(),
        policy: None,
        notices: Vec::new(),
        fields: Vec::new(),
        entities: Vec::new(),
        actions: vec![action],
    }))
}

async fn load_extension_control_context(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> anyhow::Result<ExtensionControlContext> {
    let extension = store
        .get_extension(extension_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("extension not found"))?;
    let manifest = parse_extension_control_manifest(&extension);
    let summary = build_extension_status_summary(state, store)
        .await?
        .items
        .into_iter()
        .find(|item| item.extension_id == extension_id)
        .ok_or_else(|| anyhow::anyhow!("extension status not found"))?;
    let instances = store.list_instances(Some(extension_id)).await?;
    let selected_instance = choose_extension_control_instance(&instances);
    let providers = if let Some(instance) = selected_instance.as_ref() {
        store.list_providers(Some(instance.instance_id)).await?
    } else {
        Vec::new()
    };
    let selected_provider = choose_extension_control_provider(&manifest, &providers);
    let control_binding =
        determine_extension_control_binding(&manifest, selected_provider.as_ref());

    Ok(ExtensionControlContext {
        extension,
        manifest,
        summary,
        instances,
        selected_instance,
        providers,
        selected_provider,
        control_binding,
    })
}

fn parse_extension_control_manifest(extension: &Extension) -> ExtensionManifest {
    match serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone()) {
        Ok(manifest) => manifest,
        Err(err) => {
            tracing::warn!(
                "extension control manifest fallback for {}: {err}",
                extension.extension_id
            );
            ExtensionManifest {
                id: extension.extension_id.clone(),
                version: extension.version.clone(),
                kind: extension.kind.clone(),
                name: extension.name.clone(),
                description: None,
                publisher: None,
                trust: None,
                permissions: Vec::new(),
                provides: Vec::new(),
                requires: Default::default(),
                conflicts: Vec::new(),
                runtime: None,
                backup: None,
                targets: Vec::new(),
                actions: Vec::new(),
                connectors: Vec::new(),
                optional_addons: Vec::new(),
                wants: Vec::new(),
                preferences: None,
                bindings: Vec::new(),
                execution: None,
                policies: None,
                networking: None,
                control_surface: None,
            }
        }
    }
}

fn choose_extension_control_instance(instances: &[ExtensionInstance]) -> Option<ExtensionInstance> {
    let mut enabled: Vec<_> = instances
        .iter()
        .filter(|instance| instance.enabled)
        .cloned()
        .collect();
    enabled.sort_by(|left, right| {
        let left_default = left.instance_name.eq_ignore_ascii_case("default");
        let right_default = right.instance_name.eq_ignore_ascii_case("default");
        right_default
            .cmp(&left_default)
            .then_with(|| left.instance_name.cmp(&right.instance_name))
    });
    if let Some(instance) = enabled.into_iter().next() {
        return Some(instance);
    }

    let mut all = instances.to_vec();
    all.sort_by(|left, right| left.instance_name.cmp(&right.instance_name));
    all.into_iter().next()
}

fn choose_extension_control_provider(
    manifest: &ExtensionManifest,
    providers: &[Provider],
) -> Option<Provider> {
    if let Some(binding) = ExtensionControlBinding::from_manifest(manifest) {
        if let Some(provider) = providers
            .iter()
            .find(|provider| ExtensionControlBinding::from_provider(provider) == Some(binding))
            .cloned()
        {
            return Some(provider);
        }
    }

    let mut sorted = providers.to_vec();
    sorted.sort_by(|left, right| left.capability.cmp(&right.capability));
    sorted.into_iter().next()
}

fn determine_extension_control_binding(
    manifest: &ExtensionManifest,
    selected_provider: Option<&Provider>,
) -> ExtensionControlBinding {
    if let Some(provider) = selected_provider {
        if let Some(binding) = ExtensionControlBinding::from_provider(provider) {
            return binding;
        }
    }
    if let Some(binding) = ExtensionControlBinding::from_manifest(manifest) {
        return binding;
    }
    if manifest
        .control_surface
        .as_ref()
        .map(|surface| surface.adapter.trim().eq_ignore_ascii_case("generic_v1"))
        .unwrap_or(false)
    {
        return ExtensionControlBinding::GenericManifest;
    }
    ExtensionControlBinding::Unsupported
}

fn control_health_for_summary(summary: &ExtensionStatusSummaryItem) -> String {
    match summary.status_code.as_str() {
        "connection_issue" => "error".to_string(),
        "provider_setup_required" => "action_required".to_string(),
        _ if summary.severity == "attention" => "attention".to_string(),
        _ if summary.severity == "disabled" => "disabled".to_string(),
        _ => "healthy".to_string(),
    }
}

fn build_extension_control_overview_section(
    context: &ExtensionControlContext,
) -> ExtensionControlSection {
    let provider_count = context.providers.len().to_string();
    let capability_list = if context.providers.is_empty() {
        "Not available yet".to_string()
    } else {
        let mut values = context
            .providers
            .iter()
            .map(|provider| provider.capability.clone())
            .collect::<Vec<_>>();
        values.sort();
        values.dedup();
        values.join(", ")
    };

    let mut fields = vec![
        control_text_field("version", "Version", "", context.extension.version.clone()),
        control_text_field(
            "trustLevel",
            "Trust",
            "",
            context.extension.trust_level.as_str().to_string(),
        ),
        control_text_field(
            "instanceCount",
            "Instances",
            "",
            context.instances.len().to_string(),
        ),
        control_text_field("providerCount", "Providers", "", provider_count),
        control_text_field("capabilities", "Capabilities", "", capability_list),
    ];

    if let Some(instance) = context.selected_instance.as_ref() {
        fields.insert(
            2,
            control_text_field(
                "instanceName",
                "Selected instance",
                "",
                instance.instance_name.clone(),
            ),
        );
    }

    ExtensionControlSection {
        id: "overview".to_string(),
        title: "Overview".to_string(),
        description: "High-level status for this extension.".to_string(),
        policy: None,
        notices: Vec::new(),
        fields,
        entities: Vec::new(),
        actions: Vec::new(),
    }
}

fn build_extension_control_service_section(
    context: &ExtensionControlContext,
    live_snapshot: &ExtensionControlLiveSnapshot,
) -> Option<ExtensionControlSection> {
    let provider = context.selected_provider.as_ref()?;
    let mut fields = vec![
        control_text_field(
            "implementation",
            "Implementation",
            "",
            provider
                .implementation
                .clone()
                .unwrap_or_else(|| provider.capability.clone()),
        ),
        control_text_field(
            "healthState",
            "Health",
            "",
            provider.health_state.as_str().to_string(),
        ),
        control_text_field(
            "lastHealthcheck",
            "Last health check",
            "",
            provider
                .last_healthcheck_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "Not yet checked".to_string()),
        ),
    ];

    if let Some(version) = live_snapshot.version.as_ref() {
        fields.insert(
            0,
            control_text_field("serviceVersion", "Service version", "", version.clone()),
        );
    }

    for metric in &live_snapshot.metrics {
        if metric.id == "version" {
            continue;
        }
        fields.push(control_text_field(
            &metric.id,
            &metric.label,
            "",
            metric.value.clone(),
        ));
    }

    Some(ExtensionControlSection {
        id: "service".to_string(),
        title: "Service".to_string(),
        description: "Live status from the managed service when it is reachable.".to_string(),
        policy: None,
        notices: Vec::new(),
        fields,
        entities: Vec::new(),
        actions: Vec::new(),
    })
}

const CONTROL_DEFAULTS_SETTING_PREFIX: &str = "extensions.control_defaults.instance.";
const ARR_DOWNLOAD_CLIENT_CACHE_SETTING_PREFIX: &str =
    "extensions.control_download_clients.instance.";

#[derive(Debug, Clone)]
struct ManagerControlDefaults {
    monitor_on_add: bool,
    search_on_add: bool,
}

impl Default for ManagerControlDefaults {
    fn default() -> Self {
        Self {
            monitor_on_add: true,
            search_on_add: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrDownloadClientPreference {
    KeepCurrent,
    Usenet,
    Torrent,
}

impl ArrDownloadClientPreference {
    fn as_str(self) -> &'static str {
        match self {
            Self::KeepCurrent => "current",
            Self::Usenet => "usenet",
            Self::Torrent => "torrent",
        }
    }

    fn from_value(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "current" => Some(Self::KeepCurrent),
            "usenet" => Some(Self::Usenet),
            "torrent" => Some(Self::Torrent),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrDownloadClientProtocol {
    Usenet,
    Torrent,
    Unknown,
}

impl ArrDownloadClientProtocol {
    fn as_setting_value(self) -> &'static str {
        match self {
            Self::Usenet => "usenet",
            Self::Torrent => "torrent",
            Self::Unknown => "unknown",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Usenet => "Usenet",
            Self::Torrent => "Torrent",
            Self::Unknown => "Other",
        }
    }
}

#[derive(Debug, Clone)]
struct ArrControlDownloadClient {
    id: i64,
    name: String,
    implementation: Option<String>,
    protocol: ArrDownloadClientProtocol,
    priority: i64,
    enabled: bool,
    raw: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArrControlDownloadClientCacheEntry {
    id: i64,
    name: String,
    implementation: Option<String>,
    protocol: String,
    priority: i64,
    enabled: bool,
}

fn control_defaults_setting_key(instance_id: Uuid) -> String {
    format!("{CONTROL_DEFAULTS_SETTING_PREFIX}{instance_id}")
}

fn arr_download_client_cache_setting_key(instance_id: Uuid) -> String {
    format!("{ARR_DOWNLOAD_CLIENT_CACHE_SETTING_PREFIX}{instance_id}")
}

async fn build_extension_control_settings_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    if matches!(
        context.control_binding,
        ExtensionControlBinding::Sonarr | ExtensionControlBinding::Radarr
    ) {
        let Some(instance) = context.selected_instance.as_ref() else {
            return Ok(None);
        };
        let defaults = load_manager_control_defaults(store, instance.instance_id).await?;
        return Ok(Some(ExtensionControlSection {
            id: "defaults".to_string(),
            title: "Add defaults".to_string(),
            description:
                "These defaults are used when you add media from Find Media into this manager."
                    .to_string(),
            policy: Some(control_policy_seeded(
                "Elixir seeds these defaults when you add media. Downstream manager edits are allowed and can become the new live value.",
            )),
            notices: Vec::new(),
            fields: vec![
                ExtensionControlField {
                    id: "monitorOnAdd".to_string(),
                    label: "Monitor on add".to_string(),
                    description:
                        "Keep new items monitored so future releases are tracked automatically."
                            .to_string(),
                    field_type: "toggle".to_string(),
                    value: serde_json::Value::Bool(defaults.monitor_on_add),
                    required: false,
                    readonly: false,
                    secret: false,
                    options: Vec::new(),
                    validation: None,
                },
                ExtensionControlField {
                    id: "searchOnAdd".to_string(),
                    label: "Search on add".to_string(),
                    description:
                        "Start a search immediately after the item is accepted by the manager."
                            .to_string(),
                    field_type: "toggle".to_string(),
                    value: serde_json::Value::Bool(defaults.search_on_add),
                    required: false,
                    readonly: false,
                    secret: false,
                    options: Vec::new(),
                    validation: None,
                },
            ],
            entities: Vec::new(),
            actions: Vec::new(),
        }));
    }

    if matches!(
        context.control_binding,
        ExtensionControlBinding::Qbittorrent | ExtensionControlBinding::Nzbget
    ) {
        let override_value = store
            .get_extension_setting(DOWNLOADER_PROFILE_SETTING_KEY)
            .await?;
        let selected = DownloaderPerformanceProfile::from_setting_value(
            override_value.as_ref(),
            state.settings.extensions.downloader_profile,
        );
        return Ok(Some(ExtensionControlSection {
            id: "defaults".to_string(),
            title: "Downloader defaults".to_string(),
            description:
                "This shared profile seeds Elixir-managed downloaders for balanced or aggressive use."
                    .to_string(),
            policy: Some(control_policy_seeded(
                "Elixir applies this profile on bootstrap, when you change it here, and during explicit repair. Steady-state reconcile observes downloader health but does not keep rewriting the profile in the background.",
            )),
            notices: Vec::new(),
            fields: vec![ExtensionControlField {
                id: "downloaderProfile".to_string(),
                label: "Performance profile".to_string(),
                description: "Balanced is safer by default. Aggressive prioritizes throughput."
                    .to_string(),
                field_type: "select".to_string(),
                value: serde_json::Value::String(selected.as_str().to_string()),
                required: true,
                readonly: false,
                secret: false,
                options: vec![
                    ExtensionControlOption {
                        value: serde_json::Value::String("balanced".to_string()),
                        label: "Balanced".to_string(),
                    },
                    ExtensionControlOption {
                        value: serde_json::Value::String("aggressive".to_string()),
                        label: "Aggressive".to_string(),
                    },
                ],
                validation: None,
            }],
            entities: Vec::new(),
            actions: Vec::new(),
        }));
    }

    Ok(None)
}

async fn build_extension_control_download_client_preference_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    if !matches!(
        context.control_binding,
        ExtensionControlBinding::Sonarr | ExtensionControlBinding::Radarr
    ) {
        return Ok(None);
    }

    let (clients, live_clients) =
        match load_arr_control_download_clients_with_fallback(state, store, context).await {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(
                    "manager download client preference unavailable for {}: {err}",
                    context.extension.extension_id
                );
                return Ok(None);
            }
        };
    if clients.is_empty() {
        return Ok(None);
    }

    let current_preference = infer_arr_download_client_preference(&clients);
    let (has_usenet, has_torrent) = arr_download_client_protocol_coverage(&clients);
    let readonly = !live_clients || !has_usenet || !has_torrent;
    let description = if !live_clients {
        "Elixir is showing the last known manager client order while Sonarr or Radarr is still reloading download-client state."
            .to_string()
    } else if !has_usenet || !has_torrent {
        "Add both a Usenet and a torrent client in the manager to switch protocol preference here."
            .to_string()
    } else {
        "Choose which protocol group Sonarr or Radarr should favor. Elixir rewrites the manager's download client priorities; Keep current order leaves the existing manager order unchanged."
            .to_string()
    };

    let mut entities = clients;
    entities.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| right.enabled.cmp(&left.enabled))
            .then_with(|| left.name.cmp(&right.name))
    });

    Ok(Some(ExtensionControlSection {
        id: "downloadClientPreference".to_string(),
        title: "Download client preference".to_string(),
        description,
        policy: Some(control_policy_seeded(
            "Elixir seeds manager protocol preference here. If you change download-client priority downstream, Elixir reflects the new live order instead of silently fighting it.",
        )),
        notices: Vec::new(),
        fields: vec![ExtensionControlField {
            id: "downloadClientPreference".to_string(),
            label: "Preferred source".to_string(),
            description:
                "Use this to favor Usenet or torrent clients in the manager without leaving Elixir."
                    .to_string(),
            field_type: "select".to_string(),
            value: serde_json::Value::String(current_preference.as_str().to_string()),
            required: true,
            readonly,
            secret: false,
            options: vec![
                ExtensionControlOption {
                    value: serde_json::Value::String(
                        ArrDownloadClientPreference::KeepCurrent
                            .as_str()
                            .to_string(),
                    ),
                    label: "Keep current order".to_string(),
                },
                ExtensionControlOption {
                    value: serde_json::Value::String(
                        ArrDownloadClientPreference::Usenet.as_str().to_string(),
                    ),
                    label: "Prefer Usenet".to_string(),
                },
                ExtensionControlOption {
                    value: serde_json::Value::String(
                        ArrDownloadClientPreference::Torrent.as_str().to_string(),
                    ),
                    label: "Prefer Torrent".to_string(),
                },
            ],
            validation: None,
        }],
        entities: entities
            .iter()
            .map(build_arr_download_client_entity)
            .collect(),
        actions: Vec::new(),
    }))
}

async fn build_extension_control_managed_items_section(
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let Some(provider) = context.selected_provider.as_ref() else {
        return Ok(None);
    };
    let implementation = provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if implementation != "sonarr" && implementation != "radarr" {
        return Ok(None);
    }

    let mut intents = store.list_active_managed_ingest_intents().await?;
    intents.retain(|intent| intent.manager_provider_id == provider.provider_id);
    intents.truncate(8);

    let entities = intents
        .iter()
        .map(|intent| build_extension_control_managed_item_entity(&implementation, intent))
        .collect::<Vec<_>>();

    Ok(Some(ExtensionControlSection {
        id: "managedItems".to_string(),
        title: "Managed items".to_string(),
        description:
            "Recent media accepted by this manager through Elixir. Use these actions to search, refresh, or remove a request."
                .to_string(),
        policy: None,
        notices: Vec::new(),
        fields: Vec::new(),
        entities,
        actions: build_extension_control_manager_actions(&implementation),
    }))
}

#[derive(Debug, Clone)]
struct ManagedArrDownloaderExpectation {
    label: &'static str,
    implementation: &'static str,
    host: String,
    aliases: Vec<String>,
    port: u16,
    category: &'static str,
}

async fn build_extension_control_managed_invariants_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let result = match context.control_binding {
        ExtensionControlBinding::Sonarr => tokio::time::timeout(
            Duration::from_secs(3),
            build_arr_managed_invariants_section(state, store, context, "sonarr"),
        )
        .await
        .ok()
        .transpose()?
        .flatten(),
        ExtensionControlBinding::Radarr => tokio::time::timeout(
            Duration::from_secs(3),
            build_arr_managed_invariants_section(state, store, context, "radarr"),
        )
        .await
        .ok()
        .transpose()?
        .flatten(),
        ExtensionControlBinding::Qbittorrent => tokio::time::timeout(
            Duration::from_secs(3),
            build_qbittorrent_managed_invariants_section(state, store, context),
        )
        .await
        .ok()
        .transpose()?
        .flatten(),
        ExtensionControlBinding::Nzbget => tokio::time::timeout(
            Duration::from_secs(3),
            build_nzbget_managed_invariants_section(state, store, context),
        )
        .await
        .ok()
        .transpose()?
        .flatten(),
        _ => None,
    };

    Ok(result)
}

async fn build_arr_managed_invariants_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    implementation: &str,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let expected = resolve_arr_managed_downloader_expectations(store, implementation).await?;
    if expected.is_empty() {
        return Ok(None);
    }

    let clients = load_arr_control_download_clients(state, store, context).await?;
    let mut notices = Vec::new();

    for expected_client in expected {
        let Some(client) =
            find_arr_download_client_by_implementation(&clients, expected_client.implementation)
        else {
            notices.push(control_notice(
                "error",
                "managed_downloader_missing",
                &format!("{} wiring missing", expected_client.label),
                format!(
                    "Elixir manages a {} download client in {}. The manager no longer exposes that client, so stack wiring has drifted.",
                    expected_client.label,
                    if implementation == "sonarr" {
                        "Sonarr"
                    } else {
                        "Radarr"
                    }
                ),
            ));
            continue;
        };

        let detail =
            load_arr_control_download_client_detail(state, store, context, client.id).await?;
        let live_host_port = extract_arr_download_client_host_port(&detail);
        match live_host_port {
            Some((host, port))
                if host_matches_expected_alias(&host, &expected_client.aliases)
                    && port == expected_client.port => {}
            Some((host, port)) => notices.push(control_notice(
                "error",
                "managed_downloader_endpoint_drift",
                &format!("{} endpoint drifted", expected_client.label),
                format!(
                    "Elixir manages this {} client at {}:{}, but the manager is currently pointing to {}:{}.",
                    expected_client.label,
                    expected_client.host,
                    expected_client.port,
                    host,
                    port
                ),
            )),
            None => notices.push(control_notice(
                "warning",
                "managed_downloader_endpoint_unknown",
                &format!("{} endpoint could not be verified", expected_client.label),
                format!(
                    "Elixir could not read a host and port for the managed {} client, so endpoint drift cannot be ruled out.",
                    expected_client.label
                ),
            )),
        }

        let live_category = extract_arr_download_client_category(&detail);
        match live_category.as_deref() {
            Some(value) if value.eq_ignore_ascii_case(expected_client.category) => {}
            Some(value) => notices.push(control_notice(
                "error",
                "managed_downloader_category_drift",
                &format!("{} category drifted", expected_client.label),
                format!(
                    "Elixir manages the {} category as '{}', but the manager is using '{}'.",
                    expected_client.label,
                    expected_client.category,
                    value
                ),
            )),
            None => notices.push(control_notice(
                "warning",
                "managed_downloader_category_missing",
                &format!("{} category could not be verified", expected_client.label),
                format!(
                    "Elixir could not read the managed category field for {}. Downstream drift cannot be verified until the manager exposes it again.",
                    expected_client.label
                ),
            )),
        }

        if !client.enabled {
            notices.push(control_notice(
                "error",
                "managed_downloader_disabled",
                &format!("{} is disabled", expected_client.label),
                format!(
                    "Elixir manages the {} client as enabled, but it is disabled downstream.",
                    expected_client.label
                ),
            ));
        }
    }

    if notices.is_empty() {
        return Ok(None);
    }

    Ok(Some(ExtensionControlSection {
        id: "managedInvariants".to_string(),
        title: "Managed invariants".to_string(),
        description:
            "Elixir owns stack-critical manager wiring here. Downstream edits are treated as drift and should be repaired instead of silently adopted."
                .to_string(),
        policy: Some(control_policy_managed(
            "These settings are stack invariants. Elixir does not silently accept downstream edits because other extensions depend on them staying aligned.",
        )),
        notices,
        fields: Vec::new(),
        entities: Vec::new(),
        actions: vec![repair_managed_invariants_action()],
    }))
}

async fn build_qbittorrent_managed_invariants_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let prefs = control::load_qbittorrent_preferences(state, store, context).await?;
    let categories = control::load_qbittorrent_categories(state, store, context).await?;
    let mut notices = Vec::new();

    let expected_prefs = [
        (
            "save_path",
            serde_json::Value::String(DOWNLOADS_ROOT.to_string()),
        ),
        (
            "temp_path",
            serde_json::Value::String(QBITTORRENT_INCOMPLETE_DIR.to_string()),
        ),
        ("temp_path_enabled", serde_json::Value::Bool(true)),
    ];

    for (key, expected) in expected_prefs {
        match prefs.get(key) {
            Some(value) if *value == expected => {}
            Some(value) => notices.push(control_notice(
                "error",
                "managed_qbittorrent_pref_drift",
                &format!("qBittorrent {} drifted", key.replace('_', " ")),
                format!(
                    "Elixir manages qBittorrent {} as {}, but the live value is {}.",
                    key.replace('_', " "),
                    display_control_json_value(&expected),
                    display_control_json_value(value)
                ),
            )),
            None => notices.push(control_notice(
                "warning",
                "managed_qbittorrent_pref_missing",
                &format!("qBittorrent {} could not be read", key.replace('_', " ")),
                format!(
                    "Elixir could not read the live qBittorrent preference '{}', so managed drift cannot be ruled out.",
                    key
                ),
            )),
        }
    }

    let expected_categories = [
        ("tv", "/downloads/tv"),
        ("anime", "/downloads/anime"),
        ("movies", "/downloads/movies"),
    ];
    for (name, expected_path) in expected_categories {
        match categories
            .get(name)
            .and_then(serde_json::Value::as_object)
            .and_then(|value| value.get("savePath"))
            .and_then(serde_json::Value::as_str)
        {
            Some(path) if path == expected_path => {}
            Some(path) => notices.push(control_notice(
                "error",
                "managed_qbittorrent_category_drift",
                &format!("qBittorrent category '{}' drifted", name),
                format!(
                    "Elixir manages qBittorrent category '{}' at {}, but the live path is {}.",
                    name, expected_path, path
                ),
            )),
            None => notices.push(control_notice(
                "error",
                "managed_qbittorrent_category_missing",
                &format!("qBittorrent category '{}' is missing", name),
                format!(
                    "Elixir manages qBittorrent category '{}' at {}, but the live category is missing.",
                    name, expected_path
                ),
            )),
        }
    }

    if notices.is_empty() {
        return Ok(None);
    }

    Ok(Some(ExtensionControlSection {
        id: "managedInvariants".to_string(),
        title: "Managed invariants".to_string(),
        description:
            "Elixir owns the downloader paths and category routing the rest of the stack depends on."
                .to_string(),
        policy: Some(control_policy_managed(
            "These qBittorrent paths and categories are stack invariants. Downstream edits are flagged as drift because manager/downloader wiring depends on them.",
        )),
        notices,
        fields: Vec::new(),
        entities: Vec::new(),
        actions: vec![repair_managed_invariants_action()],
    }))
}

async fn build_nzbget_managed_invariants_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let config = control::load_nzbget_live_config_map(state, store, context).await?;
    let mut notices = Vec::new();

    for (key, expected) in [
        ("DestDir", "/downloads"),
        ("InterDir", "/runtime/incomplete"),
        ("NzbDir", "/runtime/nzb"),
        ("QueueDir", "/runtime/queue"),
        ("TempDir", "/runtime/tmp"),
        ("LockFile", "/config/nzbget.lock"),
    ] {
        match config.get(key) {
            Some(value) if value == expected => {}
            Some(value) => notices.push(control_notice(
                "error",
                "managed_nzbget_path_drift",
                &format!("NZBGet {} drifted", key),
                format!(
                    "Elixir manages NZBGet {} as {}, but the live value is {}.",
                    key, expected, value
                ),
            )),
            None => notices.push(control_notice(
                "warning",
                "managed_nzbget_path_missing",
                &format!("NZBGet {} could not be read", key),
                format!(
                    "Elixir could not read live NZBGet setting '{}', so managed drift cannot be ruled out.",
                    key
                ),
            )),
        }
    }

    let category_paths = parse_nzbget_live_category_paths(&config);
    for (name, expected_path) in [
        ("tv", "/downloads/tv"),
        ("anime", "/downloads/anime"),
        ("movies", "/downloads/movies"),
    ] {
        match category_paths.get(name) {
            Some(path) if path == expected_path => {}
            Some(path) => notices.push(control_notice(
                "error",
                "managed_nzbget_category_drift",
                &format!("NZBGet category '{}' drifted", name),
                format!(
                    "Elixir manages NZBGet category '{}' at {}, but the live path is {}.",
                    name, expected_path, path
                ),
            )),
            None => notices.push(control_notice(
                "error",
                "managed_nzbget_category_missing",
                &format!("NZBGet category '{}' is missing", name),
                format!(
                    "Elixir manages NZBGet category '{}' at {}, but the live category is missing.",
                    name, expected_path
                ),
            )),
        }
    }

    if notices.is_empty() {
        return Ok(None);
    }

    Ok(Some(ExtensionControlSection {
        id: "managedInvariants".to_string(),
        title: "Managed invariants".to_string(),
        description: "Elixir owns the NZBGet paths and categories the Arr stack depends on."
            .to_string(),
        policy: Some(control_policy_managed(
            "These NZBGet defaults are stack invariants. Downstream edits are flagged as drift because manager autowiring depends on them remaining aligned.",
        )),
        notices,
        fields: Vec::new(),
        entities: Vec::new(),
        actions: vec![repair_managed_invariants_action()],
    }))
}

async fn resolve_arr_managed_downloader_expectations(
    store: &ExtensionStore<'_>,
    implementation: &str,
) -> anyhow::Result<Vec<ManagedArrDownloaderExpectation>> {
    let category = if implementation == "sonarr" {
        "tv"
    } else {
        "movies"
    };
    let mut items = Vec::new();

    if let Some((host, aliases, port)) = resolve_first_party_downloader_endpoint(
        store,
        "elixir.modules.nzbget",
        "downloader.nzb",
        "nzbget",
    )
    .await?
    {
        items.push(ManagedArrDownloaderExpectation {
            label: "NZBGet",
            implementation: "nzbget",
            host,
            aliases,
            port,
            category,
        });
    }
    if let Some((host, aliases, port)) = resolve_first_party_downloader_endpoint(
        store,
        "elixir.modules.qbittorrent",
        "downloader.torrent",
        "qbittorrent",
    )
    .await?
    {
        items.push(ManagedArrDownloaderExpectation {
            label: "qBittorrent",
            implementation: "qbittorrent",
            host,
            aliases,
            port,
            category,
        });
    }

    Ok(items)
}

async fn resolve_first_party_downloader_endpoint(
    store: &ExtensionStore<'_>,
    extension_id: &str,
    capability: &str,
    implementation: &str,
) -> anyhow::Result<Option<(String, Vec<String>, u16)>> {
    let instances = store.list_instances(Some(extension_id)).await?;
    let Some(instance) = choose_extension_control_instance(&instances) else {
        return Ok(None);
    };
    let providers = store.list_providers(Some(instance.instance_id)).await?;
    let Some(provider) = providers.into_iter().find(|provider| {
        provider.capability == capability
            && provider
                .implementation
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case(implementation))
                .unwrap_or(false)
    }) else {
        return Ok(None);
    };
    let endpoint_json = provider
        .endpoint_json
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let extension = store
        .get_extension(extension_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("extension '{}' not found", extension_id))?;
    let manifest = parse_extension_control_manifest(&extension);
    let aliases = control_endpoint_aliases(extension_id, &manifest, &instance, &endpoint);
    Ok(Some((endpoint.host, aliases, endpoint.port)))
}

fn host_matches_expected_alias(host: &str, aliases: &[String]) -> bool {
    aliases
        .iter()
        .any(|alias| alias.eq_ignore_ascii_case(host.trim()))
}

fn control_endpoint_aliases(
    extension_id: &str,
    manifest: &ExtensionManifest,
    instance: &ExtensionInstance,
    endpoint: &ProviderEndpoint,
) -> Vec<String> {
    let service_name = manifest_runtime_service_name(manifest);
    let (computed, _) = build_aliases(
        extension_id,
        &instance.instance_name,
        instance.instance_id,
        service_name,
    );
    let mut aliases = vec![endpoint.host.clone()];
    for alias in computed {
        if !aliases
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(&alias))
        {
            aliases.push(alias);
        }
    }
    aliases
}

fn manifest_runtime_service_name(manifest: &ExtensionManifest) -> Option<String> {
    manifest
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.service_name.as_ref())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn find_arr_download_client_by_implementation<'a>(
    clients: &'a [ArrControlDownloadClient],
    implementation: &str,
) -> Option<&'a ArrControlDownloadClient> {
    clients
        .iter()
        .find(|client| {
            client
                .implementation
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case(implementation))
                .unwrap_or(false)
        })
        .or_else(|| {
            clients.iter().find(|client| {
                client
                    .name
                    .eq_ignore_ascii_case(if implementation == "qbittorrent" {
                        "qBittorrent"
                    } else {
                        "NZBGet"
                    })
            })
        })
}

fn extract_control_json_field_text(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .and_then(|fields| {
            fields.iter().find_map(|field| {
                let field_name = field.get("name").and_then(serde_json::Value::as_str)?;
                if !field_name.eq_ignore_ascii_case(name) {
                    return None;
                }
                field
                    .get("value")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| field.as_str())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            })
        })
}

fn extract_arr_download_client_host_port(value: &serde_json::Value) -> Option<(String, u16)> {
    if let Some(url_text) = extract_control_json_field_text(value, "baseUrl")
        .or_else(|| extract_control_json_field_text(value, "url"))
    {
        if let Ok(parsed) = Url::parse(&url_text) {
            if let Some(host) = parsed.host_str() {
                return Some((
                    host.to_string(),
                    parsed.port_or_known_default().unwrap_or(80),
                ));
            }
        }
    }

    let host = extract_control_json_field_text(value, "host")?;
    let port = extract_control_json_field_i64(value, "port")
        .and_then(|number| u16::try_from(number).ok())?;
    Some((host, port))
}

fn extract_arr_download_client_category(value: &serde_json::Value) -> Option<String> {
    for key in ["category", "tvCategory", "movieCategory"] {
        if let Some(value) = extract_control_json_field_text(value, key) {
            return Some(value);
        }
    }
    None
}

fn display_control_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn parse_nzbget_live_category_paths(config: &BTreeMap<String, String>) -> HashMap<String, String> {
    let mut names = HashMap::new();
    let mut paths = HashMap::new();

    for (key, value) in config {
        let Some((slot, field)) = parse_nzbget_live_category_key(key) else {
            continue;
        };
        match field {
            "Name" => {
                names.insert(slot, value.trim().to_ascii_lowercase());
            }
            "DestDir" => {
                paths.insert(slot, value.trim().to_string());
            }
            _ => {}
        }
    }

    let mut categories = HashMap::new();
    for (slot, name) in names {
        if name.is_empty() {
            continue;
        }
        if let Some(path) = paths.get(&slot) {
            categories.insert(name, path.clone());
        }
    }
    categories
}

fn parse_nzbget_live_category_key(name: &str) -> Option<(u32, &str)> {
    let remainder = name.strip_prefix("Category")?;
    let (slot, field) = remainder.split_once('.')?;
    Some((slot.parse().ok()?, field))
}

fn build_extension_control_managed_item_entity(
    implementation: &str,
    intent: &ManagedIngestIntent,
) -> ExtensionControlEntity {
    let title = match intent.year {
        Some(year) => format!("{} ({year})", intent.title),
        None => intent.title.clone(),
    };
    let subtitle = intent.manager_label.clone().or_else(|| {
        Some(if implementation == "sonarr" {
            "Tracked by Sonarr".to_string()
        } else {
            "Tracked by Radarr".to_string()
        })
    });
    let mut details = vec![format!("Requested {}", intent.created_at.to_rfc3339())];
    if let Some(item_id) = intent.manager_item_id.as_deref() {
        details.push(format!("Manager item id {item_id}"));
    }
    if let Some(matched_at) = intent.last_matched_at {
        details.push(format!("Matched in library {}", matched_at.to_rfc3339()));
    }
    let mut actions = Vec::new();
    if intent.manager_item_id.is_some() {
        actions.push(ExtensionControlAction {
            id: "search_item".to_string(),
            label: "Search".to_string(),
            description: "Start a search for this title now.".to_string(),
            kind: "secondary".to_string(),
            params: Some(json!({ "intentId": intent.intent_id.to_string() })),
            confirm_text: None,
            navigate_extension_id: None,
            navigate_view: None,
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        });
        actions.push(ExtensionControlAction {
            id: "refresh_item".to_string(),
            label: "Refresh".to_string(),
            description: "Refresh this title from the manager.".to_string(),
            kind: "secondary".to_string(),
            params: Some(json!({ "intentId": intent.intent_id.to_string() })),
            confirm_text: None,
            navigate_extension_id: None,
            navigate_view: None,
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        });
        actions.push(ExtensionControlAction {
            id: "remove_item".to_string(),
            label: "Remove".to_string(),
            description: "Remove this title from the manager and stop tracking it in Elixir."
                .to_string(),
            kind: "danger".to_string(),
            params: Some(json!({ "intentId": intent.intent_id.to_string() })),
            confirm_text: Some(format!("Remove {} from this manager?", title)),
            navigate_extension_id: None,
            navigate_view: None,
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        });
    }

    ExtensionControlEntity {
        id: intent.intent_id.to_string(),
        title,
        subtitle,
        details,
        actions,
    }
}

fn build_extension_control_manager_actions(implementation: &str) -> Vec<ExtensionControlAction> {
    let search_label = if implementation == "sonarr" {
        "Search missing"
    } else {
        "Search missing"
    };
    vec![
        ExtensionControlAction {
            id: "refresh_manager".to_string(),
            label: "Refresh library".to_string(),
            description: "Refresh the manager so Elixir sees the latest manager state.".to_string(),
            kind: "secondary".to_string(),
            params: None,
            confirm_text: None,
            navigate_extension_id: None,
            navigate_view: None,
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        },
        ExtensionControlAction {
            id: "search_missing".to_string(),
            label: search_label.to_string(),
            description: "Start the manager's built-in search for monitored missing items."
                .to_string(),
            kind: "primary".to_string(),
            params: None,
            confirm_text: None,
            navigate_extension_id: None,
            navigate_view: None,
            open_url: None,
            required_fields: Vec::new(),
            secret_keys: Vec::new(),
            secret_scope_instance_id: None,
        },
    ]
}

async fn load_manager_control_defaults(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<ManagerControlDefaults> {
    let key = control_defaults_setting_key(instance_id);
    let value = store.get_extension_setting(&key).await?;
    let mut defaults = ManagerControlDefaults::default();
    if let Some(object) = value.as_ref().and_then(serde_json::Value::as_object) {
        if let Some(value) = object
            .get("monitorOnAdd")
            .and_then(serde_json::Value::as_bool)
        {
            defaults.monitor_on_add = value;
        }
        if let Some(value) = object
            .get("searchOnAdd")
            .and_then(serde_json::Value::as_bool)
        {
            defaults.search_on_add = value;
        }
    }
    Ok(defaults)
}

async fn save_manager_control_defaults(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    values: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<()> {
    let key = control_defaults_setting_key(instance_id);
    let existing = store
        .get_extension_setting(&key)
        .await?
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let mut object = existing;
    let mut updated = false;

    for (field_id, value) in values {
        match field_id.as_str() {
            "monitorOnAdd" => {
                let bool_value = value
                    .as_bool()
                    .ok_or_else(|| anyhow::anyhow!("monitorOnAdd must be a boolean"))?;
                object.insert(field_id.clone(), serde_json::Value::Bool(bool_value));
                updated = true;
            }
            "searchOnAdd" => {
                let bool_value = value
                    .as_bool()
                    .ok_or_else(|| anyhow::anyhow!("searchOnAdd must be a boolean"))?;
                object.insert(field_id.clone(), serde_json::Value::Bool(bool_value));
                updated = true;
            }
            other => anyhow::bail!("unsupported control setting '{other}'"),
        }
    }

    if updated {
        store
            .upsert_extension_setting(&key, &serde_json::Value::Object(object))
            .await?;
    }

    Ok(())
}

fn build_arr_download_client_entity(client: &ArrControlDownloadClient) -> ExtensionControlEntity {
    let subtitle = Some(client.protocol.label().to_string());
    let mut details = vec![format!("Client priority {}", client.priority)];
    details.push(if client.enabled {
        "Enabled".to_string()
    } else {
        "Disabled".to_string()
    });
    if let Some(implementation) = client
        .implementation
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        details.push(format!("Implementation {implementation}"));
    }

    ExtensionControlEntity {
        id: client.id.to_string(),
        title: client.name.clone(),
        subtitle,
        details,
        actions: Vec::new(),
    }
}

fn arr_download_client_cache_entry_from_client(
    client: &ArrControlDownloadClient,
) -> ArrControlDownloadClientCacheEntry {
    ArrControlDownloadClientCacheEntry {
        id: client.id,
        name: client.name.clone(),
        implementation: client.implementation.clone(),
        protocol: client.protocol.as_setting_value().to_string(),
        priority: client.priority,
        enabled: client.enabled,
    }
}

fn arr_download_client_from_cache_entry(
    entry: ArrControlDownloadClientCacheEntry,
) -> ArrControlDownloadClient {
    ArrControlDownloadClient {
        id: entry.id,
        name: entry.name,
        implementation: entry.implementation,
        protocol: arr_download_client_protocol_from_str(&entry.protocol),
        priority: entry.priority,
        enabled: entry.enabled,
        raw: json!({}),
    }
}

fn arr_download_client_protocol_from_str(value: &str) -> ArrDownloadClientProtocol {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.contains("usenet")
        || normalized.contains("nzb")
        || normalized.contains("sab")
        || normalized.contains("newshost")
    {
        ArrDownloadClientProtocol::Usenet
    } else if normalized.contains("torrent")
        || normalized.contains("qbittorrent")
        || normalized.contains("transmission")
        || normalized.contains("deluge")
        || normalized.contains("utorrent")
        || normalized.contains("rtorrent")
        || normalized.contains("vuze")
        || normalized.contains("hadouken")
    {
        ArrDownloadClientProtocol::Torrent
    } else {
        ArrDownloadClientProtocol::Unknown
    }
}

fn arr_download_client_protocol_coverage(clients: &[ArrControlDownloadClient]) -> (bool, bool) {
    let mut has_usenet = false;
    let mut has_torrent = false;
    for client in clients.iter().filter(|client| client.enabled) {
        match client.protocol {
            ArrDownloadClientProtocol::Usenet => has_usenet = true,
            ArrDownloadClientProtocol::Torrent => has_torrent = true,
            ArrDownloadClientProtocol::Unknown => {}
        }
    }
    (has_usenet, has_torrent)
}

async fn load_cached_arr_control_download_clients(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<Vec<ArrControlDownloadClient>> {
    let Some(value) = store
        .get_extension_setting(&arr_download_client_cache_setting_key(instance_id))
        .await?
    else {
        return Ok(Vec::new());
    };
    let entries: Vec<ArrControlDownloadClientCacheEntry> =
        serde_json::from_value(value).context("parsing cached arr download clients")?;
    Ok(entries
        .into_iter()
        .map(arr_download_client_from_cache_entry)
        .collect())
}

async fn save_cached_arr_control_download_clients(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    clients: &[ArrControlDownloadClient],
) -> anyhow::Result<()> {
    let entries = clients
        .iter()
        .map(arr_download_client_cache_entry_from_client)
        .collect::<Vec<_>>();
    store
        .upsert_extension_setting(
            &arr_download_client_cache_setting_key(instance_id),
            &serde_json::to_value(entries)?,
        )
        .await?;
    Ok(())
}

async fn cached_arr_control_download_client_count(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<Option<usize>> {
    let clients = load_cached_arr_control_download_clients(store, instance_id).await?;
    if clients.is_empty() {
        Ok(None)
    } else {
        Ok(Some(clients.len()))
    }
}

fn infer_arr_download_client_preference(
    clients: &[ArrControlDownloadClient],
) -> ArrDownloadClientPreference {
    let usenet_priority = clients
        .iter()
        .filter(|client| client.enabled && client.protocol == ArrDownloadClientProtocol::Usenet)
        .map(|client| client.priority)
        .min();
    let torrent_priority = clients
        .iter()
        .filter(|client| client.enabled && client.protocol == ArrDownloadClientProtocol::Torrent)
        .map(|client| client.priority)
        .min();

    match (usenet_priority, torrent_priority) {
        (Some(_), None) => ArrDownloadClientPreference::Usenet,
        (None, Some(_)) => ArrDownloadClientPreference::Torrent,
        (Some(usenet), Some(torrent)) if usenet < torrent => ArrDownloadClientPreference::Usenet,
        (Some(usenet), Some(torrent)) if torrent < usenet => ArrDownloadClientPreference::Torrent,
        _ => ArrDownloadClientPreference::KeepCurrent,
    }
}

fn control_json_value_as_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        .or_else(|| {
            value
                .as_str()
                .and_then(|number| number.trim().parse::<i64>().ok())
        })
}

fn extract_control_json_field_i64(value: &serde_json::Value, name: &str) -> Option<i64> {
    value
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .and_then(|fields| {
            fields.iter().find_map(|field| {
                let field_name = field.get("name").and_then(serde_json::Value::as_str)?;
                if !field_name.eq_ignore_ascii_case(name) {
                    return None;
                }
                field
                    .get("value")
                    .and_then(control_json_value_as_i64)
                    .or_else(|| control_json_value_as_i64(field))
            })
        })
}

fn parse_arr_control_download_client(
    value: &serde_json::Value,
) -> Option<ArrControlDownloadClient> {
    let id = value.get("id").and_then(control_json_value_as_i64)?;
    let name = value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Download client")
        .to_string();
    let implementation = value
        .get("implementation")
        .or_else(|| value.get("implementationName"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let protocol = value
        .get("protocol")
        .and_then(serde_json::Value::as_str)
        .map(arr_download_client_protocol_from_str)
        .unwrap_or_else(|| {
            implementation
                .as_deref()
                .map(arr_download_client_protocol_from_str)
                .unwrap_or(ArrDownloadClientProtocol::Unknown)
        });
    let priority = value
        .get("priority")
        .and_then(control_json_value_as_i64)
        .or_else(|| extract_control_json_field_i64(value, "priority"))
        .unwrap_or(1);
    let enabled = value
        .get("enable")
        .or_else(|| value.get("enabled"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);

    Some(ArrControlDownloadClient {
        id,
        name,
        implementation,
        protocol,
        priority,
        enabled,
        raw: value.clone(),
    })
}

async fn load_arr_control_download_clients(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Vec<ArrControlDownloadClient>> {
    let (base_url, api_key) =
        resolve_extension_control_arr_connection(state, store, context).await?;
    let value = request_control_json(
        &base_url,
        &api_key,
        &["api/v3/downloadclient", "api/v4/downloadclient"],
    )
    .await?;
    let Some(items) = value.as_array() else {
        anyhow::bail!("download client inventory response was not an array");
    };

    Ok(items
        .iter()
        .filter_map(parse_arr_control_download_client)
        .collect())
}

async fn load_arr_control_download_client_detail(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    client_id: i64,
) -> anyhow::Result<serde_json::Value> {
    let (base_url, api_key) =
        resolve_extension_control_arr_connection(state, store, context).await?;
    request_control_json(
        &base_url,
        &api_key,
        &[
            &format!("api/v3/downloadclient/{client_id}"),
            &format!("api/v4/downloadclient/{client_id}"),
        ],
    )
    .await
}

async fn load_arr_control_download_clients_with_fallback(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<(Vec<ArrControlDownloadClient>, bool)> {
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    match tokio::time::timeout(
        Duration::from_secs(2),
        load_arr_control_download_clients(state, store, context),
    )
    .await
    {
        Ok(Ok(clients)) => {
            save_cached_arr_control_download_clients(store, instance.instance_id, &clients).await?;
            Ok((clients, true))
        }
        Ok(Err(err)) => {
            let cached =
                load_cached_arr_control_download_clients(store, instance.instance_id).await?;
            if cached.is_empty() {
                Err(err)
            } else {
                tracing::debug!(
                    "using cached arr download client inventory for {} after live load failed: {err}",
                    context.extension.extension_id
                );
                Ok((cached, false))
            }
        }
        Err(_) => {
            let cached =
                load_cached_arr_control_download_clients(store, instance.instance_id).await?;
            if cached.is_empty() {
                anyhow::bail!("manager download client inventory timed out")
            } else {
                tracing::debug!(
                    "using cached arr download client inventory for {} after live load timed out",
                    context.extension.extension_id
                );
                Ok((cached, false))
            }
        }
    }
}

fn set_arr_control_download_client_priority(
    value: &mut serde_json::Value,
    priority: i64,
) -> anyhow::Result<()> {
    let mut updated = false;

    if let Some(object) = value.as_object_mut() {
        if object.contains_key("priority") {
            object.insert("priority".to_string(), serde_json::Value::from(priority));
            updated = true;
        }
    }

    if let Some(fields) = value
        .get_mut("fields")
        .and_then(serde_json::Value::as_array_mut)
    {
        for field in fields {
            let field_name = field.get("name").and_then(serde_json::Value::as_str);
            if field_name != Some("priority") {
                continue;
            }
            if let Some(object) = field.as_object_mut() {
                object.insert("value".to_string(), serde_json::Value::from(priority));
                updated = true;
            }
        }
    }

    if !updated {
        anyhow::bail!("download client priority is unavailable for this manager")
    }

    Ok(())
}

fn plan_arr_download_client_priority_updates(
    clients: &[ArrControlDownloadClient],
    preference: ArrDownloadClientPreference,
) -> anyhow::Result<Vec<(i64, i64, serde_json::Value)>> {
    if preference == ArrDownloadClientPreference::KeepCurrent
        || infer_arr_download_client_preference(clients) == preference
    {
        return Ok(Vec::new());
    }

    let (has_usenet, has_torrent) = arr_download_client_protocol_coverage(clients);
    if !has_usenet || !has_torrent {
        anyhow::bail!(
            "add both a Usenet and a torrent client in the manager before changing protocol preference"
        );
    }

    let base_priority = clients
        .iter()
        .filter(|client| client.enabled)
        .map(|client| client.priority)
        .min()
        .unwrap_or(1);
    let mut updates = Vec::new();

    for client in clients.iter().filter(|client| client.enabled) {
        let target_priority = match (preference, client.protocol) {
            (ArrDownloadClientPreference::Usenet, ArrDownloadClientProtocol::Usenet)
            | (ArrDownloadClientPreference::Torrent, ArrDownloadClientProtocol::Torrent) => {
                base_priority
            }
            (ArrDownloadClientPreference::Usenet, ArrDownloadClientProtocol::Torrent)
            | (ArrDownloadClientPreference::Torrent, ArrDownloadClientProtocol::Usenet) => {
                base_priority + 1
            }
            (_, ArrDownloadClientProtocol::Unknown) => base_priority + 2,
            (ArrDownloadClientPreference::KeepCurrent, _) => client.priority,
        };
        if client.priority == target_priority {
            continue;
        }
        let mut raw = client.raw.clone();
        set_arr_control_download_client_priority(&mut raw, target_priority)?;
        updates.push((client.id, target_priority, raw));
    }

    Ok(updates)
}

async fn update_arr_download_client_preference(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    let raw_value = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("downloadClientPreference must be a string"))?;
    let preference = ArrDownloadClientPreference::from_value(raw_value).ok_or_else(|| {
        anyhow::anyhow!("downloadClientPreference must be one of current, usenet, or torrent")
    })?;
    let (base_url, api_key) =
        resolve_extension_control_arr_connection(state, store, context).await?;
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let clients = load_arr_control_download_clients(state, store, context).await?;
    save_cached_arr_control_download_clients(store, instance.instance_id, &clients).await?;
    let updates = plan_arr_download_client_priority_updates(&clients, preference)?;

    for (id, target_priority, _) in &updates {
        let mut detail =
            load_arr_control_download_client_detail(state, store, context, *id).await?;
        set_arr_control_download_client_priority(&mut detail, *target_priority)?;
        request_control_write(
            &base_url,
            &api_key,
            ReqwestMethod::PUT,
            &[
                &format!("api/v3/downloadclient/{id}"),
                &format!("api/v4/downloadclient/{id}"),
            ],
            Some(&detail),
        )
        .await?;
    }

    if !updates.is_empty() {
        let mut cached_clients = clients;
        for (id, target_priority, _) in updates {
            if let Some(client) = cached_clients.iter_mut().find(|client| client.id == id) {
                client.priority = target_priority;
            }
        }
        save_cached_arr_control_download_clients(store, instance.instance_id, &cached_clients)
            .await?;
    }

    Ok(())
}

fn build_extension_control_actions(
    context: &ExtensionControlContext,
) -> Vec<ExtensionControlAction> {
    control::build_actions(context)
}

async fn build_extension_control_open_service_ui_action(
    context: &ExtensionControlContext,
) -> Option<ExtensionControlAction> {
    let provider = context.selected_provider.as_ref()?;
    let implementation = provider.implementation.as_deref()?.to_ascii_lowercase();
    let (label, description) = match implementation.as_str() {
        "prowlarr" => (
            "Open Prowlarr UI",
            "Open the native Prowlarr UI for advanced or site-specific setup.",
        ),
        "sonarr" => (
            "Open Sonarr UI",
            "Open the native Sonarr UI for advanced manager setup.",
        ),
        "radarr" => (
            "Open Radarr UI",
            "Open the native Radarr UI for advanced manager setup.",
        ),
        "bazarr" => (
            "Open Bazarr UI",
            "Open the native Bazarr UI for advanced subtitle setup.",
        ),
        "qbittorrent" => (
            "Open qBittorrent UI",
            "Open the native qBittorrent UI for queue and transfer management.",
        ),
        "nzbget" => (
            "Open NZBGet UI",
            "Open the native NZBGet UI for queue and post-processing management.",
        ),
        _ => return None,
    };
    if !matches!(
        implementation.as_str(),
        "prowlarr" | "sonarr" | "radarr" | "bazarr" | "qbittorrent" | "nzbget"
    ) {
        return None;
    }
    let instance = context.selected_instance.as_ref()?;
    let start_path = format!(
        "/api/v1/extensions/instances/{}/ui/start",
        instance.instance_id
    );
    Some(ExtensionControlAction {
        id: "open_service_ui".to_string(),
        label: label.to_string(),
        description: description.to_string(),
        kind: "secondary".to_string(),
        params: None,
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: None,
        open_url: Some(start_path),
        required_fields: Vec::new(),
        secret_keys: Vec::new(),
        secret_scope_instance_id: None,
    })
}

fn extension_ui_proxy_prefix(instance_id: Uuid) -> String {
    format!("/api/v1/extensions/instances/{instance_id}/ui")
}

struct ExtensionUiProxyTarget {
    instance_id: Uuid,
    endpoint_url: String,
    base_url: String,
    proxy_prefix: String,
    host_header: Option<String>,
    upstream_auth: ExtensionUiUpstreamAuth,
}

enum ExtensionUiUpstreamAuth {
    None,
    ApiKey(String),
    BasicAuth { username: String, password: String },
    QbittorrentSession { username: String, password: String },
}

async fn resolve_extension_ui_proxy_target(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> anyhow::Result<ExtensionUiProxyTarget> {
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("extension instance not found"))?;
    if !instance.enabled {
        anyhow::bail!("extension instance is not enabled");
    }

    let providers = store.list_providers(Some(instance_id)).await?;
    let provider = providers
        .into_iter()
        .find(|provider| provider.endpoint_json.is_some())
        .ok_or_else(|| anyhow::anyhow!("extension instance does not expose a reachable service"))?;
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is not available"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url = resolve_control_provider_transport_base_url(instance_id, &endpoint).await?;
    let implementation = provider.implementation.unwrap_or_default();
    let implementation_key = implementation.trim().to_ascii_lowercase();
    let canonical_url = endpoint.canonical_url()?;
    let host_header = extension_ui_proxy_host_header(&canonical_url, &base_url);
    let upstream_auth = match implementation_key.as_str() {
        "prowlarr" => ExtensionUiUpstreamAuth::ApiKey(
            resolve_control_api_key(state, store, &instance, &["prowlarr_api_key", "api_key"])
                .await?,
        ),
        "sonarr" => ExtensionUiUpstreamAuth::ApiKey(
            resolve_control_api_key(state, store, &instance, &["sonarr_api_key", "api_key"])
                .await?,
        ),
        "radarr" => ExtensionUiUpstreamAuth::ApiKey(
            resolve_control_api_key(state, store, &instance, &["radarr_api_key", "api_key"])
                .await?,
        ),
        "bazarr" => ExtensionUiUpstreamAuth::ApiKey(
            resolve_control_api_key(state, store, &instance, &["bazarr_api_key", "api_key"])
                .await?,
        ),
        "nzbget" => ExtensionUiUpstreamAuth::BasicAuth {
            username: resolve_control_secret_value(
                state,
                store,
                &instance,
                &["nzbget_username", "username"],
            )
            .await?,
            password: resolve_control_secret_value(
                state,
                store,
                &instance,
                &["nzbget_password", "password"],
            )
            .await?,
        },
        "qbittorrent" => ExtensionUiUpstreamAuth::QbittorrentSession {
            username: resolve_control_secret_value(
                state,
                store,
                &instance,
                &["qbittorrent_username", "username"],
            )
            .await?,
            password: resolve_control_secret_value(
                state,
                store,
                &instance,
                &["qbittorrent_password", "password"],
            )
            .await?,
        },
        _ => ExtensionUiUpstreamAuth::None,
    };

    Ok(ExtensionUiProxyTarget {
        instance_id,
        endpoint_url: canonical_url,
        base_url,
        proxy_prefix: extension_ui_proxy_prefix(instance_id),
        host_header,
        upstream_auth,
    })
}

async fn proxy_extension_ui_impl(
    state: &AppState,
    _user: CurrentUser,
    instance_id: Uuid,
    path: String,
    method: Method,
    headers: AxumHeaderMap,
    original_uri: axum::http::Uri,
    body: Bytes,
) -> ApiResult<Response> {
    let store = ExtensionStore::new(&state.db_pool);
    let target = resolve_extension_ui_proxy_target(state, &store, instance_id)
        .await
        .map_err(ApiError::from)?;
    let upstream_url = build_extension_ui_proxy_url(&target.base_url, &path, original_uri.query())
        .map_err(ApiError::from)?;
    let client = build_extension_ui_proxy_client().map_err(ApiError::from)?;
    let reqwest_method = ReqwestMethod::from_bytes(method.as_str().as_bytes())
        .map_err(anyhow::Error::from)
        .map_err(ApiError::from)?;
    let mut request = build_extension_ui_upstream_request(
        &client,
        &target,
        reqwest_method,
        upstream_url,
        &headers,
    )
    .await
    .map_err(ApiError::from)?;
    if !body.is_empty() && method != Method::GET && method != Method::HEAD {
        request = request.body(body.to_vec());
    }

    let upstream = request
        .send()
        .await
        .map_err(anyhow::Error::from)
        .map_err(ApiError::from)?;
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let mut response_headers = AxumHeaderMap::new();
    copy_extension_ui_response_headers(
        &upstream_headers,
        &mut response_headers,
        &target.base_url,
        &target.proxy_prefix,
    );

    let content_type = upstream_headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body_bytes = upstream
        .bytes()
        .await
        .map_err(anyhow::Error::from)
        .map_err(ApiError::from)?;
    let normalized_path = path.trim_matches('/');
    let response_bytes = if content_type.to_ascii_lowercase().contains("text/html") {
        rewrite_extension_ui_html(&body_bytes, &target.proxy_prefix).into_bytes()
    } else if normalized_path.eq_ignore_ascii_case("initialize.json") {
        rewrite_extension_ui_initialize_json(&body_bytes, &target.proxy_prefix).into_bytes()
    } else {
        body_bytes.to_vec()
    };

    let mut response = Response::builder()
        .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY))
        .body(axum::body::Body::from(response_bytes))
        .map_err(|err| ApiError::internal(err.to_string()))?;
    *response.headers_mut() = response_headers;
    Ok(response)
}

pub(crate) async fn request_instance_service_json(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    method: ReqwestMethod,
    path: &str,
    body: Option<Value>,
) -> anyhow::Result<Value> {
    let target = resolve_extension_ui_proxy_target(state, store, instance_id).await?;
    let client = build_extension_ui_proxy_client()?;
    let upstream_url = build_extension_ui_proxy_url(&target.base_url, path, None)?;
    let mut request = build_extension_ui_upstream_request(
        &client,
        &target,
        method.clone(),
        upstream_url,
        &AxumHeaderMap::new(),
    )
    .await?;
    if let Some(body) = body {
        request = request.json(&body);
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("sending service {} {path}", method.as_str()))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading service {} {path} response", method.as_str()))?;
    if !status.is_success() {
        let detail = describe_service_response_body(&bytes);
        anyhow::bail!(
            "service {} {path} failed ({}): {detail}",
            method.as_str(),
            status
        );
    }
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing service {} {path} response", method.as_str()))
}

pub(crate) async fn request_instance_service_form(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    path: &str,
    fields: &std::collections::HashMap<String, String>,
) -> anyhow::Result<()> {
    let target = resolve_extension_ui_proxy_target(state, store, instance_id).await?;
    let client = build_extension_ui_proxy_client()?;
    let upstream_url = build_extension_ui_proxy_url(&target.base_url, path, None)?;
    let request = build_extension_ui_upstream_request(
        &client,
        &target,
        ReqwestMethod::POST,
        upstream_url,
        &AxumHeaderMap::new(),
    )
    .await?;

    let response = request
        .form(fields)
        .send()
        .await
        .with_context(|| format!("sending service POST {path}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading service POST {path} response"))?;
    if !status.is_success() {
        let detail = describe_service_response_body(&bytes);
        anyhow::bail!("service POST {path} failed ({status}): {detail}");
    }
    Ok(())
}

fn build_extension_ui_proxy_url(
    base_url: &str,
    path: &str,
    query: Option<&str>,
) -> anyhow::Result<Url> {
    let normalized_path = path.trim_matches('/');
    let upstream_path = if normalized_path.is_empty() {
        "/".to_string()
    } else {
        format!("/{normalized_path}")
    };
    let mut url = build_extension_control_url(base_url, &upstream_path)?;
    url.set_query(query);
    Ok(url)
}

fn describe_service_response_body(body: &[u8]) -> String {
    match std::str::from_utf8(body) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => "<empty response>".to_string(),
    }
}

fn build_extension_ui_proxy_client() -> anyhow::Result<Client> {
    let mut headers = ReqwestHeaderMap::new();
    headers.insert(USER_AGENT, ReqwestHeaderValue::from_static("Elixir/1.0"));
    Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .default_headers(headers)
        .build()
        .map_err(anyhow::Error::from)
}

async fn build_extension_ui_upstream_request(
    client: &Client,
    target: &ExtensionUiProxyTarget,
    method: ReqwestMethod,
    upstream_url: Url,
    headers: &AxumHeaderMap,
) -> anyhow::Result<reqwest::RequestBuilder> {
    let mut request =
        client
            .request(method, upstream_url.clone())
            .headers(build_extension_ui_request_headers(
                headers,
                target,
                &upstream_url,
            ));
    if let Some(host_header) = target.host_header.as_deref() {
        request = request.header(
            REQWEST_HOST,
            ReqwestHeaderValue::from_str(host_header).map_err(anyhow::Error::from)?,
        );
    }
    match &target.upstream_auth {
        ExtensionUiUpstreamAuth::None => {}
        ExtensionUiUpstreamAuth::ApiKey(api_key) => {
            request = request.header(
                "X-Api-Key",
                ReqwestHeaderValue::from_str(api_key).map_err(anyhow::Error::from)?,
            );
        }
        ExtensionUiUpstreamAuth::BasicAuth { username, password } => {
            request = request.basic_auth(username, Some(password));
        }
        ExtensionUiUpstreamAuth::QbittorrentSession { username, password } => {
            let cookie = authenticate_qbittorrent_ui_session(target, username, password).await?;
            request = request.header(
                REQWEST_COOKIE,
                ReqwestHeaderValue::from_str(&cookie).map_err(anyhow::Error::from)?,
            );
        }
    }
    Ok(request)
}

async fn authenticate_qbittorrent_ui_session(
    target: &ExtensionUiProxyTarget,
    username: &str,
    password: &str,
) -> anyhow::Result<String> {
    bootstrap_qbittorrent_session_cookie(
        &target.endpoint_url,
        Some(&target.base_url),
        target.instance_id,
        username,
        password,
    )
    .await
    .context("authenticating qbittorrent ui session")
}

fn extension_ui_proxy_host_header(canonical_url: &str, transport_url: &str) -> Option<String> {
    let Ok(canonical) = Url::parse(canonical_url) else {
        return None;
    };
    let Ok(transport) = Url::parse(transport_url) else {
        return None;
    };
    let canonical_host = canonical.host_str()?;
    let canonical_port = canonical.port_or_known_default().unwrap_or(80);
    let transport_host = transport.host_str()?;
    let transport_port = transport.port_or_known_default().unwrap_or(80);
    if canonical_host.eq_ignore_ascii_case(transport_host) && canonical_port == transport_port {
        return None;
    }
    Some(format!("{canonical_host}:{canonical_port}"))
}

fn build_extension_ui_request_headers(
    source: &AxumHeaderMap,
    target: &ExtensionUiProxyTarget,
    upstream_url: &Url,
) -> ReqwestHeaderMap {
    let mut headers = ReqwestHeaderMap::new();
    const FORWARDED_HEADERS: &[&str] = &[
        "accept",
        "accept-language",
        "content-type",
        "user-agent",
        "x-requested-with",
    ];

    for key in FORWARDED_HEADERS {
        if let Some(value) = source.get(*key) {
            if let Ok(value) = ReqwestHeaderValue::from_bytes(value.as_bytes()) {
                headers.insert(*key, value);
            }
        }
    }

    let upstream_origin = extension_ui_upstream_origin(target, upstream_url);
    if source.contains_key("origin") {
        if let Some(origin) = upstream_origin.as_deref() {
            if let Ok(value) = ReqwestHeaderValue::from_str(origin) {
                headers.insert("origin", value);
            }
        }
    }
    if source.contains_key("referer") {
        if let Some(referer) = extension_ui_upstream_referer(target, upstream_url).as_deref() {
            if let Ok(value) = ReqwestHeaderValue::from_str(referer) {
                headers.insert("referer", value);
            }
        }
    }

    headers
}

fn extension_ui_upstream_origin(
    target: &ExtensionUiProxyTarget,
    upstream_url: &Url,
) -> Option<String> {
    let authority = if let Some(host_header) = target.host_header.as_deref() {
        host_header.to_string()
    } else {
        let host = upstream_url.host_str()?.to_string();
        match upstream_url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host,
        }
    };
    Some(format!("{}://{}", upstream_url.scheme(), authority))
}

fn extension_ui_upstream_referer(
    target: &ExtensionUiProxyTarget,
    upstream_url: &Url,
) -> Option<String> {
    let mut referer = Url::parse(&extension_ui_upstream_origin(target, upstream_url)?).ok()?;
    referer.set_path("/");
    referer.set_query(None);
    referer.set_fragment(None);
    Some(referer.to_string())
}

fn copy_extension_ui_response_headers(
    source: &ReqwestHeaderMap,
    target: &mut AxumHeaderMap,
    base_url: &str,
    proxy_prefix: &str,
) {
    for header_name in [
        reqwest::header::CACHE_CONTROL,
        reqwest::header::CONTENT_TYPE,
        reqwest::header::ETAG,
        reqwest::header::LAST_MODIFIED,
    ] {
        if let Some(value) = source.get(&header_name) {
            if let (Ok(name), Ok(value)) = (
                AxumHeaderName::from_bytes(header_name.as_str().as_bytes()),
                AxumHeaderValue::from_bytes(value.as_bytes()),
            ) {
                target.insert(name, value);
            }
        }
    }

    if let Some(location) = source
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
    {
        let rewritten = rewrite_extension_ui_location(location, base_url, proxy_prefix);
        if let Ok(value) = AxumHeaderValue::from_str(&rewritten) {
            target.insert(axum_header::LOCATION, value);
        }
    }
}

fn rewrite_extension_ui_location(location: &str, base_url: &str, proxy_prefix: &str) -> String {
    if location.starts_with(proxy_prefix) {
        return location.to_string();
    }
    if let Some(rest) = location.strip_prefix(base_url) {
        let rest = rest.trim_start_matches('/');
        return if rest.is_empty() {
            proxy_prefix.to_string()
        } else {
            format!("{proxy_prefix}/{rest}")
        };
    }
    if let Some(rest) = location.strip_prefix('/') {
        return if rest.is_empty() {
            proxy_prefix.to_string()
        } else {
            format!("{proxy_prefix}/{rest}")
        };
    }
    location.to_string()
}

fn rewrite_extension_ui_html(bytes: &[u8], proxy_prefix: &str) -> String {
    let html = String::from_utf8_lossy(bytes).into_owned();
    let prefix = format!("{proxy_prefix}/");
    html.replace("urlBase: ''", &format!("urlBase: '{proxy_prefix}'"))
        .replace("urlBase:\"\"", &format!("urlBase:\"{proxy_prefix}\""))
        .replace("href=\"/", &format!("href=\"{prefix}"))
        .replace("src=\"/", &format!("src=\"{prefix}"))
        .replace("action=\"/", &format!("action=\"{prefix}"))
        .replace("content=\"/", &format!("content=\"{prefix}"))
        .replace("href='/", &format!("href='{prefix}"))
        .replace("src='/", &format!("src='{prefix}"))
        .replace("action='/", &format!("action='{prefix}"))
        .replace("content='/", &format!("content='{prefix}"))
}

fn rewrite_extension_ui_initialize_json(bytes: &[u8], proxy_prefix: &str) -> String {
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return String::from_utf8_lossy(bytes).into_owned();
    };
    let Some(object) = value.as_object_mut() else {
        return String::from_utf8_lossy(bytes).into_owned();
    };

    object.insert(
        "urlBase".to_string(),
        serde_json::Value::String(proxy_prefix.to_string()),
    );

    if let Some(api_root) = object.get("apiRoot").and_then(|value| value.as_str()) {
        let rewritten_api_root = if api_root.starts_with(proxy_prefix) {
            api_root.to_string()
        } else if api_root.starts_with('/') {
            format!("{proxy_prefix}{api_root}")
        } else {
            format!("{proxy_prefix}/{api_root}")
        };
        object.insert(
            "apiRoot".to_string(),
            serde_json::Value::String(rewritten_api_root),
        );
    }

    serde_json::to_string(&value).unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned())
}

#[cfg(test)]
mod extension_ui_proxy_tests {
    use super::{
        ExtensionControlBinding, ExtensionControlContext, ExtensionStatusSummaryItem,
        ExtensionUiProxyTarget, ExtensionUiUpstreamAuth,
        build_extension_control_open_service_ui_action, build_extension_ui_proxy_client,
        build_extension_ui_start_html, build_extension_ui_upstream_request,
        control_transport_container_candidates, parse_control_published_host_port,
        rewrite_extension_ui_initialize_json,
    };
    use crate::db::models::{
        Extension, ExtensionInstance, ExtensionKind, ExtensionTrustLevel, Provider,
        ProviderHealthState, SlotCardinality,
    };
    use crate::extensions::manifest::ExtensionManifest;
    use axum::{Router, http::HeaderMap as AxumHeaderMap, routing::post};
    use chrono::Utc;
    use reqwest::{Method as ReqwestMethod, Url};
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    #[test]
    fn start_html_bootstrap_targets_proxy_prefix() {
        let html = build_extension_ui_start_html("/api/v1/extensions/instances/test-instance/ui");
        assert!(html.contains("Opening extension UI"));
        assert!(html.contains("serviceWorker"));
        assert!(html.contains("caches.keys()"));
        assert!(html.contains("localStorage.clear()"));
        assert!(html.contains("indexedDB.deleteDatabase"));
        assert!(html.contains("document.cookie"));
        assert!(html.contains("const targetUrl = scopePrefix;"));
        assert!(!html.contains("_elixir_ui"));
        assert!(html.contains("/api/v1/extensions/instances/test-instance/ui"));
    }

    #[test]
    fn rewrites_initialize_json_to_proxy_prefix() {
        let body = br#"{
            "apiRoot": "/api/v1",
            "urlBase": "",
            "instanceName": "Prowlarr"
        }"#;

        let rewritten = rewrite_extension_ui_initialize_json(
            body,
            "/api/v1/extensions/instances/test-instance/ui",
        );
        let value: Value = serde_json::from_str(&rewritten).expect("valid rewritten json");

        assert_eq!(
            value.get("urlBase").and_then(Value::as_str),
            Some("/api/v1/extensions/instances/test-instance/ui")
        );
        assert_eq!(
            value.get("apiRoot").and_then(Value::as_str),
            Some("/api/v1/extensions/instances/test-instance/ui/api/v1")
        );
    }

    #[test]
    fn control_transport_candidates_try_app_before_warp_gateway() {
        let candidates = control_transport_container_candidates(
            "elx-ba4bf0-vpn\nelx-ba4bf0\nelx-ba4bf0-vpn-rollback\n",
        );
        assert_eq!(
            candidates,
            vec![
                "elx-ba4bf0".to_string(),
                "elx-ba4bf0-vpn".to_string(),
                "elx-ba4bf0-vpn-rollback".to_string(),
            ]
        );
    }

    #[test]
    fn parses_control_published_host_port() {
        let ports = r#"{"8080/tcp":[{"HostIp":"0.0.0.0","HostPort":"32801"}]}"#;
        assert_eq!(parse_control_published_host_port(ports, 8080), Some(32801));
        assert_eq!(parse_control_published_host_port(ports, 6789), None);
    }

    #[test]
    fn upstream_request_adds_basic_auth_header() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let client = build_extension_ui_proxy_client().expect("client");
            let target = ExtensionUiProxyTarget {
                instance_id: Uuid::new_v4(),
                endpoint_url: "http://127.0.0.1:1234/".to_string(),
                base_url: "http://127.0.0.1:1234/".to_string(),
                proxy_prefix: "/api/v1/extensions/instances/test/ui".to_string(),
                host_header: None,
                upstream_auth: ExtensionUiUpstreamAuth::BasicAuth {
                    username: "elixir".to_string(),
                    password: "secret".to_string(),
                },
            };

            let request = build_extension_ui_upstream_request(
                &client,
                &target,
                ReqwestMethod::GET,
                Url::parse("http://127.0.0.1:1234/").expect("url"),
                &AxumHeaderMap::new(),
            )
            .await
            .expect("request")
            .build()
            .expect("built request");

            let authorization = request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok());
            assert_eq!(authorization, Some("Basic ZWxpeGlyOnNlY3JldA=="));
        });
    }

    #[test]
    fn upstream_request_rewrites_origin_and_referer_to_upstream_origin() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let client = build_extension_ui_proxy_client().expect("client");
            let target = ExtensionUiProxyTarget {
                instance_id: Uuid::new_v4(),
                endpoint_url: "http://svc-elixir-modules-qbittorrent-default:8080/".to_string(),
                base_url: "http://127.0.0.1:32836/".to_string(),
                proxy_prefix: "/api/v1/extensions/instances/test/ui".to_string(),
                host_header: Some("svc-elixir-modules-qbittorrent-default:8080".to_string()),
                upstream_auth: ExtensionUiUpstreamAuth::None,
            };
            let mut headers = AxumHeaderMap::new();
            headers.insert(
                axum::http::header::ORIGIN,
                "http://ryans-macbook-pro.local:44301"
                    .parse()
                    .expect("origin header"),
            );
            headers.insert(
                axum::http::header::REFERER,
                "http://ryans-macbook-pro.local:44301/api/v1/extensions/instances/test/ui/start"
                    .parse()
                    .expect("referer header"),
            );

            let request = build_extension_ui_upstream_request(
                &client,
                &target,
                ReqwestMethod::GET,
                Url::parse("http://127.0.0.1:32836/").expect("url"),
                &headers,
            )
            .await
            .expect("request")
            .build()
            .expect("built request");

            let origin = request
                .headers()
                .get(reqwest::header::ORIGIN)
                .and_then(|value| value.to_str().ok());
            let referer = request
                .headers()
                .get(reqwest::header::REFERER)
                .and_then(|value| value.to_str().ok());
            assert_eq!(
                origin,
                Some("http://svc-elixir-modules-qbittorrent-default:8080")
            );
            assert_eq!(
                referer,
                Some("http://svc-elixir-modules-qbittorrent-default:8080/")
            );
        });
    }

    #[tokio::test]
    async fn upstream_request_bootstraps_qbittorrent_cookie() {
        async fn login() -> impl axum::response::IntoResponse {
            (
                [(axum::http::header::SET_COOKIE, "SID=test-cookie; HttpOnly")],
                "Ok.",
            )
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("local addr");
        let app = Router::new().route("/api/v2/auth/login", post(login));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let client = build_extension_ui_proxy_client().expect("client");
        let target = ExtensionUiProxyTarget {
            instance_id: Uuid::new_v4(),
            endpoint_url: format!("http://elx-qbittorrent:8080/"),
            base_url: format!("http://127.0.0.1:{}/", addr.port()),
            proxy_prefix: "/api/v1/extensions/instances/test/ui".to_string(),
            host_header: Some("elx-qbittorrent:8080".to_string()),
            upstream_auth: ExtensionUiUpstreamAuth::QbittorrentSession {
                username: "elixir".to_string(),
                password: "secret".to_string(),
            },
        };

        let request = build_extension_ui_upstream_request(
            &client,
            &target,
            ReqwestMethod::GET,
            Url::parse(&format!("http://127.0.0.1:{}/", addr.port())).expect("url"),
            &AxumHeaderMap::new(),
        )
        .await
        .expect("request")
        .build()
        .expect("built request");

        let cookie = request
            .headers()
            .get(reqwest::header::COOKIE)
            .and_then(|value| value.to_str().ok());
        let host = request
            .headers()
            .get(reqwest::header::HOST)
            .and_then(|value| value.to_str().ok());
        assert_eq!(cookie, Some("SID=test-cookie"));
        assert_eq!(host, Some("elx-qbittorrent:8080"));

        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn open_service_ui_action_supports_sonarr_and_qbittorrent() {
        fn context_for(
            extension_id: &str,
            extension_name: &str,
            implementation: &str,
        ) -> ExtensionControlContext {
            let instance_id = Uuid::new_v4();
            let manifest = ExtensionManifest {
                id: extension_id.to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                name: extension_name.to_string(),
                description: None,
                publisher: None,
                trust: None,
                permissions: Vec::new(),
                provides: Vec::new(),
                requires: Default::default(),
                conflicts: Vec::new(),
                runtime: None,
                backup: None,
                targets: Vec::new(),
                actions: Vec::new(),
                connectors: Vec::new(),
                optional_addons: Vec::new(),
                wants: Vec::new(),
                preferences: None,
                bindings: Vec::new(),
                execution: None,
                policies: None,
                networking: None,
                control_surface: None,
            };
            ExtensionControlContext {
                extension: Extension {
                    extension_id: extension_id.to_string(),
                    name: extension_name.to_string(),
                    version: "1.0.0".to_string(),
                    kind: ExtensionKind::Module,
                    publisher_name: None,
                    signing_key_id: None,
                    trust_level: ExtensionTrustLevel::Community,
                    manifest_json: serde_json::json!({}),
                    package_hash: None,
                    installed_at: Utc::now(),
                    enabled: true,
                },
                manifest,
                summary: ExtensionStatusSummaryItem {
                    extension_id: extension_id.to_string(),
                    name: extension_name.to_string(),
                    version: "1.0.0".to_string(),
                    kind: ExtensionKind::Module,
                    trust_level: ExtensionTrustLevel::Community,
                    enabled: true,
                    severity: "ready".to_string(),
                    status_code: "ready".to_string(),
                    label: "Ready".to_string(),
                    description: "Ready".to_string(),
                    primary_action: "open".to_string(),
                    primary_action_label: "Open".to_string(),
                    auto_update: None,
                    optional_addons: Vec::new(),
                },
                instances: vec![ExtensionInstance {
                    instance_id,
                    extension_id: extension_id.to_string(),
                    instance_name: "default".to_string(),
                    config_json: None,
                    runtime_version: None,
                    rollback_version: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    enabled: true,
                }],
                selected_instance: Some(ExtensionInstance {
                    instance_id,
                    extension_id: extension_id.to_string(),
                    instance_name: "default".to_string(),
                    config_json: None,
                    runtime_version: None,
                    rollback_version: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    enabled: true,
                }),
                providers: vec![Provider {
                    provider_id: Uuid::new_v4(),
                    instance_id,
                    capability: "service".to_string(),
                    slot_id: "default".to_string(),
                    cardinality: SlotCardinality::One,
                    implementation: Some(implementation.to_string()),
                    scope_json: None,
                    endpoint_json: None,
                    health_state: ProviderHealthState::Healthy,
                    last_healthcheck_at: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                }],
                selected_provider: Some(Provider {
                    provider_id: Uuid::new_v4(),
                    instance_id,
                    capability: "service".to_string(),
                    slot_id: "default".to_string(),
                    cardinality: SlotCardinality::One,
                    implementation: Some(implementation.to_string()),
                    scope_json: None,
                    endpoint_json: None,
                    health_state: ProviderHealthState::Healthy,
                    last_healthcheck_at: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                }),
                control_binding: ExtensionControlBinding::Unsupported,
            }
        }

        let sonarr_action = build_extension_control_open_service_ui_action(&context_for(
            "elixir.modules.sonarr",
            "Sonarr",
            "sonarr",
        ))
        .await
        .expect("sonarr action");
        assert_eq!(sonarr_action.label, "Open Sonarr UI");
        assert!(
            sonarr_action
                .open_url
                .unwrap_or_default()
                .ends_with("/ui/start")
        );

        let qbittorrent_action = build_extension_control_open_service_ui_action(&context_for(
            "elixir.modules.qbittorrent",
            "qBittorrent",
            "qbittorrent",
        ))
        .await
        .expect("qbittorrent action");
        assert_eq!(qbittorrent_action.label, "Open qBittorrent UI");
        assert!(
            qbittorrent_action
                .open_url
                .unwrap_or_default()
                .ends_with("/ui/start")
        );
    }
}

fn control_text_field(
    id: &str,
    label: &str,
    description: &str,
    value: String,
) -> ExtensionControlField {
    ExtensionControlField {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: "text".to_string(),
        value: serde_json::Value::String(value),
        required: false,
        readonly: true,
        secret: false,
        options: Vec::new(),
        validation: None,
    }
}

async fn execute_extension_control_action(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension_id: &str,
    action_id: &str,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<String> {
    let context = load_extension_control_context(state, store, extension_id).await?;
    control::execute_action(state, store, &context, action_id, params).await
}

async fn load_cached_registry_entry_by_extension_id(
    state: &AppState,
    extension_id: &str,
) -> anyhow::Result<Option<RegistryEntry>> {
    let storage_paths = ExtensionStoragePaths::new(&state.settings.extensions.storage_root);
    let cache_store = RegistryCacheStore::new(storage_paths.registry_cache_dir.clone());
    let cache = cache_store.load().await?;
    Ok(cache.and_then(|cache| {
        cache
            .index
            .extensions
            .into_iter()
            .filter(|entry| entry.id == extension_id)
            .max_by(|left, right| {
                Version::parse(&left.version)
                    .ok()
                    .cmp(&Version::parse(&right.version).ok())
            })
    }))
}

async fn resolve_extension_control_arr_connection(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<(String, String)> {
    let provider = context
        .selected_provider
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active provider is available for this extension yet"))?;
    let instance = context
        .selected_instance
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no active instance is available for this extension yet"))?;
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url =
        resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?;
    let implementation = provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let candidate_keys = match implementation.as_str() {
        "sonarr" => vec!["sonarr_api_key", "api_key"],
        "radarr" => vec!["radarr_api_key", "api_key"],
        "prowlarr" => vec!["prowlarr_api_key", "api_key"],
        _ => vec!["api_key"],
    };
    let api_key = resolve_control_api_key(state, store, instance, &candidate_keys).await?;
    Ok((base_url, api_key))
}

async fn resolve_extension_control_intent(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    params: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<ManagedIngestIntent> {
    let intent_id = params
        .get("intentId")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("intentId is required"))?;
    let intent_id = Uuid::parse_str(intent_id).context("parsing intentId")?;
    let intents = store.list_active_managed_ingest_intents().await?;
    intents
        .into_iter()
        .find(|intent| intent.intent_id == intent_id && intent.manager_provider_id == provider_id)
        .ok_or_else(|| anyhow::anyhow!("managed item is no longer available"))
}

async fn execute_extension_control_manager_command(
    implementation: &str,
    base_url: &str,
    api_key: &str,
    action_id: &str,
    item_id: Option<i64>,
) -> anyhow::Result<String> {
    match (implementation, action_id) {
        ("sonarr", "search_missing") => {
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "MissingEpisodeSearch" })),
            )
            .await?;
            Ok("Sonarr started a missing episode search.".to_string())
        }
        ("sonarr", "refresh_manager") => {
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "RefreshSeries" })),
            )
            .await?;
            Ok("Sonarr refresh started.".to_string())
        }
        ("sonarr", "search_item") => {
            let item_id = item_id.ok_or_else(|| anyhow::anyhow!("manager item id is required"))?;
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "SeriesSearch", "seriesId": item_id })),
            )
            .await?;
            Ok("Sonarr started a search for this series.".to_string())
        }
        ("sonarr", "refresh_item") => {
            let item_id = item_id.ok_or_else(|| anyhow::anyhow!("manager item id is required"))?;
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "RefreshSeries", "seriesId": item_id })),
            )
            .await?;
            Ok("Sonarr refresh started for this series.".to_string())
        }
        ("sonarr", "remove_item") => {
            let item_id = item_id.ok_or_else(|| anyhow::anyhow!("manager item id is required"))?;
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::DELETE,
                &[
                    &format!("api/v3/series/{item_id}?deleteFiles=false"),
                    &format!("api/v4/series/{item_id}?deleteFiles=false"),
                ],
                None,
            )
            .await?;
            Ok("Sonarr removed this series.".to_string())
        }
        ("radarr", "search_missing") => {
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "MissingMoviesSearch" })),
            )
            .await?;
            Ok("Radarr started a missing movie search.".to_string())
        }
        ("radarr", "refresh_manager") => {
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "RefreshMovie" })),
            )
            .await?;
            Ok("Radarr refresh started.".to_string())
        }
        ("radarr", "search_item") => {
            let item_id = item_id.ok_or_else(|| anyhow::anyhow!("manager item id is required"))?;
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "MoviesSearch", "movieIds": [item_id] })),
            )
            .await?;
            Ok("Radarr started a search for this movie.".to_string())
        }
        ("radarr", "refresh_item") => {
            let item_id = item_id.ok_or_else(|| anyhow::anyhow!("manager item id is required"))?;
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::POST,
                &["api/v3/command", "api/v4/command"],
                Some(&json!({ "name": "RefreshMovie", "movieId": item_id })),
            )
            .await?;
            Ok("Radarr refresh started for this movie.".to_string())
        }
        ("radarr", "remove_item") => {
            let item_id = item_id.ok_or_else(|| anyhow::anyhow!("manager item id is required"))?;
            request_control_write(
                base_url,
                api_key,
                ReqwestMethod::DELETE,
                &[
                    &format!("api/v3/movie/{item_id}?deleteFiles=false"),
                    &format!("api/v4/movie/{item_id}?deleteFiles=false"),
                ],
                None,
            )
            .await?;
            Ok("Radarr removed this movie.".to_string())
        }
        _ => anyhow::bail!("unsupported control action '{action_id}' for {implementation}"),
    }
}

pub(crate) async fn remove_managed_library_item_from_manager(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &crate::db::models::Provider,
    manager_item_id: i64,
) -> anyhow::Result<String> {
    let instance = store
        .get_instance(provider.instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("manager instance is no longer available"))?;
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url =
        resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?;
    let implementation = provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let candidate_keys = match implementation.as_str() {
        "sonarr" => vec!["sonarr_api_key", "api_key"],
        "radarr" => vec!["radarr_api_key", "api_key"],
        _ => anyhow::bail!("stop tracking is not supported for {}", implementation),
    };
    let api_key = resolve_control_api_key(state, store, &instance, &candidate_keys).await?;
    execute_extension_control_manager_command(
        implementation.as_str(),
        &base_url,
        &api_key,
        "remove_item",
        Some(manager_item_id),
    )
    .await
}

async fn load_sonarr_control_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base_url: &str,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let api_key =
        resolve_control_api_key(state, store, instance, &["sonarr_api_key", "api_key"]).await?;
    let status = request_control_json(
        base_url,
        &api_key,
        &["api/v3/system/status", "api/v4/system/status"],
    )
    .await?;
    let series =
        request_control_json(base_url, &api_key, &["api/v3/series", "api/v4/series"]).await?;
    let mut metrics = vec![
        ExtensionControlMetric {
            id: "version".to_string(),
            label: "Service version".to_string(),
            value: status
                .get("version")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Unknown")
                .to_string(),
        },
        ExtensionControlMetric {
            id: "seriesCount".to_string(),
            label: "Series".to_string(),
            value: series
                .as_array()
                .map(|value| value.len())
                .unwrap_or(0)
                .to_string(),
        },
    ];
    if let Some(download_client_count) =
        cached_arr_control_download_client_count(store, instance.instance_id).await?
    {
        metrics.push(ExtensionControlMetric {
            id: "downloadClientCount".to_string(),
            label: "Download clients".to_string(),
            value: download_client_count.to_string(),
        });
    }

    Ok(ExtensionControlLiveSnapshot {
        version: status
            .get("version")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        metrics,
    })
}

async fn load_radarr_control_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base_url: &str,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let api_key =
        resolve_control_api_key(state, store, instance, &["radarr_api_key", "api_key"]).await?;
    let status = request_control_json(
        base_url,
        &api_key,
        &["api/v3/system/status", "api/v4/system/status"],
    )
    .await?;
    let movies =
        request_control_json(base_url, &api_key, &["api/v3/movie", "api/v4/movie"]).await?;
    let mut metrics = vec![
        ExtensionControlMetric {
            id: "version".to_string(),
            label: "Service version".to_string(),
            value: status
                .get("version")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Unknown")
                .to_string(),
        },
        ExtensionControlMetric {
            id: "movieCount".to_string(),
            label: "Movies".to_string(),
            value: movies
                .as_array()
                .map(|value| value.len())
                .unwrap_or(0)
                .to_string(),
        },
    ];
    if let Some(download_client_count) =
        cached_arr_control_download_client_count(store, instance.instance_id).await?
    {
        metrics.push(ExtensionControlMetric {
            id: "downloadClientCount".to_string(),
            label: "Download clients".to_string(),
            value: download_client_count.to_string(),
        });
    }

    Ok(ExtensionControlLiveSnapshot {
        version: status
            .get("version")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        metrics,
    })
}

async fn load_prowlarr_control_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base_url: &str,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let api_key =
        resolve_control_api_key(state, store, instance, &["prowlarr_api_key", "api_key"]).await?;
    let status = request_control_json(base_url, &api_key, &["api/v1/system/status"]).await?;
    let indexers = request_control_json(base_url, &api_key, &["api/v1/indexer"]).await?;
    let applications = request_control_json(base_url, &api_key, &["api/v1/applications"]).await?;

    Ok(ExtensionControlLiveSnapshot {
        version: status
            .get("version")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        metrics: vec![
            ExtensionControlMetric {
                id: "version".to_string(),
                label: "Service version".to_string(),
                value: status
                    .get("version")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Unknown")
                    .to_string(),
            },
            ExtensionControlMetric {
                id: "indexerCount".to_string(),
                label: "Indexers".to_string(),
                value: indexers
                    .as_array()
                    .map(|value| value.len())
                    .unwrap_or(0)
                    .to_string(),
            },
            ExtensionControlMetric {
                id: "appCount".to_string(),
                label: "Connected apps".to_string(),
                value: applications
                    .as_array()
                    .map(|value| value.len())
                    .unwrap_or(0)
                    .to_string(),
            },
        ],
    })
}

pub(crate) async fn resolve_control_api_key(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    candidate_keys: &[&str],
) -> anyhow::Result<String> {
    if let Some(value) = instance
        .config_json
        .as_ref()
        .and_then(|value| value.get("api_key"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(value.to_string());
    }

    for key in candidate_keys {
        if let Some(secret) = store
            .get_secret(SecretScope::Instance, Some(instance.instance_id), key)
            .await?
        {
            let value = state.secrets.decrypt(&secret.value_encrypted)?;
            let value = value.trim();
            if !value.is_empty() {
                return Ok(value.to_string());
            }
        }
    }

    anyhow::bail!("service api key is not available yet");
}

async fn resolve_control_secret_value(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    candidate_keys: &[&str],
) -> anyhow::Result<String> {
    for key in candidate_keys {
        if let Some(value) = instance
            .config_json
            .as_ref()
            .and_then(|config| config.get(*key))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(value.to_string());
        }
    }

    for key in candidate_keys {
        if let Some(secret) = store
            .get_secret(SecretScope::Instance, Some(instance.instance_id), key)
            .await?
        {
            let value = state.secrets.decrypt(&secret.value_encrypted)?;
            let value = value.trim();
            if !value.is_empty() {
                return Ok(value.to_string());
            }
        }
    }

    anyhow::bail!("required service credential is not available yet");
}

async fn request_control_json(
    base_url: &str,
    api_key: &str,
    paths: &[&str],
) -> anyhow::Result<serde_json::Value> {
    let client = build_extension_control_arr_client(api_key)?;

    for path in paths {
        let url = build_extension_control_url(base_url, path)?;
        let resp = client
            .request(ReqwestMethod::GET, url)
            .send()
            .await
            .map_err(anyhow::Error::from)?;
        if resp.status() == ReqwestStatusCode::NOT_FOUND {
            continue;
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            anyhow::bail!("{path} failed ({status}): {}", detail.trim());
        }
        return resp
            .json::<serde_json::Value>()
            .await
            .map_err(anyhow::Error::from);
    }

    anyhow::bail!("service endpoint is not available")
}

async fn request_control_write(
    base_url: &str,
    api_key: &str,
    method: ReqwestMethod,
    paths: &[&str],
    body: Option<&serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let client = build_extension_control_arr_client(api_key)?;

    for path in paths {
        let url = build_extension_control_url(base_url, path)?;
        let mut request = client.request(method.clone(), url);
        if let Some(body) = body {
            request = request.json(body);
        }
        let resp = request.send().await.map_err(anyhow::Error::from)?;
        if resp.status() == ReqwestStatusCode::NOT_FOUND {
            continue;
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            anyhow::bail!("{} failed ({status}): {}", path, detail.trim());
        }
        let bytes = resp.bytes().await.map_err(anyhow::Error::from)?;
        if bytes.is_empty() {
            return Ok(json!({}));
        }
        let value =
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| json!({}));
        return Ok(value);
    }

    anyhow::bail!("service endpoint is not available")
}

pub(crate) async fn resolve_control_provider_transport_base_url(
    instance_id: Uuid,
    endpoint: &ProviderEndpoint,
) -> anyhow::Result<String> {
    let canonical = endpoint.canonical_url()?;
    if control_endpoint_host_resolves(&endpoint.host, endpoint.port).await {
        return Ok(canonical);
    }

    if let Some(host_port) =
        lookup_control_docker_published_port(instance_id, endpoint.port).await?
    {
        let base_path = if endpoint.base_path.trim().is_empty() {
            "/"
        } else {
            endpoint.base_path.as_str()
        };
        return Ok(format!(
            "{}://127.0.0.1:{}{}",
            endpoint.scheme, host_port, base_path
        ));
    }

    anyhow::bail!(
        "provider endpoint {}:{} is not reachable from the server host",
        endpoint.host,
        endpoint.port
    )
}

async fn control_endpoint_host_resolves(host: &str, port: u16) -> bool {
    lookup_host((host, port))
        .await
        .map(|mut values| values.next().is_some())
        .unwrap_or(false)
}

async fn lookup_control_docker_published_port(
    instance_id: Uuid,
    container_port: u16,
) -> anyhow::Result<Option<u16>> {
    let container_names = run_control_docker_stdout(&[
        "ps",
        "-a",
        "--filter",
        &format!("label=elixir.instance_id={instance_id}"),
        "--format",
        "{{.Names}}",
    ])
    .await?;
    for container_name in control_transport_container_candidates(&container_names) {
        if let Some(host_port) =
            lookup_control_container_published_port(&container_name, container_port).await?
        {
            return Ok(Some(host_port));
        }
    }

    Ok(None)
}

async fn lookup_control_container_published_port(
    container_name: &str,
    container_port: u16,
) -> anyhow::Result<Option<u16>> {
    let ports_json = run_control_docker_stdout(&[
        "inspect",
        "--format",
        "{{json .NetworkSettings.Ports}}",
        container_name,
    ])
    .await?;
    Ok(parse_control_published_host_port(
        ports_json.trim(),
        container_port,
    ))
}

fn control_transport_container_candidates(output: &str) -> Vec<String> {
    let mut names = output
        .lines()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    names.sort_by(|left, right| {
        control_transport_container_rank(left)
            .cmp(&control_transport_container_rank(right))
            .then_with(|| left.cmp(right))
    });
    names
}

fn control_transport_container_rank(name: &str) -> u8 {
    if name.ends_with("-vpn") || name.contains("-vpn-") {
        1
    } else {
        0
    }
}

fn parse_control_published_host_port(ports_json: &str, container_port: u16) -> Option<u16> {
    let ports: serde_json::Value = serde_json::from_str(ports_json.trim()).ok()?;
    let key = format!("{container_port}/tcp");
    let binding = ports
        .get(&key)
        .and_then(serde_json::Value::as_array)
        .and_then(|values| values.first());
    binding
        .and_then(|binding| binding.get("HostPort"))
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.trim().parse::<u16>().ok())
}

async fn run_control_docker_stdout(args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("docker").args(args).output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("docker {} failed: {}", args.join(" "), stderr.trim());
    }
    String::from_utf8(output.stdout).map_err(anyhow::Error::from)
}

fn build_extension_control_arr_client(api_key: &str) -> anyhow::Result<Client> {
    let mut headers = ReqwestHeaderMap::new();
    headers.insert(
        "X-Api-Key",
        ReqwestHeaderValue::from_str(api_key).map_err(anyhow::Error::from)?,
    );
    headers.insert(USER_AGENT, ReqwestHeaderValue::from_static("Elixir/1.0"));
    headers.insert(ACCEPT, ReqwestHeaderValue::from_static("application/json"));
    Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .default_headers(headers)
        .build()
        .map_err(anyhow::Error::from)
}

fn build_extension_control_url(base_url: &str, path: &str) -> anyhow::Result<Url> {
    let mut root = Url::parse(base_url)?;
    let (path_only, query) = match path.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path, None),
    };
    let trimmed = root.path().trim_end_matches('/');
    let next_path = if trimmed.is_empty() || trimmed == "/" {
        format!("/{}", path_only.trim_start_matches('/'))
    } else {
        format!("{}/{}", trimmed, path_only.trim_start_matches('/'))
    };
    root.set_path(&next_path);
    root.set_query(query);
    Ok(root)
}

pub async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<RunsQuery>,
) -> ApiResult<Json<Vec<OrchestratorRun>>> {
    let store = ExtensionStore::new(&state.db_pool);
    let runs = store.list_runs(query.limit).await.map_err(ApiError::from)?;
    Ok(Json(runs))
}

pub async fn clear_runs(State(state): State<AppState>) -> ApiResult<Json<RunsClearResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let stale_before = chrono::Utc::now()
        - chrono::Duration::seconds(state.settings.extensions.apply_lock_ttl_seconds.max(1) as i64);
    let _ = store
        .reap_stale_running_runs(
            stale_before,
            "run history cleared; stale running run reaped",
        )
        .await;
    let deleted = store.delete_run_history().await.map_err(ApiError::from)?;
    Ok(Json(RunsClearResponse { deleted }))
}

pub async fn list_desired_blueprints(
    State(state): State<AppState>,
    Query(query): Query<DesiredBlueprintsQuery>,
) -> ApiResult<Json<Vec<DesiredBlueprint>>> {
    let store = ExtensionStore::new(&state.db_pool);
    let desired = store
        .list_desired_blueprints(query.applied)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(desired))
}

pub async fn clear_desired_blueprints(
    State(state): State<AppState>,
    Query(query): Query<DesiredBlueprintsQuery>,
) -> ApiResult<Json<DesiredBlueprintsClearResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let deleted = store
        .delete_desired_blueprints(query.applied)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(DesiredBlueprintsClearResponse { deleted }))
}

pub async fn run_detail(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<RunDetailResponse>> {
    let run_id = Uuid::parse_str(&run_id).map_err(|_| ApiError::bad_request("invalid run id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let run = store
        .get_run(run_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let steps = store.list_steps(run_id).await.map_err(ApiError::from)?;
    let stage_summary = run
        .plan_json
        .as_ref()
        .and_then(|plan_json| serde_json::from_value::<Plan>(plan_json.clone()).ok())
        .map(|plan| summarize_run_stages(&run, &steps, &plan));
    Ok(Json(RunDetailResponse {
        run,
        steps,
        stage_summary,
    }))
}

fn summarize_run_stages(
    run: &OrchestratorRun,
    steps: &[OperationStep],
    plan: &Plan,
) -> RunStageSummary {
    let mut stages = Vec::new();
    for stage in &plan.stages {
        stages.push(stage_progress(stage, steps));
    }

    let current = stages
        .iter()
        .find(|stage| matches!(stage.status.as_str(), "failed" | "running" | "pending"))
        .or_else(|| {
            stages
                .iter()
                .rev()
                .find(|stage| stage.status == "completed")
        });

    let blocked_stage = if matches!(
        run.status,
        OrchestratorRunStatus::Pending | OrchestratorRunStatus::Running
    ) {
        plan.blocked_stage.clone()
    } else {
        stages
            .iter()
            .find(|stage| stage.status == "failed")
            .map(|stage| PlanBlockedStage {
                stage_id: stage.stage_id.clone(),
                code: run.status.as_str().to_string(),
                detail: run.error.clone(),
            })
            .or_else(|| {
                if run.status == OrchestratorRunStatus::Canceled {
                    current.map(|stage| PlanBlockedStage {
                        stage_id: stage.stage_id.clone(),
                        code: "canceled".to_string(),
                        detail: run.error.clone(),
                    })
                } else {
                    None
                }
            })
    };

    RunStageSummary {
        current_stage_id: current.map(|stage| stage.stage_id.clone()),
        current_stage_status: current.map(|stage| stage.status.clone()),
        blocked_stage,
        stages,
    }
}

fn stage_progress(stage: &PlanStage, steps: &[OperationStep]) -> RunStageProgress {
    let stage_steps: Vec<&OperationStep> = steps
        .iter()
        .filter(|step| {
            let index = step.step_index.max(0) as usize;
            index >= stage.action_start_index && index < stage.action_end_index
        })
        .collect();
    let step_count = stage
        .action_end_index
        .saturating_sub(stage.action_start_index);
    let completed_step_count = stage_steps
        .iter()
        .filter(|step| step.status == OperationStepStatus::Completed)
        .count();

    let status = if stage_steps
        .iter()
        .any(|step| step.status == OperationStepStatus::Failed)
    {
        "failed"
    } else if stage_steps
        .iter()
        .any(|step| step.status == OperationStepStatus::Running)
    {
        "running"
    } else if step_count > 0 && completed_step_count == step_count {
        "completed"
    } else if !stage_steps.is_empty() {
        "pending"
    } else if step_count == 0 {
        "empty"
    } else {
        "pending"
    };

    RunStageProgress {
        stage_id: stage.stage_id.clone(),
        status: status.to_string(),
        step_count,
        completed_step_count,
    }
}

struct ExtensionStoragePaths {
    root: PathBuf,
    packages_dir: PathBuf,
    unpacked_dir: PathBuf,
    tmp_dir: PathBuf,
    registry_cache_dir: PathBuf,
}

impl ExtensionStoragePaths {
    fn new(root: &str) -> Self {
        let root = PathBuf::from(root);
        Self {
            root: root.clone(),
            packages_dir: root.join("packages"),
            unpacked_dir: root.join("unpacked"),
            tmp_dir: root.join("tmp"),
            registry_cache_dir: root.join("registry-cache"),
        }
    }

    async fn ensure_dirs(&self) -> Result<(), anyhow::Error> {
        fs::create_dir_all(&self.root).await?;
        fs::create_dir_all(&self.packages_dir).await?;
        fs::create_dir_all(&self.unpacked_dir).await?;
        fs::create_dir_all(&self.tmp_dir).await?;
        fs::create_dir_all(&self.registry_cache_dir).await?;
        Ok(())
    }
}

async fn download_package(url: &str, dest_dir: &std::path::Path) -> Result<PathBuf, anyhow::Error> {
    fs::create_dir_all(dest_dir).await?;
    let response = reqwest::get(url).await?;
    if !response.status().is_success() {
        anyhow::bail!("download failed with status {}", response.status());
    }
    let bytes = response.bytes().await?;
    let filename = format!("{}.elx", Uuid::new_v4());
    let dest_path = dest_dir.join(filename);
    fs::write(&dest_path, &bytes).await?;
    Ok(dest_path)
}

async fn remove_downloaded_package(
    packages_dir: &std::path::Path,
    package_hash: &str,
) -> Result<(), anyhow::Error> {
    let mut entries = match fs::read_dir(packages_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let is_elx = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("elx"))
            .unwrap_or(false);
        if !is_elx {
            continue;
        }
        let hash = match compute_sha256(&path).await {
            Ok(hash) => hash,
            Err(err) => {
                tracing::warn!("failed to hash extension package {}: {err}", path.display());
                continue;
            }
        };
        if hash.eq_ignore_ascii_case(package_hash) {
            let _ = fs::remove_file(&path).await;
            break;
        }
    }
    Ok(())
}

async fn uninstall_extension_record(
    store: &ExtensionStore<'_>,
    storage_paths: &ExtensionStoragePaths,
    extension: &Extension,
) -> Result<(), anyhow::Error> {
    store.delete_extension(&extension.extension_id).await?;
    let _ = fs::remove_dir_all(storage_paths.unpacked_dir.join(&extension.extension_id)).await;
    if let Some(hash) = extension.package_hash.as_deref() {
        if let Err(err) = remove_downloaded_package(&storage_paths.packages_dir, hash).await {
            tracing::warn!(
                "failed to remove package for {}: {err}",
                extension.extension_id
            );
        }
    }
    Ok(())
}

async fn remove_extension_instances(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> Result<(), anyhow::Error> {
    let instances = store.list_instances(Some(extension_id)).await?;
    for instance in instances {
        remove_instance_record(state, store, instance.instance_id).await?;
    }
    Ok(())
}

async fn remove_instance_record(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<(), anyhow::Error> {
    state
        .orchestrator
        .remove_instance_runtime(instance_id)
        .await?;
    for provider in store.list_providers(Some(instance_id)).await? {
        store.delete_provider(provider.provider_id).await?;
    }
    store
        .delete_secrets_by_scope(SecretScope::Instance, Some(instance_id))
        .await?;
    store.delete_instance(instance_id).await?;
    Ok(())
}

fn blueprint_dependency_ids(manifest: &ExtensionManifest, blueprint_id: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    let Some(execution) = manifest.execution.as_ref() else {
        return out;
    };

    for extension_id in &execution.packages {
        let trimmed = extension_id.trim();
        if trimmed.is_empty() || trimmed == blueprint_id {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            out.push(trimmed.to_string());
        }
    }

    out
}

fn dependency_referenced_by_other_blueprints(
    installed: &[Extension],
    uninstalling_blueprint_id: &str,
    dependency_id: &str,
) -> bool {
    for extension in installed {
        if extension.kind != ExtensionKind::Blueprint {
            continue;
        }
        if extension.extension_id == uninstalling_blueprint_id {
            continue;
        }
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        else {
            continue;
        };
        if blueprint_dependency_ids(&manifest, &extension.extension_id)
            .iter()
            .any(|item| item == dependency_id)
        {
            return true;
        }
    }
    false
}

fn map_unique_violation(err: anyhow::Error, message: &str) -> ApiError {
    let details = err.to_string();
    if details.contains("UNIQUE") || details.contains("unique") {
        return ApiError::conflict(message);
    }
    ApiError::internal(details)
}

fn validate_semver_upgrade(
    existing: &Extension,
    new_version: &Version,
    package_hash: Option<&str>,
    policy: InstallPolicy,
) -> anyhow::Result<()> {
    let existing_version = Version::parse(&existing.version)
        .map_err(|_| anyhow::anyhow!("existing extension version is not valid semver"))?;
    if new_version < &existing_version {
        if policy.allow_downgrade {
            return Ok(());
        }
        anyhow::bail!("extension version downgrade is not allowed");
    }
    if new_version == &existing_version {
        if policy.allow_same_version_replace {
            return Ok(());
        }
        if let (Some(existing_hash), Some(new_hash)) =
            (existing.package_hash.as_deref(), package_hash)
        {
            if existing_hash.eq_ignore_ascii_case(new_hash) {
                return Ok(());
            }
        }
        anyhow::bail!("extension version is already installed");
    }
    Ok(())
}

fn secret_response(secret: Secret) -> SecretResponse {
    SecretResponse {
        secret_id: secret.secret_id,
        scope: secret.scope,
        scope_id: secret.scope_id,
        key: secret.key,
        created_at: secret.created_at,
        rotatable: secret.rotatable,
    }
}

fn parse_secret_scope(raw: &str) -> ApiResult<SecretScope> {
    raw.parse()
        .map_err(|_| ApiError::bad_request("invalid secret scope"))
}

fn parse_scope_id(
    scope: SecretScope,
    scope_id: Option<&str>,
    require_id: bool,
) -> ApiResult<Option<Uuid>> {
    match scope {
        SecretScope::Global => {
            if scope_id.is_some() {
                return Err(ApiError::bad_request("global secrets do not use scope_id"));
            }
            Ok(None)
        }
        SecretScope::Instance | SecretScope::Provider => match scope_id {
            Some(value) => Uuid::parse_str(value)
                .map(Some)
                .map_err(|_| ApiError::bad_request("invalid scope_id")),
            None => {
                if require_id {
                    Err(ApiError::bad_request("scope_id is required for this scope"))
                } else {
                    Ok(None)
                }
            }
        },
    }
}

fn parse_scope_and_id(
    scope: &str,
    scope_id: Option<&str>,
    require_id: bool,
) -> ApiResult<(SecretScope, Option<Uuid>)> {
    let scope = parse_secret_scope(scope)?;
    let scope_id = parse_scope_id(scope, scope_id, require_id)?;
    Ok((scope, scope_id))
}

fn path_within(path: &std::path::Path, root: &std::path::Path) -> bool {
    let path = std::fs::canonicalize(path).ok();
    let root = std::fs::canonicalize(root).ok();
    match (path, root) {
        (Some(path), Some(root)) => path.starts_with(root),
        _ => false,
    }
}

async fn copy_dir_recursive(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> Result<(), anyhow::Error> {
    let mut stack = vec![(src.to_path_buf(), dst.to_path_buf())];
    while let Some((src_dir, dst_dir)) = stack.pop() {
        fs::create_dir_all(&dst_dir).await?;
        let mut entries = fs::read_dir(&src_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            let src_path = entry.path();
            let dst_path = dst_dir.join(entry.file_name());
            if file_type.is_dir() {
                stack.push((src_path, dst_path));
            } else if file_type.is_file() {
                fs::copy(&src_path, &dst_path).await?;
            }
        }
    }
    Ok(())
}

async fn fetch_registry_entry(
    registry_urls: &[String],
    download_url: Option<&str>,
    manifest: &crate::extensions::manifest::ExtensionManifest,
) -> Option<RegistryEntry> {
    if registry_urls.is_empty() {
        return None;
    }
    let client = match RegistryClient::new(Duration::from_secs(10)) {
        Ok(client) => client,
        Err(err) => {
            tracing::warn!("registry client init failed: {err}");
            return None;
        }
    };

    let mut entries = Vec::new();
    for url in registry_urls {
        match client.fetch(url).await {
            Ok(index) => entries.extend(index.extensions),
            Err(err) => {
                tracing::warn!("registry fetch failed for {}: {}", url, err);
            }
        }
    }

    if let Some(download_url) = download_url {
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.download_url == download_url)
        {
            return Some(entry.clone());
        }
    }

    entries
        .into_iter()
        .find(|entry| entry.id == manifest.id && entry.version == manifest.version)
}

#[cfg(test)]
mod tests {
    use super::{
        managed_prowlarr_proxy_cleanup_from_manifest, missing_required_connector_targets,
        prowlarr_entity_id, prowlarr_entity_tag_ids,
    };
    use crate::extensions::manifest::ExtensionManifest;
    use serde_json::json;
    use std::collections::HashSet;

    #[test]
    fn proxy_cleanup_manifest_extracts_names_and_tags() {
        let manifest: ExtensionManifest = serde_json::from_value(json!({
            "id": "elixir.connectors.prowlarr_byparr_proxy",
            "version": "1.0.0",
            "kind": "connector",
            "name": "Prowlarr Byparr Proxy",
            "targets": [
                {
                    "capability": "indexer.registry",
                    "slot": "default"
                }
            ],
            "actions": [
                {
                    "type": "driver_patch",
                    "target": {
                        "capability": "indexer.registry",
                        "slot": "default"
                    },
                    "patch": {
                        "op": "register_indexer_proxies",
                        "proxies": [
                            {
                                "name": "Byparr",
                                "implementation": "FlareSolverr",
                                "tags": ["byparr"],
                                "settings": {
                                    "host": "http://elx-byparr:8191/",
                                    "requestTimeout": 180
                                }
                            }
                        ]
                    }
                }
            ]
        }))
        .expect("manifest");

        let cleanup = managed_prowlarr_proxy_cleanup_from_manifest(&manifest);
        assert_eq!(cleanup.proxies.len(), 1);
        assert_eq!(cleanup.proxies[0].name, "Byparr");
        assert_eq!(cleanup.proxies[0].tags, vec!["byparr".to_string()]);
        assert_eq!(
            cleanup
                .target_ref
                .as_ref()
                .map(|target| target.capability.as_str()),
            Some("indexer.registry")
        );
    }

    #[test]
    fn prowlarr_entity_helpers_handle_numeric_ids() {
        let signed = json!({
            "id": 7,
            "tags": [2, 7]
        });
        let unsigned = json!({
            "id": 9u64,
            "tags": [2u64, 9u64]
        });

        assert_eq!(prowlarr_entity_id(&signed), Some(7));
        assert_eq!(prowlarr_entity_id(&unsigned), Some(9));
        assert_eq!(prowlarr_entity_tag_ids(&signed), vec![2, 7]);
        assert_eq!(prowlarr_entity_tag_ids(&unsigned), vec![2, 9]);
    }

    #[test]
    fn connector_requirement_helper_reports_missing_non_optional_targets() {
        let manifest: ExtensionManifest = serde_json::from_value(json!({
            "id": "elixir.connectors.prowlarr_flaresolverr_proxy",
            "version": "1.1.0",
            "kind": "connector",
            "name": "Prowlarr FlareSolverr Proxy",
            "requires": [
                { "capability": "indexer.proxy", "slot": "flaresolverr" },
                { "capability": "indexer.proxy", "slot": "optional", "optional": true }
            ],
            "targets": [
                {
                    "capability": "indexer.registry",
                    "slot": "default"
                }
            ],
            "actions": [
                {
                    "type": "driver_patch",
                    "target": {
                        "capability": "indexer.registry",
                        "slot": "default"
                    },
                    "patch": {
                        "op": "register_indexer_proxies",
                        "proxies": [
                            {
                                "name": "FlareSolverr",
                                "implementation": "FlareSolverr",
                                "tags": ["flaresolverr"],
                                "settings": {
                                    "host": "http://elx-flaresolverr:8191/",
                                    "requestTimeout": 180
                                }
                            }
                        ]
                    }
                }
            ]
        }))
        .expect("manifest");

        let available = HashSet::from([("indexer.registry".to_string(), "default".to_string())]);
        assert_eq!(
            missing_required_connector_targets(&manifest, &available),
            vec!["indexer.proxy/flaresolverr".to_string()]
        );
    }
}
