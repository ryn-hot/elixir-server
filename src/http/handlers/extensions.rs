use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use base64::{Engine as _, engine::general_purpose};
use rand::{RngCore, rngs::OsRng};
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method as ReqwestMethod, StatusCode as ReqwestStatusCode, Url};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::lookup_host;
use tokio::fs;
use tokio::process::Command;
use uuid::Uuid;

use crate::config::{DownloaderPerformanceProfile, RunEnvironment};
use crate::db::models::{
    Binding, BindingStatus, DesiredBlueprint, Extension, ExtensionInstance, ExtensionKind,
    ExtensionTrustLevel, OperationStep, OperationStepStatus, OrchestratorRun,
    OrchestratorRunStatus, Provider, ProviderHealthState, RuntimeLog, Secret, SecretScope,
};
use crate::extensions::manifest::ExtensionManifest;
use crate::extensions::package::{
    PackageManifest, compute_sha256, read_manifest_from_dir, read_package_signature,
    unpack_package, verify_signature,
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
use crate::http::error::{ApiError, ApiResult};
use crate::orchestrator::plan_executor::{PlanExecutor, PlannedStep};
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::plan_validation::{
    has_unresolved_conflicts, missing_required_secrets_for_plan,
};
use crate::orchestrator::planner::{
    Plan, PlanAction, PlanDecisions, Planner, SlotConflictResolution,
};
use crate::orchestrator::reconcile::ReconcileConfig;
use crate::state::AppState;

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
pub struct AutoWireStatusResponse {
    pub enabled: bool,
    pub pending_plan_id: Option<Uuid>,
    pub pending_reason: Option<String>,
    pub pending_conflicts: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoWireUpdateRequest {
    pub enabled: bool,
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
pub struct RunDetailResponse {
    pub run: OrchestratorRun,
    pub steps: Vec<OperationStep>,
}

#[derive(Debug, Serialize)]
pub struct ReconcileRunResponse {
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
    pub items: Vec<ExtensionStatusSummaryItem>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub optional_addons: Vec<ExtensionOptionalAddonSummaryItem>,
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
    #[serde(default)]
    pub fields: Vec<ExtensionControlField>,
    #[serde(default)]
    pub entities: Vec<ExtensionControlEntity>,
    #[serde(default)]
    pub actions: Vec<ExtensionControlAction>,
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
    let planner = Planner::new();
    let mut plan = planner
        .plan_blueprint(&store, payload.blueprint_id, payload.params)
        .await
        .map_err(ApiError::from)?;
    let blueprint = store
        .get_extension(&plan.blueprint_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("blueprint extension not found"))?;

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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmPlanRequest {
    #[serde(default)]
    pub decisions: Option<PlanDecisions>,
}

pub async fn confirm_plan(
    State(state): State<AppState>,
    Path(plan_id): Path<String>,
    payload: Option<Json<ConfirmPlanRequest>>,
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

    let decisions = payload.and_then(|payload| payload.decisions.clone());
    if let Some(decisions) = decisions.as_ref() {
        if decisions
            .slot_conflicts
            .iter()
            .any(|decision| matches!(decision.action, SlotConflictResolution::Abort))
        {
            return Err(ApiError::conflict("plan confirmation aborted"));
        }
    }

    let plan = if let Some(decisions) = decisions.as_ref() {
        let planner = Planner::new();
        let mut resolved = planner
            .plan_blueprint_with_decisions(
                &store,
                plan.blueprint_id.clone(),
                plan.params.clone(),
                Some(decisions),
            )
            .await
            .map_err(ApiError::from)?;
        resolved.plan_id = run_id;
        let plan_json =
            serde_json::to_value(&resolved).map_err(|err| ApiError::internal(err.to_string()))?;
        store
            .update_run_plan(run_id, plan_json)
            .await
            .map_err(ApiError::from)?;
        resolved
    } else {
        plan
    };

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

    let decisions_json = decisions
        .as_ref()
        .map(|value| serde_json::to_value(value))
        .transpose()
        .map_err(|err| ApiError::internal(err.to_string()))?;

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
                decisions_json: decisions_json.clone(),
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

    if state.settings.extensions.registries.is_empty() {
        return Ok(CatalogResponse {
            installed,
            available: Vec::new(),
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
        available: cache.index.extensions,
        registry_errors: cache.registry_errors,
        last_refreshed_at: Some(cache.fetched_at),
        last_refresh_success_at: cache.last_success_at,
        last_refresh_error: cache.last_error,
        core_extensions: state.settings.extensions.core_extensions.clone(),
    })
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
        let context = load_extension_control_context(&store, &extension_id)
            .await
            .map_err(ApiError::from)?;
        let extension_key = context.extension.extension_id.to_ascii_lowercase();
        if extension_key.contains("sonarr") || extension_key.contains("radarr") {
            let Some(instance) = context.selected_instance.as_ref() else {
                return Err(ApiError::conflict(
                    "no active instance is available for this extension yet",
                ));
            };
            save_manager_control_defaults(&store, instance.instance_id, &payload.values)
                .await
                .map_err(ApiError::from)?;
        } else if extension_key.contains("qbittorrent") || extension_key.contains("nzbget") {
            let Some(profile) = payload
                .values
                .get("downloaderProfile")
                .and_then(serde_json::Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase())
            else {
                return Err(ApiError::conflict(
                    "downloaderProfile is required for downloader defaults",
                ));
            };
            match profile.as_str() {
                "balanced" => {
                    if state.settings.extensions.downloader_profile
                        == DownloaderPerformanceProfile::Balanced
                    {
                        store
                            .delete_extension_setting(DOWNLOADER_PROFILE_SETTING_KEY)
                            .await
                            .map_err(ApiError::from)?;
                    } else {
                        store
                            .upsert_extension_setting(
                                DOWNLOADER_PROFILE_SETTING_KEY,
                                &serde_json::Value::String(profile),
                            )
                            .await
                            .map_err(ApiError::from)?;
                    }
                }
                "aggressive" => {
                    if state.settings.extensions.downloader_profile
                        == DownloaderPerformanceProfile::Aggressive
                    {
                        store
                            .delete_extension_setting(DOWNLOADER_PROFILE_SETTING_KEY)
                            .await
                            .map_err(ApiError::from)?;
                    } else {
                        store
                            .upsert_extension_setting(
                                DOWNLOADER_PROFILE_SETTING_KEY,
                                &serde_json::Value::String(profile),
                            )
                            .await
                            .map_err(ApiError::from)?;
                    }
                }
                _ => {
                    return Err(ApiError::conflict(
                        "downloaderProfile must be balanced or aggressive",
                    ))
                }
            }
        } else {
            return Err(ApiError::conflict(
                "this extension does not expose editable settings yet",
            ));
        }
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

pub async fn install_extension(
    State(state): State<AppState>,
    Json(payload): Json<InstallRequest>,
) -> ApiResult<Json<InstallResponse>> {
    let storage_paths = ExtensionStoragePaths::new(&state.settings.extensions.storage_root);
    storage_paths.ensure_dirs().await.map_err(ApiError::from)?;

    let is_dev = state.settings.environment == RunEnvironment::Development;
    let allow_unsigned = is_dev && state.settings.extensions.allow_unsigned;
    let allow_directory_install = is_dev && state.settings.extensions.allow_directory_install;

    let package_path = match (&payload.download_url, &payload.package_path) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "provide download_url or package_path, not both",
            ));
        }
        (Some(url), None) => download_package(url, &storage_paths.packages_dir)
            .await
            .map_err(ApiError::from)?,
        (None, Some(path)) => PathBuf::from(path),
        (None, None) => {
            return Err(ApiError::bad_request(
                "download_url or package_path is required",
            ));
        }
    };

    if !package_path.exists() {
        return Err(ApiError::bad_request("package path does not exist"));
    }

    let bundled_dir = PathBuf::from(&state.settings.extensions.bundled_dir);
    let is_bundled_source = path_within(&package_path, &bundled_dir);

    let staging_dir = storage_paths.tmp_dir.join(Uuid::new_v4().to_string());
    let mut package_hash = None;
    let staged = if package_path.is_dir() {
        if !allow_directory_install && !is_bundled_source {
            return Err(ApiError::bad_request(
                "directory installs are only allowed in development with extensions.allow_directory_install=true",
            ));
        }
        if !allow_unsigned && !is_bundled_source {
            return Err(ApiError::bad_request(
                "unsigned installs are disabled; enable extensions.allow_unsigned for development",
            ));
        }
        copy_dir_recursive(&package_path, &staging_dir)
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?;
        staging_dir.clone()
    } else if package_path.is_file() {
        let hash = compute_sha256(&package_path)
            .await
            .map_err(ApiError::from)?;
        package_hash = Some(hash);
        unpack_package(&package_path, &staging_dir)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?
    } else {
        return Err(ApiError::bad_request(
            "package path is not a file or directory",
        ));
    };

    let PackageManifest {
        manifest, raw_json, ..
    } = read_manifest_from_dir(&staged)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

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
            return Err(ApiError::bad_request(
                "manifest id/version does not match registry entry",
            ));
        }
        if let Some(expected_hash) = entry.sha256.as_deref() {
            if !expected_hash.trim().eq_ignore_ascii_case(hash) {
                return Err(ApiError::bad_request(
                    "package hash does not match registry",
                ));
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
                return Err(ApiError::bad_request(
                    "publisher key mismatch between manifest and registry",
                ));
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
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let signature = registry_entry
            .as_ref()
            .and_then(|entry| entry.signature.as_deref())
            .or(package_signature.as_deref());
        let has_material = signature.is_some() || publisher_key_id.is_some();
        if has_material {
            verify_signature(hash, signature, publisher_key_id)
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
        } else if !allow_unsigned && !is_bundled_source {
            return Err(ApiError::bad_request("package signature is required"));
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

    let policy = PermissionPolicy::new();
    policy
        .enforce(trust_level, &manifest.permissions, &manifest.id)
        .map_err(|err| ApiError::forbidden(err.to_string()))?;

    let new_version = Version::parse(&manifest.version)
        .map_err(|_| ApiError::bad_request("extension version is not valid semver"))?;
    let store = ExtensionStore::new(&state.db_pool);
    if let Some(existing) = store
        .get_extension(&manifest.id)
        .await
        .map_err(ApiError::from)?
    {
        validate_semver_upgrade(&existing, &new_version, package_hash.as_deref())?;
    }
    let required = required_secrets_from_manifest(&manifest)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if !required.is_empty() {
        let instances = store
            .list_instances(Some(&manifest.id))
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

    let extension_id = manifest.id;
    let name = manifest.name;
    let version = manifest.version;
    let kind = manifest.kind;

    let extension_root = storage_paths.unpacked_dir.join(&extension_id);
    fs::create_dir_all(&extension_root)
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?;
    let unpacked_dir = extension_root.join(&version);
    if unpacked_dir.exists() {
        fs::remove_dir_all(&unpacked_dir)
            .await
            .map_err(|err| ApiError::internal(err.to_string()))?;
    }
    fs::rename(&staged, &unpacked_dir)
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?;

    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.clone(),
            name: name.clone(),
            version: version.clone(),
            kind,
            publisher_name,
            signing_key_id,
            trust_level,
            manifest_json: raw_json,
            package_hash,
            enabled: true,
        })
        .await
        .map_err(ApiError::from)?;

    Ok(Json(InstallResponse {
        extension_id,
        name,
        version,
        kind,
        trust_level,
        enabled: true,
    }))
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

    remove_extension_instances(&state, &store, &existing.extension_id)
        .await
        .map_err(ApiError::from)?;

    uninstall_extension_record(&store, &storage_paths, &existing)
        .await
        .map_err(ApiError::from)?;

    for dependency in cascade_targets {
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
    Ok(Json(instance))
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
    Ok(Json(ProviderHealthResponse {
        provider_id,
        health_state: provider.health_state,
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

pub async fn auto_wire_status(
    State(state): State<AppState>,
) -> ApiResult<Json<AutoWireStatusResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let enabled = store
        .get_auto_wire_enabled()
        .await
        .map_err(ApiError::from)?;
    let pending = store
        .get_latest_run_by_source("auto_wire", Some(OrchestratorRunStatus::Pending))
        .await
        .map_err(ApiError::from)?;

    let mut pending_plan_id = None;
    let mut pending_reason = None;
    let mut pending_conflicts = None;

    if let Some(run) = pending {
        if let Some(plan_json) = run.plan_json {
            if let Ok(plan) = serde_json::from_value::<Plan>(plan_json) {
                pending_plan_id = Some(plan.plan_id);
                pending_conflicts = Some(plan.conflicts.len());
                let has_missing = plan.conflicts.iter().any(|conflict| {
                    conflict.get("code").and_then(|value| value.as_str())
                        == Some("missing_required_secrets")
                });
                pending_reason = Some(if has_missing {
                    "Missing required secrets".to_string()
                } else if !plan.conflicts.is_empty() {
                    "Conflicts require review".to_string()
                } else {
                    "Pending auto-wire actions".to_string()
                });
            } else {
                pending_plan_id = Some(run.run_id);
            }
        } else {
            pending_plan_id = Some(run.run_id);
        }
    }

    Ok(Json(AutoWireStatusResponse {
        enabled,
        pending_plan_id,
        pending_reason,
        pending_conflicts,
    }))
}

pub async fn update_auto_wire(
    State(state): State<AppState>,
    Json(payload): Json<AutoWireUpdateRequest>,
) -> ApiResult<Json<AutoWireStatusResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    store
        .set_auto_wire_enabled(payload.enabled)
        .await
        .map_err(ApiError::from)?;

    if !payload.enabled {
        let _ = store
            .cancel_pending_runs_by_source("auto_wire", Some("auto-wire disabled"))
            .await;
    } else {
        let config = ReconcileConfig::from_settings(&state.settings);
        let orchestrator = state.orchestrator.clone();
        tokio::spawn(async move {
            let _ = orchestrator.reconcile_once(&config).await;
        });
    }

    auto_wire_status(State(state)).await
}

pub async fn auto_wire_plan(State(state): State<AppState>) -> ApiResult<Json<Plan>> {
    let store = ExtensionStore::new(&state.db_pool);
    let run = store
        .get_latest_run_by_source("auto_wire", Some(OrchestratorRunStatus::Pending))
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("no pending auto-wire plan"))?;
    let plan_json = run
        .plan_json
        .ok_or_else(|| ApiError::not_found("auto-wire plan missing"))?;
    let plan: Plan = serde_json::from_value(plan_json)
        .map_err(|err| ApiError::bad_request(format!("invalid plan payload: {err}")))?;
    Ok(Json(plan))
}

pub async fn status_summary(
    State(state): State<AppState>,
) -> ApiResult<Json<ExtensionStatusSummaryResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let response = build_extension_status_summary(&store)
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
    store: &ExtensionStore<'_>,
) -> anyhow::Result<ExtensionStatusSummaryResponse> {
    let extensions = store.list_extensions().await?;
    let instances = store.list_instances(None).await?;
    let providers = store.list_providers(None).await?;
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
                summarize_connector_extension(&extension, manifest.as_ref(), &available_targets)
            }
            ExtensionKind::Module => {
                summarize_module_extension(
                    store,
                    &extension,
                    manifest.as_ref(),
                    &instances,
                    &providers_by_instance,
                    &failed_bindings_by_consumer,
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
        items,
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

fn summarize_connector_extension(
    extension: &Extension,
    manifest: Option<&ExtensionManifest>,
    available_targets: &HashSet<(String, String)>,
) -> ExtensionStatusSummaryItem {
    if !extension.enabled {
        return disabled_extension_status(
            extension,
            "disabled",
            "Disabled",
            "This connector is installed but turned off.",
            "enable",
            "Enable",
        );
    }

    let has_target = manifest
        .map(|manifest| {
            manifest.targets.iter().any(|target| {
                available_targets.contains(&(target.capability.clone(), target.slot.clone()))
            })
        })
        .unwrap_or(true);

    if !has_target {
        return attention_extension_status(
            extension,
            "waiting_for_app",
            "Needs setup",
            "Install a compatible app to use this connector.",
            "finish_setup",
            "Finish setup",
        );
    }

    ready_extension_status(
        extension,
        "ready",
        "Ready",
        "This connector is installed and ready for compatible apps.",
        "open",
        "Open",
    )
}

async fn summarize_module_extension(
    store: &ExtensionStore<'_>,
    extension: &Extension,
    manifest: Option<&ExtensionManifest>,
    instances: &[ExtensionInstance],
    providers_by_instance: &HashMap<Uuid, Vec<Provider>>,
    failed_bindings_by_consumer: &HashMap<Uuid, usize>,
) -> anyhow::Result<ExtensionStatusSummaryItem> {
    if !extension.enabled {
        return Ok(disabled_extension_status(
            extension,
            "disabled",
            "Disabled",
            "This extension is installed but turned off.",
            "enable",
            "Enable",
        ));
    }

    if instances.is_empty() {
        return Ok(attention_extension_status(
            extension,
            "missing_instance",
            "Needs setup",
            "Create an instance to start using this extension.",
            "finish_setup",
            "Finish setup",
        ));
    }

    let required_secret_keys = manifest
        .and_then(|manifest| required_secrets_from_manifest(manifest).ok())
        .unwrap_or_default();

    let mut enabled_instance_count = 0usize;
    let mut provider_count = 0usize;
    let mut missing_secret_count = 0usize;
    let mut unhealthy_provider_count = 0usize;
    let mut degraded_provider_count = 0usize;
    let mut failed_binding_count = 0usize;

    for instance in instances {
        if !instance.enabled {
            continue;
        }
        enabled_instance_count += 1;

        if !required_secret_keys.is_empty() {
            missing_secret_count += missing_required_secrets_for_instance(
                store,
                instance.instance_id,
                &required_secret_keys,
            )
            .await?
            .len();
        }

        if let Some(providers) = providers_by_instance.get(&instance.instance_id) {
            provider_count += providers.len();
            for provider in providers {
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
        return Ok(attention_extension_status(
            extension,
            "missing_required_secrets",
            "Needs setup",
            &format!(
                "Finish setup to add the {} required {noun} still missing.",
                missing_secret_count
            ),
            "finish_setup",
            "Finish setup",
        ));
    }

    if enabled_instance_count == 0 {
        return Ok(disabled_extension_status(
            extension,
            "instances_disabled",
            "Disabled",
            "All instances for this extension are turned off.",
            "finish_setup",
            "Open",
        ));
    }

    if provider_count == 0 {
        return Ok(attention_extension_status(
            extension,
            "provider_not_ready",
            "Needs setup",
            "This extension is still finishing setup.",
            "finish_setup",
            "Finish setup",
        ));
    }

    if unhealthy_provider_count > 0 || failed_binding_count > 0 {
        let description = if unhealthy_provider_count > 0 && failed_binding_count > 0 {
            "This extension has connection problems and is not working normally."
        } else if unhealthy_provider_count > 0 {
            "This extension is not responding normally right now."
        } else {
            "This extension has a broken connection that needs repair."
        };
        return Ok(attention_extension_status(
            extension,
            "connection_issue",
            "Connection issue",
            description,
            "fix",
            "Fix",
        ));
    }

    if degraded_provider_count > 0 {
        return Ok(attention_extension_status(
            extension,
            "degraded_runtime",
            "Needs attention",
            "This extension is working, but it needs attention.",
            "fix",
            "Fix",
        ));
    }

    let description = format!(
        "{} instance{} configured and working normally.",
        enabled_instance_count,
        if enabled_instance_count == 1 { "" } else { "s" }
    );
    Ok(ready_extension_status(
        extension,
        "ready",
        "Ready",
        &description,
        "open",
        "Open",
    ))
}

fn extension_status_sort_order(severity: &str) -> usize {
    match severity {
        "attention" => 0,
        "ready" => 1,
        "disabled" => 2,
        _ => 3,
    }
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
        optional_addons: Vec::new(),
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
        "v1" | "balanced-v1" => Some(DownloaderPerformanceProfile::Balanced),
        "aggressive-v1" => Some(DownloaderPerformanceProfile::Aggressive),
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
    summary: ExtensionStatusSummaryItem,
    instances: Vec<ExtensionInstance>,
    selected_instance: Option<ExtensionInstance>,
    providers: Vec<Provider>,
    selected_provider: Option<Provider>,
}

#[derive(Debug, Default, Clone)]
struct ExtensionControlLiveSnapshot {
    version: Option<String>,
    metrics: Vec<ExtensionControlMetric>,
}

async fn build_extension_control_surface(
    state: &AppState,
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> anyhow::Result<ExtensionControlSurface> {
    let context = load_extension_control_context(store, extension_id).await?;
    let live_snapshot = load_extension_control_live_snapshot(state, store, &context)
        .await
        .unwrap_or_default();

    let mut details = Vec::new();
    if !context.summary.description.trim().is_empty() {
        details.push(context.summary.description.clone());
    }
    if context.selected_instance.is_none() && !context.instances.is_empty() {
        details.push("Create or enable a default instance to manage this extension here.".to_string());
    }
    let status = ExtensionControlStatus {
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
    let mut sections = Vec::new();
    if let Some(section) = build_extension_control_settings_section(state, store, &context).await? {
        sections.push(section);
    }
    if let Some(section) = build_extension_control_managed_items_section(store, &context).await? {
        sections.push(section);
    }
    if let Some(section) = build_extension_control_service_section(&context, &live_snapshot) {
        sections.push(section);
    }
    sections.push(build_extension_control_overview_section(&context));

    Ok(ExtensionControlSurface {
        extension_id: context.extension.extension_id.clone(),
        name: context.extension.name.clone(),
        version: context.extension.version.clone(),
        kind: context.extension.kind.clone(),
        trust_level: context.extension.trust_level.clone(),
        enabled: context.extension.enabled,
        instance_id: context.selected_instance.as_ref().map(|instance| instance.instance_id),
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

async fn load_extension_control_context(
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> anyhow::Result<ExtensionControlContext> {
    let extension = store
        .get_extension(extension_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("extension not found"))?;
    let summary = build_extension_status_summary(store)
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
    let selected_provider = choose_extension_control_provider(extension_id, &providers);

    Ok(ExtensionControlContext {
        extension,
        summary,
        instances,
        selected_instance,
        providers,
        selected_provider,
    })
}

fn choose_extension_control_instance(
    instances: &[ExtensionInstance],
) -> Option<ExtensionInstance> {
    let mut enabled: Vec<_> = instances.iter().filter(|instance| instance.enabled).cloned().collect();
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
    extension_id: &str,
    providers: &[Provider],
) -> Option<Provider> {
    let extension_id = extension_id.to_ascii_lowercase();
    let preferred_capability = if extension_id.contains("sonarr") {
        Some("media.manager.tv")
    } else if extension_id.contains("radarr") {
        Some("media.manager.movies")
    } else if extension_id.contains("prowlarr") {
        Some("indexer.registry")
    } else if extension_id.contains("qbittorrent") {
        Some("downloader.torrent")
    } else if extension_id.contains("nzbget") {
        Some("downloader.nzb")
    } else {
        None
    };

    if let Some(capability) = preferred_capability {
        if let Some(provider) = providers
            .iter()
            .find(|provider| provider.capability == capability)
            .cloned()
        {
            return Some(provider);
        }
    }

    let mut sorted = providers.to_vec();
    sorted.sort_by(|left, right| left.capability.cmp(&right.capability));
    sorted.into_iter().next()
}

fn control_health_for_summary(summary: &ExtensionStatusSummaryItem) -> String {
    match summary.status_code.as_str() {
        "connection_issue" => "error".to_string(),
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
            control_text_field("instanceName", "Selected instance", "", instance.instance_name.clone()),
        );
    }

    ExtensionControlSection {
        id: "overview".to_string(),
        title: "Overview".to_string(),
        description: "High-level status for this extension.".to_string(),
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
        fields,
        entities: Vec::new(),
        actions: Vec::new(),
    })
}

const CONTROL_DEFAULTS_SETTING_PREFIX: &str = "extensions.control_defaults.instance.";

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

fn control_defaults_setting_key(instance_id: Uuid) -> String {
    format!("{CONTROL_DEFAULTS_SETTING_PREFIX}{instance_id}")
}

async fn build_extension_control_settings_section(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<Option<ExtensionControlSection>> {
    let extension_id = context.extension.extension_id.to_ascii_lowercase();

    if extension_id.contains("sonarr") || extension_id.contains("radarr") {
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

    if extension_id.contains("qbittorrent") || extension_id.contains("nzbget") {
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
                "This shared profile tunes Elixir-managed downloaders for balanced or aggressive use."
                    .to_string(),
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
        fields: Vec::new(),
        entities,
        actions: build_extension_control_manager_actions(&implementation),
    }))
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
        });
        actions.push(ExtensionControlAction {
            id: "refresh_item".to_string(),
            label: "Refresh".to_string(),
            description: "Refresh this title from the manager.".to_string(),
            kind: "secondary".to_string(),
            params: Some(json!({ "intentId": intent.intent_id.to_string() })),
            confirm_text: None,
        });
        actions.push(ExtensionControlAction {
            id: "remove_item".to_string(),
            label: "Remove".to_string(),
            description: "Remove this title from the manager and stop tracking it in Elixir."
                .to_string(),
            kind: "danger".to_string(),
            params: Some(json!({ "intentId": intent.intent_id.to_string() })),
            confirm_text: Some(format!("Remove {} from this manager?", title)),
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

fn build_extension_control_manager_actions(
    implementation: &str,
) -> Vec<ExtensionControlAction> {
    let search_label = if implementation == "sonarr" {
        "Search missing"
    } else {
        "Search missing"
    };
    vec![
        ExtensionControlAction {
            id: "refresh_manager".to_string(),
            label: "Refresh library".to_string(),
            description: "Refresh the manager so Elixir sees the latest manager state."
                .to_string(),
            kind: "secondary".to_string(),
            params: None,
            confirm_text: None,
        },
        ExtensionControlAction {
            id: "search_missing".to_string(),
            label: search_label.to_string(),
            description: "Start the manager's built-in search for monitored missing items."
                .to_string(),
            kind: "primary".to_string(),
            params: None,
            confirm_text: None,
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
        if let Some(value) = object.get("monitorOnAdd").and_then(serde_json::Value::as_bool) {
            defaults.monitor_on_add = value;
        }
        if let Some(value) = object.get("searchOnAdd").and_then(serde_json::Value::as_bool) {
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

fn build_extension_control_actions(
    context: &ExtensionControlContext,
) -> Vec<ExtensionControlAction> {
    if context.selected_provider.is_none() {
        return Vec::new();
    }

    let extension_id = context.extension.extension_id.to_ascii_lowercase();
    if extension_id.contains("sonarr")
        || extension_id.contains("radarr")
        || extension_id.contains("prowlarr")
    {
        return vec![ExtensionControlAction {
            id: "test_connection".to_string(),
            label: "Test connection".to_string(),
            description: "Check that Elixir can reach this service and read its status.".to_string(),
            kind: "primary".to_string(),
            params: None,
            confirm_text: None,
        }];
    }

    Vec::new()
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
    let context = load_extension_control_context(store, extension_id).await?;
    let Some(provider) = context.selected_provider.as_ref() else {
        anyhow::bail!("no active provider is available for this extension yet");
    };
    let Some(instance) = context.selected_instance.as_ref() else {
        anyhow::bail!("no active instance is available for this extension yet");
    };

    match action_id {
        "test_connection" => {
            let snapshot = load_extension_control_live_snapshot(state, store, &context).await?;
            let implementation = provider
                .implementation
                .as_deref()
                .unwrap_or(extension_id)
                .to_ascii_lowercase();
            let label = if implementation == "sonarr" {
                "Sonarr"
            } else if implementation == "radarr" {
                "Radarr"
            } else if implementation == "prowlarr" {
                "Prowlarr"
            } else {
                instance.instance_name.as_str()
            };
            let message = match snapshot.version {
                Some(version) => format!("{label} is reachable. Version {version}."),
                None => format!("{label} is reachable."),
            };
            Ok(message)
        }
        "search_missing" => {
            let implementation = provider
                .implementation
                .as_deref()
                .map(|value| value.trim().to_ascii_lowercase())
                .unwrap_or_default();
            let (base_url, api_key) =
                resolve_extension_control_arr_connection(state, store, &context).await?;
            execute_extension_control_manager_command(
                &implementation,
                &base_url,
                &api_key,
                action_id,
                None,
            )
            .await
        }
        "refresh_manager" => {
            let implementation = provider
                .implementation
                .as_deref()
                .map(|value| value.trim().to_ascii_lowercase())
                .unwrap_or_default();
            let (base_url, api_key) =
                resolve_extension_control_arr_connection(state, store, &context).await?;
            execute_extension_control_manager_command(
                &implementation,
                &base_url,
                &api_key,
                action_id,
                None,
            )
            .await
        }
        "search_item" | "refresh_item" | "remove_item" => {
            let implementation = provider
                .implementation
                .as_deref()
                .map(|value| value.trim().to_ascii_lowercase())
                .unwrap_or_default();
            if implementation != "sonarr" && implementation != "radarr" {
                anyhow::bail!("item actions are not supported for this extension");
            }
            let intent = resolve_extension_control_intent(store, provider.provider_id, params).await?;
            let manager_item_id = intent
                .manager_item_id
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("manager item id is not available for this item"))?;
            let manager_item_id = manager_item_id
                .parse::<i64>()
                .context("parsing manager item id")?;
            let (base_url, api_key) =
                resolve_extension_control_arr_connection(state, store, &context).await?;
            let message = execute_extension_control_manager_command(
                &implementation,
                &base_url,
                &api_key,
                action_id,
                Some(manager_item_id),
            )
            .await?;
            if action_id == "remove_item" {
                store
                    .deactivate_managed_ingest_intent(intent.intent_id)
                    .await?;
            }
            Ok(message)
        }
        _ => anyhow::bail!("unsupported control action '{action_id}'"),
    }
}

async fn load_extension_control_live_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    context: &ExtensionControlContext,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let provider = match context.selected_provider.as_ref() {
        Some(value) => value,
        None => return Ok(ExtensionControlLiveSnapshot::default()),
    };
    let instance = match context.selected_instance.as_ref() {
        Some(value) => value,
        None => return Ok(ExtensionControlLiveSnapshot::default()),
    };
    let implementation = provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let endpoint_json = provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)?;
    let base_url = resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?;

    match implementation.as_str() {
        "sonarr" => load_sonarr_control_snapshot(state, store, instance, &base_url).await,
        "radarr" => load_radarr_control_snapshot(state, store, instance, &base_url).await,
        "prowlarr" => load_prowlarr_control_snapshot(state, store, instance, &base_url).await,
        _ => Ok(ExtensionControlLiveSnapshot::default()),
    }
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
                &[&format!("api/v3/series/{item_id}?deleteFiles=false"), &format!("api/v4/series/{item_id}?deleteFiles=false")],
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
                &[&format!("api/v3/movie/{item_id}?deleteFiles=false"), &format!("api/v4/movie/{item_id}?deleteFiles=false")],
                None,
            )
            .await?;
            Ok("Radarr removed this movie.".to_string())
        }
        _ => anyhow::bail!("unsupported control action '{action_id}' for {implementation}"),
    }
}

async fn load_sonarr_control_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base_url: &str,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let api_key = resolve_control_api_key(state, store, instance, &["sonarr_api_key", "api_key"]).await?;
    let status = request_control_json(base_url, &api_key, &["api/v3/system/status", "api/v4/system/status"]).await?;
    let series = request_control_json(base_url, &api_key, &["api/v3/series", "api/v4/series"]).await?;
    let downloaders = request_control_json(
        base_url,
        &api_key,
        &["api/v3/downloadclient", "api/v4/downloadclient"],
    )
    .await?;

    Ok(ExtensionControlLiveSnapshot {
        version: status.get("version").and_then(serde_json::Value::as_str).map(str::to_string),
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
                id: "seriesCount".to_string(),
                label: "Series".to_string(),
                value: series.as_array().map(|value| value.len()).unwrap_or(0).to_string(),
            },
            ExtensionControlMetric {
                id: "downloadClientCount".to_string(),
                label: "Download clients".to_string(),
                value: downloaders
                    .as_array()
                    .map(|value| value.len())
                    .unwrap_or(0)
                    .to_string(),
            },
        ],
    })
}

async fn load_radarr_control_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base_url: &str,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let api_key = resolve_control_api_key(state, store, instance, &["radarr_api_key", "api_key"]).await?;
    let status = request_control_json(base_url, &api_key, &["api/v3/system/status", "api/v4/system/status"]).await?;
    let movies = request_control_json(base_url, &api_key, &["api/v3/movie", "api/v4/movie"]).await?;
    let downloaders = request_control_json(
        base_url,
        &api_key,
        &["api/v3/downloadclient", "api/v4/downloadclient"],
    )
    .await?;

    Ok(ExtensionControlLiveSnapshot {
        version: status.get("version").and_then(serde_json::Value::as_str).map(str::to_string),
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
                id: "movieCount".to_string(),
                label: "Movies".to_string(),
                value: movies.as_array().map(|value| value.len()).unwrap_or(0).to_string(),
            },
            ExtensionControlMetric {
                id: "downloadClientCount".to_string(),
                label: "Download clients".to_string(),
                value: downloaders
                    .as_array()
                    .map(|value| value.len())
                    .unwrap_or(0)
                    .to_string(),
            },
        ],
    })
}

async fn load_prowlarr_control_snapshot(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base_url: &str,
) -> anyhow::Result<ExtensionControlLiveSnapshot> {
    let api_key = resolve_control_api_key(state, store, instance, &["prowlarr_api_key", "api_key"]).await?;
    let status = request_control_json(base_url, &api_key, &["api/v1/system/status"]).await?;
    let indexers = request_control_json(base_url, &api_key, &["api/v1/indexer"]).await?;
    let applications = request_control_json(base_url, &api_key, &["api/v1/applications"]).await?;

    Ok(ExtensionControlLiveSnapshot {
        version: status.get("version").and_then(serde_json::Value::as_str).map(str::to_string),
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

async fn resolve_control_api_key(
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
        return resp.json::<serde_json::Value>().await.map_err(anyhow::Error::from);
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
        let resp = request
            .send()
            .await
            .map_err(anyhow::Error::from)?;
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
        let value = serde_json::from_slice::<serde_json::Value>(&bytes)
            .unwrap_or_else(|_| json!({}));
        return Ok(value);
    }

    anyhow::bail!("service endpoint is not available")
}

async fn resolve_control_provider_transport_base_url(
    instance_id: Uuid,
    endpoint: &ProviderEndpoint,
) -> anyhow::Result<String> {
    let canonical = endpoint.canonical_url()?;
    if control_endpoint_host_resolves(&endpoint.host, endpoint.port).await {
        return Ok(canonical);
    }

    if let Some(host_port) = lookup_control_docker_published_port(instance_id, endpoint.port).await?
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
    let Some(container_name) = container_names
        .lines()
        .map(str::trim)
        .find(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    let ports_json = run_control_docker_stdout(&[
        "inspect",
        "--format",
        "{{json .NetworkSettings.Ports}}",
        container_name,
    ])
    .await?;
    let ports: serde_json::Value = serde_json::from_str(ports_json.trim())?;
    let key = format!("{container_port}/tcp");
    let binding = ports
        .get(&key)
        .and_then(serde_json::Value::as_array)
        .and_then(|values| values.first());
    let Some(binding) = binding else {
        return Ok(None);
    };
    Ok(binding
        .get("HostPort")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .parse::<u16>()
        .ok())
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
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Api-Key",
        HeaderValue::from_str(api_key).map_err(anyhow::Error::from)?,
    );
    headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    Client::builder()
        .timeout(std::time::Duration::from_secs(15))
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
    Ok(Json(RunDetailResponse { run, steps }))
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

    for connector in &manifest.connectors {
        let trimmed = connector.trim();
        if trimmed.is_empty() || trimmed == blueprint_id {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            out.push(trimmed.to_string());
        }
    }

    if let Some(preferences) = manifest.preferences.as_ref() {
        for provider_preference in preferences.providers.values() {
            for extension_id in &provider_preference.prefer {
                let trimmed = extension_id.trim();
                if trimmed.is_empty() || trimmed == blueprint_id {
                    continue;
                }
                if seen.insert(trimmed.to_string()) {
                    out.push(trimmed.to_string());
                }
            }
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
) -> ApiResult<()> {
    let existing_version = Version::parse(&existing.version)
        .map_err(|_| ApiError::bad_request("existing extension version is not valid semver"))?;
    if new_version < &existing_version {
        return Err(ApiError::bad_request(
            "extension version downgrade is not allowed",
        ));
    }
    if new_version == &existing_version {
        if let (Some(existing_hash), Some(new_hash)) =
            (existing.package_hash.as_deref(), package_hash)
        {
            if existing_hash.eq_ignore_ascii_case(new_hash) {
                return Ok(());
            }
        }
        return Err(ApiError::bad_request(
            "extension version is already installed",
        ));
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
