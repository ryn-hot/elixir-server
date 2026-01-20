use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use crate::config::RunEnvironment;
use crate::db::models::{
    Binding, Extension, ExtensionInstance, ExtensionKind, ExtensionTrustLevel, OperationStep,
    OperationStepStatus, OrchestratorRun, OrchestratorRunStatus, Provider, ProviderHealthState,
    RuntimeLog,
};
use crate::extensions::package::{
    PackageManifest, compute_sha256, read_manifest_from_dir, read_package_signature,
    unpack_package, verify_signature,
};
use crate::extensions::registry::{RegistryClient, RegistryEntry, merge_indexes};
use crate::extensions::store::{
    ExtensionStore, NewExtension, NewExtensionInstance, NewOperationStep, NewOrchestratorRun,
};
use crate::http::error::{ApiError, ApiResult};
use crate::orchestrator::plan_executor::{PlanExecutor, PlannedStep};
use crate::orchestrator::planner::{Plan, Planner};
use crate::state::AppState;

#[derive(Debug, Serialize)]
pub struct CatalogResponse {
    pub installed: Vec<Extension>,
    pub available: Vec<RegistryEntry>,
    pub registry_errors: Vec<RegistryError>,
}

#[derive(Debug, Serialize)]
pub struct RegistryError {
    pub url: String,
    pub error: String,
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
pub struct RunDetailResponse {
    pub run: OrchestratorRun,
    pub steps: Vec<OperationStep>,
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
    let plan = planner
        .plan_blueprint(&store, payload.blueprint_id, payload.params)
        .await
        .map_err(ApiError::from)?;
    let plan_json = serde_json::to_value(&plan).map_err(|err| ApiError::internal(err.to_string()))?;
    store
        .create_run(&NewOrchestratorRun {
            run_id: plan.plan_id,
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
) -> ApiResult<Json<PlanRunResponse>> {
    let run_id =
        Uuid::parse_str(&plan_id).map_err(|_| ApiError::bad_request("invalid plan id"))?;
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

    if !plan.conflicts.is_empty() {
        return Err(ApiError::conflict("plan has unresolved conflicts"));
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
                .update_run_status(run_id, OrchestratorRunStatus::Completed, Some("completed"), None)
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
    let run_id =
        Uuid::parse_str(&plan_id).map_err(|_| ApiError::bad_request("invalid plan id"))?;
    let store = ExtensionStore::new(&state.db_pool);
    let run = store
        .get_run(run_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("plan not found"))?;

    if run.status != OrchestratorRunStatus::Pending {
        return Err(ApiError::conflict("plan cannot be canceled in current state"));
    }

    store
        .update_run_status(run_id, OrchestratorRunStatus::Canceled, Some("canceled"), None)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(PlanRunResponse {
        run_id,
        status: OrchestratorRunStatus::Canceled,
    }))
}

pub async fn catalog(State(state): State<AppState>) -> ApiResult<Json<CatalogResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let installed = store.list_extensions().await.map_err(ApiError::from)?;

    let mut registry_errors = Vec::new();
    let mut available = Vec::new();
    if !state.settings.extensions.registries.is_empty() {
        let client = RegistryClient::new(Duration::from_secs(10))
            .map_err(|err| ApiError::internal(err.to_string()))?;
        let mut indexes = Vec::new();
        for url in &state.settings.extensions.registries {
            match client.fetch(url).await {
                Ok(index) => indexes.push(index),
                Err(err) => registry_errors.push(RegistryError {
                    url: url.clone(),
                    error: err.to_string(),
                }),
            }
        }
        let merged = merge_indexes(indexes);
        available = merged.extensions;
    }

    Ok(Json(CatalogResponse {
        installed,
        available,
        registry_errors,
    }))
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

pub async fn install_extension(
    State(state): State<AppState>,
    Json(payload): Json<InstallRequest>,
) -> ApiResult<Json<InstallResponse>> {
    let storage_paths = ExtensionStoragePaths::new(&state.settings.extensions.storage_root);
    storage_paths.ensure_dirs().await.map_err(ApiError::from)?;

    let is_dev = state.settings.environment == RunEnvironment::Development;
    let allow_unsigned = is_dev && state.settings.extensions.allow_unsigned;
    let allow_directory_install =
        is_dev && state.settings.extensions.allow_directory_install;

    let package_path = match (&payload.download_url, &payload.package_path) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request("provide download_url or package_path, not both"))
        }
        (Some(url), None) => {
            download_package(url, &storage_paths.packages_dir)
                .await
                .map_err(ApiError::from)?
        }
        (None, Some(path)) => PathBuf::from(path),
        (None, None) => {
            return Err(ApiError::bad_request("download_url or package_path is required"))
        }
    };

    if !package_path.exists() {
        return Err(ApiError::bad_request("package path does not exist"));
    }

    let staging_dir = storage_paths.tmp_dir.join(Uuid::new_v4().to_string());
    let mut package_hash = None;
    let staged = if package_path.is_dir() {
        if !allow_directory_install {
            return Err(ApiError::bad_request(
                "directory installs are only allowed in development with extensions.allow_directory_install=true",
            ));
        }
        if !allow_unsigned {
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
        return Err(ApiError::bad_request("package path is not a file or directory"));
    };

    let PackageManifest {
        manifest,
        raw_json,
        ..
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
        } else if !allow_unsigned {
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

    let store = ExtensionStore::new(&state.db_pool);
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
    let store = ExtensionStore::new(&state.db_pool);
    let existing = store
        .get_extension(&extension_id)
        .await
        .map_err(ApiError::from)?;
    if existing.is_none() {
        return Err(ApiError::not_found("extension not found"));
    }
    store
        .delete_extension(&extension_id)
        .await
        .map_err(ApiError::from)?;

    let storage_paths = ExtensionStoragePaths::new(&state.settings.extensions.storage_root);
    let _ = fs::remove_dir_all(storage_paths.unpacked_dir.join(&extension_id)).await;

    Ok(Json(serde_json::json!({ "status": "deleted" })))
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
        return Err(ApiError::bad_request("blueprint extensions cannot create instances"));
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

pub async fn graph(State(state): State<AppState>) -> ApiResult<Json<GraphResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = store.list_providers(None).await.map_err(ApiError::from)?;
    let bindings = store.list_bindings().await.map_err(ApiError::from)?;
    Ok(Json(GraphResponse { providers, bindings }))
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
    let instance_id = Uuid::parse_str(&instance_id)
        .map_err(|_| ApiError::bad_request("invalid instance id"))?;
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

pub async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<RunsQuery>,
) -> ApiResult<Json<Vec<OrchestratorRun>>> {
    let store = ExtensionStore::new(&state.db_pool);
    let runs = store.list_runs(query.limit).await.map_err(ApiError::from)?;
    Ok(Json(runs))
}

pub async fn run_detail(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<RunDetailResponse>> {
    let run_id =
        Uuid::parse_str(&run_id).map_err(|_| ApiError::bad_request("invalid run id"))?;
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
}

impl ExtensionStoragePaths {
    fn new(root: &str) -> Self {
        let root = PathBuf::from(root);
        Self {
            root: root.clone(),
            packages_dir: root.join("packages"),
            unpacked_dir: root.join("unpacked"),
            tmp_dir: root.join("tmp"),
        }
    }

    async fn ensure_dirs(&self) -> Result<(), anyhow::Error> {
        fs::create_dir_all(&self.root).await?;
        fs::create_dir_all(&self.packages_dir).await?;
        fs::create_dir_all(&self.unpacked_dir).await?;
        fs::create_dir_all(&self.tmp_dir).await?;
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

fn map_unique_violation(err: anyhow::Error, message: &str) -> ApiError {
    let details = err.to_string();
    if details.contains("UNIQUE") || details.contains("unique") {
        return ApiError::conflict(message);
    }
    ApiError::internal(details)
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
