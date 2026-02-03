use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

use crate::config::Settings;
use crate::db::models::{BindingStatus, ExtensionKind, ProviderHealthState};
use crate::extensions::manifest::ExtensionManifest;
use crate::extensions::store::{ExtensionStore, NewBinding, ProviderDetails};
use crate::orchestrator::executor::{Executor, ExecutorAction};
use crate::orchestrator::lock::APPLY_LOCK_NAME;
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::naming::{build_aliases, container_name};
use crate::orchestrator::plan_validation::{
    has_unresolved_conflicts, missing_required_secrets_for_plan,
};
use crate::orchestrator::planner::{Plan, PlanDecisions, Planner};
use crate::runtime::model::ContainerHandle;
use crate::runtime::probe::ProbeRunner;
use crate::runtime::{RuntimeManager, RuntimePaths};
use crate::secrets::SecretsManager;
use crate::metrics;

#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    pub interval: Duration,
    pub retry_attempts: u32,
    pub retry_backoff: Duration,
    pub lock_ttl: Duration,
}

impl ReconcileConfig {
    pub fn from_settings(settings: &Settings) -> Self {
        let interval = Duration::from_secs(
            settings
                .extensions
                .reconcile_interval_seconds
                .max(1),
        );
        let retry_attempts = settings.extensions.reconcile_retry_attempts.max(1);
        let retry_backoff = Duration::from_secs(
            settings
                .extensions
                .reconcile_retry_backoff_seconds
                .max(1),
        );
        let lock_ttl = Duration::from_secs(
            settings
                .extensions
                .apply_lock_ttl_seconds
                .max(1),
        );
        Self {
            interval,
            retry_attempts,
            retry_backoff,
            lock_ttl,
        }
    }
}

pub struct Reconciler<'a> {
    pool: &'a sqlx::AnyPool,
    store: ExtensionStore<'a>,
    probe: &'a dyn ProbeRunner,
    runtime: &'a dyn RuntimeManager,
    runtime_paths: RuntimePaths,
    drivers: &'a crate::drivers::DriverRegistry,
    secrets: &'a SecretsManager,
    retry_attempts: u32,
    retry_backoff: Duration,
    lock_ttl: Duration,
}

impl<'a> Reconciler<'a> {
    pub fn new(
        pool: &'a sqlx::AnyPool,
        probe: &'a dyn ProbeRunner,
        runtime: &'a dyn RuntimeManager,
        drivers: &'a crate::drivers::DriverRegistry,
        runtime_paths: RuntimePaths,
        secrets: &'a SecretsManager,
        config: &ReconcileConfig,
    ) -> Self {
        Self {
            pool,
            store: ExtensionStore::new(pool),
            probe,
            runtime,
            runtime_paths,
            drivers,
            secrets,
            retry_attempts: config.retry_attempts.max(1),
            retry_backoff: config.retry_backoff,
            lock_ttl: config.lock_ttl,
        }
    }

    pub async fn run_once(&self) -> Result<()> {
        let owner_id = Uuid::new_v4().to_string();
        if !self
            .store
            .acquire_lock(APPLY_LOCK_NAME, &owner_id, self.lock_ttl)
            .await?
        {
            metrics::RECONCILE_RUNS
                .with_label_values(&["skipped_lock"])
                .inc();
            return Ok(());
        }

        let run_id = Uuid::new_v4();
        let result = if let Err(err) = self
            .store
            .create_run(&crate::extensions::store::NewOrchestratorRun {
                run_id,
                status: crate::db::models::OrchestratorRunStatus::Running,
                phase: Some("reconcile".to_string()),
                plan_json: None,
                error: None,
            })
            .await
        {
            Err(err)
        } else {
            self.run_once_inner(run_id).await
        };

        match result {
            Ok(()) => {
                let _ = self
                    .store
                    .update_run_status(
                        run_id,
                        crate::db::models::OrchestratorRunStatus::Completed,
                        Some("reconcile"),
                        None,
                    )
                    .await;
                metrics::RECONCILE_RUNS.with_label_values(&["ok"]).inc();
            }
            Err(ref err) => {
                let _ = self
                    .store
                    .update_run_status(
                        run_id,
                        crate::db::models::OrchestratorRunStatus::Failed,
                        Some("reconcile"),
                        Some(&err.to_string()),
                    )
                    .await;
                metrics::RECONCILE_RUNS.with_label_values(&["error"]).inc();
            }
        }

        let _ = self.store.release_lock(APPLY_LOCK_NAME, &owner_id).await;
        result
    }

    async fn run_once_inner(&self, run_id: Uuid) -> Result<()> {
        let mut step_index = 0;
        self.reconcile_desired_state(run_id, &mut step_index)
            .await?;
        let providers = self.store.list_provider_details().await?;
        let instances = self.store.list_instances(None).await?;
        let extensions = self.store.list_extensions().await?;
        let bindings = self.store.list_bindings().await?;

        let instance_map: HashMap<Uuid, _> =
            instances.into_iter().map(|inst| (inst.instance_id, inst)).collect();
        let extension_map: HashMap<_, _> = extensions
            .into_iter()
            .map(|extension| (extension.extension_id.clone(), extension))
            .collect();

        let mut manifest_cache: HashMap<String, ExtensionManifest> = HashMap::new();

        self.reconcile_providers(
            run_id,
            &mut step_index,
            &providers,
            &instance_map,
            &extension_map,
            &mut manifest_cache,
        )
        .await?;
        self.reconcile_bindings(run_id, &mut step_index, &bindings, &instance_map)
            .await?;

        Ok(())
    }

    async fn reconcile_desired_state(
        &self,
        run_id: Uuid,
        step_index: &mut i32,
    ) -> Result<()> {
        let desired = self.store.list_desired_blueprints(Some(true)).await?;
        if desired.is_empty() {
            return Ok(());
        }

        let planner = Planner::new();
        let executor = Executor::new(
            self.pool,
            self.probe,
            self.drivers,
            self.runtime,
            self.runtime_paths.clone(),
            self.secrets,
        );

        for item in desired {
            let decisions = self
                .decisions_for_desired(item.desired_id, item.decisions_json.clone())
                .await?;
            let plan = match planner
                .plan_blueprint_with_decisions(
                    &self.store,
                    item.blueprint_extension_id.clone(),
                    item.params_json.clone(),
                    decisions.as_ref(),
                )
                .await
            {
                Ok(plan) => plan,
                Err(err) => {
                    warn!(
                        "reconcile: failed to plan blueprint {}: {}",
                        item.blueprint_extension_id, err
                    );
                    metrics::RECONCILE_ACTIONS
                        .with_label_values(&["plan_blueprint", "error"])
                        .inc();
                    continue;
                }
            };

            let missing = match missing_required_secrets_for_plan(&self.store, &plan.actions).await
            {
                Ok(missing) => missing,
                Err(err) => {
                    warn!(
                        "reconcile: required secrets lookup failed for blueprint {}: {}",
                        item.blueprint_extension_id, err
                    );
                    metrics::RECONCILE_ACTIONS
                        .with_label_values(&["required_secrets", "error"])
                        .inc();
                    continue;
                }
            };
            if !missing.is_empty() {
                warn!(
                    "reconcile: blueprint {} missing required secrets: {}",
                    item.blueprint_extension_id,
                    missing.join(", ")
                );
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["required_secrets", "missing"])
                    .inc();
                continue;
            }

            if has_unresolved_conflicts(&plan.conflicts) {
                warn!(
                    "reconcile: blueprint {} has unresolved conflicts",
                    item.blueprint_extension_id
                );
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["plan_conflicts", "blocked"])
                    .inc();
                continue;
            }

            if plan.actions.is_empty() {
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["plan_actions", "empty"])
                    .inc();
                continue;
            }

            let mut failed = false;
            for action in plan.actions {
                let action_json = match serde_json::to_value(&action) {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(
                            "reconcile: failed to serialize plan action for {}: {}",
                            item.blueprint_extension_id, err
                        );
                        failed = true;
                        break;
                    }
                };
                let executor_action = match action.clone().try_into() {
                    Ok(action) => action,
                    Err(err) => {
                        warn!(
                            "reconcile: invalid plan action for {}: {}",
                            item.blueprint_extension_id, err
                        );
                        failed = true;
                        break;
                    }
                };
                let action_type = action.action_type().to_string();
                let result = self
                    .run_step(run_id, step_index, &action_type, action_json, || {
                        executor.apply(executor_action)
                    })
                    .await;
                if let Err(err) = result {
                    warn!(
                        "reconcile: plan action {} failed for {}: {}",
                        action_type, item.blueprint_extension_id, err
                    );
                    metrics::RECONCILE_ACTIONS
                        .with_label_values(&[action_type.as_str(), "error"])
                        .inc();
                    failed = true;
                    break;
                }
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&[action_type.as_str(), "ok"])
                    .inc();
            }

            if failed {
                continue;
            }
        }

        Ok(())
    }

    async fn decisions_for_desired(
        &self,
        desired_id: Uuid,
        decisions_json: Option<serde_json::Value>,
    ) -> Result<Option<PlanDecisions>> {
        if let Some(decisions_json) = decisions_json {
            match serde_json::from_value(decisions_json) {
                Ok(decisions) => return Ok(Some(decisions)),
                Err(err) => {
                    warn!(
                        "reconcile: failed to parse stored decisions for {}: {}",
                        desired_id, err
                    );
                }
            }
        }
        let run = self.store.get_run(desired_id).await?;
        let Some(run) = run else {
            return Ok(None);
        };
        let Some(plan_json) = run.plan_json else {
            return Ok(None);
        };
        let plan: Plan = match serde_json::from_value(plan_json) {
            Ok(plan) => plan,
            Err(_) => return Ok(None),
        };
        Ok(PlanDecisions::from_conflicts(&plan.conflicts))
    }

    async fn reconcile_providers(
        &self,
        run_id: Uuid,
        step_index: &mut i32,
        providers: &[ProviderDetails],
        instances: &HashMap<Uuid, crate::db::models::ExtensionInstance>,
        extensions: &HashMap<String, crate::db::models::Extension>,
        manifests: &mut HashMap<String, ExtensionManifest>,
    ) -> Result<()> {
        let executor = Executor::new(
            self.pool,
            self.probe,
            self.drivers,
            self.runtime,
            self.runtime_paths.clone(),
            self.secrets,
        );

        for detail in providers {
            let provider = &detail.provider;
            let instance = match instances.get(&provider.instance_id) {
                Some(instance) if instance.enabled => instance,
                Some(_) => continue,
                None => {
                    warn!(
                        "reconcile: provider {} missing instance {}",
                        provider.provider_id, provider.instance_id
                    );
                    let _ = self
                        .store
                        .update_provider_health(provider.provider_id, ProviderHealthState::Unhealthy)
                        .await;
                    continue;
                }
            };

            let extension = match extensions.get(&instance.extension_id) {
                Some(extension) if extension.enabled => extension,
                Some(_) => continue,
                None => {
                    warn!(
                        "reconcile: instance {} missing extension {}",
                        instance.instance_id, instance.extension_id
                    );
                    let _ = self
                        .store
                        .update_provider_health(provider.provider_id, ProviderHealthState::Unhealthy)
                        .await;
                    continue;
                }
            };

            if provider.endpoint_json.is_none() {
                let _ = self
                    .store
                    .update_provider_health(provider.provider_id, ProviderHealthState::Unhealthy)
                    .await;
                continue;
            }

            let health_step = self
                .run_step(
                    run_id,
                    step_index,
                    "health_check",
                    serde_json::json!({ "provider_id": provider.provider_id }),
                    || executor.check_provider_health(provider.provider_id),
                )
                .await;
            if health_step.is_ok() {
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["health_check", "ok"])
                    .inc();
                continue;
            }
            metrics::RECONCILE_ACTIONS
                .with_label_values(&["health_check", "error"])
                .inc();

            let mut last_err = None;
            let mut restarted = false;
            for attempt in 0..self.retry_attempts {
                if attempt > 0 {
                    sleep(self.retry_backoff).await;
                    metrics::RECONCILE_ACTIONS
                        .with_label_values(&["retry_backoff", "ok"])
                        .inc();
                }

                if !restarted {
                    let restart = self
                        .run_step(
                            run_id,
                            step_index,
                            "restart_runtime",
                            serde_json::json!({ "instance_id": instance.instance_id }),
                            || self.restart_runtime(instance, extension, manifests),
                        )
                        .await;
                    if let Err(err) = restart
                    {
                        warn!(
                            "reconcile: restart failed for instance {}: {}",
                            instance.instance_id, err
                        );
                        metrics::RECONCILE_ACTIONS
                            .with_label_values(&["restart_runtime", "error"])
                            .inc();
                    } else {
                        metrics::RECONCILE_ACTIONS
                            .with_label_values(&["restart_runtime", "ok"])
                            .inc();
                    }
                    restarted = true;
                }

                match self
                    .run_step(
                        run_id,
                        step_index,
                        "health_check",
                        serde_json::json!({ "provider_id": provider.provider_id, "attempt": attempt + 1 }),
                        || executor.check_provider_health(provider.provider_id),
                    )
                    .await
                {
                    Ok(()) => {
                        last_err = None;
                        metrics::RECONCILE_ACTIONS
                            .with_label_values(&["health_check", "ok"])
                            .inc();
                        break;
                    }
                    Err(err) => {
                        last_err = Some(err);
                        metrics::RECONCILE_ACTIONS
                            .with_label_values(&["health_check", "error"])
                            .inc();
                        continue;
                    }
                }
            }

            if let Some(err) = last_err {
                let _ = self
                    .store
                    .update_provider_health(provider.provider_id, ProviderHealthState::Unhealthy)
                    .await;
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["mark_unhealthy", "ok"])
                    .inc();
                warn!(
                    "reconcile: provider {} unhealthy after retries: {}",
                    provider.provider_id, err
                );
            }
        }

        Ok(())
    }

    async fn restart_runtime(
        &self,
        instance: &crate::db::models::ExtensionInstance,
        extension: &crate::db::models::Extension,
        manifests: &mut HashMap<String, ExtensionManifest>,
    ) -> Result<()> {
        if extension.kind != ExtensionKind::Module {
            bail!("extension '{}' has no runtime", extension.extension_id);
        }

        let manifest = if let Some(manifest) = manifests.get(&extension.extension_id) {
            manifest.clone()
        } else {
            let manifest: ExtensionManifest =
                serde_json::from_value(extension.manifest_json.clone())
                    .context("parsing extension manifest")?;
            manifest.validate()?;
            manifests.insert(extension.extension_id.clone(), manifest.clone());
            manifest
        };

        let runtime = manifest
            .runtime
            .clone()
            .ok_or_else(|| anyhow::anyhow!("module runtime is missing"))?;
        let networking = manifest.networking.clone();

        if runtime.r#type != "container" {
            bail!("unsupported runtime type '{}'", runtime.r#type);
        }

        let (aliases, _) = build_aliases(
            &extension.extension_id,
            &instance.instance_name,
            instance.instance_id,
            runtime.service_name.clone(),
        );

        let handle = ContainerHandle {
            id: container_name(instance.instance_id),
            name: container_name(instance.instance_id),
        };
        if let Err(err) = self.runtime.stop_container(&handle).await {
            warn!(
                "reconcile: failed to stop container {}: {}",
                handle.name, err
            );
        }

        let executor = Executor::new(
            self.pool,
            self.probe,
            self.drivers,
            self.runtime,
            self.runtime_paths.clone(),
            self.secrets,
        );

        executor
            .apply(ExecutorAction::EnsureRuntimeRunning {
                instance_id: instance.instance_id,
                extension_id: extension.extension_id.clone(),
                instance_name: instance.instance_name.clone(),
                runtime,
                networking,
                aliases,
            })
            .await?;

        Ok(())
    }

    async fn reconcile_bindings(
        &self,
        run_id: Uuid,
        step_index: &mut i32,
        bindings: &[crate::db::models::Binding],
        instances: &HashMap<Uuid, crate::db::models::ExtensionInstance>,
    ) -> Result<()> {
        let providers = self.store.list_providers(None).await?;
        let provider_map: HashMap<Uuid, _> =
            providers.into_iter().map(|p| (p.provider_id, p)).collect();

        let executor = Executor::new(
            self.pool,
            self.probe,
            self.drivers,
            self.runtime,
            self.runtime_paths.clone(),
            self.secrets,
        );

        for binding in bindings {
            let consumer = match provider_map.get(&binding.consumer_provider_id) {
                Some(provider) => provider,
                None => {
                    let _ = self
                        .store
                        .update_binding_status(
                            binding.binding_id,
                            BindingStatus::Failed,
                            Some("consumer provider missing"),
                        )
                        .await;
                    continue;
                }
            };
            let target = match provider_map.get(&binding.target_provider_id) {
                Some(provider) => provider,
                None => {
                    let _ = self
                        .store
                        .update_binding_status(
                            binding.binding_id,
                            BindingStatus::Failed,
                            Some("target provider missing"),
                        )
                        .await;
                    continue;
                }
            };

            if let Some(instance) = instances.get(&consumer.instance_id) {
                if !instance.enabled {
                    continue;
                }
            }
            if let Some(instance) = instances.get(&target.instance_id) {
                if !instance.enabled {
                    continue;
                }
            }

            let consumer_endpoint = match parse_endpoint(&consumer.endpoint_json) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    let _ = self
                        .store
                        .update_binding_status(
                            binding.binding_id,
                            BindingStatus::Failed,
                            Some(&format!("consumer endpoint invalid: {err}")),
                        )
                        .await;
                    continue;
                }
            };
            let provider_endpoint = match parse_endpoint(&target.endpoint_json) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    let _ = self
                        .store
                        .update_binding_status(
                            binding.binding_id,
                            BindingStatus::Failed,
                            Some(&format!("provider endpoint invalid: {err}")),
                        )
                        .await;
                    continue;
                }
            };

            let action = ExecutorAction::ApplyBinding {
                binding: NewBinding {
                    binding_id: binding.binding_id,
                    consumer_provider_id: binding.consumer_provider_id,
                    requires_capability: binding.requires_capability.clone(),
                    requires_slot_id: binding.requires_slot_id.clone(),
                    target_provider_id: binding.target_provider_id,
                    binding_params_json: binding.binding_params_json.clone(),
                    status: binding.status,
                },
                consumer_endpoint,
                provider_endpoint,
                reverse_probe: false,
            };

            let result = self
                .run_step(
                    run_id,
                    step_index,
                    "apply_binding",
                    serde_json::json!({ "binding_id": binding.binding_id }),
                    || executor.apply(action),
                )
                .await;
            if let Err(err) = result {
                warn!(
                    "reconcile: binding {} failed: {}",
                    binding.binding_id, err
                );
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["apply_binding", "error"])
                    .inc();
            } else {
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["apply_binding", "ok"])
                    .inc();
            }
        }

        Ok(())
    }

    async fn run_step<F, Fut>(
        &self,
        run_id: Uuid,
        step_index: &mut i32,
        action_type: &str,
        action_json: serde_json::Value,
        f: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let step_id = Uuid::new_v4();
        let current_index = *step_index;
        *step_index += 1;
        self.store
            .create_step(&crate::extensions::store::NewOperationStep {
                step_id,
                run_id,
                step_index: current_index,
                action_type: action_type.to_string(),
                action_json: Some(action_json),
                status: crate::db::models::OperationStepStatus::Running,
                error: None,
            })
            .await?;

        let result = f().await;
        match &result {
            Ok(()) => {
                self.store
                    .update_step_status(
                        step_id,
                        crate::db::models::OperationStepStatus::Completed,
                        None,
                    )
                    .await?;
            }
            Err(err) => {
                self.store
                    .update_step_status(
                        step_id,
                        crate::db::models::OperationStepStatus::Failed,
                        Some(&err.to_string()),
                    )
                    .await?;
            }
        }
        result
    }
}

fn parse_endpoint(value: &Option<serde_json::Value>) -> Result<ProviderEndpoint> {
    let value = value
        .clone()
        .ok_or_else(|| anyhow::anyhow!("endpoint missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(value).context("parsing provider endpoint")?;
    endpoint.validate()?;
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use serde_json::json;
    use tempfile::TempDir;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{
        BindingStatus, ExtensionKind, ExtensionTrustLevel, OrchestratorRunStatus,
        ProviderHealthState, SlotCardinality,
    };
    use crate::drivers::DriverRegistry;
    use crate::extensions::store::{
        ExtensionStore, NewBinding, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
        NewProvider,
    };
    use crate::runtime::model::{ContainerHandle, ContainerSpec, ContainerState};
    use crate::runtime::RuntimePaths;
    use crate::secrets::SecretsManager;

    #[derive(Default)]
    struct StubProbe;

    #[async_trait]
    impl ProbeRunner for StubProbe {
        async fn probe_dns(&self, _name: &str) -> Result<crate::runtime::probe::ProbeResult> {
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_tcp(
            &self,
            _host: &str,
            _port: u16,
        ) -> Result<crate::runtime::probe::ProbeResult> {
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_http(&self, _url: &str) -> Result<crate::runtime::probe::ProbeResult> {
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }
    }

    struct StubRuntime;

    #[async_trait]
    impl RuntimeManager for StubRuntime {
        async fn ensure_network(&self, _name: &str) -> Result<()> {
            Ok(())
        }

        async fn ensure_container(&self, _spec: &ContainerSpec) -> Result<ContainerHandle> {
            Ok(ContainerHandle {
                id: "stub".to_string(),
                name: "stub".to_string(),
            })
        }

        async fn get_container_handle(&self, _name: &str) -> Result<Option<ContainerHandle>> {
            Ok(Some(ContainerHandle {
                id: "stub".to_string(),
                name: "stub".to_string(),
            }))
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> Result<()> {
            Ok(())
        }

        async fn stop_container(&self, _handle: &ContainerHandle) -> Result<()> {
            Ok(())
        }

        async fn rename_container(
            &self,
            handle: &ContainerHandle,
            new_name: &str,
        ) -> Result<ContainerHandle> {
            Ok(ContainerHandle {
                id: handle.id.clone(),
                name: new_name.to_string(),
            })
        }

        async fn remove_container(&self, _handle: &ContainerHandle) -> Result<()> {
            Ok(())
        }

        async fn container_logs(
            &self,
            _handle: &ContainerHandle,
            _since: Option<chrono::DateTime<chrono::Utc>>,
        ) -> Result<String> {
            Ok(String::new())
        }

        async fn inspect(&self, _handle: &ContainerHandle) -> Result<ContainerState> {
            Ok(ContainerState {
                id: "stub".to_string(),
                name: "stub".to_string(),
                status: "running".to_string(),
                running: true,
                health: None,
            })
        }
    }

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    #[tokio::test]
    async fn reconcile_reapplies_failed_binding() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_extension(&NewExtension {
                extension_id: "ext.test".to_string(),
                name: "ext.test".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({}),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.test".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let consumer_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let consumer_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-consumer".to_string(),
            9898,
            Some("/health".to_string()),
            Some("elixir_net".to_string()),
        )?;
        let provider_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-provider".to_string(),
            7878,
            Some("/health".to_string()),
            Some("elixir_net".to_string()),
        )?;

        store
            .upsert_provider(&NewProvider {
                provider_id: consumer_id,
                instance_id,
                capability: "consumer.capability".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: None,
                endpoint_json: Some(serde_json::to_value(consumer_endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "provider.capability".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: None,
                endpoint_json: Some(serde_json::to_value(provider_endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let binding_id = Uuid::new_v4();
        store
            .upsert_binding(&NewBinding {
                binding_id,
                consumer_provider_id: consumer_id,
                requires_capability: "provider.capability".to_string(),
                requires_slot_id: "default".to_string(),
                target_provider_id: provider_id,
                binding_params_json: None,
                status: BindingStatus::Failed,
            })
            .await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir.path().join("extensions").to_string_lossy().as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            lock_ttl: Duration::from_secs(60),
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            &config,
        );
        reconciler.run_once().await?;

        let runs = store.list_runs(Some(1)).await?;
        let run = runs.first().expect("reconcile run");
        assert_eq!(run.phase.as_deref(), Some("reconcile"));
        assert_eq!(run.status, OrchestratorRunStatus::Completed);
        let steps = store.list_steps(run.run_id).await?;
        assert!(!steps.is_empty(), "reconcile should record steps");

        let bindings = store.list_bindings().await?;
        let binding = bindings
            .into_iter()
            .find(|item| item.binding_id == binding_id)
            .expect("binding");
        assert_eq!(binding.status, BindingStatus::Applied);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_applies_desired_blueprint_plan() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_extension(&NewExtension {
                extension_id: "ext.module".to_string(),
                name: "Module".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "ext.module",
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "Module",
                    "provides": [
                        {
                            "capability": "media.manager.tv",
                            "slot": "default",
                            "implementation": "sonarr"
                        }
                    ],
                    "runtime": {
                        "type": "container",
                        "image": "example/test:1",
                        "service_name": "elx-module"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 8989
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        store
            .upsert_extension(&NewExtension {
                extension_id: "blueprint.desired".to_string(),
                name: "Desired Blueprint".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Blueprint,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "blueprint.desired",
                    "version": "1.0.0",
                    "kind": "blueprint",
                    "name": "Desired Blueprint",
                    "wants": [
                        {
                            "capability": "media.manager.tv",
                            "slot": "default"
                        }
                    ],
                    "preferences": {
                        "providers": {
                            "media.manager.tv/default": {
                                "prefer": ["ext.module"]
                            }
                        }
                    },
                    "policies": {
                        "reuse_existing": false
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let desired_id = Uuid::new_v4();
        store
            .create_desired_blueprint(&NewDesiredBlueprint {
                desired_id,
                blueprint_extension_id: "blueprint.desired".to_string(),
                blueprint_version: "1.0.0".to_string(),
                params_json: None,
                decisions_json: None,
            })
            .await?;
        store.mark_desired_applied(desired_id, true).await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir.path().join("extensions").to_string_lossy().as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            lock_ttl: Duration::from_secs(60),
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            &config,
        );
        reconciler.run_once().await?;

        let instances = store.list_instances(None).await?;
        assert!(
            instances.iter().any(|instance| instance.extension_id == "ext.module"),
            "expected instance to be created"
        );

        let providers = store.list_providers(None).await?;
        assert!(
            providers.iter().any(|provider| {
                provider.capability == "media.manager.tv" && provider.slot_id == "default"
            }),
            "expected provider to be created"
        );

        let runs = store.list_runs(Some(1)).await?;
        let run = runs.first().expect("reconcile run");
        let steps = store.list_steps(run.run_id).await?;
        assert!(
            steps
                .iter()
                .any(|step| step.action_type == "ensure_instance_installed"),
            "expected ensure_instance_installed step"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_honors_persisted_keep_existing_decisions() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_extension(&NewExtension {
                extension_id: "ext.existing".to_string(),
                name: "Existing Module".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "ext.existing",
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "Existing Module",
                    "provides": [
                        {
                            "capability": "media.manager.tv",
                            "slot": "default",
                            "implementation": "sonarr"
                        }
                    ],
                    "runtime": {
                        "type": "container",
                        "image": "example/test:1",
                        "service_name": "elx-existing"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 8989
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        store
            .upsert_extension(&NewExtension {
                extension_id: "ext.new".to_string(),
                name: "New Module".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "ext.new",
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "New Module",
                    "provides": [
                        {
                            "capability": "media.manager.tv",
                            "slot": "default",
                            "implementation": "sonarr"
                        }
                    ],
                    "runtime": {
                        "type": "container",
                        "image": "example/test:1",
                        "service_name": "elx-new"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 8989
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        store
            .upsert_extension(&NewExtension {
                extension_id: "blueprint.keep".to_string(),
                name: "Keep Blueprint".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Blueprint,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "blueprint.keep",
                    "version": "1.0.0",
                    "kind": "blueprint",
                    "name": "Keep Blueprint",
                    "wants": [
                        {
                            "capability": "media.manager.tv",
                            "slot": "default"
                        }
                    ],
                    "preferences": {
                        "providers": {
                            "media.manager.tv/default": {
                                "prefer": ["ext.new"]
                            }
                        }
                    },
                    "policies": {
                        "reuse_existing": false
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let existing_instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id: existing_instance_id,
                extension_id: "ext.existing".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let existing_provider_id = Uuid::new_v4();
        let existing_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-existing".to_string(),
            8989,
            Some("/health".to_string()),
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id: existing_provider_id,
                instance_id: existing_instance_id,
                capability: "media.manager.tv".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("sonarr".to_string()),
                endpoint_json: Some(serde_json::to_value(existing_endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let decisions_json = json!({
            "slotConflicts": [
                {
                    "conflictId": "media.manager.tv/default",
                    "action": "keep_existing"
                }
            ]
        });

        let desired_id = Uuid::new_v4();
        store
            .create_desired_blueprint(&NewDesiredBlueprint {
                desired_id,
                blueprint_extension_id: "blueprint.keep".to_string(),
                blueprint_version: "1.0.0".to_string(),
                params_json: None,
                decisions_json: Some(decisions_json),
            })
            .await?;
        store.mark_desired_applied(desired_id, true).await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir.path().join("extensions").to_string_lossy().as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            lock_ttl: Duration::from_secs(60),
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            &config,
        );
        reconciler.run_once().await?;

        let instances = store.list_instances(None).await?;
        assert!(
            instances.iter().all(|instance| instance.extension_id != "ext.new"),
            "keep_existing decisions should prevent installing ext.new"
        );

        Ok(())
    }
}
