use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::watch;
use tokio::time::{sleep, timeout};
use tracing::warn;
use uuid::Uuid;

use crate::config::{DownloaderPerformanceProfile, Settings};
use crate::db::models::{
    BindingStatus, ExtensionKind, ProviderHealthState, ProviderReadinessPhase, SlotCardinality,
};
use crate::extensions::auto_managed::filter_auto_managed_runtime_missing;
use crate::extensions::manifest::ExtensionManifest;
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_runtime,
};
use crate::extensions::store::{ExtensionStore, NewBinding, ProviderDetails};
use crate::metrics;
use crate::orchestrator::executor::{Executor, ExecutorAction, deferred_dependency_message};
use crate::orchestrator::lock::APPLY_LOCK_NAME;
use crate::orchestrator::naming::{build_aliases, container_name};
use crate::orchestrator::plan_validation::{
    has_unresolved_conflicts, missing_required_secrets_for_plan,
};
use crate::orchestrator::planner::{
    Plan, PlanAction, PlanStage, Planner, ProviderSpec, RuntimeSpec, build_provider_endpoint,
    stable_provider_id,
};
use crate::runtime::docker::classify_docker_runtime_failure;
use crate::runtime::docker::describe_docker_runtime_failure;
use crate::runtime::health::DockerRuntimeSupervisor;
use crate::runtime::model::ContainerHandle;
use crate::runtime::probe::ProbeRunner;
use crate::runtime::{RuntimeManager, RuntimePaths};
use crate::secrets::SecretsManager;

#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    pub interval: Duration,
    pub retry_attempts: u32,
    pub retry_backoff: Duration,
    pub startup_settle: Duration,
    pub lock_ttl: Duration,
    pub mode: ReconcileMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileMode {
    SteadyState,
    ExplicitRepair,
}

impl ReconcileMode {
    fn run_source(self) -> &'static str {
        match self {
            Self::SteadyState => "reconcile",
            Self::ExplicitRepair => "repair",
        }
    }

    fn run_phase(self) -> &'static str {
        match self {
            Self::SteadyState => "reconcile",
            Self::ExplicitRepair => "repair",
        }
    }

    fn is_explicit_repair(self) -> bool {
        matches!(self, Self::ExplicitRepair)
    }
}

impl ReconcileConfig {
    pub fn from_settings(settings: &Settings) -> Self {
        let interval = Duration::from_secs(settings.extensions.reconcile_interval_seconds.max(1));
        let retry_attempts = settings.extensions.reconcile_retry_attempts.max(1);
        let retry_backoff =
            Duration::from_secs(settings.extensions.reconcile_retry_backoff_seconds.max(1));
        let startup_settle = Duration::from_secs(
            settings
                .extensions
                .reconcile_startup_settle_seconds
                .min(300),
        );
        let lock_ttl = Duration::from_secs(settings.extensions.apply_lock_ttl_seconds.max(1));
        Self {
            interval,
            retry_attempts,
            retry_backoff,
            startup_settle,
            lock_ttl,
            mode: ReconcileMode::SteadyState,
        }
    }

    pub fn explicit_repair_from_settings(settings: &Settings) -> Self {
        let mut config = Self::from_settings(settings);
        config.mode = ReconcileMode::ExplicitRepair;
        config
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
    wireguard_gateway_image: String,
    default_wireguard_config_secret: Option<String>,
    default_downloader_profile: DownloaderPerformanceProfile,
    retry_attempts: u32,
    retry_backoff: Duration,
    lock_ttl: Duration,
    mode: ReconcileMode,
    runtime_health: Arc<DockerRuntimeSupervisor>,
    core_extensions: Vec<String>,
}

impl<'a> Reconciler<'a> {
    pub fn new(
        pool: &'a sqlx::AnyPool,
        probe: &'a dyn ProbeRunner,
        runtime: &'a dyn RuntimeManager,
        drivers: &'a crate::drivers::DriverRegistry,
        runtime_paths: RuntimePaths,
        secrets: &'a SecretsManager,
        wireguard_gateway_image: String,
        default_wireguard_config_secret: Option<String>,
        default_downloader_profile: DownloaderPerformanceProfile,
        config: &ReconcileConfig,
    ) -> Self {
        Self::new_with_runtime_health(
            pool,
            probe,
            runtime,
            drivers,
            runtime_paths,
            secrets,
            wireguard_gateway_image,
            default_wireguard_config_secret,
            default_downloader_profile,
            config,
            Arc::new(DockerRuntimeSupervisor::new(None)),
            Vec::new(),
        )
    }

    pub fn new_with_runtime_health(
        pool: &'a sqlx::AnyPool,
        probe: &'a dyn ProbeRunner,
        runtime: &'a dyn RuntimeManager,
        drivers: &'a crate::drivers::DriverRegistry,
        runtime_paths: RuntimePaths,
        secrets: &'a SecretsManager,
        wireguard_gateway_image: String,
        default_wireguard_config_secret: Option<String>,
        default_downloader_profile: DownloaderPerformanceProfile,
        config: &ReconcileConfig,
        runtime_health: Arc<DockerRuntimeSupervisor>,
        core_extensions: Vec<String>,
    ) -> Self {
        Self {
            pool,
            store: ExtensionStore::new(pool),
            probe,
            runtime,
            runtime_paths,
            drivers,
            secrets,
            wireguard_gateway_image,
            default_wireguard_config_secret,
            default_downloader_profile,
            retry_attempts: config.retry_attempts.max(1),
            retry_backoff: config.retry_backoff,
            lock_ttl: config.lock_ttl,
            mode: config.mode,
            runtime_health,
            core_extensions,
        }
    }

    pub async fn run_once(&self) -> Result<()> {
        if let Some((until, reason)) = self.runtime_health.circuit_open_until() {
            warn!(
                "reconcile: docker runtime circuit open until {}: {}",
                until.to_rfc3339(),
                reason
            );
            metrics::RECONCILE_RUNS
                .with_label_values(&["skipped_runtime_circuit_open"])
                .inc();
            return Ok(());
        }

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

        let stale_before =
            chrono::Utc::now() - chrono::Duration::seconds(self.lock_ttl.as_secs().max(1) as i64);
        let stale_reason = "reconcile run expired and was reaped";
        if let Ok(reaped) = self
            .store
            .reap_stale_running_runs(stale_before, stale_reason)
            .await
        {
            if reaped > 0 {
                tracing::warn!("reconcile: reaped {} stale running run(s)", reaped);
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["reap_stale_runs", "ok"])
                    .inc();
            }
        } else {
            metrics::RECONCILE_ACTIONS
                .with_label_values(&["reap_stale_runs", "error"])
                .inc();
        }

        let (stop_heartbeat_tx, stop_heartbeat_rx) = watch::channel(false);
        let heartbeat_pool = self.pool.clone();
        let heartbeat_owner_id = owner_id.clone();
        let heartbeat_ttl = self.lock_ttl;
        let heartbeat_task = tokio::spawn(async move {
            let store = ExtensionStore::new(&heartbeat_pool);
            let mut stop_rx = stop_heartbeat_rx;
            let mut ticker =
                tokio::time::interval(Duration::from_secs((heartbeat_ttl.as_secs() / 2).max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let _ = store.touch_lock(APPLY_LOCK_NAME, &heartbeat_owner_id).await;
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        let run_id = Uuid::new_v4();
        let run_source = self.mode.run_source().to_string();
        let run_phase = self.mode.run_phase().to_string();
        let result = if let Err(err) = self
            .store
            .create_run(&crate::extensions::store::NewOrchestratorRun {
                run_id,
                source: run_source.clone(),
                status: crate::db::models::OrchestratorRunStatus::Running,
                phase: Some(run_phase.clone()),
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
                        Some(&run_phase),
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
                        Some(&run_phase),
                        Some(&err.to_string()),
                    )
                    .await;
                metrics::RECONCILE_RUNS.with_label_values(&["error"]).inc();
            }
        }

        let _ = stop_heartbeat_tx.send(true);
        let _ = heartbeat_task.await;
        let _ = self.store.release_lock(APPLY_LOCK_NAME, &owner_id).await;
        result
    }

    async fn run_once_inner(&self, run_id: Uuid) -> Result<()> {
        let mut step_index = 0;
        if self.mode.is_explicit_repair() {
            self.reconcile_explicit_repairs(run_id, &mut step_index)
                .await?;
        }
        let mut providers = self.store.list_provider_details().await?;
        let instances = self.store.list_instances(None).await?;
        let extensions = self.store.list_extensions().await?;
        let bindings = self.store.list_bindings().await?;

        let instance_map: HashMap<Uuid, _> = instances
            .into_iter()
            .map(|inst| (inst.instance_id, inst))
            .collect();
        let extension_map: HashMap<_, _> = extensions
            .into_iter()
            .map(|extension| (extension.extension_id.clone(), extension))
            .collect();

        let mut manifest_cache: HashMap<String, ExtensionManifest> = HashMap::new();

        let activated_standalone_providers = self
            .reconcile_standalone_module_instances(
                run_id,
                &mut step_index,
                &providers,
                &instance_map,
                &extension_map,
                &mut manifest_cache,
            )
            .await?;
        if activated_standalone_providers {
            providers = self.store.list_provider_details().await?;
        }

        self.reconcile_providers(
            run_id,
            &mut step_index,
            &providers,
            &instance_map,
            &extension_map,
            &mut manifest_cache,
        )
        .await?;
        if let Some((until, reason)) = self.runtime_health.should_defer_dependency_actions() {
            warn!(
                "reconcile: deferring dependency and binding work until {} while runtime recovers: {}",
                until.to_rfc3339(),
                reason
            );
            metrics::RECONCILE_ACTIONS
                .with_label_values(&["defer_dependency_actions", "ok"])
                .inc();
            return Ok(());
        }
        self.reconcile_bindings(run_id, &mut step_index, &bindings, &instance_map)
            .await?;

        Ok(())
    }

    async fn reconcile_standalone_module_instances(
        &self,
        run_id: Uuid,
        step_index: &mut i32,
        providers: &[ProviderDetails],
        instances: &HashMap<Uuid, crate::db::models::ExtensionInstance>,
        extensions: &HashMap<String, crate::db::models::Extension>,
        manifests: &mut HashMap<String, ExtensionManifest>,
    ) -> Result<bool> {
        let mut provider_instances = HashSet::new();
        for detail in providers {
            provider_instances.insert(detail.provider.instance_id);
        }

        let mut ordered: Vec<_> = instances.values().collect();
        ordered.sort_by(|left, right| {
            self.core_extension_order(&left.extension_id)
                .cmp(&self.core_extension_order(&right.extension_id))
                .then_with(|| left.instance_id.cmp(&right.instance_id))
        });

        let executor = Executor::new(
            self.pool,
            self.probe,
            self.drivers,
            self.runtime,
            self.runtime_paths.clone(),
            self.secrets,
        )
        .with_wireguard_gateway_image(self.wireguard_gateway_image.clone())
        .with_default_wireguard_config_secret(self.default_wireguard_config_secret.clone())
        .with_default_downloader_profile(self.default_downloader_profile);

        let mut activated = false;
        for instance in ordered {
            if let Some((until, reason)) = self.runtime_health.circuit_open_until() {
                warn!(
                    "reconcile: deferring standalone module activation while docker runtime is degraded until {}: {}",
                    until.to_rfc3339(),
                    reason
                );
                break;
            }
            if !instance.enabled || provider_instances.contains(&instance.instance_id) {
                continue;
            }

            let Some(extension) = extensions.get(&instance.extension_id) else {
                warn!(
                    "reconcile: standalone instance {} missing extension {}",
                    instance.instance_id, instance.extension_id
                );
                continue;
            };
            if !extension.enabled || extension.kind != ExtensionKind::Module {
                continue;
            }
            if manifest_uses_internal_runtime(&extension.manifest_json) {
                continue;
            }

            let manifest = if let Some(manifest) = manifests.get(&extension.extension_id) {
                manifest.clone()
            } else {
                let manifest: ExtensionManifest =
                    match serde_json::from_value(extension.manifest_json.clone()) {
                        Ok(manifest) => manifest,
                        Err(err) => {
                            warn!(
                                "reconcile: failed to parse standalone module manifest {}: {}",
                                extension.extension_id, err
                            );
                            continue;
                        }
                    };
                if let Err(err) = manifest.validate() {
                    warn!(
                        "reconcile: standalone module manifest {} is invalid: {}",
                        extension.extension_id, err
                    );
                    continue;
                }
                manifests.insert(extension.extension_id.clone(), manifest.clone());
                manifest
            };

            let Some(runtime) = manifest.runtime.clone() else {
                continue;
            };
            if manifest.provides.is_empty() {
                continue;
            }

            let required = required_secrets_from_runtime(&runtime)?;
            if !required.is_empty() {
                let missing = filter_auto_managed_runtime_missing(
                    &extension.extension_id,
                    missing_required_secrets_for_instance(
                        &self.store,
                        instance.instance_id,
                        &required,
                    )
                    .await?,
                );
                if !missing.is_empty() {
                    warn!(
                        "reconcile: standalone module {} instance {} is missing required runtime secrets: {}",
                        extension.extension_id,
                        instance.instance_id,
                        missing.join(", ")
                    );
                    continue;
                }
            }

            let networking = manifest.networking.clone();
            let (aliases, primary_alias) = build_aliases(
                &extension.extension_id,
                &instance.instance_name,
                instance.instance_id,
                runtime.service_name.clone(),
            );

            let mut provider_specs = Vec::new();
            for provide in &manifest.provides {
                let endpoint = match build_provider_endpoint(provide, &networking, &primary_alias) {
                    Ok(endpoint) => endpoint,
                    Err(err) => {
                        warn!(
                            "reconcile: standalone module {} provider {}:{} has invalid endpoint: {}",
                            extension.extension_id, provide.capability, provide.slot, err
                        );
                        continue;
                    }
                };
                provider_specs.push(ProviderSpec {
                    provider_id: stable_provider_id(
                        instance.instance_id,
                        &provide.capability,
                        &provide.slot,
                    ),
                    instance_id: instance.instance_id,
                    capability: provide.capability.clone(),
                    slot_id: provide.slot.clone(),
                    cardinality: provide.cardinality.unwrap_or(SlotCardinality::One),
                    implementation: provide.implementation.clone(),
                    scope_json: provide
                        .scope
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|err| anyhow::anyhow!("serializing provider scope: {err}"))?,
                    endpoint,
                });
            }
            if provider_specs.is_empty() {
                continue;
            }

            let runtime_action = PlanAction::EnsureRuntimeRunning {
                runtime: RuntimeSpec {
                    instance_id: instance.instance_id,
                    extension_id: extension.extension_id.clone(),
                    instance_name: instance.instance_name.clone(),
                    runtime,
                    networking: networking.clone(),
                    aliases,
                },
            };
            let action_type = runtime_action.action_type().to_string();
            let action_json = serde_json::to_value(&runtime_action)
                .context("serializing standalone runtime action")?;
            let executor_action: ExecutorAction = runtime_action.try_into()?;
            let runtime_result = self
                .run_step(run_id, step_index, &action_type, action_json, || {
                    executor.apply(executor_action)
                })
                .await;
            if let Err(err) = runtime_result {
                warn!(
                    "reconcile: standalone module runtime activation failed for {} instance {}: {}",
                    extension.extension_id, instance.instance_id, err
                );
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&[action_type.as_str(), "error"])
                    .inc();
                continue;
            }
            metrics::RECONCILE_ACTIONS
                .with_label_values(&[action_type.as_str(), "ok"])
                .inc();

            for provider in provider_specs {
                let provider_action = PlanAction::CreateOrUpdateProvider { provider };
                let action_type = provider_action.action_type().to_string();
                let action_json = serde_json::to_value(&provider_action)
                    .context("serializing standalone provider action")?;
                let executor_action: ExecutorAction = provider_action.try_into()?;
                let provider_result = self
                    .run_step(run_id, step_index, &action_type, action_json, || {
                        executor.apply(executor_action)
                    })
                    .await;
                if let Err(err) = provider_result {
                    warn!(
                        "reconcile: standalone module provider registration failed for {} instance {}: {}",
                        extension.extension_id, instance.instance_id, err
                    );
                    metrics::RECONCILE_ACTIONS
                        .with_label_values(&[action_type.as_str(), "error"])
                        .inc();
                    continue;
                }
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&[action_type.as_str(), "ok"])
                    .inc();
                activated = true;
            }
        }

        Ok(activated)
    }

    async fn reconcile_explicit_repairs(&self, run_id: Uuid, step_index: &mut i32) -> Result<()> {
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
        )
        .with_wireguard_gateway_image(self.wireguard_gateway_image.clone())
        .with_default_wireguard_config_secret(self.default_wireguard_config_secret.clone())
        .with_default_downloader_profile(self.default_downloader_profile);

        for item in desired {
            let plan = match planner
                .plan_blueprint(
                    &self.store,
                    item.blueprint_extension_id.clone(),
                    item.params_json.clone(),
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

            let repair_plan = bounded_repair_subgraph(plan);
            if repair_plan.actions.is_empty() {
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["repair_subgraph", "empty"])
                    .inc();
                continue;
            }

            for action in repair_plan.actions {
                let action_type = action.action_type().to_string();
                if plan_action_is_dependency_work(&action) {
                    if let Some((until, reason)) =
                        self.runtime_health.should_defer_dependency_actions()
                    {
                        warn!(
                            "reconcile: deferring repair action {} for {} until {} while runtime recovers: {}",
                            action_type,
                            item.blueprint_extension_id,
                            until.to_rfc3339(),
                            reason
                        );
                        metrics::RECONCILE_ACTIONS
                            .with_label_values(&[action_type.as_str(), "deferred"])
                            .inc();
                        break;
                    }
                }
                let action_json = match serde_json::to_value(&action) {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(
                            "reconcile: failed to serialize plan action for {}: {}",
                            item.blueprint_extension_id, err
                        );
                        metrics::RECONCILE_ACTIONS
                            .with_label_values(&["serialize_repair_action", "error"])
                            .inc();
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
                        metrics::RECONCILE_ACTIONS
                            .with_label_values(&["invalid_repair_action", "error"])
                            .inc();
                        break;
                    }
                };
                let result = self
                    .run_step(run_id, step_index, &action_type, action_json, || {
                        executor.apply(executor_action)
                    })
                    .await;
                if let Err(err) = result {
                    if action_type == "apply_driver_patch" {
                        if let Some(message) = deferred_dependency_message(&err) {
                            warn!(
                                "reconcile: deferring repair action {} for {}: {}",
                                action_type, item.blueprint_extension_id, message
                            );
                            metrics::RECONCILE_ACTIONS
                                .with_label_values(&[action_type.as_str(), "deferred"])
                                .inc();
                            break;
                        }
                    }
                    warn!(
                        "reconcile: repair action {} failed for {}: {}",
                        action_type, item.blueprint_extension_id, err
                    );
                    metrics::RECONCILE_ACTIONS
                        .with_label_values(&[action_type.as_str(), "error"])
                        .inc();
                    break;
                }
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&[action_type.as_str(), "ok"])
                    .inc();
            }
        }

        Ok(())
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
        )
        .with_wireguard_gateway_image(self.wireguard_gateway_image.clone())
        .with_default_wireguard_config_secret(self.default_wireguard_config_secret.clone())
        .with_default_downloader_profile(self.default_downloader_profile);

        let mut ordered: Vec<&ProviderDetails> = providers.iter().collect();
        ordered.sort_by(|left, right| {
            let left_order = instances
                .get(&left.provider.instance_id)
                .map(|instance| self.core_extension_order(&instance.extension_id))
                .unwrap_or(usize::MAX);
            let right_order = instances
                .get(&right.provider.instance_id)
                .map(|instance| self.core_extension_order(&instance.extension_id))
                .unwrap_or(usize::MAX);
            left_order
                .cmp(&right_order)
                .then_with(|| left.provider.provider_id.cmp(&right.provider.provider_id))
        });

        for detail in ordered {
            if let Some((until, reason)) = self.runtime_health.circuit_open_until() {
                warn!(
                    "reconcile: stopping provider restarts while docker runtime is degraded until {}: {}",
                    until.to_rfc3339(),
                    reason
                );
                break;
            }

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
                        .update_provider_health(
                            provider.provider_id,
                            ProviderHealthState::Unhealthy,
                        )
                        .await;
                    let _ = self
                        .store
                        .upsert_provider_readiness(
                            provider.provider_id,
                            ProviderReadinessPhase::Unknown,
                            Some("provider instance is missing"),
                        )
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
                        .update_provider_health(
                            provider.provider_id,
                            ProviderHealthState::Unhealthy,
                        )
                        .await;
                    let _ = self
                        .store
                        .upsert_provider_readiness(
                            provider.provider_id,
                            ProviderReadinessPhase::Unknown,
                            Some("provider extension is missing"),
                        )
                        .await;
                    continue;
                }
            };
            if manifest_uses_internal_runtime(&extension.manifest_json) {
                continue;
            }

            let mut endpoint_available = provider.endpoint_json.is_some();
            match self
                .refresh_provider_runtime_endpoint(
                    &executor, provider, instance, extension, manifests,
                )
                .await
            {
                Ok(Some(changed)) => {
                    endpoint_available = true;
                    if changed {
                        tracing::info!(
                            provider_id = %provider.provider_id,
                            instance_id = %instance.instance_id,
                            "refreshed provider runtime endpoint"
                        );
                    }
                }
                Ok(None) => {}
                Err(err) => warn!(
                    "reconcile: failed to refresh runtime endpoint for provider {}: {}",
                    provider.provider_id, err
                ),
            }

            if !endpoint_available {
                let _ = self
                    .store
                    .update_provider_health(provider.provider_id, ProviderHealthState::Unhealthy)
                    .await;
                let _ = self
                    .store
                    .upsert_provider_readiness(
                        provider.provider_id,
                        ProviderReadinessPhase::Unknown,
                        Some("provider endpoint is missing"),
                    )
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
                self.runtime_health
                    .clear_instance_quarantine(instance.instance_id);
                continue;
            }
            metrics::RECONCILE_ACTIONS
                .with_label_values(&["health_check", "error"])
                .inc();

            let mut last_err = None;
            let mut restarted = false;
            for attempt in 0..self.retry_attempts {
                if let Some(quarantine) = self
                    .runtime_health
                    .quarantined_instance(instance.instance_id)
                {
                    warn!(
                        "reconcile: instance {} is quarantined until {}: {}",
                        instance.instance_id,
                        quarantine.until.to_rfc3339(),
                        quarantine.reason
                    );
                    last_err = Some(anyhow::anyhow!(quarantine.reason));
                    break;
                }
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
                    if let Err(err) = restart {
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
                        if let Err(err) = self
                            .refresh_provider_runtime_endpoint(
                                &executor, provider, instance, extension, manifests,
                            )
                            .await
                        {
                            warn!(
                                "reconcile: failed to refresh runtime endpoint after restarting provider {}: {}",
                                provider.provider_id, err
                            );
                        }
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
                        self.runtime_health
                            .clear_instance_quarantine(instance.instance_id);
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
                let detail = err.to_string();
                let _ = self
                    .store
                    .upsert_provider_readiness(
                        provider.provider_id,
                        ProviderReadinessPhase::Unknown,
                        Some(detail.as_str()),
                    )
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

    async fn refresh_provider_runtime_endpoint(
        &self,
        executor: &Executor<'_>,
        provider: &crate::db::models::Provider,
        instance: &crate::db::models::ExtensionInstance,
        extension: &crate::db::models::Extension,
        manifests: &mut HashMap<String, ExtensionManifest>,
    ) -> Result<Option<bool>> {
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
        let Some(provide) = manifest.provides.iter().find(|provide| {
            provide.capability == provider.capability && provide.slot == provider.slot_id
        }) else {
            return Ok(None);
        };
        let (_, primary_alias) = build_aliases(
            &extension.extension_id,
            &instance.instance_name,
            instance.instance_id,
            manifest
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.service_name.clone()),
        );
        let declared_endpoint =
            build_provider_endpoint(provide, &manifest.networking, &primary_alias)?;
        executor
            .refresh_provider_runtime_endpoint(provider, declared_endpoint)
            .await
            .map(Some)
    }

    async fn restart_runtime(
        &self,
        instance: &crate::db::models::ExtensionInstance,
        extension: &crate::db::models::Extension,
        manifests: &mut HashMap<String, ExtensionManifest>,
    ) -> Result<()> {
        if let Some(quarantine) = self
            .runtime_health
            .quarantined_instance(instance.instance_id)
        {
            bail!(
                "instance '{}' is quarantined until {}: {}",
                instance.instance_name,
                quarantine.until.to_rfc3339(),
                quarantine.reason
            );
        }

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

        if let Some(delay) = self.runtime_health.restart_delay() {
            sleep(delay).await;
        }
        self.runtime_health.note_restart_started();

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
            self.handle_runtime_restart_failure(instance, extension, &err);
            warn!(
                "reconcile: failed to stop container {}: {}",
                handle.name, err
            );
        }
        match self.runtime.get_container_handle(&handle.name).await {
            Ok(Some(existing)) => {
                if let Err(err) = self.runtime.remove_container(&existing).await {
                    self.handle_runtime_restart_failure(instance, extension, &err);
                    warn!(
                        "reconcile: failed to remove container {} after restart attempt: {}",
                        existing.name, err
                    );
                }
            }
            Ok(None) => {}
            Err(err) => {
                self.handle_runtime_restart_failure(instance, extension, &err);
                warn!(
                    "reconcile: failed to inspect container {} after restart attempt: {}",
                    handle.name, err
                );
            }
        }

        let executor = Executor::new(
            self.pool,
            self.probe,
            self.drivers,
            self.runtime,
            self.runtime_paths.clone(),
            self.secrets,
        )
        .with_wireguard_gateway_image(self.wireguard_gateway_image.clone())
        .with_default_wireguard_config_secret(self.default_wireguard_config_secret.clone())
        .with_default_downloader_profile(self.default_downloader_profile);

        executor
            .apply(ExecutorAction::EnsureRuntimeRunning {
                instance_id: instance.instance_id,
                extension_id: extension.extension_id.clone(),
                instance_name: instance.instance_name.clone(),
                runtime,
                networking,
                aliases,
            })
            .await
            .map_err(|err| {
                self.handle_runtime_restart_failure(instance, extension, &err);
                err
            })?;

        Ok(())
    }

    fn handle_runtime_restart_failure(
        &self,
        instance: &crate::db::models::ExtensionInstance,
        extension: &crate::db::models::Extension,
        err: &anyhow::Error,
    ) {
        let Some(kind) = classify_docker_runtime_failure(err) else {
            return;
        };
        self.runtime_health
            .record_engine_failure(kind.code(), describe_docker_runtime_failure(kind, err));
        if matches!(
            kind,
            crate::runtime::docker::DockerRuntimeFailureKind::EngineKillStuck
                | crate::runtime::docker::DockerRuntimeFailureKind::EngineDeadlineExceeded
        ) {
            let quarantine = self.runtime_health.quarantine_instance(
                instance.instance_id,
                extension.extension_id.clone(),
                extension.name.clone(),
                instance.instance_name.clone(),
                err.to_string(),
            );
            warn!(
                "reconcile: quarantined instance {} until {}: {}",
                quarantine.instance_id,
                quarantine.until.to_rfc3339(),
                quarantine.reason
            );
        }
    }

    fn core_extension_order(&self, extension_id: &str) -> usize {
        self.core_extensions
            .iter()
            .position(|value| value == extension_id)
            .unwrap_or(self.core_extensions.len().saturating_add(1))
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
        )
        .with_wireguard_gateway_image(self.wireguard_gateway_image.clone())
        .with_default_wireguard_config_secret(self.default_wireguard_config_secret.clone())
        .with_default_downloader_profile(self.default_downloader_profile);

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
            if let Some(reason) =
                binding_dependency_not_ready_reason(&self.store, consumer, target).await?
            {
                warn!(
                    "reconcile: deferring binding {} until provider dependencies are readable: {}",
                    binding.binding_id, reason
                );
                metrics::RECONCILE_ACTIONS
                    .with_label_values(&["apply_binding", "deferred"])
                    .inc();
                continue;
            }

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
                warn!("reconcile: binding {} failed: {}", binding.binding_id, err);
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

        let step_timeout = Duration::from_secs(self.lock_ttl.as_secs().saturating_sub(1).max(1));
        let result = match timeout(step_timeout, f()).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "step '{}' timed out after {}s",
                action_type,
                step_timeout.as_secs()
            )),
        };
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

fn bounded_repair_subgraph(plan: Plan) -> Plan {
    let mut retained_actions = Vec::new();
    let mut retained_stages = Vec::new();

    for stage in &plan.stages {
        let stage_start = retained_actions.len();
        for action in plan.actions[stage.action_start_index..stage.action_end_index]
            .iter()
            .cloned()
        {
            if repair_action_allowed(&action) {
                retained_actions.push(action);
            }
        }
        let stage_end = retained_actions.len();
        if stage_end > stage_start {
            retained_stages.push(PlanStage {
                stage_id: stage.stage_id.clone(),
                barrier: stage.barrier,
                action_start_index: stage_start,
                action_end_index: stage_end,
            });
        }
    }

    Plan {
        plan_id: plan.plan_id,
        blueprint_id: plan.blueprint_id,
        params: plan.params,
        stages: retained_stages,
        actions: retained_actions,
        conflicts: Vec::new(),
        blocked_stage: None,
    }
}

fn repair_action_allowed(action: &PlanAction) -> bool {
    matches!(
        action,
        PlanAction::EnsureRuntimeRunning { .. }
            | PlanAction::CreateOrUpdateProvider { .. }
            | PlanAction::TransportGate { .. }
            | PlanAction::BootstrapGate { .. }
            | PlanAction::HealthGate { .. }
            | PlanAction::ApplyDriverPatch { .. }
            | PlanAction::ApplyBinding { .. }
    )
}

fn plan_action_is_dependency_work(action: &PlanAction) -> bool {
    matches!(
        action,
        PlanAction::ApplyDriverPatch { .. } | PlanAction::ApplyBinding { .. }
    )
}

async fn binding_dependency_not_ready_reason(
    store: &ExtensionStore<'_>,
    consumer: &crate::db::models::Provider,
    target: &crate::db::models::Provider,
) -> Result<Option<String>> {
    for (role, provider) in [("consumer", consumer), ("target", target)] {
        if !provider_requires_runtime_readiness(provider) {
            continue;
        }
        if !matches!(
            provider.health_state,
            ProviderHealthState::Healthy | ProviderHealthState::Degraded
        ) {
            return Ok(Some(format!(
                "{role} provider {} health is {}",
                provider.provider_id,
                provider.health_state.as_str()
            )));
        }
        let phase = store
            .get_provider_readiness(provider.provider_id)
            .await?
            .map(|readiness| readiness.readiness_phase)
            .unwrap_or(ProviderReadinessPhase::Unknown);
        if !readiness_satisfies(phase, ProviderReadinessPhase::DriverReady) {
            return Ok(Some(format!(
                "{role} provider {} readiness is {}; waiting for driver_ready",
                provider.provider_id,
                phase.as_str()
            )));
        }
    }
    Ok(None)
}

fn provider_requires_runtime_readiness(provider: &crate::db::models::Provider) -> bool {
    matches!(
        provider.capability.as_str(),
        "media.manager.tv"
            | "media.manager.movies"
            | "indexer.registry"
            | "downloader.torrent"
            | "downloader.nzb"
    ) || matches!(
        provider.implementation.as_deref(),
        Some("sonarr" | "radarr" | "prowlarr" | "qbittorrent" | "nzbget")
    )
}

fn manifest_uses_internal_runtime(manifest: &serde_json::Value) -> bool {
    manifest
        .pointer("/runtime/type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|runtime_type| runtime_type.eq_ignore_ascii_case("internal"))
}

fn readiness_satisfies(current: ProviderReadinessPhase, required: ProviderReadinessPhase) -> bool {
    readiness_rank(current) >= readiness_rank(required)
}

fn readiness_rank(phase: ProviderReadinessPhase) -> u8 {
    match phase {
        ProviderReadinessPhase::Unknown => 0,
        ProviderReadinessPhase::TransportReady => 1,
        ProviderReadinessPhase::BootstrapReady => 2,
        ProviderReadinessPhase::DriverReady => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::json;
    use tempfile::TempDir;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{
        BindingStatus, ExtensionKind, ExtensionTrustLevel, OrchestratorRunStatus,
        ProviderHealthState, ProviderReadinessPhase, SlotCardinality,
    };
    use crate::drivers::{
        ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, DriverRegistry, StateSnapshot,
    };
    use crate::extensions::store::{
        ExtensionStore, NewBinding, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
        NewProvider,
    };
    use crate::orchestrator::model::ProviderEndpoint;
    use crate::orchestrator::planner::{Plan, PlanAction};
    use crate::runtime::RuntimePaths;
    use crate::runtime::model::{ContainerHandle, ContainerSpec, ContainerState};
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

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            _path: &str,
        ) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            _source_path: &std::path::Path,
            _destination_path: &str,
        ) -> Result<()> {
            Ok(())
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> Result<()> {
            Ok(())
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
            Ok(false)
        }
    }

    struct RestartCaptureRuntime {
        calls: Mutex<Vec<String>>,
        base_name: String,
        stop_error: Option<String>,
    }

    impl RestartCaptureRuntime {
        fn new(base_name: String, fail_stop: bool) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                base_name,
                stop_error: fail_stop.then(|| "stop failed".to_string()),
            }
        }

        fn with_stop_error(base_name: String, stop_error: impl Into<String>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                base_name,
                stop_error: Some(stop_error.into()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls
                .lock()
                .expect("restart runtime calls lock")
                .clone()
        }
    }

    #[async_trait]
    impl RuntimeManager for RestartCaptureRuntime {
        async fn ensure_network(&self, name: &str) -> Result<()> {
            self.calls
                .lock()
                .expect("restart runtime calls lock")
                .push(format!("ensure_network:{name}"));
            Ok(())
        }

        async fn ensure_container(&self, spec: &ContainerSpec) -> Result<ContainerHandle> {
            self.calls
                .lock()
                .expect("restart runtime calls lock")
                .push(format!("ensure_container:{}", spec.name));
            Ok(ContainerHandle {
                id: spec.name.clone(),
                name: spec.name.clone(),
            })
        }

        async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
            self.calls
                .lock()
                .expect("restart runtime calls lock")
                .push(format!("get:{name}"));
            if name == self.base_name {
                Ok(Some(ContainerHandle {
                    id: name.to_string(),
                    name: name.to_string(),
                }))
            } else {
                Ok(None)
            }
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn stop_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("restart runtime calls lock")
                .push(format!("stop:{}", handle.name));
            if let Some(error) = self.stop_error.as_deref() {
                bail!(error.to_string())
            } else {
                Ok(())
            }
        }

        async fn rename_container(
            &self,
            _handle: &ContainerHandle,
            _new_name: &str,
        ) -> Result<ContainerHandle> {
            bail!("unexpected runtime call")
        }

        async fn remove_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("restart runtime calls lock")
                .push(format!("remove:{}", handle.name));
            Ok(())
        }

        async fn container_logs(
            &self,
            _handle: &ContainerHandle,
            _since: Option<chrono::DateTime<chrono::Utc>>,
        ) -> Result<String> {
            bail!("unexpected runtime call")
        }

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            _path: &str,
        ) -> Result<Option<Vec<u8>>> {
            bail!("unexpected runtime call")
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            _source_path: &std::path::Path,
            _destination_path: &str,
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
            bail!("unexpected runtime call")
        }

        async fn inspect(&self, _handle: &ContainerHandle) -> Result<ContainerState> {
            bail!("unexpected runtime call")
        }
    }

    #[derive(Default)]
    struct StubIndexerDriver;

    #[async_trait]
    impl CapabilityDriver for StubIndexerDriver {
        fn capability(&self) -> &'static str {
            "indexer.registry"
        }

        async fn read_state(&self, _ctx: DriverCtx) -> Result<StateSnapshot> {
            Ok(StateSnapshot {
                summary: None,
                activity: None,
            })
        }

        async fn apply_patch(&self, _ctx: DriverCtx, _patch: DriverPatch) -> Result<ApplyResult> {
            Ok(ApplyResult::applied())
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
    async fn binding_dependency_readiness_waits_for_arr_downloader_target() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_extension(&NewExtension {
                extension_id: "ext.arr".to_string(),
                name: "ext.arr".to_string(),
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
                extension_id: "ext.arr".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let consumer_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();
        store
            .upsert_provider(&NewProvider {
                provider_id: consumer_id,
                instance_id,
                capability: "media.manager.tv".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("sonarr".to_string()),
                scope_json: None,
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        store
            .upsert_provider_readiness(consumer_id, ProviderReadinessPhase::DriverReady, None)
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id: target_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let providers = store
            .list_providers(None)
            .await?
            .into_iter()
            .map(|provider| (provider.provider_id, provider))
            .collect::<HashMap<_, _>>();
        let reason = binding_dependency_not_ready_reason(
            &store,
            providers.get(&consumer_id).expect("consumer provider"),
            providers.get(&target_id).expect("target provider"),
        )
        .await?
        .expect("target should not be ready");
        assert!(reason.contains("target provider"));
        assert!(reason.contains("readiness is unknown"));

        store
            .upsert_provider_readiness(target_id, ProviderReadinessPhase::DriverReady, None)
            .await?;
        let providers = store
            .list_providers(None)
            .await?
            .into_iter()
            .map(|provider| (provider.provider_id, provider))
            .collect::<HashMap<_, _>>();
        let ready = binding_dependency_not_ready_reason(
            &store,
            providers.get(&consumer_id).expect("consumer provider"),
            providers.get(&target_id).expect("target provider"),
        )
        .await?;
        assert_eq!(ready, None);

        Ok(())
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
                scope_json: None,
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
                scope_json: None,
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
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths.clone(),
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
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
    async fn reconcile_defers_bindings_while_runtime_is_recovering() -> Result<()> {
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
                scope_json: None,
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
                scope_json: None,
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
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };
        let runtime_health = Arc::new(DockerRuntimeSupervisor::new(None));
        runtime_health.record_engine_failure("docker_runtime_unavailable", "daemon missing");
        runtime_health.record_engine_ready(true);

        let reconciler = Reconciler::new_with_runtime_health(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
            runtime_health,
            Vec::new(),
        );
        reconciler.run_once().await?;

        let runs = store.list_runs(Some(1)).await?;
        let run = runs.first().expect("reconcile run");
        let steps = store.list_steps(run.run_id).await?;
        assert!(
            steps.iter().all(|step| step.action_type != "apply_binding"),
            "binding work should be deferred while runtime is recovering"
        );

        let binding = store
            .list_bindings()
            .await?
            .into_iter()
            .find(|item| item.binding_id == binding_id)
            .expect("binding");
        assert_eq!(binding.status, BindingStatus::Failed);

        Ok(())
    }

    #[tokio::test]
    async fn restart_runtime_force_removes_container_when_graceful_stop_fails() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let extension_id = "ext.test.module";
        let version = "1.0.0";
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "Test Module".to_string(),
                version: version.to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": extension_id,
                    "version": version,
                    "kind": "module",
                    "name": "Test Module",
                    "runtime": {
                        "type": "container",
                        "image": "example/test:1",
                        "service_name": "svc-test"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 8080
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, version, None)
            .await?;

        let instance = store
            .get_instance(instance_id)
            .await?
            .expect("instance should exist");
        let extension = store
            .get_extension(extension_id)
            .await?
            .expect("extension should exist");

        let probe = StubProbe::default();
        let runtime = RestartCaptureRuntime::new(container_name(instance_id), true);
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };
        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths.clone(),
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
        );

        reconciler
            .restart_runtime(&instance, &extension, &mut HashMap::new())
            .await?;

        assert_eq!(
            runtime.calls(),
            vec![
                format!("stop:{}", container_name(instance_id)),
                format!("get:{}", container_name(instance_id)),
                format!("remove:{}", container_name(instance_id)),
                "ensure_network:elixir_net".to_string(),
                format!("ensure_container:{}", container_name(instance_id)),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn restart_runtime_quarantines_hard_docker_failures() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let extension_id = "ext.test.module";
        let version = "1.0.0";
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "Test Module".to_string(),
                version: version.to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": extension_id,
                    "version": version,
                    "kind": "module",
                    "name": "Test Module",
                    "runtime": {
                        "type": "container",
                        "image": "example/test:1",
                        "service_name": "elx-test"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 8080
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let extension = store
            .get_extension(extension_id)
            .await?
            .expect("extension exists");
        let instance = store
            .get_instance(instance_id)
            .await?
            .expect("instance exists");

        let probe = StubProbe::default();
        let runtime = RestartCaptureRuntime::with_stop_error(
            container_name(instance_id),
            "docker stop elx-test failed (status Some(1)): Error response from daemon: cannot stop container: elx-test: tried to kill container, but did not receive an exit event",
        );
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };
        let runtime_health = Arc::new(DockerRuntimeSupervisor::new(None));
        let reconciler = Reconciler::new_with_runtime_health(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
            runtime_health.clone(),
            Vec::new(),
        );

        reconciler
            .restart_runtime(&instance, &extension, &mut HashMap::new())
            .await?;

        let quarantine = runtime_health
            .quarantined_instance(instance_id)
            .expect("instance should be quarantined");
        assert_eq!(quarantine.extension_id, extension_id);
        assert_eq!(
            runtime_health.snapshot().state,
            crate::runtime::health::DockerRuntimeHealthState::Degraded
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_reaps_stale_running_runs_before_new_run() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let stale_run_id = Uuid::new_v4();
        store
            .create_run(&crate::extensions::store::NewOrchestratorRun {
                run_id: stale_run_id,
                source: "reconcile".to_string(),
                status: OrchestratorRunStatus::Running,
                phase: Some("reconcile".to_string()),
                plan_json: None,
                error: None,
            })
            .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE orchestrator_runs SET created_at = datetime('now', '-10 minutes') WHERE run_id = $1",
        )
        .bind(stale_run_id.to_string())
        .execute(&database.pool)
        .await?;

        let stale_step_id = Uuid::new_v4();
        store
            .create_step(&crate::extensions::store::NewOperationStep {
                step_id: stale_step_id,
                run_id: stale_run_id,
                step_index: 0,
                action_type: "health_check".to_string(),
                action_json: Some(json!({ "provider_id": Uuid::new_v4() })),
                status: crate::db::models::OperationStepStatus::Running,
                error: None,
            })
            .await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths.clone(),
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
        );
        reconciler.run_once().await?;

        let stale_run = store.get_run(stale_run_id).await?.expect("stale run");
        assert_eq!(stale_run.status, OrchestratorRunStatus::Failed);
        assert_eq!(
            stale_run.error.as_deref(),
            Some("reconcile run expired and was reaped")
        );

        let stale_steps = store.list_steps(stale_run_id).await?;
        assert_eq!(stale_steps.len(), 1);
        assert_eq!(
            stale_steps[0].status,
            crate::db::models::OperationStepStatus::Failed
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_skips_when_runtime_circuit_is_open() -> Result<()> {
        let database = setup_db().await?;
        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };
        let runtime_health = Arc::new(DockerRuntimeSupervisor::new(None));
        runtime_health
            .record_engine_failure("docker_runtime_unhealthy", "docker.raw.sock is missing");

        let reconciler = Reconciler::new_with_runtime_health(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
            runtime_health,
            Vec::new(),
        );

        reconciler.run_once().await?;

        let store = ExtensionStore::new(&database.pool);
        assert!(store.list_runs(None).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_steady_state_does_not_replay_applied_blueprints() -> Result<()> {
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
                    "execution": {
                        "packages": ["ext.module"],
                        "instances": [
                            {
                                "id": "module",
                                "extension_id": "ext.module",
                                "name": "default"
                            }
                        ],
                        "phases": [
                            {
                                "id": "install_packages",
                                "steps": [
                                    { "type": "ensure_package_installed", "extension_id": "ext.module" }
                                ]
                            },
                            {
                                "id": "create_instances",
                                "steps": [
                                    { "type": "ensure_instance_installed", "instance": "module" }
                                ]
                            },
                            {
                                "id": "start_runtime",
                                "steps": [
                                    { "type": "ensure_runtime_running", "instance": "module" }
                                ]
                            },
                            {
                                "id": "register_providers",
                                "steps": [
                                    { "type": "create_or_update_providers", "instance": "module" }
                                ]
                            }
                        ]
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
            })
            .await?;
        store.mark_desired_applied(desired_id, true).await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
        );
        reconciler.run_once().await?;

        assert!(
            store.list_instances(None).await?.is_empty(),
            "steady-state reconcile must not install desired blueprints"
        );
        let runs = store.list_runs(None).await?;
        assert_eq!(runs.len(), 1, "expected only the reconcile run");
        assert_eq!(runs[0].source, "reconcile");

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_explicit_repair_runs_bounded_subgraph_only() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_extension(&NewExtension {
                extension_id: "ext.registry".to_string(),
                name: "Registry Module".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "ext.registry",
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "Registry Module",
                    "provides": [
                        {
                            "capability": "indexer.registry",
                            "slot": "default",
                            "implementation": "test"
                        }
                    ],
                    "runtime": {
                        "type": "container",
                        "image": "example/test:1",
                        "service_name": "localhost"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 9696
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .upsert_extension(&NewExtension {
                extension_id: "ext.connector".to_string(),
                name: "Registry Connector".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Connector,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "ext.connector",
                    "version": "1.0.0",
                    "kind": "connector",
                    "name": "Registry Connector",
                    "targets": [
                        { "capability": "indexer.registry", "slot": "default" }
                    ],
                    "actions": [
                        {
                            "type": "driver_patch",
                            "target": { "capability": "indexer.registry", "slot": "default" },
                            "patch": {
                                "op": "register_apps",
                                "apps": [
                                    {
                                        "name": "Elixir",
                                        "implementation": "Test",
                                        "url": "http://example.test:9696",
                                        "enabled": true
                                    }
                                ]
                            }
                        }
                    ]
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .upsert_extension(&NewExtension {
                extension_id: "blueprint.repair".to_string(),
                name: "Repair Blueprint".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Blueprint,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": "blueprint.repair",
                    "version": "1.0.0",
                    "kind": "blueprint",
                    "name": "Repair Blueprint",
                    "execution": {
                        "packages": ["ext.registry", "ext.connector"],
                        "instances": [
                            {
                                "id": "registry",
                                "extension_id": "ext.registry",
                                "name": "default"
                            }
                        ],
                        "ownership": [
                            {
                                "domain": "indexer.registry.defaults",
                                "owner": "ext.connector"
                            }
                        ],
                        "phases": [
                            {
                                "id": "install_packages",
                                "steps": [
                                    { "type": "ensure_package_installed", "extension_id": "ext.registry" },
                                    { "type": "ensure_package_installed", "extension_id": "ext.connector" }
                                ]
                            },
                            {
                                "id": "create_instances",
                                "steps": [
                                    { "type": "ensure_instance_installed", "instance": "registry" }
                                ]
                            },
                            {
                                "id": "start_runtime",
                                "steps": [
                                    { "type": "ensure_runtime_running", "instance": "registry" }
                                ]
                            },
                            {
                                "id": "configure_registry",
                                "steps": [
                                    {
                                        "type": "apply_connector",
                                        "connector_id": "ext.connector",
                                        "target_instance": "registry",
                                        "target_capability": "indexer.registry",
                                        "ownership_domains": ["indexer.registry.defaults"]
                                    }
                                ]
                            }
                        ]
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "example.com".to_string(),
            9696,
            None,
            None,
        )?;
        let _ = store
            .create_instance(&NewExtensionInstance {
                instance_id: Uuid::new_v4(),
                extension_id: "ext.registry".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        let (instance_id, provider_id) = {
            let instances = store.list_instances(None).await?;
            let instance = instances
                .into_iter()
                .find(|instance| {
                    instance.extension_id == "ext.registry" && instance.instance_name == "default"
                })
                .expect("existing instance");
            let provider_id = Uuid::new_v4();
            store
                .upsert_provider(&NewProvider {
                    provider_id,
                    instance_id: instance.instance_id,
                    capability: "indexer.registry".to_string(),
                    slot_id: "default".to_string(),
                    cardinality: SlotCardinality::One,
                    implementation: Some("test".to_string()),
                    scope_json: None,
                    endpoint_json: Some(serde_json::to_value(endpoint)?),
                    health_state: ProviderHealthState::Healthy,
                })
                .await?;
            store
                .update_instance_runtime_version(instance.instance_id, "0.9.0", None)
                .await?;
            (instance.instance_id, provider_id)
        };

        let desired_id = Uuid::new_v4();
        store
            .create_desired_blueprint(&NewDesiredBlueprint {
                desired_id,
                blueprint_extension_id: "blueprint.repair".to_string(),
                blueprint_version: "1.0.0".to_string(),
                params_json: None,
            })
            .await?;
        store.mark_desired_applied(desired_id, true).await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let mut drivers = DriverRegistry::new();
        drivers.register(StubIndexerDriver);
        let planner = Planner::new();
        let repair_plan = planner
            .plan_blueprint(&store, "blueprint.repair".to_string(), None)
            .await?;
        let planned_patch_provider_ids = repair_plan
            .actions
            .iter()
            .filter_map(|action| match action {
                PlanAction::ApplyDriverPatch { patch } => Some(patch.target_provider_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(planned_patch_provider_ids, vec![provider_id]);
        let repair_action_types = bounded_repair_subgraph(repair_plan)
            .actions
            .into_iter()
            .map(|action| action.action_type().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            repair_action_types,
            vec![
                "ensure_runtime_running",
                "transport_gate",
                "bootstrap_gate",
                "health_gate",
                "apply_driver_patch",
            ]
        );
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::ExplicitRepair,
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
        );
        reconciler.run_once().await?;

        let run = store
            .list_runs(None)
            .await?
            .into_iter()
            .find(|run| run.source == "repair")
            .expect("repair run");
        let steps = store.list_steps(run.run_id).await?;
        let step_debug = steps
            .iter()
            .map(|step| {
                format!(
                    "{}:{:?}:{}",
                    step.action_type,
                    step.status,
                    step.error.as_deref().unwrap_or("")
                )
            })
            .collect::<Vec<_>>();
        let action_types = steps
            .iter()
            .map(|step| step.action_type.as_str())
            .collect::<Vec<_>>();
        assert!(
            !action_types.contains(&"ensure_instance_installed"),
            "bounded repair must not replay install actions: {step_debug:?}"
        );
        assert!(
            action_types.contains(&"ensure_runtime_running"),
            "repair should include runtime reconciliation: {step_debug:?}"
        );
        assert!(
            action_types.contains(&"transport_gate"),
            "repair should include transport gating: {step_debug:?}"
        );
        assert!(
            action_types.contains(&"bootstrap_gate"),
            "repair should include bootstrap gating: {step_debug:?}"
        );
        assert!(
            action_types.contains(&"health_gate"),
            "repair should include driver readiness gating: {step_debug:?}"
        );
        assert!(
            action_types.contains(&"apply_driver_patch"),
            "repair should include driver patch replay: {step_debug:?}"
        );

        let providers = store.list_providers(None).await?;
        assert!(
            providers
                .iter()
                .any(|provider| provider.provider_id == provider_id)
        );
        let instances = store.list_instances(None).await?;
        assert!(
            instances
                .iter()
                .any(|instance| instance.instance_id == instance_id)
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_activates_standalone_module_when_provider_rows_are_missing() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let extension_id = "ext.standalone.source";
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "Standalone Source".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "Standalone Source",
                    "provides": [
                        {
                            "capability": "source.test",
                            "slot": "default",
                            "implementation": "standalone_source"
                        }
                    ],
                    "runtime": {
                        "type": "container",
                        "image": "example/source:1",
                        "service_name": "elx-standalone-source"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 8097
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
        );
        reconciler.run_once().await?;

        let instance = store
            .get_instance(instance_id)
            .await?
            .expect("standalone instance");
        assert_eq!(instance.runtime_version.as_deref(), Some("1.0.0"));

        let providers = store.list_providers(Some(instance_id)).await?;
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].capability, "source.test");
        assert_eq!(providers[0].health_state, ProviderHealthState::Healthy);

        let runs = store.list_runs(Some(1)).await?;
        let run = runs.first().expect("reconcile run");
        let action_types = store
            .list_steps(run.run_id)
            .await?
            .into_iter()
            .map(|step| step.action_type)
            .collect::<Vec<_>>();
        assert!(
            action_types.contains(&"ensure_runtime_running".to_string()),
            "standalone activation should start the runtime: {action_types:?}"
        );
        assert!(
            action_types.contains(&"create_or_update_provider".to_string()),
            "standalone activation should register declared providers: {action_types:?}"
        );
        assert!(
            action_types.contains(&"health_check".to_string()),
            "newly registered provider should enter normal health reconciliation: {action_types:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_does_not_activate_instances_that_already_have_providers() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let extension_id = "elixir.modules.nzbget";
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "NZBGet".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "NZBGet",
                    "provides": [
                        {
                            "capability": "downloader.nzb",
                            "slot": "default",
                            "implementation": "nzbget"
                        }
                    ],
                    "runtime": {
                        "type": "container",
                        "image": "lscr.io/linuxserver/nzbget:latest",
                        "service_name": "elx-nzbget"
                    },
                    "networking": {
                        "service_port": {
                            "scheme": "http",
                            "container_port": 6789
                        }
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "elx-nzbget".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.nzb".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("nzbget".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(&endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let runtime = StubRuntime;
        let drivers = DriverRegistry::new();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let config = ReconcileConfig {
            interval: Duration::from_secs(1),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: ReconcileMode::SteadyState,
        };

        let reconciler = Reconciler::new(
            &database.pool,
            &probe,
            &runtime,
            &drivers,
            runtime_paths,
            &secrets,
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DownloaderPerformanceProfile::Balanced,
            &config,
        );
        reconciler.run_once().await?;

        let runs = store.list_runs(Some(1)).await?;
        let run = runs.first().expect("reconcile run");
        let action_types = store
            .list_steps(run.run_id)
            .await?
            .into_iter()
            .map(|step| step.action_type)
            .collect::<Vec<_>>();
        assert!(
            !action_types.contains(&"ensure_runtime_running".to_string()),
            "existing provider-backed instances must remain on the normal provider repair path: {action_types:?}"
        );
        assert!(
            !action_types.contains(&"create_or_update_provider".to_string()),
            "existing provider-backed instances must not be re-registered by standalone activation: {action_types:?}"
        );
        assert_eq!(action_types, vec!["health_check".to_string()]);

        let providers = store.list_providers(Some(instance_id)).await?;
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].provider_id, provider_id);
        assert_eq!(providers[0].health_state, ProviderHealthState::Healthy);

        Ok(())
    }

    #[test]
    fn bounded_repair_subgraph_filters_install_only_actions() {
        let provider_id = Uuid::new_v4();
        let instance_id = Uuid::new_v4();
        let plan = Plan {
            plan_id: Uuid::new_v4(),
            blueprint_id: "blueprint.repair".to_string(),
            params: None,
            stages: vec![
                PlanStage {
                    stage_id: "install_packages".to_string(),
                    barrier: true,
                    action_start_index: 0,
                    action_end_index: 1,
                },
                PlanStage {
                    stage_id: "repair_runtime".to_string(),
                    barrier: true,
                    action_start_index: 1,
                    action_end_index: 7,
                },
            ],
            actions: vec![
                PlanAction::EnsureInstanceInstalled {
                    instance: crate::orchestrator::planner::InstanceSpec {
                        instance_id,
                        extension_id: "ext.registry".to_string(),
                        instance_name: "default".to_string(),
                        config_json: None,
                        enabled: true,
                    },
                },
                PlanAction::EnsureRuntimeRunning {
                    runtime: crate::orchestrator::planner::RuntimeSpec {
                        instance_id,
                        extension_id: "ext.registry".to_string(),
                        instance_name: "default".to_string(),
                        runtime: crate::extensions::manifest::ManifestRuntime {
                            r#type: "container".to_string(),
                            image: Some("example/test:1".to_string()),
                            network: None,
                            service_name: Some("elx-registry".to_string()),
                            ports: Vec::new(),
                            volumes: Vec::new(),
                            env: Vec::new(),
                            egress: None,
                            security: Default::default(),
                        },
                        networking: None,
                        aliases: Vec::new(),
                    },
                },
                PlanAction::CreateOrUpdateProvider {
                    provider: crate::orchestrator::planner::ProviderSpec {
                        provider_id,
                        instance_id,
                        capability: "indexer.registry".to_string(),
                        slot_id: "default".to_string(),
                        cardinality: SlotCardinality::One,
                        implementation: Some("test".to_string()),
                        scope_json: None,
                        endpoint: ProviderEndpoint::new(
                            "http".to_string(),
                            "svc-registry".to_string(),
                            9696,
                            None,
                            Some("elixir_net".to_string()),
                        )
                        .expect("endpoint"),
                    },
                },
                PlanAction::TransportGate {
                    provider_id,
                    timeout_seconds: 60,
                },
                PlanAction::BootstrapGate {
                    provider_id,
                    timeout_seconds: 60,
                },
                PlanAction::HealthGate {
                    provider_id,
                    timeout_seconds: 60,
                },
                PlanAction::RollbackRuntime { instance_id },
            ],
            conflicts: Vec::new(),
            blocked_stage: None,
        };

        let repair = bounded_repair_subgraph(plan);
        let action_types = repair
            .actions
            .iter()
            .map(PlanAction::action_type)
            .collect::<Vec<_>>();
        assert_eq!(
            action_types,
            vec![
                "ensure_runtime_running",
                "create_or_update_provider",
                "transport_gate",
                "bootstrap_gate",
                "health_gate"
            ]
        );
        assert_eq!(repair.stages.len(), 1);
        assert_eq!(repair.stages[0].stage_id, "repair_runtime");
        assert_eq!(repair.stages[0].action_start_index, 0);
        assert_eq!(repair.stages[0].action_end_index, 5);
    }

    #[test]
    fn reconcile_config_uses_startup_settle_from_settings() {
        let mut settings = Settings::default();
        settings.extensions.reconcile_startup_settle_seconds = 23;

        let config = ReconcileConfig::from_settings(&settings);

        assert_eq!(config.startup_settle, Duration::from_secs(23));
    }

    #[test]
    fn internal_runtime_is_not_owned_by_container_reconciliation() {
        assert!(manifest_uses_internal_runtime(&json!({
            "runtime": { "type": "internal" }
        })));
        assert!(!manifest_uses_internal_runtime(&json!({
            "runtime": { "type": "container" }
        })));
    }
}
