use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::AnyPool;
use std::collections::HashSet;
use std::time::Duration;
use tokio::fs;
use tokio::net::TcpStream;

use crate::config::DownloaderPerformanceProfile;
use crate::db::models::{ExtensionInstance, Provider};
use crate::drivers::DriverRegistry;
use crate::extensions::manifest::ExtensionManifest;
use crate::extensions::store::ExtensionStore;
use crate::orchestrator::executor::{Executor, ExecutorAction, build_driver_ctx_for_provider};
use crate::orchestrator::lock::APPLY_LOCK_NAME;
use crate::orchestrator::naming::container_name;
use crate::orchestrator::reconcile::{ReconcileConfig, Reconciler};
use crate::runtime::docker::{
    DockerDaemonStatus, DockerRuntimeManager, DockerStartupConfig, classify_docker_runtime_failure,
    describe_docker_runtime_failure,
};
use crate::runtime::health::{
    DockerAutoResetDecision, DockerRuntimeHealthSnapshot, DockerRuntimeHealthState,
    DockerRuntimeSupervisor, PersistedDockerRuntimeHealthState,
    detect_docker_desktop_filesharing_warning, runtime_health_poll_interval,
};
use crate::runtime::probe::{NetworkProbe, ProbeConfig, ProbeRunner};
use crate::runtime::{RuntimeManager, RuntimePaths};
use crate::secrets::SecretsManager;

const STARTUP_STALE_INSTANCE_GRACE_MINUTES: i64 = 30;
const KNOWN_STALE_BAZARR_EXTENSION_ID: &str = "elixir.modules.bazarr";
const MANAGED_RUNTIME_NETWORKS: [&str; 1] = ["elixir_net"];
const RUNTIME_HEALTH_STATE_SETTING_KEY: &str = "docker_runtime.health";

#[derive(Debug, Clone)]
pub struct RuntimeResetOutcome {
    pub status: String,
    pub message: String,
    pub docker_restarted: bool,
    pub reboot_recommended: bool,
    pub removed_containers: Vec<String>,
    pub recreated_networks: Vec<String>,
}

#[derive(Debug, Default, Clone, Copy)]
struct CoreRuntimeLiveness {
    total: usize,
    ready: usize,
}

#[derive(Clone)]
pub struct OrchestratorService {
    pool: AnyPool,
    runtime_paths: RuntimePaths,
    wireguard_gateway_image: String,
    default_wireguard_config_secret: Option<String>,
    docker_startup: DockerStartupConfig,
    default_downloader_profile: DownloaderPerformanceProfile,
    drivers: std::sync::Arc<DriverRegistry>,
    secrets: std::sync::Arc<SecretsManager>,
    probe: std::sync::Arc<NetworkProbe>,
    runtime: std::sync::Arc<DockerRuntimeManager>,
    runtime_health: std::sync::Arc<DockerRuntimeSupervisor>,
    runtime_reset_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    core_extensions: Vec<String>,
}

impl OrchestratorService {
    pub fn new(
        pool: AnyPool,
        storage_root: String,
        bundled_dir: String,
        media_root: String,
        core_extensions: Vec<String>,
        wireguard_gateway_image: String,
        default_wireguard_config_secret: Option<String>,
        docker_startup: DockerStartupConfig,
        default_downloader_profile: DownloaderPerformanceProfile,
        secrets: std::sync::Arc<SecretsManager>,
    ) -> Self {
        let runtime_paths = RuntimePaths::from_roots(&storage_root, &media_root);
        let probe = std::sync::Arc::new(NetworkProbe::new(
            ProbeConfig::with_storage_and_bundled_dirs(&storage_root, &bundled_dir),
        ));
        let runtime = std::sync::Arc::new(DockerRuntimeManager::new(None));
        let runtime_health = std::sync::Arc::new(DockerRuntimeSupervisor::new(
            detect_docker_desktop_filesharing_warning(),
        ));
        if let Some(warning) = runtime_health.snapshot().host_warning.clone() {
            tracing::warn!("docker runtime host warning: {}", warning);
        }
        Self {
            pool,
            runtime_paths,
            wireguard_gateway_image,
            default_wireguard_config_secret,
            docker_startup,
            default_downloader_profile,
            drivers: std::sync::Arc::new(DriverRegistry::with_defaults()),
            secrets,
            probe,
            runtime,
            runtime_health,
            runtime_reset_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            core_extensions,
        }
    }

    pub async fn apply_actions(&self, actions: Vec<ExecutorAction>) -> Result<()> {
        self.ensure_runtime_ready().await?;
        self.apply_actions_with_probe(actions, self.probe.as_ref(), self.runtime.as_ref())
            .await
    }

    pub async fn prepare_probe_binary(&self) -> Result<()> {
        self.ensure_runtime_ready().await?;
        self.probe.prepare_binary().await
    }

    pub async fn remove_instance_runtime(&self, instance_id: uuid::Uuid) -> Result<()> {
        self.ensure_runtime_ready().await?;

        let base_name = container_name(instance_id);
        for name in [
            base_name.clone(),
            format!("{base_name}-rollback"),
            format!("{base_name}-vpn"),
            format!("{base_name}-vpn-rollback"),
        ] {
            if let Some(handle) = self.runtime.get_container_handle(&name).await? {
                let _ = self.runtime.stop_container(&handle).await;
                self.runtime.remove_container(&handle).await?;
            }
        }

        let wireguard_dir = std::path::Path::new(&self.runtime_paths.data_root)
            .join("extensions")
            .join("wireguard")
            .join(instance_id.to_string());
        let _ = std::fs::remove_dir_all(wireguard_dir);
        Ok(())
    }

    pub async fn read_instance_container_text_file(
        &self,
        instance_id: uuid::Uuid,
        container_path: &str,
    ) -> Result<Option<String>> {
        self.ensure_runtime_ready().await?;
        let Some(handle) = self
            .runtime
            .get_container_handle(&container_name(instance_id))
            .await?
        else {
            return Ok(None);
        };
        let Some(bytes) = self
            .runtime
            .read_container_file(&handle, container_path)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(String::from_utf8(bytes).with_context(|| {
            format!(
                "decoding container file '{}' for instance {}",
                container_path, instance_id
            )
        })?))
    }

    pub async fn replace_instance_container_text_file_and_restart(
        &self,
        instance_id: uuid::Uuid,
        container_path: &str,
        text: &str,
    ) -> Result<()> {
        self.ensure_runtime_ready().await?;
        let handle_name = container_name(instance_id);
        let handle = self
            .runtime
            .get_container_handle(&handle_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("runtime container '{}' not found", handle_name))?;

        let temp_dir =
            std::env::temp_dir().join(format!("elixir-instance-file-{}", instance_id.simple()));
        fs::create_dir_all(&temp_dir).await.with_context(|| {
            format!(
                "creating temp directory '{}' for instance file replacement",
                temp_dir.display()
            )
        })?;
        let temp_file = temp_dir.join("payload");
        fs::write(&temp_file, text.as_bytes())
            .await
            .with_context(|| {
                format!(
                    "writing temp replacement file '{}' for instance {}",
                    temp_file.display(),
                    instance_id
                )
            })?;

        self.runtime.stop_container(&handle).await?;
        let copy_result = self
            .runtime
            .copy_host_path_to_container(&handle, &temp_file, container_path)
            .await;
        let start_result = self.runtime.start_container(&handle).await;
        let _ = fs::remove_dir_all(&temp_dir).await;

        copy_result?;
        start_result?;
        Ok(())
    }

    pub async fn read_provider_state(
        &self,
        provider: &Provider,
        instance: &ExtensionInstance,
    ) -> Result<crate::drivers::StateSnapshot> {
        let driver = self
            .drivers
            .get(&provider.capability)
            .ok_or_else(|| anyhow::anyhow!("no driver registered for {}", provider.capability))?;
        let store = ExtensionStore::new(&self.pool);
        let ctx = build_driver_ctx_for_provider(
            &store,
            self.secrets.as_ref(),
            self.runtime.as_ref(),
            provider,
            instance,
        )
        .await?;
        driver.read_state(ctx).await
    }

    pub async fn apply_builtin_downloader_profiles_now(&self) -> Result<()> {
        self.ensure_runtime_ready().await?;
        let executor = Executor::new(
            &self.pool,
            self.probe.as_ref(),
            self.drivers.as_ref(),
            self.runtime.as_ref(),
            self.runtime_paths.clone(),
            self.secrets.as_ref(),
        )
        .with_wireguard_gateway_image(self.wireguard_gateway_image.clone())
        .with_default_wireguard_config_secret(self.default_wireguard_config_secret.clone())
        .with_default_downloader_profile(self.default_downloader_profile);
        executor.apply_builtin_downloader_profiles_now().await
    }

    pub fn start_reconcile_loop(self: std::sync::Arc<Self>, config: ReconcileConfig) {
        if config.interval.is_zero() {
            return;
        }
        tokio::spawn(async move {
            if !config.startup_settle.is_zero() {
                tracing::info!(
                    "orchestrator: waiting {:?} before first reconcile after startup",
                    config.startup_settle
                );
                tokio::time::sleep(config.startup_settle).await;
            }
            // Run once on startup, then always wait a full interval after each run.
            // This avoids an endless "catch-up" loop when reconcile duration exceeds interval.
            loop {
                if let Err(err) = self.reconcile_once(&config).await {
                    tracing::warn!("reconcile loop error: {}", err);
                }
                tokio::time::sleep(config.interval).await;
            }
        });
    }

    pub fn start_runtime_health_loop(self: std::sync::Arc<Self>, config: ReconcileConfig) {
        tokio::spawn(async move {
            loop {
                if let Err(err) = self.run_runtime_health_iteration(&config).await {
                    tracing::warn!("docker runtime supervisor iteration failed: {}", err);
                }
                tokio::time::sleep(runtime_health_poll_interval()).await;
            }
        });
    }

    pub async fn recover_orphaned_state_after_restart(
        &self,
        config: &ReconcileConfig,
    ) -> Result<()> {
        let store = ExtensionStore::new(&self.pool);
        let cleared = store.force_release_lock(APPLY_LOCK_NAME).await?;
        if cleared > 0 {
            tracing::warn!(
                "orchestrator: cleared {} stale apply lock(s) on startup",
                cleared
            );
        }

        // On process restart there should be no active in-flight run in this process;
        // mark leftover running runs as failed immediately so UI/API never linger at "running".
        let stale_before = Utc::now() + chrono::Duration::seconds(config.lock_ttl.as_secs() as i64);
        let reaped = store
            .reap_stale_running_runs(stale_before, "server restarted")
            .await?;
        if reaped > 0 {
            tracing::warn!(
                "orchestrator: marked {} orphaned running run(s) as failed on startup",
                reaped
            );
        }

        let canceled_blueprint_previews = store
            .cancel_pending_runs_by_source(
                "blueprint",
                Some("server restarted before plan confirmation"),
            )
            .await?;
        if canceled_blueprint_previews > 0 {
            tracing::warn!(
                "orchestrator: canceled {} stale blueprint preview run(s) on startup",
                canceled_blueprint_previews
            );
        }

        let deleted_pending_desired = store.delete_desired_blueprints(Some(false)).await?;
        if deleted_pending_desired > 0 {
            tracing::warn!(
                "orchestrator: deleted {} stale pending desired blueprint row(s) on startup",
                deleted_pending_desired
            );
        }

        let stale_before =
            Utc::now() - chrono::Duration::minutes(STARTUP_STALE_INSTANCE_GRACE_MINUTES);
        let pruned = store
            .prune_stale_suffix_instances(KNOWN_STALE_BAZARR_EXTENSION_ID, "default", stale_before)
            .await?;
        if pruned > 0 {
            tracing::warn!(
                "orchestrator: pruned {} stale startup instance(s) for {}",
                pruned,
                KNOWN_STALE_BAZARR_EXTENSION_ID
            );
        }

        let active_instance_ids: HashSet<String> = store
            .list_instances(None)
            .await?
            .into_iter()
            .map(|instance| instance.instance_id.to_string())
            .collect();
        let removed_containers = self
            .runtime
            .prune_orphaned_managed_containers(&active_instance_ids)
            .await?;
        if !removed_containers.is_empty() {
            tracing::warn!(
                "orchestrator: removed {} orphaned managed container(s) on startup: {:?}",
                removed_containers.len(),
                removed_containers
            );
        }

        let pruned_secrets = store.prune_orphaned_instance_secrets().await?;
        if pruned_secrets > 0 {
            tracing::warn!(
                "orchestrator: pruned {} orphaned instance secret(s) on startup",
                pruned_secrets
            );
        }
        Ok(())
    }

    pub async fn restore_persisted_runtime_health_state(&self) -> Result<()> {
        let store = ExtensionStore::new(&self.pool);
        let persisted = store
            .get_extension_setting(RUNTIME_HEALTH_STATE_SETTING_KEY)
            .await?;
        if let Some(value) = persisted {
            match serde_json::from_value::<PersistedDockerRuntimeHealthState>(value) {
                Ok(state) => self.runtime_health.restore(state),
                Err(err) => {
                    tracing::warn!(
                        "orchestrator: failed to parse persisted docker runtime health state: {}",
                        err
                    );
                }
            }
        }
        self.persist_runtime_health_state().await
    }

    async fn persist_runtime_health_state(&self) -> Result<()> {
        let store = ExtensionStore::new(&self.pool);
        store
            .upsert_extension_setting(
                RUNTIME_HEALTH_STATE_SETTING_KEY,
                &serde_json::to_value(self.runtime_health.persisted_state())?,
            )
            .await
    }

    async fn run_runtime_health_iteration(&self, config: &ReconcileConfig) -> Result<()> {
        match self
            .runtime
            .ensure_daemon_available(&self.docker_startup)
            .await
        {
            Ok(status) => {
                self.runtime_health
                    .record_engine_ready(matches!(status, DockerDaemonStatus::StartedByElixir));
                match self.probe_core_runtime_liveness().await {
                    Ok(progress) => self
                        .runtime_health
                        .record_recovery_progress(progress.ready, progress.total),
                    Err(err) => {
                        tracing::debug!(
                            "docker runtime supervisor: core liveness probe failed: {}",
                            err
                        );
                    }
                }
            }
            Err(err) => {
                if let Some(kind) = classify_docker_runtime_failure(&err) {
                    self.runtime_health.record_engine_failure(
                        kind.code(),
                        describe_docker_runtime_failure(kind, &err),
                    );
                    match self.runtime_health.auto_reset_decision() {
                        DockerAutoResetDecision::AttemptNow => {
                            if let Ok(_guard) = self.runtime_reset_lock.clone().try_lock_owned() {
                                self.runtime_health.note_auto_reset_attempt();
                                self.persist_runtime_health_state().await?;
                                let outcome = self.reset_elixir_runtime_inner(config).await?;
                                if outcome.reboot_recommended {
                                    self.runtime_health
                                        .mark_reboot_recommended(outcome.message.clone());
                                }
                            }
                        }
                        DockerAutoResetDecision::BudgetExceeded => {
                            self.runtime_health.mark_reboot_recommended(
                                "Elixir already used its automatic Docker recovery budget for this hour. Reboot the computer, then relaunch Elixir.",
                            );
                        }
                        DockerAutoResetDecision::NotNeeded
                        | DockerAutoResetDecision::Cooldown
                        | DockerAutoResetDecision::RebootRequired => {}
                    }
                } else {
                    return Err(err);
                }
            }
        }

        self.persist_runtime_health_state().await
    }

    async fn probe_core_runtime_liveness(&self) -> Result<CoreRuntimeLiveness> {
        let store = ExtensionStore::new(&self.pool);
        let mut instances = store.list_instances(None).await?;
        let extensions = store.list_extensions().await?;
        let extension_map: std::collections::HashMap<String, crate::db::models::Extension> =
            extensions
                .into_iter()
                .map(|extension| (extension.extension_id.clone(), extension))
                .collect();
        instances.sort_by(|left, right| {
            self.core_extension_order(&left.extension_id)
                .cmp(&self.core_extension_order(&right.extension_id))
                .then_with(|| left.instance_id.cmp(&right.instance_id))
        });

        let mut progress = CoreRuntimeLiveness::default();
        for instance in instances {
            if !instance.enabled {
                continue;
            }
            let Some(extension) = extension_map.get(&instance.extension_id) else {
                continue;
            };
            if !extension.enabled {
                continue;
            }

            let manifest: ExtensionManifest =
                match serde_json::from_value(extension.manifest_json.clone()) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
            let Some(runtime) = manifest.runtime.as_ref() else {
                continue;
            };
            if !runtime.r#type.eq_ignore_ascii_case("container") {
                continue;
            }
            let Some(networking) = manifest.networking.as_ref() else {
                continue;
            };

            progress.total += 1;

            let handle_name = container_name(instance.instance_id);
            let Some(handle) = self.runtime.get_container_handle(&handle_name).await? else {
                continue;
            };
            let state = self.runtime.inspect(&handle).await?;
            if !state.running {
                continue;
            }
            let Some(host_port) = self
                .runtime
                .lookup_published_host_port(&handle.name, networking.service_port.container_port)
                .await?
            else {
                continue;
            };

            let connect = tokio::time::timeout(
                Duration::from_secs(2),
                TcpStream::connect(("127.0.0.1", host_port)),
            )
            .await;
            if matches!(connect, Ok(Ok(_))) {
                progress.ready += 1;
            }
        }

        Ok(progress)
    }

    fn core_extension_order(&self, extension_id: &str) -> usize {
        self.core_extensions
            .iter()
            .position(|value| value == extension_id)
            .unwrap_or(self.core_extensions.len().saturating_add(1))
    }

    pub async fn reconcile_once(&self, config: &ReconcileConfig) -> Result<()> {
        self.ensure_runtime_ready().await?;
        self.reconcile_once_with_probe(config, self.probe.as_ref(), self.runtime.as_ref())
            .await
    }

    pub async fn reset_elixir_runtime(
        &self,
        config: &ReconcileConfig,
    ) -> Result<RuntimeResetOutcome> {
        let _guard = self.runtime_reset_lock.lock().await;
        self.runtime_health.note_manual_reset_attempt();
        self.persist_runtime_health_state().await?;
        let outcome = self.reset_elixir_runtime_inner(config).await?;
        self.persist_runtime_health_state().await?;
        Ok(outcome)
    }

    async fn reset_elixir_runtime_inner(
        &self,
        config: &ReconcileConfig,
    ) -> Result<RuntimeResetOutcome> {
        let snapshot = self.runtime_health.snapshot();
        let mut docker_restarted = false;
        let mut removed_containers = Vec::new();
        let mut recreated_networks = Vec::new();
        let mut should_restart_runtime = snapshot.state == DockerRuntimeHealthState::Degraded;

        match self
            .runtime
            .ensure_daemon_available(&self.docker_startup)
            .await
        {
            Ok(status) => {
                self.runtime_health
                    .record_engine_ready(matches!(status, DockerDaemonStatus::StartedByElixir));
            }
            Err(err) => {
                if let Some(kind) = classify_docker_runtime_failure(&err) {
                    self.runtime_health.record_engine_failure(
                        kind.code(),
                        describe_docker_runtime_failure(kind, &err),
                    );
                    should_restart_runtime = true;
                } else {
                    return Err(err);
                }
            }
        }

        if should_restart_runtime {
            match self
                .runtime
                .restart_docker_runtime(&self.docker_startup)
                .await
            {
                Ok(status) => {
                    docker_restarted = true;
                    self.runtime_health
                        .record_engine_ready(matches!(status, DockerDaemonStatus::StartedByElixir));
                }
                Err(err) => {
                    if let Some(kind) = classify_docker_runtime_failure(&err) {
                        self.runtime_health.mark_reboot_recommended(format!(
                            "{} Elixir could not recover Docker automatically; reboot the host and relaunch Elixir.",
                            describe_docker_runtime_failure(kind, &err)
                        ));
                        return Ok(RuntimeResetOutcome {
                            status: "reboot_recommended".to_string(),
                            message: format!(
                                "Elixir could not restart Docker cleanly ({}). Reboot the computer, then relaunch Elixir.",
                                kind.code()
                            ),
                            docker_restarted: false,
                            reboot_recommended: true,
                            removed_containers,
                            recreated_networks,
                        });
                    }
                    return Err(err);
                }
            }
        }

        match self.runtime.stop_and_remove_managed_containers().await {
            Ok(removed) => removed_containers = removed,
            Err(err) => {
                if let Some(kind) = classify_docker_runtime_failure(&err) {
                    self.runtime_health.record_engine_failure(
                        kind.code(),
                        describe_docker_runtime_failure(kind, &err),
                    );
                    if !docker_restarted {
                        match self
                            .runtime
                            .restart_docker_runtime(&self.docker_startup)
                            .await
                        {
                            Ok(status) => {
                                docker_restarted = true;
                                self.runtime_health.record_engine_ready(matches!(
                                    status,
                                    DockerDaemonStatus::StartedByElixir
                                ));
                                match self.runtime.stop_and_remove_managed_containers().await {
                                    Ok(removed) => removed_containers = removed,
                                    Err(retry_err) => {
                                        if let Some(retry_kind) =
                                            classify_docker_runtime_failure(&retry_err)
                                        {
                                            self.runtime_health.mark_reboot_recommended(
                                                format!(
                                                    "{} Elixir could not recover Docker automatically; reboot the host and relaunch Elixir.",
                                                    describe_docker_runtime_failure(
                                                        retry_kind,
                                                        &retry_err
                                                    )
                                                ),
                                            );
                                            return Ok(RuntimeResetOutcome {
                                                status: "reboot_recommended".to_string(),
                                                message: format!(
                                                    "Docker is still unhealthy after Elixir retried the managed reset ({}). Reboot the computer, then relaunch Elixir.",
                                                    retry_kind.code()
                                                ),
                                                docker_restarted,
                                                reboot_recommended: true,
                                                removed_containers,
                                                recreated_networks,
                                            });
                                        }
                                        return Err(retry_err);
                                    }
                                }
                            }
                            Err(restart_err) => {
                                self.runtime_health.mark_reboot_recommended(format!(
                                    "{} Elixir could not recover Docker automatically; reboot the host and relaunch Elixir.",
                                    restart_err
                                ));
                                return Ok(RuntimeResetOutcome {
                                    status: "reboot_recommended".to_string(),
                                    message: "Docker is still unhealthy after an Elixir runtime reset attempt. Reboot the computer, then relaunch Elixir."
                                        .to_string(),
                                    docker_restarted: false,
                                    reboot_recommended: true,
                                    removed_containers,
                                    recreated_networks,
                                });
                            }
                        }
                    } else {
                        self.runtime_health.mark_reboot_recommended(format!(
                            "Docker is still unhealthy after Elixir restarted it ({}). Reboot the computer, then relaunch Elixir.",
                            kind.code()
                        ));
                        return Ok(RuntimeResetOutcome {
                            status: "reboot_recommended".to_string(),
                            message: format!(
                                "Docker is still unhealthy after Elixir restarted it ({}). Reboot the computer, then relaunch Elixir.",
                                kind.code()
                            ),
                            docker_restarted,
                            reboot_recommended: true,
                            removed_containers,
                            recreated_networks,
                        });
                    }
                } else {
                    return Err(err);
                }
            }
        }

        for network in MANAGED_RUNTIME_NETWORKS {
            match self.runtime.recreate_managed_network(network).await {
                Ok(_) => recreated_networks.push(network.to_string()),
                Err(err) => {
                    if let Some(kind) = classify_docker_runtime_failure(&err) {
                        self.runtime_health
                            .mark_reboot_recommended(describe_docker_runtime_failure(kind, &err));
                        return Ok(RuntimeResetOutcome {
                            status: "reboot_recommended".to_string(),
                            message: "Elixir could not recreate the managed Docker network because Docker is still unhealthy. Reboot the computer, then relaunch Elixir."
                                .to_string(),
                            docker_restarted,
                            reboot_recommended: true,
                            removed_containers,
                            recreated_networks,
                        });
                    }
                    return Err(err);
                }
            }
        }

        self.runtime_health.clear_all_quarantines();
        self.runtime_health.record_engine_ready(docker_restarted);

        match self.reconcile_once(config).await {
            Ok(()) => Ok(RuntimeResetOutcome {
                status: "recovered".to_string(),
                message: if docker_restarted {
                    format!(
                        "Elixir restarted Docker, removed {} managed container(s), recreated {} managed network(s), and kicked off a fresh reconcile.",
                        removed_containers.len(),
                        recreated_networks.len()
                    )
                } else {
                    format!(
                        "Elixir removed {} managed container(s), recreated {} managed network(s), and kicked off a fresh reconcile.",
                        removed_containers.len(),
                        recreated_networks.len()
                    )
                },
                docker_restarted,
                reboot_recommended: false,
                removed_containers,
                recreated_networks,
            }),
            Err(err) => {
                if let Some(kind) = classify_docker_runtime_failure(&err) {
                    self.runtime_health.mark_reboot_recommended(format!(
                        "{} Elixir could not fully recover Docker automatically; reboot the host and relaunch Elixir.",
                        describe_docker_runtime_failure(kind, &err)
                    ));
                    return Ok(RuntimeResetOutcome {
                        status: "reboot_recommended".to_string(),
                        message: format!(
                            "Elixir reset the managed runtime, but Docker is still unhealthy ({}). Reboot the computer, then relaunch Elixir.",
                            kind.code()
                        ),
                        docker_restarted,
                        reboot_recommended: true,
                        removed_containers,
                        recreated_networks,
                    });
                }

                Ok(RuntimeResetOutcome {
                    status: "partial".to_string(),
                    message: format!(
                        "Elixir reset the managed Docker runtime, but reconcile still reported a follow-up issue: {}",
                        err
                    ),
                    docker_restarted,
                    reboot_recommended: false,
                    removed_containers,
                    recreated_networks,
                })
            }
        }
    }

    pub fn docker_runtime_snapshot(&self) -> DockerRuntimeHealthSnapshot {
        self.runtime_health.snapshot()
    }

    pub fn record_docker_runtime_failure(
        &self,
        code: impl Into<String>,
        reason: impl Into<String>,
    ) {
        self.runtime_health.record_engine_failure(code, reason);
    }

    pub(crate) fn runtime_health(&self) -> &std::sync::Arc<DockerRuntimeSupervisor> {
        &self.runtime_health
    }

    async fn ensure_runtime_ready(&self) -> Result<()> {
        match self
            .runtime
            .ensure_daemon_available(&self.docker_startup)
            .await
        {
            Ok(status) => {
                self.runtime_health.record_engine_ready(matches!(
                    status,
                    crate::runtime::docker::DockerDaemonStatus::StartedByElixir
                ));
                Ok(())
            }
            Err(err) => {
                if let Some(kind) = crate::runtime::docker::classify_docker_runtime_failure(&err) {
                    self.runtime_health.record_engine_failure(
                        kind.code(),
                        describe_docker_runtime_failure(kind, &err),
                    );
                }
                Err(err)
            }
        }
    }

    pub(crate) async fn reconcile_once_with_probe(
        &self,
        config: &ReconcileConfig,
        probe: &dyn ProbeRunner,
        runtime: &dyn crate::runtime::RuntimeManager,
    ) -> Result<()> {
        let reconciler = Reconciler::new_with_runtime_health(
            &self.pool,
            probe,
            runtime,
            &self.drivers,
            self.runtime_paths.clone(),
            self.secrets.as_ref(),
            self.wireguard_gateway_image.clone(),
            self.default_wireguard_config_secret.clone(),
            self.default_downloader_profile,
            config,
            self.runtime_health.clone(),
            self.core_extensions.clone(),
        );
        reconciler.run_once().await
    }

    pub(crate) async fn apply_actions_with_probe(
        &self,
        actions: Vec<ExecutorAction>,
        probe: &dyn ProbeRunner,
        runtime: &dyn crate::runtime::RuntimeManager,
    ) -> Result<()> {
        let executor = Executor::new(
            &self.pool,
            probe,
            &self.drivers,
            runtime,
            self.runtime_paths.clone(),
            self.secrets.as_ref(),
        )
        .with_wireguard_gateway_image(self.wireguard_gateway_image.clone())
        .with_default_wireguard_config_secret(self.default_wireguard_config_secret.clone())
        .with_default_downloader_profile(self.default_downloader_profile);
        for action in actions {
            executor.apply(action).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{
        BindingStatus, ExtensionKind, ExtensionTrustLevel, OrchestratorRunStatus,
        ProviderHealthState, SecretScope, SlotCardinality,
    };
    use crate::extensions::store::{
        ExtensionStore, NewBinding, NewExtension, NewExtensionInstance, NewOrchestratorRun,
        NewProvider, NewSecret,
    };
    use crate::orchestrator::model::ProviderEndpoint;
    use crate::runtime::probe::{ProbeResult, ProbeRunner};
    use crate::secrets::SecretsManager;

    #[derive(Clone, Default)]
    struct MockProbe {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl MockProbe {
        async fn calls(&self) -> Vec<String> {
            self.calls.lock().await.clone()
        }
    }

    #[async_trait]
    impl ProbeRunner for MockProbe {
        async fn probe_dns(&self, name: &str) -> Result<ProbeResult> {
            self.calls.lock().await.push(format!("dns:{name}"));
            Ok(ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_tcp(&self, host: &str, port: u16) -> Result<ProbeResult> {
            self.calls.lock().await.push(format!("tcp:{host}:{port}"));
            Ok(ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_http(&self, url: &str) -> Result<ProbeResult> {
            self.calls.lock().await.push(format!("http:{url}"));
            Ok(ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }
    }

    #[tokio::test]
    async fn apply_actions_uses_probe_order() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        let store = ExtensionStore::new(&database.pool);
        let extension_id = "elixir.test".to_string();

        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.clone(),
                name: "Test Extension".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({}),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id,
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let consumer_provider_id = Uuid::new_v4();
        let target_provider_id = Uuid::new_v4();

        store
            .upsert_provider(&NewProvider {
                provider_id: consumer_provider_id,
                instance_id,
                capability: "consumer.capability".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: None,
                scope_json: None,
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id: target_provider_id,
                instance_id,
                capability: "provider.capability".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: None,
                scope_json: None,
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let binding = NewBinding {
            binding_id: Uuid::new_v4(),
            consumer_provider_id,
            requires_capability: "provider.capability".to_string(),
            requires_slot_id: "default".to_string(),
            target_provider_id,
            binding_params_json: None,
            status: BindingStatus::Pending,
        };

        let consumer_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-consumer".to_string(),
            7878,
            Some("/health".to_string()),
            Some("elixir_net".to_string()),
        )?;
        let provider_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-provider".to_string(),
            9696,
            Some("/health".to_string()),
            Some("elixir_net".to_string()),
        )?;

        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let service = OrchestratorService::new(
            database.pool.clone(),
            "data/extensions".to_string(),
            "extensions/bundled".to_string(),
            "data/library".to_string(),
            vec![
                "elixir.modules.qbittorrent".to_string(),
                "elixir.modules.nzbget".to_string(),
            ],
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DockerStartupConfig {
                auto_start_runtime: false,
                startup_timeout: Duration::from_secs(1),
                startup_poll_interval: Duration::from_millis(100),
            },
            DownloaderPerformanceProfile::Balanced,
            Arc::new(secrets),
        );
        let probe = MockProbe::default();

        service
            .apply_actions_with_probe(
                vec![ExecutorAction::ApplyBinding {
                    binding,
                    consumer_endpoint,
                    provider_endpoint,
                    reverse_probe: true,
                }],
                &probe,
                &crate::runtime::docker::DockerRuntimeManager::new(None),
            )
            .await?;

        let calls = probe.calls().await;
        assert_eq!(
            calls,
            vec![
                "dns:svc-provider".to_string(),
                "tcp:svc-provider:9696".to_string(),
                "http:http://svc-provider:9696/health".to_string(),
                "dns:svc-consumer".to_string(),
                "tcp:svc-consumer:7878".to_string(),
                "http:http://svc-consumer:7878/health".to_string(),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn recover_orphaned_state_clears_lock_and_running_runs() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);

        let run_id = Uuid::new_v4();
        store
            .create_run(&NewOrchestratorRun {
                run_id,
                source: "reconcile".to_string(),
                status: OrchestratorRunStatus::Running,
                phase: Some("reconcile".to_string()),
                plan_json: None,
                error: None,
            })
            .await?;

        let preview_run_id = Uuid::new_v4();
        store
            .create_run(&NewOrchestratorRun {
                run_id: preview_run_id,
                source: "blueprint".to_string(),
                status: OrchestratorRunStatus::Pending,
                phase: Some("planned".to_string()),
                plan_json: Some(json!({
                    "plan_id": preview_run_id,
                    "blueprint_id": "elixir.blueprints.arr_stack",
                    "actions": [],
                    "conflicts": []
                })),
                error: None,
            })
            .await?;
        store
            .create_desired_blueprint(&crate::extensions::store::NewDesiredBlueprint {
                desired_id: preview_run_id,
                blueprint_extension_id: "elixir.blueprints.arr_stack".to_string(),
                blueprint_version: "1.0.0".to_string(),
                params_json: None,
                decisions_json: None,
            })
            .await?;

        let original_owner = Uuid::new_v4().to_string();
        let acquired = store
            .acquire_lock(APPLY_LOCK_NAME, &original_owner, Duration::from_secs(60))
            .await?;
        assert!(acquired, "expected initial apply lock acquisition");

        let service = OrchestratorService::new(
            database.pool.clone(),
            "/tmp/extensions".to_string(),
            "/tmp/extensions/bundled".to_string(),
            "/tmp/media".to_string(),
            vec![],
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DockerStartupConfig {
                auto_start_runtime: false,
                startup_timeout: Duration::from_secs(1),
                startup_poll_interval: Duration::from_millis(100),
            },
            DownloaderPerformanceProfile::Balanced,
            Arc::new(SecretsManager::from_key_bytes([7u8; 32], true)),
        );
        let reconcile_config = ReconcileConfig {
            interval: Duration::from_secs(60),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: crate::orchestrator::reconcile::ReconcileMode::SteadyState,
        };

        service
            .recover_orphaned_state_after_restart(&reconcile_config)
            .await?;

        let run = store
            .get_run(run_id)
            .await?
            .expect("orchestrator run should still exist");
        assert_eq!(run.status, OrchestratorRunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("server restarted"));

        let preview_run = store
            .get_run(preview_run_id)
            .await?
            .expect("preview run should still exist");
        assert_eq!(preview_run.status, OrchestratorRunStatus::Canceled);
        assert_eq!(
            preview_run.error.as_deref(),
            Some("server restarted before plan confirmation")
        );

        assert!(
            store
                .list_desired_blueprints(Some(false))
                .await?
                .into_iter()
                .all(|row| row.desired_id != preview_run_id),
            "stale pending desired blueprints should be removed on startup"
        );

        let lock_rows = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM orchestrator_locks WHERE lock_name = ?",
        )
        .bind(APPLY_LOCK_NAME)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(lock_rows, 0, "stale apply lock should be cleared");

        Ok(())
    }

    #[tokio::test]
    async fn recover_orphaned_state_prunes_stale_bazarr_suffix_instances() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);

        store
            .upsert_extension(&NewExtension {
                extension_id: KNOWN_STALE_BAZARR_EXTENSION_ID.to_string(),
                name: "Bazarr".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({}),
                package_hash: None,
                enabled: true,
            })
            .await?;

        let default_instance = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id: default_instance,
                extension_id: KNOWN_STALE_BAZARR_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({"runtime": {"config_dir": "/tmp/bazarr"}})),
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(default_instance, "1.0.0", None)
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id: Uuid::new_v4(),
                instance_id: default_instance,
                capability: "subtitles.manager".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("bazarr".to_string()),
                scope_json: None,
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let stale_instance = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id: stale_instance,
                extension_id: KNOWN_STALE_BAZARR_EXTENSION_ID.to_string(),
                instance_name: "default-2".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances
             SET created_at = '2026-01-01 00:00:00', updated_at = '2026-01-01 00:00:00'
             WHERE instance_id = ?",
        )
        .bind(stale_instance.to_string())
        .execute(&database.pool)
        .await?;

        let keep_instance = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id: keep_instance,
                extension_id: KNOWN_STALE_BAZARR_EXTENSION_ID.to_string(),
                instance_name: "default-3".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(keep_instance),
                key: "api_key".to_string(),
                value_encrypted: "encrypted".to_string(),
                rotatable: false,
            })
            .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances
             SET created_at = '2026-01-01 00:00:00', updated_at = '2026-01-01 00:00:00'
             WHERE instance_id = ?",
        )
        .bind(keep_instance.to_string())
        .execute(&database.pool)
        .await?;

        let service = OrchestratorService::new(
            database.pool.clone(),
            "/tmp/extensions".to_string(),
            "/tmp/extensions/bundled".to_string(),
            "/tmp/media".to_string(),
            vec![],
            "qmcgaw/gluetun:v3.39.0".to_string(),
            None,
            DockerStartupConfig {
                auto_start_runtime: false,
                startup_timeout: Duration::from_secs(1),
                startup_poll_interval: Duration::from_millis(100),
            },
            DownloaderPerformanceProfile::Balanced,
            Arc::new(SecretsManager::from_key_bytes([7u8; 32], true)),
        );
        let reconcile_config = ReconcileConfig {
            interval: Duration::from_secs(60),
            retry_attempts: 1,
            retry_backoff: Duration::from_secs(1),
            startup_settle: Duration::ZERO,
            lock_ttl: Duration::from_secs(60),
            mode: crate::orchestrator::reconcile::ReconcileMode::SteadyState,
        };

        service
            .recover_orphaned_state_after_restart(&reconcile_config)
            .await?;

        assert!(
            store.get_instance(stale_instance).await?.is_none(),
            "stale bazarr suffix instance should be pruned"
        );
        assert!(
            store.get_instance(keep_instance).await?.is_some(),
            "instances with attached secrets should be preserved"
        );
        assert!(
            store.get_instance(default_instance).await?.is_some(),
            "primary provider-backed instance should be preserved"
        );

        Ok(())
    }
}
