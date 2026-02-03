use anyhow::Result;
use sqlx::AnyPool;

use crate::drivers::DriverRegistry;
use crate::orchestrator::executor::{Executor, ExecutorAction};
use crate::orchestrator::reconcile::{ReconcileConfig, Reconciler};
use crate::runtime::docker::DockerRuntimeManager;
use crate::runtime::probe::{NetworkProbe, ProbeConfig, ProbeRunner};
use crate::runtime::RuntimePaths;
use crate::secrets::SecretsManager;

#[derive(Clone)]
pub struct OrchestratorService {
    pool: AnyPool,
    storage_root: String,
    runtime_paths: RuntimePaths,
    drivers: std::sync::Arc<DriverRegistry>,
    secrets: std::sync::Arc<SecretsManager>,
}

impl OrchestratorService {
    pub fn new(
        pool: AnyPool,
        storage_root: String,
        media_root: String,
        secrets: std::sync::Arc<SecretsManager>,
    ) -> Self {
        let runtime_paths = RuntimePaths::from_roots(&storage_root, &media_root);
        Self {
            pool,
            storage_root,
            runtime_paths,
            drivers: std::sync::Arc::new(DriverRegistry::with_defaults()),
            secrets,
        }
    }

    pub async fn apply_actions(&self, actions: Vec<ExecutorAction>) -> Result<()> {
        let probe = NetworkProbe::new(ProbeConfig::with_storage_root(&self.storage_root));
        let runtime = DockerRuntimeManager::new(None);
        self.apply_actions_with_probe(actions, &probe, &runtime).await
    }

    pub fn start_reconcile_loop(self: std::sync::Arc<Self>, config: ReconcileConfig) {
        if config.interval.is_zero() {
            return;
        }
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Err(err) = self.reconcile_once(&config).await {
                    tracing::warn!("reconcile loop error: {}", err);
                }
            }
        });
    }

    pub async fn reconcile_once(&self, config: &ReconcileConfig) -> Result<()> {
        let probe = NetworkProbe::new(ProbeConfig::with_storage_root(&self.storage_root));
        let runtime = DockerRuntimeManager::new(None);
        self.reconcile_once_with_probe(config, &probe, &runtime).await
    }

    pub(crate) async fn reconcile_once_with_probe(
        &self,
        config: &ReconcileConfig,
        probe: &dyn ProbeRunner,
        runtime: &dyn crate::runtime::RuntimeManager,
    ) -> Result<()> {
        let reconciler = Reconciler::new(
            &self.pool,
            probe,
            runtime,
            &self.drivers,
            self.runtime_paths.clone(),
            self.secrets.as_ref(),
            config,
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
        );
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
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{
        BindingStatus, ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality,
    };
    use crate::extensions::store::{
        ExtensionStore, NewBinding, NewExtension, NewExtensionInstance, NewProvider,
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
            self.calls
                .lock()
                .await
                .push(format!("tcp:{host}:{port}"));
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
            "data/library".to_string(),
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
}
