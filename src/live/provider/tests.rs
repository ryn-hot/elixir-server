use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::post};
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    config::DatabaseConfig,
    db::{
        Database,
        models::{
            ExtensionKind, ExtensionTrustLevel, ProviderHealthState, ProviderReadinessPhase,
            SlotCardinality,
        },
    },
    extensions::{
        manifest::{ExtensionManifest, LIVE_CATALOG_PROVIDER_CAPABILITY},
        store::{ExtensionStore, NewExtension, NewExtensionInstance, NewProvider},
    },
    live::{
        config::LiveConfig,
        contract::{
            CatalogPageRequest, ClientDisclosure, FilterValue, MetaRequest, ProviderFailureCode,
            ProviderRequestContext, RefreshFailure, RefreshFailureCategory, RefreshRequest,
            RefreshSessionContext, ResolveRequest, SensitiveString, ServerEgress, StreamProtocol,
        },
        diagnostics::LiveRedactor,
    },
};

use super::*;

pub(crate) struct NativeFixture {
    _temporary: TempDir,
    child: tokio::process::Child,
    port: u16,
}

impl NativeFixture {
    pub(crate) async fn start() -> Result<Self> {
        let temporary = tempfile::tempdir()?;
        let ready_file = temporary.path().join("ready.json");
        let provider = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../fixtures/live/native-provider/src/provider.py");
        let child = Command::new("python3")
            .arg(provider)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg("0")
            .arg("--ready-file")
            .arg(&ready_file)
            .kill_on_drop(true)
            .spawn()
            .context("starting native Live fixture")?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let port = loop {
            if let Ok(bytes) = tokio::fs::read(&ready_file).await {
                let ready: Value = serde_json::from_slice(&bytes)?;
                break ready["port"]
                    .as_u64()
                    .and_then(|port| u16::try_from(port).ok())
                    .context("fixture ready file port")?;
            }
            if Instant::now() >= deadline {
                anyhow::bail!("native Live fixture did not become ready");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        Ok(Self {
            _temporary: temporary,
            child,
            port,
        })
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) async fn stop(mut self) -> Result<()> {
        self.child.kill().await?;
        let _ = self.child.wait().await;
        Ok(())
    }
}

struct StremioAdapterFixture {
    _temporary: TempDir,
    child: tokio::process::Child,
    port: u16,
    approved_addon_authority: String,
}

impl StremioAdapterFixture {
    async fn start() -> Result<Self> {
        let temporary = tempfile::tempdir()?;
        let ready_file = temporary.path().join("ready.json");
        let harness = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../extensions/marketplace/stremio-live-provider/fixtures/integrationHarness.mjs",
        );
        let child = Command::new("node")
            .arg(harness)
            .arg(&ready_file)
            .kill_on_drop(true)
            .spawn()
            .context("starting Stremio Live adapter fixture")?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let (port, approved_addon_authority) = loop {
            if let Ok(bytes) = tokio::fs::read(&ready_file).await {
                let ready: Value = serde_json::from_slice(&bytes)?;
                let port = ready["providerPort"]
                    .as_u64()
                    .and_then(|port| u16::try_from(port).ok())
                    .context("Stremio adapter ready file port")?;
                let authority = ready["approvedAddonAuthority"]
                    .as_str()
                    .context("Stremio adapter approved authority")?
                    .to_string();
                break (port, authority);
            }
            if Instant::now() >= deadline {
                anyhow::bail!("Stremio Live adapter did not become ready");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        Ok(Self {
            _temporary: temporary,
            child,
            port,
            approved_addon_authority,
        })
    }

    async fn stop(mut self) -> Result<()> {
        self.child.kill().await?;
        let _ = self.child.wait().await;
        Ok(())
    }
}

pub(crate) async fn test_database() -> Result<Database> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:s11-live-provider-{}?mode=memory&cache=shared",
            Uuid::new_v4()
        ),
        max_connections: 8,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    Ok(database)
}

pub(crate) async fn seed_provider(
    database: &Database,
    port: u16,
    config: Value,
) -> Result<(Uuid, Uuid)> {
    let manifest_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fixtures/live/native-provider/manifest.yaml");
    seed_provider_manifest(database, port, config, &manifest_path, "s11-native-fixture").await
}

async fn seed_provider_manifest(
    database: &Database,
    port: u16,
    config: Value,
    manifest_path: &std::path::Path,
    package_hash: &str,
) -> Result<(Uuid, Uuid)> {
    let mut manifest_json: Value = serde_yaml::from_str(&std::fs::read_to_string(manifest_path)?)?;
    manifest_json["provides"][0]["endpoint"]["port"] = json!(port);
    manifest_json["networking"]["service_port"]["container_port"] = json!(port);
    let manifest: ExtensionManifest = serde_json::from_value(manifest_json.clone())?;
    manifest.validate()?;

    let store = ExtensionStore::new(&database.pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            kind: ExtensionKind::Module,
            publisher_name: manifest
                .publisher
                .as_ref()
                .map(|publisher| publisher.name.clone()),
            signing_key_id: manifest
                .publisher
                .as_ref()
                .and_then(|publisher| publisher.key_id.clone()),
            trust_level: ExtensionTrustLevel::Community,
            manifest_json,
            package_hash: Some(package_hash.to_string()),
            enabled: true,
        })
        .await?;
    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: manifest.id.clone(),
            instance_name: format!("s11-{}", Uuid::new_v4().simple()),
            config_json: Some(config),
            enabled: true,
        })
        .await?;
    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: LIVE_CATALOG_PROVIDER_CAPABILITY.to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: manifest.provides[0].implementation.clone(),
            scope_json: Some(serde_json::to_value(
                manifest.provides[0]
                    .scope
                    .as_ref()
                    .context("fixture provider scope")?,
            )?),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": port,
                "base_path": "/",
                "network": "s11-fixture"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_provider_readiness(
            provider_id,
            ProviderReadinessPhase::DriverReady,
            Some("live_provider_v1"),
        )
        .await?;
    Ok((instance_id, provider_id))
}

fn context() -> ProviderRequestContext {
    ProviderRequestContext {
        locale: "en-US".to_string(),
        timezone: "America/Chicago".to_string(),
        now: Utc.with_ymd_and_hms(2026, 7, 10, 20, 0, 0).unwrap(),
    }
}

pub(crate) fn build_client(
    database: &Database,
    limits: Option<crate::live::config::LiveProviderLimits>,
) -> Arc<LiveProviderClient> {
    Arc::new(
        LiveProviderClient::new_for_test(
            database.pool.clone(),
            limits.unwrap_or_else(|| LiveConfig::default().providers),
            Arc::new(LiveRedactor::default()),
        )
        .expect("valid provider client"),
    )
}

#[tokio::test]
async fn x10_stremio_adapter_real_provider_client_catalog_meta_resolve_refresh() -> Result<()> {
    let fixture = StremioAdapterFixture::start().await?;
    let database = test_database().await?;
    let manifest_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/marketplace/stremio-live-provider/manifest.yaml");
    let (_, provider_id) = seed_provider_manifest(
        &database,
        fixture.port,
        json!({
            "expectedAddonId": "org.elixir.synthetic.live",
            "approvedAddonAuthority": fixture.approved_addon_authority.clone(),
            "versionPin": "1.0.0",
            "rightsAcknowledged": true,
            "timeoutMs": 1000
        }),
        &manifest_path,
        "x10-stremio-live-fixture",
    )
    .await?;
    let client = build_client(&database, None);
    let provider = client.directory().get(provider_id).await?;
    let cancellation = CancellationToken::new();

    let health = client.health(&provider, &cancellation).await?;
    assert_eq!(
        health.status,
        crate::live::contract::ProviderHealthStatus::Healthy
    );
    let catalogs = client
        .catalogs(&provider, Uuid::new_v4(), &context(), &cancellation)
        .await?;
    assert_eq!(catalogs.catalogs.len(), 1);
    let catalog = &catalogs.catalogs[0];
    assert!(catalog.id.starts_with("stremio."));
    assert!(catalog.filters.iter().any(|filter| filter.id == "genre"));

    let page = client
        .catalog(
            &provider,
            Uuid::new_v4(),
            &context(),
            &CatalogPageRequest {
                catalog_id: catalog.id.clone(),
                cursor: None,
                limit: 10,
                filters: std::collections::BTreeMap::from([(
                    "genre".to_string(),
                    FilterValue::Text("Sports".to_string()),
                )]),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].title, "North City vs South City");
    assert!(page.items[0].id.starts_with("stx1."));

    let item_id = page.items[0].id.clone();
    let metadata = client
        .meta(
            &provider,
            Uuid::new_v4(),
            &context(),
            &MetaRequest {
                item_id: item_id.clone(),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(metadata.item.id, item_id);
    assert_eq!(metadata.streams.len(), 1);
    assert_eq!(metadata.streams[0].id, "auto");

    let resolved = client
        .resolve(
            &provider,
            Uuid::new_v4(),
            &context(),
            &ResolveRequest {
                item_id: item_id.clone(),
                stream_id: "auto".to_string(),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(resolved.descriptor.protocol, StreamProtocol::Hls);
    assert_eq!(
        resolved.descriptor.client_disclosure,
        ClientDisclosure::ServerOnly
    );
    assert_eq!(resolved.descriptor.server_egress, ServerEgress::Preferred);
    assert!(
        resolved
            .descriptor
            .request_headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("authorization"))
    );
    assert_eq!(resolved.descriptor.time_shift.window_seconds, Some(3600));
    assert_eq!(resolved.alternatives.len(), 1);
    assert_eq!(resolved.alternatives[0].protocol, StreamProtocol::Dash);
    assert_eq!(
        resolved.alternatives[0].client_disclosure,
        ClientDisclosure::Public
    );
    assert_eq!(
        resolved.alternatives[0].server_egress,
        ServerEgress::NotRequired
    );
    assert!(resolved.alternatives[0].refresh_handle.is_none());

    let refreshed = client
        .refresh(
            &provider,
            Uuid::new_v4(),
            &context(),
            &RefreshRequest {
                item_id,
                stream_id: "auto".to_string(),
                refresh_handle: resolved
                    .descriptor
                    .refresh_handle
                    .clone()
                    .context("Stremio adapter refresh handle")?,
                failure: RefreshFailure {
                    category: RefreshFailureCategory::ExpiryThreshold,
                    http_status: None,
                },
                session: RefreshSessionContext {
                    started_at: context().now,
                    source_attempt: 1,
                },
            },
            &cancellation,
        )
        .await?;
    assert_eq!(refreshed.descriptor.protocol, StreamProtocol::Hls);

    fixture.stop().await?;
    Ok(())
}

async fn update_config(database: &Database, instance_id: Uuid, config: Value) -> Result<()> {
    ExtensionStore::new(&database.pool)
        .update_instance_config(instance_id, Some(&config))
        .await
}

#[tokio::test]
async fn s11_native_fixture_discovery_happy_path_late_resolve_refresh_and_faults() -> Result<()> {
    let fixture = NativeFixture::start().await?;
    let database = test_database().await?;
    let (instance_id, provider_id) =
        seed_provider(&database, fixture.port, json!({"runtime": {"volumes": []}})).await?;
    let client = build_client(&database, None);
    let cancellation = CancellationToken::new();
    let provider = client.directory().get(provider_id).await?;
    assert_eq!(provider.config(), &json!({}));

    let health = client.health(&provider, &cancellation).await?;
    assert_eq!(
        health.status,
        crate::live::contract::ProviderHealthStatus::Healthy
    );
    let catalogs = client
        .catalogs(&provider, Uuid::new_v4(), &context(), &cancellation)
        .await?;
    assert_eq!(
        catalogs
            .catalogs
            .iter()
            .map(|catalog| catalog.id.as_str())
            .collect::<Vec<_>>(),
        ["events", "channels"]
    );
    let page = client
        .catalog(
            &provider,
            Uuid::new_v4(),
            &context(),
            &CatalogPageRequest {
                catalog_id: "events".to_string(),
                cursor: None,
                limit: 2,
                filters: std::collections::BTreeMap::from([(
                    "category".to_string(),
                    FilterValue::Multiple(vec!["sports".to_string()]),
                )]),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(page.items.len(), 2);
    assert!(page.diagnostics.is_empty());

    let metadata = client
        .meta(
            &provider,
            Uuid::new_v4(),
            &context(),
            &MetaRequest {
                item_id: "event-live".to_string(),
            },
            &cancellation,
        )
        .await?;
    assert!(!metadata.streams.is_empty());
    let stream_id = metadata.streams[0].id.clone();
    let resolved = client
        .resolve(
            &provider,
            Uuid::new_v4(),
            &context(),
            &ResolveRequest {
                item_id: "event-live".to_string(),
                stream_id: stream_id.clone(),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(resolved.descriptor.stream_id, stream_id);
    assert!(!resolved.alternatives.is_empty());
    let refresh_handle = resolved
        .descriptor
        .refresh_handle
        .clone()
        .context("fixture refresh handle")?;
    let refreshed = client
        .refresh(
            &provider,
            Uuid::new_v4(),
            &context(),
            &RefreshRequest {
                item_id: "event-live".to_string(),
                stream_id: stream_id.clone(),
                refresh_handle,
                failure: RefreshFailure {
                    category: RefreshFailureCategory::ExpiryThreshold,
                    http_status: None,
                },
                session: RefreshSessionContext {
                    started_at: context().now,
                    source_attempt: 1,
                },
            },
            &cancellation,
        )
        .await?;
    assert_eq!(refreshed.descriptor.stream_id, stream_id);

    update_config(
        &database,
        instance_id,
        json!({"fixtureFault": "provider_error"}),
    )
    .await?;
    let provider = client.directory().get(provider_id).await?;
    let error = client
        .catalogs(&provider, Uuid::new_v4(), &context(), &cancellation)
        .await
        .expect_err("fixture provider error");
    assert!(matches!(
        error,
        ProviderInvocationError::Provider(ref failure)
            if failure.code == ProviderFailureCode::UpstreamUnavailable
                && failure.retryable
                && failure.retry_after_seconds == Some(2)
    ));

    update_config(
        &database,
        instance_id,
        json!({"fixtureFault": "malformed_json"}),
    )
    .await?;
    let provider = client.directory().get(provider_id).await?;
    assert!(matches!(
        client
            .catalogs(&provider, Uuid::new_v4(), &context(), &cancellation)
            .await,
        Err(ProviderInvocationError::Contract(
            crate::live::contract::ContractErrorCode::MalformedJson
        ))
    ));

    update_config(
        &database,
        instance_id,
        json!({"fixtureFault": "oversized_body"}),
    )
    .await?;
    let provider = client.directory().get(provider_id).await?;
    assert!(matches!(
        client
            .catalogs(&provider, Uuid::new_v4(), &context(), &cancellation)
            .await,
        Err(ProviderInvocationError::ResponseTooLarge)
    ));

    update_config(
        &database,
        instance_id,
        json!({"fixtureFault": "delay", "fixtureDelayMs": 1000}),
    )
    .await?;
    let provider = client.directory().get(provider_id).await?;
    let cancellation = CancellationToken::new();
    let canceller = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        canceller.cancel();
    });
    let started = Instant::now();
    assert!(matches!(
        client
            .catalogs(&provider, Uuid::new_v4(), &context(), &cancellation)
            .await,
        Err(ProviderInvocationError::Cancelled)
    ));
    assert!(started.elapsed() < Duration::from_millis(500));

    update_config(
        &database,
        instance_id,
        json!({"fixtureFault": "delay", "fixtureDelayMs": 1500}),
    )
    .await?;
    let timeout_client = build_client(
        &database,
        Some(crate::live::config::LiveProviderLimits {
            request_timeout_seconds: 1,
            hard_timeout_seconds: 2,
            ..LiveConfig::default().providers
        }),
    );
    let provider = timeout_client.directory().get(provider_id).await?;
    assert!(matches!(
        timeout_client
            .catalogs(
                &provider,
                Uuid::new_v4(),
                &context(),
                &CancellationToken::new()
            )
            .await,
        Err(ProviderInvocationError::RequestTimeout)
    ));

    fixture.stop().await?;
    Ok(())
}

#[tokio::test]
async fn s11_provider_revision_change_and_disablement_discard_in_flight_results() -> Result<()> {
    let fixture = NativeFixture::start().await?;
    let database = test_database().await?;
    let (instance_id, provider_id) = seed_provider(
        &database,
        fixture.port,
        json!({"fixtureFault": "delay", "fixtureDelayMs": 400}),
    )
    .await?;
    let client = build_client(&database, None);
    let provider = client.directory().get(provider_id).await?;
    let worker_client = client.clone();
    let worker_provider = provider.clone();
    let worker = tokio::spawn(async move {
        worker_client
            .catalogs(
                &worker_provider,
                Uuid::new_v4(),
                &context(),
                &CancellationToken::new(),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    update_config(&database, instance_id, json!({"fixtureFault": "none"})).await?;
    assert!(matches!(
        worker.await?,
        Err(ProviderInvocationError::RevisionChanged)
    ));
    assert!(matches!(
        client
            .catalogs(
                &provider,
                Uuid::new_v4(),
                &context(),
                &CancellationToken::new()
            )
            .await,
        Err(ProviderInvocationError::RevisionChanged)
    ));

    let current = client.directory().get(provider_id).await?;
    ExtensionStore::new(&database.pool)
        .update_provider_health(provider_id, ProviderHealthState::Degraded)
        .await?;
    assert!(matches!(
        client
            .catalogs(
                &current,
                Uuid::new_v4(),
                &context(),
                &CancellationToken::new()
            )
            .await,
        Err(ProviderInvocationError::NotReady)
    ));
    fixture.stop().await?;
    Ok(())
}

#[derive(Clone)]
struct ConcurrencyState {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}

struct ActiveCall(Arc<AtomicUsize>);

impl Drop for ActiveCall {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn delayed_catalogs(State(state): State<ConcurrencyState>) -> Json<Value> {
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.maximum.fetch_max(active, Ordering::SeqCst);
    let _active = ActiveCall(state.active.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    Json(json!({
        "catalogs": [],
        "cache": {"maxAgeSeconds": 0, "staleWhileRevalidateSeconds": 0}
    }))
}

#[tokio::test]
async fn s11_provider_and_user_concurrency_are_independently_bounded() -> Result<()> {
    let state = ConcurrencyState {
        active: Arc::new(AtomicUsize::new(0)),
        maximum: Arc::new(AtomicUsize::new(0)),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let shutdown = CancellationToken::new();
    let shutdown_worker = shutdown.clone();
    let server_state = state.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/v1/live/catalogs", post(delayed_catalogs))
                .with_state(server_state),
        )
        .with_graceful_shutdown(shutdown_worker.cancelled_owned())
        .await
    });
    let database = test_database().await?;
    let (_, provider_id) = seed_provider(&database, port, json!({})).await?;
    assert_eq!(
        LiveProviderDirectory::new(database.pool.clone())
            .get(provider_id)
            .await
            .expect_err("production directory rejects loopback runtime endpoints")
            .code(),
        ProviderDirectoryErrorCode::InvalidSnapshot
    );
    let client = build_client(
        &database,
        Some(crate::live::config::LiveProviderLimits {
            request_timeout_seconds: 2,
            hard_timeout_seconds: 3,
            concurrency_per_provider: 2,
            concurrency_per_user: 1,
            ..LiveConfig::default().providers
        }),
    );
    let provider = client.directory().get(provider_id).await?;
    let user = Uuid::new_v4();
    let cancellation = CancellationToken::new();
    let call_context = context();
    let (first, second) = tokio::join!(
        client.catalogs(&provider, user, &call_context, &cancellation),
        client.catalogs(&provider, user, &call_context, &cancellation)
    );
    first?;
    second?;
    assert_eq!(state.maximum.load(Ordering::SeqCst), 1);

    state.maximum.store(0, Ordering::SeqCst);
    let first_user = Uuid::new_v4();
    let second_user = Uuid::new_v4();
    let (first, second) = tokio::join!(
        client.catalogs(&provider, first_user, &call_context, &cancellation),
        client.catalogs(&provider, second_user, &call_context, &cancellation)
    );
    first?;
    second?;
    assert_eq!(state.maximum.load(Ordering::SeqCst), 2);

    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn s11_client_build_limits_fail_closed() {
    let limits = crate::live::config::LiveProviderLimits {
        concurrency_per_provider: 0,
        ..LiveConfig::default().providers
    };
    let pool = sqlx::any::AnyPoolOptions::new()
        .connect_lazy("sqlite::memory:")
        .unwrap();
    assert!(matches!(
        LiveProviderClient::new(pool, limits, Arc::new(LiveRedactor::default())),
        Err(ProviderClientBuildError::InvalidLimits)
    ));
    let secret = SensitiveString::new("canary");
    assert_eq!(format!("{secret:?}"), "<sensitive>");
}
