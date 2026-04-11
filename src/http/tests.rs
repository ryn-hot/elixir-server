use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use argon2::{
    Argon2,
    password_hash::{PasswordHasher, SaltString},
};
use axum::{
    Json, Router,
    body::{self, Body},
    extract::{Path as AxumPath, State},
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
};
use base64::{Engine as _, engine::general_purpose};
use ed25519_dalek::{Signer, SigningKey};
use rand::{RngCore, rngs::OsRng};
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};
use tower::ServiceExt;
use uuid::Uuid;
use zip::{ZipWriter, write::FileOptions};

use crate::{
    artwork::ArtworkService,
    auth::AuthService,
    config::{
        AuthConfig, ClassifierConfig, DatabaseConfig, LibraryConfig, RunEnvironment, SecretsConfig,
        ServerConfig, Settings, TelemetryConfig,
    },
    db::Database,
    db::models::{
        BindingStatus, ExtensionKind, ExtensionTrustLevel, MediaType, OrchestratorRunStatus,
        ProviderHealthState, SecretScope, SlotCardinality,
    },
    extensions::ExtensionManager,
    extensions::ExternalIds,
    extensions::FileDescriptor,
    extensions::MediaFileCandidate,
    extensions::MediaIdentity,
    extensions::package::compute_sha256,
    extensions::store::{
        ExtensionStore, NewBinding, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
        NewManagedIngestIntent, NewOrchestratorRun, NewProvider, NewSecret,
    },
    http::router,
    library::LinkerService,
    library::normalize_override_key,
    library::run_full_scan,
    metadata::MetadataService,
    orchestrator::model::ProviderEndpoint,
    orchestrator::plan_validation::missing_required_secrets_for_plan,
    orchestrator::planner::{DriverPatchSpec, Plan, PlanAction, Planner, ProviderSpec},
    secrets::SecretsManager,
    state::AppState,
};

fn test_settings_with_db() -> Settings {
    let master_key = general_purpose::STANDARD.encode([0u8; 32]);
    Settings {
        environment: RunEnvironment::Development,
        server: ServerConfig::default(),
        database: DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        },
        library: LibraryConfig::default(),
        extensions: crate::config::ExtensionsConfig::default(),
        auth: AuthConfig::default(),
        secrets: SecretsConfig {
            master_key: Some(master_key),
        },
        telemetry: TelemetryConfig::default(),
        metadata: crate::config::MetadataConfig::default(),
        classifier: ClassifierConfig::default(),
        playback: crate::config::PlaybackConfig::default(),
        network: crate::config::NetworkConfig::default(),
    }
}

fn test_artwork_service(settings: &Settings) -> Result<ArtworkService> {
    ArtworkService::new(
        settings.library.artwork_cache_dir.clone(),
        settings.metadata.request_timeout_seconds,
    )
}

fn control_surface_section<'a>(payload: &'a Value, section_id: &str) -> &'a Value {
    payload
        .get("sections")
        .and_then(Value::as_array)
        .and_then(|sections| {
            sections.iter().find(|section| {
                section.get("id").and_then(Value::as_str) == Some(section_id)
            })
        })
        .unwrap_or_else(|| panic!("missing control-surface section '{section_id}': {payload}"))
}

async fn setup_extension_instance(
    extension_id: &str,
    name: &str,
    runtime_env: Option<Vec<Value>>,
    instance_enabled: bool,
) -> Result<(Router, Uuid)> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    let mut runtime = json!({
        "type": "container",
        "image": "example/test:1"
    });
    if let Some(env) = runtime_env {
        if let Some(obj) = runtime.as_object_mut() {
            obj.insert("env".to_string(), Value::Array(env));
        }
    }

    let manifest = json!({
        "id": extension_id,
        "version": "0.1.0",
        "kind": "module",
        "name": name,
        "provides": [
            {
                "capability": "media.manager.tv",
                "slot": "default",
                "implementation": "sonarr"
            }
        ],
        "runtime": runtime
    });
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: name.to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: manifest,
            package_hash: None,
            enabled: true,
        })
        .await?;

    let instance_id = if instance_enabled {
        let create_resp = app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/extensions/{extension_id}/instances"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?,
            )
            .await?;
        assert_eq!(create_resp.status(), StatusCode::OK);
        let create_body = body::to_bytes(create_resp.into_body(), 1_048_576).await?;
        let create_json: Value = serde_json::from_slice(&create_body)?;
        let instance_id = create_json
            .get("instance_id")
            .and_then(Value::as_str)
            .expect("instance_id");
        Uuid::parse_str(instance_id).expect("valid instance_id")
    } else {
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: false,
            })
            .await?;
        instance_id
    };

    Ok((app, instance_id))
}

struct TestPackage {
    path: PathBuf,
    hash: String,
    signature: String,
    publisher_key_id: String,
    extension_id: String,
    version: String,
}

struct RegistryState {
    registry_json: Value,
    package_bytes: Vec<u8>,
}

async fn build_signed_package(temp_dir: &std::path::Path) -> Result<TestPackage> {
    let extension_id = "elixir.test.signed".to_string();
    let version = "0.1.0".to_string();
    let mut secret = [0u8; 32];
    OsRng.fill_bytes(&mut secret);
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key();
    let publisher_key_id = format!(
        "ed25519:{}",
        general_purpose::STANDARD.encode(public_key.to_bytes())
    );

    let manifest = format!(
        "id: {extension_id}\nversion: {version}\nkind: module\nname: \"Signed Test\"\npublisher:\n  name: \"Test Publisher\"\n  key_id: \"{publisher_key_id}\"\nprovides:\n  - capability: media.manager.tv\n    slot: default\n    implementation: \"sonarr\"\nruntime:\n  type: container\n  image: \"example/test:1\"\n"
    );

    let package_path = temp_dir.join("signed.elx");
    let file = File::create(&package_path)?;
    let mut zip = ZipWriter::new(file);
    let options = FileOptions::<()>::default();
    zip.start_file("manifest.yaml", options)?;
    zip.write_all(manifest.as_bytes())?;
    zip.start_file("README.txt", options)?;
    zip.write_all(b"signed package test")?;
    zip.finish()?;

    let hash = compute_sha256(&package_path).await?;
    let signature = signing_key.sign(hash.as_bytes());
    let signature = general_purpose::STANDARD.encode(signature.to_bytes());

    Ok(TestPackage {
        path: package_path,
        hash,
        signature,
        publisher_key_id,
        extension_id,
        version,
    })
}

async fn start_registry_server(
    build_registry: impl FnOnce(SocketAddr) -> Value + Send + 'static,
    package_bytes: Vec<u8>,
) -> Result<(SocketAddr, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let registry_json = build_registry(addr);
    let state = Arc::new(RegistryState {
        registry_json,
        package_bytes,
    });

    let app = Router::new()
        .route("/registry.json", get(registry_handler))
        .route("/package.elx", get(package_handler))
        .with_state(state);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    Ok((addr, shutdown_tx))
}

async fn registry_handler(State(state): State<Arc<RegistryState>>) -> Json<Value> {
    Json(state.registry_json.clone())
}

async fn package_handler(State(state): State<Arc<RegistryState>>) -> impl IntoResponse {
    (StatusCode::OK, state.package_bytes.clone())
}

fn discover_test_host_ip() -> Result<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let host = socket.local_addr()?.ip().to_string();
    if host == "0.0.0.0"
        || matches!(host.as_str(), "127.0.0.1" | "::1")
    {
        anyhow::bail!("failed to discover a non-localhost test host ip");
    }
    Ok(host)
}

async fn start_mock_sonarr_server() -> Result<(String, SocketAddr, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;

    let app = Router::new()
        .route(
            "/api/v3/system/status",
            get(|| async { Json(json!({ "version": "4.0.0.778" })) }),
        )
        .route(
            "/api/v3/series",
            get(|| async { Json(json!([{ "id": 1 }, { "id": 2 }])) }),
        )
        .route(
            "/api/v3/downloadclient",
            get(|| async { Json(json!([{ "id": 11 }])) }),
        );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let ready_url = format!("http://{}:{}/api/v3/system/status", host, addr.port());
    for _ in 0..20 {
        if let Ok(response) = reqwest::get(&ready_url).await {
            if response.status().is_success() {
                break;
            }
        }
        sleep(Duration::from_millis(25)).await;
    }

    Ok((host, addr, shutdown_tx))
}

async fn start_mock_prowlarr_indexer_server(
    indexer_names: Vec<&'static str>,
) -> Result<(String, SocketAddr, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let names = Arc::new(indexer_names.into_iter().map(str::to_string).collect::<Vec<_>>());

    let app = Router::new()
        .route(
            "/api/v1/indexer",
            get({
                let names = Arc::clone(&names);
                move || {
                    let names = Arc::clone(&names);
                    async move {
                        Json(Value::Array(
                            names.iter()
                                .map(|name| json!({ "name": name }))
                                .collect(),
                        ))
                    }
                }
            }),
        );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let ready_url = format!("http://{}:{}/api/v1/indexer", host, addr.port());
    for _ in 0..20 {
        if let Ok(response) = reqwest::get(&ready_url).await {
            if response.status().is_success() {
                break;
            }
        }
        sleep(Duration::from_millis(25)).await;
    }

    Ok((host, addr, shutdown_tx))
}

async fn start_mock_prowlarr_control_server(
    indexers: Vec<Value>,
    applications: Vec<Value>,
) -> Result<(String, SocketAddr, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let indexers = Arc::new(indexers);
    let applications = Arc::new(applications);

    let app = Router::new()
        .route(
            "/api/v1/system/status",
            get(|| async { Json(json!({ "version": "1.17.2.4511" })) }),
        )
        .route(
            "/api/v1/indexer",
            get({
                let indexers = Arc::clone(&indexers);
                move || {
                    let indexers = Arc::clone(&indexers);
                    async move { Json(Value::Array(indexers.as_ref().clone())) }
                }
            }),
        )
        .route(
            "/api/v1/applications",
            get({
                let applications = Arc::clone(&applications);
                move || {
                    let applications = Arc::clone(&applications);
                    async move { Json(Value::Array(applications.as_ref().clone())) }
                }
            }),
        );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let ready_url = format!("http://{}:{}/api/v1/system/status", host, addr.port());
    for _ in 0..20 {
        if let Ok(response) = reqwest::get(&ready_url).await {
            if response.status().is_success() {
                break;
            }
        }
        sleep(Duration::from_millis(25)).await;
    }

    Ok((host, addr, shutdown_tx))
}

#[derive(Clone, Default)]
struct MockArrControlState {
    commands: Arc<Mutex<Vec<Value>>>,
    deletes: Arc<Mutex<Vec<String>>>,
}

async fn start_mock_sonarr_control_server(
) -> Result<(String, SocketAddr, MockArrControlState, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let state = MockArrControlState::default();

    async fn system_status() -> Json<Value> {
        Json(json!({ "version": "4.0.0.778" }))
    }

    async fn series_list() -> Json<Value> {
        Json(json!([{ "id": 42 }, { "id": 99 }]))
    }

    async fn download_clients() -> Json<Value> {
        Json(json!([{ "id": 11 }]))
    }

    async fn command_handler(
        State(state): State<MockArrControlState>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        state.commands.lock().unwrap().push(payload.clone());
        Json(json!({ "id": 500, "state": "started" }))
    }

    async fn delete_series_handler(
        State(state): State<MockArrControlState>,
        AxumPath(series_id): AxumPath<String>,
    ) -> impl IntoResponse {
        state.deletes.lock().unwrap().push(series_id);
        StatusCode::OK
    }

    let app = Router::new()
        .route("/api/v3/system/status", get(system_status))
        .route("/api/v3/series", get(series_list))
        .route("/api/v3/downloadclient", get(download_clients))
        .route("/api/v3/command", post(command_handler))
        .route("/api/v3/series/:id", delete(delete_series_handler))
        .with_state(state.clone());

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let ready_url = format!("http://{}:{}/api/v3/system/status", host, addr.port());
    for _ in 0..20 {
        if let Ok(response) = reqwest::get(&ready_url).await {
            if response.status().is_success() {
                break;
            }
        }
        sleep(Duration::from_millis(25)).await;
    }

    Ok((host, addr, state, shutdown_tx))
}

#[tokio::test]
async fn health_and_settings_endpoints_work() -> Result<()> {
    let mut settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;

    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        MetadataService::new(crate::config::MetadataConfig::default())?,
        linkers,
        artwork,
        secrets,
    ));

    let health_response = app
        .clone()
        .oneshot(Request::get("/health").body(Body::empty())?)
        .await?;

    assert_eq!(health_response.status(), StatusCode::OK);
    let body = body::to_bytes(health_response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(json.get("status").and_then(Value::as_str), Some("ok"));
    assert_eq!(
        json.get("database")
            .and_then(|d| d.get("status"))
            .and_then(Value::as_str),
        Some("ok")
    );

    let settings_response = app
        .clone()
        .oneshot(Request::get("/api/v1/settings").body(Body::empty())?)
        .await?;

    assert_eq!(settings_response.status(), StatusCode::OK);
    let body = body::to_bytes(settings_response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json.get("environment").and_then(Value::as_str),
        Some("development")
    );
    assert!(json.get("network").is_some());
    assert_eq!(
        json.get("database")
            .and_then(|d| d.get("driver"))
            .and_then(Value::as_str),
        Some("sqlite")
    );

    Ok(())
}

#[tokio::test]
async fn downloader_profile_reports_default_profile_and_telemetry() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.qbittorrent".to_string(),
            name: "qBittorrent".to_string(),
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
            extension_id: "elixir.modules.qbittorrent".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({
                "managed_defaults": {
                    "qbittorrent_performance_profile_version": "v1"
                }
            })),
            enabled: true,
        })
        .await?;
    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "downloader.torrent".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("qbittorrent".to_string()),
            scope_json: None,
            endpoint_json: Some(serde_json::to_value(ProviderEndpoint::new(
                "http".to_string(),
                "elx-qbittorrent".to_string(),
                8080,
                None,
                Some("elixir_net".to_string()),
            )?)?),
            health_state: ProviderHealthState::Unknown,
        })
        .await?;
    store
        .upsert_extension_setting(
            &format!("extensions.downloaders.telemetry.{provider_id}"),
            &json!({
                "lastSuccessfulSampleAt": "2026-04-07T12:00:00Z",
                "lastErrorAt": "2026-04-07T12:05:00Z"
            }),
        )
        .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/downloaders/profile").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json.get("profile").and_then(Value::as_str),
        Some("balanced")
    );
    assert_eq!(
        json.get("defaultProfile").and_then(Value::as_str),
        Some("balanced")
    );
    assert_eq!(json.get("source").and_then(Value::as_str), Some("config"));
    assert_eq!(
        json.get("pendingUpdateCount").and_then(Value::as_u64),
        Some(0)
    );
    let downloaders = json
        .get("downloaders")
        .and_then(Value::as_array)
        .expect("downloaders");
    assert_eq!(downloaders.len(), 1);
    assert_eq!(
        downloaders[0].get("name").and_then(Value::as_str),
        Some("qBittorrent")
    );
    assert_eq!(
        downloaders[0].get("appliedProfile").and_then(Value::as_str),
        Some("balanced")
    );
    assert_eq!(
        downloaders[0].get("syncState").and_then(Value::as_str),
        Some("up_to_date")
    );
    assert!(
        downloaders[0]
            .get("telemetryError")
            .map(Value::is_null)
            .unwrap_or(true)
    );
    assert_eq!(
        downloaders[0]
            .get("lastSuccessfulSampleAt")
            .and_then(Value::as_str),
        Some("2026-04-07T12:00:00Z")
    );
    assert_eq!(
        downloaders[0].get("lastErrorAt").and_then(Value::as_str),
        Some("2026-04-07T12:05:00Z")
    );

    Ok(())
}

#[tokio::test]
async fn downloader_profile_update_persists_override_and_marks_pending_updates() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.qbittorrent".to_string(),
            name: "qBittorrent".to_string(),
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
            extension_id: "elixir.modules.qbittorrent".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({
                "managed_defaults": {
                    "qbittorrent_performance_profile_version": "balanced-v1"
                }
            })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id,
            capability: "downloader.torrent".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("qbittorrent".to_string()),
            scope_json: None,
            endpoint_json: Some(serde_json::to_value(ProviderEndpoint::new(
                "http".to_string(),
                "elx-qbittorrent".to_string(),
                8080,
                None,
                Some("elixir_net".to_string()),
            )?)?),
            health_state: ProviderHealthState::Unknown,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::patch("/api/v1/extensions/downloaders/profile")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "profile": "aggressive" }).to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json.get("profile").and_then(Value::as_str),
        Some("aggressive")
    );
    assert_eq!(json.get("source").and_then(Value::as_str), Some("override"));
    assert!(json.get("updatedAt").is_some());
    assert_eq!(
        json.get("pendingUpdateCount").and_then(Value::as_u64),
        Some(1)
    );
    let stored = store
        .get_extension_setting("downloader_profile")
        .await?
        .expect("stored override");
    assert_eq!(stored.as_str(), Some("aggressive"));

    Ok(())
}

#[tokio::test]
async fn extension_status_summary_surfaces_setup_and_connection_issues() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.secretful".to_string(),
            name: "Secretful".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.secretful",
                "version": "1.0.0",
                "kind": "module",
                "name": "Secretful",
                "provides": [{
                    "capability": "media.manager.tv",
                    "slot": "default",
                    "implementation": "sonarr"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/secretful:latest",
                    "env": [{
                        "name": "API_KEY",
                        "from_secret": "instance:api_key"
                    }]
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    let secretful_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: secretful_instance_id,
            extension_id: "elixir.modules.secretful".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: secretful_instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-secretful",
                "port": 8989,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.healthy".to_string(),
            name: "Healthy".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.healthy",
                "version": "1.0.0",
                "kind": "module",
                "name": "Healthy",
                "provides": [{
                    "capability": "media.manager.movies",
                    "slot": "default",
                    "implementation": "radarr"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/healthy:latest"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    let healthy_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: healthy_instance_id,
            extension_id: "elixir.modules.healthy".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    let healthy_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: healthy_provider_id,
            instance_id: healthy_instance_id,
            capability: "media.manager.movies".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("radarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-healthy",
                "port": 7878,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.broken".to_string(),
            name: "Broken".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.broken",
                "version": "1.0.0",
                "kind": "module",
                "name": "Broken",
                "provides": [{
                    "capability": "subtitles.manager",
                    "slot": "default",
                    "implementation": "bazarr"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/broken:latest"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    let broken_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: broken_instance_id,
            extension_id: "elixir.modules.broken".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    let broken_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: broken_provider_id,
            instance_id: broken_instance_id,
            capability: "subtitles.manager".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("bazarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-broken",
                "port": 6767,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_binding(&NewBinding {
            binding_id: Uuid::new_v4(),
            consumer_provider_id: broken_provider_id,
            requires_capability: "media.manager.movies".to_string(),
            requires_slot_id: "default".to_string(),
            target_provider_id: healthy_provider_id,
            binding_params_json: None,
            status: BindingStatus::Failed,
        })
        .await?;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.connectors.waiting".to_string(),
            name: "Waiting Connector".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Connector,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.connectors.waiting",
                "version": "1.0.0",
                "kind": "connector",
                "name": "Waiting Connector",
                "targets": [{
                    "capability": "indexer.registry",
                    "slot": "default"
                }],
                "actions": [{
                    "type": "driver_patch",
                    "target": {
                        "capability": "indexer.registry",
                        "slot": "default"
                    },
                    "patch": {
                        "op": "noop"
                    }
                }]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;

    assert_eq!(
        json.get("needsAttentionCount").and_then(Value::as_u64),
        Some(3)
    );

    let items = json
        .get("items")
        .and_then(Value::as_array)
        .expect("summary items");
    let item_by_id = |extension_id: &str| -> &Value {
        items
            .iter()
            .find(|item| {
                item.get("extensionId")
                    .and_then(Value::as_str)
                    .map(|value| value == extension_id)
                    .unwrap_or(false)
            })
            .expect("summary item")
    };

    let secretful = item_by_id("elixir.modules.secretful");
    assert_eq!(
        secretful.get("severity").and_then(Value::as_str),
        Some("attention")
    );
    assert_eq!(
        secretful.get("statusCode").and_then(Value::as_str),
        Some("missing_required_secrets")
    );

    let broken = item_by_id("elixir.modules.broken");
    assert_eq!(
        broken.get("severity").and_then(Value::as_str),
        Some("attention")
    );
    assert_eq!(
        broken.get("statusCode").and_then(Value::as_str),
        Some("connection_issue")
    );

    let waiting = item_by_id("elixir.connectors.waiting");
    assert_eq!(
        waiting.get("severity").and_then(Value::as_str),
        Some("attention")
    );
    assert_eq!(
        waiting.get("statusCode").and_then(Value::as_str),
        Some("waiting_for_app")
    );

    let healthy = item_by_id("elixir.modules.healthy");
    assert_eq!(
        healthy.get("severity").and_then(Value::as_str),
        Some("ready")
    );
    assert_eq!(
        healthy.get("statusCode").and_then(Value::as_str),
        Some("ready")
    );

    Ok(())
}

#[tokio::test]
async fn extension_status_summary_includes_optional_addons_for_blueprints() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.prowlarr".to_string(),
            name: "Prowlarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.prowlarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Prowlarr",
                "provides": [{
                    "capability": "indexer.registry",
                    "slot": "default",
                    "implementation": "prowlarr"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/prowlarr:latest"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    let prowlarr_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: prowlarr_instance_id,
            extension_id: "elixir.modules.prowlarr".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: prowlarr_instance_id,
            capability: "indexer.registry".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("prowlarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-prowlarr",
                "port": 9696,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.blueprints.arr_stack".to_string(),
            name: "Arr Stack".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Blueprint,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.blueprints.arr_stack",
                "version": "1.0.0",
                "kind": "blueprint",
                "name": "Arr Stack",
                "wants": [{
                    "capability": "indexer.registry",
                    "slot": "default"
                }],
                "optional_addons": [{
                    "extension_id": "elixir.connectors.prowlarr_nzbgeek",
                    "title": "NZBGeek",
                    "description": "Add your NZBGeek account to Prowlarr.",
                    "target": {
                        "capability": "indexer.registry",
                        "slot": "default"
                    },
                    "required_fields": ["api_key"],
                    "secret_key_prefix": "indexer.nzbgeek"
                }]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let response = app
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .expect("status items");
    let arr_stack = items
        .iter()
        .find(|item| {
            item.get("extensionId").and_then(Value::as_str) == Some("elixir.blueprints.arr_stack")
        })
        .expect("arr stack summary");
    let addons = arr_stack
        .get("optionalAddons")
        .and_then(Value::as_array)
        .expect("optional addons");
    assert_eq!(addons.len(), 1);
    let addon = &addons[0];
    assert_eq!(
        addon.get("extensionId").and_then(Value::as_str),
        Some("elixir.connectors.prowlarr_nzbgeek")
    );
    assert_eq!(addon.get("title").and_then(Value::as_str), Some("NZBGeek"));
    assert_eq!(
        addon.get("action").and_then(Value::as_str),
        Some("activate")
    );
    assert_eq!(
        addon
            .get("requiredFields")
            .and_then(Value::as_array)
            .map(|fields| { fields.iter().filter_map(Value::as_str).collect::<Vec<_>>() }),
        Some(vec!["api_key"])
    );
    assert_eq!(
        addon
            .get("secretKeys")
            .and_then(Value::as_array)
            .map(|fields| { fields.iter().filter_map(Value::as_str).collect::<Vec<_>>() }),
        Some(vec!["indexer.nzbgeek.api_key"])
    );
    let expected_instance_id = prowlarr_instance_id.to_string();
    assert_eq!(
        addon.get("secretScopeInstanceId").and_then(Value::as_str),
        Some(expected_instance_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn extension_status_summary_marks_public_indexer_connector_attention_when_downstream_missing()
-> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets.clone(),
    ));

    let (host, addr, shutdown_tx) = start_mock_prowlarr_indexer_server(vec![]).await?;
    let store = ExtensionStore::new(&db_pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.prowlarr".to_string(),
            name: "Prowlarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.prowlarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Prowlarr",
                "provides": [{
                    "capability": "indexer.registry",
                    "slot": "default"
                }]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.prowlarr".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id,
            capability: "indexer.registry".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("prowlarr".to_string()),
            scope_json: None,
            endpoint_json: Some(serde_json::to_value(ProviderEndpoint::new(
                "http".to_string(),
                host,
                addr.port(),
                Some("/".to_string()),
                None,
            )?)?),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "api_key".to_string(),
            value_encrypted: secrets.encrypt("test-api-key")?,
            rotatable: false,
        })
        .await?;
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.connectors.prowlarr_public_indexers".to_string(),
            name: "Prowlarr Public Indexers".to_string(),
            version: "1.0.2".to_string(),
            kind: ExtensionKind::Connector,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.connectors.prowlarr_public_indexers",
                "version": "1.0.2",
                "kind": "connector",
                "name": "Prowlarr Public Indexers",
                "targets": [{
                    "capability": "indexer.registry",
                    "slot": "default"
                }],
                "actions": [{
                    "type": "driver_patch",
                    "target": {
                        "capability": "indexer.registry",
                        "slot": "default"
                    },
                    "patch": {
                        "op": "register_indexers",
                        "indexers": [
                            {
                                "name": "AnimeTosho",
                                "implementation": "Torznab",
                                "url": "https://feed.animetosho.org",
                                "auth": { "requires_account": false },
                                "tags": ["public"],
                                "enabled": true
                            },
                            {
                                "name": "SubsPlease",
                                "implementation": "SubsPlease",
                                "url": "https://subsplease.org/",
                                "auth": { "requires_account": false },
                                "tags": ["public"],
                                "enabled": true
                            }
                        ]
                    }
                }]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let items = json
        .get("items")
        .and_then(Value::as_array)
        .expect("summary items");
    let connector = items
        .iter()
        .find(|item| {
            item.get("extensionId")
                .and_then(Value::as_str)
                == Some("elixir.connectors.prowlarr_public_indexers")
        })
        .expect("public indexer connector summary");

    assert_eq!(
        connector.get("severity").and_then(Value::as_str),
        Some("attention")
    );
    assert_eq!(
        connector.get("statusCode").and_then(Value::as_str),
        Some("downstream_incomplete")
    );
    assert!(
        connector
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("AnimeTosho"),
        "expected missing downstream description: {connector}"
    );

    let _ = shutdown_tx.send(());
    Ok(())
}

#[tokio::test]
async fn login_returns_access_token() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "test-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let user_id = Uuid::new_v4().to_string();
    let email = "user@example.com";
    let password = "correct horse battery staple";
    let password_hash = hash_password(password);

    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id)
        .bind(email)
        .bind(password_hash)
        .execute(&db_pool)
        .await?;

    let login_body = serde_json::json!({
        "email": email,
        "password": password
    });

    let login_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(login_body.to_string()))?,
        )
        .await?;

    let login_status = login_response.status();
    let login_bytes = body::to_bytes(login_response.into_body(), 1_048_576).await?;
    let login_json: Value = serde_json::from_slice(&login_bytes)?;
    assert_eq!(
        login_status,
        StatusCode::OK,
        "login failed body: {}",
        login_json
    );
    let access_token = login_json
        .get("accessToken")
        .or_else(|| login_json.get("access_token"))
        .and_then(Value::as_str);
    assert!(
        access_token.is_some(),
        "missing access token in login response"
    );

    Ok(())
}

#[tokio::test]
async fn signup_and_password_reset_flow() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "signup-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    // Signup
    let signup_body = serde_json::json!({
        "email": "newuser@example.com",
        "password": "strongpassword"
    });
    let signup_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/signup")
                .header("content-type", "application/json")
                .body(Body::from(signup_body.to_string()))?,
        )
        .await?;
    let signup_status = signup_resp.status();
    let signup_bytes = body::to_bytes(signup_resp.into_body(), 1_048_576).await?;
    let signup_json: Value = serde_json::from_slice(&signup_bytes)?;
    assert_eq!(
        signup_status,
        StatusCode::OK,
        "signup body: {}",
        signup_json
    );
    assert!(signup_json.get("accessToken").is_some() || signup_json.get("access_token").is_some());

    // Duplicate signup should fail
    let dup_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/signup")
                .header("content-type", "application/json")
                .body(Body::from(signup_body.to_string()))?,
        )
        .await?;
    assert_eq!(dup_resp.status(), StatusCode::CONFLICT);

    // Start reset
    let reset_start_body = serde_json::json!({
        "email": "newuser@example.com"
    });
    let reset_start_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/reset/start")
                .header("content-type", "application/json")
                .body(Body::from(reset_start_body.to_string()))?,
        )
        .await?;
    let reset_status = reset_start_resp.status();
    let reset_bytes = body::to_bytes(reset_start_resp.into_body(), 1_048_576).await?;
    let reset_json: Value = serde_json::from_slice(&reset_bytes)?;
    eprintln!("reset_start_status={} body={}", reset_status, reset_json);
    assert_eq!(reset_status, StatusCode::OK);
    let token = reset_json
        .get("token")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert!(!token.is_empty());

    // Complete reset
    let complete_body = serde_json::json!({
        "token": token,
        "new_password": "newstrongpassword"
    });
    let complete_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/reset/complete")
                .header("content-type", "application/json")
                .body(Body::from(complete_body.to_string()))?,
        )
        .await?;
    let complete_status = complete_resp.status();
    let complete_bytes = body::to_bytes(complete_resp.into_body(), 1_048_576).await?;
    let complete_json: Value = serde_json::from_slice(&complete_bytes)?;
    assert_eq!(
        complete_status,
        StatusCode::OK,
        "reset complete body: {}",
        complete_json
    );

    // Login with new password should succeed
    let login_body = serde_json::json!({
        "email": "newuser@example.com",
        "password": "newstrongpassword"
    });
    let login_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(login_body.to_string()))?,
        )
        .await?;
    assert_eq!(login_resp.status(), StatusCode::OK);

    // Invalid reset should fail
    let bad_complete = serde_json::json!({
        "token": "bad-token",
        "new_password": "anotherpassword"
    });
    let bad_resp = app
        .oneshot(
            Request::post("/api/v1/auth/reset/complete")
                .header("content-type", "application/json")
                .body(Body::from(bad_complete.to_string()))?,
        )
        .await?;
    assert_eq!(bad_resp.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test]
async fn settings_and_registry_require_auth_and_show_wan() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "wan-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    // Seed user
    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("wan@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;

    let token = state.auth_service.issue_access_token(user_id)?.token;

    // Unauth access should fail
    let unauth_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/me/servers").body(Body::empty())?)
        .await?;
    assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);

    // Authenticated settings should include network fields
    let settings_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/settings")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(settings_resp.status(), StatusCode::OK);
    let settings_body = body::to_bytes(settings_resp.into_body(), 1_048_576).await?;
    let settings_json: Value = serde_json::from_slice(&settings_body)?;
    assert!(settings_json.get("network").is_some());

    // Register with auth should succeed
    let register_body = serde_json::json!({
        "device_name": "Test Device",
        "lan_addresses": ["127.0.0.1:1234"],
        "wan_direct_endpoint": "203.0.113.1:1234",
        "overlay_endpoint": null
    });
    let register_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/servers/register")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(register_body.to_string()))?,
        )
        .await?;
    assert_eq!(register_resp.status(), StatusCode::OK);

    // Bad payload should 400
    let bad_body = serde_json::json!({
        "device_name": "",
        "lan_addresses": []
    });
    let bad_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/servers/register")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(bad_body.to_string()))?,
        )
        .await?;
    assert_eq!(bad_resp.status(), StatusCode::BAD_REQUEST);

    // Authenticated list should return the registered server
    let list_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/me/servers")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = body::to_bytes(list_resp.into_body(), 1_048_576).await?;
    let list_json: Value = serde_json::from_slice(&list_body)?;
    let servers = list_json.as_array().cloned().unwrap_or_default();
    assert!(!servers.is_empty());
    assert!(servers[0].get("wan_direct_endpoint").is_some());

    // Registry health should be open (no auth)
    let health_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/servers/register/health").body(Body::empty())?)
        .await?;
    assert_eq!(health_resp.status(), StatusCode::OK);
    let health_body = body::to_bytes(health_resp.into_body(), 1_048_576).await?;
    let health_json: Value = serde_json::from_slice(&health_body)?;
    assert_eq!(health_json, Value::String("ok".to_string()));

    // Schema should be accessible without auth
    let schema_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/servers/register/schema").body(Body::empty())?)
        .await?;
    assert_eq!(schema_resp.status(), StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn discovery_requires_auth() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    // Suggest should also require auth
    let suggest_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/discovery/suggest?q=test").body(Body::empty())?)
        .await?;
    assert_eq!(suggest_resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app
        .clone()
        .oneshot(Request::get("/api/v1/discovery/search?q=test").body(Body::empty())?)
        .await?;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let find_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/discovery/find?q=test&type=movie").body(Body::empty())?)
        .await?;
    assert_eq!(find_resp.status(), StatusCode::UNAUTHORIZED);

    let prefs_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/discovery/manager-preferences").body(Body::empty())?)
        .await?;
    assert_eq!(prefs_resp.status(), StatusCode::UNAUTHORIZED);

    let targets_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/find-media/targets?media_type=tv").body(Body::empty())?)
        .await?;
    assert_eq!(targets_resp.status(), StatusCode::UNAUTHORIZED);

    let find_search_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"mediaType":"tv","query":"test"}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(find_search_resp.status(), StatusCode::UNAUTHORIZED);

    let find_add_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/add")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "tv",
                        "item": { "title": "Test Title" }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(find_add_resp.status(), StatusCode::UNAUTHORIZED);

    let find_prefs_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/find-media/preferences").body(Body::empty())?)
        .await?;
    assert_eq!(find_prefs_resp.status(), StatusCode::UNAUTHORIZED);

    let alias_targets_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/find/targets?media_type=tv").body(Body::empty())?)
        .await?;
    assert_eq!(alias_targets_resp.status(), StatusCode::UNAUTHORIZED);

    let alias_search_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find/search")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"mediaType":"tv","query":"test"}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(alias_search_resp.status(), StatusCode::UNAUTHORIZED);

    let alias_add_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find/add")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "tv",
                        "item": { "title": "Test Title" }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(alias_add_resp.status(), StatusCode::UNAUTHORIZED);

    let alias_prefs_resp = app
        .oneshot(Request::get("/api/v1/find/preferences").body(Body::empty())?)
        .await?;
    assert_eq!(alias_prefs_resp.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn discovery_manager_preferences_round_trip() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "discovery-preferences-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());
    let store = ExtensionStore::new(&db_pool);

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-prefs@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr",
                        "scope": { "media_types": ["series", "anime"] }
                    }
                ],
                "runtime": { "type": "container", "image": "example/sonarr:latest" }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.radarr".to_string(),
            name: "Radarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.radarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Radarr",
                "provides": [
                    {
                        "capability": "media.manager.movies",
                        "slot": "default",
                        "implementation": "radarr",
                        "scope": { "media_types": ["movie"] }
                    }
                ],
                "runtime": { "type": "container", "image": "example/radarr:latest" }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let sonarr_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: sonarr_instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "sonarr-api-key" })),
            enabled: true,
        })
        .await?;
    let sonarr_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: sonarr_provider_id,
            instance_id: sonarr_instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({ "media_types": ["series", "anime"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let radarr_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: radarr_instance_id,
            extension_id: "elixir.modules.radarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "radarr-api-key" })),
            enabled: true,
        })
        .await?;
    let radarr_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: radarr_provider_id,
            instance_id: radarr_instance_id,
            capability: "media.manager.movies".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("radarr".to_string()),
            scope_json: Some(json!({ "media_types": ["movie"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let initial_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/discovery/manager-preferences")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(initial_resp.status(), StatusCode::OK);
    let initial_body = body::to_bytes(initial_resp.into_body(), 1_048_576).await?;
    let initial_json: Value = serde_json::from_slice(&initial_body)?;
    assert_eq!(
        initial_json
            .get("movieProviders")
            .and_then(Value::as_array)
            .map(|value| value.len()),
        Some(1)
    );
    assert_eq!(
        initial_json
            .get("seriesProviders")
            .and_then(Value::as_array)
            .map(|value| value.len()),
        Some(1)
    );
    assert_eq!(
        initial_json
            .get("animeProviders")
            .and_then(Value::as_array)
            .map(|value| value.len()),
        Some(1)
    );
    assert_eq!(
        initial_json
            .get("preferences")
            .and_then(|value| value.get("movieProviderId")),
        Some(&Value::Null)
    );

    let update_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/discovery/manager-preferences")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "movieProviderId": radarr_provider_id,
                        "seriesProviderId": sonarr_provider_id,
                        "animeProviderId": sonarr_provider_id
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(update_resp.status(), StatusCode::OK);
    let update_body = body::to_bytes(update_resp.into_body(), 1_048_576).await?;
    let update_json: Value = serde_json::from_slice(&update_body)?;
    let radarr_provider_id_text = radarr_provider_id.to_string();
    let sonarr_provider_id_text = sonarr_provider_id.to_string();
    assert_eq!(
        update_json
            .get("preferences")
            .and_then(|value| value.get("movieProviderId"))
            .and_then(Value::as_str),
        Some(radarr_provider_id_text.as_str())
    );
    assert_eq!(
        update_json
            .get("preferences")
            .and_then(|value| value.get("seriesProviderId"))
            .and_then(Value::as_str),
        Some(sonarr_provider_id_text.as_str())
    );
    assert_eq!(
        update_json
            .get("preferences")
            .and_then(|value| value.get("animeProviderId"))
            .and_then(Value::as_str),
        Some(sonarr_provider_id_text.as_str())
    );

    let persisted_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/discovery/manager-preferences")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(persisted_resp.status(), StatusCode::OK);
    let persisted_body = body::to_bytes(persisted_resp.into_body(), 1_048_576).await?;
    let persisted_json: Value = serde_json::from_slice(&persisted_body)?;
    assert_eq!(
        persisted_json
            .get("preferences")
            .and_then(|value| value.get("movieProviderId"))
            .and_then(Value::as_str),
        Some(radarr_provider_id_text.as_str())
    );

    let new_prefs_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/find-media/preferences")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(new_prefs_resp.status(), StatusCode::OK);
    let new_prefs_body = body::to_bytes(new_prefs_resp.into_body(), 1_048_576).await?;
    let new_prefs_json: Value = serde_json::from_slice(&new_prefs_body)?;
    assert_eq!(
        new_prefs_json
            .get("preferences")
            .and_then(|value| value.get("moviesDefaultManagerProviderId"))
            .and_then(Value::as_str),
        Some(radarr_provider_id_text.as_str())
    );
    assert_eq!(
        new_prefs_json
            .get("preferences")
            .and_then(|value| value.get("tvDefaultManagerProviderId"))
            .and_then(Value::as_str),
        Some(sonarr_provider_id_text.as_str())
    );

    let patch_resp = app
        .oneshot(
            Request::patch("/api/v1/find-media/preferences")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "moviesDefaultManagerProviderId": Value::Null,
                        "tvDefaultManagerProviderId": sonarr_provider_id,
                        "animeDefaultManagerProviderId": sonarr_provider_id
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(patch_resp.status(), StatusCode::OK);
    let patch_body = body::to_bytes(patch_resp.into_body(), 1_048_576).await?;
    let patch_json: Value = serde_json::from_slice(&patch_body)?;
    assert_eq!(
        patch_json
            .get("preferences")
            .and_then(|value| value.get("moviesDefaultManagerProviderId")),
        Some(&Value::Null)
    );

    Ok(())
}

#[tokio::test]
async fn discovery_find_media_filters_providers_by_scope() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "discovery-find-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());
    let store = ExtensionStore::new(&db_pool);

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr",
                        "scope": { "media_types": ["series", "anime"] }
                    },
                    {
                        "capability": "media.search.tv",
                        "slot": "default",
                        "implementation": "sonarr",
                        "scope": { "media_types": ["series"] }
                    },
                    {
                        "capability": "media.search.anime",
                        "slot": "default",
                        "implementation": "sonarr",
                        "scope": { "media_types": ["anime"] }
                    }
                ],
                "runtime": { "type": "container", "image": "example/sonarr:latest" }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.radarr".to_string(),
            name: "Radarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.radarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Radarr",
                "provides": [
                    {
                        "capability": "media.manager.movies",
                        "slot": "default",
                        "implementation": "radarr",
                        "scope": { "media_types": ["movie"] }
                    },
                    {
                        "capability": "media.search.movies",
                        "slot": "default",
                        "implementation": "radarr",
                        "scope": { "media_types": ["movie"] }
                    }
                ],
                "runtime": { "type": "container", "image": "example/radarr:latest" }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let sonarr_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: sonarr_instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "sonarr-api-key" })),
            enabled: true,
        })
        .await?;

    let sonarr_manager_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: sonarr_manager_provider_id,
            instance_id: sonarr_instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({ "media_types": ["series", "anime"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: sonarr_instance_id,
            capability: "media.search.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({ "media_types": ["series"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    let sonarr_anime_search_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: sonarr_anime_search_provider_id,
            instance_id: sonarr_instance_id,
            capability: "media.search.anime".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({ "media_types": ["anime"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let radarr_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: radarr_instance_id,
            extension_id: "elixir.modules.radarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "radarr-api-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: radarr_instance_id,
            capability: "media.manager.movies".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("radarr".to_string()),
            scope_json: Some(json!({ "media_types": ["movie"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: radarr_instance_id,
            capability: "media.search.movies".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("radarr".to_string()),
            scope_json: Some(json!({ "media_types": ["movie"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/discovery/find?q=naruto&type=anime&provider_id={}",
                sonarr_anime_search_provider_id
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let sonarr_manager_provider_id_text = sonarr_manager_provider_id.to_string();
    let sonarr_anime_search_provider_id_text = sonarr_anime_search_provider_id.to_string();

    assert_eq!(
        payload.get("mediaType").and_then(Value::as_str),
        Some("anime")
    );
    let search_providers = payload
        .get("searchProviders")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(search_providers.len(), 1);
    let manager_providers = payload
        .get("managerProviders")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(manager_providers.len(), 1);
    assert_eq!(
        payload
            .get("defaultManagerProviderId")
            .and_then(Value::as_str),
        Some(sonarr_manager_provider_id_text.as_str())
    );

    let provider_ids: Vec<_> = search_providers
        .iter()
        .filter_map(|provider| provider.get("providerId").and_then(Value::as_str))
        .collect();
    assert!(provider_ids.contains(&sonarr_anime_search_provider_id_text.as_str()));

    let provider_errors = payload
        .get("providerErrors")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(provider_errors.len(), 1);
    assert_eq!(
        payload
            .get("results")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(0)
    );

    let targets_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/find-media/targets?media_type=anime")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(targets_resp.status(), StatusCode::OK);
    let targets_body = body::to_bytes(targets_resp.into_body(), 1_048_576).await?;
    let targets_json: Value = serde_json::from_slice(&targets_body)?;
    assert_eq!(
        targets_json.get("mediaType").and_then(Value::as_str),
        Some("anime")
    );
    assert_eq!(
        targets_json
            .get("searchProviders")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(1)
    );
    assert_eq!(
        targets_json
            .get("managerCandidates")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(1)
    );

    let managers_alias_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/find/managers?media_type=anime")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(managers_alias_resp.status(), StatusCode::OK);
    let managers_alias_body = body::to_bytes(managers_alias_resp.into_body(), 1_048_576).await?;
    let managers_alias_json: Value = serde_json::from_slice(&managers_alias_body)?;
    assert_eq!(
        managers_alias_json
            .get("managerCandidates")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(1)
    );

    let search_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/search")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "anime",
                        "query": "naruto"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(search_resp.status(), StatusCode::OK);
    let search_body = body::to_bytes(search_resp.into_body(), 1_048_576).await?;
    let search_json: Value = serde_json::from_slice(&search_body)?;
    assert_eq!(
        search_json.get("mediaType").and_then(Value::as_str),
        Some("anime")
    );
    assert_eq!(
        search_json
            .get("providerErrors")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(0)
    );

    let search_alias_resp = app
        .oneshot(
            Request::post("/api/v1/find/search")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "anime",
                        "query": "naruto"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(search_alias_resp.status(), StatusCode::OK);
    let search_alias_body = body::to_bytes(search_alias_resp.into_body(), 1_048_576).await?;
    let search_alias_json: Value = serde_json::from_slice(&search_alias_body)?;
    assert_eq!(
        search_alias_json
            .get("providerErrors")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(0)
    );

    Ok(())
}

#[tokio::test]
async fn find_media_preferences_clear_stale_manager_provider_ids() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-stale-pref-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());
    let store = ExtensionStore::new(&db_pool);

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media-stale-pref@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr",
                        "scope": { "media_types": ["series", "anime"] }
                    }
                ],
                "runtime": { "type": "container", "image": "example/sonarr:latest" }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "sonarr-api-key" })),
            enabled: true,
        })
        .await?;
    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({ "media_types": ["series", "anime"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let stale_id = Uuid::new_v4();
    store
        .upsert_extension_setting("manager_preference.series", &json!(stale_id.to_string()))
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/find-media/preferences")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload
            .get("preferences")
            .and_then(|value| value.get("tvDefaultManagerProviderId")),
        Some(&Value::Null)
    );

    assert_eq!(
        store
            .get_extension_setting("manager_preference.series")
            .await?,
        None
    );

    let targets_response = app
        .oneshot(
            Request::get("/api/v1/find-media/targets?media_type=series")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(targets_response.status(), StatusCode::OK);
    let targets_body = body::to_bytes(targets_response.into_body(), 1_048_576).await?;
    let targets_payload: Value = serde_json::from_slice(&targets_body)?;
    assert_eq!(
        targets_payload
            .get("preferredManagerProviderId")
            .and_then(Value::as_str),
        None
    );
    assert_eq!(
        targets_payload
            .get("defaultManagerProviderId")
            .and_then(Value::as_str),
        Some(provider_id.to_string().as_str())
    );

    Ok(())
}

#[tokio::test]
async fn find_media_acquisition_lists_active_intents() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-acquisition-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());
    let store = ExtensionStore::new(&db_pool);

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media-acquisition@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr",
                        "scope": { "media_types": ["series", "anime"] }
                    }
                ],
                "runtime": { "type": "container", "image": "example/sonarr:latest" }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "sonarr-api-key" })),
            enabled: true,
        })
        .await?;
    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({ "media_types": ["series", "anime"] })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 7878,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let intent_id = store
        .upsert_managed_ingest_intent(&NewManagedIngestIntent {
            media_type: MediaType::Series,
            title: "Noble House".to_string(),
            normalized_title: "noble house".to_string(),
            year: Some(1988),
            external_ids: Some(ExternalIds {
                tvdb: Some("74493".to_string()),
                ..ExternalIds::default()
            }),
            manager_provider_id: provider_id,
            manager_item_id: None,
            manager_label: Some("default (sonarr)".to_string()),
            source: "find_media".to_string(),
        })
        .await?;

    let response = app
        .oneshot(
            Request::get("/api/v1/find/acquisition?limit=5")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    assert_eq!(
        payload.get("activeCount").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        payload.get("downloadingCount").and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        payload.get("needsAttentionCount").and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        payload
            .get("recentCompletedCount")
            .and_then(Value::as_u64),
        Some(0)
    );

    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .context("acquisition items missing")?;
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("intentId").and_then(Value::as_str),
        Some(intent_id.to_string().as_str())
    );
    assert_eq!(items[0].get("title").and_then(Value::as_str), Some("Noble House"));
    assert_eq!(items[0].get("mediaType").and_then(Value::as_str), Some("tv"));
    assert_eq!(items[0].get("stage").and_then(Value::as_str), Some("requested"));
    assert_eq!(
        items[0].get("stageLabel").and_then(Value::as_str),
        Some("Requested")
    );

    Ok(())
}

#[tokio::test]
async fn find_media_search_without_providers_returns_ok() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-no-providers-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media-no-providers@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let response = app
        .oneshot(
            Request::post("/api/v1/find/search")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "movies",
                        "query": "matrix"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("mediaType").and_then(Value::as_str),
        Some("movies")
    );
    assert!(payload.get("results").and_then(Value::as_array).is_some());
    assert_eq!(
        payload
            .get("providerErrors")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(0)
    );

    Ok(())
}

#[tokio::test]
async fn find_media_add_returns_missing_manager_conflict() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-add-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media-add@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let response = app
        .oneshot(
            Request::post("/api/v1/find-media/add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "tv",
                        "item": {
                            "title": "Example Show",
                            "externalIds": { "tvdbSeries": "12345" }
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("code").and_then(Value::as_str),
        Some("missing_manager")
    );

    Ok(())
}

#[tokio::test]
async fn find_media_add_returns_manager_selection_required_conflict() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-add-manager-selection-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());
    let store = ExtensionStore::new(&db_pool);

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media-add-selection@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let manager_manifest = |id: &str| {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "module",
            "name": id,
            "provides": [
                {
                    "capability": "media.manager.tv",
                    "slot": "default",
                    "implementation": "sonarr",
                    "scope": {
                        "media_types": ["series"],
                        "actions": ["add", "search", "monitor"]
                    }
                }
            ],
            "runtime": { "type": "container", "image": "example/sonarr:latest" }
        })
    };

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.manager_a".to_string(),
            name: "Manager A".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: manager_manifest("elixir.modules.manager_a"),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.manager_b".to_string(),
            name: "Manager B".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: manager_manifest("elixir.modules.manager_b"),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let manager_a_instance = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: manager_a_instance,
            extension_id: "elixir.modules.manager_a".to_string(),
            instance_name: "a".to_string(),
            config_json: Some(json!({ "api_key": "a-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: manager_a_instance,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({
                "media_types": ["series"],
                "actions": ["add", "search", "monitor"]
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let manager_b_instance = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: manager_b_instance,
            extension_id: "elixir.modules.manager_b".to_string(),
            instance_name: "b".to_string(),
            config_json: Some(json!({ "api_key": "b-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: manager_b_instance,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({
                "media_types": ["series"],
                "actions": ["add", "search", "monitor"]
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let response = app
        .oneshot(
            Request::post("/api/v1/find-media/add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "tv",
                        "item": { "title": "Example Show" }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("code").and_then(Value::as_str),
        Some("manager_selection_required")
    );

    Ok(())
}

#[tokio::test]
async fn find_media_add_returns_missing_required_secrets_conflict() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-add-missing-secrets-secret".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());
    let store = ExtensionStore::new(&db_pool);

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media-add-missing-secrets@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.requires_api".to_string(),
            name: "Requires API".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.requires_api",
                "version": "1.0.0",
                "kind": "module",
                "name": "Requires API",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr",
                        "scope": {
                            "media_types": ["series"],
                            "actions": ["add"],
                            "requires_account": true,
                            "required_fields": ["api_key"]
                        }
                    }
                ],
                "runtime": { "type": "container", "image": "example/sonarr:latest" }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.requires_api".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({
                "media_types": ["series"],
                "actions": ["add"],
                "requires_account": true,
                "required_fields": ["api_key"]
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let response = app
        .oneshot(
            Request::post("/api/v1/find-media/add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "tv",
                        "item": { "title": "Example Show" }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("code").and_then(Value::as_str),
        Some("missing_required_secrets")
    );

    Ok(())
}

#[tokio::test]
async fn find_media_targets_manager_resolution_precedence() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-targets-precedence".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());
    let store = ExtensionStore::new(&db_pool);

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("find-media-targets@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let manager_manifest = |id: &str| {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "module",
            "name": id,
            "provides": [
                {
                    "capability": "media.manager.tv",
                    "slot": "default",
                    "implementation": "sonarr",
                    "scope": {
                        "media_types": ["series"],
                        "actions": ["add", "search", "monitor"]
                    }
                }
            ],
            "runtime": { "type": "container", "image": "example/sonarr:latest" }
        })
    };

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.community".to_string(),
            name: "Community".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: manager_manifest("elixir.modules.community"),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.verified".to_string(),
            name: "Verified".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: manager_manifest("elixir.modules.verified"),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let community_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: community_instance_id,
            extension_id: "elixir.modules.community".to_string(),
            instance_name: "community".to_string(),
            config_json: Some(json!({ "api_key": "community-api-key" })),
            enabled: true,
        })
        .await?;
    let community_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: community_provider_id,
            instance_id: community_instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({
                "media_types": ["series"],
                "actions": ["add", "search", "monitor"]
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let verified_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: verified_instance_id,
            extension_id: "elixir.modules.verified".to_string(),
            instance_name: "verified".to_string(),
            config_json: Some(json!({ "api_key": "verified-api-key" })),
            enabled: true,
        })
        .await?;
    let verified_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: verified_provider_id,
            instance_id: verified_instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: Some(json!({
                "media_types": ["series"],
                "actions": ["add", "search", "monitor"]
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "127.0.0.1",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let initial = app
        .clone()
        .oneshot(
            Request::get("/api/v1/find-media/targets?media_type=tv")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(initial.status(), StatusCode::OK);
    let initial_body = body::to_bytes(initial.into_body(), 1_048_576).await?;
    let initial_json: Value = serde_json::from_slice(&initial_body)?;
    let verified_provider_id_text = verified_provider_id.to_string();
    assert_eq!(
        initial_json
            .get("defaultManagerProviderId")
            .and_then(Value::as_str),
        Some(verified_provider_id_text.as_str())
    );

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.blueprints.pref".to_string(),
            name: "Blueprint Preference".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Blueprint,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.blueprints.pref",
                "version": "1.0.0",
                "kind": "blueprint",
                "name": "pref",
                "preferences": {
                    "providers": {
                        "media.manager.tv/default": {
                            "prefer": ["elixir.modules.community"]
                        }
                    }
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
            blueprint_extension_id: "elixir.blueprints.pref".to_string(),
            blueprint_version: "1.0.0".to_string(),
            params_json: None,
            decisions_json: None,
        })
        .await?;
    store.mark_desired_applied(desired_id, true).await?;

    let blueprint = app
        .clone()
        .oneshot(
            Request::get("/api/v1/find-media/targets?media_type=tv")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(blueprint.status(), StatusCode::OK);
    let blueprint_body = body::to_bytes(blueprint.into_body(), 1_048_576).await?;
    let blueprint_json: Value = serde_json::from_slice(&blueprint_body)?;
    let community_provider_id_text = community_provider_id.to_string();
    assert_eq!(
        blueprint_json
            .get("defaultManagerProviderId")
            .and_then(Value::as_str),
        Some(community_provider_id_text.as_str())
    );

    store
        .upsert_extension_setting(
            "manager_preference.series",
            &json!(verified_provider_id.to_string()),
        )
        .await?;
    let preferred = app
        .oneshot(
            Request::get("/api/v1/find-media/targets?media_type=tv")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(preferred.status(), StatusCode::OK);
    let preferred_body = body::to_bytes(preferred.into_body(), 1_048_576).await?;
    let preferred_json: Value = serde_json::from_slice(&preferred_body)?;
    assert_eq!(
        preferred_json
            .get("defaultManagerProviderId")
            .and_then(Value::as_str),
        Some(verified_provider_id_text.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn playback_profile_requires_auth() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "profile-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    // Seed user
    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("profile@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;

    let token = state.auth_service.issue_access_token(user_id)?.token;

    // Unauth should fail
    let unauth = app
        .clone()
        .oneshot(Request::get("/api/v1/profile/playback").body(Body::empty())?)
        .await?;
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    // Auth should return profile
    let auth_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/profile/playback")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(auth_resp.status(), StatusCode::OK);
    let body = body::to_bytes(auth_resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert!(json.get("profile").is_some());

    Ok(())
}

fn hash_password(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes(), &salt)
        .expect("hashing to succeed")
        .to_string()
}

#[tokio::test]
async fn ingest_scan_endpoint_ingests_candidates() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let extensions = ExtensionManager::new();
    let metadata = crate::metadata::MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        extensions,
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    // Seed media via run_full_scan directly.
    let candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: crate::db::models::MediaType::Movie,
            external_ids: ExternalIds::default(),
            title: "Scan Test".to_string(),
            year: Some(2024),
            season: None,
            episode: None,
        },
        files: vec![FileDescriptor {
            path: "/tmp/scan_test.mkv".to_string(),
            size_bytes: Some(2048),
            hash: None,
            container: Some("mkv".to_string()),
            video_codec: None,
            audio_codec: None,
        }],
        extension_metadata: Default::default(),
        source_config_id: None,
    };
    run_full_scan(&state.db_pool, vec![candidate], false).await?;

    let resp = app
        .clone()
        .oneshot(Request::get("/api/v1/library/items").body(Body::empty())?)
        .await?;
    let status = resp.status();
    let body = body::to_bytes(resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "body: {}", json);
    assert!(json.as_array().is_some());
    let items = json.as_array().unwrap();
    let first_id = items
        .iter()
        .find(|item| item.get("title") == Some(&Value::String("Scan Test".to_string())))
        .and_then(|i| i.get("id").and_then(Value::as_str))
        .expect("id present")
        .to_string();

    let detail_resp = app
        .clone()
        .oneshot(Request::get(format!("/api/v1/library/items/{first_id}")).body(Body::empty())?)
        .await?;
    assert_eq!(detail_resp.status(), StatusCode::OK);
    let detail_bytes = body::to_bytes(detail_resp.into_body(), 1_048_576).await?;
    let detail_json: Value = serde_json::from_slice(&detail_bytes)?;
    assert_eq!(
        detail_json.get("title").and_then(Value::as_str),
        Some("Scan Test")
    );

    Ok(())
}

#[tokio::test]
async fn review_queue_apply_updates_movie_and_override() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("review@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let dir = tempdir()?;
    let media_path = dir.path().join("Review.Movie.2024.mkv");
    std::fs::write(&media_path, b"")?;

    let candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: crate::db::models::MediaType::Movie,
            external_ids: ExternalIds::default(),
            title: "Review Movie".to_string(),
            year: Some(2024),
            season: None,
            episode: None,
        },
        files: vec![FileDescriptor {
            path: media_path.to_string_lossy().to_string(),
            size_bytes: Some(1234),
            hash: None,
            container: Some("mkv".to_string()),
            video_codec: None,
            audio_codec: None,
        }],
        extension_metadata: Default::default(),
        source_config_id: None,
    };
    run_full_scan(&state.db_pool, vec![candidate], false).await?;

    let media_file_id: String =
        sqlx::query_scalar("SELECT id FROM media_files WHERE path = ? LIMIT 1")
            .bind(media_path.to_string_lossy().to_string())
            .fetch_one(&state.db_pool)
            .await?;

    let review_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO review_queue (id, media_file_id, status, confidence, hint_json, candidates_json) VALUES (?, ?, 'pending', ?, ?, ?)",
    )
    .bind(&review_id)
    .bind(&media_file_id)
    .bind(0.4_f32)
    .bind(r#"{"title":"Review Movie"}"#)
    .bind(r#"{"candidates":[]}"#)
    .execute(&state.db_pool)
    .await?;

    let apply_body = serde_json::json!({
        "library_type": "movie",
        "external_ids": {
            "imdb": "tt1234567",
            "tmdb": 9999
        }
    });
    let response = app
        .oneshot(
            Request::post(format!("/api/v1/library/review/queue/{review_id}/apply"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(apply_body.to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let imdb_id: Option<String> = sqlx::query_scalar("SELECT external_imdb FROM movies LIMIT 1")
        .fetch_one(&state.db_pool)
        .await?;
    assert_eq!(imdb_id.as_deref(), Some("tt1234567"));

    let status: String = sqlx::query_scalar("SELECT status FROM review_queue WHERE id = ? LIMIT 1")
        .bind(&review_id)
        .fetch_one(&state.db_pool)
        .await?;
    assert_eq!(status, "applied");

    let override_key: String = sqlx::query_scalar(
        "SELECT normalized_key FROM classifier_overrides WHERE library_type = 'movie' LIMIT 1",
    )
    .fetch_one(&state.db_pool)
    .await?;
    let expected_key = normalize_override_key("Review.Movie.2024").expect("normalized key");
    assert_eq!(override_key, expected_key);

    Ok(())
}

#[tokio::test]
async fn play_endpoint_returns_stream_url() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "play-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let extensions = ExtensionManager::new();
    let metadata = crate::metadata::MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        extensions,
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    // Create a real file on disk to serve.
    let dir = tempdir()?;
    let file_path = dir.path().join("Play.Movie.2024.mkv");
    tokio::fs::write(&file_path, b"0123456789").await?;

    let candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: crate::db::models::MediaType::Movie,
            external_ids: ExternalIds::default(),
            title: "Play Movie".to_string(),
            year: Some(2024),
            season: None,
            episode: None,
        },
        files: vec![FileDescriptor {
            path: file_path.to_string_lossy().to_string(),
            size_bytes: Some(10),
            hash: None,
            container: Some("mkv".to_string()),
            video_codec: None,
            audio_codec: None,
        }],
        extension_metadata: Default::default(),
        source_config_id: None,
    };
    run_full_scan(&state.db_pool, vec![candidate], false).await?;

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("play-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;

    let (item_id,): (String,) = sqlx::query_as("SELECT id FROM movies LIMIT 1")
        .fetch_one(&state.db_pool)
        .await?;

    let token = state.auth_service.issue_access_token(user_id)?.token;

    let play_body = serde_json::json!({
        "media_item_id": item_id,
        "preferred_file_id": null,
        "network_type": "lan",
        "client_capabilities": {}
    });

    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;

    let status = resp.status();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(status, StatusCode::OK, "body: {}", json);
    assert!(json.get("streamUrl").is_some() || json.get("stream_url").is_some());
    assert_eq!(
        json.get("mode").and_then(Value::as_str),
        Some("direct_play")
    );
    assert_eq!(json.get("state").and_then(Value::as_str), Some("active"));
    assert_eq!(
        json.get("logicalPositionSeconds")
            .or_else(|| json.get("logical_position_seconds"))
            .and_then(Value::as_f64)
            .unwrap_or(-1.0) as i32,
        0
    );

    Ok(())
}

#[tokio::test]
async fn direct_stream_supports_range() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "stream-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let extensions = ExtensionManager::new();
    let metadata = crate::metadata::MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        extensions,
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    let dir = tempdir()?;
    let file_path = dir.path().join("RangeTest.2024.mkv");
    let file_bytes = b"abcdefghij";
    tokio::fs::write(&file_path, file_bytes).await?;

    let candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: crate::db::models::MediaType::Movie,
            external_ids: ExternalIds::default(),
            title: "Range Test".to_string(),
            year: Some(2024),
            season: None,
            episode: None,
        },
        files: vec![FileDescriptor {
            path: file_path.to_string_lossy().to_string(),
            size_bytes: Some(file_bytes.len() as i64),
            hash: None,
            container: Some("mkv".to_string()),
            video_codec: None,
            audio_codec: None,
        }],
        extension_metadata: Default::default(),
        source_config_id: None,
    };
    run_full_scan(&state.db_pool, vec![candidate], false).await?;

    // Create a user and session via /play.
    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("range-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;

    let token = state.auth_service.issue_access_token(user_id)?.token;

    let media_item_id: String = sqlx::query_scalar("SELECT id FROM movies LIMIT 1")
        .fetch_one(&state.db_pool)
        .await?;

    let play_body = serde_json::json!({
        "media_item_id": media_item_id,
        "preferred_file_id": null,
        "network_type": "lan",
        "client_capabilities": {}
    });

    let play_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    assert_eq!(play_resp.status(), StatusCode::OK);
    let play_bytes = body::to_bytes(play_resp.into_body(), 1_048_576).await?;
    let play_json: Value = serde_json::from_slice(&play_bytes)?;
    let stream_url = play_json
        .get("streamUrl")
        .or_else(|| play_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url");

    let resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=0-3")
                .body(Body::empty())?,
        )
        .await?;

    let status = resp.status();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "body: {}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(&bytes[..], b"abcd");

    Ok(())
}

#[tokio::test]
#[ignore = "requires ELIXIR_TEST_MEDIA_PATH or default sample file present"]
async fn hls_integration_transcodes_when_media_present() -> Result<()> {
    use std::path::Path;

    let default_path = "/Users/ryanhotard/downloads/Solo.Leveling.S02E02.I.Suppose.You.Arent.Aware.1080p.CR.WEB-DL.AAC2.0.H.264.DUAL-VARYG.mkv";
    let media_path =
        std::env::var("ELIXIR_TEST_MEDIA_PATH").unwrap_or_else(|_| default_path.to_string());

    if !Path::new(&media_path).exists() {
        // Skip silently when file is not present.
        return Ok(());
    }

    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "hls-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let extensions = ExtensionManager::new();
    let metadata = crate::metadata::MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        extensions,
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("hls-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;

    let candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: crate::db::models::MediaType::Movie,
            external_ids: ExternalIds::default(),
            title: "HLS Sample".to_string(),
            year: Some(2024),
            season: None,
            episode: None,
        },
        files: vec![FileDescriptor {
            path: media_path.clone(),
            size_bytes: None,
            hash: None,
            container: Path::new(&media_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_string()),
            video_codec: None,
            audio_codec: None,
        }],
        extension_metadata: Default::default(),
        source_config_id: None,
    };
    run_full_scan(&state.db_pool, vec![candidate], false).await?;

    let token = state.auth_service.issue_access_token(user_id)?.token;
    let media_item_id: String = sqlx::query_scalar("SELECT id FROM movies LIMIT 1")
        .fetch_one(&state.db_pool)
        .await?;

    // Use a capability profile that forces transcode (no mkv support).
    let play_body = serde_json::json!({
        "media_item_id": media_item_id,
        "preferred_file_id": null,
        "network_type": "wan",
        "client_capabilities": {
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "max_bitrate_bps": 5_000_000
        }
    });

    let play_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    assert_eq!(play_resp.status(), StatusCode::OK);
    let play_bytes = body::to_bytes(play_resp.into_body(), 1_048_576).await?;
    let play_json: Value = serde_json::from_slice(&play_bytes)?;
    assert_eq!(
        play_json.get("mode").and_then(Value::as_str),
        Some("transcode"),
        "should choose transcode for mkv"
    );
    let stream_url = play_json
        .get("streamUrl")
        .or_else(|| play_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url");

    // Fetch playlist
    let playlist_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    let playlist_status = playlist_resp.status();
    let playlist_body = body::to_bytes(playlist_resp.into_body(), 5_000_000).await?;
    assert_eq!(
        playlist_status,
        StatusCode::OK,
        "playlist failed: {}",
        String::from_utf8_lossy(&playlist_body)
    );
    let playlist_str = String::from_utf8_lossy(&playlist_body);
    let first_seg = playlist_str
        .lines()
        .find(|line| line.starts_with("seg_"))
        .unwrap_or("seg_00000.ts?session=");

    let base = stream_url.split('/').take(3).collect::<Vec<_>>().join("/");
    let segment_url = format!("{base}/{}", first_seg);

    let seg_resp = app
        .clone()
        .oneshot(
            Request::get(&segment_url)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    let seg_status = seg_resp.status();
    let seg_bytes = body::to_bytes(seg_resp.into_body(), 20_000_000).await?;
    assert_eq!(
        seg_status,
        StatusCode::OK,
        "segment failed: {}",
        String::from_utf8_lossy(&seg_bytes)
    );
    assert!(!seg_bytes.is_empty(), "segment should return data");

    Ok(())
}

#[tokio::test]
async fn extensions_install_signed_package() -> Result<()> {
    let temp = tempdir()?;
    let package = build_signed_package(temp.path()).await?;
    let package_bytes = tokio::fs::read(&package.path).await?;

    let extension_id = package.extension_id.clone();
    let version = package.version.clone();
    let hash = package.hash.clone();
    let signature = package.signature.clone();
    let publisher_key_id = package.publisher_key_id.clone();
    let (addr, shutdown_tx) = start_registry_server(
        move |addr| {
            let download_url = format!("http://{addr}/package.elx");
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": extension_id,
                        "version": version,
                        "download_url": download_url,
                        "sha256": hash,
                        "signature": signature,
                        "publisher_key_id": publisher_key_id
                    }
                ]
            })
        },
        package_bytes,
    )
    .await?;

    let download_url = format!("http://{addr}/package.elx");
    let registry_url = format!("http://{addr}/registry.json");

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![registry_url];
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;

    let storage_root = settings.extensions.storage_root.clone();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "downloadUrl": download_url
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    let install_status = install_resp.status();
    let install_body = body::to_bytes(install_resp.into_body(), 1_048_576).await?;
    let install_json: Value = serde_json::from_slice(&install_body)?;
    assert_eq!(
        install_status,
        StatusCode::OK,
        "install failed body: {}",
        install_json
    );

    let unpacked_dir = PathBuf::from(storage_root)
        .join("unpacked")
        .join(&package.extension_id)
        .join(&package.version);
    let manifest_path = unpacked_dir.join("manifest.yaml");
    assert!(
        tokio::fs::metadata(&manifest_path).await.is_ok(),
        "manifest not unpacked at {}",
        manifest_path.display()
    );

    let _ = shutdown_tx.send(());
    Ok(())
}

#[tokio::test]
async fn extensions_rejects_downgrade_install() -> Result<()> {
    let temp = tempdir()?;
    let package = build_signed_package(temp.path()).await?;
    let package_bytes = tokio::fs::read(&package.path).await?;

    let extension_id = package.extension_id.clone();
    let version = package.version.clone();
    let hash = package.hash.clone();
    let signature = package.signature.clone();
    let publisher_key_id = package.publisher_key_id.clone();
    let (addr, shutdown_tx) = start_registry_server(
        move |addr| {
            let download_url = format!("http://{addr}/package.elx");
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": extension_id,
                        "version": version,
                        "download_url": download_url,
                        "sha256": hash,
                        "signature": signature,
                        "publisher_key_id": publisher_key_id
                    }
                ]
            })
        },
        package_bytes,
    )
    .await?;

    let download_url = format!("http://{addr}/package.elx");
    let registry_url = format!("http://{addr}/registry.json");

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![registry_url];
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let store = ExtensionStore::new(&database.pool);
    let manifest_json = json!({
        "id": package.extension_id,
        "version": "1.0.0",
        "kind": "module",
        "name": "Signed Test",
        "provides": [
            {
                "capability": "media.manager.tv",
                "slot": "default",
                "implementation": "sonarr"
            }
        ],
        "runtime": {
            "type": "container",
            "image": "example/test:1"
        }
    });
    store
        .upsert_extension(&NewExtension {
            extension_id: package.extension_id.clone(),
            name: "Signed Test".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json,
            package_hash: None,
            enabled: true,
        })
        .await?;

    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "downloadUrl": download_url
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    assert_eq!(install_resp.status(), StatusCode::BAD_REQUEST);
    let body = body::to_bytes(install_resp.into_body(), 1_048_576).await?;
    let error_json: Value = serde_json::from_slice(&body)?;
    let message = error_json
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains("downgrade"),
        "expected downgrade rejection, got: {}",
        message
    );

    let _ = shutdown_tx.send(());
    Ok(())
}

#[tokio::test]
async fn extensions_allows_reinstall_same_version_hash() -> Result<()> {
    let temp = tempdir()?;
    let package = build_signed_package(temp.path()).await?;
    let package_bytes = tokio::fs::read(&package.path).await?;

    let extension_id = package.extension_id.clone();
    let version = package.version.clone();
    let hash = package.hash.clone();
    let signature = package.signature.clone();
    let publisher_key_id = package.publisher_key_id.clone();
    let (addr, shutdown_tx) = start_registry_server(
        move |addr| {
            let download_url = format!("http://{addr}/package.elx");
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": extension_id,
                        "version": version,
                        "download_url": download_url,
                        "sha256": hash,
                        "signature": signature,
                        "publisher_key_id": publisher_key_id
                    }
                ]
            })
        },
        package_bytes,
    )
    .await?;

    let download_url = format!("http://{addr}/package.elx");
    let registry_url = format!("http://{addr}/registry.json");

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![registry_url];
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let store = ExtensionStore::new(&database.pool);
    let manifest_json = json!({
        "id": package.extension_id,
        "version": package.version,
        "kind": "module",
        "name": "Signed Test",
        "provides": [
            {
                "capability": "media.manager.tv",
                "slot": "default",
                "implementation": "sonarr"
            }
        ],
        "runtime": {
            "type": "container",
            "image": "example/test:1"
        }
    });
    store
        .upsert_extension(&NewExtension {
            extension_id: package.extension_id.clone(),
            name: "Signed Test".to_string(),
            version: package.version.clone(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json,
            package_hash: Some(package.hash.clone()),
            enabled: true,
        })
        .await?;

    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "downloadUrl": download_url
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    let install_status = install_resp.status();
    let install_body = body::to_bytes(install_resp.into_body(), 1_048_576).await?;
    let install_json: Value = serde_json::from_slice(&install_body)?;
    assert_eq!(
        install_status,
        StatusCode::OK,
        "install failed body: {}",
        install_json
    );
    assert_eq!(
        install_json.get("extension_id").and_then(Value::as_str),
        Some(package.extension_id.as_str())
    );
    assert_eq!(
        install_json.get("version").and_then(Value::as_str),
        Some(package.version.as_str())
    );

    let _ = shutdown_tx.send(());
    Ok(())
}

#[tokio::test]
async fn extensions_rejects_same_version_different_hash() -> Result<()> {
    let temp = tempdir()?;
    let package = build_signed_package(temp.path()).await?;
    let package_bytes = tokio::fs::read(&package.path).await?;

    let extension_id = package.extension_id.clone();
    let version = package.version.clone();
    let hash = package.hash.clone();
    let signature = package.signature.clone();
    let publisher_key_id = package.publisher_key_id.clone();
    let (addr, shutdown_tx) = start_registry_server(
        move |addr| {
            let download_url = format!("http://{addr}/package.elx");
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": extension_id,
                        "version": version,
                        "download_url": download_url,
                        "sha256": hash,
                        "signature": signature,
                        "publisher_key_id": publisher_key_id
                    }
                ]
            })
        },
        package_bytes,
    )
    .await?;

    let download_url = format!("http://{addr}/package.elx");
    let registry_url = format!("http://{addr}/registry.json");

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![registry_url];
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let store = ExtensionStore::new(&database.pool);
    let manifest_json = json!({
        "id": package.extension_id,
        "version": package.version,
        "kind": "module",
        "name": "Signed Test",
        "provides": [
            {
                "capability": "media.manager.tv",
                "slot": "default",
                "implementation": "sonarr"
            }
        ],
        "runtime": {
            "type": "container",
            "image": "example/test:1"
        }
    });
    store
        .upsert_extension(&NewExtension {
            extension_id: package.extension_id.clone(),
            name: "Signed Test".to_string(),
            version: package.version.clone(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json,
            package_hash: Some(format!("{}-different", package.hash)),
            enabled: true,
        })
        .await?;

    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "downloadUrl": download_url
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    assert_eq!(install_resp.status(), StatusCode::BAD_REQUEST);
    let body = body::to_bytes(install_resp.into_body(), 1_048_576).await?;
    let error_json: Value = serde_json::from_slice(&body)?;
    let message = error_json
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains("already installed"),
        "expected duplicate version rejection, got: {}",
        message
    );

    let _ = shutdown_tx.send(());
    Ok(())
}

#[tokio::test]
async fn extensions_catalog_refresh_uses_cache() -> Result<()> {
    let temp = tempdir()?;
    let extension_id = "elixir.test.catalog".to_string();
    let version = "1.0.0".to_string();
    let (addr, shutdown_tx) = start_registry_server(
        move |addr| {
            let download_url = format!("http://{addr}/package.elx");
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": extension_id,
                        "version": version,
                        "download_url": download_url,
                        "trust": "community"
                    }
                ]
            })
        },
        b"test".to_vec(),
    )
    .await?;

    let registry_url = format!("http://{addr}/registry.json");

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![registry_url];

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let refresh_resp = app
        .clone()
        .oneshot(Request::post("/api/v1/extensions/registries/refresh").body(Body::empty())?)
        .await?;
    assert_eq!(refresh_resp.status(), StatusCode::OK);
    let refresh_body = body::to_bytes(refresh_resp.into_body(), 1_048_576).await?;
    let refresh_json: Value = serde_json::from_slice(&refresh_body)?;
    assert_eq!(
        refresh_json["available"]
            .as_array()
            .map(|items| items.len())
            .unwrap_or(0),
        1
    );
    assert!(
        refresh_json["last_refreshed_at"].is_string(),
        "expected last_refreshed_at in refresh response"
    );
    assert!(
        refresh_json["last_refresh_success_at"].is_string(),
        "expected last_refresh_success_at in refresh response"
    );
    assert!(
        refresh_json["last_refresh_error"].is_null(),
        "expected last_refresh_error to be null on success"
    );

    let _ = shutdown_tx.send(());

    let catalog_resp = app
        .oneshot(Request::get("/api/v1/extensions/catalog").body(Body::empty())?)
        .await?;
    assert_eq!(catalog_resp.status(), StatusCode::OK);
    let catalog_body = body::to_bytes(catalog_resp.into_body(), 1_048_576).await?;
    let catalog_json: Value = serde_json::from_slice(&catalog_body)?;
    assert_eq!(
        catalog_json["available"]
            .as_array()
            .map(|items| items.len())
            .unwrap_or(0),
        1
    );
    assert!(
        catalog_json["last_refreshed_at"].is_string(),
        "expected last_refreshed_at in catalog response"
    );
    assert!(
        catalog_json["last_refresh_success_at"].is_string(),
        "expected last_refresh_success_at in catalog response"
    );
    assert!(
        catalog_json["last_refresh_error"].is_null(),
        "expected last_refresh_error to be null on success"
    );

    Ok(())
}

#[tokio::test]
async fn extensions_catalog_refresh_preserves_last_success_on_failure() -> Result<()> {
    let temp = tempdir()?;
    let extension_id = "elixir.test.catalog.failure".to_string();
    let version = "1.0.0".to_string();
    let (addr, shutdown_tx) = start_registry_server(
        move |addr| {
            let download_url = format!("http://{addr}/package.elx");
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": extension_id,
                        "version": version,
                        "download_url": download_url,
                        "trust": "community"
                    }
                ]
            })
        },
        b"test".to_vec(),
    )
    .await?;

    let registry_url = format!("http://{addr}/registry.json");

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![registry_url.clone()];

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let refresh_resp = app
        .clone()
        .oneshot(Request::post("/api/v1/extensions/registries/refresh").body(Body::empty())?)
        .await?;
    assert_eq!(refresh_resp.status(), StatusCode::OK);
    let refresh_body = body::to_bytes(refresh_resp.into_body(), 1_048_576).await?;
    let refresh_json: Value = serde_json::from_slice(&refresh_body)?;
    let first_success = refresh_json["last_refresh_success_at"]
        .as_str()
        .expect("expected last_refresh_success_at")
        .to_string();
    assert!(
        refresh_json["last_refresh_error"].is_null(),
        "expected last_refresh_error to be null on success"
    );

    let _ = shutdown_tx.send(());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let refresh_resp = app
        .oneshot(Request::post("/api/v1/extensions/registries/refresh").body(Body::empty())?)
        .await?;
    assert_eq!(refresh_resp.status(), StatusCode::OK);
    let refresh_body = body::to_bytes(refresh_resp.into_body(), 1_048_576).await?;
    let refresh_json: Value = serde_json::from_slice(&refresh_body)?;
    let second_success = refresh_json["last_refresh_success_at"]
        .as_str()
        .expect("expected last_refresh_success_at on failure")
        .to_string();
    assert_eq!(
        second_success, first_success,
        "last_refresh_success_at should remain from prior success"
    );
    let last_error = refresh_json["last_refresh_error"]
        .as_object()
        .expect("expected last_refresh_error object");
    assert_eq!(
        last_error
            .get("url")
            .and_then(|value| value.as_str())
            .unwrap_or_default(),
        registry_url
    );
    assert!(
        last_error
            .get("occurred_at")
            .and_then(|value| value.as_str())
            .is_some(),
        "expected occurred_at timestamp on last_refresh_error"
    );

    Ok(())
}

#[tokio::test]
async fn extensions_rejects_bad_signature() -> Result<()> {
    let temp = tempdir()?;
    let package = build_signed_package(temp.path()).await?;
    let package_bytes = tokio::fs::read(&package.path).await?;

    let bad_signature = "deadbeef".to_string();
    let extension_id = package.extension_id.clone();
    let version = package.version.clone();
    let hash = package.hash.clone();
    let publisher_key_id = package.publisher_key_id.clone();
    let (addr, shutdown_tx) = start_registry_server(
        move |addr| {
            let download_url = format!("http://{addr}/package.elx");
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": extension_id,
                        "version": version,
                        "download_url": download_url,
                        "sha256": hash,
                        "signature": bad_signature,
                        "publisher_key_id": publisher_key_id
                    }
                ]
            })
        },
        package_bytes,
    )
    .await?;

    let download_url = format!("http://{addr}/package.elx");
    let registry_url = format!("http://{addr}/registry.json");

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![registry_url];
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "downloadUrl": download_url
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    assert_eq!(install_resp.status(), StatusCode::BAD_REQUEST);

    let _ = shutdown_tx.send(());
    Ok(())
}

#[tokio::test]
async fn extensions_allows_unsigned_bundled_directory_install_in_production() -> Result<()> {
    let temp = tempdir()?;
    let bundled_dir = temp.path().join("bundled");
    let package_dir = bundled_dir.join("nzbget-module");
    std::fs::create_dir_all(&package_dir)?;
    let manifest = r#"id: elixir.modules.nzbget
version: 1.0.0
kind: module
name: "NZBGet"
provides:
  - capability: downloader.nzb
    slot: default
    implementation: "nzbget"
runtime:
  type: container
  image: "example/nzbget:1"
  env:
    - name: "NZBGET_USER"
      from_secret: "instance:nzbget_username"
    - name: "NZBGET_PASS"
      from_secret: "instance:nzbget_password"
"#;
    std::fs::write(package_dir.join("manifest.yaml"), manifest)?;

    let mut settings = test_settings_with_db();
    settings.environment = RunEnvironment::Production;
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.bundled_dir = bundled_dir.to_string_lossy().to_string();
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "packagePath": package_dir.to_string_lossy()
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    let status = install_resp.status();
    let body = body::to_bytes(install_resp.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "install failed body: {payload}");
    assert_eq!(
        payload.get("extension_id").and_then(Value::as_str),
        Some("elixir.modules.nzbget")
    );

    Ok(())
}

#[tokio::test]
async fn extensions_allows_unsigned_bundled_elx_install_in_production() -> Result<()> {
    let temp = tempdir()?;
    let bundled_dir = temp.path().join("bundled");
    std::fs::create_dir_all(&bundled_dir)?;
    let package_path = bundled_dir.join("qbittorrent-module.elx");
    let manifest = r#"id: elixir.modules.qbittorrent
version: 1.0.0
kind: module
name: "qBittorrent"
provides:
  - capability: downloader.torrent
    slot: default
    implementation: "qbittorrent"
runtime:
  type: container
  image: "example/qbittorrent:1"
  env:
    - name: "QBITTORRENT_USERNAME"
      from_secret: "instance:qbittorrent_username"
    - name: "QBITTORRENT_PASSWORD"
      from_secret: "instance:qbittorrent_password"
"#;
    let file = File::create(&package_path)?;
    let mut zip = ZipWriter::new(file);
    let options = FileOptions::<()>::default();
    zip.start_file("manifest.yaml", options)?;
    zip.write_all(manifest.as_bytes())?;
    zip.finish()?;

    let mut settings = test_settings_with_db();
    settings.environment = RunEnvironment::Production;
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.bundled_dir = bundled_dir.to_string_lossy().to_string();
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "packagePath": package_path.to_string_lossy()
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    let status = install_resp.status();
    let body = body::to_bytes(install_resp.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "install failed body: {payload}");
    assert_eq!(
        payload.get("extension_id").and_then(Value::as_str),
        Some("elixir.modules.qbittorrent")
    );

    Ok(())
}

#[tokio::test]
async fn extensions_rejects_untrusted_extension() -> Result<()> {
    let temp = tempdir()?;
    let package_dir = temp.path().join("package");
    std::fs::create_dir_all(&package_dir)?;
    let manifest = r#"id: elixir.test.permission
version: 0.1.0
kind: module
name: "Permission Test"
trust: untrusted
permissions:
  - runtime.manage_containers
provides:
  - capability: media.manager.tv
    slot: default
    implementation: "sonarr"
runtime:
  type: container
  image: "example/test:1"
"#;
    std::fs::write(package_dir.join("manifest.yaml"), manifest)?;

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.allow_unsigned = true;
    settings.extensions.allow_directory_install = true;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "packagePath": package_dir.to_string_lossy()
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    assert_eq!(install_resp.status(), StatusCode::FORBIDDEN);

    Ok(())
}

#[tokio::test]
async fn extensions_secrets_crud_and_rotate() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let create_body = json!({
        "scope": "global",
        "key": "api_key",
        "value": "initial-secret",
        "rotatable": true
    });
    let create_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/secrets")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))?,
        )
        .await?;
    assert_eq!(create_resp.status(), StatusCode::OK);
    let create_body = body::to_bytes(create_resp.into_body(), 1_048_576).await?;
    let create_json: Value = serde_json::from_slice(&create_body)?;
    let secret_id = create_json
        .get("secretId")
        .and_then(Value::as_str)
        .expect("secretId");

    let list_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/secrets?scope=global").body(Body::empty())?)
        .await?;
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = body::to_bytes(list_resp.into_body(), 1_048_576).await?;
    let list_json: Value = serde_json::from_slice(&list_body)?;
    assert!(
        list_json
            .as_array()
            .and_then(|items| items.first())
            .and_then(|item| item.get("key"))
            .and_then(Value::as_str)
            .is_some(),
        "expected secret list entry"
    );

    let get_resp = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/extensions/secrets/{secret_id}")).body(Body::empty())?,
        )
        .await?;
    assert_eq!(get_resp.status(), StatusCode::OK);

    let update_body = json!({
        "value": "updated-secret"
    });
    let update_resp = app
        .clone()
        .oneshot(
            Request::patch(format!("/api/v1/extensions/secrets/{secret_id}"))
                .header("content-type", "application/json")
                .body(Body::from(update_body.to_string()))?,
        )
        .await?;
    assert_eq!(update_resp.status(), StatusCode::OK);

    let rotate_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/extensions/secrets/{secret_id}/rotate"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(rotate_resp.status(), StatusCode::OK);
    let rotate_body = body::to_bytes(rotate_resp.into_body(), 1_048_576).await?;
    let rotate_json: Value = serde_json::from_slice(&rotate_body)?;
    let rotated_value = rotate_json
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(!rotated_value.is_empty(), "rotate should return a value");

    let delete_resp = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/extensions/secrets/{secret_id}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let get_resp = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/extensions/secrets/{secret_id}")).body(Body::empty())?,
        )
        .await?;
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn extensions_install_requires_global_secret() -> Result<()> {
    let temp = tempdir()?;
    let package_dir = temp.path().join("package");
    std::fs::create_dir_all(&package_dir)?;
    let manifest = r#"id: elixir.test.secret_install
version: 0.1.0
kind: module
name: "Secret Install"
provides:
  - capability: media.manager.tv
    slot: default
    implementation: "sonarr"
runtime:
  type: container
  image: "example/test:1"
  env:
    - name: "API_KEY"
      from_secret: "global:api_key"
"#;
    std::fs::write(package_dir.join("manifest.yaml"), manifest)?;

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.allow_unsigned = true;
    settings.extensions.allow_directory_install = true;

    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let secrets_for_store = secrets.clone();
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let install_body = json!({
        "packagePath": package_dir.to_string_lossy()
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    assert_eq!(install_resp.status(), StatusCode::BAD_REQUEST);

    let store = ExtensionStore::new(&db_pool);
    let encrypted = secrets_for_store.encrypt("api-key")?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: "api_key".to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await?;

    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_body.to_string()))?,
        )
        .await?;
    assert_eq!(install_resp.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn extensions_enable_requires_global_secret() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let secrets_for_store = secrets.clone();
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    let manifest = json!({
        "id": "elixir.test.secret_enable",
        "version": "0.1.0",
        "kind": "module",
        "name": "Secret Enable",
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
            "env": [
                {
                    "name": "API_KEY",
                    "from_secret": "global:api_key"
                }
            ]
        }
    });
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.test.secret_enable".to_string(),
            name: "Secret Enable".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: manifest,
            package_hash: None,
            enabled: false,
        })
        .await?;

    let enable_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/elixir.test.secret_enable/enable")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(enable_resp.status(), StatusCode::BAD_REQUEST);

    let encrypted = secrets_for_store.encrypt("api-key")?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: "api_key".to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await?;

    let enable_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/elixir.test.secret_enable/enable")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(enable_resp.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn extensions_enable_instance_requires_secret() -> Result<()> {
    let runtime_env = vec![json!({
        "name": "API_KEY",
        "from_secret": "instance:api_key"
    })];
    let (app, instance_id) = setup_extension_instance(
        "elixir.test.instance_secret",
        "Instance Secret",
        Some(runtime_env),
        false,
    )
    .await?;

    let enable_body = json!({ "enabled": true });
    let enable_resp = app
        .clone()
        .oneshot(
            Request::patch(format!("/api/v1/extensions/instances/{instance_id}"))
                .header("content-type", "application/json")
                .body(Body::from(enable_body.to_string()))?,
        )
        .await?;
    assert_eq!(enable_resp.status(), StatusCode::BAD_REQUEST);

    let secret_body = json!({
        "scope": "instance",
        "scopeId": instance_id.to_string(),
        "key": "api_key",
        "value": "instance-key"
    });
    let secret_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/secrets")
                .header("content-type", "application/json")
                .body(Body::from(secret_body.to_string()))?,
        )
        .await?;
    assert_eq!(secret_resp.status(), StatusCode::OK);

    let enable_resp = app
        .clone()
        .oneshot(
            Request::patch(format!("/api/v1/extensions/instances/{instance_id}"))
                .header("content-type", "application/json")
                .body(Body::from(enable_body.to_string()))?,
        )
        .await?;
    assert_eq!(enable_resp.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn extensions_plan_confirm_resolves_slot_conflict() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);

    store
        .upsert_extension(&NewExtension {
            extension_id: "ext.existing".to_string(),
            name: "Existing Module".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "ext.existing",
                "version": "0.1.0",
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
                    "image": "example/test:1"
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
            extension_id: "ext.prompt".to_string(),
            name: "Prompt Module".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "ext.prompt",
                "version": "0.1.0",
                "kind": "module",
                "name": "Prompt Module",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr"
                    }
                ],
                "conflicts": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "policy": "prompt_replace"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "example/test:1"
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
            extension_id: "blueprint.conflict".to_string(),
            name: "Conflict Blueprint".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Blueprint,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "blueprint.conflict",
                "version": "0.1.0",
                "kind": "blueprint",
                "name": "Conflict Blueprint",
                "wants": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default"
                    }
                ],
                "preferences": {
                    "providers": {
                        "media.manager.tv/default": {
                            "prefer": ["ext.prompt"]
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
    store
        .update_instance_runtime_version(existing_instance_id, "0.1.0", None)
        .await?;

    let existing_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: existing_provider_id,
            instance_id: existing_instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: None,
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let plan_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/blueprints/apply")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "blueprint_id": "blueprint.conflict"
                    })
                    .to_string(),
                ))?,
        )
        .await?;

    assert_eq!(plan_response.status(), StatusCode::OK);
    let body = body::to_bytes(plan_response.into_body(), 1_048_576).await?;
    let plan_json: Value = serde_json::from_slice(&body)?;
    let plan_id = plan_json
        .get("plan_id")
        .and_then(Value::as_str)
        .expect("plan_id");
    let plan_uuid = Uuid::parse_str(plan_id)?;
    assert!(
        store
            .list_desired_blueprints(None)
            .await?
            .into_iter()
            .all(|item| item.desired_id != plan_uuid),
        "preview should not persist durable desired state"
    );

    let conflicts = plan_json
        .get("conflicts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        conflicts.iter().any(|conflict| {
            conflict.get("code") == Some(&json!("slot_conflict"))
                && conflict.get("policy") == Some(&json!("prompt"))
        }),
        "expected prompt slot conflict"
    );

    let confirm_response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/extensions/plan/{plan_id}/confirm"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "decisions": {
                            "slotConflicts": [
                                {
                                    "conflictId": "media.manager.tv/default",
                                    "action": "keep_existing"
                                }
                            ]
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;

    assert_eq!(confirm_response.status(), StatusCode::OK);
    let body = body::to_bytes(confirm_response.into_body(), 1_048_576).await?;
    let confirm_json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        confirm_json.get("status").and_then(Value::as_str),
        Some("completed")
    );

    let run_id = confirm_json
        .get("run_id")
        .and_then(Value::as_str)
        .expect("run_id");
    let run = store
        .get_run(Uuid::parse_str(run_id)?)
        .await?
        .expect("run exists");
    let resolved_plan = run.plan_json.expect("plan_json");
    let resolved_conflicts = resolved_plan
        .get("conflicts")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        !resolved_conflicts
            .iter()
            .any(|conflict| { conflict.get("code") == Some(&json!("slot_conflict")) }),
        "expected slot conflict to be resolved"
    );

    let desired = store.list_desired_blueprints(None).await?;
    let desired_entry = desired
        .iter()
        .find(|item| item.desired_id == plan_uuid)
        .expect("desired blueprint");
    assert!(desired_entry.applied, "expected desired blueprint applied");
    let decisions = desired_entry
        .decisions_json
        .as_ref()
        .and_then(|value| value.get("slotConflicts"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        decisions.iter().any(|decision| {
            decision.get("conflictId") == Some(&json!("media.manager.tv/default"))
                && decision.get("action") == Some(&json!("keep_existing"))
        }),
        "expected keep_existing decision to persist"
    );

    Ok(())
}

#[tokio::test]
async fn extensions_apply_blueprint_is_idempotent_for_pending_plan() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: "ext.idempotent.module".to_string(),
            name: "Idempotent Module".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "ext.idempotent.module",
                "version": "1.0.0",
                "kind": "module",
                "name": "Idempotent Module",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "example/test:1"
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
            extension_id: "blueprint.idempotent".to_string(),
            name: "Idempotent Blueprint".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Blueprint,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "blueprint.idempotent",
                "version": "1.0.0",
                "kind": "blueprint",
                "name": "Idempotent Blueprint",
                "wants": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default"
                    }
                ],
                "preferences": {
                    "providers": {
                        "media.manager.tv/default": {
                            "prefer": ["ext.idempotent.module"]
                        }
                    }
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let apply_body = json!({
        "blueprint_id": "blueprint.idempotent"
    })
    .to_string();
    let first = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/blueprints/apply")
                .header("content-type", "application/json")
                .body(Body::from(apply_body.clone()))?,
        )
        .await?;
    assert_eq!(first.status(), StatusCode::OK);
    let first_json: Value =
        serde_json::from_slice(&body::to_bytes(first.into_body(), 1_048_576).await?)?;
    let first_plan_id = first_json
        .get("plan_id")
        .and_then(Value::as_str)
        .expect("first plan id");

    let second = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/blueprints/apply")
                .header("content-type", "application/json")
                .body(Body::from(apply_body))?,
        )
        .await?;
    assert_eq!(second.status(), StatusCode::OK);
    let second_json: Value =
        serde_json::from_slice(&body::to_bytes(second.into_body(), 1_048_576).await?)?;
    let second_plan_id = second_json
        .get("plan_id")
        .and_then(Value::as_str)
        .expect("second plan id");
    assert_eq!(first_plan_id, second_plan_id, "expected plan reuse");

    let pending: Vec<_> = store
        .list_desired_blueprints(Some(false))
        .await?
        .into_iter()
        .filter(|item| item.blueprint_extension_id == "blueprint.idempotent")
        .collect();
    assert!(
        pending.is_empty(),
        "preview should not create pending desired blueprint rows"
    );

    let pending_runs = store
        .list_runs_by_source_status("blueprint", OrchestratorRunStatus::Pending)
        .await?;
    assert_eq!(
        pending_runs.len(),
        1,
        "expected a single reusable pending blueprint run"
    );
    assert_eq!(
        pending_runs[0].run_id.to_string(),
        first_plan_id,
        "pending preview run id should match reused plan id"
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_returns_sonarr_metrics_and_action() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let (sonarr_host, sonarr_addr, shutdown_tx) = start_mock_sonarr_server().await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "lscr.io/linuxserver/sonarr:latest"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({
                "api_key": "test-sonarr-key"
            })),
            enabled: true,
        })
        .await?;

    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": sonarr_host,
                "port": sonarr_addr.port(),
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.sonarr/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    assert_eq!(
        payload.get("extensionId").and_then(Value::as_str),
        Some("elixir.modules.sonarr")
    );
    assert_eq!(payload.get("name").and_then(Value::as_str), Some("Sonarr"));
    assert_eq!(
        payload.pointer("/status/summary").and_then(Value::as_str),
        Some("Ready")
    );
    assert_eq!(
        payload
            .pointer("/actions/0/id")
            .and_then(Value::as_str),
        Some("test_connection")
    );

    let metrics = payload
        .pointer("/status/telemetry/metrics")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        metrics
            .iter()
            .any(|metric| metric.get("id").and_then(Value::as_str) == Some("seriesCount")
                && metric.get("value").and_then(Value::as_str) == Some("2")),
        "expected series count metric in control surface: {}",
        payload
    );
    assert!(
        metrics
            .iter()
            .any(|metric| metric.get("id").and_then(Value::as_str) == Some("downloadClientCount")
                && metric.get("value").and_then(Value::as_str) == Some("1")),
        "expected download client count metric in control surface: {}",
        payload
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_action_test_connection_returns_success() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let (sonarr_host, sonarr_addr, shutdown_tx) = start_mock_sonarr_server().await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "lscr.io/linuxserver/sonarr:latest"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({
                "api_key": "test-sonarr-key"
            })),
            enabled: true,
        })
        .await?;

    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": sonarr_host,
                "port": sonarr_addr.port(),
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.sonarr/control-surface/actions/test_connection",
            )
            .header("content-type", "application/json")
            .body(Body::from("{}"))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected control action response: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload.get("success").and_then(Value::as_bool), Some(true));
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("Sonarr is reachable. Version 4.0.0.778."),
        "expected connection success message in control action response"
    );
    assert_eq!(
        payload
            .pointer("/controlSurface/actions/0/id")
            .and_then(Value::as_str),
        Some("test_connection")
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_includes_sonarr_defaults_and_managed_items() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let (sonarr_host, sonarr_addr, _server_state, shutdown_tx) =
        start_mock_sonarr_control_server().await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
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
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "test-sonarr-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": sonarr_host,
                "port": sonarr_addr.port(),
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_managed_ingest_intent(&NewManagedIngestIntent {
            media_type: MediaType::Series,
            title: "Noble House".to_string(),
            normalized_title: "noble house".to_string(),
            year: Some(1988),
            external_ids: None,
            manager_provider_id: provider_id,
            manager_item_id: Some("42".to_string()),
            manager_label: Some("default (sonarr)".to_string()),
            source: "find_media".to_string(),
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.sonarr/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    let defaults_fields = control_surface_section(&payload, "defaults")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        defaults_fields
            .iter()
            .any(|field| field.get("id").and_then(Value::as_str) == Some("monitorOnAdd")
                && field.get("value").and_then(Value::as_bool) == Some(true)),
        "expected monitorOnAdd field in defaults section: {}",
        payload
    );
    assert!(
        defaults_fields
            .iter()
            .any(|field| field.get("id").and_then(Value::as_str) == Some("searchOnAdd")
                && field.get("value").and_then(Value::as_bool) == Some(true)),
        "expected searchOnAdd field in defaults section: {}",
        payload
    );

    let entities = control_surface_section(&payload, "managedItems")
        .get("entities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        entities.iter().any(|entity| entity.get("title").and_then(Value::as_str)
            == Some("Noble House (1988)")),
        "expected Noble House managed item in control surface: {}",
        payload
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_action_remove_item_deactivates_intent() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let (sonarr_host, sonarr_addr, server_state, shutdown_tx) =
        start_mock_sonarr_control_server().await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
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
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "test-sonarr-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": sonarr_host,
                "port": sonarr_addr.port(),
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    let intent_id = store
        .upsert_managed_ingest_intent(&NewManagedIngestIntent {
            media_type: MediaType::Series,
            title: "Noble House".to_string(),
            normalized_title: "noble house".to_string(),
            year: Some(1988),
            external_ids: None,
            manager_provider_id: provider_id,
            manager_item_id: Some("42".to_string()),
            manager_label: Some("default (sonarr)".to_string()),
            source: "find_media".to_string(),
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.sonarr/control-surface/actions/remove_item",
            )
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&json!({
                "params": {
                    "intentId": intent_id.to_string()
                }
            }))?))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);

    assert_eq!(
        store.list_active_managed_ingest_intents().await?.len(),
        0,
        "remove action should deactivate the managed ingest intent"
    );
    assert_eq!(
        server_state.deletes.lock().unwrap().as_slice(),
        &["42".to_string()],
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_action_search_and_refresh_item_issue_sonarr_commands() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let (sonarr_host, sonarr_addr, server_state, shutdown_tx) =
        start_mock_sonarr_control_server().await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
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
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "test-sonarr-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": sonarr_host,
                "port": sonarr_addr.port(),
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    let intent_id = store
        .upsert_managed_ingest_intent(&NewManagedIngestIntent {
            media_type: MediaType::Series,
            title: "Noble House".to_string(),
            normalized_title: "noble house".to_string(),
            year: Some(1988),
            external_ids: None,
            manager_provider_id: provider_id,
            manager_item_id: Some("42".to_string()),
            manager_label: Some("default (sonarr)".to_string()),
            source: "find_media".to_string(),
        })
        .await?;

    for action_id in ["search_item", "refresh_item"] {
        let response = app
            .clone()
            .oneshot(
                Request::post(format!(
                    "/api/v1/extensions/elixir.modules.sonarr/control-surface/actions/{action_id}"
                ))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "params": {
                        "intentId": intent_id.to_string()
                    }
                }))?))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let _ = shutdown_tx.send(());

    let commands = server_state.commands.lock().unwrap().clone();
    assert!(
        commands.iter().any(|payload| {
            payload.get("name").and_then(Value::as_str) == Some("SeriesSearch")
                && payload.get("seriesId").and_then(Value::as_i64) == Some(42)
        }),
        "expected SeriesSearch command: {:?}",
        commands
    );
    assert!(
        commands.iter().any(|payload| {
            payload.get("name").and_then(Value::as_str) == Some("RefreshSeries")
                && payload.get("seriesId").and_then(Value::as_i64) == Some(42)
        }),
        "expected RefreshSeries command: {:?}",
        commands
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_settings_update_persists_sonarr_defaults() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
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
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "test-sonarr-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.tv".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("sonarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "svc-sonarr",
                "port": 8989,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/extensions/elixir.modules.sonarr/control-surface")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "values": {
                        "monitorOnAdd": false,
                        "searchOnAdd": false
                    }
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    let fields = control_surface_section(&payload, "defaults")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("monitorOnAdd")
                && field.get("value").and_then(Value::as_bool) == Some(false)
        }),
        "expected updated monitorOnAdd field: {}",
        payload
    );
    assert!(
        fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("searchOnAdd")
                && field.get("value").and_then(Value::as_bool) == Some(false)
        }),
        "expected updated searchOnAdd field: {}",
        payload
    );
    let stored = store
        .get_extension_setting(&format!("extensions.control_defaults.instance.{instance_id}"))
        .await?
        .unwrap_or(Value::Null);
    assert_eq!(stored.get("monitorOnAdd").and_then(Value::as_bool), Some(false));
    assert_eq!(stored.get("searchOnAdd").and_then(Value::as_bool), Some(false));

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_includes_prowlarr_managed_and_manual_indexers() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let (prowlarr_host, prowlarr_addr, shutdown_tx) = start_mock_prowlarr_control_server(
        vec![
            json!({
                "name": "AnimeTosho",
                "implementation": "Torznab",
                "enable": true,
                "appProfileId": 1
            }),
            json!({
                "name": "Private Tracker",
                "implementation": "Torznab",
                "enable": true,
                "appProfileId": 1
            }),
        ],
        vec![json!({ "name": "Sonarr" }), json!({ "name": "Radarr" })],
    )
    .await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.prowlarr".to_string(),
            name: "Prowlarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.prowlarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Prowlarr",
                "provides": [{
                    "capability": "indexer.registry",
                    "slot": "default",
                    "implementation": "prowlarr"
                }]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.prowlarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "test-prowlarr-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "indexer.registry".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("prowlarr".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": prowlarr_host,
                "port": prowlarr_addr.port(),
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.connectors.prowlarr_public_indexers".to_string(),
            name: "Prowlarr Public Indexers".to_string(),
            version: "1.0.2".to_string(),
            kind: ExtensionKind::Connector,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.connectors.prowlarr_public_indexers",
                "version": "1.0.2",
                "kind": "connector",
                "name": "Prowlarr Public Indexers",
                "targets": [{
                    "capability": "indexer.registry",
                    "slot": "default"
                }],
                "actions": [{
                    "type": "driver_patch",
                    "target": {
                        "capability": "indexer.registry",
                        "slot": "default"
                    },
                    "patch": {
                        "op": "register_indexers",
                        "indexers": [{
                            "name": "AnimeTosho",
                            "implementation": "Torznab",
                            "url": "https://feed.animetosho.org",
                            "enabled": true
                        }]
                    }
                }]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.blueprints.arr_stack".to_string(),
            name: "Arr Stack".to_string(),
            version: "1.0.5".to_string(),
            kind: ExtensionKind::Blueprint,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.blueprints.arr_stack",
                "version": "1.0.5",
                "kind": "blueprint",
                "name": "Arr Stack",
                "optional_addons": [{
                    "extension_id": "elixir.connectors.prowlarr_nzbgeek",
                    "title": "NZBGeek",
                    "description": "Add NZBGeek to Prowlarr.",
                    "required_fields": ["api_key"],
                    "secret_key_prefix": "nzbgeek",
                    "target": {
                        "capability": "indexer.registry",
                        "slot": "default"
                    }
                }]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.prowlarr/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    let managed_entities = control_surface_section(&payload, "managedIndexers")
        .get("entities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let anime_tosho = managed_entities
        .iter()
        .find(|entity| entity.get("title").and_then(Value::as_str) == Some("AnimeTosho"))
        .expect("AnimeTosho managed indexer entity");
    assert_eq!(
        anime_tosho.get("subtitle").and_then(Value::as_str),
        Some("Managed by Elixir via Prowlarr Public Indexers")
    );
    let private_tracker = managed_entities
        .iter()
        .find(|entity| entity.get("title").and_then(Value::as_str) == Some("Private Tracker"))
        .expect("manual indexer entity");
    assert_eq!(
        private_tracker.get("subtitle").and_then(Value::as_str),
        Some("Custom in Prowlarr")
    );

    let connector_entities = control_surface_section(&payload, "addConnector")
        .get("entities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let public_connector = connector_entities
        .iter()
        .find(|entity| {
            entity.get("title").and_then(Value::as_str)
                == Some("Prowlarr Public Indexers")
        })
        .expect("public connector entity");
    assert_eq!(
        public_connector.pointer("/actions/0/navigateExtensionId").and_then(Value::as_str),
        Some("elixir.connectors.prowlarr_public_indexers")
    );

    let nzbgeek = connector_entities
        .iter()
        .find(|entity| entity.get("title").and_then(Value::as_str) == Some("NZBGeek"))
        .expect("nzbgeek optional addon entity");
    assert_eq!(
        nzbgeek.pointer("/actions/0/id").and_then(Value::as_str),
        Some("activate_connector")
    );
    assert_eq!(
        nzbgeek.pointer("/actions/0/requiredFields/0").and_then(Value::as_str),
        Some("api_key")
    );
    assert_eq!(
        nzbgeek.pointer("/actions/0/secretKeys/0").and_then(Value::as_str),
        Some("nzbgeek.api_key")
    );

    let manual_actions = control_surface_section(&payload, "manualSetup")
        .get("actions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        manual_actions
            .iter()
            .find(|action| action.get("id").and_then(Value::as_str) == Some("open_service_ui"))
            .and_then(|action| action.get("label"))
            .and_then(Value::as_str),
        Some("Open Prowlarr UI")
    );
    assert!(
        manual_actions
            .iter()
            .find(|action| action.get("id").and_then(Value::as_str) == Some("open_service_ui"))
            .and_then(|action| action.get("openUrl"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("/api/v1/extensions/instances/"),
        "expected manual section to expose proxied Elixir UI entrypoint: {}",
        payload
    );
    assert!(
        manual_actions
            .iter()
            .find(|action| action.get("id").and_then(Value::as_str) == Some("open_service_ui"))
            .and_then(|action| action.get("openUrl"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .ends_with("/ui/start"),
        "expected manual section to expose proxied start path: {}",
        payload
    );

    Ok(())
}

#[tokio::test]
async fn extensions_plan_validation_allows_planned_provider_target() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let store = ExtensionStore::new(&database.pool);

    let provider_id = Uuid::new_v4();
    let instance_id = Uuid::new_v4();
    let actions = vec![
        PlanAction::CreateOrUpdateProvider {
            provider: ProviderSpec {
                provider_id,
                instance_id,
                capability: "indexer.registry".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("prowlarr".to_string()),
                scope_json: None,
                endpoint: ProviderEndpoint {
                    scheme: "http".to_string(),
                    host: "svc-prowlarr".to_string(),
                    port: 9696,
                    base_path: "/".to_string(),
                    network: Some("elixir_net".to_string()),
                },
            },
        },
        PlanAction::ApplyDriverPatch {
            patch: DriverPatchSpec {
                connector_extension_id: "connector.test".to_string(),
                target_provider_id: provider_id,
                target_capability: "indexer.registry".to_string(),
                target_slot_id: "default".to_string(),
                patch: json!({
                    "op": "register_indexers",
                    "indexers": []
                }),
            },
        },
    ];

    let missing = missing_required_secrets_for_plan(&store, &actions).await?;
    assert!(
        missing.is_empty(),
        "planned provider target should validate without provider DB preexistence"
    );
    Ok(())
}

#[tokio::test]
async fn extensions_desired_blueprints_list_and_clear() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
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
                ]
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let applied_id = Uuid::new_v4();
    let applied_id_str = applied_id.to_string();
    store
        .create_desired_blueprint(&NewDesiredBlueprint {
            desired_id: applied_id,
            blueprint_extension_id: "blueprint.keep".to_string(),
            blueprint_version: "1.0.0".to_string(),
            params_json: None,
            decisions_json: None,
        })
        .await?;
    store.mark_desired_applied(applied_id, true).await?;

    let pending_id = Uuid::new_v4();
    let pending_id_str = pending_id.to_string();
    store
        .create_desired_blueprint(&NewDesiredBlueprint {
            desired_id: pending_id,
            blueprint_extension_id: "blueprint.keep".to_string(),
            blueprint_version: "1.0.0".to_string(),
            params_json: None,
            decisions_json: None,
        })
        .await?;

    let applied_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/desired-blueprints?applied=true")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(applied_resp.status(), StatusCode::OK);
    let applied_body = body::to_bytes(applied_resp.into_body(), 1_048_576).await?;
    let applied_items: Vec<Value> = serde_json::from_slice(&applied_body)?;
    assert_eq!(applied_items.len(), 1);
    assert_eq!(
        applied_items[0].get("desired_id").and_then(Value::as_str),
        Some(applied_id_str.as_str())
    );

    let pending_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/desired-blueprints?applied=false")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(pending_resp.status(), StatusCode::OK);
    let pending_body = body::to_bytes(pending_resp.into_body(), 1_048_576).await?;
    let pending_items: Vec<Value> = serde_json::from_slice(&pending_body)?;
    assert_eq!(pending_items.len(), 1);
    assert_eq!(
        pending_items[0].get("desired_id").and_then(Value::as_str),
        Some(pending_id_str.as_str())
    );

    let delete_resp = app
        .clone()
        .oneshot(
            Request::delete("/api/v1/extensions/desired-blueprints?applied=false")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(delete_resp.status(), StatusCode::OK);
    let delete_body = body::to_bytes(delete_resp.into_body(), 1_048_576).await?;
    let delete_json: Value = serde_json::from_slice(&delete_body)?;
    assert_eq!(delete_json.get("deleted").and_then(Value::as_u64), Some(1));

    let remaining_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/desired-blueprints").body(Body::empty())?)
        .await?;
    assert_eq!(remaining_resp.status(), StatusCode::OK);
    let remaining_body = body::to_bytes(remaining_resp.into_body(), 1_048_576).await?;
    let remaining_items: Vec<Value> = serde_json::from_slice(&remaining_body)?;
    assert_eq!(remaining_items.len(), 1);
    assert_eq!(
        remaining_items[0].get("desired_id").and_then(Value::as_str),
        Some(applied_id_str.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn extensions_uninstall_blueprint_cascades_dependencies() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.sonarr".to_string(),
            name: "Sonarr".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "lscr.io/linuxserver/sonarr:latest"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.connectors.sonarr_defaults".to_string(),
            name: "Sonarr Defaults".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Connector,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.connectors.sonarr_defaults",
                "version": "1.0.0",
                "kind": "connector",
                "name": "Sonarr Defaults",
                "targets": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default"
                    }
                ],
                "actions": [
                    {
                        "type": "driver_patch",
                        "target": {
                            "capability": "media.manager.tv",
                            "slot": "default"
                        },
                        "patch": {
                            "op": "set_tags",
                            "params": {
                                "tags": ["elixir"]
                            }
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
            extension_id: "elixir.blueprints.arr_stack".to_string(),
            name: "Arr Stack".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Blueprint,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.blueprints.arr_stack",
                "version": "1.0.0",
                "kind": "blueprint",
                "name": "Arr Stack",
                "wants": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default"
                    }
                ],
                "connectors": ["elixir.connectors.sonarr_defaults"],
                "preferences": {
                    "providers": {
                        "media.manager.tv/default": {
                            "prefer": ["elixir.modules.sonarr"]
                        }
                    }
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    store
        .create_desired_blueprint(&NewDesiredBlueprint {
            desired_id: Uuid::new_v4(),
            blueprint_extension_id: "elixir.blueprints.arr_stack".to_string(),
            blueprint_version: "1.0.0".to_string(),
            params_json: None,
            decisions_json: None,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/elixir.blueprints.arr_stack/uninstall")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let deleted = payload
        .get("deletedExtensions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        deleted
            .iter()
            .any(|item| item.as_str() == Some("elixir.blueprints.arr_stack"))
    );
    assert!(
        deleted
            .iter()
            .any(|item| item.as_str() == Some("elixir.connectors.sonarr_defaults"))
    );
    assert!(
        deleted
            .iter()
            .any(|item| item.as_str() == Some("elixir.modules.sonarr"))
    );

    assert!(
        store
            .get_extension("elixir.blueprints.arr_stack")
            .await?
            .is_none()
    );
    assert!(
        store
            .get_extension("elixir.connectors.sonarr_defaults")
            .await?
            .is_none()
    );
    assert!(
        store
            .get_extension("elixir.modules.sonarr")
            .await?
            .is_none()
    );

    let desired = store.list_desired_blueprints(None).await?;
    assert!(
        desired.is_empty(),
        "expected desired blueprints to be removed with blueprint uninstall"
    );

    Ok(())
}

#[tokio::test]
async fn extensions_clear_runs_deletes_pending_entries() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    store
        .create_run(&NewOrchestratorRun {
            run_id: Uuid::new_v4(),
            source: "manual".to_string(),
            status: OrchestratorRunStatus::Pending,
            phase: Some("plan".to_string()),
            plan_json: None,
            error: None,
        })
        .await?;
    store
        .create_run(&NewOrchestratorRun {
            run_id: Uuid::new_v4(),
            source: "manual".to_string(),
            status: OrchestratorRunStatus::Completed,
            phase: Some("completed".to_string()),
            plan_json: None,
            error: None,
        })
        .await?;
    let stale_running_id = Uuid::new_v4();
    store
        .create_run(&NewOrchestratorRun {
            run_id: stale_running_id,
            source: "reconcile".to_string(),
            status: OrchestratorRunStatus::Running,
            phase: Some("reconcile".to_string()),
            plan_json: None,
            error: None,
        })
        .await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE orchestrator_runs SET created_at = '2000-01-01 00:00:00' WHERE run_id = ?",
    )
    .bind(stale_running_id.to_string())
    .execute(&db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(Request::delete("/api/v1/extensions/runs").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload.get("deleted").and_then(Value::as_u64), Some(3));

    let remaining = store.list_runs(None).await?;
    assert!(remaining.is_empty(), "expected all clearable runs removed");

    Ok(())
}

#[tokio::test]
async fn extensions_reconcile_now_and_latest() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let latest_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/reconcile/latest").body(Body::empty())?)
        .await?;
    assert_eq!(latest_resp.status(), StatusCode::OK);
    let latest_body = body::to_bytes(latest_resp.into_body(), 1_048_576).await?;
    let latest_json: Value = serde_json::from_slice(&latest_body)?;
    assert!(
        latest_json.get("run").is_none() || latest_json.get("run") == Some(&Value::Null),
        "expected no reconcile run yet"
    );

    let now_resp = app
        .clone()
        .oneshot(Request::post("/api/v1/extensions/reconcile/now").body(Body::empty())?)
        .await?;
    assert_eq!(now_resp.status(), StatusCode::OK);
    let now_body = body::to_bytes(now_resp.into_body(), 1_048_576).await?;
    let now_json: Value = serde_json::from_slice(&now_body)?;
    let run_id = now_json
        .get("run")
        .and_then(|value| value.get("run_id"))
        .and_then(Value::as_str)
        .expect("run_id");

    let latest_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/reconcile/latest").body(Body::empty())?)
        .await?;
    assert_eq!(latest_resp.status(), StatusCode::OK);
    let latest_body = body::to_bytes(latest_resp.into_body(), 1_048_576).await?;
    let latest_json: Value = serde_json::from_slice(&latest_body)?;
    let latest_run_id = latest_json
        .get("run")
        .and_then(|value| value.get("run_id"))
        .and_then(Value::as_str)
        .expect("latest run_id");
    assert_eq!(latest_run_id, run_id);

    Ok(())
}

#[tokio::test]
async fn extensions_auto_wire_status_default() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let status_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/auto-wire").body(Body::empty())?)
        .await?;
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_body = body::to_bytes(status_resp.into_body(), 1_048_576).await?;
    let status_json: Value = serde_json::from_slice(&status_body)?;
    assert_eq!(
        status_json.get("enabled").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(status_json.get("pendingPlanId"), Some(&Value::Null));
    assert_eq!(status_json.get("pendingReason"), Some(&Value::Null));
    assert_eq!(status_json.get("pendingConflicts"), Some(&Value::Null));

    Ok(())
}

#[tokio::test]
async fn extensions_auto_wire_status_and_plan_pending() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let db_pool = database.pool.clone();
    let store = ExtensionStore::new(&db_pool);
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let plan_id = Uuid::new_v4();
    let instance_id = Uuid::new_v4();
    let plan = Plan {
        plan_id,
        blueprint_id: Planner::AUTO_WIRE_BLUEPRINT_ID.to_string(),
        params: None,
        actions: Vec::new(),
        conflicts: vec![json!({
            "code": "missing_required_secrets",
            "extension_id": "ext.indexer",
            "instance_id": instance_id,
            "instance_name": "default",
            "missing": [format!("instance:{instance_id}:indexer.test-indexer.api_key")],
        })],
    };
    store
        .create_run(&NewOrchestratorRun {
            run_id: plan_id,
            source: "auto_wire".to_string(),
            status: OrchestratorRunStatus::Pending,
            phase: Some("auto_wire".to_string()),
            plan_json: Some(serde_json::to_value(&plan)?),
            error: None,
        })
        .await?;

    let status_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/auto-wire").body(Body::empty())?)
        .await?;
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_body = body::to_bytes(status_resp.into_body(), 1_048_576).await?;
    let status_json: Value = serde_json::from_slice(&status_body)?;
    assert_eq!(
        status_json.get("enabled").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        status_json.get("pendingPlanId").and_then(Value::as_str),
        Some(plan_id.to_string().as_str())
    );
    assert_eq!(
        status_json.get("pendingReason").and_then(Value::as_str),
        Some("Missing required secrets")
    );
    assert_eq!(
        status_json.get("pendingConflicts").and_then(Value::as_u64),
        Some(1)
    );

    let plan_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/auto-wire/plan").body(Body::empty())?)
        .await?;
    assert_eq!(plan_resp.status(), StatusCode::OK);
    let plan_body = body::to_bytes(plan_resp.into_body(), 1_048_576).await?;
    let plan_json: Value = serde_json::from_slice(&plan_body)?;
    assert_eq!(
        plan_json.get("plan_id").and_then(Value::as_str),
        Some(plan_id.to_string().as_str())
    );
    assert_eq!(
        plan_json.get("blueprint_id").and_then(Value::as_str),
        Some(Planner::AUTO_WIRE_BLUEPRINT_ID)
    );

    Ok(())
}

#[tokio::test]
async fn extensions_auto_wire_toggle_disables_and_triggers_reconcile() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let db_pool = database.pool.clone();
    let store = ExtensionStore::new(&db_pool);
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let plan_id = Uuid::new_v4();
    let pending_plan = Plan {
        plan_id,
        blueprint_id: Planner::AUTO_WIRE_BLUEPRINT_ID.to_string(),
        params: None,
        actions: Vec::new(),
        conflicts: Vec::new(),
    };
    store
        .create_run(&NewOrchestratorRun {
            run_id: plan_id,
            source: "auto_wire".to_string(),
            status: OrchestratorRunStatus::Pending,
            phase: Some("auto_wire".to_string()),
            plan_json: Some(serde_json::to_value(&pending_plan)?),
            error: None,
        })
        .await?;

    let disable_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/auto-wire")
                .header("content-type", "application/json")
                .body(Body::from("{\"enabled\":false}"))?,
        )
        .await?;
    assert_eq!(disable_resp.status(), StatusCode::OK);

    let canceled = store
        .get_latest_run_by_source("auto_wire", Some(OrchestratorRunStatus::Canceled))
        .await?
        .expect("auto-wire run canceled");
    assert_eq!(canceled.run_id, plan_id);

    let disabled_body = body::to_bytes(disable_resp.into_body(), 1_048_576).await?;
    let disabled_json: Value = serde_json::from_slice(&disabled_body)?;
    assert_eq!(
        disabled_json.get("pendingConflicts"),
        Some(&Value::Null),
        "pending_conflicts should be cleared after disable"
    );
    assert_eq!(
        disabled_json.get("pendingPlanId"),
        Some(&Value::Null),
        "pending_plan_id should be cleared after disable"
    );
    assert_eq!(
        disabled_json.get("pendingReason"),
        Some(&Value::Null),
        "pending_reason should be cleared after disable"
    );

    let enable_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/auto-wire")
                .header("content-type", "application/json")
                .body(Body::from("{\"enabled\":true}"))?,
        )
        .await?;
    assert_eq!(enable_resp.status(), StatusCode::OK);

    let mut reconcile_run = None;
    for _ in 0..20 {
        reconcile_run = store.get_latest_run_by_phase("reconcile").await?;
        if reconcile_run.is_some() {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    assert!(
        reconcile_run.is_some(),
        "expected reconcile run after enable"
    );

    Ok(())
}

#[tokio::test]
async fn extensions_rollback_plan_previews_availability() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(crate::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    ));

    let store = ExtensionStore::new(&db_pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: "ext.rollback".to_string(),
            name: "Rollback Module".to_string(),
            version: "1.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "ext.rollback",
                "version": "1.1.0",
                "kind": "module",
                "name": "Rollback Module",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "implementation": "sonarr"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "example/test:1"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let unavailable_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: unavailable_instance_id,
            extension_id: "ext.rollback".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;

    let unavailable_resp = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/extensions/instances/{unavailable_instance_id}/rollback"
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(unavailable_resp.status(), StatusCode::OK);
    let body = body::to_bytes(unavailable_resp.into_body(), 1_048_576).await?;
    let plan_json: Value = serde_json::from_slice(&body)?;
    let conflicts = plan_json
        .get("conflicts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        conflicts
            .iter()
            .any(|conflict| { conflict.get("code") == Some(&json!("rollback_unavailable")) }),
        "expected rollback_unavailable conflict"
    );

    let available_instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id: available_instance_id,
            extension_id: "ext.rollback".to_string(),
            instance_name: "default-2".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .update_instance_runtime_version(available_instance_id, "1.1.0", Some("1.0.0"))
        .await?;

    let available_resp = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/extensions/instances/{available_instance_id}/rollback"
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(available_resp.status(), StatusCode::OK);
    let body = body::to_bytes(available_resp.into_body(), 1_048_576).await?;
    let plan_json: Value = serde_json::from_slice(&body)?;
    let conflicts = plan_json
        .get("conflicts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(conflicts.is_empty(), "unexpected conflicts: {conflicts:?}");
    let actions = plan_json
        .get("actions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let available_instance_str = available_instance_id.to_string();
    assert!(
        actions.iter().any(|action| {
            action.get("type") == Some(&json!("rollback_runtime"))
                && action.get("instance_id").and_then(Value::as_str)
                    == Some(available_instance_str.as_str())
        }),
        "expected rollback_runtime action"
    );

    Ok(())
}

#[tokio::test]
async fn extensions_delete_instance_cleans_secrets() -> Result<()> {
    let (app, instance_id) =
        setup_extension_instance("elixir.test.instance_delete", "Instance Delete", None, true)
            .await?;

    let instance_secret = json!({
        "scope": "instance",
        "scopeId": instance_id.to_string(),
        "key": "api_key",
        "value": "instance-secret"
    });
    let secret_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/secrets")
                .header("content-type", "application/json")
                .body(Body::from(instance_secret.to_string()))?,
        )
        .await?;
    assert_eq!(secret_resp.status(), StatusCode::OK);

    let global_secret = json!({
        "scope": "global",
        "key": "global_key",
        "value": "global-secret"
    });
    let global_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/secrets")
                .header("content-type", "application/json")
                .body(Body::from(global_secret.to_string()))?,
        )
        .await?;
    assert_eq!(global_resp.status(), StatusCode::OK);

    let delete_resp = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/extensions/instances/{instance_id}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let list_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/extensions/secrets?scope=instance&scopeId={instance_id}"
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = body::to_bytes(list_resp.into_body(), 1_048_576).await?;
    let list_json: Value = serde_json::from_slice(&list_body)?;
    assert_eq!(
        list_json.as_array().map(|items| items.len()),
        Some(0),
        "instance secrets should be deleted"
    );

    let global_list_resp = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/secrets?scope=global").body(Body::empty())?)
        .await?;
    assert_eq!(global_list_resp.status(), StatusCode::OK);
    let global_body = body::to_bytes(global_list_resp.into_body(), 1_048_576).await?;
    let global_json: Value = serde_json::from_slice(&global_body)?;
    assert!(
        global_json.as_array().map(|items| {
            items.iter().any(|item| {
                item.get("key")
                    .and_then(Value::as_str)
                    .map(|key| key == "global_key")
                    .unwrap_or(false)
            })
        }) == Some(true),
        "global secret should remain"
    );

    Ok(())
}

#[tokio::test]
async fn extensions_delete_instance_is_idempotent() -> Result<()> {
    let (app, instance_id) = setup_extension_instance(
        "elixir.test.instance_delete_idempotent",
        "Instance Delete Idempotent",
        None,
        true,
    )
    .await?;

    let delete_resp = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/extensions/instances/{instance_id}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(delete_resp.status(), StatusCode::OK);

    let delete_again = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/extensions/instances/{instance_id}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(delete_again.status(), StatusCode::NOT_FOUND);

    Ok(())
}
