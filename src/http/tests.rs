use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use argon2::{
    Argon2,
    password_hash::{PasswordHasher, SaltString},
};
use axum::{
    Json,
    body::{self, Body},
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
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
    auth::AuthService,
    artwork::ArtworkService,
    config::{
        AuthConfig, ClassifierConfig, DatabaseConfig, LibraryConfig, RunEnvironment, SecretsConfig,
        ServerConfig, Settings, TelemetryConfig,
    },
    db::Database,
    db::models::{
        ExtensionKind, ExtensionTrustLevel, OrchestratorRunStatus, ProviderHealthState, SecretScope,
        SlotCardinality,
    },
    extensions::ExtensionManager,
    extensions::ExternalIds,
    extensions::FileDescriptor,
    extensions::MediaFileCandidate,
    extensions::MediaIdentity,
    extensions::package::compute_sha256,
    extensions::store::{
        ExtensionStore, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
        NewOrchestratorRun, NewProvider, NewSecret,
    },
    http::router,
    library::LinkerService,
    library::normalize_override_key,
    library::run_full_scan,
    metadata::MetadataService,
    orchestrator::planner::{Plan, Planner},
    state::AppState,
    secrets::SecretsManager,
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
    let servers = list_json
        .as_array()
        .cloned()
        .unwrap_or_default();
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
        .oneshot(Request::get("/api/v1/discovery/search?q=test").body(Body::empty())?)
        .await?;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
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

    let media_file_id: String = sqlx::query_scalar(
        "SELECT id FROM media_files WHERE path = ? LIMIT 1",
    )
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
            Request::post(format!(
                "/api/v1/library/review/queue/{review_id}/apply"
            ))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(apply_body.to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let imdb_id: Option<String> = sqlx::query_scalar(
        "SELECT external_imdb FROM movies LIMIT 1",
    )
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(imdb_id.as_deref(), Some("tt1234567"));

    let status: String = sqlx::query_scalar(
        "SELECT status FROM review_queue WHERE id = ? LIMIT 1",
    )
    .bind(&review_id)
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(status, "applied");

    let override_key: String = sqlx::query_scalar(
        "SELECT normalized_key FROM classifier_overrides WHERE library_type = 'movie' LIMIT 1",
    )
    .fetch_one(&state.db_pool)
    .await?;
    let expected_key =
        normalize_override_key("Review.Movie.2024").expect("normalized key");
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
        .oneshot(
            Request::post("/api/v1/extensions/registries/refresh")
                .body(Body::empty())?,
        )
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
        .oneshot(
            Request::post("/api/v1/extensions/registries/refresh")
                .body(Body::empty())?,
        )
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
        .oneshot(
            Request::post("/api/v1/extensions/registries/refresh")
                .body(Body::empty())?,
        )
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
        .oneshot(Request::delete(format!("/api/v1/extensions/secrets/{secret_id}")).body(Body::empty())?)
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
    let desired = store.list_desired_blueprints(None).await?;
    let desired_entry = desired
        .iter()
        .find(|item| item.desired_id == plan_uuid)
        .expect("desired blueprint");
    assert_eq!(desired_entry.blueprint_extension_id, "blueprint.conflict");
    assert!(!desired_entry.applied, "expected desired blueprint to be pending");

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
        !resolved_conflicts.iter().any(|conflict| {
            conflict.get("code") == Some(&json!("slot_conflict"))
        }),
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
        applied_items[0]
            .get("desired_id")
            .and_then(Value::as_str),
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
        pending_items[0]
            .get("desired_id")
            .and_then(Value::as_str),
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
    assert_eq!(
        delete_json.get("deleted").and_then(Value::as_u64),
        Some(1)
    );

    let remaining_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/desired-blueprints")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(remaining_resp.status(), StatusCode::OK);
    let remaining_body = body::to_bytes(remaining_resp.into_body(), 1_048_576).await?;
    let remaining_items: Vec<Value> = serde_json::from_slice(&remaining_body)?;
    assert_eq!(remaining_items.len(), 1);
    assert_eq!(
        remaining_items[0]
            .get("desired_id")
            .and_then(Value::as_str),
        Some(applied_id_str.as_str())
    );

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
        .oneshot(
            Request::get("/api/v1/extensions/reconcile/latest")
                .body(Body::empty())?,
        )
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
        .oneshot(
            Request::post("/api/v1/extensions/reconcile/now")
                .body(Body::empty())?,
        )
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
        .oneshot(
            Request::get("/api/v1/extensions/reconcile/latest")
                .body(Body::empty())?,
        )
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
        .oneshot(
            Request::get("/api/v1/extensions/auto-wire")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_body = body::to_bytes(status_resp.into_body(), 1_048_576).await?;
    let status_json: Value = serde_json::from_slice(&status_body)?;
    assert_eq!(status_json.get("enabled").and_then(Value::as_bool), Some(true));
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
        .oneshot(
            Request::get("/api/v1/extensions/auto-wire")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_body = body::to_bytes(status_resp.into_body(), 1_048_576).await?;
    let status_json: Value = serde_json::from_slice(&status_body)?;
    assert_eq!(status_json.get("enabled").and_then(Value::as_bool), Some(true));
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
        .oneshot(
            Request::get("/api/v1/extensions/auto-wire/plan")
                .body(Body::empty())?,
        )
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
    assert!(reconcile_run.is_some(), "expected reconcile run after enable");

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
        conflicts.iter().any(|conflict| {
            conflict.get("code") == Some(&json!("rollback_unavailable"))
        }),
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
    let (app, instance_id) = setup_extension_instance(
        "elixir.test.instance_delete",
        "Instance Delete",
        None,
        true,
    )
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
