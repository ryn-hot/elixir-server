use anyhow::Result;
use argon2::{
    Argon2,
    password_hash::{PasswordHasher, SaltString},
};
use axum::{
    body::{self, Body},
    http::{Request, StatusCode},
};
use rand::rngs::OsRng;
use serde_json::Value;
use tempfile::tempdir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::{
    auth::AuthService,
    config::{
        AuthConfig, DatabaseConfig, LibraryConfig, RunEnvironment, ServerConfig, Settings,
        TelemetryConfig,
    },
    db::Database,
    extensions::ExtensionManager,
    extensions::ExternalIds,
    extensions::FileDescriptor,
    extensions::MediaFileCandidate,
    extensions::MediaIdentity,
    http::router,
    library::run_full_scan,
    metadata::MetadataService,
    state::AppState,
};

fn test_settings_with_db() -> Settings {
    Settings {
        environment: RunEnvironment::Development,
        server: ServerConfig::default(),
        database: DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        },
        library: LibraryConfig::default(),
        auth: AuthConfig::default(),
        telemetry: TelemetryConfig::default(),
        metadata: crate::config::MetadataConfig::default(),
        playback: crate::config::PlaybackConfig::default(),
        network: crate::config::NetworkConfig::default(),
    }
}

#[tokio::test]
async fn health_and_settings_endpoints_work() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;

    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        MetadataService::new(crate::config::MetadataConfig::default())?,
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
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
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
    let app = router(AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
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
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
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
        .get("servers")
        .and_then(Value::as_array)
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
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
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
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
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
    let state = AppState::new(settings, database, auth_service, extensions, metadata);
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
async fn play_endpoint_returns_stream_url() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "play-secret-key".to_string();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let extensions = ExtensionManager::new();
    let metadata = crate::metadata::MetadataService::new(crate::config::MetadataConfig::default())?;
    let state = AppState::new(settings, database, auth_service, extensions, metadata);
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

    let (item_id,): (String,) = sqlx::query_as("SELECT id FROM media_items LIMIT 1")
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
    let state = AppState::new(settings, database, auth_service, extensions, metadata);
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

    let media_item_id: String = sqlx::query_scalar("SELECT id FROM media_items LIMIT 1")
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
    let state = AppState::new(settings, database, auth_service, extensions, metadata);
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
    let media_item_id: String = sqlx::query_scalar("SELECT id FROM media_items LIMIT 1")
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
