use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Instant, UNIX_EPOCH};

use anyhow::{Context, Result};
use argon2::{
    Argon2,
    password_hash::{PasswordHasher, SaltString},
};
use axum::{
    Json, Router,
    body::{self, Body},
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, Request, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
};
use base64::{Engine as _, engine::general_purpose};
use ed25519_dalek::{Signer, SigningKey};
use rand::{RngCore, rngs::OsRng};
use serde_json::{Value, json};
use sqlx::Row;
use tempfile::{TempDir, tempdir};
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
        AuthConfig, ClassifierConfig, DatabaseConfig, LibraryConfig, MediaInteractionsConfig,
        PlaybackRemotePolicyOverride, RunEnvironment, SecretsConfig, ServerConfig, Settings,
        TelemetryConfig,
    },
    db::Database,
    db::models::{
        BindingStatus, ExtensionKind, ExtensionTrustLevel, MediaType, OrchestratorRunStatus,
        ProviderHealthState, ProviderReadinessPhase, SecretScope, SlotCardinality,
    },
    extensions::ExtensionManager,
    extensions::ExternalIds,
    extensions::FileDescriptor,
    extensions::MediaFileCandidate,
    extensions::MediaIdentity,
    extensions::package::compute_sha256,
    extensions::store::{
        ExtensionStore, NewBinding, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
        NewManagedEpisodeTombstone, NewManagedIngestIntent, NewManagedLibraryProvenance,
        NewManagedMediaTombstone, NewMediaOwnership, NewOrchestratorRun, NewProvider, NewSecret,
    },
    http::router,
    library::LinkerService,
    library::normalize_override_key,
    library::run_full_scan,
    metadata::MetadataService,
    orchestrator::model::ProviderEndpoint,
    orchestrator::plan_validation::missing_required_secrets_for_plan,
    orchestrator::planner::{DriverPatchSpec, PlanAction, ProviderSpec},
    playback::plan::{Delivery, PlaybackMode},
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
        media_interactions: MediaInteractionsConfig::default(),
        live: crate::live::config::LiveConfig::default(),
        network: crate::config::NetworkConfig::default(),
    }
}

async fn seed_test_media_probe(
    pool: &sqlx::AnyPool,
    media_file_id: &str,
    path: &str,
    container: &str,
    video_codec: &str,
    audio_codec: &str,
    width: i32,
    height: i32,
    bitrate_bps: i64,
) -> Result<()> {
    let metadata = tokio::fs::metadata(path).await?;
    let source_size_bytes = metadata.len() as i64;
    let source_mtime_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64);
    let probe_version = crate::playback::probe::MEDIA_CAPABILITIES_PROBE_VERSION;
    let normalized = json!({
        "probe_version": probe_version,
        "ffprobe_version": "test-fixture",
        "probe_status": "ok",
        "probe_error": null,
        "probed_at": null,
        "path": path,
        "container": {
            "format_names": [container],
            "canonical": container,
            "major_brand": null,
            "compatible_brands": []
        },
        "duration_seconds": 60.0,
        "size_bytes": source_size_bytes,
        "overall_bitrate_bps": bitrate_bps,
        "start_time_seconds": 0.0,
        "video_streams": [{
            "index": 0,
            "codec": video_codec,
            "profile": "High",
            "level": 41,
            "pixel_format": "yuv420p",
            "width": width,
            "height": height,
            "frame_rate": 24.0,
            "bit_depth": 8,
            "bitrate_bps": bitrate_bps,
            "color_primaries": null,
            "color_transfer": null,
            "color_matrix": null,
            "hdr10": false,
            "hdr10_plus": false,
            "dolby_vision": false,
            "mastering_metadata": false,
            "content_light_metadata": false,
            "dolby_vision_profile": null,
            "dolby_vision_has_hdr10_fallback": false,
            "is_default": true,
            "is_forced": false
        }],
        "audio_streams": [{
            "index": 1,
            "codec": audio_codec,
            "profile": null,
            "channels": 2,
            "channel_layout": "stereo",
            "sample_rate": 48000,
            "bitrate_bps": 192000,
            "language": "eng",
            "title": null,
            "is_default": true,
            "is_forced": false
        }],
        "subtitle_streams": [],
        "chapters_present": false,
        "attachments_present": false
    });

    sqlx::query("DELETE FROM media_file_probes WHERE media_file_id = $1")
        .bind(media_file_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO media_file_probes
            (media_file_id, probe_version, ffprobe_version, probe_status, probed_at,
             source_mtime_ms, source_size_bytes, normalized_json, raw_json, error,
             created_at, updated_at)
         VALUES ($1, $2, 'test-fixture', 'ok', CURRENT_TIMESTAMP, $3, $4, $5, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(media_file_id)
    .bind(probe_version)
    .bind(source_mtime_ms)
    .bind(source_size_bytes)
    .bind(normalized.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

struct PlaybackRouteFixture {
    app: Router,
    state: AppState,
    token: String,
    user_id: String,
    item_id: String,
    media_file_id: String,
    _dir: TempDir,
}

async fn setup_playback_route_fixture(
    settings: Settings,
    file_name: &str,
    container: &str,
    video_codec: &str,
    audio_codec: &str,
    width: i32,
    height: i32,
    bitrate_bps: i64,
) -> Result<PlaybackRouteFixture> {
    setup_playback_route_fixture_with_user(
        settings,
        Uuid::new_v4(),
        file_name,
        container,
        video_codec,
        audio_codec,
        width,
        height,
        bitrate_bps,
    )
    .await
}

async fn setup_playback_route_fixture_with_user(
    settings: Settings,
    user_id: Uuid,
    file_name: &str,
    container: &str,
    video_codec: &str,
    audio_codec: &str,
    width: i32,
    height: i32,
    bitrate_bps: i64,
) -> Result<PlaybackRouteFixture> {
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
    let file_path = dir.path().join(file_name);
    tokio::fs::write(&file_path, b"0123456789").await?;
    let title = format!("Phase13 {file_name}");
    let candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: crate::db::models::MediaType::Movie,
            external_ids: ExternalIds::default(),
            title: title.clone(),
            year: Some(2026),
            season: None,
            episode: None,
        },
        files: vec![FileDescriptor {
            path: file_path.to_string_lossy().to_string(),
            size_bytes: Some(10),
            hash: None,
            container: Some(container.to_string()),
            video_codec: Some(video_codec.to_string()),
            audio_codec: Some(audio_codec.to_string()),
        }],
        extension_metadata: Default::default(),
        source_config_id: None,
    };
    run_full_scan(&state.db_pool, vec![candidate], false).await?;

    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind(format!("phase13-{}@example.com", Uuid::new_v4()))
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let item_id: String =
        sqlx::query_scalar("SELECT id FROM movies WHERE title = $1 AND year = 2026 LIMIT 1")
            .bind(&title)
            .fetch_one(&state.db_pool)
            .await?;
    let media_file_id: String =
        sqlx::query_scalar("SELECT id FROM media_files WHERE path = $1 LIMIT 1")
            .bind(file_path.to_string_lossy().to_string())
            .fetch_one(&state.db_pool)
            .await?;
    seed_test_media_probe(
        &state.db_pool,
        &media_file_id,
        &file_path.to_string_lossy(),
        container,
        video_codec,
        audio_codec,
        width,
        height,
        bitrate_bps,
    )
    .await?;

    Ok(PlaybackRouteFixture {
        app,
        state,
        token,
        user_id: user_id.to_string(),
        item_id,
        media_file_id,
        _dir: dir,
    })
}

async fn play_route_json(app: &Router, token: &str, body: Value) -> Result<Value> {
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))?,
        )
        .await?;
    let status = resp.status();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(status, StatusCode::OK, "body: {}", json);
    Ok(json)
}

async fn play_fixture_json(fixture: &PlaybackRouteFixture, body: Value) -> Result<Value> {
    play_route_json(&fixture.app, &fixture.token, body).await
}

fn timing_budget_from_env(name: &str, default_ms: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(default_ms))
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
            sections
                .iter()
                .find(|section| section.get("id").and_then(Value::as_str) == Some(section_id))
        })
        .unwrap_or_else(|| panic!("missing control-surface section '{section_id}': {payload}"))
}

fn control_section_field<'a>(section: &'a Value, field_id: &str) -> &'a Value {
    section
        .get("fields")
        .and_then(Value::as_array)
        .and_then(|fields| {
            fields
                .iter()
                .find(|field| field.get("id").and_then(Value::as_str) == Some(field_id))
        })
        .unwrap_or_else(|| panic!("missing control-surface field '{field_id}': {section}"))
}

fn control_section_entity<'a>(section: &'a Value, entity_id: &str) -> &'a Value {
    section
        .get("entities")
        .and_then(Value::as_array)
        .and_then(|entities| {
            entities
                .iter()
                .find(|entity| entity.get("id").and_then(Value::as_str) == Some(entity_id))
        })
        .unwrap_or_else(|| panic!("missing control-surface entity '{entity_id}': {section}"))
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

async fn setup_generic_control_surface_extension_with_id(
    extension_id: &str,
) -> Result<(Router, AppState, Uuid)> {
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
    let store = ExtensionStore::new(&state.db_pool);

    let manifest = json!({
        "id": extension_id,
        "version": "0.1.0",
        "kind": "module",
        "name": "Generic Control Module",
        "provides": [
            {
                "capability": "service.generic",
                "slot": "default",
                "implementation": "generic_control"
            }
        ],
        "runtime": {
            "type": "internal"
        },
        "control_surface": {
            "adapter": "generic_v1",
            "owned_settings": [
                {
                    "id": "mode",
                    "label": "Mode",
                    "type": "select",
                    "ownership": "seeded",
                    "storage": {
                        "type": "extension_setting",
                        "key": "mode"
                    },
                    "options": [
                        {
                            "value": "balanced",
                            "label": "Balanced"
                        },
                        {
                            "value": "aggressive",
                            "label": "Aggressive"
                        }
                    ]
                },
                {
                    "id": "refreshInterval",
                    "label": "Refresh interval",
                    "type": "number",
                    "ownership": "managed",
                    "storage": {
                        "type": "instance_setting",
                        "key": "refresh_interval"
                    }
                },
                {
                    "id": "apiKey",
                    "label": "API key",
                    "type": "password",
                    "secret": true,
                    "ownership": "managed",
                    "storage": {
                        "type": "instance_secret",
                        "key": "community_api_key"
                    }
                }
            ],
            "observed_state": [
                {
                    "id": "status",
                    "label": "Status"
                }
            ],
            "actions": [
                {
                    "id": "sync_now",
                    "label": "Sync now",
                    "target": "service",
                    "kind": "primary"
                }
            ],
            "native_only": [
                {
                    "id": "advanced_filters",
                    "title": "Advanced filters",
                    "description": "Managed only in the extension's native UI."
                }
            ]
        }
    });

    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: "Generic Control Module".to_string(),
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

    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: extension_id.to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({
                "refresh_interval": 30
            })),
            enabled: true,
        })
        .await?;

    let mode_key = format!("control_surface:{extension_id}:mode");
    store
        .upsert_extension_setting(&mode_key, &json!("aggressive"))
        .await?;

    let encrypted = state.secrets.encrypt("initial-secret")?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "community_api_key".to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await?;

    Ok((app, state, instance_id))
}

async fn setup_generic_control_surface_extension() -> Result<(Router, AppState, Uuid)> {
    setup_generic_control_surface_extension_with_id("elixir.modules.generic_control").await
}

async fn setup_debrid_control_surface_extension() -> Result<(Router, AppState, Uuid)> {
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
    crate::debrid::ensure_debrid_builtin(&state).await?;
    let store = ExtensionStore::new(&state.db_pool);
    let instance = store
        .list_instances(Some(crate::debrid::DEBRID_EXTENSION_ID))
        .await?
        .into_iter()
        .next()
        .context("debrid default instance should exist")?;
    let app = router(state.clone());
    Ok((app, state, instance.instance_id))
}

async fn seed_lifecycle_safety_extension_provider(
    store: &ExtensionStore<'_>,
    extension_id: &str,
    name: &str,
    capability: &str,
    implementation: &str,
) -> Result<(Uuid, Uuid)> {
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: name.to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": extension_id,
                "version": "1.0.0",
                "kind": "module",
                "name": name,
                "provides": [{
                    "capability": capability,
                    "slot": "default",
                    "implementation": implementation
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
            extension_id: extension_id.to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "fixture": true })),
            enabled: true,
        })
        .await?;

    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: capability.to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some(implementation.to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "localhost",
                "port": 1,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    Ok((instance_id, provider_id))
}

async fn start_mock_torbox_account_server() -> Result<(String, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new().route("/api/user/me", get(mock_torbox_account));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    Ok((format!("http://{address}/api"), shutdown_tx))
}

async fn mock_torbox_account(headers: HeaderMap) -> impl IntoResponse {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if authorization != "Bearer tb-token" {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "success": false,
                "detail": "Invalid API token."
            })),
        )
            .into_response();
    }
    Json(json!({
        "success": true,
        "detail": "User data retrieved successfully.",
        "data": {
            "id": 44,
            "username": "torbox-user"
        }
    }))
    .into_response()
}

async fn start_mock_all_debrid_account_server() -> Result<(String, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new().route("/v4/user", get(mock_all_debrid_account));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    Ok((format!("http://{address}/v4"), shutdown_tx))
}

async fn mock_all_debrid_account(headers: HeaderMap) -> impl IntoResponse {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if authorization != "Bearer ad-token" {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "status": "error",
                "error": {
                    "code": "AUTH_BAD_APIKEY",
                    "message": "The auth apikey is invalid"
                }
            })),
        )
            .into_response();
    }
    Json(json!({
        "status": "success",
        "data": {
            "user": {
                "id": 55,
                "username": "alldebrid-user"
            }
        }
    }))
    .into_response()
}

async fn start_mock_premiumize_account_server() -> Result<(String, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new().route("/api/account/info", get(mock_premiumize_account));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    Ok((format!("http://{address}/api"), shutdown_tx))
}

async fn mock_premiumize_account(headers: HeaderMap) -> impl IntoResponse {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if authorization != "Bearer pm-token" {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "status": "error",
                "code": "authentication_failed",
                "message": "The API token is invalid"
            })),
        )
            .into_response();
    }
    Json(json!({
        "status": "success",
        "customer_id": "pm-customer-123",
        "premium_until": 1799999999_i64,
        "limit_used": 0.23
    }))
    .into_response()
}

#[derive(Debug, Clone)]
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
    packages: BTreeMap<String, Vec<u8>>,
}

struct TestSigningIdentity {
    signing_key: SigningKey,
    publisher_key_id: String,
}

async fn build_signed_package(temp_dir: &std::path::Path) -> Result<TestPackage> {
    let extension_id = "elixir.test.signed".to_string();
    let version = "0.1.0".to_string();
    let identity = test_signing_identity();
    let manifest = signed_test_manifest(&extension_id, &version, &identity.publisher_key_id);
    build_signed_package_from_manifest(
        temp_dir,
        "signed.elx",
        manifest,
        extension_id,
        version,
        &identity,
    )
    .await
}

fn test_signing_identity() -> TestSigningIdentity {
    let mut secret = [0u8; 32];
    OsRng.fill_bytes(&mut secret);
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key();
    let publisher_key_id = format!(
        "ed25519:{}",
        general_purpose::STANDARD.encode(public_key.to_bytes())
    );
    TestSigningIdentity {
        signing_key,
        publisher_key_id,
    }
}

fn signed_test_manifest(extension_id: &str, version: &str, publisher_key_id: &str) -> String {
    format!(
        "id: {extension_id}\nversion: {version}\nkind: module\nname: \"Signed Test\"\npublisher:\n  name: \"Test Publisher\"\n  key_id: \"{publisher_key_id}\"\nprovides:\n  - capability: media.manager.tv\n    slot: default\n    implementation: \"sonarr\"\nruntime:\n  type: container\n  image: \"example/test:1\"\n"
    )
}

fn torrentio_candidate_provider_manifest(version: &str, publisher_key_id: &str) -> String {
    format!(
        r#"id: elixir.sources.torrentio_stremio
version: {version}
kind: module
name: "Torrentio-Compatible Source"
description: "External source provider that converts Stremio/Torrentio-compatible stream results into Elixir acquisition candidates."
publisher:
  name: "Elixir Community"
  key_id: "{publisher_key_id}"
trust: community
permissions:
  - runtime.manage_containers
  - network.egress
provides:
  - capability: acquisition.candidate_provider
    slot: default
    cardinality: many
    implementation: torrentio_stremio
    scope:
      media_types: ["movie", "tv", "anime"]
      actions: ["search"]
      requires_account: false
      required_fields: []
runtime:
  type: container
  image: "elixir/torrentio-candidate-provider:{version}"
  network: "elixir_net"
  service_name: "elx-torrentio-source"
  ports:
    - container: 8097
      host: 0
  env:
    - name: PORT
      value: "8097"
networking:
  service_port:
    scheme: http
    container_port: 8097
control_surface:
  adapter: generic_v1
  owned_settings:
    - id: baseUrl
      label: "Addon base URL"
      type: text
      ownership: seeded
      advanced: true
      default: "https://torrentio.strem.fun"
      storage:
        type: instance_setting
        key: baseUrl
    - id: addonPath
      label: "Addon path"
      type: text
      ownership: seeded
      advanced: true
      default: ""
      storage:
        type: instance_setting
        key: addonPath
    - id: routePolicy
      label: "Route policy"
      type: select
      ownership: seeded
      advanced: true
      default: "debrid_first"
      storage:
        type: instance_setting
        key: routePolicy
      options:
        - value: "debrid_first"
          label: "Prefer debrid"
        - value: "debrid_only"
          label: "Debrid only"
        - value: "torrent_only"
          label: "Torrent only"
    - id: allowedQualities
      label: "Allowed qualities"
      type: text
      ownership: seeded
      advanced: true
      default: ""
      storage:
        type: instance_setting
        key: allowedQualities
    - id: maxSizeGb
      label: "Max size GB"
      type: number
      ownership: seeded
      advanced: true
      storage:
        type: instance_setting
        key: maxSizeGb
    - id: requiredLanguages
      label: "Required languages"
      type: text
      ownership: seeded
      advanced: true
      default: ""
      storage:
        type: instance_setting
        key: requiredLanguages
    - id: resultLimit
      label: "Result limit"
      type: number
      ownership: seeded
      advanced: true
      default: 50
      storage:
        type: instance_setting
        key: resultLimit
    - id: timeoutMs
      label: "Source timeout ms"
      type: number
      ownership: seeded
      advanced: true
      default: 12000
      storage:
        type: instance_setting
        key: timeoutMs
    - id: retryCount
      label: "Retry count"
      type: number
      ownership: seeded
      advanced: true
      default: 1
      storage:
        type: instance_setting
        key: retryCount
    - id: retryBackoffMs
      label: "Retry backoff ms"
      type: number
      ownership: seeded
      advanced: true
      default: 300
      storage:
        type: instance_setting
        key: retryBackoffMs
    - id: minRequestIntervalMs
      label: "Minimum request interval ms"
      type: number
      ownership: seeded
      advanced: true
      default: 250
      storage:
        type: instance_setting
        key: minRequestIntervalMs
    - id: maxLookupAttempts
      label: "Max lookup attempts"
      type: number
      ownership: seeded
      advanced: true
      default: 6
      storage:
        type: instance_setting
        key: maxLookupAttempts
    - id: releaseDelaySeconds
      label: "Release delay seconds"
      type: number
      ownership: seeded
      advanced: true
      default: 0
      storage:
        type: instance_setting
        key: releaseDelaySeconds
"#
    )
}

async fn build_signed_package_from_manifest(
    temp_dir: &std::path::Path,
    file_name: &str,
    manifest: String,
    extension_id: String,
    version: String,
    identity: &TestSigningIdentity,
) -> Result<TestPackage> {
    let package_path = temp_dir.join(file_name);
    let file = File::create(&package_path)?;
    let mut zip = ZipWriter::new(file);
    let options = FileOptions::<()>::default();
    zip.start_file("manifest.yaml", options)?;
    zip.write_all(manifest.as_bytes())?;
    zip.start_file("README.txt", options)?;
    zip.write_all(b"signed package test")?;
    zip.finish()?;

    let hash = compute_sha256(&package_path).await?;
    let signature = identity.signing_key.sign(hash.as_bytes());
    let signature = general_purpose::STANDARD.encode(signature.to_bytes());

    Ok(TestPackage {
        path: package_path,
        hash,
        signature,
        publisher_key_id: identity.publisher_key_id.clone(),
        extension_id,
        version,
    })
}

#[tokio::test]
async fn extension_control_surface_renders_generic_manifest_contract_sections() -> Result<()> {
    let (app, _state, _instance_id) = setup_generic_control_surface_extension().await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.generic_control/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    let seeded_section = control_surface_section(&payload, "ownedSettingsSeeded");
    assert_eq!(
        seeded_section
            .get("policy")
            .and_then(|value| value.get("mode"))
            .and_then(Value::as_str),
        Some("seeded")
    );
    assert_eq!(
        seeded_section
            .get("fields")
            .and_then(Value::as_array)
            .and_then(|fields| fields.first())
            .and_then(|field| field.get("id"))
            .and_then(Value::as_str),
        Some("mode")
    );

    let managed_section = control_surface_section(&payload, "ownedSettingsManaged");
    assert_eq!(
        managed_section
            .get("policy")
            .and_then(|value| value.get("mode"))
            .and_then(Value::as_str),
        Some("managed")
    );
    let managed_fields = managed_section
        .get("fields")
        .and_then(Value::as_array)
        .expect("managed fields");
    assert!(
        managed_fields
            .iter()
            .any(
                |field| field.get("id").and_then(Value::as_str) == Some("refreshInterval")
                    && field.get("value").and_then(Value::as_i64) == Some(30)
            )
    );
    assert!(
        managed_fields
            .iter()
            .any(
                |field| field.get("id").and_then(Value::as_str) == Some("apiKey")
                    && field.get("secret").and_then(Value::as_bool) == Some(true)
                    && field.get("value").and_then(Value::as_str) == Some("saved")
            )
    );

    let native_only = control_surface_section(&payload, "nativeOnly");
    assert_eq!(
        native_only
            .get("notices")
            .and_then(Value::as_array)
            .and_then(|notices| notices.first())
            .and_then(|notice| notice.get("code"))
            .and_then(Value::as_str),
        Some("native_only")
    );

    let runtime_bridge = control_surface_section(&payload, "runtimeBridge");
    assert_eq!(
        runtime_bridge
            .get("policy")
            .and_then(|value| value.get("mode"))
            .and_then(Value::as_str),
        Some("observed")
    );
    assert!(
        runtime_bridge
            .get("notices")
            .and_then(Value::as_array)
            .map(|notices| !notices.is_empty())
            .unwrap_or(false)
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_updates_generic_manifest_owned_settings() -> Result<()> {
    let (app, state, instance_id) = setup_generic_control_surface_extension().await?;

    let response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/extensions/elixir.modules.generic_control/control-surface")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "values": {
                            "mode": "balanced",
                            "refreshInterval": 45,
                            "apiKey": "updated-secret"
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let managed_section = control_surface_section(&payload, "ownedSettingsManaged");
    let managed_fields = managed_section
        .get("fields")
        .and_then(Value::as_array)
        .expect("managed fields");
    assert!(
        managed_fields
            .iter()
            .any(
                |field| field.get("id").and_then(Value::as_str) == Some("refreshInterval")
                    && field.get("value").and_then(Value::as_i64) == Some(45)
            )
    );
    assert!(
        managed_fields
            .iter()
            .any(
                |field| field.get("id").and_then(Value::as_str) == Some("apiKey")
                    && field.get("value").and_then(Value::as_str) == Some("saved")
            )
    );

    let seeded_section = control_surface_section(&payload, "ownedSettingsSeeded");
    assert_eq!(
        seeded_section
            .get("fields")
            .and_then(Value::as_array)
            .and_then(|fields| fields.first())
            .and_then(|field| field.get("value"))
            .and_then(Value::as_str),
        Some("balanced")
    );

    let store = ExtensionStore::new(&state.db_pool);
    assert_eq!(
        store
            .get_extension_setting("control_surface:elixir.modules.generic_control:mode")
            .await?,
        Some(json!("balanced"))
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("instance should exist");
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|value| value.get("refresh_interval"))
            .and_then(Value::as_i64),
        Some(45)
    );

    let secret = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            "community_api_key",
        )
        .await?
        .expect("secret should exist");
    assert_eq!(
        state.secrets.decrypt(&secret.value_encrypted)?,
        "updated-secret".to_string()
    );

    Ok(())
}

#[tokio::test]
async fn debrid_control_surface_redacts_tokens_and_lists_all_services() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;
    let store = ExtensionStore::new(&state.db_pool);
    for (key, value) in [
        (
            crate::debrid::DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
            "rd-secret",
        ),
        (crate::debrid::DEBRID_TORBOX_TOKEN_SECRET_KEY, "tb-secret"),
    ] {
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: key.to_string(),
                value_encrypted: state.secrets.encrypt(value)?,
                rotatable: false,
            })
            .await?;
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.debrid/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let serialized = serde_json::to_string(&payload)?;
    assert!(!serialized.contains("rd-secret"));
    assert!(!serialized.contains("tb-secret"));

    let accounts = control_surface_section(&payload, "debridAccounts");
    assert_eq!(
        control_section_field(accounts, "activeService")
            .get("value")
            .and_then(Value::as_str),
        Some("real_debrid")
    );
    assert_eq!(
        control_section_field(
            accounts,
            crate::debrid::DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY
        )
        .get("value")
        .and_then(Value::as_i64),
        Some(crate::debrid::DEFAULT_DEBRID_CONCURRENT_DOWNLOADS)
    );
    assert_eq!(
        control_section_field(accounts, "token.real_debrid")
            .get("value")
            .and_then(Value::as_str),
        Some("saved")
    );
    assert_eq!(
        control_section_field(accounts, "token.torbox")
            .get("value")
            .and_then(Value::as_str),
        Some("saved")
    );
    assert_eq!(
        control_section_entity(accounts, "debridAccount.torbox")
            .get("subtitle")
            .and_then(Value::as_str),
        Some("Configured")
    );
    let premiumize_entity = control_section_entity(accounts, "debridAccount.premiumize");
    assert_eq!(
        premiumize_entity.get("subtitle").and_then(Value::as_str),
        Some("Not configured")
    );
    assert!(
        premiumize_entity
            .get("actions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|action| action.get("label").and_then(Value::as_str) == Some("Add account"))
    );
    assert!(
        premiumize_entity
            .get("actions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|action| {
                action.get("openUrl").and_then(Value::as_str)
                    == Some(crate::debrid::DebridServiceKind::Premiumize.docs_url())
            })
    );
    Ok(())
}

#[tokio::test]
async fn debrid_control_surface_legacy_extension_id_resolves_after_migration() -> Result<()> {
    let (app, _state, _instance_id) = setup_debrid_control_surface_extension().await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.real_debrid/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("extensionId").and_then(Value::as_str),
        Some(crate::debrid::DEBRID_EXTENSION_ID)
    );
    assert_eq!(
        control_section_field(
            control_surface_section(&payload, "debridAccounts"),
            "activeService"
        )
        .get("value")
        .and_then(Value::as_str),
        Some(crate::debrid::REAL_DEBRID_IMPLEMENTATION)
    );
    Ok(())
}

#[tokio::test]
async fn debrid_control_surface_updates_multiple_account_tokens() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;

    let response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/extensions/elixir.modules.debrid/control-surface")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "values": {
                            "token.torbox": "tb-token",
                            "token.premiumize": "pm-token"
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let store = ExtensionStore::new(&state.db_pool);
    let torbox = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            crate::debrid::DEBRID_TORBOX_TOKEN_SECRET_KEY,
        )
        .await?
        .context("torbox token should be stored")?;
    let premiumize = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            crate::debrid::DEBRID_PREMIUMIZE_TOKEN_SECRET_KEY,
        )
        .await?
        .context("premiumize token should be stored")?;
    assert_eq!(state.secrets.decrypt(&torbox.value_encrypted)?, "tb-token");
    assert_eq!(
        state.secrets.decrypt(&premiumize.value_encrypted)?,
        "pm-token"
    );

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let accounts = control_surface_section(&payload, "debridAccounts");
    assert_eq!(
        control_section_field(accounts, "token.torbox")
            .get("value")
            .and_then(Value::as_str),
        Some("saved")
    );
    Ok(())
}

#[tokio::test]
async fn debrid_control_surface_updates_concurrency_cap() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;

    let response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/extensions/elixir.modules.debrid/control-surface")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "values": {
                            "maxConcurrentDownloads": 3
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let store = ExtensionStore::new(&state.db_pool);
    let instance = store
        .get_instance(instance_id)
        .await?
        .context("debrid instance should exist")?;
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.get(crate::debrid::DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY))
            .and_then(Value::as_i64),
        Some(3)
    );

    let providers = store.list_providers(Some(instance_id)).await?;
    let provider = providers
        .iter()
        .find(|provider| provider.capability == "debrid.resolver" && provider.slot_id == "default")
        .context("debrid provider should exist")?;
    assert_eq!(
        provider
            .scope_json
            .as_ref()
            .and_then(|scope| scope.pointer("/download_broker/maxConcurrentDownloads"))
            .and_then(Value::as_i64),
        Some(3)
    );

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        control_section_field(
            control_surface_section(&payload, "debridAccounts"),
            crate::debrid::DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY
        )
        .get("value")
        .and_then(Value::as_i64),
        Some(3)
    );
    Ok(())
}

#[tokio::test]
async fn debrid_control_action_switches_active_service_and_provider_registration() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;
    let store = ExtensionStore::new(&state.db_pool);
    let (torbox_base_url, torbox_shutdown) = start_mock_torbox_account_server().await?;
    let instance = store
        .get_instance(instance_id)
        .await?
        .context("debrid instance should exist")?;
    let mut config = instance
        .config_json
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    config.insert("testTorBoxApiBaseUrl".to_string(), json!(torbox_base_url));
    store
        .update_instance_config(instance_id, Some(&Value::Object(config)))
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: crate::debrid::DEBRID_TORBOX_TOKEN_SECRET_KEY.to_string(),
            value_encrypted: state.secrets.encrypt("tb-token")?,
            rotatable: false,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.debrid/control-surface/actions/set_active_debrid_service",
            )
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "params": {
                        "service": "torbox"
                    }
                })
                .to_string(),
            ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("TorBox account 'torbox-user' is reachable")
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .context("debrid instance should exist")?;
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.get("activeService"))
            .and_then(Value::as_str),
        Some("torbox")
    );
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/serviceValidation/torbox/state"))
            .and_then(Value::as_str),
        Some("healthy")
    );

    let providers = store.list_providers(Some(instance_id)).await?;
    let debrid_providers = providers
        .iter()
        .filter(|provider| {
            provider.capability == "debrid.resolver" && provider.slot_id == "default"
        })
        .collect::<Vec<_>>();
    assert_eq!(debrid_providers.len(), 1);
    let provider = debrid_providers[0];
    assert_eq!(provider.implementation.as_deref(), Some("torbox"));
    assert_eq!(
        provider
            .scope_json
            .as_ref()
            .and_then(|scope| scope.pointer("/download_broker/activeService"))
            .and_then(Value::as_str),
        Some("torbox")
    );
    let _ = torbox_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn debrid_control_action_switches_to_all_debrid_with_account_validation() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;
    let store = ExtensionStore::new(&state.db_pool);
    let (all_debrid_base_url, all_debrid_shutdown) = start_mock_all_debrid_account_server().await?;
    let instance = store
        .get_instance(instance_id)
        .await?
        .context("debrid instance should exist")?;
    let mut config = instance
        .config_json
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    config.insert(
        "testAllDebridApiBaseUrl".to_string(),
        json!(all_debrid_base_url),
    );
    store
        .update_instance_config(instance_id, Some(&Value::Object(config)))
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: crate::debrid::DEBRID_ALL_DEBRID_TOKEN_SECRET_KEY.to_string(),
            value_encrypted: state.secrets.encrypt("ad-token")?,
            rotatable: false,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.debrid/control-surface/actions/set_active_debrid_service",
            )
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "params": {
                        "service": "all_debrid"
                    }
                })
                .to_string(),
            ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("AllDebrid account 'alldebrid-user' is reachable")
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .context("debrid instance should exist")?;
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.get("activeService"))
            .and_then(Value::as_str),
        Some("all_debrid")
    );
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/serviceValidation/all_debrid/state"))
            .and_then(Value::as_str),
        Some("healthy")
    );

    let providers = store.list_providers(Some(instance_id)).await?;
    let debrid_providers = providers
        .iter()
        .filter(|provider| {
            provider.capability == "debrid.resolver" && provider.slot_id == "default"
        })
        .collect::<Vec<_>>();
    assert_eq!(debrid_providers.len(), 1);
    let provider = debrid_providers[0];
    assert_eq!(provider.implementation.as_deref(), Some("all_debrid"));
    assert_eq!(
        provider
            .scope_json
            .as_ref()
            .and_then(|scope| scope.pointer("/download_broker/activeService"))
            .and_then(Value::as_str),
        Some("all_debrid")
    );
    let _ = all_debrid_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn debrid_control_action_switches_to_premiumize_and_preserves_tokens() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;
    let store = ExtensionStore::new(&state.db_pool);
    let (premiumize_base_url, premiumize_shutdown) = start_mock_premiumize_account_server().await?;
    let instance = store
        .get_instance(instance_id)
        .await?
        .context("debrid instance should exist")?;
    let mut config = instance
        .config_json
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    config.insert(
        "testPremiumizeApiBaseUrl".to_string(),
        json!(premiumize_base_url),
    );
    store
        .update_instance_config(instance_id, Some(&Value::Object(config)))
        .await?;

    for (key, token) in [
        (
            crate::debrid::DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
            "rd-token",
        ),
        (crate::debrid::DEBRID_TORBOX_TOKEN_SECRET_KEY, "tb-token"),
        (
            crate::debrid::DEBRID_ALL_DEBRID_TOKEN_SECRET_KEY,
            "ad-token",
        ),
        (
            crate::debrid::DEBRID_PREMIUMIZE_TOKEN_SECRET_KEY,
            "pm-token",
        ),
    ] {
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: key.to_string(),
                value_encrypted: state.secrets.encrypt(token)?,
                rotatable: false,
            })
            .await?;
    }

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.debrid/control-surface/actions/set_active_debrid_service",
            )
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "params": {
                        "service": "premiumize"
                    }
                })
                .to_string(),
            ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("Premiumize account 'pm-customer-123' is reachable")
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .context("debrid instance should exist")?;
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.get("activeService"))
            .and_then(Value::as_str),
        Some("premiumize")
    );
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/serviceValidation/premiumize/state"))
            .and_then(Value::as_str),
        Some("healthy")
    );

    let providers = store.list_providers(Some(instance_id)).await?;
    let debrid_providers = providers
        .iter()
        .filter(|provider| {
            provider.capability == "debrid.resolver" && provider.slot_id == "default"
        })
        .collect::<Vec<_>>();
    assert_eq!(debrid_providers.len(), 1);
    let provider = debrid_providers[0];
    assert_eq!(provider.implementation.as_deref(), Some("premiumize"));
    assert_eq!(
        provider
            .scope_json
            .as_ref()
            .and_then(|scope| scope.pointer("/download_broker/activeService"))
            .and_then(Value::as_str),
        Some("premiumize")
    );

    for (key, token) in [
        (
            crate::debrid::DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
            "rd-token",
        ),
        (crate::debrid::DEBRID_TORBOX_TOKEN_SECRET_KEY, "tb-token"),
        (
            crate::debrid::DEBRID_ALL_DEBRID_TOKEN_SECRET_KEY,
            "ad-token",
        ),
        (
            crate::debrid::DEBRID_PREMIUMIZE_TOKEN_SECRET_KEY,
            "pm-token",
        ),
    ] {
        let secret = store
            .get_secret(SecretScope::Instance, Some(instance_id), key)
            .await?
            .with_context(|| format!("{key} should still be stored after active switch"))?;
        assert_eq!(state.secrets.decrypt(&secret.value_encrypted)?, token);
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.debrid/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let accounts = control_surface_section(&payload, "debridAccounts");
    let premiumize = control_section_entity(accounts, "debridAccount.premiumize");
    assert_eq!(
        premiumize.get("subtitle").and_then(Value::as_str),
        Some("Active")
    );
    assert!(
        premiumize
            .get("actions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|action| {
                action.get("openUrl").and_then(Value::as_str)
                    == Some(crate::debrid::DebridServiceKind::Premiumize.docs_url())
            })
    );

    let _ = premiumize_shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn debrid_disable_preserves_history_and_materialized_files() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;
    let store = ExtensionStore::new(&state.db_pool);
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: crate::debrid::DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY.to_string(),
            value_encrypted: state.secrets.encrypt("rd-token")?,
            rotatable: false,
        })
        .await?;
    let provider_id =
        crate::debrid::reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id)
            .await?;

    let materialized_root = tempdir()?;
    let materialized_path = materialized_root
        .path()
        .join("movies")
        .join("Example Movie (2024).mkv");
    tokio::fs::create_dir_all(
        materialized_path
            .parent()
            .context("materialized path should have a parent")?,
    )
    .await?;
    tokio::fs::write(&materialized_path, b"materialized media").await?;

    let source_extension_id = "elixir.sources.torrentio_lifecycle_disable";
    let (_source_instance_id, source_provider_id) = seed_lifecycle_safety_extension_provider(
        &store,
        source_extension_id,
        "Torrentio Lifecycle Disable Fixture",
        "acquisition.candidate_provider",
        "torrentio_stremio",
    )
    .await?;

    let subscription_id = Uuid::new_v4();
    let release_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_subscriptions (
            subscription_id,
            media_type,
            title,
            normalized_title,
            source_provider_id
         ) VALUES ($1, 'movie', 'Example Movie', 'example movie', $2)",
    )
    .bind(subscription_id.to_string())
    .bind(source_provider_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_releases (
            release_id,
            subscription_id,
            source_provider_id,
            source_extension_id,
            owner_id,
            media_type,
            title,
            release_title,
            source,
            source_kind,
            info_hash,
            fingerprint,
            release_kind,
            resolver_kind,
            resolver_version,
            confidence,
            score,
            selected_route_logical_id,
            selected_provider_id,
            download_id,
            remote_release_id,
            state,
            selected_candidate_json
         ) VALUES ($1, $2, $3, $4, 'default', 'movie', 'Example Movie', 'Example.Movie.2024.1080p', $5, 'magnet', $6, $7, 'single', 'tv', 'test', 'high', 100.0, 'acquisition.debrid.default', $8, $9, 'rd-release-1', 'downloaded', $10)",
    )
    .bind(release_id.to_string())
    .bind(subscription_id.to_string())
    .bind(source_provider_id.to_string())
    .bind(source_extension_id)
    .bind("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
    .bind("0123456789abcdef0123456789abcdef01234567")
    .bind("fixture-disable")
    .bind(provider_id.to_string())
    .bind(job_id.to_string())
    .bind(
        json!({
            "providerId": source_provider_id,
            "releaseTitle": "Example.Movie.2024.1080p"
        })
        .to_string(),
    )
    .execute(&state.db_pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO debrid_download_jobs (
            job_id,
            provider_id,
            instance_id,
            owner_id,
            source,
            source_kind,
            category,
            display_name,
            remote_torrent_id,
            remote_download_id,
            status,
            local_path,
            links_json,
            progress,
            downloaded_bytes,
            total_bytes,
            download_rate_bps,
            provider_implementation,
            remote_release_id,
            remote_release_status,
            provider_capabilities_json,
            selection_mode,
            selected_file_ids_json,
            skipped_file_ids_json,
            release_id
         ) VALUES ($1, $2, $3, 'default', $4, 'magnet', 'movies', 'Example Movie', 'rd-torrent-1', 'rd-download-1', 'completed', $5, '[]', 1.0, 1024, 1024, 0, 'real_debrid', 'rd-release-1', 'downloaded', $6, 'all', '[]', '[]', $7)",
    )
    .bind(job_id.to_string())
    .bind(provider_id.to_string())
    .bind(instance_id.to_string())
    .bind("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
    .bind(materialized_path.to_string_lossy().to_string())
    .bind(json!({ "service": "real_debrid" }).to_string())
    .bind(release_id.to_string())
    .execute(&state.db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/elixir.modules.real_debrid/disable")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("extension_id").and_then(Value::as_str),
        Some(crate::debrid::DEBRID_EXTENSION_ID)
    );
    assert_eq!(payload.get("enabled").and_then(Value::as_bool), Some(false));

    let routes = crate::download_broker::list_acquisition_routes(&state.db_pool, &store).await?;
    let route = routes
        .routes
        .iter()
        .find(|route| {
            route.logical_id == "acquisition.debrid.default" && route.owner_id == "default"
        })
        .context("missing debrid acquisition route")?;
    assert_eq!(route.selected_provider_id, None);
    assert_eq!(
        route.blocker.as_deref(),
        Some(crate::download_broker::DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE)
    );

    let job_count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*) FROM debrid_download_jobs WHERE job_id = $1",
    )
    .bind(job_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(job_count, 1);
    let release_candidate = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT selected_candidate_json FROM acquisition_releases WHERE release_id = $1",
    )
    .bind(release_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert!(
        release_candidate.contains("Example.Movie.2024.1080p"),
        "release provenance should remain queryable after disable"
    );
    let source_ref = sqlx::query_scalar::<sqlx::Any, Option<String>>(
        "SELECT source_provider_id FROM acquisition_releases WHERE release_id = $1",
    )
    .bind(release_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(
        source_ref.as_deref(),
        Some(source_provider_id.to_string().as_str())
    );
    assert!(tokio::fs::metadata(&materialized_path).await.is_ok());
    assert!(store.get_provider(provider_id).await?.is_some());
    assert!(store.get_provider(source_provider_id).await?.is_some());

    Ok(())
}

#[tokio::test]
async fn debrid_uninstall_and_instance_delete_are_blocked_without_cleanup() -> Result<()> {
    let (app, state, instance_id) = setup_debrid_control_surface_extension().await?;
    let store = ExtensionStore::new(&state.db_pool);
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: crate::debrid::DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY.to_string(),
            value_encrypted: state.secrets.encrypt("rd-token")?,
            rotatable: false,
        })
        .await?;
    let provider_id =
        crate::debrid::reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id)
            .await?;

    let protected_file_root = tempdir()?;
    let protected_file = protected_file_root
        .path()
        .join("completed")
        .join("Episode.mkv");
    tokio::fs::create_dir_all(
        protected_file
            .parent()
            .context("protected file should have a parent")?,
    )
    .await?;
    tokio::fs::write(&protected_file, b"completed file").await?;

    let source_extension_id = "elixir.sources.torrentio_lifecycle_uninstall";
    let (source_instance_id, source_provider_id) = seed_lifecycle_safety_extension_provider(
        &store,
        source_extension_id,
        "Torrentio Lifecycle Uninstall Fixture",
        "acquisition.candidate_provider",
        "torrentio_stremio",
    )
    .await?;
    let (qb_instance_id, qb_provider_id) = seed_lifecycle_safety_extension_provider(
        &store,
        "elixir.modules.qbittorrent",
        "qBittorrent",
        "download.torrent.client",
        "qbittorrent",
    )
    .await?;
    let (nzb_instance_id, nzb_provider_id) = seed_lifecycle_safety_extension_provider(
        &store,
        "elixir.modules.nzbget",
        "NZBGet",
        "download.usenet.client",
        "nzbget",
    )
    .await?;

    let subscription_id = Uuid::new_v4();
    let release_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_subscriptions (
            subscription_id,
            media_type,
            title,
            normalized_title,
            source_provider_id
         ) VALUES ($1, 'episode', 'Example Show', 'example show', $2)",
    )
    .bind(subscription_id.to_string())
    .bind(source_provider_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_releases (
            release_id,
            subscription_id,
            source_provider_id,
            source_extension_id,
            owner_id,
            media_type,
            title,
            release_title,
            source,
            source_kind,
            info_hash,
            fingerprint,
            release_kind,
            resolver_kind,
            resolver_version,
            confidence,
            score,
            selected_route_logical_id,
            selected_provider_id,
            download_id,
            remote_release_id,
            state,
            selected_candidate_json
         ) VALUES ($1, $2, $3, $4, 'default', 'episode', 'Example Show', 'Example.Show.S01E01.1080p', $5, 'magnet', $6, $7, 'single', 'tv', 'test', 'high', 100.0, 'acquisition.debrid.default', $8, $9, 'rd-release-2', 'downloaded', $10)",
    )
    .bind(release_id.to_string())
    .bind(subscription_id.to_string())
    .bind(source_provider_id.to_string())
    .bind(source_extension_id)
    .bind("magnet:?xt=urn:btih:fedcba9876543210fedcba9876543210fedcba98")
    .bind("fedcba9876543210fedcba9876543210fedcba98")
    .bind("fixture-uninstall")
    .bind(provider_id.to_string())
    .bind(job_id.to_string())
    .bind(
        json!({
            "providerId": source_provider_id,
            "releaseTitle": "Example.Show.S01E01.1080p"
        })
        .to_string(),
    )
    .execute(&state.db_pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO debrid_download_jobs (
            job_id,
            provider_id,
            instance_id,
            owner_id,
            source,
            source_kind,
            category,
            display_name,
            remote_torrent_id,
            remote_download_id,
            status,
            local_path,
            links_json,
            progress,
            downloaded_bytes,
            total_bytes,
            download_rate_bps,
            provider_implementation,
            remote_release_id,
            remote_release_status,
            provider_capabilities_json,
            selection_mode,
            selected_file_ids_json,
            skipped_file_ids_json,
            release_id
         ) VALUES ($1, $2, $3, 'default', $4, 'magnet', 'tv', 'Example Show S01E01', 'rd-torrent-2', 'rd-download-2', 'completed', $5, '[]', 1.0, 2048, 2048, 0, 'real_debrid', 'rd-release-2', 'downloaded', $6, 'all', '[]', '[]', $7)",
    )
    .bind(job_id.to_string())
    .bind(provider_id.to_string())
    .bind(instance_id.to_string())
    .bind("magnet:?xt=urn:btih:fedcba9876543210fedcba9876543210fedcba98")
    .bind(protected_file.to_string_lossy().to_string())
    .bind(json!({ "service": "real_debrid" }).to_string())
    .bind(release_id.to_string())
    .execute(&state.db_pool)
    .await?;

    for extension_id in [
        crate::debrid::DEBRID_EXTENSION_ID,
        crate::debrid::LEGACY_REAL_DEBRID_EXTENSION_ID,
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/extensions/{extension_id}/uninstall"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    let delete_response = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/extensions/instances/{instance_id}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(delete_response.status(), StatusCode::FORBIDDEN);

    assert!(
        store
            .get_extension(crate::debrid::DEBRID_EXTENSION_ID)
            .await?
            .is_some()
    );
    assert!(store.get_instance(instance_id).await?.is_some());
    assert!(store.get_provider(provider_id).await?.is_some());
    assert!(store.get_instance(source_instance_id).await?.is_some());
    assert!(store.get_provider(source_provider_id).await?.is_some());
    assert!(store.get_instance(qb_instance_id).await?.is_some());
    assert!(store.get_provider(qb_provider_id).await?.is_some());
    assert!(store.get_instance(nzb_instance_id).await?.is_some());
    assert!(store.get_provider(nzb_provider_id).await?.is_some());

    let job_count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*) FROM debrid_download_jobs WHERE job_id = $1",
    )
    .bind(job_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(job_count, 1);
    let release_source = sqlx::query_scalar::<sqlx::Any, Option<String>>(
        "SELECT source_provider_id FROM acquisition_releases WHERE release_id = $1",
    )
    .bind(release_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(
        release_source.as_deref(),
        Some(source_provider_id.to_string().as_str())
    );
    let release_selected = sqlx::query_scalar::<sqlx::Any, Option<String>>(
        "SELECT selected_provider_id FROM acquisition_releases WHERE release_id = $1",
    )
    .bind(release_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(
        release_selected.as_deref(),
        Some(provider_id.to_string().as_str())
    );
    assert!(tokio::fs::metadata(&protected_file).await.is_ok());

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_does_not_route_generic_manifest_by_extension_id_substring()
-> Result<()> {
    let extension_id = "elixir.modules.sonarr_helper";
    let (app, _state, _instance_id) =
        setup_generic_control_surface_extension_with_id(extension_id).await?;

    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/extensions/{extension_id}/control-surface"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let sections = payload
        .get("sections")
        .and_then(Value::as_array)
        .expect("control surface sections");

    assert!(
        sections
            .iter()
            .any(|section| section.get("id").and_then(Value::as_str) == Some("ownedSettingsSeeded"))
    );
    assert!(
        sections.iter().any(
            |section| section.get("id").and_then(Value::as_str) == Some("ownedSettingsManaged")
        )
    );
    assert!(
        !sections
            .iter()
            .any(|section| section.get("id").and_then(Value::as_str) == Some("defaults"))
    );
    assert!(!sections.iter().any(|section| {
        section.get("id").and_then(Value::as_str) == Some("downloadClientPreference")
    }));

    Ok(())
}

async fn start_registry_server(
    build_registry: impl FnOnce(SocketAddr) -> Value + Send + 'static,
    package_bytes: Vec<u8>,
) -> Result<(SocketAddr, oneshot::Sender<()>)> {
    start_registry_server_with_packages(
        build_registry,
        BTreeMap::from([("package.elx".to_string(), package_bytes)]),
    )
    .await
}

async fn start_registry_server_with_packages(
    build_registry: impl FnOnce(SocketAddr) -> Value + Send + 'static,
    packages: BTreeMap<String, Vec<u8>>,
) -> Result<(SocketAddr, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let registry_json = build_registry(addr);
    let state = Arc::new(RegistryState {
        registry_json,
        packages,
    });

    let app = Router::new()
        .route("/registry.json", get(registry_handler))
        .route("/package.elx", get(package_handler))
        .route("/:package", get(named_package_handler))
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
    package_response(&state, "package.elx")
}

async fn named_package_handler(
    AxumPath(package): AxumPath<String>,
    State(state): State<Arc<RegistryState>>,
) -> impl IntoResponse {
    package_response(&state, &package)
}

fn package_response(state: &RegistryState, package: &str) -> (StatusCode, Vec<u8>) {
    match state.packages.get(package) {
        Some(bytes) => (StatusCode::OK, bytes.clone()),
        None => (StatusCode::NOT_FOUND, Vec::new()),
    }
}

async fn extension_status_summary_item(app: &Router, extension_id: &str) -> Result<Value> {
    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    payload
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("extensionId").and_then(Value::as_str) == Some(extension_id))
                .cloned()
        })
        .ok_or_else(|| anyhow::anyhow!("status summary item '{extension_id}' not found"))
}

fn discover_test_host_ip() -> Result<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let host = socket.local_addr()?.ip().to_string();
    if host == "0.0.0.0" || matches!(host.as_str(), "127.0.0.1" | "::1") {
        anyhow::bail!("failed to discover a non-localhost test host ip");
    }
    Ok(host)
}

fn mock_arr_download_clients() -> Vec<Value> {
    vec![
        json!({
            "id": 11,
            "name": "NZBGet",
            "enable": true,
            "protocol": "usenet",
            "implementation": "Nzbget",
            "priority": 10
        }),
        json!({
            "id": 12,
            "name": "qBittorrent",
            "enable": true,
            "protocol": "torrent",
            "implementation": "QBittorrent",
            "priority": 20
        }),
    ]
}

fn mock_arr_download_client_details() -> Vec<Value> {
    vec![
        json!({
            "id": 11,
            "name": "NZBGet",
            "enable": true,
            "protocol": "usenet",
            "priority": 10,
            "implementation": "Nzbget",
            "fields": [
                { "name": "host", "value": "elx-nzbget" },
                { "name": "port", "value": 6789 },
                { "name": "username", "value": "elixir" },
                { "name": "password", "value": "********" },
                { "name": "tvCategory", "value": "tv" }
            ]
        }),
        json!({
            "id": 12,
            "name": "qBittorrent",
            "enable": true,
            "protocol": "torrent",
            "priority": 20,
            "implementation": "QBittorrent",
            "fields": [
                { "name": "host", "value": "elx-qbittorrent" },
                { "name": "port", "value": 8080 },
                { "name": "username", "value": "elixir" },
                { "name": "password", "value": "********" },
                { "name": "tvCategory", "value": "tv" }
            ]
        }),
    ]
}

fn mock_arr_tags() -> Vec<Value> {
    vec![json!({
        "label": "elixir",
        "id": 1
    })]
}

async fn start_mock_sonarr_server() -> Result<(String, SocketAddr, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let download_clients = Arc::new(mock_arr_download_clients());
    let tags = Arc::new(mock_arr_tags());

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
            "/api/v3/series/lookup",
            get(|| async {
                Json(json!([{
                    "id": 42,
                    "title": "Blocked Show",
                    "tvdbId": 321,
                    "monitored": true
                }]))
            }),
        )
        .route(
            "/api/v3/qualityprofile",
            get(|| async { Json(json!([{ "id": 1, "name": "Default" }])) }),
        )
        .route(
            "/api/v3/rootfolder",
            get(|| async { Json(json!([{ "path": "/downloads/tv" }])) }),
        )
        .route(
            "/api/v3/tag",
            get({
                let tags = Arc::clone(&tags);
                move || {
                    let tags = Arc::clone(&tags);
                    async move { Json(Value::Array(tags.as_ref().clone())) }
                }
            }),
        )
        .route(
            "/api/v3/downloadclient",
            get({
                let download_clients = Arc::clone(&download_clients);
                move || {
                    let download_clients = Arc::clone(&download_clients);
                    async move { Json(Value::Array(download_clients.as_ref().clone())) }
                }
            }),
        )
        .route(
            "/api/v3/series",
            post(|| async { Json(json!({ "id": 42 })) }),
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

#[derive(Clone, Default)]
struct MockRadarrAddState {
    created_movies: Arc<Mutex<Vec<Value>>>,
}

async fn start_mock_radarr_server()
-> Result<(String, SocketAddr, MockRadarrAddState, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let state = MockRadarrAddState::default();
    let tags = Arc::new(mock_arr_tags());

    async fn movie_lookup(Query(params): Query<HashMap<String, String>>) -> Json<Value> {
        let term = params.get("term").map(String::as_str).unwrap_or_default();
        if term == "tmdb:4232"
            || term == "imdb:tt0117571"
            || term == "Scream 1996"
            || term == "Scream"
        {
            return Json(json!([{
                "id": 4232,
                "title": "Scream",
                "tmdbId": 4232,
                "imdbId": "tt0117571",
                "year": 1996,
                "monitored": true
            }]));
        }
        Json(json!([]))
    }

    async fn create_movie(
        State(state): State<MockRadarrAddState>,
        Json(payload): Json<Value>,
    ) -> impl IntoResponse {
        if payload
            .get("addOptions")
            .and_then(Value::as_object)
            .and_then(|options| options.get("monitor"))
            .is_some()
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
                    "title": "One or more validation errors occurred.",
                    "status": 400,
                    "errors": {
                        "$.addOptions.monitor": [
                            "The JSON value could not be converted to NzbDrone.Core.Movies.MonitorTypes."
                        ]
                    }
                })),
            )
                .into_response();
        }

        state.created_movies.lock().unwrap().push(payload.clone());
        (StatusCode::OK, Json(json!({ "id": 4232 }))).into_response()
    }

    let app = Router::new()
        .route(
            "/api/v3/system/status",
            get(|| async { Json(json!({ "version": "5.9.1.9070" })) }),
        )
        .route("/api/v3/movie/lookup", get(movie_lookup))
        .route(
            "/api/v3/tag",
            get({
                let tags = Arc::clone(&tags);
                move || {
                    let tags = Arc::clone(&tags);
                    async move { Json(Value::Array(tags.as_ref().clone())) }
                }
            }),
        )
        .route(
            "/api/v3/qualityprofile",
            get(|| async { Json(json!([{ "id": 1, "name": "Default" }])) }),
        )
        .route(
            "/api/v3/rootfolder",
            get(|| async { Json(json!([{ "path": "/downloads/movies" }])) }),
        )
        .route("/api/v3/movie", post(create_movie))
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

async fn start_mock_prowlarr_indexer_server(
    indexer_names: Vec<&'static str>,
) -> Result<(String, SocketAddr, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let names = Arc::new(
        indexer_names
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
    );

    let app = Router::new().route(
        "/api/v1/indexer",
        get({
            let names = Arc::clone(&names);
            move || {
                let names = Arc::clone(&names);
                async move {
                    Json(Value::Array(
                        names.iter().map(|name| json!({ "name": name })).collect(),
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
    download_clients: Arc<Mutex<Vec<Value>>>,
    download_client_updates: Arc<Mutex<Vec<Value>>>,
}

async fn start_mock_sonarr_control_server()
-> Result<(String, SocketAddr, MockArrControlState, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let state = MockArrControlState {
        download_clients: Arc::new(Mutex::new(mock_arr_download_client_details())),
        ..MockArrControlState::default()
    };

    async fn system_status() -> Json<Value> {
        Json(json!({ "version": "4.0.0.778" }))
    }

    async fn series_list() -> Json<Value> {
        Json(json!([{ "id": 42 }, { "id": 99 }]))
    }

    async fn download_clients(State(state): State<MockArrControlState>) -> Json<Value> {
        let items = state
            .download_clients
            .lock()
            .unwrap()
            .iter()
            .map(|client| {
                json!({
                    "id": client.get("id").cloned().unwrap_or(Value::Null),
                    "name": client.get("name").cloned().unwrap_or(Value::Null),
                    "enable": client.get("enable").cloned().unwrap_or(Value::Bool(true)),
                    "protocol": client.get("protocol").cloned().unwrap_or(Value::Null),
                    "implementation": client.get("implementation").cloned().unwrap_or(Value::Null),
                    "priority": client.get("priority").cloned().unwrap_or(Value::from(1))
                })
            })
            .collect();
        Json(Value::Array(items))
    }

    async fn download_client_handler(
        State(state): State<MockArrControlState>,
        AxumPath(client_id): AxumPath<i64>,
    ) -> impl IntoResponse {
        let clients = state.download_clients.lock().unwrap();
        if let Some(client) = clients
            .iter()
            .find(|client| client.get("id").and_then(Value::as_i64) == Some(client_id))
        {
            return (StatusCode::OK, Json(client.clone())).into_response();
        }
        StatusCode::NOT_FOUND.into_response()
    }

    async fn update_download_client_handler(
        State(state): State<MockArrControlState>,
        AxumPath(client_id): AxumPath<i64>,
        Json(payload): Json<Value>,
    ) -> impl IntoResponse {
        let field_names = payload
            .get("fields")
            .and_then(Value::as_array)
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(|field| field.get("name").and_then(Value::as_str))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !field_names.contains(&"password") || !field_names.contains(&"tvCategory") {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!([
                    {
                        "propertyName": "TvCategory",
                        "errorMessage": "Category does not exist",
                        "severity": "error"
                    }
                ])),
            )
                .into_response();
        }
        state
            .download_client_updates
            .lock()
            .unwrap()
            .push(payload.clone());
        let mut clients = state.download_clients.lock().unwrap();
        if let Some(existing) = clients
            .iter_mut()
            .find(|client| client.get("id").and_then(Value::as_i64) == Some(client_id))
        {
            *existing = payload.clone();
            return (StatusCode::OK, Json(payload)).into_response();
        }
        (StatusCode::OK, Json(payload)).into_response()
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
        .route(
            "/api/v3/downloadclient/:id",
            get(download_client_handler).put(update_download_client_handler),
        )
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

#[derive(Clone)]
struct MockNzbgetControlState {
    config: Arc<Mutex<BTreeMap<String, String>>>,
    save_calls: Arc<Mutex<Vec<Value>>>,
    test_calls: Arc<Mutex<Vec<Value>>>,
    testserver_result: Arc<Mutex<Value>>,
    config_failures_remaining: Arc<Mutex<usize>>,
    config_failures_after_save: Arc<Mutex<usize>>,
    testserver_error_remaining: Arc<Mutex<Option<String>>>,
    testserver_error_after_save: Arc<Mutex<Option<String>>>,
}

async fn start_mock_nzbget_control_server(
    initial_config: Vec<(String, String)>,
    testserver_result: Value,
) -> Result<(
    String,
    SocketAddr,
    MockNzbgetControlState,
    oneshot::Sender<()>,
)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let host = discover_test_host_ip()?;
    let state = MockNzbgetControlState {
        config: Arc::new(Mutex::new(initial_config.into_iter().collect())),
        save_calls: Arc::new(Mutex::new(Vec::new())),
        test_calls: Arc::new(Mutex::new(Vec::new())),
        testserver_result: Arc::new(Mutex::new(testserver_result)),
        config_failures_remaining: Arc::new(Mutex::new(0)),
        config_failures_after_save: Arc::new(Mutex::new(0)),
        testserver_error_remaining: Arc::new(Mutex::new(None)),
        testserver_error_after_save: Arc::new(Mutex::new(None)),
    };

    async fn rpc_handler(
        State(state): State<MockNzbgetControlState>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = payload
            .get("params")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()));

        let response = match method {
            "config" => {
                let mut failures_remaining = state.config_failures_remaining.lock().unwrap();
                if *failures_remaining > 0 {
                    *failures_remaining -= 1;
                    json!({
                        "version": "1.1",
                        "result": Value::Null,
                        "error": { "message": "config temporarily unavailable" },
                        "id": 1
                    })
                } else {
                    let config = state.config.lock().unwrap();
                    json!({
                        "version": "1.1",
                        "result": Value::Array(
                            config
                                .iter()
                                .map(|(name, value)| json!({ "Name": name, "Value": value }))
                                .collect(),
                        ),
                        "error": Value::Null,
                        "id": 1
                    })
                }
            }
            "saveconfig" => {
                state.save_calls.lock().unwrap().push(params.clone());
                if let Some(updates) = params.get(0).and_then(Value::as_array) {
                    let mut config = state.config.lock().unwrap();
                    for update in updates {
                        let Some(name) = update.get("Name").and_then(Value::as_str) else {
                            continue;
                        };
                        let value = update
                            .get("Value")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        config.insert(name.to_string(), value.to_string());
                    }
                }
                let fail_after_save = *state.config_failures_after_save.lock().unwrap();
                *state.config_failures_remaining.lock().unwrap() = fail_after_save;
                let testserver_error_after_save =
                    state.testserver_error_after_save.lock().unwrap().clone();
                *state.testserver_error_remaining.lock().unwrap() = testserver_error_after_save;
                json!({
                    "version": "1.1",
                    "result": json!(true),
                    "error": Value::Null,
                    "id": 1
                })
            }
            "reload" => json!({
                "version": "1.1",
                "result": json!(true),
                "error": Value::Null,
                "id": 1
            }),
            "testserver" => {
                state.test_calls.lock().unwrap().push(params.clone());
                let testserver_error = state.testserver_error_remaining.lock().unwrap().clone();
                if let Some(message) = testserver_error {
                    json!({
                        "version": "1.1",
                        "result": Value::Null,
                        "error": { "message": message },
                        "id": 1
                    })
                } else {
                    json!({
                        "version": "1.1",
                        "result": state.testserver_result.lock().unwrap().clone(),
                        "error": Value::Null,
                        "id": 1
                    })
                }
            }
            _ => json!({
                "version": "1.1",
                "result": Value::Null,
                "error": Value::Null,
                "id": 1
            }),
        };
        Json(response)
    }

    let app = Router::new()
        .route("/jsonrpc", post(rpc_handler))
        .with_state(state.clone());

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    sleep(Duration::from_millis(25)).await;

    Ok((host, addr, state, shutdown_tx))
}

async fn seed_nzbget_control_extension(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    host: String,
    port: u16,
) -> Result<Uuid> {
    let instance_id = Uuid::new_v4();
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.nzbget".to_string(),
            name: "NZBGet".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.nzbget",
                "version": "1.0.0",
                "kind": "module",
                "name": "NZBGet",
                "provides": [{
                    "capability": "downloader.nzb",
                    "slot": "default",
                    "implementation": "nzbget"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/nzbget:latest"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: "elixir.modules.nzbget".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id,
            capability: "downloader.nzb".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("nzbget".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": host,
                "port": port,
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget_username".to_string(),
            value_encrypted: secrets.encrypt("service-user")?,
            rotatable: true,
        })
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget_password".to_string(),
            value_encrypted: secrets.encrypt("service-pass")?,
            rotatable: true,
        })
        .await?;

    Ok(instance_id)
}

async fn setup_download_broker_test_app() -> Result<(Router, sqlx::AnyPool, String)> {
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
    let token = state.auth_service.issue_access_token(Uuid::new_v4())?.token;
    let app = router(state);
    Ok((app, db_pool, token))
}

async fn seed_download_broker_provider(
    store: &ExtensionStore<'_>,
    extension_id: &str,
    capability: &str,
    implementation: &str,
    host: &str,
    provider_kind: Option<&str>,
    health_state: ProviderHealthState,
) -> Result<Uuid> {
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: extension_id.to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({ "id": extension_id, "version": "1.0.0" }),
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
        host.to_string(),
        if capability == "downloader.nzb" {
            6789
        } else {
            8080
        },
        None,
        Some("elixir_net".to_string()),
    )?;
    let scope_json = provider_kind.map(|kind| {
        json!({
            "download_broker": {
                "provider_kind": kind
            }
        })
    });
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: capability.to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some(implementation.to_string()),
            scope_json,
            endpoint_json: Some(serde_json::to_value(endpoint)?),
            health_state,
        })
        .await?;
    Ok(provider_id)
}

async fn seed_acquisition_candidate_provider(
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> Result<Uuid> {
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: "Test Candidate Source".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": extension_id,
                "version": "1.0.0",
                "kind": "module",
                "name": "Test Candidate Source",
                "provides": [
                    {
                        "capability": "acquisition.candidate_provider",
                        "slot": "default",
                        "cardinality": "many",
                        "implementation": "test_candidate_provider",
                        "scope": {
                            "media_types": ["movie", "tv", "anime"],
                            "actions": ["search"]
                        }
                    }
                ],
                "requires": {
                    "downloads": [
                        { "kind": "debrid", "mode": "broker", "optional": true },
                        { "kind": "torrent", "mode": "broker", "optional": true }
                    ]
                },
                "runtime": {
                    "type": "container",
                    "image": "test/candidate-source:1.0.0"
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
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "acquisition.candidate_provider".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("test_candidate_provider".to_string()),
            scope_json: Some(json!({
                "media_types": ["movie", "tv", "anime"],
                "actions": ["search"]
            })),
            endpoint_json: Some(serde_json::to_value(ProviderEndpoint::new(
                "http".to_string(),
                "candidate-source".to_string(),
                8097,
                None,
                Some("elixir_net".to_string()),
            )?)?),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    Ok(provider_id)
}

async fn start_scope_preview_tvdb_fixture() -> Result<(String, tokio::task::JoinHandle<()>)> {
    async fn login() -> impl IntoResponse {
        Json(json!({ "data": { "token": "scope-preview-token" } }))
    }

    async fn series_extended(AxumPath(series_id): AxumPath<String>) -> impl IntoResponse {
        if series_id != "321" {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "unknown series" })),
            );
        }
        (
            StatusCode::OK,
            Json(json!({
                "data": {
                    "seasons": [
                        { "number": 0, "type": { "type": "specials" } },
                        { "number": 1, "type": { "type": "official" } },
                        { "number": 2, "type": { "type": "official" } }
                    ]
                }
            })),
        )
    }

    async fn season_episodes(
        AxumPath(series_id): AxumPath<String>,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if series_id != "321" {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "unknown series" })),
            );
        }
        let season = query
            .get("season")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or_default();
        let episodes = match season {
            1 => json!([
                {
                    "id": 101,
                    "seasonNumber": 1,
                    "number": 1,
                    "absoluteNumber": 1,
                    "name": "Chapter I",
                    "overview": "A richer existing library row must survive preview.",
                    "image": "https://example.invalid/s1e1.jpg"
                },
                {
                    "id": 102,
                    "seasonNumber": 1,
                    "number": 2,
                    "absoluteNumber": 2,
                    "name": "Chapter II",
                    "image": "https://example.invalid/s1e2.jpg"
                }
            ]),
            2 => json!([
                {
                    "id": 201,
                    "seasonNumber": 2,
                    "number": 1,
                    "absoluteNumber": 3,
                    "name": "Chapter III",
                    "image": "https://example.invalid/s2e1.jpg"
                }
            ]),
            _ => json!([]),
        };
        (
            StatusCode::OK,
            Json(json!({ "data": { "episodes": episodes } })),
        )
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let app = Router::new()
        .route("/login", post(login))
        .route("/series/:series_id/extended", get(series_extended))
        .route("/series/:series_id/episodes/default", get(season_episodes));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("scope preview tvdb fixture");
    });
    Ok((format!("http://{addr}"), handle))
}

async fn find_media_scope_test_app(
    auth_secret: &str,
    email: &str,
    tvdb_base_url: String,
    tvdb_api_key: Option<&str>,
    extension_id: &str,
) -> Result<(AppState, Router, String, Uuid)> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = auth_secret.to_string();
    settings.classifier.tvdb_base_url = tvdb_base_url;
    settings.classifier.tvdb_api_key = tvdb_api_key.map(str::to_string);
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
    let source_provider_id = seed_acquisition_candidate_provider(&store, extension_id).await?;

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind(email)
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    Ok((state, app, token, source_provider_id))
}

#[tokio::test]
async fn health_and_settings_endpoints_work() -> Result<()> {
    let settings = test_settings_with_db();
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
    assert_eq!(
        json.get("runtime")
            .and_then(|runtime| runtime.get("status"))
            .and_then(Value::as_str),
        Some("healthy")
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
    let rollout_gates = json
        .pointer("/playback/rollout_gates")
        .and_then(Value::as_array)
        .expect("settings should expose playback rollout gates");
    let gate = |flag: &str| {
        rollout_gates
            .iter()
            .find(|gate| gate.get("flag").and_then(Value::as_str) == Some(flag))
            .unwrap_or_else(|| panic!("missing playback rollout gate {flag}"))
    };
    let expected_flags = [
        "playback.plan_contract_enabled",
        "playback.hls_direct_stream_enabled",
        "playback.audio_transcode_enabled",
        "playback.subtitle_transcode_enabled",
        "playback.video_transcode_enabled",
        "playback.transcode_feasibility_enabled",
        "playback.adaptive_quality_enabled",
        "playback.hardware_acceleration_enabled",
        "playback.hdr_tone_mapping_enabled",
        "playback.public_corpus_required",
        "playback.client_automation_required",
    ];
    assert_eq!(rollout_gates.len(), expected_flags.len());
    for flag in expected_flags {
        assert!(gate(flag).get("raw_enabled").is_some());
    }
    let plan_contract = gate("playback.plan_contract_enabled");
    assert_eq!(
        plan_contract.get("raw_enabled").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        plan_contract
            .get("effective_enabled")
            .and_then(Value::as_bool),
        Some(true),
        "plan contract should be enabled by default"
    );
    assert_eq!(
        plan_contract
            .get("default_enabled")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        gate("playback.video_transcode_enabled")
            .get("effective_enabled")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        gate("playback.transcode_feasibility_enabled")
            .get("effective_enabled")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        gate("playback.adaptive_quality_enabled")
            .get("effective_enabled")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        gate("playback.hardware_acceleration_enabled")
            .get("effective_enabled")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        gate("playback.hdr_tone_mapping_enabled")
            .get("effective_enabled")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        gate("playback.public_corpus_required")
            .get("release_evidence_gate")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        gate("playback.client_automation_required")
            .get("release_evidence_gate")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        gate("playback.client_automation_required")
            .get("runtime_enforced")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        json.pointer("/playback/planner_policy/allow_video_transcode")
            .and_then(Value::as_bool),
        Some(true)
    );

    Ok(())
}

#[tokio::test]
async fn s10_disabled_live_settings_are_explicit_and_live_routes_are_absent() -> Result<()> {
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
    state.live.initialize().await?;
    let app = router(state);
    let settings_response = app
        .clone()
        .oneshot(Request::get("/api/v1/settings").body(Body::empty())?)
        .await?;
    assert_eq!(settings_response.status(), StatusCode::OK);
    let settings_json: Value =
        serde_json::from_slice(&body::to_bytes(settings_response.into_body(), 1_048_576).await?)?;
    assert_eq!(
        settings_json.pointer("/live/raw_enabled"),
        Some(&json!(false))
    );
    assert_eq!(
        settings_json.pointer("/live/effective_enabled"),
        Some(&json!(false))
    );
    assert_eq!(settings_json.pointer("/live/ready"), Some(&json!(false)));
    assert_eq!(
        settings_json.pointer("/live/disabled_reason"),
        Some(&json!("live_disabled"))
    );
    let features = settings_json
        .pointer("/live/features")
        .and_then(Value::as_array)
        .expect("Live feature status list");
    assert_eq!(features.len(), 12);
    assert!(features.iter().all(|feature| {
        feature["raw_enabled"] == false && feature["effective_enabled"] == false
    }));

    let live_response = app
        .oneshot(Request::get("/api/v1/live/catalogs").body(Body::empty())?)
        .await?;
    assert_eq!(live_response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn health_endpoint_reports_degraded_docker_runtime_state() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;

    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        MetadataService::new(crate::config::MetadataConfig::default())?,
        linkers,
        artwork,
        secrets,
    );
    state.orchestrator.record_docker_runtime_failure(
        "docker_runtime_unavailable",
        "Docker daemon is unavailable during startup probe.",
    );
    let app = router(state);

    let health_response = app
        .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
        .await?;

    assert_eq!(health_response.status(), StatusCode::OK);
    let body = body::to_bytes(health_response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let runtime = json
        .get("runtime")
        .unwrap_or_else(|| panic!("missing runtime health: {json}"));
    assert_eq!(
        runtime.get("status").and_then(Value::as_str),
        Some("degraded")
    );
    assert_eq!(
        runtime.get("code").and_then(Value::as_str),
        Some("docker_runtime_unavailable")
    );
    assert!(
        runtime
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("Docker daemon is unavailable")
    );
    assert_eq!(
        runtime.get("state").and_then(Value::as_str),
        Some("degraded")
    );

    let docker_runtime = json
        .get("docker_runtime")
        .unwrap_or_else(|| panic!("missing docker_runtime health alias: {json}"));
    assert_eq!(
        docker_runtime.get("state").and_then(Value::as_str),
        Some("degraded")
    );
    assert_eq!(
        docker_runtime.get("code").and_then(Value::as_str),
        Some("docker_runtime_unavailable")
    );
    assert_eq!(
        docker_runtime
            .get("last_failure")
            .and_then(|value| value.get("code"))
            .and_then(Value::as_str),
        Some("docker_runtime_unavailable")
    );
    assert_eq!(
        docker_runtime
            .get("auto_reset_budget")
            .and_then(|value| value.get("attempts_allowed"))
            .and_then(Value::as_u64),
        Some(1)
    );
    let affected = docker_runtime
        .get("affected_subsystems")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing affected_subsystems: {docker_runtime}"));
    assert!(affected.iter().any(|subsystem| {
        subsystem.get("id").and_then(Value::as_str) == Some("arr_stack")
            && subsystem.get("status").and_then(Value::as_str) == Some("blocked")
    }));

    Ok(())
}

#[tokio::test]
async fn control_plane_health_and_status_build_without_docker_probe() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;

    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        MetadataService::new(crate::config::MetadataConfig::default())?,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state);

    let health_response = app
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
        .await?;
    assert_eq!(health_response.status(), StatusCode::OK);
    let body = body::to_bytes(health_response.into_body(), 1_048_576).await?;
    let health: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        health
            .get("docker_runtime")
            .and_then(|runtime| runtime.get("state"))
            .and_then(Value::as_str),
        Some("healthy"),
        "control-plane health should be available before any Docker probe mutates runtime state: {health}"
    );

    let summary_response = app
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(summary_response.status(), StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn extension_status_summary_reports_docker_runtime_state() -> Result<()> {
    let settings = test_settings_with_db();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = test_artwork_service(&settings)?;
    let secrets = SecretsManager::from_settings(&settings)?;

    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        MetadataService::new(crate::config::MetadataConfig::default())?,
        linkers,
        artwork,
        secrets,
    );
    state.orchestrator.record_docker_runtime_failure(
        "docker_runtime_unavailable",
        "Docker daemon is unavailable during status-summary probe.",
    );
    let app = router(state);

    let response = app
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let runtime = json
        .get("dockerRuntime")
        .unwrap_or_else(|| panic!("missing docker runtime summary: {json}"));
    assert_eq!(
        runtime.get("state").and_then(Value::as_str),
        Some("degraded")
    );
    assert_eq!(
        runtime.get("code").and_then(Value::as_str),
        Some("docker_runtime_unavailable")
    );
    assert_eq!(
        runtime.get("severity").and_then(Value::as_str),
        Some("attention")
    );
    assert!(
        runtime
            .get("dependencyActionsDeferredUntil")
            .and_then(Value::as_str)
            .is_some(),
        "runtime dependency deferral should be visible: {runtime}"
    );
    assert_eq!(
        runtime
            .get("lastFailure")
            .and_then(|value| value.get("code"))
            .and_then(Value::as_str),
        Some("docker_runtime_unavailable")
    );
    assert_eq!(
        runtime
            .get("autoResetBudget")
            .and_then(|value| value.get("attemptsAllowed"))
            .and_then(Value::as_u64),
        Some(1)
    );
    let affected = runtime
        .get("affectedSubsystems")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing affectedSubsystems: {runtime}"));
    assert!(affected.iter().any(|subsystem| {
        subsystem.get("id").and_then(Value::as_str) == Some("qbittorrent")
            && subsystem.get("status").and_then(Value::as_str) == Some("blocked")
    }));

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_explains_degraded_runtime_blocker() -> Result<()> {
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
    let store = ExtensionStore::new(&db_pool);
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.runtime_blocked".to_string(),
            name: "Runtime Blocked".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.runtime_blocked",
                "version": "1.0.0",
                "kind": "module",
                "name": "Runtime Blocked",
                "provides": [
                    {
                        "capability": "indexer.proxy",
                        "slot": "default",
                        "implementation": "runtime_blocked"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "example/runtime-blocked:1"
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
    state.orchestrator.record_docker_runtime_failure(
        "docker_runtime_unavailable",
        "Docker daemon is unavailable during control-surface probe.",
    );

    let response = app
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.runtime_blocked/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let status = payload
        .get("status")
        .unwrap_or_else(|| panic!("missing control status: {payload}"));
    assert_eq!(
        status.get("summary").and_then(Value::as_str),
        Some("Status stale")
    );
    let details = status
        .get("details")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing control status details: {status}"))
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        details.contains("Runtime operations are blocked because Docker is degraded"),
        "runtime blocker should be explicit: {details}"
    );
    assert!(
        details.contains("Affected subsystems:"),
        "affected subsystems should be explicit: {details}"
    );

    Ok(())
}

#[tokio::test]
async fn network_protection_status_reports_default_external_only() -> Result<()> {
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

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/network/protection/status").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json.get("mode").and_then(Value::as_str),
        Some("external_only")
    );
    assert_eq!(json.get("state").and_then(Value::as_str), Some("unknown"));
    assert!(json.get("blocker").is_none_or(Value::is_null));
    Ok(())
}

#[tokio::test]
async fn network_protection_first_run_existing_stack_sets_external_routes() -> Result<()> {
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

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/network/protection/first-run")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"choice":"existing_stack","apply":true}"#))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(json.get("completed").and_then(Value::as_bool), Some(true));
    assert_eq!(
        json.pointer("/profile/kind").and_then(Value::as_str),
        Some("external_only")
    );
    let routes = json
        .pointer("/routes/routes")
        .and_then(Value::as_array)
        .context("routes array")?;
    for logical_id in ["downloaders.torrent.default", "downloaders.usenet.default"] {
        let route = routes
            .iter()
            .find(|route| {
                route
                    .get("logicalId")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == logical_id)
                    && route
                        .get("ownerId")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == "default")
            })
            .with_context(|| format!("missing first-run route '{logical_id}'"))?;
        assert_eq!(
            route.get("bindingKind").and_then(Value::as_str),
            Some("external")
        );
    }
    Ok(())
}

#[tokio::test]
async fn network_protection_status_blocks_missing_legacy_wireguard_secret() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.network.vpn.enabled = true;
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
    store
        .create_instance(&NewExtensionInstance {
            instance_id: Uuid::new_v4(),
            extension_id: "elixir.modules.qbittorrent".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/network/protection/status").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json.get("mode").and_then(Value::as_str),
        Some("wireguard_config")
    );
    assert_eq!(json.get("state").and_then(Value::as_str), Some("blocked"));
    assert_eq!(
        json.pointer("/blocker/code").and_then(Value::as_str),
        Some("wireguard_config_secret_missing")
    );
    assert_eq!(
        json.get("protected_apps")
            .and_then(Value::as_array)
            .and_then(|apps| apps.first())
            .and_then(Value::as_str),
        Some("qbittorrent")
    );
    Ok(())
}

#[tokio::test]
async fn download_broker_inventory_exposes_stable_paths_without_raw_endpoints() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    seed_download_broker_provider(
        &store,
        "elixir.modules.qbittorrent",
        "downloader.torrent",
        "qbittorrent",
        "svc-qbittorrent",
        None,
        ProviderHealthState::Healthy,
    )
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/download-broker/downloaders")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let downloader = json
        .get("downloaders")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .expect("broker downloader");
    assert_eq!(
        downloader.get("logicalId").and_then(Value::as_str),
        Some("downloaders.torrent.default")
    );
    assert_eq!(
        downloader
            .pointer("/endpoints/submitPath")
            .and_then(Value::as_str),
        Some("/api/v1/download-broker/downloaders.torrent.default/submit")
    );
    assert!(downloader.get("endpoint").is_none());
    assert!(downloader.get("brokerPath").is_some());
    Ok(())
}

#[tokio::test]
async fn download_broker_debrid_route_without_active_service_uses_generic_blocker() -> Result<()> {
    let (app, _db_pool, token) = setup_download_broker_test_app().await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/download-broker/routes")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let route = json
        .get("routes")
        .and_then(Value::as_array)
        .and_then(|routes| {
            routes.iter().find(|route| {
                route.get("logicalId").and_then(Value::as_str) == Some("acquisition.debrid.default")
                    && route.get("ownerId").and_then(Value::as_str) == Some("default")
            })
        })
        .context("missing default debrid route")?;
    assert_eq!(
        route.get("blocker").and_then(Value::as_str),
        Some("Active debrid service is not configured")
    );
    Ok(())
}

#[tokio::test]
async fn download_broker_route_can_select_external_provider() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    seed_download_broker_provider(
        &store,
        "elixir.modules.qbittorrent",
        "downloader.torrent",
        "qbittorrent",
        "svc-qbittorrent",
        None,
        ProviderHealthState::Healthy,
    )
    .await?;
    let external_id = seed_download_broker_provider(
        &store,
        "external.stack.qbit",
        "downloader.torrent",
        "qbittorrent",
        "external-qbit",
        Some("external"),
        ProviderHealthState::Healthy,
    )
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/download-broker/routes/downloaders.torrent.default")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(r#"{"bindingKind":"external"}"#))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json.get("bindingKind").and_then(Value::as_str),
        Some("external")
    );
    let external_id = external_id.to_string();
    assert_eq!(
        json.get("selectedProviderId").and_then(Value::as_str),
        Some(external_id.as_str())
    );
    assert!(json.get("blocker").is_none_or(Value::is_null));
    Ok(())
}

#[tokio::test]
async fn download_broker_debrid_route_exposes_native_provider() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    let provider_id = seed_download_broker_provider(
        &store,
        "elixir.modules.real_debrid",
        "debrid.resolver",
        "real_debrid",
        "api.real-debrid.com",
        Some("debrid"),
        ProviderHealthState::Healthy,
    )
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/download-broker/routes")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let route = json
        .get("routes")
        .and_then(Value::as_array)
        .and_then(|routes| {
            routes.iter().find(|route| {
                route.get("logicalId").and_then(Value::as_str) == Some("acquisition.debrid.default")
                    && route.get("ownerId").and_then(Value::as_str) == Some("default")
            })
        })
        .context("missing default debrid route")?;
    assert_eq!(
        route.get("role").and_then(Value::as_str),
        Some("debrid_resolver")
    );
    let provider_id = provider_id.to_string();
    assert_eq!(
        route.get("selectedProviderId").and_then(Value::as_str),
        Some(provider_id.as_str())
    );
    assert_eq!(
        route.get("selectedProviderKind").and_then(Value::as_str),
        Some("debrid")
    );
    assert!(route.get("blocker").is_none_or(Value::is_null));
    Ok(())
}

#[tokio::test]
async fn download_broker_debrid_route_candidates_preserve_provider_evidence() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    let providers = [
        (
            "elixir.modules.debrid.real_debrid",
            "real_debrid",
            "api.real-debrid.com",
        ),
        ("elixir.modules.debrid.torbox", "torbox", "api.torbox.app"),
        (
            "elixir.modules.debrid.all_debrid",
            "all_debrid",
            "api.alldebrid.com",
        ),
        (
            "elixir.modules.debrid.premiumize",
            "premiumize",
            "www.premiumize.me",
        ),
    ];
    for (extension_id, implementation, host) in providers {
        seed_download_broker_provider(
            &store,
            extension_id,
            "debrid.resolver",
            implementation,
            host,
            Some("debrid"),
            ProviderHealthState::Healthy,
        )
        .await?;
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/download-broker/routes")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let route = json
        .get("routes")
        .and_then(Value::as_array)
        .and_then(|routes| {
            routes.iter().find(|route| {
                route.get("logicalId").and_then(Value::as_str) == Some("acquisition.debrid.default")
                    && route.get("ownerId").and_then(Value::as_str) == Some("default")
            })
        })
        .context("missing default debrid route")?;

    assert_eq!(
        route.get("blocker").and_then(Value::as_str),
        Some("Active debrid service is not configured")
    );
    assert!(!route.to_string().contains("Real-Debrid"));

    let candidates = route
        .get("candidates")
        .and_then(Value::as_array)
        .context("debrid route candidates")?;
    assert_eq!(candidates.len(), providers.len());
    for (_extension_id, implementation, _host) in providers {
        assert!(
            candidates.iter().any(|candidate| {
                candidate.get("providerKind").and_then(Value::as_str) == Some("debrid")
                    && candidate.get("implementation").and_then(Value::as_str)
                        == Some(implementation)
                    && candidate.get("healthState").and_then(Value::as_str) == Some("healthy")
            }),
            "missing debrid provider candidate evidence for {implementation}: {candidates:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn download_broker_debrid_route_blocks_unhealthy_active_service() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    seed_download_broker_provider(
        &store,
        "elixir.modules.debrid",
        "debrid.resolver",
        "torbox",
        "api.torbox.app",
        Some("debrid"),
        ProviderHealthState::Unhealthy,
    )
    .await?;

    let routes_response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/download-broker/routes")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(routes_response.status(), StatusCode::OK);
    let body = body::to_bytes(routes_response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let route = json
        .get("routes")
        .and_then(Value::as_array)
        .and_then(|routes| {
            routes.iter().find(|route| {
                route.get("logicalId").and_then(Value::as_str) == Some("acquisition.debrid.default")
                    && route.get("ownerId").and_then(Value::as_str) == Some("default")
            })
        })
        .context("missing default debrid route")?;
    assert_eq!(
        route.get("blocker").and_then(Value::as_str),
        Some("Active debrid service is unavailable")
    );
    assert!(
        route
            .get("candidates")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|candidate| {
                candidate.get("implementation").and_then(Value::as_str) == Some("torbox")
                    && candidate.get("healthState").and_then(Value::as_str) == Some("unhealthy")
            })
    );

    let submit_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/download-broker/acquisition.debrid.default/submit")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"source":"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567","category":"debrid"}"#,
                ))?,
        )
        .await?;
    assert_eq!(submit_response.status(), StatusCode::CONFLICT);
    let body = body::to_bytes(submit_response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json.get("message").and_then(Value::as_str),
        Some("Active debrid service is unavailable")
    );
    Ok(())
}

#[tokio::test]
async fn download_broker_debrid_submit_without_token_fails_closed() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    seed_download_broker_provider(
        &store,
        "elixir.modules.real_debrid",
        "debrid.resolver",
        "real_debrid",
        "api.real-debrid.com",
        Some("debrid"),
        ProviderHealthState::Healthy,
    )
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/download-broker/acquisition.debrid.default/submit")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"source":"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567","category":"debrid"}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(json.get("code").and_then(Value::as_str), Some("conflict"));
    assert_eq!(
        json.get("message").and_then(Value::as_str),
        Some("Add debrid account")
    );
    Ok(())
}

#[test]
fn desktop_acquisition_debrid_copy_has_no_generic_real_debrid_language() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let paths = [
        "elixir-client/src/qml/views/FindMediaView.qml",
        "elixir-client/src/qml/views/AcquisitionView.qml",
        "elixir-client/src/qml/components/AcquisitionReviewPanel.qml",
    ];

    for relative in paths {
        let path = root.join(relative);
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("reading client Debrid copy audit path {}", path.display()))?;
        for (index, line) in content.lines().enumerate() {
            let has_real_debrid_copy = line.contains("Real-Debrid") || line.contains("Real Debrid");
            if !has_real_debrid_copy {
                continue;
            }
            let allowed_legacy_sanitizer = line.contains("Real-Debrid API token is not configured")
                || line.contains("Real Debrid API token is not configured");
            let allowed_provider_evidence = line.contains("return \"Real-Debrid\"");
            assert!(
                allowed_legacy_sanitizer || allowed_provider_evidence,
                "generic acquisition UI must not expose Real-Debrid-only copy at {}:{}: {}",
                relative,
                index + 1,
                line.trim()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn acquisition_intent_endpoints_create_and_reuse_native_subscription() -> Result<()> {
    let (app, _db_pool, token) = setup_download_broker_test_app().await?;
    let request = json!({
        "mediaType": "series",
        "title": "Endpoint Show",
        "year": 2026,
        "externalIds": {
            "tvdbSeries": "98765"
        },
        "target": {
            "kind": "season",
            "seasonNumber": 2,
            "episodeStart": 1,
            "episodeEnd": 2
        }
    });

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find/acquisition")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let first: Value = serde_json::from_slice(&body)?;
    assert_eq!(first.get("created").and_then(Value::as_bool), Some(true));
    assert_eq!(
        first.get("expandedTargetCount").and_then(Value::as_u64),
        Some(2)
    );
    assert_eq!(
        first
            .pointer("/detail/subscription/monitorPolicy")
            .and_then(Value::as_str),
        Some("selected_targets")
    );
    assert_eq!(
        first
            .pointer("/detail/subscription/routePolicy")
            .and_then(Value::as_str),
        Some("debrid_first")
    );
    let subscription_id = first
        .pointer("/detail/subscription/subscriptionId")
        .and_then(Value::as_str)
        .context("created subscription id")?
        .to_string();
    let target_keys = first
        .pointer("/detail/targets")
        .and_then(Value::as_array)
        .context("created targets")?
        .iter()
        .map(|target| target.get("targetKey").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(target_keys, vec![Some("S02E01"), Some("S02E02")]);

    let reused_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/intents")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    assert_eq!(reused_response.status(), StatusCode::OK);
    let body = body::to_bytes(reused_response.into_body(), 1_048_576).await?;
    let reused: Value = serde_json::from_slice(&body)?;
    assert_eq!(reused.get("created").and_then(Value::as_bool), Some(false));
    assert_eq!(
        reused
            .pointer("/detail/subscription/subscriptionId")
            .and_then(Value::as_str),
        Some(subscription_id.as_str())
    );

    let movie_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/acquisition")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"mediaType":"movie","title":"Endpoint Movie","year":2026}"#,
                ))?,
        )
        .await?;
    assert_eq!(movie_response.status(), StatusCode::OK);
    let body = body::to_bytes(movie_response.into_body(), 1_048_576).await?;
    let movie: Value = serde_json::from_slice(&body)?;
    assert_eq!(movie.get("created").and_then(Value::as_bool), Some(true));
    assert_eq!(
        movie
            .pointer("/detail/targets/0/targetKey")
            .and_then(Value::as_str),
        Some("MOVIE")
    );

    Ok(())
}

#[tokio::test]
async fn osr2_acquisition_request_endpoints_are_idempotent_cancelable_and_retryable() -> Result<()>
{
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let request = json!({
        "mediaType": "series",
        "title": "OSR API Show",
        "year": 2026,
        "idempotencyKey": "osr2-api-show-season-1",
        "requestMode": "one_shot",
        "target": {
            "kind": "range",
            "seasonNumber": 1,
            "episodeStart": 1,
            "episodeEnd": 2
        }
    });

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/requests")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let first: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "create response: {first}");
    assert_eq!(first.get("created").and_then(Value::as_bool), Some(true));
    assert_eq!(first.get("existing").and_then(Value::as_bool), Some(false));
    assert_eq!(
        first
            .pointer("/detail/subscription/requestMode")
            .and_then(Value::as_str),
        Some("one_shot")
    );
    assert_eq!(
        first
            .pointer("/detail/subscription/idempotencyKey")
            .and_then(Value::as_str),
        Some("osr2-api-show-season-1")
    );
    assert_eq!(
        first
            .pointer("/targetCounts/pending")
            .and_then(Value::as_u64),
        Some(2)
    );
    let subscription_id = first
        .pointer("/detail/subscription/subscriptionId")
        .and_then(Value::as_str)
        .context("created subscription id")?
        .to_string();

    let duplicate_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/requests")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    let body = body::to_bytes(duplicate_response.into_body(), 1_048_576).await?;
    let duplicate: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        duplicate.get("created").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        duplicate.get("existing").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        duplicate
            .pointer("/detail/subscription/subscriptionId")
            .and_then(Value::as_str),
        Some(subscription_id.as_str())
    );

    let get_response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/acquisition/requests/{subscription_id}"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    let body = body::to_bytes(get_response.into_body(), 1_048_576).await?;
    let request_detail: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        request_detail
            .pointer("/request/subscriptionId")
            .and_then(Value::as_str),
        Some(subscription_id.as_str())
    );
    assert_eq!(
        request_detail
            .pointer("/targetCounts/total")
            .and_then(Value::as_u64),
        Some(2)
    );

    let cancel_response = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/acquisition/requests/{subscription_id}/cancel"
            ))
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from(r#"{"mode":"stop_tracking"}"#))?,
        )
        .await?;
    assert_eq!(cancel_response.status(), StatusCode::OK);
    let cancelled_active: i64 = sqlx::query_scalar(
        "SELECT CAST(active AS INTEGER) FROM acquisition_subscriptions WHERE subscription_id = $1",
    )
    .bind(&subscription_id)
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(cancelled_active, 0);

    let retry_response = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/acquisition/requests/{subscription_id}/retry"
            ))
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from(r#"{"reason":"retry OSR-2 request"}"#))?,
        )
        .await?;
    let status = retry_response.status();
    let body = body::to_bytes(retry_response.into_body(), 1_048_576).await?;
    let retry: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "retry response: {retry}");
    assert_eq!(retry.get("targetsReset").and_then(Value::as_u64), Some(2));
    assert_eq!(
        retry
            .pointer("/targetCounts/pending")
            .and_then(Value::as_u64),
        Some(2)
    );
    assert_eq!(
        retry
            .pointer("/detail/subscription/status")
            .and_then(Value::as_str),
        Some("active")
    );

    sqlx::query(
        "UPDATE acquisition_subscriptions
         SET status = 'completed', active = 0
         WHERE subscription_id = $1",
    )
    .bind(&subscription_id)
    .execute(&db_pool)
    .await?;

    let requeue_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/requests")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    let status = requeue_response.status();
    let body = body::to_bytes(requeue_response.into_body(), 1_048_576).await?;
    let requeue: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "requeue response: {requeue}");
    assert_eq!(requeue.get("created").and_then(Value::as_bool), Some(true));
    assert_ne!(
        requeue
            .pointer("/detail/subscription/subscriptionId")
            .and_then(Value::as_str),
        Some(subscription_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn mmr4_selected_episode_retry_uses_explicit_targets_and_idempotency() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let request = json!({
        "mediaType": "series",
        "title": "MMR4 Selected Show",
        "year": 2026,
        "idempotencyKey": "mmr4-selected-s01e01-s01e03",
        "requestMode": "one_shot",
        "requestScope": "selected_targets",
        "metadataPolicy": "initial_only",
        "completionPolicy": "terminal_selected_targets",
        "monitorPolicy": "selected_targets",
        "target": {
            "kind": "selected_targets",
            "seasonNumber": 1,
            "metadata": {
                "requestedFrom": "media_detail",
                "mediaItemId": "library-series-1",
                "targetKeys": ["S01E01", "S01E03"]
            }
        },
        "targets": [
            {
                "targetKey": "S01E01",
                "mediaType": "series",
                "title": "Episode 1",
                "seasonNumber": 1,
                "episodeNumber": 1,
                "state": "pending",
                "metadata": {
                    "mediaItemId": "library-series-1",
                    "libraryEpisodeId": "episode-1"
                }
            },
            {
                "targetKey": "S01E03",
                "mediaType": "series",
                "title": "Episode 3",
                "seasonNumber": 1,
                "episodeNumber": 3,
                "state": "pending",
                "metadata": {
                    "mediaItemId": "library-series-1",
                    "libraryEpisodeId": "episode-3"
                }
            }
        ],
        "scope": {
            "requestedFrom": "media_detail",
            "requestScope": "selected_targets",
            "targetCount": 2
        }
    });

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/requests")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let first: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "create response: {first}");
    assert_eq!(first.get("created").and_then(Value::as_bool), Some(true));
    assert_eq!(first.get("existing").and_then(Value::as_bool), Some(false));
    assert_eq!(
        first
            .pointer("/detail/subscription/requestScope")
            .and_then(Value::as_str),
        Some("selected_targets")
    );
    assert_eq!(
        first
            .pointer("/detail/subscription/idempotencyKey")
            .and_then(Value::as_str),
        Some("mmr4-selected-s01e01-s01e03")
    );
    assert_eq!(
        first
            .pointer("/targetCounts/pending")
            .and_then(Value::as_u64),
        Some(2)
    );
    let subscription_id = first
        .pointer("/detail/subscription/subscriptionId")
        .and_then(Value::as_str)
        .context("created subscription id")?
        .to_string();
    let target_keys = first
        .pointer("/detail/targets")
        .and_then(Value::as_array)
        .context("created targets")?
        .iter()
        .map(|target| target.get("targetKey").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(target_keys, vec![Some("S01E01"), Some("S01E03")]);

    let duplicate_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/requests")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    let body = body::to_bytes(duplicate_response.into_body(), 1_048_576).await?;
    let duplicate: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        duplicate.get("created").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        duplicate.get("existing").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        duplicate
            .pointer("/detail/subscription/subscriptionId")
            .and_then(Value::as_str),
        Some(subscription_id.as_str())
    );
    let active_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM acquisition_subscriptions
         WHERE idempotency_key = $1 AND active = 1",
    )
    .bind("mmr4-selected-s01e01-s01e03")
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(active_count, 1);

    sqlx::query(
        "UPDATE acquisition_subscriptions
         SET status = 'completed', active = 0
         WHERE subscription_id = $1",
    )
    .bind(&subscription_id)
    .execute(&db_pool)
    .await?;

    let retry_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/requests")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    let status = retry_response.status();
    let body = body::to_bytes(retry_response.into_body(), 1_048_576).await?;
    let retry: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "retry response: {retry}");
    assert_eq!(retry.get("created").and_then(Value::as_bool), Some(true));
    assert_ne!(
        retry
            .pointer("/detail/subscription/subscriptionId")
            .and_then(Value::as_str),
        Some(subscription_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn acquisition_intent_accepts_extension_suite_source_selection_route() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    let source_provider_id =
        seed_acquisition_candidate_provider(&store, "elixir.sources.suite_route").await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find/acquisition")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&json!({
                    "mediaType": "movie",
                    "title": "Suite Route Movie",
                    "year": 2026,
                    "sourceProviderId": source_provider_id,
                    "sourceSelectionRoute": "suite:default",
                    "sourceSelectionMode": "suite",
                    "sourceSuiteId": "default"
                }))?))?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "create response: {payload}");
    assert_eq!(
        payload.pointer("/detail/subscription/sourceProviderId"),
        Some(&json!(source_provider_id.to_string()))
    );
    assert_eq!(
        payload
            .pointer("/detail/subscription/routePolicy")
            .and_then(Value::as_str),
        Some("debrid_first")
    );
    assert_eq!(
        payload
            .pointer("/detail/subscription/scope/sourceSelection/mode")
            .and_then(Value::as_str),
        Some("suite")
    );
    assert_eq!(
        payload
            .pointer("/detail/subscription/scope/sourceSelection/route")
            .and_then(Value::as_str),
        Some("suite:default")
    );

    Ok(())
}

#[tokio::test]
async fn mmr5_acquisition_history_links_show_and_summarizes_terminal_one_shot_counts() -> Result<()>
{
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    let source_provider_id =
        seed_acquisition_candidate_provider(&store, "elixir.sources.mmr5").await?;
    let media_item_id = Uuid::new_v4().to_string();
    let request = json!({
        "mediaType": "series",
        "title": "MMR5 History Show",
        "year": 2026,
        "idempotencyKey": "mmr5-history-show-s01",
        "requestMode": "one_shot",
        "requestScope": "selected_targets",
        "metadataPolicy": "initial_only",
        "completionPolicy": "terminal_selected_targets",
        "monitorPolicy": "selected_targets",
        "sourceProviderId": source_provider_id,
        "target": {
            "kind": "selected_targets",
            "seasonNumber": 1,
            "metadata": {
                "requestedFrom": "media_detail",
                "mediaItemId": media_item_id,
                "targetKeys": ["S01E01", "S01E02"]
            }
        },
        "targets": [
            {
                "targetKey": "S01E01",
                "mediaType": "series",
                "title": "Episode 1",
                "seasonNumber": 1,
                "episodeNumber": 1,
                "state": "pending",
                "metadata": {
                    "mediaItemId": media_item_id,
                    "libraryEpisodeId": "episode-1"
                }
            },
            {
                "targetKey": "S01E02",
                "mediaType": "series",
                "title": "Episode 2",
                "seasonNumber": 1,
                "episodeNumber": 2,
                "state": "pending",
                "metadata": {
                    "mediaItemId": media_item_id,
                    "libraryEpisodeId": "episode-2"
                }
            }
        ],
        "scope": {
            "requestedFrom": "media_detail",
            "mediaItemId": media_item_id,
            "requestScope": "selected_targets",
            "targetCount": 2
        }
    });

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/requests")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request)?))?,
        )
        .await?;
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let created: Value = serde_json::from_slice(&body)?;
    let subscription_id = created
        .pointer("/detail/subscription/subscriptionId")
        .and_then(Value::as_str)
        .context("subscription id")?;
    let targets = created
        .pointer("/detail/targets")
        .and_then(Value::as_array)
        .context("targets")?;
    let imported_target_id = targets[0]
        .get("targetId")
        .and_then(Value::as_str)
        .context("imported target id")?;
    let no_results_target_id = targets[1]
        .get("targetId")
        .and_then(Value::as_str)
        .context("no-results target id")?;

    sqlx::query(
        "UPDATE acquisition_targets
         SET state = 'imported', state_reason = 'Imported into library.'
         WHERE target_id = $1",
    )
    .bind(imported_target_id)
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "UPDATE acquisition_targets
         SET state = 'excluded', state_reason = 'No matching acquisition candidates were returned.'
         WHERE target_id = $1",
    )
    .bind(no_results_target_id)
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "UPDATE acquisition_subscriptions
         SET status = 'completed', active = 0, updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = $1",
    )
    .bind(subscription_id)
    .execute(&db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/find/acquisition?limit=50")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "history response: {payload}");
    assert_eq!(
        payload.get("recentCompletedCount").and_then(Value::as_u64),
        Some(1),
        "payload: {payload}"
    );
    let item = payload
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("intentId").and_then(Value::as_str) == Some(subscription_id))
        })
        .context("completed acquisition item")?;
    assert_eq!(item.get("phase").and_then(Value::as_str), Some("completed"));
    assert_eq!(
        item.get("mediaItemId").and_then(Value::as_str),
        Some(media_item_id.as_str())
    );
    assert_eq!(
        item.get("headline").and_then(Value::as_str),
        Some("1 target imported, 1 target no results.")
    );
    let evidence = item
        .get("evidence")
        .and_then(Value::as_array)
        .context("evidence")?;
    assert!(evidence.iter().any(|entry| {
        entry.get("label").and_then(Value::as_str) == Some("Imported")
            && entry.get("value").and_then(Value::as_str) == Some("1")
    }));
    assert!(evidence.iter().any(|entry| {
        entry.get("label").and_then(Value::as_str) == Some("No results")
            && entry.get("value").and_then(Value::as_str) == Some("1")
    }));
    let actions = item
        .get("actions")
        .and_then(Value::as_array)
        .context("actions")?;
    assert!(actions.iter().any(|action| {
        action.get("id").and_then(Value::as_str) == Some("open_show")
            && action.get("navigateMediaItemId").and_then(Value::as_str)
                == Some(media_item_id.as_str())
    }));
    assert!(actions.iter().any(|action| {
        action.get("id").and_then(Value::as_str) == Some("retry_missing")
            && action.get("subscriptionId").and_then(Value::as_str) == Some(subscription_id)
    }));
    let children = item
        .get("children")
        .and_then(Value::as_array)
        .context("children")?;
    assert!(children.iter().any(|child| {
        child.get("title").and_then(Value::as_str) == Some("S01E02")
            && child.get("status").and_then(Value::as_str) == Some("no_results")
            && child.get("phaseLabel").and_then(Value::as_str) == Some("No results")
    }));
    let active_after_read: i64 = sqlx::query_scalar(
        "SELECT CAST(active AS INTEGER)
         FROM acquisition_subscriptions
         WHERE subscription_id = $1",
    )
    .bind(subscription_id)
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(
        active_after_read, 0,
        "completed one-shot history reads must not reactivate requests"
    );

    Ok(())
}

#[tokio::test]
async fn acquisition_intent_uses_source_provider_advanced_defaults() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    let extension_id = "elixir.sources.torrentio_stremio";
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: "Torrentio-Compatible Source".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": extension_id,
                "version": "0.1.0",
                "kind": "module",
                "name": "Torrentio-Compatible Source",
                "provides": [{
                    "capability": "acquisition.candidate_provider",
                    "slot": "default",
                    "cardinality": "many",
                    "implementation": "torrentio_stremio"
                }],
                "runtime": {
                    "type": "container",
                    "image": "elixir/torrentio-candidate-provider:0.1.0"
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
            config_json: Some(json!({
                "routePolicy": "torrent_only",
                "releaseDelaySeconds": 900,
                "allowedQualities": "2160p,1080p",
                "requiredLanguages": "en,ja",
                "maxSizeGb": 12.5,
                "resultLimit": 25
            })),
            enabled: true,
        })
        .await?;
    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "acquisition.candidate_provider".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("torrentio_stremio".to_string()),
            scope_json: Some(json!({
                "media_types": ["movie", "tv", "anime"],
                "actions": ["search"]
            })),
            endpoint_json: Some(serde_json::to_value(ProviderEndpoint::new(
                "http".to_string(),
                "candidate-source".to_string(),
                8097,
                None,
                Some("elixir_net".to_string()),
            )?)?),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find/acquisition")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&json!({
                    "mediaType": "movie",
                    "title": "Configured Movie",
                    "year": 2026,
                    "sourceProviderId": provider_id
                }))?))?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "create response: {payload}");
    assert_eq!(
        payload
            .pointer("/detail/subscription/routePolicy")
            .and_then(Value::as_str),
        Some("torrent_only")
    );
    assert_eq!(
        payload
            .pointer("/detail/subscription/releaseDelaySeconds")
            .and_then(Value::as_i64),
        Some(900)
    );
    assert_eq!(
        payload.pointer("/detail/subscription/sourceProviderId"),
        Some(&json!(provider_id.to_string()))
    );
    assert_eq!(
        payload
            .pointer("/detail/subscription/qualityProfile/allowedQualities")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(2)
    );
    assert_eq!(
        payload
            .pointer("/detail/subscription/qualityProfile/requiredLanguages")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(2)
    );
    assert_eq!(
        payload
            .pointer("/detail/subscription/qualityProfile/maxSizeBytes")
            .and_then(Value::as_u64),
        Some((12.5_f64 * 1024.0 * 1024.0 * 1024.0).round() as u64)
    );
    Ok(())
}

#[tokio::test]
async fn acquisition_target_submit_records_source_owned_debrid_account_blocker() -> Result<()> {
    let (app, db_pool, token) = setup_download_broker_test_app().await?;
    let store = ExtensionStore::new(&db_pool);
    seed_download_broker_provider(
        &store,
        "elixir.modules.real_debrid",
        "debrid.resolver",
        "real_debrid",
        "api.real-debrid.com",
        Some("debrid"),
        ProviderHealthState::Healthy,
    )
    .await?;
    let source_extension_id = "elixir.sources.test_candidate_provider";
    let source_provider_id =
        seed_acquisition_candidate_provider(&store, source_extension_id).await?;

    let subscription_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/acquisition/subscriptions")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&json!({
                    "mediaType": "movie",
                    "title": "Example Movie",
                    "routePolicy": "debrid_first",
                    "sourceProviderId": source_provider_id,
                    "targets": [
                        {
                            "targetKey": "movie",
                            "mediaType": "movie",
                            "title": "Example Movie"
                        }
                    ]
                }))?))?,
        )
        .await?;

    let subscription_status = subscription_response.status();
    let body = body::to_bytes(subscription_response.into_body(), 1_048_576).await?;
    let created: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        subscription_status,
        StatusCode::OK,
        "create subscription response: {created}"
    );
    let target_id = created
        .get("targets")
        .and_then(Value::as_array)
        .and_then(|targets| targets.first())
        .and_then(|target| target.get("targetId"))
        .and_then(Value::as_str)
        .context("created target id")?
        .to_string();
    let subscription_id = created
        .pointer("/subscription/subscriptionId")
        .and_then(Value::as_str)
        .context("created subscription id")?
        .to_string();

    let submit_response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/acquisition/targets/{target_id}/submit"))
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&json!({
                    "providerId": source_provider_id,
                    "candidate": {
                        "title": "Example Movie 1080p",
                        "source": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                        "sourceKind": "magnet",
                        "infoHash": "0123456789abcdef0123456789abcdef01234567",
                        "supportedRoutes": [
                            "acquisition.debrid.default",
                            "downloaders.torrent.default"
                        ],
                        "defaultRoute": "acquisition.debrid.default"
                    }
                }))?))?,
        )
        .await?;

    assert_eq!(submit_response.status(), StatusCode::CONFLICT);
    let body = body::to_bytes(submit_response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(json.get("code").and_then(Value::as_str), Some("conflict"));
    assert_eq!(
        json.get("message").and_then(Value::as_str),
        Some("Add debrid account")
    );

    let detail_response = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/acquisition/subscriptions/{subscription_id}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(detail_response.status(), StatusCode::OK);
    let body = body::to_bytes(detail_response.into_body(), 1_048_576).await?;
    let detail: Value = serde_json::from_slice(&body)?;
    let target = detail
        .get("targets")
        .and_then(Value::as_array)
        .and_then(|targets| targets.first())
        .context("blocked target")?;
    assert_eq!(target.get("state").and_then(Value::as_str), Some("blocked"));
    assert_eq!(
        target
            .get("selectedProviderId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        Some(source_provider_id)
    );
    assert_eq!(
        target.get("selectedRouteLogicalId").and_then(Value::as_str),
        Some("acquisition.debrid.default")
    );
    let state_reason = target
        .get("stateReason")
        .and_then(Value::as_str)
        .context("target state reason")?;
    assert_eq!(
        state_reason,
        "Debrid route failed: Add debrid account; torrent fallback failed: Active debrid service is not configured"
    );
    assert!(!state_reason.contains("Real-Debrid"));
    assert_eq!(
        target
            .pointer("/selectedCandidate/sourceProviderId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        Some(source_provider_id)
    );
    assert_eq!(
        target
            .pointer("/selectedCandidate/sourceExtensionId")
            .and_then(Value::as_str),
        Some(source_extension_id)
    );
    assert_eq!(
        target
            .pointer("/selectedCandidate/sourceKind")
            .and_then(Value::as_str),
        Some("magnet")
    );
    assert_eq!(target.get("downloadId").and_then(Value::as_str), None);
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
async fn extension_control_surface_updates_builtin_downloader_profile() -> Result<()> {
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

    let initial_response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.qbittorrent/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(initial_response.status(), StatusCode::OK);
    let initial_body = body::to_bytes(initial_response.into_body(), 1_048_576).await?;
    let initial_payload: Value = serde_json::from_slice(&initial_body)?;
    let initial_fields = control_surface_section(&initial_payload, "defaults")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        initial_fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("downloaderProfile")
                && field.get("value").and_then(Value::as_str) == Some("balanced")
        }),
        "expected balanced downloader profile field in control surface: {}",
        initial_payload
    );

    let aggressive_response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/extensions/elixir.modules.qbittorrent/control-surface")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "values": {
                        "downloaderProfile": "aggressive"
                    }
                }))?))?,
        )
        .await?;
    assert_eq!(aggressive_response.status(), StatusCode::OK);
    let aggressive_body = body::to_bytes(aggressive_response.into_body(), 1_048_576).await?;
    let aggressive_payload: Value = serde_json::from_slice(&aggressive_body)?;
    let aggressive_fields = control_surface_section(&aggressive_payload, "defaults")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        aggressive_fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("downloaderProfile")
                && field.get("value").and_then(Value::as_str) == Some("aggressive")
        }),
        "expected aggressive downloader profile field in control surface: {}",
        aggressive_payload
    );
    let aggressive_override = store.get_extension_setting("downloader_profile").await?;
    assert_eq!(aggressive_override, Some(json!("aggressive")));

    let balanced_response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/extensions/elixir.modules.qbittorrent/control-surface")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "values": {
                        "downloaderProfile": "balanced"
                    }
                }))?))?,
        )
        .await?;
    assert_eq!(balanced_response.status(), StatusCode::OK);
    let balanced_body = body::to_bytes(balanced_response.into_body(), 1_048_576).await?;
    let balanced_payload: Value = serde_json::from_slice(&balanced_body)?;
    let balanced_fields = control_surface_section(&balanced_payload, "defaults")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        balanced_fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("downloaderProfile")
                && field.get("value").and_then(Value::as_str) == Some("balanced")
        }),
        "expected balanced downloader profile field after reset: {}",
        balanced_payload
    );
    assert!(
        store
            .get_extension_setting("downloader_profile")
            .await?
            .is_none(),
        "expected downloader profile override to be cleared when reset to default"
    );

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
async fn extension_status_summary_auto_provisions_missing_zero_config_instance() -> Result<()> {
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
            extension_id: "elixir.sources.torrentio_stremio".to_string(),
            name: "Torrentio".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.sources.torrentio_stremio",
                "version": "0.1.0",
                "kind": "module",
                "name": "Torrentio",
                "provides": [{
                    "capability": "acquisition.candidate_provider",
                    "slot": "torrentio",
                    "implementation": "torrentio"
                }],
                "runtime": {
                    "type": "container",
                    "image": "elixir/torrentio-candidate-provider:0.1.0"
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

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .expect("summary items");
    let torrentio = items
        .iter()
        .find(|item| {
            item.get("extensionId").and_then(Value::as_str)
                == Some("elixir.sources.torrentio_stremio")
        })
        .expect("torrentio summary item");
    assert_ne!(
        torrentio.get("statusCode").and_then(Value::as_str),
        Some("missing_instance")
    );
    assert_eq!(
        torrentio.get("statusCode").and_then(Value::as_str),
        Some("provider_registration_pending")
    );
    assert_eq!(
        torrentio.get("label").and_then(Value::as_str),
        Some("Starting up")
    );
    assert_eq!(
        torrentio.get("primaryAction").and_then(Value::as_str),
        Some("open")
    );

    let instances = store
        .list_instances(Some("elixir.sources.torrentio_stremio"))
        .await?;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_name, "default");
    assert!(instances[0].enabled);

    Ok(())
}

#[tokio::test]
async fn candidate_provider_status_summary_distinguishes_runtime_readiness() -> Result<()> {
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
    seed_candidate_provider_summary_case(
        &store,
        "elixir.sources.pending",
        "Pending Source",
        None,
        None,
    )
    .await?;
    seed_candidate_provider_summary_case(
        &store,
        "elixir.sources.transport",
        "Transport Source",
        Some(ProviderHealthState::Unknown),
        Some(ProviderReadinessPhase::TransportReady),
    )
    .await?;
    seed_candidate_provider_summary_case(
        &store,
        "elixir.sources.unhealthy",
        "Unhealthy Source",
        Some(ProviderHealthState::Unhealthy),
        Some(ProviderReadinessPhase::Unknown),
    )
    .await?;
    seed_candidate_provider_summary_case(
        &store,
        "elixir.sources.healthy",
        "Healthy Source",
        Some(ProviderHealthState::Healthy),
        Some(ProviderReadinessPhase::DriverReady),
    )
    .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .expect("summary items");
    let status_for = |extension_id: &str| -> Option<&str> {
        items
            .iter()
            .find(|item| item.get("extensionId").and_then(Value::as_str) == Some(extension_id))
            .and_then(|item| item.get("statusCode"))
            .and_then(Value::as_str)
    };

    assert_eq!(
        status_for("elixir.sources.pending"),
        Some("provider_registration_pending")
    );
    assert_eq!(
        status_for("elixir.sources.transport"),
        Some("runtime_starting")
    );
    assert_eq!(
        status_for("elixir.sources.unhealthy"),
        Some("connection_issue")
    );
    assert_eq!(status_for("elixir.sources.healthy"), Some("ready"));

    Ok(())
}

#[tokio::test]
async fn live_provider_status_summary_reports_startup_as_progress_until_driver_ready() -> Result<()>
{
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
    seed_live_provider_summary_case(&store, "elixir.live.pending", "Pending Live", None, None)
        .await?;
    seed_live_provider_summary_case(
        &store,
        "elixir.live.transport",
        "Transport Live",
        Some(ProviderHealthState::Unknown),
        Some(ProviderReadinessPhase::TransportReady),
    )
    .await?;
    seed_live_provider_summary_case(
        &store,
        "elixir.live.bootstrap",
        "Bootstrap Live",
        Some(ProviderHealthState::Healthy),
        Some(ProviderReadinessPhase::BootstrapReady),
    )
    .await?;
    seed_live_provider_summary_case(
        &store,
        "elixir.live.ready",
        "Ready Live",
        Some(ProviderHealthState::Healthy),
        Some(ProviderReadinessPhase::DriverReady),
    )
    .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("needsAttentionCount").and_then(Value::as_u64),
        Some(0)
    );
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .expect("summary items");
    let status_for = |extension_id: &str| -> (&str, &str, &str) {
        let item = items
            .iter()
            .find(|item| item.get("extensionId").and_then(Value::as_str) == Some(extension_id))
            .expect("Live summary item");
        (
            item.get("severity")
                .and_then(Value::as_str)
                .expect("severity"),
            item.get("statusCode")
                .and_then(Value::as_str)
                .expect("status code"),
            item.get("label").and_then(Value::as_str).expect("label"),
        )
    };

    assert_eq!(
        status_for("elixir.live.pending"),
        ("progress", "provider_registration_pending", "Starting up")
    );
    assert_eq!(
        status_for("elixir.live.transport"),
        ("progress", "runtime_starting", "Connecting")
    );
    assert_eq!(
        status_for("elixir.live.bootstrap"),
        ("progress", "bootstrap_in_progress", "Finishing setup")
    );
    assert_eq!(status_for("elixir.live.ready"), ("ready", "ready", "Ready"));

    Ok(())
}

async fn seed_candidate_provider_summary_case(
    store: &ExtensionStore<'_>,
    extension_id: &str,
    name: &str,
    provider_health: Option<ProviderHealthState>,
    readiness_phase: Option<ProviderReadinessPhase>,
) -> Result<()> {
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: name.to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": extension_id,
                "version": "0.1.0",
                "kind": "module",
                "name": name,
                "provides": [{
                    "capability": "acquisition.candidate_provider",
                    "slot": "default",
                    "implementation": "torrentio_stremio",
                    "scope": {
                        "media_types": ["movie", "tv", "anime"],
                        "actions": ["search"],
                        "requires_account": false,
                        "required_fields": []
                    }
                }],
                "runtime": {
                    "type": "container",
                    "image": "elixir/torrentio-candidate-provider:0.1.0"
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
    let Some(provider_health) = provider_health else {
        return Ok(());
    };

    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "acquisition.candidate_provider".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("torrentio_stremio".to_string()),
            scope_json: Some(json!({
                "media_types": ["movie", "tv", "anime"],
                "actions": ["search"],
                "requires_account": false,
                "required_fields": []
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-source",
                "port": 8097,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: provider_health,
        })
        .await?;
    if let Some(readiness_phase) = readiness_phase {
        store
            .upsert_provider_readiness(provider_id, readiness_phase, None)
            .await?;
    }
    Ok(())
}

async fn seed_live_provider_summary_case(
    store: &ExtensionStore<'_>,
    extension_id: &str,
    name: &str,
    provider_health: Option<ProviderHealthState>,
    readiness_phase: Option<ProviderReadinessPhase>,
) -> Result<()> {
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: name.to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": extension_id,
                "version": "0.1.0",
                "kind": "module",
                "name": name,
                "permissions": [
                    "live.catalog.read",
                    "live.stream.resolve",
                    "network.egress"
                ],
                "provides": [{
                    "capability": "live.catalog_provider",
                    "slot": "default",
                    "cardinality": "many",
                    "implementation": "fixture_live",
                    "contract_version": 1,
                    "scope": {
                        "actions": ["catalog", "meta", "resolve"],
                        "live_item_types": ["channel"],
                        "stream_protocols": ["hls"],
                        "requires_account": false,
                        "required_fields": []
                    },
                    "endpoint": {
                        "type": "http",
                        "scheme": "http",
                        "port": 8112,
                        "base_path": "/"
                    },
                    "healthcheck": {
                        "type": "http",
                        "path": "/health"
                    }
                }],
                "runtime": {
                    "type": "container",
                    "image": "elixir/fixture-live:0.1.0",
                    "network": "elixir_net",
                    "service_name": "elx-fixture-live",
                    "ports": [{"container": 8112, "host": 0}]
                },
                "networking": {
                    "service_port": {
                        "scheme": "http",
                        "container_port": 8112
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
    let Some(provider_health) = provider_health else {
        return Ok(());
    };

    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "live.catalog_provider".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("fixture_live".to_string()),
            scope_json: Some(json!({
                "actions": ["catalog", "meta", "resolve"],
                "live_item_types": ["channel"],
                "stream_protocols": ["hls"],
                "requires_account": false,
                "required_fields": []
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-fixture-live",
                "port": 8112,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: provider_health,
        })
        .await?;
    if let Some(readiness_phase) = readiness_phase {
        store
            .upsert_provider_readiness(provider_id, readiness_phase, None)
            .await?;
    }
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
            item.get("extensionId").and_then(Value::as_str)
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

    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
async fn auth_session_refresh_reuse_logout_and_bootstrap_flow() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.database.url = format!(
        "sqlite:file:http-auth-sessions-{}?mode=memory&cache=shared",
        Uuid::new_v4()
    );
    settings.auth.access_token_secret =
        "http-auth-access-secret-00000000000000000000000000".to_string();
    settings.auth.refresh_token_secret =
        Some("http-auth-refresh-secret-0000000000000000000000000".to_string());
    settings.auth.csrf_secret =
        Some("http-auth-csrf-secret-000000000000000000000000000".to_string());
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

    let user_id = Uuid::new_v4();
    let email = "session-flow@example.test";
    let password = "correct horse battery staple";
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind(email)
        .bind(hash_password(password))
        .execute(&db_pool)
        .await?;

    let login_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/login")
                .header("content-type", "application/json")
                .header("user-agent", "elixir-auth-flow-test/1")
                .body(Body::from(
                    json!({
                        "email": email,
                        "password": password,
                        "remember_device": true,
                        "device_name": "Test Desktop",
                        "device_type": "desktop",
                        "client_name": "elixir-test",
                        "client_version": "1.0.0"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(login_response.status(), StatusCode::OK);
    assert_eq!(
        login_response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store, private")
    );
    let login: Value =
        serde_json::from_slice(&body::to_bytes(login_response.into_body(), 1_048_576).await?)?;
    let access = login["access_token"]
        .as_str()
        .expect("login access token")
        .to_string();
    let refresh = login["refresh_token"]
        .as_str()
        .expect("login refresh token")
        .to_string();
    let csrf = login["csrf_token"]
        .as_str()
        .expect("login CSRF token")
        .to_string();
    let session_id = login["session_id"]
        .as_str()
        .expect("login session id")
        .to_string();
    assert!(refresh.starts_with("elx_refresh_v1_"));
    assert!(csrf.starts_with("elx_csrf_v1_"));
    let stored: (String, String, String) = sqlx::query_as(
        "SELECT rt.token_hash, s.device_name, s.client_name
         FROM refresh_tokens AS rt
         JOIN account_sessions AS s ON s.id = rt.session_id
         WHERE s.id = $1",
    )
    .bind(&session_id)
    .fetch_one(&db_pool)
    .await?;
    assert_ne!(stored.0, refresh);
    assert_eq!(stored.1, "Test Desktop");
    assert_eq!(stored.2, "elixir-test");

    let bootstrap = app
        .clone()
        .oneshot(
            Request::get("/api/v1/auth/session")
                .header("authorization", format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(bootstrap.status(), StatusCode::OK);
    assert_eq!(
        bootstrap
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store, private")
    );
    assert_eq!(
        bootstrap
            .headers()
            .get("pragma")
            .and_then(|value| value.to_str().ok()),
        Some("no-cache")
    );
    let bootstrap_json: Value =
        serde_json::from_slice(&body::to_bytes(bootstrap.into_body(), 1_048_576).await?)?;
    assert_eq!(bootstrap_json["user_id"], user_id.to_string());
    assert_eq!(bootstrap_json["session_id"], session_id);
    assert_eq!(bootstrap_json["role"], "owner");
    assert_eq!(bootstrap_json["csrf_token"], csrf);
    assert!(bootstrap_json.get("access_token").is_none());
    assert!(bootstrap_json.get("refresh_token").is_none());

    let query_bootstrap = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/auth/session?access_token={access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(query_bootstrap.status(), StatusCode::FORBIDDEN);
    let cookie_bootstrap = app
        .clone()
        .oneshot(
            Request::get("/api/v1/auth/session")
                .header("cookie", format!("elixir_ui_token={access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(cookie_bootstrap.status(), StatusCode::FORBIDDEN);

    let refreshed = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/refresh")
                .header("content-type", "application/json")
                .body(Body::from(json!({"refresh_token": refresh}).to_string()))?,
        )
        .await?;
    assert_eq!(refreshed.status(), StatusCode::OK);
    assert_eq!(
        refreshed
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store, private")
    );
    let refreshed_json: Value =
        serde_json::from_slice(&body::to_bytes(refreshed.into_body(), 1_048_576).await?)?;
    let rotated_access = refreshed_json["access_token"]
        .as_str()
        .expect("rotated access token")
        .to_string();
    let rotated_refresh = refreshed_json["refresh_token"]
        .as_str()
        .expect("rotated refresh token")
        .to_string();
    assert_ne!(rotated_refresh, refresh);
    assert_ne!(refreshed_json["csrf_token"], csrf);

    let reused = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/refresh")
                .header("content-type", "application/json")
                .body(Body::from(json!({"refresh_token": refresh}).to_string()))?,
        )
        .await?;
    assert_eq!(reused.status(), StatusCode::UNAUTHORIZED);
    let reused_body = body::to_bytes(reused.into_body(), 1_048_576).await?;
    let reused_body = String::from_utf8(reused_body.to_vec())?;
    assert!(!reused_body.contains(&refresh));
    assert!(!reused_body.contains(&rotated_refresh));
    let revoked_bootstrap = app
        .clone()
        .oneshot(
            Request::get("/api/v1/auth/session")
                .header("authorization", format!("Bearer {rotated_access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(revoked_bootstrap.status(), StatusCode::UNAUTHORIZED);

    let second_login = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"email": email, "password": password}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(second_login.status(), StatusCode::OK);
    let second_login_json: Value =
        serde_json::from_slice(&body::to_bytes(second_login.into_body(), 1_048_576).await?)?;
    let second_access = second_login_json["access_token"]
        .as_str()
        .expect("second access token");
    let second_refresh = second_login_json["refresh_token"]
        .as_str()
        .expect("second refresh token");
    let second_session = second_login_json["session_id"]
        .as_str()
        .expect("second session id");
    let query_logout = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/auth/logout?access_token={second_access}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(query_logout.status(), StatusCode::FORBIDDEN);
    let still_active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM account_sessions WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(second_session)
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(still_active, 1);
    let logout = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/logout")
                .header("authorization", format!("Bearer {second_access}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(logout.status(), StatusCode::OK);
    let revoked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM account_sessions WHERE id = $1 AND revoked_at IS NOT NULL",
    )
    .bind(second_session)
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(revoked, 1);
    let refresh_after_logout = app
        .oneshot(
            Request::post("/api/v1/auth/refresh")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"refresh_token": second_refresh}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(refresh_after_logout.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn a14_profile_list_select_pin_and_revocation_preserve_legacy_routes() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.database.url = "sqlite::memory:?cache=shared".to_string();
    settings.database.max_connections = 1;
    settings.auth.access_token_secret =
        "a14-access-secret-000000000000000000000000000000".to_string();
    settings.auth.refresh_token_secret =
        Some("a14-refresh-secret-00000000000000000000000000000".to_string());
    settings.auth.csrf_secret = Some("a14-csrf-secret-0000000000000000000000000000000".to_string());
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let db_pool = database.pool.clone();
    let auth_service = AuthService::new(settings.auth.clone())?;
    let mut revocations = auth_service.authorization_revocation_notifier().subscribe();
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

    let user_id = Uuid::new_v4();
    let email = "a14-owner@example.test";
    let password = "correct horse battery staple";
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind(email)
        .bind(hash_password(password))
        .execute(&db_pool)
        .await?;
    let login_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"email": email, "password": password}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(login_response.status(), StatusCode::OK);
    let login: Value =
        serde_json::from_slice(&body::to_bytes(login_response.into_body(), 1_048_576).await?)?;
    let access = login["access_token"]
        .as_str()
        .expect("access token")
        .to_string();
    let refresh = login["refresh_token"]
        .as_str()
        .expect("refresh token")
        .to_string();
    let home_id = login["home_id"].as_str().expect("home id").to_string();
    let owner_profile_id = login["profile_id"]
        .as_str()
        .expect("owner profile id")
        .to_string();
    let session_id = login["session_id"]
        .as_str()
        .expect("session id")
        .to_string();
    let initial_csrf = login["csrf_token"].as_str().expect("CSRF token");

    let managed_profile_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO profiles (
            id, home_id, user_id, profile_type, display_name, avatar_color,
            pin_hash, is_default
         ) VALUES ($1, $2, NULL, 'managed', 'Kids', '#44AA88', $3, FALSE)",
    )
    .bind(managed_profile_id.to_string())
    .bind(&home_id)
    .bind(hash_password("1234"))
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
         VALUES ($1, $2, 1)",
    )
    .bind(managed_profile_id.to_string())
    .bind(&home_id)
    .execute(&db_pool)
    .await?;

    let profiles_response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/profiles")
                .header("authorization", format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(profiles_response.status(), StatusCode::OK);
    let profiles: Value =
        serde_json::from_slice(&body::to_bytes(profiles_response.into_body(), 1_048_576).await?)?;
    let profiles = profiles["profiles"].as_array().expect("profile list");
    assert_eq!(profiles.len(), 2);
    let managed = profiles
        .iter()
        .find(|profile| profile["id"] == managed_profile_id.to_string())
        .expect("managed profile summary");
    assert_eq!(managed["profile_type"], "managed");
    assert_eq!(managed["role"], "viewer");
    assert_eq!(managed["has_pin"], true);
    assert!(managed.get("pin_hash").is_none());
    let query_profiles = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/profiles?access_token={access}")).body(Body::empty())?,
        )
        .await?;
    assert_eq!(query_profiles.status(), StatusCode::FORBIDDEN);

    let wrong_pin = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/profiles/{managed_profile_id}/select"))
                .header("authorization", format!("Bearer {access}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": "9999"}).to_string()))?,
        )
        .await?;
    assert_eq!(wrong_pin.status(), StatusCode::FORBIDDEN);
    assert!(matches!(
        revocations.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let unchanged_profile: String =
        sqlx::query_scalar("SELECT active_profile_id FROM account_sessions WHERE id = $1")
            .bind(&session_id)
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(unchanged_profile, owner_profile_id);

    let selected_response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/profiles/{managed_profile_id}/select"))
                .header("authorization", format!("Bearer {access}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": "1234"}).to_string()))?,
        )
        .await?;
    assert_eq!(selected_response.status(), StatusCode::OK);
    assert_eq!(
        selected_response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store, private")
    );
    let selected: Value =
        serde_json::from_slice(&body::to_bytes(selected_response.into_body(), 1_048_576).await?)?;
    assert_eq!(
        selected["active_profile_id"],
        managed_profile_id.to_string()
    );
    assert_eq!(selected["role"], "viewer");
    assert_eq!(selected["profile"]["profile_type"], "managed");
    assert_ne!(selected["csrf_token"], initial_csrf);
    let capabilities = selected["capabilities"]
        .as_array()
        .expect("selected capabilities");
    assert!(capabilities.contains(&json!("live_browse")));
    assert!(capabilities.contains(&json!("live_play")));
    assert!(!capabilities.contains(&json!("live_manage")));
    assert!(!capabilities.contains(&json!("extensions_manage")));
    let event_id = revocations.try_recv()?;
    let event: (String, String, String, String) = sqlx::query_as(
        "SELECT event_type, subject_type, subject_id, payload_json
         FROM authorization_revocation_outbox WHERE id = $1",
    )
    .bind(event_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(event.0, "profile_switched");
    assert_eq!(event.1, "account_session");
    assert_eq!(event.2, session_id);
    let event_payload: Value = serde_json::from_str(&event.3)?;
    assert_eq!(event_payload["previous_profile_id"], owner_profile_id);
    assert_eq!(
        event_payload["selected_profile_id"],
        managed_profile_id.to_string()
    );

    let bootstrap = app
        .clone()
        .oneshot(
            Request::get("/api/v1/auth/session")
                .header("authorization", format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(bootstrap.status(), StatusCode::OK);
    let bootstrap: Value =
        serde_json::from_slice(&body::to_bytes(bootstrap.into_body(), 1_048_576).await?)?;
    assert_eq!(
        bootstrap["active_profile_id"],
        managed_profile_id.to_string()
    );
    assert_eq!(bootstrap["role"], "viewer");

    let legacy_profile = app
        .clone()
        .oneshot(
            Request::get("/api/v1/profile/playback")
                .header("authorization", format!("Bearer {access}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(legacy_profile.status(), StatusCode::OK);

    let refreshed = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/refresh")
                .header("content-type", "application/json")
                .body(Body::from(json!({"refresh_token": refresh}).to_string()))?,
        )
        .await?;
    assert_eq!(refreshed.status(), StatusCode::OK);
    let refreshed: Value =
        serde_json::from_slice(&body::to_bytes(refreshed.into_body(), 1_048_576).await?)?;
    assert_eq!(refreshed["profile_id"], managed_profile_id.to_string());
    assert_eq!(refreshed["role"], "viewer");
    let rotated_access = refreshed["access_token"]
        .as_str()
        .expect("rotated access token");

    let selected_owner = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/profiles/{owner_profile_id}/select"))
                .header("authorization", format!("Bearer {rotated_access}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(selected_owner.status(), StatusCode::OK);
    let selected_owner: Value =
        serde_json::from_slice(&body::to_bytes(selected_owner.into_body(), 1_048_576).await?)?;
    assert_eq!(selected_owner["active_profile_id"], owner_profile_id);
    assert_eq!(selected_owner["role"], "owner");
    let _ = revocations.try_recv()?;
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM authorization_revocation_outbox
         WHERE event_type = 'profile_switched' AND account_session_id = $1",
    )
    .bind(&session_id)
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(event_count, 2);
    Ok(())
}

#[tokio::test]
async fn a15_production_qt_client_and_axum_server_complete_auth_lifecycle() -> Result<()> {
    let client_binary = std::env::var("ELIXIR_A15_CLIENT_TEST_BINARY")
        .context("ELIXIR_A15_CLIENT_TEST_BINARY must name the Qt integration test executable")?;
    anyhow::ensure!(
        std::path::Path::new(&client_binary).is_file(),
        "Qt integration test executable does not exist: {client_binary}"
    );

    let mut settings = test_settings_with_db();
    settings.database.url = format!(
        "sqlite:file:a15-auth-live-{}?mode=memory&cache=shared",
        Uuid::new_v4()
    );
    settings.database.max_connections = 4;
    settings.auth.access_token_secret =
        "a15-access-secret-000000000000000000000000000000".to_string();
    settings.auth.refresh_token_secret =
        Some("a15-refresh-secret-00000000000000000000000000000".to_string());
    settings.auth.csrf_secret = Some("a15-csrf-secret-0000000000000000000000000000000".to_string());
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

    let user_id = Uuid::new_v4();
    let email = "a15-owner@example.test";
    let password = "correct horse battery staple";
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind(email)
        .bind(hash_password(password))
        .execute(&db_pool)
        .await?;
    let owner = crate::auth::home_profiles::HomeProfileRepository::new(&db_pool)
        .ensure_owner_home(user_id)
        .await?;
    let managed_profile_id = Uuid::new_v4();
    let managed_pin = "2468";
    sqlx::query(
        "INSERT INTO profiles (
            id, home_id, user_id, profile_type, display_name, avatar_color,
            pin_hash, is_default
         ) VALUES ($1, $2, NULL, 'managed', 'A15 Kids', '#44AA88', $3, FALSE)",
    )
    .bind(managed_profile_id.to_string())
    .bind(owner.home.id.to_string())
    .bind(hash_password(managed_pin))
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
         VALUES ($1, $2, 1)",
    )
    .bind(managed_profile_id.to_string())
    .bind(owner.home.id.to_string())
    .execute(&db_pool)
    .await?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state))
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut command = tokio::process::Command::new(&client_binary);
    command
        .kill_on_drop(true)
        .env("ELIXIR_A15_SERVER_URL", format!("http://{address}"))
        .env("ELIXIR_A15_EMAIL", email)
        .env("ELIXIR_A15_PASSWORD", password)
        .env("ELIXIR_A15_OWNER_PROFILE_ID", owner.profile.id.to_string())
        .env(
            "ELIXIR_A15_MANAGED_PROFILE_ID",
            managed_profile_id.to_string(),
        )
        .env("ELIXIR_A15_MANAGED_PIN", managed_pin);
    let client_result = tokio::time::timeout(Duration::from_secs(60), command.output()).await;

    let _ = shutdown_tx.send(());
    let server_result = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .context("A15 Axum server did not shut down")??;
    server_result.context("A15 Axum server failed")?;

    let client_output = client_result
        .context("A15 Qt integration test exceeded 60 seconds")?
        .context("failed to launch A15 Qt integration test")?;
    anyhow::ensure!(
        client_output.status.success(),
        "A15 Qt integration test failed (status {}):\nstdout:\n{}\nstderr:\n{}",
        client_output.status,
        String::from_utf8_lossy(&client_output.stdout),
        String::from_utf8_lossy(&client_output.stderr)
    );

    let session: (String, Option<String>) = sqlx::query_as(
        "SELECT active_profile_id, CAST(revoked_at AS TEXT)
         FROM account_sessions
         WHERE user_id = $1
         ORDER BY created_at DESC, id DESC
         LIMIT 1",
    )
    .bind(user_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(session.0, owner.profile.id.to_string());
    assert!(
        session.1.is_some(),
        "logout must revoke the account session"
    );

    let profile_switch_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM authorization_revocation_outbox
         WHERE event_type = 'profile_switched'",
    )
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(profile_switch_events, 2);
    let logout_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM authorization_revocation_outbox
         WHERE event_type = 'account_session_revoked'
           AND reason_code = 'user_logout'",
    )
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(logout_events, 1);
    let refresh_state: (i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) AS token_count,
                SUM(CASE WHEN used_at IS NOT NULL THEN 1 ELSE 0 END) AS used_count,
                SUM(CASE WHEN revoked_at IS NOT NULL THEN 1 ELSE 0 END) AS revoked_count
         FROM refresh_tokens
         WHERE session_id IN (SELECT id FROM account_sessions WHERE user_id = $1)",
    )
    .bind(user_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(refresh_state, (3, 2, 3));
    Ok(())
}

#[tokio::test]
async fn signup_and_password_reset_flow() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.database.url = format!(
        "sqlite:file:http-signup-reset-{}?mode=memory&cache=shared",
        Uuid::new_v4()
    );
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

    let rejected_signup = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/signup")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "email": "newuser@example.com",
                        "password": "strongpassword",
                        "device_name": "x".repeat(129)
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(rejected_signup.status(), StatusCode::BAD_REQUEST);

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
    assert_eq!(
        signup_resp
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store, private")
    );
    let signup_bytes = body::to_bytes(signup_resp.into_body(), 1_048_576).await?;
    let signup_json: Value = serde_json::from_slice(&signup_bytes)?;
    assert_eq!(
        signup_status,
        StatusCode::OK,
        "signup body: {}",
        signup_json
    );
    assert!(signup_json.get("accessToken").is_some() || signup_json.get("access_token").is_some());
    let signup_refresh_token = signup_json
        .get("refresh_token")
        .and_then(Value::as_str)
        .expect("signup refresh token")
        .to_string();

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
    assert_eq!(
        reset_start_resp
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store, private")
    );
    let reset_bytes = body::to_bytes(reset_start_resp.into_body(), 1_048_576).await?;
    let reset_json: Value = serde_json::from_slice(&reset_bytes)?;
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
    let revoked_refresh = app
        .clone()
        .oneshot(
            Request::post("/api/v1/auth/refresh")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"refresh_token": signup_refresh_token}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(revoked_refresh.status(), StatusCode::UNAUTHORIZED);

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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    let source_provider_id =
        seed_acquisition_candidate_provider(&store, "elixir.sources.test_candidate_provider")
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
    assert_eq!(
        new_prefs_json
            .get("moviesSourceCandidates")
            .and_then(Value::as_array)
            .map(|value| value.len()),
        Some(1)
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
                        "animeDefaultManagerProviderId": sonarr_provider_id,
                        "moviesDefaultSourceProviderId": source_provider_id,
                        "tvDefaultSourceProviderId": source_provider_id,
                        "animeDefaultSourceProviderId": source_provider_id
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
    let source_provider_id_text = source_provider_id.to_string();
    assert_eq!(
        patch_json
            .get("preferences")
            .and_then(|value| value.get("moviesDefaultSourceProviderId"))
            .and_then(Value::as_str),
        Some(source_provider_id_text.as_str())
    );
    assert_eq!(
        patch_json
            .get("preferences")
            .and_then(|value| value.get("tvDefaultSourceProviderId"))
            .and_then(Value::as_str),
        Some(source_provider_id_text.as_str())
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    let source_provider_id =
        seed_acquisition_candidate_provider(&store, "elixir.sources.find_media_source").await?;

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
    let source_provider_id_text = source_provider_id.to_string();

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
    let source_providers = payload
        .get("sourceProviders")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(source_providers.len(), 1);
    assert_eq!(
        payload
            .get("defaultManagerProviderId")
            .and_then(Value::as_str),
        Some(sonarr_manager_provider_id_text.as_str())
    );
    assert_eq!(
        payload
            .get("defaultSourceProviderId")
            .and_then(Value::as_str),
        Some(source_provider_id_text.as_str())
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
    assert_eq!(
        targets_json
            .get("sourceCandidates")
            .and_then(Value::as_array)
            .map(|items| items.len()),
        Some(1)
    );
    assert_eq!(
        targets_json
            .get("defaultSourceProviderId")
            .and_then(Value::as_str),
        Some(source_provider_id_text.as_str())
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
async fn find_media_scope_preview_tvdb_is_read_only_and_deterministic() -> Result<()> {
    let (tvdb_base_url, fixture) = start_scope_preview_tvdb_fixture().await?;
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-scope-preview-secret".to_string();
    settings.classifier.tvdb_base_url = tvdb_base_url;
    settings.classifier.tvdb_api_key = Some("fixture-tvdb-key".to_string());
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
    let source_provider_id =
        seed_acquisition_candidate_provider(&store, "elixir.sources.scope_preview").await?;

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("scope-preview@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let media_item_id = Uuid::new_v4();
    let rich_metadata = json!({
        "overview": "Do not replace this rich local metadata",
        "poster": "https://example.invalid/poster.jpg"
    });
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year, season, episode, metadata_json, created_at, updated_at)
         VALUES ($1, 'series', $2, 'Fixture Series', 2003, NULL, NULL, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(media_item_id.to_string())
    .bind(json!({ "tvdbSeries": "321" }).to_string())
    .bind(rich_metadata.to_string())
    .execute(&db_pool)
    .await?;

    let before_targets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM acquisition_targets")
        .fetch_one(&db_pool)
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/scope-preview")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "providerId": source_provider_id,
                        "mediaType": "series",
                        "result": {
                            "kind": "series",
                            "title": "Fixture Series",
                            "year": 2003,
                            "externalIds": {
                                "tvdbSeries": "321"
                            }
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    assert_eq!(
        payload
            .get("blockers")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    assert_eq!(
        payload
            .get("capabilities")
            .and_then(|value| value.get("seasons"))
            .and_then(Value::as_bool),
        Some(true)
    );
    let seasons = payload
        .get("seasons")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(seasons.len(), 2);
    let target_keys: Vec<_> = seasons
        .iter()
        .flat_map(|season| {
            season
                .get("episodes")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        })
        .filter_map(|episode| {
            episode
                .get("targetKey")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    assert_eq!(target_keys, vec!["S01E01", "S01E02", "S02E01"]);
    assert_eq!(
        payload
            .get("media")
            .and_then(|value| value.get("externalIds"))
            .and_then(|value| value.get("tvdbSeries"))
            .and_then(Value::as_str),
        Some("321")
    );

    let after_targets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM acquisition_targets")
        .fetch_one(&db_pool)
        .await?;
    assert_eq!(after_targets, before_targets);

    let persisted_metadata: String =
        sqlx::query_scalar("SELECT CAST(metadata_json AS TEXT) FROM media_items WHERE id = $1")
            .bind(media_item_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(persisted_metadata, rich_metadata.to_string());

    fixture.abort();
    Ok(())
}

#[tokio::test]
async fn find_media_scope_preview_reports_missing_metadata_credentials() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-scope-preview-missing-secret".to_string();
    settings.classifier.tvdb_api_key = None;
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
    let source_provider_id =
        seed_acquisition_candidate_provider(&store, "elixir.sources.scope_preview_missing").await?;

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("scope-preview-missing@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let response = app
        .oneshot(
            Request::post("/api/v1/find-media/scope-preview")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "providerId": source_provider_id,
                        "mediaType": "anime",
                        "result": {
                            "kind": "anime",
                            "title": "Anime With TVDB Fallback",
                            "externalIds": {
                                "tvdbSeries": "321"
                            }
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let blocker_codes: Vec<_> = payload
        .get("blockers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|blocker| {
            blocker
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    assert_eq!(blocker_codes, vec!["missing_metadata_credentials"]);
    assert_eq!(
        payload
            .get("seasons")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    Ok(())
}

#[tokio::test]
async fn find_media_scoped_add_season_creates_only_selected_targets_and_is_idempotent() -> Result<()>
{
    let (tvdb_base_url, fixture) = start_scope_preview_tvdb_fixture().await?;
    let (state, app, token, source_provider_id) = find_media_scope_test_app(
        "find-media-scoped-add-season-secret",
        "scoped-season@example.com",
        tvdb_base_url,
        Some("fixture-tvdb-key"),
        "elixir.sources.scoped_add_season",
    )
    .await?;

    let request = json!({
        "providerId": source_provider_id,
        "mediaType": "series",
        "result": {
            "kind": "series",
            "title": "Fixture Series",
            "year": 2003,
            "externalIds": { "tvdbSeries": "321" }
        },
        "scope": {
            "type": "season",
            "seasonNumber": 2
        }
    });
    let first = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/scoped-add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))?,
        )
        .await?;
    let status = first.status();
    let first_body = body::to_bytes(first.into_body(), 1_048_576).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&first_body)
    );
    let first_payload: Value = serde_json::from_slice(&first_body)?;
    assert_eq!(first_payload["targetCount"], json!(1));
    assert_eq!(first_payload["requestMode"], json!("one_shot"));
    assert_eq!(first_payload["requestScope"], json!("season"));
    let subscription_id = first_payload["subscriptionId"]
        .as_str()
        .context("subscription id")?
        .to_string();

    let second = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/scoped-add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))?,
        )
        .await?;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = body::to_bytes(second.into_body(), 1_048_576).await?;
    let second_payload: Value = serde_json::from_slice(&second_body)?;
    assert_eq!(
        second_payload["subscriptionId"].as_str(),
        Some(subscription_id.as_str())
    );

    let target_rows = sqlx::query(
        "SELECT target_key, state FROM acquisition_targets WHERE subscription_id = $1 ORDER BY target_key",
    )
    .bind(&subscription_id)
    .fetch_all(&state.db_pool)
    .await?;
    assert_eq!(target_rows.len(), 1);
    assert_eq!(target_rows[0].try_get::<String, _>("target_key")?, "S02E01");
    assert_eq!(target_rows[0].try_get::<String, _>("state")?, "pending");

    let series_id: String =
        sqlx::query_scalar("SELECT id FROM series WHERE external_tvdb_series = '321' LIMIT 1")
            .fetch_one(&state.db_pool)
            .await?;
    let episode_slots = sqlx::query(
        "SELECT season_number, episode_number FROM episodes WHERE series_id = $1 ORDER BY season_number, episode_number",
    )
    .bind(series_id)
    .fetch_all(&state.db_pool)
    .await?;
    assert_eq!(episode_slots.len(), 1);
    assert_eq!(episode_slots[0].try_get::<i64, _>("season_number")?, 2);
    assert_eq!(episode_slots[0].try_get::<i64, _>("episode_number")?, 1);

    fixture.abort();
    Ok(())
}

#[tokio::test]
async fn find_media_scoped_add_selected_targets_preserves_rich_existing_metadata() -> Result<()> {
    let (tvdb_base_url, fixture) = start_scope_preview_tvdb_fixture().await?;
    let (state, app, token, source_provider_id) = find_media_scope_test_app(
        "find-media-scoped-add-selected-secret",
        "scoped-selected@example.com",
        tvdb_base_url,
        Some("fixture-tvdb-key"),
        "elixir.sources.scoped_add_selected",
    )
    .await?;

    let media_item_id = Uuid::new_v4();
    let rich_metadata = json!({
        "overview": "Existing rich metadata must survive a partial scoped add.",
        "poster": "https://example.invalid/rich-poster.jpg"
    });
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year, metadata_json, created_at, updated_at)
         VALUES ($1, 'series', $2, 'Fixture Series', 2003, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(media_item_id.to_string())
    .bind(json!({ "tvdbSeries": "321" }).to_string())
    .bind(rich_metadata.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO series (id, title, year, library_type, external_tvdb_series, metadata_json, created_at, updated_at)
         VALUES ($1, 'Fixture Series', 2003, 'series', '321', $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(media_item_id.to_string())
    .bind(rich_metadata.to_string())
    .execute(&state.db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/scoped-add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "providerId": source_provider_id,
                        "mediaType": "series",
                        "result": {
                            "kind": "series",
                            "title": "Fixture Series",
                            "year": 2003,
                            "externalIds": { "tvdbSeries": "321" }
                        },
                        "scope": {
                            "type": "selected_targets",
                            "targetKeys": ["S01E01", "S01E02"]
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["targetCount"], json!(2));

    let persisted_metadata: String =
        sqlx::query_scalar("SELECT CAST(metadata_json AS TEXT) FROM media_items WHERE id = $1")
            .bind(media_item_id.to_string())
            .fetch_one(&state.db_pool)
            .await?;
    assert_eq!(persisted_metadata, rich_metadata.to_string());

    let target_keys = sqlx::query_scalar::<_, String>(
        "SELECT target_key FROM acquisition_targets ORDER BY target_key",
    )
    .fetch_all(&state.db_pool)
    .await?;
    assert_eq!(target_keys, vec!["S01E01", "S01E02"]);
    let episode_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM episodes WHERE series_id = $1")
            .bind(media_item_id.to_string())
            .fetch_one(&state.db_pool)
            .await?;
    assert_eq!(episode_count, 2);
    let unselected_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM episodes WHERE series_id = $1 AND season_number = 2",
    )
    .bind(media_item_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(unselected_count, 0);

    fixture.abort();
    Ok(())
}

#[tokio::test]
async fn find_media_scoped_add_episode_singleton_queues_one_target() -> Result<()> {
    let (tvdb_base_url, fixture) = start_scope_preview_tvdb_fixture().await?;
    let (state, app, token, source_provider_id) = find_media_scope_test_app(
        "find-media-scoped-add-episode-secret",
        "scoped-episode@example.com",
        tvdb_base_url,
        Some("fixture-tvdb-key"),
        "elixir.sources.scoped_add_episode",
    )
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/scoped-add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "providerId": source_provider_id,
                        "mediaType": "series",
                        "result": {
                            "kind": "series",
                            "title": "Fixture Series",
                            "year": 2003,
                            "externalIds": { "tvdbSeries": "321" }
                        },
                        "scope": {
                            "type": "episode",
                            "seasonNumber": 1,
                            "episodeNumber": 2
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["targetCount"], json!(1));
    assert_eq!(payload["requestScope"], json!("episode"));
    let subscription_id = payload["subscriptionId"]
        .as_str()
        .context("subscription id")?;
    let target_key: String = sqlx::query_scalar(
        "SELECT target_key FROM acquisition_targets WHERE subscription_id = $1 LIMIT 1",
    )
    .bind(subscription_id)
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(target_key, "S01E02");

    fixture.abort();
    Ok(())
}

#[tokio::test]
async fn find_media_scoped_add_anime_range_uses_anime_catalog_type() -> Result<()> {
    let (tvdb_base_url, fixture) = start_scope_preview_tvdb_fixture().await?;
    let (state, app, token, source_provider_id) = find_media_scope_test_app(
        "find-media-scoped-add-anime-secret",
        "scoped-anime@example.com",
        tvdb_base_url,
        Some("fixture-tvdb-key"),
        "elixir.sources.scoped_add_anime",
    )
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/find-media/scoped-add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "providerId": source_provider_id,
                        "mediaType": "anime",
                        "result": {
                            "kind": "anime",
                            "title": "Anime With TVDB Fallback",
                            "year": 2003,
                            "externalIds": { "tvdbSeries": "321" }
                        },
                        "scope": {
                            "type": "range",
                            "seasonNumber": 1,
                            "episodeStart": 1,
                            "episodeEnd": 2
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["targetCount"], json!(2));
    assert_eq!(payload["requestScope"], json!("range"));

    let library_type: String =
        sqlx::query_scalar("SELECT library_type FROM series WHERE external_tvdb_series = '321'")
            .fetch_one(&state.db_pool)
            .await?;
    assert_eq!(library_type, "anime");
    let target_keys = sqlx::query_scalar::<_, String>(
        "SELECT target_key FROM acquisition_targets ORDER BY target_key",
    )
    .fetch_all(&state.db_pool)
    .await?;
    assert_eq!(target_keys, vec!["S01E01", "S01E02"]);

    fixture.abort();
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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

    assert_eq!(payload.get("activeCount").and_then(Value::as_u64), Some(1));
    assert_eq!(
        payload.get("downloadingCount").and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        payload.get("needsAttentionCount").and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        payload.get("recentCompletedCount").and_then(Value::as_u64),
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
    assert_eq!(
        items[0].get("title").and_then(Value::as_str),
        Some("Noble House")
    );
    assert_eq!(
        items[0].get("mediaType").and_then(Value::as_str),
        Some("tv")
    );
    assert_eq!(
        items[0].get("phase").and_then(Value::as_str),
        Some("requested")
    );
    assert_eq!(
        items[0].get("phaseLabel").and_then(Value::as_str),
        Some("Requested")
    );
    assert_eq!(
        items[0].get("headline").and_then(Value::as_str),
        Some("Waiting for manager confirmation.")
    );
    assert_eq!(
        items[0].get("stage").and_then(Value::as_str),
        Some("requested")
    );
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
async fn find_media_add_movie_uses_prefixed_manager_lookup_terms() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-add-movie-prefixed-lookup".to_string();
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("find-media-add-movie@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let (radarr_host, radarr_addr, radarr_state, shutdown_tx) = start_mock_radarr_server().await?;
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();

    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.radarr".to_string(),
            name: "Radarr".to_string(),
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
            extension_id: "elixir.modules.radarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "test-radarr-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "media.manager.movies".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("radarr".to_string()),
            scope_json: Some(json!({
                "media_types": ["movie"],
                "actions": ["add", "search", "monitor"]
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": radarr_host,
                "port": radarr_addr.port(),
                "base_path": "/"
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
                        "mediaType": "movie",
                        "managerProviderId": provider_id.to_string(),
                        "item": {
                            "title": "Scream",
                            "year": 1996,
                            "externalIds": {
                                "tmdb": "4232",
                                "imdb": "tt0117571"
                            }
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.get("managerItemId").and_then(Value::as_str),
        Some("4232")
    );
    let created_movies = radarr_state.created_movies.lock().unwrap();
    assert_eq!(created_movies.len(), 1);
    assert_eq!(
        created_movies[0]
            .get("addOptions")
            .and_then(Value::as_object)
            .and_then(|options| options.get("searchForMovie"))
            .and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        created_movies[0]
            .get("addOptions")
            .and_then(Value::as_object)
            .and_then(|options| options.get("monitor"))
            .is_none(),
        "expected Radarr addOptions.monitor to be omitted: {}",
        created_movies[0]
    );
    assert_eq!(
        created_movies[0]
            .get("tags")
            .and_then(Value::as_array)
            .cloned(),
        Some(vec![json!(1)]),
        "expected created movie to inherit the managed elixir tag: {}",
        created_movies[0]
    );

    Ok(())
}

#[tokio::test]
async fn find_media_add_clears_show_and_episode_tombstones_for_readded_show() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "find-media-add-clears-episode-blocks".to_string();
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("find-media-add-readd@example.com")
        .bind("hashed")
        .execute(&db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let (sonarr_host, sonarr_addr, shutdown_tx) = start_mock_sonarr_server().await?;
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
            scope_json: Some(json!({
                "media_types": ["series"],
                "actions": ["add", "search", "monitor"]
            })),
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
        .upsert_managed_media_tombstone(&NewManagedMediaTombstone {
            media_type: MediaType::Series,
            title: "Blocked Show".to_string(),
            normalized_title: "blockedshow".to_string(),
            year: Some(2024),
            external_ids: Some(ExternalIds {
                tvdb: Some("321".to_string()),
                tvdb_series: Some("321".to_string()),
                ..Default::default()
            }),
            manager_provider_id: Some(provider_id),
            manager_item_id: Some("42".to_string()),
            manager_label: Some("default (sonarr)".to_string()),
            manager_implementation: Some("sonarr".to_string()),
            action: "stop_tracking".to_string(),
        })
        .await?;
    for episode_number in [1, 2] {
        store
            .upsert_managed_episode_tombstone(&NewManagedEpisodeTombstone {
                media_type: MediaType::Series,
                title: "Blocked Show".to_string(),
                normalized_title: "blockedshow".to_string(),
                year: Some(2024),
                external_ids: Some(ExternalIds {
                    tvdb: Some("321".to_string()),
                    tvdb_series: Some("321".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: Some(provider_id),
                manager_item_id: Some("42".to_string()),
                manager_label: Some("default (sonarr)".to_string()),
                manager_implementation: Some("sonarr".to_string()),
                season_number: 1,
                episode_number,
                absolute_episode_number: None,
                action: "block_episode".to_string(),
            })
            .await?;
    }

    let response = app
        .oneshot(
            Request::post("/api/v1/find-media/add")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "mediaType": "tv",
                        "managerProviderId": provider_id.to_string(),
                        "item": {
                            "title": "Blocked Show",
                            "year": 2024,
                            "externalIds": {
                                "tvdbSeries": "321",
                                "tvdb": "321"
                            }
                        }
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);

    assert!(
        store
            .list_active_managed_media_tombstones()
            .await?
            .is_empty(),
        "expected title tombstone to be cleared on show re-add"
    );
    assert!(
        store
            .list_active_managed_episode_tombstones()
            .await?
            .is_empty(),
        "expected episode tombstones to be cleared on show re-add"
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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

#[tokio::test]
async fn phase25_profile_endpoint_negotiates_effective_profile_and_policy() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase25-profile-endpoint-secret".to_string();
    settings.playback.default_wan_max_bitrate_bps = Some(6_000_000);
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase25.Profile.Endpoint.mp4",
        "mp4",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get(
                "/api/v1/profile/playback?network_type=wan&profile_id=web_safari&profile_version=999&app_version=17.5",
            )
            .header("authorization", format!("Bearer {}", fixture.token))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;

    assert_eq!(
        json.pointer("/profile/profile_id").and_then(Value::as_str),
        Some("web_safari"),
        "{json}"
    );
    assert_eq!(
        json.pointer("/profile/profile_version")
            .and_then(Value::as_u64),
        Some(5),
        "{json}"
    );
    assert_eq!(
        json.pointer("/profile/max_bitrate_bps")
            .and_then(Value::as_i64),
        Some(6_000_000),
        "{json}"
    );
    assert_eq!(
        json.pointer("/negotiation/effective_profile_id")
            .and_then(Value::as_str),
        Some("web_safari"),
        "{json}"
    );
    assert!(
        json.pointer("/negotiation/compatibility")
            .and_then(Value::as_array)
            .is_some_and(|reasons| reasons.iter().any(|reason| {
                reason.as_str() == Some("requested_profile_version_capped_to_server_catalog")
            })),
        "{json}"
    );
    assert_eq!(
        json.pointer("/effective_policy/network_class")
            .and_then(Value::as_str),
        Some("wan"),
        "{json}"
    );
    assert_eq!(
        json.pointer("/effective_policy/max_bitrate_bps")
            .and_then(Value::as_i64),
        Some(6_000_000),
        "{json}"
    );
    assert!(
        json.pointer("/catalog")
            .and_then(Value::as_array)
            .is_some_and(|catalog| catalog.iter().any(|entry| {
                entry.get("id").and_then(Value::as_str) == Some("native_mpv_desktop")
            }) && catalog.iter().any(|entry| {
                entry.get("id").and_then(Value::as_str) == Some("game_console_conservative")
            })),
        "{json}"
    );

    Ok(())
}

#[tokio::test]
async fn phase25_profile_fixtures_drive_planner_modes_and_unknown_spoof_is_safe() -> Result<()> {
    let mut direct_stream_settings = test_settings_with_db();
    direct_stream_settings.auth.access_token_secret =
        "phase25-direct-stream-profile-secret".to_string();
    direct_stream_settings.playback.allow_direct_stream = true;
    direct_stream_settings.playback.hls_direct_stream_enabled = true;
    let direct_stream_fixture = setup_playback_route_fixture(
        direct_stream_settings,
        "Phase25.Profile.DirectStream.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let native_body = json!({
        "media_item_id": direct_stream_fixture.item_id,
        "preferred_file_id": direct_stream_fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "profile_id": "native_mpv_desktop",
            "profile_version": 5,
            "app_version": "0.1.0",
            "quality_mode": "original"
        }
    });
    let native_json = play_fixture_json(&direct_stream_fixture, native_body).await?;
    assert_eq!(
        native_json.get("mode").and_then(Value::as_str),
        Some("direct_play"),
        "{native_json}"
    );
    assert_eq!(
        native_json
            .pointer("/playback_plan/client_profile/profile_id")
            .and_then(Value::as_str),
        Some("native_mpv_desktop"),
        "{native_json}"
    );

    let spoofed_unknown_body = json!({
        "media_item_id": direct_stream_fixture.item_id,
        "preferred_file_id": direct_stream_fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "profile_id": "totally-unknown-player",
            "profile_version": 5,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv", "mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supported_hls_segment_types": ["fmp4"],
            "quality_mode": "original"
        }
    });
    let unknown_json = play_fixture_json(&direct_stream_fixture, spoofed_unknown_body).await?;
    assert_eq!(
        unknown_json.get("mode").and_then(Value::as_str),
        Some("direct_stream"),
        "{unknown_json}"
    );
    assert_eq!(
        unknown_json
            .pointer("/playback_plan/client_profile/profile_id")
            .and_then(Value::as_str),
        Some("web_chromium"),
        "{unknown_json}"
    );
    assert_eq!(
        unknown_json
            .pointer("/playback_plan/client_profile/direct_play_preferred")
            .and_then(Value::as_bool),
        Some(false),
        "{unknown_json}"
    );
    assert_eq!(
        unknown_json
            .pointer("/playback_plan/client_profile/supported_containers/0")
            .and_then(Value::as_str),
        Some("mp4"),
        "{unknown_json}"
    );

    let mut transcode_settings = test_settings_with_db();
    transcode_settings.auth.access_token_secret = "phase25-transcode-profile-secret".to_string();
    transcode_settings.playback.allow_direct_stream = true;
    transcode_settings.playback.allow_video_transcode = true;
    transcode_settings.playback.hls_direct_stream_enabled = true;
    transcode_settings.playback.unknown_performance_policy = "allow_best_effort".to_string();
    let transcode_fixture = setup_playback_route_fixture(
        transcode_settings,
        "Phase25.Profile.VideoTranscode.mkv",
        "mkv",
        "hevc",
        "aac",
        1920,
        1080,
        8_000_000,
    )
    .await?;
    let native_hevc = play_fixture_json(
        &transcode_fixture,
        json!({
            "media_item_id": transcode_fixture.item_id,
            "preferred_file_id": transcode_fixture.media_file_id,
            "network_type": "lan",
            "client_capabilities": {
                "profile_id": "native_mpv_desktop",
                "profile_version": 5,
                "quality_mode": "original"
            }
        }),
    )
    .await?;
    assert_eq!(
        native_hevc.get("mode").and_then(Value::as_str),
        Some("direct_play"),
        "{native_hevc}"
    );
    let browser_hevc = play_fixture_json(
        &transcode_fixture,
        json!({
            "media_item_id": transcode_fixture.item_id,
            "preferred_file_id": transcode_fixture.media_file_id,
            "network_type": "lan",
            "client_capabilities": {
                "profile_id": "web_chromium",
                "profile_version": 5,
                "quality_mode": "fixed",
                "fixed_bitrate_bps": 4_000_000,
                "fixed_resolution": "720p"
            }
        }),
    )
    .await?;
    assert_eq!(
        browser_hevc.get("mode").and_then(Value::as_str),
        Some("video_transcode"),
        "{browser_hevc}"
    );
    assert_eq!(
        browser_hevc
            .pointer("/playback_plan/client_profile/fixed_resolution")
            .and_then(Value::as_str),
        Some("720p"),
        "{browser_hevc}"
    );
    assert_eq!(
        browser_hevc
            .pointer("/playback_plan/effective_policy/fixed_bitrate_bps")
            .and_then(Value::as_i64),
        Some(4_000_000),
        "{browser_hevc}"
    );

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
async fn library_items_include_managed_card_lifecycle() -> Result<()> {
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
    let series_id = Uuid::new_v4();

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
                "host": "elx-sonarr",
                "port": 8989,
                "base_path": "/"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    sqlx::query(
        "INSERT INTO series (
            id, title, year, library_type, external_imdb, metadata_json
         ) VALUES ($1, $2, $3, $4, $5, NULL)",
    )
    .bind(series_id.to_string())
    .bind("Managed Show")
    .bind(2024)
    .bind("series")
    .bind("tt1234567")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(series_id.to_string())
    .bind("series")
    .bind(serde_json::to_string(&json!({ "imdb": "tt1234567" }))?)
    .bind("Managed Show")
    .bind(2024)
    .execute(&db_pool)
    .await?;

    store
        .upsert_managed_library_provenance(&NewManagedLibraryProvenance {
            media_item_id: series_id,
            media_type: MediaType::Series,
            title: "Managed Show".to_string(),
            normalized_title: "managedshow".to_string(),
            year: Some(2024),
            external_ids: Some(ExternalIds {
                imdb: Some("tt1234567".to_string()),
                ..Default::default()
            }),
            manager_provider_id: provider_id,
            manager_item_id: Some("42".to_string()),
            manager_label: Some("default (sonarr)".to_string()),
            manager_implementation: Some("sonarr".to_string()),
            intent_id: None,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/library/items").body(Body::empty())?)
        .await?;
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(status, StatusCode::OK, "body: {}", payload);

    let series_id_text = series_id.to_string();
    let item = payload
        .as_array()
        .and_then(|items| {
            items.iter().find(|entry| {
                entry.get("id").and_then(Value::as_str) == Some(series_id_text.as_str())
            })
        })
        .context("managed show list item")?;
    let lifecycle = item
        .get("lifecycle")
        .context("library item lifecycle payload")?;
    assert_eq!(
        lifecycle.get("trackedByManager").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        lifecycle.get("canStopTracking").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        lifecycle.get("managerLabel").and_then(Value::as_str),
        Some("Sonarr")
    );
    let primary_owner = lifecycle
        .get("primaryOwner")
        .context("library item primary owner")?;
    assert_eq!(
        primary_owner.get("ownerType").and_then(Value::as_str),
        Some("extension")
    );
    assert_eq!(
        primary_owner
            .get("ownerImplementation")
            .and_then(Value::as_str),
        Some("sonarr")
    );
    assert_eq!(
        primary_owner.get("ownerExternalId").and_then(Value::as_str),
        Some("42")
    );
    assert_eq!(
        primary_owner
            .get("releaseCapability")
            .and_then(Value::as_str),
        Some("manager.remove_item")
    );

    Ok(())
}

#[tokio::test]
async fn library_items_project_continue_watching_from_user_media_state() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "library-continue-state-secret-key".to_string();
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

    let user_one_id = Uuid::new_v4();
    let user_two_id = Uuid::new_v4();
    for (user_id, email) in [
        (user_one_id, "continue-one@example.com"),
        (user_two_id, "continue-two@example.com"),
    ] {
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(user_id.to_string())
            .bind(email)
            .bind("hashed")
            .execute(&db_pool)
            .await?;
    }
    let user_one_token = state.auth_service.issue_access_token(user_one_id)?.token;
    let user_two_token = state.auth_service.issue_access_token(user_two_id)?.token;

    let movie_id = Uuid::new_v4();
    let watched_movie_id = Uuid::new_v4();
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_id = Uuid::new_v4();
    let movie_id_text = movie_id.to_string();
    let watched_movie_id_text = watched_movie_id.to_string();
    let series_id_text = series_id.to_string();
    let episode_id_text = episode_id.to_string();
    sqlx::query(
        "INSERT INTO movies (id, title, year, runtime_seconds, metadata_json)
         VALUES ($1, 'Continue Movie', 2026, 2400, NULL)",
    )
    .bind(movie_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO movies (id, title, year, runtime_seconds, metadata_json)
         VALUES ($1, 'Watched Movie', 2026, 3600, NULL)",
    )
    .bind(watched_movie_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query("INSERT INTO series (id, title, year, library_type) VALUES ($1, $2, $3, $4)")
        .bind(series_id.to_string())
        .bind("Continue Show")
        .bind(2026)
        .bind("series")
        .execute(&db_pool)
        .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 1, $3)")
        .bind(season_id.to_string())
        .bind(series_id.to_string())
        .bind("Season 1")
        .execute(&db_pool)
        .await?;
    sqlx::query(
        "INSERT INTO episodes (
            id, series_id, season_id, season_number, episode_number, title,
            runtime_seconds, has_file
         ) VALUES ($1, $2, $3, 1, 1, 'Pilot', 1500, 1)",
    )
    .bind(episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .execute(&db_pool)
    .await?;

    sqlx::query(
        "INSERT INTO user_media_state (
            user_id, item_type, item_id, resume_seconds, duration_seconds,
            watched, watched_at, play_count, last_played_at, state_source
         ) VALUES ($1, 'movie', $2, 600, 2400, 0, NULL, 0,
             '2026-07-04 10:00:00', 'playback')",
    )
    .bind(user_one_id.to_string())
    .bind(movie_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO user_media_state (
            user_id, item_type, item_id, resume_seconds, duration_seconds,
            watched, watched_at, play_count, last_played_at, state_source
         ) VALUES ($1, 'movie', $2, 0, 3600, 1, '2026-07-04 09:00:00', 1,
             '2026-07-04 09:00:00', 'manual')",
    )
    .bind(user_one_id.to_string())
    .bind(watched_movie_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO user_media_state (
            user_id, item_type, item_id, series_id, season_id, resume_seconds,
            duration_seconds, watched, watched_at, play_count, last_played_at, state_source
         ) VALUES ($1, 'episode', $2, $3, $4, 300, 1500, 0, NULL, 0,
             '2026-07-04 11:00:00', 'playback')",
    )
    .bind(user_one_id.to_string())
    .bind(episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO user_media_state (
            user_id, item_type, item_id, resume_seconds, duration_seconds,
            watched, watched_at, play_count, last_played_at, state_source
         ) VALUES ($1, 'movie', $2, 0, 2400, 1, '2026-07-04 12:00:00', 1,
             '2026-07-04 12:00:00', 'manual')",
    )
    .bind(user_two_id.to_string())
    .bind(movie_id.to_string())
    .execute(&db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/library/items")
                .header("authorization", format!("Bearer {user_one_token}"))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(status, StatusCode::OK, "body: {}", payload);
    let items = payload.as_array().expect("library items");
    let movie = items
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(movie_id_text.as_str()))
        .context("continue movie item")?;
    let watched_movie = items
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(watched_movie_id_text.as_str()))
        .context("watched movie item")?;
    let series = items
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(series_id_text.as_str()))
        .context("continue series item")?;
    assert!(
        (movie
            .get("progress")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            - 0.25)
            .abs()
            < 0.001,
        "expected movie progress from user_media_state: {movie}"
    );
    assert_eq!(
        movie
            .pointer("/playback_state/item_type")
            .and_then(Value::as_str),
        Some("movie")
    );
    assert_eq!(
        movie
            .pointer("/playback_state/last_played_at")
            .and_then(Value::as_str),
        Some("2026-07-04 10:00:00")
    );
    assert_eq!(
        watched_movie.get("progress").and_then(Value::as_f64),
        Some(0.0)
    );
    assert!(
        watched_movie.get("playback_state").is_none(),
        "watched rows must not appear as Continue Watching state: {watched_movie}"
    );
    assert!(
        (series
            .get("progress")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            - 0.2)
            .abs()
            < 0.001,
        "expected series progress from latest resumable episode state: {series}"
    );
    assert_eq!(
        series
            .pointer("/playback_state/item_type")
            .and_then(Value::as_str),
        Some("episode")
    );
    assert_eq!(
        series
            .pointer("/playback_state/item_id")
            .and_then(Value::as_str),
        Some(episode_id_text.as_str())
    );

    let other_response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/library/items")
                .header("authorization", format!("Bearer {user_two_token}"))
                .body(Body::empty())?,
        )
        .await?;
    let other_status = other_response.status();
    let other_payload: Value =
        serde_json::from_slice(&body::to_bytes(other_response.into_body(), usize::MAX).await?)?;
    assert_eq!(other_status, StatusCode::OK, "body: {}", other_payload);
    let other_movie = other_payload
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("id").and_then(Value::as_str) == Some(movie_id_text.as_str()))
        })
        .context("other user's movie item")?;
    assert_eq!(
        other_movie.get("progress").and_then(Value::as_f64),
        Some(0.0)
    );
    assert!(
        other_movie.get("playback_state").is_none(),
        "another user's watched state must not leak resumable progress: {other_movie}"
    );

    let anonymous_response = app
        .oneshot(Request::get("/api/v1/library/items").body(Body::empty())?)
        .await?;
    let anonymous_status = anonymous_response.status();
    let anonymous_payload: Value =
        serde_json::from_slice(&body::to_bytes(anonymous_response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        anonymous_status,
        StatusCode::OK,
        "body: {}",
        anonymous_payload
    );
    assert!(
        anonymous_payload
            .as_array()
            .expect("anonymous library items")
            .iter()
            .all(|item| item.get("playback_state").is_none()
                && item.get("progress").and_then(Value::as_f64) == Some(0.0)),
        "anonymous library items must not include per-user continue state: {anonymous_payload}"
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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
        sqlx::query_scalar("SELECT id FROM media_files WHERE path = $1 LIMIT 1")
            .bind(media_path.to_string_lossy().to_string())
            .fetch_one(&state.db_pool)
            .await?;

    let review_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO review_queue (id, media_file_id, status, confidence, hint_json, candidates_json) VALUES ($1, $2, 'pending', $3, $4, $5)",
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

    let status: String =
        sqlx::query_scalar("SELECT status FROM review_queue WHERE id = $1 LIMIT 1")
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
async fn media_segment_candidate_routes_activate_and_disable_segments() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "segment-secret-key".to_string();
    settings.media_interactions.support_api_enabled = true;
    let fixture = setup_playback_route_fixture(
        settings,
        "Segment.Movie.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let candidate_body = json!({
        "mediaFileId": fixture.media_file_id,
        "segmentType": "intro",
        "startSeconds": 0.0,
        "endSeconds": 45.0,
        "providerKind": "chapter",
        "providerId": "ffprobe_chapters",
        "providerVersion": "1",
        "confidence": 0.9,
        "identityStrength": "file_fingerprint",
        "sourcePayload": {
            "title": "Intro"
        }
    });
    let create_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segments/candidates")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(candidate_body.to_string()))?,
        )
        .await?;
    let create_status = create_resp.status();
    let create_bytes = body::to_bytes(create_resp.into_body(), 1_048_576).await?;
    let create_json: Value = serde_json::from_slice(&create_bytes)?;
    assert_eq!(create_status, StatusCode::OK, "body: {}", create_json);
    assert_eq!(
        create_json
            .pointer("/candidate/validation_state")
            .and_then(Value::as_str),
        Some("accepted")
    );
    let segment_id = create_json
        .pointer("/activated_segment/id")
        .and_then(Value::as_str)
        .context("candidate should activate a canonical segment")?
        .to_string();

    let manual_candidate_body = json!({
        "mediaFileId": fixture.media_file_id,
        "itemType": "movie",
        "itemId": fixture.item_id,
        "segmentType": "intro",
        "startSeconds": 2.0,
        "endSeconds": 50.0,
        "providerKind": "manual",
        "providerId": "support-console",
        "confidence": 1.0,
        "identityStrength": "manual",
        "sourcePayload": {
            "title": "Manual intro"
        }
    });
    let manual_create_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segments/candidates")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(manual_candidate_body.to_string()))?,
        )
        .await?;
    let manual_create_status = manual_create_resp.status();
    let manual_create_bytes = body::to_bytes(manual_create_resp.into_body(), 1_048_576).await?;
    let manual_create_json: Value = serde_json::from_slice(&manual_create_bytes)?;
    assert_eq!(
        manual_create_status,
        StatusCode::BAD_REQUEST,
        "body: {}",
        manual_create_json
    );
    let manual_candidate_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM media_segment_candidates
         WHERE media_file_id = $1
           AND (provider_kind = 'manual' OR identity_strength = 'manual')",
    )
    .bind(&fixture.media_file_id)
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(
        manual_candidate_count, 0,
        "support APIs must not create manual locked markers"
    );

    let low_confidence_body = json!({
        "mediaFileId": fixture.media_file_id,
        "segmentType": "recap",
        "startSeconds": 12.0,
        "endSeconds": 30.0,
        "providerKind": "chapter",
        "providerId": "ffprobe_chapters_low_confidence",
        "providerVersion": "1",
        "confidence": 0.10,
        "identityStrength": "file_fingerprint",
        "sourcePayload": {
            "title": "Previously On"
        }
    });
    let low_confidence_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segments/candidates")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(low_confidence_body.to_string()))?,
        )
        .await?;
    let low_confidence_status = low_confidence_resp.status();
    let low_confidence_bytes = body::to_bytes(low_confidence_resp.into_body(), 1_048_576).await?;
    let low_confidence_json: Value = serde_json::from_slice(&low_confidence_bytes)?;
    assert_eq!(
        low_confidence_status,
        StatusCode::OK,
        "body: {}",
        low_confidence_json
    );
    assert_eq!(
        low_confidence_json
            .pointer("/candidate/validation_reason")
            .and_then(Value::as_str),
        Some("confidence_below_threshold")
    );

    let review_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/media-segments/candidates?lowConfidence=true&providerKind=chapter&mediaFileId={}",
                fixture.media_file_id
            ))
            .header("authorization", format!("Bearer {}", fixture.token))
            .body(Body::empty())?,
        )
        .await?;
    let review_status = review_resp.status();
    let review_bytes = body::to_bytes(review_resp.into_body(), 1_048_576).await?;
    let review_json: Value = serde_json::from_slice(&review_bytes)?;
    assert_eq!(review_status, StatusCode::OK, "body: {}", review_json);
    assert_eq!(
        review_json
            .pointer("/candidates/0/validation_reason")
            .and_then(Value::as_str),
        Some("confidence_below_threshold")
    );
    assert_eq!(
        review_json
            .pointer("/candidates/0/segment_type")
            .and_then(Value::as_str),
        Some("recap")
    );
    assert!(
        review_json
            .pointer("/candidates/0/created_at")
            .and_then(Value::as_str)
            .is_some(),
        "review candidates should expose timestamps: {review_json}"
    );

    let list_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/media-files/{}/media-segments",
                fixture.media_file_id
            ))
            .header("authorization", format!("Bearer {}", fixture.token))
            .body(Body::empty())?,
        )
        .await?;
    let list_status = list_resp.status();
    let list_bytes = body::to_bytes(list_resp.into_body(), 1_048_576).await?;
    let list_json: Value = serde_json::from_slice(&list_bytes)?;
    assert_eq!(list_status, StatusCode::OK, "body: {}", list_json);
    assert_eq!(
        list_json
            .pointer("/active/0/segment_type")
            .and_then(Value::as_str),
        Some("intro")
    );

    let item_list_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/items/movie/{}/segments", fixture.item_id))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let item_list_status = item_list_resp.status();
    let item_list_bytes = body::to_bytes(item_list_resp.into_body(), 1_048_576).await?;
    let item_list_json: Value = serde_json::from_slice(&item_list_bytes)?;
    assert_eq!(item_list_status, StatusCode::OK, "body: {}", item_list_json);
    assert_eq!(
        item_list_json
            .pointer("/active/0/segment_type")
            .and_then(Value::as_str),
        Some("intro")
    );
    assert_eq!(
        item_list_json
            .pointer("/candidates/0/validation_state")
            .and_then(Value::as_str),
        Some("accepted")
    );

    let disable_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/media-segments/{segment_id}/disable"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(json!({"reason": "bad_marker"}).to_string()))?,
        )
        .await?;
    let disable_status = disable_resp.status();
    let disable_bytes = body::to_bytes(disable_resp.into_body(), 1_048_576).await?;
    let disable_json: Value = serde_json::from_slice(&disable_bytes)?;
    assert_eq!(disable_status, StatusCode::OK, "body: {}", disable_json);
    assert_eq!(
        disable_json.get("disabled").and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        disable_json
            .get("active")
            .and_then(Value::as_array)
            .is_some_and(|active| active.is_empty()),
        "disabled segment should leave no active segment for this file: {disable_json}"
    );

    Ok(())
}

#[tokio::test]
async fn false_positive_detector_segment_disable_is_support_only_and_removes_skip_metadata()
-> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "detector-false-positive-secret-key".to_string();
    settings.media_interactions.support_api_enabled = true;
    let fixture = setup_playback_route_fixture(
        settings,
        "Detector.False.Positive.Movie.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let candidate_body = json!({
        "mediaFileId": fixture.media_file_id,
        "segmentType": "credits",
        "startSeconds": 35.0,
        "endSeconds": 55.0,
        "providerKind": "local_visual_recurring",
        "providerId": "local_visual_recurring",
        "providerVersion": "local_visual_credits_v1",
        "confidence": 0.95,
        "identityStrength": "file_fingerprint",
        "sourcePayload": {
            "detector": "local_visual_recurring",
            "release_gate": "false_positive_disable"
        }
    });
    let create_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segments/candidates")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(candidate_body.to_string()))?,
        )
        .await?;
    let create_status = create_resp.status();
    let create_bytes = body::to_bytes(create_resp.into_body(), 1_048_576).await?;
    let create_json: Value = serde_json::from_slice(&create_bytes)?;
    assert_eq!(create_status, StatusCode::OK, "body: {}", create_json);
    assert_eq!(
        create_json
            .pointer("/candidate/provider_kind")
            .and_then(Value::as_str),
        Some("local_visual_recurring")
    );
    assert_eq!(
        create_json
            .pointer("/candidate/validation_state")
            .and_then(Value::as_str),
        Some("accepted")
    );
    let segment_id = create_json
        .pointer("/activated_segment/id")
        .and_then(Value::as_str)
        .context("detector candidate should activate a canonical segment")?
        .to_string();

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"]
        }
    });
    let play_json = play_fixture_json(&fixture, play_body.clone()).await?;
    assert_eq!(
        play_json
            .pointer("/segments/0/segment_type")
            .and_then(Value::as_str),
        Some("credits")
    );
    let session_id = play_json
        .get("session_id")
        .and_then(Value::as_str)
        .context("play response should include session_id")?;

    let progress_body = json!({
        "positionSeconds": 40.0,
        "durationSeconds": 60,
        "eventType": "progress"
    });
    let progress_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/progress"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(progress_body.to_string()))?,
        )
        .await?;
    let progress_status = progress_resp.status();
    let progress_bytes = body::to_bytes(progress_resp.into_body(), 1_048_576).await?;
    let progress_json: Value = serde_json::from_slice(&progress_bytes)?;
    assert_eq!(progress_status, StatusCode::OK, "body: {}", progress_json);
    assert_eq!(
        progress_json
            .pointer("/active_segment/segment_type")
            .and_then(Value::as_str),
        Some("credits")
    );

    let disable_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/media-segments/{segment_id}/disable"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"reason": "false_positive_detector_marker"}).to_string(),
                ))?,
        )
        .await?;
    let disable_status = disable_resp.status();
    let disable_bytes = body::to_bytes(disable_resp.into_body(), 1_048_576).await?;
    let disable_json: Value = serde_json::from_slice(&disable_bytes)?;
    assert_eq!(disable_status, StatusCode::OK, "body: {}", disable_json);
    assert_eq!(
        disable_json.get("disabled").and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        disable_json
            .get("active")
            .and_then(Value::as_array)
            .is_some_and(|active| active.is_empty()),
        "disabled false-positive segment should leave no active segment: {disable_json}"
    );

    let (candidate_state, candidate_reason): (String, Option<String>) = sqlx::query_as(
        "SELECT validation_state, validation_reason
         FROM media_segment_candidates
         WHERE media_file_id = $1
           AND provider_kind = 'local_visual_recurring'
         LIMIT 1",
    )
    .bind(&fixture.media_file_id)
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(candidate_state, "rejected");
    assert_eq!(
        candidate_reason.as_deref(),
        Some("false_positive_detector_marker")
    );

    let progress_after_disable_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/progress"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(progress_body.to_string()))?,
        )
        .await?;
    let progress_after_disable_status = progress_after_disable_resp.status();
    let progress_after_disable_bytes =
        body::to_bytes(progress_after_disable_resp.into_body(), 1_048_576).await?;
    let progress_after_disable_json: Value = serde_json::from_slice(&progress_after_disable_bytes)?;
    assert_eq!(
        progress_after_disable_status,
        StatusCode::OK,
        "body: {}",
        progress_after_disable_json
    );
    assert!(
        progress_after_disable_json
            .get("active_segment")
            .is_some_and(Value::is_null),
        "disabled detector marker must not surface as an active segment: {progress_after_disable_json}"
    );

    let replay_json = play_fixture_json(&fixture, play_body).await?;
    assert!(
        replay_json
            .get("segments")
            .and_then(Value::as_array)
            .is_some_and(|segments| segments.is_empty()),
        "disabled detector marker must not be returned to normal playback: {replay_json}"
    );

    Ok(())
}

#[tokio::test]
async fn media_segment_job_routes_enqueue_and_list_supported_jobs() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "segment-job-secret-key".to_string();
    settings.media_interactions.support_api_enabled = true;
    let fixture = setup_playback_route_fixture(
        settings,
        "Segment.Job.Movie.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let enqueue_body = json!({
        "jobType": "provider_refresh",
        "scopeType": "media_file",
        "scopeId": fixture.media_file_id,
        "providerKind": "theintrodb",
        "priority": 25
    });
    let enqueue_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segment-jobs")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(enqueue_body.to_string()))?,
        )
        .await?;
    let enqueue_status = enqueue_resp.status();
    let enqueue_bytes = body::to_bytes(enqueue_resp.into_body(), 1_048_576).await?;
    let enqueue_json: Value = serde_json::from_slice(&enqueue_bytes)?;
    assert_eq!(enqueue_status, StatusCode::OK, "body: {}", enqueue_json);
    assert_eq!(
        enqueue_json
            .pointer("/job/job_type")
            .and_then(Value::as_str),
        Some("provider_refresh")
    );
    assert_eq!(
        enqueue_json.pointer("/job/status").and_then(Value::as_str),
        Some("queued")
    );
    let provider_job_id = enqueue_json
        .pointer("/job/id")
        .and_then(Value::as_str)
        .context("provider refresh job id")?
        .to_string();

    let list_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/api/v1/media-segment-jobs?providerKind=theintrodb&status=queued")
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let list_status = list_resp.status();
    let list_bytes = body::to_bytes(list_resp.into_body(), 1_048_576).await?;
    let list_json: Value = serde_json::from_slice(&list_bytes)?;
    assert_eq!(list_status, StatusCode::OK, "body: {}", list_json);
    assert_eq!(
        list_json
            .pointer("/jobs/0/provider_kind")
            .and_then(Value::as_str),
        Some("theintrodb")
    );
    assert_eq!(
        list_json
            .pointer("/jobs/0/scope_id")
            .and_then(Value::as_str),
        Some(fixture.media_file_id.as_str())
    );

    let cancel_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/media-segment-jobs/{provider_job_id}/cancel"
            ))
            .header("authorization", format!("Bearer {}", fixture.token))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"reason": "operator paused provider refresh"}).to_string(),
            ))?,
        )
        .await?;
    let cancel_status = cancel_resp.status();
    let cancel_bytes = body::to_bytes(cancel_resp.into_body(), 1_048_576).await?;
    let cancel_json: Value = serde_json::from_slice(&cancel_bytes)?;
    assert_eq!(cancel_status, StatusCode::OK, "body: {}", cancel_json);
    assert_eq!(
        cancel_json.pointer("/job/status").and_then(Value::as_str),
        Some("cancelled")
    );
    assert_eq!(
        cancel_json
            .pointer("/job/error/reason")
            .and_then(Value::as_str),
        Some("admin_cancelled")
    );

    let retry_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/media-segment-jobs/{provider_job_id}/retry"
            ))
            .header("authorization", format!("Bearer {}", fixture.token))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"reason": "operator resumed provider refresh"}).to_string(),
            ))?,
        )
        .await?;
    let retry_status = retry_resp.status();
    let retry_bytes = body::to_bytes(retry_resp.into_body(), 1_048_576).await?;
    let retry_json: Value = serde_json::from_slice(&retry_bytes)?;
    assert_eq!(retry_status, StatusCode::OK, "body: {}", retry_json);
    assert_eq!(
        retry_json.pointer("/job/status").and_then(Value::as_str),
        Some("queued")
    );
    assert_eq!(
        retry_json.pointer("/job/attempts").and_then(Value::as_i64),
        Some(0)
    );
    assert_eq!(
        retry_json
            .pointer("/job/error/reason")
            .and_then(Value::as_str),
        Some("admin_retry")
    );
    assert_eq!(
        retry_json
            .pointer("/job/error/previous_status")
            .and_then(Value::as_str),
        Some("cancelled")
    );

    let visual_hash_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segment-jobs")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "jobType": "video_frame_hash",
                        "scopeType": "media_file",
                        "scopeId": fixture.media_file_id,
                        "providerKind": "local_visual_recurring",
                        "priority": 45
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let visual_hash_status = visual_hash_resp.status();
    let visual_hash_bytes = body::to_bytes(visual_hash_resp.into_body(), 1_048_576).await?;
    let visual_hash_json: Value = serde_json::from_slice(&visual_hash_bytes)?;
    assert_eq!(
        visual_hash_status,
        StatusCode::OK,
        "body: {}",
        visual_hash_json
    );
    assert_eq!(
        visual_hash_json
            .pointer("/job/job_type")
            .and_then(Value::as_str),
        Some("video_frame_hash")
    );
    assert_eq!(
        visual_hash_json
            .pointer("/job/provider_kind")
            .and_then(Value::as_str),
        Some("local_visual_recurring")
    );

    let local_visual_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segment-jobs")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "jobType": "local_detector",
                        "scopeType": "media_file",
                        "scopeId": fixture.media_file_id,
                        "providerKind": "local_visual_recurring",
                        "priority": 50
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    let local_visual_status = local_visual_resp.status();
    let local_visual_bytes = body::to_bytes(local_visual_resp.into_body(), 1_048_576).await?;
    let local_visual_json: Value = serde_json::from_slice(&local_visual_bytes)?;
    assert_eq!(
        local_visual_status,
        StatusCode::OK,
        "body: {}",
        local_visual_json
    );
    assert_eq!(
        local_visual_json
            .pointer("/job/job_type")
            .and_then(Value::as_str),
        Some("local_detector")
    );
    assert_eq!(
        local_visual_json
            .pointer("/job/provider_kind")
            .and_then(Value::as_str),
        Some("local_visual_recurring")
    );

    let unsupported_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segment-jobs")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "jobType": "provider_refresh",
                        "scopeType": "media_file",
                        "scopeId": fixture.media_file_id,
                        "providerKind": "local_visual_recurring"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(unsupported_resp.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test]
async fn media_segment_item_analyze_route_enqueues_supported_jobs() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "segment-item-analyze-secret-key".to_string();
    settings.media_interactions.support_api_enabled = true;
    let fixture = setup_playback_route_fixture(
        settings,
        "Segment.Item.Analyze.Movie.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/items/movie/{}/media-segment-jobs/analyze",
                fixture.item_id
            ))
            .header("authorization", format!("Bearer {}", fixture.token))
            .header("content-type", "application/json")
            .body(Body::from(json!({"force": true}).to_string()))?,
        )
        .await?;
    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), 1_048_576).await?;
    let response_json: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(status, StatusCode::OK, "body: {}", response_json);
    assert_eq!(
        response_json
            .pointer("/summary/item_type")
            .and_then(Value::as_str),
        Some("movie")
    );
    assert_eq!(
        response_json
            .pointer("/summary/media_files_seen")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        response_json
            .pointer("/summary/jobs/0/job_type")
            .and_then(Value::as_str),
        Some("provider_refresh")
    );
    assert_eq!(
        response_json
            .pointer("/summary/jobs/0/provider_kind")
            .and_then(Value::as_str),
        Some("theintrodb")
    );
    assert_eq!(
        response_json
            .pointer("/summary/jobs/0/scope_id")
            .and_then(Value::as_str),
        Some(fixture.media_file_id.as_str())
    );
    assert!(
        response_json
            .pointer("/summary/failures")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty),
        "item analysis should not report failures: {response_json}"
    );

    Ok(())
}

#[tokio::test]
async fn media_segment_support_routes_are_disabled_by_default() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "segment-support-gate-secret-key".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "Segment.Support.Gate.Movie.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let support_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/media-files/{}/media-segments",
                fixture.media_file_id
            ))
            .header("authorization", format!("Bearer {}", fixture.token))
            .body(Body::empty())?,
        )
        .await?;
    let support_status = support_resp.status();
    let support_bytes = body::to_bytes(support_resp.into_body(), 1_048_576).await?;
    let support_json: Value = serde_json::from_slice(&support_bytes)?;
    assert_eq!(
        support_status,
        StatusCode::FORBIDDEN,
        "body: {}",
        support_json
    );

    let disable_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/media-segments/{}/disable", Uuid::new_v4()))
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(json!({"reason": "bad_marker"}).to_string()))?,
        )
        .await?;
    let disable_status = disable_resp.status();
    let disable_bytes = body::to_bytes(disable_resp.into_body(), 1_048_576).await?;
    let disable_json: Value = serde_json::from_slice(&disable_bytes)?;
    assert_eq!(
        disable_status,
        StatusCode::FORBIDDEN,
        "body: {}",
        disable_json
    );

    let candidate_body = json!({
        "mediaFileId": fixture.media_file_id,
        "segmentType": "intro",
        "startSeconds": 0.0,
        "endSeconds": 45.0,
        "providerKind": "chapter",
        "providerId": "ffprobe_chapters",
        "confidence": 0.9,
        "identityStrength": "file_fingerprint",
        "sourcePayload": {
            "title": "Intro"
        }
    });
    let candidate_create_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segments/candidates")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(candidate_body.to_string()))?,
        )
        .await?;
    let candidate_create_status = candidate_create_resp.status();
    let candidate_create_bytes =
        body::to_bytes(candidate_create_resp.into_body(), 1_048_576).await?;
    let candidate_create_json: Value = serde_json::from_slice(&candidate_create_bytes)?;
    assert_eq!(
        candidate_create_status,
        StatusCode::FORBIDDEN,
        "body: {}",
        candidate_create_json
    );

    let candidate_review_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/api/v1/media-segments/candidates?lowConfidence=true")
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let candidate_review_status = candidate_review_resp.status();
    let candidate_review_bytes =
        body::to_bytes(candidate_review_resp.into_body(), 1_048_576).await?;
    let candidate_review_json: Value = serde_json::from_slice(&candidate_review_bytes)?;
    assert_eq!(
        candidate_review_status,
        StatusCode::FORBIDDEN,
        "body: {}",
        candidate_review_json
    );

    let preferences_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/api/v1/profile/playback-interactions")
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let preferences_status = preferences_resp.status();
    let preferences_bytes = body::to_bytes(preferences_resp.into_body(), 1_048_576).await?;
    let preferences_json: Value = serde_json::from_slice(&preferences_bytes)?;
    assert_eq!(
        preferences_status,
        StatusCode::OK,
        "body: {}",
        preferences_json
    );
    assert_eq!(
        preferences_json
            .pointer("/preferences/segment_provider_settings/theintrodb/enabled")
            .and_then(Value::as_bool),
        Some(true)
    );

    Ok(())
}

#[tokio::test]
async fn playback_interaction_preferences_default_and_update() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "interaction-secret-key".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "Interaction.Movie.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let get_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/api/v1/profile/playback-interactions")
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let get_status = get_resp.status();
    let get_bytes = body::to_bytes(get_resp.into_body(), 1_048_576).await?;
    let get_json: Value = serde_json::from_slice(&get_bytes)?;
    assert_eq!(get_status, StatusCode::OK, "body: {}", get_json);
    assert_eq!(
        get_json
            .pointer("/preferences/skip_intro_behavior")
            .and_then(Value::as_str),
        Some("prompt")
    );
    assert_eq!(
        get_json
            .pointer("/preferences/segment_provider_settings/theintrodb/enabled")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        get_json
            .pointer("/preferences/segment_provider_settings/aniskip/enabled")
            .and_then(Value::as_bool),
        Some(true)
    );

    let patch = json!({
        "skipIntroBehavior": "disabled",
        "autoplayEnabled": false,
        "autoplayCountdownSeconds": 0,
        "segmentProviderSettings": {
            "aniskip": false,
            "theintrodb": {
                "enabled": true
            }
        }
    });
    let update_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::put("/api/v1/profile/playback-interactions")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(patch.to_string()))?,
        )
        .await?;
    let update_status = update_resp.status();
    let update_bytes = body::to_bytes(update_resp.into_body(), 1_048_576).await?;
    let update_json: Value = serde_json::from_slice(&update_bytes)?;
    assert_eq!(update_status, StatusCode::OK, "body: {}", update_json);
    assert_eq!(
        update_json
            .pointer("/preferences/skip_intro_behavior")
            .and_then(Value::as_str),
        Some("disabled")
    );
    assert_eq!(
        update_json
            .pointer("/preferences/autoplay_enabled")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        update_json
            .pointer("/preferences/segment_provider_settings/aniskip/enabled")
            .and_then(Value::as_bool),
        Some(false)
    );

    let play_json = play_fixture_json(
        &fixture,
        json!({
            "media_item_id": fixture.item_id,
            "preferred_file_id": fixture.media_file_id,
            "network_type": "lan",
            "client_capabilities": {
                "client_kind": "native_mpv",
                "direct_play_preferred": true,
                "supported_containers": ["mkv"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"]
            }
        }),
    )
    .await?;
    assert_eq!(
        play_json
            .pointer("/playback_preferences/skip_intro_behavior")
            .and_then(Value::as_str),
        Some("disabled")
    );
    assert_eq!(
        play_json
            .pointer("/playback_preferences/segment_provider_settings/aniskip/enabled")
            .and_then(Value::as_bool),
        Some(false)
    );

    Ok(())
}

#[tokio::test]
async fn play_endpoint_does_not_call_segment_providers() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "play-segment-provider-gate-secret-key".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "Interaction.Provider.Gate.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    sqlx::query("UPDATE movies SET external_imdb = 'tt7650001' WHERE id = $1")
        .bind(&fixture.item_id)
        .execute(&fixture.state.db_pool)
        .await?;

    let provider_hits = Arc::new(AtomicUsize::new(0));
    let provider_hits_for_handler = provider_hits.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address: SocketAddr = listener.local_addr()?;
    tokio::spawn(async move {
        let app = Router::new().route(
            "/segments",
            get(move || {
                let provider_hits = provider_hits_for_handler.clone();
                async move {
                    provider_hits.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_secs(5)).await;
                    Json(json!({
                        "segments": [{
                            "id": "should-not-be-called",
                            "type": "intro",
                            "start_sec": 0,
                            "end_sec": 45
                        }]
                    }))
                }
            }),
        );
        let _ = axum::serve(listener, app).await;
    });
    let provider_base_url = format!("http://{address}");

    let patch = json!({
        "segmentProviderSettings": {
            "theintrodb": {
                "enabled": true,
                "baseUrl": provider_base_url,
                "cacheTtlSeconds": 0,
                "rateLimitPerMinute": 60
            },
            "aniskip": false
        }
    });
    let update_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::put("/api/v1/profile/playback-interactions")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(patch.to_string()))?,
        )
        .await?;
    let update_status = update_resp.status();
    let update_bytes = body::to_bytes(update_resp.into_body(), 1_048_576).await?;
    let update_json: Value = serde_json::from_slice(&update_bytes)?;
    assert_eq!(update_status, StatusCode::OK, "body: {}", update_json);
    assert_eq!(
        update_json
            .pointer("/preferences/segment_provider_settings/theintrodb/base_url")
            .and_then(Value::as_str),
        Some(provider_base_url.as_str())
    );

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"]
        }
    });
    let started_at = Instant::now();
    let play_resp = tokio::time::timeout(
        Duration::from_secs(2),
        fixture.app.clone().oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        ),
    )
    .await
    .context("play endpoint blocked on a configured segment provider")??;
    let elapsed = started_at.elapsed();
    let play_status = play_resp.status();
    let play_bytes = body::to_bytes(play_resp.into_body(), 1_048_576).await?;
    let play_json: Value = serde_json::from_slice(&play_bytes)?;
    assert_eq!(play_status, StatusCode::OK, "body: {}", play_json);
    assert!(
        elapsed < Duration::from_secs(2),
        "play endpoint should return without provider latency, elapsed={elapsed:?}"
    );
    assert_eq!(
        provider_hits.load(Ordering::SeqCst),
        0,
        "play endpoint must not call segment providers synchronously"
    );
    assert_eq!(
        play_json
            .pointer("/playback_preferences/segment_provider_settings/theintrodb/enabled")
            .and_then(Value::as_bool),
        Some(true)
    );

    Ok(())
}

#[tokio::test]
async fn play_startup_stays_within_midm_budget() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "play-startup-budget-secret-key".to_string();
    settings.media_interactions.support_api_enabled = true;
    let fixture = setup_playback_route_fixture(
        settings,
        "Interaction.Startup.Budget.2026.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let candidate_body = json!({
        "mediaFileId": fixture.media_file_id,
        "segmentType": "intro",
        "startSeconds": 0.0,
        "endSeconds": 45.0,
        "providerKind": "chapter",
        "providerId": "startup_budget_fixture",
        "providerVersion": "1",
        "confidence": 0.92,
        "identityStrength": "file_fingerprint",
        "sourcePayload": {
            "title": "Intro"
        }
    });
    let create_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/media-segments/candidates")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(candidate_body.to_string()))?,
        )
        .await?;
    let create_status = create_resp.status();
    let create_bytes = body::to_bytes(create_resp.into_body(), 1_048_576).await?;
    let create_json: Value = serde_json::from_slice(&create_bytes)?;
    assert_eq!(create_status, StatusCode::OK, "body: {}", create_json);
    assert_eq!(
        create_json
            .pointer("/candidate/validation_state")
            .and_then(Value::as_str),
        Some("accepted")
    );

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"]
        }
    });
    let warm_play = play_fixture_json(&fixture, play_body.clone()).await?;
    let warm_session_id = warm_play
        .get("session_id")
        .and_then(Value::as_str)
        .context("warm play response should include session_id")?;
    let progress_body = json!({
        "positionSeconds": 37.0,
        "durationSeconds": 2000.0,
        "paused": false,
        "eventType": "progress"
    });
    let progress_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{warm_session_id}/progress"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(progress_body.to_string()))?,
        )
        .await?;
    let progress_status = progress_resp.status();
    let progress_bytes = body::to_bytes(progress_resp.into_body(), 1_048_576).await?;
    let progress_json: Value = serde_json::from_slice(&progress_bytes)?;
    assert_eq!(progress_status, StatusCode::OK, "body: {}", progress_json);

    let median_budget = timing_budget_from_env("ELIXIR_PLAY_STARTUP_MEDIAN_BUDGET_MS", 250);
    let max_budget = timing_budget_from_env("ELIXIR_PLAY_STARTUP_MAX_BUDGET_MS", 1500);
    let mut durations = Vec::new();
    for _ in 0..8 {
        let started_at = Instant::now();
        let play_json = tokio::time::timeout(
            max_budget + Duration::from_secs(1),
            play_fixture_json(&fixture, play_body.clone()),
        )
        .await
        .context("play endpoint exceeded startup timeout")??;
        let elapsed = started_at.elapsed();
        assert_eq!(
            play_json.get("mode").and_then(Value::as_str),
            Some("direct_play")
        );
        assert_eq!(
            play_json
                .get("logicalStartSeconds")
                .or_else(|| play_json.get("logical_start_seconds"))
                .and_then(Value::as_i64),
            Some(37)
        );
        assert_eq!(
            play_json
                .pointer("/segments/0/segment_type")
                .and_then(Value::as_str),
            Some("intro"),
            "play startup gate should exercise active MIDM segment lookup: {play_json}"
        );
        durations.push(elapsed);
    }

    durations.sort_unstable();
    let median = durations[durations.len() / 2];
    let max = *durations.last().context("startup durations should exist")?;
    assert!(
        median <= median_budget,
        "median play startup exceeded MIDM budget: median={median:?}, budget={median_budget:?}, samples={durations:?}"
    );
    assert!(
        max <= max_budget,
        "max play startup exceeded MIDM budget: max={max:?}, budget={max_budget:?}, samples={durations:?}"
    );

    Ok(())
}

#[tokio::test]
async fn media_interaction_library_settings_update_provider_policy() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "library-interaction-secret-key".to_string();
    settings.media_interactions.support_api_enabled = true;
    let fixture = setup_playback_route_fixture(
        settings,
        "Interaction.Library.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let server_id = Uuid::new_v4().to_string();
    let source_config_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO server_instances
            (id, user_id, device_name, lan_addresses)
         VALUES ($1, $2, 'MIDM Test Server', '[]')",
    )
    .bind(&server_id)
    .bind(&fixture.user_id)
    .execute(&fixture.state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO source_configs
            (id, server_id, extension_id, config_json, enabled)
         VALUES ($1, $2, 'elixir.local_folder', '{}', 1)",
    )
    .bind(&source_config_id)
    .bind(&server_id)
    .execute(&fixture.state.db_pool)
    .await?;
    sqlx::query("UPDATE media_files SET source_config_id = $1 WHERE id = $2")
        .bind(&source_config_id)
        .bind(&fixture.media_file_id)
        .execute(&fixture.state.db_pool)
        .await?;

    let list_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/api/v1/media-interaction-libraries")
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let list_status = list_resp.status();
    let list_bytes = body::to_bytes(list_resp.into_body(), 1_048_576).await?;
    let list_json: Value = serde_json::from_slice(&list_bytes)?;
    assert_eq!(list_status, StatusCode::OK, "body: {}", list_json);
    assert!(
        list_json
            .get("libraries")
            .and_then(Value::as_array)
            .is_some_and(|libraries| libraries.iter().any(|library| {
                library.get("source_config_id").and_then(Value::as_str)
                    == Some(source_config_id.as_str())
            })),
        "expected source config in library settings list: {list_json}"
    );

    let patch = json!({
        "segmentProviderSettings": {
            "theintrodb": false
        }
    });
    let update_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::put(format!(
                "/api/v1/media-interaction-libraries/{source_config_id}"
            ))
            .header("authorization", format!("Bearer {}", fixture.token))
            .header("content-type", "application/json")
            .body(Body::from(patch.to_string()))?,
        )
        .await?;
    let update_status = update_resp.status();
    let update_bytes = body::to_bytes(update_resp.into_body(), 1_048_576).await?;
    let update_json: Value = serde_json::from_slice(&update_bytes)?;
    assert_eq!(update_status, StatusCode::OK, "body: {}", update_json);
    assert_eq!(
        update_json
            .pointer("/library/segment_provider_settings/theintrodb/enabled")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        update_json
            .pointer("/library/effective_segment_provider_settings/theintrodb/enabled")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        update_json
            .pointer("/library/effective_segment_provider_settings/aniskip/enabled")
            .and_then(Value::as_bool),
        Some(true)
    );

    let stored_enabled = sqlx::query_scalar::<_, i64>(
        "SELECT CASE WHEN enabled THEN 1 ELSE 0 END
         FROM media_interaction_library_provider_settings
         WHERE source_config_id = $1
           AND provider_kind = 'theintrodb'",
    )
    .bind(&source_config_id)
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(stored_enabled, 0);

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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("play-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;

    let (item_id,): (String,) =
        sqlx::query_as("SELECT id FROM movies WHERE title = 'Play Movie' AND year = 2024 LIMIT 1")
            .fetch_one(&state.db_pool)
            .await?;
    let media_file_id: String =
        sqlx::query_scalar("SELECT id FROM media_files WHERE path = $1 LIMIT 1")
            .bind(file_path.to_string_lossy().to_string())
            .fetch_one(&state.db_pool)
            .await?;
    seed_test_media_probe(
        &state.db_pool,
        &media_file_id,
        &file_path.to_string_lossy(),
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let token = state.auth_service.issue_access_token(user_id)?.token;

    let play_body = serde_json::json!({
        "media_item_id": item_id,
        "preferred_file_id": media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"]
        }
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
    assert_eq!(
        json.get("delivery").and_then(Value::as_str),
        Some("direct_file")
    );
    assert_eq!(
        json.get("decision_reason").and_then(Value::as_str),
        Some("direct_play_all_capabilities_satisfied")
    );
    assert_eq!(
        json.pointer("/playback_plan/probe_version"),
        None,
        "probe data should not be embedded directly in the plan"
    );
    assert_eq!(
        json.pointer("/playback_plan/mode").and_then(Value::as_str),
        Some("direct_play")
    );
    assert_eq!(
        json.pointer("/playback_plan/compatibility_report/media_file_id_valid")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        json.pointer("/playback_plan/expected_outputs")
            .and_then(Value::as_array)
            .is_some_and(|outputs| outputs.iter().any(|output| {
                output.get("name").and_then(Value::as_str) == Some("direct_file")
                    && output.get("kind").and_then(Value::as_str) == Some("direct_file")
            })),
        "expected direct file output descriptor in plan: {json}"
    );
    assert!(
        json.get("decision_reasons")
            .and_then(Value::as_array)
            .is_some_and(|reasons| reasons
                .iter()
                .any(|reason| reason.as_str() == Some("direct_play_all_capabilities_satisfied"))),
        "play response should include machine-readable decision reasons: {json}"
    );
    let session_id = json
        .get("session_id")
        .and_then(Value::as_str)
        .context("play response should include session_id")?;
    let persisted_plan: Option<String> = sqlx::query_scalar(
        "SELECT playback_plan_json FROM playback_sessions WHERE id = $1 LIMIT 1",
    )
    .bind(session_id)
    .fetch_one(&state.db_pool)
    .await?;
    let persisted_plan: Value = serde_json::from_str(
        persisted_plan
            .as_deref()
            .context("playback session should persist playback_plan_json")?,
    )?;
    assert_eq!(
        persisted_plan,
        *json
            .get("playback_plan")
            .context("play response should include playback_plan")?,
        "stored playback_plan_json must match the plan returned to the client"
    );
    assert_eq!(
        json.get("server_seek_required").and_then(Value::as_bool),
        Some(false)
    );
    let detail_resp = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/sessions/{session_id}"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    let detail_status = detail_resp.status();
    let detail_bytes = body::to_bytes(detail_resp.into_body(), 1_048_576).await?;
    let detail_json: Value = serde_json::from_slice(&detail_bytes)?;
    assert_eq!(detail_status, StatusCode::OK, "body: {}", detail_json);
    assert_eq!(
        detail_json.get("delivery").and_then(Value::as_str),
        Some("direct_file")
    );
    assert_eq!(
        detail_json
            .get("server_seek_required")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        detail_json
            .pointer("/plan_summary/decision_reason")
            .and_then(Value::as_str),
        Some("direct_play_all_capabilities_satisfied")
    );
    assert_eq!(json.get("state").and_then(Value::as_str), Some("active"));
    assert_eq!(
        json.get("logicalPositionSeconds")
            .or_else(|| json.get("logical_position_seconds"))
            .and_then(Value::as_f64)
            .unwrap_or(-1.0) as i32,
        0
    );
    assert_eq!(
        json.pointer("/playback_state/item_type")
            .and_then(Value::as_str),
        Some("movie")
    );
    assert!(
        json.get("segments")
            .and_then(Value::as_array)
            .is_some_and(|segments| segments.is_empty()),
        "play response should include additive segment metadata: {json}"
    );

    let progress_body = serde_json::json!({
        "positionSeconds": 35.4,
        "durationSeconds": 2000,
        "paused": false
    });
    let progress_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/progress"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(progress_body.to_string()))?,
        )
        .await?;
    let progress_status = progress_resp.status();
    let progress_bytes = body::to_bytes(progress_resp.into_body(), 1_048_576).await?;
    let progress_json: Value = serde_json::from_slice(&progress_bytes)?;
    assert_eq!(progress_status, StatusCode::OK, "body: {}", progress_json);
    assert_eq!(
        progress_json
            .pointer("/playback_state/item_type")
            .and_then(Value::as_str),
        Some("movie")
    );
    assert_eq!(
        progress_json
            .pointer("/playback_state/watched")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        progress_json
            .pointer("/playback_state/resume_seconds")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            .round() as i32,
        35
    );

    let persisted_resume: f64 = sqlx::query_scalar(
        "SELECT resume_seconds FROM user_media_state WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
    )
    .bind(user_id.to_string())
    .bind(item_id.clone())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(persisted_resume.round() as i32, 35);

    let end_body = serde_json::json!({
        "positionSeconds": 45,
        "durationSeconds": 2000
    });
    let end_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/end"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(end_body.to_string()))?,
        )
        .await?;
    let end_status = end_resp.status();
    let end_bytes = body::to_bytes(end_resp.into_body(), 1_048_576).await?;
    let end_json: Value = serde_json::from_slice(&end_bytes)?;
    assert_eq!(end_status, StatusCode::OK, "body: {}", end_json);
    assert_eq!(
        end_json
            .pointer("/playback_state/resume_seconds")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            .round() as i32,
        45
    );

    let resumed_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    let resumed_status = resumed_resp.status();
    let resumed_bytes = body::to_bytes(resumed_resp.into_body(), 1_048_576).await?;
    let resumed_json: Value = serde_json::from_slice(&resumed_bytes)?;
    assert_eq!(resumed_status, StatusCode::OK, "body: {}", resumed_json);
    assert_eq!(
        resumed_json
            .get("logicalStartSeconds")
            .or_else(|| resumed_json.get("logical_start_seconds"))
            .and_then(Value::as_i64),
        Some(45)
    );

    Ok(())
}

#[tokio::test]
async fn progress_writes_are_bounded_under_concurrent_playback() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "concurrent-progress-secret-key".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "Concurrent.Progress.Movie.2026.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let second_user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(second_user_id.to_string())
        .bind("concurrent-progress-second@example.com")
        .bind("hashed")
        .execute(&fixture.state.db_pool)
        .await?;
    let second_user_token = fixture
        .state
        .auth_service
        .issue_access_token(second_user_id)?
        .token;

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"]
        }
    });

    let user_one_first_play =
        play_route_json(&fixture.app, &fixture.token, play_body.clone()).await?;
    let user_one_second_play =
        play_route_json(&fixture.app, &fixture.token, play_body.clone()).await?;
    let user_two_play = play_route_json(&fixture.app, &second_user_token, play_body).await?;
    let user_one_first_session = user_one_first_play
        .get("session_id")
        .and_then(Value::as_str)
        .context("first user first session id")?
        .to_string();
    let user_one_second_session = user_one_second_play
        .get("session_id")
        .and_then(Value::as_str)
        .context("first user second session id")?
        .to_string();
    let user_two_session = user_two_play
        .get("session_id")
        .and_then(Value::as_str)
        .context("second user session id")?
        .to_string();

    let progress_cases = [
        (
            fixture.token.clone(),
            user_one_first_session.clone(),
            40_i32,
            2_i32,
        ),
        (
            fixture.token.clone(),
            user_one_second_session.clone(),
            80_i32,
            2_i32,
        ),
        (
            second_user_token.clone(),
            user_two_session.clone(),
            200_i32,
            2_i32,
        ),
    ];
    let mut handles = Vec::new();
    for (token, session_id, start_position, count) in progress_cases {
        for offset in 0..count {
            let app = fixture.app.clone();
            let token = token.clone();
            let session_id = session_id.clone();
            let position_seconds = start_position + offset;
            handles.push(tokio::spawn(async move {
                let request = json!({
                    "positionSeconds": position_seconds,
                    "durationSeconds": 2000,
                    "paused": false,
                    "eventType": "progress"
                });
                let response = tokio::time::timeout(
                    Duration::from_secs(5),
                    app.oneshot(
                        Request::post(format!("/api/v1/sessions/{session_id}/progress"))
                            .header("authorization", format!("Bearer {token}"))
                            .header("content-type", "application/json")
                            .body(Body::from(request.to_string()))?,
                    ),
                )
                .await
                .context("concurrent progress request timed out")??;
                let status = response.status();
                let bytes = body::to_bytes(response.into_body(), 1_048_576).await?;
                let payload: Value = serde_json::from_slice(&bytes)?;
                assert_eq!(status, StatusCode::OK, "body: {}", payload);
                assert_eq!(payload.get("ok").and_then(Value::as_bool), Some(true));
                Ok::<(), anyhow::Error>(())
            }));
        }
    }
    for handle in handles {
        handle.await??;
    }

    let state_row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM user_media_state
         WHERE item_type = 'movie'
           AND item_id = $1",
    )
    .bind(&fixture.item_id)
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(
        state_row_count, 2,
        "concurrent playback must collapse to one durable state row per user/item"
    );

    let (user_one_resume, user_one_watched, user_one_last_session): (f64, i64, Option<String>) =
        sqlx::query_as(
            "SELECT resume_seconds,
                    CASE WHEN watched THEN 1 ELSE 0 END,
                    last_session_id
             FROM user_media_state
             WHERE user_id = $1
               AND item_type = 'movie'
               AND item_id = $2",
        )
        .bind(&fixture.user_id)
        .bind(&fixture.item_id)
        .fetch_one(&fixture.state.db_pool)
        .await?;
    assert_eq!(user_one_watched, 0);
    assert!(
        (40.0..=81.0).contains(&user_one_resume),
        "same-user concurrent sessions must persist a valid submitted resume point, got {user_one_resume}"
    );
    assert!(
        [
            user_one_first_session.as_str(),
            user_one_second_session.as_str()
        ]
        .contains(&user_one_last_session.as_deref().unwrap_or_default()),
        "same-user state must point at one of that user's sessions, got {user_one_last_session:?}"
    );

    let (user_two_resume, user_two_watched, user_two_last_session): (f64, i64, Option<String>) =
        sqlx::query_as(
            "SELECT resume_seconds,
                    CASE WHEN watched THEN 1 ELSE 0 END,
                    last_session_id
             FROM user_media_state
             WHERE user_id = $1
               AND item_type = 'movie'
               AND item_id = $2",
        )
        .bind(second_user_id.to_string())
        .bind(&fixture.item_id)
        .fetch_one(&fixture.state.db_pool)
        .await?;
    assert_eq!(user_two_watched, 0);
    assert!(
        (200.0..=201.0).contains(&user_two_resume),
        "second user must keep an isolated submitted resume point, got {user_two_resume}"
    );
    assert_eq!(
        user_two_last_session.as_deref(),
        Some(user_two_session.as_str())
    );

    let progress_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM playback_progress_events
         WHERE event_type = 'progress'",
    )
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(
        progress_event_count, 0,
        "heartbeat progress writes must not create an unbounded audit event stream"
    );

    let total_event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM playback_progress_events")
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(
        total_event_count, 3,
        "only the three session start audit events should be recorded"
    );

    Ok(())
}

#[tokio::test]
async fn progress_load_coalesces_many_heartbeats_without_audit_growth() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "progress-load-secret-key".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "Progress.Load.Movie.2026.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"]
        }
    });

    let mut session_ids = Vec::new();
    for _ in 0..8 {
        let play_json = play_route_json(&fixture.app, &fixture.token, play_body.clone()).await?;
        session_ids.push(
            play_json
                .get("session_id")
                .and_then(Value::as_str)
                .context("load play response should include session_id")?
                .to_string(),
        );
    }

    let mut handles = Vec::new();
    for (session_index, session_id) in session_ids.iter().enumerate() {
        for heartbeat_index in 0..10 {
            let app = fixture.app.clone();
            let token = fixture.token.clone();
            let session_id = session_id.clone();
            let position_seconds = 30 + (session_index as i32 * 20) + heartbeat_index;
            handles.push(tokio::spawn(async move {
                let request = json!({
                    "positionSeconds": position_seconds,
                    "durationSeconds": 2000,
                    "paused": false,
                    "eventType": "progress"
                });
                let response = tokio::time::timeout(
                    Duration::from_secs(5),
                    app.oneshot(
                        Request::post(format!("/api/v1/sessions/{session_id}/progress"))
                            .header("authorization", format!("Bearer {token}"))
                            .header("content-type", "application/json")
                            .body(Body::from(request.to_string()))?,
                    ),
                )
                .await
                .context("progress load request timed out")??;
                let status = response.status();
                let bytes = body::to_bytes(response.into_body(), 1_048_576).await?;
                let payload: Value = serde_json::from_slice(&bytes)?;
                assert_eq!(status, StatusCode::OK, "body: {}", payload);
                assert_eq!(payload.get("ok").and_then(Value::as_bool), Some(true));
                Ok::<(), anyhow::Error>(())
            }));
        }
    }
    for handle in handles {
        handle.await??;
    }

    let state_row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM user_media_state
         WHERE user_id = $1
           AND item_type = 'movie'
           AND item_id = $2",
    )
    .bind(&fixture.user_id)
    .bind(&fixture.item_id)
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(
        state_row_count, 1,
        "heartbeat load must still collapse to one durable state row"
    );

    let (resume_seconds, last_session_id): (f64, Option<String>) = sqlx::query_as(
        "SELECT resume_seconds, last_session_id
         FROM user_media_state
         WHERE user_id = $1
           AND item_type = 'movie'
           AND item_id = $2",
    )
    .bind(&fixture.user_id)
    .bind(&fixture.item_id)
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert!(
        (30.0..=179.0).contains(&resume_seconds),
        "load test persisted a resume point outside submitted heartbeat range: {resume_seconds}"
    );
    assert!(
        session_ids.contains(&last_session_id.clone().unwrap_or_default()),
        "load test state must point at one of the active sessions, got {last_session_id:?}"
    );

    let progress_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM playback_progress_events
         WHERE event_type = 'progress'",
    )
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(
        progress_event_count, 0,
        "heartbeat load must not create per-progress audit rows"
    );

    let total_event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM playback_progress_events")
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(
        total_event_count,
        session_ids.len() as i64,
        "only session start audit events should be recorded under heartbeat load"
    );

    Ok(())
}

#[tokio::test]
async fn manual_watch_state_routes_mark_unwatch_and_reset_progress() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "manual-watch-state-secret-key".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "Manual.Watch.State.Movie.2024.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let watch_state_path = format!("/api/v1/items/movie/{}/watch-state", fixture.item_id);

    let initial_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(&watch_state_path)
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let initial_status = initial_resp.status();
    let initial_bytes = body::to_bytes(initial_resp.into_body(), 1_048_576).await?;
    let initial_json: Value = serde_json::from_slice(&initial_bytes)?;
    assert_eq!(initial_status, StatusCode::OK, "body: {}", initial_json);
    assert_eq!(
        initial_json
            .pointer("/playback_state/watched")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        initial_json
            .pointer("/playback_state/resume_seconds")
            .and_then(Value::as_f64),
        Some(0.0)
    );

    let watched_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("{watch_state_path}/watched"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(json!({"durationSeconds": 2000}).to_string()))?,
        )
        .await?;
    let watched_status = watched_resp.status();
    let watched_bytes = body::to_bytes(watched_resp.into_body(), 1_048_576).await?;
    let watched_json: Value = serde_json::from_slice(&watched_bytes)?;
    assert_eq!(watched_status, StatusCode::OK, "body: {}", watched_json);
    assert_eq!(
        watched_json.get("action").and_then(Value::as_str),
        Some("watched")
    );
    assert_eq!(
        watched_json
            .pointer("/playback_state/watched")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        watched_json
            .pointer("/playback_state/state_source")
            .and_then(Value::as_str),
        Some("manual")
    );
    assert_eq!(
        watched_json
            .pointer("/playback_state/duration_seconds")
            .and_then(Value::as_i64),
        Some(2000)
    );

    let duplicate_watched_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("{watch_state_path}/watched"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(duplicate_watched_resp.status(), StatusCode::OK);
    let play_count: i64 = sqlx::query_scalar(
        "SELECT play_count
         FROM user_media_state
         WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
    )
    .bind(&fixture.user_id)
    .bind(&fixture.item_id)
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(play_count, 1);

    let unwatched_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("{watch_state_path}/unwatched"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let unwatched_status = unwatched_resp.status();
    let unwatched_bytes = body::to_bytes(unwatched_resp.into_body(), 1_048_576).await?;
    let unwatched_json: Value = serde_json::from_slice(&unwatched_bytes)?;
    assert_eq!(unwatched_status, StatusCode::OK, "body: {}", unwatched_json);
    assert_eq!(
        unwatched_json
            .pointer("/playback_state/watched")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        unwatched_json
            .pointer("/playback_state/resume_seconds")
            .and_then(Value::as_f64),
        Some(0.0)
    );

    sqlx::query(
        "UPDATE user_media_state
         SET resume_seconds = 88,
             state_source = 'playback',
             updated_at = CURRENT_TIMESTAMP
         WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
    )
    .bind(&fixture.user_id)
    .bind(&fixture.item_id)
    .execute(&fixture.state.db_pool)
    .await?;

    let reset_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("{watch_state_path}/reset"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let reset_status = reset_resp.status();
    let reset_bytes = body::to_bytes(reset_resp.into_body(), 1_048_576).await?;
    let reset_json: Value = serde_json::from_slice(&reset_bytes)?;
    assert_eq!(reset_status, StatusCode::OK, "body: {}", reset_json);
    assert_eq!(
        reset_json
            .pointer("/playback_state/resume_seconds")
            .and_then(Value::as_f64),
        Some(0.0)
    );
    assert_eq!(
        reset_json
            .pointer("/playback_state/watched")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        reset_json
            .pointer("/playback_state/state_source")
            .and_then(Value::as_str),
        Some("manual")
    );

    Ok(())
}

#[tokio::test]
async fn play_endpoint_scopes_series_episode_preferred_id() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "play-series-secret-key".to_string();
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
    let s1e1_path = dir.path().join("Playback.Scope.S01E01.mkv");
    let s2e10_path = dir.path().join("Playback.Scope.S02E10.mkv");
    let s2e11_path = dir.path().join("Playback.Scope.S02E11.mkv");
    tokio::fs::write(&s1e1_path, b"s01e01").await?;
    tokio::fs::write(&s2e10_path, b"s02e10").await?;
    tokio::fs::write(&s2e11_path, b"s02e11").await?;

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("play-series-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;
    sqlx::query(
        "INSERT INTO user_playback_preferences
            (user_id, skip_intro_behavior, skip_recap_behavior, skip_preview_behavior,
             skip_credits_behavior, skip_outro_behavior, autoplay_enabled,
             autoplay_countdown_seconds, autoplay_max_consecutive,
             segment_provider_settings_json)
         VALUES ($1, 'prompt', 'prompt', 'prompt', 'prompt', 'prompt', 1, 10, 1, '{}')",
    )
    .bind(user_id.to_string())
    .execute(&state.db_pool)
    .await?;

    let series_id = Uuid::new_v4();
    let season_one_id = Uuid::new_v4();
    let season_two_id = Uuid::new_v4();
    let episode_one_id = Uuid::new_v4();
    let episode_later_id = Uuid::new_v4();
    let episode_final_id = Uuid::new_v4();
    let file_one_id = Uuid::new_v4();
    let file_later_id = Uuid::new_v4();
    let file_final_id = Uuid::new_v4();
    let file_one_id_text = file_one_id.to_string();
    let file_later_id_text = file_later_id.to_string();
    let file_final_id_text = file_final_id.to_string();
    let episode_one_id_text = episode_one_id.to_string();
    let episode_later_id_text = episode_later_id.to_string();
    let episode_final_id_text = episode_final_id.to_string();

    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, 'series', '{}', 'Playback Scope', 2024)",
    )
    .bind(series_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO series (id, title, year, library_type, metadata_json)
         VALUES ($1, 'Playback Scope', 2024, 'series', NULL)",
    )
    .bind(series_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 1, $3)")
        .bind(season_one_id.to_string())
        .bind(series_id.to_string())
        .bind("Season 1")
        .execute(&state.db_pool)
        .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 2, $3)")
        .bind(season_two_id.to_string())
        .bind(series_id.to_string())
        .bind("Season 2")
        .execute(&state.db_pool)
        .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, has_file)
         VALUES ($1, $2, $3, 1, 1, 'Episode One', 1)",
    )
    .bind(episode_one_id.to_string())
    .bind(series_id.to_string())
    .bind(season_one_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, has_file)
         VALUES ($1, $2, $3, 2, 10, 'Later Episode', 1)",
    )
    .bind(episode_later_id.to_string())
    .bind(series_id.to_string())
    .bind(season_two_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, has_file)
         VALUES ($1, $2, $3, 2, 11, 'Final Episode', 1)",
    )
    .bind(episode_final_id.to_string())
    .bind(series_id.to_string())
    .bind(season_two_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
         VALUES ($1, $2, $3, 6, 'mkv', 'h264', 'aac', 1280, 720, 2000000, 'ok')",
    )
    .bind(file_one_id.to_string())
    .bind(series_id.to_string())
    .bind(s1e1_path.to_string_lossy().to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
         VALUES ($1, $2, $3, 6, 'mkv', 'h264', 'aac', 3840, 2160, 18000000, 'ok')",
    )
    .bind(file_later_id.to_string())
    .bind(series_id.to_string())
    .bind(s2e10_path.to_string_lossy().to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
         VALUES ($1, $2, $3, 6, 'mkv', 'h264', 'aac', 1280, 720, 2000000, 'ok')",
    )
    .bind(file_final_id.to_string())
    .bind(series_id.to_string())
    .bind(s2e11_path.to_string_lossy().to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
        .bind(episode_one_id.to_string())
        .bind(file_one_id.to_string())
        .execute(&state.db_pool)
        .await?;
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
        .bind(episode_later_id.to_string())
        .bind(file_later_id.to_string())
        .execute(&state.db_pool)
        .await?;
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
        .bind(episode_final_id.to_string())
        .bind(file_final_id.to_string())
        .execute(&state.db_pool)
        .await?;
    seed_test_media_probe(
        &state.db_pool,
        &file_one_id.to_string(),
        &s1e1_path.to_string_lossy(),
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    seed_test_media_probe(
        &state.db_pool,
        &file_later_id.to_string(),
        &s2e10_path.to_string_lossy(),
        "mkv",
        "h264",
        "aac",
        3840,
        2160,
        18_000_000,
    )
    .await?;
    seed_test_media_probe(
        &state.db_pool,
        &file_final_id.to_string(),
        &s2e11_path.to_string_lossy(),
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let play_body = serde_json::json!({
        "media_item_id": series_id,
        "preferred_episode_id": episode_one_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264", "h265"],
            "supported_audio_codecs": ["aac"]
        }
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
    let selected_file_id = json
        .get("mediaFileId")
        .or_else(|| json.get("media_file_id"))
        .and_then(Value::as_str)
        .context("media file id in play response")?;
    assert_eq!(selected_file_id, file_one_id.to_string());
    assert_eq!(
        json.pointer("/up_next/available").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        json.pointer("/up_next/episode_id").and_then(Value::as_str),
        Some(episode_later_id_text.as_str())
    );
    assert_eq!(
        json.pointer("/up_next/reason").and_then(Value::as_str),
        Some("next_unwatched_episode")
    );
    assert_eq!(
        json.pointer("/up_next/autoplay/allowed")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        json.pointer("/up_next/autoplay/max_consecutive")
            .and_then(Value::as_i64),
        Some(1)
    );

    let legacy_play_body = serde_json::json!({
        "media_item_id": series_id,
        "preferred_file_id": episode_one_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true
        }
    });
    let legacy_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(legacy_play_body.to_string()))?,
        )
        .await?;
    let legacy_status = legacy_resp.status();
    let legacy_bytes = body::to_bytes(legacy_resp.into_body(), 1_048_576).await?;
    let legacy_json: Value = serde_json::from_slice(&legacy_bytes)?;
    assert_eq!(legacy_status, StatusCode::OK, "body: {}", legacy_json);
    let legacy_selected_file_id = legacy_json
        .get("mediaFileId")
        .or_else(|| legacy_json.get("media_file_id"))
        .and_then(Value::as_str)
        .context("media file id in legacy play response")?;
    assert_eq!(legacy_selected_file_id, file_one_id.to_string());

    let auto_play_body = serde_json::json!({
        "media_item_id": series_id,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264", "h265"],
            "supported_audio_codecs": ["aac"]
        }
    });
    let auto_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(auto_play_body.to_string()))?,
        )
        .await?;
    let auto_status = auto_resp.status();
    let auto_bytes = body::to_bytes(auto_resp.into_body(), 1_048_576).await?;
    let auto_json: Value = serde_json::from_slice(&auto_bytes)?;
    assert_eq!(auto_status, StatusCode::OK, "body: {}", auto_json);
    assert_eq!(
        auto_json.get("media_file_id").and_then(Value::as_str),
        Some(file_one_id_text.as_str()),
        "series-level Play must pick the first available episode, not the highest-quality file"
    );
    assert_eq!(
        auto_json.get("selected_episode_id").and_then(Value::as_str),
        Some(episode_one_id_text.as_str())
    );
    assert_eq!(
        auto_json
            .get("episode_selection_reason")
            .and_then(Value::as_str),
        Some("next_unwatched_episode")
    );

    sqlx::query(
        "INSERT INTO user_media_state
            (user_id, item_type, item_id, media_file_id, series_id, season_id,
             resume_seconds, duration_seconds, watched, watched_at, play_count,
             last_played_at, state_source)
         VALUES ($1, 'episode', $2, $3, $4, $5, 0, 1000, 1, CURRENT_TIMESTAMP, 1,
                 CURRENT_TIMESTAMP, 'playback')
         ON CONFLICT(user_id, item_type, item_id) DO UPDATE SET
             resume_seconds = 0,
             duration_seconds = 1000,
             watched = 1,
             watched_at = CURRENT_TIMESTAMP,
             last_played_at = CURRENT_TIMESTAMP,
             state_source = 'playback',
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(user_id.to_string())
    .bind(episode_one_id.to_string())
    .bind(file_one_id.to_string())
    .bind(series_id.to_string())
    .bind(season_one_id.to_string())
    .execute(&state.db_pool)
    .await?;

    let next_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(auto_play_body.to_string()))?,
        )
        .await?;
    let next_status = next_resp.status();
    let next_bytes = body::to_bytes(next_resp.into_body(), 1_048_576).await?;
    let next_json: Value = serde_json::from_slice(&next_bytes)?;
    assert_eq!(next_status, StatusCode::OK, "body: {}", next_json);
    assert_eq!(
        next_json.get("media_file_id").and_then(Value::as_str),
        Some(file_later_id_text.as_str())
    );
    assert_eq!(
        next_json
            .get("episode_selection_reason")
            .and_then(Value::as_str),
        Some("next_unwatched_episode")
    );

    sqlx::query(
        "INSERT INTO user_media_state
            (user_id, item_type, item_id, media_file_id, series_id, season_id,
             resume_seconds, duration_seconds, watched, play_count, last_played_at,
             state_source)
         VALUES ($1, 'episode', $2, $3, $4, $5, 45, 1000, 0, 0, CURRENT_TIMESTAMP,
                 'playback')
         ON CONFLICT(user_id, item_type, item_id) DO UPDATE SET
             resume_seconds = 45,
             duration_seconds = 1000,
             watched = 0,
             watched_at = NULL,
             last_played_at = CURRENT_TIMESTAMP,
             state_source = 'playback',
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(user_id.to_string())
    .bind(episode_later_id.to_string())
    .bind(file_later_id.to_string())
    .bind(series_id.to_string())
    .bind(season_two_id.to_string())
    .execute(&state.db_pool)
    .await?;

    let continue_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(auto_play_body.to_string()))?,
        )
        .await?;
    let continue_status = continue_resp.status();
    let continue_bytes = body::to_bytes(continue_resp.into_body(), 1_048_576).await?;
    let continue_json: Value = serde_json::from_slice(&continue_bytes)?;
    assert_eq!(continue_status, StatusCode::OK, "body: {}", continue_json);
    assert_eq!(
        continue_json.get("media_file_id").and_then(Value::as_str),
        Some(file_later_id_text.as_str())
    );
    assert_eq!(
        continue_json
            .get("episode_selection_reason")
            .and_then(Value::as_str),
        Some("continue_watching_episode")
    );

    let autoplay_source_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    let autoplay_source_status = autoplay_source_resp.status();
    let autoplay_source_bytes = body::to_bytes(autoplay_source_resp.into_body(), 1_048_576).await?;
    let autoplay_source_json: Value = serde_json::from_slice(&autoplay_source_bytes)?;
    assert_eq!(
        autoplay_source_status,
        StatusCode::OK,
        "body: {}",
        autoplay_source_json
    );
    let autoplay_source_session_id = autoplay_source_json
        .get("session_id")
        .and_then(Value::as_str)
        .context("autoplay source session id")?;
    let autoplay_end_body = json!({
        "positionSeconds": 990,
        "durationSeconds": 1000,
        "eventType": "autoplay_next"
    });
    let autoplay_end_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{autoplay_source_session_id}/end"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(autoplay_end_body.to_string()))?,
        )
        .await?;
    let autoplay_end_status = autoplay_end_resp.status();
    let autoplay_end_bytes = body::to_bytes(autoplay_end_resp.into_body(), 1_048_576).await?;
    let autoplay_end_json: Value = serde_json::from_slice(&autoplay_end_bytes)?;
    assert_eq!(
        autoplay_end_status,
        StatusCode::OK,
        "body: {}",
        autoplay_end_json
    );

    let durable_count: i64 = sqlx::query_scalar(
        "SELECT consecutive_count
         FROM user_autoplay_sessions
         WHERE user_id = $1 AND series_id = $2 AND canceled = FALSE
         ORDER BY updated_at DESC
         LIMIT 1",
    )
    .bind(user_id.to_string())
    .bind(series_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(durable_count, 1);

    let later_play_body = serde_json::json!({
        "media_item_id": series_id,
        "preferred_episode_id": episode_later_id,
        "start_position_seconds": 0,
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264", "h265"],
            "supported_audio_codecs": ["aac"]
        }
    });
    let blocked_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(later_play_body.to_string()))?,
        )
        .await?;
    let blocked_status = blocked_resp.status();
    let blocked_bytes = body::to_bytes(blocked_resp.into_body(), 1_048_576).await?;
    let blocked_json: Value = serde_json::from_slice(&blocked_bytes)?;
    assert_eq!(blocked_status, StatusCode::OK, "body: {}", blocked_json);
    assert_eq!(
        blocked_json.get("media_file_id").and_then(Value::as_str),
        Some(file_later_id_text.as_str())
    );
    assert_eq!(
        blocked_json
            .pointer("/up_next/episode_id")
            .and_then(Value::as_str),
        Some(episode_final_id_text.as_str())
    );
    assert_eq!(
        blocked_json
            .pointer("/up_next/media_file_id")
            .and_then(Value::as_str),
        Some(file_final_id_text.as_str())
    );
    assert_eq!(
        blocked_json
            .pointer("/up_next/autoplay/allowed")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        blocked_json
            .pointer("/up_next/autoplay/block_reason")
            .and_then(Value::as_str),
        Some("max_consecutive_reached")
    );
    assert_eq!(
        blocked_json
            .pointer("/up_next/autoplay/consecutive_count")
            .and_then(Value::as_i64),
        Some(1)
    );

    let blocked_session_id = blocked_json
        .get("session_id")
        .and_then(Value::as_str)
        .context("blocked autoplay session id")?;
    let manual_play_next_body = json!({
        "positionSeconds": 120,
        "durationSeconds": 1000,
        "eventType": "play_next"
    });
    let manual_reset_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{blocked_session_id}/end"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(manual_play_next_body.to_string()))?,
        )
        .await?;
    let manual_reset_status = manual_reset_resp.status();
    let manual_reset_bytes = body::to_bytes(manual_reset_resp.into_body(), 1_048_576).await?;
    let manual_reset_json: Value = serde_json::from_slice(&manual_reset_bytes)?;
    assert_eq!(
        manual_reset_status,
        StatusCode::OK,
        "body: {}",
        manual_reset_json
    );

    let cancelled_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM user_autoplay_sessions
         WHERE user_id = $1 AND series_id = $2 AND canceled = FALSE",
    )
    .bind(user_id.to_string())
    .bind(series_id.to_string())
    .fetch_one(&state.db_pool)
    .await?;
    assert_eq!(cancelled_count, 0);

    let reset_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(later_play_body.to_string()))?,
        )
        .await?;
    let reset_status = reset_resp.status();
    let reset_bytes = body::to_bytes(reset_resp.into_body(), 1_048_576).await?;
    let reset_json: Value = serde_json::from_slice(&reset_bytes)?;
    assert_eq!(reset_status, StatusCode::OK, "body: {}", reset_json);
    assert_eq!(
        reset_json
            .pointer("/up_next/autoplay/allowed")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        reset_json
            .pointer("/up_next/autoplay/consecutive_count")
            .and_then(Value::as_i64),
        Some(0)
    );

    let invalid_preferred_body = serde_json::json!({
        "media_item_id": series_id,
        "preferred_file_id": Uuid::new_v4(),
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true
        }
    });
    let invalid_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(invalid_preferred_body.to_string()))?,
        )
        .await?;
    let invalid_status = invalid_resp.status();
    let invalid_body = body::to_bytes(invalid_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        invalid_status,
        StatusCode::NOT_FOUND,
        "body: {}",
        String::from_utf8_lossy(&invalid_body)
    );

    let invalid_episode_body = serde_json::json!({
        "media_item_id": series_id,
        "preferred_episode_id": Uuid::new_v4(),
        "network_type": "lan",
        "client_capabilities": {
            "client_kind": "native_mpv",
            "direct_play_preferred": true
        }
    });
    let invalid_episode_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(invalid_episode_body.to_string()))?,
        )
        .await?;
    let invalid_episode_status = invalid_episode_resp.status();
    let invalid_episode_body = body::to_bytes(invalid_episode_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        invalid_episode_status,
        StatusCode::NOT_FOUND,
        "body: {}",
        String::from_utf8_lossy(&invalid_episode_body)
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("range-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;

    let token = state.auth_service.issue_access_token(user_id)?.token;

    let media_item_id: String = sqlx::query_scalar("SELECT id FROM movies LIMIT 1")
        .fetch_one(&state.db_pool)
        .await?;
    let selected_file_id: String =
        sqlx::query_scalar("SELECT id FROM media_files WHERE path = $1 LIMIT 1")
            .bind(file_path.to_string_lossy().to_string())
            .fetch_one(&state.db_pool)
            .await?;
    seed_test_media_probe(
        &state.db_pool,
        &selected_file_id,
        &file_path.to_string_lossy(),
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let other_file_id = Uuid::new_v4();
    let other_file_path = dir.path().join("Other.RangeTest.2024.mkv");
    tokio::fs::write(&other_file_path, b"other-file").await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
         VALUES ($1, $2, $3, 10, 'mkv', 'h264', 'aac', 1280, 720, 2000000, 'ok')",
    )
    .bind(other_file_id.to_string())
    .bind(&media_item_id)
    .bind(other_file_path.to_string_lossy().to_string())
    .execute(&state.db_pool)
    .await?;

    let play_body = serde_json::json!({
        "media_item_id": media_item_id,
        "preferred_file_id": selected_file_id,
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
    let play_status = play_resp.status();
    let play_bytes = body::to_bytes(play_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        play_status,
        StatusCode::OK,
        "play failed: {}",
        String::from_utf8_lossy(&play_bytes)
    );
    let play_json: Value = serde_json::from_slice(&play_bytes)?;
    let stream_url = play_json
        .get("streamUrl")
        .or_else(|| play_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url");
    assert!(
        !stream_url.contains("token=") && !stream_url.contains("access_token="),
        "direct stream URL should rely on auth headers, not bearer query tokens: {stream_url}"
    );
    let session_id = play_json
        .get("sessionId")
        .or_else(|| play_json.get("session_id"))
        .and_then(Value::as_str)
        .expect("session_id");
    let media_file_id = play_json
        .get("mediaFileId")
        .or_else(|| play_json.get("media_file_id"))
        .and_then(Value::as_str)
        .expect("media_file_id");

    let tampered_url = stream_url.replacen(media_file_id, &other_file_id.to_string(), 1);
    let tampered_resp = app
        .clone()
        .oneshot(
            Request::get(&tampered_url)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(tampered_resp.status(), StatusCode::UNAUTHORIZED);

    sqlx::query("UPDATE playback_sessions SET updated_at = '2000-01-01 00:00:00' WHERE id = $1")
        .bind(session_id)
        .execute(&state.db_pool)
        .await?;

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
    let partial_headers = resp.headers().clone();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "body: {}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(&bytes[..], b"abcd");
    assert_eq!(
        partial_headers
            .get("accept-ranges")
            .and_then(|value| value.to_str().ok()),
        Some("bytes")
    );
    assert_eq!(
        partial_headers
            .get("content-range")
            .and_then(|value| value.to_str().ok()),
        Some("bytes 0-3/10")
    );
    assert_eq!(
        partial_headers
            .get("content-length")
            .and_then(|value| value.to_str().ok()),
        Some("4")
    );
    assert!(partial_headers.get("etag").is_some());
    assert!(partial_headers.get("last-modified").is_some());
    assert!(
        crate::metrics::DIRECT_STREAM_BYTES
            .with_label_values(&[
                session_id,
                &user_id.to_string(),
                media_file_id,
                "direct_file"
            ])
            .get()
            >= 4
    );

    let open_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=4-")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(open_resp.status(), StatusCode::PARTIAL_CONTENT);
    let open_headers = open_resp.headers().clone();
    let open_bytes = body::to_bytes(open_resp.into_body(), 1_048_576).await?;
    assert_eq!(&open_bytes[..], b"efghij");
    assert_eq!(
        open_headers
            .get("content-range")
            .and_then(|value| value.to_str().ok()),
        Some("bytes 4-9/10")
    );

    let suffix_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=-3")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(suffix_resp.status(), StatusCode::PARTIAL_CONTENT);
    let suffix_headers = suffix_resp.headers().clone();
    let suffix_bytes = body::to_bytes(suffix_resp.into_body(), 1_048_576).await?;
    assert_eq!(&suffix_bytes[..], b"hij");
    assert_eq!(
        suffix_headers
            .get("content-range")
            .and_then(|value| value.to_str().ok()),
        Some("bytes 7-9/10")
    );

    let etag = partial_headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("etag")
        .to_string();
    let if_range_match_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=1-2")
                .header("if-range", etag)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(if_range_match_resp.status(), StatusCode::PARTIAL_CONTENT);
    let if_range_match_bytes = body::to_bytes(if_range_match_resp.into_body(), 1_048_576).await?;
    assert_eq!(&if_range_match_bytes[..], b"bc");

    let if_range_mismatch_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=1-2")
                .header("if-range", "\"does-not-match\"")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(if_range_mismatch_resp.status(), StatusCode::OK);
    assert!(
        if_range_mismatch_resp
            .headers()
            .get("content-range")
            .is_none()
    );
    let if_range_mismatch_bytes =
        body::to_bytes(if_range_mismatch_resp.into_body(), 1_048_576).await?;
    assert_eq!(&if_range_mismatch_bytes[..], file_bytes);

    let invalid_range_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=99-120")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(
        invalid_range_resp.status(),
        StatusCode::RANGE_NOT_SATISFIABLE
    );
    assert_eq!(
        invalid_range_resp
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok()),
        Some("bytes */10")
    );

    let multi_range_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=0-1,3-4")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(multi_range_resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    let multi_range_body = body::to_bytes(multi_range_resp.into_body(), 1_048_576).await?;
    assert!(
        String::from_utf8_lossy(&multi_range_body).contains("multiple byte ranges"),
        "{}",
        String::from_utf8_lossy(&multi_range_body)
    );

    let head_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(head_resp.status(), StatusCode::OK);
    assert_eq!(
        head_resp
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok()),
        Some("10")
    );
    let head_body = body::to_bytes(head_resp.into_body(), 1_048_576).await?;
    assert!(head_body.is_empty());

    let head_invalid_range_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=99-120")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(
        head_invalid_range_resp.status(),
        StatusCode::RANGE_NOT_SATISFIABLE
    );
    assert_eq!(
        head_invalid_range_resp
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok()),
        Some("bytes */10")
    );
    let head_invalid_range_body =
        body::to_bytes(head_invalid_range_resp.into_body(), 1_048_576).await?;
    assert!(head_invalid_range_body.is_empty());

    let updated_at: String =
        sqlx::query_scalar("SELECT CAST(updated_at AS TEXT) FROM playback_sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(&state.db_pool)
            .await?;
    assert_ne!(updated_at.trim(), "2000-01-01 00:00:00");

    sqlx::query("UPDATE playback_sessions SET mode = 'transcode' WHERE id = $1")
        .bind(session_id)
        .execute(&state.db_pool)
        .await?;
    let wrong_mode_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(wrong_mode_resp.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[tokio::test]
async fn phase22_direct_routes_reject_cross_user_revoked_expired_and_share_policy_bypass()
-> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase22-direct-route-security".to_string();
    settings.playback.stream_token_ttl_seconds = 60;
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase22.Direct.Route.Security.mp4",
        "mp4",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let play_body = || {
        json!({
            "media_item_id": fixture.item_id,
            "preferred_file_id": fixture.media_file_id,
            "network_type": "wan",
            "client_capabilities": {
                "profile_version": 2,
                "client_kind": "native_mpv",
                "direct_play_preferred": true,
                "supported_containers": ["mp4", "mkv"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "quality_mode": "original",
                "supports_auth_headers_for_media": true
            }
        })
    };

    let play_json = play_fixture_json(&fixture, play_body()).await?;
    assert_eq!(
        play_json.get("mode").and_then(Value::as_str),
        Some("direct_play")
    );
    let stream_url = play_json
        .get("streamUrl")
        .or_else(|| play_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url")
        .to_string();
    let session_id = play_json
        .get("sessionId")
        .or_else(|| play_json.get("session_id"))
        .and_then(Value::as_str)
        .expect("session_id")
        .to_string();
    let media_file_id = play_json
        .get("mediaFileId")
        .or_else(|| play_json.get("media_file_id"))
        .and_then(Value::as_str)
        .expect("media_file_id");
    assert!(
        play_json
            .get("decision_reasons")
            .and_then(Value::as_array)
            .is_some_and(|reasons| reasons
                .iter()
                .any(|reason| reason.as_str() == Some("remote_policy_applied"))),
        "expected remote policy reason: {play_json}"
    );
    assert!(
        play_json
            .get("remote_access")
            .and_then(|value| value.get("stream_token_expires_at"))
            .and_then(Value::as_str)
            .is_some(),
        "remote access contract should expose token expiry: {play_json}"
    );

    let other_user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(other_user_id.to_string())
        .bind("phase22-direct-other@example.com")
        .bind("hashed")
        .execute(&fixture.state.db_pool)
        .await?;
    let other_token = fixture
        .state
        .auth_service
        .issue_access_token(other_user_id)?
        .token;

    let cross_user_range_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(&stream_url)
                .header("authorization", format!("Bearer {other_token}"))
                .header("range", "bytes=0-3")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(cross_user_range_resp.status(), StatusCode::UNAUTHORIZED);

    let mismatched_url = stream_url.replacen(media_file_id, &Uuid::new_v4().to_string(), 1);
    let mismatched_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(&mismatched_url)
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(mismatched_resp.status(), StatusCode::UNAUTHORIZED);

    sqlx::query(
        "UPDATE playback_sessions SET token_expires_at = '2000-01-01T00:00:00Z' WHERE id = $1",
    )
    .bind(&session_id)
    .execute(&fixture.state.db_pool)
    .await?;
    let expired_token_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(&stream_url)
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(expired_token_resp.status(), StatusCode::UNAUTHORIZED);
    let expired_state: String =
        sqlx::query_scalar("SELECT state FROM playback_sessions WHERE id = $1 LIMIT 1")
            .bind(&session_id)
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(expired_state, "ended");

    let revoked_json = play_fixture_json(&fixture, play_body()).await?;
    let revoked_stream_url = revoked_json
        .get("streamUrl")
        .or_else(|| revoked_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url");
    let revoked_session_id = revoked_json
        .get("sessionId")
        .or_else(|| revoked_json.get("session_id"))
        .and_then(Value::as_str)
        .expect("session_id");
    let end_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{revoked_session_id}/end"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(end_resp.status(), StatusCode::OK);
    let revoked_stream_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(revoked_stream_url)
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(revoked_stream_resp.status(), StatusCode::UNAUTHORIZED);

    let share_json = play_fixture_json(&fixture, play_body()).await?;
    let share_stream_url = share_json
        .get("streamUrl")
        .or_else(|| share_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url");
    let share_session_id = share_json
        .get("sessionId")
        .or_else(|| share_json.get("session_id"))
        .and_then(Value::as_str)
        .expect("session_id");
    let deny_direct_policy = json!({
        "applied": true,
        "scope": "share",
        "policy_sources": ["phase22-test"],
        "user_id": fixture.user_id,
        "share_id": "phase22-no-downloads",
        "allow_downloads": false,
        "allow_direct_play": false,
        "allow_transcode": true,
        "allow_hardware_transcode": true,
        "reasons": ["remote_policy_downloads_disabled", "remote_policy_direct_play_disabled"]
    });
    sqlx::query(
        "UPDATE playback_sessions SET share_id = $1, remote_policy_json = $2 WHERE id = $3",
    )
    .bind("phase22-no-downloads")
    .bind(deny_direct_policy.to_string())
    .bind(share_session_id)
    .execute(&fixture.state.db_pool)
    .await?;
    let share_policy_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(share_stream_url)
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(share_policy_resp.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[tokio::test]
async fn phase22_wan_policy_selects_direct_stream_fixed_and_adaptive_modes() -> Result<()> {
    let remote_user_id = Uuid::new_v4();
    let mut direct_settings = test_settings_with_db();
    direct_settings.auth.access_token_secret = "phase22-direct-mode".to_string();
    direct_settings.playback.unknown_performance_policy = "allow_best_effort".to_string();
    direct_settings.playback.remote_user_policies.insert(
        remote_user_id.to_string(),
        PlaybackRemotePolicyOverride {
            max_remote_bitrate_bps: Some(6_000_000),
            max_resolution: Some("1080p".to_string()),
            allow_hardware_transcode: Some(false),
            max_sessions: Some(4),
            ..PlaybackRemotePolicyOverride::default()
        },
    );
    let direct_fixture = setup_playback_route_fixture_with_user(
        direct_settings,
        remote_user_id,
        "Phase22.Direct.Mode.mp4",
        "mp4",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let direct_json = play_fixture_json(
        &direct_fixture,
        json!({
            "media_item_id": direct_fixture.item_id,
            "preferred_file_id": direct_fixture.media_file_id,
            "network_type": "wan",
            "client_capabilities": {
                "profile_version": 2,
                "client_kind": "web",
                "direct_play_preferred": true,
                "supported_containers": ["mp4"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_hls_segment_types": ["fmp4"],
                "quality_mode": "fixed",
                "fixed_bitrate_bps": 8_000_000,
                "fixed_resolution": "1080p",
                "supports_auth_headers_for_media": true
            }
        }),
    )
    .await?;
    assert_phase22_remote_play_response(
        &direct_json,
        "direct_play",
        "playback.remote_user_policies",
    );
    assert!(
        direct_json
            .get("decision_reasons")
            .and_then(Value::as_array)
            .is_some_and(|reasons| {
                reasons.iter().any(|reason| {
                    reason.as_str() == Some("remote_policy_hardware_transcode_disabled")
                })
            }),
        "hardware transcode policy should be explicit: {direct_json}"
    );

    let mut direct_stream_settings = test_settings_with_db();
    direct_stream_settings.auth.access_token_secret = "phase22-direct-stream-mode".to_string();
    direct_stream_settings.playback.unknown_performance_policy = "allow_best_effort".to_string();
    direct_stream_settings
        .playback
        .remote_share_policies
        .insert(
            "phase22-share-stream".to_string(),
            PlaybackRemotePolicyOverride {
                max_remote_bitrate_bps: Some(6_000_000),
                allow_downloads: Some(false),
                allow_direct_play: Some(false),
                ..PlaybackRemotePolicyOverride::default()
            },
        );
    let direct_stream_fixture = setup_playback_route_fixture(
        direct_stream_settings,
        "Phase22.Direct.Stream.Mode.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let direct_stream_json = play_fixture_json(
        &direct_stream_fixture,
        json!({
            "media_item_id": direct_stream_fixture.item_id,
            "preferred_file_id": direct_stream_fixture.media_file_id,
            "network_type": "wan",
            "share_id": "phase22-share-stream",
            "client_capabilities": {
                "profile_version": 2,
                "client_kind": "web",
                "direct_play_preferred": true,
                "supported_containers": ["mp4"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_hls_segment_types": ["fmp4"],
                "quality_mode": "fixed",
                "fixed_bitrate_bps": 8_000_000,
                "fixed_resolution": "1080p",
                "supports_auth_headers_for_media": true
            }
        }),
    )
    .await?;
    assert_phase22_remote_play_response(
        &direct_stream_json,
        "direct_stream",
        "playback.remote_share_policies",
    );
    assert!(
        direct_stream_json
            .get("decision_reasons")
            .and_then(Value::as_array)
            .is_some_and(|reasons| reasons
                .iter()
                .any(|reason| reason.as_str() == Some("remote_policy_downloads_disabled"))),
        "share no-downloads policy should be explicit: {direct_stream_json}"
    );

    let mut fixed_settings = test_settings_with_db();
    fixed_settings.auth.access_token_secret = "phase22-fixed-mode".to_string();
    fixed_settings.playback.unknown_performance_policy = "allow_best_effort".to_string();
    fixed_settings.playback.default_wan_max_bitrate_bps = Some(3_000_000);
    fixed_settings.playback.default_remote_policy.max_resolution = Some("720p".to_string());
    let fixed_fixture = setup_playback_route_fixture(
        fixed_settings,
        "Phase22.Fixed.Transcode.Mode.mkv",
        "mkv",
        "hevc",
        "aac",
        1920,
        1080,
        8_000_000,
    )
    .await?;
    let fixed_json = play_fixture_json(
        &fixed_fixture,
        json!({
            "media_item_id": fixed_fixture.item_id,
            "preferred_file_id": fixed_fixture.media_file_id,
            "network_type": "wan",
            "client_capabilities": {
                "profile_version": 2,
                "client_kind": "mobile",
                "direct_play_preferred": false,
                "supported_containers": ["mp4"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_hls_segment_types": ["fmp4"],
                "quality_mode": "fixed",
                "fixed_bitrate_bps": 3_000_000,
                "fixed_resolution": "720p",
                "max_bitrate_bps": 3_000_000,
                "max_resolution": "720p"
            }
        }),
    )
    .await?;
    assert_phase22_remote_play_response(
        &fixed_json,
        "video_transcode",
        "playback.default_wan_max_bitrate_bps",
    );
    assert!(
        fixed_json
            .get("decision_reasons")
            .and_then(Value::as_array)
            .is_some_and(|reasons| reasons
                .iter()
                .any(|reason| reason.as_str() == Some("remote_policy_max_resolution_applied"))),
        "remote max-resolution policy should be explicit: {fixed_json}"
    );

    let mut adaptive_settings = test_settings_with_db();
    adaptive_settings.auth.access_token_secret = "phase22-adaptive-mode".to_string();
    adaptive_settings.playback.unknown_performance_policy = "allow_best_effort".to_string();
    adaptive_settings.playback.default_wan_max_bitrate_bps = Some(4_000_000);
    adaptive_settings.playback.allow_adaptive_transcode = true;
    adaptive_settings.playback.adaptive_quality_enabled = true;
    let adaptive_fixture = setup_playback_route_fixture(
        adaptive_settings,
        "Phase22.Adaptive.Transcode.Mode.mkv",
        "mkv",
        "hevc",
        "aac",
        1920,
        1080,
        8_000_000,
    )
    .await?;
    let adaptive_json = play_fixture_json(
        &adaptive_fixture,
        json!({
            "media_item_id": adaptive_fixture.item_id,
            "preferred_file_id": adaptive_fixture.media_file_id,
            "network_type": "wan",
            "client_capabilities": {
                "profile_version": 2,
                "client_kind": "web",
                "direct_play_preferred": false,
                "supported_containers": ["mp4"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_hls_segment_types": ["fmp4"],
                "quality_mode": "automatic",
                "automatic_min_bitrate_bps": 800_000,
                "automatic_max_bitrate_bps": 4_000_000,
                "automatic_min_resolution": "360p",
                "automatic_max_resolution": "1080p",
                "abr_support_type": "hls.js"
            }
        }),
    )
    .await?;
    assert_phase22_remote_play_response(
        &adaptive_json,
        "adaptive_transcode",
        "playback.default_wan_max_bitrate_bps",
    );
    assert_eq!(
        adaptive_json.get("adaptive").and_then(Value::as_bool),
        Some(true)
    );

    let mut limit_settings = test_settings_with_db();
    limit_settings.auth.access_token_secret = "phase22-session-limit".to_string();
    limit_settings.playback.default_remote_policy.max_sessions = Some(1);
    let limit_fixture = setup_playback_route_fixture(
        limit_settings,
        "Phase22.Session.Limit.Mode.mp4",
        "mp4",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let limit_body = json!({
        "media_item_id": limit_fixture.item_id,
        "preferred_file_id": limit_fixture.media_file_id,
        "network_type": "wan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "web",
            "direct_play_preferred": true,
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supported_hls_segment_types": ["fmp4"],
            "quality_mode": "fixed",
            "fixed_bitrate_bps": 5_000_000,
            "fixed_resolution": "720p"
        }
    });
    let first_limit_json = play_fixture_json(&limit_fixture, limit_body.clone()).await?;
    assert_phase22_remote_play_response(
        &first_limit_json,
        "direct_play",
        "playback.default_remote_policy",
    );
    let second_limit_resp = limit_fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", limit_fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(limit_body.to_string()))?,
        )
        .await?;
    let second_limit_status = second_limit_resp.status();
    let second_limit_bytes = body::to_bytes(second_limit_resp.into_body(), 1_048_576).await?;
    let second_limit_json: Value = serde_json::from_slice(&second_limit_bytes)?;
    assert_eq!(
        second_limit_status,
        StatusCode::CONFLICT,
        "body: {second_limit_json}"
    );
    assert_eq!(
        second_limit_json.get("code").and_then(Value::as_str),
        Some("transcode_capacity_exhausted")
    );
    assert_eq!(
        second_limit_json
            .pointer("/details/capacity/resource")
            .and_then(Value::as_str),
        Some("remote_user_sessions")
    );

    Ok(())
}

fn assert_phase22_remote_play_response(json: &Value, mode: &str, policy_source: &str) {
    assert_eq!(
        json.get("mode").and_then(Value::as_str),
        Some(mode),
        "{json}"
    );
    let reasons = json
        .get("decision_reasons")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing decision_reasons: {json}"));
    for expected in [
        "remote_policy_applied",
        "remote_policy_max_bitrate_applied",
        "remote_transport_https_required_by_policy",
    ] {
        assert!(
            reasons
                .iter()
                .any(|reason| reason.as_str() == Some(expected)),
            "missing reason {expected}: {json}"
        );
    }
    let sources = json
        .get("remote_policy")
        .and_then(|value| value.get("policy_sources"))
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing policy sources: {json}"));
    assert!(
        sources
            .iter()
            .any(|source| source.as_str() == Some(policy_source)),
        "missing policy source {policy_source}: {json}"
    );
    assert!(
        json.get("remote_access")
            .and_then(|value| value.get("lan_direct_endpoint"))
            .and_then(Value::as_str)
            .is_some(),
        "missing LAN endpoint contract: {json}"
    );
    assert!(
        json.get("stream_token_expires_at")
            .and_then(Value::as_str)
            .is_some(),
        "missing token expiry: {json}"
    );
}

#[tokio::test]
async fn phase22_remote_smoke_requires_tls_and_accepts_reverse_proxy_forwarding() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.environment = RunEnvironment::Production;
    settings.auth.access_token_secret = "phase22-remote-smoke".to_string();
    settings.playback.plan_contract_enabled = true;
    settings.playback.remote_require_https = true;
    settings.playback.remote_allow_insecure = false;
    settings.playback.remote_reverse_proxy_endpoint =
        Some("https://media.example.test".to_string());
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase22.Remote.Proxy.Smoke.mp4",
        "mp4",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "wan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "web",
            "direct_play_preferred": true,
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supported_hls_segment_types": ["fmp4"],
            "quality_mode": "fixed",
            "fixed_bitrate_bps": 5_000_000,
            "fixed_resolution": "720p",
            "supports_auth_headers_for_media": true
        }
    });

    let insecure_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    let insecure_status = insecure_resp.status();
    let insecure_bytes = body::to_bytes(insecure_resp.into_body(), 1_048_576).await?;
    let insecure_json: Value = serde_json::from_slice(&insecure_bytes)?;
    assert_eq!(
        insecure_status,
        StatusCode::CONFLICT,
        "body: {insecure_json}"
    );
    assert_eq!(
        insecure_json.get("code").and_then(Value::as_str),
        Some("remote_https_required")
    );

    let forwarded_tls_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .header("x-forwarded-proto", "https")
                .header("x-forwarded-host", "media.example.test")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    let forwarded_status = forwarded_tls_resp.status();
    let forwarded_bytes = body::to_bytes(forwarded_tls_resp.into_body(), 1_048_576).await?;
    let forwarded_json: Value = serde_json::from_slice(&forwarded_bytes)?;
    assert_eq!(forwarded_status, StatusCode::OK, "body: {forwarded_json}");
    assert_eq!(
        forwarded_json
            .pointer("/remote_access/reverse_proxy_endpoint")
            .and_then(Value::as_str),
        Some("https://media.example.test")
    );
    assert_eq!(
        forwarded_json
            .pointer("/remote_access/request_transport")
            .and_then(Value::as_str),
        Some("https_or_forwarded_https")
    );
    assert_eq!(
        forwarded_json
            .pointer("/remote_access/secure_connection_policy")
            .and_then(Value::as_str),
        Some("require_https")
    );

    Ok(())
}

async fn phase23_seed_active_session(
    pool: &sqlx::AnyPool,
    user_id: &str,
    media_file_id: &str,
    mode: &str,
    token: &str,
) -> Result<Uuid> {
    let session_id = Uuid::new_v4();
    let session_id_text = session_id.to_string();
    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, token, updated_at)
         VALUES ($1, $2, $3, $4, 'active', 'lan', 0, 21600, $5, CURRENT_TIMESTAMP)",
    )
    .bind(&session_id_text)
    .bind(user_id)
    .bind(media_file_id)
    .bind(mode)
    .bind(token)
    .execute(pool)
    .await?;
    Ok(session_id)
}

#[tokio::test]
async fn phase23_simulated_six_hour_direct_play_range_soak_continues_until_end() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase23-direct-soak".to_string();
    settings.playback.session_ttl_seconds = 6 * 60 * 60 + 300;
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase23.Direct.Range.Soak.mkv",
        "mkv",
        "hevc",
        "dts",
        1920,
        1080,
        8_000_000,
    )
    .await?;
    let play_json = play_fixture_json(
        &fixture,
        json!({
            "media_item_id": fixture.item_id,
            "preferred_file_id": fixture.media_file_id,
            "network_type": "lan",
            "client_capabilities": {
                "profile_version": 2,
                "client_kind": "native_mpv",
                "direct_play_preferred": true,
                "supported_containers": ["mkv"],
                "supported_video_codecs": ["hevc"],
                "supported_audio_codecs": ["dts"],
                "supports_auth_headers_for_media": true,
                "quality_mode": "original"
            }
        }),
    )
    .await?;
    assert_eq!(
        play_json.get("mode").and_then(Value::as_str),
        Some("direct_play")
    );
    let stream_url = play_json
        .get("streamUrl")
        .or_else(|| play_json.get("stream_url"))
        .and_then(Value::as_str)
        .context("stream url")?
        .to_string();
    let session_id = play_json
        .get("sessionId")
        .or_else(|| play_json.get("session_id"))
        .and_then(Value::as_str)
        .context("session id")?
        .to_string();

    for simulated_minute in (0..=360).step_by(15) {
        let start = (simulated_minute % 7) as u64;
        let end = start + 2;
        let resp = fixture
            .app
            .clone()
            .oneshot(
                Request::get(&stream_url)
                    .header("authorization", format!("Bearer {}", fixture.token))
                    .header("range", format!("bytes={start}-{end}"))
                    .body(Body::empty())?,
            )
            .await?;
        let status = resp.status();
        let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
        assert_eq!(
            status,
            StatusCode::PARTIAL_CONTENT,
            "minute {simulated_minute} body: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(bytes.len(), 3, "minute {simulated_minute}");
    }

    let active_state: String =
        sqlx::query_scalar("SELECT state FROM playback_sessions WHERE id = $1")
            .bind(&session_id)
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(active_state, "active");

    let end_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/end"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(end_resp.status(), StatusCode::OK);

    let after_end = fixture
        .app
        .clone()
        .oneshot(
            Request::get(&stream_url)
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("range", "bytes=0-2")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(after_end.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn phase23_simulated_six_hour_hls_direct_stream_soak_reads_segments_until_end() -> Result<()>
{
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase23-hls-soak".to_string();
    settings.playback.session_ttl_seconds = 6 * 60 * 60 + 300;
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase23.Hls.Direct.Stream.Soak.mkv",
        "mkv",
        "h264",
        "aac",
        1920,
        1080,
        6_000_000,
    )
    .await?;
    let session_id = Uuid::new_v4();
    let session_token = "phase23-hls-soak-session";
    let temp = tempdir()?;
    tokio::fs::write(
        temp.path().join("master.m3u8"),
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080\nmedia.m3u8\n",
    )
    .await?;
    tokio::fs::write(
        temp.path().join("media.m3u8"),
        "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4.000,\nsegment_00000.m4s\n#EXT-X-ENDLIST\n",
    )
    .await?;
    tokio::fs::write(temp.path().join("init.mp4"), b"init").await?;
    tokio::fs::write(temp.path().join("segment_00000.m4s"), b"segment").await?;
    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, token, updated_at)
         VALUES ($1, $2, $3, 'direct_stream', 'active', 'lan', 0, 21600, $4, CURRENT_TIMESTAMP)",
    )
    .bind(session_id.to_string())
    .bind(&fixture.user_id)
    .bind(&fixture.media_file_id)
    .bind(session_token)
    .execute(&fixture.state.db_pool)
    .await?;
    fixture
        .state
        .transcodes
        .insert_test_job_for_plan(
            session_id,
            temp.path().to_path_buf(),
            PlaybackMode::DirectStream,
            Delivery::HlsFmp4,
            0,
        )
        .await;

    for simulated_minute in (0..=360).step_by(20) {
        let segment_resp = fixture
            .app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/sessions/{session_id}/segment_00000.m4s?session={session_token}"
                ))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
            )
            .await?;
        let status = segment_resp.status();
        let bytes = body::to_bytes(segment_resp.into_body(), 1_048_576).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "minute {simulated_minute} body: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(&bytes[..], b"segment");

        let heartbeat_resp = fixture
            .app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/sessions/{session_id}/heartbeat"))
                    .header("authorization", format!("Bearer {}", fixture.token))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(heartbeat_resp.status(), StatusCode::OK);
        let resume_resp = fixture
            .app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/sessions/{session_id}/resume"))
                    .header("authorization", format!("Bearer {}", fixture.token))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(resume_resp.status(), StatusCode::OK);
    }

    let end_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/end"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(end_resp.status(), StatusCode::OK);
    assert!(
        fixture
            .state
            .transcodes
            .snapshot(session_id)
            .await
            .is_none()
    );
    assert!(tokio::fs::metadata(temp.path()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn phase23_simulated_six_hour_video_transcode_loop_soak_reads_segments_until_end()
-> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase23-video-soak".to_string();
    settings.playback.session_ttl_seconds = 6 * 60 * 60 + 300;
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase23.Video.Transcode.Loop.Soak.mkv",
        "mkv",
        "hevc",
        "dts",
        3840,
        2160,
        24_000_000,
    )
    .await?;
    let session_id = Uuid::new_v4();
    let session_token = "phase23-video-soak-session";
    let temp = tempdir()?;
    tokio::fs::write(
        temp.path().join("master.m3u8"),
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1920x1080,CODECS=\"avc1.640028,mp4a.40.2\"\nstream_0.m3u8\n",
    )
    .await?;
    tokio::fs::write(
        temp.path().join("stream_0.m3u8"),
        "#EXTM3U\n#EXT-X-VERSION:7\n#EXTINF:4.000,\nseg_0_00000.m4s\n#EXT-X-ENDLIST\n",
    )
    .await?;
    tokio::fs::write(temp.path().join("seg_0_00000.m4s"), b"transcoded-segment").await?;
    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, token, updated_at)
         VALUES ($1, $2, $3, 'video_transcode', 'active', 'wan', 0, 21600, $4, CURRENT_TIMESTAMP)",
    )
    .bind(session_id.to_string())
    .bind(&fixture.user_id)
    .bind(&fixture.media_file_id)
    .bind(session_token)
    .execute(&fixture.state.db_pool)
    .await?;
    fixture
        .state
        .transcodes
        .insert_test_job_for_plan(
            session_id,
            temp.path().to_path_buf(),
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            0,
        )
        .await;

    for simulated_minute in (0..=360).step_by(30) {
        let playlist_resp = fixture
            .app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/sessions/{session_id}/stream_0.m3u8?session={session_token}"
                ))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
            )
            .await?;
        assert_eq!(
            playlist_resp.status(),
            StatusCode::OK,
            "minute {simulated_minute}"
        );
        let segment_resp = fixture
            .app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/sessions/{session_id}/seg_0_00000.m4s?session={session_token}"
                ))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
            )
            .await?;
        let status = segment_resp.status();
        let bytes = body::to_bytes(segment_resp.into_body(), 1_048_576).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "minute {simulated_minute} body: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(&bytes[..], b"transcoded-segment");
    }

    let end_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/end"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(end_resp.status(), StatusCode::OK);
    assert!(
        fixture
            .state
            .transcodes
            .snapshot(session_id)
            .await
            .is_none()
    );
    assert!(tokio::fs::metadata(temp.path()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn phase23_concurrent_load_rejects_video_over_capacity_before_job_start() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase23-load-matrix".to_string();
    settings.playback.unknown_performance_policy = "allow_best_effort".to_string();
    settings.playback.max_active_video_transcodes = Some(4);
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase23.Concurrent.Load.mkv",
        "mkv",
        "hevc",
        "dts",
        3840,
        2160,
        24_000_000,
    )
    .await?;

    let direct_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["hevc"],
            "supported_audio_codecs": ["dts"],
            "supports_auth_headers_for_media": true,
            "quality_mode": "original"
        }
    });
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let app = fixture.app.clone();
        let token = fixture.token.clone();
        let request_body = direct_body.clone();
        tasks.push(tokio::spawn(async move {
            let resp = app
                .oneshot(
                    Request::post("/api/v1/play")
                        .header("authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .body(Body::from(request_body.to_string()))?,
                )
                .await?;
            let status = resp.status();
            let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
            let json: Value = serde_json::from_slice(&bytes)?;
            assert_eq!(status, StatusCode::OK, "body: {}", json);
            Ok::<Value, anyhow::Error>(json)
        }));
    }
    for task in tasks {
        let json = task.await??;
        assert_eq!(
            json.get("mode").and_then(Value::as_str),
            Some("direct_play")
        );
    }

    let mut temp_dirs = Vec::new();
    for idx in 0..10 {
        let session_id = phase23_seed_active_session(
            &fixture.state.db_pool,
            &fixture.user_id,
            &fixture.media_file_id,
            "direct_stream",
            &format!("phase23-remux-{idx}"),
        )
        .await?;
        let temp = tempdir()?;
        tokio::fs::write(temp.path().join("master.m3u8"), "#EXTM3U\nmedia.m3u8\n").await?;
        tokio::fs::write(
            temp.path().join("media.m3u8"),
            "#EXTM3U\nsegment_00000.m4s\n",
        )
        .await?;
        tokio::fs::write(temp.path().join("segment_00000.m4s"), b"segment").await?;
        fixture
            .state
            .transcodes
            .insert_test_job_for_plan(
                session_id,
                temp.path().to_path_buf(),
                PlaybackMode::DirectStream,
                Delivery::HlsFmp4,
                0,
            )
            .await;
        temp_dirs.push(temp);
    }
    let mixed_modes = [
        ("audio_transcode", PlaybackMode::AudioTranscode),
        ("video_transcode", PlaybackMode::VideoTranscode),
        ("video_transcode", PlaybackMode::VideoTranscode),
        ("adaptive_transcode", PlaybackMode::AdaptiveTranscode),
    ];
    for (idx, (mode, playback_mode)) in mixed_modes.iter().enumerate() {
        let session_id = phase23_seed_active_session(
            &fixture.state.db_pool,
            &fixture.user_id,
            &fixture.media_file_id,
            mode,
            &format!("phase23-mixed-{idx}"),
        )
        .await?;
        let temp = tempdir()?;
        tokio::fs::write(temp.path().join("master.m3u8"), "#EXTM3U\nstream_0.m3u8\n").await?;
        tokio::fs::write(
            temp.path().join("stream_0.m3u8"),
            "#EXTM3U\nseg_0_00000.m4s\n",
        )
        .await?;
        tokio::fs::write(temp.path().join("seg_0_00000.m4s"), b"segment").await?;
        fixture
            .state
            .transcodes
            .insert_test_job_for_plan(
                session_id,
                temp.path().to_path_buf(),
                *playback_mode,
                Delivery::HlsFmp4,
                0,
            )
            .await;
        temp_dirs.push(temp);
    }

    let before_count: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&fixture.state.db_pool)
            .await?;
    let transcode_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "wan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "native_mpv",
            "direct_play_preferred": false,
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supports_auth_headers_for_media": true,
            "quality_mode": "fixed",
            "max_bitrate_bps": 4_000_000
        }
    });

    let resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(transcode_body.to_string()))?,
        )
        .await?;
    let status = resp.status();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(status, StatusCode::CONFLICT, "body: {}", json);
    assert_eq!(
        json.get("code").and_then(Value::as_str),
        Some("transcode_capacity_exhausted")
    );
    assert!(
        json.pointer("/details/reasons")
            .and_then(Value::as_array)
            .is_some_and(|reasons| reasons
                .iter()
                .any(|reason| reason.as_str() == Some("transcode_capacity_exhausted"))),
        "body: {json}"
    );
    assert_eq!(
        json.pointer("/details/retry/strategy")
            .and_then(Value::as_str),
        Some("retry_same_request")
    );
    let after_count: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(
        after_count, before_count,
        "capacity rejection must not insert another session"
    );
    let active_direct_streams: String = sqlx::query_scalar(
        "SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions WHERE state = 'active' AND mode = 'direct_stream'",
    )
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(active_direct_streams, "10");
    let active_video_weight: String = sqlx::query_scalar(
        "SELECT CAST(COALESCE(SUM(CASE WHEN mode = 'adaptive_transcode' THEN 2 ELSE 1 END), 0) AS TEXT)
         FROM playback_sessions
         WHERE state = 'active' AND mode IN ('video_transcode', 'adaptive_transcode', 'transcode')",
    )
    .fetch_one(&fixture.state.db_pool)
    .await?;
    assert_eq!(active_video_weight, "4");
    assert_eq!(fixture.state.transcodes.startup_queue_len(), 0);
    fixture.state.transcodes.stop_all().await;
    drop(temp_dirs);
    Ok(())
}

#[tokio::test]
async fn phase23_capacity_edges_reject_per_user_temp_and_startup_before_session_insert()
-> Result<()> {
    let mut per_user_settings = test_settings_with_db();
    per_user_settings.auth.access_token_secret = "phase23-per-user".to_string();
    per_user_settings.playback.max_sessions_per_user = Some(1);
    let per_user = setup_playback_route_fixture(
        per_user_settings,
        "Phase23.Per.User.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    phase23_seed_active_session(
        &per_user.state.db_pool,
        &per_user.user_id,
        &per_user.media_file_id,
        "direct_play",
        "phase23-existing-user-session",
    )
    .await?;
    let direct_body = json!({
        "media_item_id": per_user.item_id,
        "preferred_file_id": per_user.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "quality_mode": "original"
        }
    });
    let before_per_user: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&per_user.state.db_pool)
            .await?;
    let per_user_resp = per_user
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", per_user.token))
                .header("content-type", "application/json")
                .body(Body::from(direct_body.to_string()))?,
        )
        .await?;
    let status = per_user_resp.status();
    let bytes = body::to_bytes(per_user_resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(status, StatusCode::CONFLICT, "body: {}", json);
    assert_eq!(
        json.pointer("/details/capacity/resource")
            .and_then(Value::as_str),
        Some("per_user_sessions")
    );
    assert!(
        crate::metrics::PLAYBACK_CAPACITY_LEVELS
            .with_label_values(&["active_sessions", "all", "all"])
            .get()
            >= 1
    );
    let after_per_user: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&per_user.state.db_pool)
            .await?;
    assert_eq!(after_per_user, before_per_user);

    let mut temp_settings = test_settings_with_db();
    temp_settings.auth.access_token_secret = "phase23-temp-limit".to_string();
    temp_settings.playback.max_temp_dir_bytes = Some(1);
    let temp_fixture = setup_playback_route_fixture(
        temp_settings,
        "Phase23.Temp.Limit.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let temp_root = crate::playback::jobs::playback_temp_root();
    let temp_limit_dir = temp_root.join(format!("phase23-temp-limit-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_limit_dir).await?;
    tokio::fs::write(temp_limit_dir.join("bytes.bin"), b"over-limit").await?;
    let hls_body = json!({
        "media_item_id": temp_fixture.item_id,
        "preferred_file_id": temp_fixture.media_file_id,
        "network_type": "wan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "native_mpv",
            "direct_play_preferred": false,
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "quality_mode": "fixed",
            "max_bitrate_bps": 2_000_000
        }
    });
    let before_temp: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&temp_fixture.state.db_pool)
            .await?;
    let temp_resp = temp_fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", temp_fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(hls_body.to_string()))?,
        )
        .await?;
    let temp_status = temp_resp.status();
    let temp_bytes = body::to_bytes(temp_resp.into_body(), 1_048_576).await?;
    let temp_json: Value = serde_json::from_slice(&temp_bytes)?;
    let _ = tokio::fs::remove_dir_all(&temp_limit_dir).await;
    assert_eq!(temp_status, StatusCode::CONFLICT, "body: {}", temp_json);
    assert_eq!(
        temp_json
            .pointer("/details/capacity/resource")
            .and_then(Value::as_str),
        Some("temp_dir_bytes")
    );
    assert!(
        crate::metrics::PLAYBACK_CAPACITY_LEVELS
            .with_label_values(&["temp_dir_bytes", "all", "all"])
            .get()
            >= 1
    );
    let after_temp: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&temp_fixture.state.db_pool)
            .await?;
    assert_eq!(after_temp, before_temp);

    let mut queue_settings = test_settings_with_db();
    queue_settings.auth.access_token_secret = "phase23-startup-queue".to_string();
    queue_settings.playback.max_startup_queue_length = Some(1);
    let queue_fixture = setup_playback_route_fixture(
        queue_settings,
        "Phase23.Startup.Queue.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    queue_fixture.state.transcodes.set_test_startup_queue_len(1);
    let queue_body = json!({
        "media_item_id": queue_fixture.item_id,
        "preferred_file_id": queue_fixture.media_file_id,
        "network_type": "wan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "native_mpv",
            "direct_play_preferred": false,
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "quality_mode": "fixed",
            "max_bitrate_bps": 2_000_000
        }
    });
    let before_queue: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&queue_fixture.state.db_pool)
            .await?;
    let queue_resp = queue_fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", queue_fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(queue_body.to_string()))?,
        )
        .await?;
    queue_fixture.state.transcodes.set_test_startup_queue_len(0);
    let queue_status = queue_resp.status();
    let queue_bytes = body::to_bytes(queue_resp.into_body(), 1_048_576).await?;
    let queue_json: Value = serde_json::from_slice(&queue_bytes)?;
    assert_eq!(queue_status, StatusCode::CONFLICT, "body: {}", queue_json);
    assert_eq!(
        queue_json
            .pointer("/details/capacity/resource")
            .and_then(Value::as_str),
        Some("startup_queue_length")
    );
    assert!(
        crate::metrics::PLAYBACK_CAPACITY_LEVELS
            .with_label_values(&["startup_queue_length", "all", "all"])
            .get()
            >= 1
    );
    let after_queue: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&queue_fixture.state.db_pool)
            .await?;
    assert_eq!(after_queue, before_queue);

    Ok(())
}

#[tokio::test]
async fn phase13_direct_stream_rollout_gate_returns_structured_error() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase13-direct-stream-gate-secret".to_string();
    settings.playback.hls_direct_stream_enabled = false;
    let fixture = setup_playback_route_fixture(
        settings,
        "Direct.Stream.Gate.2026.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "profile_version": 1,
            "client_kind": "web",
            "direct_play_preferred": false,
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supported_hls_segment_types": ["fmp4"],
            "quality_mode": "fixed",
            "max_bitrate_bps": 8_000_000
        }
    });
    let resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    let status = resp.status();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;

    assert_eq!(status, StatusCode::CONFLICT, "body: {}", json);
    assert_eq!(
        json.get("code").and_then(Value::as_str),
        Some("direct_stream_disabled")
    );
    assert_eq!(
        json.pointer("/details/flag").and_then(Value::as_str),
        Some("playback.hls_direct_stream_enabled")
    );
    assert_eq!(
        json.pointer("/details/plan_summary/mode")
            .and_then(Value::as_str),
        Some("direct_stream")
    );
    let session_count: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(
        session_count, "0",
        "gate rejection must not persist a session"
    );

    Ok(())
}

#[tokio::test]
async fn phase13_video_transcode_disabled_returns_structured_error() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase13-video-disabled-secret".to_string();
    settings.playback.allow_video_transcode = false;
    let fixture = setup_playback_route_fixture(
        settings,
        "Video.Transcode.Disabled.2026.mkv",
        "mkv",
        "hevc",
        "aac",
        1920,
        1080,
        8_000_000,
    )
    .await?;

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "wan",
        "client_capabilities": {
            "profile_version": 1,
            "client_kind": "web",
            "direct_play_preferred": false,
            "supported_containers": ["mp4"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supported_hls_segment_types": ["fmp4"],
            "quality_mode": "fixed",
            "max_bitrate_bps": 4_000_000
        }
    });
    let resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    let status = resp.status();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;

    assert_eq!(status, StatusCode::CONFLICT, "body: {}", json);
    assert_eq!(
        json.get("code").and_then(Value::as_str),
        Some("video_transcode_disabled")
    );
    assert!(
        json.pointer("/details/reasons")
            .and_then(Value::as_array)
            .is_some_and(|reasons| reasons
                .iter()
                .any(|reason| { reason.as_str() == Some("video_transcode_disabled_by_policy") })),
        "expected disabled video transcode reason: {json}"
    );
    let session_count: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(
        session_count, "0",
        "not-playable rejection must not persist a session"
    );

    Ok(())
}

#[tokio::test]
async fn phase13_capacity_gate_rejects_before_session_insert_and_records_metric() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase13-capacity-secret".to_string();
    settings.playback.max_active_sessions = Some(0);
    let fixture = setup_playback_route_fixture(
        settings,
        "Capacity.Gate.2026.mkv",
        "mkv",
        "h264",
        "aac",
        1280,
        720,
        2_000_000,
    )
    .await?;
    let labels = [
        "active_sessions",
        "direct_play",
        "direct_file",
        "native_mpv",
        "lan",
        "transcode_capacity_exhausted",
    ];
    let before = crate::metrics::PLAYBACK_CAPACITY_REJECTIONS
        .with_label_values(&labels)
        .get();

    let play_body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": "lan",
        "client_capabilities": {
            "profile_version": 1,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "quality_mode": "original"
        }
    });
    let resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/play")
                .header("authorization", format!("Bearer {}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(play_body.to_string()))?,
        )
        .await?;
    let status = resp.status();
    let bytes = body::to_bytes(resp.into_body(), 1_048_576).await?;
    let json: Value = serde_json::from_slice(&bytes)?;

    assert_eq!(status, StatusCode::CONFLICT, "body: {}", json);
    assert_eq!(
        json.get("code").and_then(Value::as_str),
        Some("transcode_capacity_exhausted")
    );
    assert_eq!(
        json.pointer("/details/capacity/resource")
            .and_then(Value::as_str),
        Some("active_sessions")
    );
    assert_eq!(
        crate::metrics::PLAYBACK_CAPACITY_REJECTIONS
            .with_label_values(&labels)
            .get(),
        before + 1
    );
    let session_count: String =
        sqlx::query_scalar("SELECT CAST(COUNT(*) AS TEXT) FROM playback_sessions")
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(
        session_count, "0",
        "capacity rejection must not persist a session"
    );

    Ok(())
}

#[tokio::test]
async fn phase24_diagnostics_bundle_and_admin_sessions_expose_support_evidence() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "phase24-diagnostics-secret".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "Phase24.Diagnostics.2026.mkv",
        "mkv",
        "hevc",
        "aac",
        3840,
        2160,
        18_000_000,
    )
    .await?;

    let session_id = Uuid::new_v4();
    let session_id_text = session_id.to_string();
    let temp = tempdir()?;
    tokio::fs::write(temp.path().join("master.m3u8"), "#EXTM3U\n").await?;
    tokio::fs::write(temp.path().join("seg_0_00000.m4s"), b"segment").await?;
    tokio::fs::write(
        temp.path().join("ffmpeg.log"),
        "Authorization: Bearer secret-token\nsegment failed token=secret-token\n",
    )
    .await?;
    let playback_plan = json!({
        "mode": "video_transcode",
        "delivery": "hls_fmp4",
        "media_file_id": fixture.media_file_id,
        "selected_video_track": 0,
        "selected_audio_track": 1,
        "selected_subtitle_track": 2,
        "video_action": "transcode",
        "audio_action": "copy",
        "subtitle_action": "burn_in",
        "hdr_action": "tone_map_to_sdr",
        "adaptive": false,
        "reasons": ["source_bitrate_exceeds_bandwidth_policy"],
        "hardware_acceleration": {
            "enabled": true,
            "api": "nvenc",
            "decoder": "cuda",
            "encoder": "h264_nvenc"
        },
        "compatibility_report": {
            "source_bitrate_bps": 18000000,
            "checks": []
        },
        "client_profile": {
            "client_kind": "web",
            "quality_mode": "fixed",
            "max_bitrate_bps": 4000000
        },
        "effective_policy": {
            "network_class": "wan",
            "max_bitrate_bps": 4000000,
            "allow_video_transcode": true
        }
    });
    let job_state = json!({
        "state": "failed",
        "mode": "video_transcode",
        "delivery": "hls_fmp4",
        "logical_start_seconds": 0.0,
        "temp_dir": temp.path().to_string_lossy(),
        "artifacts": ["master.m3u8", "seg_0_00000.m4s"],
        "started_at": "2026-07-02T00:00:00Z",
        "last_progress_at": "2026-07-02T00:00:02Z",
        "last_segment_at": "2026-07-02T00:00:02Z",
        "log_path": temp.path().join("ffmpeg.log").to_string_lossy(),
        "error": "startup_failed: ffmpeg exited before playlist token=secret-token",
        "error_code": "startup_failed",
        "error_kind": "ffmpeg_exit",
        "log_tail": "Authorization: Bearer secret-token\nfailed token=secret-token",
        "ffmpeg_command": [
            "ffmpeg",
            "-i",
            "http://127.0.0.1/media.mkv?session=secret-session&token=secret-token",
            "-f",
            "hls"
        ]
    });

    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, media_file_id, mode, state, network_type,
             logical_position_seconds, duration_seconds, client_capabilities,
             transcode_state, token, token_expires_at, remote_policy_json,
             playback_plan_json, job_state_json)
         VALUES ($1, $2, $3, 'video_transcode', 'error', 'wan', 0, 3600, $4, $5, $6, '2099-01-01 00:00:00', $7, $8, $9)",
    )
    .bind(session_id.to_string())
    .bind(&fixture.user_id)
    .bind(&fixture.media_file_id)
    .bind(json!({"client_kind": "web"}).to_string())
    .bind(job_state.to_string())
    .bind("diagnostic-session-token")
    .bind(json!({"applied": false, "scope": "none"}).to_string())
    .bind(playback_plan.to_string())
    .bind(job_state.to_string())
    .execute(&fixture.state.db_pool)
    .await?;
    crate::metrics::init_metrics();
    crate::metrics::PLAYBACK_ERRORS
        .with_label_values(&[
            "video_transcode",
            "hls_fmp4",
            "web",
            "wan",
            "ffmpeg_startup_failed",
            "hardware_decode_encode",
        ])
        .inc();

    let bundle_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/sessions/{session_id}/diagnostics"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let bundle_status = bundle_resp.status();
    let bundle_bytes = body::to_bytes(bundle_resp.into_body(), 1_048_576).await?;
    let bundle: Value = serde_json::from_slice(&bundle_bytes)?;
    assert_eq!(bundle_status, StatusCode::OK, "body: {}", bundle);
    assert_eq!(
        bundle.pointer("/session/token").and_then(Value::as_str),
        Some("[redacted]")
    );
    assert_eq!(
        bundle
            .pointer("/client_profile/client_kind")
            .and_then(Value::as_str),
        Some("web")
    );
    assert_eq!(
        bundle
            .pointer("/effective_policy/network_class")
            .and_then(Value::as_str),
        Some("wan")
    );
    assert_eq!(
        bundle
            .pointer("/source_probe/probe_status")
            .and_then(Value::as_str),
        Some("ok")
    );
    assert_eq!(
        bundle.get("error_taxonomy").and_then(Value::as_str),
        Some("ffmpeg_startup_failed")
    );
    let command = bundle
        .get("ffmpeg_command")
        .and_then(Value::as_array)
        .context("ffmpeg command in diagnostics bundle")?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(command.contains("session=[redacted]"));
    assert!(command.contains("token=[redacted]"));
    assert!(!command.contains("secret-token"));
    let stderr_tail = bundle
        .get("stderr_tail")
        .and_then(Value::as_str)
        .context("stderr tail in diagnostics bundle")?;
    assert!(stderr_tail.contains("[redacted]"));
    assert!(!stderr_tail.contains("secret-token"));
    assert!(
        bundle
            .get("artifacts")
            .and_then(Value::as_array)
            .is_some_and(|artifacts| artifacts
                .iter()
                .any(
                    |artifact| artifact.get("name").and_then(Value::as_str) == Some("master.m3u8")
                )),
        "expected artifact listing: {bundle}"
    );
    assert!(
        bundle
            .get("recent_server_metrics")
            .and_then(Value::as_array)
            .is_some_and(|metrics| !metrics.is_empty()),
        "expected playback metrics snapshot: {bundle}"
    );
    assert_eq!(
        bundle
            .pointer("/client_player_events/available")
            .and_then(Value::as_bool),
        Some(false)
    );

    let admin_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/api/v1/playback/admin/sessions")
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let admin_status = admin_resp.status();
    let admin_bytes = body::to_bytes(admin_resp.into_body(), 1_048_576).await?;
    let admin: Value = serde_json::from_slice(&admin_bytes)?;
    assert_eq!(admin_status, StatusCode::OK, "body: {}", admin);
    let session = admin
        .get("sessions")
        .and_then(Value::as_array)
        .and_then(|sessions| {
            sessions.iter().find(|session| {
                session.get("id").and_then(Value::as_str) == Some(session_id_text.as_str())
            })
        })
        .context("phase24 admin session row")?;
    assert_eq!(
        session
            .pointer("/selected_tracks/subtitle")
            .and_then(Value::as_i64),
        Some(2)
    );
    assert_eq!(
        session.pointer("/hardware/api").and_then(Value::as_str),
        Some("nvenc")
    );
    assert_eq!(
        session
            .get("bandwidth_estimate_bps")
            .and_then(Value::as_i64),
        Some(18_000_000)
    );
    assert_eq!(
        session.get("error_taxonomy").and_then(Value::as_str),
        Some("ffmpeg_startup_failed")
    );
    let expected_stop_url = format!("/api/v1/sessions/{session_id}/end");
    assert_eq!(
        session.get("stop_url").and_then(Value::as_str),
        Some(expected_stop_url.as_str())
    );

    let stop_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/end"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(stop_resp.status(), StatusCode::OK);
    let state: String = sqlx::query_scalar("SELECT state FROM playback_sessions WHERE id = $1")
        .bind(session_id.to_string())
        .fetch_one(&fixture.state.db_pool)
        .await?;
    assert_eq!(state, "ended");

    Ok(())
}

#[tokio::test]
async fn midm_diagnostics_bundle_redacts_segment_job_error_details() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "midm-diagnostics-redaction-secret".to_string();
    let fixture = setup_playback_route_fixture(
        settings,
        "MIDM.Diagnostics.Redaction.2026.mkv",
        "mkv",
        "h264",
        "aac",
        1920,
        1080,
        8_000_000,
    )
    .await?;

    let session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, media_file_id, mode, state, network_type,
             logical_position_seconds, duration_seconds, client_capabilities,
             transcode_state, token, token_expires_at, remote_policy_json,
             playback_plan_json, job_state_json, selected_item_type, selected_item_id)
         VALUES ($1, $2, $3, 'direct', 'playing', 'lan', 120, 1800, $4, '{}',
                 $5, '2099-01-01 00:00:00', $6, '{}', '{}', 'movie', $7)",
    )
    .bind(session_id.to_string())
    .bind(&fixture.user_id)
    .bind(&fixture.media_file_id)
    .bind(json!({"client_kind": "desktop"}).to_string())
    .bind("midm-diagnostic-session-token")
    .bind(json!({"applied": false, "scope": "none"}).to_string())
    .bind(&fixture.item_id)
    .execute(&fixture.state.db_pool)
    .await?;

    sqlx::query(
        "INSERT INTO media_segment_jobs
            (id, job_type, scope_type, scope_id, provider_kind, status,
             priority, attempts, max_attempts, started_at, finished_at, error_json)
         VALUES ($1, 'provider_refresh', 'media_file', $2, 'theintrodb', 'failed',
                 10, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $3)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(&fixture.media_file_id)
    .bind(
        json!({
            "reason": "provider_refresh_failed",
            "url": "https://theintrodb.example/segments?api_key=secret-api-key&token=secret-token",
            "error": "Authorization: Bearer secret-token",
            "headers": {
                "authorization": "Bearer secret-token"
            }
        })
        .to_string(),
    )
    .execute(&fixture.state.db_pool)
    .await?;

    let bundle_resp = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/sessions/{session_id}/diagnostics"))
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    let bundle_status = bundle_resp.status();
    let bundle_bytes = body::to_bytes(bundle_resp.into_body(), 1_048_576).await?;
    let bundle: Value = serde_json::from_slice(&bundle_bytes)?;
    assert_eq!(bundle_status, StatusCode::OK, "body: {}", bundle);

    let jobs = bundle
        .pointer("/media_interactions/recent_segment_jobs")
        .and_then(Value::as_array)
        .context("MIDM recent jobs in diagnostics bundle")?;
    let job = jobs
        .iter()
        .find(|job| job.get("provider_kind").and_then(Value::as_str) == Some("theintrodb"))
        .context("TheIntroDB job summary in diagnostics bundle")?;
    assert_eq!(
        job.get("error_reason").and_then(Value::as_str),
        Some("provider_refresh_failed")
    );
    assert!(job.get("error").is_none(), "raw job error leaked: {job}");
    assert!(
        job.get("error_json").is_none(),
        "raw job error leaked: {job}"
    );

    let serialized = serde_json::to_string(&bundle)?;
    for secret in [
        "secret-token",
        "secret-api-key",
        "Authorization",
        "authorization",
        "Bearer",
        "theintrodb.example/segments",
    ] {
        assert!(
            !serialized.contains(secret),
            "diagnostics bundle leaked {secret}: {serialized}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn hls_segment_serving_requires_registered_artifact_and_containment() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "hls-artifact-secret-key".to_string();
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("hls-artifact-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let media_item_id = Uuid::new_v4();
    let media_file_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let session_token = "session-token-for-artifacts";
    let temp = tempdir()?;
    let media_path = temp.path().join("Artifact.Movie.2024.mkv");
    tokio::fs::write(&media_path, b"source").await?;
    tokio::fs::write(temp.path().join("seg_0_00000.ts"), b"segment").await?;
    tokio::fs::write(temp.path().join("init_0.mp4"), b"init").await?;
    tokio::fs::write(temp.path().join("secret.ts"), b"secret").await?;
    tokio::fs::write(
        temp.path().join("master.m3u8"),
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nstream_0.m3u8?token=evil-master&x-plex-token=evil-plex&keep=master\n",
    )
    .await?;
    tokio::fs::write(
        temp.path().join("stream_0.m3u8"),
        "#EXTM3U\n#EXT-X-MAP:URI=\"init_0.mp4?access_token=evil-init&keep=init\"\n#EXTINF:4.000,\nseg_0_00000.ts?sid=evil-sid&keep=seg\n#EXT-X-ENDLIST\n",
    )
    .await?;
    tokio::fs::write(
        temp.path().join("sub_0.m3u8"),
        "#EXTM3U\nsub_0_00000.vtt?token=evil-sub&keep=sub\n",
    )
    .await?;
    tokio::fs::write(temp.path().join("sub_0_00000.vtt"), "WEBVTT\n").await?;

    #[cfg(unix)]
    {
        let outside = temp
            .path()
            .parent()
            .unwrap_or(temp.path())
            .join("outside-secret.ts");
        std::fs::write(&outside, b"outside")?;
        std::os::unix::fs::symlink(&outside, temp.path().join("seg_0_00001.ts"))?;
    }

    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, 'movie', '{}', 'Artifact Movie', 2024)",
    )
    .bind(media_item_id.to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
         VALUES ($1, $2, $3, 6, 'mkv', 'h264', 'aac', 1280, 720, 2000000, 'ok')",
    )
    .bind(media_file_id.to_string())
    .bind(media_item_id.to_string())
    .bind(media_path.to_string_lossy().to_string())
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, token, updated_at)
         VALUES ($1, $2, $3, 'transcode', 'active', 'lan', 0, 6, $4, '2099-01-01 00:00:00')",
    )
    .bind(session_id.to_string())
    .bind(user_id.to_string())
    .bind(media_file_id.to_string())
    .bind(session_token)
    .execute(&state.db_pool)
    .await?;

    state
        .transcodes
        .insert_test_job(session_id, temp.path().to_path_buf(), 1)
        .await;

    let missing_auth_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/master.m3u8?session={session_token}"
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(missing_auth_resp.status(), StatusCode::UNAUTHORIZED);

    let missing_session_resp = app
        .clone()
        .oneshot(
            Request::get(format!("/sessions/{session_id}/master.m3u8"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(missing_session_resp.status(), StatusCode::UNAUTHORIZED);

    let other_user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(other_user_id.to_string())
        .bind("hls-artifact-other-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;
    let other_token = state.auth_service.issue_access_token(other_user_id)?.token;
    let wrong_owner_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/master.m3u8?session={session_token}"
            ))
            .header("authorization", format!("Bearer {other_token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(wrong_owner_resp.status(), StatusCode::UNAUTHORIZED);

    let wrong_owner_segment_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/seg_0_00000.ts?session={session_token}"
            ))
            .header("authorization", format!("Bearer {other_token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(wrong_owner_segment_resp.status(), StatusCode::UNAUTHORIZED);

    let master_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/master.m3u8?session={session_token}&token=scoped-auth&ts=cache1"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    let master_status = master_resp.status();
    let master_bytes = body::to_bytes(master_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        master_status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&master_bytes)
    );
    let master_playlist = String::from_utf8_lossy(&master_bytes);
    assert!(master_playlist.contains("stream_0.m3u8?keep=master&session="));
    assert!(master_playlist.contains("&token=scoped-auth"));
    assert!(master_playlist.contains("&ts=cache1"));
    assert!(master_playlist.contains("sub_0.m3u8?"));
    assert!(master_playlist.contains("session=session-token-for-artifacts"));
    assert!(!master_playlist.contains("evil-master"));
    assert!(!master_playlist.contains("x-plex-token"));

    let valid_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/seg_0_00000.ts?session={session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    let valid_status = valid_resp.status();
    let valid_bytes = body::to_bytes(valid_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        valid_status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&valid_bytes)
    );
    assert_eq!(&valid_bytes[..], b"segment");

    let deny_hls_policy = json!({
        "applied": true,
        "scope": "share",
        "policy_sources": ["phase22-test"],
        "user_id": user_id.to_string(),
        "share_id": "phase22-no-transcode",
        "allow_downloads": true,
        "allow_direct_play": true,
        "allow_transcode": false,
        "allow_hardware_transcode": true,
        "reasons": ["remote_policy_transcode_disabled"]
    });
    sqlx::query(
        "UPDATE playback_sessions SET share_id = $1, remote_policy_json = $2 WHERE id = $3",
    )
    .bind("phase22-no-transcode")
    .bind(deny_hls_policy.to_string())
    .bind(session_id.to_string())
    .execute(&state.db_pool)
    .await?;
    let denied_share_segment_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/seg_0_00000.ts?session={session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(denied_share_segment_resp.status(), StatusCode::UNAUTHORIZED);
    sqlx::query(
        "UPDATE playback_sessions SET share_id = NULL, remote_policy_json = NULL WHERE id = $1",
    )
    .bind(session_id.to_string())
    .execute(&state.db_pool)
    .await?;

    let updated_at: String =
        sqlx::query_scalar("SELECT CAST(updated_at AS TEXT) FROM playback_sessions WHERE id = $1")
            .bind(session_id.to_string())
            .fetch_one(&state.db_pool)
            .await?;
    assert_ne!(updated_at.trim(), "2099-01-01 00:00:00");

    let playlist_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/stream_0.m3u8?session={session_token}&token=scoped-auth&ts=cache1"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    let playlist_status = playlist_resp.status();
    let playlist_bytes = body::to_bytes(playlist_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        playlist_status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&playlist_bytes)
    );
    let playlist = String::from_utf8_lossy(&playlist_bytes);
    assert!(playlist.contains("#EXT-X-MAP:URI=\"init_0.mp4?keep=init&session="));
    assert!(playlist.contains("seg_0_00000.ts?keep=seg&session="));
    assert!(playlist.contains("&token=scoped-auth"));
    assert!(playlist.contains("&ts=cache1"));
    assert!(!playlist.contains("access_token=evil-init"));
    assert!(!playlist.contains("sid=evil-sid"));

    let init_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/init_0.mp4?session={session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(init_resp.status(), StatusCode::OK);
    let init_bytes = body::to_bytes(init_resp.into_body(), 1_048_576).await?;
    assert_eq!(&init_bytes[..], b"init");

    let subtitle_playlist_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/sub_0.m3u8?session={session_token}&token=scoped-auth&ts=cache1"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(subtitle_playlist_resp.status(), StatusCode::OK);
    let subtitle_playlist_bytes =
        body::to_bytes(subtitle_playlist_resp.into_body(), 1_048_576).await?;
    let subtitle_playlist = String::from_utf8_lossy(&subtitle_playlist_bytes);
    assert!(subtitle_playlist.contains("sub_0_00000.vtt?keep=sub&session="));
    assert!(subtitle_playlist.contains("&token=scoped-auth"));
    assert!(subtitle_playlist.contains("&ts=cache1"));
    assert!(!subtitle_playlist.contains("evil-sub"));

    let subtitle_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/sub_0_00000.vtt?session={session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    let subtitle_status = subtitle_resp.status();
    let subtitle_bytes = body::to_bytes(subtitle_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        subtitle_status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&subtitle_bytes)
    );
    assert!(String::from_utf8_lossy(&subtitle_bytes).contains("WEBVTT"));

    sqlx::query("UPDATE playback_sessions SET updated_at = '2099-01-01 00:00:00' WHERE id = $1")
        .bind(session_id.to_string())
        .execute(&state.db_pool)
        .await?;
    let poll_resp = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/sessions/{session_id}/poll"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(poll_resp.status(), StatusCode::OK);
    let poll_updated_at: String =
        sqlx::query_scalar("SELECT CAST(updated_at AS TEXT) FROM playback_sessions WHERE id = $1")
            .bind(session_id.to_string())
            .fetch_one(&state.db_pool)
            .await?;
    assert_ne!(poll_updated_at.trim(), "2099-01-01 00:00:00");

    sqlx::query("UPDATE playback_sessions SET updated_at = '2099-01-01 00:00:00' WHERE id = $1")
        .bind(session_id.to_string())
        .execute(&state.db_pool)
        .await?;
    let heartbeat_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/heartbeat"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(heartbeat_resp.status(), StatusCode::OK);
    let heartbeat_updated_at: String =
        sqlx::query_scalar("SELECT CAST(updated_at AS TEXT) FROM playback_sessions WHERE id = $1")
            .bind(session_id.to_string())
            .fetch_one(&state.db_pool)
            .await?;
    assert_ne!(heartbeat_updated_at.trim(), "2099-01-01 00:00:00");

    let present_but_unregistered = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/secret.ts?session={session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(present_but_unregistered.status(), StatusCode::NOT_FOUND);

    let encoded_traversal = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/%2e%2e%2Fsecret.ts?session={session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(encoded_traversal.status(), StatusCode::NOT_FOUND);

    #[cfg(unix)]
    {
        let symlink_escape = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/sessions/{session_id}/seg_0_00001.ts?session={session_token}"
                ))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
            )
            .await?;
        assert_eq!(symlink_escape.status(), StatusCode::NOT_FOUND);
    }

    sqlx::query("UPDATE playback_sessions SET updated_at = '2000-01-01 00:00:00' WHERE id = $1")
        .bind(session_id.to_string())
        .execute(&state.db_pool)
        .await?;
    let expired_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{session_id}/seg_0_00000.ts?session={session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(expired_resp.status(), StatusCode::UNAUTHORIZED);
    let expired_state: String =
        sqlx::query_scalar("SELECT state FROM playback_sessions WHERE id = $1 LIMIT 1")
            .bind(session_id.to_string())
            .fetch_one(&state.db_pool)
            .await?;
    assert_eq!(expired_state, "ended");
    assert!(state.transcodes.snapshot(session_id).await.is_none());
    assert!(
        !temp.path().exists(),
        "expired HLS session should remove temp artifacts"
    );

    let direct_session_id = Uuid::new_v4();
    let direct_session_token = "direct-stream-session-token";
    let direct_temp = tempdir()?;
    tokio::fs::write(
        direct_temp.path().join("master.m3u8"),
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nmedia.m3u8\n",
    )
    .await?;
    tokio::fs::write(
        direct_temp.path().join("media.m3u8"),
        "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4.000,\nsegment_00000.m4s\n#EXT-X-ENDLIST\n",
    )
    .await?;
    tokio::fs::write(direct_temp.path().join("init.mp4"), b"init").await?;
    tokio::fs::write(direct_temp.path().join("segment_00000.m4s"), b"segment").await?;
    tokio::fs::write(direct_temp.path().join("seg_0_00000.ts"), b"old-segment").await?;
    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, token, updated_at)
         VALUES ($1, $2, $3, 'direct_stream', 'active', 'lan', 0, 6, $4, '2099-01-01 00:00:00')",
    )
    .bind(direct_session_id.to_string())
    .bind(user_id.to_string())
    .bind(media_file_id.to_string())
    .bind(direct_session_token)
    .execute(&state.db_pool)
    .await?;
    state
        .transcodes
        .insert_test_job_for_plan(
            direct_session_id,
            direct_temp.path().to_path_buf(),
            PlaybackMode::DirectStream,
            Delivery::HlsFmp4,
            0,
        )
        .await;

    let direct_master_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{direct_session_id}/master.m3u8?session={direct_session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(direct_master_resp.status(), StatusCode::OK);
    let direct_master_bytes = body::to_bytes(direct_master_resp.into_body(), 1_048_576).await?;
    let direct_master = String::from_utf8_lossy(&direct_master_bytes);
    assert!(direct_master.contains("media.m3u8?session=direct-stream-session-token"));

    let direct_media_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{direct_session_id}/media.m3u8?session={direct_session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(direct_media_resp.status(), StatusCode::OK);
    let direct_media_bytes = body::to_bytes(direct_media_resp.into_body(), 1_048_576).await?;
    let direct_media = String::from_utf8_lossy(&direct_media_bytes);
    assert!(
        direct_media.contains("#EXT-X-MAP:URI=\"init.mp4?session=direct-stream-session-token\"")
    );
    assert!(direct_media.contains("segment_00000.m4s?session=direct-stream-session-token"));

    let direct_init_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{direct_session_id}/init.mp4?session={direct_session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(direct_init_resp.status(), StatusCode::OK);
    let direct_init_bytes = body::to_bytes(direct_init_resp.into_body(), 1_048_576).await?;
    assert_eq!(&direct_init_bytes[..], b"init");

    let direct_segment_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{direct_session_id}/segment_00000.m4s?session={direct_session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(direct_segment_resp.status(), StatusCode::OK);
    let direct_segment_bytes = body::to_bytes(direct_segment_resp.into_body(), 1_048_576).await?;
    assert_eq!(&direct_segment_bytes[..], b"segment");

    let old_transcode_name_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{direct_session_id}/seg_0_00000.ts?session={direct_session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(old_transcode_name_resp.status(), StatusCode::NOT_FOUND);

    let end_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{direct_session_id}/end"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(end_resp.status(), StatusCode::OK);
    assert!(state.transcodes.snapshot(direct_session_id).await.is_none());
    assert!(
        !direct_temp.path().exists(),
        "ended direct-stream HLS session should remove temp artifacts"
    );
    let ended_segment_resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/sessions/{direct_session_id}/segment_00000.m4s?session={direct_session_token}"
            ))
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(ended_segment_resp.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[tokio::test]
#[ignore = "requires ELIXIR_TEST_MEDIA_PATH or default sample file present"]
async fn direct_play_range_integration_when_media_present() -> Result<()> {
    use std::path::Path;

    let default_path = "/Users/ryanhotard/downloads/Solo.Leveling.S02E02.I.Suppose.You.Arent.Aware.1080p.CR.WEB-DL.AAC2.0.H.264.DUAL-VARYG.mkv";
    let media_path =
        std::env::var("ELIXIR_TEST_MEDIA_PATH").unwrap_or_else(|_| default_path.to_string());

    if !Path::new(&media_path).exists() {
        return Ok(());
    }

    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "direct-live-secret-key".to_string();
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("direct-live-user@example.com")
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;

    let candidate = MediaFileCandidate {
        identity: MediaIdentity {
            r#type: crate::db::models::MediaType::Movie,
            external_ids: ExternalIds::default(),
            title: "Direct Live Sample".to_string(),
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

    let play_body = serde_json::json!({
        "media_item_id": media_item_id,
        "preferred_file_id": null,
        "network_type": "lan",
        "client_capabilities": {
            "profile_version": 2,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mp4", "mkv", "mov", "avi", "webm"],
            "supported_video_codecs": ["h264", "hevc", "h265", "av1", "mpeg4", "mpeg2video", "vc1"],
            "supported_audio_codecs": ["aac", "ac3", "eac3", "dts", "truehd", "opus", "vorbis", "flac", "mp3"],
            "supported_subtitle_codecs": ["srt", "webvtt", "ass", "ssa", "mov_text", "pgs", "dvd_subtitle"],
            "supports_auth_headers_for_media": true,
            "quality_mode": "original"
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
    let play_status = play_resp.status();
    let play_bytes = body::to_bytes(play_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        play_status,
        StatusCode::OK,
        "play failed: {}",
        String::from_utf8_lossy(&play_bytes)
    );
    let play_json: Value = serde_json::from_slice(&play_bytes)?;
    assert_eq!(
        play_json.get("mode").and_then(Value::as_str),
        Some("direct_play")
    );
    assert_eq!(
        play_json
            .get("server_seek_required")
            .and_then(Value::as_bool),
        Some(false)
    );
    let stream_url = play_json
        .get("streamUrl")
        .or_else(|| play_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url");
    assert!(!stream_url.contains("token=") && !stream_url.contains("access_token="));
    let session_id = play_json
        .get("sessionId")
        .or_else(|| play_json.get("session_id"))
        .and_then(Value::as_str)
        .expect("session_id");

    let head_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(head_resp.status(), StatusCode::OK);
    assert_eq!(
        head_resp
            .headers()
            .get("accept-ranges")
            .and_then(|value| value.to_str().ok()),
        Some("bytes")
    );
    let head_body = body::to_bytes(head_resp.into_body(), 1_048_576).await?;
    assert!(head_body.is_empty());

    let range_resp = app
        .clone()
        .oneshot(
            Request::get(stream_url)
                .header("authorization", format!("Bearer {token}"))
                .header("range", "bytes=0-1048575")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(range_resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        range_resp
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.starts_with("bytes 0-")),
        Some(true)
    );
    let range_body = body::to_bytes(range_resp.into_body(), 2_000_000).await?;
    assert!(!range_body.is_empty());

    let seek_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/seek"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "position_seconds": 30.0 }).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(seek_resp.status(), StatusCode::UNAUTHORIZED);

    let end_resp = app
        .oneshot(
            Request::post(format!("/api/v1/sessions/{session_id}/end"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(end_resp.status(), StatusCode::OK);

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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
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

    // Use a Plex-style capability profile: MP4/H.264/AAC can direct play, while
    // MKV/HEVC/AV1/etc. should fall through to the transcoding path.
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
    let play_status = play_resp.status();
    let play_bytes = body::to_bytes(play_resp.into_body(), 1_048_576).await?;
    assert_eq!(
        play_status,
        StatusCode::OK,
        "play failed: {}",
        String::from_utf8_lossy(&play_bytes)
    );
    let play_json: Value = serde_json::from_slice(&play_bytes)?;
    let mode = play_json.get("mode").and_then(Value::as_str);
    let stream_url = play_json
        .get("streamUrl")
        .or_else(|| play_json.get("stream_url"))
        .and_then(Value::as_str)
        .expect("stream_url");

    match mode {
        Some("direct_play") => {
            let direct_resp = app
                .clone()
                .oneshot(
                    Request::get(stream_url)
                        .header("authorization", format!("Bearer {token}"))
                        .header("range", "bytes=0-65535")
                        .body(Body::empty())?,
                )
                .await?;
            let direct_status = direct_resp.status();
            let direct_body = body::to_bytes(direct_resp.into_body(), 1_048_576).await?;
            assert_eq!(
                direct_status,
                StatusCode::PARTIAL_CONTENT,
                "direct stream failed: {}",
                String::from_utf8_lossy(&direct_body)
            );
            assert!(!direct_body.is_empty(), "direct stream should return data");
        }
        Some("transcode")
        | Some("direct_stream")
        | Some("audio_transcode")
        | Some("subtitle_transcode")
        | Some("video_transcode")
        | Some("adaptive_transcode") => {
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
            let session_base = stream_url
                .split_once("/master.m3u8")
                .map(|(base, _)| base.to_string())
                .or_else(|| {
                    stream_url
                        .rsplit_once('/')
                        .map(|(base, _)| base.to_string())
                })
                .context("session playlist base")?;
            let resolve_session_url = |entry: &str| {
                if entry.starts_with('/') {
                    entry.to_string()
                } else {
                    format!("{session_base}/{entry}")
                }
            };
            let first_entry = playlist_str
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with('#'))
                .context("master playlist entry")?;

            let media_playlist = if first_entry.contains(".m3u8") {
                let media_playlist_url = resolve_session_url(first_entry);
                let media_playlist_resp = app
                    .clone()
                    .oneshot(
                        Request::get(&media_playlist_url)
                            .header("authorization", format!("Bearer {token}"))
                            .body(Body::empty())?,
                    )
                    .await?;
                let media_playlist_status = media_playlist_resp.status();
                let media_playlist_body =
                    body::to_bytes(media_playlist_resp.into_body(), 5_000_000).await?;
                assert_eq!(
                    media_playlist_status,
                    StatusCode::OK,
                    "media playlist failed: {}",
                    String::from_utf8_lossy(&media_playlist_body)
                );
                String::from_utf8(media_playlist_body.to_vec())?
            } else {
                playlist_str.to_string()
            };

            let first_seg = media_playlist
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with('#') && !line.contains(".m3u8"))
                .context("media segment entry")?;
            let segment_url = resolve_session_url(first_seg);

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
        }
        other => anyhow::bail!("unexpected playback mode: {other:?}"),
    }

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

    let store = ExtensionStore::new(&db_pool);
    let instances = store.list_instances(Some(&package.extension_id)).await?;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_name, "default");
    assert!(instances[0].enabled);

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
async fn extensions_install_auto_provisions_default_instance_for_zero_config_module() -> Result<()>
{
    let temp = tempdir()?;
    let package_dir = temp.path().join("byparr-module");
    std::fs::create_dir_all(&package_dir)?;
    let manifest = r#"id: elixir.modules.byparr
version: 1.0.1
kind: module
name: "Byparr"
provides:
  - capability: indexer.proxy
    slot: byparr
    implementation: byparr
runtime:
  type: container
  image: "example/byparr:1"
  network: "elixir_net"
  service_name: "elx-byparr"
networking:
  service_port:
    scheme: http
    container_port: 8191
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

    let store = ExtensionStore::new(&db_pool);
    let instances = store.list_instances(Some("elixir.modules.byparr")).await?;
    assert_eq!(instances.len(), 1, "expected auto-provisioned instance");
    assert_eq!(instances[0].instance_name, "default");
    assert!(instances[0].enabled);

    Ok(())
}

#[tokio::test]
async fn extensions_allows_bundled_elx_install_without_signature_when_manifest_declares_key()
-> Result<()> {
    let temp = tempdir()?;
    let bundled_dir = temp.path().join("bundled");
    std::fs::create_dir_all(&bundled_dir)?;
    let package_path = bundled_dir.join("prowlarr-nzbgeek-connector.elx");
    let manifest = r#"id: elixir.connectors.prowlarr_nzbgeek
version: 1.0.0
kind: connector
name: "Prowlarr NZBGeek Indexer"
publisher:
  name: "Elixir"
  key_id: "ed25519:koGwR9yOr6ynyG9xjnVlVjQ9B61vJtdCqEPtfV/avq0="
permissions:
  - drivers.configure.indexer.registry
targets:
  - capability: indexer.registry
    slot: default
actions:
  - type: driver_patch
    target:
      capability: indexer.registry
      slot: default
    patch:
      op: register_indexers
      indexers:
        - name: "NZBGeek"
          implementation: "NZBgeek"
          url: "https://api.nzbgeek.info"
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
        Some("elixir.connectors.prowlarr_nzbgeek")
    );

    Ok(())
}

#[tokio::test]
async fn extensions_catalog_lists_bundled_elx_packages_without_registries() -> Result<()> {
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
"#;
    let file = File::create(&package_path)?;
    let mut zip = ZipWriter::new(file);
    let options = FileOptions::<()>::default();
    zip.start_file("manifest.yaml", options)?;
    zip.write_all(manifest.as_bytes())?;
    zip.finish()?;

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.bundled_dir = bundled_dir.to_string_lossy().to_string();
    settings.extensions.registries = Vec::new();

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

    let response = app
        .oneshot(Request::get("/api/v1/extensions/catalog").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let available = payload["available"].as_array().cloned().unwrap_or_default();
    let entry = available
        .iter()
        .find(|item| item["id"].as_str() == Some("elixir.modules.qbittorrent"))
        .expect("expected bundled qBittorrent package in catalog");
    assert_eq!(entry["download_url"].as_str(), Some(""));
    assert_eq!(
        entry["package_path"].as_str(),
        Some(package_path.to_string_lossy().as_ref())
    );

    Ok(())
}

#[tokio::test]
async fn torrentio_candidate_provider_marketplace_lifecycle_matrix() -> Result<()> {
    let temp = tempdir()?;
    let identity = test_signing_identity();
    let extension_id = "elixir.sources.torrentio_stremio";
    let package_v1 = build_signed_package_from_manifest(
        temp.path(),
        "torrentio-0.1.0.elx",
        torrentio_candidate_provider_manifest("0.1.0", &identity.publisher_key_id),
        extension_id.to_string(),
        "0.1.0".to_string(),
        &identity,
    )
    .await?;
    let package_v2 = build_signed_package_from_manifest(
        temp.path(),
        "torrentio-0.2.0.elx",
        torrentio_candidate_provider_manifest("0.2.0", &identity.publisher_key_id),
        extension_id.to_string(),
        "0.2.0".to_string(),
        &identity,
    )
    .await?;
    let package_v1_bytes = tokio::fs::read(&package_v1.path).await?;
    let package_v2_bytes = tokio::fs::read(&package_v2.path).await?;

    let v1_for_registry = package_v1.clone();
    let v2_for_registry = package_v2.clone();
    let (addr, shutdown_tx) = start_registry_server_with_packages(
        move |addr| {
            json!({
                "registry_version": 1,
                "extensions": [
                    {
                        "id": v1_for_registry.extension_id,
                        "version": v1_for_registry.version,
                        "download_url": format!("http://{addr}/torrentio-0.1.0.elx"),
                        "sha256": v1_for_registry.hash,
                        "signature": v1_for_registry.signature,
                        "publisher_key_id": v1_for_registry.publisher_key_id,
                        "trust": "community"
                    },
                    {
                        "id": v2_for_registry.extension_id,
                        "version": v2_for_registry.version,
                        "download_url": format!("http://{addr}/torrentio-0.2.0.elx"),
                        "sha256": v2_for_registry.hash,
                        "signature": v2_for_registry.signature,
                        "publisher_key_id": v2_for_registry.publisher_key_id,
                        "trust": "community"
                    }
                ]
            })
        },
        BTreeMap::from([
            ("torrentio-0.1.0.elx".to_string(), package_v1_bytes),
            ("torrentio-0.2.0.elx".to_string(), package_v2_bytes),
        ]),
    )
    .await?;

    let mut settings = test_settings_with_db();
    settings.extensions.storage_root = temp.path().join("extensions").to_string_lossy().to_string();
    settings.extensions.registries = vec![format!("http://{addr}/registry.json")];
    settings.extensions.allow_unsigned = false;
    settings.extensions.allow_directory_install = false;
    let storage_root = settings.extensions.storage_root.clone();

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

    let catalog_resp = app
        .clone()
        .oneshot(Request::post("/api/v1/extensions/registries/refresh").body(Body::empty())?)
        .await?;
    assert_eq!(catalog_resp.status(), StatusCode::OK);
    let catalog_body = body::to_bytes(catalog_resp.into_body(), 1_048_576).await?;
    let catalog_json: Value = serde_json::from_slice(&catalog_body)?;
    let available = catalog_json["available"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let entry_for = |version: &str| -> Value {
        available
            .iter()
            .find(|entry| {
                entry.get("id").and_then(Value::as_str) == Some(extension_id)
                    && entry.get("version").and_then(Value::as_str) == Some(version)
            })
            .cloned()
            .unwrap_or_else(|| panic!("catalog entry {extension_id} {version} not found"))
    };
    let v1_entry = entry_for("0.1.0");
    let v2_entry = entry_for("0.2.0");

    let install_v1 = json!({
        "downloadUrl": v1_entry.get("download_url").and_then(Value::as_str).unwrap_or_default()
    });
    let install_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_v1.to_string()))?,
        )
        .await?;
    let install_status = install_resp.status();
    let install_body = body::to_bytes(install_resp.into_body(), 1_048_576).await?;
    let install_json: Value = serde_json::from_slice(&install_body)?;
    assert_eq!(
        install_status,
        StatusCode::OK,
        "install failed: {install_json}"
    );
    assert_eq!(
        install_json.get("extension_id").and_then(Value::as_str),
        Some(extension_id)
    );
    assert_eq!(
        install_json.get("version").and_then(Value::as_str),
        Some("0.1.0")
    );

    let store = ExtensionStore::new(&db_pool);
    let installed = store
        .get_extension(extension_id)
        .await?
        .expect("installed extension");
    assert_eq!(
        installed.package_hash.as_deref(),
        Some(package_v1.hash.as_str())
    );
    let instances = store.list_instances(Some(extension_id)).await?;
    assert_eq!(instances.len(), 1, "expected one default instance");
    let instance_id = instances[0].instance_id;
    assert_eq!(instances[0].instance_name, "default");
    assert!(instances[0].enabled);
    let default_config = instances[0]
        .config_json
        .as_ref()
        .expect("torrentio default instance config should be seeded");
    assert_eq!(
        default_config.get("baseUrl").and_then(Value::as_str),
        Some("https://torrentio.strem.fun")
    );
    assert_eq!(
        default_config.get("routePolicy").and_then(Value::as_str),
        Some("debrid_first")
    );
    assert_eq!(
        default_config.get("resultLimit").and_then(Value::as_i64),
        Some(50)
    );
    assert_eq!(
        default_config.get("timeoutMs").and_then(Value::as_i64),
        Some(12000)
    );
    assert_eq!(
        default_config.get("retryBackoffMs").and_then(Value::as_i64),
        Some(300)
    );
    assert_eq!(
        default_config.get("configVersion").and_then(Value::as_str),
        Some("elixir.sources.torrentio_stremio@0.1.0")
    );

    let unpacked_v1 = PathBuf::from(&storage_root)
        .join("unpacked")
        .join(extension_id)
        .join("0.1.0");
    assert!(
        tokio::fs::metadata(unpacked_v1.join("manifest.yaml"))
            .await
            .is_ok(),
        "expected v1 package to be unpacked"
    );

    let pending_status = extension_status_summary_item(&app, extension_id).await?;
    assert_eq!(
        pending_status.get("statusCode").and_then(Value::as_str),
        Some("provider_registration_pending")
    );

    let instance_config = json!({
        "configVersion": "elixir.sources.torrentio_stremio@0.1.0",
        "baseUrl": "https://torrentio.strem.fun",
        "addonPath": "",
        "routePolicy": "debrid_first",
        "allowedQualities": "2160p,1080p",
        "requiredLanguages": "en,ja",
        "maxSizeGb": 25,
        "resultLimit": 25,
        "timeoutMs": 9000,
        "retryCount": 2,
        "retryBackoffMs": 750,
        "minRequestIntervalMs": 1000,
        "maxLookupAttempts": 4,
        "releaseDelaySeconds": 1800
    });
    store
        .update_instance_config(instance_id, Some(&instance_config))
        .await?;
    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "acquisition.candidate_provider".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("torrentio_stremio".to_string()),
            scope_json: Some(json!({
                "media_types": ["movie", "tv", "anime"],
                "actions": ["search"],
                "requires_account": false,
                "required_fields": []
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-torrentio-source",
                "port": 8097,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_provider_readiness(provider_id, ProviderReadinessPhase::DriverReady, None)
        .await?;
    let ready_status = extension_status_summary_item(&app, extension_id).await?;
    assert_eq!(
        ready_status.get("statusCode").and_then(Value::as_str),
        Some("ready")
    );

    store.delete_provider(provider_id).await?;
    let recovered_provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id: recovered_provider_id,
            instance_id,
            capability: "acquisition.candidate_provider".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("torrentio_stremio".to_string()),
            scope_json: Some(json!({
                "media_types": ["movie", "tv", "anime"],
                "actions": ["search"],
                "requires_account": false,
                "required_fields": []
            })),
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-torrentio-source",
                "port": 8097,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_provider_readiness(
            recovered_provider_id,
            ProviderReadinessPhase::DriverReady,
            None,
        )
        .await?;
    let recovered_instance = store
        .get_instance(instance_id)
        .await?
        .expect("recovered instance");
    assert_eq!(
        recovered_instance.config_json.as_ref(),
        Some(&instance_config)
    );

    let install_v2 = json!({
        "downloadUrl": v2_entry.get("download_url").and_then(Value::as_str).unwrap_or_default()
    });
    let upgrade_resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/extensions/install")
                .header("content-type", "application/json")
                .body(Body::from(install_v2.to_string()))?,
        )
        .await?;
    let upgrade_status = upgrade_resp.status();
    let upgrade_body = body::to_bytes(upgrade_resp.into_body(), 1_048_576).await?;
    let upgrade_json: Value = serde_json::from_slice(&upgrade_body)?;
    assert_eq!(
        upgrade_status,
        StatusCode::OK,
        "upgrade failed: {upgrade_json}"
    );
    assert_eq!(
        upgrade_json.get("version").and_then(Value::as_str),
        Some("0.2.0")
    );
    let upgraded = store
        .get_extension(extension_id)
        .await?
        .expect("upgraded extension");
    assert_eq!(upgraded.version, "0.2.0");
    assert_eq!(
        upgraded.package_hash.as_deref(),
        Some(package_v2.hash.as_str())
    );
    assert_eq!(
        upgraded
            .manifest_json
            .pointer("/runtime/image")
            .and_then(Value::as_str),
        Some("elixir/torrentio-candidate-provider:0.2.0")
    );
    let upgraded_instances = store.list_instances(Some(extension_id)).await?;
    assert_eq!(upgraded_instances.len(), 1);
    assert_eq!(upgraded_instances[0].instance_id, instance_id);
    assert_eq!(
        upgraded_instances[0].config_json.as_ref(),
        Some(&instance_config)
    );
    let unpacked_v2 = PathBuf::from(&storage_root)
        .join("unpacked")
        .join(extension_id)
        .join("0.2.0");
    assert!(
        tokio::fs::metadata(unpacked_v2.join("manifest.yaml"))
            .await
            .is_ok(),
        "expected v2 package to be unpacked"
    );

    store
        .update_instance_runtime_version(instance_id, "0.2.0", Some("0.1.0"))
        .await?;
    let rollback_resp = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/extensions/instances/{instance_id}/rollback"
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(rollback_resp.status(), StatusCode::OK);
    let rollback_body = body::to_bytes(rollback_resp.into_body(), 1_048_576).await?;
    let rollback_json: Value = serde_json::from_slice(&rollback_body)?;
    assert!(
        rollback_json
            .get("conflicts")
            .and_then(Value::as_array)
            .map(Vec::is_empty)
            .unwrap_or(false),
        "rollback should be available: {rollback_json}"
    );
    let rollback_actions = rollback_json
        .get("actions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        rollback_actions
            .iter()
            .any(|action| action.get("type") == Some(&json!("rollback_runtime"))),
        "expected rollback runtime action: {rollback_json}"
    );

    let subscription_id = Uuid::new_v4();
    let release_id = Uuid::new_v4();
    let release_job_id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_subscriptions (
            subscription_id,
            media_type,
            title,
            normalized_title,
            source_provider_id
         ) VALUES ($1, 'movie', 'Example Movie', 'example movie', $2)",
    )
    .bind(subscription_id.to_string())
    .bind(recovered_provider_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_releases (
            release_id,
            subscription_id,
            source_provider_id,
            source_extension_id,
            owner_id,
            media_type,
            title,
            release_title,
            source,
            source_kind,
            fingerprint,
            release_kind,
            resolver_kind,
            resolver_version,
            confidence,
            selected_provider_id
         ) VALUES ($1, $2, $3, $4, 'default', 'movie', 'Example Movie', 'Example.Movie.2024.1080p', $5, 'magnet', $6, 'single', 'tv', 'test', 'high', $7)",
    )
    .bind(release_id.to_string())
    .bind(subscription_id.to_string())
    .bind(recovered_provider_id.to_string())
    .bind(extension_id)
    .bind("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
    .bind("fixture-fingerprint")
    .bind(recovered_provider_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_release_jobs (
            release_job_id,
            release_id,
            route_logical_id,
            provider_id,
            state
         ) VALUES ($1, $2, 'acquisition.debrid.default', $3, 'queued')",
    )
    .bind(release_job_id.to_string())
    .bind(release_id.to_string())
    .bind(recovered_provider_id.to_string())
    .execute(&db_pool)
    .await?;

    let uninstall_resp = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/extensions/{extension_id}/uninstall"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(uninstall_resp.status(), StatusCode::OK);
    assert!(store.get_extension(extension_id).await?.is_none());
    assert!(store.list_instances(Some(extension_id)).await?.is_empty());
    assert!(store.list_providers(Some(instance_id)).await?.is_empty());
    assert!(
        tokio::fs::metadata(
            PathBuf::from(&storage_root)
                .join("unpacked")
                .join(extension_id)
        )
        .await
        .is_err(),
        "uninstall should remove unpacked runtime package state"
    );

    let remaining_package_count = {
        let mut count = 0i64;
        let mut entries =
            tokio::fs::read_dir(PathBuf::from(&storage_root).join("packages")).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry
                .path()
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| value.eq_ignore_ascii_case("elx"))
                .unwrap_or(false)
            {
                count += 1;
            }
        }
        count
    };
    assert_eq!(
        remaining_package_count, 0,
        "uninstall should remove downloaded packages for every installed version"
    );

    let subscription_source_cleared = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT CASE WHEN source_provider_id IS NULL THEN 1 ELSE 0 END
         FROM acquisition_subscriptions
         WHERE subscription_id = $1",
    )
    .bind(subscription_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(subscription_source_cleared, 1);
    let release_provider_refs_cleared = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT CASE WHEN source_provider_id IS NULL AND selected_provider_id IS NULL THEN 1 ELSE 0 END
         FROM acquisition_releases
         WHERE release_id = $1",
    )
    .bind(release_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(release_provider_refs_cleared, 1);
    let release_job_provider_cleared = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT CASE WHEN provider_id IS NULL THEN 1 ELSE 0 END
         FROM acquisition_release_jobs
         WHERE release_job_id = $1",
    )
    .bind(release_job_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(release_job_provider_cleared, 1);

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
async fn extension_control_surface_auto_provisions_zero_config_module_instance() -> Result<()> {
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
            extension_id: "elixir.modules.byparr".to_string(),
            name: "Byparr".to_string(),
            version: "1.0.1".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({
                "id": "elixir.modules.byparr",
                "version": "1.0.1",
                "kind": "module",
                "name": "Byparr",
                "provides": [
                    {
                        "capability": "indexer.proxy",
                        "slot": "byparr",
                        "implementation": "byparr"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "example/byparr:1"
                },
                "networking": {
                    "service_port": {
                        "scheme": "http",
                        "container_port": 8191
                    }
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;

    let surface_resp = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.byparr/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(surface_resp.status(), StatusCode::OK);
    let body = body::to_bytes(surface_resp.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let actions = payload
        .get("actions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(!actions.iter().any(|action| {
        action.get("id").and_then(Value::as_str) == Some("create_default_instance")
    }));

    let instances = store.list_instances(Some("elixir.modules.byparr")).await?;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_name, "default");
    assert!(instances[0].enabled);

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
        payload.pointer("/actions/0/id").and_then(Value::as_str),
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
            .any(
                |metric| metric.get("id").and_then(Value::as_str) == Some("seriesCount")
                    && metric.get("value").and_then(Value::as_str) == Some("2")
            ),
        "expected series count metric in control surface: {}",
        payload
    );
    let preference_fields = control_surface_section(&payload, "downloadClientPreference")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        preference_fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("downloadClientPreference")
                && field.get("value").and_then(Value::as_str) == Some("usenet")
        }),
        "expected sonarr download client preference field: {}",
        payload
    );

    let open_ui_actions = control_surface_section(&payload, "manualSetup")
        .get("actions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        open_ui_actions
            .iter()
            .find(|action| action.get("id").and_then(Value::as_str) == Some("open_service_ui"))
            .and_then(|action| action.get("label"))
            .and_then(Value::as_str),
        Some("Open Sonarr UI")
    );
    assert!(
        open_ui_actions
            .iter()
            .find(|action| action.get("id").and_then(Value::as_str) == Some("open_service_ui"))
            .and_then(|action| action.get("openUrl"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .ends_with("/ui/start"),
        "expected Sonarr control surface to expose proxied UI start path: {}",
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
    assert_eq!(
        control_surface_section(&payload, "defaults")
            .get("policy")
            .and_then(|policy| policy.get("mode"))
            .and_then(Value::as_str),
        Some("seeded")
    );
    assert!(
        defaults_fields
            .iter()
            .any(
                |field| field.get("id").and_then(Value::as_str) == Some("monitorOnAdd")
                    && field.get("value").and_then(Value::as_bool) == Some(true)
            ),
        "expected monitorOnAdd field in defaults section: {}",
        payload
    );
    assert!(
        defaults_fields
            .iter()
            .any(
                |field| field.get("id").and_then(Value::as_str) == Some("searchOnAdd")
                    && field.get("value").and_then(Value::as_bool) == Some(true)
            ),
        "expected searchOnAdd field in defaults section: {}",
        payload
    );

    let preference_fields = control_surface_section(&payload, "downloadClientPreference")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        control_surface_section(&payload, "downloadClientPreference")
            .get("policy")
            .and_then(|policy| policy.get("mode"))
            .and_then(Value::as_str),
        Some("seeded")
    );
    assert!(
        preference_fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("downloadClientPreference")
                && field.get("value").and_then(Value::as_str) == Some("usenet")
        }),
        "expected sonarr download client preference field: {}",
        payload
    );
    let preference_entities = control_surface_section(&payload, "downloadClientPreference")
        .get("entities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        preference_entities.iter().any(|entity| {
            entity.get("title").and_then(Value::as_str) == Some("NZBGet")
                && entity
                    .get("details")
                    .and_then(Value::as_array)
                    .map(|details| {
                        details
                            .iter()
                            .any(|detail| detail.as_str() == Some("Client priority 10"))
                    })
                    .unwrap_or(false)
        }),
        "expected NZBGet client details in control surface: {}",
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
async fn extension_control_surface_reports_sonarr_managed_downloader_drift() -> Result<()> {
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

    let (sonarr_host, sonarr_addr, server_state, sonarr_shutdown_tx) =
        start_mock_sonarr_control_server().await?;
    let (nzbget_host, nzbget_addr, _nzbget_mock_state, nzbget_shutdown_tx) =
        start_mock_nzbget_control_server(Vec::new(), json!("")).await?;
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
    let _nzbget_instance =
        seed_nzbget_control_extension(&store, &secrets, nzbget_host.clone(), nzbget_addr.port())
            .await?;

    {
        let mut clients = server_state.download_clients.lock().unwrap();
        clients[0] = json!({
            "id": 11,
            "name": "NZBGet",
            "enable": true,
            "protocol": "usenet",
            "priority": 10,
            "implementation": "Nzbget",
            "fields": [
                { "name": "host", "value": "wrong-nzbget" },
                { "name": "port", "value": 6789 },
                { "name": "username", "value": "elixir" },
                { "name": "password", "value": "********" },
                { "name": "tvCategory", "value": "tv" }
            ]
        });
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.sonarr/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = sonarr_shutdown_tx.send(());
    let _ = nzbget_shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    assert_eq!(
        payload.pointer("/status/summary").and_then(Value::as_str),
        Some("Managed drift detected")
    );
    let section = control_surface_section(&payload, "managedInvariants");
    assert_eq!(
        section
            .get("policy")
            .and_then(|policy| policy.get("mode"))
            .and_then(Value::as_str),
        Some("managed")
    );
    let notices = section
        .get("notices")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        notices.iter().any(|notice| {
            notice.get("code").and_then(Value::as_str) == Some("managed_downloader_endpoint_drift")
                && notice.get("title").and_then(Value::as_str) == Some("NZBGet endpoint drifted")
        }),
        "expected managed downloader drift notice in control surface: {}",
        payload
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_accepts_managed_downloader_alias_hosts() -> Result<()> {
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

    let (sonarr_host, sonarr_addr, _server_state, sonarr_shutdown_tx) =
        start_mock_sonarr_control_server().await?;
    let store = ExtensionStore::new(&db_pool);
    let sonarr_instance_id = Uuid::new_v4();
    let sonarr_provider_id = Uuid::new_v4();

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
            instance_id: sonarr_instance_id,
            extension_id: "elixir.modules.sonarr".to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({ "api_key": "test-sonarr-key" })),
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: sonarr_provider_id,
            instance_id: sonarr_instance_id,
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

    let nzbget_instance_id = Uuid::new_v4();
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.nzbget".to_string(),
            name: "NZBGet".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.nzbget",
                "version": "1.0.0",
                "kind": "module",
                "name": "NZBGet",
                "provides": [{
                    "capability": "downloader.nzb",
                    "slot": "default",
                    "implementation": "nzbget"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/nzbget:latest",
                    "service_name": "elx-nzbget"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .create_instance(&NewExtensionInstance {
            instance_id: nzbget_instance_id,
            extension_id: "elixir.modules.nzbget".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: nzbget_instance_id,
            capability: "downloader.nzb".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("nzbget".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "svc-elixir-modules-nzbget-default",
                "port": 6789,
                "network_name": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let qbittorrent_instance_id = Uuid::new_v4();
    store
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.qbittorrent".to_string(),
            name: "qBittorrent".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": "elixir.modules.qbittorrent",
                "version": "1.0.0",
                "kind": "module",
                "name": "qBittorrent",
                "provides": [{
                    "capability": "downloader.torrent",
                    "slot": "default",
                    "implementation": "qbittorrent"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/qbittorrent:latest",
                    "service_name": "elx-qbittorrent"
                }
            }),
            package_hash: None,
            enabled: true,
        })
        .await?;
    store
        .create_instance(&NewExtensionInstance {
            instance_id: qbittorrent_instance_id,
            extension_id: "elixir.modules.qbittorrent".to_string(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store
        .upsert_provider(&NewProvider {
            provider_id: Uuid::new_v4(),
            instance_id: qbittorrent_instance_id,
            capability: "downloader.torrent".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some("qbittorrent".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "svc-elixir-modules-qbittorrent-default",
                "port": 8080,
                "network_name": "elixir_net"
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
    let _ = sonarr_shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    assert_ne!(
        payload.pointer("/status/summary").and_then(Value::as_str),
        Some("Managed drift detected"),
        "expected alias-compatible downloader hosts to avoid managed drift: {}",
        payload
    );
    assert!(
        payload
            .get("sections")
            .and_then(Value::as_array)
            .is_some_and(|sections| sections.iter().all(|section| {
                section.get("id").and_then(Value::as_str) != Some("managedInvariants")
            })),
        "expected no managed invariant drift section when legacy aliases still map to the managed providers: {}",
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
async fn library_delete_item_can_stop_tracking_and_create_tombstone() -> Result<()> {
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
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_id = Uuid::new_v4();
    let media_file_id = Uuid::new_v4();
    let temp_dir = tempdir()?;
    let media_path = temp_dir.path().join("Noble.House.S01E01.mkv");
    let subtitle_path = temp_dir.path().join("Noble.House.S01E01.srt");
    std::fs::write(&media_path, b"video-bytes")?;
    std::fs::write(&subtitle_path, b"subtitle-bytes")?;

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
            normalized_title: "noblehouse".to_string(),
            year: Some(1988),
            external_ids: Some(ExternalIds {
                imdb: Some("tt0094518".to_string()),
                ..Default::default()
            }),
            manager_provider_id: provider_id,
            manager_item_id: Some("42".to_string()),
            manager_label: Some("default (sonarr)".to_string()),
            source: "find_media".to_string(),
        })
        .await?;

    sqlx::query(
        "INSERT INTO series (
            id, title, year, library_type, external_imdb, metadata_json
         ) VALUES ($1, $2, $3, $4, $5, NULL)",
    )
    .bind(series_id.to_string())
    .bind("Noble House")
    .bind(1988)
    .bind("series")
    .bind("tt0094518")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title, metadata_json)
         VALUES ($1, $2, 1, $3, NULL)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .bind("Season 1")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes (
            id, series_id, season_id, season_number, episode_number, title, has_file, metadata_json
         ) VALUES ($1, $2, $3, 1, 1, $4, 1, NULL)",
    )
    .bind(episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind("Episode 1")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(series_id.to_string())
    .bind("series")
    .bind(serde_json::to_string(&json!({ "imdb": "tt0094518" }))?)
    .bind("Noble House")
    .bind(1988)
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_files (
            id, media_item_id, path, scan_state
         ) VALUES ($1, $2, $3, 'ok')",
    )
    .bind(media_file_id.to_string())
    .bind(series_id.to_string())
    .bind(media_path.to_string_lossy().to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO episode_files (episode_id, media_file_id)
         VALUES ($1, $2)",
    )
    .bind(episode_id.to_string())
    .bind(media_file_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO external_subtitles (
            id, media_file_id, path, language
         ) VALUES ($1, $2, $3, $4)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(media_file_id.to_string())
    .bind(subtitle_path.to_string_lossy().to_string())
    .bind("en")
    .execute(&db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/library/items/{}", series_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "stopTracking": true
                }))?))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        payload
            .pointer("/ownerRelease/releasedCount")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        payload
            .pointer("/ownerRelease/owners/0/status")
            .and_then(Value::as_str),
        Some("succeeded")
    );

    assert!(!media_path.exists(), "expected media file to be removed");
    assert!(
        !subtitle_path.exists(),
        "expected subtitle file to be removed"
    );

    let (series_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM series WHERE id = $1")
        .bind(series_id.to_string())
        .fetch_one(&db_pool)
        .await?;
    assert_eq!(series_count, 0);

    let (media_item_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM media_items WHERE id = $1")
            .bind(series_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(media_item_count, 0);

    let active_intents = store.list_active_managed_ingest_intents().await?;
    assert!(
        active_intents
            .iter()
            .all(|intent| intent.intent_id != intent_id),
        "expected delete route to deactivate the matching intent"
    );

    let tombstones = store.list_active_managed_media_tombstones().await?;
    assert_eq!(tombstones.len(), 1);
    assert_eq!(tombstones[0].manager_provider_id, Some(provider_id));
    assert_eq!(tombstones[0].manager_item_id.as_deref(), Some("42"));
    assert_eq!(tombstones[0].action, "stop_tracking");

    assert_eq!(
        server_state.deletes.lock().unwrap().as_slice(),
        &["42".to_string()],
    );

    let row = sqlx::query(
        "SELECT owner_type, status, CAST(status_reason AS TEXT) AS status_reason
         FROM media_owner_release_events
         LIMIT 1",
    )
    .fetch_one(&db_pool)
    .await?;
    let owner_type: String = row.try_get("owner_type")?;
    let status: String = row.try_get("status")?;
    let status_reason: Option<String> = row.try_get("status_reason").ok();
    assert_eq!(owner_type, "extension");
    assert_eq!(status, "succeeded");
    assert!(
        status_reason
            .as_deref()
            .unwrap_or_default()
            .contains("Sonarr removed")
    );

    Ok(())
}

#[tokio::test]
async fn library_delete_acquisition_owned_item_stops_subscription() -> Result<()> {
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
    let movie_id = Uuid::new_v4();
    let subscription_id = Uuid::new_v4();
    let target_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, 'movie', $2, 'Fixture Movie', 2026)",
    )
    .bind(movie_id.to_string())
    .bind(serde_json::to_string(&json!({ "tmdb": "999001" }))?)
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO movies (id, title, year, external_tmdb, metadata_json)
         VALUES ($1, 'Fixture Movie', 2026, '999001', NULL)",
    )
    .bind(movie_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO acquisition_subscriptions (
            subscription_id, media_type, title, normalized_title, year,
            monitor_policy, route_policy, status, active
         ) VALUES ($1, 'movie', 'Fixture Movie', 'fixturemovie', 2026,
            'all_missing', 'debrid_first', 'active', 1)",
    )
    .bind(subscription_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO acquisition_targets (
            target_id, subscription_id, target_key, media_type, title, state
         ) VALUES ($1, $2, 'movie:fixturemovie:2026', 'movie', 'Fixture Movie', 'pending')",
    )
    .bind(target_id.to_string())
    .bind(subscription_id.to_string())
    .execute(&db_pool)
    .await?;
    store
        .upsert_acquisition_media_ownership(
            movie_id,
            subscription_id,
            None,
            Some("elixir.sources.torrentio_stremio"),
        )
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/library/items/{}", movie_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "ownerReleaseAction": "delete_and_release_owner"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        payload
            .pointer("/ownerReleaseAction")
            .and_then(Value::as_str),
        Some("delete_and_release_owner")
    );
    assert_eq!(
        payload
            .pointer("/ownerRelease/owners/0/ownerType")
            .and_then(Value::as_str),
        Some("acquisition")
    );
    assert_eq!(
        payload
            .pointer("/ownerRelease/owners/0/status")
            .and_then(Value::as_str),
        Some("succeeded")
    );

    let subscription_row = sqlx::query(
        "SELECT status, CAST(active AS INTEGER) AS active
         FROM acquisition_subscriptions
         WHERE subscription_id = $1",
    )
    .bind(subscription_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    let status: String = subscription_row.try_get("status")?;
    let active: i64 = subscription_row.try_get("active")?;
    assert_eq!(status, "paused");
    assert_eq!(active, 0);

    let target_state: String =
        sqlx::query_scalar("SELECT state FROM acquisition_targets WHERE target_id = $1")
            .bind(target_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(target_state, "excluded");

    let event_status: String =
        sqlx::query_scalar("SELECT status FROM media_owner_release_events LIMIT 1")
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(event_status, "succeeded");

    Ok(())
}

#[tokio::test]
async fn library_release_owner_only_keeps_local_library_rows() -> Result<()> {
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
    let movie_id = Uuid::new_v4();
    let media_file_id = Uuid::new_v4();
    let subscription_id = Uuid::new_v4();
    let target_id = Uuid::new_v4();
    let temp_dir = tempdir()?;
    let media_path = temp_dir.path().join("Fixture.Movie.2026.mkv");
    std::fs::write(&media_path, b"video-bytes")?;

    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, 'movie', $2, 'Fixture Movie', 2026)",
    )
    .bind(movie_id.to_string())
    .bind(serde_json::to_string(&json!({ "tmdb": "999002" }))?)
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO movies (id, title, year, external_tmdb, metadata_json)
         VALUES ($1, 'Fixture Movie', 2026, '999002', NULL)",
    )
    .bind(movie_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_files (
            id, media_item_id, path, scan_state
         ) VALUES ($1, $2, $3, 'ok')",
    )
    .bind(media_file_id.to_string())
    .bind(movie_id.to_string())
    .bind(media_path.to_string_lossy().to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO acquisition_subscriptions (
            subscription_id, media_type, title, normalized_title, year,
            monitor_policy, route_policy, status, active
         ) VALUES ($1, 'movie', 'Fixture Movie', 'fixturemovie', 2026,
            'all_missing', 'debrid_first', 'active', 1)",
    )
    .bind(subscription_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO acquisition_targets (
            target_id, subscription_id, target_key, media_type, title, state
         ) VALUES ($1, $2, 'movie:fixturemovie:2026', 'movie', 'Fixture Movie', 'pending')",
    )
    .bind(target_id.to_string())
    .bind(subscription_id.to_string())
    .execute(&db_pool)
    .await?;
    store
        .upsert_acquisition_media_ownership(
            movie_id,
            subscription_id,
            None,
            Some("elixir.sources.torrentio_stremio"),
        )
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/library/items/{}", movie_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "ownerReleaseAction": "release_owner_only"
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        payload
            .pointer("/ownerReleaseAction")
            .and_then(Value::as_str),
        Some("release_owner_only")
    );
    assert_eq!(
        payload
            .pointer("/localDeletePerformed")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        payload
            .pointer("/ownerRelease/owners/0/status")
            .and_then(Value::as_str),
        Some("succeeded")
    );

    assert!(
        media_path.exists(),
        "release_owner_only must not delete local files"
    );
    let media_item_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM media_items WHERE id = $1")
            .bind(movie_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(media_item_count, 1);
    let movie_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM movies WHERE id = $1")
        .bind(movie_id.to_string())
        .fetch_one(&db_pool)
        .await?;
    assert_eq!(movie_count, 1);

    let subscription_active: i64 = sqlx::query_scalar(
        "SELECT CAST(active AS INTEGER) FROM acquisition_subscriptions WHERE subscription_id = $1",
    )
    .bind(subscription_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(subscription_active, 0);
    let target_state: String =
        sqlx::query_scalar("SELECT state FROM acquisition_targets WHERE target_id = $1")
            .bind(target_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(target_state, "excluded");

    let response = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/api/v1/library/items/{}/owner-release/events?limit=10",
                movie_id
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let audit_payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        audit_payload.pointer("/total").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        audit_payload
            .pointer("/events/0/requestedAction")
            .and_then(Value::as_str),
        Some("release_owner_only")
    );
    assert_eq!(
        audit_payload
            .pointer("/events/0/status")
            .and_then(Value::as_str),
        Some("succeeded")
    );

    Ok(())
}

#[tokio::test]
async fn library_owner_release_reconcile_repairs_missing_and_stale_owners() -> Result<()> {
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
    let orphan_movie_id = Uuid::new_v4();
    let stale_movie_id = Uuid::new_v4();
    let extension_id = "elixir.modules.sonarr";
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();
    let stale_ownership_id = Uuid::new_v4();

    for (movie_id, tmdb_id) in [(orphan_movie_id, "999003"), (stale_movie_id, "999004")] {
        sqlx::query(
            "INSERT INTO media_items (id, type, external_ids, title, year)
             VALUES ($1, 'movie', $2, 'Fixture Movie', 2026)",
        )
        .bind(movie_id.to_string())
        .bind(serde_json::to_string(&json!({ "tmdb": tmdb_id }))?)
        .execute(&db_pool)
        .await?;
        sqlx::query(
            "INSERT INTO movies (id, title, year, external_tmdb, metadata_json)
             VALUES ($1, 'Fixture Movie', 2026, $2, NULL)",
        )
        .bind(movie_id.to_string())
        .bind(tmdb_id)
        .execute(&db_pool)
        .await?;
    }

    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
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
            extension_id: extension_id.to_string(),
            instance_name: "default".to_string(),
            config_json: Some(json!({})),
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
            endpoint_json: None,
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_media_ownership(&NewMediaOwnership {
            ownership_id: stale_ownership_id,
            media_item_id: stale_movie_id,
            owner_type: "extension".to_string(),
            owner_role: "primary".to_string(),
            owner_label: Some("default (sonarr)".to_string()),
            owner_implementation: Some("sonarr".to_string()),
            owner_provider_id: Some(provider_id),
            owner_instance_id: Some(instance_id),
            owner_extension_id: Some(extension_id.to_string()),
            owner_external_id: Some("42".to_string()),
            acquisition_subscription_id: None,
            acquisition_target_scope: None,
            release_capability: "manager.remove_item".to_string(),
            release_policy: "supported".to_string(),
            metadata: None,
            active: true,
        })
        .await?;

    sqlx::query("DELETE FROM providers WHERE provider_id = $1")
        .bind(provider_id.to_string())
        .execute(&db_pool)
        .await?;

    let response = app
        .clone()
        .oneshot(Request::post("/api/v1/library/owner-release/reconcile").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        payload
            .pointer("/report/externalOwnersCreated")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        payload
            .pointer("/report/staleOwnersMarkedUnsupported")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        payload
            .pointer("/report/unsupportedEventsCreated")
            .and_then(Value::as_u64),
        Some(1)
    );

    let orphan_owner_type: String = sqlx::query_scalar(
        "SELECT owner_type FROM media_ownerships WHERE media_item_id = $1 AND active = 1",
    )
    .bind(orphan_movie_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(orphan_owner_type, "external");

    let stale_release_capability: String = sqlx::query_scalar(
        "SELECT release_capability FROM media_ownerships WHERE ownership_id = $1",
    )
    .bind(stale_ownership_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(stale_release_capability, "none");

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/library/owner-release/events?limit=5").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let audit_payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        audit_payload
            .pointer("/events/0/requestedAction")
            .and_then(Value::as_str),
        Some("reconcile_owner")
    );
    assert_eq!(
        audit_payload
            .pointer("/events/0/status")
            .and_then(Value::as_str),
        Some("unsupported")
    );

    Ok(())
}

#[tokio::test]
async fn library_delete_episode_can_block_locally_and_keep_series_scaffold() -> Result<()> {
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
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_id = Uuid::new_v4();
    let media_file_id = Uuid::new_v4();
    let subscription_id = Uuid::new_v4();
    let target_id = Uuid::new_v4();
    let temp_dir = tempdir()?;
    let media_path = temp_dir.path().join("Show.S01E02.mkv");
    let subtitle_path = temp_dir.path().join("Show.S01E02.srt");
    std::fs::write(&media_path, b"video-bytes")?;
    std::fs::write(&subtitle_path, b"subtitle-bytes")?;

    sqlx::query(
        "INSERT INTO series (
            id, title, year, library_type, external_tvdb_series, metadata_json
         ) VALUES ($1, $2, $3, $4, $5, NULL)",
    )
    .bind(series_id.to_string())
    .bind("Blocked Show")
    .bind(2024)
    .bind("series")
    .bind("321")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title, metadata_json)
         VALUES ($1, $2, 1, $3, NULL)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .bind("Season 1")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes (
            id, series_id, season_id, season_number, episode_number, title, has_file, metadata_json
         ) VALUES ($1, $2, $3, 1, 2, $4, 1, NULL)",
    )
    .bind(episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind("Episode 2")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(series_id.to_string())
    .bind("series")
    .bind(serde_json::to_string(
        &json!({ "tvdb": "321", "tvdb_series": "321" }),
    )?)
    .bind("Blocked Show")
    .bind(2024)
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO acquisition_subscriptions (
            subscription_id, media_type, title, normalized_title, year,
            monitor_policy, route_policy, status, active
         ) VALUES ($1, 'series', 'Blocked Show', 'blockedshow', 2024,
            'all_missing', 'debrid_first', 'active', 1)",
    )
    .bind(subscription_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO acquisition_targets (
            target_id, subscription_id, target_key, media_type, title,
            season_number, episode_number, state
         ) VALUES ($1, $2, 'S01E02', 'series', 'Blocked Show', 1, 2, 'pending')",
    )
    .bind(target_id.to_string())
    .bind(subscription_id.to_string())
    .execute(&db_pool)
    .await?;
    store
        .upsert_acquisition_media_ownership(
            series_id,
            subscription_id,
            None,
            Some("elixir.sources.torrentio_stremio"),
        )
        .await?;
    sqlx::query(
        "INSERT INTO media_files (
            id, media_item_id, path, scan_state
         ) VALUES ($1, $2, $3, 'ok')",
    )
    .bind(media_file_id.to_string())
    .bind(series_id.to_string())
    .bind(media_path.to_string_lossy().to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO episode_files (episode_id, media_file_id)
         VALUES ($1, $2)",
    )
    .bind(episode_id.to_string())
    .bind(media_file_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO external_subtitles (
            id, media_file_id, path, language
         ) VALUES ($1, $2, $3, $4)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(media_file_id.to_string())
    .bind(subtitle_path.to_string_lossy().to_string())
    .bind("en")
    .execute(&db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/library/episodes/{}", episode_id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "blockInElixir": true
                }))?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let delete_payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        delete_payload
            .pointer("/ownerReleaseAction")
            .and_then(Value::as_str),
        Some("block_episode")
    );
    assert_eq!(
        delete_payload
            .pointer("/ownerRelease/owners/0/ownerType")
            .and_then(Value::as_str),
        Some("acquisition")
    );

    assert!(!media_path.exists(), "expected media file to be removed");
    assert!(
        !subtitle_path.exists(),
        "expected subtitle file to be removed"
    );

    let (episode_has_file,): (i64,) =
        sqlx::query_as("SELECT CAST(has_file AS INTEGER) FROM episodes WHERE id = $1")
            .bind(episode_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(episode_has_file, 0);

    let (media_file_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM media_files WHERE id = $1")
            .bind(media_file_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(media_file_count, 0);

    let tombstones = store.list_active_managed_episode_tombstones().await?;
    assert_eq!(tombstones.len(), 1);
    assert_eq!(tombstones[0].season_number, 1);
    assert_eq!(tombstones[0].episode_number, 2);
    assert_eq!(tombstones[0].action, "block_episode");

    let target_state: String =
        sqlx::query_scalar("SELECT state FROM acquisition_targets WHERE target_id = $1")
            .bind(target_id.to_string())
            .fetch_one(&db_pool)
            .await?;
    assert_eq!(target_state, "excluded");

    let subscription_active: i64 = sqlx::query_scalar(
        "SELECT CAST(active AS INTEGER) FROM acquisition_subscriptions WHERE subscription_id = $1",
    )
    .bind(subscription_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(
        subscription_active, 1,
        "episode blocking must preserve the series subscription"
    );

    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/library/seasons/{}/episodes", season_id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    let blocked_episode = payload
        .as_array()
        .and_then(|items| items.first())
        .expect("blocked episode payload");
    let blocked = blocked_episode
        .get("lifecycle")
        .and_then(|value| value.get("blockedInElixir"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    assert!(
        blocked,
        "expected episode lifecycle to reflect blocked state"
    );
    assert_eq!(
        blocked_episode
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("blocked")
    );
    assert_eq!(
        blocked_episode
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("allow_again")
    );

    Ok(())
}

#[tokio::test]
async fn mmr2_library_episode_api_exposes_compact_acquisition_recovery_states() -> Result<()> {
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

    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO series (
            id, title, year, library_type, external_tvdb_series, metadata_json
         ) VALUES ($1, $2, $3, $4, $5, NULL)",
    )
    .bind(series_id.to_string())
    .bind("MMR Show")
    .bind(2026)
    .bind("series")
    .bind("998877")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title, metadata_json)
         VALUES ($1, $2, 1, $3, NULL)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .bind("Season 1")
    .execute(&db_pool)
    .await?;

    let episode_ids = (1..=5).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
    for (index, episode_id) in episode_ids.iter().enumerate() {
        let episode_number = (index + 1) as i32;
        sqlx::query(
            "INSERT INTO episodes (
                id, series_id, season_id, season_number, episode_number, title, has_file, metadata_json
             ) VALUES ($1, $2, $3, 1, $4, $5, $6, NULL)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(episode_number)
        .bind(format!("Episode {episode_number}"))
        .bind(if episode_number == 5 { 1 } else { 0 })
        .execute(&db_pool)
        .await?;
    }

    let projection_rows = [
        (
            episode_ids[1],
            "S01E02",
            "no_results",
            Some("no_safe_candidates"),
            Some("Torrentio"),
            None,
            Some(0_i64),
        ),
        (
            episode_ids[2],
            "S01E03",
            "review_needed",
            Some("review_required"),
            Some("Torrentio"),
            None,
            Some(3_i64),
        ),
        (
            episode_ids[3],
            "S01E04",
            "downloading",
            Some("submitted"),
            Some("Torrentio"),
            Some("TorBox"),
            Some(1_i64),
        ),
        (
            episode_ids[4],
            "S01E05",
            "imported",
            Some("imported"),
            Some("Torrentio"),
            Some("TorBox"),
            Some(1_i64),
        ),
    ];
    for (
        episode_id,
        target_key,
        state,
        reason_code,
        source_provider_label,
        route_provider_label,
        candidate_count,
    ) in projection_rows
    {
        sqlx::query(
            "INSERT INTO library_episode_acquisition_state (
                episode_id,
                media_item_id,
                season_id,
                target_key,
                state,
                reason_code,
                source_provider_label,
                route_provider_label,
                candidate_count,
                selected_release_title,
                last_attempt_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(target_key)
        .bind(state)
        .bind(reason_code)
        .bind(source_provider_label)
        .bind(route_provider_label)
        .bind(candidate_count)
        .bind(format!("Release {target_key}"))
        .execute(&db_pool)
        .await?;
    }

    let response = app
        .oneshot(
            Request::get(format!("/api/v1/library/seasons/{}/episodes", season_id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    let episodes = payload.as_array().expect("episode list");
    assert_eq!(episodes.len(), 5);

    assert_eq!(
        episodes[0]
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("missing")
    );
    assert_eq!(
        episodes[0]
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("get_episode")
    );
    assert_eq!(
        episodes[1]
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("no_results")
    );
    assert_eq!(
        episodes[1]
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("search_again")
    );
    assert_eq!(
        episodes[1]
            .pointer("/acquisition/sourceProviderLabel")
            .and_then(Value::as_str),
        Some("Torrentio")
    );
    assert_eq!(
        episodes[1]
            .pointer("/acquisition/candidateCount")
            .and_then(Value::as_i64),
        Some(0)
    );
    assert_eq!(
        episodes[2]
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("review_needed")
    );
    assert_eq!(
        episodes[2]
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("review")
    );
    assert_eq!(
        episodes[3]
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("downloading")
    );
    assert_eq!(
        episodes[3]
            .pointer("/acquisition/routeProviderLabel")
            .and_then(Value::as_str),
        Some("TorBox")
    );
    assert_eq!(
        episodes[3]
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("view_progress")
    );
    assert_eq!(
        episodes[4]
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("available")
    );
    assert_eq!(
        episodes[4].pointer("/has_file").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        episodes[4]
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("play")
    );
    assert_eq!(
        episodes[4]
            .pointer("/lifecycle/canDeleteLocally")
            .and_then(Value::as_bool),
        Some(true),
        "existing episode lifecycle fields must remain available with acquisition state present"
    );

    Ok(())
}

#[tokio::test]
async fn library_episode_api_projects_authenticated_playback_state_per_user() -> Result<()> {
    let mut settings = test_settings_with_db();
    settings.auth.access_token_secret = "episode-playback-state-secret-key".to_string();
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

    let user_one_id = Uuid::new_v4();
    let user_two_id = Uuid::new_v4();
    for (user_id, email) in [
        (user_one_id, "episode-progress-one@example.com"),
        (user_two_id, "episode-progress-two@example.com"),
    ] {
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(user_id.to_string())
            .bind(email)
            .bind("hashed")
            .execute(&db_pool)
            .await?;
    }
    let user_one_token = state.auth_service.issue_access_token(user_one_id)?.token;
    let user_two_token = state.auth_service.issue_access_token(user_two_id)?.token;

    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_one_id = Uuid::new_v4();
    let episode_two_id = Uuid::new_v4();
    let episode_one_id_text = episode_one_id.to_string();
    let episode_two_id_text = episode_two_id.to_string();
    sqlx::query("INSERT INTO series (id, title, year, library_type) VALUES ($1, $2, $3, $4)")
        .bind(series_id.to_string())
        .bind("Playback State Show")
        .bind(2026)
        .bind("series")
        .execute(&db_pool)
        .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 1, $3)")
        .bind(season_id.to_string())
        .bind(series_id.to_string())
        .bind("Season 1")
        .execute(&db_pool)
        .await?;
    for (episode_id, episode_number, runtime_seconds) in [
        (episode_one_id, 1_i32, 1800_i32),
        (episode_two_id, 2_i32, 1600_i32),
    ] {
        sqlx::query(
            "INSERT INTO episodes (
                id, series_id, season_id, season_number, episode_number, title,
                runtime_seconds, has_file, metadata_json
             ) VALUES ($1, $2, $3, 1, $4, $5, $6, 1, NULL)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(episode_number)
        .bind(format!("Episode {episode_number}"))
        .bind(runtime_seconds)
        .execute(&db_pool)
        .await?;
    }

    sqlx::query(
        "INSERT INTO user_media_state (
            user_id, item_type, item_id, series_id, season_id, resume_seconds,
            duration_seconds, watched, watched_at, play_count, last_played_at, state_source
         ) VALUES ($1, 'episode', $2, $3, $4, 321.4, 1800, 0, NULL, 0, CURRENT_TIMESTAMP, 'playback')",
    )
    .bind(user_one_id.to_string())
    .bind(episode_one_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO user_media_state (
            user_id, item_type, item_id, series_id, season_id, resume_seconds,
            duration_seconds, watched, watched_at, play_count, last_played_at, state_source
         ) VALUES ($1, 'episode', $2, $3, $4, 0, 1800, 1, CURRENT_TIMESTAMP, 1, CURRENT_TIMESTAMP, 'manual')",
    )
    .bind(user_two_id.to_string())
    .bind(episode_one_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .execute(&db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/library/seasons/{}/episodes", season_id))
                .header("authorization", format!("Bearer {user_one_token}"))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(status, StatusCode::OK, "body: {}", payload);
    let episodes = payload.as_array().expect("episode list");
    let episode_one = episodes
        .iter()
        .find(|episode| {
            episode.get("id").and_then(Value::as_str) == Some(episode_one_id_text.as_str())
        })
        .expect("episode one");
    let episode_two = episodes
        .iter()
        .find(|episode| {
            episode.get("id").and_then(Value::as_str) == Some(episode_two_id_text.as_str())
        })
        .expect("episode two");
    assert_eq!(
        episode_one
            .pointer("/playback_state/watched")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert!(
        (episode_one
            .pointer("/playback_state/resume_seconds")
            .and_then(Value::as_f64)
            .unwrap_or_default()
            - 321.4)
            .abs()
            < 0.001,
        "expected user one's resume state: {episode_one}"
    );
    assert_eq!(
        episode_one
            .pointer("/playback_state/duration_seconds")
            .and_then(Value::as_i64),
        Some(1800)
    );
    assert_eq!(
        episode_one
            .pointer("/playback_state/state_source")
            .and_then(Value::as_str),
        Some("playback")
    );
    assert!(
        episode_two.get("playback_state").is_none(),
        "episodes without state for the authenticated user must omit playback_state: {episode_two}"
    );

    let other_response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/library/seasons/{}/episodes", season_id))
                .header("authorization", format!("Bearer {user_two_token}"))
                .body(Body::empty())?,
        )
        .await?;
    let other_status = other_response.status();
    let other_payload: Value =
        serde_json::from_slice(&body::to_bytes(other_response.into_body(), usize::MAX).await?)?;
    assert_eq!(other_status, StatusCode::OK, "body: {}", other_payload);
    let other_episode_one = other_payload
        .as_array()
        .and_then(|episodes| {
            episodes.iter().find(|episode| {
                episode.get("id").and_then(Value::as_str) == Some(episode_one_id_text.as_str())
            })
        })
        .expect("other user's episode one");
    assert_eq!(
        other_episode_one
            .pointer("/playback_state/watched")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        other_episode_one
            .pointer("/playback_state/state_source")
            .and_then(Value::as_str),
        Some("manual")
    );
    assert_eq!(
        other_episode_one
            .pointer("/playback_state/resume_seconds")
            .and_then(Value::as_f64),
        Some(0.0)
    );

    let anonymous_response = app
        .oneshot(
            Request::get(format!("/api/v1/library/seasons/{}/episodes", season_id))
                .body(Body::empty())?,
        )
        .await?;
    let anonymous_status = anonymous_response.status();
    let anonymous_payload: Value =
        serde_json::from_slice(&body::to_bytes(anonymous_response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        anonymous_status,
        StatusCode::OK,
        "body: {}",
        anonymous_payload
    );
    assert!(
        anonymous_payload
            .as_array()
            .expect("anonymous episode list")
            .iter()
            .all(|episode| episode.get("playback_state").is_none()),
        "anonymous episode responses must not include per-user playback_state: {anonymous_payload}"
    );

    Ok(())
}

#[tokio::test]
async fn mmr2_library_episode_api_exposes_post_processing_and_failed_recovery_states() -> Result<()>
{
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

    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_ids = [Uuid::new_v4(), Uuid::new_v4()];
    sqlx::query("INSERT INTO series (id, title, year, library_type) VALUES ($1, $2, $3, $4)")
        .bind(series_id.to_string())
        .bind("MMR State Show")
        .bind(2026)
        .bind("series")
        .execute(&db_pool)
        .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 1, $3)")
        .bind(season_id.to_string())
        .bind(series_id.to_string())
        .bind("Season 1")
        .execute(&db_pool)
        .await?;
    for (index, episode_id) in episode_ids.iter().enumerate() {
        let episode_number = (index + 1) as i32;
        sqlx::query(
            "INSERT INTO episodes (
                id, series_id, season_id, season_number, episode_number, title, has_file
             ) VALUES ($1, $2, $3, 1, $4, $5, 0)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(episode_number)
        .bind(format!("Episode {episode_number}"))
        .execute(&db_pool)
        .await?;
    }
    for (episode_id, target_key, state, reason_code) in [
        (
            episode_ids[0],
            "S01E01",
            "post_processing",
            "post_processing",
        ),
        (episode_ids[1], "S01E02", "failed", "route_failed"),
    ] {
        sqlx::query(
            "INSERT INTO library_episode_acquisition_state (
                episode_id, media_item_id, season_id, target_key, state, reason_code, last_attempt_at
             ) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(target_key)
        .bind(state)
        .bind(reason_code)
        .execute(&db_pool)
        .await?;
    }

    let response = app
        .oneshot(
            Request::get(format!("/api/v1/library/seasons/{}/episodes", season_id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    let episodes = payload.as_array().expect("episode list");
    assert_eq!(
        episodes[0]
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("post_processing")
    );
    assert_eq!(
        episodes[0]
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("view_progress")
    );
    assert_eq!(
        episodes[1]
            .pointer("/acquisition/state")
            .and_then(Value::as_str),
        Some("failed")
    );
    assert_eq!(
        episodes[1]
            .pointer("/acquisition/action")
            .and_then(Value::as_str),
        Some("try_again")
    );
    assert_eq!(
        episodes[1]
            .pointer("/acquisition/reasonCode")
            .and_then(Value::as_str),
        Some("route_failed")
    );

    Ok(())
}

#[tokio::test]
async fn library_restore_blocked_episodes_clears_episode_tombstones() -> Result<()> {
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
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_one_id = Uuid::new_v4();
    let episode_two_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO series (
            id, title, year, library_type, external_tvdb_series, metadata_json
         ) VALUES ($1, $2, $3, $4, $5, NULL)",
    )
    .bind(series_id.to_string())
    .bind("Blocked Show")
    .bind(2024)
    .bind("series")
    .bind("321")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title, metadata_json)
         VALUES ($1, $2, 1, $3, NULL)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .bind("Season 1")
    .execute(&db_pool)
    .await?;
    for (episode_id, episode_number) in [(episode_one_id, 1), (episode_two_id, 2)] {
        sqlx::query(
            "INSERT INTO episodes (
                id, series_id, season_id, season_number, episode_number, title, has_file, metadata_json
             ) VALUES ($1, $2, $3, 1, $4, $5, 0, NULL)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(episode_number)
        .bind(format!("Episode {episode_number}"))
        .execute(&db_pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(series_id.to_string())
    .bind("series")
    .bind(serde_json::to_string(
        &json!({ "tvdb": "321", "tvdb_series": "321" }),
    )?)
    .bind("Blocked Show")
    .bind(2024)
    .execute(&db_pool)
    .await?;

    for episode_number in [1, 2] {
        store
            .upsert_managed_episode_tombstone(&NewManagedEpisodeTombstone {
                media_type: MediaType::Series,
                title: "Blocked Show".to_string(),
                normalized_title: "blockedshow".to_string(),
                year: Some(2024),
                external_ids: Some(ExternalIds {
                    tvdb: Some("321".to_string()),
                    tvdb_series: Some("321".to_string()),
                    ..Default::default()
                }),
                manager_provider_id: None,
                manager_item_id: None,
                manager_label: None,
                manager_implementation: None,
                season_number: 1,
                episode_number,
                absolute_episode_number: None,
                action: "block_episode".to_string(),
            })
            .await?;
    }

    let response = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/library/items/{}/restore-blocked-episodes",
                series_id
            ))
            .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(
        payload.get("restoredCount").and_then(Value::as_i64),
        Some(2)
    );
    assert!(
        store
            .list_active_managed_episode_tombstones()
            .await?
            .is_empty(),
        "expected all episode tombstones to be cleared"
    );

    Ok(())
}

#[tokio::test]
async fn library_restore_episode_clears_matching_tombstone() -> Result<()> {
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
    let series_id = Uuid::new_v4();
    let season_id = Uuid::new_v4();
    let episode_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO series (
            id, title, year, library_type, external_tvdb_series, metadata_json
         ) VALUES ($1, $2, $3, $4, $5, NULL)",
    )
    .bind(series_id.to_string())
    .bind("Blocked Show")
    .bind(2024)
    .bind("series")
    .bind("321")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title, metadata_json)
         VALUES ($1, $2, 1, $3, NULL)",
    )
    .bind(season_id.to_string())
    .bind(series_id.to_string())
    .bind("Season 1")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes (
            id, series_id, season_id, season_number, episode_number, title, has_file, metadata_json
         ) VALUES ($1, $2, $3, 1, 2, $4, 0, NULL)",
    )
    .bind(episode_id.to_string())
    .bind(series_id.to_string())
    .bind(season_id.to_string())
    .bind("Episode 2")
    .execute(&db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(series_id.to_string())
    .bind("series")
    .bind(serde_json::to_string(
        &json!({ "tvdb": "321", "tvdb_series": "321" }),
    )?)
    .bind("Blocked Show")
    .bind(2024)
    .execute(&db_pool)
    .await?;

    store
        .upsert_managed_episode_tombstone(&NewManagedEpisodeTombstone {
            media_type: MediaType::Series,
            title: "Blocked Show".to_string(),
            normalized_title: "blockedshow".to_string(),
            year: Some(2024),
            external_ids: Some(ExternalIds {
                tvdb: Some("321".to_string()),
                tvdb_series: Some("321".to_string()),
                ..Default::default()
            }),
            manager_provider_id: None,
            manager_item_id: None,
            manager_label: None,
            manager_implementation: None,
            season_number: 1,
            episode_number: 2,
            absolute_episode_number: None,
            action: "block_episode".to_string(),
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/library/episodes/{}/restore", episode_id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        store
            .list_active_managed_episode_tombstones()
            .await?
            .is_empty(),
        "expected matching episode tombstone to be cleared"
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
        .get_extension_setting(&format!(
            "extensions.control_defaults.instance.{instance_id}"
        ))
        .await?
        .unwrap_or(Value::Null);
    assert_eq!(
        stored.get("monitorOnAdd").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        stored.get("searchOnAdd").and_then(Value::as_bool),
        Some(false)
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_uses_cached_sonarr_download_client_inventory_when_live_load_fails()
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
                "host": "svc-sonarr-timeout",
                "port": 8989,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_extension_setting(
            &format!("extensions.control_download_clients.instance.{instance_id}"),
            &json!([
                {
                    "id": 1,
                    "name": "NZBGet",
                    "implementation": "Nzbget",
                    "protocol": "usenet",
                    "priority": 2,
                    "enabled": true
                },
                {
                    "id": 2,
                    "name": "qBittorrent",
                    "implementation": "QBittorrent",
                    "protocol": "torrent",
                    "priority": 1,
                    "enabled": true
                }
            ]),
        )
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.sonarr/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    let preference_fields = control_surface_section(&payload, "downloadClientPreference")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        preference_fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("downloadClientPreference")
                && field.get("value").and_then(Value::as_str) == Some("torrent")
                && field.get("readonly").and_then(Value::as_bool) == Some(true)
        }),
        "expected cached sonarr preference field in control surface: {}",
        payload
    );
    let preference_entities = control_surface_section(&payload, "downloadClientPreference")
        .get("entities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        preference_entities
            .iter()
            .any(|entity| entity.get("title").and_then(Value::as_str) == Some("qBittorrent")),
        "expected cached qBittorrent entity in control surface: {}",
        payload
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_settings_update_applies_sonarr_download_client_preference() -> Result<()>
{
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

    let response = app
        .clone()
        .oneshot(
            Request::put("/api/v1/extensions/elixir.modules.sonarr/control-surface")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "values": {
                        "downloadClientPreference": "torrent"
                    }
                }))?))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    let fields = control_surface_section(&payload, "downloadClientPreference")
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        fields.iter().any(|field| {
            field.get("id").and_then(Value::as_str) == Some("downloadClientPreference")
                && field.get("value").and_then(Value::as_str) == Some("torrent")
        }),
        "expected updated sonarr download client preference field: {}",
        payload
    );

    let updates = server_state.download_client_updates.lock().unwrap().clone();
    assert_eq!(
        updates.len(),
        2,
        "expected both download clients to be updated"
    );

    let clients = server_state.download_clients.lock().unwrap().clone();
    let nzbget_priority = clients
        .iter()
        .find(|client| client.get("name").and_then(Value::as_str) == Some("NZBGet"))
        .and_then(|client| {
            client
                .get("fields")
                .and_then(Value::as_array)
                .and_then(|fields| {
                    fields.iter().find_map(|field| {
                        if field.get("name").and_then(Value::as_str) == Some("priority") {
                            field.get("value").and_then(Value::as_i64)
                        } else {
                            None
                        }
                    })
                })
        });
    let qbittorrent_priority = clients
        .iter()
        .find(|client| client.get("name").and_then(Value::as_str) == Some("qBittorrent"))
        .and_then(|client| {
            client
                .get("fields")
                .and_then(Value::as_array)
                .and_then(|fields| {
                    fields.iter().find_map(|field| {
                        if field.get("name").and_then(Value::as_str) == Some("priority") {
                            field.get("value").and_then(Value::as_i64)
                        } else {
                            None
                        }
                    })
                })
        });
    assert_eq!(nzbget_priority, Some(11));
    assert_eq!(qbittorrent_priority, Some(10));

    Ok(())
}

#[tokio::test]
async fn extension_status_summary_marks_nzbget_provider_setup_required() -> Result<()> {
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

    let (host, addr, _mock_state, shutdown_tx) =
        start_mock_nzbget_control_server(Vec::new(), json!("")).await?;
    let store = ExtensionStore::new(&db_pool);
    seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .expect("summary items");
    let nzbget = items
        .iter()
        .find(|item| {
            item.get("extensionId").and_then(Value::as_str) == Some("elixir.modules.nzbget")
        })
        .expect("nzbget summary item");
    assert_eq!(
        nzbget.get("statusCode").and_then(Value::as_str),
        Some("provider_setup_required")
    );
    assert_eq!(
        nzbget.get("label").and_then(Value::as_str),
        Some("Add provider")
    );
    assert_eq!(
        nzbget.get("primaryActionLabel").and_then(Value::as_str),
        Some("Add provider")
    );

    Ok(())
}

#[tokio::test]
async fn extension_status_summary_restores_persisted_nzbget_provider_inventory() -> Result<()> {
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

    let (host, addr, mock_state, shutdown_tx) =
        start_mock_nzbget_control_server(Vec::new(), json!("")).await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget.server.1.username".to_string(),
            value_encrypted: secrets.encrypt("reader")?,
            rotatable: true,
        })
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget.server.1.password".to_string(),
            value_encrypted: secrets.encrypt("provider-secret")?,
            rotatable: true,
        })
        .await?;
    store
        .update_instance_config(
            instance_id,
            Some(&json!({
                "server_inventory": [{
                    "slot": 1,
                    "active": true,
                    "name": "Newshosting",
                    "level": 0,
                    "host": "news.newshosting.com",
                    "encryption": true,
                    "port": 563,
                    "connections": 30,
                    "cert_verification": "strict"
                }]
            })),
        )
        .await?;

    let response = app
        .clone()
        .oneshot(Request::get("/api/v1/extensions/status-summary").body(Body::empty())?)
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .expect("summary items");
    let nzbget = items
        .iter()
        .find(|item| {
            item.get("extensionId").and_then(Value::as_str) == Some("elixir.modules.nzbget")
        })
        .expect("nzbget summary item");
    assert_eq!(
        nzbget.get("statusCode").and_then(Value::as_str),
        Some("ready")
    );
    assert_eq!(nzbget.get("label").and_then(Value::as_str), Some("Ready"));

    let save_calls = mock_state.save_calls.lock().unwrap().clone();
    assert_eq!(save_calls.len(), 1, "expected one restore saveconfig call");
    let save_updates = save_calls[0]
        .get(0)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        save_updates.iter().any(|update| {
            update.get("Name").and_then(Value::as_str) == Some("Server1.Host")
                && update.get("Value").and_then(Value::as_str) == Some("news.newshosting.com")
        }),
        "expected persisted provider to be restored into NZBGet config: {:?}",
        save_updates
    );
    assert!(
        mock_state
            .config
            .lock()
            .unwrap()
            .get("Server1.Host")
            .map(|value| value == "news.newshosting.com")
            .unwrap_or(false),
        "expected restored provider host in live NZBGet config"
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_drops_stale_nzbget_provider_inventory_without_credentials()
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

    let (host, addr, mock_state, shutdown_tx) =
        start_mock_nzbget_control_server(Vec::new(), json!("")).await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;
    store
        .update_instance_config(
            instance_id,
            Some(&json!({
                "server_inventory": [{
                    "slot": 1,
                    "active": true,
                    "name": "XSNews",
                    "level": 0,
                    "host": "reader.xsnews.nl",
                    "encryption": true,
                    "port": 563,
                    "connections": 20,
                    "cert_verification": "strict"
                }]
            })),
        )
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.nzbget/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.pointer("/status/summary").and_then(Value::as_str),
        Some("Add provider")
    );

    let servers = control_surface_section(&payload, "servers");
    assert_eq!(
        servers
            .get("entities")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("nzbget instance");
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/server_inventory"))
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    let save_calls = mock_state.save_calls.lock().unwrap().clone();
    assert_eq!(
        save_calls.len(),
        0,
        "stale persisted inventory should not be restored without credentials"
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_ignores_stale_live_nzbget_inventory_absent_from_persisted_state()
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

    let (host, addr, mock_state, shutdown_tx) = start_mock_nzbget_control_server(
        vec![
            ("Server1.Active".to_string(), "yes".to_string()),
            ("Server1.Name".to_string(), "XSNews".to_string()),
            ("Server1.Level".to_string(), "0".to_string()),
            ("Server1.Host".to_string(), "news.xsnews.nl".to_string()),
            ("Server1.Encryption".to_string(), "yes".to_string()),
            ("Server1.Port".to_string(), "563".to_string()),
            ("Server1.Username".to_string(), "reader".to_string()),
            (
                "Server1.Password".to_string(),
                "provider-secret".to_string(),
            ),
            ("Server1.Connections".to_string(), "32".to_string()),
            ("Server1.CertVerification".to_string(), "strict".to_string()),
        ],
        json!(""),
    )
    .await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;
    store
        .update_instance_config(instance_id, Some(&json!({ "server_inventory": [] })))
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.nzbget/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.pointer("/status/summary").and_then(Value::as_str),
        Some("Add provider")
    );

    let servers = control_surface_section(&payload, "servers");
    assert_eq!(
        servers
            .get("entities")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("nzbget instance");
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/server_inventory"))
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    let save_calls = mock_state.save_calls.lock().unwrap().clone();
    assert_eq!(
        save_calls.len(),
        0,
        "stale live inventory should not be re-persisted when persisted state is empty"
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_ignores_sample_live_nzbget_inventory_without_persisted_state()
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

    let (host, addr, mock_state, shutdown_tx) = start_mock_nzbget_control_server(
        vec![
            ("Server1.Active".to_string(), "yes".to_string()),
            ("Server1.Name".to_string(), "my.newsserver.com".to_string()),
            ("Server1.Level".to_string(), "0".to_string()),
            ("Server1.Host".to_string(), "my.newsserver.com".to_string()),
            ("Server1.Encryption".to_string(), "yes".to_string()),
            ("Server1.Port".to_string(), "563".to_string()),
            ("Server1.Username".to_string(), "user".to_string()),
            ("Server1.Password".to_string(), "pass".to_string()),
            ("Server1.Connections".to_string(), "8".to_string()),
            ("Server1.CertVerification".to_string(), "strict".to_string()),
        ],
        json!(""),
    )
    .await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.nzbget/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        payload.pointer("/status/summary").and_then(Value::as_str),
        Some("Add provider")
    );

    let servers = control_surface_section(&payload, "servers");
    assert_eq!(
        servers
            .get("entities")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("nzbget instance");
    assert!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/server_inventory"))
            .is_none(),
        "sample live inventory should not be persisted when Elixir does not own it"
    );

    let save_calls = mock_state.save_calls.lock().unwrap().clone();
    assert_eq!(
        save_calls.len(),
        0,
        "sample live inventory should not trigger saveconfig writes"
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_includes_nzbget_servers_section() -> Result<()> {
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

    let (host, addr, _mock_state, shutdown_tx) = start_mock_nzbget_control_server(
        vec![
            ("Server1.Active".to_string(), "yes".to_string()),
            ("Server1.Name".to_string(), "Newshosting".to_string()),
            ("Server1.Level".to_string(), "0".to_string()),
            (
                "Server1.Host".to_string(),
                "news.newshosting.com".to_string(),
            ),
            ("Server1.Encryption".to_string(), "yes".to_string()),
            ("Server1.Port".to_string(), "563".to_string()),
            ("Server1.Username".to_string(), "reader".to_string()),
            ("Server1.Password".to_string(), "secret".to_string()),
            ("Server1.Connections".to_string(), "30".to_string()),
            ("Server1.CertVerification".to_string(), "strict".to_string()),
        ],
        json!(""),
    )
    .await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget.server.1.username".to_string(),
            value_encrypted: secrets.encrypt("reader")?,
            rotatable: true,
        })
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget.server.1.password".to_string(),
            value_encrypted: secrets.encrypt("secret")?,
            rotatable: true,
        })
        .await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.nzbget/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    let servers = control_surface_section(&payload, "servers");
    let actions = servers
        .get("actions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        actions
            .iter()
            .find(|action| action.get("id").and_then(Value::as_str) == Some("add_server"))
            .and_then(|action| action.get("label"))
            .and_then(Value::as_str),
        Some("Add provider")
    );
    assert!(
        actions
            .iter()
            .find(|action| action.get("id").and_then(Value::as_str) == Some("add_server"))
            .and_then(|action| action.pointer("/params/promptFields"))
            .and_then(Value::as_array)
            .map(|fields| !fields.is_empty())
            .unwrap_or(false),
        "expected add_server prompt fields in NZBGet control surface: {}",
        payload
    );

    let entities = servers
        .get("entities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let provider = entities
        .iter()
        .find(|entity| entity.get("title").and_then(Value::as_str) == Some("Newshosting"))
        .expect("nzbget provider entity");
    assert_eq!(
        provider.pointer("/actions/0/id").and_then(Value::as_str),
        Some("edit_server")
    );
    assert_eq!(
        provider.pointer("/actions/1/id").and_then(Value::as_str),
        Some("test_server")
    );
    assert_eq!(
        provider.pointer("/actions/2/id").and_then(Value::as_str),
        Some("remove_server")
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_surface_reports_nzbget_managed_invariant_drift() -> Result<()> {
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

    let (host, addr, _mock_state, shutdown_tx) = start_mock_nzbget_control_server(
        vec![
            ("DestDir".to_string(), "/other/downloads".to_string()),
            ("InterDir".to_string(), "/other/incomplete".to_string()),
            ("NzbDir".to_string(), "/runtime/nzb".to_string()),
            ("QueueDir".to_string(), "/runtime/queue".to_string()),
            ("TempDir".to_string(), "/runtime/tmp".to_string()),
            ("LockFile".to_string(), "/config/nzbget.lock".to_string()),
            ("Category1.Name".to_string(), "tv".to_string()),
            (
                "Category1.DestDir".to_string(),
                "/other/downloads/tv".to_string(),
            ),
            ("Category2.Name".to_string(), "anime".to_string()),
            (
                "Category2.DestDir".to_string(),
                "/downloads/anime".to_string(),
            ),
            ("Category3.Name".to_string(), "movies".to_string()),
            (
                "Category3.DestDir".to_string(),
                "/downloads/movies".to_string(),
            ),
        ],
        json!(""),
    )
    .await?;
    let store = ExtensionStore::new(&db_pool);
    seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;

    let response = app
        .clone()
        .oneshot(
            Request::get("/api/v1/extensions/elixir.modules.nzbget/control-surface")
                .body(Body::empty())?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;

    assert_eq!(
        payload.pointer("/status/summary").and_then(Value::as_str),
        Some("Managed drift detected")
    );
    let section = control_surface_section(&payload, "managedInvariants");
    let notices = section
        .get("notices")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        notices.iter().any(|notice| {
            notice.get("code").and_then(Value::as_str) == Some("managed_nzbget_path_drift")
                && notice.get("title").and_then(Value::as_str) == Some("NZBGet DestDir drifted")
        }),
        "expected NZBGet path drift notice in control surface: {}",
        payload
    );
    assert!(
        notices.iter().any(|notice| {
            notice.get("code").and_then(Value::as_str) == Some("managed_nzbget_category_drift")
                && notice.get("title").and_then(Value::as_str)
                    == Some("NZBGet category 'tv' drifted")
        }),
        "expected NZBGet category drift notice in control surface: {}",
        payload
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_action_remove_nzbget_server_survives_reload_gap() -> Result<()> {
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

    let (host, addr, mock_state, shutdown_tx) = start_mock_nzbget_control_server(
        vec![
            ("Server1.Active".to_string(), "yes".to_string()),
            ("Server1.Name".to_string(), "XSNews".to_string()),
            ("Server1.Level".to_string(), "0".to_string()),
            ("Server1.Host".to_string(), "reader.xsnews.nl".to_string()),
            ("Server1.Encryption".to_string(), "yes".to_string()),
            ("Server1.Port".to_string(), "563".to_string()),
            ("Server1.Username".to_string(), "reader".to_string()),
            (
                "Server1.Password".to_string(),
                "provider-secret".to_string(),
            ),
            ("Server1.Connections".to_string(), "20".to_string()),
            ("Server1.CertVerification".to_string(), "strict".to_string()),
        ],
        json!(""),
    )
    .await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget.server.1.username".to_string(),
            value_encrypted: secrets.encrypt("reader")?,
            rotatable: true,
        })
        .await?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "nzbget.server.1.password".to_string(),
            value_encrypted: secrets.encrypt("provider-secret")?,
            rotatable: true,
        })
        .await?;
    *mock_state.config_failures_after_save.lock().unwrap() = 4;

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.nzbget/control-surface/actions/remove_server",
            )
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&json!({
                "params": { "slot": 1 }
            }))?))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected nzbget remove_server response: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload.get("success").and_then(Value::as_bool), Some(true));
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("Removed XSNews from NZBGet."),
        "expected successful removal message: {}",
        payload
    );
    assert_eq!(
        payload
            .pointer("/controlSurface/status/summary")
            .and_then(Value::as_str),
        Some("Add provider")
    );

    let surface = payload
        .get("controlSurface")
        .expect("control surface in action response");
    let servers = control_surface_section(surface, "servers");
    assert_eq!(
        servers
            .get("entities")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    assert!(
        store
            .get_secret(
                SecretScope::Instance,
                Some(instance_id),
                "nzbget.server.1.username",
            )
            .await?
            .is_none(),
        "expected username secret to be removed"
    );
    assert!(
        store
            .get_secret(
                SecretScope::Instance,
                Some(instance_id),
                "nzbget.server.1.password",
            )
            .await?
            .is_none(),
        "expected password secret to be removed"
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("nzbget instance");
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/server_inventory"))
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    let live_config = mock_state.config.lock().unwrap().clone();
    assert_eq!(
        live_config.get("Server1.Host").map(String::as_str),
        Some("")
    );
    assert_eq!(
        live_config.get("Server1.Active").map(String::as_str),
        Some("no")
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_action_add_nzbget_server_saves_config_and_secrets() -> Result<()> {
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

    let (host, addr, mock_state, shutdown_tx) =
        start_mock_nzbget_control_server(Vec::new(), json!("")).await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.nzbget/control-surface/actions/add_server",
            )
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&json!({
                "params": {
                    "name": "Newshosting",
                    "host": "news.newshosting.com",
                    "port": 563,
                    "username": "reader",
                    "password": "provider-secret",
                    "encryption": true,
                    "connections": 30,
                    "priority": 0,
                    "certVerification": "strict",
                    "active": true
                }
            }))?))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected nzbget add_server response: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload.get("success").and_then(Value::as_bool), Some(true));
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("Saved and validated Newshosting."),
        "expected successful validation message: {}",
        payload
    );
    assert_eq!(
        payload
            .pointer("/controlSurface/status/summary")
            .and_then(Value::as_str),
        Some("Ready")
    );

    let username_secret = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            "nzbget.server.1.username",
        )
        .await?
        .expect("nzbget provider username secret");
    let password_secret = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            "nzbget.server.1.password",
        )
        .await?
        .expect("nzbget provider password secret");
    assert_eq!(secrets.decrypt(&username_secret.value_encrypted)?, "reader");
    assert_eq!(
        secrets.decrypt(&password_secret.value_encrypted)?,
        "provider-secret"
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("nzbget instance");
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/server_inventory/0/host"))
            .and_then(Value::as_str),
        Some("news.newshosting.com")
    );
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/server_inventory/0/cert_verification"))
            .and_then(Value::as_str),
        Some("strict")
    );

    let save_calls = mock_state.save_calls.lock().unwrap().clone();
    assert_eq!(save_calls.len(), 1, "expected one saveconfig call");
    let save_updates = save_calls[0]
        .get(0)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        save_updates.iter().any(|update| {
            update.get("Name").and_then(Value::as_str) == Some("Server1.Host")
                && update.get("Value").and_then(Value::as_str) == Some("news.newshosting.com")
        }),
        "expected Server1.Host saveconfig update: {:?}",
        save_updates
    );
    assert!(
        save_updates.iter().any(|update| {
            update.get("Name").and_then(Value::as_str) == Some("Server1.Connections")
                && update.get("Value").and_then(Value::as_str) == Some("30")
        }),
        "expected Server1.Connections saveconfig update: {:?}",
        save_updates
    );

    let test_calls = mock_state.test_calls.lock().unwrap().clone();
    assert_eq!(test_calls.len(), 1, "expected one testserver call");
    let first_test = test_calls[0].as_array().cloned().unwrap_or_default();
    assert_eq!(
        first_test.get(0).and_then(Value::as_str),
        Some("news.newshosting.com")
    );
    assert_eq!(first_test.get(2).and_then(Value::as_str), Some("reader"));
    assert_eq!(
        first_test.get(3).and_then(Value::as_str),
        Some("provider-secret")
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_action_add_nzbget_server_survives_validation_reload_gap() -> Result<()> {
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

    let (host, addr, mock_state, shutdown_tx) =
        start_mock_nzbget_control_server(Vec::new(), json!("")).await?;
    let store = ExtensionStore::new(&db_pool);
    let instance_id = seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;
    *mock_state.testserver_error_after_save.lock().unwrap() =
        Some("validation temporarily unavailable".to_string());

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.nzbget/control-surface/actions/add_server",
            )
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&json!({
                "params": {
                    "name": "XSNews",
                    "host": "news.xsnews.nl",
                    "port": 563,
                    "username": "reader",
                    "password": "provider-secret",
                    "encryption": true,
                    "connections": 32,
                    "priority": 0,
                    "certVerification": "strict",
                    "active": true
                }
            }))?))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    let status = response.status();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected nzbget add_server response: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: Value = serde_json::from_slice(&body)?;
    assert_eq!(payload.get("success").and_then(Value::as_bool), Some(true));
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("did not come back quickly enough to validate"),
        "expected validation retry/deferred message: {}",
        payload
    );
    assert_eq!(
        payload
            .pointer("/controlSurface/status/summary")
            .and_then(Value::as_str),
        Some("Ready")
    );
    let surface = payload
        .get("controlSurface")
        .expect("control surface in action response");
    let servers = control_surface_section(surface, "servers");
    assert_eq!(
        servers
            .get("entities")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("nzbget instance");
    assert_eq!(
        instance
            .config_json
            .as_ref()
            .and_then(|config| config.pointer("/server_inventory/0/host"))
            .and_then(Value::as_str),
        Some("news.xsnews.nl")
    );

    let test_calls = mock_state.test_calls.lock().unwrap().clone();
    assert_eq!(
        test_calls.len(),
        4,
        "expected validation retries across reload gap"
    );

    Ok(())
}

#[tokio::test]
async fn extension_control_action_add_nzbget_server_reports_tls_validation_failures() -> Result<()>
{
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

    let (host, addr, _mock_state, shutdown_tx) =
        start_mock_nzbget_control_server(Vec::new(), json!("certificate verify failed")).await?;
    let store = ExtensionStore::new(&db_pool);
    seed_nzbget_control_extension(&store, &secrets, host, addr.port()).await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(
                "/api/v1/extensions/elixir.modules.nzbget/control-surface/actions/add_server",
            )
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&json!({
                "params": {
                    "name": "TLS Provider",
                    "host": "tls.example.com",
                    "port": 563,
                    "username": "reader",
                    "password": "provider-secret",
                    "encryption": true,
                    "connections": 20,
                    "priority": 0,
                    "certVerification": "strict",
                    "active": true
                }
            }))?))?,
        )
        .await?;
    let _ = shutdown_tx.send(());
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    let payload: Value = serde_json::from_slice(&body)?;
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .contains("TLS mismatch."),
        "expected TLS validation classification: {}",
        payload
    );

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
            entity.get("title").and_then(Value::as_str) == Some("Prowlarr Public Indexers")
        })
        .expect("public connector entity");
    assert_eq!(
        public_connector
            .pointer("/actions/0/navigateExtensionId")
            .and_then(Value::as_str),
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
        nzbgeek
            .pointer("/actions/0/requiredFields/0")
            .and_then(Value::as_str),
        Some("api_key")
    );
    assert_eq!(
        nzbgeek
            .pointer("/actions/0/secretKeys/0")
            .and_then(Value::as_str),
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
async fn extensions_uninstall_source_preserves_acquisition_history() -> Result<()> {
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
    let extension_id = "elixir.sources.torrentio_stremio";
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.to_string(),
            name: "Torrentio-Compatible Source".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Community,
            manifest_json: json!({
                "id": extension_id,
                "version": "0.1.0",
                "kind": "module",
                "name": "Torrentio-Compatible Source",
                "provides": [{
                    "capability": "acquisition.candidate_provider",
                    "slot": "default",
                    "implementation": "torrentio_stremio",
                    "scope": {
                        "requires_account": false,
                        "required_fields": []
                    }
                }],
                "runtime": {
                    "type": "container",
                    "image": "elixir/torrentio-candidate-provider:0.1.0"
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
            config_json: Some(json!({ "baseUrl": "https://torrentio.strem.fun" })),
            enabled: true,
        })
        .await?;
    let provider_id = Uuid::new_v4();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "acquisition.candidate_provider".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::Many,
            implementation: Some("torrentio_stremio".to_string()),
            scope_json: None,
            endpoint_json: Some(json!({
                "scheme": "http",
                "host": "elx-torrentio-source",
                "port": 8097,
                "base_path": "/",
                "network": "elixir_net"
            })),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;

    let subscription_id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_subscriptions (
            subscription_id,
            media_type,
            title,
            normalized_title,
            source_provider_id
         ) VALUES ($1, 'movie', 'Example Movie', 'example movie', $2)",
    )
    .bind(subscription_id.to_string())
    .bind(provider_id.to_string())
    .execute(&db_pool)
    .await?;

    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/extensions/{extension_id}/uninstall"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    assert!(store.get_extension(extension_id).await?.is_none());
    assert!(store.list_instances(Some(extension_id)).await?.is_empty());
    assert!(store.list_providers(Some(instance_id)).await?.is_empty());

    let source_provider_cleared = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT CASE WHEN source_provider_id IS NULL THEN 1 ELSE 0 END
         FROM acquisition_subscriptions
         WHERE subscription_id = $1",
    )
    .bind(subscription_id.to_string())
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(
        source_provider_cleared, 1,
        "expected acquisition history to remain with source provider reference cleared"
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
        "UPDATE orchestrator_runs SET created_at = '2000-01-01 00:00:00' WHERE run_id = $1",
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
