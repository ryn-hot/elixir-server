//! Real router/authentication/CSRF and bounded DATA; no native/network proof.
use super::*;
use anyhow::{Context, Result};
use axum::{
    body::{self, Body},
    extract::FromRequestParts,
    http::Request,
};
use serde_json::{Value, json};
use std::path::Path as FsPath;
use tower::ServiceExt;

fn mutation_frame<F: std::future::Future>(
    _: fn(State<AppState>, CurrentPrincipal, HeaderMap, Bytes) -> F,
) -> usize {
    std::mem::size_of::<F>()
}
fn action_frame<F: std::future::Future>(
    _: fn(State<AppState>, CurrentPrincipal, Path<Uuid>, HeaderMap, Bytes) -> F,
) -> usize {
    std::mem::size_of::<F>()
}
fn status_frame<F: std::future::Future>(
    _: fn(State<AppState>, CurrentPrincipal, Path<Uuid>) -> F,
) -> usize {
    std::mem::size_of::<F>()
}
fn current_frame<F: std::future::Future>(_: fn(State<AppState>, CurrentPrincipal) -> F) -> usize {
    std::mem::size_of::<F>()
}
fn upload_frame<F: std::future::Future>(
    _: fn(State<AppState>, CurrentPrincipal, Path<(Uuid, Uuid)>, HeaderMap, Body) -> F,
) -> usize {
    std::mem::size_of::<F>()
}
#[test]
fn source_upload_headers_reject_ambiguous_encoded_or_unbounded_inputs() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert("content-length", HeaderValue::from_static("42"));
    assert_eq!(upload_length(&headers).unwrap(), 42);
    for value in [
        "0",
        "+42",
        " 42",
        "42 ",
        "18446744073709551616",
        "999999999",
    ] {
        let mut bad = headers.clone();
        bad.insert("content-length", HeaderValue::from_str(value).unwrap());
        assert!(upload_length(&bad).is_err(), "{value}");
    }
    for (name, value) in [
        ("content-encoding", "gzip"),
        ("transfer-encoding", "chunked"),
        ("content-type", "application/json"),
    ] {
        let mut bad = headers.clone();
        bad.insert(name, HeaderValue::from_static(value));
        assert!(upload_length(&bad).is_err());
    }
    let mut bad = headers.clone();
    bad.append("content-length", HeaderValue::from_static("42"));
    assert!(upload_length(&bad).is_err());
    headers.remove("content-length");
    assert!(upload_length(&headers).is_err());
}
#[test]
fn management_http_futures_keep_the_existing_128_kib_frame_bound() {
    for (name, bytes) in [
        ("targets", mutation_frame(targets)),
        ("start", mutation_frame(start)),
        ("publisher_seeding", mutation_frame(publisher_seeding)),
        ("submit_withdrawal", mutation_frame(submit_withdrawal)),
        ("upload_source", upload_frame(upload_source)),
        ("status", status_frame(status)),
        ("current", current_frame(current)),
        ("action", action_frame(action)),
    ] {
        assert!(
            bytes <= 128 * 1024,
            "{name} HTTP future grew to {bytes} bytes"
        );
    }
}

pub(crate) async fn authenticated_fixture(
    database: Database,
    root: &FsPath,
    url: &str,
) -> Result<(AppState, Router, CurrentPrincipal, String)> {
    let mut settings = crate::http::test_settings_with_db();
    settings.database.url = url.into();
    settings.extensions.storage_root = root.join("extensions").display().to_string();
    settings.extensions.bundled_dir = root.join("bundled").display().to_string();
    let auth = AuthService::new(settings.auth.clone())?;
    let metadata = crate::metadata::MetadataService::new(settings.metadata.clone())?;
    let linkers = crate::library::LinkerService::new(settings.classifier.clone())?;
    let artwork = crate::http::test_artwork_service(&settings)?;
    let secrets = crate::secrets::SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth,
        crate::extensions::ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = crate::http::router(state.clone());
    // Local fixture signup through actual auth code, never provider enrollment
    // or an asserted/serialized owner principal.
    let response=app.clone().oneshot(Request::post("/api/v1/auth/signup").header("content-type","application/json")
        .body(Body::from(json!({"email":format!("{}@management.example.test",Uuid::new_v4()),"password":"fixture-password-only"}).to_string()))?).await?;
    anyhow::ensure!(
        response.status().is_success(),
        "local fixture signup failed"
    );
    let tokens: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), 64 * 1024).await?)?;
    let token = tokens["access_token"]
        .as_str()
        .context("actual fixture token missing")?
        .to_owned();
    let (request, _) = Request::get("/")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())?
        .into_parts();
    let mut request = request;
    let principal = CurrentPrincipal::from_request_parts(&mut request, &state)
        .await
        .map_err(|_| anyhow::anyhow!("actual fixture token extraction failed"))?;
    let change = crate::marketplace::preferences::Change {
        expected_revision: 0,
        mutation_id: Uuid::new_v4(),
        preferences: Default::default(),
    };
    let response = app
        .clone()
        .oneshot(
            Request::patch("/api/v1/marketplace/settings")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&change)?))?,
        )
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "actual owner preference binding failed"
    );
    Ok((state, app, principal, token))
}
fn extension() -> Result<Value> {
    let vector: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    Ok(vector["release"]["payload"]["extension_id"].clone())
}
fn install() -> Result<Value> {
    Ok(
        json!({"schema_version":1,"query_id":Uuid::new_v4(),"extension_id":extension()?,"release_id":elixir_marketplace::identity::Digest::of(b"request DATA"),"target":{"kind":"new","name":"A"},"expected_revision":0,"mutation_id":Uuid::new_v4(),"controls":{"kind":"absent"}}),
    )
}

#[tokio::test]
async fn trusted_local_management_selection_mounts_normal_routes_without_granting_admission()
-> Result<()> {
    let (_root, mut state, _, _, token) = state().await?;
    let mut settings = state.settings.as_ref().clone();
    assert!(!settings.marketplace.installation_management_configured());
    settings.marketplace.installation_management = true;
    assert!(!settings.marketplace.installation_management_configured());
    settings.marketplace.startup_file = Some("/local/not-read/startup.json".into());
    assert!(!settings.marketplace.installation_management_configured());
    settings.marketplace.installed_startup_file = Some("/local/not-read/installed.json".into());
    assert!(settings.marketplace.installation_management_configured());
    state.settings = std::sync::Arc::new(settings);
    let app = crate::http::router(state.clone());
    let path = "/api/v1/marketplace/installations";
    assert_eq!(
        app.clone()
            .oneshot(Request::post(path).body(Body::from(install()?.to_string()))?)
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = app
        .clone()
        .oneshot(
            Request::post(path)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(install()?.to_string()))?,
        )
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "routing selection grants no allocation, connection or source authority"
    );
    let response = app
        .oneshot(
            Request::post("/api/v1/marketplace/install/targets")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"schema_version":1,"extension_id":extension()?}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let view: Value = serde_json::from_slice(&body::to_bytes(response.into_body(), 65536).await?)?;
    assert_eq!(view["installation_ready"], false);
    assert!(view.get("review_available").is_none());
    assert!(!state.marketplace.installation_review_available());
    assert_eq!(
        state.marketplace.snapshot().phase,
        RuntimePhase::NotConnected
    );
    state.marketplace.shutdown().await?;
    Ok(())
}
async fn state() -> Result<(
    tempfile::TempDir,
    AppState,
    Router,
    CurrentPrincipal,
    String,
)> {
    let root = tempfile::tempdir()?;
    let url = format!("sqlite://{}", root.path().join("http.sqlite").display());
    let database = Database::connect(&crate::config::DatabaseConfig {
        url: url.clone(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let (state, app, principal, token) = authenticated_fixture(database, root.path(), &url).await?;
    Ok((root, state, app, principal, token))
}
#[tokio::test]
async fn public_target_route_requires_real_current_owner_and_only_returns_bounded_choice_data()
-> Result<()> {
    let (_root, state, app, p, token) = state().await?;
    let bytes = json!({"schema_version":1,"extension_id":extension()?}).to_string();
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/marketplace/install/targets")
                .header("content-type", "application/json")
                .body(Body::from(bytes.clone()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["cache-control"], "no-store, private");
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/marketplace/install/targets")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(bytes.clone()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store, private");
    let view: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), 64 * 1024).await?)?;
    assert_eq!(view["new_instance"]["controls"]["kind"], "absent");
    assert_eq!(view["installation_ready"], false);
    assert_eq!(view["instances"], json!([]));
    for (path, data, expected) in [
        (
            format!("/api/v1/marketplace/install/targets?token={token}"),
            bytes.clone(),
            StatusCode::FORBIDDEN,
        ),
        (
            "/api/v1/marketplace/install/targets".into(),
            "x".repeat(4097),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "/api/v1/marketplace/install/targets".into(),
            json!({"schema_version":1,"extension_id":extension()?,"budget":4}).to_string(),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let mut request = Request::post(path).header("content-type", "application/json");
        if expected != StatusCode::FORBIDDEN {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        assert_eq!(
            app.clone()
                .oneshot(request.body(Body::from(data))?)
                .await?
                .status(),
            expected
        );
    }
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='test' WHERE id=$1")
        .bind(p.account_session_id.to_string())
        .execute(&state.db_pool)
        .await?;
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/marketplace/install/targets")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(bytes))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        app.oneshot(
            Request::get("/api/v1/marketplace/installations/current")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?
        )
        .await?
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        state.marketplace.snapshot().phase,
        RuntimePhase::NotConnected
    );
    state.marketplace.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn management_mutations_are_not_public_and_private_handlers_enforce_real_auth_bounds_and_csrf()
-> Result<()> {
    let (_root, state, public, p, token) = state().await?;
    let bytes = install()?.to_string();
    let response = public
        .clone()
        .oneshot(
            Request::post("/api/v1/marketplace/installations")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(bytes.clone()))?,
        )
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "no public launch-gate bypass"
    );
    assert_eq!(
        public
            .clone()
            .oneshot(
                Request::post("/api/v1/marketplace/installations/direct")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?
            )
            .await?
            .status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "direct owner consent must not bypass the unfinished public runtime/network rollout"
    );
    let unknown = Uuid::new_v4();
    let upload_path = format!(
        "/api/v1/marketplace/installations/{unknown}/uploads/{}",
        Uuid::new_v4()
    );
    assert_eq!(
        public
            .clone()
            .oneshot(
                Request::post(&upload_path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/octet-stream")
                    .header("content-length", "1")
                    .body(Body::from("x"))?
            )
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        public
            .clone()
            .oneshot(
                Request::post("/api/v1/marketplace/publisher-seeding")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?
            )
            .await?
            .status(),
        StatusCode::NOT_FOUND,
        "publisher consent cannot activate unfinished public marketplace routes"
    );
    assert_eq!(public.clone().oneshot(Request::post(format!("/api/v1/marketplace/installations/{unknown}/actions"))
        .header("authorization",format!("Bearer {token}")).header("content-type","application/json")
        .body(Body::from(json!({"schema_version":1,"request":{"action":"cancel","mutation_id":Uuid::new_v4()}}).to_string()))?).await?.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        public
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/marketplace/installations/{unknown}"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())?
            )
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        public
            .clone()
            .oneshot(
                Request::get("/api/v1/marketplace/installations/current")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())?
            )
            .await?
            .status(),
        StatusCode::OK,
        "owner-only data reads do not require installation admission"
    );
    let app = public.merge(installation_routes().with_state(state.clone()));
    assert_eq!(
        app.clone()
            .oneshot(
                Request::post(&upload_path)
                    .header("content-type", "application/octet-stream")
                    .header("content-length", "1")
                    .body(Body::from("x"))?
            )
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(
                Request::post(&upload_path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/octet-stream")
                    .header("content-length", "1")
                    .body(Body::from("x"))?
            )
            .await?
            .status(),
        StatusCode::CONFLICT,
        "raw bodies cannot create operations, preparations or new upload authority"
    );
    assert_eq!(
        app.clone()
            .oneshot(Request::get("/api/v1/marketplace/installations/current").body(Body::empty())?)
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let empty = app
        .clone()
        .oneshot(
            Request::get("/api/v1/marketplace/installations/current")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(empty.status(), StatusCode::OK);
    assert_eq!(empty.headers()["cache-control"], "no-store, private");
    let empty: Value = serde_json::from_slice(&body::to_bytes(empty.into_body(), 1024).await?)?;
    assert_eq!(empty, json!({"schema_version":1,"operation":null}));
    assert_eq!(
        app.clone()
            .oneshot(
                Request::post("/api/v1/marketplace/installations")
                    .header("content-type", "application/json")
                    .body(Body::from(bytes.clone()))?
            )
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/marketplace/installations")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(bytes.clone()))?,
        )
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "configuration is not authority or implicit startup"
    );
    let id = Uuid::new_v4();
    let action = format!("/api/v1/marketplace/installations/{id}/actions");
    for (path,data,expected) in [
        ("/api/v1/marketplace/installations".into(),"x".repeat(management::MAX_INSTALL_BYTES+1),StatusCode::PAYLOAD_TOO_LARGE),
        ("/api/v1/marketplace/installations/direct".into(),"{}".into(),StatusCode::BAD_REQUEST),
        ("/api/v1/marketplace/installations/direct".into(),"x".repeat(management::MAX_DIRECT_INSTALL_BYTES+1),StatusCode::PAYLOAD_TOO_LARGE),
        ("/api/v1/marketplace/publisher-seeding".into(),"{}".into(),StatusCode::BAD_REQUEST),
        ("/api/v1/marketplace/publisher-seeding".into(),"x".repeat(management::MAX_PUBLISHER_SEEDING_BYTES+1),StatusCode::PAYLOAD_TOO_LARGE),
        (action.clone(),"x".repeat(management::MAX_ACTION_BYTES+1),StatusCode::PAYLOAD_TOO_LARGE),
        (action.clone(),json!({"schema_version":1,"request":{"action":"setup","mutation_id":Uuid::new_v4(),"credentials":[{"operation":"set","key":"k","value":"private-fixture","rotatable":false,"owner":true}]}}).to_string(),StatusCode::BAD_REQUEST),
    ] {
        let response=app.clone().oneshot(Request::post(path).header("authorization",format!("Bearer {token}"))
            .header("content-type","application/json").body(Body::from(data))?).await?;
        assert_eq!(response.status(),expected);
        assert_eq!(response.headers()["cache-control"], "no-store, private");
        assert!(!String::from_utf8_lossy(&body::to_bytes(response.into_body(),64*1024).await?).contains("private-fixture"));
    }
    let csrf = state
        .auth_service
        .csrf_token(p.account_session_id, p.csrf_revision)?;
    let cookie = format!("elixir_ui_token={token}");
    assert_eq!(
        app.clone()
            .oneshot(
                Request::post("/api/v1/marketplace/publisher-seeding")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?
            )
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(
                Request::post("/api/v1/marketplace/publisher-seeding")
                    .header("cookie", &cookie)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?
            )
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    for provided in [None, Some("bad"), Some(csrf.expose_secret())] {
        let mut request = Request::post("/api/v1/marketplace/installations")
            .header("cookie", &cookie)
            .header("content-type", "application/json");
        if let Some(value) = provided {
            request = request.header("x-elixir-csrf", value);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(bytes.clone()))?)
            .await?;
        assert_eq!(
            response.status(),
            if provided == Some(csrf.expose_secret()) {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::FORBIDDEN
            }
        );
    }
    assert_eq!(
        state.marketplace.snapshot().phase,
        RuntimePhase::NotConnected
    );
    state.marketplace.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn withdrawal_submission_is_private_owner_only_bounded_and_never_implicit_startup()
-> Result<()> {
    let (_root, state, public, p, token) = state().await?;
    let path = "/api/v1/marketplace/withdrawals";
    let input = crate::marketplace::withdrawal::fixture_request();
    let bytes = serde_json::to_vec(&input)?;
    assert_eq!(
        public
            .clone()
            .oneshot(
                Request::post(path)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(bytes.clone()))?
            )
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let app = public.merge(installation_routes().with_state(state.clone()));
    assert_eq!(
        app.clone()
            .oneshot(Request::post(path).body(Body::from(bytes.clone()))?)
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(
                Request::post(format!("{path}?token={token}")).body(Body::from(bytes.clone()))?
            )
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let mut unconfirmed = input.clone();
    unconfirmed["accept_permanent_withdrawal"] = false.into();
    for (body, expected) in [
        (
            vec![b'x'; crate::marketplace::withdrawal::MAX_REQUEST_BYTES + 1],
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (serde_json::to_vec(&unconfirmed)?, StatusCode::BAD_REQUEST),
        (bytes.clone(), StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::post(path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))?,
            )
            .await?;
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()["cache-control"], "no-store, private");
    }
    let csrf = state
        .auth_service
        .csrf_token(p.account_session_id, p.csrf_revision)?;
    for provided in [None, Some("bad"), Some(csrf.expose_secret())] {
        let mut request = Request::post(path).header("cookie", format!("elixir_ui_token={token}"));
        if let Some(value) = provided {
            request = request.header("x-elixir-csrf", value);
        }
        assert_eq!(
            app.clone()
                .oneshot(request.body(Body::from(bytes.clone()))?)
                .await?
                .status(),
            if provided == Some(csrf.expose_secret()) {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::FORBIDDEN
            }
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_security")
        .fetch_one(&state.db_pool)
        .await?;
    assert_eq!(
        count, 0,
        "a configured route is not native authority or identity allocation"
    );
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='withdrawal test' WHERE id=$1")
        .bind(p.account_session_id.to_string()).execute(&state.db_pool).await?;
    assert_eq!(
        app.oneshot(
            Request::post(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(bytes))?
        )
        .await?
        .status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}
