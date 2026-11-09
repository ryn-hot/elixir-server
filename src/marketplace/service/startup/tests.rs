use super::*;

#[tokio::test(start_paused = true)]
async fn startup_hints_are_coalesced_owned_progress_not_worker_readiness() -> Result<()> {
    let service = super::super::MarketplaceService::default();
    assert!(!service.connection_configuration_changed());
    assert!(service.claim_startup_progress().is_err());
    service.select_startup();
    assert!(!service.connection_configuration_changed());
    let owner = service.claim_startup_progress()?;
    assert!(service.claim_startup_progress().is_err());
    for _ in 0..10_000 {
        assert!(service.connection_configuration_changed());
    }
    service.wait_connection_configuration_changed().await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(1),
            service.wait_connection_configuration_changed()
        )
        .await
        .is_err()
    );
    let waiting = tokio::spawn({
        let service = service.clone();
        async move { service.wait_connection_configuration_changed().await }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    for _ in 0..10_000 {
        assert!(service.connection_configuration_changed());
    }
    waiting.await?;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(1),
            service.wait_connection_configuration_changed()
        )
        .await
        .is_err(),
        "a delivered wake plus a stored Notify permit must not become two hints"
    );
    service.startup_retry_after(Some(Duration::from_secs(1_000)));
    let view = service.startup_snapshot();
    assert!(view.configured && view.retry_owner_present);
    assert_eq!(view.retry_after_seconds, Some(300));
    tokio::time::advance(Duration::from_millis(500)).await;
    assert_eq!(service.startup_snapshot().retry_after_seconds, Some(300));
    tokio::time::advance(Duration::from_secs(300)).await;
    assert_eq!(service.startup_snapshot().retry_after_seconds, Some(0));
    assert_eq!(
        service.snapshot().phase,
        super::super::RuntimePhase::NotConnected
    );
    assert!(service.snapshot().participation.is_none());
    drop(owner);
    let view = service.startup_snapshot();
    assert!(view.configured && !view.retry_owner_present);
    assert_eq!(view.retry_after_seconds, None);
    assert!(!service.connection_configuration_changed());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn authenticated_retry_and_committed_import_only_wake_the_existing_startup_owner()
-> Result<()> {
    use axum::{
        body::{self, Body},
        http::{Request, StatusCode},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};
    use tower::ServiceExt;
    let root = tempfile::tempdir()?;
    let url = format!("sqlite://{}", root.path().join("retry.sqlite").display());
    let database = crate::db::Database::connect(&crate::config::DatabaseConfig {
        url: url.clone(),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let (state, app, principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture(database, root.path(), &url)
            .await?;
    let retry = "/api/v1/marketplace/connection/retry";
    let post = |path: &str, body: Value| {
        Request::post(path)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
    };
    let value = |requested| json!({"schema_version":1,"requested":requested});
    let reply = app
        .clone()
        .oneshot(post(retry, json!({"schema_version":1}))?)
        .await?;
    assert_eq!(reply.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body::to_bytes(reply.into_body(), 1024).await?)?,
        value(false)
    );
    assert!(!state.marketplace.startup_snapshot().configured);
    state.marketplace.select_startup();
    let owner = state.marketplace.claim_startup_progress()?;
    for body in [
        json!({"schema_version":2}),
        json!({"schema_version":1,"ready":true}),
    ] {
        let reply = app.clone().oneshot(post(retry, body)?).await?;
        assert_eq!(reply.status(), StatusCode::BAD_REQUEST);
    }
    let reply = app
        .clone()
        .oneshot(
            Request::post(retry)
                .header("content-type", "application/json")
                .body(Body::from("{\"schema_version\":1}"))?,
        )
        .await?;
    assert_eq!(reply.status(), StatusCode::UNAUTHORIZED);
    let reply = app
        .clone()
        .oneshot(
            Request::post(retry)
                .header("cookie", format!("elixir_ui_token={token}"))
                .header("content-type", "application/json")
                .body(Body::from("{\"schema_version\":1}"))?,
        )
        .await?;
    assert_eq!(reply.status(), StatusCode::FORBIDDEN);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            state.marketplace.wait_connection_configuration_changed()
        )
        .await
        .is_err()
    );
    let csrf = state
        .auth_service
        .csrf_token(principal.account_session_id, principal.csrf_revision)?;
    let reply = app
        .clone()
        .oneshot(
            Request::post(retry)
                .header("cookie", format!("elixir_ui_token={token}"))
                .header("x-elixir-csrf", csrf.expose_secret())
                .header("content-type", "application/json")
                .body(Body::from("{\"schema_version\":1}"))?,
        )
        .await?;
    assert_eq!(reply.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body::to_bytes(reply.into_body(), 1024).await?)?,
        value(true)
    );
    tokio::time::timeout(
        Duration::from_secs(1),
        state.marketplace.wait_connection_configuration_changed(),
    )
    .await?;
    let import = "/api/v1/network/protection/import/wireguard";
    let payload = json!({"profileId":"marketplace-network","name":"Retry fixture","purpose":"marketplace_network",
        "config":format!("[Interface]\nPrivateKey = {}\nAddress = 10.66.0.2/32\n[Peer]\nPublicKey = {}\nAllowedIPs = 0.0.0.0/0\nEndpoint = 8.8.8.8:51820\n",STANDARD.encode([17;32]),STANDARD.encode([34;32]))});
    let reply = app.clone().oneshot(post(import, payload)?).await?;
    assert_eq!(reply.status(), StatusCode::OK);
    tokio::time::timeout(
        Duration::from_secs(1),
        state.marketplace.wait_connection_configuration_changed(),
    )
    .await?;
    let reply = app
        .clone()
        .oneshot(post(
            import,
            json!({"purpose":"marketplace_network","config":"invalid"}),
        )?)
        .await?;
    assert!(reply.status().is_client_error());
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            state.marketplace.wait_connection_configuration_changed()
        )
        .await
        .is_err()
    );
    let reply = app
        .clone()
        .oneshot(
            Request::get("/api/v1/marketplace/settings")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(reply.status(), StatusCode::OK);
    let settings: Value =
        serde_json::from_slice(&body::to_bytes(reply.into_body(), 16 * 1024).await?)?;
    assert_eq!(settings["startup"]["retry_owner_present"], true);
    assert_eq!(settings["runtime_status"], "not_connected");
    assert!(settings["runtime"]["participation"].is_null());
    drop(owner);
    let reply = app
        .clone()
        .oneshot(post(retry, json!({"schema_version":1}))?)
        .await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&body::to_bytes(reply.into_body(), 1024).await?)?,
        value(false)
    );
    Ok(())
}
