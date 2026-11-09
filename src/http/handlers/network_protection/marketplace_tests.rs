//! Real normal-router owner/session/CSRF checks, not asserted UI authority.
//! These imports do not start a runtime, provider account or public worker.
use super::*;
use anyhow::{Context, Result};
use axum::{
    body::{self, Body},
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use tower::ServiceExt;

const PATH: &str = "/api/v1/network/protection/import/wireguard";
fn payload(purpose: &str) -> Value {
    json!({"profileId":"marketplace-network","name":"Dedicated fixture","purpose":purpose,
        "config":format!("[Interface]\nPrivateKey = {}\nAddress = 10.66.0.2/32\n[Peer]\nPublicKey = {}\nAllowedIPs = 0.0.0.0/0\nEndpoint = 8.8.8.8:51820\n",STANDARD.encode([17;32]),STANDARD.encode([34;32]))})
}
async fn fixture() -> Result<(
    tempfile::TempDir,
    AppState,
    axum::Router,
    CurrentPrincipal,
    String,
)> {
    let root = tempfile::tempdir()?;
    let url = format!(
        "sqlite://{}",
        root.path().join("connection.sqlite").display()
    );
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
    Ok((root, state, app, principal, token))
}
async fn unchanged(state: &AppState) -> Result<()> {
    let profiles:i64=sqlx::query_scalar("SELECT COUNT(*) FROM download_network_profiles WHERE scope IN ('marketplace_network','marketplace_http')").fetch_one(&state.db_pool).await?;
    assert_eq!(profiles, 0);
    let secrets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM download_network_profile_secrets")
        .fetch_one(&state.db_pool)
        .await?;
    assert_eq!(secrets, 0);
    Ok(())
}
#[tokio::test]
async fn marketplace_import_requires_current_owner_and_cookie_csrf_before_any_secret_or_profile_write()
-> Result<()> {
    let (_root, state, app, principal, token) = fixture().await?;
    for purpose in ["marketplace_network", "marketplace_http"] {
        let response = app
            .clone()
            .oneshot(
                Request::post(PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(payload(purpose).to_string()))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        unchanged(&state).await?;
    }
    let cookie = format!("elixir_ui_token={token}");
    let csrf = state
        .auth_service
        .csrf_token(principal.account_session_id, principal.csrf_revision)?;
    for wrong in [None, Some("wrong")] {
        let mut request = Request::post(PATH)
            .header("cookie", &cookie)
            .header("content-type", "application/json");
        if let Some(value) = wrong {
            request = request.header("x-elixir-csrf", value);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(payload("marketplace_network").to_string()))?)
            .await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        unchanged(&state).await?;
    }
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("{PATH}?token={token}"))
                .header("content-type", "application/json")
                .body(Body::from(payload("marketplace_network").to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    unchanged(&state).await?;
    let response = app
        .clone()
        .oneshot(
            Request::post(PATH)
                .header("cookie", cookie)
                .header("x-elixir-csrf", csrf.expose_secret())
                .header("content-type", "application/json")
                .body(Body::from(payload("marketplace_network").to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), 64 * 1024).await?;
    assert!(!String::from_utf8_lossy(&bytes).contains(&STANDARD.encode([17; 32])));
    let value: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(value["profile"]["scope"], "marketplace_network");
    assert_eq!(
        state.marketplace.snapshot().phase,
        crate::marketplace::service::RuntimePhase::NotConnected
    );
    let material = crate::network::protection::marketplace_network_material(
        &state.db_pool,
        &state.secrets,
        "marketplace-network",
    )
    .await?;
    assert!(material.contains("PrivateKey"));
    let ciphertext:String=sqlx::query_scalar("SELECT value_encrypted FROM secrets WHERE key IN (SELECT substr(secret_ref,8) FROM download_network_profile_secrets WHERE profile_id='marketplace-network')").fetch_optional(&state.db_pool).await?.context("encrypted config row")?;
    assert!(ciphertext.starts_with("enc:v1:"));
    assert!(!ciphertext.contains(&STANDARD.encode([17; 32])));
    Ok(())
}
#[tokio::test]
async fn marketplace_import_rechecks_current_owner_role_and_logout_not_just_valid_jwt_claims()
-> Result<()> {
    let (_root, state, app, p, token) = fixture().await?;
    sqlx::query("UPDATE home_members SET role='admin' WHERE home_id=$1 AND user_id=$2")
        .bind(p.home_id.to_string())
        .bind(p.user_id.to_string())
        .execute(&state.db_pool)
        .await?;
    let request = || {
        Request::post(PATH)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(payload("marketplace_network").to_string()))
    };
    let response = app.clone().oneshot(request()?).await?;
    assert!(matches!(
        response.status(),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
    ));
    unchanged(&state).await?;
    sqlx::query("UPDATE home_members SET role='owner' WHERE home_id=$1 AND user_id=$2")
        .bind(p.home_id.to_string())
        .bind(p.user_id.to_string())
        .execute(&state.db_pool)
        .await?;
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='logout' WHERE id=$1").bind(p.account_session_id.to_string()).execute(&state.db_pool).await?;
    let response = app.clone().oneshot(request()?).await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    unchanged(&state).await?;
    Ok(())
}
