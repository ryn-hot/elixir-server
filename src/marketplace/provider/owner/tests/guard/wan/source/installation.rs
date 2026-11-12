//! Real public source -> normal authenticated HTTP upload/review/activation ->
//! native Live provider -> manual update -> clean offline operator restart.
//! No synthetic positive source/protection/host/paid receipts or public seeding.
use super::*;
use crate::{marketplace::application::Allocation, state::AppState};
use anyhow::bail;
use axum::{
    Router,
    body::{self, Body},
    http::Request,
};
use futures_util::FutureExt;
use serde_json::{Value, json};
use std::{io::Write, os::unix::fs::OpenOptionsExt};
use tower::ServiceExt;
use uuid::Uuid;
mod holders;

async fn http(
    app: &Router,
    token: &str,
    method: &str,
    path: &str,
    value: Option<Value>,
) -> Result<Value> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    let bytes = if let Some(value) = value {
        request = request.header("content-type", "application/json");
        serde_json::to_vec(&value)?
    } else {
        vec![]
    };
    response(
        app.clone()
            .oneshot(request.body(Body::from(bytes))?)
            .await?,
        path,
    )
    .await
}
async fn response(response: axum::response::Response, path: &str) -> Result<Value> {
    ensure!(
        response.status().is_success(),
        "public-source management request failed: {} {path}",
        response.status()
    );
    Ok(serde_json::from_slice(
        &body::to_bytes(response.into_body(), 128 * 1024).await?,
    )?)
}
// Target availability is display/scheduling data, not an admission receipt.
// Mirror the UI's bounded read-only refresh when its verification slot is busy
// or a routine protection check starts between the ready check and HTTP reply.
async fn target_choices(
    app: &Router,
    token: &str,
    extension: &str,
    wait: Duration,
) -> Result<Value> {
    let expires = tokio::time::Instant::now() + wait;
    let request = json!({"schema_version":1,"extension_id":extension});
    loop {
        ensure!(
            tokio::time::Instant::now() < expires,
            "original target review-readiness deadline"
        );
        let targets = tokio::time::timeout_at(
            expires,
            http(
                app,
                token,
                "POST",
                "/api/v1/marketplace/install/targets",
                Some(request.clone()),
            ),
        )
        .await
        .context("original target review-readiness deadline")??;
        ensure!(
            targets["extension_id"] == extension,
            "target response changed extension"
        );
        ensure!(
            targets["installation_ready"] == false,
            "normal target response must never advertise installation authority"
        );
        match targets.get("review_available") {
            Some(Value::Bool(true)) => return Ok(targets),
            None | Some(Value::Bool(false)) => {}
            _ => bail!("target review availability must be a boolean or absent"),
        }
        tokio::time::sleep_until(
            expires.min(tokio::time::Instant::now() + Duration::from_millis(250)),
        )
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn target_controls_waits_for_true_review_without_creating_an_installation() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let app = Router::new().route(
        "/api/v1/marketplace/install/targets",
        axum::routing::post(move |axum::Json(request): axum::Json<Value>| {
            let count = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(
                    request,
                    json!({"schema_version":1,"extension_id":"fixture"})
                );
                let mut reply = json!({"extension_id":"fixture","installation_ready":false});
                if count != 0 {
                    reply["review_available"] = json!(true);
                }
                axum::Json(reply)
            }
        }),
    );
    let view = target_choices(&app, "fixture-only", "fixture", Duration::from_secs(2)).await?;
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    assert_eq!(view["installation_ready"], false);
    assert_eq!(view["review_available"], true);
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn target_controls_never_accepts_install_authority_or_non_boolean_review() -> Result<()> {
    for reply in [
        json!({"extension_id":"fixture","installation_ready":true,"review_available":true}),
        json!({"extension_id":"fixture","installation_ready":false,"review_available":"true"}),
    ] {
        let app = Router::new().route(
            "/api/v1/marketplace/install/targets",
            axum::routing::post(move || {
                let reply = reply.clone();
                async move { axum::Json(reply) }
            }),
        );
        assert!(
            target_choices(&app, "fixture-only", "fixture", Duration::from_secs(2))
                .await
                .is_err()
        );
    }
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn target_controls_readiness_wait_has_one_deadline() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let app = Router::new().route("/api/v1/marketplace/install/targets", axum::routing::post(
        move || { observed.fetch_add(1, Ordering::SeqCst); async {
            axum::Json(json!({"extension_id":"fixture","installation_ready":false,"review_available":false}))
        } }));
    let began = tokio::time::Instant::now();
    let error = target_choices(&app, "fixture-only", "fixture", Duration::from_millis(40))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("original target review-readiness deadline")
    );
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert!(began.elapsed() < Duration::from_secs(1));
    Ok(())
}
async fn action(app: &Router, token: &str, id: Uuid, request: Value) -> Result<()> {
    http(
        app,
        token,
        "POST",
        &format!("/api/v1/marketplace/installations/{id}/actions"),
        Some(json!({"schema_version":1,"request":request})),
    )
    .await?;
    Ok(())
}
pub(super) async fn save_off(app: &Router, token: &str) -> Result<()> {
    let reply = http(app, token, "PATCH", "/api/v1/marketplace/settings", Some(json!({
        "expected_revision":1,"mutation_id":Uuid::new_v4(),"preferences":Preferences {
            contribution:ContributionMode::Off,metering:ConnectionMetering::Metered,..Default::default()
        }}))).await?;
    ensure!(
        reply["settings"]["revision"] == 2
            && reply["settings"]["preferences"]["contribution"] == "off"
            && reply["settings"]["preferences"]["metering"] == "metered",
        "actual saved preference response differs"
    );
    Ok(())
}

#[tokio::test]
async fn public_install_preferences_use_the_real_authenticated_settings_envelope() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let url = format!(
        "sqlite://{}",
        directory.path().join("settings.sqlite").display()
    );
    let database = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let (state, app, principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture(
            database.clone(),
            directory.path(),
            &url,
        )
        .await?;
    save_off(&app, &token).await?;
    let stored = preferences::load(&database, &principal, chrono::Utc::now()).await?;
    ensure!(
        stored.revision == 2
            && stored.preferences.contribution == ContributionMode::Off
            && stored.preferences.metering == ConnectionMetering::Metered,
        "saved choice differs from HTTP acknowledgement"
    );
    ensure!(
        state.marketplace.client().is_err(),
        "settings fixture unexpectedly created a protected owner"
    );
    state.marketplace.shutdown().await?;
    database.pool.close().await;
    Ok(())
}
async fn phase(
    app: &Router,
    token: &str,
    id: Uuid,
    wanted: &str,
    source: Option<&SourceFixture>,
) -> Result<Value> {
    let until = tokio::time::Instant::now() + Duration::from_secs(240);
    let mut sent = std::collections::BTreeSet::new();
    loop {
        let view = http(
            app,
            token,
            "GET",
            &format!("/api/v1/marketplace/installations/{id}"),
            None,
        )
        .await?;
        let current = view["phase"]
            .as_str()
            .context("installation phase missing")?;
        let pending = matches!(
            view["latest_action"]["status"].as_str(),
            Some("queued" | "running")
        );
        if current == wanted && !pending {
            ensure!(
                source.is_none() || sent.len() == 2,
                "permission review did not follow both original native upload receipts"
            );
            return Ok(view);
        }
        ensure!(
            !matches!(current, "stopped" | "cancelled" | "recovery_required"),
            "public-source original operation reached {current}, expected {wanted}"
        );
        if let Some(upload) = view["preparation"]
            .get("upload")
            .filter(|value| !value.is_null())
        {
            let fixture = source.context("unexpected upload after source preparation")?;
            let upload_id: Uuid = serde_json::from_value(upload["upload_id"].clone())?;
            if upload["claimed"] == false && sent.insert(upload_id) {
                ensure!(
                    sent.len() <= 2,
                    "source input allocated more than the signed package/proof pair"
                );
                let (bytes, blob) = match upload["purpose"].as_str() {
                    Some("package") => (
                        &fixture.package,
                        &fixture.publication.release.payload.package,
                    ),
                    Some("proof") => (
                        &fixture.proof,
                        &fixture.publication.release.payload.source.proof,
                    ),
                    _ => anyhow::bail!("unexpected source upload purpose"),
                };
                ensure!(
                    upload["blob"] == serde_json::to_value(blob)?,
                    "upload differs from signed input"
                );
                let path = format!("/api/v1/marketplace/installations/{id}/uploads/{upload_id}");
                let request = Request::post(&path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/octet-stream")
                    .header("content-length", bytes.len().to_string())
                    .body(Body::from(bytes.clone()))?;
                let result = response(app.clone().oneshot(request).await?, &path).await?;
                ensure!(
                    result["upload_id"] == json!(upload_id)
                        && result["integrity_verified"] == true
                        && result["source_verified"] == false
                        && result["installation_ready"] == false,
                    "HTTP upload acknowledgement became a source/install grant"
                );
            }
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "public-source original phase deadline ({current} -> {wanted})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// The private known-peer fixture must model normal discovery: a locally
// created onion address is not an advertised, reachable marketplace peer.
// Wait on the SAME startup owner, then obtain a real verified HTTP search card
// before starting the separate direct-link install flow. No source/install
// authority is supplied by this wait or by the card.
fn first_source_search_request(
    extension: &elixir_marketplace::identity::ExtensionId,
) -> Result<Value> {
    // Match the real browser's canonical request: omitted cursor/default false,
    // not an explicitly present false field rejected by the strict decoder.
    Ok(serde_json::to_value(
        crate::marketplace::search::SearchRequest {
            query: elixir_marketplace::catalog::SearchQuery {
                text: extension.to_string(),
                tags: vec![],
                limit: 1,
            },
            cursor: None,
            prepare_catalog: false,
        },
    )?)
}

#[test]
fn first_verified_search_request_uses_the_normal_canonical_browser_shape() -> Result<()> {
    use crate::marketplace::search::SearchRequest;
    let extension = format!("elx:{}/fixture", "a".repeat(52)).parse()?;
    let request = first_source_search_request(&extension)?;
    let parsed = SearchRequest::parse(&serde_json::to_vec(&request)?)?;
    assert_eq!(parsed.query.text, extension.as_str());
    assert_eq!(parsed.query.limit, 1);
    assert!(parsed.query.tags.is_empty());
    assert!(parsed.cursor.is_none());
    assert!(!parsed.prepare_catalog);
    assert!(request.get("prepare_catalog").is_none());
    assert!(request.get("cursor").is_none());

    let mut malformed = request.clone();
    malformed["prepare_catalog"] = json!(false);
    assert!(SearchRequest::parse(&serde_json::to_vec(&malformed)?).is_err());
    let mut malformed = request;
    malformed["cursor"] = Value::Null;
    assert!(SearchRequest::parse(&serde_json::to_vec(&malformed)?).is_err());
    Ok(())
}

async fn discovered_public_source(
    app: &Router,
    state: &AppState,
    token: &str,
    fixture: &SourceFixture,
) -> Result<()> {
    let until = tokio::time::Instant::now() + Duration::from_secs(600);
    let mut refresh = tokio::time::Instant::now();
    loop {
        ensure!(
            tokio::time::Instant::now() < until,
            "original advertised-peer readiness deadline"
        );
        if let Ok(client) = state.marketplace.client() {
            if let Ok(Ok(status)) = tokio::time::timeout_at(until, client.network_status()).await
                && !status.peers.is_empty()
            {
                break;
            }
            if tokio::time::Instant::now() >= refresh {
                // Read-only discovery refresh uses the existing bounded manager;
                // it cannot register a provider, change roles or renew a grant.
                let _ = tokio::time::timeout_at(until, client.refresh_network()).await;
                refresh = tokio::time::Instant::now() + Duration::from_secs(10);
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let extension = &fixture.publication.release.payload.extension_id;
    let started = http(
        app,
        token,
        "POST",
        "/api/v1/marketplace/search",
        Some(first_source_search_request(extension)?),
    )
    .await?;
    let query: Uuid = serde_json::from_value(started["query_id"].clone())?;
    let until = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let view = tokio::time::timeout_at(
            until,
            http(
                app,
                token,
                "GET",
                &format!("/api/v1/marketplace/search/{query}"),
                None,
            ),
        )
        .await
        .context("original public-source search deadline")??;
        match view["phase"].as_str() {
            Some("checking") => {}
            Some("ready") => {
                let rows = view["results"]
                    .as_array()
                    .context("public search results missing")?;
                ensure!(
                    rows.len() == 1
                        && rows[0]["extension_id"] == json!(extension)
                        && rows[0]["release_id"] == json!(fixture.publication.release_id()?)
                        && rows[0]["support"]["discovery_qualified"] == true
                        && rows[0]["metadata_only"] == true
                        && view["metadata_only"] == true
                        && view["network_complete"] == false,
                    "public-source search did not return the exact independently admitted card"
                );
                eprintln!(
                    "{}",
                    json!({"stage":"actual_private_onion_first_verified_search_card",
                    "verified_cards":1,"real_paid_credentials":0,"public_discovery":false,
                    "installation_authorized":false})
                );
                return Ok(());
            }
            _ => bail!(
                "original public-source search became unavailable: phase={:?}, checked={}, omitted={}",
                view["phase"],
                view["checked"],
                view["omitted"]
            ),
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "original public-source search deadline"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn install(
    app: &Router,
    state: &AppState,
    token: &str,
    fixture: &SourceFixture,
    existing: Option<Uuid>,
    upload_source: bool,
) -> Result<Uuid> {
    install_with_updates(app, state, token, fixture, existing, upload_source, true).await
}

async fn install_with_updates(
    app: &Router,
    state: &AppState,
    token: &str,
    fixture: &SourceFixture,
    existing: Option<Uuid>,
    upload_source: bool,
    automatic_updates: bool,
) -> Result<Uuid> {
    let publication = &fixture.publication;
    let extension = &publication.release.payload.extension_id;
    let targets = target_choices(app, token, extension.as_str(), Duration::from_secs(30)).await;
    if targets.is_err() {
        // Closed scheduling facts only; no provider output, URLs, keys or
        // credentials. Diagnostics must not replace the original error/cleanup.
        let scheduling = state
            .marketplace
            .client()
            .ok()
            .map(|client| client.fixture_preparation_diagnostics()["source"].clone())
            .unwrap_or(Value::Null);
        eprintln!(
            "{}",
            json!({"stage":"original_target_review_wait_failed","scheduling":scheduling})
        );
    }
    let targets = targets?;
    let (target, revision, controls) = if let Some(instance) = existing {
        let row = targets["instances"]
            .as_array()
            .context("instance choices missing")?
            .iter()
            .find(|row| row["instance_id"] == json!(instance))
            .context("original configured instance missing")?;
        ensure!(
            row["pending"] == false,
            "original configured instance still pending"
        );
        (
            json!({"kind":"existing","instance_id":instance}),
            row["expected_revision"].clone(),
            row["controls"].clone(),
        )
    } else {
        (
            json!({"kind":"new","name":"Public qualification source"}),
            targets["new_instance"]["expected_revision"].clone(),
            targets["new_instance"]["controls"].clone(),
        )
    };
    let request = json!({"schema_version":1,"request_id":Uuid::new_v4(),"publication":publication,
        "accepted_reference":Digest::of(publication.to_bytes()?),"accept_unqualified":true,"upload_source":upload_source,
        "target":target,"expected_revision":revision,"mutation_id":Uuid::new_v4(),"controls":controls});
    let started = http(
        app,
        token,
        "POST",
        "/api/v1/marketplace/installations/direct",
        Some(request.clone()),
    )
    .await?;
    let id: Uuid = serde_json::from_value(started["operation_id"].clone())?;
    ensure!(
        http(
            app,
            token,
            "POST",
            "/api/v1/marketplace/installations/direct",
            Some(request)
        )
        .await?["operation_id"]
            == json!(id),
        "identical direct start did not retain the original command"
    );
    let review = phase(
        app,
        token,
        id,
        "awaiting_approval",
        upload_source.then_some(fixture),
    )
    .await?;
    let instance =
        activate_direct_review(app, state, token, id, &review, existing, automatic_updates).await?;
    eprintln!(
        "{}",
        json!({"stage":"actual_public_source_http_activation","version":publication.release.payload.version,
        "source_uploads":if upload_source {2} else {0},"peer_downloads":!upload_source,
        "paid_credentials":0,"discovery_qualified":false})
    );
    Ok(instance)
}

async fn activate_direct_review(
    app: &Router,
    state: &AppState,
    token: &str,
    id: Uuid,
    review: &Value,
    existing: Option<Uuid>,
    automatic_updates: bool,
) -> Result<Uuid> {
    ensure!(
        review["review_id"].is_string() && review["approval_revision"].is_i64(),
        "native source equality did not produce a real review"
    );
    action(app, token, id, json!({"action":"approve","review_id":review["review_id"],
        "expected_revision":review["approval_revision"],"mutation_id":Uuid::new_v4(),"automatic_updates":automatic_updates})).await?;
    phase(app, token, id, "awaiting_setup", None).await?;
    action(
        app,
        token,
        id,
        json!({"action":"setup","mutation_id":Uuid::new_v4(),"credentials":[]}),
    )
    .await?;
    phase(app, token, id, "awaiting_commit", None).await?;
    action(
        app,
        token,
        id,
        json!({"action":"activate","mutation_id":Uuid::new_v4()}),
    )
    .await?;
    let active = phase(app, token, id, "activated", None).await?;
    state
        .marketplace
        .fixture_original_installation()
        .fixture_join_original(id)
        .await?;
    let instance: Uuid = serde_json::from_value(active["configured_instance"].clone())?;
    ensure!(
        existing.is_none_or(|original| instance == original),
        "manual update replaced the configured instance"
    );
    Ok(instance)
}

async fn provider(
    state: &AppState,
    database: &Database,
    user: Uuid,
    instance: Uuid,
    version: &str,
) -> Result<()> {
    use crate::live::{
        config::LiveConfig,
        contract::{ProviderOperation, ProviderRequestContext},
        diagnostics::LiveRedactor,
        provider::LiveProviderClient,
    };
    let descriptors = state.marketplace.source_providers(database).await?;
    ensure!(
        descriptors.len() == 1
            && descriptors[0].instance == instance
            && descriptors[0].version == version,
        "actual registered source provider differs"
    );
    let live = LiveProviderClient::new_with_marketplace(
        database.clone(),
        state.marketplace.clone(),
        LiveConfig::default().providers,
        Arc::new(LiveRedactor::default()),
    )?;
    let providers = live.directory().discover().await?;
    ensure!(
        providers.len() == 1 && providers[0].instance_id == instance,
        "Live provider directory differs"
    );
    ensure!(
        providers[0]
            .operation_url(ProviderOperation::Catalogs)
            .is_err(),
        "source provider gained an HTTP fallback"
    );
    let context = ProviderRequestContext {
        locale: "en-US".into(),
        timezone: "UTC".into(),
        now: chrono::Utc::now(),
    };
    let catalogs = live
        .catalogs(
            &providers[0],
            user,
            &context,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    ensure!(
        catalogs.catalogs.len() == 1
            && catalogs.catalogs[0].name == format!("Qualification {version}"),
        "native provider executed different source bytes"
    );
    eprintln!(
        "{}",
        json!({"stage":"actual_public_source_live_catalogs","version":version,"default_product_timeouts":true})
    );
    Ok(())
}

async fn unattended_update(
    app: &Router,
    state: &AppState,
    database: &Database,
    principal: &crate::http::auth::CurrentPrincipal,
    token: &str,
    worker: &str,
    allocation: &Allocation,
    instance: Uuid,
    updated: &SourceFixture,
) -> Result<()> {
    let extension = &updated.publication.release.payload.extension_id;
    let grant_query = "SELECT revision,review_json,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants WHERE extension_id=$1";
    let before: (i64, String, i64) = sqlx::query_as(grant_query)
        .bind(extension.as_str())
        .fetch_one(&database.pool)
        .await?;
    ensure!(
        before.2 == 1,
        "automatic test requires actual saved owner opt-in"
    );
    let request = docker(&[
        "exec",
        worker,
        "/usr/local/bin/node",
        "/opt/elixir-marketplace/test/private-peer-control.mjs",
        "advance",
    ])
    .await?;
    let request = elixir_marketplace::canonical::parse(request.as_bytes(), 1024)?;
    ensure!(
        request["fixture_only"] == true && request["advance_requested"] == true,
        "private source advance request was not acknowledged"
    );
    let until = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let snapshot = docker(&[
            "exec",
            worker,
            "/usr/local/bin/node",
            "/opt/elixir-marketplace/test/private-peer-control.mjs",
            "snapshot",
        ])
        .await?;
        let snapshot = elixir_marketplace::canonical::parse(snapshot.as_bytes(), 1024)?;
        if snapshot["fixture_only"] == true && snapshot["head_version"] == "1.1.0" {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "private signed head did not advance"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Normal logout, then prove the old bearer cannot read the public management
    // route. The scheduler receives neither that bearer nor a replacement login.
    http(app, token, "POST", "/api/v1/auth/logout", Some(json!({}))).await?;
    let denied = app
        .clone()
        .oneshot(
            Request::get("/api/v1/marketplace/installations/current")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    ensure!(
        denied.status() == axum::http::StatusCode::UNAUTHORIZED,
        "logged-out bearer unexpectedly retained management access"
    );
    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_sessions")
        .fetch_one(&database.pool)
        .await?;
    let mut runtime = state
        .marketplace
        .client()?
        .installed_configuration(allocation.source.clone(), allocation.retention)?;
    // Match normal installed_startup::configuration and the preceding manual
    // Host: retain the SAME dormant local transport owner, not None or a newly
    // created client. This does not grant any local/public HTTP permission.
    runtime.local_http = state.marketplace.local_http_client();
    state.marketplace.begin_automatic_updates(
        database.clone(),
        state.secrets.as_ref().clone(),
        runtime,
    )?;
    let service = state.marketplace.fixture_original_installation();
    let expected = updated.publication.release_id()?;
    let began = tokio::time::Instant::now();
    let mut next_report = Duration::from_secs(15);
    loop {
        // Internal read-only observation of the ORIGINAL task, not HTTP access
        // or a new principal/selection/approval. All effects belong to the real
        // startup scheduler and its independently authenticated standing grant.
        let current = service.current_for(principal)?;
        let view = current
            .map(|id| service.view_for(principal, id))
            .transpose()?;
        if let Some(view) = &view {
            if view.release_id == expected {
                ensure!(
                    !matches!(
                        view.phase,
                        crate::marketplace::application::Phase::Stopped
                            | crate::marketplace::application::Phase::Cancelled
                            | crate::marketplace::application::Phase::RecoveryRequired
                    ),
                    "original automatic operation reached {:?}",
                    view.phase
                );
                if view.phase == crate::marketplace::application::Phase::Activated {
                    ensure!(
                        view.configured_instance == Some(instance),
                        "automatic update changed configured instance"
                    );
                    service.fixture_join_original(view.operation_id).await?;
                    break;
                }
            }
        }
        if began.elapsed() >= next_report {
            eprintln!(
                "{}",
                json!({"stage":"private_unattended_update_wait",
                "elapsed_ms":began.elapsed().as_millis(),"phase":view.as_ref().map(|view|view.phase),
                "readiness":state.marketplace.client()?.fixture_preparation_diagnostics(),
                "scheduler":state.marketplace.fixture_original_update_diagnostics()?})
            );
            next_report = began.elapsed() + Duration::from_secs(15);
        }
        ensure!(
            began.elapsed() < Duration::from_secs(600),
            "original automatic update did not activate within its fixture bound"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let after: (i64, String, i64) = sqlx::query_as(grant_query)
        .bind(extension.as_str())
        .fetch_one(&database.pool)
        .await?;
    ensure!(
        before == after,
        "automatic update ratcheted the original permission grant"
    );
    ensure!(
        sessions
            == sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM account_sessions")
                .fetch_one(&database.pool)
                .await?,
        "automatic update manufactured an account session"
    );
    provider(state, database, principal.user_id, instance, "1.1.0").await?;
    eprintln!(
        "{}",
        json!({"stage":"actual_private_onion_unattended_update",
        "logged_out_bearer_rejected":true,"new_user_actions":0,"grant_unchanged":true,
        "same_instance":true,"real_paid_credentials":0,"public_discovery":false})
    );
    Ok(())
}

fn write_profile(root: &Path, image: &str, allocation: &Allocation) -> Result<()> {
    let a = allocation;
    let value = json!({"schema_version":1,"worker_image":image,
        "installations":{"entries":a.installations.entries,"bytes":a.installations.bytes},
        "source":{"memory_bytes":a.source.memory_bytes,"cpu_millicores":a.source.cpu_millicores,
        "disk_bytes":a.source.disk_bytes,"input_bytes":a.source.input_bytes,"output_bytes":a.source.output_bytes,"wall_seconds":a.source.wall_seconds},
        "retention":{"bytes":a.retention.bytes,"releases":a.retention.releases},"execution":a.execution});
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root.join("installed-startup.json"))?;
    file.write_all(&serde_json::to_vec(&value)?)?;
    file.sync_all()?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn write_network_profile(rig: &Rig, guard: &str, worker: &str) -> Result<PathBuf> {
    write_network_profile_with_network(
        rig,
        guard,
        worker,
        config(Digest::of(Uuid::new_v4().as_bytes()), false)?,
    )
}

fn write_network_profile_with_network(
    rig: &Rig,
    guard: &str,
    worker: &str,
    network: elixir_marketplace::worker_network::NetworkConfig,
) -> Result<PathBuf> {
    write_network_profile_with_provider(rig, guard, worker, network, false)
}

fn write_network_profile_with_provider(
    rig: &Rig,
    guard: &str,
    worker: &str,
    network: elixir_marketplace::worker_network::NetworkConfig,
    first_run: bool,
) -> Result<PathBuf> {
    let issuer = PublicKey::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[3; 32])
            .verifying_key()
            .to_bytes(),
    )?;
    let provider = if first_run {
        json!({"type":"native_warp_setup","image":rig.image})
    } else {
        json!({"type":"native_warp","image":rig.image,
            "consent_version":elixir_marketplace::warp_provider::CONSENT_VERSION,
            "enrollment":rig.scope.enrollment})
    };
    let value = json!({"schema_version":1,"guard_image":guard,"worker_image":worker,
        "issuer_keys":[issuer],"network":network,"provider":provider});
    let path = rig.root.path().canonicalize()?.join("network-startup.json");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)?;
    file.write_all(&serde_json::to_vec(&value)?)?;
    file.sync_all()?;
    std::fs::File::open(rig.root.path())?.sync_all()?;
    Ok(path)
}

#[tokio::test]
#[ignore = "approved provider/public test source through normal auth/HTTP/native install+manual update+offline restart; no paid eligibility or public package hosting"]
async fn approved_public_source_installs_updates_and_restarts_through_actual_application()
-> Result<()> {
    qualify_installation(false, false).await
}

#[tokio::test]
#[ignore = "approved protected private known-peer onion package/proof download + public source + normal authenticated install/manual update/offline restart; discovery/adoption fixture only, no public package seeding"]
async fn private_onion_source_installs_updates_and_restarts_through_actual_application()
-> Result<()> {
    qualify_installation(true, false).await
}

#[tokio::test]
#[ignore = "approved protected private onion download/public source/normal install, then real startup scheduler after normal logout, unchanged grants and executed update/offline restart; discovery/adoption synthetic"]
async fn private_onion_source_updates_automatically_after_logout_and_restarts() -> Result<()> {
    qualify_installation(true, true).await
}

async fn qualify_installation(private_peer: bool, automatic: bool) -> Result<()> {
    ensure!(
        [
            "ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST",
            "ELIXIR_MARKETPLACE_WAN_LIVE_TEST",
            "ELIXIR_MARKETPLACE_PUBLIC_SOURCE_LIVE_TEST",
            "ELIXIR_MARKETPLACE_PUBLIC_INSTALL_LIVE_TEST"
        ]
        .iter()
        .all(|name| std::env::var(name).as_deref() == Ok("1")),
        "explicit approved public-install test consent required"
    );
    if private_peer {
        ensure!(
            std::env::var("ELIXIR_MARKETPLACE_PRIVATE_PEER_LIVE_TEST").as_deref() == Ok("1"),
            "explicit private-only known-peer qualification required"
        );
    }
    if automatic {
        ensure!(
            private_peer
                && std::env::var("ELIXIR_MARKETPLACE_PRIVATE_PEER_AUTOMATIC_LIVE_TEST").as_deref()
                    == Ok("1"),
            "explicit private automatic-update qualification required"
        );
    }
    // This exact ignored/live test already requires explicit private consent.
    // Preserve bounded original runtime failures that recovery correctly does
    // not confuse with cleanup. Never enable private error chains by default
    // in the shipped application or expose them through a status response.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn,elixir_server::marketplace::lifecycle=debug")
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
    // An explicit diagnostic-only run may stop on the first real startup
    // fault, after its original joined failure was logged. This cannot pass
    // the full install/update case or change production retries/authority.
    let first_fault_diagnostic =
        std::env::var("ELIXIR_MARKETPLACE_PRIVATE_STARTUP_DIAGNOSTIC").as_deref() == Ok("1");
    ensure!(
        !first_fault_diagnostic || (private_peer && !automatic),
        "startup diagnostic is private/manual only"
    );
    let loaded = configured_fixtures()?; // Exact source/package/Git/license validation before registration.
    let provider_image = image()?;
    let guard_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    if private_peer {
        let marker = docker(&[
            "image",
            "inspect",
            &worker_image,
            "--format",
            "{{index .Config.Labels \"elixir.marketplace.worker.private_peer_fixture\"}}",
        ])
        .await?;
        ensure!(
            marker.trim() == "1",
            "private peer fixture image required before enrollment"
        );
    }
    if automatic {
        let marker = docker(&[
            "image",
            "inspect",
            &worker_image,
            "--format",
            "{{index .Config.Labels \"elixir.marketplace.worker.private_peer_updates_fixture\"}}",
        ])
        .await?;
        ensure!(
            marker.trim() == "1",
            "private signed-head control image required before enrollment"
        );
    }
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let mut rig = Rig::new_layout(runtime, &provider_image, true).await?;
    let root = rig.root.path().canonicalize()?;
    let url = format!("sqlite://{}", root.join("wan.sqlite").display());
    // Discard the original router immediately. A named `_public` binding would
    // retain AppState/controller OS locks until this parent test returns,
    // incorrectly overlapping the separate-process restart below.
    let (mut state, _, principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture(
            rig.database.clone(),
            &root,
            &url,
        )
        .await?;
    let mut allocation = crate::marketplace::application::fixture_allocation();
    allocation.source.cpu_millicores = 200;
    // Explicit protected-WAN fixture, not the short offline unit profile:
    // cold bootstrap/circuit establishment must fit inside the original job.
    allocation.source.wall_seconds = 600;
    allocation.validate()?;
    write_profile(&root, &worker_image, &allocation)?;
    let mut settings = state.settings.as_ref().clone();
    settings.marketplace.startup_file =
        Some(write_network_profile(&rig, &guard_image, &worker_image)?);
    settings.marketplace.installed_startup_file = Some(root.join("installed-startup.json"));
    settings.marketplace.installation_management = true;
    state.settings = Arc::new(settings);
    // Use the same actual encryption owner as normal application startup and
    // preserve it for exact-identity test revocation. No downloader credentials.
    rig.secrets = state.secrets.clone();
    let app = crate::http::router(state.clone());
    state
        .marketplace
        .configure_installation(allocation.clone())?;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut startup = None;
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        save_off(&app, &token).await?;
        rig.enrollment_started = true;
        rig.application = Some(state.marketplace.clone());
        startup = Some(crate::marketplace::startup::spawn_configured(&state, shutdown.clone())
            .context("actual configured native startup owner missing")?);
        let until = tokio::time::Instant::now() + Duration::from_secs(600);
        loop {
            // A retained client may legitimately still be Starting/Applying.
            // Observe the SAME production availability predicate used by the
            // normal target route; waiting is not a new start, grant or retry.
            if state.marketplace.installation_review_available() { break; }
            if first_fault_diagnostic && state.marketplace.snapshot().phase == crate::marketplace::service::RuntimePhase::Failed {
                anyhow::bail!("private diagnostic stopped on original startup failure; installation remains unqualified");
            }
            ensure!(!startup.as_ref().unwrap().is_finished(), "original native startup/recovery owner terminated");
            ensure!(tokio::time::Instant::now() < until, "actual native startup readiness deadline");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        source_ready(&state.marketplace.client()?).await?;
        ensure!(state.marketplace.installation_review_available(), "normal protected review availability missing");
        let original_worker = rig.runtime.get_container_handle(&format!("elixir-marketplace-worker-{}",rig.deployment))
            .await?.context("original owned native worker missing")?.id;
        if private_peer {
            discovered_public_source(&app, &state, &token, &loaded[0]).await?;
        }
        let mut instance = None;
        for fixture in loaded.iter().take(if automatic {1} else {2}) {
            let configured = install(&app, &state, &token, fixture, instance, !private_peer).await?;
            instance = Some(configured);
            provider(&state, &rig.database, principal.user_id, configured, &fixture.publication.release.payload.version).await?;
        }
        if automatic {
            unattended_update(&app, &state, &rig.database, &principal, &token, &original_worker,
                &allocation, instance.context("original automatic target missing")?, &loaded[1]).await?;
        }
        if private_peer {
            // Read counts from the exact original worker only. Counts are not
            // source/install authority: normal consumer hash, Git/license,
            // permission, activation and executed-provider checks above are.
            let stats = docker(&["exec", &original_worker, "/usr/local/bin/node", "--input-type=module", "-e",
                "import{readFileSync,lstatSync}from'node:fs';const p='/tmp/private-peer-stats.json',s=lstatSync(p);if(!s.isFile()||s.isSymbolicLink()||s.size>1024)process.exit(1);process.stdout.write(readFileSync(p));"])
                .await?;
            let stats: Value = elixir_marketplace::canonical::parse(stats.as_bytes(), 1024)?;
            ensure!(stats["schema_version"] == 1 && stats["fixture_only"] == true
                && stats["public_discovery"] == false, "private peer transport fixture differs");
            for purpose in ["package_v1", "proof_v1", "package_v2", "proof_v2"] {
                ensure!(stats["blob_opens"][purpose].as_u64().is_some_and(|count| count >= 1),
                    "original private peer did not open {purpose}");
            }
        }
        let extension = &loaded[1].publication.release.payload.extension_id;
        let automatic_opt_in:i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants WHERE extension_id=$1")
            .bind(extension.as_str()).fetch_one(&rig.database.pool).await?;
        ensure!(automatic_opt_in == 1, "owner opt-in was not saved");
        let roles = state.marketplace.client()?.snapshot().applied_roles.context("actual final roles missing")?;
        ensure!(roles.consumer && !roles.catalog && !roles.routing, "source installation silently became infrastructure");
        Ok::<(), anyhow::Error>(())
    })).catch_unwind().await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("Public installation operation failed before cleanup: {error:#}");
        if let Ok(Some(failure)) = state.marketplace.fixture_original_preparation_failure() {
            eprintln!("Original private source preparation failed: {failure:#}");
        }
        if let Ok(Some(failure)) = state.marketplace.fixture_original_update_failure() {
            eprintln!("Original private automatic-update check failed: {failure:#}");
        }
        if let Ok((original, cleanup)) = state.marketplace.fixture_original_installation_failures()
        {
            if let Some(failure) = original {
                eprintln!("Original private installation failed: {failure:#}");
            }
            if let Some(failure) = cleanup {
                eprintln!("Original private installation cleanup failed: {failure:#}");
            }
        }
    }
    shutdown.cancel();
    let startup_cleanup = match startup {
        Some(task) => task
            .await
            .context("original native startup join")
            .and_then(|result| result),
        None => Ok(()),
    };
    let cleanup = rig.close().await.and(startup_cleanup);
    if let Err(error) = &cleanup {
        eprintln!("Public installation original cleanup failed: {error:#}");
    }
    rig.database.pool.close().await;
    drop(app);
    drop(state);
    drop(principal);
    drop(token);
    if !outcome.as_ref().is_ok_and(|value| value.is_ok()) || cleanup.is_err() {
        rig.root.preserve();
    }
    cleanup?;
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    // A separate process sees only persisted operator configuration and state.
    let restarted = tokio::process::Command::new(std::env::current_exe()?)
        .arg("marketplace::provider::owner::tests::guard::wan::source::installation::approved_public_source_offline_restart_child")
        .args(["--ignored","--exact","--nocapture","--test-threads=1"])
        .env("ELIXIR_MARKETPLACE_PUBLIC_INSTALL_RESTART_ROOT", &root).kill_on_drop(false).status().await;
    if !restarted.as_ref().is_ok_and(|status| status.success()) {
        rig.root.preserve();
    }
    ensure!(
        restarted?.success(),
        "original public-source offline restart child failed"
    );
    eprintln!(
        "{}",
        json!({"actual_public_source_install_and_manual_update":!automatic,"offline_restart":true,
        "automatic_opt_in_saved":true,"same_identity_revoked":true,
        "peer_payload_downloads":private_peer,"unattended_update_after_logout":automatic,
        "manual_update":!automatic,"public_discovery":false,"real_paid_credentials":0,
        "excludes":"public peer discovery, real installed sharing grants, UI, playback, scale"})
    );
    Ok(())
}

#[tokio::test]
#[ignore = "child only of actual public-source application test; existing database/private profile, no signup/source/protection factories"]
async fn approved_public_source_offline_restart_child() -> Result<()> {
    let root = PathBuf::from(std::env::var(
        "ELIXIR_MARKETPLACE_PUBLIC_INSTALL_RESTART_ROOT",
    )?);
    ensure!(
        root.is_absolute() && root.canonicalize()? == root,
        "canonical existing fixture root required"
    );
    let fixture = elixir_marketplace::canonical::parse(
        &bounded(&root.join("provider-fixture.json"), 8192)?,
        8192,
    )?;
    ensure!(
        fixture["fixture"] == "native-public-source-install",
        "original public-install fixture required"
    );
    let url = format!("sqlite://{}", root.join("wan.sqlite").display());
    let database = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_sessions")
        .fetch_one(&database.pool)
        .await?;
    let mut settings = crate::http::test_settings_with_db();
    settings.database.url = url;
    settings.extensions.storage_root = root.join("extensions").display().to_string();
    settings.extensions.bundled_dir = root.join("bundled").display().to_string();
    settings.marketplace.installed_startup_file = Some(root.join("installed-startup.json"));
    let state = AppState::new(
        settings.clone(),
        database.clone(),
        crate::auth::AuthService::new(settings.auth.clone())?,
        crate::extensions::ExtensionManager::new(),
        crate::metadata::MetadataService::new(settings.metadata.clone())?,
        crate::library::LinkerService::new(settings.classifier.clone())?,
        crate::http::test_artwork_service(&settings)?,
        SecretsManager::from_settings(&settings)?,
    );
    let shutdown = tokio_util::sync::CancellationToken::new();
    let outcome = async {
        crate::marketplace::installed_startup::spawn_configured(&state, shutdown.clone())
            .context("actual offline installed startup missing")?
            .await??;
        ensure!(
            state.marketplace.client().is_err(),
            "offline restoration acquired marketplace networking"
        );
        let descriptors = state.marketplace.source_providers(&database).await?;
        ensure!(
            descriptors.len() == 1 && descriptors[0].version == "1.1.0",
            "persisted updated provider missing"
        );
        let user: String = sqlx::query_scalar(
            "SELECT approved_by_user_id FROM marketplace_extension_grants WHERE extension_id=$1",
        )
        .bind(descriptors[0].extension.as_str())
        .fetch_one(&database.pool)
        .await?;
        provider(
            &state,
            &database,
            Uuid::parse_str(&user)?,
            descriptors[0].instance,
            "1.1.0",
        )
        .await?;
        ensure!(
            sessions
                == sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM account_sessions")
                    .fetch_one(&database.pool)
                    .await?,
            "offline restart created an account session"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    shutdown.cancel();
    let cleanup = state.marketplace.shutdown().await;
    database.pool.close().await;
    if let Err(error) = &outcome {
        eprintln!("Original offline installed startup failed: {error:#}");
    }
    if let Err(error) = &cleanup {
        eprintln!("Original offline installed cleanup failed: {error:#}");
    }
    cleanup?;
    outcome?;
    eprintln!(
        "actual public-source updated provider restored and invoked offline through operator startup"
    );
    Ok(())
}
