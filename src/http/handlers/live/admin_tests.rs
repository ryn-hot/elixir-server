use std::sync::Arc;

use anyhow::Result;
use axum::{
    Router,
    body::{self, Body},
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use crate::{
    artwork::ArtworkService,
    auth::{AuthService, home_profiles::HomeRole},
    authz::{AuthorizationRepository, Capability},
    config::{
        AuthConfig, ClassifierConfig, DatabaseConfig, LibraryConfig, MediaInteractionsConfig,
        RunEnvironment, SecretsConfig, ServerConfig, Settings, TelemetryConfig,
    },
    db::Database,
    extensions::ExtensionManager,
    http::router,
    library::LinkerService,
    live::{
        config::LiveConfig,
        crypto::SecretBytes,
        service::LiveService,
        session::{
            DeliveryMode, IdempotencyRequest, NewSession, SessionOwner, SessionProtocol,
            SessionState,
        },
    },
    metadata::MetadataService,
    secrets::SecretsManager,
    state::AppState,
};

#[tokio::test]
async fn o11_real_router_destination_policy_enforces_owner_csrf_cas_audit_and_revocation()
-> Result<()> {
    let settings = settings();
    let database = Database::connect(&settings.database).await?;
    database.run_migrations().await?;
    let pool = database.pool.clone();
    let provider_id = seed_provider(&database).await?;
    let mut state = AppState::new(
        settings.clone(),
        database,
        AuthService::new(settings.auth.clone())?,
        ExtensionManager::new(),
        MetadataService::new(settings.metadata.clone())?,
        LinkerService::new(settings.classifier.clone())?,
        ArtworkService::new(
            settings.library.artwork_cache_dir.clone(),
            settings.metadata.request_timeout_seconds,
        )?,
        SecretsManager::from_settings(&settings)?,
    );
    seed_live_rotation_keys(&pool, &state.secrets).await?;
    state.live = Arc::new(LiveService::new_for_test(
        settings.live.clone(),
        settings.environment,
        pool.clone(),
        state.secrets.clone(),
    ));
    state.live.initialize().await?;
    let app = router(state.clone());
    let route = format!("/api/v1/live/admin/providers/{provider_id}/destination-rules");

    let (status, unauthenticated) =
        response_json(request(&app, "GET", &route, &[], None).await?).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauthenticated["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

    let (status, signup) = response_json(
        request(
            &app,
            "POST",
            "/api/v1/auth/signup",
            &[],
            Some(json!({
                "email": format!("o11-owner-{}@example.invalid", Uuid::new_v4()),
                "password": "correct horse battery staple"
            })),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let access = signup["access_token"].as_str().unwrap().to_string();
    let csrf = signup["csrf_token"].as_str().unwrap().to_string();
    let user_id = Uuid::parse_str(
        &sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM homes WHERE id = $1")
            .bind(signup["home_id"].as_str().unwrap())
            .fetch_one(&pool)
            .await?,
    )?;
    let home_id = Uuid::parse_str(signup["home_id"].as_str().unwrap())?;
    let profile_id = Uuid::parse_str(signup["profile_id"].as_str().unwrap())?;
    let account_session_id = Uuid::parse_str(signup["session_id"].as_str().unwrap())?;

    let query_route = format!("{route}?access_token={access}");
    let (status, query_auth) =
        response_json(request(&app, "GET", &query_route, &[], None).await?).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(query_auth["errors"][0]["code"], "LIVE_AUTH_REQUIRED");

    let admin_access = add_capable_admin(&state, &pool, home_id, user_id).await?;
    let (status, non_owner) = response_json(
        request(
            &app,
            "GET",
            &route,
            &[("authorization", format!("Bearer {admin_access}"))],
            None,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(non_owner["errors"][0]["code"], "LIVE_CAPABILITY_REQUIRED");

    let grant_route = format!("/api/v1/live/admin/providers/{provider_id}/grants/{profile_id}");
    let admin_bearer = [("authorization", format!("Bearer {admin_access}"))];
    let (status, granted) = response_json(
        request(
            &app,
            "PUT",
            &grant_route,
            &admin_bearer,
            Some(json!({
                "canBrowse": true,
                "canPlay": true,
                "expectedRevision": 1
            })),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(granted["data"]["revision"], 2);
    assert_eq!(granted["data"]["audit"]["action"], "provider_grant_set");
    assert!(granted["data"].get("revocationEventId").is_none());
    let (status, revoked) = response_json(
        request(
            &app,
            "DELETE",
            &grant_route,
            &admin_bearer,
            Some(json!({"expectedRevision": 2})),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(revoked["data"]["revision"], 3);
    assert_eq!(revoked["data"]["audit"]["action"], "provider_grant_revoke");

    let rule = json!({
        "expectedProviderRevision": 1,
        "scheme": "HTTPS",
        "host": "Sports.Example.",
        "port": 443,
        "path": "/league/./round/../live.m3u8",
        "networkScope": "public",
        "allowFetch": true,
        "allowCredentials": false,
        "allowClientDisclosure": true
    });
    let missing_csrf = [("cookie", format!("elixir_ui_token={access}"))];
    let (status, csrf_error) =
        response_json(request(&app, "POST", &route, &missing_csrf, Some(rule.clone())).await?)
            .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(csrf_error["errors"][0]["code"], "LIVE_CSRF_REQUIRED");

    let cookie = [
        ("cookie", format!("elixir_ui_token={access}")),
        ("origin", "http://127.0.0.1:44301".to_string()),
        ("x-elixir-csrf", csrf),
    ];
    let forbidden_before: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM media_files),
            (SELECT COUNT(*) FROM acquisition_subscriptions),
            (SELECT COUNT(*) FROM playback_sessions)",
    )
    .fetch_one(&pool)
    .await?;
    let (status, created) =
        response_json(request(&app, "POST", &route, &cookie, Some(rule.clone())).await?).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["data"]["rule"]["host"], "sports.example");
    assert_eq!(created["data"]["rule"]["path"], "/league/live.m3u8");
    assert!(created["data"].get("providerRevision").is_none());
    assert!(created["data"].get("terminateProviderSessions").is_none());
    assert_eq!(
        created["data"]["audit"]["recordHash"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    let rule_id = Uuid::parse_str(created["data"]["ruleId"].as_str().unwrap())?;

    let bearer = [("authorization", format!("Bearer {access}"))];
    let (status, stale) =
        response_json(request(&app, "POST", &route, &bearer, Some(rule.clone())).await?).await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale["errors"][0]["code"], "LIVE_REVISION_CONFLICT");

    let now = Utc::now();
    let fence = state.live.control_fencing_token().await.unwrap();
    let session = state
        .live
        .session_repository()
        .unwrap()
        .create(
            NewSession {
                owner: SessionOwner {
                    user_id,
                    home_id,
                    profile_id,
                    account_session_id,
                    provider_id,
                },
                item_key: SecretBytes::from_utf8("o11-item".to_string()),
                stream_option_key: SecretBytes::from_utf8("o11-stream".to_string()),
                item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                descriptor: SecretBytes::from_utf8("{}".to_string()),
                delivery_mode: DeliveryMode::ClientDirect,
                protocol: SessionProtocol::Hls,
                source_index: 0,
                control_fencing_token: fence,
                now,
            },
            None,
        )
        .await?;

    let detail_route = format!("{route}/{rule_id}");
    let update = json!({
        "expectedRevision": 1,
        "scheme": "https",
        "host": "replacement.example",
        "port": 8443,
        "path": "/live.m3u8",
        "networkScope": "public",
        "allowFetch": true,
        "allowCredentials": false,
        "allowClientDisclosure": false
    });
    let (status, updated) =
        response_json(request(&app, "PUT", &detail_route, &bearer, Some(update)).await?).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["data"]["revision"], 2);
    let terminated = state
        .live
        .session_repository()
        .unwrap()
        .get_owned(session.session.owner, session.session.id)
        .await?
        .expect("policy-revoked session diagnostic row");
    assert_eq!(terminated.state, SessionState::Failed);
    assert_eq!(
        terminated.error_code.as_deref(),
        Some("LIVE_DESTINATION_POLICY_CHANGED")
    );

    let (status, deleted) = response_json(
        request(
            &app,
            "DELETE",
            &detail_route,
            &cookie,
            Some(json!({"expectedRevision": 2})),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["data"]["deleted"], true);
    assert!(deleted["data"]["rule"].is_null());

    let admin_session = state
        .live
        .session_repository()
        .unwrap()
        .create(
            NewSession {
                owner: session.session.owner,
                item_key: SecretBytes::from_utf8("o11-admin-item".to_string()),
                stream_option_key: SecretBytes::from_utf8("o11-admin-stream".to_string()),
                item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                descriptor: SecretBytes::from_utf8("{}".to_string()),
                delivery_mode: DeliveryMode::ClientDirect,
                protocol: SessionProtocol::Hls,
                source_index: 0,
                control_fencing_token: fence,
                now: Utc::now(),
            },
            Some(IdempotencyRequest {
                key: SecretBytes::from_utf8("o11-admin-idempotency".to_string()),
                request_identity: SecretBytes::from_utf8("o11-admin-request".to_string()),
            }),
        )
        .await?;
    let admin_sessions_route = "/api/v1/live/admin/sessions";
    let (status, sessions) =
        response_json(request(&app, "GET", admin_sessions_route, &admin_bearer, None).await?)
            .await?;
    assert_eq!(status, StatusCode::OK);
    let listed = sessions["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|candidate| candidate["sessionId"] == admin_session.session.id.to_string())
        .expect("active session in bounded administrative list");
    assert_eq!(listed["state"], "resolving");
    assert_eq!(listed["revision"], 1);
    for forbidden in [
        "userId",
        "accountSessionId",
        "itemKey",
        "streamOptionKey",
        "descriptor",
        "tokenHash",
        "egressBindingId",
        "remuxJobId",
        "errorDetailRedacted",
    ] {
        assert!(listed.get(forbidden).is_none(), "leaked {forbidden}");
    }

    sqlx::query(
        "UPDATE live_playback_sessions
         SET error_detail_redacted = $1 WHERE id = $2",
    )
    .bind(
        "Authorization: Bearer diagnostics-secret-value \
         source=https://origin.invalid/live.m3u8?token=query-secret",
    )
    .bind(admin_session.session.id.to_string())
    .execute(&pool)
    .await?;
    let diagnostics_route = format!(
        "/api/v1/live/admin/sessions/{}/diagnostics",
        admin_session.session.id
    );
    assert!(
        state
            .live
            .session_repository()
            .unwrap()
            .get_for_home(Uuid::new_v4(), admin_session.session.id)
            .await?
            .is_none(),
        "cross-home diagnostics lookup must be indistinguishable from absence"
    );
    let (status, unauthenticated_diagnostics) =
        response_json(request(&app, "GET", &diagnostics_route, &[], None).await?).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        unauthenticated_diagnostics["errors"][0]["code"],
        "LIVE_AUTH_REQUIRED"
    );
    let (status, diagnostics_query_rejected) = response_json(
        request(
            &app,
            "GET",
            &format!("{diagnostics_route}?include=descriptor"),
            &admin_bearer,
            None,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        diagnostics_query_rejected["errors"][0]["code"],
        "LIVE_INVALID_REQUEST"
    );
    let diagnostics_response =
        request(&app, "GET", &diagnostics_route, &admin_bearer, None).await?;
    assert_eq!(
        diagnostics_response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let (status, diagnostics) = response_json(diagnostics_response).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(diagnostics["data"]["schemaVersion"], 1);
    assert_eq!(
        diagnostics["data"]["session"]["sessionId"],
        admin_session.session.id.to_string()
    );
    assert_eq!(
        diagnostics["data"]["planner"]["descriptorStatus"],
        "invalid"
    );
    assert_eq!(diagnostics["data"]["partial"], true);
    assert_eq!(diagnostics["meta"]["partial"], true);
    assert_eq!(diagnostics["data"]["cleanup"]["terminal"], false);
    assert_eq!(diagnostics["data"]["cleanup"]["complete"], false);
    assert_eq!(diagnostics["data"]["upstream"]["scope"], "server_aggregate");
    let diagnostics_text = diagnostics.to_string();
    for secret in [
        "diagnostics-secret-value",
        "query-secret",
        "https://origin.invalid",
    ] {
        assert!(!diagnostics_text.contains(secret), "leaked {secret}");
    }
    for forbidden in [
        "accountSessionId",
        "itemKey",
        "streamOptionKey",
        "descriptor",
        "playbackUrl",
        "tokenHash",
        "requestHeaders",
        "cookies",
        "refreshHandle",
        "policyId",
        "expectedEgressIps",
    ] {
        assert_json_key_absent(&diagnostics, forbidden);
    }

    let key_state_route = "/api/v1/live/admin/keys";
    let (status, initial_keys) =
        response_json(request(&app, "GET", key_state_route, &admin_bearer, None).await?).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(initial_keys["data"]["revision"], 1);
    assert_eq!(
        initial_keys["data"]["envelopePrimaryKeyId"],
        "live-envelope-1"
    );
    assert_eq!(
        initial_keys["data"]["tokenHashPrimaryKeyId"],
        "live-token-hash-1"
    );
    assert_eq!(initial_keys["data"]["auditPrimaryKeyId"], "live-audit-1");
    assert!(initial_keys.to_string().find("material").is_none());
    assert!(initial_keys.to_string().find("valueEncrypted").is_none());

    let (status, envelope_rotation) = response_json(
        request(
            &app,
            "POST",
            "/api/v1/live/admin/keys/envelope/rotate",
            &admin_bearer,
            Some(json!({
                "expectedRevision": 1,
                "keyId": "live-envelope-2"
            })),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(envelope_rotation["data"]["keyDomain"], "envelope");
    assert_eq!(envelope_rotation["data"]["revision"], 2);
    assert_eq!(envelope_rotation["data"]["reencryptedSessions"], 1);
    assert_eq!(envelope_rotation["data"]["reencryptedReplays"], 1);
    assert_eq!(envelope_rotation["data"]["primaryKeyId"], "live-envelope-2");
    assert_eq!(
        envelope_rotation["data"]["previousPrimaryKeyId"],
        "live-envelope-1"
    );
    let encrypted: (String, String) = sqlx::query_as(
        "SELECT encrypted_item_snapshot, encrypted_descriptor
         FROM live_playback_sessions WHERE id = $1",
    )
    .bind(admin_session.session.id.to_string())
    .fetch_one(&pool)
    .await?;
    assert!(encrypted.0.starts_with("elx-live:v1:live-envelope-2:"));
    assert!(encrypted.1.starts_with("elx-live:v1:live-envelope-2:"));
    let encrypted_replay: String = sqlx::query_scalar(
        "SELECT encrypted_response FROM live_session_idempotency WHERE session_id = $1",
    )
    .bind(admin_session.session.id.to_string())
    .fetch_one(&pool)
    .await?;
    assert!(encrypted_replay.starts_with("elx-live:v1:live-envelope-2:"));

    let (status, audit_rotation) = response_json(
        request(
            &app,
            "POST",
            "/api/v1/live/admin/keys/audit/rotate",
            &admin_bearer,
            Some(json!({
                "expectedRevision": 2,
                "keyId": "live-audit-2"
            })),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(audit_rotation["data"]["keyDomain"], "audit");
    assert_eq!(audit_rotation["data"]["revision"], 3);
    let audit_rotation_key: String = sqlx::query_scalar(
        "SELECT audit_key_id FROM live_admin_audit_events
         WHERE action = 'audit_key_rotate' ORDER BY occurred_at DESC, id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(audit_rotation_key, "live-audit-1");

    let server_delivery_session = state
        .live
        .session_repository()
        .unwrap()
        .create(
            NewSession {
                owner: session.session.owner,
                item_key: SecretBytes::from_utf8("o11-token-item".to_string()),
                stream_option_key: SecretBytes::from_utf8("o11-token-stream".to_string()),
                item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                descriptor: SecretBytes::from_utf8("{}".to_string()),
                delivery_mode: DeliveryMode::ServerRelay,
                protocol: SessionProtocol::Hls,
                source_index: 0,
                control_fencing_token: fence,
                now: Utc::now(),
            },
            None,
        )
        .await?;
    let (status, token_rotation) = response_json(
        request(
            &app,
            "POST",
            "/api/v1/live/admin/keys/token-hash/rotate",
            &admin_bearer,
            Some(json!({
                "expectedRevision": 3,
                "keyId": "live-token-hash-2"
            })),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(token_rotation["data"]["keyDomain"], "token_hash");
    assert_eq!(token_rotation["data"]["revision"], 4);
    assert_eq!(token_rotation["data"]["terminatedSessions"], 1);
    let token_terminated = state
        .live
        .session_repository()
        .unwrap()
        .get_owned(
            server_delivery_session.session.owner,
            server_delivery_session.session.id,
        )
        .await?
        .expect("token-rotation terminal diagnostic row");
    assert_eq!(token_terminated.state, SessionState::Failed);
    assert_eq!(
        token_terminated.error_code.as_deref(),
        Some("LIVE_TOKEN_KEY_ROTATED")
    );
    let crypto = state.live.crypto().await.unwrap();
    assert_eq!(crypto.primary_key_id()?, "live-envelope-2");
    assert_eq!(crypto.token_hash_primary_key_id()?, "live-token-hash-2");

    let terminate_route = format!(
        "/api/v1/live/admin/sessions/{}/terminate",
        admin_session.session.id
    );
    let (status, stale_terminate) = response_json(
        request(
            &app,
            "POST",
            &terminate_route,
            &admin_bearer,
            Some(json!({"expectedRevision": 1})),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        stale_terminate["errors"][0]["code"],
        "LIVE_REVISION_CONFLICT"
    );
    let (status, terminated_by_admin) = response_json(
        request(
            &app,
            "POST",
            &terminate_route,
            &admin_bearer,
            Some(json!({"expectedRevision": 2})),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(terminated_by_admin["data"]["status"], "completed");
    assert_eq!(terminated_by_admin["data"]["revision"], 3);
    assert_eq!(terminated_by_admin["data"]["state"], "ended");
    assert_eq!(
        terminated_by_admin["data"]["audit"]["action"],
        "session_terminate"
    );
    let terminated = state
        .live
        .session_repository()
        .unwrap()
        .get_owned(admin_session.session.owner, admin_session.session.id)
        .await?
        .expect("administratively terminated session diagnostic row");
    assert_eq!(terminated.state, SessionState::Ended);
    let (status, terminal_diagnostics) =
        response_json(request(&app, "GET", &diagnostics_route, &admin_bearer, None).await?).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(terminal_diagnostics["data"]["session"]["state"], "ended");
    assert_eq!(
        terminal_diagnostics["data"]["planner"]["descriptorStatus"],
        "terminal_tombstone"
    );
    assert_eq!(terminal_diagnostics["data"]["partial"], false);
    assert_eq!(terminal_diagnostics["data"]["cleanup"]["terminal"], true);
    assert_eq!(terminal_diagnostics["data"]["cleanup"]["complete"], true);

    let disable_session = state
        .live
        .session_repository()
        .unwrap()
        .create(
            NewSession {
                owner: session.session.owner,
                item_key: SecretBytes::from_utf8("o11-disable-item".to_string()),
                stream_option_key: SecretBytes::from_utf8("o11-disable-stream".to_string()),
                item_snapshot: SecretBytes::from_utf8("{}".to_string()),
                descriptor: SecretBytes::from_utf8("{}".to_string()),
                delivery_mode: DeliveryMode::ClientDirect,
                protocol: SessionProtocol::Hls,
                source_index: 0,
                control_fencing_token: fence,
                now: Utc::now(),
            },
            None,
        )
        .await?;
    let disable_route = format!("/api/v1/live/admin/providers/{provider_id}/disable");
    let (status, stale_disable) = response_json(
        request(
            &app,
            "POST",
            &disable_route,
            &admin_bearer,
            Some(json!({"expectedRevision": 3})),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale_disable["errors"][0]["code"], "LIVE_REVISION_CONFLICT");
    let (status, disabled) = response_json(
        request(
            &app,
            "POST",
            &disable_route,
            &admin_bearer,
            Some(json!({"expectedRevision": 4})),
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(disabled["data"]["status"], "accepted");
    assert_eq!(disabled["data"]["revision"], 5);
    assert_eq!(disabled["data"]["providerId"], provider_id.to_string());
    assert_eq!(disabled["data"]["audit"]["action"], "provider_disable");
    assert!(disabled["data"]["operationId"].as_str().is_some());
    assert!(disabled["data"].get("revocationEventIds").is_none());
    let terminated = state
        .live
        .session_repository()
        .unwrap()
        .get_owned(disable_session.session.owner, disable_session.session.id)
        .await?
        .expect("provider-disabled session diagnostic row");
    assert_eq!(terminated.state, SessionState::Failed);
    assert_eq!(
        terminated.error_code.as_deref(),
        Some("LIVE_PROVIDER_UNAVAILABLE")
    );
    let instance_enabled: i64 = sqlx::query_scalar(
        "SELECT CAST(CASE WHEN instances.enabled THEN 1 ELSE 0 END AS BIGINT)
         FROM extension_instances AS instances
         JOIN providers ON providers.instance_id = instances.instance_id
         WHERE providers.provider_id = $1",
    )
    .bind(provider_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(instance_enabled, 0);

    let (status, providers) =
        response_json(request(&app, "GET", "/api/v1/live/admin/providers", &bearer, None).await?)
            .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(providers["data"].as_array().unwrap().len(), 1);
    assert_eq!(providers["data"][0]["providerId"], provider_id.to_string());
    assert_eq!(providers["data"][0]["enabled"], false);
    assert_eq!(providers["data"][0]["providerRevision"], 5);
    assert_eq!(providers["data"][0]["grantRevision"], 3);
    assert_eq!(providers["data"][0]["activeSessions"], 0);

    let audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM live_admin_audit_events
         WHERE home_id = $1 AND target_type = 'destination_rule'",
    )
    .bind(home_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(audit_count, 3);
    let revocation_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM authorization_revocation_outbox
         WHERE event_type = 'provider_policy_changed' AND provider_id = $1",
    )
    .bind(provider_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(revocation_count, 2);
    let provider_disable_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM authorization_revocation_outbox
         WHERE event_type = 'provider_disabled' AND provider_id = $1",
    )
    .bind(provider_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(provider_disable_count, 1);
    let provider_disable_audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM live_admin_audit_events
         WHERE home_id = $1 AND target_type = 'provider' AND action = 'provider_disable'",
    )
    .bind(home_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(provider_disable_audit_count, 1);
    let session_terminate_audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM live_admin_audit_events
         WHERE home_id = $1 AND target_type = 'session' AND action = 'session_terminate'",
    )
    .bind(home_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(session_terminate_audit_count, 1);
    let post_rotation_audit_keys: Vec<String> = sqlx::query_scalar(
        "SELECT audit_key_id FROM live_admin_audit_events
         WHERE action IN ('token_hash_key_rotate', 'session_terminate', 'provider_disable')
         ORDER BY occurred_at, id",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(post_rotation_audit_keys.len(), 3);
    assert!(
        post_rotation_audit_keys
            .iter()
            .all(|key_id| key_id == "live-audit-2")
    );
    let forbidden_after: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM media_files),
            (SELECT COUNT(*) FROM acquisition_subscriptions),
            (SELECT COUNT(*) FROM playback_sessions)",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(forbidden_after, forbidden_before);
    Ok(())
}

async fn add_capable_admin(
    state: &AppState,
    pool: &sqlx::AnyPool,
    home_id: Uuid,
    owner_user_id: Uuid,
) -> Result<String> {
    let user_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, 'hashed')")
        .bind(user_id.to_string())
        .bind(format!("o11-admin-{user_id}@example.invalid"))
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO home_members (id, home_id, user_id, role, status)
         VALUES ($1, $2, $3, 'admin', 'active')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(home_id.to_string())
    .bind(user_id.to_string())
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO profiles (id, home_id, user_id, profile_type, display_name, is_default)
         VALUES ($1, $2, $3, 'account', 'O11 Admin', FALSE)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .bind(user_id.to_string())
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
         VALUES ($1, $2, 1)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .execute(pool)
    .await?;
    AuthorizationRepository::new(pool)
        .set_profile_override(
            owner_user_id,
            "O11 Owner",
            profile_id,
            Capability::LiveManage,
            true,
            None,
        )
        .await?;
    AuthorizationRepository::new(pool)
        .set_profile_override(
            owner_user_id,
            "O11 Owner",
            profile_id,
            Capability::SecretsManage,
            true,
            None,
        )
        .await?;
    AuthorizationRepository::new(pool)
        .set_profile_override(
            owner_user_id,
            "O11 Owner",
            profile_id,
            Capability::ExtensionsManage,
            true,
            None,
        )
        .await?;
    sqlx::query(
        "INSERT INTO account_sessions (
            id, user_id, home_id, active_profile_id, remember_device, expires_at
         ) VALUES ($1, $2, $3, $4, FALSE, $5)",
    )
    .bind(session_id.to_string())
    .bind(user_id.to_string())
    .bind(home_id.to_string())
    .bind(profile_id.to_string())
    .bind((Utc::now() + Duration::hours(1)).to_rfc3339())
    .execute(pool)
    .await?;
    let (token, _) = state.auth_service.sign_session_access_token(
        user_id,
        session_id,
        home_id,
        profile_id,
        HomeRole::Admin,
    )?;
    Ok(token)
}

async fn seed_live_rotation_keys(
    pool: &sqlx::AnyPool,
    secrets: &Arc<SecretsManager>,
) -> Result<()> {
    for (key, material) in [
        ("live.crypto.envelope.live-envelope-2", [41_u8; 32]),
        ("live.crypto.token_hash.live-token-hash-2", [42_u8; 32]),
        ("live.crypto.audit.live-audit-2", [43_u8; 32]),
    ] {
        let encoded = general_purpose::STANDARD.encode(material);
        sqlx::query(
            "INSERT INTO secrets
                (secret_id, scope, scope_id, key, value_encrypted, rotatable)
             VALUES ($1, 'global', NULL, $2, $3, TRUE)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(key)
        .bind(secrets.encrypt(&encoded)?)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_provider(database: &Database) -> Result<Uuid> {
    let extension_id = format!("elixir.test.o11.http.{}", Uuid::new_v4());
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO extensions
            (extension_id, name, version, kind, trust_level, manifest_json, enabled)
         VALUES ($1, 'O11 HTTP Provider', '1.0.0', 'module', 'verified', '{}', TRUE)",
    )
    .bind(&extension_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO extension_instances (instance_id, extension_id, instance_name, enabled)
         VALUES ($1, $2, 'default', TRUE)",
    )
    .bind(instance_id.to_string())
    .bind(&extension_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO providers (
            provider_id, instance_id, capability, slot_id, cardinality, health_state
         ) VALUES ($1, $2, 'live.catalog_provider', 'default', 'one', 'healthy')",
    )
    .bind(provider_id.to_string())
    .bind(instance_id.to_string())
    .execute(&database.pool)
    .await?;
    Ok(provider_id)
}

fn settings() -> Settings {
    Settings {
        environment: RunEnvironment::Development,
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 44301,
        },
        database: DatabaseConfig {
            url: format!(
                "sqlite:file:/tmp/o11-live-http-{}?mode=memory&cache=shared",
                Uuid::new_v4()
            ),
            max_connections: 8,
            connect_timeout_seconds: 5,
        },
        library: LibraryConfig::default(),
        extensions: crate::config::ExtensionsConfig::default(),
        auth: AuthConfig::default(),
        secrets: SecretsConfig {
            master_key: Some(general_purpose::STANDARD.encode([31u8; 32])),
        },
        telemetry: TelemetryConfig::default(),
        metadata: crate::config::MetadataConfig::default(),
        classifier: ClassifierConfig::default(),
        playback: crate::config::PlaybackConfig::default(),
        media_interactions: MediaInteractionsConfig::default(),
        live: LiveConfig {
            enabled: true,
            catalog_enabled: true,
            playback_enabled: true,
            client_direct_enabled: true,
            ..LiveConfig::default()
        },
        network: crate::config::NetworkConfig::default(),
    }
}

async fn request(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, String)],
    body_value: Option<Value>,
) -> Result<axum::response::Response> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    let body = if let Some(value) = body_value {
        builder = builder.header("content-type", "application/json");
        Body::from(value.to_string())
    } else {
        Body::empty()
    };
    Ok(app.clone().oneshot(builder.body(body)?).await?)
}

async fn response_json(response: axum::response::Response) -> Result<(StatusCode, Value)> {
    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), 2 * 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

fn assert_json_key_absent(value: &Value, forbidden: &str) {
    match value {
        Value::Object(object) => {
            assert!(
                !object.contains_key(forbidden),
                "support bundle leaked field {forbidden}"
            );
            for child in object.values() {
                assert_json_key_absent(child, forbidden);
            }
        }
        Value::Array(values) => {
            for child in values {
                assert_json_key_absent(child, forbidden);
            }
        }
        _ => {}
    }
}
