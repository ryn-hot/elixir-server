use super::*;
use crate::{
    config::DatabaseConfig,
    db::models::SecretScope,
    extensions::store::ExtensionStore,
    marketplace::{grant_store::tests::owner, preferences::local_setup::bind_initial_home},
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn worker_startup_credit_has_headroom_without_raising_the_monthly_cap_or_minimum() -> Result<()> {
    let limits = OrdinaryLimits::default();
    assert_eq!(
        worker_startup_bytes(limits.monthly_transfer_bytes)?,
        8 * 1024 * 1024
    );
    assert_eq!(
        worker_startup_bytes(INITIAL_WORKER_BYTES)?,
        INITIAL_WORKER_BYTES
    );
    assert_eq!(
        worker_startup_bytes(3 * INITIAL_WORKER_BYTES)?,
        3 * INITIAL_WORKER_BYTES
    );
    assert!(worker_startup_bytes(INITIAL_WORKER_BYTES - 1).is_err());
    assert!(worker_startup_bytes(0).is_err());
    assert!(PREFERRED_WORKER_STARTUP_BYTES <= elixir_marketplace::packet_guard::MAX_CREDIT_BYTES);
    Ok(())
}

#[tokio::test]
async fn worker_startup_credit_is_committed_once_and_unused_bytes_are_not_refunded() -> Result<()> {
    let (_directory, database) = database().await?;
    let now = 1_790_000_000;
    let limits = OrdinaryLimits {
        monthly_transfer_bytes: 9 * 1024 * 1024,
        ..Default::default()
    };
    let first = reserve_worker_startup_credit(&database, now, &limits).await?;
    assert_eq!(first.bytes(), PREFERRED_WORKER_STARTUP_BYTES);
    assert_eq!(first.remaining_after(), INITIAL_WORKER_BYTES);
    let original_revision = first.into_packet_credit()?.revision;
    let second = reserve_worker_startup_credit(&database, now, &limits).await?;
    assert_eq!(second.bytes(), INITIAL_WORKER_BYTES);
    assert_eq!(second.remaining_after(), 0);
    assert!(second.into_packet_credit()?.revision > original_revision);
    assert_eq!(available_transfer(&database, now, &limits).await?, 0);
    assert!(
        reserve_worker_startup_credit(&database, now, &limits)
            .await
            .is_err()
    );
    assert_eq!(available_transfer(&database, now, &limits).await?, 0);
    Ok(())
}

pub(super) fn profile_json() -> Value {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    ))
    .unwrap();
    json!({"schema_version":1,
        "guard_image":"sha256:40e592563f2d757c2d8d3464be357502b6485b0ed94d9fb75c086cca12918b58",
        "worker_image":"sha256:2174d9f0681d1bc114afc721fa9f735e540ef4db37bdbe444c6502d15bb302a4",
        "issuer_keys":[fixture["issuer_key"]],
        "network":{"bootstrap":[{"address":"1.1.1.1","port":49737}],"topics":[Digest::of(b"startup fixture")],
            "roles":{"consumer":true,"catalog":false,"routing":false},"limits":elixir_marketplace::worker_network::NetworkLimits::default()},
        "provider":{"type":"native_wireguard","endpoint_address":"8.8.8.8","endpoint_port":51820,"local_port":51821,
            "tunnel_addresses":["10.66.0.2"],"peer_public_key":STANDARD.encode([34;32]),
            "device_secret":"marketplace.vpn.device","preshared_secret":null,"mtu":1420,"keepalive_seconds":25,"require_warp":false}})
}

#[cfg(unix)]
#[test]
fn native_provider_requires_explicit_versioned_consent_and_closed_pinned_setup() -> Result<()> {
    let mut valid = profile_json();
    valid["provider"] = json!({"type":"native_warp", "image":valid["guard_image"],
        "consent_version":elixir_marketplace::warp_provider::CONSENT_VERSION,
        "enrollment":Digest::of(b"distinct explicit setup")});
    let profile = parsed(&valid)?;
    assert!(profile.probe_policy().require_warp);
    assert!(
        profile.policy().is_err(),
        "native provider must never fabricate WireGuard setup"
    );
    for (key, value) in [
        ("image", json!("mutable:latest")),
        ("consent_version", json!("")),
        ("consent_version", json!("future-consent")),
        ("enrollment", json!(Digest::from_bytes([0; 32]))),
        ("resume", json!(false)),
        ("protected", json!(true)),
        ("device_secret", json!("downloader.identity")),
    ] {
        let mut bad = valid.clone();
        bad["provider"][key] = value;
        assert!(parsed(&bad).is_err(), "{key}");
    }
    for missing in ["image", "consent_version", "enrollment"] {
        let mut bad = valid.clone();
        bad["provider"].as_object_mut().unwrap().remove(missing);
        assert!(parsed(&bad).is_err());
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn customer_provider_selection_is_closed_pinned_and_contains_no_acceptance_or_identity()
-> Result<()> {
    let mut valid = profile_json();
    valid["provider"] = json!({"type":"native_warp_setup","image":valid["guard_image"]});
    let profile = parsed(&valid)?;
    assert!(profile.probe_policy().require_warp);
    assert!(profile.policy().is_err());
    for (field, value) in [
        ("image", json!("mutable:latest")),
        ("consent_version", json!("elixir-marketplace-warp-v1")),
        ("enrollment", json!(Digest::of(b"caller enrollment"))),
        ("resume", json!(false)),
        ("protected", json!(true)),
    ] {
        let mut invalid = valid.clone();
        invalid["provider"][field] = value;
        assert!(parsed(&invalid).is_err(), "{field}");
    }
    valid["provider"].as_object_mut().unwrap().remove("image");
    assert!(parsed(&valid).is_err());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn customer_provider_http_consent_commits_once_but_cannot_create_a_startup_owner()
-> Result<()> {
    use axum::{
        body::{self, Body},
        extract::State,
        http::{Request, StatusCode},
    };
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    use tower::ServiceExt;
    let (root, database) = database().await?;
    let url = format!("sqlite://{}", root.path().join("startup.sqlite").display());
    let (mut state, _, principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture(
            database.clone(),
            root.path(),
            &url,
        )
        .await?;
    let mut profile = profile_json();
    profile["provider"] = json!({"type":"native_warp_setup","image":profile["guard_image"]});
    let path = root.path().canonicalize()?.join("customer-startup.json");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(&serde_json::to_vec(&profile)?)?;
    file.sync_all()?;
    let mut settings = state.settings.as_ref().clone();
    settings.marketplace.startup_file = Some(path);
    state.settings = Arc::new(settings);
    let app = crate::http::router(state.clone());
    let endpoint = "/api/v1/marketplace/connection/provider";
    let accept = json!({"schema_version":1,"expected_revision":0,"mutation_id":Uuid::new_v4(),"enabled":true,
        "consent_version":elixir_marketplace::warp_provider::CONSENT_VERSION});
    assert_eq!(
        app.clone()
            .oneshot(Request::patch(endpoint).body(Body::from(accept.to_string()))?)
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let before = app
        .clone()
        .oneshot(
            Request::get("/api/v1/marketplace/settings")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(before.status(), StatusCode::OK);
    let before: Value = serde_json::from_slice(&body::to_bytes(before.into_body(), 65536).await?)?;
    assert_eq!(before["provider_setup"]["revision"], 0);
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(
                Request::patch(endpoint)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(accept.to_string()))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let response: Value =
            serde_json::from_slice(&body::to_bytes(response.into_body(), 65536).await?)?;
        assert_eq!(response["provider_setup"]["revision"], 1);
        assert_eq!(response["provider_setup"]["consent_current"], true);
        assert!(response["provider_setup"].get("enrollment").is_none());
        assert!(!state.marketplace.startup_snapshot().retry_owner_present);
        assert!(state.marketplace.client().is_err());
    }
    let mut cookie = principal;
    cookie.transport = crate::http::auth::AccountAuthTransport::Cookie;
    assert!(
        crate::http::handlers::marketplace::update_provider_setup(
            State(state.clone()),
            cookie,
            axum::http::HeaderMap::new(),
            serde_json::to_vec(&accept)?.into()
        )
        .await
        .is_err()
    );
    let disable = json!({"schema_version":1,"expected_revision":1,"mutation_id":Uuid::new_v4(),"enabled":false,"consent_version":null});
    let response = app
        .oneshot(
            Request::patch(endpoint)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(disable.to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value =
        serde_json::from_slice(&body::to_bytes(response.into_body(), 65536).await?)?;
    assert_eq!(response["provider_setup"]["revision"], 2);
    assert_eq!(response["provider_setup"]["enabled"], false);
    assert!(state.marketplace.client().is_err());
    Ok(())
}

#[cfg(unix)]
async fn activation_fixture() -> Result<(
    tempfile::TempDir,
    Database,
    AppState,
    crate::http::auth::CurrentPrincipal,
)> {
    let (root, database) = database().await?;
    let url = format!("sqlite://{}", root.path().join("startup.sqlite").display());
    let (state, _, principal, _) = crate::http::handlers::marketplace::installation_http_fixture(
        database.clone(),
        root.path(),
        &url,
    )
    .await?;
    Ok((root, database, state, principal))
}

#[cfg(unix)]
fn activation_profile(root: &Path, name: &str, bytes: &[u8]) -> Result<PathBuf> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let path = root.canonicalize()?.join(name);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(path)
}

#[cfg(unix)]
#[tokio::test]
async fn profile_preflight_errors_never_claim_a_physical_generation_or_unknown_cleanup()
-> Result<()> {
    use crate::marketplace::service::RuntimePhase;
    let (root, database, state, _) = activation_fixture().await?;
    let missing = root.path().canonicalize()?.join("missing.json");
    let malformed = activation_profile(root.path(), "malformed.json", b"{}")?;
    for path in [&missing, &malformed, &missing] {
        assert!(activate_once(&state, path).await.is_err());
        assert_eq!(
            state.marketplace.snapshot().phase,
            RuntimePhase::NotConnected
        );
        assert!(state.marketplace.snapshot().participation.is_none());
        assert!(state.marketplace.client().is_err());
    }
    unchanged_accounting(&database).await?;
    state.marketplace.shutdown().await?;
    assert_eq!(state.marketplace.snapshot().phase, RuntimePhase::Stopped);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn missing_consent_preflight_retries_without_a_generation_or_provider_mutation() -> Result<()>
{
    use crate::marketplace::service::RuntimePhase;
    let (root, database, state, _) = activation_fixture().await?;
    let mut profile = profile_json();
    profile["provider"] = json!({"type":"native_warp_setup","image":profile["guard_image"]});
    let path = activation_profile(root.path(), "startup.json", &serde_json::to_vec(&profile)?)?;
    for _ in 0..2 {
        let error = activate_once(&state, &path).await.err().unwrap();
        assert!(
            error
                .to_string()
                .contains("explicit provider consent is required")
        );
        assert_eq!(
            state.marketplace.snapshot().phase,
            RuntimePhase::NotConnected
        );
        assert!(state.marketplace.snapshot().participation.is_none());
    }
    let (revision, decision): (i64, String) = sqlx::query_as(
        "SELECT revision,setup_json FROM marketplace_provider_setup WHERE singleton=1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(revision, 0);
    assert!(decision.is_empty());
    unchanged_accounting(&database).await?;
    state.marketplace.shutdown().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_or_stop_during_consent_preflight_cannot_claim_a_late_owner() -> Result<()> {
    use crate::marketplace::{provider_setup, service::RuntimePhase};
    for cancel in [false, true] {
        let (root, database, state, principal) = activation_fixture().await?;
        let mut profile = profile_json();
        profile["provider"] = json!({"type":"native_warp_setup","image":profile["guard_image"]});
        let bytes = serde_json::to_vec(&profile)?;
        let path = activation_profile(root.path(), "startup.json", &bytes)?;
        let change = provider_setup::Change::parse(&canonical::to_vec(&json!({
            "schema_version":1,"expected_revision":0,"mutation_id":Uuid::new_v4(),
            "enabled":true,"consent_version":elixir_marketplace::warp_provider::CONSENT_VERSION
        }))?)?;
        let (before, _) = provider_setup::save(
            &database,
            &principal,
            Digest::of(&bytes),
            &change,
            chrono::Utc::now(),
        )
        .await?;
        // Occupy the actual one-connection pool. Local preflight may wait;
        // there is no test substitute for capture and no provider is started.
        let held = database.pool.acquire().await?;
        let mut pending = Box::pin(activate_once(&state, &path));
        assert!(
            timeout(Duration::from_millis(100), pending.as_mut())
                .await
                .is_err()
        );
        assert_eq!(
            state.marketplace.snapshot().phase,
            RuntimePhase::NotConnected
        );
        if cancel {
            drop(pending);
            state.marketplace.shutdown().await?;
            drop(held);
        } else {
            state.marketplace.shutdown().await?;
            drop(held);
            let error = timeout(Duration::from_secs(5), pending)
                .await?
                .err()
                .unwrap();
            assert!(error.to_string().contains("vacant startup owner"));
        }
        assert_eq!(state.marketplace.snapshot().phase, RuntimePhase::Stopped);
        assert!(state.marketplace.snapshot().participation.is_none());
        assert!(state.marketplace.client().is_err());
        let after = provider_setup::load(
            &database,
            &principal,
            Digest::of(&bytes),
            chrono::Utc::now(),
        )
        .await?;
        assert_eq!(before, after);
        unchanged_accounting(&database).await?;
    }
    Ok(())
}

#[cfg(unix)]
fn cancelled_directory(state: &AppState) -> Result<PathBuf> {
    let paths = RuntimePaths::from_roots(
        &state.settings.extensions.storage_root,
        &state.settings.library.local_root,
    );
    Ok(PathBuf::from(paths.data_root)
        .canonicalize()?
        .join("marketplace"))
}

#[cfg(unix)]
fn cancel_empty_generation(state: &AppState) -> Result<()> {
    drop(state.marketplace.begin_startup()?);
    state.marketplace.request_stop();
    assert_eq!(
        state.marketplace.snapshot().phase,
        crate::marketplace::service::RuntimePhase::Failed
    );
    Ok(())
}

#[cfg(unix)]
// Real SQLite uses an external worker thread: a paused runtime can expire its
// connection timer before that thread replies. Keep this real 300-ms lock wait.
#[tokio::test]
async fn cancelled_native_retirement_waits_for_original_lease_and_binds_same_service() -> Result<()>
{
    use crate::marketplace::service::RuntimePhase;
    let (_root, _database, state, _) = activation_fixture().await?;
    cancel_empty_generation(&state)?;
    let directory = cancelled_directory(&state)?;
    let held = StartupLeases::acquire_inner(&directory, true)?;
    let original = state.clone();
    let retiring = tokio::spawn(async move { native_provider::retire_cancelled(&original).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !retiring.is_finished(),
        "cannot replace the original controller lease"
    );
    assert_eq!(state.marketplace.snapshot().phase, RuntimePhase::Failed);
    drop(held);
    let proof = retiring.await??;
    let foreign = crate::marketplace::service::MarketplaceService::default();
    drop(foreign.begin_startup()?);
    foreign.request_stop();
    assert!(foreign.finish_cancelled_startup(&proof).is_err());
    assert_eq!(foreign.snapshot().phase, RuntimePhase::Failed);
    state.marketplace.finish_cancelled_startup(&proof)?;
    assert_eq!(state.marketplace.snapshot().phase, RuntimePhase::Stopped);
    assert!(state.marketplace.client().is_err());
    assert!(
        !directory.join("worker/provider-state.v1").exists(),
        "empty retirement must never enroll"
    );
    drop(proof);
    state.marketplace.shutdown().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn corrupt_native_preparation_never_produces_a_stopped_acknowledgement() -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let (_root, _database, state, _) = activation_fixture().await?;
    cancel_empty_generation(&state)?;
    let directory = cancelled_directory(&state)?;
    let leases = StartupLeases::acquire_inner(&directory, true)?;
    let fence = directory.join("worker/provider-native.pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&fence)?;
    file.write_all(b"malformed original intent")?;
    file.sync_all()?;
    drop(file);
    drop(leases);
    assert!(native_provider::retire_cancelled(&state).await.is_err());
    assert_eq!(std::fs::read(&fence)?, b"malformed original intent");
    assert_eq!(
        state.marketplace.snapshot().phase,
        crate::marketplace::service::RuntimePhase::Failed
    );
    assert!(state.marketplace.client().is_err());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn replaced_controller_inode_invalidates_cancelled_retirement_acknowledgement() -> Result<()>
{
    use std::os::unix::fs::OpenOptionsExt;
    let (_root, _database, state, _) = activation_fixture().await?;
    cancel_empty_generation(&state)?;
    let proof = native_provider::retire_cancelled(&state).await?;
    let lock = cancelled_directory(&state)?.join("worker/controller.lock");
    std::fs::rename(&lock, lock.with_file_name("original-controller.lock"))?;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&lock)?
        .sync_all()?;
    assert!(state.marketplace.finish_cancelled_startup(&proof).is_err());
    assert_eq!(
        state.marketplace.snapshot().phase,
        crate::marketplace::service::RuntimePhase::Failed
    );
    assert!(state.marketplace.client().is_err());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn original_local_profile_change_or_loss_invalidates_connection_binding() -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let root = tempfile::tempdir()?;
    let path = root.path().canonicalize()?.join("startup.json");
    let bytes = serde_json::to_vec(&profile_json())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(&bytes)?;
    let binding = ConnectionBinding {
        path: path.clone(),
        digest: Digest::of(&bytes),
        saved: None,
        provider: None,
    };
    binding.current().await?;
    std::fs::write(&path, b"{}")?;
    assert!(binding.current().await.is_err());
    std::fs::remove_file(&path)?;
    assert!(binding.current().await.is_err());
    Ok(())
}
fn parsed(value: &Value) -> Result<Profile> {
    Profile::parse(&serde_json::to_vec(value)?)
}

#[test]
fn configured_startup_is_absent_by_default_and_has_separate_stable_runtime_ownership() -> Result<()>
{
    assert!(
        crate::config::Settings::default()
            .marketplace
            .startup_file
            .is_none()
    );
    assert!(
        serde_json::from_value::<crate::config::MarketplaceConfig>(json!({"enable":true})).is_err()
    );
    let paths = RuntimePaths::from_roots("/fixture/extensions", "/fixture/media");
    let deployment = marketplace_deployment(&paths);
    assert_ne!(
        deployment,
        paths.deployment_id(),
        "the extension orphan collector must not own marketplace actors"
    );
    assert_eq!(deployment, marketplace_deployment(&paths));
    assert!(
        deployment.len() <= 64
            && deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    );
    Ok(())
}
pub(super) async fn database() -> Result<(tempfile::TempDir, Database)> {
    let directory = tempfile::tempdir()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            directory.path().join("startup.sqlite").display()
        ),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    Ok((directory, database))
}
async fn store(
    database: &Database,
    manager: &SecretsManager,
    key: &str,
    value: &str,
    encrypted: bool,
) -> Result<()> {
    let ciphertext = if encrypted {
        manager.encrypt(value)?
    } else {
        value.into()
    };
    // The fixture updates an exact existing row. Corrupt legacy NULL-scope
    // duplicates must be injected explicitly, not through the safe upsert.
    let changed=sqlx::query("UPDATE secrets SET value_encrypted=$1 WHERE scope='global' AND scope_id IS NULL AND key=$2")
        .bind(&ciphertext).bind(key).execute(&database.pool).await?.rows_affected();
    if changed == 1 {
        return Ok(());
    }
    ensure!(changed == 0, "fixture secret is already ambiguous");
    ExtensionStore::new(&database.pool)
        .upsert_secret(&crate::extensions::store::NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: key.into(),
            value_encrypted: ciphertext,
            rotatable: true,
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn ambiguous_or_oversized_secret_rows_never_select_one_identity() -> Result<()> {
    let (_directory, database) = database().await?;
    let actor = owner(&database).await?;
    bind_initial_home(&database, actor.home, Uuid::new_v4()).await?;
    let manager = SecretsManager::from_key_bytes([77; 32], false);
    let profile = parsed(&profile_json())?;
    store(
        &database,
        &manager,
        "marketplace.vpn.device",
        &STANDARD.encode([17; 32]),
        true,
    )
    .await?;
    let original = prepare_vpn(&database, &manager, &profile).await?;
    ExtensionStore::new(&database.pool)
        .upsert_secret(&crate::extensions::store::NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: "marketplace.vpn.device".into(),
            value_encrypted: manager.encrypt(&STANDARD.encode([18; 32]))?,
            rotatable: true,
        })
        .await?;
    // The managed API rotates one logical key; it must not manufacture the
    // legacy duplicate state that this rejection fixture needs to exercise.
    let rotated = prepare_vpn(&database, &manager, &profile).await?;
    assert_ne!(original.identity.device, rotated.identity.device);
    assert_eq!(
        *stored_key(&database, &manager, "marketplace.vpn.device").await?,
        [18; 32]
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM secrets WHERE scope='global' AND scope_id IS NULL AND key='marketplace.vpn.device'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(count, 1);
    unchanged_accounting(&database).await?;

    // Simulate an actual pre-existing corrupt database, bypassing the API that
    // now prevents duplicate creation. Keep both unequal/equal-key rejection
    // checks: neither ordering nor matching plaintext resolves ambiguity.
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'global',NULL,'marketplace.vpn.device',$2,TRUE)")
        .bind(Uuid::new_v4().to_string())
        .bind(manager.encrypt(&STANDARD.encode([17; 32]))?)
        .execute(&database.pool)
        .await?;
    assert!(prepare_vpn(&database, &manager, &profile).await.is_err());
    // Still refuse if every duplicate happens to contain the same key.
    sqlx::query("UPDATE secrets SET value_encrypted=$1 WHERE key='marketplace.vpn.device'")
        .bind(manager.encrypt(&STANDARD.encode([17; 32]))?)
        .execute(&database.pool)
        .await?;
    assert!(prepare_vpn(&database, &manager, &profile).await.is_err());
    unchanged_accounting(&database).await?;
    let (_other_directory, other) = self::database().await?;
    store(
        &other,
        &manager,
        "marketplace.vpn.device",
        &"x".repeat(8192),
        false,
    )
    .await?;
    assert!(
        stored_key(&other, &manager, "marketplace.vpn.device")
            .await
            .is_err()
    );
    unchanged_accounting(&other).await?;
    Ok(())
}
async fn unchanged_accounting(database: &Database) -> Result<()> {
    let (revision, json): (i64, String) = sqlx::query_as(
        "SELECT revision,state_json FROM marketplace_transfer_accounting WHERE singleton=1",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(revision, 0);
    assert!(json.is_empty());
    Ok(())
}

#[test]
fn profile_is_closed_pinned_and_cannot_choose_contribution_or_borrow_other_keys() -> Result<()> {
    let original = profile_json();
    let valid = parsed(&original)?;
    assert_eq!(valid.limits.memory_bytes, 256 * 1024 * 1024);
    assert_eq!(valid.limits.cpu_millicores, 100);
    assert_eq!(valid.policy()?.endpoints.len(), 1);
    assert_eq!(valid.probe_policy().family, ProbeFamily::Ipv4);
    for (pointer, value) in [
        ("/schema_version", json!(2)),
        ("/guard_image", json!("elixir-marketplace-netguard:latest")),
        ("/worker_image", json!("image:tag")),
        ("/issuer_keys", json!([])),
        ("/network/roles/catalog", json!(true)),
        ("/network/roles/routing", json!(true)),
        ("/network/roles/consumer", json!(false)),
        ("/network/bootstrap/0/address", json!("127.0.0.1")),
        (
            "/network/bootstrap/0/address",
            json!("bootstrap.example.com"),
        ),
        ("/network/limits/established_peers", json!(999)),
        ("/provider/endpoint_address", json!("192.168.1.1")),
        ("/provider/device_secret", json!("global:download_private")),
        ("/provider/device_secret", json!("wireguard_config")),
        (
            "/provider/preshared_secret",
            json!("marketplace.vpn.device"),
        ),
        ("/provider/mtu", json!(1000)),
        ("/provider/type", json!("free_builtin")),
    ] {
        let mut invalid = original.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        assert!(parsed(&invalid).is_err(), "{pointer}");
    }
    for key in [
        "command",
        "environment",
        "ports",
        "private_key",
        "runtime_health",
    ] {
        let mut invalid = original.clone();
        invalid[key] = json!(true);
        assert!(parsed(&invalid).is_err());
        let mut invalid = original.clone();
        invalid["provider"][key] = json!(true);
        assert!(parsed(&invalid).is_err());
    }
    let mut large = original.clone();
    let mut limits = serde_json::to_value(OrdinaryLimits::default())?;
    limits["memory_bytes"] = json!(1024 * 1024 * 1024);
    large["limits"] = limits;
    assert!(parsed(&large).is_err());
    let duplicate = serde_json::to_string(&original)?.replacen(
        "\"schema_version\":1",
        "\"schema_version\":1,\"schema_version\":1",
        1,
    );
    assert!(Profile::parse(duplicate.as_bytes()).is_err());
    assert!(Profile::parse(&vec![b' '; MAX_PROFILE_BYTES + 1]).is_err());
    Ok(())
}

#[tokio::test]
async fn key_preparation_requires_bound_owner_encrypted_scoped_keys_and_never_debits() -> Result<()>
{
    let (_directory, database) = database().await?;
    let profile = parsed(&profile_json())?;
    let manager = SecretsManager::from_key_bytes([77; 32], true);
    store(
        &database,
        &manager,
        "marketplace.vpn.device",
        &STANDARD.encode([17; 32]),
        true,
    )
    .await?;
    assert!(
        prepare_vpn(&database, &manager, &profile).await.is_err(),
        "no first-caller owner election"
    );
    let actor = owner(&database).await?;
    bind_initial_home(&database, actor.home, Uuid::new_v4()).await?;
    let prepared = prepare_vpn(&database, &manager, &profile).await?;
    prepared.secrets.check_device(&prepared.setup)?;
    assert_eq!(prepared.setup.device_public_key, prepared.identity.device);
    assert_eq!(
        preferences::load_internal(&database).await?.preferences,
        preferences::Preferences::default()
    );
    unchanged_accounting(&database).await?;
    for value in [
        STANDARD.encode([0; 32]),
        "invalid-secret-do-not-print".into(),
        STANDARD.encode([9; 31]),
    ] {
        store(&database, &manager, "marketplace.vpn.device", &value, true).await?;
        let error = match prepare_vpn(&database, &manager, &profile).await {
            Ok(_) => panic!("invalid secret accepted"),
            Err(e) => e,
        };
        assert!(!format!("{error:#}").contains(&value));
    }
    store(
        &database,
        &manager,
        "marketplace.vpn.device",
        &STANDARD.encode([17; 32]),
        false,
    )
    .await?;
    assert!(
        prepare_vpn(&database, &manager, &profile).await.is_err(),
        "development plaintext cannot bypass encryption"
    );
    unchanged_accounting(&database).await?;
    Ok(())
}

#[tokio::test]
async fn missing_or_wrong_key_store_and_spent_allowance_cannot_prepare_a_provider() -> Result<()> {
    let (_directory, database) = database().await?;
    let actor = owner(&database).await?;
    bind_initial_home(&database, actor.home, Uuid::new_v4()).await?;
    let profile = parsed(&profile_json())?;
    let manager = SecretsManager::from_key_bytes([77; 32], false);
    assert!(prepare_vpn(&database, &manager, &profile).await.is_err());
    store(
        &database,
        &manager,
        "marketplace.vpn.device",
        &STANDARD.encode([17; 32]),
        true,
    )
    .await?;
    assert!(
        prepare_vpn(
            &database,
            &SecretsManager::from_key_bytes([99; 32], false),
            &profile
        )
        .await
        .is_err()
    );
    let limits = profile.limits.clone();
    reserve_transfer(&database, now()?, limits.monthly_transfer_bytes, &limits).await?;
    assert!(prepare_vpn(&database, &manager, &profile).await.is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn startup_file_rejects_shared_alias_oversized_and_nonregular_inputs() -> Result<()> {
    use std::{
        io::Write,
        os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink},
    };
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let path = root.join("startup.json");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(&serde_json::to_vec(&profile_json())?)?;
    file.sync_all()?;
    drop(file);
    read_profile(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
    assert!(read_profile(&path).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let link = root.join("alias.json");
    symlink(&path, &link)?;
    assert!(read_profile(&link).is_err());
    let hard = root.join("hard.json");
    std::fs::hard_link(&path, &hard)?;
    assert!(read_profile(&path).is_err());
    assert!(read_profile(&root).is_err());
    let oversized = root.join("large.json");
    std::fs::write(&oversized, vec![b' '; MAX_PROFILE_BYTES + 1])?;
    assert!(read_profile(&oversized).is_err());
    let fifo = root.join("pipe");
    let encoded = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes())?;
    assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);
    assert!(
        read_profile(&fifo).is_err(),
        "opening a FIFO must not block a configuration reader"
    );
    Ok(())
}
