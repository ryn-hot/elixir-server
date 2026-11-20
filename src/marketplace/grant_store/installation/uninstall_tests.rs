//! Owner uninstall over real signed releases, archives, journals and SQL in a
//! temporary database. ACTIVE enrollment is the same synthetic step the
//! standing tests use; containers are a recording fake (no Docker/network).
use super::*;
use crate::{
    db::models::{ExtensionKind, ExtensionTrustLevel},
    extensions::{
        manifest::ExtensionManifest,
        store::{ExtensionStore, NewExtension},
    },
    marketplace::{
        service::{MarketplaceService, UninstallError, UninstallOptions, Uninstalled},
        stack::{self, MemberRuntime, StackReconcile, StackRuntime},
    },
    orchestrator::lock::APPLY_LOCK_NAME,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use tower::ServiceExt;

use super::super::standing::{
    InstalledBinding,
    tests::{
        limit,
        local_http::{configured, secret},
    },
};
use super::super::uninstall;

/// Records container work instead of doing it.
#[derive(Default)]
struct Recorder {
    removed: std::sync::Mutex<Vec<Uuid>>,
}
impl Recorder {
    fn removed(&self) -> BTreeSet<Uuid> {
        self.removed.lock().unwrap().iter().copied().collect()
    }
}
#[async_trait::async_trait]
impl MemberRuntime for Recorder {
    async fn remove_member_runtime(&self, instance: Uuid) -> Result<()> {
        self.removed.lock().unwrap().push(instance);
        Ok(())
    }
}
#[async_trait::async_trait]
impl StackRuntime for Recorder {
    async fn ensure_image(&self, _image: &str) -> Result<()> {
        Ok(())
    }
}

fn options() -> UninstallOptions {
    UninstallOptions::new(Duration::from_secs(2), Duration::from_secs(60))
}

async fn uninstall_with(
    f: &Fixture,
    id: &str,
    runtime: &Recorder,
    options: UninstallOptions,
) -> Result<Uninstalled, UninstallError> {
    MarketplaceService::default()
        .uninstall_extension(&f.db, &f.actor.fixture_principal(), id, runtime, options)
        .await
}

fn extension(f: &Fixture) -> Result<String> {
    Ok(f.material()?
        .review
        .scope()
        .extension_id()
        .as_str()
        .to_owned())
}

/// Row counts of every table an uninstall touches, plus the budget and grant.
async fn snapshot(db: &Database) -> Result<Vec<String>> {
    let mut rows = Vec::new();
    for table in [
        "extensions",
        "extension_instances",
        "providers",
        "bindings",
        "secrets",
        "extension_settings",
        "desired_blueprints",
        "runtime_logs",
        "marketplace_installations",
        "marketplace_installation_metadata",
        "marketplace_extension_choices",
        "marketplace_instance_choices",
        "marketplace_sdk_namespaces",
        "marketplace_instance_sdk_state",
        "marketplace_instance_sdk_values",
        "marketplace_generation_secrets",
        "marketplace_installed_blob_hints",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&db.pool)
            .await?;
        rows.push(format!("{table}={count}"));
    }
    let budget: (String, i64, i64) = sqlx::query_as("SELECT CAST(revision AS TEXT),entries,encoded_bytes FROM marketplace_installation_budget WHERE singleton=1")
        .fetch_one(&db.pool).await?;
    rows.push(format!("budget={budget:?}"));
    let grants: Vec<(String, i64, i64, i64)> = sqlx::query_as("SELECT extension_id,revision,revocation_revision,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants ORDER BY extension_id")
        .fetch_all(&db.pool).await?;
    rows.push(format!("grants={grants:?}"));
    Ok(rows)
}

/// The installation budget must equal what the journals hold.
async fn assert_budget_consistent(db: &Database) -> Result<(i64, i64)> {
    let ledger: (i64, i64) = sqlx::query_as(
        "SELECT entries,encoded_bytes FROM marketplace_installation_budget WHERE singleton=1",
    )
    .fetch_one(&db.pool)
    .await?;
    let actual: (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*),CAST(COALESCE(SUM(encoded_bytes),0) AS BIGINT) FROM marketplace_installations",
    )
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(ledger, actual, "budget ledger must match the journals");
    Ok(ledger)
}

async fn count(db: &Database, sql: &str, value: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(sql)
        .bind(value)
        .fetch_one(&db.pool)
        .await?)
}

/// What an installed instance accumulates while it runs: a provider with a
/// secret, an instance secret, a saved setting and a log.
async fn seed_operational(db: &Database, instance: Uuid, capability: &str) -> Result<Uuid> {
    let provider = Uuid::new_v4();
    sqlx::query("INSERT INTO providers (provider_id,instance_id,capability,slot_id,cardinality,implementation) VALUES ($1,$2,$3,'default','many','fixture')")
        .bind(provider.to_string()).bind(instance.to_string()).bind(capability).execute(&db.pool).await?;
    for (scope, owner, key) in [
        ("provider", provider, "token"),
        ("instance", instance, "api_key"),
    ] {
        sqlx::query("INSERT INTO secrets (secret_id,scope,scope_id,key,value_encrypted) VALUES ($1,$2,$3,$4,'fixture-cipher')")
            .bind(Uuid::new_v4().to_string()).bind(scope).bind(owner.to_string()).bind(key).execute(&db.pool).await?;
    }
    sqlx::query("INSERT INTO extension_settings (setting_key,value_json) VALUES ($1,'{}')")
        .bind(format!("extensions.control_defaults.{instance}"))
        .execute(&db.pool)
        .await?;
    sqlx::query("INSERT INTO runtime_logs (log_id,instance_id,log_uri) VALUES ($1,$2,'file:///fixture.log')")
        .bind(Uuid::new_v4().to_string()).bind(instance.to_string()).execute(&db.pool).await?;
    Ok(provider)
}

/// The built-in qBittorrent a stack's setup uses (never part of the stack).
async fn seed_qbittorrent(db: &Database) -> Result<(Uuid, Uuid)> {
    ExtensionStore::new(&db.pool)
        .upsert_extension(&NewExtension {
            extension_id: "elixir.modules.qbittorrent".into(),
            name: "qBittorrent".into(),
            version: "1.0.1".into(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: json!({"id":"elixir.modules.qbittorrent","version":"1.0.1","kind":"module","name":"qBittorrent"}),
            package_hash: None,
            enabled: true,
        })
        .await?;
    let instance = Uuid::new_v4();
    sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,'elixir.modules.qbittorrent','default',TRUE)")
        .bind(instance.to_string()).execute(&db.pool).await?;
    let provider = seed_operational(db, instance, "downloader.torrent").await?;
    Ok((instance, provider))
}

async fn qbittorrent_rows(db: &Database, instance: Uuid, provider: Uuid) -> Result<Vec<i64>> {
    Ok(vec![
        count(
            db,
            "SELECT COUNT(*) FROM extensions WHERE extension_id=$1",
            "elixir.modules.qbittorrent",
        )
        .await?,
        count(
            db,
            "SELECT COUNT(*) FROM extension_instances WHERE instance_id=$1",
            &instance.to_string(),
        )
        .await?,
        count(
            db,
            "SELECT COUNT(*) FROM providers WHERE provider_id=$1",
            &provider.to_string(),
        )
        .await?,
        count(
            db,
            "SELECT COUNT(*) FROM secrets WHERE scope_id=$1",
            &instance.to_string(),
        )
        .await?,
        count(
            db,
            "SELECT COUNT(*) FROM secrets WHERE scope_id=$1",
            &provider.to_string(),
        )
        .await?,
        count(
            db,
            "SELECT COUNT(*) FROM extension_settings WHERE setting_key LIKE $1",
            &format!("%{instance}"),
        )
        .await?,
    ])
}

/// The Arr Stack's real `stack` when the repository is present, otherwise a
/// small stack with one app, one connector and a setup using qBittorrent.
fn arr_stack() -> Result<(Value, Value)> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../extensions/marketplace-sources/arr-stack/manifest.yaml"
    );
    if let Ok(text) = std::fs::read_to_string(path) {
        let raw: Value = serde_yaml::from_str(&text)?;
        return Ok((raw["stack"].clone(), raw["permissions"].clone()));
    }
    const IMAGE: &str = "lscr.io/linuxserver/sonarr@sha256:f83bc8426c8260e1901094b43fb9796bd47b3ebd874e4c53a3711ca4df791dcc";
    Ok((
        json!({
            "modules": [{"id": "sonarr", "version": "1.0.0", "kind": "module", "name": "Sonarr",
                "permissions": ["runtime.manage_containers", "network.egress"],
                "provides": [{"capability": "media.manager.tv", "slot": "default", "cardinality": "one", "implementation": "sonarr"}],
                "runtime": {"type": "container", "image": IMAGE, "ports": [{"container": 8989}],
                    "volumes": ["{downloads}:/downloads", "{media}/tv:/tv"]},
                "networking": {"service_port": {"scheme": "http", "container_port": 8989}}}],
            "connectors": [{"id": "sonarr-qbittorrent", "version": "1.0.0", "kind": "connector", "name": "Sonarr to qBittorrent",
                "permissions": ["drivers.configure.media.manager.tv"],
                "targets": [{"capability": "media.manager.tv", "slot": "default"}],
                "actions": [{"type": "driver_patch", "target": {"capability": "media.manager.tv", "slot": "default"},
                    "patch": {"op": "set_downloaders", "downloaders": []}}]}],
            "blueprint": {"id": "setup", "version": "1.0.0", "kind": "blueprint", "name": "Setup",
                "execution": {"packages": ["sonarr", "elixir.modules.qbittorrent", "sonarr-qbittorrent"],
                    "instances": [{"id": "sonarr", "extension_id": "sonarr"},
                        {"id": "qbittorrent", "extension_id": "elixir.modules.qbittorrent"}],
                    "phases": [{"id": "install", "barrier": true, "steps": [
                        {"type": "ensure_package_installed", "extension_id": "sonarr"},
                        {"type": "ensure_package_installed", "extension_id": "sonarr-qbittorrent"}]}]}}
        }),
        json!([
            "runtime.manage_containers",
            "network.egress",
            "drivers.configure.media.manager.tv"
        ]),
    ))
}

/// Gives the installed extension's operational row a container stack (what
/// activation writes for a release that declares one) and lets the stack
/// pass write its members, then does what the stack's setup does: an
/// instance with a provider, secrets and settings for each app, and a
/// binding from the first app to the built-in qBittorrent.
async fn install_stack(
    f: &Fixture,
    id: &str,
    runtime: &Recorder,
    qbittorrent: Uuid,
) -> Result<(usize, BTreeSet<Uuid>)> {
    let (stack, permissions) = arr_stack()?;
    let text: String =
        sqlx::query_scalar("SELECT manifest_json FROM extensions WHERE extension_id=$1")
            .bind(id)
            .fetch_one(&f.db.pool)
            .await?;
    let mut manifest: Value = serde_json::from_str(&text)?;
    manifest["stack"] = stack;
    manifest["permissions"] = permissions;
    let parsed: ExtensionManifest = serde_json::from_value(manifest.clone())?;
    let members =
        stack::materialize(id, &parsed.version, parsed.stack.as_ref().context("stack")?)?.len();
    sqlx::query("UPDATE extensions SET manifest_json=$1 WHERE extension_id=$2")
        .bind(serde_json::to_string(&manifest)?)
        .bind(id)
        .execute(&f.db.pool)
        .await?;
    let summary = stack::reconcile_with(&f.db.pool, runtime).await?;
    assert_eq!(summary.stacks, 1);
    assert_eq!(summary.members_written, members);
    assert_eq!(summary.setups_offered, 1);
    let prefix = stack::member_prefix(id);
    let mut instances = BTreeSet::new();
    let mut bound = false;
    for row in ExtensionStore::new(&f.db.pool).list_extensions().await? {
        if !row.extension_id.starts_with(&prefix) || row.kind != ExtensionKind::Module {
            continue;
        }
        let manifest: ExtensionManifest = serde_json::from_value(row.manifest_json)?;
        let instance = Uuid::new_v4();
        sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,$2,'default',TRUE)")
            .bind(instance.to_string()).bind(&row.extension_id).execute(&f.db.pool).await?;
        let capability = &manifest.provides.first().context("app service")?.capability;
        let provider = seed_operational(&f.db, instance, capability).await?;
        if !bound {
            sqlx::query("INSERT INTO bindings (binding_id,consumer_provider_id,requires_capability,requires_slot_id,target_provider_id) VALUES ($1,$2,'downloader.torrent','default',$3)")
                .bind(Uuid::new_v4().to_string()).bind(provider.to_string()).bind(qbittorrent.to_string()).execute(&f.db.pool).await?;
            bound = true;
        }
        instances.insert(instance);
    }
    assert!(!instances.is_empty());
    Ok((members, instances))
}

async fn assert_stack_gone(db: &Database, id: &str, members: &BTreeSet<Uuid>) -> Result<()> {
    let prefix = format!("{}%", stack::member_prefix(id));
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM extensions WHERE extension_id LIKE $1",
            &prefix
        )
        .await?,
        0
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM extension_instances WHERE extension_id LIKE $1",
            &prefix
        )
        .await?,
        0
    );
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM desired_blueprints WHERE blueprint_extension_id LIKE $1",
            &prefix
        )
        .await?,
        0
    );
    for instance in members {
        let instance = instance.to_string();
        for sql in [
            "SELECT COUNT(*) FROM providers WHERE instance_id=$1",
            "SELECT COUNT(*) FROM secrets WHERE scope_id=$1",
            "SELECT COUNT(*) FROM runtime_logs WHERE instance_id=$1",
        ] {
            assert_eq!(count(db, sql, &instance).await?, 0, "{sql}");
        }
        assert_eq!(
            count(
                db,
                "SELECT COUNT(*) FROM extension_settings WHERE setting_key LIKE $1",
                &format!("%{instance}")
            )
            .await?,
            0
        );
    }
    assert_eq!(
        count(
            db,
            "SELECT COUNT(*) FROM bindings WHERE requires_slot_id=$1",
            "default"
        )
        .await?,
        0
    );
    Ok(())
}

/// Nothing of the extension or its installation is left.
async fn assert_records_gone(f: &Fixture, id: &str, instance: Uuid) -> Result<()> {
    for sql in [
        "SELECT COUNT(*) FROM extensions WHERE extension_id=$1",
        "SELECT COUNT(*) FROM extension_instances WHERE extension_id=$1",
        "SELECT COUNT(*) FROM marketplace_installations WHERE extension_id=$1",
        "SELECT COUNT(*) FROM marketplace_installation_metadata WHERE extension_id=$1",
        "SELECT COUNT(*) FROM marketplace_extension_choices WHERE extension_id=$1",
        "SELECT COUNT(*) FROM marketplace_instance_choices WHERE extension_id=$1",
        "SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE extension_id=$1",
        "SELECT COUNT(*) FROM marketplace_instance_sdk_state WHERE extension_id=$1",
        "SELECT COUNT(*) FROM marketplace_installed_blob_hints WHERE extension_id=$1",
    ] {
        assert_eq!(count(&f.db, sql, id).await?, 0, "{sql}");
    }
    for sql in [
        "SELECT COUNT(*) FROM providers WHERE instance_id=$1",
        "SELECT COUNT(*) FROM secrets WHERE scope_id=$1",
        "SELECT COUNT(*) FROM runtime_logs WHERE instance_id=$1",
    ] {
        assert_eq!(count(&f.db, sql, &instance.to_string()).await?, 0, "{sql}");
    }
    for table in [
        "marketplace_instance_sdk_values",
        "marketplace_generation_secrets",
        "marketplace_sdk_namespaces",
    ] {
        let rows: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&f.db.pool)
            .await?;
        assert_eq!(rows, 0, "{table}");
    }
    // The grant stays as a revoked tombstone at the next revision.
    let grant: (i64, i64, i64) = sqlx::query_as("SELECT revision,revocation_revision,CAST(CASE WHEN enabled OR automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants WHERE extension_id=$1")
        .bind(id).fetch_one(&f.db.pool).await?;
    assert_eq!(grant, (f.revision + 1, f.revision + 1, 0));
    Ok(())
}

/// A namespace value and a generation secret, as a running extension writes.
async fn seed_sdk_state(f: &Fixture, held: &InstalledBinding) -> Result<()> {
    secret(f, held, "fixture-playlist").await?;
    let namespace = held.coordinates().namespace;
    sqlx::query("INSERT INTO marketplace_instance_sdk_values (instance_id,key,revision,value_encrypted,encoded_bytes) VALUES ($1,'cursor',1,'fixture',7)")
        .bind(namespace.to_string()).execute(&f.db.pool).await?;
    Ok(())
}

async fn post_uninstall(app: &axum::Router, token: &str, id: &str) -> Result<(StatusCode, Value)> {
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/marketplace/installations/uninstall")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({ "extension_id": id }).to_string()))?,
        )
        .await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok((status, serde_json::from_slice(&body).unwrap_or(Value::Null)))
}

#[tokio::test]
async fn http_uninstall_removes_a_plain_source_and_it_can_be_installed_again() -> Result<()> {
    let root = tempfile::tempdir()?;
    let url = format!("sqlite://{}", root.path().join("install.sqlite").display());
    let db = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    db.run_migrations().await?;
    let (state, public, principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture(
            db.clone(),
            root.path(),
            &url,
        )
        .await?;
    let app = public
        .merge(crate::http::handlers::marketplace::installation_routes().with_state(state.clone()));
    let actor = Actor::from_principal(&principal, false)?;
    let mut f = Fixture::with_database_and_actor(root, db, Some(actor), "fixture-provider").await?;
    let (instance, held) = configured(&f, "Torrentio").await?;
    let id = extension(&f)?;
    seed_operational(&f.db, instance, "acquisition.candidate_provider").await?;
    seed_sdk_state(&f, &held).await?;
    let before = assert_budget_consistent(&f.db).await?;
    assert_eq!(before.0, 1);

    let (status, body) = post_uninstall(&app, &token, &id).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["extension_id"], id.as_str());
    assert_eq!(body["name"], "Fixture Provider");
    assert_eq!(body["removed_members"], 0);
    assert_records_gone(&f, &id, instance).await?;
    assert_eq!(assert_budget_consistent(&f.db).await?, (0, 0));

    // Gone means 404, as is anything that is not a marketplace install.
    for other in [
        id.as_str(),
        "elixir.modules.qbittorrent",
        "stack.0a1b2c3d.sonarr",
        "nope",
    ] {
        let (status, body) = post_uninstall(&app, &token, other).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{other}: {body}");
        assert_eq!(body["code"], "marketplace_extension_not_installed");
    }
    // Owner authentication like every other installation route.
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/marketplace/installations/uninstall")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "extension_id": id }).to_string()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // A clean reinstall: a fresh owner approval at the next grant revision,
    // then the normal staging and activation path.
    let grant = super::super::super::approve_review_with_binding(
        &f.db,
        &f.actor,
        &f.material()?.review,
        &ApprovalSelection {
            review_id: f.material()?.review.review_id()?,
            expected_revision: f.revision + 1,
            mutation_id: Uuid::new_v4(),
            automatic_updates: false,
        },
        Utc::now(),
        true,
    )
    .await?;
    f.revision = grant.revision();
    let (again, _) = configured(&f, "Torrentio").await?;
    assert_ne!(again, instance);
    assert_eq!(assert_budget_consistent(&f.db).await?.0, 1);
    let (status, body) = post_uninstall(&app, &token, &id).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(assert_budget_consistent(&f.db).await?, (0, 0));
    state.marketplace.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn uninstall_names_only_its_own_archived_releases_and_removes_them() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, _) = configured(&f, "Torrentio").await?;
    let id = extension(&f)?;
    let inventory = uninstall::inventory(&f.db, &id)
        .await?
        .context("installed extension")?;
    assert_eq!(inventory.instances, vec![instance]);
    assert!(!inventory.activating);
    let removed = uninstall::remove(&f.db, &f.actor, &inventory, false).await?;
    assert_eq!(removed.instances, vec![instance]);
    assert_eq!(removed.releases, vec![f.archive.release_id()]);
    let archive = f.root.path().canonicalize()?.join("archive");
    let release = archive
        .join("releases")
        .join(&f.archive.release_id().to_string()[7..]);
    assert!(release.exists());
    assert_eq!(
        crate::marketplace::archive::remove_releases(&archive, removed.releases.clone()).await?,
        1
    );
    assert!(!release.exists());
    assert_eq!(
        crate::marketplace::archive::remove_releases(&archive, removed.releases).await?,
        0
    );
    assert!(uninstall::inventory(&f.db, &id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn uninstalling_a_stack_removes_its_apps_and_leaves_built_ins_alone() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, _) = configured(&f, "Arr Stack").await?;
    let id = extension(&f)?;
    let (qbittorrent, qbittorrent_provider) = seed_qbittorrent(&f.db).await?;
    let runtime = Recorder::default();
    let (members, apps) = install_stack(&f, &id, &runtime, qbittorrent_provider).await?;
    let built_in = qbittorrent_rows(&f.db, qbittorrent, qbittorrent_provider).await?;

    let done = uninstall_with(&f, &id, &runtime, options())
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(done.removed_members, members);
    assert_records_gone(&f, &id, instance).await?;
    assert_stack_gone(&f.db, &id, &apps).await?;
    assert_eq!(assert_budget_consistent(&f.db).await?, (0, 0));
    // The apps' containers and config volumes, and nothing else.
    assert_eq!(runtime.removed(), apps);
    assert!(!runtime.removed().contains(&qbittorrent));
    assert_eq!(
        qbittorrent_rows(&f.db, qbittorrent, qbittorrent_provider).await?,
        built_in
    );

    // Neither the stack pass nor a restart's pass brings anything back.
    assert_eq!(
        stack::reconcile_with(&f.db.pool, &runtime).await?,
        StackReconcile::default()
    );
    assert_stack_gone(&f.db, &id, &apps).await?;
    assert_eq!(
        count(
            &f.db,
            "SELECT COUNT(*) FROM extensions WHERE extension_id=$1",
            &id
        )
        .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn a_failure_before_commit_leaves_a_source_installed_and_working() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, held) = configured(&f, "Torrentio").await?;
    let id = extension(&f)?;
    seed_operational(&f.db, instance, "acquisition.candidate_provider").await?;
    seed_sdk_state(&f, &held).await?;
    let runtime = Recorder::default();
    let before = snapshot(&f.db).await?;

    let failed = uninstall_with(&f, &id, &runtime, options().failing_before_commit()).await;
    assert!(
        matches!(failed, Err(UninstallError::Failed(_))),
        "{failed:?}"
    );
    assert_eq!(snapshot(&f.db).await?, before, "nothing may change");
    // Still a working installation: its ACTIVE binding loads as before.
    InstalledBinding::load_configured(
        &f.db,
        f.actor.home,
        held.coordinates().extension,
        instance,
        limit(),
    )
    .await?;

    uninstall_with(&f, &id, &runtime, options())
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_records_gone(&f, &id, instance).await?;
    assert_eq!(assert_budget_consistent(&f.db).await?, (0, 0));
    Ok(())
}

#[tokio::test]
async fn a_failure_before_commit_leaves_the_stack_and_its_apps_in_place() -> Result<()> {
    let f = Fixture::new().await?;
    let (instance, _) = configured(&f, "Arr Stack").await?;
    let id = extension(&f)?;
    let (_, qbittorrent_provider) = seed_qbittorrent(&f.db).await?;
    let runtime = Recorder::default();
    let (members, apps) = install_stack(&f, &id, &runtime, qbittorrent_provider).await?;
    let before = snapshot(&f.db).await?;

    let failed = uninstall_with(&f, &id, &runtime, options().failing_before_commit()).await;
    assert!(
        matches!(failed, Err(UninstallError::Failed(_))),
        "{failed:?}"
    );
    assert_eq!(snapshot(&f.db).await?, before, "nothing may change");
    assert!(runtime.removed().is_empty(), "no app was touched");
    // The stack pass still finds the stack and leaves its apps as they are.
    let pass = stack::reconcile_with(&f.db.pool, &runtime).await?;
    assert_eq!(
        (
            pass.stacks,
            pass.members_written,
            pass.members_removed,
            pass.members_disabled
        ),
        (1, 0, 0, 0)
    );

    let done = uninstall_with(&f, &id, &runtime, options())
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    assert_eq!(done.removed_members, members);
    assert_records_gone(&f, &id, instance).await?;
    assert_stack_gone(&f.db, &id, &apps).await?;
    assert_eq!(runtime.removed(), apps);
    Ok(())
}

#[tokio::test]
async fn uninstall_waits_for_setup_and_refuses_an_activation_in_progress() -> Result<()> {
    let f = Fixture::new().await?;
    let (_, _) = configured(&f, "Arr Stack").await?;
    let id = extension(&f)?;
    let runtime = Recorder::default();
    let before = snapshot(&f.db).await?;

    // A setup run (or reconcile pass) holds the orchestrator's apply lock.
    let store = ExtensionStore::new(&f.db.pool);
    assert!(
        store
            .acquire_lock(APPLY_LOCK_NAME, "setup-run", Duration::from_secs(600))
            .await?
    );
    let options = UninstallOptions::new(Duration::from_millis(300), Duration::from_secs(60));
    let busy = uninstall_with(&f, &id, &runtime, options).await;
    assert!(matches!(busy, Err(UninstallError::Busy(_))), "{busy:?}");
    assert_eq!(snapshot(&f.db).await?, before);
    store.release_lock(APPLY_LOCK_NAME, "setup-run").await?;

    // A journal between activation phases.
    let json: String = sqlx::query_scalar(
        "SELECT record_json FROM marketplace_installations WHERE extension_id=$1",
    )
    .bind(&id)
    .fetch_one(&f.db.pool)
    .await?;
    let mut fenced: Value = serde_json::from_str(&json)?;
    fenced["activation"] = json!({"phase": "quiescing"});
    sqlx::query("UPDATE marketplace_installations SET record_json=$1 WHERE extension_id=$2")
        .bind(serde_json::to_string(&fenced)?)
        .bind(&id)
        .execute(&f.db.pool)
        .await?;
    let busy = uninstall_with(&f, &id, &runtime, self::options()).await;
    assert!(matches!(busy, Err(UninstallError::Busy(_))), "{busy:?}");
    sqlx::query("UPDATE marketplace_installations SET record_json=$1 WHERE extension_id=$2")
        .bind(&json)
        .bind(&id)
        .execute(&f.db.pool)
        .await?;
    assert_eq!(snapshot(&f.db).await?, before);

    uninstall_with(&f, &id, &runtime, self::options())
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    Ok(())
}

#[tokio::test]
async fn one_uninstall_at_a_time_and_nothing_new_starts_meanwhile() -> Result<()> {
    let service = crate::marketplace::application::Service::default();
    let held = service
        .begin_uninstall()
        .await
        .map_err(|busy| anyhow::anyhow!("{}", busy.0))?;
    assert!(!service.background_idle()?);
    assert!(service.begin_uninstall().await.is_err());
    drop(held);
    service
        .begin_uninstall()
        .await
        .map_err(|busy| anyhow::anyhow!("{}", busy.0))?;
    Ok(())
}

#[tokio::test]
async fn a_stack_that_no_longer_exists_loses_its_apps_and_a_switched_off_one_keeps_them()
-> Result<()> {
    let root = tempfile::tempdir()?;
    let db = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", root.path().join("orphans.sqlite").display()),
        max_connections: 2,
        connect_timeout_seconds: 5,
    })
    .await?;
    db.run_migrations().await?;
    let store = ExtensionStore::new(&db.pool);
    let row = |id: &str, kind: ExtensionKind, enabled: bool| NewExtension {
        extension_id: id.into(),
        name: id.into(),
        version: "1.0.0".into(),
        kind,
        publisher_name: None,
        signing_key_id: None,
        trust_level: ExtensionTrustLevel::Community,
        manifest_json: json!({}),
        package_hash: None,
        enabled,
    };
    // A stack whose `elx:` extension was deleted without its members (the
    // old generic uninstall did this).
    let gone = stack::member_prefix("elx:gone/arr-stack");
    store
        .upsert_extension(&row(&format!("{gone}sonarr"), ExtensionKind::Module, true))
        .await?;
    store
        .upsert_extension(&row(
            &format!("{gone}setup"),
            ExtensionKind::Blueprint,
            false,
        ))
        .await?;
    sqlx::query("INSERT INTO desired_blueprints (desired_id,blueprint_extension_id,blueprint_version) VALUES ($1,$2,'1.0.0')")
        .bind(Uuid::new_v4().to_string()).bind(format!("{gone}setup")).execute(&db.pool).await?;
    let app = Uuid::new_v4();
    sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,$2,'default',TRUE)")
        .bind(app.to_string()).bind(format!("{gone}sonarr")).execute(&db.pool).await?;
    seed_operational(&db, app, "media.manager.tv").await?;
    // A stack that is installed but switched off keeps its (disabled) apps.
    store
        .upsert_extension(&row("elx:kept/arr-stack", ExtensionKind::Module, false))
        .await?;
    let kept = format!("{}sonarr", stack::member_prefix("elx:kept/arr-stack"));
    store
        .upsert_extension(&row(&kept, ExtensionKind::Module, true))
        .await?;
    let (qbittorrent, qbittorrent_provider) = seed_qbittorrent(&db).await?;
    let built_in = qbittorrent_rows(&db, qbittorrent, qbittorrent_provider).await?;

    let runtime = Recorder::default();
    // While a setup run holds the orchestrator's lock the apps are only
    // switched off; the next pass removes them.
    assert!(
        store
            .acquire_lock(APPLY_LOCK_NAME, "setup-run", Duration::from_secs(600))
            .await?
    );
    let summary = stack::reconcile_with(&db.pool, &runtime).await?;
    assert_eq!((summary.members_removed, summary.members_disabled), (0, 2));
    assert!(runtime.removed().is_empty());
    assert!(
        !store
            .get_extension(&format!("{gone}sonarr"))
            .await?
            .context("deferred member")?
            .enabled
    );
    store.release_lock(APPLY_LOCK_NAME, "setup-run").await?;
    let summary = stack::reconcile_with(&db.pool, &runtime).await?;
    assert_eq!((summary.members_removed, summary.members_disabled), (2, 0));
    assert_eq!(runtime.removed(), BTreeSet::from([app]));
    let like = format!("{gone}%");
    for sql in [
        "SELECT COUNT(*) FROM extensions WHERE extension_id LIKE $1",
        "SELECT COUNT(*) FROM extension_instances WHERE extension_id LIKE $1",
        "SELECT COUNT(*) FROM desired_blueprints WHERE blueprint_extension_id LIKE $1",
    ] {
        assert_eq!(count(&db, sql, &like).await?, 0, "{sql}");
    }
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM providers WHERE instance_id=$1",
            &app.to_string()
        )
        .await?,
        0
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM secrets WHERE scope_id=$1",
            &app.to_string()
        )
        .await?,
        0
    );
    assert!(
        !store
            .get_extension(&kept)
            .await?
            .context("kept member")?
            .enabled
    );
    assert_eq!(
        qbittorrent_rows(&db, qbittorrent, qbittorrent_provider).await?,
        built_in
    );
    // Idempotent.
    assert_eq!(
        stack::reconcile_with(&db.pool, &runtime).await?,
        StackReconcile::default()
    );
    Ok(())
}

#[tokio::test]
async fn a_retiring_executor_refuses_calls_until_its_uninstall_ends() -> Result<()> {
    use elixir_marketplace::source_execution::RuntimeJson;
    let f = Fixture::new().await?;
    let (instance, held) = configured(&f, "Arr Stack").await?;
    let configuration = super::super::runtime::tests::configuration(
        &f,
        "registry.invalid/elixir-worker@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        "uninstall-retire",
    )?;
    let host = super::super::serve::ServeHost::new(
        f.db.clone(),
        crate::secrets::SecretsManager::from_key_bytes([57; 32], false),
        configuration,
        None,
    );
    let extension = held.coordinates().extension.clone();
    let call = || async {
        host.call(
            f.actor.home,
            &extension,
            instance,
            limit(),
            RuntimeJson::from_value(json!({}))?,
        )
        .await
        .map(|_| ())
    };
    host.retire(&[instance], Duration::from_secs(1)).await?;
    let refused = call().await.expect_err("a retiring instance takes no call");
    assert!(
        refused.to_string().contains("being uninstalled"),
        "{refused:#}"
    );
    // A failed uninstall gives the extension back: calls are admitted again
    // (this one then fails later, at the fixture's unavailable image).
    host.finish_retirement(&[instance], false);
    let admitted = call().await.expect_err("fixture image is unavailable");
    assert!(
        !admitted.to_string().contains("being uninstalled"),
        "{admitted:#}"
    );
    Ok(())
}
