//! Real migrated SQLite/secrets; runtime double does read-only local image lookup.
//! These tests establish configuration binding, not VPN readiness or network authority.
use super::*;
use crate::{
    config::Settings,
    db::models::SecretScope,
    extensions::store::{ExtensionStore, NewSecret},
    runtime::model::{ContainerHandle, ContainerSpec, ContainerState},
};
use std::{
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use uuid::Uuid;

const WG: &str = "[Interface]\nPrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=\nAddress = 10.64.0.2/32,fd00::2/128\nDNS = 10.64.0.1\n[Peer]\nPublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=\nAllowedIPs = 0.0.0.0/0,::/0\nEndpoint = 1.1.1.1:51820\n";
const OVPN: &str = "client\ndev tun\nproto udp\nremote 1.1.1.1 1194\nresolv-retry infinite\nnobind\npull\nremote-cert-tls server\nverify-x509-name vpn.example name\ntls-version-min 1.2\nauth-user-pass old.txt\n<ca>\ncertificate\n</ca>\n<tls-crypt>\nkey\n</tls-crypt>\n";

#[tokio::test]
async fn original_protection_context_binds_material_frame_and_revokes_after_secret_rotation()
-> Result<()> {
    use crate::marketplace::application_egress::Protection;
    use elixir_marketplace::identity::Digest;
    use std::sync::Arc;
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    let secrets = Arc::new(secrets);
    let runtime = Arc::new(runtime);
    let context = Protection::capture(db.clone(), secrets.clone(), runtime)
        .await?
        .context("accepted context")?;
    let (profile, input) = context
        .prepare(
            "fixture",
            &"b".repeat(64),
            Digest::of(b"source"),
            Digest::of(b"invocation"),
        )
        .await?;
    assert!(input.body().starts_with(b"[Interface]"));
    assert_eq!(input.trailer(), b"ELIXIR_GATEWAY_MATERIAL_V1");
    assert!(!serde_json::to_string(profile.spec())?.contains("yAnzdtF2rM8Nl1N8MPm"));
    put(&db, &secrets, "config", &WG.replace("51820", "51821")).await?;
    assert!(context.current().await.is_err());
    Ok(())
}

#[tokio::test]
async fn accepted_selection_compiles_only_its_matching_ephemeral_gateway_material() -> Result<()> {
    use crate::marketplace::application_egress::gateway::ApplicationGatewayProfile;
    use elixir_marketplace::identity::Digest;
    for kind in ["wireguard_config", "openvpn_config", "cloudflare_warp"] {
        let (db, secrets, runtime) = fixture(kind).await?;
        if kind == "openvpn_config" {
            put(&db, &secrets, "username", "fixture-user").await?;
            put(&db, &secrets, "password", "fixture-password").await?;
            reference(&db, "openvpn_username", "global:username").await?;
            reference(&db, "openvpn_password", "global:password").await?;
        }
        let selected = Selection::capture(&db, &secrets, &runtime)
            .await?
            .context("selected fixture")?;
        let files = selected.material(&db, &secrets, &runtime).await?;
        let profile = ApplicationGatewayProfile::gluetun(
            "fixture",
            &"b".repeat(64),
            &selected,
            &files,
            Digest::of(b"source"),
            Digest::of(b"invocation"),
        );
        if kind == "cloudflare_warp" {
            // WARP is not silently represented as a Gluetun/direct profile.
            assert!(profile.is_err());
        } else {
            let profile = profile?;
            assert_eq!(profile.spec().image, selected.image_id());
            assert!(!profile.input(&selected.profile().kind, &files)?.is_empty());
            selected.current(&db, &secrets, &runtime).await?;
        }
    }
    Ok(())
}

struct Runtime {
    image: Mutex<String>,
    lookups: Mutex<Vec<String>>,
    hold_call: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Runtime {
    fn new() -> Self {
        Self {
            image: Mutex::new(format!("sha256:{}", "a".repeat(64))),
            lookups: Mutex::default(),
            hold_call: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
    fn calls(&self) -> usize {
        self.lookups.lock().unwrap().len()
    }
}
fn unused<T>() -> Result<T> {
    bail!("unexpected effect in configuration selection fixture")
}
#[async_trait::async_trait]
impl RuntimeManager for Runtime {
    async fn local_image_id(&self, reference: &str) -> Result<String> {
        let call = {
            let mut calls = self.lookups.lock().unwrap();
            calls.push(reference.to_owned());
            calls.len()
        };
        if self.hold_call.load(Ordering::SeqCst) == call {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(self.image.lock().unwrap().clone())
    }
    async fn ensure_network(&self, _: &str) -> Result<()> {
        unused()
    }
    async fn ensure_container(&self, _: &ContainerSpec) -> Result<ContainerHandle> {
        unused()
    }
    async fn get_container_handle(&self, _: &str) -> Result<Option<ContainerHandle>> {
        unused()
    }
    async fn start_container(&self, _: &ContainerHandle) -> Result<()> {
        unused()
    }
    async fn stop_container(&self, _: &ContainerHandle) -> Result<()> {
        unused()
    }
    async fn rename_container(&self, _: &ContainerHandle, _: &str) -> Result<ContainerHandle> {
        unused()
    }
    async fn remove_container(&self, _: &ContainerHandle) -> Result<()> {
        unused()
    }
    async fn container_logs(
        &self,
        _: &ContainerHandle,
        _: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
        unused()
    }
    async fn inspect(&self, _: &ContainerHandle) -> Result<ContainerState> {
        unused()
    }
    async fn read_container_file(&self, _: &ContainerHandle, _: &str) -> Result<Option<Vec<u8>>> {
        unused()
    }
    async fn copy_host_path_to_container(
        &self,
        _: &ContainerHandle,
        _: &Path,
        _: &str,
    ) -> Result<()> {
        unused()
    }
    async fn ensure_container_directories(&self, _: &ContainerHandle, _: &[String]) -> Result<()> {
        unused()
    }
    async fn ensure_container_directories_owned_like(
        &self,
        _: &ContainerHandle,
        _: &str,
        _: &[String],
    ) -> Result<bool> {
        unused()
    }
}
async fn database() -> Result<Database> {
    let mut settings = Settings::default();
    settings.database.url = "sqlite::memory:?cache=shared".into();
    settings.database.max_connections = 1;
    settings.database.connect_timeout_seconds = 5;
    sqlx::any::install_default_drivers();
    let db = Database::connect(&settings.database).await?;
    db.run_migrations().await?;
    Ok(db)
}
async fn put(db: &Database, secrets: &SecretsManager, key: &str, value: &str) -> Result<()> {
    ExtensionStore::new(&db.pool)
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: key.into(),
            value_encrypted: secrets.encrypt(value)?,
            rotatable: true,
        })
        .await
}
async fn reference(db: &Database, key: &str, value: &str) -> Result<()> {
    sqlx::query("INSERT INTO download_network_profile_secrets (profile_id,key,secret_ref) VALUES ('selected',$1,$2)")
        .bind(key).bind(value).execute(&db.pool).await?;
    Ok(())
}
async fn fixture(kind: &str) -> Result<(Database, SecretsManager, Runtime)> {
    let db = database().await?;
    let secrets = SecretsManager::from_key_bytes([91; 32], false);
    sqlx::query("INSERT INTO download_network_profiles (id,name,kind,enabled,strict,scope,gateway_runtime,config_json,status,active) VALUES ('selected','Selected VPN',$1,TRUE,TRUE,'marketplace_http',$2,'{}','protected',FALSE)")
        .bind(kind).bind(match kind { "wireguard_config" => "gluetun_wireguard", "openvpn_config" => "gluetun_openvpn", "cloudflare_warp" => "cloudflare_warp", _ => "none" })
        .execute(&db.pool).await?;
    match kind {
        "wireguard_config" | "openvpn_config" => {
            put(
                &db,
                &secrets,
                "config",
                if kind == "wireguard_config" { WG } else { OVPN },
            )
            .await?;
            reference(&db, kind, "global:config").await?;
            if kind == "wireguard_config" {
                use crate::network::vpn_identity::{self, Purpose};
                let keys = vpn_identity::wireguard_keys(WG)?
                    .into_iter()
                    .map(|key| (key, Purpose::MarketplaceHttp))
                    .collect::<Vec<_>>();
                vpn_identity::reserve(&db.pool, &keys).await?;
            }
        }
        "cloudflare_warp" => {
            put(&db, &secrets, "identity", "local-enrollment-secret-fixture").await?;
            sqlx::query("INSERT INTO download_warp_enrollments (id,profile_id,enrollment_id,identity_secret_ref,status,disclosure_version) VALUES ('row','selected','fixture-enrollment','global:identity','ready',$1)")
                .bind(cloudflare_warp_disclosure().version).execute(&db.pool).await?;
        }
        _ => {}
    }
    Ok((db, secrets, Runtime::new()))
}
async fn capture(db: &Database, secrets: &SecretsManager, runtime: &Runtime) -> Result<Selection> {
    Selection::capture(db, secrets, runtime)
        .await?
        .context("fixture should be eligible")
}

#[tokio::test]
async fn no_selected_profile_never_touches_runtime() -> Result<()> {
    let db = database().await?;
    let secrets = SecretsManager::from_key_bytes([91; 32], false);
    let runtime = Runtime::new();
    assert!(Selection::capture(&db, &secrets, &runtime).await?.is_none());
    assert_eq!(runtime.calls(), 0);
    Ok(())
}
#[tokio::test]
async fn direct_disabled_nonstrict_unprotected_and_missing_config_do_not_select() -> Result<()> {
    for change in [
        "kind = 'direct'",
        "enabled = FALSE",
        "strict = FALSE",
        "status = 'blocked'",
        "active = TRUE",
    ] {
        let (db, secrets, runtime) = fixture("wireguard_config").await?;
        sqlx::query(&format!("UPDATE download_network_profiles SET {change}"))
            .execute(&db.pool)
            .await?;
        assert!(
            Selection::capture(&db, &secrets, &runtime).await?.is_none(),
            "{change}"
        );
        assert_eq!(runtime.calls(), 0);
    }
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    sqlx::query("DELETE FROM download_network_profile_secrets")
        .execute(&db.pool)
        .await?;
    assert!(Selection::capture(&db, &secrets, &runtime).await?.is_none());
    assert_eq!(runtime.calls(), 0);
    Ok(())
}
#[tokio::test]
async fn unchanged_values_reencrypt_but_rotated_contents_invalidate_same_reference() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    assert_eq!(selected.profile().profile_id, "selected");
    assert_eq!(selected.image_id(), format!("sha256:{}", "a".repeat(64)));
    selected.current(&db, &secrets, &runtime).await?;
    put(&db, &secrets, "config", WG).await?; // encryption nonce changes, contents do not
    selected.current(&db, &secrets, &runtime).await?;
    put(&db, &secrets, "config", &WG.replace("51820", "51821")).await?;
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    assert!(selected.material(&db, &secrets, &runtime).await.is_err());
    Ok(())
}
#[tokio::test]
async fn selected_profile_edits_revoke_existing_selection() -> Result<()> {
    for change in [
        "enabled = FALSE",
        "strict = FALSE",
        "status = 'blocked'",
        "active = TRUE",
        "name = 'Changed'",
        "kind = 'direct'",
        "config_json = '{\"gatewayImage\":\"different:tag\"}'",
    ] {
        let (db, secrets, runtime) = fixture("wireguard_config").await?;
        let selected = capture(&db, &secrets, &runtime).await?;
        sqlx::query(&format!("UPDATE download_network_profiles SET {change}"))
            .execute(&db.pool)
            .await?;
        assert!(
            selected.current(&db, &secrets, &runtime).await.is_err(),
            "{change}"
        );
    }
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    put(&db, &secrets, "other", WG).await?;
    sqlx::query("UPDATE download_network_profile_secrets SET secret_ref='global:other'")
        .execute(&db.pool)
        .await?;
    assert!(
        selected.current(&db, &secrets, &runtime).await.is_err(),
        "same bytes under another reference are a new choice"
    );
    Ok(())
}
#[tokio::test]
async fn mutable_runtime_ids_and_tag_retargeting_cannot_survive() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    for invalid in [
        "image:tag".into(),
        "a".repeat(64),
        format!("sha256:{}", "A".repeat(64)),
        "sha256:short".into(),
    ] {
        *runtime.image.lock().unwrap() = invalid;
        assert!(Selection::capture(&db, &secrets, &runtime).await.is_err());
    }
    *runtime.image.lock().unwrap() = format!("sha256:{}", "a".repeat(64));
    let selected = capture(&db, &secrets, &runtime).await?;
    *runtime.image.lock().unwrap() = format!("sha256:{}", "b".repeat(64));
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    sqlx::query("UPDATE download_network_profiles SET config_json=$1")
        .bind(format!(
            "{{\"gatewayImage\":\"sha256:{}\"}}",
            "a".repeat(64)
        ))
        .execute(&db.pool)
        .await?;
    assert!(
        Selection::capture(&db, &secrets, &runtime).await.is_err(),
        "pinned reference must resolve to that exact image"
    );
    Ok(())
}
#[tokio::test]
async fn exact_image_id_avoids_repeated_resolution_but_never_caches_configuration() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    let image = format!("sha256:{}", "a".repeat(64));
    sqlx::query("UPDATE download_network_profiles SET config_json=$1")
        .bind(serde_json::json!({"gatewayImage":image}).to_string())
        .execute(&db.pool)
        .await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    assert_eq!(
        runtime.calls(),
        1,
        "capture must resolve the real local image once"
    );
    for _ in 0..3 {
        selected.current(&db, &secrets, &runtime).await?;
    }
    assert_eq!(selected.image_id(), image);
    assert_eq!(runtime.calls(), 1, "no mutable tag exists to resolve again");
    put(&db, &secrets, "config", &WG.replace("51820", "51821")).await?;
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    assert_eq!(
        runtime.calls(),
        1,
        "secret rotation must still be checked in SQL"
    );
    put(&db, &secrets, "config", WG).await?;
    sqlx::query("UPDATE download_network_profiles SET config_json=$1")
        .bind(serde_json::json!({"gatewayImage":format!("sha256:{}", "b".repeat(64))}).to_string())
        .execute(&db.pool)
        .await?;
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    assert_eq!(
        runtime.calls(),
        1,
        "a changed pinned choice must revoke, not resolve/rebind"
    );
    Ok(())
}
#[tokio::test]
async fn configuration_is_rechecked_after_each_runtime_await() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    runtime.hold_call.store(1, Ordering::SeqCst);
    let (result, changed) = tokio::join!(Selection::capture(&db, &secrets, &runtime), async {
        runtime.entered.notified().await;
        let changed = put(&db, &secrets, "config", &WG.replace("51820", "51821")).await;
        runtime.release.notify_one();
        changed
    });
    changed?;
    assert!(result.is_err());
    let selected = capture(&db, &secrets, &runtime).await?;
    runtime
        .hold_call
        .store(runtime.calls() + 1, Ordering::SeqCst);
    let (result, changed) = tokio::join!(selected.current(&db, &secrets, &runtime), async {
        runtime.entered.notified().await;
        let changed = put(&db, &secrets, "config", WG).await;
        runtime.release.notify_one();
        changed
    });
    changed?;
    assert!(result.is_err());
    Ok(())
}
#[tokio::test]
async fn wireguard_material_uses_existing_safe_normalizer_and_redacted_errors() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    let files = selected.material(&db, &secrets, &runtime).await?;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_name, "wg0.conf");
    assert!(std::str::from_utf8(files[0].contents())?.contains("1.1.1.1:51820"));
    for bad in [
        WG.replace("[Peer]", "PostUp = forbidden-secret-command\n[Peer]"),
        WG.replace("1.1.1.1:51820", "127.0.0.1:51820"),
    ] {
        put(&db, &secrets, "config", &bad).await?;
        let selected = capture(&db, &secrets, &runtime).await?;
        let error = selected
            .material(&db, &secrets, &runtime)
            .await
            .err()
            .context("unsafe material must fail")?;
        assert_eq!(format!("{error:#}"), "selected VPN material is invalid");
    }
    Ok(())
}
#[tokio::test]
async fn material_caps_and_nul_checks_apply_before_image_lookup() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    for value in ["x".repeat(MATERIAL_LIMIT + 1), "private\0value".into()] {
        put(&db, &secrets, "config", &value).await?;
        let error = Selection::capture(&db, &secrets, &runtime)
            .await
            .err()
            .context("invalid material must fail")?;
        assert_eq!(
            error.to_string(),
            "selected protection material exceeds its bounds"
        );
    }
    assert_eq!(runtime.calls(), 0);
    Ok(())
}
#[tokio::test]
async fn openvpn_requires_paired_auth_and_rotation_revokes_the_selection() -> Result<()> {
    let (db, secrets, runtime) = fixture("openvpn_config").await?;
    put(&db, &secrets, "username", "fixture-user").await?;
    reference(&db, "openvpn_username", "global:username").await?;
    assert!(Selection::capture(&db, &secrets, &runtime).await?.is_none());
    assert_eq!(runtime.calls(), 0);
    put(&db, &secrets, "password", "fixture-password").await?;
    reference(&db, "openvpn_password", "global:password").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    let files = selected.material(&db, &secrets, &runtime).await?;
    assert_eq!(files.len(), 3);
    assert_eq!(files[1].contents(), b"fixture-user");
    assert_eq!(files[2].contents(), b"fixture-password");
    put(&db, &secrets, "password", "rotated-password").await?;
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    put(&db, &secrets, "password", &"x".repeat(AUTH_LIMIT + 1)).await?;
    assert!(Selection::capture(&db, &secrets, &runtime).await.is_err());
    Ok(())
}
#[tokio::test]
async fn warp_requires_current_recorded_acceptance_and_exact_identity_contents() -> Result<()> {
    let (db, secrets, runtime) = fixture("cloudflare_warp").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    assert_eq!(
        selected.profile().enrollment_id.as_deref(),
        Some("fixture-enrollment")
    );
    assert!(selected.material(&db, &secrets, &runtime).await?.is_empty());
    put(&db, &secrets, "identity", "rotated-enrollment-secret").await?;
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    let selected = capture(&db, &secrets, &runtime).await?;
    sqlx::query("UPDATE download_warp_enrollments SET disclosure_version='old'")
        .execute(&db.pool)
        .await?;
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    assert!(Selection::capture(&db, &secrets, &runtime).await.is_err());
    sqlx::query("UPDATE download_warp_enrollments SET disclosure_version=$1,status='pending'")
        .bind(cloudflare_warp_disclosure().version)
        .execute(&db.pool)
        .await?;
    assert!(Selection::capture(&db, &secrets, &runtime).await?.is_none());
    Ok(())
}
#[tokio::test]
async fn missing_unreadable_warp_identity_never_touches_runtime() -> Result<()> {
    let (db, secrets, runtime) = fixture("cloudflare_warp").await?;
    sqlx::query("DELETE FROM secrets").execute(&db.pool).await?;
    let error = Selection::capture(&db, &secrets, &runtime)
        .await
        .err()
        .context("missing identity must fail")?;
    assert_eq!(
        format!("{error:#}"),
        "selected protection material is unavailable"
    );
    assert_eq!(runtime.calls(), 0);
    put(&db, &secrets, "identity", "fixture").await?;
    sqlx::query("UPDATE secrets SET value_encrypted='not-an-encrypted-value'")
        .execute(&db.pool)
        .await?;
    assert!(Selection::capture(&db, &secrets, &runtime).await.is_err());
    assert_eq!(runtime.calls(), 0);
    Ok(())
}

#[tokio::test]
async fn legacy_duplicate_records_are_rejected_not_treated_as_old_credentials() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    // Existing corrupt data, not something the repaired production upsert creates.
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'global',NULL,'config',$2,TRUE)")
        .bind(Uuid::new_v4().to_string()).bind(secrets.encrypt(&WG.replace("51820","51821"))?)
        .execute(&db.pool).await?;
    let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM secrets WHERE scope='global' AND scope_id IS NULL AND key='config'")
        .fetch_one(&db.pool).await?.try_get("n")?;
    assert_eq!(
        count, 2,
        "regression models existing ambiguous nullable-scope data"
    );
    let calls = runtime.calls();
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    assert!(Selection::capture(&db, &secrets, &runtime).await?.is_none());
    assert_eq!(
        runtime.calls(),
        calls,
        "ambiguity is rejected before runtime"
    );
    Ok(())
}

#[tokio::test]
async fn multiple_active_profiles_are_not_resolved_by_timestamp_or_row_order() -> Result<()> {
    let (db, secrets, runtime) = fixture("wireguard_config").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    sqlx::query("INSERT INTO download_network_profiles (id,name,kind,enabled,strict,scope,config_json,status,active) VALUES ('other','Other','direct',TRUE,TRUE,'managed_downloaders','{}','direct',TRUE)")
        .execute(&db.pool).await?;
    sqlx::query("INSERT INTO download_network_profiles (id,name,kind,enabled,strict,scope,config_json,status,active) VALUES ('another','Another','direct',TRUE,TRUE,'managed_downloaders','{}','direct',TRUE)")
        .execute(&db.pool).await?;
    let calls = runtime.calls();
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    assert!(Selection::capture(&db, &secrets, &runtime).await.is_err());
    assert_eq!(runtime.calls(), calls);
    Ok(())
}

#[tokio::test]
async fn tied_latest_warp_enrollments_are_not_resolved_by_row_order() -> Result<()> {
    let (db, secrets, runtime) = fixture("cloudflare_warp").await?;
    let selected = capture(&db, &secrets, &runtime).await?;
    sqlx::query("INSERT INTO download_warp_enrollments (id,profile_id,enrollment_id,identity_secret_ref,status,disclosure_version,created_at) SELECT 'row2',profile_id,'other-enrollment',identity_secret_ref,status,disclosure_version,created_at FROM download_warp_enrollments WHERE id='row'")
        .execute(&db.pool).await?;
    let calls = runtime.calls();
    assert!(selected.current(&db, &secrets, &runtime).await.is_err());
    assert!(Selection::capture(&db, &secrets, &runtime).await.is_err());
    assert_eq!(runtime.calls(), calls);
    Ok(())
}
