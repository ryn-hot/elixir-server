use super::*;
use crate::{
    config::DatabaseConfig,
    db::Database,
    marketplace::{
        accounting::reserve_transfer, connectivity::BOOTSTRAP_PROBE_BYTES,
        provider::TRANSPORT_BOOTSTRAP_BYTES,
    },
};
use elixir_marketplace::policy::OrdinaryLimits;
mod wan;

#[tokio::test]
#[ignore = "explicit approved free provider + protected DNS/TLS, actual namespace guard and child-first recovery"]
async fn provider_handoff_retains_original_namespace_and_child_failure_blocks_parent_replacement()
-> Result<()> {
    handoff(None).await
}

#[tokio::test]
#[ignore = "approved free provider + durable worker handoff, actual protected worker DNS/TLS and continuous supervision"]
async fn provider_worker_is_supervised_and_child_cleanup_failure_holds_both_ancestors() -> Result<()>
{
    let image = std::env::var("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    ensure!(
        image
            .strip_prefix("sha256:")
            .is_some_and(crate::runtime::packet_network::immutable_id),
        "immutable worker image required"
    );
    handoff(Some(image)).await
}

async fn handoff(worker_image: Option<String>) -> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1"),
        "explicit free-provider test consent required"
    );
    let guard_image = std::env::var("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    ensure!(
        guard_image
            .strip_prefix("sha256:")
            .is_some_and(crate::runtime::packet_network::immutable_id),
        "immutable guard image required"
    );
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let image = image()?;
    let root = PreservedRoot::new()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            root.path().join("provider-health.sqlite").display()
        ),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../../../migrations/0060_marketplace_transfer_accounting.sql"
    ))
    .execute(&database.pool)
    .await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let limits = OrdinaryLimits::default();
    let directory = root.path().canonicalize()?.join("provider");
    let deployment = format!("guard-owner-{}", uuid::Uuid::new_v4().simple());
    let scope = Scope {
        deployment: Profile::deployment_scope(&deployment),
        enrollment: Digest::of(uuid::Uuid::new_v4().as_bytes()),
    };
    let secrets = Arc::new(SecretsManager::from_key_bytes([97; 32], false));
    let network_spec = Profile::network_spec(&deployment)?;
    let network = runtime.ensure_packet_uplink(&network_spec).await?;
    network_spec.verify(&network)?;
    root.describe(
        &deployment,
        scope,
        &image,
        &network.id,
        "native-provider-guard",
    )?;
    let mut emergency = Emergency {
        actors: vec![],
        network: network_spec,
        network_id: network.id.clone(),
    };
    let mut interrupted: Option<(String, String)> = None;
    let mut identity = None;
    let mut health_outcome = Ok(());
    for phase in 0..2 {
        let mut owner = Owner::launch(
            runtime.clone(),
            directory.clone(),
            deployment.clone(),
            image.clone(),
            scope,
            phase != 0,
            secrets.clone(),
        )
        .wait()
        .await?;
        if let Some((parent, child)) = interrupted.take() {
            ensure!(
                runtime.get_container_handle(&child).await?.is_none()
                    && runtime.get_container_handle(&parent).await?.is_none(),
                "replacement left original native owners behind"
            );
        }
        emergency.actors.push((
            owner.handle().clone(),
            owner.profile().spec().labels.clone(),
        ));
        let saved = provider_state::load(&owner.lease, &secrets, scope, image.parse()?)?
            .context("encrypted identity missing")?;
        let state = State::decode(saved.bytes, scope)?;
        if let Some(prior) = identity {
            ensure!(
                state.identity() == prior,
                "guard recovery changed provider identity"
            );
        } else {
            identity = Some(state.identity());
        }
        owner.connect().await?;
        let mut protected = false;
        for _ in 0..6 {
            owner.inspect().await?;
            let probe=docker(&["exec",&owner.handle().id,"/bin/bash","-c","set -o pipefail; curl --noproxy '*' --fail --silent --connect-timeout 2 --max-time 4 https://www.cloudflare.com/cdn-cgi/trace 2>/dev/null | grep -q '^warp=on$' && printf protected || printf unavailable"]).await?;
            if probe == "protected" {
                protected = true;
                break;
            }
        }
        ensure!(protected, "initial provider TLS unavailable");
        let parent = owner.handle().id.clone();
        let namespace = runtime
            .provider_namespace(owner.handle(), owner.profile())
            .await?;
        let transport = owner
            .transport()
            .await
            .context("actual native provider transport")?;
        let mut policy = crate::marketplace::provider::guard::tests::policy();
        policy.lease_seconds = 15;
        // The production handoff MUST replace these example tuples/addresses
        // with the exact connected socket and actual TUN addresses itself.
        let guarded = owner
            .guard(
                guard_image.clone(),
                policy,
                limits.clone(),
                reserve_transfer(&database, now, TRANSPORT_BOOTSTRAP_BYTES, &limits).await?,
            )
            .wait()
            .await;
        let mut guard = match guarded {
            Ok(guard) => guard,
            Err(error) => {
                // A known-retired startup can still leave a valid enrollment.
                // Revoke using encrypted state in a NEW owned namespace, never
                // by flushing the failed guard or silently enrolling again.
                if let Ok(revoker) = Owner::launch(
                    runtime.clone(),
                    directory.clone(),
                    deployment.clone(),
                    image.clone(),
                    scope,
                    true,
                    secrets.clone(),
                )
                .wait()
                .await
                {
                    emergency.actors.push((
                        revoker.handle().clone(),
                        revoker.profile().spec().labels.clone(),
                    ));
                    eprintln!(
                        "failed guard identity revoke confirmed={}",
                        revoker.close(true).await.is_ok()
                    );
                }
                return Err(error.context("owned provider-to-guard initialization"));
            }
        };
        ensure!(
            guard.namespace_inode() == namespace,
            "guard did not join original provider namespace"
        );
        transport.require_policy(namespace, guard.profile().policy())?;
        let child = guard.handle().id.clone();
        let child_name = guard.handle().name.clone();
        emergency.actors.push((
            guard.handle().clone(),
            guard.profile().container_spec().labels.clone(),
        ));
        guard
            .pause(runtime.as_ref())
            .await
            .context("closed provider guard pause")?;
        guard
            .observe_host_resources(runtime.as_ref())
            .await
            .context("one-use provider startup resource sample")?;
        if phase == 0 {
            let held_name = format!("elixir-guard-held-{}", uuid::Uuid::new_v4().simple());
            docker(&["rename", &child, &held_name]).await?;
            drop(guard);
            ensure!(
                directory.join(FENCE).is_file(),
                "dropped guard lost parent/child fence"
            );
            assert!(ControllerLease::acquire(&directory).is_err());
            let failed = Owner::launch(
                runtime.clone(),
                directory.clone(),
                deployment.clone(),
                image.clone(),
                scope,
                true,
                secrets.clone(),
            )
            .wait()
            .await;
            assert!(failed.is_err(), "renamed guard was silently adopted");
            ensure!(
                runtime.get_container_handle(&parent).await?.is_some(),
                "failed child retirement removed provider parent"
            );
            docker(&["rename", &child, &child_name]).await?;
            interrupted = Some((parent, child));
        } else {
            health_outcome = async {
                use elixir_marketplace::connectivity::ProbeFamily;
                let health = guard
                    .bootstrap_provider_connectivity(
                        runtime.as_ref(),
                        reserve_transfer(&database, now, BOOTSTRAP_PROBE_BYTES, &limits).await?,
                        ProbeFamily::Ipv4,
                    )
                    .await
                    .context("prepaid protected provider DNS/TLS bootstrap")?;
                ensure!(
                    health.is_recent_for(&guard) && health.family() == ProbeFamily::Ipv4,
                    "protected provider health lost original binding"
                );
                guard
                    .renew_provider_lease(runtime.as_ref(), &health)
                    .await
                    .context("fresh provider transport/TUN lease renewal")?;
                guard.pause(runtime.as_ref()).await?;
                // A new failed bootstrap MUST invalidate the old successful token;
                // its committed debit is lost, not refunded or replayed.
                ensure!(
                    guard
                        .bootstrap_provider_connectivity(
                            runtime.as_ref(),
                            reserve_transfer(&database, now, 8192, &limits).await?,
                            ProbeFamily::Ipv4
                        )
                        .await
                        .is_err(),
                    "wrong bootstrap debit accepted"
                );
                ensure!(
                    !health.is_recent_for(&guard)
                        && guard
                            .renew_provider_lease(runtime.as_ref(), &health)
                            .await
                            .is_err(),
                    "old provider health masked a newer failed attempt"
                );
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Some(worker_image) = &worker_image {
                if health_outcome.is_ok() {
                    health_outcome = worker_handoff(
                        runtime.clone(),
                        &database,
                        now,
                        &limits,
                        &directory,
                        &deployment,
                        worker_image,
                        guard,
                        &parent,
                        &child,
                        &mut emergency,
                    )
                    .await;
                } else {
                    guard.stop(runtime.as_ref()).await?;
                }
            } else {
                guard.stop(runtime.as_ref()).await?;
                ensure!(
                    runtime.get_container_handle(&child).await?.is_none()
                        && runtime.get_container_handle(&parent).await?.is_none(),
                    "normal guard retirement left native owners"
                );
            }
        }
    }
    if worker_image.is_some() {
        let lease = ControllerLease::acquire_for_recovery(&directory)?;
        super::super::super::fence::Fence::recover(runtime.as_ref(), &lease)
            .await
            .context("durable worker-first fixture recovery")?;
        for (handle, _) in &emergency.actors {
            ensure!(
                runtime.get_container_handle(&handle.id).await?.is_none(),
                "worker-first recovery left an original native actor"
            );
        }
    }
    // Exercise recovery after the OLD bridge itself is gone. Cleanup must use
    // the actual resumed profile's new immutable network ID, not the descriptor.
    runtime
        .remove_packet_uplink(&emergency.network, &emergency.network_id)
        .await?;
    // Revoke through a clean resumed provider, not through a closed guard or
    // a guessed control-plane firewall exception. Still the same free identity.
    let owner = Owner::launch(
        runtime.clone(),
        directory.clone(),
        deployment.clone(),
        image.clone(),
        scope,
        true,
        secrets,
    )
    .wait()
    .await?;
    emergency.actors.push((
        owner.handle().clone(),
        owner.profile().spec().labels.clone(),
    ));
    emergency.network_id = owner.profile().spec().network.clone();
    revoke_and_retire_network(owner, runtime.as_ref(), &deployment).await?;
    ensure!(
        docker(&[
            "ps",
            "-a",
            "--filter",
            &format!("label={ELIXIR_DEPLOYMENT_ID_LABEL}={deployment}"),
            "--format",
            "{{.ID}}"
        ])
        .await?
        .is_empty(),
        "owned provider/guard actors remain"
    );
    emergency.actors.clear();
    if worker_image.is_some() {
        // Only this fixture's own catalog volume, after all original namespace
        // holders are positively absent. Never touch historical/foreign state.
        let volume = format!("elixir-marketplace-{deployment}");
        let names = docker(&[
            "volume",
            "ls",
            "--quiet",
            "--filter",
            &format!("label={ELIXIR_DEPLOYMENT_ID_LABEL}={deployment}"),
        ])
        .await?;
        if names.lines().any(|name| name == volume) {
            let raw = docker(&["volume", "inspect", &volume]).await?;
            let values: Vec<Value> = serde_json::from_str(&raw)?;
            ensure!(
                values.len() == 1
                    && values[0]["Name"] == volume
                    && values[0]["Labels"][ELIXIR_DEPLOYMENT_ID_LABEL] == deployment
                    && values[0]["Labels"]["elixir.marketplace.volume"] == "fixed-v1",
                "worker fixture volume ownership differs"
            );
            docker(&["volume", "rm", &volume]).await?;
        }
    }
    let _ = ControllerLease::acquire(&directory)?;
    root.complete();
    health_outcome?;
    eprintln!(
        "{}",
        serde_json::json!({"same_original_namespace":true,"guards":2,"provider_starts":3,"registration_calls":1,
        "exact_native_udp_transport":true,"protected_doh_tls_bootstrap":true,"bootstrap_debit_bytes":65536,
        "transport_debit_bytes_per_guard":65536,"startup_host_sample_with_transport_only_credit":true,
        "fresh_provider_tun_renewal":true,"failed_attempt_invalidates_old_health":true,
        "child_failure_blocks_parent_retirement":true,"recovery_before_replacement":true,"owned_retirement":true,"recreated_bridge_retired_by_actual_id":true,
        "durable_protected_worker":worker_image.is_some(),"worker_dns_tls_and_supervision":worker_image.is_some(),
        "excludes":"provider endpoint rotation/control-plane refresh, public WAN/P2P, full process/power loss/cancellation and aggregate playback qualification"})
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)] // exact owned fixture context, not a public constructor
async fn worker_handoff(
    runtime: Arc<dyn RuntimeManager>,
    database: &Database,
    now: u64,
    limits: &OrdinaryLimits,
    directory: &std::path::Path,
    deployment: &str,
    image: &str,
    mut guard: crate::marketplace::packet::PacketGuardController,
    parent: &str,
    child: &str,
    emergency: &mut Emergency,
) -> Result<()> {
    use crate::marketplace::{
        connectivity::ProbePolicy,
        worker::{ProtectedWorker, ProtectedWorkerConfig, ProtectionState},
    };
    use elixir_marketplace::{
        catalog::SearchQuery,
        connectivity::ProbeFamily,
        identity::PublicKey,
        peer::{PeerOperation, PeerOutcome},
    };
    let health = match guard
        .bootstrap_provider_connectivity(
            runtime.as_ref(),
            reserve_transfer(database, now, BOOTSTRAP_PROBE_BYTES, limits).await?,
            ProbeFamily::Ipv4,
        )
        .await
    {
        Ok(health) => health,
        Err(error) => {
            guard.stop(runtime.as_ref()).await?;
            return Err(error);
        }
    };
    let issuer = PublicKey::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[3; 32])
            .verifying_key()
            .to_bytes(),
    )?;
    let mut worker = ProtectedWorker::start_provider(
        runtime.clone(),
        ProtectedWorkerConfig {
            controller_directory: directory.to_owned(),
            deployment: deployment.into(),
            image: image.into(),
            issuer_keys: vec![issuer],
            probe_policy: ProbePolicy {
                family: ProbeFamily::Ipv4,
                require_warp: true,
            },
        },
        database.clone(),
        guard,
        health,
        reserve_transfer(database, now, 1024 * 1024, limits).await?,
    )
    .wait()
    .await
    .context("owned provider worker startup/actual protected DNS-TLS")?;
    let handle = worker.handle().clone();
    let labels = worker.profile().container_spec().labels.clone();
    emergency.actors.push((handle.clone(), labels));
    ensure!(
        worker.profile().container_spec().network_mode.as_deref()
            == Some(format!("container:{child}").as_str()),
        "worker joined a different guard"
    );
    ensure!(
        !worker.network_status().await?.active,
        "worker silently enabled public participation"
    );
    let response = worker
        .request(PeerOperation::Search {
            query: SearchQuery {
                text: String::new(),
                tags: vec![],
                limit: 1,
            },
            cursor: None,
        })
        .await?;
    ensure!(
        matches!(response.response, PeerOutcome::Search { .. }),
        "owned worker private search failed"
    );
    // Multiple REAL renewal/resource/ownership checks. No test heartbeat or
    // fabricated WireGuard identity keeps this actual provider worker alive.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        ensure!(
            matches!(
                worker.protection_state(),
                ProtectionState::Ready | ProtectionState::Checking
            ),
            "provider worker supervision failed"
        );
    }
    let name = worker.handle().name.clone();
    let held = format!("elixir-worker-held-{}", uuid::Uuid::new_v4().simple());
    docker(&["rename", &handle.id, &held]).await?;
    ensure!(
        worker.retire_native_fixture().await.is_err(),
        "renamed worker cleanup was silently accepted"
    );
    ensure!(
        runtime.get_container_handle(child).await?.is_some()
            && runtime.get_container_handle(parent).await?.is_some(),
        "failed worker cleanup retired an ancestor"
    );
    ensure!(
        directory.join(FENCE).is_file(),
        "failed worker cleanup erased durable ownership"
    );
    docker(&["rename", &handle.id, &name]).await?;
    // The common final same-identity resume must recover worker -> guard ->
    // provider before replacement. It then revokes that sole fixture identity.
    Ok(())
}

async fn revoke_and_retire_network(
    owner: Owner,
    runtime: &dyn RuntimeManager,
    deployment: &str,
) -> Result<()> {
    let network = owner.profile().spec().network.clone();
    owner
        .close(true)
        .await
        .context("same fixture identity revocation unconfirmed")?;
    runtime
        .remove_packet_uplink(&Profile::network_spec(deployment)?, &network)
        .await?;
    ensure!(
        runtime.inspect_packet_uplink(&network).await?.is_none(),
        "recovered fixture bridge remains"
    );
    Ok(())
}

fn recovery_directory(
    root: &std::path::Path,
    fixture: &str,
    deployment: &str,
) -> Result<std::path::PathBuf> {
    let installed = fixture == "native-public-source-install";
    let suffix = deployment
        .strip_prefix("guard-owner-")
        .filter(|_| !installed)
        .or_else(|| deployment.strip_prefix("wan-owner-"));
    let legacy = suffix.is_some_and(|value| {
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    });
    // The installed application now derives its deployment from the durable
    // storage namespace, not the old WAN fixture UUID. This selects ONLY the
    // original directory; exact image/scope/enrollment journals still gate
    // same-identity resume and revocation below. Never rewrite the descriptor.
    let current = installed
        && deployment
            .strip_prefix("marketplace-storage-")
            .is_some_and(|value| {
                value.len() == 16
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            });
    ensure!(
        (fixture == "native-provider-guard" || installed) && (legacy || current),
        "exact original native fixture deployment required"
    );
    Ok(root.join(if installed {
        "marketplace/worker"
    } else {
        "provider"
    }))
}

#[test]
fn recovery_layout_is_closed_and_keeps_the_original_wan_identity_location() -> Result<()> {
    let root = std::path::Path::new("/fixture-only");
    for prefix in ["guard-owner-", "wan-owner-"] {
        let deployment = format!("{prefix}{}", "a".repeat(32));
        assert_eq!(
            recovery_directory(root, "native-provider-guard", &deployment)?,
            root.join("provider")
        );
    }
    assert_eq!(
        recovery_directory(
            root,
            "native-public-source-install",
            &format!("wan-owner-{}", "a".repeat(32))
        )?,
        root.join("marketplace/worker")
    );
    assert_eq!(
        recovery_directory(
            root,
            "native-public-source-install",
            &format!("marketplace-storage-{}", "a".repeat(16))
        )?,
        root.join("marketplace/worker")
    );
    for (fixture, deployment) in [
        ("unknown", format!("wan-owner-{}", "a".repeat(32))),
        (
            "native-public-source-install",
            format!("guard-owner-{}", "a".repeat(32)),
        ),
        ("native-provider-guard", "wan-owner-../foreign".into()),
        (
            "native-provider-guard",
            format!("wan-owner-{}", "A".repeat(32)),
        ),
        (
            "native-provider-guard",
            format!("marketplace-storage-{}", "a".repeat(16)),
        ),
        ("unknown", format!("marketplace-storage-{}", "a".repeat(16))),
        (
            "native-public-source-install",
            format!("marketplace-storage-{}", "A".repeat(16)),
        ),
        (
            "native-public-source-install",
            format!("marketplace-storage-{}", "a".repeat(15)),
        ),
        (
            "native-public-source-install",
            format!("marketplace-storage-{}", "a".repeat(17)),
        ),
        (
            "native-public-source-install",
            "marketplace-storage-../foreign".into(),
        ),
    ] {
        assert!(recovery_directory(root, fixture, &deployment).is_err());
    }
    Ok(())
}

fn recovery_secrets(fixture: &str) -> Result<SecretsManager> {
    match fixture {
        // Normal authenticated application startup owns the installed fixture's
        // encryption. Rig switches to that exact manager before enrollment.
        "native-public-source-install" => {
            SecretsManager::from_settings(&crate::http::test_settings_with_db())
        }
        "native-provider-guard" => Ok(SecretsManager::from_key_bytes([97; 32], false)),
        _ => anyhow::bail!("known original fixture encryption owner required"),
    }
}

#[test]
fn recovery_secrets_match_each_original_fixture_owner_without_fallback() -> Result<()> {
    let application = SecretsManager::from_settings(&crate::http::test_settings_with_db())?;
    let guard = SecretsManager::from_key_bytes([97; 32], false);
    let installed_cipher = application.encrypt("fixture-only")?;
    let guard_cipher = guard.encrypt("fixture-only")?;
    let installed = recovery_secrets("native-public-source-install")?;
    let legacy = recovery_secrets("native-provider-guard")?;
    assert_eq!(installed.decrypt(&installed_cipher)?, "fixture-only");
    assert_eq!(legacy.decrypt(&guard_cipher)?, "fixture-only");
    assert!(installed.decrypt(&guard_cipher).is_err());
    assert!(legacy.decrypt(&installed_cipher).is_err());
    assert!(recovery_secrets("unknown").is_err());
    Ok(())
}

/// Cleanup-only escape hatch for a preserved approved provider fixture.
/// Never registers a new identity, changes its image/scope, or clears a fence.
#[tokio::test]
#[ignore = "explicit same-identity recovery/revocation of a preserved approved guard test fixture"]
async fn recover_preserved_guard_fixture_and_revoke_same_identity() -> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1"),
        "explicit free-provider test consent required"
    );
    let root = std::path::PathBuf::from(std::env::var(
        "ELIXIR_MARKETPLACE_PROVIDER_RECOVERY_FIXTURE",
    )?)
    .canonicalize()?;
    let descriptor = std::fs::read(root.join("provider-fixture.json"))?;
    ensure!(descriptor.len() <= 8192, "recovery descriptor bounds");
    let descriptor: Value = elixir_marketplace::canonical::parse(&descriptor, 8192)?;
    let deployment = descriptor["deployment"]
        .as_str()
        .context("recovery deployment missing")?;
    let image = image()?;
    let installed = descriptor["fixture"] == "native-public-source-install";
    let fixture = descriptor["fixture"]
        .as_str()
        .context("recovery kind missing")?;
    let directory = recovery_directory(&root, fixture, deployment)?;
    ensure!(
        descriptor["image"] == image,
        "recovery must use the exact original approved guard fixture"
    );
    let scope: Scope = serde_json::from_value(descriptor["scope"].clone())?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let owner = Owner::launch(
        runtime.clone(),
        directory,
        deployment.into(),
        image,
        scope,
        true,
        Arc::new(recovery_secrets(fixture)?),
    )
    .wait()
    .await?;
    revoke_and_retire_network(owner, runtime.as_ref(), deployment).await?;
    eprintln!("preserved guard fixture: same identity revoked; original native owners retired");
    if installed {
        eprintln!("installed-source journals retained for separate operator startup recovery");
    }
    Ok(())
}
