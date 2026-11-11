//! Actual WAN discovery/Noise and approved source checks through owned VPNs.
//! Empty test catalogs only; no production startup, paid eligibility, extension
//! publication, public Elixir relay, private-IP bootstrap or plaintext fallback.
use super::*;
use crate::marketplace::{
    connectivity::ProbePolicy,
    worker::{ParticipatingWorker, ProtectedWorker, ProtectedWorkerConfig, ProtectionState},
};
use elixir_marketplace::{
    catalog::SearchQuery,
    connectivity::ProbeFamily,
    identity::PublicKey,
    peer::{PeerOperation, PeerOutcome, RejectionCode},
    worker_control::WorkerControlOutcome,
    worker_network::{BootstrapEndpoint, NetworkConfig, NetworkLimits, NetworkRoles},
};
use futures_util::FutureExt;
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
mod pilot;
mod source;

fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}
fn immutable_image(name: &str) -> Result<String> {
    let image = std::env::var(name)?;
    ensure!(
        image
            .strip_prefix("sha256:")
            .is_some_and(crate::runtime::packet_network::immutable_id),
        "immutable WAN fixture image required"
    );
    Ok(image)
}
struct Rig {
    root: PreservedRoot,
    directory: PathBuf,
    deployment: String,
    scope: Scope,
    image: String,
    secrets: Arc<SecretsManager>,
    database: Database,
    runtime: Arc<dyn RuntimeManager>,
    emergency: Emergency,
    enrollment_started: bool,
    worker: Option<ProtectedWorker>,
    participating: Option<ParticipatingWorker>,
    application: Option<crate::marketplace::service::MarketplaceService>,
}
impl Rig {
    async fn new(runtime: Arc<dyn RuntimeManager>, image: &str) -> Result<Self> {
        Self::new_layout(runtime, image, false).await
    }
    async fn new_layout(
        runtime: Arc<dyn RuntimeManager>,
        image: &str,
        installed: bool,
    ) -> Result<Self> {
        let root = PreservedRoot::new()?;
        let directory = if installed {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(root.path().join("marketplace"))?;
            root.path()
                .canonicalize()?
                .join("marketplace")
                .join("worker")
        } else {
            root.path().canonicalize()?.join("provider")
        };
        let deployment = if installed {
            let paths = crate::runtime::RuntimePaths::from_roots(
                &root
                    .path()
                    .canonicalize()?
                    .join("extensions")
                    .display()
                    .to_string(),
                &root.path().join("media").display().to_string(),
            );
            crate::marketplace::startup::marketplace_deployment(&paths)
        } else {
            format!("wan-owner-{}", uuid::Uuid::new_v4().simple())
        };
        let scope = Scope {
            deployment: Profile::deployment_scope(&deployment),
            enrollment: Digest::of(uuid::Uuid::new_v4().as_bytes()),
        };
        let database = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", root.path().join("wan.sqlite").display()),
            max_connections: if installed { 6 } else { 1 },
            connect_timeout_seconds: 5,
        })
        .await?;
        // The source-owner factory reads real preferences/security/account
        // state. Use normal migrations, never a synthetic consent/status watch.
        database.run_migrations().await?;
        let spec = Profile::network_spec(&deployment)?;
        // The native daemon may commit CREATE after its CLI deadline. Keep
        // the exact expected bridge profile even if no actor was enrolled and
        // there is not yet an immutable network ID for the full descriptor.
        root.describe_uplink_attempt(&spec)?;
        let prepared = async {
            let network = runtime.ensure_packet_uplink(&spec).await?;
            spec.verify(&network)?;
            ensure!(network.members.is_empty(), "WAN fixture uplink is occupied");
            // Recovery knows the exact scope/image and NEVER enrolls a replacement.
            // Installed fixtures switch to the normal application's manager.
            root.describe(
                &deployment,
                scope,
                image,
                &network.id,
                if installed {
                    "native-public-source-install"
                } else {
                    "native-provider-guard"
                },
            )?;
            Ok::<_, anyhow::Error>(network)
        }
        .await;
        if prepared.is_err() {
            root.preserve();
        }
        let network = prepared?;
        let emergency = Emergency {
            actors: vec![],
            network: spec,
            network_id: network.id.clone(),
        };
        Ok(Self {
            root,
            directory,
            deployment,
            scope,
            image: image.into(),
            secrets: Arc::new(SecretsManager::from_key_bytes([97; 32], false)),
            database,
            runtime,
            emergency,
            enrollment_started: false,
            worker: None,
            participating: None,
            application: None,
        })
    }
    async fn open(&mut self, guard_image: &str, worker_image: &str) -> Result<()> {
        self.enrollment_started = true;
        let mut owner = Owner::launch(
            self.runtime.clone(),
            self.directory.clone(),
            self.deployment.clone(),
            self.image.clone(),
            self.scope,
            false,
            self.secrets.clone(),
        )
        .wait()
        .await?;
        self.emergency.actors.push((
            owner.handle().clone(),
            owner.profile().spec().labels.clone(),
        ));
        owner.connect().await?;
        let mut connected = false;
        for _ in 0..6 {
            owner.inspect().await?;
            let probe = docker(&["exec", &owner.handle().id, "/bin/bash", "-c", "set -o pipefail; curl --noproxy '*' --fail --silent --connect-timeout 2 --max-time 4 https://www.cloudflare.com/cdn-cgi/trace 2>/dev/null | grep -q '^warp=on$' && printf protected || printf unavailable"]).await?;
            if probe == "protected" {
                connected = true;
                break;
            }
        }
        ensure!(
            connected,
            "WAN fixture initial protected provider unavailable"
        );
        let limits = OrdinaryLimits::default();
        let mut policy = crate::marketplace::provider::guard::tests::policy();
        policy.lease_seconds = 15;
        let mut guard = owner
            .guard(
                guard_image.into(),
                policy,
                limits.clone(),
                reserve_transfer(&self.database, now()?, TRANSPORT_BOOTSTRAP_BYTES, &limits)
                    .await?,
            )
            .wait()
            .await?;
        self.emergency.actors.push((
            guard.handle().clone(),
            guard.profile().container_spec().labels.clone(),
        ));
        guard.pause(self.runtime.as_ref()).await?;
        guard.observe_host_resources(self.runtime.as_ref()).await?;
        let health = guard
            .bootstrap_provider_connectivity(
                self.runtime.as_ref(),
                reserve_transfer(&self.database, now()?, BOOTSTRAP_PROBE_BYTES, &limits).await?,
                ProbeFamily::Ipv4,
            )
            .await?;
        let issuer = PublicKey::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[3; 32])
                .verifying_key()
                .to_bytes(),
        )?;
        let worker = ProtectedWorker::start_provider(
            self.runtime.clone(),
            ProtectedWorkerConfig {
                controller_directory: self.directory.clone(),
                deployment: self.deployment.clone(),
                image: worker_image.into(),
                issuer_keys: vec![issuer],
                probe_policy: ProbePolicy {
                    family: ProbeFamily::Ipv4,
                    require_warp: true,
                },
            },
            self.database.clone(),
            guard,
            health,
            crate::marketplace::startup::reserve_worker_startup_credit(
                &self.database,
                now()?,
                &limits,
            )
            .await?,
        )
        .wait()
        .await?;
        self.emergency.actors.push((
            worker.handle().clone(),
            worker.profile().container_spec().labels.clone(),
        ));
        self.worker = Some(worker);
        Ok(())
    }
    fn worker(&mut self) -> Result<&mut ProtectedWorker> {
        self.worker
            .as_mut()
            .context("WAN fixture worker unavailable")
    }
    async fn close(&mut self) -> Result<()> {
        let stopped = if let Some(application) = self.application.take() {
            application.shutdown().await
        } else if let Some(worker) = self.participating.take() {
            worker.stop().await
        } else if let Some(worker) = self.worker.take() {
            // A WAN run expects healthy operation. Preserve a late supervisor
            // error as well as cleanup; discarding it hides the reason behind
            // a generic network-status "protection unavailable" consequence.
            worker.stop().await
        } else {
            Ok(())
        };
        if self.enrollment_started {
            // Check retained account state BEFORE retiring uncertain native
            // enrollment. Missing state is NOT proof that registration failed.
            let lease = ControllerLease::acquire_for_recovery(&self.directory)?;
            ensure!(
                provider_state::load(&lease, &self.secrets, self.scope, self.image.parse()?)?
                    .is_some(),
                "WAN fixture enrollment/revocation unresolved; preserve original journal"
            );
            drop(lease);
            let owner = Owner::launch(
                self.runtime.clone(),
                self.directory.clone(),
                self.deployment.clone(),
                self.image.clone(),
                self.scope,
                true,
                self.secrets.clone(),
            )
            .wait()
            .await?;
            self.emergency.network_id = owner.profile().spec().network.clone();
            self.emergency.actors.push((
                owner.handle().clone(),
                owner.profile().spec().labels.clone(),
            ));
            revoke_and_retire_network(owner, self.runtime.as_ref(), &self.deployment).await?;
        } else {
            self.runtime
                .remove_packet_uplink(&self.emergency.network, &self.emergency.network_id)
                .await?;
        }
        for (handle, _) in &self.emergency.actors {
            ensure!(
                self.runtime
                    .get_container_handle(&handle.id)
                    .await?
                    .is_none(),
                "WAN fixture retained a native namespace holder"
            );
        }
        let volume = format!("elixir-marketplace-{}", self.deployment);
        let names = docker(&[
            "volume",
            "ls",
            "--quiet",
            "--filter",
            &format!("label={ELIXIR_DEPLOYMENT_ID_LABEL}={}", self.deployment),
        ])
        .await?;
        if names.lines().any(|name| name == volume) {
            let values: Vec<Value> =
                serde_json::from_str(&docker(&["volume", "inspect", &volume]).await?)?;
            ensure!(
                values.len() == 1
                    && values[0]["Name"] == volume
                    && values[0]["Labels"][ELIXIR_DEPLOYMENT_ID_LABEL] == self.deployment
                    && values[0]["Labels"]["elixir.marketplace.volume"] == "fixed-v1",
                "WAN fixture volume ownership differs"
            );
            docker(&["volume", "rm", &volume]).await?;
        }
        self.emergency.actors.clear();
        self.root.complete();
        // Identity revocation and exact native cleanup above always finish,
        // even when this original operation error must be returned afterward.
        stopped.context("WAN worker operation failed before joined recovery")
    }
}

async fn ready(worker: &ProtectedWorker) -> Result<()> {
    let until = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        match worker.protection_state() {
            ProtectionState::Ready => return Ok(()),
            ProtectionState::Starting | ProtectionState::Checking
                if tokio::time::Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(25)).await
            }
            _ => anyhow::bail!("WAN fixture protection unavailable"),
        }
    }
}
fn config(topic: Digest, catalog: bool) -> Result<NetworkConfig> {
    // Exact pinned hyperdht 6.34.1 public bootstrap constants; literal addresses
    // avoid host DNS, private IPs and default/hidden fallback bootstrap lists.
    Ok(NetworkConfig {
        bootstrap: ["88.99.3.86", "142.93.90.113", "138.68.147.8"]
            .into_iter()
            .map(|address| {
                Ok(BootstrapEndpoint {
                    address: address.parse()?,
                    port: 49737,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        topics: vec![topic],
        roles: NetworkRoles {
            consumer: true,
            catalog,
            routing: false,
        },
        limits: NetworkLimits::default(),
    })
}
fn search() -> PeerOperation {
    PeerOperation::Search {
        query: SearchQuery {
            text: String::new(),
            tags: vec![],
            limit: 1,
        },
        cursor: None,
    }
}

#[tokio::test]
#[ignore = "explicit two approved free providers and real public DHT/Noise through owned guarded VPN workers; no extensions"]
async fn two_owned_provider_workers_discover_and_exchange_authenticated_metadata_over_wan()
-> Result<()> {
    qualify_owned_wan(false).await
}

#[tokio::test]
#[ignore = "two approved free VPN identities; empty isolated topics; no source or package publication"]
async fn old_and_candidate_noise_workers_exchange_metadata_in_both_roles_over_wan() -> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_MIXED_NOISE_LIVE_TEST").as_deref() == Ok("1"),
        "explicit mixed Noise qualification required"
    );
    qualify_owned_wan(true).await
}

async fn qualify_owned_wan(mixed: bool) -> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1")
            && std::env::var("ELIXIR_MARKETPLACE_WAN_LIVE_TEST").as_deref() == Ok("1"),
        "explicit free-provider/WAN test consent required"
    );
    let provider_image = image()?;
    let guard_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let second_image = if mixed {
        immutable_image("ELIXIR_MARKETPLACE_MIXED_NOISE_WORKER_IMAGE")?
    } else {
        worker_image.clone()
    };
    let observe_dial = mixed
        && std::env::var("ELIXIR_MARKETPLACE_MIXED_NOISE_DIAL_OBSERVATION").as_deref() == Ok("1");
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    for (index, image) in [&worker_image, &second_image].into_iter().enumerate() {
        let labels = runtime.image_labels(image).await?;
        let label = |name: &str| {
            labels
                .get(&format!("elixir.marketplace.worker.{name}"))
                .map(String::as_str)
        };
        ensure!(
            label("wan_query_budget") == Some("1"),
            "bounded WAN query budget required"
        );
        if mixed {
            ensure!(
                label("onion_transport") == Some("2") && label("onion_fixture") == Some("0"),
                "mixed qualification requires both ordinary onion workers"
            );
            ensure!(
                (index == 1) == (label("private_noise_candidate") == Some("1")),
                "expected baseline and private Noise candidate in order"
            );
            if index == 1 {
                ensure!(
                    label("onion_publication_ready") == Some("0"),
                    "candidate must not be publishable"
                );
            }
            if observe_dial {
                ensure!(
                    label("private_discovery_trace") == Some("1")
                        && label("onion_publication_ready") == Some("0"),
                    "dial observation requires both private trace images"
                );
            }
        }
    }
    let mut a = Rig::new(runtime.clone(), &provider_image).await?;
    let mut b = match Rig::new(runtime, &provider_image).await {
        Ok(rig) => rig,
        Err(error) => {
            a.close()
                .await
                .context("retire first rig after second rig creation failed")?;
            return Err(error);
        }
    };
    if mixed {
        for (index, rig) in [&a, &b].into_iter().enumerate() {
            eprintln!(
                "{}",
                serde_json::json!({"stage":"mixed_noise_fixture_layout","participant":index+1,
                "deployment":rig.deployment,"recovery_root":rig.root.path()})
            );
        }
    }
    let outcome = std::panic::AssertUnwindSafe(async {
        a.open(&guard_image, &worker_image)
            .await
            .context("WAN catalog protected startup")?;
        eprintln!("WAN stage: catalog protected startup passed");
        b.open(&guard_image, &second_image)
            .await
            .context("WAN consumer protected startup")?;
        eprintln!("WAN stage: consumer protected startup passed");
        ensure!(
            a.worker()?.profile().container_spec().network_mode
                != b.worker()?.profile().container_spec().network_mode,
            "WAN workers did not have independent namespaces"
        );
        exchange_empty_catalog(&mut a, &mut b, observe_dial.then_some("baseline")).await?;
        if mixed {
            eprintln!(
                "{}",
                serde_json::json!({"stage":"mixed_noise_direction_verified","catalog":"baseline"})
            );
            exchange_empty_catalog(&mut b, &mut a, observe_dial.then_some("candidate")).await?;
            eprintln!(
                "{}",
                serde_json::json!({"stage":"mixed_noise_direction_verified","catalog":"candidate"})
            );
        }
        Ok::<(), anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    // Join both same-identity cleanup paths before reporting any failure.
    let left = a.close().await;
    let right = b.close().await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("WAN operation failed before retirement: {error:#}");
    }
    if mixed {
        for (index, retired) in [&left, &right].into_iter().enumerate() {
            if retired.is_ok() {
                let enrollment_started = if index == 0 {
                    a.enrollment_started
                } else {
                    b.enrollment_started
                };
                eprintln!(
                    "{}",
                    serde_json::json!({"stage":"mixed_noise_fixture_retired","participant":index+1,
                    "enrollment_started":enrollment_started})
                );
            }
        }
    }
    left.context("WAN first same-identity cleanup/revocation")?;
    right.context("WAN second same-identity cleanup/revocation")?;
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    eprintln!(
        "{}",
        serde_json::json!({"actual_public_dht":true,"independent_vpn_namespaces":2,"authenticated_noise_marketplace_exchange":true,"consumer_does_not_serve":true,"owned_retirement":true,"free_identities_revoked":2,"mixed_noise_roles":mixed,"excludes":"persistent routing reachability, different physical ISPs/sites, long soak, source/package transfer, paid issuer, full crash/power loss, aggregate playback and production distribution"})
    );
    Ok(())
}

const READINESS_WINDOW: Duration = Duration::from_secs(90);
// Test-only observation of the already in-flight consumer dial: Tor's 120 s
// SOCKS allowance, Noise's 15 s allowance and one polling margin. This never
// changes the worker's deadlines, refreshes discovery, or starts a new dial.
const DIAL_OBSERVATION_WINDOW: Duration = Duration::from_secs(140);

fn wait_for_wan_peer(
    elapsed: Duration,
    observe: bool,
    first_dial: Option<Duration>,
    pending: bool,
) -> bool {
    elapsed < READINESS_WINDOW
        || (observe
            && pending
            && first_dial.is_some_and(|started| {
                elapsed < started.min(READINESS_WINDOW) + DIAL_OBSERVATION_WINDOW
            }))
}

#[test]
fn wan_observation_extends_only_for_a_pending_dial_within_a_fixed_window() {
    let seconds = Duration::from_secs;
    assert!(wait_for_wan_peer(seconds(89), false, None, false));
    assert!(!wait_for_wan_peer(
        seconds(90),
        false,
        Some(seconds(5)),
        true
    ));
    assert!(!wait_for_wan_peer(seconds(90), true, None, false));
    assert!(!wait_for_wan_peer(
        seconds(90),
        true,
        Some(seconds(5)),
        false
    ));
    assert!(wait_for_wan_peer(seconds(90), true, Some(seconds(5)), true));
    assert!(!wait_for_wan_peer(
        seconds(145),
        true,
        Some(seconds(5)),
        true
    ));
    assert!(!wait_for_wan_peer(
        seconds(230),
        true,
        Some(seconds(110)),
        true
    ));
}

async fn exchange_empty_catalog(a: &mut Rig, b: &mut Rig, observation: Option<&str>) -> Result<()> {
    let database = a.database.clone();
    a.worker()?.reconcile_native_catalog(&database).await?;
    let topic = Digest::of(uuid::Uuid::new_v4().as_bytes());
    ready(a.worker()?).await?;
    let catalog = a
        .worker()?
        .start_network(config(topic, true)?)
        .await
        .context("WAN catalog DHT/listener startup")?;
    ensure!(
        catalog.active && catalog.catalog && !catalog.routing && !catalog.adaptive,
        "WAN catalog roles differ"
    );
    ready(b.worker()?).await?;
    let consumer = b
        .worker()?
        .start_network(config(topic, false)?)
        .await
        .context("WAN consumer DHT startup")?;
    ensure!(
        consumer.active && !consumer.catalog && !consumer.routing && !consumer.adaptive,
        "WAN consumer roles differ"
    );
    eprintln!("WAN stage: both public discovery roles started");
    let started = tokio::time::Instant::now();
    let mut first_dial = None;
    let mut readiness_missed = false;
    let (catalog_peer, consumer_peer, ready_elapsed, pending_at_ready) = loop {
        let left = a.worker()?.network_status().await?;
        let right = b.worker()?.network_status().await?;
        left.within_limits(&NetworkLimits::default())?;
        right.within_limits(&NetworkLimits::default())?;
        let elapsed = started.elapsed();
        let pending = right.pending_dials > 0;
        if pending && elapsed < READINESS_WINDOW {
            first_dial.get_or_insert(elapsed);
        }
        if let Some(catalog) = observation
            && elapsed >= READINESS_WINDOW
            && !readiness_missed
        {
            readiness_missed = true;
            eprintln!(
                "{}",
                serde_json::json!({"stage":"mixed_noise_readiness_missed","catalog":catalog,
                    "elapsed_ms":elapsed.as_millis(),"pending":pending})
            );
        }
        if left.peers.len() == 1 && right.peers.len() == 1 {
            ensure!(
                right.peers[0] != left.peers[0],
                "WAN peers were not distinct"
            );
            break (
                right.peers[0].clone(),
                left.peers[0].clone(),
                elapsed,
                pending,
            );
        }
        if !wait_for_wan_peer(elapsed, observation.is_some(), first_dial, pending) {
            if let Some(catalog) = observation {
                eprintln!(
                    "{}",
                    serde_json::json!({"stage":"mixed_noise_peer_observation_finished","catalog":catalog,
                        "elapsed_ms":elapsed.as_millis(),"ready":false,"pending":pending})
                );
            }
            anyhow::bail!("actual WAN discovery/Noise did not converge");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    if let Some(catalog) = observation {
        eprintln!(
            "{}",
            serde_json::json!({"stage":"mixed_noise_peer_observation_finished","catalog":catalog,
                "elapsed_ms":ready_elapsed.as_millis(),"ready":true,"pending":pending_at_ready})
        );
    }
    ready(b.worker()?).await?;
    let response = b
        .worker()?
        .request_peer(catalog_peer, search())
        .await
        .context("actual WAN Noise marketplace request")?;
    let WorkerControlOutcome::NetworkReply {
        response,
        control_bytes,
        ..
    } = response.control_response
    else {
        anyhow::bail!("actual WAN peer did not return a marketplace reply");
    };
    let PeerOutcome::Search { page } = response.response else {
        anyhow::bail!("WAN search was not an authenticated marketplace response");
    };
    ensure!(
        control_bytes > 0
            && page.results.is_empty()
            && page.next_cursor.is_none()
            && !page.network_complete
            && page.metadata_only,
        "WAN fixture returned a nonempty or unbounded page"
    );
    ready(a.worker()?).await?;
    let response = a.worker()?.request_peer(consumer_peer, search()).await?;
    let WorkerControlOutcome::NetworkReply { response, .. } = response.control_response else {
        anyhow::bail!("WAN consumer refusal unavailable");
    };
    ensure!(
        matches!(
            response.response,
            PeerOutcome::Rejected {
                code: RejectionCode::Unauthorized
            }
        ),
        "WAN consumer silently served its cache"
    );
    ensure!(
        !a.worker()?.stop_network().await?.active && !b.worker()?.stop_network().await?.active,
        "WAN stop did not close public roles"
    );
    Ok(())
}
