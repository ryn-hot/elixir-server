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
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
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
        let network = runtime.ensure_packet_uplink(&spec).await?;
        spec.verify(&network)?;
        let emergency = Emergency {
            actors: vec![],
            network: spec,
            network_id: network.id.clone(),
        };
        // Recovery knows this exact scope/image and NEVER enrolls a replacement.
        // Guard fixtures keep their fixed test encryption owner. Installed
        // fixtures switch to the normal application's manager before enrollment.
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
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST").as_deref() == Ok("1")
            && std::env::var("ELIXIR_MARKETPLACE_WAN_LIVE_TEST").as_deref() == Ok("1"),
        "explicit free-provider/WAN test consent required"
    );
    let provider_image = image()?;
    let guard_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    ensure!(
        runtime
            .image_labels(&worker_image)
            .await?
            .get("elixir.marketplace.worker.wan_query_budget")
            .map(String::as_str)
            == Some("1"),
        "WAN fixture requires the bounded WAN query budget before enrollment"
    );
    let mut a = Rig::new(runtime.clone(), &provider_image).await?;
    let mut b = Rig::new(runtime, &provider_image).await?;
    let outcome = async {
        a.open(&guard_image, &worker_image)
            .await
            .context("WAN catalog protected startup")?;
        eprintln!("WAN stage: catalog protected startup passed");
        b.open(&guard_image, &worker_image)
            .await
            .context("WAN consumer protected startup")?;
        eprintln!("WAN stage: consumer protected startup passed");
        ensure!(
            a.worker()?.profile().container_spec().network_mode
                != b.worker()?.profile().container_spec().network_mode,
            "WAN workers did not have independent namespaces"
        );
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
        let until = tokio::time::Instant::now() + Duration::from_secs(90);
        let (catalog_peer, consumer_peer) = loop {
            let left = a.worker()?.network_status().await?;
            let right = b.worker()?.network_status().await?;
            left.within_limits(&NetworkLimits::default())?;
            right.within_limits(&NetworkLimits::default())?;
            if left.peers.len() == 1 && right.peers.len() == 1 {
                break (right.peers[0].clone(), left.peers[0].clone());
            }
            ensure!(
                tokio::time::Instant::now() < until,
                "actual WAN discovery/Noise did not converge: catalog={left:?}, consumer={right:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
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
        ensure!(
            control_bytes > 0 && matches!(response.response, PeerOutcome::Search { .. }),
            "WAN search was not an authenticated marketplace response"
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
        Ok::<(), anyhow::Error>(())
    }
    .await;
    // Always join BOTH original cleanup paths, including when discovery fails.
    let left = a.close().await;
    let right = b.close().await;
    // Preserve the operation failure as well as retirement failures. Otherwise
    // a later allowance-stop error masks the first causal failure completely.
    if let Err(error) = &outcome {
        eprintln!("WAN operation failed before retirement: {error:#}");
    }
    left.context("WAN catalog same-identity cleanup/revocation")?;
    right.context("WAN consumer same-identity cleanup/revocation")?;
    outcome?;
    eprintln!(
        "{}",
        serde_json::json!({"actual_public_dht":true,"independent_vpn_namespaces":2,"authenticated_noise_marketplace_exchange":true,"consumer_does_not_serve":true,"owned_retirement":true,"free_identities_revoked":2,"excludes":"persistent routing reachability, different physical ISPs/sites, long soak, source/package transfer, paid issuer, full crash/power loss, aggregate playback and production distribution"})
    );
    Ok(())
}
