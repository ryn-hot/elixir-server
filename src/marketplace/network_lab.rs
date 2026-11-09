//! Explicit TEST ONLY. Fresh labelled namespaces and keys; no host networking,
//! published ports, provider account, custom CA or fabricated health. The local
//! WireGuard exit NATs to the internet for real public DNS/TLS probes. This does
//! not establish off-site ISP privacy or production-owned-uplink qualification.
use super::{
    packet::{PacketGuardController, PacketGuardProfile},
    runtime::image_is_pinned,
    worker::{
        ParticipatingWorker, ParticipationClient, ProtectedWorker, ProtectedWorkerConfig,
        ProtectionState,
    },
};
use crate::{
    db::Database,
    runtime::{
        RuntimeManager,
        docker::DockerRuntimeManager,
        model::{ContainerHandle, ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL},
    },
};
use anyhow::{Context, Result, bail, ensure};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use elixir_marketplace::{
    identity::Digest,
    network_probe::WireguardIdentity,
    packet_guard::{PacketPolicy, VpnEndpoint},
    policy::OrdinaryLimits,
    wireguard_setup::{WireguardSecrets, WireguardSetup},
};
use rand::RngCore;
use std::{collections::HashMap, path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use zeroize::Zeroizing;

const TOOLS: &str = "sha256:0b11c8bc24df09f988781122d1b5f22604d3152b1342545a9369387a39e23403";
const TEST_LABEL: &str = "elixir.marketplace.public_path_test";
mod application;

struct Target {
    name: String,
    labels: HashMap<String, String>,
    id: Option<String>,
    image: Option<String>,
}
pub(in crate::marketplace) struct NetworkLab {
    pub runtime: Arc<dyn RuntimeManager>,
    pub database: Database,
    pub owner: String,
    pub root: PathBuf,
    pub limits: OrdinaryLimits,
    pub playback: Arc<crate::playback::PlaybackJobManager>,
    pub guard: Option<PacketGuardController>,
    pub identity: Option<WireguardIdentity>,
    pub protected: Option<ProtectedWorker>,
    participating: Option<ParticipatingWorker>,
    participation_start_unknown: bool,
    _directory: Option<tempfile::TempDir>,
    targets: Vec<Target>,
    volume: Option<(String, HashMap<String, String>)>,
    application_network: Option<(crate::runtime::packet_network::PacketNetworkSpec, String)>,
    application_activity: Arc<application::Activity>,
    application_gateways: Arc<tokio::sync::Mutex<Vec<ContainerHandle>>>,
}
impl NetworkLab {
    pub fn new(database: Database) -> Result<Self> {
        ensure!(
            std::env::var("ELIXIR_MARKETPLACE_PUBLIC_PATH").as_deref() == Ok("1"),
            "explicit public DNS/TLS fixture consent required"
        );
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let playback = Arc::new(crate::playback::PlaybackJobManager::new(
            database.pool.clone(),
            None,
        ));
        Ok(Self {
            runtime: Arc::new(DockerRuntimeManager::new(None)),
            database,
            owner: format!("http-lab-{}", uuid::Uuid::new_v4().simple()),
            root,
            limits: OrdinaryLimits::default(),
            playback,
            guard: None,
            identity: None,
            protected: None,
            participating: None,
            participation_start_unknown: false,
            _directory: Some(directory),
            targets: vec![],
            volume: None,
            application_network: None,
            application_activity: Arc::default(),
            application_gateways: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        })
    }
    pub async fn start(&mut self, guard_image: &str) -> Result<()> {
        self.start_connection(guard_image, None).await
    }
    pub async fn start_saved_connection(
        &mut self,
        guard_image: &str,
        manager: &crate::secrets::SecretsManager,
    ) -> Result<()> {
        self.start_connection(guard_image, Some(manager)).await
    }
    async fn start_connection(
        &mut self,
        guard_image: &str,
        saved: Option<&crate::secrets::SecretsManager>,
    ) -> Result<()> {
        ensure!(
            image_is_pinned(guard_image),
            "immutable lab guard image required"
        );
        let policy = PacketPolicy {
            schema_version: 1,
            epoch: Digest::of(b"discarded-lab-epoch"),
            tunnel_interface: "wg0".into(),
            tunnel_addresses: vec!["10.66.0.2".parse()?],
            endpoints: vec![VpnEndpoint {
                address: "8.8.8.8".parse()?,
                port: 51820,
                local_port: 51821,
            }],
            lease_seconds: 15,
            upload_bytes_per_second: self.limits.upload_bytes_per_second,
            download_bytes_per_second: self.limits.download_bytes_per_second,
        };
        let profile = PacketGuardProfile::isolated(&self.owner, guard_image, policy, &self.limits)?;
        self.targets.push(Target {
            name: profile.container_spec().name.clone(),
            labels: profile.container_spec().labels.clone(),
            id: None,
            image: Some(guard_image.to_owned()),
        });
        let guard = PacketGuardController::start_isolated(
            self.runtime.as_ref(),
            &self.root.join("guard"),
            profile,
        )
        .await?;
        self.targets.last_mut().unwrap().id = Some(guard.handle().id.clone());
        self.guard = Some(guard);
        let peer = self
            .container(
                "peer",
                &["--network=bridge", "--sysctl=net.ipv4.ip_forward=1"],
            )
            .await?;
        let guard_id = self.guard.as_ref().unwrap().handle().id.clone();
        let config = self
            .container(
                "config",
                &[
                    "--network",
                    &format!("container:{guard_id}"),
                    "--pid",
                    &format!("container:{peer}"),
                ],
            )
            .await?;
        // Only the owned, otherwise disconnected guard namespace is modified.
        // The provider retains a separate default-bridge uplink. No Docker-host
        // routes/firewall/sysctls or existing network are changed.
        for (id, args) in [
            (
                &config,
                vec![
                    "ip", "link", "add", "eth0", "type", "veth", "peer", "name", "lab-vpn",
                ],
            ),
            (&config, vec!["ip", "link", "set", "lab-vpn", "netns", "1"]),
            (
                &config,
                vec!["ip", "addr", "add", "198.18.17.2/30", "dev", "eth0"],
            ),
            (&config, vec!["ip", "link", "set", "eth0", "up"]),
            (
                &config,
                vec![
                    "ip",
                    "route",
                    "add",
                    "default",
                    "via",
                    "198.18.17.1",
                    "dev",
                    "eth0",
                ],
            ),
            (
                &peer,
                vec!["ip", "addr", "add", "198.18.17.1/30", "dev", "lab-vpn"],
            ),
            (&peer, vec!["ip", "link", "set", "lab-vpn", "up"]),
            (&peer, vec!["ip", "addr", "add", "8.8.8.8/32", "dev", "lo"]),
            // Owned local endpoint for test bootstrap/HTTP, never Google or a
            // real third-party peer. It is reachable only through this lab.
            (&peer, vec!["ip", "addr", "add", "8.8.4.4/32", "dev", "lo"]),
            (&peer, vec!["ip", "link", "add", "wg0", "type", "wireguard"]),
        ] {
            let mut command = vec!["exec", id.as_str()];
            command.extend(args);
            docker(&command, &[]).await?;
        }
        let mut private = Zeroizing::new([0; 32]);
        let mut peer_private = Zeroizing::new([0; 32]);
        rand::rngs::OsRng.fill_bytes(private.as_mut());
        rand::rngs::OsRng.fill_bytes(peer_private.as_mut());
        let secrets = WireguardSecrets::from_keys(private, None)?;
        let peer_secrets = WireguardSecrets::from_keys(peer_private, None)?;
        let device = secrets.device_public_key()?;
        let peer_key = peer_secrets.device_public_key()?;
        let public_device = STANDARD.encode(URL_SAFE_NO_PAD.decode(&device)?);
        let mut private_text = Zeroizing::new(STANDARD.encode(&peer_secrets.private_frame()[..32]));
        private_text.push('\n');
        docker(
            &[
                "exec",
                "-i",
                &peer,
                "wg",
                "set",
                "wg0",
                "private-key",
                "/dev/stdin",
                "listen-port",
                "51820",
                "peer",
                &public_device,
                "allowed-ips",
                "10.66.0.2/32",
            ],
            private_text.as_bytes(),
        )
        .await?;
        for args in [
            vec!["ip", "addr", "add", "10.66.0.1/32", "dev", "wg0"],
            vec!["ip", "link", "set", "wg0", "mtu", "1280", "up"],
            vec!["ip", "route", "add", "10.66.0.2/32", "dev", "wg0"],
        ] {
            let mut command = vec!["exec", peer.as_str()];
            command.extend(args);
            docker(&command, &[]).await?;
        }
        docker(
            &["exec", "-i", &peer, "nft", "-f", "-"],
            br#"add table ip elx_lab_nat
add chain ip elx_lab_nat postrouting { type nat hook postrouting priority 100; policy accept; }
add rule ip elx_lab_nat postrouting ip saddr 10.66.0.2 oifname "eth0" masquerade
"#,
        )
        .await?;
        let identity = WireguardIdentity {
            device: device.clone(),
            peer: peer_key,
        };
        let (setup, secrets, identity) = if let Some(manager) = saved {
            let material = Zeroizing::new(format!(
                "[Interface]\nPrivateKey = {}\nAddress = 10.66.0.2/32\nListenPort = 51821\nMTU = 1280\n[Peer]\nPublicKey = {}\nAllowedIPs = 0.0.0.0/0\nEndpoint = 8.8.8.8:51820\nPersistentKeepalive = 1\n",
                STANDARD.encode(&secrets.private_frame()[..32]),
                STANDARD.encode(URL_SAFE_NO_PAD.decode(&identity.peer)?)
            ));
            crate::network::protection::import_wireguard_profile(&self.database.pool,manager,serde_json::from_value(serde_json::json!({"profileId":"marketplace-network","name":"Disposable saved connection","purpose":"marketplace_network","config":&*material}))?).await?;
            super::startup::saved_connection_for_fixture(&self.database, manager, &self.limits)
                .await?
        } else {
            (
                WireguardSetup {
                    device_public_key: device,
                    peer_public_key: identity.peer.clone(),
                    mtu: 1280,
                    keepalive_seconds: 1,
                },
                secrets,
                identity,
            )
        };
        self.guard
            .as_mut()
            .unwrap()
            .configure_wireguard(self.runtime.as_ref(), setup, secrets)
            .await?;
        self.identity = Some(identity);
        Ok(())
    }
    pub async fn bootstrap(
        &mut self,
        image: &str,
    ) -> Result<super::connectivity::ConnectivityMonitor> {
        use super::{
            accounting::reserve_transfer,
            connectivity::{BOOTSTRAP_PROBE_BYTES, ConnectivityMonitor, ProbePolicy},
        };
        ensure!(image_is_pinned(image), "immutable worker image required");
        let guard = self.guard.as_mut().context("lab guard missing")?;
        let mut monitor = ConnectivityMonitor::default();
        let debit = reserve_transfer(
            &self.database,
            chrono::Utc::now().timestamp().try_into()?,
            BOOTSTRAP_PROBE_BYTES,
            &self.limits,
        )
        .await?;
        monitor
            .observe_bootstrap(
                self.runtime.as_ref(),
                guard,
                image,
                self.identity.as_ref().context("lab identity missing")?,
                debit,
                ProbePolicy {
                    family: elixir_marketplace::connectivity::ProbeFamily::Ipv4,
                    require_warp: false,
                },
            )
            .await?;
        let report = monitor
            .current(guard)
            .context("real bootstrap observation missing")?
            .report();
        ensure!(
            report.kernel.network_namespace_inode == guard.namespace_inode(),
            "bootstrap namespace differs"
        );
        ensure!(
            report.source_address == "10.66.0.2".parse::<std::net::IpAddr>()?,
            "bootstrap did not use the tunnel source"
        );
        ensure!(
            elixir_marketplace::packet_guard::public_ip(report.exit_address),
            "bootstrap exit is not public"
        );
        Ok(monitor)
    }
    pub async fn start_protected(&mut self, image: &str) -> Result<()> {
        use super::{accounting::reserve_transfer, connectivity::ProbePolicy};
        ensure!(
            self.protected.is_none(),
            "lab already owns a protected worker"
        );
        timeout(
            Duration::from_secs(30),
            ProtectedWorker::await_startup_host(&self.limits, &self.playback),
        )
        .await
        .context("test host admission wait expired without starting a worker")??;
        ProtectedWorker::admit_startup_resources(
            self.runtime.as_ref(),
            self.guard.as_mut().context("lab guard missing")?,
            &self.limits,
            &self.playback,
        )
        .await?;
        let monitor = self
            .bootstrap(image)
            .await
            .context("real worker bootstrap")?;
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), self.owner.clone()),
            (
                "elixir.marketplace.volume".to_owned(),
                "fixed-v1".to_owned(),
            ),
        ]);
        // Retain intended identities BEFORE provisioning. Unknown startup does
        // not permit deleting a foreign container or resetting a volume.
        self.volume = Some((format!("elixir-marketplace-{}", self.owner), labels.clone()));
        let mut worker_labels = labels;
        worker_labels.insert(
            "elixir.marketplace.isolation".to_owned(),
            "fixed-v1".to_owned(),
        );
        self.targets.push(Target {
            name: format!("elixir-marketplace-worker-{}", self.owner),
            labels: worker_labels,
            id: None,
            image: Some(image.to_owned()),
        });
        let debit = reserve_transfer(
            &self.database,
            chrono::Utc::now().timestamp().try_into()?,
            1024 * 1024,
            &self.limits,
        )
        .await?;
        let worker = Box::pin(ProtectedWorker::start(
            self.runtime.clone(),
            ProtectedWorkerConfig {
                controller_directory: self.root.join("worker"),
                deployment: self.owner.clone(),
                image: image.to_owned(),
                issuer_keys: vec![elixir_marketplace::identity::PublicKey::from_bytes(
                    ed25519_dalek::SigningKey::from_bytes(&[3; 32])
                        .verifying_key()
                        .to_bytes(),
                )?],
                probe_policy: ProbePolicy {
                    family: elixir_marketplace::connectivity::ProbeFamily::Ipv4,
                    require_warp: false,
                },
            },
            self.database.clone(),
            self.guard.take().context("lab guard missing")?,
            monitor,
            self.identity.take().context("lab identity missing")?,
            debit,
        ))
        .await
        .context("actual protected worker startup")?;
        let target = self.targets.last_mut().unwrap();
        target.id = Some(worker.handle().id.clone());
        target.labels = worker.profile().container_spec().labels.clone();
        self.protected = Some(worker);
        Ok(())
    }
    pub async fn start_participating(&mut self, image: &str) -> Result<ParticipationClient> {
        use elixir_marketplace::worker_network::{
            BootstrapEndpoint, NetworkConfig, NetworkLimits, NetworkRoles,
        };
        self.start_protected(image).await?;
        let worker = self
            .protected
            .take()
            .context("original protected owner missing")?;
        self.participation_start_unknown = true;
        let owner = ParticipatingWorker::start(
            worker,
            NetworkConfig {
                bootstrap: vec![BootstrapEndpoint {
                    address: "8.8.4.4".parse()?,
                    port: 49737,
                }],
                topics: vec![Digest::of(self.owner.as_bytes())],
                roles: NetworkRoles {
                    consumer: true,
                    catalog: false,
                    routing: false,
                },
                limits: NetworkLimits::default(),
            },
            self.database.clone(),
            self.playback.clone(),
        )
        .await?;
        let client = owner.client();
        self.participating = Some(owner);
        self.participation_start_unknown = false;
        Ok(client)
    }
    fn peer(&self) -> Result<&str> {
        let name = format!("elixir-marketplace-{}-peer", self.owner);
        self.targets
            .iter()
            .find(|target| target.name == name)
            .and_then(|target| target.id.as_deref())
            .context("original fixture exit missing")
    }
    /// A second real WireGuard client for application-Gluetun tests. Reuses only
    /// this lab's disposable exit, never the catalog's guard/credits/identity.
    pub async fn application_exit(
        &mut self,
    ) -> Result<(Arc<dyn RuntimeManager>, String, Zeroizing<String>)> {
        use crate::runtime::packet_network::PacketNetworkSpec;
        ensure!(
            self.application_network.is_none(),
            "application lab already configured"
        );
        let spec = PacketNetworkSpec::internet(&format!("app-{}", self.owner), false)?;
        let actual = self.runtime.ensure_packet_uplink(&spec).await?;
        spec.verify(&actual)?;
        self.application_network = Some((spec, actual.id.clone()));
        let peer = self.peer()?.to_owned();
        let mut private = Zeroizing::new([0; 32]);
        rand::rngs::OsRng.fill_bytes(private.as_mut());
        let keys = WireguardSecrets::from_keys(private, None)?;
        let public = STANDARD.encode(URL_SAFE_NO_PAD.decode(keys.device_public_key()?)?);
        docker(
            &[
                "exec",
                &peer,
                "wg",
                "set",
                "wg0",
                "peer",
                &public,
                "allowed-ips",
                "10.66.0.3/32",
            ],
            &[],
        )
        .await?;
        docker(
            &[
                "exec",
                &peer,
                "ip",
                "route",
                "add",
                "10.66.0.3/32",
                "dev",
                "wg0",
            ],
            &[],
        )
        .await?;
        docker(
            &["exec", "-i", &peer, "nft", "-f", "-"],
            b"add rule ip elx_lab_nat postrouting ip saddr 10.66.0.3 oifname \"eth0\" masquerade\n",
        )
        .await?;
        let peer_key = STANDARD.encode(
            URL_SAFE_NO_PAD.decode(
                &self
                    .identity
                    .as_ref()
                    .context("original lab identity absent")?
                    .peer,
            )?,
        );
        let private = Zeroizing::new(STANDARD.encode(&keys.private_frame()[..32]));
        let config = Zeroizing::new(format!(
            "[Interface]\nPrivateKey = {}\nAddress = 10.66.0.3/32\nMTU = 1280\n[Peer]\nPublicKey = {}\nAllowedIPs = 0.0.0.0/0\nEndpoint = 8.8.8.8:51820\nPersistentKeepalive = 1\n",
            &*private, peer_key,
        ));
        Ok((
            application::runtime(
                self.runtime.clone(),
                actual.id.clone(),
                peer,
                self.owner.clone(),
                self.application_activity.clone(),
                self.application_gateways.clone(),
            ),
            actual.id,
            config,
        ))
    }
    pub fn application_gateway_starts(&self) -> usize {
        self.application_activity
            .gateways
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn application_http_starts(&self) -> usize {
        self.application_activity
            .http
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn application_image_lookups(&self) -> usize {
        self.application_activity
            .image_lookups
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn application_http_modes(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering;
        (
            self.application_activity
                .buffered_http
                .load(Ordering::SeqCst),
            self.application_activity
                .streaming_http
                .load(Ordering::SeqCst),
        )
    }
    pub fn application_source_modes(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering;
        (
            self.application_activity
                .source_matches
                .load(Ordering::SeqCst),
            self.application_activity
                .verified_invokes
                .load(Ordering::SeqCst),
        )
    }
    pub fn application_control_times(
        &self,
    ) -> std::collections::BTreeMap<&'static str, (u64, u128)> {
        self.application_activity.times()
    }
    /// Exact real tunnel's cgroup/process state after multiple invocations.
    /// A timeout's orphan monitor must not accumulate toward the fixed PID cap.
    pub async fn require_application_process_headroom(&self) -> Result<()> {
        let gateways = self.application_gateways.lock().await.clone();
        ensure!(
            gateways.len() == 1,
            "expected the same original warm tunnel"
        );
        let script = r#"set -eu
zombies=0
for file in /proc/[0-9]*/status; do
    while IFS= read -r row; do
        case "$row" in State:*zombie*) zombies=$((zombies+1));; esac
    done < "$file" 2>/dev/null || :
done
IFS= read -r pids < /sys/fs/cgroup/pids.current
read -r name events < /sys/fs/cgroup/pids.events
[ "$name" = max ]
printf '%s\n' "$zombies" "$pids" "$events"
"#;
        let command = [
            "/bin/busybox",
            "timeout",
            "-s",
            "KILL",
            "5",
            "/bin/sh",
            "-ec",
            script,
        ]
        .map(str::to_owned);
        let output = timeout(
            Duration::from_secs(8),
            self.runtime
                .exec_container_command(&gateways[0], "0:0", &command),
        )
        .await??;
        ensure!(output.len() <= 128, "process-headroom observation bounds");
        let counts = output
            .lines()
            .map(str::parse::<u64>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let [zombies, current, limit_hits] = counts.as_slice() else {
            bail!("process-headroom observation shape");
        };
        // A child can briefly be a zombie before PID1's next scheduling turn;
        // fixed tiny slack is not permission for growth over repeated probes.
        ensure!(
            *zombies <= 2 && *current <= 64 && *limit_hits == 0,
            "application process growth: zombies={zombies}, current={current}, PID-limit hits={limit_hits}"
        );
        eprintln!(
            "actual warm tunnel process bounds: zombies={zombies}, current={current}, PID-limit hits={limit_hits}"
        );
        Ok(())
    }
    /// Failure-only, bounded, read-only lab diagnostics. Never dump credentials,
    /// a configuration file, process environment, HTTP input or response data.
    /// Captured handles come only from successful real gateway START calls.
    pub async fn diagnose_application_exit(&self) {
        let gateways = self.application_gateways.lock().await.clone();
        for handle in gateways.iter().rev().take(1) {
            let command = [
                "/bin/busybox", "timeout", "-s", "KILL", "5", "/bin/sh", "-ec",
                "ip route show table all; ip -s link show tun0; iptables-nft -L OUTPUT -n -v; iptables-nft -L INPUT -n -v; iptables-nft -t mangle -L ELIXIR_APP -n -v",
            ].map(str::to_owned);
            let result = timeout(
                Duration::from_secs(8),
                self.runtime.exec_container_command(handle, "0:0", &command),
            )
            .await;
            match result {
                Ok(Ok(output)) if output.len() <= 16 * 1024 => {
                    eprintln!("application VPN lab diagnostics (test only):\n{output}");
                }
                _ => eprintln!("application VPN lab diagnostics unavailable"),
            }
        }
    }
    pub async fn start_slow_http(&self) -> Result<()> {
        // Test infrastructure only, inside the already owned exit namespace.
        // No host port, filesystem state, custom CA, public listener or account.
        let script = r#"from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading,time
lock=threading.Lock()
posts=0
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_GET(self):
  with lock: value=str(posts).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(value)));self.end_headers();self.wfile.write(value)
 def do_POST(self):
  global posts
  size=int(self.headers.get('Content-Length','0'))
  if self.path!='/slow' or size<0 or size>128: self.send_error(400);return
  self.rfile.read(size)
  with lock: posts+=1
  time.sleep(12)
  try:
   self.send_response(200);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'ok')
  except (BrokenPipeError,ConnectionResetError): pass
server=ThreadingHTTPServer(('8.8.4.4',18080),Handler)
server.daemon_threads=True
server.serve_forever()
"#;
        docker(
            &["exec", "-d", self.peer()?, "python3", "-u", "-c", script],
            &[],
        )
        .await?;
        timeout(Duration::from_secs(10), async {
            loop {
                if self.slow_posts().await.is_ok() {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("owned slow HTTP server readiness deadline")??;
        Ok(())
    }
    pub async fn slow_posts(&self) -> Result<usize> {
        let value = docker(&["exec", self.peer()?, "python3", "-c",
            "import urllib.request;o=urllib.request.build_opener(urllib.request.ProxyHandler({}));print(o.open('http://8.8.4.4:18080/count',timeout=1).read(32).decode())"], &[]).await?;
        let count: usize = serde_json::from_str(&value)?;
        ensure!(count <= 2, "unexpected fixture POST count");
        Ok(count)
    }
    async fn container(&mut self, role: &str, network: &[&str]) -> Result<String> {
        let name = format!("elixir-marketplace-{}-{role}", self.owner);
        let labels = HashMap::from([(TEST_LABEL.to_owned(), self.owner.clone())]);
        self.targets.push(Target {
            name: name.clone(),
            labels,
            id: None,
            image: Some(TOOLS.to_owned()),
        });
        let label = format!("{TEST_LABEL}={}", self.owner);
        let mut command = vec![
            "run",
            "-d",
            "--pull=never",
            "--name",
            &name,
            "--label",
            &label,
            "--read-only",
            "--user=0:0",
            "--cap-drop=ALL",
            "--cap-add=NET_ADMIN",
            "--security-opt=no-new-privileges",
            "--memory=64m",
            "--memory-swap=64m",
            "--cpus=0.1",
            "--pids-limit=32",
            "--log-driver=none",
            "--ulimit=core=0:0",
            "--entrypoint=/bin/sleep",
        ];
        command.extend_from_slice(network);
        command.extend([TOOLS, "900"]);
        let id = docker(&command, &[]).await?;
        ensure!(
            crate::runtime::packet_network::immutable_id(&id),
            "lab container identity missing"
        );
        self.targets.last_mut().unwrap().id = Some(id.clone());
        Ok(id)
    }
    /// Always called outside the test's fallible/panicking operation. Stop and
    /// join the original protected owner/guard first, then exact labelled targets.
    pub async fn cleanup(&mut self) -> Result<()> {
        let mut failed = self.participation_start_unknown;
        if let Some(owner) = self.participating.take() {
            failed |= owner.retire().await.is_err();
        }
        if let Some(worker) = self.protected.take() {
            failed |= worker.retire_native_fixture().await.is_err();
        }
        if let Some(guard) = self.guard.take() {
            failed |= guard.stop(self.runtime.as_ref()).await.is_err();
        }
        for target in self.targets.iter().rev() {
            let result = async {
                let Some(handle) = self.runtime.get_container_handle(&target.name).await? else {
                    return Ok(());
                };
                let actual = self
                    .runtime
                    .describe_container_runtime_state(&handle.id)
                    .await?
                    .context("lab cleanup inspection missing")?;
                ensure!(
                    actual.name == target.name
                        && target.id.as_ref().is_none_or(|id| id == &handle.id)
                        && target.image.as_ref().is_none_or(|image| {
                            actual.security.image_reference.as_ref() == Some(image)
                                && actual.security.image_id.as_ref() == Some(image)
                        })
                        && target
                            .labels
                            .iter()
                            .all(|(key, value)| actual.labels.get(key) == Some(value)),
                    "refusing foreign lab cleanup"
                );
                let exact = ContainerHandle {
                    id: handle.id.clone(),
                    name: handle.id.clone(),
                };
                self.runtime.stop_container(&exact).await?;
                ensure!(
                    !self.runtime.inspect(&exact).await?.running,
                    "lab stop unconfirmed"
                );
                self.runtime.remove_container(&exact).await?;
                ensure!(
                    self.runtime
                        .get_container_handle(&handle.id)
                        .await?
                        .is_none(),
                    "lab removal unconfirmed"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            failed |= result.is_err();
        }
        if !failed && let Some((name, labels)) = &self.volume {
            // A lingering/unknown provisioner or foreign volume user makes
            // Docker refuse removal. Preserve its volume and controller then;
            // never infer writer retirement from a CLI timeout or missing PID.
            failed |= self
                .runtime
                .remove_private_file_volume(name, labels)
                .await
                .is_err();
        }
        if !failed && let Some((spec, id)) = &self.application_network {
            failed |= self.runtime.remove_packet_uplink(spec, id).await.is_err();
        }
        if failed {
            if let Some(directory) = self._directory.take() {
                let _ = directory.keep();
            }
            bail!(
                "lab cleanup remains unconfirmed; retained controller at {}",
                self.root.display()
            );
        }
        Ok(())
    }
}

/// Finite setup commands: bounded output and actual original CLI wait, even on
/// deadline. Container identities are retained BEFORE any create is dispatched.
async fn docker(args: &[&str], input: &[u8]) -> Result<String> {
    let mut child = tokio::process::Command::new("docker")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let result = timeout(Duration::from_secs(25), async {
        let write = async {
            stdin.write_all(input).await?;
            stdin.shutdown().await?;
            drop(stdin);
            Ok::<_, std::io::Error>(())
        };
        let read = async {
            let mut bytes = Vec::new();
            stdout.take(128 * 1024 + 1).read_to_end(&mut bytes).await?;
            Ok::<_, std::io::Error>(bytes)
        };
        let diagnostic = async {
            let mut bytes = Zeroizing::new(Vec::new());
            stderr.take(4097).read_to_end(&mut bytes).await?;
            Ok::<_, std::io::Error>(bytes)
        };
        tokio::join!(write, read, diagnostic, child.wait())
    })
    .await;
    match result {
        Ok((write, output, errors, status)) => {
            write?;
            let output = output?;
            let errors = errors?;
            ensure!(
                output.len() <= 128 * 1024 && errors.len() <= 4096 && status?.success(),
                "bounded lab Docker operation failed"
            );
            Ok(String::from_utf8(output)?.trim().to_owned())
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            bail!("lab Docker operation deadline");
        }
    }
}

#[tokio::test]
#[ignore = "explicit public DNS/TLS through local disposable WireGuard exit; not a production/off-site VPN provider"]
async fn native_wireguard_bootstrap_obtains_real_public_dns_tls_health_without_custom_ca()
-> Result<()> {
    use crate::config::DatabaseConfig;
    use futures_util::FutureExt;
    let dir = tempfile::tempdir()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", dir.path().join("network.sqlite").display()),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let mut lab = NetworkLab::new(database)?;
    let guard_image = std::env::var("ELIXIR_MARKETPLACE_GUARD_IMAGE")?;
    let worker_image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    ensure!(
        image_is_pinned(&worker_image),
        "immutable worker image required"
    );
    let outcome=std::panic::AssertUnwindSafe(async {
        lab.start(&guard_image).await?;
        let _actual_monitor=lab.bootstrap(&worker_image).await?;
        eprintln!("real public DNS/TLS bootstrap through disposable local WireGuard exit passed; no custom CA/provider account; excludes off-site ISP privacy, owned Docker uplink, sustained supervision and installed SDK HTTP");
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    let cleanup = lab.cleanup().await;
    cleanup?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
#[ignore = "explicit real public DNS/TLS, resource admission and protected worker; 35-second lease supervision then guard-loss cleanup, not periodic refresh/off-site/P2P/installed HTTP qualification"]
async fn native_protected_worker_renews_multiple_leases_and_guard_loss_fences_calls() -> Result<()>
{
    Box::pin(protected_worker_supervision(false)).await
}

#[tokio::test]
#[ignore = "explicit real public DNS/TLS and 650-second protected-worker supervision beyond bootstrap-health expiry, then guard-loss cleanup; excludes off-site/production uplink/P2P/aggregate playback qualification"]
async fn native_protected_worker_survives_periodic_health_refresh_and_guard_loss() -> Result<()> {
    Box::pin(protected_worker_supervision(true)).await
}

async fn protected_worker_supervision(require_refresh: bool) -> Result<()> {
    use crate::config::DatabaseConfig;
    use futures_util::FutureExt;
    // A Ready lease after the ORIGINAL bootstrap sample's maximum age cannot
    // be sustained by heartbeats alone. Only a successful independent fresh
    // DNS/TLS observation can replace that sample. Use real time, no health or
    // scheduler override; the paused-clock unit test separately checks cadence.
    let bootstrap_age = super::connectivity::MAX_OBSERVATION_AGE;
    let duration = if require_refresh {
        bootstrap_age + Duration::from_secs(50)
    } else {
        Duration::from_secs(35)
    };
    let directory = tempfile::tempdir()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            directory.path().join("protected.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let mut lab = NetworkLab::new(database)?;
    let guard_image = std::env::var("ELIXIR_MARKETPLACE_GUARD_IMAGE")?;
    let worker_image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let outcome = std::panic::AssertUnwindSafe(async {
        lab.start(&guard_image).await?;
        let guard = lab.guard.as_ref().unwrap().handle().clone();
        lab.start_protected(&worker_image).await?;
        eprintln!("actual protected worker started with real bootstrap and host admission; checking {} seconds of real-time supervision", duration.as_secs());
        let worker = lab.protected.as_mut().unwrap();
        super::runtime::inspect_fixed_worker(lab.runtime.as_ref(), worker.handle(), worker.profile()).await?;
        let started = tokio::time::Instant::now();
        let mut replies = 0usize;
        let mut ready_after_bootstrap_expiry = false;
        let mut next_progress = Duration::from_secs(60);
        while started.elapsed() < duration {
            let protection = worker.protection_state();
            ensure!(matches!(protection, ProtectionState::Ready | ProtectionState::Checking),
                "actual protection expired during lease supervision");
            if started.elapsed() > bootstrap_age + Duration::from_secs(20)
                && protection == ProtectionState::Ready
            {
                ready_after_bootstrap_expiry = true;
            }
            let status = worker.network_status().await?;
            ensure!(!status.active && !status.consumer && !status.catalog && !status.routing && status.peers.is_empty(),
                "protection silently enabled peer participation");
            replies += 1;
            if started.elapsed() >= next_progress {
                eprintln!("actual protected supervision: {} seconds, {replies} original worker replies, no participation roles", started.elapsed().as_secs());
                next_progress = started.elapsed() + Duration::from_secs(60);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        ensure!(replies >= 2, "multiple original worker replies required");
        ensure!(!require_refresh || ready_after_bootstrap_expiry,
            "no actual Ready lease after the original bootstrap health evidence expired");
        if require_refresh {
            eprintln!("actual Ready supervision continued beyond original bootstrap-health expiry; fresh periodic DNS/TLS evidence was required; no clock/health override or larger allocation");
        }
        super::runtime::inspect_fixed_worker(lab.runtime.as_ref(), worker.handle(), worker.profile()).await?;
        // Only our retained immutable guard is stopped, not a host interface or
        // provider. The actual supervisor must revoke access independently.
        lab.runtime.stop_container(&guard).await?;
        ensure!(!lab.runtime.inspect(&guard).await?.running, "injected guard loss unconfirmed");
        timeout(Duration::from_secs(45), async {
            while worker.protection_state() != ProtectionState::Failed {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.context("actual guard loss did not fail supervision")?;
        ensure!(worker.network_status().await.is_err(), "lost protection still permits private worker calls");
        eprintln!("actual protected worker survived requested supervision and guard loss fenced calls; original owner cleanup follows; periodic_refresh_observed={require_refresh}; excludes off-site privacy, owned uplink, peer participation and installed SDK HTTP");
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    if let Err(error) = lab.cleanup().await {
        let retained = directory.keep();
        bail!(
            "protected supervision cleanup unconfirmed; retained fixture database at {}: {error}",
            retained.display()
        );
    }
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
