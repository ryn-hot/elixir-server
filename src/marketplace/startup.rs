//! Trusted local protected startup, not a public provider/configuration RPC.
//! Native provider selection requires explicit versioned local consent and a
//! durable one-shot enrollment intent; retries never borrow downloader keys.
use super::{
    accounting::{CommittedTransferReservation, available_transfer, reserve_transfer},
    connectivity::{BOOTSTRAP_PROBE_BYTES, ConnectivityMonitor, ProbePolicy},
    lease::ControllerLease,
    packet::{PacketGuardController, PacketWorkloadBudget},
    preferences,
    runtime::image_is_pinned,
    worker::{ParticipatingWorker, ProtectedWorker, ProtectedWorkerConfig},
};
use crate::{db::Database, runtime::RuntimePaths, secrets::SecretsManager, state::AppState};
use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use elixir_marketplace::{
    canonical,
    connectivity::ProbeFamily,
    identity::{Digest, PublicKey},
    network_probe::WireguardIdentity,
    packet_guard::{PacketPolicy, VpnEndpoint},
    policy::OrdinaryLimits,
    wireguard_setup::{WireguardSecrets, WireguardSetup},
    worker_network::{NetworkConfig, NetworkRoles},
};
use serde::{Deserialize, de::IntoDeserializer};
use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    io::Read,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

const MAX_PROFILE_BYTES: usize = 32 * 1024;
// A one-use prepaid startup window, not a rolling quota or lifetime grant.
// Renewal policy must use fresh durable debits; an expired/exhausted window
// closes independently. Refill/retry are wired; continuous protected operation
// and positive provider qualification remain launch gates.
const INITIAL_WORKER_BYTES: u64 = 1024 * 1024;
const PREFERRED_WORKER_STARTUP_BYTES: u64 = 8 * 1024 * 1024;

fn worker_startup_bytes(available: u64) -> Result<u64> {
    ensure!(
        available >= INITIAL_WORKER_BYTES,
        "marketplace startup transfer allowance exhausted"
    );
    Ok(available.min(PREFERRED_WORKER_STARTUP_BYTES))
}

/// One durable, non-refundable window for protected worker startup. Tor's
/// initial directory/circuit traffic shares this meter; it must not start with
/// a tiny idle-renewal window. This changes neither rate limits nor the monthly
/// cap. Lower remaining budgets retain the existing minimum and fail closed.
pub(in crate::marketplace) async fn reserve_worker_startup_credit(
    database: &Database,
    now: u64,
    limits: &OrdinaryLimits,
) -> Result<CommittedTransferReservation> {
    let available = timeout(
        Duration::from_secs(5),
        available_transfer(database, now, limits),
    )
    .await
    .context("worker startup allowance read deadline")??;
    let bytes = worker_startup_bytes(available)?;
    // The read is only a scheduling hint. Only this original committed debit
    // can become kernel credit; a concurrent debit or uncertain commit grants
    // nothing, and there is no refund, quota reset or automatic retry here.
    timeout(
        Duration::from_secs(5),
        reserve_transfer(database, now, bytes, limits),
    )
    .await
    .context("worker startup allowance debit deadline")?
}

/// Exclusive ownership for reconciling a failed preparation, not a claim that
/// its old processes are already stopped or that protection is healthy.
pub(super) struct StartupLeases {
    directory: PathBuf,
    root: ControllerLease,
    packet: ControllerLease,
    worker: ControllerLease,
}
impl StartupLeases {
    pub(super) fn acquire(directory: &Path) -> Result<Self> {
        Self::acquire_inner(directory, false)
    }
    fn acquire_inner(directory: &Path, provider: bool) -> Result<Self> {
        Ok(Self {
            directory: directory.to_owned(),
            root: ControllerLease::acquire(directory)?,
            packet: ControllerLease::acquire(&directory.join("packet"))?,
            worker: if provider {
                ControllerLease::acquire_for_recovery(&directory.join("worker"))?
            } else {
                ControllerLease::acquire(&directory.join("worker"))?
            },
        })
    }
    pub(super) fn is_current(&self) -> bool {
        self.root.matches_directory(&self.directory)
            && self
                .packet
                .matches_directory(&self.directory.join("packet"))
            && self
                .worker
                .matches_directory(&self.directory.join("worker"))
    }
}
struct StartOwner {
    service: super::service::MarketplaceService,
    initial: Option<super::service::StartupAttempt>,
}
impl StartOwner {
    fn begin(self, leases: &StartupLeases) -> Result<super::service::StartupAttempt> {
        ensure!(leases.is_current(), "marketplace startup ownership changed");
        match self.initial {
            Some(attempt) => Ok(attempt),
            None => self.service.begin_recovery(leases),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    schema_version: u32,
    guard_image: String,
    worker_image: String,
    issuer_keys: Vec<PublicKey>,
    #[serde(default)]
    limits: OrdinaryLimits,
    network: NetworkConfig,
    provider: Provider,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Provider {
    /// Operator-selected immutable implementation; consent and stable
    /// enrollment come only from the separately authenticated durable choice.
    NativeWarpSetup {
        image: String,
    },
    NativeWarp {
        image: String,
        consent_version: String,
        /// Stable purpose-specific setup identity, never regenerated on retry.
        enrollment: Digest,
    },
    SavedWireguard {
        profile_id: String,
    },
    NativeWireguard {
        endpoint_address: Ipv4Addr,
        endpoint_port: u16,
        local_port: u16,
        tunnel_addresses: Vec<Ipv4Addr>,
        peer_public_key: String,
        device_secret: String,
        preshared_secret: Option<String>,
        mtu: u32,
        keepalive_seconds: u16,
        require_warp: bool,
    },
}

impl Profile {
    fn parse(bytes: &[u8]) -> Result<Self> {
        // Local, unsigned configuration intentionally supports omitted defaults.
        // Still reject duplicates/inexact JSON and every ignored nested field;
        // signed protocol records keep their stricter round-trip shape check.
        let raw = canonical::parse(bytes, MAX_PROFILE_BYTES)
            .map_err(|_| anyhow::anyhow!("invalid closed marketplace startup profile"))?;
        let mut ignored = false;
        let value: Self =
            serde_ignored::deserialize(raw.into_deserializer(), |_| ignored = true)
                .map_err(|_| anyhow::anyhow!("invalid closed marketplace startup profile"))?;
        ensure!(!ignored, "unknown marketplace startup field");
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported marketplace startup profile"
        );
        ensure!(
            image_is_pinned(&self.guard_image) && image_is_pinned(&self.worker_image),
            "startup requires immutable images"
        );
        ensure!(
            (1..=4).contains(&self.issuer_keys.len())
                && self.issuer_keys.iter().collect::<BTreeSet<_>>().len() == self.issuer_keys.len(),
            "startup requires distinct trusted issuer keys"
        );
        let split = PacketWorkloadBudget::split(&self.limits)?;
        ensure!(
            self.limits.monthly_transfer_bytes >= BOOTSTRAP_PROBE_BYTES + INITIAL_WORKER_BYTES,
            "startup budget cannot cover its fixed bootstrap and worker debits"
        );
        self.network.validate()?;
        self.network
            .limits
            .validate_against(split.worker_limits())?;
        ensure!(
            self.network.roles
                == (NetworkRoles {
                    consumer: true,
                    catalog: false,
                    routing: false
                }),
            "startup cannot configure contribution roles"
        );
        if let Provider::NativeWarpSetup { image } = &self.provider {
            ensure!(
                image_is_pinned(image),
                "native provider requires an immutable image"
            );
            ensure!(
                self.limits.monthly_transfer_bytes
                    >= BOOTSTRAP_PROBE_BYTES + INITIAL_WORKER_BYTES + 64 * 1024,
                "native provider startup budget is unavailable"
            );
            #[cfg(not(unix))]
            anyhow::bail!("native marketplace provider is not qualified on this platform");
            return Ok(());
        }
        if let Provider::NativeWarp {
            image,
            consent_version,
            enrollment,
        } = &self.provider
        {
            ensure!(
                image_is_pinned(image)
                    && consent_version == elixir_marketplace::warp_provider::CONSENT_VERSION
                    && enrollment.as_bytes() != &[0; 32],
                "native provider requires immutable image and explicit current consent/enrollment"
            );
            ensure!(
                self.limits.monthly_transfer_bytes
                    >= BOOTSTRAP_PROBE_BYTES + INITIAL_WORKER_BYTES + 64 * 1024,
                "native provider startup budget is unavailable"
            );
            #[cfg(not(unix))]
            anyhow::bail!("native marketplace provider is not qualified on this platform");
            return Ok(());
        }
        if let Provider::SavedWireguard { profile_id } = &self.provider {
            ensure!(
                !profile_id.is_empty()
                    && profile_id.len() <= 64
                    && profile_id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
                "invalid saved marketplace network profile identity"
            );
            return Ok(());
        }
        let Provider::NativeWireguard {
            peer_public_key,
            device_secret,
            preshared_secret,
            mtu,
            keepalive_seconds,
            ..
        } = &self.provider
        else {
            unreachable!()
        };
        peer_key(peer_public_key)?;
        secret_key(device_secret)?;
        if let Some(key) = preshared_secret {
            secret_key(key)?;
        }
        ensure!(
            preshared_secret.as_ref() != Some(device_secret),
            "device and preshared secrets must differ"
        );
        ensure!(
            (1280..=9000).contains(mtu) && *keepalive_seconds <= 120,
            "invalid native WireGuard setup bounds"
        );
        self.policy()?.validate()?;
        Ok(())
    }
    fn policy(&self) -> Result<PacketPolicy> {
        let Provider::NativeWireguard {
            endpoint_address,
            endpoint_port,
            local_port,
            tunnel_addresses,
            ..
        } = &self.provider
        else {
            anyhow::bail!("saved network material must be resolved before packet setup");
        };
        Ok(PacketPolicy {
            schema_version: 1,
            epoch: Digest::of(b"unassigned-startup-epoch"),
            tunnel_interface: "wg0".into(),
            tunnel_addresses: tunnel_addresses.iter().map(|ip| (*ip).into()).collect(),
            endpoints: vec![VpnEndpoint {
                address: (*endpoint_address).into(),
                port: *endpoint_port,
                local_port: *local_port,
            }],
            lease_seconds: 15,
            upload_bytes_per_second: self.limits.upload_bytes_per_second,
            download_bytes_per_second: self.limits.download_bytes_per_second,
        })
    }
    fn probe_policy(&self) -> ProbePolicy {
        let require_warp = match &self.provider {
            Provider::NativeWireguard { require_warp, .. } => *require_warp,
            Provider::SavedWireguard { .. } => false,
            Provider::NativeWarp { .. } | Provider::NativeWarpSetup { .. } => true,
        };
        ProbePolicy {
            family: ProbeFamily::Ipv4,
            require_warp,
        }
    }
}
fn secret_key(value: &str) -> Result<()> {
    ensure!(
        value.starts_with("marketplace.vpn.")
            && value.len() > "marketplace.vpn.".len()
            && value.len() <= 64
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')),
        "startup requires a marketplace-only global secret name"
    );
    Ok(())
}

fn peer_key(value: &str) -> Result<String> {
    ensure!(
        value.len() == 44,
        "provider peer key requires canonical WireGuard base64"
    );
    let bytes = STANDARD
        .decode(value)
        .map_err(|_| anyhow::anyhow!("invalid provider peer key"))?;
    ensure!(
        bytes.len() == 32 && STANDARD.encode(&bytes) == value,
        "invalid provider peer key"
    );
    Ok(elixir_marketplace::network_probe::key(&bytes)?)
}

/// No plaintext key, arbitrary env/command or remotely supplied path is read.
/// The caller must already have local server-configuration authority.
#[cfg(all(test, unix))]
fn read_profile(path: &Path) -> Result<Profile> {
    Profile::parse(&read_private_profile_bytes(path)?)
}
#[cfg(unix)]
pub(super) fn read_private_profile_bytes(path: &Path) -> Result<Vec<u8>> {
    ensure!(
        path.is_absolute() && path.canonicalize()? == path,
        "startup profile requires a canonical absolute path"
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .context("opening marketplace startup profile")?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.len() <= MAX_PROFILE_BYTES as u64,
        "startup profile must be a bounded regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.nlink() == 1
                && metadata.mode() & 0o077 == 0,
            "startup profile must be private and unshared under the server account"
        );
    }
    let mut bytes = Vec::new();
    file.take(MAX_PROFILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}
#[cfg(not(unix))]
pub(super) fn read_private_profile_bytes(_path: &Path) -> Result<Vec<u8>> {
    anyhow::bail!("marketplace startup-file privacy is not qualified on this platform")
}

async fn stored_key(
    database: &Database,
    manager: &SecretsManager,
    key: &str,
) -> Result<Zeroizing<[u8; 32]>> {
    secret_key(key)?;
    // The legacy UNIQUE(scope,scope_id,key) permits duplicate NULL-scope rows
    // on both supported databases. Never select an arbitrary first identity.
    // Read at most two bounded ciphertext prefixes, without sorting/history.
    let mut records: Vec<String> = sqlx::query_scalar(
        "SELECT SUBSTR(value_encrypted,1,513) FROM secrets WHERE scope='global' AND scope_id IS NULL AND key=$1 LIMIT 2"
    ).bind(key).fetch_all(&database.pool).await?;
    ensure!(
        records.len() == 1,
        "marketplace VPN secret is missing or ambiguous"
    );
    let ciphertext = records.pop().unwrap();
    ensure!(
        ciphertext.len() <= 512 && SecretsManager::is_encrypted(&ciphertext),
        "marketplace VPN secrets must be encrypted, including development"
    );
    let plaintext = Zeroizing::new(
        manager
            .decrypt(&ciphertext)
            .map_err(|_| anyhow::anyhow!("marketplace VPN secret cannot be decrypted"))?,
    );
    ensure!(
        plaintext.len() == 44,
        "marketplace VPN secret must contain one canonical WireGuard key"
    );
    let bytes = Zeroizing::new(
        STANDARD
            .decode(plaintext.as_bytes())
            .map_err(|_| anyhow::anyhow!("invalid marketplace VPN key encoding"))?,
    );
    let encoded = Zeroizing::new(STANDARD.encode(bytes.as_slice()));
    ensure!(
        bytes.len() == 32
            && encoded.as_str() == plaintext.as_str()
            && bytes.iter().any(|b| *b != 0),
        "invalid marketplace VPN key"
    );
    let mut result = Zeroizing::new([0; 32]);
    result.copy_from_slice(&bytes);
    Ok(result)
}
struct PreparedVpn {
    setup: WireguardSetup,
    secrets: WireguardSecrets,
    identity: WireguardIdentity,
    policy: PacketPolicy,
    binding: Option<saved_wireguard::Binding>,
}
mod saved_wireguard;
pub(crate) fn validate_saved_wireguard(text: &str) -> Result<()> {
    saved_wireguard::validate(text)
}
#[cfg(test)]
pub(in crate::marketplace) async fn saved_connection_for_fixture(
    database: &Database,
    manager: &SecretsManager,
    limits: &OrdinaryLimits,
) -> Result<(WireguardSetup, WireguardSecrets, WireguardIdentity)> {
    let prepared =
        saved_wireguard::resolve(database, manager, "marketplace-network", limits).await?;
    Ok((prepared.setup, prepared.secrets, prepared.identity))
}
async fn prepare_vpn(
    database: &Database,
    manager: &SecretsManager,
    profile: &Profile,
) -> Result<PreparedVpn> {
    // Binding is established by authenticated single-home setup or the trusted
    // local operator tool, never by this startup file or a remote first caller.
    ensure!(
        preferences::load_internal(database).await?.revision > 0,
        "marketplace owner setup is required before startup"
    );
    ensure!(
        available_transfer(database, now()?, &profile.limits).await?
            >= BOOTSTRAP_PROBE_BYTES + INITIAL_WORKER_BYTES,
        "marketplace startup allowance is unavailable"
    );
    if let Provider::SavedWireguard { profile_id } = &profile.provider {
        return saved_wireguard::resolve(database, manager, profile_id, &profile.limits).await;
    }
    let Provider::NativeWireguard {
        peer_public_key,
        device_secret,
        preshared_secret,
        mtu,
        keepalive_seconds,
        ..
    } = &profile.provider
    else {
        unreachable!()
    };
    let device = stored_key(database, manager, device_secret).await?;
    let preshared = match preshared_secret {
        Some(key) => Some(stored_key(database, manager, key).await?),
        None => None,
    };
    let secrets = WireguardSecrets::from_keys(device, preshared)?;
    let device = secrets.device_public_key()?;
    crate::network::protection::reserve_marketplace_network_identity(
        &database.pool,
        manager,
        &device,
    )
    .await?;
    let peer = peer_key(peer_public_key)?;
    let setup = WireguardSetup {
        device_public_key: device.clone(),
        peer_public_key: peer.clone(),
        mtu: *mtu,
        keepalive_seconds: *keepalive_seconds,
    };
    let policy = profile.policy()?;
    setup.validate(&policy)?;
    Ok(PreparedVpn {
        setup,
        secrets,
        identity: WireguardIdentity { device, peer },
        policy,
        binding: None,
    })
}
fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

async fn prepare_worker(
    orchestrator: Arc<crate::orchestrator::OrchestratorService>,
    database: Database,
    manager: &SecretsManager,
    profile: Profile,
    paths: RuntimePaths,
    playback: Arc<crate::playback::PlaybackJobManager>,
    owner: StartOwner,
    provider_setup: Option<&super::provider_setup::Binding>,
) -> Result<Option<saved_wireguard::Binding>> {
    #[cfg(unix)]
    if matches!(&profile.provider, Provider::NativeWarp { .. }) {
        native_provider::prepare(
            orchestrator,
            database,
            manager,
            profile,
            paths,
            playback,
            owner,
            provider_setup,
        )
        .await?;
        return Ok(None);
    }
    let PreparedVpn {
        setup,
        secrets,
        identity,
        policy,
        binding,
    } = timeout(
        Duration::from_secs(10),
        prepare_vpn(&database, manager, &profile),
    )
    .await
    .context("marketplace key preparation deadline")??;
    ProtectedWorker::await_startup_host(&profile.limits, &playback).await?;
    // Honor the server's existing Docker startup policy and report actual
    // engine health; a one-time CLI race must not silently select another runtime.
    orchestrator.ensure_runtime_ready().await?;
    ProtectedWorker::await_startup_host(&profile.limits, &playback).await?;
    let runtime = orchestrator.runtime_manager();
    // Storage lives under the existing server root, not a caller-selected mount
    // or per-retry directory. Separate stable OS leases own guard and worker.
    let directory = PathBuf::from(&paths.data_root)
        .canonicalize()?
        .join("marketplace");
    // Keep ALL controller leases across recovery, bootstrap and construction.
    // Otherwise a stale worker can block bootstrap, or another local controller
    // can start it again between the stop check and the new protected worker.
    let leases = StartupLeases::acquire(&directory)?;
    let attempt = owner.begin(&leases)?;
    let StartupLeases {
        root,
        packet: packet_lease,
        worker: worker_lease,
        ..
    } = leases;
    let deployment = marketplace_deployment(&paths);
    super::worker::recover_owned_worker(runtime.as_ref(), &deployment).await?;
    ConnectivityMonitor::recover_bootstrap_probe(
        runtime.as_ref(),
        &deployment,
        &profile.worker_image,
        PacketWorkloadBudget::split(&profile.limits)?.worker_limits(),
    )
    .await?;
    saved_wireguard::current(&binding).await?;
    let mut guard = PacketGuardController::start_with_uplink_leased(
        runtime.as_ref(),
        &directory.join("packet"),
        &deployment,
        &profile.guard_image,
        policy,
        &profile.limits,
        packet_lease,
    )
    .await?;
    let guarded = async {
        ProtectedWorker::admit_startup_resources(
            runtime.as_ref(),
            &mut guard,
            &profile.limits,
            &playback,
        )
        .await?;
        guard.inspect_owned(runtime.as_ref()).await?;
        saved_wireguard::current(&binding).await?;
        guard
            .configure_wireguard(runtime.as_ref(), setup, secrets)
            .await?;
        let debit =
            reserve_transfer(&database, now()?, BOOTSTRAP_PROBE_BYTES, &profile.limits).await?;
        let mut monitor = ConnectivityMonitor::default();
        saved_wireguard::current(&binding).await?;
        monitor
            .observe_bootstrap(
                runtime.as_ref(),
                &mut guard,
                &profile.worker_image,
                &identity,
                debit,
                profile.probe_policy(),
            )
            .await?;
        // No public worker is provisioned before independent DNS/TLS and kernel
        // identity succeed. Every later namespace gets fresh durable credit.
        let debit = reserve_worker_startup_credit(&database, now()?, &profile.limits).await?;
        saved_wireguard::current(&binding).await?;
        Ok::<_, anyhow::Error>((monitor, debit))
    }
    .await;
    let (monitor, debit) = match guarded {
        Ok(value) => value,
        Err(error) => {
            let _ = guard.stop(runtime.as_ref()).await;
            return Err(error);
        }
    };
    let probe_policy = profile.probe_policy();
    let worker = ProtectedWorker::start_leased(
        runtime,
        ProtectedWorkerConfig {
            controller_directory: directory.join("worker"),
            deployment,
            image: profile.worker_image,
            issuer_keys: profile.issuer_keys,
            probe_policy,
        },
        database.clone(),
        guard,
        monitor,
        identity,
        debit,
        worker_lease,
    )
    .await?;
    if let Err(error) = saved_wireguard::current(&binding).await {
        worker.stop().await?;
        return Err(error);
    }
    let owner = ParticipatingWorker::start(worker, profile.network, database, playback).await?;
    if let Err(error) = saved_wireguard::current(&binding).await {
        owner.retire().await?;
        return Err(error);
    }
    attempt.attach(owner)?;
    drop(root);
    Ok(binding)
}

pub(super) fn marketplace_deployment(paths: &RuntimePaths) -> String {
    // The extension orchestrator prunes/reset-cleans its own deployment's
    // unknown instance IDs. Its namespace must not include separately owned
    // guard/probe/catalog actors, whose lifecycle and cleanup live here.
    format!("marketplace-{}", paths.deployment_id())
}

struct ConnectionBinding {
    path: PathBuf,
    digest: Digest,
    saved: Option<saved_wireguard::Binding>,
    provider: Option<super::provider_setup::Binding>,
}
impl ConnectionBinding {
    async fn current(&self) -> Result<()> {
        let path = self.path.clone();
        let digest =
            tokio::task::spawn_blocking(move || read_private_profile_bytes(&path).map(Digest::of))
                .await??;
        ensure!(
            digest == self.digest,
            "marketplace startup configuration changed"
        );
        saved_wireguard::current(&self.saved).await?;
        require_provider_consent(self.provider.as_ref()).await
    }
}

async fn require_provider_consent(binding: Option<&super::provider_setup::Binding>) -> Result<()> {
    if let Some(binding) = binding {
        timeout(Duration::from_secs(5), binding.current())
            .await
            .context("provider consent check deadline")??;
    }
    Ok(())
}

/// Read-only trusted-local configuration selection, after HTTP owner checks.
/// Returns no keys, endpoint, image or path; cannot create a startup task.
pub(crate) async fn customer_provider_configuration(state: &AppState) -> Result<Option<Digest>> {
    let Some(path) = state.settings.marketplace.startup_file.clone() else {
        return Ok(None);
    };
    tokio::task::spawn_blocking(move || {
        let bytes = read_private_profile_bytes(&path)?;
        let profile = Profile::parse(&bytes)?;
        Ok(matches!(profile.provider, Provider::NativeWarpSetup { .. }).then(|| Digest::of(bytes)))
    })
    .await
    .context("provider setup configuration read task")?
}

async fn activate_once(state: &AppState, path: &Path) -> Result<ConnectionBinding> {
    let status = state.marketplace.snapshot();
    ensure!(
        status.phase == super::service::RuntimePhase::NotConnected
            || (status.phase == super::service::RuntimePhase::Failed
                && status.participation.is_none()),
        "marketplace activation requires a vacant startup owner"
    );
    let profile_path = path.to_owned();
    let (mut profile, digest) = tokio::task::spawn_blocking(move || {
        let bytes = read_private_profile_bytes(&profile_path)?;
        Ok::<_, anyhow::Error>((Profile::parse(&bytes)?, Digest::of(&bytes)))
    })
    .await??;
    let paths = RuntimePaths::from_roots(
        &state.settings.extensions.storage_root,
        &state.settings.library.local_root,
    );
    let database = Database {
        pool: state.db_pool.clone(),
        driver: state.db_driver,
    };
    let provider = if let Provider::NativeWarpSetup { image } = &profile.provider {
        let binding = timeout(
            Duration::from_secs(10),
            super::provider_setup::capture(&database, digest),
        )
        .await
        .context("provider consent capture deadline")??;
        profile.provider = Provider::NativeWarp {
            image: image.clone(),
            consent_version: elixir_marketplace::warp_provider::CONSENT_VERSION.into(),
            enrollment: binding.enrollment(),
        };
        Some(binding)
    } else {
        None
    };
    // Profile/standing-consent preflight creates no physical owner, enrollment
    // or allowance debit. Capture takes authorization locks, not new consent.
    // A missing file/consent or cancellation there must not manufacture a
    // failed physical generation whose cleanup can never be acknowledged.
    // Recheck after the awaits; begin_startup also atomically checks stop/slot
    // ownership. Actual preparation failures retain the existing strict fence.
    let status = state.marketplace.snapshot();
    let initial = match status.phase {
        super::service::RuntimePhase::NotConnected => Some(state.marketplace.begin_startup()?),
        super::service::RuntimePhase::Failed if status.participation.is_none() => None,
        _ => anyhow::bail!("marketplace activation requires a vacant startup owner"),
    };
    let owner = StartOwner {
        service: state.marketplace.clone(),
        initial,
    };
    let saved = prepare_worker(
        state.orchestrator.clone(),
        database,
        &state.secrets,
        profile,
        paths,
        state.transcodes.clone(),
        owner,
        provider.as_ref(),
    )
    .await?;
    let binding = ConnectionBinding {
        path: path.to_owned(),
        digest,
        saved,
        provider,
    };
    // A local consent/configuration change during bootstrap must retire the
    // original owner too; it cannot be silently acknowledged as current.
    if let Err(error) = binding.current().await {
        state.marketplace.client()?.invalidate_connection();
        return Err(error);
    }
    Ok(binding)
}

/// Main owns and joins this retry task; individual attempts are bounded.
/// No configured path means no
/// task, process, provider contact, accounting mutation or runtime inspection.
/// Native terms acceptance must already be explicit in the private local profile.
pub fn spawn_configured(
    state: &AppState,
    shutdown: CancellationToken,
) -> Option<JoinHandle<Result<()>>> {
    let path = state.settings.marketplace.startup_file.clone()?;
    state.marketplace.select_startup();
    Some(retry::spawn(state.clone(), path, shutdown))
}

#[cfg(unix)]
mod native_provider;
mod retry;
#[cfg(test)]
mod tests;
