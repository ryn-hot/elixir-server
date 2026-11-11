//! Protected process lifecycle and explicit private transport controls, not
//! automatic contribution or a public marketplace administration interface.
//! One independent task owns the packet controller. No caller can obtain its
//! mutable guard or fabricate a credit from peer/account JSON.
use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    evidence::EvidenceBatch,
    identity::{ExtensionId, PublicKey},
    network_probe::WireguardIdentity,
    peer::{PeerOperation, PeerResponse},
    worker_control::{WorkerControlOperation, WorkerControlOutcome, WorkerControlResponse},
    worker_network::{NetworkConfig, NetworkStatus, PeerKey},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};

use super::{WorkerProcess, recover_owned_worker};
use crate::{
    marketplace::{
        accounting::CommittedTransferReservation,
        connectivity::{ConnectivityMonitor, ProbePolicy},
        lease::ControllerLease,
        lifecycle::Exit,
        packet::PacketGuardController,
        runtime::{image_is_pinned, inspect_fixed_worker},
        storage::{FixedStateVolume, FixedWorkerSpec},
    },
    runtime::{
        RuntimeManager,
        model::{ContainerHandle, ELIXIR_DEPLOYMENT_ID_LABEL},
    },
};

mod allowance;
mod blob;
mod participation;
pub(in crate::marketplace) use participation::IndependentAdmission;
pub(in crate::marketplace) use participation::Retirement;
pub(in crate::marketplace) use participation::SourceResourceWait;
pub(in crate::marketplace) use participation::WarmSearchAdmission;
#[cfg(all(test, unix))]
pub(in crate::marketplace) use participation::tests::peer_pipe::Fixture as PeerPipeFixture;
#[cfg(test)]
pub(in crate::marketplace) use participation::tests::synthetic_blob_owner;
mod source_preflight;
pub use participation::{
    PARTICIPATION_DISCLOSURE_VERSION, ParticipatingWorker, ParticipationClient, ParticipationPhase,
    ParticipationSettings, ParticipationSnapshot,
};
pub use source_preflight::{
    DiscoverySourceFact, EvidenceObservation, MetadataObservation, PublicSourceObservation,
    PublicationObservation, SearchSourceObservation, SecurityObservation,
};
pub(in crate::marketplace) use source_preflight::{SdkHttpAuthority, SdkHttpCall, SdkHttpClient};

const RENEW_DEADLINE: Duration = Duration::from_secs(12);
// A foreground caller may wait for this existing inspection/credit/refresh
// cycle BEFORE launching a helper. This is not a native lease or permission
// to send input while protection is Checking.
const CHECKING_DEADLINE: Duration = Duration::from_secs(72);
const INSPECTION_INTERVAL: Duration = Duration::from_secs(10);
const REFRESH_INTERVAL: Duration = Duration::from_secs(300);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(45);

/// Trusted server configuration, never decoded from a marketplace request.
pub struct ProtectedWorkerConfig {
    pub controller_directory: PathBuf,
    pub deployment: String,
    pub image: String,
    pub issuer_keys: Vec<PublicKey>,
    pub probe_policy: ProbePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectionState {
    Starting,
    Checking,
    Ready,
    Expired,
    Stopped,
    Failed,
}

/// Local pre-dispatch result only, never decoded from a peer or status JSON.
/// The same queued request may wait for the routine check to finish; it has not
/// been admitted to private IPC and this carries no positive protection claim.
#[derive(Debug)]
struct ProtectionCheckInProgress;
impl std::fmt::Display for ProtectionCheckInProgress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("marketplace protection is undergoing a routine check")
    }
}
impl std::error::Error for ProtectionCheckInProgress {}

#[derive(Clone, Copy)]
struct Snapshot {
    state: ProtectionState,
    until: Option<Instant>,
}
impl Snapshot {
    fn state(self) -> ProtectionState {
        if matches!(
            self.state,
            ProtectionState::Ready | ProtectionState::Checking
        ) && self.until.is_none_or(|until| Instant::now() >= until)
        {
            ProtectionState::Expired
        } else {
            self.state
        }
    }
}

fn observed_state(status: &watch::Receiver<Snapshot>) -> ProtectionState {
    let snapshot = *status.borrow();
    if status.has_changed().is_err()
        && !matches!(
            snapshot.state,
            ProtectionState::Stopped | ProtectionState::Failed
        )
    {
        ProtectionState::Failed
    } else {
        snapshot.state()
    }
}

async fn send_credit<C: Send + 'static>(
    sender: &mpsc::Sender<CreditRequest<C>>,
    status: &watch::Receiver<Snapshot>,
    credit: C,
) -> Result<()> {
    if !matches!(
        observed_state(status),
        ProtectionState::Ready | ProtectionState::Checking
    ) {
        bail!("marketplace protection is unavailable");
    }
    let (reply, received) = oneshot::channel();
    sender
        .try_send(CreditRequest { credit, reply })
        .map_err(|_| anyhow::anyhow!("marketplace credit slot is unavailable"))?;
    timeout(Duration::from_secs(30), received)
        .await
        .context("marketplace credit acknowledgement deadline")?
        .context("marketplace credit acknowledgement unavailable")?
        .map_err(anyhow::Error::msg)
}

struct CreditRequest<C> {
    credit: C,
    reply: oneshot::Sender<std::result::Result<(), &'static str>>,
}

// Private test seam, not an implementable public network-authorization API.
trait Protection: Send + 'static {
    type Credit: Send + 'static;
    fn renew(&mut self) -> impl Future<Output = Result<Duration>> + Send;
    fn inspect_worker(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn maintain_credit(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn refresh(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn credit(&mut self, credit: Self::Credit) -> impl Future<Output = Result<()>> + Send;
    fn shutdown(self) -> impl Future<Output = Result<()>> + Send;
}

struct Supervisor<C> {
    faults: mpsc::Sender<()>,
    credits: mpsc::Sender<CreditRequest<C>>,
    status: watch::Receiver<Snapshot>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Exit>>,
}

// A cancelled pipe operation can leave uncertain worker state. The private
// client kills its attachment; this guard ALSO stops independent packet lease
// renewal, even when the caller retains the ProtectedWorker after cancellation.
struct StopOnChannelLoss<'a> {
    stop: &'a mut Option<oneshot::Sender<()>>,
    armed: bool,
}
impl<'a> StopOnChannelLoss<'a> {
    fn new(stop: &'a mut Option<oneshot::Sender<()>>) -> Self {
        Self { stop, armed: true }
    }
    fn completed(&mut self, success: bool) {
        self.armed = !success;
    }
}
impl Drop for StopOnChannelLoss<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Some(stop) = self.stop.take()
        {
            let _ = stop.send(());
        }
    }
}
impl<C: Send + 'static> Supervisor<C> {
    fn spawn<P: Protection<Credit = C>>(mut protection: P) -> (Self, oneshot::Receiver<()>) {
        let (credits, receiver) = mpsc::channel(1);
        let (status, observed) = watch::channel(Snapshot {
            state: ProtectionState::Starting,
            until: None,
        });
        let (stop, stopped) = oneshot::channel();
        let (faults, mut failed) = mpsc::channel(1);
        let (ready, readiness) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = tokio::select! {
                biased;
                _ = stopped => Ok(()),
                _ = failed.recv() => Err(anyhow::anyhow!("independent source helper cleanup is unknown")),
                result = supervise(&mut protection, receiver, &status, ready) => result,
            };
            // Publish terminal state before potentially slow Docker cleanup.
            status.send_replace(Snapshot {
                state: if result.is_ok() {
                    ProtectionState::Stopped
                } else {
                    ProtectionState::Failed
                },
                until: None,
            });
            let cleanup = timeout(CLEANUP_DEADLINE, protection.shutdown())
                .await
                .context("protected worker cleanup deadline")
                .and_then(|result| result);
            let exit = Exit::new(result, cleanup);
            exit.trace_failures("protection");
            exit
        });
        (
            Self {
                faults,
                credits,
                status: observed,
                stop: Some(stop),
                task: Some(task),
            },
            readiness,
        )
    }
    fn state(&self) -> ProtectionState {
        observed_state(&self.status)
    }
    fn require_active(&self) -> Result<()> {
        if !matches!(
            self.state(),
            ProtectionState::Ready | ProtectionState::Checking
        ) {
            bail!("marketplace protection is unavailable");
        }
        Ok(())
    }
    fn require_ready(&self) -> Result<()> {
        match self.state() {
            ProtectionState::Ready => Ok(()),
            ProtectionState::Checking => Err(ProtectionCheckInProgress.into()),
            _ => bail!("marketplace protection is not currently ready"),
        }
    }
    // One observation distinguishes a bounded routine check from loss of
    // protection. Background scheduling can race that independent check;
    // it must defer, not turn a healthy refresh into a restart storm.
    fn background_ready(&self) -> Result<bool> {
        match self.state() {
            ProtectionState::Ready => Ok(true),
            ProtectionState::Checking => Ok(false),
            _ => bail!("marketplace protection is unavailable"),
        }
    }
    async fn add_credit(&self, credit: C) -> Result<()> {
        send_credit(&self.credits, &self.status, credit).await
    }
    async fn await_ready(self, ready: oneshot::Receiver<()>) -> Result<Self> {
        let started = timeout(Duration::from_secs(45), ready)
            .await
            .context("protected worker supervisor readiness deadline")
            .and_then(|result| result.context("protected worker supervisor unavailable"))
            .and_then(|()| self.require_active());
        if let Err(readiness) = started {
            // A closed readiness channel is usually the consequence, not the
            // cause. Join original cleanup and preserve its actual operation
            // failure rather than discarding it as a generic channel error.
            self.stop()
                .await
                .context("protected worker supervisor startup failed")?;
            return Err(readiness);
        }
        Ok(self)
    }
    async fn stop(self) -> Result<()> {
        self.finish().await?.result()
    }
    async fn finish(mut self) -> Result<Exit> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let mut task = self.task.take().context("missing protection supervisor")?;
        match timeout(CLEANUP_DEADLINE + Duration::from_secs(2), &mut task).await {
            Ok(result) => result.context("protection supervisor failed"),
            Err(_) => {
                task.abort();
                bail!("protection supervisor stop deadline");
            }
        }
    }
}
impl<C> Drop for Supervisor<C> {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // Dropping a JoinHandle deliberately leaves bounded cleanup running.
        // The native lease expires independently even if the task is stalled.
    }
}

async fn supervise<P: Protection>(
    protection: &mut P,
    mut credits: mpsc::Receiver<CreditRequest<P::Credit>>,
    status: &watch::Sender<Snapshot>,
    ready: oneshot::Sender<()>,
) -> Result<()> {
    let mut ready = Some(ready);
    let mut next_inspection = Instant::now();
    let mut next_refresh = Instant::now() + REFRESH_INTERVAL;
    loop {
        status.send_replace(Snapshot {
            state: ProtectionState::Checking,
            until: Some(Instant::now() + CHECKING_DEADLINE),
        });
        if Instant::now() >= next_inspection {
            timeout(INSPECTION_INTERVAL, protection.inspect_worker())
                .await
                .context("protected worker inspection deadline")?
                .context("protected worker ownership inspection")?;
            next_inspection = Instant::now() + INSPECTION_INTERVAL;
        }
        // This may stop the lease while replacing a prepaid window. Check it
        // before DNS/TLS refresh so an exhausted old window is not mistaken
        // for provider failure. Fresh tunnel evidence still gates renewal.
        timeout(Duration::from_secs(30), protection.maintain_credit())
            .await
            .context("protected worker allowance maintenance deadline")?
            .context("protected worker allowance maintenance")?;
        if Instant::now() >= next_refresh {
            timeout(Duration::from_secs(50), protection.refresh())
                .await
                .context("protected worker connectivity refresh deadline")?
                .context("protected worker DNS/TLS refresh")?;
            next_refresh = Instant::now() + REFRESH_INTERVAL;
        }
        let started = Instant::now();
        let lease = timeout(RENEW_DEADLINE, protection.renew())
            .await
            .context("protected worker renewal deadline")?
            .context("protected worker lease renewal")?;
        if lease.is_zero() || lease > Duration::from_secs(15) || started + lease <= Instant::now() {
            bail!("invalid or already expired protection lease");
        }
        status.send_replace(Snapshot {
            state: ProtectionState::Ready,
            until: Some(started + lease),
        });
        if let Some(ready) = ready.take() {
            let _ = ready.send(());
        }
        let next_check = (Instant::now() + (lease / 3).min(Duration::from_secs(3)))
            .min(next_inspection)
            .min(next_refresh);
        tokio::select! {
            biased;
            _ = sleep_until(next_check) => {},
            Some(request) = credits.recv() => {
                status.send_replace(Snapshot { state: ProtectionState::Checking, until: Some(Instant::now() + Duration::from_secs(20)) });
                let result = timeout(Duration::from_secs(20), protection.credit(request.credit)).await
                    .context("protected worker credit deadline").and_then(|result| result);
                let _ = request.reply.send(if result.is_ok() { Ok(()) } else { Err("marketplace credit apply failed") });
                result?;
            }
        }
    }
}

struct LiveProtection {
    runtime: Arc<dyn RuntimeManager>,
    handle: ContainerHandle,
    profile: FixedWorkerSpec,
    guard: PacketGuardController,
    health: Health,
    policy: ProbePolicy,
    allowance: allowance::Allowance,
}
enum Health {
    Wireguard {
        monitor: ConnectivityMonitor,
        identity: WireguardIdentity,
    },
    #[cfg(unix)]
    Provider(Option<crate::marketplace::packet::ProviderConnectivity>),
}
impl Health {
    fn is_provider(&self) -> bool {
        match self {
            Self::Wireguard { .. } => false,
            #[cfg(unix)]
            Self::Provider(_) => true,
        }
    }
    fn require_current(&self, guard: &PacketGuardController, policy: ProbePolicy) -> Result<()> {
        match self {
            Self::Wireguard { monitor, .. } => {
                monitor.require_current(guard, policy)?;
            }
            #[cfg(unix)]
            Self::Provider(sample) => {
                if !sample
                    .as_ref()
                    .is_some_and(|s| s.is_recent_for(guard) && s.family() == policy.family)
                {
                    bail!("missing current provider DNS/TLS health");
                }
            }
        }
        Ok(())
    }
    fn invalidate(&mut self) {
        match self {
            Self::Wireguard { monitor, .. } => monitor.invalidate(),
            #[cfg(unix)]
            Self::Provider(sample) => *sample = None,
        }
    }
    async fn renew(
        &mut self,
        runtime: &dyn RuntimeManager,
        guard: &mut PacketGuardController,
        policy: ProbePolicy,
    ) -> Result<()> {
        self.require_current(guard, policy)?;
        match self {
            Self::Wireguard { monitor, identity } => {
                monitor
                    .renew_checked(runtime, guard, identity, policy)
                    .await
            }
            #[cfg(unix)]
            Self::Provider(sample) => {
                guard
                    .renew_provider_lease(
                        runtime,
                        sample.as_ref().context("provider health missing")?,
                    )
                    .await
            }
        }
    }
    async fn refresh(
        &mut self,
        runtime: &dyn RuntimeManager,
        handle: &ContainerHandle,
        profile: &FixedWorkerSpec,
        guard: &mut PacketGuardController,
        policy: ProbePolicy,
    ) -> Result<()> {
        self.require_current(guard, policy)?;
        match self {
            Self::Wireguard { monitor, identity } => {
                monitor
                    .observe_fixed_worker(runtime, handle, profile, guard, identity, policy)
                    .await
            }
            #[cfg(unix)]
            Self::Provider(slot) => {
                let previous = slot.take().context("provider health missing")?;
                match crate::marketplace::connectivity::refresh::refresh_provider(
                    runtime, handle, profile, guard, previous,
                )
                .await
                {
                    Ok(next) => {
                        *slot = Some(next);
                        Ok(())
                    }
                    Err(error) => {
                        let _ = guard.pause(runtime).await;
                        Err(error)
                    }
                }
            }
        }
    }
}
impl Protection for LiveProtection {
    type Credit = CommittedTransferReservation;
    async fn maintain_credit(&mut self) -> Result<()> {
        self.health.require_current(&self.guard, self.policy)?;
        self.allowance
            .maintain(self.runtime.as_ref(), &mut self.guard)
            .await
    }
    async fn renew(&mut self) -> Result<Duration> {
        self.health
            .renew(self.runtime.as_ref(), &mut self.guard, self.policy)
            .await?;
        Ok(Duration::from_secs(u64::from(
            self.guard.profile().policy().lease_seconds,
        )))
    }
    async fn inspect_worker(&mut self) -> Result<()> {
        if !self.runtime.inspect(&self.handle).await?.running {
            bail!("protected worker stopped");
        }
        inspect_fixed_worker(self.runtime.as_ref(), &self.handle, &self.profile).await?;
        Ok(())
    }
    async fn refresh(&mut self) -> Result<()> {
        self.health
            .refresh(
                self.runtime.as_ref(),
                &self.handle,
                &self.profile,
                &mut self.guard,
                self.policy,
            )
            .await
    }
    async fn credit(&mut self, credit: Self::Credit) -> Result<()> {
        self.guard.apply_credit(self.runtime.as_ref(), credit).await
    }
    async fn shutdown(mut self) -> Result<()> {
        self.health.invalidate();
        let _ = self.guard.pause(self.runtime.as_ref()).await;
        // Known immutable ID + ownership tuple, not a reusable name. Drifted
        // settings do not turn our process into somebody else's container.
        let stopped = async {
            let actual = self
                .runtime
                .describe_container_runtime_state(&self.handle.id)
                .await?
                .context("protected worker cleanup ownership unavailable")?;
            if actual.name != self.profile.container_spec().name
                || self
                    .profile
                    .container_spec()
                    .labels
                    .iter()
                    .any(|(key, value)| actual.labels.get(key) != Some(value))
            {
                bail!("refusing unowned protected worker cleanup");
            }
            self.runtime.stop_container(&self.handle).await
        }
        .await;
        let guard = self.guard.stop(self.runtime.as_ref()).await;
        // A poisoned pause channel is not evidence that a later successful
        // hard stop failed. Both actual owned process stops remain required.
        stopped.and(guard)
    }
}

pub struct ProtectedWorker {
    // Drop signals protection shutdown before the private worker pipe is lost.
    supervisor: Supervisor<CommittedTransferReservation>,
    worker: WorkerProcess,
    runtime: Arc<dyn RuntimeManager>,
    // Stable server-owned lane shared by foreground source/build/archive work.
    // Neither a peer nor an HTTP request can select another directory.
    source_job_directory: PathBuf,
    source_binding: Option<source_preflight::Binding>,
    catalog_jobs: bool,
    catalog_sync: bool,
    catalog_security: bool,
    catalog_security_ready: bool,
    blob_transport: bool,
    blob_discovery: bool,
    blob_sharing: bool,
    blob: blob::State,
}
impl ProtectedWorker {
    /// Original task retains the guard/provider/OS lease across native create,
    /// start and readiness. Dropping the waiter requests cancellation, never a
    /// replacement or a second enrollment. Public participation is separate.
    #[cfg(unix)]
    pub(in crate::marketplace) fn start_provider(
        runtime: Arc<dyn RuntimeManager>,
        config: ProtectedWorkerConfig,
        database: crate::db::Database,
        guard: PacketGuardController,
        health: crate::marketplace::packet::ProviderConnectivity,
        debit: CommittedTransferReservation,
    ) -> crate::marketplace::provider::Pending<Self> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let request = cancel.clone();
        let task = tokio::spawn(async move {
            let worker = Self::start_owned(
                runtime,
                config,
                database,
                guard,
                Health::Provider(Some(health)),
                debit,
                None,
                Some(request.clone()),
            )
            .await?;
            if request.is_cancelled() {
                worker.stop().await?;
                bail!("protected provider worker startup cancelled");
            }
            Ok(worker)
        });
        crate::marketplace::provider::Pending::new(task, cancel)
    }
    pub(crate) async fn await_startup_host(
        limits: &elixir_marketplace::policy::OrdinaryLimits,
        playback: &crate::playback::PlaybackJobManager,
    ) -> Result<()> {
        participation::await_startup_host(limits, playback).await
    }
    pub(crate) async fn admit_startup_resources(
        runtime: &dyn RuntimeManager,
        guard: &mut PacketGuardController,
        limits: &elixir_marketplace::policy::OrdinaryLimits,
        playback: &crate::playback::PlaybackJobManager,
    ) -> Result<()> {
        participation::admit_startup_resources(runtime, guard, limits, playback).await
    }
    /// Caller supplies a separately enrolled/configured guard, a successful
    /// bootstrap monitor, and an actual committed server debit. No provider
    /// registration, peer discovery or automatic contribution occurs here.
    pub async fn start(
        runtime: Arc<dyn RuntimeManager>,
        config: ProtectedWorkerConfig,
        database: crate::db::Database,
        guard: PacketGuardController,
        monitor: ConnectivityMonitor,
        identity: WireguardIdentity,
        debit: CommittedTransferReservation,
    ) -> Result<Self> {
        Self::start_owned(
            runtime,
            config,
            database,
            guard,
            Health::Wireguard { monitor, identity },
            debit,
            None,
            None,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)] // one-use lease/debit capabilities stay separate from public config
    pub(in crate::marketplace) async fn start_leased(
        runtime: Arc<dyn RuntimeManager>,
        config: ProtectedWorkerConfig,
        database: crate::db::Database,
        guard: PacketGuardController,
        monitor: ConnectivityMonitor,
        identity: WireguardIdentity,
        debit: CommittedTransferReservation,
        lease: ControllerLease,
    ) -> Result<Self> {
        Self::start_owned(
            runtime,
            config,
            database,
            guard,
            Health::Wireguard { monitor, identity },
            debit,
            Some(lease),
            None,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn start_owned(
        runtime: Arc<dyn RuntimeManager>,
        config: ProtectedWorkerConfig,
        database: crate::db::Database,
        mut guard: PacketGuardController,
        mut health: Health,
        debit: CommittedTransferReservation,
        mut held_lease: Option<ControllerLease>,
        cancel: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Self> {
        let provider = health.is_provider();
        let startup = async {
            // Missing health must fail before provisioning/stopping any worker.
            health.require_current(&guard, config.probe_policy)?;
            guard.require_credit_observation()?;
            let deployment = guard
                .profile()
                .container_spec()
                .labels
                .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                .context("missing protected deployment")?;
            if deployment != &config.deployment
                || !image_is_pinned(&config.image)
                || !(1..=4).contains(&config.issuer_keys.len())
            {
                bail!("protected worker configuration and controller differ");
            }
            guard.pause(runtime.as_ref()).await?;
            let features = runtime.image_labels(&config.image).await?;
            if [
                "elixir.marketplace.worker.control",
                "elixir.marketplace.worker.network_control",
            ]
            .iter()
            .any(|key| features.get(*key).map(String::as_str) != Some("1"))
                || features
                    .get("elixir.marketplace.worker.connectivity_probe")
                    .map(String::as_str)
                    != Some("2")
            {
                bail!("protected worker image protocols are unsupported");
            }
            let worker = match &health {
                #[cfg(unix)]
                Health::Provider(sample) => {
                    WorkerProcess::start_provider(
                        runtime.as_ref(),
                        &mut guard,
                        &config.controller_directory,
                        &config.image,
                        &config.issuer_keys,
                        sample.as_ref().context("provider health missing")?,
                        cancel
                            .as_ref()
                            .context("original provider startup task missing")?,
                    )
                    .await?
                }
                Health::Wireguard { monitor, .. } => {
                    let lease = match held_lease.take() {
                        Some(lease) if lease.matches_directory(&config.controller_directory) => {
                            lease
                        }
                        Some(_) => {
                            bail!("worker controller lease does not match startup directory")
                        }
                        None => ControllerLease::acquire(&config.controller_directory)?,
                    };
                    recover_owned_worker(runtime.as_ref(), &config.deployment).await?;
                    let volume = FixedStateVolume::ensure(
                        runtime.as_ref(),
                        &config.deployment,
                        &config.image,
                        guard.profile().budget().worker_limits(),
                    )
                    .await?;
                    let profile = FixedWorkerSpec::protected(
                        &volume,
                        &config.issuer_keys,
                        &guard,
                        monitor,
                        config.probe_policy,
                    )?;
                    WorkerProcess::start_profile(runtime.as_ref(), Arc::new(lease), profile).await?
                }
            };
            let ready = async {
                if cancel
                    .as_ref()
                    .is_some_and(|request| request.is_cancelled())
                {
                    bail!("provider worker cancelled before protected readiness");
                }
                health.require_current(&guard, config.probe_policy)?;
                guard.apply_credit(runtime.as_ref(), debit).await?;
                health
                    .refresh(
                        runtime.as_ref(),
                        &worker.handle,
                        &worker.profile,
                        &mut guard,
                        config.probe_policy,
                    )
                    .await
            }
            .await;
            if let Err(error) = ready {
                let _ = worker.stop(runtime.as_ref()).await;
                return Err(error);
            }
            let source_binding = if features
                .get("elixir.marketplace.worker.source_preflight")
                .map(String::as_str)
                == Some("2")
            {
                match source_preflight::Binding::capture(
                    &guard,
                    &worker.profile,
                    config.probe_policy,
                    &features,
                ) {
                    Ok(binding) => Some(binding),
                    Err(error) => {
                        let _ = worker.stop(runtime.as_ref()).await;
                        return Err(error);
                    }
                }
            } else {
                None
            };
            Ok((
                worker,
                source_binding,
                features
                    .get("elixir.marketplace.worker.catalog_jobs")
                    .map(String::as_str)
                    == Some("1"),
                features
                    .get("elixir.marketplace.worker.catalog_sync")
                    .map(String::as_str)
                    == Some("1"),
                features
                    .get("elixir.marketplace.worker.catalog_security")
                    .map(String::as_str)
                    == Some("1"),
                features
                    .get("elixir.marketplace.worker.blob_transport")
                    .map(String::as_str)
                    == Some("1"),
                features
                    .get("elixir.marketplace.worker.blob_discovery")
                    .map(String::as_str)
                    == Some("1"),
                features
                    .get("elixir.marketplace.worker.blob_sharing")
                    .map(String::as_str)
                    == Some("2"),
            ))
        };
        // The provider's original task must observe physical native outcomes.
        // Each native/private operation has its own deadline; an outer timeout
        // must not drop CREATE/START and allow replacement while it is pending.
        let result = if provider {
            startup.await
        } else {
            timeout(Duration::from_secs(300), startup)
                .await
                .context("protected worker startup deadline")
                .and_then(|r| r)
        };
        let (
            worker,
            source_binding,
            catalog_jobs,
            catalog_sync,
            catalog_security,
            blob_transport,
            blob_discovery,
            blob_sharing,
        ) = match result {
            Ok(worker) => worker,
            Err(error) => {
                health.invalidate();
                let _ = guard.pause(runtime.as_ref()).await;
                if provider {
                    guard
                        .stop(runtime.as_ref())
                        .await
                        .context("failed provider worker startup remains fenced")?;
                }
                return Err(error);
            }
        };
        let allowance =
            allowance::Allowance::new(database, guard.profile().budget().worker_limits().clone());
        let (supervisor, ready) = Supervisor::spawn(LiveProtection {
            runtime: runtime.clone(),
            handle: worker.handle.clone(),
            profile: worker.profile.clone(),
            guard,
            health,
            policy: config.probe_policy,
            allowance,
        });
        let supervisor = match supervisor.await_ready(ready).await {
            Ok(supervisor) => supervisor,
            Err(error) => {
                worker
                    .stop(runtime.as_ref())
                    .await
                    .context("failed protected startup worker cleanup")?;
                return Err(error);
            }
        };
        Ok(Self {
            supervisor,
            worker,
            runtime,
            source_job_directory: config.controller_directory.join("foreground-source"),
            source_binding,
            catalog_jobs,
            catalog_sync,
            catalog_security,
            catalog_security_ready: false,
            blob_transport,
            blob_discovery,
            blob_sharing,
            blob: blob::State::default(),
        })
    }
    pub fn protection_state(&self) -> ProtectionState {
        self.supervisor.state()
    }
    pub(crate) fn catalog_jobs_supported(&self) -> bool {
        self.catalog_jobs
    }
    pub(crate) fn catalog_sync_supported(&self) -> bool {
        self.catalog_jobs && self.catalog_sync
    }
    pub(crate) fn catalog_security_supported(&self) -> bool {
        self.catalog_security
    }
    pub(crate) async fn catalog_heads(
        &mut self,
        extension_ids: Vec<elixir_marketplace::identity::ExtensionId>,
    ) -> Result<WorkerControlResponse> {
        self.supervisor.require_active()?;
        let response = self
            .network_control(WorkerControlOperation::CatalogHeads { extension_ids })
            .await?;
        self.supervisor.require_active()?;
        Ok(response)
    }
    pub(crate) async fn catalog_security(
        &mut self,
        operation: WorkerControlOperation,
    ) -> Result<WorkerControlResponse> {
        if !self.catalog_security
            || !matches!(
                operation,
                WorkerControlOperation::CatalogSecurityBegin { .. }
                    | WorkerControlOperation::CatalogSecurityApply { .. }
                    | WorkerControlOperation::CatalogSecurityFinish { .. }
            )
        {
            bail!("catalog security protocol unavailable");
        }
        self.supervisor.require_active()?;
        let was_ready = self.catalog_security_ready;
        self.catalog_security_ready = false;
        let applying = matches!(
            operation,
            WorkerControlOperation::CatalogSecurityApply { .. }
        );
        let response = self.network_control(operation).await?;
        self.supervisor.require_active()?;
        if applying
            && was_ready
            && matches!(
                response.control_response,
                WorkerControlOutcome::CatalogSecurity { ready: true, .. }
            )
        {
            self.catalog_security_ready = true;
        }
        Ok(response)
    }
    pub(crate) async fn catalog_job(
        &mut self,
        operation: WorkerControlOperation,
    ) -> Result<Option<elixir_marketplace::worker_control::CatalogJobState>> {
        if !self.catalog_jobs {
            bail!("catalog jobs unavailable");
        }
        if matches!(&operation, WorkerControlOperation::CatalogSync { .. }) && !self.catalog_sync {
            bail!("incremental catalog sync unavailable");
        }
        if !matches!(
            operation,
            WorkerControlOperation::CatalogPopulate { .. }
                | WorkerControlOperation::CatalogSync { .. }
                | WorkerControlOperation::CatalogMaintain
                | WorkerControlOperation::CatalogJobStatus
        ) {
            bail!("invalid catalog job operation");
        }
        if !self.supervisor.background_ready()? {
            return Ok(None);
        }
        let response = self.network_control(operation).await?;
        self.supervisor.require_active()?;
        match response.control_response {
            WorkerControlOutcome::CatalogJob { state } => Ok(Some(state)),
            _ => bail!("catalog job rejected"),
        }
    }
    pub fn handle(&self) -> &ContainerHandle {
        &self.worker.handle
    }
    pub fn profile(&self) -> &FixedWorkerSpec {
        &self.worker.profile
    }
    // These are private cached-metadata operations, never source/install or
    // execution capabilities. Checking can continue serving that local cache.
    pub async fn request(&mut self, operation: PeerOperation) -> Result<PeerResponse> {
        operation.validate()?;
        self.supervisor.require_active()?;
        let mut cancellation = StopOnChannelLoss::new(&mut self.supervisor.stop);
        let result = self.worker.client.request(operation).await;
        cancellation.completed(result.is_ok());
        result
    }
    pub async fn merge_evidence(
        &mut self,
        extension_id: ExtensionId,
        batch: EvidenceBatch,
    ) -> Result<WorkerControlResponse> {
        self.supervisor.require_active()?;
        let mut cancellation = StopOnChannelLoss::new(&mut self.supervisor.stop);
        let result = self.worker.client.merge_evidence(extension_id, batch).await;
        cancellation.completed(result.is_ok());
        result
    }
    pub async fn add_credit(&self, debit: CommittedTransferReservation) -> Result<()> {
        self.supervisor.add_credit(debit).await
    }
    /// Trusted parent configuration AFTER evaluating host participation policy.
    /// Starting this lifecycle alone never implies discovery or contribution.
    pub async fn start_network(&mut self, config: NetworkConfig) -> Result<NetworkStatus> {
        config.validate()?;
        if config.roles.catalog && (!self.catalog_security || !self.catalog_security_ready) {
            bail!("catalog security baseline is not reconciled");
        }
        config
            .limits
            .validate_against(&self.worker.profile.limits)?;
        // Old images remain recoverable/stoppable, but must not silently use
        // the former five-second WAN query deadline for new public work.
        if self
            .runtime
            .image_labels(&self.worker.profile.container_spec().image)
            .await?
            .get("elixir.marketplace.worker.wan_query_budget")
            .map(String::as_str)
            != Some("1")
        {
            bail!("protected worker WAN query budget is unsupported");
        }
        self.supervisor.require_ready()?;
        let response = self
            .network_control(WorkerControlOperation::NetworkStart { config })
            .await?;
        self.supervisor.require_active()?;
        network_state(response)
    }
    pub async fn network_status(&mut self) -> Result<NetworkStatus> {
        self.supervisor.require_active()?;
        network_state(
            self.network_control(WorkerControlOperation::NetworkStatus)
                .await?,
        )
    }
    pub async fn refresh_network(&mut self) -> Result<NetworkStatus> {
        self.supervisor.require_ready()?;
        let response = self
            .network_control(WorkerControlOperation::NetworkRefresh)
            .await?;
        self.supervisor.require_active()?;
        network_state(response)
    }
    async fn refresh_background_network(&mut self) -> Result<()> {
        if !self.supervisor.background_ready()? {
            return Ok(());
        }
        let response = self
            .network_control(WorkerControlOperation::NetworkRefresh)
            .await?;
        self.supervisor.require_active()?;
        network_state(response)?;
        Ok(())
    }
    /// No destination URL/IP/socket: only an already admitted Noise identity,
    /// and only the existing closed metadata operations. Reverify returned
    /// evidence before catalog admission; this is not installation authority.
    pub async fn request_peer(
        &mut self,
        peer: PeerKey,
        request: PeerOperation,
    ) -> Result<WorkerControlResponse> {
        self.supervisor.require_ready()?;
        let response = self
            .network_control(WorkerControlOperation::NetworkRequest { peer, request })
            .await?;
        self.supervisor.require_active()?;
        Ok(response)
    }
    pub async fn stop_network(&mut self) -> Result<NetworkStatus> {
        // Stop remains available during a health transition. If the pipe is
        // unavailable, kill the owned worker/guard instead of acknowledging off.
        let result = self
            .network_control(WorkerControlOperation::NetworkStop)
            .await;
        match result.and_then(network_state) {
            Ok(status) => {
                self.blob.reset();
                Ok(status)
            }
            Err(error) => {
                if let Some(stop) = self.supervisor.stop.take() {
                    let _ = stop.send(());
                }
                Err(error)
            }
        }
    }
    async fn network_control(
        &mut self,
        operation: WorkerControlOperation,
    ) -> Result<WorkerControlResponse> {
        let checked = elixir_marketplace::worker_control::WorkerControlRequest {
            schema_version: 1,
            request_id: 1,
            control: operation,
        };
        checked.validate()?;
        let mut cancellation = StopOnChannelLoss::new(&mut self.supervisor.stop);
        let result = self.worker.client.control(checked.control).await;
        cancellation.completed(result.is_ok());
        result
    }
    pub async fn stop(self) -> Result<()> {
        self.finish().await?.result()
    }
    /// Test lifecycle only: consume the ORIGINAL joined cleanup outcome even
    /// when the test deliberately broke protection. This creates no authority.
    #[cfg(test)]
    pub(in crate::marketplace) async fn retire_native_fixture(self) -> Result<()> {
        self.finish().await?.cleanup()
    }
    async fn finish(self) -> Result<Exit> {
        let protection = self.supervisor.finish().await;
        let worker = self.worker.stop(self.runtime.as_ref()).await;
        Ok(protection?.with_cleanup(worker))
    }
}

fn network_state(response: WorkerControlResponse) -> Result<NetworkStatus> {
    match response.control_response {
        WorkerControlOutcome::NetworkState { status } => Ok(status),
        _ => bail!("marketplace network control was rejected"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn private_channel_loss_stops_renewal_even_when_the_worker_owner_is_retained() {
        let (sender, received) = oneshot::channel();
        let mut stop = Some(sender);
        {
            let cancellation = StopOnChannelLoss::new(&mut stop);
            let future = async move {
                let _cancellation = cancellation;
                std::future::pending::<()>().await;
            };
            tokio::pin!(future);
            assert!(
                timeout(Duration::from_millis(5), &mut future)
                    .await
                    .is_err()
            );
        }
        received.await.unwrap();
        assert!(stop.is_none());
        let (sender, mut received) = oneshot::channel();
        let mut stop = Some(sender);
        {
            let mut guard = StopOnChannelLoss::new(&mut stop);
            guard.completed(true);
        }
        assert!(matches!(
            received.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        {
            let mut guard = StopOnChannelLoss::new(&mut stop);
            guard.completed(false);
        }
        received.await.unwrap();
    }

    #[derive(Default)]
    struct Observed {
        renewals: AtomicUsize,
        inspections: AtomicUsize,
        refreshes: AtomicUsize,
        credits: AtomicUsize,
        shutdowns: AtomicUsize,
        renewal_times: Mutex<Vec<Instant>>,
    }
    struct Fixture {
        observed: Arc<Observed>,
        fail_renewal: Option<usize>,
        stall_renewal: Option<usize>,
        stall_refresh: bool,
        fail_refresh: bool,
        fail_credit: bool,
        fail_allowance: bool,
        stall_allowance: bool,
        fail_shutdown: bool,
    }
    impl Fixture {
        fn new() -> (Self, Arc<Observed>) {
            let observed = Arc::new(Observed::default());
            (
                Self {
                    observed: observed.clone(),
                    fail_renewal: None,
                    stall_renewal: None,
                    stall_refresh: false,
                    fail_refresh: false,
                    fail_credit: false,
                    fail_allowance: false,
                    stall_allowance: false,
                    fail_shutdown: false,
                },
                observed,
            )
        }
    }
    impl Protection for Fixture {
        type Credit = u64;
        async fn maintain_credit(&mut self) -> Result<()> {
            if self.fail_allowance {
                bail!("fixture allowance exhausted or unknown");
            }
            if self.stall_allowance {
                std::future::pending::<()>().await;
            }
            Ok(())
        }
        async fn renew(&mut self) -> Result<Duration> {
            let call = self.observed.renewals.fetch_add(1, Ordering::SeqCst) + 1;
            self.observed
                .renewal_times
                .lock()
                .unwrap()
                .push(Instant::now());
            if self.fail_renewal == Some(call) {
                bail!("fixture tunnel failure");
            }
            if self.stall_renewal == Some(call) {
                std::future::pending::<()>().await;
            }
            Ok(Duration::from_secs(15))
        }
        async fn inspect_worker(&mut self) -> Result<()> {
            self.observed.inspections.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn refresh(&mut self) -> Result<()> {
            self.observed.refreshes.fetch_add(1, Ordering::SeqCst);
            if self.fail_refresh {
                bail!("fixture DNS/TLS failure");
            }
            if self.stall_refresh {
                std::future::pending::<()>().await;
            }
            Ok(())
        }
        async fn credit(&mut self, _: u64) -> Result<()> {
            self.observed.credits.fetch_add(1, Ordering::SeqCst);
            if self.fail_credit {
                bail!("fixture unknown credit apply");
            }
            Ok(())
        }
        async fn shutdown(self) -> Result<()> {
            self.observed.shutdowns.fetch_add(1, Ordering::SeqCst);
            if self.fail_shutdown {
                bail!("fixture cleanup unavailable");
            }
            Ok(())
        }
    }
    async fn failed(supervisor: &mut Supervisor<u64>) {
        while supervisor.state() != ProtectionState::Failed {
            supervisor.status.changed().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_source_helper_retirement_stops_renewal_and_joins_owned_cleanup() -> Result<()>
    {
        let (fixture, observed) = Fixture::new();
        let (mut supervisor, ready) = Supervisor::spawn(fixture);
        ready.await?;
        let renewals = observed.renewals.load(Ordering::SeqCst);
        supervisor.faults.try_send(())?;
        failed(&mut supervisor).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(observed.renewals.load(Ordering::SeqCst), renewals);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        supervisor.finish().await?.cleanup()?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn original_startup_failure_survives_readiness_channel_loss_and_joins_cleanup()
    -> Result<()> {
        for allowance in [false, true] {
            let (mut fixture, observed) = Fixture::new();
            fixture.fail_allowance = allowance;
            fixture.fail_renewal = Some(1);
            let (supervisor, ready) = Supervisor::spawn(fixture);
            let Err(error) = supervisor.await_ready(ready).await else {
                bail!("failed fixture became ready");
            };
            let reason = format!("{error:#}");
            assert!(
                reason.contains(if allowance {
                    "fixture allowance exhausted or unknown"
                } else {
                    "fixture tunnel failure"
                }),
                "{reason}"
            );
            assert!(
                reason.contains(if allowance {
                    "allowance maintenance"
                } else {
                    "lease renewal"
                }),
                "{reason}"
            );
            assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_joined_failed_operation_exposes_cleanup_independently_without_hiding_unknown_stop()
    -> Result<()> {
        for unknown_cleanup in [false, true] {
            let (mut fixture, observed) = Fixture::new();
            fixture.fail_renewal = Some(1);
            fixture.fail_shutdown = unknown_cleanup;
            let (supervisor, ready) = Supervisor::spawn(fixture);
            assert!(ready.await.is_err());
            assert_eq!(supervisor.state(), ProtectionState::Failed);
            let exit = supervisor.finish().await?;
            assert_eq!(exit.cleanup().is_err(), unknown_cleanup);
            assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_or_exhausted_allowance_never_publishes_readiness_or_renews() {
        let (mut fixture, observed) = Fixture::new();
        fixture.fail_allowance = true;
        let (supervisor, ready) = Supervisor::spawn(fixture);
        assert!(ready.await.is_err());
        assert_eq!(supervisor.state(), ProtectionState::Failed);
        assert_eq!(observed.renewals.load(Ordering::SeqCst), 0);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        assert!(supervisor.stop().await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_allowance_has_a_deadline_and_owner_loss_interrupts_it() -> Result<()> {
        let start = Instant::now();
        let (mut fixture, observed) = Fixture::new();
        fixture.stall_allowance = true;
        let (supervisor, ready) = Supervisor::spawn(fixture);
        assert!(ready.await.is_err());
        assert_eq!(Instant::now() - start, Duration::from_secs(30));
        assert_eq!(observed.renewals.load(Ordering::SeqCst), 0);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        assert!(supervisor.stop().await.is_err());
        let (mut fixture, observed) = Fixture::new();
        fixture.stall_allowance = true;
        let (supervisor, ready) = Supervisor::spawn(fixture);
        let start = Instant::now();
        tokio::task::yield_now().await;
        drop(supervisor);
        assert!(ready.await.is_err());
        tokio::task::yield_now().await;
        assert_eq!(Instant::now(), start);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn independent_task_renews_while_the_catalog_caller_is_busy() -> Result<()> {
        let (fixture, observed) = Fixture::new();
        let (supervisor, ready) = Supervisor::spawn(fixture);
        ready.await?;
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert_eq!(observed.renewals.load(Ordering::SeqCst), 3);
        assert_eq!(supervisor.state(), ProtectionState::Ready);
        supervisor.stop().await?;
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn failed_tunnel_is_terminal_and_never_renews_or_restarts_again() -> Result<()> {
        let (mut fixture, observed) = Fixture::new();
        fixture.fail_renewal = Some(3);
        let (mut supervisor, ready) = Supervisor::spawn(fixture);
        ready.await?;
        failed(&mut supervisor).await;
        assert!(supervisor.add_credit(123).await.is_err());
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(observed.renewals.load(Ordering::SeqCst), 3);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        assert!(supervisor.stop().await.is_err());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn credit_slot_is_bounded_and_each_accepted_message_is_applied_once() -> Result<()> {
        let (fixture, observed) = Fixture::new();
        let (supervisor, ready) = Supervisor::spawn(fixture);
        ready.await?;
        let (reply, acknowledgement) = oneshot::channel();
        assert!(
            supervisor
                .credits
                .try_send(CreditRequest { credit: 1, reply })
                .is_ok()
        );
        let (reply, _) = oneshot::channel();
        assert!(matches!(
            supervisor
                .credits
                .try_send(CreditRequest { credit: 2, reply }),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        assert!(acknowledgement.await?.is_ok());
        supervisor.add_credit(3).await?;
        assert_eq!(observed.credits.load(Ordering::SeqCst), 2);
        supervisor.stop().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_credit_apply_fails_without_acknowledging_success_or_retrying() -> Result<()> {
        let (mut fixture, observed) = Fixture::new();
        fixture.fail_credit = true;
        let (mut supervisor, ready) = Supervisor::spawn(fixture);
        ready.await?;
        assert!(supervisor.add_credit(123).await.is_err());
        failed(&mut supervisor).await;
        assert_eq!(observed.credits.load(Ordering::SeqCst), 1);
        assert!(supervisor.stop().await.is_err());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_initial_check_has_a_deadline_and_never_publishes_readiness() {
        let start = Instant::now();
        let (mut fixture, observed) = Fixture::new();
        fixture.stall_renewal = Some(1);
        let (supervisor, ready) = Supervisor::spawn(fixture);
        assert!(ready.await.is_err());
        assert_eq!(Instant::now() - start, RENEW_DEADLINE);
        assert_eq!(supervisor.state(), ProtectionState::Failed);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        assert!(supervisor.stop().await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_owner_preempts_a_stalled_initial_check() -> Result<()> {
        let start = Instant::now();
        let (mut fixture, observed) = Fixture::new();
        fixture.stall_renewal = Some(1);
        let (supervisor, ready) = Supervisor::spawn(fixture);
        tokio::task::yield_now().await;
        drop(supervisor);
        assert!(ready.await.is_err());
        tokio::task::yield_now().await;
        assert_eq!(Instant::now(), start);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn worker_inspection_and_periodic_refresh_are_not_starved_by_heartbeats() -> Result<()> {
        let start = Instant::now();
        let (mut fixture, observed) = Fixture::new();
        fixture.fail_refresh = true;
        let (mut supervisor, ready) = Supervisor::spawn(fixture);
        ready.await?;
        failed(&mut supervisor).await;
        assert_eq!(Instant::now() - start, REFRESH_INTERVAL);
        assert!(observed.inspections.load(Ordering::SeqCst) >= 30);
        assert_eq!(observed.refreshes.load(Ordering::SeqCst), 1);
        assert!(supervisor.stop().await.is_err());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn stop_during_refresh_discards_queued_credit_without_applying_or_acknowledging_it()
    -> Result<()> {
        let (mut fixture, observed) = Fixture::new();
        fixture.stall_refresh = true;
        let (supervisor, ready) = Supervisor::spawn(fixture);
        ready.await?;
        tokio::time::sleep(REFRESH_INTERVAL + Duration::from_secs(1)).await;
        assert_eq!(observed.refreshes.load(Ordering::SeqCst), 1);
        let (reply, acknowledgement) = oneshot::channel();
        assert!(
            supervisor
                .credits
                .try_send(CreditRequest { credit: 123, reply })
                .is_ok()
        );
        supervisor.stop().await?;
        assert!(acknowledgement.await.is_err());
        assert_eq!(observed.credits.load(Ordering::SeqCst), 0);
        assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn background_admission_defers_routine_checks_but_rejects_expiry_and_lost_owner()
    -> Result<()> {
        let (sender, status) = watch::channel(Snapshot {
            state: ProtectionState::Ready,
            until: Some(Instant::now() + Duration::from_secs(15)),
        });
        let (credits, _) = mpsc::channel::<CreditRequest<u64>>(1);
        let (faults, _) = mpsc::channel(1);
        let supervisor = Supervisor {
            faults,
            credits,
            status,
            stop: None,
            task: None,
        };
        assert!(supervisor.background_ready()?);
        supervisor.require_ready()?;
        sender.send_replace(Snapshot {
            state: ProtectionState::Checking,
            until: Some(Instant::now() + Duration::from_secs(15)),
        });
        assert!(!supervisor.background_ready()?);
        assert!(supervisor.require_active().is_ok());
        assert!(
            supervisor
                .require_ready()
                .unwrap_err()
                .is::<ProtectionCheckInProgress>()
        );
        assert!(
            !anyhow::anyhow!("marketplace protection is undergoing a routine check")
                .is::<ProtectionCheckInProgress>(),
            "peer text is not local pre-dispatch provenance"
        );
        tokio::time::advance(Duration::from_secs(16)).await;
        assert_eq!(supervisor.state(), ProtectionState::Expired);
        assert!(supervisor.background_ready().is_err());
        assert!(
            !supervisor
                .require_ready()
                .unwrap_err()
                .is::<ProtectionCheckInProgress>()
        );
        sender.send_replace(Snapshot {
            state: ProtectionState::Checking,
            until: Some(Instant::now() + Duration::from_secs(15)),
        });
        drop(sender);
        assert_eq!(supervisor.state(), ProtectionState::Failed);
        assert!(supervisor.background_ready().is_err());
        assert!(
            !supervisor
                .require_ready()
                .unwrap_err()
                .is::<ProtectionCheckInProgress>()
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn stale_or_closed_status_cannot_remain_ready() -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        let snapshot = Snapshot {
            state: ProtectionState::Ready,
            until: Some(deadline),
        };
        assert_eq!(snapshot.state(), ProtectionState::Ready);
        tokio::time::sleep(Duration::from_secs(16)).await;
        assert_eq!(snapshot.state(), ProtectionState::Expired);
        let (sender, status) = watch::channel(Snapshot {
            state: ProtectionState::Checking,
            until: Some(deadline),
        });
        let (credits, _) = mpsc::channel::<CreditRequest<u64>>(1);
        let (faults, _) = mpsc::channel(1);
        let supervisor = Supervisor {
            faults,
            credits,
            status,
            stop: None,
            task: None,
        };
        drop(sender);
        assert_eq!(supervisor.state(), ProtectionState::Failed);
        assert!(supervisor.require_active().is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "isolated native guard admission; no provider registration or public network"]
    async fn missing_health_refuses_provisioning_preserves_foreign_worker_and_keeps_real_debit_spent()
    -> Result<()> {
        use crate::{
            config::DatabaseConfig,
            db::Database,
            marketplace::{accounting::reserve_transfer, packet::PacketGuardProfile},
            runtime::docker::DockerRuntimeManager,
        };
        use elixir_marketplace::{
            connectivity::ProbeFamily,
            identity::Digest,
            packet_guard::{PacketPolicy, VpnEndpoint},
            policy::{OrdinaryLimits, TransferLedger},
        };
        use std::os::unix::fs::PermissionsExt;

        async fn docker(args: &[&str]) -> Result<String> {
            let output = timeout(
                Duration::from_secs(20),
                tokio::process::Command::new("docker")
                    .args(args)
                    .kill_on_drop(true)
                    .output(),
            )
            .await??;
            if !output.status.success() {
                bail!("isolated protected admission fixture Docker operation failed");
            }
            Ok(String::from_utf8(output.stdout)?.trim().to_owned())
        }
        struct Cleanup {
            names: Vec<String>,
            owner: String,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                for name in &self.names {
                    for label in [
                        "elixir.deployment_id",
                        "elixir.marketplace.protected_fixture",
                    ] {
                        let inspected = std::process::Command::new("docker")
                            .args([
                                "inspect",
                                "--format",
                                &format!("{{{{index .Config.Labels {label:?}}}}}"),
                                name,
                            ])
                            .output();
                        if inspected.is_ok_and(|value| {
                            value.status.success()
                                && String::from_utf8_lossy(&value.stdout).trim() == self.owner
                        }) {
                            let _ = std::process::Command::new("docker")
                                .args(["rm", "-f", name])
                                .output();
                            break;
                        }
                    }
                }
            }
        }
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        let database = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", directory.join("server.sqlite").display()),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/0060_marketplace_transfer_accounting.sql"
        ))
        .execute(&database.pool)
        .await?;
        let runtime = Arc::new(DockerRuntimeManager::new(None));
        let guard_image = std::env::var("ELIXIR_MARKETPLACE_PACKET_GUARD_IMAGE")
            .unwrap_or_else(|_| "elixir-marketplace-netguard:development".to_owned());
        let worker_image = std::env::var("ELIXIR_MARKETPLACE_CONNECTIVITY_IMAGE")
            .unwrap_or_else(|_| "elixir-marketplace-worker:connectivity-probe".to_owned());
        let guard_image =
            docker(&["image", "inspect", "--format", "{{.Id}}", &guard_image]).await?;
        let worker_image =
            docker(&["image", "inspect", "--format", "{{.Id}}", &worker_image]).await?;
        let owner = format!("admission-{}", uuid::Uuid::new_v4().simple());
        let worker_name = format!("elixir-marketplace-worker-{owner}");
        let limits = OrdinaryLimits::default();
        let profile = PacketGuardProfile::isolated(
            &owner,
            &guard_image,
            PacketPolicy {
                schema_version: 1,
                epoch: Digest::of(b"discarded-test-epoch"),
                tunnel_interface: "wg0".to_owned(),
                tunnel_addresses: vec!["10.66.0.2".parse()?],
                endpoints: vec![VpnEndpoint {
                    address: "8.8.8.8".parse()?,
                    port: 51820,
                    local_port: 51821,
                }],
                lease_seconds: 15,
                upload_bytes_per_second: 32768,
                download_bytes_per_second: 131072,
            },
            &limits,
        )?;
        let cleanup = Cleanup {
            names: vec![profile.container_spec().name.clone(), worker_name.clone()],
            owner: owner.clone(),
        };
        let guard = PacketGuardController::start_isolated(
            runtime.as_ref(),
            &directory.join("guard-controller"),
            profile,
        )
        .await?;
        let foreign = docker(&[
            "create",
            "--name",
            &worker_name,
            "--label",
            &format!("elixir.marketplace.protected_fixture={owner}"),
            "--network=none",
            &worker_image,
        ])
        .await?;
        let worker_directory = directory.join("worker-controller");
        let key = PublicKey::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[3; 32])
                .verifying_key()
                .to_bytes(),
        )?;
        let debit = reserve_transfer(&database, 1_790_000_000, 64 * 1024, &limits).await?;
        let result = ProtectedWorker::start(
            runtime.clone(),
            ProtectedWorkerConfig {
                controller_directory: worker_directory.clone(),
                deployment: owner.clone(),
                image: worker_image,
                issuer_keys: vec![key],
                probe_policy: ProbePolicy {
                    family: ProbeFamily::Ipv4,
                    require_warp: false,
                },
            },
            database.clone(),
            guard,
            ConnectivityMonitor::default(),
            WireguardIdentity {
                device: elixir_marketplace::network_probe::key(&[17; 32])?,
                peer: elixir_marketplace::network_probe::key(&[34; 32])?,
            },
            debit,
        )
        .await;
        assert!(
            matches!(result, Err(error) if error.to_string().contains("independent connectivity"))
        );
        assert!(
            !worker_directory.exists(),
            "health failure must precede controller/storage provisioning"
        );
        let preserved = runtime
            .get_container_handle(&worker_name)
            .await?
            .context("foreign worker disappeared")?;
        assert_eq!(preserved.id, foreign);
        assert!(!runtime.inspect(&preserved).await?.running);
        assert!(
            docker(&[
                "volume",
                "ls",
                "--filter",
                &format!("label=elixir.deployment_id={owner}"),
                "--quiet"
            ])
            .await?
            .is_empty()
        );
        let state: String = sqlx::query_scalar(
            "SELECT state_json FROM marketplace_transfer_accounting WHERE singleton=1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            TransferLedger::restore(state.as_bytes())?.used_bytes(),
            64 * 1024
        );
        database.pool.close().await;
        drop(cleanup);
        Ok(())
    }
}
