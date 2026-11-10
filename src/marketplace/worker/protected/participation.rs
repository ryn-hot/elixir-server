//! One owner for automatic transport roles and the private metadata pipe.
//! Host observations are collected locally, not deserialized from a peer or UI.
//! This does not register a provider or publish support. Installed-byte sharing
//! requires a separate current parent lease and a capable protected worker.
pub(super) mod metrics;
pub(in crate::marketplace) use metrics::IndependentAdmission;
pub(in crate::marketplace) use metrics::SourceAdmissionDeferred as SourceResourceWait;
mod blob_requests;
mod catalog_progress;
mod catalog_setup;
mod catalog_snapshot;
mod head_batch;
mod retirement;
pub use catalog_progress::CatalogSnapshot;
pub(in crate::marketplace) use retirement::Confirmed as Retirement;
mod security_mirror;
#[cfg(test)]
pub(in crate::marketplace) mod tests;

use super::{
    CommittedTransferReservation, CreditRequest, ProtectedWorker, ProtectionState, Snapshot,
    observed_state, send_credit,
};
use crate::marketplace::provider_lookup::Completion as LookupCompletion;
use crate::{db::Database, playback::PlaybackJobManager};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use elixir_marketplace::{
    canonical,
    evidence::EvidenceBatch,
    identity::ExtensionId,
    peer::{PeerOperation, PeerResponse},
    policy::{
        Blocker, ContributionMode, HostEvidence, Metering, OrdinaryLimits, ParticipationController,
    },
    worker_blob::BlobReadState,
    worker_control::{
        CATALOG_CONTROL_BUDGET, CatalogJobKind, CatalogJobPhase, CatalogJobState,
        WorkerControlOperation, WorkerControlOutcome, WorkerControlResponse,
    },
    worker_network::{NetworkConfig, NetworkRoles, NetworkStatus, PeerKey},
};
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};

pub const PARTICIPATION_DISCLOSURE_VERSION: u32 = 1;

pub(super) async fn admit_startup_resources(
    runtime: &dyn crate::runtime::RuntimeManager,
    guard: &mut crate::marketplace::packet::PacketGuardController,
    limits: &OrdinaryLimits,
    playback: &PlaybackJobManager,
) -> Result<()> {
    metrics::admit_startup_resources(runtime, guard, limits, playback).await
}
pub(super) async fn await_startup_host(
    limits: &OrdinaryLimits,
    playback: &PlaybackJobManager,
) -> Result<()> {
    metrics::await_startup_host(limits, playback).await
}
const OBSERVATION_INTERVAL: Duration = Duration::from_secs(10);
const SAMPLE_MAX_AGE: Duration = Duration::from_secs(30);
const OWNER_STOP_DEADLINE: Duration = Duration::from_secs(72);

#[cfg(test)]
impl ProtectedWorker {
    /// Native transport fixture only: exercise the SAME durable server-side
    /// baseline replay used by participation. No peer flag or production bypass.
    pub(in crate::marketplace) async fn reconcile_native_catalog(
        &mut self,
        database: &Database,
    ) -> Result<()> {
        let _ = security_mirror::Mirror::initialize(self, database).await?;
        Ok(())
    }
}

/// Server-owned owner choices. These are separate from paid-support consent.
/// A product handler must durably authorize/store a choice before publishing it.
#[derive(Clone, Copy, Debug)]
pub struct ParticipationSettings {
    pub revision: Option<i64>,
    pub mode: ContributionMode,
    pub metering: Metering,
    pub accepted_disclosure_version: Option<u32>,
}
impl Default for ParticipationSettings {
    fn default() -> Self {
        Self {
            revision: None,
            mode: ContributionMode::Auto,
            metering: Metering::Unknown,
            accepted_disclosure_version: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParticipationPhase {
    Starting,
    Running,
    Applying,
    WaitingForProtection,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct ParticipationSnapshot {
    pub phase: ParticipationPhase,
    /// Last committed settings revision evaluated by the controller. None
    /// means unavailable, not acknowledgement of the caller's saved settings.
    pub preferences_revision: Option<i64>,
    pub desired_roles: NetworkRoles,
    /// None means unknown, NOT a successful off acknowledgement.
    pub applied_roles: Option<NetworkRoles>,
    pub blockers: Vec<Blocker>,
    pub host_sample_current: bool,
    pub recovery_seconds: Option<u64>,
    /// Installed-archive sharing and independent protected inbound reachability
    /// are separate unfinished integrations; never advertise them as active.
    pub package_sharing_active: bool,
    pub routing_qualified: bool,
    /// Progress only, not public-source eligibility or global coverage.
    pub catalog: Option<CatalogSnapshot>,
}
impl Default for ParticipationSnapshot {
    fn default() -> Self {
        Self {
            phase: ParticipationPhase::Starting,
            preferences_revision: None,
            desired_roles: NetworkRoles::default(),
            applied_roles: None,
            blockers: vec![],
            host_sample_current: false,
            recovery_seconds: None,
            package_sharing_active: false,
            routing_qualified: false,
            catalog: None,
        }
    }
}

enum Request {
    CatalogSetup(catalog_setup::Page),
    Sharing(
        WorkerControlOperation,
        oneshot::Sender<Result<elixir_marketplace::worker_sharing::SharingState>>,
    ),
    Providers(
        elixir_marketplace::identity::Digest,
        elixir_marketplace::identity::Digest,
        oneshot::Sender<LookupCompletion>,
    ),
    Blob(
        WorkerControlOperation,
        oneshot::Sender<Result<BlobReadState>>,
    ),
    Cached(PeerOperation, oneshot::Sender<Result<PeerResponse>>),
    Heads(
        Vec<ExtensionId>,
        oneshot::Sender<Result<WorkerControlResponse>>,
    ),
    Peer(
        PeerKey,
        PeerOperation,
        oneshot::Sender<Result<WorkerControlResponse>>,
    ),
    Merge(
        ExtensionId,
        EvidenceBatch,
        oneshot::Sender<Result<WorkerControlResponse>>,
    ),
    Security(
        WorkerControlOperation,
        oneshot::Sender<Result<WorkerControlResponse>>,
    ),
    Peers(oneshot::Sender<Result<NetworkStatus>>),
    Refresh(oneshot::Sender<Result<NetworkStatus>>),
}

impl Request {
    fn transfer_io(&self) -> bool {
        matches!(self, Self::Sharing(..))
            || matches!(self, Self::Blob(operation, _) if transfer_io(operation))
    }
}

fn transfer_io(operation: &WorkerControlOperation) -> bool {
    matches!(operation, WorkerControlOperation::BlobRead { .. })
}

// One slot per lane, still one serial native owner. A known-unsent metadata
// request may wait through Checking without withholding the commands that
// renew/drain an existing transfer. START/discovery never use this lane. CANCEL
// stays ordered behind START so an early no-op cancel cannot launch work later.
struct RequestInbox {
    regular: mpsc::Receiver<Request>,
    transfer: mpsc::Receiver<Request>,
}

// Local negative provenance only: try_send returned Full before any IPC.
// A peer's text, closed owner or uncertain acknowledgement cannot mint this.
#[derive(Debug, thiserror::Error)]
#[error("marketplace request slot is unavailable")]
struct RequestQueueBusy;

// The typed local deferral is produced only before native IPC. Text from a
// peer, a lost acknowledgement, or a later transport failure cannot replay an
// uncertain operation. Returning the original sender preserves cancellation.
fn reply_or_wait<T>(
    reply: oneshot::Sender<Result<T>>,
    result: Result<T>,
) -> Option<oneshot::Sender<Result<T>>> {
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.is::<super::ProtectionCheckInProgress>())
    {
        Some(reply)
    } else {
        let _ = reply.send(result);
        None
    }
}

/// Cloning a client does not create another worker or another request queue.
#[derive(Clone)]
pub struct ParticipationClient {
    blob_discovery: bool,
    blob_sharing: bool,
    database: Database,
    requests: mpsc::Sender<Request>,
    transfer_requests: mpsc::Sender<Request>,
    status: watch::Receiver<ParticipationSnapshot>,
    protection: watch::Receiver<Snapshot>,
    credits: mpsc::Sender<CreditRequest<CommittedTransferReservation>>,
    source_preflight: Option<super::source_preflight::Client>,
    source_jobs: Option<Arc<metrics::Source>>,
    security_mirror: Option<Arc<security_mirror::Mirror>>,
    faults: mpsc::Sender<()>,
    retirement: retirement::Observation,
}
impl ParticipationClient {
    pub(in crate::marketplace) const SHARING_RENEWAL_LEAD: Duration =
        super::source_preflight::SHARING_RENEWAL_LEAD;

    pub(in crate::marketplace) fn queue_busy(error: &anyhow::Error) -> bool {
        error.is::<RequestQueueBusy>()
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_queue_busy() -> anyhow::Error {
        RequestQueueBusy.into()
    }
    pub(in crate::marketplace) async fn original_retirement(
        &self,
        expires: Instant,
    ) -> Result<Retirement> {
        self.retirement.wait(expires).await
    }
    pub(in crate::marketplace) fn accepts_retirement(&self, proof: &Retirement) -> bool {
        self.retirement.accepts(proof)
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_preparation_diagnostics(&self) -> serde_json::Value {
        serde_json::json!({
            "participation": self.snapshot(),
            "source": self.source_preflight.as_ref().map(|source| source.fixture_preparation_diagnostics()),
        })
    }
    // The existing peer-network budget permits a 20s lookup, 120s onion dial
    // and 15s Noise handshake. Allow that ONE attempt plus 10s of scheduling
    // margin; do not end a cold search before its allowed dial can complete.
    // This changes no transport timeout, resource cap or number of attempts.
    pub(in crate::marketplace) const DISCOVERY_WAIT: Duration = Duration::from_secs(165);
    pub(in crate::marketplace) async fn prepare_catalog_page(
        &self,
        peer: PeerKey,
    ) -> Result<Option<CatalogJobState>> {
        if !self.catalog_preparation_ready()? {
            return Ok(None);
        }
        if self.requests.capacity() == 0 {
            return Ok(None);
        }
        let admission = self
            .source_preflight
            .as_ref()
            .context("catalog verification is unavailable")?
            .clone();
        let (send, reply) = oneshot::channel();
        self.enqueue(
            Request::CatalogSetup(catalog_setup::Page {
                peer,
                started: false,
                reconnect_until: None,
                admission,
                reply: send,
            }),
            reply,
        )
        .await
    }
    pub(in crate::marketplace) fn sharing_capable(&self) -> bool {
        self.blob_sharing && self.require_blob_owner().is_ok()
    }
    pub(in crate::marketplace) fn sharing_supported(&self) -> bool {
        self.blob_sharing
            && self.snapshot().phase == ParticipationPhase::Running
            && self
                .snapshot()
                .applied_roles
                .is_some_and(|roles| roles.catalog)
    }
    pub(in crate::marketplace) fn sharing_verification_deferred(error: &anyhow::Error) -> bool {
        super::source_preflight::verification_deferred(error)
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_sharing_deferral() -> anyhow::Error {
        super::source_preflight::fixture_sharing_deferral()
    }
    pub(in crate::marketplace) async fn sharing_control(
        &self,
        operation: WorkerControlOperation,
    ) -> Result<elixir_marketplace::worker_sharing::SharingState> {
        if !self.sharing_supported() {
            bail!("installed sharing unavailable");
        }
        if !matches!(
            operation,
            WorkerControlOperation::SharingSet { .. }
                | WorkerControlOperation::SharingPoll
                | WorkerControlOperation::SharingChunk { .. }
                | WorkerControlOperation::SharingAuthorize { .. }
        ) {
            bail!("invalid installed sharing command");
        }
        let (send, reply) = oneshot::channel();
        self.enqueue(Request::Sharing(operation, send), reply).await
    }
    /// Optional capability from this actual protected actor, not an endpoint,
    /// JSON observation or permission to send an extension's request. Installed
    /// callers still need current standing grants and their native executor.
    pub(in crate::marketplace) fn installed_http(
        &self,
    ) -> Option<super::source_preflight::SdkHttpClient> {
        self.source_preflight.as_ref()?.sdk_http()
    }
    /// Same main-server database captured by the actual protected owner. It is
    /// never selected by an extension, peer response or preparation request.
    pub(in crate::marketplace) fn source_database(&self) -> &Database {
        &self.database
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn installed_configuration(
        &self,
        budget: crate::marketplace::quarantine::SourceJobBudget,
        retention: crate::marketplace::archive::ArchiveBudget,
    ) -> Result<crate::marketplace::grant_store::installation::runtime::InstalledRuntime> {
        self.require_blob_owner()?;
        self.source_jobs
            .as_ref()
            .context("foreground source jobs unavailable")?
            .installed_configuration(budget, retention)
    }
    pub(in crate::marketplace) fn provider_discovery_enabled(&self) -> bool {
        self.blob_discovery && self.require_blob_owner().is_ok()
    }
    pub(in crate::marketplace) async fn find_blob_providers(
        &self,
        sha256: elixir_marketplace::identity::Digest,
    ) -> LookupCompletion {
        if !self.provider_discovery_enabled() {
            return LookupCompletion::not_dispatched(anyhow::anyhow!(
                "protected provider discovery is unavailable"
            ));
        }
        let job = elixir_marketplace::identity::Digest::of(uuid::Uuid::new_v4().as_bytes());
        let (send, reply) = oneshot::channel();
        // Keep admission and acknowledgement distinct. Generic enqueue's Err
        // cannot tell whether a request was never queued or lost after dispatch.
        if self
            .requests
            .try_send(Request::Providers(job, sha256, send))
            .is_err()
        {
            return LookupCompletion::not_dispatched(anyhow::anyhow!(
                "marketplace request slot is unavailable"
            ));
        }
        match timeout(Duration::from_secs(30), reply).await {
            Ok(Ok(completion)) => completion.revalidate(self.require_blob_owner()),
            Ok(Err(error)) => LookupCompletion::unconfirmed(
                anyhow::Error::new(error).context("marketplace provider request owner stopped"),
            ),
            Err(error) => LookupCompletion::unconfirmed(
                anyhow::Error::new(error)
                    .context("marketplace provider request acknowledgement deadline"),
            ),
        }
    }
    pub(in crate::marketplace) fn source_resources_deferred(error: &anyhow::Error) -> bool {
        error.is::<metrics::SourceAdmissionDeferred>()
    }
    pub(in crate::marketplace) fn source_resource_wait(
        error: &anyhow::Error,
    ) -> Option<SourceResourceWait> {
        error.downcast_ref::<SourceResourceWait>().copied()
    }
    /// Exact source-verification scheduling reason for qualification, not the
    /// separate installed-source quarantine's measured resource-wait type.
    #[cfg(test)]
    pub(in crate::marketplace) fn source_verification_waiting_for_playback(
        error: &anyhow::Error,
    ) -> bool {
        super::source_preflight::playback_startup_deferred(error)
    }
    /// Only the same actual protected owner can open this source lane. The plan
    /// is derived from a sealed observation, and host resource admission runs
    /// before any volume/writer creation. This does not grant peer transfer,
    /// package/source equality, installation, execution or sharing.
    pub(in crate::marketplace) async fn open_source_quarantine_retained(
        &self,
        slot: &mut Option<crate::marketplace::quarantine::SourceQuarantineJob>,
        metadata: &dyn crate::marketplace::selected_source::SelectedSource,
        artifact: Option<usize>,
        budget: &crate::marketplace::quarantine::SourceJobBudget,
        runtime: Option<Arc<dyn crate::runtime::RuntimeManager>>,
    ) -> Result<()> {
        self.require_blob_owner()?;
        let source = self
            .source_jobs
            .as_ref()
            .context("foreground source jobs are unavailable")?;
        self.check_selected_source(metadata).await?;
        let plan = metadata.plan(artifact, budget)?;
        source
            .open_source_job_retained(slot, &plan, runtime)
            .await?;
        // The caller retains the original slot even if this final check fails.
        // It, not this borrowed await, must join/dispose any actual native work.
        self.require_blob_owner()?;
        self.check_selected_source(metadata).await
    }
    #[cfg(unix)]
    pub(in crate::marketplace) async fn open_activation_source(
        &self,
        slot: &mut Option<crate::marketplace::quarantine::SourceQuarantineJob>,
        start: crate::marketplace::grant_store::installation::activation::ActivationStart,
        request: &crate::marketplace::grant_store::installation::activation::SourceOpen<'_>,
    ) -> Result<crate::marketplace::grant_store::installation::activation::SourceOpening> {
        use crate::marketplace::grant_store::installation::activation::{
            self, SourceOpen, SourceOpening,
        };
        let SourceOpen {
            metadata,
            public,
            archive,
            plan,
            budget,
        } = *request;
        self.require_blob_owner()?;
        let source = self
            .source_jobs
            .as_ref()
            .context("foreground source jobs are unavailable")?;
        self.check_selected_source(metadata).await?;
        match source.open_activation_job(slot, &start, request).await {
            Ok(()) => {}
            Err(error)
                if Self::source_resources_deferred(&error)
                    && slot.as_ref().is_some_and(|job| job.admission_unstarted()) =>
            {
                // open_owned has only its first provisioning admission here;
                // this typed error proves no native operation was begun.
                let wait = Self::source_resource_wait(&error)
                    .context("typed source resource wait missing")?;
                *slot = None;
                return Ok(SourceOpening::Deferred(start, wait));
            }
            Err(error) => return Err(error),
        }
        self.require_blob_owner()?;
        self.check_selected_source(metadata).await?;
        activation::require_native_start(
            &self.database,
            metadata,
            public,
            archive,
            &start.native_check(),
            plan,
            budget,
        )
        .await?;
        // Success consumes the original claim. There is no returned token to
        // provision another workspace; status/recovery data cannot replace it.
        Ok(SourceOpening::Opened)
    }
    pub(in crate::marketplace) fn require_blob_owner(&self) -> Result<()> {
        let status = self.snapshot();
        if status.phase != ParticipationPhase::Running
            || !status.applied_roles.is_some_and(|roles| roles.consumer)
        {
            bail!("protected marketplace consumer is unavailable");
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn control_blob(
        &self,
        operation: WorkerControlOperation,
    ) -> Result<BlobReadState> {
        self.reserve_blob(operation).await?.send()?.wait().await
    }
    pub(in crate::marketplace) fn fail_blob_cleanup(&self) {
        // A failed/lost cancellation acknowledgement cannot be reported as
        // joined cleanup. Force the actual protected lifecycle toward shutdown.
        let _ = self.faults.try_send(());
    }
    /// Server-owned connection configuration was withdrawn/changed. Close the
    /// actual protected lifecycle; a future retry still requires joined cleanup
    /// and fresh provider/identity/credit checks. Not a public RPC or role flag.
    pub(in crate::marketplace) fn invalidate_connection(&self) {
        let _ = self.faults.try_send(());
    }
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        self.requests.same_channel(&other.requests)
    }
    async fn security_result<T>(
        &self,
        work: impl std::future::Future<Output = Result<T>>,
        mutating: bool,
    ) -> Result<T> {
        // A cancelled/uncertain authority commit cannot leave old live receipts
        // usable. Read cancellation does not mutate authority and needs no latch.
        struct Commit<'a> {
            client: Option<&'a super::source_preflight::Client>,
            armed: bool,
        }
        impl Drop for Commit<'_> {
            fn drop(&mut self) {
                if self.armed {
                    tracing::warn!(
                        mutating = true,
                        "marketplace security commit waiter cancelled; retaining failure fence"
                    );
                    if let Some(client) = self.client {
                        client.fail_security_storage();
                    }
                }
            }
        }
        let mut commit = Commit {
            client: self.source_preflight.as_ref(),
            armed: mutating,
        };
        let result = timeout(Duration::from_secs(10), work)
            .await
            .context("marketplace security persistence/readback deadline")
            .and_then(|result| result);
        if let Err(error) = &result
            && !crate::marketplace::security_state::is_expected_rejection(error)
        {
            crate::marketplace::lifecycle::trace_failure("security_storage", error);
            if let Some(client) = &self.source_preflight {
                client.fail_security_storage();
            }
        }
        commit.armed = false;
        result
    }
    pub fn snapshot(&self) -> ParticipationSnapshot {
        let mut snapshot = self.status.borrow().clone();
        if let Some(catalog) = &mut snapshot.catalog {
            catalog.age();
        }
        if (self.status.has_changed().is_err()
            || self
                .source_preflight
                .as_ref()
                .is_some_and(|client| client.failed())
            || !matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            ))
            && !matches!(
                snapshot.phase,
                ParticipationPhase::Stopped | ParticipationPhase::Failed
            )
        {
            snapshot.phase = ParticipationPhase::Failed;
            snapshot.applied_roles = None;
        }
        if snapshot.phase != ParticipationPhase::Running {
            snapshot.catalog = None;
        }
        snapshot
    }
    async fn enqueue<T>(&self, request: Request, reply: oneshot::Receiver<Result<T>>) -> Result<T> {
        let acknowledgement = match &request {
            // These requests may retain their ORIGINAL receiver while a known
            // pre-dispatch health check completes. No packet/IPC retry, extra
            // queue slot on retry or renewal of this fixed acknowledgement deadline.
            Request::Refresh(_) | Request::Peer(..) => {
                super::CHECKING_DEADLINE + Duration::from_secs(30)
            }
            Request::CatalogSetup(_) => Self::DISCOVERY_WAIT + Duration::from_secs(30),
            _ => Duration::from_secs(30),
        };
        if matches!(
            self.snapshot().phase,
            ParticipationPhase::Stopped | ParticipationPhase::Stopping | ParticipationPhase::Failed
        ) {
            bail!("marketplace participation is unavailable");
        }
        let queue = if request.transfer_io() {
            &self.transfer_requests
        } else {
            &self.requests
        };
        queue.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => anyhow::Error::from(RequestQueueBusy),
            mpsc::error::TrySendError::Closed(_) => {
                anyhow::anyhow!("marketplace request owner stopped")
            }
        })?;
        timeout(acknowledgement, reply)
            .await
            .context("marketplace request acknowledgement deadline")?
            .context("marketplace request owner stopped")?
    }
    /// Current-state reads, source-admitted cache offers and an already-committed
    /// security mirror delta must not fail merely because the SAME local pipe is full.
    /// Reserve once before dispatch; capacity and reply share one deadline.
    /// Cancellation before send drops only unsent capacity, never a native RPC.
    async fn enqueue_after_capacity<T>(
        &self,
        request: Request,
        reply: oneshot::Receiver<Result<T>>,
    ) -> Result<T> {
        self.enqueue_after_capacity_checked(request, reply, || Ok(()))
            .await
    }
    async fn enqueue_after_capacity_checked<T>(
        &self,
        request: Request,
        reply: oneshot::Receiver<Result<T>>,
        facts_current: impl Fn() -> Result<()>,
    ) -> Result<T> {
        if !matches!(
            &request,
            Request::Cached(
                PeerOperation::GetHead { .. } | PeerOperation::Offer { .. },
                _
            ) | Request::Heads(..)
                | Request::Security(..)
        ) {
            bail!("invalid capacity-waiting marketplace operation");
        }
        let current = || {
            if self.snapshot().phase != ParticipationPhase::Running {
                bail!("original marketplace request owner is not running");
            }
            facts_current()
        };
        current()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let permit = tokio::time::timeout_at(deadline, self.requests.reserve())
            .await
            .context("marketplace local capacity deadline before dispatch")?
            .context("marketplace request owner stopped before dispatch")?;
        current()?;
        if Instant::now() >= deadline {
            bail!("marketplace original deadline elapsed before dispatch");
        }
        permit.send(request);
        let result = tokio::time::timeout_at(deadline, reply)
            .await
            .context("marketplace original request acknowledgement deadline")?
            .context("marketplace original request owner stopped")??;
        current()?;
        Ok(result)
    }
    pub async fn request(&self, operation: PeerOperation) -> Result<PeerResponse> {
        operation.validate()?;
        let (send, reply) = oneshot::channel();
        self.enqueue(Request::Cached(operation, send), reply).await
    }
    pub async fn request_peer(
        &self,
        peer: PeerKey,
        operation: PeerOperation,
    ) -> Result<WorkerControlResponse> {
        operation.validate()?;
        let (send, reply) = oneshot::channel();
        self.enqueue(Request::Peer(peer, operation, send), reply)
            .await
    }
    pub async fn merge_evidence(
        &self,
        id: ExtensionId,
        batch: EvidenceBatch,
    ) -> Result<WorkerControlResponse> {
        EvidenceBatch::parse(&canonical::to_vec(&batch)?)?;
        let (send, reply) = oneshot::channel();
        self.enqueue(Request::Merge(id, batch, send), reply).await
    }
    pub async fn network_status(&self) -> Result<NetworkStatus> {
        let (send, reply) = oneshot::channel();
        self.enqueue(Request::Peers(send), reply).await
    }
    pub(crate) async fn refresh_network(&self) -> Result<NetworkStatus> {
        let (send, reply) = oneshot::channel();
        self.enqueue(Request::Refresh(send), reply).await
    }
    pub async fn add_credit(&self, credit: CommittedTransferReservation) -> Result<()> {
        // Independent bounded protection channel, not behind catalog requests.
        send_credit(&self.credits, &self.protection, credit).await
    }
    /// Controller-only background preflight. Not a peer operation, foreground
    /// budget, source-safety verdict or permission to display/install a release.
    pub async fn preflight_source(
        &self,
        release: &elixir_marketplace::security::AuthorizedRelease,
        location: usize,
    ) -> Result<super::PublicSourceObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        self.source_preflight
            .as_ref()
            .context("source preflight is unavailable")?
            .request(release, location)
            .await
    }
    /// Cache independently authenticated metadata/proof bytes. This creates no
    /// listener, serving role, packet credit, install grant or score authority.
    pub(crate) async fn retain_evidence(
        &self,
        metadata: &super::MetadataObservation,
        source: &impl super::DiscoverySourceFact,
        batch: &EvidenceBatch,
        observation: &super::EvidenceObservation,
    ) -> Result<bool> {
        self.check_metadata_current(metadata).await?;
        let retained = observation.retained_batch(metadata, batch)?;
        if retained.pairs.is_empty() {
            return Ok(false);
        }
        if source.release_id() != retained.release_id
            || source.valid_for() < Duration::from_secs(2)
            || metadata.valid_for() < Duration::from_secs(2)
        {
            bail!("retained metadata/source checks expired");
        }
        let offered = self
            .request(PeerOperation::Offer {
                descriptor: Box::new(metadata.descriptor().clone()),
            })
            .await?;
        if !matches!(offered.response,elixir_marketplace::peer::PeerOutcome::Offered {release_id} if release_id==retained.release_id)
        {
            return Ok(false);
        }
        self.check_metadata_current(metadata).await?;
        if !observation.is_current() || source.valid_for() < Duration::from_secs(2) {
            return Ok(false);
        }
        let result = self
            .merge_evidence(
                metadata.descriptor().release.payload.extension_id.clone(),
                retained,
            )
            .await?;
        self.check_metadata_current(metadata).await?;
        // The bounded worker re-verifies signatures/scope before retention.
        // Receipt counts are never added to the parent's independently scored facts.
        Ok(matches!(
            result.control_response,
            elixir_marketplace::worker_control::WorkerControlOutcome::EvidenceMerged { .. }
        ))
    }
    /// Retain a discovery-qualified descriptor in the EXISTING bounded native
    /// index, independently of optional ranking enrichment. Search-only source
    /// facts can authorize metadata retention, never installation or serving.
    pub(in crate::marketplace) async fn retain_admitted_metadata(
        &self,
        metadata: &super::MetadataObservation,
        source: &impl super::DiscoverySourceFact,
    ) -> Result<bool> {
        self.retain_metadata(metadata, source, false).await
    }
    async fn retain_metadata(
        &self,
        metadata: &super::MetadataObservation,
        source: &impl super::DiscoverySourceFact,
        wait_for_capacity: bool,
    ) -> Result<bool> {
        self.check_metadata_current(metadata).await?;
        let release = elixir_marketplace::identity::Digest::of(canonical::to_vec(
            &metadata.descriptor().release.payload,
        )?);
        let facts_current = || {
            if source.release_id() != release
                || source.valid_for() < Duration::from_secs(2)
                || metadata.valid_for() < Duration::from_secs(2)
            {
                bail!("retained metadata/source checks expired");
            }
            Ok(())
        };
        facts_current()?;
        let operation = PeerOperation::Offer {
            descriptor: Box::new(metadata.descriptor().clone()),
        };
        operation.validate()?;
        let response = if wait_for_capacity {
            // New search heads may need this write to replace a superseded
            // cache entry. Wait for ONE local slot, never replay an offer.
            // Source/native facts are rechecked immediately BEFORE dispatch.
            let (send, reply) = oneshot::channel();
            self.enqueue_after_capacity_checked(
                Request::Cached(operation, send),
                reply,
                facts_current,
            )
            .await
        } else {
            self.request(operation).await
        };
        let response = match response {
            Err(error) if Self::queue_busy(&error) => None,
            other => Some(other?),
        };
        self.check_metadata_current(metadata).await?;
        Ok(source.valid_for() >= Duration::from_secs(2)
            && response.is_some_and(|response| matches!(response.response,elixir_marketplace::peer::PeerOutcome::Offered{release_id} if release_id==release)))
    }
    /// A newly verified head can already fence out its cached predecessor.
    /// Retain only source-admitted metadata BEFORE the unsigned cache veto, so
    /// that predecessor cannot permanently prevent its own replacement. Every
    /// native/durable/source check remains mandatory; retention is not trust.
    pub(in crate::marketplace) async fn retain_current_search_metadata(
        &self,
        metadata: &super::MetadataObservation,
        source: &impl super::DiscoverySourceFact,
    ) -> Result<()> {
        let _ = self.retain_metadata(metadata, source, true).await?;
        if source.valid_for() < Duration::from_secs(2) {
            bail!("retained search source expired before current-head check");
        }
        self.check_catalog_current(metadata).await?;
        if source.valid_for() < Duration::from_secs(2) {
            bail!("retained search source expired during current-head check");
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn warm_metadata(
        &self,
        descriptors: &[elixir_marketplace::admission::CandidateDescriptor],
    ) -> Result<()> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        self.source_preflight
            .as_ref()
            .context("metadata verification is unavailable")?
            .warm_metadata(descriptors)
            .await
    }
    pub(in crate::marketplace) fn search_batch_needed(
        &self,
        release: &elixir_marketplace::records::Release,
    ) -> Result<bool> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        self.source_preflight
            .as_ref()
            .context("source preflight unavailable")?
            .search_batch_needed(release)
    }
    pub(in crate::marketplace) async fn cached_search_metadata(
        &self,
        descriptor: &elixir_marketplace::admission::CandidateDescriptor,
    ) -> Result<Option<super::MetadataObservation>> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        let value = self
            .source_preflight
            .as_ref()
            .context("source preflight unavailable")?
            .cached_metadata(descriptor)?;
        if let Some(metadata) = &value {
            self.persist_metadata_security(metadata).await?;
        }
        Ok(value)
    }
    /// Only ephemeral search receipts. Callers first await normal durable
    /// metadata/security checks; cancellation here cannot abandon those writes.
    pub(in crate::marketplace) async fn warm_search_sources(
        &self,
        metadata: Vec<super::MetadataObservation>,
    ) -> Result<()> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        self.source_preflight
            .as_ref()
            .context("source preflight unavailable")?
            .warm_search_sources(metadata)
            .await
    }
    /// Independent, bounded signature/adoption check of one candidate. This
    /// Durably compares publisher/head/revocation fences. This still does not
    /// establish public source, global latest state, visibility or install rights.
    pub async fn verify_metadata(
        &self,
        descriptor: &elixir_marketplace::admission::CandidateDescriptor,
    ) -> Result<super::MetadataObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        let observation = self
            .source_preflight
            .as_ref()
            .context("metadata verification is unavailable")?
            .verify_metadata(descriptor)
            .await?;
        self.persist_metadata_security(&observation).await?;
        Ok(observation)
    }
    pub(in crate::marketplace) fn catalog_preparation_ready(&self) -> Result<bool> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        self.source_preflight
            .as_ref()
            .context("catalog verification is unavailable")?
            .catalog_preparation_ready()
    }
    pub(in crate::marketplace) async fn verify_selection_metadata(
        &self,
        descriptor: &elixir_marketplace::admission::CandidateDescriptor,
    ) -> Result<super::MetadataObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        let observation = self
            .source_preflight
            .as_ref()
            .context("metadata verification unavailable")?
            .verify_selection_metadata(descriptor)
            .await?;
        self.persist_metadata_security(&observation).await?;
        Ok(observation)
    }
    /// Private long-upload renewal only. Original timestamps are never
    /// extended: a nearly expired cache hit requires a new bounded verifier.
    pub(in crate::marketplace) async fn verify_sharing_metadata(
        &self,
        descriptor: &elixir_marketplace::admission::CandidateDescriptor,
    ) -> Result<super::MetadataObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        let observation = self
            .source_preflight
            .as_ref()
            .context("metadata verification unavailable")?
            .verify_background_metadata(descriptor)
            .await?;
        self.persist_metadata_security(&observation).await?;
        Ok(observation)
    }
    /// Explicit direct-reference inspection, never searchable admission. Signed
    /// known authority changes are reconciled before returning the receipt;
    /// unknown references allocate no durable catalog/security rows.
    pub async fn verify_publication(
        &self,
        publication: &elixir_marketplace::publication::PreparedPublication,
    ) -> Result<super::PublicationObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        let value = self
            .source_preflight
            .as_ref()
            .context("direct-reference verification unavailable")?
            .verify_publication(publication)
            .await?;
        self.persist_publication_security(&value).await?;
        Ok(value)
    }
    /// Explicit installed publisher consent only. Uses the SAME bounded native
    /// verifier, but subordinate background scheduling for upload renewal.
    pub(in crate::marketplace) async fn verify_sharing_publication(
        &self,
        publication: &elixir_marketplace::publication::PreparedPublication,
    ) -> Result<super::PublicationObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        let value = self
            .source_preflight
            .as_ref()
            .context("publisher verification unavailable")?
            .verify_background_publication(publication)
            .await?;
        self.persist_publication_security(&value).await?;
        Ok(value)
    }
    async fn persist_publication_security(
        &self,
        value: &super::PublicationObservation,
    ) -> Result<bool> {
        self.security_result(
            async {
                let mirror = self
                    .security_mirror
                    .as_ref()
                    .context("catalog security reconciliation unavailable")?;
                let mut revision = mirror.revision.lock().await;
                let mutation = crate::marketplace::security_state::catalog_observe_publication(
                    &self.database,
                    value,
                    *revision,
                )
                .await?;
                mirror.apply(self, mutation, &mut revision).await?;
                crate::marketplace::security_state::require_publication_current(
                    &self.database,
                    value,
                )
                .await
            },
            true,
        )
        .await
    }
    pub async fn check_publication_current(
        &self,
        value: &super::PublicationObservation,
    ) -> Result<bool> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        self.security_result(
            crate::marketplace::security_state::require_publication_current(&self.database, value),
            false,
        )
        .await
    }
    pub(in crate::marketplace) async fn accept_direct_source(
        &self,
        value: &crate::marketplace::selected_source::AcceptedPublication,
    ) -> Result<()> {
        self.require_blob_owner()?;
        self.security_result(
            async {
                let mirror = self
                    .security_mirror
                    .as_ref()
                    .context("catalog security reconciliation unavailable")?;
                let mut revision = mirror.revision.lock().await;
                let mutation = crate::marketplace::security_state::catalog_accept_publication(
                    &self.database,
                    value,
                    *revision,
                )
                .await?;
                mirror.apply(self, mutation, &mut revision).await?;
                crate::marketplace::selected_source::SelectedSource::require_current(
                    value,
                    &self.database,
                )
                .await
            },
            true,
        )
        .await
    }
    pub(in crate::marketplace) async fn check_selected_source(
        &self,
        value: &dyn crate::marketplace::selected_source::SelectedSource,
    ) -> Result<()> {
        self.require_blob_owner()?;
        if let Some(metadata) = value.discovery() {
            self.check_catalog_current(metadata).await
        } else {
            value.require_current(&self.database).await
        }
    }
    /// Protected public text for an explicit direct reference, still outside
    /// discovery. Known head/revocation fences apply before AND after fetch;
    /// untracked references remain untracked and allocate no catalog rows here.
    pub async fn preflight_publication(
        &self,
        publication: &super::PublicationObservation,
        location: usize,
    ) -> Result<super::PublicSourceObservation> {
        self.preflight_publication_mode(publication, location, false)
            .await
    }
    /// Informational preview only. May reuse exact, still-current native
    /// metadata; source selection/installation use verify_publication instead.
    pub async fn verify_publication_for_preview(
        &self,
        publication: &elixir_marketplace::publication::PreparedPublication,
    ) -> Result<super::PublicationObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        let value = self
            .source_preflight
            .as_ref()
            .context("source preflight unavailable")?
            .verify_publication_for_preview(publication)
            .await
            .context("direct preview native signature receipt")?;
        self.persist_publication_security(&value)
            .await
            .context("direct preview security reconciliation")?;
        Ok(value)
    }
    /// Installation/update selection must fetch again, even when the exact
    /// direct reference has a current five-minute availability observation.
    /// Neither this receipt nor public source alone approves installation.
    pub(in crate::marketplace) async fn preflight_fresh_publication(
        &self,
        publication: &super::PublicationObservation,
        location: usize,
    ) -> Result<super::PublicSourceObservation> {
        self.preflight_publication_mode(publication, location, true)
            .await
    }
    async fn preflight_publication_mode(
        &self,
        publication: &super::PublicationObservation,
        location: usize,
        fresh: bool,
    ) -> Result<super::PublicSourceObservation> {
        self.check_publication_current(publication).await?;
        let preflight = self
            .source_preflight
            .as_ref()
            .context("source preflight is unavailable")?;
        let source = if fresh {
            preflight
                .request_fresh_publication(publication, location)
                .await?
        } else {
            preflight.request_publication(publication, location).await?
        };
        self.check_publication_current(publication).await?;
        Ok(source)
    }
    async fn persist_metadata_security(
        &self,
        observation: &super::MetadataObservation,
    ) -> Result<()> {
        self.security_result(
            async {
                let mirror = self
                    .security_mirror
                    .as_ref()
                    .context("catalog security reconciliation unavailable")?;
                let mut revision = mirror.revision.lock().await;
                let mutation = crate::marketplace::security_state::catalog_admit(
                    &self.database,
                    observation,
                    *revision,
                )
                .await?;
                mirror.apply(self, mutation, &mut revision).await?;
                crate::marketplace::security_state::require_current(&self.database, observation)
                    .await
            },
            true,
        )
        .await
    }
    /// Protected public text for a natively verified metadata candidate. A
    /// positive cache hit preserves its original TTL/owner and cannot skip
    /// retained security state, full source equality or installation checks.
    pub async fn preflight_metadata(
        &self,
        metadata: &super::MetadataObservation,
        location: usize,
    ) -> Result<super::PublicSourceObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        self.security_result(
            crate::marketplace::security_state::require_current(&self.database, metadata),
            false,
        )
        .await?;
        let observation = self
            .source_preflight
            .as_ref()
            .context("source preflight is unavailable")?
            .request_metadata(metadata, location)
            .await?;
        self.security_result(
            crate::marketplace::security_state::require_current(&self.database, metadata),
            false,
        )
        .await?;
        Ok(observation)
    }
    pub(in crate::marketplace) async fn preflight_search_metadata(
        &self,
        metadata: &super::MetadataObservation,
        location: usize,
    ) -> Result<super::SearchSourceObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        self.security_result(
            crate::marketplace::security_state::require_current(&self.database, metadata),
            false,
        )
        .await?;
        let observation = self
            .source_preflight
            .as_ref()
            .context("source preflight unavailable")?
            .request_search_metadata(metadata, location)
            .await?;
        self.security_result(
            crate::marketplace::security_state::require_current(&self.database, metadata),
            false,
        )
        .await?;
        Ok(observation)
    }
    pub(in crate::marketplace) async fn preflight_fresh_metadata(
        &self,
        metadata: &super::MetadataObservation,
        location: usize,
    ) -> Result<super::PublicSourceObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation unavailable");
        }
        self.security_result(
            crate::marketplace::security_state::require_current(&self.database, metadata),
            false,
        )
        .await?;
        let observation = self
            .source_preflight
            .as_ref()
            .context("source preflight unavailable")?
            .request_fresh_metadata(metadata, location)
            .await?;
        self.security_result(
            crate::marketplace::security_state::require_current(&self.database, metadata),
            false,
        )
        .await?;
        Ok(observation)
    }
    pub async fn check_metadata_current(
        &self,
        metadata: &super::MetadataObservation,
    ) -> Result<()> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        self.security_result(
            crate::marketplace::security_state::require_current(&self.database, metadata),
            false,
        )
        .await
    }
    /// A bounded local availability veto, never a replacement for native
    /// signature/adoption checks or durable anti-rollback/revocation fences.
    pub(in crate::marketplace) async fn check_catalog_current(
        &self,
        metadata: &super::MetadataObservation,
    ) -> Result<()> {
        self.check_metadata_current(metadata).await?;
        if self
            .source_preflight
            .as_ref()
            .is_some_and(|client| client.catalog_head_snapshot_supported())
        {
            let operation = PeerOperation::GetHead {
                extension_id: metadata.descriptor().release.payload.extension_id.clone(),
            };
            operation.validate()?;
            let (send, reply) = oneshot::channel();
            let response = self
                .enqueue_after_capacity(Request::Cached(operation, send), reply)
                .await?;
            match response.response {
                elixir_marketplace::peer::PeerOutcome::Head { state }
                    if catalog_snapshot::allows(metadata.descriptor(), &state)? => {}
                // No local observation is not evidence against an independently
                // verified remote candidate. All other errors fail closed.
                elixir_marketplace::peer::PeerOutcome::Rejected {
                    code: elixir_marketplace::peer::RejectionCode::Invalid,
                } => {}
                _ => bail!("local marketplace observation changed or is unavailable"),
            }
        }
        self.check_metadata_current(metadata).await
    }
    /// Ranking facts from the fixed native process, not a worker-reported total.
    /// Metadata/source discovery and an exact-release gate remain mandatory.
    pub async fn verify_evidence(
        &self,
        metadata: &super::MetadataObservation,
        batch: &elixir_marketplace::evidence::EvidenceBatch,
    ) -> Result<super::EvidenceObservation> {
        self.check_metadata_current(metadata).await?;
        let observation = self
            .source_preflight
            .as_ref()
            .context("evidence verification is unavailable")?
            .verify_evidence(metadata, batch)
            .await?;
        self.check_metadata_current(metadata).await?;
        Ok(observation)
    }
    pub fn evidence_enabled(&self) -> bool {
        self.snapshot().phase == ParticipationPhase::Running
            && self
                .source_preflight
                .as_ref()
                .is_some_and(|client| client.evidence_enabled())
    }
    pub async fn ranking_summary(
        &self,
        metadata: &super::MetadataObservation,
    ) -> Result<elixir_marketplace::evidence::SupportSummary> {
        self.check_metadata_current(metadata).await?;
        let summary = self
            .source_preflight
            .as_ref()
            .context("ranking is unavailable")?
            .ranking_summary(metadata)?;
        self.check_metadata_current(metadata).await?;
        Ok(summary)
    }
    /// No discovery eligibility or source fetch is implied. The durable
    /// security store may retain these observations for already-tracked IDs.
    pub async fn verify_security(
        &self,
        records: &elixir_marketplace::security_job::SecurityRecords,
    ) -> Result<super::SecurityObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        let observation = self
            .source_preflight
            .as_ref()
            .context("security verification is unavailable")?
            .verify_security(records)
            .await?;
        self.retain_security_observation(&observation).await?;
        Ok(observation)
    }
    pub(in crate::marketplace) async fn authenticate_owner_security(
        &self,
        records: &elixir_marketplace::security_job::SecurityRecords,
    ) -> Result<super::SecurityObservation> {
        if self.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace participation is unavailable");
        }
        self.source_preflight
            .as_ref()
            .context("security verification is unavailable")?
            .verify_owner_security(records)
            .await
    }
    pub(in crate::marketplace) async fn retain_security_observation(
        &self,
        observation: &super::SecurityObservation,
    ) -> Result<bool> {
        self.security_result(
            async {
                let mirror = self
                    .security_mirror
                    .as_ref()
                    .context("catalog security reconciliation unavailable")?;
                let mut revision = mirror.revision.lock().await;
                let mutation = crate::marketplace::security_state::catalog_observe(
                    &self.database,
                    observation,
                    *revision,
                )
                .await?;
                let tracked = mutation.tracked();
                mirror.apply(self, mutation, &mut revision).await?;
                Ok(tracked)
            },
            true,
        )
        .await
    }
}

pub struct ParticipatingWorker {
    client: ParticipationClient,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<crate::marketplace::lifecycle::Exit>>,
}
impl ParticipatingWorker {
    pub async fn start(
        mut worker: ProtectedWorker,
        mut config: NetworkConfig,
        database: Database,
        playback: Arc<PlaybackJobManager>,
    ) -> Result<Self> {
        // Consume an actual successful protected lifecycle. No bool, endpoint,
        // worker assertion or owner preference can manufacture that capability.
        worker.supervisor.require_active()?;
        config.roles = NetworkRoles {
            consumer: true,
            catalog: false,
            routing: false,
        };
        config.validate()?;
        config
            .limits
            .validate_against(&worker.worker.profile.limits)?;
        let preferences =
            crate::marketplace::preferences::Observer::start(database.clone()).await?;
        let settings = preferences.subscribe();
        let source = Arc::new(metrics::Source::from_worker(
            &worker,
            database.clone(),
            playback.clone(),
        )?);
        worker.stop_network().await?;
        let reconciled = timeout(
            Duration::from_secs(120),
            security_mirror::Mirror::initialize(&mut worker, &database),
        )
        .await
        .context("catalog security baseline replay deadline")
        .and_then(|result| result);
        let security_mirror = match reconciled {
            Ok(mirror) => Some(Arc::new(mirror)),
            Err(error) => {
                let cleanup = worker.stop().await;
                return Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup) => error.context(format!(
                        "catalog baseline failed; cleanup unconfirmed: {cleanup}"
                    )),
                });
            }
        };
        let protection = worker.supervisor.status.clone();
        let credits = worker.supervisor.credits.clone();
        let faults = worker.supervisor.faults.clone();
        let (requests, receiver) = mpsc::channel(1);
        let (transfer_requests, transfer_receiver) = mpsc::channel(1);
        let (status, observed) = watch::channel(ParticipationSnapshot::default());
        let (stop, stopped) = oneshot::channel();
        let (retirement_owner, retirement) = retirement::channel();
        let (samples, sample_task) = source.clone().spawn();
        let source_preflight = super::source_preflight::Client::from_worker(
            &worker,
            settings.clone(),
            samples.clone(),
            playback,
        );
        let client = ParticipationClient {
            blob_discovery: worker.blob_discovery,
            blob_sharing: worker.blob_sharing,
            database,
            source_preflight,
            source_jobs: Some(source),
            requests,
            transfer_requests,
            status: observed,
            protection: protection.clone(),
            credits,
            security_mirror,
            faults,
            retirement,
        };
        let limits = sample_task.limits.clone();
        let task = spawn_owned(
            Live {
                worker,
                security_mirror: client.security_mirror.as_ref().unwrap().clone(),
                database: client.database.clone(),
                config,
                applied: Some(NetworkRoles::default()),
                population: PopulationCursor::default(),
                catalog_progress: catalog_progress::Tracker::default(),
                next_maintenance: Instant::now() + Duration::from_secs(60),
                sharing_until: None,
                sharing_blobs: 0,
            },
            limits,
            settings,
            samples,
            protection,
            RequestInbox {
                regular: receiver,
                transfer: transfer_receiver,
            },
            status,
            retirement_owner,
            stopped,
            Observers {
                _host: sample_task,
                _preferences: Some(preferences),
            },
        );
        Ok(Self {
            client,
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub fn client(&self) -> ParticipationClient {
        self.client.clone()
    }
    pub(crate) fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
    pub async fn stop(self) -> Result<()> {
        self.finish().await?.result()
    }
    pub(crate) async fn retire(self) -> Result<()> {
        self.finish().await?.cleanup()
    }
    async fn finish(mut self) -> Result<crate::marketplace::lifecycle::Exit> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let mut task = self.task.take().context("missing participation task")?;
        match timeout(OWNER_STOP_DEADLINE, &mut task).await {
            Ok(result) => result.context("participation task failed"),
            Err(_) => {
                task.abort();
                bail!("participation cleanup deadline");
            }
        }
    }
}
impl Drop for ParticipatingWorker {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[async_trait]
trait Network: Send + 'static {
    fn catalog_progress(&self, _allowed: bool) -> Option<CatalogSnapshot> {
        None
    }
    fn package_sharing_active(&self) -> bool {
        false
    }
    fn protection(&self) -> ProtectionState;
    fn applied(&self) -> Option<NetworkRoles>;
    async fn apply(&mut self, roles: NetworkRoles) -> Result<bool>;
    async fn request(&mut self, request: Request) -> Option<Request>;
    async fn background(&mut self, population: bool, maintenance: bool) -> Result<()>;
    async fn shutdown(self) -> Result<crate::marketplace::lifecycle::Exit>;
}
struct Live {
    worker: ProtectedWorker,
    security_mirror: Arc<security_mirror::Mirror>,
    database: Database,
    config: NetworkConfig,
    applied: Option<NetworkRoles>,
    population: PopulationCursor,
    catalog_progress: catalog_progress::Tracker,
    next_maintenance: Instant,
    sharing_until: Option<Instant>,
    sharing_blobs: usize,
}
#[async_trait]
impl Network for Live {
    fn catalog_progress(&self, allowed: bool) -> Option<CatalogSnapshot> {
        Some(
            self.catalog_progress
                .view(self.worker.catalog_jobs_supported(), allowed),
        )
    }
    fn package_sharing_active(&self) -> bool {
        self.applied.is_some_and(|roles| roles.catalog)
            && self.sharing_blobs > 0
            && self
                .sharing_until
                .is_some_and(|until| Instant::now() < until)
    }
    fn protection(&self) -> ProtectionState {
        self.worker.protection_state()
    }
    fn applied(&self) -> Option<NetworkRoles> {
        self.applied
    }
    async fn apply(&mut self, roles: NetworkRoles) -> Result<bool> {
        if self.applied == Some(roles) {
            return Ok(true);
        }
        if roles.catalog && !self.security_mirror.current(&self.database).await? {
            return Ok(false);
        }
        self.applied = None;
        self.sharing_until = None;
        self.sharing_blobs = 0;
        let stopped = self.worker.stop_network().await?;
        if stopped.active {
            bail!("network stop did not close contribution");
        }
        self.catalog_progress.paused();
        self.applied = Some(NetworkRoles::default());
        if !roles.consumer && !roles.catalog && !roles.routing {
            return Ok(true);
        }
        if self.protection() == ProtectionState::Checking {
            return Ok(false);
        }
        self.worker.supervisor.require_ready()?;
        let mut config = self.config.clone();
        config.roles = roles;
        let started = self.worker.start_network(config).await?;
        self.applied = Some(started.roles());
        Ok(true)
    }
    async fn request(&mut self, request: Request) -> Option<Request> {
        match request {
            Request::CatalogSetup(page) => return catalog_setup::step(self, page).await,
            Request::Sharing(operation, reply) => {
                if !reply.is_closed() {
                    let result = async {
                        if !self.worker.blob_sharing
                            || !self.applied.is_some_and(|roles| roles.catalog)
                        {
                            bail!("installed sharing is not active");
                        }
                        let requested = elixir_marketplace::worker_control::WorkerControlRequest {
                            schema_version: 1,
                            request_id: 1,
                            control: operation,
                        };
                        requested.validate()?;
                        self.worker.supervisor.require_active()?;
                        let lease = match &requested.control {
                            WorkerControlOperation::SharingSet {
                                inventory,
                                lease_ms,
                                resolve_installed,
                            } => Some((
                                inventory.blobs.len().max(usize::from(*resolve_installed)),
                                *lease_ms,
                            )),
                            WorkerControlOperation::SharingPoll
                            | WorkerControlOperation::SharingChunk { .. }
                            | WorkerControlOperation::SharingAuthorize { .. } => None,
                            _ => bail!("invalid installed sharing command"),
                        };
                        let started = Instant::now();
                        let response = self.worker.network_control(requested.control).await?;
                        self.worker.supervisor.require_active()?;
                        let WorkerControlOutcome::Sharing { state } = response.control_response
                        else {
                            bail!("installed sharing command rejected");
                        };
                        state.validate()?;
                        if let Some((count, millis)) = lease {
                            self.sharing_blobs = count;
                            self.sharing_until =
                                Some(started + Duration::from_millis(u64::from(millis)));
                        }
                        Ok(state)
                    }
                    .await;
                    let _ = reply.send(result);
                }
            }
            Request::Providers(job, sha256, reply) => {
                if !reply.is_closed() {
                    if !self.applied.is_some_and(|roles| roles.consumer)
                        || !self.worker.blob_discovery
                    {
                        let _ = reply.send(LookupCompletion::not_dispatched(anyhow::anyhow!(
                            "provider discovery is unavailable"
                        )));
                        return None;
                    }
                    if let Err(error) = self.worker.supervisor.require_ready() {
                        if error.is::<super::ProtectionCheckInProgress>() {
                            return Some(Request::Providers(job, sha256, reply));
                        }
                        let _ = reply.send(LookupCompletion::not_dispatched(error));
                        return None;
                    }
                    let completion = match self
                        .worker
                        .network_control(WorkerControlOperation::BlobProviders { job, sha256 })
                        .await
                    {
                        Ok(response) => {
                            LookupCompletion::replied(match response.control_response {
                                WorkerControlOutcome::BlobProviders {
                                    job: actual,
                                    sha256: observed,
                                    peers,
                                } if actual == job && observed == sha256 => Ok(peers),
                                _ => Err(anyhow::anyhow!(
                                    "provider routing hints do not match the selected blob"
                                )),
                            })
                            .revalidate(self.worker.supervisor.require_active())
                        }
                        Err(error) => LookupCompletion::unconfirmed(error),
                    };
                    let _ = reply.send(completion);
                }
            }
            Request::Blob(operation, reply) => {
                let (job, cancel) = match &operation {
                    WorkerControlOperation::BlobStart { job, .. }
                    | WorkerControlOperation::BlobRead { job, .. } => (*job, false),
                    WorkerControlOperation::BlobCancel { job } => (*job, true),
                    _ => {
                        let _ = reply.send(Err(anyhow::anyhow!("invalid blob request")));
                        return None;
                    }
                };
                if matches!(operation, WorkerControlOperation::BlobStart { .. })
                    && !self.applied.is_some_and(|roles| roles.consumer)
                {
                    let _ = reply.send(Err(anyhow::anyhow!("marketplace consumer is unavailable")));
                    return None;
                }
                if !reply.is_closed() || cancel {
                    let result = self.worker.blob_control_retained(&operation).await;
                    if matches!(&operation, WorkerControlOperation::BlobStart { .. })
                        && result
                            .as_ref()
                            .err()
                            .is_some_and(|error| error.is::<super::ProtectionCheckInProgress>())
                    {
                        return Some(Request::Blob(operation, reply));
                    }
                    if reply.send(result).is_err()
                        && !cancel
                        && self
                            .worker
                            .blob_control(WorkerControlOperation::BlobCancel { job })
                            .await
                            .is_err()
                        && let Some(stop) = self.worker.supervisor.stop.take()
                    {
                        let _ = stop.send(());
                    }
                }
            }
            Request::Cached(operation, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.worker.request(operation).await);
                }
            }
            Request::Heads(ids, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.worker.catalog_heads(ids).await);
                }
            }
            Request::Peer(peer, operation, reply) => {
                if !reply.is_closed() {
                    // Local typed provenance only: no native IPC was sent.
                    // Do not confuse an unknown transport outcome with this
                    // safe wait for the SAME request/receiver.
                    if let Err(error) = self.worker.supervisor.require_ready() {
                        return reply_or_wait(reply, Err(error))
                            .map(|reply| Request::Peer(peer, operation, reply));
                    }
                    if self.worker.catalog_jobs_supported() {
                        match self
                            .worker
                            .catalog_job(WorkerControlOperation::CatalogJobStatus)
                            .await
                        {
                            Ok(Some(state)) if state.phase != CatalogJobPhase::Running => {}
                            Ok(_) => return Some(Request::Peer(peer, operation, reply)),
                            Err(error) => {
                                let _ = reply.send(Err(error));
                                return None;
                            }
                        }
                    }
                    let result = self
                        .worker
                        .request_peer(peer.clone(), operation.clone())
                        .await;
                    // Checking may start while reading catalog-job state;
                    // request_peer's typed error is still before its IPC.
                    return reply_or_wait(reply, result)
                        .map(|reply| Request::Peer(peer, operation, reply));
                }
            }
            Request::Merge(id, batch, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.worker.merge_evidence(id, batch).await);
                }
            }
            Request::Security(operation, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.worker.catalog_security(operation).await);
                }
            }
            Request::Peers(reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.worker.network_status().await);
                }
            }
            Request::Refresh(reply) => {
                if !reply.is_closed() {
                    let result = self.worker.refresh_network().await;
                    return reply_or_wait(reply, result).map(Request::Refresh);
                }
            }
        }
        None
    }
    async fn background(&mut self, population: bool, maintenance: bool) -> Result<()> {
        if !self.security_mirror.current(&self.database).await? {
            return Ok(());
        }
        if (!population && !maintenance) || !self.worker.catalog_jobs_supported() {
            return Ok(());
        }
        let Some(state) = self
            .worker
            .catalog_job(WorkerControlOperation::CatalogJobStatus)
            .await?
        else {
            return Ok(());
        };
        self.catalog_progress.observed(&state);
        if state.phase == CatalogJobPhase::Running {
            return Ok(());
        }
        if maintenance && Instant::now() >= self.next_maintenance {
            if self
                .worker
                .catalog_job(WorkerControlOperation::CatalogMaintain)
                .await?
                .is_some()
            {
                self.next_maintenance = Instant::now() + Duration::from_secs(60);
            }
        } else if population {
            let status = self.worker.network_status().await?;
            if status.peers.is_empty() {
                self.worker.refresh_background_network().await?;
                return Ok(());
            }
            let peer = self
                .population
                .choose(&state, &status.peers)
                .context("catalog population peer unavailable")?;
            // An attempt ceiling inside the already durably prepaid kernel
            // allowance, never new credit or permission for direct egress.
            let operation = if self.worker.catalog_sync_supported() {
                WorkerControlOperation::CatalogSync {
                    peer,
                    max_control_bytes: CATALOG_CONTROL_BUDGET,
                }
            } else if self.applied.is_some_and(|roles| roles.catalog) {
                WorkerControlOperation::CatalogPopulate {
                    peer,
                    max_control_bytes: CATALOG_CONTROL_BUDGET,
                }
            } else {
                return Ok(());
            };
            if let Some(state) = self.worker.catalog_job(operation).await? {
                self.catalog_progress.started(&state);
            }
        }
        Ok(())
    }
    async fn shutdown(self) -> Result<crate::marketplace::lifecycle::Exit> {
        self.worker.finish().await
    }
}

/// Finish one admitted peer's keyset walk before rotating. Round-robining
/// overlapping catalogs page-by-page can spend the entire examined-entry
/// budget on duplicates before a single catalog reaches its last page.
#[derive(Default)]
struct PopulationCursor {
    peer: Option<PeerKey>,
    next: usize,
}
impl PopulationCursor {
    fn choose(&mut self, state: &CatalogJobState, peers: &[PeerKey]) -> Option<PeerKey> {
        let finished = state.kind == Some(CatalogJobKind::Population)
            && state.peer.as_ref() == self.peer.as_ref()
            && !matches!(
                state.phase,
                CatalogJobPhase::Running | CatalogJobPhase::Progress
            );
        if finished || self.peer.as_ref().is_some_and(|p| !peers.contains(p)) {
            self.peer = None;
        }
        if self.peer.is_none() && !peers.is_empty() {
            self.peer = Some(peers[self.next % peers.len()].clone());
            self.next = self.next.wrapping_add(1);
        }
        self.peer.clone()
    }
}

struct Engine {
    policy: ParticipationController,
    origin: Instant,
    network_failed: bool,
}

/// Keep both local observation owners alive for exactly the participation
/// lifecycle. Dropping this bundle aborts their tasks and closes the watches.
struct Observers {
    _host: metrics::Sampler,
    _preferences: Option<crate::marketplace::preferences::Observer>,
}
impl Engine {
    fn new() -> Self {
        Self {
            policy: ParticipationController::recovering(30),
            origin: Instant::now(),
            network_failed: false,
        }
    }
    fn decide(
        &mut self,
        settings: ParticipationSettings,
        facts: Option<&metrics::Facts>,
        phase: ProtectionState,
        limits: &OrdinaryLimits,
    ) -> Result<ParticipationSnapshot> {
        let now = 30 + self.origin.elapsed().as_secs();
        let current = facts.filter(|facts| facts.observed.elapsed() <= SAMPLE_MAX_AGE);
        let healthy = matches!(phase, ProtectionState::Ready | ProtectionState::Checking);
        let evidence = HostEvidence {
            observed_at_monotonic: now,
            hard_memory_cpu_limits: healthy,
            persistent_quota_enforced: healthy,
            swap_and_logs_bounded: healthy,
            protected_packet_path_verified: healthy,
            protection_healthy: healthy,
            disclosure_seen: settings.accepted_disclosure_version
                == Some(PARTICIPATION_DISCLOSURE_VERSION),
            spare_memory_bytes: current.and_then(|x| x.available_memory).unwrap_or(0),
            reserved_gateway_memory_bytes: 0,
            disk_headroom: current.is_some_and(|x| x.disk_headroom),
            metering: settings.metering,
            playback_pressure: current.is_some_and(|x| x.playback_pressure),
            resource_pressure: current.is_none_or(|x| x.resource_pressure),
            network_healthy: healthy && !std::mem::take(&mut self.network_failed),
            transfer_allowance_remaining: current.is_some_and(|x| x.background_allowance),
            // Never infer externally reachable routing from an outgoing probe.
            protected_inbound_reachable: false,
        };
        let decision = self
            .policy
            .evaluate(settings.mode, limits, &evidence, now)?;
        Ok(ParticipationSnapshot {
            phase: ParticipationPhase::Running,
            preferences_revision: settings.revision,
            desired_roles: NetworkRoles {
                consumer: decision.roles.consumer,
                catalog: decision.roles.catalog,
                routing: decision.roles.routing,
            },
            applied_roles: None,
            blockers: decision.blockers,
            host_sample_current: current.is_some(),
            recovery_seconds: decision
                .resume_after_monotonic
                .map(|until| until.saturating_sub(now)),
            package_sharing_active: false,
            routing_qualified: false,
            catalog: None,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_owned<N: Network>(
    mut network: N,
    limits: OrdinaryLimits,
    mut settings: watch::Receiver<ParticipationSettings>,
    mut facts: watch::Receiver<Option<metrics::Facts>>,
    mut protection: watch::Receiver<Snapshot>,
    mut requests: RequestInbox,
    status: watch::Sender<ParticipationSnapshot>,
    retirement: retirement::Owner,
    mut stopped: oneshot::Receiver<()>,
    observers: Observers,
) -> JoinHandle<crate::marketplace::lifecycle::Exit> {
    tokio::spawn(async move {
        let result = tokio::select! {biased;
            _=&mut stopped=>Ok(()),
            result=drive(&mut network,&limits,&mut settings,&mut facts,&mut protection,&mut requests,&status)=>result,
        };
        // Stop is not yet an off acknowledgement. In-flight private work is
        // cancelled here, so its cancellation guard also stops kernel renewal.
        let mut snapshot = status.borrow().clone();
        snapshot.phase = ParticipationPhase::Stopping;
        snapshot.applied_roles = None;
        status.send_replace(snapshot.clone());
        drop(observers);
        let dependency = timeout(Duration::from_secs(70), network.shutdown())
            .await
            .context("participation shutdown deadline")
            .and_then(|result| result);
        let exit =
            crate::marketplace::lifecycle::Exit::new(result, Ok(())).with_dependency(dependency);
        exit.trace_failures("participation");
        retirement.complete(&exit);
        snapshot.phase = if exit.succeeded() {
            ParticipationPhase::Stopped
        } else {
            ParticipationPhase::Failed
        };
        snapshot.applied_roles = if exit.cleanup_confirmed() {
            // This is an acknowledgement of the joined native shutdown, not
            // the pre-stop lease snapshot. Unknown cleanup must retain doubt.
            snapshot.package_sharing_active = false;
            Some(NetworkRoles::default())
        } else {
            None
        };
        snapshot.desired_roles = NetworkRoles::default();
        snapshot.catalog = None;
        status.send_replace(snapshot);
        exit
    })
}

async fn drive(
    network: &mut impl Network,
    limits: &OrdinaryLimits,
    settings: &mut watch::Receiver<ParticipationSettings>,
    facts: &mut watch::Receiver<Option<metrics::Facts>>,
    protection: &mut watch::Receiver<Snapshot>,
    requests: &mut RequestInbox,
    status: &watch::Sender<ParticipationSnapshot>,
) -> Result<()> {
    let mut engine = Engine::new();
    let mut tick = tokio::time::interval(OBSERVATION_INTERVAL);
    // Job polling must not inherit the ten-second host-sampling cadence.
    let mut catalog_tick = tokio::time::interval(Duration::from_secs(1));
    catalog_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pending = None;
    let mut retry = tokio::time::interval(Duration::from_millis(250));
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if settings.has_changed().is_err()
            || facts.has_changed().is_err()
            || protection.has_changed().is_err()
        {
            bail!("participation observation owner stopped");
        }
        if !matches!(
            network.protection(),
            ProtectionState::Ready | ProtectionState::Checking
        ) {
            bail!("participation protection failed");
        }
        let settings_now = *settings.borrow_and_update();
        let facts_now = facts.borrow_and_update().clone();
        protection.borrow_and_update();
        let mut snapshot = engine.decide(
            settings_now,
            facts_now.as_ref(),
            network.protection(),
            limits,
        )?;
        snapshot.applied_roles = network.applied();
        if snapshot.applied_roles != Some(snapshot.desired_roles) {
            snapshot.phase = ParticipationPhase::Applying;
            status.send_replace(snapshot.clone());
            match network.apply(snapshot.desired_roles).await {
                Ok(true) => snapshot.phase = ParticipationPhase::Running,
                Ok(false) => snapshot.phase = ParticipationPhase::WaitingForProtection,
                Err(error) => {
                    if network.applied().is_none() || !snapshot.desired_roles.catalog {
                        return Err(error);
                    }
                    // A cleanly rejected catalog start may fall back to the
                    // consumer and a new healthy cooldown, not a restart storm.
                    engine.network_failed = true;
                    snapshot = engine.decide(
                        settings_now,
                        facts_now.as_ref(),
                        network.protection(),
                        limits,
                    )?;
                    if !network.apply(snapshot.desired_roles).await? {
                        snapshot.phase = ParticipationPhase::WaitingForProtection;
                    }
                }
            }
        }
        snapshot.applied_roles = network.applied();
        snapshot.package_sharing_active = network.package_sharing_active();
        let population = snapshot.phase == ParticipationPhase::Running
            && snapshot.host_sample_current
            && snapshot.applied_roles.is_some_and(|roles| roles.consumer)
            && network.protection() == ProtectionState::Ready
            && settings_now.metering == Metering::Unmetered
            && facts_now.as_ref().is_some_and(|f| {
                f.background_allowance
                    && !f.resource_pressure
                    && !f.playback_pressure
                    && f.disk_headroom
                    && f.available_memory
                        .is_some_and(|bytes| bytes >= limits.memory_bytes)
            });
        let maintenance = snapshot.phase == ParticipationPhase::Running
            && network.protection() == ProtectionState::Ready
            && facts_now.as_ref().is_some_and(|f| {
                f.observed.elapsed() <= SAMPLE_MAX_AGE
                    && !f.resource_pressure
                    && !f.playback_pressure
                    && f.disk_headroom
                    && f.available_memory
                        .is_some_and(|bytes| bytes >= limits.memory_bytes)
            });
        snapshot.catalog = network.catalog_progress(population);
        status.send_replace(snapshot);
        tokio::select! {biased;
            result=settings.changed()=>{result.context("participation settings owner stopped")?;},
            result=facts.changed()=>{result.context("host observation owner stopped")?;},
            result=protection.changed()=>{result.context("protection owner stopped")?;},
            _=tick.tick()=>{},
            _=catalog_tick.tick()=>{if pending.is_none() && requests.regular.is_empty() && requests.transfer.is_empty() {network.background(population,maintenance).await?;}},
            _=retry.tick(),if pending.is_some()=>{
                // Yield between polls so Off, host pressure and protection
                // observations stay responsive while the existing bounded
                // catalog job retires. Do not overlap that peer's packet RPC.
                pending=network.request(pending.take().expect("pending request")).await;
            },
            request=requests.transfer.recv()=>{
                let request=request.context("transfer clients stopped")?;
                if !request.transfer_io() {
                    bail!("invalid transfer-lane operation");
                }
                // These operations cannot be pre-dispatch deferred. Never
                // displace the original pending request or send parallel IPC.
                if network.request(request).await.is_some() {
                    bail!("transfer I/O was unexpectedly deferred");
                }
            },
            request=requests.regular.recv(),if pending.is_none()=>{
                let request=request.context("participation clients stopped")?;
                pending=network.request(request).await;
            },
        }
    }
}
