//! One application-owned source preparation and explicit owner permission
//! decision. Neither review nor a committed grant installs, runs or shares code.
use super::{
    grants::GrantReview,
    manifest::ManifestCheckedQuarantine,
    quarantine::{
        QuarantinedSource, SourceJobBudget, SourceMatchedQuarantine, SourceQuarantineJob,
    },
    search::Selection,
    worker::ParticipationClient,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use elixir_marketplace::{identity::Digest, quarantine::QuarantinePurpose};
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
    time::{Instant, sleep, timeout_at},
};
use uuid::Uuid;

mod approval;
mod authorization;
#[cfg(unix)]
mod compiled;
mod upload;
pub(crate) use upload::UploadClaim;
#[cfg(unix)]
mod handoff;
#[cfg(unix)]
pub(in crate::marketplace) use handoff::ArchivedPreparation;

const RETRY: Duration = Duration::from_secs(10);
const REVIEW_TICK: Duration = Duration::from_secs(5);
const MIB: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    WaitingForResources,
    Downloading,
    CheckingSource,
    CheckingManifest,
    AwaitingApproval,
    Approving,
    PermissionGranted,
    Archiving,
    SourceArchived,
    Cancelling,
    Cancelled,
    Failed,
    CleanupFailed,
}
#[derive(Clone, Serialize)]
pub struct View {
    pub schema_version: u32,
    pub preparation_id: Uuid,
    pub release_id: Digest,
    pub phase: Phase,
    pub review: Option<GrantReview>,
    // A review is NOT an approval, reproducible-build/runtime closure or install
    // capability. Never enable the old registry installer from this response.
    pub installation_ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload: Option<upload::UploadView>,
    /// Display only: neither a revocation proof nor permission to restart.
    #[serde(skip_serializing_if = "is_false")]
    pub authorization_lost: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}
struct Published {
    view: View,
    observed: Instant,
    // One private diagnostic, not serialized or cleanup/installation authority.
    operation_failure: Option<Arc<anyhow::Error>>,
}
enum Joined {
    Running(JoinHandle<Result<()>>),
    Done(std::result::Result<(), Arc<anyhow::Error>>),
}
struct Entry {
    id: Uuid,
    status: watch::Receiver<Published>,
    cancel: watch::Sender<bool>,
    approval: Mutex<approval::Slot>,
    upload: Option<upload::Slot>,
    task: AsyncMutex<Joined>,
    #[cfg(unix)]
    handoff: Option<handoff::Slot>,
}
/// Clones share ONE slot, no queue and no completed-job history. HTTP/peers
/// cannot supply a Work implementation, verification flags, runtime or budget.
#[derive(Clone, Default)]
pub(super) struct Service(Arc<Mutex<Option<Arc<Entry>>>>);
impl Service {
    /// The ordinary application install/update path. Only trusted native host
    /// configuration selects a compiled artifact/compiler/resource profile.
    #[cfg(unix)]
    pub(super) fn start_install(
        &self,
        selected: Selection,
        upload: bool,
        authorization: Option<super::grant_store::source_authorization::SourceAuthorization>,
        configuration: &super::grant_store::installation::runtime::InstalledRuntime,
        secrets: crate::secrets::SecretsManager,
    ) -> Result<Uuid> {
        let compiled = compiled::Configuration::select(
            selected.metadata()?.release_payload(),
            configuration,
            secrets,
        )?;
        let artifact = compiled
            .as_ref()
            .map(|configuration| configuration.artifact);
        let mut work = Live::new_profile(selected, artifact, upload, compiled)?;
        if let Some(authorization) = authorization {
            work = work.with_authorization(authorization)?;
        }
        self.start_owned(work.with_retention(configuration.retention)?)
    }
    pub(super) fn start(&self, selected: Selection, artifact: Option<usize>) -> Result<Uuid> {
        let work = Live::new(selected, artifact)?;
        self.start_owned(work)
    }
    #[cfg(unix)]
    pub(super) fn start_retained(
        &self,
        selected: Selection,
        artifact: Option<usize>,
        budget: super::archive::ArchiveBudget,
    ) -> Result<Uuid> {
        self.start_owned(Live::new(selected, artifact)?.with_retention(budget)?)
    }
    #[cfg(unix)]
    pub(super) fn start_retained_upload(
        &self,
        selected: Selection,
        budget: super::archive::ArchiveBudget,
    ) -> Result<Uuid> {
        self.start_owned(Live::new_input(selected, None, true)?.with_retention(budget)?)
    }
    #[cfg(unix)]
    pub(super) fn start_for_owner(
        &self,
        selected: Selection,
        budget: super::archive::ArchiveBudget,
        upload: bool,
        authorization: super::grant_store::source_authorization::SourceAuthorization,
    ) -> Result<Uuid> {
        self.start_owned(
            Live::new_input(selected, None, upload)?
                .with_authorization(authorization)?
                .with_retention(budget)?,
        )
    }
    pub(super) fn claim_upload(
        &self,
        preparation: Uuid,
        upload: Uuid,
        size: u64,
    ) -> Result<UploadClaim> {
        self.entry(Some(preparation))?
            .upload
            .as_ref()
            .context("original preparation does not accept SDK uploads")?
            .claim(upload, size)
    }
    fn start_owned<W: Work>(&self, work: W) -> Result<Uuid> {
        let mut slot = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("preparation owner unavailable"))?;
        if slot.is_some() {
            bail!("one marketplace preparation is already owned");
        }
        let id = Uuid::new_v4();
        let (cancel, cancellation) = watch::channel(false);
        let (approval, requests) = approval::Slot::channel();
        let view = View {
            schema_version: 1,
            preparation_id: id,
            release_id: work.release(),
            phase: Phase::Preparing,
            review: None,
            installation_ready: false,
            upload: None,
            authorization_lost: false,
        };
        let (status, seen) = watch::channel(Published {
            view,
            observed: Instant::now(),
            operation_failure: None,
        });
        #[cfg(unix)]
        let handoff = work.handoff();
        let upload = work.upload();
        let task = tokio::spawn(run(work, cancellation, status, requests));
        *slot = Some(Arc::new(Entry {
            id,
            status: seen,
            cancel,
            approval: Mutex::new(approval),
            upload,
            task: AsyncMutex::new(Joined::Running(task)),
            #[cfg(unix)]
            handoff,
        }));
        Ok(id)
    }
    fn entry(&self, id: Option<Uuid>) -> Result<Arc<Entry>> {
        self.current()?
            .filter(|entry| id.is_none_or(|id| entry.id == id))
            .context("preparation is unavailable")
    }
    fn current(&self) -> Result<Option<Arc<Entry>>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("preparation owner unavailable"))?
            .as_ref()
            .cloned())
    }
    pub(super) fn view(&self, id: Uuid) -> Result<View> {
        let entry = self.entry(Some(id))?;
        let published = entry.status.borrow();
        let mut view = published.view.clone();
        view.upload = entry.upload.as_ref().and_then(upload::Slot::view);
        // A panic/closed owner or an overdue liveness check cannot leave a
        // positive review on screen indefinitely. This is only display data.
        if entry.status.has_changed().is_err()
            && !matches!(
                view.phase,
                Phase::Cancelled | Phase::Failed | Phase::CleanupFailed | Phase::SourceArchived
            )
        {
            view.phase = Phase::CleanupFailed;
            view.review = None;
        } else if matches!(
            view.phase,
            Phase::AwaitingApproval | Phase::PermissionGranted
        ) && published.observed.elapsed() > REVIEW_TICK * 3
        {
            view.phase = Phase::Failed;
            view.review = None;
        }
        Ok(view)
    }
    pub(super) fn operation_failure(&self, id: Uuid) -> Result<Option<Arc<anyhow::Error>>> {
        let entry = self.entry(Some(id))?;
        let failure = entry.status.borrow().operation_failure.clone();
        Ok(failure)
    }
    #[cfg(test)]
    pub(super) fn fixture_current_failure(&self) -> Result<Option<Arc<anyhow::Error>>> {
        match self.current()? {
            Some(entry) => self.operation_failure(entry.id),
            None => Ok(None),
        }
    }
    pub(super) fn request_stop(&self) {
        // Poison cannot restore positive authority, but still deliver a stop to
        // the actual owned task rather than leave it running until process exit.
        let slot = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = slot.as_ref() {
            entry.cancel.send_replace(true);
        }
    }
    pub(super) async fn cancel(&self, id: Uuid) -> Result<()> {
        let entry = self.entry(Some(id))?;
        entry.cancel.send_replace(true);
        self.join(entry).await
    }
    pub(super) async fn shutdown(&self) -> Result<()> {
        if let Some(entry) = self.current()? {
            entry.cancel.send_replace(true);
            self.join(entry).await?;
        }
        Ok(())
    }
    async fn join(&self, entry: Arc<Entry>) -> Result<()> {
        // Await by mutable reference: a cancelled HTTP/shutdown future cannot
        // detach or consume this cleanup handle, clear its slot, or rearm work.
        let mut task = entry.task.lock().await;
        if let Err(error) = task.join_cleanup().await {
            bail!("preparation cleanup is unconfirmed; ownership remains fenced: {error:#}");
        }
        let mut slot = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("preparation owner unavailable"))?;
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            *slot = None;
        }
        Ok(())
    }
}

impl Joined {
    async fn join_cleanup(&mut self) -> std::result::Result<(), Arc<anyhow::Error>> {
        match self {
            Self::Running(handle) => {
                let result = match handle.await {
                    Ok(result) => result,
                    Err(error) => Err(error).context("original preparation task stopped"),
                }
                .map_err(Arc::new);
                *self = Self::Done(result.clone());
                result
            }
            Self::Done(result) => result.clone(),
        }
    }
}

// Private scheduling seam for deterministic ownership tests, never a public
// factory or evidence substitute. Only Live is attached by MarketplaceService.
#[async_trait]
trait Work: Send + 'static {
    fn release(&self) -> Digest;
    fn expires(&self) -> Instant;
    async fn advance(&mut self) -> Result<Phase>;
    async fn review(&mut self) -> Result<GrantReview>;
    async fn approve(&mut self, request: approval::Request) -> Result<()>;
    fn continues_after_approval(&self) -> bool {
        false
    }
    fn upload(&self) -> Option<upload::Slot> {
        None
    }
    fn authorization(&self) -> Option<authorization::Binding> {
        None
    }
    #[cfg(unix)]
    fn handoff(&self) -> Option<handoff::Slot> {
        None
    }
    async fn cleanup(&mut self) -> Result<()>;
}
fn publish(status: &watch::Sender<Published>, phase: Phase, review: Option<GrantReview>) {
    status.send_modify(|published| {
        published.view.phase = phase;
        published.view.review = review;
        published.observed = Instant::now();
    });
}
async fn cancelled(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
async fn run<W: Work>(
    mut work: W,
    mut stop: watch::Receiver<bool>,
    status: watch::Sender<Published>,
    mut approval: tokio::sync::oneshot::Receiver<approval::Request>,
) -> Result<()> {
    let expires = work.expires();
    let binding = work.authorization();
    let (stop_monitor, stopped) = watch::channel(false);
    let (failed, mut failure) = watch::channel(false);
    let monitor = authorization::monitor(binding, stopped, failed);
    tokio::pin!(monitor);
    let mut monitor_joined = false;
    let mut authorization_lost = false;
    let outcome = async {
        loop {
            let phase = match work.advance().await {
                Ok(phase) => phase,
                Err(error) => return Err::<Phase, anyhow::Error>(error),
            };
            if phase == Phase::SourceArchived {
                return Ok(phase);
            }
            if phase == Phase::AwaitingApproval {
                let mut granted = false;
                loop {
                    let review = work.review().await?;
                    publish(
                        &status,
                        if granted {
                            Phase::PermissionGranted
                        } else {
                            Phase::AwaitingApproval
                        },
                        Some(review),
                    );
                    if granted {
                        sleep(REVIEW_TICK).await;
                    } else {
                        tokio::select! {
                            biased;
                            request=&mut approval=>{
                                let request=request.context("approval owner stopped")?;
                                publish(&status,Phase::Approving,None);
                                work.approve(request).await?;
                                granted=true;
                                if work.continues_after_approval() {
                                    publish(&status,Phase::Archiving,None);
                                    break;
                                }
                            }
                            _=sleep(REVIEW_TICK)=>{}
                        }
                    }
                }
                continue;
            }
            publish(&status, phase, None);
            if phase == Phase::WaitingForResources {
                sleep(RETRY).await;
            }
        }
    };
    // Cancellation drops only the borrowed operation, NOT work/its workspace
    // or an owned opening task. Native startup fences remain independently held.
    // Keep operational failure separate from cleanup. Losing its cause must
    // not require another live operation just to discover what failed, and an
    // operation error must never be mistaken for proof that cleanup succeeded.
    let mut operation_failure = None;
    let mut outcome = tokio::select! {
        biased;
        _ = cancelled(&mut stop) => Phase::Cancelled,
        _ = cancelled(&mut failure) => {authorization_lost=true;Phase::Failed},
        result=&mut monitor=>{
            monitor_joined=true;
            authorization_lost=true;
            if let Err(error)=result {
                tracing::warn!(error=%error,"source authorization lost");
                operation_failure=Some(error.context("source authorization lost"));
            }
            Phase::Failed
        },
        result = timeout_at(expires, outcome) => {
            match result {
                Ok(Ok(phase))=>phase,
                Ok(Err(error))=>{
                    tracing::warn!(error = %error, "marketplace preparation failed");
                    operation_failure=Some(error);
                    Phase::Failed
                }
                Err(error)=>{
                    operation_failure=Some(anyhow::Error::new(error).context("marketplace preparation deadline exceeded"));
                    Phase::Failed
                },
            }
        }
    };
    publish(&status, Phase::Cancelling, None);
    stop_monitor.send_replace(true);
    // Never subject cleanup to the expired work deadline. Each native cleanup
    // already has its own bounded timeout. Errors keep the service slot closed.
    let (cleanup, monitor_result) = tokio::join!(work.cleanup(), async {
        if monitor_joined {
            Ok(())
        } else {
            monitor.await
        }
    });
    // An authorization rejection is expected terminal work failure, not an
    // unknown cleanup. Reaching here proves the original monitor actually
    // returned; do not re-poll it or mistake its Err for a lost task owner.
    if let Err(error) = monitor_result {
        authorization_lost = true;
        tracing::debug!(error=%error,"source monitor joined after withdrawal");
        if operation_failure.is_none() {
            operation_failure = Some(error.context("source authorization lost"));
        }
        if outcome != Phase::Cancelled {
            outcome = Phase::Failed;
        }
    }
    status.send_modify(|s| s.view.authorization_lost = authorization_lost);
    let operation_failure = operation_failure.map(Arc::new);
    status.send_modify(|s| s.operation_failure = operation_failure.clone());
    if let Err(error) = cleanup {
        publish(&status, Phase::CleanupFailed, None);
        let context = match operation_failure {
            Some(failure) => format!(
                "marketplace preparation cleanup unconfirmed; original preparation failed: {failure:#}"
            ),
            None => "marketplace preparation cleanup unconfirmed".to_owned(),
        };
        return Err(error.context(context));
    }
    #[cfg(unix)]
    if outcome == Phase::SourceArchived
        && let Some(slot) = work.handoff()
        && let Err(error) = handoff::complete(&slot)
    {
        publish(&status, Phase::CleanupFailed, None);
        return Err(error);
    }
    publish(&status, outcome, None);
    Ok(())
}

enum Workspace {
    Pending,
    Download(Box<Download>),
    Source(Box<Option<QuarantinedSource>>),
    Manifest(Box<Option<SourceMatchedQuarantine>>),
    #[cfg(unix)]
    Compiling(Box<compiled::Fresh>),
    Review(Box<Option<ManifestCheckedQuarantine>>),
    Granted(Box<super::grant_store::GrantCheckedQuarantine>),
    #[cfg(unix)]
    Archiving(Box<super::archive::Copy>),
    #[cfg(unix)]
    Archived,
    Disposed,
}
impl Workspace {
    fn resource_retryable(&self) -> bool {
        #[cfg(unix)]
        if let Self::Compiling(fresh) = self {
            return fresh.admission_retryable();
        }
        matches!(
            self,
            Self::Pending | Self::Download(_) | Self::Source(_) | Self::Manifest(_)
        )
    }
}
struct Download {
    job: Option<SourceQuarantineJob>,
    purposes: Vec<QuarantinePurpose>,
    slot: usize,
    peer: usize,
    providers: Option<Vec<elixir_marketplace::worker_network::PeerKey>>,
    // Only the last failed candidate, not a growing per-peer history. Private
    // diagnostic data; never a receipt or permission to retry unknown cleanup.
    last_failure: Option<anyhow::Error>,
}
// One workspace slot keeps at most six DHT hints plus six selected catalog
// identities. A hint is not availability: empty/exhausted DHT results must not
// hide an installed source served by an already-known catalog. Deduplicate the
// two sets so a stale candidate is never retried under a different provenance.
// All attempts still spend the original non-refundable job/packet allowances.
fn download_roster(
    mut providers: Vec<elixir_marketplace::worker_network::PeerKey>,
    catalogs: &[elixir_marketplace::worker_network::PeerKey],
) -> Result<Vec<elixir_marketplace::worker_network::PeerKey>> {
    elixir_marketplace::worker_blob::validate_provider_hints(&providers)?;
    if catalogs.len() > super::search::MAX_CATALOG_PEERS {
        bail!("too many selected catalog candidates");
    }
    for peer in catalogs {
        if !providers.contains(peer) {
            providers.push(peer.clone());
        }
    }
    Ok(providers)
}
type OpeningResult = (Selection, Box<Option<SourceQuarantineJob>>, Result<()>);
struct Live {
    selected: Option<Selection>,
    opening: Option<JoinHandle<OpeningResult>>,
    // One original source slot, including before provisioning/after a deferral.
    // Box keeps its bounded owner off nested async continuation stack frames.
    opening_source: Option<Box<Option<SourceQuarantineJob>>>,
    workspace: Workspace,
    artifact: Option<usize>,
    budget: SourceJobBudget,
    release: Digest,
    expires: Instant,
    opening_uncertain: bool,
    instance: Uuid,
    upload: Option<upload::Slot>,
    authorization: Option<Box<authorization::Binding>>,
    #[cfg(unix)]
    retention: Option<super::archive::ArchiveBudget>,
    #[cfg(unix)]
    handoff: Option<handoff::Slot>,
    #[cfg(unix)]
    approved: Option<(Digest, i64)>,
    #[cfg(unix)]
    compiled: Option<Box<compiled::Configuration>>,
    #[cfg(unix)]
    build_record: Option<Box<super::quarantine::local_build::LocalBuildRecord>>,
}
impl Live {
    fn new(selected: Selection, artifact: Option<usize>) -> Result<Self> {
        Self::new_input(selected, artifact, false)
    }
    fn new_input(selected: Selection, artifact: Option<usize>, upload: bool) -> Result<Self> {
        Self::new_profile(
            selected,
            artifact,
            upload,
            #[cfg(unix)]
            None,
        )
    }
    fn new_profile(
        selected: Selection,
        artifact: Option<usize>,
        upload: bool,
        #[cfg(unix)] compiled: Option<compiled::Configuration>,
    ) -> Result<Self> {
        let metadata = selected.metadata()?;
        let payload = metadata.release_payload();
        // Trusted foreground profile; not an HTTP/peer-supplied resource claim.
        // At most twelve candidate identities per blob plus verification share
        // these NON-refundable counters and one absolute deadline. Extra hints
        // never replenish byte/CPU/packet budgets or raise background limits.
        let bytes = payload.package.size
            + payload.source.proof.size
            + artifact
                .map(|i| {
                    payload
                        .artifacts
                        .get(i)
                        .map(|a| a.blob.size)
                        .context("unsigned artifact selection")
                })
                .transpose()?
                .unwrap_or(0);
        let budget = SourceJobBudget {
            memory_bytes: 64 * MIB,
            cpu_millicores: 100,
            disk_bytes: bytes
                .checked_mul(2)
                .and_then(|x| x.checked_add(128 * MIB))
                .context("preparation disk bounds")?,
            input_bytes: bytes
                .checked_mul(12)
                .and_then(|x| x.checked_add(128 * MIB))
                .context("preparation read bounds")?,
            output_bytes: bytes
                .checked_mul(9)
                .and_then(|x| x.checked_add(128 * MIB))
                .context("preparation write bounds")?,
            wall_seconds: elixir_marketplace::quarantine::MAX_JOB_SECONDS,
        };
        #[cfg(unix)]
        let budget = compiled
            .as_ref()
            .map(|configuration| configuration.budget.clone())
            .unwrap_or(budget);
        let plan = metadata.plan(artifact, &budget)?;
        if !upload
            && !selected.client().provider_discovery_enabled()
            && selected.catalog_peers()?.is_empty()
        {
            bail!("no known candidate peers for this release");
        }
        let release = plan.workspace().release_id;
        let authorization = metadata.source_authorization().map(|authorization| {
            Box::new(authorization::Binding {
                authorization,
                database: selected.client().source_database().clone(),
            })
        });
        let expires = Instant::now() + Duration::from_secs(budget.wall_seconds);
        Ok(Self {
            selected: Some(selected),
            opening: None,
            opening_source: Some(Box::new(None)),
            workspace: Workspace::Pending,
            artifact,
            budget,
            release,
            expires,
            opening_uncertain: false,
            instance: Uuid::new_v4(),
            upload: upload.then(upload::Slot::default),
            authorization,
            #[cfg(unix)]
            retention: None,
            #[cfg(unix)]
            handoff: None,
            #[cfg(unix)]
            approved: None,
            #[cfg(unix)]
            compiled: compiled.map(Box::new),
            #[cfg(unix)]
            build_record: None,
        })
    }
    fn with_authorization(
        mut self,
        authorization: super::grant_store::source_authorization::SourceAuthorization,
    ) -> Result<Self> {
        let selected = self
            .selected
            .as_ref()
            .context("original owner selection missing")?;
        let (user, home) = authorization.coordinates();
        selected.metadata()?.require_actor(user, home)?;
        if let Some(original) = &self.authorization {
            if original.authorization.coordinates() != (user, home) {
                bail!("source owner continuation changed");
            }
            // A direct selection already owns the original accepting session.
            // A reconnecting owner must not swap it for another session or
            // replace its fixed deadline while the source is in flight.
            return Ok(self);
        }
        self.authorization = Some(Box::new(authorization::Binding {
            authorization,
            database: selected.client().source_database().clone(),
        }));
        Ok(self)
    }
    #[cfg(unix)]
    fn with_retention(mut self, budget: super::archive::ArchiveBudget) -> Result<Self> {
        self.budget.input_bytes = self
            .budget
            .input_bytes
            .checked_add(budget.metadata_read_allowance()?)
            .context("source retention read bounds")?;
        self.retention = Some(budget);
        self.handoff = Some(Arc::default());
        Ok(self)
    }
    async fn opening(&mut self) -> Result<()> {
        if self.opening.is_none() {
            let mut selected = self
                .selected
                .take()
                .context("preparation selection missing")?;
            let artifact = self.artifact;
            let budget = self.budget.clone();
            let mut source = self
                .opening_source
                .take()
                .context("original opening slot missing")?;
            self.opening = Some(tokio::spawn(async move {
                let result = async {
                    selected.maintain().await?;
                    selected
                        .open_quarantine_retained(&mut source, artifact, &budget)
                        .await
                }
                .await;
                (selected, source, result)
            }));
        }
        // The handle stays in self across cancellation of this await. Cleanup
        // must join the real task before it can dispose or release ownership.
        let opened = self
            .opening
            .as_mut()
            .context("preparation opening missing")?
            .await;
        self.opening = None;
        let (selected, source, result) = match opened {
            Ok(value) => value,
            Err(error) => {
                self.opening_uncertain = true;
                return Err(error).context("owned preparation opening stopped");
            }
        };
        self.selected = Some(selected);
        if let Some(job) = source.as_ref() {
            self.expires = self.expires.min(job.expires());
        }
        self.opening_source = Some(source);
        match result {
            Ok(()) => {
                let slot = self.opening_source.as_mut().context("opening slot lost")?;
                let purposes = slot
                    .as_ref()
                    .as_ref()
                    .context("opening completed without its original job")?
                    .plan_workspace()
                    .descriptors()?
                    .into_iter()
                    .map(|d| d.purpose)
                    .collect();
                let job = slot.take().context("original opening job lost")?;
                self.expires = self.expires.min(job.expires());
                self.workspace = Workspace::Download(Box::new(Download {
                    job: Some(job),
                    purposes,
                    slot: 0,
                    peer: 0,
                    providers: None,
                    last_failure: None,
                }));
                Ok(())
            }
            Err(error) => {
                // A retained job owns its real cleanup, even after an error.
                // Without that original object, never infer a missing job is safe.
                self.opening_uncertain = !self
                    .opening_source
                    .as_ref()
                    .is_some_and(|slot| slot.is_some());
                Err(error)
            }
        }
    }
    fn resource_retryable(&self) -> bool {
        if !self.workspace.resource_retryable() {
            return false;
        }
        if !matches!(self.workspace, Workspace::Pending) {
            return true;
        }
        self.opening_source.as_ref().is_some_and(|slot| {
            slot.as_ref()
                .as_ref()
                .is_some_and(SourceQuarantineJob::admission_unstarted)
        }) && self
            .selected
            .as_ref()
            .is_some_and(|selection| selection.metadata().is_ok())
    }
    async fn next(&mut self) -> Result<Phase> {
        #[cfg(unix)]
        if let Workspace::Archiving(copy) = &mut self.workspace {
            let (selected, archive) = copy.finish().await?;
            let selected = selected.context("archive selection owner missing")?;
            if archive.release_id() != self.release {
                bail!("archive did not retain the selected release");
            }
            let (review, grant_revision) =
                self.approved.context("original archive approval missing")?;
            handoff::retain(
                self.handoff
                    .as_ref()
                    .context("archive handoff owner missing")?,
                selected,
                archive,
                review,
                grant_revision,
            )?;
            self.workspace = Workspace::Archived;
            return Ok(Phase::SourceArchived);
        }
        #[cfg(unix)]
        if matches!(self.workspace, Workspace::Granted(_)) {
            let budget = self
                .retention
                .context("source retention was not requested")?;
            budget.validate()?;
            let selected = self
                .selected
                .as_mut()
                .context("archive selection owner missing")?;
            selected.maintain().await?;
            let Workspace::Granted(source) = &mut self.workspace else {
                unreachable!("matched granted workspace")
            };
            let path = source.source().source().archive_directory()?;
            source.refresh_public_source(selected.public_source()?)?;
            let database = selected.client().source_database().clone();
            if self.compiled.is_some() && self.build_record.is_none() {
                bail!("compiled archive requires actual fresh reproduction history");
            }
            if let Some(record) = self.build_record.as_ref() {
                let matched = source.source().source();
                record
                    .require_descriptor_data(&matched.execution_descriptor(), matched.summary())?;
            }
            // No await between removing these owners and storing their new
            // task. That task survives a cancelled advance/HTTP waiter.
            let selected = self.selected.take().expect("borrowed archive selection");
            let Workspace::Granted(source) =
                std::mem::replace(&mut self.workspace, Workspace::Disposed)
            else {
                unreachable!("matched granted workspace")
            };
            self.workspace =
                Workspace::Archiving(Box::new(super::archive::Copy::begin_with_build(
                    database,
                    *source,
                    Some(selected),
                    path,
                    budget,
                    self.build_record.take().map(|record| *record),
                )));
            return Ok(Phase::Archiving);
        }
        if matches!(self.workspace, Workspace::Pending) {
            self.opening().await?;
            return Ok(Phase::Downloading);
        }
        let selected = self
            .selected
            .as_mut()
            .context("preparation selection missing")?;
        selected.maintain().await?;
        let client = selected.client().clone();
        let peers = selected.catalog_peers()?.to_vec();
        let public_source = selected.public_source()?;
        let step = async {
            match &mut self.workspace {
                Workspace::Download(download) => {
                    let Download {
                        job,
                        purposes,
                        slot,
                        peer,
                        providers,
                        last_failure,
                    } = download.as_mut();
                    if *slot == purposes.len() {
                        let source = SourceQuarantineJob::finish_retained(job).await?;
                        self.workspace = Workspace::Source(Box::new(Some(source)));
                        return Ok(Phase::CheckingSource);
                    }
                    if let Some(upload) = self.upload.as_ref() {
                        let job = job.as_mut().context("original upload quarantine missing")?;
                        let purpose = purposes[*slot];
                        let blob = job.plan_workspace().descriptor(purpose)?.blob;
                        let mut input = upload.receive(purpose, blob).await?;
                        let result = job.receive_upload_owned(purpose, &mut input.stream).await;
                        upload.complete(input, result.is_ok())?;
                        result?;
                        *slot += 1;
                        return Ok(Phase::Downloading);
                    }
                    if client.provider_discovery_enabled() && providers.is_none() {
                        let found = job
                            .as_mut()
                            .context("provider workspace missing")?
                            .find_providers(&client, purposes[*slot])
                            .await?;
                        *providers = Some(download_roster(found, &peers)?);
                        return Ok(Phase::Downloading);
                    }
                    let roster = providers.as_ref().unwrap_or(&peers);
                    let Some(candidate) = roster.get(*peer).cloned() else {
                        return Err(match last_failure.take() {
                            Some(error) => {
                                error.context("known candidates did not supply the signed blob")
                            }
                            None => {
                                anyhow::anyhow!("known candidates did not supply the signed blob")
                            }
                        });
                    };
                    let result = job
                        .as_mut()
                        .context("download workspace missing")?
                        .receive_peer_owned(&client, candidate, purposes[*slot])
                        .await;
                    match result {
                        Ok(()) => {
                            *slot += 1;
                            *peer = 0;
                            *providers = None;
                            *last_failure = None;
                        }
                        Err(error) if ParticipationClient::source_resources_deferred(&error) => {
                            return Err(error);
                        }
                        Err(error) => {
                            *last_failure = Some(error);
                            *peer += 1;
                        }
                    }
                    Ok(Phase::Downloading)
                }
                Workspace::Source(source) => {
                    let matched = QuarantinedSource::match_retained(source).await?;
                    self.workspace = Workspace::Manifest(Box::new(Some(matched)));
                    Ok(Phase::CheckingManifest)
                }
                #[cfg(unix)]
                Workspace::Compiling(fresh) => {
                    if fresh.advance(public_source).await? {
                        let (source, record) = fresh.take_ready(
                            self.compiled
                                .as_deref()
                                .context("trusted compiled configuration missing")?,
                        )?;
                        self.workspace = Workspace::Manifest(Box::new(Some(source)));
                        self.build_record = Some(Box::new(record));
                    }
                    Ok(Phase::CheckingSource)
                }
                Workspace::Manifest(source) => {
                    source
                        .as_mut()
                        .as_mut()
                        .context("matched source missing")?
                        .refresh_public_source(public_source)?;
                    #[cfg(unix)]
                    if let Some(configuration) = self.compiled.as_deref() {
                        if self.build_record.is_none() {
                            let fresh = compiled::Fresh::begin(source, configuration)?;
                            self.workspace = Workspace::Compiling(Box::new(fresh));
                            return Ok(Phase::CheckingSource);
                        }
                    }
                    let checked = SourceMatchedQuarantine::check_manifest_retained(source)?;
                    self.workspace = Workspace::Review(Box::new(Some(checked)));
                    Ok(Phase::AwaitingApproval)
                }
                _ => bail!("preparation phase does not admit another step"),
            }
        };
        tokio::select! {
            biased;
            result = selected.watch_current() => { result?; bail!("preparation authority watcher stopped"); }
            result = step => result,
        }
    }
}
#[async_trait]
impl Work for Live {
    fn authorization(&self) -> Option<authorization::Binding> {
        self.authorization.as_deref().cloned()
    }
    fn upload(&self) -> Option<upload::Slot> {
        self.upload.clone()
    }
    #[cfg(unix)]
    fn handoff(&self) -> Option<handoff::Slot> {
        self.handoff.clone()
    }
    fn continues_after_approval(&self) -> bool {
        #[cfg(unix)]
        {
            self.retention.is_some()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
    fn release(&self) -> Digest {
        self.release
    }
    fn expires(&self) -> Instant {
        self.expires
    }
    async fn advance(&mut self) -> Result<Phase> {
        match self.next().await {
            Err(error)
                if self.resource_retryable()
                    && ParticipationClient::source_resources_deferred(&error) =>
            {
                // One owned job, no new queue/lease/counters/deadline. The outer
                // owner can cancel this wait, and only actual typed headroom
                // deferrals retry; profile, authority and transport errors fail.
                tracing::debug!(error = %error, "preparation resource admission deferred");
                Ok(Phase::WaitingForResources)
            }
            result => result,
        }
    }
    async fn review(&mut self) -> Result<GrantReview> {
        let selected = self.selected.as_mut().context("review selection missing")?;
        selected.maintain().await?;
        match &mut self.workspace {
            Workspace::Review(source) => {
                let source = source
                    .as_mut()
                    .as_mut()
                    .context("permission review source missing")?;
                source.refresh_public_source(selected.public_source()?)?;
                source.source().require_public_source()?;
                source.grant_review()
            }
            Workspace::Granted(source) => {
                source.refresh_public_source(selected.public_source()?)?;
                source
                    .ensure_current(selected.client().source_database())
                    .await?;
                Ok(source.requested().clone())
            }
            _ => bail!("permission review is not prepared"),
        }
    }
    async fn approve(&mut self, request: approval::Request) -> Result<()> {
        let selected = self
            .selected
            .as_mut()
            .context("approval selection missing")?;
        selected.maintain().await?;
        let Workspace::Review(source) = &mut self.workspace else {
            bail!("permission review is not prepared");
        };
        let checked = source
            .as_mut()
            .as_mut()
            .context("permission review source missing")?;
        checked.refresh_public_source(selected.public_source()?)?;
        let database = selected.client().source_database();
        let grant = match request {
            approval::Request::Owner { actor, selection } => {
                selected.metadata()?.require_actor(actor.user, actor.home)?;
                super::grant_store::approve_for_actor(database, &actor, checked, &selection).await?
            }
            #[cfg(unix)]
            approval::Request::Automatic { authorization } => {
                authorization.current(database).await?;
                authorization.require_review(&checked.grant_review()?)?;
                let grant = super::grant_store::load(database, authorization.extension())
                    .await?
                    .context("automatic-update grant disappeared")?;
                if !grant.automatic_updates() || grant.revision() != authorization.grant_revision()
                {
                    bail!("automatic-update consent changed during preparation");
                }
                // Never approve/write the candidate as a new baseline here.
                // check_scope_retained below checks its delta against the
                // original committed review and fresh current grant again.
                grant
            }
        };
        let checked = grant
            .check_scope_retained(database, source, self.instance)
            .await?;
        #[cfg(unix)]
        {
            self.approved = Some((checked.requested().review_id()?, checked.grant_revision()));
        }
        self.workspace = Workspace::Granted(Box::new(checked));
        // A commit may already exist if cancellation/authority loss wins. It is
        // never an install grant, and cleanup must still retain the workspace.
        selected.maintain().await?;
        Ok(())
    }
    async fn cleanup(&mut self) -> Result<()> {
        if let Some(upload) = &self.upload {
            upload.close();
        }
        if self.opening.is_some() {
            // Completion may return a late workspace even after cancellation.
            if let Err(error) = self.opening().await {
                // This typed result proves no new native process was admitted.
                // A cancelled owner may dispose its empty pending phase, not
                // falsely report an unknown container or retry a fresh job.
                if !ParticipationClient::source_resources_deferred(&error)
                    && !self
                        .opening_source
                        .as_ref()
                        .is_some_and(|slot| slot.is_some())
                {
                    return Err(error);
                }
            }
        }
        if self.opening_uncertain {
            bail!("native preparation startup requires confirmed recovery");
        }
        if let Some(slot) = &mut self.opening_source {
            if let Some(job) = slot.as_mut() {
                job.discard_retained().await?;
            }
            **slot = None;
        }
        match &mut self.workspace {
            Workspace::Pending | Workspace::Disposed => {}
            #[cfg(unix)]
            Workspace::Archived => {}
            #[cfg(unix)]
            Workspace::Archiving(copy) => copy.cancel().await?,
            Workspace::Download(download) => {
                download
                    .job
                    .as_mut()
                    .context("cleanup workspace missing")?
                    .discard_retained()
                    .await?
            }
            Workspace::Source(source) => {
                source
                    .as_mut()
                    .as_mut()
                    .context("cleanup source missing")?
                    .discard_retained()
                    .await?
            }
            Workspace::Manifest(source) => {
                source
                    .as_mut()
                    .as_mut()
                    .context("cleanup matched source missing")?
                    .discard_retained()
                    .await?
            }
            #[cfg(unix)]
            Workspace::Compiling(fresh) => fresh.discard().await?,
            Workspace::Review(source) => {
                source
                    .as_mut()
                    .as_mut()
                    .context("cleanup reviewed source missing")?
                    .discard_retained()
                    .await?
            }
            Workspace::Granted(source) => source.discard_retained().await?,
        }
        self.workspace = Workspace::Disposed;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod native_tests;
#[cfg(all(test, unix))]
pub(in crate::marketplace) use native_tests::remove_activation_host as remove_native_activation_host;
#[cfg(test)]
mod tests;
