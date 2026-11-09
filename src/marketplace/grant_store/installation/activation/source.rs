//! One owned claim -> admitted native source -> verified-source handoff.
//! No installed execution, readiness, activation commit or public entry point. Pending
//! opens and cleanup survive cancelled waiters; uncertainty never reissues a
//! claim or clears the durable installation/native fences.
use super::*;
use crate::marketplace::{
    archive::ArchiveBudget,
    quarantine::execution::Broker,
    quarantine::{QuarantinedSource, Restoration, SourceMatchedQuarantine, SourceQuarantineJob},
    worker::{ParticipationClient, SourceResourceWait},
};
use std::time::Duration;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant as Deadline, sleep, sleep_until, timeout},
};

mod closed;
mod prepared;
use closed::Disposal;
pub(in crate::marketplace::grant_store::installation) use closed::DisposedSource;
pub(in crate::marketplace) use prepared::PreparedSource;

const RETRY: Duration = Duration::from_secs(10);
const STOP_GRACE: Duration = Duration::from_secs(45);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::marketplace) enum SourcePhase {
    Opening,
    Restoring,
    CheckingSource,
    SourceVerified,
    Cancelling,
    Cancelled,
    Failed,
}
/// Latest local observation only: no history, peer data or authority token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::marketplace) struct SourceStatus {
    pub phase: SourcePhase,
    pub last_resource_wait: Option<SourceResourceWait>,
}
fn publish(status: &watch::Sender<SourceStatus>, phase: SourcePhase) {
    status.send_replace(SourceStatus {
        phase,
        last_resource_wait: None,
    });
}
async fn resource_pause(
    status: &watch::Sender<SourceStatus>,
    error: anyhow::Error,
    pause: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    let Some(waiting) = ParticipationClient::source_resource_wait(&error) else {
        return Err(error);
    };
    status.send_modify(|status| status.last_resource_wait = Some(waiting));
    pause.await?;
    // A retry timer is not a successful resource sample. Keep the latest
    // reason through another admission check, until actual work progresses.
    Ok(())
}

pub(in crate::marketplace) struct SourceLaunch {
    database: Database,
    home: Uuid,
    configured_instance: Option<Uuid>,
    attempt: Uuid,
    stop: watch::Sender<bool>,
    status: watch::Sender<SourceStatus>,
    task: Option<JoinHandle<LaunchCompletion>>,
    result: Option<Result<ActivationSource>>,
    disposed: Option<DisposedSource>,
    join_failed: bool,
    handed_off: bool,
}
struct LaunchCompletion {
    result: Result<ActivationSource>,
    disposed: Option<DisposedSource>,
}
/// Original source inputs, retained separately from the one-use start claim
/// until pure preflight succeeds. Neither this packet nor its paths are public.
pub(super) struct SourceInputs {
    pub(super) selected: Selection,
    pub(super) archive: ArchivedSource,
    pub(super) plan: SourceActivationPlan,
    pub(super) budget: InstallBudget,
    pub(super) retention: ArchiveBudget,
}
impl SourceLaunch {
    pub(in crate::marketplace) fn begin(
        selected: Selection,
        archive: ArchivedSource,
        start: ActivationStart,
        plan: SourceActivationPlan,
        budget: InstallBudget,
        retention: ArchiveBudget,
    ) -> Result<Self> {
        Self::begin_retained(
            &mut Some(SourceInputs {
                selected,
                archive,
                plan,
                budget,
                retention,
            }),
            &mut Some(start),
        )
    }
    /// All rejection happens before moving either original owner. An owned
    /// activation request can permanently retire an unlaunched claim on error.
    pub(super) fn begin_retained(
        inputs: &mut Option<SourceInputs>,
        start: &mut Option<ActivationStart>,
    ) -> Result<Self> {
        let SourceInputs {
            selected,
            archive,
            plan,
            budget,
            retention,
        } = inputs.as_ref().context("original source inputs missing")?;
        let original = start.as_ref().context("original source start missing")?;
        budget.validate()?;
        retention.validate()?;
        if original.reference.plan != plan.id()
            || original.reference.release != archive.release_id()
            || original.reference.release != plan.source().workspace().release_id
        {
            bail!("source launch differs from its exact start claim");
        }
        let executor = tokio::runtime::Handle::try_current()
            .context("activation source executor unavailable")?;
        let database = selected.client().source_database().clone();
        let home = original.intent.actor.home;
        let configured_instance = original.reference.configured_instance;
        let attempt = original.reference.attempt;
        let (stop, stopped) = watch::channel(false);
        let (status, _) = watch::channel(SourceStatus {
            phase: SourcePhase::Opening,
            last_resource_wait: None,
        });
        let SourceInputs {
            selected,
            archive,
            plan,
            budget,
            retention,
        } = inputs.take().expect("borrowed original source inputs");
        let start = start.take().expect("borrowed original source start");
        let work = Work {
            selected,
            archive,
            start,
            plan,
            budget,
            retention,
        };
        let task = executor.spawn(run(work, stopped, status.clone()));
        Ok(Self {
            database,
            home,
            configured_instance,
            attempt,
            stop,
            status,
            task: Some(task),
            result: None,
            disposed: None,
            join_failed: false,
            handed_off: false,
        })
    }
    pub(in crate::marketplace) fn phase(&self) -> SourcePhase {
        self.status.borrow().phase
    }
    pub(in crate::marketplace) fn status(&self) -> SourceStatus {
        *self.status.borrow()
    }
    /// Read-only coordinates from the original start, not a replacement claim.
    pub(in crate::marketplace::grant_store::installation) fn operation_context(
        &self,
    ) -> (&Database, Uuid, Option<Uuid>, Uuid) {
        (
            &self.database,
            self.home,
            self.configured_instance,
            self.attempt,
        )
    }
    pub(in crate::marketplace::grant_store::installation) fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("activation source owner stopped before confirmed cleanup");
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            match result {
                Ok(completed) => {
                    self.result = Some(completed.result);
                    self.disposed = completed.disposed;
                }
                Err(error) => {
                    self.join_failed = true;
                    return Err(error).context("activation source task stopped");
                }
            }
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn finish(&mut self) -> Result<ActivationSource> {
        self.join().await?;
        let result = self
            .result
            .take()
            .context("activation source result already consumed")?;
        self.handed_off = result.is_ok();
        result
    }
    pub(in crate::marketplace::grant_store::installation) fn take_disposed(
        &mut self,
    ) -> Option<DisposedSource> {
        self.disposed.take()
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        if self.handed_off {
            bail!("activation source was handed off; cancel its current owner");
        }
        self.stop.send_replace(true);
        publish(&self.status, SourcePhase::Cancelling);
        timeout(STOP_GRACE, async {
            self.join().await?;
            if let Some(Ok(source)) = self.result.as_mut() {
                source.cancel().await?;
            }
            if let Some(Ok(source)) = self.result.take() {
                self.disposed = Some(source.into_disposed()?);
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("activation source cleanup is still pending")??;
        publish(&self.status, SourcePhase::Cancelled);
        Ok(())
    }
}
impl Drop for SourceLaunch {
    fn drop(&mut self) {
        // Do not abort the owned open/cleanup task. A completed result owns its
        // own disposal guard, including when the caller never receives it.
        self.stop.send_replace(true);
    }
}

/// Actual native-matched bytes retained under their original lane. Not an
/// execution/installed/serving capability. Dropping an unconsumed handoff joins
/// disposal under that lane rather than leaking an unfenced native workspace.
pub(in crate::marketplace) struct ActivationSource {
    owner: Owner,
    selected: Selection,
    archive: ArchivedSource,
    check: NativeStartCheck,
    plan: SourceActivationPlan,
    budget: InstallBudget,
    expires: Deadline,
    disposal: Option<Disposal>,
}
impl ActivationSource {
    /// A distinct restricted module-load check, not normal invoke or SDK
    /// effects. The successful wrapper retains this exact live source owner;
    /// a serialized record or a publisher-returned JSON value cannot mint it.
    pub(in crate::marketplace) async fn probe(self) -> Result<ProbedSource> {
        let mut slot = Some(self);
        match Self::probe_retained(&mut slot).await {
            Ok(probed) => Ok(probed),
            Err(error) => {
                if let Some(source) = slot.as_mut() {
                    source.confirm_discard().await;
                }
                Err(error)
            }
        }
    }
    pub(in crate::marketplace::grant_store::installation) async fn probe_retained(
        slot: &mut Option<Self>,
    ) -> Result<ProbedSource> {
        slot.as_mut()
            .context("original matched source missing")?
            .check_probe()
            .await?;
        Ok(ProbedSource {
            source: slot.take().expect("borrowed original matched source"),
        })
    }
    async fn check_probe(&mut self) -> Result<()> {
        async {
            self.current().await?;
            let limits = self.plan.probe_limits()?;
            let Stage::Matched(source) = &mut self.owner.stage else {
                bail!("source probe has no matched owner");
            };
            source.refresh_public_source(self.selected.public_source()?)?;
            {
                let mut broker = ProbeBroker {
                    authority: Authority {
                        archive: &self.archive,
                        check: &self.check,
                        plan: &self.plan,
                        budget: self.budget,
                    },
                    selected: &mut self.selected,
                    expires: self.expires,
                    checked: None,
                };
                loop {
                    broker.checked = None;
                    broker.current().await?;
                    match source.probe_source(limits.clone(), &mut broker).await {
                        Ok(()) => break,
                        Err(error) if ParticipationClient::source_resources_deferred(&error) => {
                            // Native admission happens BEFORE reservation charging;
                            // retrying this typed wait keeps the same cumulative
                            // counters, owner and original deadline.
                            let retry = Deadline::now() + RETRY;
                            while Deadline::now() < retry {
                                broker.current().await?;
                                sleep(
                                    Duration::from_millis(500)
                                        .min(retry.saturating_duration_since(Deadline::now())),
                                )
                                .await;
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
                // Force current SQL/security checks after positive native exit and
                // joined cleanup, not just the broker's polling grace.
                broker.checked = None;
                broker.current().await?;
            }
            source.refresh_public_source(self.selected.public_source()?)?;
            active(self.expires)
        }
        .await
    }
    pub(in crate::marketplace) fn summary(
        &self,
    ) -> Result<&elixir_marketplace::source_job::SourceMatchSummary> {
        match &self.owner.stage {
            Stage::Matched(source) => Ok(source.summary()),
            _ => bail!("activation source is unavailable"),
        }
    }
    pub(in crate::marketplace) async fn current(&mut self) -> Result<()> {
        if !matches!(self.owner.stage, Stage::Matched(_)) {
            bail!("activation source is unavailable");
        }
        active(self.expires)?;
        Authority {
            archive: &self.archive,
            check: &self.check,
            plan: &self.plan,
            budget: self.budget,
        }
        .current(&mut self.selected)
        .await?;
        active(self.expires)
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.dispose().await
    }
    async fn dispose(&mut self) -> Result<()> {
        if self.disposal.is_none() {
            self.owner.discard().await?;
            self.disposal = Some(Disposal { _private: () });
        }
        Ok(())
    }
    async fn confirm_discard(&mut self) {
        if self.disposal.is_none() {
            self.owner.confirm_discard().await;
            self.disposal = Some(Disposal { _private: () });
        }
    }
    fn into_disposed(self) -> Result<DisposedSource> {
        let receipt = self
            .disposal
            .context("original source disposal has not joined")?;
        Ok(DisposedSource::new(
            self.selected.client().source_database().clone(),
            self.check,
            self.budget,
            receipt,
        ))
    }
    pub(in crate::marketplace) async fn close(mut self) -> Result<DisposedSource> {
        self.confirm_discard().await;
        self.into_disposed()
    }
}
/// Verified bytes plus an actual no-effects native interface/load check. Not
/// activation commit, long-lived runtime authority or installed-file serving.
pub(in crate::marketplace) struct ProbedSource {
    source: ActivationSource,
}
impl ProbedSource {
    /// Complete source preparation without a second permanent native copy.
    /// The already retained, bounded archive is the durable source. Delete
    /// temporary native work and release its global lane BEFORE recording this
    /// phase. A lost SQL reply is recovered by authenticated journal inspection,
    /// never by replaying a native start or deleting the retained archive.
    pub(in crate::marketplace) async fn retain(self) -> Result<PreparedSource> {
        Self::retain_retained(&mut Some(self)).await
    }
    pub(in crate::marketplace::grant_store::installation) async fn retain_retained(
        slot: &mut Option<Self>,
    ) -> Result<PreparedSource> {
        let reference = slot
            .as_mut()
            .context("original probed source missing")?
            .retain_reference()
            .await?;
        Ok(PreparedSource::from_disposed(
            slot.take().expect("borrowed original probed source").source,
            reference,
        ))
    }
    async fn retain_reference(&mut self) -> Result<ActivationReference> {
        let source = &mut self.source;
        source.current().await?;
        source.dispose().await?;
        // Only a successfully probed owner whose exact native work has joined
        // disposal can reach here. Cancellation during either await retains
        // Owner's existing cleanup guard. No native workspace survives success.
        let proof = DisposedProbe {
            check: &source.check,
        };
        source.selected.maintain().await?;
        let public = source.selected.public_source()?;
        let step = Step {
            database: source.selected.client().source_database(),
            metadata: source.selected.metadata()?,
            public: &public,
            material: Material::from_archive(&source.archive, source.selected.metadata()?)?,
            intent: source.check.intent.clone(),
            plan: &source.plan,
            budget: source.budget,
        };
        let reference = match step.transition(Action::Retain(&proof)).await? {
            Transition::SourcePrepared(reference) => reference,
            _ => bail!("source preparation did not commit its exact probe"),
        };
        Ok(reference)
    }
    /// Fault-injection adapter: perform the real SQL transition but deliberately
    /// discard its acknowledgement, retaining the original native disposal owner.
    #[cfg(test)]
    pub(in crate::marketplace) async fn fixture_discard_retention_ack(&mut self) -> Result<()> {
        self.retain_reference().await?;
        Ok(())
    }
    pub(in crate::marketplace) async fn close(self) -> Result<DisposedSource> {
        self.source.close().await
    }
    pub(in crate::marketplace) async fn current(&mut self) -> Result<()> {
        self.source.current().await
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.source.cancel().await
    }
    pub(in crate::marketplace) fn summary(
        &self,
    ) -> Result<&elixir_marketplace::source_job::SourceMatchSummary> {
        self.source.summary()
    }
}
/// Non-cloneable, non-serializable completion proof, scoped to the actual
/// successful probe/disposal owner. The parent can inspect, not construct it.
pub(super) struct DisposedProbe<'a> {
    check: &'a NativeStartCheck,
}
impl DisposedProbe<'_> {
    pub(super) fn check(&self) -> &NativeStartCheck {
        self.check
    }
    /// SQL-only fixtures do not claim native probe or cleanup qualification.
    #[cfg(test)]
    pub(super) fn fixture(check: &NativeStartCheck) -> DisposedProbe<'_> {
        DisposedProbe { check }
    }
}
struct ProbeBroker<'a> {
    authority: Authority<'a>,
    selected: &'a mut Selection,
    expires: Deadline,
    checked: Option<Deadline>,
}
#[async_trait::async_trait]
impl Broker for ProbeBroker<'_> {
    async fn current(&mut self) -> Result<()> {
        active(self.expires)?;
        self.selected.client().require_blob_owner()?;
        if self
            .checked
            .is_none_or(|last| last.elapsed() >= Duration::from_millis(500))
        {
            self.authority.current(self.selected).await?;
            self.checked = Some(Deadline::now());
        }
        active(self.expires)
    }
    async fn request(
        &mut self,
        _: &elixir_marketplace::source_execution::RuntimeJson,
        _: usize,
    ) -> Result<elixir_marketplace::source_execution::SourceSdkReply> {
        bail!("source interface probes cannot request SDK effects")
    }
}

#[derive(Default)]
enum Stage {
    #[default]
    Empty,
    Opening(Box<Option<SourceQuarantineJob>>),
    Restoring(Box<Restoration>),
    Source(Box<Option<QuarantinedSource>>),
    Matched(Box<SourceMatchedQuarantine>),
}
#[derive(Default)]
struct Owner {
    stage: Stage,
}
impl Owner {
    async fn confirm_discard(&mut self) {
        let mut reported = false;
        loop {
            match self.discard().await {
                Ok(()) => return,
                Err(error) => {
                    if !reported {
                        tracing::warn!(error=%error,"source probe cleanup pending; retaining its owner and lane");
                        reported = true;
                    }
                    sleep(RETRY).await;
                }
            }
        }
    }
    async fn discard(&mut self) -> Result<()> {
        discard(&mut self.stage).await
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let stage = std::mem::take(&mut self.stage);
        if matches!(stage, Stage::Empty) {
            return;
        }
        if let Ok(executor) = tokio::runtime::Handle::try_current() {
            executor.spawn(confirm_disposal(stage));
        }
        // Shutdown without an executor cannot manufacture cleanup. Native
        // activity fences and the Starting installation journal stay durable.
    }
}
async fn discard(stage: &mut Stage) -> Result<()> {
    match stage {
        Stage::Empty => {}
        Stage::Opening(slot) => {
            if let Some(job) = slot.as_mut() {
                job.discard_retained().await?;
            }
        }
        Stage::Restoring(source) => source.cancel().await?,
        Stage::Source(slot) => {
            if let Some(source) = slot.as_mut() {
                source.discard_retained().await?;
            }
        }
        Stage::Matched(source) => source.discard_retained().await?,
    }
    *stage = Stage::Empty;
    Ok(())
}
async fn confirm_disposal(mut stage: Stage) {
    let mut reported = false;
    loop {
        match discard(&mut stage).await {
            Ok(()) => return,
            Err(error) => {
                if !reported {
                    tracing::warn!(error=%error,"activation source cleanup pending; retaining its owner and lane");
                    reported = true;
                }
                sleep(RETRY).await;
            }
        }
    }
}
fn active(expires: Deadline) -> Result<()> {
    if Deadline::now() >= expires {
        bail!("activation source deadline exceeded");
    }
    Ok(())
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() || stop.changed().await.is_err() {
            return;
        }
    }
}
struct Authority<'a> {
    archive: &'a ArchivedSource,
    check: &'a NativeStartCheck,
    plan: &'a SourceActivationPlan,
    budget: InstallBudget,
}
impl Authority<'_> {
    async fn wait_for_resources(
        &self,
        selected: &mut Selection,
        stop: &mut watch::Receiver<bool>,
        expires: Deadline,
        status: &watch::Sender<SourceStatus>,
        error: anyhow::Error,
    ) -> Result<()> {
        // Same owner, claim, reservations and absolute deadline. Only the
        // typed pre-native admission rejection is retryable, never an unknown
        // native result. Refresh current authority even while idle.
        resource_pause(
            status,
            error,
            self.guarded(selected, stop, expires, async {
                sleep(RETRY).await;
                Ok(())
            }),
        )
        .await
    }
    async fn current(&self, selected: &mut Selection) -> Result<()> {
        selected.maintain().await?;
        require_native_start(
            selected.client().source_database(),
            selected.metadata()?,
            &selected.public_source()?,
            self.archive,
            self.check,
            self.plan,
            self.budget,
        )
        .await
    }
    async fn guarded<T>(
        &self,
        selected: &mut Selection,
        stop: &mut watch::Receiver<bool>,
        expires: Deadline,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::pin!(future);
        loop {
            tokio::select! {biased;
                _=stopped(stop)=>bail!("activation source cancelled"),
                _=sleep_until(expires)=>bail!("activation source deadline exceeded"),
                _=sleep(Duration::from_secs(1))=>{
                    // Join each refresh instead of dropping a partly completed
                    // selection mutation when the native future becomes ready.
                    // The native process remains independently deadline-bound.
                    self.current(selected).await?;
                },
                result=&mut future=>return result,
            }
        }
    }
}
struct Work {
    selected: Selection,
    archive: ArchivedSource,
    start: ActivationStart,
    plan: SourceActivationPlan,
    budget: InstallBudget,
    retention: ArchiveBudget,
}
async fn run(
    work: Work,
    mut stop: watch::Receiver<bool>,
    status: watch::Sender<SourceStatus>,
) -> LaunchCompletion {
    let Work {
        mut selected,
        archive,
        start,
        plan,
        budget,
        retention,
    } = work;
    let expires = Deadline::now() + Duration::from_secs(plan.source().workspace().timeout_seconds);
    let check = start.native_check();
    let mut owner = Owner {
        stage: Stage::Opening(Box::new(None)),
    };
    let result = async {
        {
            let authority = Authority {
                archive: &archive,
                check: &check,
                plan: &plan,
                budget,
            };
            let mut start = Some(start);
            loop {
                active(expires)?;
                if *stop.borrow() {
                    bail!("activation source cancelled");
                }
                authority.current(&mut selected).await?;
                active(expires)?;
                if *stop.borrow() {
                    bail!("activation source cancelled");
                }
                let Stage::Opening(slot) = &mut owner.stage else {
                    bail!("native opening owner missing");
                };
                // Intentionally not aborted by cancellation/deadline. Every
                // partial job is already in slot before the first native call.
                let opened = selected
                    .open_activation_source(
                        slot,
                        &archive,
                        start.take().context("original start claim missing")?,
                        &plan,
                        budget,
                    )
                    .await?;
                match opened {
                    SourceOpening::Deferred(claim, waiting) => {
                        start = Some(claim);
                        authority
                            .wait_for_resources(
                                &mut selected,
                                &mut stop,
                                expires,
                                &status,
                                waiting.into(),
                            )
                            .await?;
                    }
                    SourceOpening::Opened => {
                        let path = slot
                            .as_ref()
                            .as_ref()
                            .context("admitted source job missing")?
                            .retained_archive_directory()?;
                        let job = slot.take().context("admitted source job missing")?;
                        owner.stage =
                            Stage::Restoring(Box::new(Restoration::new(job, path, retention)));
                        break;
                    }
                }
            }
            publish(&status, SourcePhase::Restoring);
            let Stage::Restoring(restoring) = &mut owner.stage else {
                bail!("restore owner missing");
            };
            loop {
                authority.current(&mut selected).await?;
                match authority
                    .guarded(&mut selected, &mut stop, expires, restoring.advance())
                    .await
                {
                    Ok(done) => {
                        status.send_modify(|status| status.last_resource_wait = None);
                        if done {
                            break;
                        }
                    }
                    Err(error) => {
                        authority
                            .wait_for_resources(&mut selected, &mut stop, expires, &status, error)
                            .await?
                    }
                }
            }
            let source = loop {
                authority.current(&mut selected).await?;
                match authority
                    .guarded(&mut selected, &mut stop, expires, restoring.finish())
                    .await
                {
                    Ok(source) => break source,
                    Err(error) => {
                        authority
                            .wait_for_resources(&mut selected, &mut stop, expires, &status, error)
                            .await?
                    }
                }
            };
            owner.stage = Stage::Source(Box::new(Some(source)));
            publish(&status, SourcePhase::CheckingSource);
            let Stage::Source(source) = &mut owner.stage else {
                bail!("source verifier owner missing");
            };
            let matched = loop {
                authority.current(&mut selected).await?;
                match authority
                    .guarded(
                        &mut selected,
                        &mut stop,
                        expires,
                        QuarantinedSource::match_retained(source),
                    )
                    .await
                {
                    Ok(matched) => {
                        status.send_modify(|status| status.last_resource_wait = None);
                        break matched;
                    }
                    Err(error) => {
                        authority
                            .wait_for_resources(&mut selected, &mut stop, expires, &status, error)
                            .await?
                    }
                }
            };
            owner.stage = Stage::Matched(Box::new(matched));
            authority.current(&mut selected).await?;
            let Stage::Matched(matched) = &mut owner.stage else {
                bail!("matched source owner missing");
            };
            let retained = Material::from_archive(&archive, selected.metadata()?)?;
            if canonical::to_vec(matched.summary())?
                != canonical::to_vec(retained.source.summary())?
            {
                bail!("native source summary differs from the retained installation source");
            }
            matched.refresh_public_source(selected.public_source()?)?;
            active(expires)?;
            if *stop.borrow() {
                bail!("activation source cancelled");
            }
        }
        publish(&status, SourcePhase::SourceVerified);
        Ok(())
    }
    .await
    .map_err(|error: anyhow::Error| {
        error.context(format!(
            "activation source stopped at {:?}",
            *status.borrow()
        ))
    });
    match result {
        Ok(()) => LaunchCompletion {
            result: Ok(ActivationSource {
                owner,
                selected,
                archive,
                check,
                plan,
                budget,
                expires,
                disposal: None,
            }),
            disposed: None,
        },
        Err(error) => {
            publish(&status, SourcePhase::Cancelling);
            owner.confirm_discard().await;
            let disposed = DisposedSource::new(
                selected.client().source_database().clone(),
                check,
                budget,
                Disposal { _private: () },
            );
            publish(
                &status,
                if *stop.borrow() {
                    SourcePhase::Cancelled
                } else {
                    SourcePhase::Failed
                },
            );
            LaunchCompletion {
                result: Err(error),
                disposed: Some(disposed),
            }
        }
    }
}

#[cfg(test)]
mod tests;
