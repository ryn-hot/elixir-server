//! One retained foreground operation, including source probing. A dropped waiter is
//! not a dropped native owner or permission to infer completion from SQL.
//! Public routing, staging ownership and latest-choice-safe policy
//! remain separate integration gates; this module does not turn them on.
use super::*;
use activation::{
    ActivationRequest, ActivationSource, DisposedSource, PreparedSource, ProbedSource,
    SourceLaunch,
    candidate::{CandidateClaim, CandidateOrRecovery},
    commit::{self, OwnerCommitIntent},
    retirement::{self, OwnerRetiredCancelIntent},
};
use elixir_marketplace::source_execution::SourceExecutionLimits;
use runtime::{InstalledController, RetiredCandidate, UnstartedCandidate};
use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::marketplace) enum Phase {
    QueuedActivation,
    PreparingActivation,
    ClaimingSource,
    PreparingSource,
    ProbingSource,
    DisposingProbe,
    PreparingCandidate,
    RunningCandidate,
    Stopping,
    AwaitingCommit,
    AwaitingCleanup,
    RecoveryRequired,
    Activated,
    Cancelled,
}

/// Bounded display/acknowledgement data only, never native or SQL authority.
#[derive(Clone, Serialize)]
pub(in crate::marketplace) struct View {
    pub operation_id: Uuid,
    pub configured_instance: Uuid,
    pub phase: Phase,
    pub revision: Option<i64>,
}

/// Joining borrows the original handle. A cancelled waiter cannot take it;
/// panic/failed join permanently fences this slot rather than minting a result.
struct OwnedTask<T> {
    handle: Option<JoinHandle<T>>,
    result: Option<T>,
    failed: bool,
}
impl<T> OwnedTask<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
            result: None,
            failed: false,
        }
    }
    fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(JoinHandle::is_finished)
    }
    async fn join(&mut self) -> Result<&mut T> {
        if self.failed {
            bail!("installation operation lost its original cleanup owner");
        }
        if let Some(handle) = self.handle.as_mut() {
            let result = handle.await;
            self.handle = None;
            match result {
                Ok(result) => self.result = Some(result),
                Err(error) => {
                    self.failed = true;
                    return Err(error).context("installation operation owner stopped");
                }
            }
        }
        self.result
            .as_mut()
            .context("installation operation result unavailable")
    }
}

enum Outcome {
    Activation(Box<ActivationRequest>),
    Source(Box<PreparedSource>),
    // Unknown original joins cannot be reconstructed from resource absence.
    SourceUnresolved(Box<SourceLaunch>),
    DisposedSource(Box<DisposedSource>),
    Retired(Box<RetiredCandidate>),
    Unstarted(Box<UnstartedCandidate>),
    // A failed seal/join still retains the original claim or controller slot.
    CandidateUnresolved(Box<Option<CandidateClaim>>),
}
struct Completion {
    controller: Option<InstalledController>,
    outcome: Outcome,
    phase: Phase,
    revision: Option<i64>,
    failure: Option<anyhow::Error>,
}

/// Not Clone/Deserialize. The application must retain ONE operation slot and
/// serialize its owner commands. Constructing this requires the actual original
/// source launch or disposed-probe handoff, and original installed controller.
pub(in crate::marketplace) struct OwnedCandidate {
    id: Uuid,
    configured_instance: Uuid,
    home: Uuid,
    database: Database,
    stop: watch::Sender<bool>,
    phase: watch::Receiver<Phase>,
    task: OwnedTask<Completion>,
}
impl OwnedCandidate {
    /// Retain the exact staged source, encrypted intent and controller BEFORE
    /// any prepare/claim SQL await. No native or SQL start is inferred on retry.
    pub(in crate::marketplace) fn begin_activation(
        owners: &mut Option<(ActivationRequest, InstalledController)>,
        limits: SourceExecutionLimits,
    ) -> Result<Self> {
        let (request, controller) = owners
            .as_ref()
            .context("original activation owners missing")?;
        request.preflight()?;
        controller.preflight_candidate(&limits)?;
        let executor = tokio::runtime::Handle::try_current()
            .context("installation operation executor unavailable")?;
        let (database, home, configured_instance) = request.context()?;
        let database = database.clone();
        // Display identity only; unlike the later entry points, this starts
        // before SQL allocates an attempt. It is never a launch/cleanup token.
        let id = Uuid::new_v4();
        let (stop, stopped) = watch::channel(false);
        let (phase, seen) = watch::channel(Phase::QueuedActivation);
        let (request, controller) = owners.take().expect("borrowed original activation owners");
        let task = OwnedTask::new(
            executor.spawn(run_activation(request, controller, limits, stopped, phase)),
        );
        Ok(Self {
            id,
            configured_instance,
            home,
            database,
            stop,
            phase: seen,
            task,
        })
    }
    /// Adopt the actual original source task BEFORE awaiting its result, probe
    /// or SQL retention. Pure preflight leaves both original owners untouched.
    pub(in crate::marketplace) fn begin_source(
        owners: &mut Option<(SourceLaunch, InstalledController)>,
        limits: SourceExecutionLimits,
    ) -> Result<Self> {
        let (source, controller) = owners.as_ref().context("original source owners missing")?;
        controller.preflight_candidate(&limits)?;
        let executor = tokio::runtime::Handle::try_current()
            .context("installation operation executor unavailable")?;
        let (database, home, configured_instance, id) = source.operation_context();
        let configured_instance = configured_instance
            .context("source operation requires an explicit configured owner")?;
        let database = database.clone();
        let (stop, stopped) = watch::channel(false);
        let (phase, seen) = watch::channel(Phase::PreparingSource);
        let (source, controller) = owners.take().expect("borrowed original source owners");
        let task =
            OwnedTask::new(executor.spawn(run_source(source, controller, limits, stopped, phase)));
        Ok(Self {
            id,
            configured_instance,
            home,
            database,
            stop,
            phase: seen,
            task,
        })
    }
    pub(in crate::marketplace) fn begin(
        owners: &mut Option<(PreparedSource, InstalledController)>,
        limits: SourceExecutionLimits,
    ) -> Result<Self> {
        let (source, controller) = owners
            .as_ref()
            .context("original installation owners missing")?;
        controller.preflight_candidate(&limits)?;
        let executor = tokio::runtime::Handle::try_current()
            .context("installation operation executor unavailable")?;
        let configured_instance = source
            .reference()
            .configured_instance
            .context("installation operation requires an explicit configured owner")?;
        let database = source.database().clone();
        let home = source.home();
        let id = source.reference().attempt();
        let (stop, stopped) = watch::channel(false);
        let (phase, seen) = watch::channel(Phase::PreparingCandidate);
        let (source, controller) = owners
            .take()
            .expect("borrowed original installation owners");
        // No await between moving the original owners and retaining their task.
        let task = OwnedTask::new(executor.spawn(run(source, controller, limits, stopped, phase)));
        Ok(Self {
            id,
            configured_instance,
            home,
            database,
            stop,
            phase: seen,
            task,
        })
    }
    /// Trusted server shutdown/stop, not an authenticated HTTP cleanup action.
    /// A cooperative signal never substitutes for the join below.
    pub(in crate::marketplace) fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    /// UI hints from the actually joined original result, not from status.
    /// They neither join a task nor authorize any effect or cleanup.
    pub(in crate::marketplace) fn display_can_commit(&self) -> bool {
        !self.task.failed
            && self.task.result.as_ref().is_some_and(|result| {
                matches!(result.outcome, Outcome::Retired(_))
                    && matches!(result.phase, Phase::AwaitingCommit | Phase::Activated)
                    && (!*self.stop.borrow() || result.phase == Phase::Activated)
            })
    }
    pub(in crate::marketplace) fn display_can_cancel(&self) -> bool {
        !self.task.failed
            && self.task.result.as_ref().is_none_or(|result| {
                result.phase != Phase::Activated
                    && !matches!(
                        result.outcome,
                        Outcome::SourceUnresolved(_) | Outcome::CandidateUnresolved(_)
                    )
            })
    }
    pub(in crate::marketplace) async fn view(&mut self) -> Result<View> {
        if self.task.is_finished() {
            let result = self.task.join().await?;
            return Ok(View {
                operation_id: self.id,
                configured_instance: self.configured_instance,
                phase: if result.failure.is_some()
                    && !matches!(
                        result.phase,
                        Phase::Activated | Phase::Cancelled | Phase::AwaitingCleanup
                    ) {
                    Phase::RecoveryRequired
                } else {
                    result.phase
                },
                revision: result.revision,
            });
        }
        Ok(View {
            operation_id: self.id,
            configured_instance: self.configured_instance,
            phase: if *self.stop.borrow() {
                Phase::Stopping
            } else {
                *self.phase.borrow()
            },
            revision: None,
        })
    }
    /// Wait for the SAME owned task. Cancellation of this caller retains both
    /// its handle and any eventual original result in this operation.
    pub(in crate::marketplace) async fn wait(&mut self) -> Result<View> {
        self.task.join().await?;
        self.view().await
    }
    /// Physical stop/join only. No journal deletion, commit, discarded-state
    /// acknowledgement or replacement of an unresolved original claim.
    pub(in crate::marketplace) async fn shutdown(&mut self) -> Result<()> {
        self.request_stop();
        let result = self.task.join().await?;
        if let Some(controller) = result.controller.as_mut() {
            controller.shutdown_cache().await?;
        }
        Ok(())
    }
    async fn authorize_cleanup(&self, principal: &CurrentPrincipal) -> Result<()> {
        let actor = Actor::from_principal(principal, false)?;
        if actor.home != self.home {
            bail!("installation operation belongs to a different home");
        }
        let now = Utc::now();
        let started = Instant::now();
        let mut connection = crate::marketplace::durability::connection(&self.database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, self.database.driver).await?;
        let expires = actor.authenticate(&mut tx, now).await?;
        crate::marketplace::preferences::require_home_binding(
            &mut tx,
            actor.home,
            self.database.driver,
        )
        .await?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        Ok(())
    }
    /// Explicit fresh owner action, not automatic permission inherited from
    /// readiness. This is NOT yet a public/latest-choice-safe update policy.
    pub(in crate::marketplace) async fn activate(
        &mut self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<View> {
        if mutation.is_nil() {
            bail!("invalid installation operation activation mutation");
        }
        self.authorize_cleanup(principal).await?;
        self.activate_with(|retired| OwnerCommitIntent::new(principal, retired, mutation))
            .await
    }
    pub(in crate::marketplace) async fn activate_automatic(
        &mut self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<View> {
        if authorization.home != self.home || mutation.is_nil() {
            bail!("automatic activation belongs to another home or has no mutation");
        }
        authorization.current(&self.database).await?;
        self.activate_with(|retired| OwnerCommitIntent::automatic(authorization, retired, mutation))
            .await
    }
    async fn activate_with(
        &mut self,
        intent: impl FnOnce(&runtime::RetiredCandidate) -> Result<OwnerCommitIntent>,
    ) -> Result<View> {
        let result = self.task.join().await?;
        if *self.stop.borrow() && result.phase != Phase::Activated {
            bail!("stopped installation operation cannot activate");
        }
        let Outcome::Retired(retired) = &result.outcome else {
            bail!("installation operation has no original retired candidate");
        };
        let receipt = commit::activate(retired, intent(retired)?).await?;
        result.phase = Phase::Activated;
        result.revision = Some(receipt.revision);
        self.view().await
    }
    /// Authorize BEFORE stopping native work, then independently authorize SQL
    /// cleanup after the actual task joins. Namespace deletion and the compact
    /// cancellation tombstone commit together, using one reserved final write.
    pub(in crate::marketplace) async fn cancel(
        &mut self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<View> {
        if mutation.is_nil() {
            bail!("invalid installation operation cleanup mutation");
        }
        self.authorize_cleanup(principal).await?;
        self.request_stop();
        let result = self.task.join().await?;
        if result.phase == Phase::Activated {
            bail!("an activated installation cannot be discarded as a candidate");
        }
        let revision = match &result.outcome {
            Outcome::Activation(request) => {
                let receipt = request.cancel(principal, mutation).await?;
                result.phase = Phase::Cancelled;
                receipt.revision()
            }
            Outcome::Source(source) => {
                let receipt = source.cancel(principal, mutation).await?;
                result.phase = Phase::Cancelled;
                receipt.revision()
            }
            Outcome::DisposedSource(source) => {
                let receipt = source.cancel(principal, mutation).await?;
                result.phase = Phase::Cancelled;
                receipt.revision()
            }
            Outcome::Retired(retired) => {
                let receipt = retirement::cancel(
                    retired,
                    OwnerRetiredCancelIntent::new(principal, mutation)?,
                )
                .await?;
                result.phase = Phase::Cancelled;
                receipt.revision()
            }
            Outcome::Unstarted(unstarted) => {
                let receipt = unstarted.cancel(principal, mutation).await?;
                result.phase = Phase::Cancelled;
                receipt.revision()
            }
            Outcome::CandidateUnresolved(claim) => {
                if claim.is_some() {
                    bail!("original unstarted claim requires owned recovery, not inferred cleanup");
                }
                bail!("failed original native join requires owned recovery");
            }
            Outcome::SourceUnresolved(source) => {
                let _original_coordinates = source.operation_context();
                bail!("original source start requires owned recovery, not inferred cleanup");
            }
        };
        result.revision = Some(revision);
        self.view().await
    }
    /// The original automatic job may stop and discard ONLY its own candidate
    /// after consent/permissions expire. It still needs the actual original
    /// joined/unstarted outcome; no status or renewed update grant can supply it.
    pub(in crate::marketplace) async fn cancel_automatic(
        &mut self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<View> {
        if authorization.home != self.home
            || authorization.instance() != self.configured_instance
            || mutation.is_nil()
        {
            bail!("automatic cleanup changed its original owner or mutation");
        }
        self.request_stop();
        let result = self.task.join().await?;
        if result.phase == Phase::Activated {
            bail!("an activated installation cannot be discarded as a candidate");
        }
        let receipt = match &result.outcome {
            Outcome::Activation(request) => {
                request.cancel_automatic(authorization, mutation).await?
            }
            Outcome::Source(source) => source.cancel_automatic(authorization, mutation).await?,
            Outcome::DisposedSource(source) => {
                source.cancel_automatic(authorization, mutation).await?
            }
            Outcome::Retired(retired) => {
                retirement::cancel(
                    retired,
                    OwnerRetiredCancelIntent::automatic(authorization, mutation)?,
                )
                .await?
            }
            Outcome::Unstarted(unstarted) => {
                unstarted.cancel_automatic(authorization, mutation).await?
            }
            Outcome::CandidateUnresolved(_) | Outcome::SourceUnresolved(_) => {
                bail!(
                    "automatic cleanup requires the original positive disposal or unstarted proof"
                );
            }
        };
        result.phase = Phase::Cancelled;
        result.revision = Some(receipt.revision());
        self.view().await
    }
    /// Return the SAME controller only after an authenticated terminal SQL
    /// acknowledgement. Unknown joins, retained claims and recovery fences
    /// cannot release the operation slot by presenting a status flag.
    pub(in crate::marketplace) fn take_controller(&mut self) -> Result<InstalledController> {
        let result = self
            .task
            .result
            .as_mut()
            .context("installation operation has not joined")?;
        if self.task.failed || !matches!(result.phase, Phase::Activated | Phase::Cancelled) {
            bail!("installation operation still owns an unresolved result");
        }
        result
            .controller
            .take()
            .context("installation controller already returned")
    }
}
impl Drop for OwnedCandidate {
    fn drop(&mut self) {
        // Never abort: the detached original task still owns its controller,
        // native cleanup and one-shot result until it actually finishes.
        self.request_stop();
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            break;
        }
    }
}
async fn run_activation(
    mut request: ActivationRequest,
    controller: InstalledController,
    limits: SourceExecutionLimits,
    stop: watch::Receiver<bool>,
    phase: watch::Sender<Phase>,
) -> Completion {
    let proceed = async {
        if *stop.borrow() || stop.has_changed().is_err() {
            return Ok(None);
        }
        phase.send_replace(Phase::PreparingActivation);
        // Do not cancel either SQL transition halfway. THIS retained task owns
        // its exact returned objects even if the request waiter disappears.
        Box::pin(request.prepare()).await?;
        if *stop.borrow() || stop.has_changed().is_err() {
            return Ok(None);
        }
        phase.send_replace(Phase::ClaimingSource);
        Box::pin(request.claim()).await?;
        if *stop.borrow() || stop.has_changed().is_err() {
            return Ok(None);
        }
        request.launch().map(Some)
    }
    .await;
    match proceed {
        Ok(Some(source)) => {
            phase.send_replace(Phase::PreparingSource);
            Box::pin(run_source(source, controller, limits, stop, phase)).await
        }
        result => {
            // If a claim actually returned, permanently consume it BEFORE
            // retaining the cleanup result. Unknown SQL replies mint nothing.
            request.retire_unstarted();
            let failure = result.err();
            Completion {
                controller: Some(controller),
                outcome: Outcome::Activation(Box::new(request)),
                phase: if failure.is_some() {
                    Phase::RecoveryRequired
                } else {
                    Phase::AwaitingCleanup
                },
                revision: None,
                failure,
            }
        }
    }
}
async fn run_source(
    mut source: SourceLaunch,
    controller: InstalledController,
    limits: SourceExecutionLimits,
    mut stop: watch::Receiver<bool>,
    phase: watch::Sender<Phase>,
) -> Completion {
    let restored = tokio::select! {
        biased;
        _=stopped(&mut stop)=>{
            phase.send_replace(Phase::Stopping);
            source.request_stop();
            source.finish().await
        }
        restored=source.finish()=>restored,
    };
    let restored = match restored {
        Ok(restored) => restored,
        Err(error) => {
            let disposed = source.take_disposed();
            return source_failure(source, controller, disposed, error);
        }
    };
    // Keep the actual output outside each fallible future. No dropped waiter,
    // changed choice, failed probe or unknown SQL reply can throw away its owner.
    let mut matched = Box::new(Some(restored));
    phase.send_replace(Phase::ProbingSource);
    let probed = match Box::pin(ActivationSource::probe_retained(&mut matched)).await {
        Ok(probed) => probed,
        Err(error) => {
            phase.send_replace(Phase::DisposingProbe);
            let disposed = Box::pin(
                matched
                    .take()
                    .expect("retained original matched source")
                    .close(),
            )
            .await;
            return match disposed {
                Ok(disposed) => source_failure(source, controller, Some(disposed), error),
                Err(cleanup) => source_failure(source, controller, None, cleanup.context(error)),
            };
        }
    };
    let mut probed = Box::new(Some(probed));
    phase.send_replace(Phase::DisposingProbe);
    let prepared = Box::pin(ProbedSource::retain_retained(&mut probed)).await;
    match prepared {
        Ok(prepared) => {
            phase.send_replace(Phase::PreparingCandidate);
            Box::pin(run(prepared, controller, limits, stop, phase)).await
        }
        Err(error) => match Box::pin(
            probed
                .take()
                .expect("retained original probed source")
                .close(),
        )
        .await
        {
            Ok(disposed) => source_failure(source, controller, Some(disposed), error),
            Err(cleanup) => source_failure(source, controller, None, cleanup.context(error)),
        },
    }
}
fn source_failure(
    source: SourceLaunch,
    controller: InstalledController,
    disposed: Option<DisposedSource>,
    error: anyhow::Error,
) -> Completion {
    let (outcome, phase) = match disposed {
        Some(disposed) => (
            Outcome::DisposedSource(Box::new(disposed)),
            Phase::AwaitingCleanup,
        ),
        None => (
            Outcome::SourceUnresolved(Box::new(source)),
            Phase::RecoveryRequired,
        ),
    };
    Completion {
        controller: Some(controller),
        outcome,
        phase,
        revision: None,
        failure: Some(error),
    }
}
async fn run(
    mut source: PreparedSource,
    mut controller: InstalledController,
    limits: SourceExecutionLimits,
    mut stop: watch::Receiver<bool>,
    phase: watch::Sender<Phase>,
) -> Completion {
    if *stop.borrow() || stop.has_changed().is_err() {
        return Completion {
            controller: Some(controller),
            outcome: Outcome::Source(Box::new(source)),
            phase: Phase::AwaitingCleanup,
            revision: None,
            failure: None,
        };
    }
    // Do NOT cancel half of quiesce/copy/claim: let its owned SQL transition
    // settle, preserve the original claim, then signal the actual native task.
    let claim = match Box::pin(source.claim_candidate(&mut controller)).await {
        Ok(CandidateOrRecovery::Claim(claim)) => claim,
        result => {
            return Completion {
                controller: Some(controller),
                outcome: Outcome::Source(Box::new(source)),
                phase: Phase::RecoveryRequired,
                revision: None,
                failure: Some(match result {
                    Err(error) => error,
                    _ => anyhow::anyhow!("installation operation requires original-claim recovery"),
                }),
            };
        }
    };
    let mut slot = Some(claim);
    // A stop during any of the owned quiesce/copy/claim SQL awaits must not
    // start publisher code just because the original claim eventually returns.
    if *stop.borrow() || stop.has_changed().is_err() {
        return unstarted_candidate(controller, slot, None);
    }
    if let Err(error) = controller.begin_candidate(&mut slot, limits) {
        return unstarted_candidate(controller, slot, Some(error));
    }
    phase.send_replace(Phase::RunningCandidate);
    let retired = tokio::select! {
        biased;
        _=stopped(&mut stop)=>{
            phase.send_replace(Phase::Stopping);
            controller.request_stop();
            controller.finish_candidate().await
        }
        retired=controller.finish_candidate()=>retired,
    };
    match retired {
        Ok(retired) => {
            let next = if *stop.borrow() || retired.migration_result().is_err() {
                Phase::AwaitingCleanup
            } else {
                Phase::AwaitingCommit
            };
            Completion {
                controller: Some(controller),
                outcome: Outcome::Retired(Box::new(retired)),
                phase: next,
                revision: None,
                failure: None,
            }
        }
        Err(error) => Completion {
            // The controller retains the failed original task/slot. No result
            // or new claim is fabricated from this uncertain join.
            controller: Some(controller),
            outcome: Outcome::CandidateUnresolved(Box::new(None)),
            phase: Phase::RecoveryRequired,
            revision: None,
            failure: Some(error),
        },
    }
}

fn unstarted_candidate(
    controller: InstalledController,
    mut claim: Option<CandidateClaim>,
    failure: Option<anyhow::Error>,
) -> Completion {
    match controller.seal_unstarted_candidate(&mut claim) {
        Ok(unstarted) => {
            let revision = Some(unstarted.reference().revision());
            Completion {
                controller: Some(controller),
                outcome: Outcome::Unstarted(Box::new(unstarted)),
                phase: Phase::AwaitingCleanup,
                revision,
                failure,
            }
        }
        Err(error) => Completion {
            controller: Some(controller),
            outcome: Outcome::CandidateUnresolved(Box::new(claim)),
            phase: Phase::RecoveryRequired,
            revision: None,
            failure: Some(match failure {
                Some(original) => error.context(original),
                None => error,
            }),
        },
    }
}

#[cfg(test)]
mod tests;
