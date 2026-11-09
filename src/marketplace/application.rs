//! One application-owned install, before its first capture/selection await.
//! Internal integration only: public install remains disabled until its other
//! source/network/runtime/HTTP/client launch gates are qualified.
use super::{
    archive::ArchiveBudget,
    grant_store::installation::{
        self, CapturedTarget, InstallAction, InstallBudget, InstallTarget, StageRequest,
    },
    grant_store::installation::{
        activation::{ActivationRequest, CredentialChange, OwnerActivationIntent},
        operation::OwnedCandidate,
        runtime::{InstalledController, InstalledRuntime},
    },
    grant_store::{self, Actor, ApprovalSelection},
    grants::GrantReview,
    preparation,
    worker::ParticipationClient,
};
use crate::{db::Database, http::auth::CurrentPrincipal, secrets::SecretsManager};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{canonical, identity::Digest, source_execution::SourceExecutionLimits};
use futures_util::FutureExt;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, watch},
    task::JoinHandle,
};
use uuid::Uuid;

mod actions;
mod failure;
mod invocation;
mod restoration;
pub(crate) use invocation::InstalledCall;
pub(in crate::marketplace) use invocation::queue::Queue as InstalledCallQueue;
pub(crate) use invocation::queue::Turn as InstalledCallTurn;
#[cfg(test)]
mod native_tests;
pub(in crate::marketplace) use actions::{ActionKind, ActionReceipt, ActionStatus};
#[cfg(test)]
pub(in crate::marketplace) use native_tests::allocation as fixture_allocation;
mod task;
#[cfg(test)]
mod tests;

/// Trusted local allocation, never supplied by a peer or HTTP DTO.
#[derive(Clone)]
pub(in crate::marketplace) struct Allocation {
    pub installations: InstallBudget,
    pub source: super::quarantine::SourceJobBudget,
    pub retention: ArchiveBudget,
    pub execution: SourceExecutionLimits,
}
impl Allocation {
    pub(in crate::marketplace) fn validate(&self) -> Result<()> {
        self.installations.validate()?;
        self.retention.validate()?;
        validate_execution_allocation(&self.source, &self.execution)
    }
}
fn validate_execution_allocation(
    budget: &super::quarantine::SourceJobBudget,
    execution: &SourceExecutionLimits,
) -> Result<()> {
    execution.validate()?;
    super::runtime::SourceJobLimits::new(budget.memory_bytes, budget.cpu_millicores)?;
    if execution.minimum_memory_bytes()? > budget.memory_bytes
        || budget.memory_bytes < 64 * 1024 * 1024
        || budget.memory_bytes % (1024 * 1024) != 0
        || budget.wall_seconds == 0
        || u64::from(execution.timeout_millis).div_ceil(1000) > budget.wall_seconds
        || budget.wall_seconds > elixir_marketplace::quarantine::MAX_JOB_SECONDS
        || budget.disk_bytes == 0
        || budget.input_bytes == 0
        || budget.output_bytes == 0
        || execution.control_input_allowance_for(
            elixir_marketplace::source_execution::SourceExecutionMode::Invoke,
        )? > budget.input_bytes
        || execution.control_output_allowance_for(
            elixir_marketplace::source_execution::SourceExecutionMode::Invoke,
        )? > budget.output_bytes
    {
        bail!("installed execution exceeds its source allocation");
    }
    Ok(())
}
pub(in crate::marketplace) struct Host {
    database: Database,
    secrets: SecretsManager,
    runtime: InstalledRuntime,
    installations: InstallBudget,
    execution: SourceExecutionLimits,
}
impl Host {
    /// Trusted application startup only; this supplies transport configuration,
    /// never endpoint consent, execution/grant authority or a new allocation.
    pub(in crate::marketplace) fn with_local_http(
        mut self,
        client: Option<Arc<crate::marketplace::local_http::Client>>,
    ) -> Self {
        self.runtime.local_http = client;
        self
    }
    pub(in crate::marketplace) fn new(
        client: &ParticipationClient,
        secrets: SecretsManager,
        allocation: Allocation,
    ) -> Result<Self> {
        allocation.validate()?;
        let runtime = client.installed_configuration(allocation.source, allocation.retention)?;
        Self::from_runtime(
            client.source_database().clone(),
            secrets,
            runtime,
            allocation.installations,
            allocation.execution,
        )
    }
    /// Trusted startup data only. Opening an actual controller still performs
    /// independent durable-lineage, OS-owner and native-fence reconciliation.
    pub(in crate::marketplace) fn from_runtime(
        database: Database,
        secrets: SecretsManager,
        runtime: InstalledRuntime,
        installations: InstallBudget,
        execution: SourceExecutionLimits,
    ) -> Result<Self> {
        installations.validate()?;
        runtime.retention.validate()?;
        validate_execution_allocation(&runtime.source_budget, &execution)?;
        if !crate::marketplace::runtime::image_is_pinned(&runtime.image) {
            bail!("installed startup requires a pinned image");
        }
        Ok(Self {
            database,
            secrets,
            runtime,
            installations,
            execution,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::marketplace) enum Phase {
    Queued,
    CapturingTarget,
    SelectingSource,
    Preparing,
    AwaitingApproval,
    Staging,
    AwaitingSetup,
    Installing,
    AwaitingCommit,
    Activating,
    Cancelling,
    Activated,
    Cancelled,
    Stopped,
    RecoveryRequired,
}
/// Secret-free original request DATA for reconnect/correlation/display. This
/// creates no CapturedTarget, approval, source/native owner or cleanup proof.
#[derive(Clone, Serialize)]
pub(in crate::marketplace) struct InitialRequest {
    schema_version: u32,
    query_id: Uuid,
    #[serde(flatten)]
    input: InstallAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    direct_reference: Option<Digest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::marketplace) upload_source: Option<bool>,
}
impl InitialRequest {
    pub(in crate::marketplace) fn new(query_id: Uuid, input: &InstallAction) -> Self {
        Self {
            schema_version: 1,
            query_id,
            input: input.clone(),
            direct_reference: None,
            upload_source: None,
        }
    }
}
#[derive(Clone, Serialize)]
pub(in crate::marketplace) struct View {
    pub operation_id: Uuid,
    pub release_id: Digest,
    pub initial_request: InitialRequest,
    pub phase: Phase,
    pub preparation: Option<preparation::View>,
    pub review: Option<GrantReview>,
    pub configured_instance: Option<Uuid>,
    pub revision: Option<i64>,
    /// Bounded display/correlation only; neither field is an authority token.
    pub available_actions: Vec<ActionKind>,
    pub latest_action: Option<ActionReceipt>,
    /// Exact original SQL inputs for explicit recovery, never execution proof.
    pub activation_request_id: Option<Uuid>,
    pub cancellation_request_id: Option<Uuid>,
    /// Display only. There is deliberately no generic installation capability.
    pub installation_ready: bool,
}

pub(in crate::marketplace) enum Action {
    Approve {
        principal: CurrentPrincipal,
        selection: ApprovalSelection,
    },
    RetryStage {
        principal: CurrentPrincipal,
        mutation: Uuid,
    },
    Setup {
        principal: CurrentPrincipal,
        mutation: Uuid,
        credentials: Vec<CredentialChange>,
    },
    Activate {
        principal: CurrentPrincipal,
        mutation: Uuid,
    },
    Cancel {
        principal: CurrentPrincipal,
        mutation: Uuid,
    },
}
impl Action {
    fn allowed(&self, phase: Phase) -> bool {
        match self {
            Self::Approve { .. } => matches!(phase, Phase::Preparing | Phase::AwaitingApproval),
            Self::RetryStage { .. } => phase == Phase::RecoveryRequired,
            Self::Setup { .. } => phase == Phase::AwaitingSetup,
            Self::Activate { .. } => {
                matches!(phase, Phase::AwaitingCommit | Phase::RecoveryRequired)
            }
            Self::Cancel { .. } => {
                !matches!(phase, Phase::Activated | Phase::Cancelled | Phase::Stopped)
            }
        }
    }
    pub(in crate::marketplace) fn validate(&self) -> Result<()> {
        if self.request_id().is_nil() {
            bail!("install action mutation must be nonzero");
        }
        if let Self::Setup { credentials, .. } = self {
            CredentialChange::validate_input(credentials)?;
        }
        Ok(())
    }
    fn principal(&self) -> &CurrentPrincipal {
        match self {
            Self::Approve { principal, .. }
            | Self::RetryStage { principal, .. }
            | Self::Setup { principal, .. }
            | Self::Activate { principal, .. }
            | Self::Cancel { principal, .. } => principal,
        }
    }
}
struct Message {
    request_id: Uuid,
    action: Action,
}
struct Completion {
    command: task::Command,
    releasable: bool,
}
enum Joined<T> {
    Running(JoinHandle<T>),
    Done(Box<T>),
    Failed,
}
impl<T> Joined<T> {
    async fn join(&mut self) -> Result<&mut T> {
        if let Self::Running(handle) = self {
            let result = handle.await;
            *self = match result {
                Ok(result) => Self::Done(Box::new(result)),
                Err(error) => {
                    *self = Self::Failed;
                    return Err(error).context("original install command stopped");
                }
            };
        }
        match self {
            Self::Done(result) => Ok(result),
            _ => bail!("original install join is unconfirmed"),
        }
    }
    fn poll_finished(&mut self) -> Result<()> {
        if let Self::Running(handle) = self
            && let Some(result) = handle.now_or_never()
        {
            *self = match result {
                Ok(result) => Self::Done(Box::new(result)),
                Err(error) => {
                    *self = Self::Failed;
                    return Err(error).context("original install command stopped");
                }
            };
        }
        Ok(())
    }
}
struct Entry {
    id: Uuid,
    user: Uuid,
    home: Uuid,
    start: Digest,
    stop: watch::Sender<bool>,
    status: watch::Receiver<View>,
    actions: mpsc::Sender<Message>,
    acknowledgements: Arc<actions::Tracker>,
    fingerprints: SecretsManager,
    failures: failure::Failures,
    task: AsyncMutex<Joined<Completion>>,
}
#[derive(Default)]
struct State {
    entry: Option<Arc<Entry>>,
    /// One latest terminal operation for status/exact acknowledgement only.
    /// Its actual command has already transferred its original controller.
    /// Replaced, never appended, when the next installation is reserved.
    completed: Option<Arc<Entry>>,
    controller: Option<InstalledController>,
    invocation: Option<Arc<invocation::Entry>>,
    restoration: Option<Arc<restoration::Entry>>,
    stopped: bool,
}
impl State {
    fn current(&self) -> Option<&Arc<Entry>> {
        self.entry.as_ref().or(self.completed.as_ref())
    }
}
impl Drop for State {
    fn drop(&mut self) {
        if let Some(entry) = &self.entry {
            entry.stop.send_replace(true);
        }
        if let Some(entry) = &self.completed {
            entry.stop.send_replace(true);
        }
        if let Some(call) = &self.invocation {
            call.request_stop();
        }
        if let Some(restoration) = &self.restoration {
            restoration.request_stop();
        }
        if let Some(controller) = &self.controller {
            controller.request_stop();
        }
    }
}
#[derive(Clone, Default)]
pub(in crate::marketplace) struct Service(Arc<Mutex<State>>);
fn start_fingerprint(actor: &Actor, query: Uuid, action: &InstallAction) -> Result<Digest> {
    let target = match &action.target {
        installation::InstallTarget::New { name } => serde_json::json!({"new":name}),
        installation::InstallTarget::Existing { instance } => {
            serde_json::json!({"existing":instance})
        }
    };
    Ok(Digest::of(canonical::to_vec(&serde_json::json!({
        "domain":"elixir-marketplace/application-install/v1","user":actor.user,"home":actor.home,
        "query":query,"extension":action.extension,"release":action.release,"target":target,
        "revision":action.expected_revision,"mutation":action.mutation,"controls":action.controls,
    }))?))
}
fn direct_start_fingerprint(
    actor: &Actor,
    query: Uuid,
    action: &InstallAction,
    reference: Digest,
    upload: bool,
) -> Result<Digest> {
    Ok(Digest::of(canonical::to_vec(&serde_json::json!({
        "domain":"elixir-marketplace/direct-install/v1",
        "request":start_fingerprint(actor, query, action)?, "reference":reference,
        "input":if upload {"sdk_upload"} else {"peers"},
    }))?))
}
impl Service {
    pub(in crate::marketplace) fn acknowledge_direct_start(
        &self,
        principal: &CurrentPrincipal,
        query: Uuid,
        action: &InstallAction,
        reference: Digest,
    ) -> Result<Option<Uuid>> {
        self.acknowledge_source_start(principal, query, action, reference, false)
    }
    pub(in crate::marketplace) fn acknowledge_source_start(
        &self,
        principal: &CurrentPrincipal,
        query: Uuid,
        action: &InstallAction,
        reference: Digest,
        upload: bool,
    ) -> Result<Option<Uuid>> {
        let actor = Actor::from_principal(principal, false)?;
        let fingerprint = direct_start_fingerprint(&actor, query, action, reference, upload)?;
        Ok(self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .current()
            .filter(|entry| {
                entry.user == actor.user && entry.home == actor.home && entry.start == fingerprint
            })
            .map(|entry| entry.id))
    }
    pub(in crate::marketplace) fn start_direct(
        &self,
        context: super::service::InstallContext,
        host: Host,
        principal: CurrentPrincipal,
        query: Uuid,
        action: InstallAction,
        reference: Digest,
    ) -> Result<Uuid> {
        let actor = Actor::from_principal(&principal, false)?;
        if context.direct_reference() != Some(reference) {
            bail!("original direct reference missing");
        }
        let start =
            direct_start_fingerprint(&actor, query, &action, reference, context.upload_source())?;
        self.start_owned(
            context,
            host,
            task::Origin::Owner(principal),
            query,
            action,
            start,
        )
    }
    pub(in crate::marketplace) fn background_idle(&self) -> Result<bool> {
        self.reap_finished()?;
        self.reap_invocation()?;
        let state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?;
        Ok(!state.stopped
            && state.entry.is_none()
            && state.invocation.is_none()
            && state.restoration.is_none()
            && state
                .controller
                .as_ref()
                .is_some_and(|controller| controller.require_idle().is_ok()))
    }
    /// Data acknowledgement of an already retained operation, including while
    /// its discovery connection is unavailable. No source/native owner is made.
    pub(in crate::marketplace) fn acknowledge_start(
        &self,
        principal: &CurrentPrincipal,
        query: Uuid,
        action: &InstallAction,
    ) -> Result<Option<Uuid>> {
        let actor = Actor::from_principal(principal, false)?;
        let fingerprint = start_fingerprint(&actor, query, action)?;
        Ok(self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .current()
            .filter(|entry| {
                entry.user == actor.user && entry.home == actor.home && entry.start == fingerprint
            })
            .map(|entry| entry.id))
    }
    pub(in crate::marketplace) fn start(
        &self,
        context: super::service::InstallContext,
        host: Host,
        principal: CurrentPrincipal,
        query: Uuid,
        action: InstallAction,
    ) -> Result<Uuid> {
        if context.direct_reference().is_some() {
            bail!("direct source needs its explicit direct-start contract");
        }
        let actor = Actor::from_principal(&principal, false)?;
        let start = start_fingerprint(&actor, query, &action)?;
        self.start_owned(
            context,
            host,
            task::Origin::Owner(principal),
            query,
            action,
            start,
        )
    }
    pub(in crate::marketplace) fn start_automatic(
        &self,
        context: super::service::InstallContext,
        host: Host,
        authorization: installation::automatic::Authorization,
        selected: super::search::Selection,
        query: Uuid,
        action: InstallAction,
    ) -> Result<Uuid> {
        authorization.require_target(
            &action.extension,
            action.release,
            match &action.target {
                InstallTarget::Existing { instance } => Some(*instance),
                InstallTarget::New { .. } => None,
            },
        )?;
        let metadata = selected.metadata()?;
        if metadata.discovery().is_none() {
            bail!("automatic update requires gated discovery");
        }
        if metadata.release_payload().extension_id != action.extension
            || Digest::of(canonical::to_vec(metadata.release_payload())?) != action.release
        {
            bail!("automatic update changed its original source selection");
        }
        let start = Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/automatic-install-command/v1",
            "authorization":authorization.request_id()?,"query":query,"input":action,
        }))?);
        self.start_owned(
            context,
            host,
            task::Origin::Automatic {
                authorization,
                selected: Some(selected),
            },
            query,
            action,
            start,
        )
    }
    fn start_owned(
        &self,
        context: super::service::InstallContext,
        host: Host,
        origin: task::Origin,
        query: Uuid,
        action: InstallAction,
        start: Digest,
    ) -> Result<Uuid> {
        let (user, home) = origin.coordinates();
        if let Some(entry) = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .current()
            && entry.user == user
            && entry.home == home
            && entry.start == start
        {
            return Ok(entry.id);
        }
        self.reap_finished()?;
        self.reap_invocation()?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?;
        if state.stopped {
            bail!("marketplace installation is stopping");
        }
        if state.invocation.is_some() || state.restoration.is_some() {
            bail!("original installed call or its result is still owned");
        }
        if let Some(entry) = &state.entry {
            if entry.user == user && entry.home == home && entry.start == start {
                return Ok(entry.id);
            }
            bail!("one marketplace install command is already owned");
        }
        if let Some(controller) = state.controller.as_ref() {
            controller.require_configuration(&host.runtime, &host.secrets)?;
        }
        let id = Uuid::new_v4();
        let (stop, stopped) = watch::channel(false);
        let (actions, requests) = mpsc::channel(1);
        let mut initial_request = InitialRequest::new(query, &action);
        initial_request.direct_reference = context.direct_reference();
        initial_request.upload_source = context.upload_source().then_some(true);
        let (status, seen) = watch::channel(View {
            operation_id: id,
            release_id: action.release,
            initial_request,
            phase: Phase::Queued,
            preparation: None,
            review: None,
            configured_instance: None,
            revision: None,
            available_actions: vec![ActionKind::Cancel],
            latest_action: None,
            activation_request_id: None,
            cancellation_request_id: None,
            installation_ready: false,
        });
        let executor = tokio::runtime::Handle::try_current()
            .context("install command executor unavailable")?;
        let acknowledgements = Arc::new(actions::Tracker::default());
        let fingerprints = host.secrets.clone();
        let command = task::Command::new(
            context,
            host,
            origin,
            query,
            action,
            state.controller.take(),
            acknowledgements.clone(),
        );
        let failures = command.failures.clone();
        let task = executor.spawn(task::run(command, requests, stopped, status));
        state.completed = None;
        state.entry = Some(Arc::new(Entry {
            id,
            user,
            home,
            start,
            stop,
            status: seen,
            actions,
            acknowledgements,
            fingerprints,
            failures,
            task: AsyncMutex::new(Joined::Running(task)),
        }));
        Ok(id)
    }
    fn entry(&self, id: Uuid) -> Result<Arc<Entry>> {
        self.0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .current()
            .filter(|entry| entry.id == id)
            .cloned()
            .context("install command unavailable")
    }
    /// Test observation of the original task only, never an execution receipt.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_current_failures(
        &self,
    ) -> Result<(Option<Arc<anyhow::Error>>, Option<Arc<anyhow::Error>>)> {
        let entry = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .current()
            .cloned();
        match entry {
            Some(entry) => entry.failures.snapshot(),
            None => Ok((None, None)),
        }
    }
    #[cfg(test)]
    pub(in crate::marketplace) async fn fixture_join_original(&self, id: Uuid) -> Result<()> {
        let entry = self.entry(id)?;
        entry.task.lock().await.join().await?;
        Ok(())
    }
    pub(in crate::marketplace) fn view(&self, id: Uuid) -> Result<View> {
        let entry = self.entry(id)?;
        let mut view = entry.status.borrow().clone();
        let closed = entry.status.has_changed().is_err();
        view.latest_action = entry.acknowledgements.current(closed)?;
        if closed
            || view.latest_action.as_ref().is_some_and(|receipt| {
                matches!(receipt.status, ActionStatus::Queued | ActionStatus::Running)
            })
        {
            view.available_actions.clear();
        }
        if closed && !matches!(view.phase, Phase::Activated | Phase::Cancelled) {
            if view.phase != Phase::Stopped {
                view.phase = Phase::RecoveryRequired;
            }
            view.review = None;
            view.preparation = None;
        }
        Ok(view)
    }
    /// Bounded recovery lookup for a reconnected client. Data only: HTTP must
    /// still authenticate the current SQL owner; no native owner is created.
    pub(in crate::marketplace) fn current_for(
        &self,
        principal: &CurrentPrincipal,
    ) -> Result<Option<Uuid>> {
        let actor = Actor::from_principal(principal, false)?;
        Ok(self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .current()
            .filter(|entry| entry.user == actor.user && entry.home == actor.home)
            .map(|entry| entry.id))
    }
    pub(in crate::marketplace) fn view_for(
        &self,
        principal: &CurrentPrincipal,
        id: Uuid,
    ) -> Result<View> {
        let actor = Actor::from_principal(principal, false)?;
        let entry = self.entry(id)?;
        if entry.user != actor.user || entry.home != actor.home {
            bail!("installation belongs to another user/home");
        }
        self.view(id)
    }
    pub(in crate::marketplace) fn submit(&self, id: Uuid, action: Action) -> Result<ActionReceipt> {
        action.validate()?;
        let entry = self.entry(id)?;
        let actor = Actor::from_principal(action.principal(), false)?;
        if actor.user != entry.user || actor.home != entry.home {
            bail!("install action belongs to another user/home");
        }
        let request_id = action.request_id();
        let kind = action.kind();
        let fingerprint = action.fingerprint(id, &entry.fingerprints)?;
        let mut receipt = entry
            .acknowledgements
            .enqueue(request_id, kind, fingerprint, || {
                if *entry.stop.borrow()
                    || entry.actions.is_closed()
                    || entry.status.has_changed().is_err()
                {
                    bail!("install command is stopping or closed");
                }
                let view = entry.status.borrow();
                if !action.allowed(view.phase) || !view.available_actions.contains(&kind) {
                    bail!("install action is not available from its original owners");
                }
                action.require_original_mutation(
                    view.activation_request_id,
                    view.cancellation_request_id,
                )?;
                entry
                    .actions
                    .try_send(Message { request_id, action })
                    .map_err(|_| anyhow::anyhow!("install command action slot busy or closed"))
            })?;
        if entry.status.has_changed().is_err()
            && matches!(receipt.status, ActionStatus::Queued | ActionStatus::Running)
        {
            receipt.status = ActionStatus::Unconfirmed;
        }
        Ok(receipt)
    }
    pub(in crate::marketplace) fn request_stop(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.stopped = true;
        if let Some(entry) = &state.entry {
            entry.stop.send_replace(true);
        }
        if let Some(entry) = &state.completed {
            entry.stop.send_replace(true);
        }
        if let Some(call) = &state.invocation {
            call.request_stop();
        }
        if let Some(restoration) = &state.restoration {
            restoration.request_stop();
        }
        if let Some(controller) = &state.controller {
            controller.request_stop();
        }
    }
    pub(in crate::marketplace) async fn shutdown(&self) -> Result<()> {
        self.request_stop();
        let restoration = self.shutdown_restoration().await;
        let invocation = self.shutdown_invocation().await;
        let entry = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .current()
            .cloned();
        if let Some(entry) = entry {
            let mut task = entry.task.lock().await;
            let result = task.join().await?;
            result.command.shutdown().await?;
        }
        restoration?;
        invocation?;
        let cache = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("installed cache cleanup owner unavailable"))?
            .controller
            .as_ref()
            .map(InstalledController::cache_cleanup_coordinates)
            .transpose()?;
        if let Some((runtime, directory)) = cache {
            let lane = super::lease::ControllerLease::acquire(&directory)?;
            super::quarantine::SourceQuarantineJob::evict_cached(runtime.as_ref(), &lane).await?;
        }
        Ok(())
    }
    fn reap_finished(&self) -> Result<()> {
        let entry = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?
            .entry
            .clone();
        let Some(entry) = entry else { return Ok(()) };
        let Ok(mut task) = entry.task.try_lock() else {
            return Ok(());
        };
        task.poll_finished()?;
        let Joined::Done(result) = &mut *task else {
            return Ok(());
        };
        if !result.releasable {
            return Ok(());
        }
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("install owner unavailable"))?;
        if state
            .entry
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            if state.controller.is_some() || state.completed.is_some() || state.invocation.is_some()
            {
                bail!("original installed controller already retained");
            }
            state.controller = result.command.take_controller()?;
            state.completed = state.entry.take();
        }
        Ok(())
    }
}
