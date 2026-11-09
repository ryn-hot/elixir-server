//! Sole owner of one foreground command and all its original handoffs.
use super::*;
use std::time::Duration;
use tokio::time::sleep;

pub(super) enum Origin {
    Owner(CurrentPrincipal),
    Automatic {
        authorization: installation::automatic::Authorization,
        selected: Option<crate::marketplace::search::Selection>,
    },
}
impl Origin {
    pub(super) fn coordinates(&self) -> (Uuid, Uuid) {
        match self {
            Self::Owner(principal) => (principal.user_id, principal.home_id),
            Self::Automatic { authorization, .. } => (authorization.user, authorization.home),
        }
    }
}

pub(super) struct Command {
    context: crate::marketplace::service::InstallContext,
    host: Host,
    origin: Origin,
    query: Uuid,
    input: InstallAction,
    captured: Option<CapturedTarget>,
    preparation: Option<Uuid>,
    archived: Option<Box<preparation::ArchivedPreparation>>,
    stage: Option<Box<StageRequest>>,
    encrypted: Option<OwnerActivationIntent>,
    activation: Option<(ActivationRequest, InstalledController)>,
    operation: Option<Box<OwnedCandidate>>,
    controller: Option<InstalledController>,
    phase: Phase,
    configured: Option<Uuid>,
    revision: Option<i64>,
    pub(super) failures: failure::Failures,
    cleanup_joined: bool,
    acknowledgements: Arc<actions::Tracker>,
    activation_mutation: Option<Uuid>,
    cancellation_mutation: Option<Uuid>,
    automatic_cleanup_attempted: bool,
    automatic_checked: Option<std::time::Instant>,
    source_authorization: Option<grant_store::source_authorization::SourceAuthorization>,
}
impl Command {
    pub(super) fn new(
        context: crate::marketplace::service::InstallContext,
        host: Host,
        origin: Origin,
        query: Uuid,
        input: InstallAction,
        controller: Option<InstalledController>,
        acknowledgements: Arc<actions::Tracker>,
    ) -> Self {
        Self {
            context,
            host,
            origin,
            query,
            input,
            captured: None,
            preparation: None,
            archived: None,
            stage: None,
            encrypted: None,
            activation: None,
            operation: None,
            controller,
            phase: Phase::Queued,
            configured: None,
            revision: None,
            failures: failure::Failures::default(),
            cleanup_joined: false,
            acknowledgements,
            activation_mutation: None,
            cancellation_mutation: None,
            automatic_cleanup_attempted: false,
            automatic_checked: None,
            source_authorization: None,
        }
    }
    fn publish(&mut self, status: &watch::Sender<View>, phase: Phase) {
        self.phase = phase;
        let preparation = self
            .preparation
            .and_then(|id| self.context.preparation.view(id).ok());
        let review = self
            .stage
            .as_ref()
            .map(|stage| stage.review().clone())
            .or_else(|| preparation.as_ref().and_then(|p| p.review.clone()));
        status.send_modify(|view| {
            view.phase = phase;
            view.preparation = preparation;
            view.review = review;
            view.configured_instance = self.configured;
            view.revision = self.revision;
            view.installation_ready = false;
            view.available_actions = self.available_actions();
            view.activation_request_id = self.activation_mutation;
            view.cancellation_request_id = self.cancellation_mutation;
        });
    }
    fn available_actions(&self) -> Vec<ActionKind> {
        let mut available = Vec::with_capacity(3);
        if matches!(
            self.phase,
            Phase::Activated | Phase::Cancelled | Phase::Stopped
        ) {
            return available;
        }
        if self
            .operation
            .as_deref()
            .is_none_or(OwnedCandidate::display_can_cancel)
        {
            available.push(ActionKind::Cancel);
        }
        if matches!(&self.origin, Origin::Automatic { .. })
            && matches!(self.phase, Phase::AwaitingSetup | Phase::AwaitingCommit)
        {
            return available;
        }
        if matches!(self.phase, Phase::Preparing | Phase::AwaitingApproval)
            && self
                .preparation
                .and_then(|id| self.context.preparation.view(id).ok())
                .is_some_and(|view| {
                    view.phase == preparation::Phase::AwaitingApproval && view.review.is_some()
                })
        {
            available.push(ActionKind::Approve);
        }
        if self.phase == Phase::AwaitingSetup
            && self.encrypted.is_none()
            && self.activation.is_none()
            && self.operation.is_none()
            && self
                .stage
                .as_ref()
                .is_some_and(|stage| stage.staged().is_some())
        {
            available.push(ActionKind::Setup);
        }
        if matches!(self.phase, Phase::AwaitingCommit | Phase::RecoveryRequired)
            && self
                .operation
                .as_deref()
                .is_some_and(OwnedCandidate::display_can_commit)
        {
            available.push(ActionKind::Activate);
        }
        if self.phase == Phase::RecoveryRequired
            && self.operation.is_none()
            && self.activation.is_none()
            && self.encrypted.is_none()
            && self.preparation.is_none()
        {
            available.push(ActionKind::RetryStage);
        }
        available
    }
    pub(super) async fn process_message(
        &mut self,
        message: Message,
        status: &watch::Sender<View>,
        stop: &watch::Receiver<bool>,
    ) -> Result<()> {
        let id = message.request_id;
        self.acknowledgements.mark(id, ActionStatus::Running)?;
        let kind = message.action.kind();
        if !message.action.allowed(self.phase)
            || !self.available_actions().contains(&kind)
            || message
                .action
                .require_original_mutation(self.activation_mutation, self.cancellation_mutation)
                .is_err()
            || Box::pin(self.authenticate(message.action.principal()))
                .await
                .is_err()
        {
            // Rejected transport/stale authorization/phase input must not
            // poison a healthy operation or overwrite its retained failure.
            self.acknowledgements.mark(id, ActionStatus::Rejected)?;
            return Ok(());
        }
        let before = self.phase;
        match Box::pin(self.action(message.action, status, stop)).await {
            Ok(()) => self.acknowledgements.mark(id, ActionStatus::Acknowledged)?,
            Err(error) => {
                let unchanged = self.phase == before
                    && match kind {
                        ActionKind::Approve | ActionKind::Cancel => true,
                        ActionKind::Setup => {
                            self.encrypted.is_none()
                                && self.activation.is_none()
                                && self.operation.is_none()
                        }
                        _ => false,
                    };
                if unchanged {
                    self.acknowledgements.mark(id, ActionStatus::Rejected)?;
                } else {
                    self.failures.original(error);
                    self.publish(status, Phase::RecoveryRequired);
                    self.acknowledgements
                        .mark(id, ActionStatus::RecoveryRequired)?;
                }
            }
        }
        Ok(())
    }
    fn same_user(&self, principal: &CurrentPrincipal) -> Result<()> {
        let actor = Actor::from_principal(principal, false)?;
        if (actor.user, actor.home) != self.origin.coordinates() {
            bail!("install action changed original user/home");
        }
        Ok(())
    }
    async fn authenticate(&self, principal: &CurrentPrincipal) -> Result<()> {
        self.same_user(principal)?;
        let actor = Actor::from_principal(principal, false)?;
        let now = chrono::Utc::now();
        let started = std::time::Instant::now();
        let mut connection =
            crate::marketplace::durability::connection(&self.host.database).await?;
        let mut tx = sqlx::Acquire::begin(&mut *connection).await?;
        grant_store::synchronous(&mut tx, self.host.database.driver).await?;
        let expires = actor.authenticate(&mut tx, now).await?;
        crate::marketplace::preferences::require_home_binding(
            &mut tx,
            actor.home,
            self.host.database.driver,
        )
        .await?;
        grant_store::ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        Ok(())
    }
    pub(super) async fn initialize(
        &mut self,
        status: &watch::Sender<View>,
        stop: &watch::Receiver<bool>,
    ) -> Result<()> {
        if self.captured.is_none() {
            self.publish(status, Phase::CapturingTarget);
            let captured = match &self.origin {
                Origin::Owner(principal) => {
                    Box::pin(CapturedTarget::capture(
                        &self.host.database,
                        principal,
                        self.input.clone(),
                    ))
                    .await?
                }
                Origin::Automatic { authorization, .. } => {
                    Box::pin(CapturedTarget::capture_automatic(
                        &self.host.database,
                        authorization,
                        self.input.clone(),
                    ))
                    .await?
                }
            };
            self.captured = Some(captured);
        }
        if let Origin::Owner(principal) = &self.origin {
            if self.source_authorization.is_none() {
                self.source_authorization = Some(
                    grant_store::source_authorization::SourceAuthorization::capture(
                        &self.host.database,
                        principal,
                    )
                    .await?,
                );
            }
            self.source_authorization
                .as_ref()
                .context("original source authorization missing")?
                .current(&self.host.database)
                .await?;
        }
        if *stop.borrow() {
            return Ok(());
        }
        if self.controller.is_none() {
            let controller = Box::pin(InstalledController::open(
                self.host.database.clone(),
                self.host.secrets.clone(),
                self.host.runtime.clone(),
            ))
            .await?;
            self.controller = Some(controller);
        }
        if *stop.borrow() {
            return Ok(());
        }
        self.publish(status, Phase::SelectingSource);
        let selected = match &mut self.origin {
            Origin::Owner(_) => {
                Box::pin(
                    self.context
                        .select(self.query, &self.input.extension, self.input.release),
                )
                .await?
            }
            Origin::Automatic { selected, .. } => {
                let mut selected = selected
                    .take()
                    .context("original automatic source selection missing")?;
                selected.maintain().await?;
                selected
            }
        };
        if *stop.borrow() {
            return Ok(());
        }
        self.preparation = Some(self.context.preparation.start_install(
            selected,
            self.context.upload_source(),
            self.source_authorization.clone(),
            &self.host.runtime,
            self.host.secrets.clone(),
        )?);
        self.publish(status, Phase::Preparing);
        Ok(())
    }
    async fn preparation_tick(&mut self, status: &watch::Sender<View>) -> Result<()> {
        let Some(id) = self.preparation else {
            return Ok(());
        };
        let view = self.context.preparation.view(id)?;
        match view.phase {
            preparation::Phase::SourceArchived => {
                self.context
                    .preparation
                    .take_archive(id, &mut self.archived)
                    .await?;
                self.preparation = None;
                Box::pin(self.stage_original(status)).await?;
            }
            preparation::Phase::Failed
            | preparation::Phase::Cancelled
            | preparation::Phase::CleanupFailed => {
                if let Some(failure) = self.context.preparation.operation_failure(id)? {
                    // Retained only in this original private command, never
                    // serialized in the HTTP view or treated as cleanup failure.
                    return Err(anyhow::anyhow!("{failure:#}")
                        .context("original source preparation did not complete"));
                }
                bail!("original source preparation did not complete")
            }
            preparation::Phase::AwaitingApproval => self.publish(status, Phase::AwaitingApproval),
            _ => self.publish(status, Phase::Preparing),
        }
        Ok(())
    }
    async fn stage_original(&mut self, status: &watch::Sender<View>) -> Result<()> {
        if self.stage.is_none() {
            Box::pin(
                self.archived
                    .as_mut()
                    .context("original archive handoff missing")?
                    .maintain(),
            )
            .await?;
            let captured = self
                .captured
                .as_ref()
                .context("original captured target missing")?;
            let stage = match &self.origin {
                Origin::Owner(principal) => StageRequest::new(
                    principal,
                    captured,
                    &mut self.archived,
                    self.host.installations,
                )?,
                Origin::Automatic { authorization, .. } => StageRequest::automatic(
                    authorization,
                    captured,
                    &mut self.archived,
                    self.host.installations,
                )?,
            };
            #[cfg(test)]
            let stage = stage.fixture_lose_ack(self.context.fixture_lose_stage_ack());
            self.stage = Some(Box::new(stage));
        }
        // Even a pure plan rejection leaves the original source in its slot.
        self.stage
            .as_ref()
            .unwrap()
            .plan(&self.host.runtime, &self.host.secrets)?;
        self.publish(status, Phase::Staging);
        let stage = self
            .stage
            .as_mut()
            .context("original staging request missing")?;
        if let Origin::Owner(principal) = &self.origin {
            stage.reauthorize(principal)?;
        }
        Box::pin(stage.stage()).await?;
        let receipt = stage
            .staged()
            .context("original staging acknowledgement missing")?;
        self.configured = receipt.configured_instance();
        self.revision = Some(receipt.revision());
        self.publish(status, Phase::AwaitingSetup);
        Ok(())
    }
    pub(super) async fn action(
        &mut self,
        action: Action,
        status: &watch::Sender<View>,
        stop: &watch::Receiver<bool>,
    ) -> Result<()> {
        self.same_user(action.principal())?;
        match action {
            Action::Approve {
                principal,
                selection,
            } => {
                if !matches!(self.phase, Phase::AwaitingApproval | Phase::Preparing) {
                    bail!("command is not awaiting permission approval")
                }
                let id = self.preparation.context("original preparation missing")?;
                self.context
                    .preparation
                    .approve(id, &principal, selection)?;
                self.origin = Origin::Owner(principal);
            }
            Action::RetryStage { principal, .. } => {
                if self.phase != Phase::RecoveryRequired
                    || self.operation.is_some()
                    || self.activation.is_some()
                    || self.encrypted.is_some()
                {
                    bail!("original command cannot retry staging")
                }
                self.authenticate(&principal).await?;
                self.origin = Origin::Owner(principal);
                self.failures.clear();
                if self.stage.is_some() || self.archived.is_some() {
                    Box::pin(self.stage_original(status)).await?
                } else if self.preparation.is_some() {
                    bail!("original preparation requires cleanup, not replacement")
                } else {
                    Box::pin(self.initialize(status, stop)).await?
                }
            }
            Action::Setup {
                principal,
                mutation,
                credentials,
            } => {
                if self.phase != Phase::AwaitingSetup
                    || self.encrypted.is_some()
                    || self.activation.is_some()
                    || self.operation.is_some()
                {
                    bail!("command already owns setup or is not awaiting it")
                }
                if self.controller.is_none() {
                    bail!("original installed controller missing")
                }
                let stage = self
                    .stage
                    .as_ref()
                    .context("original staged request missing")?;
                let staged = stage.staged().context("original staging receipt missing")?;
                // Encrypt synchronously before ANY subsequent SQL/native await.
                let intent = if credentials.is_empty() {
                    OwnerActivationIntent::new(
                        &principal,
                        staged,
                        stage.review(),
                        stage.grant_revision(),
                        mutation,
                    )?
                } else {
                    OwnerActivationIntent::new_with_credentials(
                        &principal,
                        staged,
                        stage.review(),
                        stage.grant_revision(),
                        mutation,
                        &self.host.secrets,
                        credentials,
                    )?
                };
                self.start_activation(intent, status)?;
            }
            Action::Activate {
                principal,
                mutation,
            } => {
                if !matches!(
                    self.phase,
                    Phase::AwaitingCommit | Phase::Activated | Phase::RecoveryRequired
                ) {
                    bail!("command has no completed candidate awaiting commit")
                }
                actions::retain_mutation(&mut self.activation_mutation, mutation)?;
                self.publish(status, Phase::Activating);
                let operation = self
                    .operation
                    .as_mut()
                    .context("original candidate operation missing")?;
                let view = Box::pin(operation.activate(&principal, mutation)).await?;
                #[cfg(test)]
                if self.context.fixture_lose_activation_ack() {
                    bail!("fixture discarded the actual committed activation acknowledgement");
                }
                self.revision = view.revision;
                self.controller = Some(operation.take_controller()?);
                self.operation = None;
                self.cleanup_joined = true;
                self.publish(status, Phase::Activated);
            }
            Action::Cancel {
                principal,
                mutation,
            } => {
                if self.phase == Phase::Activated {
                    bail!("activated extension requires explicit disable/uninstall")
                }
                self.authenticate(&principal).await?;
                actions::retain_mutation(&mut self.cancellation_mutation, mutation)?;
                self.publish(status, Phase::Cancelling);
                if matches!(&self.origin, Origin::Automatic { .. }) {
                    // Retry the SAME original rollback intent after a lost
                    // acknowledgement, rather than aliasing it with a new
                    // session-based cancellation hash.
                    Box::pin(self.rollback_automatic()).await?;
                    self.publish(status, Phase::Cancelled);
                    return Ok(());
                }
                if let Some(operation) = self.operation.as_mut() {
                    let view = Box::pin(operation.cancel(&principal, mutation)).await?;
                    self.revision = view.revision;
                    self.controller = Some(operation.take_controller()?);
                    self.operation = None;
                } else if let Some((request, controller)) = self.activation.as_mut() {
                    let receipt = Box::pin(request.cancel(&principal, mutation)).await?;
                    self.revision = Some(receipt.revision());
                    controller.cancel().await?;
                    let (_, controller) = self
                        .activation
                        .take()
                        .expect("borrowed original activation owners");
                    self.controller = Some(controller);
                } else if let Some(stage) = self.stage.as_mut() {
                    self.revision = Box::pin(stage.cancel(&principal, mutation)).await?;
                }
                if let Some(id) = self.preparation {
                    self.context.preparation.cancel(id).await?;
                    self.preparation = None;
                }
                if let Some(controller) = self.controller.as_mut() {
                    controller.cancel().await?;
                }
                self.cleanup_joined = true;
                self.publish(status, Phase::Cancelled);
            }
        }
        Ok(())
    }
    fn start_activation(
        &mut self,
        intent: OwnerActivationIntent,
        status: &watch::Sender<View>,
    ) -> Result<()> {
        self.encrypted = Some(intent);
        let stage = self
            .stage
            .as_ref()
            .context("original staged request missing")?;
        let plan = stage.plan(&self.host.runtime, &self.host.secrets)?;
        let request = ActivationRequest::from_stage(
            self.stage.as_mut().unwrap(),
            &mut self.encrypted,
            plan,
            self.host.runtime.retention,
        )?;
        let controller = self
            .controller
            .take()
            .context("original installed controller missing")?;
        self.activation = Some((request, controller));
        self.operation = Some(Box::new(OwnedCandidate::begin_activation(
            &mut self.activation,
            self.host.execution.clone(),
        )?));
        self.publish(status, Phase::Installing);
        Ok(())
    }
    async fn automatic_tick(&mut self, status: &watch::Sender<View>) -> Result<()> {
        if matches!(
            self.phase,
            Phase::Activated | Phase::Cancelled | Phase::Stopped
        ) {
            return Ok(());
        }
        let authorization = match &self.origin {
            Origin::Owner(_) => return Ok(()),
            Origin::Automatic { authorization, .. } => authorization.clone(),
        };
        authorization.check_job()?;
        // Observe withdrawn standing consent while preparation/native work is
        // running, not only at the next stage boundary. Native effects retain
        // their own current checks; this low-frequency check requests cleanup.
        if self
            .automatic_checked
            .is_none_or(|checked| checked.elapsed() >= Duration::from_secs(5))
        {
            authorization.current(&self.host.database).await?;
            self.automatic_checked = Some(std::time::Instant::now());
        }
        match self.phase {
            Phase::AwaitingApproval => {
                let id = self.preparation.context("original preparation missing")?;
                let view = self.context.preparation.view(id)?;
                let Some(review) = view.review else {
                    return Ok(());
                };
                // A wider contract stays at the existing owner review screen.
                // An explicit owner approval converts this command to manual;
                // never silently ratchet its standing baseline.
                if authorization.require_review(&review).is_err() {
                    return Ok(());
                }
                authorization.current(&self.host.database).await?;
                self.context
                    .preparation
                    .approve_automatic(id, &authorization)?;
            }
            Phase::AwaitingSetup => {
                authorization.current(&self.host.database).await?;
                let stage = self
                    .stage
                    .as_ref()
                    .context("original staged request missing")?;
                let staged = stage.staged().context("original staged receipt missing")?;
                let intent = OwnerActivationIntent::automatic(
                    &authorization,
                    staged,
                    stage.review(),
                    Uuid::new_v4(),
                )?;
                // No new credentials or settings are supplied by a scheduler.
                // Existing configured-choice/secret/state migration checks run
                // through the SAME retained activation driver as manual updates.
                self.start_activation(intent, status)?;
            }
            Phase::AwaitingCommit => {
                let mutation = *self.activation_mutation.get_or_insert_with(Uuid::new_v4);
                self.publish(status, Phase::Activating);
                let operation = self
                    .operation
                    .as_mut()
                    .context("original candidate missing")?;
                let view = Box::pin(operation.activate_automatic(&authorization, mutation)).await?;
                self.revision = view.revision;
                self.controller = Some(operation.take_controller()?);
                self.operation = None;
                self.cleanup_joined = true;
                self.publish(status, Phase::Activated);
            }
            _ => {}
        }
        Ok(())
    }
    /// Retire only actual original owners. This never enables the old code or
    /// rewrites its settings/grant; fresh retained-execution checks decide if
    /// the original generation may run again after its fence is cleared.
    async fn rollback_automatic(&mut self) -> Result<()> {
        if matches!(self.phase, Phase::Activated | Phase::Cancelled) {
            return Ok(());
        }
        let authorization = match &self.origin {
            Origin::Automatic { authorization, .. } => authorization.clone(),
            Origin::Owner(_) => bail!("manual installation requires an explicit owner cleanup"),
        };
        self.automatic_cleanup_attempted = true;
        let mutation = *self.cancellation_mutation.get_or_insert_with(Uuid::new_v4);
        if let Some(operation) = self.operation.as_mut() {
            let view = Box::pin(operation.cancel_automatic(&authorization, mutation)).await?;
            self.revision = view.revision;
            self.controller = Some(operation.take_controller()?);
            self.operation = None;
        } else if let Some((request, controller)) = self.activation.as_mut() {
            let receipt = Box::pin(request.cancel_automatic(&authorization, mutation)).await?;
            self.revision = Some(receipt.revision());
            controller.cancel().await?;
            let (_, controller) = self
                .activation
                .take()
                .expect("borrowed original activation owners");
            self.controller = Some(controller);
        } else if let Some(stage) = self.stage.as_mut() {
            self.revision = Box::pin(stage.cancel_automatic(&authorization, mutation)).await?;
        }
        if let Some(id) = self.preparation {
            self.context.preparation.cancel(id).await?;
            self.preparation = None;
        }
        if let Some(controller) = self.controller.as_mut() {
            controller.cancel().await?;
        }
        // These are now data/source references, not live native owners. Drop
        // only AFTER physical and SQL acknowledgements, so the lane/selection
        // cannot be pinned by a completed command retained for UI history.
        self.encrypted = None;
        self.stage = None;
        self.archived = None;
        if let Origin::Automatic { selected, .. } = &mut self.origin {
            *selected = None;
        }
        self.cleanup_joined = true;
        self.phase = Phase::Cancelled;
        Ok(())
    }
    async fn recover_automatic(&mut self, status: &watch::Sender<View>) {
        if !matches!(&self.origin, Origin::Automatic { .. }) || self.automatic_cleanup_attempted {
            return;
        }
        self.publish(status, Phase::Cancelling);
        match Box::pin(self.rollback_automatic()).await {
            Ok(()) => self.publish(status, Phase::Cancelled),
            Err(error) => {
                self.failures.cleanup(error);
                self.publish(status, Phase::RecoveryRequired);
            }
        }
    }
    pub(super) async fn shutdown(&mut self) -> Result<()> {
        let recovery = if matches!(&self.origin, Origin::Automatic { .. })
            && !matches!(self.phase, Phase::Activated | Phase::Cancelled)
        {
            Box::pin(self.rollback_automatic()).await
        } else {
            Ok(())
        };
        // Even an unconfirmed SQL rollback must still signal/join the actual
        // native owners on shutdown. Keep their unresolved proof/fence retained.
        if let Some(id) = self.preparation {
            self.context.preparation.cancel(id).await?;
            self.preparation = None;
        }
        if let Some(operation) = self.operation.as_mut() {
            operation.shutdown().await?;
        }
        if let Some((_, controller)) = self.activation.as_mut() {
            controller.shutdown_cache().await?;
        }
        if let Some(controller) = self.controller.as_mut() {
            controller.shutdown_cache().await?;
        }
        recovery
    }
    pub(super) fn take_controller(&mut self) -> Result<Option<InstalledController>> {
        if !self.cleanup_joined
            || !matches!(self.phase, Phase::Activated | Phase::Cancelled)
            || self.operation.is_some()
            || self.activation.is_some()
            || self.preparation.is_some()
        {
            bail!("install command still owns unresolved cleanup");
        }
        Ok(self.controller.take())
    }
}

pub(super) async fn run(
    mut command: Command,
    mut requests: mpsc::Receiver<Message>,
    mut stop: watch::Receiver<bool>,
    status: watch::Sender<View>,
) -> Completion {
    if !*stop.borrow()
        && stop.has_changed().is_ok()
        && let Err(error) = Box::pin(command.initialize(&status, &stop)).await
    {
        command.failures.original(error);
        command.publish(&status, Phase::RecoveryRequired);
        Box::pin(command.recover_automatic(&status)).await;
    }
    let mut stopping = false;
    loop {
        if stopping || *stop.borrow() || stop.has_changed().is_err() {
            if let Err(error) = Box::pin(command.shutdown()).await {
                command.failures.cleanup(error);
                command.publish(&status, Phase::RecoveryRequired);
            } else {
                let phase = if matches!(command.phase, Phase::Activated | Phase::Cancelled) {
                    command.phase
                } else {
                    Phase::Stopped
                };
                command.publish(&status, phase);
            }
            break;
        }
        if matches!(command.phase, Phase::Activated | Phase::Cancelled) {
            break;
        }
        enum Event {
            Stop,
            Action(Option<Message>),
            Tick,
            Candidate(Result<installation::operation::View>),
        }
        let event = if let Some(operation) = command.operation.as_mut() {
            tokio::select! { biased; _=stop.changed()=>Event::Stop, action=requests.recv()=>Event::Action(action), result=operation.wait(),if command.phase==Phase::Installing=>Event::Candidate(result), _=sleep(Duration::from_millis(100))=>Event::Tick }
        } else {
            tokio::select! { biased; _=stop.changed()=>Event::Stop, action=requests.recv()=>Event::Action(action), _=sleep(Duration::from_millis(100))=>Event::Tick }
        };
        let result = match event {
            Event::Stop | Event::Action(None) => {
                stopping = true;
                continue;
            }
            Event::Action(Some(message)) => {
                Box::pin(command.process_message(message, &status, &stop)).await
            }
            Event::Tick
                if command.preparation.is_some() && command.phase != Phase::RecoveryRequired =>
            {
                Box::pin(command.preparation_tick(&status)).await
            }
            Event::Tick => Ok(()),
            Event::Candidate(result) => match result {
                Ok(view) => {
                    command.revision = view.revision;
                    command.publish(
                        &status,
                        if view.phase == installation::operation::Phase::AwaitingCommit {
                            Phase::AwaitingCommit
                        } else {
                            Phase::RecoveryRequired
                        },
                    );
                    Ok(())
                }
                Err(error) => Err(error),
            },
        };
        if let Err(error) = result {
            // Do not log credentials or publisher-controlled error text.
            command.failures.original(error);
            command.publish(&status, Phase::RecoveryRequired);
        } else if command.phase != Phase::RecoveryRequired
            && let Err(error) = Box::pin(command.automatic_tick(&status)).await
        {
            command.failures.original(error);
            command.publish(&status, Phase::RecoveryRequired);
        }
        if command.phase == Phase::RecoveryRequired {
            Box::pin(command.recover_automatic(&status)).await;
        }
    }
    let releasable =
        command.cleanup_joined && matches!(command.phase, Phase::Activated | Phase::Cancelled);
    Completion {
        command,
        releasable,
    }
}
