//! Original encrypted activation input retained BEFORE prepare/claim SQL.
//! Staging remains a separate explicit owner action; no public routing here.
use super::*;
use crate::marketplace::archive::ArchiveBudget;
use source::SourceInputs;

/// Permanently consumes the ORIGINAL one-use source start without launching it.
/// No Clone/Deserialize, reference-to-proof constructor or conversion back to a
/// start. This is not native retirement: no native task received this claim.
pub(in crate::marketplace::grant_store::installation) struct UnstartedSourceStart {
    start: ActivationStart,
}
impl UnstartedSourceStart {
    pub(in crate::marketplace::grant_store::installation) fn reference(
        &self,
    ) -> &ActivationReference {
        &self.start.reference
    }
}

/// One request and its original returned SQL objects, never reconstructed from
/// status. Credentials are already encrypted in the sole owner intent.
pub(in crate::marketplace) struct ActivationRequest {
    staged: StagedInstallation,
    intent: Arc<OwnerActivationIntent>,
    inputs: Option<SourceInputs>,
    preparation: Option<PreparedOrRecovery>,
    prepare_attempted: bool,
    claim_attempted: bool,
    recovery: Option<ActivationReference>,
    start: Option<ActivationStart>,
    unstarted: Option<UnstartedSourceStart>,
}
impl ActivationRequest {
    /// Pure preflight before moving ANY original staging or encrypted setup
    /// owner. A rejected handoff leaves both caller-owned slots intact.
    pub(in crate::marketplace) fn from_stage(
        stage: &mut StageRequest,
        intent: &mut Option<OwnerActivationIntent>,
        plan: SourceActivationPlan,
        retention: ArchiveBudget,
    ) -> Result<Self> {
        stage.budget.validate()?;
        retention.validate()?;
        let setup = intent
            .as_ref()
            .context("original encrypted setup missing")?;
        let staged = stage
            .receipt
            .as_ref()
            .context("original staging receipt missing")?;
        let (selected, archive) = stage
            .owners
            .as_ref()
            .context("original staging source missing")?;
        let metadata = selected.metadata()?;
        let material = Material::from_archive(archive, metadata)?;
        plan.require_material(&material)?;
        material.current(metadata, &selected.public_source()?)?;
        Self::check_receipt(staged, setup, &material.review)?;
        let (selected, archive) = stage
            .owners
            .take()
            .expect("borrowed original staging owners");
        Ok(Self {
            staged: stage
                .receipt
                .take()
                .expect("borrowed original staging receipt"),
            intent: Arc::new(intent.take().expect("borrowed original encrypted setup")),
            inputs: Some(SourceInputs {
                selected,
                archive,
                plan,
                budget: stage.budget,
                retention,
            }),
            preparation: None,
            prepare_attempted: false,
            claim_attempted: false,
            recovery: None,
            start: None,
            unstarted: None,
        })
    }
    pub(in crate::marketplace) fn staged(&self) -> &StagedInstallation {
        &self.staged
    }
    pub(in crate::marketplace) fn new(
        selected: Selection,
        archive: ArchivedSource,
        staged: StagedInstallation,
        intent: OwnerActivationIntent,
        plan: SourceActivationPlan,
        budget: InstallBudget,
        retention: ArchiveBudget,
    ) -> Result<Self> {
        let request = Self {
            staged,
            intent: Arc::new(intent),
            inputs: Some(SourceInputs {
                selected,
                archive,
                plan,
                budget,
                retention,
            }),
            preparation: None,
            prepare_attempted: false,
            claim_attempted: false,
            recovery: None,
            start: None,
            unstarted: None,
        };
        request.preflight()?;
        Ok(request)
    }
    pub(in crate::marketplace::grant_store::installation) fn preflight(&self) -> Result<()> {
        if self.prepare_attempted
            || self.claim_attempted
            || self.preparation.is_some()
            || self.recovery.is_some()
            || self.start.is_some()
            || self.unstarted.is_some()
        {
            bail!("activation request already owns a transition");
        }
        let inputs = self
            .inputs
            .as_ref()
            .context("original activation inputs missing")?;
        inputs.budget.validate()?;
        inputs.retention.validate()?;
        let staged = &self.staged;
        let intent = &self.intent;
        let metadata = inputs.selected.metadata()?;
        let material = Material::from_archive(&inputs.archive, metadata)?;
        Self::check_receipt(staged, intent, &material.review)?;
        inputs.plan.require_material(&material)?;
        material.current(metadata, &inputs.selected.public_source()?)?;
        Ok(())
    }
    fn check_receipt(
        staged: &StagedInstallation,
        intent: &OwnerActivationIntent,
        review: &GrantReview,
    ) -> Result<()> {
        if staged.configured_instance.is_none()
            || intent.configured_instance != staged.configured_instance
            || intent.instance != staged.instance
            || intent.extension != staged.extension
            || intent.release != staged.release
            || intent.staged_revision != staged.revision
        {
            bail!("activation request differs from its explicit configured staging");
        }
        if intent.review != review.review_id()? {
            bail!("activation request review differs from the retained source");
        }
        Ok(())
    }
    pub(in crate::marketplace::grant_store::installation) fn context(
        &self,
    ) -> Result<(&Database, Uuid, Uuid)> {
        let inputs = self
            .inputs
            .as_ref()
            .context("original activation inputs missing")?;
        Ok((
            inputs.selected.client().source_database(),
            self.intent.actor.home,
            self.staged
                .configured_instance
                .context("configured activation owner missing")?,
        ))
    }
    pub(in crate::marketplace::grant_store::installation) async fn prepare(
        &mut self,
    ) -> Result<()> {
        if self.prepare_attempted {
            bail!("activation preparation requires original-result recovery");
        }
        self.prepare_attempted = true;
        let inputs = self
            .inputs
            .as_mut()
            .context("original activation inputs missing")?;
        inputs.selected.maintain().await?;
        let public = inputs.selected.public_source()?;
        let prepared = prepare_checked(Step {
            database: inputs.selected.client().source_database(),
            metadata: inputs.selected.metadata()?,
            public: &public,
            material: Material::from_archive(&inputs.archive, inputs.selected.metadata()?)?,
            intent: self.intent.clone(),
            plan: &inputs.plan,
            budget: inputs.budget,
        })
        .await?;
        self.preparation = Some(prepared);
        if !matches!(self.preparation, Some(PreparedOrRecovery::Prepared(_))) {
            bail!("activation preparation returned recovery coordinates, not a start claim");
        }
        Ok(())
    }
    pub(in crate::marketplace::grant_store::installation) async fn claim(&mut self) -> Result<()> {
        if self.claim_attempted {
            bail!("source claim requires original-result recovery");
        }
        let Some(PreparedOrRecovery::Prepared(prepared)) = self.preparation.as_ref() else {
            bail!("original activation preparation missing");
        };
        self.claim_attempted = true;
        let inputs = self
            .inputs
            .as_mut()
            .context("original activation inputs missing")?;
        inputs.selected.maintain().await?;
        let public = inputs.selected.public_source()?;
        match claim_checked(
            Step {
                database: inputs.selected.client().source_database(),
                metadata: inputs.selected.metadata()?,
                public: &public,
                material: Material::from_archive(&inputs.archive, inputs.selected.metadata()?)?,
                intent: self.intent.clone(),
                plan: &inputs.plan,
                budget: inputs.budget,
            },
            prepared,
        )
        .await?
        {
            StartOrRecovery::Start(start) => self.start = Some(start),
            StartOrRecovery::Recovery(reference) => {
                self.recovery = Some(reference);
                bail!("source start requires recovery, never a replacement claim");
            }
        }
        Ok(())
    }
    pub(in crate::marketplace::grant_store::installation) fn retire_unstarted(&mut self) {
        if let Some(start) = self.start.take() {
            self.unstarted = Some(UnstartedSourceStart { start });
        }
    }
    pub(in crate::marketplace::grant_store::installation) fn launch(
        &mut self,
    ) -> Result<SourceLaunch> {
        SourceLaunch::begin_retained(&mut self.inputs, &mut self.start)
    }
    pub(in crate::marketplace) async fn cancel(
        &self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        self.cancel_with(Actor::from_principal(principal, false)?.into(), mutation)
            .await
    }
    pub(in crate::marketplace) async fn cancel_automatic(
        &self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        self.cancel_with(automatic::EffectActor::recovery(authorization), mutation)
            .await
    }
    async fn cancel_with(
        &self,
        actor: automatic::EffectActor,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        let inputs = self
            .inputs
            .as_ref()
            .context("source ownership was handed off; cancel its current owner")?;
        let intent = if let Some(unstarted) = &self.unstarted {
            cancellation::OwnerCancelIntent::unstarted_source_with(actor, unstarted, mutation)?
        } else {
            if self.start.is_some() || self.recovery.is_some() {
                bail!("source start is unresolved; cannot infer unused cleanup");
            }
            match &self.preparation {
                Some(PreparedOrRecovery::Prepared(prepared)) => {
                    cancellation::OwnerCancelIntent::prepared_with(actor, prepared, mutation)?
                }
                Some(PreparedOrRecovery::Recovery(_)) => {
                    bail!("activation preparation requires owned recovery")
                }
                None => cancellation::OwnerCancelIntent::for_effect_actor(
                    actor,
                    &self.staged,
                    mutation,
                )?,
            }
        };
        cancellation::cancel(
            inputs.selected.client().source_database(),
            intent,
            inputs.budget,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
