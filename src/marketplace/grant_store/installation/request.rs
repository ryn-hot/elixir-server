//! Original source and staging input owned by the application before SQL.
//! Recovering its exact disabled receipt is data acknowledgement, never a
//! native launch/disposal proof or permission to reconstruct another request.
use super::*;
use crate::marketplace::preparation::ArchivedPreparation;
mod recovery;

pub(in crate::marketplace) struct StageRequest {
    pub(super) owners: Option<(Selection, ArchivedSource)>,
    pub(super) receipt: Option<StagedInstallation>,
    intent: OwnerInstallIntent,
    review: GrantReview,
    request: Digest,
    source: Digest,
    attempted: bool,
    #[cfg(test)]
    lose_ack: bool,
    pub(super) budget: InstallBudget,
}
impl StageRequest {
    /// Discard a real committed receipt in an explicit local composition test.
    /// This is not a TCP fault or a native/execution capability constructor.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_lose_ack(mut self, lose: bool) -> Self {
        self.lose_ack = lose;
        self
    }
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        captured: &CapturedTarget,
        archived: &mut Option<Box<ArchivedPreparation>>,
        budget: InstallBudget,
    ) -> Result<Self> {
        Self::new_with_intent(archived, budget, |review, revision| {
            OwnerInstallIntent::new_captured(principal, review, revision, captured)
        })
    }
    pub(in crate::marketplace) fn automatic(
        authorization: &automatic::Authorization,
        captured: &CapturedTarget,
        archived: &mut Option<Box<ArchivedPreparation>>,
        budget: InstallBudget,
    ) -> Result<Self> {
        Self::new_with_intent(archived, budget, |review, revision| {
            if revision != authorization.grant_revision() {
                bail!("automatic-update preparation changed grant revision");
            }
            OwnerInstallIntent::new_automatic(authorization, review, captured)
        })
    }
    fn new_with_intent(
        archived: &mut Option<Box<ArchivedPreparation>>,
        budget: InstallBudget,
        intent: impl FnOnce(&GrantReview, i64) -> Result<OwnerInstallIntent>,
    ) -> Result<Self> {
        budget.validate()?;
        let (selected, archive, approved, grant_revision) = archived
            .as_ref()
            .context("original preparation handoff missing")?
            .parts();
        let material = Material::from_archive(archive, selected.metadata()?)?;
        if material.review.review_id()? != approved {
            bail!("original archive review differs from its permission decision");
        }
        let intent = intent(&material.review, grant_revision)?;
        let request = intent.request_id(&material)?;
        let source = material.source.id()?;
        // Pure preflight above leaves the original pair untouched on failure.
        let (selected, archive, _, _) = archived
            .take()
            .expect("borrowed original handoff")
            .into_parts();
        Ok(Self {
            owners: Some((selected, archive)),
            receipt: None,
            intent,
            review: material.review,
            request,
            source,
            attempted: false,
            #[cfg(test)]
            lose_ack: false,
            budget,
        })
    }
    pub(in crate::marketplace) fn review(&self) -> &GrantReview {
        &self.review
    }
    pub(in crate::marketplace) fn grant_revision(&self) -> i64 {
        self.intent.grant_revision
    }
    pub(in crate::marketplace) fn staged(&self) -> Option<&StagedInstallation> {
        self.receipt.as_ref()
    }
    pub(in crate::marketplace) fn plan(
        &self,
        configuration: &runtime::InstalledRuntime,
        secrets: &SecretsManager,
    ) -> Result<activation::SourceActivationPlan> {
        let (selected, archive) = self
            .owners
            .as_ref()
            .context("original staged source already handed off")?;
        let material = Material::from_archive(archive, selected.metadata()?)?;
        activation::SourceActivationPlan::for_material(&material, configuration, secrets)
    }
    /// An explicit renewed session can authenticate the SAME user's original
    /// action, never replace its home/release/mutation/expected choice versions.
    pub(in crate::marketplace) fn reauthorize(
        &mut self,
        principal: &CurrentPrincipal,
    ) -> Result<()> {
        let actor =
            Actor::from_principal(principal, !self.review.scope().secret_keys().is_empty())?;
        self.require_same(&actor)?;
        self.intent.actor = actor.into();
        Ok(())
    }
    fn require_same(&self, actor: &Actor) -> Result<()> {
        if actor.user != self.intent.actor.user || actor.home != self.intent.actor.home {
            bail!("staging belongs to the original user and home");
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn stage(&mut self) -> Result<()> {
        if self.receipt.is_some() {
            return Ok(());
        }
        let (selected, archive) = self
            .owners
            .as_mut()
            .context("original staging source missing")?;
        selected.maintain().await?;
        let material = Material::from_archive(archive, selected.metadata()?)?;
        if self.intent.request_id(&material)? != self.request
            || material.source.id()? != self.source
        {
            bail!("original staging request changed");
        }
        let public = selected.public_source()?;
        // A local cache refusal must not block installation or create authority.
        if selected.client().sharing_capable()
            && let Some(metadata) = selected.metadata()?.discovery()
        {
            let _ = selected
                .client()
                .retain_admitted_metadata(metadata, &public)
                .await;
        }
        // Preserve the exact input and any eventual receipt in THIS object;
        // caller cancellation must only lose a borrow of its owned task.
        self.attempted = true;
        let receipt = stage_borrowed(
            selected.client().source_database(),
            selected.metadata()?,
            &public,
            material,
            &self.intent,
            self.budget,
        )
        .await?;
        #[cfg(test)]
        if self.lose_ack {
            self.lose_ack = false;
            drop(receipt);
            bail!("fixture discarded the actual staging acknowledgement")
        }
        self.receipt = Some(receipt);
        Ok(())
    }
    /// Only THIS original attempted request can acknowledge an exact committed
    /// staging result after reply loss, without requiring source/grant renewal.
    /// No activation fence, altered journal or status-only caller is accepted.
    pub(in crate::marketplace) async fn recover_receipt(
        &mut self,
        principal: &CurrentPrincipal,
    ) -> Result<bool> {
        let actor = Actor::from_principal(principal, false)?;
        self.require_same(&actor)?;
        self.recover_receipt_with(actor.into()).await
    }
    async fn recover_receipt_with(&mut self, actor: automatic::EffectActor) -> Result<bool> {
        if self.receipt.is_some() {
            return Ok(true);
        }
        if !self.attempted {
            return Ok(false);
        }
        let (selected, _) = self
            .owners
            .as_ref()
            .context("original staging source missing")?;
        let database = selected.client().source_database();
        let receipt = recovery::read_with_actor(
            database,
            actor,
            &self.intent,
            self.review.scope().extension_id(),
            self.request,
            self.source,
        )
        .await?;
        self.receipt = receipt;
        Ok(self.receipt.is_some())
    }
    pub(in crate::marketplace) async fn cancel(
        &mut self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<Option<i64>> {
        let actor = Actor::from_principal(principal, false)?;
        self.require_same(&actor)?;
        self.cancel_with(actor.into(), mutation).await
    }
    pub(in crate::marketplace) async fn cancel_automatic(
        &mut self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<Option<i64>> {
        self.cancel_with(automatic::EffectActor::recovery(authorization), mutation)
            .await
    }
    async fn cancel_with(
        &mut self,
        actor: automatic::EffectActor,
        mutation: Uuid,
    ) -> Result<Option<i64>> {
        if actor.user != self.intent.actor.user || actor.home != self.intent.actor.home {
            bail!("cleanup belongs to the original update user/home");
        }
        actor.require_cleanup_target(
            self.review.scope().extension_id(),
            self.intent.release,
            self.intent
                .configured
                .as_ref()
                .map(|target| target.instance()),
        )?;
        if mutation.is_nil() {
            bail!("cancellation mutation must be nonzero")
        }
        if self.receipt.is_none() && !self.recover_receipt_with(actor.clone()).await? {
            return Ok(None);
        }
        let staged = self
            .receipt
            .as_ref()
            .context("original disabled staging receipt missing")?;
        let (selected, _) = self
            .owners
            .as_ref()
            .context("original staging owners missing")?;
        let receipt = cancellation::cancel(
            selected.client().source_database(),
            cancellation::OwnerCancelIntent::for_effect_actor(actor, staged, mutation)?,
            self.budget,
        )
        .await?;
        Ok(Some(receipt.revision()))
    }
}
