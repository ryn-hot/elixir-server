//! Original post-probe context, after positive native disposal. No new native
//! allowance or capability reconstructed from a journal/status observation.
use super::*;
use crate::marketplace::grant_store::installation::runtime::InstalledController;

/// Not Clone/Deserialize. The original encrypted credential intent stays with
/// the exact selected archive/plan; callers never resubmit secrets per phase.
/// This owns no live native workspace and is not readiness/activation proof.
pub(in crate::marketplace) struct PreparedSource {
    selected: Selection,
    archive: ArchivedSource,
    intent: Arc<OwnerActivationIntent>,
    plan: SourceActivationPlan,
    budget: InstallBudget,
    reference: ActivationReference,
    preparation: Preparation,
}

enum Preparation {
    NotStarted,
    // Owned before the first await. Only original acknowledged outputs replace
    // this slot; later failures cannot throw them away or refresh their stamp.
    Continuing(Option<Box<pre_candidate::PreCandidateReceipt>>),
    HandedOff,
}

impl PreparedSource {
    // Only the parent can call this, after its actual probe/disposal/SQL commit.
    pub(super) fn from_disposed(source: ActivationSource, reference: ActivationReference) -> Self {
        Self {
            selected: source.selected,
            archive: source.archive,
            intent: source.check.intent,
            plan: source.plan,
            budget: source.budget,
            reference,
            preparation: Preparation::NotStarted,
        }
    }
    pub(in crate::marketplace) fn reference(&self) -> &ActivationReference {
        &self.reference
    }
    pub(in crate::marketplace::grant_store::installation) fn database(&self) -> &Database {
        self.selected.client().source_database()
    }
    pub(in crate::marketplace::grant_store::installation) fn home(&self) -> Uuid {
        self.intent.actor.home
    }
    /// Independently authorized cleanup, with no network/source/grant renewal.
    /// Borrow through acknowledgement so a cancelled waiter can retry exactly.
    pub(in crate::marketplace) async fn cancel(
        &self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        match &self.preparation {
            Preparation::Continuing(Some(receipt)) => {
                return receipt.cancel(principal, mutation).await;
            }
            Preparation::HandedOff => {
                bail!("candidate claim was handed off; use its original owner for cleanup");
            }
            Preparation::NotStarted | Preparation::Continuing(None) => {}
        }
        cancellation::cancel(
            self.selected.client().source_database(),
            cancellation::OwnerCancelIntent::source_prepared(principal, self, mutation)?,
            self.budget,
        )
        .await
    }
    pub(in crate::marketplace) async fn cancel_automatic(
        &self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        match &self.preparation {
            Preparation::Continuing(Some(receipt)) => {
                return receipt.cancel_automatic(authorization, mutation).await;
            }
            Preparation::HandedOff => {
                bail!("candidate claim was handed off; use its original owner for cleanup");
            }
            Preparation::NotStarted | Preparation::Continuing(None) => {}
        }
        cancellation::cancel(
            self.selected.client().source_database(),
            cancellation::OwnerCancelIntent::source_prepared_with(
                automatic::EffectActor::recovery(authorization),
                self,
                mutation,
            )?,
            self.budget,
        )
        .await
    }
    async fn fresh(&mut self) -> Result<PublicSourceObservation> {
        self.selected.maintain().await?;
        self.selected.public_source()
    }
    fn step<'a>(
        &'a self,
        database: &'a Database,
        public: &'a PublicSourceObservation,
    ) -> Result<Step<'a>> {
        Ok(Step {
            database,
            metadata: self.selected.metadata()?,
            public,
            material: Material::from_archive(&self.archive, self.selected.metadata()?)?,
            intent: self.intent.clone(),
            plan: &self.plan,
            budget: self.budget,
        })
    }
    /// Continue only the original attempt. Every transition reauthenticates;
    /// the actual controller joins this instance's old call before copying.
    /// Known committed outputs remain owned through later failures. An unknown
    /// commit still refuses cleanup unless the exact preceding record remains.
    /// This continuation is one-use; retrying never recreates claims or copies.
    pub(in crate::marketplace) async fn claim_candidate(
        &mut self,
        controller: &mut InstalledController,
    ) -> Result<candidate::CandidateOrRecovery> {
        if self.reference.configured_instance.is_none() {
            bail!("candidate continuation requires an explicit configured owner");
        }
        if !matches!(self.preparation, Preparation::NotStarted) {
            bail!("candidate continuation already owned; cleanup or original recovery required");
        }
        self.preparation = Preparation::Continuing(None);
        let public = self.fresh().await?;
        let quiescing = quiescence::quiesce_retained(
            self.step(self.selected.client().source_database(), &public)?,
            &self.reference,
        )
        .await?
        .original()?;
        self.preparation = Preparation::Continuing(Some(quiescing));
        let public = self.fresh().await?;
        let scope = quiescence::observe_checked(
            self.step(self.selected.client().source_database(), &public)?,
            self.preparation_reference()?,
        )
        .await?;
        let guard = controller.quiesce(scope).await?;
        let public = self.fresh().await?;
        let copied = state_copy::copy_retained(self.step(guard.database(), &public)?, &guard)
            .await?
            .original()?;
        self.preparation = Preparation::Continuing(Some(copied));
        let public = self.fresh().await?;
        let result = candidate::claim_checked(
            self.step(guard.database(), &public)?,
            &guard,
            self.preparation_reference()?,
        )
        .await;
        if result.is_ok() {
            // A claim OR recovery result cannot retain a pre-claim cleanup
            // path. Never turn observed CandidateStarting into an owned claim.
            self.preparation = Preparation::HandedOff;
        }
        result
    }
    fn preparation_reference(&self) -> Result<&ActivationReference> {
        match &self.preparation {
            Preparation::Continuing(Some(receipt)) => Ok(receipt.reference()),
            _ => bail!("original preparation output missing"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_context_and_continuation_keep_bounded_stack_ownership() {
        fn future_size<F: std::future::Future>(
            _: impl FnOnce(&'static mut PreparedSource, &'static mut InstalledController) -> F,
        ) -> usize {
            std::mem::size_of::<F>()
        }
        // Type measurement only: no fabricated source/probe/runtime objects.
        let bytes = future_size(|prepared, controller| prepared.claim_candidate(controller));
        assert!(std::mem::size_of::<PreparedSource>() <= 16 * 1024);
        assert!(
            bytes <= 128 * 1024,
            "continuation future exceeds bound: {bytes}"
        );
    }
}
