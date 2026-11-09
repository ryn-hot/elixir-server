//! Original committed pre-claim outputs, never native-stop or launch proof.
//! Retrying a transition yields coordinates only, not another owned receipt.
use super::*;

pub(super) enum PreparationResult {
    Committed(Box<PreCandidateReceipt>),
    Recovery(ActivationReference),
}
impl PreparationResult {
    pub(super) fn into_reference(self) -> ActivationReference {
        match self {
            Self::Committed(receipt) => receipt.reference,
            Self::Recovery(reference) => reference,
        }
    }
    pub(super) fn original(self) -> Result<Box<PreCandidateReceipt>> {
        match self {
            Self::Committed(receipt) => Ok(receipt),
            Self::Recovery(_) => bail!("preparation requires its original committed output"),
        }
    }
}

/// No Clone/Deserialize, conversion from status, or candidate-claim constructor.
/// Only a NEW Quiescing/StateCopied commit returns this. The entire journal is
/// retained by hash; a later transition, even with identical coordinates, is
/// not cleanup authority. It owns no live native candidate.
pub(in crate::marketplace::grant_store::installation) struct PreCandidateReceipt {
    database: Database,
    reference: ActivationReference,
    journal: Digest,
    phase: ActivationPhase,
    budget: InstallBudget,
}
impl PreCandidateReceipt {
    pub(super) fn from_written(
        database: &Database,
        stored: &Stored,
        budget: InstallBudget,
    ) -> Result<Box<Self>> {
        let phase = stored
            .journal
            .activation
            .as_ref()
            .context("preparation fence missing")?
            .phase;
        if !matches!(
            phase,
            ActivationPhase::Quiescing | ActivationPhase::StateCopied
        ) {
            bail!("preparation receipt cannot own an issued candidate claim");
        }
        Ok(Box::new(Self {
            database: database.clone(),
            reference: reference(stored)?,
            journal: Digest::of(canonical::to_vec(&stored.journal)?),
            phase,
            budget,
        }))
    }
    pub(in crate::marketplace::grant_store::installation) fn reference(
        &self,
    ) -> &ActivationReference {
        &self.reference
    }
    pub(in crate::marketplace::grant_store::installation) fn journal(&self) -> Digest {
        self.journal
    }
    pub(in crate::marketplace::grant_store::installation) fn phase(&self) -> ActivationPhase {
        self.phase
    }
    /// One separately authenticated atomic cancellation. No source renewal,
    /// original-session reuse, old-runtime stop, or two-commit resume/delete.
    /// StateCopied cleanup also verifies the original ciphertext/secret seals;
    /// native candidate code has never received a claim for this exact record.
    pub(in crate::marketplace::grant_store::installation) async fn cancel(
        &self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        cancellation::cancel(
            &self.database,
            cancellation::OwnerCancelIntent::pre_candidate(principal, self, mutation)?,
            self.budget,
        )
        .await
    }
    pub(in crate::marketplace::grant_store::installation) async fn cancel_automatic(
        &self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        cancellation::cancel(
            &self.database,
            cancellation::OwnerCancelIntent::pre_candidate_with(
                automatic::EffectActor::recovery(authorization),
                self,
                mutation,
            )?,
            self.budget,
        )
        .await
    }
}
