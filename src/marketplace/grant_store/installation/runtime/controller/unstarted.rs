//! Permanently consumed original candidate claim that never entered a task.
//! This is NOT native retirement, readiness, or a replacement launch claim.
use super::*;
use crate::marketplace::grant_store::installation::activation::{
    ActivationReference,
    candidate::{CandidateBinding, CandidateClaim},
};

pub(in crate::marketplace) struct UnstartedCandidate {
    claim: CandidateClaim,
    owner: Arc<RuntimeOwner>,
    database: Database,
    secrets: SecretsManager,
}
impl UnstartedCandidate {
    // Controller-only construction: the native task module cannot turn a
    // claim it already received into a never-launched cleanup proof.
    pub(super) fn new(
        claim: CandidateClaim,
        owner: Arc<RuntimeOwner>,
        database: Database,
        secrets: SecretsManager,
    ) -> Self {
        Self {
            claim,
            owner,
            database,
            secrets,
        }
    }
    pub(in crate::marketplace::grant_store::installation) fn reference(
        &self,
    ) -> &ActivationReference {
        self.claim.binding().reference()
    }
    pub(in crate::marketplace::grant_store::installation) fn secrets(&self) -> &SecretsManager {
        &self.secrets
    }
    pub(in crate::marketplace::grant_store::installation) async fn require_owner(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        self.owner.require(tx, driver).await
    }
    /// Fresh owner action, borrowed through acknowledgement and exact retry.
    /// No source availability or renewal of the original browser session.
    pub(in crate::marketplace) async fn cancel(
        &self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        activation::retirement::cancel_unstarted(self, principal, mutation).await
    }
    pub(in crate::marketplace) async fn cancel_automatic(
        &self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        activation::retirement::cancel_unstarted_automatic(self, authorization, mutation).await
    }
}

/// Borrowed cancellation restriction only. The two original owned proofs stay
/// distinct; this view cannot enter execution, activation, or claim extraction.
pub(in crate::marketplace::grant_store::installation) enum ClosedCandidate<'a> {
    Retired(&'a RetiredCandidate),
    Unstarted(&'a UnstartedCandidate),
}
impl ClosedCandidate<'_> {
    pub(in crate::marketplace::grant_store::installation) fn binding(&self) -> &CandidateBinding {
        match self {
            Self::Retired(retired) => retired.binding(),
            Self::Unstarted(unstarted) => unstarted.claim.binding(),
        }
    }
    pub(in crate::marketplace::grant_store::installation) fn database(&self) -> &Database {
        match self {
            Self::Retired(retired) => retired.database(),
            Self::Unstarted(unstarted) => &unstarted.database,
        }
    }
    pub(in crate::marketplace::grant_store::installation) fn capsule(&self) -> &Capsule {
        match self {
            Self::Retired(retired) => retired.capsule(),
            Self::Unstarted(unstarted) => unstarted.claim.capsule(),
        }
    }
    pub(in crate::marketplace::grant_store::installation) async fn require_owner(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        match self {
            Self::Retired(retired) => retired.require_owner(tx, driver).await,
            Self::Unstarted(unstarted) => unstarted.require_owner(tx, driver).await,
        }
    }
}
