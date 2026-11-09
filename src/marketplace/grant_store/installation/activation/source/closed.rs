//! Original one-use source task after positively joined disposal. No constructor
//! from status/reference data and no conversion to a native start or probe.
use super::*;

pub(in crate::marketplace) struct DisposedSource {
    database: Database,
    check: NativeStartCheck,
    budget: InstallBudget,
}
impl DisposedSource {
    /// SQL-only fixture. This does NOT qualify native disposal; it consumes a
    /// real original SQL start but deliberately substitutes its native boundary.
    #[cfg(test)]
    pub(in crate::marketplace::grant_store::installation) fn fixture(
        database: Database,
        start: ActivationStart,
        budget: InstallBudget,
    ) -> Self {
        Self::new(
            database,
            start.native_check(),
            budget,
            Disposal { _private: () },
        )
    }
    pub(super) fn new(
        database: Database,
        check: NativeStartCheck,
        budget: InstallBudget,
        _disposal: Disposal,
    ) -> Self {
        Self {
            database,
            check,
            budget,
        }
    }
    pub(in crate::marketplace::grant_store::installation) fn reference(
        &self,
    ) -> &ActivationReference {
        &self.check.reference
    }
    pub(in crate::marketplace::grant_store::installation) fn starting_journal(&self) -> Digest {
        self.check.starting_journal
    }
    pub(in crate::marketplace) async fn cancel(
        &self,
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        cancellation::cancel(
            &self.database,
            cancellation::OwnerCancelIntent::disposed_source(principal, self, mutation)?,
            self.budget,
        )
        .await
    }
    pub(in crate::marketplace) async fn cancel_automatic(
        &self,
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<cancellation::CancelledInstallation> {
        cancellation::cancel(
            &self.database,
            cancellation::OwnerCancelIntent::disposed_source_with(
                automatic::EffectActor::recovery(authorization),
                self,
                mutation,
            )?,
            self.budget,
        )
        .await
    }
}

// Created by the source module only after its actual owned disposal call joins.
// Keeping the receipt across a later SQL error is different from inspecting an
// empty stage, a missing container name or a free OS lock.
pub(super) struct Disposal {
    pub(super) _private: (),
}

#[cfg(test)]
mod tests;
