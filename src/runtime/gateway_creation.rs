//! Original physical create outcome. An error, cancelled waiter, elapsed time
//! or missing name is never a "not attempted" result.
use crate::marketplace::application_egress::{ApplicationGatewayProfile, GatewayCreateAttempt};
use crate::runtime::model::ContainerHandle;
use anyhow::Result;

pub enum GatewayCreation {
    Created(ContainerHandle),
    NotAttempted(GatewayNotAttempted),
}
pub struct GatewayNotAttempted {
    attempt: GatewayCreateAttempt,
    reason: anyhow::Error,
}
impl GatewayCreation {
    pub(super) fn not_attempted(attempt: GatewayCreateAttempt, reason: anyhow::Error) -> Self {
        Self::NotAttempted(GatewayNotAttempted { attempt, reason })
    }
}
impl GatewayNotAttempted {
    pub(crate) fn require(
        &self,
        operation: uuid::Uuid,
        profile: &ApplicationGatewayProfile,
    ) -> Result<()> {
        self.attempt.require(operation, profile)
    }
    pub(crate) fn into_error(self) -> anyhow::Error {
        self.reason
    }
}
