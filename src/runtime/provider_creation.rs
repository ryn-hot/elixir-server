//! Only the original create invocation can prove it never submitted CREATE.
//! Exceptions/cancellation/name absence are not such proof.
use crate::{
    marketplace::provider::{CreateAttempt, Profile},
    runtime::model::ContainerHandle,
};
use anyhow::Result;
pub enum WorkerCreation {
    Created(ContainerHandle),
    NotAttempted(WorkerNotAttempted),
}
pub struct WorkerNotAttempted {
    attempt: crate::marketplace::provider::WorkerCreateAttempt,
    reason: anyhow::Error,
}
impl WorkerCreation {
    pub(super) fn not_attempted(
        attempt: crate::marketplace::provider::WorkerCreateAttempt,
        reason: anyhow::Error,
    ) -> Self {
        Self::NotAttempted(WorkerNotAttempted { attempt, reason })
    }
}
impl WorkerNotAttempted {
    pub(crate) fn require(
        &self,
        operation: uuid::Uuid,
        profile: &crate::marketplace::provider::WorkerProfile,
    ) -> Result<()> {
        self.attempt.require(operation, profile)
    }
    pub(crate) fn into_error(self) -> anyhow::Error {
        self.reason
    }
}
pub enum GuardCreation {
    Created(ContainerHandle),
    NotAttempted(GuardNotAttempted),
}
pub struct GuardNotAttempted {
    attempt: crate::marketplace::provider::GuardCreateAttempt,
    reason: anyhow::Error,
}
impl GuardCreation {
    pub(super) fn not_attempted(
        attempt: crate::marketplace::provider::GuardCreateAttempt,
        reason: anyhow::Error,
    ) -> Self {
        Self::NotAttempted(GuardNotAttempted { attempt, reason })
    }
}
impl GuardNotAttempted {
    pub(crate) fn require(
        &self,
        operation: uuid::Uuid,
        profile: &crate::marketplace::provider::GuardProfile,
    ) -> Result<()> {
        self.attempt.require(operation, profile)
    }
    pub(crate) fn into_error(self) -> anyhow::Error {
        self.reason
    }
}
pub enum ProviderCreation {
    Created(ContainerHandle),
    NotAttempted(NotAttempted),
}
pub struct NotAttempted {
    attempt: CreateAttempt,
    reason: anyhow::Error,
}
impl ProviderCreation {
    pub(super) fn not_attempted(attempt: CreateAttempt, reason: anyhow::Error) -> Self {
        Self::NotAttempted(NotAttempted { attempt, reason })
    }
}
impl NotAttempted {
    pub(crate) fn require(&self, operation: uuid::Uuid, profile: &Profile) -> Result<()> {
        self.attempt.require(operation, profile)
    }
    pub(crate) fn into_error(self) -> anyhow::Error {
        self.reason
    }
}
