//! A result of the original physical create call, never an error-string or
//! elapsed-time inference. Rejection carries its consumed, single-use attempt.
use crate::marketplace::runtime::application_http::{ApplicationHttpProfile, HttpCreateAttempt};
use crate::runtime::model::ContainerHandle;
use anyhow::Result;

pub enum HttpCreation {
    Created(ContainerHandle),
    NotAttempted(HttpNotAttempted),
}
/// No Clone, Deserialize, Default or public constructor. Only a runtime backend
/// that still owns the original attempt may return this before issuing CREATE.
pub struct HttpNotAttempted {
    attempt: HttpCreateAttempt,
    reason: anyhow::Error,
}
impl HttpCreation {
    pub(super) fn not_attempted(attempt: HttpCreateAttempt, reason: anyhow::Error) -> Self {
        Self::NotAttempted(HttpNotAttempted { attempt, reason })
    }
}
impl HttpNotAttempted {
    pub(crate) fn require(
        &self,
        operation: uuid::Uuid,
        profile: &ApplicationHttpProfile,
    ) -> Result<()> {
        self.attempt.require(operation, profile)
    }
    pub(crate) fn into_error(self) -> anyhow::Error {
        self.reason
    }
}
