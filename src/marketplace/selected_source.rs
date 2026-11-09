//! Source enrollment is either gated discovery or an explicit owner's direct
//! acceptance. Neither path can manufacture discovery evidence. No public DTO
//! implements this sealed interface or becomes an installation capability.
use super::{
    grant_store::source_authorization::SourceAuthorization,
    quarantine::{SourceJobBudget, SourceQuarantinePlan},
    worker::{MetadataObservation, PublicationObservation},
};
use crate::{
    db::{Database, DatabaseDriver},
    http::auth::CurrentPrincipal,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use elixir_marketplace::{identity::Digest, records::Release, retained_authority::AuthorityRecord};
use sqlx::{Any, Transaction};
use std::time::Duration;
use uuid::Uuid;

mod sealed {
    pub trait Sealed {}
}

#[async_trait]
pub(in crate::marketplace) trait SelectedSource:
    sealed::Sealed + Send + Sync
{
    fn discovery(&self) -> Option<&MetadataObservation> {
        None
    }
    fn source_authorization(&self) -> Option<SourceAuthorization> {
        None
    }
    fn valid_for(&self) -> Duration;
    fn release_payload(&self) -> &Release;
    fn authority_record(&self) -> Result<AuthorityRecord>;
    fn plan(
        &self,
        artifact: Option<usize>,
        budget: &SourceJobBudget,
    ) -> Result<SourceQuarantinePlan>;
    fn require_actor(&self, _user: Uuid, _home: Uuid) -> Result<()> {
        Ok(())
    }
    async fn require_current(&self, database: &Database) -> Result<()>;
    async fn require_in_transaction(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()>;
}
impl sealed::Sealed for MetadataObservation {}
#[async_trait]
impl SelectedSource for MetadataObservation {
    fn discovery(&self) -> Option<&MetadataObservation> {
        Some(self)
    }
    fn valid_for(&self) -> Duration {
        self.valid_for()
    }
    fn release_payload(&self) -> &Release {
        &self.descriptor().release.payload
    }
    fn authority_record(&self) -> Result<AuthorityRecord> {
        self.support()?;
        Ok(AuthorityRecord::from_descriptor(self.descriptor())?)
    }
    fn plan(
        &self,
        artifact: Option<usize>,
        budget: &SourceJobBudget,
    ) -> Result<SourceQuarantinePlan> {
        SourceQuarantinePlan::for_metadata(self, artifact, budget)
    }
    async fn require_current(&self, database: &Database) -> Result<()> {
        super::security_state::require_current(database, self).await
    }
    async fn require_in_transaction(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        super::security_state::retained::require_current_in_transaction(tx, driver, self).await
    }
}

/// One exact owner/session + signed reference. Not Clone/Deserialize, paid
/// adoption, a grant, permission to auto-share, or a generic preview upgrade.
pub(in crate::marketplace) struct AcceptedPublication {
    observation: PublicationObservation,
    authorization: SourceAuthorization,
}
impl AcceptedPublication {
    pub(in crate::marketplace) async fn capture(
        database: &Database,
        observation: PublicationObservation,
        principal: &CurrentPrincipal,
        accepted_reference: Digest,
    ) -> Result<Self> {
        if observation.summary()?.reference_id != accepted_reference {
            bail!("owner acceptance does not match the exact signed reference");
        }
        let value = Self {
            observation,
            authorization: SourceAuthorization::capture(database, principal).await?,
        };
        value.check()?;
        Ok(value)
    }
    pub(in crate::marketplace) fn observation(&self) -> &PublicationObservation {
        &self.observation
    }
    pub(in crate::marketplace) fn check(&self) -> Result<()> {
        if self.authorization.check().is_err() {
            return Err(super::security_state::Rejection::OwnerAuthorization.into());
        }
        self.observation
            .summary()
            .map(|_| ())
            .map_err(|_| super::security_state::Rejection::Expired.into())
    }
    pub(in crate::marketplace) fn renew(
        &self,
        observation: PublicationObservation,
    ) -> Result<Self> {
        if observation.summary()?.reference_id
            != Digest::of(self.observation.publication().to_bytes()?)
        {
            bail!("direct source refresh changed the accepted reference");
        }
        let value = Self {
            observation,
            authorization: self.authorization.clone(),
        };
        value.check()?;
        Ok(value)
    }
    pub(in crate::marketplace) async fn authenticate(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<chrono::DateTime<Utc>> {
        self.check()?;
        let expires = self
            .authorization
            .authenticate(tx, Utc::now())
            .await
            .map_err(|_| super::security_state::Rejection::OwnerAuthorization)?;
        super::preferences::require_home_binding(tx, self.authorization.coordinates().1, driver)
            .await?;
        Ok(expires)
    }
    pub(in crate::marketplace) fn before_commit(
        &self,
        expires: chrono::DateTime<Utc>,
    ) -> Result<()> {
        self.check()?;
        if Utc::now() >= expires {
            return Err(super::security_state::Rejection::OwnerAuthorization.into());
        }
        Ok(())
    }
}
impl sealed::Sealed for AcceptedPublication {}
#[async_trait]
impl SelectedSource for AcceptedPublication {
    fn source_authorization(&self) -> Option<SourceAuthorization> {
        Some(self.authorization.clone())
    }
    fn valid_for(&self) -> Duration {
        self.observation
            .valid_for()
            .min(self.authorization.valid_for())
    }
    fn release_payload(&self) -> &Release {
        &self.observation.publication().release.payload
    }
    fn authority_record(&self) -> Result<AuthorityRecord> {
        self.check()?;
        Ok(AuthorityRecord::from_publication(
            self.observation.publication(),
        )?)
    }
    fn plan(
        &self,
        artifact: Option<usize>,
        budget: &SourceJobBudget,
    ) -> Result<SourceQuarantinePlan> {
        self.check()?;
        SourceQuarantinePlan::for_publication(&self.observation, artifact, budget)
    }
    fn require_actor(&self, user: Uuid, home: Uuid) -> Result<()> {
        if (user, home) != self.authorization.coordinates() {
            bail!("direct source belongs to its accepting owner and home");
        }
        self.check()
    }
    async fn require_current(&self, database: &Database) -> Result<()> {
        let mut connection = super::durability::connection(database).await?;
        let mut tx = sqlx::Acquire::begin(&mut connection).await?;
        self.require_in_transaction(&mut tx, database.driver)
            .await?;
        tx.rollback().await?;
        Ok(())
    }
    async fn require_in_transaction(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<()> {
        let expires = self.authenticate(tx, driver).await?;
        super::security_state::retained::require_publication_in_transaction(
            tx,
            driver,
            &self.observation,
        )
        .await
        .context("accepted direct source is no longer current")?;
        self.before_commit(expires)
    }
}

#[cfg(test)]
mod tests;
