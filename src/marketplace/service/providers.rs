//! Installed provider DATA and a restricted owned-call handoff. No public raw
//! invocation route, legacy endpoint fallback or readiness-bit authorization.
use super::*;
use crate::{
    db::Database,
    marketplace::grant_store::installation::{providers, standing::InstalledBinding},
};
pub(crate) use providers::Descriptor as SourceProviderDescriptor;
pub(crate) use providers::SOURCE_ENDPOINT as SOURCE_PROVIDER_ENDPOINT;

impl MarketplaceService {
    #[cfg(test)]
    pub(crate) fn fixture_source_call_slots_used(&self) -> usize {
        self.source_calls.slots_used()
    }
    pub(crate) async fn source_providers(
        &self,
        database: &Database,
        capability: &'static str,
    ) -> Result<Vec<SourceProviderDescriptor>> {
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("source allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone());
        let Some(allocation) = allocation else {
            return Ok(vec![]);
        };
        let _permit = self
            .source_provider_reads
            .clone()
            .try_acquire_owned()
            .context("source provider metadata busy")?;
        let mut providers = Vec::new();
        for coordinates in providers::candidates(database, capability, None).await? {
            match InstalledBinding::load_provider(
                database,
                &coordinates,
                allocation.installations,
                None,
            )
            .await
            {
                Ok((_, descriptor)) => providers.push(descriptor),
                Err(_) => {
                    tracing::warn!(provider_id=%coordinates.provider,"omitting unavailable installed source provider")
                }
            }
        }
        Ok(providers)
    }
    pub(crate) async fn source_provider(
        &self,
        database: &Database,
        capability: &'static str,
        id: uuid::Uuid,
    ) -> Result<Option<SourceProviderDescriptor>> {
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("source allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone());
        let Some(allocation) = allocation else {
            return Ok(None);
        };
        let _permit = self
            .source_provider_reads
            .clone()
            .try_acquire_owned()
            .context("source provider metadata busy")?;
        let candidates = providers::candidates(database, capability, Some(id)).await?;
        let Some(coordinates) = candidates.first() else {
            return Ok(None);
        };
        let (_, descriptor) =
            InstalledBinding::load_provider(database, coordinates, allocation.installations, None)
                .await?;
        Ok(Some(descriptor))
    }
    /// Scheduling only. Caller deadlines/cancellation include this wait, and
    /// caller membership must be checked again afterwards, before starting code.
    pub(crate) async fn source_provider_turn(
        &self,
    ) -> Result<super::super::application::InstalledCallTurn> {
        self.source_calls.acquire().await
    }
    pub(crate) fn begin_source_provider_call(
        &self,
        descriptor: &SourceProviderDescriptor,
        arguments: elixir_marketplace::source_execution::RuntimeJson,
        turn: super::super::application::InstalledCallTurn,
    ) -> Result<InstalledCall> {
        self.source_calls.require(&turn)?;
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("source allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone())
            .context("source execution not configured by trusted startup")?;
        self.installation.begin_provider_call(
            descriptor,
            arguments,
            allocation.installations,
            allocation.execution,
            self.client()
                .ok()
                .and_then(|client| client.installed_http()),
            turn,
        )
    }
}
