//! Explicit owner submission of root-signed restrictions, not installation,
//! paid enrollment or proof that any remote peer received the notice.
use super::{
    preferences,
    service::MarketplaceService,
    worker::{ParticipationClient, SecurityObservation},
};
use crate::{db::Database, http::auth::CurrentPrincipal};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use elixir_marketplace::{
    MAX_CONTROL_FRAME_BYTES,
    admission::CandidateDescriptor,
    canonical,
    identity::{Digest, ExtensionId},
    peer::{PeerOperation, PeerOutcome, PeerResponse},
    security_job::SecurityRecords,
};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_REQUEST_BYTES: usize = MAX_CONTROL_FRAME_BYTES;
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    schema_version: u32,
    descriptor: Box<CandidateDescriptor>,
    accepted_descriptor_id: Digest,
    revocation_id: Digest,
    accept_permanent_withdrawal: bool,
}
impl Request {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = canonical::decode(bytes, MAX_REQUEST_BYTES)?;
        let descriptor = &value.descriptor;
        if value.schema_version != 1
            || !value.accept_permanent_withdrawal
            || Digest::of(descriptor.to_bytes()?) != value.accepted_descriptor_id
        {
            bail!("explicit acceptance of the exact withdrawal descriptor is required");
        }
        let notice = descriptor
            .revocations
            .iter()
            .find(|record| record.to_bytes().map(Digest::of).ok() == Some(value.revocation_id))
            .context("selected withdrawal record is missing")?;
        let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
        if notice.payload.extension_id != descriptor.release.payload.extension_id
            || (!notice.payload.release_ids.contains(&release)
                && !notice
                    .payload
                    .release_keys
                    .contains(&descriptor.release.signer_key)
                && !notice
                    .payload
                    .release_keys
                    .contains(&descriptor.head.signer_key))
        {
            bail!("withdrawal must restrict the supplied exact release or signing key");
        }
        Ok(value)
    }
    fn records(&self) -> SecurityRecords {
        SecurityRecords {
            schema_version: 1,
            extension_id: self.descriptor.release.payload.extension_id.clone(),
            publisher: self.descriptor.publisher.clone(),
            head: None,
            revocations: self.descriptor.revocations.clone(),
        }
    }
}
#[derive(Serialize)]
pub(crate) struct View {
    schema_version: u32,
    descriptor_id: Digest,
    extension_id: ExtensionId,
    revocation_id: Digest,
    signatures_verified: bool,
    security_tracked: bool,
    security_applied: bool,
    local_catalog_retained: bool,
    peer_delivery_confirmed: bool,
    metadata_only: bool,
}
// Private test seam; only a real protected participation client is used by the
// service. No HTTP/peer JSON can supply receipts or verification booleans.
#[async_trait]
trait Backend: Send + Sync {
    async fn authorize(&self) -> Result<()>;
    async fn authenticate(&self, records: &SecurityRecords) -> Result<SecurityObservation>;
    async fn retain(&self, observation: &SecurityObservation) -> Result<bool>;
    async fn request(&self, operation: PeerOperation) -> Result<PeerResponse>;
    fn fail_worker(&self);
}
struct Live<'a> {
    service: &'a MarketplaceService,
    database: &'a Database,
    principal: &'a CurrentPrincipal,
    client: ParticipationClient,
}
#[async_trait]
impl Backend for Live<'_> {
    async fn authorize(&self) -> Result<()> {
        if self.service.stop_requested() || !self.client.same_owner(&self.service.client()?) {
            bail!("withdrawal protected owner changed");
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            preferences::load(self.database, self.principal, chrono::Utc::now()),
        )
        .await
        .context("withdrawal owner authorization deadline")??;
        if self.service.stop_requested() || !self.client.same_owner(&self.service.client()?) {
            bail!("withdrawal protected owner changed during authorization");
        }
        Ok(())
    }
    async fn authenticate(&self, records: &SecurityRecords) -> Result<SecurityObservation> {
        self.client.authenticate_owner_security(records).await
    }
    async fn retain(&self, observation: &SecurityObservation) -> Result<bool> {
        self.client.retain_security_observation(observation).await
    }
    async fn request(&self, operation: PeerOperation) -> Result<PeerResponse> {
        self.client.request(operation).await
    }
    fn fail_worker(&self) {
        self.client.invalidate_connection();
    }
}
async fn submit(backend: &dyn Backend, request: Request) -> Result<View> {
    backend.authorize().await?;
    let records = request.records();
    let observation = backend.authenticate(&records).await?;
    if !observation.is_current()
        || canonical::to_vec(observation.records())? != canonical::to_vec(&records)?
    {
        bail!("withdrawal verification receipt differs or expired");
    }
    backend.authorize().await?;
    observation.summary()?;
    let tracked = backend.retain(&observation).await?;
    let mut view = View {
        schema_version: 1,
        descriptor_id: request.accepted_descriptor_id,
        extension_id: records.extension_id.clone(),
        revocation_id: request.revocation_id,
        signatures_verified: true,
        security_tracked: tracked,
        security_applied: tracked,
        local_catalog_retained: false,
        peer_delivery_confirmed: false,
        metadata_only: true,
    };
    if tracked {
        backend.authorize().await?;
        // Root restrictions are already durable/mirrored before offering their
        // signed proof. The selected notice guarantees this code cannot qualify.
        let response = backend
            .request(PeerOperation::Offer {
                descriptor: request.descriptor,
            })
            .await?;
        if !matches!(response.response, PeerOutcome::Rejected { .. }) {
            backend.fail_worker();
            bail!("withdrawn code was unexpectedly admitted");
        }
        let head = backend
            .request(PeerOperation::GetHead {
                extension_id: records.extension_id.clone(),
            })
            .await?;
        match head.response {
            PeerOutcome::Head { state }
                if state.metadata_only
                    && state.publisher.payload.root_key == records.publisher.payload.root_key =>
            {
                view.local_catalog_retained = state.revocations.iter().any(|record| {
                    record.to_bytes().map(Digest::of).ok() == Some(request.revocation_id)
                });
            }
            PeerOutcome::Rejected { .. } => {} // e.g. a previously evicted local entry
            _ => {
                backend.fail_worker();
                bail!("withdrawal catalog readback differs");
            }
        }
    }
    backend.authorize().await?;
    Ok(view)
}
impl MarketplaceService {
    pub(crate) async fn submit_withdrawal(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        request: Request,
    ) -> Result<View> {
        // Share the existing nonqueued owner-import slot. No new actor/history,
        // retry queue, enrollment, delivery promise or larger resource budget.
        let _permit = self
            .imports
            .clone()
            .try_acquire_owned()
            .context("owner import is busy")?;
        let backend = Live {
            service: self,
            database,
            principal,
            client: self.client()?,
        };
        submit(&backend, request).await
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) fn fixture_request() -> serde_json::Value {
    tests::input()
}
