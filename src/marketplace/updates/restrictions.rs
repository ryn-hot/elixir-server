//! Carry already-received local catalog restrictions into durable installed
//! fences. One cursor and one bounded untrusted record, no peer fetch/source
//! download, installation, opt-in change, history or independent scheduler.
use super::*;
use async_trait::async_trait;
use elixir_marketplace::{
    peer::{PeerOperation, PeerOutcome, RejectionCode},
    security_job::SecurityRecords,
};

#[async_trait]
trait Backend: Send + Sync {
    async fn next(&self, after: Option<&ExtensionId>) -> Result<Option<ExtensionId>>;
    async fn head(&self, id: &ExtensionId) -> Result<Option<SecurityRecords>>;
    async fn needed(&self, records: &SecurityRecords) -> Result<bool>;
    async fn verify_and_retain(&self, records: &SecurityRecords) -> Result<()>;
}

struct Pending {
    extension: ExtensionId,
    records: Option<SecurityRecords>,
}
#[derive(Default)]
pub(super) struct Scan {
    cursor: Option<ExtensionId>,
    pending: Option<Pending>,
    failures: u8,
}
impl Scan {
    fn advance(&mut self) {
        self.cursor = self.pending.take().map(|pending| pending.extension);
        self.failures = 0;
    }
    async fn step(&mut self, backend: &dyn Backend) -> Result<()> {
        if self.pending.is_none() {
            let mut next = backend.next(self.cursor.as_ref()).await?;
            if next.is_none() && self.cursor.is_some() {
                self.cursor = None;
                next = backend.next(None).await?;
            }
            let Some(extension) = next else { return Ok(()) };
            self.pending = Some(Pending {
                extension,
                records: None,
            });
        }
        let pending = self.pending.as_mut().unwrap();
        if pending.records.is_none() {
            pending.records = backend.head(&pending.extension).await?;
            if pending.records.is_none() {
                // Eviction/absence is not revocation, permission or evidence
                // against an offline installed source. Never erase fences.
                self.advance();
                return Ok(());
            }
        }
        let records = pending.records.as_ref().unwrap();
        records.validate_shape()?;
        if records.extension_id != pending.extension {
            bail!("local security observation scope differs");
        }
        if backend.needed(records).await? {
            backend.verify_and_retain(records).await?;
        }
        self.advance();
        Ok(())
    }
    async fn poll(&mut self, backend: &dyn Backend) -> Result<()> {
        let result = self.step(backend).await;
        if result.is_err() {
            self.failures += 1;
            if self.failures == 3 {
                // One bad/busy identity cannot starve the rest. Immutable
                // records are DATA only; every retry needs native authority.
                self.advance();
            }
        }
        result
    }
    pub(super) async fn check(
        &mut self,
        client: crate::marketplace::worker::ParticipationClient,
        database: &Database,
    ) -> Result<()> {
        self.poll(&Live { client, database }).await
    }
}

struct Live<'a> {
    client: crate::marketplace::worker::ParticipationClient,
    database: &'a Database,
}
#[async_trait]
impl Backend for Live<'_> {
    async fn next(&self, after: Option<&ExtensionId>) -> Result<Option<ExtensionId>> {
        next_installed(self.database, after).await
    }
    async fn head(&self, id: &ExtensionId) -> Result<Option<SecurityRecords>> {
        // This is the private LOCAL index operation, not request_peer or a
        // discovery/search job. It cannot add any external peer traffic.
        let response = self
            .client
            .request(PeerOperation::GetHead {
                extension_id: id.clone(),
            })
            .await?;
        match response.response {
            PeerOutcome::Head { state } if state.metadata_only => Ok(Some(SecurityRecords {
                schema_version: 1,
                extension_id: id.clone(),
                publisher: state.publisher,
                head: state.head,
                revocations: state.revocations,
            })),
            PeerOutcome::Rejected {
                code: RejectionCode::Invalid,
            } => Ok(None),
            _ => bail!("local security head response differs"),
        }
    }
    async fn needed(&self, records: &SecurityRecords) -> Result<bool> {
        crate::marketplace::security_state::observation_needed(self.database, records).await
    }
    async fn verify_and_retain(&self, records: &SecurityRecords) -> Result<()> {
        // No boolean/catalog response is authority. This invokes the existing
        // source-free native verifier and ordered durable/mirrored merge; only
        // already-tracked identities can acquire more restrictions.
        self.client.verify_security(records).await?;
        Ok(())
    }
}
async fn next_installed(
    database: &Database,
    after: Option<&ExtensionId>,
) -> Result<Option<ExtensionId>> {
    // Include manual-update, disabled and pending instances: none may opt out
    // of signed restrictions. Grouping uses the bounded installation keyset.
    let next: Option<String> = sqlx::query_scalar(
        "SELECT SUBSTR(m.extension_id,1,129) FROM marketplace_installations m \
        WHERE m.extension_id>$1 AND EXISTS (SELECT 1 FROM marketplace_extension_security s \
        WHERE s.extension_id=m.extension_id) GROUP BY m.extension_id ORDER BY m.extension_id LIMIT 1",
    )
    .bind(after.map(ExtensionId::as_str).unwrap_or(""))
    .fetch_optional(&database.pool)
    .await?;
    next.map(|id| id.parse().map_err(anyhow::Error::from))
        .transpose()
}

#[cfg(test)]
mod tests;
