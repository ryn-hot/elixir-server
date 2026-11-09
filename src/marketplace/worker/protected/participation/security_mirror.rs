//! One ordered mirror per actual protected owner. A peer cannot construct this
//! capability through JSON/status/settings. No company service participates.
use super::*;
use elixir_marketplace::{identity::Digest, worker_control::WorkerControlOutcome};

pub(super) struct Mirror {
    generation: Digest,
    pub(super) revision: tokio::sync::Mutex<u64>,
}
impl Mirror {
    #[cfg(test)]
    pub(in crate::marketplace::worker::protected::participation) fn fixture(
        generation: Digest,
        revision: u64,
    ) -> Self {
        Self {
            generation,
            revision: tokio::sync::Mutex::new(revision),
        }
    }
    pub(super) async fn current(&self, database: &Database) -> Result<bool> {
        let Ok(revision) = self.revision.try_lock() else {
            return Ok(false);
        };
        let row = timeout(
            Duration::from_secs(5),
            sqlx::query(
                "SELECT revision,blocked FROM marketplace_security_budget WHERE singleton=1",
            )
            .fetch_one(&database.pool),
        )
        .await
        .context("catalog security revision check deadline")??;
        use sqlx::Row;
        if row.try_get::<i64, _>("blocked")? != 0
            || row.try_get::<i64, _>("revision")? != *revision as i64
        {
            bail!("catalog security mirror requires a fresh baseline");
        }
        Ok(true)
    }
    pub(super) async fn initialize(
        worker: &mut ProtectedWorker,
        database: &Database,
    ) -> Result<Self> {
        if !worker.catalog_security_supported() {
            bail!("worker image lacks catalog security reconciliation");
        }
        let baseline = crate::marketplace::security_state::catalog_baseline(database).await?;
        let generation = Digest::of(uuid::Uuid::new_v4().as_bytes());
        let revision = baseline.revision();
        acknowledge(
            worker.catalog_security(baseline.begin(generation)).await?,
            generation,
            revision,
            false,
        )?;
        for batch in baseline.batches() {
            acknowledge(
                worker
                    .catalog_security(WorkerControlOperation::CatalogSecurityApply {
                        generation,
                        batch,
                    })
                    .await?,
                generation,
                revision,
                false,
            )?;
        }
        baseline.require_current(database).await?;
        acknowledge(
            worker.catalog_security(baseline.finish(generation)).await?,
            generation,
            revision,
            true,
        )?;
        baseline.require_current(database).await?;
        worker.supervisor.require_active()?;
        // A worker ACK alone is not this parent-side capability: both durable
        // revision checks and the actual owned protection are mandatory.
        worker.catalog_security_ready = true;
        Ok(Self {
            generation,
            revision: tokio::sync::Mutex::new(revision),
        })
    }
    pub(super) async fn apply(
        &self,
        client: &ParticipationClient,
        mutation: crate::marketplace::security_state::CatalogMutation,
        revision: &mut u64,
    ) -> Result<()> {
        let next = mutation.revision();
        let tracked = mutation.tracked();
        if next < *revision || next > *revision + 1 {
            bail!("catalog security mirror revision gap");
        }
        if let Some(batch) = mutation.into_batch() {
            if !tracked {
                bail!("untracked catalog security delta");
            }
            if next != *revision + 1 {
                bail!("catalog security mirror delta revision differs");
            }
            let (reply, received) = oneshot::channel();
            let response = client
                .enqueue_after_capacity(
                    Request::Security(
                        WorkerControlOperation::CatalogSecurityApply {
                            generation: self.generation,
                            batch,
                        },
                        reply,
                    ),
                    received,
                )
                .await?;
            acknowledge(response, self.generation, next, true)?;
        } else if next != *revision {
            bail!("catalog security mirror change lacks a delta");
        }
        *revision = next;
        Ok(())
    }
}
fn acknowledge(
    response: WorkerControlResponse,
    generation: Digest,
    revision: u64,
    ready: bool,
) -> Result<()> {
    if !matches!(response.control_response,WorkerControlOutcome::CatalogSecurity { generation:observed,revision:version,ready:status }
        if observed==generation && version==revision && status==ready)
    {
        bail!("catalog security mirror was not acknowledged");
    }
    Ok(())
}
