//! Retained archive -> native quarantine, not activation or execution. The
//! already admitted job, blocking open and current reader stay in this owner
//! across cancelled awaits. No peer/HTTP constructor or new resource allowance.
use super::*;
use crate::marketplace::archive::{ArchiveBudget, Reader, Replay};
use std::path::PathBuf;
use tokio::task::JoinHandle;

pub(in crate::marketplace) struct Restoration {
    job: Option<SourceQuarantineJob>,
    path: PathBuf,
    budget: ArchiveBudget,
    initialized: bool,
    opening: Option<JoinHandle<Result<Replay>>>,
    replay: Option<Replay>,
    reader: Option<Reader>,
    purposes: Vec<QuarantinePurpose>,
    next: usize,
}
impl Restoration {
    /// Takes an actual admitted native quarantine job, not a source receipt or
    /// start claim. The activation coordinator must still fence and authorize
    /// native provisioning BEFORE obtaining this job. This does not run code.
    pub(in crate::marketplace) fn new(
        job: SourceQuarantineJob,
        path: PathBuf,
        budget: ArchiveBudget,
    ) -> Self {
        Self {
            job: Some(job),
            path,
            budget,
            initialized: false,
            opening: None,
            replay: None,
            reader: None,
            purposes: Vec::new(),
            next: 0,
        }
    }
    fn begin_open(&mut self) -> Result<()> {
        self.initialized = true;
        let job = self.job.as_mut().context("restoration job is missing")?;
        if !job.provisioned
            || !job.completed.is_empty()
            || job.activity.is_some()
            || job.launch.is_some()
            || job.failed_launch.is_some()
            || job.uncertain_start
            || job.peer_transfer.is_some()
            || job.provider_lookup.is_some()
            || job.export_reserved.is_some()
        {
            bail!("restoration requires a fresh exclusively owned quarantine");
        }
        let expected = job.plan.source_match.clone();
        let allowance = Replay::read_allowance(self.budget, &expected)?;
        let archive = expected.workspace.volume_reservation_bytes()? + 2 * MIB;
        if job
            .plan
            .disk_reservation
            .checked_add(archive)
            .is_none_or(|total| total > job.plan.disk_limit)
        {
            bail!("restoration did not reserve both archive and quarantine storage");
        }
        // Host readback is charged separately; native receive and Git/package
        // verification continue using these same non-refundable job counters.
        charge_bytes(&mut job.input_used, allowance, job.plan.input_limit)?;
        self.purposes = expected
            .workspace
            .descriptors()?
            .iter()
            .map(|d| d.purpose)
            .collect();
        let path = self.path.clone();
        let budget = self.budget;
        let lease = job._lease.clone();
        let expires = job.expires;
        self.opening = Some(tokio::task::spawn_blocking(move || {
            Replay::open(&path, budget, &expected, lease, allowance, expires)
        }));
        Ok(())
    }
    async fn join_open(&mut self) -> Result<()> {
        if let Some(task) = self.opening.as_mut() {
            let result = task.await;
            self.opening = None;
            self.replay = Some(result.context("archive replay open stopped")??);
        }
        Ok(())
    }
    /// Copies at most one signed slot per step. A true result is only copy
    /// progress; finish still inspects the native workspace, and source matching
    /// and current authority/grant/readiness checks remain separate.
    pub(in crate::marketplace) async fn advance(&mut self) -> Result<bool> {
        if !self.initialized {
            self.begin_open()?;
        }
        self.join_open().await?;
        let replay = self
            .replay
            .as_mut()
            .context("archive replay is unavailable")?;
        let job = self.job.as_mut().context("restoration job is missing")?;
        let Some(purpose) = self.purposes.get(self.next).copied() else {
            return Ok(true);
        };
        if self.reader.is_none() {
            self.reader = Some(replay.take(purpose)?);
        }
        let reader = self
            .reader
            .as_mut()
            .context("archive replay reader is missing")?;
        let runtime = job.runtime.clone();
        let result = job.receive(runtime.as_ref(), purpose, reader).await;
        reader.drain().await?;
        result?;
        self.reader = None;
        self.next += 1;
        Ok(self.next == self.purposes.len())
    }
    pub(in crate::marketplace) async fn finish(&mut self) -> Result<QuarantinedSource> {
        if self.replay.is_none()
            || self.opening.is_some()
            || self.reader.is_some()
            || self.next != self.purposes.len()
        {
            bail!("archive restoration is incomplete");
        }
        let source = SourceQuarantineJob::finish_retained(&mut self.job).await?;
        self.replay = None;
        Ok(source)
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        // Returned errors here mean the blocking operation HAS joined (bad
        // content/open failure/panic), not that I/O remains live. Cancelling
        // either await leaves its handle in self and never reaches disposal.
        let _ = self.join_open().await;
        if let Some(reader) = self.reader.as_mut() {
            let _ = reader.drain().await;
        }
        if let Some(job) = self.job.as_mut() {
            job.discard_retained().await?;
        }
        self.job = None;
        self.reader = None;
        self.replay = None;
        Ok(())
    }
}
