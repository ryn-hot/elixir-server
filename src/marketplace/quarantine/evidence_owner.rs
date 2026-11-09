//! Original in-process source-job identity, not a source/build/runtime grant.
//! Weak ownership prevents a receipt from keeping a disposed job's lane locked.
use super::*;
use std::sync::Weak;

pub(super) struct EvidenceOwner {
    lease: Weak<ControllerLease>,
    expires: Instant,
    deployment: String,
    release: Digest,
}
impl EvidenceOwner {
    pub(super) fn capture(source: &SourceMatchedQuarantine) -> Result<Self> {
        source.ensure_job_active()?;
        let lease = &source.source.job._lease;
        if !lease.matches_directory(lease.directory()) {
            bail!("native evidence lost its original source lease");
        }
        Ok(Self {
            lease: Arc::downgrade(lease),
            expires: source.expires(),
            deployment: source.source.job.deployment.clone(),
            release: source.workspace().release_id,
        })
    }
    pub(super) fn require(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        source.ensure_job_active()?;
        let original = self
            .lease
            .upgrade()
            .context("native evidence source owner was disposed")?;
        if !Arc::ptr_eq(&original, &source.source.job._lease)
            || !original.matches_directory(original.directory())
            || self.expires != source.expires()
            || self.deployment != source.source.job.deployment
            || self.release != source.workspace().release_id
        {
            bail!("native evidence belongs to another original source job");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn byte_identical_jobs_deadline_changes_and_disposed_owners_cannot_reuse_evidence()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let parent = root.path().canonicalize()?;
        let mut original = super::super::tests::matched_fixture(&parent.join("original")).await?;
        let mut other = super::super::tests::matched_fixture(&parent.join("other")).await?;
        let evidence = EvidenceOwner::capture(&original)?;
        evidence.require(&original)?;
        // Equal DATA deliberately cannot replace the exact live OS-lock owner.
        // This fixture is not an actual native build/verification receipt.
        other.source.job.plan = original.source.job.plan.clone();
        other.summary = original.summary.clone();
        other.source.job.expires = original.expires();
        other.source.job.deployment = original.source.job.deployment.clone();
        assert!(evidence.require(&other).is_err());
        let expires = original.expires();
        original.source.job.expires += Duration::from_millis(1);
        assert!(evidence.require(&original).is_err());
        original.source.job.expires = expires;
        evidence.require(&original)?;
        let weak = evidence.lease.clone();
        drop(original);
        assert!(
            weak.upgrade().is_none(),
            "evidence must not retain its source lane"
        );
        assert!(ControllerLease::acquire(&parent.join("original")).is_ok());
        assert!(evidence.require(&other).is_err());
        Ok(())
    }
}
