//! One original archive/selection pair, retained through physical cleanup.
//! SourceArchived display data alone can never construct or extract this.
use super::*;
use crate::marketplace::archive::ArchivedSource;

/// Data/integrity handoff, not permission to install, activate, execute or seed.
/// No Clone/Deserialize, public constructor, filesystem path or status factory.
pub(in crate::marketplace) struct ArchivedPreparation {
    selected: Selection,
    archive: ArchivedSource,
    review: Digest,
    grant_revision: i64,
}
impl ArchivedPreparation {
    pub(in crate::marketplace) async fn maintain(&mut self) -> Result<()> {
        self.selected.maintain().await
    }
    pub(in crate::marketplace) fn parts(&self) -> (&Selection, &ArchivedSource, Digest, i64) {
        (
            &self.selected,
            &self.archive,
            self.review,
            self.grant_revision,
        )
    }
    pub(in crate::marketplace) fn into_parts(
        self: Box<Self>,
    ) -> (Selection, ArchivedSource, Digest, i64) {
        let Self {
            selected,
            archive,
            review,
            grant_revision,
        } = *self;
        (selected, archive, review, grant_revision)
    }
}

#[derive(Default)]
pub(super) struct Output {
    value: Option<Box<ArchivedPreparation>>,
    cleanup_joined: bool,
}
pub(super) type Slot = Arc<Mutex<Output>>;

pub(super) fn retain(
    slot: &Slot,
    selected: Selection,
    archive: ArchivedSource,
    review: Digest,
    grant_revision: i64,
) -> Result<()> {
    let mut output = slot
        .lock()
        .map_err(|_| anyhow::anyhow!("archive handoff owner unavailable"))?;
    if output.value.is_some() || output.cleanup_joined || grant_revision <= 0 {
        bail!("original archive handoff already owned or invalid");
    }
    output.value = Some(Box::new(ArchivedPreparation {
        selected,
        archive,
        review,
        grant_revision,
    }));
    Ok(())
}

/// Called only by the original preparation task AFTER its cleanup succeeds.
pub(super) fn complete(slot: &Slot) -> Result<()> {
    let mut output = slot
        .lock()
        .map_err(|_| anyhow::anyhow!("archive handoff owner unavailable"))?;
    if output.value.is_none() || output.cleanup_joined {
        bail!("original archive completion missing or already acknowledged");
    }
    output.cleanup_joined = true;
    Ok(())
}

impl Service {
    /// Internal application handoff only. The caller must already own its
    /// install command before awaiting this; no HTTP route returns this pair.
    /// A cancelled waiter leaves the SAME task/output in the service slot.
    pub(in crate::marketplace) async fn take_archive(
        &self,
        id: Uuid,
        destination: &mut Option<Box<super::ArchivedPreparation>>,
    ) -> Result<()> {
        if destination.is_some() {
            bail!("archive destination already owns an original result");
        }
        let entry = self.entry(Some(id))?;
        let mut task = entry.task.lock().await;
        if let Err(error) = task.join_cleanup().await {
            bail!("archive cleanup is unconfirmed; original ownership remains fenced: {error:#}");
        }
        // Same lock order as cancellation: task -> service -> output. There is
        // NO await after moving either original output or clearing this slot.
        let mut current = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("preparation owner unavailable"))?;
        if current
            .as_ref()
            .is_none_or(|current| !Arc::ptr_eq(current, &entry))
            || *entry.cancel.borrow()
        {
            bail!("original preparation was cancelled or replaced");
        }
        let slot = entry
            .handoff
            .as_ref()
            .context("preparation has no original archive output")?;
        let mut output = slot
            .lock()
            .map_err(|_| anyhow::anyhow!("archive handoff owner unavailable"))?;
        if !output.cleanup_joined {
            bail!("original archive cleanup acknowledgement missing");
        }
        *destination = Some(
            output
                .value
                .take()
                .context("original archive already handed off")?,
        );
        *current = None;
        Ok(())
    }
}
