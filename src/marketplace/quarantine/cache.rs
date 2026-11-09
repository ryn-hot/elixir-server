//! One disposable byte workspace per GLOBAL foreground lane. No process,
//! verification result, job deadline, resource sample or grant survives reuse.
//! Its name is recoverable without a registry; labels are only cache hints.
use super::*;
use crate::runtime::model::SourceCacheState;

const LABEL: &str = "elixir.marketplace.source_cache";
const SLOT: &str = "elixir.marketplace.cache_slot";

struct Slot {
    deployment: String,
    name: String,
    labels: HashMap<String, String>,
}
impl Slot {
    fn new(lease: &ControllerLease) -> Result<Self> {
        let deployment = lease.storage_namespace()?;
        Ok(Self {
            name: format!("elixir-marketplace-source-{deployment}"),
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
                (
                    "elixir.marketplace.volume".to_owned(),
                    "quarantine-v1".to_owned(),
                ),
                (SLOT.to_owned(), "source-v1".to_owned()),
            ]),
            deployment,
        })
    }
    async fn state(&self, runtime: &dyn RuntimeManager) -> Result<SourceCacheState> {
        timeout(
            Duration::from_secs(10),
            runtime.source_cache_state(&self.name, &self.labels),
        )
        .await
        .context("source cache metadata inspection deadline")?
    }
    async fn evict(&self, runtime: &dyn RuntimeManager) -> Result<()> {
        // Only a stopped, exactly owned immutable identity may be removed. A
        // live/unknown process must first undergo ordinary fenced recovery.
        SourceQuarantineJob::remove_stopped_writer(runtime, &self.deployment).await?;
        runtime
            .remove_private_file_volume(&self.name, &self.labels)
            .await
    }
}

pub(super) struct Choice {
    pub deployment: String,
    pub key: Digest,
    pub reuse: bool,
}
fn key(plan: &SourceQuarantinePlan, image: &str) -> Result<Digest> {
    Ok(Digest::of(canonical::to_vec(&(
        "elixir-source-cache-bytes-v1",
        image,
        &plan.source_match,
    ))?))
}

/// Before ANY other foreground workspace is allocated, evict this one. Thus
/// cache bytes use the existing single workspace allocation, not extra storage.
pub(super) async fn evict(runtime: &dyn RuntimeManager, lease: &ControllerLease) -> Result<()> {
    if !cfg!(unix) {
        return Ok(());
    }
    let slot = Slot::new(lease)?;
    match slot.state(runtime).await? {
        SourceCacheState::Unsupported | SourceCacheState::Absent => Ok(()),
        SourceCacheState::Present(_) => slot.evict(runtime).await,
    }
}

pub(super) async fn choose(
    runtime: &dyn RuntimeManager,
    lease: &ControllerLease,
    plan: &SourceQuarantinePlan,
    image: &str,
) -> Result<Option<Choice>> {
    if !cfg!(unix) {
        return Ok(None);
    }
    let slot = Slot::new(lease)?;
    let expected = key(plan, image)?;
    let reuse = match slot.state(runtime).await? {
        SourceCacheState::Unsupported => return Ok(None),
        SourceCacheState::Absent => false,
        SourceCacheState::Present(found)
            if found.as_deref() == Some(expected.to_string().as_str()) =>
        {
            true
        }
        SourceCacheState::Present(_) => {
            // Switching release/image/layout never accumulates old volumes.
            slot.evict(runtime).await?;
            false
        }
    };
    Ok(Some(Choice {
        deployment: slot.deployment,
        key: expected,
        reuse,
    }))
}

pub(super) fn label(labels: &mut HashMap<String, String>, key: Digest) {
    labels.insert(SLOT.to_owned(), "source-v1".to_owned());
    labels.insert(LABEL.to_owned(), key.to_string());
}

fn require_cached(job: &SourceQuarantineJob) -> Result<bool> {
    if !job.volume.labels.contains_key(LABEL) {
        return Ok(false);
    }
    let slot = Slot::new(&job._lease)?;
    if job.deployment != slot.deployment
        || job.volume.name != slot.name
        || slot
            .labels
            .iter()
            .any(|(k, v)| job.volume.labels.get(k) != Some(v))
        || job.volume.labels.get(LABEL) != Some(&key(&job.plan, &job.volume.image)?.to_string())
    {
        bail!("source cache does not match its original lane and byte plan");
    }
    Ok(true)
}

/// Only an actual read-only native root inspection has established the slots'
/// physical shape. No receive-completed or source-verified flags are invented:
/// this is still quarantined input to a NEW actual native package/Git match.
pub(super) fn take_candidate(slot: &mut Option<SourceQuarantineJob>) -> Result<QuarantinedSource> {
    let job = slot.as_ref().context("cached source owner missing")?;
    if !require_cached(job)?
        || !job.provisioned
        || Instant::now() >= job.expires
        || !job.completed.is_empty()
        || job.activity.is_some()
        || job.launch.is_some()
        || job.failed_launch.is_some()
        || job.uncertain_start
        || job.peer_transfer.is_some()
        || job.provider_lookup.is_some()
        || job.export_reserved.is_some()
    {
        bail!("cached source requires a fresh inspected and exclusively owned job");
    }
    // Preserve the existing combined archive + workspace disk ceiling even
    // though this path does not reread/copy the authoritative host archive.
    let archive = job.plan.workspace.volume_reservation_bytes()? + 2 * MIB;
    if job
        .plan
        .disk_reservation
        .checked_add(archive)
        .is_none_or(|n| n > job.plan.disk_limit)
    {
        bail!("cached source did not reserve both archive and workspace storage");
    }
    Ok(QuarantinedSource {
        job: slot.take().expect("borrowed cached source"),
    })
}

/// Called only through actual RetainedCode after native matching/execution.
/// Cleanup still joins/stops/removes the original process before the lease is
/// released. Dropping the old job retains bytes, NEVER its execution authority.
pub(super) async fn park(job: &mut SourceQuarantineJob) -> Result<bool> {
    if !require_cached(job)? {
        return Ok(false);
    }
    let runtime = job.runtime.clone();
    job.retire_in_place(runtime.as_ref(), false).await?;
    // The retained volume is data, not a way to park a native actor or passive
    // daemon-image owner. Unknown cleanup must keep the original lane fenced.
    if Fence::load(&job._lease)?.is_some() {
        bail!("raw source cache cannot retain a native activity fence");
    }
    #[cfg(unix)]
    image_reference::require_absent(&job._lease)?;
    Ok(true)
}

#[cfg(all(test, unix))]
mod tests;
