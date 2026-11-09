//! Foreground, offline source quarantine. No RPC accepts this plan from a peer.
//! Hash equality here does not authorize source, execution, installation or
//! sharing. The protected network, source verifier and installed store remain
//! separate boundaries. One global expensive-job lease spans this workspace.
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    canonical,
    identity::Digest,
    quarantine::{
        JOB_END, JOB_START, MAX_JOB_SECONDS, QuarantineDescriptor, QuarantineEvent,
        QuarantinePurpose, QuarantineWorkspace,
    },
    security::AuthorizedRelease,
    source_job::{
        MAX_SOURCE_MATCH_EVENT_BYTES, SourceMatchDescriptor, SourceMatchEvent, SourceMatchSummary,
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::{Instant, timeout, timeout_at},
};

use super::{
    lease::{
        ControllerLease,
        activity::{Fence, Target},
    },
    runtime::{SourceJobLimits, image_is_pinned, inspect_quarantine_worker, prepare_source_worker},
};
use crate::runtime::{
    PrivateProcessStdio, RuntimeManager,
    model::{
        ContainerHandle, ContainerSpec, ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL,
        ELIXIR_INSTANCE_ID_LABEL, ELIXIR_MANAGED_LABEL, QuarantineMarketplaceVolumeSpec,
        VolumeMount, VolumeMountSourceKind, apply_container_spec_fingerprint,
    },
};

const MIB: u64 = 1024 * 1024;
fn source_input_idle(received: u64) -> Duration {
    Duration::from_secs(if received == 0 {
        elixir_marketplace::quarantine::INITIAL_PAYLOAD_WAIT_SECONDS
    } else {
        elixir_marketplace::quarantine::BODY_IDLE_WAIT_SECONDS
    })
}
#[cfg(unix)]
pub(in crate::marketplace) mod build;
mod build_inputs;
#[cfg(unix)]
mod container_image;
#[cfg(unix)]
mod container_filesystem;
#[cfg(unix)]
mod evidence_owner;
#[cfg(unix)]
pub(in crate::marketplace) mod image_export;
#[cfg(unix)]
pub(in crate::marketplace) mod image_import;
#[cfg(unix)]
pub(in crate::marketplace) mod image_reference;
#[cfg(unix)]
pub(in crate::marketplace) mod local_build;
mod cache;
pub(in crate::marketplace) mod execution;
mod launch;
mod measurement;
pub(in crate::marketplace) use measurement::HostObservation;
#[cfg(unix)]
mod restore;
#[cfg(unix)]
pub(in crate::marketplace) use restore::Restoration;
// Ready + terminal reply (including the exact manifest), bounded diagnostics.
const SOURCE_MATCH_CONTROL_RESERVATION: u64 =
    2 * (4 + MAX_SOURCE_MATCH_EVENT_BYTES as u64) + 64 * 1024;
/// Explicit foreground owner profile, never borrowed from automatic catalog
/// storage. All positive limits must be admitted against aggregate host usage
/// by the controller BEFORE opening this job. No network permission is implied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceJobBudget {
    pub memory_bytes: u64,
    pub cpu_millicores: u32,
    pub disk_bytes: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub wall_seconds: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SourceQuarantinePlan {
    workspace: QuarantineWorkspace,
    limits: SourceJobLimits,
    disk_reservation: u64,
    preallocated_bytes: u64,
    disk_limit: u64,
    input_limit: u64,
    output_limit: u64,
    initial_output: u64,
    source_match: SourceMatchDescriptor,
}

/// Private server-owned scheduling source. A peer/HTTP DTO cannot replace it.
/// Each NEW native process needs fresh admission; a completed workspace is not
/// an ongoing resource, transfer, execution or installation grant.
/// The same preallocated job phase may reuse a still-current host sample, never
/// extend its original lifetime or skip the actual admission checks.
#[async_trait::async_trait]
pub(super) trait ResourceAdmission: Send + Sync {
    fn needs_measurement(&self) -> bool {
        false
    }
    async fn before_measurement(&self, _plan: &SourceQuarantinePlan) -> Result<()> {
        Ok(())
    }
    async fn check(
        &self,
        plan: &SourceQuarantinePlan,
        provisioned: bool,
        measurement: Option<&HostObservation>,
    ) -> Result<()>;
}
impl SourceQuarantinePlan {
    /// Authority, current head/revocation, public-source preflight, owner
    /// approval and aggregate job admission are distinct controller checks.
    /// `artifact` selects a signed blob only; it never supplies a URL/path.
    pub fn for_release(
        release: &AuthorizedRelease,
        artifact: Option<usize>,
        budget: &SourceJobBudget,
    ) -> Result<Self> {
        Self::for_payload(release.release_id(), release.payload(), artifact, budget)
    }
    /// A sealed native observation, not a deserialized "verified" flag. This
    /// plans quarantine only; current retained authority, public-source checks,
    /// owner admission and permissions remain separate controller gates.
    pub fn for_metadata(
        observation: &super::worker::MetadataObservation,
        artifact: Option<usize>,
        budget: &SourceJobBudget,
    ) -> Result<Self> {
        observation.support()?;
        let release = &observation.descriptor().release.payload;
        Self::for_payload(
            Digest::of(canonical::to_vec(release)?),
            release,
            artifact,
            budget,
        )
    }
    pub fn for_publication(
        observation: &super::worker::PublicationObservation,
        artifact: Option<usize>,
        budget: &SourceJobBudget,
    ) -> Result<Self> {
        Self::for_payload(
            observation.summary()?.release_id,
            &observation.publication().release.payload,
            artifact,
            budget,
        )
    }
    fn for_payload(
        release_id: Digest,
        release: &elixir_marketplace::records::Release,
        artifact: Option<usize>,
        budget: &SourceJobBudget,
    ) -> Result<Self> {
        let artifact = artifact
            .map(|index| {
                release
                    .artifacts
                    .get(index)
                    .map(|a| a.blob.clone())
                    .context("selected artifact is not in the authorized release")
            })
            .transpose()?;
        let workspace = QuarantineWorkspace {
            schema_version: 1,
            release_id,
            package: release.package.clone(),
            proof: release.source.proof.clone(),
            artifact,
            timeout_seconds: budget.wall_seconds,
        };
        workspace.validate()?;
        let source_match = SourceMatchDescriptor {
            schema_version: 1,
            workspace: workspace.clone(),
            release: release.clone(),
        };
        source_match.validate()?;
        let limits = SourceJobLimits::new(budget.memory_bytes, budget.cpu_millicores)?;
        let descriptors = workspace.descriptors()?;
        let input = descriptors.iter().map(|d| d.blob.size).sum::<u64>();
        // Reserve an EOF probe per slot. A malicious extra byte is consumed as
        // bounded input but never written. Charge initialization conservatively
        // even when an existing workspace needs only read-only inspection.
        let initial_output = input + 4096;
        // The provisioner uses 8 MiB tmpfs + 1 MiB shared memory; the receiver
        // uses no more. Charge these plus control metadata alongside ALL slots.
        let mut disk_reservation = workspace.volume_reservation_bytes()? + 10 * MIB;
        let mut build_input = 0;
        let mut image_output = 0;
        if workspace.artifact.is_some() {
            let build = super::runtime::build::execution(
                &source_match,
                (budget.wall_seconds * 1000) as u32,
            )?;
            disk_reservation += super::runtime::build::extra_disk(&build)?;
            // Native receive, repeated package/Git/recipe verification and
            // materializer write/readback all spend the ORIGINAL job allowance.
            build_input = 2 * build.inputs.input_allowance()?
                + workspace.package.size
                + workspace.proof.size
                + 3 * workspace.package.size
                + workspace
                    .artifact
                    .as_ref()
                    .context("build artifact missing")?
                    .size
                + 1;
            let staging = super::archive::ArchiveBudget {
                bytes: super::runtime::build::staging_reservation(&source_match)?,
                releases: 1,
            };
            build_input += staging.metadata_read_allowance()?
                + 3 * (workspace.package.size + workspace.proof.size + 2);
            // One read-only OCI graph check AFTER actual reproduction. This is
            // included before the original job opens, not new helper credit.
            build_input += workspace.artifact.as_ref().context("container artifact missing")?.size
                + 4 * elixir_marketplace::container_image::MAX_METADATA_BYTES as u64 + 1;
            // Combined graph/filesystem work stays in the ORIGINAL admission.
            // Its uncompressed/entry counters are not a daemon disk allocation.
            build_input += 3 * workspace.artifact.as_ref().context("container artifact missing")?.size
                + 4 * elixir_marketplace::container_image::MAX_METADATA_BYTES as u64 + 4;
            // Reverify and stream the derived image AFTER actual reproduction
            // and filesystem evidence. No new I/O grant after job creation.
            let artifact = workspace.artifact.as_ref().context("container artifact missing")?;
            build_input += 5 * artifact.size
                + 4 * elixir_marketplace::container_image::MAX_METADATA_BYTES as u64 + 8;
            // Actual host staging verification and later import readback stay
            // within this original admission; no archive parser runs here.
            let image_export_limit = elixir_marketplace::container_image::export::output_limit(artifact)?;
            // Seal rehash, import preflight rehash and exact streaming read.
            build_input += 3 * (image_export_limit + 1);
            build_input += MIB; // bounded import control/actual daemon observations
            #[cfg(unix)]
            {image_output=container_image::CONTROL_RESERVATION + container_filesystem::CONTROL_RESERVATION
                + elixir_marketplace::container_image::export::control_output_allowance(artifact)?
                + image_export_limit + 2*MIB;}
        }
        if budget.memory_bytes < 64 * MIB
            || budget.memory_bytes % MIB != 0
            || budget.wall_seconds == 0
            || budget.wall_seconds > MAX_JOB_SECONDS
            || budget.disk_bytes < disk_reservation
            || budget.output_bytes < initial_output + input + SOURCE_MATCH_CONTROL_RESERVATION + image_output
            || budget.input_bytes
                < input + descriptors.len() as u64 + source_match.input_allowance()? + build_input
        {
            bail!(
                "source job needs positive sufficient RAM/CPU/disk/input/output/time reservations"
            );
        }
        Ok(Self {
            workspace,
            limits,
            disk_reservation,
            preallocated_bytes: input,
            disk_limit: budget.disk_bytes,
            input_limit: budget.input_bytes,
            output_limit: budget.output_bytes,
            initial_output,
            source_match,
        })
    }
    pub fn workspace(&self) -> &QuarantineWorkspace {
        &self.workspace
    }
    /// Expected byte/job coordinates only, never matched native bytes.
    pub(in crate::marketplace) fn source_descriptor(&self) -> &SourceMatchDescriptor {
        &self.source_match
    }
    pub fn disk_reservation_bytes(&self) -> u64 {
        self.disk_reservation
    }
    pub(super) fn memory_reservation_bytes(&self) -> u64 {
        self.limits.memory_bytes
    }
    pub(super) fn remaining_disk_reservation_bytes(&self) -> u64 {
        // The trusted native provisioner proves every signed slot is physically
        // allocated before returning. Keep all allocation slack, metadata and
        // temporary/control reservations, but do not reserve those bytes twice.
        self.disk_reservation - self.preallocated_bytes
    }
}

pub struct SourceQuarantineJob {
    plan: SourceQuarantinePlan,
    volume: QuarantineMarketplaceVolumeSpec,
    deployment: String,
    completed: Vec<QuarantinePurpose>,
    input_used: u64,
    output_used: u64,
    expires: Instant,
    _lease: Arc<ControllerLease>,
    activity: Option<Fence>,
    runtime: Arc<dyn RuntimeManager>,
    launch: Option<launch::Launch>,
    failed_launch: Option<launch::Started>,
    uncertain_start: bool,
    admission: Option<Arc<dyn ResourceAdmission>>,
    // One already-retired probe's snapshot, not code or cleanup authority.
    // Phase and original sample age bound reuse; no persistent cache/history.
    cached_measurement: Option<(bool, HostObservation)>,
    provisioned: bool,
    // Monotonic in-memory history for ORIGINAL-claim admission deferrals only.
    // Never a cleanup/recovery proof and never reset after a native attempt.
    native_attempted: bool,
    peer_transfer: Option<super::blob_download::Transfer>,
    provider_lookup: Option<super::provider_lookup::Lookup>,
    // Typed admission deferral proves no export process/read began. Retain one
    // reservation across that wait; never refund it or reserve it twice.
    export_reserved: Option<QuarantinePurpose>,
}
/// Remains quarantined, with exclusive ownership held across later source
/// verification. Not cloneable, deserializable or an installation capability.
pub struct QuarantinedSource {
    job: SourceQuarantineJob,
}
/// Package/Git equality only. The lease still protects the files. Live public
/// source, manifest/runtime/grant closure and compiled equality remain separate
/// requirements before installation, execution or public sharing.
pub struct SourceMatchedQuarantine {
    source: QuarantinedSource,
    summary: SourceMatchSummary,
    public_source: Option<super::worker::PublicSourceObservation>,
    disposed: bool,
    #[cfg(unix)]
    retained_build: Option<Box<local_build::RetainedBuildBinding>>,
}
impl SourceMatchedQuarantine {
    /// Attach a current, actual protected public-text observation to this exact
    /// Git-matched package, including the license file's verified digest. This
    /// preserves the existing job/lease/budgets and is not installation authority.
    pub fn bind_public_source(
        mut self,
        observation: super::worker::PublicSourceObservation,
    ) -> Result<Self> {
        self.refresh_public_source(observation)?;
        Ok(self)
    }
    pub fn refresh_public_source(
        &mut self,
        observation: super::worker::PublicSourceObservation,
    ) -> Result<()> {
        self.ensure_job_active()?;
        #[cfg(unix)]
        if self.retained_build.is_some() {
            bail!("retained compiled provenance cannot become foreground availability authority");
        }
        observation.require_matches_source(&self.source.job.plan.source_match, &self.summary)?;
        self.public_source = Some(observation);
        Ok(())
    }
    pub fn require_public_source(&self) -> Result<()> {
        self.ensure_job_active()?;
        self.public_source
            .as_ref()
            .context("no live public-source binding on this quarantine")?
            .require_matches_source(&self.source.job.plan.source_match, &self.summary)
    }
    #[cfg(unix)]
    fn require_compiled_source(&self) -> Result<()> {
        self.ensure_job_active()?;
        match (&self.public_source, &self.retained_build) {
            (Some(_), None) => self.require_public_source(),
            (None, Some(binding)) => binding.require_source(self),
            _ => bail!("compiled code requires actual foreground or authenticated retained provenance"),
        }
    }
    #[cfg(unix)]
    fn require_compiled_image(
        &self,
        image: &elixir_marketplace::container_image::job::ContainerImageSummary,
    ) -> Result<()> {
        self.require_compiled_source()?;
        if let Some(binding) = &self.retained_build {
            binding.require_image(image)?;
        }
        Ok(())
    }
    pub fn summary(&self) -> &SourceMatchSummary {
        &self.summary
    }
    pub fn volume_name(&self) -> &str {
        self.source.volume_name()
    }
    pub(super) fn release(&self) -> &elixir_marketplace::records::Release {
        &self.source.job.plan.source_match.release
    }
    pub(super) fn selected_artifact(&self) -> Option<&elixir_marketplace::records::Blob> {
        self.source.workspace().artifact.as_ref()
    }
    pub(super) fn ensure_active(&self) -> Result<()> {
        self.ensure_job_active()?;
        #[cfg(unix)]
        if self.retained_build.is_some() {
            self.require_compiled_source()?;
        }
        if self.public_source.is_some() {
            self.require_public_source()?;
        }
        Ok(())
    }
    pub(super) fn require_foreground_manifest(&self) -> Result<()> {
        #[cfg(unix)]
        if self.retained_build.is_some() {
            bail!("retained compiled integrity cannot become foreground approval authority");
        }
        self.ensure_active()
    }
    fn ensure_job_active(&self) -> Result<()> {
        if self.disposed || Instant::now() >= self.source.job.expires {
            bail!("source quarantine job has expired");
        }
        Ok(())
    }
    pub(super) fn expires(&self) -> Instant {
        self.source.job.expires
    }
    /// Blocking archive I/O may outlive its cancelled async caller. Retain the
    /// same global lane, not just the archive directory's separate store lock.
    pub(super) fn job_lease(&self) -> Arc<ControllerLease> {
        self.source.job._lease.clone()
    }
    pub(super) fn runtime(&self) -> Arc<dyn RuntimeManager> {
        self.source.job.runtime.clone()
    }
    #[cfg(unix)]
    pub(super) fn archive_directory(&self) -> Result<std::path::PathBuf> {
        self.source.job.retained_archive_directory()
    }
    pub(super) fn workspace(&self) -> &QuarantineWorkspace {
        self.source.workspace()
    }
    pub(super) fn reserve_archive(&mut self, bytes: u64, reads: u64, writes: u64) -> Result<()> {
        self.ensure_active()?;
        if self
            .source
            .job
            .plan
            .disk_reservation
            .checked_add(bytes)
            .is_none_or(|total| total > self.source.job.plan.disk_limit)
        {
            bail!("foreground job did not reserve both quarantine and archive storage");
        }
        charge_bytes(
            &mut self.source.job.input_used,
            reads,
            self.source.job.plan.input_limit,
        )?;
        charge_bytes(
            &mut self.source.job.output_used,
            writes,
            self.source.job.plan.output_limit,
        )?;
        Ok(())
    }
    pub(super) fn reserve_build_staging(
        &mut self,
        bytes: u64,
        reads: u64,
        writes: u64,
    ) -> Result<()> {
        let job = &self.source.job;
        let expected = super::runtime::build::staging_reservation(&job.plan.source_match)?;
        let descriptor = super::runtime::build::execution(
            &job.plan.source_match,
            u32::try_from(job.plan.workspace.timeout_seconds * 1000)?,
        )?;
        let required = job.plan.workspace.volume_reservation_bytes()?
            + 10 * MIB
            + super::runtime::build::extra_disk(&descriptor)?;
        if bytes != expected || job.plan.disk_reservation < required {
            bail!("build staging was not included in original source admission");
        }
        self.reserve_archive(0, reads, writes)
    }
    pub(super) async fn discard_retained(&mut self) -> Result<()> {
        if self.disposed {
            return Ok(());
        }
        // Revoke this wrapper only after the original cleanup actually joins.
        // Cancellation/failure leaves the same owner available for a retry.
        self.source.job.discard_retained().await?;
        self.disposed = true;
        #[cfg(unix)]
        {
            self.retained_build = None;
        }
        Ok(())
    }
    /// Integrity-only readback under the same exclusive foreground lease and
    /// cumulative byte/time budget. The destination must be separately reserved.
    pub(super) async fn export_blob(
        &mut self,
        runtime: &dyn RuntimeManager,
        purpose: QuarantinePurpose,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        self.source.job.require_runtime(runtime)?;
        use sha2::{Digest as _, Sha256};
        self.ensure_active()?;
        let descriptor = self.workspace().descriptor(purpose)?;
        let job = &mut self.source.job;
        // Local archive readback is not free network input and cannot reset a
        // previous receive/verification allowance. Failed attempts stay charged.
        job.reserve_export(purpose)?;
        let expires = job.expires;
        let operation = async {
            job.cleanup_activity(runtime).await?;
            job.recover_writer(runtime).await?;
            let mut spec = job.profile(&descriptor)?;
            spec.command = vec![
                "--export-blob".to_owned(),
                String::from_utf8(canonical::to_vec(&descriptor)?)?,
            ];
            spec.volumes[0].read_only = true;
            spec.security.file_size_limit_bytes = Some(1);
            apply_container_spec_fingerprint(&mut spec);
            let handle = job.launch_writer(&spec).await?;
            job.export_reserved = None;
            let result = async {
                let mut channel =
                    SourceChannel::new(runtime.attach_private_stdio(&handle, &spec.labels).await?)?;
                channel.marker(JOB_START).await?;
                match channel.event().await? {
                    QuarantineEvent::Ready {
                        descriptor: ready, ..
                    } if ready.same_job(&descriptor) => {}
                    _ => bail!("source export did not become ready"),
                }
                inspect_quarantine_worker(
                    runtime,
                    &handle,
                    &spec,
                    &job.plan.limits,
                    &job.volume.name,
                    1,
                    true,
                )
                .await?;
                channel.marker(JOB_END).await?;
                let mut hash = Sha256::new();
                let mut remaining = descriptor.blob.size;
                // Async state machines are moved through several controller
                // layers. Keep bounded streaming storage off their stack frames.
                let mut bytes = vec![0u8; 64 * 1024].into_boxed_slice();
                while remaining != 0 {
                    let maximum = remaining.min(bytes.len() as u64) as usize;
                    let size = timeout(
                        Duration::from_secs(10),
                        channel.output.read(&mut bytes[..maximum]),
                    )
                    .await
                    .context("source export idle deadline exceeded")??;
                    if size == 0 {
                        bail!("short source export");
                    }
                    destination.write_all(&bytes[..size]).await?;
                    hash.update(&bytes[..size]);
                    remaining -= size as u64;
                }
                let mut extra = [0; 1];
                if timeout(Duration::from_secs(10), channel.output.read(&mut extra)).await?? != 0
                    || Digest::from_bytes(hash.finalize().into()) != descriptor.blob.sha256
                    || !timeout(Duration::from_secs(10), channel.child.wait())
                        .await??
                        .success()
                {
                    bail!("source export integrity or termination failed");
                }
                timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                    .await
                    .context("source export diagnostics did not close")???;
                runtime
                    .confirm_quarantine_marketplace_success(&handle, &spec.labels)
                    .await?;
                destination.flush().await?;
                Ok(())
            }
            .await;
            job.cleanup_activity(runtime).await?;
            result
        };
        let result = timeout_at(expires, operation)
            .await
            .context("source export deadline exceeded")
            .and_then(|r| r);
        if !result
            .as_ref()
            .err()
            .is_some_and(super::worker::ParticipationClient::source_resources_deferred)
        {
            job.export_reserved = None;
        }
        if result.is_err() {
            timeout(Duration::from_secs(25), job.recover_writer(runtime))
                .await
                .context("source export cleanup timed out")??;
            job.cleanup_activity(runtime).await?;
        }
        result
    }
}
impl QuarantinedSource {
    pub fn release_id(&self) -> Digest {
        self.job.plan.workspace.release_id
    }
    pub fn workspace(&self) -> &QuarantineWorkspace {
        &self.job.plan.workspace
    }
    pub fn volume_name(&self) -> &str {
        &self.job.volume.name
    }
    pub async fn match_source(
        mut self,
        runtime: &dyn RuntimeManager,
    ) -> Result<SourceMatchedQuarantine> {
        let summary = self.match_in_place(runtime).await?;
        Ok(SourceMatchedQuarantine {
            summary,
            source: self,
            public_source: None,
            disposed: false,
            #[cfg(unix)]
            retained_build: None,
        })
    }
    /// Keep the exclusive workspace in the controller's slot across errors or
    /// cancellation. Only remove it after the last awaited check succeeded.
    pub(super) async fn match_retained(slot: &mut Option<Self>) -> Result<SourceMatchedQuarantine> {
        let source = slot
            .as_mut()
            .context("source verification owner is missing")?;
        let runtime = source.job.runtime.clone();
        let summary = source.match_in_place(runtime.as_ref()).await?;
        Ok(SourceMatchedQuarantine {
            summary,
            source: slot.take().expect("borrowed source owner"),
            public_source: None,
            disposed: false,
            #[cfg(unix)]
            retained_build: None,
        })
    }
    pub(super) async fn discard_retained(&mut self) -> Result<()> {
        self.job.discard_retained().await
    }
    async fn match_in_place(&mut self, runtime: &dyn RuntimeManager) -> Result<SourceMatchSummary> {
        self.job.require_runtime(runtime)?;
        let descriptor = self.job.plan.source_match.clone();
        let job_id = descriptor.job_id()?;
        let expires = self.job.expires;
        let operation = async {
            self.job.cleanup_activity(runtime).await?;
            self.job.recover_writer(runtime).await?;
            let spec = self.job.source_match_profile()?;
            let handle = self.job.launch_writer(&spec).await?;
            let result = async {
                // Reserve before any private start marker/read. A resource
                // deferral launches nothing and must not spend verifier bytes;
                // an actual attempt remains conservative/non-refundable.
                charge_bytes(
                    &mut self.job.input_used,
                    descriptor.input_allowance()?,
                    self.job.plan.input_limit,
                )?;
                charge_bytes(
                    &mut self.job.output_used,
                    SOURCE_MATCH_CONTROL_RESERVATION,
                    self.job.plan.output_limit,
                )?;
                let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
                let mut channel = SourceChannel::new(process)?;
                channel.marker(JOB_START).await?;
                match SourceMatchEvent::parse(
                    &channel
                        .frame(Duration::from_secs(10), MAX_SOURCE_MATCH_EVENT_BYTES)
                        .await?,
                )? {
                    SourceMatchEvent::Ready { job_id: ready, .. } if ready == job_id => {}
                    _ => bail!("source verifier did not become ready for this job"),
                }
                inspect_quarantine_worker(
                    runtime,
                    &handle,
                    &spec,
                    &self.job.plan.limits,
                    &self.job.volume.name,
                    1,
                    true,
                )
                .await?;
                channel.marker(JOB_END).await?;
                let remaining = expires.saturating_duration_since(Instant::now());
                let summary = match SourceMatchEvent::parse(
                    &channel
                        .frame(remaining, MAX_SOURCE_MATCH_EVENT_BYTES)
                        .await?,
                )? {
                    SourceMatchEvent::Complete {
                        job_id: complete,
                        source_match_only: true,
                        result,
                        ..
                    } if complete == job_id => {
                        result.matches(&descriptor)?;
                        *result
                    }
                    _ => bail!("source verifier did not prove this package/Git equality"),
                };
                let mut trailing = [0; 1];
                if timeout(Duration::from_secs(10), channel.output.read(&mut trailing)).await?? != 0
                {
                    bail!("unexpected source verifier output");
                }
                if !timeout(Duration::from_secs(10), channel.child.wait())
                    .await??
                    .success()
                {
                    bail!("source verifier exited unsuccessfully");
                }
                timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                    .await
                    .context("source verifier diagnostics did not close")???;
                runtime
                    .confirm_quarantine_marketplace_success(&handle, &spec.labels)
                    .await?;
                Ok(summary)
            }
            .await;
            self.job.cleanup_activity(runtime).await?;
            result
        };
        let result = timeout_at(expires, operation)
            .await
            .context("source verification job deadline exceeded")
            .and_then(|r| r);
        if result.is_err() {
            timeout(Duration::from_secs(25), self.job.recover_writer(runtime))
                .await
                .context("source verifier cleanup timed out")??;
            self.job.cleanup_activity(runtime).await?;
        }
        result
    }
}

fn valid_deployment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
fn writer_name(deployment: &str) -> String {
    format!("elixir-marketplace-source-writer-{deployment}")
}

fn charge_bytes(used: &mut u64, amount: u64, maximum: u64) -> Result<()> {
    let next = used
        .checked_add(amount)
        .filter(|next| *next <= maximum)
        .context("source job byte budget exceeded")?;
    *used = next;
    Ok(())
}

impl SourceQuarantineJob {
    fn reserve_export(&mut self, purpose: QuarantinePurpose) -> Result<()> {
        if let Some(reserved) = self.export_reserved {
            if reserved != purpose {
                bail!("a different source export reservation is still owned");
            }
            return Ok(());
        }
        let size = self.plan.workspace.descriptor(purpose)?.blob.size;
        charge_bytes(&mut self.input_used, size + 1, self.plan.input_limit)?;
        charge_bytes(
            &mut self.output_used,
            size + 4096 + 64 * 1024,
            self.plan.output_limit,
        )?;
        self.export_reserved = Some(purpose);
        Ok(())
    }
    /// Recover only a durably bound immutable writer identity while holding
    /// the global lane. A missing/uncertain startup identity cannot be proved
    /// stopped by a name lookup and remains fenced, even across server restart.
    /// This does not discard bytes or authorize a new install/runtime.
    pub async fn recover_interrupted(
        runtime: &dyn RuntimeManager,
        job_directory: &Path,
    ) -> Result<bool> {
        let lease = ControllerLease::acquire_for_recovery(job_directory)?;
        Self::recover_leased(runtime, &lease).await
    }
    /// The caller retains the SAME global lane while establishing its runtime
    /// controller. False means no recorded operation, not an inspected native
    /// stop. Known operations still require immutable-ID retirement; uncertain
    /// startup never becomes an idle-controller acknowledgement.
    pub(in crate::marketplace) async fn recover_leased(
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<bool> {
        if !lease.matches_directory(lease.directory()) {
            bail!("native recovery lost its original controller lock");
        }
        // Same OS lease means the old process/HTTP owner is gone. Retire its
        // child BEFORE clearing the parent; name absence is never sufficient
        // for an uncertain unbound CREATE. A malformed child blocks recovery.
        super::lease::activity::http::HttpFence::recover(runtime, lease).await?;
        #[cfg(unix)]
        super::lease::activity::gateway::GatewayFence::recover(runtime, lease).await?;
        let mut recovered = if let Some(mut activity) = Fence::load(lease)? {
            activity.reconcile_writer_create(runtime, lease).await?;
            #[cfg(unix)]
            let import=image_import::retire_activity(runtime,lease,&mut activity).await?;
            #[cfg(not(unix))]
            let import=false;
            if !import {Self::stop_activity(runtime, activity.target()).await?;}
            activity.clear(lease)?;
            true
        } else {
            false
        };
        #[cfg(unix)]
        {
            recovered |= image_reference::recover(runtime,lease).await?;
            image_import::cleanup_idle(lease)?;
        }
        // A restart drops this disposable acceleration, not its authoritative
        // archive. Its deterministic name is known even after interrupted copy.
        cache::evict(runtime, lease).await?;
        runtime
            .cleanup_idle_marketplace_build_output(&lease.storage_namespace()?)
            .await?;
        Ok(recovered)
    }
    fn begin_writer(&mut self, spec: &ContainerSpec) -> Result<()> {
        if !super::runtime::source_writer::supported_command(&spec.command) {
            bail!("unsupported source writer command before native creation");
        }
        let intent = super::runtime::source_writer::SourceWriterIntent::capture(spec)?;
        if intent.deployment() != self.deployment {
            bail!("source creation belongs to another job");
        }
        self.begin_activity(Target::WriterCreateIntent { intent })
    }
    fn begin_activity(&mut self, target: Target) -> Result<()> {
        if self.activity.is_some()
            || self.launch.is_some()
            || self.failed_launch.is_some()
            || self.uncertain_start
        {
            bail!("previous native source work requires confirmed cleanup");
        }
        let measurement = matches!(target, Target::Measurement { .. });
        self.activity = Some(Fence::begin(&self._lease, target)?);
        // Trusted read-only measurements consume this job's time/allocation,
        // but are not a source/provision start. A resource deferral is returned
        // only after their ACTUAL cleanup; in-flight/failed fields still fence
        // admission_unstarted, never a reset of source-attempt history.
        self.native_attempted |= !measurement;
        Ok(())
    }
    fn require_runtime(&self, runtime: &dyn RuntimeManager) -> Result<()> {
        if !std::ptr::addr_eq(self.runtime.as_ref(), runtime) {
            bail!("source job runtime owner changed");
        }
        Ok(())
    }
    async fn launch_writer(&mut self, spec: &ContainerSpec) -> Result<ContainerHandle> {
        self.launch_writer_until(spec, self.expires).await
    }
    async fn launch_writer_until(
        &mut self,
        spec: &ContainerSpec,
        expires: Instant,
    ) -> Result<ContainerHandle> {
        self.launch_writer_budgeted(spec, expires, None).await
    }
    async fn launch_writer_budgeted(
        &mut self,
        spec: &ContainerSpec,
        expires: Instant,
        reservation: Option<(u64, u64)>,
    ) -> Result<ContainerHandle> {
        let expires = expires.min(self.expires);
        self.admit_native(expires).await?;
        if Instant::now() >= expires {
            bail!("native source startup deadline exceeded");
        }
        if let Some((input, output)) = reservation {
            // Only after typed resource admission, BEFORE native creation.
            // Calculate both counters first; no partial reservation or refund.
            let input = self
                .input_used
                .checked_add(input)
                .filter(|n| *n <= self.plan.input_limit)
                .context("source job input allowance exhausted")?;
            let output = self
                .output_used
                .checked_add(output)
                .filter(|n| *n <= self.plan.output_limit)
                .context("source job output allowance exhausted")?;
            self.input_used = input;
            self.output_used = output;
        }
        self.begin_writer(spec)?;
        let (fence, handle) = self
            .launch_native(
                launch::Operation::Writer(Box::new(spec.clone()), self.plan.limits.clone()),
                expires,
            )
            .await?;
        self.activity = fence;
        handle.context("native writer startup returned no identity")
    }
    async fn provision(&mut self, expires: Instant) -> Result<()> {
        self.admit_native(expires).await?;
        if Instant::now() >= expires {
            bail!("native source provisioning deadline exceeded");
        }
        self.begin_activity(Target::SourceProvisioner {
            deployment: self.deployment.clone(),
        })?;
        let (fence, handle) = self
            .launch_native(
                launch::Operation::Provision(Box::new(self.volume.clone())),
                expires,
            )
            .await?;
        if fence.is_some() || handle.is_some() {
            bail!("native provisioning result differs");
        }
        self.provisioned = true;
        Ok(())
    }
    async fn admit_native(&mut self, expires: Instant) -> Result<()> {
        // Consume before any await. Denial, timeout or cancellation cannot
        // retain an older positive sample for an implicit retry.
        let previous = self.cached_measurement.take();
        if let Some(admission) = self.admission.clone() {
            let expires = self.expires.min(expires);
            let operation = async {
                let measurement = if admission.needs_measurement() {
                    // Still inspect current server headroom/playback on every
                    // admission, even when the Docker-host sample is current.
                    admission.before_measurement(&self.plan).await?;
                    Some(
                        match measurement::reusable_sample(previous, self.provisioned) {
                            Some(sample) => sample,
                            None => self.measure_host(expires).await?,
                        },
                    )
                } else {
                    None
                };
                admission
                    .check(&self.plan, self.provisioned, measurement.as_ref())
                    .await?;
                if let Some(sample) = measurement {
                    // A slow check must not turn an expired observation into
                    // admission or reset its timestamp when putting it back.
                    sample.report()?;
                    self.cached_measurement = Some((self.provisioned, sample));
                }
                Ok::<_, anyhow::Error>(())
            };
            timeout_at(
                expires.min(Instant::now() + Duration::from_secs(8)),
                operation,
            )
            .await
            .context("native source resource admission exceeded the job deadline")??;
        }
        Ok(())
    }
    async fn launch_native(
        &mut self,
        operation: launch::Operation,
        expires: Instant,
    ) -> Result<(Option<Fence>, Option<ContainerHandle>)> {
        if self.launch.is_some() {
            bail!("native startup is still owned");
        }
        let fence = self
            .activity
            .take()
            .context("native startup is not fenced")?;
        self.launch = Some(launch::Launch::begin(
            self.runtime.clone(),
            self._lease.clone(),
            fence,
            operation,
            expires,
        ));
        let result = self
            .launch
            .as_mut()
            .context("native startup owner is missing")?
            .wait()
            .await;
        self.launch = None;
        if result.is_err() {
            self.uncertain_start = true;
        }
        let mut started = result?;
        if let Some(error) = started.take_error() {
            self.failed_launch = Some(started);
            return Err(error);
        }
        started.adopt()
    }
    fn complete_activity(&mut self) -> Result<()> {
        if let Some(activity) = &mut self.activity {
            activity.clear(&self._lease)?;
        }
        self.activity = None;
        Ok(())
    }
    async fn cleanup_activity(&mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.require_runtime(runtime)?;
        self.cleanup_lookup().await?;
        if self
            .peer_transfer
            .as_ref()
            .is_some_and(|peer| peer.needs_cleanup())
        {
            self.cleanup_peer().await?;
        }
        self.cleanup_native_activity(runtime).await
    }
    async fn cleanup_native_activity(&mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.require_runtime(runtime)?;
        if self.uncertain_start && self.launch.is_none() {
            bail!("native source startup cleanup remains uncertain");
        }
        if let Some(launch) = self.launch.as_mut() {
            // Borrow the SAME owner across cancellation, then adopt its
            // immutable fence before any further await. Taking a local Launch
            // or Started::cleanup future here would lose the acknowledgement
            // when a cleanup waiter timed out, leaving false uncertainty.
            let result = launch.wait().await;
            self.launch = None;
            match result {
                Ok(mut started) => {
                    if started.take_error().is_some() {
                        self.failed_launch = Some(started);
                    } else {
                        let (fence, _) = started.adopt()?;
                        self.activity = fence;
                    }
                    self.uncertain_start = false;
                }
                Err(error) => {
                    self.uncertain_start = true;
                    return Err(error);
                }
            }
        }
        if let Some(failed) = self.failed_launch.as_mut() {
            // Keep the same owner through cancelled/failed stop waits. A known
            // CID is not downgraded to an unknown outcome just because start
            // returned an error; an unknown create/provision remains fenced.
            failed.cleanup_retained().await?;
            self.failed_launch = None;
        }
        if let Some(activity) = &mut self.activity {
            #[cfg(unix)]
            let import=image_import::retire_activity(runtime,&self._lease,activity).await?;
            #[cfg(not(unix))]
            let import=false;
            if !import {Self::stop_activity(runtime, activity.target()).await?;}
        }
        self.complete_activity()
    }
    async fn stop_activity(runtime: &dyn RuntimeManager, target: &Target) -> Result<()> {
        if let Target::BuildOutputAllocation { deployment } = target {
            // No executable actor exists in this phase. Reconcile only the
            // bounded empty storage slot, not a fictitious process completion.
            return runtime
                .remove_marketplace_build_output_volume(deployment)
                .await;
        }
        let (deployment, id, measurement) = match target {
            Target::SourceWriter {
                deployment,
                container_id: Some(id),
            } => (deployment, id, false),
            Target::Measurement {
                deployment,
                container_id: Some(id),
            } => (deployment, id, true),
            _ => {
                bail!("native startup identity is uncertain; the global source lane remains fenced")
            }
        };
        timeout(Duration::from_secs(25), async {
            if let Some(handle) = runtime.get_container_handle(id).await? {
                if handle.id != *id {
                    bail!("native source recovery identity differs");
                }
                let state = runtime
                    .describe_container_runtime_state(id)
                    .await?
                    .context("native source recovery ownership is unavailable")?;
                let name = if measurement {
                    crate::runtime::measurement::name(deployment)
                } else {
                    writer_name(deployment)
                };
                let isolation = if measurement {
                    "measurement-v1"
                } else {
                    "quarantine-v1"
                };
                if state.name != name
                    || state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                    || state.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) != Some(deployment)
                    || state
                        .labels
                        .get("elixir.marketplace.isolation")
                        .map(String::as_str)
                        != Some(isolation)
                {
                    bail!("refusing native source recovery for an unowned immutable writer");
                }
                if measurement {
                    let identity = ContainerHandle {
                        id: id.clone(),
                        name: id.clone(),
                    };
                    runtime.stop_host_measurement(&identity).await?;
                    let actual = runtime.inspect(&identity).await?;
                    if actual.id != *id || actual.running {
                        bail!("measurement is still running");
                    }
                } else {
                    Self::stop_writer(
                        runtime,
                        &ContainerHandle {
                            id: id.clone(),
                            name: id.clone(),
                        },
                    )
                    .await?;
                }
                // A timed-out start may still be in flight inside the daemon
                // even when inspection currently says `created`. Retire this
                // exact identity before releasing the workspace/lane, so no
                // late start can execute against it.
                runtime
                    .remove_container(&ContainerHandle {
                        id: id.clone(),
                        name: id.clone(),
                    })
                    .await?;
                if runtime.get_container_handle(id).await?.is_some() {
                    bail!("native source writer was not positively retired");
                }
            }
            // A positively absent immutable ID cannot later be reused. Unlike
            // absence of a name during create, it cannot receive a late start.
            Ok(())
        })
        .await
        .context("interrupted native source cleanup timed out")?
    }
    /// Pull only the exact signed slot in this already provisioned foreground
    /// workspace. Existing packet credit is required; this mints none. The
    /// native receiver independently rehashes every byte and proves writer
    /// cleanup before this can mark a slot complete. No install authority.
    pub async fn receive_peer(
        &mut self,
        runtime: &dyn RuntimeManager,
        client: &super::worker::ParticipationClient,
        peer: elixir_marketplace::worker_network::PeerKey,
        purpose: QuarantinePurpose,
    ) -> Result<()> {
        use elixir_marketplace::worker_blob::{
            BLOB_REPLY_ALLOWANCE, BLOB_REQUEST_ALLOWANCE, BlobDownloadSpec, MAX_BLOB_JOB_MS,
        };
        let descriptor = self.plan.workspace.descriptor(purpose)?;
        let remaining = self
            .expires
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(MAX_BLOB_JOB_MS as u128) as u64;
        if remaining == 0 {
            bail!("source quarantine job has expired");
        }
        let size = descriptor.blob.size;
        let transfer = BlobDownloadSpec {
            blob: descriptor.blob,
            maximum_bytes: purpose.maximum(),
            timeout_ms: remaining,
            upload_bytes: BLOB_REQUEST_ALLOWANCE,
            download_bytes: BLOB_REPLY_ALLOWANCE + size,
        };
        self.cleanup_peer().await?;
        let owner = super::blob_download::Transfer::retained(client, self._lease.clone());
        self.peer_transfer = Some(owner.clone());
        let received = self
            .receive_with(runtime, purpose, async {
                // Do not open a peer or consume packet credit until the native
                // receiver is ready and its actual kernel profile is inspected.
                super::blob_download::Reader::open_retained(owner, peer, transfer).await
            })
            .await;
        let cancelled = self.cleanup_peer().await;
        match (received, cancelled) {
            (result, Ok(())) => result,
            (Ok(()), Err(error)) => Err(error.context("source blob cleanup is unconfirmed")),
            (Err(failure), Err(error)) => Err(error.context(format!(
                "source blob cleanup is unconfirmed; original transfer failed: {failure:#}"
            ))),
        }
    }
    async fn cleanup_peer(&mut self) -> Result<()> {
        if let Some(peer) = self.peer_transfer.as_ref() {
            peer.cancel().await?;
        }
        self.peer_transfer = None;
        Ok(())
    }
    /// Resolve only this signed workspace slot. Hints grant no availability,
    /// body/packet credit, source equality or installation authority.
    pub(super) async fn find_providers(
        &mut self,
        client: &super::worker::ParticipationClient,
        purpose: QuarantinePurpose,
    ) -> Result<Vec<elixir_marketplace::worker_network::PeerKey>> {
        let sha256 = self.plan.workspace.descriptor(purpose)?.blob.sha256;
        client.require_blob_owner()?;
        if Instant::now() >= self.expires {
            bail!("source quarantine job has expired");
        }
        if self.provider_lookup.is_none() {
            self.provider_lookup = Some(super::provider_lookup::Lookup::begin(
                client,
                sha256,
                self._lease.clone(),
            ));
        }
        let lookup = self
            .provider_lookup
            .as_mut()
            .context("provider lookup owner missing")?;
        if !lookup.matches(client, sha256) {
            bail!("provider lookup scope or owner changed");
        }
        let peers = timeout_at(self.expires, lookup.wait())
            .await
            .context("provider lookup exceeded source job deadline")??;
        self.provider_lookup = None;
        client.require_blob_owner()?;
        Ok(peers)
    }
    async fn cleanup_lookup(&mut self) -> Result<()> {
        if let Some(lookup) = self.provider_lookup.as_mut() {
            lookup.finish().await?;
        }
        self.provider_lookup = None;
        Ok(())
    }
    /// Isolated native qualification only. Production selection uses the actual
    /// protected owner's resource admission and pre-acquired global lane below.
    #[cfg(test)]
    pub(in crate::marketplace) async fn open(
        runtime: Arc<dyn RuntimeManager>,
        job_directory: &Path,
        deployment: &str,
        image: &str,
        plan: &SourceQuarantinePlan,
    ) -> Result<Self> {
        if !valid_deployment(deployment) || !image_is_pinned(image) {
            bail!("invalid trusted source quarantine deployment or image");
        }
        let lease = ControllerLease::acquire(job_directory)?;
        Self::open_owned(runtime, job_directory, deployment, image, plan, lease, None).await
    }
    pub(super) async fn discard_owned(self) -> Result<()> {
        let runtime = self.runtime.clone();
        self.discard(runtime.as_ref()).await
    }
    pub(super) async fn discard_retained(&mut self) -> Result<()> {
        let runtime = self.runtime.clone();
        self.discard_in_place(runtime.as_ref()).await
    }
    pub(super) async fn receive_peer_owned(
        &mut self,
        client: &super::worker::ParticipationClient,
        peer: elixir_marketplace::worker_network::PeerKey,
        purpose: QuarantinePurpose,
    ) -> Result<()> {
        let runtime = self.runtime.clone();
        self.receive_peer(runtime.as_ref(), client, peer, purpose)
            .await
    }
    /// Owner-authenticated SDK input, still independently hashed/written by the
    /// SAME no-network native quarantine receiver and original job counters.
    pub(super) async fn receive_upload_owned(
        &mut self,
        purpose: QuarantinePurpose,
        input: &mut (impl AsyncRead + Unpin),
    ) -> Result<()> {
        let runtime = self.runtime.clone();
        self.receive(runtime.as_ref(), purpose, input).await
    }
    pub(super) fn expires(&self) -> Instant {
        self.expires
    }
    pub(super) fn plan_workspace(&self) -> &QuarantineWorkspace {
        &self.plan.workspace
    }
    #[cfg(unix)]
    pub(super) fn retained_archive_directory(&self) -> Result<std::path::PathBuf> {
        let directory = self._lease.directory();
        if !self._lease.matches_directory(directory) {
            bail!("source archive lost its original controller directory");
        }
        Ok(directory
            .parent()
            .context("source archive needs an owned parent")?
            .join("source-archives"))
    }
    /// The foreground controller acquires the SAME global lane before measuring
    /// host headroom. Keep that lease through native startup, not a new lock
    /// acquired after an admission snapshot can race another source/build job.
    #[cfg(test)]
    pub(super) async fn open_leased(
        runtime: Arc<dyn RuntimeManager>,
        job_directory: &Path,
        deployment: &str,
        image: &str,
        plan: &SourceQuarantinePlan,
        lease: ControllerLease,
        admission: Arc<dyn ResourceAdmission>,
    ) -> Result<Self> {
        Self::open_owned(
            runtime,
            job_directory,
            deployment,
            image,
            plan,
            lease,
            Some(admission),
        )
        .await
    }
    /// The activation owner retains this slot across every await, including
    /// failed/uncertain startup. No caller may drop the slot and claim cleanup.
    pub(super) async fn open_leased_retained(
        slot: &mut Option<Self>,
        runtime: Arc<dyn RuntimeManager>,
        deployment: &str,
        image: &str,
        plan: &SourceQuarantinePlan,
        lease: ControllerLease,
        admission: Arc<dyn ResourceAdmission>,
    ) -> Result<()> {
        Self::open_retained(
            slot,
            runtime,
            deployment,
            image,
            plan,
            lease,
            Some(admission),
        )
        .await
    }
    /// Installed calls only. Metadata can select byte reuse, never native
    /// equality; every hit still gets a new job, root inspection and matcher.
    pub(super) async fn open_installed_retained(
        slot: &mut Option<Self>,
        runtime: Arc<dyn RuntimeManager>,
        deployment: &str,
        image: &str,
        plan: &SourceQuarantinePlan,
        lease: ControllerLease,
        admission: Arc<dyn ResourceAdmission>,
    ) -> Result<bool> {
        let choice = cache::choose(runtime.as_ref(), &lease, plan, image).await?;
        let reuse = choice.as_ref().is_some_and(|choice| choice.reuse);
        Self::open_retained_mode(
            slot,
            runtime,
            (
                choice
                    .as_ref()
                    .map_or(deployment, |choice| choice.deployment.as_str()),
                choice.as_ref().map(|choice| choice.key),
            ),
            image,
            plan,
            lease,
            Some(admission),
        )
        .await?;
        Ok(reuse)
    }
    pub(super) fn take_cached_candidate(slot: &mut Option<Self>) -> Result<QuarantinedSource> {
        cache::take_candidate(slot)
    }
    pub(super) async fn evict_cached(
        runtime: &dyn RuntimeManager,
        lease: &ControllerLease,
    ) -> Result<()> {
        cache::evict(runtime, lease).await
    }
    /// Only a typed BEFORE-provision admission error may use this in-memory
    /// invariant to return the SAME original claim. This is not a stopped-job
    /// or missing-container proof, and cannot authorize fence removal.
    pub(super) fn admission_unstarted(&self) -> bool {
        !self.native_attempted
            && !self.provisioned
            && self.activity.is_none()
            && self.launch.is_none()
            && self.failed_launch.is_none()
            && !self.uncertain_start
            && self.completed.is_empty()
            && self.peer_transfer.is_none()
            && self.provider_lookup.is_none()
            && self.export_reserved.is_none()
    }
    /// Resume ONLY the same never-provisioned job after a typed admission
    /// deferral. This is not native recovery, a new lease, or cleanup proof.
    /// Any helper must already have positively retired before this invariant
    /// can hold. Provisioning always performs fresh admission again.
    pub(super) async fn resume_unstarted_retained(&mut self, expires: Instant) -> Result<()> {
        if !self.admission_unstarted() {
            bail!("only an originally unstarted source admission may resume");
        }
        self.provision(self.expires.min(expires)).await
    }
    /// Initial preparation may resume only its SAME unopened object, runtime,
    /// admission owner and complete input plan. This does not refresh a lease,
    /// counters, deadline or verification observation, or prove native cleanup.
    pub(super) async fn resume_opening_retained(
        &mut self,
        runtime: &dyn RuntimeManager,
        admission: &Arc<dyn ResourceAdmission>,
        plan: &SourceQuarantinePlan,
    ) -> Result<()> {
        self.require_runtime(runtime)?;
        if !self
            .admission
            .as_ref()
            .is_some_and(|owner| Arc::ptr_eq(owner, admission))
            || canonical::to_vec(&self.plan)? != canonical::to_vec(plan)?
        {
            bail!("original source opening owner or input changed");
        }
        self.resume_unstarted_retained(self.expires).await
    }
    #[cfg(test)]
    async fn open_owned(
        runtime: Arc<dyn RuntimeManager>,
        job_directory: &Path,
        deployment: &str,
        image: &str,
        plan: &SourceQuarantinePlan,
        lease: ControllerLease,
        admission: Option<Arc<dyn ResourceAdmission>>,
    ) -> Result<Self> {
        if !lease.matches_directory(job_directory) {
            bail!("source quarantine lease differs from its configured lane");
        }
        let mut slot = None;
        Self::open_retained(
            &mut slot, runtime, deployment, image, plan, lease, admission,
        )
        .await?;
        slot.context("native source opening lost its job")
    }
    async fn open_retained(
        slot: &mut Option<Self>,
        runtime: Arc<dyn RuntimeManager>,
        deployment: &str,
        image: &str,
        plan: &SourceQuarantinePlan,
        lease: ControllerLease,
        admission: Option<Arc<dyn ResourceAdmission>>,
    ) -> Result<()> {
        Self::open_retained_mode(
            slot,
            runtime,
            (deployment, None),
            image,
            plan,
            lease,
            admission,
        )
        .await
    }
    async fn open_retained_mode(
        slot: &mut Option<Self>,
        runtime: Arc<dyn RuntimeManager>,
        workspace: (&str, Option<Digest>),
        image: &str,
        plan: &SourceQuarantinePlan,
        lease: ControllerLease,
        admission: Option<Arc<dyn ResourceAdmission>>,
    ) -> Result<()> {
        let (deployment, cache_key) = workspace;
        if !valid_deployment(deployment)
            || !image_is_pinned(image)
            || slot.is_some()
            || !lease.matches_directory(lease.directory())
        {
            bail!("source quarantine lease does not match its trusted configuration");
        }
        let lease = Arc::new(lease);
        #[cfg(unix)]
        image_reference::require_absent(&lease)?;
        #[cfg(unix)]
        super::archive::ImageExportStaging::cleanup_idle(lease.clone()).await?;
        runtime
            .cleanup_idle_marketplace_build_output(&lease.storage_namespace()?)
            .await?;
        if cache_key.is_none() {
            cache::evict(runtime.as_ref(), &lease).await?;
        }
        let features = timeout(Duration::from_secs(10), runtime.image_labels(image))
            .await
            .context("source verifier image inspection deadline")??;
        if features
            .get("elixir.marketplace.worker.source_match")
            .map(String::as_str)
            != Some("2")
            || features
                .get("elixir.marketplace.worker.native_deadline")
                .map(String::as_str)
                != Some("1")
        {
            bail!(
                "source verifier image lacks the source summary or independent deadline contract"
            );
        }
        let mut volume = QuarantineMarketplaceVolumeSpec {
            name: format!("elixir-marketplace-source-{deployment}"),
            image: image.to_owned(),
            memory_limit_mb: plan.limits.memory_bytes / MIB,
            cpu_millicores: plan.limits.cpu_millicores,
            workspace: plan.workspace.clone(),
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.to_owned()),
                (
                    "elixir.marketplace.volume".to_owned(),
                    "quarantine-v1".to_owned(),
                ),
            ]),
        };
        if let Some(key) = cache_key {
            cache::label(&mut volume.labels, key);
        }
        *slot = Some(Self {
            plan: plan.clone(),
            volume,
            deployment: deployment.to_owned(),
            completed: vec![],
            input_used: 0,
            output_used: plan.initial_output,
            expires: Instant::now() + Duration::from_secs(plan.workspace.timeout_seconds),
            _lease: lease,
            activity: None,
            runtime: runtime.clone(),
            launch: None,
            failed_launch: None,
            uncertain_start: false,
            admission,
            cached_measurement: None,
            provisioned: false,
            native_attempted: false,
            peer_transfer: None,
            provider_lookup: None,
            export_reserved: None,
        });
        let job = slot
            .as_mut()
            .context("native source opening lost its owner")?;
        timeout(
            Duration::from_secs(plan.workspace.timeout_seconds.min(180)),
            async {
                job.recover_writer(runtime.as_ref()).await?;
                job.provision(job.expires.min(Instant::now() + Duration::from_secs(180)))
                    .await
            },
        )
        .await
        .context("source quarantine startup deadline exceeded")??;
        Ok(())
    }

    async fn stop_writer(runtime: &dyn RuntimeManager, handle: &ContainerHandle) -> Result<()> {
        timeout(Duration::from_secs(20), async {
            // `inspect` accepts an immutable identity via its name argument too.
            let identity = ContainerHandle {
                id: handle.id.clone(),
                name: handle.id.clone(),
            };
            runtime
                .stop_quarantine_marketplace_writer(&identity)
                .await?;
            let observed = runtime.inspect(&identity).await?;
            if observed.id != handle.id || observed.running {
                bail!("source quarantine writer is still running");
            }
            Ok(())
        })
        .await
        .context("stopping source quarantine writer timed out")?
    }
    async fn recover_writer(&self, runtime: &dyn RuntimeManager) -> Result<()> {
        let name = writer_name(&self.deployment);
        if let Some(handle) = runtime.get_container_handle(&name).await? {
            let state = runtime
                .describe_container_runtime_state(&handle.id)
                .await?
                .context("source writer ownership unavailable")?;
            if state.name != name
                || state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                || state
                    .labels
                    .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    .map(String::as_str)
                    != Some(self.deployment.as_str())
                || state
                    .labels
                    .get("elixir.marketplace.isolation")
                    .map(String::as_str)
                    != Some("quarantine-v1")
            {
                bail!("refusing to stop an unowned source writer");
            }
            Self::stop_writer(runtime, &handle).await?;
        }
        Ok(())
    }

    fn profile(&self, descriptor: &QuarantineDescriptor) -> Result<ContainerSpec> {
        let mut spec = ContainerSpec {
            name: writer_name(&self.deployment),
            image: self.volume.image.clone(),
            network: String::new(),
            network_mode: Some("none".to_owned()),
            aliases: vec![],
            env: vec![],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::new(),
            command: vec![
                "--quarantine".to_owned(),
                String::from_utf8(canonical::to_vec(descriptor)?)?,
            ],
            cap_add: vec![],
            cap_drop: vec![],
            devices: vec![],
            sysctls: HashMap::new(),
            security: Default::default(),
        };
        prepare_source_worker(&mut spec, &self.plan.limits)?;
        spec.security.tmpfs.retain(|mount| mount.path == "/tmp");
        spec.security.file_size_limit_bytes = Some(descriptor.blob.size);
        spec.security.stdin_open = true;
        spec.volumes.push(VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: self.volume.name.clone(),
            container_path: "/quarantine".to_owned(),
            read_only: false,
        });
        spec.labels = self.volume.labels.clone();
        spec.labels.insert(
            "elixir.marketplace.isolation".to_owned(),
            "quarantine-v1".to_owned(),
        );
        spec.labels.insert(
            ELIXIR_EXTENSION_ID_LABEL.to_owned(),
            "elixir-marketplace-source".to_owned(),
        );
        spec.labels.insert(
            ELIXIR_INSTANCE_ID_LABEL.to_owned(),
            format!("marketplace-source-{}", self.deployment),
        );
        apply_container_spec_fingerprint(&mut spec);
        Ok(spec)
    }

    fn source_match_profile(&self) -> Result<ContainerSpec> {
        let mut spec =
            self.profile(&self.plan.workspace.descriptor(QuarantinePurpose::Package)?)?;
        spec.command = vec![
            "--source-match".to_owned(),
            String::from_utf8(canonical::to_vec(&self.plan.source_match)?)?,
        ];
        spec.security.file_size_limit_bytes = Some(1);
        spec.volumes[0].read_only = true;
        apply_container_spec_fingerprint(&mut spec);
        Ok(spec)
    }

    /// Raw bytes from an already authorized, metered transport. One operation
    /// at a time, no queue, no path/command parsing. The trusted controller must
    /// abort on lost transfer/job authority; future cancellation drops stdin and
    /// kills the attachment. The native orphan deadline remains independent.
    pub async fn receive(
        &mut self,
        runtime: &dyn RuntimeManager,
        purpose: QuarantinePurpose,
        input: &mut (impl AsyncRead + Unpin),
    ) -> Result<()> {
        self.receive_with(runtime, purpose, async { Ok(input) })
            .await
    }
    async fn receive_with<I: AsyncRead + Unpin>(
        &mut self,
        runtime: &dyn RuntimeManager,
        purpose: QuarantinePurpose,
        input: impl std::future::Future<Output = Result<I>>,
    ) -> Result<()> {
        self.require_runtime(runtime)?;
        let descriptor = self.plan.workspace.descriptor(purpose)?;
        self.completed.retain(|p| *p != purpose);
        if Instant::now() >= self.expires {
            bail!("source quarantine job has expired");
        }
        let expires = self.expires;
        let operation = async {
            self.cleanup_activity(runtime).await?;
            self.recover_writer(runtime).await?;
            let spec = self.profile(&descriptor)?;
            let handle = self.launch_writer(&spec).await?;
            let result = async {
                let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
                let mut channel = SourceChannel::new(process)?;
                channel.marker(JOB_START).await?;
                match channel.event().await? {
                    QuarantineEvent::Ready {
                        descriptor: ready, ..
                    } if ready.same_job(&descriptor) => {}
                    _ => bail!("source quarantine did not become ready for this descriptor"),
                }
                inspect_quarantine_worker(
                    runtime,
                    &handle,
                    &spec,
                    &self.plan.limits,
                    &self.volume.name,
                    descriptor.blob.size,
                    false,
                )
                .await?;
                let mut input = input.await?;
                let mut buffer = vec![0u8; 64 * 1024].into_boxed_slice();
                let mut count = 0u64;
                loop {
                    let available = self
                        .plan
                        .input_limit
                        .saturating_sub(self.input_used)
                        .min(buffer.len() as u64) as usize;
                    if available == 0 {
                        bail!("source job input allowance exhausted");
                    }
                    let size = timeout(
                        source_input_idle(count),
                        input.read(&mut buffer[..available]),
                    )
                    .await
                    .context("source input idle deadline exceeded")??;
                    if size == 0 {
                        break;
                    }
                    charge_bytes(&mut self.input_used, size as u64, self.plan.input_limit)?;
                    count = count
                        .checked_add(size as u64)
                        .context("source byte counter overflow")?;
                    if count > descriptor.blob.size {
                        bail!("source stream exceeds its signed size");
                    }
                    // Reserve before the write: cancellation/partial writes
                    // never refund bytes that may already have reached disk.
                    charge_bytes(&mut self.output_used, size as u64, self.plan.output_limit)?;
                    timeout(
                        Duration::from_secs(10),
                        channel
                            .input
                            .as_mut()
                            .context("source input is closed")?
                            .write_all(&buffer[..size]),
                    )
                    .await
                    .context("source sink idle deadline exceeded")??;
                }
                if count != descriptor.blob.size {
                    bail!("source stream is shorter than its signed size");
                }
                // Only after upstream EOF and its exact length check may this
                // controller finish the private body. Docker attachment EOF
                // itself is not a reliable detached-container boundary.
                channel.marker(JOB_END).await?;
                // Keep the attachment open through the final reply. Closing
                // stdin can detach Docker's client before stdout is drained.
                match channel.event().await? {
                    QuarantineEvent::Complete {
                        descriptor: complete,
                        integrity_only: true,
                        ..
                    } if complete.same_job(&descriptor) => {}
                    _ => bail!("source quarantine did not verify these exact bytes"),
                }
                let mut trailing = [0u8; 1];
                if timeout(Duration::from_secs(10), channel.output.read(&mut trailing)).await?? != 0
                {
                    bail!("unexpected source quarantine output");
                }
                if !timeout(Duration::from_secs(10), channel.child.wait())
                    .await??
                    .success()
                {
                    bail!("source quarantine exited unsuccessfully");
                }
                timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                    .await
                    .context("source diagnostics did not close")???;
                runtime
                    .confirm_quarantine_marketplace_success(&handle, &spec.labels)
                    .await?;
                Ok(())
            }
            .await;
            // Stop by immutable ID and prove it stopped BEFORE another purpose
            // or any read-only verifier can use the workspace.
            self.cleanup_activity(runtime).await?;
            result
        };
        let result = timeout_at(expires, operation)
            .await
            .context("source quarantine job deadline exceeded")
            .and_then(|r| r);
        if result.is_err() {
            // A cancelled future leaves its durable activity fenced. Confirmed
            // immutable identities can recover; an uncertain startup cannot
            // release the global lane merely because a name lookup is empty.
            timeout(Duration::from_secs(25), self.recover_writer(runtime))
                .await
                .context("source writer cleanup timed out")??;
            self.cleanup_activity(runtime).await?;
        }
        result?;
        self.completed.push(purpose);
        Ok(())
    }

    pub async fn finish(mut self, runtime: &dyn RuntimeManager) -> Result<QuarantinedSource> {
        self.inspect_finished(runtime).await?;
        Ok(QuarantinedSource { job: self })
    }
    pub(super) async fn finish_retained(slot: &mut Option<Self>) -> Result<QuarantinedSource> {
        let job = slot.as_mut().context("source workspace owner is missing")?;
        let runtime = job.runtime.clone();
        job.inspect_finished(runtime.as_ref()).await?;
        Ok(QuarantinedSource {
            job: slot.take().expect("borrowed workspace owner"),
        })
    }
    async fn inspect_finished(&mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.require_runtime(runtime)?;
        for descriptor in self.plan.workspace.descriptors()? {
            if !self.completed.contains(&descriptor.purpose) {
                bail!("source workspace is incomplete");
            }
        }
        timeout_at(
            self.expires.min(Instant::now() + Duration::from_secs(30)),
            async {
                self.recover_writer(runtime).await?;
                self.cleanup_activity(runtime).await?;
                self.provision(self.expires.min(Instant::now() + Duration::from_secs(30)))
                    .await
            },
        )
        .await
        .context("source quarantine final inspection timed out")??;
        Ok(())
    }
    /// Stop owned work without deleting or resetting any quarantine bytes.
    pub async fn cancel(mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.require_runtime(runtime)?;
        self.cleanup_lookup().await?;
        self.cleanup_peer().await?;
        timeout(Duration::from_secs(25), self.cleanup_activity(runtime))
            .await
            .context("native source startup cancellation is still pending")??;
        timeout(Duration::from_secs(25), self.recover_writer(runtime))
            .await
            .context("source cancellation timed out")??;
        self.cleanup_activity(runtime).await
    }
    /// Explicit controller-owned disposal, never a peer RPC or automatic repair
    /// of corrupt data. Removing the stopped immutable container first releases
    /// its volume reference. The OS job lease spans the entire deletion.
    pub async fn discard(mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.discard_in_place(runtime).await
    }
    async fn discard_in_place(&mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.retire_in_place(runtime, true).await
    }
    async fn retire_in_place(
        &mut self,
        runtime: &dyn RuntimeManager,
        remove_volume: bool,
    ) -> Result<()> {
        self.require_runtime(runtime)?;
        self.cleanup_lookup().await?;
        self.cleanup_peer().await?;
        timeout(Duration::from_secs(30), async {
            self.cleanup_activity(runtime).await?;
            self.recover_writer(runtime).await?;
            Self::remove_stopped_writer(runtime, &self.deployment).await?;
            #[cfg(unix)]
            image_reference::recover(runtime,&self._lease).await?;
            runtime
                .cleanup_idle_marketplace_build_output(&self._lease.storage_namespace()?)
                .await?;
            if remove_volume {
                runtime
                    .remove_private_file_volume(&self.volume.name, &self.volume.labels)
                    .await?;
            }
            #[cfg(unix)]
            super::archive::ImageExportStaging::cleanup_idle(self._lease.clone()).await?;
            Ok(())
        })
        .await
        .context("source discard deadline exceeded")?
    }
    async fn remove_stopped_writer(runtime: &dyn RuntimeManager, deployment: &str) -> Result<()> {
        let name = writer_name(deployment);
        if let Some(handle) = runtime.get_container_handle(&name).await? {
            let state = runtime
                .describe_container_runtime_state(&handle.id)
                .await?
                .context("source discard ownership unavailable")?;
            let observed = runtime
                .inspect(&ContainerHandle {
                    id: handle.id.clone(),
                    name: handle.id.clone(),
                })
                .await?;
            if state.name != name
                || observed.id != handle.id
                || observed.name != name
                || observed.running
                || state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                || state
                    .labels
                    .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    .map(String::as_str)
                    != Some(deployment)
                || state
                    .labels
                    .get("elixir.marketplace.isolation")
                    .map(String::as_str)
                    != Some("quarantine-v1")
            {
                bail!("refusing to discard a live or unowned source container");
            }
            runtime
                .remove_container(&ContainerHandle {
                    id: handle.id.clone(),
                    name: handle.id.clone(),
                })
                .await?;
            if runtime.get_container_handle(&handle.id).await?.is_some() {
                bail!("source container remains after removal");
            }
        }
        Ok(())
    }
}

struct DiagnosticDrain(tokio::task::JoinHandle<Result<()>>);
impl Drop for DiagnosticDrain {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct SourceChannel {
    child: tokio::process::Child,
    input: Option<tokio::process::ChildStdin>,
    output: tokio::process::ChildStdout,
    diagnostics: DiagnosticDrain,
}
impl SourceChannel {
    async fn marker(&mut self, marker: &[u8; 8]) -> Result<()> {
        timeout(
            Duration::from_secs(10),
            self.input
                .as_mut()
                .context("source input is closed")?
                .write_all(marker),
        )
        .await
        .context("source marker deadline exceeded")??;
        Ok(())
    }
    fn new(mut process: PrivateProcessStdio) -> Result<Self> {
        let mut error = process
            .diagnostics
            .take()
            .context("source diagnostics unavailable")?;
        let diagnostics = DiagnosticDrain(tokio::spawn(async move {
            let mut bytes = 0;
            let mut buffer = [0u8; 1024];
            loop {
                let size = error.read(&mut buffer).await?;
                if size == 0 {
                    return Ok(());
                }
                bytes += size;
                if bytes > 64 * 1024 {
                    bail!("source diagnostic budget exceeded");
                }
            }
        }));
        Ok(Self {
            child: process.child,
            input: Some(process.input),
            output: process.output,
            diagnostics,
        })
    }
    async fn event(&mut self) -> Result<QuarantineEvent> {
        Ok(QuarantineEvent::parse(
            &self.frame(Duration::from_secs(10), 4096).await?,
        )?)
    }
    async fn frame(&mut self, wait: Duration, maximum: usize) -> Result<Vec<u8>> {
        timeout(wait, async {
            let length = self.output.read_u32().await?;
            let size = elixir_marketplace::wire::control_frame_length(length, maximum)
                .map_err(|_| anyhow::anyhow!("invalid source event length"))?;
            let mut bytes = vec![0; size];
            timeout(Duration::from_secs(10), self.output.read_exact(&mut bytes))
                .await
                .context("source event body deadline exceeded")??;
            Ok(bytes)
        })
        .await
        .context("source event deadline exceeded")?
    }
}
impl Drop for SourceChannel {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[cfg(all(test, unix))]
pub(in crate::marketplace) mod peer_tests;
#[cfg(test)]
mod recovery_tests;

#[cfg(all(test, unix))]
mod launch_tests;

#[cfg(test)]
pub(in crate::marketplace) mod tests {
    use super::*;
    use crate::runtime::docker::DockerRuntimeManager;
    use base64::Engine as _;
    use elixir_marketplace::{
        records::{Publisher, Release, SignedRecord},
        security::AuthorizedPublisher,
    };

    #[test]
    fn source_input_idle_covers_one_cold_transport_wait_then_short_body_reads() {
        assert_eq!(source_input_idle(0), Duration::from_secs(370));
        assert_eq!(source_input_idle(1), Duration::from_secs(10));
        assert_eq!(source_input_idle(u64::MAX), Duration::from_secs(10));
    }

    pub(in crate::marketplace) fn fixture() -> Result<(AuthorizedRelease, Vec<u8>, Vec<u8>)> {
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))?;
        decode_fixture(value)
    }
    pub(in crate::marketplace) fn decode_fixture(
        value: serde_json::Value,
    ) -> Result<(AuthorizedRelease, Vec<u8>, Vec<u8>)> {
        let publisher =
            SignedRecord::<Publisher>::parse(&serde_json::to_vec(&value["publisher"])?)?;
        let publisher = AuthorizedPublisher::verify(&publisher)?;
        let release = SignedRecord::<Release>::parse(&serde_json::to_vec(&value["release"])?)?;
        Ok((
            AuthorizedRelease::verify(&release, &publisher, &publisher)?,
            base64::engine::general_purpose::STANDARD.decode(
                value["package_base64"]
                    .as_str()
                    .context("package fixture")?,
            )?,
            base64::engine::general_purpose::STANDARD.decode(
                value["source_proof_base64"]
                    .as_str()
                    .context("proof fixture")?,
            )?,
        ))
    }
    pub(in crate::marketplace) async fn operational_fixture()
    -> Result<(AuthorizedRelease, Vec<u8>, Vec<u8>)> {
        operational_fixture_source(None).await
    }
    pub(in crate::marketplace) async fn operational_fixture_with_source(
        source: &str,
    ) -> Result<(AuthorizedRelease, Vec<u8>, Vec<u8>)> {
        if source.len() > 8 * 1024 {
            bail!("oversized operational test source");
        }
        operational_fixture_source(Some(source)).await
    }
    async fn operational_fixture_source(
        source: Option<&str>,
    ) -> Result<(AuthorizedRelease, Vec<u8>, Vec<u8>)> {
        // Keep the cryptographic golden fixture unchanged. The actual handoff
        // uses a separate SDK-generated operational module, never schema repair.
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../extensions/sdk/marketplace/test/fixture.mjs");
        let mut command = tokio::process::Command::new("node");
        command.args(["--input-type=module", "-e", "import{pathToFileURL}from'node:url';const{createFixture}=await import(pathToFileURL(process.argv[1]));const source=process.argv[2];process.stdout.write(JSON.stringify(createFixture({operational:true,...(source===undefined?{}:{sourceBytes:Buffer.from(source)})})));" ]).arg(path).kill_on_drop(true);
        if let Some(source) = source {
            command.arg(source);
        }
        let output = timeout(Duration::from_secs(10), command.output()).await??;
        if !output.status.success() || output.stdout.len() > 64 * 1024 {
            bail!("SDK operational fixture generation failed");
        }
        decode_fixture(serde_json::from_slice(&output.stdout)?)
    }
    #[test]
    fn native_publication_blob_plan_matches_authorized_release_without_forging_install_authority()
    -> Result<()> {
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))?;
        let publication = serde_json::from_value(serde_json::json!({"schema_version":1,
            "publisher":value["publisher"],"release":value["release"],"head":value["head"]}))?;
        let (observation, healthy) =
            super::super::worker::PublicationObservation::fixture_for_verified_publication(
                publication,
            )?;
        let (release, _, _) = fixture()?;
        let from_native = SourceQuarantinePlan::for_publication(&observation, None, &budget())?;
        let from_core = SourceQuarantinePlan::for_release(&release, None, &budget())?;
        assert_eq!(
            canonical::to_vec(from_native.workspace())?,
            canonical::to_vec(from_core.workspace())?
        );
        assert!(SourceQuarantinePlan::for_publication(&observation, Some(99), &budget()).is_err());
        healthy(false);
        assert!(SourceQuarantinePlan::for_publication(&observation, None, &budget()).is_err());
        Ok(())
    }

    pub(in crate::marketplace) fn budget() -> SourceJobBudget {
        SourceJobBudget {
            memory_bytes: 64 * MIB,
            cpu_millicores: 100,
            disk_bytes: 128 * MIB,
            input_bytes: MIB,
            output_bytes: 128 * MIB,
            wall_seconds: 120,
        }
    }

    // Real SDK bytes/Git verification but no native process or live public
    // origin: the observation owner below is explicitly a unit-test fixture.
    pub(super) async fn matched_fixture(directory: &Path) -> Result<SourceMatchedQuarantine> {
        let (release, package, proof) = operational_fixture().await?;
        let plan = SourceQuarantinePlan::for_release(
            &release,
            None,
            &SourceJobBudget {
                wall_seconds: 600,
                ..budget()
            },
        )?;
        let summary = plan
            .source_match
            .verify(std::io::Cursor::new(package), std::io::Cursor::new(proof))?;
        let lease = ControllerLease::acquire(directory)?;
        Ok(SourceMatchedQuarantine {
            source: QuarantinedSource {
                job: SourceQuarantineJob {
                    deployment: "fixture".into(),
                    completed: vec![],
                    input_used: 123,
                    output_used: 456,
                    expires: Instant::now() + Duration::from_secs(600),
                    _lease: Arc::new(lease),
                    activity: None,
                    runtime: Arc::new(DockerRuntimeManager::new(None)),
                    launch: None,
                    failed_launch: None,
                    uncertain_start: false,
                    admission: None,
                    cached_measurement: None,
                    provisioned: true,
                    native_attempted: true,
                    peer_transfer: None,
                    provider_lookup: None,
                    export_reserved: None,
                    volume: QuarantineMarketplaceVolumeSpec {
                        name: "elixir-marketplace-source-fixture".into(),
                        image: format!("sha256:{}", "1".repeat(64)),
                        workspace: plan.workspace.clone(),
                        memory_limit_mb: 64,
                        cpu_millicores: 100,
                        labels: HashMap::new(),
                    },
                    plan,
                },
            },
            summary,
            public_source: None,
            disposed: false,
            #[cfg(unix)]
            retained_build: None,
        })
    }
    #[tokio::test]
    async fn admission_resume_cannot_reissue_a_provisioned_or_uncertain_native_job() -> Result<()> {
        let root = tempfile::tempdir()?;
        let lane = root.path().canonicalize()?.join("expensive");
        let mut matched = matched_fixture(&lane).await?;
        let job = &mut matched.source.job;
        let original = (job.input_used, job.output_used, job.expires);
        assert!(job.resume_unstarted_retained(job.expires).await.is_err());
        job.provisioned = false;
        job.native_attempted = false;
        job.uncertain_start = true;
        assert!(job.resume_unstarted_retained(job.expires).await.is_err());
        assert_eq!(original, (job.input_used, job.output_used, job.expires));
        assert!(ControllerLease::acquire(&lane).is_err());
        Ok(())
    }
    #[tokio::test]
    async fn deferred_export_reuses_only_its_unstarted_reservation_without_refunding_or_double_charging()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let private = root.path().canonicalize()?.join("expensive");
        let mut matched = matched_fixture(&private).await?;
        let job = &mut matched.source.job;
        let original = (job.input_used, job.output_used, job.expires);
        let size = job.plan.workspace.package.size;
        job.reserve_export(QuarantinePurpose::Package)?;
        let reserved = (job.input_used, job.output_used);
        assert_eq!(
            reserved,
            (original.0 + size + 1, original.1 + size + 4096 + 64 * 1024)
        );
        job.reserve_export(QuarantinePurpose::Package)?;
        assert_eq!((job.input_used, job.output_used), reserved);
        assert!(job.reserve_export(QuarantinePurpose::Proof).is_err());
        assert_eq!((job.input_used, job.output_used), reserved);
        job.export_reserved = None; // the actual launch consumes this in production
        job.reserve_export(QuarantinePurpose::Package)?;
        assert_eq!(
            (job.input_used, job.output_used),
            (reserved.0 + size + 1, reserved.1 + size + 4096 + 64 * 1024)
        );
        assert_eq!(job.expires, original.2);
        Ok(())
    }
    #[tokio::test]
    async fn public_license_binding_survives_manifest_handoff_expires_and_refreshes_without_resetting_the_job()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let matched = matched_fixture(&directory).await?;
        tokio::time::pause();
        assert!(matched.require_public_source().is_err());
        let id = matched.summary.release_id;
        let license = matched.summary.license.sha256;
        let expires = matched.expires();
        let (observation, _owner) =
            super::super::worker::PublicSourceObservation::fixture_for_source(id, license);
        let mut checked = matched.bind_public_source(observation)?.check_manifest()?;
        checked.source().require_public_source()?;
        assert_eq!(checked.source().expires(), expires);
        assert!(ControllerLease::acquire(&directory).is_err());
        tokio::time::advance(Duration::from_secs(301)).await;
        assert!(checked.source().ensure_active().is_err());
        let (fresh, owner) =
            super::super::worker::PublicSourceObservation::fixture_for_source(id, license);
        checked.refresh_public_source(fresh)?;
        checked.source().ensure_active()?;
        assert_eq!(checked.source().expires(), expires);
        assert_eq!(
            (
                checked.source().source.job.input_used,
                checked.source().source.job.output_used
            ),
            (123, 456)
        );
        owner(false);
        assert!(checked.source().ensure_active().is_err());
        drop(checked);
        assert!(ControllerLease::acquire(&directory).is_ok());
        Ok(())
    }
    #[tokio::test]
    async fn wrong_license_release_or_failed_protection_cannot_bind_a_git_matched_package()
    -> Result<()> {
        for case in ["license", "release", "protection"] {
            let root = tempfile::tempdir()?;
            let directory = root.path().canonicalize()?.join("expensive");
            let matched = matched_fixture(&directory).await?;
            let id = if case == "release" {
                Digest::of(b"wrong release")
            } else {
                matched.summary.release_id
            };
            let license = if case == "license" {
                Digest::of(b"wrong license")
            } else {
                matched.summary.license.sha256
            };
            let (observation, owner) =
                super::super::worker::PublicSourceObservation::fixture_for_source(id, license);
            if case == "protection" {
                owner(false);
            }
            assert!(matched.bind_public_source(observation).is_err(), "{case}");
            assert!(ControllerLease::acquire(&directory).is_ok());
        }
        Ok(())
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn retained_transitions_preserve_workspace_lease_counters_and_deadline_on_failure()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let matched = matched_fixture(&directory).await?;
        let mut source = Some(matched);
        let expires = source.as_ref().unwrap().expires();
        // Offline equality does not imply a public-source binding. A rejected
        // manifest transition MUST leave the source owner in its original slot.
        assert!(SourceMatchedQuarantine::check_manifest_retained(&mut source).is_err());
        assert!(source.is_some());
        assert!(ControllerLease::acquire(&directory).is_err());
        let source = source.take().unwrap();
        let mut job = source.source.job;
        job.runtime = Arc::new(super::peer_tests::Unavailable);
        let mut slot = Some(job);
        // Incomplete receipt fails before native work; no consumed ownership.
        assert!(
            SourceQuarantineJob::finish_retained(&mut slot)
                .await
                .is_err()
        );
        let job = slot.as_ref().unwrap();
        assert_eq!(
            (job.input_used, job.output_used, job.expires),
            (123, 456, expires)
        );
        assert!(ControllerLease::acquire(&directory).is_err());
        // Runtime loss before a source verifier starts retains the SAME job;
        // no fresh counters, deadline or expensive-job lane are fabricated.
        let mut source = Some(QuarantinedSource {
            job: slot.take().unwrap(),
        });
        assert!(
            QuarantinedSource::match_retained(&mut source)
                .await
                .is_err()
        );
        let job = &source.as_ref().unwrap().job;
        assert_eq!(
            (job.input_used, job.output_used, job.expires),
            (123, 456, expires)
        );
        assert!(Fence::load(&job._lease)?.is_none());
        assert!(ControllerLease::acquire(&directory).is_err());
        drop(source);
        assert!(ControllerLease::acquire(&directory).is_ok());
        Ok(())
    }

    #[test]
    fn byte_allowances_are_aggregate_across_retries_and_cannot_overflow_or_refund() {
        let mut used = 5;
        charge_bytes(&mut used, 3, 10).unwrap();
        assert_eq!(used, 8);
        assert!(charge_bytes(&mut used, 3, 10).is_err());
        assert_eq!(used, 8);
        charge_bytes(&mut used, 2, 10).unwrap();
        assert_eq!(used, 10);
        assert!(charge_bytes(&mut used, 1, 10).is_err());
        let mut used = u64::MAX - 1;
        assert!(charge_bytes(&mut used, 2, u64::MAX).is_err());
        assert_eq!(used, u64::MAX - 1);
    }

    #[tokio::test]
    async fn closing_input_really_delivers_eof_without_closing_the_private_output() -> Result<()> {
        use std::process::Stdio;
        let (release, _, _) = fixture()?;
        let plan = SourceQuarantinePlan::for_release(&release, None, &budget())?;
        let event = QuarantineEvent::Ready {
            schema_version: 1,
            descriptor: plan.workspace.descriptor(QuarantinePurpose::Package)?,
        };
        let bytes = base64::engine::general_purpose::STANDARD.encode(canonical::to_vec(&event)?);
        let script = "process.stdin.resume();process.stdin.on('end',()=>{const b=Buffer.from(process.argv[1],'base64');const h=Buffer.alloc(4);h.writeUInt32BE(b.length);process.stdout.write(h);process.stdout.write(b);});";
        let mut child = tokio::process::Command::new("node")
            .args(["-e", script, &bytes])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let process = PrivateProcessStdio {
            input: child.stdin.take().context("fixture input")?,
            output: child.stdout.take().context("fixture output")?,
            diagnostics: child.stderr.take(),
            child,
        };
        let mut channel = SourceChannel::new(process)?;
        drop(channel.input.take());
        assert!(matches!(
            timeout(Duration::from_secs(5), channel.event()).await??,
            QuarantineEvent::Ready { .. }
        ));
        assert!(
            timeout(Duration::from_secs(5), channel.child.wait())
                .await??
                .success()
        );
        Ok(())
    }

    #[tokio::test]
    async fn private_frame_reader_accepts_its_protocol_limit_without_borrowing_source_eval_limits()
    -> Result<()> {
        use elixir_marketplace::container_image::filesystem::job::{
            ContainerFilesystemEvent, MAX_EVENT_BYTES,
        };
        use std::process::Stdio;
        let job_id = Digest::of(b"private filesystem frame regression");
        let event = ContainerFilesystemEvent::Ready {
            schema_version: 1,
            job_id,
        };
        let bytes = base64::engine::general_purpose::STANDARD.encode(canonical::to_vec(&event)?);
        let script = "const b=Buffer.from(process.argv[1],'base64');const h=Buffer.alloc(4);h.writeUInt32BE(b.length);process.stdout.write(h);process.stdout.write(b);";
        let mut child = tokio::process::Command::new("node")
            .args(["-e", script, &bytes])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let process = PrivateProcessStdio {
            input: child.stdin.take().context("fixture stdin")?,
            output: child.stdout.take().context("fixture stdout")?,
            diagnostics: child.stderr.take(),
            child,
        };
        let mut channel = SourceChannel::new(process)?;
        let bytes = channel.frame(Duration::from_secs(5), MAX_EVENT_BYTES).await?;
        assert!(matches!(ContainerFilesystemEvent::parse(&bytes)?,
            ContainerFilesystemEvent::Ready { job_id: actual, .. } if actual == job_id));
        assert!(timeout(Duration::from_secs(5), channel.child.wait()).await??.success());
        timeout(Duration::from_secs(1), &mut channel.diagnostics.0).await???;
        Ok(())
    }

    #[test]
    fn plan_requires_sufficient_separate_positive_resources_and_only_signed_artifacts() -> Result<()>
    {
        let (release, package, proof) = fixture()?;
        let plan = SourceQuarantinePlan::for_release(&release, None, &budget())?;
        assert_eq!(plan.workspace.package.size, package.len() as u64);
        assert_eq!(plan.workspace.proof.size, proof.len() as u64);
        assert_eq!(plan.disk_reservation_bytes(), 43 * MIB + 8192);
        assert_eq!(plan.limits.memory_bytes, 64 * MIB);
        assert!(SourceQuarantinePlan::for_release(&release, Some(0), &budget()).is_err());
        assert!(
            SourceQuarantinePlan::for_release(
                &release,
                None,
                &SourceJobBudget {
                    input_bytes: (package.len() + proof.len() + 2) as u64,
                    ..budget()
                }
            )
            .is_err(),
            "reserve verification reads before starting downloads"
        );
        for bad in [
            SourceJobBudget {
                memory_bytes: 0,
                ..budget()
            },
            SourceJobBudget {
                memory_bytes: 63 * MIB,
                ..budget()
            },
            SourceJobBudget {
                cpu_millicores: 0,
                ..budget()
            },
            SourceJobBudget {
                disk_bytes: plan.disk_reservation_bytes() - 1,
                ..budget()
            },
            SourceJobBudget {
                output_bytes: 0,
                ..budget()
            },
            SourceJobBudget {
                input_bytes: package.len() as u64,
                ..budget()
            },
            SourceJobBudget {
                wall_seconds: 0,
                ..budget()
            },
            SourceJobBudget {
                wall_seconds: MAX_JOB_SECONDS + 1,
                ..budget()
            },
        ] {
            assert!(SourceQuarantinePlan::for_release(&release, None, &bad).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn receiver_profile_grants_one_volume_no_network_and_the_selected_files_own_limit()
    -> Result<()> {
        let (release, package, proof) = fixture()?;
        let plan = SourceQuarantinePlan::for_release(&release, None, &budget())?;
        let directory = tempfile::tempdir()?;
        let private = directory.path().canonicalize()?.join("expensive");
        let lease = ControllerLease::acquire(&private)?;
        let mut job = SourceQuarantineJob {
            plan: plan.clone(),
            deployment: "fixture".to_owned(),
            completed: vec![],
            input_used: 0,
            output_used: plan.initial_output,
            expires: Instant::now() + Duration::from_secs(120),
            _lease: Arc::new(lease),
            activity: None,
            runtime: Arc::new(DockerRuntimeManager::new(None)),
            launch: None,
            failed_launch: None,
            uncertain_start: false,
            admission: None,
            cached_measurement: None,
            provisioned: true,
            native_attempted: true,
            peer_transfer: None,
            provider_lookup: None,
            export_reserved: None,
            volume: QuarantineMarketplaceVolumeSpec {
                name: "elixir-marketplace-source-fixture".to_owned(),
                image: format!("sha256:{}", "1".repeat(64)),
                workspace: plan.workspace.clone(),
                memory_limit_mb: 64,
                cpu_millicores: 100,
                labels: HashMap::new(),
            },
        };
        for purpose in [QuarantinePurpose::Package, QuarantinePurpose::Proof] {
            let descriptor = plan.workspace.descriptor(purpose)?;
            let spec = job.profile(&descriptor)?;
            assert_eq!(spec.network_mode.as_deref(), Some("none"));
            assert_eq!(spec.volumes.len(), 1);
            assert_eq!(spec.volumes[0].container_path, "/quarantine");
            assert_eq!(
                spec.volumes[0].source_kind,
                VolumeMountSourceKind::NamedVolume
            );
            assert_eq!(
                spec.security.file_size_limit_bytes,
                Some(descriptor.blob.size)
            );
            assert!(spec.security.stdin_open);
            assert_eq!(spec.security.memory_limit_mb, Some(64));
            assert_eq!(spec.security.memory_swap_limit_mb, Some(64));
            assert_eq!(spec.security.tmpfs.len(), 1);
            assert_eq!(spec.security.tmpfs[0].path, "/tmp");
            assert_eq!(spec.command.len(), 2);
            assert_eq!(spec.command[0], "--quarantine");
            assert!(spec.ports.is_empty() && spec.env.is_empty() && spec.cap_add.is_empty());
        }
        assert!(ControllerLease::acquire(&private).is_err());
        let mut unsupported = job.source_match_profile()?;
        unsupported.command[0] = "--unsupported-source-operation".into();
        apply_container_spec_fingerprint(&mut unsupported);
        assert!(job.begin_writer(&unsupported).is_err());
        assert!(
            job.activity.is_none(),
            "unsupported commands must not leave an ambiguous CREATE record"
        );
        assert!(Fence::load(&job._lease)?.is_none());
        struct Deny(std::sync::Mutex<Vec<bool>>);
        #[async_trait::async_trait]
        impl ResourceAdmission for Deny {
            async fn check(
                &self,
                _: &SourceQuarantinePlan,
                provisioned: bool,
                _: Option<&HostObservation>,
            ) -> Result<()> {
                self.0.lock().unwrap().push(provisioned);
                bail!("test-only resource deferral")
            }
        }
        let denied = Arc::new(Deny(Default::default()));
        job.admission = Some(denied.clone());
        let spec = job.profile(&plan.workspace.descriptor(QuarantinePurpose::Package)?)?;
        assert!(job.launch_writer(&spec).await.is_err());
        assert!(job.provision(job.expires).await.is_err());
        job.provisioned = false;
        assert!(job.provision(job.expires).await.is_err());
        assert_eq!(*denied.0.lock().unwrap(), [true, true, false]);
        assert!(job.activity.is_none() && job.launch.is_none());
        assert!(
            Fence::load(&job._lease)?.is_none(),
            "resource deferral must precede native markers/processes"
        );
        job.admission = None; // only this synthetic native-profile unit fixture
        job.provisioned = true;
        let checker = job.source_match_profile()?;
        assert!(checker.volumes[0].read_only);
        assert_eq!(checker.security.file_size_limit_bytes, Some(1));
        assert_eq!(checker.command[0], "--source-match");
        assert_eq!(checker.network_mode.as_deref(), Some("none"));
        let runtime = Arc::new(DockerRuntimeManager::new(None));
        let mut input = package.as_slice();
        let receive = job.receive(runtime.as_ref(), QuarantinePurpose::Package, &mut input);
        assert!(
            std::mem::size_of_val(&receive) < 64 * 1024,
            "stream buffers must not live in movable async frames"
        );
        drop(receive);
        let summary = plan
            .source_match
            .verify(std::io::Cursor::new(&package), std::io::Cursor::new(&proof))?;
        let mut matched = SourceMatchedQuarantine {
            source: QuarantinedSource { job },
            summary,
            public_source: None,
            disposed: false,
            #[cfg(unix)]
            retained_build: None,
        };
        assert!(
            matched.require_public_source().is_err(),
            "offline equality must never imply public availability"
        );
        let mut destination = tokio::io::sink();
        let export = matched.export_blob(
            runtime.as_ref(),
            QuarantinePurpose::Package,
            &mut destination,
        );
        assert!(
            std::mem::size_of_val(&export) < 64 * 1024,
            "archive readback must not restore a large nested async stack"
        );
        drop(export);
        drop(matched);
        assert!(ControllerLease::acquire(&private).is_ok());
        Ok(())
    }

    async fn docker(args: &[&str]) -> Result<String> {
        let output = timeout(
            Duration::from_secs(20),
            tokio::process::Command::new("docker")
                .args(args)
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() {
            bail!(
                "quarantine fixture Docker command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }
    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "explicit offline Linux Docker source-quarantine server integration"]
    async fn server_quarantines_sdk_bytes_recovers_cancelled_owner_and_never_repairs_corruption()
    -> Result<()> {
        use futures_util::FutureExt;
        let runtime = Arc::new(DockerRuntimeManager::new(None));
        let reference = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
            .unwrap_or_else(|_| "elixir-marketplace-worker:development".into());
        let image = docker(&["image", "inspect", "--format", "{{.Id}}", &reference]).await?;
        let deployment = format!("fixture-{}", uuid::Uuid::new_v4().simple());
        let volume = format!("elixir-marketplace-source-{deployment}");
        let name = writer_name(&deployment);
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
            (
                "elixir.marketplace.volume".to_owned(),
                "quarantine-v1".to_owned(),
            ),
        ]);
        let directory = tempfile::tempdir()?;
        let private = directory.path().canonicalize()?.join("expensive");
        let result=std::panic::AssertUnwindSafe(async {
            let (release,package,proof)=operational_fixture().await?;
            let plan=SourceQuarantinePlan::for_release(&release,None,&budget())?;
            let mut job=SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await?;
            assert!(SourceQuarantineJob::open(runtime.clone(),&private,"other",&image,&plan).await.is_err());
            job.receive(runtime.as_ref(),QuarantinePurpose::Package,&mut package.as_slice()).await?;
            assert!(job.receive(runtime.as_ref(),QuarantinePurpose::Proof,&mut vec![9u8;proof.len()].as_slice()).await.is_err());
            assert_eq!(job.completed,vec![QuarantinePurpose::Package]);
            assert_eq!(job.input_used,(package.len()+proof.len()) as u64);
            assert_eq!(job.output_used,plan.initial_output+(package.len()+proof.len()) as u64);
            job.receive(runtime.as_ref(),QuarantinePurpose::Proof,&mut proof.as_slice()).await?;
            let mut receiving=Some(job);
            let ready=SourceQuarantineJob::finish_retained(&mut receiving).await?;
            assert!(receiving.is_none());
            assert_eq!(ready.release_id(),release.release_id());assert_eq!(ready.volume_name(),volume);
            assert!(SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await.is_err());
            let mut verifying=Some(ready);
            let matched=QuarantinedSource::match_retained(&mut verifying).await?;
            assert!(verifying.is_none());
            assert_eq!(matched.summary().release_id,release.release_id());
            assert_eq!(matched.summary().files,3);
            assert!(SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await.is_err());
            let checked=matched.check_manifest()?;
            assert_eq!(checked.manifest().id,release.payload().extension_id.as_str());
            assert!(matches!(checked.selection(),super::super::manifest::PublicRuntimeSelection::SourceJs {..}));
            assert!(SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await.is_err());
            let database_url=if let Ok(url)=std::env::var("ELIXIR_MARKETPLACE_GRANTS_POSTGRES_URL") {
                let parsed=reqwest::Url::parse(&url)?;
                if !matches!(parsed.scheme(),"postgres"|"postgresql") || !matches!(parsed.host_str(),Some("127.0.0.1"|"[::1]"|"::1")) || parsed.path()!="/elixir_marketplace_grants_test" {
                    bail!("requires the dedicated loopback marketplace grants test database");
                }
                url
            } else { format!("sqlite://{}",directory.path().join("source-grants.sqlite").display()) };
            let database=crate::db::Database::connect(&crate::config::DatabaseConfig {
                url:database_url,max_connections:2,connect_timeout_seconds:5,
            }).await?;
            if database.driver==crate::db::DatabaseDriver::Postgres {
                let tables:i64=sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'").fetch_one(&database.pool).await?;
                if tables!=0 { bail!("PostgreSQL source grants fixture must start empty"); }
            }
            database.run_migrations().await?;
            let granted=super::super::grant_store::tests::approve_source_fixture(&database,checked).await?;
            assert_eq!(granted.grant_revision(),3);
            assert!(SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await.is_err());
            let archive_path=directory.path().canonicalize()?.join("archive");
            let archive_budget=super::super::archive::ArchiveBudget {bytes:128*MIB,releases:2};
            let archived=super::super::archive::persist(&database,runtime.as_ref(),granted,&archive_path,archive_budget).await?;
            assert_eq!(archived.release_id(),release.release_id());
            assert!(!runtime.private_file_volume_owned(&volume,&labels).await?);
            assert!(runtime.get_container_handle(&name).await?.is_none());
            let retained=super::super::archive::verify_retained(&archive_path,archive_budget,&release,&private,MIB,10).await?;
            assert_eq!(retained.release_id(),release.release_id());
            database.pool.close().await;

            let mut job=SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await?;
            // A new purpose replaces the stopped verifier container. Ignore
            // that known old identity; inspecting it concurrently with removal
            // races legitimately. The new receiver cannot advance/replace
            // itself while this test deliberately withholds the rest of stdin.
            let previous=runtime.get_container_handle(&name).await?.map(|handle|handle.id);
            let (mut sender,mut blocked)=tokio::io::duplex(1024);
            sender.write_all(&[5,6,7]).await?;
            let mut receive=Box::pin(job.receive(runtime.as_ref(),QuarantinePurpose::Package,&mut blocked));
            let old=timeout(Duration::from_secs(8),async {
                loop {tokio::select! {
                    result=&mut receive=>{result?;bail!("blocked source unexpectedly completed");},
                    _=tokio::time::sleep(Duration::from_millis(50))=>{
                        if let Some(handle)=runtime.get_container_handle(&name).await? {
                            let identity=ContainerHandle {id:handle.id.clone(),name:handle.id.clone()};
                            if Some(handle.id.as_str())!=previous.as_deref() && runtime.inspect(&identity).await?.running {
                                let command=["/usr/bin/od","-An","-N3","-tx1","/quarantine/package.blob"].map(str::to_owned);
                                if let Ok(prefix)=runtime.exec_container_command(&handle,"10004:10004",&command).await {
                                    if prefix.split_whitespace().collect::<String>()=="050607" {break Ok::<_,anyhow::Error>(handle);}
                                }
                            }
                        }
                    }
                }}
            }).await??;
            drop(receive);drop(job); // simulate lost controller during a read
            assert!(ControllerLease::acquire(&private).is_err(),"a dropped controller is not proof of native cleanup");
            assert!(SourceQuarantineJob::open(runtime.clone(),&private,"other",&image,&plan).await.is_err(),"a different deployment cannot skip interrupted work");
            assert!(SourceQuarantineJob::recover_interrupted(runtime.as_ref(),&private).await?);
            assert!(!SourceQuarantineJob::recover_interrupted(runtime.as_ref(),&private).await?);
            let mut job=SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await?;
            assert!(runtime.get_container_handle(&old.id).await?.is_none());
            job.receive(runtime.as_ref(),QuarantinePurpose::Package,&mut package.as_slice()).await?;
            job.receive(runtime.as_ref(),QuarantinePurpose::Proof,&mut proof.as_slice()).await?;
            let ready=job.finish(runtime.as_ref()).await?;

            // A previous integrity receipt is not enough: rehash the sealed
            // files in the source job, catching same-length post-download damage.
            docker(&["run","--rm","--pull=never","--network=none","--read-only","--user=10004:10004","--cap-drop=ALL",
                "--security-opt=no-new-privileges","--memory=64m","--memory-swap=64m","--cpus=0.1","--pids-limit=16","--log-driver=none",
                "--mount",&format!("type=volume,source={volume},target=/quarantine,volume-nocopy"),"--entrypoint=/usr/local/bin/node",&image,
                "-e","const fs=require('node:fs');const fd=fs.openSync('/quarantine/proof.blob','r+');fs.writeSync(fd,Buffer.from([9]),0,1,0);fs.fsyncSync(fd);fs.closeSync(fd)"]).await?;
            assert!(ready.match_source(runtime.as_ref()).await.is_err());

            // Deliberate corruption ONLY in this disposable test volume, with
            // the owned writer confirmed stopped. Never silently reinitialize.
            docker(&["run","--rm","--pull=never","--network=none","--read-only","--user=10004:10004","--cap-drop=ALL",
                "--security-opt=no-new-privileges","--memory=64m","--memory-swap=64m","--cpus=0.1","--pids-limit=16","--log-driver=none",
                "--mount",&format!("type=volume,source={volume},target=/quarantine,volume-nocopy"),"--entrypoint=/usr/local/bin/node",&image,
                "-e","require('node:fs').truncateSync('/quarantine/package.blob',0)"]).await?;
            assert!(SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await.is_err());
            let size=docker(&["run","--rm","--pull=never","--network=none","--read-only","--user=10004:10004","--cap-drop=ALL",
                "--memory=64m","--memory-swap=64m","--cpus=0.1","--pids-limit=16","--log-driver=none",
                "--mount",&format!("type=volume,source={volume},target=/quarantine,volume-nocopy,readonly"),"--entrypoint=/usr/local/bin/node",&image,
                "-e","process.stdout.write(String(require('node:fs').statSync('/quarantine/package.blob').size))"]).await?;
            assert_eq!(size,"0");
            println!("source/grant/archive/recovery lifecycle passed: {image}, 64 MiB/no swap, 0.1 core, offline SDK bytes; not public source/runtime/install authority");
            Ok::<_,anyhow::Error>(())
        }).catch_unwind().await;
        // Ownership-checked cleanup runs even when a fixture assertion panics.
        for target in [&name, &format!("{volume}-provision")] {
            if let Some(handle) = runtime.get_container_handle(target).await? {
                let state = runtime
                    .describe_container_runtime_state(&handle.id)
                    .await?
                    .context("fixture cleanup inspection unavailable")?;
                if state.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) != Some(&deployment)
                    || state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                {
                    bail!("quarantine fixture cleanup ownership mismatch");
                }
                runtime.remove_container(&handle).await?;
            }
        }
        if runtime.private_file_volume_owned(&volume, &labels).await? {
            runtime.remove_private_file_volume(&volume, &labels).await?;
        }
        match result {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}
