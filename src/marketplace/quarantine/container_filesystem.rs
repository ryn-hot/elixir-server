//! Combined image/filesystem check after actual source reproduction, within the
//! ORIGINAL source owner. No extraction, daemon import, execution or activation.
use super::*;
use elixir_marketplace::container_image::filesystem::{
    FilesystemLimits, MAX_ENTRIES, MAX_UNPACKED_BYTES,
    job::{
        ContainerFilesystemDescriptor, ContainerFilesystemEvent, ContainerFilesystemSummary,
        MAX_EVENT_BYTES,
    },
};

pub(super) const CONTROL_RESERVATION: u64 = 2 * (4 + MAX_EVENT_BYTES as u64) + 64 * 1024;

/// Native observation only. Parsed peer/event data cannot construct this type.
/// It is deliberately neither Clone nor serializable, and is NOT a runtime grant.
pub(in crate::marketplace) struct NativeContainerFilesystemMatch {
    owner: super::evidence_owner::EvidenceOwner,
    summary: ContainerFilesystemSummary,
}
impl NativeContainerFilesystemMatch {
    pub(in crate::marketplace) fn summary(&self) -> &ContainerFilesystemSummary {
        &self.summary
    }
    pub(super) fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        self.owner.require(source)?;
        source.require_compiled_source()?;
        source.require_compiled_image(&self.summary.image)?;
        self.summary.matches(&descriptor(source)?)?;
        Ok(())
    }
}
impl SourceMatchedQuarantine {
    pub(in crate::marketplace) async fn check_container_filesystem(
        &mut self,
        build: &build::NativeBuildMatch,
    ) -> Result<NativeContainerFilesystemMatch> {
        Box::pin(self.check_container_filesystem_with(Reproduction::Fresh(build))).await
    }
    pub(in crate::marketplace) async fn check_retained_container_filesystem(
        &mut self,
        build: &local_build::VerifiedLocalBuild,
    ) -> Result<NativeContainerFilesystemMatch> {
        Box::pin(self.check_container_filesystem_with(Reproduction::Retained(build))).await
    }
    async fn check_container_filesystem_with(
        &mut self,
        build: Reproduction<'_>,
    ) -> Result<NativeContainerFilesystemMatch> {
        build.require_source(self)?;
        let expires = self.expires();
        let d = descriptor(self)?;
        let job_id = d.job_id()?;
        let runtime = self.runtime();
        let operation = async {
            let features = timeout(
                Duration::from_secs(10),
                runtime.image_labels(&self.source.job.volume.image),
            )
            .await??;
            if features
                .get("elixir.marketplace.worker.container_filesystem")
                .map(String::as_str)
                != Some("1")
            {
                bail!("worker lacks native container filesystem contract");
            }
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
            self.source.job.recover_writer(runtime.as_ref()).await?;
            let spec = profile(&self.source.job, &d)?;
            let handle = self.source.job.launch_writer(&spec).await?;
            let result = async {
                charge_bytes(
                    &mut self.source.job.input_used,
                    d.input_allowance()?,
                    self.source.job.plan.input_limit,
                )?;
                charge_bytes(
                    &mut self.source.job.output_used,
                    CONTROL_RESERVATION,
                    self.source.job.plan.output_limit,
                )?;
                let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
                let mut channel = SourceChannel::new(process)?;
                channel.marker(JOB_START).await?;
                match ContainerFilesystemEvent::parse(
                    &channel
                        .frame(Duration::from_secs(10), MAX_EVENT_BYTES)
                        .await?,
                )? {
                    ContainerFilesystemEvent::Ready {
                        schema_version: 1,
                        job_id: ready,
                    } if ready == job_id => {}
                    _ => bail!("container filesystem checker did not become ready"),
                }
                inspect_quarantine_worker(
                    runtime.as_ref(),
                    &handle,
                    &spec,
                    &self.source.job.plan.limits,
                    &self.source.job.volume.name,
                    1,
                    true,
                )
                .await?;
                build.require_source(self)?;
                channel.marker(JOB_END).await?;
                let summary = match ContainerFilesystemEvent::parse(
                    &channel
                        .frame(
                            expires.saturating_duration_since(Instant::now()),
                            MAX_EVENT_BYTES,
                        )
                        .await?,
                )? {
                    ContainerFilesystemEvent::Complete {
                        schema_version: 1,
                        job_id: complete,
                        filesystem_shape_only: true,
                        result,
                    } if complete == job_id => {
                        result.matches(&d)?;
                        *result
                    }
                    _ => bail!("native container filesystem rejected"),
                };
                let mut trailing = [0; 1];
                if channel.output.read(&mut trailing).await? != 0 {
                    bail!("unexpected native container filesystem output");
                }
                if !channel.child.wait().await?.success() {
                    bail!("native container filesystem helper failed");
                }
                timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                    .await
                    .context("container filesystem diagnostics did not close")???;
                runtime
                    .confirm_quarantine_marketplace_success(&handle, &spec.labels)
                    .await?;
                build.require_source(self)?;
                Ok(summary)
            }
            .await;
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
            result
        };
        let result = timeout_at(expires, operation)
            .await
            .context("original container filesystem deadline exceeded")
            .and_then(|v| v);
        if result.is_err() {
            timeout(
                Duration::from_secs(25),
                self.source.job.recover_writer(runtime.as_ref()),
            )
            .await
            .context("container filesystem cleanup still pending")??;
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
        }
        let summary = result?;
        build.require_image(&summary.image)?;
        let observed = NativeContainerFilesystemMatch {
            owner: super::evidence_owner::EvidenceOwner::capture(self)?,
            summary,
        };
        observed.require_source(self)?;
        Ok(observed)
    }
}
enum Reproduction<'a> {
    Fresh(&'a build::NativeBuildMatch),
    Retained(&'a local_build::VerifiedLocalBuild),
}
impl Reproduction<'_> {
    fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        match self {
            Self::Fresh(build) => build.require_source(source),
            Self::Retained(build) => build.require_source(source),
        }
    }
    fn require_image(
        &self,
        image: &elixir_marketplace::container_image::job::ContainerImageSummary,
    ) -> Result<()> {
        if let Self::Retained(build) = self {
            build.require_image(image)?;
        }
        Ok(())
    }
}
fn limits(job: &SourceQuarantineJob) -> FilesystemLimits {
    // Conservative per-entry analysis envelope within the admitted worker RAM.
    // The kernel/cgroup limit remains authoritative. No disk is unpacked here;
    // import must separately reserve/enforce actual allocation before starting.
    FilesystemLimits {
        entries: ((job.plan.limits.memory_bytes.saturating_sub(32 * MIB) / 2048)
            .min(u64::from(MAX_ENTRIES))) as u32,
        unpacked_bytes: job.plan.disk_limit.min(MAX_UNPACKED_BYTES),
    }
}
pub(super) fn descriptor(
    source: &SourceMatchedQuarantine,
) -> Result<ContainerFilesystemDescriptor> {
    let d = ContainerFilesystemDescriptor {
        schema_version: 1,
        image: super::container_image::descriptor(source)?,
        limits: limits(&source.source.job),
    };
    d.validate()?;
    Ok(d)
}
fn profile(job: &SourceQuarantineJob, d: &ContainerFilesystemDescriptor) -> Result<ContainerSpec> {
    d.validate()?;
    if canonical::to_vec(&d.image.source)? != canonical::to_vec(&job.plan.source_match)?
        || canonical::to_vec(&d.limits)? != canonical::to_vec(&limits(job))?
    {
        bail!("container filesystem descriptor differs from original source/resources");
    }
    let mut spec = job.source_match_profile()?;
    spec.command = vec![
        "--container-filesystem-check".into(),
        String::from_utf8(canonical::to_vec(d)?)?,
    ];
    apply_container_spec_fingerprint(&mut spec);
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn filesystem_check_keeps_original_read_only_owner_and_finite_resources() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let lane = directory.path().canonicalize()?.join("lane");
        let mut source = super::super::build_inputs::tests::compiled_source(&lane).await?;
        let before = (
            source.expires(),
            source.source.job.input_used,
            source.source.job.output_used,
        );
        let d = descriptor(&source)?;
        let spec = profile(&source.source.job, &d)?;
        assert_eq!(spec.command[0], "--container-filesystem-check");
        assert_eq!(spec.volumes.len(), 1);
        assert_eq!(spec.volumes[0].host_path, source.volume_name());
        assert!(spec.volumes.iter().all(|m| m.read_only));
        assert_eq!(spec.network_mode.as_deref(), Some("none"));
        assert_eq!(spec.security.file_size_limit_bytes, Some(1));
        assert!(spec.env.is_empty() && spec.ports.is_empty());
        super::super::super::runtime::source_writer::SourceWriterIntent::capture(&spec)?;
        let mut foreign = d.clone();
        foreign.limits.entries += 1;
        assert!(profile(&source.source.job, &foreign).is_err());
        foreign = d.clone();
        foreign.limits.unpacked_bytes += 1;
        assert!(profile(&source.source.job, &foreign).is_err());
        foreign = d.clone();
        foreign.image.source.workspace.timeout_seconds += 1;
        assert!(profile(&source.source.job, &foreign).is_err());
        assert_eq!(d.limits.entries, 16_384);
        assert_eq!(d.limits.unpacked_bytes, source.source.job.plan.disk_limit);
        source.source.job.plan.limits = SourceJobLimits::new(256 * MIB, 100)?;
        assert_eq!(limits(&source.source.job).entries, MAX_ENTRIES);
        assert!(SourceJobLimits::new(32 * MIB, 100).is_err());
        source.source.job.plan.disk_limit = 8191;
        assert!(descriptor(&source).is_err());
        assert_eq!(
            before,
            (
                source.expires(),
                source.source.job.input_used,
                source.source.job.output_used
            )
        );
        assert!(source.source.job.activity.is_none());
        assert!(ControllerLease::acquire(&lane).is_err());
        Ok(())
    }
}
