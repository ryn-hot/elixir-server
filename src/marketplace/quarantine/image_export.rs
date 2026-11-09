//! Native byte export under the ORIGINAL build/source owner. No daemon API,
//! image import, publisher command, permissions or activation are introduced.
use super::super::archive::ImageExportStaging;
use super::*;
use elixir_marketplace::container_image::export::{
    ContainerExportDescriptor, ContainerExportEvent, ContainerExportPart, ContainerExportReceiver,
    ContainerExportSummary, MAX_EVENT_BYTES,
};
use elixir_marketplace::container_image::filesystem::job::ContainerFilesystemSummary;

pub(in crate::marketplace) struct Export {
    source: Option<SourceMatchedQuarantine>,
    owner: evidence_owner::EvidenceOwner,
    expected: ContainerFilesystemSummary,
    descriptor: ContainerExportDescriptor,
    staging: Option<ImageExportStaging>,
    started: bool,
    terminal: bool,
    matched: Option<ContainerExportSummary>,
}
/// Constructible only after actual joined native export and sealed/rehashed
/// file. It owns disposable staging, not an imported image or execution grant.
pub(in crate::marketplace) struct VerifiedImageExport {
    owner: evidence_owner::EvidenceOwner,
    summary: ContainerExportSummary,
    staging: ImageExportStaging,
}
impl VerifiedImageExport {
    pub(in crate::marketplace) fn summary(&self) -> &ContainerExportSummary {
        &self.summary
    }
    pub(in crate::marketplace) fn require_source(
        &self,
        source: &SourceMatchedQuarantine,
    ) -> Result<()> {
        self.owner.require(source)?;
        source.require_compiled_source()?;
        source.require_compiled_image(&self.summary.filesystem.image)?;
        let d = descriptor(source, &self.summary.filesystem)?;
        self.summary.matches(&d)?;
        self.staging.require_sealed(&self.summary.archive)
    }
    pub(in crate::marketplace) async fn discard(&mut self) -> Result<()> {
        self.staging.retire().await
    }
}
impl SourceMatchedQuarantine {
    /// Borrow prerequisite capabilities before taking the original source. A
    /// parsed summary cannot call this in place of actual native receipts.
    pub(in crate::marketplace) fn begin_image_export(
        source: &mut Option<Self>,
        build: &build::NativeBuildMatch,
        filesystem: &container_filesystem::NativeContainerFilesystemMatch,
    ) -> Result<Export> {
        let original = source
            .as_ref()
            .context("image export source owner missing")?;
        build.require_source(original)?;
        Self::begin_image_export_bound(source, filesystem)
    }
    pub(in crate::marketplace) fn begin_retained_image_export(
        source: &mut Option<Self>,
        build: &local_build::VerifiedLocalBuild,
        filesystem: &container_filesystem::NativeContainerFilesystemMatch,
    ) -> Result<Export> {
        let original = source
            .as_ref()
            .context("retained image source owner missing")?;
        build.require_source(original)?;
        build.require_image(&filesystem.summary().image)?;
        Self::begin_image_export_bound(source, filesystem)
    }
    fn begin_image_export_bound(
        source: &mut Option<Self>,
        filesystem: &container_filesystem::NativeContainerFilesystemMatch,
    ) -> Result<Export> {
        let original = source
            .as_ref()
            .context("image export source owner missing")?;
        filesystem.require_source(original)?;
        let expected = filesystem.summary().clone();
        let descriptor = descriptor(original, &expected)?;
        let execution =
            super::super::runtime::build::execution(&original.source.job.plan.source_match, 1)?;
        if super::super::runtime::build::extra_disk(&execution)? < descriptor.output_limit + MIB {
            bail!("image export staging exceeds the original retired build allocation");
        }
        let owner = evidence_owner::EvidenceOwner::capture(original)?;
        let staging = ImageExportStaging::new(original)?;
        Ok(Export {
            source: source.take(),
            owner,
            expected,
            descriptor,
            staging: Some(staging),
            started: false,
            terminal: false,
            matched: None,
        })
    }
}
impl Export {
    fn source(&self) -> Result<&SourceMatchedQuarantine> {
        self.source
            .as_ref()
            .context("image export source owner missing")
    }
    fn source_mut(&mut self) -> Result<&mut SourceMatchedQuarantine> {
        self.source
            .as_mut()
            .context("image export source owner missing")
    }
    fn current(&self) -> Result<()> {
        let source = self.source()?;
        self.owner.require(source)?;
        source.require_compiled_source()?;
        // The exclusively owned byte plan is compared at construction/native
        // launch and again in the final summary. Do not re-serialize the whole
        // source recipe for every 64-KiB chunk; keep live authority checks here.
        Ok(())
    }
    pub(in crate::marketplace) async fn run(&mut self) -> Result<()> {
        if self.started || self.terminal {
            bail!("image export attempt cannot restart");
        }
        self.started = true;
        let expires = self.source()?.expires();
        let result = timeout_at(expires, self.run_inner())
            .await
            .context("original image export deadline exceeded")
            .and_then(|v| v);
        if let Err(error) = result {
            // Same owner and counters survive failed/cancelled cleanup waits.
            self.cancel().await?;
            return Err(error);
        }
        Ok(())
    }
    async fn run_inner(&mut self) -> Result<()> {
        self.current()?;
        let runtime = self.source()?.runtime();
        let features = timeout(
            Duration::from_secs(10),
            runtime.image_labels(&self.source()?.source.job.volume.image),
        )
        .await??;
        if features
            .get("elixir.marketplace.worker.container_export")
            .map(String::as_str)
            != Some("1")
        {
            bail!("worker lacks bounded native container export contract");
        }
        let d = self.descriptor.clone();
        let expires = self.source()?.expires();
        let job_id = d.job_id()?;
        {
            let job = &mut self.source_mut()?.source.job;
            job.cleanup_activity(runtime.as_ref()).await?;
            job.recover_writer(runtime.as_ref()).await?;
            // Playback/headroom admission precedes host disk preallocation,
            // not merely the later native parser/container launch.
            job.admit_native(expires).await?;
        }
        // Predeclared host writes/verification + future import-read credit. The
        // actual daemon import is still absent and gains no authority here.
        {
            let job = &mut self.source_mut()?.source.job;
            let mut input = job.input_used;
            let mut output = job.output_used;
            charge_bytes(&mut input, 3 * (d.output_limit + 1), job.plan.input_limit)?;
            charge_bytes(&mut output, d.output_limit + MIB, job.plan.output_limit)?;
            job.input_used = input;
            job.output_used = output;
        }
        self.staging
            .as_mut()
            .context("image export staging missing")?
            .open()
            .await?;
        // Host free space changed. A pre-allocation snapshot is not evidence
        // for launching the next native phase, even under this same job.
        self.source_mut()?.source.job.cached_measurement = None;
        let spec = profile(&self.source()?.source.job, &d)?;
        let handle = {
            let job = &mut self.source_mut()?.source.job;
            job.cleanup_activity(runtime.as_ref()).await?;
            job.recover_writer(runtime.as_ref()).await?;
            job.launch_writer_budgeted(
                &spec,
                expires,
                Some((
                    d.input_allowance()?,
                    elixir_marketplace::container_image::export::control_output_allowance(
                        d.filesystem.image.artifact()?,
                    )?,
                )),
            )
            .await?
        };
        let result = async {
            let mut channel =
                SourceChannel::new(runtime.attach_private_stdio(&handle, &spec.labels).await?)?;
            channel.marker(JOB_START).await?;
            match ContainerExportEvent::parse(
                &channel
                    .frame(Duration::from_secs(10), MAX_EVENT_BYTES)
                    .await?,
            )? {
                ContainerExportEvent::Ready {
                    schema_version: 1,
                    job_id: ready,
                } if ready == job_id => {}
                _ => bail!("native image exporter did not become ready"),
            }
            inspect_quarantine_worker(
                runtime.as_ref(),
                &handle,
                &spec,
                &self.source()?.source.job.plan.limits,
                &self.source()?.source.job.volume.name,
                1,
                true,
            )
            .await?;
            self.current()?;
            channel.marker(JOB_END).await?;
            let mut receiver = ContainerExportReceiver::new(d.clone())?;
            let summary = loop {
                self.current()?;
                let event = ContainerExportEvent::parse(
                    &channel
                        .frame(
                            expires.saturating_duration_since(Instant::now()),
                            MAX_EVENT_BYTES,
                        )
                        .await?,
                )?;
                match receiver.accept(event)? {
                    ContainerExportPart::Chunk(bytes) => {
                        self.staging
                            .as_mut()
                            .context("image export staging missing")?
                            .write_all(&bytes)
                            .await?;
                    }
                    ContainerExportPart::Complete(summary) => {
                        if canonical::to_vec(&summary.filesystem)?
                            != canonical::to_vec(&self.expected)?
                        {
                            bail!("native export differs from original native filesystem result");
                        }
                        break *summary;
                    }
                }
            };
            let mut trailing = [0; 1];
            if channel.output.read(&mut trailing).await? != 0 {
                bail!("unexpected trailing image export bytes");
            }
            if !channel.child.wait().await?.success() {
                bail!("native image export helper failed");
            }
            timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                .await
                .context("image export diagnostics did not close")???;
            runtime
                .confirm_quarantine_marketplace_success(&handle, &spec.labels)
                .await?;
            self.current()?;
            Ok::<_, anyhow::Error>(summary)
        }
        .await;
        self.source_mut()?
            .source
            .job
            .cleanup_activity(runtime.as_ref())
            .await?;
        let summary = result?;
        self.staging
            .as_mut()
            .context("image export staging missing")?
            .seal(&summary.archive)
            .await?;
        self.current()?;
        self.matched = Some(summary);
        self.terminal = true;
        Ok(())
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        let runtime = self.source()?.runtime();
        let job = &mut self.source_mut()?.source.job;
        job.cleanup_activity(runtime.as_ref()).await?;
        job.recover_writer(runtime.as_ref()).await?;
        job.cleanup_activity(runtime.as_ref()).await?;
        self.staging
            .as_mut()
            .context("image export staging missing")?
            .retire()
            .await?;
        self.matched = None;
        self.terminal = true;
        Ok(())
    }
    pub(in crate::marketplace) fn finish(
        &mut self,
    ) -> Result<(SourceMatchedQuarantine, VerifiedImageExport)> {
        self.current()?;
        if !self.terminal {
            bail!("native image export is unfinished");
        }
        let summary = self
            .matched
            .as_ref()
            .context("native image export has no joined result")?;
        self.staging
            .as_ref()
            .context("image export staging missing")?
            .require_sealed(&summary.archive)?;
        let owner = evidence_owner::EvidenceOwner::capture(self.source()?)?;
        Ok((
            self.source
                .take()
                .context("image export source owner missing")?,
            VerifiedImageExport {
                owner,
                summary: self
                    .matched
                    .take()
                    .context("image export summary missing")?,
                staging: self
                    .staging
                    .take()
                    .context("image export staging missing")?,
            },
        ))
    }
    pub(in crate::marketplace) fn take_cancelled_source(
        &mut self,
    ) -> Result<SourceMatchedQuarantine> {
        if !self.terminal || self.matched.is_some() {
            bail!("image export cleanup is unfinished");
        }
        self.source
            .take()
            .context("image export source owner missing")
    }
}
impl Drop for Export {
    fn drop(&mut self) {
        if self.source.is_none() {
            return;
        }
        let Ok(executor) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut source = self.source.take().expect("checked original source");
        let mut staging = self.staging.take();
        executor.spawn(async move {
            let mut reported = false;
            loop {
                let result = async {
                    let runtime = source.runtime();
                    source.source.job.cleanup_activity(runtime.as_ref()).await?;
                    source.source.job.recover_writer(runtime.as_ref()).await?;
                    source.source.job.cleanup_activity(runtime.as_ref()).await?;
                    if let Some(staging) = staging.as_mut() {
                        staging.retire().await?;
                    }
                    source.discard_retained().await
                }
                .await;
                match result {
                    Ok(()) => return,
                    Err(error) => {
                        if !reported {
                            tracing::warn!(error=%error, "original image export cleanup pending");
                            reported = true;
                        }
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
    }
}
fn descriptor(
    source: &SourceMatchedQuarantine,
    expected: &ContainerFilesystemSummary,
) -> Result<ContainerExportDescriptor> {
    let filesystem = super::container_filesystem::descriptor(source)?;
    expected.matches(&filesystem)?;
    let d = ContainerExportDescriptor {
        schema_version: 1,
        root_sha256: expected.root_sha256,
        output_limit: elixir_marketplace::container_image::export::output_limit(
            filesystem.image.artifact()?,
        )?,
        filesystem,
    };
    d.validate()?;
    Ok(d)
}
fn profile(job: &SourceQuarantineJob, d: &ContainerExportDescriptor) -> Result<ContainerSpec> {
    d.validate()?;
    if canonical::to_vec(&d.filesystem.image.source)? != canonical::to_vec(&job.plan.source_match)?
    {
        bail!("image exporter differs from original source owner");
    }
    let mut spec = job.source_match_profile()?;
    spec.command = vec![
        "--container-image-export".into(),
        String::from_utf8(canonical::to_vec(d)?)?,
    ];
    apply_container_spec_fingerprint(&mut spec);
    Ok(spec)
}
