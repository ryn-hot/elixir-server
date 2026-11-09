//! Read-only OCI check under the SAME source/build owner. Never called from
//! search and never treats a parsed image summary as runtime/activation proof.
use super::*;
use elixir_marketplace::container_image::job::{
    ContainerImageDescriptor, ContainerImageEvent, ContainerImageSummary, MAX_EVENT_BYTES,
};

pub(super) const CONTROL_RESERVATION: u64 = 2 * (4 + MAX_EVENT_BYTES as u64) + 64 * 1024;
/// Constructible only after actual native work is joined. Still image-graph
/// evidence, not an imported image, sandbox receipt or permission to activate.
pub(in crate::marketplace) struct NativeContainerImageMatch {
    owner: super::evidence_owner::EvidenceOwner,
    summary: ContainerImageSummary,
}
impl NativeContainerImageMatch {
    pub(in crate::marketplace) fn summary(&self) -> &ContainerImageSummary {
        &self.summary
    }
    pub(super) fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        self.owner.require(source)?;
        source.require_compiled_source()?;
        source.require_compiled_image(&self.summary)?;
        self.summary.matches(&descriptor(source)?)?;
        Ok(())
    }
}
impl SourceMatchedQuarantine {
    pub(in crate::marketplace) async fn check_container_image(
        &mut self,
        build: &build::NativeBuildMatch,
    ) -> Result<NativeContainerImageMatch> {
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
                .get("elixir.marketplace.worker.container_image")
                .map(String::as_str)
                != Some("1")
            {
                bail!("worker lacks native container image graph contract");
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
                match ContainerImageEvent::parse(
                    &channel
                        .frame(Duration::from_secs(10), MAX_EVENT_BYTES)
                        .await?,
                )? {
                    ContainerImageEvent::Ready {
                        schema_version: 1,
                        job_id: ready,
                    } if ready == job_id => {}
                    _ => bail!("container graph checker did not become ready"),
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
                let summary = match ContainerImageEvent::parse(
                    &channel
                        .frame(
                            expires.saturating_duration_since(Instant::now()),
                            MAX_EVENT_BYTES,
                        )
                        .await?,
                )? {
                    ContainerImageEvent::Complete {
                        schema_version: 1,
                        job_id: complete,
                        image_graph_only: true,
                        result,
                    } if complete == job_id => {
                        result.matches(&d)?;
                        *result
                    }
                    _ => bail!("native container image graph rejected"),
                };
                let mut trailing = [0; 1];
                if channel.output.read(&mut trailing).await? != 0 {
                    bail!("unexpected native container graph output");
                }
                if !channel.child.wait().await?.success() {
                    bail!("native container image graph helper failed");
                }
                timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                    .await
                    .context("container graph diagnostics did not close")???;
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
            .context("original container image check deadline exceeded")
            .and_then(|v| v);
        if result.is_err() {
            timeout(
                Duration::from_secs(25),
                self.source.job.recover_writer(runtime.as_ref()),
            )
            .await
            .context("container graph cleanup still pending")??;
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
        }
        let summary = result?;
        let observed = NativeContainerImageMatch {
            owner: super::evidence_owner::EvidenceOwner::capture(self)?,
            summary,
        };
        observed.require_source(self)?;
        Ok(observed)
    }
}
pub(super) fn descriptor(source: &SourceMatchedQuarantine) -> Result<ContainerImageDescriptor> {
    let d = super::super::runtime::build::execution(
        &source.source.job.plan.source_match,
        u32::try_from(
            source
                .expires()
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )?,
    )?;
    let d = ContainerImageDescriptor {
        schema_version: 1,
        source: d.inputs.source,
        platform: d.inputs.platform,
        timeout_millis: d.inputs.timeout_millis,
    };
    d.validate()?;
    Ok(d)
}
fn profile(job: &SourceQuarantineJob, d: &ContainerImageDescriptor) -> Result<ContainerSpec> {
    d.validate()?;
    if canonical::to_vec(&d.source)? != canonical::to_vec(&job.plan.source_match)? {
        bail!("container image descriptor differs from original owner");
    }
    let mut spec = job.source_match_profile()?;
    spec.command = vec![
        "--container-image-check".into(),
        String::from_utf8(canonical::to_vec(d)?)?,
    ];
    apply_container_spec_fingerprint(&mut spec);
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn native_image_check_is_read_only_original_owner_data_without_import_or_execution()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let lane = directory.path().canonicalize()?.join("lane");
        let source = super::super::build_inputs::tests::compiled_source(&lane).await?;
        let expires = source.expires();
        let input = source.source.job.input_used;
        let d = descriptor(&source)?;
        let spec = profile(&source.source.job, &d)?;
        assert_eq!(spec.command[0], "--container-image-check");
        assert!(spec.volumes.iter().all(|m| m.read_only));
        assert_eq!(spec.volumes.len(), 1);
        assert_eq!(spec.volumes[0].host_path, source.source.job.volume.name);
        assert_eq!(spec.network_mode.as_deref(), Some("none"));
        assert!(spec.ports.is_empty());
        assert!(spec.env.is_empty());
        super::super::super::runtime::source_writer::SourceWriterIntent::capture(&spec)?;
        let mut wrong = d;
        wrong.source.workspace.timeout_seconds += 1;
        assert!(profile(&source.source.job, &wrong).is_err());
        assert_eq!(source.expires(), expires);
        assert_eq!(source.source.job.input_used, input);
        assert!(ControllerLease::acquire(&lane).is_err());
        Ok(())
    }
}
