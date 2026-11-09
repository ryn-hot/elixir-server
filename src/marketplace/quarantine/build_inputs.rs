//! One read-only prerequisite under the original quarantine's native owner.
//! Does not launch a compiler or grant execution/activation/reproduction.
use super::*;
use elixir_marketplace::reproduction::{
    BuildInputDescriptor, BuildInputEvent, BuildInputSummary, BuildPlatform,
    MAX_BUILD_INPUT_EVENT_BYTES,
};

const CONTROL_RESERVATION: u64 = 2 * (4 + MAX_BUILD_INPUT_EVENT_BYTES as u64) + 64 * 1024;

impl SourceMatchedQuarantine {
    /// Trusted install/update controller only. Ordinary search never invokes
    /// this native job. Parsing the returned data is NOT a build receipt; the
    /// same source owner must retain authority through the future build phase.
    pub(in crate::marketplace) async fn check_build_inputs(
        &mut self,
        platform: BuildPlatform,
    ) -> Result<BuildInputSummary> {
        self.require_public_source()?;
        let expires = self.source.job.expires;
        let descriptor = descriptor(self, platform)?;
        let job_id = descriptor.job_id()?;
        let runtime = self.source.job.runtime.clone();
        let operation = async {
            let features = timeout(
                Duration::from_secs(10),
                runtime.image_labels(&self.source.job.volume.image),
            )
            .await
            .context("build input image inspection deadline")??;
            if features
                .get("elixir.marketplace.worker.build_inputs")
                .map(String::as_str)
                != Some("1")
            {
                bail!("native worker lacks the read-only build input contract");
            }
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
            self.source.job.recover_writer(runtime.as_ref()).await?;
            let spec = profile(&self.source.job, &descriptor)?;
            let handle = self.source.job.launch_writer(&spec).await?;
            let result = async {
                charge_bytes(
                    &mut self.source.job.input_used,
                    descriptor.input_allowance()?,
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
                match BuildInputEvent::parse(
                    &channel
                        .frame(Duration::from_secs(10), MAX_BUILD_INPUT_EVENT_BYTES)
                        .await?,
                )? {
                    BuildInputEvent::Ready { job_id: ready, .. } if ready == job_id => {}
                    _ => bail!("native build input check did not become ready for this job"),
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
                self.require_public_source()?;
                channel.marker(JOB_END).await?;
                let remaining = expires.saturating_duration_since(Instant::now());
                let summary = match BuildInputEvent::parse(
                    &channel
                        .frame(remaining, MAX_BUILD_INPUT_EVENT_BYTES)
                        .await?,
                )? {
                    BuildInputEvent::Complete {
                        job_id: complete,
                        source_inputs_only: true,
                        result,
                        ..
                    } if complete == job_id => {
                        result.matches(&descriptor)?;
                        if canonical::to_vec(&result.source)? != canonical::to_vec(&self.summary)? {
                            bail!("native build inputs differ from the original matched source");
                        }
                        *result
                    }
                    _ => bail!("native build input check rejected the source recipe"),
                };
                let mut trailing = [0; 1];
                if timeout(Duration::from_secs(10), channel.output.read(&mut trailing)).await?? != 0
                {
                    bail!("unexpected native build input output");
                }
                if !timeout(Duration::from_secs(10), channel.child.wait())
                    .await??
                    .success()
                {
                    bail!("native build input check exited unsuccessfully");
                }
                timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                    .await
                    .context("native build input diagnostics did not close")???;
                runtime
                    .confirm_quarantine_marketplace_success(&handle, &spec.labels)
                    .await?;
                self.require_public_source()?;
                Ok(summary)
            }
            .await;
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
            result
        };
        let result = timeout_at(expires, operation)
            .await
            .context("native build input check deadline exceeded")
            .and_then(|result| result);
        if result.is_err() {
            timeout(
                Duration::from_secs(25),
                self.source.job.recover_writer(runtime.as_ref()),
            )
            .await
            .context("native build input cleanup remains pending")??;
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
        }
        result
    }
}

fn descriptor(
    source: &SourceMatchedQuarantine,
    platform: BuildPlatform,
) -> Result<BuildInputDescriptor> {
    let remaining = source
        .expires()
        .saturating_duration_since(Instant::now())
        .as_millis();
    let value = BuildInputDescriptor {
        schema_version: 1,
        source: source.source.job.plan.source_match.clone(),
        platform,
        timeout_millis: u32::try_from(remaining)
            .context("native build input remaining deadline")?,
    };
    value.validate()?;
    Ok(value)
}

fn profile(job: &SourceQuarantineJob, descriptor: &BuildInputDescriptor) -> Result<ContainerSpec> {
    descriptor.validate()?;
    if canonical::to_vec(&descriptor.source)? != canonical::to_vec(&job.plan.source_match)? {
        bail!("native build input descriptor belongs to another quarantine");
    }
    let mut spec = job.source_match_profile()?;
    spec.command = vec![
        "--build-inputs-check".into(),
        String::from_utf8(canonical::to_vec(descriptor)?)?,
    ];
    apply_container_spec_fingerprint(&mut spec);
    Ok(spec)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(in crate::marketplace) async fn compiled_source(
        directory: &Path,
    ) -> Result<SourceMatchedQuarantine> {
        let mut source = super::super::tests::matched_fixture(directory).await?;
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../extensions/sdk/marketplace/test/publication-interop.mjs");
        let output = timeout(
            Duration::from_secs(10),
            tokio::process::Command::new("node")
                .arg(script)
                .args(["sample", "compiled"])
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() || output.stdout.len() > 128 * 1024 {
            bail!("compiled source test fixture failed");
        }
        let (release, package, proof) =
            super::super::tests::decode_fixture(serde_json::from_slice(&output.stdout)?)?;
        let plan = SourceQuarantinePlan::for_release(
            &release,
            Some(0),
            &SourceJobBudget {
                wall_seconds: 600,
                disk_bytes: 256 * MIB,
                // Includes seal, import preflight/streaming and control reads.
                input_bytes: 16 * MIB,
                ..super::super::tests::budget()
            },
        )?;
        source.summary = plan
            .source_match
            .verify(std::io::Cursor::new(package), std::io::Cursor::new(proof))?;
        source.source.job.volume.workspace = plan.workspace.clone();
        source.source.job.volume.labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.into(), "true".into()),
            (
                ELIXIR_DEPLOYMENT_ID_LABEL.into(),
                source.source.job.deployment.clone(),
            ),
        ]);
        source.source.job.plan = plan;
        Ok(source)
    }
    #[tokio::test]
    async fn compiled_input_profile_is_read_only_and_uses_the_original_native_owner() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("lane");
        let source = compiled_source(&directory).await?;
        let original_expires = source.expires();
        let descriptor = descriptor(&source, BuildPlatform::LinuxAmd64)?;
        let spec = profile(&source.source.job, &descriptor)?;
        assert_eq!(spec.command[0], "--build-inputs-check");
        assert_eq!(spec.network_mode.as_deref(), Some("none"));
        assert_eq!(spec.volumes.len(), 1);
        assert!(spec.volumes[0].read_only);
        assert_eq!(spec.volumes[0].host_path, source.volume_name());
        assert_eq!(spec.security.file_size_limit_bytes, Some(1));
        assert!(spec.env.is_empty());
        assert!(spec.ports.is_empty());
        super::super::super::runtime::source_writer::SourceWriterIntent::capture(&spec)?;
        assert!(
            descriptor.timeout_millis > 0
                && u64::from(descriptor.timeout_millis)
                    <= descriptor.source.workspace.timeout_seconds * 1000
        );
        assert_eq!(source.expires(), original_expires);
        assert!(ControllerLease::acquire(&directory).is_err());
        Ok(())
    }
    #[tokio::test]
    async fn bad_platform_source_and_missing_public_origin_cannot_spend_a_native_attempt()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("lane");
        let mut source = compiled_source(&directory).await?;
        let before = (
            source.source.job.input_used,
            source.source.job.output_used,
            source.expires(),
        );
        assert!(descriptor(&source, BuildPlatform::LinuxArm64).is_err());
        assert!(
            source
                .check_build_inputs(BuildPlatform::LinuxAmd64)
                .await
                .is_err()
        );
        assert_eq!(
            before,
            (
                source.source.job.input_used,
                source.source.job.output_used,
                source.expires()
            )
        );
        assert!(source.source.job.activity.is_none());
        assert!(!source.source.job.uncertain_start);
        let mut foreign = descriptor(&source, BuildPlatform::LinuxAmd64)?;
        foreign.source.release.artifacts[0].blob.sha256 = Digest::of(b"foreign output");
        assert!(profile(&source.source.job, &foreign).is_err());
        assert!(ControllerLease::acquire(&directory).is_err());
        Ok(())
    }
}
