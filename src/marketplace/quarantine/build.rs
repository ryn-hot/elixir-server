//! One install/update build under the ORIGINAL source job. Never called by
//! search; no build daemon, new lease, deadline reset or activation shortcut.
use super::super::{
    archive::{BuildInputs, Reader, Replay},
    runtime::build as profile,
};
use super::*;
use elixir_marketplace::{build_job::BuildExecutionDescriptor, reproduction::BuildInputSummary};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod native;

/// Private in-memory comparison evidence only. Not serializable/cloneable and
/// not sufficient for activation without the still-missing runtime closure.
pub(in crate::marketplace) struct NativeBuildMatch {
    owner: super::evidence_owner::EvidenceOwner,
    release: Digest,
    inputs: BuildInputSummary,
    artifact: elixir_marketplace::records::Blob,
    toolchain: Digest,
}
impl NativeBuildMatch {
    pub(super) fn require_source(&self, source: &SourceMatchedQuarantine) -> Result<()> {
        self.owner.require(source)?;
        source.require_public_source()?;
        let expected = profile::execution(&source.source.job.plan.source_match, 1)?;
        self.inputs.matches(&expected.inputs)?;
        if self.release != source.workspace().release_id
            || self.artifact.sha256 != expected.output.artifact.sha256
            || self.artifact.size != expected.output.artifact.size
            || self.toolchain != expected.toolchain_image_digest
            || canonical::to_vec(&self.inputs.source)? != canonical::to_vec(source.summary())?
        {
            bail!("native build evidence differs from original container source");
        }
        Ok(())
    }
}
pub(in crate::marketplace) struct Build {
    source: Option<SourceMatchedQuarantine>,
    package: Option<Reader>,
    proof: Option<Reader>,
    staging: Option<BuildInputs>,
    descriptor: BuildExecutionDescriptor,
    slot: String,
    started: bool,
    terminal: bool,
    drop_cleanup_owned: bool,
    matched: Option<NativeBuildMatch>,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Event {
    Ready {
        schema_version: u32,
        job_id: Digest,
    },
    Provisioned {
        schema_version: u32,
        job_id: Digest,
        reservation_bytes: u64,
    },
    Complete {
        schema_version: u32,
        job_id: Digest,
        integrity_only: bool,
        metadata_only: bool,
    },
}
impl SourceMatchedQuarantine {
    /// Replay comes from the existing owned archive path. Take ONLY package/
    /// proof readers; the artifact reader is never passed to the compiler.
    pub(in crate::marketplace) fn begin_build(
        owner: &mut Option<Self>,
        replay: &mut Replay,
        approved: &profile::ApprovedToolchains,
    ) -> Result<Build> {
        let source = owner.as_ref().context("build source owner missing")?;
        source.require_public_source()?;
        let remaining = u32::try_from(
            source
                .expires()
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )?;
        let descriptor = profile::execution(&source.source.job.plan.source_match, remaining)?;
        approved.require(descriptor.toolchain_image_digest)?;
        let slot = source.source.job._lease.storage_namespace()?;
        let package = replay.take(QuarantinePurpose::Package)?;
        let proof = replay.take(QuarantinePurpose::Proof)?;
        Ok(Build {
            source: Some(owner.take().context("build source owner missing")?),
            package: Some(package),
            proof: Some(proof),
            staging: None,
            descriptor,
            slot,
            started: false,
            terminal: false,
            drop_cleanup_owned: false,
            matched: None,
        })
    }
}
impl SourceMatchedQuarantine {
    /// Fresh install/update: raw source staging does not require activation or
    /// a completed permission grant. Compiler execution remains offline/owned.
    pub(in crate::marketplace) fn begin_fresh_build(
        owner: &mut Option<Self>,
        approved: &profile::ApprovedToolchains,
    ) -> Result<Build> {
        let source = owner.as_ref().context("build source owner missing")?;
        source.require_public_source()?;
        let remaining = u32::try_from(
            source
                .expires()
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )?;
        let descriptor = profile::execution(&source.source.job.plan.source_match, remaining)?;
        approved.require(descriptor.toolchain_image_digest)?;
        let slot = source.source.job._lease.storage_namespace()?;
        let staging = BuildInputs::new(source)?;
        Ok(Build {
            source: Some(owner.take().context("build source owner missing")?),
            package: None,
            proof: None,
            staging: Some(staging),
            descriptor,
            slot,
            started: false,
            terminal: false,
            drop_cleanup_owned: false,
            matched: None,
        })
    }
}
impl Build {
    fn source(&self) -> Result<&SourceMatchedQuarantine> {
        self.source.as_ref().context("build source owner missing")
    }
    fn source_mut(&mut self) -> Result<&mut SourceMatchedQuarantine> {
        self.source.as_mut().context("build source owner missing")
    }
    pub(in crate::marketplace) async fn run(&mut self) -> Result<()> {
        if self.started || self.terminal {
            bail!("native build attempt is already owned or consumed");
        }
        self.started = true;
        let expires = self.source()?.expires();
        let result = timeout_at(expires, self.run_inner())
            .await
            .context("original native build deadline exceeded")
            .and_then(|v| v);
        match result {
            Ok(()) => Ok(()),
            Err(original) => match self.cancel().await {
                Ok(()) => Err(original),
                Err(cleanup) => Err(cleanup
                    .context("native build cleanup remains pending")
                    .context(original)),
            },
        }
    }
    async fn run_inner(&mut self) -> Result<()> {
        let runtime = self.source()?.runtime();
        let features = timeout(
            Duration::from_secs(10),
            runtime.image_labels(&self.source()?.source.job.volume.image),
        )
        .await??;
        if features
            .get("elixir.marketplace.worker.build_output")
            .map(String::as_str)
            != Some("1")
        {
            bail!("worker lacks trusted native build output contract");
        }
        let platform = self.descriptor.inputs.platform;
        let inputs = self.source_mut()?.check_build_inputs(platform).await?;
        if let Some(staging) = self.staging.as_mut() {
            staging
                .prepare(self.source.as_mut().context("build source owner missing")?)
                .await?;
            let mut replay = staging.take_readers()?;
            self.package = Some(replay.take(QuarantinePurpose::Package)?);
            self.proof = Some(replay.take(QuarantinePurpose::Proof)?);
        }
        let d = self.descriptor.clone();
        let slot = self.slot.clone();
        {
            let job = &mut self.source_mut()?.source.job;
            job.cleanup_activity(runtime.as_ref()).await?;
            job.recover_writer(runtime.as_ref()).await?;
            job.admit_native(job.expires).await?;
            job.begin_activity(Target::BuildOutputAllocation {
                deployment: slot.clone(),
            })?;
            let (fence, _) = job
                .launch_native(launch::Operation::BuildOutput(slot.clone()), job.expires)
                .await?;
            job.activity = fence;
        }
        self.initialize_output(&d, &slot).await?;
        self.compile(&d, &slot).await?;
        self.compare_output(&d, &slot).await?;
        if let Some(reader) = self.package.as_mut() {
            reader.drain().await?;
        }
        if let Some(reader) = self.proof.as_mut() {
            reader.drain().await?;
        }
        // Closing the last read FDs is part of retiring staging allocation.
        // Unlink alone would retain those bytes into the next export phase.
        self.package = None;
        self.proof = None;
        if let Some(staging) = self.staging.as_mut() {
            staging.retire().await?;
        }
        // Retired staging owns a strong reference to the original lane. Once
        // its actual I/O joins, a completed Build must not pin later jobs.
        self.staging = None;
        self.source()?.require_public_source()?;
        runtime
            .remove_marketplace_build_output_volume(&slot)
            .await?;
        self.matched = Some(NativeBuildMatch {
            owner: super::evidence_owner::EvidenceOwner::capture(self.source()?)?,
            release: d.output.release_id,
            artifact: d.output.artifact.clone(),
            toolchain: d.toolchain_image_digest,
            inputs,
        });
        self.terminal = true;
        Ok(())
    }
    fn spec(&self, d: &BuildExecutionDescriptor, slot: &str, mode: &str) -> Result<ContainerSpec> {
        profile::configure(
            &self.source()?.source.job.source_match_profile()?,
            slot,
            d,
            mode,
        )
    }
    async fn initialize_output(&mut self, d: &BuildExecutionDescriptor, slot: &str) -> Result<()> {
        let spec = self.spec(d, slot, "--build-output-init")?;
        let runtime = self.source()?.runtime();
        let job = &mut self.source_mut()?.source.job;
        let handle = job
            .launch_writer_budgeted(
                &spec,
                job.expires,
                Some((0, d.output.artifact.size + 4096 + 64 * 1024)),
            )
            .await?;
        let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
        let mut channel = SourceChannel::new(process)?;
        ready(&mut channel, d.output.job_id()?).await?;
        // Exact OCI user/capabilities/mounts were verified BEFORE start. No
        // publisher code runs in this trusted root-only allocation helper.
        channel.marker(JOB_END).await?;
        let bytes = channel
            .frame(
                self.source()?
                    .expires()
                    .saturating_duration_since(Instant::now()),
                4096,
            )
            .await?;
        match canonical::decode::<Event>(&bytes, 4096)? {
            Event::Provisioned {
                schema_version: 1,
                job_id,
                reservation_bytes,
            } if job_id == d.output.job_id()?
                && reservation_bytes == d.output.reservation_bytes()? => {}
            _ => bail!("build output provision report differs"),
        }
        let mut trailing = [0; 1];
        if channel.output.read(&mut trailing).await? != 0 {
            bail!("unexpected provisioner output");
        }
        finish_channel(&mut channel, runtime.as_ref(), &handle, &spec).await?;
        self.source_mut()?
            .source
            .job
            .cleanup_activity(runtime.as_ref())
            .await
    }
    async fn compile(&mut self, d: &BuildExecutionDescriptor, slot: &str) -> Result<()> {
        let spec = self.spec(d, slot, "--build-run")?;
        let runtime = self.source()?.runtime();
        let handle = {
            let job = &mut self.source_mut()?.source.job;
            job.launch_writer_budgeted(
                &spec,
                job.expires,
                Some((
                    d.inputs.input_allowance()?
                        + d.inputs.source.workspace.package.size
                        + d.inputs.source.workspace.proof.size
                        + 3 * d.inputs.source.workspace.package.size,
                    128 * 1024,
                )),
            )
            .await?
        };
        let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
        let mut channel = SourceChannel::new(process)?;
        ready(&mut channel, d.job_id()?).await?;
        profile::inspect_running(
            runtime.as_ref(),
            &handle,
            &spec,
            &self.source()?.source.job.plan.limits,
        )
        .await?;
        self.source()?.require_public_source()?;
        channel.marker(JOB_END).await?;
        let input = channel.input.as_mut().context("build source pipe closed")?;
        transfer(
            self.package
                .as_mut()
                .context("build package reader missing")?,
            input,
            d.inputs.source.workspace.package.size,
        )
        .await?;
        transfer(
            self.proof.as_mut().context("build proof reader missing")?,
            input,
            d.inputs.source.workspace.proof.size,
        )
        .await?;
        channel.marker(JOB_END).await?;
        // Keep Docker's separately attached stdin alive. Public/archive EOF
        // was checked by each Reader before JOB_END; attach EOF is not a
        // reliable native completion signal and may detach the CLI early.
        let mut trailing = [0; 1];
        if channel.output.read(&mut trailing).await? != 0 {
            bail!("untrusted compiler emitted unexpected control output");
        }
        finish_channel(&mut channel, runtime.as_ref(), &handle, &spec).await?;
        // This removes the immutable container before any exporter starts;
        // compiler exit or attach EOF alone is not a stopped-descendant proof.
        self.source_mut()?
            .source
            .job
            .cleanup_activity(runtime.as_ref())
            .await
    }
    async fn compare_output(&mut self, d: &BuildExecutionDescriptor, slot: &str) -> Result<()> {
        let spec = self.spec(d, slot, "--build-output-check")?;
        let runtime = self.source()?.runtime();
        let handle = {
            let job = &mut self.source_mut()?.source.job;
            job.launch_writer_budgeted(
                &spec,
                job.expires,
                Some((d.output.artifact.size + 1, 128 * 1024)),
            )
            .await?
        };
        let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
        let mut channel = SourceChannel::new(process)?;
        ready(&mut channel, d.output.job_id()?).await?;
        profile::inspect_running(
            runtime.as_ref(),
            &handle,
            &spec,
            &self.source()?.source.job.plan.limits,
        )
        .await?;
        self.source()?.require_public_source()?;
        channel.marker(JOB_END).await?;
        let event: Event = canonical::decode(
            &channel
                .frame(
                    self.source()?
                        .expires()
                        .saturating_duration_since(Instant::now()),
                    4096,
                )
                .await?,
            4096,
        )?;
        match event {
            Event::Complete {
                schema_version: 1,
                job_id,
                integrity_only: true,
                metadata_only: false,
            } if job_id == d.output.job_id()? => {}
            _ => bail!("native build output did not match selected signed artifact"),
        }
        let mut trailing = [0; 1];
        if channel.output.read(&mut trailing).await? != 0 {
            bail!("unexpected exporter control output");
        }
        finish_channel(&mut channel, runtime.as_ref(), &handle, &spec).await?;
        self.source_mut()?
            .source
            .job
            .cleanup_activity(runtime.as_ref())
            .await
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.terminal = false;
        self.matched = None;
        // Retain the same reader objects while joining any original blocking
        // read; a cancelled waiter cannot turn it into completed cleanup.
        if let Some(reader) = self.package.as_mut() {
            reader.drain().await?;
        }
        if let Some(reader) = self.proof.as_mut() {
            reader.drain().await?;
        }
        self.package = None;
        self.proof = None;
        let runtime = self.source()?.runtime();
        let slot = self.slot.clone();
        let job = &mut self.source_mut()?.source.job;
        job.cleanup_activity(runtime.as_ref()).await?;
        job.recover_writer(runtime.as_ref()).await?;
        SourceQuarantineJob::remove_stopped_writer(runtime.as_ref(), &job.deployment).await?;
        if let Some(staging) = self.staging.as_mut() {
            staging.retire().await?;
        }
        self.staging = None;
        runtime
            .remove_marketplace_build_output_volume(&slot)
            .await?;
        self.terminal = true;
        Ok(())
    }
    pub(in crate::marketplace) fn finish(
        &mut self,
    ) -> Result<(SourceMatchedQuarantine, NativeBuildMatch)> {
        if !self.terminal {
            bail!("native build owner has not joined");
        }
        let source = self.source.as_ref().context("build source owner missing")?;
        source.require_public_source()?;
        let result = self
            .matched
            .as_ref()
            .context("native build has no verified output")?;
        result.owner.require(source)?;
        if result.release != source.workspace().release_id
            || result.artifact.sha256 != self.descriptor.output.artifact.sha256
            || result.toolchain != self.descriptor.toolchain_image_digest
        {
            bail!("native build result changed scope");
        }
        result.inputs.matches(&self.descriptor.inputs)?;
        Ok((
            self.source.take().context("build source owner missing")?,
            self.matched.take().context("native build result missing")?,
        ))
    }
    /// Failed/cancelled work may hand back its original source for explicit
    /// disposal only AFTER readers, staging and native actors are joined.
    pub(in crate::marketplace) fn take_cancelled_source(
        &mut self,
    ) -> Result<SourceMatchedQuarantine> {
        if !self.terminal || self.matched.is_some() {
            bail!("native build cleanup is not complete");
        }
        self.source.take().context("build source owner missing")
    }
}
impl Drop for Build {
    fn drop(&mut self) {
        if self.source.is_none() || self.drop_cleanup_owned {
            return;
        }
        let Ok(executor) = tokio::runtime::Handle::try_current() else {
            // Shutdown/crash is not a stop receipt. Keep the original durable
            // native fence; independent guardians still bound native jobs.
            return;
        };
        let mut retained = Build {
            source: self.source.take(),
            package: self.package.take(),
            proof: self.proof.take(),
            staging: self.staging.take(),
            descriptor: self.descriptor.clone(),
            slot: self.slot.clone(),
            started: true,
            terminal: false,
            // A runtime-shutdown drop of this one cleanup task must not
            // recursively spawn another task. It is never a cleanup receipt.
            drop_cleanup_owned: true,
            matched: None,
        };
        executor.spawn(async move {
            let mut reported=false;
            loop {
                let result=async {
                    retained.cancel().await?;
                    retained.source_mut()?.discard_retained().await
                }.await;
                match result {
                    Ok(())=>{retained.source.take();return;},
                    Err(error)=>{
                        if !reported {tracing::warn!(error=%error,"compiled build cleanup pending; retaining original owner and lane");reported=true;}
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    },
                }
            }
        });
    }
}
async fn ready(channel: &mut SourceChannel, expected: Digest) -> Result<()> {
    channel.marker(JOB_START).await?;
    match canonical::decode::<Event>(&channel.frame(Duration::from_secs(10), 4096).await?, 4096)? {
        Event::Ready {
            schema_version: 1,
            job_id,
        } if job_id == expected => Ok(()),
        _ => bail!("native build helper not ready for original job"),
    }
}
async fn transfer(
    reader: &mut Reader,
    input: &mut tokio::process::ChildStdin,
    expected: u64,
) -> Result<()> {
    let mut left = expected;
    let mut bytes = [0; 64 * 1024];
    while left > 0 {
        let limit = left.min(bytes.len() as u64) as usize;
        let n = reader.read(&mut bytes[..limit]).await?;
        if n == 0 {
            bail!("archive build input ended early");
        }
        input.write_all(&bytes[..n]).await?;
        left -= n as u64;
    }
    if reader.read(&mut bytes[..1]).await? != 0 {
        bail!("archive build input has extra bytes");
    }
    Ok(())
}
async fn finish_channel(
    channel: &mut SourceChannel,
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    spec: &ContainerSpec,
) -> Result<()> {
    if !channel.child.wait().await?.success() {
        bail!("native build helper failed");
    }
    timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
        .await
        .context("build diagnostics did not close")???;
    runtime
        .confirm_quarantine_marketplace_success(handle, &spec.labels)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn build_profiles_keep_original_identity_and_exclude_quarantine_and_secrets() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let path = root.path().canonicalize()?.join("lane");
        let source = super::super::build_inputs::tests::compiled_source(&path).await?;
        let job = &source.source.job;
        let expires = job.expires;
        let descriptor = profile::execution(&job.plan.source_match, 30_000)?;
        let slot = job._lease.storage_namespace()?;
        let base = job.source_match_profile()?;
        for mode in ["--build-output-init", "--build-run", "--build-output-check"] {
            let spec = profile::configure(&base, &slot, &descriptor, mode)?;
            assert_eq!(spec.name, base.name);
            assert_eq!(spec.network_mode.as_deref(), Some("none"));
            assert_eq!(spec.volumes.len(), 1);
            assert_eq!(spec.volumes[0].container_path, "/build-output");
            assert_ne!(spec.volumes[0].host_path, job.volume.name);
            assert!(spec.env.is_empty());
            assert!(spec.ports.is_empty());
            assert!(spec.devices.is_empty());
            super::super::super::runtime::source_writer::SourceWriterIntent::capture(&spec)?;
            assert_eq!(
                spec.security.user.as_deref(),
                Some(if mode == "--build-output-init" {
                    "0:0"
                } else {
                    "10004:10004"
                })
            );
            if mode == "--build-output-init" {
                assert_eq!(spec.cap_add, ["CAP_CHOWN"]);
            }
            assert_eq!(spec.volumes[0].read_only, mode == "--build-output-check");
            for fault in 0..7 {
                let mut wrong = spec.clone();
                match fault {
                    0 => wrong.network_mode = Some("host".into()),
                    1 => wrong.volumes[0].container_path = "/quarantine".into(),
                    2 => wrong.volumes[0].source_kind = VolumeMountSourceKind::Bind,
                    3 => wrong.cap_add.push("DAC_OVERRIDE".into()),
                    4 => wrong.security.tmpfs[0].no_exec = false,
                    5 => wrong
                        .labels
                        .insert(profile::SLOT_LABEL.into(), "foreign".into())
                        .map(|_| ())
                        .unwrap_or(()),
                    _ => wrong.security.file_size_limit_bytes = Some(u64::MAX),
                }
                assert!(
                    profile::validate_spec(&wrong).is_err(),
                    "{mode} fault {fault}"
                );
            }
        }
        assert_eq!(job.expires, expires);
        assert!(job.activity.is_none());
        assert!(ControllerLease::acquire(&path).is_err());
        Ok(())
    }
    #[test]
    fn toolchain_approval_is_separate_empty_by_default_and_bounded() -> Result<()> {
        let digest = Digest::of(b"publisher-chosen compiler");
        assert!(
            profile::ApprovedToolchains::default()
                .require(digest)
                .is_err()
        );
        let policy = profile::ApprovedToolchains::from_operator_digests([digest])?;
        assert_eq!(policy.require(digest)?, digest.to_string());
        assert!(policy.require(Digest::of(b"other")).is_err());
        assert!(
            profile::ApprovedToolchains::from_operator_digests((0u8..9).map(|n| Digest::of([n])))
                .is_err()
        );
        assert!(profile::name("../foreign").is_err());
        Ok(())
    }
    #[test]
    fn build_control_is_closed_and_a_compiler_cannot_claim_an_exporter_match() -> Result<()> {
        let id = Digest::of(b"job");
        let ready = serde_json::json!({"type":"ready","schema_version":1,"job_id":id});
        assert!(canonical::decode::<Event>(&canonical::to_vec(&ready)?, 4096).is_ok());
        let mut forged = ready;
        forged["approved"] = serde_json::json!(true);
        assert!(canonical::decode::<Event>(&canonical::to_vec(&forged)?, 4096).is_err());
        let bad = serde_json::json!({"type":"complete","schema_version":1,"job_id":id,"integrity_only":true});
        assert!(canonical::decode::<Event>(&canonical::to_vec(&bad)?, 4096).is_err());
        Ok(())
    }
}
