//! Original-source/image-owned compiled calls. No public route or installed
//! activation is added here; those must retain actual reviewed provenance.
use super::*;
use crate::marketplace::runtime::container_execution as profile;
use elixir_marketplace::{
    container_execution::{
        ContainerExecutionDescriptor, GUARDIAN_PATH, LIFECYCLE_RESULT, PROBE_RESULT,
    },
    source_execution::NO_RESERVATION,
};

/// The worker image's contract for Unrestricted compiled calls (the guardian
/// accepts the profile with no frame, SDK, memory or CPU limit). Images
/// without it keep running the finite profiles.
const CONTAINER_UNRESTRICTED_LABEL: &str =
    "elixir.marketplace.worker.container_execution_unrestricted";

/// Configuration check only, against the job's pinned worker image labels.
/// Finite profiles need nothing new; Unrestricted needs the explicit contract.
fn require_container_profile(
    labels: &std::collections::HashMap<String, String>,
    limits: &SourceExecutionLimits,
) -> Result<()> {
    if limits.is_unrestricted()
        && labels.get(CONTAINER_UNRESTRICTED_LABEL).map(String::as_str) != Some("1")
    {
        bail!(
            "compiled invocation image lacks the unrestricted container execution profile \
             ({CONTAINER_UNRESTRICTED_LABEL}=1); update the worker image or use a finite profile"
        );
    }
    Ok(())
}

/// Memory admission for one compiled call. Unrestricted returns
/// NO_RESERVATION (0): there is no minimum to compare.
///
/// The per-call container's cgroup memory limit is not this value: it is the
/// job plan's `memory_bytes`, which the guardian and kernel inspection see.
/// For Unrestricted the server should create the job with its 1 TiB
/// no-ceiling budget (`installed_startup::UNBOUNDED_BYTES`), so `memory.max`
/// stays a readable number; the guardian then requires no particular value.
fn require_memory(
    limits: &SourceExecutionLimits,
    mode: SourceExecutionMode,
    admitted: u64,
) -> Result<()> {
    let minimum = limits.minimum_memory_bytes_for(mode)?;
    if minimum != NO_RESERVATION && minimum > admitted {
        bail!("compiled invocation exceeds the original admitted memory allocation");
    }
    Ok(())
}

/// Private (input, output) bytes charged to the job budget before launch.
/// Unrestricted reserves nothing (`None`), never a 0-byte budget; finite
/// profiles keep their control allowances plus 64 KiB of diagnostics.
fn io_reservation(
    limits: &SourceExecutionLimits,
    mode: SourceExecutionMode,
) -> Result<Option<(u64, u64)>> {
    let input = limits.control_input_allowance_for(mode)?;
    let output = limits.control_output_allowance_for(mode)?;
    if limits.is_unrestricted() {
        return Ok(None);
    }
    Ok(Some((input, output + 64 * 1024)))
}

fn require_acknowledgement(mode: SourceExecutionMode, result: &RuntimeJson) -> Result<()> {
    let expected = match mode {
        SourceExecutionMode::Probe => PROBE_RESULT,
        SourceExecutionMode::Lifecycle => LIFECYCLE_RESULT,
        SourceExecutionMode::Invoke => bail!("invoke is not a compiled readiness acknowledgement"),
    };
    if result.as_str().as_bytes() != expected {
        bail!("compiled mode lacks its runner-generated ABI acknowledgement");
    }
    Ok(())
}

impl SourceMatchedQuarantine {
    /// Interface/clean-exit probe only, never installation or readiness authority.
    pub(in crate::marketplace) async fn probe_container(
        &mut self,
        image: &super::super::image_reference::RuntimeImage,
        limits: SourceExecutionLimits,
        broker: &mut dyn Broker,
    ) -> Result<()> {
        let result = Box::pin(self.evaluate_container(
            image,
            SourceExecutionMode::Probe,
            limits,
            &RuntimeJson::parse(b"null")?,
            broker,
        ))
        .await?;
        require_acknowledgement(SourceExecutionMode::Probe, &result)
    }
    /// Exact original candidate execution ID, not a random replacement claim.
    pub(in crate::marketplace) async fn lifecycle_container(
        &mut self,
        image: &super::super::image_reference::RuntimeImage,
        limits: SourceExecutionLimits,
        context: &SourceLifecycleContext,
        execution: Digest,
        broker: &mut dyn CandidateBroker,
    ) -> Result<()> {
        context.require_target(&self.release().version)?;
        let result = Box::pin(self.evaluate_container_as(
            image,
            SourceExecutionMode::Lifecycle,
            limits,
            &context.arguments()?,
            broker,
            execution,
        ))
        .await?;
        require_acknowledgement(SourceExecutionMode::Lifecycle, &result)
    }
    pub(in crate::marketplace) async fn invoke_container(
        &mut self,
        image: &super::super::image_reference::RuntimeImage,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        self.evaluate_container(
            image,
            SourceExecutionMode::Invoke,
            limits,
            arguments,
            broker,
        )
        .await
    }
    async fn evaluate_container(
        &mut self,
        image: &super::super::image_reference::RuntimeImage,
        mode: SourceExecutionMode,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        if mode == SourceExecutionMode::Lifecycle {
            bail!("compiled lifecycle requires its explicit original execution identity");
        }
        Box::pin(self.evaluate_container_as(
            image,
            mode,
            limits,
            arguments,
            broker,
            Digest::of(uuid::Uuid::new_v4().as_bytes()),
        ))
        .await
    }
    async fn evaluate_container_as(
        &mut self,
        image: &super::super::image_reference::RuntimeImage,
        mode: SourceExecutionMode,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
        execution: Digest,
    ) -> Result<RuntimeJson> {
        image.require_source(self)?;
        let runtime = self.source.job.runtime.clone();
        let guardian = runtime
            .marketplace_container_guardian()
            .context("compiled execution guardian is not independently configured")?;
        let descriptor = ContainerExecutionDescriptor {
            schema_version: 1,
            invocation_id: execution,
            source: self.execution_descriptor(),
            image: image.image().clone(),
            limits,
            mode,
        };
        elixir_marketplace::container_execution::validate_arguments(&descriptor, arguments)?;
        require_memory(
            &descriptor.limits,
            mode,
            self.source.job.plan.limits.memory_bytes,
        )?;
        let reservation = io_reservation(&descriptor.limits, mode)?;
        let expires = self.expires().min(
            Instant::now() + Duration::from_millis(u64::from(descriptor.limits.timeout_millis)),
        );
        // Keep this multi-phase future off callers' finite native stacks.
        // Pinning changes neither its owner/deadline nor cancellation cleanup.
        let operation = Box::pin(async {
            broker.current().await?;
            if descriptor.limits.is_unrestricted() {
                let labels = runtime.image_labels(&self.source.job.volume.image).await?;
                require_container_profile(&labels, &descriptor.limits)?;
            }
            self.source.job.cleanup_activity(runtime.as_ref()).await?;
            self.source.job.recover_writer(runtime.as_ref()).await?;
            image.require_source(self)?;
            let mut spec = self.source.job.source_match_profile()?;
            profile::prepare(&mut spec, &descriptor, &guardian)?;
            // Exact independent image import provenance, not image labels or a
            // publisher-supplied config digest, must survive fresh admission.
            let view = image.runtime_view(self)?;
            let handle = self
                .source
                .job
                .launch_writer_budgeted(&spec, expires, reservation)
                .await?;
            image.require_source(self)?;
            let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
            let mut channel = SourceChannel::new(process)?;
            guarded(broker, channel.marker(JOB_START)).await?;
            match guarded(
                broker,
                channel.execution_event(descriptor.job_id()?, Duration::from_secs(10)),
            )
            .await?
            {
                Event::Ready { .. } => (),
                _ => bail!("compiled guardian did not become ready"),
            }
            guarded(broker, async {
                guardian.require()?;
                let actual = runtime
                    .describe_container_runtime_state(&handle.id)
                    .await?
                    .context("compiled kernel/config inspection unavailable")?;
                profile::verify_configuration(&spec, &actual, &self.source.job.plan.limits)?;
                let identity = ContainerHandle {
                    id: handle.id.clone(),
                    name: handle.id.clone(),
                };
                let command = [GUARDIAN_PATH.to_owned(), "--kernel-report".into()];
                let report = runtime
                    .exec_container_command(&identity, "10004:10004", &command)
                    .await?;
                profile::verify_kernel(&report, &self.source.job.plan.limits)?;
                image.require_source(self)?;
                view.require_descriptor(&descriptor)
            })
            .await?;
            let (_scope, invocation) = native::NativeScope::open_container(
                runtime.clone(),
                handle.clone(),
                &descriptor,
                self.source.job._lease.clone(),
                expires,
                view,
            )?;
            broker.native_started(invocation)?;
            let result = exchange_bound(
                &mut channel,
                descriptor.job_id()?,
                &descriptor.limits,
                arguments,
                broker,
            )
            .await?;
            runtime
                .confirm_quarantine_marketplace_success(&handle, &spec.labels)
                .await?;
            image.require_source(self)?;
            Ok::<_, anyhow::Error>(result)
        });
        let result = timeout_at(expires, operation)
            .await
            .context("compiled invocation absolute deadline exceeded")
            .and_then(|r| r);
        let effects = broker.retire_effects().await;
        self.source.job.cleanup_activity(runtime.as_ref()).await?;
        effects?;
        if result.is_err() {
            self.source.job.recover_writer(runtime.as_ref()).await?;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Image = super::super::super::image_reference::RuntimeImage;
    #[test]
    fn compiled_acknowledgements_require_the_abi_not_source_js_exports_or_publisher_null()
    -> Result<()> {
        require_acknowledgement(
            SourceExecutionMode::Probe,
            &RuntimeJson::parse(PROBE_RESULT)?,
        )?;
        require_acknowledgement(
            SourceExecutionMode::Lifecycle,
            &RuntimeJson::parse(LIFECYCLE_RESULT)?,
        )?;
        assert!(
            require_acknowledgement(
                SourceExecutionMode::Probe,
                &RuntimeJson::parse(SOURCE_PROBE_RESULT)?
            )
            .is_err()
        );
        assert!(
            require_acknowledgement(
                SourceExecutionMode::Lifecycle,
                &RuntimeJson::parse(SOURCE_LIFECYCLE_RESULT)?
            )
            .is_err()
        );
        assert!(
            require_acknowledgement(SourceExecutionMode::Probe, &RuntimeJson::parse(b"null")?)
                .is_err()
        );
        assert!(
            require_acknowledgement(
                SourceExecutionMode::Lifecycle,
                &RuntimeJson::parse(PROBE_RESULT)?
            )
            .is_err()
        );
        Ok(())
    }
    #[test]
    fn unrestricted_compiled_calls_need_the_image_contract_and_finite_calls_do_not() -> Result<()> {
        let unrestricted = SourceExecutionLimits::unrestricted();
        let finite = SourceExecutionLimits::large_feed();
        let mut labels = std::collections::HashMap::new();
        // Old images keep working with finite profiles.
        require_container_profile(&labels, &finite)?;
        let error = require_container_profile(&labels, &unrestricted)
            .err()
            .context("unrestricted compiled call admitted without the image contract")?;
        assert!(format!("{error:#}").contains(CONTAINER_UNRESTRICTED_LABEL));
        // The QuickJS runtime's contract is not the compiled one.
        labels.insert(
            "elixir.marketplace.worker.source_eval_unrestricted".to_owned(),
            "1".to_owned(),
        );
        assert!(require_container_profile(&labels, &unrestricted).is_err());
        labels.insert(CONTAINER_UNRESTRICTED_LABEL.to_owned(), "0".to_owned());
        assert!(require_container_profile(&labels, &unrestricted).is_err());
        labels.insert(CONTAINER_UNRESTRICTED_LABEL.to_owned(), "1".to_owned());
        require_container_profile(&labels, &unrestricted)?;
        require_container_profile(&labels, &finite)?;
        Ok(())
    }
    #[test]
    fn unrestricted_compiled_calls_reserve_no_memory_or_io_and_finite_calls_keep_theirs()
    -> Result<()> {
        let invoke = SourceExecutionMode::Invoke;
        let unrestricted = SourceExecutionLimits::unrestricted();
        // No minimum and no I/O budget, whatever the job admitted.
        for admitted in [0, 64 * 1024 * 1024, 1 << 40] {
            require_memory(&unrestricted, invoke, admitted)?;
        }
        assert_eq!(io_reservation(&unrestricted, invoke)?, None);
        // Still Invoke only.
        for mode in [SourceExecutionMode::Probe, SourceExecutionMode::Lifecycle] {
            assert!(require_memory(&unrestricted, mode, 1 << 40).is_err());
            assert!(io_reservation(&unrestricted, mode).is_err());
        }
        let standard = SourceExecutionLimits {
            profile: Default::default(),
            heap_bytes: 32 * 1024 * 1024,
            timeout_millis: 10_000,
            max_file_bytes: 8 * 1024 * 1024,
            json_bytes: 1024 * 1024,
            pending_jobs: 128,
            sdk: None,
        };
        // Finite profiles keep their exact minimum and reservation.
        for finite in [standard, SourceExecutionLimits::large_feed()] {
            let minimum = finite.minimum_memory_bytes_for(invoke)?;
            assert!(minimum > NO_RESERVATION);
            require_memory(&finite, invoke, minimum)?;
            assert!(require_memory(&finite, invoke, minimum - 1).is_err());
            assert_eq!(
                io_reservation(&finite, invoke)?,
                Some((
                    finite.control_input_allowance_for(invoke)?,
                    finite.control_output_allowance_for(invoke)? + 64 * 1024,
                ))
            );
        }
        Ok(())
    }
    #[test]
    fn compiled_invocation_keeps_a_bounded_caller_future() {
        fn bytes<F: std::future::Future>(
            _: impl FnOnce(
                &'static mut SourceMatchedQuarantine,
                &'static Image,
                SourceExecutionLimits,
                &'static RuntimeJson,
                &'static mut dyn Broker,
            ) -> F,
        ) -> usize {
            std::mem::size_of::<F>()
        }
        // Type measurement only: no fabricated owner, image or native receipt.
        let size = bytes(|source, image, limits, arguments, broker| {
            source.invoke_container(image, limits, arguments, broker)
        });
        assert!(
            size <= 128 * 1024,
            "compiled invocation future too large: {size}"
        );
    }
}
