//! Original-source/image-owned compiled calls. No public route or installed
//! activation is added here; those must retain actual reviewed provenance.
use super::*;
use crate::marketplace::runtime::container_execution as profile;
use elixir_marketplace::container_execution::{ContainerExecutionDescriptor, GUARDIAN_PATH};

impl SourceMatchedQuarantine {
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
    pub(in crate::marketplace) async fn evaluate_container(
        &mut self,
        image: &super::super::image_reference::RuntimeImage,
        mode: SourceExecutionMode,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        image.require_source(self)?;
        let runtime = self.source.job.runtime.clone();
        let guardian = runtime
            .marketplace_container_guardian()
            .context("compiled execution guardian is not independently configured")?;
        let descriptor = ContainerExecutionDescriptor {
            schema_version: 1,
            invocation_id: Digest::of(uuid::Uuid::new_v4().as_bytes()),
            source: self.execution_descriptor(),
            image: image.image().clone(),
            limits,
            mode,
        };
        elixir_marketplace::container_execution::validate_arguments(&descriptor, arguments)?;
        if descriptor.limits.minimum_memory_bytes_for(mode)?
            > self.source.job.plan.limits.memory_bytes
        {
            bail!("compiled invocation exceeds the original admitted memory allocation");
        }
        let expires = self.expires().min(
            Instant::now() + Duration::from_millis(u64::from(descriptor.limits.timeout_millis)),
        );
        // Keep this multi-phase future off callers' finite native stacks.
        // Pinning changes neither its owner/deadline nor cancellation cleanup.
        let operation = Box::pin(async {
            broker.current().await?;
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
                .launch_writer_budgeted(
                    &spec,
                    expires,
                    Some((
                        descriptor.limits.control_input_allowance_for(mode)?,
                        descriptor.limits.control_output_allowance_for(mode)? + 64 * 1024,
                    )),
                )
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
