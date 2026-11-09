//! Cancellation-safe ownership of ONE native startup under the existing lane.
//! No new queue, execution authority, resource credit or filesystem allocation.
use super::*;
use tokio::sync::oneshot;

pub(super) enum Operation {
    BuildOutput(String),
    Writer(Box<ContainerSpec>, SourceJobLimits),
    Provision(Box<QuarantineMarketplaceVolumeSpec>),
    Measurement(Box<crate::runtime::measurement::MeasurementSpec>),
}

pub(super) struct Launch {
    reply: oneshot::Receiver<Result<Started>>,
    finished: bool,
}
impl Launch {
    pub(super) fn begin(
        runtime: Arc<dyn RuntimeManager>,
        lease: Arc<ControllerLease>,
        fence: Fence,
        operation: Operation,
        expires: Instant,
    ) -> Self {
        let (reply, response) = oneshot::channel();
        tokio::spawn(async move {
            let mut started = Started {
                runtime: runtime.clone(),
                lease,
                fence: Some(fence),
                handle: None,
                error: None,
            };
            // Caller cancellation does not cancel this future or its lease.
            // The original absolute deadline still bounds startup; an unknown
            // create outcome at that deadline stays durably fenced.
            let result = timeout_at(expires, async {
                match operation {
                    Operation::BuildOutput(slot) => {
                        runtime
                            .create_marketplace_build_output_volume(&slot)
                            .await?;
                        started
                            .fence
                            .as_mut()
                            .context("build output allocation lost its fence")?
                            .clear(&started.lease)?;
                        started.fence = None;
                    }
                    Operation::Measurement(spec) => {
                        let handle = runtime.create_host_measurement(&spec).await?;
                        started.handle = Some(handle.clone());
                        started
                            .fence
                            .as_mut()
                            .context("measurement startup lost its fence")?
                            .bind_measurement(&started.lease, &handle.id)?;
                        runtime
                            .start_host_measurement(
                                &ContainerHandle {
                                    id: handle.id.clone(),
                                    name: handle.id.clone(),
                                },
                                &spec,
                            )
                            .await?;
                    }
                    Operation::Writer(spec, limits) => {
                        let handle = runtime.create_quarantine_marketplace_writer(&spec).await?;
                        started.handle = Some(handle.clone());
                        started
                            .fence
                            .as_mut()
                            .context("native startup lost its fence")?
                            .bind_writer(&started.lease, &handle.id)?;
                        // A start can lose its response, but its exact immutable
                        // identity is already durable before execution begins.
                        runtime
                            .start_quarantine_marketplace_writer(
                                &ContainerHandle {
                                    id: handle.id.clone(),
                                    name: handle.id.clone(),
                                },
                                &spec,
                                &limits,
                            )
                            .await?;
                    }
                    Operation::Provision(spec) => {
                        runtime.ensure_quarantine_marketplace_volume(&spec).await?;
                        started
                            .fence
                            .as_mut()
                            .context("native provisioning lost its fence")?
                            .clear(&started.lease)?;
                        started.fence = None;
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("owned native startup deadline exceeded")
            .and_then(|value| value);
            // Even a failed start hands off its ACTUAL owner. Throwing away
            // that owner would poison known identities as permanently unknown
            // or race an unjoined background cleanup against the caller.
            started.error = result.err();
            let _ = reply.send(Ok(started));
            // A closed reply or dropped queued result drops Started. Known
            // late writers then stop under the retained lease; unknown failures
            // keep their durable fence instead of declaring cleanup complete.
        });
        Self {
            reply: response,
            finished: false,
        }
    }
    pub(super) async fn wait(&mut self) -> Result<Started> {
        if self.finished {
            bail!("native startup result was already consumed");
        }
        let result = (&mut self.reply).await;
        self.finished = true;
        result.context("owned native startup task stopped")?
    }
}

pub(super) struct Started {
    runtime: Arc<dyn RuntimeManager>,
    lease: Arc<ControllerLease>,
    fence: Option<Fence>,
    handle: Option<ContainerHandle>,
    error: Option<anyhow::Error>,
}
impl Started {
    pub(super) fn take_error(&mut self) -> Option<anyhow::Error> {
        self.error.take()
    }
    pub(super) fn adopt(mut self) -> Result<(Option<Fence>, Option<ContainerHandle>)> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        Ok((self.fence.take(), self.handle.take()))
    }
    pub(super) async fn cleanup_retained(&mut self) -> Result<()> {
        let Some(fence) = self.fence.as_ref() else {
            return Ok(());
        };
        let hint = self.target(fence);
        let target = prepare_cleanup_target(
            self.runtime.as_ref(),
            &self.lease,
            self.fence
                .as_mut()
                .context("native startup fence missing")?,
            hint,
        )
        .await?;
        SourceQuarantineJob::stop_activity(self.runtime.as_ref(), &target).await?;
        self.fence
            .as_mut()
            .context("native cleanup fence missing")?
            .clear(&self.lease)?;
        self.fence = None;
        self.handle = None;
        Ok(())
    }
    fn target(&self, fence: &Fence) -> Option<Target> {
        if let Target::Measurement {
            deployment,
            container_id,
        } = fence.target()
        {
            let id = container_id
                .as_ref()
                .or_else(|| self.handle.as_ref().map(|h| &h.id))?;
            if !crate::runtime::packet_network::immutable_id(id) {
                return None;
            }
            return Some(Target::Measurement {
                deployment: deployment.clone(),
                container_id: Some(id.clone()),
            });
        }
        let (deployment, container_id): (&str, Option<&String>) = match fence.target() {
            Target::SourceWriter {
                deployment,
                container_id,
            } => (deployment, container_id.as_ref()),
            Target::WriterCreate { deployment } => (deployment, None),
            Target::WriterCreateIntent { intent } => (intent.deployment(), None),
            _ => return None,
        };
        let id = container_id.or_else(|| self.handle.as_ref().map(|handle| &handle.id))?;
        if !crate::runtime::packet_network::immutable_id(id) {
            return None;
        }
        Some(Target::SourceWriter {
            deployment: deployment.to_owned(),
            container_id: Some(id.clone()),
        })
    }
}
impl Drop for Started {
    fn drop(&mut self) {
        let Some(fence) = self.fence.take() else {
            return;
        };
        let target = self.target(&fence);
        if target.is_none()
            && !matches!(
                fence.target(),
                Target::WriterCreateIntent { .. } | Target::BuildOutputAllocation { .. }
            )
        {
            return;
        }
        let runtime = self.runtime.clone();
        let lease = self.lease.clone();
        if let Ok(executor) = tokio::runtime::Handle::try_current() {
            executor.spawn(async move {
                let _ = cleanup(runtime, lease, fence, target).await;
            });
        }
        // Without an executor (shutdown/crash), the known immutable identity
        // remains durable for ordinary explicit recovery. Never erase by Drop.
    }
}
async fn cleanup(
    runtime: Arc<dyn RuntimeManager>,
    lease: Arc<ControllerLease>,
    mut fence: Fence,
    target: Option<Target>,
) -> Result<()> {
    let target = prepare_cleanup_target(runtime.as_ref(), &lease, &mut fence, target).await?;
    SourceQuarantineJob::stop_activity(runtime.as_ref(), &target).await?;
    fence.clear(&lease)
}

async fn prepare_cleanup_target(
    runtime: &dyn RuntimeManager,
    lease: &ControllerLease,
    fence: &mut Fence,
    hint: Option<Target>,
) -> Result<Target> {
    if matches!(
        fence.target(),
        Target::WriterCreate { .. }
            | Target::WriterCreateIntent { .. }
            | Target::SourceWriter {
                container_id: None,
                ..
            }
    ) {
        if let Some(Target::SourceWriter {
            container_id: Some(id),
            ..
        }) = &hint
        {
            // A known late CID must be durable BEFORE deletion, including a
            // retry after failed initial binding. Never lose it on restart.
            fence.bind_writer(lease, id)?;
        } else {
            fence.reconcile_writer_create(runtime, lease).await?;
        }
    }
    let target = hint.unwrap_or_else(|| fence.target().clone());
    if matches!(
        target,
        Target::SourceWriter {
            container_id: Some(_),
            ..
        } | Target::BuildOutputAllocation { .. }
            | Target::Measurement {
                container_id: Some(_),
                ..
            }
    ) {
        Ok(target)
    } else {
        bail!("native startup identity remains uncertain")
    }
}
