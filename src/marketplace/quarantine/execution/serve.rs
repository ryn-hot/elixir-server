//! Long-lived installed executor ("serve"): one verified module answers many
//! bounded calls. Same pinned worker image, kernel guard and broker contract as
//! a per-call executor. The package and proof travel on the private pipe and
//! the executor verifies them against the signed release before loading any
//! publisher code. See docs/architecture/arr-stack-marketplace-extension.md.
use super::*;
use crate::marketplace::runtime::{
    SERVE_EXTENSION_LABEL, SERVE_ISOLATION, SERVE_NAME_PREFIX, inspect_serve_executor,
};
use elixir_marketplace::{source_execution::SourceServeDescriptor, source_job::SourceMatchSummary};

/// Package verification and module load at start (the executor's own hour
/// limit still applies; this is the controller's patience).
const START_WAIT: Duration = Duration::from_secs(120);
/// After a call's own deadline: time for the result frames to arrive.
const CALL_GRACE: Duration = Duration::from_secs(5);

/// What one call produced. A publisher error leaves the executor serving.
pub(in crate::marketplace) enum ServeOutcome {
    Result(RuntimeJson),
    Failed,
}

pub(in crate::marketplace) struct ServeSession {
    runtime: Arc<dyn RuntimeManager>,
    handle: Option<ContainerHandle>,
    channel: SourceChannel,
    descriptor: SourceServeDescriptor,
    sequence: u64,
    started: Instant,
    used: Instant,
    healthy: bool,
}

impl ServeSession {
    /// The closed container profile. The deployment is a fresh nonce, never the
    /// orchestrator's, so its orphan pruning never matches this executor.
    fn profile(
        image: &str,
        descriptor: &SourceServeDescriptor,
        limits: &SourceJobLimits,
    ) -> Result<ContainerSpec> {
        let deployment = uuid::Uuid::new_v4().simple().to_string();
        let mut spec = ContainerSpec {
            name: format!("{SERVE_NAME_PREFIX}{deployment}"),
            image: image.to_owned(),
            network: String::new(),
            network_mode: Some("none".to_owned()),
            aliases: vec![],
            env: vec![],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::new(),
            command: vec![
                "--source-serve".to_owned(),
                String::from_utf8(canonical::to_vec(descriptor)?)?,
            ],
            cap_add: vec![],
            cap_drop: vec![],
            devices: vec![],
            sysctls: HashMap::new(),
            security: Default::default(),
        };
        prepare_source_worker(&mut spec, limits)?;
        spec.security.tmpfs.retain(|mount| mount.path == "/tmp");
        spec.security.file_size_limit_bytes = Some(1);
        spec.security.stdin_open = true;
        // No fixed CPU limit: the playback governor caps it during playback.
        spec.security.cpu_quota = None;
        spec.labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (
                "elixir.marketplace.isolation".to_owned(),
                SERVE_ISOLATION.to_owned(),
            ),
            (
                ELIXIR_EXTENSION_ID_LABEL.to_owned(),
                SERVE_EXTENSION_LABEL.to_owned(),
            ),
            (
                ELIXIR_INSTANCE_ID_LABEL.to_owned(),
                format!("marketplace-serve-{deployment}"),
            ),
            (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment),
        ]);
        apply_container_spec_fingerprint(&mut spec);
        Ok(spec)
    }

    /// Starts, inspects and loads one executor. Any failure removes the
    /// container before returning.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::marketplace) async fn start(
        runtime: Arc<dyn RuntimeManager>,
        image: &str,
        descriptor: SourceServeDescriptor,
        limits: &SourceJobLimits,
        package: &[u8],
        proof: &[u8],
        expected: &SourceMatchSummary,
        broker: &mut dyn Broker,
    ) -> Result<Self> {
        descriptor.validate()?;
        if descriptor.minimum_memory_bytes()? > limits.memory_bytes {
            bail!("long-lived executor needs more memory than it was given");
        }
        let features = timeout(Duration::from_secs(10), runtime.image_labels(image))
            .await
            .context("worker image inspection deadline")??;
        if features
            .get("elixir.marketplace.worker.source_serve")
            .map(String::as_str)
            != Some("1")
            || features
                .get("elixir.marketplace.worker.source_eval")
                .map(String::as_str)
                != Some("3")
        {
            bail!("the worker image does not support long-lived extension executors");
        }
        let spec = Self::profile(image, &descriptor, limits)?;
        let handle = runtime.create_marketplace_serve_executor(&spec).await?;
        let started = async {
            runtime
                .start_marketplace_serve_executor(&handle, &spec)
                .await?;
            let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
            let mut channel = SourceChannel::new(process)?;
            let job = descriptor.job_id()?;
            guarded(broker, channel.marker(JOB_START)).await?;
            match guarded(
                broker,
                channel.execution_event(job, Duration::from_secs(10)),
            )
            .await?
            {
                Event::Ready { .. } => {}
                _ => bail!("long-lived executor did not become ready"),
            }
            // Actual configuration and kernel guards, before any package byte.
            guarded(
                broker,
                inspect_serve_executor(runtime.as_ref(), &handle, &spec, limits),
            )
            .await?;
            for (kind, bytes) in [(PayloadKind::Package, package), (PayloadKind::Proof, proof)] {
                for event in SourcePayloadFrames::material(job, kind, bytes)? {
                    guarded(broker, channel.execution_write(&event)).await?;
                }
            }
            guarded(broker, channel.marker(JOB_END)).await?;
            match guarded(broker, channel.execution_event(job, START_WAIT)).await? {
                Event::Verified { result, .. }
                    if canonical::to_vec(result.as_ref())? == canonical::to_vec(expected)? => {}
                Event::Verified { .. } => {
                    bail!("long-lived executor verified a different package")
                }
                Event::Rejected { .. } => {
                    bail!("long-lived executor refused the installed package")
                }
                _ => bail!("long-lived executor start protocol rejected"),
            }
            Ok(channel)
        }
        .await;
        match started {
            Ok(channel) => Ok(Self {
                runtime,
                handle: Some(handle),
                channel,
                descriptor,
                sequence: 0,
                started: Instant::now(),
                used: Instant::now(),
                healthy: true,
            }),
            Err(error) => {
                let _ = runtime.remove_container(&handle).await;
                Err(error)
            }
        }
    }

    /// One call. Any transport or protocol failure, or the call's deadline,
    /// leaves this session unhealthy; the owner must stop it.
    pub(in crate::marketplace) async fn call(
        &mut self,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<ServeOutcome> {
        if !self.healthy {
            bail!("long-lived executor is not serving");
        }
        if arguments.accounted_bytes() > self.descriptor.limits.profile.maximum_argument_bytes() {
            bail!("long-lived call arguments exceed limit");
        }
        self.healthy = false;
        self.sequence += 1;
        let job = self.descriptor.call_id(self.sequence)?;
        let limits = self.descriptor.limits.clone();
        let wait = Duration::from_millis(u64::from(limits.timeout_millis)) + CALL_GRACE;
        let outcome = timeout(wait, self.exchange(job, &limits, arguments, broker))
            .await
            .context("long-lived call deadline exceeded")??;
        self.healthy = true;
        self.used = Instant::now();
        Ok(outcome)
    }

    async fn exchange(
        &mut self,
        job: Digest,
        limits: &SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<ServeOutcome> {
        let channel = &mut self.channel;
        broker.current().await?;
        channel
            .execution_send(job, PayloadKind::Arguments, arguments, broker)
            .await?;
        guarded(broker, channel.marker(JOB_END)).await?;
        let mut counters = SdkCounters::default();
        loop {
            let event = guarded(
                broker,
                channel
                    .execution_event(job, Duration::from_millis(u64::from(limits.timeout_millis))),
            )
            .await?;
            match event {
                Event::SdkCall { call_id, .. } => {
                    answer_sdk_call(channel, job, limits, broker, &mut counters, call_id).await?;
                }
                first @ Event::Payload {
                    kind: PayloadKind::Result,
                    ..
                } => {
                    let result = channel
                        .execution_payload(
                            first,
                            job,
                            PayloadKind::Result,
                            limits.json_bytes as usize,
                            broker,
                        )
                        .await?;
                    broker.current().await?;
                    guarded(broker, channel.marker(RESULT_ACK)).await?;
                    return Ok(ServeOutcome::Result(result));
                }
                // A publisher error: the executor says Ready again for this
                // same call ID when it keeps serving; otherwise it has exited.
                Event::Rejected { .. } => {
                    match guarded(broker, channel.execution_event(job, Duration::from_secs(5)))
                        .await?
                    {
                        Event::Ready { .. } => return Ok(ServeOutcome::Failed),
                        _ => bail!("long-lived executor stopped after a failed call"),
                    }
                }
                _ => bail!("long-lived call protocol rejected"),
            }
        }
    }

    pub(in crate::marketplace) fn healthy(&self) -> bool {
        self.healthy
    }
    pub(in crate::marketplace) fn age(&self) -> Duration {
        self.started.elapsed()
    }
    pub(in crate::marketplace) fn idle(&self) -> Duration {
        self.used.elapsed()
    }
    pub(in crate::marketplace) fn container_id(&self) -> Option<&str> {
        self.handle.as_ref().map(|handle| handle.id.as_str())
    }

    /// Closes the private input (a clean stop between calls) and removes the
    /// container, which also ends it if it did not stop on its own.
    pub(in crate::marketplace) async fn stop(mut self) -> Result<()> {
        self.healthy = false;
        self.channel.input = None;
        let _ = timeout(Duration::from_secs(2), self.channel.child.wait()).await;
        match self.handle.take() {
            Some(handle) => self.runtime.remove_container(&handle).await,
            None => Ok(()),
        }
    }
}

impl Drop for ServeSession {
    /// A session dropped without `stop` (a cancelled caller) still goes away.
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let runtime = self.runtime.clone();
            if let Ok(executor) = tokio::runtime::Handle::try_current() {
                executor.spawn(async move {
                    let _ = runtime.remove_container(&handle).await;
                });
            }
        }
    }
}
