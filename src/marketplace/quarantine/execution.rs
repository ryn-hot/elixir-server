//! Private, owner-directed source calls. No HTTP/peer operation, installation,
//! activation, networking or reusable execution authority is introduced here.
use super::*;
use elixir_marketplace::source_execution::{
    MAX_ARGUMENT_BYTES, MAX_EVENT_BYTES, PayloadKind, RESULT_ACK, RuntimeJson,
    SOURCE_LIFECYCLE_RESULT, SOURCE_PROBE_RESULT, SdkStatus, SourceExecutionDescriptor,
    SourceExecutionEvent as Event, SourceExecutionLimits, SourceExecutionMode,
    SourceLifecycleContext, SourcePayloadFrames, SourcePayloadReceiver, SourceSdkReply,
    SourceSdkRequest,
};

#[cfg(unix)]
mod container;
mod native;
mod retained;
#[cfg(unix)]
pub(in crate::marketplace) use native::GatewayLaunch;
pub(in crate::marketplace) use native::{HttpLaunch, NativeInvocation};
pub(in crate::marketplace) use retained::RetainedCode;

// Configuration check only, NOT evidence of a running executor or permission
// to execute. Called before native creation against the pinned image's labels.
fn require_execution_features(
    features: &HashMap<String, String>,
    mode: SourceExecutionMode,
    limits: &SourceExecutionLimits,
) -> Result<()> {
    limits.validate_for(mode)?;
    if features
        .get("elixir.marketplace.worker.source_eval")
        .map(String::as_str)
        != Some("3")
    {
        bail!("source invocation image lacks independently guarded SDK protocol");
    }
    if limits.profile == elixir_marketplace::source_execution::SourceExecutionProfile::LargeFeed
        && features
            .get("elixir.marketplace.worker.source_eval_large_feed")
            .map(String::as_str)
            != Some("1")
    {
        bail!("source invocation image lacks the finite large-feed execution profile");
    }
    let extra = match mode {
        SourceExecutionMode::Invoke => None,
        SourceExecutionMode::Probe => Some("elixir.marketplace.worker.source_probe"),
        SourceExecutionMode::Lifecycle => Some("elixir.marketplace.worker.source_lifecycle"),
    };
    if extra.is_some_and(|name| features.get(name).map(String::as_str) != Some("1")) {
        bail!("source image lacks its selected no-effects probe or local lifecycle contract");
    }
    Ok(())
}

/// Implemented by the authenticated controller, never the worker or a DTO.
/// Foreground calls require live publisher/head/protection and explicit owner
/// authority. Installed calls require exact durable enrollment and independently
/// matched retained bytes instead. Neither accepts a stored grant alone.
/// Effects recheck their scope in the actual transaction. A read stall does not
/// suspend these checks.
#[async_trait::async_trait]
pub(in crate::marketplace) trait Broker: Send {
    /// Called only after actual native Ready AND independent executor checks,
    /// before arguments or SDK effects. Installed brokers must retain this
    /// revocable view; foreground/probe brokers have their separate live scope.
    fn native_started(&mut self, _invocation: NativeInvocation) -> Result<()> {
        Ok(())
    }
    async fn current(&mut self) -> Result<()>;
    /// Retire ORIGINAL SDK/native children before clearing the source parent.
    /// Owners stay outside this fallible exchange; cancelled waits retain them.
    async fn retire_effects(&mut self) -> Result<()> {
        Ok(())
    }
    async fn request(
        &mut self,
        request: &RuntimeJson,
        maximum_reply_bytes: usize,
    ) -> Result<SourceSdkReply>;
}
/// Only the separately owned candidate controller may implement this for a
/// disabled isolated namespace. Active/foreground brokers are NOT candidates.
/// This type distinction itself is not native ownership or readiness proof.
pub(in crate::marketplace) trait CandidateBroker: Broker {}

/// Keep a partially consumed frame future alive during heartbeat checks. Dropping
/// and recreating read_exact at each tick would lose private-stream framing.
async fn guarded<T>(
    broker: &mut dyn Broker,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    broker.current().await?;
    tokio::pin!(future);
    loop {
        tokio::select! {
            result = &mut future => return result,
            _ = tokio::time::sleep(Duration::from_millis(500)) => broker.current().await?,
        }
    }
}

impl SourceChannel {
    async fn execution_event(&mut self, job: Digest, wait: Duration) -> Result<Event> {
        Event::parse(&self.frame(wait, MAX_EVENT_BYTES).await?, job)
            // Serde can echo an attacker-chosen variant/field name. This private
            // stream can carry secrets: do not retain its raw parser error chain.
            .map_err(|_| anyhow::anyhow!("invalid private source event"))
    }
    async fn execution_write(&mut self, event: &Event) -> Result<()> {
        // The shared codec validates the complete closed frame before writing.
        // One frame only, never a buffered invocation/SDK transcript.
        let mut bytes = Vec::new();
        event.write(&mut bytes)?;
        self.input
            .as_mut()
            .context("source input is closed")?
            .write_all(&bytes)
            .await?;
        self.input
            .as_mut()
            .context("source input is closed")?
            .flush()
            .await?;
        Ok(())
    }
    async fn execution_payload(
        &mut self,
        first: Event,
        job: Digest,
        kind: PayloadKind,
        maximum: usize,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        let mut receiver = SourcePayloadReceiver::new(first, job, kind, maximum)?;
        loop {
            let next = guarded(broker, self.execution_event(job, Duration::from_secs(10))).await?;
            if let Some(value) = receiver
                .accept(next)
                .map_err(|_| anyhow::anyhow!("invalid private source payload"))?
            {
                return Ok(value);
            }
        }
    }
    async fn execution_send(
        &mut self,
        job: Digest,
        kind: PayloadKind,
        json: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<()> {
        for event in SourcePayloadFrames::new(job, kind, json)? {
            guarded(broker, self.execution_write(&event)).await?;
        }
        Ok(())
    }
}

/// After independently inspected Ready. Parent enforces SDK limits as well as
/// the interpreter: a compromised child cannot overrun them or return a result
/// before completing the previous call. No response body is logged or Debugged.
async fn exchange(
    channel: &mut SourceChannel,
    descriptor: &SourceExecutionDescriptor,
    arguments: &RuntimeJson,
    broker: &mut dyn Broker,
) -> Result<RuntimeJson> {
    exchange_bound(
        channel,
        descriptor.job_id()?,
        &descriptor.limits,
        arguments,
        broker,
    )
    .await
}
// Wire/session limits only, not a constructor for native/grant authority.
// Each caller must first establish its distinct actual native provenance.
async fn exchange_bound(
    channel: &mut SourceChannel,
    job: Digest,
    limits: &SourceExecutionLimits,
    arguments: &RuntimeJson,
    broker: &mut dyn Broker,
) -> Result<RuntimeJson> {
    broker.current().await?;
    channel
        .execution_send(job, PayloadKind::Arguments, arguments, broker)
        .await?;
    guarded(broker, channel.marker(JOB_END)).await?;
    let mut calls = 0u32;
    let mut requests = 0u64;
    let mut responses = 0u64;
    loop {
        // Large response parsing/computation can be quiet. The enclosing
        // original absolute job/invocation deadline AND native guardian still
        // bound the entire exchange; frames/SDK calls never extend them.
        let event = guarded(
            broker,
            channel.execution_event(job, Duration::from_millis(u64::from(limits.timeout_millis))),
        )
        .await?;
        match event {
            Event::SdkCall { call_id, .. } => {
                let allowance = limits
                    .sdk
                    .as_ref()
                    .context("source SDK transport is disabled")?;
                calls += 1;
                if call_id != calls || calls > allowance.calls {
                    bail!("source SDK call sequence or allowance differs");
                }
                let remaining = allowance
                    .request_bytes
                    .saturating_sub(requests)
                    .min(MAX_ARGUMENT_BYTES as u64) as usize;
                if remaining == 0 {
                    bail!("source SDK request allowance exhausted");
                }
                let first = guarded(
                    broker,
                    channel.execution_event(job, Duration::from_secs(10)),
                )
                .await?;
                let raw = channel
                    .execution_payload(first, job, PayloadKind::SdkRequest, remaining, broker)
                    .await?;
                requests = requests
                    .checked_add(raw.accounted_bytes() as u64)
                    .context("SDK request counter overflow")?;
                if requests > allowance.request_bytes {
                    bail!("source SDK request allowance exhausted");
                }
                let operation = SourceSdkRequest::parse(&raw)
                    .map_err(|_| anyhow::anyhow!("invalid private SDK request"))?;
                // Reserve response capacity BEFORE the effect. An effect must
                // not commit when no reply (even JSON true/null) can fit.
                let maximum = allowance
                    .response_bytes
                    .saturating_sub(responses)
                    .min(limits.json_bytes);
                if maximum == 0 {
                    bail!("source SDK reply allowance exhausted");
                }
                broker.current().await?;
                let reply = broker.request(&raw, maximum as usize).await?;
                broker.current().await?;
                let status = match &reply {
                    SourceSdkReply::Denied => SdkStatus::Denied,
                    SourceSdkReply::Unavailable => SdkStatus::Unavailable,
                    SourceSdkReply::Value(json) => {
                        operation.validate_reply(json)?;
                        if json.accounted_bytes() as u64 > maximum {
                            bail!("source SDK reply allowance exhausted");
                        }
                        responses += json.accounted_bytes() as u64;
                        SdkStatus::Ok
                    }
                };
                guarded(
                    broker,
                    channel.execution_write(&Event::SdkReturn {
                        schema_version: 1,
                        job_id: job,
                        call_id,
                        status,
                    }),
                )
                .await?;
                if let SourceSdkReply::Value(json) = reply {
                    channel
                        .execution_send(job, PayloadKind::SdkResponse, &json, broker)
                        .await?;
                }
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
                let mut trailing = [0; 1];
                let size = guarded(broker, async {
                    Ok(channel.output.read(&mut trailing).await?)
                })
                .await?;
                if size != 0 {
                    bail!("unexpected source invocation trailing output");
                }
                let status = guarded(broker, async { Ok(channel.child.wait().await?) }).await?;
                if !status.success() {
                    bail!("source invocation exited unsuccessfully");
                }
                timeout(Duration::from_secs(1), &mut channel.diagnostics.0)
                    .await
                    .context("source invocation diagnostics did not close")???;
                broker.current().await?;
                return Ok(result);
            }
            _ => bail!("source invocation protocol rejected"),
        }
    }
}

impl SourceQuarantinePlan {
    pub(in crate::marketplace) fn require_execution_resources(
        &self,
        resources: &super::super::grants::RuntimeResources,
    ) -> Result<()> {
        let plan = self;
        let tmp_bytes = 8.min(plan.limits.memory_bytes / MIB / 16) * MIB;
        if plan.limits.memory_bytes > resources.memory_bytes
            || plan.limits.cpu_millicores > resources.cpu_millicores
            || super::super::runtime::WORKER_PIDS > u64::from(resources.pids)
            || !resources
                .temporary_mounts
                .iter()
                .any(|m| m.path == "/tmp" && m.bytes >= tmp_bytes)
        {
            bail!("source invocation profile exceeds owner-approved resources");
        }
        Ok(())
    }
}

impl SourceMatchedQuarantine {
    pub(in crate::marketplace) fn execution_descriptor(
        &self,
    ) -> elixir_marketplace::source_job::SourceMatchDescriptor {
        self.source.job.plan.source_match.clone()
    }
    pub(in crate::marketplace) fn require_execution_resources(
        &self,
        resources: &super::super::grants::RuntimeResources,
    ) -> Result<()> {
        self.source.job.plan.require_execution_resources(resources)
    }
    /// Borrowed live public-source fact for the separate controller while the
    /// actual workspace is mutably held by the native lifecycle. No TTL refresh.
    pub(in crate::marketplace) fn execution_source(
        &self,
    ) -> Result<super::super::worker::PublicSourceObservation> {
        self.require_public_source()?;
        Ok(self
            .public_source
            .as_ref()
            .context("source binding missing")?
            .retained_copy())
    }
    /// Explicit controller action only. Approval does not automatically call
    /// this method. The same workspace, lane, admission and cumulative budgets
    /// remain held. Cancellation leaves actual startup/activity fences intact.
    pub(in crate::marketplace) async fn invoke_source(
        &mut self,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        self.evaluate_source(SourceExecutionMode::Invoke, limits, arguments, broker)
            .await
    }
    pub(in crate::marketplace) async fn probe_source(
        &mut self,
        limits: SourceExecutionLimits,
        broker: &mut dyn Broker,
    ) -> Result<()> {
        let result = self
            .evaluate_source(
                SourceExecutionMode::Probe,
                limits,
                &RuntimeJson::parse(b"null")?,
                broker,
            )
            .await?;
        if result.as_str().as_bytes() != SOURCE_PROBE_RESULT {
            bail!("native source probe returned a non-runner interface result");
        }
        Ok(())
    }
    async fn evaluate_source(
        &mut self,
        mode: SourceExecutionMode,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        self.require_public_source()?;
        self.evaluate_native(mode, limits, arguments, broker).await
    }
    /// Only the foreground public-source wrapper and the independently checked
    /// retained-byte owner may reach this shared native lifecycle. No boolean
    /// switch or deserialized receipt bypasses either wrapper's requirements.
    async fn evaluate_native(
        &mut self,
        mode: SourceExecutionMode,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
    ) -> Result<RuntimeJson> {
        self.evaluate_native_as(
            mode,
            limits,
            arguments,
            broker,
            Digest::of(uuid::Uuid::new_v4().as_bytes()),
        )
        .await
    }
    /// An invocation coordinate is data, not execution authority. Candidate
    /// brokers compare it to their original claim AND require actual native
    /// ownership/source/lifecycle inspection before any local state effect.
    async fn evaluate_native_as(
        &mut self,
        mode: SourceExecutionMode,
        limits: SourceExecutionLimits,
        arguments: &RuntimeJson,
        broker: &mut dyn Broker,
        invocation_id: Digest,
    ) -> Result<RuntimeJson> {
        self.ensure_job_active()?;
        evaluate_job(
            &mut self.source.job,
            mode,
            limits,
            arguments,
            broker,
            invocation_id,
            None,
        )
        .await
    }
}

/// Pending installed bytes remain quarantined. Only this actual executor's
/// checked native report can cross the argument/effect boundary; no matched
/// source owner is manufactured from the historical enrollment summary.
pub(super) async fn evaluate_verified(
    source: &mut QuarantinedSource,
    expected: &SourceMatchSummary,
    limits: SourceExecutionLimits,
    arguments: &RuntimeJson,
    broker: &mut dyn Broker,
) -> Result<RuntimeJson> {
    if Instant::now() >= source.job.expires {
        bail!("source quarantine job has expired");
    }
    evaluate_job(
        &mut source.job,
        SourceExecutionMode::Invoke,
        limits,
        arguments,
        broker,
        Digest::of(uuid::Uuid::new_v4().as_bytes()),
        Some(expected),
    )
    .await
}

fn require_verified_feature(
    features: &HashMap<String, String>,
    mode: SourceExecutionMode,
) -> Result<()> {
    if mode != SourceExecutionMode::Invoke
        || features
            .get("elixir.marketplace.worker.source_eval_verified")
            .map(String::as_str)
            != Some("1")
    {
        bail!("installed inline verification requires its exact native invocation contract");
    }
    Ok(())
}
fn require_verified_report(
    event: Event,
    descriptor: &SourceExecutionDescriptor,
    expected: &SourceMatchSummary,
) -> Result<()> {
    let Event::Verified {
        schema_version: 1,
        job_id,
        result,
    } = event
    else {
        bail!("native executor did not verify this source before arguments");
    };
    if job_id != descriptor.job_id()? {
        bail!("native source verification belongs to another invocation");
    }
    expected.matches(&descriptor.source)?;
    result.matches(&descriptor.source)?;
    if canonical::to_vec(&*result)? != canonical::to_vec(expected)? {
        bail!("native executor bytes differ from the enrolled source summary");
    }
    Ok(())
}
async fn evaluate_job(
    job: &mut SourceQuarantineJob,
    mode: SourceExecutionMode,
    limits: SourceExecutionLimits,
    arguments: &RuntimeJson,
    broker: &mut dyn Broker,
    invocation_id: Digest,
    expected: Option<&SourceMatchSummary>,
) -> Result<RuntimeJson> {
    if arguments.accounted_bytes() > MAX_ARGUMENT_BYTES {
        bail!("source invocation arguments exceed limit");
    }
    let descriptor = SourceExecutionDescriptor {
        mode,
        schema_version: 1,
        invocation_id,
        source: job.plan.source_match.clone(),
        limits,
    };
    descriptor.validate()?;
    if descriptor.limits.minimum_memory_bytes_for(mode)? > job.plan.limits.memory_bytes {
        bail!("source invocation exceeds its admitted memory reservation");
    }
    let runtime = job.runtime.clone();
    let expires = job
        .expires
        .min(Instant::now() + Duration::from_millis(u64::from(descriptor.limits.timeout_millis)));
    let operation = async {
        broker.current().await?;
        let features = runtime.image_labels(&job.volume.image).await?;
        require_execution_features(&features, mode, &descriptor.limits)?;
        if expected.is_some() {
            require_verified_feature(&features, mode)?;
        }
        job.cleanup_activity(runtime.as_ref()).await?;
        job.recover_writer(runtime.as_ref()).await?;
        let mut spec = job.source_match_profile()?;
        spec.command = vec![
            if expected.is_some() {
                "--source-eval-verified"
            } else {
                "--source-eval"
            }
            .into(),
            String::from_utf8(canonical::to_vec(&descriptor)?)?,
        ];
        apply_container_spec_fingerprint(&mut spec);
        // No refund after a launch/error/cancel. Native package/Git reads,
        // frames, results, SDK replies and diagnostics share the job budget.
        let handle = job
            .launch_writer_budgeted(
                &spec,
                expires,
                Some((
                    descriptor.input_allowance()?,
                    descriptor.output_allowance()?
                        + 64 * 1024
                        + if expected.is_some() {
                            (MAX_EVENT_BYTES + 4) as u64
                        } else {
                            0
                        },
                )),
            )
            .await?;
        let process = runtime.attach_private_stdio(&handle, &spec.labels).await?;
        let mut channel = SourceChannel::new(process)?;
        guarded(broker, channel.marker(JOB_START)).await?;
        match guarded(
            broker,
            channel.execution_event(descriptor.job_id()?, Duration::from_secs(10)),
        )
        .await?
        {
            Event::Ready { .. } => {}
            _ => bail!("source invocation did not become ready"),
        }
        guarded(
            broker,
            super::super::runtime::inspect_source_executor(
                runtime.as_ref(),
                &handle,
                &spec,
                &job.plan.limits,
                &job.volume.name,
            ),
        )
        .await?;
        if let Some(expected) = expected {
            // This permits only verification in the inspected offline
            // native process. Arguments and SDK authority are still absent.
            guarded(broker, channel.marker(JOB_END)).await?;
            let event = guarded(
                broker,
                channel.execution_event(
                    descriptor.job_id()?,
                    expires.saturating_duration_since(Instant::now()),
                ),
            )
            .await?;
            require_verified_report(event, &descriptor, expected)?;
            // Verification may take time. Bind no original invocation from
            // a pre-verification inspection or a serialized native report.
            guarded(
                broker,
                super::super::runtime::inspect_source_executor(
                    runtime.as_ref(),
                    &handle,
                    &spec,
                    &job.plan.limits,
                    &job.volume.name,
                ),
            )
            .await?;
        }
        let (_scope, invocation) = native::NativeScope::open(
            runtime.clone(),
            handle.clone(),
            &descriptor,
            job._lease.clone(),
            expires,
        )?;
        broker.native_started(invocation)?;
        let result = exchange(&mut channel, &descriptor, arguments, broker).await?;
        runtime
            .confirm_quarantine_marketplace_success(&handle, &spec.labels)
            .await?;
        Ok(result)
    };
    let result = timeout_at(expires, operation)
        .await
        .context("source invocation absolute deadline exceeded")
        .and_then(|r| r);
    // An installed invocation can retain one gateway across several SDK
    // requests. Join its original children BEFORE source-fence cleanup;
    // failure never turns a completed interpreter into an idle controller.
    let effects = broker.retire_effects().await;
    // Detached container success, not merely an attached CLI's exit. A
    // cleanup failure masks result success and retains the durable fence.
    job.cleanup_activity(runtime.as_ref()).await?;
    effects?;
    if result.is_err() {
        job.recover_writer(runtime.as_ref()).await?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn inline_source_report_must_match_the_entire_enrollment_before_arguments() -> Result<()> {
        let (release, package, proof) = super::super::tests::fixture()?;
        let plan =
            SourceQuarantinePlan::for_release(&release, None, &super::super::tests::budget())?;
        let expected = plan
            .source_match
            .verify(std::io::Cursor::new(package), std::io::Cursor::new(proof))?;
        let mut invocation = descriptor()?;
        invocation.source = plan.source_match;
        let job_id = invocation.job_id()?;
        let event = |result| Event::Verified {
            schema_version: 1,
            job_id,
            result: Box::new(result),
        };
        require_verified_report(event(expected.clone()), &invocation, &expected)?;
        for forged in [
            Event::Ready {
                schema_version: 1,
                job_id,
            },
            Event::SdkCall {
                schema_version: 1,
                job_id,
                call_id: 1,
            },
            Event::Verified {
                schema_version: 1,
                job_id: Digest::of("other invocation"),
                result: Box::new(expected.clone()),
            },
        ] {
            assert!(require_verified_report(forged, &invocation, &expected).is_err());
        }
        let mut changed = expected.clone();
        changed
            .entrypoint
            .as_mut()
            .context("fixture entrypoint")?
            .sha256 = Digest::of("other entrypoint");
        assert!(require_verified_report(event(changed), &invocation, &expected).is_err());
        let features = HashMap::from([(
            "elixir.marketplace.worker.source_eval_verified".into(),
            "1".into(),
        )]);
        require_verified_feature(&features, SourceExecutionMode::Invoke)?;
        assert!(require_verified_feature(&features, SourceExecutionMode::Lifecycle).is_err());
        assert!(require_verified_feature(&HashMap::new(), SourceExecutionMode::Invoke).is_err());
        Ok(())
    }

    #[test]
    fn large_feed_feature_is_additional_to_the_existing_guarded_execution_contract() -> Result<()> {
        // Feature DATA only; no runtime/executor/grant capability is minted.
        let mut features =
            HashMap::from([("elixir.marketplace.worker.source_eval".into(), "3".into())]);
        let ordinary = descriptor()?.limits;
        require_execution_features(&features, SourceExecutionMode::Invoke, &ordinary)?;
        let heavy = SourceExecutionLimits::large_feed();
        assert!(
            require_execution_features(&features, SourceExecutionMode::Invoke, &heavy).is_err()
        );
        features.insert(
            "elixir.marketplace.worker.source_eval_large_feed".into(),
            "1".into(),
        );
        require_execution_features(&features, SourceExecutionMode::Invoke, &heavy)?;
        assert!(
            require_execution_features(&features, SourceExecutionMode::Lifecycle, &heavy).is_err()
        );
        assert!(require_execution_features(&features, SourceExecutionMode::Probe, &heavy).is_err());
        features.insert(
            "elixir.marketplace.worker.source_eval_large_feed".into(),
            "2".into(),
        );
        assert!(
            require_execution_features(&features, SourceExecutionMode::Invoke, &heavy).is_err()
        );
        features.insert(
            "elixir.marketplace.worker.source_eval_large_feed".into(),
            "1".into(),
        );
        features.remove("elixir.marketplace.worker.source_eval");
        assert!(
            require_execution_features(&features, SourceExecutionMode::Invoke, &heavy).is_err()
        );
        Ok(())
    }

    struct Guard {
        live: Arc<AtomicBool>,
        calls: usize,
        checks: usize,
    }
    #[async_trait::async_trait]
    impl Broker for Guard {
        async fn current(&mut self) -> Result<()> {
            self.checks += 1;
            if !self.live.load(Ordering::SeqCst) {
                bail!("fixture authority withdrawn");
            }
            Ok(())
        }
        async fn request(&mut self, _: &RuntimeJson, _: usize) -> Result<SourceSdkReply> {
            self.calls += 1;
            bail!("unexpected fixture SDK effect")
        }
    }
    fn descriptor() -> Result<SourceExecutionDescriptor> {
        let (release, _, _) = super::super::tests::fixture()?;
        let plan =
            SourceQuarantinePlan::for_release(&release, None, &super::super::tests::budget())?;
        Ok(SourceExecutionDescriptor {
            mode: elixir_marketplace::source_execution::SourceExecutionMode::Invoke,
            schema_version: 1,
            invocation_id: Digest::of(b"controller fixture"),
            source: plan.source_match,
            limits: SourceExecutionLimits {
                profile: Default::default(),
                heap_bytes: 16 * MIB,
                timeout_millis: 3000,
                max_file_bytes: 4 * MIB,
                json_bytes: MIB,
                pending_jobs: 128,
                sdk: None,
            },
        })
    }
    async fn channel(job: Digest, case: &str) -> Result<SourceChannel> {
        use std::process::Stdio;
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/elixir_marketplace/worker/test/source-controller-pipe-fixture.mjs");
        let mut child = tokio::process::Command::new("node")
            .arg(fixture)
            .arg(job.to_string())
            .arg(case)
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
        SourceChannel::new(process)
    }
    #[tokio::test]
    async fn controller_preserves_partial_frame_reads_and_rejects_unbound_or_unsuccessful_results()
    -> Result<()> {
        for case in ["normal", "partial", "wrong_job", "trailing", "failure"] {
            let descriptor = descriptor()?;
            let mut channel = channel(descriptor.job_id()?, case).await?;
            let mut guard = Guard {
                live: Arc::new(AtomicBool::new(true)),
                calls: 0,
                checks: 0,
            };
            let result = timeout(
                Duration::from_secs(5),
                exchange(
                    &mut channel,
                    &descriptor,
                    &RuntimeJson::parse(b"{}")?,
                    &mut guard,
                ),
            )
            .await?;
            if matches!(case, "normal" | "partial") {
                assert_eq!(result?.as_str(), "{\"value\":0.75}");
            } else {
                assert!(result.is_err(), "{case}");
            }
            assert_eq!(guard.calls, 0);
        }
        Ok(())
    }
    #[tokio::test]
    async fn controller_denies_sdk_sequence_disabled_transport_and_raw_byte_overruns_before_effects()
    -> Result<()> {
        for case in [
            "sdk_disabled",
            "sdk_sequence",
            "sdk_budget",
            "sdk_whitespace",
        ] {
            let mut descriptor = descriptor()?;
            if case != "sdk_disabled" {
                descriptor.limits.sdk =
                    Some(elixir_marketplace::source_execution::SourceSdkLimits {
                        calls: 1,
                        request_bytes: if case == "sdk_budget" { 1 } else { 4096 },
                        response_bytes: MIB,
                    });
            }
            let mut channel = channel(descriptor.job_id()?, case).await?;
            let mut guard = Guard {
                live: Arc::new(AtomicBool::new(true)),
                calls: 0,
                checks: 0,
            };
            assert!(
                timeout(
                    Duration::from_secs(5),
                    exchange(
                        &mut channel,
                        &descriptor,
                        &RuntimeJson::parse(b"{}")?,
                        &mut guard
                    )
                )
                .await?
                .is_err(),
                "{case}"
            );
            assert_eq!(guard.calls, 0, "{case}");
        }
        Ok(())
    }
    #[tokio::test]
    async fn controller_private_parser_failures_never_echo_child_data_in_error_chains() -> Result<()>
    {
        for (case, expected) in [
            ("event_sensitive_variant", "invalid private source event"),
            ("event_sensitive_field", "invalid private source event"),
            ("payload_sensitive_json", "invalid private source payload"),
            ("sdk_sensitive_operation", "invalid private SDK request"),
            ("sdk_sensitive_field", "invalid private SDK request"),
        ] {
            let mut descriptor = descriptor()?;
            descriptor.limits.sdk = Some(elixir_marketplace::source_execution::SourceSdkLimits {
                calls: 1,
                request_bytes: MIB,
                response_bytes: MIB,
            });
            let mut channel = channel(descriptor.job_id()?, case).await?;
            let mut guard = Guard {
                live: Arc::new(AtomicBool::new(true)),
                calls: 0,
                checks: 0,
            };
            let result = timeout(
                Duration::from_secs(5),
                exchange(
                    &mut channel,
                    &descriptor,
                    &RuntimeJson::parse(b"{}")?,
                    &mut guard,
                ),
            )
            .await?;
            let error = match result {
                Err(error) => error,
                Ok(_) => bail!("private parser fixture unexpectedly succeeded"),
            };
            // Exact category also proves the intended decoder was exercised,
            // rather than merely observing an unrelated child/transport exit.
            assert_eq!(error.to_string(), expected, "{case}");
            for display in [
                format!("{error:#}"),
                format!("{error:?}"),
                format!("{error:#?}"),
            ] {
                assert!(!display.contains("PRIVATE_SECRET_DO_NOT_ECHO"), "{case}");
            }
            assert_eq!(guard.calls, 0, "{case}");
        }
        Ok(())
    }
    #[tokio::test]
    async fn controller_rechecks_authority_during_stalled_private_reads() -> Result<()> {
        let descriptor = descriptor()?;
        let mut channel = channel(descriptor.job_id()?, "stall").await?;
        let live = Arc::new(AtomicBool::new(true));
        let mut guard = Guard {
            live: live.clone(),
            calls: 0,
            checks: 0,
        };
        let revoke = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            live.store(false, Ordering::SeqCst);
        };
        let arguments = RuntimeJson::parse(b"{}")?;
        let (result, _) = tokio::join!(
            timeout(
                Duration::from_secs(2),
                exchange(&mut channel, &descriptor, &arguments, &mut guard)
            ),
            revoke
        );
        assert!(result?.is_err());
        assert!(guard.checks >= 2);
        assert_eq!(guard.calls, 0);
        Ok(())
    }
}
