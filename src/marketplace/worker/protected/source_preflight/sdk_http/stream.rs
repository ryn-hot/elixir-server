//! One retained native duplex helper, not a public SDK route. Demand-driven
//! replies share the original authority/deadline and never restart the HTTP
//! request. Cancellation still drains and joins the same native parent.
use super::*;
use elixir_marketplace::sdk_http::{
    NATIVE_EXIT_MARKER,
    stream::{MAX_STREAM_OUTPUT, StreamProtocol},
};
use tokio::process::ChildStdout;

enum Command {
    Read {
        maximum: usize,
        reply: oneshot::Sender<Result<SourceSdkReply>>,
    },
    Close {
        reply: oneshot::Sender<Result<SourceSdkReply>>,
    },
}
struct StreamCompletion {
    retired: bool,
    result: Result<()>,
}
pub(in crate::marketplace) struct SdkHttpStreamCall {
    pub(in crate::marketplace) id: u32,
    lifeline: Option<oneshot::Receiver<()>>,
    headers: Option<oneshot::Receiver<Result<SourceSdkReply>>>,
    commands: mpsc::Sender<Command>,
    task: Option<JoinHandle<StreamCompletion>>,
    completed: Option<StreamCompletion>,
    join_failed: bool,
}
impl SdkHttpClient {
    /// Internal protected owner only. A later application-data owner must
    /// supply its own admitted networking/resources before public routing.
    pub(in crate::marketplace) fn start_stream(
        &self,
        raw: &RuntimeJson,
        maximum: usize,
        authority: Arc<dyn SdkHttpAuthority>,
    ) -> Result<SdkHttpStreamCall> {
        self.0.admit_http(false)?;
        if !self.0.binding.sdk_http_stream {
            bail!("protected worker does not support SDK HTTP streaming");
        }
        let mut protocol = StreamProtocol::new(
            raw,
            maximum,
            Digest::of(uuid::Uuid::new_v4().as_bytes()),
            self.0.binding.family,
        )?;
        let opening = Zeroizing::new(protocol.open()?);
        let id = protocol.id();
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("protected HTTP helper slot is busy")?;
        let (mut cancel, lifeline) = oneshot::channel();
        let (send_headers, headers) = oneshot::channel();
        let (commands, receive) = mpsc::channel(1);
        let owner = self.0.clone();
        let task = tokio::runtime::Handle::try_current()?.spawn(async move {
            let _permit = permit;
            let mut retired = true; // No helper has been launched.
            let result = async {
                if cancel.is_closed() {
                    bail!("SDK HTTP stream cancelled before launch");
                }
                authorized(authority.as_ref()).await?;
                owner.headroom().await?;
                if !timeout(
                    READY_DEADLINE,
                    owner.http_ready(&mut cancel, authority.as_ref()),
                )
                .await
                .context("SDK HTTP stream readiness deadline")??
                {
                    bail!("SDK HTTP stream cancelled before launch");
                }
                let mut fault = FaultOnDrop {
                    owner: owner.clone(),
                    armed: true,
                };
                retired = false;
                let process = timeout(
                    Duration::from_secs(10),
                    owner.runtime.exec_marketplace_sdk_http_stream(
                        &owner.handle,
                        &owner.profile.container_spec().labels,
                    ),
                )
                .await
                .context("SDK HTTP stream launch deadline")??;
                let result = timeout(
                    HTTP_RETIRE_DEADLINE,
                    StreamExchange {
                        protocol: &mut protocol,
                        opening: &opening,
                        owner: &owner,
                        cancel: &mut cancel,
                        authority: authority.as_ref(),
                        headers: send_headers,
                        commands: receive,
                    }
                    .run(process, &mut retired),
                )
                .await;
                if retired {
                    fault.armed = false;
                }
                let reply = result.context("SDK HTTP stream retirement deadline")??;
                let mut integrity = FaultOnDrop {
                    owner: owner.clone(),
                    armed: true,
                };
                timeout(
                    Duration::from_secs(5),
                    inspect_fixed_worker(owner.runtime.as_ref(), &owner.handle, &owner.profile),
                )
                .await
                .context("SDK HTTP stream ownership readback deadline")??;
                integrity.armed = false;
                owner.admit_http(false)?;
                authorized(authority.as_ref()).await?;
                acknowledge_close(reply)
            }
            .await;
            StreamCompletion { retired, result }
        });
        Ok(SdkHttpStreamCall {
            id,
            lifeline: Some(lifeline),
            headers: Some(headers),
            commands,
            task: Some(task),
            completed: None,
            join_failed: false,
        })
    }
}
impl SdkHttpStreamCall {
    async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("original SDK HTTP stream join failed");
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            match result {
                Ok(result) => self.completed = Some(result),
                Err(_) => {
                    self.join_failed = true;
                    bail!("original SDK HTTP stream join failed");
                }
            }
        }
        if !self.completed.as_ref().is_some_and(|c| c.retired) {
            bail!("original SDK HTTP stream native retirement is unconfirmed");
        }
        Ok(())
    }
    async fn ended(&mut self) -> Result<SourceSdkReply> {
        self.join().await?;
        if self.completed.as_ref().is_some_and(|c| c.result.is_ok()) {
            bail!("original SDK HTTP stream reply already consumed");
        }
        Ok(SourceSdkReply::Unavailable)
    }
    pub(in crate::marketplace) async fn headers(&mut self) -> Result<SourceSdkReply> {
        let answer = self
            .headers
            .as_mut()
            .context("SDK HTTP stream headers already consumed")?
            .await;
        self.headers = None;
        match answer {
            Ok(reply) => reply,
            Err(_) => self.ended().await,
        }
    }
    pub(in crate::marketplace) async fn read(&mut self, maximum: usize) -> Result<SourceSdkReply> {
        if maximum == 0 || maximum > elixir_marketplace::source_execution::MAX_JSON_BYTES {
            bail!("invalid SDK HTTP stream reply allowance");
        }
        let (reply, answer) = oneshot::channel();
        match self.commands.try_send(Command::Read { maximum, reply }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                bail!("SDK HTTP stream read already pending")
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return self.ended().await,
        }
        match answer.await {
            Ok(reply) => reply,
            Err(_) => self.ended().await,
        }
    }
    pub(in crate::marketplace) async fn close(&mut self) -> Result<SourceSdkReply> {
        let (reply, answer) = oneshot::channel();
        match self.commands.try_send(Command::Close { reply }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                bail!("SDK HTTP stream command already pending")
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return self.ended().await,
        }
        let result = answer.await;
        // The acknowledgement is released only after the original actual wait;
        // retain that same task even when this waiter is itself cancelled.
        self.join().await?;
        match result {
            Ok(reply) => reply,
            Err(_) => self.ended().await,
        }
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        drop(self.lifeline.take());
        self.join().await
    }
}
impl Drop for SdkHttpStreamCall {
    fn drop(&mut self) {
        drop(self.lifeline.take());
    }
}

// Only the last native-marker-sized suffix is retained during cancellation.
// Already-read partial frames contribute to this tail before any further await.
struct Output {
    stream: ChildStdout,
    tail: Zeroizing<Vec<u8>>,
    total: usize,
}
impl Output {
    fn new(stream: ChildStdout) -> Self {
        Self {
            stream,
            tail: Zeroizing::new(Vec::new()),
            total: 0,
        }
    }
    async fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
        let n = self.stream.read(bytes).await?;
        let maximum = MAX_STREAM_OUTPUT + NATIVE_EXIT_MARKER.len() + 1;
        if n > maximum.saturating_sub(self.total) {
            bail!("SDK HTTP stream aggregate output bounds");
        }
        self.total += n;
        let count = NATIVE_EXIT_MARKER.len() + 1;
        if n >= count {
            self.tail.clear();
            self.tail.extend_from_slice(&bytes[n - count..n]);
        } else {
            let discard = (self.tail.len() + n).saturating_sub(count);
            self.tail.drain(..discard);
            self.tail.extend_from_slice(&bytes[..n]);
        }
        Ok(n)
    }
    async fn exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        let mut used = 0;
        while used < bytes.len() {
            let n = self.read(&mut bytes[used..]).await?;
            if n == 0 {
                bail!("SDK HTTP stream truncated frame");
            }
            used += n;
        }
        Ok(())
    }
    async fn frame(&mut self) -> Result<Zeroizing<Vec<u8>>> {
        let mut header = [0; 4];
        self.exact(&mut header).await?;
        let length = u32::from_be_bytes(header) as usize;
        if length == 0 || length > elixir_marketplace::sdk_http::MAX_HTTP_REPLY {
            bail!("SDK HTTP stream frame bounds");
        }
        let mut bytes = Zeroizing::new(vec![0; length]);
        self.exact(&mut bytes).await?;
        Ok(bytes)
    }
    async fn drain(&mut self) -> Result<()> {
        let mut bytes = Zeroizing::new([0; 16 * 1024]);
        while self.read(bytes.as_mut_slice()).await? != 0 {}
        Ok(())
    }
}

/// Wrap PURE pipe/channel I/O only. Authority checks are serialized here, never
/// suspended by a second timer-driven check of their own SQL/native locks.
async fn watched<T>(
    owner: &Owner,
    cancel: &mut oneshot::Sender<()>,
    authority: &dyn SdkHttpAuthority,
    checks: &mut tokio::time::Interval,
    cancelled: &mut bool,
    expires: Instant,
    io: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::pin!(io);
    let mut protection = owner.protection.clone();
    loop {
        tokio::select! { biased;
            _ = cancel.closed() => { *cancelled=true;bail!("SDK HTTP stream cancelled"); }
            _ = tokio::time::sleep_until(expires) => {
                *cancelled=true;bail!("original SDK HTTP stream deadline");
            }
            result = protection.changed() => {
                if result.is_err() || owner.admit_http(false).is_err() {
                    *cancelled=true;bail!("SDK HTTP stream protection stopped");
                }
            }
            _ = checks.tick() => {
                if owner.admit_http(false).is_err() || authorized(authority).await.is_err() {
                    *cancelled=true;bail!("SDK HTTP stream authority stopped");
                }
                checks.reset_after(AUTHORITY_INTERVAL);
            }
            result = &mut io => return result,
        }
    }
}

struct StreamExchange<'a> {
    protocol: &'a mut StreamProtocol,
    opening: &'a [u8],
    owner: &'a Arc<Owner>,
    cancel: &'a mut oneshot::Sender<()>,
    authority: &'a dyn SdkHttpAuthority,
    headers: oneshot::Sender<Result<SourceSdkReply>>,
    commands: mpsc::Receiver<Command>,
}
impl StreamExchange<'_> {
    async fn run(
        self,
        process: PrivateProcessStdio,
        retired: &mut bool,
    ) -> Result<oneshot::Sender<Result<SourceSdkReply>>> {
        let Self {
            protocol,
            opening,
            owner,
            cancel,
            authority,
            headers,
            mut commands,
        } = self;
        let PrivateProcessStdio {
            child,
            mut input,
            output,
            diagnostics,
        } = process;
        let mut child = Child(child);
        let mut output = Output::new(output);
        let mut checks = tokio::time::interval(AUTHORITY_INTERVAL);
        let mut cancelled = false;
        let mut close_reply = None;
        // Leave time after the helper's fixed 15-second watchdog to drain its
        // marker and join it. An idle reader cannot defer retirement indefinitely.
        let expires = Instant::now()
            + Duration::from_millis(elixir_marketplace::sdk_http::HTTP_DEADLINE_MILLIS)
            + Duration::from_secs(2);
        let flow = async {
            let result = async {
                if !timeout(READY_DEADLINE, owner.http_ready(cancel, authority))
                    .await
                    .context("SDK HTTP stream input readiness deadline")??
                {
                    cancelled = true;
                    bail!("SDK HTTP stream cancelled before input");
                }
                watched(
                    owner,
                    cancel,
                    authority,
                    &mut checks,
                    &mut cancelled,
                    expires,
                    async {
                        input
                            .write_all(&(opening.len() as u32).to_be_bytes())
                            .await?;
                        input.write_all(opening).await?;
                        input.flush().await?;
                        Ok(())
                    },
                )
                .await?;
                let bytes = watched(
                    owner,
                    cancel,
                    authority,
                    &mut checks,
                    &mut cancelled,
                    expires,
                    output.frame(),
                )
                .await?;
                let value = protocol.accept(&bytes, &owner.binding.context(&owner.profile))?;
                // A changed grant after a read must not expose buffered bytes.
                if owner.admit_http(false).is_err() || authorized(authority).await.is_err() {
                    cancelled = true;
                    bail!("SDK HTTP stream reply authority stopped");
                }
                checks.reset_after(AUTHORITY_INTERVAL);
                if headers.send(Ok(value)).is_err() {
                    cancelled = true;
                    bail!("SDK HTTP stream header waiter stopped");
                }
                loop {
                    let next = watched(
                        owner,
                        cancel,
                        authority,
                        &mut checks,
                        &mut cancelled,
                        expires,
                        async { Ok(commands.recv().await) },
                    )
                    .await?;
                    let Some(next) = next else {
                        cancelled = true;
                        bail!("SDK HTTP stream caller stopped");
                    };
                    if !timeout(READY_DEADLINE, owner.http_ready(cancel, authority))
                        .await
                        .context("SDK HTTP stream command readiness deadline")??
                    {
                        cancelled = true;
                        bail!("SDK HTTP stream command cancelled");
                    }
                    let (bytes, reply, closing) = match next {
                        Command::Read { maximum, reply } => (protocol.read(maximum), reply, false),
                        Command::Close { reply } => (protocol.close(), reply, true),
                    };
                    let bytes = match bytes {
                        Ok(bytes) => Zeroizing::new(bytes),
                        Err(error) => {
                            cancelled = true;
                            return Err(error.into());
                        }
                    };
                    watched(
                        owner,
                        cancel,
                        authority,
                        &mut checks,
                        &mut cancelled,
                        expires,
                        async {
                            input.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
                            input.write_all(&bytes).await?;
                            input.flush().await?;
                            Ok(())
                        },
                    )
                    .await?;
                    let bytes = watched(
                        owner,
                        cancel,
                        authority,
                        &mut checks,
                        &mut cancelled,
                        expires,
                        output.frame(),
                    )
                    .await?;
                    let value = protocol.accept(&bytes, &owner.binding.context(&owner.profile))?;
                    if owner.admit_http(false).is_err() || authorized(authority).await.is_err() {
                        cancelled = true;
                        bail!("SDK HTTP stream chunk authority stopped");
                    }
                    checks.reset_after(AUTHORITY_INTERVAL);
                    if closing {
                        close_reply = Some(reply);
                        return Ok(());
                    }
                    if reply.send(Ok(value)).is_err() {
                        cancelled = true;
                        bail!("SDK HTTP stream read waiter stopped");
                    }
                }
            }
            .await;
            // A partial frame is not discarded retirement evidence: Output has
            // already retained its suffix, then drains the SAME stdout through EOF.
            drop(input);
            let before = output.total;
            let drained = output.drain().await;
            (result, drained, before)
        };
        let diagnostics = async {
            let mut bytes = Zeroizing::new(Vec::new());
            diagnostics
                .context("SDK HTTP stream diagnostic pipe missing")?
                .take(1025)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() > 1024 {
                bail!("SDK HTTP stream diagnostic bounds");
            }
            Ok::<_, anyhow::Error>(())
        };
        let ((result, drained, before), diagnostic, status) =
            tokio::join!(flow, diagnostics, child.0.wait());
        drained?;
        let status = status.context("SDK HTTP stream original CLI wait failed")?;
        let exit = NativeHttpExit::parse(&output.tail)?;
        if !exit.output.is_empty() {
            bail!("SDK HTTP stream native trailer bounds");
        }
        *retired = true;
        if cancelled || !exit.succeeded || !status.success() {
            bail!("SDK HTTP stream retired without a usable reply");
        }
        let mut integrity = FaultOnDrop {
            owner: owner.clone(),
            armed: true,
        };
        diagnostic?;
        result?;
        if !protocol.closed() || output.total - before != NATIVE_EXIT_MARKER.len() + 1 {
            bail!("SDK HTTP stream trailing output");
        }
        integrity.armed = false;
        close_reply.context("SDK HTTP stream close waiter missing")
    }
}
fn acknowledge_close(reply: oneshot::Sender<Result<SourceSdkReply>>) -> Result<()> {
    reply
        .send(Ok(SourceSdkReply::Value(RuntimeJson::from_value(
            serde_json::json!(true),
        )?)))
        .map_err(|_| anyhow::anyhow!("SDK HTTP stream close waiter stopped"))
}

#[cfg(test)]
mod tests;
