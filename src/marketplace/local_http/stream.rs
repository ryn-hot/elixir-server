//! Demand-driven text response. One original socket/lookup/authority owner;
//! bounded channels and one chunk, never a full-body cache or a restart token.
use super::*;
use axum::body::Bytes;
use elixir_marketplace::source_execution::STREAM_CHUNK_BYTES;
use tokio::sync::{mpsc, oneshot};

struct Read {
    maximum: usize,
    reply: oneshot::Sender<Result<RuntimeJson>>,
}
#[derive(Debug)]
struct StreamDeadline;
impl std::fmt::Display for StreamDeadline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("original local SDK stream deadline expired")
    }
}
impl std::error::Error for StreamDeadline {}
pub(in crate::marketplace) struct StreamCall {
    pub(in crate::marketplace) id: u32,
    started: Instant,
    cancellation: CancellationToken,
    headers: Option<oneshot::Receiver<Result<RuntimeJson>>>,
    reads: mpsc::Sender<Read>,
    task: Option<JoinHandle<Result<()>>>,
    result: Option<Result<()>>,
    join_failed: bool,
}
impl Client {
    pub(in crate::marketplace) fn start_stream(
        self: &Arc<Self>,
        raw: &RuntimeJson,
        maximum: usize,
        endpoint: LocalEndpoint,
        authority: Arc<dyn SdkHttpAuthority>,
    ) -> Result<StreamCall> {
        let SourceSdkRequest::FetchOpen {
            stream_id,
            request,
            maximum_body_bytes,
        } = SourceSdkRequest::parse(raw)?
        else {
            bail!("local SDK stream open required")
        };
        let request = self.prepare(&request.to_json()?, maximum, endpoint)?;
        self.start_stream_request(request, stream_id, maximum_body_bytes as usize, authority)
    }
    pub(super) fn start_stream_request(
        self: &Arc<Self>,
        request: Request,
        stream_id: u32,
        maximum_body_bytes: usize,
        authority: Arc<dyn SdkHttpAuthority>,
    ) -> Result<StreamCall> {
        let cancellation = CancellationToken::new();
        let started = Instant::now();
        let expires = started + Duration::from_millis(HTTP_DEADLINE_MILLIS);
        self.admitted(&cancellation, expires)?;
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .context("local SDK HTTP slot is busy")?;
        let (send_headers, headers) = oneshot::channel();
        let (reads, receive) = mpsc::channel(1);
        let client = self.clone();
        let stopped = cancellation.clone();
        let task = tokio::runtime::Handle::try_current()?.spawn(async move {
            let _permit = permit;
            let target = client
                .resolve(&request, authority.as_ref(), &stopped, expires)
                .await?;
            let mut exchange = Box::pin(
                Flow {
                    client: &client,
                    request: &request,
                    target: &target,
                    id: stream_id,
                    maximum_body: maximum_body_bytes,
                    headers: send_headers,
                    reads: receive,
                    authority: authority.as_ref(),
                    cancellation: &stopped,
                    expires,
                }
                .run(),
            );
            // Flow owns all authority checks. A timer here must NEVER suspend
            // a check holding an SQL lock and start another check of that lock.
            let result = tokio::select! { biased;
                _ = stopped.cancelled() => Err(anyhow::anyhow!("local SDK stream cancelled")),
                _ = sleep_until(expires) => Err(anyhow::Error::new(StreamDeadline)),
                answer = &mut exchange => answer,
            };
            // This drops the original HTTP response before task completion.
            drop(exchange);
            result
        });
        Ok(StreamCall {
            id: stream_id,
            started,
            cancellation,
            headers: Some(headers),
            reads,
            task: Some(task),
            result: None,
            join_failed: false,
        })
    }
}
impl StreamCall {
    fn closed_error(&self) -> anyhow::Error {
        let kind = self
            .result
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(safe_failure)
            .unwrap_or("closed");
        anyhow::anyhow!(
            "original local SDK stream closed ({kind}, {} ms)",
            self.started.elapsed().as_millis()
        )
    }
    async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("original local SDK stream join failed")
        }
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            match result {
                Ok(result) => self.result = Some(result),
                Err(_) => {
                    self.join_failed = true;
                    bail!("original local SDK stream join failed")
                }
            }
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn headers(&mut self) -> Result<SourceSdkReply> {
        let answer = self
            .headers
            .as_mut()
            .context("local SDK stream headers already consumed")?
            .await;
        self.headers = None;
        match answer {
            Ok(reply) => Ok(SourceSdkReply::Value(reply?)),
            Err(_) => {
                self.join().await?;
                Err(self.closed_error())
            }
        }
    }
    pub(in crate::marketplace) async fn read(&mut self, maximum: usize) -> Result<SourceSdkReply> {
        if maximum == 0 || maximum > MAX_JSON_BYTES {
            bail!("invalid stream reply allowance")
        }
        let (reply, answer) = oneshot::channel();
        match self.reads.try_send(Read { maximum, reply }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.join().await?;
                return Err(self.closed_error());
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                bail!("original local SDK stream read is already pending")
            }
        }
        match answer.await {
            Ok(reply) => Ok(SourceSdkReply::Value(reply?)),
            Err(_) => {
                self.join().await?;
                Err(self.closed_error())
            }
        }
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.cancellation.cancel();
        self.join().await?;
        self.result = None;
        Ok(())
    }
}
impl Drop for StreamCall {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct Flow<'a> {
    client: &'a Client,
    request: &'a Request,
    target: &'a ResolvedTarget,
    id: u32,
    maximum_body: usize,
    headers: oneshot::Sender<Result<RuntimeJson>>,
    reads: mpsc::Receiver<Read>,
    authority: &'a dyn SdkHttpAuthority,
    cancellation: &'a CancellationToken,
    expires: Instant,
}
impl Flow<'_> {
    async fn run(self) -> Result<()> {
        let Self {
            client,
            request,
            target,
            id,
            maximum_body,
            headers,
            mut reads,
            authority,
            cancellation,
            expires,
        } = self;
        let mut checks = interval_at(Instant::now() + AUTHORITY_INTERVAL, AUTHORITY_INTERVAL);
        checks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let Opened {
            mut response,
            status,
            headers: reply_headers,
            head,
        } = watched(
            client,
            authority,
            cancellation,
            expires,
            &mut checks,
            open(request, target, cancellation, maximum_body),
        )
        .await?;
        client.current(authority, cancellation, expires).await?;
        checks.reset_after(AUTHORITY_INTERVAL);
        let value = RuntimeJson::from_value(
            serde_json::json!({"stream_id":id,"status":status,"headers":reply_headers}),
        )?;
        if value.accounted_bytes() > request.maximum {
            bail!("local SDK stream headers exceed allowance")
        }
        headers
            .send(Ok(value))
            .map_err(|_| anyhow::anyhow!("local SDK stream header waiter closed"))?;
        let mut pending = Bytes::new();
        let mut carry = Vec::new();
        let mut received = 0usize;
        let mut ended = false;
        while let Some(read) = watched(
            client,
            authority,
            cancellation,
            expires,
            &mut checks,
            async { Ok(reads.recv().await) },
        )
        .await?
        {
            client.current(authority, cancellation, expires).await?;
            checks.reset_after(AUTHORITY_INTERVAL);
            // Charge actual JSON escaping, not six bytes for EVERY input byte.
            // Plain feeds can fill the bounded chunk without extra broker/
            // native-inspection round trips. Controls/quotes still pay their
            // exact encoded size, and the complete reply is checked below.
            let budget = read.maximum.saturating_sub(256);
            if budget < 24 {
                bail!("local SDK stream reply allowance exhausted")
            }
            let mut data = std::mem::take(&mut carry);
            let mut encoded = data.iter().copied().map(encoded_byte).sum::<usize>();
            while data.len() < STREAM_CHUNK_BYTES && budget.saturating_sub(encoded) >= 6 && !ended {
                if pending.is_empty() {
                    match watched(
                        client,
                        authority,
                        cancellation,
                        expires,
                        &mut checks,
                        async {
                            response
                                .chunk()
                                .await
                                .map_err(|_| anyhow::anyhow!("local SDK stream response failed"))
                        },
                    )
                    .await?
                    {
                        Some(chunk) => {
                            if chunk.len() > maximum_body.saturating_sub(received)
                                || (head && !chunk.is_empty())
                            {
                                bail!("local SDK stream body exceeds allowance")
                            }
                            received += chunk.len();
                            pending = chunk;
                            if pending.is_empty() {
                                continue;
                            }
                        }
                        None => {
                            ended = true;
                            break;
                        }
                    }
                }
                let mut count = 0;
                for byte in pending
                    .iter()
                    .copied()
                    .take(STREAM_CHUNK_BYTES - data.len())
                {
                    let cost = encoded_byte(byte);
                    if cost > budget.saturating_sub(encoded) {
                        break;
                    }
                    encoded += cost;
                    count += 1;
                }
                if count == 0 {
                    break;
                }
                data.extend_from_slice(&pending[..count]);
                pending = pending.slice(count..);
            }
            let valid = match std::str::from_utf8(&data) {
                Ok(_) => data.len(),
                Err(error) if error.error_len().is_none() && !ended => error.valid_up_to(),
                Err(_) => bail!("local SDK stream body must be UTF-8"),
            };
            carry.extend_from_slice(&data[valid..]);
            data.truncate(valid);
            let done = ended && pending.is_empty() && carry.is_empty();
            let chunk = String::from_utf8(data).context("local SDK stream UTF-8 boundary")?;
            let value = RuntimeJson::from_value(
                serde_json::json!({"stream_id":id,"chunk":chunk,"done":done}),
            )?;
            SourceSdkRequest::FetchRead { stream_id: id }.validate_reply(&value)?;
            if value.accounted_bytes() > read.maximum {
                bail!("local SDK stream encoded reply exceeds allowance")
            }
            client.current(authority, cancellation, expires).await?;
            checks.reset_after(AUTHORITY_INTERVAL);
            read.reply
                .send(Ok(value))
                .map_err(|_| anyhow::anyhow!("local SDK stream read waiter closed"))?;
            if done {
                return Ok(());
            }
        }
        bail!("local SDK stream caller closed")
    }
}

// Only wrap pure socket/channel I/O here, NEVER a future that itself checks
// authority or owns an SQL transaction. Keep the same original I/O future
// across every check; one interval spans the whole response, including idle
// readers and many small body chunks, rather than resetting on network progress.
async fn watched<T>(
    client: &Client,
    authority: &dyn SdkHttpAuthority,
    cancellation: &CancellationToken,
    expires: Instant,
    checks: &mut tokio::time::Interval,
    io: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::pin!(io);
    loop {
        tokio::select! { biased;
            _ = checks.tick() => {
                client.current(authority, cancellation, expires).await?;
                checks.reset_after(AUTHORITY_INTERVAL);
            }
            result = &mut io => return result,
        }
    }
}

fn encoded_byte(byte: u8) -> usize {
    match byte {
        b'"' | b'\\' | b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 2,
        0..=31 => 6,
        _ => 1,
    }
}

// Whitelist locally generated diagnostics. Never format arbitrary error chains:
// HTTP/SQL errors can contain endpoint URLs or instance credentials.
fn safe_failure(error: &anyhow::Error) -> &'static str {
    if error.is::<StreamDeadline>() {
        return "deadline";
    }
    if error.is::<tokio::time::error::Elapsed>() {
        return "authority_timeout";
    }
    for cause in error.chain() {
        let kind = match cause.to_string().as_str() {
            "native source invocation is no longer current" => "native_scope_closed",
            "native source executor identity is not running" => "native_not_running",
            "native effect inspection deadline" => "native_inspection_timeout",
            "local SDK HTTP is stopped, disabled, expired or waiting for playback startup/seek" => {
                "admission_stopped"
            }
            "local SDK HTTP cancelled" | "local SDK stream cancelled" => "cancelled",
            "configured local endpoint is no longer authorized" => "endpoint_revoked",
            "configured local endpoint changed during its original request" => "endpoint_changed",
            "local SDK stream body exceeds allowance" => "body_limit",
            "local SDK stream encoded reply exceeds allowance" => "encoded_limit",
            "local SDK stream reply allowance exhausted" => "reply_budget",
            "local SDK stream response failed" => "response_transport",
            "local SDK stream body must be UTF-8" => "body_encoding",
            "local SDK stream read waiter closed" | "local SDK stream caller closed" => {
                "caller_closed"
            }
            _ => continue,
        };
        return kind;
    }
    "scope_or_transport"
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_cost_matches_the_actual_canonical_json_string_encoder() -> Result<()> {
        let mut text = (0..=127).map(char::from).collect::<String>();
        text.push_str("é🌍\u{2028}\u{2029}");
        let actual = RuntimeJson::from_value(serde_json::json!(text))?;
        assert_eq!(
            actual.accounted_bytes(),
            text.bytes().map(encoded_byte).sum::<usize>() + 2
        );
        Ok(())
    }
    #[test]
    fn terminal_diagnostics_do_not_echo_untrusted_error_text() {
        assert_eq!(
            safe_failure(&anyhow::anyhow!(
                "https://private.example/?credential=secret"
            )),
            "scope_or_transport"
        );
        assert_eq!(
            safe_failure(&anyhow::anyhow!("local SDK stream body exceeds allowance")),
            "body_limit"
        );
        assert_eq!(
            safe_failure(&anyhow::Error::new(StreamDeadline)),
            "deadline"
        );
    }
}
