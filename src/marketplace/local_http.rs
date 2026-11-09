//! One installed SDK request to an explicitly configured LAN endpoint. This
//! never handles marketplace/source traffic or falls back to public networking.
//! Native/instance/endpoint authority is independently checked by its original
//! caller. No URL, proxy, credential scope or resolver is selected by a peer.
use super::{grant_store::LocalEndpoint, worker::SdkHttpAuthority};
use crate::{
    live::upstream::{
        DestinationPolicy, DirectEgressConnector, EgressConnector, LocalDestinationDenylist,
        PreparedRequest, ResolvedTarget,
    },
    playback::PlaybackJobManager,
};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    sdk_http::{HTTP_DEADLINE_MILLIS, MAX_HTTP_BODY},
    source_execution::{
        MAX_JSON_BYTES, RuntimeJson, SdkHeader, SdkHttpMethod, SourceSdkReply, SourceSdkRequest,
    },
};
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::{
    net::{IpAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout},
};
use tokio_util::sync::CancellationToken;

const AUTHORITY_INTERVAL: Duration = Duration::from_millis(500);
const AUTHORITY_TIMEOUT: Duration = Duration::from_secs(5);
mod stream;
pub(in crate::marketplace) use stream::StreamCall;

trait Resolver: Send + Sync {
    fn start(&self, host: String, port: u16) -> Result<JoinHandle<Result<Vec<IpAddr>>>>;
    #[cfg(test)]
    fn fixture_loopback(&self) -> bool {
        false
    }
}
struct HostResolver;
impl Resolver for HostResolver {
    fn start(&self, host: String, port: u16) -> Result<JoinHandle<Result<Vec<IpAddr>>>> {
        Ok(
            tokio::runtime::Handle::try_current()?.spawn_blocking(move || {
                // Unlike a dropped Tokio lookup_host future, this ORIGINAL task is
                // retained and joined even after cancellation/deadline/owner loss.
                let addresses: Vec<_> = (host.as_str(), port)
                    .to_socket_addrs()
                    .map_err(|_| anyhow::anyhow!("local SDK DNS lookup failed"))?
                    .take(9)
                    .map(|address| address.ip())
                    .collect();
                if addresses.is_empty() || addresses.len() > 8 {
                    bail!("local SDK DNS address count is outside its bound");
                }
                Ok(addresses)
            }),
        )
    }
}

/// Trusted startup configuration only. Construction performs no I/O and starts
/// no service/worker. The existing global source lane plus this one slot bound
/// work; stalled lookup retirement cannot silently free either for replacement.
pub(in crate::marketplace) struct Client {
    enabled: bool,
    playback: Arc<PlaybackJobManager>,
    resolver: Arc<dyn Resolver>,
    slot: Arc<Semaphore>,
}
impl Client {
    /// Actual IPC/socket fixture only; no production profile or SDK request can
    /// opt into this classifier. Native/source/grant checks remain real.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_loopback(playback: Arc<PlaybackJobManager>) -> Self {
        tests::fixture_client(playback)
    }
    pub(in crate::marketplace) fn new(enabled: bool, playback: Arc<PlaybackJobManager>) -> Self {
        Self {
            enabled,
            playback,
            resolver: Arc::new(HostResolver),
            slot: Arc::new(Semaphore::new(1)),
        }
    }
    fn admitted(&self, cancellation: &CancellationToken, expires: Instant) -> Result<()> {
        if !self.enabled
            || cancellation.is_cancelled()
            || Instant::now() >= expires
            || self.playback.startup_queue_len() > 0
        {
            bail!(
                "local SDK HTTP is stopped, disabled, expired or waiting for playback startup/seek"
            );
        }
        Ok(())
    }
    async fn current(
        &self,
        authority: &dyn SdkHttpAuthority,
        cancellation: &CancellationToken,
        expires: Instant,
    ) -> Result<()> {
        self.admitted(cancellation, expires)?;
        tokio::select! { biased;
            _ = cancellation.cancelled() => bail!("local SDK HTTP cancelled"),
            result = timeout(AUTHORITY_TIMEOUT.min(expires.saturating_duration_since(Instant::now())), authority.current()) => {
                result.context("local SDK HTTP authority check deadline")??;
            }
        }
        self.admitted(cancellation, expires)
    }
    pub(in crate::marketplace) fn start(
        self: &Arc<Self>,
        raw: &RuntimeJson,
        maximum: usize,
        endpoint: LocalEndpoint,
        authority: Arc<dyn SdkHttpAuthority>,
    ) -> Result<Call> {
        self.start_request(self.prepare(raw, maximum, endpoint)?, authority)
    }
    fn prepare(
        &self,
        raw: &RuntimeJson,
        maximum: usize,
        endpoint: LocalEndpoint,
    ) -> Result<Request> {
        let request = SourceSdkRequest::parse(raw)?;
        let SourceSdkRequest::Fetch { url, .. } = &request else {
            bail!("local SDK HTTP operation required")
        };
        if maximum == 0 || maximum > MAX_JSON_BYTES {
            bail!("invalid local SDK HTTP reply allowance")
        }
        // Current local interface data, never a caller-supplied denylist or a
        // private-host exception. Unknown inventory prevents dispatch.
        let mut local: Vec<IpAddr> = local_ip_address::list_afinet_netifas()?
            .into_iter()
            .map(|(_, address)| address)
            .collect();
        local.extend([
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        ]);
        #[cfg(test)]
        if self.resolver.fixture_loopback() {
            local.retain(|address| !address.is_loopback());
            // Keep a nonempty fixture denylist even on loopback-only CI hosts.
            local.push(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
        }
        let policy = endpoint.policy_for(
            url,
            self.enabled,
            LocalDestinationDenylist::new(local, vec![])?,
        )?;
        #[cfg(test)]
        let policy = if self.resolver.fixture_loopback() {
            policy.allow_fixture_loopback()
        } else {
            policy
        };
        Ok(Request {
            request,
            maximum,
            policy,
        })
    }
    fn start_request(
        self: &Arc<Self>,
        request: Request,
        authority: Arc<dyn SdkHttpAuthority>,
    ) -> Result<Call> {
        let cancellation = CancellationToken::new();
        let expires = Instant::now() + Duration::from_millis(HTTP_DEADLINE_MILLIS);
        self.admitted(&cancellation, expires)?;
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .context("local SDK HTTP slot is busy")?;
        let client = self.clone();
        let stopped = cancellation.clone();
        let task = tokio::runtime::Handle::try_current()?.spawn(async move {
            let _permit = permit;
            // run never abandons its original DNS task. HTTP has no retry,
            // redirect, pool retention, detached caller task or cookie jar.
            client
                .run(request, authority.as_ref(), &stopped, expires)
                .await
        });
        Ok(Call {
            cancellation,
            task: Some(task),
            result: None,
            join_failed: false,
        })
    }
    async fn resolve(
        &self,
        request: &Request,
        authority: &dyn SdkHttpAuthority,
        cancellation: &CancellationToken,
        expires: Instant,
    ) -> Result<ResolvedTarget> {
        self.current(authority, cancellation, expires).await?;
        let SourceSdkRequest::Fetch { url, .. } = &request.request else {
            bail!("local SDK HTTP operation")
        };
        let target = request.policy.validate_initial(url)?;
        let mut ticks = interval_at(Instant::now() + AUTHORITY_INTERVAL, AUTHORITY_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let addresses = if let Ok(address) = target.host.parse::<IpAddr>() {
            vec![address]
        } else {
            // Cancellation stops usefulness immediately, NOT original-task
            // ownership. A slow OS resolver must actually finish before reuse.
            let mut lookup = self.resolver.start(target.host.clone(), target.port)?;
            let mut stopped = None;
            loop {
                tokio::select! { biased;
                    _ = cancellation.cancelled(), if stopped.is_none() => {
                        stopped = Some(anyhow::anyhow!("local SDK DNS cancelled; joining original lookup"));
                    }
                    _ = sleep_until(expires), if stopped.is_none() => {
                        stopped = Some(anyhow::anyhow!("local SDK DNS deadline; joining original lookup"));
                    }
                    answer = &mut lookup => {
                        let answer = answer.context("original local SDK DNS owner failed")?;
                        if let Some(reason) = stopped { return Err(reason); }
                        break answer?;
                    }
                    _ = ticks.tick(), if stopped.is_none() => {
                        if let Err(reason) = self.current(authority, cancellation, expires).await {
                            stopped = Some(reason);
                        }
                    }
                }
            }
        };
        if addresses.is_empty() || addresses.len() > 8 {
            bail!("local SDK DNS address count")
        }
        let target = request.policy.resolve_target(target, addresses)?;
        // Exact original authority is checked AGAIN after DNS and before any
        // HTTP bytes, including when cancellation wins as DNS finishes.
        self.current(authority, cancellation, expires).await?;
        Ok(target)
    }
    async fn run(
        &self,
        request: Request,
        authority: &dyn SdkHttpAuthority,
        cancellation: &CancellationToken,
        expires: Instant,
    ) -> Result<SourceSdkReply> {
        let target = self
            .resolve(&request, authority, cancellation, expires)
            .await?;
        let mut ticks = interval_at(Instant::now() + AUTHORITY_INTERVAL, AUTHORITY_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut exchange = Box::pin(exchange(&request, &target, cancellation));
        let result = loop {
            tokio::select! { biased;
                _ = cancellation.cancelled() => break Err(anyhow::anyhow!("local SDK HTTP cancelled")),
                _ = sleep_until(expires) => break Err(anyhow::anyhow!("local SDK HTTP deadline")),
                _ = ticks.tick() => {
                    if let Err(error) = self.current(authority, cancellation, expires).await { break Err(error); }
                }
                result = &mut exchange => break result,
            }
        };
        // Drop the actual request/response owner before acknowledging completion.
        // Cancelling an HTTP POST cannot retract effects at a remote service;
        // this path never retries or claims such a rollback.
        drop(exchange);
        self.current(authority, cancellation, expires).await?;
        result
    }
}
struct Request {
    request: SourceSdkRequest,
    maximum: usize,
    policy: DestinationPolicy,
}

/// Stored OUTSIDE the fallible VM operation. Cancel signals and joins this SAME
/// task. A cancelled waiter, timeout or panic cannot become an idle receipt.
pub(in crate::marketplace) struct Call {
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<SourceSdkReply>>>,
    result: Option<Result<SourceSdkReply>>,
    join_failed: bool,
}
impl Call {
    async fn join(&mut self) -> Result<()> {
        if self.join_failed {
            bail!("original local SDK HTTP owner join failed")
        }
        if let Some(task) = self.task.as_mut() {
            let joined = task.await;
            self.task = None;
            match joined {
                Ok(result) => self.result = Some(result),
                _ => {
                    self.join_failed = true;
                    bail!("original local SDK HTTP owner join failed")
                }
            }
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn finish(&mut self) -> Result<SourceSdkReply> {
        self.join().await?;
        self.result
            .take()
            .context("local SDK HTTP result already consumed")?
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.cancellation.cancel();
        self.join().await?;
        self.result = None;
        Ok(())
    }
}
impl Drop for Call {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct Opened {
    response: reqwest::Response,
    status: u16,
    headers: Vec<SdkHeader>,
    head: bool,
}
async fn open(
    request: &Request,
    target: &ResolvedTarget,
    cancellation: &CancellationToken,
    maximum: usize,
) -> Result<Opened> {
    let SourceSdkRequest::Fetch {
        method,
        headers,
        body,
        ..
    } = &request.request
    else {
        bail!("local SDK HTTP operation")
    };
    let mut outgoing = HeaderMap::new();
    for header in headers {
        outgoing.append(
            HeaderName::from_bytes(header.name.as_bytes())?,
            HeaderValue::from_str(&header.value)?,
        );
    }
    outgoing.insert("accept-encoding", HeaderValue::from_static("identity"));
    outgoing.insert("connection", HeaderValue::from_static("close"));
    let method = match method {
        SdkHttpMethod::Get => Method::GET,
        SdkHttpMethod::Head => Method::HEAD,
        SdkHttpMethod::Post => Method::POST,
    };
    let response = DirectEgressConnector::for_sdk()
        .execute(
            target,
            PreparedRequest {
                method,
                headers: outgoing,
                body: body.clone(),
            },
            Duration::from_secs(5),
            cancellation,
        )
        .await?;
    let status = response.status().as_u16();
    if !(200..=599).contains(&status) {
        bail!("local SDK HTTP response status")
    }
    let encoding: Vec<_> = response
        .headers()
        .get_all("content-encoding")
        .iter()
        .collect();
    if encoding.len() > 1
        || encoding
            .first()
            .is_some_and(|value| value.as_bytes() != b"identity")
    {
        bail!("local SDK HTTP compressed response is not supported");
    }
    let head = matches!(
        request.request,
        SourceSdkRequest::Fetch {
            method: SdkHttpMethod::Head,
            ..
        }
    );
    if !head
        && response
            .content_length()
            .is_some_and(|length| length > maximum as u64)
    {
        bail!("local SDK HTTP body exceeds allowance");
    }
    let mut headers = Vec::new();
    let mut used = 0usize;
    for (name, value) in response.headers() {
        if matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "transfer-encoding"
                | "content-encoding"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "upgrade"
                | "trailer"
        ) {
            continue;
        }
        let value = value
            .to_str()
            .context("invalid local SDK HTTP response header")?;
        used += name.as_str().len() + value.len();
        if name.as_str().len() > 64 || value.len() > 4096 || used > 8192 || headers.len() >= 32 {
            bail!("local SDK HTTP response headers exceed allowance");
        }
        headers.push(SdkHeader {
            name: name.to_string(),
            value: value.to_owned(),
        });
    }
    Ok(Opened {
        response,
        status,
        headers,
        head,
    })
}
async fn exchange(
    request: &Request,
    target: &ResolvedTarget,
    cancellation: &CancellationToken,
) -> Result<SourceSdkReply> {
    let maximum = MAX_HTTP_BODY.min(request.maximum);
    let Opened {
        mut response,
        status,
        headers,
        head,
    } = open(request, target, cancellation, maximum).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| anyhow::anyhow!("local SDK HTTP response stream failed"))?
    {
        if (head && !chunk.is_empty()) || chunk.len() > maximum.saturating_sub(bytes.len()) {
            bail!("local SDK HTTP body exceeds allowance");
        }
        bytes.extend_from_slice(&chunk);
    }
    let body = String::from_utf8(bytes).context("local SDK HTTP body must be UTF-8")?;
    let value = RuntimeJson::from_value(
        serde_json::json!({"status":status,"headers":headers,"body":body}),
    )?;
    request.request.validate_reply(&value)?;
    if value.accounted_bytes() > request.maximum {
        bail!("local SDK HTTP encoded reply exceeds allowance")
    }
    Ok(SourceSdkReply::Value(value))
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(in crate::marketplace) use tests::native_origin as fixture_origin;
