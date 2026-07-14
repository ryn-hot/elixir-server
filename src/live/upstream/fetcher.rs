use std::{
    collections::HashSet,
    fmt,
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use reqwest::{
    Method, Response, StatusCode,
    header::{
        ACCEPT_RANGES, AGE, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE,
        CONTENT_TYPE, DATE, ETAG, EXPIRES, HeaderMap, LAST_MODIFIED, LOCATION, RETRY_AFTER,
    },
};
use tokio_util::sync::CancellationToken;

use crate::live::contract::SensitiveString;

use super::{
    connector::{EgressConnector, PreparedRequest},
    credentials::{CredentialSet, SafeRequestHeaders},
    error::{Result, UpstreamErrorCode},
    policy::{DestinationPolicy, ResolvedTarget, ResponseOrigin, ValidatedUrl},
    resolver::DnsResolver,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamMethod {
    Get,
    Head,
}

impl UpstreamMethod {
    fn reqwest(self) -> Method {
        match self {
            Self::Get => Method::GET,
            Self::Head => Method::HEAD,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UpstreamLimits {
    pub connect_timeout: Duration,
    pub header_timeout: Duration,
    pub idle_timeout: Duration,
    pub total_timeout: Duration,
    pub max_response_bytes: u64,
    pub max_response_headers: usize,
    pub max_response_header_bytes: usize,
    pub max_redirects: usize,
}

impl Default for UpstreamLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            header_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(15),
            total_timeout: Duration::from_secs(120),
            max_response_bytes: 64 * 1024 * 1024,
            max_response_headers: 64,
            max_response_header_bytes: 32 * 1024,
            max_redirects: 5,
        }
    }
}

impl UpstreamLimits {
    fn validate(&self) -> Result<()> {
        let timeouts = [
            self.connect_timeout,
            self.header_timeout,
            self.idle_timeout,
            self.total_timeout,
        ];
        if timeouts.iter().any(Duration::is_zero)
            || self.connect_timeout > Duration::from_secs(60)
            || self.header_timeout > Duration::from_secs(60)
            || self.idle_timeout > Duration::from_secs(300)
            || self.total_timeout > Duration::from_secs(24 * 60 * 60)
            || self.max_response_bytes == 0
            || self.max_response_bytes > 4 * 1024 * 1024 * 1024
            || !(1..=256).contains(&self.max_response_headers)
            || !(1_024..=256 * 1024).contains(&self.max_response_header_bytes)
            || self.max_redirects > 10
        {
            return Err(UpstreamErrorCode::EgressRejected.into());
        }
        Ok(())
    }
}

pub struct FetchRequest {
    url: SensitiveString,
    method: UpstreamMethod,
    policy: DestinationPolicy,
    safe_headers: SafeRequestHeaders,
    credentials: Option<Arc<CredentialSet>>,
    cancellation: CancellationToken,
}

impl fmt::Debug for FetchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FetchRequest")
            .field("url", &"<sensitive>")
            .field("method", &self.method)
            .field("policy", &self.policy)
            .field("safe_headers", &self.safe_headers)
            .field("has_credentials", &self.credentials.is_some())
            .finish()
    }
}

impl FetchRequest {
    pub fn new(
        url: SensitiveString,
        method: UpstreamMethod,
        policy: DestinationPolicy,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            url,
            method,
            policy,
            safe_headers: SafeRequestHeaders::new(),
            credentials: None,
            cancellation,
        }
    }

    pub fn with_safe_headers(mut self, headers: SafeRequestHeaders) -> Self {
        self.safe_headers = headers;
        self
    }

    pub fn with_credentials(mut self, credentials: Arc<CredentialSet>) -> Self {
        self.credentials = Some(credentials);
        self
    }
}

pub struct UpstreamFetcher {
    resolver: Arc<dyn DnsResolver>,
    connector: Arc<dyn EgressConnector>,
    limits: UpstreamLimits,
}

impl fmt::Debug for UpstreamFetcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamFetcher")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl UpstreamFetcher {
    pub fn new(resolver: Arc<dyn DnsResolver>, limits: UpstreamLimits) -> Result<Self> {
        Self::with_direct_connector(
            resolver,
            super::connector::DirectEgressConnector::new(),
            limits,
        )
    }

    pub fn with_direct_connector(
        resolver: Arc<dyn DnsResolver>,
        connector: super::connector::DirectEgressConnector,
        limits: UpstreamLimits,
    ) -> Result<Self> {
        Self::with_connector(resolver, Arc::new(connector), limits)
    }

    pub(crate) fn with_connector(
        resolver: Arc<dyn DnsResolver>,
        connector: Arc<dyn EgressConnector>,
        limits: UpstreamLimits,
    ) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            resolver,
            connector,
            limits,
        })
    }

    pub async fn fetch(&self, request: FetchRequest) -> Result<UpstreamResponse> {
        let started = Instant::now();
        let total_deadline = started + self.limits.total_timeout;
        let mut target = request.policy.validate_initial(request.url.expose())?;
        let mut visited = HashSet::with_capacity(self.limits.max_redirects + 1);
        let mut redirects = 0usize;
        loop {
            if !visited.insert(target.canonical_visit_key()) {
                return Err(UpstreamErrorCode::RedirectLoop.into());
            }
            let resolved = self.resolve(&request, target, total_deadline).await?;
            let mut headers = request.safe_headers.to_header_map()?;
            if let Some(credentials) = &request.credentials {
                credentials.apply(&resolved.target, &mut headers)?;
            }
            let response = self
                .execute(
                    &request,
                    &resolved,
                    PreparedRequest {
                        method: request.method.reqwest(),
                        headers,
                    },
                    total_deadline,
                )
                .await?;
            validate_raw_headers(response.headers(), &self.limits)?;
            if let Some(credentials) = &request.credentials {
                credentials.ingest_response(&resolved.target, response.headers())?;
            }
            if is_redirect(response.status()) {
                if redirects >= self.limits.max_redirects {
                    return Err(UpstreamErrorCode::RedirectLimit.into());
                }
                let locations = response.headers().get_all(LOCATION);
                if locations.iter().count() != 1 {
                    return Err(UpstreamErrorCode::RedirectInvalid.into());
                }
                let location = locations
                    .iter()
                    .next()
                    .and_then(|value| value.to_str().ok())
                    .ok_or(UpstreamErrorCode::RedirectInvalid)?;
                target = request
                    .policy
                    .validate_redirect(&resolved.target, location)?;
                redirects += 1;
                continue;
            }
            if request.method == UpstreamMethod::Get
                && let Some(length) = response.content_length()
                && length > self.limits.max_response_bytes
            {
                return Err(UpstreamErrorCode::BodyTooLarge.into());
            }
            if response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| {
                    value
                        .to_str()
                        .map(|value| !value.trim().eq_ignore_ascii_case("identity"))
                        .unwrap_or(true)
                })
            {
                return Err(UpstreamErrorCode::UpstreamProtocol.into());
            }
            let sanitized_headers =
                sanitize_response_headers(response.headers(), request.credentials.as_deref())?;
            let stats = Arc::new(FetchStatsInner {
                started,
                bytes_received: AtomicU64::new(0),
                redirects: AtomicUsize::new(redirects),
            });
            let status = response.status();
            let final_url = resolved.target.url.clone();
            return Ok(UpstreamResponse {
                response,
                status,
                headers: sanitized_headers,
                origin: resolved.target.origin(),
                final_url,
                cancellation: request.cancellation,
                total_deadline,
                idle_timeout: self.limits.idle_timeout,
                max_response_bytes: self.limits.max_response_bytes,
                stats,
            });
        }
    }

    async fn resolve(
        &self,
        request: &FetchRequest,
        target: ValidatedUrl,
        total_deadline: Instant,
    ) -> Result<ResolvedTarget> {
        let addresses = match target.host.parse::<IpAddr>() {
            Ok(address) => vec![address],
            Err(_) => {
                let remaining = remaining(total_deadline)?;
                tokio::select! {
                    _ = request.cancellation.cancelled() => {
                        return Err(UpstreamErrorCode::Cancelled.into());
                    }
                    result = tokio::time::timeout(
                        remaining,
                        self.resolver.resolve(
                            &target.host,
                            target.port,
                            &request.cancellation,
                        ),
                    ) => match result {
                        Ok(value) => value?,
                        Err(_) => return Err(UpstreamErrorCode::TotalTimeout.into()),
                    }
                }
            }
        };
        request.policy.resolve_target(target, addresses)
    }

    async fn execute(
        &self,
        request: &FetchRequest,
        target: &ResolvedTarget,
        prepared: PreparedRequest,
        total_deadline: Instant,
    ) -> Result<Response> {
        let total_remaining = remaining(total_deadline)?;
        let wait = self.limits.header_timeout.min(total_remaining);
        let header_is_total = total_remaining <= self.limits.header_timeout;
        tokio::select! {
            _ = request.cancellation.cancelled() => Err(UpstreamErrorCode::Cancelled.into()),
            result = tokio::time::timeout(
                wait,
                self.connector.execute(
                    target,
                    prepared,
                    self.limits.connect_timeout,
                    &request.cancellation,
                ),
            ) => match result {
                Ok(value) => value,
                Err(_) if header_is_total => Err(UpstreamErrorCode::TotalTimeout.into()),
                Err(_) => Err(UpstreamErrorCode::HeaderTimeout.into()),
            }
        }
    }
}

pub struct UpstreamResponse {
    response: Response,
    status: StatusCode,
    headers: HeaderMap,
    origin: ResponseOrigin,
    final_url: reqwest::Url,
    cancellation: CancellationToken,
    total_deadline: Instant,
    idle_timeout: Duration,
    max_response_bytes: u64,
    stats: Arc<FetchStatsInner>,
}

impl fmt::Debug for UpstreamResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field("origin", &self.origin)
            .field("stats", &self.stats())
            .finish()
    }
}

impl Drop for UpstreamResponse {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl UpstreamResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn origin(&self) -> &ResponseOrigin {
        &self.origin
    }

    pub fn final_url(&self) -> &reqwest::Url {
        &self.final_url
    }

    pub fn stats(&self) -> FetchStats {
        FetchStats {
            inner: self.stats.clone(),
        }
    }

    pub async fn next_chunk(&mut self) -> Result<Option<UpstreamChunk>> {
        let total_remaining = remaining(self.total_deadline)?;
        let wait = self.idle_timeout.min(total_remaining);
        let total_wins = total_remaining <= self.idle_timeout;
        let chunk = tokio::select! {
            _ = self.cancellation.cancelled() => {
                return Err(UpstreamErrorCode::Cancelled.into());
            }
            result = tokio::time::timeout(wait, self.response.chunk()) => match result {
                Ok(Ok(value)) => value,
                Ok(Err(_)) => return Err(UpstreamErrorCode::UpstreamProtocol.into()),
                Err(_) if total_wins => return Err(UpstreamErrorCode::TotalTimeout.into()),
                Err(_) => return Err(UpstreamErrorCode::IdleTimeout.into()),
            }
        };
        let Some(chunk) = chunk else {
            return Ok(None);
        };
        let length = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let previous = self
            .stats
            .bytes_received
            .fetch_add(length, Ordering::AcqRel);
        if previous.saturating_add(length) > self.max_response_bytes {
            return Err(UpstreamErrorCode::BodyTooLarge.into());
        }
        Ok(Some(UpstreamChunk(chunk.to_vec())))
    }

    pub async fn collect(self) -> Result<UpstreamBody> {
        let maximum_bytes = self.max_response_bytes;
        self.collect_bounded(maximum_bytes).await
    }

    pub async fn collect_bounded(mut self, maximum_bytes: u64) -> Result<UpstreamBody> {
        if maximum_bytes == 0 || maximum_bytes > self.max_response_bytes {
            return Err(UpstreamErrorCode::BodyTooLarge.into());
        }
        if self
            .response
            .content_length()
            .is_some_and(|length| length > maximum_bytes)
        {
            return Err(UpstreamErrorCode::BodyTooLarge.into());
        }
        let capacity = self
            .response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(1024 * 1024);
        let mut output = Vec::with_capacity(capacity);
        while let Some(chunk) = self.next_chunk().await? {
            if u64::try_from(output.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(chunk.as_bytes().len()).unwrap_or(u64::MAX))
                > maximum_bytes
            {
                return Err(UpstreamErrorCode::BodyTooLarge.into());
            }
            output.extend_from_slice(chunk.as_bytes());
        }
        Ok(UpstreamBody(output))
    }
}

pub struct UpstreamChunk(Vec<u8>);

impl UpstreamChunk {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for UpstreamChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamChunk")
            .field("bytes", &self.0.len())
            .finish()
    }
}

pub struct UpstreamBody(Vec<u8>);

impl UpstreamBody {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for UpstreamBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamBody")
            .field("bytes", &self.0.len())
            .finish()
    }
}

struct FetchStatsInner {
    started: Instant,
    bytes_received: AtomicU64,
    redirects: AtomicUsize,
}

#[derive(Clone)]
pub struct FetchStats {
    inner: Arc<FetchStatsInner>,
}

impl fmt::Debug for FetchStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FetchStats")
            .field("bytes_received", &self.bytes_received())
            .field("redirects", &self.redirects())
            .finish()
    }
}

impl FetchStats {
    pub fn bytes_received(&self) -> u64 {
        self.inner.bytes_received.load(Ordering::Acquire)
    }

    pub fn redirects(&self) -> usize {
        self.inner.redirects.load(Ordering::Acquire)
    }

    pub fn elapsed(&self) -> Duration {
        self.inner.started.elapsed()
    }

    pub fn average_bytes_per_second(&self) -> u64 {
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed <= f64::EPSILON {
            return 0;
        }
        (self.bytes_received() as f64 / elapsed) as u64
    }
}

fn validate_raw_headers(headers: &HeaderMap, limits: &UpstreamLimits) -> Result<()> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for (name, value) in headers {
        count = count.saturating_add(1);
        bytes = bytes
            .saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len());
        if count > limits.max_response_headers || bytes > limits.max_response_header_bytes {
            return Err(UpstreamErrorCode::ResponseHeadersTooLarge.into());
        }
    }
    Ok(())
}

fn sanitize_response_headers(
    headers: &HeaderMap,
    credentials: Option<&CredentialSet>,
) -> Result<HeaderMap> {
    let allowed = [
        ACCEPT_RANGES,
        AGE,
        CACHE_CONTROL,
        CONTENT_LENGTH,
        CONTENT_RANGE,
        CONTENT_TYPE,
        DATE,
        ETAG,
        EXPIRES,
        LAST_MODIFIED,
        RETRY_AFTER,
    ];
    let mut output = HeaderMap::new();
    for name in allowed {
        for value in headers.get_all(&name) {
            let text = value
                .to_str()
                .map_err(|_| UpstreamErrorCode::UpstreamProtocol)?;
            if text.contains("ELIXIR_LIVE_CANARY_")
                || credentials.is_some_and(|set| set.value_is_sensitive(text))
            {
                return Err(UpstreamErrorCode::SensitiveResponse.into());
            }
            output.append(name.clone(), value.clone());
        }
    }
    Ok(output)
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn remaining(deadline: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or_else(|| UpstreamErrorCode::TotalTimeout.into())
}
