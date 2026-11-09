use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::{Certificate, Client, Method, Response, redirect::Policy};
use tokio_util::sync::CancellationToken;

use super::{
    error::{Result, UpstreamError, UpstreamErrorCode},
    policy::ResolvedTarget,
};

pub(crate) struct PreparedRequest {
    pub(crate) method: Method,
    pub(crate) headers: reqwest::header::HeaderMap,
    pub(crate) body: Option<String>,
}

#[async_trait]
pub(crate) trait EgressConnector: Send + Sync {
    async fn execute(
        &self,
        target: &ResolvedTarget,
        request: PreparedRequest,
        connect_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<Response>;
}

#[derive(Clone, Default)]
pub struct DirectEgressConnector {
    additional_roots: Arc<Vec<Certificate>>,
    sdk: bool,
}

impl fmt::Debug for DirectEgressConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectEgressConnector")
            .field("additional_root_count", &self.additional_roots.len())
            .field("sdk", &self.sdk)
            .finish()
    }
}

impl DirectEgressConnector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fixed local SDK transport only. No custom roots, transparent body
    /// decompression, redirects, retries or HTTP/2 connection/header state.
    pub(crate) fn for_sdk() -> Self {
        Self {
            sdk: true,
            ..Self::default()
        }
    }

    pub fn with_additional_roots(roots: Vec<Certificate>) -> Self {
        Self {
            additional_roots: Arc::new(roots),
            sdk: false,
        }
    }

    fn client(&self, target: &ResolvedTarget, connect_timeout: Duration) -> Result<Client> {
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(connect_timeout)
            .pool_max_idle_per_host(0)
            .tcp_nodelay(true)
            .referer(false);
        if self.sdk {
            builder = builder
                .http1_only()
                .retry(reqwest::retry::never())
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd();
        }
        for root in self.additional_roots.iter() {
            builder = builder.add_root_certificate(root.clone());
        }
        if target.target.host.parse::<std::net::IpAddr>().is_err() {
            builder =
                builder.resolve_to_addrs(&target.target.host, target.socket_addresses.as_slice());
        }
        builder
            .build()
            .map_err(|_| UpstreamErrorCode::EgressRejected.into())
    }
}

#[async_trait]
impl EgressConnector for DirectEgressConnector {
    async fn execute(
        &self,
        target: &ResolvedTarget,
        request: PreparedRequest,
        connect_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<Response> {
        let client = self.client(target, connect_timeout)?;
        let mut outgoing = client
            .request(request.method, target.target.url.clone())
            .headers(request.headers);
        if let Some(body) = request.body {
            outgoing = outgoing.body(body);
        }
        let future = outgoing.send();
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(UpstreamErrorCode::Cancelled.into()),
            result = future => result.map_err(|error| {
                if error.is_timeout() {
                    UpstreamError::from(UpstreamErrorCode::ConnectTimeout)
                } else {
                    UpstreamError::from(UpstreamErrorCode::UpstreamConnect)
                }
            })?,
        };
        let peer = response
            .remote_addr()
            .ok_or(UpstreamErrorCode::PeerUnverified)?;
        if !target
            .socket_addresses
            .iter()
            .any(|pinned| pinned.ip() == peer.ip() && pinned.port() == peer.port())
        {
            return Err(UpstreamErrorCode::PeerUnverified.into());
        }
        Ok(response)
    }
}
