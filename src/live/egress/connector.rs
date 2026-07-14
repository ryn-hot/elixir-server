use std::{fmt, net::IpAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use reqwest::{Client, Response, Url, redirect::Policy};
use tokio_util::sync::CancellationToken;

use crate::live::upstream::{
    DnsResolver, EgressConnector, PreparedRequest, ResolvedTarget, UpstreamError,
    UpstreamErrorCode, UpstreamFetcher, UpstreamLimits,
};

type UpstreamResult<T> = std::result::Result<T, UpstreamError>;

use super::control::{
    ControlKeys, FetchControlRequest, ReadinessControlResponse, ResolveControlRequest,
    ResolveControlResponse, control_request_id, request_signature, seal_control_request,
    verify_response_signature,
};

const AUTH_HEADER: &str = "x-elixir-live-egress-auth";
const RESPONSE_SIGNATURE_HEADER: &str = "x-elixir-live-egress-response";
const RESPONSE_REQUEST_HEADER: &str = "x-elixir-live-egress-request";
const RESPONSE_PEER_HEADER: &str = "x-elixir-live-egress-peer";
const RESPONSE_FENCE_HEADER: &str = "x-elixir-live-egress-fence";
const RESPONSE_KIND_HEADER: &str = "x-elixir-live-egress-kind";
const MAX_CONTROL_RESPONSE_BYTES: u64 = 65_536;

#[derive(Clone)]
pub(crate) struct ProtectedEgressTransport {
    client: Arc<ControlClient>,
}

impl fmt::Debug for ProtectedEgressTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtectedEgressTransport")
            .field("endpoint", &self.client.endpoint)
            .field("control_fencing_token", &self.client.control_fencing_token)
            .finish_non_exhaustive()
    }
}

impl ProtectedEgressTransport {
    pub(crate) fn new(
        endpoint: Url,
        keys: ControlKeys,
        control_fencing_token: i64,
        timeout: Duration,
    ) -> UpstreamResult<Self> {
        if endpoint.scheme() != "http"
            || endpoint.host_str() != Some("127.0.0.1")
            || endpoint.port().is_none()
            || endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || control_fencing_token < 1
            || timeout.is_zero()
            || timeout > Duration::from_secs(60)
        {
            return Err(UpstreamErrorCode::EgressRejected.into());
        }
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(timeout)
            .pool_max_idle_per_host(4)
            .tcp_nodelay(true)
            .referer(false)
            .build()
            .map_err(|_| UpstreamError::from(UpstreamErrorCode::EgressRejected))?;
        Ok(Self {
            client: Arc::new(ControlClient {
                endpoint,
                keys,
                control_fencing_token,
                timeout,
                http,
            }),
        })
    }

    pub(crate) fn fetcher(&self, limits: UpstreamLimits) -> UpstreamResult<UpstreamFetcher> {
        UpstreamFetcher::with_connector(
            Arc::new(ProtectedDnsResolver {
                client: self.client.clone(),
            }),
            Arc::new(ProtectedConnector {
                client: self.client.clone(),
            }),
            limits,
        )
    }

    pub(crate) async fn readiness(
        &self,
        cancellation: &CancellationToken,
    ) -> UpstreamResult<ReadinessControlResponse> {
        let body = seal_control_request(&self.client.keys, &serde_json::json!({}), Utc::now())
            .map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let request_id =
            control_request_id(&body).map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let response = self
            .client
            .request("v1/readiness", body, cancellation)
            .await?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_CONTROL_RESPONSE_BYTES)
        {
            return Err(UpstreamErrorCode::EgressRejected.into());
        }
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| UpstreamErrorCode::EgressRejected)?;
        if bytes.len() as u64 > MAX_CONTROL_RESPONSE_BYTES {
            return Err(UpstreamErrorCode::EgressRejected.into());
        }
        self.client.verify_response_parts(
            status,
            &headers,
            request_id,
            "readiness",
            Some(&bytes),
        )?;
        serde_json::from_slice(&bytes).map_err(|_| UpstreamErrorCode::EgressRejected.into())
    }
}

struct ControlClient {
    endpoint: Url,
    keys: ControlKeys,
    control_fencing_token: i64,
    timeout: Duration,
    http: Client,
}

impl fmt::Debug for ControlClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlClient")
            .field("endpoint", &self.endpoint)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl ControlClient {
    async fn request(
        &self,
        path: &str,
        body: Vec<u8>,
        cancellation: &CancellationToken,
    ) -> UpstreamResult<Response> {
        let signature =
            request_signature(&self.keys, &body).map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let url = self
            .endpoint
            .join(path)
            .map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let future = self
            .http
            .post(url)
            .header(AUTH_HEADER, signature)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send();
        tokio::select! {
            _ = cancellation.cancelled() => Err(UpstreamErrorCode::Cancelled.into()),
            result = tokio::time::timeout(self.timeout, future) => match result {
                Ok(Ok(response)) => {
                    if !response.remote_addr().is_some_and(|peer| peer.ip().is_loopback()) {
                        return Err(UpstreamErrorCode::EgressRejected.into());
                    }
                    Ok(response)
                }
                Ok(Err(error)) if error.is_timeout() => Err(UpstreamErrorCode::ConnectTimeout.into()),
                Ok(Err(_)) => Err(UpstreamErrorCode::UpstreamConnect.into()),
                Err(_) => Err(UpstreamErrorCode::ConnectTimeout.into()),
            }
        }
    }

    fn verify_response(
        &self,
        response: &Response,
        request_id: uuid::Uuid,
        operation: &str,
        body: Option<&[u8]>,
    ) -> UpstreamResult<String> {
        self.verify_response_parts(
            response.status(),
            response.headers(),
            request_id,
            operation,
            body,
        )
    }

    fn verify_response_parts(
        &self,
        status: reqwest::StatusCode,
        headers: &reqwest::header::HeaderMap,
        request_id: uuid::Uuid,
        operation: &str,
        body: Option<&[u8]>,
    ) -> UpstreamResult<String> {
        let returned_request = header(headers, RESPONSE_REQUEST_HEADER)?;
        if returned_request != request_id.to_string() {
            return Err(UpstreamErrorCode::EgressRejected.into());
        }
        if header(headers, RESPONSE_KIND_HEADER)? != operation {
            return Err(UpstreamErrorCode::EgressRejected.into());
        }
        if header(headers, RESPONSE_FENCE_HEADER)?.parse::<i64>().ok()
            != Some(self.control_fencing_token)
        {
            return Err(UpstreamErrorCode::EgressRejected.into());
        }
        let peer = header(headers, RESPONSE_PEER_HEADER)?;
        verify_response_signature(
            &self.keys,
            header(headers, RESPONSE_SIGNATURE_HEADER)?.as_str(),
            request_id,
            operation,
            status.as_u16(),
            &peer,
            self.control_fencing_token,
            body,
        )
        .map_err(|_| UpstreamErrorCode::EgressRejected)?;
        Ok(peer)
    }
}

struct ProtectedDnsResolver {
    client: Arc<ControlClient>,
}

impl fmt::Debug for ProtectedDnsResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtectedDnsResolver")
    }
}

#[async_trait]
impl DnsResolver for ProtectedDnsResolver {
    async fn resolve(
        &self,
        host: &str,
        port: u16,
        cancellation: &CancellationToken,
    ) -> UpstreamResult<Vec<IpAddr>> {
        let body = seal_control_request(
            &self.client.keys,
            &ResolveControlRequest {
                host: host.to_string(),
                port,
            },
            Utc::now(),
        )
        .map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let request_id =
            control_request_id(&body).map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let response = self
            .client
            .request("v1/resolve", body, cancellation)
            .await?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_CONTROL_RESPONSE_BYTES)
        {
            return Err(UpstreamErrorCode::DnsFailed.into());
        }
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| UpstreamErrorCode::DnsFailed)?;
        if bytes.len() as u64 > MAX_CONTROL_RESPONSE_BYTES {
            return Err(UpstreamErrorCode::DnsFailed.into());
        }
        self.client
            .verify_response_parts(status, &headers, request_id, "resolve", Some(&bytes))?;
        let response: ResolveControlResponse =
            serde_json::from_slice(&bytes).map_err(|_| UpstreamErrorCode::DnsFailed)?;
        if response.addresses.is_empty() || response.addresses.len() > 16 {
            return Err(UpstreamErrorCode::DnsEmpty.into());
        }
        Ok(response.addresses)
    }
}

struct ProtectedConnector {
    client: Arc<ControlClient>,
}

impl fmt::Debug for ProtectedConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtectedConnector")
    }
}

#[async_trait]
impl EgressConnector for ProtectedConnector {
    async fn execute(
        &self,
        target: &ResolvedTarget,
        request: PreparedRequest,
        connect_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> UpstreamResult<Response> {
        let headers = request
            .headers
            .iter()
            .map(|(name, value)| {
                value
                    .to_str()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
                    .map_err(|_| UpstreamError::from(UpstreamErrorCode::EgressRejected))
            })
            .collect::<UpstreamResult<Vec<_>>>()?;
        let body = seal_control_request(
            &self.client.keys,
            &FetchControlRequest {
                method: request.method.as_str().to_string(),
                url: target.target.url.to_string(),
                socket_addresses: target
                    .socket_addresses
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                headers,
                connect_timeout_millis: u64::try_from(connect_timeout.as_millis())
                    .map_err(|_| UpstreamErrorCode::EgressRejected)?,
            },
            Utc::now(),
        )
        .map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let request_id =
            control_request_id(&body).map_err(|_| UpstreamErrorCode::EgressRejected)?;
        let mut response = self.client.request("v1/fetch", body, cancellation).await?;
        let peer = self
            .client
            .verify_response(&response, request_id, "fetch", None)?
            .parse::<std::net::SocketAddr>()
            .map_err(|_| UpstreamErrorCode::PeerUnverified)?;
        if !target.socket_addresses.contains(&peer) {
            return Err(UpstreamErrorCode::PeerUnverified.into());
        }
        for name in [
            RESPONSE_SIGNATURE_HEADER,
            RESPONSE_REQUEST_HEADER,
            RESPONSE_PEER_HEADER,
            RESPONSE_FENCE_HEADER,
            RESPONSE_KIND_HEADER,
        ] {
            response.headers_mut().remove(name);
        }
        Ok(response)
    }
}

fn header(headers: &reqwest::header::HeaderMap, name: &str) -> UpstreamResult<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .ok_or_else(|| UpstreamErrorCode::EgressRejected.into())
}
