//! Application HTTP over the original owned VPN namespace. Unlike catalog
//! HTTP this never spends a relay allowance or uses the catalog's helper slot.
//! The installed owner must supply live effect/resource authority; this module
//! retains the native owners outside any fallible/cancelled VM exchange.
use super::{ApplicationGatewayObservation, Request};
use crate::marketplace::quarantine::execution::{GatewayLaunch, HttpLaunch};
use crate::marketplace::runtime::application_http::HttpMode;
use crate::marketplace::worker::IndependentAdmission;
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    connectivity::{ProbeContext, ProbeFamily},
    identity::Digest,
    sdk_http::{HttpReply, HttpRequest},
    source_execution::{RuntimeJson, SourceSdkReply},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use zeroize::Zeroizing;

/// Trusted startup configuration only. The underlay is an exact owned native
/// network, not a public peer URL, relay allowance, or proof of a working VPN.
#[derive(Clone)]
pub(in crate::marketplace) struct Configuration {
    network_id: Option<String>,
    family: ProbeFamily,
    pool: Option<Arc<super::WarmPool>>,
}
impl Configuration {
    pub(in crate::marketplace) fn new(network_id: String, family: ProbeFamily) -> Result<Self> {
        if !crate::runtime::packet_network::immutable_id(&network_id) {
            bail!("application HTTP requires an immutable owned underlay ID");
        }
        Ok(Self {
            network_id: Some(network_id),
            family,
            pool: None,
        })
    }
    /// Selected but unavailable is distinct from not selecting application
    /// protection. Never turn provisioning failure into a legacy relay fallback.
    pub(in crate::marketplace) fn unavailable(family: ProbeFamily) -> Self {
        Self {
            network_id: None,
            family,
            pool: None,
        }
    }
    pub(in crate::marketplace) fn network(&self) -> Option<(&str, ProbeFamily)> {
        self.network_id.as_deref().map(|id| (id, self.family))
    }
    pub(in crate::marketplace) fn has_network(&self) -> bool {
        self.network_id.is_some()
    }
    pub(in crate::marketplace) fn pool(&self) -> Option<&Arc<super::WarmPool>> {
        self.pool.as_ref()
    }
    pub(in crate::marketplace) fn set_pool(&mut self, pool: Arc<super::WarmPool>) {
        self.pool = Some(pool);
    }
    pub(in crate::marketplace) async fn shutdown(&self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.shutdown().await?;
        }
        Ok(())
    }
}

enum Gateway {
    Invocation(GatewayLaunch),
    Warm(super::warm::Lease),
}
impl Gateway {
    async fn materialized(&mut self) -> Result<()> {
        match self {
            Self::Invocation(gateway) => {
                gateway.materialized().await?;
                Ok(())
            }
            Self::Warm(lease) => lease.materialized().await,
        }
    }
    async fn observe(&self) -> Result<ApplicationGatewayObservation> {
        match self {
            Self::Invocation(gateway) => gateway.observe().await,
            Self::Warm(lease) => lease.observe().await,
        }
    }
    async fn wait_tunnel(&self) -> Result<ApplicationGatewayObservation> {
        match self {
            Self::Invocation(gateway) => gateway.wait_tunnel().await,
            Self::Warm(lease) => lease.wait_tunnel().await,
        }
    }
    async fn launch_http(&mut self, image: &str, mode: HttpMode) -> Result<HttpLaunch> {
        match self {
            Self::Invocation(gateway) => gateway.launch_http_mode(image, mode).await,
            Self::Warm(lease) => lease.launch_http(image, mode).await,
        }
    }
    async fn cancel(&mut self) -> Result<()> {
        match self {
            Self::Invocation(gateway) => gateway.cancel().await,
            Self::Warm(lease) => lease.release(),
        }
    }
}

pub(in crate::marketplace) struct Stream {
    gateway: Gateway,
    http: Option<HttpLaunch>,
    image: String,
    protocol: Request,
    probing: Option<HttpRequest>,
    original: Option<ApplicationGatewayObservation>,
    family: ProbeFamily,
    started: bool,
    opened: bool,
    closing: bool,
    admission: Arc<IndependentAdmission>,
    probed: Option<Instant>,
}
impl Stream {
    /// No effects or Ready here. Retain this object in the installed call owner
    /// BEFORE awaiting open(), including when its VM/request waiter disappears.
    pub(in crate::marketplace) fn new(
        gateway: GatewayLaunch,
        image: String,
        protocol: Request,
        family: ProbeFamily,
        admission: Arc<IndependentAdmission>,
    ) -> Self {
        Self::with_gateway(
            Gateway::Invocation(gateway),
            image,
            protocol,
            family,
            admission,
        )
    }
    pub(in crate::marketplace) fn warm(
        lease: super::warm::Lease,
        image: String,
        protocol: Request,
        family: ProbeFamily,
        admission: Arc<IndependentAdmission>,
    ) -> Self {
        Self::with_gateway(Gateway::Warm(lease), image, protocol, family, admission)
    }
    fn with_gateway(
        gateway: Gateway,
        image: String,
        protocol: Request,
        family: ProbeFamily,
        admission: Arc<IndependentAdmission>,
    ) -> Self {
        Self {
            gateway,
            http: None,
            image,
            protocol,
            probing: None,
            original: None,
            family,
            started: false,
            opened: false,
            closing: false,
            admission,
            probed: None,
        }
    }
    pub(in crate::marketplace) fn id(&self) -> Option<u32> {
        self.protocol.id()
    }
    /// Closed HTTP + actually retired child, not permission to use the VPN.
    /// Keep the same gateway for this ORIGINAL invocation's next SDK request.
    pub(in crate::marketplace) fn idle(&self) -> bool {
        self.protocol.closed() && self.http.is_none() && self.probing.is_none()
    }
    pub(in crate::marketplace) fn reset(&mut self, protocol: Request) -> Result<()> {
        if !self.idle() {
            bail!("the original application HTTP child is not retired");
        }
        self.protocol = protocol;
        self.started = false;
        self.opened = false;
        self.closing = false;
        Ok(())
    }
    async fn observation(&self) -> Result<ApplicationGatewayObservation> {
        let actual = self.gateway.observe().await?;
        if let Some(original) = &self.original
            && !actual.same_guard(original)
        {
            bail!("application HTTP original tunnel or policy changed");
        }
        if !actual.addresses().iter().any(|address| {
            matches!(
                (self.family, address),
                (ProbeFamily::Ipv4, std::net::IpAddr::V4(_))
                    | (ProbeFamily::Ipv6, std::net::IpAddr::V6(_))
            )
        }) {
            bail!("application HTTP tunnel lacks the selected address family");
        }
        self.admission.application_current(&actual).await?;
        Ok(actual)
    }
    fn context(actual: &ApplicationGatewayObservation) -> ProbeContext<'_> {
        ProbeContext {
            namespace_inode: actual.namespace(),
            tunnel_addresses: actual.addresses(),
            memory_bytes: crate::marketplace::runtime::application_http::MEMORY_BYTES,
            cpu_millicores: crate::marketplace::runtime::application_http::CPU_MILLICORES,
            pids: 64,
        }
    }
    async fn launch(&mut self, mode: HttpMode) -> Result<()> {
        if self.http.is_some() {
            bail!("the original application HTTP helper is still owned");
        }
        // The factory has no await between its durable marker/task creation
        // and return. Retain that original task before its startup/attach wait.
        self.http = Some(self.gateway.launch_http(&self.image, mode).await?);
        self.http.as_mut().unwrap().attach_io().await
    }
    async fn exchange(
        &mut self,
        bytes: &[u8],
    ) -> Result<(Zeroizing<Vec<u8>>, ApplicationGatewayObservation)> {
        self.observation().await?;
        let answer = self
            .http
            .as_mut()
            .context("application HTTP original helper absent")?
            .exchange_io(bytes)
            .await?;
        // Return the fresh POST-exchange observation with these exact bytes.
        // The codec consumes both synchronously, with no intervening effect or
        // await. Re-observing again in each caller only repeats native/resource
        // checks; it does not strengthen this boundary. Keep the independent
        // supervision and every pre/post-input authority check unchanged.
        let actual = self.observation().await?;
        Ok((answer, actual))
    }
    async fn retire_http(&mut self) -> Result<()> {
        if let Some(http) = self.http.as_mut() {
            http.cancel().await?;
            self.http = None;
        }
        Ok(())
    }
    /// Fixed public DNS + certificate-verified HTTPS probe uses a separate
    /// original helper, never the extension's URL. No redirects, custom roots,
    /// proxy, fallback, retry or publisher-controlled bytes enter this probe.
    async fn probe(&mut self) -> Result<()> {
        let raw = probe_request()?;
        self.probing = Some(HttpRequest::new(&raw, 4096, nonce(), self.family)?);
        self.launch(HttpMode::Buffered).await?;
        let bytes = Zeroizing::new(self.probing.as_ref().unwrap().to_bytes()?);
        let (answer, actual) = self.exchange(&bytes).await?;
        let opened = HttpReply::parse(
            &answer,
            self.probing.as_ref().unwrap(),
            &Self::context(&actual),
        )?;
        let SourceSdkReply::Value(opened) = opened else {
            bail!("application connectivity probe did not return headers");
        };
        let opened: serde_json::Value = serde_json::from_str(opened.as_str())?;
        let status = opened
            .get("status")
            .and_then(serde_json::Value::as_u64)
            .context("application connectivity probe status missing")?;
        if status != 200 {
            bail!("application connectivity probe returned unexpected status {status}");
        }
        self.http.as_mut().unwrap().finish_io().await?;
        self.retire_http().await?;
        self.probing = None;
        Ok(())
    }
    async fn prepare(&mut self) -> Result<()> {
        if self.started {
            bail!("application HTTP open cannot be retried");
        }
        self.started = true;
        self.gateway.materialized().await?;
        if self.original.is_none() {
            self.original = Some(self.gateway.wait_tunnel().await?);
        }
        self.observation().await?;
        if self
            .probed
            .is_none_or(|at| at.elapsed() >= Duration::from_secs(15))
        {
            self.probe().await?;
            self.probed = Some(Instant::now());
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn open(&mut self) -> Result<SourceSdkReply> {
        self.protocol.stream()?;
        self.prepare().await?;
        self.launch(HttpMode::StreamLarge).await?;
        let bytes = Zeroizing::new(self.protocol.stream()?.open()?);
        let (answer, actual) = self.exchange(&bytes).await?;
        let value = self
            .protocol
            .stream()?
            .accept(&answer, &Self::context(&actual))?;
        self.opened = true;
        Ok(value)
    }
    pub(in crate::marketplace) async fn read(&mut self, maximum: usize) -> Result<SourceSdkReply> {
        if !self.opened || self.closing {
            bail!("application HTTP stream is not readable");
        }
        let bytes = Zeroizing::new(self.protocol.stream()?.read(maximum)?);
        let (answer, actual) = self.exchange(&bytes).await?;
        Ok(self
            .protocol
            .stream()?
            .accept(&answer, &Self::context(&actual))?)
    }
    pub(in crate::marketplace) async fn close(&mut self) -> Result<SourceSdkReply> {
        if !self.opened || self.closing {
            bail!("application HTTP stream is not closable");
        }
        self.closing = true;
        let bytes = Zeroizing::new(self.protocol.stream()?.close()?);
        let (answer, actual) = self.exchange(&bytes).await?;
        let value = self
            .protocol
            .stream()?
            .accept(&answer, &Self::context(&actual))?;
        if !self.protocol.closed() {
            bail!("application HTTP protocol did not close");
        }
        self.http.as_mut().unwrap().finish_io().await?;
        // Close is exposed only after native child/CLI/readers have actually
        // joined and the exact child marker is retired. The same invocation
        // retains its gateway; the broker retires it before source disposal.
        self.retire_http().await?;
        Ok(value)
    }
    /// One already supported buffered helper/exchange, not a synthetic stream.
    /// Same 2-MiB body bound; no retries, Range requests or publisher process.
    pub(in crate::marketplace) async fn buffered(
        &mut self,
        maximum: usize,
    ) -> Result<SourceSdkReply> {
        if self.protocol.buffered()?.maximum_reply_bytes as usize != maximum {
            bail!("buffered HTTP reply allowance differs from the original request");
        }
        self.prepare().await?;
        self.launch(HttpMode::Buffered).await?;
        let bytes = Zeroizing::new(self.protocol.buffered()?.to_bytes()?);
        let (answer, actual) = self.exchange(&bytes).await?;
        let reply = HttpReply::parse(&answer, self.protocol.buffered()?, &Self::context(&actual))?;
        self.http.as_mut().unwrap().finish_io().await?;
        self.retire_http().await?;
        self.protocol.complete_buffered()?;
        Ok(reply)
    }
    pub(in crate::marketplace) async fn cancel(&mut self) -> Result<()> {
        self.closing = true;
        // Never tear down/reuse the namespace before its SAME HTTP owner joins.
        self.retire_http().await?;
        self.gateway.cancel().await
    }
}
fn nonce() -> Digest {
    Digest::of(uuid::Uuid::new_v4().as_bytes())
}
fn probe_request() -> Result<RuntimeJson> {
    Ok(RuntimeJson::from_value(serde_json::json!({
        "operation":"fetch", "method":"GET",
            "url":"https://www.cloudflare.com/cdn-cgi/trace",
            "headers":[], "body":null
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn application_configuration_requires_an_exact_native_network_not_a_name_or_url() {
        let unavailable = Configuration::unavailable(ProbeFamily::Ipv4);
        assert!(unavailable.network().is_none());
        assert!(!unavailable.has_network());
        assert!(Configuration::new("a".repeat(64), ProbeFamily::Ipv4).is_ok());
        for invalid in [
            "bridge",
            "host",
            "container:abc",
            "https://example.com",
            "",
            &"A".repeat(64),
        ] {
            assert!(Configuration::new(invalid.into(), ProbeFamily::Ipv4).is_err());
        }
    }
    #[test]
    fn connectivity_input_is_fixed_small_and_independent_of_extension_http() -> Result<()> {
        use elixir_marketplace::source_execution::{SdkHttpMethod, SourceSdkRequest};
        let raw = probe_request()?;
        let SourceSdkRequest::Fetch {
            method,
            url,
            headers,
            body,
        } = SourceSdkRequest::parse(&raw)?
        else {
            panic!("fixed fetch required");
        };
        assert!(matches!(method, SdkHttpMethod::Get));
        assert_eq!(url, "https://www.cloudflare.com/cdn-cgi/trace");
        assert!(headers.is_empty() && body.is_none());
        let request = HttpRequest::new(&raw, 4096, nonce(), ProbeFamily::Ipv4)?;
        let encoded = request.to_bytes()?;
        assert!(encoded.len() < 1024);
        assert_eq!(HttpRequest::parse(&encoded)?.maximum_reply_bytes, 4096);
        Ok(())
    }
}
