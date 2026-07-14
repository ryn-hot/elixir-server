use std::{fmt, net::IpAddr, time::Duration};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::error::{Result, UpstreamErrorCode};

#[async_trait]
pub trait DnsResolver: Send + Sync {
    async fn resolve(
        &self,
        host: &str,
        port: u16,
        cancellation: &CancellationToken,
    ) -> Result<Vec<IpAddr>>;
}

#[derive(Clone)]
pub struct SystemDnsResolver {
    timeout: Duration,
}

impl fmt::Debug for SystemDnsResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemDnsResolver")
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl SystemDnsResolver {
    pub fn new(timeout: Duration) -> Result<Self> {
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(UpstreamErrorCode::DnsTimeout.into());
        }
        Ok(Self { timeout })
    }
}

#[async_trait]
impl DnsResolver for SystemDnsResolver {
    async fn resolve(
        &self,
        host: &str,
        port: u16,
        cancellation: &CancellationToken,
    ) -> Result<Vec<IpAddr>> {
        let lookup = tokio::net::lookup_host((host, port));
        let addresses = tokio::select! {
            _ = cancellation.cancelled() => return Err(UpstreamErrorCode::Cancelled.into()),
            result = tokio::time::timeout(self.timeout, lookup) => match result {
                Ok(Ok(values)) => values.map(|value| value.ip()).collect::<Vec<_>>(),
                Ok(Err(_)) => return Err(UpstreamErrorCode::DnsFailed.into()),
                Err(_) => return Err(UpstreamErrorCode::DnsTimeout.into()),
            },
        };
        if addresses.is_empty() {
            return Err(UpstreamErrorCode::DnsEmpty.into());
        }
        Ok(addresses)
    }
}
