use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;

use crate::orchestrator::model::ProviderEndpoint;
use crate::runtime::probe::ProbeRunner;

pub async fn ensure_binding_connectivity(
    probe: &dyn ProbeRunner,
    consumer: &ProviderEndpoint,
    provider: &ProviderEndpoint,
    reverse: bool,
) -> Result<()> {
    const CONNECTIVITY_ATTEMPTS: usize = 5;
    const CONNECTIVITY_RETRY_DELAY_MS: u64 = 500;

    let mut last_error = None;
    for attempt in 0..CONNECTIVITY_ATTEMPTS {
        match ensure_binding_connectivity_once(probe, consumer, provider, reverse).await {
            Ok(()) => return Ok(()),
            Err(err)
                if attempt + 1 < CONNECTIVITY_ATTEMPTS
                    && binding_connectivity_error_retryable(&err) =>
            {
                last_error = Some(err);
                sleep(Duration::from_millis(CONNECTIVITY_RETRY_DELAY_MS)).await;
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("binding connectivity check failed unexpectedly")))
}

async fn ensure_binding_connectivity_once(
    probe: &dyn ProbeRunner,
    consumer: &ProviderEndpoint,
    provider: &ProviderEndpoint,
    reverse: bool,
) -> Result<()> {
    probe_endpoint(probe, provider).await?;
    if reverse {
        probe_endpoint(probe, consumer).await?;
    }
    Ok(())
}

async fn probe_endpoint(probe: &dyn ProbeRunner, endpoint: &ProviderEndpoint) -> Result<()> {
    let url = match endpoint.scheme.as_str() {
        "http" | "https" => Some(endpoint.canonical_url()?),
        _ => None,
    };
    probe
        .assert_reachable(&endpoint.host, endpoint.port, url.as_deref())
        .await
}

fn binding_connectivity_error_retryable(err: &anyhow::Error) -> bool {
    let lower = err.to_string().to_ascii_lowercase();
    lower.contains("probe tcp failed")
        || lower.contains("probe http failed")
        || lower.contains("tcp connect failed")
        || lower.contains("connection refused")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("temporarily unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use async_trait::async_trait;

    struct FlakyProbe {
        remaining_tcp_failures: Mutex<usize>,
    }

    #[async_trait]
    impl ProbeRunner for FlakyProbe {
        async fn probe_dns(&self, _name: &str) -> Result<crate::runtime::probe::ProbeResult> {
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_tcp(
            &self,
            _host: &str,
            _port: u16,
        ) -> Result<crate::runtime::probe::ProbeResult> {
            let mut remaining = self
                .remaining_tcp_failures
                .lock()
                .expect("remaining_tcp_failures lock");
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(crate::runtime::probe::ProbeResult {
                    ok: false,
                    latency_ms: Some(1),
                    details: Some(serde_json::json!({ "error": "tcp connect failed" })),
                });
            }
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_http(&self, _url: &str) -> Result<crate::runtime::probe::ProbeResult> {
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }
    }

    #[tokio::test]
    async fn binding_connectivity_retries_transient_tcp_failures() -> Result<()> {
        let probe = FlakyProbe {
            remaining_tcp_failures: Mutex::new(2),
        };
        let provider = ProviderEndpoint::new(
            "http".to_string(),
            "svc-provider".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        ensure_binding_connectivity(&probe, &provider, &provider, false).await?;
        Ok(())
    }
}
