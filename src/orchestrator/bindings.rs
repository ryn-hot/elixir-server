use anyhow::Result;

use crate::orchestrator::model::ProviderEndpoint;
use crate::runtime::probe::ProbeRunner;

pub async fn ensure_binding_connectivity(
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
