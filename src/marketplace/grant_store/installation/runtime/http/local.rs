//! Local endpoint restrictions stay attached to the ORIGINAL installed/native
//! call. A selected local endpoint never falls through to public networking.
use super::*;
use crate::marketplace::grant_store::{LocalEndpoint, sdk::local_http};
use zeroize::Zeroizing;

struct LocalAuthority {
    base: Authority,
    secrets: SecretsManager,
    requested: Zeroizing<String>,
    endpoint_revision: Digest,
}
#[async_trait::async_trait]
impl SdkHttpAuthority for LocalAuthority {
    async fn current(&self) -> Result<()> {
        self.base.check()?;
        self.base.native.current().await?;
        let endpoint = local_http::load(
            &self.base.database,
            &self.secrets,
            &self.base.binding,
            &self.requested,
        )
        .await?
        .context("configured local endpoint is no longer authorized")?;
        if endpoint.revision() != self.endpoint_revision {
            bail!("configured local endpoint changed during its original request");
        }
        self.base.check()
    }
}
impl InstalledBroker<'_> {
    pub(super) fn local_call(
        &self,
        raw: &RuntimeJson,
        maximum: usize,
        endpoint: LocalEndpoint,
        native: Arc<NativeInvocation>,
        streaming: bool,
    ) -> Result<Option<Call>> {
        let Some(client) = self.local_http else {
            return Ok(None);
        };
        let operation = elixir_marketplace::source_execution::SourceSdkRequest::parse(raw)?;
        let Some(elixir_marketplace::source_execution::SourceSdkRequest::Fetch { url, .. }) =
            operation.http_request()
        else {
            bail!("configured local HTTP operation required")
        };
        let authority = Arc::new(LocalAuthority {
            base: Authority {
                database: self.database.clone(),
                binding: self.binding.clone(),
                native,
                stop: self.stop.clone(),
                expires: self.expires,
                application_admission: None,
            },
            secrets: self.secrets.clone(),
            requested: Zeroizing::new(url.clone()),
            endpoint_revision: endpoint.revision(),
        });
        let started = if streaming {
            client
                .start_stream(raw, maximum, endpoint, authority)
                .map(Call::LocalStream)
        } else {
            client
                .start(raw, maximum, endpoint, authority)
                .map(Call::Local)
        };
        match started {
            Ok(call) => Ok(Some(call)),
            Err(error) => {
                tracing::debug!(reason = %error, "configured local SDK HTTP admission unavailable");
                self.check()?;
                Ok(None)
            }
        }
    }
}
