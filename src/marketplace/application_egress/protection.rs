//! Original accepted configuration and its actual readonly dependencies. This
//! does not attest a live tunnel, resource admission, consent or an SDK grant.
use super::{
    gateway::{ApplicationGatewayProfile, GatewayInput},
    selection::Selection,
};
use crate::{db::Database, runtime::RuntimeManager, secrets::SecretsManager};
use anyhow::Result;
use elixir_marketplace::identity::Digest;
use std::sync::Arc;

pub(in crate::marketplace) struct Protection {
    selection: Selection,
    database: Database,
    secrets: Arc<SecretsManager>,
    runtime: Arc<dyn RuntimeManager>,
}
impl Protection {
    pub(in crate::marketplace) async fn capture(
        database: Database,
        secrets: Arc<SecretsManager>,
        runtime: Arc<dyn RuntimeManager>,
    ) -> Result<Option<Self>> {
        let Some(selection) = Selection::capture(&database, &secrets, runtime.as_ref()).await?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            selection,
            database,
            secrets,
            runtime,
        }))
    }
    pub(in crate::marketplace) async fn current(&self) -> Result<()> {
        self.selection
            .current(&self.database, &self.secrets, self.runtime.as_ref())
            .await
    }
    pub(in crate::marketplace) async fn prepare(
        &self,
        deployment: &str,
        network_id: &str,
        scope: Digest,
        invocation: Digest,
    ) -> Result<(ApplicationGatewayProfile, GatewayInput)> {
        let files = self
            .selection
            .material(&self.database, &self.secrets, self.runtime.as_ref())
            .await?;
        let profile = ApplicationGatewayProfile::gluetun(
            deployment,
            network_id,
            &self.selection,
            &files,
            scope,
            invocation,
        )?;
        let input = GatewayInput::new(profile.input(&self.selection.profile().kind, &files)?)?;
        self.current().await?;
        Ok((profile, input))
    }
}
