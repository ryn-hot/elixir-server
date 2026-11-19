//! Physical source-job admission for installed extensions, without a catalog,
//! browser, issuer, receipt, network credit or marketplace connection.
//! Installed extensions are not held back for playback, server memory, I/O
//! pressure or disk headroom: their sandbox's own ceilings (memory, process
//! count, the 10-minute stop) are the only bounds. No measurement container
//! runs before a call.
use super::*;
use crate::marketplace::quarantine::{HostObservation, ResourceAdmission};

pub(in crate::marketplace) struct IndependentAdmission {
    application: bool,
}
impl IndependentAdmission {
    /// Trusted retained-storage configuration, not a peer/HTTP path. It must
    /// exist: unavailable storage is not an optimistic free-space estimate.
    pub(in crate::marketplace) fn new(
        playback: Arc<PlaybackJobManager>,
        storage: PathBuf,
    ) -> Result<Arc<Self>> {
        Self::configured(playback, storage, false)
    }
    pub(in crate::marketplace) fn for_application(
        playback: Arc<PlaybackJobManager>,
        storage: PathBuf,
    ) -> Result<Arc<Self>> {
        Self::configured(playback, storage, true)
    }
    fn configured(
        _playback: Arc<PlaybackJobManager>,
        storage: PathBuf,
        application: bool,
    ) -> Result<Arc<Self>> {
        if !storage.is_absolute() || !std::fs::symlink_metadata(&storage)?.is_dir() {
            bail!("installed source storage must be an existing real directory");
        }
        Ok(Arc::new(Self { application }))
    }
    pub(in crate::marketplace) async fn application_server_current(&self) -> Result<()> {
        if !self.application {
            bail!("application HTTP requires its independent resource reservation");
        }
        Ok(())
    }
    pub(in crate::marketplace) async fn application_current(
        &self,
        actual: &crate::marketplace::application_egress::ApplicationGatewayObservation,
    ) -> Result<()> {
        self.application_server_current().await?;
        // Parse the gateway's report so a malformed one still fails closed;
        // its memory and pressure figures do not hold the call back.
        linux_available(actual.resources())?;
        Ok(())
    }
}
/// A long-lived executor starts without waiting for playback or for spare
/// server memory; its sandbox memory ceiling is its only bound.
pub(in crate::marketplace) async fn serve_start_admitted(
    _playback: &crate::playback::PlaybackJobManager,
    _memory_bytes: u64,
) -> Result<()> {
    Ok(())
}
#[async_trait::async_trait]
impl ResourceAdmission for IndependentAdmission {
    async fn check(
        &self,
        _plan: &SourceQuarantinePlan,
        _provisioned: bool,
        _observation: Option<&HostObservation>,
    ) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn installed_calls_are_never_held_back_for_server_resources() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = directory.path().canonicalize()?;
        let database = crate::db::Database::connect(&crate::config::DatabaseConfig {
            url: "sqlite::memory:".to_owned(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        let playback = Arc::new(PlaybackJobManager::new(database.pool.clone(), None));
        let plain = IndependentAdmission::new(playback.clone(), storage.clone())?;
        let application = IndependentAdmission::for_application(playback.clone(), storage)?;
        assert!(!plain.needs_measurement());
        assert!(plain.application_server_current().await.is_err());
        application.application_server_current().await?;
        serve_start_admitted(&playback, 64 * 1024 * 1024 * 1024).await?;
        assert!(IndependentAdmission::new(playback, PathBuf::from("relative")).is_err());
        Ok(())
    }
}
