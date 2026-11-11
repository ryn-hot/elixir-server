use super::{Profile, fence::Fence};
use crate::{
    marketplace::{lease::ControllerLease, provider_client::Client, provider_state},
    runtime::{RuntimeManager, model::ContainerHandle, provider_creation::ProviderCreation},
    secrets::SecretsManager,
};
use anyhow::{Context, Result, ensure};
use elixir_marketplace::{identity::Digest, warp_provider::Scope};
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;

/// A waiter does not own/cancel a native CREATE future. Dropping it requests
/// cancellation; the original task retains its lease and physical outcomes.
pub(in crate::marketplace) struct Pending<T = Owner> {
    pub(super) task: tokio::task::JoinHandle<Result<T>>,
    pub(super) cancel: CancellationToken,
}

#[cfg(test)]
mod tests;
impl<T> Pending<T> {
    pub(in crate::marketplace) fn new(
        task: tokio::task::JoinHandle<Result<T>>,
        cancel: CancellationToken,
    ) -> Self {
        Self { task, cancel }
    }
    pub(in crate::marketplace) async fn wait(mut self) -> Result<T> {
        (&mut self.task)
            .await
            .context("provider owner startup task")?
    }
    pub(in crate::marketplace) fn cancel(&self) {
        self.cancel.cancel();
    }
}
impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Retain the exact native profile, CID, journal, private client and OS lease.
/// This is NOT VPN health or a public worker capability. Drop kills its original
/// attachment; the native watchdog is finite and the journal is NOT cleared.
pub(in crate::marketplace) struct Owner {
    pub(super) runtime: Arc<dyn RuntimeManager>,
    pub(super) profile: Profile,
    pub(super) handle: ContainerHandle,
    pub(super) lease: Arc<ControllerLease>,
    pub(super) fence: Option<Fence>,
    pub(super) client: Option<tokio::sync::Mutex<Client>>,
}
impl Owner {
    /// Local accepted-owner setup only. `resume` is explicit intent, never
    /// inferred as fresh enrollment after a missing/corrupt identity.
    pub(in crate::marketplace) fn launch(
        runtime: Arc<dyn RuntimeManager>,
        directory: PathBuf,
        deployment: String,
        image: String,
        scope: Scope,
        resume: bool,
        secrets: Arc<SecretsManager>,
    ) -> Pending {
        Self::launch_owned(
            runtime,
            directory,
            deployment,
            image,
            scope,
            Some(resume),
            secrets,
            None,
        )
    }
    /// Trusted startup retains the exact original controller lease. Explicit
    /// consent was checked in the local profile; the durable one-shot enrollment
    /// marker below, not a missing checkpoint, chooses initial registration.
    pub(in crate::marketplace) fn launch_consented(
        runtime: Arc<dyn RuntimeManager>,
        lease: ControllerLease,
        deployment: String,
        image: String,
        scope: Scope,
        secrets: Arc<SecretsManager>,
    ) -> Pending {
        Self::launch_owned(
            runtime,
            lease.directory().to_owned(),
            deployment,
            image,
            scope,
            None,
            secrets,
            Some(lease),
        )
    }
    fn launch_owned(
        runtime: Arc<dyn RuntimeManager>,
        directory: PathBuf,
        deployment: String,
        image: String,
        scope: Scope,
        intent: Option<bool>,
        secrets: Arc<SecretsManager>,
        lease: Option<ControllerLease>,
    ) -> Pending {
        let cancel = CancellationToken::new();
        let request = cancel.clone();
        let task = tokio::spawn(async move {
            let lease = tokio::task::spawn_blocking(move || match lease {
                Some(lease) => {
                    ensure!(
                        lease.matches_directory(&directory),
                        "provider startup lease changed"
                    );
                    Ok(lease)
                }
                None => ControllerLease::acquire_for_recovery(&directory),
            })
            .await
            .context("provider OS owner task")??;
            let lease = Arc::new(lease);
            let image_digest: Digest = image.parse()?;
            scope.validate()?;
            ensure!(
                scope.deployment == Profile::deployment_scope(&deployment),
                "provider setup belongs to another deployment"
            );
            let spec = Profile::network_spec(&deployment)?;
            lease.require_provider_lane()?;
            if let Some(fence) = Fence::load(&lease)? {
                fence.require_setup(&deployment, scope, &image)?;
            }
            Fence::recover(runtime.as_ref(), &lease).await?;
            let owner = lease.clone();
            let key = secrets.clone();
            let resume = tokio::task::spawn_blocking(move || -> Result<bool> {
                provider_state::recover(&owner, &key, scope, image_digest)?;
                let exists = provider_state::load(&owner, &key, scope, image_digest)?.is_some();
                match intent {
                    Some(resume) => {
                        ensure!(
                            exists == resume,
                            "provider startup intent differs from retained identity"
                        );
                        Ok(resume)
                    }
                    None => super::enrollment::intent(&owner, scope, image_digest, exists),
                }
            })
            .await
            .context("provider identity preparation task")??;
            ensure!(!request.is_cancelled(), "provider startup cancelled");
            let network = runtime.ensure_packet_uplink(&spec).await?;
            spec.verify(&network)?;
            ensure!(
                network.members.is_empty(),
                "provider uplink has existing or foreign members"
            );
            let profile = Profile::compile(
                &deployment,
                &image,
                &network.id,
                scope,
                Digest::of(uuid::Uuid::new_v4().as_bytes()),
                resume,
            )?;
            let mut fence = Fence::begin(&lease, &profile)?;
            let result = async {
                ensure!(!request.is_cancelled(), "provider startup cancelled");
                let attempt = fence.creating(&lease)?;
                let handle = match runtime.create_provider(&profile, attempt).await? {
                    ProviderCreation::Created(handle) => handle,
                    ProviderCreation::NotAttempted(outcome) => {
                        return Err(fence.not_attempted(&lease, outcome)?);
                    }
                };
                let start = fence.bind(&lease, &handle)?;
                ensure!(
                    !request.is_cancelled(),
                    "provider startup cancelled before start"
                );
                let stream = runtime.launch_provider(&handle, &profile, start).await?;
                #[cfg(all(test, unix))]
                if !resume {
                    // The fixed native actor waits for its private Start frame
                    // before spawning the vendor service or enrolling. This
                    // optional test barrier is never present in a release.
                    super::guard::test_capture::provider_prestart(&deployment, &handle.id).await?;
                    ensure!(
                        !request.is_cancelled(),
                        "provider startup cancelled before enrollment"
                    );
                }
                let client = Client::start_shared(
                    stream,
                    profile.start_data(),
                    image_digest,
                    lease.clone(),
                    secrets,
                )
                .await?;
                Ok::<_, anyhow::Error>((handle, client))
            }
            .await;
            match result {
                Ok((handle, client)) => {
                    let owner = Owner {
                        runtime,
                        profile,
                        handle,
                        lease,
                        fence: Some(fence),
                        client: Some(tokio::sync::Mutex::new(client)),
                    };
                    if request.is_cancelled() {
                        owner.close(false).await?;
                        anyhow::bail!("provider startup cancelled");
                    }
                    owner.require_owned().await?;
                    Ok(owner)
                }
                Err(error) => {
                    // The same original task has observed its CREATE/START
                    // result. Recovery may stop it, never START from a record.
                    Fence::recover(runtime.as_ref(), &lease)
                        .await
                        .context("provider startup failed and native outcome remains fenced")?;
                    Err(error)
                }
            }
        });
        Pending { task, cancel }
    }
    pub(in crate::marketplace) async fn require_owned(&self) -> Result<()> {
        self.lease.require_provider_lane()?;
        self.fence
            .as_ref()
            .context("provider native fence missing")?
            .current(&self.lease)?;
        ensure!(
            self.lease.matches_directory(self.lease.directory()),
            "provider original OS lease changed"
        );
        let state = self
            .runtime
            .inspect_provider(&self.handle, &self.profile)
            .await?;
        ensure!(
            state.running && state.status == "running",
            "owned provider is not running"
        );
        Ok(())
    }
    pub(in crate::marketplace) async fn connect(&mut self) -> Result<()> {
        self.require_owned().await?;
        self.client
            .as_mut()
            .context("provider client retired")?
            .get_mut()
            .connect()
            .await
    }
    /// Fresh configuration/private-channel check and raw link presence only.
    /// The future guard/DNS/TLS stage must produce its own health capability.
    pub(in crate::marketplace) async fn inspect(&self) -> Result<bool> {
        self.require_owned().await?;
        self.client
            .as_ref()
            .context("provider client retired")?
            .lock()
            .await
            .inspect()
            .await
    }
    /// Fresh private-service liveness plus the exact native socket, executable,
    /// namespace and tunnel addresses. Still DATA, not protected DNS/TLS health.
    pub(in crate::marketplace) async fn transport(
        &self,
    ) -> Result<crate::runtime::provider_observation::ProviderTransport> {
        ensure!(self.inspect().await?, "provider tunnel is absent");
        let transport = self
            .runtime
            .provider_transport(&self.handle, &self.profile)
            .await?;
        ensure!(self.inspect().await?, "provider tunnel disappeared");
        Ok(transport)
    }
    pub(in crate::marketplace) async fn probe_exit(&self, ipv6: bool) -> Result<()> {
        ensure!(self.inspect().await?, "provider tunnel is absent");
        self.runtime
            .probe_provider_exit(&self.handle, &self.profile, ipv6)
            .await?;
        ensure!(
            self.inspect().await?,
            "provider tunnel disappeared during protected probe"
        );
        Ok(())
    }
    pub(in crate::marketplace) fn handle(&self) -> &ContainerHandle {
        &self.handle
    }
    pub(in crate::marketplace) fn profile(&self) -> &Profile {
        &self.profile
    }
    pub(in crate::marketplace) async fn close(mut self, revoke: bool) -> Result<()> {
        self.fence
            .as_mut()
            .context("provider native fence missing")?
            .retire_guard(self.runtime.as_ref(), &self.lease)
            .await?;
        if let Some(client) = self.client.take() {
            let client = client.into_inner();
            if revoke {
                client.revoke().await?;
            } else {
                client.stop().await?;
            }
        }
        let mut fence = self.fence.take().context("provider native fence missing")?;
        fence.retire(self.runtime.as_ref(), &self.lease).await?;
        Ok(())
    }
}
