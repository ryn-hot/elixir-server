//! One durable worker child of the ORIGINAL provider guard. Profiles/records
//! are cleanup DATA; only original create/start tokens permit native actions.
use super::{GuardProfile, Owner};
use crate::{
    marketplace::storage::{FixedStateVolume, FixedWorkerSpec},
    runtime::{
        PrivateProcessStdio,
        model::{
            CONTAINER_SPEC_HASH_LABEL, ContainerHandle, ContainerSpec, ELIXIR_DEPLOYMENT_ID_LABEL,
        },
        provider_creation::WorkerCreation,
    },
};
use anyhow::{Context, Result, ensure};
use elixir_marketplace::identity::PublicKey;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    image: String,
    issuer_keys: Vec<PublicKey>,
    fingerprint: String,
}
impl Intent {
    pub(super) fn profile(&self, parent: GuardProfile, id: &str) -> Result<Profile> {
        let profile = Profile::compile(parent, id, &self.image, &self.issuer_keys)?;
        ensure!(
            profile.intent == *self,
            "worker intent differs from frozen profile"
        );
        Ok(profile)
    }
}
pub struct Profile {
    pub(super) intent: Intent,
    fixed: FixedWorkerSpec,
    parent: GuardProfile,
    guard: ContainerHandle,
}
impl Profile {
    pub(super) fn compile(
        parent: GuardProfile,
        id: &str,
        image: &str,
        issuer_keys: &[PublicKey],
    ) -> Result<Self> {
        ensure!(
            crate::runtime::packet_network::immutable_id(id) && id != parent.parent().1.id,
            "worker requires original guard identity"
        );
        let deployment = &parent.spec().labels[ELIXIR_DEPLOYMENT_ID_LABEL];
        let volume =
            FixedStateVolume::profile(deployment, image, parent.packet.budget().worker_limits())?;
        let guard = ContainerHandle {
            id: id.into(),
            name: parent.spec().name.clone(),
        };
        let fixed = FixedWorkerSpec::guarded(&volume, issuer_keys, &parent.packet, &guard)?;
        Ok(Self {
            intent: Intent {
                image: image.into(),
                issuer_keys: issuer_keys.to_vec(),
                fingerprint: fixed.container_spec().labels[CONTAINER_SPEC_HASH_LABEL].clone(),
            },
            fixed,
            parent,
            guard,
        })
    }
    pub fn spec(&self) -> &ContainerSpec {
        self.fixed.container_spec()
    }
    pub(crate) fn fixed(&self) -> &FixedWorkerSpec {
        &self.fixed
    }
    pub(crate) fn parent(&self) -> (&GuardProfile, &ContainerHandle) {
        (&self.parent, &self.guard)
    }
}
pub struct WorkerCreateAttempt {
    pub(super) operation: uuid::Uuid,
    pub(super) fingerprint: String,
}
impl WorkerCreateAttempt {
    pub(crate) fn require(&self, operation: uuid::Uuid, profile: &Profile) -> Result<()> {
        ensure!(
            !operation.is_nil() && operation == self.operation,
            "worker create operation differs"
        );
        self.require_profile(profile)
    }
    pub(crate) fn require_profile(&self, profile: &Profile) -> Result<()> {
        ensure!(
            self.fingerprint == profile.intent.fingerprint,
            "worker create profile differs"
        );
        Ok(())
    }
}
pub struct WorkerStartAttempt {
    pub(super) fingerprint: String,
    pub(super) id: String,
}
impl WorkerStartAttempt {
    pub(crate) fn require(&self, profile: &Profile, handle: &ContainerHandle) -> Result<()> {
        ensure!(
            self.id == handle.id
                && handle.name == profile.spec().name
                && self.fingerprint == profile.intent.fingerprint,
            "worker start differs from original bound create"
        );
        Ok(())
    }
}
impl Owner {
    /// Called only inside an original retained startup task, after its guard
    /// has checked protected DNS/TLS health and paused public data. No timeout
    /// drops the physical CREATE future or converts its outcome to absence.
    pub(in crate::marketplace) async fn create_worker<T, F, Fut>(
        &mut self,
        image: &str,
        issuer_keys: &[PublicKey],
        cancel: &CancellationToken,
        guard: &mut crate::marketplace::packet_client::PacketClient,
        ready: F,
    ) -> Result<T>
    where
        T: Send,
        F: FnOnce(ContainerHandle, FixedWorkerSpec, PrivateProcessStdio) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T>> + Send,
    {
        self.require_owned().await?;
        let lease = self.lease.clone();
        let runtime = self.runtime.clone();
        let fence = self.fence.as_mut().context("provider fence missing")?;
        let profile = fence.begin_worker(&lease, image, issuer_keys)?;
        let startup = async {
            ensure!(!cancel.is_cancelled(), "provider worker startup cancelled");
            FixedStateVolume::ensure(
                runtime.as_ref(),
                &profile.spec().labels[ELIXIR_DEPLOYMENT_ID_LABEL],
                image,
                profile.fixed().limits(),
            )
            .await?;
            let attempt = fence.creating_worker(&lease)?;
            let handle = match runtime.create_provider_worker(&profile, attempt).await? {
                WorkerCreation::Created(handle) => handle,
                WorkerCreation::NotAttempted(outcome) => {
                    return Err(fence.worker_not_attempted(&lease, outcome)?);
                }
            };
            let start = fence.bind_worker(&lease, &handle)?;
            ensure!(
                !cancel.is_cancelled(),
                "provider worker cancelled before START"
            );
            let process = runtime
                .launch_provider_worker(&handle, &profile, start)
                .await?;
            ready(handle, profile.fixed().clone(), process).await
        };
        tokio::pin!(startup);
        let stop = CancellationToken::new();
        let keepalive = async {
            loop {
                tokio::select! { _ = stop.cancelled() => return Ok::<(), anyhow::Error>(()), _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => () }
                let mut provider = self
                    .client
                    .as_ref()
                    .context("provider client missing")?
                    .lock()
                    .await;
                // Cold storage/Node readiness can exceed either actor's idle
                // watchdog. Keep BOTH original streams alive without granting
                // a data lease or replenishing credit. Join both original
                // private frames even if one fails; do not poison the other by
                // dropping its physical exchange at a select boundary.
                let (interface_present, paused) = tokio::join!(
                    provider.inspect(),
                    guard.request(elixir_marketplace::guard_control::GuardCommand::Pause)
                );
                paused.context("closed worker-startup guard keepalive")?;
                ensure!(
                    interface_present.context("provider worker-startup keepalive unavailable")?,
                    "provider tunnel interface unavailable during worker startup"
                );
            }
        };
        tokio::pin!(keepalive);
        tokio::select! {
            result = &mut startup => { stop.cancel(); keepalive.await?; result },
            failure = &mut keepalive => { cancel.cancel(); let _ = startup.await; Err(failure.err().unwrap_or_else(||anyhow::anyhow!("provider worker keepalive ended"))) }
        }
    }
}
