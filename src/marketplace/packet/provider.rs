//! Provider-owned DNS/TLS composition, never fabricated WireGuard evidence.
//! Bootstrap uses one prepaid FETCH_UID operation inside the original parent;
//! no separate container can retain an unjournaled namespace reference.
use super::*;
use crate::marketplace::connectivity::{BOOTSTRAP_PROBE_BYTES, MAX_OBSERVATION_AGE};
use anyhow::ensure;
use elixir_marketplace::connectivity::ProbeFamily;

/// Constructible only after actual protected DNS/TLS and fresh owned transport
/// + kernel checks. Not Clone/Deserialize; not public-worker/admission authority.
pub(in crate::marketplace) struct ProviderConnectivity {
    guard: String,
    epoch: Digest,
    namespace: u64,
    nonce: Digest,
    started: ObservationTime,
    family: ProbeFamily,
}
impl ProviderConnectivity {
    pub(in crate::marketplace) fn is_recent_for(&self, guard: &PacketGuardController) -> bool {
        self.guard == guard.handle.id
            && self.epoch == guard.profile.policy.epoch
            && self.namespace == guard.namespace_inode()
            && guard.provider_connectivity_nonce == Some(self.nonce)
            && guard.provider.is_some()
            && guard.channel_usable()
            && self.started.is_current(MAX_OBSERVATION_AGE)
    }
    pub(in crate::marketplace) fn family(&self) -> ProbeFamily {
        self.family
    }
}
impl PacketGuardController {
    pub(in crate::marketplace) fn provider_lease(
        &self,
        directory: &std::path::Path,
    ) -> Result<Arc<ControllerLease>> {
        ensure!(
            self.provider.is_some() && self._lease.matches_directory(directory),
            "provider worker lost original controller directory"
        );
        Ok(self._lease.clone())
    }
    pub(in crate::marketplace) async fn start_provider_worker<T, F, Fut>(
        &mut self,
        runtime: &dyn RuntimeManager,
        image: &str,
        issuer_keys: &[elixir_marketplace::identity::PublicKey],
        health: &ProviderConnectivity,
        cancel: &tokio_util::sync::CancellationToken,
        ready: F,
    ) -> Result<T>
    where
        T: Send,
        F: FnOnce(
                ContainerHandle,
                crate::marketplace::storage::FixedWorkerSpec,
                crate::runtime::PrivateProcessStdio,
            ) -> Fut
            + Send,
        Fut: std::future::Future<Output = Result<T>> + Send,
    {
        ensure!(
            health.is_recent_for(self),
            "worker creation requires current provider DNS/TLS health"
        );
        self.pause(runtime).await?;
        self.provider
            .as_mut()
            .context("provider owner missing")?
            .create_worker(image, issuer_keys, cancel, &mut self.client, ready)
            .await
    }
    pub(in crate::marketplace) async fn accept_provider_worker_connectivity(
        &mut self,
        runtime: &dyn RuntimeManager,
        sample: crate::marketplace::connectivity::ConnectivityObservation,
    ) -> Result<ProviderConnectivity> {
        self.provider_connectivity_nonce = None;
        ensure!(
            sample.is_recent_for(self)
                && sample.report().warp == elixir_marketplace::connectivity::WarpState::On,
            "provider worker DNS/TLS evidence differs"
        );
        self.check_provider_transport(runtime).await?;
        ensure!(
            sample.is_recent_for(self),
            "provider worker health expired during inspection"
        );
        self.client.request(GuardCommand::Heartbeat).await?;
        let nonce = Digest::of(uuid::Uuid::new_v4().as_bytes());
        self.provider_connectivity_nonce = Some(nonce);
        Ok(ProviderConnectivity {
            guard: self.handle.id.clone(),
            epoch: self.profile.policy.epoch,
            namespace: self.namespace_inode(),
            nonce,
            started: sample.started_at(),
            family: sample.report().family,
        })
    }
    async fn check_provider_transport(&mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        let transport = self
            .provider
            .as_ref()
            .context("provider owner missing")?
            .transport()
            .await?;
        transport.require_policy(self.namespace_inode(), &self.profile.policy)?;
        // Independent native TUN kind, link, routes, addresses and UID/protocol
        // checks. An SDK, saved socket tuple or interface name cannot supply it.
        self.observe_provider_tun(runtime).await?;
        Ok(())
    }
    /// Initial health closes its lease on completion. This does not create a
    /// public worker. A later supervisor must renew with fresh provider/kernel
    /// checks and enforce adoption/admission, resource and transfer policy.
    pub(in crate::marketplace) async fn bootstrap_provider_connectivity(
        &mut self,
        runtime: &dyn RuntimeManager,
        debit: CommittedTransferReservation,
        family: ProbeFamily,
    ) -> Result<ProviderConnectivity> {
        self.provider_connectivity_nonce = None;
        self.network_observation_sequence = None;
        let result = async {
            self.pause(runtime).await?;
            ensure!(
                debit.bytes() == BOOTSTRAP_PROBE_BYTES,
                "provider bootstrap requires its fixed debit"
            );
            let deployment = self
                .profile
                .spec
                .labels
                .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                .context("provider deployment missing")?;
            ensure!(
                runtime
                    .get_container_handle(&format!("elixir-marketplace-worker-{deployment}"))
                    .await?
                    .is_none(),
                "initial provider health cannot run alongside a marketplace worker"
            );
            ensure!(
                self.profile
                    .policy
                    .tunnel_addresses
                    .iter()
                    .any(|a| a.is_ipv6() == (family == ProbeFamily::Ipv6)),
                "provider lacks requested tunnel family"
            );
            self.check_provider_transport(runtime).await?;
            self.apply_credit(runtime, debit).await?;
            self.renew_bootstrap_lease(runtime).await?;
            let started = ObservationTime::now();
            let probed = {
                let provider = self.provider.as_ref().context("provider owner missing")?;
                let probe = provider.probe_exit(family == ProbeFamily::Ipv6);
                tokio::pin!(probe);
                loop {
                    tokio::select! {
                        outcome = &mut probe => break outcome,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {
                            // No public worker exists; this is a fixed, prepaid
                            // <=8s bootstrap operation. Renew the ORIGINAL guard
                            // channel without contending for the provider IPC
                            // mutex held by the concurrently polled probe.
                            // Always join an original request before selecting
                            // again: do not cancel an in-flight private frame.
                            if let Err(error) = self.client.request(GuardCommand::Heartbeat).await {
                                let _ = probe.await;
                                break Err(error);
                            }
                        }
                    }
                }
            };
            // Always close before propagating a DNS/TLS error. Cancellation
            // cannot preserve a nonce; native <=15s lease +8s curl timeout bound
            // abandonment independently of this future.
            let paused = self.pause(runtime).await;
            probed?;
            paused?;
            self.check_provider_transport(runtime).await?;
            let nonce = Digest::of(uuid::Uuid::new_v4().as_bytes());
            self.provider_connectivity_nonce = Some(nonce);
            Ok(ProviderConnectivity {
                guard: self.handle.id.clone(),
                epoch: self.profile.policy.epoch,
                namespace: self.namespace_inode(),
                nonce,
                started,
                family,
            })
        }
        .await;
        if result.is_err() {
            self.provider_connectivity_nonce = None;
            let _ = self.client.request(GuardCommand::Pause).await;
        }
        result
    }
    /// No raw TUN snapshot/telemetry may renew a provider lease. Preserve the
    /// original successful DNS/TLS binding and re-observe actual transport and
    /// kernel state. A failed/cancelled attempt invalidates previous health.
    pub(in crate::marketplace) async fn renew_provider_lease(
        &mut self,
        runtime: &dyn RuntimeManager,
        health: &ProviderConnectivity,
    ) -> Result<()> {
        let valid = health.is_recent_for(self);
        self.provider_connectivity_nonce = None;
        let result = async {
            ensure!(
                valid,
                "provider lease requires current protected DNS/TLS evidence"
            );
            self.check_provider_transport(runtime).await?;
            ensure!(
                health.started.is_current(MAX_OBSERVATION_AGE),
                "protected provider connectivity observation expired"
            );
            self.client.request(GuardCommand::Heartbeat).await?;
            self.provider_connectivity_nonce = Some(health.nonce);
            Ok(())
        }
        .await;
        if result.is_err() {
            self.network_observation_sequence = None;
            let _ = self.client.request(GuardCommand::Pause).await;
        }
        result
    }
}
