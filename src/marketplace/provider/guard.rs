//! Original provider -> guarded namespace handoff. Intent is cleanup DATA;
//! neither the profile, namespace number nor successful boot grants traffic.
use super::{Owner, Pending, Profile as ParentProfile};
use crate::{
    marketplace::{
        packet::{
            GUARD_CPU_MILLICORES, GUARD_MEMORY_BYTES, GUARD_PIDS, PacketGuardController,
            PacketGuardProfile,
        },
        packet_client::PacketClient,
    },
    runtime::{
        model::{
            CONTAINER_SPEC_HASH_LABEL, ContainerHandle, ContainerSpec, PrivateActorInspection,
        },
        packet_network::immutable_id,
        provider_creation::GuardCreation,
    },
};
use anyhow::{Context, Result, ensure};
use elixir_marketplace::{
    guard_control::GuardCommand, identity::Digest, packet_guard::PacketPolicy,
    policy::OrdinaryLimits,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

// Qualification instrumentation only: no socket, wait or environment switch is
// present in a production executable.
#[cfg(all(test, unix))]
pub(in crate::marketplace) mod test_capture;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    image: String,
    parent: String,
    pub(super) namespace: u64,
    policy: PacketPolicy,
    limits: OrdinaryLimits,
    fingerprint: String,
}
impl Intent {
    pub(super) fn profile(&self, parent: &ParentProfile, id: &str) -> Result<Profile> {
        ensure!(self.parent == id, "guard belongs to another provider");
        let profile = Profile::compile(
            parent,
            id,
            self.namespace,
            &self.image,
            self.policy.clone(),
            self.limits.clone(),
        )?;
        ensure!(
            profile.intent == *self,
            "guard intent differs from frozen profile"
        );
        Ok(profile)
    }
}
/// Closed frozen data only. Constructed by the original Owner, not by callers
/// that happen to possess a container ID, serialized record or namespace inode.
pub struct Profile {
    pub(super) packet: PacketGuardProfile,
    pub(super) intent: Intent,
    parent: ParentProfile,
}
impl Profile {
    pub(super) fn compile(
        parent: &ParentProfile,
        id: &str,
        namespace: u64,
        image: &str,
        policy: PacketPolicy,
        limits: OrdinaryLimits,
    ) -> Result<Self> {
        ensure!(
            immutable_id(id)
                && (1..(1u64 << 53)).contains(&namespace)
                && image.strip_prefix("sha256:").is_some_and(immutable_id),
            "exact provider guard identity required"
        );
        let packet = PacketGuardProfile::provider(
            &parent.intent().deployment,
            image,
            policy.clone(),
            &limits,
            id,
            namespace,
        )?;
        let fingerprint = packet.container_spec().labels[CONTAINER_SPEC_HASH_LABEL].clone();
        Ok(Self {
            packet,
            intent: Intent {
                image: image.into(),
                parent: id.into(),
                namespace,
                policy,
                limits,
                fingerprint,
            },
            parent: parent.intent().profile()?,
        })
    }
    pub fn spec(&self) -> &ContainerSpec {
        self.packet.container_spec()
    }
    pub(crate) fn parent(&self) -> (&ParentProfile, ContainerHandle) {
        (
            &self.parent,
            ContainerHandle {
                id: self.intent.parent.clone(),
                name: self.parent.spec().name.clone(),
            },
        )
    }
    pub(crate) fn namespace(&self) -> u64 {
        self.intent.namespace
    }
    pub(crate) fn verify(
        &self,
        handle: &ContainerHandle,
        actual: &PrivateActorInspection,
    ) -> Result<()> {
        self.packet.verify(handle, actual, false)
    }
}
pub struct GuardCreateAttempt {
    pub(super) operation: uuid::Uuid,
    pub(super) fingerprint: String,
}
impl GuardCreateAttempt {
    pub(crate) fn require(&self, operation: uuid::Uuid, profile: &Profile) -> Result<()> {
        ensure!(
            !operation.is_nil() && self.operation == operation,
            "guard create operation differs"
        );
        self.require_profile(profile)
    }
    pub(crate) fn require_profile(&self, profile: &Profile) -> Result<()> {
        ensure!(
            self.fingerprint == profile.intent.fingerprint,
            "guard create profile differs"
        );
        Ok(())
    }
}
pub struct GuardStartAttempt {
    pub(super) id: String,
    pub(super) fingerprint: String,
}
impl GuardStartAttempt {
    pub(crate) fn require(&self, profile: &Profile, handle: &ContainerHandle) -> Result<()> {
        ensure!(
            self.id == handle.id
                && self.fingerprint == profile.intent.fingerprint
                && handle.name == profile.spec().name,
            "guard start differs from original bound create"
        );
        Ok(())
    }
}
impl Owner {
    pub(in crate::marketplace) fn require_guard_binding(
        &self,
        lease: &std::sync::Arc<crate::marketplace::lease::ControllerLease>,
        handle: &ContainerHandle,
        profile: &PacketGuardProfile,
        namespace: u64,
    ) -> Result<()> {
        ensure!(
            std::sync::Arc::ptr_eq(lease, &self.lease)
                && profile.container_spec().network_mode.as_deref()
                    == Some(format!("container:{}", self.handle.id).as_str()),
            "guard handoff lost its original provider/OS lease"
        );
        self.fence
            .as_ref()
            .context("provider native fence missing")?
            .require_guard_binding(
                &self.lease,
                handle,
                &profile.container_spec().labels[CONTAINER_SPEC_HASH_LABEL],
                namespace,
            )
    }
    pub(in crate::marketplace) fn guard_namespace(&self) -> Result<u64> {
        self.fence
            .as_ref()
            .context("provider fence missing")?
            .guard_namespace(&self.lease)
    }
    /// Consume the actual provider owner. Cancellation retains the original
    /// boot task and native outcomes; no timeout can manufacture NotAttempted.
    pub(in crate::marketplace) fn guard(
        self,
        image: String,
        mut policy: PacketPolicy,
        limits: OrdinaryLimits,
        debit: crate::marketplace::accounting::CommittedTransferReservation,
    ) -> Pending<PacketGuardController> {
        let cancel = CancellationToken::new();
        let request = cancel.clone();
        let task = tokio::spawn(async move {
            let mut owner = self;
            owner.require_owned().await?;
            ensure!(
                debit.bytes() == super::TRANSPORT_BOOTSTRAP_BYTES,
                "provider transport startup requires its fixed committed debit"
            );
            // New starts need the current rate policy. Legacy pinned guards
            // remain inspectable/stoppable by the runtime during retirement.
            let labels = owner.runtime.image_labels(&image).await?;
            ensure!(
                crate::marketplace::packet::packet_image_protocol_supported(&labels)
                    && labels
                        .get("elixir.marketplace.guard.provider_transport_budget")
                        .map(String::as_str)
                        == Some("1"),
                "guard image lacks current prepaid provider transport startup"
            );
            let credit = debit.into_packet_credit()?;
            let transport = owner.transport().await?;
            #[cfg(all(test, unix))]
            test_capture::begin(&owner, &transport).await?;
            let namespace = transport.namespace_inode;
            // A caller/SDK cannot select provider exceptions or reuse example
            // endpoints. Freeze only the ORIGINAL service's current UDP tuple.
            policy.endpoints = vec![transport.endpoint];
            policy.tunnel_addresses = transport.tunnel_addresses;
            policy.epoch = Digest::of(uuid::Uuid::new_v4().as_bytes());
            let profile = Profile::compile(
                &owner.profile,
                &owner.handle.id,
                namespace,
                &image,
                policy,
                limits,
            )?;
            let lease = owner.lease.clone();
            let runtime = owner.runtime.clone();
            let mut fence = owner.fence.take().context("provider fence missing")?;
            fence.begin_guard(&lease, &profile)?;
            let mut cancellation = None;
            let result = {
                let startup = async {
                    ensure!(!request.is_cancelled(), "guard startup cancelled");
                    let attempt = fence.creating_guard(&lease)?;
                    let handle = match runtime.create_provider_guard(&profile, attempt).await? {
                        GuardCreation::Created(handle) => handle,
                        GuardCreation::NotAttempted(outcome) => {
                            return Err(fence.guard_not_attempted(&lease, outcome)?);
                        }
                    };
                    let start = fence.bind_guard(&lease, &handle)?;
                    ensure!(
                        !request.is_cancelled(),
                        "guard startup cancelled before START"
                    );
                    let stream = runtime
                        .launch_provider_guard(&handle, &profile, start)
                        .await?;
                    let mut client = PacketClient::new_joined(
                        stream,
                        profile.packet.policy().epoch,
                        GUARD_MEMORY_BYTES,
                        GUARD_CPU_MILLICORES,
                        GUARD_PIDS,
                    )
                    .await?;
                    let ready = async {
                        client
                            .request(GuardCommand::InitializeProvider {
                                policy: profile.packet.policy().clone(),
                                credit,
                            })
                            .await?;
                        ensure!(
                            client.namespace_inode() == Some(namespace),
                            "guard joined another kernel namespace"
                        );
                        runtime.inspect_provider_guard(&handle, &profile).await?;
                        #[cfg(all(test, unix))]
                        test_capture::guard_ready(
                            &owner.profile.intent().deployment,
                            &owner.handle.id,
                            &handle.id,
                            namespace,
                            owner.profile.intent().resume,
                        )
                        .await?;
                        Ok::<_, anyhow::Error>(())
                    }
                    .await;
                    if let Err(error) = ready {
                        client.close_joined().await?;
                        return Err(error);
                    }
                    Ok::<_, anyhow::Error>((handle, client))
                };
                tokio::pin!(startup);
                let stop_pulse = CancellationToken::new();
                let keepalive = async {
                    loop {
                        tokio::select! {
                            _ = stop_pulse.cancelled() => return Ok::<(),anyhow::Error>(()),
                            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => (),
                        }
                        // Poll concurrently: a slow provider reply must not
                        // starve the guard's original private frame/start task.
                        owner
                            .client
                            .as_ref()
                            .context("provider client missing")?
                            .lock()
                            .await
                            .inspect()
                            .await?;
                    }
                };
                tokio::pin!(keepalive);
                tokio::select! {
                    result = &mut startup => {
                        stop_pulse.cancel();
                        // Join any original in-flight request before handing
                        // its stream over; cancellation must not poison it.
                        if let Err(error) = keepalive.await { cancellation = Some(error); }
                        result
                    },
                    failed = &mut keepalive => {
                        cancellation = Some(failed.err().unwrap_or_else(||anyhow::anyhow!("provider keepalive ended")));
                        request.cancel();
                        // The ORIGINAL physical task is still joined. Failure
                        // cannot synthesize a not-attempted CREATE outcome.
                        startup.await
                    }
                }
            };
            owner.fence = Some(fence);
            match result {
                Ok((handle, client)) if !request.is_cancelled() && cancellation.is_none() => {
                    owner.require_owned().await?;
                    PacketGuardController::from_provider(
                        handle,
                        profile.packet,
                        client,
                        lease,
                        owner,
                    )
                }
                Ok((_, client)) => {
                    client.close_joined().await?;
                    owner.close(false).await?;
                    Err(cancellation.unwrap_or_else(|| anyhow::anyhow!("guard startup cancelled")))
                }
                Err(error) => {
                    owner
                        .close(false)
                        .await
                        .context("guard startup outcome remains fenced")?;
                    Err(error)
                }
            }
        });
        Pending { task, cancel }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    pub(in crate::marketplace::provider) fn policy() -> PacketPolicy {
        PacketPolicy {
            schema_version: 1,
            epoch: Digest::of(uuid::Uuid::new_v4().as_bytes()),
            tunnel_interface: "CloudflareWARP".into(),
            tunnel_addresses: vec!["172.16.0.2".parse().unwrap()],
            endpoints: vec![elixir_marketplace::packet_guard::VpnEndpoint {
                address: "1.1.1.1".parse().unwrap(),
                port: 443,
                local_port: 44444,
            }],
            lease_seconds: 3,
            upload_bytes_per_second: 32768,
            download_bytes_per_second: 131072,
        }
    }
    #[test]
    fn provider_guard_is_bound_to_exact_parent_namespace_policy_and_existing_budget() -> Result<()>
    {
        let parent = ParentProfile::fixture(
            "guard-fixture",
            &format!("sha256:{}", "a".repeat(64)),
            &"b".repeat(64),
        )?;
        let p = Profile::compile(
            &parent,
            &"c".repeat(64),
            12345,
            &format!("sha256:{}", "d".repeat(64)),
            policy(),
            OrdinaryLimits::default(),
        )?;
        let restored = p.intent.profile(&parent, &"c".repeat(64))?;
        assert_eq!(
            restored.spec().network_mode,
            Some(format!("container:{}", "c".repeat(64)))
        );
        assert_eq!(
            p.packet.budget().worker_limits().memory_bytes + GUARD_MEMORY_BYTES,
            OrdinaryLimits::default().memory_bytes
        );
        assert!(p.intent.profile(&parent, &"e".repeat(64)).is_err());
        let mut changed = p.intent.clone();
        changed.namespace += 1;
        // Namespace is DATA but still part of the exact frozen fingerprint.
        assert!(changed.profile(&parent, &"c".repeat(64)).is_err());
        changed.policy.lease_seconds = 0;
        assert!(changed.profile(&parent, &"c".repeat(64)).is_err());
        let (handle, mut native) = crate::marketplace::packet::tests::inspection(&p.packet);
        native.runtime.network_mode = p.spec().network_mode.clone();
        native.networks.clear();
        p.verify(&handle, &native)?;
        native.networks.push("bridge".into());
        assert!(p.verify(&handle, &native).is_err());
        native.networks.clear();
        native.runtime.network_mode = Some(format!("container:{}", "e".repeat(64)));
        assert!(p.verify(&handle, &native).is_err());
        Ok(())
    }
}
