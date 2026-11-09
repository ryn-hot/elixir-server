//! Trusted server-owned catalog provisioning. No peer-controlled paths, image
//! selection, reset operation, or host filesystem fallback is exposed here.
use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use elixir_marketplace::{identity::PublicKey, policy::OrdinaryLimits};

use super::runtime::{image_is_pinned, prepare_ephemeral_worker};
use crate::runtime::{
    RuntimeManager,
    model::{
        ContainerSpec, ContainerTmpfsMount, ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL,
        ELIXIR_INSTANCE_ID_LABEL, ELIXIR_MANAGED_LABEL, FixedMarketplaceVolumeSpec, VolumeMount,
        VolumeMountSourceKind, apply_container_spec_fingerprint,
    },
};

pub const FIXED_FILE_BYTES: u64 = 192 * 1024 * 1024;
pub const FIXED_STATE_RESERVATION_BYTES: u64 = 429 * 1024 * 1024;
// Tor's ordinary consensus/microdescriptor cache filled the old 8 MiB /tmp
// during a protected cold install. Leave bounded room for cache replacement;
// tmpfs pages still count inside the existing worker memory/swap ceiling.
const FIXED_TEMPORARY_MIB: u64 = 32;

#[derive(Debug, Clone)]
pub struct FixedStateVolume {
    spec: FixedMarketplaceVolumeSpec,
    deployment: String,
}

impl FixedStateVolume {
    /// Existing/partial state is inspected, never re-created as empty storage.
    pub async fn ensure(
        runtime: &dyn RuntimeManager,
        deployment: &str,
        image: &str,
        limits: &OrdinaryLimits,
    ) -> Result<Self> {
        let profile = Self::profile(deployment, image, limits)?;
        runtime
            .ensure_fixed_marketplace_volume(&profile.spec)
            .await?;
        Ok(profile)
    }

    // Frozen desired DATA for recovery. This neither creates storage nor
    // authorizes CREATE/START in another actor's network namespace.
    pub(in crate::marketplace) fn profile(
        deployment: &str,
        image: &str,
        limits: &OrdinaryLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !image_is_pinned(image)
            || limits.memory_bytes < 64 * 1024 * 1024
            || limits.memory_bytes % (1024 * 1024) != 0
            || limits.persistent_bytes < FIXED_STATE_RESERVATION_BYTES
        {
            bail!(
                "fixed marketplace storage requires a stable deployment, immutable image and its existing disk reservation"
            );
        }
        let spec = FixedMarketplaceVolumeSpec {
            name: format!("elixir-marketplace-{deployment}"),
            image: image.to_owned(),
            memory_limit_mb: limits.memory_bytes / (1024 * 1024),
            cpu_millicores: limits.cpu_millicores,
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.to_owned()),
                (
                    "elixir.marketplace.volume".to_owned(),
                    "fixed-v1".to_owned(),
                ),
            ]),
        };
        Ok(Self {
            spec,
            deployment: deployment.to_owned(),
        })
    }

    pub fn name(&self) -> &str {
        &self.spec.name
    }
    pub fn labels(&self) -> &HashMap<String, String> {
        &self.spec.labels
    }
    pub fn image(&self) -> &str {
        &self.spec.image
    }
}

/// Immutable desired profile. It cannot be decoded from marketplace metadata.
/// This initial constructor is deliberately offline. Protected networking must
/// later require independent packet/allowance evidence, not a namespace string.
#[derive(Debug, Clone)]
pub struct FixedWorkerSpec {
    pub(super) spec: ContainerSpec,
    pub(super) limits: OrdinaryLimits,
    pub(super) volume: FixedStateVolume,
}

impl FixedWorkerSpec {
    #[cfg(test)]
    pub(in crate::marketplace) fn source_preflight_fixture() -> Result<Self> {
        let limits = super::packet::PacketWorkloadBudget::split(&OrdinaryLimits::default())?
            .worker_limits()
            .clone();
        let volume = FixedStateVolume {
            deployment: "source-fixture".into(),
            spec: FixedMarketplaceVolumeSpec {
                name: "elixir-marketplace-source-fixture".into(),
                image: format!("sha256:{}", "1".repeat(64)),
                memory_limit_mb: limits.memory_bytes / (1024 * 1024),
                cpu_millicores: limits.cpu_millicores,
                labels: HashMap::new(),
            },
        };
        Self::offline(
            &volume,
            &[PublicKey::from_bytes(
                ed25519_dalek::SigningKey::from_bytes(&[3; 32])
                    .verifying_key()
                    .to_bytes(),
            )?],
            &limits,
        )
    }
    pub fn offline(
        volume: &FixedStateVolume,
        issuer_keys: &[PublicKey],
        limits: &OrdinaryLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if !(1..=4).contains(&issuer_keys.len())
            || limits.persistent_bytes < FIXED_STATE_RESERVATION_BYTES
        {
            bail!("invalid fixed worker issuer keys or reserved storage");
        }
        let mut spec = ContainerSpec {
            name: format!("elixir-marketplace-worker-{}", volume.deployment),
            image: volume.spec.image.clone(),
            network: String::new(),
            network_mode: Some("none".to_owned()),
            aliases: vec![],
            env: vec![],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::new(),
            command: issuer_keys
                .iter()
                .flat_map(|key| ["--issuer-key".to_owned(), key.to_string()])
                .collect(),
            cap_add: vec![],
            cap_drop: vec![],
            devices: vec![],
            sysctls: HashMap::new(),
            security: Default::default(),
        };
        prepare_ephemeral_worker(&mut spec, limits)?;
        spec.security.tmpfs.retain(|mount| mount.path == "/tmp");
        spec.security
            .tmpfs
            .iter_mut()
            .find(|mount| mount.path == "/tmp")
            .context("fixed worker temporary filesystem missing")?
            .size_mb = Some(FIXED_TEMPORARY_MIB);
        spec.security.tmpfs.push(ContainerTmpfsMount {
            path: elixir_marketplace::source_preflight::radicle::SCRATCH_ROOT.into(),
            size_mb: Some(elixir_marketplace::source_preflight::radicle::SCRATCH_MIB),
            owner_uid: Some(10005),
            owner_gid: Some(10005),
            mode: Some(0o700),
            no_exec: true,
            no_suid: true,
            no_dev: true,
        });
        spec.security.file_size_limit_bytes = Some(FIXED_FILE_BYTES);
        spec.security.stdin_open = true;
        spec.volumes.push(VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: volume.name().to_owned(),
            container_path: "/state".to_owned(),
            read_only: false,
        });
        spec.command
            .extend(["--state-mode".to_owned(), "durable-files".to_owned()]);
        spec.labels = volume.spec.labels.clone();
        spec.labels.insert(
            "elixir.marketplace.isolation".to_owned(),
            "fixed-v1".to_owned(),
        );
        spec.labels.insert(
            ELIXIR_EXTENSION_ID_LABEL.to_owned(),
            "elixir-marketplace-worker".to_owned(),
        );
        spec.labels.insert(
            ELIXIR_INSTANCE_ID_LABEL.to_owned(),
            format!("marketplace-{}", volume.deployment),
        );
        apply_container_spec_fingerprint(&mut spec);
        Ok(Self {
            spec,
            limits: limits.clone(),
            volume: volume.clone(),
        })
    }

    pub fn container_spec(&self) -> &ContainerSpec {
        &self.spec
    }
    pub(in crate::marketplace) fn limits(&self) -> &OrdinaryLimits {
        &self.limits
    }

    /// Desired protected profile only; traffic stays closed until the owning
    /// lifecycle performs a new private probe and starts continuous supervision.
    pub(super) fn protected(
        volume: &FixedStateVolume,
        issuer_keys: &[PublicKey],
        guard: &super::packet::PacketGuardController,
        monitor: &super::connectivity::ConnectivityMonitor,
        policy: super::connectivity::ProbePolicy,
    ) -> Result<Self> {
        monitor.require_current(guard, policy)?;
        Self::guarded(volume, issuer_keys, guard.profile(), guard.handle())
    }

    /// Frozen DATA only. The provider lifecycle requires its original journal
    /// and one-use native tokens before this profile can be created or started.
    pub(in crate::marketplace) fn guarded(
        volume: &FixedStateVolume,
        issuer_keys: &[PublicKey],
        guard: &super::packet::PacketGuardProfile,
        handle: &crate::runtime::model::ContainerHandle,
    ) -> Result<Self> {
        let deployment = guard
            .container_spec()
            .labels
            .get(ELIXIR_DEPLOYMENT_ID_LABEL)
            .context("missing protected deployment")?;
        if deployment != &volume.deployment
            || !crate::runtime::packet_network::immutable_id(&handle.id)
            || handle.name != guard.container_spec().name
        {
            bail!("protected catalog and packet controller ownership differ");
        }
        let mut profile = Self::offline(volume, issuer_keys, guard.budget().worker_limits())?;
        profile.spec.network_mode = Some(format!("container:{}", handle.id));
        profile
            .spec
            .command
            .extend(["--network-control".to_owned(), "protected".to_owned()]);
        profile.spec.labels.insert(
            "elixir.marketplace.packet.guard".to_owned(),
            handle.id.clone(),
        );
        profile.spec.labels.insert(
            "elixir.marketplace.packet.epoch".to_owned(),
            guard.policy().epoch.to_string(),
        );
        apply_container_spec_fingerprint(&mut profile.spec);
        Ok(profile)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn volume() -> FixedStateVolume {
        FixedStateVolume {
            deployment: "fixture".to_owned(),
            spec: FixedMarketplaceVolumeSpec {
                name: "elixir-marketplace-fixture".to_owned(),
                image: format!("sha256:{}", "1".repeat(64)),
                memory_limit_mb: 256,
                cpu_millicores: 100,
                labels: HashMap::from([
                    (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                    (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), "fixture".to_owned()),
                    (
                        "elixir.marketplace.volume".to_owned(),
                        "fixed-v1".to_owned(),
                    ),
                ]),
            },
        }
    }

    #[test]
    fn fixed_profile_has_one_owned_mount_no_network_and_keeps_private_input_open() -> Result<()> {
        let key = PublicKey::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[3; 32])
                .verifying_key()
                .to_bytes(),
        )?;
        let profile = FixedWorkerSpec::offline(&volume(), &[key], &OrdinaryLimits::default())?;
        let spec = profile.container_spec();
        assert_eq!(spec.network_mode.as_deref(), Some("none"));
        assert_eq!(spec.volumes.len(), 1);
        assert_eq!(spec.volumes[0].host_path, "elixir-marketplace-fixture");
        assert_eq!(spec.volumes[0].container_path, "/state");
        assert!(!spec.volumes[0].read_only);
        assert!(spec.security.stdin_open);
        assert_eq!(spec.security.file_size_limit_bytes, Some(FIXED_FILE_BYTES));
        assert_eq!(spec.security.tmpfs.len(), 2);
        assert_eq!(spec.security.tmpfs[0].path, "/tmp");
        assert_eq!(spec.security.tmpfs[0].size_mb, Some(32));
        assert_eq!(spec.security.tmpfs[0].owner_uid, Some(10004));
        assert_eq!(spec.security.tmpfs[0].owner_gid, Some(10004));
        assert_eq!(spec.security.tmpfs[0].mode, Some(0o700));
        assert!(
            spec.security.tmpfs[0].no_exec
                && spec.security.tmpfs[0].no_suid
                && spec.security.tmpfs[0].no_dev
        );
        let scratch = &spec.security.tmpfs[1];
        assert_eq!(scratch.path, "/radicle-source");
        assert_eq!(scratch.size_mb, Some(32));
        assert_eq!(scratch.owner_uid, Some(10005));
        assert_eq!(scratch.mode, Some(0o700));
        assert!(scratch.no_exec && scratch.no_suid && scratch.no_dev);
        assert_eq!(spec.security.memory_limit_mb, Some(256));
        assert_eq!(spec.security.memory_swap_limit_mb, Some(256));
        assert!(
            spec.command
                .ends_with(&["--state-mode".to_owned(), "durable-files".to_owned()])
        );
        assert!(
            spec.ports.is_empty()
                && spec.env.is_empty()
                && spec.cap_add.is_empty()
                && spec.devices.is_empty()
        );
        assert!(FixedWorkerSpec::offline(&volume(), &[], &OrdinaryLimits::default()).is_err());
        assert!(
            FixedWorkerSpec::offline(
                &volume(),
                &[key],
                &OrdinaryLimits {
                    persistent_bytes: 256 * 1024 * 1024,
                    ..Default::default()
                }
            )
            .is_err()
        );
        Ok(())
    }
}
