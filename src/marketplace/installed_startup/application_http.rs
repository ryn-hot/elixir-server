//! Optional operator wiring for separately protected installed HTTP. This only
//! provisions one stable, owned bridge; it supplies no credentials, packet
//! credit, native source/grant authority or proof that a VPN works.
use crate::{
    marketplace::application_egress::Configuration,
    runtime::{RuntimeManager, packet_network::PacketNetworkSpec},
};
use anyhow::{Context, Result, ensure};
use elixir_marketplace::{connectivity::ProbeFamily, identity::Digest};
use serde::Deserialize;
use std::{path::Path, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Profile {
    family: ProbeFamily,
}
impl Profile {
    pub(super) fn unavailable(&self) -> Configuration {
        Configuration::unavailable(self.family)
    }
    fn network(&self, directory: &Path) -> Result<PacketNetworkSpec> {
        ensure!(
            directory.is_absolute(),
            "installed application root must be absolute"
        );
        // Stable across process restarts and invocations. Never create a new
        // Docker network for each request or accept an operator-selected CID.
        let mut bytes = b"elixir/installed-application-underlay/v1\0".to_vec();
        bytes.extend_from_slice(directory.as_os_str().as_encoded_bytes());
        let digest = Digest::of(bytes).to_string();
        let deployment = format!(
            "installed-http-{}",
            &digest.trim_start_matches("sha256:")[..32]
        );
        PacketNetworkSpec::internet(&deployment, matches!(self.family, ProbeFamily::Ipv6))
    }
    pub(super) async fn configure(
        &self,
        runtime: &dyn RuntimeManager,
        directory: &Path,
    ) -> Result<Configuration> {
        let spec = self.network(directory)?;
        let actual =
            tokio::time::timeout(Duration::from_secs(15), runtime.ensure_packet_uplink(&spec))
                .await
                .context("installed HTTP underlay provisioning deadline")??;
        spec.verify(&actual)?;
        // A bridge is not a protected namespace. Each invocation still creates
        // its exact owned VPN namespace, checks its current selected config,
        // actual kernel guard and DNS/TLS, then launches its isolated HTTP child.
        Configuration::new(actual.id, self.family)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_underlay_is_stable_scoped_and_not_a_readiness_or_cid_input() -> Result<()> {
        let ipv4: Profile = serde_json::from_str(r#"{"family":"ipv4"}"#)?;
        let ipv6: Profile = serde_json::from_str(r#"{"family":"ipv6"}"#)?;
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let first = ipv4.network(&root)?;
        assert_eq!(first.name(), ipv4.network(&root)?.name());
        assert_ne!(
            first.name(),
            ipv4.network(&root.join("another-owner"))?.name()
        );
        assert!(!first.internal() && !first.ipv6());
        assert!(ipv6.network(&root)?.ipv6());
        assert!(ipv4.unavailable().network().is_none());
        assert!(!ipv4.unavailable().has_network());
        assert_eq!(
            first.options()["com.docker.network.bridge.enable_icc"],
            "false"
        );
        assert!(ipv4.network(Path::new("relative")).is_err());
        for invalid in [
            r#"{}"#,
            r#"{"family":"direct"}"#,
            r#"{"family":"ipv4","network_id":"host"}"#,
            r#"{"family":"ipv4","protected":true}"#,
            r#"{"family":"ipv4","image":"publisher-selected"}"#,
        ] {
            assert!(serde_json::from_str::<Profile>(invalid).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "explicit disconnected Docker bridge provisioning/reuse only; no VPN, credentials, HTTP, source invocation or production activation"]
    async fn native_operator_underlay_reuses_one_owned_network_across_startup_attempts()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?;
        let profile: Profile = serde_json::from_str(r#"{"family":"ipv4"}"#)?;
        let runtime = crate::runtime::docker::DockerRuntimeManager::new(None);
        let spec = profile.network(&directory)?;
        let original = runtime.ensure_packet_uplink(&spec).await?;
        let result = async {
            let first = profile.configure(&runtime, &directory).await?;
            let second = profile.configure(&runtime, &directory).await?;
            assert_eq!(
                first.network().map(|(id, _)| id),
                Some(original.id.as_str())
            );
            assert_eq!(
                second.network().map(|(id, _)| id),
                Some(original.id.as_str())
            );
            assert!(
                runtime
                    .inspect_packet_uplink(&original.id)
                    .await?
                    .context("original test underlay missing")?
                    .members
                    .is_empty()
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        // Remove only the exact label-owned, empty fixture bridge. A failed
        // cleanup must remain an error; do not recover by name replacement.
        if let Err(error) = runtime.remove_packet_uplink(&spec, &original.id).await {
            let retained = root.keep();
            return Err(error).with_context(|| {
                format!(
                    "fixture underlay {} cleanup unconfirmed; retained {}",
                    original.id,
                    retained.display()
                )
            });
        }
        assert!(runtime.inspect_packet_uplink(&original.id).await?.is_none());
        result
    }
}
