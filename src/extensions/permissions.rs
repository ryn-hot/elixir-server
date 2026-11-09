use anyhow::{Result, bail};

use crate::db::models::ExtensionTrustLevel;

/// Public IDs require the verified pipeline, not local-registry trust labels.
pub(crate) fn require_legacy_extension_id(extension_id: &str) -> Result<()> {
    if extension_id.starts_with("elx:") {
        bail!("public marketplace extensions require the verified marketplace pipeline");
    }
    Ok(())
}

pub(crate) fn public_permission_name(permission: &str) -> bool {
    matches!(
        permission,
        "runtime.manage_containers"
            | "network.egress"
            | "live.catalog.read"
            | "live.stream.resolve"
            | "live.artwork.fetch"
            | "live.network.private"
            | "drivers.configure.indexer.registry"
            | "drivers.configure.media.manager.tv"
            | "drivers.configure.media.manager.movies"
            | "drivers.configure.downloader.torrent"
            | "drivers.configure.downloader.nzb"
    )
}

#[derive(Debug, Clone, Default)]
pub struct PermissionPolicy;

impl PermissionPolicy {
    pub fn new() -> Self {
        Self
    }

    pub fn enforce(
        &self,
        trust: ExtensionTrustLevel,
        permissions: &[String],
        extension_id: &str,
    ) -> Result<()> {
        require_legacy_extension_id(extension_id)?;
        if trust == ExtensionTrustLevel::Untrusted {
            bail!("untrusted extensions are not allowed ('{extension_id}')");
        }

        for permission in permissions {
            if permission.trim().is_empty() {
                bail!("extension '{extension_id}' declares an empty permission");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_trust_cannot_bypass_the_public_pipeline_and_legacy_policy_stays_compatible() {
        for trust in [
            ExtensionTrustLevel::Verified,
            ExtensionTrustLevel::Community,
            ExtensionTrustLevel::Untrusted,
        ] {
            assert!(
                PermissionPolicy
                    .enforce(trust, &[], "elx:publisher/source")
                    .is_err()
            );
        }
        assert!(
            PermissionPolicy
                .enforce(
                    ExtensionTrustLevel::Community,
                    &["legacy.permission".into()],
                    "elixir.sources.example"
                )
                .is_ok()
        );
        assert!(
            PermissionPolicy
                .enforce(
                    ExtensionTrustLevel::Untrusted,
                    &[],
                    "elixir.sources.example"
                )
                .is_err()
        );
    }
}
