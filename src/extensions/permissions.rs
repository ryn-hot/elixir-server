use anyhow::{Result, bail};

use crate::db::models::ExtensionTrustLevel;

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
