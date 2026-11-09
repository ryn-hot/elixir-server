//! Explicit local-endpoint declarations. These are reviewable restrictions,
//! not user consent or runtime network capabilities. No URL/default/DNS or
//! credentials supplied by a publisher can enable local traffic by itself.
use super::*;

const MAX_LAN_ENDPOINTS: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestControlLanEndpoint {
    /// ID of a separate, owner-controlled, default-off instance toggle.
    pub enabled_by: String,
    pub access: ManifestLanEndpointAccess,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestLanEndpointAccess {
    ExactUrl,
    Origin,
}

pub(super) fn validate(surface: &ManifestControlSurface, permissions: &[String]) -> Result<()> {
    let declared: Vec<_> = surface
        .owned_settings
        .iter()
        .filter(|field| field.lan_endpoint.is_some())
        .collect();
    if declared.is_empty() {
        return Ok(());
    }
    if declared.len() > MAX_LAN_ENDPOINTS
        || !permissions.iter().any(|p| p == "network.egress")
        || !permissions
            .iter()
            .any(|p| p == LIVE_NETWORK_PRIVATE_PERMISSION)
    {
        bail!(
            "local endpoints require network.egress and live.network.private, at most eight endpoints"
        );
    }
    for field in declared {
        let endpoint = field.lan_endpoint.as_ref().expect("selected endpoint");
        if !matches!(field.field_type.as_str(), "text" | "password")
            || field.ownership_mode() != "managed"
            || !matches!(
                field.storage.r#type.as_str(),
                "instance_setting" | "instance_secret"
            )
            || field.default.is_some()
            || endpoint.enabled_by.is_empty()
            || endpoint.enabled_by.len() > 128
            || endpoint.enabled_by == field.id
        {
            bail!(
                "local endpoints must be managed instance text/password fields with no publisher default and a separate enable toggle"
            );
        }
        let enable = surface
            .owned_settings
            .iter()
            .find(|setting| setting.id == endpoint.enabled_by)
            .context("local endpoint enable toggle is undeclared")?;
        if enable.field_type != "toggle"
            || enable.storage.r#type != "instance_setting"
            || enable.secret
            || enable.default != Some(serde_json::Value::Bool(false))
            || enable.lan_endpoint.is_some()
        {
            bail!("local endpoint enable control must be a separate default-false instance toggle");
        }
        // An alias must not turn an owner URL into its own enabling flag, or
        // let another declaration/default overwrite either value indirectly.
        for protected in [field, enable] {
            if surface.owned_settings.iter().any(|other| {
                other.id != protected.id
                    && other.storage.r#type == protected.storage.r#type
                    && other.storage.key == protected.storage.key
            }) {
                bail!("local endpoint controls cannot share their storage keys with other fields");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
