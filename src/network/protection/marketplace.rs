//! Purpose-approved extension HTTP config, never the active downloader identity.
use super::*;
use crate::network::vpn_identity::{self, Purpose};
const SCOPE: &str = "marketplace_http";
const NETWORK_SCOPE: &str = "marketplace_network";
const MAX_PROFILES: usize = 256;

async fn configured_keys(
    pool: &AnyPool,
    secrets: &SecretsManager,
) -> Result<Vec<(String, Purpose)>> {
    let rows = sqlx::query("SELECT p.id,p.scope,CAST(CASE WHEN p.active THEN 1 ELSE 0 END AS BIGINT) AS active,s.secret_ref FROM download_network_profiles p JOIN download_network_profile_secrets s ON s.profile_id=p.id AND s.key='wireguard_config' WHERE p.kind='wireguard_config' ORDER BY p.id LIMIT 257")
        .fetch_all(pool).await?;
    if rows.len() > MAX_PROFILES {
        bail!("VPN profile allocation exceeded");
    }
    let mut identities = Vec::new();
    for row in rows {
        let scope: String = row.try_get("scope")?;
        let purpose = match scope.as_str() {
            SCOPE => Purpose::MarketplaceHttp,
            NETWORK_SCOPE => Purpose::MarketplaceNetwork,
            _ => Purpose::Downloads,
        };
        let reference: String = row.try_get("secret_ref")?;
        let keys = match resolve_live_egress_secret(pool, secrets, &reference).await {
            Ok(config) => vpn_identity::wireguard_keys(&config),
            Err(error) => Err(error),
        };
        match keys {
            Ok(keys) => identities.extend(keys.into_iter().map(|key| (key, purpose))),
            Err(_)
                if row.try_get::<i64, _>("active")? != 0
                    || matches!(scope.as_str(), SCOPE | NETWORK_SCOPE) =>
            {
                bail!(
                    "existing VPN identity cannot be checked; repair its configuration before assigning a new network purpose"
                );
            }
            Err(_) => {} // Invalid inactive legacy config has no usable key.
        }
    }
    Ok(identities)
}

pub(super) async fn validate_import(
    pool: &AnyPool,
    secrets: &SecretsManager,
    profile_id: &str,
    purpose: VpnProfilePurpose,
    kind: &DownloadNetworkProfileKind,
    config: &str,
) -> Result<()> {
    if let Some(old) = load_download_network_profile(pool, profile_id).await? {
        let old_purpose = match old.scope.as_str() {
            SCOPE => VpnProfilePurpose::MarketplaceHttp,
            NETWORK_SCOPE => VpnProfilePurpose::MarketplaceNetwork,
            _ => VpnProfilePurpose::Downloads,
        };
        if old_purpose != purpose {
            bail!("VPN profile purpose cannot be changed by importing over an existing profile");
        }
    }
    if matches!(
        purpose,
        VpnProfilePurpose::MarketplaceHttp | VpnProfilePurpose::MarketplaceNetwork
    ) {
        let existing: Option<String> =
            sqlx::query_scalar("SELECT id FROM download_network_profiles WHERE scope=$1 LIMIT 1")
                .bind(purpose.scope())
                .fetch_optional(pool)
                .await?;
        if existing.is_some_and(|id| id != profile_id) {
            bail!(
                "update the existing profile for this marketplace purpose instead of adding another"
            );
        }
    }
    if purpose == VpnProfilePurpose::MarketplaceNetwork {
        if *kind != DownloadNetworkProfileKind::WireguardConfig {
            bail!("marketplace network startup requires a dedicated WireGuard profile");
        }
        crate::marketplace::startup::validate_saved_wireguard(config)?;
    }
    if *kind == DownloadNetworkProfileKind::WireguardConfig {
        // Legacy malformed imports may still be saved as blocked downloader
        // drafts. They cannot supply a valid independent application identity.
        let keys = match vpn_identity::wireguard_keys(config) {
            Ok(keys) => keys,
            Err(_) if purpose == VpnProfilePurpose::Downloads => return Ok(()),
            Err(error) => return Err(error),
        };
        let mut identities = configured_keys(pool, secrets).await?;
        let role = match purpose {
            VpnProfilePurpose::MarketplaceHttp => Purpose::MarketplaceHttp,
            VpnProfilePurpose::MarketplaceNetwork => Purpose::MarketplaceNetwork,
            VpnProfilePurpose::Downloads => Purpose::Downloads,
        };
        identities.extend(keys.into_iter().map(|key| (key, role)));
        vpn_identity::reserve(pool, &identities).await?;
    }
    Ok(())
}

/// Read current dedicated network material, never the downloader or installed
/// HTTP selection. Callers retain their own captured fingerprint and lifecycle.
pub(crate) async fn marketplace_network_material(
    pool: &AnyPool,
    secrets: &SecretsManager,
    id: &str,
) -> Result<Zeroizing<String>> {
    if !live_profile_component(id) {
        bail!("invalid marketplace network profile identity");
    }
    let profile = load_download_network_profile(pool, id)
        .await?
        .context("marketplace network profile is not configured")?;
    let active: Option<String> =
        sqlx::query_scalar("SELECT id FROM download_network_profiles WHERE id=$1 AND active=TRUE")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    if profile.scope != NETWORK_SCOPE
        || profile.kind != DownloadNetworkProfileKind::WireguardConfig
        || !profile.enabled
        || !profile.strict
        || active.is_some()
        || !matches!(
            profile.status,
            DownloadProtectionState::Unknown | DownloadProtectionState::Protected
        )
    {
        bail!("dedicated marketplace network profile is unavailable");
    }
    let rows: Vec<String> = sqlx::query_scalar("SELECT secret_ref FROM download_network_profile_secrets WHERE profile_id=$1 AND key='wireguard_config' LIMIT 2")
        .bind(id).fetch_all(pool).await?;
    if rows.len() != 1 {
        bail!("marketplace network material is unavailable or ambiguous");
    }
    let material = resolve_live_egress_secret(pool, secrets, &rows[0]).await?;
    if material.len() > 256 * 1024 || material.contains('\0') {
        bail!("marketplace network material exceeds its bounds");
    }
    Ok(material)
}

pub(crate) async fn reserve_marketplace_network_identity(
    pool: &AnyPool,
    secrets: &SecretsManager,
    public: &str,
) -> Result<()> {
    let mut identities = configured_keys(pool, secrets).await?;
    identities.push((public.to_owned(), Purpose::MarketplaceNetwork));
    vpn_identity::reserve(pool, &identities).await
}

pub(crate) async fn marketplace_http_profile(
    pool: &AnyPool,
    secrets: &SecretsManager,
) -> Result<Option<ActiveLiveEgressProfile>> {
    let ids: Vec<(String,i64)> = sqlx::query_as("SELECT id,CAST(CASE WHEN active THEN 1 ELSE 0 END AS BIGINT) FROM download_network_profiles WHERE scope='marketplace_http' LIMIT 2").fetch_all(pool).await?;
    let Some((id, active)) = ids.first() else {
        return Ok(None);
    };
    if ids.len() != 1 {
        bail!("marketplace HTTP profile is ambiguous");
    }
    let profile = load_download_network_profile(pool, id)
        .await?
        .context("marketplace HTTP profile disappeared")?;
    if !profile.enabled
        || !profile.strict
        || *active != 0
        || !matches!(
            profile.status,
            DownloadProtectionState::Unknown | DownloadProtectionState::Protected
        )
        || !live_profile_component(&profile.id)
        || profile.name.trim().is_empty()
        || profile.name.len() > 128
    {
        return Ok(None);
    }
    let projected = project_egress_profile(pool, secrets, profile).await?;
    if let Some(profile) = &projected
        && profile.kind == DownloadNetworkProfileKind::WireguardConfig
    {
        let config = resolve_live_egress_secret(
            pool,
            secrets,
            profile
                .config_secret_ref
                .as_deref()
                .context("marketplace VPN config missing")?,
        )
        .await?;
        if config.len() > 256 * 1024 || config.contains('\0') {
            bail!("selected protection material exceeds its bounds");
        }
        let keys = vpn_identity::wireguard_keys(&config)?;
        if keys.len() != 1 {
            bail!("marketplace HTTP requires exactly one VPN device identity");
        }
        vpn_identity::current(pool, &keys[0], Purpose::MarketplaceHttp).await?;
        // Import/startup reserve all current legacy keys before admitting a
        // new purpose. Do not decrypt/scan every profile on each HTTP chunk;
        // the immutable registry enforces subsequent supported imports.
    }
    Ok(projected)
}

#[cfg(test)]
mod tests;
