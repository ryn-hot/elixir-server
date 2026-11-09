//! Bounded local identity-purpose bindings. Not provider registration, proof of
//! a live VPN, or permission to run simultaneous gateways with the same key.
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use elixir_marketplace::{identity::Digest, wireguard_setup::WireguardSecrets};
use sqlx::AnyPool;
use std::collections::BTreeSet;
use zeroize::Zeroizing;

const MAX_IDENTITIES: i64 = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose {
    Downloads,
    MarketplaceNetwork,
    MarketplaceHttp,
}
impl Purpose {
    fn name(self) -> &'static str {
        match self {
            Self::Downloads => "downloads",
            Self::MarketplaceNetwork => "marketplace_network",
            Self::MarketplaceHttp => "marketplace_http",
        }
    }
}
fn id(public: &str) -> Result<String> {
    let bytes = elixir_marketplace::wireguard_setup::public_key(public)?;
    Ok(Digest::of(bytes).to_string())
}

/// Canonical PUBLIC identities, including clamped-private-key aliases. No
/// private key, config, endpoint, profile name or account enters the registry.
/// Collect all valid interface keys in a legacy config; never guess which a
/// lenient legacy parser might choose. Full app normalization is independent.
pub(crate) fn wireguard_keys(config: &str) -> Result<Vec<String>> {
    if config.len() > 256 * 1024 || config.contains('\0') {
        bail!("VPN configuration exceeds its bounds");
    }
    let mut interface = false;
    let mut keys = BTreeSet::new();
    for line in config.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.starts_with('[') {
            interface = line.eq_ignore_ascii_case("[Interface]");
        } else if interface && let Some((name, value)) = line.split_once('=') {
            if !name.trim().eq_ignore_ascii_case("PrivateKey") {
                continue;
            }
            let Ok(bytes) = STANDARD.decode(value.trim()) else {
                continue;
            };
            let bytes = Zeroizing::new(bytes);
            if bytes.len() != 32 {
                continue;
            }
            let mut private = Zeroizing::new([0; 32]);
            private.copy_from_slice(&bytes);
            if let Ok(key) = WireguardSecrets::from_keys(private, None) {
                keys.insert(key.device_public_key()?);
            }
            if keys.len() > 8 {
                bail!("too many VPN identities in one configuration");
            }
        }
    }
    if keys.is_empty() {
        bail!("VPN configuration has no verifiable device identity");
    }
    Ok(keys.into_iter().collect())
}

/// Serialize cross-purpose imports/startup even across server processes. A
/// cancelled waiter may leave a committed reservation, never permission to
/// repurpose a live/old identity. Exact retries are idempotent.
pub(crate) async fn reserve(pool: &AnyPool, identities: &[(String, Purpose)]) -> Result<()> {
    if identities.len() > MAX_IDENTITIES as usize {
        bail!("VPN identity import exceeds its fixed local allocation");
    }
    let mut tx = pool.begin().await?;
    if sqlx::query("UPDATE network_vpn_identity_lock SET singleton=singleton WHERE singleton=1")
        .execute(&mut *tx)
        .await?
        .rows_affected()
        != 1
    {
        bail!("VPN identity registry is unavailable; no implicit reset");
    }
    let mut count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM network_vpn_identity_purposes")
        .fetch_one(&mut *tx)
        .await?;
    if count > MAX_IDENTITIES {
        bail!("VPN identity allocation exceeded");
    }
    for (public, purpose) in identities {
        let identity = id(public)?;
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT purpose FROM network_vpn_identity_purposes WHERE identity_sha256=$1",
        )
        .bind(&identity)
        .fetch_optional(&mut *tx)
        .await?;
        match existing {
            Some(existing) if existing == purpose.name() => {}
            Some(_) => bail!(
                "VPN device identity is already assigned to another network purpose; supply independent provider credentials"
            ),
            None => {
                if count >= MAX_IDENTITIES {
                    bail!("VPN identity allocation is full; existing bindings remain intact");
                }
                sqlx::query("INSERT INTO network_vpn_identity_purposes(identity_sha256,purpose) VALUES($1,$2)")
                    .bind(identity).bind(purpose.name()).execute(&mut *tx).await?;
                count += 1;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn current(pool: &AnyPool, public: &str, purpose: Purpose) -> Result<()> {
    let existing: String = sqlx::query_scalar(
        "SELECT purpose FROM network_vpn_identity_purposes WHERE identity_sha256=$1",
    )
    .bind(id(public)?)
    .fetch_optional(pool)
    .await?
    .context("VPN identity purpose is not registered")?;
    if existing != purpose.name() {
        bail!("VPN identity purpose changed");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
