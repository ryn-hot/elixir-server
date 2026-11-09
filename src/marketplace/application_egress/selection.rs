//! Capture/recheck the actual accepted operator VPN configuration and material.
//! No Deserialize/Clone/Debug; a row, fingerprint or image ID is NOT a live
//! protected path. A later gateway owner must retain its own OS/runtime lease,
//! kill-switch/readiness, aggregate admission and original installed authority.
use crate::{
    db::Database,
    live::{
        config::LiveEgressProfileKind,
        egress::{PreparedMaterialFile, prepare_gateway_material_from_secret_values},
    },
    network::protection::{
        ActiveLiveEgressProfile, DownloadNetworkProfileKind, cloudflare_warp_disclosure,
        marketplace_http_profile, profile_kind_as_str, resolve_live_egress_secret,
    },
    runtime::{RuntimeManager, packet_network::immutable_id},
    secrets::SecretsManager,
};
use anyhow::{Context, Result, bail};
use sqlx::Row;
use std::time::Duration;
use tokio::time::timeout;
use zeroize::Zeroizing;

const MATERIAL_LIMIT: usize = 256 * 1024;
const AUTH_LIMIT: usize = 4096;
const PREFLIGHT: Duration = Duration::from_secs(10);
const DOMAIN: &[u8] = b"elixir/application-egress/selected-configuration/v1";

pub(super) struct Selection {
    profile: ActiveLiveEgressProfile,
    image_id: String,
    fingerprint: [u8; 32],
}
struct Snapshot {
    profile: ActiveLiveEgressProfile,
    values: Values,
    fingerprint: [u8; 32],
}
#[derive(Default)]
struct Values {
    config: Option<Zeroizing<String>>,
    username: Option<Zeroizing<String>>,
    password: Option<Zeroizing<String>>,
    identity: Option<Zeroizing<String>>,
}
impl Selection {
    pub(super) async fn capture(
        database: &Database,
        secrets: &SecretsManager,
        runtime: &dyn RuntimeManager,
    ) -> Result<Option<Self>> {
        timeout(PREFLIGHT, async {
            let Some(snapshot) = read(database, secrets).await? else {
                return Ok(None);
            };
            let image_id = runtime
                .local_image_id(&snapshot.profile.gateway_image)
                .await?;
            if !image_id.strip_prefix("sha256:").is_some_and(immutable_id)
                || (snapshot.profile.gateway_image.starts_with("sha256:")
                    && snapshot.profile.gateway_image != image_id)
            {
                bail!("selected gateway image is not immutable");
            }
            let selected = Self {
                profile: snapshot.profile,
                image_id,
                fingerprint: snapshot.fingerprint,
            };
            // No effect occurred. Recheck after the runtime await so a setting,
            // secret rotation or tag replacement cannot silently change intent.
            selected.current_inner(database, secrets, runtime).await?;
            Ok(Some(selected))
        })
        .await
        .context("application protection selection deadline")?
    }
    pub(super) fn profile(&self) -> &ActiveLiveEgressProfile {
        &self.profile
    }
    pub(super) fn image_id(&self) -> &str {
        &self.image_id
    }
    pub(super) async fn current(
        &self,
        database: &Database,
        secrets: &SecretsManager,
        runtime: &dyn RuntimeManager,
    ) -> Result<()> {
        timeout(PREFLIGHT, self.current_inner(database, secrets, runtime))
            .await
            .context("application protection recheck deadline")?
    }
    async fn current_inner(
        &self,
        database: &Database,
        secrets: &SecretsManager,
        runtime: &dyn RuntimeManager,
    ) -> Result<()> {
        self.snapshot(database, secrets).await?;
        // Capture already resolved/checked the actual local image. An exact
        // image-ID reference cannot be retagged; every real gateway still uses
        // that frozen ID and its actual native configuration is inspected.
        // Repeated image existence queries are not live VPN/grant evidence.
        // Mutable tags and repository references retain change detection and
        // the post-runtime-await configuration/secret recheck.
        if self.profile.gateway_image != self.image_id {
            if runtime.local_image_id(&self.profile.gateway_image).await? != self.image_id {
                bail!("selected gateway image changed");
            }
            self.snapshot(database, secrets).await?;
        }
        Ok(())
    }
    async fn snapshot(&self, database: &Database, secrets: &SecretsManager) -> Result<Snapshot> {
        let snapshot = read(database, secrets)
            .await?
            .context("selected protection is no longer available")?;
        if snapshot.profile != self.profile || snapshot.fingerprint != self.fingerprint {
            bail!("selected protection configuration changed");
        }
        Ok(snapshot)
    }
    /// Bounded normalized files for a future owned gateway. Resolving VPN
    /// endpoints here uses the existing trusted operator-config normalizer,
    /// never a publisher's source URL. No gateway is started or health granted.
    pub(super) async fn material(
        &self,
        database: &Database,
        secrets: &SecretsManager,
        runtime: &dyn RuntimeManager,
    ) -> Result<Vec<PreparedMaterialFile>> {
        timeout(PREFLIGHT, async {
            self.current_inner(database, secrets, runtime).await?;
            let snapshot = self.snapshot(database, secrets).await?;
            let values = snapshot.values;
            let files = match self.profile.kind {
                DownloadNetworkProfileKind::CloudflareWarp => Vec::new(),
                DownloadNetworkProfileKind::WireguardConfig
                | DownloadNetworkProfileKind::OpenvpnConfig => {
                    let kind = if self.profile.kind == DownloadNetworkProfileKind::WireguardConfig {
                        LiveEgressProfileKind::Wireguard
                    } else {
                        LiveEgressProfileKind::Openvpn
                    };
                    prepare_gateway_material_from_secret_values(
                        kind,
                        values
                            .config
                            .as_ref()
                            .context("selected VPN material unavailable")?
                            .as_bytes(),
                        values.username.as_ref().map(|s| s.as_bytes()),
                        values.password.as_ref().map(|s| s.as_bytes()),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("selected VPN material is invalid"))?
                }
                _ => bail!("application protection cannot use a direct profile"),
            };
            self.current_inner(database, secrets, runtime).await?;
            Ok(files)
        })
        .await
        .context("application protection material deadline")?
    }
}

async fn secret(
    database: &Database,
    secrets: &SecretsManager,
    reference: Option<&str>,
    maximum: usize,
) -> Result<Option<Zeroizing<String>>> {
    let Some(reference) = reference else {
        return Ok(None);
    };
    // The shared store rejects ambiguous logical keys. Trusted operator values
    // are decrypted before this cap; this is not an untrusted DB read bound.
    let value = resolve_live_egress_secret(&database.pool, secrets, reference)
        .await
        .map_err(|_| anyhow::anyhow!("selected protection material is unavailable"))?;
    if value.len() > maximum || value.contains('\0') {
        bail!("selected protection material exceeds its bounds");
    }
    Ok(Some(value))
}
fn part(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}
async fn read(database: &Database, secrets: &SecretsManager) -> Result<Option<Snapshot>> {
    if sqlx::query("SELECT id FROM download_network_profiles WHERE active=TRUE LIMIT 2")
        .fetch_all(&database.pool)
        .await?
        .len()
        > 1
    {
        bail!("selected protection profile is ambiguous");
    }
    let Some(profile) = marketplace_http_profile(&database.pool, secrets).await? else {
        return Ok(None);
    };
    if profile.kind == DownloadNetworkProfileKind::CloudflareWarp {
        // Reuse only recorded acceptance, never create/repair an enrollment.
        let rows=sqlx::query("SELECT enrollment_id,identity_secret_ref,status,disclosure_version,CAST(disclosure_accepted_at AS TEXT) AS accepted FROM download_warp_enrollments WHERE profile_id=$1 AND created_at=(SELECT MAX(created_at) FROM download_warp_enrollments WHERE profile_id=$1) LIMIT 2")
            .bind(&profile.profile_id).fetch_all(&database.pool).await?;
        if rows.len() != 1 {
            bail!("selected WARP enrollment is unavailable or ambiguous");
        }
        let row = &rows[0];
        if row.try_get::<String, _>("enrollment_id")?
            != profile.enrollment_id.as_deref().unwrap_or("")
            || row.try_get::<String, _>("identity_secret_ref")?
                != profile.identity_secret_ref.as_deref().unwrap_or("")
            || row.try_get::<String, _>("status")? != "ready"
            || row.try_get::<String, _>("disclosure_version")?
                != cloudflare_warp_disclosure().version
            || row
                .try_get::<Option<String>, _>("accepted")?
                .is_none_or(|s| s.trim().is_empty())
        {
            bail!("selected WARP disclosure or enrollment is not current");
        }
    }
    let values = Values {
        config: secret(
            database,
            secrets,
            profile.config_secret_ref.as_deref(),
            MATERIAL_LIMIT,
        )
        .await?,
        username: secret(
            database,
            secrets,
            profile.username_secret_ref.as_deref(),
            AUTH_LIMIT,
        )
        .await?,
        password: secret(
            database,
            secrets,
            profile.password_secret_ref.as_deref(),
            AUTH_LIMIT,
        )
        .await?,
        identity: secret(
            database,
            secrets,
            profile.identity_secret_ref.as_deref(),
            MATERIAL_LIMIT,
        )
        .await?,
    };
    let mut input = Zeroizing::new(Vec::new());
    for value in [&profile.profile_id, &profile.name, &profile.gateway_image] {
        part(&mut input, value.as_bytes());
    }
    part(&mut input, profile_kind_as_str(&profile.kind).as_bytes());
    for value in [
        &profile.enrollment_id,
        &profile.config_secret_ref,
        &profile.username_secret_ref,
        &profile.password_secret_ref,
        &profile.identity_secret_ref,
    ] {
        input.push(u8::from(value.is_some()));
        part(&mut input, value.as_deref().unwrap_or("").as_bytes());
    }
    for value in [
        &values.config,
        &values.username,
        &values.password,
        &values.identity,
    ] {
        input.push(u8::from(value.is_some()));
        part(
            &mut input,
            value.as_ref().map_or(b"".as_slice(), |s| s.as_bytes()),
        );
    }
    let fingerprint = secrets.request_fingerprint(DOMAIN, &input)?;
    Ok(Some(Snapshot {
        profile,
        values,
        fingerprint,
    }))
}

#[cfg(test)]
mod tests;
