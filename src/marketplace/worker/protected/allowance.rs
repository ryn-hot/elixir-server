//! Automatic, prepaid renewal driven only by the owned kernel counter. No
//! worker activity report, timer-only full grant, refund or retry after apply.
use crate::{
    db::Database,
    marketplace::{
        accounting::{CommittedTransferReservation, available_transfer, reserve_transfer},
        packet::PacketGuardController,
    },
    runtime::RuntimeManager,
};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    packet_guard::{CREDIT_MARGIN, MAX_CREDIT_BYTES, PacketCredit},
    packet_usage::PacketUsage,
    policy::OrdinaryLimits,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, timeout};

const MIN_TARGET_BYTES: u64 = 64 * 1024;
const EXPIRY_HEADROOM_MILLIS: u64 = 60_000;
const CHECK_INTERVAL: Duration = Duration::from_secs(3);

pub(super) struct Allowance {
    database: Database,
    limits: OrdinaryLimits,
    next_check: Instant,
}
impl Allowance {
    pub fn new(database: Database, limits: OrdinaryLimits) -> Self {
        Self {
            database,
            limits,
            next_check: Instant::now(),
        }
    }
    pub async fn maintain(
        &mut self,
        runtime: &dyn RuntimeManager,
        guard: &mut PacketGuardController,
    ) -> Result<()> {
        if Instant::now() < self.next_check {
            return Ok(());
        }
        let usage = guard.observe_credit(runtime).await?;
        if let Some(debit) = self.reserve_replacement(usage).await? {
            guard.apply_credit(runtime, debit).await?;
        }
        self.next_check = Instant::now() + CHECK_INTERVAL;
        Ok(())
    }
    async fn reserve_replacement(
        &self,
        usage: PacketUsage,
    ) -> Result<Option<CommittedTransferReservation>> {
        if let Some(target) = requirement(usage)? {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let available = timeout(
                Duration::from_secs(5),
                available_transfer(&self.database, now, &self.limits),
            )
            .await
            .context("marketplace allowance read deadline")??;
            let bytes = target.min(available);
            if bytes < 2 * CREDIT_MARGIN {
                bail!("marketplace monthly transfer allowance exhausted");
            }
            let debit = timeout(
                Duration::from_secs(5),
                reserve_transfer(&self.database, now, bytes, &self.limits),
            )
            .await
            .context("marketplace allowance debit deadline")??;
            return Ok(Some(debit));
        }
        Ok(None)
    }
}

/// Pure scheduling policy only; calling this with JSON grants nothing. The
/// caller must obtain a fresh owned observation, commit a debit and consume it.
fn requirement(usage: PacketUsage) -> Result<Option<u64>> {
    usage.validate(PacketCredit {
        revision: usage.revision,
        bytes: usage.prepaid_bytes,
    })?;
    let remaining = usage
        .prepaid_bytes
        .saturating_sub(CREDIT_MARGIN)
        .saturating_sub(usage.charged_bytes);
    let headroom = (usage.prepaid_bytes / 8).clamp(2 * CREDIT_MARGIN, 512 * 1024);
    if remaining > headroom && usage.valid_for_millis > EXPIRY_HEADROOM_MILLIS {
        return Ok(None);
    }
    // Scale active grants with measured charge, but shrink idle replacement
    // windows. Expiry alone must not repeatedly spend a 16 MiB full grant.
    Ok(Some(
        usage
            .charged_bytes
            .saturating_mul(2)
            .clamp(MIN_TARGET_BYTES, MAX_CREDIT_BYTES),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn usage() -> PacketUsage {
        PacketUsage {
            revision: 1,
            prepaid_bytes: 1024 * 1024,
            charged_bytes: 0,
            valid_for_millis: 3_600_000,
        }
    }
    #[test]
    fn fresh_idle_or_part_used_credit_does_not_trigger_a_timer_debit() -> Result<()> {
        assert_eq!(requirement(usage())?, None);
        assert_eq!(
            requirement(PacketUsage {
                charged_bytes: 512 * 1024,
                ..usage()
            })?,
            None
        );
        assert_eq!(
            requirement(PacketUsage {
                valid_for_millis: 60_001,
                ..usage()
            })?,
            None
        );
        Ok(())
    }
    #[test]
    fn real_charge_or_near_expiry_requests_only_a_bounded_adaptive_window() -> Result<()> {
        assert_eq!(
            requirement(PacketUsage {
                valid_for_millis: 60_000,
                ..usage()
            })?,
            Some(MIN_TARGET_BYTES)
        );
        assert_eq!(
            requirement(PacketUsage {
                valid_for_millis: 0,
                ..usage()
            })?,
            Some(MIN_TARGET_BYTES)
        );
        assert_eq!(
            requirement(PacketUsage {
                charged_bytes: 1000 * 1024,
                ..usage()
            })?,
            Some(2000 * 1024)
        );
        assert_eq!(
            requirement(PacketUsage {
                prepaid_bytes: MAX_CREDIT_BYTES,
                charged_bytes: MAX_CREDIT_BYTES - 1,
                ..usage()
            })?,
            Some(MAX_CREDIT_BYTES)
        );
        assert!(
            requirement(PacketUsage {
                charged_bytes: 1024 * 1024,
                ..usage()
            })
            .is_err()
        );
        assert!(
            requirement(PacketUsage {
                valid_for_millis: 3_600_001,
                ..usage()
            })
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn replacement_requires_a_real_durable_debit_without_refunds_or_quota_reset() -> Result<()>
    {
        let directory = tempfile::tempdir()?;
        let database = Database::connect(&crate::config::DatabaseConfig {
            url: format!(
                "sqlite://{}",
                directory.path().join("allowance.sqlite").display()
            ),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        sqlx::raw_sql(include_str!(
            "../../../../migrations/0060_marketplace_transfer_accounting.sql"
        ))
        .execute(&database.pool)
        .await?;
        let limits = OrdinaryLimits {
            monthly_transfer_bytes: MIN_TARGET_BYTES + 8192,
            ..Default::default()
        };
        let allowance = Allowance::new(database.clone(), limits.clone());
        assert!(allowance.reserve_replacement(usage()).await?.is_none());
        let revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM marketplace_transfer_accounting WHERE singleton=1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            revision, 0,
            "no timer debit while sufficient native credit remains"
        );
        let expired = PacketUsage {
            valid_for_millis: 0,
            ..usage()
        };
        {
            let first = allowance.reserve_replacement(expired).await?.unwrap();
            assert_eq!(first.bytes(), MIN_TARGET_BYTES);
            // Leaving scope without apply never refunds a committed debit.
        }
        {
            let final_window = allowance.reserve_replacement(expired).await?.unwrap();
            assert_eq!(final_window.bytes(), 8192);
            assert_eq!(final_window.remaining_after(), 0);
        }
        assert!(allowance.reserve_replacement(expired).await.is_err());
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        assert_eq!(available_transfer(&database, now, &limits).await?, 0);
        sqlx::query("DELETE FROM marketplace_transfer_accounting")
            .execute(&database.pool)
            .await?;
        assert!(
            allowance.reserve_replacement(expired).await.is_err(),
            "missing ledger is never a fresh month"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "explicit owned network-none native counter and durable allowance integration"]
    async fn native_counter_drives_one_prepaid_replacement_and_corrupt_accounting_grants_nothing()
    -> Result<()> {
        use crate::{
            config::DatabaseConfig, marketplace::packet::PacketGuardProfile,
            runtime::docker::DockerRuntimeManager,
        };
        use elixir_marketplace::{
            identity::Digest,
            packet_guard::{PacketPolicy, VpnEndpoint},
        };
        let image = std::env::var("ELIXIR_MARKETPLACE_PACKET_GUARD_IMAGE")
            .context("explicit digest-pinned credit-usage guard image required")?;
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite://{}",
                root.join("native-allowance.sqlite").display()
            ),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        sqlx::raw_sql(include_str!(
            "../../../../migrations/0060_marketplace_transfer_accounting.sql"
        ))
        .execute(&database.pool)
        .await?;
        let limits = OrdinaryLimits {
            monthly_transfer_bytes: 96 * 1024,
            ..Default::default()
        };
        let deployment = format!("allowance-{}", uuid::Uuid::new_v4().simple());
        let profile = PacketGuardProfile::isolated(
            &deployment,
            &image,
            PacketPolicy {
                schema_version: 1,
                epoch: Digest::of(b"discarded-allowance-fixture-epoch"),
                tunnel_interface: "wg0".into(),
                tunnel_addresses: vec!["10.66.0.2".parse()?],
                endpoints: vec![VpnEndpoint {
                    address: "8.8.8.8".parse()?,
                    port: 51820,
                    local_port: 51821,
                }],
                lease_seconds: 15,
                upload_bytes_per_second: 32768,
                download_bytes_per_second: 131072,
            },
            &limits,
        )?;
        struct Cleanup {
            name: String,
            owner: String,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let owned = std::process::Command::new("docker")
                    .args([
                        "inspect",
                        "--format",
                        "{{index .Config.Labels \"elixir.deployment_id\"}}",
                        &self.name,
                    ])
                    .output()
                    .is_ok_and(|result| {
                        result.status.success()
                            && String::from_utf8_lossy(&result.stdout).trim() == self.owner
                    });
                if owned {
                    let _ = std::process::Command::new("docker")
                        .args(["rm", "-f", &self.name])
                        .output();
                }
            }
        }
        let cleanup = Cleanup {
            name: profile.container_spec().name.clone(),
            owner: deployment,
        };
        let runtime = DockerRuntimeManager::new(None);
        let mut guard =
            PacketGuardController::start_isolated(&runtime, &root.join("controller"), profile)
                .await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        guard
            .apply_credit(
                &runtime,
                reserve_transfer(&database, now, 8192, &limits).await?,
            )
            .await?;
        let first = guard.observe_credit(&runtime).await?;
        assert_eq!(
            (first.revision, first.prepaid_bytes, first.charged_bytes),
            (1, 8192, 0)
        );
        let mut allowance = Allowance::new(database.clone(), limits.clone());
        allowance.maintain(&runtime, &mut guard).await?;
        let replaced = guard.observe_credit(&runtime).await?;
        assert_eq!(
            (
                replaced.revision,
                replaced.prepaid_bytes,
                replaced.charged_bytes
            ),
            (2, MIN_TARGET_BYTES, 0)
        );
        assert_eq!(
            available_transfer(&database, now, &limits).await?,
            24 * 1024,
            "old unused credit is not refunded"
        );
        allowance.maintain(&runtime, &mut guard).await?;
        assert_eq!(
            available_transfer(&database, now, &limits).await?,
            24 * 1024,
            "fresh window does not debit on every loop"
        );
        guard
            .apply_credit(
                &runtime,
                reserve_transfer(&database, now, 8192, &limits).await?,
            )
            .await?;
        sqlx::query("UPDATE marketplace_transfer_accounting SET state_json='corrupt'")
            .execute(&database.pool)
            .await?;
        allowance.next_check = Instant::now();
        assert!(allowance.maintain(&runtime, &mut guard).await.is_err());
        let unchanged = guard.observe_credit(&runtime).await?;
        assert_eq!(
            (unchanged.revision, unchanged.prepaid_bytes),
            (3, 8192),
            "no packet grant follows unknown accounting"
        );
        let revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM marketplace_transfer_accounting WHERE singleton=1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(revision, 3);
        guard.stop(&runtime).await?;
        drop(cleanup);
        Ok(())
    }
}
