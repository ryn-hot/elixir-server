//! Main-server transfer accounting, outside disposable worker state. A debit
//! must commit before a controller grants at most those bytes to a packet
//! meter. This is not itself a rate limiter, VPN attestation, or packet meter.
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    packet_guard::{MAX_CREDIT_REVISION, PacketCredit},
    policy::{OrdinaryLimits, TransferLedger},
};
use sqlx::{Acquire, Row};

use crate::db::{Database, DatabaseDriver};

const MAX_CAS_ATTEMPTS: usize = 8;

/// Scheduling hint only. Reading does not roll the durable month, reserve
/// bytes, refund lost credit, or authorize traffic. The guard still requires
/// an actual CommittedTransferReservation before applying any allowance.
pub async fn available_transfer(
    database: &Database,
    now: u64,
    limits: &OrdinaryLimits,
) -> Result<u64> {
    limits.validate()?;
    let mut connection = super::durability::connection(database).await?;
    let row = sqlx::query(
        "SELECT revision,state_json FROM marketplace_transfer_accounting WHERE singleton=1",
    )
    .fetch_optional(&mut *connection)
    .await?
    .context("marketplace transfer accounting is missing")?;
    let revision: i64 = row.try_get("revision")?;
    let snapshot: String = row.try_get("state_json")?;
    if revision < 0 || revision > MAX_CREDIT_REVISION as i64 {
        bail!("invalid marketplace accounting revision");
    }
    let ledger = if revision == 0 && snapshot.is_empty() {
        TransferLedger::new_first_boot(now)?
    } else {
        if revision == 0 || snapshot.is_empty() {
            bail!("inconsistent marketplace accounting");
        }
        TransferLedger::restore(snapshot.as_bytes())?
    };
    Ok(ledger.available_bytes(now, limits.monthly_transfer_bytes)?)
}

/// Evidence of one committed debit, not permission to send unmetered traffic.
/// Deliberately not Clone/Deserialize: a controller must consume a debit
/// once when installing its bounded packet allowance. Unused bytes are lost;
/// no worker-provided completion report can refund them.
#[derive(Debug)]
pub struct CommittedTransferReservation {
    revision: u64,
    bytes: u64,
    remaining_after: u64,
}

impl CommittedTransferReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn remaining_after(&self) -> u64 {
        self.remaining_after
    }

    /// Consume this committed debit for a single kernel credit transaction.
    /// The CAS revision is durable and never starts again at a month boundary.
    /// The returned wire data is not a VPN attestation: only the trusted guard
    /// controller may send it to its owned, inspected namespace. Never replay it
    /// after an uncertain apply or reuse it in a replacement namespace. A failed
    /// conversion/application loses the debit; it cannot create a refund.
    pub fn into_packet_credit(self) -> Result<PacketCredit> {
        let credit = PacketCredit {
            revision: self.revision,
            bytes: self.bytes,
        };
        credit.validate()?;
        Ok(credit)
    }
}

/// Trusted controller only. `now` must be reliable controller UTC, never a
/// worker/peer timestamp. An uncertain clock must stop allowance renewal.
/// Concurrent processes use compare-and-swap; no process-local lock is relied
/// on. Failed/uncertain commits grant nothing and are never refunded/replayed.
pub async fn reserve_transfer(
    database: &Database,
    now: u64,
    bytes: u64,
    limits: &OrdinaryLimits,
) -> Result<CommittedTransferReservation> {
    limits.validate()?;
    let mut connection = super::durability::connection(database).await?;
    for _ in 0..MAX_CAS_ATTEMPTS {
        let row = sqlx::query(
            "SELECT revision, state_json FROM marketplace_transfer_accounting WHERE singleton=1",
        )
        .fetch_optional(&mut *connection)
        .await?
        .context("marketplace transfer accounting is missing; refusing to reset allowance")?;
        let revision: i64 = row.try_get("revision")?;
        let snapshot: String = row.try_get("state_json")?;
        let next_revision = revision
            .checked_add(1)
            .filter(|next| revision >= 0 && *next <= MAX_CREDIT_REVISION as i64)
            .context("marketplace transfer accounting revision exhausted")?;
        let mut ledger = if revision == 0 && snapshot.is_empty() {
            TransferLedger::new_first_boot(now)?
        } else {
            if revision == 0 || snapshot.is_empty() {
                bail!("marketplace transfer accounting is inconsistent");
            }
            TransferLedger::restore(snapshot.as_bytes())?
        };
        ledger.reserve(now, bytes, limits.monthly_transfer_bytes)?;
        let encoded = String::from_utf8(ledger.to_database_snapshot()?)?;
        let mut transaction = connection.begin().await?;
        if database.driver == DatabaseDriver::Postgres {
            sqlx::query("SET LOCAL synchronous_commit=on")
                .execute(&mut *transaction)
                .await?;
        }
        let changed = sqlx::query(
            "UPDATE marketplace_transfer_accounting SET state_json=$1, revision=$3 \
             WHERE singleton=1 AND revision=$2",
        )
        .bind(encoded)
        .bind(revision)
        .bind(next_revision)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        if changed == 1 {
            return Ok(CommittedTransferReservation {
                revision: next_revision as u64,
                bytes,
                remaining_after: limits.monthly_transfer_bytes - ledger.used_bytes(),
            });
        }
        // A second process won the debit. Reload rather than overwriting it.
        tokio::task::yield_now().await;
    }
    bail!("marketplace transfer accounting is busy; no allowance granted")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::DatabaseConfig;

    const NOW: u64 = 1_790_000_000;
    const MIGRATION: &str =
        include_str!("../../migrations/0060_marketplace_transfer_accounting.sql");

    async fn connect(path: &Path) -> Result<Database> {
        Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", path.display()),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await
    }

    async fn initialize(database: &Database) -> Result<()> {
        sqlx::raw_sql(MIGRATION).execute(&database.pool).await?;
        Ok(())
    }

    fn limits(bytes: u64) -> OrdinaryLimits {
        OrdinaryLimits {
            monthly_transfer_bytes: bytes,
            ..Default::default()
        }
    }

    async fn used(database: &Database) -> Result<u64> {
        let row =
            sqlx::query("SELECT state_json FROM marketplace_transfer_accounting WHERE singleton=1")
                .fetch_one(&database.pool)
                .await?;
        Ok(TransferLedger::restore(row.try_get::<String, _>(0)?.as_bytes())?.used_bytes())
    }

    #[tokio::test]
    async fn committed_debits_survive_restart_and_unused_allowance_is_not_refunded() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("server.sqlite");
        let database = connect(&path).await?;
        initialize(&database).await?;
        let reservation = reserve_transfer(&database, NOW, 100, &limits(150)).await?;
        assert_eq!(reservation.bytes(), 100);
        assert_eq!(reservation.remaining_after(), 50);
        // A crash before the packet allowance is installed loses the reserved
        // bytes. There is deliberately no Drop/refund behavior.
        drop(reservation);
        database.pool.close().await;
        let restarted = connect(&path).await?;
        assert_eq!(used(&restarted).await?, 100);
        assert!(
            reserve_transfer(&restarted, NOW + 1, 51, &limits(150))
                .await
                .is_err()
        );
        assert_eq!(used(&restarted).await?, 100);
        reserve_transfer(&restarted, NOW + 1, 50, &limits(150)).await?;
        assert_eq!(used(&restarted).await?, 150);
        assert!(
            reserve_transfer(&restarted, NOW - 1000, 1, &limits(150))
                .await
                .is_err()
        );
        reserve_transfer(&restarted, NOW + 40 * 86400, 100, &limits(150)).await?;
        assert_eq!(used(&restarted).await?, 100);
        Ok(())
    }

    #[tokio::test]
    async fn allowance_observation_is_read_only_and_missing_or_corrupt_state_is_not_fresh_budget()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let database = connect(&directory.path().join("preview.sqlite")).await?;
        initialize(&database).await?;
        let limits = limits(150);
        assert_eq!(available_transfer(&database, NOW, &limits).await?, 150);
        let row = sqlx::query(
            "SELECT revision,state_json FROM marketplace_transfer_accounting WHERE singleton=1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(row.try_get::<i64, _>(0)?, 0);
        assert_eq!(row.try_get::<String, _>(1)?, "");
        reserve_transfer(&database, NOW, 100, &limits).await?;
        assert_eq!(available_transfer(&database, NOW + 1, &limits).await?, 50);
        assert_eq!(used(&database).await?, 100);
        assert!(
            available_transfer(&database, NOW - 301, &limits)
                .await
                .is_err()
        );
        sqlx::query("UPDATE marketplace_transfer_accounting SET state_json='broken'")
            .execute(&database.pool)
            .await?;
        assert!(available_transfer(&database, NOW, &limits).await.is_err());
        sqlx::query("DELETE FROM marketplace_transfer_accounting")
            .execute(&database.pool)
            .await?;
        assert!(available_transfer(&database, NOW, &limits).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn independent_server_pools_cannot_double_spend_a_shared_allowance() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("server.sqlite");
        let database = connect(&path).await?;
        initialize(&database).await?;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let independent = connect(&path).await?;
            tasks.push(tokio::spawn(async move {
                reserve_transfer(&independent, NOW, 100, &limits(250)).await
            }));
        }
        let mut successes = 0;
        for task in tasks {
            if task.await?.is_ok() {
                successes += 1;
            }
        }
        assert_eq!(successes, 2);
        assert_eq!(used(&database).await?, 200);
        reserve_transfer(&database, NOW, 50, &limits(250)).await?;
        assert!(
            reserve_transfer(&database, NOW, 1, &limits(250))
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn packet_credits_consume_real_debits_and_keep_revision_across_months() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("server.sqlite");
        let database = connect(&path).await?;
        initialize(&database).await?;
        let first = reserve_transfer(&database, NOW, 8192, &limits(32768))
            .await?
            .into_packet_credit()?;
        assert_eq!((first.revision, first.bytes), (1, 8192));
        database.pool.close().await;
        let restarted = connect(&path).await?;
        let next = reserve_transfer(&restarted, NOW + 40 * 86400, 16384, &limits(32768))
            .await?
            .into_packet_credit()?;
        assert_eq!((next.revision, next.bytes), (2, 16384));
        assert_eq!(used(&restarted).await?, 16384);
        let unusable = reserve_transfer(&restarted, NOW + 40 * 86400, 1, &limits(32768)).await?;
        assert!(unusable.into_packet_credit().is_err());
        assert_eq!(used(&restarted).await?, 16385, "no implicit refund");
        Ok(())
    }

    #[tokio::test]
    async fn exhausted_packet_revision_never_commits_an_unrepresentable_credit() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let database = connect(&directory.path().join("server.sqlite")).await?;
        initialize(&database).await?;
        reserve_transfer(&database, NOW, 8192, &limits(32768)).await?;
        sqlx::query("UPDATE marketplace_transfer_accounting SET revision=$1")
            .bind(MAX_CREDIT_REVISION as i64)
            .execute(&database.pool)
            .await?;
        assert!(
            reserve_transfer(&database, NOW, 8192, &limits(32768))
                .await
                .is_err()
        );
        assert_eq!(used(&database).await?, 8192);
        Ok(())
    }

    #[tokio::test]
    async fn missing_or_corrupt_accounting_never_creates_a_fresh_month() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let database = connect(&directory.path().join("server.sqlite")).await?;
        initialize(&database).await?;
        reserve_transfer(&database, NOW, 100, &limits(150)).await?;
        sqlx::query("UPDATE marketplace_transfer_accounting SET state_json='{}'")
            .execute(&database.pool)
            .await?;
        assert!(
            reserve_transfer(&database, NOW + 40 * 86400, 100, &limits(150))
                .await
                .is_err()
        );
        sqlx::query("DELETE FROM marketplace_transfer_accounting")
            .execute(&database.pool)
            .await?;
        assert!(
            reserve_transfer(&database, NOW, 1, &limits(150))
                .await
                .is_err()
        );
        let count: i64 = sqlx::query("SELECT COUNT(*) FROM marketplace_transfer_accounting")
            .fetch_one(&database.pool)
            .await?
            .try_get(0)?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn unsafe_journal_modes_and_invalid_reservations_are_not_authorized() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let database = connect(&directory.path().join("server.sqlite")).await?;
        initialize(&database).await?;
        assert!(
            reserve_transfer(&database, NOW, 0, &limits(150))
                .await
                .is_err()
        );
        assert!(
            reserve_transfer(&database, NOW, 151, &limits(150))
                .await
                .is_err()
        );
        sqlx::query("PRAGMA main.journal_mode=OFF")
            .execute(&database.pool)
            .await?;
        assert!(
            reserve_transfer(&database, NOW, 1, &limits(150))
                .await
                .is_err()
        );
        let revision: i64 = sqlx::query("SELECT revision FROM marketplace_transfer_accounting")
            .fetch_one(&database.pool)
            .await?
            .try_get(0)?;
        assert_eq!(revision, 0);
        Ok(())
    }

    #[tokio::test]
    async fn volatile_databases_cannot_authorize_network_allowance() -> Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:".to_owned(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        initialize(&database).await?;
        assert!(
            reserve_transfer(&database, NOW, 1, &limits(150))
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn server_migrations_create_only_one_initial_accounting_row() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let database = connect(&directory.path().join("server.sqlite")).await?;
        database.run_migrations().await?;
        reserve_transfer(&database, NOW, 100, &limits(150)).await?;
        database.run_migrations().await?;
        assert_eq!(used(&database).await?, 100);
        assert!(
            sqlx::query("INSERT INTO marketplace_transfer_accounting VALUES(2,0,'')")
                .execute(&database.pool)
                .await
                .is_err()
        );
        assert!(
            sqlx::query("UPDATE marketplace_transfer_accounting SET state_json=$1")
                .bind("x".repeat(1025))
                .execute(&database.pool)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "explicit dedicated local PostgreSQL marketplace accounting test"]
    async fn postgres_migrations_debits_concurrency_restart_and_failure_are_durable() -> Result<()>
    {
        let url = std::env::var("ELIXIR_MARKETPLACE_TEST_POSTGRES_URL")
            .context("requires a fresh dedicated local PostgreSQL test database")?;
        let parsed = reqwest::Url::parse(&url)?;
        if parsed.host_str() != Some("127.0.0.1")
            || parsed.path() != "/elixir_marketplace_accounting_test"
            || !matches!(parsed.scheme(), "postgres" | "postgresql")
        {
            bail!("requires the dedicated loopback marketplace accounting test database");
        }
        let config = DatabaseConfig {
            url,
            max_connections: 2,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        let tables: i64 =
            sqlx::query("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")
                .fetch_one(&database.pool)
                .await?
                .try_get(0)?;
        if tables != 0 {
            bail!("PostgreSQL accounting test database must be empty");
        }
        database.run_migrations().await?;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let independent = Database::connect(&config).await?;
            tasks.push(tokio::spawn(async move {
                reserve_transfer(&independent, NOW, 100, &limits(250)).await
            }));
        }
        let mut successes = 0;
        for task in tasks {
            if task.await?.is_ok() {
                successes += 1;
            }
        }
        assert_eq!(successes, 2);
        assert_eq!(used(&database).await?, 200);
        database.pool.close().await;
        let restarted = Database::connect(&config).await?;
        restarted.run_migrations().await?;
        assert_eq!(used(&restarted).await?, 200);
        assert!(
            reserve_transfer(&restarted, NOW, 51, &limits(250))
                .await
                .is_err()
        );
        let final_debit = reserve_transfer(&restarted, NOW, 50, &limits(250)).await?;
        assert_eq!(final_debit.remaining_after(), 0);
        assert!(
            reserve_transfer(&restarted, NOW - 1000, 1, &limits(250))
                .await
                .is_err()
        );
        reserve_transfer(&restarted, NOW + 40 * 86400, 100, &limits(250)).await?;
        sqlx::query("UPDATE marketplace_transfer_accounting SET state_json='{}'")
            .execute(&restarted.pool)
            .await?;
        assert!(
            reserve_transfer(&restarted, NOW, 1, &limits(250))
                .await
                .is_err()
        );
        sqlx::query("DELETE FROM marketplace_transfer_accounting")
            .execute(&restarted.pool)
            .await?;
        assert!(
            reserve_transfer(&restarted, NOW, 1, &limits(250))
                .await
                .is_err()
        );
        Ok(())
    }
}
