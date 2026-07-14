use std::{error::Error, fmt, time::Duration};

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::AnyPool;
use uuid::Uuid;

pub const LIVE_CONTROL_LEASE_NAME: &str = "live-control-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlLease {
    pub owner_instance_id: Uuid,
    pub fencing_token: i64,
    pub acquired_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedControlLease {
    pub owner_instance_id: Option<Uuid>,
    pub fencing_token: i64,
    pub acquired_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub enum ControlLeaseError {
    Held,
    FenceExhausted,
    InvalidState,
    Database(sqlx::Error),
}

impl fmt::Display for ControlLeaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held => formatter.write_str("another Live control server owns the lease"),
            Self::FenceExhausted => formatter.write_str("Live control fencing token is exhausted"),
            Self::InvalidState => {
                formatter.write_str("Live control lease has invalid persisted state")
            }
            Self::Database(_) => {
                formatter.write_str("Live control lease database operation failed")
            }
        }
    }
}

impl Error for ControlLeaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Held | Self::FenceExhausted | Self::InvalidState => None,
        }
    }
}

impl From<sqlx::Error> for ControlLeaseError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

#[derive(Clone)]
pub struct ControlLeaseRepository {
    pool: AnyPool,
    ttl: Duration,
}

impl ControlLeaseRepository {
    pub fn new(pool: AnyPool, ttl: Duration) -> Self {
        Self { pool, ttl }
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs((self.ttl.as_secs() / 3).max(1))
    }

    pub async fn acquire(
        &self,
        owner_instance_id: Uuid,
    ) -> Result<ControlLease, ControlLeaseError> {
        let now = database_now(&self.pool).await?;
        let expires_at = now
            + chrono::Duration::from_std(self.ttl).map_err(|_| ControlLeaseError::InvalidState)?;
        let owner = owner_instance_id.to_string();
        let row: Option<LeaseRow> = sqlx::query_as(
            "UPDATE live_control_server_leases
             SET fencing_token = CASE
                     WHEN owner_instance_id = $1 AND expires_at > CURRENT_TIMESTAMP
                     THEN fencing_token
                     ELSE fencing_token + 1
                 END,
                 owner_instance_id = $2,
                 acquired_at = CASE
                     WHEN owner_instance_id = $3 AND expires_at > CURRENT_TIMESTAMP
                     THEN acquired_at
                     ELSE CURRENT_TIMESTAMP
                 END,
                 heartbeat_at = CURRENT_TIMESTAMP,
                 expires_at = $4
             WHERE lease_name = $5
               AND fencing_token < $6
               AND (
                   owner_instance_id IS NULL
                   OR owner_instance_id = $7
                   OR expires_at <= CURRENT_TIMESTAMP
               )
             RETURNING
                 COALESCE(CAST(owner_instance_id AS TEXT), '') AS owner_instance_id,
                 fencing_token,
                 COALESCE(CAST(acquired_at AS TEXT), '') AS acquired_at,
                 COALESCE(CAST(heartbeat_at AS TEXT), '') AS heartbeat_at,
                 COALESCE(CAST(expires_at AS TEXT), '') AS expires_at",
        )
        .bind(&owner)
        .bind(&owner)
        .bind(&owner)
        .bind(expires_at.to_rfc3339())
        .bind(LIVE_CONTROL_LEASE_NAME)
        .bind(i64::MAX)
        .bind(&owner)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = row {
            return row.into_active();
        }

        let current = self.current().await?;
        if current.fencing_token == i64::MAX {
            return Err(ControlLeaseError::FenceExhausted);
        }
        if current.owner_instance_id.is_some() {
            return Err(ControlLeaseError::Held);
        }
        Err(ControlLeaseError::InvalidState)
    }

    pub async fn renew(
        &self,
        lease: &ControlLease,
    ) -> Result<Option<ControlLease>, ControlLeaseError> {
        let now = database_now(&self.pool).await?;
        let expires_at = now
            + chrono::Duration::from_std(self.ttl).map_err(|_| ControlLeaseError::InvalidState)?;
        let row: Option<LeaseRow> = sqlx::query_as(
            "UPDATE live_control_server_leases
             SET heartbeat_at = CURRENT_TIMESTAMP,
                 expires_at = $1
             WHERE lease_name = $2
               AND owner_instance_id = $3
               AND fencing_token = $4
               AND expires_at > CURRENT_TIMESTAMP
             RETURNING
                 COALESCE(CAST(owner_instance_id AS TEXT), '') AS owner_instance_id,
                 fencing_token,
                 COALESCE(CAST(acquired_at AS TEXT), '') AS acquired_at,
                 COALESCE(CAST(heartbeat_at AS TEXT), '') AS heartbeat_at,
                 COALESCE(CAST(expires_at AS TEXT), '') AS expires_at",
        )
        .bind(expires_at.to_rfc3339())
        .bind(LIVE_CONTROL_LEASE_NAME)
        .bind(lease.owner_instance_id.to_string())
        .bind(lease.fencing_token)
        .fetch_optional(&self.pool)
        .await?;
        row.map(LeaseRow::into_active).transpose()
    }

    pub async fn release(&self, lease: &ControlLease) -> Result<bool, ControlLeaseError> {
        let released: Option<i64> = sqlx::query_scalar(
            "UPDATE live_control_server_leases
             SET owner_instance_id = NULL,
                 acquired_at = NULL,
                 heartbeat_at = NULL,
                 expires_at = NULL
             WHERE lease_name = $1
               AND owner_instance_id = $2
               AND fencing_token = $3
             RETURNING fencing_token",
        )
        .bind(LIVE_CONTROL_LEASE_NAME)
        .bind(lease.owner_instance_id.to_string())
        .bind(lease.fencing_token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(released.is_some())
    }

    pub async fn current(&self) -> Result<PersistedControlLease, ControlLeaseError> {
        let row: LeaseRow = sqlx::query_as(
            "SELECT
                 COALESCE(CAST(owner_instance_id AS TEXT), '') AS owner_instance_id,
                 fencing_token,
                 COALESCE(CAST(acquired_at AS TEXT), '') AS acquired_at,
                 COALESCE(CAST(heartbeat_at AS TEXT), '') AS heartbeat_at,
                 COALESCE(CAST(expires_at AS TEXT), '') AS expires_at
             FROM live_control_server_leases
             WHERE lease_name = $1",
        )
        .bind(LIVE_CONTROL_LEASE_NAME)
        .fetch_one(&self.pool)
        .await?;
        row.into_persisted()
    }
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    owner_instance_id: String,
    fencing_token: i64,
    acquired_at: String,
    heartbeat_at: String,
    expires_at: String,
}

impl LeaseRow {
    fn into_active(self) -> Result<ControlLease, ControlLeaseError> {
        let persisted = self.into_persisted()?;
        Ok(ControlLease {
            owner_instance_id: persisted
                .owner_instance_id
                .ok_or(ControlLeaseError::InvalidState)?,
            fencing_token: persisted.fencing_token,
            acquired_at: persisted
                .acquired_at
                .ok_or(ControlLeaseError::InvalidState)?,
            heartbeat_at: persisted
                .heartbeat_at
                .ok_or(ControlLeaseError::InvalidState)?,
            expires_at: persisted
                .expires_at
                .ok_or(ControlLeaseError::InvalidState)?,
        })
    }

    fn into_persisted(self) -> Result<PersistedControlLease, ControlLeaseError> {
        if self.fencing_token < 0 {
            return Err(ControlLeaseError::InvalidState);
        }
        let owner_instance_id = optional_uuid(&self.owner_instance_id)?;
        let acquired_at = optional_timestamp(&self.acquired_at)?;
        let heartbeat_at = optional_timestamp(&self.heartbeat_at)?;
        let expires_at = optional_timestamp(&self.expires_at)?;
        let timestamps_present = [
            acquired_at.is_some(),
            heartbeat_at.is_some(),
            expires_at.is_some(),
        ];
        if owner_instance_id.is_some() != timestamps_present.iter().all(|value| *value)
            || (owner_instance_id.is_none() && timestamps_present.iter().any(|value| *value))
        {
            return Err(ControlLeaseError::InvalidState);
        }
        Ok(PersistedControlLease {
            owner_instance_id,
            fencing_token: self.fencing_token,
            acquired_at,
            heartbeat_at,
            expires_at,
        })
    }
}

async fn database_now(pool: &AnyPool) -> Result<DateTime<Utc>, ControlLeaseError> {
    let value: String = sqlx::query_scalar("SELECT CAST(CURRENT_TIMESTAMP AS TEXT)")
        .fetch_one(pool)
        .await?;
    parse_timestamp(&value)
}

fn optional_uuid(value: &str) -> Result<Option<Uuid>, ControlLeaseError> {
    if value.is_empty() {
        Ok(None)
    } else {
        Uuid::parse_str(value)
            .map(Some)
            .map_err(|_| ControlLeaseError::InvalidState)
    }
}

fn optional_timestamp(value: &str) -> Result<Option<DateTime<Utc>>, ControlLeaseError> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_timestamp(value).map(Some)
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, ControlLeaseError> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%.f%z",
    ] {
        if let Ok(value) = DateTime::parse_from_str(value, format) {
            return Ok(value.with_timezone(&Utc));
        }
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(value.and_utc());
        }
    }
    Err(ControlLeaseError::InvalidState)
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{config::DatabaseConfig, db::Database};

    use super::*;

    async fn test_database() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite:file:s10-live-lease-{}?mode=memory&cache=shared",
                Uuid::new_v4()
            ),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    #[tokio::test]
    async fn s10_control_lease_acquire_renew_release_takeover_and_fence() -> Result<()> {
        let database = test_database().await?;
        let repository =
            ControlLeaseRepository::new(database.pool.clone(), Duration::from_secs(30));
        let first_owner = Uuid::new_v4();
        let second_owner = Uuid::new_v4();

        let first = repository.acquire(first_owner).await?;
        assert_eq!(first.fencing_token, 1);
        let idempotent = repository.acquire(first_owner).await?;
        assert_eq!(idempotent.fencing_token, first.fencing_token);
        assert!(matches!(
            repository.acquire(second_owner).await,
            Err(ControlLeaseError::Held)
        ));
        let renewed = repository.renew(&idempotent).await?.expect("renewed lease");
        assert_eq!(renewed.fencing_token, first.fencing_token);
        assert!(
            !repository
                .release(&ControlLease {
                    owner_instance_id: second_owner,
                    ..renewed.clone()
                })
                .await?
        );

        sqlx::query(
            "UPDATE live_control_server_leases
             SET expires_at = '2000-01-01T00:00:00Z'
             WHERE lease_name = $1",
        )
        .bind(LIVE_CONTROL_LEASE_NAME)
        .execute(&database.pool)
        .await?;
        let takeover = repository.acquire(second_owner).await?;
        assert_eq!(takeover.fencing_token, 2);
        assert!(repository.renew(&renewed).await?.is_none());
        assert!(!repository.release(&renewed).await?);
        assert!(repository.release(&takeover).await?);
        let released = repository.current().await?;
        assert_eq!(released.fencing_token, 2);
        assert!(released.owner_instance_id.is_none());
        assert!(
            sqlx::query("DELETE FROM live_control_server_leases WHERE lease_name = $1")
                .bind(LIVE_CONTROL_LEASE_NAME)
                .execute(&database.pool)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn s10_control_lease_concurrent_acquire_has_one_owner() -> Result<()> {
        let database = test_database().await?;
        let repository =
            ControlLeaseRepository::new(database.pool.clone(), Duration::from_secs(30));
        let first = repository.clone();
        let second = repository.clone();
        let first_owner = Uuid::new_v4();
        let second_owner = Uuid::new_v4();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let first_barrier = barrier.clone();
        let second_barrier = barrier.clone();
        let first_task = tokio::spawn(async move {
            first_barrier.wait().await;
            first.acquire(first_owner).await
        });
        let second_task = tokio::spawn(async move {
            second_barrier.wait().await;
            second.acquire(second_owner).await
        });
        barrier.wait().await;
        let outcomes = [first_task.await?, second_task.await?];
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Err(ControlLeaseError::Held)))
                .count(),
            1
        );
        assert_eq!(repository.current().await?.fencing_token, 1);
        Ok(())
    }

    #[test]
    fn s10_control_lease_parses_postgres_timestamp_offsets() {
        let parsed = parse_timestamp("2026-07-12 01:09:13.921277-05").unwrap();
        assert_eq!(parsed.to_rfc3339(), "2026-07-12T06:09:13.921277+00:00");
    }
}
