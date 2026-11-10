//! Trusted database-operator setup, deliberately not exposed through HTTP.
//! Binds only pristine state; never transfers ownership or grants consent.
use super::{Preferences, SELECT, Stored};
use crate::db::{Database, DatabaseDriver};
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use sqlx::{Acquire, any::AnyPoolOptions};
use std::time::Duration;
use uuid::Uuid;

mod sqlite_url;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindingReceipt {
    pub home_id: Uuid,
    pub revision: i64,
}

/// Connect only to an existing database. Unlike normal server bootstrap, this
/// command must not create SQLite files/directories or run schema migrations.
/// Errors deliberately exclude the supplied URL, which may contain credentials.
pub async fn connect_existing(url: &str) -> Result<Database> {
    let driver = DatabaseDriver::from_url(url)
        .map_err(|_| anyhow::anyhow!("unsupported setup database driver"))?;
    let normalized;
    let url = if driver == DatabaseDriver::Sqlite {
        normalized = sqlite_url::existing_url(url).await?;
        normalized.as_str()
    } else {
        url
    };
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await
        .map_err(|_| anyhow::anyhow!("failed to connect to the existing setup database"))?;
    Ok(Database { driver, pool })
}

/// Authority is the operator's existing direct database access, not a home
/// account, request field or saved UI preference. Do not expose this function
/// through a remote endpoint. A stable mutation ID permits an exact retry after
/// uncertain completion, but cannot overwrite any subsequent owner choice.
pub async fn bind_initial_home(
    database: &Database,
    home_id: Uuid,
    mutation_id: Uuid,
) -> Result<BindingReceipt> {
    ensure!(
        !home_id.is_nil() && !mutation_id.is_nil(),
        "setup IDs must be non-nil UUIDs"
    );
    let mut connection = super::super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    if database.driver == DatabaseDriver::Postgres {
        sqlx::query("SET LOCAL synchronous_commit=on")
            .execute(&mut *tx)
            .await?;
    }
    // Lock the actual home, account profile and active owner through commit.
    // Match remote approvals' profile-before-membership lock order; do not take
    // the global preference row first and deadlock with a concurrent approval.
    let owner: String = sqlx::query_scalar(
        "UPDATE homes SET updated_at=updated_at WHERE id=$1 RETURNING owner_user_id",
    )
    .bind(home_id.to_string())
    .fetch_optional(&mut *tx)
    .await?
    .context("setup home does not exist")?;
    let owner_id = Uuid::parse_str(&owner).context("setup home owner is invalid")?;
    ensure!(!owner_id.is_nil(), "setup home owner is invalid");
    let profile: Option<String> = sqlx::query_scalar("UPDATE profiles SET updated_at=updated_at WHERE home_id=$1 AND user_id=$2 AND profile_type='account' RETURNING id")
        .bind(home_id.to_string()).bind(&owner).fetch_optional(&mut *tx).await?;
    let member: Option<String> = sqlx::query_scalar("UPDATE home_members SET updated_at=updated_at WHERE home_id=$1 AND user_id=$2 AND role='owner' AND status='active' RETURNING id")
        .bind(home_id.to_string()).bind(&owner).fetch_optional(&mut *tx).await?;
    ensure!(
        profile.is_some() && member.is_some(),
        "setup home requires its current active account owner"
    );
    let previous = Stored::parse(
        sqlx::query(SELECT)
            .fetch_optional(&mut *tx)
            .await?
            .context("marketplace preferences are missing; setup cannot recreate authority")?,
    )?;
    if previous.snapshot.revision == 1
        && previous.home == Some(home_id)
        && previous.mutation == Some(mutation_id)
        && previous.actor.is_none()
        && previous.snapshot.preferences == Preferences::default()
    {
        tx.commit().await?;
        return Ok(BindingReceipt {
            home_id,
            revision: 1,
        });
    }
    if previous.snapshot.revision != 0 {
        bail!("marketplace settings are already bound; local setup cannot replace them");
    }
    let json = String::from_utf8(elixir_marketplace::canonical::to_vec(
        &Preferences::default(),
    )?)?;
    let updated = sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=NULL,updated_at=CURRENT_TIMESTAMP WHERE singleton=1 AND revision=0 AND home_id IS NULL AND preferences_json='' AND mutation_id IS NULL AND approved_by_user_id IS NULL")
        .bind(home_id.to_string()).bind(json).bind(mutation_id.to_string())
        .execute(&mut *tx).await?.rows_affected();
    ensure!(
        updated == 1,
        "marketplace setup changed concurrently; inspect before retrying"
    );
    tx.commit().await?;
    Ok(BindingReceipt {
        home_id,
        revision: 1,
    })
}

#[cfg(test)]
mod tests;
