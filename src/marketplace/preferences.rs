//! Durable owner preferences, not networking, provider or paid-issuer authority.
//! Mutations reuse the grant store's transaction-current owner/session checks.
use super::{
    grant_store::{Actor, advancing_now, ensure_before_expiry},
    worker::{PARTICIPATION_DISCLOSURE_VERSION, ParticipationSettings},
};
use crate::{
    authz::Capability,
    db::{Database, DatabaseDriver},
    http::auth::CurrentPrincipal,
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use elixir_marketplace::{
    canonical,
    policy::{ContributionMode, Metering},
};
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Row, any::AnyRow};
use std::time::Instant;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Duration, timeout},
};
use uuid::Uuid;

pub const ISSUER_DISCLOSURE_VERSION: u32 = 1;
pub mod local_setup;

const MAX_REVISION: i64 = (1i64 << 53) - 1;
pub const MAX_SETTINGS_BYTES: usize = 4096;

/// An owner's declaration, never inferred from link speed or peer traffic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionMetering {
    #[default]
    Unknown,
    Metered,
    Unmetered,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preferences {
    pub schema_version: u32,
    pub contribution: ContributionMode,
    pub metering: ConnectionMetering,
    pub contribution_disclosure_version: Option<u32>,
    /// Separate server preference, not an issuer capability or another account's
    /// consent. Issuance must bind consent to its actual requesting paid account.
    /// Contribution or paid status never implies endorsement.
    pub paid_support_consent: bool,
    pub issuer_disclosure_version: Option<u32>,
}
impl Default for Preferences {
    fn default() -> Self {
        Self {
            schema_version: 1,
            contribution: ContributionMode::Auto,
            metering: ConnectionMetering::Unknown,
            contribution_disclosure_version: None,
            paid_support_consent: false,
            issuer_disclosure_version: None,
        }
    }
}
impl Preferences {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1
            || self
                .contribution_disclosure_version
                .is_some_and(|version| version != PARTICIPATION_DISCLOSURE_VERSION)
            || self
                .issuer_disclosure_version
                .is_some_and(|version| version != ISSUER_DISCLOSURE_VERSION)
            || (self.paid_support_consent
                && self.issuer_disclosure_version != Some(ISSUER_DISCLOSURE_VERSION))
        {
            return Err(ChangeError::Invalid.into());
        }
        Ok(())
    }
    pub(super) fn participation(&self) -> ParticipationSettings {
        ParticipationSettings {
            revision: None,
            mode: self.contribution,
            metering: match self.metering {
                ConnectionMetering::Unknown => Metering::Unknown,
                ConnectionMetering::Metered => Metering::Metered,
                ConnectionMetering::Unmetered => Metering::Unmetered,
            },
            accepted_disclosure_version: self.contribution_disclosure_version,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub expected_revision: i64,
    pub mutation_id: Uuid,
    pub preferences: Preferences,
}
impl Change {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Self = canonical::decode(bytes, MAX_SETTINGS_BYTES)?;
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<()> {
        if !(0..MAX_REVISION).contains(&self.expected_revision) || self.mutation_id.is_nil() {
            return Err(ChangeError::Invalid.into());
        }
        self.preferences.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Snapshot {
    pub revision: i64,
    pub preferences: Preferences,
}
impl Snapshot {
    fn participation(&self) -> ParticipationSettings {
        let mut settings = self.preferences.participation();
        settings.revision = Some(self.revision);
        settings
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    #[error("invalid marketplace preferences or disclosure version")]
    Invalid,
    #[error("marketplace preferences changed; reload before retrying")]
    Conflict,
    #[error("current server owner authorization is required")]
    Forbidden,
    #[error("marketplace owner setup is ambiguous; trusted local setup is required")]
    OwnerSetupRequired,
}

struct Stored {
    snapshot: Snapshot,
    home: Option<Uuid>,
    actor: Option<Uuid>,
    mutation: Option<Uuid>,
}
impl Stored {
    fn parse(row: AnyRow) -> Result<Self> {
        let revision: i64 = row.try_get("revision")?;
        let json: String = row.try_get("preferences_json")?;
        let home = row
            .try_get::<Option<String>, _>("home_id")?
            .map(|s| s.parse::<Uuid>())
            .transpose()?;
        let actor = row
            .try_get::<Option<String>, _>("approved_by_user_id")?
            .map(|s| s.parse::<Uuid>())
            .transpose()?;
        let mutation = row
            .try_get::<Option<String>, _>("mutation_id")?
            .map(|s| s.parse::<Uuid>())
            .transpose()?;
        if !(0..=MAX_REVISION).contains(&revision) {
            bail!("invalid stored marketplace preference revision");
        }
        let preferences = if revision == 0 {
            if !json.is_empty() || home.is_some() || actor.is_some() || mutation.is_some() {
                bail!("inconsistent initial marketplace preferences");
            }
            Preferences::default()
        } else {
            if home.is_none_or(|id| id.is_nil())
                || mutation.is_none_or(|id| id.is_nil())
                || json.is_empty()
            {
                bail!("inconsistent stored marketplace preferences");
            }
            let preferences: Preferences = canonical::decode(json.as_bytes(), MAX_SETTINGS_BYTES)?;
            preferences
                .validate()
                .map_err(|_| anyhow::anyhow!("stored marketplace preferences are invalid"))?;
            preferences
        };
        Ok(Self {
            snapshot: Snapshot {
                revision,
                preferences,
            },
            home,
            actor,
            mutation,
        })
    }
}
const SELECT: &str = "SELECT revision,home_id,preferences_json,mutation_id,approved_by_user_id FROM marketplace_preferences WHERE singleton=1";

fn owner(principal: &CurrentPrincipal) -> Result<Actor> {
    if !principal.has_capability(Capability::SettingsManage) {
        return Err(ChangeError::Forbidden.into());
    }
    Actor::from_principal(principal, false).map_err(|_| ChangeError::Forbidden.into())
}

async fn require_owner_binding(
    stored: &Stored,
    actor: &Actor,
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
) -> Result<()> {
    if let Some(home) = stored.home {
        if home != actor.home {
            return Err(ChangeError::Forbidden.into());
        }
    } else {
        // Home ownership is not automatically authority over every home on a
        // server. Fresh single-home setup is unambiguous; never let whichever
        // account calls first claim global settings on a multi-home database.
        let homes: Vec<String> = sqlx::query_scalar("SELECT id FROM homes ORDER BY id LIMIT 2")
            .fetch_all(&mut **tx)
            .await?;
        if homes.len() != 1 || homes[0] != actor.home.to_string() {
            return Err(ChangeError::OwnerSetupRequired.into());
        }
    }
    Ok(())
}

/// Serialize source approval with the server-wide owner binding, not just the
/// requesting home's membership. This changes no preference or consent value.
pub(super) async fn require_actor_binding(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    actor: &Actor,
) -> Result<()> {
    let row=sqlx::query("UPDATE marketplace_preferences SET updated_at=updated_at WHERE singleton=1 RETURNING revision,home_id,preferences_json,mutation_id,approved_by_user_id")
        .fetch_optional(&mut **tx).await?.context("marketplace preferences are missing")?;
    require_owner_binding(&Stored::parse(row)?, actor, tx).await
}
/// Installed controllers use the committed server-home binding, not a current
/// browser session. Changing/losing that binding stops old-home SDK effects.
pub(super) async fn require_home_binding(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    home: Uuid,
    driver: DatabaseDriver,
) -> Result<()> {
    // PostgreSQL readers may coexist, but any owner rebind still waits. SQLite
    // must take the writer position first: SELECT-then-upgrade can deadlock
    // concurrent deferred transactions. Neither path weakens the binding check.
    let query = match driver {
        DatabaseDriver::Postgres => {
            "SELECT revision,home_id,preferences_json,mutation_id,approved_by_user_id FROM marketplace_preferences WHERE singleton=1 FOR SHARE"
        }
        DatabaseDriver::Sqlite => {
            "UPDATE marketplace_preferences SET updated_at=updated_at WHERE singleton=1 RETURNING revision,home_id,preferences_json,mutation_id,approved_by_user_id"
        }
    };
    let row = sqlx::query(query)
        .fetch_optional(&mut **tx)
        .await?
        .context("marketplace preferences are missing")?;
    if Stored::parse(row)?.home != Some(home) {
        bail!("SDK grant is outside the server home binding");
    }
    Ok(())
}

/// Internal server read. Missing state is an error, never a default reset.
pub(super) async fn load_internal(database: &Database) -> Result<Snapshot> {
    let mut connection = super::durability::connection(database).await?;
    let row = sqlx::query(SELECT)
        .fetch_optional(&mut *connection)
        .await?
        .context("marketplace preferences are missing")?;
    Ok(Stored::parse(row)?.snapshot)
}

pub async fn load(
    database: &Database,
    principal: &CurrentPrincipal,
    now: DateTime<Utc>,
) -> Result<Snapshot> {
    load_as(database, &owner(principal)?, now).await
}
async fn load_as(database: &Database, actor: &Actor, now: DateTime<Utc>) -> Result<Snapshot> {
    let started = Instant::now();
    let mut connection = super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    let until = actor
        .authenticate(&mut tx, advancing_now(now, started)?)
        .await
        .map_err(|_| ChangeError::Forbidden)?;
    let row = sqlx::query(SELECT)
        .fetch_optional(&mut *tx)
        .await?
        .context("marketplace preferences are missing")?;
    let stored = Stored::parse(row)?;
    require_owner_binding(&stored, actor, &mut tx).await?;
    ensure_before_expiry(until, now, started)?;
    tx.commit().await?;
    Ok(stored.snapshot)
}

pub async fn save(
    database: &Database,
    principal: &CurrentPrincipal,
    change: &Change,
    now: DateTime<Utc>,
) -> Result<Snapshot> {
    save_as(database, &owner(principal)?, change, now).await
}
async fn save_as(
    database: &Database,
    actor: &Actor,
    change: &Change,
    now: DateTime<Utc>,
) -> Result<Snapshot> {
    let started = Instant::now();
    change.validate()?;
    let json = String::from_utf8(canonical::to_vec(&change.preferences)?)?;
    anyhow::ensure!(json.len() <= MAX_SETTINGS_BYTES, "preference bounds");
    let mut connection = super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    if database.driver == DatabaseDriver::Postgres {
        sqlx::query("SET LOCAL synchronous_commit=on")
            .execute(&mut *tx)
            .await?;
    }
    let until = actor
        .authenticate(&mut tx, advancing_now(now, started)?)
        .await
        .map_err(|_| ChangeError::Forbidden)?;
    let previous = Stored::parse(
        sqlx::query(SELECT)
            .fetch_optional(&mut *tx)
            .await?
            .context("marketplace preferences are missing")?,
    )?;
    require_owner_binding(&previous, actor, &mut tx).await?;
    if previous.mutation == Some(change.mutation_id) {
        if previous.actor != Some(actor.user)
            || previous.snapshot.preferences != change.preferences
            || previous.snapshot.revision != change.expected_revision + 1
        {
            return Err(ChangeError::Conflict.into());
        }
        ensure_before_expiry(until, now, started)?;
        tx.commit().await?;
        return Ok(previous.snapshot);
    }
    if previous.snapshot.revision != change.expected_revision {
        return Err(ChangeError::Conflict.into());
    }
    let revision = change.expected_revision + 1;
    let updated = sqlx::query("UPDATE marketplace_preferences SET revision=$1,home_id=$2,preferences_json=$3,mutation_id=$4,approved_by_user_id=$5,updated_at=CURRENT_TIMESTAMP WHERE singleton=1 AND revision=$6")
        .bind(revision).bind(actor.home.to_string()).bind(json).bind(change.mutation_id.to_string())
        .bind(actor.user.to_string()).bind(change.expected_revision).execute(&mut *tx).await?.rows_affected();
    if updated != 1 {
        return Err(ChangeError::Conflict.into());
    }
    ensure_before_expiry(until, now, started)?;
    tx.commit().await?;
    Ok(Snapshot {
        revision,
        preferences: change.preferences.clone(),
    })
}

/// Owns one bounded local observer. No caller-supplied health or consent value
/// reaches the production role controller; it reads committed preferences.
pub(super) struct Observer {
    observed: watch::Receiver<ParticipationSettings>,
    task: JoinHandle<()>,
}
impl Observer {
    pub async fn start(database: Database) -> Result<Self> {
        let mut last = timeout(Duration::from_secs(5), load_internal(&database)).await??;
        let (sender, observed) = watch::channel(last.participation());
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if sender.is_closed() {
                    break;
                }
                let next = timeout(Duration::from_secs(5), load_internal(&database))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .and_then(|next| accept_observation(&mut last, next).ok());
                // Unknown state cannot maintain a positive consent assertion.
                // It pauses contribution, not the protected consumer. Never
                // rewrite the store or reset the revision in response.
                sender.send_replace(next.unwrap_or_default());
            }
        });
        Ok(Self { observed, task })
    }
    pub fn subscribe(&self) -> watch::Receiver<ParticipationSettings> {
        self.observed.clone()
    }
}
impl Drop for Observer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn accept_observation(last: &mut Snapshot, next: Snapshot) -> Result<ParticipationSettings> {
    if next.revision < last.revision || (next.revision == last.revision && next != *last) {
        bail!("marketplace preference state regressed or changed without a revision");
    }
    next.preferences.validate()?;
    let settings = next.participation();
    *last = next;
    Ok(settings)
}

#[cfg(test)]
mod tests;
