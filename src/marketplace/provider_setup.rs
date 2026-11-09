//! One durable native-provider choice. No registration, credentials, health,
//! process/controller factory or browser-selected image belongs in this module.
use super::{
    durability,
    grant_store::{Actor, advancing_now, ensure_before_expiry},
    preferences::{self, ChangeError},
};
use crate::{
    authz::Capability,
    db::{Database, DatabaseDriver},
    http::auth::CurrentPrincipal,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use elixir_marketplace::{canonical, identity::Digest, warp_provider::CONSENT_VERSION};
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Row, any::AnyRow};
use std::time::Instant;
use uuid::Uuid;

const MAX_REVISION: i64 = (1_i64 << 53) - 1;
const MAX_BYTES: usize = 2048;
const SELECT: &str = "SELECT revision,home_id,setup_json,mutation_id,approved_by_user_id FROM marketplace_provider_setup WHERE singleton=1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Change {
    schema_version: u32,
    expected_revision: i64,
    mutation_id: Uuid,
    enabled: bool,
    consent_version: Option<String>,
}
impl Change {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let change: Self = canonical::decode(bytes, MAX_BYTES)?;
        change.validate()?;
        Ok(change)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1
                && (0..MAX_REVISION).contains(&self.expected_revision)
                && !self.mutation_id.is_nil()
                && if self.enabled {
                    self.consent_version.as_deref() == Some(CONSENT_VERSION)
                } else {
                    self.consent_version.is_none()
                },
            "invalid provider setup choice"
        );
        Ok(())
    }
}

/// A saved choice, explicitly NOT protected readiness or completed registration.
/// No provider identity, account ID, credentials or controller path is exposed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct View {
    pub schema_version: u32,
    pub revision: i64,
    pub enabled: bool,
    pub consent_current: bool,
    pub consent_version: &'static str,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    enabled: bool,
    enrollment: Option<Digest>,
    configuration: Option<Digest>,
    consent_version: Option<String>,
    profile: Option<Uuid>,
    authorization_revision: Option<i64>,
}
impl Decision {
    fn validate(&self) -> Result<()> {
        let unaccepted = self.enrollment.is_none()
            && self.configuration.is_none()
            && self.consent_version.is_none()
            && self.profile.is_none()
            && self.authorization_revision.is_none();
        let accepted = self.enrollment.is_some_and(|id| id.as_bytes() != &[0; 32])
            && self
                .configuration
                .is_some_and(|id| id.as_bytes() != &[0; 32])
            && self
                .consent_version
                .as_ref()
                .is_some_and(|v| !v.is_empty() && v.len() <= 128)
            && self.profile.is_some_and(|id| !id.is_nil())
            && self
                .authorization_revision
                .is_some_and(|r| (1..=MAX_REVISION).contains(&r));
        ensure!(
            (unaccepted && !self.enabled) || accepted,
            "invalid retained provider decision"
        );
        Ok(())
    }
}
struct Stored {
    revision: i64,
    home: Option<Uuid>,
    mutation: Option<Uuid>,
    actor: Option<Uuid>,
    decision: Decision,
}
impl Stored {
    fn parse(row: AnyRow) -> Result<Self> {
        let revision: i64 = row.try_get("revision")?;
        let uuid = |name| -> Result<Option<Uuid>> {
            let value: Option<String> = row.try_get(name)?;
            value
                .map(|v| Uuid::parse_str(&v).context("invalid provider owner record"))
                .transpose()
        };
        let (home, mutation, actor) = (
            uuid("home_id")?,
            uuid("mutation_id")?,
            uuid("approved_by_user_id")?,
        );
        ensure!(
            (0..=MAX_REVISION).contains(&revision),
            "invalid provider control revision"
        );
        let json: String = row.try_get("setup_json")?;
        let decision = if revision == 0 {
            ensure!(
                home.is_none() && mutation.is_none() && actor.is_none() && json.is_empty(),
                "invalid pristine provider setup"
            );
            Decision::default()
        } else {
            ensure!(
                home.is_some_and(|id| !id.is_nil())
                    && mutation.is_some_and(|id| !id.is_nil())
                    && actor.is_none_or(|id| !id.is_nil()),
                "invalid retained provider owner"
            );
            let decision: Decision = canonical::decode(json.as_bytes(), MAX_BYTES)?;
            decision.validate()?;
            decision
        };
        Ok(Self {
            revision,
            home,
            mutation,
            actor,
            decision,
        })
    }
    fn view(&self, configuration: Digest) -> View {
        View {
            schema_version: 1,
            revision: self.revision,
            enabled: self.decision.enabled,
            consent_current: self.actor.is_some()
                && self.decision.enabled
                && self.decision.configuration == Some(configuration)
                && self.decision.consent_version.as_deref() == Some(CONSENT_VERSION),
            consent_version: CONSENT_VERSION,
        }
    }
}
fn actor(principal: &CurrentPrincipal) -> Result<Actor> {
    if !principal.has_capability(Capability::SettingsManage) {
        return Err(ChangeError::Forbidden.into());
    }
    Actor::from_principal(principal, true).map_err(|_| ChangeError::Forbidden.into())
}
async fn synchronous(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    driver: DatabaseDriver,
) -> Result<()> {
    if driver == DatabaseDriver::Postgres {
        sqlx::query("SET LOCAL synchronous_commit=on")
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
pub(crate) async fn load(
    database: &Database,
    principal: &CurrentPrincipal,
    configuration: Digest,
    now: DateTime<Utc>,
) -> Result<View> {
    load_as(database, &actor(principal)?, configuration, now).await
}
async fn load_as(
    database: &Database,
    actor: &Actor,
    configuration: Digest,
    now: DateTime<Utc>,
) -> Result<View> {
    let started = Instant::now();
    let mut connection = durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    let until = actor
        .authenticate(&mut tx, advancing_now(now, started)?)
        .await?;
    preferences::require_actor_binding(&mut tx, actor).await?;
    let stored = Stored::parse(
        sqlx::query(SELECT)
            .fetch_optional(&mut *tx)
            .await?
            .context("provider setup state is missing")?,
    )?;
    ensure!(
        stored.home.is_none_or(|home| home == actor.home),
        "provider setup belongs to another server home"
    );
    ensure_before_expiry(until, now, started)?;
    tx.commit().await?;
    let mut view = stored.view(configuration);
    let (profile, revision) = actor.profile_authorization();
    if stored.actor != Some(actor.user)
        || stored.decision.profile != Some(profile)
        || stored.decision.authorization_revision != Some(revision)
    {
        view.consent_current = false;
    }
    Ok(view)
}
pub(crate) async fn save(
    database: &Database,
    principal: &CurrentPrincipal,
    configuration: Digest,
    change: &Change,
    now: DateTime<Utc>,
) -> Result<(View, bool)> {
    save_as(database, &actor(principal)?, configuration, change, now).await
}
async fn save_as(
    database: &Database,
    actor: &Actor,
    configuration: Digest,
    change: &Change,
    now: DateTime<Utc>,
) -> Result<(View, bool)> {
    change.validate()?;
    ensure!(
        configuration.as_bytes() != &[0; 32],
        "provider configuration is unavailable"
    );
    let started = Instant::now();
    let mut connection = durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let until = actor
        .authenticate(&mut tx, advancing_now(now, started)?)
        .await?;
    preferences::require_actor_binding(&mut tx, actor).await?;
    // A separate saved marketplace choice binds the server home first. This
    // endpoint cannot claim an unbound/multi-home server or imply contribution.
    preferences::require_home_binding(&mut tx, actor.home, database.driver).await?;
    let previous = Stored::parse(
        sqlx::query(SELECT)
            .fetch_optional(&mut *tx)
            .await?
            .context("provider setup state is missing")?,
    )?;
    ensure!(
        previous.home.is_none_or(|home| home == actor.home),
        "provider setup belongs to another server home"
    );
    if previous.mutation == Some(change.mutation_id) {
        ensure!(
            previous.actor == Some(actor.user)
                && previous.revision == change.expected_revision + 1
                && previous.decision.enabled == change.enabled
                && (!change.enabled
                    || (previous.decision.configuration == Some(configuration)
                        && previous.decision.consent_version == change.consent_version)),
            "provider setup mutation conflict"
        );
        ensure_before_expiry(until, now, started)?;
        tx.commit().await?;
        return Ok((previous.view(configuration), false));
    }
    ensure!(
        previous.revision == change.expected_revision,
        "provider setup revision conflict"
    );
    let mut decision = previous.decision;
    decision.enabled = change.enabled;
    if change.enabled {
        // One retained ID, never one ID per request, retry, toggle or restart.
        decision
            .enrollment
            .get_or_insert_with(|| Digest::of(Uuid::new_v4().as_bytes()));
        decision.configuration = Some(configuration);
        decision.consent_version = change.consent_version.clone();
        let (profile, revision) = actor.profile_authorization();
        decision.profile = Some(profile);
        decision.authorization_revision = Some(revision);
    }
    decision.validate()?;
    let revision = change.expected_revision + 1;
    let bytes = canonical::to_vec(&decision)?;
    ensure!(bytes.len() <= MAX_BYTES, "provider decision size bound");
    let updated = sqlx::query("UPDATE marketplace_provider_setup SET revision=$1,home_id=$2,setup_json=$3,mutation_id=$4,approved_by_user_id=$5,updated_at=CURRENT_TIMESTAMP WHERE singleton=1 AND revision=$6")
        .bind(revision).bind(actor.home.to_string()).bind(String::from_utf8(bytes)?).bind(change.mutation_id.to_string())
        .bind(actor.user.to_string()).bind(change.expected_revision).execute(&mut *tx).await?.rows_affected();
    ensure!(updated == 1, "provider setup changed concurrently");
    ensure_before_expiry(until, now, started)?;
    tx.commit().await?;
    Ok((
        Stored {
            revision,
            home: Some(actor.home),
            mutation: Some(change.mutation_id),
            actor: Some(actor.user),
            decision,
        }
        .view(configuration),
        true,
    ))
}

/// Private retained decision binding only. It cannot register, start a native
/// process, grant health/packets, select another image or repair missing state.
pub(super) struct Binding {
    database: Database,
    configuration: Digest,
    revision: i64,
    home: Uuid,
    enrollment: Digest,
}
impl Binding {
    pub(super) fn enrollment(&self) -> Digest {
        self.enrollment
    }
    pub(super) async fn current(&self) -> Result<()> {
        let next = capture(&self.database, self.configuration).await?;
        ensure!(
            next.revision == self.revision
                && next.home == self.home
                && next.enrollment == self.enrollment,
            "provider setup changed or was withdrawn"
        );
        Ok(())
    }
}
pub(super) async fn capture(database: &Database, configuration: Digest) -> Result<Binding> {
    let mut connection = durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    if database.driver == DatabaseDriver::Sqlite {
        // Reserve SQLite's single writer BEFORE reading a snapshot. Otherwise
        // a concurrent settings writer can make the later authorization UPDATE
        // fail immediately during read-to-write upgrade, bypassing busy wait.
        // This changes no decision/revision and is not consent or authority.
        // PostgreSQL keeps its existing profile -> home -> provider lock order.
        sqlx::query("UPDATE marketplace_provider_setup SET singleton=singleton WHERE singleton=1")
            .execute(&mut *tx)
            .await?;
    }
    let stored = Stored::parse(
        sqlx::query(SELECT)
            .fetch_optional(&mut *tx)
            .await?
            .context("provider setup state is missing")?,
    )?;
    ensure!(
        stored.view(configuration).consent_current,
        "current explicit provider consent is required"
    );
    let home = stored.home.context("provider owner home missing")?;
    let user = stored.actor.context("provider consenting owner missing")?;
    let profile = stored
        .decision
        .profile
        .context("provider consenting profile missing")?;
    let authorization_revision = stored
        .decision
        .authorization_revision
        .context("provider authorization revision missing")?;
    // Standing consent survives logout, not changed owner/profile permissions.
    // Match interactive profile -> membership -> authorization -> server-home
    // -> provider-row lock order, without inventing any browser session.
    let profile_row: Option<String> = sqlx::query_scalar("UPDATE profiles SET updated_at=updated_at WHERE id=$1 AND home_id=$2 AND user_id=$3 AND profile_type='account' RETURNING id")
        .bind(profile.to_string()).bind(home.to_string()).bind(user.to_string()).fetch_optional(&mut *tx).await?;
    let owner: Option<String> = sqlx::query_scalar("UPDATE home_members SET updated_at=updated_at WHERE home_id=$1 AND user_id=$2 AND role='owner' AND status='active' RETURNING id")
        .bind(home.to_string()).bind(user.to_string()).fetch_optional(&mut *tx).await?;
    let authorization: Option<i64> = sqlx::query_scalar("UPDATE profile_authorization_revisions SET revision=revision WHERE profile_id=$1 AND home_id=$2 AND revision=$3 RETURNING revision")
        .bind(profile.to_string()).bind(home.to_string()).bind(authorization_revision).fetch_optional(&mut *tx).await?;
    ensure!(
        profile_row.is_some() && owner.is_some() && authorization.is_some(),
        "provider owner authorization changed"
    );
    preferences::require_home_binding(&mut tx, home, database.driver).await?;
    let current = Stored::parse(sqlx::query("UPDATE marketplace_provider_setup SET updated_at=updated_at WHERE singleton=1 RETURNING revision,home_id,setup_json,mutation_id,approved_by_user_id")
        .fetch_optional(&mut *tx).await?.context("provider setup state is missing")?)?;
    ensure!(
        current.revision == stored.revision
            && current.home == stored.home
            && current.actor == stored.actor
            && current.decision == stored.decision
            && current.view(configuration).consent_current,
        "provider setup changed during capture"
    );
    let enrollment = current
        .decision
        .enrollment
        .context("provider enrollment identity missing")?;
    tx.commit().await?;
    Ok(Binding {
        database: database.clone(),
        configuration,
        revision: current.revision,
        home,
        enrollment,
    })
}

#[cfg(test)]
mod tests;
