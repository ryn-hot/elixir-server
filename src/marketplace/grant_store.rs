//! Durable owner decisions. No marketplace peer can issue these decisions and
//! no deserialized DTO is an approved grant. Public routes/execution are not
//! enabled here. Mutation handlers must additionally enforce normal HTTP CSRF.
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use elixir_marketplace::{
    canonical,
    identity::{Digest, ExtensionId},
};
use serde::Deserialize;
use sqlx::{Acquire, Any, Row, Transaction, any::AnyRow};
use std::time::Instant;
use uuid::Uuid;
#[cfg(unix)]
pub(in crate::marketplace) mod installation;
pub(in crate::marketplace) mod invocation;
mod sdk;
pub(in crate::marketplace) mod source_authorization;
#[cfg(unix)]
pub(in crate::marketplace) use sdk::local_http::LocalEndpoint;

use super::{grants::GrantReview, manifest::ManifestCheckedQuarantine};
use crate::{
    auth::home_profiles::{HomeRole, ProfileType},
    authz::Capability,
    db::{Database, DatabaseDriver},
    http::auth::{AccountAuthTransport, CurrentPrincipal},
    runtime::model::EnvVar,
    secrets::SecretsManager,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalSelection {
    pub review_id: Digest,
    pub expected_revision: i64,
    pub mutation_id: Uuid,
    #[serde(default)]
    pub automatic_updates: bool,
}

/// Only an authenticated, committed, currently enabled row creates this object.
/// It still does not authorize installation, execution or an update by itself.
pub struct CommittedOwnerGrant {
    extension_id: ExtensionId,
    home_id: Uuid,
    revision: i64,
    review: GrantReview,
    automatic_updates: bool,
}
impl CommittedOwnerGrant {
    pub fn revision(&self) -> i64 {
        self.revision
    }
    pub fn review(&self) -> &GrantReview {
        &self.review
    }
    pub fn automatic_updates(&self) -> bool {
        self.automatic_updates
    }
    fn covers(&self, candidate: &GrantReview) -> Result<bool> {
        if candidate.scope().extension_id() != &self.extension_id {
            return Ok(false);
        }
        Ok(candidate.review_id()? == self.review.review_id()?
            || (self.automatic_updates
                && !candidate
                    .scope()
                    .delta_from(self.review.scope())?
                    .requires_approval()))
    }
    pub async fn check_scope(
        self,
        database: &Database,
        source: ManifestCheckedQuarantine,
        instance_id: Uuid,
    ) -> Result<GrantCheckedQuarantine> {
        self.check_scope_retained(database, &mut Some(source), instance_id)
            .await
    }
    /// A cancelled approval/check keeps the source in its application's slot.
    /// Only move it after the last awaited validation has succeeded.
    pub(super) async fn check_scope_retained(
        self,
        database: &Database,
        source: &mut Option<ManifestCheckedQuarantine>,
        instance_id: Uuid,
    ) -> Result<GrantCheckedQuarantine> {
        let checked = source.as_ref().context("grant source owner missing")?;
        if instance_id.is_nil() {
            bail!("runtime target must be a controller-selected instance");
        }
        let candidate = checked.grant_review()?;
        if !self.covers(&candidate)? {
            bail!("extension update requires fresh owner approval");
        }
        let current = load(database, &self.extension_id)
            .await?
            .context("extension grant is revoked or missing")?;
        if current.home_id != self.home_id
            || current.revision != self.revision
            || !current.covers(&candidate)?
        {
            bail!("extension grant revision changed");
        }
        let target: Option<String> =
            sqlx::query_scalar("SELECT extension_id FROM extension_instances WHERE instance_id=$1")
                .bind(instance_id.to_string())
                .fetch_optional(&database.pool)
                .await?;
        if target.is_some_and(|id| id != self.extension_id.as_str()) {
            bail!("runtime target belongs to a different extension");
        }
        checked.source().ensure_active()?;
        Ok(GrantCheckedQuarantine {
            source: source.take().expect("validated retained source owner"),
            grant: current,
            requested: candidate,
            instance_id,
        })
    }
}

pub struct GrantCheckedQuarantine {
    source: ManifestCheckedQuarantine,
    grant: CommittedOwnerGrant,
    requested: GrantReview,
    instance_id: Uuid,
}
impl GrantCheckedQuarantine {
    /// Allocate fresh disabled instance metadata and its state counters atomically.
    /// Existing targets (even with missing state) fail; no repair or activation.
    pub async fn initialize_sdk_instance(&self, database: &Database, name: &str) -> Result<()> {
        sdk::initialize(
            database,
            &self.grant,
            &self.requested,
            self.instance_id,
            name,
            || self.source.source().require_public_source(),
        )
        .await
    }
    /// Private owner-scope effects for an already-authorized future controller.
    /// This does not authorize execution, install, activate, or open networking.
    pub async fn resolve_sdk_local(
        &self,
        database: &Database,
        secrets: &SecretsManager,
        request: &elixir_marketplace::source_execution::RuntimeJson,
    ) -> Result<elixir_marketplace::source_execution::SourceSdkReply> {
        sdk::execute(
            database,
            secrets,
            &self.grant,
            &self.requested,
            self.instance_id,
            request,
            || self.source.source().require_public_source(),
        )
        .await
    }
    /// Availability refresh never widens/replaces the committed owner grant.
    pub fn refresh_public_source(
        &mut self,
        observation: super::worker::PublicSourceObservation,
    ) -> Result<()> {
        self.source.refresh_public_source(observation)
    }
    pub fn source(&self) -> &ManifestCheckedQuarantine {
        &self.source
    }
    pub fn requested(&self) -> &GrantReview {
        &self.requested
    }
    pub fn grant_revision(&self) -> i64 {
        self.grant.revision
    }
    pub(super) fn source_mut(&mut self) -> &mut ManifestCheckedQuarantine {
        &mut self.source
    }
    pub(super) async fn discard_retained(&mut self) -> Result<()> {
        self.source.discard_retained().await
    }
    pub(super) async fn ensure_current(&self, database: &Database) -> Result<()> {
        self.source.source().ensure_active()?;
        let current = load(database, &self.grant.extension_id)
            .await?
            .context("extension grant is revoked or missing")?;
        if current.home_id != self.grant.home_id
            || current.revision != self.grant.revision
            || !current.covers(&self.requested)?
        {
            bail!("extension grant revision changed");
        }
        self.source.source().ensure_active()?;
        Ok(())
    }
    /// Deliberately no Debug/Serialize on plaintext. Only a trusted future
    /// runtime builder may consume it after the remaining admission gates.
    pub async fn resolve_environment(
        &self,
        database: &Database,
        secrets: &SecretsManager,
    ) -> Result<SecretEnvironment> {
        self.source.source().ensure_active()?;
        let mut connection = super::durability::connection(database).await?;
        let mut transaction = connection.begin().await?;
        synchronous(&mut transaction, database.driver).await?;
        // Serialize this exposure decision with grant revocation. Already
        // injected secrets cannot be unlearned; a controller must stop revoked
        // runtimes, and credential rotation remains an owner action.
        let row = sqlx::query("UPDATE marketplace_extension_grants SET updated_at=updated_at WHERE extension_id=$1 AND home_id=$2 AND revision=$3 AND enabled=TRUE RETURNING extension_id, home_id, revision, review_json, review_id, mutation_id, approved_by_user_id, CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled, CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates")
            .bind(self.grant.extension_id.as_str()).bind(self.grant.home_id.to_string()).bind(self.grant.revision)
            .fetch_optional(&mut *transaction).await?.context("extension grant is no longer current")?;
        let current = StoredGrant::parse(row)?
            .active()
            .context("extension grant is disabled")?;
        if !current.covers(&self.requested)? {
            bail!("extension grant no longer covers the runtime");
        }
        let bindings = self
            .source
            .manifest()
            .runtime
            .as_ref()
            .map_or(&[][..], |runtime| runtime.env.as_slice());
        let environment = resolve_instance_bindings(
            &mut transaction,
            secrets,
            &self.grant.extension_id,
            self.instance_id,
            bindings,
            self.requested.scope().secret_keys(),
        )
        .await?;
        self.source.source().ensure_active()?;
        transaction.commit().await?;
        Ok(environment)
    }
}
pub struct SecretEnvironment {
    values: Vec<EnvVar>,
}
impl SecretEnvironment {
    pub fn into_runtime_env(self) -> Vec<EnvVar> {
        self.values
    }
}

async fn resolve_instance_bindings(
    tx: &mut Transaction<'_, Any>,
    secrets: &SecretsManager,
    extension_id: &ExtensionId,
    instance_id: Uuid,
    bindings: &[crate::extensions::manifest::ManifestRuntimeEnv],
    allowed: &[String],
) -> Result<SecretEnvironment> {
    let instance: String =
        sqlx::query_scalar("SELECT extension_id FROM extension_instances WHERE instance_id=$1")
            .bind(instance_id.to_string())
            .fetch_optional(&mut **tx)
            .await?
            .context("extension instance is missing")?;
    if instance != extension_id.as_str() {
        bail!("secret scope belongs to a different extension");
    }
    let mut values = Vec::new();
    let mut names = std::collections::BTreeSet::new();
    for binding in bindings {
        if !super::grants::valid_env_name(&binding.name) || !names.insert(&binding.name) {
            bail!("invalid runtime environment binding");
        }
        let value = match (&binding.from_secret, &binding.value, binding.optional) {
            (Some(reference), None, optional) => {
                let key = reference
                    .strip_prefix("instance:")
                    .filter(|key| allowed.iter().any(|allowed| allowed == key))
                    .context("secret binding is outside the reviewed instance scope")?;
                let encrypted:Option<String>=sqlx::query_scalar("SELECT value_encrypted FROM secrets WHERE scope='instance' AND scope_id=$1 AND key=$2")
                    .bind(instance_id.to_string()).bind(key).fetch_optional(&mut **tx).await?;
                match encrypted {
                    Some(value) => secrets
                        .decrypt(&value)
                        .context("instance secret could not be decrypted")?,
                    None if optional => continue,
                    None => bail!("required instance secret is missing"),
                }
            }
            (None, Some(value), false) => value.clone(),
            _ => bail!("invalid runtime environment binding"),
        };
        if value.len() > 4096 || value.contains('\0') {
            bail!("instance runtime value is outside bounds");
        }
        values.push(EnvVar {
            name: binding.name.clone(),
            value,
        });
    }
    Ok(SecretEnvironment { values })
}

pub async fn approve(
    database: &Database,
    principal: &CurrentPrincipal,
    source: &ManifestCheckedQuarantine,
    selection: &ApprovalSelection,
    now: DateTime<Utc>,
) -> Result<CommittedOwnerGrant> {
    let review = source.grant_review()?;
    let actor = Actor::from_principal(principal, !review.scope().secret_keys().is_empty())?;
    approve_review_with_binding(database, &actor, &review, selection, now, true).await
}

/// Application-owned approval, never a peer-supplied grant or success receipt.
/// The actor was sealed from a normal authenticated owner principal. Its actual
/// account/profile/session authorization is rechecked in the commit transaction.
pub(super) async fn approve_for_actor(
    database: &Database,
    actor: &Actor,
    source: &ManifestCheckedQuarantine,
    selection: &ApprovalSelection,
) -> Result<CommittedOwnerGrant> {
    source.source().require_public_source()?;
    let review = source.grant_review()?;
    approve_review_with_binding(database, actor, &review, selection, Utc::now(), true).await
}

pub async fn load(
    database: &Database,
    extension_id: &ExtensionId,
) -> Result<Option<CommittedOwnerGrant>> {
    let mut connection = super::durability::connection(database).await?;
    let row = sqlx::query("SELECT extension_id,home_id,revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1")
        .bind(extension_id.as_str()).fetch_optional(&mut *connection).await?;
    row.map(StoredGrant::parse)
        .transpose()
        .map(|r| r.and_then(StoredGrant::active))
}

pub async fn revoke(
    database: &Database,
    principal: &CurrentPrincipal,
    extension_id: &ExtensionId,
    expected_revision: i64,
    mutation_id: Uuid,
    now: DateTime<Utc>,
) -> Result<i64> {
    let actor = Actor::from_principal(principal, false)?;
    revoke_as(
        database,
        &actor,
        extension_id,
        expected_revision,
        mutation_id,
        now,
    )
    .await
}

#[derive(Clone)]
pub(super) struct Actor {
    pub(super) user: Uuid,
    pub(super) home: Uuid,
    profile: Uuid,
    session: Uuid,
    csrf_revision: i32,
    authorization_revision: i64,
    expires: DateTime<Utc>,
}
impl Actor {
    /// Correlation data only. Consumers must authenticate this actor in the
    /// SAME transaction before persisting a standing owner decision.
    pub(super) fn profile_authorization(&self) -> (Uuid, i64) {
        (self.profile, self.authorization_revision)
    }
    /// Test setup only; every lifecycle transition still authenticates this
    /// exact session/profile/revision against the actual database.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_principal(&self) -> CurrentPrincipal {
        CurrentPrincipal::fixture_owner(
            self.user,
            self.home,
            self.profile,
            self.session,
            self.authorization_revision,
            self.expires,
            crate::authz::CapabilitySet::all(),
        )
    }
    /// One preparation retains only this correlation hash, not an approval log
    /// or reusable authorization. Exact retries acknowledge request ownership;
    /// they do not reissue grants or assert that the commit succeeded.
    pub(super) fn approval_id(&self, selection: &ApprovalSelection) -> Result<Digest> {
        if selection.mutation_id.is_nil() || selection.expected_revision < 0 {
            bail!("invalid owner approval selection");
        }
        Ok(Digest::of(canonical::to_vec(&serde_json::json!({
            "user":self.user,"home":self.home,"profile":self.profile,"session":self.session,
            "csrf_revision":self.csrf_revision,"authorization_revision":self.authorization_revision,
            "review":selection.review_id,"revision":selection.expected_revision,
            "mutation":selection.mutation_id,"automatic_updates":selection.automatic_updates
        }))?))
    }
    pub(super) fn from_principal(p: &CurrentPrincipal, has_secrets: bool) -> Result<Self> {
        if p.role != HomeRole::Owner
            || p.profile_type != ProfileType::Account
            || p.transport == AccountAuthTransport::Query
            || !p.has_capability(Capability::ServerAdmin)
            || !p.has_capability(Capability::ExtensionsManage)
            || (has_secrets && !p.has_capability(Capability::SecretsManage))
        {
            bail!("active server owner required for extension grants");
        }
        Ok(Self {
            user: p.user_id,
            home: p.home_id,
            profile: p.profile_id,
            session: p.account_session_id,
            csrf_revision: p.csrf_revision,
            authorization_revision: p.capability_revision,
            expires: p.access_expires_at.min(p.session_expires_at),
        })
    }
    pub(super) async fn authenticate(
        &self,
        tx: &mut Transaction<'_, Any>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        let started = Instant::now();
        if self.expires <= now {
            bail!("owner access has expired");
        }
        // Conditional no-op writes lock the actual owner/profile/session rows,
        // not stale request claims, through the approval commit on both DBs.
        let profile: Option<String> = sqlx::query_scalar("UPDATE profiles SET updated_at=updated_at WHERE id=$1 AND home_id=$2 AND user_id=$3 AND profile_type='account' RETURNING id")
            .bind(self.profile.to_string()).bind(self.home.to_string()).bind(self.user.to_string()).fetch_optional(&mut **tx).await?;
        let owner: Option<String> = sqlx::query_scalar("UPDATE home_members SET updated_at=updated_at WHERE home_id=$1 AND user_id=$2 AND role='owner' AND status='active' RETURNING id")
            .bind(self.home.to_string()).bind(self.user.to_string()).fetch_optional(&mut **tx).await?;
        let authorization: Option<i64> = sqlx::query_scalar("UPDATE profile_authorization_revisions SET revision=revision WHERE profile_id=$1 AND home_id=$2 AND revision=$3 RETURNING revision")
            .bind(self.profile.to_string()).bind(self.home.to_string()).bind(self.authorization_revision)
            .fetch_optional(&mut **tx).await?;
        let expires: Option<String> = sqlx::query_scalar("UPDATE account_sessions SET last_seen_at=last_seen_at WHERE id=$1 AND user_id=$2 AND home_id=$3 AND active_profile_id=$4 AND csrf_revision=$5 AND revoked_at IS NULL RETURNING CAST(expires_at AS TEXT)")
            .bind(self.session.to_string()).bind(self.user.to_string()).bind(self.home.to_string()).bind(self.profile.to_string()).bind(self.csrf_revision)
            .fetch_optional(&mut **tx).await?;
        let expires = expires
            .as_deref()
            .map(crate::auth::sessions::parse_timestamp)
            .transpose()?
            .context("owner session is no longer current")?
            .min(self.expires);
        if profile.is_none()
            || owner.is_none()
            || authorization.is_none()
            || expires <= advancing_now(now, started)?
        {
            bail!("owner authorization or session is no longer current");
        }
        Ok(expires)
    }
}

struct StoredGrant {
    grant: CommittedOwnerGrant,
    enabled: bool,
    mutation: Uuid,
    actor: Option<Uuid>,
}
impl StoredGrant {
    fn parse(row: AnyRow) -> Result<Self> {
        let review_json: String = row.try_get("review_json")?;
        let review: GrantReview = canonical::decode(review_json.as_bytes(), 16 * 1024)?;
        review.validate()?;
        let extension_id: ExtensionId = row.try_get::<String, _>("extension_id")?.parse()?;
        let review_id: Digest = row.try_get::<String, _>("review_id")?.parse()?;
        let revision: i64 = row.try_get("revision")?;
        let enabled: i64 = row.try_get("enabled")?;
        let automatic_updates: i64 = row.try_get("automatic_updates")?;
        if review.scope().extension_id() != &extension_id
            || review.review_id()? != review_id
            || revision < 1
            || !matches!(enabled, 0 | 1)
            || !matches!(automatic_updates, 0 | 1)
        {
            bail!("corrupt extension grant state");
        }
        Ok(Self {
            grant: CommittedOwnerGrant {
                extension_id,
                home_id: Uuid::parse_str(&row.try_get::<String, _>("home_id")?)?,
                revision,
                review,
                automatic_updates: automatic_updates == 1,
            },
            enabled: enabled == 1,
            mutation: Uuid::parse_str(&row.try_get::<String, _>("mutation_id")?)?,
            actor: row
                .try_get::<Option<String>, _>("approved_by_user_id")?
                .map(|v| Uuid::parse_str(&v))
                .transpose()?,
        })
    }
    fn active(self) -> Option<CommittedOwnerGrant> {
        self.enabled.then_some(self.grant)
    }
}

pub(super) async fn synchronous(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
) -> Result<()> {
    if driver == DatabaseDriver::Postgres {
        sqlx::query("SET LOCAL synchronous_commit=on")
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
#[cfg(test)]
async fn approve_review(
    database: &Database,
    actor: &Actor,
    review: &GrantReview,
    selection: &ApprovalSelection,
    now: DateTime<Utc>,
) -> Result<CommittedOwnerGrant> {
    approve_review_with_binding(database, actor, review, selection, now, false).await
}
async fn approve_review_with_binding(
    database: &Database,
    actor: &Actor,
    review: &GrantReview,
    selection: &ApprovalSelection,
    now: DateTime<Utc>,
    require_server_owner: bool,
) -> Result<CommittedOwnerGrant> {
    let started = Instant::now();
    let review_id = review.review_id()?;
    let json = String::from_utf8(canonical::to_vec(review)?)?;
    if review_id != selection.review_id || selection.expected_revision < 0 || json.len() > 16 * 1024
    {
        bail!("owner selection does not match the verified grant review");
    }
    if require_server_owner {
        actor.approval_id(selection)?;
    }
    let mut connection = super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let valid_until = actor
        .authenticate(&mut tx, advancing_now(now, started)?)
        .await?;
    if require_server_owner {
        super::preferences::require_actor_binding(&mut tx, actor).await?;
    }
    let previous = sqlx::query("SELECT extension_id,home_id,revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1")
        .bind(review.scope().extension_id().as_str()).fetch_optional(&mut *tx).await?.map(StoredGrant::parse).transpose()?;
    if let Some(previous) = &previous {
        if previous.grant.home_id != actor.home {
            bail!("extension grant belongs to a different home");
        }
        if previous.enabled
            && previous.mutation == selection.mutation_id
            && previous.actor == Some(actor.user)
            && previous.grant.review.review_id()? == review_id
            && previous.grant.automatic_updates == selection.automatic_updates
        {
            let grant = previous.grant.revision;
            ensure_before_expiry(valid_until, now, started)?;
            tx.commit().await?;
            return Ok(CommittedOwnerGrant {
                extension_id: review.scope().extension_id().clone(),
                home_id: actor.home,
                revision: grant,
                review: review.clone(),
                automatic_updates: selection.automatic_updates,
            });
        }
        if previous.mutation == selection.mutation_id {
            bail!("extension grant mutation ID was reused with different input");
        }
    }
    if previous.as_ref().map_or(0, |p| p.grant.revision) != selection.expected_revision {
        bail!("extension grant revision changed");
    }
    let revision = selection
        .expected_revision
        .checked_add(1)
        .context("grant revision exhausted")?;
    let changed = if previous.is_none() {
        sqlx::query("INSERT INTO marketplace_extension_grants (extension_id,home_id,revision,enabled,automatic_updates,review_json,review_id,mutation_id,approved_by_user_id) VALUES ($1,$2,$3,TRUE,$4,$5,$6,$7,$8) ON CONFLICT(extension_id) DO NOTHING")
            .bind(review.scope().extension_id().as_str()).bind(actor.home.to_string()).bind(revision).bind(selection.automatic_updates).bind(&json).bind(review_id.to_string()).bind(selection.mutation_id.to_string()).bind(actor.user.to_string())
            .execute(&mut *tx).await?.rows_affected()
    } else {
        sqlx::query("UPDATE marketplace_extension_grants SET revision=$1,enabled=TRUE,automatic_updates=$2,review_json=$3,review_id=$4,mutation_id=$5,approved_by_user_id=$6,updated_at=CURRENT_TIMESTAMP WHERE extension_id=$7 AND home_id=$8 AND revision=$9")
            .bind(revision).bind(selection.automatic_updates).bind(&json).bind(review_id.to_string()).bind(selection.mutation_id.to_string()).bind(actor.user.to_string()).bind(review.scope().extension_id().as_str()).bind(actor.home.to_string()).bind(selection.expected_revision)
            .execute(&mut *tx).await?.rows_affected()
    };
    if changed != 1 {
        bail!("extension grant revision changed");
    }
    ensure_before_expiry(valid_until, now, started)?;
    tx.commit().await?;
    Ok(CommittedOwnerGrant {
        extension_id: review.scope().extension_id().clone(),
        home_id: actor.home,
        revision,
        review: review.clone(),
        automatic_updates: selection.automatic_updates,
    })
}
async fn revoke_as(
    database: &Database,
    actor: &Actor,
    extension_id: &ExtensionId,
    expected: i64,
    mutation: Uuid,
    now: DateTime<Utc>,
) -> Result<i64> {
    let started = Instant::now();
    let next = expected
        .checked_add(1)
        .filter(|_| expected > 0)
        .context("invalid grant revision")?;
    let mut connection = super::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let valid_until = actor
        .authenticate(&mut tx, advancing_now(now, started)?)
        .await?;
    // Independent of installation journal JSON or storage accounting. Routine
    // approvals leave this boundary alone, so narrower installed grants can
    // survive normal updates but never an explicit revoke/reapprove cycle.
    let changed=sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,automatic_updates=FALSE,revision=$1,revocation_revision=CASE WHEN revocation_revision>$1 THEN revocation_revision ELSE $1 END,mutation_id=$2,approved_by_user_id=$3,updated_at=CURRENT_TIMESTAMP WHERE extension_id=$4 AND home_id=$5 AND revision=$6")
        .bind(next).bind(mutation.to_string()).bind(actor.user.to_string()).bind(extension_id.as_str()).bind(actor.home.to_string()).bind(expected).execute(&mut *tx).await?.rows_affected();
    if changed != 1 {
        bail!("extension grant revision changed");
    }
    ensure_before_expiry(valid_until, now, started)?;
    tx.commit().await?;
    Ok(next)
}

pub(super) fn advancing_now(utc: DateTime<Utc>, started: Instant) -> Result<DateTime<Utc>> {
    utc.checked_add_signed(chrono::Duration::from_std(started.elapsed())?)
        .context("owner authorization clock overflow")
}
pub(super) fn ensure_before_expiry(
    expires: DateTime<Utc>,
    utc: DateTime<Utc>,
    started: Instant,
) -> Result<()> {
    if expires <= advancing_now(utc, started)? {
        bail!("owner access expired before grant commit");
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::{
        auth::home_profiles::HomeProfileRepository, config::DatabaseConfig,
        extensions::manifest::ManifestRuntimeEnv,
    };

    #[test]
    fn authorization_deadlines_advance_monotonically_before_commit() -> Result<()> {
        let utc = Utc::now();
        let started = Instant::now();
        assert!(ensure_before_expiry(utc - chrono::Duration::seconds(1), utc, started).is_err());
        ensure_before_expiry(utc + chrono::Duration::minutes(1), utc, started)?;
        assert!(advancing_now(utc, started)? >= utc);
        Ok(())
    }

    pub(super) async fn connect(path: &std::path::Path) -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", path.display()),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }
    pub(in crate::marketplace) async fn owner(database: &Database) -> Result<Actor> {
        let user = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id,email,password_hash) VALUES ($1,$2,'fixture hash')")
            .bind(user.to_string())
            .bind(format!("{user}@grant.example.test"))
            .execute(&database.pool)
            .await?;
        let home = HomeProfileRepository::new(&database.pool)
            .ensure_owner_home(user)
            .await?;
        let session = Uuid::new_v4();
        let authorization_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM profile_authorization_revisions WHERE profile_id=$1",
        )
        .bind(home.profile.id.to_string())
        .fetch_one(&database.pool)
        .await?;
        let expires = Utc::now() + chrono::Duration::days(1);
        sqlx::query("INSERT INTO account_sessions (id,user_id,home_id,active_profile_id,expires_at) VALUES ($1,$2,$3,$4,$5)")
            .bind(session.to_string()).bind(user.to_string()).bind(home.home.id.to_string()).bind(home.profile.id.to_string()).bind(expires.to_rfc3339()).execute(&database.pool).await?;
        Ok(Actor {
            user,
            home: home.home.id,
            profile: home.profile.id,
            session,
            csrf_revision: 1,
            authorization_revision,
            expires,
        })
    }
    pub(super) fn selection(review: &GrantReview, revision: i64) -> Result<ApprovalSelection> {
        Ok(ApprovalSelection {
            review_id: review.review_id()?,
            expected_revision: revision,
            mutation_id: Uuid::new_v4(),
            automatic_updates: false,
        })
    }
    #[tokio::test]
    async fn source_approval_requires_the_server_home_binding_in_its_commit_transaction()
    -> Result<()> {
        let dir = tempfile::tempdir()?;
        let database = connect(&dir.path().join("bound.sqlite")).await?;
        let owner = owner(&database).await?;
        let review = super::super::grants::fixture_review()?;
        let input = selection(&review, 0)?;
        let json = String::from_utf8(canonical::to_vec(
            &super::super::preferences::Preferences::default(),
        )?)?;
        sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
            .bind(owner.home.to_string()).bind(json).bind(Uuid::new_v4().to_string()).bind(owner.user.to_string()).execute(&database.pool).await?;
        let foreign = self::owner(&database).await?;
        assert!(
            approve_review_with_binding(&database, &foreign, &review, &input, Utc::now(), true)
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_grants")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(count, 0);
        let approved =
            approve_review_with_binding(&database, &owner, &review, &input, Utc::now(), true)
                .await?;
        assert_eq!(approved.revision(), 1);
        assert!(!approved.automatic_updates());
        sqlx::query(
            "UPDATE marketplace_preferences SET preferences_json='corrupt' WHERE singleton=1",
        )
        .execute(&database.pool)
        .await?;
        assert!(
            approve_review_with_binding(&database, &owner, &review, &input, Utc::now(), true)
                .await
                .is_err(),
            "even an exact retry must not bypass corrupt owner authority"
        );
        assert_eq!(
            load(&database, review.scope().extension_id())
                .await?
                .unwrap()
                .revision(),
            1
        );
        Ok(())
    }
    pub(super) async fn instance(
        database: &Database,
        extension_id: &str,
        name: &str,
    ) -> Result<Uuid> {
        sqlx::query("INSERT INTO extensions (extension_id,name,version,kind,trust_level,manifest_json,enabled) VALUES ($1,'Grant fixture','1.0.0','module','community','{}',FALSE) ON CONFLICT(extension_id) DO NOTHING")
            .bind(extension_id).execute(&database.pool).await?;
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,$2,$3,FALSE)")
            .bind(id.to_string()).bind(extension_id).bind(name).execute(&database.pool).await?;
        Ok(id)
    }
    pub(super) async fn secret(
        database: &Database,
        scope: &str,
        target: Option<Uuid>,
        key: &str,
        value: &str,
        manager: &SecretsManager,
    ) -> Result<()> {
        sqlx::query("INSERT INTO secrets (secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES ($1,$2,$3,$4,$5,FALSE)")
            .bind(Uuid::new_v4().to_string()).bind(scope).bind(target.map(|id|id.to_string())).bind(key).bind(manager.encrypt(value)?).execute(&database.pool).await?;
        Ok(())
    }
    #[tokio::test]
    async fn approvals_survive_restart_require_exact_selection_and_keep_revocation_high_water()
    -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("main.sqlite");
        let database = connect(&path).await?;
        let actor = owner(&database).await?;
        let review = super::super::grants::fixture_review()?;
        let input = selection(&review, 0)?;
        let approved = approve_review(&database, &actor, &review, &input, Utc::now()).await?;
        assert_eq!(approved.revision(), 1);
        assert!(!approved.automatic_updates());
        assert_eq!(
            approve_review(&database, &actor, &review, &input, Utc::now())
                .await?
                .revision(),
            1,
            "same mutation is idempotent"
        );
        let mut wrong = selection(&review, 1)?;
        wrong.review_id = Digest::of(b"different review");
        assert!(
            approve_review(&database, &actor, &review, &wrong, Utc::now())
                .await
                .is_err()
        );
        let mut reused = ApprovalSelection {
            automatic_updates: true,
            expected_revision: 1,
            ..input
        };
        assert!(
            approve_review(&database, &actor, &review, &reused, Utc::now())
                .await
                .is_err()
        );
        drop(approved);
        database.pool.close().await;
        let database = connect(&path).await?;
        let id = review.scope().extension_id();
        assert_eq!(load(&database, id).await?.unwrap().revision(), 1);
        assert_eq!(
            revoke_as(&database, &actor, id, 1, Uuid::new_v4(), Utc::now()).await?,
            2
        );
        assert!(load(&database, id).await?.is_none());
        assert!(
            approve_review(
                &database,
                &actor,
                &review,
                &selection(&review, 0)?,
                Utc::now()
            )
            .await
            .is_err()
        );
        reused.mutation_id = Uuid::new_v4();
        reused.expected_revision = 2;
        assert_eq!(
            approve_review(&database, &actor, &review, &reused, Utc::now())
                .await?
                .revision(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM marketplace_extension_grants")
                .fetch_one(&database.pool)
                .await?,
            1
        );
        Ok(())
    }
    #[tokio::test]
    async fn auto_mode_is_explicit_and_only_covers_a_subset_of_the_approved_scope() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let database = connect(&dir.path().join("main.sqlite")).await?;
        let actor = owner(&database).await?;
        let review = super::super::grants::fixture_review()?;
        let manual = approve_review(
            &database,
            &actor,
            &review,
            &selection(&review, 0)?,
            Utc::now(),
        )
        .await?;
        let mut raw = serde_json::to_value(&review)?;
        raw["release_id"] = serde_json::json!(Digest::of(b"new code"));
        let next: GrantReview = serde_json::from_value(raw.clone())?;
        assert!(!manual.covers(&next)?);
        let input = ApprovalSelection {
            automatic_updates: true,
            ..selection(&review, 1)?
        };
        let automatic = approve_review(&database, &actor, &review, &input, Utc::now()).await?;
        assert!(automatic.covers(&next)?);
        raw["scope"]["secret_keys"] = serde_json::json!(["other_secret", "source_api"]);
        assert!(!automatic.covers(&serde_json::from_value(raw)?)?);
        Ok(())
    }
    #[tokio::test]
    async fn owner_scope_and_live_session_are_rechecked_in_the_commit_transaction() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let database = connect(&dir.path().join("main.sqlite")).await?;
        let actor = owner(&database).await?;
        let review = super::super::grants::fixture_review()?;
        approve_review(
            &database,
            &actor,
            &review,
            &selection(&review, 0)?,
            Utc::now(),
        )
        .await?;
        let another = owner(&database).await?;
        assert!(
            approve_review(
                &database,
                &another,
                &review,
                &selection(&review, 1)?,
                Utc::now()
            )
            .await
            .is_err()
        );
        sqlx::query("UPDATE home_members SET role='admin' WHERE user_id=$1")
            .bind(actor.user.to_string())
            .execute(&database.pool)
            .await?;
        assert!(
            approve_review(
                &database,
                &actor,
                &review,
                &selection(&review, 1)?,
                Utc::now()
            )
            .await
            .is_err()
        );
        sqlx::query("UPDATE home_members SET role='owner' WHERE user_id=$1")
            .bind(actor.user.to_string())
            .execute(&database.pool)
            .await?;
        sqlx::query("UPDATE account_sessions SET csrf_revision=2 WHERE id=$1")
            .bind(actor.session.to_string())
            .execute(&database.pool)
            .await?;
        assert!(
            approve_review(
                &database,
                &actor,
                &review,
                &selection(&review, 1)?,
                Utc::now()
            )
            .await
            .is_err()
        );
        sqlx::query("UPDATE account_sessions SET csrf_revision=1,revoked_at=CURRENT_TIMESTAMP,revoked_reason='test' WHERE id=$1").bind(actor.session.to_string()).execute(&database.pool).await?;
        assert!(
            approve_review(
                &database,
                &actor,
                &review,
                &selection(&review, 1)?,
                Utc::now()
            )
            .await
            .is_err()
        );
        assert_eq!(
            load(&database, review.scope().extension_id())
                .await?
                .unwrap()
                .revision(),
            1
        );
        Ok(())
    }
    #[tokio::test]
    async fn independent_owners_requests_cannot_overwrite_one_expected_revision() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("main.sqlite");
        let database = connect(&path).await?;
        let actor = owner(&database).await?;
        let review = super::super::grants::fixture_review()?;
        let other = connect(&path).await?;
        let first = selection(&review, 0)?;
        let second = selection(&review, 0)?;
        let (a, b) = tokio::join!(
            approve_review(&database, &actor, &review, &first, Utc::now()),
            approve_review(&other, &actor, &review, &second, Utc::now())
        );
        assert_ne!(a.is_ok(), b.is_ok());
        assert_eq!(
            load(&database, review.scope().extension_id())
                .await?
                .unwrap()
                .revision(),
            1
        );
        Ok(())
    }
    #[tokio::test]
    async fn corrupt_or_missing_authority_does_not_create_a_replacement_grant() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let database = connect(&dir.path().join("main.sqlite")).await?;
        let actor = owner(&database).await?;
        let review = super::super::grants::fixture_review()?;
        approve_review(
            &database,
            &actor,
            &review,
            &selection(&review, 0)?,
            Utc::now(),
        )
        .await?;
        sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
            .execute(&database.pool)
            .await?;
        assert!(
            load(&database, review.scope().extension_id())
                .await
                .is_err()
        );
        assert!(
            approve_review(
                &database,
                &actor,
                &review,
                &selection(&review, 1)?,
                Utc::now()
            )
            .await
            .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT revision FROM marketplace_extension_grants")
                .fetch_one(&database.pool)
                .await?,
            1
        );
        let volatile = Database::connect(&crate::config::DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        assert!(
            super::super::durability::connection(&volatile)
                .await
                .is_err()
        );
        Ok(())
    }
    #[tokio::test]
    async fn runtime_secrets_are_own_instance_only_without_global_provider_or_missing_optional_fallback()
    -> Result<()> {
        let dir = tempfile::tempdir()?;
        let database = connect(&dir.path().join("main.sqlite")).await?;
        let review = super::super::grants::fixture_review()?;
        let id = review.scope().extension_id();
        let own = instance(&database, id.as_str(), "own").await?;
        let other = instance(&database, "elixir.test.unrelated", "other").await?;
        let secrets = SecretsManager::from_key_bytes([77; 32], false);
        secret(
            &database,
            "global",
            None,
            "source_api",
            "global-never",
            &secrets,
        )
        .await?;
        secret(
            &database,
            "provider",
            Some(own),
            "source_api",
            "provider-never",
            &secrets,
        )
        .await?;
        secret(
            &database,
            "instance",
            Some(other),
            "source_api",
            "other-never",
            &secrets,
        )
        .await?;
        let mut binding: ManifestRuntimeEnv = serde_json::from_value(
            serde_json::json!({"name":"API_KEY","from_secret":"instance:source_api","optional":true}),
        )?;
        let mut tx = database.pool.begin().await?;
        assert!(
            resolve_instance_bindings(
                &mut tx,
                &secrets,
                id,
                own,
                &[binding.clone()],
                review.scope().secret_keys()
            )
            .await?
            .into_runtime_env()
            .is_empty()
        );
        binding.optional = false;
        assert!(
            resolve_instance_bindings(
                &mut tx,
                &secrets,
                id,
                own,
                &[binding.clone()],
                review.scope().secret_keys()
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        secret(
            &database,
            "instance",
            Some(own),
            "source_api",
            "own-only",
            &secrets,
        )
        .await?;
        let mut tx = database.pool.begin().await?;
        let values = resolve_instance_bindings(
            &mut tx,
            &secrets,
            id,
            own,
            &[binding.clone()],
            review.scope().secret_keys(),
        )
        .await?
        .into_runtime_env();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].value, "own-only");
        assert!(
            resolve_instance_bindings(
                &mut tx,
                &secrets,
                id,
                other,
                &[binding.clone()],
                review.scope().secret_keys()
            )
            .await
            .is_err()
        );
        binding.from_secret = Some("global:source_api".into());
        assert!(
            resolve_instance_bindings(
                &mut tx,
                &secrets,
                id,
                own,
                &[binding],
                review.scope().secret_keys()
            )
            .await
            .is_err()
        );
        tx.rollback().await?;
        Ok(())
    }

    /// The real source lifecycle invokes this only with a native-verified token.
    /// Test rows are in its disposable main DB, never user accounts or installs.
    pub(crate) async fn approve_source_fixture(
        database: &Database,
        source: ManifestCheckedQuarantine,
    ) -> Result<GrantCheckedQuarantine> {
        let actor = owner(database).await?;
        let review = source.grant_review()?;
        let approved = approve_review(
            database,
            &actor,
            &review,
            &selection(&review, 0)?,
            Utc::now(),
        )
        .await?;
        let id = instance(database, review.scope().extension_id().as_str(), "verified").await?;
        let manager = SecretsManager::from_key_bytes([77; 32], false);
        if review
            .scope()
            .secret_keys()
            .iter()
            .any(|key| key == "source_api")
        {
            secret(
                database,
                "global",
                None,
                "source_api",
                "never-global",
                &manager,
            )
            .await?;
            secret(
                database,
                "instance",
                Some(id),
                "source_api",
                "verified-own-instance",
                &manager,
            )
            .await?;
        }
        let ticket = approved.check_scope(database, source, id).await?;
        let environment = ticket
            .resolve_environment(database, &manager)
            .await?
            .into_runtime_env();
        assert!(environment.iter().all(|e| e.name != "NODE_OPTIONS"));
        if review
            .scope()
            .secret_keys()
            .iter()
            .any(|key| key == "source_api")
        {
            assert_eq!(
                environment
                    .iter()
                    .find(|e| e.name == "SOURCE_API_KEY")
                    .map(|e| e.value.as_str()),
                Some("verified-own-instance")
            );
        }
        let _ = revoke_as(
            database,
            &actor,
            review.scope().extension_id(),
            ticket.grant_revision(),
            Uuid::new_v4(),
            Utc::now(),
        )
        .await?;
        assert!(
            ticket
                .resolve_environment(database, &SecretsManager::from_key_bytes([77; 32], false))
                .await
                .is_err()
        );
        // A fresh owner decision is required before the archive handoff. The
        // revoked capability above must never regain authority in place.
        let approved = approve_review(
            database,
            &actor,
            &review,
            &selection(&review, 2)?,
            Utc::now(),
        )
        .await?;
        approved
            .check_scope(database, ticket.source, ticket.instance_id)
            .await
    }
}
