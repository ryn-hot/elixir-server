//! Standing authorization for ONE unattended update, not an account session,
//! first-install permission, mutable grant, native claim or cleanup receipt.
use super::*;
use crate::{
    authz::{AuthorizationRepository, Capability},
    marketplace::worker::MetadataObservation,
};
use std::time::Duration;

const JOB_LIFETIME: Duration = Duration::from_secs(30 * 60);

// A checked stable candidate that cannot advance this installed version is a
// normal no-op, not a transport/authority failure to retry. This error never
// constructs an Authorization or permits an effect.
#[derive(Debug, thiserror::Error)]
#[error("automatic updates require a newer stable semantic version")]
pub(in crate::marketplace) struct NoNewStableRelease;

/// Only capture from current admitted metadata and an actually installed target.
/// No Deserialize/public field/status factory. Clones retain the same deadline,
/// grant baseline, selected release and configured instance; none renew consent.
#[derive(Clone)]
pub(in crate::marketplace) struct Authorization {
    pub(in crate::marketplace) user: Uuid,
    pub(in crate::marketplace) home: Uuid,
    profile: Uuid,
    driver: DatabaseDriver,
    authorization_revision: i64,
    grant_revision: i64,
    baseline: GrantReview,
    instance: Uuid,
    generation: Digest,
    release: Digest,
    expires: DateTime<Utc>,
    monotonic_expires: Instant,
}

fn newer_stable(current: &str, candidate: &str) -> Result<()> {
    let current = semver::Version::parse(current)?;
    let candidate = semver::Version::parse(candidate)?;
    if !candidate.pre.is_empty() || !candidate.cmp_precedence(&current).is_gt() {
        return Err(NoNewStableRelease.into());
    }
    Ok(())
}

impl Authorization {
    pub(in crate::marketplace) async fn capture(
        database: &Database,
        home: Uuid,
        instance: Uuid,
        metadata: &MetadataObservation,
        budget: InstallBudget,
    ) -> Result<Self> {
        if home.is_nil() || instance.is_nil() || !metadata.is_current() {
            bail!("automatic update requires current admitted metadata and an existing target");
        }
        // MetadataObservation is sealed by the exact-release paid admission
        // gate and protected native owner. A signature or advertised total alone
        // cannot construct it. Require its stable head, never the prerelease.
        let descriptor = metadata.descriptor();
        let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
        if descriptor.head.payload.stable_release_id != release {
            bail!("automatic update is not the selected stable release");
        }
        let extension = &descriptor.release.payload.extension_id;
        let binding = standing::InstalledBinding::load_configured(
            database, home, extension, instance, budget,
        )
        .await?;
        let active = binding.material(database).await?;
        let old = active.facts()?;
        newer_stable(
            &old.release().payload().version,
            &descriptor.release.payload.version,
        )?;
        if old.release().release_id() == release {
            bail!("automatic update already installed");
        }
        let query = "SELECT extension_id,home_id,revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1";
        let stored = sqlx::query(query)
            .bind(extension.as_str())
            .fetch_optional(&database.pool)
            .await?
            .context("automatic-update consent is missing")?;
        let stored = StoredGrant::parse(stored)?;
        let user = stored
            .actor
            .context("automatic-update consent has no owner")?;
        let grant = stored
            .active()
            .context("automatic-update grant is disabled")?;
        if grant.home_id != home || !grant.automatic_updates {
            bail!("automatic updates are not enabled for this home and extension");
        }
        let profiles: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM profiles WHERE home_id=$1 AND user_id=$2 AND profile_type='account' LIMIT 2",
        ).bind(home.to_string()).bind(user.to_string()).fetch_all(&database.pool).await?;
        if profiles.len() != 1 {
            bail!("automatic-update owner profile is unavailable or ambiguous");
        }
        let profile = Uuid::parse_str(&profiles[0])?;
        let effective = AuthorizationRepository::new(&database.pool)
            .load_effective(profile, HomeRole::Owner, ProfileType::Account)
            .await?;
        for capability in [Capability::ServerAdmin, Capability::ExtensionsManage] {
            if !effective.capabilities.contains(capability) {
                bail!("automatic-update owner lacks current permissions");
            }
        }
        if !grant.review.scope().secret_keys().is_empty()
            && !effective.capabilities.contains(Capability::SecretsManage)
        {
            bail!("automatic-update owner lacks current secret permissions");
        }
        let now = Utc::now();
        let selected = Self {
            user,
            home,
            profile,
            driver: database.driver,
            authorization_revision: effective.revision,
            grant_revision: grant.revision,
            baseline: grant.review,
            instance,
            generation: Digest::of(canonical::to_vec(active.as_ref())?),
            release,
            expires: now + chrono::Duration::seconds(JOB_LIFETIME.as_secs() as i64),
            monotonic_expires: Instant::now() + JOB_LIFETIME,
        };
        selected.current(database).await?;
        binding.current(database).await?;
        if !metadata.is_current() {
            bail!("automatic-update admission expired during capture");
        }
        Ok(selected)
    }

    pub(in crate::marketplace) fn instance(&self) -> Uuid {
        self.instance
    }
    pub(in crate::marketplace) fn release(&self) -> Digest {
        self.release
    }
    pub(in crate::marketplace) fn extension(&self) -> &ExtensionId {
        self.baseline.scope().extension_id()
    }
    pub(in crate::marketplace) fn grant_revision(&self) -> i64 {
        self.grant_revision
    }
    pub(in crate::marketplace) fn request_id(&self) -> Result<Digest> {
        Ok(Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/automatic-update-authorization/v1",
            "user":self.user,"home":self.home,"profile":self.profile,
            "authorization_revision":self.authorization_revision,
            "grant_revision":self.grant_revision,"baseline":self.baseline.review_id()?,
            "instance":self.instance,"generation":self.generation,"release":self.release,
            "expires":self.expires.timestamp_micros(),
        }))?))
    }
    /// Snapshot only this configured target's actual journal and operational
    /// choices. The command captures/checks them again BEFORE source work;
    /// neither this returned request data nor its mutation is an authority.
    pub(in crate::marketplace) async fn action(
        &self,
        database: &Database,
    ) -> Result<InstallAction> {
        let now = Utc::now();
        let started = Instant::now();
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let expires = self.authenticate(&mut tx, now).await?;
        let stored = store::load_configured_shared(
            &mut tx,
            database.driver,
            self.extension(),
            Some(self.instance),
        )
        .await?
        .context("automatic-update original journal missing")?;
        if stored.journal.pending.is_some() || stored.journal.activation.is_some() {
            bail!("automatic-update target already owns pending work");
        }
        self.require_original(&stored.journal)?;
        let (extension_revision, instance_revision) = choices::capture(
            &mut tx,
            database.driver,
            self.home,
            self.extension(),
            self.instance,
        )
        .await?
        .revisions();
        let action = InstallAction {
            extension: self.extension().clone(),
            release: self.release,
            target: InstallTarget::Existing {
                instance: self.instance,
            },
            expected_revision: stored.journal.revision,
            mutation: Uuid::new_v4(),
            controls: ExpectedControls::Existing {
                extension_revision,
                instance_revision,
            },
        };
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        Ok(action)
    }

    pub(in crate::marketplace) fn require_review(&self, review: &GrantReview) -> Result<()> {
        self.check_deadline(Utc::now())?;
        if review.release_id() != self.release
            || review.scope().extension_id() != self.extension()
            || review
                .scope()
                .delta_from(self.baseline.scope())?
                .requires_approval()
        {
            bail!("automatic update exceeds its exact release or approved baseline");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn require_target(
        &self,
        extension: &ExtensionId,
        release: Digest,
        instance: Option<Uuid>,
    ) -> Result<()> {
        self.check_deadline(Utc::now())?;
        if extension != self.extension()
            || release != self.release
            || instance != Some(self.instance)
        {
            bail!("automatic update changed its exact configured target");
        }
        Ok(())
    }
    pub(super) fn require_original(&self, journal: &Journal) -> Result<()> {
        if journal.configured_instance != Some(self.instance)
            || journal.extension != *self.extension()
            || journal.home != self.home
            || journal
                .active
                .as_ref()
                .map(|active| canonical::to_vec(active.as_ref()))
                .transpose()?
                .map(Digest::of)
                != Some(self.generation)
        {
            bail!("automatic-update original installed generation changed");
        }
        Ok(())
    }
    fn check_deadline(&self, now: DateTime<Utc>) -> Result<()> {
        if now >= self.expires || Instant::now() >= self.monotonic_expires {
            bail!("original automatic-update job expired");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn check_job(&self) -> Result<()> {
        self.check_deadline(Utc::now())
    }
    pub(in crate::marketplace) async fn current(&self, database: &Database) -> Result<()> {
        let now = Utc::now();
        let started = Instant::now();
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let expires = self.authenticate(&mut tx, now).await?;
        crate::marketplace::preferences::require_home_binding(&mut tx, self.home, database.driver)
            .await?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        Ok(())
    }
    /// Locks current owner/profile/capability revision and exact stored consent
    /// through the caller's effect transaction. No account_sessions row is read,
    /// invented or issued; a logged-out owner can still have standing consent.
    pub(in crate::marketplace::grant_store) async fn authenticate(
        &self,
        tx: &mut Transaction<'_, Any>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        self.check_deadline(now)?;
        let profile: Option<String> = sqlx::query_scalar("UPDATE profiles SET updated_at=updated_at WHERE id=$1 AND home_id=$2 AND user_id=$3 AND profile_type='account' RETURNING id")
            .bind(self.profile.to_string()).bind(self.home.to_string()).bind(self.user.to_string()).fetch_optional(&mut **tx).await?;
        let owner: Option<String> = sqlx::query_scalar("UPDATE home_members SET updated_at=updated_at WHERE home_id=$1 AND user_id=$2 AND role='owner' AND status='active' RETURNING id")
            .bind(self.home.to_string()).bind(self.user.to_string()).fetch_optional(&mut **tx).await?;
        let revision: Option<i64> = sqlx::query_scalar("UPDATE profile_authorization_revisions SET revision=revision WHERE profile_id=$1 AND home_id=$2 AND revision=$3 RETURNING revision")
            .bind(self.profile.to_string()).bind(self.home.to_string()).bind(self.authorization_revision).fetch_optional(&mut **tx).await?;
        if profile.is_none() || owner.is_none() || revision.is_none() {
            bail!("automatic-update owner authorization changed");
        }
        // Same owner -> server-home -> grant lock order as interactive grant
        // writers. Taking the grant before server-home would invert that order.
        crate::marketplace::preferences::require_home_binding(tx, self.home, self.driver).await?;
        let row = sqlx::query("UPDATE marketplace_extension_grants SET updated_at=updated_at WHERE extension_id=$1 AND home_id=$2 AND revision=$3 AND enabled=TRUE AND automatic_updates=TRUE AND approved_by_user_id=$4 RETURNING extension_id,home_id,revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates")
            .bind(self.extension().as_str()).bind(self.home.to_string()).bind(self.grant_revision).bind(self.user.to_string()).fetch_optional(&mut **tx).await?
            .context("automatic-update consent changed or was withdrawn")?;
        let grant = StoredGrant::parse(row)?
            .active()
            .context("automatic-update grant disabled")?;
        if grant.review.review_id()? != self.baseline.review_id()? {
            bail!("automatic-update approved baseline changed");
        }
        self.check_deadline(Utc::now())?;
        Ok(self.expires)
    }
}

/// Lifecycle effects accept either real session authorization or the sealed
/// standing update above. Grant approval/revocation APIs still accept only Actor.
#[derive(Clone)]
pub(in crate::marketplace::grant_store) struct EffectActor {
    pub user: Uuid,
    pub home: Uuid,
    authority: EffectAuthority,
}
#[derive(Clone)]
enum EffectAuthority {
    Session(Actor),
    Update(Authorization),
    // Cleanup of this ORIGINAL owned update only. No expiry/opt-in renewal and
    // no new source execution. Cancellation primitives independently require
    // the original staged/unstarted/positively retired proof, never status.
    Recovery(Authorization),
}
impl From<Actor> for EffectActor {
    fn from(actor: Actor) -> Self {
        Self {
            user: actor.user,
            home: actor.home,
            authority: EffectAuthority::Session(actor),
        }
    }
}
impl From<&Authorization> for EffectActor {
    fn from(actor: &Authorization) -> Self {
        Self {
            user: actor.user,
            home: actor.home,
            authority: EffectAuthority::Update(actor.clone()),
        }
    }
}
impl EffectActor {
    pub(super) fn recovery(authorization: &Authorization) -> Self {
        Self {
            user: authorization.user,
            home: authorization.home,
            authority: EffectAuthority::Recovery(authorization.clone()),
        }
    }
    pub(super) fn recovery_id(&self) -> Result<Option<Digest>> {
        match &self.authority {
            EffectAuthority::Recovery(authorization) => Ok(Some(authorization.request_id()?)),
            _ => Ok(None),
        }
    }
    pub(super) fn require_cleanup_target(
        &self,
        extension: &ExtensionId,
        release: Digest,
        instance: Option<Uuid>,
    ) -> Result<()> {
        match &self.authority {
            EffectAuthority::Session(_) => Ok(()),
            EffectAuthority::Update(_) => bail!("update execution is not a cleanup intent"),
            EffectAuthority::Recovery(authorization) => {
                if extension != authorization.extension()
                    || release != authorization.release
                    || instance != Some(authorization.instance)
                {
                    bail!("automatic recovery changed its exact original target")
                }
                Ok(())
            }
        }
    }
    pub(super) fn capture_id(&self) -> Result<Option<Digest>> {
        match &self.authority {
            EffectAuthority::Session(_) => Ok(None),
            EffectAuthority::Update(actor) => Ok(Some(actor.request_id()?)),
            EffectAuthority::Recovery(_) => bail!("recovery cannot capture an installation"),
        }
    }
    pub(super) fn require_capture(&self, captured: Option<Digest>) -> Result<()> {
        if matches!(&self.authority, EffectAuthority::Recovery(_)) {
            bail!("recovery cannot reuse an installation capture");
        }
        if let EffectAuthority::Update(actor) = &self.authority
            && captured != Some(actor.request_id()?)
        {
            bail!("automatic update cannot rebase onto a different captured target");
        }
        Ok(())
    }
    pub(super) fn require_original(&self, journal: &Journal) -> Result<()> {
        match &self.authority {
            EffectAuthority::Session(_) => Ok(()),
            EffectAuthority::Update(actor) => actor.require_original(journal),
            EffectAuthority::Recovery(_) => bail!("recovery cannot stage original source"),
        }
    }
    pub(in crate::marketplace::grant_store) async fn authenticate(
        &self,
        tx: &mut Transaction<'_, Any>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        match &self.authority {
            EffectAuthority::Session(actor) => actor.authenticate(tx, now).await,
            EffectAuthority::Update(actor) => actor.authenticate(tx, now).await,
            EffectAuthority::Recovery(actor) => {
                // This is system-owned rollback, not impersonation of the
                // original user's current session/profile. Opt-out, logout,
                // role changes, or expiry cannot strand an owned candidate.
                // Fresh home/grant binding + actual lifecycle proofs remain
                // mandatory. No enabled flag, review, credentials or source is
                // rewritten here; retained execution checks govern old code.
                crate::marketplace::preferences::require_home_binding(tx, actor.home, actor.driver)
                    .await?;
                store::lock_grant_binding(tx, actor.driver, actor.home, actor.extension()).await?;
                Ok(now + chrono::Duration::seconds(30))
            }
        }
    }
    pub(in crate::marketplace::grant_store) fn require_review(
        &self,
        review: &GrantReview,
    ) -> Result<()> {
        match &self.authority {
            EffectAuthority::Session(_) => Ok(()),
            EffectAuthority::Update(actor) => actor.require_review(review),
            EffectAuthority::Recovery(_) => bail!("recovery cannot approve or execute source"),
        }
    }
    pub(in crate::marketplace::grant_store) fn require_target(
        &self,
        extension: &ExtensionId,
        release: Digest,
        instance: Option<Uuid>,
    ) -> Result<()> {
        match &self.authority {
            EffectAuthority::Session(_) => Ok(()),
            EffectAuthority::Update(actor) => actor.require_target(extension, release, instance),
            EffectAuthority::Recovery(_) => bail!("recovery cannot start or commit an update"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marketplace::grant_store::{approve_review_with_binding, tests as owners};

    async fn fixture() -> Result<(tempfile::TempDir, Database, Actor, Authorization)> {
        let directory = tempfile::tempdir()?;
        let database = owners::connect(&directory.path().join("automatic.sqlite")).await?;
        let owner = owners::owner(&database).await?;
        let baseline = crate::marketplace::grants::fixture_review()?;
        let preferences = String::from_utf8(canonical::to_vec(
            &crate::marketplace::preferences::Preferences::default(),
        )?)?;
        sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
            .bind(owner.home.to_string()).bind(preferences).bind(Uuid::new_v4().to_string())
            .bind(owner.user.to_string()).execute(&database.pool).await?;
        let selection = ApprovalSelection {
            review_id: baseline.review_id()?,
            expected_revision: 0,
            mutation_id: Uuid::new_v4(),
            automatic_updates: true,
        };
        let grant =
            approve_review_with_binding(&database, &owner, &baseline, &selection, Utc::now(), true)
                .await?;
        // Only this test module can construct a synthetic job. Production must
        // capture an actual installed generation + current admitted metadata.
        let authorization = Authorization {
            user: owner.user,
            home: owner.home,
            profile: owner.profile,
            driver: database.driver,
            authorization_revision: owner.authorization_revision,
            grant_revision: grant.revision(),
            baseline,
            instance: Uuid::new_v4(),
            generation: Digest::of(b"original generation"),
            release: Digest::of(b"candidate release"),
            expires: Utc::now() + chrono::Duration::minutes(30),
            monotonic_expires: Instant::now() + JOB_LIFETIME,
        };
        authorization.current(&database).await?;
        Ok((directory, database, owner, authorization))
    }

    #[tokio::test]
    async fn standing_consent_does_not_require_or_create_an_account_session() -> Result<()> {
        let (_directory, database, owner, authorization) = fixture().await?;
        sqlx::query(
            "UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='logout'",
        )
        .execute(&database.pool)
        .await?;
        let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_sessions")
            .fetch_one(&database.pool)
            .await?;
        authorization.current(&database).await?;
        let mut connection = crate::marketplace::durability::connection(&database).await?;
        let mut tx = connection.begin().await?;
        assert!(owner.authenticate(&mut tx, Utc::now()).await.is_err());
        tx.rollback().await?;
        drop(connection);
        let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_sessions")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(before, after);
        Ok(())
    }

    #[tokio::test]
    async fn current_consent_owner_capabilities_and_server_home_are_required() -> Result<()> {
        for change in [
            "UPDATE marketplace_extension_grants SET automatic_updates=FALSE",
            "UPDATE marketplace_extension_grants SET enabled=FALSE",
            "UPDATE marketplace_extension_grants SET revision=revision+1",
            "UPDATE marketplace_extension_grants SET review_json='{}'",
            "UPDATE home_members SET role='viewer'",
            "UPDATE home_members SET role='viewer',status='suspended'",
            "UPDATE profile_authorization_revisions SET revision=revision+1",
            "UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL",
        ] {
            let (_directory, database, _owner, authorization) = fixture().await?;
            sqlx::query(change).execute(&database.pool).await?;
            assert!(authorization.current(&database).await.is_err(), "{change}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn standing_jobs_cannot_renew_deadlines_change_targets_or_widen_the_baseline()
    -> Result<()> {
        let (_directory, database, _owner, authorization) = fixture().await?;
        let mut value = serde_json::to_value(&authorization.baseline)?;
        value["release_id"] = serde_json::json!(authorization.release);
        let candidate: GrantReview = serde_json::from_value(value.clone())?;
        authorization.require_review(&candidate)?;
        authorization.require_target(
            authorization.extension(),
            authorization.release,
            Some(authorization.instance),
        )?;
        assert!(
            authorization
                .require_target(authorization.extension(), authorization.release, None)
                .is_err()
        );
        assert!(
            authorization
                .require_target(
                    authorization.extension(),
                    authorization.release,
                    Some(Uuid::new_v4())
                )
                .is_err()
        );
        assert!(
            authorization
                .require_review(&authorization.baseline)
                .is_err()
        );
        value["scope"]["authority_contract"] = serde_json::json!(Digest::of(b"new contract"));
        assert!(
            authorization
                .require_review(&serde_json::from_value(value)?)
                .is_err()
        );
        let original = authorization.request_id()?;
        assert_eq!(authorization.clone().request_id()?, original);
        let mut expired = authorization.clone();
        expired.monotonic_expires = Instant::now() - Duration::from_secs(1);
        assert!(expired.current(&database).await.is_err());
        expired = authorization.clone();
        expired.expires = Utc::now() - chrono::Duration::seconds(1);
        assert!(expired.current(&database).await.is_err());
        assert_eq!(authorization.request_id()?, original);
        Ok(())
    }
    #[tokio::test]
    async fn recovery_survives_expiry_logout_and_optout_but_cannot_authorize_execution()
    -> Result<()> {
        let (_directory, database, _owner, mut authorization) = fixture().await?;
        authorization.expires = Utc::now() - chrono::Duration::seconds(1);
        authorization.monotonic_expires = Instant::now() - Duration::from_secs(1);
        sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='cleanup fixture logout'").execute(&database.pool).await?;
        sqlx::query("UPDATE marketplace_extension_grants SET automatic_updates=FALSE,enabled=FALSE,revision=revision+1,revocation_revision=revision+1,review_json='{}'").execute(&database.pool).await?;
        sqlx::query("UPDATE home_members SET role='viewer'")
            .execute(&database.pool)
            .await?;
        assert!(authorization.current(&database).await.is_err());
        let recovery = EffectActor::recovery(&authorization);
        let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_sessions")
            .fetch_one(&database.pool)
            .await?;
        let mut connection = crate::marketplace::durability::connection(&database).await?;
        let mut tx = connection.begin().await?;
        recovery.authenticate(&mut tx, Utc::now()).await?;
        tx.commit().await?;
        drop(connection);
        recovery.require_cleanup_target(
            authorization.extension(),
            authorization.release,
            Some(authorization.instance),
        )?;
        assert!(
            recovery
                .require_cleanup_target(
                    authorization.extension(),
                    authorization.release,
                    Some(Uuid::new_v4())
                )
                .is_err()
        );
        assert!(
            recovery
                .require_cleanup_target(
                    authorization.extension(),
                    Digest::of(b"other release"),
                    Some(authorization.instance)
                )
                .is_err()
        );
        assert!(recovery.capture_id().is_err());
        assert!(
            recovery
                .require_capture(Some(authorization.request_id()?))
                .is_err()
        );
        assert!(recovery.require_review(&authorization.baseline).is_err());
        assert!(
            recovery
                .require_target(
                    authorization.extension(),
                    authorization.release,
                    Some(authorization.instance)
                )
                .is_err()
        );
        assert_eq!(recovery.recovery_id()?, Some(authorization.request_id()?));
        assert_eq!(
            sessions,
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM account_sessions")
                .fetch_one(&database.pool)
                .await?
        );
        // A home rebinding still denies SQL cleanup; expiry does not waive it.
        sqlx::query("UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL").execute(&database.pool).await?;
        let mut connection = crate::marketplace::durability::connection(&database).await?;
        let mut tx = connection.begin().await?;
        assert!(recovery.authenticate(&mut tx, Utc::now()).await.is_err());
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn original_staging_proof_cleans_after_revocation_without_reenabling_or_changing_the_target()
    -> Result<()> {
        use super::super::tests::{Fixture, budget};
        use crate::extensions::store::ExtensionStore;
        let f = Fixture::new().await?;
        let staged = f
            .stage_configured(
                InstallTarget::New {
                    name: "Owned cleanup fixture".into(),
                },
                Uuid::new_v4(),
                0,
                budget(),
            )
            .await?;
        let material = f.material()?;
        let target = staged.configured_instance.context("configured target")?;
        // Synthetic job authorization for SQL/proof-boundary testing only;
        // production must capture from an actual active generation/metadata.
        let authorization = Authorization {
            user: f.actor.user,
            home: f.actor.home,
            profile: f.actor.profile,
            driver: f.db.driver,
            authorization_revision: f.actor.authorization_revision,
            grant_revision: f.revision,
            baseline: material.review,
            instance: target,
            generation: Digest::of(b"synthetic original generation"),
            release: staged.release,
            expires: Utc::now() - chrono::Duration::seconds(1),
            monotonic_expires: Instant::now() - Duration::from_secs(1),
        };
        ExtensionStore::new(&f.db.pool)
            .set_instance_enabled(target, false)
            .await?;
        ExtensionStore::new(&f.db.pool)
            .set_extension_enabled(authorization.extension().as_str(), false)
            .await?;
        sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='rollback after logout'").execute(&f.db.pool).await?;
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,automatic_updates=FALSE,revision=revision+1,revocation_revision=revision+1,review_json='{}'").execute(&f.db.pool).await?;
        let before: String =
            sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                .fetch_one(&f.db.pool)
                .await?;
        let mut wrong = authorization.clone();
        wrong.instance = Uuid::new_v4();
        assert!(
            cancellation::OwnerCancelIntent::for_effect_actor(
                EffectActor::recovery(&wrong),
                &staged,
                Uuid::new_v4()
            )
            .is_err()
        );
        assert!(
            cancellation::OwnerCancelIntent::for_effect_actor(
                (&authorization).into(),
                &staged,
                Uuid::new_v4()
            )
            .is_err()
        );
        assert_eq!(
            before,
            sqlx::query_scalar::<_, String>("SELECT record_json FROM marketplace_installations")
                .fetch_one(&f.db.pool)
                .await?
        );
        let mutation = Uuid::new_v4();
        for _ in 0..2 {
            let intent = cancellation::OwnerCancelIntent::for_effect_actor(
                EffectActor::recovery(&authorization),
                &staged,
                mutation,
            )?;
            assert_eq!(
                cancellation::cancel(&f.db, intent, budget())
                    .await?
                    .revision(),
                staged.revision + 1
            );
        }
        let flags: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extensions),(SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances),(SELECT CAST(CASE WHEN enabled OR automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants)").fetch_one(&f.db.pool).await?;
        assert_eq!(flags, (0, 0, 0));
        let private: i64 = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_generation_secrets)").fetch_one(&f.db.pool).await?;
        assert_eq!(private, 0);
        Ok(())
    }
    #[test]
    fn automatic_versions_never_accept_replay_downgrade_prerelease_or_build_only_change()
    -> Result<()> {
        newer_stable("1.0.0", "1.0.1")?;
        newer_stable("1.0.1-rc.1", "1.0.1")?;
        for candidate in ["1.0.0", "0.9.9", "1.1.0-rc.1", "1.0.0+new", "latest", ""] {
            assert!(newer_stable("1.0.0", candidate).is_err(), "{candidate}");
        }
        for candidate in ["1.0.0", "0.9.9", "1.1.0-rc.1", "1.0.0+new"] {
            assert!(
                newer_stable("1.0.0", candidate)
                    .unwrap_err()
                    .downcast_ref::<NoNewStableRelease>()
                    .is_some()
            );
        }
        for candidate in ["latest", ""] {
            assert!(
                newer_stable("1.0.0", candidate)
                    .unwrap_err()
                    .downcast_ref::<NoNewStableRelease>()
                    .is_none()
            );
        }
        Ok(())
    }
}
