//! Finite continuation of a freshly authenticated source operation. This is
//! NOT Actor: it cannot approve a grant, stage, set credentials or activate.
//! Routine access/CSRF refresh does not cancel input already accepted by the
//! owner. Logout, session/profile/home/authorization loss still stops it.
use super::*;

#[derive(Clone)]
pub(in crate::marketplace) struct SourceAuthorization {
    actor: Actor,
    expires: DateTime<Utc>,
    monotonic_expires: tokio::time::Instant,
}
impl SourceAuthorization {
    pub(in crate::marketplace) async fn capture(
        database: &Database,
        principal: &CurrentPrincipal,
    ) -> Result<Self> {
        let actor = Actor::from_principal(principal, false)?;
        let now = Utc::now();
        let started = Instant::now();
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let access = actor.authenticate(&mut tx, now).await?;
        crate::marketplace::preferences::require_home_binding(&mut tx, actor.home, database.driver)
            .await?;
        let session: String =
            sqlx::query_scalar("SELECT CAST(expires_at AS TEXT) FROM account_sessions WHERE id=$1")
                .bind(actor.session.to_string())
                .fetch_one(&mut *tx)
                .await?;
        let expires = crate::auth::sessions::parse_timestamp(&session)?
            .min(principal.session_expires_at)
            .min(
                now + chrono::Duration::seconds(
                    elixir_marketplace::quarantine::MAX_JOB_SECONDS as i64,
                ),
            );
        ensure_before_expiry(access, now, started)?;
        let remaining = (expires - Utc::now())
            .to_std()
            .context("original source session expired")?;
        let monotonic_expires = tokio::time::Instant::now() + remaining;
        tx.commit().await?;
        let value = Self {
            actor,
            expires,
            monotonic_expires,
        };
        value.check()?;
        Ok(value)
    }
    pub(in crate::marketplace) fn coordinates(&self) -> (Uuid, Uuid) {
        (self.actor.user, self.actor.home)
    }
    pub(in crate::marketplace) fn check(&self) -> Result<()> {
        if Utc::now() >= self.expires || tokio::time::Instant::now() >= self.monotonic_expires {
            bail!("original source operation authorization expired");
        }
        Ok(())
    }
    pub(in crate::marketplace) fn valid_for(&self) -> std::time::Duration {
        if self.check().is_err() {
            return std::time::Duration::ZERO;
        }
        (self.expires - Utc::now())
            .to_std()
            .unwrap_or_default()
            .min(
                self.monotonic_expires
                    .saturating_duration_since(tokio::time::Instant::now()),
            )
    }
    pub(in crate::marketplace) async fn authenticate(
        &self,
        tx: &mut Transaction<'_, Any>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        self.check()?;
        let a = &self.actor;
        let profile:Option<String>=sqlx::query_scalar("UPDATE profiles SET updated_at=updated_at WHERE id=$1 AND home_id=$2 AND user_id=$3 AND profile_type='account' RETURNING id")
            .bind(a.profile.to_string()).bind(a.home.to_string()).bind(a.user.to_string()).fetch_optional(&mut **tx).await?;
        let owner:Option<String>=sqlx::query_scalar("UPDATE home_members SET updated_at=updated_at WHERE home_id=$1 AND user_id=$2 AND role='owner' AND status='active' RETURNING id")
            .bind(a.home.to_string()).bind(a.user.to_string()).fetch_optional(&mut **tx).await?;
        let revision:Option<i64>=sqlx::query_scalar("UPDATE profile_authorization_revisions SET revision=revision WHERE profile_id=$1 AND home_id=$2 AND revision=$3 RETURNING revision")
            .bind(a.profile.to_string()).bind(a.home.to_string()).bind(a.authorization_revision).fetch_optional(&mut **tx).await?;
        // A refreshed session may rotate CSRF and extend its own login expiry,
        // but never extends this ORIGINAL job's fixed wall/monotonic deadline.
        let session:Option<String>=sqlx::query_scalar("UPDATE account_sessions SET last_seen_at=last_seen_at WHERE id=$1 AND user_id=$2 AND home_id=$3 AND active_profile_id=$4 AND revoked_at IS NULL RETURNING CAST(expires_at AS TEXT)")
            .bind(a.session.to_string()).bind(a.user.to_string()).bind(a.home.to_string()).bind(a.profile.to_string()).fetch_optional(&mut **tx).await?;
        let expires = session
            .as_deref()
            .map(crate::auth::sessions::parse_timestamp)
            .transpose()?
            .context("original source session is no longer current")?
            .min(self.expires);
        if profile.is_none() || owner.is_none() || revision.is_none() || expires <= now {
            bail!("original source owner/profile authorization changed");
        }
        self.check()?;
        Ok(expires)
    }
    pub(in crate::marketplace) async fn current(&self, database: &Database) -> Result<()> {
        let now = Utc::now();
        let started = Instant::now();
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let expires = self.authenticate(&mut tx, now).await?;
        crate::marketplace::preferences::require_home_binding(
            &mut tx,
            self.actor.home,
            database.driver,
        )
        .await?;
        self.check()?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
