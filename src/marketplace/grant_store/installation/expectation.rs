//! User-observed control versions, captured by one owned command BEFORE long
//! source work. These are optimistic-concurrency data, never execution proof.
use super::super::sdk::{RowLock, locked};
use super::*;

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::marketplace) enum ExpectedControls {
    // Empty struct deliberately: Serde's tagged unit variant accepts extra
    // fields despite deny_unknown_fields, which is not a closed wire input.
    Absent {},
    NewInstance {
        extension_revision: i64,
    },
    Existing {
        extension_revision: i64,
        instance_revision: i64,
    },
}
impl ExpectedControls {
    pub(in crate::marketplace) fn validate(self, existing: Option<Uuid>) -> Result<()> {
        let valid = |revision| (1..MAX_REVISION).contains(&revision);
        let matches = match self {
            Self::Absent {} => existing.is_none(),
            Self::NewInstance { extension_revision } => {
                existing.is_none() && valid(extension_revision)
            }
            Self::Existing {
                extension_revision,
                instance_revision,
            } => {
                existing.is_some_and(|id| !id.is_nil())
                    && valid(extension_revision)
                    && valid(instance_revision)
            }
        };
        if !matches {
            bail!("invalid original target control expectation");
        }
        Ok(())
    }
    pub(super) async fn require(
        self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
        home: Uuid,
        extension: &ExtensionId,
        existing: Option<Uuid>,
    ) -> Result<()> {
        self.validate(existing)?;
        if let Self::Existing {
            extension_revision,
            instance_revision,
        } = self
        {
            let actual = choices::capture(
                tx,
                driver,
                home,
                extension,
                existing.context("existing target missing")?,
            )
            .await?;
            if actual.revisions() != (extension_revision, instance_revision) {
                bail!("target choices changed since the original owner action");
            }
            return Ok(());
        }
        let query = locked(
            "SELECT extension_id FROM extensions WHERE extension_id=$1",
            driver,
            RowLock::Exclusive,
        );
        let parent: Option<String> = sqlx::query_scalar(&query)
            .bind(extension.as_str())
            .fetch_optional(&mut **tx)
            .await?;
        // PostgreSQL cannot FOR SHARE the nullable side of an outer join. The
        // parent extension lock already serializes its control writes; read
        // the marker/counter without introducing an inverted nullable lock.
        let marker = sqlx::query("SELECT m.home_id,COALESCE(c.revision,9007199254740991) AS revision FROM marketplace_installation_metadata m LEFT JOIN marketplace_extension_choices c ON c.extension_id=m.extension_id WHERE m.extension_id=$1")
            .bind(extension.as_str())
            .fetch_optional(&mut **tx)
            .await?;
        match (self, parent, marker) {
            (Self::Absent {}, None, None) => Ok(()),
            (Self::NewInstance { extension_revision }, Some(_), Some(row))
                if row.try_get::<String, _>("home_id")? == home.to_string()
                    && row.try_get::<i64, _>("revision")? == extension_revision =>
            {
                Ok(())
            }
            _ => bail!("target parent changed, is foreign/unmanaged, or needs control recovery"),
        }
    }
}

/// Input data held by the application task before its first await. A caller's
/// observed versions are checked, never silently replaced with newer controls.
#[derive(Clone, Serialize)]
pub(in crate::marketplace) struct InstallAction {
    #[serde(rename = "extension_id")]
    pub extension: ExtensionId,
    #[serde(rename = "release_id")]
    pub release: Digest,
    pub target: InstallTarget,
    pub expected_revision: i64,
    #[serde(rename = "mutation_id")]
    pub mutation: Uuid,
    pub controls: ExpectedControls,
}

/// No Clone/Deserialize or status factory. A later principal may renew the
/// same user's session, not replace its target/release/mutation or restamp OFF.
pub(in crate::marketplace) struct CapturedTarget {
    user: Uuid,
    home: Uuid,
    extension: ExtensionId,
    release: Digest,
    target: InstallTarget,
    expected_revision: i64,
    mutation: Uuid,
    controls: ExpectedControls,
    automatic: Option<Digest>,
}
impl CapturedTarget {
    pub(in crate::marketplace) async fn capture(
        database: &Database,
        principal: &CurrentPrincipal,
        action: InstallAction,
    ) -> Result<Self> {
        Self::capture_for_actor(
            database,
            Actor::from_principal(principal, false)?.into(),
            action,
        )
        .await
    }
    pub(in crate::marketplace) async fn capture_automatic(
        database: &Database,
        authorization: &automatic::Authorization,
        action: InstallAction,
    ) -> Result<Self> {
        authorization.require_target(
            &action.extension,
            action.release,
            match &action.target {
                InstallTarget::Existing { instance } => Some(*instance),
                InstallTarget::New { .. } => None,
            },
        )?;
        Self::capture_for_actor(database, authorization.into(), action).await
    }
    async fn capture_for_actor(
        database: &Database,
        actor: automatic::EffectActor,
        action: InstallAction,
    ) -> Result<Self> {
        let InstallAction {
            extension,
            release,
            target,
            expected_revision,
            mutation,
            controls,
        } = action;
        let existing = match &target {
            InstallTarget::New { name } => {
                if expected_revision != 0
                    || name.is_empty()
                    || name.len() > 128
                    || name.chars().any(char::is_control)
                {
                    bail!("invalid original new target");
                }
                None
            }
            InstallTarget::Existing { instance } => Some(*instance),
        };
        controls.validate(existing)?;
        if mutation.is_nil() || !(0..MAX_REVISION).contains(&expected_revision) {
            bail!("invalid original install action");
        }
        let now = Utc::now();
        let started = Instant::now();
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        let expires = actor.authenticate(&mut tx, now).await?;
        crate::marketplace::preferences::require_home_binding(&mut tx, actor.home, database.driver)
            .await?;
        if let Some(owner) = existing {
            // Same journal -> parent/control lock order as staging. Never
            // hold an instance parent while waiting for its journal writer.
            let old =
                store::load_configured_shared(&mut tx, database.driver, &extension, Some(owner))
                    .await?
                    .context("original configured target journal missing")?;
            if old.journal.home != actor.home
                || old.journal.revision != expected_revision
                || old.journal.pending.is_some()
                || old.journal.activation.is_some()
            {
                bail!("original configured target changed or already owns pending work");
            }
            actor.require_original(&old.journal)?;
        }
        controls
            .require(&mut tx, database.driver, actor.home, &extension, existing)
            .await?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        Ok(Self {
            user: actor.user,
            home: actor.home,
            extension,
            release,
            target,
            expected_revision,
            mutation,
            controls,
            automatic: actor.capture_id()?,
        })
    }
    pub(super) fn inputs(
        &self,
        actor: &automatic::EffectActor,
        review: &GrantReview,
    ) -> Result<(InstallTarget, i64, Uuid, ExpectedControls)> {
        actor.require_capture(self.automatic)?;
        if self.user != actor.user
            || self.home != actor.home
            || &self.extension != review.scope().extension_id()
            || self.release != review.release_id()
        {
            bail!("installation differs from the original user/target/release action");
        }
        let target = match &self.target {
            InstallTarget::New { name } => InstallTarget::New { name: name.clone() },
            InstallTarget::Existing { instance } => InstallTarget::Existing {
                instance: *instance,
            },
        };
        Ok((target, self.expected_revision, self.mutation, self.controls))
    }
}

#[cfg(test)]
mod tests;
