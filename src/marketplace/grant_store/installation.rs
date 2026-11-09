//! Explicit owner staging, not activation. Only an actual verified archive and
//! a fresh selected discovery/source owner can enroll a disabled candidate.
use super::*;
use crate::marketplace::{
    archive::ArchivedSource,
    search::Selection,
    worker::{MetadataObservation, PublicSourceObservation},
};
use elixir_marketplace::retained_authority::AuthorityRecord;
use serde::{Deserialize, Serialize};

pub(in crate::marketplace) mod activation;
pub(in crate::marketplace) mod automatic;
pub(in crate::marketplace) mod cancellation;
mod choices;
mod configured;
mod expectation;
mod request;
pub(in crate::marketplace) use expectation::{CapturedTarget, ExpectedControls, InstallAction};
pub(in crate::marketplace) use request::StageRequest;
#[cfg(unix)]
pub(in crate::marketplace) mod operation;
pub(in crate::marketplace) mod providers;
mod record;
pub(in crate::marketplace) mod runtime;
pub(in crate::marketplace) mod sharing;
pub(in crate::marketplace) mod standing;
mod store;
pub(in crate::marketplace) mod targets;
pub(in crate::marketplace) use configured::InstallTarget;
use record::{Capsule, Journal, MAX_REVISION, Material, ROW_BYTES, Stored};

/// Explicit main-DB metadata allocation, not an increase to ordinary worker
/// limits or a physical database/WAL quota. Archive/runtime state is separate.
#[derive(Clone, Copy)]
pub(in crate::marketplace) struct InstallBudget {
    pub entries: usize,
    pub bytes: u64,
}
impl InstallBudget {
    pub(in crate::marketplace) fn validate(self) -> Result<()> {
        if !(1..=4096).contains(&self.entries)
            || self.bytes <= ROW_BYTES as u64
            || self.bytes > 1024 * 1024 * 1024
        {
            bail!("invalid owner installation metadata budget");
        }
        Ok(())
    }
}

/// No deserializer/clone and no conversion from permission approval. Creating
/// this intent does not commit anything; stage authenticates the actual owner
/// and exact selected source again through its durable transaction.
pub(in crate::marketplace) struct OwnerInstallIntent {
    configured: Option<configured::Target>,
    expected_controls: Option<ExpectedControls>,
    actor: automatic::EffectActor,
    review: Digest,
    release: Digest,
    grant_revision: i64,
    expected_revision: i64,
    mutation: Uuid,
}
impl OwnerInstallIntent {
    pub(in crate::marketplace) fn new_captured(
        principal: &CurrentPrincipal,
        review: &GrantReview,
        grant_revision: i64,
        captured: &CapturedTarget,
    ) -> Result<Self> {
        let actor = Actor::from_principal(principal, !review.scope().secret_keys().is_empty())?;
        Self::for_captured(actor.into(), review, grant_revision, captured)
    }
    pub(in crate::marketplace) fn new_automatic(
        authorization: &automatic::Authorization,
        review: &GrantReview,
        captured: &CapturedTarget,
    ) -> Result<Self> {
        Self::for_captured(
            authorization.into(),
            review,
            authorization.grant_revision(),
            captured,
        )
    }
    fn for_captured(
        actor: automatic::EffectActor,
        review: &GrantReview,
        grant_revision: i64,
        captured: &CapturedTarget,
    ) -> Result<Self> {
        let (target, expected_revision, mutation, controls) = captured.inputs(&actor, review)?;
        actor.require_target(
            review.scope().extension_id(),
            review.release_id(),
            match &target {
                InstallTarget::Existing { instance } => Some(*instance),
                InstallTarget::New { .. } => None,
            },
        )?;
        let mut intent =
            Self::for_effect_actor(actor, review, grant_revision, expected_revision, mutation)?;
        intent.configured = Some(configured::Target::new(
            &intent,
            target,
            review.scope().extension_id(),
        )?);
        intent.expected_controls = Some(controls);
        Ok(intent)
    }
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        review: &GrantReview,
        grant_revision: i64,
        expected_revision: i64,
        mutation: Uuid,
        target: InstallTarget,
    ) -> Result<Self> {
        let mut intent = Self::for_actor(
            Actor::from_principal(principal, !review.scope().secret_keys().is_empty())?,
            review,
            grant_revision,
            expected_revision,
            mutation,
        )?;
        intent.configured = Some(configured::Target::new(
            &intent,
            target,
            review.scope().extension_id(),
        )?);
        Ok(intent)
    }
    fn for_actor(
        actor: Actor,
        review: &GrantReview,
        grant_revision: i64,
        expected_revision: i64,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_effect_actor(
            actor.into(),
            review,
            grant_revision,
            expected_revision,
            mutation,
        )
    }
    fn for_effect_actor(
        actor: automatic::EffectActor,
        review: &GrantReview,
        grant_revision: i64,
        expected_revision: i64,
        mutation: Uuid,
    ) -> Result<Self> {
        actor.require_review(review)?;
        if !(1..=MAX_REVISION).contains(&grant_revision)
            || !(0..MAX_REVISION).contains(&expected_revision)
            || mutation.is_nil()
        {
            bail!("invalid explicit installation intent");
        }
        Ok(Self {
            configured: None,
            expected_controls: None,
            actor,
            review: review.review_id()?,
            release: review.release_id(),
            grant_revision,
            expected_revision,
            mutation,
        })
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_actor(
        actor: Actor,
        review: &GrantReview,
        grant_revision: i64,
        expected_revision: i64,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(actor, review, grant_revision, expected_revision, mutation)
    }
    fn request_id(&self, material: &Material) -> Result<Digest> {
        if let Some(target) = &self.configured {
            return target.request_id(self, material);
        }
        Ok(Digest::of(canonical::to_vec(
            &serde_json::json!({"domain":"elixir-marketplace/stage-install/v1",
            "user":self.actor.user,"home":self.actor.home,"review":self.review,"release":self.release,
            "grant_revision":self.grant_revision,"expected_revision":self.expected_revision,
            "mutation":self.mutation,"source":material.source.id()?}),
        )?))
    }
}

/// Receipt for a DISABLED persisted candidate. It is deliberately not a public
/// DTO, runtime ticket, source-byte lease or a flag that permits execution.
pub(in crate::marketplace) struct StagedInstallation {
    choices: Option<choices::Snapshot>,
    configured_instance: Option<Uuid>,
    extension: ExtensionId,
    revision: i64,
    instance: Uuid,
    release: Digest,
}
impl StagedInstallation {
    pub(in crate::marketplace) fn configured_instance(&self) -> Option<Uuid> {
        self.configured_instance
    }
    pub(in crate::marketplace) fn revision(&self) -> i64 {
        self.revision
    }
    pub(in crate::marketplace) fn instance(&self) -> Uuid {
        self.instance
    }
    pub(in crate::marketplace) fn release(&self) -> Digest {
        self.release
    }
    pub(in crate::marketplace) fn extension(&self) -> &ExtensionId {
        &self.extension
    }
}

pub(in crate::marketplace) async fn stage(
    selected: &mut Selection,
    archive: &ArchivedSource,
    intent: OwnerInstallIntent,
    budget: InstallBudget,
) -> Result<StagedInstallation> {
    selected.maintain().await?;
    let material = Material::from_archive(archive, selected.metadata()?)?;
    let public = selected.public_source()?;
    stage_checked(
        selected.client().source_database(),
        selected.metadata()?,
        &public,
        material,
        intent,
        budget,
    )
    .await
}

async fn stage_checked(
    database: &Database,
    metadata: &dyn crate::marketplace::selected_source::SelectedSource,
    public: &PublicSourceObservation,
    material: Material,
    intent: OwnerInstallIntent,
    budget: InstallBudget,
) -> Result<StagedInstallation> {
    stage_borrowed(database, metadata, public, material, &intent, budget).await
}

async fn stage_borrowed(
    database: &Database,
    metadata: &dyn crate::marketplace::selected_source::SelectedSource,
    public: &PublicSourceObservation,
    material: Material,
    intent: &OwnerInstallIntent,
    budget: InstallBudget,
) -> Result<StagedInstallation> {
    budget.validate()?;
    let current = || material.current(metadata, public);
    current()?;
    metadata.require_actor(intent.actor.user, intent.actor.home)?;
    if intent.review != material.review.review_id()?
        || intent.release != material.review.release_id()
    {
        bail!("installation intent differs from the verified source review");
    }
    let request = intent.request_id(&material)?;
    let started = Instant::now();
    let now = Utc::now();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let expires = intent.actor.authenticate(&mut tx, now).await?;
    // Unlike settings/permission previews, persistent installation cannot infer
    // a home from single-home setup. Its standing runtime needs this same
    // committed binding after the browser session and discovery receipt expire.
    crate::marketplace::preferences::require_home_binding(
        &mut tx,
        intent.actor.home,
        database.driver,
    )
    .await?;
    let grant_query = "SELECT extension_id,home_id,revision,revocation_revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1";
    let grant_query = if database.driver == DatabaseDriver::Postgres {
        format!("{grant_query} FOR SHARE")
    } else {
        grant_query.to_owned()
    };
    let row = sqlx::query(&grant_query)
        .bind(material.review.scope().extension_id().as_str())
        .fetch_optional(&mut *tx)
        .await?
        .context("installation grant is missing")?;
    let revoked: i64 = row.try_get("revocation_revision")?;
    let grant = StoredGrant::parse(row)?
        .active()
        .context("installation grant is disabled")?;
    if revoked < 0
        || revoked > grant.revision
        || grant.home_id != intent.actor.home
        || grant.revision != intent.grant_revision
        || !grant.covers(&material.review)?
    {
        bail!("installation grant changed or does not cover this exact request");
    }
    let ledger = store::lock_budget(&mut tx, budget).await?;
    let previous = store::load_configured(
        &mut tx,
        database.driver,
        material.review.scope().extension_id(),
        intent.configured.as_ref().map(|target| target.instance()),
    )
    .await?;
    // Lock order: grant -> installation budget/journal -> security -> instances.
    // Never invert journal/security locks with future installed SDK effects.
    metadata
        .require_in_transaction(&mut tx, database.driver)
        .await?;
    current()?;
    if let Some(existing) = &previous {
        if existing.journal.home != intent.actor.home {
            bail!("installation belongs to a different home");
        }
        if existing.journal.activation.is_some() {
            bail!("activation is already fenced; staging cannot replace it");
        }
        if existing.journal.mutation == intent.mutation {
            if existing.journal.request != request
                || existing.journal.approved_by != intent.actor.user
            {
                bail!("installation mutation ID reused with different input");
            }
            let pending = existing
                .journal
                .pending
                .as_ref()
                .context("installation request is no longer staged")?;
            if pending.grant_revision != grant.revision
                || pending.grant_revision < existing.floor
                || pending.source.id()? != material.source.id()?
            {
                bail!("staged installation authority differs");
            }
            crate::marketplace::security_state::retained::require_in_transaction(
                &mut tx,
                database.driver,
                &pending.facts()?,
            )
            .await?;
            store::verify_instances(&mut tx, database.driver, &existing.journal).await?;
            current()?;
            ensure_before_expiry(expires, now, started)?;
            let receipt = existing.receipt()?;
            tx.commit().await?;
            return Ok(receipt);
        }
        if existing.journal.revision != intent.expected_revision
            || existing.journal.pending.is_some()
        {
            bail!("installation revision changed or another candidate is staged");
        }
        if grant.revision < existing.floor {
            bail!("installation approval predates revocation");
        }
        store::verify_instances(&mut tx, database.driver, &existing.journal).await?;
    } else if intent.expected_revision != 0 {
        bail!("installation journal is missing; no implicit repair");
    }
    let id = material.review.scope().extension_id().clone();
    if let Some(expected) = intent.expected_controls {
        let target = intent
            .configured
            .as_ref()
            .context("captured controls require configured staging")?;
        expected
            .require(
                &mut tx,
                database.driver,
                intent.actor.home,
                &id,
                target.existing_instance(),
            )
            .await?;
    }
    if let Some(target) = &intent.configured {
        configured::prepare(
            &mut tx,
            database.driver,
            &intent,
            &material,
            previous.as_ref(),
            target,
        )
        .await?;
    } else {
        store::prepare_extension(&mut tx, &material, previous.as_ref()).await?;
    }
    let instance = Uuid::new_v4();
    if intent.configured.is_none() {
        sqlx::query("INSERT INTO extension_instances (instance_id,extension_id,instance_name,enabled) VALUES ($1,$2,$3,FALSE)")
            .bind(instance.to_string()).bind(id.as_str()).bind(format!("marketplace-{instance}")).execute(&mut *tx).await?;
    }
    super::sdk::namespaces::initialize(
        &mut tx,
        database.driver,
        super::sdk::namespaces::Coordinates {
            home: intent.actor.home,
            extension: &id,
            instance: intent
                .configured
                .as_ref()
                .map_or(instance, |target| target.instance()),
            namespace: instance,
        },
        super::sdk::namespaces::Purpose::Installed,
    )
    .await?;
    let next = intent
        .expected_revision
        .checked_add(1)
        .filter(|r| *r <= MAX_REVISION)
        .context("installation revision exhausted")?;
    let (active, old) = match previous.as_ref() {
        Some(p) => (p.journal.active.clone(), p.journal.previous.clone()),
        None => (None, None),
    };
    let floor = revoked.max(1);
    let choices = if let Some(target) = &intent.configured {
        Some(
            choices::capture(
                &mut tx,
                database.driver,
                intent.actor.home,
                &id,
                target.instance(),
            )
            .await?,
        )
    } else {
        None
    };
    let journal = Journal {
        choices,
        schema_version: if intent.configured.is_some() { 2 } else { 1 },
        configured_instance: intent.configured.as_ref().map(|target| target.instance()),
        extension: id.clone(),
        home: intent.actor.home,
        revision: next,
        mutation: intent.mutation,
        request,
        approved_by: intent.actor.user,
        active,
        previous: old,
        pending: Some(Box::new(Capsule {
            authority: material.authority,
            source: material.source,
            review: intent.review,
            grant_revision: grant.revision,
            instance,
            build: material.build,
        })),
        cancelled: None,
        activation: None,
    };
    // Leave room for the bounded preparation/start fence, so a newly staged
    // record cannot fill its entire JSON allocation before activation begins.
    if canonical::to_vec(&journal)?.len() > record::MAX_RECORD_BYTES - 1024 {
        bail!("staged installation lacks activation metadata headroom");
    }
    let written = store::write(&mut tx, ledger, previous.as_ref(), journal, floor, budget).await?;
    if written.journal.configured_instance.is_some() {
        store::verify_instances(&mut tx, database.driver, &written.journal).await?;
    }
    // Record was built from the same checked source; keep protection/TTL checks
    // around commit without treating any post-commit error as permission to retry
    // with another instance. Exact mutation retries recover the persisted one.
    metadata.authority_record()?;
    public.require_matches_source(
        &written.journal.pending.as_ref().unwrap().descriptor()?,
        written.journal.pending.as_ref().unwrap().source.summary(),
    )?;
    ensure_before_expiry(expires, now, started)?;
    let receipt = written.receipt()?;
    tx.commit().await?;
    Ok(receipt)
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(in crate::marketplace) use tests::compiled::stage_and_reload_compiled;
