//! Durable effect barrier and safe PRE-migration abort. Coordinates/flags are
//! never native-stop, state-snapshot, readiness, activation or serving proof.
use super::super::runtime::Generation;
use super::*;

/// Fresh authenticated journal coordinates, NOT a native-stop capability.
/// Only the separately owned installed controller can quiesce this source.
pub(in crate::marketplace) struct CutoverScope {
    reference: ActivationReference,
    home: Uuid,
    active: Option<Generation>,
    budget: InstallBudget,
}
impl CutoverScope {
    pub(super) fn from_stored(
        expected: &ActivationReference,
        stored: &Stored,
        budget: InstallBudget,
    ) -> Result<Self> {
        let reference = reference(stored)?;
        let active = stored
            .journal
            .active
            .as_ref()
            .map(|capsule| {
                Generation::from_capsule(
                    stored.journal.home,
                    &stored.journal.extension,
                    stored.journal.configured_instance,
                    capsule,
                )
            })
            .transpose()?;
        let scope = Self {
            reference,
            home: stored.journal.home,
            active,
            budget,
        };
        scope.require(stored)?;
        let actual = &scope.reference;
        if actual.extension != expected.extension
            || actual.configured_instance != expected.configured_instance
            || actual.release != expected.release
            || actual.instance != expected.instance
            || actual.revision != expected.revision
            || actual.attempt != expected.attempt
            || actual.plan != expected.plan
        {
            bail!("cutover scope differs from the exact quiescence reference");
        }
        Ok(scope)
    }
    pub(in crate::marketplace::grant_store::installation) fn extension(&self) -> &ExtensionId {
        &self.reference.extension
    }
    pub(in crate::marketplace::grant_store::installation) fn configured_instance(
        &self,
    ) -> Option<Uuid> {
        self.reference.configured_instance
    }
    pub(in crate::marketplace::grant_store::installation) fn active(&self) -> Option<&Generation> {
        self.active.as_ref()
    }
    pub(in crate::marketplace::grant_store::installation) fn require(
        &self,
        stored: &Stored,
    ) -> Result<()> {
        let actual = reference(stored)?;
        let expected = &self.reference;
        let active = stored
            .journal
            .active
            .as_ref()
            .map(|capsule| {
                Generation::from_capsule(
                    stored.journal.home,
                    &stored.journal.extension,
                    stored.journal.configured_instance,
                    capsule,
                )
            })
            .transpose()?;
        let fence = stored
            .journal
            .activation
            .as_ref()
            .context("cutover fence missing")?;
        // The original quiescing borrow remains valid through ONLY its exact
        // copy transition. Copying cannot enable/start another generation.
        let revision_matches = match fence.phase {
            ActivationPhase::Quiescing => actual.revision == expected.revision,
            ActivationPhase::StateCopied => {
                actual.revision == expected.revision
                    || expected.revision == fence.staged_revision + 4
                        && actual.revision == expected.revision + 1
            }
            _ => false,
        };
        if stored.journal.home != self.home
            || !revision_matches
            || actual.extension != expected.extension
            || actual.configured_instance != expected.configured_instance
            || actual.release != expected.release
            || actual.instance != expected.instance
            || actual.attempt != expected.attempt
            || actual.plan != expected.plan
            || active != self.active
        {
            bail!("cutover lost the exact durable old-generation barrier");
        }
        Ok(())
    }
    pub(in crate::marketplace::grant_store::installation) async fn current(
        &self,
        database: &Database,
    ) -> Result<()> {
        let mut connection = crate::marketplace::durability::connection(database).await?;
        let mut tx = connection.begin().await?;
        synchronous(&mut tx, database.driver).await?;
        crate::marketplace::preferences::require_home_binding(&mut tx, self.home, database.driver)
            .await?;
        store::lock_grant_binding(&mut tx, database.driver, self.home, self.extension()).await?;
        store::read_budget(&mut tx, database.driver, self.budget).await?;
        let stored = store::load_configured_shared(
            &mut tx,
            database.driver,
            self.extension(),
            self.reference.configured_instance,
        )
        .await?
        .context("cutover journal is missing; no implicit repair")?;
        self.require(&stored)?;
        tx.rollback().await?;
        Ok(())
    }
}

pub(in crate::marketplace) async fn observe_cutover(
    selected: &mut Selection,
    archive: &ArchivedSource,
    intent: OwnerActivationIntent,
    quiescing: &ActivationReference,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<CutoverScope> {
    selected.maintain().await?;
    let public = selected.public_source()?;
    observe_checked(
        Step {
            database: selected.client().source_database(),
            metadata: selected.metadata()?,
            public: &public,
            material: Material::from_archive(archive, selected.metadata()?)?,
            intent: Arc::new(intent),
            plan,
            budget,
        },
        quiescing,
    )
    .await
}
pub(super) async fn observe_checked(
    step: Step<'_>,
    quiescing: &ActivationReference,
) -> Result<CutoverScope> {
    match step.transition(Action::ObserveCutover(quiescing)).await? {
        Transition::CutoverScope(scope) => Ok(scope),
        _ => bail!("cutover observation did not retain the exact quiescing attempt"),
    }
}

pub(in crate::marketplace) async fn quiesce(
    selected: &mut Selection,
    archive: &ArchivedSource,
    intent: OwnerActivationIntent,
    source_prepared: &ActivationReference,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<ActivationReference> {
    selected.maintain().await?;
    let public = selected.public_source()?;
    quiesce_checked(
        Step {
            database: selected.client().source_database(),
            metadata: selected.metadata()?,
            public: &public,
            material: Material::from_archive(archive, selected.metadata()?)?,
            intent: Arc::new(intent),
            plan,
            budget,
        },
        source_prepared,
    )
    .await
}
pub(super) async fn quiesce_checked(
    step: Step<'_>,
    source_prepared: &ActivationReference,
) -> Result<ActivationReference> {
    Ok(quiesce_retained(step, source_prepared)
        .await?
        .into_reference())
}
pub(super) async fn quiesce_retained(
    step: Step<'_>,
    source_prepared: &ActivationReference,
) -> Result<pre_candidate::PreparationResult> {
    match step.transition(Action::Quiesce(source_prepared)).await? {
        Transition::Quiescing(output) => Ok(output),
        _ => bail!("quiescence did not retain its exact source-prepared attempt"),
    }
}
pub(super) fn require_source_prepared(
    expected: &ActivationReference,
    stored: &Stored,
) -> Result<()> {
    let actual = reference(stored)?;
    let fence = stored
        .journal
        .activation
        .as_ref()
        .context("source preparation is not fenced")?;
    let source_revision = fence
        .staged_revision
        .checked_add(3)
        .context("source preparation revision overflow")?;
    if !matches!(
        fence.phase,
        ActivationPhase::SourcePrepared | ActivationPhase::Quiescing
    ) || expected.revision != source_revision
        || actual.extension != expected.extension
        || actual.configured_instance != expected.configured_instance
        || actual.release != expected.release
        || actual.instance != expected.instance
        || actual.attempt != expected.attempt
        || actual.plan != expected.plan
    {
        bail!("quiescence differs from the exact source-prepared attempt");
    }
    Ok(())
}

/// A separate owner request. No deserializer/Clone or conversion from a grant
/// or status flag. Aborting before state migration never enables an instance.
pub(in crate::marketplace) struct OwnerResumeIntent {
    actor: Actor,
    reference: ActivationReference,
    mutation: Uuid,
}
impl OwnerResumeIntent {
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        reference: &ActivationReference,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(
            Actor::from_principal(principal, false)?,
            reference,
            mutation,
        )
    }
    fn for_actor(actor: Actor, reference: &ActivationReference, mutation: Uuid) -> Result<Self> {
        if mutation.is_nil()
            || reference.instance.is_nil()
            || reference
                .configured_instance
                .is_some_and(|owner| owner.is_nil() || owner == reference.instance)
            || reference.attempt.is_nil()
            || !(1..MAX_REVISION).contains(&reference.revision)
        {
            bail!("invalid pre-migration resume request");
        }
        Ok(Self {
            actor,
            mutation,
            reference: ActivationReference {
                configured_instance: reference.configured_instance,
                extension: reference.extension.clone(),
                release: reference.release,
                instance: reference.instance,
                revision: reference.revision,
                attempt: reference.attempt,
                plan: reference.plan,
            },
        })
    }
    #[cfg(test)]
    pub(in crate::marketplace::grant_store::installation) fn fixture_for_actor(
        actor: Actor,
        reference: &ActivationReference,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(actor, reference, mutation)
    }
    fn request(&self) -> Result<Digest> {
        let r = &self.reference;
        let mut value = serde_json::json!({
            "domain":"elixir-marketplace/abort-quiescence/v1","user":self.actor.user,"home":self.actor.home,
            "extension":r.extension,"release":r.release,"instance":r.instance,"revision":r.revision,
            "attempt":r.attempt,"plan":r.plan,"mutation":self.mutation,
        });
        if let Some(owner) = r.configured_instance {
            value["domain"] = serde_json::json!("elixir-marketplace/abort-quiescence/v2");
            value["configured_instance"] = serde_json::json!(owner);
        }
        Ok(Digest::of(canonical::to_vec(&value)?))
    }
}

/// Removes ONLY the pre-migration effect barrier and, if present, its verified
/// untouched host-created copy. Never deletes original data, changes enabled
/// flags, erases native fences or declares native work stopped.
/// Standing grants/security still independently fence a disabled/revoked source.
pub(in crate::marketplace) async fn resume(
    database: &Database,
    intent: OwnerResumeIntent,
    budget: InstallBudget,
) -> Result<StagedInstallation> {
    budget.validate()?;
    let request = intent.request()?;
    let r = &intent.reference;
    let now = Utc::now();
    let started = Instant::now();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let expires = intent.actor.authenticate(&mut tx, now).await?;
    crate::marketplace::preferences::require_home_binding(
        &mut tx,
        intent.actor.home,
        database.driver,
    )
    .await?;
    // Cleanup must remain possible without discovery/issuer/public-source or a
    // healthy enabled grant-review JSON. These are not runtime authority.
    store::lock_grant_binding(&mut tx, database.driver, intent.actor.home, &r.extension).await?;
    let ledger = store::lock_budget(&mut tx, budget).await?;
    let old = store::load_configured(
        &mut tx,
        database.driver,
        &r.extension,
        r.configured_instance,
    )
    .await?
    .context("quiescence journal is missing; no repair")?;
    let pending = old
        .journal
        .pending
        .as_ref()
        .context("quiescence candidate is missing")?;
    if old.journal.home != intent.actor.home
        || pending.instance != r.instance
        || pending.facts()?.release().release_id() != r.release
    {
        bail!("resume belongs to a different home or candidate");
    }
    if old.journal.mutation == intent.mutation {
        if old.journal.activation.is_some()
            || old.journal.request != request
            || old.journal.approved_by != intent.actor.user
            || old.journal.revision != r.revision + 1
        {
            bail!("resume mutation was reused with different input");
        }
        store::verify_instances(&mut tx, database.driver, &old.journal).await?;
        let receipt = old.receipt()?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        return Ok(receipt);
    }
    let fence = old
        .journal
        .activation
        .as_ref()
        .context("no quiescence barrier to resume")?;
    let actual = reference(&old)?;
    if !matches!(
        fence.phase,
        ActivationPhase::Quiescing | ActivationPhase::StateCopied
    ) || actual.revision != r.revision
        || actual.extension != r.extension
        || actual.configured_instance != r.configured_instance
        || actual.release != r.release
        || actual.instance != r.instance
        || actual.attempt != r.attempt
        || actual.plan != r.plan
    {
        bail!("only the exact pre-migration quiescence can be resumed");
    }
    // Lock actual generations and prove candidate state is still disabled,
    // empty and dependency-free. No flags/counters substitute for native stop.
    store::verify_instances(&mut tx, database.driver, &old.journal).await?;
    if fence.phase == ActivationPhase::StateCopied {
        super::state_copy::discard(&mut tx, database.driver, &old.journal).await?;
    }
    let journal = Journal {
        choices: old.journal.choices,
        configured_instance: old.journal.configured_instance,
        schema_version: old.journal.schema_version,
        extension: old.journal.extension.clone(),
        home: old.journal.home,
        revision: r.revision + 1,
        mutation: intent.mutation,
        request,
        approved_by: intent.actor.user,
        active: old.journal.active.clone(),
        previous: old.journal.previous.clone(),
        pending: old.journal.pending.clone(),
        cancelled: None,
        activation: None,
    };
    let written = store::write(&mut tx, ledger, Some(&old), journal, old.floor, budget).await?;
    store::verify_instances(&mut tx, database.driver, &written.journal).await?;
    let receipt = written.receipt()?;
    ensure_before_expiry(expires, now, started)?;
    tx.commit().await?;
    Ok(receipt)
}
