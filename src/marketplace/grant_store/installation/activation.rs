//! Durable preparation and one-way start claims, not runtime readiness or an
//! execution permit. Native admission, owned launch/cleanup and activation
//! commit are separate checks. A replay NEVER creates another start claim.
use super::*;
use record::{ActivationFence, ActivationPhase, OperationalMetadata};
use std::sync::Arc;
#[cfg(test)]
pub(in crate::marketplace) use tests::compiled_native::{CompiledUpdateFixtureState, run_compiled_activation_fixture};

pub(in crate::marketplace) mod candidate;
pub(in crate::marketplace) mod commit;
mod credentials;
pub(in crate::marketplace) use credentials::CredentialChange;
mod plan;
pub(super) mod pre_candidate;
#[cfg(unix)]
mod request;
#[cfg(unix)]
pub(in crate::marketplace) use request::ActivationRequest;
#[cfg(unix)]
pub(super) use request::UnstartedSourceStart;
pub(in crate::marketplace) mod quiescence;
pub(in crate::marketplace) mod retirement;
pub(in crate::marketplace) mod state_copy;
pub(in crate::marketplace) use plan::SourceActivationPlan;
#[cfg(unix)]
mod source;
#[cfg(unix)]
pub(in crate::marketplace::grant_store::installation) use source::PreparedSource;
#[cfg(unix)]
pub(in crate::marketplace::grant_store::installation) use source::{
    ActivationSource, DisposedSource, ProbedSource,
};
#[cfg(unix)]
pub(in crate::marketplace) use source::{SourceLaunch, SourcePhase};

/// One explicit owner decision for this staged source and current grant. No
/// deserializer/clone; permission approval alone never constructs this intent.
pub(in crate::marketplace) struct OwnerActivationIntent {
    choices: Option<super::choices::Snapshot>,
    actor: automatic::EffectActor,
    extension: ExtensionId,
    release: Digest,
    instance: Uuid,
    configured_instance: Option<Uuid>,
    staged_revision: i64,
    review: Digest,
    grant_revision: i64,
    mutation: Uuid,
    credentials: Option<credentials::Patch>,
}
impl OwnerActivationIntent {
    pub(in crate::marketplace) fn new_with_credentials(
        principal: &CurrentPrincipal,
        staged: &StagedInstallation,
        review: &GrantReview,
        grant_revision: i64,
        mutation: Uuid,
        secrets: &SecretsManager,
        changes: Vec<CredentialChange>,
    ) -> Result<Self> {
        Self::for_actor(
            Actor::from_principal(principal, true)?,
            staged,
            review,
            grant_revision,
            mutation,
        )?
        .attach_credentials(secrets, review, changes)
    }
    fn attach_credentials(
        mut self,
        manager: &SecretsManager,
        review: &GrantReview,
        changes: Vec<CredentialChange>,
    ) -> Result<Self> {
        if self.credentials.is_some() {
            bail!("credential input already attached to this owner intent");
        }
        self.credentials = Some(credentials::Patch::prepare(
            manager, &self, review, changes,
        )?);
        Ok(self)
    }
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        staged: &StagedInstallation,
        review: &GrantReview,
        grant_revision: i64,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(
            Actor::from_principal(principal, !review.scope().secret_keys().is_empty())?,
            staged,
            review,
            grant_revision,
            mutation,
        )
    }
    pub(super) fn for_actor(
        actor: Actor,
        staged: &StagedInstallation,
        review: &GrantReview,
        grant_revision: i64,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_effect_actor(actor.into(), staged, review, grant_revision, mutation)
    }
    pub(in crate::marketplace) fn automatic(
        authorization: &automatic::Authorization,
        staged: &StagedInstallation,
        review: &GrantReview,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_effect_actor(
            authorization.into(),
            staged,
            review,
            authorization.grant_revision(),
            mutation,
        )
    }
    fn for_effect_actor(
        actor: automatic::EffectActor,
        staged: &StagedInstallation,
        review: &GrantReview,
        grant_revision: i64,
        mutation: Uuid,
    ) -> Result<Self> {
        actor.require_review(review)?;
        actor.require_target(
            &staged.extension,
            staged.release,
            staged.configured_instance,
        )?;
        match (staged.configured_instance, staged.choices) {
            (Some(_), Some(choices)) => choices.validate()?,
            (None, None) => {}
            _ => bail!("configured activation needs its original fresh staged choice snapshot"),
        }
        if !(1..MAX_REVISION - 2).contains(&staged.revision)
            || staged
                .configured_instance
                .is_some_and(|owner| owner.is_nil() || owner == staged.instance)
            || !(1..=MAX_REVISION).contains(&grant_revision)
            || staged.instance.is_nil()
            || mutation.is_nil()
            || staged.release != review.release_id()
            || staged.extension != *review.scope().extension_id()
        {
            bail!("invalid explicit activation intent");
        }
        Ok(Self {
            choices: staged.choices,
            actor,
            extension: staged.extension.clone(),
            release: staged.release,
            instance: staged.instance,
            configured_instance: staged.configured_instance,
            staged_revision: staged.revision,
            review: review.review_id()?,
            grant_revision,
            mutation,
            credentials: None,
        })
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_actor(
        actor: Actor,
        staged: &StagedInstallation,
        review: &GrantReview,
        grant_revision: i64,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(actor, staged, review, grant_revision, mutation)
    }
    fn request_id(&self, source: Digest, plan: Digest) -> Result<Digest> {
        let mut value = serde_json::json!({
            "domain":"elixir-marketplace/prepare-activation/v1",
            "user":self.actor.user,"home":self.actor.home,
            "extension":self.extension,"release":self.release,"instance":self.instance,
            "staged_revision":self.staged_revision,"review":self.review,
            "grant_revision":self.grant_revision,"mutation":self.mutation,
            "source":source,"plan":plan
        });
        if let Some(owner) = self.configured_instance {
            value["domain"] = serde_json::json!("elixir-marketplace/prepare-activation/v2");
            value["configured_instance"] = serde_json::json!(owner);
        }
        if let Some(credentials) = &self.credentials {
            value["domain"] = serde_json::json!("elixir-marketplace/prepare-activation/v3");
            value["credential_request"] = serde_json::json!(credentials.id());
        }
        if let Some(choices) = self.choices {
            value["domain"] = serde_json::json!("elixir-marketplace/prepare-activation/v4");
            value["choices"] = serde_json::to_value(choices)?;
        }
        Ok(Digest::of(canonical::to_vec(&value)?))
    }
    async fn require_choices(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
        journal: &Journal,
    ) -> Result<()> {
        if journal.choices != self.choices {
            bail!("activation choice snapshot differs from its original staging");
        }
        if let Some(owner) = self.configured_instance {
            self.choices
                .context("configured activation has no choice snapshot")?
                .require(tx, driver, self.actor.home, &self.extension, owner)
                .await?;
        } else if self.choices.is_some() {
            bail!("legacy activation cannot adopt configured choices");
        }
        Ok(())
    }
}

/// Recovery coordinates only. No Deserialize/Clone or conversion to a native
/// launch, source-byte, runtime or serving capability.
pub(in crate::marketplace) struct ActivationReference {
    pub(super) configured_instance: Option<Uuid>,
    pub(super) extension: ExtensionId,
    pub(super) release: Digest,
    pub(super) instance: Uuid,
    pub(super) revision: i64,
    pub(super) attempt: Uuid,
    pub(super) plan: Digest,
}
impl ActivationReference {
    pub(in crate::marketplace) fn revision(&self) -> i64 {
        self.revision
    }
    pub(in crate::marketplace) fn attempt(&self) -> Uuid {
        self.attempt
    }
}
pub(in crate::marketplace) struct PreparedActivation {
    reference: ActivationReference,
}
impl PreparedActivation {
    pub(super) fn reference(&self) -> &ActivationReference {
        &self.reference
    }
    pub(in crate::marketplace) fn revision(&self) -> i64 {
        self.reference.revision
    }
    pub(in crate::marketplace) fn attempt(&self) -> Uuid {
        self.reference.attempt
    }
}
/// Returned ONLY by the first committed Prepared -> Starting transition.
/// There is no Clone/Deserialize/reload constructor. Actual native code must
/// additionally own the global source lane and its existing write-ahead fence;
/// this claim alone neither starts code nor proves readiness/cleanup.
pub(in crate::marketplace) struct ActivationStart {
    starting_journal: Digest,
    reference: ActivationReference,
    // Shared request DATA only, not another start claim. The non-cloneable
    // claim retains the exact original actor/input for fresh native checks.
    intent: Arc<OwnerActivationIntent>,
}
impl ActivationStart {
    pub(in crate::marketplace) fn native_check(&self) -> NativeStartCheck {
        let r = &self.reference;
        NativeStartCheck {
            starting_journal: self.starting_journal,
            reference: ActivationReference {
                configured_instance: r.configured_instance,
                extension: r.extension.clone(),
                release: r.release,
                instance: r.instance,
                revision: r.revision,
                attempt: r.attempt,
                plan: r.plan,
            },
            intent: self.intent.clone(),
        }
    }
    pub(in crate::marketplace) fn revision(&self) -> i64 {
        self.reference.revision
    }
    pub(in crate::marketplace) fn attempt(&self) -> Uuid {
        self.reference.attempt
    }
    pub(in crate::marketplace) fn deployment(&self) -> String {
        format!("activation-{}", self.reference.attempt.simple())
    }
}
/// Read-only authority coordinates. This cannot open a native job or mint a
/// claim; only the original ActivationStart can enter the opening boundary.
pub(in crate::marketplace) struct NativeStartCheck {
    starting_journal: Digest,
    reference: ActivationReference,
    intent: Arc<OwnerActivationIntent>,
}
pub(in crate::marketplace) enum SourceOpening {
    // Returned only for a typed admission deferral BEFORE native provisioning.
    Deferred(
        ActivationStart,
        crate::marketplace::worker::SourceResourceWait,
    ),
    Opened,
}
/// Borrowed controller inputs, never a deserializable/public request. The job
/// slot and original start claim remain separately owned through native opens.
pub(in crate::marketplace) struct SourceOpen<'a> {
    pub metadata: &'a dyn crate::marketplace::selected_source::SelectedSource,
    pub public: &'a PublicSourceObservation,
    pub archive: &'a ArchivedSource,
    pub plan: &'a SourceActivationPlan,
    pub budget: InstallBudget,
}
pub(in crate::marketplace) enum PreparedOrRecovery {
    Prepared(PreparedActivation),
    Recovery(ActivationReference),
}
pub(in crate::marketplace) enum StartOrRecovery {
    Start(ActivationStart),
    Recovery(ActivationReference),
}
pub(in crate::marketplace) enum ActivationStatus {
    Prepared(PreparedActivation),
    Starting(ActivationReference),
    SourcePrepared(ActivationReference),
    Quiescing(ActivationReference),
    StateCopied(ActivationReference),
    CandidateStarting(ActivationReference),
}

/// Authenticated recovery/status, not admission. Works when source, issuer,
/// credentials or grant-review JSON are unavailable. Starting is never turned
/// back into a launch claim or an unused-candidate cleanup acknowledgement.
pub(in crate::marketplace) async fn inspect(
    database: &Database,
    principal: &CurrentPrincipal,
    id: &ExtensionId,
    budget: InstallBudget,
) -> Result<ActivationStatus> {
    inspect_as(
        database,
        Actor::from_principal(principal, false)?,
        id,
        budget,
    )
    .await
}
async fn inspect_as(
    database: &Database,
    actor: Actor,
    id: &ExtensionId,
    budget: InstallBudget,
) -> Result<ActivationStatus> {
    inspect_configured_as(database, actor, id, None, budget).await
}
pub(in crate::marketplace) async fn inspect_configured(
    database: &Database,
    principal: &CurrentPrincipal,
    id: &ExtensionId,
    instance: Uuid,
    budget: InstallBudget,
) -> Result<ActivationStatus> {
    if instance.is_nil() {
        bail!("configured owner is required");
    }
    inspect_configured_as(
        database,
        Actor::from_principal(principal, false)?,
        id,
        Some(instance),
        budget,
    )
    .await
}
async fn inspect_configured_as(
    database: &Database,
    actor: Actor,
    id: &ExtensionId,
    instance: Option<Uuid>,
    budget: InstallBudget,
) -> Result<ActivationStatus> {
    budget.validate()?;
    let now = Utc::now();
    let started = Instant::now();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let expires = actor.authenticate(&mut tx, now).await?;
    crate::marketplace::preferences::require_home_binding(&mut tx, actor.home, database.driver)
        .await?;
    store::lock_grant_binding(&mut tx, database.driver, actor.home, id).await?;
    store::lock_budget(&mut tx, budget).await?;
    let stored = store::load_configured(&mut tx, database.driver, id, instance)
        .await?
        .context("activation journal missing")?;
    if stored.journal.home != actor.home {
        bail!("activation belongs to a different home");
    }
    let phase = stored
        .journal
        .activation
        .as_ref()
        .context("no activation is fenced")?
        .phase;
    let reference = reference(&stored)?;
    // Prepared still means unused, so its metadata/empty state is verified.
    // Starting may have state or a process: do not infer cleanup from flags.
    if phase == ActivationPhase::Prepared {
        store::verify_instances(&mut tx, database.driver, &stored.journal).await?;
    }
    ensure_before_expiry(expires, now, started)?;
    tx.commit().await?;
    Ok(match phase {
        ActivationPhase::Prepared => ActivationStatus::Prepared(PreparedActivation { reference }),
        ActivationPhase::Starting => ActivationStatus::Starting(reference),
        ActivationPhase::SourcePrepared => ActivationStatus::SourcePrepared(reference),
        ActivationPhase::Quiescing => ActivationStatus::Quiescing(reference),
        ActivationPhase::StateCopied => ActivationStatus::StateCopied(reference),
        ActivationPhase::CandidateStarting => ActivationStatus::CandidateStarting(reference),
    })
}
enum Transition {
    Prepared(PreparedActivation),
    Start(ActivationStart),
    Recovery(ActivationReference),
    Authorized,
    #[cfg(unix)]
    SourcePrepared(ActivationReference),
    Quiescing(pre_candidate::PreparationResult),
    CutoverScope(quiescence::CutoverScope),
    StateCopied(pre_candidate::PreparationResult),
    Candidate(candidate::CandidateClaim),
}

pub(in crate::marketplace) async fn prepare(
    selected: &mut Selection,
    archive: &ArchivedSource,
    intent: OwnerActivationIntent,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<PreparedOrRecovery> {
    selected.maintain().await?;
    let material = Material::from_archive(archive, selected.metadata()?)?;
    let public = selected.public_source()?;
    prepare_checked(Step {
        database: selected.client().source_database(),
        metadata: selected.metadata()?,
        public: &public,
        material,
        intent: Arc::new(intent),
        plan,
        budget,
    })
    .await
}
async fn prepare_checked(step: Step<'_>) -> Result<PreparedOrRecovery> {
    match step.advance(None).await? {
        Transition::Prepared(p) => Ok(PreparedOrRecovery::Prepared(p)),
        Transition::Recovery(r) => Ok(PreparedOrRecovery::Recovery(r)),
        Transition::Start(_) | Transition::Authorized => {
            bail!("preparation cannot issue a native start claim")
        }
        #[cfg(unix)]
        Transition::SourcePrepared(_) => bail!("preparation cannot complete a source probe"),
        Transition::Quiescing(_) => bail!("preparation cannot fence an active generation"),
        Transition::CutoverScope(_) => bail!("preparation cannot acquire a cutover scope"),
        Transition::StateCopied(_) => bail!("preparation cannot copy state"),
        Transition::Candidate(_) => bail!("preparation cannot claim candidate execution"),
    }
}
pub(in crate::marketplace) async fn claim_start(
    selected: &mut Selection,
    archive: &ArchivedSource,
    intent: OwnerActivationIntent,
    prepared: &PreparedActivation,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<StartOrRecovery> {
    selected.maintain().await?;
    let material = Material::from_archive(archive, selected.metadata()?)?;
    let public = selected.public_source()?;
    claim_checked(
        Step {
            database: selected.client().source_database(),
            metadata: selected.metadata()?,
            public: &public,
            material,
            intent: Arc::new(intent),
            plan,
            budget,
        },
        prepared,
    )
    .await
}
async fn claim_checked(step: Step<'_>, prepared: &PreparedActivation) -> Result<StartOrRecovery> {
    match step.advance(Some(prepared)).await? {
        Transition::Start(s) => Ok(StartOrRecovery::Start(s)),
        Transition::Recovery(r) => Ok(StartOrRecovery::Recovery(r)),
        Transition::Prepared(_) | Transition::Authorized => {
            bail!("start claim cannot return an unclaimed preparation")
        }
        #[cfg(unix)]
        Transition::SourcePrepared(_) => bail!("start claim cannot complete a source probe"),
        Transition::Quiescing(_) => bail!("start claim cannot fence an active generation"),
        Transition::CutoverScope(_) => bail!("start claim cannot acquire a cutover scope"),
        Transition::StateCopied(_) => bail!("start claim cannot copy state"),
        Transition::Candidate(_) => bail!("source start cannot claim candidate execution"),
    }
}

/// Recheck the original one-shot claim while the actual native owner holds its
/// global lane. This mints nothing, changes no journal phase and cannot recover
/// a claim from status data. It authorizes source quarantine only, not code,
/// SDK effects, readiness or activation commit.
pub(in crate::marketplace) async fn require_native_start(
    database: &Database,
    metadata: &dyn crate::marketplace::selected_source::SelectedSource,
    public: &PublicSourceObservation,
    archive: &ArchivedSource,
    check: &NativeStartCheck,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<()> {
    let step = Step {
        database,
        metadata,
        public,
        material: Material::from_archive(archive, metadata)?,
        intent: check.intent.clone(),
        plan,
        budget,
    };
    match step.transition(Action::Native(check)).await? {
        Transition::Authorized => Ok(()),
        _ => bail!("native start did not retain its exact claim"),
    }
}

enum Action<'a> {
    Prepare,
    Claim(&'a PreparedActivation),
    Native(&'a NativeStartCheck),
    // Only the actual owned probe/disposal path can construct this proof.
    // A Starting row, archive receipt or serialized flag cannot construct it.
    #[cfg(unix)]
    Retain(&'a source::DisposedProbe<'a>),
    Quiesce(&'a ActivationReference),
    ObserveCutover(&'a ActivationReference),
    Copy(&'a runtime::QuiescentRuntime<'a>),
    Candidate(&'a runtime::QuiescentRuntime<'a>, &'a ActivationReference),
}

struct Step<'a> {
    database: &'a Database,
    metadata: &'a dyn crate::marketplace::selected_source::SelectedSource,
    public: &'a PublicSourceObservation,
    material: Material,
    intent: Arc<OwnerActivationIntent>,
    plan: &'a SourceActivationPlan,
    budget: InstallBudget,
}

impl<'s> Step<'s> {
    async fn advance(self, claim: Option<&PreparedActivation>) -> Result<Transition> {
        self.transition(match claim {
            Some(claim) => Action::Claim(claim),
            None => Action::Prepare,
        })
        .await
    }
    /// This transaction is reached through several native-controller polling
    /// layers. Keep its bounded journal/SQL future on the heap instead of
    /// embedding that whole frame in each enclosing source-start future.
    fn transition<'a>(
        self,
        action: Action<'a>,
    ) -> futures_util::future::BoxFuture<'a, Result<Transition>>
    where
        's: 'a,
    {
        Box::pin(self.transition_owned(action))
    }
    async fn transition_owned(self, action: Action<'_>) -> Result<Transition> {
        let Self {
            database,
            metadata,
            public,
            material,
            intent,
            plan,
            budget,
        } = self;
        budget.validate()?;
        material.current(metadata, public)?;
        metadata.require_actor(intent.actor.user, intent.actor.home)?;
        plan.require_material(&material)?;
        if intent.release != material.review.release_id()
            || intent.review != material.review.review_id()?
        {
            bail!("activation intent differs from verified source review");
        }
        let request = intent.request_id(material.source.id()?, plan.id())?;
        let started = Instant::now();
        let now = Utc::now();
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
        let query = "SELECT extension_id,home_id,revision,revocation_revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1";
        let query = if database.driver == DatabaseDriver::Postgres {
            format!("{query} FOR SHARE")
        } else {
            query.into()
        };
        let row = sqlx::query(&query)
            .bind(intent.extension.as_str())
            .fetch_optional(&mut *tx)
            .await?
            .context("activation grant is missing")?;
        let epoch: i64 = row.try_get("revocation_revision")?;
        let grant = StoredGrant::parse(row)?
            .active()
            .context("activation grant is disabled")?;
        if epoch < 0
            || epoch > grant.revision
            || grant.revision != intent.grant_revision
            || grant.home_id != intent.actor.home
            || !grant.covers(&material.review)?
        {
            bail!("activation grant changed or does not cover this source");
        }
        let ledger = store::lock_budget(&mut tx, budget).await?;
        let old = store::load_configured(
            &mut tx,
            database.driver,
            &intent.extension,
            intent.configured_instance,
        )
        .await?
        .context("activation journal missing; no implicit repair")?;
        if old.journal.home != intent.actor.home {
            bail!("activation belongs to a different home");
        }
        intent
            .require_choices(&mut tx, database.driver, &old.journal)
            .await?;
        let pending = old
            .journal
            .pending
            .as_ref()
            .context("activation has no staged candidate")?;
        if pending.instance != intent.instance
            || pending.review != intent.review
            || pending.source.id()? != material.source.id()?
            || pending.grant_revision < old.floor
            || pending.grant_revision > grant.revision
            || pending.metadata_id()?
                != OperationalMetadata::from_release(
                    material.authority.verify()?.release(),
                    &material.manifest,
                )?
                .id()?
        {
            bail!("activation source, instance, review or revocation boundary differs");
        }
        metadata
            .require_in_transaction(&mut tx, database.driver)
            .await?;
        crate::marketplace::security_state::retained::require_in_transaction(
            &mut tx,
            database.driver,
            &pending.facts()?,
        )
        .await?;
        material.current(metadata, public)?;
        if let Some(fence) = &old.journal.activation {
            if old.journal.mutation != intent.mutation
                || old.journal.request != request
                || old.journal.approved_by != intent.actor.user
                || fence.staged_revision != intent.staged_revision
                || fence.grant_revision != grant.revision
                || fence.plan != plan.id()
            {
                bail!("activation attempt/input changed");
            }
            if let Action::Claim(claim) = action {
                require_prepared(claim, &old)?;
            }
            if let Action::Native(start) = action {
                require_start(start, &old)?;
                // This is the first native handoff, not reuse of a configured
                // or already-started candidate. A saved Starting row alone is
                // never accepted as a replacement for the original claim.
                store::verify_instances(&mut tx, database.driver, &old.journal).await?;
                ensure_before_expiry(expires, now, started)?;
                tx.commit().await?;
                return Ok(Transition::Authorized);
            }
            if let Action::ObserveCutover(expected) = action {
                let scope = quiescence::CutoverScope::from_stored(expected, &old, budget)?;
                store::verify_instances(&mut tx, database.driver, &old.journal).await?;
                material.current(metadata, public)?;
                ensure_before_expiry(expires, now, started)?;
                tx.commit().await?;
                return Ok(Transition::CutoverScope(scope));
            }
            if let Action::Candidate(guard, copied) = action {
                candidate::require_copied(copied, &old)?;
                if fence.phase == ActivationPhase::CandidateStarting {
                    // A retry/status read must NEVER reconstruct a launch or
                    // SDK capability, even when no process is observable.
                    let reference = reference(&old)?;
                    ensure_before_expiry(expires, now, started)?;
                    tx.commit().await?;
                    return Ok(Transition::Recovery(reference));
                }
                guard.require(&mut tx, database.driver, &old).await?;
                store::verify_instances(&mut tx, database.driver, &old.journal).await?;
                ledger.require_normal_capacity()?;
                old.journal
                    .revision
                    .checked_add(2)
                    .filter(|r| *r <= MAX_REVISION)
                    .context("candidate claim needs a completion/cleanup revision")?;
                let mut journal: Journal =
                    canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
                journal.revision += 1;
                journal
                    .activation
                    .as_mut()
                    .context("candidate fence missing")?
                    .phase = ActivationPhase::CandidateStarting;
                let written =
                    store::write(&mut tx, ledger, Some(&old), journal, old.floor, budget).await?;
                store::verify_instances(&mut tx, database.driver, &written.journal).await?;
                guard.require_owner(&mut tx, database.driver).await?;
                let claim =
                    candidate::CandidateClaim::from_written(&written, intent, plan, budget)?;
                material.current(metadata, public)?;
                ensure_before_expiry(expires, now, started)?;
                tx.commit().await?;
                return Ok(Transition::Candidate(claim));
            }
            if let Action::Copy(guard) = action {
                guard.require(&mut tx, database.driver, &old).await?;
                store::verify_instances(&mut tx, database.driver, &old.journal).await?;
                if fence.phase == ActivationPhase::StateCopied {
                    // Same attempt only; never overwrite an existing copy.
                    material.current(metadata, public)?;
                    ensure_before_expiry(expires, now, started)?;
                    let reference = reference(&old)?;
                    tx.commit().await?;
                    return Ok(Transition::StateCopied(
                        pre_candidate::PreparationResult::Recovery(reference),
                    ));
                }
                if fence.phase != ActivationPhase::Quiescing {
                    bail!("state copy requires the exact quiescing barrier");
                }
                ledger.require_normal_capacity()?;
                old.journal
                    .revision
                    .checked_add(2)
                    .filter(|r| *r <= MAX_REVISION)
                    .context("state copy needs an abort revision reservation")?;
                let seal = state_copy::copy(
                    &mut tx,
                    database.driver,
                    guard.secrets(),
                    &old.journal,
                    intent.credentials.as_ref().map(|p| p.changes()),
                )
                .await?;
                let mut journal: Journal =
                    canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
                journal.revision += 1;
                let fence = journal.activation.as_mut().context("copy fence missing")?;
                fence.phase = ActivationPhase::StateCopied;
                fence.state_copy = Some(seal);
                let written =
                    store::write(&mut tx, ledger, Some(&old), journal, old.floor, budget).await?;
                store::verify_instances(&mut tx, database.driver, &written.journal).await?;
                guard.require(&mut tx, database.driver, &written).await?;
                material.current(metadata, public)?;
                ensure_before_expiry(expires, now, started)?;
                let receipt =
                    pre_candidate::PreCandidateReceipt::from_written(database, &written, budget)?;
                tx.commit().await?;
                return Ok(Transition::StateCopied(
                    pre_candidate::PreparationResult::Committed(receipt),
                ));
            }
            #[cfg(unix)]
            if let Action::Retain(probe) = action {
                require_start(probe.check(), &old)?;
                old.journal
                    .revision
                    .checked_add(2)
                    .filter(|r| *r <= MAX_REVISION)
                    .context("source preparation needs a final cleanup revision")?;
                store::verify_instances(&mut tx, database.driver, &old.journal).await?;
                let mut journal = Journal {
                    choices: old.journal.choices,
                    schema_version: old.journal.schema_version,
                    configured_instance: old.journal.configured_instance,
                    extension: old.journal.extension.clone(),
                    home: old.journal.home,
                    revision: old
                        .journal
                        .revision
                        .checked_add(1)
                        .filter(|revision| *revision <= MAX_REVISION)
                        .context("source preparation revision exhausted")?,
                    mutation: old.journal.mutation,
                    request: old.journal.request,
                    approved_by: old.journal.approved_by,
                    active: old.journal.active.clone(),
                    previous: old.journal.previous.clone(),
                    pending: old.journal.pending.clone(),
                    cancelled: None,
                    activation: old.journal.activation.clone(),
                };
                journal
                    .activation
                    .as_mut()
                    .context("source preparation fence missing")?
                    .phase = ActivationPhase::SourcePrepared;
                let written =
                    store::write(&mut tx, ledger, Some(&old), journal, old.floor, budget).await?;
                material.current(metadata, public)?;
                ensure_before_expiry(expires, now, started)?;
                let reference = reference(&written)?;
                tx.commit().await?;
                return Ok(Transition::SourcePrepared(reference));
            }
            if let Action::Quiesce(expected) = action {
                quiescence::require_source_prepared(expected, &old)?;
                store::verify_instances(&mut tx, database.driver, &old.journal).await?;
                if fence.phase == ActivationPhase::Quiescing {
                    let reference = reference(&old)?;
                    ensure_before_expiry(expires, now, started)?;
                    tx.commit().await?;
                    return Ok(Transition::Quiescing(
                        pre_candidate::PreparationResult::Recovery(reference),
                    ));
                }
                // Always leave a revision for a safe pre-migration abort. No
                // terminal-counter fence may strand a previously active source.
                old.journal
                    .revision
                    .checked_add(2)
                    .filter(|r| *r <= MAX_REVISION)
                    .context("quiescence needs an abort revision reservation")?;
                ledger.require_normal_capacity()?;
                let mut journal: Journal =
                    canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
                journal.revision += 1;
                journal
                    .activation
                    .as_mut()
                    .context("quiescence fence missing")?
                    .phase = ActivationPhase::Quiescing;
                let written =
                    store::write(&mut tx, ledger, Some(&old), journal, old.floor, budget).await?;
                material.current(metadata, public)?;
                ensure_before_expiry(expires, now, started)?;
                let receipt =
                    pre_candidate::PreCandidateReceipt::from_written(database, &written, budget)?;
                tx.commit().await?;
                return Ok(Transition::Quiescing(
                    pre_candidate::PreparationResult::Committed(receipt),
                ));
            }
            if fence.phase != ActivationPhase::Prepared {
                // This is status/recovery data, NEVER a replacement start cap.
                let reference = reference(&old)?;
                ensure_before_expiry(expires, now, started)?;
                tx.commit().await?;
                return Ok(Transition::Recovery(reference));
            }
            store::verify_instances(&mut tx, database.driver, &old.journal).await?;
            if matches!(action, Action::Prepare) {
                let prepared = PreparedActivation {
                    reference: reference(&old)?,
                };
                ensure_before_expiry(expires, now, started)?;
                tx.commit().await?;
                return Ok(Transition::Prepared(prepared));
            }
        } else {
            if !matches!(action, Action::Prepare)
                || old.journal.revision != intent.staged_revision
                || old.journal.mutation == intent.mutation
                || old.journal.cancelled.is_some()
            {
                bail!("activation revision changed or mutation ID was reused");
            }
            store::verify_instances(&mut tx, database.driver, &old.journal).await?;
        }
        let phase = if matches!(action, Action::Claim(_)) {
            ActivationPhase::Starting
        } else {
            ActivationPhase::Prepared
        };
        let attempt = old
            .journal
            .activation
            .as_ref()
            .map_or_else(Uuid::new_v4, |f| f.attempt);
        let next = old
            .journal
            .revision
            .checked_add(1)
            .filter(|n| *n <= MAX_REVISION)
            .context("activation revision exhausted")?;
        let journal = Journal {
            choices: old.journal.choices,
            schema_version: old.journal.schema_version,
            configured_instance: old.journal.configured_instance,
            extension: intent.extension.clone(),
            home: intent.actor.home,
            revision: next,
            mutation: intent.mutation,
            request,
            approved_by: intent.actor.user,
            active: old.journal.active.clone(),
            previous: old.journal.previous.clone(),
            pending: old.journal.pending.clone(),
            cancelled: None,
            activation: Some(ActivationFence {
                attempt,
                staged_revision: intent.staged_revision,
                grant_revision: grant.revision,
                plan: plan.id(),
                phase,
                state_copy: None,
            }),
        };
        let written = store::write(&mut tx, ledger, Some(&old), journal, old.floor, budget).await?;
        material.current(metadata, public)?;
        ensure_before_expiry(expires, now, started)?;
        let reference = reference(&written)?;
        let starting_journal = Digest::of(canonical::to_vec(&written.journal)?);
        tx.commit().await?;
        Ok(if phase == ActivationPhase::Starting {
            Transition::Start(ActivationStart {
                reference,
                intent,
                starting_journal,
            })
        } else {
            Transition::Prepared(PreparedActivation { reference })
        })
    }
}
fn reference(stored: &Stored) -> Result<ActivationReference> {
    let pending = stored
        .journal
        .pending
        .as_ref()
        .context("activation candidate missing")?;
    let fence = stored
        .journal
        .activation
        .as_ref()
        .context("activation fence missing")?;
    Ok(ActivationReference {
        configured_instance: stored.journal.configured_instance,
        extension: stored.journal.extension.clone(),
        release: pending.facts()?.release().release_id(),
        instance: pending.instance,
        revision: stored.journal.revision,
        attempt: fence.attempt,
        plan: fence.plan,
    })
}
fn require_prepared(prepared: &PreparedActivation, stored: &Stored) -> Result<()> {
    let actual = reference(stored)?;
    let expected = &prepared.reference;
    let stage = stored.journal.activation.as_ref().unwrap().staged_revision;
    if actual.extension != expected.extension
        || actual.configured_instance != expected.configured_instance
        || actual.release != expected.release
        || actual.instance != expected.instance
        || actual.attempt != expected.attempt
        || actual.plan != expected.plan
        || expected.revision != stage + 1
    {
        bail!("prepared activation reference differs");
    }
    Ok(())
}

fn require_start(start: &NativeStartCheck, stored: &Stored) -> Result<()> {
    let actual = reference(stored)?;
    let expected = &start.reference;
    if stored.journal.activation.as_ref().unwrap().phase != ActivationPhase::Starting
        || actual.configured_instance != expected.configured_instance
        || actual.extension != expected.extension
        || actual.release != expected.release
        || actual.instance != expected.instance
        || actual.revision != expected.revision
        || actual.attempt != expected.attempt
        || actual.plan != expected.plan
    {
        bail!("native start claim differs from the durable fence");
    }
    Ok(())
}

#[cfg(test)]
pub(in crate::marketplace::grant_store::installation) mod tests;
