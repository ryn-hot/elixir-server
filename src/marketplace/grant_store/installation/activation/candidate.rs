//! One original candidate claim and its disabled local-effects restriction.
//! Neither is native-start, completion, retirement or activation proof. There
//! is deliberately no constructor from a status/reference or saved journal.
use super::*;
use elixir_marketplace::{
    source_execution::SourceLifecycleContext, source_job::SourceMatchDescriptor,
};

pub(in crate::marketplace) enum CandidateOrRecovery {
    Claim(CandidateClaim),
    Recovery(ActivationReference),
}

/// Only the successful StateCopied -> CandidateStarting commit returns this.
/// No Clone/Deserialize/reload. Losing it requires owned reconciliation, not
/// another run of publisher migration code against possibly changed state.
pub(in crate::marketplace) struct CandidateClaim {
    // The bounded source/review tree must not enlarge every enclosing native
    // owner/transition future or a controller's executor stack reservation.
    binding: Box<CandidateBinding>,
    context: SourceLifecycleContext,
    capsule: Box<Capsule>,
}
impl CandidateClaim {
    pub(super) fn from_written(
        stored: &Stored,
        intent: Arc<OwnerActivationIntent>,
        plan: &SourceActivationPlan,
        budget: InstallBudget,
    ) -> Result<Self> {
        let pending = stored
            .journal
            .pending
            .as_ref()
            .context("candidate missing")?;
        let facts = pending.facts()?;
        let (_, review) = pending.source.review(facts.release())?;
        let from = stored
            .journal
            .active
            .as_ref()
            .map(|a| -> Result<String> { Ok(a.facts()?.release().payload().version.clone()) })
            .transpose()?;
        let context =
            SourceLifecycleContext::new(from.as_deref(), &facts.release().payload().version)?;
        let fence = stored
            .journal
            .activation
            .as_ref()
            .context("candidate fence missing")?;
        if fence.phase != ActivationPhase::CandidateStarting || fence.plan != plan.id() {
            bail!("candidate claim differs from its committed launch plan");
        }
        Ok(Self {
            binding: Box::new(CandidateBinding {
                reference: reference(stored)?,
                journal: Digest::of(canonical::to_vec(&stored.journal)?),
                intent,
                review,
                source: plan.source().source_descriptor().clone(),
                budget,
            }),
            context,
            capsule: pending.clone(),
        })
    }
    pub(in crate::marketplace::grant_store) fn binding(&self) -> &CandidateBinding {
        &self.binding
    }
    pub(in crate::marketplace) fn context(&self) -> &SourceLifecycleContext {
        &self.context
    }
    pub(in crate::marketplace::grant_store::installation) fn capsule(&self) -> &Capsule {
        &self.capsule
    }
    pub(in crate::marketplace::grant_store::installation) fn deployment(&self) -> String {
        format!("candidate-{}", self.binding.reference.attempt.simple())
    }
    /// Pure preflight. A rejected configuration must leave the original claim
    /// in its caller-owned slot; it cannot consume or recreate that claim.
    pub(in crate::marketplace::grant_store::installation) fn require_configuration(
        &self,
        image: &str,
        budget: &crate::marketplace::quarantine::SourceJobBudget,
    ) -> Result<()> {
        let facts = self.capsule.facts()?;
        let plan =
            SourceActivationPlan::new(facts.release(), self.binding.review(), image, budget)?;
        if plan.id() != self.binding.reference.plan {
            bail!("candidate configuration differs from its original claimed plan");
        }
        self.context
            .require_target(&facts.release().payload().version)?;
        Ok(())
    }
}

/// Original claim-owned restriction, not an independent launch/SDK permit.
/// Each effect also requires the actual lifecycle NativeInvocation and the
/// controller's live cancellation/deadline/OS ownership restriction.
pub(in crate::marketplace::grant_store) struct CandidateBinding {
    reference: ActivationReference,
    journal: Digest,
    intent: Arc<OwnerActivationIntent>,
    review: GrantReview,
    source: SourceMatchDescriptor,
    budget: InstallBudget,
}
impl CandidateBinding {
    pub(in crate::marketplace::grant_store::installation) async fn require_choices(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
        journal: &Journal,
    ) -> Result<()> {
        self.intent.require_choices(tx, driver, journal).await
    }
    /// Current restriction for a separately authenticated commit owner. This
    /// does not reuse the original browser session or grant execution authority.
    pub(in crate::marketplace::grant_store::installation) async fn grant_for_commit(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
    ) -> Result<CommittedOwnerGrant> {
        let query = super::super::super::sdk::locked(
            "SELECT extension_id,home_id,revision,revocation_revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1",
            driver,
            super::super::super::sdk::RowLock::Shared,
        );
        let row = sqlx::query(&query)
            .bind(self.reference.extension.as_str())
            .fetch_optional(&mut **tx)
            .await?
            .context("candidate grant missing")?;
        let revoked: i64 = row.try_get("revocation_revision")?;
        let grant = StoredGrant::parse(row)?
            .active()
            .context("candidate grant disabled")?;
        if grant.home_id != self.home()
            || grant.revision != self.intent.grant_revision
            || !(0..grant.revision).contains(&revoked)
            || !grant.covers(&self.review)?
        {
            bail!("candidate commit grant changed");
        }
        Ok(grant)
    }
    pub(in crate::marketplace::grant_store) fn review(&self) -> &GrantReview {
        &self.review
    }
    pub(in crate::marketplace::grant_store) fn instance(&self) -> Uuid {
        self.reference.instance
    }
    pub(in crate::marketplace::grant_store) fn coordinates(
        &self,
    ) -> super::super::super::sdk::namespaces::Coordinates<'_> {
        super::super::super::sdk::namespaces::Coordinates {
            home: self.home(),
            extension: &self.reference.extension,
            instance: self
                .reference
                .configured_instance
                .unwrap_or(self.reference.instance),
            namespace: self.reference.instance,
        }
    }
    pub(in crate::marketplace::grant_store) fn source(&self) -> &SourceMatchDescriptor {
        &self.source
    }
    pub(in crate::marketplace::grant_store) fn execution(&self) -> Result<Digest> {
        Ok(Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/candidate-execution/v1", "journal":self.journal,
        }))?))
    }
    pub(in crate::marketplace::grant_store::installation) fn reference(
        &self,
    ) -> &ActivationReference {
        &self.reference
    }
    pub(in crate::marketplace::grant_store::installation) fn budget(&self) -> InstallBudget {
        self.budget
    }
    pub(in crate::marketplace::grant_store::installation) fn home(&self) -> Uuid {
        self.intent.actor.home
    }
    pub(in crate::marketplace::grant_store::installation) fn journal_id(&self) -> Digest {
        self.journal
    }
    /// Cleanup restriction only: disabled/corrupt grant review, expired original
    /// session and unavailable discovery must not mint execution or block an
    /// independently authenticated owner's original-retirement cleanup.
    pub(in crate::marketplace::grant_store::installation) fn require_cleanup_journal(
        &self,
        stored: &Stored,
    ) -> Result<()> {
        if stored.journal.home != self.home()
            || Digest::of(canonical::to_vec(&stored.journal)?) != self.journal
            || stored
                .journal
                .activation
                .as_ref()
                .is_none_or(|f| f.phase != ActivationPhase::CandidateStarting)
        {
            bail!("cleanup lost the original candidate execution fence");
        }
        Ok(())
    }
    /// Fresh authority in the SAME transaction as a local effect. Session
    /// expiry remains bounded through commit; it is not standing auto consent.
    pub(in crate::marketplace::grant_store) async fn require_in_transaction(
        &self,
        tx: &mut Transaction<'_, Any>,
        driver: DatabaseDriver,
        now: chrono::DateTime<Utc>,
    ) -> Result<(CommittedOwnerGrant, chrono::DateTime<Utc>)> {
        let actor = &self.intent.actor;
        let valid = actor.authenticate(tx, now).await?;
        crate::marketplace::preferences::require_home_binding(tx, actor.home, driver).await?;
        let query = "SELECT extension_id,home_id,revision,revocation_revision,review_json,review_id,mutation_id,approved_by_user_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) AS enabled,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) AS automatic_updates FROM marketplace_extension_grants WHERE extension_id=$1";
        let query = if driver == DatabaseDriver::Postgres {
            format!("{query} FOR SHARE")
        } else {
            query.into()
        };
        let row = sqlx::query(&query)
            .bind(self.reference.extension.as_str())
            .fetch_optional(&mut **tx)
            .await?
            .context("candidate grant missing")?;
        let revoked: i64 = row.try_get("revocation_revision")?;
        let grant = StoredGrant::parse(row)?
            .active()
            .context("candidate grant disabled")?;
        if grant.home_id != actor.home
            || grant.revision != self.intent.grant_revision
            || !(0..=grant.revision).contains(&revoked)
            || !grant.covers(&self.review)?
        {
            bail!("candidate owner grant changed");
        }
        store::read_budget(tx, driver, self.budget).await?;
        let stored = store::load_configured_shared(
            tx,
            driver,
            &self.reference.extension,
            self.reference.configured_instance,
        )
        .await?
        .context("candidate journal missing; no implicit repair")?;
        if stored.journal.home != actor.home
            || Digest::of(canonical::to_vec(&stored.journal)?) != self.journal
            || stored
                .journal
                .activation
                .as_ref()
                .is_none_or(|f| f.phase != ActivationPhase::CandidateStarting)
        {
            bail!("candidate no longer owns the exact execution fence");
        }
        self.require_choices(tx, driver, &stored.journal).await?;
        let pending = stored
            .journal
            .pending
            .as_ref()
            .context("candidate missing")?;
        if pending.instance != self.reference.instance
            || pending.grant_revision <= revoked
            || pending.grant_revision > grant.revision
        {
            bail!("candidate enrollment or revocation boundary changed");
        }
        crate::marketplace::security_state::retained::require_in_transaction(
            tx,
            driver,
            &pending.facts()?,
        )
        .await?;
        store::verify_instances(tx, driver, &stored.journal).await?;
        Ok((grant, valid))
    }
}

pub(in crate::marketplace) async fn claim(
    guard: &runtime::QuiescentRuntime<'_>,
    selected: &mut Selection,
    archive: &ArchivedSource,
    intent: OwnerActivationIntent,
    copied: &ActivationReference,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<CandidateOrRecovery> {
    selected.maintain().await?;
    let public = selected.public_source()?;
    claim_checked(
        Step {
            database: guard.database(),
            metadata: selected.metadata()?,
            public: &public,
            material: Material::from_archive(archive, selected.metadata()?)?,
            intent: Arc::new(intent),
            plan,
            budget,
        },
        guard,
        copied,
    )
    .await
}

pub(super) async fn claim_checked(
    step: Step<'_>,
    guard: &runtime::QuiescentRuntime<'_>,
    copied: &ActivationReference,
) -> Result<CandidateOrRecovery> {
    if !std::ptr::eq(step.database, guard.database()) {
        bail!("candidate claim must use its original controller database");
    }
    match step.transition(Action::Candidate(guard, copied)).await? {
        Transition::Candidate(claim) => Ok(CandidateOrRecovery::Claim(claim)),
        Transition::Recovery(reference) => Ok(CandidateOrRecovery::Recovery(reference)),
        _ => bail!("candidate claim did not retain its exact attempt"),
    }
}

pub(super) fn require_copied(expected: &ActivationReference, stored: &Stored) -> Result<()> {
    let actual = reference(stored)?;
    let fence = stored
        .journal
        .activation
        .as_ref()
        .context("candidate fence missing")?;
    if !matches!(
        fence.phase,
        ActivationPhase::StateCopied | ActivationPhase::CandidateStarting
    ) || expected.revision != fence.staged_revision + 5
        || actual.extension != expected.extension
        || actual.configured_instance != expected.configured_instance
        || actual.release != expected.release
        || actual.instance != expected.instance
        || actual.attempt != expected.attempt
        || actual.plan != expected.plan
    {
        bail!("candidate execution differs from the exact copied generation");
    }
    Ok(())
}
