//! Cancel only an unstarted, disabled candidate. No network/issuer/source
//! availability is needed, and no archive, active runtime or grant is removed.
use super::*;
use record::CancelledCandidate;

enum CancelTarget {
    Staged,
    Prepared {
        attempt: Uuid,
        plan: Digest,
    },
    SourcePrepared {
        attempt: Uuid,
        plan: Digest,
    },
    UnstartedSource {
        attempt: Uuid,
        plan: Digest,
    },
    DisposedSource {
        attempt: Uuid,
        plan: Digest,
        starting_journal: Digest,
    },
    PreCandidate {
        attempt: Uuid,
        plan: Digest,
        phase: record::ActivationPhase,
        journal: Digest,
    },
}

/// A separate cleanup action: a fresh owner or the original system-owned update.
/// Neither authority substitutes for the exact unstarted/retired proof below.
pub(in crate::marketplace) struct OwnerCancelIntent {
    actor: automatic::EffectActor,
    extension: ExtensionId,
    release: Digest,
    instance: Uuid,
    configured_instance: Option<Uuid>,
    expected_revision: i64,
    mutation: Uuid,
    target: CancelTarget,
}
impl OwnerCancelIntent {
    /// Only an original acknowledged pre-claim transition, never an inspect
    /// result or a generic resume reference. No native cleanup is inferred.
    pub(super) fn pre_candidate(
        principal: &CurrentPrincipal,
        receipt: &activation::pre_candidate::PreCandidateReceipt,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::pre_candidate_with(
            Actor::from_principal(principal, false)?.into(),
            receipt,
            mutation,
        )
    }
    pub(super) fn pre_candidate_with(
        actor: automatic::EffectActor,
        receipt: &activation::pre_candidate::PreCandidateReceipt,
        mutation: Uuid,
    ) -> Result<Self> {
        let r = receipt.reference();
        if r.configured_instance.is_none() {
            bail!("pre-claim cancellation requires a configured owner");
        }
        let staged = StagedInstallation {
            choices: None,
            configured_instance: r.configured_instance,
            extension: r.extension.clone(),
            release: r.release,
            instance: r.instance,
            revision: r.revision,
        };
        let mut intent = Self::for_effect_actor(actor, &staged, mutation)?;
        intent.target = CancelTarget::PreCandidate {
            attempt: r.attempt,
            plan: r.plan,
            phase: receipt.phase(),
            journal: receipt.journal(),
        };
        Ok(intent)
    }
    /// Actual original source disposal, including failed restore/probe or a
    /// lost retention acknowledgement. No source/network/grant renewal here.
    pub(super) fn disposed_source(
        principal: &CurrentPrincipal,
        source: &activation::DisposedSource,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::disposed_source_with(
            Actor::from_principal(principal, false)?.into(),
            source,
            mutation,
        )
    }
    pub(super) fn disposed_source_with(
        actor: automatic::EffectActor,
        source: &activation::DisposedSource,
        mutation: Uuid,
    ) -> Result<Self> {
        let r = source.reference();
        if r.configured_instance.is_none() {
            bail!("disposed source cleanup requires a configured owner");
        }
        let staged = StagedInstallation {
            choices: None,
            configured_instance: r.configured_instance,
            extension: r.extension.clone(),
            release: r.release,
            instance: r.instance,
            revision: r.revision,
        };
        let mut intent = Self::for_effect_actor(actor, &staged, mutation)?;
        intent.target = CancelTarget::DisposedSource {
            attempt: r.attempt,
            plan: r.plan,
            starting_journal: source.starting_journal(),
        };
        Ok(intent)
    }
    /// The original start capability has been permanently consumed by a sealed
    /// never-launched owner. A reference/status or borrowed live start is not
    /// accepted, so this intent cannot coexist with a launchable capability.
    #[cfg(unix)]
    pub(super) fn unstarted_source(
        principal: &CurrentPrincipal,
        source: &super::activation::UnstartedSourceStart,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::unstarted_source_with(
            Actor::from_principal(principal, false)?.into(),
            source,
            mutation,
        )
    }
    #[cfg(unix)]
    pub(super) fn unstarted_source_with(
        actor: automatic::EffectActor,
        source: &super::activation::UnstartedSourceStart,
        mutation: Uuid,
    ) -> Result<Self> {
        let r = source.reference();
        if r.configured_instance.is_none() {
            bail!("unstarted source cleanup requires an explicit configured owner");
        }
        let staged = StagedInstallation {
            choices: None,
            configured_instance: r.configured_instance,
            extension: r.extension.clone(),
            release: r.release,
            instance: r.instance,
            revision: r.revision,
        };
        let mut intent = Self::for_effect_actor(actor, &staged, mutation)?;
        intent.target = CancelTarget::UnstartedSource {
            attempt: r.attempt,
            plan: r.plan,
        };
        Ok(intent)
    }
    /// Only the original successful probe/disposal handoff can construct this
    /// restriction. A saved reference or status response cannot substitute.
    #[cfg(unix)]
    pub(super) fn source_prepared(
        principal: &CurrentPrincipal,
        source: &super::activation::PreparedSource,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_source_reference(
            Actor::from_principal(principal, false)?.into(),
            source.reference(),
            mutation,
        )
    }
    #[cfg(unix)]
    pub(super) fn source_prepared_with(
        actor: automatic::EffectActor,
        source: &super::activation::PreparedSource,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_source_reference(actor, source.reference(), mutation)
    }
    fn for_source_reference(
        actor: automatic::EffectActor,
        r: &super::activation::ActivationReference,
        mutation: Uuid,
    ) -> Result<Self> {
        let staged = StagedInstallation {
            choices: None,
            configured_instance: r.configured_instance,
            extension: r.extension.clone(),
            revision: r.revision,
            instance: r.instance,
            release: r.release,
        };
        let mut intent = Self::for_effect_actor(actor, &staged, mutation)?;
        intent.target = CancelTarget::SourcePrepared {
            attempt: r.attempt,
            plan: r.plan,
        };
        Ok(intent)
    }
    /// SQL/crypto fixture only, never production/native-retirement evidence.
    #[cfg(test)]
    pub(super) fn fixture_disposed_source(
        principal: &CurrentPrincipal,
        reference: &super::activation::ActivationReference,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_source_reference(
            Actor::from_principal(principal, false)?.into(),
            reference,
            mutation,
        )
    }
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        staged: &StagedInstallation,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(Actor::from_principal(principal, false)?, staged, mutation)
    }
    pub(in crate::marketplace) fn prepared(
        principal: &CurrentPrincipal,
        prepared: &super::activation::PreparedActivation,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_prepared_actor(Actor::from_principal(principal, false)?, prepared, mutation)
    }
    pub(super) fn for_prepared_actor(
        actor: Actor,
        prepared: &super::activation::PreparedActivation,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::prepared_with(actor.into(), prepared, mutation)
    }
    pub(super) fn prepared_with(
        actor: automatic::EffectActor,
        prepared: &super::activation::PreparedActivation,
        mutation: Uuid,
    ) -> Result<Self> {
        let r = prepared.reference();
        let staged = StagedInstallation {
            choices: None,
            configured_instance: r.configured_instance,
            extension: r.extension.clone(),
            revision: r.revision,
            instance: r.instance,
            release: r.release,
        };
        let mut intent = Self::for_effect_actor(actor, &staged, mutation)?;
        intent.target = CancelTarget::Prepared {
            attempt: r.attempt,
            plan: r.plan,
        };
        Ok(intent)
    }
    pub(super) fn for_actor(
        actor: Actor,
        staged: &StagedInstallation,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_effect_actor(actor.into(), staged, mutation)
    }
    pub(super) fn for_effect_actor(
        actor: automatic::EffectActor,
        staged: &StagedInstallation,
        mutation: Uuid,
    ) -> Result<Self> {
        actor.require_cleanup_target(
            &staged.extension,
            staged.release,
            staged.configured_instance,
        )?;
        if !(1..MAX_REVISION).contains(&staged.revision)
            || staged.instance.is_nil()
            || staged
                .configured_instance
                .is_some_and(|owner| owner.is_nil() || owner == staged.instance)
            || mutation.is_nil()
        {
            bail!("invalid explicit installation cancellation");
        }
        Ok(Self {
            actor,
            extension: staged.extension.clone(),
            release: staged.release,
            instance: staged.instance,
            configured_instance: staged.configured_instance,
            expected_revision: staged.revision,
            mutation,
            target: CancelTarget::Staged,
        })
    }
    fn request_id(&self) -> Result<Digest> {
        let mut value = serde_json::json!({
            "domain":"elixir-marketplace/cancel-install/v1",
            "user":self.actor.user,"home":self.actor.home,
            "extension":self.extension,"release":self.release,"instance":self.instance,
            "expected_revision":self.expected_revision,"mutation":self.mutation
        });
        if let CancelTarget::Prepared { attempt, plan } = &self.target {
            value["domain"] = serde_json::json!("elixir-marketplace/cancel-prepared-activation/v1");
            value["activation"] = serde_json::json!({"attempt":attempt,"plan":plan});
        }
        if let CancelTarget::SourcePrepared { attempt, plan } = &self.target {
            value["domain"] = serde_json::json!("elixir-marketplace/cancel-source-prepared/v1");
            value["activation"] = serde_json::json!({"attempt":attempt,"plan":plan});
        }
        if let CancelTarget::UnstartedSource { attempt, plan } = &self.target {
            value["domain"] = serde_json::json!("elixir-marketplace/cancel-unstarted-source/v1");
            value["activation"] = serde_json::json!({"attempt":attempt,"plan":plan});
        }
        if let CancelTarget::DisposedSource {
            attempt,
            plan,
            starting_journal,
        } = &self.target
        {
            value["domain"] = serde_json::json!("elixir-marketplace/cancel-disposed-source/v1");
            value["activation"] = serde_json::json!({"attempt":attempt,"plan":plan,"starting_journal":starting_journal});
        }
        if let CancelTarget::PreCandidate {
            attempt,
            plan,
            phase,
            journal,
        } = &self.target
        {
            value["domain"] = serde_json::json!("elixir-marketplace/cancel-pre-candidate/v1");
            value["activation"] =
                serde_json::json!({"attempt":attempt,"plan":plan,"phase":phase,"journal":journal});
        }
        if let Some(owner) = self.configured_instance {
            value["domain"] = serde_json::json!(if matches!(
                &self.target,
                CancelTarget::UnstartedSource { .. }
            ) {
                "elixir-marketplace/cancel-unstarted-source/v2"
            } else if matches!(&self.target, CancelTarget::DisposedSource { .. }) {
                "elixir-marketplace/cancel-disposed-source/v2"
            } else if matches!(&self.target, CancelTarget::SourcePrepared { .. }) {
                "elixir-marketplace/cancel-source-prepared/v2"
            } else if matches!(&self.target, CancelTarget::PreCandidate { .. }) {
                "elixir-marketplace/cancel-pre-candidate/v2"
            } else {
                "elixir-marketplace/cancel-install/v2"
            });
            value["configured_instance"] = serde_json::json!(owner);
        }
        if let Some(recovery) = self.actor.recovery_id()? {
            value["automatic_recovery"] = serde_json::json!(recovery);
        }
        Ok(Digest::of(canonical::to_vec(&value)?))
    }
    /// Closed-writer restriction derived only from the original typed handoff.
    pub(super) fn require_closed_source(&self, stored: &Stored) -> Result<()> {
        let fence = stored
            .journal
            .activation
            .as_ref()
            .context("source fence missing")?;
        let mut expected_revision = self.expected_revision;
        let (attempt, plan, phase) = match &self.target {
            CancelTarget::SourcePrepared { attempt, plan } => {
                (attempt, plan, record::ActivationPhase::SourcePrepared)
            }
            CancelTarget::UnstartedSource { attempt, plan } => {
                (attempt, plan, record::ActivationPhase::Starting)
            }
            CancelTarget::DisposedSource {
                attempt,
                plan,
                starting_journal,
            } => {
                // A retention reply may be lost AFTER the commit. Reconcile
                // only these two exact forms of the ORIGINAL starting record,
                // using the retained positive disposal proof, never status.
                if stored.journal.revision == self.expected_revision + 1
                    && fence.phase == record::ActivationPhase::SourcePrepared
                {
                    expected_revision += 1;
                } else if stored.journal.revision != self.expected_revision
                    || fence.phase != record::ActivationPhase::Starting
                {
                    bail!("disposed source moved beyond its original retention boundary");
                }
                let mut projected: Journal = canonical::decode(
                    &canonical::to_vec(&stored.journal)?,
                    record::MAX_RECORD_BYTES,
                )?;
                projected.revision = self.expected_revision;
                projected
                    .activation
                    .as_mut()
                    .context("source fence missing")?
                    .phase = record::ActivationPhase::Starting;
                if Digest::of(canonical::to_vec(&projected)?) != *starting_journal {
                    bail!("disposed source lost its exact original starting journal");
                }
                (attempt, plan, fence.phase)
            }
            CancelTarget::PreCandidate {
                attempt,
                plan,
                phase,
                journal,
            } => {
                if !matches!(
                    phase,
                    record::ActivationPhase::Quiescing | record::ActivationPhase::StateCopied
                ) || fence.phase != *phase
                    || Digest::of(canonical::to_vec(&stored.journal)?) != *journal
                {
                    bail!("pre-claim cancellation lost its original committed journal");
                }
                (attempt, plan, *phase)
            }
            _ => bail!("source cancellation requires its original closed source owner"),
        };
        let fence = stored
            .journal
            .activation
            .as_ref()
            .context("source fence missing")?;
        let pending = stored
            .journal
            .pending
            .as_ref()
            .context("source candidate missing")?;
        if stored.journal.home != self.actor.home
            || stored.journal.extension != self.extension
            || stored.journal.configured_instance != self.configured_instance
            || stored.journal.revision != expected_revision
            || fence.phase != phase
            || &fence.attempt != attempt
            || &fence.plan != plan
            || pending.instance != self.instance
            || pending.facts()?.release().release_id() != self.release
        {
            bail!("source cancellation lost its exact closed source attempt");
        }
        Ok(())
    }
    pub(super) fn require_cancelled_record(&self, journal: &Journal) -> Result<()> {
        let revision_matches = journal.revision == self.expected_revision + 1
            || (matches!(&self.target, CancelTarget::DisposedSource { .. })
                && journal.revision == self.expected_revision + 2);
        if !revision_matches
            || journal.mutation != self.mutation
            || journal.approved_by != self.actor.user
            || journal.request != self.request_id()?
        {
            bail!("source cancellation result differs from its authenticated request");
        }
        Ok(())
    }
}

/// The latest cancellation revision, not permission to run, seed or delete an
/// archive. Only the current journal head supports an exact retry.
pub(in crate::marketplace) struct CancelledInstallation {
    revision: i64,
}
impl CancelledInstallation {
    pub(super) fn from_revision(revision: i64) -> Result<Self> {
        if !(1..=MAX_REVISION).contains(&revision) {
            bail!("invalid cancellation acknowledgement revision");
        }
        Ok(Self { revision })
    }
    pub(in crate::marketplace) fn revision(&self) -> i64 {
        self.revision
    }
}

pub(in crate::marketplace) async fn cancel(
    database: &Database,
    intent: OwnerCancelIntent,
    budget: InstallBudget,
) -> Result<CancelledInstallation> {
    budget.validate()?;
    let request = intent.request_id()?;
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
    // Same lock order as staging. An enabled grant, healthy review JSON, fresh
    // discovery, and publisher approval are intentionally NOT cleanup gates.
    store::lock_grant_binding(
        &mut tx,
        database.driver,
        intent.actor.home,
        &intent.extension,
    )
    .await?;
    let ledger = store::lock_budget(&mut tx, budget).await?;
    let previous = store::load_configured(
        &mut tx,
        database.driver,
        &intent.extension,
        intent.configured_instance,
    )
    .await?
    .context("installation journal is missing; no implicit repair")?;
    if previous.journal.home != intent.actor.home {
        bail!("installation belongs to a different home");
    }
    if previous.journal.mutation == intent.mutation {
        let cancelled = previous
            .journal
            .cancelled
            .as_ref()
            .context("installation mutation is not a cancellation")?;
        intent.require_cancelled_record(&previous.journal)?;
        if cancelled.release != intent.release || cancelled.instance != intent.instance {
            bail!("installation mutation ID reused with different input");
        }
        store::verify_instances(&mut tx, database.driver, &previous.journal).await?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        return Ok(CancelledInstallation {
            revision: previous.journal.revision,
        });
    }
    if !matches!(&intent.target, CancelTarget::DisposedSource { .. })
        && previous.journal.revision != intent.expected_revision
    {
        bail!("installation revision changed");
    }
    match (&intent.target, &previous.journal.activation) {
        (CancelTarget::Staged, None) => {}
        (CancelTarget::Prepared { attempt, plan }, Some(fence))
            if fence.phase == record::ActivationPhase::Prepared
                && &fence.attempt == attempt
                && &fence.plan == plan => {}
        (
            CancelTarget::SourcePrepared { .. }
            | CancelTarget::UnstartedSource { .. }
            | CancelTarget::DisposedSource { .. }
            | CancelTarget::PreCandidate { .. },
            Some(_),
        ) => {
            intent.require_closed_source(&previous)?;
        }
        _ => {
            bail!("activation may have started; unused-candidate cancellation cannot prove cleanup")
        }
    }
    let next = previous
        .journal
        .revision
        .checked_add(1)
        .filter(|r| *r <= MAX_REVISION)
        .context("source cancellation revision exhausted")?;
    let pending = previous
        .journal
        .pending
        .as_ref()
        .context("installation has no staged candidate")?;
    let facts = pending.facts()?;
    if pending.instance != intent.instance || facts.release().release_id() != intent.release {
        bail!("staged candidate differs from the cancellation request");
    }
    // This locks the actual disabled instance and proves empty SDK state and
    // no operational dependants before deletion. Never cascade-delete work
    // from an unexpectedly started/corrupt generation.
    store::verify_instances(&mut tx, database.driver, &previous.journal).await?;
    let cancelled = CancelledCandidate::from_capsule(pending)?;
    let removed = if let Some(owner) = intent.configured_instance {
        // The stable operational row is never generation garbage. Its
        // namespace FK removes only private state/secrets in this transaction.
        sqlx::query("DELETE FROM marketplace_sdk_namespaces WHERE namespace_id=$1 AND instance_id=$2 AND extension_id=$3 AND purpose='installed'")
            .bind(intent.instance.to_string()).bind(owner.to_string()).bind(intent.extension.as_str())
            .execute(&mut *tx).await?.rows_affected()
    } else {
        sqlx::query("DELETE FROM extension_instances WHERE instance_id=$1 AND extension_id=$2 AND enabled=FALSE")
            .bind(intent.instance.to_string()).bind(intent.extension.as_str())
            .execute(&mut *tx).await?.rows_affected()
    };
    if removed != 1 {
        bail!("staged instance changed before cancellation");
    }
    // The empty state head is removed by its FK cascade in this transaction.
    // Keep one compact current tombstone, not per-attempt history or a source
    // capsule. A fresh staging action must reverify its own archived bytes.
    let journal = Journal {
        choices: previous.journal.choices,
        schema_version: previous.journal.schema_version,
        configured_instance: previous.journal.configured_instance,
        extension: intent.extension.clone(),
        home: intent.actor.home,
        revision: next,
        mutation: intent.mutation,
        request,
        approved_by: intent.actor.user,
        active: previous.journal.active.clone(),
        previous: previous.journal.previous.clone(),
        pending: None,
        cancelled: Some(cancelled),
        activation: None,
    };
    let written = if matches!(
        &intent.target,
        CancelTarget::SourcePrepared { .. }
            | CancelTarget::UnstartedSource { .. }
            | CancelTarget::DisposedSource { .. }
            | CancelTarget::PreCandidate { .. }
    ) {
        store::write_closed_source_cancellation(
            &mut tx, ledger, &previous, journal, &intent, budget,
        )
        .await?
    } else {
        store::write(
            &mut tx,
            ledger,
            Some(&previous),
            journal,
            previous.floor,
            budget,
        )
        .await?
    };
    store::verify_instances(&mut tx, database.driver, &written.journal).await?;
    ensure_before_expiry(expires, now, started)?;
    tx.commit().await?;
    Ok(CancelledInstallation { revision: next })
}
