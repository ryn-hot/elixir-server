//! Atomic cancellation under ORIGINAL joined retirement OR a permanently
//! sealed never-launched claim. Neither proof can be reconstructed from status.
use super::*;
use crate::marketplace::grant_store::installation::cancellation::CancelledInstallation;

// Fresh authorization only. This intent supplies no native or claim proof.
pub(in crate::marketplace) struct OwnerCandidateCancelIntent {
    actor: automatic::EffectActor,
    mutation: Uuid,
}
impl OwnerCandidateCancelIntent {
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<Self> {
        if mutation.is_nil() {
            bail!("invalid candidate cancellation mutation");
        }
        Ok(Self {
            actor: Actor::from_principal(principal, false)?.into(),
            mutation,
        })
    }
    pub(in crate::marketplace) fn automatic(
        authorization: &automatic::Authorization,
        mutation: Uuid,
    ) -> Result<Self> {
        if mutation.is_nil() {
            bail!("invalid automatic cancellation mutation")
        }
        Ok(Self {
            actor: automatic::EffectActor::recovery(authorization),
            mutation,
        })
    }
    fn request(&self, closed: &runtime::ClosedCandidate<'_>) -> Result<Digest> {
        let domain = match closed {
            runtime::ClosedCandidate::Retired(_) => {
                "elixir-marketplace/cancel-retired-candidate/v1"
            }
            runtime::ClosedCandidate::Unstarted(_) => {
                "elixir-marketplace/cancel-unstarted-candidate/v1"
            }
        };
        let mut value = serde_json::json!({
            "domain":domain,
            "user":self.actor.user,"home":self.actor.home,"mutation":self.mutation,
            "journal":closed.binding().journal_id(),
        });
        if let Some(recovery) = self.actor.recovery_id()? {
            value["automatic_recovery"] = serde_json::json!(recovery);
        }
        Ok(Digest::of(canonical::to_vec(&value)?))
    }
    pub(in crate::marketplace::grant_store::installation) fn require_result(
        &self,
        closed: &runtime::ClosedCandidate<'_>,
        journal: &Journal,
    ) -> Result<()> {
        if self.actor.home != closed.binding().home()
            || journal.home != self.actor.home
            || journal.revision != closed.binding().reference().revision + 1
            || journal.mutation != self.mutation
            || journal.approved_by != self.actor.user
            || journal.request != self.request(closed)?
        {
            bail!("candidate cancellation differs from its authenticated request");
        }
        Ok(())
    }
}

/// Borrow the original result through acknowledgement. Losing this waiter
/// cannot consume its proof, rerun migration, or leave a two-commit SQL gap.
pub(in crate::marketplace) async fn cancel(
    retired: &runtime::RetiredCandidate,
    intent: OwnerCandidateCancelIntent,
) -> Result<CancelledInstallation> {
    cancel_closed(runtime::ClosedCandidate::Retired(retired), intent).await
}
pub(in crate::marketplace) async fn cancel_unstarted(
    unstarted: &runtime::UnstartedCandidate,
    principal: &CurrentPrincipal,
    mutation: Uuid,
) -> Result<CancelledInstallation> {
    cancel_closed(
        runtime::ClosedCandidate::Unstarted(unstarted),
        OwnerCandidateCancelIntent::new(principal, mutation)?,
    )
    .await
}
pub(in crate::marketplace) async fn cancel_unstarted_automatic(
    unstarted: &runtime::UnstartedCandidate,
    authorization: &automatic::Authorization,
    mutation: Uuid,
) -> Result<CancelledInstallation> {
    cancel_closed(
        runtime::ClosedCandidate::Unstarted(unstarted),
        OwnerCandidateCancelIntent::automatic(authorization, mutation)?,
    )
    .await
}
async fn cancel_closed(
    closed: runtime::ClosedCandidate<'_>,
    intent: OwnerCandidateCancelIntent,
) -> Result<CancelledInstallation> {
    let database = closed.database();
    let binding = closed.binding();
    let reference = binding.reference();
    let owner = reference
        .configured_instance
        .context("candidate cancellation requires an explicit configured owner")?;
    intent.actor.require_cleanup_target(
        &reference.extension,
        reference.release,
        reference.configured_instance,
    )?;
    let budget = binding.budget();
    budget.validate()?;
    if intent.actor.home != binding.home() {
        bail!("candidate cancellation belongs to a different home");
    }
    let request = intent.request(&closed)?;
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
    store::lock_grant_binding(
        &mut tx,
        database.driver,
        intent.actor.home,
        &reference.extension,
    )
    .await?;
    let ledger = store::lock_budget(&mut tx, budget).await?;
    let old = store::load_configured(&mut tx, database.driver, &reference.extension, Some(owner))
        .await?
        .context("candidate cancellation journal missing; no repair")?;
    closed.require_owner(&mut tx, database.driver).await?;
    if old.journal.home != intent.actor.home {
        bail!("candidate cancellation home changed");
    }
    if old.journal.mutation == intent.mutation {
        intent.require_result(&closed, &old.journal)?;
        let expected = record::CancelledCandidate::from_capsule(closed.capsule())?;
        if old.journal.pending.is_some()
            || old.journal.activation.is_some()
            || old
                .journal
                .cancelled
                .as_ref()
                .map(canonical::to_vec)
                .transpose()?
                != Some(canonical::to_vec(&expected)?)
        {
            bail!("candidate cancellation mutation reused with different input");
        }
        store::verify_instances(&mut tx, database.driver, &old.journal).await?;
        let receipt = CancelledInstallation::from_revision(old.journal.revision)?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        return Ok(receipt);
    }
    binding.require_cleanup_journal(&old)?;
    store::verify_instances(&mut tx, database.driver, &old.journal).await?;
    // A never-launched claim requires the exact unchanged copy, not the more
    // permissive post-migration audit used by a positively retired candidate.
    match &closed {
        runtime::ClosedCandidate::Retired(retired) => {
            state_copy::audit_retired(&mut tx, database.driver, &old.journal, retired, false)
                .await?;
        }
        runtime::ClosedCandidate::Unstarted(unstarted) => {
            state_copy::audit_unstarted(&mut tx, database.driver, &old.journal, unstarted).await?;
        }
    }
    let pending = old.journal.pending.as_ref().context("candidate missing")?;
    if pending.instance != reference.instance
        || canonical::to_vec(pending.as_ref())? != canonical::to_vec(closed.capsule())?
    {
        bail!("candidate cancellation lost its exact original candidate");
    }
    store::require_no_operational_dependants(&mut tx, pending.instance).await?;
    let cancelled = record::CancelledCandidate::from_capsule(pending)?;
    if sqlx::query("DELETE FROM marketplace_sdk_namespaces WHERE namespace_id=$1 AND instance_id=$2 AND extension_id=$3 AND home_id=$4 AND purpose='installed'")
        .bind(pending.instance.to_string()).bind(owner.to_string()).bind(reference.extension.as_str()).bind(intent.actor.home.to_string())
        .execute(&mut *tx).await?.rows_affected()!=1 {
        bail!("candidate namespace changed before cancellation");
    }
    let mut journal: Journal =
        canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
    journal.revision = journal
        .revision
        .checked_add(1)
        .context("cancellation revision exhausted")?;
    journal.mutation = intent.mutation;
    journal.request = request;
    journal.approved_by = intent.actor.user;
    journal.pending = None;
    journal.activation = None;
    journal.cancelled = Some(cancelled);
    let written = store::write_candidate_cancellation(
        &mut tx,
        database.driver,
        ledger,
        &old,
        journal,
        &closed,
        &intent,
    )
    .await?;
    store::verify_instances(&mut tx, database.driver, &written.journal).await?;
    closed.require_owner(&mut tx, database.driver).await?;
    ensure_before_expiry(expires, now, started)?;
    let receipt = CancelledInstallation::from_revision(written.journal.revision)?;
    tx.commit().await?;
    Ok(receipt)
}
