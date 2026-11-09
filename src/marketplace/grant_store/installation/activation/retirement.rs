//! Authenticated cleanup of a positively joined native candidate. This never
//! enables code, repairs ownership, clears native fences or recreates a claim.
use super::*;

mod cancel;
pub(in crate::marketplace) use cancel::OwnerCandidateCancelIntent as OwnerRetiredCancelIntent;
pub(in crate::marketplace) use cancel::{
    OwnerCandidateCancelIntent, cancel, cancel_unstarted, cancel_unstarted_automatic,
};

pub(in crate::marketplace) struct OwnerDiscardIntent {
    actor: Actor,
    mutation: Uuid,
}
impl OwnerDiscardIntent {
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(Actor::from_principal(principal, false)?, mutation)
    }
    fn for_actor(actor: Actor, mutation: Uuid) -> Result<Self> {
        if mutation.is_nil() {
            bail!("invalid candidate discard intent");
        }
        Ok(Self { actor, mutation })
    }
    #[cfg(test)]
    pub(in crate::marketplace::grant_store::installation) fn fixture(
        actor: Actor,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(actor, mutation)
    }
    fn request(&self, retired: &runtime::RetiredCandidate) -> Result<Digest> {
        Ok(Digest::of(canonical::to_vec(&serde_json::json!({
            "domain":"elixir-marketplace/discard-retired-candidate/v1",
            "user":self.actor.user,"home":self.actor.home,"mutation":self.mutation,
            "journal":retired.binding().journal_id(),
        }))?))
    }
}

/// Borrows the original result through commit acknowledgement, so cancelling
/// this waiter cannot consume the only cleanup proof. Latest exact retries are
/// read-only acknowledgements, including after a lost successful commit reply.
pub(in crate::marketplace) async fn discard(
    retired: &runtime::RetiredCandidate,
    intent: OwnerDiscardIntent,
) -> Result<StagedInstallation> {
    let database = retired.database();
    let binding = retired.binding();
    let reference = binding.reference();
    let budget = binding.budget();
    budget.validate()?;
    if intent.actor.home != binding.home() {
        bail!("candidate discard belongs to a different home");
    }
    let request = intent.request(retired)?;
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
    let old = store::load_configured(
        &mut tx,
        database.driver,
        &reference.extension,
        reference.configured_instance,
    )
    .await?
    .context("candidate discard journal missing; no implicit repair")?;
    retired.require_owner(&mut tx, database.driver).await?;
    let pending = old
        .journal
        .pending
        .as_ref()
        .context("candidate discard source missing")?;
    if old.journal.home != intent.actor.home
        || pending.instance != reference.instance
        || pending.facts()?.release().release_id() != reference.release
    {
        bail!("candidate discard lost its exact original identity");
    }
    if old.journal.mutation == intent.mutation {
        if old.journal.activation.is_some()
            || old.journal.request != request
            || old.journal.approved_by != intent.actor.user
            || old.journal.revision != reference.revision + 1
        {
            bail!("candidate discard mutation was reused with different input");
        }
        store::verify_instances(&mut tx, database.driver, &old.journal).await?;
        let receipt = old.receipt()?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        return Ok(receipt);
    }
    binding.require_cleanup_journal(&old)?;
    store::verify_instances(&mut tx, database.driver, &old.journal).await?;
    // Checks the complete original ciphertext/secret seal, decrypts every
    // candidate entry, and verifies immutable copied secrets before deletion.
    state_copy::audit_retired(&mut tx, database.driver, &old.journal, retired, true).await?;
    let mut journal: Journal =
        canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
    journal.revision = journal
        .revision
        .checked_add(1)
        .context("candidate cleanup revision exhausted")?;
    journal.mutation = intent.mutation;
    journal.request = request;
    journal.approved_by = intent.actor.user;
    journal.activation = None;
    let written =
        store::write_retired_cleanup(&mut tx, database.driver, ledger, &old, journal, retired)
            .await?;
    store::verify_instances(&mut tx, database.driver, &written.journal).await?;
    retired.require_owner(&mut tx, database.driver).await?;
    ensure_before_expiry(expires, now, started)?;
    let receipt = written.receipt()?;
    tx.commit().await?;
    Ok(receipt)
}
