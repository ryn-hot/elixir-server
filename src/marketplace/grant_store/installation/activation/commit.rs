//! Atomic configured-generation promotion. Only an original joined successful
//! native result plus fresh owner intent can enter; status/flags never can.
use super::super::super::sdk::snapshot::{self, Namespace};
use super::*;

pub(in crate::marketplace) struct OwnerCommitIntent {
    actor: automatic::EffectActor,
    mutation: Uuid,
}
impl OwnerCommitIntent {
    pub(in crate::marketplace) fn new(
        principal: &CurrentPrincipal,
        retired: &runtime::RetiredCandidate,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(
            Actor::from_principal(
                principal,
                !retired.binding().review().scope().secret_keys().is_empty(),
            )?,
            mutation,
        )
    }
    fn for_actor(actor: Actor, mutation: Uuid) -> Result<Self> {
        if mutation.is_nil() {
            bail!("invalid activation commit intent");
        }
        Ok(Self {
            actor: actor.into(),
            mutation,
        })
    }
    pub(in crate::marketplace) fn automatic(
        authorization: &automatic::Authorization,
        retired: &runtime::RetiredCandidate,
        mutation: Uuid,
    ) -> Result<Self> {
        if mutation.is_nil() {
            bail!("invalid automatic activation commit intent");
        }
        let review = retired.binding().review();
        authorization.require_review(review)?;
        authorization.require_target(
            review.scope().extension_id(),
            review.release_id(),
            retired.binding().reference().configured_instance,
        )?;
        Ok(Self {
            actor: authorization.into(),
            mutation,
        })
    }
    #[cfg(test)]
    pub(in crate::marketplace::grant_store::installation) fn fixture(
        actor: Actor,
        mutation: Uuid,
    ) -> Result<Self> {
        Self::for_actor(actor, mutation)
    }
}

/// Acknowledgement DATA only, not a reusable source or runtime capability.
pub(in crate::marketplace) struct ActivatedInstallation {
    pub(in crate::marketplace) configured_instance: Uuid,
    pub(in crate::marketplace) namespace: Uuid,
    pub(in crate::marketplace) revision: i64,
    pub(in crate::marketplace) release: Digest,
}
fn receipt(stored: &Stored) -> Result<ActivatedInstallation> {
    let active = stored
        .journal
        .active
        .as_ref()
        .context("active source missing")?;
    Ok(ActivatedInstallation {
        configured_instance: stored
            .journal
            .configured_instance
            .context("configured owner missing")?,
        namespace: active.instance,
        revision: stored.journal.revision,
        release: active.facts()?.release().release_id(),
    })
}

/// Borrow through commit acknowledgement. A cancelled waiter keeps the only
/// original retirement proof for exact retry; it never reruns migration code.
pub(in crate::marketplace) async fn activate(
    retired: &runtime::RetiredCandidate,
    intent: OwnerCommitIntent,
) -> Result<ActivatedInstallation> {
    if retired.migration_result().is_err() {
        bail!("candidate did not complete native readiness");
    }
    let database = retired.database();
    let binding = retired.binding();
    let reference = binding.reference();
    let owner = reference
        .configured_instance
        .context("legacy generations require explicit conversion; no legacy activation")?;
    if intent.actor.home != binding.home() {
        bail!("activation commit belongs to a different home");
    }
    let request = Digest::of(canonical::to_vec(&serde_json::json!({
        "domain":"elixir-marketplace/commit-retired-generation/v1", "user":intent.actor.user,
        "home":intent.actor.home,"mutation":intent.mutation,"journal":binding.journal_id(),
    }))?);
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
    let ledger = store::lock_budget(&mut tx, binding.budget()).await?;
    let old = store::load_configured(&mut tx, database.driver, &reference.extension, Some(owner))
        .await?
        .context("activation journal missing; no repair")?;
    retired.require_owner(&mut tx, database.driver).await?;
    if old.journal.home != intent.actor.home {
        bail!("activation commit home changed");
    }
    if old.journal.mutation == intent.mutation {
        let active = old
            .journal
            .active
            .as_ref()
            .context("commit was not activated")?;
        if old.journal.activation.is_some()
            || old.journal.pending.is_some()
            || old.journal.request != request
            || old.journal.approved_by != intent.actor.user
            || old.journal.revision != reference.revision + 1
            || canonical::to_vec(active.as_ref())? != canonical::to_vec(retired.capsule())?
        {
            bail!("activation commit mutation reused with different input");
        }
        store::verify_instances(&mut tx, database.driver, &old.journal).await?;
        let result = receipt(&old)?;
        ensure_before_expiry(expires, now, started)?;
        tx.commit().await?;
        return Ok(result);
    }
    binding.require_cleanup_journal(&old)?;
    // Lock actual operational parents before choice counters, matching real
    // control writes/triggers. Exact retries above acknowledge only; they must
    // never re-enable after a newer disable or require the old choice stamp.
    binding
        .require_choices(&mut tx, database.driver, &old.journal)
        .await?;
    let grant = binding.grant_for_commit(&mut tx, database.driver).await?;
    let pending = old.journal.pending.as_ref().context("candidate missing")?;
    if pending.instance != reference.instance
        || pending.grant_revision < old.floor
        || pending.grant_revision > grant.revision
        || canonical::to_vec(pending.as_ref())? != canonical::to_vec(retired.capsule())?
    {
        bail!("activation commit lost its exact enrolled source");
    }
    crate::marketplace::security_state::retained::require_in_transaction(
        &mut tx,
        database.driver,
        &pending.facts()?,
    )
    .await?;
    store::verify_instances(&mut tx, database.driver, &old.journal).await?;
    state_copy::audit_retired(&mut tx, database.driver, &old.journal, retired, false).await?;
    // Previous is never executable through active bindings. The original
    // candidate claim came through owned quiescence of this instance's active
    // source; its joined result and retained registry owner close the sequence.
    // Collection touches only fully audited private data, not operational IDs.
    if let Some(previous) = &old.journal.previous {
        let facts = previous.facts()?;
        let (_, review) = previous.source.review(facts.release())?;
        snapshot::audit_namespace(
            &mut tx,
            database.driver,
            retired.secrets(),
            Namespace {
                home: old.journal.home,
                extension: &old.journal.extension,
                instance: previous.instance,
                owner,
                review: &review,
            },
        )
        .await?;
        if sqlx::query("DELETE FROM marketplace_sdk_namespaces WHERE namespace_id=$1 AND instance_id=$2 AND extension_id=$3 AND home_id=$4 AND purpose='installed'")
            .bind(previous.instance.to_string()).bind(owner.to_string()).bind(old.journal.extension.as_str()).bind(old.journal.home.to_string())
            .execute(&mut *tx).await?.rows_affected() != 1 { bail!("previous namespace changed before collection"); }
    }
    let version = pending.facts()?.release().payload().version.clone();
    let rollback = old
        .journal
        .active
        .as_ref()
        .map(|a| -> Result<String> { Ok(a.facts()?.release().payload().version.clone()) })
        .transpose()?;
    let mut journal: Journal =
        canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
    journal.revision = journal
        .revision
        .checked_add(1)
        .context("activation revision exhausted")?;
    journal.mutation = intent.mutation;
    journal.request = request;
    journal.approved_by = intent.actor.user;
    journal.previous = journal.active.take();
    journal.active = journal.pending.take();
    journal.activation = None;
    super::super::providers::register(
        &mut tx,
        owner,
        journal.active.as_ref().context("active source missing")?,
    )
    .await?;
    if sqlx::query("UPDATE extension_instances SET enabled=TRUE,runtime_version=$1,rollback_version=$2,updated_at=CURRENT_TIMESTAMP WHERE instance_id=$3 AND extension_id=$4")
        .bind(version).bind(rollback).bind(owner.to_string()).bind(reference.extension.as_str()).execute(&mut *tx).await?.rows_affected() != 1 {
        bail!("configured instance changed before activation");
    }
    if sqlx::query("UPDATE extensions SET enabled=TRUE WHERE extension_id=$1")
        .bind(reference.extension.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected()
        != 1
    {
        bail!("extension metadata missing");
    }
    let written =
        store::write_retired_activation(&mut tx, database.driver, ledger, &old, journal, retired)
            .await?;
    store::verify_instances(&mut tx, database.driver, &written.journal).await?;
    retired.require_owner(&mut tx, database.driver).await?;
    ensure_before_expiry(expires, now, started)?;
    let result = receipt(&written)?;
    tx.commit().await?;
    Ok(result)
}
