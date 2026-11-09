//! Atomic re-encrypted fork into the disabled candidate. No package code runs
//! here. A seal is data-integrity evidence, NOT native/readiness authority.
use super::super::super::sdk::snapshot::{self, CopySeal, Namespace};
use super::*;

pub(in crate::marketplace) async fn copy_state(
    guard: &runtime::QuiescentRuntime<'_>,
    selected: &mut Selection,
    archive: &ArchivedSource,
    intent: OwnerActivationIntent,
    plan: &SourceActivationPlan,
    budget: InstallBudget,
) -> Result<ActivationReference> {
    selected.maintain().await?;
    let public = selected.public_source()?;
    copy_checked(
        Step {
            // Never pair a native guard with a caller-selected database/secrets.
            database: guard.database(),
            metadata: selected.metadata()?,
            public: &public,
            material: Material::from_archive(archive, selected.metadata()?)?,
            intent: Arc::new(intent),
            plan,
            budget,
        },
        guard,
    )
    .await
}
pub(super) async fn copy_checked(
    step: Step<'_>,
    guard: &runtime::QuiescentRuntime<'_>,
) -> Result<ActivationReference> {
    Ok(copy_retained(step, guard).await?.into_reference())
}
pub(super) async fn copy_retained(
    step: Step<'_>,
    guard: &runtime::QuiescentRuntime<'_>,
) -> Result<pre_candidate::PreparationResult> {
    // Test callers must also use the actual controller's DB. Production's
    // entry point above constructs the Step from that owner directly.
    if !std::ptr::eq(step.database, guard.database()) {
        bail!("state copy must use its owned controller database");
    }
    match step.transition(Action::Copy(guard)).await? {
        Transition::StateCopied(output) => Ok(output),
        _ => bail!("copy did not retain its exact activation attempt"),
    }
}

fn review(capsule: &Capsule) -> Result<GrantReview> {
    let facts = capsule.facts()?;
    Ok(capsule.source.review(facts.release())?.1)
}
async fn operate(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
    manager: Option<&SecretsManager>,
    discard: bool,
    credentials: Option<&[super::super::super::sdk::generation_secrets::PreparedChange]>,
) -> Result<Option<CopySeal>> {
    let pending = journal.pending.as_ref().context("copy candidate missing")?;
    let pending_review = review(pending)?;
    let active_review = journal.active.as_ref().map(|a| review(a)).transpose()?;
    let target = Namespace {
        home: journal.home,
        extension: &journal.extension,
        instance: pending.instance,
        owner: journal.configured_instance.unwrap_or(pending.instance),
        review: &pending_review,
    };
    let source = journal
        .active
        .as_ref()
        .zip(active_review.as_ref())
        .map(|(a, r)| Namespace {
            home: journal.home,
            extension: &journal.extension,
            instance: a.instance,
            owner: journal.configured_instance.unwrap_or(a.instance),
            review: r,
        });
    if let Some(manager) = manager {
        return Ok(Some(
            snapshot::copy_with_credentials(tx, driver, manager, source, target, credentials)
                .await?,
        ));
    }
    let fence = journal.activation.as_ref().context("copy fence missing")?;
    if fence.phase != ActivationPhase::StateCopied {
        bail!("no sealed pre-migration copy");
    }
    let seal = fence
        .state_copy
        .as_ref()
        .context("state-copy seal missing")?;
    if discard {
        snapshot::discard(tx, driver, source, target, seal).await?;
    } else {
        snapshot::verify(tx, driver, source, target, seal).await?;
    }
    Ok(None)
}
pub(super) async fn copy(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    manager: &SecretsManager,
    journal: &Journal,
    credentials: Option<&[super::super::super::sdk::generation_secrets::PreparedChange]>,
) -> Result<CopySeal> {
    operate(tx, driver, journal, Some(manager), false, credentials)
        .await?
        .context("state copy seal missing")
}
pub(in crate::marketplace::grant_store::installation) async fn verify(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
) -> Result<()> {
    operate(tx, driver, journal, None, false, None).await?;
    Ok(())
}
pub(super) async fn discard(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
) -> Result<()> {
    operate(tx, driver, journal, None, true, None).await?;
    Ok(())
}

/// Local effect restrictions only, NOT candidate readiness, native retirement
/// or the final source/candidate snapshot audit required by activation commit.
pub(in crate::marketplace::grant_store::installation) async fn verify_candidate(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
) -> Result<()> {
    candidate_operation(tx, driver, journal, None, false, false).await
}

/// The same transaction must hold the actual original retirement owner and
/// exact journal before a full audit or partial-migration discard is possible.
pub(super) async fn audit_retired(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
    retired: &runtime::RetiredCandidate,
    discard: bool,
) -> Result<()> {
    // Journal identity was checked by the parent against its locked Stored.
    retired.require_owner(tx, driver).await?;
    candidate_operation(tx, driver, journal, Some(retired.secrets()), discard, false).await
}

/// The original claim was permanently sealed before any task received it.
/// Require the exact unchanged copy AND decrypt/audit every copied value and
/// credential; a merely valid post-migration lineage is insufficient here.
pub(super) async fn audit_unstarted(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
    unstarted: &runtime::UnstartedCandidate,
) -> Result<()> {
    unstarted.require_owner(tx, driver).await?;
    candidate_operation(tx, driver, journal, Some(unstarted.secrets()), false, true).await
}

async fn candidate_operation(
    tx: &mut Transaction<'_, Any>,
    driver: DatabaseDriver,
    journal: &Journal,
    manager: Option<&SecretsManager>,
    discard: bool,
    unchanged: bool,
) -> Result<()> {
    let fence = journal
        .activation
        .as_ref()
        .context("candidate fence missing")?;
    if fence.phase != ActivationPhase::CandidateStarting {
        bail!("candidate effects require their exact execution phase");
    }
    let pending = journal.pending.as_ref().context("candidate missing")?;
    let pending_review = review(pending)?;
    let active_review = journal.active.as_ref().map(|a| review(a)).transpose()?;
    let source = journal
        .active
        .as_ref()
        .zip(active_review.as_ref())
        .map(|(a, r)| Namespace {
            home: journal.home,
            extension: &journal.extension,
            instance: a.instance,
            owner: journal.configured_instance.unwrap_or(a.instance),
            review: r,
        });
    let target = Namespace {
        home: journal.home,
        extension: &journal.extension,
        instance: pending.instance,
        owner: journal.configured_instance.unwrap_or(pending.instance),
        review: &pending_review,
    };
    let seal = fence
        .state_copy
        .as_ref()
        .context("candidate copy lineage missing")?;
    if unchanged {
        snapshot::verify(tx, driver, source, target, seal).await?;
    }
    if let Some(manager) = manager {
        if discard {
            snapshot::discard_migrated(tx, driver, manager, source, target, seal).await?;
        } else {
            snapshot::audit_migrated(tx, driver, manager, source, target, seal).await?;
        }
    } else {
        if discard {
            bail!("candidate discard requires its original retired key owner");
        }
        snapshot::verify_candidate(tx, driver, source, target, seal).await?;
    }
    Ok(())
}
