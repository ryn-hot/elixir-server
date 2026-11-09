//! Data acknowledgement for the sole retained staging input, not grant renewal
//! or native evidence. Only its private owner calls this after an attempted write.
use super::*;

pub(super) async fn read(
    database: &Database,
    principal: &CurrentPrincipal,
    intent: &OwnerInstallIntent,
    extension: &ExtensionId,
    request: Digest,
    source: Digest,
) -> Result<Option<StagedInstallation>> {
    let actor = Actor::from_principal(principal, false)?;
    read_with_actor(database, actor.into(), intent, extension, request, source).await
}

pub(super) async fn read_with_actor(
    database: &Database,
    actor: automatic::EffectActor,
    intent: &OwnerInstallIntent,
    extension: &ExtensionId,
    request: Digest,
    source: Digest,
) -> Result<Option<StagedInstallation>> {
    if actor.user != intent.actor.user || actor.home != intent.actor.home {
        bail!("staging receipt belongs to another user/home")
    }
    let target = intent
        .configured
        .as_ref()
        .context("original configured target missing")?;
    actor.require_cleanup_target(extension, intent.release, Some(target.instance()))?;
    let now = Utc::now();
    let started = Instant::now();
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let expires = actor.authenticate(&mut tx, now).await?;
    crate::marketplace::preferences::require_home_binding(&mut tx, actor.home, database.driver)
        .await?;
    let stored =
        store::load_configured_shared(&mut tx, database.driver, extension, Some(target.instance()))
            .await?;
    let receipt = match stored {
        None if intent.expected_revision == 0 => None,
        Some(stored)
            if stored.journal.home == actor.home
                && stored.journal.revision == intent.expected_revision
                && stored.journal.pending.is_none()
                && stored.journal.activation.is_none() =>
        {
            None
        }
        Some(stored) => {
            let journal = &stored.journal;
            let pending = journal
                .pending
                .as_ref()
                .context("original staging result is no longer pending")?;
            if journal.home != actor.home
                || journal.approved_by != actor.user
                || journal.configured_instance != Some(target.instance())
                || journal.revision != intent.expected_revision + 1
                || journal.mutation != intent.mutation
                || journal.request != request
                || journal.activation.is_some()
                || pending.source.id()? != source
                || pending.review != intent.review
                || pending.grant_revision != intent.grant_revision
            {
                bail!("journal is not the exact original disabled staging result");
            }
            // A later revocation floor does NOT prevent acknowledging DATA for
            // fresh cleanup. Launch/activation still reauthenticate the actual
            // current grant, original choices and all independent native owners.
            store::verify_instances(&mut tx, database.driver, journal).await?;
            Some(stored.receipt()?)
        }
        _ => bail!("original staging journal is missing or changed"),
    };
    ensure_before_expiry(expires, now, started)?;
    tx.commit().await?;
    Ok(receipt)
}

#[cfg(test)]
mod tests;
