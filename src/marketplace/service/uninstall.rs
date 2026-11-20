//! Owner uninstall of an extension installed from the marketplace (`elx:`),
//! including one that declares a container stack. The order makes it all or
//! nothing:
//!
//! 1. Hold the installed application slot (no install, update, restoration or
//!    installed call can start; a running call is cancelled), the
//!    orchestrator's apply lock (no setup run or reconcile pass is running)
//!    and the stack pass (no pass can write the stack's members back).
//! 2. Stop the extension's long-lived executors.
//! 3. Remove every record in ONE transaction
//!    (`grant_store::installation::uninstall`). If it fails nothing changed and
//!    the extension serves again on its next call.
//! 4. Only after the commit: remove the stack apps' containers and config
//!    volumes and the archived releases nothing else names. These are best
//!    effort; leftovers cannot run (their records are gone) and leftover
//!    managed containers are removed when the server next starts.
use super::MarketplaceService;
use crate::{
    db::Database,
    extensions::store::ExtensionStore,
    http::auth::CurrentPrincipal,
    marketplace::{
        grant_store::{
            Actor,
            installation::uninstall::{self, Busy},
        },
        stack::{self, MemberRuntime},
    },
    orchestrator::lock::APPLY_LOCK_NAME,
};
use serde::Serialize;
use std::time::Duration;

/// The answer to a completed uninstall.
#[derive(Debug, Serialize)]
pub(crate) struct Uninstalled {
    pub extension_id: String,
    pub name: String,
    /// Stack member extensions (apps, connectors, setup) removed with it.
    pub removed_members: usize,
}

#[derive(Debug)]
pub(crate) enum UninstallError {
    /// Not an installed marketplace extension.
    NotFound,
    /// The principal is not the current server owner.
    Forbidden,
    /// Something it depends on is in progress; nothing was changed.
    Busy(String),
    /// Nothing was changed.
    Failed(anyhow::Error),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct UninstallOptions {
    /// How long to wait for a running setup run or reconcile pass, a stack
    /// pass, or an extension call to stop, before answering "busy".
    pub wait: Duration,
    /// Lifetime of the orchestrator apply lock if this server stops while
    /// holding it.
    pub lock_ttl: Duration,
    fail_before_commit: bool,
}

impl UninstallOptions {
    pub(crate) fn new(wait: Duration, lock_ttl: Duration) -> Self {
        Self {
            wait,
            lock_ttl,
            fail_before_commit: false,
        }
    }
    /// Tests: fail inside the transaction after every row was deleted.
    #[cfg(test)]
    pub(crate) fn failing_before_commit(mut self) -> Self {
        self.fail_before_commit = true;
        self
    }
}

impl MarketplaceService {
    pub(crate) async fn uninstall_extension(
        &self,
        database: &Database,
        principal: &CurrentPrincipal,
        extension_id: &str,
        runtime: &dyn MemberRuntime,
        options: UninstallOptions,
    ) -> Result<Uninstalled, UninstallError> {
        let actor =
            Actor::from_principal(principal, false).map_err(|_| UninstallError::Forbidden)?;
        let inventory = uninstall::inventory(database, extension_id)
            .await
            .map_err(UninstallError::Failed)?
            .ok_or(UninstallError::NotFound)?;
        if inventory.activating {
            return Err(UninstallError::Busy(
                "an activation of this extension is in progress".into(),
            ));
        }
        let slot = self
            .installation
            .begin_uninstall()
            .await
            .map_err(|busy| UninstallError::Busy(busy.0.into()))?;
        let store = ExtensionStore::new(&database.pool);
        let owner = uuid::Uuid::new_v4().to_string();
        let deadline = tokio::time::Instant::now() + options.wait;
        loop {
            match store
                .acquire_lock(APPLY_LOCK_NAME, &owner, options.lock_ttl)
                .await
            {
                Ok(true) => break,
                Ok(false) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(250)).await
                }
                Ok(false) => {
                    return Err(UninstallError::Busy(
                        "extension setup or maintenance is running".into(),
                    ));
                }
                Err(error) => return Err(UninstallError::Failed(error)),
            }
        }
        let result = self
            .uninstall_locked(database, &actor, &inventory, runtime, options)
            .await;
        if let Err(error) = store.release_lock(APPLY_LOCK_NAME, &owner).await {
            tracing::warn!("could not release the orchestrator lock after an uninstall: {error:#}");
        }
        drop(slot);
        result
    }

    async fn uninstall_locked(
        &self,
        database: &Database,
        actor: &Actor,
        inventory: &uninstall::Inventory,
        runtime: &dyn MemberRuntime,
        options: UninstallOptions,
    ) -> Result<Uninstalled, UninstallError> {
        let pass = tokio::time::timeout(options.wait, stack::exclusive_pass())
            .await
            .map_err(|_| UninstallError::Busy("container stacks are being updated".into()))?;
        let serve = self.serve.lock().ok().and_then(|host| host.clone());
        if let Some(host) = &serve
            && let Err(error) = host.retire(&inventory.instances, options.wait).await
        {
            host.finish_retirement(&inventory.instances, false);
            tracing::warn!("uninstall could not stop the extension's executor: {error:#}");
            return Err(UninstallError::Busy(
                "the extension's running call did not stop".into(),
            ));
        }
        let removed =
            uninstall::remove(database, actor, inventory, options.fail_before_commit).await;
        if let Some(host) = &serve {
            host.finish_retirement(&inventory.instances, removed.is_ok());
        }
        drop(pass);
        let removed = removed.map_err(|error| match error.downcast_ref::<Busy>() {
            Some(busy) => UninstallError::Busy(busy.0.into()),
            None => UninstallError::Failed(error),
        })?;
        tracing::info!(
            extension = inventory.extension.as_str(),
            instances = removed.instances.len(),
            members = removed.members.extensions.len(),
            "uninstalled a marketplace extension"
        );
        stack::remove_member_runtimes(runtime, &removed.members.instances).await;
        self.remove_archived_releases(removed.releases).await;
        Ok(Uninstalled {
            extension_id: inventory.extension.as_str().to_owned(),
            name: inventory.name.clone(),
            removed_members: removed.members.extensions.len(),
        })
    }

    /// The archive is shared with installs and package sharing, which hold
    /// its OS lease briefly; a few attempts ride that out.
    async fn remove_archived_releases(&self, releases: Vec<elixir_marketplace::identity::Digest>) {
        if releases.is_empty() {
            return;
        }
        let archive = self
            .installation_allocation
            .lock()
            .ok()
            .and_then(|configuration| {
                configuration
                    .as_ref()
                    .and_then(|configuration| configuration.runtime.as_ref())
                    .map(|runtime| runtime.archive.clone())
            });
        let Some(archive) = archive else { return };
        let mut attempt = 0;
        loop {
            attempt += 1;
            match crate::marketplace::archive::remove_releases(&archive, releases.clone()).await {
                Ok(count) => {
                    tracing::info!(
                        count,
                        "removed an uninstalled extension's archived releases"
                    );
                    return;
                }
                Err(error) if attempt >= 10 => {
                    tracing::warn!(
                        "could not remove an uninstalled extension's archived releases: {error:#}"
                    );
                    return;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }
}
