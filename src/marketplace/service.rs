//! One application-owned lifecycle. Constructing AppState does not create
//! sockets, enroll a provider, or infer readiness from saved preferences.
pub use super::preparation::View as PreparationView;
use super::{
    preferences::Snapshot as Preferences,
    worker::{ParticipatingWorker, ParticipationClient, ParticipationPhase, ParticipationSnapshot},
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use elixir_marketplace::policy::ContributionMode;
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::timeout;
#[cfg(unix)]
pub(crate) mod management;
mod startup;
pub(in crate::marketplace) use startup::StartupOwner;
pub use startup::StartupView;
#[cfg(unix)]
mod providers;
#[cfg(unix)]
pub(crate) use super::application::InstalledCall;
#[cfg(unix)]
pub(crate) use providers::SourceProviderDescriptor;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePhase {
    NotConnected,
    Starting,
    Running,
    Applying,
    WaitingForProtection,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct RuntimeSnapshot {
    pub phase: RuntimePhase,
    pub participation: Option<ParticipationSnapshot>,
}
impl RuntimeSnapshot {
    pub fn participation_applied(&self, saved: &Preferences) -> bool {
        let Some(state) = self.participation.as_ref() else {
            return false;
        };
        self.phase == RuntimePhase::Running
            && state.phase == ParticipationPhase::Running
            && state.preferences_revision == Some(saved.revision)
            && state.applied_roles == Some(state.desired_roles)
            && (saved.preferences.contribution != ContributionMode::Off
                || (state
                    .applied_roles
                    .is_some_and(|roles| roles.consumer && !roles.catalog && !roles.routing)
                    && !state.package_sharing_active))
    }
}

/// Clone shares the same owner slot; it never starts another worker. Public
/// attachment consumes only the real protected participation capability.
#[derive(Clone)]
pub struct MarketplaceService {
    startup: Arc<startup::Control>,
    inner: Arc<Managed<ParticipatingWorker>>,
    search: Arc<super::search::SearchService>,
    pub(in crate::marketplace) imports: Arc<tokio::sync::Semaphore>,
    preparation: super::preparation::Service,
    #[cfg(unix)]
    installation: super::application::Service,
    #[cfg(unix)]
    updates: super::updates::Service,
    #[cfg(unix)]
    sharing: super::sharing::Service,
    #[cfg(unix)]
    installation_allocation: Arc<Mutex<Option<InstallationConfiguration>>>,
    #[cfg(unix)]
    target_reads: Arc<tokio::sync::Semaphore>,
    #[cfg(unix)]
    source_provider_reads: Arc<tokio::sync::Semaphore>,
    #[cfg(unix)]
    source_calls: Arc<super::application::InstalledCallQueue>,
    #[cfg(unix)]
    local_http: Option<Arc<super::local_http::Client>>,
    #[cfg(all(test, unix))]
    installation_context: Arc<Mutex<Option<InstallContext>>>,
}
/// One immutable startup selection shared by fresh/manual installs and retained
/// restoration. The optional native adapter never comes from management input.
#[cfg(unix)]
#[derive(Clone)]
struct InstallationConfiguration {
    allocation: super::application::Allocation,
    runtime: Option<super::grant_store::installation::runtime::InstalledRuntime>,
}
impl Default for MarketplaceService {
    fn default() -> Self {
        Self {
            startup: Arc::default(),
            inner: Arc::default(),
            search: Arc::default(),
            imports: Arc::new(tokio::sync::Semaphore::new(1)),
            preparation: super::preparation::Service::default(),
            #[cfg(unix)]
            installation: super::application::Service::default(),
            #[cfg(unix)]
            updates: super::updates::Service::default(),
            #[cfg(unix)]
            sharing: super::sharing::Service::default(),
            #[cfg(unix)]
            installation_allocation: Arc::default(),
            #[cfg(unix)]
            target_reads: Arc::new(tokio::sync::Semaphore::new(1)),
            #[cfg(unix)]
            source_provider_reads: Arc::new(tokio::sync::Semaphore::new(1)),
            #[cfg(unix)]
            source_calls: Arc::new(super::application::InstalledCallQueue::default()),
            #[cfg(unix)]
            local_http: None,
            #[cfg(all(test, unix))]
            installation_context: Arc::default(),
        }
    }
}
impl MarketplaceService {
    pub(in crate::marketplace) fn select_startup(&self) {
        self.startup.select();
    }
    pub(in crate::marketplace) fn claim_startup_progress(&self) -> Result<StartupOwner> {
        self.startup.claim()
    }
    pub fn startup_snapshot(&self) -> StartupView {
        self.startup.snapshot()
    }
    /// A hint to an existing owner only; no task, role, health or credit is
    /// created. Call only after authenticated configuration commit/intent.
    pub(crate) fn connection_configuration_changed(&self) -> bool {
        self.startup.notify()
    }
    /// Called only after an authenticated durable provider-choice commit. This
    /// invalidates old network admission, not identity/fences or accounting.
    pub(crate) fn provider_configuration_changed(&self) {
        if let Ok(client) = self.client() {
            client.invalidate_connection();
        }
        self.connection_configuration_changed();
    }
    pub(in crate::marketplace) async fn wait_connection_configuration_changed(&self) {
        self.startup.changed().await;
    }
    pub(in crate::marketplace) fn startup_retry_after(&self, delay: Option<Duration>) {
        self.startup.retry(delay);
    }
    /// Server startup only. No DNS, socket, background task or owner permission
    /// is created by configuring this dormant local transport.
    #[cfg(unix)]
    pub(crate) fn with_local_http(
        enabled: bool,
        playback: Arc<crate::playback::PlaybackJobManager>,
    ) -> Self {
        Self {
            local_http: Some(Arc::new(super::local_http::Client::new(enabled, playback))),
            ..Self::default()
        }
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn local_http_client(
        &self,
    ) -> Option<Arc<super::local_http::Client>> {
        self.local_http.clone()
    }
    #[cfg(unix)]
    fn installation_host(
        &self,
        client: &ParticipationClient,
        secrets: crate::secrets::SecretsManager,
        allocation: super::application::Allocation,
    ) -> Result<super::application::Host> {
        // Fresh installs must reuse the SAME trusted startup configuration as
        // restoration: runtime, playback, paths, HTTP underlay and compiler
        // policy. A network client still owns fresh protected source admission;
        // its observations cannot replace that immutable operator selection.
        let configured = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installation configuration unavailable"))?
            .as_ref()
            .and_then(|configuration| configuration.runtime.clone());
        let runtime = match configured {
            Some(runtime) => runtime,
            None => client.installed_configuration(allocation.source, allocation.retention)?,
        };
        Ok(super::application::Host::from_runtime(
            client.source_database().clone(),
            secrets,
            runtime,
            allocation.installations,
            allocation.execution,
        )?
        .with_local_http(self.local_http_client()))
    }
    /// Operator startup only. No network/discovery/provider receipt or status
    /// can open this slot. The task retains its actual native reconciliation.
    #[cfg(unix)]
    pub(in crate::marketplace) fn begin_installed_restoration(
        &self,
        database: crate::db::Database,
        secrets: crate::secrets::SecretsManager,
        configuration: super::grant_store::installation::runtime::InstalledRuntime,
    ) -> Result<()> {
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installed allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone())
            .context("installed restoration not configured by trusted startup")?;
        if configuration.source_budget != allocation.source
            || (
                configuration.retention.bytes,
                configuration.retention.releases,
            ) != (allocation.retention.bytes, allocation.retention.releases)
        {
            bail!("installed restoration differs from its configured allocation");
        }
        let host = super::application::Host::from_runtime(
            database,
            secrets,
            configuration,
            allocation.installations,
            allocation.execution,
        )?;
        self.installation.begin_restoration(host)
    }
    #[cfg(unix)]
    pub(in crate::marketplace) async fn finish_installed_restoration(&self) -> Result<()> {
        self.installation.finish_restoration().await
    }
    /// Startup-owned task only. Per-extension standing consent is still read
    /// from current authenticated grant rows, never inferred from this call.
    #[cfg(unix)]
    pub(in crate::marketplace) fn begin_automatic_updates(
        &self,
        database: crate::db::Database,
        secrets: crate::secrets::SecretsManager,
        configuration: super::grant_store::installation::runtime::InstalledRuntime,
    ) -> Result<()> {
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installed allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone())
            .context("automatic updates require configured installed startup")?;
        if configuration.source_budget != allocation.source
            || (
                configuration.retention.bytes,
                configuration.retention.releases,
            ) != (allocation.retention.bytes, allocation.retention.releases)
        {
            bail!("automatic updates differ from the installed allocation")
        }
        self.updates
            .begin(self.clone(), database, secrets, configuration, allocation)
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn automatic_update_idle(&self) -> Result<bool> {
        Ok(!self.stop_requested()
            && self.source_calls.is_idle()
            && self.installation.background_idle()?)
    }
    /// Trusted installed startup only. This contributes existing active source
    /// archives through the same protected actor; no new cache or relay actor.
    #[cfg(unix)]
    pub(in crate::marketplace) fn begin_installed_sharing(
        &self,
        database: crate::db::Database,
        configuration: &super::grant_store::installation::runtime::InstalledRuntime,
    ) -> Result<()> {
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installed allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone())
            .context("sharing requires configured installed startup")?;
        if configuration.source_budget != allocation.source
            || (
                configuration.retention.bytes,
                configuration.retention.releases,
            ) != (allocation.retention.bytes, allocation.retention.releases)
        {
            bail!("sharing differs from the installed allocation")
        }
        self.sharing.begin(
            self.clone(),
            database,
            configuration.archive.clone(),
            allocation.retention,
            allocation.installations,
        )
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn start_automatic_install(
        &self,
        query: uuid::Uuid,
        selected: super::search::Selection,
        authorization: super::grant_store::installation::automatic::Authorization,
        action: super::grant_store::installation::InstallAction,
        host: super::application::Host,
        search: Arc<super::search::SearchService>,
    ) -> Result<uuid::Uuid> {
        if !self.automatic_update_idle()? {
            bail!("automatic update yields to installed activity")
        }
        let client = self.client()?;
        if !client.same_owner(selected.client()) {
            bail!("automatic update changed protected owner")
        }
        let context = InstallContext {
            inner: self.inner.clone(),
            search,
            preparation: self.preparation.clone(),
            client,
            direct: None,
            #[cfg(test)]
            fixture: None,
        };
        self.installation
            .start_automatic(context, host, authorization, selected, query, action)
    }
    /// Trusted application/provider integration only. Local execution needs no
    /// discovery connection; HTTP optionally borrows the current protected actor.
    /// Never reconstruct/open a controller from journal/status data.
    #[cfg(unix)]
    pub(crate) fn begin_installed_call(
        &self,
        home: uuid::Uuid,
        extension: elixir_marketplace::identity::ExtensionId,
        instance: uuid::Uuid,
        arguments: elixir_marketplace::source_execution::RuntimeJson,
    ) -> Result<InstalledCall> {
        let allocation = self
            .installation_allocation
            .lock()
            .map_err(|_| anyhow::anyhow!("installed allocation unavailable"))?
            .as_ref()
            .map(|configuration| configuration.allocation.clone())
            .context("installed execution not configured by trusted startup")?;
        let turn = self.source_calls.try_acquire()?;
        self.installation.begin_installed_http_call(
            home,
            extension,
            instance,
            arguments,
            allocation.installations,
            allocation.execution,
            self.client()
                .ok()
                .and_then(|client| client.installed_http()),
            turn,
        )
    }
    /// Internal only until HTTP/CSRF/client and the remaining runtime/network
    /// launch gates are qualified. No await precedes reserving the command.
    #[cfg(unix)]
    pub(in crate::marketplace) fn start_install(
        &self,
        principal: &crate::http::auth::CurrentPrincipal,
        query: uuid::Uuid,
        action: super::grant_store::installation::InstallAction,
        secrets: crate::secrets::SecretsManager,
        allocation: super::application::Allocation,
    ) -> Result<uuid::Uuid> {
        if self.stop_requested() {
            bail!("marketplace is stopping")
        }
        let context = InstallContext {
            inner: self.inner.clone(),
            search: self.search.clone(),
            preparation: self.preparation.clone(),
            client: self.client()?,
            direct: None,
            #[cfg(test)]
            fixture: None,
        };
        let host = self.installation_host(&context.client, secrets, allocation)?;
        let id = self
            .installation
            .start(context, host, principal.clone(), query, action)?;
        if self.stop_requested() {
            self.installation.request_stop();
            bail!("marketplace is stopping; original install retained for cleanup")
        }
        Ok(id)
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn install_view(
        &self,
        id: uuid::Uuid,
    ) -> Result<super::application::View> {
        self.installation.view(id)
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn install_action(
        &self,
        id: uuid::Uuid,
        action: super::application::Action,
    ) -> Result<super::application::ActionReceipt> {
        self.installation.submit(id, action)
    }
    pub fn attach(&self, worker: ParticipatingWorker) -> Result<()> {
        self.inner.attach(worker)
    }
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.inner.snapshot()
    }
    pub(in crate::marketplace) fn finish_cancelled_startup(
        &self,
        proof: &super::startup::CancelledStartupRetirement,
    ) -> Result<()> {
        if !proof.is_current() || !Arc::ptr_eq(&self.inner, &proof.service().inner) {
            bail!("cancelled startup retirement belongs to another controller");
        }
        self.inner.finish_cancelled_startup()
    }
    pub fn client(&self) -> Result<ParticipationClient> {
        self.inner.client()
    }
    pub fn request_stop(&self) {
        #[cfg(unix)]
        self.sharing.request_stop();
        #[cfg(unix)]
        self.updates.request_stop();
        #[cfg(unix)]
        self.source_calls.close();
        #[cfg(unix)]
        self.installation.request_stop();
        self.preparation.request_stop();
        self.search.request_stop();
        self.inner.request_stop();
    }
    pub fn start_search(&self, request: super::search::SearchRequest) -> Result<uuid::Uuid> {
        if self.stop_requested() {
            bail!("marketplace is stopping");
        }
        self.search.start(self.client()?, request)
    }
    pub async fn search_view(&self, id: uuid::Uuid) -> Result<super::search::SearchView> {
        self.search.interactive_view(id).await
    }
    /// Owner-selected source preparation only, never a legacy install request.
    /// The owned task survives HTTP cancellation and waits at permission review;
    /// a later explicit permission decision still does not install or share it.
    pub async fn prepare_search(
        &self,
        query: uuid::Uuid,
        extension: &elixir_marketplace::identity::ExtensionId,
        release: elixir_marketplace::identity::Digest,
        artifact: Option<usize>,
    ) -> Result<uuid::Uuid> {
        let selection = self.select_search(query, extension, release).await?;
        let id = self.preparation.start(selection, artifact)?;
        if self.stop_requested() {
            self.preparation.request_stop();
            bail!("marketplace is stopping; preparation cancellation requested");
        }
        Ok(id)
    }
    /// Explicit foreground retention allocation, outside automatic worker
    /// storage. Only a later owner permission decision starts the copy. This
    /// accepts no filesystem path and creates no install or sharing authority.
    #[cfg(unix)]
    pub async fn prepare_search_retained(
        &self,
        query: uuid::Uuid,
        extension: &elixir_marketplace::identity::ExtensionId,
        release: elixir_marketplace::identity::Digest,
        artifact: Option<usize>,
        budget: super::archive::ArchiveBudget,
    ) -> Result<uuid::Uuid> {
        budget.validate()?;
        let selection = self.select_search(query, extension, release).await?;
        let id = self
            .preparation
            .start_retained(selection, artifact, budget)?;
        if self.stop_requested() {
            self.preparation.request_stop();
            bail!("marketplace is stopping; retention cancellation requested");
        }
        Ok(id)
    }
    pub fn preparation_view(&self, id: uuid::Uuid) -> Result<PreparationView> {
        self.preparation.view(id)
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_original_update_diagnostics(
        &self,
    ) -> Result<serde_json::Value> {
        self.updates.fixture_diagnostics()
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_original_update_failure(
        &self,
    ) -> Result<Option<std::sync::Arc<anyhow::Error>>> {
        self.updates.fixture_failure()
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_original_preparation_failure(
        &self,
    ) -> Result<Option<std::sync::Arc<anyhow::Error>>> {
        self.preparation.fixture_current_failure()
    }
    pub async fn cancel_preparation(&self, id: uuid::Uuid) -> Result<()> {
        self.preparation.cancel(id).await
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_original_installation_failures(
        &self,
    ) -> Result<(
        Option<std::sync::Arc<anyhow::Error>>,
        Option<std::sync::Arc<anyhow::Error>>,
    )> {
        self.installation.fixture_current_failures()
    }
    /// Queue one owner decision for the current exact review. This acknowledges
    /// request ownership, not a committed grant or installation. A future HTTP
    /// mutation must additionally enforce normal CSRF and poll preparation_view.
    pub fn approve_preparation(
        &self,
        id: uuid::Uuid,
        principal: &crate::http::auth::CurrentPrincipal,
        selection: super::grant_store::ApprovalSelection,
    ) -> Result<()> {
        if self.stop_requested() {
            bail!("marketplace is stopping");
        }
        self.preparation.approve(id, principal, selection)?;
        if self.stop_requested() {
            self.preparation.request_stop();
            bail!("marketplace is stopping; approval cancellation requested");
        }
        Ok(())
    }
    /// Internal handoff to foreground source preparation, not an HTTP install
    /// token. Only a current admitted card from the same protected owner wins.
    pub(in crate::marketplace) async fn select_search(
        &self,
        id: uuid::Uuid,
        extension: &elixir_marketplace::identity::ExtensionId,
        release: elixir_marketplace::identity::Digest,
    ) -> Result<super::search::Selection> {
        if self.stop_requested() {
            bail!("marketplace is stopping");
        }
        let client = self.client()?;
        let selected = self.search.select(&client, id, extension, release).await?;
        if self.stop_requested() || !client.same_owner(&self.client()?) {
            bail!("selected marketplace owner changed");
        }
        Ok(selected)
    }
    /// At most one owner-requested direct preview, with no preview queue or
    /// retained history. It may briefly wait for the existing shared helper.
    pub async fn import_preview(
        &self,
        publication: elixir_marketplace::publication::PreparedPublication,
    ) -> Result<super::direct_import::ImportPreview> {
        let _permit = self
            .imports
            .clone()
            .try_acquire_owned()
            .context("direct-reference preview is busy")?;
        if self.stop_requested() {
            bail!("marketplace is stopping");
        }
        let client = self.client()?;
        let observation = client
            .verify_publication_for_preview(&publication)
            .await
            .context("direct preview signature verification")?;
        let tracked = client
            .check_publication_current(&observation)
            .await
            .context("direct preview retained security check")?;
        if self.stop_requested() || !client.same_owner(&self.client()?) {
            bail!("direct-reference preview owner changed");
        }
        super::direct_import::ImportPreview::checked(&observation, tracked)
    }
    pub(in crate::marketplace) fn stop_requested(&self) -> bool {
        self.inner.stop_requested()
    }
    pub async fn shutdown(&self) -> Result<()> {
        // Signal every owner BEFORE any possibly slow SQL/native join. In
        // particular, installed cleanup must not keep networking unnotified.
        self.request_stop();
        #[cfg(unix)]
        let sharing = self.sharing.shutdown().await;
        #[cfg(unix)]
        let updates = self.updates.shutdown().await;
        #[cfg(unix)]
        let installation = self.installation.shutdown().await;
        let (worker, search, preparation) = tokio::join!(
            self.inner.clone().shutdown(),
            self.search.join(),
            self.preparation.shutdown()
        );
        #[cfg(unix)]
        sharing?;
        #[cfg(unix)]
        installation?;
        #[cfg(unix)]
        updates?;
        worker?;
        search?;
        preparation
    }
    /// Reap a failed attached lifecycle and rearm its slot only after its
    /// actual cleanup task joins successfully. Not a reset for failed startup
    /// attempts or unknown cleanup, and not a network/health factory.
    pub async fn recover_failed(&self) -> Result<bool> {
        self.inner.clone().recover_failed().await
    }
    pub(crate) fn begin_startup(&self) -> Result<StartupAttempt> {
        Ok(StartupAttempt {
            inner: self.inner.begin_startup()?,
        })
    }
    pub(in crate::marketplace) fn begin_recovery(
        &self,
        leases: &super::startup::StartupLeases,
    ) -> Result<StartupAttempt> {
        Ok(StartupAttempt {
            inner: self.inner.begin_recovery(leases)?,
        })
    }
}

/// The owned install task may retain this context, never MarketplaceService
/// itself (which contains that task's slot). The ORIGINAL protected client is
/// checked before and after selection; a new network owner is not substituted.
#[cfg(unix)]
pub(in crate::marketplace) struct InstallContext {
    inner: Arc<Managed<ParticipatingWorker>>,
    search: Arc<super::search::SearchService>,
    pub(in crate::marketplace) preparation: super::preparation::Service,
    client: ParticipationClient,
    direct: Option<DirectInstallSource>,
    #[cfg(test)]
    fixture: Option<InstallSelectionFixture>,
}
#[cfg(unix)]
struct DirectInstallSource {
    principal: crate::http::auth::CurrentPrincipal,
    publication: elixir_marketplace::publication::PreparedPublication,
    reference: elixir_marketplace::identity::Digest,
    query: uuid::Uuid,
    upload_source: bool,
}
#[cfg(all(test, unix))]
struct InstallSelectionFixture {
    query: uuid::Uuid,
    selected: Mutex<Option<super::search::Selection>>,
    lose_stage_ack: std::sync::atomic::AtomicBool,
    lose_activation_ack: std::sync::atomic::AtomicBool,
}
#[cfg(unix)]
impl InstallContext {
    pub(in crate::marketplace) fn upload_source(&self) -> bool {
        self.direct.as_ref().is_some_and(|s| s.upload_source)
    }
    pub(in crate::marketplace) fn with_upload(mut self) -> Result<Self> {
        self.direct
            .as_mut()
            .context("SDK upload requires explicit direct-source consent")?
            .upload_source = true;
        Ok(self)
    }
    pub(in crate::marketplace) fn direct_reference(
        &self,
    ) -> Option<elixir_marketplace::identity::Digest> {
        self.direct.as_ref().map(|source| source.reference)
    }
    pub(in crate::marketplace) fn with_direct(
        mut self,
        principal: crate::http::auth::CurrentPrincipal,
        publication: elixir_marketplace::publication::PreparedPublication,
        reference: elixir_marketplace::identity::Digest,
        query: uuid::Uuid,
    ) -> Self {
        self.direct = Some(DirectInstallSource {
            principal,
            publication,
            reference,
            query,
            upload_source: false,
        });
        self
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_empty(
        client: ParticipationClient,
        query: uuid::Uuid,
    ) -> Self {
        Self {
            inner: Arc::default(),
            search: Arc::default(),
            preparation: Default::default(),
            client,
            direct: None,
            fixture: Some(InstallSelectionFixture {
                query,
                selected: Mutex::new(None),
                lose_stage_ack: std::sync::atomic::AtomicBool::new(false),
                lose_activation_ack: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }
    /// Synthetic selection/admission ONLY; the supplied Selection still owns
    /// its actual original native client. Never compiled into production.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_selected(
        client: ParticipationClient,
        query: uuid::Uuid,
        selected: super::search::Selection,
        lose_stage_ack: bool,
    ) -> Self {
        Self {
            inner: Arc::default(),
            search: Arc::default(),
            preparation: Default::default(),
            client,
            direct: None,
            fixture: Some(InstallSelectionFixture {
                query,
                selected: Mutex::new(Some(selected)),
                lose_stage_ack: std::sync::atomic::AtomicBool::new(lose_stage_ack),
                lose_activation_ack: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_lose_stage_ack(&self) -> bool {
        self.fixture.as_ref().is_some_and(|fixture| {
            fixture
                .lose_stage_ack
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        })
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_discard_activation_ack(self) -> Self {
        self.fixture
            .as_ref()
            .expect("private native fixture")
            .lose_activation_ack
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_lose_activation_ack(&self) -> bool {
        self.fixture.as_ref().is_some_and(|fixture| {
            fixture
                .lose_activation_ack
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        })
    }
    pub(in crate::marketplace) async fn select(
        &self,
        id: uuid::Uuid,
        extension: &elixir_marketplace::identity::ExtensionId,
        release: elixir_marketplace::identity::Digest,
    ) -> Result<super::search::Selection> {
        #[cfg(test)]
        if let Some(fixture) = &self.fixture {
            if fixture.query != id {
                bail!("fixture query differs from original command")
            }
            let mut selected = fixture
                .selected
                .lock()
                .map_err(|_| anyhow::anyhow!("fixture selection poisoned"))?
                .take()
                .context("original fixture selection already handed off")?;
            if !selected.client().same_owner(&self.client) {
                bail!("fixture cannot replace the original native client")
            }
            let metadata = selected.metadata()?;
            if &metadata.release_payload().extension_id != extension
                || elixir_marketplace::identity::Digest::of(elixir_marketplace::canonical::to_vec(
                    metadata.release_payload(),
                )?) != release
            {
                bail!("fixture selection differs from original release")
            }
            selected.maintain().await?;
            return Ok(selected);
        }
        if self.inner.stop_requested() || !self.client.same_owner(&self.inner.client()?) {
            bail!("original marketplace owner changed")
        }
        let selected = if let Some(source) = &self.direct {
            if source.query != id
                || &source.publication.release.payload.extension_id != extension
                || source.publication.release_id()? != release
            {
                bail!("direct source differs from original command");
            }
            self.search
                .select_direct(
                    &self.client,
                    &source.principal,
                    source.publication.clone(),
                    source.reference,
                )
                .await?
        } else {
            self.search
                .select(&self.client, id, extension, release)
                .await?
        };
        if self.inner.stop_requested() || !self.client.same_owner(&self.inner.client()?) {
            bail!("original marketplace owner changed during selection")
        }
        Ok(selected)
    }
}

/// One local startup generation. Drop/cancellation cannot restore an old
/// positive status or claim that partially prepared resources were cleaned up.
pub(crate) struct StartupAttempt {
    inner: Starting<ParticipatingWorker>,
}
impl StartupAttempt {
    pub(crate) fn attach(self, worker: ParticipatingWorker) -> Result<()> {
        self.inner.attach(worker)
    }
}

// Private test seams. Peers/HTTP handlers cannot supply a fake owner/client,
// status, resource claim or network factory through this interface.
trait Status: Clone + Send + Sync + 'static {
    fn snapshot(&self) -> ParticipationSnapshot;
}
impl Status for ParticipationClient {
    fn snapshot(&self) -> ParticipationSnapshot {
        ParticipationClient::snapshot(self)
    }
}
#[async_trait]
trait Owned: Send + 'static {
    type Client: Status;
    fn client(&self) -> Self::Client;
    fn request_stop(&mut self);
    async fn stop(self) -> Result<()>;
    async fn retire(self) -> Result<()>;
}
#[async_trait]
impl Owned for ParticipatingWorker {
    type Client = ParticipationClient;
    fn client(&self) -> Self::Client {
        ParticipatingWorker::client(self)
    }
    fn request_stop(&mut self) {
        ParticipatingWorker::request_stop(self);
    }
    async fn stop(self) -> Result<()> {
        ParticipatingWorker::stop(self).await
    }
    async fn retire(self) -> Result<()> {
        ParticipatingWorker::retire(self).await
    }
}
struct Slot<W: Owned> {
    owner: Option<W>,
    client: Option<W::Client>,
    phase: RuntimePhase,
    closing: bool,
    generation: u64,
    stop_requested: bool,
}
struct Managed<W: Owned> {
    slot: Mutex<Slot<W>>,
}
impl<W: Owned> Default for Managed<W> {
    fn default() -> Self {
        Self {
            slot: Mutex::new(Slot {
                owner: None,
                client: None,
                phase: RuntimePhase::NotConnected,
                closing: false,
                generation: 0,
                stop_requested: false,
            }),
        }
    }
}
impl<W: Owned> Managed<W> {
    fn finish_cancelled_startup(&self) -> Result<()> {
        let mut slot = self
            .slot
            .lock()
            .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
        if !slot.stop_requested
            || slot.owner.is_some()
            || slot.client.is_some()
            || slot.closing
            || !matches!(slot.phase, RuntimePhase::Failed | RuntimePhase::Stopped)
        {
            bail!("cancelled startup slot is not exclusively vacant and stopping");
        }
        slot.phase = RuntimePhase::Stopped;
        Ok(())
    }
    fn begin_startup(self: &Arc<Self>) -> Result<Starting<W>> {
        self.begin_at(RuntimePhase::NotConnected)
    }
    fn begin_recovery(
        self: &Arc<Self>,
        leases: &super::startup::StartupLeases,
    ) -> Result<Starting<W>> {
        if !leases.is_current() {
            bail!("marketplace recovery lacks exclusive controller ownership");
        }
        self.begin_at(RuntimePhase::Failed)
    }
    fn begin_at(self: &Arc<Self>, expected: RuntimePhase) -> Result<Starting<W>> {
        let mut slot = self
            .slot
            .lock()
            .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
        if slot.owner.is_some()
            || slot.client.is_some()
            || slot.closing
            || slot.stop_requested
            || slot.phase != expected
        {
            bail!("marketplace startup owner slot is unavailable");
        }
        slot.generation = slot
            .generation
            .checked_add(1)
            .context("marketplace owner generation exhausted")?;
        slot.phase = RuntimePhase::Starting;
        Ok(Starting {
            inner: self.clone(),
            generation: slot.generation,
            completed: false,
        })
    }
    fn attach(&self, worker: W) -> Result<()> {
        self.attach_generation(worker, None)
    }
    fn attach_generation(&self, mut worker: W, starting: Option<u64>) -> Result<()> {
        let mut slot = self
            .slot
            .lock()
            .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
        if slot.owner.is_some()
            || slot.closing
            || slot.stop_requested
            || match starting {
                Some(generation) => {
                    slot.generation != generation || slot.phase != RuntimePhase::Starting
                }
                None => slot.phase != RuntimePhase::NotConnected,
            }
        {
            worker.request_stop();
            bail!("marketplace owner slot is unavailable");
        }
        slot.generation = slot
            .generation
            .checked_add(1)
            .context("marketplace owner generation exhausted")?;
        slot.client = Some(worker.client());
        slot.owner = Some(worker);
        slot.phase = RuntimePhase::Starting;
        Ok(())
    }
    fn snapshot(&self) -> RuntimeSnapshot {
        let Ok(slot) = self.slot.lock() else {
            return RuntimeSnapshot {
                phase: RuntimePhase::Failed,
                participation: None,
            };
        };
        let mut participation = slot.client.as_ref().map(Status::snapshot);
        let phase = match slot.phase {
            RuntimePhase::Stopping | RuntimePhase::Stopped | RuntimePhase::Failed => slot.phase,
            _ => participation
                .as_ref()
                .map_or(slot.phase, |state| match state.phase {
                    ParticipationPhase::Starting => RuntimePhase::Starting,
                    ParticipationPhase::Running => RuntimePhase::Running,
                    ParticipationPhase::Applying => RuntimePhase::Applying,
                    ParticipationPhase::WaitingForProtection => RuntimePhase::WaitingForProtection,
                    ParticipationPhase::Stopping => RuntimePhase::Stopping,
                    ParticipationPhase::Stopped => RuntimePhase::Stopped,
                    ParticipationPhase::Failed => RuntimePhase::Failed,
                }),
        };
        if matches!(phase, RuntimePhase::Stopping | RuntimePhase::Failed) {
            // Ownership loss/pending shutdown invalidates any earlier applied
            // state, even before the underlying watcher publishes its failure.
            if let Some(state) = &mut participation {
                state.phase = if phase == RuntimePhase::Stopping {
                    ParticipationPhase::Stopping
                } else {
                    ParticipationPhase::Failed
                };
                state.applied_roles = None;
                state.catalog = None;
            }
        }
        RuntimeSnapshot {
            phase,
            participation,
        }
    }
    fn client(&self) -> Result<W::Client> {
        let slot = self
            .slot
            .lock()
            .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
        if slot.owner.is_none()
            || slot.closing
            || matches!(
                slot.phase,
                RuntimePhase::Stopping | RuntimePhase::Stopped | RuntimePhase::Failed
            )
        {
            bail!("marketplace worker is unavailable");
        }
        let client = slot
            .client
            .clone()
            .context("marketplace worker is not connected")?;
        if matches!(
            client.snapshot().phase,
            ParticipationPhase::Stopping | ParticipationPhase::Stopped | ParticipationPhase::Failed
        ) {
            bail!("marketplace worker is unavailable");
        }
        Ok(client)
    }
    fn stop_requested(&self) -> bool {
        match self.slot.lock() {
            Ok(slot) => slot.stop_requested,
            Err(poisoned) => {
                // Poison never restores authority or a positive state. Read
                // the held owner only to send its independent stop signal.
                let mut slot = poisoned.into_inner();
                slot.stop_requested = true;
                slot.phase = RuntimePhase::Failed;
                if let Some(owner) = slot.owner.as_mut() {
                    owner.request_stop();
                }
                true
            }
        }
    }
    fn request_stop(&self) {
        match self.slot.lock() {
            Ok(mut slot) => {
                slot.stop_requested = true;
                if slot.phase == RuntimePhase::Stopped
                    || (slot.phase == RuntimePhase::Failed && slot.owner.is_none())
                {
                    return;
                }
                slot.phase = RuntimePhase::Stopping;
                if let Some(owner) = slot.owner.as_mut() {
                    owner.request_stop();
                }
            }
            Err(poisoned) => {
                let mut slot = poisoned.into_inner();
                slot.stop_requested = true;
                slot.phase = RuntimePhase::Failed;
                if let Some(owner) = slot.owner.as_mut() {
                    owner.request_stop();
                }
            }
        }
    }
    async fn shutdown(self: Arc<Self>) -> Result<()> {
        self.request_stop();
        let (owner, generation) = {
            let mut slot = self
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
            if slot.closing {
                bail!("marketplace shutdown is already in progress");
            }
            let Some(mut owner) = slot.owner.take() else {
                if slot.phase == RuntimePhase::Stopped {
                    return Ok(());
                }
                if slot.client.is_some() || slot.phase == RuntimePhase::Failed {
                    slot.phase = RuntimePhase::Failed;
                    bail!("marketplace shutdown outcome is unknown");
                }
                slot.phase = RuntimePhase::Stopped;
                return Ok(());
            };
            owner.request_stop();
            slot.closing = true;
            (owner, slot.generation)
        };
        let mut pending = PendingStop {
            inner: self.clone(),
            generation,
            finished: false,
        };
        let mut result = timeout(Duration::from_secs(75), owner.stop())
            .await
            .context("marketplace shutdown deadline")
            .and_then(|result| result);
        {
            let mut slot = self
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
            if result.is_ok()
                && !slot.client.as_ref().is_some_and(|client| {
                    let state = client.snapshot();
                    state.phase == ParticipationPhase::Stopped
                        && state.applied_roles == Some(Default::default())
                        && !state.package_sharing_active
                })
            {
                result = Err(anyhow::anyhow!(
                    "marketplace cleanup acknowledgement is unavailable"
                ));
            }
            slot.closing = false;
            slot.phase = if result.is_ok() {
                RuntimePhase::Stopped
            } else {
                RuntimePhase::Failed
            };
        }
        pending.finished = true;
        result
    }
    async fn recover_failed(self: Arc<Self>) -> Result<bool> {
        let (owner, generation, next_generation) = {
            let mut slot = self
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
            if slot.stop_requested {
                return Ok(false);
            }
            if slot.closing
                || slot
                    .client
                    .as_ref()
                    .is_none_or(|client| client.snapshot().phase != ParticipationPhase::Failed)
            {
                bail!("marketplace recovery requires one failed attached owner");
            }
            let next_generation = slot
                .generation
                .checked_add(1)
                .context("marketplace owner generation exhausted")?;
            let mut owner = slot
                .owner
                .take()
                .context("marketplace cleanup ownership is unknown")?;
            owner.request_stop();
            slot.closing = true;
            slot.phase = RuntimePhase::Stopping;
            (owner, slot.generation, next_generation)
        };
        let mut pending = PendingStop {
            inner: self.clone(),
            generation,
            finished: false,
        };
        let mut result = timeout(Duration::from_secs(75), owner.retire())
            .await
            .context("marketplace recovery cleanup deadline")
            .and_then(|result| result);
        let rearmed = {
            let mut slot = self
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("marketplace ownership state is unavailable"))?;
            if result.is_ok()
                && (slot.generation != generation
                    || slot.owner.is_some()
                    || !slot.client.as_ref().is_some_and(|client| {
                        let state = client.snapshot();
                        matches!(
                            state.phase,
                            ParticipationPhase::Stopped | ParticipationPhase::Failed
                        ) && state.applied_roles == Some(Default::default())
                            && state.desired_roles == Default::default()
                            && !state.package_sharing_active
                    }))
            {
                result = Err(anyhow::anyhow!(
                    "marketplace recovery cleanup acknowledgement is unavailable"
                ));
            }
            slot.closing = false;
            if result.is_err() {
                slot.phase = RuntimePhase::Failed;
                false
            } else if slot.stop_requested {
                // The joined receipt proves shutdown, but the old client may
                // retain its operational Failed status. Do not expose a mixed
                // Stopped/Failed public snapshot or retain a stale owner view.
                slot.client = None;
                slot.phase = RuntimePhase::Stopped;
                false
            } else {
                slot.client = None;
                slot.generation = next_generation;
                slot.phase = RuntimePhase::NotConnected;
                true
            }
        };
        pending.finished = true;
        result?;
        Ok(rearmed)
    }
}
struct Starting<W: Owned> {
    inner: Arc<Managed<W>>,
    generation: u64,
    completed: bool,
}
impl<W: Owned> Starting<W> {
    fn attach(mut self, worker: W) -> Result<()> {
        self.inner
            .attach_generation(worker, Some(self.generation))?;
        self.completed = true;
        Ok(())
    }
}
impl<W: Owned> Drop for Starting<W> {
    fn drop(&mut self) {
        if !self.completed
            && let Ok(mut slot) = self.inner.slot.lock()
            && slot.generation == self.generation
            && slot.owner.is_none()
        {
            slot.phase = RuntimePhase::Failed;
        }
    }
}
struct PendingStop<W: Owned> {
    inner: Arc<Managed<W>>,
    generation: u64,
    finished: bool,
}
impl<W: Owned> Drop for PendingStop<W> {
    fn drop(&mut self) {
        if !self.finished
            && let Ok(mut slot) = self.inner.slot.lock()
            && slot.generation == self.generation
        {
            slot.phase = RuntimePhase::Failed;
            slot.closing = false;
        }
    }
}

#[cfg(test)]
mod tests;
