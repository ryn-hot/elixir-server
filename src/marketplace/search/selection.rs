//! One owner-selected discovery fact, never an install/runtime/transfer grant.
//! No raw peer DTO or client-supplied "verified" flag constructs this object.
use super::*;
use crate::marketplace::selected_source::{AcceptedPublication, SelectedSource};
use std::collections::BTreeSet;
use tokio::sync::OwnedSemaphorePermit;

const MIN_LIFETIME: Duration = Duration::from_secs(2);
const CHECK_DEADLINE: Duration = Duration::from_secs(10);
// An owned transfer can occupy the same private pipe while this read waits.
// Two existing 10s durable checks bracket one existing 30s capacity+RPC wait.
// Do not cut off legitimate work at 10s; allow only that 50s total + scheduling
// margin. Inner deadlines, native/source TTLs and independent authorization/
// protection monitoring remain unchanged; timeout never renews authority.
const CURRENT_CHECK_DEADLINE: Duration = Duration::from_secs(55);
pub(in crate::marketplace) const MAX_CATALOG_PEERS: usize = 6;
mod source_failures;
pub(super) use source_failures::SourceFailures;

pub(super) struct Selected {
    pub(super) value: Arc<Verified>,
    pub(super) peers: Vec<PeerKey>,
    // At most one retained selection across query replacements. It holds no
    // old remote session, cursor history or per-query backend after handoff.
    _capacity: OwnedSemaphorePermit,
}
// Distinct from an admitted search card. Only a fresh owned source preflight
// can populate this installation-side capability in production.
struct InstalledSelected {
    candidate: Selected,
    metadata: MetadataObservation,
    source: PublicSourceObservation,
}
#[cfg(test)]
impl Selected {
    fn fixture_install(self) -> Box<InstalledSelected> {
        let metadata = self.value.metadata.retained_copy();
        let source = self.value.source.fixture_install_source();
        Box::new(InstalledSelected {
            candidate: self,
            metadata,
            source,
        })
    }
}
pub(in crate::marketplace) struct Selection {
    client: ParticipationClient,
    selected: SelectionValue,
    invalidated: bool,
}
enum SelectionValue {
    // Both variants own one bounded selection. Keep signed metadata off every
    // enclosing async task frame as the prelaunch record gains small fields.
    Discovery(Box<InstalledSelected>),
    Direct(Box<DirectSelected>),
}
struct DirectSelected {
    accepted: AcceptedPublication,
    source: PublicSourceObservation,
    _capacity: OwnedSemaphorePermit,
}
impl Selection {
    /// Synthetic receipt/protection owner only; the direct capability still
    /// authenticates its real SQLite/Postgres owner and strict security fences.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_native_direct(
        client: ParticipationClient,
        accepted: AcceptedPublication,
        source: PublicSourceObservation,
    ) -> Result<Self> {
        let value = Self {
            client,
            invalidated: false,
            selected: SelectionValue::Direct(Box::new(DirectSelected {
                accepted,
                source,
                _capacity: Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned()?,
            })),
        };
        value.require_local()?;
        value.metadata()?.authority_record()?;
        Ok(value)
    }
    fn source_unchecked(&self) -> &dyn SelectedSource {
        match &self.selected {
            SelectionValue::Discovery(selected) => &selected.metadata,
            SelectionValue::Direct(selected) => &selected.accepted,
        }
    }
    fn valid_for(&self) -> Duration {
        match &self.selected {
            SelectionValue::Discovery(selected) => selected
                .metadata
                .valid_for()
                .min(selected.source.valid_for()),
            SelectionValue::Direct(selected) => selected
                .accepted
                .valid_for()
                .min(selected.source.valid_for()),
        }
    }
    /// Synthetic admission/protection observations ONLY for offline native
    /// composition tests. No production constructor or deserializer exists.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_native_preparation(
        client: ParticipationClient,
        metadata: MetadataObservation,
        source: PublicSourceObservation,
        peers: Vec<PeerKey>,
    ) -> Result<Self> {
        let value = Self {
            client,
            selected: SelectionValue::Discovery(Box::new(InstalledSelected {
                candidate: Selected {
                    value: Arc::new(Verified::new(
                        metadata.retained_copy(),
                        SearchSourceObservation::from_source(&source)?,
                    )?),
                    peers,
                    _capacity: Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned()?,
                },
                metadata,
                source,
            })),
            invalidated: false,
        };
        value.require_local()?;
        value.metadata()?.authority_record()?;
        Ok(value)
    }
    fn require_local(&self) -> Result<()> {
        self.client.require_blob_owner()?;
        if self.invalidated || self.valid_for() < MIN_LIFETIME {
            bail!("selected marketplace facts require refresh");
        }
        Ok(())
    }
    /// Checked source admission, not necessarily discovery eligibility.
    pub(in crate::marketplace) fn metadata(&self) -> Result<&dyn SelectedSource> {
        self.require_local()?;
        Ok(self.source_unchecked())
    }
    pub(in crate::marketplace) fn discovery_metadata(&self) -> Result<&MetadataObservation> {
        self.require_local()?;
        self.source_unchecked()
            .discovery()
            .context("direct acceptance is not paid discovery admission")
    }
    /// Only existing catalog identities seen by discovery. They may not host
    /// these bytes; every body still needs its signed digest/size and native
    /// quarantine. No addresses, URLs, downloads or packet credit are granted.
    pub(in crate::marketplace) fn catalog_peers(&self) -> Result<&[PeerKey]> {
        self.require_local()?;
        Ok(match &self.selected {
            SelectionValue::Discovery(selected) => &selected.candidate.peers,
            SelectionValue::Direct(_) => &[],
        })
    }
    pub(in crate::marketplace) fn client(&self) -> &ParticipationClient {
        &self.client
    }
    pub(in crate::marketplace) fn public_source(&self) -> Result<PublicSourceObservation> {
        self.require_local()?;
        Ok(match &self.selected {
            SelectionValue::Discovery(selected) => selected.source.retained_copy(),
            SelectionValue::Direct(selected) => selected.source.retained_copy(),
        })
    }
    pub(in crate::marketplace) async fn maintain(&mut self) -> Result<()> {
        if self.invalidated || self.valid_for() < Duration::from_secs(30) {
            self.refresh().await
        } else {
            self.current().await
        }
    }
    /// Exact selected release only. This guard may renew observations while a
    /// long protected transfer runs; it never changes the chosen descriptor.
    pub(in crate::marketplace) async fn watch_current(&mut self) -> Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.maintain().await?;
        }
    }
    /// One source workspace admitted against the actual protected owner's host.
    /// The controller supplies an explicit foreground budget, not HTTP/peer
    /// JSON. This opens no peer connection and grants no install/execution right.
    pub(in crate::marketplace) async fn open_quarantine_retained(
        &mut self,
        slot: &mut Option<crate::marketplace::quarantine::SourceQuarantineJob>,
        artifact: Option<usize>,
        budget: &crate::marketplace::quarantine::SourceJobBudget,
        runtime: Option<Arc<dyn crate::runtime::RuntimeManager>>,
    ) -> Result<()> {
        self.require_local()?;
        self.invalidated = true;
        let result = self
            .client
            .open_source_quarantine_retained(
                slot,
                self.source_unchecked(),
                artifact,
                budget,
                runtime,
            )
            .await;
        let retryable = result
            .as_ref()
            .err()
            .is_some_and(ParticipationClient::source_resources_deferred)
            && slot.as_ref().is_some_and(
                crate::marketplace::quarantine::SourceQuarantineJob::admission_unstarted,
            );
        if result.is_ok() || retryable {
            // A scheduling denial cannot refresh stale authority. Recheck the
            // SAME original observation/owner before allowing a bounded retry.
            self.client.require_blob_owner()?;
            self.client
                .check_selected_source(self.source_unchecked())
                .await?;
            if self.valid_for() < MIN_LIFETIME {
                bail!("selection expired during foreground admission");
            }
            self.invalidated = false;
        }
        result
    }
    #[cfg(unix)]
    pub(in crate::marketplace) async fn open_activation_source(
        &mut self,
        slot: &mut Option<crate::marketplace::quarantine::SourceQuarantineJob>,
        archive: &crate::marketplace::archive::ArchivedSource,
        start: crate::marketplace::grant_store::installation::activation::ActivationStart,
        plan: &crate::marketplace::grant_store::installation::activation::SourceActivationPlan,
        budget: crate::marketplace::grant_store::installation::InstallBudget,
    ) -> Result<crate::marketplace::grant_store::installation::activation::SourceOpening> {
        use crate::marketplace::grant_store::installation::activation::SourceOpen;
        self.require_local()?;
        self.invalidated = true;
        let public = match &self.selected {
            SelectionValue::Discovery(selected) => selected.source.retained_copy(),
            SelectionValue::Direct(selected) => selected.source.retained_copy(),
        };
        let request = SourceOpen {
            metadata: self.source_unchecked(),
            public: &public,
            archive,
            plan,
            budget,
        };
        let outcome = self
            .client
            .open_activation_source(slot, start, &request)
            .await?;
        if let Err(error) = self.client.require_blob_owner().and_then(|()| {
            if self.valid_for() < MIN_LIFETIME {
                bail!("selection expired during activation source admission");
            }
            Ok(())
        }) {
            return Err(error);
        }
        self.invalidated = false;
        Ok(outcome)
    }
    pub(in crate::marketplace) async fn current(&mut self) -> Result<()> {
        self.require_local()?;
        self.invalidated = true;
        timeout(
            CURRENT_CHECK_DEADLINE,
            self.client.check_selected_source(self.source_unchecked()),
        )
        .await
        .context("selected marketplace security check timed out")??;
        self.client.require_blob_owner()?;
        if self.valid_for() < MIN_LIFETIME {
            bail!("selection expired during security check");
        }
        self.invalidated = false;
        Ok(())
    }
    /// Renew expiring observations by independently verifying the SAME signed
    /// descriptor. Never substitute a newer release or bypass paid adoption,
    /// current publisher/head fences, public source or permission review.
    pub(in crate::marketplace) async fn refresh(&mut self) -> Result<()> {
        self.invalidated = true;
        self.client.require_blob_owner()?;
        if let SelectionValue::Direct(selected) = &self.selected {
            let observation = self
                .client
                .verify_publication(selected.accepted.observation().publication())
                .await?;
            let accepted = selected.accepted.renew(observation)?;
            self.client.check_selected_source(&accepted).await?;
            let mut failures = SourceFailures::default();
            for location in 0..accepted.release_payload().source.locations.len() {
                if let Some(source) = failures.record(
                    self.client
                        .preflight_fresh_publication(accepted.observation(), location)
                        .await,
                ) {
                    self.client.check_selected_source(&accepted).await?;
                    if accepted.valid_for().min(source.valid_for()) < MIN_LIFETIME {
                        bail!("refreshed direct source expires too soon");
                    }
                    let SelectionValue::Direct(selected) = &mut self.selected else {
                        unreachable!()
                    };
                    selected.accepted = accepted;
                    selected.source = source;
                    self.invalidated = false;
                    return Ok(());
                }
            }
            return Err(failures.unavailable("accepted direct public source is unavailable"));
        }
        let SelectionValue::Discovery(selected) = &self.selected else {
            unreachable!()
        };
        let descriptor = selected.metadata.descriptor();
        let expected = selected.source.release_id();
        let metadata = self.client.verify_selection_metadata(descriptor).await?;
        if metadata.descriptor().head.payload.stable_release_id != expected
            || Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?) != expected
        {
            bail!("selected release changed during refresh");
        }
        let mut failures = SourceFailures::default();
        for location in 0..metadata.descriptor().release.payload.source.locations.len() {
            self.client.require_blob_owner()?;
            if let Some(source) = failures.record(
                self.client
                    .preflight_fresh_metadata(&metadata, location)
                    .await,
            ) {
                if metadata.valid_for().min(source.valid_for()) < MIN_LIFETIME {
                    bail!("refreshed selection expires too soon");
                }
                self.client.check_catalog_current(&metadata).await?;
                if metadata.valid_for().min(source.valid_for()) < MIN_LIFETIME {
                    bail!("refreshed selection expired during security check");
                }
                let SelectionValue::Discovery(selected) = &mut self.selected else {
                    unreachable!()
                };
                selected.metadata = metadata;
                selected.source = source;
                self.invalidated = false;
                return Ok(());
            }
        }
        Err(failures.unavailable("selected public source is unavailable"))
    }
}

impl SearchService {
    pub(in crate::marketplace) async fn select(
        &self,
        client: &ParticipationClient,
        id: Uuid,
        extension: &ExtensionId,
        release: Digest,
    ) -> Result<Selection> {
        client.require_blob_owner()?;
        let selected = self
            .selected(id, extension, release, |backend| backend.same_owner(client))
            .await?;
        client.require_blob_owner()?;
        let metadata = client
            .verify_selection_metadata(selected.value.metadata.descriptor())
            .await?;
        let mut fresh = None;
        let mut failures = SourceFailures::default();
        for location in 0..metadata.descriptor().release.payload.source.locations.len() {
            client.require_blob_owner()?;
            if let Some(source) =
                failures.record(client.preflight_fresh_metadata(&metadata, location).await)
            {
                fresh = Some(source);
                break;
            }
        }
        let source =
            fresh.ok_or_else(|| failures.unavailable("selected public source unavailable"))?;
        client.check_catalog_current(&metadata).await?;
        client.require_blob_owner()?;
        if source.release_id() != release
            || metadata.valid_for().min(source.valid_for()) < MIN_LIFETIME
        {
            bail!("selected source expired or changed");
        }
        // Source retrieval yields. Do not turn an old/replaced search into a
        // capability after it finishes; the original selection still owns its
        // single reserved slot while these checks run.
        {
            let slot = self
                .inner
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
            let query = slot
                .query
                .as_ref()
                .filter(|query| {
                    query.id == id
                        && query.backend.same_owner(client)
                        && matches!(query.phase, SearchPhase::Checking | SearchPhase::Ready)
                })
                .context("selected search changed during source preflight")?;
            if !query
                .values
                .iter()
                .any(|value| Arc::ptr_eq(value, &selected.value))
            {
                bail!("selected result was replaced");
            }
        }
        Ok(Selection {
            client: client.clone(),
            selected: SelectionValue::Discovery(Box::new(InstalledSelected {
                candidate: selected,
                metadata,
                source,
            })),
            invalidated: false,
        })
    }
    /// Owner-triggered source selection only. No paid proofs, search entry,
    /// arbitrary addresses or reusable preview token are created.
    pub(in crate::marketplace) async fn select_direct(
        &self,
        client: &ParticipationClient,
        principal: &crate::http::auth::CurrentPrincipal,
        publication: elixir_marketplace::publication::PreparedPublication,
        accepted_reference: Digest,
    ) -> Result<Selection> {
        let _view = self
            .views
            .try_acquire()
            .context("marketplace source selection is busy")?;
        let capacity = self
            .selections
            .clone()
            .try_acquire_owned()
            .context("one marketplace release is already selected")?;
        client.require_blob_owner()?;
        if !client.provider_discovery_enabled() {
            bail!("direct source requires protected content discovery");
        }
        let observation = client.verify_publication(&publication).await?;
        let accepted = AcceptedPublication::capture(
            client.source_database(),
            observation,
            principal,
            accepted_reference,
        )
        .await?;
        client.accept_direct_source(&accepted).await?;
        let mut failures = SourceFailures::default();
        for location in 0..accepted.release_payload().source.locations.len() {
            if let Some(source) = failures.record(
                client
                    .preflight_fresh_publication(accepted.observation(), location)
                    .await,
            ) {
                client.check_selected_source(&accepted).await?;
                if accepted.valid_for().min(source.valid_for()) < MIN_LIFETIME {
                    bail!("accepted direct source expires too soon");
                }
                return Ok(Selection {
                    client: client.clone(),
                    invalidated: false,
                    selected: SelectionValue::Direct(Box::new(DirectSelected {
                        accepted,
                        source,
                        _capacity: capacity,
                    })),
                });
            }
        }
        Err(failures.unavailable("accepted direct public source is unavailable"))
    }
    // Private fault-test seam. Production owner matching always comes from the
    // actual client above; neither HTTP nor a peer can provide this closure.
    pub(super) async fn selected<F>(
        &self,
        id: Uuid,
        extension: &ExtensionId,
        release: Digest,
        owner_matches: F,
    ) -> Result<Selected>
    where
        F: Fn(&dyn Backend) -> bool + Send + Sync,
    {
        let _view = self
            .views
            .try_acquire()
            .context("marketplace selection/view is busy")?;
        let capacity = self
            .selections
            .clone()
            .try_acquire_owned()
            .context("one marketplace release is already selected")?;
        let (backend, value) = {
            let mut slot = self
                .inner
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
            let query = slot
                .query
                .as_mut()
                .filter(|query| {
                    query.id == id
                        && matches!(query.phase, SearchPhase::Checking | SearchPhase::Ready)
                })
                .context("selected search was replaced, stopped or unavailable")?;
            if !owner_matches(query.backend.as_ref()) {
                bail!("selected search belongs to another protected owner");
            }
            let value = query
                .values
                .iter()
                .find(|value| {
                    value.source.release_id() == release
                        && &value.metadata.descriptor().release.payload.extension_id == extension
                })
                .context("release is not an admitted result of this search")?
                .clone();
            query.last_active = Instant::now();
            (query.backend.clone(), value)
        };
        if value.valid_for() < MIN_LIFETIME {
            bail!("selected discovery facts expired");
        }
        let peers = timeout(CHECK_DEADLINE, backend.catalog_peers(&value))
            .await
            .context("selection catalog hints timed out")??;
        if peers.len() > MAX_CATALOG_PEERS {
            bail!("too many selection catalog hints");
        }
        let mut seen = BTreeSet::new();
        let peers = peers
            .into_iter()
            .filter(|peer| seen.insert(peer.clone()))
            .collect();
        timeout(CHECK_DEADLINE, backend.current(&value))
            .await
            .context("selection security check timed out")??;
        // Recheck after ALL awaits. Old receipts, another owner, a replaced
        // query or removed/replaced result can never become a source-job seed.
        let slot = self
            .inner
            .slot
            .lock()
            .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
        let query = slot
            .query
            .as_ref()
            .filter(|query| {
                query.id == id
                    && matches!(query.phase, SearchPhase::Checking | SearchPhase::Ready)
                    && owner_matches(query.backend.as_ref())
                    && Arc::ptr_eq(&query.backend, &backend)
            })
            .context("selection owner/query changed during verification")?;
        if value.valid_for() < MIN_LIFETIME
            || !query
                .values
                .iter()
                .any(|current| Arc::ptr_eq(current, &value))
        {
            bail!("selected discovery facts no longer belong to this result page");
        }
        Ok(Selected {
            value,
            peers,
            _capacity: capacity,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Synthetic protected consumer and local signed facts, not native/network
    //! qualification. This uses the existing typed blob-owner test fixture.
    use super::*;
    use std::{future::Future, task::Poll};

    async fn chosen() -> Result<(SearchService, Selected)> {
        let backend = Arc::new(super::super::tests::Fake::new(vec![
            super::super::tests::descriptor(0, 1)?,
        ]));
        let service = SearchService::default();
        let id = service.start_with(backend, super::super::tests::request("example"))?;
        let view = super::super::tests::ready(&service, id).await?;
        let card = view
            .results
            .first()
            .context("selected fixture card missing")?;
        let selected = service
            .selected(id, &card.extension_id, card.release_id, |_| true)
            .await?;
        Ok((service, selected))
    }

    #[tokio::test]
    async fn a_failed_refresh_revokes_old_accessors_without_releasing_a_second_selection_slot()
    -> Result<()> {
        let (service, selected) = chosen().await?;
        let owner = crate::marketplace::worker::synthetic_blob_owner();
        let mut value = Selection {
            client: owner.client.clone(),
            selected: SelectionValue::Discovery(selected.fixture_install()),
            invalidated: false,
        };
        assert!(value.metadata().is_ok());
        assert!(value.catalog_peers().is_ok());
        assert!(value.client().same_owner(&owner.client));
        // The synthetic owner intentionally has no independent source helper.
        assert!(value.refresh().await.is_err());
        assert!(value.metadata().is_err());
        assert!(value.catalog_peers().is_err());
        assert_eq!(service.selections.available_permits(), 0);
        drop(value);
        assert_eq!(service.selections.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_retained_security_check_cannot_leave_earlier_facts_accessible() -> Result<()>
    {
        let (_service, selected) = chosen().await?;
        let owner = crate::marketplace::worker::synthetic_blob_owner();
        let mut value = Selection {
            client: owner.client.clone(),
            selected: SelectionValue::Discovery(selected.fixture_install()),
            invalidated: false,
        };
        // Its empty local test database cannot authenticate retained fences.
        assert!(value.current().await.is_err());
        assert!(value.metadata().is_err());
        assert!(value.catalog_peers().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn a_consumer_without_real_host_context_cannot_create_source_workspaces() -> Result<()> {
        let (_service, selected) = chosen().await?;
        let owner = crate::marketplace::worker::synthetic_blob_owner();
        let mut value = Selection {
            client: owner.client.clone(),
            selected: SelectionValue::Discovery(selected.fixture_install()),
            invalidated: false,
        };
        let mut slot = None;
        let result = value
            .open_quarantine_retained(
                &mut slot,
                None,
                &crate::marketplace::quarantine::tests::budget(),
                Some(Arc::new(crate::runtime::docker::DockerRuntimeManager::new(
                    None,
                ))),
            )
            .await;
        let error = match result {
            Ok(_) => bail!("synthetic consumer opened a source workspace"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("foreground source jobs are unavailable")
        );
        assert!(value.metadata().is_err());
        assert!(value.catalog_peers().is_err());
        assert!(slot.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_pending_security_read_keeps_the_selection_invalidated() -> Result<()> {
        let (_service, selected) = chosen().await?;
        let owner = crate::marketplace::worker::synthetic_blob_owner();
        let mut value = Selection {
            client: owner.client.clone(),
            selected: SelectionValue::Discovery(selected.fixture_install()),
            invalidated: false,
        };
        let mut check = Box::pin(value.current());
        let pending = std::future::poll_fn(|context| {
            Poll::Ready(matches!(check.as_mut().poll(context), Poll::Pending))
        })
        .await;
        assert!(
            pending,
            "the lazy SQL pool must yield before a retained-state result"
        );
        drop(check);
        assert!(value.metadata().is_err());
        assert!(value.catalog_peers().is_err());
        Ok(())
    }
}
