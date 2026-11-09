//! One startup-owned installed-archive contributor. No package cache/copy,
//! foreground lease, publisher paths, session, or automatic execution. Native
//! protection/roles, current consent and ACTIVE binding all apply. Automatic
//! sharing needs paid metadata; explicit publisher seeding binds one capsule.
use super::{
    archive::{ArchiveBudget, SharingReader},
    grant_store::installation::{
        InstallBudget,
        sharing::{SharingSource, hints, publisher},
    },
    preferences,
    service::MarketplaceService,
    worker::ParticipationClient,
};
use crate::db::Database;
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use elixir_marketplace::{
    catalog::SearchQuery,
    identity::Digest,
    peer::{PeerOperation, PeerOutcome},
    policy::ContributionMode,
    worker_control::WorkerControlOperation as Op,
    worker_sharing::{SharingInventory, SharingState},
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use uuid::Uuid;

#[derive(Default, Clone)]
pub(super) struct Service(Arc<Mutex<Option<Arc<Entry>>>>);
struct Entry {
    stop: watch::Sender<bool>,
    task: AsyncMutex<Owned>,
    state: Arc<AsyncMutex<State>>,
}
struct Owned {
    handle: Option<JoinHandle<Result<()>>>,
    failed: bool,
}
#[derive(Clone)]
struct Configuration {
    archive: PathBuf,
    retention: ArchiveBudget,
    installations: InstallBudget,
}
type Lookup = JoinHandle<Result<Option<SharingSource>>>;
struct State {
    configuration: Configuration,
    source: Option<SharingSource>,
    client: Option<ParticipationClient>,
    generation: Digest,
    source_request: Option<Digest>,
    cursor: Option<hints::Cursor>,
    lookup: Option<Lookup>,
    lookup_request: Option<(Digest, Digest)>,
    next_backfill: Instant,
    advertised: Vec<elixir_marketplace::records::Blob>,
    advertised_after: Option<Digest>,
    next_hints: Instant,
    next_refresh: Instant,
    delay: Duration,
    renewed: Option<Instant>,
    io: Io,
}
type ReadTask = JoinHandle<(SharingReader, Result<Vec<u8>>)>;
#[derive(Default)]
struct Io {
    request: Option<Digest>,
    opening: Option<JoinHandle<Result<SharingReader>>>,
    reading: Option<ReadTask>,
    reader: Option<SharingReader>,
    failed: bool,
}
impl Io {
    async fn retire(&mut self) -> Result<()> {
        if let Some(task) = self.opening.as_mut() {
            let result = task.await;
            self.opening = None;
            match result {
                Ok(Ok(reader)) => self.reader = Some(reader),
                Ok(Err(_)) => {}
                Err(_) => self.failed = true,
            }
        }
        if let Some(task) = self.reading.as_mut() {
            let result = task.await;
            self.reading = None;
            match result {
                Ok((reader, _)) => self.reader = Some(reader),
                Err(_) => self.failed = true,
            }
        }
        self.reader = None;
        self.request = None;
        if self.failed {
            bail!("sharing lost its original file-operation owner");
        }
        Ok(())
    }
    async fn read(
        &mut self,
        configuration: &Configuration,
        source: &SharingSource,
        request: Digest,
        blob: &elixir_marketplace::records::Blob,
        offset: u64,
        maximum: usize,
    ) -> Result<Vec<u8>> {
        if self.request != Some(request) {
            self.retire().await?;
            if offset != 0 {
                bail!("sharing cannot resume an unowned file offset");
            }
            let descriptor = source.descriptor(blob)?;
            let path = configuration.archive.clone();
            let budget = configuration.retention;
            let blob = blob.clone();
            self.request = Some(request);
            self.opening = Some(tokio::task::spawn_blocking(move || {
                SharingReader::open(&path, budget, &descriptor, &blob)
            }));
        }
        if let Some(task) = self.opening.as_mut() {
            let result = task.await;
            self.opening = None;
            match result {
                Ok(result) => self.reader = Some(result?),
                Err(error) => {
                    self.failed = true;
                    return Err(error.into());
                }
            }
        }
        if self.reading.is_none() {
            let mut reader = self
                .reader
                .take()
                .context("original sharing reader missing")?;
            self.reading = Some(tokio::task::spawn_blocking(move || {
                let result = reader.read(offset, maximum);
                (reader, result)
            }));
        }
        let task = self
            .reading
            .as_mut()
            .context("original sharing read missing")?;
        let result = task.await;
        self.reading = None;
        match result {
            Ok((reader, bytes)) => {
                self.reader = Some(reader);
                bytes
            }
            Err(error) => {
                self.failed = true;
                Err(error.into())
            }
        }
    }
}
impl Service {
    pub(super) fn begin(
        &self,
        marketplace: MarketplaceService,
        database: Database,
        archive: PathBuf,
        retention: ArchiveBudget,
        installations: InstallBudget,
    ) -> Result<()> {
        retention.validate()?;
        installations.validate()?;
        if !archive.is_absolute() {
            bail!("sharing archive root must be absolute");
        }
        let mut slot = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("sharing owner unavailable"))?;
        if slot.is_some() {
            bail!("installed sharing already owned");
        }
        let executor = tokio::runtime::Handle::try_current()?;
        let (stop, stopped) = watch::channel(false);
        let state = Arc::new(AsyncMutex::new(State {
            configuration: Configuration {
                archive,
                retention,
                installations,
            },
            source: None,
            client: None,
            generation: Digest::of(Uuid::new_v4().as_bytes()),
            source_request: None,
            cursor: None,
            lookup: None,
            lookup_request: None,
            next_backfill: Instant::now() + Duration::from_secs(30),
            advertised: vec![],
            advertised_after: None,
            next_hints: Instant::now(),
            next_refresh: Instant::now(),
            delay: Duration::from_secs(1),
            renewed: None,
            io: Io::default(),
        }));
        let task = executor.spawn(run(marketplace, database, state.clone(), stopped));
        *slot = Some(Arc::new(Entry {
            stop,
            task: AsyncMutex::new(Owned {
                handle: Some(task),
                failed: false,
            }),
            state,
        }));
        Ok(())
    }
    pub(super) fn request_stop(&self) {
        let slot = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = slot.as_ref() {
            entry.stop.send_replace(true);
        }
    }
    pub(super) async fn shutdown(&self) -> Result<()> {
        self.request_stop();
        let entry = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("sharing owner unavailable"))?
            .clone();
        let Some(entry) = entry else { return Ok(()) };
        let mut owned = entry.task.lock().await;
        if let Some(task) = owned.handle.as_mut() {
            let result = timeout(Duration::from_secs(95), &mut *task)
                .await
                .context("sharing retirement pending; original owner retained")?;
            owned.handle = None;
            owned.failed = !matches!(result, Ok(Ok(())));
        }
        entry.state.lock().await.retire().await?;
        if owned.failed {
            bail!("installed sharing owner did not confirm retirement");
        }
        Ok(())
    }
}
impl State {
    async fn revoke(&mut self) -> Result<()> {
        self.generation = Digest::of(Uuid::new_v4().as_bytes());
        if let Some(client) = &self.client
            && client.sharing_supported()
        {
            // Empty means no serving authority, not a claim that sockets stopped.
            let _ = client
                .sharing_control(Op::SharingSet {
                    inventory: SharingInventory {
                        generation: Digest::of(Uuid::new_v4().as_bytes()),
                        blobs: vec![],
                    },
                    lease_ms: 1,
                    resolve_installed: false,
                })
                .await;
        }
        self.renewed = None;
        self.source = None;
        self.source_request = None;
        self.io.retire().await
    }
    async fn retire(&mut self) -> Result<()> {
        let revoked = self.revoke().await;
        let mut joined = Ok(());
        if let Some(task) = self.lookup.as_mut() {
            let result = task.await;
            self.lookup = None;
            if result.is_err() {
                joined = Err(anyhow::anyhow!(
                    "sharing lost its original metadata lookup owner"
                ));
            }
        }
        revoked?;
        joined
    }
    async fn finish_lookup(&mut self, generation: Digest, request: Digest) -> Result<bool> {
        if !self.lookup.as_ref().is_some_and(JoinHandle::is_finished) {
            return Ok(false);
        }
        let outcome = self.lookup.as_mut().unwrap().await;
        self.lookup = None;
        let original = self.lookup_request.take();
        let candidate = match outcome.context("original sharing lookup owner failed")? {
            Ok(candidate) => candidate,
            Err(error) if ParticipationClient::sharing_verification_deferred(&error) => {
                tracing::debug!(target: "elixir_marketplace::sharing_diagnostics",
                    stage = "verification_deferred", "installed sharing verification deferred");
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if original != Some((generation, request)) {
            return Ok(false);
        }
        self.io.retire().await?;
        self.source = candidate;
        Ok(true)
    }
    async fn step(&mut self, marketplace: &MarketplaceService, database: &Database) -> Result<()> {
        self.delay = Duration::from_secs(1);
        let client = match marketplace.client() {
            Ok(client) => client,
            Err(_) => {
                self.revoke().await?;
                return Ok(());
            }
        };
        if self
            .client
            .as_ref()
            .is_some_and(|old| !old.same_owner(&client))
        {
            self.retire().await?;
        }
        self.client = Some(client.clone());
        if !allowed(marketplace, database, &client).await? {
            if self.source.is_some() || self.renewed.is_some() {
                self.revoke().await?;
            }
            return Ok(());
        }
        // Renew the SAME active archive's metadata before its five-minute
        // observation expires. This never changes/reopens an in-flight file.
        if self.lookup_request.is_none()
            && self.lookup.as_ref().is_some_and(JoinHandle::is_finished)
        {
            let outcome = self.lookup.as_mut().unwrap().await;
            self.lookup = None;
            if let Ok(Some(fresh)) = outcome.context("original refresh owner failed")? {
                if self
                    .source
                    .as_ref()
                    .is_some_and(|old| old.same_archive(&fresh))
                {
                    self.source = Some(fresh);
                } else {
                    self.revoke().await?;
                }
            }
            self.next_refresh = Instant::now() + Duration::from_secs(15);
        }
        if self.io.request.is_some()
            && self.lookup.is_none()
            && Instant::now() >= self.next_refresh
            && self
                .source
                .as_ref()
                .is_some_and(|source| renewal_due(source.valid_for()))
        {
            let digest = self.source.as_ref().unwrap().inventory()?.blobs[0].sha256;
            let database = database.clone();
            let client = client.clone();
            let budget = self.configuration.installations;
            self.lookup_request = None;
            self.lookup = Some(tokio::spawn(lookup(database, client, digest, budget)));
        }
        // One bounded legacy-index repair per tick, never a full scan/list.
        // Ordinary installation writes keep hints atomic without this repair.
        if self.io.request.is_none()
            && self.lookup.is_none()
            && Instant::now() >= self.next_backfill
        {
            self.cursor = hints::backfill(
                database,
                self.cursor.as_ref(),
                self.configuration.installations,
            )
            .await?;
            self.next_backfill =
                Instant::now() + Duration::from_secs(if self.cursor.is_some() { 1 } else { 1800 });
        }
        if Instant::now() >= self.next_hints {
            let mut page = hints::page(database, self.advertised_after).await?;
            if page.is_empty() && self.advertised_after.is_some() {
                page = hints::page(database, None).await?;
            }
            self.advertised_after = page.last().map(|blob| blob.sha256);
            self.advertised = page;
            self.next_hints = Instant::now() + Duration::from_secs(30);
        }
        if self
            .renewed
            .is_none_or(|at| at.elapsed() >= Duration::from_secs(3))
        {
            let started = Instant::now();
            client
                .sharing_control(Op::SharingSet {
                    inventory: SharingInventory {
                        generation: self.generation,
                        blobs: self.advertised.clone(),
                    },
                    lease_ms: 15_000,
                    resolve_installed: !self.advertised.is_empty(),
                })
                .await?;
            self.renewed = Some(started);
        }
        match client.sharing_control(Op::SharingPoll).await? {
            SharingState::Resolve {
                generation,
                request,
                sha256,
            } => {
                self.delay = Duration::from_millis(250);
                if generation != self.generation {
                    bail!("sharing request changed its serving lease");
                }
                // A different request cannot displace an original running
                // verification. Retain/join it before another lookup starts.
                let resolved = self.finish_lookup(generation, request).await?;
                let reusable = match self.source.as_ref() {
                    Some(source) => {
                        source
                            .inventory()
                            .is_ok_and(|i| i.blobs.iter().any(|b| b.sha256 == sha256))
                            && source.current(database).await.is_ok()
                    }
                    None => false,
                };
                if reusable {
                    let blob = self
                        .source
                        .as_ref()
                        .unwrap()
                        .inventory()?
                        .blobs
                        .into_iter()
                        .find(|b| b.sha256 == sha256);
                    let state = client
                        .sharing_control(Op::SharingAuthorize {
                            generation,
                            request,
                            blob,
                        })
                        .await?;
                    accepted(state, request, 0)?;
                    self.source_request = Some(request);
                } else if resolved {
                    accepted(
                        client
                            .sharing_control(Op::SharingAuthorize {
                                generation,
                                request,
                                blob: None,
                            })
                            .await?,
                        request,
                        0,
                    )?;
                    self.source_request = None;
                } else if self.lookup.is_none() {
                    let database = database.clone();
                    let client = client.clone();
                    let budget = self.configuration.installations;
                    self.lookup_request = Some((generation, request));
                    self.lookup = Some(tokio::spawn(lookup(database, client, sha256, budget)));
                }
            }
            SharingState::Read {
                generation,
                request,
                blob,
                offset,
                maximum,
            } => {
                let source = self
                    .source
                    .as_ref()
                    .context("original sharing source missing")?;
                if generation != self.generation || self.source_request != Some(request) {
                    self.revoke().await?;
                    return Ok(());
                }
                let data = self
                    .io
                    .read(&self.configuration, source, request, &blob, offset, maximum)
                    .await
                    .inspect_err(|_| {
                        tracing::debug!(target: "elixir_marketplace::sharing_diagnostics", stage = "archive_read", "installed sharing unavailable");
                    });
                if self.io.failed {
                    bail!("sharing file owner failed; retirement required");
                }
                // Recheck AFTER the actual blocking I/O joins and BEFORE handing
                // any bytes to the worker. Disable/Off never renews a source.
                let permitted = allowed(marketplace, database, &client).await?
                    && source
                        .current(database)
                        .await
                        .inspect_err(|_| {
                            tracing::debug!(target: "elixir_marketplace::sharing_diagnostics",
                                stage = "source_recheck",
                                "installed sharing unavailable"
                            );
                        })
                        .is_ok();
                let data = if permitted {
                    data.ok().map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                } else {
                    None
                };
                let state = client
                    .sharing_control(Op::SharingChunk {
                        generation,
                        request,
                        offset,
                        data,
                    })
                    .await?;
                accepted(state, request, offset)?;
                self.delay = Duration::from_millis(10);
                if !permitted {
                    tracing::debug!(target: "elixir_marketplace::sharing_diagnostics", stage = "serving_admission", "installed sharing unavailable");
                    self.revoke().await?;
                }
            }
            SharingState::Ready { serving: false, .. } => {
                self.io.retire().await?;
                // Drain stale lookup results without adopting their authority.
                if self.lookup.as_ref().is_some_and(JoinHandle::is_finished) {
                    let result = self.lookup.as_mut().unwrap().await;
                    self.lookup = None;
                    self.lookup_request = None;
                    result.context("original sharing lookup owner failed")??;
                }
            }
            SharingState::Ready { .. } => self.delay = Duration::from_millis(250),
            _ => bail!("sharing poll returned an unrelated receipt"),
        }
        Ok(())
    }
}
fn accepted(state: SharingState, request: Digest, offset: u64) -> Result<()> {
    if !matches!(state,SharingState::Accepted{request:actual,offset:at} if actual==request && at==offset)
    {
        bail!("sharing acknowledgement changed its original request");
    }
    Ok(())
}
async fn allowed(
    marketplace: &MarketplaceService,
    database: &Database,
    client: &ParticipationClient,
) -> Result<bool> {
    if marketplace.stop_requested()
        || !client.sharing_supported()
        || !marketplace.automatic_update_idle()?
    {
        return Ok(false);
    }
    let saved = preferences::load_internal(database).await?;
    Ok(contribution_enabled(saved.preferences.contribution)
        && saved.preferences.metering == preferences::ConnectionMetering::Unmetered
        && saved.preferences.contribution_disclosure_version
            == Some(super::worker::PARTICIPATION_DISCLOSURE_VERSION)
        && client.snapshot().preferences_revision == Some(saved.revision)
        && client.snapshot().host_sample_current)
}
fn contribution_enabled(mode: ContributionMode) -> bool {
    matches!(mode, ContributionMode::Auto | ContributionMode::On)
}
fn renewal_due(remaining: Duration) -> bool {
    remaining < ParticipationClient::SHARING_RENEWAL_LEAD
}
async fn lookup(
    database: Database,
    client: ParticipationClient,
    digest: Digest,
    budget: InstallBudget,
) -> Result<Option<SharingSource>> {
    let targets = hints::lookup(&database, digest).await?;
    if targets.is_empty() {
        tracing::debug!(target: "elixir_marketplace::sharing_diagnostics",
            stage = "no_installed_target",
            "installed sharing unavailable"
        );
    }
    let mut deferred = None;
    for target in targets {
        match lookup_source(&database, &client, &target, budget)
            .await
            .inspect_err(|_| {
                tracing::debug!(target: "elixir_marketplace::sharing_diagnostics", stage = "lookup_source", "installed sharing unavailable");
            })
        {
            Ok(source) => {
                source.descriptor(&target.blob).inspect_err(|_| {
                    tracing::debug!(target: "elixir_marketplace::sharing_diagnostics", stage = "blob_descriptor", "installed sharing unavailable");
                })?;
                return Ok(Some(source));
            }
            Err(error) if ParticipationClient::sharing_verification_deferred(&error) => {
                deferred.get_or_insert(error);
            }
            Err(_) => {}
        }
    }
    // Try independent installed targets, but do not turn a positively unsent
    // verification into an authoritative absence. The existing request and
    // its original peer deadline bound any retry; no new queue is created.
    deferred.map_or(Ok(None), Err)
}
async fn lookup_source(
    database: &Database,
    client: &ParticipationClient,
    target: &hints::Target,
    budget: InstallBudget,
) -> Result<SharingSource> {
    if let Some(consent) =
        publisher::load(database, target.home, &target.extension, target.instance).await?
    {
        let verified = match client
            .verify_sharing_publication(&consent.publication)
            .await
            .inspect_err(|_| {
                tracing::debug!(target: "elixir_marketplace::sharing_diagnostics",
                    stage = "publisher_verification",
                    "installed sharing unavailable"
                );
            }) {
            Err(error) if ParticipationClient::sharing_verification_deferred(&error) => {
                return Err(error);
            }
            result => result,
        };
        if let Ok(publication) = verified {
            // An explicit choice is not five-proof metadata. A stale publisher
            // choice cannot disable independently qualifying ordinary sharing.
            if let Ok(source) = SharingSource::capture_publisher(
                database,
                target.home,
                target.instance,
                consent,
                publication,
                budget,
            )
            .await
            .inspect_err(|_| {
                tracing::debug!(target: "elixir_marketplace::sharing_diagnostics", stage = "publisher_binding", "installed sharing unavailable");
            }) {
                return Ok(source);
            }
        }
    }
    let response = client
        .request(PeerOperation::Search {
            query: SearchQuery {
                text: target.extension.to_string(),
                tags: vec![],
                limit: 1,
            },
            cursor: None,
        })
        .await?;
    let PeerOutcome::Search { page } = response.response else {
        bail!("installed source unavailable in local catalog")
    };
    let candidate = page
        .results
        .into_iter()
        .find(|entry| entry.descriptor.release.payload.extension_id == target.extension)
        .context("installed source is not admitted in the local catalog")?;
    let metadata = client
        .verify_sharing_metadata(&candidate.descriptor)
        .await?;
    SharingSource::capture(database, target.home, target.instance, metadata, budget).await
}
async fn run(
    marketplace: MarketplaceService,
    database: Database,
    state: Arc<AsyncMutex<State>>,
    mut stopped: watch::Receiver<bool>,
) -> Result<()> {
    while !*stopped.borrow() && stopped.has_changed().is_ok() {
        let result = state.lock().await.step(&marketplace, &database).await;
        if result.is_err() {
            tracing::debug!(target: "elixir_marketplace::sharing_diagnostics", stage = "owner_step", "installed sharing unavailable");
            state.lock().await.revoke().await?;
        }
        let delay = state.lock().await.delay;
        tokio::select! {biased;_=stopped.changed()=>{},_=sleep(delay)=>{}}
    }
    state.lock().await.retire().await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renewal_starts_before_the_bounded_check_and_keeps_the_original_expiry() {
        let lead = ParticipationClient::SHARING_RENEWAL_LEAD;
        let healthy_check_and_renewal = Duration::from_secs(72 + 15 + 10 + 20 + 5 + 10 + 5);
        assert!(lead > healthy_check_and_renewal);
        assert!(lead < Duration::from_secs(300));
        assert!(!renewal_due(Duration::from_secs(300)));
        assert!(!renewal_due(lead));
        assert!(renewal_due(lead - Duration::from_millis(1)));
        assert!(renewal_due(Duration::ZERO));
        // With the old 60-s lead, this otherwise bounded healthy cycle could
        // finish after expiry. Renew earlier; do not extend the old receipt.
        assert!(healthy_check_and_renewal > Duration::from_secs(60));
    }
    #[test]
    fn explicit_on_and_auto_use_the_same_contribution_gates() {
        assert!(contribution_enabled(ContributionMode::Auto));
        assert!(contribution_enabled(ContributionMode::On));
        assert!(!contribution_enabled(ContributionMode::Off));
    }
    fn state() -> State {
        State {
            configuration: Configuration {
                archive: PathBuf::from("/unused-sharing-test"),
                retention: ArchiveBudget {
                    bytes: 4 * 1024 * 1024,
                    releases: 4,
                },
                installations: InstallBudget {
                    bytes: 4 * 1024 * 1024,
                    entries: 4,
                },
            },
            source: None,
            generation: Digest::of(Uuid::new_v4().as_bytes()),
            source_request: None,
            client: None,
            cursor: None,
            lookup: None,
            lookup_request: None,
            next_backfill: Instant::now(),
            advertised: vec![],
            advertised_after: None,
            next_hints: Instant::now(),
            next_refresh: Instant::now(),
            delay: Duration::from_secs(1),
            renewed: None,
            io: Io::default(),
        }
    }
    #[tokio::test]
    async fn deferred_lookup_is_not_authoritative_absence_and_never_renews_the_lease() -> Result<()>
    {
        let mut state = state();
        let generation = state.generation;
        let request = Digest::of(b"original-request");
        let renewed = Instant::now();
        state.renewed = Some(renewed);
        state.lookup_request = Some((generation, request));
        let task = tokio::spawn(async { Err(ParticipationClient::fixture_sharing_deferral()) });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        state.lookup = Some(task);
        assert!(!state.finish_lookup(generation, request).await?);
        assert!(state.lookup.is_none());
        assert!(state.lookup_request.is_none());
        assert!(state.source.is_none());
        assert!(state.source_request.is_none());
        assert_eq!(state.generation, generation);
        assert_eq!(state.renewed, Some(renewed));
        Ok(())
    }
    #[tokio::test]
    async fn lookup_absence_requires_the_exact_original_request_and_generation() -> Result<()> {
        for mismatch in 0..3 {
            let mut state = state();
            let generation = state.generation;
            let request = Digest::of(b"original-request");
            state.lookup_request = Some((
                if mismatch == 1 {
                    Digest::of(b"stale-generation")
                } else {
                    generation
                },
                if mismatch == 2 {
                    Digest::of(b"stale-request")
                } else {
                    request
                },
            ));
            let task = tokio::spawn(async { Ok(None) });
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            state.lookup = Some(task);
            assert_eq!(
                state.finish_lookup(generation, request).await?,
                mismatch == 0
            );
            assert!(state.lookup.is_none());
            assert!(state.source_request.is_none());
        }
        Ok(())
    }
    #[tokio::test]
    async fn pending_lookup_and_genuine_failures_are_not_replaced_or_reclassified() -> Result<()> {
        let mut state = state();
        let generation = state.generation;
        let request = Digest::of(b"original-request");
        let (send, receive) = tokio::sync::oneshot::channel();
        state.lookup_request = Some((generation, request));
        state.lookup = Some(tokio::spawn(async { receive.await? }));
        assert!(!state.finish_lookup(generation, request).await?);
        assert!(state.lookup.is_some());
        assert_eq!(state.lookup_request, Some((generation, request)));
        assert!(
            send.send(Err(anyhow::anyhow!(
                "background source preflight is not admitted"
            )))
            .is_ok()
        );
        while !state.lookup.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
        assert!(state.finish_lookup(generation, request).await.is_err());
        assert!(state.lookup.is_none());
        assert!(state.source.is_none());
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn cancelled_retirement_keeps_the_original_lookup_until_it_actually_joins() -> Result<()>
    {
        let mut state = state();
        state.lookup = Some(tokio::spawn(async {
            sleep(Duration::from_secs(10)).await;
            Ok(None)
        }));
        assert!(
            timeout(Duration::from_secs(2), state.retire())
                .await
                .is_err()
        );
        assert!(state.lookup.is_some());
        tokio::time::advance(Duration::from_secs(10)).await;
        state.retire().await?;
        assert!(state.lookup.is_none());
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn shutdown_timeout_never_detaches_or_replaces_its_task() -> Result<()> {
        let service = Service::default();
        let (stop, _) = watch::channel(false);
        let entry = Arc::new(Entry {
            stop,
            state: Arc::new(AsyncMutex::new(state())),
            task: AsyncMutex::new(Owned {
                handle: Some(tokio::spawn(async {
                    sleep(Duration::from_secs(100)).await;
                    Ok(())
                })),
                failed: false,
            }),
        });
        *service.0.lock().unwrap() = Some(entry.clone());
        assert!(service.shutdown().await.is_err());
        assert!(entry.task.lock().await.handle.is_some());
        tokio::time::advance(Duration::from_secs(6)).await;
        service.shutdown().await?;
        assert!(entry.task.lock().await.handle.is_none());
        Ok(())
    }
}
