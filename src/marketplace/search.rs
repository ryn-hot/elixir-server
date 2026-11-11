//! Owner-requested, bounded discovery admission. No provider enrollment,
//! global coverage assertion, unverified peer score, install or public hosting.
use super::worker::{
    EvidenceObservation, MetadataObservation, ParticipationClient, ParticipationPhase,
    PublicSourceObservation, SearchSourceObservation,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use elixir_marketplace::worker_network::PeerKey;
use elixir_marketplace::{
    MAX_CONTROL_FRAME_BYTES, MAX_SEARCH_RESULTS, SCHEMA_VERSION,
    admission::CandidateDescriptor,
    canonical,
    catalog::{SearchPage, SearchQuery},
    evidence::{EvidenceBatch, SupportSummary},
    identity::{Digest, ExtensionId},
    peer::{PeerOperation, PeerOutcome},
    records::MediaTag,
};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use uuid::Uuid;
mod catalog;
mod remote;
mod selection;
pub(in crate::marketplace) use selection::{MAX_CATALOG_PEERS, Selection};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub query: SearchQuery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub prepare_catalog: bool,
}
impl SearchRequest {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let mut request: Self = canonical::decode(bytes, 4096)?;
        request.query = request.query.normalized()?;
        request.validate_preparation()?;
        PeerOperation::Search {
            query: request.query.clone(),
            cursor: request.cursor.clone(),
        }
        .validate()?;
        Ok(request)
    }
    fn validate_preparation(&self) -> Result<()> {
        if self.prepare_catalog
            && (!self.query.text.is_empty()
                || !self.query.tags.is_empty()
                || self.query.limit != 20
                || self.cursor.is_some())
        {
            bail!("catalog preparation requires the initial default browse");
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchPhase {
    Checking,
    Ready,
    Unavailable,
    Stale,
}
#[derive(Serialize)]
pub struct SearchCard {
    pub extension_id: ExtensionId,
    pub release_id: Digest,
    pub name: String,
    pub publisher: String,
    pub version: String,
    pub description: String,
    pub media_tags: Vec<MediaTag>,
    pub support: SupportSummary,
    pub ranking_partial: bool,
    pub metadata_only: bool,
}
#[derive(Serialize)]
pub struct SearchView {
    pub schema_version: u32,
    pub query_id: Uuid,
    pub query: SearchQuery,
    pub phase: SearchPhase,
    pub results: Vec<SearchCard>,
    pub checked: usize,
    pub omitted: usize,
    pub pending: usize,
    pub searching_peers: bool,
    // Presentation only: an ordinary local page is finished while optional
    // discovery may still run. Never setup, global coverage or install authority.
    pub local_page_complete: bool,
    pub next_cursor: Option<String>,
    pub network_complete: bool,
    pub ranking_partial: bool,
    pub metadata_only: bool,
    pub valid_for_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preparation: Option<catalog::Progress>,
}
struct Verified {
    metadata: MetadataObservation,
    source: SearchSourceObservation,
}
impl Verified {
    fn new(metadata: MetadataObservation, source: SearchSourceObservation) -> Result<Self> {
        let release = metadata.ids()?.release;
        if source.release_id() != release {
            bail!("search source fact does not match the checked release");
        }
        Ok(Self { metadata, source })
    }
    fn valid_for(&self) -> Duration {
        // Both facts and their release binding are immutable. Their live
        // expiry and protection checks are still required on every read.
        self.metadata.valid_for().min(self.source.valid_for())
    }
    fn card(&self, support: SupportSummary) -> Result<SearchCard> {
        let release = &self.metadata.descriptor().release.payload;
        Ok(SearchCard {
            extension_id: release.extension_id.clone(),
            release_id: self.source.release_id(),
            name: release.display_name.clone(),
            publisher: release.publisher_name.clone(),
            version: release.version.clone(),
            description: release.description.clone(),
            media_tags: release.media_tags.clone(),
            support,
            ranking_partial: true,
            metadata_only: true,
        })
    }
}
// This private marker is only produced after native metadata, stable-head and
// durable-current checks. A repository outage is not a dishonest catalog peer.
#[derive(Debug, thiserror::Error)]
#[error("marketplace public source is not currently available")]
struct PublicSourceUnavailable;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdmissionFeedback {
    Accepted,
    SourceUnavailable,
    LocalUnavailable,
    Rejected,
}
impl AdmissionFeedback {
    fn from_result(result: &Result<Arc<Verified>>) -> Self {
        match result {
            Ok(_) => Self::Accepted,
            Err(error) if error.is::<PublicSourceUnavailable>() => Self::SourceUnavailable,
            Err(error) if ParticipationClient::queue_busy(error) => Self::LocalUnavailable,
            Err(_) => Self::Rejected,
        }
    }
}
// Private test seam: only an actual participation client creates production
// backends. Neither HTTP nor peers can submit verification booleans/factories.
#[async_trait]
trait Backend: Send + Sync {
    async fn warm_metadata(
        &self,
        _descriptors: &[CandidateDescriptor],
        _interest: &Interest,
    ) -> Result<()> {
        Ok(())
    }
    async fn populate(&self, _interest: &Interest, _deadline: Instant) -> Result<()> {
        Ok(())
    }
    async fn inventory(
        &self,
        _after: Option<ExtensionId>,
        _interest: &Interest,
    ) -> Result<elixir_marketplace::catalog::InventoryPage> {
        bail!("local catalog preparation is unavailable")
    }
    fn preparation_ready(&self) -> Result<bool> {
        Ok(true)
    }
    fn searching_peers(&self) -> bool {
        false
    }
    fn local_page_complete(&self, _query: Uuid) -> bool {
        false
    }
    async fn warm(&self, descriptor: CandidateDescriptor, interest: &Interest) -> Result<()> {
        self.admit(descriptor, interest).await.map(|_| ())
    }
    async fn page(&self, request: &SearchRequest, interest: &Interest) -> Result<SearchPage>;
    // An initially empty page may be retried once after bounded population.
    // Never reuse a consumed continuation or restart a nonempty displayed page.
    async fn reset_browse(&self) -> Result<()> {
        Ok(())
    }
    async fn more(
        &self,
        _request: &SearchRequest,
        _interest: &Interest,
    ) -> Result<Option<SearchPage>> {
        Ok(None)
    }
    async fn admission_result(
        &self,
        _descriptor: Digest,
        _feedback: AdmissionFeedback,
    ) -> Result<()> {
        Ok(())
    }
    fn same_owner(&self, _client: &ParticipationClient) -> bool {
        false
    }
    async fn admit(
        &self,
        descriptor: CandidateDescriptor,
        interest: &Interest,
    ) -> Result<Arc<Verified>>;
    async fn current(&self, value: &Verified) -> Result<()>;
    async fn support(&self, value: &Verified) -> Result<SupportSummary> {
        self.current(value).await?;
        Ok(value.metadata.support()?.clone())
    }
    async fn supports(&self, values: &[Arc<Verified>]) -> Result<Vec<Option<SupportSummary>>> {
        let mut result = Vec::with_capacity(values.len());
        for value in values {
            result.push(self.support(value).await.ok());
        }
        Ok(result)
    }
    async fn evidence(
        &self,
        _value: &Verified,
        _step: &EvidenceStep,
        _interest: &Interest,
    ) -> Result<Option<EvidenceObservation>> {
        Ok(None)
    }
    async fn evidence_sources(
        &self,
        _value: &Verified,
        _interest: &Interest,
    ) -> Result<Vec<EvidenceOrigin>> {
        Ok(vec![EvidenceOrigin::Cache])
    }
    async fn catalog_peers(&self, _value: &Verified) -> Result<Vec<PeerKey>> {
        Ok(vec![])
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EvidenceOrigin {
    Cache,
    Peer(PeerKey),
}
struct EvidenceStep {
    origin: EvidenceOrigin,
    cursor: Option<String>,
    snapshot: Option<Digest>,
    remaining: usize,
}
struct Live {
    client: ParticipationClient,
    session: tokio::sync::Mutex<remote::Session>,
    discovery_progress: remote::DiscoveryProgress,
}
impl Live {
    async fn prefetch_then_verify(
        &self,
        descriptor: &CandidateDescriptor,
        descriptor_id: Digest,
        interest: &Interest,
        local_pending: bool,
    ) -> Result<(MetadataObservation, Option<SearchSourceObservation>)> {
        // Do not spend the once-per-page source attempt before metadata warming
        // succeeds. A resource/protection deferral retries the same candidate.
        let sources =
            remote::prefetch_after_metadata(&self.session, descriptor, |batch| async move {
                self.client.warm_metadata(&batch).await
            })
            .await?;
        #[cfg(test)]
        let trace_prefetch = !sources.is_empty();
        #[cfg(test)]
        if trace_prefetch {
            eprintln!(
                "{}",
                serde_json::json!({"stage":"search_prefetch_group", "source_rows":sources.len()})
            );
            eprintln!(
                "{}",
                serde_json::json!({"stage":"search_metadata_prefetch_finished", "success":true})
            );
        }
        let source_warm = self.warm_sources(&sources, interest, local_pending).await;
        #[cfg(test)]
        if trace_prefetch {
            eprintln!(
                "{}",
                serde_json::json!({"stage":"search_source_prefetch_finished", "success":source_warm.is_ok()})
            );
        }
        source_warm?;
        let warm = self.session.lock().await.take_warm_admission(descriptor_id);
        if let Some(warm) = warm
            && let Some(facts) = self.client.use_warm_search_admission(warm).await?
        {
            return Ok(facts);
        }
        Ok((self.client.verify_metadata(descriptor).await?, None))
    }

    async fn warm_sources(
        &self,
        batch: &[CandidateDescriptor],
        interest: &Interest,
        local_pending: bool,
    ) -> Result<()> {
        let Some(first) = batch.first() else {
            return Ok(());
        };
        let needed = self.client.search_batch_needed(&first.release.payload)?;
        #[cfg(test)]
        eprintln!(
            "{}",
            serde_json::json!({"stage":"search_source_prefetch_needed", "rows":batch.len(), "needed":needed})
        );
        if !needed {
            if local_pending && batch.len() > 1 && interest.active() {
                let admissions = self.client.warm_local_search_admissions(batch).await?;
                if interest.active() {
                    self.session.lock().await.set_warm_admissions(admissions);
                }
            }
            return Ok(());
        }
        let mut checked = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        #[cfg(test)]
        let mut cached = 0usize;
        for descriptor in batch.iter().take(20) {
            if !interest.active() {
                bail!("marketplace search no longer requested");
            }
            if !seen.insert(descriptor.release.payload.extension_id.clone()) {
                continue;
            }
            // Await each SAME durable authority operation, even on replacement.
            if let Ok(Some(metadata)) = self.client.cached_search_metadata(descriptor).await {
                #[cfg(test)]
                {
                    cached += 1;
                }
                let release = metadata.ids()?.release;
                if metadata.descriptor().head.payload.stable_release_id == release
                    && self.client.check_catalog_current(&metadata).await.is_ok()
                {
                    checked.push(metadata);
                }
            }
        }
        #[cfg(test)]
        eprintln!(
            "{}",
            serde_json::json!({"stage":"search_source_prefetch_selected", "rows":batch.len(), "cached_metadata":cached, "current_metadata":checked.len()})
        );
        // This is now a read-only waiter. Dropping it closes the original
        // native lifeline; its owner still joins retirement before a new job.
        remote::while_interested(interest, self.client.warm_search_sources(checked)).await
    }
}
#[async_trait]
impl Backend for Live {
    async fn warm_metadata(&self, descriptors: &[CandidateDescriptor], _: &Interest) -> Result<()> {
        self.client.warm_metadata(descriptors).await
    }
    async fn populate(&self, interest: &Interest, deadline: Instant) -> Result<()> {
        catalog::populate(&self.client, interest, deadline).await
    }
    async fn inventory(
        &self,
        after: Option<ExtensionId>,
        interest: &Interest,
    ) -> Result<elixir_marketplace::catalog::InventoryPage> {
        let response = remote::while_interested(
            interest,
            self.client.request(PeerOperation::Inventory {
                after,
                limit: 20,
                known: vec![],
            }),
        )
        .await?;
        match response.response {
            PeerOutcome::Inventory { page } => Ok(*page),
            _ => bail!("local catalog inventory is unavailable"),
        }
    }
    fn preparation_ready(&self) -> Result<bool> {
        self.client.catalog_preparation_ready()
    }
    fn searching_peers(&self) -> bool {
        self.discovery_progress.waiting()
    }
    fn local_page_complete(&self, query: Uuid) -> bool {
        self.client.snapshot().phase == ParticipationPhase::Running
            && self.discovery_progress.local_page_complete(query)
    }
    async fn page(&self, request: &SearchRequest, interest: &Interest) -> Result<SearchPage> {
        self.session
            .lock()
            .await
            .page(&self.client, request, interest)
            .await
    }
    async fn reset_browse(&self) -> Result<()> {
        *self.session.lock().await =
            remote::Session::with_progress(self.discovery_progress.clone());
        Ok(())
    }
    async fn more(
        &self,
        request: &SearchRequest,
        interest: &Interest,
    ) -> Result<Option<SearchPage>> {
        self.session
            .lock()
            .await
            .more(&self.client, request, interest)
            .await
    }
    async fn admission_result(
        &self,
        descriptor: Digest,
        feedback: AdmissionFeedback,
    ) -> Result<()> {
        self.session
            .lock()
            .await
            .admission_result(descriptor, feedback)
    }
    fn same_owner(&self, client: &ParticipationClient) -> bool {
        self.client.same_owner(client)
    }
    async fn admit(
        &self,
        descriptor: CandidateDescriptor,
        interest: &Interest,
    ) -> Result<Arc<Verified>> {
        let descriptor_id = Digest::of(canonical::to_vec(&descriptor)?);
        let (local_pending, warm) = {
            let mut session = self.session.lock().await;
            (
                session.local_pending(descriptor_id),
                session.take_warm_admission(descriptor_id),
            )
        };
        let metadata = match warm {
            Some(warm) => self.client.use_warm_search_admission(warm).await?,
            None => None,
        };
        let (metadata, cached_source) = match metadata {
            Some(facts) => facts,
            None => {
                self.prefetch_then_verify(&descriptor, descriptor_id, interest, local_pending)
                    .await?
            }
        };
        let release = metadata.ids()?.release;
        if metadata.descriptor().head.payload.stable_release_id != release {
            bail!("default search requires the stable head");
        }
        if local_pending && let Some(source) = cached_source {
            if !interest.active() {
                bail!("marketplace search is no longer requested");
            }
            let value = Verified::new(metadata, source)?;
            if value.valid_for() < Duration::from_secs(2) {
                bail!("marketplace checks need refresh");
            }
            // No cache mutation or grant. The UI still reads the fresh durable
            // and private-head fences before showing this same observation.
            return Ok(Arc::new(value));
        }
        // Only a session-owned local pending hint may defer this availability
        // veto to view()'s mandatory page check. It is NOT a display/selection
        // grant: native/durable/source checks remain, and selection rechecks.
        // Remote/unknown hints must pass the same veto AFTER source-admitted
        // retention: their newly committed head may already fence the OLD
        // cache entry. Reading that predecessor first would block its update.
        let mut failures = selection::SourceFailures::default();
        for location in 0..metadata.descriptor().release.payload.source.locations.len() {
            if !interest.active() {
                bail!("marketplace search is no longer requested");
            }
            if let Some(source) = failures.record(
                remote::while_interested(
                    interest,
                    self.client.preflight_search_metadata(&metadata, location),
                )
                .await,
            ) {
                let value = Arc::new(Verified::new(metadata, source)?);
                if !local_pending {
                    if !interest.active() {
                        bail!("marketplace search is no longer requested");
                    }
                    // First checked remote cards must survive query replacement
                    // even when no optional ranking page arrives. Reuse the
                    // bounded native index; this is not a serving/install grant.
                    // A capacity rejection may leave a valid card uncached;
                    // transport/durable failures retain their fail-closed path.
                    self.client
                        .retain_current_search_metadata(&value.metadata, &value.source)
                        .await?;
                }
                if !interest.active() {
                    bail!("marketplace search is no longer requested");
                }
                if value.valid_for() < Duration::from_secs(2) {
                    bail!("marketplace checks need refresh");
                }
                return Ok(value);
            }
            if self.client.snapshot().phase != ParticipationPhase::Running {
                break;
            }
        }
        Err(failures.unavailable_error(PublicSourceUnavailable))
    }
    async fn current(&self, value: &Verified) -> Result<()> {
        self.client.check_catalog_current(&value.metadata).await
    }
    async fn catalog_peers(&self, value: &Verified) -> Result<Vec<PeerKey>> {
        let origins = self
            .session
            .lock()
            .await
            .evidence_origins(Digest::of(canonical::to_vec(value.metadata.descriptor())?))
            .into_iter()
            .filter_map(|origin| match origin {
                EvidenceOrigin::Peer(peer) => Some(peer),
                EvidenceOrigin::Cache => None,
            })
            .collect::<Vec<_>>();
        // Selection, not every keystroke, reads the existing authenticated
        // roster. Local-first searches need not discover remote peers first.
        let current = timeout(Duration::from_secs(2), self.client.network_status())
            .await
            .ok()
            .and_then(Result::ok)
            .map(|status| status.peers)
            .unwrap_or_default();
        Ok(remote::download_peers(origins, current))
    }
    async fn support(&self, value: &Verified) -> Result<SupportSummary> {
        let support = self.client.ranking_summary(&value.metadata).await?;
        self.current(value).await?;
        Ok(support)
    }
    async fn supports(&self, values: &[Arc<Verified>]) -> Result<Vec<Option<SupportSummary>>> {
        let metadata = values
            .iter()
            .map(|value| &value.metadata)
            .collect::<Vec<_>>();
        self.client.ranking_summaries_current(&metadata).await
    }
    async fn evidence(
        &self,
        value: &Verified,
        step: &EvidenceStep,
        interest: &Interest,
    ) -> Result<Option<EvidenceObservation>> {
        if !self.client.evidence_enabled() {
            return Ok(None);
        }
        let batch = remote::evidence(
            &self.client,
            &step.origin,
            value.source.release_id(),
            step.cursor.clone(),
            step.remaining,
            interest,
        )
        .await?;
        let batch = EvidenceBatch::parse(&canonical::to_vec(&batch)?)?;
        if batch.release_id != value.source.release_id()
            || batch.pairs.len() > step.remaining
            || step.snapshot.is_some_and(|id| id != batch.snapshot_id)
            || (batch.next_cursor.is_some() && batch.next_cursor == step.cursor)
        {
            bail!("ranking page changed or did not progress");
        }
        if batch.pairs.is_empty() {
            return Ok(None);
        }
        let observation = self.client.verify_evidence(&value.metadata, &batch).await?;
        if !matches!(step.origin, EvidenceOrigin::Cache)
            && interest.active()
            && value.valid_for() >= Duration::from_secs(2)
        {
            // Best-effort bounded redistribution, not a prerequisite for an
            // already independently verified partial score. Owned transport
            // and durable-security failures retain their normal fail-closed
            // guards; ordinary cache rejections simply leave less retained data.
            let _ = self
                .client
                .retain_evidence(&value.metadata, &value.source, &batch, &observation)
                .await;
        }
        Ok(Some(observation))
    }
    async fn evidence_sources(
        &self,
        value: &Verified,
        _interest: &Interest,
    ) -> Result<Vec<EvidenceOrigin>> {
        if !self.client.evidence_enabled() {
            return Ok(vec![]);
        }
        Ok(self
            .session
            .lock()
            .await
            .evidence_origins(Digest::of(canonical::to_vec(value.metadata.descriptor())?)))
    }
}
// A single presentation-only wait on the first interactive read of a query.
// Replacing or stopping the query drops its sender, which wakes the old read.
enum FirstView {
    Unclaimed,
    Waiting(oneshot::Sender<()>),
    Finished,
}
impl FirstView {
    fn finish(&mut self) {
        if let Self::Waiting(sender) = std::mem::replace(self, Self::Finished) {
            let _ = sender.send(());
        }
    }
}
const FIRST_VIEW_WAIT: Duration = Duration::from_millis(100);

struct Query {
    id: Uuid,
    request: SearchRequest,
    backend: Arc<dyn Backend>,
    last_active: Instant,
    phase: SearchPhase,
    total: usize,
    checked: usize,
    omitted: usize,
    next_cursor: Option<String>,
    values: Vec<Arc<Verified>>,
    preparation: Option<catalog::Progress>,
    preparation_until: Option<Instant>,
    first_view: FirstView,
}
#[derive(Default)]
struct Slot {
    query: Option<Query>,
    running: bool,
    task: Option<JoinHandle<()>>,
}
#[derive(Default)]
struct Inner {
    slot: Mutex<Slot>,
}
struct Interest {
    inner: Weak<Inner>,
    id: Uuid,
}
impl Interest {
    fn active(&self) -> bool {
        self.inner
            .upgrade()
            .and_then(|inner| {
                inner.slot.lock().ok().map(|slot| {
                    slot.query.as_ref().is_some_and(|query| {
                        query.id == self.id
                            && query.preparation_until.map_or_else(
                                || query.last_active.elapsed() < Duration::from_secs(30),
                                |until| Instant::now() < until,
                            )
                    })
                })
            })
            .unwrap_or(false)
    }
}
pub struct SearchService {
    inner: Arc<Inner>,
    views: Semaphore,
    selections: Arc<Semaphore>,
    retiring: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}
impl Default for SearchService {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            views: Semaphore::new(1),
            selections: Arc::new(Semaphore::new(1)),
            retiring: tokio::sync::Mutex::new(None),
        }
    }
}
impl SearchService {
    /// Startup-owned exact-identity refresh. Do not let a fast, still-current
    /// local card suppress discovery of a newer publisher head indefinitely.
    /// Candidates retain the normal native/source/adoption admission pipeline.
    pub(super) fn start_update(
        &self,
        client: ParticipationClient,
        extension: &ExtensionId,
    ) -> Result<Uuid> {
        if client.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace is not connected");
        }
        let discovery_progress = remote::DiscoveryProgress::default();
        self.start_with(
            Arc::new(Live {
                client,
                session: tokio::sync::Mutex::new(remote::Session::for_update(
                    discovery_progress.clone(),
                )),
                discovery_progress,
            }),
            SearchRequest {
                query: SearchQuery {
                    text: extension.to_string(),
                    tags: vec![],
                    limit: 1,
                },
                cursor: None,
                prepare_catalog: false,
            },
        )
    }
    pub(super) fn start(
        &self,
        client: ParticipationClient,
        mut request: SearchRequest,
    ) -> Result<Uuid> {
        if client.snapshot().phase != ParticipationPhase::Running {
            bail!("marketplace is not connected");
        }
        request.query = request.query.normalized()?;
        let backend = if request.cursor.is_some() {
            let slot = self
                .inner
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
            let previous = slot
                .query
                .as_ref()
                .filter(|q| {
                    matches!(q.phase, SearchPhase::Ready | SearchPhase::Unavailable)
                        && q.next_cursor == request.cursor
                        && q.backend.same_owner(&client)
                })
                .context("search continuation is unavailable for this owner")?;
            if canonical::to_vec(&previous.request.query)? != canonical::to_vec(&request.query)? {
                bail!("search continuation belongs to another query");
            }
            previous.backend.clone()
        } else {
            let discovery_progress = remote::DiscoveryProgress::default();
            Arc::new(Live {
                client,
                session: tokio::sync::Mutex::new(remote::Session::with_progress(
                    discovery_progress.clone(),
                )),
                discovery_progress,
            }) as Arc<dyn Backend>
        };
        self.start_with(backend, request)
    }
    fn start_with(&self, backend: Arc<dyn Backend>, mut request: SearchRequest) -> Result<Uuid> {
        request.query = request.query.normalized()?;
        request.validate_preparation()?;
        PeerOperation::Search {
            query: request.query.clone(),
            cursor: request.cursor.clone(),
        }
        .validate()?;
        let mut slot = self
            .inner
            .slot
            .lock()
            .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
        let id = Uuid::new_v4();
        let preparation = request.prepare_catalog.then(catalog::Progress::default);
        slot.query = Some(Query {
            id,
            request,
            backend,
            last_active: Instant::now(),
            phase: SearchPhase::Checking,
            total: 0,
            checked: 0,
            omitted: 0,
            next_cursor: None,
            values: vec![],
            preparation,
            preparation_until: None,
            first_view: FirstView::Unclaimed,
        });
        if !slot.running {
            // At most one active task and one replaceable latest query. Old
            // durable candidate work retires normally; only read-only waiters
            // cancel on replacement. Never abort an uncertain database commit.
            slot.running = true;
            let inner = self.inner.clone();
            slot.task = Some(tokio::spawn(async move {
                if std::panic::AssertUnwindSafe(run(inner.clone()))
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    // Do not leave a query permanently "checking" or a stuck
                    // running flag after a task unwind. Native job retirement
                    // and uncertain-commit guards retain their own ownership.
                    if let Ok(mut slot) = inner.slot.lock() {
                        slot.running = false;
                        if let Some(query) = slot.query.as_mut() {
                            query.phase = SearchPhase::Unavailable;
                            query.values.clear();
                            query.first_view.finish();
                        }
                    }
                    tracing::error!("marketplace search task failed; no results retained");
                }
            }));
        }
        Ok(id)
    }
    pub(super) fn request_stop(&self) {
        if let Ok(mut slot) = self.inner.slot.lock() {
            slot.query = None;
        }
    }
    pub(super) async fn join(&self) -> Result<()> {
        // Keep the SAME handle in the owner if this borrowed wait times out or
        // is cancelled. A later update cycle must not replace detached work.
        let mut retiring = self.retiring.lock().await;
        loop {
            if retiring.is_none() {
                *retiring = self
                    .inner
                    .slot
                    .lock()
                    .map_err(|_| anyhow::anyhow!("search owner unavailable"))?
                    .task
                    .take();
            }
            let Some(task) = retiring.as_mut() else {
                return Ok(());
            };
            let joined = timeout(Duration::from_secs(80), &mut *task)
                .await
                .context("search retirement deadline; original task retained")?;
            *retiring = None;
            joined?;
        }
    }
    /// Owner HTTP only. If the first local page is nearly done, avoid an
    /// empty round-trip plus a UI polling interval. No view permit is held
    /// during the wait, and the ordinary view still rechecks every card.
    pub(super) async fn interactive_view(&self, id: Uuid) -> Result<SearchView> {
        let receiver = {
            let mut slot = self
                .inner
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
            let query = slot
                .query
                .as_mut()
                .filter(|query| query.id == id)
                .context("search query was replaced or stopped")?;
            if !matches!(query.first_view, FirstView::Unclaimed) {
                None
            } else if query.request.prepare_catalog
                || query.request.cursor.is_some()
                || query.phase != SearchPhase::Checking
                || !query.values.is_empty()
                || query.backend.searching_peers()
            {
                query.first_view.finish();
                None
            } else {
                let (sender, receiver) = oneshot::channel();
                query.first_view = FirstView::Waiting(sender);
                Some(receiver)
            }
        };
        if let Some(receiver) = receiver {
            let _ = timeout(FIRST_VIEW_WAIT, receiver).await;
        }
        self.view(id).await
    }

    pub(super) async fn view(&self, id: Uuid) -> Result<SearchView> {
        #[cfg(test)]
        let (timing, began) = (super::search_timing::view(id), std::time::Instant::now());
        let _permit = self.views.try_acquire().context("search view is busy")?;
        let (
            request,
            backend,
            mut phase,
            mut total,
            mut checked,
            mut omitted,
            mut next_cursor,
            values,
            mut preparation,
        ) = {
            let mut slot = self
                .inner
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
            let query = slot
                .query
                .as_mut()
                .filter(|query| query.id == id)
                .context("search query was replaced or stopped")?;
            query.last_active = Instant::now();
            (
                query.request.clone(),
                query.backend.clone(),
                query.phase,
                query.total,
                query.checked,
                query.omitted,
                query.next_cursor.clone(),
                query.values.clone(),
                query.preparation.clone(),
            )
        };
        let mut current = vec![];
        let mut invalidated = false;
        let mut live = Vec::with_capacity(values.len());
        for value in &values {
            if value.valid_for() < Duration::from_secs(2) {
                invalidated = true;
                continue;
            }
            live.push(value.clone());
        }
        #[cfg(test)]
        let supports_began = std::time::Instant::now();
        let pending_summaries = backend.supports(&live);
        #[cfg(test)]
        let summaries = super::search_timing::scope(timing, pending_summaries).await;
        #[cfg(not(test))]
        let summaries = pending_summaries.await;
        #[cfg(test)]
        let supports_us = super::search_timing::micros(supports_began);
        let summaries = summaries
            .ok()
            .filter(|rows| rows.len() == live.len())
            .unwrap_or_else(|| vec![None; live.len()]);
        for (value, summary) in live.into_iter().zip(summaries) {
            match summary {
                Some(support) => current.push((value, support)),
                None => invalidated = true,
            }
        }
        // A later database check may take long enough for an earlier receipt
        // to expire or lose protection. Construct cards and the remaining TTL
        // only after all awaits, never from a previously sampled lifetime.
        let mut cards = vec![];
        let mut valid_for = Duration::from_secs(300);
        for (value, support) in current {
            let now = u64::try_from(chrono::Utc::now().timestamp())
                .context("ranking clock unavailable")?;
            let remaining = value.valid_for().min(Duration::from_secs(
                support.valid_until.unwrap_or(now).saturating_sub(now),
            ));
            if remaining < Duration::from_secs(2) {
                invalidated = true;
                continue;
            }
            let relevance = request
                .query
                .release_relevance(&value.metadata.descriptor().release.payload)?
                .context("verified search relevance changed")?;
            valid_for = valid_for.min(remaining);
            cards.push((relevance, value.card(support)?));
        }
        cards.sort_by(|(ar, a), (br, b)| {
            ar.cmp(br)
                .then_with(|| b.support.weighted_score.cmp(&a.support.weighted_score))
                .then_with(|| {
                    b.support
                        .verified_supporters
                        .cmp(&a.support.verified_supporters)
                })
                .then_with(|| a.extension_id.cmp(&b.extension_id))
        });
        {
            // Recheck generation after awaits; an older request must never
            // paint a newer query or keep its interest alive.
            let slot = self
                .inner
                .slot
                .lock()
                .map_err(|_| anyhow::anyhow!("search owner unavailable"))?;
            let query = slot
                .query
                .as_ref()
                .filter(|query| query.id == id)
                .context("search query was replaced or stopped")?;
            // The page may finish while its final security checks await IPC.
            // Reuse the latest presentation progress ONLY for the exact same
            // observation objects just checked. A new/replaced candidate still
            // needs another view, even if its ID or signed bytes are equal.
            // No cached verdict, new row, lifetime or install grant is added.
            if values.len() == query.values.len()
                && values
                    .iter()
                    .zip(&query.values)
                    .all(|(checked, latest)| Arc::ptr_eq(checked, latest))
            {
                phase = query.phase;
                total = query.total;
                checked = query.checked;
                omitted = query.omitted;
                next_cursor = query.next_cursor.clone();
                preparation = query.preparation.clone();
            }
        }
        let view = SearchView {
            schema_version: SCHEMA_VERSION,
            query_id: id,
            query: request.query,
            phase: if invalidated && phase != SearchPhase::Checking {
                SearchPhase::Stale
            } else if phase == SearchPhase::Ready && cards.is_empty() && omitted > 0 {
                SearchPhase::Unavailable
            } else {
                phase
            },
            results: cards.into_iter().map(|(_, card)| card).collect(),
            checked,
            omitted,
            pending: total.saturating_sub(checked),
            searching_peers: phase == SearchPhase::Checking && backend.searching_peers(),
            local_page_complete: !request.prepare_catalog
                && (phase == SearchPhase::Ready || backend.local_page_complete(id)),
            next_cursor,
            network_complete: false,
            ranking_partial: true,
            metadata_only: true,
            valid_for_seconds: valid_for.as_secs().saturating_sub(1),
            preparation,
        };
        #[cfg(test)]
        if let Some(context) = timing {
            super::search_timing::emit(super::search_timing::Event::SearchViewTiming {
                context,
                supports_us,
                total_us: super::search_timing::micros(began),
                cards: view.results.len(),
                phase: view.phase,
            });
        }
        Ok(view)
    }
}
async fn run(inner: Arc<Inner>) {
    loop {
        let Some((id, request, backend)) = inner.slot.lock().ok().and_then(|mut slot| {
            let next = slot
                .query
                .as_ref()
                .filter(|query| query.phase == SearchPhase::Checking)
                .map(|query| (query.id, query.request.clone(), query.backend.clone()));
            if next.is_none() {
                slot.running = false;
            }
            next
        }) else {
            return;
        };
        let interest = Interest {
            inner: Arc::downgrade(&inner),
            id,
        };
        let result = process(&inner, &interest, &request, backend.clone())
            .await
            .inspect_err(|error| crate::marketplace::lifecycle::trace_failure("search", error));
        let still_requested = interest.active();
        let mut prepare_until = None;
        if let Ok(mut slot) = inner.slot.lock()
            && let Some(query) = slot.query.as_mut().filter(|query| query.id == id)
        {
            query.phase = if result.is_ok() {
                SearchPhase::Ready
            } else {
                SearchPhase::Unavailable
            };
            query.first_view.finish();
            if result.is_ok() && request.prepare_catalog && still_requested {
                // Explicit setup work may continue behind other application
                // screens. Its fixed deadline is never extended by HTTP polls.
                // The SAME task/query/slots retire on replacement or owner stop.
                let until = Instant::now() + catalog::LIFETIME;
                query.preparation_until = Some(until);
                prepare_until = Some(until);
            }
        }
        if result.is_ok() && still_requested {
            // The independently checked page is already ready. Additional
            // popularity evidence is optional, never another visibility gate.
            // Same task/interest/slots: no detached work or uncertain job abort.
            let _ = enrich(&inner, &interest, backend.as_ref()).await;
        }
        if let Some(until) = prepare_until {
            let prepared = catalog::prepare(&inner, &interest, backend.as_ref(), until).await;
            if let Err(error) = &prepared {
                crate::marketplace::lifecycle::trace_failure("catalog_preparation", error);
                catalog::failed(&inner, &interest);
            }
            // An empty bootstrap browse cannot open setup. If population
            // succeeded, obtain a new independently checked page once, rather
            // than waiting for the owner to guess when to retry. Nonempty
            // pages/cursors remain stable; warming is not display authority.
            let retry = prepared.is_ok()
                && interest.active()
                && inner.slot.lock().ok().is_some_and(|mut slot| {
                    let Some(query) = slot.query.as_mut().filter(|q| q.id == id) else {
                        return false;
                    };
                    if !query.values.is_empty() {
                        return false;
                    }
                    query.phase = SearchPhase::Checking;
                    query.total = 0;
                    query.checked = 0;
                    query.omitted = 0;
                    query.next_cursor = None;
                    true
                });
            if retry {
                let refreshed = match backend.reset_browse().await {
                    Ok(()) => process(&inner, &interest, &request, backend.clone()).await,
                    Err(error) => Err(error),
                }
                .inspect_err(|error| {
                    crate::marketplace::lifecycle::trace_failure("catalog_browse_refresh", error)
                });
                if let Ok(mut slot) = inner.slot.lock()
                    && let Some(query) = slot.query.as_mut().filter(|q| q.id == id)
                {
                    query.phase = if refreshed.is_ok() {
                        SearchPhase::Ready
                    } else {
                        SearchPhase::Unavailable
                    };
                }
                if refreshed.is_ok() && interest.active() {
                    let _ = enrich(&inner, &interest, backend.as_ref()).await;
                }
            }
            if let Ok(mut slot) = inner.slot.lock()
                && let Some(query) = slot.query.as_mut().filter(|q| q.id == id)
            {
                query.preparation_until = None;
            }
        }
    }
}
async fn process(
    inner: &Arc<Inner>,
    interest: &Interest,
    request: &SearchRequest,
    backend: Arc<dyn Backend>,
) -> Result<()> {
    if !interest.active() {
        bail!("search interest expired");
    }
    #[cfg(test)]
    let began = Instant::now();
    let mut page = Some(backend.page(request, interest).await?);
    #[cfg(test)]
    let initial_page_ms = began.elapsed().as_millis();
    #[cfg(test)]
    let (mut admission_time, mut continuation_time, mut first_admitted_ms) =
        (Duration::ZERO, Duration::ZERO, None);
    let mut seen = std::collections::BTreeSet::new();
    let mut attempts = 0;
    let mut reported_candidate_failure = false;
    while let Some(current) = page {
        if current.schema_version != SCHEMA_VERSION
            || !current.metadata_only
            || current.results.len() > MAX_SEARCH_RESULTS - attempts
            || current.results.len() > request.query.limit
            || canonical::to_vec(&current)?.len() > MAX_CONTROL_FRAME_BYTES
        {
            bail!("search page bounds or schema");
        }
        if let Some(cursor) = &current.next_cursor {
            PeerOperation::Search {
                query: request.query.clone(),
                cursor: Some(cursor.clone()),
            }
            .validate()?;
        }
        if let Ok(mut slot) = inner.slot.lock()
            && let Some(query) = slot.query.as_mut().filter(|query| query.id == interest.id)
        {
            query.total += current.results.len();
            query.next_cursor = current.next_cursor;
        }
        let empty = current.results.is_empty();
        for candidate in current.results {
            if !interest.active() {
                bail!("search interest expired");
            }
            let d = candidate.descriptor;
            let relevant = request
                .query
                .release_relevance(&d.release.payload)?
                .is_some();
            let identity = Digest::of(canonical::to_vec(&d)?);
            let unique = seen.insert(identity);
            // Never read candidate.retained_support: it is an untrusted peer total.
            #[cfg(test)]
            let admission_began = Instant::now();
            let result = if relevant && unique {
                admit_requested(backend.as_ref(), &d, interest).await?
            } else {
                Err(anyhow::anyhow!("not a matching unique candidate"))
            };
            #[cfg(test)]
            {
                admission_time += admission_began.elapsed();
                if result.is_ok() && first_admitted_ms.is_none() {
                    first_admitted_ms = Some(began.elapsed().as_millis());
                }
            }
            // One first cause per bounded query, not a per-peer/error history.
            // Default logs contain only the closed owner; detail is private,
            // explicitly enabled and bounded by lifecycle::trace_failure.
            if relevant && unique && !reported_candidate_failure {
                if let Err(error) = &result {
                    crate::marketplace::lifecycle::trace_failure("search_candidate", error);
                    reported_candidate_failure = true;
                }
            }
            // This private feedback comes only after native/durable/source
            // admission (or failure). Raw peer claims can never approve a source.
            backend
                .admission_result(identity, AdmissionFeedback::from_result(&result))
                .await?;
            attempts += 1;
            if let Ok(mut slot) = inner.slot.lock()
                && let Some(query) = slot.query.as_mut().filter(|query| query.id == interest.id)
            {
                query.checked += 1;
                match result {
                    Ok(value) => {
                        let id = &value.metadata.descriptor().release.payload.extension_id;
                        query.values.retain(|old| {
                            &old.metadata.descriptor().release.payload.extension_id != id
                        });
                        query.values.push(value);
                    }
                    Err(_) => query.omitted += 1,
                }
            }
        }
        let admitted = inner
            .slot
            .lock()
            .ok()
            .and_then(|slot| {
                slot.query
                    .as_ref()
                    .filter(|query| query.id == interest.id)
                    .map(|query| query.values.len())
            })
            .unwrap_or(0);
        if empty
            || attempts >= MAX_SEARCH_RESULTS
            || admitted >= request.query.limit
            || !interest.active()
        {
            break;
        }
        #[cfg(test)]
        let continuation_began = Instant::now();
        page = backend.more(request, interest).await?;
        #[cfg(test)]
        {
            continuation_time += continuation_began.elapsed();
        }
    }
    // One bounded aggregate per successful query in explicit test binaries.
    // No query text, identities, key material or production logging changes.
    #[cfg(test)]
    super::search_timing::emit(super::search_timing::Event::SearchQueryTiming {
        query_id: interest.id,
        candidates: attempts,
        initial_page_ms,
        admission_ms: admission_time.as_millis(),
        continuation_ms: continuation_time.as_millis(),
        first_admitted_ms,
        total_ms: began.elapsed().as_millis(),
    });
    Ok(())
}
// Best-effort enrichment of an already admitted page. Errors cannot turn
// trusted cards into unavailable, or weaken their independent current checks.
async fn enrich(inner: &Arc<Inner>, interest: &Interest, backend: &dyn Backend) -> Result<()> {
    let values = inner
        .slot
        .lock()
        .ok()
        .and_then(|slot| {
            slot.query
                .as_ref()
                .filter(|query| query.id == interest.id)
                .map(|query| query.values.clone())
        })
        .unwrap_or_default();
    let mut passes = vec![];
    for value in values {
        if !interest.active() || !backend.preparation_ready()? {
            return Ok(());
        }
        let sources = backend
            .evidence_sources(&value, interest)
            .await
            .unwrap_or_default();
        passes.push(RankingPass::with_sources(value, sources)?);
    }
    // Admission finishes first. Then give each release one bounded batch per
    // round, so a popular release cannot withhold all other checked cards.
    for _ in 0..64 {
        if passes.iter().all(|pass| pass.done) {
            break;
        }
        for pass in &mut passes {
            if !interest.active() || !backend.preparation_ready()? {
                return Ok(());
            }
            if pass.done {
                continue;
            }
            if pass.value.valid_for() < Duration::from_secs(2) {
                pass.done = true;
                continue;
            }
            let Some(step) = pass.step() else {
                continue;
            };
            match backend.evidence(&pass.value, &step, interest).await {
                Ok(Some(observation)) => {
                    // Bad/moving evidence ends this source, not another
                    // independent peer's opportunity to provide proofs.
                    let _ = pass.accept(&observation);
                }
                _ => pass.failed(),
            }
        }
    }
    Ok(())
}
// A transient resource/protection pause is not a negative listing verdict.
// Outer errors stop this query without retiring its source or counting an
// omission; inner errors are ordinary completed candidate rejections.
async fn admit_requested(
    backend: &dyn Backend,
    descriptor: &CandidateDescriptor,
    interest: &Interest,
) -> Result<Result<Arc<Verified>>> {
    loop {
        if !interest.active() {
            bail!("search interest expired");
        }
        // Check owner loss before another attempt. A false resource-readiness
        // value still allows already-current cached facts; an error does not.
        let _ = backend.preparation_ready()?;
        // Await the SAME owned native/durable operation; never abort uncertain
        // security writes when a query is replaced or playback starts.
        let result = backend.admit(descriptor.clone(), interest).await;
        if !interest.active() {
            bail!("search interest expired");
        }
        if result
            .as_ref()
            .err()
            .is_some_and(ParticipationClient::queue_busy)
        {
            // Only this failed request is known NOT to have entered private
            // IPC. Earlier awaited checks completed normally. Keep the SAME
            // bounded candidate/interest; do not condemn its peer or replay an
            // unknown transport/security outcome. Yield instead of busy-polling.
            let _ = backend.preparation_ready()?;
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        if result.is_ok() || backend.preparation_ready()? {
            return Ok(result);
        }
        // No helper, disk or network work while deferred. The existing 30s
        // interest lease bounds an unattended waiter; a visible user's polls
        // may keep the one existing query alive until readiness returns.
        loop {
            if !interest.active() {
                bail!("search interest expired");
            }
            if backend.preparation_ready()? {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

struct RankingPass {
    value: Arc<Verified>,
    sources: Vec<RankingSource>,
    next: usize,
    active: Option<usize>,
    pairs: usize,
    batches: usize,
    done: bool,
}
struct RankingSource {
    origin: EvidenceOrigin,
    cursor: Option<String>,
    snapshot: Option<Digest>,
    seen: std::collections::BTreeSet<String>,
    done: bool,
}
impl RankingPass {
    #[cfg(test)]
    fn new(value: Arc<Verified>) -> Self {
        Self::with_sources(value, vec![EvidenceOrigin::Cache]).expect("one cache origin")
    }
    fn with_sources(value: Arc<Verified>, sources: Vec<EvidenceOrigin>) -> Result<Self> {
        let unique = sources
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if sources.len() > 7
            || unique.len() != sources.len()
            || sources
                .iter()
                .filter(|source| matches!(source, EvidenceOrigin::Peer(_)))
                .count()
                > 6
        {
            bail!("ranking origin limit or duplicate");
        }
        let done = sources.is_empty();
        Ok(Self {
            value,
            sources: sources
                .into_iter()
                .map(|origin| RankingSource {
                    origin,
                    cursor: None,
                    snapshot: None,
                    seen: std::collections::BTreeSet::new(),
                    done: false,
                })
                .collect(),
            next: 0,
            active: None,
            pairs: 0,
            batches: 0,
            done,
        })
    }
    fn step(&mut self) -> Option<EvidenceStep> {
        if self.done || self.active.is_some() {
            return None;
        }
        let count = self.sources.len();
        let selected = (0..count)
            .map(|offset| (self.next + offset) % count)
            .find(|i| !self.sources[*i].done)?;
        // Finish a source before rotating, avoiding duplicate proof walks
        // through replicas. Different releases still share rounds fairly.
        self.next = selected;
        self.active = Some(selected);
        self.batches += 1;
        let source = &self.sources[selected];
        Some(EvidenceStep {
            origin: source.origin.clone(),
            cursor: source.cursor.clone(),
            snapshot: source.snapshot,
            remaining: elixir_marketplace::MAX_SUPPORTERS - self.pairs,
        })
    }
    fn refresh_done(&mut self) {
        self.done = self.batches >= 64
            || self.pairs >= elixir_marketplace::MAX_SUPPORTERS
            || self.sources.iter().all(|s| s.done);
    }
    fn failed(&mut self) {
        if let Some(active) = self.active.take() {
            self.sources[active].done = true;
        }
        self.refresh_done();
    }
    fn accept(&mut self, observation: &EvidenceObservation) -> Result<()> {
        let selected = self
            .active
            .take()
            .context("ranking receipt has no scheduled request")?;
        // Charge every completed native observation, including a subsequently
        // rejected scope/snapshot. Failed read attempts already spend a batch;
        // 64 attempts of at most 64 pairs also bound all raw replies to 4,096.
        let examined = observation.examined();
        if examined > elixir_marketplace::EVIDENCE_BATCH_SIZE
            || self.pairs + examined > elixir_marketplace::MAX_SUPPORTERS
        {
            self.sources[selected].done = true;
            self.refresh_done();
            bail!("ranking evidence work limit");
        }
        self.pairs += examined;
        let result = self.accept_source(selected, observation);
        if result.is_err() {
            self.sources[selected].done = true;
        }
        self.refresh_done();
        result
    }
    fn accept_source(&mut self, selected: usize, observation: &EvidenceObservation) -> Result<()> {
        let source = &mut self.sources[selected];
        if !observation.is_current()
            || observation.extension_id()
                != &self
                    .value
                    .metadata
                    .descriptor()
                    .release
                    .payload
                    .extension_id
            || observation.release_id() != self.value.source.release_id()
            || observation.issuer_policy() != self.value.metadata.issuer_policy()
            || observation.examined() > elixir_marketplace::EVIDENCE_BATCH_SIZE
            || source
                .snapshot
                .is_some_and(|id| id != observation.snapshot_id())
        {
            bail!("ranking pass scope or work budget differs");
        }
        if let Some(cursor) = observation.next_cursor()
            && (Some(cursor) == source.cursor.as_deref() || !source.seen.insert(cursor.to_owned()))
        {
            bail!("ranking cursor repeated");
        }
        source.snapshot = Some(observation.snapshot_id());
        source.cursor = observation.next_cursor().map(str::to_owned);
        source.done = source.cursor.is_none();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
