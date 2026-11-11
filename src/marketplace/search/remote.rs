//! Bounded, owner-local discovery pagination. Nothing here authenticates a
//! descriptor, a popularity hint or source availability. The caller must admit
//! every emitted candidate through the native/durable/source pipeline.
use super::super::worker::WarmSearchAdmission;
use super::*;
use elixir_marketplace::{
    catalog::SearchResult, worker_control::WorkerControlOutcome, worker_network::PeerKey,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};

const INITIAL_PEERS: usize = 3;
const MAX_PEERS: usize = 6;
const MAX_SOURCE_PAGES: usize = 512;
const MAX_REQUESTS: usize = 7 * MAX_SOURCE_PAGES;
const MAX_EXAMINED: usize = 70_000;
const MAX_SEEN: usize = 10_000;
const PAGE_REQUESTS: usize = 7;
const PAGE_EXAMINED: usize = 140;
const IDLE: Duration = Duration::from_secs(600);
const LIFETIME: Duration = Duration::from_secs(3600);

// Presentation progress only. This must remain readable while the session's
// mutex is held across peer discovery; it grants no completeness or authority.
#[derive(Clone, Default)]
pub(super) struct DiscoveryProgress(Arc<DiscoveryState>);
#[derive(Default)]
struct DiscoveryState {
    waiting: AtomicBool,
    local_page_complete: Mutex<Option<Uuid>>,
}
impl DiscoveryProgress {
    pub(super) fn waiting(&self) -> bool {
        self.0.waiting.load(Ordering::SeqCst)
    }
    pub(super) fn local_page_complete(&self, query: Uuid) -> bool {
        self.0
            .local_page_complete
            .lock()
            .is_ok_and(|current| *current == Some(query))
    }
    fn begin(&self) -> DiscoveryWait {
        self.0.waiting.store(true, Ordering::SeqCst);
        DiscoveryWait(self.0.clone())
    }
}
struct DiscoveryWait(Arc<DiscoveryState>);
impl Drop for DiscoveryWait {
    fn drop(&mut self) {
        self.0.waiting.store(false, Ordering::SeqCst);
    }
}

// Keep the once-only source marker untouched until the SAME awaited native
// metadata warming succeeds. A transient deferral is not a spent source batch.
pub(super) async fn prefetch_after_metadata<F>(
    session: &tokio::sync::Mutex<Session>,
    descriptor: &CandidateDescriptor,
    warm: impl FnOnce(Vec<CandidateDescriptor>) -> F,
) -> Result<Vec<CandidateDescriptor>>
where
    F: std::future::Future<Output = Result<()>>,
{
    let batch = session.lock().await.metadata_batch(descriptor);
    warm(batch).await?;
    session.lock().await.source_batch(descriptor)
}

async fn inactive(interest: &Interest) {
    while interest.active() {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// Only for read-only waiters. Native verification that commits durable
// security observations must finish/retire through its existing owner.
pub(super) async fn while_interested<T>(
    interest: &Interest,
    read: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = inactive(interest) => bail!("marketplace search is no longer requested"),
        result = read => result,
    }
}

pub(super) fn download_peers(
    origins: impl IntoIterator<Item = PeerKey>,
    current: impl IntoIterator<Item = PeerKey>,
) -> Vec<PeerKey> {
    let mut seen = BTreeSet::new();
    origins
        .into_iter()
        .chain(current)
        .filter(|peer| seen.insert(peer.clone()))
        .take(MAX_CATALOG_PEERS)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Location {
    Local,
    Peer(PeerKey),
}
#[async_trait]
trait Transport: Send + Sync {
    async fn peers(&self, interest: &Interest) -> Result<Vec<PeerKey>>;
    async fn fetch(
        &self,
        location: &Location,
        query: &SearchQuery,
        cursor: Option<String>,
        interest: &Interest,
    ) -> Result<SearchPage>;
}
// Private read/status seam. Only the actual participation client supplies this
// in production; no endpoint, peer address, authority or queue is introduced.
#[async_trait]
trait Discovery: Send + Sync {
    async fn status(&self) -> Result<Vec<PeerKey>>;
    async fn refresh(&self) -> Result<()>;
}
#[async_trait]
impl Discovery for ParticipationClient {
    async fn status(&self) -> Result<Vec<PeerKey>> {
        Ok(self.network_status().await?.peers)
    }
    async fn refresh(&self) -> Result<()> {
        self.refresh_network().await.map(|_| ())
    }
}
pub(super) async fn discover(
    client: &ParticipationClient,
    interest: &Interest,
    wanted: Option<&PeerKey>,
) -> Result<Vec<PeerKey>> {
    while_interested(interest, discover_from(client, interest, wanted)).await
}
async fn discover_from(
    client: &impl Discovery,
    interest: &Interest,
    wanted: Option<&PeerKey>,
) -> Result<Vec<PeerKey>> {
    if !interest.active() {
        bail!("marketplace search is no longer requested");
    }
    let mut peers = client.status().await?;
    let present =
        |peers: &Vec<PeerKey>| wanted.map_or(!peers.is_empty(), |key| peers.contains(key));
    if !present(&peers) && interest.active() {
        // This only refreshes the existing bounded protected topic manager.
        // It never dials a caller-provided address or forwards a cursor.
        // Admission to this one refresh has its own fixed queue deadline. A
        // routine protection check may delay dispatch; do not spend the actual
        // discovery window before that refresh has even been accepted. Errors
        // remain errors, not a successful empty marketplace.
        client
            .refresh()
            .await
            .context("marketplace discovery refresh failed")?;
        let until = Instant::now() + ParticipationClient::DISCOVERY_WAIT;
        let mut delay = Duration::from_millis(250);
        while interest.active() && Instant::now() < until {
            // Status polling backs off; the existing worker owns the one
            // bounded lookup/dial. Replacement cancels only this read waiter.
            tokio::time::sleep(delay.min(until.saturating_duration_since(Instant::now()))).await;
            if !interest.active() {
                bail!("marketplace search is no longer requested");
            }
            peers = client.status().await?;
            if present(&peers) {
                break;
            }
            delay = (delay * 2).min(Duration::from_millis(2500));
        }
    }
    Ok(peers)
}
#[async_trait]
impl Transport for ParticipationClient {
    async fn peers(&self, interest: &Interest) -> Result<Vec<PeerKey>> {
        discover(self, interest, None).await
    }
    async fn fetch(
        &self,
        location: &Location,
        query: &SearchQuery,
        cursor: Option<String>,
        interest: &Interest,
    ) -> Result<SearchPage> {
        if let Location::Peer(peer) = location {
            // Native/source checks can outlast the transport's idle timer.
            // Re-admit the SAME Noise identity before using its saved cursor;
            // a different peer may never inherit that continuation.
            if !discover(self, interest, Some(peer)).await?.contains(peer) {
                bail!("search peer did not reconnect");
            }
        }
        let operation = PeerOperation::Search {
            query: query.clone(),
            cursor,
        };
        let response = match location {
            Location::Local => self.request(operation).await?,
            Location::Peer(peer) => match self
                .request_peer(peer.clone(), operation)
                .await?
                .control_response
            {
                WorkerControlOutcome::NetworkReply {
                    peer: actual,
                    response,
                    ..
                } if actual == *peer => *response,
                _ => bail!("peer search is unavailable"),
            },
        };
        match response.response {
            PeerOutcome::Search { page } => Ok(*page),
            _ => bail!("catalog search is unavailable"),
        }
    }
}

// Read-only requests only. A sealed local Full proves this attempt did not
// enter the controller queue; unknown acknowledgements/transport are never
// replayed. One fixed capacity window, original interest/cursor and no extra
// queue slot or public option. Read cancellation leaves actual IPC ownership
// with the original controller, as before.
async fn queue_read<T, F, Fut>(interest: &Interest, mut read: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        if !interest.active() {
            bail!("search interest expired");
        }
        let response = tokio::select! {
            response = read() => response,
            _ = inactive(interest) => bail!("search interest expired"),
        };
        match response {
            Err(error) if ParticipationClient::queue_busy(&error) => {
                let remaining = until.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                tokio::time::sleep(Duration::from_millis(250).min(remaining)).await;
                if Instant::now() >= until {
                    return Err(error);
                }
            }
            other => return other,
        }
    }
}

struct Source {
    location: Location,
    enabled: bool,
    cursor: Option<String>,
    snapshot: Option<Digest>,
    cursors: BTreeSet<String>,
    pages: usize,
    done: bool,
    rejected: bool,
    pending: VecDeque<SearchResult>,
}
impl Source {
    fn new(location: Location, enabled: bool) -> Self {
        Self {
            location,
            enabled,
            cursor: None,
            snapshot: None,
            cursors: BTreeSet::new(),
            pages: 0,
            done: false,
            rejected: false,
            pending: VecDeque::new(),
        }
    }
    fn more(&self) -> bool {
        !self.pending.is_empty() || !self.done
    }
    fn retire(&mut self) {
        self.done = true;
        self.rejected = true;
        self.pending.clear();
    }
}

pub(super) struct Session {
    query: Option<Digest>,
    token: Option<String>,
    born: Instant,
    touched: Instant,
    sources: Vec<Source>,
    discovered: bool,
    next: usize,
    requests: usize,
    examined: usize,
    seen: BTreeSet<Digest>,
    sequence: u64,
    // Bounded displayed-page work, shared by its one-candidate admission
    // chunks. Native feedback is private; peers cannot retire other sources.
    page_requests: usize,
    page_examined: usize,
    emitted: BTreeMap<Digest, usize>,
    admitted: BTreeMap<Digest, usize>,
    source_warmed: BTreeSet<Digest>,
    warm_admissions: BTreeMap<Digest, WarmSearchAdmission>,
    discovery_progress: DiscoveryProgress,
    remote_only: bool,
}
impl Default for Session {
    fn default() -> Self {
        Self {
            query: None,
            token: None,
            born: Instant::now(),
            touched: Instant::now(),
            sources: vec![],
            discovered: false,
            next: 0,
            requests: 0,
            examined: 0,
            seen: BTreeSet::new(),
            sequence: 0,
            page_requests: 0,
            page_examined: 0,
            emitted: BTreeMap::new(),
            admitted: BTreeMap::new(),
            source_warmed: BTreeSet::new(),
            warm_admissions: BTreeMap::new(),
            discovery_progress: DiscoveryProgress::default(),
            remote_only: false,
        }
    }
}
impl Session {
    pub(super) fn with_progress(discovery_progress: DiscoveryProgress) -> Self {
        Self {
            discovery_progress,
            ..Self::default()
        }
    }
    // Automatic checks must ask peers for the installed identity's current
    // stable release. A usable cached card is not evidence of a remote check.
    // This is a private scheduling choice, not a public search/authority flag.
    pub(super) fn for_update(discovery_progress: DiscoveryProgress) -> Self {
        Self {
            remote_only: true,
            ..Self::with_progress(discovery_progress)
        }
    }
    pub(super) fn source_batch(
        &mut self,
        descriptor: &CandidateDescriptor,
    ) -> Result<Vec<CandidateDescriptor>> {
        let id = Digest::of(descriptor.to_bytes()?);
        if self.source_warmed.contains(&id) {
            return Ok(vec![]);
        }
        let batch = self.metadata_batch(descriptor);
        if self.source_warmed.len() + batch.len() > PAGE_EXAMINED {
            return Ok(vec![]);
        }
        for row in &batch {
            self.source_warmed.insert(Digest::of(row.to_bytes()?));
        }
        Ok(batch)
    }
    pub(super) fn metadata_batch(
        &self,
        descriptor: &CandidateDescriptor,
    ) -> Vec<CandidateDescriptor> {
        let mut batch = vec![descriptor.clone()];
        let Some(source) = canonical::to_vec(descriptor)
            .ok()
            .map(Digest::of)
            .and_then(|id| self.emitted.get(&id))
            .and_then(|index| self.sources.get(*index))
            .filter(|source| source.enabled)
        else {
            return batch;
        };
        // At most the remainder of this same source's CURRENT bounded page.
        // No discovery, cursor advancement, candidate count or admission credit.
        batch.extend(
            source
                .pending
                .iter()
                .take(elixir_marketplace::verifier::MAX_METADATA_BATCH - 1)
                .map(|candidate| candidate.descriptor.clone()),
        );
        batch
    }
    /// Owned provenance only, never authentication or display authority. A
    /// remote/unknown/consumed hint must retain its early single-head veto.
    pub(super) fn local_pending(&self, descriptor: Digest) -> bool {
        self.emitted
            .get(&descriptor)
            .and_then(|index| self.sources.get(*index))
            .is_some_and(|source| {
                source.enabled && !source.rejected && matches!(source.location, Location::Local)
            })
    }
    pub(super) fn set_warm_admissions(
        &mut self,
        admissions: BTreeMap<Digest, WarmSearchAdmission>,
    ) {
        self.warm_admissions = admissions;
    }
    pub(super) fn take_warm_admission(
        &mut self,
        descriptor: Digest,
    ) -> Option<WarmSearchAdmission> {
        if !self.local_pending(descriptor) {
            return None;
        }
        self.warm_admissions.remove(&descriptor)
    }
    pub(super) fn evidence_origins(&self, descriptor: Digest) -> Vec<EvidenceOrigin> {
        let mut origins: Vec<_> = self
            .sources
            .iter()
            .filter(|source| source.enabled && !source.rejected)
            .filter_map(|source| match &source.location {
                Location::Peer(peer) => Some(EvidenceOrigin::Peer(peer.clone())),
                Location::Local => None,
            })
            .take(MAX_PEERS)
            .collect();
        origins.push(EvidenceOrigin::Cache);
        if let Some(origin) = self
            .admitted
            .get(&descriptor)
            .and_then(|index| self.sources.get(*index))
        {
            let primary = match &origin.location {
                Location::Local => EvidenceOrigin::Cache,
                Location::Peer(peer) => EvidenceOrigin::Peer(peer.clone()),
            };
            if let Some(index) = origins.iter().position(|candidate| *candidate == primary) {
                let primary = origins.remove(index);
                origins.insert(0, primary);
            }
        }
        origins
    }
    pub(super) async fn page(
        &mut self,
        client: &ParticipationClient,
        request: &SearchRequest,
        interest: &Interest,
    ) -> Result<SearchPage> {
        self.begin_page();
        self.read_chunk(client, request, interest, 1).await
    }
    pub(super) async fn more(
        &mut self,
        client: &ParticipationClient,
        request: &SearchRequest,
        interest: &Interest,
    ) -> Result<Option<SearchPage>> {
        self.more_from(client, request, interest).await
    }
    async fn more_from(
        &mut self,
        transport: &impl Transport,
        request: &SearchRequest,
        interest: &Interest,
    ) -> Result<Option<SearchPage>> {
        let Some(cursor) = self.token.clone() else {
            return Ok(None);
        };
        // `more` fills this displayed page, not a new user-requested page.
        // Once local data is exhausted, a verified nonempty page is usable:
        // do not hold Ready behind discovery just to fill its remaining rows.
        // Keep the unconsumed token; an explicit continuation resets admitted
        // in begin_page and may discover peers normally. Empty/rejected local
        // data still falls through to discovery, never a false-ready gate.
        if !self.discovered && !self.admitted.is_empty() && !self.sources.iter().any(Source::more) {
            return Ok(None);
        }
        if self.page_examined >= PAGE_EXAMINED
            || (self.page_requests >= PAGE_REQUESTS
                && !self
                    .sources
                    .iter()
                    .any(|s| s.enabled && !s.pending.is_empty()))
        {
            return Ok(None);
        }
        let mut next = request.clone();
        next.cursor = Some(cursor);
        Ok(Some(self.read_chunk(transport, &next, interest, 1).await?))
    }
    fn begin_page(&mut self) {
        self.page_requests = 0;
        self.page_examined = 0;
        self.emitted.clear();
        self.admitted.clear();
        self.source_warmed.clear();
        self.warm_admissions.clear();
    }
    pub(super) fn admission_result(
        &mut self,
        descriptor: Digest,
        feedback: AdmissionFeedback,
    ) -> Result<()> {
        self.warm_admissions.remove(&descriptor);
        let origin = self
            .emitted
            .remove(&descriptor)
            .context("unknown search candidate admission")?;
        if feedback == AdmissionFeedback::Accepted {
            if self.admitted.len() >= MAX_SEARCH_RESULTS {
                bail!("search admitted origin limit");
            }
            self.admitted.insert(descriptor, origin);
        } else {
            // A single stale/unavailable local row must not discard unrelated
            // cached listings. An authenticated publisher's source outage is
            // likewise not proof that its catalog peer sent invalid metadata.
            // Only invalid remote metadata retires that query's source (never
            // a persistent ban). Each omitted row still spends the normal
            // page/session work budget and receives no display authority.
            if feedback == AdmissionFeedback::Rejected
                && matches!(self.sources[origin].location, Location::Peer(_))
            {
                self.sources[origin].retire();
            }
            for source in &mut self.sources {
                source.enabled = true;
            }
        }
        Ok(())
    }
    #[cfg(test)]
    async fn read(
        &mut self,
        transport: &impl Transport,
        request: &SearchRequest,
        interest: &Interest,
    ) -> Result<SearchPage> {
        self.begin_page();
        self.read_chunk(transport, request, interest, request.query.limit)
            .await
    }
    async fn read_chunk(
        &mut self,
        transport: &impl Transport,
        request: &SearchRequest,
        interest: &Interest,
        emission_limit: usize,
    ) -> Result<SearchPage> {
        if !interest.active() {
            bail!("search interest expired");
        }
        let query = request.query.normalized()?;
        let key = Digest::of(canonical::to_vec(&query)?);
        if let Some(previous) = self.query {
            if previous != key
                || request.cursor.is_none()
                || request.cursor != self.token
                || self.touched.elapsed() > IDLE
                || self.born.elapsed() > LIFETIME
            {
                bail!("search continuation is stale or belongs to another query");
            }
        } else {
            if request.cursor.is_some() {
                bail!("unknown search continuation");
            }
            self.query = Some(key);
            if !self.remote_only {
                self.sources.push(Source::new(Location::Local, true));
            }
        }
        // Single use. An error cannot replay network work with the same token.
        self.token = None;
        let mut results = vec![];
        let mut output_bytes = 256usize;
        loop {
            if !interest.active() {
                bail!("search interest expired");
            }
            // Local pages are emitted before discovering or sampling peers.
            // A slow/offline onion network must not hold cached cards hostage.
            // Once needed, peer pages retain their own snapshot/cursor/queue.
            // For the first browsable candidate, do not serially wait on the
            // other peers after one returned data. It is still only a hint;
            // admission, currentness and source checks run before display.
            // After the first examined hint (including rejection), preserve
            // the ordinary merge and its fallback scheduling.
            // Automatic update checks keep their existing scheduling.
            let first_hint = emission_limit == 1 && self.page_examined == 0 && !self.remote_only;
            let count = self.sources.len();
            let buffered = first_hint
                && self
                    .sources
                    .iter()
                    .any(|s| s.enabled && !s.pending.is_empty());
            for offset in 0..if buffered { 0 } else { count } {
                // Reuse the emission rotor when recovering from an omitted
                // first hint; an earlier source cannot monopolize the slot.
                let index = if first_hint {
                    (self.next + offset) % count
                } else {
                    offset
                };
                let source = &mut self.sources[index];
                if source.enabled
                    && !source.done
                    && source.pending.is_empty()
                    && self.page_requests < PAGE_REQUESTS
                    && self.requests < MAX_REQUESTS
                {
                    // Only a sealed, known-unsent local Full result may wait.
                    // Preserve this source/cursor and spend one page request
                    // only after a real outcome, not for each capacity poll.
                    let response = queue_read(interest, || {
                        transport.fetch(&source.location, &query, source.cursor.clone(), interest)
                    })
                    .await;
                    if !interest.active() {
                        bail!("search interest expired");
                    }
                    if response
                        .as_ref()
                        .err()
                        .is_some_and(ParticipationClient::queue_busy)
                    {
                        // An expired LOCAL wait is not an invalid peer or a
                        // successful empty page. Retire no source on this path.
                        return response;
                    }
                    self.page_requests += 1;
                    self.requests += 1;
                    source.pages += 1;
                    let Ok(page) = response else {
                        source.retire();
                        continue;
                    };
                    let valid = page.schema_version == SCHEMA_VERSION
                        && page.metadata_only
                        && !page.network_complete
                        && page.results.len() <= query.limit
                        && page.results.len() <= MAX_SEARCH_RESULTS
                        && canonical::to_vec(&page)
                            .is_ok_and(|b| b.len() <= MAX_CONTROL_FRAME_BYTES)
                        && source
                            .snapshot
                            .is_none_or(|snapshot| snapshot == page.snapshot_id)
                        && page.next_cursor.as_ref().is_none_or(|cursor| {
                            !page.results.is_empty()
                                && Some(cursor) != source.cursor.as_ref()
                                && !source.cursors.contains(cursor)
                                && PeerOperation::Search {
                                    query: query.clone(),
                                    cursor: Some(cursor.clone()),
                                }
                                .validate()
                                .is_ok()
                        });
                    if !valid {
                        source.retire();
                        continue;
                    }
                    source.snapshot = Some(page.snapshot_id);
                    if let Some(cursor) = &page.next_cursor {
                        source.cursors.insert(cursor.clone());
                    }
                    source.cursor = page.next_cursor;
                    source.done = source.cursor.is_none() || source.pages >= MAX_SOURCE_PAGES;
                    source.pending = page.results.into();
                    if first_hint && !source.pending.is_empty() {
                        break;
                    }
                }
            }
            while results.len() < emission_limit.min(query.limit)
                && self.page_examined < PAGE_EXAMINED
                && self.examined < MAX_EXAMINED
                && self.seen.len() < MAX_SEEN
            {
                let count = self.sources.len();
                let index = (0..count)
                    .map(|offset| (self.next + offset) % count)
                    .find(|i| self.sources[*i].enabled && !self.sources[*i].pending.is_empty());
                let Some(index) = index else {
                    break;
                };
                self.next = (index + 1) % count;
                let candidate = self.sources[index]
                    .pending
                    .pop_front()
                    .expect("selected pending candidate");
                self.page_examined += 1;
                self.examined += 1;
                let bytes = canonical::to_vec(&candidate)?;
                let identity = Digest::of(canonical::to_vec(&candidate.descriptor)?);
                if self.seen.contains(&identity)
                    || query
                        .release_relevance(&candidate.descriptor.release.payload)?
                        .is_none()
                {
                    continue;
                }
                if output_bytes + bytes.len() + 1 > MAX_CONTROL_FRAME_BYTES {
                    if !results.is_empty() {
                        self.sources[index].pending.push_front(candidate);
                        break;
                    }
                    continue;
                }
                // This is raw-data deduplication only. Invalid first proofs
                // cannot suppress different valid proofs for the same ID.
                self.seen.insert(identity);
                if self.emitted.len() >= MAX_SEARCH_RESULTS {
                    bail!("search admission feedback limit");
                }
                self.emitted.insert(identity, index);
                output_bytes += bytes.len() + 1;
                results.push(candidate);
            }
            if results.len() == emission_limit.min(query.limit)
                || output_bytes >= MAX_CONTROL_FRAME_BYTES - 256
                || self.page_requests >= PAGE_REQUESTS
                || self.requests >= MAX_REQUESTS
                || self.page_examined >= PAGE_EXAMINED
                || self.examined >= MAX_EXAMINED
                || self.seen.len() >= MAX_SEEN
            {
                break;
            }
            if self
                .sources
                .iter()
                .any(|s| s.enabled && !s.pending.is_empty())
            {
                break;
            }
            if self.sources.iter().any(|s| s.enabled && !s.done) {
                continue;
            }
            let mut activated = false;
            for source in &mut self.sources {
                if !source.enabled {
                    source.enabled = true;
                    activated = true;
                }
            }
            if activated {
                continue;
            }
            if self.discovered || !results.is_empty() {
                break;
            }
            // The requested local page has finished, not the peer lookup or
            // global catalog. Publish presentation progress BEFORE waiting for
            // discovery. Source admission still gates every positive card.
            // Errors/invalid local replies, setup and exact update lookups may
            // never substitute this signal for their existing readiness gates.
            if !self.remote_only
                && !request.prepare_catalog
                && self.sources.iter().any(|source| {
                    matches!(source.location, Location::Local)
                        && source.done
                        && !source.rejected
                        && source.pending.is_empty()
                })
            {
                if let Ok(mut current) = self.discovery_progress.0.local_page_complete.lock() {
                    *current = Some(interest.id);
                }
            }
            self.discovered = true;
            let waiting = self.discovery_progress.begin();
            let mut peers = tokio::select! {
                result=transport.peers(interest)=>result.context("marketplace peer discovery failed")?,
                _=inactive(interest)=>bail!("search interest expired"),
            };
            drop(waiting);
            if peers.len() > 8 {
                peers.clear();
            }
            peers.sort();
            peers.dedup();
            // Do not privilege fixed roster order on every new query.
            let nonce = Uuid::new_v4();
            peers.sort_by_key(|peer| {
                Digest::of([nonce.as_bytes().as_slice(), peer.to_string().as_bytes()].concat())
            });
            for (n, peer) in peers.into_iter().take(MAX_PEERS).enumerate() {
                self.sources
                    .push(Source::new(Location::Peer(peer), n < INITIAL_PEERS));
            }
            if !self.sources.iter().any(Source::more) {
                break;
            }
        }
        let more = self.requests < MAX_REQUESTS
            && self.examined < MAX_EXAMINED
            && self.seen.len() < MAX_SEEN
            && (!self.discovered || self.sources.iter().any(Source::more));
        self.token = more.then(|| Uuid::new_v4().simple().to_string());
        self.touched = Instant::now();
        self.sequence += 1;
        let page = SearchPage {
            schema_version: SCHEMA_VERSION,
            snapshot_id: Digest::of(
                [key.as_bytes().as_slice(), &self.sequence.to_be_bytes()].concat(),
            ),
            results,
            next_cursor: self.token.clone(),
            network_complete: false,
            metadata_only: true,
        };
        if canonical::to_vec(&page)?.len() > MAX_CONTROL_FRAME_BYTES {
            bail!("combined search page limit");
        }
        Ok(page)
    }
}

pub(super) async fn evidence(
    client: &ParticipationClient,
    origin: &EvidenceOrigin,
    release: Digest,
    cursor: Option<String>,
    remaining: usize,
    interest: &Interest,
) -> Result<EvidenceBatch> {
    let request = PeerOperation::GetEvidence {
        release_id: release,
        limit: remaining.min(elixir_marketplace::EVIDENCE_BATCH_SIZE),
        cursor,
    };
    let read = async {
        let response = match origin {
            EvidenceOrigin::Cache => client.request(request).await?,
            EvidenceOrigin::Peer(peer) => {
                if !discover(client, interest, Some(peer)).await?.contains(peer) {
                    bail!("ranking peer did not reconnect");
                }
                match client
                    .request_peer(peer.clone(), request)
                    .await?
                    .control_response
                {
                    WorkerControlOutcome::NetworkReply {
                        peer: actual,
                        response,
                        ..
                    } if actual == *peer => *response,
                    _ => bail!("ranking peer unavailable"),
                }
            }
        };
        match response.response {
            PeerOutcome::Evidence { batch } => Ok(batch),
            _ => bail!("ranking evidence unavailable"),
        }
    };
    tokio::select! {response=read=>response,_=inactive(interest)=>bail!("ranking interest expired")}
}

#[cfg(test)]
mod tests;
