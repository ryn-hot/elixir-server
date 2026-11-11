//! One private source/verification helper per owned worker, outside its metadata
//! queue. Caller cancellation closes the input lifeline but does NOT drop the
//! retirement task or release its slot before the native helper exits. Unknown
//! retirement disables new jobs and stops independent packet-lease renewal.
//! Purpose selects scheduling only; foreground work keeps the same hard limits.
use std::{
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    connectivity::{ProbeContext, ProbeFamily},
    identity::Digest,
    policy::Metering,
    records::Release,
    security::AuthorizedRelease,
    source_preflight::{MAX_SOURCE_PREFLIGHT_REPLY, SourcePreflightReply, SourcePreflightRequest},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Semaphore, mpsc, oneshot, watch},
    time::{Instant, timeout},
};

use super::{
    ProtectedWorker, ProtectionState, Snapshot, observed_state,
    participation::{PARTICIPATION_DISCLOSURE_VERSION, ParticipationSettings, metrics::Facts},
};
use crate::{
    marketplace::{
        connectivity::ProbePolicy, packet::PacketGuardController, runtime::inspect_fixed_worker,
        storage::FixedWorkerSpec,
    },
    playback::PlaybackJobManager,
    runtime::{PrivateProcessStdio, RuntimeManager, model::ContainerHandle},
};

const RETIRE_DEADLINE: Duration = Duration::from_secs(40);
const HELPER_HEADROOM: u64 = 96 * 1024 * 1024;
// These compact caches live in the server, not inside the worker cgroup.
const HOST_HELPER_HEADROOM: u64 = HELPER_HEADROOM + 12 * 1024 * 1024;
const OBSERVATION_TTL: Duration = Duration::from_secs(300);
// Begin renewing with three minutes left, not sixty seconds: an ordinary
// protected health cycle may itself take 72 s, before a helper/readback and
// the existing 15-s retry cadence. This is scheduling, never a TTL extension.
pub(super) const SHARING_RENEWAL_LEAD: Duration = Duration::from_secs(180);

fn observation_utc() -> Result<u64> {
    u64::try_from(chrono::Utc::now().timestamp()).context("source observation clock unavailable")
}
fn wall_clock_current(observed: u64, now: i64) -> bool {
    u64::try_from(now)
        .ok()
        .and_then(|now| now.checked_sub(observed))
        .is_some_and(|age| age <= OBSERVATION_TTL.as_secs())
}

mod admission;
mod cache;
mod receipt_fifo;
mod search_source;
use admission::Work;
pub(super) fn verification_deferred(error: &anyhow::Error) -> bool {
    admission::verification_deferred(error)
}
#[cfg(test)]
pub(super) fn playback_startup_deferred(error: &anyhow::Error) -> bool {
    admission::playback_startup_deferred(error)
}
#[cfg(test)]
pub(super) fn fixture_sharing_deferral() -> anyhow::Error {
    admission::fixture_deferral()
}
pub use search_source::{DiscoverySourceFact, SearchSourceObservation};
mod evidence;
mod metadata;
mod publication;
mod sdk_http;
mod security;
pub use evidence::EvidenceObservation;
pub use metadata::MetadataObservation;
pub use publication::PublicationObservation;
pub(in crate::marketplace) use sdk_http::{SdkHttpAuthority, SdkHttpCall, SdkHttpClient};
pub use security::SecurityObservation;

#[derive(Clone)]
pub(super) struct Binding {
    namespace: u64,
    addresses: Vec<IpAddr>,
    family: ProbeFamily,
    issuer_keys: Vec<elixir_marketplace::identity::PublicKey>,
    metadata_verifier: bool,
    metadata_batch: bool,
    source_batch: bool,
    source_batch_diagnostics: bool,
    catalog_head_snapshot: bool,
    catalog_head_batch: bool,
    security_verifier: bool,
    evidence_verifier: bool,
    publication_verifier: bool,
    sdk_http: bool,
    sdk_http_stream: bool,
    radicle_source: bool,
}
impl Binding {
    pub(super) fn capture(
        guard: &PacketGuardController,
        profile: &FixedWorkerSpec,
        policy: ProbePolicy,
        features: &std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let spec = profile.container_spec();
        if !guard.channel_usable()
            || spec.network_mode.as_deref()
                != Some(format!("container:{}", guard.handle().id).as_str())
            || spec.labels.get("elixir.marketplace.packet.guard") != Some(&guard.handle().id)
            || spec.labels.get("elixir.marketplace.packet.epoch")
                != Some(&guard.profile().policy().epoch.to_string())
            || profile.limits.memory_bytes != guard.profile().budget().worker_limits().memory_bytes
            || profile.limits.cpu_millicores
                != guard.profile().budget().worker_limits().cpu_millicores
        {
            bail!("source job requires the actual owned protected worker");
        }
        guard.require_credit_observation()?;
        let issuer_keys = spec
            .command
            .chunks_exact(2)
            .filter(|pair| pair[0] == "--issuer-key")
            .map(|pair| pair[1].parse())
            .collect::<elixir_marketplace::Result<Vec<_>>>()?;
        elixir_marketplace::evidence::IssuerPolicy::new(issuer_keys.iter().copied())?;
        let metadata = features
            .get("elixir.marketplace.worker.metadata_verifier")
            .map(String::as_str);
        let sdk_http = features
            .get("elixir.marketplace.worker.sdk_http")
            .map(String::as_str)
            == Some("1");
        Ok(Self {
            namespace: guard.namespace_inode(),
            addresses: guard.profile().policy().tunnel_addresses.clone(),
            family: policy.family,
            issuer_keys,
            metadata_verifier: matches!(metadata, Some("1" | "2" | "3")),
            metadata_batch: features
                .get("elixir.marketplace.worker.metadata_batch")
                .map(String::as_str)
                == Some("1"),
            source_batch: features
                .get("elixir.marketplace.worker.source_batch")
                .map(String::as_str)
                == Some("1"),
            source_batch_diagnostics: features
                .get("elixir.marketplace.worker.source_batch_diagnostics")
                .map(String::as_str)
                == Some("1"),
            catalog_head_snapshot: features
                .get("elixir.marketplace.worker.catalog_head_snapshot")
                .map(String::as_str)
                == Some("1"),
            catalog_head_batch: features
                .get("elixir.marketplace.worker.catalog_head_batch")
                .map(String::as_str)
                == Some("1"),
            security_verifier: matches!(metadata, Some("2" | "3")),
            evidence_verifier: metadata == Some("3"),
            publication_verifier: features
                .get("elixir.marketplace.worker.publication_verifier")
                .map(String::as_str)
                == Some("1"),
            sdk_http,
            radicle_source: features
                .get("elixir.marketplace.worker.radicle_source")
                .map(String::as_str)
                == Some("2"),
            sdk_http_stream: sdk_http
                && features
                    .get("elixir.marketplace.worker.sdk_http_stream")
                    .map(String::as_str)
                    == Some("1"),
        })
    }
    fn context<'a>(&'a self, profile: &FixedWorkerSpec) -> ProbeContext<'a> {
        ProbeContext {
            namespace_inode: self.namespace,
            memory_bytes: profile.limits.memory_bytes,
            cpu_millicores: profile.limits.cpu_millicores,
            pids: crate::marketplace::runtime::WORKER_PIDS,
            tunnel_addresses: &self.addresses,
        }
    }
}

/// Only the private owned process path constructs this, not JSON or a worker
/// assertion. Public-text availability only: current publisher/head/adoption,
/// full Git/package/license equality and runtime/grants remain separate gates.
pub struct PublicSourceObservation {
    release_id: Digest,
    location_id: Digest,
    license_sha256: Digest,
    observed: Instant,
    observed_utc: u64,
    protection: watch::Receiver<Snapshot>,
}
impl PublicSourceObservation {
    pub fn valid_for(&self) -> Duration {
        if !self.current_for_id(self.release_id) {
            return Duration::ZERO;
        }
        let now = chrono::Utc::now().timestamp() as u64;
        OBSERVATION_TTL
            .saturating_sub(self.observed.elapsed())
            .min(Duration::from_secs(
                OBSERVATION_TTL
                    .as_secs()
                    .saturating_sub(now.saturating_sub(self.observed_utc)),
            ))
    }
    // A cache hit preserves the original observation time/protection owner;
    // copying this read-only capability must never refresh its validity.
    pub(in crate::marketplace) fn retained_copy(&self) -> Self {
        Self {
            release_id: self.release_id,
            location_id: self.location_id,
            license_sha256: self.license_sha256,
            observed: self.observed,
            observed_utc: self.observed_utc,
            protection: self.protection.clone(),
        }
    }
    /// Synthetic owner for deterministic unit tests only; never public-fetch
    /// evidence. Production observations have no constructor or deserializer.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_source(
        release_id: Digest,
        license_sha256: Digest,
    ) -> (Self, impl Fn(bool)) {
        let (owner, protection) = watch::channel(Snapshot::timed(
            ProtectionState::Ready,
            Duration::from_secs(600),
        ));
        (
            Self {
                release_id,
                location_id: Digest::of(b"unit-test-only"),
                license_sha256,
                observed: Instant::now(),
                observed_utc: observation_utc().expect("unit-test observation clock"),
                protection,
            },
            move |healthy| {
                owner.send_replace(Snapshot::timed(
                    if healthy {
                        ProtectionState::Ready
                    } else {
                        ProtectionState::Failed
                    },
                    Duration::from_secs(600),
                ));
            },
        )
    }
    pub fn release_id(&self) -> Digest {
        self.release_id
    }
    pub fn location_id(&self) -> Digest {
        self.location_id
    }
    pub fn license_sha256(&self) -> Digest {
        self.license_sha256
    }
    pub fn is_current_for(&self, release: &AuthorizedRelease) -> bool {
        self.current_for_id(release.release_id())
    }
    fn current_for_id(&self, release_id: Digest) -> bool {
        self.release_id == release_id
            && self.observed.elapsed() <= OBSERVATION_TTL
            && wall_clock_current(self.observed_utc, chrono::Utc::now().timestamp())
            && matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            )
    }
    /// Only live private-process observations can supply this half of the
    /// binding. The caller must also hold the actual Git-matched quarantine;
    /// a deserialized summary alone remains untrusted data.
    pub(in crate::marketplace) fn require_matches_source(
        &self,
        descriptor: &elixir_marketplace::source_job::SourceMatchDescriptor,
        summary: &elixir_marketplace::source_job::SourceMatchSummary,
    ) -> Result<()> {
        summary.matches(descriptor)?;
        if !self.current_for_id(descriptor.workspace.release_id)
            || self.license_sha256 != summary.license.sha256
        {
            bail!("public license/source binding is missing, expired or different");
        }
        Ok(())
    }
}

struct Owner {
    runtime: Arc<dyn RuntimeManager>,
    handle: ContainerHandle,
    profile: FixedWorkerSpec,
    binding: Binding,
    protection: watch::Receiver<Snapshot>,
    settings: watch::Receiver<ParticipationSettings>,
    facts: watch::Receiver<Option<Facts>>,
    playback: Arc<PlaybackJobManager>,
    faults: mpsc::Sender<()>,
    failed: AtomicBool,
    slots: Arc<Semaphore>,
    source_cache: Mutex<cache::SourceCache>,
    search_source_cache: Mutex<search_source::SearchSourceCache>,
    metadata_cache: Mutex<metadata::ReceiptCache>,
    ranking_cache: Mutex<evidence::ScoreCache>,
}
#[derive(Clone)]
pub(super) struct Client(Arc<Owner>);
impl Client {
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_preparation_diagnostics(&self) -> serde_json::Value {
        // Closed, local scheduling data only; never raw helper/provider output,
        // network addresses, signed records, credentials or authority.
        let ready = Work::Foreground.admit(&self.0, true);
        let facts = self.0.facts.borrow();
        serde_json::json!({
            "foreground_ready": ready.is_ok(),
            "foreground_blocker": ready.err().map(|error| error.to_string()),
            "available_slots": self.0.slots.available_permits(),
            "owner_failed": self.0.failed.load(Ordering::SeqCst),
            "protection": format!("{:?}", observed_state(&self.0.protection)),
            "background_resource_pressure": facts.as_ref().map(|facts| facts.resource_pressure),
            "foreground_resource_pressure": facts.as_ref().map(|facts| facts.foreground_resource_pressure),
        })
    }
    pub(in crate::marketplace::worker::protected) fn catalog_preparation_ready(
        &self,
    ) -> Result<bool> {
        self.0.cache_admit()?;
        if !self.0.binding.metadata_verifier {
            bail!("native catalog verification is unavailable");
        }
        Ok(Work::Foreground.admit(&self.0, true).is_ok() && self.0.slots.available_permits() != 0)
    }
    pub(super) fn catalog_head_snapshot_supported(&self) -> bool {
        self.0.binding.catalog_head_snapshot
    }
    pub(super) fn catalog_head_batch_supported(&self) -> bool {
        self.0.binding.catalog_head_snapshot && self.0.binding.catalog_head_batch
    }
    #[cfg(test)]
    pub(super) fn fixture_for_security_storage() -> Result<(Self, impl Send)> {
        let fixture = tests::fixture()?;
        Ok((Self(fixture.owner.clone()), fixture))
    }
    #[cfg(test)]
    pub(super) fn fixture_for_catalog_snapshot(batch: bool) -> Result<(Self, impl Send)> {
        let mut fixture = tests::fixture()?;
        Arc::get_mut(&mut fixture.owner)
            .expect("unshared fixture owner")
            .binding
            .catalog_head_snapshot = true;
        Arc::get_mut(&mut fixture.owner)
            .expect("unshared fixture owner")
            .binding
            .catalog_head_batch = batch;
        Ok((Self(fixture.owner.clone()), fixture))
    }
    pub(super) fn failed(&self) -> bool {
        self.0.failed.load(Ordering::SeqCst)
    }
    pub(super) fn fail_security_storage(&self) {
        self.0.failed.store(true, Ordering::SeqCst);
        let _ = self.0.faults.try_send(());
    }
    pub(super) fn from_worker(
        worker: &ProtectedWorker,
        settings: watch::Receiver<ParticipationSettings>,
        facts: watch::Receiver<Option<Facts>>,
        playback: Arc<PlaybackJobManager>,
    ) -> Option<Self> {
        let binding = worker.source_binding.clone()?;
        Some(Self(Arc::new(Owner {
            runtime: worker.runtime.clone(),
            handle: worker.worker.handle.clone(),
            profile: worker.worker.profile.clone(),
            binding,
            protection: worker.supervisor.status.clone(),
            faults: worker.supervisor.faults.clone(),
            settings,
            facts,
            playback,
            failed: AtomicBool::new(false),
            slots: Arc::new(Semaphore::new(1)),
            source_cache: Mutex::new(cache::SourceCache::default()),
            search_source_cache: Mutex::new(search_source::SearchSourceCache::default()),
            metadata_cache: Mutex::new(metadata::ReceiptCache::default()),
            ranking_cache: Mutex::new(evidence::ScoreCache::default()),
        })))
    }
    pub(super) async fn request(
        &self,
        release: &AuthorizedRelease,
        location: usize,
    ) -> Result<PublicSourceObservation> {
        self.request_release(release.payload(), location, Work::Background, true)
            .await
    }
    /// A private-process metadata receipt supplies this release, not a peer's
    /// unsigned payload or a caller-provided "verified" flag. Check it again
    /// after retrieval so expiry/cancellation cannot promote a stale candidate.
    pub(super) async fn request_metadata(
        &self,
        metadata: &MetadataObservation,
        location: usize,
    ) -> Result<PublicSourceObservation> {
        self.request_metadata_mode(metadata, location, true).await
    }
    pub(super) async fn request_fresh_metadata(
        &self,
        metadata: &MetadataObservation,
        location: usize,
    ) -> Result<PublicSourceObservation> {
        self.request_metadata_mode(metadata, location, false).await
    }
    async fn request_metadata_mode(
        &self,
        metadata: &MetadataObservation,
        location: usize,
        use_cache: bool,
    ) -> Result<PublicSourceObservation> {
        let policy = elixir_marketplace::evidence::IssuerPolicy::new(
            self.0.binding.issuer_keys.iter().copied(),
        )?
        .fingerprint();
        if !metadata.is_current() || metadata.issuer_policy() != policy {
            bail!("metadata observation is no longer current");
        }
        let source = self
            .request_release(
                &metadata.descriptor().release.payload,
                location,
                Work::Foreground,
                use_cache,
            )
            .await?;
        if !metadata.is_current() {
            bail!("metadata observation expired during source preflight");
        }
        Ok(source)
    }
    /// Direct-reference source availability only. An authenticated publication
    /// is not paid adoption, package/source equality or installation authority.
    pub(super) async fn request_publication(
        &self,
        publication: &PublicationObservation,
        location: usize,
    ) -> Result<PublicSourceObservation> {
        self.request_publication_mode(publication, location, true)
            .await
    }
    pub(super) async fn request_fresh_publication(
        &self,
        publication: &PublicationObservation,
        location: usize,
    ) -> Result<PublicSourceObservation> {
        self.request_publication_mode(publication, location, false)
            .await
    }
    async fn request_publication_mode(
        &self,
        publication: &PublicationObservation,
        location: usize,
        use_cache: bool,
    ) -> Result<PublicSourceObservation> {
        let expected = publication.summary()?.release_id;
        let source = self
            .request_release(
                &publication.publication().release.payload,
                location,
                Work::Foreground,
                use_cache,
            )
            .await?;
        if publication.summary()?.release_id != expected
            || source.release_id() != expected
            || source.valid_for().is_zero()
        {
            bail!("direct-reference source observation changed or expired");
        }
        Ok(source)
    }
    async fn request_release(
        &self,
        release: &Release,
        location: usize,
        work: Work,
        use_cache: bool,
    ) -> Result<PublicSourceObservation> {
        work.admit_start(&self.0)?;
        let request = SourcePreflightRequest::for_release(
            release,
            location,
            Digest::of(uuid::Uuid::new_v4().as_bytes()),
            self.0.binding.family,
        )?;
        if use_cache
            && let Some(observation) = self
                .0
                .source_cache
                .lock()
                .map_err(|_| anyhow::anyhow!("source receipt cache is unavailable"))?
                .get(request.plan.release_id, request.plan.location_id)
        {
            return Ok(observation);
        }
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("source preflight slot is busy")?;
        let owner = self.0.clone();
        let release = release.clone();
        let (mut completion, response) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = owner
                .run(request, release, location, &mut completion, work)
                .await;
            let _ = completion.send(result);
        });
        response.await.context("source preflight owner stopped")?
    }
}
fn background_admitted(settings: &ParticipationSettings, facts: Option<&Facts>) -> bool {
    settings.accepted_disclosure_version == Some(PARTICIPATION_DISCLOSURE_VERSION)
        && settings.metering == Metering::Unmetered
        && background_resources_admitted(facts)
}
fn background_resources_admitted(facts: Option<&Facts>) -> bool {
    facts.is_some_and(|f| {
        f.observed.elapsed() <= Duration::from_secs(30)
            && f.available_memory
                .is_some_and(|n| n >= HOST_HELPER_HEADROOM)
            && f.disk_headroom
            && !f.resource_pressure
            && !f.playback_pressure
            && f.background_allowance
    })
}
impl Owner {
    fn cache_admit(&self) -> Result<()> {
        if self.failed.load(Ordering::SeqCst)
            || !matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            )
            || self.settings.has_changed().is_err()
            || self.facts.has_changed().is_err()
        {
            bail!("ranking cache owner is unavailable");
        }
        Ok(())
    }
    fn admit(&self, ready: bool) -> Result<()> {
        self.admit_background(ready, false)
    }
    // Restriction verification is local-only: it cannot fetch sources, discover
    // peers or install code. Turning off contribution/automatic updates must
    // not opt an installed source out of already-received security notices.
    fn admit_local_restrictions(&self, ready: bool) -> Result<()> {
        self.admit_background(ready, true)
    }
    fn admit_background(&self, ready: bool, local_restrictions: bool) -> Result<()> {
        let phase = observed_state(&self.protection);
        let blocked = self.failed.load(Ordering::SeqCst)
            || !matches!(phase, ProtectionState::Ready | ProtectionState::Checking)
            || self.settings.has_changed().is_err()
            || self.facts.has_changed().is_err()
            || if local_restrictions {
                !background_resources_admitted(self.facts.borrow().as_ref())
            } else {
                !background_admitted(&self.settings.borrow(), self.facts.borrow().as_ref())
            };
        let checking = ready && phase == ProtectionState::Checking;
        if blocked || checking {
            let settings = self.settings.borrow();
            let facts = self.facts.borrow();
            tracing::debug!(target: "elixir_marketplace::sharing_diagnostics",
                stage = "background_admission",
                local_restrictions,
                protection = ?phase,
                owner_failed = self.failed.load(Ordering::SeqCst),
                settings_live = self.settings.has_changed().is_ok(),
                facts_live = self.facts.has_changed().is_ok(),
                disclosure_accepted = settings.accepted_disclosure_version == Some(PARTICIPATION_DISCLOSURE_VERSION),
                metering = ?settings.metering,
                sample_current = facts.as_ref().is_some_and(|f| f.observed.elapsed() <= Duration::from_secs(30)),
                host_memory_available = facts.as_ref().and_then(|f| f.available_memory),
                resource_pressure = facts.as_ref().map(|f| f.resource_pressure),
                playback_pressure = facts.as_ref().map(|f| f.playback_pressure),
                background_allowance = facts.as_ref().map(|f| f.background_allowance),
                "background marketplace helper unavailable"
            );
            if !blocked && checking {
                return Err(admission::Checking.into());
            }
            bail!("background source preflight is not admitted");
        }
        Ok(())
    }
    async fn headroom(&self) -> Result<()> {
        timeout(Duration::from_secs(8), async {
            inspect_fixed_worker(self.runtime.as_ref(), &self.handle, &self.profile).await?;
            let text = self
                .runtime
                .exec_container_command(
                    &self.handle,
                    "10004:10004",
                    &[
                        "/bin/cat".into(),
                        "/sys/fs/cgroup/memory.current".into(),
                        "/sys/fs/cgroup/memory.stat".into(),
                    ],
                )
                .await?;
            memory_headroom(&text, self.profile.limits.memory_bytes)
        })
        .await
        .context("source preflight admission deadline")?
    }
    async fn run(
        self: &Arc<Self>,
        request: SourcePreflightRequest,
        release: Release,
        location: usize,
        completion: &mut oneshot::Sender<Result<PublicSourceObservation>>,
        work: Work,
    ) -> Result<PublicSourceObservation> {
        if completion.is_closed() {
            bail!("source preflight cancelled before admission");
        }
        self.headroom().await?;
        work.ready_before_launch(self, completion).await?;
        let radicle = matches!(
            request.plan.access,
            elixir_marketplace::source_preflight::SourceAccess::Radicle { .. }
        );
        if radicle && !self.binding.radicle_source {
            bail!("this immutable worker does not support native Radicle source verification");
        }
        if completion.is_closed() {
            bail!("source preflight cancelled before launch");
        }
        // After this point cancellation/unknown launch must stop protection.
        // Only confirmed native exit + stream completion can disarm this guard.
        let mut cleanup = FaultOnDrop {
            owner: self.clone(),
            armed: true,
        };
        let process = timeout(Duration::from_secs(10), async {
            if radicle {
                self.runtime
                    .exec_marketplace_radicle_source(
                        &self.handle,
                        &self.profile.container_spec().labels,
                    )
                    .await
            } else {
                self.runtime
                    .exec_marketplace_source_preflight(
                        &self.handle,
                        &self.profile.container_spec().labels,
                    )
                    .await
            }
        })
        .await
        .context("source preflight startup deadline")
        .and_then(|result| result)
        .inspect_err(|error| {
            crate::marketplace::lifecycle::trace_failure("source_preflight_launch", error)
        })?;
        let retire = exchange(
            process, &request, &release, location, self, completion, work,
        );
        let result = timeout(RETIRE_DEADLINE, retire)
            .await
            .context("source preflight retirement deadline")
            .and_then(|result| result)
            .inspect_err(|error| {
                crate::marketplace::lifecycle::trace_failure("source_preflight_retirement", error)
            })?;
        cleanup.armed = false;
        let report = result?;
        work.admit(self, false)?;
        let mut integrity = FaultOnDrop {
            owner: self.clone(),
            armed: true,
        };
        timeout(
            Duration::from_secs(5),
            inspect_fixed_worker(self.runtime.as_ref(), &self.handle, &self.profile),
        )
        .await
        .context("source preflight ownership readback deadline")
        .and_then(|result| result)
        .inspect_err(|error| {
            crate::marketplace::lifecycle::trace_failure("source_preflight_readback", error)
        })?;
        integrity.armed = false;
        work.admit(self, false)?;
        let observation = PublicSourceObservation {
            release_id: request.plan.release_id,
            location_id: request.plan.location_id,
            license_sha256: report.license_sha256()?,
            observed: Instant::now(),
            observed_utc: observation_utc()?,
            protection: self.protection.clone(),
        };
        self.source_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("source receipt cache is unavailable"))?
            .insert(&observation);
        Ok(observation)
    }
}
fn memory_headroom(text: &str, limit: u64) -> Result<()> {
    if text.len() > 8192 {
        bail!("source preflight memory observation bounds");
    }
    let mut lines = text.lines();
    let integer = |s: &str| -> Result<u64> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            bail!("source preflight memory observation unavailable");
        }
        Ok(s.parse()?)
    };
    let used = integer(lines.next().context("source memory observation missing")?)?;
    let mut fields = std::collections::BTreeMap::new();
    for line in lines {
        let (key, value) = line
            .split_once(' ')
            .context("source memory statistic malformed")?;
        if key.is_empty()
            || !key.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
            || fields.len() >= 128
            || fields.insert(key, integer(value)?).is_some()
        {
            bail!("source memory statistic malformed");
        }
    }
    let field = |name| {
        fields
            .get(name)
            .copied()
            .context("source memory statistic missing")
    };
    // Discount only clean inactive file cache, at most one helper's headroom.
    // Otherwise a prior large SQLite read can permanently prevent preflight.
    let inactive = field("inactive_file")?;
    let dirty = field("file_dirty")?
        .checked_add(field("file_writeback")?)
        .context("source memory statistic overflow")?;
    let reclaimable = inactive.saturating_sub(dirty).min(HELPER_HEADROOM);
    if used == 0
        || used > limit
        || inactive > used
        || used
            .saturating_sub(reclaimable)
            .checked_add(HELPER_HEADROOM)
            .is_none_or(|n| n > limit)
    {
        tracing::debug!(target: "elixir_marketplace::sharing_diagnostics",
            stage = "worker_memory_reservation",
            used_bytes = used,
            reclaimable_bytes = reclaimable,
            reserved_helper_bytes = HELPER_HEADROOM,
            limit_bytes = limit,
            "marketplace helper headroom unavailable"
        );
        bail!("source preflight needs headroom inside its existing worker allocation");
    }
    Ok(())
}
struct FaultOnDrop {
    owner: Arc<Owner>,
    armed: bool,
}
impl Drop for FaultOnDrop {
    fn drop(&mut self) {
        if self.armed {
            tracing::warn!(
                "marketplace native helper cleanup is unconfirmed; retaining protection failure fence"
            );
            self.owner.failed.store(true, Ordering::SeqCst);
            let _ = self.owner.faults.try_send(());
        }
    }
}
struct Child(tokio::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

// Outer error = retirement/protected context unknown, stop the worker. Inner
// error = confirmed-exited unavailable/cancelled source, safe to retry later.
async fn exchange(
    process: PrivateProcessStdio,
    request: &SourcePreflightRequest,
    release: &Release,
    location: usize,
    owner: &Owner,
    completion: &mut oneshot::Sender<Result<PublicSourceObservation>>,
    work: Work,
) -> Result<Result<SourcePreflightReply>> {
    let bytes = request.to_bytes(release, location)?;
    let body = match exchange_bytes_witness(
        process,
        &bytes,
        MAX_SOURCE_PREFLIGHT_REPLY,
        owner,
        completion,
        false,
        work,
        matches!(
            request.plan.access,
            elixir_marketplace::source_preflight::SourceAccess::Radicle { .. }
        ),
    )
    .await?
    {
        Ok(body) => body,
        Err(error) => return Ok(Err(error)),
    };
    let report = SourcePreflightReply::parse_with_policy(
        &body,
        request,
        release,
        location,
        &owner.binding.context(&owner.profile),
    )?;
    // Cleanly retired, authentic but inadmissible publisher content is a
    // listing rejection. Malformed native evidence above remains fail-stop.
    Ok(report.map_err(anyhow::Error::from))
}

/// One slot owns all streams through native retirement, including when the
/// caller disappears. Source jobs keep stdin as a cancellation lifeline;
/// finite native metadata jobs require EOF and have their own hard deadline.
async fn exchange_bytes<T>(
    process: PrivateProcessStdio,
    bytes: &[u8],
    maximum_reply: usize,
    owner: &Owner,
    completion: &mut oneshot::Sender<Result<T>>,
    finite_input: bool,
    work: Work,
) -> Result<Result<Vec<u8>>> {
    exchange_bytes_witness(
        process,
        bytes,
        maximum_reply,
        owner,
        completion,
        finite_input,
        work,
        false,
    )
    .await
}
async fn exchange_bytes_witness<T>(
    process: PrivateProcessStdio,
    bytes: &[u8],
    maximum_reply: usize,
    owner: &Owner,
    completion: &mut oneshot::Sender<Result<T>>,
    finite_input: bool,
    work: Work,
    native_radicle: bool,
) -> Result<Result<Vec<u8>>> {
    let PrivateProcessStdio {
        child,
        mut input,
        mut output,
        diagnostics,
    } = process;
    let mut child = Child(child);
    // A launched helper is still owned if admission changes. Deny input but
    // close/join its original process instead of returning an idle inference.
    let admitted = work.ready(owner, completion).await.is_ok();
    let written = timeout(Duration::from_secs(3), async {
        if !admitted {
            return Err(std::io::Error::other("helper input admission denied"));
        }
        input.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        input.write_all(bytes).await?;
        input.flush().await?;
        if finite_input {
            input.shutdown().await?;
        }
        Ok::<_, std::io::Error>(())
    })
    .await;
    let mut input = Some(input);
    let mut cancelled = !matches!(written, Ok(Ok(())));
    if cancelled || finite_input {
        // Tokio's child-pipe shutdown/flush is not an EOF guarantee on every
        // backend. The finite verifier waits for EOF before processing; close
        // our actual write handle before waiting for its result/retirement.
        drop(input.take());
    }
    let read = async {
        let length = output.read_u32().await? as usize;
        if (length == 0 && !native_radicle) || length > maximum_reply {
            bail!("independent marketplace helper response bounds");
        }
        let mut bytes = vec![0; length];
        output.read_exact(&mut bytes).await?;
        if native_radicle {
            let expected = elixir_marketplace::source_preflight::radicle::NATIVE_EXIT_MARKER;
            let mut marker = vec![0; expected.len()];
            output.read_exact(&mut marker).await?;
            if marker != expected {
                bail!("native Radicle retirement witness mismatch");
            }
        }
        if output.read(&mut [0; 1]).await? != 0 {
            bail!("trailing independent marketplace helper response");
        }
        Ok::<_, anyhow::Error>(bytes)
    };
    let diagnostic = async {
        let mut stream = diagnostics
            .context("independent marketplace helper diagnostics missing")?
            .take(1025);
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await?;
        if bytes.len() > 1024 {
            bail!("independent marketplace helper diagnostic bounds");
        }
        Ok::<_, anyhow::Error>(())
    };
    let retired = async { tokio::join!(read, diagnostic, child.0.wait()) };
    tokio::pin!(retired);
    let mut protection = owner.protection.clone();
    let mut check = tokio::time::interval(Duration::from_secs(1));
    let (body, diagnostic, status) = loop {
        tokio::select! {biased;
            _=completion.closed(),if !cancelled=>{cancelled=true;drop(input.take());},
            _=protection.changed(),if !cancelled=>{if work.admit(owner,false).is_err(){cancelled=true;drop(input.take());}},
            _=check.tick(),if !cancelled=>{if work.admit(owner,false).is_err(){cancelled=true;drop(input.take());}},
            result=&mut retired=>break result,
        }
    };
    let status = status.context("independent marketplace helper retirement unknown")?;
    drop(input);
    if native_radicle {
        // A killed/crashed native owner is not evidence that its children and
        // scratch retired. Missing witness is an OUTER fault, even on cancel.
        diagnostic?;
        let body = body.context("native Radicle cleanup/retirement is unconfirmed")?;
        if !status.success() {
            bail!("native Radicle owner retirement failed");
        }
        if cancelled || body.is_empty() {
            return Ok(Err(anyhow::anyhow!(
                "Radicle source unavailable or cancelled"
            )));
        }
        return Ok(Ok(body));
    }
    if cancelled || !status.success() {
        return Ok(Err(anyhow::anyhow!(
            "independent marketplace helper failed or was cancelled"
        )));
    }
    diagnostic?;
    Ok(Ok(body?))
}

#[cfg(test)]
mod tests;
