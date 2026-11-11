//! Search-only fact. Deliberately cannot authorize source matching/install.
use super::receipt_fifo::ReceiptFifo;
use super::*;
mod batch;

// Owner-approved availability cache for discovery only. Metadata/ranking and
// installation facts keep their independent freshness and security checks.
const SEARCH_SOURCE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub struct SearchSourceObservation {
    release_id: Digest,
    location_id: Digest,
    license_sha256: Digest,
    observed: Instant,
    observed_utc: u64,
    protection: watch::Receiver<Snapshot>,
}
impl SearchSourceObservation {
    pub(in crate::marketplace) fn from_source(source: &PublicSourceObservation) -> Result<Self> {
        if source.valid_for().is_zero() {
            bail!("search source fact needs a current native observation");
        }
        Ok(Self {
            release_id: source.release_id,
            location_id: source.location_id,
            license_sha256: source.license_sha256,
            observed: source.observed,
            observed_utc: source.observed_utc,
            protection: source.protection.clone(),
        })
    }
    fn copy(&self) -> Self {
        Self {
            release_id: self.release_id,
            location_id: self.location_id,
            license_sha256: self.license_sha256,
            observed: self.observed,
            observed_utc: self.observed_utc,
            protection: self.protection.clone(),
        }
    }
    pub fn release_id(&self) -> Digest {
        self.release_id
    }
    pub fn valid_for(&self) -> Duration {
        let Some(age) = u64::try_from(chrono::Utc::now().timestamp())
            .ok()
            .and_then(|now| now.checked_sub(self.observed_utc))
        else {
            return Duration::ZERO;
        };
        if !matches!(
            observed_state(&self.protection),
            ProtectionState::Ready | ProtectionState::Checking
        ) {
            return Duration::ZERO;
        }
        SEARCH_SOURCE_TTL
            .saturating_sub(self.observed.elapsed())
            .min(SEARCH_SOURCE_TTL.saturating_sub(Duration::from_secs(age)))
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_install_source(&self) -> PublicSourceObservation {
        // Synthetic fixtures ONLY. No production conversion/deserializer.
        PublicSourceObservation {
            release_id: self.release_id,
            location_id: self.location_id,
            license_sha256: self.license_sha256,
            observed: self.observed,
            observed_utc: self.observed_utc,
            protection: self.protection.clone(),
        }
    }
}
#[derive(Default)]
pub(super) struct SearchSourceCache(ReceiptFifo<Digest, SearchSourceObservation>);
impl SearchSourceCache {
    pub(super) fn get(
        &mut self,
        extension: Digest,
        release: Digest,
    ) -> Option<SearchSourceObservation> {
        let key = extension;
        let value = self.0.get(&key)?;
        if value.release_id != release {
            return None;
        }
        if value.valid_for().is_zero() {
            self.0.remove(&key);
            return None;
        }
        Some(value.copy())
    }
    fn insert(&mut self, extension: Digest, value: &SearchSourceObservation) {
        if !value.valid_for().is_zero() {
            self.0.insert(extension, value.copy());
        }
    }
}
impl Client {
    #[cfg(test)]
    pub(in crate::marketplace::worker::protected) fn fixture_cache_search_source(
        &self,
        metadata: &MetadataObservation,
        source: &PublicSourceObservation,
    ) -> Result<()> {
        let (key, release) = self.search_metadata_plan(metadata, 0)?;
        if source.release_id() != release {
            bail!("search fixture release differs");
        }
        let fact = SearchSourceObservation::from_source(source)?;
        self.0
            .search_source_cache
            .lock()
            .unwrap()
            .insert(key, &fact);
        Ok(())
    }
    pub(in crate::marketplace::worker::protected) fn search_batch_needed(
        &self,
        release: &Release,
    ) -> Result<bool> {
        self.0.cache_admit()?;
        if !self.0.binding.source_batch {
            return Ok(false);
        }
        let key = Digest::of(release.extension_id.as_str().as_bytes());
        let id = Digest::of(elixir_marketplace::canonical::to_vec(release)?);
        Ok(self
            .0
            .search_source_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("search source cache unavailable"))?
            .get(key, id)
            .is_none())
    }
    fn search_metadata_plan(
        &self,
        metadata: &MetadataObservation,
        location: usize,
    ) -> Result<(Digest, Digest)> {
        self.0.cache_admit()?;
        let policy = elixir_marketplace::evidence::IssuerPolicy::new(
            self.0.binding.issuer_keys.iter().copied(),
        )?
        .fingerprint();
        if !metadata.is_current() || metadata.issuer_policy() != policy {
            bail!("search metadata is no longer current");
        }
        let plan = SourcePreflightRequest::for_release(
            &metadata.descriptor().release.payload,
            location,
            Digest::of(b"search-cache-key"),
            self.0.binding.family,
        )?
        .plan;
        let key = Digest::of(
            metadata
                .descriptor()
                .release
                .payload
                .extension_id
                .as_str()
                .as_bytes(),
        );
        Ok((key, plan.release_id))
    }
    fn cached_search_source(
        &self,
        key: Digest,
        release: Digest,
    ) -> Result<Option<SearchSourceObservation>> {
        Ok(self
            .0
            .search_source_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("search source cache unavailable"))?
            .get(key, release))
    }
    /// An exact cache hit only. A miss cannot launch a helper or imply source
    /// authority; the caller still owns the durable currentness read.
    pub(in crate::marketplace::worker::protected) fn cached_search_metadata_source(
        &self,
        metadata: &MetadataObservation,
        location: usize,
    ) -> Result<Option<SearchSourceObservation>> {
        let (key, release) = self.search_metadata_plan(metadata, location)?;
        self.cached_search_source(key, release)
    }
    pub(in crate::marketplace::worker::protected) async fn request_search_metadata(
        &self,
        metadata: &MetadataObservation,
        location: usize,
    ) -> Result<SearchSourceObservation> {
        let (key, release) = self.search_metadata_plan(metadata, location)?;
        if let Some(value) = self.cached_search_source(key, release)? {
            return Ok(value);
        }
        let source = self.request_metadata(metadata, location).await?;
        let fact = SearchSourceObservation::from_source(&source)?;
        self.0
            .search_source_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("search source cache unavailable"))?
            .insert(key, &fact);
        Ok(fact)
    }
}

mod sealed {
    pub trait Sealed {}
}
/// Availability for metadata redistribution only. Not an install/source grant.
pub trait DiscoverySourceFact: sealed::Sealed + Sync {
    fn release_id(&self) -> Digest;
    fn valid_for(&self) -> Duration;
}
impl sealed::Sealed for PublicSourceObservation {}
impl sealed::Sealed for SearchSourceObservation {}
impl DiscoverySourceFact for PublicSourceObservation {
    fn release_id(&self) -> Digest {
        self.release_id()
    }
    fn valid_for(&self) -> Duration {
        self.valid_for()
    }
}
impl DiscoverySourceFact for SearchSourceObservation {
    fn release_id(&self) -> Digest {
        self.release_id()
    }
    fn valid_for(&self) -> Duration {
        self.valid_for()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn source(index: usize, protection: &watch::Receiver<Snapshot>) -> PublicSourceObservation {
        PublicSourceObservation {
            release_id: Digest::of(index.to_be_bytes()),
            location_id: Digest::of(b"public location"),
            license_sha256: Digest::of(b"MIT"),
            observed: Instant::now(),
            observed_utc: observation_utc().unwrap(),
            protection: protection.clone(),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn search_only_facts_preserve_time_scope_and_die_with_their_owner() -> Result<()> {
        let (owner, protection) = watch::channel(Snapshot::timed(
            ProtectionState::Ready,
            SEARCH_SOURCE_TTL * 2,
        ));
        let native = source(1, &protection);
        let fact = SearchSourceObservation::from_source(&native)?;
        let mut cache = SearchSourceCache::default();
        let extension = Digest::of(b"extension");
        cache.insert(extension, &fact);
        assert!(
            cache
                .get(Digest::of(b"other extension"), fact.release_id)
                .is_none()
        );
        assert!(cache.get(extension, Digest::of(b"other release")).is_none());
        tokio::time::advance(Duration::from_secs(301)).await;
        assert!(
            native.valid_for().is_zero(),
            "installation fact must still expire after five minutes"
        );
        assert!(
            SearchSourceObservation::from_source(&native).is_err(),
            "an expired native fact cannot start a new 24-hour window"
        );
        assert!(
            cache.get(extension, fact.release_id).is_some(),
            "search availability may outlive installation authority"
        );
        tokio::time::advance(SEARCH_SOURCE_TTL - Duration::from_secs(302)).await;
        let copied = cache.get(extension, fact.release_id).unwrap();
        assert_eq!(copied.observed, fact.observed);
        assert_eq!(copied.observed_utc, fact.observed_utc);
        assert!(copied.valid_for() <= Duration::from_secs(1));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(cache.get(extension, fact.release_id).is_none());
        assert!(SearchSourceObservation::from_source(&native).is_err());
        let native = source(2, &protection);
        let mut fresh = SearchSourceObservation::from_source(&native)?;
        fresh.observed_utc += 3600;
        assert!(fresh.valid_for().is_zero());
        fresh.observed_utc = native.observed_utc - SEARCH_SOURCE_TTL.as_secs() - 1;
        assert!(fresh.valid_for().is_zero());
        fresh.observed_utc = native.observed_utc;
        cache.insert(extension, &fresh);
        drop(owner);
        assert!(cache.get(extension, fresh.release_id).is_none());
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn ten_thousand_search_facts_retain_no_packages_or_event_history() -> Result<()> {
        let (_owner, protection) = watch::channel(Snapshot::timed(
            ProtectionState::Ready,
            Duration::from_secs(3600),
        ));
        let mut cache = SearchSourceCache::default();
        for n in 0..10_064 {
            let fact = SearchSourceObservation::from_source(&source(n, &protection))?;
            cache.insert(fact.release_id, &fact);
            assert!(cache.0.len() <= 10_000);
        }
        assert_eq!(cache.0.len(), 10_000);
        assert!(
            cache
                .get(
                    source(0, &protection).release_id,
                    source(0, &protection).release_id
                )
                .is_none()
        );
        for n in 64..10_064 {
            let native = source(n, &protection);
            assert!(cache.get(native.release_id, native.release_id).is_some());
        }
        assert_eq!(cache.0.len(), 10_000);
        let old = source(64, &protection);
        let replacement = SearchSourceObservation::from_source(&source(20_000, &protection))?;
        cache.insert(old.release_id, &replacement);
        assert_eq!(cache.0.len(), 10_000);
        assert!(cache.get(old.release_id, old.release_id).is_none());
        assert!(cache.get(old.release_id, replacement.release_id).is_some());
        Ok(())
    }
}
