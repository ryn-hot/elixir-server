//! Small positive-only availability cache, owned by one actual protected
//! worker. No bytes, peer assertions, negative results or durable authority.
use super::{Digest, PublicSourceObservation};
use std::collections::VecDeque;

pub(super) const MAX_ENTRIES: usize = 256;

#[derive(Default)]
pub(super) struct SourceCache {
    entries: VecDeque<PublicSourceObservation>,
}
impl SourceCache {
    #[cfg(test)]
    pub(super) fn entries_for_test_clear(&mut self) {
        self.entries.clear();
    }
    fn prune(&mut self) {
        self.entries
            .retain(|entry| entry.current_for_id(entry.release_id));
    }
    pub(super) fn get(
        &mut self,
        release_id: Digest,
        location_id: Digest,
    ) -> Option<PublicSourceObservation> {
        self.prune();
        let position = self
            .entries
            .iter()
            .position(|entry| entry.release_id == release_id && entry.location_id == location_id)?;
        let entry = self.entries.remove(position)?;
        let result = entry.retained_copy();
        // Recency changes eviction only, never the original observation TTL.
        self.entries.push_back(entry);
        Some(result)
    }
    pub(super) fn insert(&mut self, observation: &PublicSourceObservation) {
        self.prune();
        if !observation.current_for_id(observation.release_id) {
            return;
        }
        self.entries.retain(|entry| {
            entry.release_id != observation.release_id
                || entry.location_id != observation.location_id
        });
        if self.entries.len() == MAX_ENTRIES {
            self.entries.pop_front();
        }
        self.entries.push_back(observation.retained_copy());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marketplace::worker::protected::{ProtectionState, Snapshot};
    use std::time::Duration;
    use tokio::{sync::watch, time::Instant};

    fn observation(
        index: usize,
        protection: &watch::Receiver<Snapshot>,
    ) -> PublicSourceObservation {
        // Synthetic unit-test ownership only, not evidence of public fetching.
        PublicSourceObservation {
            release_id: Digest::of(index.to_be_bytes()),
            location_id: Digest::of(b"location"),
            license_sha256: Digest::of(b"license"),
            observed: Instant::now(),
            observed_utc: super::super::observation_utc().unwrap(),
            protection: protection.clone(),
        }
    }
    fn owner() -> (watch::Sender<Snapshot>, watch::Receiver<Snapshot>) {
        watch::channel(Snapshot {
            state: ProtectionState::Ready,
            until: Some(Instant::now() + Duration::from_secs(3600)),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn cache_reads_do_not_renew_ttl_or_survive_protection_failure() {
        let (owner, protection) = owner();
        let entry = observation(0, &protection);
        let mut cache = SourceCache::default();
        cache.insert(&entry);
        tokio::time::advance(Duration::from_secs(299)).await;
        let copied = cache.get(entry.release_id, entry.location_id).unwrap();
        assert_eq!(copied.observed, entry.observed);
        assert_eq!(copied.observed_utc, entry.observed_utc);
        assert_eq!(copied.license_sha256, entry.license_sha256);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(cache.get(entry.release_id, entry.location_id).is_none());
        assert!(cache.entries.is_empty());
        assert!(!copied.current_for_id(entry.release_id));
        let entry = observation(1, &protection);
        cache.insert(&entry);
        owner.send_replace(Snapshot {
            state: ProtectionState::Failed,
            until: None,
        });
        assert!(cache.get(entry.release_id, entry.location_id).is_none());
        cache.insert(&entry);
        assert!(cache.entries.is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn cache_is_exactly_release_and_location_scoped_and_plateaus_at_capacity() {
        let (owner, protection) = owner();
        let mut cache = SourceCache::default();
        for index in 0..MAX_ENTRIES * 4 {
            cache.insert(&observation(index, &protection));
            assert!(cache.entries.len() <= MAX_ENTRIES);
        }
        let first = observation(0, &protection);
        assert!(cache.get(first.release_id, first.location_id).is_none());
        let latest = observation(MAX_ENTRIES * 4 - 1, &protection);
        assert!(
            cache
                .get(latest.release_id, Digest::of(b"wrong locator"))
                .is_none()
        );
        assert!(
            cache
                .get(Digest::of(b"wrong release"), latest.location_id)
                .is_none()
        );
        let read = cache.get(latest.release_id, latest.location_id).unwrap();
        cache.insert(&read);
        assert_eq!(cache.entries.len(), MAX_ENTRIES);
        let old_owner_observation = read.retained_copy();
        drop(owner);
        assert!(cache.get(latest.release_id, latest.location_id).is_none());
        assert!(cache.entries.is_empty());
        assert!(!old_owner_observation.current_for_id(latest.release_id));
    }

    #[tokio::test(start_paused = true)]
    async fn wall_clock_expiry_and_rollback_cannot_hide_behind_a_fresh_monotonic_clock() {
        let (_owner, protection) = owner();
        let mut cache = SourceCache::default();
        let mut entry = observation(0, &protection);
        let now = entry.observed_utc;
        entry.observed_utc = now - 301;
        cache.insert(&entry);
        assert!(cache.entries.is_empty());
        entry.observed_utc = now + 3600;
        cache.insert(&entry);
        assert!(cache.entries.is_empty());
        entry.observed_utc = now;
        cache.insert(&entry);
        assert!(cache.get(entry.release_id, entry.location_id).is_some());
        assert!(!super::super::wall_clock_current(now, -1));
        assert!(!super::super::wall_clock_current(now, (now - 1) as i64));
    }
}
