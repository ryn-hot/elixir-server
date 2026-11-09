//! Owner-local positive facts, not packages or a per-query/event history.
use std::collections::{BTreeMap, VecDeque};
pub(super) const MAX_ENTRIES: usize = 10_000;
pub(super) const ENTRY_BYTES: usize = 512;
pub(super) const MAX_BYTES: usize = 6 * 1024 * 1024;
pub(super) struct ReceiptFifo<K, V> {
    values: BTreeMap<K, V>,
    order: VecDeque<K>,
}
impl<K, V> Default for ReceiptFifo<K, V> {
    fn default() -> Self {
        Self {
            values: BTreeMap::new(),
            order: VecDeque::new(),
        }
    }
}
impl<K: Ord + Copy, V> ReceiptFifo<K, V> {
    pub(super) fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }
    pub(super) fn insert(&mut self, key: K, value: V) {
        // Values used here have no variable-size owned data; allow conservative
        // B-tree/FIFO allocation overhead in addition to their actual fields.
        assert!(std::mem::size_of::<K>() + std::mem::size_of::<V>() + 256 <= ENTRY_BYTES);
        if let Some(existing) = self.values.get_mut(&key) {
            *existing = value;
            return;
        }
        if self.values.len() == MAX_ENTRIES {
            if let Some(old) = self.order.pop_front() {
                self.values.remove(&old);
            }
        }
        self.values.insert(key, value);
        self.order.push_back(key);
        debug_assert!(self.values.len() * ENTRY_BYTES <= MAX_BYTES);
    }
    pub(super) fn remove(&mut self, key: &K) {
        if self.values.remove(key).is_some() {
            self.order.retain(|old| old != key);
        }
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_facts_have_no_duplicate_queue_or_lifetime_history() {
        let mut cache = ReceiptFifo::default();
        for n in 0..MAX_ENTRIES * 3 {
            cache.insert(n, n);
            assert!(cache.len() <= MAX_ENTRIES);
        }
        for n in MAX_ENTRIES * 2..MAX_ENTRIES * 3 {
            cache.insert(n, n + 1);
        }
        assert_eq!(cache.order.len(), MAX_ENTRIES);
        assert_eq!(cache.len(), MAX_ENTRIES);
        assert!(cache.get(&0).is_none());
        cache.remove(&(MAX_ENTRIES * 2));
        assert_eq!(cache.order.len(), MAX_ENTRIES - 1);
        assert_eq!(MAX_ENTRIES * ENTRY_BYTES, 5_120_000);
        assert!(MAX_ENTRIES * ENTRY_BYTES <= MAX_BYTES);
    }
}
