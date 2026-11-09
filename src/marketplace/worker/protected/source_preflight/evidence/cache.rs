//! Same-owner positive crypto cache. Compact subject sets are distinct from
//! exact-pair signature memoization; neither grants source/runtime authority.
use super::*;
use elixir_marketplace::{MAX_SUPPORTERS, evidence::SupportSummary};
use std::collections::{BTreeMap, VecDeque};

const MAX_ENTRIES: usize = 32;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MEMO_ENTRIES: usize = 8192;
const MEMO_CHARGE: usize = 256;
const ROW_BYTES: usize = MAX_BYTES - MEMO_ENTRIES * MEMO_CHARGE;
const ENTRY_CHARGE: usize = 2048;
const FACT_CHARGE: usize = 64;

#[derive(Clone, PartialEq, Eq)]
struct Key {
    extension: ExtensionId,
    release: Digest,
    policy: Digest,
}
#[derive(Clone, Copy)]
struct CompactFact {
    subject: Digest,
    weight: u8,
    expires_at: u64,
}
struct Entry {
    key: Key,
    subjects: Vec<CompactFact>,
}
impl Entry {
    fn bytes(&self) -> usize {
        ENTRY_CHARGE + self.subjects.capacity() * FACT_CHARGE
    }
    fn prune(&mut self, now: u64) {
        self.subjects.retain(|f| f.expires_at > now);
    }
    fn known(&self, subject: Digest) -> Option<&CompactFact> {
        self.subjects
            .binary_search_by_key(&subject, |f| f.subject)
            .ok()
            .map(|i| &self.subjects[i])
    }
    fn merge(&mut self, values: impl IntoIterator<Item = CompactFact>, now: u64) {
        let updates = values
            .into_iter()
            .filter(|v| {
                v.expires_at > now
                    && (1..=4).contains(&v.weight)
                    && self
                        .known(v.subject)
                        .is_none_or(|old| (old.weight, old.expires_at) < (v.weight, v.expires_at))
            })
            .collect::<Vec<_>>();
        if updates.is_empty() {
            return;
        }
        let needed = self.subjects.len() + updates.len();
        if needed > self.subjects.capacity() {
            self.subjects.reserve_exact(needed - self.subjects.len());
        }
        self.subjects.extend(updates);
        self.subjects.sort_unstable_by(|a, b| {
            a.subject
                .cmp(&b.subject)
                .then_with(|| b.weight.cmp(&a.weight))
                .then_with(|| b.expires_at.cmp(&a.expires_at))
        });
        self.subjects.dedup_by_key(|f| f.subject);
        if self.subjects.len() > MAX_SUPPORTERS {
            self.subjects
                .select_nth_unstable_by(MAX_SUPPORTERS, |a, b| {
                    b.weight
                        .cmp(&a.weight)
                        .then_with(|| a.subject.cmp(&b.subject))
                });
            self.subjects.truncate(MAX_SUPPORTERS);
            self.subjects.sort_unstable_by_key(|f| f.subject);
        }
    }
    #[cfg(test)]
    fn insert(&mut self, _pair: Digest, value: VerifiedWitness, now: u64, _policy: &IssuerPolicy) {
        self.merge(
            [CompactFact {
                subject: value.subject,
                weight: value.weight,
                expires_at: value.expires_at,
            }],
            now,
        );
    }
    fn summary(&self) -> SupportSummary {
        SupportSummary {
            verified_supporters: self.subjects.len(),
            weighted_score: self.subjects.iter().map(|f| u32::from(f.weight)).sum(),
            discovery_qualified: self.subjects.len()
                >= elixir_marketplace::MIN_DISCOVERY_SUPPORTERS,
            capped: self.subjects.len() == MAX_SUPPORTERS,
            is_global_total: false,
            valid_until: self.subjects.iter().map(|f| f.expires_at).min(),
        }
    }
}

#[derive(Default)]
pub(in crate::marketplace::worker::protected::source_preflight) struct ScoreCache {
    entries: VecDeque<Entry>,
    bytes: usize,
    last_utc: Option<u64>,
    memo: BTreeMap<Digest, VerifiedWitness>,
    fifo: VecDeque<Digest>,
    memo_until: Option<u64>,
}
impl ScoreCache {
    fn memo_key(key: &Key, pair: &elixir_marketplace::evidence::EvidencePair) -> Result<Digest> {
        Ok(Digest::of(canonical::to_vec(&(
            key.extension.clone(),
            key.release,
            key.policy,
            Digest::of(canonical::to_vec(pair)?),
        ))?))
    }
    fn remember(&mut self, id: Digest, value: VerifiedWitness) {
        if self.memo.contains_key(&id) {
            return;
        }
        while self.memo.len() >= MEMO_ENTRIES {
            if let Some(old) = self.fifo.pop_front() {
                self.memo.remove(&old);
            } else {
                break;
            }
        }
        self.memo_until = Some(
            self.memo_until
                .map_or(value.expires_at, |old| old.min(value.expires_at)),
        );
        self.memo.insert(id, value);
        self.fifo.push_back(id);
    }
    fn take(
        &mut self,
        metadata: &MetadataObservation,
        policy: &IssuerPolicy,
        now: u64,
    ) -> Result<Entry> {
        if self.last_utc.is_some_and(|last| now < last) {
            bail!("ranking cache clock moved backwards");
        }
        self.last_utc = Some(now);
        if !metadata.is_current() || metadata.issuer_policy() != policy.fingerprint() {
            bail!("ranking cache requires current matching metadata");
        }
        // Avoid a full memo/tree walk for every view or 64-pair page. An old
        // minimum after FIFO eviction is conservative and costs one prune.
        if self.memo_until.is_some_and(|until| until <= now) {
            self.memo.retain(|_, v| v.expires_at > now);
            self.fifo.retain(|id| self.memo.contains_key(id));
            self.memo_until = self.memo.values().map(|v| v.expires_at).min();
        }
        let release = &metadata.descriptor().release.payload;
        let key = Key {
            extension: release.extension_id.clone(),
            release: Digest::of(canonical::to_vec(release)?),
            policy: policy.fingerprint(),
        };
        let mut entry = if let Some(at) = self.entries.iter().position(|e| e.key == key) {
            let entry = self
                .entries
                .remove(at)
                .context("ranking cache index differs")?;
            self.bytes -= entry.bytes();
            entry
        } else {
            Entry {
                key,
                subjects: vec![],
            }
        };
        entry.prune(now);
        let mut gate = vec![];
        // These pairs originate in the independent native metadata receipt.
        for (index, pair) in metadata.descriptor().gate_evidence.iter().enumerate() {
            let c = &pair.credential.payload;
            let witness = VerifiedWitness {
                pair_index: index,
                subject: c.subject,
                weight: c.tenure_weight,
                expires_at: c.expires_at,
                issuer: pair.credential.signer_key,
                credential_id: Digest::of(pair.credential.to_bytes()?),
            };
            self.remember(Self::memo_key(&entry.key, pair)?, witness);
            gate.push(CompactFact {
                subject: c.subject,
                weight: c.tenure_weight,
                expires_at: c.expires_at,
            });
        }
        entry.merge(gate, now);
        Ok(entry)
    }
    fn retain(&mut self, entry: Entry) {
        let charge = entry.bytes();
        while self.entries.len() >= MAX_ENTRIES || self.bytes + charge > ROW_BYTES {
            let Some(old) = self.entries.pop_front() else {
                return;
            };
            self.bytes -= old.bytes();
        }
        self.bytes += charge;
        self.entries.push_back(entry);
    }
    pub(super) fn lookup(
        &mut self,
        metadata: &MetadataObservation,
        batch: &EvidenceBatch,
        policy: &IssuerPolicy,
        now: u64,
    ) -> Result<(Vec<VerifiedWitness>, Vec<usize>)> {
        let entry = self.take(metadata, policy, now)?;
        if batch.release_id != entry.key.release {
            self.retain(entry);
            bail!("ranking cache release differs");
        }
        let mut found = vec![];
        let mut missing = vec![];
        for (index, pair) in batch.pairs.iter().enumerate() {
            let id = Self::memo_key(&entry.key, pair)?;
            if let Some(value) = self.memo.get(&id) {
                let mut value = value.clone();
                value.pair_index = index;
                found.push(value);
            } else if entry
                .known(pair.credential.payload.subject)
                .is_some_and(|known| {
                    (known.weight, known.expires_at)
                        >= (
                            pair.credential.payload.tenure_weight,
                            pair.credential.payload.expires_at,
                        )
                })
            {
                // Untrusted claims may only suppress redundant work, NEVER
                // add a fact or become a verified input. The retained signed
                // subject already supplies at least this lower-bound influence.
            } else {
                missing.push(index);
            }
        }
        self.retain(entry);
        Ok((found, missing))
    }
    pub(super) fn merge(
        &mut self,
        metadata: &MetadataObservation,
        batch: &EvidenceBatch,
        observation: &EvidenceObservation,
        policy: &IssuerPolicy,
        now: u64,
    ) -> Result<()> {
        if !observation.is_current()
            || observation.issuer_policy != policy.fingerprint()
            || observation.batch_id != Digest::of(canonical::to_vec(batch)?)
        {
            bail!("ranking cache observation scope differs");
        }
        let mut entry = self.take(metadata, policy, now)?;
        if observation.extension_id != entry.key.extension
            || observation.release_id != entry.key.release
        {
            self.retain(entry);
            bail!("ranking cache identity differs");
        }
        let mut facts = vec![];
        for value in &observation.verified {
            let pair = batch
                .pairs
                .get(value.pair_index)
                .context("ranking fact index differs")?;
            if value.expires_at > now && policy.allows(&value.issuer) {
                self.remember(Self::memo_key(&entry.key, pair)?, value.clone());
                facts.push(CompactFact {
                    subject: value.subject,
                    weight: value.weight,
                    expires_at: value.expires_at,
                });
            }
        }
        entry.merge(facts, now);
        self.retain(entry);
        Ok(())
    }
    pub(super) fn summary(
        &mut self,
        metadata: &MetadataObservation,
        policy: &IssuerPolicy,
        now: u64,
    ) -> Result<SupportSummary> {
        let entry = self.take(metadata, policy, now)?;
        let summary = entry.summary();
        self.retain(entry);
        Ok(summary)
    }
}
#[cfg(test)]
mod tests;
