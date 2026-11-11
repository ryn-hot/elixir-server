//! Metadata-only receipts from the fixed native UID-10005 verifier. Never
//! deserialize these capabilities or accept a peer's copy of a process report.
use super::*;
use elixir_marketplace::{
    MAX_CONTROL_FRAME_BYTES, MIN_DISCOVERY_SUPPORTERS, SCHEMA_VERSION,
    admission::CandidateDescriptor,
    canonical,
    evidence::{IssuerPolicy, SupportSummary},
    identity::ExtensionId,
    verifier::VerificationRequest,
};
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

const MAX_REPLY: usize = 4096;
mod batch;
mod input;
use super::receipt_fifo::ReceiptFifo;
#[cfg(test)]
use super::receipt_fifo::{MAX_BYTES as MAX_CACHE_BYTES, MAX_ENTRIES as MAX_CACHE_ENTRIES};
use input::CheckedDescriptor;

#[derive(Default)]
pub(super) struct ReceiptCache {
    entries: ReceiptFifo<Digest, CompactReceipt>,
}
pub(super) struct CompactReceipt {
    descriptor_id: Digest,
    publication_id: Option<Digest>,
    support: SupportSummary,
    issuer_policy: Digest,
    observed: Instant,
    observed_utc: u64,
    protection: watch::Receiver<Snapshot>,
}
impl CompactReceipt {
    /// Only a current, exact private-native descriptor receipt can supply
    /// signature facts for the same three signed records. No TTL renewal.
    pub(super) fn publication_basis(
        &self,
        publication: Digest,
        policy: Digest,
    ) -> Option<(Instant, u64, watch::Receiver<Snapshot>)> {
        let now = chrono::Utc::now().timestamp();
        (self.publication_id == Some(publication)
            && self.issuer_policy == policy
            && self.observed.elapsed() <= OBSERVATION_TTL
            && wall_clock_current(self.observed_utc, now)
            && self
                .support
                .valid_until
                .is_some_and(|until| (now as u64) < until)
            && matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            ))
        .then(|| (self.observed, self.observed_utc, self.protection.clone()))
    }
}
impl ReceiptCache {
    #[cfg(test)]
    pub(super) fn fixture_insert(&mut self, value: &MetadataObservation) -> Result<()> {
        self.insert(value)
    }
    pub(super) fn get_publication(
        &self,
        publication: &elixir_marketplace::publication::PreparedPublication,
        policy: Digest,
    ) -> Result<Option<super::publication::PublicationObservation>> {
        let key = Digest::of(publication.release.payload.extension_id.as_str().as_bytes());
        let Some(entry) = self.entries.get(&key) else {
            return Ok(None);
        };
        super::publication::PublicationObservation::from_exact_metadata(publication, entry, policy)
    }
    fn get(
        &mut self,
        input: &CheckedDescriptor<'_>,
        policy: Digest,
    ) -> Option<MetadataObservation> {
        // The body is supplied again, not retained 10,000 times. An exact hash
        // match to a private-native receipt is required before reconstitution.
        // The input owns the digest of this exact borrowed body, not an
        // independently supplied key that would need a second encoding.
        let descriptor = input.descriptor();
        let key = Digest::of(descriptor.release.payload.extension_id.as_str().as_bytes());
        let entry = self.entries.get(&key)?;
        if entry.issuer_policy != policy || entry.descriptor_id != input.id() {
            return None;
        }
        let value = MetadataObservation {
            descriptor: descriptor.clone(),
            ids: OnceLock::new(),
            support: entry.support.clone(),
            issuer_policy: entry.issuer_policy,
            observed: entry.observed,
            observed_utc: entry.observed_utc,
            protection: entry.protection.clone(),
        };
        if !value.is_current() {
            self.entries.remove(&key);
            return None;
        }
        Some(value)
    }
    fn get_with_validity(
        &mut self,
        input: &CheckedDescriptor<'_>,
        policy: Digest,
        minimum: Duration,
    ) -> Option<MetadataObservation> {
        self.get(input, policy)
            .filter(|value| value.valid_for() >= minimum)
    }
    fn insert(&mut self, value: &MetadataObservation) -> Result<()> {
        if !value.is_current() {
            return Ok(());
        }
        let bytes = value.descriptor.to_bytes()?;
        let id = Digest::of(&bytes);
        let publication = elixir_marketplace::publication::PreparedPublication {
            schema_version: SCHEMA_VERSION,
            publisher: value.descriptor.publisher.clone(),
            release: value.descriptor.release.clone(),
            head: value.descriptor.head.clone(),
        };
        self.entries.insert(
            Digest::of(
                value
                    .descriptor
                    .release
                    .payload
                    .extension_id
                    .as_str()
                    .as_bytes(),
            ),
            CompactReceipt {
                descriptor_id: id,
                // PreparedPublication has no separate historical authority.
                // Do not turn a richer descriptor proof into a weaker-format
                // signature claim: those previews need their own native check.
                publication_id: if value.descriptor.release_authority.is_none() {
                    Some(Digest::of(publication.to_bytes()?))
                } else {
                    None
                },
                support: value.support.clone(),
                issuer_policy: value.issuer_policy,
                observed: value.observed,
                observed_utc: value.observed_utc,
                protection: value.protection.clone(),
            },
        );
        Ok(())
    }
}

/// Owns the exact checked input, not a worker-provided score. This receipt is
/// metadata-only; protected public-source checks and retained publisher/head/
/// revocation state must still pass before a search result becomes visible.
pub struct MetadataObservation {
    descriptor: CandidateDescriptor,
    ids: OnceLock<MetadataIds>,
    support: SupportSummary,
    issuer_policy: Digest,
    observed: Instant,
    observed_utc: u64,
    protection: watch::Receiver<Snapshot>,
}
/// Pure identifiers of this owned immutable body. Never a freshness verdict,
/// proof of availability, or additional entry in the compact receipt cache.
pub(in crate::marketplace) struct MetadataIds {
    pub release: Digest,
    pub publisher_payload: Digest,
    pub head_payload: Digest,
}
impl MetadataObservation {
    #[cfg(test)]
    pub(in crate::marketplace::worker::protected::source_preflight) fn observation_times(
        &self,
    ) -> (Instant, u64) {
        (self.observed, self.observed_utc)
    }
    #[cfg(test)]
    pub(in crate::marketplace) fn retained_copy(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            ids: OnceLock::new(),
            support: self.support.clone(),
            issuer_policy: self.issuer_policy,
            observed: self.observed,
            observed_utc: self.observed_utc,
            protection: self.protection.clone(),
        }
    }
    pub fn valid_for(&self) -> Duration {
        if !self.is_current() {
            return Duration::ZERO;
        }
        let now = chrono::Utc::now().timestamp() as u64;
        let monotonic = OBSERVATION_TTL.saturating_sub(self.observed.elapsed());
        let wall = Duration::from_secs(
            OBSERVATION_TTL
                .as_secs()
                .saturating_sub(now.saturating_sub(self.observed_utc)),
        );
        let expiry =
            Duration::from_secs(self.support.valid_until.unwrap_or(now).saturating_sub(now));
        monotonic.min(wall).min(expiry)
    }
    /// Synthetic test owner with real local cryptography, not OS-process proof.
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_verified_descriptor(
        descriptor: CandidateDescriptor,
        policy: &IssuerPolicy,
    ) -> Result<(Self, impl Fn(bool) + use<>)> {
        let now = observation_utc()?;
        let verified = descriptor.verify_metadata(
            now,
            policy,
            &mut elixir_marketplace::records::SignatureCache::new(128)?,
        )?;
        let support = verified.admitted_support().clone();
        let (owner, protection) = watch::channel(Snapshot {
            state: ProtectionState::Ready,
            until: Some(Instant::now() + Duration::from_secs(3600)),
        });
        Ok((
            Self {
                descriptor,
                ids: OnceLock::new(),
                support,
                issuer_policy: policy.fingerprint(),
                observed: Instant::now(),
                observed_utc: now,
                protection,
            },
            move |active| {
                owner.send_replace(Snapshot {
                    state: if active {
                        ProtectionState::Ready
                    } else {
                        ProtectionState::Failed
                    },
                    until: Some(Instant::now() + Duration::from_secs(3600)),
                });
            },
        ))
    }
    pub fn descriptor(&self) -> &CandidateDescriptor {
        &self.descriptor
    }
    pub(in crate::marketplace) fn ids(&self) -> Result<&MetadataIds> {
        if let Some(ids) = self.ids.get() {
            return Ok(ids);
        }
        let d = self.descriptor();
        let ids = MetadataIds {
            release: Digest::of(canonical::to_vec(&d.release.payload)?),
            publisher_payload: Digest::of(canonical::to_vec(&d.publisher.payload)?),
            head_payload: Digest::of(canonical::to_vec(&d.head.payload)?),
        };
        Ok(self.ids.get_or_init(|| ids))
    }
    pub fn issuer_policy(&self) -> Digest {
        self.issuer_policy
    }
    pub fn is_current(&self) -> bool {
        let now = chrono::Utc::now().timestamp();
        self.observed.elapsed() <= OBSERVATION_TTL
            && wall_clock_current(self.observed_utc, now)
            && self
                .support
                .valid_until
                .is_some_and(|until| (now as u64) < until)
            && matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            )
    }
    /// Exactly five independently verified gate supporters, not fuller ranking
    /// evidence, a verified download count, or a global total.
    pub fn support(&self) -> Result<&SupportSummary> {
        if !self.is_current() {
            bail!("metadata observation is no longer current");
        }
        Ok(&self.support)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    schema_version: u32,
    request_id: u64,
    outcome: Outcome,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Outcome {
    MetadataBatch {
        results: Vec<batch::Row>,
        metadata_only: bool,
    },
    MetadataGate {
        extension_id: ExtensionId,
        release_id: Digest,
        publisher_state_id: Digest,
        publisher_sequence: u64,
        head_sequence: u64,
        stable: bool,
        support: SupportSummary,
        metadata_only: bool,
    },
    Rejected {
        code: String,
    },
}

// Pure parser: returns data only. Only the owned process path below can turn
// this into a receipt after confirmed clean exit and actual ownership readback.
fn parse_reply(
    bytes: &[u8],
    id: u64,
    descriptor: &CandidateDescriptor,
) -> Result<Result<SupportSummary>> {
    let reply: Reply = canonical::decode(bytes, MAX_REPLY)?;
    if reply.schema_version != SCHEMA_VERSION || reply.request_id != id {
        bail!("metadata response scope differs");
    }
    parse_outcome(reply.outcome, descriptor)
}
fn parse_outcome(
    outcome: Outcome,
    descriptor: &CandidateDescriptor,
) -> Result<Result<SupportSummary>> {
    match outcome {
        Outcome::MetadataBatch { .. } => bail!("unexpected nested metadata batch"),
        Outcome::Rejected { code } => {
            if !matches!(
                code.as_str(),
                "limit"
                    | "signature"
                    | "unauthorized"
                    | "expired"
                    | "conflict"
                    | "stale"
                    | "io"
                    | "invalid"
            ) {
                bail!("invalid metadata rejection code");
            }
            Ok(Err(anyhow::anyhow!(
                "marketplace metadata was not admitted"
            )))
        }
        Outcome::MetadataGate {
            extension_id,
            release_id,
            publisher_state_id,
            publisher_sequence,
            head_sequence,
            stable,
            support,
            metadata_only,
        } => {
            let expected_release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
            let expected_weight: u32 = descriptor
                .gate_evidence
                .iter()
                .map(|pair| u32::from(pair.credential.payload.tenure_weight))
                .sum();
            let expected_expiry = descriptor
                .gate_evidence
                .iter()
                .map(|pair| pair.credential.payload.expires_at)
                .min();
            if !metadata_only
                || extension_id != descriptor.release.payload.extension_id
                || release_id != expected_release
                || publisher_state_id != Digest::of(descriptor.publisher.to_bytes()?)
                || publisher_sequence != descriptor.publisher.payload.sequence
                || head_sequence != descriptor.head.payload.sequence
                || stable != (descriptor.head.payload.stable_release_id == expected_release)
                || support.verified_supporters != MIN_DISCOVERY_SUPPORTERS
                || support.weighted_score != expected_weight
                || !support.discovery_qualified
                || support.capped
                || support.is_global_total
                || support.valid_until != expected_expiry
            {
                bail!("metadata response differs from checked descriptor");
            }
            Ok(Ok(support))
        }
    }
}

fn descriptor_request(descriptor: &CandidateDescriptor) -> Result<(u64, Vec<u8>)> {
    // Protocol JSON numbers are exact only through 2^53-1. A full random u64
    // would fail strict framing before launch; this private one-shot pipe does
    // not need an unbounded counter or a globally unique request number.
    let random = u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into()?);
    let id = (random & ((1u64 << 53) - 1)).max(1);
    let bytes = canonical::to_vec(&VerificationRequest::VerifyDescriptor {
        schema_version: SCHEMA_VERSION,
        request_id: id,
        descriptor: Box::new(descriptor.clone()),
    })?;
    if bytes.len() > MAX_CONTROL_FRAME_BYTES {
        bail!("metadata request bounds");
    }
    Ok((id, bytes))
}

impl Client {
    #[cfg(test)]
    pub(in crate::marketplace::worker::protected) fn fixture_cache_metadata(
        &self,
        value: &MetadataObservation,
    ) -> Result<()> {
        self.0.metadata_cache.lock().unwrap().insert(value)
    }
    /// Missing/negative batch results never launch another verifier for a
    /// warming neighbor. Only a current exact positive receipt is returned.
    pub(in crate::marketplace::worker::protected) fn cached_metadata(
        &self,
        descriptor: &CandidateDescriptor,
    ) -> Result<Option<MetadataObservation>> {
        Ok(self
            .cached_metadata_with_id(descriptor)?
            .map(|(_, value)| value))
    }
    pub(in crate::marketplace::worker::protected) fn cached_metadata_with_id(
        &self,
        descriptor: &CandidateDescriptor,
    ) -> Result<Option<(Digest, MetadataObservation)>> {
        self.0.cache_admit()?;
        if !self.0.binding.metadata_verifier {
            bail!("independent metadata verification is unavailable");
        }
        let policy = IssuerPolicy::new(self.0.binding.issuer_keys.iter().copied())?.fingerprint();
        let input = CheckedDescriptor::new(descriptor)?;
        Ok(self
            .0
            .metadata_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("metadata receipt cache unavailable"))?
            .get(&input, policy)
            .map(|value| (input.id(), value)))
    }
    pub(in crate::marketplace::worker::protected) fn warm_metadata_current(
        &self,
        value: &MetadataObservation,
    ) -> Result<bool> {
        self.0.cache_admit()?;
        let policy = IssuerPolicy::new(self.0.binding.issuer_keys.iter().copied())?.fingerprint();
        Ok(self.0.binding.metadata_verifier
            && value.issuer_policy() == policy
            && value.is_current())
    }
    pub(in crate::marketplace::worker::protected) async fn verify_metadata(
        &self,
        descriptor: &CandidateDescriptor,
    ) -> Result<MetadataObservation> {
        self.verify_metadata_scheduled(descriptor, Duration::ZERO, Work::Foreground)
            .await
    }
    pub(in crate::marketplace::worker::protected) async fn verify_background_metadata(
        &self,
        descriptor: &CandidateDescriptor,
    ) -> Result<MetadataObservation> {
        // The sharing owner starts renewal below this SAME margin. Returning
        // its aging cached receipt here would postpone a real native renewal
        // until too late, even though the owner began early enough.
        self.verify_metadata_scheduled(descriptor, SHARING_RENEWAL_LEAD, Work::Background)
            .await
    }
    pub(in crate::marketplace::worker::protected) async fn verify_selection_metadata(
        &self,
        descriptor: &CandidateDescriptor,
    ) -> Result<MetadataObservation> {
        self.verify_metadata_scheduled(descriptor, Duration::from_secs(120), Work::Foreground)
            .await
    }
    async fn verify_metadata_scheduled(
        &self,
        descriptor: &CandidateDescriptor,
        minimum: Duration,
        work: Work,
    ) -> Result<MetadataObservation> {
        if minimum > OBSERVATION_TTL {
            bail!("metadata validity request exceeds its original lifetime");
        }
        self.0.cache_admit()?;
        if !self.0.binding.metadata_verifier {
            bail!("independent metadata verification is unavailable");
        }
        let input = CheckedDescriptor::new(descriptor)?;
        let policy = IssuerPolicy::new(self.0.binding.issuer_keys.iter().copied())?.fingerprint();
        if let Some(value) = self
            .0
            .metadata_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("metadata receipt cache is unavailable"))?
            .get_with_validity(&input, policy, minimum)
        {
            return Ok(value);
        }
        work.admit_start(&self.0)?;
        // Share the source job's single slot: two helper classes must not each
        // consume the same reserved worker headroom concurrently. No job queue.
        let permit = admission::try_slot(&self.0, work, "metadata/source slot is busy")?;
        let descriptor = CandidateDescriptor::parse(input.bytes())?;
        let (id, bytes) = descriptor_request(&descriptor)?;
        let owner = self.0.clone();
        let (mut completion, response) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = match run(&owner, descriptor, id, bytes, &mut completion, work).await {
                Ok(value) => owner
                    .metadata_cache
                    .lock()
                    .map_err(|_| anyhow::anyhow!("metadata receipt cache is unavailable"))
                    .and_then(|mut cache| {
                        cache.insert(&value)?;
                        Ok(value)
                    }),
                Err(error) => Err(error),
            };
            let _ = completion.send(result);
        });
        response
            .await
            .context("metadata verification owner stopped")?
    }
}

async fn run(
    owner: &Arc<Owner>,
    descriptor: CandidateDescriptor,
    id: u64,
    bytes: Vec<u8>,
    completion: &mut oneshot::Sender<Result<MetadataObservation>>,
    work: Work,
) -> Result<MetadataObservation> {
    if completion.is_closed() {
        bail!("metadata verification cancelled before admission");
    }
    owner.headroom().await?;
    work.ready_before_launch(owner, completion).await?;
    if completion.is_closed() {
        bail!("metadata verification cancelled before launch");
    }
    let observed_utc =
        u64::try_from(chrono::Utc::now().timestamp()).context("metadata clock unavailable")?;
    let mut cleanup = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    let process = timeout(
        Duration::from_secs(10),
        owner.runtime.exec_marketplace_metadata_verifier(
            &owner.handle,
            &owner.profile.container_spec().labels,
            &owner.binding.issuer_keys,
        ),
    )
    .await
    .context("metadata verifier startup deadline")??;
    let result = timeout(
        Duration::from_secs(20),
        exchange_bytes(process, &bytes, MAX_REPLY, owner, completion, true, work),
    )
    .await
    .context("metadata verifier retirement deadline")??;
    // Invalid successful reports remain an integrity fault. A well-formed
    // native rejection or confirmed failed/cancelled process can safely retry.
    let result = match result {
        Ok(body) => parse_reply(&body, id, &descriptor)?,
        Err(error) => Err(error),
    };
    cleanup.armed = false;
    let support = result?;
    work.admit(owner, false)?;
    let mut integrity = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    timeout(
        Duration::from_secs(5),
        inspect_fixed_worker(owner.runtime.as_ref(), &owner.handle, &owner.profile),
    )
    .await
    .context("metadata verifier ownership readback deadline")??;
    integrity.armed = false;
    work.admit(owner, false)?;
    let observation = MetadataObservation {
        descriptor,
        ids: OnceLock::new(),
        support,
        issuer_policy: IssuerPolicy::new(owner.binding.issuer_keys.iter().copied())?.fingerprint(),
        observed: Instant::now(),
        observed_utc,
        protection: owner.protection.clone(),
    };
    if !observation.is_current() {
        bail!("metadata observation expired during verification");
    }
    Ok(observation)
}

#[cfg(test)]
mod tests;
