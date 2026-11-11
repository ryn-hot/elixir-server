//! Independently authenticated ranking facts, never peer totals. One bounded
//! batch per private native process; aggregation/deduplication remains separate.
use super::*;
use elixir_marketplace::{
    EVIDENCE_BATCH_SIZE, MAX_CONTROL_FRAME_BYTES, SCHEMA_VERSION, canonical,
    evidence::{EvidenceBatch, IssuerPolicy},
    identity::ExtensionId,
    verifier::{VerificationRequest, VerifiedWitness},
};
use serde::{Deserialize, Serialize};

const MAX_REPLY: usize = 32 * 1024;
mod cache;
pub(super) use cache::ScoreCache;

/// No deserializer, public constructor, or public Clone. Only the owned native
/// process path or its same-owner positive cryptographic cache creates these
/// current validation receipts. Cache reuse does not claim a new native process
/// ran or renew source availability. Facts may be expired
/// individually; consumers must deduplicate subjects and check expires_at.
pub struct EvidenceObservation {
    extension_id: ExtensionId,
    release_id: Digest,
    batch_id: Digest,
    issuer_policy: Digest,
    snapshot_id: Digest,
    next_cursor: Option<String>,
    examined: usize,
    verified: Vec<VerifiedWitness>,
    observed: Instant,
    observed_utc: u64,
    protection: watch::Receiver<Snapshot>,
}
impl EvidenceObservation {
    /// Select only exact authenticated bytes for bounded local redistribution.
    /// A dominated untrusted claim omitted by the score cache is deliberately
    /// absent: saving crypto work is not permission to relay that claim.
    pub(crate) fn retained_batch(
        &self,
        metadata: &MetadataObservation,
        batch: &EvidenceBatch,
    ) -> Result<EvidenceBatch> {
        let release = Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?);
        if !metadata.is_current()
            || !self.is_current()
            || self.extension_id() != &metadata.descriptor().release.payload.extension_id
            || self.release_id() != release
            || batch.release_id != release
            || self.issuer_policy() != metadata.issuer_policy()
            || self.batch_id() != Digest::of(canonical::to_vec(batch)?)
            || self.examined() != batch.pairs.len()
        {
            bail!("retained evidence scope or input differs");
        }
        let now = observation_utc()?;
        let mut indices = 0u64;
        let mut pairs = vec![];
        for witness in self.witnesses()? {
            let pair = batch
                .pairs
                .get(witness.pair_index)
                .context("retained evidence index differs")?;
            let bit = 1u64
                .checked_shl(witness.pair_index as u32)
                .context("retained evidence index limit")?;
            if indices & bit != 0 {
                bail!("duplicate retained evidence index");
            }
            indices |= bit;
            if witness.expires_at <= now {
                continue;
            }
            if witness.subject != pair.credential.payload.subject
                || witness.weight != pair.credential.payload.tenure_weight
                || witness.expires_at != pair.credential.payload.expires_at
                || witness.issuer != pair.credential.signer_key
                || witness.credential_id != Digest::of(pair.credential.to_bytes()?)
            {
                bail!("retained evidence bytes differ from authenticated facts");
            }
            pairs.push(pair.clone());
        }
        let retained = EvidenceBatch {
            schema_version: SCHEMA_VERSION,
            release_id: release,
            snapshot_id: Digest::of(
                [
                    b"elixir-marketplace/retained-evidence/v1\0".as_slice(),
                    self.batch_id().as_bytes(),
                ]
                .concat(),
            ),
            pairs,
            next_cursor: None,
        };
        // The source peer's continuation is never stored or served as ours.
        EvidenceBatch::parse(&canonical::to_vec(&retained)?)?;
        Ok(retained)
    }
    pub fn extension_id(&self) -> &ExtensionId {
        &self.extension_id
    }
    pub fn release_id(&self) -> Digest {
        self.release_id
    }
    pub fn batch_id(&self) -> Digest {
        self.batch_id
    }
    pub fn issuer_policy(&self) -> Digest {
        self.issuer_policy
    }
    pub fn snapshot_id(&self) -> Digest {
        self.snapshot_id
    }
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }
    pub fn examined(&self) -> usize {
        self.examined
    }
    pub fn witnesses(&self) -> Result<&[VerifiedWitness]> {
        if !self.is_current() {
            bail!("evidence observation is no longer current");
        }
        Ok(&self.verified)
    }
    pub fn is_current(&self) -> bool {
        self.observed.elapsed() <= OBSERVATION_TTL
            && wall_clock_current(self.observed_utc, chrono::Utc::now().timestamp())
            && matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    schema_version: u32,
    request_id: u64,
    outcome: Outcome,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Outcome {
    EvidenceSignatures {
        extension_id: ExtensionId,
        release_id: Digest,
        snapshot_id: Digest,
        verified: Vec<VerifiedWitness>,
        invalid: usize,
    },
    Rejected {
        code: String,
    },
}

// Data parser only. No report received over the network can manufacture the
// private observation above; successful process retirement/readback is required.
fn parse(
    bytes: &[u8],
    id: u64,
    extension: &ExtensionId,
    batch: &EvidenceBatch,
    policy: &IssuerPolicy,
) -> Result<Result<Vec<VerifiedWitness>>> {
    let reply: Reply = canonical::decode(bytes, MAX_REPLY)?;
    if reply.schema_version != SCHEMA_VERSION || reply.request_id != id {
        bail!("evidence response scope differs");
    }
    match reply.outcome {
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
                bail!("invalid evidence rejection code");
            }
            Ok(Err(anyhow::anyhow!("evidence batch was not authenticated")))
        }
        Outcome::EvidenceSignatures {
            extension_id,
            release_id,
            snapshot_id,
            verified,
            invalid,
        } => {
            if extension_id != *extension
                || release_id != batch.release_id
                || snapshot_id != batch.snapshot_id
                || verified.len() > EVIDENCE_BATCH_SIZE
                || invalid > batch.pairs.len()
                || verified.len().checked_add(invalid) != Some(batch.pairs.len())
            {
                bail!("evidence response bounds differ");
            }
            let mut indices = 0u64;
            for witness in &verified {
                let pair = batch
                    .pairs
                    .get(witness.pair_index)
                    .context("evidence response index differs")?;
                let bit = 1u64
                    .checked_shl(witness.pair_index as u32)
                    .context("evidence index bounds")?;
                if indices & bit != 0 {
                    bail!("duplicate verified evidence index");
                }
                indices |= bit;
                let credential = &pair.credential.payload;
                let endorsement = &pair.endorsement.payload;
                if witness.subject != credential.subject
                    || witness.weight != credential.tenure_weight
                    || !(1..=4).contains(&witness.weight)
                    || witness.expires_at != credential.expires_at
                    || witness.issuer != pair.credential.signer_key
                    || !policy.allows(&witness.issuer)
                    || witness.issuer != credential.issuer_key_id
                    || credential.extension_id != *extension
                    || endorsement.extension_id != *extension
                    || endorsement.release_id != batch.release_id
                    || witness.credential_id != Digest::of(pair.credential.to_bytes()?)
                    || witness.credential_id != endorsement.credential_id
                    || pair.endorsement.signer_key != credential.endorsement_public_key
                {
                    bail!("evidence response differs from checked input");
                }
            }
            Ok(Ok(verified))
        }
    }
}

fn request(extension: ExtensionId, batch: EvidenceBatch) -> Result<(u64, Vec<u8>)> {
    let random = u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into()?);
    let id = (random & ((1u64 << 53) - 1)).max(1);
    let bytes = canonical::to_vec(&VerificationRequest::VerifyEvidence {
        schema_version: SCHEMA_VERSION,
        request_id: id,
        extension_id: extension,
        release_id: batch.release_id,
        batch,
    })?;
    if bytes.len() > MAX_CONTROL_FRAME_BYTES {
        bail!("evidence request bounds");
    }
    Ok((id, bytes))
}
impl Client {
    pub(in crate::marketplace::worker::protected) async fn verify_evidence(
        &self,
        metadata: &MetadataObservation,
        batch: &EvidenceBatch,
    ) -> Result<EvidenceObservation> {
        self.0.cache_admit()?;
        if !self.0.binding.evidence_verifier {
            bail!("independent evidence verification is unavailable");
        }
        let policy = IssuerPolicy::new(self.0.binding.issuer_keys.iter().copied())?;
        if !metadata.is_current() || metadata.issuer_policy() != policy.fingerprint() {
            bail!("current matching metadata is required for ranking");
        }
        let extension = metadata.descriptor().release.payload.extension_id.clone();
        let release = Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?);
        let batch = EvidenceBatch::parse(&canonical::to_vec(batch)?)?;
        if batch.release_id != release {
            bail!("ranking release differs from verified metadata");
        }
        let (mut verified, missing) = self
            .0
            .ranking_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("ranking cache unavailable"))?
            .lookup(metadata, &batch, &policy, observation_utc()?)?;
        if missing.is_empty() {
            return Ok(EvidenceObservation {
                extension_id: extension,
                release_id: release,
                batch_id: Digest::of(canonical::to_vec(&batch)?),
                issuer_policy: policy.fingerprint(),
                snapshot_id: batch.snapshot_id,
                next_cursor: batch.next_cursor,
                examined: batch.pairs.len(),
                verified,
                observed: Instant::now(),
                observed_utc: observation_utc()?,
                protection: self.0.protection.clone(),
            });
        }
        Work::Foreground.admit_start(&self.0)?;
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .context("metadata/source/security/evidence slot is busy")?;
        let subset = EvidenceBatch {
            pairs: missing.iter().map(|i| batch.pairs[*i].clone()).collect(),
            ..batch.clone()
        };
        let (id, bytes) = request(extension.clone(), subset.clone())?;
        let owner = self.0.clone();
        let (mut completion, response) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = run(
                &owner,
                extension,
                subset,
                policy,
                id,
                bytes,
                &mut completion,
            )
            .await;
            let _ = completion.send(result);
        });
        let mut observation = response
            .await
            .context("evidence verifier owner stopped")??;
        if !metadata.is_current() || !observation.is_current() {
            bail!("ranking checks expired during verification");
        }
        for mut value in observation.verified {
            value.pair_index = *missing
                .get(value.pair_index)
                .context("native subset index differs")?;
            verified.push(value);
        }
        observation.verified = verified;
        observation.batch_id = Digest::of(canonical::to_vec(&batch)?);
        observation.snapshot_id = batch.snapshot_id;
        observation.next_cursor = batch.next_cursor.clone();
        observation.examined = batch.pairs.len();
        self.0
            .ranking_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("ranking cache unavailable"))?
            .merge(
                metadata,
                &batch,
                &observation,
                &IssuerPolicy::new(self.0.binding.issuer_keys.iter().copied())?,
                observation_utc()?,
            )?;
        Ok(observation)
    }
    pub(in crate::marketplace::worker::protected) fn evidence_enabled(&self) -> bool {
        self.0.binding.evidence_verifier
    }
    pub(in crate::marketplace::worker::protected) fn ranking_summary(
        &self,
        metadata: &MetadataObservation,
    ) -> Result<elixir_marketplace::evidence::SupportSummary> {
        self.0.cache_admit()?;
        self.0
            .ranking_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("ranking cache unavailable"))?
            .summary(
                metadata,
                &IssuerPolicy::new(self.0.binding.issuer_keys.iter().copied())?,
                observation_utc()?,
            )
    }
}

#[cfg(test)]
impl EvidenceObservation {
    /// Real local signature verification with a synthetic protection owner,
    /// solely for controller/scheduler tests. Not native-process proof.
    pub(in crate::marketplace) fn fixture_for_batch(
        metadata: &MetadataObservation,
        batch: &EvidenceBatch,
        policy: &IssuerPolicy,
    ) -> Result<(Self, impl Fn(bool) + use<>)> {
        let extension = metadata.descriptor().release.payload.extension_id.clone();
        let (id, bytes) = request(extension.clone(), batch.clone())?;
        let report = elixir_marketplace::verifier::Verifier::new(policy.clone())?
            .process(&bytes, observation_utc()?);
        let verified = parse(&canonical::to_vec(&report)?, id, &extension, batch, policy)??;
        let (owner, protection) = watch::channel(Snapshot::timed(
            ProtectionState::Ready,
            Duration::from_secs(3600),
        ));
        Ok((
            Self {
                extension_id: extension,
                release_id: batch.release_id,
                batch_id: Digest::of(canonical::to_vec(batch)?),
                issuer_policy: policy.fingerprint(),
                snapshot_id: batch.snapshot_id,
                next_cursor: batch.next_cursor.clone(),
                examined: batch.pairs.len(),
                verified,
                observed: Instant::now(),
                observed_utc: observation_utc()?,
                protection,
            },
            move |active| {
                owner.send_replace(Snapshot::timed(
                    if active {
                        ProtectionState::Ready
                    } else {
                        ProtectionState::Failed
                    },
                    Duration::from_secs(3600),
                ));
            },
        ))
    }
}

async fn run(
    owner: &Arc<Owner>,
    extension: ExtensionId,
    batch: EvidenceBatch,
    policy: IssuerPolicy,
    id: u64,
    bytes: Vec<u8>,
    completion: &mut oneshot::Sender<Result<EvidenceObservation>>,
) -> Result<EvidenceObservation> {
    if completion.is_closed() {
        bail!("evidence verification cancelled before admission");
    }
    owner.headroom().await?;
    Work::Foreground
        .ready_before_launch(owner, completion)
        .await?;
    if completion.is_closed() {
        bail!("evidence verification cancelled before launch");
    }
    let observed_utc = observation_utc()?;
    let mut cleanup = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    let process = timeout(
        Duration::from_secs(10),
        owner.runtime.exec_marketplace_evidence_verifier(
            &owner.handle,
            &owner.profile.container_spec().labels,
            &owner.binding.issuer_keys,
        ),
    )
    .await
    .context("evidence verifier startup deadline")??;
    let result = timeout(
        Duration::from_secs(20),
        exchange_bytes(
            process,
            &bytes,
            MAX_REPLY,
            owner,
            completion,
            true,
            Work::Foreground,
        ),
    )
    .await
    .context("evidence verifier retirement deadline")??;
    let result = match result {
        Ok(body) => parse(&body, id, &extension, &batch, &policy)?,
        Err(error) => Err(error),
    };
    cleanup.armed = false;
    let verified = result?;
    Work::Foreground.admit(owner, false)?;
    let mut integrity = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    timeout(
        Duration::from_secs(5),
        inspect_fixed_worker(owner.runtime.as_ref(), &owner.handle, &owner.profile),
    )
    .await
    .context("evidence verifier ownership readback deadline")??;
    integrity.armed = false;
    Work::Foreground.admit(owner, false)?;
    let observation = EvidenceObservation {
        extension_id: extension,
        release_id: batch.release_id,
        batch_id: Digest::of(canonical::to_vec(&batch)?),
        issuer_policy: policy.fingerprint(),
        snapshot_id: batch.snapshot_id,
        next_cursor: batch.next_cursor,
        examined: batch.pairs.len(),
        verified,
        observed: Instant::now(),
        observed_utc,
        protection: owner.protection.clone(),
    };
    if !observation.is_current() {
        bail!("evidence observation expired during verification");
    }
    Ok(observation)
}

#[cfg(test)]
pub(super) mod tests;
