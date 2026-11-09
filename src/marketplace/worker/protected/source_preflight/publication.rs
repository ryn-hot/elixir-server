//! Direct-reference signature receipt. Deliberately not MetadataObservation:
//! zero paid proofs cannot create a searchable descriptor or an install grant.
use super::*;
use elixir_marketplace::{
    SCHEMA_VERSION, canonical,
    publication::{PreparedPublication, PublicationSummary},
    verifier::VerificationRequest,
};
use serde::{Deserialize, Serialize};

const PREVIEW_MIN_VALIDITY: Duration = Duration::from_secs(30);

// Closed local diagnostics only: never log native output, records, account or
// peer identities. A stage is not an admission decision or a retry permission.
fn unavailable(stage: &'static str) {
    tracing::debug!(target: "elixir_marketplace::sharing_diagnostics", stage, "publication verification unavailable");
}

pub struct PublicationObservation {
    publication: PreparedPublication,
    summary: PublicationSummary,
    observed: Instant,
    observed_utc: u64,
    protection: watch::Receiver<Snapshot>,
}
impl PublicationObservation {
    pub(super) fn from_exact_metadata(
        publication: &PreparedPublication,
        receipt: &super::metadata::CompactReceipt,
        policy: Digest,
    ) -> Result<Option<Self>> {
        let reference_id = Digest::of(publication.to_bytes()?);
        let Some((observed, observed_utc, protection)) =
            receipt.publication_basis(reference_id, policy)
        else {
            return Ok(None);
        };
        // Hashing exact authenticated records is not signature verification in
        // the host. The opaque native receipt above is the sole proof source.
        let release_id = Digest::of(canonical::to_vec(&publication.release.payload)?);
        let summary = PublicationSummary {
            reference_id,
            extension_id: publication.release.payload.extension_id.clone(),
            release_id,
            publisher_state_id: Digest::of(publication.publisher.to_bytes()?),
            publisher_payload_id: Digest::of(canonical::to_vec(&publication.publisher.payload)?),
            publisher_sequence: publication.publisher.payload.sequence,
            head_record_id: Digest::of(publication.head.to_bytes()?),
            head_payload_id: Digest::of(canonical::to_vec(&publication.head.payload)?),
            head_sequence: publication.head.payload.sequence,
            stable: publication.head.payload.stable_release_id == release_id,
        };
        let value = Self {
            publication: publication.clone(),
            summary,
            observed,
            observed_utc,
            protection,
        };
        Ok(value.is_current().then_some(value))
    }
    pub fn publication(&self) -> &PreparedPublication {
        &self.publication
    }
    pub fn summary(&self) -> Result<&PublicationSummary> {
        if !self.is_current() {
            bail!("direct-reference observation expired");
        }
        Ok(&self.summary)
    }
    pub fn is_current(&self) -> bool {
        self.observed.elapsed() <= OBSERVATION_TTL
            && wall_clock_current(self.observed_utc, chrono::Utc::now().timestamp())
            && matches!(
                observed_state(&self.protection),
                ProtectionState::Ready | ProtectionState::Checking
            )
    }
    pub fn valid_for(&self) -> Duration {
        if !self.is_current() {
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
    #[cfg(test)]
    pub(in crate::marketplace) fn fixture_for_verified_publication(
        publication: PreparedPublication,
    ) -> Result<(Self, impl Fn(bool) + use<>)> {
        let observed_utc = observation_utc()?;
        let summary = publication.verify(observed_utc)?;
        let (owner, protection) = watch::channel(Snapshot {
            state: ProtectionState::Ready,
            until: Some(Instant::now() + Duration::from_secs(3600)),
        });
        Ok((
            Self {
                publication,
                summary,
                observed: Instant::now(),
                observed_utc,
                protection,
            },
            move |healthy| {
                owner.send_replace(Snapshot {
                    state: if healthy {
                        ProtectionState::Ready
                    } else {
                        ProtectionState::Failed
                    },
                    until: Some(Instant::now() + Duration::from_secs(3600)),
                });
            },
        ))
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
    PublicationSignatures {
        summary: Box<PublicationSummary>,
        metadata_only: bool,
        discovery_qualified: bool,
    },
    Rejected {
        code: String,
    },
}
fn parse_reply(
    bytes: &[u8],
    id: u64,
    publication: &PreparedPublication,
) -> Result<Result<PublicationSummary>> {
    let reply: Reply = canonical::decode(bytes, 4096)?;
    if reply.schema_version != SCHEMA_VERSION || reply.request_id != id {
        bail!("direct-reference response correlation differs");
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
                bail!("invalid direct-reference rejection");
            }
            Ok(Err(anyhow::anyhow!(
                "direct reference was not authenticated"
            )))
        }
        Outcome::PublicationSignatures {
            summary,
            metadata_only,
            discovery_qualified,
        } => {
            let expected_release = Digest::of(canonical::to_vec(&publication.release.payload)?);
            if !metadata_only
                || discovery_qualified
                || summary.reference_id != Digest::of(publication.to_bytes()?)
                || summary.extension_id != publication.release.payload.extension_id
                || summary.release_id != expected_release
                || summary.publisher_state_id != Digest::of(publication.publisher.to_bytes()?)
                || summary.publisher_payload_id
                    != Digest::of(canonical::to_vec(&publication.publisher.payload)?)
                || summary.publisher_sequence != publication.publisher.payload.sequence
                || summary.head_record_id != Digest::of(publication.head.to_bytes()?)
                || summary.head_payload_id
                    != Digest::of(canonical::to_vec(&publication.head.payload)?)
                || summary.head_sequence != publication.head.payload.sequence
                || summary.stable
                    != (publication.head.payload.stable_release_id == expected_release)
            {
                bail!("direct-reference response binding differs");
            }
            Ok(Ok(*summary))
        }
    }
}

impl Client {
    pub(in crate::marketplace::worker::protected) async fn verify_publication(
        &self,
        publication: &PreparedPublication,
    ) -> Result<PublicationObservation> {
        self.verify_publication_scheduled(publication, Work::Foreground, false)
            .await
    }
    pub(in crate::marketplace::worker::protected) async fn verify_publication_for_preview(
        &self,
        publication: &PreparedPublication,
    ) -> Result<PublicationObservation> {
        self.verify_publication_scheduled(publication, Work::Foreground, true)
            .await
    }
    pub(in crate::marketplace::worker::protected) async fn verify_background_publication(
        &self,
        publication: &PreparedPublication,
    ) -> Result<PublicationObservation> {
        self.verify_publication_scheduled(publication, Work::Background, false)
            .await
    }
    async fn verify_publication_scheduled(
        &self,
        publication: &PreparedPublication,
        work: Work,
        preview: bool,
    ) -> Result<PublicationObservation> {
        work.admit_start(&self.0)
            .inspect_err(|_| unavailable("live_admission"))?;
        if !self.0.binding.publication_verifier {
            bail!("independent direct-reference verification unavailable");
        }
        let publication = PreparedPublication::parse(&publication.to_bytes()?)?;
        let policy = elixir_marketplace::evidence::IssuerPolicy::new(
            self.0.binding.issuer_keys.iter().copied(),
        )?
        .fingerprint();
        if preview
            && let Some(observation) = self
                .0
                .metadata_cache
                .lock()
                .map_err(|_| anyhow::anyhow!("metadata receipt cache unavailable"))?
                .get_publication(&publication, policy)?
            && observation.valid_for() >= PREVIEW_MIN_VALIDITY
        {
            return Ok(observation);
        }
        let permit = if preview {
            publication_slot(&self.0, work).await?
        } else {
            // Install/update selection and publisher renewal retain their
            // original fresh verification and immediate no-queue behavior.
            admission::try_slot(&self.0, work, "source/verification slot is busy")
                .inspect_err(|_| unavailable("helper_slot"))?
        };
        let random = u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into()?);
        let id = (random & ((1u64 << 53) - 1)).max(1);
        let bytes = canonical::to_vec(&VerificationRequest::VerifyPublication {
            schema_version: SCHEMA_VERSION,
            request_id: id,
            publication: Box::new(publication.clone()),
        })?;
        let owner = self.0.clone();
        let (mut completion, response) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = run(&owner, publication, id, bytes, &mut completion, work).await;
            let _ = completion.send(result);
        });
        response
            .await
            .context("direct-reference verifier owner stopped")?
    }
}

async fn publication_slot(owner: &Owner, work: Work) -> Result<tokio::sync::OwnedSemaphorePermit> {
    let permit = match work {
        Work::Background => admission::try_slot(owner, work, "source/verification slot is busy")?,
        // The service already admits only one preview and one installation.
        // Wait fairly for that existing slot, not an unbounded job queue.
        // Dropping this borrowed wait cancels before any native launch/input.
        Work::Foreground => timeout(
            super::super::RENEW_DEADLINE,
            owner.slots.clone().acquire_owned(),
        )
        .await
        .context("direct-reference verification slot deadline")?
        .context("direct-reference verification owner stopped")?,
    };
    work.admit_start(owner)?;
    Ok(permit)
}
async fn run(
    owner: &Arc<Owner>,
    publication: PreparedPublication,
    id: u64,
    bytes: Vec<u8>,
    completion: &mut oneshot::Sender<Result<PublicationObservation>>,
    work: Work,
) -> Result<PublicationObservation> {
    if completion.is_closed() {
        bail!("direct-reference verification cancelled");
    }
    owner
        .headroom()
        .await
        .inspect_err(|_| unavailable("worker_headroom"))?;
    work.ready_before_launch(owner, completion)
        .await
        .inspect_err(|_| unavailable("protected_readiness"))?;
    if completion.is_closed() {
        bail!("direct-reference verification cancelled before launch");
    }
    let observed_utc = observation_utc()?;
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
    .context("direct-reference verifier startup deadline")
    .inspect_err(|_| unavailable("native_start"))?
    .inspect_err(|_| unavailable("native_start"))?;
    let result = timeout(
        Duration::from_secs(20),
        exchange_bytes(process, &bytes, 4096, owner, completion, true, work),
    )
    .await
    .context("direct-reference verifier retirement deadline")
    .inspect_err(|_| unavailable("native_exchange"))?
    .inspect_err(|_| unavailable("native_exchange"))?;
    let result = match result {
        Ok(body) => {
            parse_reply(&body, id, &publication).inspect_err(|_| unavailable("native_receipt"))?
        }
        Err(error) => Err(error),
    };
    cleanup.armed = false;
    let summary = result.inspect_err(|_| unavailable("native_authorization"))?;
    work.admit(owner, false)
        .inspect_err(|_| unavailable("post_verification_admission"))?;
    let mut integrity = FaultOnDrop {
        owner: owner.clone(),
        armed: true,
    };
    timeout(
        Duration::from_secs(5),
        inspect_fixed_worker(owner.runtime.as_ref(), &owner.handle, &owner.profile),
    )
    .await
    .context("direct-reference verifier ownership readback deadline")
    .inspect_err(|_| unavailable("ownership_readback"))?
    .inspect_err(|_| unavailable("ownership_readback"))?;
    integrity.armed = false;
    work.admit(owner, false)
        .inspect_err(|_| unavailable("post_readback_admission"))?;
    let value = PublicationObservation {
        publication,
        summary,
        observed: Instant::now(),
        observed_utc,
        protection: owner.protection.clone(),
    };
    if !value.is_current() {
        bail!("direct-reference verification expired");
    }
    Ok(value)
}

#[cfg(test)]
mod tests;
