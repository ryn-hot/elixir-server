//! Private test networks only. Built only with the off-by-default
//! `private-test-network` feature; ordinary builds do not contain it.
//!
//! A listing appears in Browse only with five paid-supporter proofs signed by
//! an issuer the network trusts. There is no paid-account issuer yet, so a
//! private test network trusts only the public TEST issuer from the protocol
//! fixtures. This signs the five public TEST supporter proofs for one signed
//! release and offers it into this server's own catalog, exactly as the pilot
//! harness does. It refuses unless this server's startup profile trusts the
//! TEST issuer and nothing else, so it cannot make a listing discoverable on a
//! network that trusts a real issuer.
use anyhow::{Context, Result, ensure};
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor,
    canonical,
    evidence::IssuerPolicy,
    identity::{Digest, PublicKey},
    peer::{PeerOperation, PeerOutcome},
    publication::PreparedPublication,
    records::{SignatureCache, SignedRecord},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::state::AppState;

/// The public TEST issuer (seed `[3; 32]`) of `protocol-v1.json`. Its key is
/// published in the repository; it must never be trusted by a real network.
const TEST_ISSUER_SEED: [u8; 32] = [3; 32];
/// The five public TEST supporters endorse with seeds `[10; 32]`..`[14; 32]`.
const TEST_ENDORSER_SEED_BASE: u8 = 10;
const MAX_DAYS: u64 = 30;
const TENURE_WEIGHT: u8 = 2;
pub(crate) const MAX_REQUEST_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    schema_version: u32,
    /// The signed `publication.json` produced by `elixir-marketplace-author`.
    publication: Value,
    /// How long the proofs stay valid; the listing stops qualifying after.
    days: u64,
}
impl Request {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let request: Self = serde_json::from_slice(bytes)?;
        ensure!(request.schema_version == 1, "unsupported schema version");
        ensure!(
            (1..=MAX_DAYS).contains(&request.days),
            "days must be 1 to {MAX_DAYS}"
        );
        Ok(request)
    }
}

#[derive(Serialize)]
pub(crate) struct Offered {
    extension_id: String,
    release_id: String,
    expires_at: u64,
}

pub(crate) async fn offer(state: &AppState, request: Request) -> Result<Offered> {
    let issuer = SigningKey::from_bytes(&TEST_ISSUER_SEED);
    let test_key = PublicKey::from_bytes(issuer.verifying_key().to_bytes())?;
    let profile = state
        .settings
        .marketplace
        .startup_file
        .as_ref()
        .context("no marketplace startup profile is configured")?;
    let trusted = crate::marketplace::startup::trusted_issuer_keys(profile)?;
    ensure!(
        trusted == [test_key],
        "this server's network must trust only the TEST issuer"
    );
    let publication = PreparedPublication::parse(&serde_json::to_vec(&request.publication)?)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let expires_at = now + request.days * 86_400;
    let descriptor = descriptor(&publication, &issuer, test_key, now, expires_at)?;
    let release_id = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
    let reply = state
        .marketplace
        .client()?
        .request(PeerOperation::Offer {
            descriptor: Box::new(descriptor),
        })
        .await?;
    match reply.response {
        PeerOutcome::Offered {
            release_id: offered,
        } if offered == release_id => Ok(Offered {
            extension_id: publication.release.payload.extension_id.to_string(),
            release_id: release_id.to_string(),
            expires_at,
        }),
        PeerOutcome::Rejected { code } => anyhow::bail!("catalog refused the offer: {code:?}"),
        _ => anyhow::bail!("catalog did not confirm the offer"),
    }
}

/// The harness's TEST descriptor: the release's own signed publisher, release
/// and head records plus five TEST supporter pairs re-signed for this release.
fn descriptor(
    publication: &PreparedPublication,
    issuer: &SigningKey,
    issuer_key: PublicKey,
    now: u64,
    expires_at: u64,
) -> Result<CandidateDescriptor> {
    let fixture: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let mut value = CandidateDescriptor::parse(&serde_json::to_vec(&json!({
        "schema_version": 1,
        "publisher": publication.publisher,
        "release": publication.release,
        "head": publication.head,
        "revocations": [],
        "gate_evidence": fixture["evidence"]["pairs"],
    }))?)?;
    ensure!(
        value.gate_evidence.len() == 5,
        "five TEST supporters required"
    );
    let extension_id = publication.release.payload.extension_id.clone();
    let release_id = Digest::of(canonical::to_vec(&publication.release.payload)?);
    for (index, pair) in value.gate_evidence.iter_mut().enumerate() {
        let mut credential = pair.credential.payload.clone();
        credential.extension_id = extension_id.clone();
        credential.tenure_weight = TENURE_WEIGHT;
        credential.issued_at = now;
        credential.expires_at = expires_at;
        pair.credential = SignedRecord::sign(credential, None, issuer)?;
        let mut endorsement = pair.endorsement.payload.clone();
        endorsement.extension_id = extension_id.clone();
        endorsement.release_id = release_id;
        endorsement.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(
            endorsement,
            None,
            &SigningKey::from_bytes(&[TEST_ENDORSER_SEED_BASE + index as u8; 32]),
        )?;
    }
    value.verify_metadata(
        now,
        &IssuerPolicy::new([issuer_key])?,
        &mut SignatureCache::new(1024)?,
    )?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector_publication() -> Result<PreparedPublication> {
        let vector: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        )))?;
        Ok(PreparedPublication::parse(&serde_json::to_vec(&json!({
            "schema_version": 1,
            "publisher": vector["publisher"],
            "release": vector["release"],
            "head": vector["head"],
        }))?)?)
    }

    #[test]
    fn signs_five_test_supporters_only_the_test_issuer_accepts() -> Result<()> {
        let publication = vector_publication()?;
        let issuer = SigningKey::from_bytes(&TEST_ISSUER_SEED);
        let key = PublicKey::from_bytes(issuer.verifying_key().to_bytes())?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let value = descriptor(&publication, &issuer, key, now, now + 14 * 86_400)?;
        assert_eq!(value.gate_evidence.len(), 5);
        let release_id = Digest::of(canonical::to_vec(&publication.release.payload)?);
        for pair in &value.gate_evidence {
            assert_eq!(pair.endorsement.payload.release_id, release_id);
            assert_eq!(pair.credential.payload.expires_at, now + 14 * 86_400);
        }
        let other =
            PublicKey::from_bytes(SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes())?;
        assert!(
            value
                .verify_metadata(
                    now,
                    &IssuerPolicy::new([other])?,
                    &mut SignatureCache::new(64)?
                )
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn requests_are_bounded() {
        let request = |schema: u32, days: u64| {
            serde_json::to_vec(&json!({"schema_version": schema, "publication": {}, "days": days}))
                .unwrap()
        };
        assert!(Request::parse(&request(1, 14)).is_ok());
        assert!(Request::parse(&request(1, 0)).is_err());
        assert!(Request::parse(&request(1, MAX_DAYS + 1)).is_err());
        assert!(Request::parse(&request(2, 14)).is_err());
        assert!(
            Request::parse(br#"{"schema_version":1,"publication":{},"days":14,"extra":1}"#)
                .is_err()
        );
    }
}
