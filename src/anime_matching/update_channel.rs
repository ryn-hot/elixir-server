//! Signed, monotonic update channel for replaceable anime inference bundles.
//!
//! Model capability remains fixed by offline qualification. This module lets a
//! future qualified bundle replace the current one without a server release,
//! while preserving one automatic path and no user-facing model controls.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    AnimeInferenceBundleManifest, QualifiedAnimeBundleApproval,
    QualifiedAnimeRuntimeProfileApproval,
};

pub const ANIME_UPDATE_CHANNEL_SCHEMA_VERSION: u32 = 2;
pub const ANIME_UPDATE_CHANNEL_NAME: &str = "stable";
const UPDATE_CHANNEL_STATE_FILE: &str = "accepted-update-channel.json";
const SIGNATURE_DOMAIN: &[u8] = b"elixir-anime-inference-update-v2\0";
const MAX_FUTURE_CLOCK_SKEW: Duration = Duration::minutes(5);
const MAX_ENVELOPE_VALIDITY: Duration = Duration::days(366);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeBundleChannelPayload {
    pub channel: String,
    pub sequence: u64,
    pub issued_at: String,
    pub expires_at: String,
    pub manifest_fingerprint: String,
    /// SHA-256 of the exact schema-v2 bundle-closure bytes revalidated by the
    /// protected release signer. This prevents a valid manifest signature from
    /// being detached from the runtime, model-build, smoke, and qualification
    /// evidence that authorized its publication.
    pub bundle_closure_fingerprint: String,
    pub model_sha256: String,
    pub qualification_report_fingerprint: String,
    /// Physical hardware/runtime certifications authorized by the same signed
    /// release closure. Omission preserves legacy deterministic-only channels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub certified_runtime_profiles: Vec<QualifiedAnimeRuntimeProfileApproval>,
    pub manifest: AnimeInferenceBundleManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignedAnimeBundleEnvelope {
    pub schema_version: u32,
    pub key_id: String,
    pub signed: AnimeBundleChannelPayload,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TrustedAnimeUpdateKey {
    key_id: String,
    public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcceptedAnimeUpdateChannel {
    schema_version: u32,
    envelope_fingerprint: String,
    accepted_at: String,
    envelope: SignedAnimeBundleEnvelope,
}

#[derive(Debug, Clone)]
pub struct VerifiedAnimeBundleEnvelope {
    pub envelope: SignedAnimeBundleEnvelope,
    pub envelope_fingerprint: String,
    pub approval: QualifiedAnimeBundleApproval,
}

impl VerifiedAnimeBundleEnvelope {
    pub fn manifest(&self) -> &AnimeInferenceBundleManifest {
        &self.envelope.signed.manifest
    }

    pub fn sequence(&self) -> u64 {
        self.envelope.signed.sequence
    }

    pub fn key_id(&self) -> &str {
        &self.envelope.key_id
    }

    pub fn bundle_closure_fingerprint(&self) -> &str {
        &self.envelope.signed.bundle_closure_fingerprint
    }
}

pub fn verify_anime_bundle_envelope(
    envelope: SignedAnimeBundleEnvelope,
    now: DateTime<Utc>,
    require_fresh: bool,
) -> Result<VerifiedAnimeBundleEnvelope> {
    let keys: Vec<TrustedAnimeUpdateKey> =
        serde_json::from_str(include_str!("trusted-anime-update-keys.json"))
            .context("decoding compiled anime update trust roots")?;
    verify_anime_bundle_envelope_with_keys(envelope, now, require_fresh, &keys)
}

fn verify_anime_bundle_envelope_with_keys(
    envelope: SignedAnimeBundleEnvelope,
    now: DateTime<Utc>,
    require_fresh: bool,
    keys: &[TrustedAnimeUpdateKey],
) -> Result<VerifiedAnimeBundleEnvelope> {
    ensure!(
        envelope.schema_version == ANIME_UPDATE_CHANNEL_SCHEMA_VERSION,
        "unsupported anime update envelope schema"
    );
    ensure!(
        envelope.signed.channel == ANIME_UPDATE_CHANNEL_NAME,
        "unsupported anime update channel"
    );
    ensure!(
        envelope.signed.sequence > 0,
        "anime update sequence must be positive"
    );
    let issued = DateTime::parse_from_rfc3339(&envelope.signed.issued_at)
        .context("anime update issuedAt is not RFC3339")?
        .with_timezone(&Utc);
    let expires = DateTime::parse_from_rfc3339(&envelope.signed.expires_at)
        .context("anime update expiresAt is not RFC3339")?
        .with_timezone(&Utc);
    ensure!(expires > issued, "anime update expiry must follow issuance");
    ensure!(
        expires - issued <= MAX_ENVELOPE_VALIDITY,
        "anime update envelope validity exceeds one year"
    );
    ensure!(
        issued <= now + MAX_FUTURE_CLOCK_SKEW,
        "anime update envelope was issued in the future"
    );
    if require_fresh {
        ensure!(expires > now, "anime update envelope has expired");
    }

    validate_sha256(
        &envelope.signed.manifest_fingerprint,
        "manifest fingerprint",
    )?;
    validate_sha256(
        &envelope.signed.bundle_closure_fingerprint,
        "bundle closure fingerprint",
    )?;
    validate_sha256(&envelope.signed.model_sha256, "model SHA-256")?;
    validate_sha256(
        &envelope.signed.qualification_report_fingerprint,
        "qualification report fingerprint",
    )?;
    let manifest_bytes = serde_json::to_vec(&envelope.signed.manifest)
        .context("encoding signed anime bundle manifest")?;
    let actual_manifest_fingerprint = sha256_prefixed(&manifest_bytes);
    ensure!(
        sha256_eq(
            &actual_manifest_fingerprint,
            &envelope.signed.manifest_fingerprint,
        ),
        "signed anime manifest fingerprint does not match its content"
    );
    ensure!(
        sha256_eq(
            &envelope.signed.model_sha256,
            &envelope.signed.manifest.model.sha256,
        ),
        "signed anime model SHA-256 does not match the manifest"
    );
    ensure!(
        sha256_eq(
            &envelope.signed.qualification_report_fingerprint,
            &envelope
                .signed
                .manifest
                .model
                .qualification_report_fingerprint,
        ),
        "signed qualification fingerprint does not match the manifest"
    );

    ensure!(!keys.is_empty(), "no anime update trust roots are compiled");
    let mut key_ids = BTreeSet::new();
    for key in keys {
        ensure!(
            !key.key_id.trim().is_empty() && key_ids.insert(key.key_id.as_str()),
            "anime update trust roots contain an empty or duplicate key ID"
        );
    }
    let trusted = keys
        .iter()
        .find(|key| key.key_id == envelope.key_id)
        .context("anime update envelope uses an untrusted key ID")?;
    let public_bytes = general_purpose::STANDARD
        .decode(trusted.public_key.trim())
        .context("decoding anime update public key")?;
    let public_bytes: [u8; 32] = public_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("anime update public key must be 32 bytes"))?;
    let verifying_key =
        VerifyingKey::from_bytes(&public_bytes).context("parsing anime update public key")?;
    let signature = decode_signature(&envelope.signature)?;
    let signed_bytes = signing_bytes(&envelope.signed)?;
    verifying_key
        .verify(&signed_bytes, &signature)
        .context("anime update envelope signature verification failed")?;

    let envelope_fingerprint =
        sha256_prefixed(&serde_json::to_vec(&envelope).context("encoding anime update envelope")?);
    let approval = QualifiedAnimeBundleApproval {
        bundle_version: envelope.signed.manifest.bundle_version.clone(),
        manifest_fingerprint: normalize_sha256(&envelope.signed.manifest_fingerprint),
        model_sha256: normalize_sha256(&envelope.signed.model_sha256),
        qualification_report_fingerprint: normalize_sha256(
            &envelope.signed.qualification_report_fingerprint,
        ),
        certified_runtime_profiles: envelope.signed.certified_runtime_profiles.clone(),
    };
    Ok(VerifiedAnimeBundleEnvelope {
        envelope,
        envelope_fingerprint,
        approval,
    })
}

pub fn ensure_monotonic_anime_update(
    accepted: Option<&VerifiedAnimeBundleEnvelope>,
    incoming: &VerifiedAnimeBundleEnvelope,
) -> Result<()> {
    let Some(accepted) = accepted else {
        return Ok(());
    };
    ensure!(
        incoming.sequence() >= accepted.sequence(),
        "anime update channel downgrade rejected"
    );
    if incoming.sequence() == accepted.sequence() {
        ensure!(
            incoming.envelope_fingerprint == accepted.envelope_fingerprint,
            "anime update sequence was reused with different content"
        );
    }
    Ok(())
}

pub fn load_accepted_anime_update(
    inference_root: &Path,
    now: DateTime<Utc>,
) -> Result<Option<VerifiedAnimeBundleEnvelope>> {
    let path = inference_root.join(UPDATE_CHANNEL_STATE_FILE);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("reading '{}'", path.display())),
    };
    let accepted: AcceptedAnimeUpdateChannel =
        serde_json::from_slice(&bytes).context("decoding accepted anime update channel")?;
    ensure!(
        accepted.schema_version == ANIME_UPDATE_CHANNEL_SCHEMA_VERSION,
        "unsupported accepted anime update channel schema"
    );
    DateTime::parse_from_rfc3339(&accepted.accepted_at)
        .context("accepted anime update timestamp is not RFC3339")?;
    let verified = verify_anime_bundle_envelope(accepted.envelope, now, false)?;
    ensure!(
        sha256_eq(
            &accepted.envelope_fingerprint,
            &verified.envelope_fingerprint,
        ),
        "accepted anime update fingerprint changed"
    );
    Ok(Some(verified))
}

pub fn commit_accepted_anime_update(
    inference_root: &Path,
    verified: &VerifiedAnimeBundleEnvelope,
    now: DateTime<Utc>,
) -> Result<()> {
    fs::create_dir_all(inference_root)
        .with_context(|| format!("creating inference root '{}'", inference_root.display()))?;
    let accepted = AcceptedAnimeUpdateChannel {
        schema_version: ANIME_UPDATE_CHANNEL_SCHEMA_VERSION,
        envelope_fingerprint: verified.envelope_fingerprint.clone(),
        accepted_at: now.to_rfc3339(),
        envelope: verified.envelope.clone(),
    };
    let bytes =
        serde_json::to_vec_pretty(&accepted).context("encoding accepted anime update channel")?;
    let path = inference_root.join(UPDATE_CHANNEL_STATE_FILE);
    write_atomic(&path, &bytes)
}

pub fn signing_bytes(payload: &AnimeBundleChannelPayload) -> Result<Vec<u8>> {
    let value = serde_json::to_value(payload).context("encoding anime update signing payload")?;
    let mut encoded = Vec::with_capacity(64 * 1024);
    encoded.extend_from_slice(SIGNATURE_DOMAIN);
    write_canonical_json(&value, &mut encoded)?;
    Ok(encoded)
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(text) => output.extend_from_slice(
            serde_json::to_string(text)
                .context("encoding canonical JSON string")?
                .as_bytes(),
        ),
        Value::Array(values) => {
            output.push(b'[');
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(item, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                output.extend_from_slice(
                    serde_json::to_string(key)
                        .context("encoding canonical JSON key")?
                        .as_bytes(),
                );
                output.push(b':');
                write_canonical_json(&values[key], output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn decode_signature(value: &str) -> Result<Signature> {
    let encoded = value
        .trim()
        .strip_prefix("ed25519:")
        .unwrap_or(value.trim());
    let bytes = general_purpose::STANDARD
        .decode(encoded)
        .context("decoding anime update signature")?;
    let bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("anime update signature must be 64 bytes"))?;
    Ok(Signature::from_bytes(&bytes))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("anime update state path has no parent")?;
    let temporary = parent.join(format!(
        ".{}.{}.partial",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("update"),
        Uuid::new_v4()
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("creating '{}'", temporary.display()))?;
        file.write_all(bytes)
            .context("writing anime update state")?;
        file.flush().context("flushing anime update state")?;
        file.sync_all().context("syncing anime update state")?;
        fs::rename(&temporary, path).with_context(|| format!("committing '{}'", path.display()))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .context("syncing anime update state directory")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn validate_sha256(value: &str, label: &str) -> Result<()> {
    let digest = value.strip_prefix("sha256:").unwrap_or(value);
    ensure!(
        digest.len() == 64
            && digest
                .chars()
                .all(|character| character.is_ascii_hexdigit()),
        "{label} must be SHA-256"
    );
    Ok(())
}

fn normalize_sha256(value: &str) -> String {
    format!(
        "sha256:{}",
        value
            .strip_prefix("sha256:")
            .unwrap_or(value)
            .to_ascii_lowercase()
    )
}

fn sha256_eq(left: &str, right: &str) -> bool {
    normalize_sha256(left) == normalize_sha256(right)
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    // RFC 8032 test key. Production verification uses only the compiled
    // first-party public key; this helper exercises canonical signing logic.
    const TEST_SECRET: [u8; 32] = [7; 32];

    fn fixture_manifest() -> AnimeInferenceBundleManifest {
        serde_json::from_str(include_str!("fixtures/update-channel-manifest.json"))
            .expect("fixture manifest")
    }

    fn test_envelope(sequence: u64) -> SignedAnimeBundleEnvelope {
        let manifest = fixture_manifest();
        let manifest_fingerprint = sha256_prefixed(&serde_json::to_vec(&manifest).unwrap());
        let payload = AnimeBundleChannelPayload {
            channel: ANIME_UPDATE_CHANNEL_NAME.to_string(),
            sequence,
            issued_at: "2026-08-08T00:00:00Z".to_string(),
            expires_at: "2027-08-08T00:00:00Z".to_string(),
            manifest_fingerprint,
            bundle_closure_fingerprint:
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
            model_sha256: manifest.model.sha256.clone(),
            qualification_report_fingerprint: manifest
                .model
                .qualification_report_fingerprint
                .clone(),
            certified_runtime_profiles: Vec::new(),
            manifest,
        };
        let signature =
            SigningKey::from_bytes(&TEST_SECRET).sign(&signing_bytes(&payload).unwrap());
        SignedAnimeBundleEnvelope {
            schema_version: ANIME_UPDATE_CHANNEL_SCHEMA_VERSION,
            key_id: "test-only".to_string(),
            signed: payload,
            signature: general_purpose::STANDARD.encode(signature.to_bytes()),
        }
    }

    fn resign(envelope: &mut SignedAnimeBundleEnvelope) {
        let signature = SigningKey::from_bytes(&TEST_SECRET)
            .sign(&signing_bytes(&envelope.signed).expect("signing bytes"));
        envelope.signature = general_purpose::STANDARD.encode(signature.to_bytes());
    }

    fn test_keys() -> Vec<TrustedAnimeUpdateKey> {
        vec![TrustedAnimeUpdateKey {
            key_id: "test-only".to_string(),
            public_key: general_purpose::STANDARD.encode(
                SigningKey::from_bytes(&TEST_SECRET)
                    .verifying_key()
                    .to_bytes(),
            ),
        }]
    }

    #[test]
    fn alm9_valid_signature_and_all_content_bindings_are_required() {
        let now = DateTime::parse_from_rfc3339("2026-08-08T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let verified =
            verify_anime_bundle_envelope_with_keys(test_envelope(7), now, true, &test_keys())
                .unwrap();
        assert_eq!(verified.sequence(), 7);
        assert_eq!(verified.key_id(), "test-only");
        assert_eq!(
            verified.bundle_closure_fingerprint(),
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        let mut tampered = test_envelope(7);
        tampered.signed.manifest.model.sha256 =
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_string();
        assert!(verify_anime_bundle_envelope_with_keys(tampered, now, true, &test_keys()).is_err());

        let mut bad_signature = test_envelope(7);
        bad_signature.signature = general_purpose::STANDARD.encode([0_u8; 64]);
        assert!(
            verify_anime_bundle_envelope_with_keys(bad_signature, now, true, &test_keys()).is_err()
        );

        let mut legacy = test_envelope(7);
        legacy.schema_version = 1;
        assert!(verify_anime_bundle_envelope_with_keys(legacy, now, true, &test_keys()).is_err());

        let mut detached_closure = test_envelope(7);
        detached_closure.signed.bundle_closure_fingerprint =
            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string();
        assert!(
            verify_anime_bundle_envelope_with_keys(detached_closure, now, true, &test_keys(),)
                .is_err()
        );
    }

    #[test]
    fn alm9_certified_profile_signed_channel_binding() {
        let now = DateTime::parse_from_rfc3339("2026-08-08T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut envelope = test_envelope(8);
        let runtime = &envelope.signed.manifest.runtimes[0];
        envelope.signed.certified_runtime_profiles = vec![QualifiedAnimeRuntimeProfileApproval {
            host_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            runtime_artifact_key: runtime.artifact_key(),
            runtime_artifact_sha256: runtime.sha256.clone(),
            execution_backend: super::super::AnimeExecutionBackend::Cpu,
            certified_profile_fingerprint:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            certification_report_fingerprint:
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
        }];
        resign(&mut envelope);
        let verified =
            verify_anime_bundle_envelope_with_keys(envelope.clone(), now, true, &test_keys())
                .expect("signed certified profile");
        assert_eq!(
            verified.approval.certified_runtime_profiles,
            envelope.signed.certified_runtime_profiles
        );

        envelope.signed.certified_runtime_profiles[0].host_fingerprint =
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_string();
        assert!(
            verify_anime_bundle_envelope_with_keys(envelope, now, true, &test_keys()).is_err(),
            "changing a certified host after signing must invalidate the channel"
        );
    }

    #[test]
    fn alm9_expired_channel_is_rejected_for_updates_but_valid_for_cached_availability() {
        let now = DateTime::parse_from_rfc3339("2028-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let envelope = test_envelope(1);
        assert!(
            verify_anime_bundle_envelope_with_keys(envelope.clone(), now, true, &test_keys(),)
                .is_err()
        );
        verify_anime_bundle_envelope_with_keys(envelope, now, false, &test_keys()).unwrap();
    }

    #[test]
    fn alm9_canonical_signing_bytes_ignore_object_insertion_order() {
        let payload = test_envelope(1).signed;
        let first = signing_bytes(&payload).unwrap();
        let reparsed: AnimeBundleChannelPayload =
            serde_json::from_value(serde_json::to_value(&payload).unwrap()).unwrap();
        assert_eq!(first, signing_bytes(&reparsed).unwrap());
        assert!(first.starts_with(SIGNATURE_DOMAIN));
    }

    #[test]
    fn alm9_monotonic_sequence_rejects_downgrade_and_equivocation() {
        let accepted_envelope = test_envelope(2);
        let accepted = VerifiedAnimeBundleEnvelope {
            envelope_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            approval: QualifiedAnimeBundleApproval {
                bundle_version: accepted_envelope.signed.manifest.bundle_version.clone(),
                manifest_fingerprint: accepted_envelope.signed.manifest_fingerprint.clone(),
                model_sha256: accepted_envelope.signed.model_sha256.clone(),
                qualification_report_fingerprint: accepted_envelope
                    .signed
                    .qualification_report_fingerprint
                    .clone(),
                certified_runtime_profiles: accepted_envelope
                    .signed
                    .certified_runtime_profiles
                    .clone(),
            },
            envelope: accepted_envelope,
        };
        let mut downgrade = accepted.clone();
        downgrade.envelope.signed.sequence = 1;
        assert!(ensure_monotonic_anime_update(Some(&accepted), &downgrade).is_err());
        let mut equivocation = accepted.clone();
        equivocation.envelope_fingerprint =
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
        assert!(ensure_monotonic_anime_update(Some(&accepted), &equivocation).is_err());
        let mut advance = accepted.clone();
        advance.envelope.signed.sequence = 3;
        advance.envelope_fingerprint =
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string();
        ensure_monotonic_anime_update(Some(&accepted), &advance).unwrap();
    }

    #[test]
    fn alm9_strict_envelope_rejects_unknown_fields() {
        let mut value = serde_json::to_value(test_envelope(1)).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_string(), Value::Bool(true));
        assert!(serde_json::from_value::<SignedAnimeBundleEnvelope>(value).is_err());
    }
}
