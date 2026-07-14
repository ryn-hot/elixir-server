use std::{net::IpAddr, time::Duration};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

const CONTROL_VERSION: &str = "elixir-live-egress-v1";
const MAX_CLOCK_SKEW_SECONDS: i64 = 30;

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub(crate) struct ControlKeys {
    auth: [u8; 32],
    encryption: [u8; 32],
}

impl std::fmt::Debug for ControlKeys {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ControlKeys([REDACTED])")
    }
}

impl ControlKeys {
    pub(crate) fn generate() -> Self {
        let mut auth = [0_u8; 32];
        let mut encryption = [0_u8; 32];
        OsRng.fill_bytes(&mut auth);
        OsRng.fill_bytes(&mut encryption);
        Self { auth, encryption }
    }

    fn from_encoded(auth: &str, encryption: &str) -> Result<Self, ControlProtocolError> {
        Ok(Self {
            auth: decode_key(auth)?,
            encryption: decode_key(encryption)?,
        })
    }

    fn encoded_auth(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.auth)
    }

    fn encoded_encryption(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.encryption)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ControlSecretDocument {
    version: String,
    pub(crate) session_id: Uuid,
    pub(crate) control_fencing_token: i64,
    pub(crate) expires_at: DateTime<Utc>,
    auth_key: String,
    encryption_key: String,
    pub(crate) readiness: WorkerReadinessConfig,
}

impl ControlSecretDocument {
    pub(crate) fn new(
        session_id: Uuid,
        control_fencing_token: i64,
        expires_at: DateTime<Utc>,
        keys: &ControlKeys,
        readiness: WorkerReadinessConfig,
    ) -> Result<Self, ControlProtocolError> {
        if control_fencing_token < 1 || expires_at <= Utc::now() {
            return Err(ControlProtocolError::Invalid);
        }
        readiness.validate()?;
        Ok(Self {
            version: CONTROL_VERSION.to_string(),
            session_id,
            control_fencing_token,
            expires_at,
            auth_key: keys.encoded_auth(),
            encryption_key: keys.encoded_encryption(),
            readiness,
        })
    }

    pub(crate) fn keys(&self) -> Result<ControlKeys, ControlProtocolError> {
        if self.version != CONTROL_VERSION
            || self.control_fencing_token < 1
            || self.expires_at <= Utc::now()
        {
            return Err(ControlProtocolError::Expired);
        }
        self.readiness.validate()?;
        ControlKeys::from_encoded(&self.auth_key, &self.encryption_key)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkerReadinessConfig {
    pub(crate) external_ip_url: String,
    pub(crate) dns_probe_host: String,
    pub(crate) expected_egress_ips: Vec<IpAddr>,
}

impl WorkerReadinessConfig {
    fn validate(&self) -> Result<(), ControlProtocolError> {
        let url = reqwest::Url::parse(&self.external_ip_url)
            .map_err(|_| ControlProtocolError::Invalid)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || self.dns_probe_host.is_empty()
            || self.dns_probe_host.len() > 253
            || self.expected_egress_ips.is_empty()
            || self.expected_egress_ips.len() > 16
        {
            return Err(ControlProtocolError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ControlEnvelope {
    version: String,
    pub(crate) request_id: Uuid,
    pub(crate) issued_at: DateTime<Utc>,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ResolveControlRequest {
    pub(crate) host: String,
    pub(crate) port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ResolveControlResponse {
    pub(crate) addresses: Vec<IpAddr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FetchControlRequest {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) socket_addresses: Vec<String>,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) connect_timeout_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReadinessControlResponse {
    pub(crate) route: bool,
    pub(crate) dns: bool,
    pub(crate) external_ip: bool,
    pub(crate) kill_switch: bool,
    pub(crate) health: bool,
    pub(crate) observed_egress_ip: Option<IpAddr>,
}

impl ReadinessControlResponse {
    pub(crate) fn ready(&self) -> bool {
        self.route && self.dns && self.external_ip && self.kill_switch && self.health
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlProtocolError {
    #[error("invalid protected-egress control message")]
    Invalid,
    #[error("protected-egress control message is unauthenticated")]
    Unauthenticated,
    #[error("protected-egress control message expired")]
    Expired,
    #[error("protected-egress control message was replayed")]
    Replay,
}

pub(crate) fn seal_control_request<T: Serialize>(
    keys: &ControlKeys,
    value: &T,
    now: DateTime<Utc>,
) -> Result<Vec<u8>, ControlProtocolError> {
    let request_id = Uuid::new_v4();
    let mut nonce = [0_u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let plaintext = serde_json::to_vec(value).map_err(|_| ControlProtocolError::Invalid)?;
    let aad = aad(request_id, now);
    let cipher =
        Aes256Gcm::new_from_slice(&keys.encryption).map_err(|_| ControlProtocolError::Invalid)?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| ControlProtocolError::Invalid)?;
    serde_json::to_vec(&ControlEnvelope {
        version: CONTROL_VERSION.to_string(),
        request_id,
        issued_at: now,
        nonce: URL_SAFE_NO_PAD.encode(nonce),
        ciphertext: URL_SAFE_NO_PAD.encode(ciphertext),
    })
    .map_err(|_| ControlProtocolError::Invalid)
}

pub(crate) fn control_request_id(body: &[u8]) -> Result<Uuid, ControlProtocolError> {
    let envelope: ControlEnvelope =
        serde_json::from_slice(body).map_err(|_| ControlProtocolError::Invalid)?;
    if envelope.version != CONTROL_VERSION || envelope.request_id.is_nil() {
        return Err(ControlProtocolError::Invalid);
    }
    Ok(envelope.request_id)
}

pub(crate) fn open_control_request<T: DeserializeOwned>(
    keys: &ControlKeys,
    body: &[u8],
    now: DateTime<Utc>,
) -> Result<(Uuid, T), ControlProtocolError> {
    let envelope: ControlEnvelope =
        serde_json::from_slice(body).map_err(|_| ControlProtocolError::Invalid)?;
    if envelope.version != CONTROL_VERSION
        || (now - envelope.issued_at).num_seconds().abs() > MAX_CLOCK_SKEW_SECONDS
    {
        return Err(ControlProtocolError::Expired);
    }
    let nonce = URL_SAFE_NO_PAD
        .decode(envelope.nonce)
        .map_err(|_| ControlProtocolError::Invalid)?;
    if nonce.len() != 12 {
        return Err(ControlProtocolError::Invalid);
    }
    let ciphertext = URL_SAFE_NO_PAD
        .decode(envelope.ciphertext)
        .map_err(|_| ControlProtocolError::Invalid)?;
    let cipher =
        Aes256Gcm::new_from_slice(&keys.encryption).map_err(|_| ControlProtocolError::Invalid)?;
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad: aad(envelope.request_id, envelope.issued_at).as_bytes(),
            },
        )
        .map_err(|_| ControlProtocolError::Unauthenticated)?;
    let value = serde_json::from_slice(&plaintext).map_err(|_| ControlProtocolError::Invalid)?;
    Ok((envelope.request_id, value))
}

pub(crate) fn request_signature(
    keys: &ControlKeys,
    body: &[u8],
) -> Result<String, ControlProtocolError> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&keys.auth)
        .map_err(|_| ControlProtocolError::Invalid)?;
    mac.update(CONTROL_VERSION.as_bytes());
    mac.update(body);
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

pub(crate) fn verify_request_signature(
    keys: &ControlKeys,
    body: &[u8],
    signature: &str,
) -> Result<(), ControlProtocolError> {
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| ControlProtocolError::Unauthenticated)?;
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&keys.auth)
        .map_err(|_| ControlProtocolError::Invalid)?;
    mac.update(CONTROL_VERSION.as_bytes());
    mac.update(body);
    mac.verify_slice(&signature)
        .map_err(|_| ControlProtocolError::Unauthenticated)
}

pub(crate) fn response_signature(
    keys: &ControlKeys,
    request_id: Uuid,
    operation: &str,
    status: u16,
    peer: &str,
    control_fencing_token: i64,
    body: Option<&[u8]>,
) -> Result<String, ControlProtocolError> {
    let body_hash = body.map(blake3::hash);
    let body_hash_bytes: &[u8] = body_hash
        .as_ref()
        .map(|hash| hash.as_bytes().as_slice())
        .unwrap_or(&[]);
    let status = status.to_string();
    let fence = control_fencing_token.to_string();
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&keys.auth)
        .map_err(|_| ControlProtocolError::Invalid)?;
    for value in [
        CONTROL_VERSION.as_bytes(),
        request_id.as_bytes(),
        operation.as_bytes(),
        status.as_bytes(),
        peer.as_bytes(),
        fence.as_bytes(),
        body_hash_bytes,
    ] {
        mac.update(value);
        mac.update(&[0]);
    }
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

pub(crate) fn verify_response_signature(
    keys: &ControlKeys,
    signature: &str,
    request_id: Uuid,
    operation: &str,
    status: u16,
    peer: &str,
    control_fencing_token: i64,
    body: Option<&[u8]>,
) -> Result<(), ControlProtocolError> {
    let expected = response_signature(
        keys,
        request_id,
        operation,
        status,
        peer,
        control_fencing_token,
        body,
    )?;
    let expected = URL_SAFE_NO_PAD
        .decode(expected)
        .map_err(|_| ControlProtocolError::Invalid)?;
    let actual = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| ControlProtocolError::Unauthenticated)?;
    if expected.len() != actual.len() {
        return Err(ControlProtocolError::Unauthenticated);
    }
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&keys.auth)
        .map_err(|_| ControlProtocolError::Invalid)?;
    mac.update(b"response-constant-time-compare");
    let mut expected_mac = <Hmac<Sha256> as Mac>::new_from_slice(&keys.auth)
        .map_err(|_| ControlProtocolError::Invalid)?;
    expected_mac.update(b"response-constant-time-compare");
    expected_mac.update(&expected);
    mac.update(&actual);
    mac.verify_slice(&expected_mac.finalize().into_bytes())
        .map_err(|_| ControlProtocolError::Unauthenticated)
}

pub(crate) fn bounded_connect_timeout(milliseconds: u64) -> Result<Duration, ControlProtocolError> {
    let duration = Duration::from_millis(milliseconds);
    if duration.is_zero() || duration > Duration::from_secs(30) {
        return Err(ControlProtocolError::Invalid);
    }
    Ok(duration)
}

fn aad(request_id: Uuid, issued_at: DateTime<Utc>) -> String {
    format!(
        "{CONTROL_VERSION}:{request_id}:{}",
        issued_at.timestamp_millis()
    )
}

fn decode_key(value: &str) -> Result<[u8; 32], ControlProtocolError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| ControlProtocolError::Invalid)?;
    bytes.try_into().map_err(|_| ControlProtocolError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn n11_control_envelope_is_random_authenticated_bounded_and_redacted() {
        let keys = ControlKeys::generate();
        let now = Utc::now();
        let request = ResolveControlRequest {
            host: "origin.example".to_string(),
            port: 443,
        };
        let first = seal_control_request(&keys, &request, now).unwrap();
        let second = seal_control_request(&keys, &request, now).unwrap();
        assert_ne!(first, second);
        assert!(!String::from_utf8_lossy(&first).contains("origin.example"));
        let signature = request_signature(&keys, &first).unwrap();
        verify_request_signature(&keys, &first, &signature).unwrap();
        let (request_id, opened): (Uuid, ResolveControlRequest) =
            open_control_request(&keys, &first, now).unwrap();
        assert_ne!(request_id, Uuid::nil());
        assert_eq!(opened.host, "origin.example");
        let mut tampered = first;
        *tampered.last_mut().unwrap() ^= 1;
        assert!(verify_request_signature(&keys, &tampered, &signature).is_err());
        assert_eq!(format!("{keys:?}"), "ControlKeys([REDACTED])");
    }
}
