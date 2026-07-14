//! Versioned, domain-separated cryptography for the Live subsystem.

use std::{collections::BTreeMap, error::Error, fmt, sync::RwLock};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use base64::{Engine as _, engine::general_purpose};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use zeroize::Zeroizing;

const ENVELOPE_PREFIX: &str = "elx-live";
const ENVELOPE_VERSION: &str = "v1";
const ENVELOPE_SALT: &[u8] = b"elixir.live.envelope.v1";
const ENVELOPE_AAD_DOMAIN: &str = "elixir.live.envelope";
const DELIVERY_TOKEN_PREFIX: &str = "elx_live_v1_";
const TOKEN_HASH_PREFIX: &str = "elx-live-token-hash";
const TOKEN_HASH_VERSION: &str = "v1";
const TOKEN_HASH_SALT: &[u8] = b"elixir.live.token-hash.v1";
const TOKEN_HASH_INFO: &str = "delivery-token";
const CORRELATION_HASH_PREFIX: &str = "elx-live-correlation-hash";
const CORRELATION_HASH_VERSION: &str = "v1";
const CORRELATION_HASH_INFO: &str = "correlation-hash";
const AES_GCM_NONCE_BYTES: usize = 12;
const AES_GCM_TAG_BYTES: usize = 16;
const DELIVERY_TOKEN_BYTES: usize = 32;
const SHA256_BYTES: usize = 32;
const MAX_KEY_ID_BYTES: usize = 32;
const MAX_CONTEXT_FIELD_BYTES: usize = 512;
const MAX_PLAINTEXT_BYTES: usize = 1_048_576;
const MAX_ENVELOPE_TEXT_BYTES: usize = 1_500_000;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LiveCryptoError {
    InvalidConfiguration(&'static str),
    EncryptionFailed,
    DecryptionFailed,
    InvalidDeliveryToken,
}

impl fmt::Debug for LiveCryptoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for LiveCryptoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(
                    formatter,
                    "invalid Live cryptography configuration: {message}"
                )
            }
            Self::EncryptionFailed => formatter.write_str("Live encryption failed"),
            Self::DecryptionFailed => {
                formatter.write_str("Live encrypted value could not be processed")
            }
            Self::InvalidDeliveryToken => formatter.write_str("invalid Live delivery token"),
        }
    }
}

impl Error for LiveCryptoError {}

pub fn validate_live_key_id(value: &str) -> Result<(), LiveCryptoError> {
    validate_key_id(value).map_err(LiveCryptoError::InvalidConfiguration)
}

pub struct SecretBytes(Zeroizing<Vec<u8>>);

impl SecretBytes {
    pub fn new(value: Vec<u8>) -> Self {
        Self(Zeroizing::new(value))
    }

    pub fn from_utf8(value: String) -> Self {
        Self::new(value.into_bytes())
    }

    pub fn expose_secret(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretBytes([REDACTED])")
    }
}

impl fmt::Display for SecretBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

pub struct LiveMasterKey {
    key_id: String,
    material: Zeroizing<[u8; 32]>,
}

impl LiveMasterKey {
    pub fn new(key_id: impl Into<String>, material: [u8; 32]) -> Result<Self, LiveCryptoError> {
        let key_id = key_id.into();
        let material = Zeroizing::new(material);
        validate_key_id(&key_id).map_err(LiveCryptoError::InvalidConfiguration)?;
        Ok(Self { key_id, material })
    }

    pub fn from_base64(key_id: impl Into<String>, encoded: &str) -> Result<Self, LiveCryptoError> {
        let decoded = general_purpose::STANDARD
            .decode(encoded.trim())
            .map_err(|_| LiveCryptoError::InvalidConfiguration("master key is not base64"))?;
        let decoded = Zeroizing::new(decoded);
        if decoded.len() != 32 {
            return Err(LiveCryptoError::InvalidConfiguration(
                "master key must contain exactly 32 bytes",
            ));
        }
        let mut material = [0u8; 32];
        material.copy_from_slice(decoded.as_slice());
        Self::new(key_id, material)
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    fn duplicate(&self) -> Self {
        Self {
            key_id: self.key_id.clone(),
            material: Zeroizing::new(*self.material),
        }
    }
}

impl fmt::Debug for LiveMasterKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveMasterKey")
            .field("key_id", &self.key_id)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EnvelopePurpose {
    Descriptor,
    ItemSnapshot,
    IdempotencyResponse,
    WorkerDescriptor,
    SealedApiKey,
    CatalogItemKey,
    CatalogCursor,
    StreamOptionKey,
    SourceKey,
    ArtworkKey,
}

impl EnvelopePurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Descriptor => "descriptor",
            Self::ItemSnapshot => "item-snapshot",
            Self::IdempotencyResponse => "idempotency-response",
            Self::WorkerDescriptor => "worker-descriptor",
            Self::SealedApiKey => "sealed-api-key",
            Self::CatalogItemKey => "catalog-item-key",
            Self::CatalogCursor => "catalog-cursor",
            Self::StreamOptionKey => "stream-option-key",
            Self::SourceKey => "source-key",
            Self::ArtworkKey => "artwork-key",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EnvelopeContext<'a> {
    purpose: EnvelopePurpose,
    table: &'a str,
    record_id: &'a str,
    column: &'a str,
}

impl<'a> EnvelopeContext<'a> {
    pub fn new(
        purpose: EnvelopePurpose,
        table: &'a str,
        record_id: &'a str,
        column: &'a str,
    ) -> Result<Self, LiveCryptoError> {
        validate_context_field(table, "table")?;
        validate_context_field(record_id, "record id")?;
        validate_context_field(column, "column")?;
        Ok(Self {
            purpose,
            table,
            record_id,
            column,
        })
    }

    pub const fn purpose(self) -> EnvelopePurpose {
        self.purpose
    }
}

pub struct LiveDeliveryToken(Zeroizing<String>);

impl LiveDeliveryToken {
    pub fn generate() -> Result<Self, LiveCryptoError> {
        let mut random = Zeroizing::new([0u8; DELIVERY_TOKEN_BYTES]);
        OsRng
            .try_fill_bytes(random.as_mut())
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        let encoded = general_purpose::URL_SAFE_NO_PAD.encode(random.as_ref());
        Ok(Self(Zeroizing::new(format!(
            "{DELIVERY_TOKEN_PREFIX}{encoded}"
        ))))
    }

    pub fn parse(value: String) -> Result<Self, LiveCryptoError> {
        let value = Zeroizing::new(value);
        validate_delivery_token(value.as_str())?;
        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for LiveDeliveryToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LiveDeliveryToken([REDACTED])")
    }
}

impl fmt::Display for LiveDeliveryToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED_LIVE_TOKEN]")
    }
}

pub struct LiveTokenHash {
    encoded: String,
    key_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelationHashPurpose {
    ItemKey,
    StreamOptionKey,
    IdempotencyKey,
    IdempotencyRequest,
}

impl CorrelationHashPurpose {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ItemKey => "item-key",
            Self::StreamOptionKey => "stream-option-key",
            Self::IdempotencyKey => "idempotency-key",
            Self::IdempotencyRequest => "idempotency-request",
        }
    }
}

impl LiveTokenHash {
    pub fn as_str(&self) -> &str {
        &self.encoded
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

impl fmt::Debug for LiveTokenHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveTokenHash")
            .field("key_id", &self.key_id)
            .field("digest", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for LiveTokenHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED_LIVE_TOKEN_HASH]")
    }
}

pub struct LiveCrypto {
    primaries: RwLock<LiveCryptoPrimaries>,
    envelope_keys: BTreeMap<String, LiveMasterKey>,
    token_hash_keys: BTreeMap<String, LiveMasterKey>,
}

struct LiveCryptoPrimaries {
    envelope_key_id: String,
    token_hash_key_id: String,
}

impl LiveCrypto {
    pub fn new(
        primary_key_id: impl Into<String>,
        keys: impl IntoIterator<Item = LiveMasterKey>,
    ) -> Result<Self, LiveCryptoError> {
        let primary_key_id = primary_key_id.into();
        Self::new_with_primaries(primary_key_id.clone(), primary_key_id, keys)
    }

    pub fn new_with_primaries(
        envelope_primary_key_id: impl Into<String>,
        token_hash_primary_key_id: impl Into<String>,
        keys: impl IntoIterator<Item = LiveMasterKey>,
    ) -> Result<Self, LiveCryptoError> {
        let keys = keys.into_iter().collect::<Vec<_>>();
        let token_hash_keys = keys
            .iter()
            .map(LiveMasterKey::duplicate)
            .collect::<Vec<_>>();
        Self::new_with_domain_keys(
            envelope_primary_key_id,
            keys,
            token_hash_primary_key_id,
            token_hash_keys,
        )
    }

    pub fn new_with_domain_keys(
        envelope_primary_key_id: impl Into<String>,
        envelope_keys: impl IntoIterator<Item = LiveMasterKey>,
        token_hash_primary_key_id: impl Into<String>,
        token_hash_keys: impl IntoIterator<Item = LiveMasterKey>,
    ) -> Result<Self, LiveCryptoError> {
        let envelope_primary_key_id = envelope_primary_key_id.into();
        let token_hash_primary_key_id = token_hash_primary_key_id.into();
        validate_key_id(&envelope_primary_key_id).map_err(LiveCryptoError::InvalidConfiguration)?;
        validate_key_id(&token_hash_primary_key_id)
            .map_err(LiveCryptoError::InvalidConfiguration)?;
        let envelope_keys = index_keys(envelope_keys)?;
        let token_hash_keys = index_keys(token_hash_keys)?;
        if !envelope_keys.contains_key(&envelope_primary_key_id)
            || !token_hash_keys.contains_key(&token_hash_primary_key_id)
        {
            return Err(LiveCryptoError::InvalidConfiguration(
                "primary key ID is not configured",
            ));
        }
        Ok(Self {
            primaries: RwLock::new(LiveCryptoPrimaries {
                envelope_key_id: envelope_primary_key_id,
                token_hash_key_id: token_hash_primary_key_id,
            }),
            envelope_keys,
            token_hash_keys,
        })
    }

    pub fn primary_key_id(&self) -> Result<String, LiveCryptoError> {
        self.primaries
            .read()
            .map(|primaries| primaries.envelope_key_id.clone())
            .map_err(|_| LiveCryptoError::EncryptionFailed)
    }

    pub fn token_hash_primary_key_id(&self) -> Result<String, LiveCryptoError> {
        self.primaries
            .read()
            .map(|primaries| primaries.token_hash_key_id.clone())
            .map_err(|_| LiveCryptoError::EncryptionFailed)
    }

    pub fn rotate_envelope_primary(&self, key_id: &str) -> Result<String, LiveCryptoError> {
        validate_key_id(key_id).map_err(LiveCryptoError::InvalidConfiguration)?;
        if !self.envelope_keys.contains_key(key_id) {
            return Err(LiveCryptoError::InvalidConfiguration(
                "envelope key ID is not configured",
            ));
        }
        let mut primaries = self
            .primaries
            .write()
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        Ok(std::mem::replace(
            &mut primaries.envelope_key_id,
            key_id.to_string(),
        ))
    }

    pub fn has_envelope_key(&self, key_id: &str) -> bool {
        validate_key_id(key_id).is_ok() && self.envelope_keys.contains_key(key_id)
    }

    pub fn rotate_token_hash_primary(&self, key_id: &str) -> Result<String, LiveCryptoError> {
        validate_key_id(key_id).map_err(LiveCryptoError::InvalidConfiguration)?;
        if !self.token_hash_keys.contains_key(key_id) {
            return Err(LiveCryptoError::InvalidConfiguration(
                "token-hash key ID is not configured",
            ));
        }
        let mut primaries = self
            .primaries
            .write()
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        Ok(std::mem::replace(
            &mut primaries.token_hash_key_id,
            key_id.to_string(),
        ))
    }

    pub fn has_token_hash_key(&self, key_id: &str) -> bool {
        validate_key_id(key_id).is_ok() && self.token_hash_keys.contains_key(key_id)
    }

    pub fn encrypt(
        &self,
        context: EnvelopeContext<'_>,
        plaintext: &SecretBytes,
    ) -> Result<String, LiveCryptoError> {
        if plaintext.len() > MAX_PLAINTEXT_BYTES {
            return Err(LiveCryptoError::EncryptionFailed);
        }
        let primary_key_id = self.primary_key_id()?;
        let master_key = self
            .envelope_key_material(&primary_key_id)
            .ok_or(LiveCryptoError::EncryptionFailed)?;
        let key = derive_envelope_key(master_key, &primary_key_id, context.purpose)
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        let cipher = <Aes256Gcm as KeyInit>::new_from_slice(key.as_ref())
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        let mut nonce = [0u8; AES_GCM_NONCE_BYTES];
        OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        let aad = envelope_aad(context, &primary_key_id)
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext.expose_secret(),
                    aad: &aad,
                },
            )
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        Ok(format!(
            "{ENVELOPE_PREFIX}:{ENVELOPE_VERSION}:{}:{}:{}",
            primary_key_id,
            general_purpose::URL_SAFE_NO_PAD.encode(nonce),
            general_purpose::URL_SAFE_NO_PAD.encode(ciphertext)
        ))
    }

    pub fn decrypt(
        &self,
        context: EnvelopeContext<'_>,
        envelope: &str,
    ) -> Result<SecretBytes, LiveCryptoError> {
        let parsed = parse_envelope(envelope)?;
        let master_key = self
            .envelope_key_material(parsed.key_id)
            .ok_or(LiveCryptoError::DecryptionFailed)?;
        let key = derive_envelope_key(master_key, parsed.key_id, context.purpose)
            .map_err(|_| LiveCryptoError::DecryptionFailed)?;
        let cipher = <Aes256Gcm as KeyInit>::new_from_slice(key.as_ref())
            .map_err(|_| LiveCryptoError::DecryptionFailed)?;
        let aad =
            envelope_aad(context, parsed.key_id).map_err(|_| LiveCryptoError::DecryptionFailed)?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&parsed.nonce),
                Payload {
                    msg: &parsed.ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| LiveCryptoError::DecryptionFailed)?;
        let plaintext = SecretBytes::new(plaintext);
        if plaintext.len() > MAX_PLAINTEXT_BYTES {
            return Err(LiveCryptoError::DecryptionFailed);
        }
        Ok(plaintext)
    }

    pub fn envelope_key_id<'a>(&self, envelope: &'a str) -> Result<&'a str, LiveCryptoError> {
        Ok(parse_envelope(envelope)?.key_id)
    }

    pub fn needs_reencryption(&self, envelope: &str) -> Result<bool, LiveCryptoError> {
        Ok(self.envelope_key_id(envelope)? != self.primary_key_id()?)
    }

    pub fn reencrypt(
        &self,
        context: EnvelopeContext<'_>,
        envelope: &str,
    ) -> Result<String, LiveCryptoError> {
        if !self.needs_reencryption(envelope)? {
            return Ok(envelope.to_string());
        }
        let plaintext = self.decrypt(context, envelope)?;
        self.encrypt(context, &plaintext)
    }

    pub fn hash_delivery_token(
        &self,
        token: &LiveDeliveryToken,
    ) -> Result<LiveTokenHash, LiveCryptoError> {
        let key_id = self.token_hash_primary_key_id()?;
        let master_key = self
            .token_hash_key_material(&key_id)
            .ok_or(LiveCryptoError::EncryptionFailed)?;
        let key = derive_token_hash_key(master_key, &key_id)
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        let mut mac = <HmacSha256 as Mac>::new_from_slice(key.as_ref())
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        mac.update(token.expose_secret().as_bytes());
        let digest = mac.finalize().into_bytes();
        let encoded_digest = general_purpose::URL_SAFE_NO_PAD.encode(digest);
        Ok(LiveTokenHash {
            encoded: format!("{TOKEN_HASH_PREFIX}:{TOKEN_HASH_VERSION}:{key_id}:{encoded_digest}"),
            key_id,
        })
    }

    pub fn verify_delivery_token(&self, presented: &str, stored_hash: &str) -> bool {
        if validate_delivery_token(presented).is_err() {
            return false;
        }
        let Some((key_id, expected_digest)) = parse_token_hash(stored_hash) else {
            return false;
        };
        let Some(master_key) = self.token_hash_key_material(key_id) else {
            return false;
        };
        let Ok(key) = derive_token_hash_key(master_key, key_id) else {
            return false;
        };
        let Ok(mut mac) = <HmacSha256 as Mac>::new_from_slice(key.as_ref()) else {
            return false;
        };
        mac.update(presented.as_bytes());
        mac.verify_slice(&expected_digest).is_ok()
    }

    pub fn hash_correlation(
        &self,
        purpose: CorrelationHashPurpose,
        value: &[u8],
    ) -> Result<String, LiveCryptoError> {
        if value.is_empty() || value.len() > 65_536 {
            return Err(LiveCryptoError::EncryptionFailed);
        }
        let key_id = self.token_hash_primary_key_id()?;
        let master_key = self
            .token_hash_key_material(&key_id)
            .ok_or(LiveCryptoError::EncryptionFailed)?;
        let key = derive_correlation_hash_key(master_key, &key_id, purpose)
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        let mut mac = <HmacSha256 as Mac>::new_from_slice(key.as_ref())
            .map_err(|_| LiveCryptoError::EncryptionFailed)?;
        mac.update(value);
        let digest = general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!(
            "{CORRELATION_HASH_PREFIX}:{CORRELATION_HASH_VERSION}:{key_id}:{digest}"
        ))
    }

    pub fn correlation_hash_key_id<'a>(&self, value: &'a str) -> Option<&'a str> {
        let mut parts = value.split(':');
        if parts.next() != Some(CORRELATION_HASH_PREFIX)
            || parts.next() != Some(CORRELATION_HASH_VERSION)
        {
            return None;
        }
        let key_id = parts.next()?;
        let digest = parts.next()?;
        if parts.next().is_some()
            || validate_key_id(key_id).is_err()
            || decode_canonical_base64url(digest).ok()?.len() != SHA256_BYTES
        {
            return None;
        }
        Some(key_id)
    }

    pub fn token_hash_key_id<'a>(&self, value: &'a str) -> Option<&'a str> {
        parse_token_hash(value).map(|(key_id, _)| key_id)
    }

    fn envelope_key_material(&self, key_id: &str) -> Option<&[u8; 32]> {
        self.envelope_keys.get(key_id).map(|key| &*key.material)
    }

    fn token_hash_key_material(&self, key_id: &str) -> Option<&[u8; 32]> {
        self.token_hash_keys.get(key_id).map(|key| &*key.material)
    }
}

fn index_keys(
    keys: impl IntoIterator<Item = LiveMasterKey>,
) -> Result<BTreeMap<String, LiveMasterKey>, LiveCryptoError> {
    let mut indexed = BTreeMap::new();
    for key in keys {
        let key_id = key.key_id.clone();
        if indexed.insert(key_id, key).is_some() {
            return Err(LiveCryptoError::InvalidConfiguration(
                "master key IDs must be unique within each key domain",
            ));
        }
    }
    Ok(indexed)
}

struct ParsedEnvelope<'a> {
    key_id: &'a str,
    nonce: [u8; AES_GCM_NONCE_BYTES],
    ciphertext: Vec<u8>,
}

fn parse_envelope(envelope: &str) -> Result<ParsedEnvelope<'_>, LiveCryptoError> {
    if envelope.len() > MAX_ENVELOPE_TEXT_BYTES || !envelope.is_ascii() {
        return Err(LiveCryptoError::DecryptionFailed);
    }
    let mut parts = envelope.split(':');
    if parts.next() != Some(ENVELOPE_PREFIX) || parts.next() != Some(ENVELOPE_VERSION) {
        return Err(LiveCryptoError::DecryptionFailed);
    }
    let key_id = parts.next().ok_or(LiveCryptoError::DecryptionFailed)?;
    let nonce_encoded = parts.next().ok_or(LiveCryptoError::DecryptionFailed)?;
    let ciphertext_encoded = parts.next().ok_or(LiveCryptoError::DecryptionFailed)?;
    if parts.next().is_some() || validate_key_id(key_id).is_err() {
        return Err(LiveCryptoError::DecryptionFailed);
    }
    let decoded_nonce = decode_canonical_base64url(nonce_encoded)?;
    if decoded_nonce.len() != AES_GCM_NONCE_BYTES {
        return Err(LiveCryptoError::DecryptionFailed);
    }
    let mut nonce = [0u8; AES_GCM_NONCE_BYTES];
    nonce.copy_from_slice(&decoded_nonce);
    let ciphertext = decode_canonical_base64url(ciphertext_encoded)?;
    if !(AES_GCM_TAG_BYTES..=MAX_PLAINTEXT_BYTES + AES_GCM_TAG_BYTES).contains(&ciphertext.len()) {
        return Err(LiveCryptoError::DecryptionFailed);
    }
    Ok(ParsedEnvelope {
        key_id,
        nonce,
        ciphertext,
    })
}

fn decode_canonical_base64url(value: &str) -> Result<Vec<u8>, LiveCryptoError> {
    if value.is_empty() || value.contains('=') {
        return Err(LiveCryptoError::DecryptionFailed);
    }
    let decoded = general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| LiveCryptoError::DecryptionFailed)?;
    if general_purpose::URL_SAFE_NO_PAD.encode(&decoded) != value {
        return Err(LiveCryptoError::DecryptionFailed);
    }
    Ok(decoded)
}

fn derive_envelope_key(
    master_key: &[u8; 32],
    key_id: &str,
    purpose: EnvelopePurpose,
) -> Result<Zeroizing<[u8; 32]>, LiveCryptoError> {
    let mut info = Vec::with_capacity(key_id.len() + purpose.as_str().len() + 8);
    append_length_prefixed(&mut info, key_id)?;
    append_length_prefixed(&mut info, purpose.as_str())?;
    let hkdf = Hkdf::<Sha256>::new(Some(ENVELOPE_SALT), master_key);
    let mut key = Zeroizing::new([0u8; 32]);
    hkdf.expand(&info, key.as_mut())
        .map_err(|_| LiveCryptoError::InvalidConfiguration("envelope key derivation failed"))?;
    Ok(key)
}

fn derive_token_hash_key(
    master_key: &[u8; 32],
    key_id: &str,
) -> Result<Zeroizing<[u8; 32]>, LiveCryptoError> {
    let mut info = Vec::with_capacity(key_id.len() + TOKEN_HASH_INFO.len() + 8);
    append_length_prefixed(&mut info, key_id)?;
    append_length_prefixed(&mut info, TOKEN_HASH_INFO)?;
    let hkdf = Hkdf::<Sha256>::new(Some(TOKEN_HASH_SALT), master_key);
    let mut key = Zeroizing::new([0u8; 32]);
    hkdf.expand(&info, key.as_mut())
        .map_err(|_| LiveCryptoError::InvalidConfiguration("token hash key derivation failed"))?;
    Ok(key)
}

fn derive_correlation_hash_key(
    master_key: &[u8; 32],
    key_id: &str,
    purpose: CorrelationHashPurpose,
) -> Result<Zeroizing<[u8; 32]>, LiveCryptoError> {
    let mut info = Vec::with_capacity(
        key_id.len() + CORRELATION_HASH_INFO.len() + purpose.as_str().len() + 12,
    );
    append_length_prefixed(&mut info, key_id)?;
    append_length_prefixed(&mut info, CORRELATION_HASH_INFO)?;
    append_length_prefixed(&mut info, purpose.as_str())?;
    let hkdf = Hkdf::<Sha256>::new(Some(TOKEN_HASH_SALT), master_key);
    let mut key = Zeroizing::new([0u8; 32]);
    hkdf.expand(&info, key.as_mut()).map_err(|_| {
        LiveCryptoError::InvalidConfiguration("correlation hash key derivation failed")
    })?;
    Ok(key)
}

fn envelope_aad(context: EnvelopeContext<'_>, key_id: &str) -> Result<Vec<u8>, LiveCryptoError> {
    let mut aad = Vec::with_capacity(
        ENVELOPE_AAD_DOMAIN.len()
            + ENVELOPE_VERSION.len()
            + context.purpose.as_str().len()
            + context.table.len()
            + context.record_id.len()
            + context.column.len()
            + key_id.len()
            + 28,
    );
    for field in [
        ENVELOPE_AAD_DOMAIN,
        ENVELOPE_VERSION,
        context.purpose.as_str(),
        context.table,
        context.record_id,
        context.column,
        key_id,
    ] {
        append_length_prefixed(&mut aad, field)?;
    }
    Ok(aad)
}

fn append_length_prefixed(output: &mut Vec<u8>, value: &str) -> Result<(), LiveCryptoError> {
    let length = u32::try_from(value.len()).map_err(|_| {
        LiveCryptoError::InvalidConfiguration("cryptographic context field is too long")
    })?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn validate_key_id(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > MAX_KEY_ID_BYTES {
        return Err("key ID must contain 1 to 32 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("key ID contains characters outside the path-safe alphabet");
    }
    Ok(())
}

fn validate_context_field(value: &str, label: &'static str) -> Result<(), LiveCryptoError> {
    if value.is_empty()
        || value.len() > MAX_CONTEXT_FIELD_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(LiveCryptoError::InvalidConfiguration(match label {
            "table" => "invalid envelope table context",
            "record id" => "invalid envelope record context",
            "column" => "invalid envelope column context",
            _ => "invalid envelope context",
        }));
    }
    Ok(())
}

fn validate_delivery_token(value: &str) -> Result<(), LiveCryptoError> {
    let encoded = value
        .strip_prefix(DELIVERY_TOKEN_PREFIX)
        .ok_or(LiveCryptoError::InvalidDeliveryToken)?;
    let decoded = general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| LiveCryptoError::InvalidDeliveryToken)?;
    let decoded = Zeroizing::new(decoded);
    if decoded.len() != DELIVERY_TOKEN_BYTES
        || general_purpose::URL_SAFE_NO_PAD.encode(decoded.as_slice()) != encoded
    {
        return Err(LiveCryptoError::InvalidDeliveryToken);
    }
    Ok(())
}

fn parse_token_hash(value: &str) -> Option<(&str, [u8; SHA256_BYTES])> {
    let mut parts = value.split(':');
    if parts.next() != Some(TOKEN_HASH_PREFIX) || parts.next() != Some(TOKEN_HASH_VERSION) {
        return None;
    }
    let key_id = parts.next()?;
    let digest_encoded = parts.next()?;
    if parts.next().is_some() || validate_key_id(key_id).is_err() || digest_encoded.contains('=') {
        return None;
    }
    let digest = general_purpose::URL_SAFE_NO_PAD
        .decode(digest_encoded)
        .ok()?;
    if digest.len() != SHA256_BYTES
        || general_purpose::URL_SAFE_NO_PAD.encode(&digest) != digest_encoded
    {
        return None;
    }
    let mut fixed = [0u8; SHA256_BYTES];
    fixed.copy_from_slice(&digest);
    Some((key_id, fixed))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn crypto(primary: &str, keys: &[(&str, u8)]) -> LiveCrypto {
        LiveCrypto::new(
            primary,
            keys.iter()
                .map(|(key_id, byte)| LiveMasterKey::new(*key_id, [*byte; 32]).unwrap()),
        )
        .unwrap()
    }

    fn descriptor_context<'a>(record_id: &'a str) -> EnvelopeContext<'a> {
        EnvelopeContext::new(
            EnvelopePurpose::Descriptor,
            "live_playback_sessions",
            record_id,
            "encrypted_descriptor",
        )
        .unwrap()
    }

    #[test]
    fn k10_envelope_round_trip_uses_unique_canonical_nonces_and_redacted_secrets() {
        let crypto = crypto("primary-1", &[("primary-1", 7)]);
        let context = descriptor_context("session-1");
        let plaintext = SecretBytes::from_utf8("https://origin.invalid/live?token=secret".into());
        let mut nonces = HashSet::new();
        for _ in 0..1_024 {
            let envelope = crypto.encrypt(context, &plaintext).unwrap();
            assert!(envelope.starts_with("elx-live:v1:primary-1:"));
            let parts: Vec<_> = envelope.split(':').collect();
            assert_eq!(parts.len(), 5);
            assert!(nonces.insert(parts[3].to_string()));
            let decrypted = crypto.decrypt(context, &envelope).unwrap();
            assert_eq!(decrypted.expose_secret(), plaintext.expose_secret());
        }
        assert_eq!(format!("{plaintext:?}"), "SecretBytes([REDACTED])");
        assert_eq!(format!("{plaintext}"), "[REDACTED]");
    }

    #[test]
    fn k10_envelope_rejects_tamper_context_mismatch_and_malformed_input_uniformly() {
        let crypto = crypto("key-a", &[("key-a", 11)]);
        let context = descriptor_context("session-1");
        let envelope = crypto
            .encrypt(context, &SecretBytes::from_utf8("secret-canary".into()))
            .unwrap();
        let wrong_context = EnvelopeContext::new(
            EnvelopePurpose::ItemSnapshot,
            "live_playback_sessions",
            "session-1",
            "encrypted_descriptor",
        )
        .unwrap();
        let mut tampered = envelope.as_bytes().to_vec();
        let last = tampered.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).unwrap();
        let cases = [
            crypto.decrypt(wrong_context, &envelope).unwrap_err(),
            crypto.decrypt(context, &tampered).unwrap_err(),
            crypto
                .decrypt(context, &envelope.replace(":key-a:", ":unknown:"))
                .unwrap_err(),
            crypto
                .decrypt(context, &envelope.replacen(":v1:", ":v2:", 1))
                .unwrap_err(),
            crypto
                .decrypt(context, "elx-live:v1:key-a:x:y")
                .unwrap_err(),
        ];
        for error in cases {
            assert_eq!(error, LiveCryptoError::DecryptionFailed);
            assert_eq!(
                error.to_string(),
                "Live encrypted value could not be processed"
            );
            assert!(!format!("{error:?}").contains("secret-canary"));
        }
    }

    #[test]
    fn k10_envelope_rotation_reads_old_keys_and_rewrites_with_primary() {
        let old = crypto("old", &[("old", 3)]);
        let context = descriptor_context("session-rotate");
        let envelope = old
            .encrypt(context, &SecretBytes::from_utf8("rotate-me".into()))
            .unwrap();
        let rotating = crypto("new", &[("new", 9), ("old", 3)]);
        assert!(rotating.needs_reencryption(&envelope).unwrap());
        assert_eq!(
            rotating
                .decrypt(context, &envelope)
                .unwrap()
                .expose_secret(),
            b"rotate-me"
        );
        let rotated = rotating.reencrypt(context, &envelope).unwrap();
        assert_eq!(rotating.envelope_key_id(&rotated).unwrap(), "new");
        assert!(!rotating.needs_reencryption(&rotated).unwrap());
        let new_only = crypto("new", &[("new", 9)]);
        assert_eq!(
            new_only.decrypt(context, &rotated).unwrap().expose_secret(),
            b"rotate-me"
        );
        assert_eq!(
            old.decrypt(context, &rotated).unwrap_err(),
            LiveCryptoError::DecryptionFailed
        );
    }

    #[test]
    fn k10_delivery_tokens_are_random_hashed_and_verified_in_constant_time_mac_path() {
        let crypto = crypto("token-key", &[("token-key", 21)]);
        let token = LiveDeliveryToken::generate().unwrap();
        let second = LiveDeliveryToken::generate().unwrap();
        assert_ne!(token.expose_secret(), second.expose_secret());
        assert_eq!(
            token.expose_secret().len(),
            DELIVERY_TOKEN_PREFIX.len() + 43
        );
        let hash = crypto.hash_delivery_token(&token).unwrap();
        assert!(
            hash.as_str()
                .starts_with("elx-live-token-hash:v1:token-key:")
        );
        assert_eq!(hash.key_id(), "token-key");
        assert!(crypto.verify_delivery_token(token.expose_secret(), hash.as_str()));
        assert!(!crypto.verify_delivery_token(second.expose_secret(), hash.as_str()));
        assert!(!crypto.verify_delivery_token("elx_live_v1_short", hash.as_str()));
        assert!(!crypto.verify_delivery_token(token.expose_secret(), "malformed"));
        assert_eq!(format!("{token:?}"), "LiveDeliveryToken([REDACTED])");
        assert!(!format!("{hash:?}").contains(hash.as_str()));
    }

    #[test]
    fn k10_token_hash_rotation_accepts_configured_read_key_only() {
        let old = crypto("old", &[("old", 31)]);
        let token = LiveDeliveryToken::generate().unwrap();
        let old_hash = old.hash_delivery_token(&token).unwrap();
        let rotating = LiveCrypto::new_with_primaries(
            "old",
            "new",
            [
                LiveMasterKey::new("new", [32u8; 32]).unwrap(),
                LiveMasterKey::new("old", [31u8; 32]).unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(rotating.primary_key_id().unwrap(), "old");
        assert_eq!(rotating.token_hash_primary_key_id().unwrap(), "new");
        assert!(rotating.verify_delivery_token(token.expose_secret(), old_hash.as_str()));
        let new_hash = rotating.hash_delivery_token(&token).unwrap();
        assert_eq!(new_hash.key_id(), "new");
        assert!(!old.verify_delivery_token(token.expose_secret(), new_hash.as_str()));
        let envelope = rotating
            .encrypt(
                descriptor_context("independent-primary"),
                &SecretBytes::from_utf8("still-old-envelope-key".into()),
            )
            .unwrap();
        assert_eq!(rotating.envelope_key_id(&envelope).unwrap(), "old");
        let envelope_key =
            derive_envelope_key(&[31u8; 32], "old", EnvelopePurpose::Descriptor).unwrap();
        let token_key = derive_token_hash_key(&[31u8; 32], "old").unwrap();
        assert_ne!(envelope_key.as_ref(), token_key.as_ref());
    }

    #[test]
    fn k10_configuration_rejects_duplicate_missing_and_unsafe_key_ids() {
        assert!(matches!(
            LiveMasterKey::new("unsafe:key", [1u8; 32]),
            Err(LiveCryptoError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            LiveMasterKey::new("..", [1u8; 32]),
            Err(LiveCryptoError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            LiveCrypto::new(
                "missing",
                [LiveMasterKey::new("present", [1u8; 32]).unwrap()]
            ),
            Err(LiveCryptoError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            LiveCrypto::new(
                "duplicate",
                [
                    LiveMasterKey::new("duplicate", [1u8; 32]).unwrap(),
                    LiveMasterKey::new("duplicate", [2u8; 32]).unwrap(),
                ]
            ),
            Err(LiveCryptoError::InvalidConfiguration(_))
        ));
    }
}
