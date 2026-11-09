use std::fs;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose};
use rand_core::{OsRng, RngCore};

use crate::config::{RunEnvironment, Settings};

const ENCRYPTED_PREFIX: &str = "enc:v1:";
// Deliberately contains ':' after the legacy prefix: a generic decrypt must
// reject this as invalid base64, even in plaintext-tolerant development mode.
const BOUND_PREFIX: &str = "enc:v1:bound:";
const MASTER_KEY_FILENAME: &str = "secrets.key";

#[derive(Clone)]
pub struct SecretsManager {
    key: [u8; 32],
    fallback_keys: Vec<[u8; 32]>,
    allow_plaintext: bool,
}

impl SecretsManager {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let allow_plaintext = settings.environment == RunEnvironment::Development;
        let configured = settings
            .secrets
            .master_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let key = if let Some(value) = configured {
            decode_master_key(value)?
        } else if settings.environment == RunEnvironment::Production {
            bail!("ELIXIR__SECRETS__MASTER_KEY is required in production");
        } else {
            let path = master_key_path(&settings.extensions.storage_root);
            load_or_create_master_key(&path)?
        };
        let fallback_keys =
            if configured.is_none() && settings.environment == RunEnvironment::Development {
                load_legacy_fallback_keys(&settings.extensions.storage_root, &key)?
            } else {
                Vec::new()
            };

        Ok(Self {
            key,
            fallback_keys,
            allow_plaintext,
        })
    }

    pub fn from_key_bytes(key: [u8; 32], allow_plaintext: bool) -> Self {
        Self {
            key,
            fallback_keys: Vec::new(),
            allow_plaintext,
        }
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String> {
        self.encrypt_payload(plaintext, b"", ENCRYPTED_PREFIX)
    }

    /// Cryptographic data binding only, never permission or runtime authority.
    pub(crate) fn encrypt_bound(&self, plaintext: &str, associated: &[u8]) -> Result<String> {
        if associated.is_empty() {
            bail!("bound secret context must not be empty");
        }
        self.encrypt_payload(plaintext, associated, BOUND_PREFIX)
    }

    /// Stable private request correlation, not authentication or authorization.
    /// A purpose-separated HKDF key avoids putting guessable password hashes
    /// in journals. Context and payload are separately length-delimited.
    pub(crate) fn request_fingerprint(&self, context: &[u8], payload: &[u8]) -> Result<[u8; 32]> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        if context.is_empty() {
            bail!("request fingerprint context must not be empty");
        }
        let mut key = zeroize::Zeroizing::new([0u8; 32]);
        hkdf::Hkdf::<Sha256>::new(Some(b"elixir/secrets/request-fingerprint/v1"), &self.key)
            .expand(b"hmac-sha256/request-id", key.as_mut())
            .map_err(|_| anyhow!("deriving private request fingerprint key failed"))?;
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.as_ref())
            .map_err(|_| anyhow!("initializing private request fingerprint failed"))?;
        mac.update(&(context.len() as u64).to_be_bytes());
        mac.update(context);
        mac.update(&(payload.len() as u64).to_be_bytes());
        mac.update(payload);
        Ok(mac.finalize().into_bytes().into())
    }

    fn encrypt_payload(&self, plaintext: &str, associated: &[u8], prefix: &str) -> Result<String> {
        let cipher = Aes256Gcm::new_from_slice(&self.key).context("initializing secrets cipher")?;
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: plaintext.as_bytes(),
                    aad: associated,
                },
            )
            .map_err(|_| anyhow!("encrypting secret failed"))?;
        let mut payload = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        payload.extend_from_slice(&nonce_bytes);
        payload.extend_from_slice(&ciphertext);
        let encoded = general_purpose::STANDARD.encode(payload);
        Ok(format!("{prefix}{encoded}"))
    }

    pub fn decrypt(&self, value: &str) -> Result<String> {
        if let Some(encoded) = value.strip_prefix(ENCRYPTED_PREFIX) {
            return self.decrypt_payload(encoded, b"");
        }

        if self.allow_plaintext {
            return Ok(value.to_string());
        }

        bail!("secret value is not encrypted");
    }

    pub fn is_encrypted(value: &str) -> bool {
        value.starts_with(ENCRYPTED_PREFIX)
    }

    pub(crate) fn is_bound(value: &str) -> bool {
        value.starts_with(BOUND_PREFIX)
    }

    /// No plaintext or unbound-ciphertext fallback, including development.
    pub(crate) fn decrypt_bound(&self, value: &str, associated: &[u8]) -> Result<String> {
        if associated.is_empty() {
            bail!("bound secret context must not be empty");
        }
        let encoded = value
            .strip_prefix(BOUND_PREFIX)
            .context("bound encrypted secret required")?;
        self.decrypt_payload(encoded, associated)
    }

    fn decrypt_payload(&self, encoded: &str, associated: &[u8]) -> Result<String> {
        let payload = general_purpose::STANDARD
            .decode(encoded)
            .context("decoding encrypted secret")?;
        if payload.len() < 12 {
            bail!("encrypted secret payload is too short");
        }
        let (nonce_bytes, ciphertext) = payload.split_at(12);
        if let Ok(value) = decrypt_with_key(self.key, nonce_bytes, ciphertext, associated) {
            return Ok(value);
        }
        for key in &self.fallback_keys {
            if let Ok(value) = decrypt_with_key(*key, nonce_bytes, ciphertext, associated) {
                return Ok(value);
            }
        }
        bail!("decrypting secret failed");
    }
}

fn master_key_path(storage_root: &str) -> PathBuf {
    Path::new(storage_root).join(MASTER_KEY_FILENAME)
}

fn load_legacy_fallback_keys(storage_root: &str, primary_key: &[u8; 32]) -> Result<Vec<[u8; 32]>> {
    let primary_path = master_key_path(storage_root);
    let mut keys = Vec::new();
    for path in legacy_master_key_paths(storage_root) {
        if path == primary_path || !path.is_file() {
            continue;
        }
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) => {
                tracing::warn!(
                    "failed to read legacy secrets key '{}': {}",
                    path.display(),
                    err
                );
                continue;
            }
        };
        let key = match decode_master_key(raw.trim()) {
            Ok(key) => key,
            Err(err) => {
                tracing::warn!(
                    "failed to decode legacy secrets key '{}': {}",
                    path.display(),
                    err
                );
                continue;
            }
        };
        if &key == primary_key || keys.contains(&key) {
            continue;
        }
        keys.push(key);
    }
    Ok(keys)
}

fn legacy_master_key_paths(storage_root: &str) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        paths.push(
            cwd.join("data")
                .join("extensions")
                .join(MASTER_KEY_FILENAME),
        );
        paths.push(
            cwd.join("elixir-server")
                .join("data")
                .join("extensions")
                .join(MASTER_KEY_FILENAME),
        );
    }

    let storage_root_path = Path::new(storage_root);
    if let Some(workspace_root) = storage_root_path
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
    {
        paths.push(
            workspace_root
                .join("elixir-server")
                .join("data")
                .join("extensions")
                .join(MASTER_KEY_FILENAME),
        );
    }

    paths.sort();
    paths.dedup();
    paths
}

fn decrypt_with_key(
    key: [u8; 32],
    nonce_bytes: &[u8],
    ciphertext: &[u8],
    associated: &[u8],
) -> Result<String> {
    let cipher = Aes256Gcm::new_from_slice(&key).context("initializing secrets cipher")?;
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(nonce_bytes),
            Payload {
                msg: ciphertext,
                aad: associated,
            },
        )
        .map_err(|_| anyhow!("decrypting secret failed"))?;
    String::from_utf8(plaintext).context("secret is not valid utf-8")
}

fn load_or_create_master_key(path: &Path) -> Result<[u8; 32]> {
    if path.exists() {
        let raw = fs::read_to_string(path).context("reading secrets.key")?;
        return decode_master_key(raw.trim());
    }

    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    let encoded = general_purpose::STANDARD.encode(key);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("creating secrets key directory")?;
    }
    fs::write(path, format!("{encoded}\n")).context("writing secrets.key")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perm = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, perm).context("setting secrets.key permissions")?;
    }

    Ok(key)
}

fn decode_master_key(raw: &str) -> Result<[u8; 32]> {
    let decoded = general_purpose::STANDARD
        .decode(raw.trim())
        .context("decoding master key")?;
    if decoded.len() != 32 {
        bail!("master key must be 32 bytes (base64-encoded)");
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&decoded);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_fingerprints_are_private_stable_context_separated_and_unambiguous() -> Result<()> {
        let manager = SecretsManager::from_key_bytes([61; 32], false);
        let id = manager.request_fingerprint(b"context-a", b"low-entropy-password")?;
        // Independent Node/OpenSSL HKDF/HMAC vector freezes the private retry
        // format; accidental key/context/framing changes must not go unnoticed.
        assert_eq!(
            id,
            [
                3, 9, 104, 220, 59, 162, 193, 97, 151, 189, 241, 84, 223, 115, 111, 41, 225, 150,
                125, 135, 252, 253, 208, 211, 188, 198, 187, 165, 184, 238, 244, 115
            ]
        );
        assert_eq!(
            id,
            manager.request_fingerprint(b"context-a", b"low-entropy-password")?
        );
        assert_ne!(
            id,
            manager.request_fingerprint(b"context-b", b"low-entropy-password")?
        );
        assert_ne!(id, manager.request_fingerprint(b"context-a", b"different")?);
        assert_ne!(
            id,
            SecretsManager::from_key_bytes([62; 32], false)
                .request_fingerprint(b"context-a", b"low-entropy-password")?
        );
        assert_ne!(
            manager.request_fingerprint(b"a", b"bc")?,
            manager.request_fingerprint(b"ab", b"c")?
        );
        assert!(manager.request_fingerprint(b"", b"input").is_err());
        // No changes to existing ciphertext format or its ordinary decoder.
        let cipher = manager.encrypt("legacy")?;
        assert_eq!(manager.decrypt(&cipher)?, "legacy");
        Ok(())
    }

    #[test]
    fn bound_ciphertexts_require_their_exact_context_and_never_fall_back_to_legacy_or_plaintext()
    -> Result<()> {
        for development in [false, true] {
            let manager = SecretsManager::from_key_bytes([61; 32], development);
            let cipher = manager.encrypt_bound("private value", b"context-a")?;
            assert_eq!(
                manager.decrypt_bound(&cipher, b"context-a")?,
                "private value"
            );
            assert!(manager.decrypt_bound(&cipher, b"context-b").is_err());
            assert!(manager.decrypt(&cipher).is_err());
            assert!(
                manager
                    .decrypt_bound(&manager.encrypt("private value")?, b"context-a")
                    .is_err()
            );
            assert!(
                manager
                    .decrypt_bound("private value", b"context-a")
                    .is_err()
            );
            assert!(manager.encrypt_bound("private value", b"").is_err());
            assert!(manager.decrypt_bound(&cipher, b"").is_err());
            assert!(
                !format!(
                    "{:#}",
                    manager.decrypt_bound(&cipher, b"wrong").unwrap_err()
                )
                .contains("private value")
            );
        }
        Ok(())
    }

    #[test]
    fn bound_ciphertexts_keep_development_key_rotation_support_without_weakening_context_checks()
    -> Result<()> {
        let old = SecretsManager::from_key_bytes([62; 32], false);
        let cipher = old.encrypt_bound("rotation value", b"context")?;
        let manager = SecretsManager {
            key: [63; 32],
            fallback_keys: vec![[62; 32]],
            allow_plaintext: false,
        };
        assert_eq!(
            manager.decrypt_bound(&cipher, b"context")?,
            "rotation value"
        );
        assert!(manager.decrypt_bound(&cipher, b"wrong").is_err());
        Ok(())
    }

    #[test]
    fn encrypts_and_decrypts() -> Result<()> {
        let manager = SecretsManager::from_key_bytes([7u8; 32], false);
        let enc = manager.encrypt("secret-value")?;
        assert!(enc.starts_with(ENCRYPTED_PREFIX));
        let dec = manager.decrypt(&enc)?;
        assert_eq!(dec, "secret-value");
        Ok(())
    }

    #[test]
    fn rejects_plaintext_when_disabled() {
        let manager = SecretsManager::from_key_bytes([7u8; 32], false);
        let err = manager.decrypt("plaintext").unwrap_err();
        assert!(err.to_string().contains("not encrypted"));
    }

    #[test]
    fn decrypts_with_fallback_key() -> Result<()> {
        let legacy = SecretsManager::from_key_bytes([3u8; 32], false);
        let encrypted = legacy.encrypt("legacy-secret")?;

        let manager = SecretsManager {
            key: [7u8; 32],
            fallback_keys: vec![[3u8; 32]],
            allow_plaintext: false,
        };
        let decrypted = manager.decrypt(&encrypted)?;
        assert_eq!(decrypted, "legacy-secret");
        Ok(())
    }
}
