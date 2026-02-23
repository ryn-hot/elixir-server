use std::fs;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose};
use rand_core::{OsRng, RngCore};

use crate::config::{RunEnvironment, Settings};

const ENCRYPTED_PREFIX: &str = "enc:v1:";
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
        let cipher = Aes256Gcm::new_from_slice(&self.key).context("initializing secrets cipher")?;
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), plaintext.as_bytes())
            .map_err(|_| anyhow!("encrypting secret failed"))?;
        let mut payload = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        payload.extend_from_slice(&nonce_bytes);
        payload.extend_from_slice(&ciphertext);
        let encoded = general_purpose::STANDARD.encode(payload);
        Ok(format!("{ENCRYPTED_PREFIX}{encoded}"))
    }

    pub fn decrypt(&self, value: &str) -> Result<String> {
        if let Some(encoded) = value.strip_prefix(ENCRYPTED_PREFIX) {
            let payload = general_purpose::STANDARD
                .decode(encoded)
                .context("decoding encrypted secret")?;
            if payload.len() < 12 {
                bail!("encrypted secret payload is too short");
            }
            let (nonce_bytes, ciphertext) = payload.split_at(12);
            if let Ok(value) = decrypt_with_key(self.key, nonce_bytes, ciphertext) {
                return Ok(value);
            }
            for key in &self.fallback_keys {
                if let Ok(value) = decrypt_with_key(*key, nonce_bytes, ciphertext) {
                    return Ok(value);
                }
            }
            bail!("decrypting secret failed");
        }

        if self.allow_plaintext {
            return Ok(value.to_string());
        }

        bail!("secret value is not encrypted");
    }

    pub fn is_encrypted(value: &str) -> bool {
        value.starts_with(ENCRYPTED_PREFIX)
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

fn decrypt_with_key(key: [u8; 32], nonce_bytes: &[u8], ciphertext: &[u8]) -> Result<String> {
    let cipher = Aes256Gcm::new_from_slice(&key).context("initializing secrets cipher")?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce_bytes), ciphertext)
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
