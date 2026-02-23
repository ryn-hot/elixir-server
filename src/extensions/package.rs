use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use tar::Archive;
use tokio::fs;
use tokio::io::AsyncReadExt;
use zip::ZipArchive;

use crate::extensions::manifest::{ExtensionManifest, ManifestParseResult, parse_manifest_yaml};

#[derive(Debug, Clone)]
pub struct PackageManifest {
    pub manifest: ExtensionManifest,
    pub raw_json: serde_json::Value,
    pub manifest_path: PathBuf,
}

pub async fn read_manifest_from_dir(dir: &Path) -> Result<PackageManifest> {
    let manifest_path = resolve_manifest_path(dir)?;
    let yaml = fs::read_to_string(&manifest_path)
        .await
        .with_context(|| format!("reading manifest at {}", manifest_path.display()))?;
    let ManifestParseResult { manifest, raw_json } = parse_manifest_yaml(&yaml)?;
    Ok(PackageManifest {
        manifest,
        raw_json,
        manifest_path,
    })
}

pub async fn unpack_package(package_path: &Path, dest_dir: &Path) -> Result<PathBuf> {
    if package_path.is_dir() {
        return Ok(package_path.to_path_buf());
    }
    if !package_path.exists() {
        bail!("package not found at {}", package_path.display());
    }
    fs::create_dir_all(dest_dir)
        .await
        .with_context(|| format!("creating package staging dir {}", dest_dir.display()))?;
    let package_path = package_path.to_path_buf();
    let dest_dir = dest_dir.to_path_buf();
    let dest_dir_clone = dest_dir.clone();
    tokio::task::spawn_blocking(move || unpack_archive(&package_path, &dest_dir_clone))
        .await
        .context("joining package unpack task")??;
    Ok(dest_dir)
}

pub async fn compute_sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)
        .await
        .with_context(|| format!("opening package at {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut out, "{:02x}", byte);
    }
    Ok(out)
}

pub async fn read_package_signature(dir: &Path) -> Result<Option<String>> {
    let signature_path = dir.join("package.sig");
    if !signature_path.is_file() {
        return Ok(None);
    }
    let sig = fs::read_to_string(&signature_path)
        .await
        .with_context(|| format!("reading signature at {}", signature_path.display()))?;
    let trimmed = sig.trim().to_string();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed))
}

pub fn verify_signature(
    package_hash: &str,
    signature: Option<&str>,
    publisher_key_id: Option<&str>,
) -> Result<()> {
    let signature = signature
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("package signature is required"))?;
    let publisher_key_id = publisher_key_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("publisher key id is required"))?;

    let verifying_key = parse_public_key(publisher_key_id)?;
    let signature = parse_signature(signature)?;
    let hash_bytes = decode_hex(package_hash)
        .with_context(|| format!("decoding package hash {}", package_hash))?;

    if verifying_key
        .verify(package_hash.as_bytes(), &signature)
        .is_ok()
    {
        return Ok(());
    }
    if verifying_key.verify(&hash_bytes, &signature).is_ok() {
        return Ok(());
    }

    bail!("signature verification failed");
}

fn resolve_manifest_path(dir: &Path) -> Result<PathBuf> {
    let yaml = dir.join("manifest.yaml");
    if yaml.is_file() {
        return Ok(yaml);
    }
    let yml = dir.join("manifest.yml");
    if yml.is_file() {
        return Ok(yml);
    }
    bail!("manifest.yaml not found in {}", dir.display());
}

fn unpack_archive(package_path: &Path, dest_dir: &Path) -> Result<()> {
    let zip_result = unpack_zip(package_path, dest_dir);
    if zip_result.is_ok() {
        return Ok(());
    }

    let tar_result = unpack_tar(package_path, dest_dir);
    if tar_result.is_ok() {
        return Ok(());
    }

    let zip_err = zip_result
        .err()
        .map(|err| err.to_string())
        .unwrap_or_default();
    let tar_err = tar_result
        .err()
        .map(|err| err.to_string())
        .unwrap_or_default();
    bail!(
        "unsupported package format for {} (zip error: {}; tar error: {})",
        package_path.display(),
        zip_err,
        tar_err
    );
}

fn unpack_zip(package_path: &Path, dest_dir: &Path) -> Result<()> {
    let file = File::open(package_path)
        .with_context(|| format!("opening zip package {}", package_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("parsing zip archive {}", package_path.display()))?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .with_context(|| format!("reading zip entry {i}"))?;
        if is_zip_symlink(&entry) {
            bail!(
                "zip entry {} is a symlink, which is not allowed",
                entry.name()
            );
        }
        let rel_path = entry
            .enclosed_name()
            .ok_or_else(|| anyhow::anyhow!("zip entry has invalid path {}", entry.name()))?;
        let out_path = dest_dir.join(rel_path);

        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .with_context(|| format!("creating directory {}", out_path.display()))?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating directory {}", parent.display()))?;
        }
        let mut outfile = File::create(&out_path)
            .with_context(|| format!("creating file {}", out_path.display()))?;
        io::copy(&mut entry, &mut outfile)
            .with_context(|| format!("writing file {}", out_path.display()))?;
    }

    Ok(())
}

fn unpack_tar(package_path: &Path, dest_dir: &Path) -> Result<()> {
    let mut file = File::open(package_path)
        .with_context(|| format!("opening tar package {}", package_path.display()))?;
    let mut magic = [0u8; 2];
    let is_gzip = file.read_exact(&mut magic).is_ok() && magic == [0x1f, 0x8b];
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewinding tar package {}", package_path.display()))?;
    let reader: Box<dyn Read> = if is_gzip {
        Box::new(GzDecoder::new(file))
    } else {
        Box::new(file)
    };
    let mut archive = Archive::new(reader);

    let entries = archive
        .entries()
        .with_context(|| format!("reading tar entries {}", package_path.display()))?;
    for entry in entries {
        let mut entry =
            entry.with_context(|| format!("parsing tar entry {}", package_path.display()))?;
        let entry_type = entry.header().entry_type();
        let entry_path = entry
            .path()
            .with_context(|| format!("reading tar entry path in {}", package_path.display()))?;
        let rel_path = sanitize_entry_path(&entry_path)?;
        let out_path = dest_dir.join(rel_path);

        if entry_type.is_dir() {
            std::fs::create_dir_all(&out_path)
                .with_context(|| format!("creating directory {}", out_path.display()))?;
            continue;
        }

        if entry_type.is_file() {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating directory {}", parent.display()))?;
            }
            entry
                .unpack(&out_path)
                .with_context(|| format!("unpacking {}", out_path.display()))?;
            continue;
        }

        bail!(
            "unsupported tar entry type {:?} for {}",
            entry_type,
            entry_path.display()
        );
    }

    Ok(())
}

fn sanitize_entry_path(path: &Path) -> Result<PathBuf> {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            _ => {
                bail!("invalid archive entry path {}", path.display());
            }
        }
    }
    if clean.as_os_str().is_empty() {
        bail!("invalid archive entry path {}", path.display());
    }
    Ok(clean)
}

fn is_zip_symlink(entry: &zip::read::ZipFile<'_>) -> bool {
    entry
        .unix_mode()
        .map(|mode| (mode & 0o170000) == 0o120000)
        .unwrap_or(false)
}

fn parse_public_key(key_id: &str) -> Result<VerifyingKey> {
    let trimmed = key_id.trim();
    let (scheme, encoded) = match trimmed.split_once(':') {
        Some((scheme, encoded)) => (scheme, encoded),
        None => ("ed25519", trimmed),
    };
    if !scheme.eq_ignore_ascii_case("ed25519") {
        bail!("unsupported signing key type {}", scheme);
    }
    let bytes = decode_text_bytes(encoded).with_context(|| "decoding public key")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("public key must be 32 bytes"))?;
    VerifyingKey::from_bytes(&bytes).context("parsing ed25519 public key")
}

fn parse_signature(signature: &str) -> Result<Signature> {
    let trimmed = signature.trim();
    let encoded = match trimmed.split_once(':') {
        Some((scheme, encoded)) => {
            if !scheme.eq_ignore_ascii_case("ed25519") {
                bail!("unsupported signature type {}", scheme);
            }
            encoded
        }
        None => trimmed,
    };
    let bytes = decode_text_bytes(encoded).with_context(|| "decoding signature")?;
    let bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("signature must be 64 bytes"))?;
    Ok(Signature::from_bytes(&bytes))
}

fn decode_text_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.trim();
    if let Some(bytes) = decode_hex_if_valid(value) {
        return Ok(bytes);
    }
    if let Ok(bytes) = general_purpose::STANDARD.decode(value) {
        return Ok(bytes);
    }
    if let Ok(bytes) = general_purpose::STANDARD_NO_PAD.decode(value) {
        return Ok(bytes);
    }
    bail!("value is not valid hex or base64")
}

fn decode_hex_if_valid(value: &str) -> Option<Vec<u8>> {
    let value = value.trim();
    if value.is_empty() || value.len() % 2 != 0 {
        return None;
    }
    if !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    decode_hex(value).ok()
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut idx = 0;
    while idx < bytes.len() {
        let hi = hex_value(bytes[idx])?;
        let lo = hex_value(bytes[idx + 1])?;
        out.push((hi << 4) | lo);
        idx += 2;
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex character"),
    }
}
