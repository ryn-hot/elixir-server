use std::fmt::Write as _;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use reqwest::{Client, Url, redirect::Policy};
use serde_json::{Map as JsonMap, Value, json};
use sha2::{Digest, Sha256};
use tokio::fs;
use uuid::Uuid;

use crate::extensions::store::{
    ExtensionSourceModule, ExtensionSourceModuleVersion, ExtensionStore,
    NewExtensionSourceHealthEvent, NewExtensionSourceModuleVersion,
};

const FETCH_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_JS_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;
const MAX_JAR_ARTIFACT_BYTES: usize = 80 * 1024 * 1024;
const NUVIO_CONTAINER_ROOT: &str = "/app/source-modules";
const STREMIO_CONTAINER_ROOT: &str = "/app/stremio-source-modules";
const CLOUDSTREAM_CONTAINER_ROOT: &str = "/app/plugins";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledSourceArtifact {
    pub kind: String,
    pub sha256: String,
    pub host_path: PathBuf,
    pub container_path: String,
    pub version: String,
}

pub async fn install_source_module_artifact(
    store: &ExtensionStore<'_>,
    storage_root: &str,
    module: &ExtensionSourceModule,
    version: &ExtensionSourceModuleVersion,
) -> Result<InstalledSourceArtifact> {
    let artifact_url = version
        .artifact_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("'{}' has no artifact URL", module.display_name))?;
    let url = normalize_safe_artifact_url(artifact_url)?;
    let kind = artifact_kind(module, version)?;
    let max_bytes = artifact_max_bytes(&kind);
    let bytes = fetch_artifact(&url, max_bytes).await?;
    if let Err(err) = smoke_source_artifact(&kind, &bytes) {
        let reason = err.to_string();
        let _ = store
            .set_source_module_version_state(version.version_id, "failed", "failed", Some(&reason))
            .await;
        let _ = store
            .create_source_health_event(&NewExtensionSourceHealthEvent {
                health_event_id: Uuid::new_v4(),
                source_module_id: module.source_module_id,
                event_type: "static_smoke".to_string(),
                state: "broken".to_string(),
                severity: "error".to_string(),
                reason: Some(reason.clone()),
                evidence_json: Some(json!({
                    "artifactUrl": url.as_str(),
                    "artifactKind": kind,
                    "version": version.version,
                })),
                observed_at: Some(Utc::now()),
            })
            .await;
        bail!(
            "source artifact static smoke failed for '{}': {reason}",
            module.display_name
        );
    }
    let actual_hash = sha256_hex(&bytes);
    if let Some(expected) = version.artifact_sha256.as_deref() {
        let expected = normalize_sha256(expected)?;
        if expected != actual_hash {
            bail!(
                "artifact hash mismatch for '{}': expected sha256-{}, got sha256-{}",
                module.display_name,
                expected,
                actual_hash
            );
        }
    }

    let ecosystem_root = artifact_host_root(storage_root, &module.ecosystem);
    let relative_path = artifact_relative_path(&actual_hash, module, &url, &kind);
    let host_path = ecosystem_root.join(&relative_path);
    if let Some(parent) = host_path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating source artifact directory {}", parent.display()))?;
    }
    if !host_path.exists() {
        let tmp_dir = Path::new(storage_root).join("tmp").join("source-artifacts");
        fs::create_dir_all(&tmp_dir)
            .await
            .with_context(|| format!("creating source artifact temp dir {}", tmp_dir.display()))?;
        let tmp_path = tmp_dir.join(format!("{}.tmp", Uuid::new_v4()));
        fs::write(&tmp_path, &bytes)
            .await
            .with_context(|| format!("writing source artifact {}", tmp_path.display()))?;
        fs::rename(&tmp_path, &host_path).await.with_context(|| {
            format!(
                "moving source artifact {} to {}",
                tmp_path.display(),
                host_path.display()
            )
        })?;
    }

    let container_path = artifact_container_path(&module.ecosystem, &relative_path)?;
    let metadata_json = installed_version_metadata(
        module,
        version,
        &kind,
        &url,
        &actual_hash,
        &host_path,
        &container_path,
    )?;
    let install_state = if version.install_state == "active" {
        "active"
    } else {
        "installed"
    };
    store
        .upsert_source_module_version(&NewExtensionSourceModuleVersion {
            version_id: version.version_id,
            source_module_id: version.source_module_id,
            version: version.version.clone(),
            artifact_url: version.artifact_url.clone(),
            artifact_sha256: Some(format!("sha256-{actual_hash}")),
            signature: version.signature.clone(),
            install_state: install_state.to_string(),
            smoke_status: version.smoke_status.clone(),
            smoke_error: version.smoke_error.clone(),
            rollback_of_version_id: version.rollback_of_version_id,
            installed_at: Some(Utc::now()),
            activated_at: version.activated_at,
            metadata_json: Some(metadata_json),
        })
        .await?;
    store
        .create_source_health_event(&NewExtensionSourceHealthEvent {
            health_event_id: Uuid::new_v4(),
            source_module_id: module.source_module_id,
            event_type: "static_smoke".to_string(),
            state: "healthy".to_string(),
            severity: "info".to_string(),
            reason: Some(
                "source artifact fetched, hash-verified, and statically validated".to_string(),
            ),
            evidence_json: Some(json!({
                "artifactKind": kind,
                "artifactSha256": format!("sha256-{actual_hash}"),
                "version": version.version,
                "containerPath": container_path,
            })),
            observed_at: Some(Utc::now()),
        })
        .await?;

    Ok(InstalledSourceArtifact {
        kind,
        sha256: format!("sha256-{actual_hash}"),
        host_path,
        container_path,
        version: version.version.clone(),
    })
}

fn smoke_source_artifact(kind: &str, bytes: &[u8]) -> Result<()> {
    match kind {
        "javascript" => {
            let source = std::str::from_utf8(bytes)
                .context("JavaScript source artifact is not valid UTF-8")?;
            if !source.contains("getStreams") {
                bail!("JavaScript source module does not expose getStreams");
            }
            Ok(())
        }
        "jar" => {
            if bytes.len() < 4 || &bytes[0..4] != b"PK\x03\x04" {
                bail!("JAR source artifact is not a zip archive");
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn installed_version_metadata(
    module: &ExtensionSourceModule,
    version: &ExtensionSourceModuleVersion,
    kind: &str,
    url: &Url,
    actual_hash: &str,
    host_path: &Path,
    container_path: &str,
) -> Result<Value> {
    let mut metadata = version
        .metadata_json
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    metadata.insert(
        "artifact".to_string(),
        json!({
            "kind": kind,
            "sourceUrl": url.as_str(),
            "sha256": format!("sha256-{actual_hash}"),
            "hostPath": host_path,
            "containerPath": container_path,
            "installedAt": Utc::now(),
        }),
    );
    match module.ecosystem.as_str() {
        "nuvio" | "stremio" => {
            merge_object(
                &mut metadata,
                "nuvio",
                json!({
                    "scriptPath": container_path,
                    "artifactPath": container_path,
                    "artifactSha256": format!("sha256-{actual_hash}"),
                }),
            )?;
        }
        "cloudstream" => {
            merge_object(
                &mut metadata,
                "cloudstream",
                json!({
                    "pluginJarPath": container_path,
                    "pluginJarSha256": format!("sha256-{actual_hash}"),
                    "artifactSha256": format!("sha256-{actual_hash}"),
                }),
            )?;
        }
        _ => {}
    }
    Ok(Value::Object(metadata))
}

fn merge_object(metadata: &mut JsonMap<String, Value>, key: &str, update: Value) -> Result<()> {
    let update = update
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("metadata update must be an object"))?;
    let mut existing = metadata
        .get(key)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (field, value) in update {
        existing.insert(field.clone(), value.clone());
    }
    metadata.insert(key.to_string(), Value::Object(existing));
    Ok(())
}

async fn fetch_artifact(url: &Url, max_bytes: usize) -> Result<Vec<u8>> {
    let client = Client::builder()
        .timeout(FETCH_TIMEOUT)
        .redirect(Policy::limited(5))
        .build()
        .context("building source artifact HTTP client")?;
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("fetching source artifact {url}"))?
        .error_for_status()
        .with_context(|| format!("source artifact {url} returned an error status"))?;
    if let Some(length) = response.content_length() {
        if length > max_bytes as u64 {
            bail!(
                "source artifact {} is too large: {} bytes exceeds {} bytes",
                url,
                length,
                max_bytes
            );
        }
    }
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading source artifact {url}"))?;
    if bytes.len() > max_bytes {
        bail!(
            "source artifact {} is too large: {} bytes exceeds {} bytes",
            url,
            bytes.len(),
            max_bytes
        );
    }
    Ok(bytes.to_vec())
}

fn normalize_safe_artifact_url(input: &str) -> Result<Url> {
    let url = Url::parse(input.trim()).context("parsing source artifact URL")?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => bail!("source artifact URL scheme '{scheme}' is not allowed"),
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("source artifact URL credentials are not allowed");
    }
    let Some(host) = url
        .host_str()
        .map(str::trim)
        .filter(|host| !host.is_empty())
    else {
        bail!("source artifact URL host is required");
    };
    let lower = host
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase();
    if lower == "localhost" || lower.ends_with(".localhost") || lower.ends_with(".local") {
        bail!("private or local source artifact host '{host}' is not allowed");
    }
    if let Ok(ip) = lower.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(ip) => {
                if ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_broadcast()
                    || ip.is_documentation()
                    || ip.octets()[0] == 0
                {
                    bail!("private or local source artifact IP address '{ip}' is not allowed");
                }
            }
            IpAddr::V6(ip) => {
                if ip.is_loopback()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || ip.is_unspecified()
                    || (ip.segments()[0] & 0xffc0) == 0xfe80
                {
                    bail!("private or local source artifact IP address '{ip}' is not allowed");
                }
            }
        }
    }
    Ok(url)
}

fn artifact_kind(
    module: &ExtensionSourceModule,
    version: &ExtensionSourceModuleVersion,
) -> Result<String> {
    if let Some(kind) = version
        .metadata_json
        .as_ref()
        .and_then(|value| value.get("artifact"))
        .and_then(|value| value.get("kind"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(kind.to_ascii_lowercase());
    }
    match module.ecosystem.as_str() {
        "nuvio" | "stremio" => Ok("javascript".to_string()),
        "cloudstream" => Ok("jar".to_string()),
        other => bail!("source module ecosystem '{other}' does not support artifact install"),
    }
}

fn artifact_max_bytes(kind: &str) -> usize {
    match kind {
        "jar" => MAX_JAR_ARTIFACT_BYTES,
        _ => MAX_JS_ARTIFACT_BYTES,
    }
}

fn artifact_host_root(storage_root: &str, ecosystem: &str) -> PathBuf {
    Path::new(storage_root)
        .join("source-artifacts")
        .join(stable_text_id(ecosystem))
}

fn artifact_relative_path(
    actual_hash: &str,
    module: &ExtensionSourceModule,
    url: &Url,
    kind: &str,
) -> PathBuf {
    let prefix = &actual_hash[0..2];
    let filename = artifact_filename(module, url, kind);
    PathBuf::from("sha256")
        .join(prefix)
        .join(actual_hash)
        .join(filename)
}

fn artifact_filename(module: &ExtensionSourceModule, url: &Url, kind: &str) -> String {
    let from_url = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(sanitize_filename);
    let fallback = format!(
        "{}.{}",
        stable_text_id(&module.display_name),
        match kind {
            "jar" => "jar",
            _ => "js",
        }
    );
    from_url.unwrap_or(fallback)
}

fn artifact_container_path(ecosystem: &str, relative_path: &Path) -> Result<String> {
    let root = match ecosystem {
        "nuvio" => NUVIO_CONTAINER_ROOT,
        "stremio" => STREMIO_CONTAINER_ROOT,
        "cloudstream" => CLOUDSTREAM_CONTAINER_ROOT,
        other => bail!("source module ecosystem '{other}' does not support artifact install"),
    };
    let relative = relative_path
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    Ok(format!("{root}/{relative}"))
}

fn normalize_sha256(value: &str) -> Result<String> {
    let normalized = value
        .trim()
        .strip_prefix("sha256-")
        .or_else(|| value.trim().strip_prefix("sha256:"))
        .unwrap_or(value.trim())
        .to_ascii_lowercase();
    if normalized.len() != 64 || !normalized.chars().all(|ch| ch.is_ascii_hexdigit()) {
        bail!("source artifact sha256 must be a 64-character hex digest");
    }
    Ok(normalized)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut output, "{:02x}", byte);
    }
    output
}

fn sanitize_filename(value: &str) -> String {
    let mut output = String::new();
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            output.push(ch);
        } else if !output.ends_with('-') {
            output.push('-');
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "source-artifact".to_string()
    } else {
        output
    }
}

fn stable_text_id(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !output.is_empty() {
            output.push('-');
            last_dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "source".to_string()
    } else {
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_supported_sha256_prefixes() -> Result<()> {
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(normalize_sha256(hash)?, hash);
        assert_eq!(normalize_sha256(&format!("sha256-{hash}"))?, hash);
        assert_eq!(normalize_sha256(&format!("sha256:{hash}"))?, hash);
        Ok(())
    }

    #[test]
    fn rejects_private_artifact_urls() {
        let err = normalize_safe_artifact_url("http://127.0.0.1/provider.js")
            .expect_err("private URL should be rejected");
        assert!(err.to_string().contains("private") || err.to_string().contains("local"));
    }

    #[test]
    fn builds_nuvio_container_path_under_source_modules_root() -> Result<()> {
        let relative = PathBuf::from("sha256/aa/hash/provider.js");
        assert_eq!(
            artifact_container_path("nuvio", &relative)?,
            "/app/source-modules/sha256/aa/hash/provider.js"
        );
        Ok(())
    }

    #[test]
    fn builds_stremio_container_path_under_prism_stremio_root() -> Result<()> {
        let relative = PathBuf::from("sha256/bb/hash/provider.js");
        assert_eq!(
            artifact_container_path("stremio", &relative)?,
            "/app/stremio-source-modules/sha256/bb/hash/provider.js"
        );
        Ok(())
    }
}
