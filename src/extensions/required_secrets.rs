use std::collections::HashSet;

use anyhow::{Result, anyhow, bail};
use uuid::Uuid;

use crate::db::models::{ExtensionKind, SecretScope};
use crate::extensions::manifest::{ExtensionManifest, ManifestRuntimeEnv};
use crate::extensions::store::ExtensionStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredSecretRef {
    pub scope: SecretScope,
    pub scope_id: Option<Uuid>,
    pub key: String,
}

pub fn required_secrets_from_manifest(
    manifest: &ExtensionManifest,
) -> Result<Vec<RequiredSecretRef>> {
    if manifest.kind != ExtensionKind::Module {
        return Ok(Vec::new());
    }
    let runtime = match manifest.runtime.as_ref() {
        Some(runtime) => runtime,
        None => return Ok(Vec::new()),
    };
    required_secrets_from_runtime(&runtime.env)
}

pub fn required_secrets_from_runtime(
    env: &[ManifestRuntimeEnv],
) -> Result<Vec<RequiredSecretRef>> {
    let mut required = Vec::new();
    for env in env {
        if let Some(from_secret) = env.from_secret.as_ref() {
            required.push(parse_required_secret(from_secret)?);
        }
    }
    Ok(required)
}

pub async fn missing_required_secrets_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    required: &[RequiredSecretRef],
) -> Result<Vec<String>> {
    if required.is_empty() {
        return Ok(Vec::new());
    }
    let mut missing = HashSet::new();
    for required in required {
        match required.scope {
            SecretScope::Global => {
                if !secret_exists(store, SecretScope::Global, None, &required.key).await? {
                    missing.insert(format!("global:{}", required.key));
                }
            }
            SecretScope::Instance => {
                if !secret_exists(
                    store,
                    SecretScope::Instance,
                    Some(instance_id),
                    &required.key,
                )
                .await?
                {
                    missing.insert(format!("instance:{}:{}", instance_id, required.key));
                }
            }
            SecretScope::Provider => {
                let scope_id = required
                    .scope_id
                    .ok_or_else(|| anyhow!("provider scope_id is required"))?;
                if !secret_exists(
                    store,
                    SecretScope::Provider,
                    Some(scope_id),
                    &required.key,
                )
                .await?
                {
                    missing.insert(format!("provider:{}:{}", scope_id, required.key));
                }
            }
        }
    }
    Ok(sorted_missing(missing))
}

pub async fn missing_required_secrets_for_instances(
    store: &ExtensionStore<'_>,
    instance_ids: &[Uuid],
    required: &[RequiredSecretRef],
) -> Result<Vec<String>> {
    if required.is_empty() {
        return Ok(Vec::new());
    }
    let mut missing = HashSet::new();
    for required in required {
        match required.scope {
            SecretScope::Global => {
                if !secret_exists(store, SecretScope::Global, None, &required.key).await? {
                    missing.insert(format!("global:{}", required.key));
                }
            }
            SecretScope::Instance => {
                if instance_ids.is_empty() {
                    continue;
                }
                for instance_id in instance_ids {
                    if !secret_exists(
                        store,
                        SecretScope::Instance,
                        Some(*instance_id),
                        &required.key,
                    )
                    .await?
                    {
                        missing.insert(format!("instance:{}:{}", instance_id, required.key));
                    }
                }
            }
            SecretScope::Provider => {
                let scope_id = required
                    .scope_id
                    .ok_or_else(|| anyhow!("provider scope_id is required"))?;
                if !secret_exists(
                    store,
                    SecretScope::Provider,
                    Some(scope_id),
                    &required.key,
                )
                .await?
                {
                    missing.insert(format!("provider:{}:{}", scope_id, required.key));
                }
            }
        }
    }
    Ok(sorted_missing(missing))
}

fn parse_required_secret(raw: &str) -> Result<RequiredSecretRef> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("from_secret must not be empty");
    }
    let parts: Vec<&str> = trimmed.split(':').collect();
    match parts.as_slice() {
        ["global", key] => {
            if key.trim().is_empty() {
                bail!("from_secret global key is required");
            }
            Ok(RequiredSecretRef {
                scope: SecretScope::Global,
                scope_id: None,
                key: (*key).to_string(),
            })
        }
        ["instance", key] => {
            if key.trim().is_empty() {
                bail!("from_secret instance key is required");
            }
            Ok(RequiredSecretRef {
                scope: SecretScope::Instance,
                scope_id: None,
                key: (*key).to_string(),
            })
        }
        ["provider", provider_id, key] => {
            if key.trim().is_empty() {
                bail!("from_secret provider key is required");
            }
            let scope_id = Uuid::parse_str(provider_id)
                .map_err(|_| anyhow!("from_secret provider id is invalid"))?;
            Ok(RequiredSecretRef {
                scope: SecretScope::Provider,
                scope_id: Some(scope_id),
                key: (*key).to_string(),
            })
        }
        _ => bail!(
            "from_secret must be global:<key>, instance:<key>, or provider:<uuid>:<key>"
        ),
    }
}

async fn secret_exists(
    store: &ExtensionStore<'_>,
    scope: SecretScope,
    scope_id: Option<Uuid>,
    key: &str,
) -> Result<bool> {
    Ok(store.get_secret(scope, scope_id, key).await?.is_some())
}

fn sorted_missing(missing: HashSet<String>) -> Vec<String> {
    let mut missing: Vec<_> = missing.into_iter().collect();
    missing.sort();
    missing
}
