use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::db::models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionManifest {
    pub id: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub publisher: Option<ManifestPublisher>,
    #[serde(default)]
    pub trust: Option<ExtensionTrustLevel>,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default)]
    pub provides: Vec<ManifestProvide>,
    #[serde(default)]
    pub requires: Vec<ManifestRequire>,
    #[serde(default)]
    pub conflicts: Vec<ManifestConflict>,
    #[serde(default)]
    pub runtime: Option<ManifestRuntime>,
    #[serde(default)]
    pub targets: Vec<ManifestCapabilityRef>,
    #[serde(default)]
    pub actions: Vec<ManifestAction>,
    #[serde(default)]
    pub wants: Vec<ManifestCapabilityRef>,
    #[serde(default)]
    pub preferences: Option<ManifestPreferences>,
    #[serde(default)]
    pub bindings: Vec<ManifestBinding>,
    #[serde(default)]
    pub policies: Option<ManifestPolicies>,
    #[serde(default)]
    pub networking: Option<ManifestNetworking>,
}

impl ExtensionManifest {
    pub fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.id, "id")?;
        ensure_non_empty(&self.version, "version")?;
        ensure_non_empty(&self.name, "name")?;

        for permission in &self.permissions {
            if permission.trim().is_empty() {
                bail!("manifest permissions must be non-empty");
            }
        }

        for provide in &self.provides {
            ensure_non_empty(&provide.capability, "provides.capability")?;
            ensure_non_empty(&provide.slot, "provides.slot")?;
            if let Some(implementation) = provide.implementation.as_ref() {
                ensure_non_empty(implementation, "provides.implementation")?;
            }
        }

        for require in &self.requires {
            ensure_non_empty(&require.capability, "requires.capability")?;
            ensure_non_empty(&require.slot, "requires.slot")?;
        }

        match self.kind {
            ExtensionKind::Module => {
                if self.provides.is_empty() {
                    bail!("module manifests must declare at least one provided capability");
                }
                if self.runtime.is_none() {
                    bail!("module manifests must declare a runtime");
                }
                for provide in &self.provides {
                    if provide
                        .implementation
                        .as_ref()
                        .map(|value| value.trim().is_empty())
                        .unwrap_or(true)
                    {
                        bail!(
                            "module manifests must declare provides.implementation for capability '{}'",
                            provide.capability
                        );
                    }
                }
            }
            ExtensionKind::Connector => {
                if self.targets.is_empty() {
                    bail!("connector manifests must declare at least one target");
                }
                if self.actions.is_empty() {
                    bail!("connector manifests must declare at least one action");
                }
            }
            ExtensionKind::Blueprint => {
                if self.wants.is_empty() {
                    bail!("blueprint manifests must declare at least one want");
                }
            }
        }

        if let Some(runtime) = &self.runtime {
            ensure_non_empty(&runtime.r#type, "runtime.type")?;
            if runtime.r#type != "container" && runtime.r#type != "internal" {
                bail!("unsupported runtime type '{}'", runtime.r#type);
            }
            for env in &runtime.env {
                ensure_non_empty(&env.name, "runtime.env.name")?;
                if env.value.is_none() && env.from_secret.is_none() {
                    bail!("runtime.env entry must include value or from_secret");
                }
                if env.value.is_some() && env.from_secret.is_some() {
                    bail!("runtime.env entry must not include both value and from_secret");
                }
                if let Some(from_secret) = env.from_secret.as_ref() {
                    if from_secret.trim().is_empty() {
                        bail!("runtime.env from_secret must not be empty");
                    }
                }
            }
        }

        for action in &self.actions {
            ensure_non_empty(&action.r#type, "actions.type")?;
            if action.r#type == "driver_patch" {
                if action.target.is_none() {
                    bail!("driver_patch actions require a target");
                }
                if action.patch.is_none() {
                    bail!("driver_patch actions require a patch");
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestPublisher {
    pub name: String,
    #[serde(default)]
    pub key_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestProvide {
    pub capability: String,
    #[serde(default = "default_slot")]
    pub slot: String,
    #[serde(default)]
    pub cardinality: Option<SlotCardinality>,
    #[serde(default)]
    pub implementation: Option<String>,
    #[serde(default)]
    pub endpoint: Option<ManifestEndpoint>,
    #[serde(default)]
    pub healthcheck: Option<ManifestHealthcheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRequire {
    pub capability: String,
    #[serde(default = "default_slot")]
    pub slot: String,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestConflict {
    pub capability: String,
    #[serde(default = "default_slot")]
    pub slot: String,
    pub policy: ConflictPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    PromptReplace,
    AutoReplace,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEndpoint {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub base_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestHealthcheck {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRuntime {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub network: Option<String>,
    #[serde(default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub ports: Vec<ManifestRuntimePort>,
    #[serde(default)]
    pub volumes: Vec<String>,
    #[serde(default)]
    pub env: Vec<ManifestRuntimeEnv>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRuntimePort {
    pub container: u16,
    #[serde(default)]
    pub host: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRuntimeEnv {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub from_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCapabilityRef {
    pub capability: String,
    #[serde(default = "default_slot")]
    pub slot: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestAction {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub target: Option<ManifestCapabilityRef>,
    #[serde(default)]
    pub patch: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestPreferences {
    #[serde(default)]
    pub providers: HashMap<String, ManifestProviderPreference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestProviderPreference {
    #[serde(default)]
    pub prefer: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestBinding {
    pub from: ManifestCapabilityRef,
    pub to: ManifestCapabilityRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestPolicies {
    #[serde(default)]
    pub reuse_existing: Option<bool>,
    #[serde(default)]
    pub conflicts: Option<String>,
    #[serde(default)]
    pub allow_community_extensions: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestNetworking {
    pub service_port: ManifestServicePort,
    #[serde(default)]
    pub extra_ports: Vec<ManifestServicePort>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestServicePort {
    #[serde(default)]
    pub name: Option<String>,
    pub scheme: String,
    pub container_port: u16,
}

#[derive(Debug, Clone)]
pub struct ManifestParseResult {
    pub manifest: ExtensionManifest,
    pub raw_json: serde_json::Value,
}

pub fn parse_manifest_yaml(yaml: &str) -> Result<ManifestParseResult> {
    let raw_yaml: serde_yaml::Value = serde_yaml::from_str(yaml)
        .context("parsing manifest yaml")?;
    let manifest: ExtensionManifest = serde_yaml::from_value(raw_yaml.clone())
        .context("parsing manifest fields")?;
    manifest.validate()?;
    let raw_json = serde_json::to_value(raw_yaml).context("converting manifest to json")?;
    Ok(ManifestParseResult { manifest, raw_json })
}

fn default_slot() -> String {
    "default".to_string()
}

fn ensure_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("manifest {} is required", field);
    }
    Ok(())
}
