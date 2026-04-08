use std::collections::HashMap;

use anyhow::{Context, Result, bail};
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
    pub connectors: Vec<String>,
    #[serde(default)]
    pub optional_addons: Vec<ManifestOptionalAddon>,
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
            if let Some(scope) = provide.scope.as_ref() {
                scope.validate()?;
                validate_scope_media_for_capability(&provide.capability, scope)?;
            }
            validate_scope_actions_for_capability(&provide.capability, provide.scope.as_ref())?;
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
            if let Some(egress) = runtime.egress.as_ref() {
                egress.validate()?;
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

        for connector in &self.connectors {
            ensure_non_empty(connector, "connectors")?;
        }

        for addon in &self.optional_addons {
            addon.validate()?;
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
    pub scope: Option<ManifestProviderScope>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManifestProviderScope {
    #[serde(default)]
    pub media_types: Vec<String>,
    #[serde(default)]
    pub actions: Vec<String>,
    #[serde(default)]
    pub requires_account: bool,
    #[serde(default)]
    pub required_fields: Vec<String>,
}

impl ManifestProviderScope {
    pub fn validate(&self) -> Result<()> {
        for media_type in &self.media_types {
            ensure_non_empty(media_type, "provides.scope.media_types")?;
            if canonical_media_type(media_type).is_none() {
                bail!(
                    "unsupported provides.scope.media_types value '{}'; expected tv|movies|anime",
                    media_type
                );
            }
        }
        for action in &self.actions {
            ensure_non_empty(action, "provides.scope.actions")?;
            if !matches!(
                action.trim().to_ascii_lowercase().as_str(),
                "search" | "add" | "monitor"
            ) {
                bail!(
                    "unsupported provides.scope.actions value '{}'; expected search|add|monitor",
                    action
                );
            }
        }
        for field in &self.required_fields {
            ensure_non_empty(field, "provides.scope.required_fields")?;
        }
        if !self.requires_account && !self.required_fields.is_empty() {
            bail!("provides.scope.required_fields requires provides.scope.requires_account=true");
        }
        Ok(())
    }
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
    #[serde(default)]
    pub egress: Option<ManifestRuntimeEgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRuntimeEgress {
    #[serde(default = "default_runtime_egress_mode")]
    pub mode: String,
    #[serde(default = "default_true")]
    pub strict: bool,
    #[serde(default)]
    pub wireguard_config_secret: Option<String>,
    #[serde(default)]
    pub wireguard_gateway_image: Option<String>,
}

impl ManifestRuntimeEgress {
    pub fn validate(&self) -> Result<()> {
        let mode = self.mode.trim().to_ascii_lowercase();
        match mode.as_str() {
            "direct" => {}
            "wireguard" => {
                let secret = self
                    .wireguard_config_secret
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default();
                if secret.is_empty() {
                    bail!("runtime.egress.wireguard_config_secret is required for wireguard mode");
                }
                validate_secret_reference(secret, "runtime.egress.wireguard_config_secret")?;
                if let Some(image) = self.wireguard_gateway_image.as_deref() {
                    ensure_non_empty(image, "runtime.egress.wireguard_gateway_image")?;
                }
            }
            _ => bail!(
                "unsupported runtime.egress.mode '{}'; expected direct|wireguard",
                self.mode
            ),
        }
        Ok(())
    }

    pub fn mode_is_wireguard(&self) -> bool {
        self.mode.trim().eq_ignore_ascii_case("wireguard")
    }
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
pub struct ManifestOptionalAddon {
    pub extension_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub required_fields: Vec<String>,
    #[serde(default)]
    pub secret_key_prefix: Option<String>,
    #[serde(default)]
    pub target: Option<ManifestCapabilityRef>,
}

impl ManifestOptionalAddon {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.extension_id, "optional_addons.extension_id")?;
        if let Some(title) = self.title.as_deref() {
            ensure_non_empty(title, "optional_addons.title")?;
        }
        if let Some(description) = self.description.as_deref() {
            ensure_non_empty(description, "optional_addons.description")?;
        }
        for field in &self.required_fields {
            ensure_non_empty(field, "optional_addons.required_fields")?;
        }
        if !self.required_fields.is_empty() {
            let Some(prefix) = self.secret_key_prefix.as_deref() else {
                bail!(
                    "optional_addons.secret_key_prefix is required when optional_addons.required_fields is set"
                );
            };
            ensure_non_empty(prefix, "optional_addons.secret_key_prefix")?;
            let Some(target) = self.target.as_ref() else {
                bail!(
                    "optional_addons.target is required when optional_addons.required_fields is set"
                );
            };
            ensure_non_empty(&target.capability, "optional_addons.target.capability")?;
            ensure_non_empty(&target.slot, "optional_addons.target.slot")?;
        } else {
            if let Some(prefix) = self.secret_key_prefix.as_deref() {
                ensure_non_empty(prefix, "optional_addons.secret_key_prefix")?;
            }
            if let Some(target) = self.target.as_ref() {
                ensure_non_empty(&target.capability, "optional_addons.target.capability")?;
                ensure_non_empty(&target.slot, "optional_addons.target.slot")?;
            }
        }
        Ok(())
    }
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
    let raw_yaml: serde_yaml::Value =
        serde_yaml::from_str(yaml).context("parsing manifest yaml")?;
    let manifest: ExtensionManifest =
        serde_yaml::from_value(raw_yaml.clone()).context("parsing manifest fields")?;
    manifest.validate()?;
    let raw_json = serde_json::to_value(raw_yaml).context("converting manifest to json")?;
    Ok(ManifestParseResult { manifest, raw_json })
}

fn default_slot() -> String {
    "default".to_string()
}

fn default_runtime_egress_mode() -> String {
    "direct".to_string()
}

fn default_true() -> bool {
    true
}

fn ensure_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("manifest {} is required", field);
    }
    Ok(())
}

fn validate_secret_reference(value: &str, field: &str) -> Result<()> {
    let parts: Vec<&str> = value.split(':').collect();
    match parts.as_slice() {
        ["instance", key] | ["global", key] => {
            if key.trim().is_empty() {
                bail!("{} key must not be empty", field);
            }
            Ok(())
        }
        ["provider", provider_id, key] => {
            if provider_id.trim().is_empty() {
                bail!("{} provider id must not be empty", field);
            }
            if key.trim().is_empty() {
                bail!("{} provider key must not be empty", field);
            }
            Ok(())
        }
        _ => bail!(
            "{} must be instance:<key>, global:<key>, or provider:<uuid>:<key>",
            field
        ),
    }
}

fn canonical_media_type(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Some("movies"),
        "series" | "tv" => Some("tv"),
        "anime" => Some("anime"),
        _ => None,
    }
}

fn infer_scope_actions_for_capability(capability: &str) -> Vec<&'static str> {
    match capability.trim().to_ascii_lowercase().as_str() {
        "media.manager.movies" | "media.manager.tv" | "media.manager.anime" => {
            vec!["add", "monitor"]
        }
        value if value.starts_with("media.search.") => vec!["search"],
        _ => Vec::new(),
    }
}

fn infer_scope_media_for_capability(capability: &str) -> Option<Vec<&'static str>> {
    match capability.trim().to_ascii_lowercase().as_str() {
        "media.manager.movies" | "media.search.movie" | "media.search.movies" => {
            Some(vec!["movies"])
        }
        "media.manager.tv" => Some(vec!["tv", "anime"]),
        "media.manager.anime" | "media.search.anime" => Some(vec!["anime"]),
        "media.search.series" | "media.search.tv" => Some(vec!["tv"]),
        _ => None,
    }
}

fn validate_scope_actions_for_capability(
    capability: &str,
    scope: Option<&ManifestProviderScope>,
) -> Result<()> {
    let mut actions = scope
        .map(|scope| scope.actions.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if actions.is_empty() {
        actions = infer_scope_actions_for_capability(capability)
            .into_iter()
            .map(str::to_string)
            .collect();
    }
    let capability = capability.trim().to_ascii_lowercase();
    if capability.starts_with("media.manager.") && !actions.iter().any(|value| value == "add") {
        bail!(
            "manager capability '{}' requires provides.scope.actions to include 'add'",
            capability
        );
    }
    if capability.starts_with("media.search.") && !actions.iter().any(|value| value == "search") {
        bail!(
            "search capability '{}' requires provides.scope.actions to include 'search'",
            capability
        );
    }
    Ok(())
}

fn validate_scope_media_for_capability(
    capability: &str,
    scope: &ManifestProviderScope,
) -> Result<()> {
    let Some(allowed) = infer_scope_media_for_capability(capability) else {
        return Ok(());
    };
    if scope.media_types.is_empty() {
        return Ok(());
    }
    for media_type in &scope.media_types {
        let Some(canonical) = canonical_media_type(media_type) else {
            continue;
        };
        if !allowed.contains(&canonical) {
            bail!(
                "scope media type '{}' is incompatible with capability '{}'",
                media_type,
                capability
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_accepts_wireguard_runtime_egress() {
        let yaml = r#"
id: elixir.modules.test
version: 1.0.0
kind: module
name: Test Module
provides:
  - capability: downloader.torrent
    slot: default
    implementation: qbittorrent
runtime:
  type: container
  image: example/test:1
  egress:
    mode: wireguard
    strict: true
    wireguard_config_secret: instance:wg_config
"#;
        let parsed = parse_manifest_yaml(yaml).expect("manifest should parse");
        let egress = parsed
            .manifest
            .runtime
            .expect("runtime")
            .egress
            .expect("egress");
        assert!(egress.mode_is_wireguard());
        assert_eq!(
            egress.wireguard_config_secret.as_deref(),
            Some("instance:wg_config")
        );
    }

    #[test]
    fn manifest_rejects_wireguard_egress_without_secret() {
        let yaml = r#"
id: elixir.modules.test
version: 1.0.0
kind: module
name: Test Module
provides:
  - capability: downloader.torrent
    slot: default
    implementation: qbittorrent
runtime:
  type: container
  image: example/test:1
  egress:
    mode: wireguard
"#;
        let err = parse_manifest_yaml(yaml).unwrap_err();
        assert!(err.to_string().contains("wireguard_config_secret"));
    }
}
