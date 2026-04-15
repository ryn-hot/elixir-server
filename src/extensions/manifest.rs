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
    // Reserved for a future generic control bridge.
    //
    // Current built-ins use handwritten server adapters, and the runtime does
    // not yet execute manifest-defined control surfaces for community
    // extensions. This field is intentionally dormant for now.
    #[serde(default)]
    pub control_surface: Option<ManifestControlSurface>,
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

        if let Some(control_surface) = &self.control_surface {
            control_surface.validate()?;
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

// Declarative control metadata reserved for a future generic runtime bridge.
//
// Validation is active so manifests can be shaped consistently, but the server
// does not yet execute this contract for community extensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlSurface {
    #[serde(default = "default_control_surface_adapter")]
    pub adapter: String,
    #[serde(default)]
    pub owned_settings: Vec<ManifestControlOwnedSetting>,
    #[serde(default)]
    pub observed_state: Vec<ManifestControlObservedState>,
    #[serde(default)]
    pub entities: Vec<ManifestControlEntityCollection>,
    #[serde(default)]
    pub actions: Vec<ManifestControlActionDef>,
    #[serde(default)]
    pub native_only: Vec<ManifestControlNativeArea>,
}

impl ManifestControlSurface {
    fn validate(&self) -> Result<()> {
        let adapter = self.adapter.trim().to_ascii_lowercase();
        if adapter != "generic_v1" {
            bail!(
                "unsupported control_surface.adapter '{}'; expected generic_v1",
                self.adapter
            );
        }

        ensure_unique_control_ids(
            &self
                .owned_settings
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            "control_surface.owned_settings",
        )?;
        ensure_unique_control_ids(
            &self
                .observed_state
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            "control_surface.observed_state",
        )?;
        ensure_unique_control_ids(
            &self
                .entities
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            "control_surface.entities",
        )?;
        ensure_unique_control_ids(
            &self
                .actions
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            "control_surface.actions",
        )?;
        ensure_unique_control_ids(
            &self
                .native_only
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            "control_surface.native_only",
        )?;

        for setting in &self.owned_settings {
            setting.validate()?;
        }
        for state in &self.observed_state {
            state.validate()?;
        }
        let action_ids = self
            .actions
            .iter()
            .map(|action| action.id.as_str())
            .collect::<Vec<_>>();
        for entity in &self.entities {
            entity.validate(&action_ids)?;
        }
        for action in &self.actions {
            action.validate()?;
        }
        for area in &self.native_only {
            area.validate()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlOwnedSetting {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub secret: bool,
    pub storage: ManifestControlStorage,
    #[serde(default)]
    pub options: Vec<ManifestControlOption>,
}

impl ManifestControlOwnedSetting {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.id, "control_surface.owned_settings.id")?;
        ensure_non_empty(&self.label, "control_surface.owned_settings.label")?;
        let field_type = self.field_type.trim().to_ascii_lowercase();
        if !matches!(
            field_type.as_str(),
            "text" | "password" | "toggle" | "select" | "number"
        ) {
            bail!(
                "unsupported control_surface.owned_settings.type '{}'; expected text|password|toggle|select|number",
                self.field_type
            );
        }
        if let Some(description) = self.description.as_deref() {
            ensure_non_empty(description, "control_surface.owned_settings.description")?;
        }
        if field_type == "password" && !self.secret {
            bail!(
                "control_surface.owned_settings '{}' must set secret=true for password fields",
                self.id
            );
        }
        if self.secret && !matches!(field_type.as_str(), "text" | "password") {
            bail!(
                "control_surface.owned_settings '{}' uses secret=true but type '{}' is not supported for secret storage",
                self.id,
                self.field_type
            );
        }
        if field_type == "select" && self.options.is_empty() {
            bail!(
                "control_surface.owned_settings '{}' requires options for select fields",
                self.id
            );
        }
        if field_type != "select" && !self.options.is_empty() {
            bail!(
                "control_surface.owned_settings '{}' only supports options for select fields",
                self.id
            );
        }
        self.storage.validate(self.secret, &self.id)?;
        for option in &self.options {
            option.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlStorage {
    #[serde(rename = "type")]
    pub r#type: String,
    pub key: String,
}

impl ManifestControlStorage {
    fn validate(&self, secret: bool, setting_id: &str) -> Result<()> {
        ensure_non_empty(&self.key, "control_surface.owned_settings.storage.key")?;
        let storage_type = self.r#type.trim().to_ascii_lowercase();
        if !matches!(
            storage_type.as_str(),
            "extension_setting" | "instance_setting" | "global_secret" | "instance_secret"
        ) {
            bail!(
                "unsupported control_surface.owned_settings.storage.type '{}'; expected extension_setting|instance_setting|global_secret|instance_secret",
                self.r#type
            );
        }
        let is_secret_storage =
            matches!(storage_type.as_str(), "global_secret" | "instance_secret");
        if secret && !is_secret_storage {
            bail!(
                "control_surface.owned_settings '{}' uses secret=true and must store into instance_secret or global_secret",
                setting_id
            );
        }
        if !secret && is_secret_storage {
            bail!(
                "control_surface.owned_settings '{}' stores into secret scope but secret=false",
                setting_id
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlOption {
    pub value: serde_json::Value,
    pub label: String,
}

impl ManifestControlOption {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.label, "control_surface.owned_settings.options.label")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlObservedState {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
}

impl ManifestControlObservedState {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.id, "control_surface.observed_state.id")?;
        ensure_non_empty(&self.label, "control_surface.observed_state.label")?;
        if let Some(description) = self.description.as_deref() {
            ensure_non_empty(description, "control_surface.observed_state.description")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlEntityCollection {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub item_label: Option<String>,
    #[serde(default)]
    pub actions: Vec<String>,
}

impl ManifestControlEntityCollection {
    fn validate(&self, action_ids: &[&str]) -> Result<()> {
        ensure_non_empty(&self.id, "control_surface.entities.id")?;
        ensure_non_empty(&self.title, "control_surface.entities.title")?;
        if let Some(description) = self.description.as_deref() {
            ensure_non_empty(description, "control_surface.entities.description")?;
        }
        if let Some(item_label) = self.item_label.as_deref() {
            ensure_non_empty(item_label, "control_surface.entities.item_label")?;
        }
        for action in &self.actions {
            ensure_non_empty(action, "control_surface.entities.actions")?;
            if !action_ids.iter().any(|candidate| *candidate == action) {
                bail!(
                    "control_surface.entities '{}' references unknown action '{}'",
                    self.id,
                    action
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlActionDef {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_control_action_target")]
    pub target: String,
    #[serde(default = "default_control_action_kind")]
    pub kind: String,
    #[serde(default)]
    pub confirm_text: Option<String>,
}

impl ManifestControlActionDef {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.id, "control_surface.actions.id")?;
        ensure_non_empty(&self.label, "control_surface.actions.label")?;
        if let Some(description) = self.description.as_deref() {
            ensure_non_empty(description, "control_surface.actions.description")?;
        }
        if let Some(confirm_text) = self.confirm_text.as_deref() {
            ensure_non_empty(confirm_text, "control_surface.actions.confirm_text")?;
        }
        let target = self.target.trim().to_ascii_lowercase();
        if !matches!(target.as_str(), "service" | "entity") {
            bail!(
                "unsupported control_surface.actions.target '{}'; expected service|entity",
                self.target
            );
        }
        let kind = self.kind.trim().to_ascii_lowercase();
        if !matches!(kind.as_str(), "primary" | "secondary" | "danger") {
            bail!(
                "unsupported control_surface.actions.kind '{}'; expected primary|secondary|danger",
                self.kind
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestControlNativeArea {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
}

impl ManifestControlNativeArea {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.id, "control_surface.native_only.id")?;
        ensure_non_empty(&self.title, "control_surface.native_only.title")?;
        if let Some(description) = self.description.as_deref() {
            ensure_non_empty(description, "control_surface.native_only.description")?;
        }
        Ok(())
    }
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

pub fn repair_builtin_manifest_json(raw_json: &mut serde_json::Value) -> bool {
    repair_builtin_manifest_json_for_hot_path_mode(raw_json, prefers_runtime_hot_paths())
}

fn repair_builtin_manifest_json_for_hot_path_mode(
    raw_json: &mut serde_json::Value,
    prefer_runtime_hot_paths: bool,
) -> bool {
    let extension_id = raw_json
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match extension_id {
        "elixir.blueprints.arr_stack" => ensure_string_array_item_after(
            raw_json,
            "connectors",
            "elixir.connectors.nzbget_defaults",
            "elixir.connectors.qbittorrent_defaults",
        ),
        "elixir.connectors.qbittorrent_defaults" => {
            repair_qbittorrent_defaults_manifest(raw_json, prefer_runtime_hot_paths)
        }
        "elixir.connectors.nzbget_defaults" => {
            repair_nzbget_defaults_manifest(raw_json, prefer_runtime_hot_paths)
        }
        _ => false,
    }
}

fn prefers_runtime_hot_paths() -> bool {
    cfg!(any(target_os = "macos", target_os = "windows"))
}

fn repair_qbittorrent_defaults_manifest(
    root: &mut serde_json::Value,
    prefer_runtime_hot_paths: bool,
) -> bool {
    let mut repaired = false;
    repaired |=
        set_driver_patch_string_field(root, "set_preferences", "default_save_path", "/downloads");
    repaired |= set_driver_patch_string_field(
        root,
        "set_preferences",
        "incomplete_path",
        qbittorrent_incomplete_path(prefer_runtime_hot_paths),
    );
    repaired |= set_driver_patch_bool_field(root, "set_preferences", "use_incomplete", true);
    repaired
}

fn repair_nzbget_defaults_manifest(
    root: &mut serde_json::Value,
    prefer_runtime_hot_paths: bool,
) -> bool {
    let mut repaired = false;
    repaired |=
        set_driver_patch_string_field(root, "set_preferences", "default_save_path", "/downloads");
    repaired |= set_driver_patch_string_field(
        root,
        "set_preferences",
        "incomplete_path",
        nzbget_incomplete_path(prefer_runtime_hot_paths),
    );
    repaired |= set_driver_patch_string_field(
        root,
        "set_preferences",
        "nzb_dir",
        nzbget_nzb_dir(prefer_runtime_hot_paths),
    );
    repaired |= set_driver_patch_string_field(
        root,
        "set_preferences",
        "queue_dir",
        nzbget_queue_dir(prefer_runtime_hot_paths),
    );
    repaired |= set_driver_patch_string_field(
        root,
        "set_preferences",
        "temp_dir",
        nzbget_temp_dir(prefer_runtime_hot_paths),
    );
    repaired |= set_driver_patch_bool_field(root, "set_preferences", "use_incomplete", true);
    repaired
}

fn qbittorrent_incomplete_path(prefer_runtime_hot_paths: bool) -> &'static str {
    if prefer_runtime_hot_paths {
        "/runtime/incomplete"
    } else {
        "/downloads/.incomplete"
    }
}

fn nzbget_incomplete_path(prefer_runtime_hot_paths: bool) -> &'static str {
    if prefer_runtime_hot_paths {
        "/runtime/incomplete"
    } else {
        "/downloads/.incomplete"
    }
}

fn nzbget_nzb_dir(prefer_runtime_hot_paths: bool) -> &'static str {
    if prefer_runtime_hot_paths {
        "/runtime/nzb"
    } else {
        "/config/nzb"
    }
}

fn nzbget_queue_dir(prefer_runtime_hot_paths: bool) -> &'static str {
    if prefer_runtime_hot_paths {
        "/runtime/queue"
    } else {
        "/config/queue"
    }
}

fn nzbget_temp_dir(prefer_runtime_hot_paths: bool) -> &'static str {
    if prefer_runtime_hot_paths {
        "/runtime/tmp"
    } else {
        "/config/tmp"
    }
}

fn set_driver_patch_string_field(
    root: &mut serde_json::Value,
    op: &str,
    key: &str,
    desired: &str,
) -> bool {
    let Some(patch) = find_driver_patch_mut(root, op) else {
        return false;
    };
    let Some(object) = patch.as_object_mut() else {
        return false;
    };
    if object.get(key).and_then(serde_json::Value::as_str) == Some(desired) {
        return false;
    }
    object.insert(
        key.to_string(),
        serde_json::Value::String(desired.to_string()),
    );
    true
}

fn set_driver_patch_bool_field(
    root: &mut serde_json::Value,
    op: &str,
    key: &str,
    desired: bool,
) -> bool {
    let Some(patch) = find_driver_patch_mut(root, op) else {
        return false;
    };
    let Some(object) = patch.as_object_mut() else {
        return false;
    };
    if object.get(key).and_then(serde_json::Value::as_bool) == Some(desired) {
        return false;
    }
    object.insert(key.to_string(), serde_json::Value::Bool(desired));
    true
}

fn find_driver_patch_mut<'a>(
    root: &'a mut serde_json::Value,
    op: &str,
) -> Option<&'a mut serde_json::Value> {
    root.get_mut("actions")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|actions| {
            actions.iter_mut().find_map(|action| {
                let patch = action.get_mut("patch")?;
                (patch.get("op").and_then(serde_json::Value::as_str) == Some(op)).then_some(patch)
            })
        })
}

fn ensure_string_array_item_after(
    root: &mut serde_json::Value,
    field: &str,
    value: &str,
    after: &str,
) -> bool {
    let Some(object) = root.as_object_mut() else {
        return false;
    };
    let entry = object
        .entry(field.to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let Some(array) = entry.as_array_mut() else {
        return false;
    };
    if array.iter().any(|item| item.as_str() == Some(value)) {
        return false;
    }
    let insert_index = array
        .iter()
        .position(|item| item.as_str() == Some(after))
        .map(|index| index + 1)
        .unwrap_or(array.len());
    array.insert(insert_index, serde_json::Value::String(value.to_string()));
    true
}

fn default_slot() -> String {
    "default".to_string()
}

fn default_control_surface_adapter() -> String {
    "generic_v1".to_string()
}

fn default_control_action_target() -> String {
    "service".to_string()
}

fn default_control_action_kind() -> String {
    "secondary".to_string()
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

fn ensure_unique_control_ids(ids: &[&str], field: &str) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for value in ids {
        ensure_non_empty(value, field)?;
        let normalized = value.trim().to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            bail!("manifest {} contains duplicate id '{}'", field, value);
        }
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
    use serde_json::json;

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

    #[test]
    fn manifest_accepts_generic_control_surface_contract() {
        let yaml = r#"
id: elixir.modules.community.test
version: 1.0.0
kind: module
name: Community Test
provides:
  - capability: utility.test
    slot: default
    implementation: community-test
runtime:
  type: container
  image: example/community-test:1
control_surface:
  adapter: generic_v1
  owned_settings:
    - id: apiKey
      label: API key
      type: password
      secret: true
      storage:
        type: instance_secret
        key: community_api_key
    - id: mode
      label: Mode
      type: select
      storage:
        type: instance_setting
        key: mode
      options:
        - value: balanced
          label: Balanced
        - value: aggressive
          label: Aggressive
  observed_state:
    - id: status
      label: Status
    - id: itemCount
      label: Items
  actions:
    - id: sync_now
      label: Sync now
      target: service
      kind: primary
    - id: remove_item
      label: Remove
      target: entity
      kind: danger
      confirm_text: Remove this item?
  entities:
    - id: queue
      title: Queue
      item_label: Item
      actions: [remove_item]
  native_only:
    - id: advanced_filters
      title: Advanced filters
      description: Managed only in the native UI.
"#;
        let parsed = parse_manifest_yaml(yaml).expect("manifest should parse");
        let control_surface = parsed
            .manifest
            .control_surface
            .expect("control surface should exist");
        assert_eq!(control_surface.adapter, "generic_v1");
        assert_eq!(control_surface.owned_settings.len(), 2);
        assert_eq!(control_surface.entities.len(), 1);
        assert_eq!(control_surface.actions.len(), 2);
    }

    #[test]
    fn manifest_rejects_secret_control_field_with_non_secret_storage() {
        let yaml = r#"
id: elixir.modules.community.test
version: 1.0.0
kind: module
name: Community Test
provides:
  - capability: utility.test
    slot: default
    implementation: community-test
runtime:
  type: container
  image: example/community-test:1
control_surface:
  owned_settings:
    - id: apiKey
      label: API key
      type: password
      secret: true
      storage:
        type: instance_setting
        key: api_key
"#;
        let err = parse_manifest_yaml(yaml).unwrap_err();
        assert!(err.to_string().contains("instance_secret or global_secret"));
    }

    #[test]
    fn repair_builtin_manifest_adds_missing_arr_stack_nzbget_defaults_connector() {
        let mut raw = json!({
            "id": "elixir.blueprints.arr_stack",
            "connectors": [
                "elixir.connectors.prowlarr_public_indexers",
                "elixir.connectors.qbittorrent_defaults",
                "elixir.connectors.sonarr_qbittorrent",
                "elixir.connectors.sonarr_nzbget"
            ]
        });

        assert!(repair_builtin_manifest_json(&mut raw));

        let connectors = raw
            .get("connectors")
            .and_then(serde_json::Value::as_array)
            .expect("connectors array");
        let values = connectors
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>();

        assert_eq!(
            values,
            vec![
                "elixir.connectors.prowlarr_public_indexers",
                "elixir.connectors.qbittorrent_defaults",
                "elixir.connectors.nzbget_defaults",
                "elixir.connectors.sonarr_qbittorrent",
                "elixir.connectors.sonarr_nzbget"
            ]
        );
    }

    #[test]
    fn repair_builtin_manifest_is_noop_when_arr_stack_nzbget_defaults_already_present() {
        let mut raw = json!({
            "id": "elixir.blueprints.arr_stack",
            "connectors": [
                "elixir.connectors.qbittorrent_defaults",
                "elixir.connectors.nzbget_defaults",
                "elixir.connectors.sonarr_nzbget"
            ]
        });

        assert!(!repair_builtin_manifest_json(&mut raw));
    }

    #[test]
    fn repair_builtin_manifest_updates_qbittorrent_defaults_for_desktop_hot_paths() {
        let mut raw = json!({
            "id": "elixir.connectors.qbittorrent_defaults",
            "actions": [
                {
                    "type": "driver_patch",
                    "patch": {
                        "op": "set_categories",
                        "categories": []
                    }
                },
                {
                    "type": "driver_patch",
                    "patch": {
                        "op": "set_preferences",
                        "default_save_path": "/downloads",
                        "incomplete_path": "/downloads/.incomplete",
                        "use_incomplete": true
                    }
                }
            ]
        });

        assert!(repair_builtin_manifest_json_for_hot_path_mode(
            &mut raw, true
        ));
        assert_eq!(
            raw.pointer("/actions/1/patch/incomplete_path")
                .and_then(serde_json::Value::as_str),
            Some("/runtime/incomplete")
        );
    }

    #[test]
    fn repair_builtin_manifest_updates_nzbget_defaults_for_desktop_hot_paths() {
        let mut raw = json!({
            "id": "elixir.connectors.nzbget_defaults",
            "actions": [
                {
                    "type": "driver_patch",
                    "patch": {
                        "op": "set_categories",
                        "categories": []
                    }
                },
                {
                    "type": "driver_patch",
                    "patch": {
                        "op": "set_preferences",
                        "default_save_path": "/downloads",
                        "incomplete_path": "/downloads/.incomplete",
                        "use_incomplete": true
                    }
                }
            ]
        });

        assert!(repair_builtin_manifest_json_for_hot_path_mode(
            &mut raw, true
        ));
        assert_eq!(
            raw.pointer("/actions/1/patch/incomplete_path")
                .and_then(serde_json::Value::as_str),
            Some("/runtime/incomplete")
        );
        assert_eq!(
            raw.pointer("/actions/1/patch/nzb_dir")
                .and_then(serde_json::Value::as_str),
            Some("/runtime/nzb")
        );
        assert_eq!(
            raw.pointer("/actions/1/patch/queue_dir")
                .and_then(serde_json::Value::as_str),
            Some("/runtime/queue")
        );
        assert_eq!(
            raw.pointer("/actions/1/patch/temp_dir")
                .and_then(serde_json::Value::as_str),
            Some("/runtime/tmp")
        );
    }

    #[test]
    fn repair_builtin_manifest_updates_nzbget_defaults_for_bind_config_hosts() {
        let mut raw = json!({
            "id": "elixir.connectors.nzbget_defaults",
            "actions": [
                {
                    "type": "driver_patch",
                    "patch": {
                        "op": "set_categories",
                        "categories": []
                    }
                },
                {
                    "type": "driver_patch",
                    "patch": {
                        "op": "set_preferences",
                        "default_save_path": "/downloads",
                        "incomplete_path": "/runtime/incomplete",
                        "nzb_dir": "/runtime/nzb",
                        "queue_dir": "/runtime/queue",
                        "temp_dir": "/runtime/tmp",
                        "use_incomplete": true
                    }
                }
            ]
        });

        assert!(repair_builtin_manifest_json_for_hot_path_mode(
            &mut raw, false
        ));
        assert_eq!(
            raw.pointer("/actions/1/patch/incomplete_path")
                .and_then(serde_json::Value::as_str),
            Some("/downloads/.incomplete")
        );
        assert_eq!(
            raw.pointer("/actions/1/patch/nzb_dir")
                .and_then(serde_json::Value::as_str),
            Some("/config/nzb")
        );
        assert_eq!(
            raw.pointer("/actions/1/patch/queue_dir")
                .and_then(serde_json::Value::as_str),
            Some("/config/queue")
        );
        assert_eq!(
            raw.pointer("/actions/1/patch/temp_dir")
                .and_then(serde_json::Value::as_str),
            Some("/config/tmp")
        );
    }
}
