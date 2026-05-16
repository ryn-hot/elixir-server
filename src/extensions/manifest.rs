use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::db::models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality};
use crate::extensions::managed_paths::{
    DOWNLOADS_ROOT, NZBGET_INCOMPLETE_DIR, NZBGET_NZB_DIR, NZBGET_QUEUE_DIR, NZBGET_TEMP_DIR,
    QBITTORRENT_INCOMPLETE_DIR,
};

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
    pub requires: ManifestRequires,
    #[serde(default)]
    pub conflicts: Vec<ManifestConflict>,
    #[serde(default)]
    pub runtime: Option<ManifestRuntime>,
    #[serde(default)]
    pub backup: Option<ManifestBackupPolicy>,
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
    pub execution: Option<ManifestExecution>,
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
            validate_debrid_provider_contract(self, provide)?;
        }

        self.requires.validate()?;

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
                if let Some(backup) = self.backup.as_ref() {
                    backup.validate()?;
                }
            }
            ExtensionKind::Connector => {
                if self.backup.is_some() {
                    bail!("only module manifests may declare backups");
                }
                if self.targets.is_empty() {
                    bail!("connector manifests must declare at least one target");
                }
                if self.actions.is_empty() {
                    bail!("connector manifests must declare at least one action");
                }
            }
            ExtensionKind::Blueprint => {
                if self.backup.is_some() {
                    bail!("only module manifests may declare backups");
                }
                let Some(execution) = self.execution.as_ref() else {
                    bail!("blueprint manifests must declare execution");
                };
                execution.validate()?;
                if !self.wants.is_empty() {
                    bail!("execution blueprints must not declare wants");
                }
                if !self.connectors.is_empty() {
                    bail!("execution blueprints must not declare connectors");
                }
                if self.preferences.is_some() {
                    bail!("execution blueprints must not declare preferences");
                }
                if !self.bindings.is_empty() {
                    bail!("execution blueprints must not declare bindings");
                }
            }
        }

        if let Some(runtime) = &self.runtime {
            ensure_non_empty(&runtime.r#type, "runtime.type")?;
            if runtime.r#type != "container" && runtime.r#type != "internal" {
                bail!("unsupported runtime type '{}'", runtime.r#type);
            }
            if self.backup.is_some() && !runtime.r#type.eq_ignore_ascii_case("container") {
                bail!("backup is only supported for container runtimes");
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
pub struct ManifestBackupPolicy {
    #[serde(default = "default_backup_retention")]
    pub retention: usize,
    #[serde(default)]
    pub items: Vec<ManifestBackupItem>,
}

impl ManifestBackupPolicy {
    fn validate(&self) -> Result<()> {
        if self.retention == 0 {
            bail!("backup.retention must be greater than zero");
        }
        if self.items.is_empty() {
            bail!("backup.items must declare at least one backup target");
        }
        let mut ids = std::collections::HashSet::new();
        let mut paths = std::collections::HashSet::new();
        for item in &self.items {
            item.validate()?;
            if !ids.insert(item.id.clone()) {
                bail!("backup.items ids must be unique");
            }
            if !paths.insert(item.container_path.clone()) {
                bail!("backup.items container_path values must be unique");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestBackupItem {
    pub id: String,
    pub label: String,
    #[serde(default = "default_backup_item_kind")]
    pub kind: String,
    pub container_path: String,
}

impl ManifestBackupItem {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.id, "backup.items.id")?;
        ensure_non_empty(&self.label, "backup.items.label")?;
        ensure_non_empty(&self.kind, "backup.items.kind")?;
        ensure_non_empty(&self.container_path, "backup.items.container_path")?;
        if !self.kind.trim().eq_ignore_ascii_case("directory") {
            bail!(
                "unsupported backup.items.kind '{}'; expected directory",
                self.kind
            );
        }
        if !self.container_path.starts_with('/') {
            bail!("backup.items.container_path must be absolute");
        }
        if self.container_path == "/" {
            bail!("backup.items.container_path must not be '/'");
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

#[derive(Debug, Clone, Serialize, Default)]
pub struct ManifestRequires {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<ManifestRequire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub downloads: Vec<ManifestDownloadRequire>,
}

impl ManifestRequires {
    pub fn iter(&self) -> std::slice::Iter<'_, ManifestRequire> {
        self.capabilities.iter()
    }

    fn validate(&self) -> Result<()> {
        for require in &self.capabilities {
            ensure_non_empty(&require.capability, "requires.capability")?;
            ensure_non_empty(&require.slot, "requires.slot")?;
        }

        let mut logical_ids = std::collections::HashSet::new();
        for download in &self.downloads {
            download.validate()?;
            if !logical_ids.insert(download.resolved_logical_id().to_string()) {
                bail!(
                    "requires.downloads logical_id '{}' is declared more than once",
                    download.resolved_logical_id()
                );
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ManifestRequires {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawRequires {
            Capabilities(Vec<ManifestRequire>),
            Structured {
                #[serde(default)]
                capabilities: Vec<ManifestRequire>,
                #[serde(default)]
                downloads: Vec<ManifestDownloadRequire>,
            },
        }

        match RawRequires::deserialize(deserializer)? {
            RawRequires::Capabilities(capabilities) => Ok(Self {
                capabilities,
                downloads: Vec::new(),
            }),
            RawRequires::Structured {
                capabilities,
                downloads,
            } => Ok(Self {
                capabilities,
                downloads,
            }),
        }
    }
}

impl<'a> IntoIterator for &'a ManifestRequires {
    type Item = &'a ManifestRequire;
    type IntoIter = std::slice::Iter<'a, ManifestRequire>;

    fn into_iter(self) -> Self::IntoIter {
        self.capabilities.iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManifestDownloadKind {
    Torrent,
    Usenet,
    Debrid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManifestDownloadMode {
    Broker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestDownloadRequire {
    pub kind: ManifestDownloadKind,
    #[serde(default = "default_download_mode")]
    pub mode: ManifestDownloadMode,
    #[serde(default)]
    pub logical_id: Option<String>,
    #[serde(default)]
    pub optional: bool,
}

impl ManifestDownloadRequire {
    fn validate(&self) -> Result<()> {
        let expected = self.default_logical_id();
        if let Some(logical_id) = self.logical_id.as_deref() {
            ensure_non_empty(logical_id, "requires.downloads.logical_id")?;
            if logical_id.trim() != expected {
                bail!(
                    "requires.downloads logical_id '{}' does not match kind '{:?}'; expected '{}'",
                    logical_id,
                    self.kind,
                    expected
                );
            }
        }
        Ok(())
    }

    pub fn resolved_logical_id(&self) -> &str {
        self.logical_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| self.default_logical_id())
    }

    fn default_logical_id(&self) -> &'static str {
        match self.kind {
            ManifestDownloadKind::Torrent => "downloaders.torrent.default",
            ManifestDownloadKind::Usenet => "downloaders.usenet.default",
            ManifestDownloadKind::Debrid => "acquisition.debrid.default",
        }
    }
}

fn default_download_mode() -> ManifestDownloadMode {
    ManifestDownloadMode::Broker
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
    #[serde(default)]
    pub download_broker: Option<ManifestDownloadBrokerProviderScope>,
    #[serde(default)]
    pub broker: Option<ManifestDownloadBrokerProviderScope>,
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
        if let Some(scope) = self.download_broker.as_ref() {
            scope.validate("provides.scope.download_broker")?;
        }
        if let Some(scope) = self.broker.as_ref() {
            scope.validate("provides.scope.broker")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManifestDownloadBrokerProviderScope {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub provider_kind: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub logical_id: Option<String>,
    #[serde(default)]
    pub capabilities: Option<serde_json::Value>,
}

impl ManifestDownloadBrokerProviderScope {
    fn validate(&self, prefix: &str) -> Result<()> {
        if let Some(provider_kind) = self
            .provider_kind
            .as_deref()
            .or(self.kind.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if !matches!(provider_kind, "managed" | "external" | "debrid") {
                bail!(
                    "unsupported {prefix}.provider_kind '{}'; expected managed|external|debrid",
                    provider_kind
                );
            }
        }
        if let Some(logical_id) = self.logical_id.as_deref() {
            ensure_non_empty(logical_id, &format!("{prefix}.logical_id"))?;
            if !matches!(
                logical_id.trim(),
                "downloaders.torrent.default"
                    | "downloaders.usenet.default"
                    | "acquisition.debrid.default"
            ) {
                bail!(
                    "unsupported {prefix}.logical_id '{}'; expected a known logical downloader id",
                    logical_id
                );
            }
        }
        if let Some(capabilities) = self.capabilities.as_ref()
            && !capabilities.is_object()
        {
            bail!("{prefix}.capabilities must be an object");
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

fn default_backup_retention() -> usize {
    5
}

fn default_backup_item_kind() -> String {
    "directory".to_string()
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
pub struct ManifestExecution {
    #[serde(default)]
    pub packages: Vec<String>,
    #[serde(default)]
    pub instances: Vec<ManifestExecutionInstance>,
    #[serde(default)]
    pub ownership: Vec<ManifestOwnershipDomain>,
    #[serde(default)]
    pub phases: Vec<ManifestExecutionPhase>,
}

impl ManifestExecution {
    fn validate(&self) -> Result<()> {
        if self.packages.is_empty() {
            bail!("execution.packages must declare at least one package");
        }
        if self.instances.is_empty() {
            bail!("execution.instances must declare at least one instance");
        }
        if self.phases.is_empty() {
            bail!("execution.phases must declare at least one phase");
        }

        let mut packages = std::collections::HashSet::new();
        for package in &self.packages {
            ensure_non_empty(package, "execution.packages")?;
            if !packages.insert(package.trim().to_string()) {
                bail!("execution.packages must be unique");
            }
        }

        let mut instance_ids = std::collections::HashSet::new();
        for instance in &self.instances {
            instance.validate(&packages)?;
            if !instance_ids.insert(instance.id.clone()) {
                bail!("execution.instances ids must be unique");
            }
        }

        let mut phase_ids = std::collections::HashSet::new();
        let mut ownership_domains = std::collections::HashSet::new();
        let mut ownership_by_domain = std::collections::HashMap::new();
        for entry in &self.ownership {
            entry.validate(&packages)?;
            if !ownership_domains.insert(entry.domain.clone()) {
                bail!("ownership domains must be unique");
            }
            ownership_by_domain.insert(entry.domain.clone(), entry.owner.clone());
        }

        for phase in &self.phases {
            if !phase_ids.insert(phase.id.clone()) {
                bail!("execution.phases ids must be unique");
            }
            phase.validate(&instance_ids, &packages, &ownership_by_domain)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestOwnershipDomain {
    pub domain: String,
    pub owner: String,
}

impl ManifestOwnershipDomain {
    fn validate(&self, packages: &std::collections::HashSet<String>) -> Result<()> {
        ensure_non_empty(&self.domain, "ownership.domain")?;
        ensure_non_empty(&self.owner, "ownership.owner")?;
        if !packages.contains(self.owner.trim()) {
            bail!(
                "ownership domain '{}' references owner '{}' that is not declared in execution.packages",
                self.domain,
                self.owner
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestExecutionInstance {
    pub id: String,
    pub extension_id: String,
    #[serde(default = "default_instance_name")]
    pub name: String,
    #[serde(default)]
    pub config: Option<serde_json::Value>,
}

impl ManifestExecutionInstance {
    fn validate(&self, packages: &std::collections::HashSet<String>) -> Result<()> {
        ensure_non_empty(&self.id, "execution.instances.id")?;
        ensure_non_empty(&self.extension_id, "execution.instances.extension_id")?;
        ensure_non_empty(&self.name, "execution.instances.name")?;
        if !packages.contains(self.extension_id.trim()) {
            bail!(
                "execution.instances '{}' references package '{}' that is not declared in execution.packages",
                self.id,
                self.extension_id
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestExecutionPhase {
    pub id: String,
    #[serde(default = "default_true")]
    pub barrier: bool,
    #[serde(default)]
    pub steps: Vec<ManifestExecutionStep>,
}

impl ManifestExecutionPhase {
    fn validate(
        &self,
        instance_ids: &std::collections::HashSet<String>,
        packages: &std::collections::HashSet<String>,
        ownership_by_domain: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        ensure_non_empty(&self.id, "execution.phases.id")?;
        if self.steps.is_empty() {
            bail!(
                "execution.phases '{}' must declare at least one step",
                self.id
            );
        }
        for step in &self.steps {
            step.validate(instance_ids, packages, ownership_by_domain)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ManifestExecutionStep {
    EnsurePackageInstalled {
        extension_id: String,
    },
    EnsureInstanceInstalled {
        instance: String,
    },
    EnsureRuntimeRunning {
        instance: String,
    },
    InstallRuntimeAsset {
        source_extension_id: String,
        source_path: String,
        target_instance: String,
        destination_path: String,
    },
    RestartRuntime {
        instance: String,
    },
    CreateOrUpdateProviders {
        instance: String,
    },
    TransportGate {
        instance: String,
        capability: String,
        #[serde(default = "default_slot")]
        slot: String,
        #[serde(default = "default_health_gate_timeout")]
        timeout_seconds: u64,
    },
    BootstrapGate {
        instance: String,
        capability: String,
        #[serde(default = "default_slot")]
        slot: String,
        #[serde(default = "default_health_gate_timeout")]
        timeout_seconds: u64,
    },
    HealthGate {
        instance: String,
        capability: String,
        #[serde(default = "default_slot")]
        slot: String,
        #[serde(default = "default_health_gate_timeout")]
        timeout_seconds: u64,
    },
    ApplyConnector {
        connector_id: String,
        target_instance: String,
        target_capability: String,
        #[serde(default = "default_slot")]
        target_slot: String,
        #[serde(default)]
        ownership_domains: Vec<String>,
    },
    ApplyBinding {
        consumer_instance: String,
        consumer_capability: String,
        #[serde(default = "default_slot")]
        consumer_slot: String,
        provider_instance: String,
        provider_capability: String,
        #[serde(default = "default_slot")]
        provider_slot: String,
        #[serde(default)]
        reverse_probe: bool,
    },
}

impl ManifestExecutionStep {
    fn validate(
        &self,
        instance_ids: &std::collections::HashSet<String>,
        packages: &std::collections::HashSet<String>,
        ownership_by_domain: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        match self {
            Self::EnsurePackageInstalled { extension_id } => {
                ensure_non_empty(extension_id, "execution.steps.extension_id")?;
                if !packages.contains(extension_id.trim()) {
                    bail!(
                        "execution step references package '{}' that is not declared in execution.packages",
                        extension_id
                    );
                }
            }
            Self::EnsureInstanceInstalled { instance }
            | Self::EnsureRuntimeRunning { instance }
            | Self::RestartRuntime { instance }
            | Self::CreateOrUpdateProviders { instance } => {
                ensure_known_execution_instance(
                    instance_ids,
                    instance,
                    "execution.steps.instance",
                )?;
            }
            Self::InstallRuntimeAsset {
                source_extension_id,
                source_path,
                target_instance,
                destination_path,
            } => {
                ensure_non_empty(source_extension_id, "execution.steps.source_extension_id")?;
                if !packages.contains(source_extension_id.trim()) {
                    bail!(
                        "execution step references package '{}' that is not declared in execution.packages",
                        source_extension_id
                    );
                }
                ensure_relative_runtime_asset_path(source_path, "execution.steps.source_path")?;
                ensure_known_execution_instance(
                    instance_ids,
                    target_instance,
                    "execution.steps.target_instance",
                )?;
                ensure_absolute_runtime_path(destination_path, "execution.steps.destination_path")?;
            }
            Self::TransportGate {
                instance,
                capability,
                slot,
                ..
            }
            | Self::BootstrapGate {
                instance,
                capability,
                slot,
                ..
            }
            | Self::HealthGate {
                instance,
                capability,
                slot,
                ..
            } => {
                ensure_known_execution_instance(
                    instance_ids,
                    instance,
                    "execution.steps.instance",
                )?;
                ensure_non_empty(capability, "execution.steps.capability")?;
                ensure_non_empty(slot, "execution.steps.slot")?;
            }
            Self::ApplyConnector {
                connector_id,
                target_instance,
                target_capability,
                target_slot,
                ownership_domains,
            } => {
                ensure_non_empty(connector_id, "execution.steps.connector_id")?;
                if !packages.contains(connector_id.trim()) {
                    bail!(
                        "execution step references connector '{}' that is not declared in execution.packages",
                        connector_id
                    );
                }
                ensure_known_execution_instance(
                    instance_ids,
                    target_instance,
                    "execution.steps.target_instance",
                )?;
                ensure_non_empty(target_capability, "execution.steps.target_capability")?;
                ensure_non_empty(target_slot, "execution.steps.target_slot")?;
                if ownership_domains.is_empty() {
                    bail!(
                        "execution apply_connector '{}' must declare ownership_domains",
                        connector_id
                    );
                }
                let mut seen_domains = std::collections::HashSet::new();
                for domain in ownership_domains {
                    ensure_non_empty(domain, "execution.steps.ownership_domains")?;
                    if !seen_domains.insert(domain.clone()) {
                        bail!(
                            "execution apply_connector '{}' must not repeat ownership domain '{}'",
                            connector_id,
                            domain
                        );
                    }
                    let Some(owner) = ownership_by_domain.get(domain) else {
                        bail!(
                            "execution apply_connector '{}' references undeclared ownership domain '{}'",
                            connector_id,
                            domain
                        );
                    };
                    if owner != connector_id {
                        bail!(
                            "execution apply_connector '{}' claims ownership domain '{}' owned by '{}'",
                            connector_id,
                            domain,
                            owner
                        );
                    }
                }
            }
            Self::ApplyBinding {
                consumer_instance,
                consumer_capability,
                consumer_slot,
                provider_instance,
                provider_capability,
                provider_slot,
                ..
            } => {
                ensure_known_execution_instance(
                    instance_ids,
                    consumer_instance,
                    "execution.steps.consumer_instance",
                )?;
                ensure_non_empty(consumer_capability, "execution.steps.consumer_capability")?;
                ensure_non_empty(consumer_slot, "execution.steps.consumer_slot")?;
                ensure_known_execution_instance(
                    instance_ids,
                    provider_instance,
                    "execution.steps.provider_instance",
                )?;
                ensure_non_empty(provider_capability, "execution.steps.provider_capability")?;
                ensure_non_empty(provider_slot, "execution.steps.provider_slot")?;
            }
        }
        Ok(())
    }
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
    #[serde(default = "default_control_owned_setting_ownership")]
    pub ownership: String,
    pub storage: ManifestControlStorage,
    #[serde(default)]
    pub options: Vec<ManifestControlOption>,
}

impl ManifestControlOwnedSetting {
    pub fn ownership_mode(&self) -> &str {
        self.ownership.trim()
    }

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
        let ownership = self.ownership.trim().to_ascii_lowercase();
        if !matches!(ownership.as_str(), "seeded" | "managed") {
            bail!(
                "unsupported control_surface.owned_settings.ownership '{}'; expected seeded|managed",
                self.ownership
            );
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
        "elixir.modules.qbittorrent" => repair_downloader_broker_scope(
            raw_json,
            "downloader.torrent",
            "managed",
            "downloaders.torrent.default",
        ),
        "elixir.modules.nzbget" => repair_downloader_broker_scope(
            raw_json,
            "downloader.nzb",
            "managed",
            "downloaders.usenet.default",
        ),
        "elixir.modules.sonarr" | "elixir.modules.radarr" => {
            repair_manager_download_requirements(raw_json)
        }
        "elixir.connectors.prowlarr_sonarr_app" | "elixir.connectors.prowlarr_radarr_app" => {
            remove_driver_patch_app_tags(raw_json)
        }
        "elixir.connectors.qbittorrent_defaults" => repair_qbittorrent_defaults_manifest(raw_json),
        "elixir.connectors.nzbget_defaults" => repair_nzbget_defaults_manifest(raw_json),
        _ => false,
    }
}

fn repair_qbittorrent_defaults_manifest(root: &mut serde_json::Value) -> bool {
    let mut repaired = false;
    repaired |=
        set_driver_patch_string_field(root, "set_preferences", "default_save_path", DOWNLOADS_ROOT);
    repaired |= set_driver_patch_string_field(
        root,
        "set_preferences",
        "incomplete_path",
        QBITTORRENT_INCOMPLETE_DIR,
    );
    repaired |= set_driver_patch_bool_field(root, "set_preferences", "use_incomplete", true);
    repaired
}

fn repair_nzbget_defaults_manifest(root: &mut serde_json::Value) -> bool {
    let mut repaired = false;
    repaired |=
        set_driver_patch_string_field(root, "set_preferences", "default_save_path", DOWNLOADS_ROOT);
    repaired |= set_driver_patch_string_field(
        root,
        "set_preferences",
        "incomplete_path",
        NZBGET_INCOMPLETE_DIR,
    );
    repaired |= set_driver_patch_string_field(root, "set_preferences", "nzb_dir", NZBGET_NZB_DIR);
    repaired |=
        set_driver_patch_string_field(root, "set_preferences", "queue_dir", NZBGET_QUEUE_DIR);
    repaired |= set_driver_patch_string_field(root, "set_preferences", "temp_dir", NZBGET_TEMP_DIR);
    repaired |= set_driver_patch_bool_field(root, "set_preferences", "use_incomplete", true);
    repaired
}

fn repair_downloader_broker_scope(
    root: &mut serde_json::Value,
    capability: &str,
    provider_kind: &str,
    logical_id: &str,
) -> bool {
    let Some(provides) = root
        .get_mut("provides")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return false;
    };
    let mut repaired = false;
    for provide in provides {
        if provide
            .get("capability")
            .and_then(serde_json::Value::as_str)
            != Some(capability)
        {
            continue;
        }
        let Some(scope) = ensure_json_object_field(provide, "scope", &mut repaired) else {
            continue;
        };
        let broker = scope
            .entry("download_broker".to_string())
            .or_insert_with(|| {
                repaired = true;
                serde_json::Value::Object(serde_json::Map::new())
            });
        if !broker.is_object() {
            *broker = serde_json::Value::Object(serde_json::Map::new());
            repaired = true;
        }
        let Some(broker) = broker.as_object_mut() else {
            continue;
        };
        repaired |= set_json_bool_field(broker, "enabled", true);
        repaired |= set_json_string_field(broker, "provider_kind", provider_kind);
        repaired |= set_json_string_field(broker, "logical_id", logical_id);
    }
    repaired
}

fn repair_manager_download_requirements(root: &mut serde_json::Value) -> bool {
    let desired = [
        ("torrent", "downloaders.torrent.default"),
        ("usenet", "downloaders.usenet.default"),
    ];
    let Some(root_object) = root.as_object_mut() else {
        return false;
    };
    let mut repaired = false;
    let requires = root_object
        .entry("requires".to_string())
        .or_insert_with(|| {
            repaired = true;
            serde_json::Value::Object(serde_json::Map::new())
        });
    if requires.is_array() {
        let legacy = std::mem::replace(requires, serde_json::Value::Object(serde_json::Map::new()));
        if let Some(object) = requires.as_object_mut() {
            if legacy
                .as_array()
                .map(|items| !items.is_empty())
                .unwrap_or(false)
            {
                object.insert("capabilities".to_string(), legacy);
            }
        }
        repaired = true;
    }
    if !requires.is_object() {
        *requires = serde_json::Value::Object(serde_json::Map::new());
        repaired = true;
    }
    let Some(requires) = requires.as_object_mut() else {
        return repaired;
    };
    let downloads = requires.entry("downloads".to_string()).or_insert_with(|| {
        repaired = true;
        serde_json::Value::Array(Vec::new())
    });
    if !downloads.is_array() {
        *downloads = serde_json::Value::Array(Vec::new());
        repaired = true;
    }
    let Some(downloads) = downloads.as_array_mut() else {
        return repaired;
    };
    for (kind, logical_id) in desired {
        if downloads.iter().any(|item| {
            item.get("kind").and_then(serde_json::Value::as_str) == Some(kind)
                || item.get("logical_id").and_then(serde_json::Value::as_str) == Some(logical_id)
        }) {
            continue;
        }
        downloads.push(serde_json::json!({
            "kind": kind,
            "mode": "broker",
            "logical_id": logical_id
        }));
        repaired = true;
    }
    repaired
}

fn ensure_json_object_field<'a>(
    value: &'a mut serde_json::Value,
    key: &str,
    repaired: &mut bool,
) -> Option<&'a mut serde_json::Map<String, serde_json::Value>> {
    let object = value.as_object_mut()?;
    let child = object.entry(key.to_string()).or_insert_with(|| {
        *repaired = true;
        serde_json::Value::Object(serde_json::Map::new())
    });
    if !child.is_object() {
        *child = serde_json::Value::Object(serde_json::Map::new());
        *repaired = true;
    }
    child.as_object_mut()
}

fn set_json_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    desired: &str,
) -> bool {
    if object.get(key).and_then(serde_json::Value::as_str) == Some(desired) {
        return false;
    }
    object.insert(
        key.to_string(),
        serde_json::Value::String(desired.to_string()),
    );
    true
}

fn set_json_bool_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    desired: bool,
) -> bool {
    if object.get(key).and_then(serde_json::Value::as_bool) == Some(desired) {
        return false;
    }
    object.insert(key.to_string(), serde_json::Value::Bool(desired));
    true
}

fn remove_driver_patch_app_tags(root: &mut serde_json::Value) -> bool {
    let Some(actions) = root
        .get_mut("actions")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return false;
    };
    let mut repaired = false;
    for action in actions {
        let Some(patch) = action.get_mut("patch") else {
            continue;
        };
        let op = patch
            .get("op")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match op {
            "register_app" => {
                if let Some(app) = patch
                    .get_mut("app")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    repaired |= app.remove("tags").is_some();
                }
            }
            "register_apps" => {
                if let Some(apps) = patch
                    .get_mut("apps")
                    .and_then(serde_json::Value::as_array_mut)
                {
                    for app in apps {
                        if let Some(object) = app.as_object_mut() {
                            repaired |= object.remove("tags").is_some();
                        }
                    }
                }
            }
            _ => {}
        }
    }
    repaired
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

fn default_instance_name() -> String {
    "default".to_string()
}

fn default_health_gate_timeout() -> u64 {
    60
}

fn default_control_surface_adapter() -> String {
    "generic_v1".to_string()
}

fn default_control_owned_setting_ownership() -> String {
    "managed".to_string()
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

fn ensure_relative_runtime_asset_path(value: &str, field: &str) -> Result<()> {
    ensure_non_empty(value, field)?;
    let path = Path::new(value);
    if path.is_absolute() {
        bail!("manifest {} must be relative", field);
    }
    if path.components().any(|component| {
        matches!(
            component,
            std::path::Component::ParentDir | std::path::Component::RootDir
        )
    }) {
        bail!("manifest {} must not escape its package root", field);
    }
    Ok(())
}

fn ensure_absolute_runtime_path(value: &str, field: &str) -> Result<()> {
    ensure_non_empty(value, field)?;
    let path = Path::new(value);
    if !path.is_absolute() {
        bail!("manifest {} must be absolute", field);
    }
    if path == Path::new("/") {
        bail!("manifest {} must not be '/'", field);
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

fn ensure_known_execution_instance(
    instance_ids: &std::collections::HashSet<String>,
    value: &str,
    field: &str,
) -> Result<()> {
    ensure_non_empty(value, field)?;
    if !instance_ids.contains(value.trim()) {
        bail!(
            "manifest {} references unknown execution instance '{}'",
            field,
            value
        );
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

fn validate_debrid_provider_contract(
    manifest: &ExtensionManifest,
    provide: &ManifestProvide,
) -> Result<()> {
    if !provide
        .capability
        .trim()
        .eq_ignore_ascii_case("debrid.resolver")
    {
        return Ok(());
    }
    let implementation = provide
        .implementation
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("debrid.resolver providers must declare provides.implementation")
        })?;
    if implementation.contains(char::is_whitespace) {
        bail!("debrid.resolver provides.implementation must be a stable id without whitespace");
    }
    let scope = provide
        .scope
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("debrid.resolver providers must declare provides.scope"))?;
    let broker = scope
        .download_broker
        .as_ref()
        .or(scope.broker.as_ref())
        .ok_or_else(|| {
            anyhow::anyhow!("debrid.resolver providers must declare provides.scope.download_broker")
        })?;
    if broker.enabled == Some(false) {
        bail!("debrid.resolver providers cannot disable provides.scope.download_broker");
    }
    let provider_kind = broker
        .provider_kind
        .as_deref()
        .or(broker.kind.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "debrid.resolver providers must declare provides.scope.download_broker.provider_kind"
            )
        })?;
    if provider_kind != "debrid" {
        bail!(
            "debrid.resolver providers must use provides.scope.download_broker.provider_kind 'debrid'"
        );
    }
    let logical_id = broker
        .logical_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "debrid.resolver providers must declare provides.scope.download_broker.logical_id"
            )
        })?;
    if logical_id != "acquisition.debrid.default" {
        bail!("debrid.resolver providers must bind to logical route 'acquisition.debrid.default'");
    }
    if provide.endpoint.is_none() && manifest.networking.is_none() {
        bail!(
            "debrid.resolver providers must declare an endpoint or module networking service port"
        );
    }
    if provide.healthcheck.is_none() {
        bail!("debrid.resolver providers must declare a healthcheck for readiness");
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
    use serde_json::{Value, json};

    #[test]
    fn manifest_accepts_container_backup_policy() {
        let yaml = r#"
id: elixir.modules.test
version: 1.0.0
kind: module
name: Test
provides:
  - capability: media.manager.tv
    slot: default
    implementation: test
    scope:
      media_types: ["series"]
      actions: ["add", "monitor", "search"]
runtime:
  type: container
  image: test:latest
  network: elixir_net
  volumes:
    - "{data}/test:/config"
backup:
  retention: 3
  items:
    - id: config
      label: Test config
      container_path: /config
"#;
        let manifest = parse_manifest_yaml(yaml).expect("manifest should parse");
        let backup = manifest
            .manifest
            .backup
            .expect("manifest should include backup policy");
        assert_eq!(backup.retention, 3);
        assert_eq!(backup.items.len(), 1);
        assert_eq!(backup.items[0].container_path, "/config");
    }

    #[test]
    fn manifest_rejects_relative_backup_path() {
        let yaml = r#"
id: elixir.modules.test
version: 1.0.0
kind: module
name: Test
provides:
  - capability: media.manager.movies
    slot: default
    implementation: test
    scope:
      media_types: ["movie"]
      actions: ["add", "monitor", "search"]
runtime:
  type: container
  image: test:latest
  network: elixir_net
backup:
  items:
    - id: config
      label: Test config
      container_path: config
"#;
        let err =
            parse_manifest_yaml(yaml).expect_err("manifest should reject relative backup paths");
        assert!(
            err.to_string()
                .contains("backup.items.container_path must be absolute")
        );
    }

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
    fn manifest_accepts_logical_download_requirements() {
        let yaml = r#"
id: elixir.modules.test
version: 1.0.0
kind: module
name: Test Manager
provides:
  - capability: media.manager.movies
    slot: default
    implementation: test
    scope:
      media_types: ["movies"]
      actions: ["add", "monitor"]
requires:
  downloads:
    - kind: torrent
      mode: broker
    - kind: usenet
      mode: broker
      logical_id: downloaders.usenet.default
runtime:
  type: container
  image: example/test:1
"#;
        let parsed = parse_manifest_yaml(yaml).expect("manifest should parse");
        assert_eq!(parsed.manifest.requires.downloads.len(), 2);
        assert_eq!(
            parsed.manifest.requires.downloads[0].resolved_logical_id(),
            "downloaders.torrent.default"
        );
        assert_eq!(
            parsed.manifest.requires.downloads[1].resolved_logical_id(),
            "downloaders.usenet.default"
        );
        assert!(parsed.manifest.requires.capabilities.is_empty());
    }

    #[test]
    fn manifest_preserves_legacy_capability_requires() {
        let yaml = r#"
id: elixir.connectors.test
version: 1.0.0
kind: connector
name: Test Connector
targets:
  - capability: media.manager.movies
    slot: default
requires:
  - capability: downloader.torrent
    slot: default
actions:
  - type: driver_patch
    target:
      capability: media.manager.movies
      slot: default
    patch:
      op: noop
"#;
        let parsed = parse_manifest_yaml(yaml).expect("manifest should parse");
        assert_eq!(parsed.manifest.requires.capabilities.len(), 1);
        assert_eq!(
            parsed.manifest.requires.capabilities[0].capability,
            "downloader.torrent"
        );
        assert!(parsed.manifest.requires.downloads.is_empty());
    }

    #[test]
    fn manifest_rejects_download_requirement_with_wrong_logical_id() {
        let yaml = r#"
id: elixir.modules.test
version: 1.0.0
kind: module
name: Test Manager
provides:
  - capability: media.manager.movies
    slot: default
    implementation: test
    scope:
      media_types: ["movies"]
      actions: ["add", "monitor"]
requires:
  downloads:
    - kind: torrent
      mode: broker
      logical_id: downloaders.usenet.default
runtime:
  type: container
  image: example/test:1
"#;
        let err = parse_manifest_yaml(yaml).unwrap_err();
        assert!(err.to_string().contains("does not match kind"));
    }

    #[test]
    fn manifest_accepts_debrid_resolver_provider_contract() {
        let yaml = r#"
id: elixir.modules.premiumize
version: 1.0.0
kind: module
name: Premiumize
provides:
  - capability: debrid.resolver
    slot: default
    implementation: premiumize
    scope:
      requires_account: true
      required_fields: ["api_token"]
      download_broker:
        enabled: true
        provider_kind: debrid
        logical_id: acquisition.debrid.default
        capabilities:
          magnetSubmit: true
          hosterUnrestrict: true
          fileListing: true
          fileSelection: true
          fileSelectionMode: before_transfer
    endpoint:
      type: http
      scheme: http
      port: 8080
      base_path: /api
    healthcheck:
      type: http
      path: /health
runtime:
  type: container
  image: example/premiumize:1
  network: elixir_net
"#;
        let parsed = parse_manifest_yaml(yaml).expect("debrid manifest should parse");
        let provide = &parsed.manifest.provides[0];
        assert_eq!(provide.capability, "debrid.resolver");
        let scope = provide.scope.as_ref().expect("scope");
        let broker = scope.download_broker.as_ref().expect("broker scope");
        assert_eq!(broker.provider_kind.as_deref(), Some("debrid"));
        assert_eq!(
            broker.logical_id.as_deref(),
            Some("acquisition.debrid.default")
        );
        assert!(broker.capabilities.as_ref().is_some_and(Value::is_object));
    }

    #[test]
    fn manifest_rejects_debrid_resolver_without_route_contract() {
        let yaml = r#"
id: elixir.modules.bad_debrid
version: 1.0.0
kind: module
name: Bad Debrid
provides:
  - capability: debrid.resolver
    slot: default
    implementation: bad_debrid
runtime:
  type: container
  image: example/bad-debrid:1
"#;
        let err = parse_manifest_yaml(yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("debrid.resolver providers must declare provides.scope")
        );
    }

    #[test]
    fn manifest_rejects_debrid_resolver_with_ambiguous_broker_kind() {
        let yaml = r#"
id: elixir.modules.bad_debrid
version: 1.0.0
kind: module
name: Bad Debrid
provides:
  - capability: debrid.resolver
    slot: default
    implementation: bad_debrid
    scope:
      download_broker:
        provider_kind: external
        logical_id: acquisition.debrid.default
    endpoint:
      type: http
      scheme: http
      port: 8080
    healthcheck:
      type: http
      path: /health
runtime:
  type: container
  image: example/bad-debrid:1
"#;
        let err = parse_manifest_yaml(yaml).unwrap_err();
        assert!(err.to_string().contains("provider_kind 'debrid'"));
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
                "elixir.connectors.prowlarr_sonarr_app",
                "elixir.connectors.prowlarr_radarr_app",
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
                "elixir.connectors.prowlarr_sonarr_app",
                "elixir.connectors.prowlarr_radarr_app",
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
                "elixir.connectors.prowlarr_radarr_app",
                "elixir.connectors.sonarr_nzbget"
            ]
        });

        assert!(!repair_builtin_manifest_json(&mut raw));
    }

    #[test]
    fn repair_builtin_manifest_updates_qbittorrent_defaults_to_downloads_incomplete_path() {
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
                        "incomplete_path": "/runtime/incomplete",
                        "use_incomplete": true
                    }
                }
            ]
        });

        assert!(repair_builtin_manifest_json(&mut raw));
        assert_eq!(
            raw.pointer("/actions/1/patch/incomplete_path")
                .and_then(serde_json::Value::as_str),
            Some("/downloads/.incomplete")
        );
    }

    #[test]
    fn repair_builtin_manifest_updates_nzbget_defaults_to_runtime_paths() {
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

        assert!(repair_builtin_manifest_json(&mut raw));
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
    fn repair_builtin_manifest_keeps_nzbget_defaults_on_runtime_paths() {
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

        assert!(!repair_builtin_manifest_json(&mut raw));
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
    fn repair_builtin_manifest_removes_prowlarr_app_tag_filters() {
        for extension_id in [
            "elixir.connectors.prowlarr_sonarr_app",
            "elixir.connectors.prowlarr_radarr_app",
        ] {
            let mut raw = json!({
                "id": extension_id,
                "actions": [
                    {
                        "type": "driver_patch",
                        "patch": {
                            "op": "register_apps",
                            "apps": [
                                {
                                    "name": "App",
                                    "implementation": "App",
                                    "url": "http://example:1234",
                                    "categories": ["5000"],
                                    "tags": ["elixir"],
                                    "enabled": true
                                }
                            ]
                        }
                    }
                ]
            });

            assert!(repair_builtin_manifest_json(&mut raw));
            assert!(
                raw.pointer("/actions/0/patch/apps/0/tags").is_none(),
                "app tags should be removed for {extension_id}"
            );
        }
    }
}
