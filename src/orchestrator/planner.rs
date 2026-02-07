use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::models::{
    BindingStatus, ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality,
};
use crate::extensions::manifest::{
    ConflictPolicy as ManifestConflictPolicy, ExtensionManifest, ManifestBinding,
    ManifestCapabilityRef, ManifestPolicies, ManifestPreferences, ManifestRuntime,
    ManifestNetworking,
};
use crate::drivers::DriverPatch;
use crate::drivers::{
    DownloaderSpec, IndexerRegistryPatch, MediaManagerMoviesPatch, MediaManagerTvPatch,
};
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_runtime,
};
use crate::extensions::store::{ExtensionStore, NewBinding, ProviderDetails};
use crate::orchestrator::executor::ExecutorAction;
use crate::orchestrator::naming::build_aliases;
use crate::orchestrator::model::ProviderEndpoint;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub plan_id: Uuid,
    pub blueprint_id: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
    #[serde(default)]
    pub actions: Vec<PlanAction>,
    #[serde(default)]
    pub conflicts: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SlotConflictResolution {
    KeepExisting,
    Replace,
    Abort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlotConflictDecision {
    pub conflict_id: String,
    pub action: SlotConflictResolution,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PlanDecisions {
    #[serde(default)]
    pub slot_conflicts: Vec<SlotConflictDecision>,
}

impl PlanDecisions {
    pub fn from_conflicts(conflicts: &[serde_json::Value]) -> Option<Self> {
        let mut slot_conflicts = Vec::new();
        for conflict in conflicts {
            let code = conflict.get("code").and_then(|value| value.as_str());
            if code != Some("slot_conflict") {
                continue;
            }
            let conflict_id = match conflict.get("conflict_id").and_then(|value| value.as_str()) {
                Some(value) => value,
                None => continue,
            };
            let decision = match conflict.get("decision").and_then(|value| value.as_str()) {
                Some(value) => value,
                None => continue,
            };
            let action = match decision {
                "keep_existing" => SlotConflictResolution::KeepExisting,
                "replace" => SlotConflictResolution::Replace,
                "abort" => SlotConflictResolution::Abort,
                _ => continue,
            };
            slot_conflicts.push(SlotConflictDecision {
                conflict_id: conflict_id.to_string(),
                action,
            });
        }
        if slot_conflicts.is_empty() {
            None
        } else {
            Some(Self { slot_conflicts })
        }
    }
}

impl Plan {
    pub fn new(blueprint_id: String, params: Option<serde_json::Value>) -> Self {
        Self {
            plan_id: Uuid::new_v4(),
            blueprint_id,
            params,
            actions: Vec::new(),
            conflicts: Vec::new(),
        }
    }

    pub fn into_executor_actions(self) -> Result<Vec<ExecutorAction>> {
        self.actions
            .into_iter()
            .map(|action| action.try_into())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PlanAction {
    EnsureInstanceInstalled {
        instance: InstanceSpec,
    },
    DeleteProvider {
        provider_id: Uuid,
    },
    EnsureRuntimeRunning {
        runtime: RuntimeSpec,
    },
    RollbackRuntime {
        instance_id: Uuid,
    },
    HealthGate {
        provider_id: Uuid,
        #[serde(default = "default_health_gate_timeout")]
        timeout_seconds: u64,
    },
    CreateOrUpdateProvider {
        provider: ProviderSpec,
    },
    ApplyDriverPatch {
        patch: DriverPatchSpec,
    },
    ApplyBinding {
        binding: BindingSpec,
        consumer_endpoint: ProviderEndpoint,
        provider_endpoint: ProviderEndpoint,
        #[serde(default)]
        reverse_probe: bool,
    },
}

impl PlanAction {
    pub fn action_type(&self) -> &'static str {
        match self {
            PlanAction::EnsureInstanceInstalled { .. } => "ensure_instance_installed",
            PlanAction::DeleteProvider { .. } => "delete_provider",
            PlanAction::EnsureRuntimeRunning { .. } => "ensure_runtime_running",
            PlanAction::RollbackRuntime { .. } => "rollback_runtime",
            PlanAction::HealthGate { .. } => "health_gate",
            PlanAction::CreateOrUpdateProvider { .. } => "create_or_update_provider",
            PlanAction::ApplyDriverPatch { .. } => "apply_driver_patch",
            PlanAction::ApplyBinding { .. } => "apply_binding",
        }
    }
}

impl TryFrom<PlanAction> for ExecutorAction {
    type Error = anyhow::Error;

    fn try_from(action: PlanAction) -> Result<Self> {
        match action {
            PlanAction::EnsureInstanceInstalled { instance } => Ok(ExecutorAction::EnsureInstanceInstalled {
                instance_id: instance.instance_id,
                extension_id: instance.extension_id,
                instance_name: instance.instance_name,
                config_json: instance.config_json,
                enabled: instance.enabled,
            }),
            PlanAction::DeleteProvider { provider_id } => {
                Ok(ExecutorAction::DeleteProvider { provider_id })
            }
            PlanAction::EnsureRuntimeRunning { runtime } => Ok(ExecutorAction::EnsureRuntimeRunning {
                instance_id: runtime.instance_id,
                extension_id: runtime.extension_id,
                instance_name: runtime.instance_name,
                runtime: runtime.runtime,
                networking: runtime.networking,
                aliases: runtime.aliases,
            }),
            PlanAction::RollbackRuntime { instance_id } => {
                Ok(ExecutorAction::RollbackRuntime { instance_id })
            }
            PlanAction::HealthGate {
                provider_id,
                timeout_seconds,
            } => Ok(ExecutorAction::HealthGate {
                provider_id,
                timeout_seconds,
            }),
            PlanAction::CreateOrUpdateProvider { provider } => Ok(ExecutorAction::CreateOrUpdateProvider {
                provider_id: provider.provider_id,
                instance_id: provider.instance_id,
                capability: provider.capability,
                slot_id: provider.slot_id,
                cardinality: provider.cardinality,
                implementation: provider.implementation,
                endpoint: provider.endpoint,
            }),
            PlanAction::ApplyDriverPatch { patch } => Ok(ExecutorAction::ApplyDriverPatch {
                connector_extension_id: patch.connector_extension_id,
                target_provider_id: patch.target_provider_id,
                patch: patch.patch,
            }),
            PlanAction::ApplyBinding {
                binding,
                consumer_endpoint,
                provider_endpoint,
                reverse_probe,
            } => {
                consumer_endpoint.validate()?;
                provider_endpoint.validate()?;
                Ok(ExecutorAction::ApplyBinding {
                    binding: binding.into_new_binding()?,
                    consumer_endpoint,
                    provider_endpoint,
                    reverse_probe,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceSpec {
    pub instance_id: Uuid,
    pub extension_id: String,
    pub instance_name: String,
    #[serde(default)]
    pub config_json: Option<serde_json::Value>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSpec {
    pub instance_id: Uuid,
    pub extension_id: String,
    pub instance_name: String,
    pub runtime: ManifestRuntime,
    #[serde(default)]
    pub networking: Option<ManifestNetworking>,
    #[serde(default)]
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderSpec {
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub capability: String,
    pub slot_id: String,
    pub cardinality: SlotCardinality,
    #[serde(default)]
    pub implementation: Option<String>,
    pub endpoint: ProviderEndpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverPatchSpec {
    pub connector_extension_id: String,
    pub target_provider_id: Uuid,
    pub target_capability: String,
    pub target_slot_id: String,
    pub patch: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindingSpec {
    pub binding_id: Uuid,
    pub consumer_provider_id: Uuid,
    pub requires_capability: String,
    #[serde(default = "default_slot")]
    pub requires_slot_id: String,
    pub target_provider_id: Uuid,
    #[serde(default)]
    pub binding_params_json: Option<serde_json::Value>,
    #[serde(default)]
    pub status: Option<BindingStatus>,
}

impl BindingSpec {
    fn into_new_binding(self) -> Result<NewBinding> {
        ensure_non_empty(&self.requires_capability, "requires_capability")?;
        ensure_non_empty(&self.requires_slot_id, "requires_slot_id")?;
        Ok(NewBinding {
            binding_id: self.binding_id,
            consumer_provider_id: self.consumer_provider_id,
            requires_capability: self.requires_capability,
            requires_slot_id: self.requires_slot_id,
            target_provider_id: self.target_provider_id,
            binding_params_json: self.binding_params_json,
            status: self.status.unwrap_or(BindingStatus::Pending),
        })
    }
}

pub struct Planner;

impl Planner {
    pub const AUTO_WIRE_BLUEPRINT_ID: &'static str = "auto_wire";

    pub fn new() -> Self {
        Self
    }

    pub async fn plan_blueprint(
        &self,
        store: &ExtensionStore<'_>,
        blueprint_id: String,
        params: Option<serde_json::Value>,
    ) -> Result<Plan> {
        self.plan_blueprint_with_decisions(store, blueprint_id, params, None)
            .await
    }

    pub async fn plan_auto_wire(&self, store: &ExtensionStore<'_>) -> Result<Plan> {
        let providers = store.list_provider_details().await?;
        let extensions = store.list_extensions().await?;
        let instances = store.list_instances(None).await?;

        let instance_map: HashMap<Uuid, _> = instances
            .iter()
            .cloned()
            .map(|instance| (instance.instance_id, instance))
            .collect();
        let extension_map: HashMap<String, crate::db::models::Extension> = extensions
            .iter()
            .cloned()
            .map(|extension| (extension.extension_id.clone(), extension))
            .collect();

        let allow_community = true;
        let connector_catalog = build_connector_catalog(&extensions, allow_community, None)?;

        let filtered_providers: Vec<ProviderDetails> = providers
            .into_iter()
            .filter(|provider| {
                let instance = match instance_map.get(&provider.provider.instance_id) {
                    Some(instance) if instance.enabled => instance,
                    _ => return false,
                };
                let extension = match extension_map.get(&instance.extension_id) {
                    Some(extension) if extension.enabled => extension,
                    _ => return false,
                };
                trust_allowed(provider.trust_level, allow_community)
                    && extension.kind == ExtensionKind::Module
            })
            .collect();

        let mut wants = Vec::new();
        let mut seen = HashSet::new();
        for provider in &filtered_providers {
            let capability = provider.provider.capability.clone();
            let slot = provider.provider.slot_id.clone();
            let key = format!("{capability}/{slot}");
            if seen.insert(key) {
                wants.push(ManifestCapabilityRef { capability, slot });
            }
        }
        wants.sort_by(|a, b| {
            let by_capability = a.capability.cmp(&b.capability);
            if by_capability == std::cmp::Ordering::Equal {
                a.slot.cmp(&b.slot)
            } else {
                by_capability
            }
        });

        let mut plan = Plan::new(Self::AUTO_WIRE_BLUEPRINT_ID.to_string(), None);
        let mut selections: HashMap<String, ProviderSelection> = HashMap::new();

        for want in &wants {
            let key = capability_key(want);
            let candidates = providers_for_want(&filtered_providers, want, allow_community);
            if let Some(selected) =
                select_existing_provider(candidates, None, want, &mut plan.conflicts)
            {
                selections.insert(key, ProviderSelection::Existing(selected.clone()));
            }
        }

        let mut actions = Vec::new();
        let mut health_gate_targets: HashSet<Uuid> = HashSet::new();
        let mut missing_secrets_by_instance: HashMap<Uuid, HashSet<String>> = HashMap::new();

        let mut connector_entries: Vec<&ConnectorCandidate> = connector_catalog.iter().collect();
        connector_entries.sort_by(|a, b| a.extension_id.cmp(&b.extension_id));
        for connector in connector_entries {
            for action in &connector.manifest.actions {
                if action.r#type != "driver_patch" {
                    continue;
                }
                let target = match action.target.as_ref() {
                    Some(target) => target,
                    None => continue,
                };
                let key = capability_key(target);
                let selection = match selections.get(&key) {
                    Some(selection) => selection,
                    None => {
                        // Auto-wire ignores missing targets; connectors apply when present.
                        continue;
                    }
                };
                let patch = match action.patch.as_ref() {
                    Some(patch) => patch.clone(),
                    None => {
                        plan.conflicts.push(conflict_driver_patch(
                            target,
                            &connector.extension_id,
                            "missing patch payload",
                        ));
                        continue;
                    }
                };
                let driver_patch =
                    match DriverPatch::from_manifest(&target.capability, patch.clone()) {
                        Ok(patch) => patch,
                        Err(err) => {
                            plan.conflicts.push(conflict_driver_patch(
                                target,
                                &connector.extension_id,
                                &err.to_string(),
                            ));
                            continue;
                        }
                    };
                if let Err(err) = driver_patch.validate() {
                    plan.conflicts.push(conflict_driver_patch(
                        target,
                        &connector.extension_id,
                        &err.to_string(),
                    ));
                    continue;
                }

                let instance_id = selection.instance_id();
                let mut missing =
                    missing_indexer_secrets_for_patch(store, instance_id, &driver_patch).await?;
                missing.extend(missing_downloader_secrets_for_patch(store, &driver_patch).await?);
                if !missing.is_empty() {
                    missing_secrets_by_instance
                        .entry(instance_id)
                        .or_default()
                        .extend(missing);
                }

                let provider_id = selection.provider_id();
                if health_gate_targets.insert(provider_id) {
                    actions.push(PlanAction::HealthGate {
                        provider_id,
                        timeout_seconds: default_health_gate_timeout(),
                    });
                }
                actions.push(PlanAction::ApplyDriverPatch {
                    patch: DriverPatchSpec {
                        connector_extension_id: connector.extension_id.clone(),
                        target_provider_id: provider_id,
                        target_capability: target.capability.clone(),
                        target_slot_id: target.slot.clone(),
                        patch,
                    },
                });
            }
        }

        let planned_instances: HashMap<String, PlannedInstance> = HashMap::new();
        for (instance_id, missing) in &missing_secrets_by_instance {
            if missing.is_empty() {
                continue;
            }
            let (extension_id, instance_name) =
                resolve_instance_info(*instance_id, &instance_map, &planned_instances)
                    .unwrap_or_else(|| ("unknown".to_string(), "instance".to_string()));
            let mut missing: Vec<_> = missing.iter().cloned().collect();
            missing.sort();
            plan.conflicts.push(conflict_missing_required_secrets(
                &extension_id,
                *instance_id,
                &instance_name,
                &missing,
            ));
        }

        plan.actions = actions;
        Ok(plan)
    }

    pub async fn plan_blueprint_with_decisions(
        &self,
        store: &ExtensionStore<'_>,
        blueprint_id: String,
        params: Option<serde_json::Value>,
        decisions: Option<&PlanDecisions>,
    ) -> Result<Plan> {
        ensure_non_empty(&blueprint_id, "blueprint_id")?;
        let blueprint = store
            .get_extension(&blueprint_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("blueprint extension '{blueprint_id}' not found"))?;
        if !blueprint.enabled {
            bail!("blueprint extension '{blueprint_id}' is disabled");
        }
        if blueprint.kind != ExtensionKind::Blueprint {
            bail!("extension '{}' is not a blueprint", blueprint_id);
        }

        let manifest: ExtensionManifest = serde_json::from_value(blueprint.manifest_json.clone())
            .context("parsing blueprint manifest")?;
        manifest.validate()?;

        let reuse_existing = manifest
            .policies
            .as_ref()
            .and_then(|policies| policies.reuse_existing)
            .unwrap_or(true);
        let allow_community = manifest
            .policies
            .as_ref()
            .and_then(|policies| policies.allow_community_extensions)
            .unwrap_or(true);

        let providers = store.list_provider_details().await?;
        let extensions = store.list_extensions().await?;
        let instances = store.list_instances(None).await?;
        let decisions_map = decisions_map(decisions);

        let mut used_instance_names = group_instance_names(&instances);
        let module_catalog = build_module_catalog(&extensions, allow_community)?;
        let connector_allowlist = if manifest.connectors.is_empty() {
            None
        } else {
            Some(manifest.connectors.as_slice())
        };
        let connector_catalog =
            build_connector_catalog(&extensions, allow_community, connector_allowlist)?;
        let extension_map: HashMap<String, crate::db::models::Extension> = extensions
            .iter()
            .cloned()
            .map(|extension| (extension.extension_id.clone(), extension))
            .collect();
        let instance_map: HashMap<Uuid, crate::db::models::ExtensionInstance> = instances
            .iter()
            .cloned()
            .map(|instance| (instance.instance_id, instance))
            .collect();

        let mut plan = Plan::new(blueprint_id, params);
        let mut selections: HashMap<String, ProviderSelection> = HashMap::new();
        let mut planned_instances: HashMap<String, PlannedInstance> = HashMap::new();

        for want in &manifest.wants {
            let key = capability_key(want);
            if selections.contains_key(&key) {
                continue;
            }

            let decision = decisions_map.get(&key);
            let force_keep = matches!(decision, Some(SlotConflictResolution::KeepExisting));
            let force_replace = matches!(decision, Some(SlotConflictResolution::Replace));

            if force_keep {
                let candidates = providers_for_want(&providers, want, allow_community);
                if let Some(selected) = select_existing_provider_for_keep(candidates) {
                    selections.insert(key, ProviderSelection::Existing(selected.clone()));
                } else {
                    plan.conflicts.push(conflict_missing_provider(
                        want,
                        Some("no existing provider to keep"),
                    ));
                }
                continue;
            }

            if reuse_existing && !force_replace {
                let candidates = providers_for_want(&providers, want, allow_community);
                if let Some(selected) = select_existing_provider(
                    candidates.clone(),
                    manifest.preferences.as_ref(),
                    want,
                    &mut plan.conflicts,
                ) {
                    selections.insert(key, ProviderSelection::Existing(selected.clone()));
                    continue;
                }
            }

            if let Some(provider) = select_or_plan_module(
                want,
                &module_catalog,
                manifest.preferences.as_ref(),
                &mut used_instance_names,
                &mut planned_instances,
                &mut plan.conflicts,
            ) {
                selections.insert(key, ProviderSelection::Planned(provider));
                continue;
            }

            plan.conflicts.push(conflict_missing_provider(want, None));
        }

        let mut upgrade_instances: HashMap<Uuid, RuntimeSpec> = HashMap::new();
        let module_manifest_map: HashMap<String, ExtensionManifest> = module_catalog
            .iter()
            .map(|candidate| (candidate.extension_id.clone(), candidate.manifest.clone()))
            .collect();
        for selection in selections.values() {
            let ProviderSelection::Existing(detail) = selection else {
                continue;
            };
            let provider = &detail.provider;
            let instance = match instance_map.get(&provider.instance_id) {
                Some(instance) if instance.enabled => instance,
                _ => continue,
            };
            let extension = match extension_map.get(&instance.extension_id) {
                Some(extension) if extension.enabled => extension,
                _ => continue,
            };
            if extension.kind != ExtensionKind::Module {
                continue;
            }
            if instance.runtime_version.as_deref() == Some(extension.version.as_str()) {
                continue;
            }
            let manifest = match module_manifest_map.get(&instance.extension_id) {
                Some(manifest) => manifest,
                None => continue,
            };
            let runtime = match manifest.runtime.clone() {
                Some(runtime) => runtime,
                None => continue,
            };
            let networking = manifest.networking.clone();
            let (aliases, _) = build_aliases(
                &instance.extension_id,
                &instance.instance_name,
                instance.instance_id,
                runtime.service_name.clone(),
            );
            upgrade_instances.entry(instance.instance_id).or_insert(RuntimeSpec {
                instance_id: instance.instance_id,
                extension_id: instance.extension_id.clone(),
                instance_name: instance.instance_name.clone(),
                runtime,
                networking,
                aliases,
            });
        }

        let mut actions = Vec::new();
        let mut instance_entries: Vec<&PlannedInstance> = planned_instances.values().collect();
        instance_entries.sort_by(|a, b| a.instance.extension_id.cmp(&b.instance.extension_id));
        let mut missing_secrets_by_instance: HashMap<Uuid, HashSet<String>> = HashMap::new();
        for instance in &instance_entries {
            let required = required_secrets_from_runtime(&instance.runtime.runtime.env)?;
            if required.is_empty() {
                continue;
            }
            let mut missing =
                missing_required_secrets_for_instance(store, instance.instance.instance_id, &required)
                    .await?;
            if is_qbittorrent_extension_id(&instance.instance.extension_id) {
                missing = filter_qbittorrent_missing(missing);
            }
            if !missing.is_empty() {
                missing_secrets_by_instance
                    .entry(instance.instance.instance_id)
                    .or_default()
                    .extend(missing);
            }
        }
        for runtime in upgrade_instances.values() {
            let required = required_secrets_from_runtime(&runtime.runtime.env)?;
            if required.is_empty() {
                continue;
            }
            let mut missing =
                missing_required_secrets_for_instance(store, runtime.instance_id, &required)
                    .await?;
            if is_qbittorrent_extension_id(&runtime.extension_id) {
                missing = filter_qbittorrent_missing(missing);
            }
            if !missing.is_empty() {
                missing_secrets_by_instance
                    .entry(runtime.instance_id)
                    .or_default()
                    .extend(missing);
            }
        }
        for instance in instance_entries {
            actions.push(PlanAction::EnsureInstanceInstalled {
                instance: instance.instance.clone(),
            });
            actions.push(PlanAction::EnsureRuntimeRunning {
                runtime: instance.runtime.clone(),
            });
            for provider in &instance.providers {
                actions.push(PlanAction::CreateOrUpdateProvider {
                    provider: provider.clone(),
                });
            }
        }
        let mut upgrade_runtime_entries: Vec<&RuntimeSpec> = upgrade_instances.values().collect();
        upgrade_runtime_entries.sort_by(|a, b| a.extension_id.cmp(&b.extension_id));
        for runtime in upgrade_runtime_entries {
            actions.push(PlanAction::EnsureRuntimeRunning {
                runtime: runtime.clone(),
            });
        }

        let mut connector_entries: Vec<&ConnectorCandidate> = connector_catalog.iter().collect();
        connector_entries.sort_by(|a, b| a.extension_id.cmp(&b.extension_id));
        let mut health_gate_targets: HashSet<Uuid> = HashSet::new();
        for connector in connector_entries {
            for action in &connector.manifest.actions {
                if action.r#type != "driver_patch" {
                    continue;
                }
                let target = match action.target.as_ref() {
                    Some(target) => target,
                    None => continue,
                };
                let key = capability_key(target);
                let selection = match selections.get(&key) {
                    Some(selection) => selection,
                    None => {
                        plan.conflicts.push(conflict_missing_provider(
                            target,
                            Some("connector target not available"),
                        ));
                        continue;
                    }
                };
                let patch = match action.patch.as_ref() {
                    Some(patch) => patch.clone(),
                    None => {
                        plan.conflicts.push(conflict_driver_patch(
                            target,
                            &connector.extension_id,
                            "missing patch payload",
                        ));
                        continue;
                    }
                };
                let driver_patch =
                    match DriverPatch::from_manifest(&target.capability, patch.clone()) {
                        Ok(patch) => patch,
                        Err(err) => {
                            plan.conflicts.push(conflict_driver_patch(
                                target,
                                &connector.extension_id,
                                &err.to_string(),
                            ));
                            continue;
                        }
                    };
                if let Err(err) = driver_patch.validate() {
                    plan.conflicts.push(conflict_driver_patch(
                        target,
                        &connector.extension_id,
                        &err.to_string(),
                    ));
                    continue;
                }
                let instance_id = selection.instance_id();
                let mut missing =
                    missing_indexer_secrets_for_patch(store, instance_id, &driver_patch).await?;
                missing.extend(missing_downloader_secrets_for_patch(store, &driver_patch).await?);
                if !missing.is_empty() {
                    missing_secrets_by_instance
                        .entry(instance_id)
                        .or_default()
                        .extend(missing);
                }

                let provider_id = selection.provider_id();
                if health_gate_targets.insert(provider_id) {
                    actions.push(PlanAction::HealthGate {
                        provider_id,
                        timeout_seconds: default_health_gate_timeout(),
                    });
                }
                actions.push(PlanAction::ApplyDriverPatch {
                    patch: DriverPatchSpec {
                        connector_extension_id: connector.extension_id.clone(),
                        target_provider_id: provider_id,
                        target_capability: target.capability.clone(),
                        target_slot_id: target.slot.clone(),
                        patch,
                    },
                });
            }
        }

        for binding in &manifest.bindings {
            if let Some(action) =
                build_binding_action(binding, &selections, &mut plan.conflicts)
            {
                actions.push(action);
            }
        }

        for (instance_id, missing) in &missing_secrets_by_instance {
            if missing.is_empty() {
                continue;
            }
            let (extension_id, instance_name) =
                resolve_instance_info(*instance_id, &instance_map, &planned_instances)
                    .unwrap_or_else(|| ("unknown".to_string(), "instance".to_string()));
            let mut missing: Vec<_> = missing.iter().cloned().collect();
            missing.sort();
            plan.conflicts.push(conflict_missing_required_secrets(
                &extension_id,
                *instance_id,
                &instance_name,
                &missing,
            ));
        }

        let mut pre_actions = resolve_slot_collisions(
            &mut plan,
            &providers,
            &planned_instances,
            &module_catalog,
            manifest.policies.as_ref(),
            &decisions_map,
            &instances,
        );
        if !pre_actions.is_empty() {
            pre_actions.extend(actions);
            actions = pre_actions;
        }

        plan.actions = actions;
        Ok(plan)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotConflictPolicy {
    Prompt,
    AutoReplace,
    Deny,
}

#[derive(Clone)]
struct ModuleCandidate {
    extension_id: String,
    manifest: ExtensionManifest,
}

#[derive(Clone)]
struct ConnectorCandidate {
    extension_id: String,
    manifest: ExtensionManifest,
}

#[derive(Clone)]
struct PlannedInstance {
    instance: InstanceSpec,
    runtime: RuntimeSpec,
    providers: Vec<ProviderSpec>,
}

#[derive(Clone)]
enum ProviderSelection {
    Existing(ProviderDetails),
    Planned(ProviderSpec),
}

impl ProviderSelection {
    fn provider_id(&self) -> Uuid {
        match self {
            ProviderSelection::Existing(value) => value.provider.provider_id,
            ProviderSelection::Planned(value) => value.provider_id,
        }
    }

    fn instance_id(&self) -> Uuid {
        match self {
            ProviderSelection::Existing(value) => value.provider.instance_id,
            ProviderSelection::Planned(value) => value.instance_id,
        }
    }

    fn endpoint(&self) -> Result<ProviderEndpoint> {
        match self {
            ProviderSelection::Existing(value) => parse_endpoint(&value.provider.endpoint_json),
            ProviderSelection::Planned(value) => Ok(value.endpoint.clone()),
        }
    }
}

fn build_module_catalog(
    extensions: &[crate::db::models::Extension],
    allow_community: bool,
) -> Result<Vec<ModuleCandidate>> {
    let mut modules = Vec::new();
    for extension in extensions {
        if !extension.enabled || extension.kind != ExtensionKind::Module {
            continue;
        }
        if !trust_allowed(extension.trust_level, allow_community) {
            continue;
        }
        let manifest: ExtensionManifest =
            serde_json::from_value(extension.manifest_json.clone()).context(format!(
                "parsing module manifest '{}'",
                extension.extension_id
            ))?;
        manifest.validate()?;
        modules.push(ModuleCandidate {
            extension_id: extension.extension_id.clone(),
            manifest,
        });
    }
    Ok(modules)
}

fn build_connector_catalog(
    extensions: &[crate::db::models::Extension],
    allow_community: bool,
    allowlist: Option<&[String]>,
) -> Result<Vec<ConnectorCandidate>> {
    let mut connectors = Vec::new();
    let allowlist = allowlist.map(|values| {
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<HashSet<String>>()
    });
    for extension in extensions {
        if let Some(allowlist) = allowlist.as_ref() {
            if !allowlist.contains(&extension.extension_id) {
                continue;
            }
        }
        if !extension.enabled || extension.kind != ExtensionKind::Connector {
            continue;
        }
        if !trust_allowed(extension.trust_level, allow_community) {
            continue;
        }
        let manifest: ExtensionManifest =
            serde_json::from_value(extension.manifest_json.clone()).context(format!(
                "parsing connector manifest '{}'",
                extension.extension_id
            ))?;
        manifest.validate()?;
        connectors.push(ConnectorCandidate {
            extension_id: extension.extension_id.clone(),
            manifest,
        });
    }
    Ok(connectors)
}

fn group_instance_names(
    instances: &[crate::db::models::ExtensionInstance],
) -> HashMap<String, HashSet<String>> {
    let mut grouped: HashMap<String, HashSet<String>> = HashMap::new();
    for instance in instances {
        grouped
            .entry(instance.extension_id.clone())
            .or_default()
            .insert(instance.instance_name.clone());
    }
    grouped
}

fn select_or_plan_module(
    want: &ManifestCapabilityRef,
    modules: &[ModuleCandidate],
    preferences: Option<&ManifestPreferences>,
    used_instance_names: &mut HashMap<String, HashSet<String>>,
    planned_instances: &mut HashMap<String, PlannedInstance>,
    conflicts: &mut Vec<serde_json::Value>,
) -> Option<ProviderSpec> {
    let mut candidates = modules_for_want(modules, want);
    if candidates.is_empty() {
        return None;
    }

    let ordered = order_module_candidates(&mut candidates, preferences, want, conflicts);
    for candidate in ordered {
        if let Some(existing) = planned_instances.get(&candidate.extension_id) {
            if let Some(provider) = find_planned_provider(existing, want) {
                return Some(provider.clone());
            }
            continue;
        }

        let used_names = used_instance_names
            .entry(candidate.extension_id.clone())
            .or_default();
        match plan_module_instance(candidate, used_names, conflicts) {
            Ok(instance) => {
                let provider = find_planned_provider(&instance, want).cloned();
                planned_instances.insert(candidate.extension_id.clone(), instance);
                if let Some(provider) = provider {
                    return Some(provider);
                }
            }
            Err(err) => {
                conflicts.push(conflict_module_invalid(want, &candidate.extension_id, &err));
                continue;
            }
        }
    }
    None
}

fn plan_module_instance(
    candidate: &ModuleCandidate,
    used_names: &mut HashSet<String>,
    conflicts: &mut Vec<serde_json::Value>,
) -> Result<PlannedInstance> {
    let runtime = candidate
        .manifest
        .runtime
        .clone()
        .ok_or_else(|| anyhow::anyhow!("module runtime is missing"))?;
    let networking = candidate.manifest.networking.clone();

    let instance_id = Uuid::new_v4();
    let instance_name = next_instance_name(used_names);
    used_names.insert(instance_name.clone());

    let (aliases, primary_alias) = build_aliases(
        &candidate.extension_id,
        &instance_name,
        instance_id,
        runtime.service_name.clone(),
    );

    let mut providers = Vec::new();
    for provide in &candidate.manifest.provides {
        let endpoint = match build_provider_endpoint(
            provide,
            &networking,
            &primary_alias,
        ) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                conflicts.push(conflict_invalid_endpoint(
                    &candidate.extension_id,
                    &provide.capability,
                    &provide.slot,
                    &err,
                ));
                continue;
            }
        };
        providers.push(ProviderSpec {
            provider_id: Uuid::new_v4(),
            instance_id,
            capability: provide.capability.clone(),
            slot_id: provide.slot.clone(),
            cardinality: provide.cardinality.unwrap_or(SlotCardinality::One),
            implementation: provide.implementation.clone(),
            endpoint,
        });
    }

    if providers.is_empty() {
        bail!("module '{}' has no usable providers", candidate.extension_id);
    }

    Ok(PlannedInstance {
        instance: InstanceSpec {
            instance_id,
            extension_id: candidate.extension_id.clone(),
            instance_name: instance_name.clone(),
            config_json: None,
            enabled: true,
        },
        runtime: RuntimeSpec {
            instance_id,
            extension_id: candidate.extension_id.clone(),
            instance_name,
            runtime,
            networking,
            aliases,
        },
        providers,
    })
}

fn next_instance_name(used_names: &HashSet<String>) -> String {
    if !used_names.contains("default") {
        return "default".to_string();
    }
    for idx in 2..1000 {
        let candidate = format!("default-{idx}");
        if !used_names.contains(&candidate) {
            return candidate;
        }
    }
    format!("default-{}", Uuid::new_v4().simple())
}

fn build_provider_endpoint(
    provide: &crate::extensions::manifest::ManifestProvide,
    networking: &Option<ManifestNetworking>,
    host: &str,
) -> Result<ProviderEndpoint> {
    let scheme = networking
        .as_ref()
        .map(|net| net.service_port.scheme.clone())
        .or_else(|| provide.endpoint.as_ref().and_then(|ep| ep.scheme.clone()))
        .unwrap_or_else(|| "http".to_string());

    let port = networking
        .as_ref()
        .map(|net| net.service_port.container_port)
        .or_else(|| provide.endpoint.as_ref().and_then(|ep| ep.port))
        .ok_or_else(|| anyhow::anyhow!("service port missing for capability '{}'", provide.capability))?;

    let base_path = provide
        .endpoint
        .as_ref()
        .and_then(|ep| ep.base_path.clone());

    ProviderEndpoint::new(
        scheme,
        host.to_string(),
        port,
        base_path,
        Some("elixir_net".to_string()),
    )
}

fn find_planned_provider<'a>(
    instance: &'a PlannedInstance,
    want: &ManifestCapabilityRef,
) -> Option<&'a ProviderSpec> {
    instance.providers.iter().find(|provider| {
        provider.capability == want.capability && provider.slot_id == want.slot
    })
}

fn modules_for_want<'a>(
    modules: &'a [ModuleCandidate],
    want: &ManifestCapabilityRef,
) -> Vec<&'a ModuleCandidate> {
    modules
        .iter()
        .filter(|module| {
            module
                .manifest
                .provides
                .iter()
                .any(|provide| provide.capability == want.capability && provide.slot == want.slot)
        })
        .collect()
}

fn order_module_candidates<'a>(
    candidates: &mut Vec<&'a ModuleCandidate>,
    preferences: Option<&ManifestPreferences>,
    want: &ManifestCapabilityRef,
    conflicts: &mut Vec<serde_json::Value>,
) -> Vec<&'a ModuleCandidate> {
    if candidates.len() == 1 {
        return candidates.clone();
    }

    let prefer = preferences
        .and_then(|prefs| prefs.providers.get(&capability_key(want)))
        .map(|pref| pref.prefer.as_slice())
        .unwrap_or_default();

    if !prefer.is_empty() {
        let mut ordered = Vec::new();
        for preferred in prefer {
            if let Some(candidate) = candidates
                .iter()
                .find(|module| module.extension_id == *preferred)
            {
                ordered.push(*candidate);
            }
        }
        if ordered.is_empty() {
            conflicts.push(conflict_multiple_providers(
                want,
                "no preferred modules installed",
            ));
        }
        return ordered;
    }

    conflicts.push(conflict_multiple_providers(
        want,
        "multiple module providers available",
    ));
    Vec::new()
}

fn providers_for_want<'a>(
    providers: &'a [ProviderDetails],
    want: &ManifestCapabilityRef,
    allow_community: bool,
) -> Vec<&'a ProviderDetails> {
    providers
        .iter()
        .filter(|provider| {
            provider.provider.capability == want.capability
                && provider.provider.slot_id == want.slot
                && trust_allowed(provider.trust_level, allow_community)
        })
        .collect()
}

fn select_existing_provider<'a>(
    mut candidates: Vec<&'a ProviderDetails>,
    preferences: Option<&ManifestPreferences>,
    want: &ManifestCapabilityRef,
    conflicts: &mut Vec<serde_json::Value>,
) -> Option<&'a ProviderDetails> {
    if candidates.is_empty() {
        return None;
    }

    let healthy: Vec<&ProviderDetails> = candidates
        .iter()
        .filter(|p| p.provider.health_state != ProviderHealthState::Unhealthy)
        .copied()
        .collect();
    if healthy.is_empty() {
        conflicts.push(conflict_missing_provider(
            want,
            Some("no healthy providers available"),
        ));
        return None;
    }
    candidates = healthy;

    if candidates.len() == 1 {
        return Some(candidates[0]);
    }

    let prefer = preferences
        .and_then(|prefs| prefs.providers.get(&capability_key(want)))
        .map(|pref| pref.prefer.as_slice())
        .unwrap_or_default();

    for preferred in prefer {
        if let Some(candidate) = candidates
            .iter()
            .find(|provider| provider.extension_id == *preferred)
        {
            return Some(*candidate);
        }
    }

    if !prefer.is_empty() {
        conflicts.push(conflict_multiple_providers(
            want,
            "no preferred providers installed",
        ));
        return None;
    }

    conflicts.push(conflict_multiple_providers(
        want,
        "multiple providers available",
    ));
    None
}

fn select_existing_provider_for_keep<'a>(
    mut candidates: Vec<&'a ProviderDetails>,
) -> Option<&'a ProviderDetails> {
    if candidates.is_empty() {
        return None;
    }

    let healthy: Vec<&ProviderDetails> = candidates
        .iter()
        .filter(|provider| provider.provider.health_state != ProviderHealthState::Unhealthy)
        .copied()
        .collect();
    if !healthy.is_empty() {
        candidates = healthy;
    }

    candidates.sort_by(|left, right| {
        let by_extension = left.extension_id.cmp(&right.extension_id);
        if by_extension == std::cmp::Ordering::Equal {
            left.provider.provider_id.cmp(&right.provider.provider_id)
        } else {
            by_extension
        }
    });

    Some(candidates[0])
}

fn build_binding_action(
    binding: &ManifestBinding,
    selections: &HashMap<String, ProviderSelection>,
    conflicts: &mut Vec<serde_json::Value>,
) -> Option<PlanAction> {
    let from_key = capability_key(&binding.from);
    let to_key = capability_key(&binding.to);
    let consumer = match selections.get(&from_key) {
        Some(value) => value,
        None => {
            conflicts.push(conflict_binding_missing(
                &binding.from,
                &binding.to,
                "consumer provider not selected",
            ));
            return None;
        }
    };
    let provider = match selections.get(&to_key) {
        Some(value) => value,
        None => {
            conflicts.push(conflict_binding_missing(
                &binding.from,
                &binding.to,
                "target provider not selected",
            ));
            return None;
        }
    };

    let consumer_endpoint = match consumer.endpoint() {
        Ok(endpoint) => endpoint,
        Err(err) => {
            conflicts.push(conflict_binding_missing(
                &binding.from,
                &binding.to,
                &format!("consumer endpoint invalid: {err}"),
            ));
            return None;
        }
    };
    let provider_endpoint = match provider.endpoint() {
        Ok(endpoint) => endpoint,
        Err(err) => {
            conflicts.push(conflict_binding_missing(
                &binding.from,
                &binding.to,
                &format!("provider endpoint invalid: {err}"),
            ));
            return None;
        }
    };

    Some(PlanAction::ApplyBinding {
        binding: BindingSpec {
            binding_id: Uuid::new_v4(),
            consumer_provider_id: consumer.provider_id(),
            requires_capability: binding.to.capability.clone(),
            requires_slot_id: binding.to.slot.clone(),
            target_provider_id: provider.provider_id(),
            binding_params_json: None,
            status: Some(BindingStatus::Pending),
        },
        consumer_endpoint,
        provider_endpoint,
        reverse_probe: false,
    })
}

fn parse_endpoint(value: &Option<serde_json::Value>) -> Result<ProviderEndpoint> {
    let value = value.clone().ok_or_else(|| anyhow::anyhow!("endpoint missing"))?;
    serde_json::from_value(value).context("parsing provider endpoint")
}

fn capability_key(value: &ManifestCapabilityRef) -> String {
    format!("{}/{}", value.capability, value.slot)
}

fn conflict_missing_provider(
    want: &ManifestCapabilityRef,
    detail: Option<&str>,
) -> serde_json::Value {
    let mut conflict = serde_json::json!({
        "code": "missing_provider",
        "capability": want.capability,
        "slot": want.slot,
    });
    if let Some(detail) = detail {
        if let Some(obj) = conflict.as_object_mut() {
            obj.insert("detail".to_string(), serde_json::Value::String(detail.to_string()));
        }
    }
    conflict
}

fn conflict_multiple_providers(want: &ManifestCapabilityRef, detail: &str) -> serde_json::Value {
    serde_json::json!({
        "code": "multiple_providers",
        "capability": want.capability,
        "slot": want.slot,
        "detail": detail,
    })
}

fn conflict_binding_missing(
    from: &ManifestCapabilityRef,
    to: &ManifestCapabilityRef,
    detail: &str,
) -> serde_json::Value {
    serde_json::json!({
        "code": "binding_missing_provider",
        "from": format!("{}/{}", from.capability, from.slot),
        "to": format!("{}/{}", to.capability, to.slot),
        "detail": detail,
    })
}

fn conflict_invalid_endpoint(
    extension_id: &str,
    capability: &str,
    slot: &str,
    err: &anyhow::Error,
) -> serde_json::Value {
    serde_json::json!({
        "code": "invalid_provider_endpoint",
        "extension_id": extension_id,
        "capability": capability,
        "slot": slot,
        "detail": err.to_string(),
    })
}

fn conflict_module_invalid(
    want: &ManifestCapabilityRef,
    extension_id: &str,
    err: &anyhow::Error,
) -> serde_json::Value {
    serde_json::json!({
        "code": "module_invalid",
        "extension_id": extension_id,
        "capability": want.capability,
        "slot": want.slot,
        "detail": err.to_string(),
    })
}

fn conflict_driver_patch(
    target: &ManifestCapabilityRef,
    connector_id: &str,
    detail: &str,
) -> serde_json::Value {
    serde_json::json!({
        "code": "driver_patch_invalid",
        "connector_id": connector_id,
        "capability": target.capability,
        "slot": target.slot,
        "detail": detail,
    })
}

fn conflict_missing_required_secrets(
    extension_id: &str,
    instance_id: Uuid,
    instance_name: &str,
    missing: &[String],
) -> serde_json::Value {
    serde_json::json!({
        "code": "missing_required_secrets",
        "extension_id": extension_id,
        "instance_id": instance_id,
        "instance_name": instance_name,
        "missing": missing,
    })
}

async fn missing_indexer_secrets_for_patch(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    patch: &DriverPatch,
) -> Result<Vec<String>> {
    let indexers: Vec<_> = match patch {
        DriverPatch::IndexerRegistry(IndexerRegistryPatch::RegisterIndexers { indexers }) => {
            indexers.iter().collect()
        }
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetIndexerRegistry { indexers }) => {
            indexers.iter().collect()
        }
        _ => Vec::new(),
    };
    let mut missing = HashSet::new();
    for indexer in indexers {
        let fields = indexer.credential_fields()?;
        for field in fields {
            let key = indexer.credential_secret_key(field);
            let exists = store
                .get_secret(crate::db::models::SecretScope::Instance, Some(instance_id), &key)
                .await?
                .is_some();
            if !exists {
                missing.insert(format!("instance:{}:{}", instance_id, key));
            }
        }
    }
    Ok(missing.into_iter().collect())
}

fn filter_qbittorrent_missing(missing: Vec<String>) -> Vec<String> {
    missing
        .into_iter()
        .filter(|value| {
            !value.ends_with(":qbittorrent_username")
                && !value.ends_with(":qbittorrent_password")
        })
        .collect()
}

fn is_qbittorrent_extension_id(extension_id: &str) -> bool {
    extension_id
        .to_ascii_lowercase()
        .contains("qbittorrent")
}

async fn missing_downloader_secrets_for_patch(
    _store: &ExtensionStore<'_>,
    patch: &DriverPatch,
) -> Result<Vec<String>> {
    let downloaders: Vec<_> = match patch {
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetDownloaders { downloaders }) => {
            downloaders.iter().collect()
        }
        DriverPatch::MediaManagerMovies(MediaManagerMoviesPatch::SetDownloaders { downloaders }) => {
            downloaders.iter().collect()
        }
        _ => Vec::new(),
    };
    for downloader in downloaders {
        if !is_qbittorrent_downloader(&downloader.r#type) {
            continue;
        }
        if downloader_has_credentials(downloader) {
            continue;
        }
        // qBittorrent credentials are auto-generated on first run.
        continue;
    }
    Ok(Vec::new())
}

fn is_qbittorrent_downloader(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("qbittorrent")
}

fn downloader_has_credentials(downloader: &DownloaderSpec) -> bool {
    !downloader_setting_missing(&downloader.settings, "username")
        && !downloader_setting_missing(&downloader.settings, "password")
}

fn downloader_setting_missing(
    settings: &std::collections::HashMap<String, serde_json::Value>,
    key: &str,
) -> bool {
    match settings.get(key) {
        None => true,
        Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(value)) => value.trim().is_empty(),
        Some(_) => false,
    }
}

fn resolve_instance_info(
    instance_id: Uuid,
    instance_map: &HashMap<Uuid, crate::db::models::ExtensionInstance>,
    planned_instances: &HashMap<String, PlannedInstance>,
) -> Option<(String, String)> {
    if let Some(instance) = instance_map.get(&instance_id) {
        return Some((instance.extension_id.clone(), instance.instance_name.clone()));
    }
    planned_instances
        .values()
        .find(|planned| planned.instance.instance_id == instance_id)
        .map(|planned| {
            (
                planned.instance.extension_id.clone(),
                planned.instance.instance_name.clone(),
            )
        })
}

fn decisions_map(decisions: Option<&PlanDecisions>) -> HashMap<String, SlotConflictResolution> {
    let mut map = HashMap::new();
    if let Some(decisions) = decisions {
        for decision in &decisions.slot_conflicts {
            map.insert(decision.conflict_id.clone(), decision.action.clone());
        }
    }
    map
}

fn resolve_slot_collisions(
    plan: &mut Plan,
    providers: &[ProviderDetails],
    planned_instances: &HashMap<String, PlannedInstance>,
    module_catalog: &[ModuleCandidate],
    policies: Option<&ManifestPolicies>,
    decisions: &HashMap<String, SlotConflictResolution>,
    instances: &[crate::db::models::ExtensionInstance],
) -> Vec<PlanAction> {
    if planned_instances.is_empty() {
        return Vec::new();
    }

    let instance_names: HashMap<Uuid, String> = instances
        .iter()
        .map(|instance| (instance.instance_id, instance.instance_name.clone()))
        .collect();
    let module_map: HashMap<String, ExtensionManifest> = module_catalog
        .iter()
        .map(|candidate| (candidate.extension_id.clone(), candidate.manifest.clone()))
        .collect();
    let planned_by_instance: HashMap<Uuid, &PlannedInstance> = planned_instances
        .values()
        .map(|instance| (instance.instance.instance_id, instance))
        .collect();

    let mut delete_actions = Vec::new();
    let mut deleted_providers = HashSet::new();
    let mut seen_conflicts = HashSet::new();

    for planned_instance in planned_instances.values() {
        for planned_provider in &planned_instance.providers {
            if planned_provider.cardinality == SlotCardinality::Many {
                continue;
            }

            let existing: Vec<&ProviderDetails> = providers
                .iter()
                .filter(|provider| {
                    provider.provider.capability == planned_provider.capability
                        && provider.provider.slot_id == planned_provider.slot_id
                })
                .collect();
            if existing.is_empty() {
                continue;
            }

            let conflict_id = format!(
                "{}/{}",
                planned_provider.capability, planned_provider.slot_id
            );
            let policy = slot_conflict_policy(
                module_map.get(&planned_instance.instance.extension_id),
                &planned_provider.capability,
                &planned_provider.slot_id,
                policies,
            );
            let decision = decisions.get(&conflict_id);
            let mut resolved = false;
            let mut replace = false;

            match decision {
                Some(SlotConflictResolution::Replace) => {
                    if policy == SlotConflictPolicy::Deny {
                        resolved = false;
                    } else {
                        resolved = true;
                        replace = true;
                    }
                }
                Some(SlotConflictResolution::KeepExisting) => {
                    resolved = true;
                }
                Some(SlotConflictResolution::Abort) => {
                    resolved = false;
                }
                None => {
                    if policy == SlotConflictPolicy::AutoReplace {
                        resolved = true;
                        replace = true;
                    }
                }
            }

            if replace {
                for provider in &existing {
                    if deleted_providers.insert(provider.provider.provider_id) {
                        delete_actions.push(PlanAction::DeleteProvider {
                            provider_id: provider.provider.provider_id,
                        });
                    }
                }
            }

            if seen_conflicts.insert(conflict_id.clone()) {
                let planned_info = planned_by_instance
                    .get(&planned_provider.instance_id)
                    .copied()
                    .unwrap_or(planned_instance);
                plan.conflicts.push(conflict_slot_collision(
                    &conflict_id,
                    planned_provider,
                    planned_info,
                    &existing,
                    &instance_names,
                    policy,
                    resolved,
                    decision.cloned(),
                ));
            }
        }
    }

    delete_actions
}

fn slot_conflict_policy(
    manifest: Option<&ExtensionManifest>,
    capability: &str,
    slot: &str,
    policies: Option<&ManifestPolicies>,
) -> SlotConflictPolicy {
    if let Some(manifest) = manifest {
        if let Some(conflict) = manifest.conflicts.iter().find(|entry| {
            entry.capability == capability && entry.slot == slot
        }) {
            return match conflict.policy {
                ManifestConflictPolicy::PromptReplace => SlotConflictPolicy::Prompt,
                ManifestConflictPolicy::AutoReplace => SlotConflictPolicy::AutoReplace,
                ManifestConflictPolicy::Deny => SlotConflictPolicy::Deny,
            };
        }
    }

    let policy = policies
        .and_then(|policies| policies.conflicts.as_deref())
        .map(|value| value.trim().to_lowercase());

    match policy.as_deref() {
        Some("auto_replace") | Some("autoreplace") => SlotConflictPolicy::AutoReplace,
        Some("deny") => SlotConflictPolicy::Deny,
        Some("prompt") | Some("prompt_replace") => SlotConflictPolicy::Prompt,
        _ => SlotConflictPolicy::Prompt,
    }
}

fn conflict_slot_collision(
    conflict_id: &str,
    planned_provider: &ProviderSpec,
    planned_instance: &PlannedInstance,
    existing: &[&ProviderDetails],
    instance_names: &HashMap<Uuid, String>,
    policy: SlotConflictPolicy,
    resolved: bool,
    decision: Option<SlotConflictResolution>,
) -> serde_json::Value {
    let existing_values: Vec<serde_json::Value> = existing
        .iter()
        .map(|provider| {
            serde_json::json!({
                "provider_id": provider.provider.provider_id,
                "instance_id": provider.provider.instance_id,
                "instance_name": instance_names.get(&provider.provider.instance_id),
                "extension_id": provider.extension_id,
                "cardinality": provider.provider.cardinality,
                "health": provider.provider.health_state,
            })
        })
        .collect();

    let planned_value = serde_json::json!({
        "provider_id": planned_provider.provider_id,
        "instance_id": planned_provider.instance_id,
        "instance_name": planned_instance.instance.instance_name,
        "extension_id": planned_instance.instance.extension_id,
        "cardinality": planned_provider.cardinality,
    });

    let policy_str = match policy {
        SlotConflictPolicy::Prompt => "prompt",
        SlotConflictPolicy::AutoReplace => "auto_replace",
        SlotConflictPolicy::Deny => "deny",
    };
    let resolution_options = match policy {
        SlotConflictPolicy::Prompt => vec!["keep_existing", "replace", "abort"],
        SlotConflictPolicy::AutoReplace => vec!["auto_replace"],
        SlotConflictPolicy::Deny => vec!["deny"],
    };

    let mut conflict = serde_json::json!({
        "code": "slot_conflict",
        "conflict_id": conflict_id,
        "capability": planned_provider.capability,
        "slot": planned_provider.slot_id,
        "policy": policy_str,
        "existing": existing_values,
        "planned": vec![planned_value],
        "resolution_options": resolution_options,
        "resolved": resolved,
    });

    if let Some(decision) = decision {
        if let Some(obj) = conflict.as_object_mut() {
            obj.insert(
                "decision".to_string(),
                serde_json::Value::String(match decision {
                    SlotConflictResolution::KeepExisting => "keep_existing",
                    SlotConflictResolution::Replace => "replace",
                    SlotConflictResolution::Abort => "abort",
                }
                .to_string()),
            );
        }
    }

    conflict
}

fn trust_allowed(level: ExtensionTrustLevel, allow_community: bool) -> bool {
    match level {
        ExtensionTrustLevel::Verified => true,
        ExtensionTrustLevel::Community => allow_community,
        ExtensionTrustLevel::Untrusted => false,
    }
}

fn default_slot() -> String {
    "default".to_string()
}

fn default_true() -> bool {
    true
}

fn default_health_gate_timeout() -> u64 {
    60
}

fn ensure_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} is required");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality};
    use crate::extensions::store::{ExtensionStore, NewExtension, NewExtensionInstance, NewProvider};
    use crate::orchestrator::model::ProviderEndpoint;

    fn module_manifest(id: &str, capability: &str) -> serde_json::Value {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "module",
            "name": id,
            "provides": [
                {
                    "capability": capability,
                    "slot": "default",
                    "cardinality": "one",
                    "implementation": "test"
                }
            ],
            "runtime": {
                "type": "container",
                "image": "example/module:1.0.0"
            },
            "networking": {
                "service_port": { "scheme": "http", "container_port": 8080 }
            }
        })
    }

    fn module_manifest_with_conflict(
        id: &str,
        capability: &str,
        policy: &str,
    ) -> serde_json::Value {
        let mut manifest = module_manifest(id, capability);
        let conflicts = json!([
            {
                "capability": capability,
                "slot": "default",
                "policy": policy
            }
        ]);
        manifest
            .as_object_mut()
            .unwrap()
            .insert("conflicts".to_string(), conflicts);
        manifest
    }

    fn module_manifest_with_secret(id: &str, capability: &str) -> serde_json::Value {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "module",
            "name": id,
            "provides": [
                {
                    "capability": capability,
                    "slot": "default",
                    "cardinality": "one",
                    "implementation": "test"
                }
            ],
            "runtime": {
                "type": "container",
                "image": "example/module:1.0.0",
                "env": [
                    {
                        "name": "API_KEY",
                        "from_secret": "instance:api_key"
                    }
                ]
            },
            "networking": {
                "service_port": { "scheme": "http", "container_port": 8080 }
            }
        })
    }

    fn connector_manifest(id: &str, target_capability: &str) -> serde_json::Value {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "connector",
            "name": id,
            "targets": [
                { "capability": target_capability, "slot": "default" }
            ],
            "actions": [
                {
                    "type": "driver_patch",
                    "target": { "capability": target_capability, "slot": "default" },
                    "patch": { "op": "set_tags", "tags": ["elixir"] }
                }
            ]
        })
    }

    fn connector_manifest_with_downloaders(id: &str, target_capability: &str) -> serde_json::Value {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "connector",
            "name": id,
            "targets": [
                { "capability": target_capability, "slot": "default" }
            ],
            "actions": [
                {
                    "type": "driver_patch",
                    "target": { "capability": target_capability, "slot": "default" },
                    "patch": {
                        "op": "set_downloaders",
                        "downloaders": [
                            {
                                "name": "qBittorrent",
                                "type": "qbittorrent",
                                "url": "http://elx-qbittorrent:8080",
                                "enabled": true
                            }
                        ]
                    }
                }
            ]
        })
    }

    fn blueprint_single_want(id: &str, capability: &str) -> serde_json::Value {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "blueprint",
            "name": id,
            "wants": [
                { "capability": capability, "slot": "default" }
            ]
        })
    }

    fn blueprint_single_want_with_policies(
        id: &str,
        capability: &str,
        reuse_existing: bool,
    ) -> serde_json::Value {
        let mut manifest = blueprint_single_want(id, capability);
        manifest.as_object_mut().unwrap().insert(
            "policies".to_string(),
            json!({ "reuse_existing": reuse_existing }),
        );
        manifest
    }

    fn blueprint_manifest(prefer: Option<Vec<&str>>, include_binding: bool) -> serde_json::Value {
        let mut manifest = json!({
            "id": "blueprint.test",
            "version": "1.0.0",
            "kind": "blueprint",
            "name": "Blueprint Test",
            "wants": [
                { "capability": "media.manager.tv", "slot": "default" },
                { "capability": "indexer.registry", "slot": "default" }
            ],
        });

        if let Some(prefer) = prefer {
            let preferences = json!({
                "providers": {
                    "media.manager.tv/default": { "prefer": prefer }
                }
            });
            manifest.as_object_mut().unwrap().insert(
                "preferences".to_string(),
                preferences,
            );
        }

        if include_binding {
            let bindings = json!([
                {
                    "from": { "capability": "media.manager.tv", "slot": "default" },
                    "to": { "capability": "indexer.registry", "slot": "default" }
                }
            ]);
            manifest.as_object_mut().unwrap().insert(
                "bindings".to_string(),
                bindings,
            );
        }

        manifest
    }

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn insert_extension(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        kind: ExtensionKind,
        manifest_json: serde_json::Value,
    ) -> Result<()> {
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: extension_id.to_string(),
                version: "1.0.0".to_string(),
                kind,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json,
                package_hash: None,
                enabled: true,
            })
            .await
    }

    async fn insert_provider(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        capability: &str,
        endpoint: serde_json::Value,
    ) -> Result<(Uuid, Uuid)> {
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        let provider_id = Uuid::new_v4();
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: capability.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: None,
                endpoint_json: Some(endpoint),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok((instance_id, provider_id))
    }

    async fn insert_provider_with_impl(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        capability: &str,
        implementation: &str,
        endpoint: serde_json::Value,
    ) -> Result<(Uuid, Uuid)> {
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        let provider_id = Uuid::new_v4();
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: capability.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some(implementation.to_string()),
                endpoint_json: Some(endpoint),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok((instance_id, provider_id))
    }

    #[tokio::test]
    async fn planner_prefers_preferred_provider() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.preferred",
            ExtensionKind::Module,
            module_manifest("ext.preferred", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.other",
            ExtensionKind::Module,
            module_manifest("ext.other", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.indexer",
            ExtensionKind::Module,
            module_manifest("ext.indexer", "indexer.registry"),
        )
        .await?;

        let preferred_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-preferred".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        let other_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-other".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        let indexer_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-indexer".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;

        let (_, preferred_provider_id) = insert_provider(
            &store,
            "ext.preferred",
            "media.manager.tv",
            serde_json::to_value(preferred_endpoint)?,
        )
        .await?;
        let _ = insert_provider(
            &store,
            "ext.other",
            "media.manager.tv",
            serde_json::to_value(other_endpoint)?,
        )
        .await?;
        let _ = insert_provider(
            &store,
            "ext.indexer",
            "indexer.registry",
            serde_json::to_value(indexer_endpoint)?,
        )
        .await?;

        insert_extension(
            &store,
            "blueprint.test",
            ExtensionKind::Blueprint,
            blueprint_manifest(Some(vec!["ext.preferred"]), true),
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.test".to_string(), None)
            .await?;

        assert!(plan.conflicts.is_empty());
        let binding = plan.actions.iter().find_map(|action| match action {
            PlanAction::ApplyBinding { binding, .. } => Some(binding),
            _ => None,
        });
        let binding = binding.expect("binding action");
        assert_eq!(binding.consumer_provider_id, preferred_provider_id);
        Ok(())
    }

    #[tokio::test]
    async fn planner_conflicts_without_preference() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.one",
            ExtensionKind::Module,
            module_manifest("ext.one", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.two",
            ExtensionKind::Module,
            module_manifest("ext.two", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.test",
            ExtensionKind::Blueprint,
            blueprint_manifest(None, false),
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-one".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        let other_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-two".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;

        let _ = insert_provider(
            &store,
            "ext.one",
            "media.manager.tv",
            serde_json::to_value(endpoint)?,
        )
        .await?;
        let _ = insert_provider(
            &store,
            "ext.two",
            "media.manager.tv",
            serde_json::to_value(other_endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.test".to_string(), None)
            .await?;

        assert!(
            plan.conflicts
                .iter()
                .any(|conflict| conflict.get("code") == Some(&json!("multiple_providers")))
        );
        Ok(())
    }

    #[tokio::test]
    async fn planner_conflict_on_invalid_endpoint() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.consumer",
            ExtensionKind::Module,
            module_manifest("ext.consumer", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.indexer",
            ExtensionKind::Module,
            module_manifest("ext.indexer", "indexer.registry"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.test",
            ExtensionKind::Blueprint,
            blueprint_manifest(None, true),
        )
        .await?;

        let invalid_endpoint = json!({ "scheme": "http" });
        let indexer_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-indexer".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;

        let _ = insert_provider(
            &store,
            "ext.consumer",
            "media.manager.tv",
            invalid_endpoint,
        )
        .await?;
        let _ = insert_provider(
            &store,
            "ext.indexer",
            "indexer.registry",
            serde_json::to_value(indexer_endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.test".to_string(), None)
            .await?;

        assert!(
            plan.conflicts.iter().any(|conflict| {
                conflict.get("code") == Some(&json!("binding_missing_provider"))
                    && conflict
                        .get("detail")
                        .and_then(|detail| detail.as_str())
                        .unwrap_or("")
                        .contains("consumer endpoint invalid")
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn planner_conflicts_on_missing_required_secrets() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.secret",
            ExtensionKind::Module,
            module_manifest_with_secret("ext.secret", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.secret",
            ExtensionKind::Blueprint,
            blueprint_single_want("blueprint.secret", "media.manager.tv"),
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.secret".to_string(), None)
            .await?;
        assert!(plan.conflicts.iter().any(|conflict| {
            conflict.get("code") == Some(&json!("missing_required_secrets"))
                && conflict.get("missing")
                    .and_then(|missing| missing.as_array())
                    .map(|missing| {
                        missing.iter().any(|entry| {
                            entry
                                .as_str()
                                .map(|value| value.contains("instance:") && value.ends_with(":api_key"))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false)
        }));

        Ok(())
    }

    #[tokio::test]
    async fn extensions_conflicts_on_missing_qbittorrent_secrets() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.sonarr",
            ExtensionKind::Module,
            module_manifest("ext.sonarr", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.qbittorrent",
            ExtensionKind::Module,
            module_manifest("ext.qbittorrent", "downloader.torrent"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.sonarr.qbittorrent",
            ExtensionKind::Connector,
            connector_manifest_with_downloaders("ext.sonarr.qbittorrent", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.qbittorrent",
            ExtensionKind::Blueprint,
            json!({
                "id": "blueprint.qbittorrent",
                "version": "1.0.0",
                "kind": "blueprint",
                "name": "QBittorrent Blueprint",
                "wants": [
                    { "capability": "media.manager.tv", "slot": "default" },
                    { "capability": "downloader.torrent", "slot": "default" }
                ],
                "connectors": ["ext.sonarr.qbittorrent"]
            }),
        )
        .await?;

        let qbittorrent_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "elx-qbittorrent".to_string(),
            8080,
            None,
            None,
        )?;
        let _ = insert_provider_with_impl(
            &store,
            "ext.qbittorrent",
            "downloader.torrent",
            "qbittorrent",
            serde_json::to_value(qbittorrent_endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.qbittorrent".to_string(), None)
            .await?;

        let mut saw_qbittorrent_missing = false;
        for conflict in &plan.conflicts {
            if conflict.get("code") != Some(&json!("missing_required_secrets")) {
                continue;
            }
            let missing = conflict.get("missing").and_then(|value| value.as_array());
            let Some(missing) = missing else { continue };
            for entry in missing {
                if let Some(value) = entry.as_str() {
                    if value.ends_with(":qbittorrent_username")
                        || value.ends_with(":qbittorrent_password")
                    {
                        saw_qbittorrent_missing = true;
                    }
                }
            }
        }

        assert!(
            !saw_qbittorrent_missing,
            "qbittorrent credentials should be auto-generated"
        );

        Ok(())
    }

    #[tokio::test]
    async fn planner_slot_conflict_auto_replace_adds_delete() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.existing",
            ExtensionKind::Module,
            module_manifest("ext.existing", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.replacer",
            ExtensionKind::Module,
            module_manifest_with_conflict("ext.replacer", "media.manager.tv", "auto_replace"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.conflict",
            ExtensionKind::Blueprint,
            {
                let mut manifest = blueprint_single_want_with_policies(
                    "blueprint.conflict",
                    "media.manager.tv",
                    false,
                );
                manifest.as_object_mut().unwrap().insert(
                    "preferences".to_string(),
                    json!({
                        "providers": {
                            "media.manager.tv/default": { "prefer": ["ext.replacer"] }
                        }
                    }),
                );
                manifest
            },
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-existing".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        let (_, existing_provider_id) = insert_provider(
            &store,
            "ext.existing",
            "media.manager.tv",
            serde_json::to_value(endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.conflict".to_string(), None)
            .await?;

        assert!(
            plan.conflicts.iter().any(|conflict| {
                conflict.get("code") == Some(&json!("slot_conflict"))
                    && conflict.get("policy") == Some(&json!("auto_replace"))
            }),
            "expected auto_replace slot conflict"
        );
        assert!(
            plan.actions.iter().any(|action| matches!(
                action,
                PlanAction::DeleteProvider {
                    provider_id
                } if *provider_id == existing_provider_id
            )),
            "expected delete_provider action"
        );

        Ok(())
    }

    #[tokio::test]
    async fn planner_slot_conflict_prompt_requires_resolution() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.existing",
            ExtensionKind::Module,
            module_manifest("ext.existing", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.prompt",
            ExtensionKind::Module,
            module_manifest_with_conflict("ext.prompt", "media.manager.tv", "prompt_replace"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.conflict",
            ExtensionKind::Blueprint,
            {
                let mut manifest = blueprint_single_want_with_policies(
                    "blueprint.conflict",
                    "media.manager.tv",
                    false,
                );
                manifest.as_object_mut().unwrap().insert(
                    "preferences".to_string(),
                    json!({
                        "providers": {
                            "media.manager.tv/default": { "prefer": ["ext.prompt"] }
                        }
                    }),
                );
                manifest
            },
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-existing".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        let _ = insert_provider(
            &store,
            "ext.existing",
            "media.manager.tv",
            serde_json::to_value(endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.conflict".to_string(), None)
            .await?;

        assert!(
            plan.conflicts.iter().any(|conflict| {
                conflict.get("code") == Some(&json!("slot_conflict"))
                    && conflict.get("policy") == Some(&json!("prompt"))
                    && conflict.get("resolved") == Some(&json!(false))
            }),
            "expected prompt slot conflict"
        );
        assert!(
            !plan.actions.iter().any(|action| matches!(
                action,
                PlanAction::DeleteProvider { .. }
            )),
            "did not expect delete_provider action"
        );

        Ok(())
    }

    #[tokio::test]
    async fn planner_keep_existing_skips_module_install() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.existing",
            ExtensionKind::Module,
            module_manifest("ext.existing", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.prompt",
            ExtensionKind::Module,
            module_manifest_with_conflict("ext.prompt", "media.manager.tv", "prompt_replace"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.conflict",
            ExtensionKind::Blueprint,
            {
                let mut manifest = blueprint_single_want_with_policies(
                    "blueprint.conflict",
                    "media.manager.tv",
                    false,
                );
                manifest.as_object_mut().unwrap().insert(
                    "preferences".to_string(),
                    json!({
                        "providers": {
                            "media.manager.tv/default": { "prefer": ["ext.prompt"] }
                        }
                    }),
                );
                manifest
            },
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-existing".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        let (existing_instance_id, _) = insert_provider(
            &store,
            "ext.existing",
            "media.manager.tv",
            serde_json::to_value(endpoint)?,
        )
        .await?;
        store
            .update_instance_runtime_version(existing_instance_id, "1.0.0", None)
            .await?;

        let decisions = PlanDecisions {
            slot_conflicts: vec![SlotConflictDecision {
                conflict_id: "media.manager.tv/default".to_string(),
                action: SlotConflictResolution::KeepExisting,
            }],
        };

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint_with_decisions(
                &store,
                "blueprint.conflict".to_string(),
                None,
                Some(&decisions),
            )
            .await?;

        assert!(
            !plan.actions.iter().any(|action| matches!(
                action,
                PlanAction::EnsureInstanceInstalled { instance }
                    if instance.extension_id == "ext.prompt"
            )),
            "did not expect module install when keep_existing is chosen"
        );
        assert!(
            !plan.conflicts.iter().any(|conflict| {
                conflict.get("code") == Some(&json!("slot_conflict"))
            }),
            "did not expect slot_conflict once keep_existing is chosen"
        );

        Ok(())
    }

    #[tokio::test]
    async fn planner_orders_health_gate_before_driver_patch() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.sonarr",
            ExtensionKind::Module,
            module_manifest("ext.sonarr", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.connector",
            ExtensionKind::Connector,
            connector_manifest("ext.connector", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.health",
            ExtensionKind::Blueprint,
            blueprint_single_want("blueprint.health", "media.manager.tv"),
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-sonarr".to_string(),
            8989,
            None,
            Some("elixir_net".to_string()),
        )?;
        let (_, provider_id) = insert_provider(
            &store,
            "ext.sonarr",
            "media.manager.tv",
            serde_json::to_value(endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.health".to_string(), None)
            .await?;

        let mut health_index = None;
        let mut patch_index = None;
        for (idx, action) in plan.actions.iter().enumerate() {
            match action {
                PlanAction::HealthGate { provider_id: id, .. } if *id == provider_id => {
                    health_index = Some(idx);
                }
                PlanAction::ApplyDriverPatch { patch, .. }
                    if patch.target_provider_id == provider_id =>
                {
                    patch_index = Some(idx);
                }
                _ => {}
            }
        }

        let health_index = health_index.expect("health gate action");
        let patch_index = patch_index.expect("driver patch action");
        assert!(
            health_index < patch_index,
            "health gate should precede driver patch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn planner_schedules_runtime_upgrade_for_existing_instance() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.upgrade",
            ExtensionKind::Module,
            module_manifest("ext.upgrade", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.upgrade",
            ExtensionKind::Blueprint,
            blueprint_single_want("blueprint.upgrade", "media.manager.tv"),
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-upgrade".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        let (instance_id, _) = insert_provider(
            &store,
            "ext.upgrade",
            "media.manager.tv",
            serde_json::to_value(endpoint)?,
        )
        .await?;
        store
            .update_instance_runtime_version(instance_id, "0.9.0", None)
            .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.upgrade".to_string(), None)
            .await?;

        assert!(plan.actions.iter().any(|action| matches!(
            action,
            PlanAction::EnsureRuntimeRunning { runtime }
                if runtime.instance_id == instance_id
        )));

        Ok(())
    }
}
