use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::models::{BindingStatus, ExtensionKind, ExtensionTrustLevel, SlotCardinality};
use crate::drivers::DriverPatch;
use crate::drivers::{
    DownloaderSpec, IndexerRegistryPatch, MediaManagerMoviesPatch, MediaManagerTvPatch,
};
use crate::extensions::auto_managed::filter_auto_managed_runtime_missing;
use crate::extensions::manifest::{
    ExtensionManifest, ManifestCapabilityRef, ManifestExecutionStep, ManifestNetworking,
    ManifestRequire, ManifestRuntime,
};
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_runtime,
};
use crate::extensions::store::{ExtensionStore, NewBinding};
use crate::orchestrator::executor::ExecutorAction;
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::naming::build_aliases;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub plan_id: Uuid,
    pub blueprint_id: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
    #[serde(default)]
    pub stages: Vec<PlanStage>,
    #[serde(default)]
    pub actions: Vec<PlanAction>,
    #[serde(default)]
    pub conflicts: Vec<serde_json::Value>,
    #[serde(default)]
    pub blocked_stage: Option<PlanBlockedStage>,
}

impl Plan {
    pub fn new(blueprint_id: String, params: Option<serde_json::Value>) -> Self {
        Self {
            plan_id: Uuid::new_v4(),
            blueprint_id,
            params,
            stages: Vec::new(),
            actions: Vec::new(),
            conflicts: Vec::new(),
            blocked_stage: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlanStage {
    pub stage_id: String,
    #[serde(default)]
    pub barrier: bool,
    pub action_start_index: usize,
    pub action_end_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlanBlockedStage {
    pub stage_id: String,
    pub code: String,
    #[serde(default)]
    pub detail: Option<String>,
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
    InstallRuntimeAsset {
        asset: RuntimeAssetSpec,
    },
    RestartRuntime {
        instance_id: Uuid,
    },
    RollbackRuntime {
        instance_id: Uuid,
    },
    TransportGate {
        provider_id: Uuid,
        #[serde(default = "default_health_gate_timeout")]
        timeout_seconds: u64,
    },
    BootstrapGate {
        provider_id: Uuid,
        #[serde(default = "default_health_gate_timeout")]
        timeout_seconds: u64,
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
    },
}

impl PlanAction {
    pub fn action_type(&self) -> &'static str {
        match self {
            PlanAction::EnsureInstanceInstalled { .. } => "ensure_instance_installed",
            PlanAction::DeleteProvider { .. } => "delete_provider",
            PlanAction::EnsureRuntimeRunning { .. } => "ensure_runtime_running",
            PlanAction::InstallRuntimeAsset { .. } => "install_runtime_asset",
            PlanAction::RestartRuntime { .. } => "restart_runtime",
            PlanAction::RollbackRuntime { .. } => "rollback_runtime",
            PlanAction::TransportGate { .. } => "transport_gate",
            PlanAction::BootstrapGate { .. } => "bootstrap_gate",
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
            PlanAction::EnsureInstanceInstalled { instance } => {
                Ok(ExecutorAction::EnsureInstanceInstalled {
                    instance_id: instance.instance_id,
                    extension_id: instance.extension_id,
                    instance_name: instance.instance_name,
                    config_json: instance.config_json,
                    enabled: instance.enabled,
                })
            }
            PlanAction::DeleteProvider { provider_id } => {
                Ok(ExecutorAction::DeleteProvider { provider_id })
            }
            PlanAction::EnsureRuntimeRunning { runtime } => {
                Ok(ExecutorAction::EnsureRuntimeRunning {
                    instance_id: runtime.instance_id,
                    extension_id: runtime.extension_id,
                    instance_name: runtime.instance_name,
                    runtime: runtime.runtime,
                    networking: runtime.networking,
                    aliases: runtime.aliases,
                })
            }
            PlanAction::InstallRuntimeAsset { asset } => Ok(ExecutorAction::InstallRuntimeAsset {
                target_instance_id: asset.target_instance_id,
                source_extension_id: asset.source_extension_id,
                source_extension_version: asset.source_extension_version,
                source_path: asset.source_path,
                destination_path: asset.destination_path,
            }),
            PlanAction::RestartRuntime { instance_id } => {
                Ok(ExecutorAction::RestartRuntime { instance_id })
            }
            PlanAction::RollbackRuntime { instance_id } => {
                Ok(ExecutorAction::RollbackRuntime { instance_id })
            }
            PlanAction::TransportGate {
                provider_id,
                timeout_seconds,
            } => Ok(ExecutorAction::TransportGate {
                provider_id,
                timeout_seconds,
            }),
            PlanAction::BootstrapGate {
                provider_id,
                timeout_seconds,
            } => Ok(ExecutorAction::BootstrapGate {
                provider_id,
                timeout_seconds,
            }),
            PlanAction::HealthGate {
                provider_id,
                timeout_seconds,
            } => Ok(ExecutorAction::HealthGate {
                provider_id,
                timeout_seconds,
            }),
            PlanAction::CreateOrUpdateProvider { provider } => {
                Ok(ExecutorAction::CreateOrUpdateProvider {
                    provider_id: provider.provider_id,
                    instance_id: provider.instance_id,
                    capability: provider.capability,
                    slot_id: provider.slot_id,
                    cardinality: provider.cardinality,
                    implementation: provider.implementation,
                    scope_json: provider.scope_json,
                    endpoint: provider.endpoint,
                })
            }
            PlanAction::ApplyDriverPatch { patch } => Ok(ExecutorAction::ApplyDriverPatch {
                connector_extension_id: patch.connector_extension_id,
                target_provider_id: patch.target_provider_id,
                patch: patch.patch,
            }),
            PlanAction::ApplyBinding { binding } => Ok(ExecutorAction::ApplyBinding {
                binding: binding.into_new_binding()?,
            }),
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
pub struct RuntimeAssetSpec {
    pub target_instance_id: Uuid,
    pub source_extension_id: String,
    pub source_extension_version: String,
    pub source_path: String,
    pub destination_path: String,
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
    #[serde(default)]
    pub scope_json: Option<serde_json::Value>,
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
    pub fn new() -> Self {
        Self
    }

    pub async fn plan_blueprint(
        &self,
        store: &ExtensionStore<'_>,
        blueprint_id: String,
        params: Option<serde_json::Value>,
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

        return self
            .plan_explicit_execution_blueprint(store, blueprint_id, params, &manifest)
            .await;
    }
}

impl Planner {
    async fn plan_explicit_execution_blueprint(
        &self,
        store: &ExtensionStore<'_>,
        blueprint_id: String,
        params: Option<serde_json::Value>,
        manifest: &ExtensionManifest,
    ) -> Result<Plan> {
        let execution = manifest
            .execution
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("execution manifest is missing"))?;
        let allow_community = manifest
            .policies
            .as_ref()
            .and_then(|policies| policies.allow_community_extensions)
            .unwrap_or(true);

        let extensions = store.list_extensions().await?;
        let instances = store.list_instances(None).await?;
        let providers = store.list_providers(None).await?;
        let extension_map: HashMap<String, crate::db::models::Extension> = extensions
            .iter()
            .cloned()
            .map(|extension| (extension.extension_id.clone(), extension))
            .collect();
        let module_catalog = build_module_catalog(&extensions, allow_community)?;
        let existing_provider_ids: HashMap<String, Uuid> = providers
            .into_iter()
            .map(|provider| {
                (
                    provider_identity_key(
                        provider.instance_id,
                        &provider.capability,
                        &provider.slot_id,
                    ),
                    provider.provider_id,
                )
            })
            .collect();

        let mut plan = Plan::new(blueprint_id, params);
        let mut planned_instances = HashMap::new();
        let ownership_by_domain: HashMap<String, String> = manifest
            .execution
            .as_ref()
            .map(|execution| execution.ownership.as_slice())
            .unwrap_or(&[])
            .iter()
            .map(|entry| (entry.domain.clone(), entry.owner.clone()))
            .collect();
        let mut claimed_ownership_domains: HashMap<String, ClaimedOwnershipDomain> = HashMap::new();

        for definition in &execution.instances {
            let extension = match extension_map.get(&definition.extension_id) {
                Some(extension)
                    if extension.enabled
                        && extension.kind == ExtensionKind::Module
                        && trust_allowed(extension.trust_level, allow_community) =>
                {
                    extension
                }
                Some(_) => {
                    plan.conflicts.push(conflict_with_stage(
                        serde_json::json!({
                        "code": "missing_package",
                        "extension_id": definition.extension_id,
                        "detail": "module package is not enabled or not allowed for this stack"
                        }),
                        "install_packages",
                    ));
                    continue;
                }
                None => {
                    plan.conflicts.push(conflict_with_stage(
                        serde_json::json!({
                        "code": "missing_package",
                        "extension_id": definition.extension_id,
                        "detail": "required execution package is not installed"
                        }),
                        "install_packages",
                    ));
                    continue;
                }
            };

            let Some(candidate) = module_catalog
                .iter()
                .find(|candidate| candidate.extension_id == extension.extension_id)
            else {
                plan.conflicts.push(conflict_with_stage(
                    serde_json::json!({
                    "code": "module_invalid",
                    "extension_id": definition.extension_id,
                    "detail": "module package is not available in the module catalog"
                    }),
                    "install_packages",
                ));
                continue;
            };

            let maybe_existing = instances.iter().find(|instance| {
                instance.enabled
                    && instance.extension_id == definition.extension_id
                    && instance.instance_name == definition.name
            });
            let planned = match maybe_existing {
                Some(existing) => plan_existing_module_instance(
                    candidate,
                    existing,
                    &existing_provider_ids,
                    &mut plan.conflicts,
                ),
                None => plan_named_module_instance(
                    candidate,
                    explicit_stack_instance_id(&manifest.id, &definition.id),
                    definition.name.clone(),
                    definition.config.clone(),
                    &mut plan.conflicts,
                ),
            };

            match planned {
                Ok(instance) => {
                    planned_instances.insert(definition.id.clone(), instance);
                }
                Err(err) => {
                    plan.conflicts.push(conflict_with_stage(
                        serde_json::json!({
                        "code": "module_invalid",
                        "extension_id": definition.extension_id,
                        "instance_id": definition.id,
                        "detail": err.to_string(),
                        }),
                        "create_instances",
                    ));
                }
            }
        }

        let mut actions = Vec::new();
        let mut transport_gate_targets = HashSet::new();
        let mut bootstrap_gate_targets = HashSet::new();
        let mut health_gate_targets = HashSet::new();
        let mut missing_secrets_by_instance: HashMap<Uuid, HashSet<String>> = HashMap::new();

        for phase in &execution.phases {
            let stage_action_start = actions.len();
            for step in &phase.steps {
                match step {
                    ManifestExecutionStep::EnsurePackageInstalled { .. } => {}
                    ManifestExecutionStep::EnsureInstanceInstalled { instance } => {
                        let Some(planned) = planned_instances.get(instance) else {
                            continue;
                        };
                        actions.push(PlanAction::EnsureInstanceInstalled {
                            instance: planned.instance.clone(),
                        });
                    }
                    ManifestExecutionStep::EnsureRuntimeRunning { instance } => {
                        let Some(planned) = planned_instances.get(instance) else {
                            continue;
                        };
                        actions.push(PlanAction::EnsureRuntimeRunning {
                            runtime: planned.runtime.clone(),
                        });

                        let required = required_secrets_from_runtime(&planned.runtime.runtime)?;
                        if !required.is_empty() {
                            let mut missing = missing_required_secrets_for_instance(
                                store,
                                planned.instance.instance_id,
                                &required,
                            )
                            .await?;
                            missing = filter_auto_managed_runtime_missing(
                                &planned.instance.extension_id,
                                missing,
                            );
                            if !missing.is_empty() {
                                missing_secrets_by_instance
                                    .entry(planned.instance.instance_id)
                                    .or_default()
                                    .extend(missing);
                            }
                        }
                    }
                    ManifestExecutionStep::InstallRuntimeAsset {
                        source_extension_id,
                        source_path,
                        target_instance,
                        destination_path,
                    } => {
                        let Some(extension) = extension_map.get(source_extension_id) else {
                            plan.conflicts.push(conflict_with_stage(
                                serde_json::json!({
                                    "code": "missing_package",
                                    "extension_id": source_extension_id,
                                    "detail": "runtime asset source package is not installed"
                                }),
                                phase.id.as_str(),
                            ));
                            continue;
                        };
                        if !extension.enabled
                            || !trust_allowed(extension.trust_level, allow_community)
                        {
                            plan.conflicts.push(conflict_with_stage(
                                serde_json::json!({
                                    "code": "missing_package",
                                    "extension_id": source_extension_id,
                                    "detail": "runtime asset source package is not enabled or not allowed for this stack"
                                }),
                                phase.id.as_str(),
                            ));
                            continue;
                        }
                        let Some(planned) = planned_instances.get(target_instance) else {
                            continue;
                        };
                        actions.push(PlanAction::InstallRuntimeAsset {
                            asset: RuntimeAssetSpec {
                                target_instance_id: planned.instance.instance_id,
                                source_extension_id: source_extension_id.clone(),
                                source_extension_version: extension.version.clone(),
                                source_path: source_path.clone(),
                                destination_path: destination_path.clone(),
                            },
                        });
                    }
                    ManifestExecutionStep::RestartRuntime { instance } => {
                        let Some(planned) = planned_instances.get(instance) else {
                            continue;
                        };
                        actions.push(PlanAction::RestartRuntime {
                            instance_id: planned.instance.instance_id,
                        });
                    }
                    ManifestExecutionStep::CreateOrUpdateProviders { instance } => {
                        let Some(planned) = planned_instances.get(instance) else {
                            continue;
                        };
                        for provider in &planned.providers {
                            actions.push(PlanAction::CreateOrUpdateProvider {
                                provider: provider.clone(),
                            });
                        }
                    }
                    ManifestExecutionStep::TransportGate {
                        instance,
                        capability,
                        slot,
                        timeout_seconds,
                    } => {
                        let target = ManifestCapabilityRef {
                            capability: capability.clone(),
                            slot: slot.clone(),
                        };
                        let Some(provider) =
                            resolve_execution_provider(&planned_instances, instance, &target)
                        else {
                            plan.conflicts.push(conflict_missing_provider(
                                &target,
                                Some("required by execution transport gate"),
                                Some(phase.id.as_str()),
                            ));
                            continue;
                        };
                        if transport_gate_targets.insert(provider.provider_id) {
                            actions.push(PlanAction::TransportGate {
                                provider_id: provider.provider_id,
                                timeout_seconds: *timeout_seconds,
                            });
                        }
                    }
                    ManifestExecutionStep::BootstrapGate {
                        instance,
                        capability,
                        slot,
                        timeout_seconds,
                    } => {
                        let target = ManifestCapabilityRef {
                            capability: capability.clone(),
                            slot: slot.clone(),
                        };
                        let Some(provider) =
                            resolve_execution_provider(&planned_instances, instance, &target)
                        else {
                            plan.conflicts.push(conflict_missing_provider(
                                &target,
                                Some("required by execution bootstrap gate"),
                                Some(phase.id.as_str()),
                            ));
                            continue;
                        };
                        if bootstrap_gate_targets.insert(provider.provider_id) {
                            actions.push(PlanAction::BootstrapGate {
                                provider_id: provider.provider_id,
                                timeout_seconds: *timeout_seconds,
                            });
                        }
                    }
                    ManifestExecutionStep::HealthGate {
                        instance,
                        capability,
                        slot,
                        timeout_seconds,
                    } => {
                        let target = ManifestCapabilityRef {
                            capability: capability.clone(),
                            slot: slot.clone(),
                        };
                        let Some(provider) =
                            resolve_execution_provider(&planned_instances, instance, &target)
                        else {
                            plan.conflicts.push(conflict_missing_provider(
                                &target,
                                Some("required by execution health gate"),
                                Some(phase.id.as_str()),
                            ));
                            continue;
                        };
                        if health_gate_targets.insert(provider.provider_id) {
                            actions.push(PlanAction::HealthGate {
                                provider_id: provider.provider_id,
                                timeout_seconds: *timeout_seconds,
                            });
                        }
                    }
                    ManifestExecutionStep::ApplyConnector {
                        connector_id,
                        target_instance,
                        target_capability,
                        target_slot,
                        ownership_domains,
                    } => {
                        let Some(extension) = extension_map.get(connector_id) else {
                            plan.conflicts.push(conflict_with_stage(
                                serde_json::json!({
                                "code": "missing_package",
                                "extension_id": connector_id,
                                "detail": "connector package is not installed"
                                }),
                                phase.id.as_str(),
                            ));
                            continue;
                        };
                        if !extension.enabled
                            || extension.kind != ExtensionKind::Connector
                            || !trust_allowed(extension.trust_level, allow_community)
                        {
                            plan.conflicts.push(conflict_with_stage(
                                serde_json::json!({
                                "code": "missing_package",
                                "extension_id": connector_id,
                                "detail": "connector package is not enabled or not allowed for this stack"
                                }),
                                phase.id.as_str(),
                            ));
                            continue;
                        }
                        let connector_manifest: ExtensionManifest =
                            serde_json::from_value(extension.manifest_json.clone()).with_context(
                                || format!("parsing connector manifest for {connector_id}"),
                            )?;
                        connector_manifest.validate()?;

                        let target = ManifestCapabilityRef {
                            capability: target_capability.clone(),
                            slot: target_slot.clone(),
                        };
                        let Some(target_provider) = resolve_execution_provider(
                            &planned_instances,
                            target_instance,
                            &target,
                        ) else {
                            plan.conflicts.push(conflict_missing_provider(
                                &target,
                                Some(
                                    format!("required target for connector {connector_id}")
                                        .as_str(),
                                ),
                                Some(phase.id.as_str()),
                            ));
                            continue;
                        };

                        let mut ownership_invalid = false;
                        let mut pending_ownership_claims = Vec::new();
                        for domain in ownership_domains {
                            let Some(owner) = ownership_by_domain.get(domain) else {
                                plan.conflicts.push(conflict_ownership_invalid(
                                    connector_id,
                                    domain,
                                    "ownership domain is not declared in blueprint ownership",
                                    Some(phase.id.as_str()),
                                ));
                                ownership_invalid = true;
                                continue;
                            };
                            if owner != connector_id {
                                plan.conflicts.push(conflict_ownership_invalid(
                                    connector_id,
                                    domain,
                                    &format!("ownership domain is assigned to '{}'", owner),
                                    Some(phase.id.as_str()),
                                ));
                                ownership_invalid = true;
                                continue;
                            }
                            if let Some(existing) = claimed_ownership_domains.get(domain) {
                                plan.conflicts.push(conflict_ownership_conflict(
                                    domain,
                                    connector_id,
                                    &existing.connector_id,
                                    Some(phase.id.as_str()),
                                ));
                                ownership_invalid = true;
                                continue;
                            }
                            pending_ownership_claims.push(domain.clone());
                        }
                        if ownership_invalid {
                            continue;
                        }
                        for domain in pending_ownership_claims {
                            claimed_ownership_domains.insert(
                                domain,
                                ClaimedOwnershipDomain {
                                    connector_id: connector_id.clone(),
                                },
                            );
                        }

                        let mut requirement_provider_ids = Vec::new();
                        for require in &connector_manifest.requires {
                            let requirement = capability_ref_from_require(require);
                            let Some(provider) =
                                resolve_execution_requirement(&planned_instances, &requirement)
                            else {
                                if !require.optional {
                                    let detail = format!("required by connector {connector_id}");
                                    plan.conflicts.push(conflict_missing_provider(
                                        &requirement,
                                        Some(detail.as_str()),
                                        Some(phase.id.as_str()),
                                    ));
                                }
                                continue;
                            };
                            requirement_provider_ids.push(provider.provider_id);
                        }

                        for action in &connector_manifest.actions {
                            if action.r#type != "driver_patch" {
                                continue;
                            }
                            let action_target = match action.target.as_ref() {
                                Some(action_target)
                                    if action_target.capability == target.capability
                                        && action_target.slot == target.slot =>
                                {
                                    action_target
                                }
                                _ => continue,
                            };
                            let patch = match action.patch.as_ref() {
                                Some(patch) => patch.clone(),
                                None => {
                                    plan.conflicts.push(conflict_driver_patch(
                                        action_target,
                                        connector_id,
                                        "missing patch payload",
                                        Some(phase.id.as_str()),
                                    ));
                                    continue;
                                }
                            };
                            let driver_patch =
                                match DriverPatch::from_manifest(&target.capability, patch.clone())
                                {
                                    Ok(patch) => patch,
                                    Err(err) => {
                                        plan.conflicts.push(conflict_driver_patch(
                                            action_target,
                                            connector_id,
                                            &err.to_string(),
                                            Some(phase.id.as_str()),
                                        ));
                                        continue;
                                    }
                                };
                            if let Err(err) = driver_patch.validate() {
                                plan.conflicts.push(conflict_driver_patch(
                                    action_target,
                                    connector_id,
                                    &err.to_string(),
                                    Some(phase.id.as_str()),
                                ));
                                continue;
                            }

                            let mut missing = missing_indexer_secrets_for_patch(
                                store,
                                target_provider.instance_id,
                                &driver_patch,
                            )
                            .await?;
                            missing.extend(
                                missing_downloader_secrets_for_patch(store, &driver_patch).await?,
                            );
                            if !missing.is_empty() {
                                missing_secrets_by_instance
                                    .entry(target_provider.instance_id)
                                    .or_default()
                                    .extend(missing);
                            }

                            push_provider_readiness_actions(
                                &mut actions,
                                target_provider.provider_id,
                                default_health_gate_timeout(),
                                &mut transport_gate_targets,
                                &mut bootstrap_gate_targets,
                                &mut health_gate_targets,
                            );
                            for provider_id in &requirement_provider_ids {
                                push_provider_readiness_actions(
                                    &mut actions,
                                    *provider_id,
                                    default_health_gate_timeout(),
                                    &mut transport_gate_targets,
                                    &mut bootstrap_gate_targets,
                                    &mut health_gate_targets,
                                );
                            }
                            actions.push(PlanAction::ApplyDriverPatch {
                                patch: DriverPatchSpec {
                                    connector_extension_id: connector_id.clone(),
                                    target_provider_id: target_provider.provider_id,
                                    target_capability: target.capability.clone(),
                                    target_slot_id: target.slot.clone(),
                                    patch,
                                },
                            });
                        }
                    }
                    ManifestExecutionStep::ApplyBinding {
                        consumer_instance,
                        consumer_capability,
                        consumer_slot,
                        provider_instance,
                        provider_capability,
                        provider_slot,
                        ..
                    } => {
                        let consumer_ref = ManifestCapabilityRef {
                            capability: consumer_capability.clone(),
                            slot: consumer_slot.clone(),
                        };
                        let provider_ref = ManifestCapabilityRef {
                            capability: provider_capability.clone(),
                            slot: provider_slot.clone(),
                        };
                        let Some(consumer_provider) = resolve_execution_provider(
                            &planned_instances,
                            consumer_instance,
                            &consumer_ref,
                        ) else {
                            plan.conflicts.push(conflict_missing_provider(
                                &consumer_ref,
                                Some("required by execution binding"),
                                Some(phase.id.as_str()),
                            ));
                            continue;
                        };
                        let Some(provider_provider) = resolve_execution_provider(
                            &planned_instances,
                            provider_instance,
                            &provider_ref,
                        ) else {
                            plan.conflicts.push(conflict_missing_provider(
                                &provider_ref,
                                Some("required by execution binding"),
                                Some(phase.id.as_str()),
                            ));
                            continue;
                        };

                        push_provider_readiness_actions(
                            &mut actions,
                            consumer_provider.provider_id,
                            default_health_gate_timeout(),
                            &mut transport_gate_targets,
                            &mut bootstrap_gate_targets,
                            &mut health_gate_targets,
                        );
                        push_provider_readiness_actions(
                            &mut actions,
                            provider_provider.provider_id,
                            default_health_gate_timeout(),
                            &mut transport_gate_targets,
                            &mut bootstrap_gate_targets,
                            &mut health_gate_targets,
                        );

                        actions.push(PlanAction::ApplyBinding {
                            binding: BindingSpec {
                                binding_id: Uuid::new_v4(),
                                consumer_provider_id: consumer_provider.provider_id,
                                requires_capability: provider_provider.capability.clone(),
                                requires_slot_id: provider_provider.slot_id.clone(),
                                target_provider_id: provider_provider.provider_id,
                                binding_params_json: None,
                                status: Some(BindingStatus::Pending),
                            },
                        });
                    }
                }
            }
            plan.stages.push(PlanStage {
                stage_id: phase.id.clone(),
                barrier: phase.barrier,
                action_start_index: stage_action_start,
                action_end_index: actions.len(),
            });
        }

        let instance_map: HashMap<Uuid, _> = instances
            .iter()
            .cloned()
            .map(|instance| (instance.instance_id, instance))
            .collect();
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
                stage_for_missing_secrets(*instance_id, &plan.stages, &actions),
            ));
        }

        plan.actions = actions;
        plan.blocked_stage = plan_blocked_stage(&plan.conflicts);
        Ok(plan)
    }
}

fn explicit_stack_instance_id(blueprint_id: &str, instance_id: &str) -> Uuid {
    let key = format!("{blueprint_id}:{instance_id}");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
}

#[derive(Debug, Clone)]
struct ClaimedOwnershipDomain {
    connector_id: String,
}

fn conflict_with_stage(mut conflict: serde_json::Value, stage_id: &str) -> serde_json::Value {
    if let Some(obj) = conflict.as_object_mut() {
        obj.insert(
            "stage_id".to_string(),
            serde_json::Value::String(stage_id.to_string()),
        );
    }
    conflict
}

fn stage_for_missing_secrets<'a>(
    instance_id: Uuid,
    stages: &'a [PlanStage],
    actions: &'a [PlanAction],
) -> Option<&'a str> {
    for stage in stages {
        if stage.action_end_index <= stage.action_start_index {
            continue;
        }
        for action in &actions[stage.action_start_index..stage.action_end_index] {
            match action {
                PlanAction::EnsureRuntimeRunning { runtime }
                    if runtime.instance_id == instance_id =>
                {
                    return Some(stage.stage_id.as_str());
                }
                PlanAction::ApplyDriverPatch { patch } => {
                    if let Some(PlanAction::CreateOrUpdateProvider { provider }) =
                        actions.iter().find(|action| {
                            matches!(
                                action,
                                PlanAction::CreateOrUpdateProvider { provider }
                                    if provider.provider_id == patch.target_provider_id
                                        && provider.instance_id == instance_id
                            )
                        })
                    {
                        let _ = provider;
                        return Some(stage.stage_id.as_str());
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn plan_blocked_stage(conflicts: &[serde_json::Value]) -> Option<PlanBlockedStage> {
    conflicts.iter().find_map(|conflict| {
        if !conflict_is_blocking(conflict) {
            return None;
        }
        let stage_id = conflict
            .get("stage_id")
            .and_then(|value| value.as_str())
            .unwrap_or("plan_validation");
        let code = conflict
            .get("code")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        let detail = conflict
            .get("detail")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .or_else(|| {
                conflict
                    .get("missing")
                    .and_then(|value| value.as_array())
                    .map(|missing| {
                        missing
                            .iter()
                            .filter_map(|value| value.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .filter(|value| !value.is_empty())
            });
        Some(PlanBlockedStage {
            stage_id: stage_id.to_string(),
            code: code.to_string(),
            detail,
        })
    })
}

fn conflict_is_blocking(conflict: &serde_json::Value) -> bool {
    match conflict.get("code").and_then(|value| value.as_str()) {
        Some("missing_required_secrets") => true,
        Some(_) => true,
        None => false,
    }
}

fn plan_named_module_instance(
    candidate: &ModuleCandidate,
    instance_id: Uuid,
    instance_name: String,
    config_json: Option<serde_json::Value>,
    conflicts: &mut Vec<serde_json::Value>,
) -> Result<PlannedInstance> {
    let runtime = candidate
        .manifest
        .runtime
        .clone()
        .ok_or_else(|| anyhow::anyhow!("module runtime is missing"))?;
    let networking = candidate.manifest.networking.clone();

    let (aliases, primary_alias) = build_aliases(
        &candidate.extension_id,
        &instance_name,
        instance_id,
        runtime.service_name.clone(),
    );

    let mut providers = Vec::new();
    for provide in &candidate.manifest.provides {
        let endpoint = match build_provider_endpoint(provide, &networking, &primary_alias) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                conflicts.push(conflict_invalid_endpoint(
                    &candidate.extension_id,
                    &provide.capability,
                    &provide.slot,
                    &err,
                    Some("create_instances"),
                ));
                continue;
            }
        };
        providers.push(ProviderSpec {
            provider_id: stable_provider_id(instance_id, &provide.capability, &provide.slot),
            instance_id,
            capability: provide.capability.clone(),
            slot_id: provide.slot.clone(),
            cardinality: provide.cardinality.unwrap_or(SlotCardinality::One),
            implementation: provide.implementation.clone(),
            scope_json: provide
                .scope
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|err| anyhow::anyhow!("serializing provider scope: {err}"))?,
            endpoint,
        });
    }

    if providers.is_empty() {
        bail!(
            "module '{}' has no usable providers",
            candidate.extension_id
        );
    }

    Ok(PlannedInstance {
        instance: InstanceSpec {
            instance_id,
            extension_id: candidate.extension_id.clone(),
            instance_name: instance_name.clone(),
            config_json,
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

fn resolve_execution_provider<'a>(
    planned_instances: &'a HashMap<String, PlannedInstance>,
    instance: &str,
    target: &ManifestCapabilityRef,
) -> Option<&'a ProviderSpec> {
    planned_instances
        .get(instance)
        .and_then(|planned| find_planned_provider(planned, target))
}

fn resolve_execution_requirement<'a>(
    planned_instances: &'a HashMap<String, PlannedInstance>,
    target: &ManifestCapabilityRef,
) -> Option<&'a ProviderSpec> {
    let mut matches = planned_instances
        .values()
        .flat_map(|planned| planned.providers.iter())
        .filter(|provider| {
            provider.capability == target.capability && provider.slot_id == target.slot
        })
        .collect::<Vec<_>>();
    matches.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
    matches.into_iter().next()
}

#[derive(Clone)]
struct ModuleCandidate {
    extension_id: String,
    manifest: ExtensionManifest,
}

#[derive(Clone)]
struct PlannedInstance {
    instance: InstanceSpec,
    runtime: RuntimeSpec,
    providers: Vec<ProviderSpec>,
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
        let manifest: ExtensionManifest = serde_json::from_value(extension.manifest_json.clone())
            .context(format!(
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

fn capability_ref_from_require(require: &ManifestRequire) -> ManifestCapabilityRef {
    ManifestCapabilityRef {
        capability: require.capability.clone(),
        slot: require.slot.clone(),
    }
}

fn plan_existing_module_instance(
    candidate: &ModuleCandidate,
    existing: &crate::db::models::ExtensionInstance,
    existing_provider_ids: &HashMap<String, Uuid>,
    conflicts: &mut Vec<serde_json::Value>,
) -> Result<PlannedInstance> {
    let runtime = candidate
        .manifest
        .runtime
        .clone()
        .ok_or_else(|| anyhow::anyhow!("module runtime is missing"))?;
    let networking = candidate.manifest.networking.clone();
    let (aliases, primary_alias) = build_aliases(
        &candidate.extension_id,
        &existing.instance_name,
        existing.instance_id,
        runtime.service_name.clone(),
    );

    let mut providers = Vec::new();
    for provide in &candidate.manifest.provides {
        let endpoint = match build_provider_endpoint(provide, &networking, &primary_alias) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                conflicts.push(conflict_invalid_endpoint(
                    &candidate.extension_id,
                    &provide.capability,
                    &provide.slot,
                    &err,
                    Some("create_instances"),
                ));
                continue;
            }
        };
        providers.push(ProviderSpec {
            provider_id: existing_provider_ids
                .get(&provider_identity_key(
                    existing.instance_id,
                    &provide.capability,
                    &provide.slot,
                ))
                .copied()
                .unwrap_or_else(|| {
                    stable_provider_id(existing.instance_id, &provide.capability, &provide.slot)
                }),
            instance_id: existing.instance_id,
            capability: provide.capability.clone(),
            slot_id: provide.slot.clone(),
            cardinality: provide.cardinality.unwrap_or(SlotCardinality::One),
            implementation: provide.implementation.clone(),
            scope_json: provide
                .scope
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|err| anyhow::anyhow!("serializing provider scope: {err}"))?,
            endpoint,
        });
    }

    if providers.is_empty() {
        bail!(
            "module '{}' has no usable providers",
            candidate.extension_id
        );
    }

    Ok(PlannedInstance {
        instance: InstanceSpec {
            instance_id: existing.instance_id,
            extension_id: existing.extension_id.clone(),
            instance_name: existing.instance_name.clone(),
            config_json: existing.config_json.clone(),
            enabled: existing.enabled,
        },
        runtime: RuntimeSpec {
            instance_id: existing.instance_id,
            extension_id: existing.extension_id.clone(),
            instance_name: existing.instance_name.clone(),
            runtime,
            networking,
            aliases,
        },
        providers,
    })
}

pub fn stable_provider_id(instance_id: Uuid, capability: &str, slot: &str) -> Uuid {
    let key = provider_identity_key(instance_id, capability, slot);
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
}

fn provider_identity_key(instance_id: Uuid, capability: &str, slot: &str) -> String {
    format!("{instance_id}:{capability}:{slot}")
}

pub fn build_provider_endpoint(
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
        .ok_or_else(|| {
            anyhow::anyhow!(
                "service port missing for capability '{}'",
                provide.capability
            )
        })?;

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
    instance
        .providers
        .iter()
        .find(|provider| provider.capability == want.capability && provider.slot_id == want.slot)
}

fn conflict_missing_provider(
    want: &ManifestCapabilityRef,
    detail: Option<&str>,
    stage_id: Option<&str>,
) -> serde_json::Value {
    let conflict = serde_json::json!({
        "code": "missing_provider",
        "capability": want.capability,
        "slot": want.slot,
    });
    let mut conflict = if let Some(stage_id) = stage_id {
        conflict_with_stage(conflict, stage_id)
    } else {
        conflict
    };
    if let Some(detail) = detail {
        if let Some(obj) = conflict.as_object_mut() {
            obj.insert(
                "detail".to_string(),
                serde_json::Value::String(detail.to_string()),
            );
        }
    }
    conflict
}

fn conflict_invalid_endpoint(
    extension_id: &str,
    capability: &str,
    slot: &str,
    err: &anyhow::Error,
    stage_id: Option<&str>,
) -> serde_json::Value {
    let conflict = serde_json::json!({
        "code": "invalid_provider_endpoint",
        "extension_id": extension_id,
        "capability": capability,
        "slot": slot,
        "detail": err.to_string(),
    });
    if let Some(stage_id) = stage_id {
        conflict_with_stage(conflict, stage_id)
    } else {
        conflict
    }
}

fn conflict_driver_patch(
    target: &ManifestCapabilityRef,
    connector_id: &str,
    detail: &str,
    stage_id: Option<&str>,
) -> serde_json::Value {
    let conflict = serde_json::json!({
        "code": "driver_patch_invalid",
        "connector_id": connector_id,
        "capability": target.capability,
        "slot": target.slot,
        "detail": detail,
    });
    if let Some(stage_id) = stage_id {
        conflict_with_stage(conflict, stage_id)
    } else {
        conflict
    }
}

fn conflict_missing_required_secrets(
    extension_id: &str,
    instance_id: Uuid,
    instance_name: &str,
    missing: &[String],
    stage_id: Option<&str>,
) -> serde_json::Value {
    let conflict = serde_json::json!({
        "code": "missing_required_secrets",
        "extension_id": extension_id,
        "instance_id": instance_id,
        "instance_name": instance_name,
        "missing": missing,
    });
    if let Some(stage_id) = stage_id {
        conflict_with_stage(conflict, stage_id)
    } else {
        conflict
    }
}

fn conflict_ownership_invalid(
    connector_id: &str,
    domain: &str,
    detail: &str,
    stage_id: Option<&str>,
) -> serde_json::Value {
    let conflict = serde_json::json!({
        "code": "ownership_invalid",
        "connector_id": connector_id,
        "domain": domain,
        "detail": detail,
    });
    if let Some(stage_id) = stage_id {
        conflict_with_stage(conflict, stage_id)
    } else {
        conflict
    }
}

fn conflict_ownership_conflict(
    domain: &str,
    connector_id: &str,
    existing_connector_id: &str,
    stage_id: Option<&str>,
) -> serde_json::Value {
    let conflict = serde_json::json!({
        "code": "ownership_conflict",
        "domain": domain,
        "connector_id": connector_id,
        "existing_connector_id": existing_connector_id,
        "detail": format!(
            "ownership domain '{}' is already claimed by '{}'",
            domain, existing_connector_id
        ),
    });
    if let Some(stage_id) = stage_id {
        conflict_with_stage(conflict, stage_id)
    } else {
        conflict
    }
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
                .get_secret(
                    crate::db::models::SecretScope::Instance,
                    Some(instance_id),
                    &key,
                )
                .await?
                .is_some();
            if !exists {
                missing.insert(format!("instance:{}:{}", instance_id, key));
            }
        }
    }
    Ok(missing.into_iter().collect())
}

async fn missing_downloader_secrets_for_patch(
    _store: &ExtensionStore<'_>,
    patch: &DriverPatch,
) -> Result<Vec<String>> {
    let downloaders: Vec<_> = match patch {
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetDownloaders { downloaders }) => {
            downloaders.iter().collect()
        }
        DriverPatch::MediaManagerMovies(MediaManagerMoviesPatch::SetDownloaders {
            downloaders,
        }) => downloaders.iter().collect(),
        _ => Vec::new(),
    };
    for downloader in downloaders {
        if !is_auto_managed_downloader(&downloader.r#type) {
            continue;
        }
        if downloader_has_credentials(downloader) {
            continue;
        }
        // Built-in downloader credentials are auto-generated on first run.
        continue;
    }
    Ok(Vec::new())
}

fn is_auto_managed_downloader(implementation: &str) -> bool {
    is_qbittorrent_downloader(implementation) || is_nzbget_downloader(implementation)
}

fn is_qbittorrent_downloader(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("qbittorrent")
}

fn is_nzbget_downloader(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("nzbget")
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
        return Some((
            instance.extension_id.clone(),
            instance.instance_name.clone(),
        ));
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

fn push_provider_readiness_actions(
    actions: &mut Vec<PlanAction>,
    provider_id: Uuid,
    timeout_seconds: u64,
    transport_gate_targets: &mut HashSet<Uuid>,
    bootstrap_gate_targets: &mut HashSet<Uuid>,
    health_gate_targets: &mut HashSet<Uuid>,
) {
    if transport_gate_targets.insert(provider_id) {
        actions.push(PlanAction::TransportGate {
            provider_id,
            timeout_seconds,
        });
    }
    if bootstrap_gate_targets.insert(provider_id) {
        actions.push(PlanAction::BootstrapGate {
            provider_id,
            timeout_seconds,
        });
    }
    if health_gate_targets.insert(provider_id) {
        actions.push(PlanAction::HealthGate {
            provider_id,
            timeout_seconds,
        });
    }
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
    use crate::db::models::{
        ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality,
    };
    use crate::extensions::store::{
        ExtensionStore, NewExtension, NewExtensionInstance, NewProvider,
    };
    use crate::orchestrator::model::ProviderEndpoint;

    fn module_manifest(id: &str, capability: &str) -> serde_json::Value {
        module_manifest_with_slot(id, capability, "default")
    }

    fn module_manifest_with_slot(id: &str, capability: &str, slot: &str) -> serde_json::Value {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "module",
            "name": id,
            "provides": [
                {
                    "capability": capability,
                    "slot": slot,
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

    fn module_manifest_with_downloader_runtime_secrets(
        id: &str,
        capability: &str,
        implementation: &str,
        username_secret: &str,
        password_secret: &str,
    ) -> serde_json::Value {
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
                    "implementation": implementation
                }
            ],
            "runtime": {
                "type": "container",
                "image": "example/module:1.0.0",
                "env": [
                    {
                        "name": "DOWNLOADER_USERNAME",
                        "from_secret": format!("instance:{username_secret}")
                    },
                    {
                        "name": "DOWNLOADER_PASSWORD",
                        "from_secret": format!("instance:{password_secret}")
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

    fn connector_manifest_with_nzbget(id: &str, target_capability: &str) -> serde_json::Value {
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
                                "name": "NZBGet",
                                "type": "nzbget",
                                "url": "http://elx-nzbget:6789",
                                "enabled": true
                            }
                        ]
                    }
                }
            ]
        })
    }

    fn execution_blueprint(
        id: &str,
        packages: Vec<&str>,
        instances: Vec<serde_json::Value>,
        phases: Vec<serde_json::Value>,
        ownership: Vec<serde_json::Value>,
    ) -> serde_json::Value {
        let manifest = json!({
            "id": id,
            "version": "1.0.0",
            "kind": "blueprint",
            "name": id,
            "execution": {
                "packages": packages,
                "instances": instances,
                "ownership": ownership,
                "phases": phases
            }
        });
        manifest
    }

    fn execution_blueprint_single_instance(
        id: &str,
        extension_id: &str,
        instance_id: &str,
    ) -> serde_json::Value {
        execution_blueprint(
            id,
            vec![extension_id],
            vec![json!({
                "id": instance_id,
                "extension_id": extension_id,
                "name": "default"
            })],
            vec![
                json!({
                    "id": "install_packages",
                    "steps": [
                        { "type": "ensure_package_installed", "extension_id": extension_id }
                    ]
                }),
                json!({
                    "id": "create_instances",
                    "steps": [
                        { "type": "ensure_instance_installed", "instance": instance_id }
                    ]
                }),
                json!({
                    "id": "start_runtime",
                    "steps": [
                        { "type": "ensure_runtime_running", "instance": instance_id }
                    ]
                }),
                json!({
                    "id": "register_providers",
                    "steps": [
                        { "type": "create_or_update_providers", "instance": instance_id }
                    ]
                }),
            ],
            Vec::new(),
        )
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
                scope_json: None,
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
                scope_json: None,
                endpoint_json: Some(endpoint),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok((instance_id, provider_id))
    }

    #[tokio::test]
    async fn planner_conflict_on_invalid_endpoint() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.consumer",
            ExtensionKind::Module,
            json!({
                "id": "ext.consumer",
                "version": "1.0.0",
                "kind": "module",
                "name": "ext.consumer",
                "provides": [
                    {
                        "capability": "media.manager.tv",
                        "slot": "default",
                        "cardinality": "one",
                        "implementation": "test"
                    }
                ],
                "runtime": {
                    "type": "container",
                    "image": "example/module:1.0.0"
                }
            }),
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
            execution_blueprint(
                "blueprint.test",
                vec!["ext.consumer", "ext.indexer"],
                vec![
                    json!({
                        "id": "consumer",
                        "extension_id": "ext.consumer",
                        "name": "default"
                    }),
                    json!({
                        "id": "indexer",
                        "extension_id": "ext.indexer",
                        "name": "default"
                    }),
                ],
                vec![
                    json!({
                        "id": "install_packages",
                        "steps": [
                            { "type": "ensure_package_installed", "extension_id": "ext.consumer" },
                            { "type": "ensure_package_installed", "extension_id": "ext.indexer" }
                        ]
                    }),
                    json!({
                        "id": "create_instances",
                        "steps": [
                            { "type": "ensure_instance_installed", "instance": "consumer" },
                            { "type": "ensure_instance_installed", "instance": "indexer" }
                        ]
                    }),
                    json!({
                        "id": "start_runtime",
                        "steps": [
                            { "type": "ensure_runtime_running", "instance": "consumer" },
                            { "type": "ensure_runtime_running", "instance": "indexer" }
                        ]
                    }),
                    json!({
                        "id": "register_providers",
                        "steps": [
                            { "type": "create_or_update_providers", "instance": "consumer" },
                            { "type": "create_or_update_providers", "instance": "indexer" }
                        ]
                    }),
                    json!({
                        "id": "wire_apps",
                        "steps": [
                            {
                                "type": "apply_binding",
                                "consumer_instance": "consumer",
                                "consumer_capability": "media.manager.tv",
                                "provider_instance": "indexer",
                                "provider_capability": "indexer.registry"
                            }
                        ]
                    }),
                ],
                Vec::new(),
            ),
        )
        .await?;

        let indexer_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-indexer".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;

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
                conflict.get("code") == Some(&json!("invalid_provider_endpoint"))
            })
        );
        let blocked = plan.blocked_stage.expect("blocked stage");
        assert_eq!(blocked.stage_id, "create_instances");
        assert_eq!(blocked.code, "invalid_provider_endpoint");
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
            execution_blueprint_single_instance("blueprint.secret", "ext.secret", "secret"),
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.secret".to_string(), None)
            .await?;
        assert!(plan.conflicts.iter().any(|conflict| {
            conflict.get("code") == Some(&json!("missing_required_secrets"))
                && conflict
                    .get("missing")
                    .and_then(|missing| missing.as_array())
                    .map(|missing| {
                        missing.iter().any(|entry| {
                            entry
                                .as_str()
                                .map(|value| {
                                    value.contains("instance:") && value.ends_with(":api_key")
                                })
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false)
        }));

        Ok(())
    }

    #[tokio::test]
    async fn planner_emits_runtime_asset_install_and_restart_actions() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.prowlarr",
            ExtensionKind::Module,
            module_manifest("ext.prowlarr", "indexer.registry"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.prowlarr.asset",
            ExtensionKind::Connector,
            json!({
                "id": "ext.prowlarr.asset",
                "version": "1.2.3",
                "kind": "connector",
                "name": "Prowlarr Asset",
                "targets": [
                    {
                        "capability": "indexer.registry",
                        "slot": "default"
                    }
                ],
                "actions": [
                    {
                        "type": "driver_patch",
                        "target": {
                            "capability": "indexer.registry",
                            "slot": "default"
                        },
                        "patch": {
                            "op": "register_indexers",
                            "indexers": []
                        }
                    }
                ]
            }),
        )
        .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.prowlarr".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        insert_extension(
            &store,
            "blueprint.ext.runtime_asset",
            ExtensionKind::Blueprint,
            execution_blueprint(
                "blueprint.ext.runtime_asset",
                vec!["ext.prowlarr", "ext.prowlarr.asset"],
                vec![json!({
                    "id": "prowlarr",
                    "extension_id": "ext.prowlarr",
                    "name": "default"
                })],
                vec![json!({
                    "id": "install_definition",
                    "steps": [
                        {
                            "type": "install_runtime_asset",
                            "source_extension_id": "ext.prowlarr.asset",
                            "source_path": "assets/config/custom-indexer.yml",
                            "target_instance": "prowlarr",
                            "destination_path": "/config/Definitions/Custom/custom-indexer.yml"
                        },
                        {
                            "type": "restart_runtime",
                            "instance": "prowlarr"
                        }
                    ]
                })],
                Vec::new(),
            ),
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.ext.runtime_asset".to_string(), None)
            .await?;

        let asset = plan
            .actions
            .iter()
            .find_map(|action| match action {
                PlanAction::InstallRuntimeAsset { asset } => Some(asset),
                _ => None,
            })
            .expect("runtime asset action");
        assert_eq!(asset.target_instance_id, instance_id);
        assert_eq!(asset.source_extension_id, "ext.prowlarr.asset");
        assert_eq!(asset.source_extension_version, "1.0.0");
        assert_eq!(asset.source_path, "assets/config/custom-indexer.yml");
        assert_eq!(
            asset.destination_path,
            "/config/Definitions/Custom/custom-indexer.yml"
        );
        assert!(plan.actions.iter().any(|action| matches!(
            action,
            PlanAction::RestartRuntime { instance_id: restart_instance_id }
            if *restart_instance_id == instance_id
        )));

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
            execution_blueprint(
                "blueprint.qbittorrent",
                vec!["ext.sonarr", "ext.qbittorrent", "ext.sonarr.qbittorrent"],
                vec![
                    json!({
                        "id": "sonarr",
                        "extension_id": "ext.sonarr",
                        "name": "default"
                    }),
                    json!({
                        "id": "qbittorrent",
                        "extension_id": "ext.qbittorrent",
                        "name": "default"
                    })
                ],
                vec![
                    json!({
                        "id": "install_packages",
                        "steps": [
                            { "type": "ensure_package_installed", "extension_id": "ext.sonarr" },
                            { "type": "ensure_package_installed", "extension_id": "ext.qbittorrent" },
                            { "type": "ensure_package_installed", "extension_id": "ext.sonarr.qbittorrent" }
                        ]
                    }),
                    json!({
                        "id": "create_instances",
                        "steps": [
                            { "type": "ensure_instance_installed", "instance": "sonarr" },
                            { "type": "ensure_instance_installed", "instance": "qbittorrent" }
                        ]
                    }),
                    json!({
                        "id": "start_runtime",
                        "steps": [
                            { "type": "ensure_runtime_running", "instance": "sonarr" },
                            { "type": "ensure_runtime_running", "instance": "qbittorrent" }
                        ]
                    }),
                    json!({
                        "id": "register_providers",
                        "steps": [
                            { "type": "create_or_update_providers", "instance": "sonarr" },
                            { "type": "create_or_update_providers", "instance": "qbittorrent" }
                        ]
                    }),
                    json!({
                        "id": "configure_downloaders",
                        "steps": [
                            {
                                "type": "apply_connector",
                                "connector_id": "ext.sonarr.qbittorrent",
                                "target_instance": "sonarr",
                                "target_capability": "media.manager.tv",
                                "ownership_domains": ["sonarr.download_clients.qbittorrent"]
                            }
                        ]
                    })
                ],
                vec![json!({
                    "domain": "sonarr.download_clients.qbittorrent",
                    "owner": "ext.sonarr.qbittorrent"
                })],
            ),
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
    async fn planner_omits_auto_managed_nzbget_runtime_secrets() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(
            &store,
            "ext.nzbget",
            ExtensionKind::Module,
            module_manifest_with_downloader_runtime_secrets(
                "ext.nzbget",
                "downloader.nzb",
                "nzbget",
                "nzbget_username",
                "nzbget_password",
            ),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.nzbget.runtime",
            ExtensionKind::Blueprint,
            execution_blueprint_single_instance("blueprint.nzbget.runtime", "ext.nzbget", "nzbget"),
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.nzbget.runtime".to_string(), None)
            .await?;

        let mut saw_nzbget_missing = false;
        for conflict in &plan.conflicts {
            if conflict.get("code") != Some(&json!("missing_required_secrets")) {
                continue;
            }
            let missing = conflict.get("missing").and_then(|value| value.as_array());
            let Some(missing) = missing else { continue };
            for entry in missing {
                if let Some(value) = entry.as_str() {
                    if value.ends_with(":nzbget_username") || value.ends_with(":nzbget_password") {
                        saw_nzbget_missing = true;
                    }
                }
            }
        }

        assert!(
            !saw_nzbget_missing,
            "nzbget credentials should be auto-generated"
        );

        Ok(())
    }

    #[tokio::test]
    async fn extensions_conflicts_on_missing_nzbget_secrets() -> Result<()> {
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
            "ext.nzbget",
            ExtensionKind::Module,
            module_manifest("ext.nzbget", "downloader.nzb"),
        )
        .await?;
        insert_extension(
            &store,
            "ext.sonarr.nzbget",
            ExtensionKind::Connector,
            connector_manifest_with_nzbget("ext.sonarr.nzbget", "media.manager.tv"),
        )
        .await?;
        insert_extension(
            &store,
            "blueprint.nzbget",
            ExtensionKind::Blueprint,
            execution_blueprint(
                "blueprint.nzbget",
                vec!["ext.sonarr", "ext.nzbget", "ext.sonarr.nzbget"],
                vec![
                    json!({
                        "id": "sonarr",
                        "extension_id": "ext.sonarr",
                        "name": "default"
                    }),
                    json!({
                        "id": "nzbget",
                        "extension_id": "ext.nzbget",
                        "name": "default"
                    })
                ],
                vec![
                    json!({
                        "id": "install_packages",
                        "steps": [
                            { "type": "ensure_package_installed", "extension_id": "ext.sonarr" },
                            { "type": "ensure_package_installed", "extension_id": "ext.nzbget" },
                            { "type": "ensure_package_installed", "extension_id": "ext.sonarr.nzbget" }
                        ]
                    }),
                    json!({
                        "id": "create_instances",
                        "steps": [
                            { "type": "ensure_instance_installed", "instance": "sonarr" },
                            { "type": "ensure_instance_installed", "instance": "nzbget" }
                        ]
                    }),
                    json!({
                        "id": "start_runtime",
                        "steps": [
                            { "type": "ensure_runtime_running", "instance": "sonarr" },
                            { "type": "ensure_runtime_running", "instance": "nzbget" }
                        ]
                    }),
                    json!({
                        "id": "register_providers",
                        "steps": [
                            { "type": "create_or_update_providers", "instance": "sonarr" },
                            { "type": "create_or_update_providers", "instance": "nzbget" }
                        ]
                    }),
                    json!({
                        "id": "configure_downloaders",
                        "steps": [
                            {
                                "type": "apply_connector",
                                "connector_id": "ext.sonarr.nzbget",
                                "target_instance": "sonarr",
                                "target_capability": "media.manager.tv",
                                "ownership_domains": ["sonarr.download_clients.nzbget"]
                            }
                        ]
                    })
                ],
                vec![json!({
                    "domain": "sonarr.download_clients.nzbget",
                    "owner": "ext.sonarr.nzbget"
                })],
            ),
        )
        .await?;

        let nzbget_endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "elx-nzbget".to_string(),
            6789,
            None,
            None,
        )?;
        let _ = insert_provider_with_impl(
            &store,
            "ext.nzbget",
            "downloader.nzb",
            "nzbget",
            serde_json::to_value(nzbget_endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.nzbget".to_string(), None)
            .await?;

        let mut saw_nzbget_missing = false;
        for conflict in &plan.conflicts {
            if conflict.get("code") != Some(&json!("missing_required_secrets")) {
                continue;
            }
            let missing = conflict.get("missing").and_then(|value| value.as_array());
            let Some(missing) = missing else { continue };
            for entry in missing {
                if let Some(value) = entry.as_str() {
                    if value.ends_with(":nzbget_username") || value.ends_with(":nzbget_password") {
                        saw_nzbget_missing = true;
                    }
                }
            }
        }

        assert!(
            !saw_nzbget_missing,
            "nzbget credentials should be auto-generated"
        );

        Ok(())
    }

    #[tokio::test]
    async fn planner_conflicts_on_repeated_ownership_domain_claim() -> Result<()> {
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
            "blueprint.ownership",
            ExtensionKind::Blueprint,
            execution_blueprint(
                "blueprint.ownership",
                vec!["ext.sonarr", "ext.connector"],
                vec![json!({
                    "id": "sonarr",
                    "extension_id": "ext.sonarr",
                    "name": "default"
                })],
                vec![
                    json!({
                        "id": "install_packages",
                        "steps": [
                            { "type": "ensure_package_installed", "extension_id": "ext.sonarr" },
                            { "type": "ensure_package_installed", "extension_id": "ext.connector" }
                        ]
                    }),
                    json!({
                        "id": "create_instances",
                        "steps": [
                            { "type": "ensure_instance_installed", "instance": "sonarr" }
                        ]
                    }),
                    json!({
                        "id": "start_runtime",
                        "steps": [
                            { "type": "ensure_runtime_running", "instance": "sonarr" }
                        ]
                    }),
                    json!({
                        "id": "register_providers",
                        "steps": [
                            { "type": "create_or_update_providers", "instance": "sonarr" }
                        ]
                    }),
                    json!({
                        "id": "configure_defaults",
                        "steps": [
                            {
                                "type": "apply_connector",
                                "connector_id": "ext.connector",
                                "target_instance": "sonarr",
                                "target_capability": "media.manager.tv",
                                "ownership_domains": ["sonarr.defaults"]
                            }
                        ]
                    }),
                    json!({
                        "id": "configure_again",
                        "steps": [
                            {
                                "type": "apply_connector",
                                "connector_id": "ext.connector",
                                "target_instance": "sonarr",
                                "target_capability": "media.manager.tv",
                                "ownership_domains": ["sonarr.defaults"]
                            }
                        ]
                    }),
                ],
                vec![json!({
                    "domain": "sonarr.defaults",
                    "owner": "ext.connector"
                })],
            ),
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-sonarr".to_string(),
            8989,
            None,
            Some("elixir_net".to_string()),
        )?;
        let _ = insert_provider(
            &store,
            "ext.sonarr",
            "media.manager.tv",
            serde_json::to_value(endpoint)?,
        )
        .await?;

        let planner = Planner::new();
        let plan = planner
            .plan_blueprint(&store, "blueprint.ownership".to_string(), None)
            .await?;

        assert!(plan.conflicts.iter().any(|conflict| {
            conflict.get("code") == Some(&json!("ownership_conflict"))
                && conflict.get("domain") == Some(&json!("sonarr.defaults"))
                && conflict.get("stage_id") == Some(&json!("configure_again"))
        }));
        let blocked = plan.blocked_stage.expect("blocked stage");
        assert_eq!(blocked.stage_id, "configure_again");
        assert_eq!(blocked.code, "ownership_conflict");

        Ok(())
    }

    #[tokio::test]
    async fn planner_orders_full_readiness_sequence_before_driver_patch() -> Result<()> {
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
            execution_blueprint(
                "blueprint.health",
                vec!["ext.sonarr", "ext.connector"],
                vec![json!({
                    "id": "sonarr",
                    "extension_id": "ext.sonarr",
                    "name": "default"
                })],
                vec![
                    json!({
                        "id": "install",
                        "steps": [
                            { "type": "ensure_package_installed", "extension_id": "ext.sonarr" },
                            { "type": "ensure_package_installed", "extension_id": "ext.connector" }
                        ]
                    }),
                    json!({
                        "id": "instance",
                        "steps": [
                            { "type": "ensure_instance_installed", "instance": "sonarr" }
                        ]
                    }),
                    json!({
                        "id": "runtime",
                        "steps": [
                            { "type": "ensure_runtime_running", "instance": "sonarr" }
                        ]
                    }),
                    json!({
                        "id": "providers",
                        "steps": [
                            { "type": "create_or_update_providers", "instance": "sonarr" }
                        ]
                    }),
                    json!({
                        "id": "patch",
                        "steps": [
                            {
                                "type": "apply_connector",
                                "connector_id": "ext.connector",
                                "target_instance": "sonarr",
                                "target_capability": "media.manager.tv",
                                "ownership_domains": ["sonarr.defaults"]
                            }
                        ]
                    }),
                ],
                vec![json!({
                    "domain": "sonarr.defaults",
                    "owner": "ext.connector"
                })],
            ),
        )
        .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-sonarr".to_string(),
            8989,
            None,
            Some("elixir_net".to_string()),
        )?;
        let _ = insert_provider(
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

        let (patch_index, patch_provider_id) = plan
            .actions
            .iter()
            .enumerate()
            .find_map(|(idx, action)| match action {
                PlanAction::ApplyDriverPatch { patch, .. } => Some((idx, patch.target_provider_id)),
                _ => None,
            })
            .expect("driver patch action");
        let transport_index = plan
            .actions
            .iter()
            .enumerate()
            .find_map(|(idx, action)| match action {
                PlanAction::TransportGate { provider_id, .. }
                    if *provider_id == patch_provider_id =>
                {
                    Some(idx)
                }
                _ => None,
            })
            .expect("transport gate action");
        let bootstrap_index = plan
            .actions
            .iter()
            .enumerate()
            .find_map(|(idx, action)| match action {
                PlanAction::BootstrapGate { provider_id, .. }
                    if *provider_id == patch_provider_id =>
                {
                    Some(idx)
                }
                _ => None,
            })
            .expect("bootstrap gate action");
        let health_index = plan
            .actions
            .iter()
            .enumerate()
            .find_map(|(idx, action)| match action {
                PlanAction::HealthGate { provider_id, .. } if *provider_id == patch_provider_id => {
                    Some(idx)
                }
                _ => None,
            })
            .expect("health gate action");
        assert!(
            transport_index < bootstrap_index,
            "transport gate should precede bootstrap gate"
        );
        assert!(
            bootstrap_index < health_index,
            "bootstrap gate should precede health gate"
        );
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
            execution_blueprint_single_instance("blueprint.upgrade", "ext.upgrade", "upgrade"),
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
