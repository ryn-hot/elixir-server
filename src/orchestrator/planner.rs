use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::models::{
    BindingStatus, ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality,
};
use crate::extensions::manifest::{
    ExtensionManifest, ManifestBinding, ManifestCapabilityRef, ManifestPreferences,
    ManifestRuntime, ManifestNetworking,
};
use crate::extensions::store::{ExtensionStore, NewBinding, ProviderDetails};
use crate::orchestrator::executor::ExecutorAction;
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
    EnsureRuntimeRunning {
        runtime: RuntimeSpec,
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
            PlanAction::EnsureRuntimeRunning { .. } => "ensure_runtime_running",
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
            PlanAction::EnsureRuntimeRunning { runtime } => Ok(ExecutorAction::EnsureRuntimeRunning {
                instance_id: runtime.instance_id,
                extension_id: runtime.extension_id,
                instance_name: runtime.instance_name,
                runtime: runtime.runtime,
                networking: runtime.networking,
                aliases: runtime.aliases,
            }),
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

        let mut used_instance_names = group_instance_names(&instances);
        let module_catalog = build_module_catalog(&extensions, allow_community)?;
        let connector_catalog = build_connector_catalog(&extensions, allow_community)?;

        let mut plan = Plan::new(blueprint_id, params);
        let mut selections: HashMap<String, ProviderSelection> = HashMap::new();
        let mut planned_instances: HashMap<String, PlannedInstance> = HashMap::new();

        for want in &manifest.wants {
            let key = capability_key(want);
            if selections.contains_key(&key) {
                continue;
            }

            if reuse_existing {
                let candidates = providers_for_want(&providers, want, allow_community);
                if let Some(selected) =
                    select_existing_provider(candidates, manifest.preferences.as_ref(), want, &mut plan.conflicts)
                {
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

        let mut actions = Vec::new();
        let mut instance_entries: Vec<&PlannedInstance> = planned_instances.values().collect();
        instance_entries.sort_by(|a, b| a.instance.extension_id.cmp(&b.instance.extension_id));
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

        plan.actions = actions;
        Ok(plan)
    }
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
) -> Result<Vec<ConnectorCandidate>> {
    let mut connectors = Vec::new();
    for extension in extensions {
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

    let (aliases, primary_alias) =
        build_aliases(&candidate.extension_id, &instance_name, instance_id, runtime.service_name.clone());

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

fn build_aliases(
    extension_id: &str,
    instance_name: &str,
    instance_id: Uuid,
    service_name: Option<String>,
) -> (Vec<String>, String) {
    let short_id = short_instance_id(instance_id);
    let slug = slugify(extension_id);
    let primary_alias = format!("svc-{}-{}", slug, instance_name);
    let mut aliases = vec![format!("svc-{}", short_id), primary_alias.clone()];
    if let Some(service_name) = service_name {
        if !aliases.contains(&service_name) {
            aliases.push(service_name);
        }
    }
    (aliases, primary_alias)
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

fn trust_allowed(level: ExtensionTrustLevel, allow_community: bool) -> bool {
    match level {
        ExtensionTrustLevel::Verified => true,
        ExtensionTrustLevel::Community => allow_community,
        ExtensionTrustLevel::Untrusted => false,
    }
}

fn slugify(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_string()
}

fn short_instance_id(instance_id: Uuid) -> String {
    let raw = instance_id.simple().to_string();
    raw.chars().take(6).collect()
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
                    "patch": { "op": "noop" }
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
}
