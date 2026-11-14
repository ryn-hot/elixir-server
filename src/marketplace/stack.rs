//! Container stacks: a `source-js-v1` module may declare container modules
//! (upstream images pinned by digest), connector data and setup steps. The
//! verified manifest holds every member inline, and the module's own source
//! is activated as usual. After activation each member becomes a derived
//! local row (`stack.<id>.<name>`) that the existing orchestrator runs under
//! the stack sandbox. See docs/architecture/arr-stack-marketplace-extension.md.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use sha2::{Digest as _, Sha256};

use crate::db::models::ExtensionKind;
use crate::extensions::manifest::{
    ExtensionManifest, ManifestExecutionStep, ManifestRuntimeSecurity, ManifestStack,
};

/// Reserved prefix of derived member IDs. Legacy installs refuse it, so only
/// marketplace materialization can create such rows.
pub(crate) const STACK_MEMBER_PREFIX: &str = "stack.";
/// Built-in extensions a stack's setup steps may use without shipping them.
pub(crate) const BUILT_IN_STACK_REFERENCES: [&str; 2] =
    ["elixir.modules.qbittorrent", "elixir.modules.nzbget"];
/// Services a stack module may provide (the server's existing drivers).
const STACK_MODULE_CAPABILITIES: [&str; 7] = [
    "media.manager.tv",
    "media.manager.movies",
    "media.manager.anime",
    "media.search.tv",
    "media.search.movies",
    "media.search.anime",
    "indexer.registry",
];

pub(crate) fn is_stack_member_id(extension_id: &str) -> bool {
    extension_id.starts_with(STACK_MEMBER_PREFIX)
}

fn member_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 32
        || name.starts_with('-')
        || name.ends_with('-')
        || name.contains("--")
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        bail!("invalid stack member name '{name}'");
    }
    Ok(())
}

fn no_nested_controls(member: &ExtensionManifest) -> Result<()> {
    if member.stack.is_some()
        || member.publisher.is_some()
        || member.trust.is_some()
        || member.owner_release.is_some()
        || member.control_surface.is_some()
    {
        bail!("stack member '{}' declares a reserved field", member.id);
    }
    Ok(())
}

/// Every member reference used by setup steps, connectors and add-ons.
fn blueprint_references(blueprint: &ExtensionManifest) -> Vec<&str> {
    let mut references: Vec<&str> = blueprint.connectors.iter().map(String::as_str).collect();
    references.extend(
        blueprint
            .optional_addons
            .iter()
            .map(|addon| addon.extension_id.as_str()),
    );
    if let Some(execution) = &blueprint.execution {
        references.extend(execution.packages.iter().map(String::as_str));
        references.extend(
            execution
                .instances
                .iter()
                .map(|instance| instance.extension_id.as_str()),
        );
        references.extend(
            execution
                .ownership
                .iter()
                .map(|domain| domain.owner.as_str()),
        );
        for phase in &execution.phases {
            for step in &phase.steps {
                match step {
                    ManifestExecutionStep::EnsurePackageInstalled { extension_id } => {
                        references.push(extension_id)
                    }
                    ManifestExecutionStep::ApplyConnector { connector_id, .. } => {
                        references.push(connector_id)
                    }
                    _ => {}
                }
            }
        }
    }
    references
}

/// Typed member rules, after the crate's structural `check_stack`.
pub(crate) fn check_members(stack: &ManifestStack, release_permissions: &[String]) -> Result<()> {
    let permissions: BTreeSet<&str> = release_permissions.iter().map(String::as_str).collect();
    let mut names = BTreeSet::new();
    let members = stack
        .modules
        .iter()
        .chain(&stack.connectors)
        .chain(std::iter::once(stack.blueprint.as_ref()));
    for member in members {
        member_name(&member.id)?;
        if !names.insert(member.id.as_str()) {
            bail!("duplicate stack member '{}'", member.id);
        }
        member
            .validate()
            .with_context(|| format!("invalid stack member '{}'", member.id))?;
        no_nested_controls(member)?;
        if let Some(missing) = member
            .permissions
            .iter()
            .find(|permission| !permissions.contains(permission.as_str()))
        {
            bail!(
                "stack member '{}' requests '{missing}' outside the release",
                member.id
            );
        }
    }
    for module in &stack.modules {
        if module.kind != ExtensionKind::Module
            || !module.actions.is_empty()
            || module.execution.is_some()
        {
            bail!("stack module '{}' must be a container module", module.id);
        }
        let runtime = module
            .runtime
            .as_ref()
            .with_context(|| format!("stack module '{}' needs a runtime", module.id))?;
        let image = runtime.image.as_deref().unwrap_or_default();
        elixir_marketplace::upstream_image::check_image_reference(image)
            .with_context(|| format!("stack module '{}' image", module.id))?;
        if runtime.r#type != "container"
            || runtime.network.is_some()
            || runtime.service_name.is_some()
            || runtime.egress.is_some()
            || serde_json::to_value(&runtime.security)?
                != serde_json::to_value(ManifestRuntimeSecurity::default())?
            || runtime
                .ports
                .iter()
                .any(|port| port.host.is_some() || port.container == 0)
            || runtime
                .env
                .iter()
                .any(|env| env.from_secret.is_some() || env.value.is_none())
        {
            bail!(
                "stack module '{}' selects a runtime control Elixir owns",
                module.id
            );
        }
        for volume in &runtime.volumes {
            elixir_marketplace::upstream_image::check_folder_role(volume)
                .with_context(|| format!("stack module '{}' folder", module.id))?;
        }
        if let Some(provide) = module
            .provides
            .iter()
            .find(|provide| !STACK_MODULE_CAPABILITIES.contains(&provide.capability.as_str()))
        {
            bail!(
                "stack module '{}' provides unsupported service '{}'",
                module.id,
                provide.capability
            );
        }
    }
    for connector in &stack.connectors {
        if connector.kind != ExtensionKind::Connector
            || connector.runtime.is_some()
            || connector
                .actions
                .iter()
                .any(|action| action.r#type != "driver_patch")
        {
            bail!(
                "stack connector '{}' may only carry driver patches",
                connector.id
            );
        }
    }
    let blueprint = stack.blueprint.as_ref();
    if blueprint.kind != ExtensionKind::Blueprint
        || blueprint.runtime.is_some()
        || blueprint.execution.is_none()
    {
        bail!(
            "stack setup '{}' must be a blueprint with execution steps",
            blueprint.id
        );
    }
    for reference in blueprint_references(blueprint) {
        if !names.contains(reference) && !BUILT_IN_STACK_REFERENCES.contains(&reference) {
            bail!("stack setup references '{reference}', which is neither a member nor built in");
        }
    }
    Ok(())
}

/// Derived local ID of a member: `stack.<8 hex of the stack ID>.<member>`.
pub(crate) fn derived_id(stack_extension_id: &str, member: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(stack_extension_id.as_bytes()));
    format!("{STACK_MEMBER_PREFIX}{}.{member}", &digest[..8])
}

/// The members as local manifests, with member names replaced by derived IDs
/// everywhere the setup steps refer to them. Built-in references are kept.
pub(crate) fn materialize(
    stack_extension_id: &str,
    version: &str,
    stack: &ManifestStack,
) -> Result<Vec<ExtensionManifest>> {
    let ids: BTreeMap<String, String> = stack
        .modules
        .iter()
        .chain(&stack.connectors)
        .chain(std::iter::once(stack.blueprint.as_ref()))
        .map(|member| {
            (
                member.id.clone(),
                derived_id(stack_extension_id, &member.id),
            )
        })
        .collect();
    let map = |value: &mut String| {
        if let Some(derived) = ids.get(value.as_str()) {
            *value = derived.clone();
        }
    };
    let mut members = Vec::new();
    for member in stack
        .modules
        .iter()
        .chain(&stack.connectors)
        .chain(std::iter::once(stack.blueprint.as_ref()))
    {
        let mut member = member.clone();
        map(&mut member.id);
        member.version = version.to_owned();
        member.connectors.iter_mut().for_each(map);
        member
            .optional_addons
            .iter_mut()
            .for_each(|addon| map(&mut addon.extension_id));
        if let Some(execution) = member.execution.as_mut() {
            execution.packages.iter_mut().for_each(map);
            execution
                .instances
                .iter_mut()
                .for_each(|instance| map(&mut instance.extension_id));
            execution
                .ownership
                .iter_mut()
                .for_each(|domain| map(&mut domain.owner));
            for phase in &mut execution.phases {
                for step in &mut phase.steps {
                    match step {
                        ManifestExecutionStep::EnsurePackageInstalled { extension_id } => {
                            map(extension_id)
                        }
                        ManifestExecutionStep::ApplyConnector { connector_id, .. } => {
                            map(connector_id)
                        }
                        _ => {}
                    }
                }
            }
        }
        member
            .validate()
            .with_context(|| format!("materialized stack member '{}'", member.id))?;
        members.push(member);
    }
    Ok(members)
}

/// What one stack reconcile pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct StackReconcile {
    pub stacks: usize,
    pub members_written: usize,
    pub images_ready: usize,
    pub setups_offered: usize,
    pub members_disabled: usize,
}

/// Idempotent: for every enabled marketplace extension that declares a stack,
/// write its derived member rows, make each pinned image present, and offer
/// its setup once ("Finish setup"). Members whose stack is no longer enabled
/// are switched off. Runs before each orchestrator reconcile, so it also
/// recovers after a crash between activation and this step.
pub(crate) async fn reconcile(
    pool: &sqlx::AnyPool,
    docker: &crate::runtime::docker::DockerRuntimeManager,
) -> Result<StackReconcile> {
    use crate::db::models::ExtensionTrustLevel;
    use crate::extensions::store::{ExtensionStore, NewDesiredBlueprint, NewExtension};
    let store = ExtensionStore::new(pool);
    let extensions = store.list_extensions().await?;
    let mut summary = StackReconcile::default();
    let mut wanted = BTreeSet::new();
    let desired = store.list_desired_blueprints(None).await?;
    for stack_row in extensions
        .iter()
        .filter(|row| row.enabled && row.extension_id.starts_with("elx:"))
    {
        let manifest: ExtensionManifest =
            match serde_json::from_value(stack_row.manifest_json.clone()) {
                Ok(manifest) => manifest,
                Err(_) => continue,
            };
        let Some(stack) = manifest.stack.as_ref() else {
            continue;
        };
        summary.stacks += 1;
        check_members(stack, &manifest.permissions)
            .with_context(|| format!("stack '{}'", stack_row.extension_id))?;
        for member in materialize(&stack_row.extension_id, &stack_row.version, stack)? {
            wanted.insert(member.id.clone());
            if let Some(image) = member
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.image.as_deref())
            {
                docker
                    .ensure_pinned_image(image)
                    .await
                    .with_context(|| format!("stack image for '{}'", member.id))?;
                summary.images_ready += 1;
            }
            let manifest_json = serde_json::to_value(&member)?;
            let current = store.get_extension(&member.id).await?;
            if current.as_ref().is_none_or(|row| {
                row.version != member.version || row.manifest_json != manifest_json
            }) {
                store
                    .upsert_extension(&NewExtension {
                        extension_id: member.id.clone(),
                        name: member.name.clone(),
                        version: member.version.clone(),
                        kind: member.kind,
                        publisher_name: stack_row.publisher_name.clone(),
                        signing_key_id: stack_row.signing_key_id.clone(),
                        trust_level: ExtensionTrustLevel::Community,
                        manifest_json,
                        package_hash: stack_row.package_hash.clone(),
                        enabled: current.as_ref().is_none_or(|row| row.enabled),
                    })
                    .await?;
                summary.members_written += 1;
            }
            if member.kind == ExtensionKind::Blueprint
                && !desired
                    .iter()
                    .any(|entry| entry.blueprint_extension_id == member.id)
            {
                store
                    .create_desired_blueprint(&NewDesiredBlueprint {
                        desired_id: uuid::Uuid::new_v4(),
                        blueprint_extension_id: member.id.clone(),
                        blueprint_version: member.version.clone(),
                        params_json: None,
                    })
                    .await?;
                summary.setups_offered += 1;
            }
        }
    }
    for orphan in extensions.iter().filter(|row| {
        row.enabled && is_stack_member_id(&row.extension_id) && !wanted.contains(&row.extension_id)
    }) {
        store
            .set_extension_enabled(&orphan.extension_id, false)
            .await?;
        summary.members_disabled += 1;
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const IMAGE: &str = "lscr.io/linuxserver/sonarr@sha256:f83bc8426c8260e1901094b43fb9796bd47b3ebd874e4c53a3711ca4df791dcc";

    fn stack() -> ManifestStack {
        serde_json::from_value(json!({
            "modules": [{"id": "sonarr", "version": "1.0.0", "kind": "module", "name": "Sonarr",
                "permissions": ["runtime.manage_containers", "network.egress"],
                "provides": [{"capability": "media.manager.tv", "slot": "default", "cardinality": "one", "implementation": "sonarr"}],
                "runtime": {"type": "container", "image": IMAGE, "ports": [{"container": 8989}],
                    "volumes": ["{downloads}:/downloads", "{media}/tv:/tv"]},
                "networking": {"service_port": {"scheme": "http", "container_port": 8989}}}],
            "connectors": [{"id": "sonarr-qbittorrent", "version": "1.0.0", "kind": "connector", "name": "Sonarr to qBittorrent",
                "permissions": ["drivers.configure.media.manager.tv"],
                "targets": [{"capability": "media.manager.tv", "slot": "default"}],
                "actions": [{"type": "driver_patch", "target": {"capability": "media.manager.tv", "slot": "default"},
                    "patch": {"op": "set_downloaders", "downloaders": []}}]}],
            "blueprint": {"id": "setup", "version": "1.0.0", "kind": "blueprint", "name": "Setup",
                "execution": {"packages": ["sonarr", "elixir.modules.qbittorrent", "sonarr-qbittorrent"],
                    "instances": [{"id": "sonarr", "extension_id": "sonarr"},
                        {"id": "qbittorrent", "extension_id": "elixir.modules.qbittorrent"}],
                    "ownership": [{"domain": "sonarr.download_clients.qbittorrent", "owner": "sonarr-qbittorrent"}],
                    "phases": [{"id": "install", "barrier": true, "steps": [
                        {"type": "ensure_package_installed", "extension_id": "sonarr"},
                        {"type": "ensure_package_installed", "extension_id": "elixir.modules.qbittorrent"},
                        {"type": "ensure_package_installed", "extension_id": "sonarr-qbittorrent"}]},
                        {"id": "wire", "barrier": true, "steps": [
                        {"type": "apply_connector", "connector_id": "sonarr-qbittorrent", "target_instance": "sonarr",
                            "target_capability": "media.manager.tv", "ownership_domains": ["sonarr.download_clients.qbittorrent"]}]}]}}
        }))
        .unwrap()
    }
    fn permissions() -> Vec<String> {
        [
            "runtime.manage_containers",
            "network.egress",
            "drivers.configure.media.manager.tv",
        ]
        .map(String::from)
        .to_vec()
    }

    #[test]
    fn a_valid_stack_materializes_with_derived_ids_and_built_ins_kept() -> Result<()> {
        let stack = stack();
        check_members(&stack, &permissions())?;
        let members = materialize("elx:publisher/arr-stack", "2.0.0", &stack)?;
        let sonarr = derived_id("elx:publisher/arr-stack", "sonarr");
        assert!(sonarr.starts_with("stack.") && sonarr.ends_with(".sonarr") && sonarr.len() == 21);
        assert_eq!(members.len(), 3);
        assert!(
            members
                .iter()
                .all(|member| is_stack_member_id(&member.id) && member.version == "2.0.0")
        );
        let setup = serde_json::to_value(&members[2])?;
        let execution = &setup["execution"];
        assert_eq!(
            execution["packages"],
            json!([
                sonarr,
                "elixir.modules.qbittorrent",
                derived_id("elx:publisher/arr-stack", "sonarr-qbittorrent")
            ])
        );
        assert_eq!(
            execution["instances"][1]["extension_id"],
            "elixir.modules.qbittorrent"
        );
        assert_eq!(
            execution["phases"][1]["steps"][0]["connector_id"],
            derived_id("elx:publisher/arr-stack", "sonarr-qbittorrent")
        );
        Ok(())
    }

    // The shipped Arr Stack manifest, when this server is checked out inside
    // the Elixir repository (skipped for a standalone server checkout).
    #[test]
    fn the_arr_stack_manifest_passes_every_stack_check() -> Result<()> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../extensions/marketplace-sources/arr-stack/manifest.yaml"
        );
        let Ok(text) = std::fs::read_to_string(path) else {
            return Ok(());
        };
        let raw: serde_json::Value = serde_yaml::from_str(&text)?;
        elixir_marketplace::upstream_image::check_stack(&raw)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let manifest: ExtensionManifest = serde_json::from_value(raw)?;
        manifest.validate()?;
        let stack = manifest.stack.as_ref().context("stack")?;
        check_members(stack, &manifest.permissions)?;
        let members = materialize("elx:publisher/arr-stack", &manifest.version, stack)?;
        assert_eq!(members.len(), 3 + 14 + 1);
        let setup = members.last().context("setup")?;
        let execution = setup.execution.as_ref().context("execution")?;
        assert_eq!(execution.packages.len(), 18);
        assert!(
            execution.packages.iter().all(
                |id| is_stack_member_id(id) || BUILT_IN_STACK_REFERENCES.contains(&id.as_str())
            )
        );
        assert!(serde_json::to_vec(&manifest)?.len() < 64 * 1024);
        Ok(())
    }

    #[test]
    fn members_cannot_take_runtime_controls_or_reach_outside_the_stack() {
        let mutations: Vec<(&str, Box<dyn Fn(&mut ManifestStack)>)> = vec![
            (
                "tag image",
                Box::new(|s| {
                    s.modules[0].runtime.as_mut().unwrap().image =
                        Some("lscr.io/linuxserver/sonarr:4.0.0".into())
                }),
            ),
            (
                "network",
                Box::new(|s| s.modules[0].runtime.as_mut().unwrap().network = Some("host".into())),
            ),
            (
                "service name",
                Box::new(|s| {
                    s.modules[0].runtime.as_mut().unwrap().service_name = Some("elx-sonarr".into())
                }),
            ),
            (
                "host port",
                Box::new(|s| s.modules[0].runtime.as_mut().unwrap().ports[0].host = Some(8989)),
            ),
            (
                "socket",
                Box::new(|s| {
                    s.modules[0]
                        .runtime
                        .as_mut()
                        .unwrap()
                        .volumes
                        .push("/var/run/docker.sock:/var/run/docker.sock".into())
                }),
            ),
            (
                "security",
                Box::new(|s| {
                    s.modules[0]
                        .runtime
                        .as_mut()
                        .unwrap()
                        .security
                        .no_new_privileges = true
                }),
            ),
            (
                "secret env",
                Box::new(|s| {
                    s.modules[0].runtime.as_mut().unwrap().env.push(
                        serde_json::from_value(json!({"name": "X", "from_secret": "instance:x"}))
                            .unwrap(),
                    )
                }),
            ),
            (
                "capability",
                Box::new(|s| s.modules[0].provides[0].capability = "downloader.torrent".into()),
            ),
            (
                "permission",
                Box::new(|s| s.modules[0].permissions.push("live.network.private".into())),
            ),
            (
                "name",
                Box::new(|s| s.modules[0].id = "elixir.modules.sonarr".into()),
            ),
            (
                "outside reference",
                Box::new(|s| {
                    s.blueprint
                        .execution
                        .as_mut()
                        .unwrap()
                        .packages
                        .push("elixir.modules.bazarr".into())
                }),
            ),
            (
                "connector action",
                Box::new(|s| s.connectors[0].actions[0].r#type = "shell".into()),
            ),
            (
                "nested stack",
                Box::new(|s| s.blueprint.stack = Some(stack())),
            ),
        ];
        for (label, mutate) in mutations {
            let mut stack = stack();
            mutate(&mut stack);
            assert!(check_members(&stack, &permissions()).is_err(), "{label}");
        }
    }
}
