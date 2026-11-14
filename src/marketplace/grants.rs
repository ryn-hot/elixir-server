//! Owner-reviewed public extension access. Reviews and deltas are data, not
//! execution capabilities. Installed state, source/build/runtime verification,
//! current authority and update-policy checks remain independent requirements.
use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    canonical,
    identity::{Digest, ExtensionId},
    records::RuntimeProfile,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::manifest::ManifestCheckedQuarantine;
use crate::extensions::{manifest::ExtensionManifest, permissions::public_permission_name};

const POLICY_VERSION: u32 = 1;
const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporaryMount {
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeResources {
    pub memory_bytes: u64,
    pub cpu_millicores: u32,
    pub pids: u32,
    pub temporary_mounts: Vec<TemporaryMount>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub state_bytes: u64,
}
impl RuntimeResources {
    fn validate(&self) -> Result<()> {
        if !(16 * MIB..=4096 * MIB).contains(&self.memory_bytes)
            || self.memory_bytes % MIB != 0
            || !(1..=8000).contains(&self.cpu_millicores)
            || !(1..=512).contains(&self.pids)
            || self.temporary_mounts.is_empty()
            || self.temporary_mounts.len() > 3
            || self.state_bytes > 16 * MIB
            || !self.state_bytes.is_multiple_of(MIB)
        {
            bail!("invalid public runtime resource bounds");
        }
        let mut paths = BTreeSet::new();
        let mut bytes = 0u64;
        for mount in &self.temporary_mounts {
            if !matches!(mount.path.as_str(), "/tmp" | "/run" | "/dev/shm")
                || !paths.insert(&mount.path)
                || mount.bytes == 0
                || mount.bytes % MIB != 0
            {
                bail!("invalid public temporary mount");
            }
            bytes = bytes
                .checked_add(mount.bytes)
                .context("temporary storage overflow")?;
        }
        if bytes > 256 * MIB || bytes > self.memory_bytes / 2 {
            bail!("temporary mounts exceed the public memory budget");
        }
        Ok(())
    }
    fn covers(&self, other: &Self) -> bool {
        other.memory_bytes <= self.memory_bytes
            && other.state_bytes <= self.state_bytes
            && other.cpu_millicores <= self.cpu_millicores
            && other.pids <= self.pids
            && other.temporary_mounts.iter().all(|new| {
                self.temporary_mounts
                    .iter()
                    .any(|old| old.path == new.path && old.bytes >= new.bytes)
            })
    }
}
fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// Closed durable representation. Deserialization alone grants nothing; the
/// repository must authenticate its binding and current approval revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantScope {
    policy_version: u32,
    extension_id: ExtensionId,
    runtime_profile: RuntimeProfile,
    permissions: Vec<String>,
    secret_keys: Vec<String>,
    runtime: Option<RuntimeResources>,
    authority_contract: Digest,
}
impl GrantScope {
    pub fn permissions(&self) -> &[String] {
        &self.permissions
    }
    pub fn secret_keys(&self) -> &[String] {
        &self.secret_keys
    }
    pub fn runtime(&self) -> Option<&RuntimeResources> {
        self.runtime.as_ref()
    }
    pub fn extension_id(&self) -> &ExtensionId {
        &self.extension_id
    }
    pub(super) fn validate(&self) -> Result<()> {
        if self.policy_version != POLICY_VERSION
            || self.permissions.len() > 32
            || self.secret_keys.len() > 64
            || !sorted_unique(&self.permissions)
            || !sorted_unique(&self.secret_keys)
            || self.permissions.iter().any(|p| !public_permission_name(p))
            || self.secret_keys.iter().any(|key| !valid_key(key))
        {
            bail!("invalid public grant scope");
        }
        match (self.runtime_profile, &self.runtime) {
            (RuntimeProfile::Declarative, None) if self.secret_keys.is_empty() => {}
            (RuntimeProfile::SourceJs | RuntimeProfile::Reproducible, Some(resources)) => {
                resources.validate()?
            }

            _ => bail!("public grant runtime profile mismatch"),
        }
        Ok(())
    }
    pub fn delta_from(&self, approved: &Self) -> Result<GrantDelta> {
        self.validate()?;
        approved.validate()?;
        Ok(GrantDelta {
            identity_or_policy_changed: self.policy_version != approved.policy_version
                || self.extension_id != approved.extension_id,
            runtime_boundary_changed: self.runtime_profile != approved.runtime_profile,
            authority_contract_changed: self.authority_contract != approved.authority_contract,
            added_permissions: self
                .permissions
                .iter()
                .filter(|p| !approved.permissions.contains(p))
                .cloned()
                .collect(),
            added_secret_keys: self
                .secret_keys
                .iter()
                .filter(|key| !approved.secret_keys.contains(key))
                .cloned()
                .collect(),
            resource_increase: match (&approved.runtime, &self.runtime) {
                (Some(old), Some(new)) => !old.covers(new),
                (None, None) => false,
                _ => true,
            },
        })
    }
}

#[derive(Debug, Serialize)]
pub struct GrantDelta {
    pub identity_or_policy_changed: bool,
    pub runtime_boundary_changed: bool,
    pub authority_contract_changed: bool,
    pub added_permissions: Vec<String>,
    pub added_secret_keys: Vec<String>,
    pub resource_increase: bool,
}
impl GrantDelta {
    /// Necessary for automatic update eligibility, never sufficient: it proves
    /// no publisher/head/adoption/source/build/migration/update-policy checks.
    pub fn requires_approval(&self) -> bool {
        self.identity_or_policy_changed
            || self.runtime_boundary_changed
            || self.authority_contract_changed
            || !self.added_permissions.is_empty()
            || !self.added_secret_keys.is_empty()
            || self.resource_increase
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantReview {
    release_id: Digest,
    manifest_sha256: Digest,
    scope: GrantScope,
    /// The containers a stack declares, shown to the owner. Absent for every
    /// extension without a stack, so their review IDs are unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stack: Option<Vec<StackContainerReview>>,
}

/// One stack container as the owner approves it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StackContainerReview {
    pub member: String,
    pub name: String,
    pub image: String,
    pub folders: Vec<String>,
    pub web_port: Option<u16>,
}
impl GrantReview {
    pub fn review_id(&self) -> Result<Digest> {
        self.validate()?;
        Ok(Digest::of(canonical::to_vec(self)?))
    }
    pub fn release_id(&self) -> Digest {
        self.release_id
    }
    pub fn scope(&self) -> &GrantScope {
        &self.scope
    }
    pub fn stack(&self) -> Option<&[StackContainerReview]> {
        self.stack.as_deref()
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if self.stack.is_some() && self.scope.runtime_profile != RuntimeProfile::SourceJs {
            bail!("only source modules declare a container stack");
        }
        Ok(())
    }
}
impl ManifestCheckedQuarantine {
    /// A review is recomputed from verified bytes, never accepted from a peer
    /// or from owner-supplied permission lists. Approval selects its exact hash.
    pub fn grant_review(&self) -> Result<GrantReview> {
        self.source().ensure_active()?;
        let source = self.source();
        let release = source.release();
        build_review(
            self.manifest(),
            release.runtime_profile,
            source.summary().release_id,
            release.manifest_sha256,
            &release.artifacts,
        )
    }
}

pub(super) fn build_review(
    manifest: &ExtensionManifest,
    profile: RuntimeProfile,
    release_id: Digest,
    manifest_sha256: Digest,
    artifacts: &[elixir_marketplace::records::CompiledArtifact],
) -> Result<GrantReview> {
    if manifest.control_surface.as_ref().is_some_and(|control| {
        control
            .owned_settings
            .iter()
            .any(|field| field.lan_endpoint.is_some())
    }) && profile != RuntimeProfile::SourceJs
    {
        bail!("configured local SDK endpoints require the source-js runtime");
    }
    for provide in &manifest.provides {
        let source = provide
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.r#type == "source-js-v1");
        if provide.capability == crate::extensions::manifest::LIVE_CATALOG_PROVIDER_CAPABILITY
            && source != (profile == RuntimeProfile::SourceJs)
        {
            bail!("live provider transport must match its verified runtime profile");
        }
        // A source-js candidate provider is served only by the held controller;
        // it has no HTTP endpoint, healthcheck or container networking.
        if provide.capability
            == crate::extensions::manifest::ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY
            && profile == RuntimeProfile::SourceJs
        {
            let endpoint = provide.endpoint.as_ref();
            if !source
                || provide.healthcheck.is_some()
                || manifest.networking.is_some()
                || endpoint.is_some_and(|e| {
                    e.base_url.is_some()
                        || e.scheme.is_some()
                        || e.host.is_some()
                        || e.port.is_some()
                        || e.base_path.is_some()
                })
            {
                bail!("source-js candidate providers require only the source-js-v1 transport");
            }
        }
    }
    let mut permissions = manifest.permissions.clone();
    permissions.sort();
    let mut secrets = BTreeSet::new();
    if let Some(control) = &manifest.control_surface {
        for field in &control.owned_settings {
            match field.storage.r#type.as_str() {
                "instance_secret" if field.secret && valid_key(&field.storage.key) => {
                    secrets.insert(field.storage.key.clone());
                }
                "extension_setting" | "instance_setting"
                    if !field.secret && valid_key(&field.storage.key) => {}
                _ => {
                    bail!("public control settings must use their own instance secrets or settings")
                }
            }
        }
    }
    let runtime = manifest.runtime.as_ref().map(|runtime| {
        if runtime.egress.is_some() || runtime.service_name.is_some() || runtime.image.is_some()
            || runtime.network.is_some() || !runtime.volumes.is_empty() || runtime.ports.iter().any(|p| p.host.is_some())
            || runtime.security.seccomp_profile.is_some() || runtime.security.apparmor_profile.is_some()
            || runtime.security.user.as_deref().is_some_and(|u| u != "10004:10004") {
            bail!("public runtimes cannot select protection, host access, identity or security profiles");
        }
        let mut env_names = BTreeSet::new();
        if runtime.env.len() > 64 { bail!("too many public environment bindings"); }
        for env in &runtime.env {
            if !valid_env_name(&env.name) || !env_names.insert(&env.name) { bail!("invalid or repeated public environment name"); }
            if let Some(reference) = &env.from_secret {
                let key = reference.strip_prefix("instance:").filter(|key| valid_key(key))
                    .context("public runtime secrets must belong to the same instance")?;
                secrets.insert(key.to_owned());
            }
            if env.value.as_ref().is_some_and(|v| v.len()>4096 || v.contains('\0')) { bail!("invalid public environment value"); }
        }
        let mut temporary_mounts = if runtime.security.tmpfs.is_empty() {
            vec![TemporaryMount {path:"/tmp".into(),bytes:8*MIB}]
        } else {
            runtime.security.tmpfs.iter().map(|m| Ok(TemporaryMount {path:m.path.clone(),bytes:m.size_mb.context("public temporary mounts require a quota")?.checked_mul(MIB).context("temporary quota overflow")?})).collect::<Result<Vec<_>>>()?
        };
        temporary_mounts.sort_by(|a,b| a.path.cmp(&b.path));
        let resources = RuntimeResources {
            memory_bytes: runtime.security.memory_limit_mb.unwrap_or(128).checked_mul(MIB).context("memory quota overflow")?,
            cpu_millicores: runtime.security.cpu_quota.as_deref().map(cpu_millicores).transpose()?.unwrap_or(100),
            pids: runtime.security.pids_limit.unwrap_or(64).try_into().context("PID quota overflow")?,
            temporary_mounts,
            state_bytes:runtime.security.sdk_state_limit_bytes.unwrap_or(0),
        };
        resources.validate()?; Ok(resources)
    }).transpose()?;
    // Declarative actions/dependencies/control bindings are authority-bearing.
    // Changes require review, even if their semantic subset is not yet known.
    // Presentation and source/artifact content hashes are not access grants.
    let build_boundaries: Vec<_> = artifacts.iter().map(|a| json!({"architecture":a.architecture,
        "builder_image_digest":a.builder_image_digest,"command":a.command,"environment":a.environment})).collect();
    let mut contract = json!({
        "provides":manifest.provides,"requires":manifest.requires,"conflicts":manifest.conflicts,
        "kind":manifest.kind,
        "targets":manifest.targets,"actions":manifest.actions,"execution":manifest.execution,
        "connectors":manifest.connectors,"optional_addons":manifest.optional_addons,"wants":manifest.wants,
        "preferences":manifest.preferences,"bindings":manifest.bindings,"policies":manifest.policies,
        "networking":manifest.networking,"control_surface":manifest.control_surface,"owner_release":manifest.owner_release,
        "build_boundaries":build_boundaries
    });
    // Images, folders, connectors and setup steps are all authority-bearing.
    // The key is added only for stacks, so other contracts keep their digest.
    let stack = match &manifest.stack {
        Some(stack) if profile == RuntimeProfile::SourceJs => {
            contract["stack"] = serde_json::to_value(stack)?;
            Some(stack_review(stack))
        }
        Some(_) => bail!("only source modules declare a container stack"),
        None => None,
    };
    let authority_contract = Digest::of(canonical::to_vec(&contract)?);
    let scope = GrantScope {
        policy_version: POLICY_VERSION,
        extension_id: manifest.id.parse()?,
        runtime_profile: profile,
        permissions,
        secret_keys: secrets.into_iter().collect(),
        runtime,
        authority_contract,
    };
    let review = GrantReview {
        release_id,
        manifest_sha256,
        scope,
        stack,
    };
    review.validate()?;
    Ok(review)
}

fn stack_review(stack: &crate::extensions::manifest::ManifestStack) -> Vec<StackContainerReview> {
    stack
        .modules
        .iter()
        .map(|module| {
            let runtime = module.runtime.as_ref();
            StackContainerReview {
                member: module.id.clone(),
                name: module.name.clone(),
                image: runtime.and_then(|r| r.image.clone()).unwrap_or_default(),
                folders: runtime.map(|r| r.volumes.clone()).unwrap_or_default(),
                web_port: module
                    .networking
                    .as_ref()
                    .map(|networking| networking.service_port.container_port),
            }
        })
        .collect()
}

fn sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}
fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
}
pub(super) fn valid_env_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.as_bytes()[0].is_ascii_digit()
        && name
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
        && !name.starts_with("LD_")
        && !name.starts_with("DYLD_")
        && !matches!(
            name,
            "NODE_OPTIONS"
                | "NODE_PATH"
                | "PATH"
                | "HOME"
                | "BASH_ENV"
                | "ENV"
                | "PYTHONPATH"
                | "PYTHONHOME"
                | "PYTHONSTARTUP"
                | "PYTHONINSPECT"
                | "RUBYOPT"
                | "PERL5OPT"
                | "JAVA_TOOL_OPTIONS"
                | "JDK_JAVA_OPTIONS"
                | "CLASSPATH"
        )
}
fn cpu_millicores(text: &str) -> Result<u32> {
    if text.len() > 32 {
        bail!("invalid public CPU quota");
    }
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let fraction = fraction.trim_end_matches('0');
    if whole.is_empty()
        || !whole.bytes().all(|c| c.is_ascii_digit())
        || fraction.len() > 3
        || !fraction.bytes().all(|c| c.is_ascii_digit())
    {
        bail!("public CPU quota must be an exact positive decimal in millicores");
    }
    let whole: u32 = whole.parse()?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u32>()? * 10u32.pow(3 - fraction.len() as u32)
    };
    let value = whole
        .checked_mul(1000)
        .and_then(|v| v.checked_add(fraction))
        .context("CPU quota overflow")?;
    if value == 0 || value > 8000 {
        bail!("public CPU quota outside bounds");
    }
    Ok(value)
}

#[cfg(test)]
pub(super) fn fixture_review() -> Result<GrantReview> {
    let (_, review) = tests::fixture()?;
    Ok(review)
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn fixture() -> Result<(ExtensionManifest, GrantReview)> {
        let golden: serde_json::Value = serde_json::from_str(include_str!(
            "../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))?;
        let manifest: ExtensionManifest = serde_json::from_value(
            json!({"id":golden["extension_id"],"version":"1.0.0","kind":"module","name":"Grant Fixture",
            "permissions":["runtime.manage_containers","network.egress"],
            "provides":[{"capability":"acquisition.candidate_provider","slot":"default","cardinality":"many","implementation":"fixture","scope":{"media_types":["movies"],"actions":["search"]},"endpoint":{"type":"source-js-v1"}}],
            "runtime":{"type":"container","env":[{"name":"API_KEY","from_secret":"instance:source_api","optional":true}]}}),
        )?;
        manifest.validate()?;
        let review = build_review(
            &manifest,
            RuntimeProfile::SourceJs,
            Digest::of(b"first release"),
            Digest::of(b"first manifest"),
            &[],
        )?;
        Ok((manifest, review))
    }
    fn review(manifest: &ExtensionManifest) -> Result<GrantReview> {
        build_review(
            manifest,
            RuntimeProfile::SourceJs,
            Digest::of(b"next release"),
            Digest::of(b"next manifest"),
            &[],
        )
    }
    #[test]
    fn configured_lan_endpoint_changes_require_review_and_only_source_js_can_use_them() -> Result<()>
    {
        let (mut manifest, _) = fixture()?;
        manifest.permissions.push("live.network.private".into());
        manifest.control_surface = Some(serde_json::from_value(json!({"owned_settings":[
            {"id":"playlist","label":"Playlist URL","type":"password","secret":true,
             "ownership":"managed","storage":{"type":"instance_secret","key":"playlist_url"},
             "lan_endpoint":{"enabled_by":"localNetwork","access":"exact_url"}},
            {"id":"localNetwork","label":"Allow this configured LAN endpoint","type":"toggle",
             "default":false,"storage":{"type":"instance_setting","key":"localNetwork"}}
        ]}))?);
        manifest.validate()?;
        let exact = review(&manifest)?;
        let contract = &mut manifest.control_surface.as_mut().unwrap().owned_settings[0];
        contract.lan_endpoint.as_mut().unwrap().access =
            crate::extensions::manifest::ManifestLanEndpointAccess::Origin;
        manifest.validate()?;
        let origin = review(&manifest)?;
        let delta = origin.scope().delta_from(exact.scope())?;
        assert!(delta.authority_contract_changed && delta.requires_approval());
        assert!(
            delta.added_permissions.is_empty(),
            "same permission name must not hide a broader endpoint contract"
        );
        for profile in [RuntimeProfile::Declarative, RuntimeProfile::Reproducible] {
            assert!(
                build_review(&manifest, profile, Digest::of(b"r"), Digest::of(b"m"), &[]).is_err()
            );
        }
        Ok(())
    }
    #[test]
    fn live_transport_must_match_the_verified_profile_not_manifest_claims() -> Result<()> {
        let (mut manifest, _) = fixture()?;
        manifest.permissions = vec![
            "live.catalog.read".into(),
            "live.stream.resolve".into(),
            "live.artwork.fetch".into(),
        ];
        manifest.provides = serde_json::from_value(
            json!([{"capability":"live.catalog_provider","slot":"default","cardinality":"many","implementation":"source_fixture","contract_version":1,
            "scope":{"actions":["catalog","meta","resolve"],"live_item_types":["event"],"stream_protocols":["hls"]},"endpoint":{"type":"source-js-v1"}}]),
        )?;
        let security = &mut manifest.runtime.as_mut().unwrap().security;
        security.run_as_non_root = true;
        security.read_only_rootfs = true;
        security.no_new_privileges = true;
        security.drop_capabilities = vec!["ALL".into()];
        security.prohibit_docker_socket = true;
        security.prohibit_host_media_mounts = true;
        manifest.validate()?;
        review(&manifest)?;
        assert!(
            build_review(
                &manifest,
                RuntimeProfile::Reproducible,
                Digest::of(b"r"),
                Digest::of(b"m"),
                &[]
            )
            .is_err()
        );
        manifest.provides[0].endpoint = Some(serde_json::from_value(
            json!({"type":"http","scheme":"http","port":8110,"base_path":"/"}),
        )?);
        manifest.provides[0].healthcheck = Some(serde_json::from_value(
            json!({"type":"http","path":"/health"}),
        )?);
        manifest.validate()?;
        assert!(
            review(&manifest).is_err(),
            "source runtime cannot fall back to an HTTP declaration"
        );
        Ok(())
    }
    #[test]
    fn reviews_bind_exact_selection_but_code_and_presentation_do_not_expand_access() -> Result<()> {
        let (mut manifest, old) = fixture()?;
        manifest.version = "1.1.0".into();
        manifest.name = "New label".into();
        manifest.description = Some("New description".into());
        let new = review(&manifest)?;
        assert_ne!(new.review_id()?, old.review_id()?);
        assert!(!new.scope.delta_from(&old.scope)?.requires_approval());
        assert_eq!(new.scope.secret_keys(), ["source_api"]);
        let resources = new.scope.runtime().unwrap();
        assert_eq!(resources.memory_bytes, 128 * MIB);
        assert_eq!(resources.cpu_millicores, 100);
        assert_eq!(resources.pids, 64);
        assert_eq!(resources.state_bytes, 0);
        Ok(())
    }
    #[test]
    fn persistent_sdk_state_is_explicit_bounded_and_requires_approval_to_increase() -> Result<()> {
        let (mut manifest, old) = fixture()?;
        let before = serde_json::to_value(&old)?;
        assert!(before["scope"]["runtime"].get("state_bytes").is_none());
        let round: GrantReview = serde_json::from_value(before)?;
        assert_eq!(round.review_id()?, old.review_id()?);
        manifest
            .runtime
            .as_mut()
            .unwrap()
            .security
            .sdk_state_limit_bytes = Some(MIB);
        let new = review(&manifest)?;
        assert!(new.scope.delta_from(&old.scope)?.requires_approval());
        assert_eq!(new.scope.runtime().unwrap().state_bytes, MIB);
        assert!(!old.scope.delta_from(&new.scope)?.requires_approval());
        for bytes in [1, MIB - 1, 16 * MIB + 1, u64::MAX] {
            manifest
                .runtime
                .as_mut()
                .unwrap()
                .security
                .sdk_state_limit_bytes = Some(bytes);
            assert!(manifest.validate().is_err());
            assert!(review(&manifest).is_err());
        }
        Ok(())
    }
    #[test]
    fn added_permissions_secrets_capabilities_builder_boundaries_and_resources_require_review()
    -> Result<()> {
        let (manifest, old) = fixture()?;
        let mut added = manifest.clone();
        added.permissions.push("live.artwork.fetch".into());
        assert!(
            review(&added)?
                .scope
                .delta_from(&old.scope)?
                .requires_approval()
        );
        let mut added = manifest.clone();
        added.runtime.as_mut().unwrap().env[0].from_secret = Some("instance:another_api".into());
        assert_eq!(
            review(&added)?
                .scope
                .delta_from(&old.scope)?
                .added_secret_keys,
            ["another_api"]
        );
        let mut added = manifest.clone();
        added.provides[0]
            .scope
            .as_mut()
            .unwrap()
            .media_types
            .push("anime".into());
        assert!(
            review(&added)?
                .scope
                .delta_from(&old.scope)?
                .authority_contract_changed
        );
        let mut added = manifest.clone();
        added.runtime.as_mut().unwrap().security.memory_limit_mb = Some(256);
        assert!(
            review(&added)?
                .scope
                .delta_from(&old.scope)?
                .resource_increase
        );
        let mut added = manifest.clone();
        added.runtime.as_mut().unwrap().security.cpu_quota = Some("0.2".into());
        assert!(
            review(&added)?
                .scope
                .delta_from(&old.scope)?
                .resource_increase
        );
        let mut reduced = manifest;
        reduced.permissions.retain(|p| p != "network.egress");
        reduced.runtime.as_mut().unwrap().env.clear();
        reduced.runtime.as_mut().unwrap().security.memory_limit_mb = Some(64);
        assert!(
            !review(&reduced)?
                .scope
                .delta_from(&old.scope)?
                .requires_approval()
        );
        Ok(())
    }
    #[test]
    fn global_and_other_provider_secrets_loader_hooks_network_override_and_unbounded_state_are_forbidden()
    -> Result<()> {
        let (manifest, _) = fixture()?;
        for reference in [
            "global:download_api",
            "provider:00000000-0000-0000-0000-000000000001:api",
            "instance:bad:key",
            "instance:",
            "instance:bad key",
        ] {
            let mut bad = manifest.clone();
            bad.runtime.as_mut().unwrap().env[0].from_secret = Some(reference.into());
            assert!(review(&bad).is_err(), "{reference}");
        }
        for name in [
            "NODE_OPTIONS",
            "LD_AUDIT",
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
            "PATH",
            "PYTHONSTARTUP",
            "JAVA_TOOL_OPTIONS",
            "1NAME",
            "name",
        ] {
            let mut bad = manifest.clone();
            bad.runtime.as_mut().unwrap().env[0].name = name.into();
            assert!(review(&bad).is_err(), "{name}");
        }
        let mut bad = manifest.clone();
        let duplicate = bad.runtime.as_ref().unwrap().env[0].clone();
        bad.runtime.as_mut().unwrap().env.push(duplicate);
        assert!(review(&bad).is_err());
        let mut bad = manifest.clone();
        bad.runtime.as_mut().unwrap().egress = Some(serde_json::from_value(
            json!({"mode":"direct","strict":false}),
        )?);
        assert!(review(&bad).is_err());
        let mut bad = manifest.clone();
        bad.runtime.as_mut().unwrap().security.tmpfs = vec![serde_json::from_value(
            json!({"path":"/var/run/docker.sock","size_mb":1}),
        )?];
        assert!(review(&bad).is_err());
        let mut bad = manifest;
        bad.runtime.as_mut().unwrap().security.tmpfs =
            vec![serde_json::from_value(json!({"path":"/tmp"}))?];
        assert!(review(&bad).is_err());
        Ok(())
    }
    #[test]
    fn instance_control_secrets_are_reviewed_but_global_control_secrets_never_are() -> Result<()> {
        let (mut manifest, _) = fixture()?;
        manifest.control_surface = Some(serde_json::from_value(
            json!({"owned_settings":[{"id":"sourceLogin","label":"Source login","type":"password","secret":true,"storage":{"type":"instance_secret","key":"source_login"}}]}),
        )?);
        assert_eq!(
            review(&manifest)?.scope.secret_keys(),
            ["source_api", "source_login"]
        );
        manifest.control_surface.as_mut().unwrap().owned_settings[0]
            .storage
            .r#type = "global_secret".into();
        assert!(review(&manifest).is_err());
        Ok(())
    }
    #[test]
    fn exact_positive_cpu_and_memory_mount_budgets_do_not_round_or_disable_limits() -> Result<()> {
        assert_eq!(cpu_millicores("0.1000")?, 100);
        assert_eq!(cpu_millicores("1.125")?, 1125);
        for value in [
            "0",
            "-1",
            "NaN",
            "1e0",
            "0.0001",
            " 1",
            "4294967296",
            "8.001",
            "+1",
        ] {
            assert!(cpu_millicores(value).is_err(), "{value}");
        }
        let (mut manifest, _) = fixture()?;
        for memory in [0, 15, 4097, u64::MAX] {
            manifest.runtime.as_mut().unwrap().security.memory_limit_mb = Some(memory);
            assert!(review(&manifest).is_err());
        }
        manifest.runtime.as_mut().unwrap().security.memory_limit_mb = Some(128);
        manifest.runtime.as_mut().unwrap().security.tmpfs =
            vec![serde_json::from_value(json!({"path":"/tmp","size_mb":65}))?];
        assert!(review(&manifest).is_err());
        Ok(())
    }
    #[test]
    fn untrusted_persisted_scopes_must_still_have_closed_versioned_and_deduplicated_shapes()
    -> Result<()> {
        let (_, value) = fixture()?;
        let mut raw = serde_json::to_value(&value)?;
        raw["scope"]["policy_version"] = json!(2);
        assert!(
            serde_json::from_value::<GrantReview>(raw)?
                .review_id()
                .is_err()
        );
        let mut raw = serde_json::to_value(&value)?;
        raw["scope"]["secret_keys"] = json!(["source_api", "source_api"]);
        assert!(
            serde_json::from_value::<GrantReview>(raw)?
                .review_id()
                .is_err()
        );
        let mut raw = serde_json::to_value(value)?;
        raw["scope"]["docker_socket"] = json!(true);
        assert!(serde_json::from_value::<GrantReview>(raw).is_err());
        Ok(())
    }

    #[test]
    fn builder_and_kind_changes_cannot_hide_behind_unchanged_permission_names() -> Result<()> {
        use elixir_marketplace::records::{Blob, CompiledArtifact};
        let (mut manifest, _) = fixture()?;
        let mut artifact = CompiledArtifact {
            blob: Blob {
                sha256: Digest::of(b"binary one"),
                size: 10,
            },
            architecture: "linux/amd64".into(),
            builder_image_digest: Digest::of(b"reviewed builder"),
            command: vec!["build".into()],
            environment: Default::default(),
            inputs: vec![],
        };
        let make = |manifest: &ExtensionManifest, artifact: &CompiledArtifact| {
            build_review(
                manifest,
                RuntimeProfile::Reproducible,
                Digest::of(b"release"),
                Digest::of(b"manifest"),
                std::slice::from_ref(artifact),
            )
        };
        let old = make(&manifest, &artifact)?;
        artifact.blob.sha256 = Digest::of(b"binary two");
        assert!(
            !make(&manifest, &artifact)?
                .scope
                .delta_from(&old.scope)?
                .requires_approval(),
            "code equality is a separate gate"
        );
        artifact.builder_image_digest = Digest::of(b"different builder");
        assert!(
            make(&manifest, &artifact)?
                .scope
                .delta_from(&old.scope)?
                .authority_contract_changed
        );
        manifest.kind = crate::db::models::ExtensionKind::Connector;
        assert!(
            make(&manifest, &artifact)?
                .scope
                .delta_from(&old.scope)?
                .authority_contract_changed
        );
        Ok(())
    }
}
