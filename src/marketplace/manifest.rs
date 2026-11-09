//! Public manifest checks, separate from the local registry's trust policy.
//! This is not a grant, runtime isolation proof, dependency/build closure check,
//! public repository preflight, or installation/activation capability.
use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    manifest::ManifestDocument,
    records::{Blob, ExtensionKind, Release, RuntimeProfile, safe_source_path},
    reproduction::require_host_platform_selection,
    source_job::{SourceEntrypoint, SourceMatchSummary},
};
use serde::de::IntoDeserializer;
use serde_json::Value;

use super::quarantine::SourceMatchedQuarantine;
use crate::{db::models::ExtensionTrustLevel, extensions::manifest::ExtensionManifest};

/// Requested execution selection only; never a runtime command or permission
/// grant. The platform chooses pinned runners and enforces their full sandbox.
#[derive(Debug)]
pub enum PublicRuntimeSelection {
    Declarative,
    SourceJs { entrypoint: SourceEntrypoint },
    Reproducible { artifact: Blob },
}

/// Consumes the source-equality token and retains its exclusive workspace lease.
/// No deserializer/clone/constructor can promote peer-supplied manifest data.
pub struct ManifestCheckedQuarantine {
    source: SourceMatchedQuarantine,
    manifest: ExtensionManifest,
    selection: PublicRuntimeSelection,
    permissions: Vec<String>,
}
impl ManifestCheckedQuarantine {
    /// Move the ORIGINAL checked workspace back to its native owner. This
    /// drops manifest presentation/selection data, never creates new evidence.
    pub(super) fn into_source(self) -> SourceMatchedQuarantine {
        self.source
    }
    /// Refresh availability only; package, manifest, job deadline and requested
    /// permissions do not change. The observation must bind this same source.
    pub fn refresh_public_source(
        &mut self,
        observation: super::worker::PublicSourceObservation,
    ) -> Result<()> {
        self.source.refresh_public_source(observation)
    }
    pub fn manifest(&self) -> &ExtensionManifest {
        &self.manifest
    }
    pub fn selection(&self) -> &PublicRuntimeSelection {
        &self.selection
    }
    pub fn requested_permissions(&self) -> &[String] {
        &self.permissions
    }
    pub fn source(&self) -> &SourceMatchedQuarantine {
        &self.source
    }
    pub(super) fn source_mut(&mut self) -> &mut SourceMatchedQuarantine {
        &mut self.source
    }
    pub(super) async fn discard_retained(&mut self) -> Result<()> {
        self.source.discard_retained().await
    }
}
impl SourceMatchedQuarantine {
    pub fn check_manifest(self) -> Result<ManifestCheckedQuarantine> {
        self.require_foreground_manifest()?;
        let (manifest, selection, permissions) = check_document(
            self.summary().manifest()?,
            self.release(),
            self.summary(),
            self.selected_artifact(),
        )?;
        self.ensure_active()?;
        Ok(ManifestCheckedQuarantine {
            source: self,
            manifest,
            selection,
            permissions,
        })
    }
    pub(super) fn check_manifest_retained(
        slot: &mut Option<Self>,
    ) -> Result<ManifestCheckedQuarantine> {
        let source = slot
            .as_ref()
            .context("manifest workspace owner is missing")?;
        source.ensure_active()?;
        source.require_public_source()?;
        let (manifest, selection, permissions) = check_document(
            source.summary().manifest()?,
            source.release(),
            source.summary(),
            source.selected_artifact(),
        )?;
        source.ensure_active()?;
        Ok(ManifestCheckedQuarantine {
            source: slot.take().expect("borrowed manifest owner"),
            manifest,
            selection,
            permissions,
        })
    }
}

pub(super) fn check_document(
    document: ManifestDocument,
    release: &Release,
    source: &SourceMatchSummary,
    selected_artifact: Option<&Blob>,
) -> Result<(ExtensionManifest, PublicRuntimeSelection, Vec<String>)> {
    document.bind_release(release)?;
    // The original YAML bytes/digest remain untouched. Only the operational
    // projection drops the public profile fields already interpreted below;
    // legacy execution must not silently ignore them or repair signed bytes.
    let mut operational = document.value().clone();
    let runtime = operational
        .get_mut("runtime")
        .and_then(Value::as_object_mut);
    let mut profile = None;
    let mut entrypoint = None;
    let mut artifact = None;
    if let Some(runtime) = runtime {
        profile = runtime.remove("profile");
        entrypoint = runtime.remove("entrypoint");
        artifact = runtime.remove("artifact");
    }
    let mut unknown = false;
    let manifest: ExtensionManifest =
        serde_ignored::deserialize(operational.into_deserializer(), |_| {
            unknown = true;
        })
        .context("invalid public extension manifest schema")?;
    if unknown {
        bail!("unknown public extension manifest field");
    }
    manifest
        .validate()
        .context("invalid public extension manifest")?;
    if manifest
        .actions
        .iter()
        .any(|action| action.r#type != "driver_patch")
    {
        bail!("unsupported public connector action");
    }
    // Untagged legacy `requires` has the same buffering behavior. Accept both
    // documented forms, but not unknown keys hidden inside either form.
    if let Some(requires) = document.value().get("requires") {
        if requires.is_array() {
            check_closed_projection(
                requires,
                &serde_json::to_value(&manifest.requires.capabilities)?,
            )?;
        } else {
            let fields = requires
                .as_object()
                .context("requires mapping or sequence required")?;
            for (key, raw) in fields {
                let typed = match key.as_str() {
                    "capabilities" => serde_json::to_value(&manifest.requires.capabilities)?,
                    "downloads" => serde_json::to_value(&manifest.requires.downloads)?,
                    _ => bail!("unknown public requires field"),
                };
                check_closed_projection(raw, &typed)?;
            }
        }
    }
    // Serde's internally tagged enum buffering can hide ignored fields from
    // serde_ignored. Execution steps have a closed scalar/list schema: check
    // their raw keys explicitly without changing legacy deserialization.
    if let (Some(phases), Some(execution)) = (
        document
            .value()
            .pointer("/execution/phases")
            .and_then(Value::as_array),
        manifest.execution.as_ref(),
    ) {
        for (raw, phase) in phases.iter().zip(&execution.phases) {
            if let Some(steps) = raw.get("steps").and_then(Value::as_array) {
                for (raw, step) in steps.iter().zip(&phase.steps) {
                    check_closed_projection(raw, &serde_json::to_value(step)?)?;
                }
            }
        }
    }
    if manifest
        .trust
        .is_some_and(|trust| trust != ExtensionTrustLevel::Community)
    {
        bail!("public manifests cannot assert verified or untrusted registry status");
    }
    let mut requested = BTreeSet::new();
    if manifest.permissions.len() > 32 {
        bail!("too many public extension permissions");
    }
    for permission in &manifest.permissions {
        if !crate::extensions::permissions::public_permission_name(permission)
            || !requested.insert(permission.clone())
        {
            bail!("unknown or repeated public extension permission");
        }
    }
    let selection = match release.runtime_profile {
        RuntimeProfile::Declarative => {
            if release.kind == ExtensionKind::Module
                || manifest.runtime.is_some()
                || selected_artifact.is_some()
            {
                bail!("declarative extensions cannot declare a code runtime or artifact");
            }
            PublicRuntimeSelection::Declarative
        }
        RuntimeProfile::SourceJs | RuntimeProfile::Reproducible => {
            let runtime = manifest
                .runtime
                .as_ref()
                .context("public module runtime required")?;
            if profile != Some(serde_json::to_value(release.runtime_profile)?)
                || runtime.r#type != "container"
            {
                bail!("manifest execution profile differs from the signed release");
            }
            // Runtime identity, mounts and network namespace belong to Elixir,
            // not publisher-controlled image names or host paths. More detailed
            // state/egress/secret/resource grant checks remain mandatory later.
            if runtime.image.is_some()
                || runtime.network.is_some()
                || !runtime.volumes.is_empty()
                || runtime
                    .ports
                    .iter()
                    .any(|port| port.container == 0 || port.host.is_some())
            {
                bail!("public runtime cannot select an image, network, host port or mount");
            }
            if release.runtime_profile == RuntimeProfile::SourceJs {
                if artifact.is_some() || selected_artifact.is_some() {
                    bail!("source runtime cannot select a compiled artifact");
                }
                let path = entrypoint
                    .as_ref()
                    .and_then(Value::as_str)
                    .context("source entrypoint required")?;
                safe_source_path(path)?;
                if !path.ends_with(".mjs") {
                    bail!("source entrypoint must be an ES module");
                }
                let file = source
                    .entrypoint
                    .as_ref()
                    .filter(|file| file.path == path)
                    .context("entrypoint is not in the verified source package")?;
                PublicRuntimeSelection::SourceJs {
                    entrypoint: file.clone(),
                }
            } else {
                if entrypoint.is_some() || source.entrypoint.is_some() {
                    bail!("compiled runtime cannot select a source entrypoint");
                }
                require_host_platform_selection(
                    &artifact.context("compiled host-platform artifact selection required")?,
                )?;
                let selected = selected_artifact
                    .context("compiled artifact was not selected in this quarantine job")?;
                if !release.artifacts.iter().any(|value| {
                    value.blob.sha256 == selected.sha256 && value.blob.size == selected.size
                }) {
                    bail!("compiled artifact is outside the signed release");
                }
                PublicRuntimeSelection::Reproducible {
                    artifact: selected.clone(),
                }
            }
        }
    };
    Ok((manifest, selection, requested.into_iter().collect()))
}

fn check_closed_projection(raw: &Value, typed: &Value) -> Result<()> {
    match (raw, typed) {
        (Value::Object(fields), Value::Object(known)) => {
            for (key, value) in fields {
                check_closed_projection(
                    value,
                    known.get(key).context("unknown public manifest field")?,
                )?;
            }
        }
        (Value::Array(items), Value::Array(known)) if items.len() == known.len() => {
            for (raw, typed) in items.iter().zip(known) {
                check_closed_projection(raw, typed)?;
            }
        }
        _ if raw == typed => {}
        _ => bail!("public manifest value was silently normalized"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use elixir_marketplace::identity::Digest;
    use serde_json::json;

    fn fixture() -> (Value, Release, SourceMatchSummary) {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))
        .unwrap();
        let release: Release =
            serde_json::from_value(fixture["release"]["payload"].clone()).unwrap();
        let value = json!({"id": release.extension_id, "version": release.version, "kind": "module", "name": release.display_name,
            "permissions": ["runtime.manage_containers", "network.egress"],
            "provides": [{"capability":"acquisition.candidate_provider", "slot":"default", "cardinality":"many", "implementation":"fixture", "scope":{"media_types":["movies"],"actions":["search"]}}],
            "runtime":{"type":"container", "profile":"source-js-v1", "entrypoint":"src/provider.mjs"}});
        let summary = SourceMatchSummary {
            release_id: Digest::of(b"test"),
            package_sha256: release.package.sha256,
            proof_sha256: release.source.proof.sha256,
            manifest_sha256: release.manifest_sha256,
            tree_sha256: release.source.tree_sha256,
            commit: release.source.commit.clone(),
            files: 3,
            manifest_base64: STANDARD.encode(serde_json::to_vec(&value).unwrap()),
            license: SourceEntrypoint {
                path: release.source.license_path.clone(),
                sha256: Digest::of(b"SPDX-License-Identifier: MIT\n"),
                size: b"SPDX-License-Identifier: MIT\n".len() as u64,
            },
            entrypoint: Some(SourceEntrypoint {
                path: "src/provider.mjs".into(),
                sha256: Digest::of(b"export default {}"),
                size: 17,
            }),
        };
        (value, release, summary)
    }
    fn check(
        value: &Value,
        release: &Release,
        source: &SourceMatchSummary,
        artifact: Option<&Blob>,
    ) -> Result<(ExtensionManifest, PublicRuntimeSelection, Vec<String>)> {
        check_document(
            ManifestDocument::parse(&serde_json::to_vec(value)?)?,
            release,
            source,
            artifact,
        )
    }
    #[test]
    fn source_profile_preserves_operational_manifest_and_requests_not_grants() -> Result<()> {
        let (value, release, source) = fixture();
        let (manifest, selection, permissions) = check(&value, &release, &source, None)?;
        assert_eq!(manifest.id, release.extension_id.as_str());
        assert!(matches!(selection, PublicRuntimeSelection::SourceJs { .. }));
        assert_eq!(permissions, ["network.egress", "runtime.manage_containers"]);
        assert!(manifest.runtime.unwrap().image.is_none());
        Ok(())
    }
    #[test]
    fn identity_schema_profile_permissions_and_host_escape_attempts_are_rejected() {
        let (value, release, source) = fixture();
        for (pointer, change) in [
            ("/id", json!("elixir.modules.qbittorrent")),
            ("/version", json!("9.0.0")),
            ("/kind", json!("blueprint")),
            ("/name", json!("Another")),
            ("/runtime/profile", json!("reproducible-v1")),
            ("/runtime/entrypoint", json!("../evil.mjs")),
            ("/permissions", json!(["network.egress", "network.egress"])),
            ("/permissions", json!(["docker.socket"])),
            ("/provides", json!([])),
            ("/provides/0/scope/actions", json!([])),
        ] {
            let mut bad = value.clone();
            *bad.pointer_mut(pointer).unwrap() = change;
            assert!(check(&bad, &release, &source, None).is_err(), "{pointer}");
        }
        for (pointer, key, change) in [
            ("", "trust", json!("verified")),
            ("", "allow_unsafe", json!(true)),
            ("/runtime", "image", json!("evil:latest")),
            ("/runtime", "network", json!("host")),
            (
                "/runtime",
                "volumes",
                json!(["/var/run/docker.sock:/var/run/docker.sock"]),
            ),
            ("/runtime", "ports", json!([{"container":8097,"host":8097}])),
            ("/runtime", "ports", json!([{"container":8097,"host":0}])),
            ("/runtime", "privileged", json!(true)),
            ("/runtime", "security", json!({"privileged":true})),
            ("/runtime", "artifact", json!(Digest::of(b"unselected"))),
            ("/provides/0", "hidden", json!({"exec":"sh"})),
        ] {
            let mut bad = value.clone();
            bad.pointer_mut(pointer).unwrap()[key] = change;
            assert!(
                check(&bad, &release, &source, None).is_err(),
                "{pointer}/{key}"
            );
        }
        let mut missing = source.clone();
        missing.entrypoint = None;
        assert!(check(&value, &release, &missing, None).is_err());
    }
    #[test]
    fn declarative_connector_uses_existing_schema_without_a_code_runtime() -> Result<()> {
        let (_, mut release, mut source) = fixture();
        release.kind = ExtensionKind::Connector;
        release.runtime_profile = RuntimeProfile::Declarative;
        source.entrypoint = None;
        let value = json!({"id":release.extension_id,"version":release.version,"kind":"connector","name":release.display_name,
            "permissions":["drivers.configure.indexer.registry"], "targets":[{"capability":"indexer.registry","slot":"default"}],
            "actions":[{"type":"driver_patch","target":{"capability":"indexer.registry","slot":"default"},"patch":{"enabled":true}}]});
        assert!(matches!(
            check(&value, &release, &source, None)?.1,
            PublicRuntimeSelection::Declarative
        ));
        let mut bad = value.clone();
        bad["actions"][0]["run_shell"] = json!("sh");
        assert!(check(&bad, &release, &source, None).is_err());
        let mut bad = value;
        bad["runtime"] = json!({"type":"internal"});
        assert!(check(&bad, &release, &source, None).is_err());
        Ok(())
    }
    #[test]
    fn reproducible_selection_binds_the_signed_quarantined_artifact_but_never_claims_reproduction()
    -> Result<()> {
        let (mut value, mut release, mut source) = fixture();
        let blob = Blob {
            sha256: Digest::of(b"compiled"),
            size: 8,
        };
        release.runtime_profile = RuntimeProfile::Reproducible;
        release
            .artifacts
            .push(elixir_marketplace::records::CompiledArtifact {
                blob: blob.clone(),
                architecture: "linux/amd64".into(),
                builder_image_digest: Digest::of(b"builder"),
                command: vec!["build".into()],
                environment: Default::default(),
                inputs: vec![],
            });
        value["runtime"] = json!({"type":"container","profile":"reproducible-v1","artifact":{"selection":"host-platform"}});
        source.entrypoint = None;
        assert!(check(&value, &release, &source, None).is_err());
        assert!(matches!(
            check(&value, &release, &source, Some(&blob))?.1,
            PublicRuntimeSelection::Reproducible { .. }
        ));
        let mut wrong = blob.clone();
        wrong.size += 1;
        assert!(check(&value, &release, &source, Some(&wrong)).is_err());
        for invalid in [
            json!(blob.sha256),
            json!({"selection":"host-platform","sha256":blob.sha256}),
            json!({"selection":"any"}),
        ] {
            value["runtime"]["artifact"] = invalid;
            assert!(check(&value, &release, &source, Some(&blob)).is_err());
        }
        Ok(())
    }
    #[test]
    fn declarative_blueprint_steps_are_closed_without_changing_legacy_schema() -> Result<()> {
        let (_, mut release, mut source) = fixture();
        release.kind = ExtensionKind::Blueprint;
        release.runtime_profile = RuntimeProfile::Declarative;
        source.entrypoint = None;
        let value = json!({"id":release.extension_id,"version":release.version,"kind":"blueprint","name":release.display_name,
            "execution":{"packages":["elixir.modules.fixture"], "instances":[{"id":"fixture","extension_id":"elixir.modules.fixture"}], "phases":[{"id":"install", "steps":[{"type":"ensure_package_installed","extension_id":"elixir.modules.fixture"}]}]}});
        assert!(matches!(
            check(&value, &release, &source, None)?.1,
            PublicRuntimeSelection::Declarative
        ));
        let mut bad = value.clone();
        bad["execution"]["phases"][0]["steps"][0]["run_shell"] = json!("sh");
        assert!(check(&bad, &release, &source, None).is_err());
        let mut bad = value;
        bad["execution"]["phases"][0]["steps"][0]["type"] = json!("run_shell");
        assert!(check(&bad, &release, &source, None).is_err());
        Ok(())
    }
    #[test]
    fn both_legacy_requires_forms_reject_silently_ignored_fields() -> Result<()> {
        let (mut value, release, source) = fixture();
        for requires in [
            json!([{"capability":"indexer.registry","slot":"default"}]),
            json!({"capabilities":[],"downloads":[{"kind":"torrent","mode":"broker","optional":true}]}),
        ] {
            value["requires"] = requires;
            check(&value, &release, &source, None)?;
        }
        for requires in [
            json!([{"capability":"indexer.registry","hidden":true}]),
            json!({"capabilities":[],"hidden":true}),
            json!({"downloads":[{"kind":"torrent","hidden":true}]}),
        ] {
            value["requires"] = requires;
            assert!(check(&value, &release, &source, None).is_err());
        }
        Ok(())
    }
}
