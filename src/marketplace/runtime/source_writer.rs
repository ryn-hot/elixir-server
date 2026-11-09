//! Bounded creation identity for cleanup only. This is not an executable spec,
//! resource/grant authority or permission to start a recovered container.
use super::image_is_pinned;
use crate::runtime::{
    model::{
        CONTAINER_SPEC_HASH_LABEL, ContainerSpec, ELIXIR_DEPLOYMENT_ID_LABEL,
        ELIXIR_EXTENSION_ID_LABEL, ELIXIR_INSTANCE_ID_LABEL, ELIXIR_MANAGED_LABEL,
        container_spec_fingerprint,
    },
    packet_network::immutable_id,
};
use anyhow::{Context, Result, bail};

/// One shared closed command set for pre-journal checks and native creation.
/// This is configuration data, never permission to create/start an actor.
pub(crate) fn supported_command(command: &[String]) -> bool {
    matches!(command, [mode, _] if matches!(mode.as_str(),
        "--quarantine" | "--export-blob" | "--source-match" | "--build-inputs-check" | "--container-image-check" | "--container-filesystem-check" | "--container-image-export" | "--source-eval" | "--source-eval-verified"
        | "--build-output-init" | "--build-run" | "--build-output-check" | "--container-eval"))
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceWriterIntent {
    deployment: String,
    fingerprint: String,
}

impl SourceWriterIntent {
    pub(crate) fn capture(spec: &ContainerSpec) -> Result<Self> {
        let value = Self {
            deployment: spec
                .labels
                .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                .context("source creation deployment missing")?
                .clone(),
            fingerprint: spec
                .labels
                .get(CONTAINER_SPEC_HASH_LABEL)
                .context("source creation fingerprint missing")?
                .clone(),
        };
        value.validate()?;
        if spec.name != value.name()
            || !image_is_pinned(&spec.image)
            || spec.network_mode.as_deref() != Some("none")
            || spec.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || spec
                .labels
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                != Some("quarantine-v1")
            || spec
                .labels
                .get(ELIXIR_EXTENSION_ID_LABEL)
                .map(String::as_str)
                != Some("elixir-marketplace-source")
            || spec.labels.get(ELIXIR_INSTANCE_ID_LABEL)
                != Some(&format!("marketplace-source-{}", value.deployment))
            || value.fingerprint != container_spec_fingerprint(spec)
        {
            bail!("source creation intent differs from its original offline profile");
        }
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.deployment.is_empty()
            || self.deployment.len() > 64
            || !self
                .deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !immutable_id(&self.fingerprint)
        {
            bail!("invalid bounded source creation intent");
        }
        Ok(())
    }
    pub(crate) fn deployment(&self) -> &str {
        &self.deployment
    }
    pub(crate) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    pub(crate) fn name(&self) -> String {
        format!("elixir-marketplace-source-writer-{}", self.deployment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closed_source_commands_are_shared_by_journaling_and_native_creation() {
        for mode in [
            "--quarantine",
            "--export-blob",
            "--source-match",
            "--build-inputs-check",
            "--container-image-check",
            "--container-filesystem-check",
            "--container-image-export",
            "--build-output-init",
            "--build-run",
            "--build-output-check",
            "--source-eval",
            "--source-eval-verified",
            "--container-eval",
        ] {
            assert!(supported_command(&[mode.into(), "{}".into()]));
        }
        for command in [
            vec![],
            vec!["--source-eval-verified".into()],
            vec!["--source-eval-verified".into(), "{}".into(), "extra".into()],
            vec!["--self-test-source-eval-verified".into(), "{}".into()],
            vec!["/bin/sh".into(), "-c".into()],
        ] {
            assert!(!supported_command(&command));
        }
    }
    #[test]
    fn capture_requires_original_fingerprint_and_dedicated_offline_identity() -> Result<()> {
        // Identity data only; this fixture is not a runnable source profile.
        let mut spec: ContainerSpec = serde_json::from_value(serde_json::json!({
            "name":"elixir-marketplace-source-writer-owned","image":format!("sha256:{}","a".repeat(64)),
            "network":"","network_mode":"none","labels":{"elixir.managed":"true","elixir.deployment_id":"owned",
                "elixir.extension_id":"elixir-marketplace-source","elixir.instance_id":"marketplace-source-owned",
                "elixir.marketplace.isolation":"quarantine-v1"}}))?;
        crate::runtime::model::apply_container_spec_fingerprint(&mut spec);
        let intent = SourceWriterIntent::capture(&spec)?;
        assert_eq!(intent.name(), spec.name);
        let mut changed = spec.clone();
        changed.command.push("changed".into());
        assert!(SourceWriterIntent::capture(&changed).is_err());
        for (field, wrong) in [
            (ELIXIR_DEPLOYMENT_ID_LABEL, "../foreign"),
            (ELIXIR_MANAGED_LABEL, "false"),
            (ELIXIR_EXTENSION_ID_LABEL, "foreign"),
        ] {
            let mut changed = spec.clone();
            changed.labels.insert(field.into(), wrong.into());
            crate::runtime::model::apply_container_spec_fingerprint(&mut changed);
            assert!(SourceWriterIntent::capture(&changed).is_err());
        }
        let mut changed = spec.clone();
        changed.image = "worker:latest".into();
        crate::runtime::model::apply_container_spec_fingerprint(&mut changed);
        assert!(SourceWriterIntent::capture(&changed).is_err());
        Ok(())
    }
}
