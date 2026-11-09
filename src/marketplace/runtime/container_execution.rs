//! Closed compiled-runner configuration DATA. Source/image evidence, guardian
//! approval, admission and live instance grants are independent requirements.
use super::*;
use crate::runtime::model::{VolumeMount, VolumeMountSourceKind};
use elixir_marketplace::{
    canonical,
    container_execution::{ContainerExecutionDescriptor, GUARDIAN_PATH},
    identity::Digest,
};

pub(crate) const GUARDIAN_LABEL: &str = "elixir.marketplace.container_guardian";
pub(crate) fn is_command(command: &[String]) -> bool {
    matches!(command, [mode, _] if mode == "--container-eval")
}
pub(crate) fn descriptor(spec: &ContainerSpec) -> Result<ContainerExecutionDescriptor> {
    if !is_command(&spec.command) {
        bail!("compiled execution command differs");
    }
    ContainerExecutionDescriptor::parse(spec.command[1].as_bytes()).map_err(Into::into)
}
pub(crate) fn prepare(
    spec: &mut ContainerSpec,
    value: &ContainerExecutionDescriptor,
    guardian: &crate::runtime::container_guardian::Profile,
) -> Result<()> {
    value.validate()?;
    guardian.require()?;
    if guardian.platform() != value.image.platform {
        bail!("guardian architecture differs from reproduced image");
    }
    spec.image = value.image.config.to_string();
    spec.command = vec![
        "--container-eval".into(),
        String::from_utf8(canonical::to_vec(value)?)?,
    ];
    spec.volumes = vec![VolumeMount {
        source_kind: VolumeMountSourceKind::Bind,
        host_path: guardian
            .path()
            .to_str()
            .context("guardian path UTF-8")?
            .into(),
        container_path: GUARDIAN_PATH.into(),
        read_only: true,
    }];
    spec.security.file_size_limit_bytes = Some(8 * MIB);
    spec.labels
        .insert(GUARDIAN_LABEL.into(), guardian.digest().to_string());
    apply_container_spec_fingerprint(spec);
    validate_spec(spec)
}
pub(crate) fn validate_spec(spec: &ContainerSpec) -> Result<()> {
    let value = descriptor(spec)?;
    let _: Digest = spec
        .labels
        .get(GUARDIAN_LABEL)
        .context("guardian binary approval absent")?
        .parse()?;
    let path = spec.volumes.first().context("guardian bind absent")?;
    super::source_writer::SourceWriterIntent::capture(spec)?;
    if spec.image != value.image.config.to_string()
        || spec.network_mode.as_deref() != Some("none")
        || !spec.env.is_empty()
        || !spec.aliases.is_empty()
        || !spec.ports.is_empty()
        || !spec.cap_add.is_empty()
        || spec.cap_drop != ["ALL"]
        || !spec.devices.is_empty()
        || !spec.sysctls.is_empty()
        || spec.volumes.len() != 1
        || path.source_kind != VolumeMountSourceKind::Bind
        || !std::path::Path::new(&path.host_path).is_absolute()
        || path.host_path.len() > 4096
        || path.host_path.chars().any(|c| c == ':' || c.is_control())
        || path.container_path != GUARDIAN_PATH
        || !path.read_only
        || spec.security.user.as_deref() != Some("10004:10004")
        || !spec.security.read_only_rootfs
        || !spec.security.no_new_privileges
        || spec.security.init_process
        || !spec.security.stdin_open
        || spec.security.file_size_limit_bytes != Some(8 * MIB)
        || spec.security.tmpfs.len() != 1
        || spec.security.tmpfs[0].path != "/tmp"
        || spec.security.seccomp_profile.is_some()
        || spec.security.apparmor_profile.is_some()
    {
        bail!("compiled executor differs from its closed offline guardian profile");
    }
    Ok(())
}
pub(crate) fn require_guardian(
    spec: &ContainerSpec,
    guardian: &crate::runtime::container_guardian::Profile,
) -> Result<()> {
    validate_spec(spec)?;
    guardian.require()?;
    let value = descriptor(spec)?;
    if spec.volumes[0].host_path != guardian.path().to_str().context("guardian path UTF-8")?
        || spec.labels.get(GUARDIAN_LABEL) != Some(&guardian.digest().to_string())
        || value.image.platform != guardian.platform()
    {
        bail!("compiled execution did not select the independently approved guardian");
    }
    Ok(())
}
pub(crate) fn bind_matches(
    spec: &ContainerSpec,
    mount: &crate::runtime::model::ContainerRuntimeMount,
) -> bool {
    is_command(&spec.command)
        && mount.mount_type == "bind"
        && mount.read_only
        && mount.destination == GUARDIAN_PATH
        && spec.volumes.iter().any(|desired| {
            desired.source_kind == VolumeMountSourceKind::Bind
                && mount.source.as_deref() == Some(desired.host_path.as_str())
                && desired.container_path == GUARDIAN_PATH
                && desired.read_only
        })
}
pub(crate) fn verify_configuration(
    spec: &ContainerSpec,
    state: &ContainerRuntimeState,
    limits: &SourceJobLimits,
) -> Result<()> {
    validate_spec(spec)?;
    super::verify_inspect(spec, state, limits)?;
    if state.security.stdin_open != Some(true)
        || state.security.tty != Some(false)
        || !state
            .security
            .file_size_limit
            .as_ref()
            .is_some_and(|l| l.soft == (8 * MIB) as i64 && l.hard == (8 * MIB) as i64)
        || state
            .mounts
            .iter()
            .filter(|m| bind_matches(spec, m))
            .count()
            != 1
    {
        bail!("compiled guardian bind/private transport differs from profile");
    }
    Ok(())
}
pub(crate) fn verify_kernel(report: &str, limits: &SourceJobLimits) -> Result<()> {
    super::verify_kernel(report, limits)?;
    if !report.lines().any(|l| l == "Pid:\t1") || !report.lines().any(|l| l == "PPid:\t0") {
        bail!("compiled guardian is not the namespace init process");
    }
    let line = report
        .lines()
        .find(|l| l.starts_with("Max file size"))
        .context("compiled file limit absent")?;
    let fields = line.split_whitespace().collect::<Vec<_>>();
    let bytes = (8 * MIB).to_string();
    if fields.get(3).copied() != Some(bytes.as_str())
        || fields.get(4).copied() != Some(bytes.as_str())
    {
        bail!("compiled file limit differs from profile");
    }
    Ok(())
}
pub(crate) fn verify_environment(spec: &ContainerSpec, actual: &serde_json::Value) -> Result<()> {
    let value = descriptor(spec)?;
    let env = actual
        .as_array()
        .context("compiled ambient environment absent")?;
    let mut keys = std::collections::BTreeSet::new();
    for item in env {
        let (key, text) = item
            .as_str()
            .and_then(|s| s.split_once('='))
            .context("invalid compiled environment")?;
        let default_path = key == "PATH"
            && !value.image.command.environment.contains_key(key)
            && text == "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
        if !keys.insert(key)
            || (!default_path
                && value.image.command.environment.get(key).map(String::as_str) != Some(text))
        {
            bail!("compiled environment differs from the independently imported image");
        }
    }
    if value
        .image
        .command
        .environment
        .keys()
        .any(|key| !keys.contains(key.as_str()))
    {
        bail!("compiled image environment missing");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use elixir_marketplace::{
        container_image::ContainerCommand,
        container_image::job::ContainerImageSummary,
        quarantine::QuarantineWorkspace,
        records::{Blob, CompiledArtifact, Release, RuntimeProfile},
        reproduction::BuildPlatform,
        source_execution::{SourceExecutionLimits, SourceExecutionMode},
        source_job::SourceMatchDescriptor,
    };
    use std::collections::BTreeMap;
    fn spec() -> Result<ContainerSpec> {
        // Pure profile DATA, never a source/import receipt or real native image.
        let json: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
        ))?;
        let mut release: Release = serde_json::from_value(json["release"]["payload"].clone())?;
        let artifact = Blob {
            sha256: Digest::of(b"artifact"),
            size: 4096,
        };
        release.runtime_profile = RuntimeProfile::Reproducible;
        release.artifacts = vec![CompiledArtifact {
            blob: artifact.clone(),
            architecture: "linux/amd64".into(),
            builder_image_digest: Digest::of(b"compiler DATA"),
            command: vec!["cc".into()],
            environment: BTreeMap::new(),
            inputs: vec![],
        }];
        let id = Digest::of(canonical::to_vec(&release)?);
        let image = ContainerImageSummary {
            schema_version: 1,
            release: id,
            artifact: artifact.clone(),
            platform: BuildPlatform::LinuxAmd64,
            manifest: Digest::of(b"manifest"),
            config: Digest::of(b"config"),
            layers: vec![Blob {
                sha256: Digest::of(b"layer"),
                size: 1024,
            }],
            command: ContainerCommand {
                entrypoint: vec!["/app/provider".into()],
                arguments: vec![],
                environment: BTreeMap::new(),
                working_directory: "/app".into(),
            },
        };
        let value = ContainerExecutionDescriptor {
            schema_version: 1,
            invocation_id: Digest::of(b"call"),
            source: SourceMatchDescriptor {
                schema_version: 1,
                workspace: QuarantineWorkspace {
                    schema_version: 1,
                    release_id: id,
                    package: release.package.clone(),
                    proof: release.source.proof.clone(),
                    artifact: Some(artifact),
                    timeout_seconds: 120,
                },
                release,
            },
            image,
            limits: SourceExecutionLimits {
                profile: Default::default(),
                heap_bytes: 32 * MIB,
                timeout_millis: 10_000,
                max_file_bytes: 8 * MIB,
                json_bytes: MIB,
                pending_jobs: 128,
                sdk: None,
            },
            mode: SourceExecutionMode::Invoke,
        };
        let mut spec: ContainerSpec = serde_json::from_value(
            serde_json::json!({"name":"elixir-marketplace-source-writer-owned",
            "image":value.image.config,"network":"","network_mode":"none","labels":{"elixir.managed":"true","elixir.deployment_id":"owned",
                "elixir.extension_id":"elixir-marketplace-source","elixir.instance_id":"marketplace-source-owned"}}),
        )?;
        prepare_source_worker(&mut spec, &SourceJobLimits::new(128 * MIB, 200)?)?;
        spec.security.tmpfs.retain(|m| m.path == "/tmp");
        spec.security.stdin_open = true;
        spec.security.file_size_limit_bytes = Some(8 * MIB);
        spec.labels.insert(
            "elixir.marketplace.isolation".into(),
            "quarantine-v1".into(),
        );
        spec.labels.insert(
            GUARDIAN_LABEL.into(),
            Digest::of(b"guardian DATA").to_string(),
        );
        spec.volumes = vec![VolumeMount {
            source_kind: VolumeMountSourceKind::Bind,
            host_path: "/independently-approved/guardian".into(),
            container_path: GUARDIAN_PATH.into(),
            read_only: true,
        }];
        spec.command = vec![
            "--container-eval".into(),
            String::from_utf8(canonical::to_vec(&value)?)?,
        ];
        apply_container_spec_fingerprint(&mut spec);
        Ok(spec)
    }
    #[test]
    fn compiled_profile_cannot_add_network_paths_secrets_shells_or_mutable_guardian() -> Result<()>
    {
        let spec = spec()?;
        validate_spec(&spec)?;
        for variant in 0..10 {
            let mut changed = spec.clone();
            match variant {
                0 => changed.network_mode = Some("host".into()),
                1 => changed.volumes[0].read_only = false,
                2 => changed.volumes[0].container_path = "/quarantine".into(),
                3 => changed.volumes.push(changed.volumes[0].clone()),
                4 => changed.command = vec!["/bin/sh".into(), "-c".into()],
                5 => changed.image = Digest::of(b"other").to_string(),
                6 => changed.security.init_process = true,
                7 => changed.security.stdin_open = false,
                8 => changed.cap_add = vec!["SYS_ADMIN".into()],
                _ => changed.security.seccomp_profile = Some("unconfined".into()),
            }
            apply_container_spec_fingerprint(&mut changed);
            assert!(validate_spec(&changed).is_err(), "variant {variant}");
        }
        Ok(())
    }
    #[tokio::test]
    async fn syntactically_valid_compiled_spec_cannot_configure_its_own_native_guardian()
    -> Result<()> {
        let manager = crate::runtime::docker::DockerRuntimeManager::new(None);
        assert!(manager.marketplace_container_guardian().is_none());
        let error = manager
            .create_quarantine_marketplace_writer(&spec()?)
            .await
            .err()
            .context("unconfigured guardian was accepted")?;
        assert!(format!("{error:#}").contains("not independently configured"));
        Ok(())
    }
    #[test]
    fn compiled_ambient_environment_is_bound_to_image_not_worker_or_owner_secrets() -> Result<()> {
        let spec = spec()?;
        verify_environment(
            &spec,
            &serde_json::json!([
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ]),
        )?;
        for value in [
            serde_json::json!(["LD_PRELOAD=/unbound.so"]),
            serde_json::json!(["PATH=/other"]),
            serde_json::json!([
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "PATH=/other"
            ]),
        ] {
            assert!(verify_environment(&spec, &value).is_err());
        }
        Ok(())
    }
}
