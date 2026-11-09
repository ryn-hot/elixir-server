//! Closed offline build profiles. Publisher digests are data, not approvals.
//! These profiles reuse the source-writer's original create/bind/start owner.
use super::*;
use crate::runtime::model::{ContainerTmpfsMount, VolumeMount, VolumeMountSourceKind};
use elixir_marketplace::{
    build_job::{BuildExecutionDescriptor, BuildOutputWorkspace},
    canonical,
    identity::Digest,
};
use std::collections::{BTreeSet, HashMap};

pub(crate) const SLOT_LABEL: &str = "elixir.marketplace.build_slot";
pub(crate) fn is_command(command: &[String]) -> bool {
    matches!(command,[mode,_] if matches!(mode.as_str(),"--build-output-init"|"--build-run"|"--build-output-check"))
}
pub(crate) fn name(slot: &str) -> Result<String> {
    if slot.len() != 64
        || !slot
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        bail!("invalid build output lane identity");
    }
    Ok(format!("elixir-marketplace-build-output-{slot}"))
}
pub(crate) fn labels(slot: &str) -> Result<HashMap<String, String>> {
    name(slot)?;
    Ok(HashMap::from([
        (
            crate::runtime::model::ELIXIR_MANAGED_LABEL.into(),
            "true".into(),
        ),
        (
            crate::runtime::model::ELIXIR_DEPLOYMENT_ID_LABEL.into(),
            slot.into(),
        ),
        ("elixir.marketplace.volume".into(), "build-output-v1".into()),
    ]))
}

/// Trusted release/operator configuration only. Empty by default; no DTO,
/// environment variable, publisher record or image feature can approve itself.
#[derive(Default)]
pub(in crate::marketplace) struct ApprovedToolchains {
    images: BTreeSet<Digest>,
}
impl ApprovedToolchains {
    pub(in crate::marketplace) fn from_operator_digests(
        images: impl IntoIterator<Item = Digest>,
    ) -> Result<Self> {
        let images: BTreeSet<_> = images.into_iter().collect();
        if images.len() > 8 {
            bail!("at most eight independently approved build toolchains");
        }
        Ok(Self { images })
    }
    pub(in crate::marketplace) fn require(&self, image: Digest) -> Result<String> {
        if !self.images.contains(&image) {
            bail!("publisher toolchain is not independently approved");
        }
        Ok(image.to_string())
    }
}

pub(crate) fn file_limit(descriptor: &BuildExecutionDescriptor) -> u64 {
    descriptor
        .output
        .artifact
        .size
        .max(descriptor.inputs.source.workspace.package.size)
        .max(descriptor.inputs.source.workspace.proof.size)
        .max(elixir_marketplace::MAX_SOURCE_PACKAGE_BYTES)
}
fn tmpfs(path: &str, bytes: u64, exec: bool) -> ContainerTmpfsMount {
    let mut mount = owned_tmpfs(path, bytes.div_ceil(MIB));
    mount.no_exec = !exec;
    mount
}
pub(crate) fn input_bytes(d: &BuildExecutionDescriptor) -> u64 {
    (d.inputs.source.workspace.package.size + d.inputs.source.workspace.proof.size).div_ceil(MIB)
        * MIB
        + MIB
}
pub(crate) fn extra_disk(d: &BuildExecutionDescriptor) -> Result<u64> {
    d.validate()?;
    Ok(d.output.reservation_bytes()?
        + input_bytes(d)
        + 2 * d.source_disk_bytes
        + staging_reservation(&d.inputs.source)?)
}
pub(crate) fn staging_reservation(
    source: &elixir_marketplace::source_job::SourceMatchDescriptor,
) -> Result<u64> {
    source.validate()?;
    let mut inputs = source.workspace.clone();
    inputs.artifact = None;
    Ok(inputs.volume_reservation_bytes()? + 2 * MIB)
}
pub(crate) fn execution(
    source: &elixir_marketplace::source_job::SourceMatchDescriptor,
    timeout_millis: u32,
) -> Result<BuildExecutionDescriptor> {
    use elixir_marketplace::reproduction::{BuildInputDescriptor, BuildPlatform};
    let selected = source
        .workspace
        .artifact
        .as_ref()
        .context("compiled build needs the selected artifact")?;
    let artifact = source
        .release
        .artifacts
        .iter()
        .find(|a| a.blob.sha256 == selected.sha256 && a.blob.size == selected.size)
        .context("compiled selection absent from release")?;
    let platform = match artifact.architecture.as_str() {
        "linux/amd64" => BuildPlatform::LinuxAmd64,
        "linux/arm64" => BuildPlatform::LinuxArm64,
        _ => bail!("unsupported native build platform"),
    };
    let d = BuildExecutionDescriptor {
        schema_version: 1,
        inputs: BuildInputDescriptor {
            schema_version: 1,
            source: source.clone(),
            platform,
            timeout_millis,
        },
        output: BuildOutputWorkspace {
            schema_version: 1,
            release_id: source.workspace.release_id,
            artifact: selected.clone(),
            timeout_millis,
        },
        toolchain_image_digest: artifact.builder_image_digest,
        source_disk_bytes: (2 * source.workspace.package.size.max(selected.size) + 16 * MIB)
            .div_ceil(MIB)
            * MIB,
    };
    d.validate()?;
    Ok(d)
}
pub(crate) fn configure(
    base: &ContainerSpec,
    slot: &str,
    descriptor: &BuildExecutionDescriptor,
    mode: &str,
) -> Result<ContainerSpec> {
    descriptor.validate()?;
    let mut spec = base.clone();
    spec.command = vec![
        mode.into(),
        String::from_utf8(canonical::to_vec(descriptor)?)?,
    ];
    if mode != "--build-run" {
        spec.command[1] = String::from_utf8(canonical::to_vec(&descriptor.output)?)?;
    }
    spec.labels.insert(SLOT_LABEL.into(), slot.into());
    spec.volumes = vec![VolumeMount {
        source_kind: VolumeMountSourceKind::NamedVolume,
        host_path: name(slot)?,
        container_path: "/build-output".into(),
        read_only: mode == "--build-output-check",
    }];
    spec.security.tmpfs.retain(|mount| mount.path == "/tmp");
    spec.security.file_size_limit_bytes = Some(1);
    if mode == "--build-output-init" {
        spec.security.user = Some("0:0".into());
        // Use Docker's canonical capability spelling so exact configuration
        // comparison remains strict rather than ignoring capability drift.
        spec.cap_add = vec!["CAP_CHOWN".into()];
        spec.security.file_size_limit_bytes = Some(descriptor.output.artifact.size.max(1024));
    } else if mode == "--build-run" {
        spec.image = descriptor.toolchain_image_digest.to_string();
        spec.security.file_size_limit_bytes = Some(file_limit(descriptor));
        spec.security.tmpfs.extend([
            tmpfs("/build-input", input_bytes(descriptor), false),
            tmpfs("/build-source", descriptor.source_disk_bytes, true),
            tmpfs("/scratch", descriptor.source_disk_bytes, true),
        ]);
    }
    apply_container_spec_fingerprint(&mut spec);
    validate_spec(&spec)?;
    Ok(spec)
}
pub(crate) fn validate_spec(spec: &ContainerSpec) -> Result<()> {
    if !is_command(&spec.command) {
        bail!("invalid closed native build command");
    }
    let slot = spec
        .labels
        .get(SLOT_LABEL)
        .context("build output lane missing")?;
    let mode = spec.command[0].as_str();
    let (expected_limit, root, source) = if mode == "--build-run" {
        let d = BuildExecutionDescriptor::parse(spec.command[1].as_bytes())?;
        if spec.image != d.toolchain_image_digest.to_string() || d.source_disk_bytes % MIB != 0 {
            bail!("native build image or disk profile differs");
        }
        (file_limit(&d), false, Some(d))
    } else {
        let d = BuildOutputWorkspace::parse(spec.command[1].as_bytes())?;
        (
            if mode == "--build-output-init" {
                d.artifact.size.max(1024)
            } else {
                1
            },
            mode == "--build-output-init",
            None,
        )
    };
    if !image_is_pinned(&spec.image)
        || spec.network_mode.as_deref() != Some("none")
        || !spec.network.is_empty()
        || !spec.env.is_empty()
        || !spec.aliases.is_empty()
        || !spec.ports.is_empty()
        || !spec.devices.is_empty()
        || !spec.sysctls.is_empty()
        || !spec.security.read_only_rootfs
        || !spec.security.no_new_privileges
        || spec.security.user.as_deref() != Some(if root { "0:0" } else { "10004:10004" })
        || spec.cap_add
            != if root {
                vec!["CAP_CHOWN".to_owned()]
            } else {
                vec![]
            }
        || spec.volumes.len() != 1
        || spec.volumes[0].source_kind != VolumeMountSourceKind::NamedVolume
        || spec.volumes[0].host_path != name(slot)?
        || spec.volumes[0].container_path != "/build-output"
        || spec.volumes[0].read_only != (mode == "--build-output-check")
        || spec.security.file_size_limit_bytes != Some(expected_limit)
        || !spec.security.stdin_open
    {
        bail!("native build requires its closed offline profile");
    }
    let memory = spec
        .security
        .memory_limit_mb
        .context("build memory missing")?;
    let mut expected = vec![owned_tmpfs("/tmp", 8.min(memory / 16))];
    if let Some(d) = source {
        expected.extend([
            tmpfs("/build-input", input_bytes(&d), false),
            tmpfs("/build-source", d.source_disk_bytes, true),
            tmpfs("/scratch", d.source_disk_bytes, true),
        ]);
    }
    if spec.security.tmpfs != expected {
        bail!("native build scratch/input profile differs");
    }
    Ok(())
}

pub(crate) fn verify_configuration(
    spec: &ContainerSpec,
    state: &ContainerRuntimeState,
    limits: &SourceJobLimits,
) -> Result<()> {
    validate_spec(spec)?;
    verify_inspect(spec, state, limits)?;
    if state.security.stdin_open != Some(true)
        || state.security.tty != Some(false)
        || !state.security.file_size_limit.as_ref().is_some_and(|v| {
            Some(v.soft as u64) == spec.security.file_size_limit_bytes && v.soft == v.hard
        })
        || state
            .mounts
            .iter()
            .filter(|m| {
                m.mount_type == "volume"
                    && m.name.as_deref() == Some(spec.volumes[0].host_path.as_str())
                    && m.destination == "/build-output"
                    && m.read_only == spec.volumes[0].read_only
            })
            .count()
            != 1
    {
        bail!("native build actual volume/stream/file limit differs");
    }
    Ok(())
}
pub(in crate::marketplace) async fn inspect_running(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    spec: &ContainerSpec,
    limits: &SourceJobLimits,
) -> Result<()> {
    let state = runtime
        .describe_container_runtime_state(&handle.id)
        .await?
        .context("build inspection unavailable")?;
    verify_configuration(spec, &state, limits)?;
    let pid = inspect_native_child_pid(runtime, handle).await?;
    let command = [
        "/bin/cat",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.swap.max",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/pids.max",
        &format!("/proc/{pid}/status"),
        &format!("/proc/{pid}/limits"),
    ]
    .map(str::to_owned);
    let report = runtime
        .exec_container_command(handle, "10004:10004", &command)
        .await?;
    verify_quarantine_kernel(
        &report,
        limits,
        spec.security
            .file_size_limit_bytes
            .context("build file limit missing")?,
        3,
        pid,
    )
}
