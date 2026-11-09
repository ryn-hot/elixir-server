//! Container isolation only, not a VPN/privacy or durable-storage attestation.
//! This initial profile has no writable host mounts. Its bounded tmpfs is an
//! ephemeral working cache; durable state and protected scheduling remain gates.
use crate::runtime::{
    RuntimeManager,
    model::{
        ContainerHandle, ContainerLogOptions, ContainerRuntimeState, ContainerSecurityOptions,
        ContainerSpec, ContainerTmpfsMount, apply_container_spec_fingerprint,
    },
};
use anyhow::{Context, Result, bail};
use elixir_marketplace::policy::OrdinaryLimits;

pub mod application_http;
pub(crate) mod build;
#[cfg(unix)]
pub(crate) mod container_execution;
pub mod source_writer;

pub const WORKER_UID: u32 = 10004;
pub const WORKER_GID: u32 = 10004;
pub const WORKER_PIDS: u64 = 64;
const MIB: u64 = 1024 * 1024;

/// Explicit foreground source-job resources, not automatic contribution limits
/// or execution authority. No Default/Deserialize/DerefMut: trusted allocation
/// must select a finite profile, then pass independent admission and review.
/// Keep the old plan encoding so unchanged 100m profiles retain their digests.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(transparent)]
pub struct SourceJobLimits(OrdinaryLimits);

impl SourceJobLimits {
    pub fn new(memory_bytes: u64, cpu_millicores: u32) -> Result<Self> {
        let limits = Self(OrdinaryLimits {
            memory_bytes,
            cpu_millicores,
            persistent_bytes: 16 * MIB,
            ..Default::default()
        });
        limits.validate()?;
        Ok(limits)
    }

    pub fn validate(&self) -> Result<()> {
        if !(64 * MIB..=256 * MIB).contains(&self.0.memory_bytes)
            || !self.0.memory_bytes.is_multiple_of(MIB)
            || !(1..=1000).contains(&self.0.cpu_millicores)
        {
            bail!("foreground source job requires 64-256 MiB and at most one CPU core");
        }
        Ok(())
    }
}

// Read-only OS configuration data shared with existing kernel inspectors.
// Ordinary worker entry points still call OrdinaryLimits::validate and reject
// more than 100m; this conversion cannot raise background resource ceilings.
impl std::ops::Deref for SourceJobLimits {
    type Target = OrdinaryLimits;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct WorkerIsolation {
    container: String,
    fixed_catalog: bool,
}

impl WorkerIsolation {
    pub fn container_name(&self) -> &str {
        &self.container
    }
    pub fn fixed_catalog_enforced(&self) -> bool {
        self.fixed_catalog
    }
}

fn namespace_is_isolated(mode: Option<&str>) -> bool {
    mode == Some("none")
        || mode
            .and_then(|mode| mode.strip_prefix("container:"))
            .is_some_and(|id| {
                id.len() == 64
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
}

pub(crate) fn image_is_pinned(image: &str) -> bool {
    let digest = image.strip_prefix("sha256:").or_else(|| {
        image
            .split_once("@sha256:")
            .filter(|(name, _)| {
                !name.is_empty() && !name.starts_with('-') && !name.chars().any(char::is_whitespace)
            })
            .map(|(_, digest)| digest)
    });
    digest.is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

fn owned_tmpfs(path: &str, size_mb: u64) -> ContainerTmpfsMount {
    ContainerTmpfsMount {
        path: path.to_owned(),
        size_mb: Some(size_mb),
        owner_uid: Some(WORKER_UID),
        owner_gid: Some(WORKER_GID),
        mode: Some(0o700),
        no_exec: true,
        no_suid: true,
        no_dev: true,
    }
}

/// Trusted controller API only. Never pass a peer-supplied ContainerSpec here.
/// Sharing a VPN namespace is insufficient protection: callers must separately
/// prove packet filtering, DNS/UDP/IPv6 behavior, allowance and health before I/O.
pub fn prepare_ephemeral_worker(spec: &mut ContainerSpec, limits: &OrdinaryLimits) -> Result<()> {
    limits.validate()?;
    configure_ephemeral_worker(spec, limits)
}

pub(crate) fn prepare_source_worker(
    spec: &mut ContainerSpec,
    limits: &SourceJobLimits,
) -> Result<()> {
    limits.validate()?;
    if spec.network_mode.as_deref() != Some("none") {
        bail!("foreground source jobs must remain offline");
    }
    configure_ephemeral_worker(spec, limits)
}

fn configure_ephemeral_worker(spec: &mut ContainerSpec, limits: &OrdinaryLimits) -> Result<()> {
    if !namespace_is_isolated(spec.network_mode.as_deref())
        || !image_is_pinned(&spec.image)
        || !spec.ports.is_empty()
        || !spec.aliases.is_empty()
        || !spec.env.is_empty()
        || !spec.volumes.is_empty()
        || !spec.cap_add.is_empty()
        || !spec.devices.is_empty()
        || !spec.sysctls.is_empty()
    {
        bail!(
            "marketplace worker requires a pinned image, isolated namespace, and no ports, secrets, devices or host mounts"
        );
    }
    if limits.memory_bytes % MIB != 0
        || limits.memory_bytes < 64 * MIB
        || limits.persistent_bytes < 16 * MIB
    {
        bail!("marketplace resource limits cannot support this isolated worker profile");
    }
    let memory_mb = limits.memory_bytes / MIB;
    let temporary_mb = 8.min(memory_mb / 16);
    let state_mb = (memory_mb * 3 / 4).min(limits.persistent_bytes / MIB - temporary_mb - 1);
    spec.cap_drop = vec!["ALL".to_string()];
    spec.security = ContainerSecurityOptions {
        user: Some(format!("{WORKER_UID}:{WORKER_GID}")),
        read_only_rootfs: true,
        no_new_privileges: true,
        tmpfs: vec![
            owned_tmpfs("/state", state_mb),
            owned_tmpfs("/tmp", temporary_mb),
        ],
        memory_limit_mb: Some(memory_mb),
        memory_swap_limit_mb: Some(memory_mb),
        pids_limit: Some(WORKER_PIDS),
        cpu_quota: Some(format!(
            "{}.{:03}",
            limits.cpu_millicores / 1000,
            limits.cpu_millicores % 1000
        )),
        shared_memory_mb: Some(1),
        disable_core_dumps: true,
        open_files_limit: Some(256),
        log_options: Some(ContainerLogOptions::Disabled),
        ..Default::default()
    };
    spec.labels.insert(
        "elixir.marketplace.isolation".to_string(),
        "ephemeral-v1".to_string(),
    );
    apply_container_spec_fingerprint(spec);
    Ok(())
}

fn verify_inspect(
    spec: &ContainerSpec,
    state: &ContainerRuntimeState,
    limits: &OrdinaryLimits,
) -> Result<()> {
    let build = build::is_command(&spec.command);
    if build {
        build::validate_spec(spec)?;
    }
    #[cfg(unix)]
    if container_execution::is_command(&spec.command) { container_execution::validate_spec(spec)?; }
    let security = &state.security;
    if state.name != spec.name
        || state.network_mode != spec.network_mode
        || !state.published_ports.is_empty()
        || spec
            .labels
            .iter()
            .any(|(key, value)| state.labels.get(key) != Some(value))
        || state.mounts.iter().any(|mount| {
            let tmpfs = mount.mount_type == "tmpfs"
                && !mount.read_only
                && spec
                    .security
                    .tmpfs
                    .iter()
                    .any(|expected| expected.path == mount.destination);
            let volume = mount.mount_type == "volume"
                && spec.volumes.iter().any(|expected| {
                    matches!(
                        expected.source_kind,
                        crate::runtime::model::VolumeMountSourceKind::NamedVolume
                    ) && mount.name.as_deref() == Some(expected.host_path.as_str())
                        && mount.destination == expected.container_path
                        && mount.read_only == expected.read_only
                });
            #[cfg(unix)]
            let guardian = container_execution::bind_matches(spec, mount);
            #[cfg(not(unix))]
            let guardian = false;
            !tmpfs && !volume && !guardian
        })
        || security.user != spec.security.user
        || security.image_reference.as_deref() != Some(spec.image.as_str())
        || !security
            .image_id
            .as_deref()
            .is_some_and(|id| id.starts_with("sha256:") && image_is_pinned(id))
        || (spec.image.starts_with("sha256:")
            && security.image_id.as_deref() != Some(spec.image.as_str()))
        || !security.read_only_rootfs
        || !security.no_new_privileges
        || security.privileged != Some(false)
        || security.cap_add != spec.cap_add
        || (!build && !security.cap_add.is_empty())
        || !security
            .cap_drop
            .iter()
            .any(|cap| cap.eq_ignore_ascii_case("ALL"))
        || security.memory_limit_bytes != Some(limits.memory_bytes as i64)
        || security.memory_swap_limit_bytes != Some(limits.memory_bytes as i64)
        || security.nano_cpus != Some(i64::from(limits.cpu_millicores) * 1_000_000)
        || security.pids_limit != Some(WORKER_PIDS as i64)
        || security.shared_memory_bytes != Some(MIB as i64)
        || !security
            .core_dump_limit
            .as_ref()
            .is_some_and(|limit| limit.soft == 0 && limit.hard == 0)
        || !security
            .open_files_limit
            .as_ref()
            .is_some_and(|limit| limit.soft == 256 && limit.hard == 256)
        || !security
            .log_config
            .as_ref()
            .is_some_and(|config| config.driver == "none" && config.options.is_empty())
        || security.seccomp_profile.as_deref() == Some("unconfined")
        || security.apparmor_profile.as_deref() == Some("unconfined")
    {
        bail!("marketplace worker container isolation differs from the required profile");
    }
    if security.tmpfs.len() != spec.security.tmpfs.len() {
        bail!("unexpected marketplace writable mounts");
    }
    for desired in &spec.security.tmpfs {
        // Compare each mount to its frozen owner, not the main worker UID:
        // the private Radicle scratch mount deliberately belongs to UID 10005.
        // Missing/unsafe desired fields are not permission to skip a check.
        let uid = desired
            .owner_uid
            .filter(|uid| *uid != 0)
            .context("missing safe tmpfs owner")?;
        let gid = desired
            .owner_gid
            .filter(|gid| *gid != 0)
            .context("missing safe tmpfs group")?;
        let mode = desired
            .mode
            .filter(|mode| *mode == 0o700)
            .context("unsafe tmpfs mode")?;
        let size = desired
            .size_mb
            .filter(|size| *size > 0)
            .context("missing bounded tmpfs size")?;
        if (!desired.no_exec && !build) || !desired.no_suid || !desired.no_dev {
            bail!("unsafe desired marketplace tmpfs flags");
        }
        let actual = security
            .tmpfs
            .iter()
            .find(|mount| mount.path == desired.path)
            .context("missing marketplace tmpfs")?;
        let options = actual
            .options
            .as_deref()
            .context("unknown marketplace tmpfs options")?
            .split(',')
            .collect::<Vec<_>>();
        let required = [
            if desired.no_exec { "noexec" } else { "exec" }.to_owned(),
            "nosuid".to_owned(),
            "nodev".to_owned(),
            format!("uid={uid}"),
            format!("gid={gid}"),
            format!("mode={mode:o}"),
            format!("size={size}m"),
        ];
        // Exact unordered options also reject duplicate/contradictory values
        // such as uid=10005,uid=0 rather than accepting one safe-looking token.
        if options.len() != required.len()
            || required
                .iter()
                .any(|expected| !options.contains(&expected.as_str()))
        {
            bail!("marketplace tmpfs ownership or limit differs from profile");
        }
    }
    Ok(())
}

fn verify_kernel(output: &str, limits: &OrdinaryLimits) -> Result<()> {
    if output.len() > 32 * 1024 {
        bail!("oversized marketplace kernel report");
    }
    let mut lines = output.lines();
    let memory: u64 = lines
        .next()
        .context("missing cgroup memory limit")?
        .parse()?;
    let swap: u64 = lines.next().context("missing cgroup swap limit")?.parse()?;
    let cpu = lines
        .next()
        .context("missing cgroup CPU limit")?
        .split_whitespace()
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let pids: u64 = lines.next().context("missing cgroup PID limit")?.parse()?;
    if memory != limits.memory_bytes
        || swap != 0
        || pids != WORKER_PIDS
        || cpu.len() != 2
        || cpu[1] == 0
        || u128::from(cpu[0]) * 1000 != u128::from(cpu[1]) * u128::from(limits.cpu_millicores)
    {
        bail!("marketplace cgroup limits are not enforced as requested");
    }
    let remaining = lines.collect::<Vec<_>>();
    for expected in [
        "NoNewPrivs:\t1",
        "Seccomp:\t2",
        "CapEff:\t0000000000000000",
        "CapInh:\t0000000000000000",
        "CapPrm:\t0000000000000000",
        "CapBnd:\t0000000000000000",
        "CapAmb:\t0000000000000000",
    ] {
        if !remaining.contains(&expected) {
            bail!("marketplace process privilege restrictions are not active");
        }
    }
    for (label, id) in [("Uid:", WORKER_UID), ("Gid:", WORKER_GID)] {
        let expected = format!("{label}\t{id}\t{id}\t{id}\t{id}");
        if !remaining.contains(&expected.as_str()) {
            bail!("marketplace process identity differs from profile");
        }
    }
    let core = remaining
        .iter()
        .find(|line| line.starts_with("Max core file size"))
        .context("unknown core-dump limit")?
        .split_whitespace()
        .collect::<Vec<_>>();
    if core.get(4) != Some(&"0") || core.get(5) != Some(&"0") {
        bail!("marketplace core dumps are not disabled");
    }
    let files = remaining
        .iter()
        .find(|line| line.starts_with("Max open files"))
        .context("unknown open-file limit")?
        .split_whitespace()
        .collect::<Vec<_>>();
    if files.get(3) != Some(&"256") || files.get(4) != Some(&"256") {
        bail!("marketplace open-file limit is not active");
    }
    Ok(())
}

pub(super) async fn inspect_quarantine_worker(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    spec: &ContainerSpec,
    limits: &SourceJobLimits,
    volume: &str,
    file_bytes: u64,
    read_only: bool,
) -> Result<()> {
    inspect_quarantine_profile(runtime, handle, spec, limits, volume, file_bytes, read_only)
        .await?;
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
        .exec_container_command(handle, &format!("{WORKER_UID}:{WORKER_GID}"), &command)
        .await?;
    verify_quarantine_kernel(&report, limits, file_bytes, 3, pid)
}

/// Source execution deliberately disables dumpability; reading its proc limits
/// would require weakening that guard. Inspect the immutable OCI ulimits and
/// the probe process's identical inherited limits instead. Native seccomp
/// denies later rlimit changes; inspect the native child's identity/filter
/// status, not the trusted PID1 deadline guardian.
pub(super) async fn inspect_source_executor(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    spec: &ContainerSpec,
    limits: &SourceJobLimits,
    volume: &str,
) -> Result<()> {
    inspect_quarantine_profile(runtime, handle, spec, limits, volume, 1, true).await?;
    let pid = inspect_native_child_pid(runtime, handle).await?;
    let command = [
        "/bin/cat",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.swap.max",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/pids.max",
        &format!("/proc/{pid}/status"),
        "/proc/self/limits",
    ]
    .map(str::to_owned);
    let report = runtime
        .exec_container_command(handle, &format!("{WORKER_UID}:{WORKER_GID}"), &command)
        .await?;
    verify_quarantine_kernel(&report, limits, 1, 4, pid)
}

fn native_child_pid(raw: &str) -> Result<u32> {
    if raw.len() > 16 {
        bail!("oversized native guardian child report");
    }
    let text = raw.trim();
    let pid: u32 = text.parse().context("missing native guardian child")?;
    if pid <= 1 || text != pid.to_string() {
        bail!("native guardian must have exactly one child");
    }
    Ok(pid)
}

async fn inspect_native_child_pid(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
) -> Result<u32> {
    // OCI setup may consume namespace PIDs before timeout forks. Never assume
    // PID2, scan proc, or accept publisher-selected process paths.
    let command = ["/bin/cat", "/proc/1/task/1/children"].map(str::to_owned);
    native_child_pid(
        &runtime
            .exec_container_command(handle, &format!("{WORKER_UID}:{WORKER_GID}"), &command)
            .await?,
    )
}

pub(crate) fn verify_measurement_kernel(report: &str, limits: &OrdinaryLimits) -> Result<()> {
    verify_kernel(report, limits)?;
    let limit = report
        .lines()
        .find(|line| line.starts_with("Max file size"))
        .context("measurement file limit missing")?
        .split_whitespace()
        .collect::<Vec<_>>();
    if limit.get(3) != Some(&"0") || limit.get(4) != Some(&"0") {
        bail!("measurement file writes are not disabled");
    }
    Ok(())
}
pub(crate) fn verify_measurement_configuration(
    spec: &ContainerSpec,
    actual: &ContainerRuntimeState,
    limits: &OrdinaryLimits,
) -> Result<()> {
    verify_inspect(spec, actual, limits)?;
    if !actual.mounts.is_empty()
        || !actual.security.tmpfs.is_empty()
        || actual.security.stdin_open != Some(true)
        || actual.security.tty != Some(false)
        || !actual
            .security
            .file_size_limit
            .as_ref()
            .is_some_and(|limit| limit.soft == 0 && limit.hard == 0)
    {
        bail!("read-only measurement configuration differs");
    }
    Ok(())
}
async fn inspect_quarantine_profile(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    spec: &ContainerSpec,
    limits: &SourceJobLimits,
    volume: &str,
    file_bytes: u64,
    read_only: bool,
) -> Result<()> {
    let state = runtime
        .describe_container_runtime_state(&handle.id)
        .await?
        .context("source quarantine inspection unavailable")?;
    verify_quarantine_configuration(spec, &state, limits, volume, file_bytes, read_only)?;
    if handle.name != spec.name {
        bail!("source quarantine handle differs from its profile");
    }
    Ok(())
}
/// OCI configuration can be inspected before execution. Kernel and successful
/// native-exit checks remain separate mandatory boundaries after startup.
pub(crate) fn verify_quarantine_configuration(
    spec: &ContainerSpec,
    state: &ContainerRuntimeState,
    limits: &SourceJobLimits,
    volume: &str,
    file_bytes: u64,
    read_only: bool,
) -> Result<()> {
    verify_inspect(spec, state, limits)?;
    if state.security.stdin_open != Some(true)
        || state.security.tty != Some(false)
        || !state
            .security
            .file_size_limit
            .as_ref()
            .is_some_and(|limit| limit.soft == file_bytes as i64 && limit.hard == file_bytes as i64)
        || state
            .mounts
            .iter()
            .filter(|mount| {
                mount.mount_type == "volume"
                    && mount.name.as_deref() == Some(volume)
                    && mount.destination == "/quarantine"
                    && mount.read_only == read_only
            })
            .count()
            != 1
    {
        bail!("source quarantine mount or private stream differs from profile");
    }
    Ok(())
}
fn verify_quarantine_kernel(
    report: &str,
    limits: &SourceJobLimits,
    file_bytes: u64,
    minimum_filters: u64,
    pid: u32,
) -> Result<()> {
    verify_kernel(report, limits)?;
    let filters = report.lines().find_map(|line| {
        line.strip_prefix("Seccomp_filters:")
            .and_then(|v| v.trim().parse::<u64>().ok())
    });
    let limit = report
        .lines()
        .find(|line| line.starts_with("Max file size"))
        .context("unknown quarantine file-size limit")?
        .split_whitespace()
        .collect::<Vec<_>>();
    let expected = file_bytes.to_string();
    let expected_pid = format!("Pid:\t{pid}");
    if filters.unwrap_or(0) < minimum_filters
        || pid <= 1
        || !report.lines().any(|line| line == expected_pid)
        || !report.lines().any(|line| line == "PPid:\t1")
        || limit.get(3).copied() != Some(expected.as_str())
        || limit.get(4).copied() != Some(expected.as_str())
    {
        bail!("source quarantine inherited kernel guards are not active");
    }
    Ok(())
}

/// Re-read actual Docker and kernel settings; desired flags/labels alone do not
/// create evidence. This proves neither public networking nor durable storage.
pub async fn inspect_isolated_worker(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    spec: &ContainerSpec,
    limits: &OrdinaryLimits,
) -> Result<WorkerIsolation> {
    let mut expected = spec.clone();
    prepare_ephemeral_worker(&mut expected, limits)?;
    let state = runtime
        .describe_container_runtime_state(&handle.id)
        .await?
        .context("worker runtime inspection unavailable")?;
    verify_inspect(&expected, &state, limits)?;
    let command = [
        "/bin/cat",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.swap.max",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/pids.max",
        "/proc/1/status",
        "/proc/1/limits",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    let report = runtime
        .exec_container_command(handle, &format!("{WORKER_UID}:{WORKER_GID}"), &command)
        .await?;
    verify_kernel(&report, limits)?;
    Ok(WorkerIsolation {
        container: handle.name.clone(),
        fixed_catalog: false,
    })
}

/// Verify the actual immutable fixed worker's mounts, cgroups and PID 1 limits.
/// This establishes no network permission or source/package execution authority.
pub async fn inspect_fixed_worker(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    profile: &super::storage::FixedWorkerSpec,
) -> Result<WorkerIsolation> {
    let state = runtime
        .describe_container_runtime_state(&handle.id)
        .await?
        .context("fixed worker inspection unavailable")?;
    verify_fixed_worker_state(handle, profile, &state)?;
    inspect_fixed_worker_kernel(runtime, handle, profile).await
}

/// Container configuration check also usable before START. Kernel checks are
/// deliberately separate; a stopped/created object's profile is not liveness.
pub(crate) fn verify_fixed_worker_state(
    handle: &ContainerHandle,
    profile: &super::storage::FixedWorkerSpec,
    state: &ContainerRuntimeState,
) -> Result<()> {
    let spec = &profile.spec;
    if handle.name != spec.name {
        bail!("fixed worker identity differs");
    }
    verify_inspect(spec, state, &profile.limits)?;
    if state.security.stdin_open != Some(true)
        || state.security.tty != Some(false)
        || !state
            .security
            .file_size_limit
            .as_ref()
            .is_some_and(|limit| {
                limit.soft == super::storage::FIXED_FILE_BYTES as i64
                    && limit.hard == super::storage::FIXED_FILE_BYTES as i64
            })
        || state
            .mounts
            .iter()
            .filter(|mount| {
                mount.mount_type == "volume"
                    && mount.name.as_deref() == Some(profile.volume.name())
                    && mount.destination == "/state"
                    && !mount.read_only
            })
            .count()
            != 1
    {
        bail!("fixed worker stream or persistent mount differs from profile");
    }
    Ok(())
}

async fn inspect_fixed_worker_kernel(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    profile: &super::storage::FixedWorkerSpec,
) -> Result<WorkerIsolation> {
    let command = [
        "/bin/cat",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.swap.max",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/pids.max",
        "/proc/1/status",
        "/proc/1/limits",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    let report = runtime
        .exec_container_command(handle, &format!("{WORKER_UID}:{WORKER_GID}"), &command)
        .await?;
    verify_kernel(&report, &profile.limits)?;
    let filters = report.lines().find_map(|line| {
        line.strip_prefix("Seccomp_filters:")
            .and_then(|value| value.trim().parse::<u64>().ok())
    });
    let file_limit = report
        .lines()
        .find(|line| line.starts_with("Max file size"))
        .context("unknown kernel file-size limit")?
        .split_whitespace()
        .collect::<Vec<_>>();
    let expected = super::storage::FIXED_FILE_BYTES.to_string();
    if filters.unwrap_or(0) < 2
        || file_limit.get(3).copied() != Some(expected.as_str())
        || file_limit.get(4).copied() != Some(expected.as_str())
    {
        bail!("fixed worker inherited kernel guards are not active");
    }
    Ok(WorkerIsolation {
        container: handle.name.clone(),
        fixed_catalog: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::model::{
        ContainerRuntimeLogConfig, ContainerRuntimeSecurityState, ContainerRuntimeTmpfsMount,
        ContainerRuntimeUlimit,
    };
    use std::collections::{BTreeMap, HashMap};

    fn spec() -> ContainerSpec {
        ContainerSpec {
            name: "elx-marketplace-worker-test".to_owned(),
            image: "alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce"
                .to_owned(),
            network: String::new(),
            network_mode: Some("none".to_owned()),
            aliases: vec![],
            env: vec![],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::new(),
            command: vec!["sleep".to_owned(), "600".to_owned()],
            cap_add: vec![],
            cap_drop: vec![],
            devices: vec![],
            sysctls: HashMap::new(),
            security: Default::default(),
        }
    }

    fn state(spec: &ContainerSpec) -> ContainerRuntimeState {
        ContainerRuntimeState {
            name: spec.name.clone(),
            network_mode: spec.network_mode.clone(),
            labels: spec.labels.clone(),
            mounts: vec![],
            published_ports: vec![],
            security: ContainerRuntimeSecurityState {
                image_reference: Some(spec.image.clone()),
                image_id: Some(format!("sha256:{}", "0".repeat(64))),
                user: spec.security.user.clone(),
                read_only_rootfs: true,
                no_new_privileges: true,
                cap_drop: vec!["ALL".to_owned()],
                tmpfs: spec
                    .security
                    .tmpfs
                    .iter()
                    .map(|mount| ContainerRuntimeTmpfsMount {
                        path: mount.path.clone(),
                        options: Some(format!(
                            "size={}m,uid={},gid={},mode={:o},noexec,nosuid,nodev",
                            mount.size_mb.unwrap(),
                            mount.owner_uid.unwrap(),
                            mount.owner_gid.unwrap(),
                            mount.mode.unwrap()
                        )),
                    })
                    .collect(),
                memory_limit_bytes: Some(256 * MIB as i64),
                memory_swap_limit_bytes: Some(256 * MIB as i64),
                pids_limit: Some(64),
                nano_cpus: Some(100_000_000),
                shared_memory_bytes: Some(MIB as i64),
                core_dump_limit: Some(ContainerRuntimeUlimit { soft: 0, hard: 0 }),
                open_files_limit: Some(ContainerRuntimeUlimit {
                    soft: 256,
                    hard: 256,
                }),
                privileged: Some(false),
                log_config: Some(ContainerRuntimeLogConfig {
                    driver: "none".to_owned(),
                    options: BTreeMap::new(),
                }),
                ..Default::default()
            },
        }
    }

    #[test]
    fn foreground_limits_are_explicit_offline_and_do_not_widen_background_or_old_plan_encoding()
    -> Result<()> {
        let baseline = SourceJobLimits::new(64 * MIB, 100)?;
        let old = OrdinaryLimits {
            memory_bytes: 64 * MIB,
            persistent_bytes: 16 * MIB,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&baseline)?,
            serde_json::to_value(&old)?
        );
        assert_eq!(
            elixir_marketplace::canonical::to_vec(&baseline)?,
            elixir_marketplace::canonical::to_vec(&old)?
        );
        for cpu in [100, 500, 1000] {
            let source = SourceJobLimits::new(64 * MIB, cpu)?;
            let mut actual = spec();
            prepare_source_worker(&mut actual, &source)?;
            assert_eq!(actual.security.memory_limit_mb, Some(64));
            assert_eq!(actual.security.memory_swap_limit_mb, Some(64));
            assert_eq!(
                actual.security.cpu_quota,
                Some(format!("{}.{:03}", cpu / 1000, cpu % 1000))
            );
            assert!(matches!(
                actual.security.log_options,
                Some(ContainerLogOptions::Disabled)
            ));
            if cpu > 100 {
                assert!(prepare_ephemeral_worker(&mut spec(), &source).is_err());
            }
        }
        for (memory, cpu) in [
            (63 * MIB, 100),
            (64 * MIB + 1, 100),
            (257 * MIB, 100),
            (64 * MIB, 0),
            (64 * MIB, 1001),
        ] {
            assert!(SourceJobLimits::new(memory, cpu).is_err());
        }
        let source = SourceJobLimits::new(64 * MIB, 500)?;
        let mut attached = spec();
        attached.network_mode = Some(format!("container:{}", "a".repeat(64)));
        assert!(prepare_source_worker(&mut attached, &source).is_err());
        assert_eq!(OrdinaryLimits::default().cpu_millicores, 100);
        assert!(
            OrdinaryLimits {
                cpu_millicores: 500,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn worker_profile_is_bounded_and_does_not_accept_ambient_host_authority() {
        let limits = OrdinaryLimits::default();
        let mut valid = spec();
        prepare_ephemeral_worker(&mut valid, &limits).unwrap();
        assert_eq!(valid.security.memory_swap_limit_mb, Some(256));
        assert_eq!(valid.security.cpu_quota.as_deref(), Some("0.100"));
        assert_eq!(valid.security.tmpfs[0].size_mb, Some(192));
        assert_eq!(valid.security.tmpfs[1].path, "/tmp");
        assert_eq!(valid.security.tmpfs[1].size_mb, Some(8));
        verify_inspect(&valid, &state(&valid), &limits).unwrap();
        for mode in ["host", "bridge", "container:mutable-name"] {
            let mut invalid = spec();
            invalid.network_mode = Some(mode.to_owned());
            assert!(prepare_ephemeral_worker(&mut invalid, &limits).is_err());
        }
        let mut tag = spec();
        tag.image = "node:latest".to_owned();
        assert!(prepare_ephemeral_worker(&mut tag, &limits).is_err());
        let mut caps = spec();
        caps.cap_add.push("NET_ADMIN".to_owned());
        assert!(prepare_ephemeral_worker(&mut caps, &limits).is_err());
        let mut env = spec();
        env.env.push(crate::runtime::model::EnvVar {
            name: "SECRET".to_owned(),
            value: "not-a-real-secret".to_owned(),
        });
        assert!(prepare_ephemeral_worker(&mut env, &limits).is_err());
    }

    #[test]
    fn worker_inspection_rejects_missing_or_drifted_actual_limits() {
        let limits = OrdinaryLimits::default();
        let mut spec = spec();
        prepare_ephemeral_worker(&mut spec, &limits).unwrap();
        for fault in [
            "swap",
            "logs",
            "privilege",
            "caps",
            "cpu",
            "memory",
            "core",
            "tmpfs",
            "namespace",
            "uid",
            "image",
            "image_id",
        ] {
            let mut actual = state(&spec);
            match fault {
                "swap" => actual.security.memory_swap_limit_bytes = Some(-1),
                "logs" => actual.security.log_config = None,
                "privilege" => actual.security.privileged = None,
                "caps" => actual.security.cap_add.push("SYS_ADMIN".to_owned()),
                "cpu" => actual.security.nano_cpus = Some(1_000_000_000),
                "memory" => actual.security.memory_limit_bytes = Some(1024 * MIB as i64),
                "core" => actual.security.core_dump_limit = None,
                "tmpfs" => actual.security.tmpfs[0].options = None,
                "namespace" => actual.network_mode = Some("host".to_owned()),
                "image" => actual.security.image_reference = None,
                "image_id" => actual.security.image_id = None,
                _ => actual.security.user = Some("0".to_owned()),
            }
            assert!(verify_inspect(&spec, &actual, &limits).is_err(), "{fault}");
        }
    }

    #[test]
    fn fixed_worker_inspection_binds_each_private_tmpfs_to_its_actual_frozen_owner() -> Result<()> {
        let profile = crate::marketplace::storage::FixedWorkerSpec::source_preflight_fixture()?;
        let spec = profile.container_spec();
        let mut actual = state(spec);
        actual.security.image_id = Some(spec.image.clone());
        actual.security.memory_limit_bytes = Some(profile.limits.memory_bytes as i64);
        actual.security.memory_swap_limit_bytes = Some(profile.limits.memory_bytes as i64);
        actual.security.nano_cpus = Some(i64::from(profile.limits.cpu_millicores) * 1_000_000);
        verify_inspect(spec, &actual, &profile.limits)?;
        let temporary = actual
            .security
            .tmpfs
            .iter()
            .position(|mount| mount.path == "/tmp")
            .unwrap();
        let original_temporary = actual.security.tmpfs[temporary].options.clone().unwrap();
        assert!(original_temporary.contains("size=32m"));
        for size in ["8m", "16m", "64m"] {
            actual.security.tmpfs[temporary].options =
                Some(original_temporary.replace("size=32m", &format!("size={size}")));
            assert!(verify_inspect(spec, &actual, &profile.limits).is_err());
        }
        actual.security.tmpfs[temporary].options = Some(original_temporary);
        verify_inspect(spec, &actual, &profile.limits)?;
        let index = actual
            .security
            .tmpfs
            .iter()
            .position(|mount| mount.path == "/radicle-source")
            .unwrap();
        let original = actual.security.tmpfs[index].options.clone().unwrap();
        assert!(original.contains("uid=10005,gid=10005,mode=700"));
        for options in [
            original.replace("uid=10005", "uid=10004"),
            original.replace("gid=10005", "gid=10004"),
            original.replace("mode=700", "mode=755"),
            original.replace("size=32m", "size=64m"),
            format!("{original},uid=0"),
            format!("{original},uid=10005"),
            format!("{original},gid=0"),
            format!("{original},size=64m"),
            format!("{original},exec"),
            original.replace("noexec,", ""),
            original.replace("nosuid,", ""),
            original.replace(",nodev", ""),
        ] {
            actual.security.tmpfs[index].options = Some(options.clone());
            assert!(
                verify_inspect(spec, &actual, &profile.limits).is_err(),
                "{options}"
            );
        }
        Ok(())
    }

    #[test]
    fn kernel_evidence_requires_real_caps_identity_and_cgroup_limits() {
        let report = "268435456\n0\n10000 100000\n64\nNoNewPrivs:\t1\nSeccomp:\t2\nCapEff:\t0000000000000000\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\nUid:\t10004\t10004\t10004\t10004\nGid:\t10004\t10004\t10004\t10004\nMax core file size 0 0 bytes\nMax open files 256 256 files\n";
        verify_kernel(report, &OrdinaryLimits::default()).unwrap();
        for invalid in [
            report.replacen("268435456", "max", 1),
            report.replace("Seccomp:\t2", "Seccomp:\t0"),
            report.replace("CapBnd:\t0000000000000000", "CapBnd:\t0000000000000001"),
            report.replace("0 0 bytes", "0 unlimited bytes"),
        ] {
            assert!(verify_kernel(&invalid, &OrdinaryLimits::default()).is_err());
        }
    }

    #[test]
    fn native_evidence_checks_the_guarded_child_not_the_namespace_init() {
        let report = "268435456\n0\n10000 100000\n64\nNoNewPrivs:\t1\nSeccomp:\t2\nCapEff:\t0000000000000000\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\nUid:\t10004\t10004\t10004\t10004\nGid:\t10004\t10004\t10004\t10004\nMax core file size 0 0 bytes\nMax open files 256 256 files\nPid:\t2\nPPid:\t1\nSeccomp_filters:\t4\nMax file size 1 1 bytes\n";
        let limits = SourceJobLimits::new(256 * MIB, 100).unwrap();
        verify_quarantine_kernel(report, &limits, 1, 4, 2).unwrap();
        assert!(verify_quarantine_kernel(report, &limits, 1, 4, 7).is_err());
        let dynamic = report.replace("\nPid:\t2\n", "\nPid:\t7\n");
        verify_quarantine_kernel(&dynamic, &limits, 1, 4, 7).unwrap();
        let foreground = SourceJobLimits::new(256 * MIB, 500).unwrap();
        let higher = report.replacen("10000 100000", "50000 100000", 1);
        verify_quarantine_kernel(&higher, &foreground, 1, 4, 2).unwrap();
        assert!(verify_quarantine_kernel(report, &foreground, 1, 4, 2).is_err());
        assert!(verify_quarantine_kernel(&higher, &limits, 1, 4, 2).is_err());
        for valid in ["2 ", "7", "12345\n"] {
            assert!(native_child_pid(valid).is_ok());
        }
        for invalid in ["", "1", "0", "-1", "02", "2 7", "4294967296", "2\n7"] {
            assert!(native_child_pid(invalid).is_err());
        }
        for invalid in [
            report.replace("\nPid:\t2\n", "\nPid:\t1\n"),
            report.replace("PPid:\t1", "PPid:\t2"),
            report.replace("Seccomp_filters:\t4", "Seccomp_filters:\t1"),
            report.replace("Max file size 1 1", "Max file size 2 2"),
        ] {
            assert!(verify_quarantine_kernel(&invalid, &limits, 1, 4, 2).is_err());
        }
    }

    #[tokio::test]
    #[ignore = "explicit local Docker isolation test; no public networking"]
    async fn live_docker_enforces_worker_isolation_and_bounded_tmpfs() -> Result<()> {
        use crate::runtime::docker::DockerRuntimeManager;
        struct Cleanup(String);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", &self.0])
                    .output();
            }
        }
        let runtime = DockerRuntimeManager::new(None);
        let mut spec = spec();
        let id = uuid::Uuid::new_v4().simple().to_string();
        spec.name = format!("elx-marketplace-isolation-{}", &id[..12]);
        for (key, value) in [
            ("elixir.instance_id", id.clone()),
            (
                "elixir.extension_id",
                format!("elixir.test.marketplace.{id}"),
            ),
            ("elixir.deployment_id", format!("test-{id}")),
            ("elixir.managed", "true".to_owned()),
        ] {
            spec.labels.insert(key.to_owned(), value);
        }
        let limits = OrdinaryLimits::default();
        prepare_ephemeral_worker(&mut spec, &limits)?;
        let handle = runtime.ensure_container(&spec).await?;
        let _cleanup = Cleanup(handle.id.clone());
        let evidence = inspect_isolated_worker(&runtime, &handle, &spec, &limits).await?;
        assert_eq!(evidence.container_name(), spec.name);
        let command = |items: &[&str]| {
            items
                .iter()
                .map(|item| (*item).to_owned())
                .collect::<Vec<_>>()
        };
        assert!(
            runtime
                .exec_container_command(
                    &handle,
                    "0",
                    &command(&["/bin/touch", "/marketplace-must-not-exist"])
                )
                .await
                .is_err(),
            "rootfs must remain read-only even for a trusted root probe"
        );
        let full = runtime
            .exec_container_command(
                &handle,
                "10004:10004",
                &command(&[
                    "/bin/dd",
                    "if=/dev/zero",
                    "of=/tmp/quota-probe",
                    "bs=1M",
                    "count=16",
                ]),
            )
            .await;
        assert!(
            full.is_err(),
            "bounded tmpfs must reject a 16 MiB write into 8 MiB"
        );
        runtime
            .exec_container_command(
                &handle,
                "10004:10004",
                &command(&["/bin/rm", "/tmp/quota-probe"]),
            )
            .await?;
        runtime
            .exec_container_command(
                &handle,
                "10004:10004",
                &command(&["/bin/cp", "/bin/true", "/tmp/noexec-probe"]),
            )
            .await?;
        assert!(
            runtime
                .exec_container_command(&handle, "10004:10004", &command(&["/tmp/noexec-probe"]))
                .await
                .is_err(),
            "temporary cache cannot become an executable code path"
        );
        let shared = runtime
            .exec_container_command(
                &handle,
                "10004:10004",
                &command(&[
                    "/bin/dd",
                    "if=/dev/zero",
                    "of=/dev/shm/quota-probe",
                    "bs=1M",
                    "count=2",
                ]),
            )
            .await;
        assert!(shared.is_err(), "shared memory must be bounded to 1 MiB");
        Ok(())
    }
}
