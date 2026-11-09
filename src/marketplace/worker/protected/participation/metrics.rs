//! Read-only measurements from the owned worker's Docker host and server OS.
//! Unknown/malformed/missing measurements prevent contribution, not a larger
//! quota or a guessed hardware capability. No remote addresses or account data.
use super::super::ProtectedWorker;
use crate::{
    db::Database,
    marketplace::{
        accounting::available_transfer,
        packet::{GUARD_CPU_MILLICORES, GUARD_MEMORY_BYTES},
        quarantine::{SourceQuarantineJob, SourceQuarantinePlan},
        runtime::inspect_fixed_worker,
        storage::FixedWorkerSpec,
    },
    playback::PlaybackJobManager,
    runtime::{RuntimeManager, model::ContainerHandle},
};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{packet_guard::CREDIT_MARGIN, policy::OrdinaryLimits};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, timeout},
};
mod cpu;
#[cfg(any(target_os = "macos", test))]
mod macos;
use crate::marketplace::host_observation as probe;
mod independent;
pub(in crate::marketplace) use independent::IndependentAdmission;

#[derive(Clone, Debug)]
pub(in crate::marketplace::worker::protected) struct Facts {
    pub observed: Instant,
    pub available_memory: Option<u64>,
    pub disk_headroom: bool,
    /// Optional catalog/routing work yields at the stricter idle thresholds.
    pub resource_pressure: bool,
    /// Owner-requested work uses the existing foreground memory/I/O thresholds,
    /// while retaining the same actual CPU-capacity and server-load vetoes.
    pub foreground_resource_pressure: bool,
    pub playback_pressure: bool,
    pub background_allowance: bool,
}

pub(super) struct Source {
    runtime: Arc<dyn RuntimeManager>,
    handle: ContainerHandle,
    profile: FixedWorkerSpec,
    database: Database,
    playback: Arc<PlaybackJobManager>,
    limits: OrdinaryLimits,
    source_job_directory: PathBuf,
    #[cfg(test)]
    defer_once: std::sync::atomic::AtomicBool,
}
pub(super) struct Sampler {
    pub limits: OrdinaryLimits,
    task: JoinHandle<()>,
}
#[derive(Clone, Copy, Debug)]
enum SourceStage {
    BeforeProvision,
    AfterProvision,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(in crate::marketplace) enum SourceAdmissionDeferred {
    #[error(
        "foreground source job is waiting for known host headroom and playback startup/seek readiness (available/required memory={memory}/{memory_needed}, disk={disk}/{disk_needed}, resource_pressure={pressure}, playback={playback})"
    )]
    Measured {
        memory: u64,
        memory_needed: u64,
        disk: u64,
        disk_needed: u64,
        pressure: bool,
        playback: bool,
    },
    #[error(
        "installed source is waiting before Docker-host sampling (server memory={memory}/{memory_needed}, server_pressure={pressure}, playback={playback}; Docker-host state not yet measured)"
    )]
    Bootstrap {
        memory: u64,
        memory_needed: u64,
        pressure: bool,
        playback: bool,
    },
}
#[cfg(test)]
impl SourceAdmissionDeferred {
    // Scheduling diagnostic only, not a resource/authority capability.
    pub(in crate::marketplace) fn fixture() -> Self {
        Self::Measured {
            memory: 1,
            memory_needed: 2,
            disk: 3,
            disk_needed: 4,
            pressure: false,
            playback: false,
        }
    }
}
impl Drop for Sampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}
#[cfg(test)]
impl Sampler {
    pub(super) fn fixture(limits: OrdinaryLimits, task: JoinHandle<()>) -> Self {
        Self { limits, task }
    }
}

impl Source {
    /// Negative scheduling fault only. Acceptance still requires the actual
    /// native host/OS checks; this cannot manufacture positive admission.
    #[cfg(test)]
    pub(super) fn fixture_defer_once(&self) {
        self.defer_once
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    #[cfg(unix)]
    pub(super) fn installed_configuration(
        &self,
        source_budget: crate::marketplace::quarantine::SourceJobBudget,
        retention: crate::marketplace::archive::ArchiveBudget,
    ) -> Result<crate::marketplace::grant_store::installation::runtime::InstalledRuntime> {
        let spec = self.profile.container_spec();
        Ok(
            crate::marketplace::grant_store::installation::runtime::InstalledRuntime {
                runtime: self.runtime.clone(),
                playback: self.playback.clone(),
                local_http: None,
                application_http: None,
                compiled_toolchains: None,
                lane: self.source_job_directory.clone(),
                archive: self
                    .source_job_directory
                    .parent()
                    .context("source archive parent missing")?
                    .join("source-archives"),
                deployment: format!("installed-{}", uuid::Uuid::new_v4().simple()),
                image: spec.image.clone(),
                source_budget,
                retention,
            },
        )
    }
    /// Actual offline native worker/OS measurements, synthetic protection only.
    /// No production constructor accepts an OfflineWorker as protected authority.
    #[cfg(all(test, unix))]
    pub(super) fn fixture_from_offline(
        runtime: Arc<dyn RuntimeManager>,
        worker: &crate::marketplace::worker::OfflineWorker,
        database: Database,
        source_job_directory: PathBuf,
    ) -> Result<Arc<Self>> {
        let mut limits = worker.profile().limits.clone();
        limits.memory_bytes = limits
            .memory_bytes
            .checked_add(GUARD_MEMORY_BYTES)
            .context("fixture combined memory budget")?;
        limits.cpu_millicores = limits
            .cpu_millicores
            .checked_add(GUARD_CPU_MILLICORES)
            .context("fixture combined CPU budget")?;
        limits.validate()?;
        Ok(Arc::new(Self {
            runtime,
            handle: worker.handle().clone(),
            profile: worker.profile().clone(),
            playback: Arc::new(PlaybackJobManager::new(database.pool.clone(), None)),
            database,
            limits,
            source_job_directory,
            defer_once: Default::default(),
        }))
    }
    pub fn from_worker(
        worker: &ProtectedWorker,
        database: Database,
        playback: Arc<PlaybackJobManager>,
    ) -> Result<Self> {
        let mut limits = worker.worker.profile.limits.clone();
        limits.memory_bytes = limits
            .memory_bytes
            .checked_add(GUARD_MEMORY_BYTES)
            .context("invalid combined memory budget")?;
        limits.cpu_millicores = limits
            .cpu_millicores
            .checked_add(GUARD_CPU_MILLICORES)
            .context("invalid combined CPU budget")?;
        limits.validate()?;
        Ok(Self {
            runtime: worker.runtime.clone(),
            handle: worker.worker.handle.clone(),
            profile: worker.worker.profile.clone(),
            database,
            playback,
            limits,
            source_job_directory: worker.source_job_directory.clone(),
            #[cfg(test)]
            defer_once: Default::default(),
        })
    }
    pub fn spawn(self: Arc<Self>) -> (watch::Receiver<Option<Facts>>, Sampler) {
        let limits = self.limits.clone();
        let (sender, receiver) = watch::channel(None);
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(super::OBSERVATION_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if sender.is_closed() {
                    break;
                }
                let facts = timeout(Duration::from_secs(8), self.collect())
                    .await
                    .ok()
                    .and_then(Result::ok);
                sender.send_replace(facts);
            }
        });
        (receiver, Sampler { limits, task })
    }
    /// Real inspected Docker-host + server-OS admission under the global source
    /// lane. No peer-provided measurements, packet credit, account telemetry,
    /// optimistic queue or change to the background worker's resource caps.
    #[cfg(test)]
    pub(super) async fn open_source_job(
        self: &Arc<Self>,
        plan: &SourceQuarantinePlan,
    ) -> Result<SourceQuarantineJob> {
        let lease =
            crate::marketplace::lease::ControllerLease::acquire(&self.source_job_directory)?;
        let spec = self.profile.container_spec();
        let deployment = spec
            .labels
            .get(crate::runtime::model::ELIXIR_DEPLOYMENT_ID_LABEL)
            .context("foreground source deployment is unavailable")?;
        SourceQuarantineJob::open_leased(
            self.runtime.clone(),
            &self.source_job_directory,
            deployment,
            &spec.image,
            plan,
            lease,
            self.clone(),
        )
        .await
    }
    pub(super) async fn open_source_job_retained(
        self: &Arc<Self>,
        slot: &mut Option<SourceQuarantineJob>,
        plan: &SourceQuarantinePlan,
    ) -> Result<()> {
        if let Some(job) = slot {
            let admission: Arc<dyn crate::marketplace::quarantine::ResourceAdmission> =
                self.clone();
            return job
                .resume_opening_retained(self.runtime.as_ref(), &admission, plan)
                .await;
        }
        let lease =
            crate::marketplace::lease::ControllerLease::acquire(&self.source_job_directory)?;
        let spec = self.profile.container_spec();
        let deployment = spec
            .labels
            .get(crate::runtime::model::ELIXIR_DEPLOYMENT_ID_LABEL)
            .context("foreground source deployment is unavailable")?;
        SourceQuarantineJob::open_leased_retained(
            slot,
            self.runtime.clone(),
            deployment,
            &spec.image,
            plan,
            lease,
            self.clone(),
        )
        .await
    }
    #[cfg(unix)]
    pub(super) async fn open_activation_job(
        self: &Arc<Self>,
        slot: &mut Option<SourceQuarantineJob>,
        start: &crate::marketplace::grant_store::installation::activation::ActivationStart,
        request: &crate::marketplace::grant_store::installation::activation::SourceOpen<'_>,
    ) -> Result<()> {
        use crate::marketplace::grant_store::installation::activation::SourceOpen;
        let SourceOpen {
            metadata,
            public,
            archive,
            plan,
            budget,
        } = *request;
        let lease =
            crate::marketplace::lease::ControllerLease::acquire(&self.source_job_directory)?;
        let spec = self.profile.container_spec();
        if plan.image() != spec.image {
            bail!("activation image differs from the actual protected source owner");
        }
        crate::marketplace::grant_store::installation::activation::require_native_start(
            &self.database,
            metadata,
            public,
            archive,
            &start.native_check(),
            plan,
            budget,
        )
        .await?;
        // Same global lane and real per-process resource admission as ordinary
        // source preparation, but the claim supplies this attempt's namespace.
        SourceQuarantineJob::open_leased_retained(
            slot,
            self.runtime.clone(),
            &start.deployment(),
            plan.image(),
            plan.source(),
            lease,
            self.clone(),
        )
        .await
    }
    async fn admit_source_resources(
        &self,
        plan: &SourceQuarantinePlan,
        stage: SourceStage,
    ) -> Result<()> {
        let operation = async {
            inspect_fixed_worker(self.runtime.as_ref(), &self.handle, &self.profile).await?;
            let (host, report) = tokio::join!(
                host_available(),
                resource_sample(self.runtime.as_ref(), &self.handle, probe::Disk::State),
            );
            let report = report?;
            let sample = probe::Snapshot::parse(&report).map_err(anyhow::Error::msg)?;
            let kernel = sample.kernel;
            let before = cpu::Sample::parse(sample.before).map_err(anyhow::Error::msg)?;
            let after = cpu::Sample::parse(sample.after).map_err(anyhow::Error::msg)?;
            let cpu_busy = after.pressured_since(before).map_err(anyhow::Error::msg)?;
            let memory_io_busy = foreground_memory_io_pressure(kernel)?;
            let host_cpu_busy = host_cpu_pressure();
            check_source_headroom(
                &self.limits,
                plan,
                stage,
                host?.min(linux_available(kernel)?),
                disk_available(sample.disk)?,
                memory_io_busy || cpu_busy || host_cpu_busy,
                self.playback.startup_queue_len() > 0,
            )
            .with_context(|| {
                let psi = kernel.lines().filter(|line| line.starts_with("some "))
                    .collect::<Vec<_>>().join(" | ");
                format!("source admission {stage:?}: guest_cpu_busy={cpu_busy}, memory_io_busy={memory_io_busy}, host_cpu_busy={host_cpu_busy}, cpu_before={before:?}, cpu_after={after:?}, PSI=[{psi}]")
            })
        };
        timeout(Duration::from_secs(8), operation)
            .await
            .context("foreground source resource admission deadline")?
    }
    async fn collect(&self) -> Result<Facts> {
        let observed = Instant::now();
        // Desired labels alone do not establish the namespace/cgroup/profile.
        inspect_fixed_worker(self.runtime.as_ref(), &self.handle, &self.profile).await?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let (report, host, allowance) = tokio::join!(
            resource_sample(self.runtime.as_ref(), &self.handle, probe::Disk::State),
            host_available(),
            available_transfer(&self.database, now, &self.limits),
        );
        let report = report?;
        let sample = probe::Snapshot::parse(&report).map_err(anyhow::Error::msg)?;
        let before = cpu::Sample::parse(sample.before).map_err(anyhow::Error::msg)?;
        let after = cpu::Sample::parse(sample.after).map_err(anyhow::Error::msg)?;
        let cpu_busy = after.pressured_since(before).map_err(anyhow::Error::msg)?;
        let guest_memory = linux_available(sample.kernel)?;
        let (resource_pressure, foreground_resource_pressure) =
            resource_pressures(sample.kernel, cpu_busy, host_cpu_pressure())?;
        let free_disk = disk_available(sample.disk)?;
        let available_memory = host.ok().map(|host| host.min(guest_memory));
        // The combined actor/worker allocation is already reserved. Keep an
        // additional whole ordinary memory/disk budget free as headroom.
        Ok(Facts {
            observed,
            available_memory,
            disk_headroom: free_disk >= self.limits.persistent_bytes,
            resource_pressure,
            foreground_resource_pressure,
            playback_pressure: self.playback.has_latency_sensitive_work(),
            // A scheduling hint only. At exhaustion, background contribution
            // pauses while any already prepaid remainder can serve the owner.
            background_allowance: allowance.is_ok_and(|available| available >= 2 * CREDIT_MARGIN),
        })
    }
}

#[async_trait::async_trait]
impl crate::marketplace::quarantine::ResourceAdmission for Source {
    async fn check(
        &self,
        plan: &SourceQuarantinePlan,
        provisioned: bool,
        measurement: Option<&crate::marketplace::quarantine::HostObservation>,
    ) -> Result<()> {
        if measurement.is_some() {
            bail!("catalog admission cannot substitute a different measurement owner");
        }
        #[cfg(test)]
        if self
            .defer_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(SourceAdmissionDeferred::fixture().into());
        }
        let stage = if provisioned {
            SourceStage::AfterProvision
        } else {
            SourceStage::BeforeProvision
        };
        self.admit_source_resources(plan, stage).await
    }
}

fn check_source_headroom(
    ordinary: &OrdinaryLimits,
    plan: &SourceQuarantinePlan,
    stage: SourceStage,
    memory: u64,
    disk: u64,
    pressure: bool,
    playback: bool,
) -> Result<()> {
    // Background ceilings are not prepaid physical RAM/disk. Conservatively
    // reserve their whole allocation for growth AND keep a second allocation
    // free for the media server, in addition to this explicit foreground job.
    // This is admission, not proof of aggregate playback latency or a promise
    // that host demand cannot change after these bounded read-only samples.
    let memory_needed = ordinary
        .memory_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(plan.memory_reservation_bytes()))
        .context("foreground source memory bounds")?;
    let pending_disk = match stage {
        SourceStage::BeforeProvision => plan.disk_allocation_bytes(),
        SourceStage::AfterProvision => plan.remaining_disk_reservation_bytes(),
    };
    let disk_needed = ordinary
        .persistent_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(pending_disk))
        .context("foreground source disk bounds")?;
    if memory < memory_needed || disk < disk_needed || pressure || playback {
        return Err(SourceAdmissionDeferred::Measured {
            memory,
            memory_needed,
            disk,
            disk_needed,
            pressure,
            playback,
        }
        .into());
    }
    Ok(())
}

async fn fixed_read(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    argv: &[&str],
) -> Result<String> {
    let args: Vec<String> = argv.iter().map(|x| (*x).to_owned()).collect();
    let value = runtime
        .exec_container_command(handle, "10004:10004", &args)
        .await?;
    if value.len() > 32 * 1024 || value.as_bytes().contains(&0) {
        bail!("host measurement bounds");
    }
    Ok(value)
}

async fn resource_sample(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    disk: probe::Disk,
) -> Result<String> {
    let command = probe::command(disk);
    let argv: Vec<_> = command.iter().map(String::as_str).collect();
    fixed_read(runtime, handle, &argv).await
}

/// Fresh scheduling admission, not VPN/worker authority. Uses the actual
/// inspected guard's host and the server OS before any bootstrap debit or
/// larger worker is created. Unknown pressure/headroom refuses startup. This
/// consumer also serves foreground requests; steady playback alone is not a
/// veto. Optional contribution still has its separate any-playback gate.
pub(super) async fn admit_startup_resources(
    runtime: &dyn RuntimeManager,
    guard: &mut crate::marketplace::packet::PacketGuardController,
    limits: &OrdinaryLimits,
    playback: &PlaybackJobManager,
) -> Result<()> {
    limits.validate()?;
    let operation = async {
        let (host, report) = tokio::join!(host_available(), guard.observe_host_resources(runtime));
        let report = report?;
        let sample = probe::Snapshot::parse(&report).map_err(anyhow::Error::msg)?;
        let kernel = sample.kernel;
        let before = cpu::Sample::parse(sample.before).map_err(anyhow::Error::msg)?;
        let after = cpu::Sample::parse(sample.after).map_err(anyhow::Error::msg)?;
        let cpu_busy = after.pressured_since(before).map_err(anyhow::Error::msg)?;
        let memory = host?.min(linux_available(kernel)?);
        let disk = disk_available(sample.disk)?;
        check_startup_headroom(
            limits,
            memory,
            disk,
            foreground_memory_io_pressure(kernel)? || cpu_busy || host_cpu_pressure(),
            playback.startup_queue_len() > 0,
        )
    };
    timeout(Duration::from_secs(8), operation)
        .await
        .context("startup resource admission deadline")?
}
pub(super) async fn await_startup_host(
    limits: &OrdinaryLimits,
    playback: &PlaybackJobManager,
) -> Result<()> {
    limits.validate()?;
    loop {
        if let Ok(available) = host_available().await
            && available
                >= limits
                    .memory_bytes
                    .checked_mul(2)
                    .context("startup memory bounds")?
            && !host_cpu_pressure()
            && playback.startup_queue_len() == 0
        {
            return Ok(());
        }
        // The application startup owner supplies the absolute deadline and
        // cancellation. No guard, packet credit or worker is held while busy.
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
fn check_startup_headroom(
    limits: &OrdinaryLimits,
    memory: u64,
    disk: u64,
    pressure: bool,
    playback: bool,
) -> Result<()> {
    if memory
        < limits
            .memory_bytes
            .checked_mul(2)
            .context("startup memory bounds")?
        || disk
            < limits
                .persistent_bytes
                .checked_mul(2)
                .context("startup disk bounds")?
        || pressure
        || playback
    {
        bail!("marketplace startup is waiting for current host headroom and idle playback");
    }
    Ok(())
}

fn linux_available(input: &str) -> Result<u64> {
    let mut found = None;
    for line in input.lines() {
        if let Some(value) = line.strip_prefix("MemAvailable:") {
            if found.is_some() {
                bail!("duplicate available memory");
            }
            let parts: Vec<_> = value.split_whitespace().collect();
            if parts.len() != 2 || parts[1] != "kB" {
                bail!("unknown memory units");
            }
            found = Some(
                parts[0]
                    .parse::<u64>()?
                    .checked_mul(1024)
                    .context("memory overflow")?,
            );
        }
    }
    found.context("available memory is unknown")
}
fn disk_available(input: &str) -> Result<u64> {
    let parts: Vec<_> = input.split_whitespace().collect();
    if parts.len() != 2 {
        bail!("unknown disk measurement");
    }
    let blocks = parts[0].parse::<u64>()?;
    let size = parts[1].parse::<u64>()?;
    if size == 0 || size > 1024 * 1024 {
        bail!("invalid disk block size");
    }
    blocks
        .checked_mul(size)
        .context("disk measurement overflow")
}
/// Validated ordered CPU/memory/I/O `some avg10` percentages. The actual
/// native samplers read those three fixed proc files in that order. PSI is a
/// share of stalled time, not memory usage or proof that all work is blocked.
/// Keep parsing identical for foreground and optional background scheduling.
fn pressure_averages(input: &str) -> Result<[f64; 3]> {
    let mut averages = [0.0; 3];
    let mut count = 0;
    for line in input.lines().filter(|line| line.starts_with("some ")) {
        if count == averages.len() {
            bail!("extra pressure measurement");
        }
        let values: Vec<_> = line
            .split_whitespace()
            .filter_map(|value| value.strip_prefix("avg10="))
            .collect();
        if values.len() != 1 {
            bail!("unknown pressure measurement");
        }
        let value = values[0].parse::<f64>()?;
        if !value.is_finite() || !(0.0..=100.0).contains(&value) {
            bail!("invalid pressure measurement");
        }
        averages[count] = value;
        count += 1;
    }
    if count != averages.len() {
        bail!("missing pressure measurements");
    }
    // CPU PSI counts waiting, including our deliberately quota-throttled jobs;
    // it is not host capacity. Validate its closed shape, but admit CPU from
    // advancing aggregate /proc/stat samples and the independent server load.
    Ok(averages)
}
fn memory_io_pressure(input: &str) -> Result<bool> {
    let [_, memory, io] = pressure_averages(input)?;
    // Optional contribution yields early. Never borrow the foreground policy
    // merely to keep a relay/catalog active during contention.
    Ok(memory >= 1.0 || io >= 10.0)
}
fn foreground_memory_io_pressure(input: &str) -> Result<bool> {
    let [_, memory, io] = pressure_averages(input)?;
    // Foreground browsing/extension calls are useful application work, not
    // idle-only contribution. A cold container or another process briefly
    // waiting on disk must not automatically kill every bounded HTTP request.
    // Still defer at substantial recent contention; physical reservations,
    // actual CPU capacity, native limits and playback startup remain separate.
    // These provisional thresholds require aggregate playback qualification.
    Ok(memory >= 10.0 || io >= 50.0)
}
fn resource_pressures(input: &str, guest_cpu: bool, server_cpu: bool) -> Result<(bool, bool)> {
    // CPU capacity is common, but optional background pressure is not an
    // admission veto for bounded foreground work. Derive both from the SAME
    // inspected sample; no extra process, reservation or optimistic fallback.
    let background = memory_io_pressure(input)?;
    let foreground = foreground_memory_io_pressure(input)?;
    let cpu = guest_cpu || server_cpu;
    Ok((background || cpu, foreground || cpu))
}
fn host_cpu_pressure() -> bool {
    #[cfg(unix)]
    {
        let mut load = [0.0f64; 1];
        // Fixed one-element writable array; this is a read-only OS query.
        let measured = unsafe { libc::getloadavg(load.as_mut_ptr(), 1) };
        let Some(cpus) = std::thread::available_parallelism().ok().map(usize::from) else {
            return true;
        };
        measured != 1 || !load[0].is_finite() || load[0] < 0.0 || load[0] / cpus as f64 >= 0.8
    }
    #[cfg(not(unix))]
    {
        true
    }
}
async fn host_available() -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        use tokio::io::AsyncReadExt;
        let file = tokio::fs::File::open("/proc/meminfo").await?;
        let mut bytes = Vec::new();
        file.take(32 * 1024 + 1).read_to_end(&mut bytes).await?;
        if bytes.len() > 32 * 1024 {
            bail!("host memory measurement bounds");
        }
        linux_available(std::str::from_utf8(&bytes)?)
    }
    #[cfg(target_os = "macos")]
    {
        use std::process::Stdio;
        let output = timeout(
            Duration::from_secs(2),
            tokio::process::Command::new("/usr/bin/vm_stat")
                .env_clear()
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() || output.stdout.len() > 32 * 1024 {
            bail!("host memory measurement unavailable");
        }
        macos::available(std::str::from_utf8(&output.stdout)?, macos::pressure()?)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        bail!("host memory measurement unsupported")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    async fn idle_source_fixture(source: &Source, plan: &SourceQuarantinePlan) -> Result<()> {
        timeout(Duration::from_secs(120), async {
            loop {
                match source
                    .admit_source_resources(plan, SourceStage::BeforeProvision)
                    .await
                {
                    Ok(()) => break Ok(()),
                    Err(error) if error.is::<SourceAdmissionDeferred>() => {
                        eprintln!("waiting for actual idle admission: {error:#}");
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                    Err(error) => break Err(error),
                }
            }
        })
        .await
        .context("native admission fixture did not reach measured idle resources")?
    }
    #[test]
    fn foreground_admission_reserves_the_job_and_background_growth_without_raising_limits()
    -> Result<()> {
        use crate::marketplace::quarantine::{
            SourceJobBudget,
            tests::{budget, fixture},
        };
        let (release, _, _) = fixture()?;
        let ordinary = OrdinaryLimits::default();
        let job = budget();
        let original = elixir_marketplace::canonical::to_vec(&ordinary)?;
        let plan = SourceQuarantinePlan::for_release(&release, None, &job)?;
        let memory = ordinary.memory_bytes * 2 + plan.memory_reservation_bytes();
        let disk = ordinary.persistent_bytes * 2 + plan.disk_reservation_bytes();
        let before = SourceStage::BeforeProvision;
        check_source_headroom(&ordinary, &plan, before, memory, disk, false, false)?;
        assert!(
            check_source_headroom(&ordinary, &plan, before, memory - 1, disk, false, false)
                .is_err()
        );
        assert!(
            check_source_headroom(&ordinary, &plan, before, memory, disk - 1, false, false)
                .is_err()
        );
        assert!(
            check_source_headroom(&ordinary, &plan, before, memory, disk, true, false).is_err()
        );
        assert!(
            check_source_headroom(&ordinary, &plan, before, memory, disk, false, true).is_err()
        );
        let larger = SourceQuarantinePlan::for_release(
            &release,
            None,
            &SourceJobBudget {
                memory_bytes: job.memory_bytes + 64 * 1024 * 1024,
                ..job
            },
        )?;
        assert!(
            check_source_headroom(&ordinary, &larger, before, memory, disk, false, false).is_err()
        );
        let allocated = plan.workspace().package.size + plan.workspace().proof.size;
        let after_disk = disk - allocated;
        assert!(
            check_source_headroom(&ordinary, &plan, before, memory, after_disk, false, false)
                .is_err()
        );
        check_source_headroom(
            &ordinary,
            &plan,
            SourceStage::AfterProvision,
            memory,
            after_disk,
            false,
            false,
        )?;
        assert!(
            check_source_headroom(
                &ordinary,
                &plan,
                SourceStage::AfterProvision,
                memory,
                after_disk - 1,
                false,
                false
            )
            .is_err()
        );
        assert_eq!(elixir_marketplace::canonical::to_vec(&ordinary)?, original);
        Ok(())
    }
    #[test]
    fn startup_admission_keeps_one_whole_budget_free_and_requires_idle_known_resources() {
        let limits = OrdinaryLimits::default();
        let memory = limits.memory_bytes * 2;
        let disk = limits.persistent_bytes * 2;
        check_startup_headroom(&limits, memory, disk, false, false).unwrap();
        assert!(check_startup_headroom(&limits, memory - 1, disk, false, false).is_err());
        assert!(check_startup_headroom(&limits, memory, disk - 1, false, false).is_err());
        assert!(check_startup_headroom(&limits, memory, disk, true, false).is_err());
        assert!(check_startup_headroom(&limits, memory, disk, false, true).is_err());
    }
    #[test]
    fn memory_disk_and_pressure_measurements_are_closed_bounded_and_conservative() {
        let kernel = "MemAvailable: 1048576 kB\nsome avg10=0.20 avg60=0.10\nfull avg10=0.0\nsome avg10=0.00\nsome avg10=0.00\n";
        assert_eq!(linux_available(kernel).unwrap(), 1024 * 1024 * 1024);
        assert!(!memory_io_pressure(kernel).unwrap());
        assert!(!memory_io_pressure(&kernel.replacen("avg10=0.20", "avg10=99.00", 1)).unwrap());
        assert!(memory_io_pressure("some avg10=0\nsome avg10=1\nsome avg10=0").unwrap());
        assert!(memory_io_pressure("some avg10=0\nsome avg10=0\nsome avg10=10").unwrap());
        assert!(memory_io_pressure(&kernel.replace("avg10=0.20", "avg10=NaN")).is_err());
        assert!(memory_io_pressure("some avg10=0\nsome avg10=0").is_err());
        assert!(linux_available("MemAvailable: 10 MB").is_err());
        assert!(linux_available("MemAvailable: 1 kB\nMemAvailable: 2 kB").is_err());
        assert_eq!(disk_available("131072 4096\n").unwrap(), 512 * 1024 * 1024);
        assert!(disk_available("18446744073709551615 4096").is_err());
        assert!(disk_available("100 0").is_err());
    }

    #[test]
    fn foreground_pressure_is_not_the_optional_background_idle_policy() -> Result<()> {
        let report = |memory: &str, io: &str| {
            format!("some avg10=99\nsome avg10={memory}\nsome avg10={io}\n")
        };
        for sample in [report("1", "0"), report("0", "10"), report("9.99", "49.99")] {
            assert!(memory_io_pressure(&sample)?);
            assert!(!foreground_memory_io_pressure(&sample)?);
            assert_eq!(resource_pressures(&sample, false, false)?, (true, false));
        }
        for sample in [report("10", "0"), report("0", "50")] {
            assert!(foreground_memory_io_pressure(&sample)?);
            assert!(memory_io_pressure(&sample)?);
            assert_eq!(resource_pressures(&sample, false, false)?, (true, true));
        }
        let idle = report("0", "0");
        assert_eq!(resource_pressures(&idle, false, false)?, (false, false));
        assert_eq!(resource_pressures(&idle, true, false)?, (true, true));
        assert_eq!(resource_pressures(&idle, false, true)?, (true, true));
        for sample in [
            report("NaN", "0"),
            report("inf", "0"),
            report("-1", "0"),
            report("0", "100.01"),
            "some avg10=0\nsome avg10=0\n".into(),
            format!("{}some avg10=0\n", report("0", "0")),
            "some avg10=0\nsome avg10=0 avg10=0\nsome avg10=0\n".into(),
        ] {
            assert!(foreground_memory_io_pressure(&sample).is_err());
            assert!(memory_io_pressure(&sample).is_err());
            assert!(resource_pressures(&sample, false, false).is_err());
            assert!(resource_pressures(&sample, true, true).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "explicit local Docker read-only host measurement integration"]
    async fn owned_worker_host_measurements_preserve_durable_transfer_accounting() -> Result<()> {
        use crate::{
            config::DatabaseConfig,
            marketplace::{accounting::reserve_transfer, worker::OfflineWorker},
            runtime::{
                docker::DockerRuntimeManager,
                model::{ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL},
            },
        };
        use elixir_marketplace::identity::PublicKey;
        use futures_util::FutureExt;
        use sqlx::Row;
        use std::collections::HashMap;
        use tokio::process::Command;

        let selected = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
            .context("explicit local worker image is required")?;
        let output = timeout(
            Duration::from_secs(20),
            Command::new("docker")
                .args(["image", "inspect", "--format", "{{.Id}}", &selected])
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        anyhow::ensure!(output.status.success(), "local fixture image unavailable");
        let image = String::from_utf8(output.stdout)?.trim().to_owned();
        anyhow::ensure!(
            crate::marketplace::runtime::image_is_pinned(&image),
            "fixture must use an immutable image"
        );
        let deployment = format!("fixture-metrics-{}", uuid::Uuid::new_v4().simple());
        let name = format!("elixir-marketplace-worker-{deployment}");
        let volume = format!("elixir-marketplace-{deployment}");
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
            (
                "elixir.marketplace.volume".to_owned(),
                "fixed-v1".to_owned(),
            ),
        ]);
        let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
        let controller_root = tempfile::tempdir()?;
        let controller = controller_root.path().canonicalize()?.join("controller");
        let database_root = tempfile::tempdir()?;
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite://{}",
                database_root.path().join("metrics.sqlite").display()
            ),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        sqlx::raw_sql(include_str!(
            "../../../../../migrations/0060_marketplace_transfer_accounting.sql"
        ))
        .execute(&database.pool)
        .await?;
        let limits = OrdinaryLimits::default();
        let worker_limits = OrdinaryLimits {
            memory_bytes: limits.memory_bytes - GUARD_MEMORY_BYTES,
            cpu_millicores: limits.cpu_millicores - GUARD_CPU_MILLICORES,
            ..limits.clone()
        };
        // Public fixture key only. No account, VPN, or production issuer secret.
        let issuer = PublicKey::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[3; 32])
                .verifying_key()
                .to_bytes(),
        )?;
        let result = std::panic::AssertUnwindSafe(async {
            let worker = OfflineWorker::start(
                runtime.as_ref(), &controller, &deployment, &image, &[issuer], &worker_limits,
            ).await?;
            assert_eq!(worker.profile().container_spec().network_mode.as_deref(), Some("none"));
            // Exercise actual OS/Docker reads, NOT protected-network admission.
            // Production can construct this source only from ProtectedWorker.
            let source = Arc::new(Source {
                runtime: runtime.clone(),
                handle: worker.handle().clone(),
                profile: worker.profile().clone(),
                database: database.clone(),
                playback: Arc::new(PlaybackJobManager::new(database.pool.clone(), None)),
                limits: limits.clone(),
                source_job_directory: controller.join("foreground-source"),
                defer_once: Default::default(),
            });
            let facts = timeout(Duration::from_secs(8), source.collect()).await??;
            assert!(facts.observed.elapsed() < Duration::from_secs(8));
            assert!(facts.available_memory.is_some_and(|bytes| bytes > 0));
            assert!(facts.background_allowance);
            assert!(!facts.playback_pressure);
            let row = sqlx::query("SELECT revision,state_json FROM marketplace_transfer_accounting WHERE singleton=1")
                .fetch_one(&database.pool).await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 0);
            assert_eq!(row.try_get::<String, _>(1)?, "");
            // Real OS/engine resource admission + offline native workspace,
            // NOT a protected consumer, packet credit or install capability.
            let (release, _, _) = crate::marketplace::quarantine::tests::fixture()?;
            let plan = SourceQuarantinePlan::for_release(&release, None,
                &crate::marketplace::quarantine::tests::budget())?;
            let held = crate::marketplace::lease::ControllerLease::acquire(&source.source_job_directory)?;
            assert!(source.open_source_job(&plan).await.is_err(), "no second source job or waiting queue");
            drop(held);
            // Creating the catalog physically writes its two fixed banks. Do
            // not assume the Docker host is idle immediately after cold start.
            // This is a bounded TEST prerequisite, never a production queue:
            // wait only for a measured deferral, retaining the original limits.
            // Unknown/malformed/profile/transport errors still fail immediately.
            eprintln!("cold-start host measurements: {facts:?}");
            idle_source_fixture(&source, &plan).await?;
            let job = source.open_source_job(&plan).await?;
            assert!(crate::marketplace::lease::ControllerLease::acquire(&source.source_job_directory).is_err());
            assert!(source.open_source_job(&plan).await.is_err());
            job.discard_owned().await?;
            idle_source_fixture(&source, &plan).await?;
            let job = source.open_source_job(&plan).await?;
            job.discard_owned().await?;
            let row = sqlx::query("SELECT revision,state_json FROM marketplace_transfer_accounting WHERE singleton=1")
                .fetch_one(&database.pool).await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 0, "offline admission cannot mint packet credit");
            assert_eq!(row.try_get::<String, _>(1)?, "");
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let debit = reserve_transfer(&database, now, limits.monthly_transfer_bytes, &limits).await?;
            assert_eq!(debit.remaining_after(), 0);
            let before: String = sqlx::query_scalar("SELECT state_json FROM marketplace_transfer_accounting WHERE singleton=1")
                .fetch_one(&database.pool).await?;
            let exhausted = timeout(Duration::from_secs(8), source.collect()).await??;
            assert!(!exhausted.background_allowance);
            let after: String = sqlx::query_scalar("SELECT state_json FROM marketplace_transfer_accounting WHERE singleton=1")
                .fetch_one(&database.pool).await?;
            assert_eq!(before, after, "measurement cannot roll or refund a debit");
            eprintln!("read-only host measurement/native source admission fixture: image={image}, facts={facts:?}, exhausted={exhausted:?}; excludes VPN/protection authority and aggregate playback qualification");
            worker.stop(runtime.as_ref()).await?;
            Ok::<_, anyhow::Error>(())
        }).catch_unwind().await;
        let cleanup: Result<()> = async {
            if let Some(handle) = runtime.get_container_handle(&name).await? {
                let state = runtime
                    .describe_container_runtime_state(&name)
                    .await?
                    .context("fixture cleanup inspection unavailable")?;
                anyhow::ensure!(
                    state.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) == Some(&deployment),
                    "fixture cleanup ownership mismatch"
                );
                runtime.remove_container(&handle).await?;
            }
            runtime.remove_private_file_volume(&volume, &labels).await?;
            let source_labels = HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
                (
                    "elixir.marketplace.volume".to_owned(),
                    "quarantine-v1".to_owned(),
                ),
            ]);
            runtime
                .remove_private_file_volume(
                    &format!("elixir-marketplace-source-{deployment}"),
                    &source_labels,
                )
                .await?;
            Ok(())
        }
        .await;
        database.pool.close().await;
        cleanup?;
        match result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}
