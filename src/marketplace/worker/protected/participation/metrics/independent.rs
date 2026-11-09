//! Physical source-job admission without a catalog, browser, issuer, receipt,
//! network credit or marketplace connection. Observations still come from an
//! actual isolated Docker-host process under the calling job's retained lane.
use super::*;
use crate::marketplace::quarantine::{HostObservation, ResourceAdmission};

pub(in crate::marketplace) struct IndependentAdmission {
    playback: Arc<PlaybackJobManager>,
    storage: PathBuf,
    ordinary: OrdinaryLimits,
    application_bytes: u64,
    server_memory: tokio::sync::Mutex<Option<ServerMemory>>,
}
const SERVER_MEMORY_MAX_AGE: Duration = Duration::from_secs(1);

/// Scheduling data, never a native/grant/VPN capability. One short-lived cache
/// per owned application context avoids vm_stat at every nested HTTP check.
/// Start-time dating prevents a slow query from extending its own freshness.
struct ServerMemory {
    started: Instant,
    available: u64,
}
impl ServerMemory {
    fn available_at(&self, now: Instant) -> Option<u64> {
        now.checked_duration_since(self.started)
            .filter(|age| *age < SERVER_MEMORY_MAX_AGE)
            .map(|_| self.available)
    }
}
impl IndependentAdmission {
    /// Trusted retained-storage configuration, not a peer/HTTP path. It must
    /// exist: unavailable storage is not an optimistic free-space estimate.
    pub(in crate::marketplace) fn new(
        playback: Arc<PlaybackJobManager>,
        storage: PathBuf,
    ) -> Result<Arc<Self>> {
        Self::configured(playback, storage, false)
    }
    pub(in crate::marketplace) fn for_application(
        playback: Arc<PlaybackJobManager>,
        storage: PathBuf,
    ) -> Result<Arc<Self>> {
        Self::configured(playback, storage, true)
    }
    fn configured(
        playback: Arc<PlaybackJobManager>,
        storage: PathBuf,
        application: bool,
    ) -> Result<Arc<Self>> {
        if !storage.is_absolute() || !std::fs::symlink_metadata(&storage)?.is_dir() {
            bail!("installed source storage must be an existing real directory");
        }
        Ok(Arc::new(Self {
            playback,
            storage: storage.canonicalize()?,
            ordinary: OrdinaryLimits::default(),
            application_bytes: if application { 224 * 1024 * 1024 } else { 0 },
            server_memory: Default::default(),
        }))
    }
    async fn application_memory(&self) -> Result<u64> {
        let mut sample = self.server_memory.lock().await;
        if let Some(available) = sample.as_ref().and_then(|s| s.available_at(Instant::now())) {
            return Ok(available);
        }
        *sample = None;
        let started = Instant::now();
        let available = host_available().await?;
        *sample = Some(ServerMemory { started, available });
        Ok(available)
    }
    pub(in crate::marketplace) async fn application_server_current(&self) -> Result<()> {
        if self.application_bytes == 0 {
            bail!("application HTTP requires its independent resource reservation");
        }
        if host_cpu_pressure() || self.playback.startup_queue_len() > 0 {
            bail!("application HTTP yields to current host pressure or playback startup");
        }
        let available = self.application_memory().await?;
        // Always recheck playback/load after a potentially asynchronous sample.
        // Neither is cached, and missing/failed memory queries are not reused.
        if available < self.ordinary.memory_bytes * 2
            || host_cpu_pressure()
            || self.playback.startup_queue_len() > 0
        {
            bail!("application HTTP yields to current host pressure or playback startup");
        }
        storage_available(&self.storage)?;
        Ok(())
    }
    pub(in crate::marketplace) async fn application_current(
        &self,
        actual: &crate::marketplace::application_egress::ApplicationGatewayObservation,
    ) -> Result<()> {
        self.application_server_current().await?;
        application_kernel_headroom(&self.ordinary, actual.resources())
    }
}
fn application_kernel_headroom(ordinary: &OrdinaryLimits, resources: &str) -> Result<()> {
    let available = linux_available(resources)?;
    let required = ordinary.memory_bytes * 2;
    let [_, memory_psi, io_psi] = pressure_averages(resources)?;
    let pressure = foreground_memory_io_pressure(resources)?;
    if available < required || pressure {
        bail!(
            "application HTTP yields to current Docker-host headroom (available/required memory={available}/{required}, memory_or_io_pressure={pressure}, memory_some_avg10={memory_psi}, io_some_avg10={io_psi})"
        );
    }
    Ok(())
}
#[async_trait::async_trait]
impl ResourceAdmission for IndependentAdmission {
    fn needs_measurement(&self) -> bool {
        true
    }
    async fn before_measurement(&self, plan: &SourceQuarantinePlan) -> Result<()> {
        // A kernel sample itself needs a process. Its 64 MiB/at-most-0.1-core
        // ceiling is a SUBSET of this already explicit source allocation and
        // never overlaps its provisioner/writer. First check actual server OS
        // headroom/playback. Unknown Docker-host headroom is not called safe:
        // the read-only bootstrap may fail, and no source work then starts.
        let memory = host_available().await?;
        let memory_needed = self
            .ordinary
            .memory_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.application_bytes))
            .and_then(|n| n.checked_add(plan.memory_reservation_bytes()))
            .context("bootstrap resource bounds")?;
        let pressure = host_cpu_pressure();
        let playback = self.playback.startup_queue_len() > 0;
        if memory < memory_needed || pressure || playback {
            return Err(SourceAdmissionDeferred::Bootstrap {
                memory,
                memory_needed,
                pressure,
                playback,
            }
            .into());
        }
        storage_available(&self.storage)?;
        Ok(())
    }
    async fn check(
        &self,
        plan: &SourceQuarantinePlan,
        provisioned: bool,
        observation: Option<&HostObservation>,
    ) -> Result<()> {
        let observation = observation
            .context("installed admission requires an actual native host observation")?;
        let report = observation.report()?;
        let sample = probe::Snapshot::parse(report).map_err(anyhow::Error::msg)?;
        let before = cpu::Sample::parse(sample.before).map_err(anyhow::Error::msg)?;
        let after = cpu::Sample::parse(sample.after).map_err(anyhow::Error::msg)?;
        let guest_cpu = after.pressured_since(before).map_err(anyhow::Error::msg)?;
        let host = host_available().await?;
        let disk = storage_available(&self.storage)?.min(disk_available(sample.disk)?);
        check_source_headroom(
            &self.ordinary,
            plan,
            if provisioned {
                SourceStage::AfterProvision
            } else {
                SourceStage::BeforeProvision
            },
            host.min(linux_available(sample.kernel)?)
                .saturating_sub(self.application_bytes),
            disk,
            guest_cpu || foreground_memory_io_pressure(sample.kernel)? || host_cpu_pressure(),
            self.playback.startup_queue_len() > 0,
        )?;
        observation.report()?;
        Ok(())
    }
}

fn storage_available(path: &std::path::Path) -> Result<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // OS writes this owned correctly-sized buffer only on success. The
        // path is immutable trusted configuration and this is a read-only query.
        if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stats = unsafe { stats.assume_init() };
        if stats.f_frsize == 0 {
            bail!("unknown retained-storage block size");
        }
        (u128::from(stats.f_bavail) * u128::from(stats.f_frsize))
            .try_into()
            .context("retained-storage free-space overflow")
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        bail!("retained-storage measurement unsupported on this host")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn application_server_memory_reuse_is_call_local_short_and_start_dated() {
        let started = Instant::now();
        let sample = ServerMemory {
            started,
            available: 123,
        };
        assert_eq!(sample.available_at(started), Some(123));
        assert_eq!(
            sample.available_at(started + Duration::from_millis(999)),
            Some(123)
        );
        assert_eq!(sample.available_at(started + SERVER_MEMORY_MAX_AGE), None);
        assert_eq!(sample.available_at(started + Duration::from_secs(20)), None);
        assert_eq!(
            sample.available_at(started - Duration::from_millis(1)),
            None
        );
        let unknown: Option<ServerMemory> = None;
        assert_eq!(unknown.as_ref().and_then(|s| s.available_at(started)), None);
    }
    #[test]
    fn application_kernel_admission_preserves_media_headroom_and_refuses_unknown_pressure()
    -> Result<()> {
        let ordinary = OrdinaryLimits::default();
        let valid = format!(
            "MemAvailable: {} kB\nsome avg10=90.00 avg60=0.00 avg300=0.00 total=0\nsome avg10=0.00 avg60=0.00 avg300=0.00 total=0\nsome avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
            ordinary.memory_bytes * 2 / 1024
        );
        application_kernel_headroom(&ordinary, &valid)?;
        for invalid in [
            valid.replace("524288 kB", "524287 kB"),
            valid.replacen("avg10=0.00", "avg10=10.00", 1),
            valid
                .rsplit_once("avg10=0.00")
                .map(|(before, after)| format!("{before}avg10=50.00{after}"))
                .unwrap(),
            valid.replace("avg10=0.00", "avg10=NaN"),
            valid.replace("MemAvailable:", "UnknownMemory:"),
            valid.replace("some avg10=90.00 avg60=0.00 avg300=0.00 total=0\n", ""),
            format!("{valid}some avg10=0.00\n"),
        ] {
            assert!(application_kernel_headroom(&ordinary, &invalid).is_err());
        }
        // Cold-start partial contention is not a reason to deny foreground
        // HTTP when measured RAM remains available. Relay policy stays lower.
        let busy = valid.replacen("avg10=0.00", "avg10=9.99", 1);
        assert!(memory_io_pressure(&busy)?);
        application_kernel_headroom(&ordinary, &busy)?;
        Ok(())
    }
}
