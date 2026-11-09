//! Closed, read-only Docker-host sampling. This is configuration, not physical
//! admission, source execution, network permission or installed authority.
use super::model::*;
use crate::marketplace::runtime::SourceJobLimits;
use anyhow::{Context, Result, bail};
use elixir_marketplace::policy::OrdinaryLimits;
use std::collections::HashMap;

pub(crate) const START: &[u8] = b"ELIXIR_RESOURCE_START_V1\n";
const PROFILE: &str = "ELIXIR_RESOURCE_PROFILE_V1\n";
pub(crate) const MAX_REPORT: usize = 32 * 1024;
pub(crate) const ENTRYPOINT: &str = "/usr/bin/timeout";

/// Constructible only by the trusted server, never a peer/HTTP DTO. The probe
/// uses a subset of the already explicit source-job allocation, not new memory,
/// CPU, persistent storage, a catalog worker or a background service.
#[derive(Clone)]
pub struct MeasurementSpec {
    spec: ContainerSpec,
    limits: OrdinaryLimits,
}
impl MeasurementSpec {
    pub(crate) fn new(deployment: &str, image: &str, source: &SourceJobLimits) -> Result<Self> {
        source.validate()?;
        if deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || source.memory_bytes < 64 * 1024 * 1024
        {
            bail!("invalid measurement owner or existing source allocation");
        }
        // The read-only bootstrap probe remains at most 0.1 core even when the
        // explicitly admitted foreground writer is allowed more. It never
        // overlaps that writer or consumes another resource allocation.
        let limits = OrdinaryLimits {
            memory_bytes: 64 * 1024 * 1024,
            cpu_millicores: source
                .cpu_millicores
                .min(OrdinaryLimits::default().cpu_millicores),
            persistent_bytes: 16 * 1024 * 1024,
            ..Default::default()
        };
        let script = format!(
            "IFS= read -r marker\n[ \"$marker\" = ELIXIR_RESOURCE_START_V1 ]\nprintf '{PROFILE}'\n\
             /bin/cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max \
             /sys/fs/cgroup/cpu.max /sys/fs/cgroup/pids.max /proc/1/status /proc/1/limits\n{}",
            crate::marketplace::host_observation::command(
                crate::marketplace::host_observation::Disk::Root
            )[3]
        );
        let mut spec = ContainerSpec {
            name: name(deployment),
            image: image.to_owned(),
            network: String::new(),
            network_mode: Some("none".into()),
            aliases: vec![],
            env: vec![],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::new(),
            command: vec![
                "--signal=KILL".into(),
                "5s".into(),
                "/bin/sh".into(),
                "-eu".into(),
                "-c".into(),
                script,
            ],
            cap_add: vec![],
            cap_drop: vec![],
            devices: vec![],
            sysctls: HashMap::new(),
            security: Default::default(),
        };
        crate::marketplace::runtime::prepare_ephemeral_worker(&mut spec, &limits)?;
        spec.security.tmpfs.clear();
        spec.security.stdin_open = true;
        spec.security.file_size_limit_bytes = Some(0);
        spec.labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.into(), "true".into()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.into()),
            (
                ELIXIR_EXTENSION_ID_LABEL.into(),
                "elixir-marketplace-measurement".into(),
            ),
            (
                ELIXIR_INSTANCE_ID_LABEL.into(),
                format!("marketplace-measurement-{deployment}"),
            ),
            (
                "elixir.marketplace.isolation".into(),
                "measurement-v1".into(),
            ),
        ]);
        apply_container_spec_fingerprint(&mut spec);
        Ok(Self { spec, limits })
    }
    pub(crate) fn spec(&self) -> &ContainerSpec {
        &self.spec
    }
    pub(crate) fn limits(&self) -> &OrdinaryLimits {
        &self.limits
    }
    pub(crate) fn report(&self, report: &str) -> Result<String> {
        if report.len() > MAX_REPORT || report.contains('\0') {
            bail!("measurement output bounds");
        }
        let report = report
            .strip_prefix(PROFILE)
            .context("measurement profile marker missing")?;
        let (kernel, resource) = report
            .split_once("ELIXIR_RESOURCE_V1\n")
            .context("measurement resource marker missing")?;
        crate::marketplace::runtime::verify_measurement_kernel(kernel, &self.limits)?;
        let resource = format!("ELIXIR_RESOURCE_V1\n{resource}");
        crate::marketplace::host_observation::Snapshot::parse(&resource)
            .map_err(anyhow::Error::msg)?;
        Ok(resource)
    }
}
pub(crate) fn name(deployment: &str) -> String {
    format!("elixir-marketplace-measurement-{deployment}")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn profile() -> MeasurementSpec {
        MeasurementSpec::new(
            "fixture",
            &format!("sha256:{}", "a".repeat(64)),
            &SourceJobLimits::new(256 * 1024 * 1024, 100).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn measurement_is_closed_read_only_and_within_the_source_allocation() {
        let p = profile();
        let spec = p.spec();
        assert_eq!(p.limits.memory_bytes, 64 * 1024 * 1024);
        assert_eq!(spec.network_mode.as_deref(), Some("none"));
        assert!(spec.volumes.is_empty() && spec.ports.is_empty() && spec.env.is_empty());
        assert!(spec.devices.is_empty() && spec.sysctls.is_empty() && spec.cap_add.is_empty());
        assert!(
            spec.security.tmpfs.is_empty()
                && spec.security.read_only_rootfs
                && spec.security.no_new_privileges
        );
        assert_eq!(spec.security.file_size_limit_bytes, Some(0));
        assert!(spec.security.stdin_open);
        assert_eq!(
            spec.command[..5],
            ["--signal=KILL", "5s", "/bin/sh", "-eu", "-c"]
        );
        assert!(!spec.command[5].contains("/state"));
        let source = SourceJobLimits::new(p.limits.memory_bytes, p.limits.cpu_millicores).unwrap();
        for owner in ["", "../other", "-bad/owner"] {
            assert!(MeasurementSpec::new(owner, &spec.image, &source).is_err());
        }
        assert!(MeasurementSpec::new("fixture", "mutable:latest", &source).is_err());
        assert!(SourceJobLimits::new(32 * 1024 * 1024, 100).is_err());
        let foreground = SourceJobLimits::new(64 * 1024 * 1024, 500).unwrap();
        let higher = MeasurementSpec::new("fixture-half-core", &spec.image, &foreground).unwrap();
        assert_eq!(higher.limits().cpu_millicores, 100);
        assert_eq!(higher.spec().security.cpu_quota.as_deref(), Some("0.100"));
    }
    #[test]
    fn measurements_require_real_kernel_limits_and_closed_resource_frames() -> Result<()> {
        let p = profile();
        let mut kernel = "67108864\n0\n10000 100000\n64\nNoNewPrivs:\t1\nSeccomp:\t2\nUid:\t10004\t10004\t10004\t10004\nGid:\t10004\t10004\t10004\t10004\n".to_owned();
        for cap in ["Eff", "Inh", "Prm", "Bnd", "Amb"] {
            kernel += &format!("Cap{cap}:\t0000000000000000\n");
        }
        kernel += "Max core file size        0 0 bytes\nMax open files            256 256 files\nMax file size             0 0 bytes\n";
        let resources = "ELIXIR_RESOURCE_V1\nCPU_BEFORE\ncpu 1\nKERNEL\nMemAvailable: 1 kB\nDISK\n10 4096\nCPU_AFTER\ncpu 2\n";
        let report = format!("{PROFILE}{kernel}{resources}");
        assert_eq!(p.report(&report)?, resources);
        for bad in [
            report.replacen("67108864", "max", 1),
            report.replacen("\n64\n", "\nmax\n", 1),
            report.replace("NoNewPrivs:\t1", "NoNewPrivs:\t0"),
            report.replace("CapEff:\t0000000000000000", "CapEff:\t0000000000000001"),
            report.replace(
                "Max file size             0 0",
                "Max file size             1 1",
            ),
            report.replace("CPU_AFTER", "KERNEL"),
            format!("{report}\0"),
            "x".repeat(MAX_REPORT + 1),
        ] {
            assert!(p.report(&bad).is_err());
        }
        Ok(())
    }
}
