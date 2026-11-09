//! Read-only measurements under the SAME source-job lane/fence/deadline. Not a
//! source-byte, SDK or installed-runtime permit; never a persistent service.
use super::*;
use crate::runtime::measurement::{MAX_REPORT, MeasurementSpec, START};

pub(in crate::marketplace) struct HostObservation {
    report: String,
    sampled: Instant,
}
impl HostObservation {
    pub(in crate::marketplace) fn report(&self) -> Result<&str> {
        if self.sampled.elapsed() > Duration::from_secs(8) {
            bail!("Docker-host measurement expired");
        }
        Ok(&self.report)
    }
}

/// Data reuse only, never native/admission authority. In particular, the free
/// space observed before preallocation cannot describe the provisioned phase.
/// Taking ownership prevents a cancelled check from leaving a cached success.
pub(super) fn reusable_sample(
    previous: Option<(bool, HostObservation)>,
    provisioned: bool,
) -> Option<HostObservation> {
    previous.and_then(|(phase, sample)| {
        (phase == provisioned && sample.report().is_ok()).then_some(sample)
    })
}

impl SourceQuarantineJob {
    pub(super) async fn measure_host(&mut self, expires: Instant) -> Result<HostObservation> {
        let spec = MeasurementSpec::new(&self.deployment, &self.volume.image, &self.plan.limits)?;
        self.begin_activity(Target::Measurement {
            deployment: self.deployment.clone(),
            container_id: None,
        })?;
        let runtime = self.runtime.clone();
        let operation = async {
            let (fence, handle) = self
                .launch_native(
                    launch::Operation::Measurement(Box::new(spec.clone())),
                    expires,
                )
                .await?;
            self.activity = fence;
            let handle = handle.context("measurement returned no immutable identity")?;
            let mut process = runtime
                .attach_private_stdio(&handle, &spec.spec().labels)
                .await?;
            let mut diagnostics = process
                .diagnostics
                .take()
                .context("measurement stderr missing")?;
            // Both private streams are bounded and read concurrently. A native
            // failure never returns partial data as a resource observation.
            let output = async {
                process.input.write_all(START).await?;
                process.input.flush().await?;
                let mut bytes = Vec::new();
                (&mut process.output)
                    .take(MAX_REPORT as u64 + 1)
                    .read_to_end(&mut bytes)
                    .await?;
                if bytes.len() > MAX_REPORT {
                    bail!("measurement stdout exceeds its bound");
                }
                if !process.child.wait().await?.success() {
                    bail!("measurement attachment failed");
                }
                Ok::<_, anyhow::Error>(bytes)
            };
            let errors = async {
                let mut bytes = Vec::new();
                (&mut diagnostics)
                    .take(4097)
                    .read_to_end(&mut bytes)
                    .await?;
                if !bytes.is_empty() {
                    bail!("measurement diagnostics are not empty");
                }
                Ok::<_, anyhow::Error>(())
            };
            let (bytes, ()) = tokio::try_join!(output, errors)?;
            runtime
                .confirm_host_measurement_success(&handle, &spec)
                .await?;
            let report = spec.report(std::str::from_utf8(&bytes)?)?;
            Ok::<_, anyhow::Error>(HostObservation {
                report,
                sampled: Instant::now(),
            })
        };
        let result = timeout_at(expires, operation)
            .await
            .context("native measurement deadline")
            .and_then(|value| value);
        // Keep the same launch/fence owner on cancelled waits; this method
        // never refunds a claim or pretends an unknown native outcome stopped.
        self.cleanup_native_activity(runtime.as_ref()).await?;
        result
    }
}

#[cfg(test)]
mod sample_tests {
    use super::*;

    // Scheduling data only, not a native measurement/cleanup/admission proof.
    fn sample() -> HostObservation {
        HostObservation {
            report: "unit scheduling sample, not native evidence".into(),
            sampled: Instant::now(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reused_samples_keep_original_expiry_and_do_not_accumulate() {
        for phase in [false, true] {
            let sample = sample();
            let original = sample.sampled;
            let mut cached = Some((phase, sample));
            tokio::time::advance(Duration::from_secs(7)).await;
            let sample = reusable_sample(cached.take(), phase).unwrap();
            assert!(cached.is_none());
            assert_eq!(sample.sampled, original);
            tokio::time::advance(Duration::from_millis(999)).await;
            let sample = reusable_sample(Some((phase, sample)), phase).unwrap();
            assert_eq!(sample.sampled, original);
            tokio::time::advance(Duration::from_millis(2)).await;
            assert!(reusable_sample(Some((phase, sample)), phase).is_none());
        }
    }

    #[test]
    fn preallocation_phase_changes_cannot_reuse_old_free_space() {
        assert!(reusable_sample(None, false).is_none());
        assert!(reusable_sample(Some((false, sample())), true).is_none());
        assert!(reusable_sample(Some((true, sample())), false).is_none());
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::marketplace::worker::{IndependentAdmission, SourceResourceWait};
    use crate::runtime::docker::DockerRuntimeManager;
    use crate::{config::DatabaseConfig, db::Database, playback::PlaybackJobManager};
    use futures_util::FutureExt;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct SampleChecks {
        mode: AtomicUsize,
        before: AtomicUsize,
        checked: AtomicUsize,
        entered: tokio::sync::Notify,
    }
    impl SampleChecks {
        async fn point(&self, point: usize) -> Result<()> {
            let mode = self.mode.load(Ordering::SeqCst);
            if mode == point {
                bail!("unit scheduling rejection");
            }
            if mode == point + 2 {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl ResourceAdmission for SampleChecks {
        fn needs_measurement(&self) -> bool {
            true
        }
        async fn before_measurement(&self, _: &SourceQuarantinePlan) -> Result<()> {
            self.before.fetch_add(1, Ordering::SeqCst);
            self.point(1).await
        }
        async fn check(
            &self,
            _: &SourceQuarantinePlan,
            _: bool,
            measurement: Option<&HostObservation>,
        ) -> Result<()> {
            measurement.context("unit sample missing")?.report()?;
            self.checked.fetch_add(1, Ordering::SeqCst);
            self.point(2).await
        }
    }

    #[tokio::test]
    async fn sample_reuse_keeps_both_checks_and_failed_or_cancelled_checks_leave_no_cached_success()
    -> Result<()> {
        // Synthetic scheduling fixture ONLY. The runtime cannot create a real
        // process; this exercises no native/physical admission authority.
        let root = tempfile::tempdir()?;
        let lane = root.path().canonicalize()?.join("sample-checks");
        let mut matched = super::super::tests::matched_fixture(&lane).await?;
        let job = &mut matched.source.job;
        job.runtime = Arc::new(super::super::peer_tests::Unavailable);
        let checks = Arc::new(SampleChecks::default());
        job.admission = Some(checks.clone());
        let counters = (
            job.input_used,
            job.output_used,
            job.expires,
            job.native_attempted,
        );
        // Pause only AFTER the SDK fixture's real Node process has joined.
        tokio::time::pause();
        let sample = || HostObservation {
            report: "unit scheduling sample, not native evidence".into(),
            sampled: Instant::now(),
        };
        let original = sample();
        let sampled = original.sampled;
        job.cached_measurement = Some((job.provisioned, original));
        for expected in 1..=2 {
            job.admit_native(job.expires).await?;
            assert_eq!(checks.before.load(Ordering::SeqCst), expected);
            assert_eq!(checks.checked.load(Ordering::SeqCst), expected);
            assert_eq!(job.cached_measurement.as_ref().unwrap().1.sampled, sampled);
        }
        for mode in [1, 2] {
            checks.mode.store(mode, Ordering::SeqCst);
            job.cached_measurement = Some((job.provisioned, sample()));
            assert!(job.admit_native(job.expires).await.is_err());
            assert!(job.cached_measurement.is_none());
        }
        for mode in [3, 4] {
            checks.mode.store(mode, Ordering::SeqCst);
            job.cached_measurement = Some((job.provisioned, sample()));
            let mut admission = Box::pin(job.admit_native(job.expires));
            tokio::select! {
                result = &mut admission => panic!("pending unit check returned: {result:?}"),
                _ = checks.entered.notified() => {}
            }
            drop(admission);
            assert!(job.cached_measurement.is_none());
        }
        // Let the unchanged eight-second admission deadline expire as well.
        checks.mode.store(4, Ordering::SeqCst);
        job.cached_measurement = Some((job.provisioned, sample()));
        assert!(job.admit_native(job.expires).await.is_err());
        assert!(job.cached_measurement.is_none());
        job.admission = None;
        job.cached_measurement = Some((job.provisioned, sample()));
        job.admit_native(job.expires).await?;
        assert!(job.cached_measurement.is_none());
        assert_eq!(
            counters,
            (
                job.input_used,
                job.output_used,
                job.expires,
                job.native_attempted
            )
        );
        assert!(job.activity.is_none() && job.launch.is_none() && job.failed_launch.is_none());
        Ok(())
    }

    struct WaitAfterActualMeasurement(Arc<IndependentAdmission>);
    #[async_trait::async_trait]
    impl ResourceAdmission for WaitAfterActualMeasurement {
        fn needs_measurement(&self) -> bool {
            true
        }
        async fn before_measurement(&self, plan: &SourceQuarantinePlan) -> Result<()> {
            self.0.before_measurement(plan).await
        }
        async fn check(
            &self,
            _: &SourceQuarantinePlan,
            _: bool,
            observation: Option<&HostObservation>,
        ) -> Result<()> {
            observation
                .context("native observation missing")?
                .report()?;
            // Artificial NEGATIVE scheduling outcome only. The successful
            // branch below uses actual independent headroom, not this fixture.
            Err(SourceResourceWait::fixture().into())
        }
    }
    #[tokio::test]
    #[ignore = "explicit offline Docker source admission without a catalog, issuer or browser; actual host/guest/storage measurements"]
    async fn independent_admission_measures_and_retires_native_probes_before_real_source_work()
    -> Result<()> {
        let runtime = Arc::new(DockerRuntimeManager::new(None));
        let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
            .context("explicit immutable image required")?;
        if !image_is_pinned(&image) {
            bail!("test requires an immutable image");
        }
        let root = tempfile::tempdir()?;
        let storage = root.path().canonicalize()?;
        let lane = storage.join("expensive");
        let deployment = format!("measure-{}", uuid::Uuid::new_v4().simple());
        let database = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", storage.join("host.sqlite").display()),
            max_connections: 2,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let playback = Arc::new(PlaybackJobManager::new(database.pool.clone(), None));
        // A managed steady-playback record must not veto essential source
        // work. This is actual native admission, not playback latency proof.
        playback
            .insert_test_job(uuid::Uuid::new_v4(), storage.join("steady-playback"), 0)
            .await;
        assert!(playback.has_latency_sensitive_work());
        assert_eq!(playback.startup_queue_len(), 0);
        let admission = IndependentAdmission::new(playback.clone(), storage.clone())?;
        let (release, package, proof) = super::super::tests::fixture()?;
        let plan =
            SourceQuarantinePlan::for_release(&release, None, &super::super::tests::budget())?;
        playback.set_test_startup_queue_len(1);
        let blocked = admission.before_measurement(&plan).await;
        playback.set_test_startup_queue_len(0);
        assert!(
            matches!(
                blocked
                    .as_ref()
                    .err()
                    .and_then(|e| e.downcast_ref::<SourceResourceWait>()),
                Some(SourceResourceWait::Bootstrap { playback: true, .. })
            ),
            "live startup/seek must stop admission before a native probe is allocated"
        );
        let volume = format!("elixir-marketplace-source-{deployment}");
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.into(), "true".into()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.clone()),
            ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
        ]);
        let mut slot = None;
        let outcome = std::panic::AssertUnwindSafe(async {
            let result = SourceQuarantineJob::open_leased_retained(
                &mut slot,
                runtime.clone(),
                &deployment,
                &image,
                &plan,
                ControllerLease::acquire(&lane)?,
                Arc::new(WaitAfterActualMeasurement(admission.clone())),
            )
            .await;
            let error =
                result.expect_err("artificial resource wait unexpectedly started source work");
            assert_eq!(error.downcast_ref::<SourceResourceWait>(), Some(&SourceResourceWait::fixture()),
                "the artificial wait must be reached AFTER actual measurement, not substituted by an earlier bootstrap deferral: {error:#}");
            let job = slot.as_mut().context("opening lost its owned job")?;
            assert!(job.admission_unstarted());
            assert!(!job.provisioned && !job.native_attempted);
            assert!(
                runtime
                    .get_container_handle(&crate::runtime::measurement::name(&deployment))
                    .await?
                    .is_none()
            );
            assert!(!runtime.private_file_volume_owned(&volume, &labels).await?);
            assert!(
                ControllerLease::acquire(&lane).is_err(),
                "same lane remains held for original work"
            );
            job.admission = Some(admission);
            job.provision(job.expires).await?;
            assert!(job.provisioned && job.native_attempted);
            job.receive(
                runtime.as_ref(),
                QuarantinePurpose::Package,
                &mut package.as_slice(),
            )
            .await?;
            job.receive(
                runtime.as_ref(),
                QuarantinePurpose::Proof,
                &mut proof.as_slice(),
            )
            .await?;
            job.discard_retained().await?;
            assert!(
                runtime
                    .get_container_handle(&crate::runtime::measurement::name(&deployment))
                    .await?
                    .is_none()
            );
            assert!(
                runtime
                    .get_container_handle(&format!("elixir-marketplace-worker-{deployment}"))
                    .await?
                    .is_none(),
                "no catalog was started"
            );
            Ok::<_, anyhow::Error>(())
        })
        .catch_unwind()
        .await;
        if let Some(job) = slot.as_mut() {
            job.discard_retained().await?;
        }
        drop(slot);
        drop(ControllerLease::acquire(&lane)?);
        assert!(!runtime.private_file_volume_owned(&volume, &labels).await?);
        database.pool.close().await;
        match outcome {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}
