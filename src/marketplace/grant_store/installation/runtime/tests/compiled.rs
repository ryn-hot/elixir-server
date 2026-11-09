//! Actual compiled retained load/controller/SQL/AES SDK. The initial ACTIVE
//! journal transition below is EXPLICITLY synthetic, not activation proof.
use super::*;
use crate::marketplace::runtime::build::ApprovedToolchains;
use std::path::Path;

const MIB: u64 = 1024 * 1024;

// Establish the actual durable runtime owner while the database is empty,
// before any source job or installed history exists. Never synthesize/adopt
// lineage after staging: the production controller deliberately rejects that.
pub(in crate::marketplace) async fn initialize_compiled_fixture(
    runtime: Arc<dyn RuntimeManager>,
    root: &Path,
    lane: &Path,
    worker: &str,
    deployment: &str,
) -> Result<()> {
    let db = grant_tests::connect(&root.join("retained-build.sqlite")).await?;
    let controller = InstalledController::open(
        db.clone(),
        SecretsManager::from_key_bytes([93; 32], false),
        compiled_fixture_configuration(&db, runtime, root, lane, worker, deployment)?,
    )
    .await?;
    controller.require_idle()?;
    drop(controller);
    db.pool.close().await;
    Ok(())
}

pub(in crate::marketplace) async fn run_compiled_fixture(
    runtime: Arc<dyn RuntimeManager>,
    root: &Path,
    lane: &Path,
    worker: &str,
    deployment: &str,
    value: &serde_json::Value,
) -> Result<()> {
    let db = grant_tests::connect(&root.join("retained-build.sqlite")).await?;
    let id: ExtensionId = value["release"]["payload"]["extension_id"]
        .as_str()
        .context("compiled fixture extension missing")?
        .parse()?;
    let (home, instance) = enroll_fixture(&db, &id).await?;
    let secrets = SecretsManager::from_key_bytes([93; 32], false);
    grant_tests::secret(&db, "global", None, "source_api", "never-global", &secrets).await?;
    grant_tests::secret(
        &db,
        "instance",
        Some(instance),
        "source_api",
        "own-native-instance",
        &secrets,
    )
    .await?;
    let config =
        compiled_fixture_configuration(&db, runtime.clone(), root, lane, worker, deployment)?;
    let image_cache = runtime
        .marketplace_import_profile()
        .is_some_and(|profile| profile.image_cache_enabled());
    // Fail before source admission; stored metadata/history cannot approve a
    // compiler, enable a missing runtime policy or substitute another key.
    for wrong_key in [false, true] {
        let mut disabled = config.clone();
        if !wrong_key {
            disabled.compiled_toolchains = None;
        }
        let manager = if wrong_key {
            SecretsManager::from_key_bytes([94; 32], true)
        } else {
            secrets.clone()
        };
        let mut controller = InstalledController::open(db.clone(), manager, disabled).await?;
        let binding =
            InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
        controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
        let error = controller
            .finish()
            .await
            .err()
            .context("compiled preflight accepted missing policy/wrong key")?;
        let text = format!("{error:#}");
        if !(text.contains("not configured")
            || text.contains("decrypt")
            || text.contains("authentication"))
        {
            return Err(error).context("negative did not reach compiled preflight");
        }
        controller.shutdown_cache().await?;
        drop(controller);
        assert!(
            runtime
                .get_container_handle(&format!(
                    "elixir-marketplace-source-writer-{}",
                    config.deployment
                ))
                .await?
                .is_none()
        );
    }
    let mut controller =
        InstalledController::open(db.clone(), secrets.clone(), config.clone()).await?;
    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    let cold_started = std::time::Instant::now();
    controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
    assert_eq!(controller.finish().await?.as_str(), "true");
    let cold_elapsed = cold_started.elapsed();
    let first_revision = revision(&db, instance).await?;
    assert!(first_revision > 0);
    let mut expected_revision = first_revision;
    let cached = cached_input_state(runtime.as_ref(), lane)
        .await?
        .context("compiled input bytes were not retained")?;
    let first_image = controller.image_cache_snapshot().await?;
    if image_cache {
        let (config, _, bytes) = first_image
            .as_ref()
            .context("cold call did not retain an actual image")?;
        assert!(*bytes > 0 && *bytes <= 512 * MIB);
        runtime.inspect_marketplace_import_image(*config).await?;
    }

    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    let warm_started = std::time::Instant::now();
    controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
    assert_eq!(controller.finish().await?.as_str(), "true");
    let warm_elapsed = warm_started.elapsed();
    println!(
        "compiled input-byte timing: cold_ms={} cached_ms={}; fresh source/filesystem; optional image cache={image_cache}",
        cold_elapsed.as_millis(),
        warm_elapsed.as_millis()
    );
    if image_cache {
        assert_eq!(
            controller.image_cache_snapshot().await?,
            first_image,
            "warm call must use the SAME image-reference owner, not recreate it"
        );
        println!(
            "compiled image-cache reuse: fresh job/source/artifact/grants; identical passive reference; original foreground lane released"
        );
        let active_recovery =
            std::env::var("ELIXIR_TEST_COMPILED_ACTIVE_RECOVERY").as_deref() == Ok("1");
        if !active_recovery {
            Box::pin(qualify_idle_expiry(
                &controller,
                runtime.as_ref(),
                first_image.as_ref().unwrap(),
            ))
            .await?;
            let binding =
                InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
            controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
            assert_eq!(controller.finish().await?.as_str(), "true");
            assert_ne!(controller.image_cache_snapshot().await?, first_image);
        }

        // Retire this parent's owners before starting a separate real process.
        // The child then fills its own actual cache and exits without Drop.
        controller.shutdown_cache().await?;
        drop(controller);
        controller = Box::pin(qualify_abrupt_restart(
            &db,
            &secrets,
            &config,
            root,
            (home, &id, instance),
            deployment,
            if active_recovery {
                RestartPhase::Active
            } else {
                RestartPhase::Finished
            },
        ))
        .await?;
        if active_recovery {
            expected_revision = revision(&db, instance).await?;
            assert_eq!(
                expected_revision,
                first_revision + 1,
                "the committed crash marker must survive, without replaying any SDK effects"
            );
        }
        let binding =
            InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
        let capsule = binding.material(&db).await?;
        let facts = capsule.facts()?;
        assert_eq!(facts.release().payload().artifacts.len(), 1);
        let plan =
            SourceQuarantinePlan::for_release(facts.release(), Some(0), &config.source_budget)?;
        let lease = ControllerLease::acquire(lane)?;
        let admission = IndependentAdmission::new(config.playback.clone(), config.archive.clone())?;
        let mut other = None;
        let opened = SourceQuarantineJob::open_leased_retained(
            &mut other,
            runtime.clone(),
            "compiled-cache-foreground-switch",
            worker,
            &plan,
            lease,
            admission,
        )
        .await;
        let retired = if let Some(job) = other.as_mut() {
            job.discard_retained().await
        } else {
            Ok(())
        };
        opened?;
        retired?;
        drop(other);
        assert!(
            controller.image_cache_snapshot().await?.is_none(),
            "other foreground source work must evict the cache BEFORE allocation"
        );
        assert!(
            runtime
                .inspect_marketplace_import_image(first_image.as_ref().unwrap().0)
                .await
                .is_err()
        );
        let binding =
            InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
        controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
        assert_eq!(controller.finish().await?.as_str(), "true");
        assert!(controller.image_cache_snapshot().await?.is_some());
        println!(
            "compiled image-cache competing source job: original lane acquired, cached image retired before other provisioning, explicit call restored"
        );
    }
    // The actual C fixture writes fixture=true on every normal call. The SDK
    // intentionally returns a no-op acknowledgement for an unchanged value.
    assert_eq!(
        revision(&db, instance).await?,
        expected_revision,
        "identical state_set must remain idempotent"
    );
    assert_eq!(
        cached_input_state(runtime.as_ref(), lane).await?.as_ref(),
        Some(&cached)
    );

    // A matching cache label/shape is not integrity or native authority. Change
    // one artifact byte in the actual owned volume and require rejection before
    // any SDK state write; a NEW explicit call may restore the original archive.
    let before_tamper = revision(&db, instance).await?;
    crate::marketplace::application::mutate_cached_fixture_bytes(
        runtime.as_ref(),
        lane,
        worker,
        elixir_marketplace::quarantine::QuarantinePurpose::Artifact,
    )
    .await?;
    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
    let rejected = controller
        .finish()
        .await
        .err()
        .context("corrupt cached artifact reached execution")?;
    if !format!("{rejected:#}").contains("native container filesystem rejected") {
        return Err(rejected)
            .context("cached artifact did not reach actual native integrity rejection");
    }
    controller.require_idle()?;
    assert_eq!(
        revision(&db, instance).await?,
        before_tamper,
        "corrupt cache produced SDK effects"
    );
    assert!(cached_input_state(runtime.as_ref(), lane).await?.is_none());
    if image_cache {
        assert!(
            controller.image_cache_snapshot().await?.is_none(),
            "artifact rejection must retire the cached reference"
        );
    }
    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    controller.begin(binding, limits(), RuntimeJson::parse(b"null")?)?;
    assert_eq!(controller.finish().await?.as_str(), "true");
    assert_eq!(
        cached_input_state(runtime.as_ref(), lane).await?.as_ref(),
        Some(&cached)
    );
    println!(
        "compiled input-byte cache: original daemon images retired, native source/artifact/image rechecks retained; corrupted artifact rejected without SDK effects and restored only by a new call"
    );

    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    controller.begin(
        binding,
        limits(),
        RuntimeJson::from_value(json!({"cancel":true}))?,
    )?;
    wait_key(&db, instance, "cancel_started").await?;
    controller.cancel().await?;
    controller.require_idle()?;
    if image_cache {
        assert!(
            controller.image_cache_snapshot().await?.is_none(),
            "cancelled code retained its cached image"
        );
    }
    assert!(
        cached_input_state(runtime.as_ref(), lane).await?.is_none(),
        "cancelled call cannot park its workspace"
    );
    assert!(
        runtime
            .get_container_handle(&format!(
                "elixir-marketplace-source-writer-{}",
                config.deployment
            ))
            .await?
            .is_none()
    );

    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    controller.begin(
        binding,
        limits(),
        RuntimeJson::from_value(json!({"revoke":true}))?,
    )?;
    let revoke = async {
        wait_key(&db, instance, "started").await?;
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,revision=revision+1,revocation_revision=revision+1 WHERE extension_id=$1")
            .bind(id.as_str()).execute(&db.pool).await?;
        Ok::<_, anyhow::Error>(())
    };
    let (result, revoked) = tokio::join!(controller.finish(), revoke);
    revoked?;
    assert!(
        result.is_err(),
        "revoked installed binding retained native effects"
    );
    controller.require_idle()?;
    assert!(
        cached_input_state(runtime.as_ref(), lane).await?.is_none(),
        "revoked call cannot park its workspace"
    );
    if image_cache {
        assert!(
            controller.image_cache_snapshot().await?.is_none(),
            "revocation retained a cached image"
        );
    }
    controller.shutdown_cache().await?;
    drop(controller);
    drop(ControllerLease::acquire(lane)?);
    assert!(
        runtime
            .get_container_handle(&format!(
                "elixir-marketplace-source-writer-{}",
                config.deployment
            ))
            .await?
            .is_none()
    );
    println!(
        "installed compiled: actual stored-proof authentication/native rematch/reimport/controller/instance SQL-AES SDK, denied network/unapproved secret, original cancellation and live grant revocation passed; ACTIVE enrollment synthetic, no activation qualification"
    );
    Ok(())
}

async fn qualify_idle_expiry(
    controller: &InstalledController,
    runtime: &dyn RuntimeManager,
    original: &(Digest, String, u64),
) -> Result<()> {
    let deadline = controller
        .image_cache_idle_deadline()
        .await
        .context("actual cached image has no idle deadline")?;
    let started = tokio::time::Instant::now();
    let before = deadline - Duration::from_secs(5);
    assert!(
        before > started,
        "fixture already missed its pre-expiry observation"
    );
    println!("compiled image-cache idle: waiting for the actual five-minute production timer");
    tokio::time::sleep_until(before).await;
    assert_eq!(
        controller.image_cache_snapshot().await?.as_ref(),
        Some(original),
        "idle image was evicted before its actual non-renewing deadline"
    );
    tokio::time::sleep_until(deadline).await;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if controller.image_cache_snapshot().await?.is_none() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .context("actual idle timer did not join native image retirement")??;
    // Pool snapshot clears only after the actual keeper's settled retirement.
    // This additional daemon observation is not used to manufacture that proof.
    assert!(
        runtime
            .inspect_marketplace_import_image(original.0)
            .await
            .is_err()
    );
    controller.require_idle()?;
    println!(
        "compiled image-cache idle: actual timer joined retirement; waited_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

const RESTART_CHILD: &str = "marketplace::grant_store::installation::runtime::tests::compiled::actual_compiled_cache_restart_child";

#[derive(Clone, Copy, PartialEq, Eq)]
enum RestartPhase {
    Finished,
    Active,
}
impl RestartPhase {
    fn name(self) -> &'static str {
        match self {
            Self::Finished => "finished",
            Self::Active => "active",
        }
    }
    fn marker(self, root: &Path) -> PathBuf {
        root.join(format!("cache-restart-{}-ready.json", self.name()))
    }
}
// Observations only. Parsing this never constructs a verified image or owner.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RestartObservation {
    image: Digest,
    operation: String,
    pid: u32,
    active: bool,
}

async fn qualify_abrupt_restart(
    db: &Database,
    secrets: &SecretsManager,
    config: &InstalledRuntime,
    root: &Path,
    coordinates: (Uuid, &ExtensionId, Uuid),
    deployment: &str,
    phase: RestartPhase,
) -> Result<InstalledController> {
    let (home, id, instance) = coordinates;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args([RESTART_CHILD, "--exact", "--ignored", "--nocapture"])
        .env_clear()
        .kill_on_drop(true);
    // Explicit fixture inputs only: no dotenv, account credentials or ambient
    // production configuration is inherited by the subprocess.
    for key in [
        "PATH",
        "TMPDIR",
        "DOCKER_HOST",
        "SQLX_OFFLINE",
        "ELIXIR_MARKETPLACE_WORKER_IMAGE",
        "ELIXIR_MARKETPLACE_TOOLCHAIN_IMAGE",
        "ELIXIR_TEST_IMAGE_IMPORT_BINARY",
        "ELIXIR_TEST_IMAGE_IMPORT_BINARY_SHA256",
        "ELIXIR_TEST_IMAGE_IMPORT_SOCKET",
        "ELIXIR_TEST_CONTAINER_GUARDIAN_BINARY",
        "ELIXIR_TEST_CONTAINER_GUARDIAN_SHA256",
        "ELIXIR_TEST_COMPILED_IMAGE_CACHE",
    ] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    command
        .env("ELIXIR_TEST_CACHE_RESTART_ROOT", root)
        .env("ELIXIR_TEST_CACHE_RESTART_HOME", home.to_string())
        .env("ELIXIR_TEST_CACHE_RESTART_EXTENSION", id.as_str())
        .env("ELIXIR_TEST_CACHE_RESTART_INSTANCE", instance.to_string())
        .env("ELIXIR_TEST_CACHE_RESTART_DEPLOYMENT", deployment)
        .env("ELIXIR_TEST_CACHE_RESTART_PHASE", phase.name());
    let mut child = command.spawn()?;
    let pid = child
        .id()
        .context("cache restart child has no process identity")?;
    let marker = phase.marker(root);
    let ready = tokio::time::timeout(Duration::from_secs(240), async {
        if phase == RestartPhase::Finished {
            return Ok::<_, anyhow::Error>(Some(child.wait().await?));
        }
        loop {
            if let Some(status) = child.try_wait()? {
                bail!("active cache child exited before its ready marker: {status}");
            }
            if marker.is_file() {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let status = match ready {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            child.start_kill()?;
            child.wait().await?;
            return Err(error);
        }
        Err(error) => {
            child.start_kill()?;
            child.wait().await?;
            return Err(error).context("cache restart child timed out and was joined");
        }
    };
    if let Some(status) = status {
        assert!(status.success(), "cache restart child failed: {status}");
    }
    assert!(std::fs::metadata(&marker)?.len() <= 4096);
    let previous: RestartObservation = serde_json::from_slice(&std::fs::read(&marker)?)?;
    assert_eq!(previous.pid, pid);
    assert_eq!(previous.active, phase == RestartPhase::Active);
    if phase == RestartPhase::Active {
        use std::os::unix::process::ExitStatusExt;
        assert!(
            ControllerLease::acquire_for_recovery(&config.lane).is_err(),
            "active native source did not retain its actual foreground owner"
        );
        child.start_kill()?;
        let status = child.wait().await?;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
    }
    config
        .runtime
        .inspect_marketplace_import_image(previous.image)
        .await?;
    assert!(
        config
            .lane
            .join("compiled-image-cache/image-cache.request")
            .is_file()
    );
    let mut controller =
        InstalledController::open(db.clone(), secrets.clone(), config.clone()).await?;
    assert!(
        controller.image_cache_snapshot().await?.is_none(),
        "restart restored executable cache authority from disk"
    );
    assert!(
        !config
            .lane
            .join("compiled-image-cache/image-cache.request")
            .exists()
    );
    assert!(
        config
            .runtime
            .inspect_marketplace_import_image(previous.image)
            .await
            .is_err()
    );
    let binding = InstalledBinding::load(db, home, id, instance, standing_tests::limit()).await?;
    let revision_before = revision(db, instance).await?;
    let arguments = if phase == RestartPhase::Active {
        RuntimeJson::from_value(json!({"verify_crash":true}))?
    } else {
        RuntimeJson::parse(b"null")?
    };
    controller.begin(binding, limits(), arguments)?;
    assert_eq!(controller.finish().await?.as_str(), "true");
    assert_eq!(revision(db, instance).await?, revision_before);
    let fresh = controller
        .image_cache_snapshot()
        .await?
        .context("post-restart call did not fill a new actual cache")?;
    assert_eq!(fresh.0, previous.image);
    assert_ne!(
        fresh.1, previous.operation,
        "restart reused the dead process's cache owner"
    );
    if phase == RestartPhase::Active {
        println!(
            "compiled image-cache active crash: committed native SDK marker, live foreground owner, exact child SIGKILL joined; actual controller reconciled native work and image journal; fresh verified call reads preserved encrypted state without replay"
        );
    } else {
        println!(
            "compiled image-cache abrupt restart: _exit without Drop; original journal retired at reopen; no restored authority; fresh source/native call and new cache owner passed"
        );
    }
    Ok(controller)
}

#[tokio::test]
#[ignore = "subprocess only: isolated existing compiled fixture; finished _exit or SIGKILL during actual native SDK work"]
async fn actual_compiled_cache_restart_child() -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    assert_eq!(std::env::var("ELIXIR_TEST_COMPILED_IMAGE_CACHE")?, "1");
    let root = std::path::PathBuf::from(
        std::env::var_os("ELIXIR_TEST_CACHE_RESTART_ROOT")
            .context("explicit isolated restart fixture root required")?,
    )
    .canonicalize()?;
    assert!(root.join("retained-build.sqlite").is_file());
    let home: Uuid = std::env::var("ELIXIR_TEST_CACHE_RESTART_HOME")?.parse()?;
    let id: ExtensionId = std::env::var("ELIXIR_TEST_CACHE_RESTART_EXTENSION")?.parse()?;
    let instance: Uuid = std::env::var("ELIXIR_TEST_CACHE_RESTART_INSTANCE")?.parse()?;
    let deployment = std::env::var("ELIXIR_TEST_CACHE_RESTART_DEPLOYMENT")?;
    let phase = match std::env::var("ELIXIR_TEST_CACHE_RESTART_PHASE")?.as_str() {
        "finished" => RestartPhase::Finished,
        "active" => RestartPhase::Active,
        _ => bail!("explicit supported child restart phase required"),
    };
    let worker = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let runtime = crate::marketplace::quarantine::build::fixture_runtime(true, true)?;
    let db = grant_tests::connect(&root.join("retained-build.sqlite")).await?;
    let config = compiled_fixture_configuration(
        &db,
        runtime.clone(),
        &root,
        &root.join("lane"),
        &worker,
        &deployment,
    )?;
    let mut controller = InstalledController::open(
        db.clone(),
        SecretsManager::from_key_bytes([93; 32], false),
        config,
    )
    .await?;
    let binding = InstalledBinding::load(&db, home, &id, instance, standing_tests::limit()).await?;
    let arguments = if phase == RestartPhase::Active {
        RuntimeJson::from_value(json!({"crash":true}))?
    } else {
        RuntimeJson::parse(b"null")?
    };
    controller.begin(binding, limits(), arguments)?;
    let observation = if phase == RestartPhase::Active {
        wait_key(&db, instance, "crash_started").await?;
        assert!(controller.require_idle().is_err());
        // The slot is actually borrowed by the live call; do NOT acquire its
        // mutex/snapshot or mint provenance from this cleanup-journal data.
        let path = root.join("lane/compiled-image-cache/image-cache.request");
        assert!(
            std::fs::metadata(&path)?.len()
                <= elixir_marketplace::container_image::reference::MAX_REQUEST_BYTES as u64
        );
        let request = elixir_marketplace::container_image::reference::ReferenceRequest::parse(
            &std::fs::read(path)?,
        )?;
        runtime
            .inspect_marketplace_import_image(request.image.config)
            .await?;
        RestartObservation {
            image: request.image.config,
            operation: request.operation_id,
            pid: std::process::id(),
            active: true,
        }
    } else {
        assert_eq!(controller.finish().await?.as_str(), "true");
        controller.require_idle()?;
        let snapshot = controller
            .image_cache_snapshot()
            .await?
            .context("child has no actual image reference")?;
        assert!(
            cached_input_state(runtime.as_ref(), &root.join("lane"))
                .await?
                .is_some()
        );
        runtime.inspect_marketplace_import_image(snapshot.0).await?;
        RestartObservation {
            image: snapshot.0,
            operation: snapshot.1,
            pid: std::process::id(),
            active: false,
        }
    };
    let ready = phase.marker(&root);
    assert!(!ready.exists());
    let pending = ready.with_extension("next");
    let mut marker = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)?;
    marker.write_all(&canonical::to_vec(&observation)?)?;
    marker.sync_all()?;
    std::fs::rename(pending, ready)?; // publish complete bytes atomically
    std::fs::File::open(&root)?.sync_all()?;
    if phase == RestartPhase::Active {
        println!(
            "compiled image-cache child: native SDK crash marker committed; active call retained; waiting for parent SIGKILL"
        );
        std::io::stdout().flush()?;
        std::future::pending::<()>().await;
        controller.require_idle()?;
        unreachable!("active crash child must be killed, not gracefully finished");
    }
    println!(
        "compiled image-cache child: actual SDK call and native retirement completed; exiting without Drop"
    );
    std::io::stdout().flush()?;
    unsafe { libc::_exit(0) }
}

// Metadata/cleanup observations only, never a source match or runtime ticket.
async fn cached_input_state(runtime: &dyn RuntimeManager, lane: &Path) -> Result<Option<String>> {
    use crate::runtime::model::{
        ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL, SourceCacheState,
    };
    let lease = ControllerLease::acquire(lane)?;
    let deployment = lease.storage_namespace()?;
    crate::marketplace::quarantine::image_reference::require_absent(&lease)?;
    assert!(
        runtime
            .get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}"))
            .await?
            .is_none(),
        "raw cache retained a native process"
    );
    match runtime
        .source_cache_state(
            &format!("elixir-marketplace-source-{deployment}"),
            &std::collections::HashMap::from([
                (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment),
                ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
                ("elixir.marketplace.cache_slot".into(), "source-v1".into()),
            ]),
        )
        .await?
    {
        SourceCacheState::Absent => Ok(None),
        SourceCacheState::Present(Some(key)) => {
            let _: elixir_marketplace::identity::Digest = key.parse()?;
            Ok(Some(key))
        }
        _ => bail!("compiled cache did not provide exactly owned bounded input bytes"),
    }
}

pub(in crate::marketplace) fn compiled_fixture_configuration(
    db: &Database,
    runtime: Arc<dyn RuntimeManager>,
    root: &Path,
    lane: &Path,
    worker: &str,
    deployment: &str,
) -> Result<InstalledRuntime> {
    Ok(InstalledRuntime {
        runtime,
        playback: Arc::new(PlaybackJobManager::new(db.pool.clone(), None)),
        local_http: None,
        application_http: None,
        compiled_toolchains: Some(Arc::new(ApprovedToolchains::from_operator_digests([
            std::env::var("ELIXIR_MARKETPLACE_TOOLCHAIN_IMAGE")?.parse()?,
        ])?)),
        lane: lane.into(),
        archive: root.join("retained-archives"),
        deployment: format!("{deployment}-installed"),
        image: worker.into(),
        source_budget: SourceJobBudget {
            memory_bytes: 256 * MIB,
            cpu_millicores: 200,
            disk_bytes: 512 * MIB,
            input_bytes: 64 * MIB,
            output_bytes: 64 * MIB,
            wall_seconds: 120,
        },
        retention: ArchiveBudget {
            bytes: 64 * MIB,
            releases: 4,
        },
    })
}

// Same explicit SQL enrollment seam as the established SourceJs standing
// fixtures. This is NOT a native lifecycle/ready/activation capability.
async fn enroll_fixture(database: &Database, id: &ExtensionId) -> Result<(Uuid, Uuid)> {
    let mut tx = database.pool.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let ledger = store::lock_budget(&mut tx, standing_tests::limit()).await?;
    let old = store::load(&mut tx, database.driver, id)
        .await?
        .context("compiled fixture journal missing")?;
    let mut journal: Journal =
        canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
    let pending = journal
        .pending
        .take()
        .context("compiled fixture pending source missing")?;
    pending.facts()?;
    let home = journal.home;
    let instance = pending.instance;
    journal.active = Some(pending);
    journal.revision += 1;
    journal.mutation = Uuid::new_v4();
    journal.request = Digest::of(b"synthetic compiled ACTIVE enrollment only");
    store::write(
        &mut tx,
        ledger,
        Some(&old),
        journal,
        old.floor,
        standing_tests::limit(),
    )
    .await?;
    sqlx::query("UPDATE extensions SET enabled=TRUE WHERE extension_id=$1")
        .bind(id.as_str())
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(instance.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((home, instance))
}
async fn revision(database: &Database, instance: Uuid) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(instance.to_string())
    .fetch_one(&database.pool)
    .await?)
}
async fn wait_key(database: &Database, instance: Uuid, key: &str) -> Result<()> {
    timeout(Duration::from_secs(60),async {
        loop {
            let found:i64=sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key=$2")
                .bind(instance.to_string()).bind(key).fetch_one(&database.pool).await?;
            if found==1 {return Ok::<_,anyhow::Error>(());}
            sleep(Duration::from_millis(20)).await;
        }
    }).await.context("actual installed native SDK marker did not commit")?
}
