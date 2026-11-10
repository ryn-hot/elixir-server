//! Actual initialized guard, before protected health/worker admission. No
//! guessed Docker-event race, synthetic cleanup receipt or production changes.
use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Interruption {
    GuardFailure,
    OwnerCancellation,
}

fn listener() -> Result<(UnixListener, PathBuf)> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_TEST_STARTUP_BARRIER").as_deref() == Ok("1"),
        "explicit startup interruption fixture required"
    );
    let path = PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_TEST_HANDOFF_CAPTURE")?);
    let temporary = PathBuf::from(std::env::var("TMPDIR")?).canonicalize()?;
    let parent = path.parent().context("startup barrier parent missing")?;
    let meta = std::fs::symlink_metadata(parent)?;
    ensure!(
        path.is_absolute()
            && parent.canonicalize()? == temporary
            && meta.is_dir()
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.mode() & 0o077 == 0
            && std::fs::symlink_metadata(&path)
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
        "exclusive private startup barrier required"
    );
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    Ok((listener, path))
}

async fn exact_actor(id: &str, deployment: &str, image: &str, isolation: &str) -> Result<()> {
    ensure!(
        crate::runtime::packet_network::immutable_id(id),
        "immutable startup actor required"
    );
    let inspected: Value = serde_json::from_str(&docker(&["inspect", id]).await?)?;
    let rows = inspected
        .as_array()
        .context("startup actor inspection shape")?;
    ensure!(rows.len() == 1, "startup actor ambiguous");
    let actor = &rows[0];
    ensure!(
        actor["Id"] == id
            && actor["Image"] == image
            && actor["State"]["Running"] == true
            && actor["Config"]["Labels"]["elixir.deployment_id"] == deployment
            && actor["Config"]["Labels"]["elixir.managed"] == "true"
            && actor["Config"]["Labels"]["elixir.marketplace.isolation"] == isolation,
        "startup actor ownership/image/state changed"
    );
    Ok(())
}

async fn control(
    listener: UnixListener,
    deployment: String,
    provider: String,
    guard: String,
    shutdown: CancellationToken,
    interruption: Interruption,
) -> Result<usize> {
    let attempts = if interruption == Interruption::GuardFailure {
        2
    } else {
        1
    };
    let mut originals: Option<(String, String)> = None;
    for attempt in 0..attempts {
        let (mut begin, _) =
            tokio::time::timeout(Duration::from_secs(600), listener.accept()).await??;
        ensure!(
            begin.peer_cred()?.uid() == unsafe { libc::geteuid() },
            "startup barrier peer owner changed"
        );
        let mut bytes = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(2),
            (&mut begin).take(2049).read_to_end(&mut bytes),
        )
        .await??;
        ensure!(
            bytes.len() <= 2048 && bytes.last() == Some(&b'\n'),
            "startup begin frame bounds"
        );
        let request: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            request["operation"] == "begin" && request["deployment"] == deployment,
            "startup begin identity changed"
        );
        let parent = request["provider_id"]
            .as_str()
            .context("startup provider missing")?
            .to_owned();
        exact_actor(&parent, &deployment, &provider, "provider-v1").await?;
        if let Some((old_parent, old_guard)) = &originals {
            ensure!(old_parent != &parent, "retry retained failed namespace");
            for id in [old_parent, old_guard] {
                ensure!(
                    docker(&[
                        "ps",
                        "-a",
                        "--quiet",
                        "--no-trunc",
                        "--filter",
                        &format!("id={id}")
                    ])
                    .await?
                    .is_empty(),
                    "retry overlapped original startup retirement"
                );
            }
        }
        begin.write_all(b"ready\n").await?;
        drop(begin);
        let (mut ready, _) =
            tokio::time::timeout(Duration::from_secs(60), listener.accept()).await??;
        ensure!(
            ready.peer_cred()?.uid() == unsafe { libc::geteuid() },
            "startup ready peer owner changed"
        );
        bytes.clear();
        tokio::time::timeout(
            Duration::from_secs(2),
            (&mut ready).take(2049).read_to_end(&mut bytes),
        )
        .await??;
        ensure!(
            bytes.len() <= 2048 && bytes.last() == Some(&b'\n'),
            "startup ready frame bounds"
        );
        let request: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            request["operation"] == "guard_ready"
                && request["deployment"] == deployment
                && request["provider_id"] == parent
                && request["resumed"] == (attempt != 0),
            "original startup/one-shot resume binding changed"
        );
        let child = request["guard_id"]
            .as_str()
            .context("startup guard missing")?
            .to_owned();
        exact_actor(&child, &deployment, &guard, "packet-v1").await?;
        ensure!(
            docker(&["inspect", "--format", "{{.HostConfig.NetworkMode}}", &child]).await?
                == format!("container:{parent}"),
            "startup guard joined another parent"
        );
        // The worker must not exist yet: this is a startup interruption, not
        // another healthy participation-loss test.
        ensure!(
            docker(&[
                "ps",
                "-a",
                "--quiet",
                "--filter",
                &format!("label=elixir.deployment_id={deployment}"),
                "--filter",
                "label=elixir.marketplace.isolation=fixed-v1"
            ])
            .await?
            .is_empty(),
            "startup interruption arrived after worker creation"
        );
        if attempt == 0 {
            if interruption == Interruption::GuardFailure {
                docker(&["kill", "--signal=KILL", &child]).await?;
                originals = Some((parent, child));
            } else {
                shutdown.cancel();
            }
            eprintln!(
                "{}",
                json!({"stage":"startup_interruption_applied",
                "guard_initialized":true,"worker_not_created":true,
                "guard_killed":interruption == Interruption::GuardFailure,
                "owner_cancelled":interruption == Interruption::OwnerCancellation})
            );
        }
        ready.write_all(b"ready\n").await?;
        drop(ready);
    }
    Ok(attempts)
}

async fn run(interruption: Interruption) -> Result<()> {
    for flag in ["WARP_LIFECYCLE", "WAN", "PUBLIC_SOURCE"] {
        ensure!(
            std::env::var(format!("ELIXIR_MARKETPLACE_{flag}_LIVE_TEST")).as_deref() == Ok("1"),
            "explicit approved free-provider/public-source consent required"
        );
    }
    let sources = configured_fixtures()?; // Validate before enrollment.
    let provider = immutable_image("ELIXIR_MARKETPLACE_WARP_PROVIDER_IMAGE")?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let (listener, socket_path) = listener()?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let mut actor = Participant::new(runtime, &provider).await?;
    eprintln!(
        "{}",
        json!({"stage":"startup_interruption_layout","recovery_root":actor.rig.root.path(),
        "deployment":actor.rig.deployment})
    );
    let mut controller = tokio::spawn(control(
        listener,
        actor.rig.deployment.clone(),
        provider,
        guard.clone(),
        actor.shutdown.clone(),
        interruption,
    ));
    let mut controller_result = None;
    let operation = std::panic::AssertUnwindSafe(Box::pin(async {
        let mut allocation = crate::marketplace::application::fixture_allocation();
        allocation.source.cpu_millicores = 200;
        allocation.source.wall_seconds = 600;
        allocation.validate()?;
        let started = {
            let starting = actor.start(
                &guard,
                &worker,
                Digest::of(Uuid::new_v4().as_bytes()),
                false,
                &allocation,
            );
            tokio::pin!(starting);
            tokio::select! {
                result = &mut starting => result,
                result = &mut controller => {
                    let healthy = matches!(&result, Ok(Ok(_)));
                    controller_result = Some(result);
                    ensure!(healthy, "original startup barrier failed");
                    starting.await
                }
            }
        };
        if interruption == Interruption::OwnerCancellation {
            ensure!(started.is_err(), "cancelled startup admitted participation");
            actor.stop().await?;
            ensure!(
                actor.client().is_err(),
                "cancelled startup retained consumer capability"
            );
            ensure!(
                docker(&[
                    "ps",
                    "-a",
                    "--quiet",
                    "--filter",
                    &format!("label=elixir.deployment_id={}", actor.rig.deployment)
                ])
                .await?
                .is_empty(),
                "cancelled startup returned before original native retirement"
            );
        } else {
            started?;
            let client = actor.client()?;
            source_ready(&client).await?;
            let metadata = client.verify_publication(&sources[0].publication).await?;
            ensure!(
                client
                    .preflight_fresh_publication(&metadata, 0)
                    .await?
                    .release_id()
                    == sources[0].matched.release_id,
                "failed-startup recovery did not restore exact fresh protected source"
            );
        }
        Ok::<_, anyhow::Error>(())
    }))
    .catch_unwind()
    .await;
    // Join the original controller, never detach it across provider cleanup.
    // Stop waiting when the operation failed before reaching its barrier.
    if controller_result.is_none() && !matches!(&operation, Ok(Ok(()))) && !controller.is_finished()
    {
        controller.abort();
    }
    let controlled = match controller_result {
        Some(result) => result,
        None => controller.await,
    };
    let cleanup = actor.close().await;
    let _ = std::fs::remove_file(socket_path);
    if cleanup.is_ok() {
        eprintln!(
            "{}",
            json!({"stage":"startup_interruption_original_identity_retired"})
        );
    }
    cleanup?;
    match operation {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    ensure!(
        controlled.context("original startup controller join")??
            == if interruption == Interruption::GuardFailure {
                2
            } else {
                1
            },
        "startup attempt count changed"
    );
    eprintln!(
        "{}",
        json!({"stage":"startup_interruption_verified","same_identity_revoked":true,
        "normal_retry":interruption == Interruption::GuardFailure,
        "cancelled_before_worker":interruption == Interruption::OwnerCancellation})
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "standing-approved one free VPN identity; real initialized-guard failure before worker admission and ordinary retry"]
async fn initialized_guard_failure_before_worker_admission_retries_original_identity() -> Result<()>
{
    run(Interruption::GuardFailure).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "standing-approved one free VPN identity; owner cancellation after guard initialization before worker admission"]
async fn owner_cancellation_after_guard_initialization_joins_original_retirement() -> Result<()> {
    run(Interruption::OwnerCancellation).await
}
