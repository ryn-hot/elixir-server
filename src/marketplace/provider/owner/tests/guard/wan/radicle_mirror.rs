//! Isolated fixed-Iris checks, not a general proxy or standing seed. Reuse
//! the native VPN owner, worker, quotas and original-identity cleanup.
use super::*;
use std::{io::Read, os::unix::fs::OpenOptionsExt, path::Path};

const BRIDGE_SHA256: &str =
    "sha256:3c2c7f49601a3b0d3d859170c2d78544bf75ac9acf00e21134f293813e6120ef";
const PUBLISH_RUNNER_SHA256: &str =
    "sha256:6e3c303f1cbfbc7724eac60e3ac07f9d4dc27006eda7f63d9c64b16722e184e4";
const MAX_SCRIPT_BYTES: usize = 16 * 1024;
const DIAL: &str = r#"
const s=await dialIris(()=>connect({path:'/tmp/elixir-onion/socks.sock',allowHalfOpen:true}),AbortSignal.timeout(120000));
s.destroy();
process.stdout.write('protected_iris_tor_connect\n');
"#;

fn exact_script(path: &Path, digest: &str) -> Result<String> {
    ensure!(
        path.is_absolute(),
        "explicit absolute bridge fixture path required"
    );
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "regular bridge fixture required"
    );
    let mut bytes = Vec::new();
    file.take(MAX_SCRIPT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_SCRIPT_BYTES && Digest::of(&bytes).to_string() == digest,
        "fixture script does not match the qualified fixed Iris check"
    );
    Ok(String::from_utf8(bytes)?)
}

enum IrisRun {
    Route(String),
    Publish { script: String, fixture: PathBuf },
}

#[tokio::test]
#[ignore = "one standing-approved free temporary identity; fixed Iris onion CONNECT only; no source, publication or seed role"]
async fn isolated_iris_onion_connect_uses_actual_protected_tor() -> Result<()> {
    let mut script = exact_script(
        Path::new(&std::env::var("ELIXIR_MARKETPLACE_IRIS_BRIDGE")?),
        BRIDGE_SHA256,
    )?;
    script.push_str(DIAL);
    run("IRIS_MIRROR_ROUTE", IrisRun::Route(script)).await?;
    eprintln!(
        "{{\"stage\":\"protected_iris_route_verified\",\"payload_bytes\":0,\"publications\":0,\"same_original_identity_revoked\":true}}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit approval: two pinned synthetic source commits on one third-party seed, one free temporary identity, no Elixir seed"]
async fn isolated_iris_mirror_publishes_only_approved_synthetic_sources() -> Result<()> {
    let runner = PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_IRIS_PUBLISH_RUNNER")?);
    let script = exact_script(&runner, PUBLISH_RUNNER_SHA256)?;
    let fixture = runner
        .parent()
        .context("private fixture directory")?
        .to_owned();
    ensure!(
        fixture.canonicalize()? == fixture && !fixture.as_os_str().is_empty(),
        "exact private fixture path required"
    );
    run("IRIS_MIRROR_PUBLISH", IrisRun::Publish { script, fixture }).await?;
    eprintln!(
        "{{\"stage\":\"protected_iris_publication_retired\",\"same_original_identity_revoked\":true}}"
    );
    Ok(())
}

async fn run(consent: &str, run: IrisRun) -> Result<()> {
    for consent in ["WARP_LIFECYCLE", "WAN", consent] {
        ensure!(
            std::env::var(format!("ELIXIR_MARKETPLACE_{consent}_LIVE_TEST")).as_deref() == Ok("1"),
            "explicit one-shot protected Iris fixture consent required"
        );
    }
    let provider = image()?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&image).await?;
    for (name, expected) in [
        ("onion_transport", "2"),
        ("onion_fixture", "0"),
        ("onion_publication_ready", "0"),
    ] {
        ensure!(
            labels
                .get(&format!("elixir.marketplace.worker.{name}"))
                .map(String::as_str)
                == Some(expected),
            "immutable private onion worker required"
        );
    }
    let mut rig = Rig::new(runtime, &provider).await?;
    let outcome = std::panic::AssertUnwindSafe(async {
        rig.open(&guard, &image).await?;
        let worker = rig.worker()?;
        ready(worker).await?;
        let status = worker
            .start_network(config(Digest::of(uuid::Uuid::new_v4().as_bytes()), false)?)
            .await?;
        ensure!(
            status.active && !status.catalog && !status.routing && !status.adaptive,
            "only consumer networking may run"
        );
        let id = &worker.handle().id;
        ensure!(
            crate::runtime::packet_network::immutable_id(id),
            "exact native worker ID required"
        );
        let namespace = worker
            .profile()
            .container_spec()
            .network_mode
            .as_deref()
            .filter(|mode| {
                mode.strip_prefix("container:")
                    .is_some_and(crate::runtime::packet_network::immutable_id)
            })
            .context("owned provider namespace required")?;
        ensure!(
            docker(&[
                "inspect",
                "--format",
                "{{.Id}} {{.Image}} {{.State.Running}} {{.Config.User}} {{.HostConfig.NetworkMode}} {{.HostConfig.Privileged}} {{.HostConfig.ReadonlyRootfs}}",
                id
            ])
            .await?
                == format!("{id} {image} true 10004:10004 {namespace} false true"),
            "probe is not attached to the owned protected worker"
        );
        ready(worker).await?;
        match &run {
            IrisRun::Route(script) => {
                let result = timeout(
                    Duration::from_secs(125),
                    tokio::process::Command::new("docker")
                        .args([
                            "exec", "--user=10004:10004", id, "/usr/local/bin/node",
                            "--input-type=module", "-e", script,
                        ])
                        .kill_on_drop(true)
                        .output(),
                )
                .await
                .context("fixed Iris Tor CONNECT deadline")??;
                ensure!(
                    result.status.success()
                        && result.stdout == b"protected_iris_tor_connect\n"
                        && result.stderr.is_empty(),
                    "fixed Iris Tor CONNECT not witnessed"
                );
            }
            IrisRun::Publish { script, fixture } => {
                // The pinned runner cleans up its exact network-none publisher
                // and loopback proxy before exiting. Stream only its fixed
                // progress receipts so an ambiguous acknowledgement cannot
                // cause an untracked second publication attempt.
                let status = timeout(
                    Duration::from_secs(190),
                    tokio::process::Command::new("/usr/local/bin/node")
                        .args(["--input-type=module", "-e", script, "--", id, &image])
                        .arg(fixture)
                        .stdin(std::process::Stdio::null())
                        .kill_on_drop(true)
                        .status(),
                )
                .await
                .context("bounded Iris publication runner deadline")??;
                ensure!(status.success(), "no exact Iris publication acknowledgement");
            }
        }
        ready(worker).await?;
        Ok::<(), anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("Original protected Iris route operation failed: {error:#}");
    }
    rig.close()
        .await
        .context("retire original protected Iris fixture identity")?;
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    Ok(())
}
