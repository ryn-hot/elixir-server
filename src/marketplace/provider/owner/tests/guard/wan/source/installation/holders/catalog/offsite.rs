//! One isolated participant per operator. Exchange only local test gates;
//! the normal protected catalog carries every listing, package and update.
use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

#[derive(Clone, Copy)]
enum Gate {
    V1Offered,
    V1Active,
    V2Offered,
    V2Active,
}
impl Gate {
    fn name(self) -> &'static str {
        match self {
            Self::V1Offered => "v1-offered",
            Self::V1Active => "v1-active",
            Self::V2Offered => "v2-offered",
            Self::V2Active => "v2-active",
        }
    }
}

struct Run {
    topic: Digest,
    receipt: Digest,
    mailbox: PathBuf,
}
impl Run {
    fn new(nonce: Digest, releases: [Digest; 2], mailbox: PathBuf) -> Result<Self> {
        let meta = std::fs::symlink_metadata(&mailbox)?;
        ensure!(
            mailbox.is_absolute()
                && mailbox.canonicalize()? == mailbox
                && meta.is_dir()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.permissions().mode() & 0o777 == 0o700,
            "explicit private canonical pilot mailbox required"
        );
        ensure!(
            nonce.as_bytes() != &[0; 32] && releases[0] != releases[1],
            "invalid pilot scope"
        );
        let topic = Digest::of(format!("elixir-offsite-desktop-topic-v1\0{nonce}"));
        let [v1, v2] = releases;
        let receipt = Digest::of(format!(
            "elixir-offsite-desktop-receipt-v1\0{nonce}\0{v1}\0{v2}"
        ));
        Ok(Self {
            topic,
            receipt,
            mailbox,
        })
    }
    fn message(&self, gate: Gate) -> String {
        format!("{} {}\n", gate.name(), self.receipt)
    }
    fn emit(&self, gate: Gate) {
        eprint!("marketplace_offsite_gate {}", self.message(gate));
    }
    fn received(&self, gate: Gate) -> Result<bool> {
        let path = self.mailbox.join(format!("{}.txt", gate.name()));
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let expected = self.message(gate);
        let meta = file.metadata()?;
        ensure!(
            meta.is_file()
                && meta.nlink() == 1
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.permissions().mode() & 0o777 == 0o600
                && meta.len() == expected.len() as u64,
            "invalid private pilot gate file"
        );
        let mut actual = Vec::new();
        file.take(expected.len() as u64 + 1)
            .read_to_end(&mut actual)?;
        ensure!(
            actual == expected.as_bytes(),
            "pilot gate does not match this run and release pair"
        );
        Ok(true)
    }
    async fn wait(&self, gate: Gate, deadline: Duration) -> Result<()> {
        timeout(deadline, async {
            while !self.received(gate)? {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .with_context(|| format!("missing original pilot gate: {}", gate.name()))?
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "one consenting independent operator and one free identity; approved synthetic desktop pilot only"]
async fn independent_site_catalog_desktop_install_and_update() -> Result<()> {
    for flag in [
        "OFFSITE_CATALOG_PILOT_TEST",
        "WARP_LIFECYCLE_LIVE_TEST",
        "WAN_LIVE_TEST",
        "PUBLIC_SOURCE_LIVE_TEST",
        "PUBLIC_INSTALL_LIVE_TEST",
    ] {
        ensure!(
            std::env::var(format!("ELIXIR_MARKETPLACE_{flag}")).as_deref() == Ok("1"),
            "explicit synthetic off-site pilot consent required"
        );
    }
    let role = std::env::var("ELIXIR_MARKETPLACE_OFFSITE_PILOT_ROLE")?;
    ensure!(
        matches!(role.as_str(), "catalog" | "consumer"),
        "unknown pilot role"
    );
    let sources = configured_fixtures()?;
    let releases = [sources[0].matched.release_id, sources[1].matched.release_id];
    let run = Run::new(
        std::env::var("ELIXIR_MARKETPLACE_OFFSITE_PILOT_NONCE")?.parse()?,
        releases,
        PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_OFFSITE_PILOT_MAILBOX")?),
    )?;
    let sdk = (role == "catalog")
        .then(sdk::PublisherSdk::configured)
        .transpose()?;
    let executable = (role == "consumer").then(desktop::executable).transpose()?;
    if let Some(sdk) = &sdk {
        sdk.preflight().await?;
    }
    let provider = image()?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&worker).await?;
    for (key, expected) in [
        ("onion_publication_ready", "1"),
        ("onion_fixture", "0"),
        ("onion_transport", "2"),
        ("blob_sharing", "2"),
        ("wan_query_budget", "1"),
    ] {
        ensure!(
            labels
                .get(&format!("elixir.marketplace.worker.{key}"))
                .map(String::as_str)
                == Some(expected),
            "reviewed pilot worker capability differs: {key}"
        );
    }
    ensure!(
        !labels
            .keys()
            .any(|key| key.starts_with("elixir.marketplace.worker.private_")),
        "private diagnostic workers may not leave the local test environment"
    );
    if role == "consumer" {
        // No enrollment until the remote holder has offered v1. This gate
        // is not a catalog card and cannot authorize the installation.
        ensure!(
            run.received(Gate::V1Offered)?,
            "wait for the publisher's exact v1 gate before starting setup"
        );
    }
    let mut allocation = crate::marketplace::application::fixture_allocation();
    allocation.source.cpu_millicores = 200;
    allocation.source.wall_seconds = 600;
    allocation.validate()?;
    let mut actor = Participant::new(runtime, &provider).await?;
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        if let Some(sdk) = sdk.as_ref() {
            actor
                .start_application(&guard, &worker, run.topic, true, &allocation)
                .await?;
            let holder = publish_and_seed(&actor, &sources[0], None, Some(sdk)).await?;
            offer(&actor, &descriptor(&sources[0], 2)?).await?;
            run.emit(Gate::V1Offered);
            run.wait(Gate::V1Active, Duration::from_secs(1200)).await?;
            publish_and_seed(&actor, &sources[1], Some(holder), Some(sdk)).await?;
            offer(&actor, &descriptor(&sources[1], 4)?).await?;
            run.emit(Gate::V2Offered);
            run.wait(Gate::V2Active, Duration::from_secs(600)).await?;
        } else {
            actor
                .start_customer(&guard, &worker, run.topic, &allocation)
                .await?;
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_grants")
                    .fetch_one(&actor.rig.database.pool)
                    .await?;
            ensure!(count == 0, "independent consumer did not start empty");
            desktop::install_and_update(
                executable.as_deref().context("desktop missing")?,
                &actor,
                &sources,
                async {
                    run.emit(Gate::V1Active);
                    run.wait(Gate::V2Offered, Duration::from_secs(600)).await
                },
            )
            .await?;
            run.emit(Gate::V2Active);
        }
        Ok::<_, anyhow::Error>(())
    }))
    .catch_unwind()
    .await;
    let retired = actor.close().await;
    if !outcome.as_ref().is_ok_and(|result| result.is_ok()) || retired.is_err() {
        actor.rig.root.preserve();
    }
    match outcome {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => {
            return Err(error.context(format!("original pilot identity retirement: {retired:?}")));
        }
        Ok(Ok(())) => retired?,
    }
    actor
        .rig
        .root
        .close_completed_catalog(CatalogArchives::Required)?;
    eprintln!(
        "{}",
        json!({"stage":"independent_site_catalog_desktop_receipt", "role":role,
        "run":run.receipt,"images":[provider,guard,worker],"same_original_identity_revoked":true,
        "test_only":true,"production_enabled":false})
    );
    Ok(())
}

#[test]
fn offsite_gates_bind_run_release_order_and_private_files() -> Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    let path = dir.path().canonicalize()?;
    let releases = [Digest::of(b"v1"), Digest::of(b"v2")];
    let nonce = Digest::of(b"fixture run");
    let run = Run::new(nonce, releases, path.clone())?;
    let other = Run::new(nonce, [releases[1], releases[0]], path.clone())?;
    assert_ne!(run.receipt, other.receipt);
    assert!(!run.received(Gate::V1Active)?);
    let gate = path.join("v1-active.txt");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&gate)?;
    file.write_all(run.message(Gate::V1Active).as_bytes())?;
    drop(file);
    assert!(run.received(Gate::V1Active)?);
    assert!(other.received(Gate::V1Active).is_err());
    std::os::unix::fs::symlink(&gate, path.join("v2-active.txt"))?;
    assert!(run.received(Gate::V2Active).is_err());
    Ok(())
}
