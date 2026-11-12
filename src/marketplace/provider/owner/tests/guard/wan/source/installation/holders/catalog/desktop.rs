//! The desktop itself chooses preferences/consent, performs login, and
//! requests installation from a naturally discovered checked catalog card.
use super::*;
use crate::marketplace::application::Phase;
use std::{os::unix::fs::PermissionsExt, process::Stdio};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_LOG: usize = 64 * 1024;

pub(super) fn executable() -> Result<PathBuf> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_DESKTOP_CATALOG_LIVE_TEST").as_deref() == Ok("1"),
        "explicit desktop catalog qualification required"
    );
    let path = PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_DESKTOP_EXECUTABLE")?);
    ensure!(
        path.is_absolute(),
        "absolute desktop test executable required"
    );
    let expected: Digest =
        std::env::var("ELIXIR_MARKETPLACE_DESKTOP_EXECUTABLE_SHA256")?.parse()?;
    ensure!(
        Digest::of(&bounded(&path, 64 * 1024 * 1024)?) == expected,
        "desktop executable does not match the reviewed first-run build"
    );
    Ok(path)
}

async fn read_log(stream: impl tokio::io::AsyncRead + Unpin) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    stream
        .take(MAX_LOG as u64 + 1)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= MAX_LOG, "bounded desktop result required");
    Ok(bytes)
}

fn write_private(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join(name))?;
    file.write_all(bytes)?;
    Ok(())
}

async fn activation(consumer: &Participant, release: Digest) -> Result<Uuid> {
    let service = consumer
        .state()?
        .marketplace
        .fixture_original_installation();
    let principal = consumer
        .principal
        .as_ref()
        .context("original fixture owner missing")?;
    let id = service
        .current_for(principal)?
        .context("desktop operation missing")?;
    let active = service.view_for(principal, id)?;
    ensure!(
        active.phase == Phase::Activated && active.release_id == release,
        "desktop marker does not match the actual activated release"
    );
    service.fixture_join_original(id).await?;
    active
        .configured_instance
        .context("desktop activated no configured instance")
}

async fn wait_for_update(directory: &Path) -> Result<()> {
    loop {
        let path = directory.join("progress.jsonl");
        if let Ok(bytes) = bounded(&path, 16 * 1024) {
            let rows = bytes.split_inclusive(|byte| *byte == b'\n');
            for row in rows.filter(|row| row.ends_with(b"\n")) {
                let progress: Value = serde_json::from_slice(row)?;
                if progress["stage"] == "catalog_update_requested" && progress["sample"] == 1 {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

pub(super) async fn install_and_update(
    executable: &Path,
    consumer: &Participant,
    sources: &[SourceFixture],
    offer_update: impl Future<Output = Result<()>>,
) -> Result<()> {
    let email: String = sqlx::query_scalar("SELECT email FROM users WHERE id=$1")
        .bind(
            consumer
                .principal
                .as_ref()
                .context("original fixture owner missing")?
                .user_id
                .to_string(),
        )
        .fetch_one(&consumer.rig.database.pool)
        .await?;
    let artifacts = tempfile::Builder::new()
        .prefix("elixir-marketplace-customer-catalog.")
        .tempdir()?;
    std::fs::set_permissions(artifacts.path(), std::fs::Permissions::from_mode(0o700))?;
    let artifacts = artifacts.keep().canonicalize()?;
    eprintln!(
        "Desktop catalog qualification artifacts: {}",
        artifacts.display()
    );
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let release = &sources[0].publication.release.payload;
    let input = serde_json::to_vec(&json!({
        "schema_version":1,"fixture_only":true,"purpose":"installation","provider_setup_required":true,
        "server_url":format!("http://{address}"),"email":email,"password":"fixture-password-only",
        "capture_directory":artifacts,"expected":[{"extension_id":release.extension_id,
            "release_id":sources[0].matched.release_id,"name":release.display_name}],
        "updates":sources.iter().map(|source| json!({"publication":source.publication,
            "release_id":source.matched.release_id})).collect::<Vec<_>>()
    }))?;
    ensure!(input.len() <= 160 * 1024, "bounded desktop input required");
    let router = consumer.app()?.clone();
    let mut child = tokio::process::Command::new(executable)
        .arg("protected_customer_first_run_and_catalog_install_update")
        .env("QT_QPA_PLATFORM", "offscreen")
        .env("QT_QUICK_BACKEND", "software")
        .env("QT_FATAL_WARNINGS", "1")
        .env("QTEST_FUNCTION_TIMEOUT", "1100000")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = tokio::spawn(read_log(
        child.stdout.take().context("desktop stdout missing")?,
    ));
    let stderr = tokio::spawn(read_log(
        child.stderr.take().context("desktop stderr missing")?,
    ));
    let stop = CancellationToken::new();
    let cancel = stop.clone();
    let mut http = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await
    });
    let operation = timeout(Duration::from_secs(1110), async {
        let mut stdin = child.stdin.take().context("desktop fixture stdin missing")?;
        timeout(Duration::from_secs(10), stdin.write_all(&input)).await??;
        stdin.shutdown().await?;
        drop(stdin);
        let advance = async {
            wait_for_update(&artifacts).await?;
            let instance = activation(consumer, sources[0].matched.release_id).await?;
            // No v2 offer before independent verification of actual v1 activation.
            offer_update.await?;
            write_private(&artifacts, "catalog-update-ready.json",
                br#"{"schema_version":1,"fixture_only":true,"version_index":1}"#)?;
            Ok::<_, anyhow::Error>(instance)
        };
        let instance = tokio::select! {
            status = child.wait() => anyhow::bail!("original desktop exited before v2 offer: {}", status?),
            instance = advance => instance?,
        };
        ensure!(child.wait().await?.success(), "original first-run desktop failed");
        ensure!(activation(consumer, sources[1].matched.release_id).await? == instance,
            "desktop update replaced the original configured instance");
        consumer.roles(false, 1).await?;
        Ok::<_, anyhow::Error>(())
    }).await;
    let retirement = async {
        if child.try_wait()?.is_none() {
            child.start_kill()?;
            timeout(Duration::from_secs(10), child.wait()).await??;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    stop.cancel();
    let joined = timeout(Duration::from_secs(10), &mut http).await;
    if joined.is_err() {
        http.abort();
        let _ = http.await;
    }
    let out = timeout(Duration::from_secs(10), stdout).await;
    let err = timeout(Duration::from_secs(10), stderr).await;
    if let Ok(Ok(Ok(bytes))) = &out {
        write_private(&artifacts, "desktop-stdout.txt", bytes)?;
    }
    if let Ok(Ok(Ok(bytes))) = &err {
        write_private(&artifacts, "desktop-stderr.txt", bytes)?;
    }
    retirement?;
    joined???;
    operation??;
    let bytes = out???;
    err???;
    let mut results = std::str::from_utf8(&bytes)?
        .lines()
        .filter_map(|line| line.strip_prefix("ELIXIR_MARKETPLACE_INSTALLATION_UI_RESULT "));
    let proof: Value = serde_json::from_str(
        results
            .next()
            .context("desktop installation receipt missing")?,
    )?;
    ensure!(
        results.next().is_none(),
        "desktop emitted more than one receipt"
    );
    for key in [
        "fixture_only",
        "real_http",
        "normal_login",
        "packaged_qml",
        "same_instance",
        "catalog_cards_used",
        "customer_first_run_completed",
        "setup_query_reused_for_install",
    ] {
        ensure!(
            proof[key] == true,
            "desktop qualification did not prove {key}"
        );
    }
    ensure!(
        proof["versions_activated"] == 2
            && proof["developer_reference_used"] == false
            && proof["automatic_opt_in_selected"] == false,
        "desktop installation scope differs"
    );
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_desktop_catalog_qualified","ui":proof,
        "real_paid_credentials":0,"consumer_source_uploads":0,"production_enabled":false})
    );
    Ok(())
}
