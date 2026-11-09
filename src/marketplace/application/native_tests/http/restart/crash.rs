//! Actual SIGKILL after a joined source call has parked bytes, not a fabricated
//! cleanup result or a clean shutdown. Used only by the existing private child.
use super::*;
use std::{
    io::Read,
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt},
        process::ExitStatusExt,
    },
};

pub(super) const NONCE_ENV: &str = "ELIXIR_TEST_MARKETPLACE_CACHE_CRASH_NONCE";
const MARKER: &str = "source-cache-crash.json";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    schema_version: u32,
    nonce: Uuid,
    child_pid: u32,
}

pub(super) fn lane(root: &Path) -> std::path::PathBuf {
    root.join("marketplace")
        .join("worker")
        .join("foreground-source")
}

async fn existing_database(url: &str) -> Result<Database> {
    // The surrounding fixture supplies its already migrated, private database.
    // This function never migrates, enrolls, repairs or seeds anything.
    Database::connect(&DatabaseConfig {
        url: url.to_owned(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await
}

fn read_marker(root: &Path) -> Result<Marker> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root.join(MARKER))?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file()
            && metadata.nlink() == 1
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0
            && metadata.len() <= 256,
        "invalid private cache-crash marker"
    );
    let mut bytes = Vec::new();
    (&mut file).take(257).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= 256, "cache-crash marker grew");
    Ok(serde_json::from_slice(&bytes)?)
}

pub(super) async fn qualify(root: &Path, url: &str, image: &str) -> Result<()> {
    let runtime = DockerRuntimeManager::new(None);
    require_idle_byte_workspace(&runtime, &lane(root), false).await?;
    let database = existing_database(url).await?;
    let before = sdk_rows(&database).await?;
    database.pool.close().await;
    anyhow::ensure!(
        !root.join(MARKER).exists(),
        "crash fixture cannot reuse an old marker"
    );
    let nonce = Uuid::new_v4();
    // Retain and join this ORIGINAL child; no observation timeout/replacement.
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .arg("marketplace::application::native_tests::http::restart::native_offline_restart_child")
        .args(["--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env("ELIXIR_TEST_MARKETPLACE_RESTORE_ROOT", root)
        .env("ELIXIR_TEST_MARKETPLACE_RESTORE_DATABASE_URL", url)
        .env("ELIXIR_MARKETPLACE_WORKER_IMAGE", image)
        .env(NONCE_ENV, nonce.to_string())
        .kill_on_drop(false)
        .spawn()?;
    let pid = child
        .id()
        .context("original cache-crash child identity missing")?;
    let status = child.wait().await?;
    anyhow::ensure!(
        status.signal() == Some(libc::SIGKILL),
        "original cache child did not reach SIGKILL"
    );
    let marker = read_marker(root)?;
    anyhow::ensure!(
        marker.schema_version == 1 && marker.nonce == nonce && marker.child_pid == pid,
        "SIGKILL did not follow the original verified cache checkpoint"
    );
    // This acquires the actual OS lease after the killed process. It also
    // positively checks owned cache metadata and absence of a native writer.
    require_idle_byte_workspace(&runtime, &lane(root), true).await?;
    let database = existing_database(url).await?;
    let after = sdk_rows(&database).await;
    database.pool.close().await;
    anyhow::ensure!(after? == before, "process crash changed durable SDK state");
    Ok(())
}

/// Call ONLY after actual call/HTTP retirement, provider health and SDK snapshot
/// checks in the original child. A clean child has no nonce and returns normally.
pub(super) async fn exit_if_requested(root: &Path) -> Result<()> {
    let Some(value) = std::env::var_os(NONCE_ENV) else {
        return Ok(());
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("invalid crash nonce encoding"))?;
    let nonce = Uuid::parse_str(&value)?;
    anyhow::ensure!(
        !nonce.is_nil() && nonce.to_string() == value,
        "invalid crash nonce"
    );
    require_idle_byte_workspace(&DockerRuntimeManager::new(None), &lane(root), true).await?;
    let marker = Marker {
        schema_version: 1,
        nonce,
        child_pid: std::process::id(),
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root.join(MARKER))?;
    file.write_all(&serde_json::to_vec(&marker)?)?;
    file.sync_all()?;
    std::fs::File::open(root)?.sync_all()?;
    // Kill this process ONLY, without Drop handlers, abort/core dump, native
    // cleanup or an inferred successful test result. Parent verifies the signal.
    let result = unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    bail!("cache-crash child unexpectedly survived SIGKILL")
}
