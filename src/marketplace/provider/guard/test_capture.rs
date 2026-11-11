//! Test-only pre-handoff barrier. Transport addresses travel over a private
//! same-user Unix socket, never stdout, persisted fixture data or packet dumps.
use super::Owner;
use crate::runtime::provider_observation::ProviderTransport;
use anyhow::{Context, Result, ensure};
use std::{
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

const ENV: &str = "ELIXIR_MARKETPLACE_TEST_HANDOFF_CAPTURE";

fn configured() -> Result<Option<PathBuf>> {
    let Some(path) = std::env::var_os(ENV) else {
        return Ok(None);
    };
    for flag in ["WARP_LIFECYCLE", "WAN", "PUBLIC_SOURCE"] {
        ensure!(
            std::env::var(format!("ELIXIR_MARKETPLACE_{flag}_LIVE_TEST")).as_deref() == Ok("1"),
            "capture requires explicit isolated live-test consent"
        );
    }
    Ok(Some(path.into()))
}

async fn exchange(path: &Path, value: serde_json::Value, ack: &[u8]) -> Result<()> {
    ensure!(
        path.is_absolute() && path.as_os_str().as_bytes().len() < 100,
        "capture socket path bounds"
    );
    let uid = unsafe { libc::geteuid() };
    let parent = std::fs::symlink_metadata(path.parent().context("capture parent missing")?)?;
    let socket = std::fs::symlink_metadata(path)?;
    ensure!(
        parent.is_dir()
            && parent.uid() == uid
            && parent.permissions().mode() & 0o077 == 0
            && socket.file_type().is_socket()
            && socket.uid() == uid
            && socket.permissions().mode() & 0o077 == 0,
        "capture requires a private same-user socket and directory"
    );
    let mut frame = serde_json::to_vec(&value)?;
    ensure!(
        frame.len() <= 2048 && !frame.contains(&b'\n'),
        "capture frame bounds"
    );
    frame.push(b'\n');
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut stream = UnixStream::connect(path)
            .await
            .context("capture connection unavailable")?;
        ensure!(
            stream.peer_cred()?.uid() == uid,
            "capture peer owner changed"
        );
        stream.write_all(&frame).await?;
        stream.shutdown().await?;
        let mut received = Vec::new();
        (&mut stream).take(16).read_to_end(&mut received).await?;
        ensure!(
            received == ack,
            "capture observer did not acknowledge exact operation"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("capture readiness deadline")?
}

pub(in crate::marketplace) async fn begin(
    owner: &Owner,
    transport: &ProviderTransport,
) -> Result<()> {
    let Some(path) = configured()? else {
        return Ok(());
    };
    exchange(
        &path,
        serde_json::json!({"operation":"begin",
        "deployment":owner.profile.intent().deployment,"provider_id":owner.handle.id,
        "namespace_inode":transport.namespace_inode,"address":transport.endpoint.address,
        "local_port":transport.endpoint.local_port,"remote_port":transport.endpoint.port}),
        b"ready\n",
    )
    .await
}

pub(in crate::marketplace) async fn finish() -> Result<()> {
    let path = configured()?.context("explicit pre-handoff capture required")?;
    exchange(&path, serde_json::json!({"operation":"end"}), b"done\n").await
}

/// Test-only observation before the original actor's private Start frame.
/// Resume has a separate opt-in. Retirement and revocation never wait here.
pub(in crate::marketplace) async fn provider_prestart(
    deployment: &str,
    provider_id: &str,
    resume: bool,
) -> Result<()> {
    let flag = if resume {
        "ELIXIR_MARKETPLACE_TEST_PROVIDER_RESUME_PRESTART"
    } else {
        "ELIXIR_MARKETPLACE_TEST_PROVIDER_PRESTART"
    };
    if std::env::var(flag).as_deref() != Ok("1") {
        return Ok(());
    }
    let path = configured()?.context("explicit bootstrap capture socket required")?;
    // The native actor is already waiting with its existing ten-second
    // startup bound. An absent observer fails without writing the Start frame.
    tokio::time::timeout(
        std::time::Duration::from_secs(7),
        exchange(
            &path,
            provider_prestart_frame(deployment, provider_id, resume),
            b"ready\n",
        ),
    )
    .await
    .context("pre-Start capture readiness deadline")?
}

fn provider_prestart_frame(deployment: &str, provider_id: &str, resume: bool) -> serde_json::Value {
    let operation = if resume {
        "provider_resume_prestart"
    } else {
        "provider_prestart"
    };
    serde_json::json!({"operation":operation,"deployment":deployment,"provider_id":provider_id})
}

/// Bind the exact still-live namespace before an isolated route-loss fault.
pub(in crate::marketplace) async fn route_loss() -> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_ROUTE_LOSS_CAPTURE").as_deref() == Ok("1"),
        "explicit isolated route-loss capture required"
    );
    let path = configured()?.context("explicit route-loss socket required")?;
    exchange(
        &path,
        serde_json::json!({"operation":"route_loss"}),
        b"ready\n",
    )
    .await
}

// Only the separate startup-interruption fixture enables this second barrier.
// Existing packet-capture runs keep their original two-operation protocol.
pub(super) async fn guard_ready(
    deployment: &str,
    provider_id: &str,
    guard_id: &str,
    namespace_inode: u64,
    resumed: bool,
) -> Result<()> {
    if std::env::var("ELIXIR_MARKETPLACE_TEST_STARTUP_BARRIER").as_deref() != Ok("1") {
        return Ok(());
    }
    let path = configured()?.context("explicit startup-interruption socket required")?;
    exchange(
        &path,
        guard_ready_frame(deployment, provider_id, guard_id, namespace_inode, resumed),
        b"ready\n",
    )
    .await
}

fn guard_ready_frame(
    deployment: &str,
    provider_id: &str,
    guard_id: &str,
    namespace_inode: u64,
    resumed: bool,
) -> serde_json::Value {
    serde_json::json!({"operation":"guard_ready", "deployment":deployment,
        "provider_id":provider_id,"guard_id":guard_id,"namespace_inode":namespace_inode,"resumed":resumed})
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    #[test]
    fn bootstrap_start_frames_bind_only_the_owned_actor_and_start_mode() {
        let id = "a".repeat(64);
        for (resume, operation) in [
            (false, "provider_prestart"),
            (true, "provider_resume_prestart"),
        ] {
            assert_eq!(
                provider_prestart_frame("fixture", &id, resume),
                serde_json::json!({"operation":operation,"deployment":"fixture","provider_id":id})
            );
        }
    }

    #[test]
    fn startup_ready_frame_is_closed_and_contains_no_transport_address() {
        for resumed in [false, true] {
            let frame =
                guard_ready_frame("fixture", &"a".repeat(64), &"b".repeat(64), 12345, resumed);
            let mut fields: Vec<_> = frame
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            fields.sort();
            assert_eq!(
                fields,
                [
                    "deployment",
                    "guard_id",
                    "namespace_inode",
                    "operation",
                    "provider_id",
                    "resumed"
                ]
            );
            assert_eq!(frame["resumed"], resumed);
            assert_eq!(frame["operation"], "guard_ready");
        }
    }

    async fn roundtrip(ack: &'static [u8], private: bool) -> Result<bool> {
        let root = tempfile::Builder::new()
            .prefix("elx-handoff-")
            .tempdir_in("/tmp")?;
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))?;
        let path = root.path().join("c.sock");
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(
            &path,
            std::fs::Permissions::from_mode(if private { 0o600 } else { 0o666 }),
        )?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            (&mut stream).take(2049).read_to_end(&mut bytes).await?;
            ensure!(
                bytes == b"{\"operation\":\"end\"}\n",
                "unexpected test frame"
            );
            stream.write_all(ack).await?;
            Ok::<_, anyhow::Error>(())
        });
        let result = exchange(&path, serde_json::json!({"operation":"end"}), b"done\n").await;
        if private {
            server.await??;
        } else {
            server.abort();
            let _ = server.await;
        }
        Ok(result.is_ok())
    }

    #[tokio::test]
    async fn private_socket_requires_exact_ack_and_rejects_open_permissions() -> Result<()> {
        assert!(roundtrip(b"done\n", true).await?);
        assert!(!roundtrip(b"ready\n", true).await?);
        assert!(!roundtrip(b"done\nextra", true).await?);
        assert!(!roundtrip(b"done\n", false).await?);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_wait_is_bounded_and_joins_original_connection() -> Result<()> {
        let root = tempfile::Builder::new()
            .prefix("elx-handoff-")
            .tempdir_in("/tmp")?;
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))?;
        let path = root.path().join("c.sock");
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut frame = Vec::new();
            (&mut stream).take(2049).read_to_end(&mut frame).await?;
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            Ok::<_, anyhow::Error>(())
        });
        assert!(
            exchange(&path, serde_json::json!({"operation":"end"}), b"done\n")
                .await
                .is_err()
        );
        server.abort();
        let _ = server.await;
        Ok(())
    }
}
