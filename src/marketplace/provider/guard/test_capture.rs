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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

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
