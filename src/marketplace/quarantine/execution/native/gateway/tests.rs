//! Actual host CLI/pipe ownership only, not container/VPN/installed authority.
use super::*;
use crate::runtime::PrivateProcessStdio;
use std::process::Stdio;

fn process(script: &str) -> Result<PrivateProcessStdio> {
    let mut child = tokio::process::Command::new("/bin/sh")
        .args(["-c", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    Ok(PrivateProcessStdio {
        input: child.stdin.take().context("fixture stdin")?,
        output: child.stdout.take().context("fixture stdout")?,
        diagnostics: child.stderr.take(),
        child,
    })
}
#[tokio::test]
async fn gateway_diagnostics_are_discarded_with_a_finite_byte_budget() -> Result<()> {
    for (length, expected) in [(64, true), (65, false)] {
        let (mut writer, reader) = tokio::io::duplex(128);
        let task = tokio::spawn(async move { writer.write_all(&vec![b'x'; length]).await });
        assert_eq!(pipes::discard(reader, 64).await, expected);
        task.await??;
    }
    Ok(())
}
#[tokio::test]
async fn gateway_cancelled_join_wait_retains_original_readers_and_actual_process_status()
-> Result<()> {
    let mut pipes = pipes::Pipes::new(process(
        "printf fixture; printf diagnostic >&2; sleep 0.2; exit 7",
    )?);
    assert!(
        timeout(Duration::from_millis(10), pipes.join())
            .await
            .is_err()
    );
    timeout(Duration::from_secs(5), pipes.join()).await??;
    assert_eq!(pipes.status_code(), Some(7));
    // A joined nonzero exit is retirement bookkeeping, never usable output,
    // successful material acceptance or a healthy native gateway.
    timeout(Duration::from_secs(5), pipes.join()).await??;
    Ok(())
}
