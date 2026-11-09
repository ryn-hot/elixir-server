//! Real local pipes/children. Not OCI identity, VPN, SDK-grant or resource proof.
use super::*;
use std::process::Stdio;

fn fixture(script: &str) -> Result<(Owner, Arc<Tracking>)> {
    let mut child = tokio::process::Command::new("/bin/sh")
        .args(["-c", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let process = PrivateProcessStdio {
        input: child.stdin.take().context("fixture stdin")?,
        output: child.stdout.take().context("fixture stdout")?,
        diagnostics: child.stderr.take(),
        child,
    };
    let pending = Arc::new(Tracking::new());
    pending.start()?;
    Ok((
        Owner {
            stage: Stage::Connected(Box::new(Pipes::new(process))),
            tracking: pending.clone(),
        },
        pending,
    ))
}

#[tokio::test]
async fn one_shot_buffered_reply_joins_without_sending_a_stream_close_or_replaying_input()
-> Result<()> {
    let (mut owner, pending) = fixture(
        "dd bs=1 count=6 2>/dev/null >/dev/null; printf '\\000\\000\\000\\002okELX_HTTP_DONE_1\\n\\000'",
    )?;
    assert_eq!(owner.exchange(b"{}").await?.as_slice(), b"ok");
    assert!(pending.pending(), "a response is not a native join");
    assert_eq!(owner.retire().await?, Some(true));
    assert!(!pending.pending());
    assert!(owner.exchange(b"{}").await.is_err());
    assert_eq!(owner.retire().await?, Some(true));
    Ok(())
}

#[tokio::test]
async fn framed_reply_and_native_marker_require_the_original_pipe_and_child_joins() -> Result<()> {
    let (mut owner, pending) = fixture(
        "printf '\\000\\000\\000\\002ok'; cat >/dev/null; printf 'ELX_HTTP_DONE_1\\n\\000'",
    )?;
    assert_eq!(owner.exchange(b"{}").await?.as_slice(), b"ok");
    assert!(pending.pending());
    assert_eq!(owner.retire().await?, Some(true));
    assert!(!pending.pending());
    assert!(owner.exchange(b"{}").await.is_err());
    assert_eq!(
        owner.retire().await?,
        Some(true),
        "same joined completion, no new process"
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_partial_frame_poisoning_cannot_retry_or_discard_retirement_bytes() -> Result<()>
{
    let (mut owner, pending) =
        fixture("printf '\\000\\000'; cat >/dev/null; printf 'ELX_HTTP_DONE_1\\n\\000'")?;
    assert!(
        timeout(Duration::from_millis(30), owner.exchange(b"{}"))
            .await
            .is_err()
    );
    assert!(
        owner.exchange(b"{}").await.is_err(),
        "partial write/read is not a new request"
    );
    assert!(pending.pending());
    assert_eq!(
        owner.retire().await?,
        Some(false),
        "joined cancellation is not a usable reply"
    );
    assert!(!pending.pending());
    Ok(())
}

#[tokio::test]
async fn cancelled_join_waiter_retains_same_child_and_original_eof_progress() -> Result<()> {
    let (mut owner, pending) =
        fixture("cat >/dev/null; sleep 0.15; printf 'ELX_HTTP_DONE_1\\n\\000'")?;
    assert!(
        timeout(Duration::from_millis(20), owner.retire())
            .await
            .is_err()
    );
    assert!(pending.pending());
    assert!(matches!(owner.stage, Stage::Connected(_)));
    assert_eq!(owner.retire().await?, Some(true));
    assert!(!pending.pending());
    Ok(())
}

#[tokio::test]
async fn failed_marker_exit_and_diagnostics_allow_cleanup_but_never_a_successful_reply()
-> Result<()> {
    for script in [
        "cat >/dev/null", // no native marker
        "cat >/dev/null; printf 'ELX_HTTP_DONE_1\\n\\000'; exit 9",
        "cat >/dev/null; head -c 1025 /dev/zero >&2; printf 'ELX_HTTP_DONE_1\\n\\000'",
        "cat >/dev/null; printf 'extraELX_HTTP_DONE_1\\n\\000'",
    ] {
        let (mut owner, pending) = fixture(script)?;
        assert_eq!(owner.retire().await?, Some(false));
        assert!(!pending.pending());
    }
    Ok(())
}

#[tokio::test]
async fn input_and_frame_caps_are_checked_before_allocating_or_resuming_an_exchange() -> Result<()>
{
    let (mut owner, _) = fixture("printf '\\177\\377\\377\\377'; cat >/dev/null")?;
    assert!(owner.exchange(b"").await.is_err());
    assert!(
        owner
            .exchange(&vec![0; MAX_STREAM_COMMAND + 1])
            .await
            .is_err()
    );
    assert!(
        owner.exchange(b"{}").await.is_err(),
        "oversized stdout frame must not allocate its advertised length"
    );
    assert_eq!(owner.retire().await?, Some(false));
    Ok(())
}

#[tokio::test]
async fn early_native_exit_is_not_a_response_frame_or_successful_retirement() -> Result<()> {
    for code in ["000", "001"] {
        let (mut owner, pending) = fixture(&format!(
            "printf 'ELX_HTTP_DONE_1\\n\\{code}'; cat >/dev/null"
        ))?;
        let error = owner.exchange(b"{}").await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "application HTTP native process ended before its framed reply"
        );
        assert!(pending.pending());
        assert!(owner.exchange(b"{}").await.is_err());
        assert_eq!(owner.retire().await?, Some(false));
        assert!(!pending.pending());
    }
    Ok(())
}

#[tokio::test]
async fn losing_a_pipe_owner_never_sets_its_joined_flag() -> Result<()> {
    let (owner, pending) = fixture("cat >/dev/null")?;
    drop(owner);
    assert!(
        pending.pending(),
        "destructors cannot mint retirement proof"
    );
    // The fixture's killed/reaped local child is not claimed as a native join.
    tokio::task::yield_now().await;
    Ok(())
}

#[test]
fn cleanup_seal_and_new_attachment_cannot_both_win_the_same_original_gate() {
    for _ in 0..100 {
        let tracking = Arc::new(Tracking::new());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a = tracking.clone();
        let entered = barrier.clone();
        let attaching = std::thread::spawn(move || {
            entered.wait();
            a.start().is_ok()
        });
        let b = tracking.clone();
        let retiring = std::thread::spawn(move || {
            barrier.wait();
            b.seal_for_retirement()
        });
        let started = attaching.join().unwrap();
        let sealed = retiring.join().unwrap();
        assert_ne!(started, sealed, "no untracked attachment or false cleanup");
        if started {
            assert!(tracking.pending());
            tracking.complete().unwrap();
            assert!(tracking.seal_for_retirement());
        }
        assert!(
            tracking.start().is_err(),
            "retirement is not a new attachment epoch"
        );
    }
}
