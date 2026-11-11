//! Synthetic owner/typed replies only, not OS protection or native quarantine.
use super::*;
use crate::marketplace::blob_download::Reader;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use elixir_marketplace::{
    identity::Digest,
    records::Blob,
    worker_blob::{BLOB_REPLY_ALLOWANCE, BLOB_REQUEST_ALLOWANCE, BlobDownloadSpec},
};
use tokio::io::AsyncReadExt;

pub(in crate::marketplace) struct Owner {
    pub(in crate::marketplace) client: ParticipationClient,
    requests: mpsc::Receiver<Request>,
    faults: mpsc::Receiver<()>,
    _status: watch::Sender<ParticipationSnapshot>,
    protection: watch::Sender<Snapshot>,
}
pub(in crate::marketplace) fn owner() -> Owner {
    let (requests, receiver) = mpsc::channel(1);
    let (faults, failed) = mpsc::channel(1);
    let (credits, _) = mpsc::channel(1);
    let (status, seen) = watch::channel(ParticipationSnapshot {
        phase: ParticipationPhase::Running,
        applied_roles: Some(NetworkRoles {
            consumer: true,
            catalog: false,
            routing: false,
        }),
        ..Default::default()
    });
    let (protection, observed) = watch::channel(Snapshot {
        state: ProtectionState::Ready,
        until: Some(Instant::now() + Duration::from_secs(3600)),
    });
    sqlx::any::install_default_drivers();
    Owner {
        client: ParticipationClient {
            blob_discovery: false,
            blob_sharing: false,
            database: Database {
                driver: crate::db::DatabaseDriver::Sqlite,
                pool: sqlx::any::AnyPoolOptions::new()
                    .connect_lazy("sqlite::memory:")
                    .unwrap(),
            },
            transfer_requests: requests.clone(),
            local_reads: requests.clone(),
            requests,
            status: seen,
            protection: observed,
            credits,
            source_preflight: None,
            source_jobs: None,
            security_mirror: None,
            faults,
            retirement: retirement::channel().1,
        },
        requests: receiver,
        faults: failed,
        _status: status,
        protection,
    }
}
fn transfer(bytes: &[u8]) -> BlobDownloadSpec {
    BlobDownloadSpec {
        blob: Blob {
            sha256: Digest::of(bytes),
            size: bytes.len() as u64,
        },
        maximum_bytes: bytes.len() as u64,
        timeout_ms: 5000,
        upload_bytes: BLOB_REQUEST_ALLOWANCE,
        download_bytes: BLOB_REPLY_ALLOWANCE + bytes.len() as u64,
    }
}

#[tokio::test(start_paused = true)]
async fn foreground_capacity_and_reply_share_one_deadline_without_a_late_retry() -> Result<()> {
    let mut owner = owner();
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill slot")?;
    let client = owner.client.clone();
    let pending = tokio::spawn(async move {
        let (send, reply) = oneshot::channel();
        client
            .enqueue_after_capacity(Request::Heads(vec![], send), reply)
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(29)).await;
    assert!(!pending.is_finished());
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    let Some(Request::Heads(_, reply)) = owner.requests.recv().await else {
        bail!("missing original head request");
    };
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(
        pending
            .await?
            .unwrap_err()
            .to_string()
            .contains("acknowledgement deadline")
    );
    assert!(reply.is_closed());
    assert!(owner.requests.try_recv().is_err());
    assert!(owner.faults.try_recv().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn foreground_capacity_wait_cancellation_or_owner_loss_never_dispatches_late() -> Result<()> {
    for lose_owner in [false, true] {
        let mut owner = owner();
        let (fill, _held) = oneshot::channel();
        owner
            .client
            .requests
            .try_send(Request::Peers(fill))
            .ok()
            .context("fill slot")?;
        let (send, reply) = oneshot::channel();
        let mut pending = Box::pin(
            owner
                .client
                .enqueue_after_capacity(Request::Heads(vec![], send), reply),
        );
        assert!(timeout(Duration::from_secs(1), &mut pending).await.is_err());
        if lose_owner {
            let mut failed = owner._status.borrow().clone();
            failed.phase = ParticipationPhase::Failed;
            failed.applied_roles = None;
            owner._status.send_replace(failed);
            assert!(matches!(
                owner.requests.recv().await,
                Some(Request::Peers(_))
            ));
            assert!(
                pending
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("not running")
            );
        } else {
            drop(pending);
            assert!(matches!(
                owner.requests.recv().await,
                Some(Request::Peers(_))
            ));
        }
        tokio::task::yield_now().await;
        assert!(owner.requests.try_recv().is_err());
        assert!(owner.faults.try_recv().is_err());
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn foreground_capacity_exhaustion_expires_unsent_and_rejects_unrelated_operations()
-> Result<()> {
    let mut owner = owner();
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill slot")?;
    let (send, reply) = oneshot::channel();
    let error = owner
        .client
        .enqueue_after_capacity(Request::Heads(vec![], send), reply)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("before dispatch"));
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    let (send, reply) = oneshot::channel();
    let error = owner
        .client
        .enqueue_after_capacity(Request::Peers(send), reply)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid capacity-waiting"));
    assert!(owner.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn local_blob_read_pressure_waits_for_one_slot_without_replaying_the_offset() -> Result<()> {
    let mut owner = owner();
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill local slot")?;
    let job = Digest::of(b"one original read");
    let client = owner.client.clone();
    let request = tokio::spawn(async move {
        client
            .control_blob(WorkerControlOperation::BlobRead { job, offset: 7 })
            .await
    });
    tokio::task::yield_now().await;
    assert!(!request.is_finished());
    assert!(owner.faults.try_recv().is_err());
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    let Some(Request::Blob(
        WorkerControlOperation::BlobRead {
            job: actual,
            offset,
        },
        reply,
    )) = owner.requests.recv().await
    else {
        bail!("the one original read must be queued");
    };
    assert_eq!(actual, job);
    assert_eq!(offset, 7);
    reply.send(Ok(BlobReadState::Pending { offset: 7 })).ok();
    assert!(matches!(
        request.await??,
        BlobReadState::Pending { offset: 7 }
    ));
    assert!(owner.requests.try_recv().is_err());
    assert!(owner.faults.try_recv().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn blob_capacity_and_acknowledgement_share_the_original_deadline() -> Result<()> {
    let mut owner = owner();
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill local slot")?;
    let client = owner.client.clone();
    let request = tokio::spawn(async move {
        client
            .control_blob(WorkerControlOperation::BlobRead {
                job: Digest::of(b"budget"),
                offset: 0,
            })
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(29)).await;
    assert!(!request.is_finished());
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    let Some(Request::Blob(_, reply)) = owner.requests.recv().await else {
        bail!("original read missing");
    };
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(
        request
            .await?
            .unwrap_err()
            .to_string()
            .contains("blob acknowledgement deadline")
    );
    assert!(
        reply.is_closed(),
        "timeout closes the original acknowledgement, not a fresh request"
    );
    assert!(owner.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn blob_local_capacity_timeout_never_dispatches_a_late_request() -> Result<()> {
    let mut owner = owner();
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill local slot")?;
    let client = owner.client.clone();
    let request = tokio::spawn(async move {
        client
            .control_blob(WorkerControlOperation::BlobRead {
                job: Digest::of(b"unstarted"),
                offset: 0,
            })
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(
        request
            .await?
            .unwrap_err()
            .to_string()
            .contains("before dispatch")
    );
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    assert!(owner.requests.try_recv().is_err());
    assert!(owner.faults.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn reserved_blob_capacity_never_bypasses_a_changed_original_consumer() -> Result<()> {
    let mut owner = owner();
    let slot = owner
        .client
        .reserve_blob(WorkerControlOperation::BlobRead {
            job: Digest::of(b"no authority from a permit"),
            offset: 0,
        })
        .await?;
    let mut failed = owner._status.borrow().clone();
    failed.phase = ParticipationPhase::Failed;
    failed.applied_roles = None;
    owner._status.send_replace(failed);
    assert!(slot.send().is_err());
    assert!(owner.requests.try_recv().is_err());
    assert!(owner.faults.try_recv().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn permanent_blob_cancel_capacity_loss_remains_unknown_without_a_replacement_cancel()
-> Result<()> {
    let mut owner = owner();
    let mut reader = opened(&mut owner, b"1234").await?;
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill local slot")?;
    let mut cancellation = Box::pin(reader.cancel());
    assert!(
        timeout(Duration::from_secs(1), &mut cancellation)
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(30)).await;
    let original = format!("{:#}", cancellation.await.unwrap_err());
    assert!(original.contains("blob local capacity deadline before dispatch"));
    assert_eq!(owner.faults.recv().await, Some(()));
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    assert_eq!(
        original,
        format!("{:#}", reader.cancel().await.unwrap_err())
    );
    assert!(
        owner.requests.try_recv().is_err(),
        "uncertainty cannot authorize another cancel"
    );
    Ok(())
}

#[tokio::test]
async fn blob_start_cancelled_during_local_capacity_wait_cannot_later_send_start() -> Result<()> {
    use crate::marketplace::{blob_download::Transfer, lease::ControllerLease};
    let directory = tempfile::tempdir()?;
    let private = directory.path().canonicalize()?.join("cancel-before-start");
    let mut owner = owner();
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill local slot")?;
    let original = Transfer::retained(&owner.client, Arc::new(ControllerLease::acquire(&private)?));
    let opening = original.clone();
    let request = tokio::spawn(async move {
        Reader::open_retained(opening, "1".repeat(64).try_into()?, transfer(b"1234")).await
    });
    tokio::task::yield_now().await;
    assert!(!request.is_finished());
    original.cancel().await?;
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    assert!(request.await?.is_err());
    assert!(
        owner.requests.try_recv().is_err(),
        "cancel seals the original decision before late capacity"
    );
    assert!(owner.faults.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn blob_cancellation_waits_for_local_capacity_and_retains_the_same_workspace() -> Result<()> {
    use crate::marketplace::{blob_download::Transfer, lease::ControllerLease};
    let directory = tempfile::tempdir()?;
    let private = directory.path().canonicalize()?.join("capacity-foreground");
    let lease = Arc::new(ControllerLease::acquire(&private)?);
    let mut owner = owner();
    let original = Transfer::retained(&owner.client, lease.clone());
    drop(lease);
    let opening = original.clone();
    let opened = tokio::spawn(async move {
        Reader::open_retained(opening, "1".repeat(64).try_into()?, transfer(b"1234")).await
    });
    let Some(Request::Blob(WorkerControlOperation::BlobStart { job, .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("original start required");
    };
    reply.send(Ok(BlobReadState::Started)).ok();
    let reader = opened.await??;
    let (fill, _held) = oneshot::channel();
    owner
        .client
        .requests
        .try_send(Request::Peers(fill))
        .ok()
        .context("fill local slot")?;
    drop(reader);
    let mut joined = Box::pin(original.cancel());
    assert!(
        timeout(Duration::from_millis(10), &mut joined)
            .await
            .is_err()
    );
    drop(joined); // This waiter is not the cancellation owner.
    assert!(ControllerLease::acquire(&private).is_err());
    assert!(
        owner.faults.try_recv().is_err(),
        "local pressure is not failed cancellation"
    );
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Peers(_))
    ));
    let Some(Request::Blob(WorkerControlOperation::BlobCancel { job: actual }, reply)) =
        owner.requests.recv().await
    else {
        bail!("one original cancel required");
    };
    assert_eq!(actual, job);
    assert!(ControllerLease::acquire(&private).is_err());
    reply.send(Ok(BlobReadState::Cancelled)).ok();
    original.cancel().await?;
    original.cancel().await?;
    assert!(ControllerLease::acquire(&private).is_ok());
    assert!(owner.requests.try_recv().is_err());
    assert!(owner.faults.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn provider_lookup_retains_the_same_global_lane_across_cancelled_wait_and_owner_drop()
-> Result<()> {
    use crate::marketplace::{lease::ControllerLease, provider_lookup::Lookup};
    use std::sync::Arc;
    let root = tempfile::tempdir()?;
    let private = root.path().canonicalize()?.join("foreground");
    let lease = Arc::new(ControllerLease::acquire(&private)?);
    let mut owner = owner();
    owner.client.blob_discovery = true;
    let digest = Digest::of(b"signed source package");
    let mut lookup = Lookup::begin(&owner.client, digest, lease.clone());
    drop(lease);
    let Some(Request::Providers(_, requested, reply)) = owner.requests.recv().await else {
        bail!("expected private provider lookup");
    };
    assert_eq!(requested, digest);
    assert!(lookup.matches(&owner.client, digest));
    assert!(!lookup.matches(&owner.client, Digest::of(b"another slot")));
    assert!(!lookup.matches(&self::owner().client, digest));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), lookup.wait())
            .await
            .is_err()
    );
    assert!(ControllerLease::acquire(&private).is_err());
    let peer = PeerKey::try_from("1".repeat(64))?;
    reply
        .send(LookupCompletion::replied(Ok(vec![peer.clone()])))
        .ok();
    assert_eq!(lookup.wait().await?, vec![peer]);
    assert!(ControllerLease::acquire(&private).is_ok());
    assert!(
        lookup.wait().await.is_err(),
        "a consumed receipt is not a new completion"
    );
    lookup.finish().await?;
    lookup.finish().await?;

    let lease = Arc::new(ControllerLease::acquire(&private)?);
    let lookup = Lookup::begin(&owner.client, digest, lease);
    let Some(Request::Providers(_, _, reply)) = owner.requests.recv().await else {
        bail!("expected detached provider lookup");
    };
    drop(lookup);
    assert!(
        ControllerLease::acquire(&private).is_err(),
        "dropped owner must not abort the actual RPC"
    );
    reply.send(LookupCompletion::replied(Ok(vec![]))).ok();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if ControllerLease::acquire(&private).is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn completed_lookup_rejections_clean_up_without_reconsuming_hints_or_renewing_authority()
-> Result<()> {
    use crate::marketplace::{lease::ControllerLease, provider_lookup::Lookup};
    for case in 0..4 {
        let root = tempfile::tempdir()?;
        let lease = Arc::new(ControllerLease::acquire(
            &root.path().canonicalize()?.join("source"),
        )?);
        let mut owner = owner();
        owner.client.blob_discovery = true;
        let mut lookup = Lookup::begin(&owner.client, Digest::of(b"exact package"), lease);
        let Some(Request::Providers(_, _, reply)) = owner.requests.recv().await else {
            bail!("expected original lookup");
        };
        let peer = PeerKey::try_from("1".repeat(64))?;
        let completion = match case {
            0 => LookupCompletion::not_dispatched(anyhow::anyhow!("local pre-dispatch rejection")),
            1 => LookupCompletion::replied(Err(anyhow::anyhow!("actual reply rejected"))),
            2 => {
                owner.protection.send_replace(Snapshot {
                    state: ProtectionState::Failed,
                    until: None,
                });
                LookupCompletion::replied(Ok(vec![peer]))
            }
            _ => LookupCompletion::replied(Ok(vec![peer.clone(), peer])),
        };
        reply.send(completion).ok();
        assert!(lookup.wait().await.is_err());
        lookup.finish().await?;
        lookup.finish().await?;
        assert!(lookup.wait().await.is_err());
        assert!(owner.requests.try_recv().is_err(), "no replacement RPC");
    }
    Ok(())
}

#[tokio::test]
async fn lookup_rejected_before_queue_admission_needs_no_rpc_cleanup_or_replacement() -> Result<()>
{
    use crate::marketplace::{lease::ControllerLease, provider_lookup::Lookup};
    for case in 0..3 {
        let root = tempfile::tempdir()?;
        let lease = Arc::new(ControllerLease::acquire(
            &root.path().canonicalize()?.join("source"),
        )?);
        let mut owner = owner();
        owner.client.blob_discovery = case != 0;
        if case == 1 {
            let (reply, _) = oneshot::channel();
            assert!(
                owner
                    .client
                    .requests
                    .try_send(Request::Peers(reply))
                    .is_ok()
            );
        } else if case == 2 {
            owner.requests.close();
        }
        let mut lookup = Lookup::begin(&owner.client, Digest::of(b"exact package"), lease);
        assert!(lookup.wait().await.is_err());
        lookup.finish().await?;
        lookup.finish().await?;
        if case == 1 {
            assert!(
                matches!(owner.requests.try_recv(), Ok(Request::Peers(_))),
                "the original full slot is not consumed or replaced"
            );
        }
        assert!(
            owner.requests.try_recv().is_err(),
            "no provider RPC admitted"
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn lost_or_timed_out_lookup_acknowledgement_stays_unknown_after_a_late_reply() -> Result<()> {
    use crate::marketplace::{lease::ControllerLease, provider_lookup::Lookup};
    for lost in [false, true] {
        let root = tempfile::tempdir()?;
        let lease = Arc::new(ControllerLease::acquire(
            &root.path().canonicalize()?.join("source"),
        )?);
        let mut owner = owner();
        owner.client.blob_discovery = true;
        let mut lookup = Lookup::begin(&owner.client, Digest::of(b"exact package"), lease);
        let Some(Request::Providers(_, _, reply)) = owner.requests.recv().await else {
            bail!("expected original lookup");
        };
        let mut late = Some(reply);
        if lost {
            drop(late.take());
        } else {
            tokio::time::advance(Duration::from_secs(31)).await;
        }
        assert!(lookup.wait().await.is_err());
        let first = format!("{:#}", lookup.finish().await.unwrap_err());
        assert!(first.contains(if lost {
            "request owner stopped"
        } else {
            "acknowledgement deadline"
        }));
        if let Some(reply) = late {
            assert!(
                reply.send(LookupCompletion::replied(Ok(vec![]))).is_err(),
                "a late reply cannot rewrite unknown completion"
            );
        }
        assert_eq!(format!("{:#}", lookup.finish().await.unwrap_err()), first);
        assert!(owner.requests.try_recv().is_err());
    }
    Ok(())
}

#[tokio::test]
async fn lookup_cleanup_before_result_consumption_seals_the_one_original_reply() -> Result<()> {
    use crate::marketplace::{lease::ControllerLease, provider_lookup::Lookup};
    let root = tempfile::tempdir()?;
    let lease = Arc::new(ControllerLease::acquire(
        &root.path().canonicalize()?.join("source"),
    )?);
    let mut owner = owner();
    owner.client.blob_discovery = true;
    let mut lookup = Lookup::begin(&owner.client, Digest::of(b"exact package"), lease);
    let Some(Request::Providers(_, _, reply)) = owner.requests.recv().await else {
        bail!("expected original lookup");
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(10), lookup.finish())
            .await
            .is_err()
    );
    reply.send(LookupCompletion::replied(Ok(vec![]))).ok();
    lookup.finish().await?;
    assert!(
        lookup.wait().await.is_err(),
        "cleanup cannot expose late hints"
    );
    lookup.finish().await?;
    Ok(())
}
async fn opened(owner: &mut Owner, bytes: &[u8]) -> Result<Reader> {
    let client = owner.client.clone();
    let spec = transfer(bytes);
    let task =
        tokio::spawn(async move { Reader::open(&client, "1".repeat(64).try_into()?, spec).await });
    let Some(Request::Blob(WorkerControlOperation::BlobStart { .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected start")
    };
    reply.send(Ok(BlobReadState::Started)).ok();
    task.await?
}

async fn retained_opened(
    owner: &mut Owner,
    private: &std::path::Path,
) -> Result<(Reader, crate::marketplace::blob_download::Transfer, Digest)> {
    use crate::marketplace::{blob_download::Transfer, lease::ControllerLease};
    let handle = Transfer::retained(&owner.client, Arc::new(ControllerLease::acquire(private)?));
    let retained = handle.clone();
    let opened = tokio::spawn(async move {
        Reader::open_retained(retained, "1".repeat(64).try_into()?, transfer(b"1234")).await
    });
    let Some(Request::Blob(WorkerControlOperation::BlobStart { job, .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected original retained start");
    };
    reply.send(Ok(BlobReadState::Started)).ok();
    Ok((opened.await??, handle, job))
}

#[tokio::test(start_paused = true)]
async fn lost_blob_cancel_waits_for_original_retirement_and_retains_the_first_failure() -> Result<()>
{
    use crate::marketplace::{lease::ControllerLease, lifecycle::Exit};
    let directory = tempfile::tempdir()?;
    let private = directory.path().canonicalize()?.join("foreground");
    let mut owner = owner();
    let (original, observation) = retirement::channel();
    owner.client.retirement = observation;
    let (mut reader, handle, job) = retained_opened(&mut owner, &private).await?;
    let mut first_waiter = Box::pin(handle.cancel());
    assert!(
        timeout(Duration::from_millis(1), &mut first_waiter)
            .await
            .is_err()
    );
    let Some(Request::Blob(WorkerControlOperation::BlobCancel { job: cancelled }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected original cancellation");
    };
    assert_eq!(cancelled, job);
    drop(reply); // The original cancellation outcome is unknown, not successful.
    assert_eq!(owner.faults.recv().await, Some(()));
    drop(first_waiter); // Losing this waiter must not lose the owned cleanup task.
    let (foreign, foreign_observation) = retirement::channel();
    foreign.complete(&Exit::new(Ok(()), Ok(())));
    let proof = foreign_observation
        .wait(Instant::now() + Duration::from_secs(1))
        .await?;
    assert!(!owner.client.accepts_retirement(&proof));
    assert!(
        timeout(Duration::from_secs(1), handle.cancel())
            .await
            .is_err()
    );
    assert!(ControllerLease::acquire(&private).is_err());
    original.complete(&Exit::new(
        Err(anyhow::anyhow!("original transport operation failed")),
        Ok(()),
    ));
    handle.cancel().await?;
    let first = handle
        .original_cancellation_failure()
        .await
        .context("lost first cancellation cause")?;
    assert!(format!("{first:#}").contains("blob owner stopped"));
    handle.cancel().await?;
    assert!(Arc::ptr_eq(
        &first,
        &handle
            .original_cancellation_failure()
            .await
            .context("lost retained cause")?
    ));
    assert!(
        reader.read(&mut [0]).await.is_err(),
        "retirement is no read authority"
    );
    assert!(
        Reader::open_retained(
            handle.clone(),
            "1".repeat(64).try_into()?,
            transfer(b"1234")
        )
        .await
        .is_err()
    );
    assert!(ControllerLease::acquire(&private).is_ok());
    assert!(
        owner.requests.try_recv().is_err(),
        "no replacement transfer or cancel"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_original_blob_retirement_keeps_the_same_lane_and_cached_failure() -> Result<()>
{
    use crate::marketplace::{lease::ControllerLease, lifecycle::Exit};
    let directory = tempfile::tempdir()?;
    let private = directory.path().canonicalize()?.join("foreground");
    let mut owner = owner();
    let (original, observation) = retirement::channel();
    owner.client.retirement = observation;
    let (reader, handle, _) = retained_opened(&mut owner, &private).await?;
    drop(reader);
    let Some(Request::Blob(WorkerControlOperation::BlobCancel { .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected original cancellation");
    };
    reply
        .send(Err(anyhow::anyhow!(
            "first original cancellation rejection"
        )))
        .ok();
    assert_eq!(owner.faults.recv().await, Some(()));
    original.complete(&Exit::new(
        Ok(()),
        Err(anyhow::anyhow!("physical cleanup unknown")),
    ));
    let first = format!("{:#}", handle.cancel().await.unwrap_err());
    assert!(first.contains("first original cancellation rejection"));
    assert!(first.contains("original protected owner cleanup is unconfirmed"));
    assert_eq!(first, format!("{:#}", handle.cancel().await.unwrap_err()));
    assert!(
        ControllerLease::acquire(&private).is_err(),
        "uncertainty cannot release the original retained lane"
    );
    assert!(owner.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn original_blob_retirement_deadline_survives_lost_waiters_and_late_proof() -> Result<()> {
    use crate::marketplace::{lease::ControllerLease, lifecycle::Exit};
    let directory = tempfile::tempdir()?;
    let private = directory.path().canonicalize()?.join("foreground");
    let mut owner = owner();
    let (original, observation) = retirement::channel();
    owner.client.retirement = observation;
    let (reader, handle, _) = retained_opened(&mut owner, &private).await?;
    drop(reader);
    let Some(Request::Blob(WorkerControlOperation::BlobCancel { .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected original cancellation");
    };
    reply
        .send(Err(anyhow::anyhow!("first original cancellation failure")))
        .ok();
    assert_eq!(owner.faults.recv().await, Some(()));
    assert!(
        timeout(Duration::from_secs(40), handle.cancel())
            .await
            .is_err()
    );
    assert!(ControllerLease::acquire(&private).is_err());
    tokio::time::advance(Duration::from_secs(33)).await;
    let first = format!("{:#}", handle.cancel().await.unwrap_err());
    assert!(first.contains("first original cancellation failure"));
    assert!(first.contains("original protected cleanup observation deadline"));
    original.complete(&Exit::new(Ok(()), Ok(())));
    assert_eq!(first, format!("{:#}", handle.cancel().await.unwrap_err()));
    assert!(
        ControllerLease::acquire(&private).is_err(),
        "late proof cannot rewrite cached uncertainty"
    );
    assert!(owner.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn blob_reader_preserves_control_failure_without_treating_it_as_cleanup() -> Result<()> {
    let mut owner = owner();
    let mut reader = opened(&mut owner, b"1234").await?;
    let receive = tokio::spawn(async move {
        let mut byte = [0];
        let error = reader.read(&mut byte).await.unwrap_err();
        (reader, error)
    });
    let Some(Request::Blob(WorkerControlOperation::BlobRead { .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected original read")
    };
    reply
        .send(Err(anyhow::anyhow!("original private read rejection")))
        .ok();
    let (mut reader, error) = receive.await?;
    let original = format!("{:#}", anyhow::Error::new(error));
    assert!(original.contains("read acknowledgement"));
    assert!(original.contains("original private read rejection"));
    let cancelled = tokio::spawn(async move { reader.cancel().await });
    let Some(Request::Blob(WorkerControlOperation::BlobCancel { .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected original cancellation, not another read")
    };
    reply.send(Ok(BlobReadState::Cancelled)).ok();
    cancelled.await??;
    assert!(owner.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn blob_reader_keeps_one_chunk_and_accepts_only_exact_integrity_eof() -> Result<()> {
    let mut owner = owner();
    let bytes = vec![11; 65543];
    let mut reader = opened(&mut owner, &bytes).await?;
    let receive = tokio::spawn(async move {
        let mut got = vec![];
        reader.read_to_end(&mut got).await?;
        Ok::<_, anyhow::Error>((reader, got))
    });
    let mut offset = 0;
    loop {
        let Some(Request::Blob(
            WorkerControlOperation::BlobRead {
                offset: requested, ..
            },
            reply,
        )) = owner.requests.recv().await
        else {
            bail!("expected read")
        };
        assert_eq!(requested, offset);
        if offset == bytes.len() as u64 {
            reply
                .send(Ok(BlobReadState::Complete {
                    blob: transfer(&bytes).blob,
                    integrity_only: true,
                }))
                .ok();
            break;
        }
        let end = (offset as usize + 65536).min(bytes.len());
        reply
            .send(Ok(BlobReadState::Data {
                offset,
                data: URL_SAFE_NO_PAD.encode(&bytes[offset as usize..end]),
            }))
            .ok();
        offset = end as u64;
    }
    let (reader, got) = receive.await??;
    assert_eq!(got, bytes);
    drop(reader);
    assert!(
        owner.requests.try_recv().is_err(),
        "successful EOF must disarm cancellation"
    );
    Ok(())
}

#[tokio::test]
async fn blob_reader_rejects_forged_eof_and_drop_requests_owned_cancellation() -> Result<()> {
    let mut owner = owner();
    let mut reader = opened(&mut owner, b"1234").await?;
    let receive = tokio::spawn(async move {
        let mut byte = [0];
        assert!(reader.read(&mut byte).await.is_err());
        drop(reader);
    });
    let Some(Request::Blob(WorkerControlOperation::BlobRead { .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected read")
    };
    reply
        .send(Ok(BlobReadState::Complete {
            blob: transfer(b"1234").blob,
            integrity_only: true,
        }))
        .ok();
    receive.await?;
    assert!(matches!(
        owner.requests.recv().await,
        Some(Request::Blob(WorkerControlOperation::BlobCancel { .. }, _))
    ));
    Ok(())
}

#[tokio::test]
async fn blob_reader_cannot_drain_buffered_data_after_owner_loss_and_full_cancel_queue_signals_shutdown()
-> Result<()> {
    let mut owner = owner();
    let mut reader = opened(&mut owner, b"1234").await?;
    let receive = tokio::spawn(async move {
        let mut byte = [0];
        reader.read_exact(&mut byte).await?;
        Ok::<_, anyhow::Error>(reader)
    });
    let Some(Request::Blob(WorkerControlOperation::BlobRead { .. }, reply)) =
        owner.requests.recv().await
    else {
        bail!("expected read")
    };
    reply
        .send(Ok(BlobReadState::Data {
            offset: 0,
            data: URL_SAFE_NO_PAD.encode(b"1234"),
        }))
        .ok();
    let mut reader = receive.await??;
    owner.protection.send_replace(Snapshot {
        state: ProtectionState::Failed,
        until: None,
    });
    assert!(reader.read(&mut [0]).await.is_err());
    let (send, _) = oneshot::channel();
    owner.client.requests.try_send(Request::Peers(send)).ok();
    drop(reader);
    assert_eq!(owner.faults.recv().await, Some(()));
    Ok(())
}

#[tokio::test]
async fn retained_cancel_joins_one_receipt_and_holds_the_expensive_lane_after_reader_loss()
-> Result<()> {
    use crate::marketplace::{blob_download::Transfer, lease::ControllerLease};
    use std::sync::Arc;
    let directory = tempfile::tempdir()?;
    let private = directory.path().canonicalize()?.join("foreground");
    let lease = Arc::new(ControllerLease::acquire(&private)?);
    let mut owner = owner();
    let handle = Transfer::retained(&owner.client, lease.clone());
    drop(lease); // only the actual cancellation task now holds this same lane
    let opened = {
        let retained = handle.clone();
        tokio::spawn(async move {
            Reader::open_retained(retained, "1".repeat(64).try_into()?, transfer(b"1234")).await
        })
    };
    let Some(Request::Blob(WorkerControlOperation::BlobStart { job, .. }, start)) =
        owner.requests.recv().await
    else {
        bail!("expected retained start");
    };
    start.send(Ok(BlobReadState::Started)).ok();
    let reader = opened.await??;
    drop(reader);
    let Some(Request::Blob(WorkerControlOperation::BlobCancel { job: cancelled }, stop)) =
        owner.requests.recv().await
    else {
        bail!("expected owned cancellation");
    };
    assert_eq!(job, cancelled);
    assert!(ControllerLease::acquire(&private).is_err());
    let mut joined = Box::pin(handle.cancel());
    assert!(
        timeout(Duration::from_millis(10), &mut joined)
            .await
            .is_err()
    );
    drop(joined); // a lost cancellation caller cannot consume the real join
    assert!(ControllerLease::acquire(&private).is_err());
    stop.send(Ok(BlobReadState::Cancelled)).ok();
    handle.cancel().await?;
    handle.cancel().await?;
    assert!(
        owner.requests.try_recv().is_err(),
        "one cancel, no duplicate/replacement job"
    );
    assert!(ControllerLease::acquire(&private).is_ok());
    Ok(())
}

#[tokio::test]
async fn explicit_reader_cancellation_cannot_drain_buffers_or_accept_a_forged_cleanup_receipt()
-> Result<()> {
    let mut owner = owner();
    let mut reader = opened(&mut owner, b"1234").await?;
    let task = tokio::spawn(async move { reader.cancel().await });
    let Some(Request::Blob(WorkerControlOperation::BlobCancel { .. }, stop)) =
        owner.requests.recv().await
    else {
        bail!("expected cancel");
    };
    stop.send(Ok(BlobReadState::Started)).ok();
    assert!(task.await?.is_err());
    assert_eq!(owner.faults.recv().await, Some(()));
    Ok(())
}

#[tokio::test]
async fn failed_blob_cleanup_retains_the_original_cause_across_repeated_join_without_an_rpc_retry()
-> Result<()> {
    for rejected_receipt in [false, true] {
        let mut owner = owner();
        let mut reader = opened(&mut owner, b"1234").await?;
        let (first, sent) = tokio::join!(reader.cancel(), async {
            let Some(Request::Blob(WorkerControlOperation::BlobCancel { .. }, stop)) =
                owner.requests.recv().await
            else {
                bail!("expected the one original cancellation");
            };
            let result = if rejected_receipt {
                Ok(BlobReadState::Started)
            } else {
                Err(anyhow::anyhow!("fake original cancellation failure")
                    .context("fake control wrapper"))
            };
            stop.send(result).ok();
            Ok::<_, anyhow::Error>(())
        });
        sent?;
        let first = format!("{:#}", first.unwrap_err());
        assert!(first.contains("protected blob cleanup is unconfirmed"));
        if rejected_receipt {
            assert!(first.contains("blob cancellation was not acknowledged"));
        } else {
            assert!(first.contains("fake control wrapper"));
            assert!(first.contains("fake original cancellation failure"));
        }
        let second = format!("{:#}", reader.cancel().await.unwrap_err());
        assert_eq!(first, second);
        assert!(owner.requests.try_recv().is_err(), "no replacement RPC");
        assert_eq!(owner.faults.recv().await, Some(()));
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_unstarted_transfer_needs_no_rpc_and_cannot_be_reused_for_a_late_start()
-> Result<()> {
    use crate::marketplace::{blob_download::Transfer, lease::ControllerLease};
    use std::sync::Arc;
    let directory = tempfile::tempdir()?;
    let private = directory.path().canonicalize()?.join("foreground");
    let lease = Arc::new(ControllerLease::acquire(&private)?);
    let mut owner = owner();
    let transfer_owner = Transfer::retained(&owner.client, lease);
    transfer_owner.cancel().await?;
    assert!(ControllerLease::acquire(&private).is_ok());
    assert!(
        Reader::open_retained(
            transfer_owner,
            "1".repeat(64).try_into()?,
            transfer(b"1234")
        )
        .await
        .is_err()
    );
    assert!(owner.requests.try_recv().is_err());
    assert!(owner.faults.try_recv().is_err());
    Ok(())
}
