//! Scheduling/ownership tests with a private fake, NOT source/network evidence.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[test]
fn download_candidates_keep_catalog_fallback_after_empty_or_exhausted_hints() -> Result<()> {
    use elixir_marketplace::{worker_blob::MAX_BLOB_PROVIDERS, worker_network::PeerKey};
    let peer = |i: usize| PeerKey::try_from(format!("{i:064x}"));
    let catalogs = (1..=super::super::search::MAX_CATALOG_PEERS)
        .map(peer)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(download_roster(vec![], &catalogs)?, catalogs);
    let hints = (20..20 + MAX_BLOB_PROVIDERS)
        .map(peer)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let roster = download_roster(hints.clone(), &catalogs)?;
    assert_eq!(&roster[..hints.len()], &hints);
    assert_eq!(&roster[hints.len()..], &catalogs);
    assert_eq!(
        roster.len(),
        MAX_BLOB_PROVIDERS + super::super::search::MAX_CATALOG_PEERS
    );
    assert!(
        roster.get(roster.len()).is_none(),
        "exhaustion is finite, not another DHT lookup"
    );
    let repeated = [hints[0].clone(), catalogs[0].clone(), catalogs[0].clone()];
    assert_eq!(
        download_roster(hints.clone(), &repeated)?,
        [hints, vec![catalogs[0].clone()]].concat()
    );
    Ok(())
}

#[test]
fn download_candidate_sources_cannot_expand_their_independent_limits() -> Result<()> {
    use elixir_marketplace::{worker_blob::MAX_BLOB_PROVIDERS, worker_network::PeerKey};
    let peers = (1..=MAX_BLOB_PROVIDERS + super::super::search::MAX_CATALOG_PEERS + 1)
        .map(|i| PeerKey::try_from(format!("{i:064x}")))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(download_roster(peers[..=MAX_BLOB_PROVIDERS].to_vec(), &[]).is_err());
    assert!(download_roster(vec![], &peers[..=super::super::search::MAX_CATALOG_PEERS]).is_err());
    assert!(download_roster(vec![peers[0].clone(), peers[0].clone()], &[]).is_err());
    Ok(())
}

#[derive(Default)]
struct Observed {
    advance: Notify,
    cleanup: Notify,
    release_cleanup: Notify,
    cleanup_calls: AtomicUsize,
    dropped: AtomicUsize,
    fail_cleanup: AtomicBool,
    fail_advance: AtomicBool,
    stall_cleanup: AtomicBool,
    ready: AtomicBool,
    authority_lost: AtomicBool,
    deferred: AtomicBool,
    advance_calls: AtomicUsize,
    approval: Notify,
    release_approval: Notify,
    approval_calls: AtomicUsize,
    stall_approval: AtomicBool,
    retention: AtomicBool,
    approved: AtomicBool,
}
struct Fake {
    seen: Arc<Observed>,
    expires: Instant,
    #[cfg(unix)]
    handoff: Option<handoff::Slot>,
}
impl Drop for Fake {
    fn drop(&mut self) {
        self.seen.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
fn fake() -> (Fake, Arc<Observed>) {
    let seen = Arc::new(Observed::default());
    (
        Fake {
            seen: seen.clone(),
            expires: Instant::now() + Duration::from_secs(60),
            #[cfg(unix)]
            handoff: None,
        },
        seen,
    )
}
#[test]
fn workspace_phase_is_a_small_handle_not_an_embedded_manifest_or_source_plan() {
    assert!(std::mem::size_of::<Workspace>() <= 64);
    assert!(std::mem::size_of::<Live>() < 1024);
    assert!(Workspace::Pending.resource_retryable());
    assert!(!Workspace::Disposed.resource_retryable());
    #[cfg(unix)]
    assert!(!Workspace::Archived.resource_retryable());
}
fn review() -> Result<GrantReview> {
    let (release, _, _) = crate::marketplace::quarantine::tests::fixture()?;
    Ok(serde_json::from_value(serde_json::json!({
        "release_id":Digest::of(b"fake-release"), "manifest_sha256":Digest::of(b"fake-manifest"),
        "scope":{"policy_version":1,"extension_id":release.payload().extension_id,
          "runtime_profile":"declarative-v1","permissions":[],"secret_keys":[],"runtime":null,
          "authority_contract":Digest::of(b"fake-contract")}
    }))?)
}
#[async_trait]
impl Work for Fake {
    #[cfg(unix)]
    fn handoff(&self) -> Option<handoff::Slot> {
        self.handoff.clone()
    }
    fn release(&self) -> Digest {
        Digest::of(b"fake-release")
    }
    fn expires(&self) -> Instant {
        self.expires
    }
    async fn advance(&mut self) -> Result<Phase> {
        self.seen.advance_calls.fetch_add(1, Ordering::SeqCst);
        self.seen.advance.notify_one();
        if self.seen.fail_advance.load(Ordering::SeqCst) {
            return Err(
                anyhow::anyhow!("fake original transfer cause").context("fake transfer wrapper")
            );
        }
        if self.seen.retention.load(Ordering::SeqCst) && self.seen.approved.load(Ordering::SeqCst) {
            return Ok(Phase::SourceArchived);
        }
        if self.seen.deferred.load(Ordering::SeqCst) {
            return Ok(Phase::WaitingForResources);
        }
        if self.seen.ready.load(Ordering::SeqCst) {
            return Ok(Phase::AwaitingApproval);
        }
        std::future::pending().await
    }
    async fn review(&mut self) -> Result<GrantReview> {
        if self.seen.authority_lost.load(Ordering::SeqCst) {
            bail!("fake authority lost");
        }
        review()
    }
    async fn approve(&mut self, _request: approval::Request) -> Result<()> {
        self.seen.approval_calls.fetch_add(1, Ordering::SeqCst);
        self.seen.approval.notify_one();
        if self.seen.stall_approval.load(Ordering::SeqCst) {
            self.seen.release_approval.notified().await;
        }
        self.seen.approved.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn continues_after_approval(&self) -> bool {
        self.seen.retention.load(Ordering::SeqCst)
    }
    async fn cleanup(&mut self) -> Result<()> {
        self.seen.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        self.seen.cleanup.notify_one();
        if self.seen.stall_cleanup.load(Ordering::SeqCst) {
            self.seen.release_cleanup.notified().await;
        }
        if self.seen.fail_cleanup.load(Ordering::SeqCst) {
            bail!("fake unconfirmed cleanup");
        }
        Ok(())
    }
}
async fn notified(signal: &Notify) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), signal.notified())
        .await
        .context("fake signal missing")?;
    Ok(())
}

#[tokio::test]
async fn cancelled_caller_cannot_lose_cleanup_handle_release_slot_or_queue_a_second_job()
-> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.stall_cleanup.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    notified(&seen.advance).await?;
    let (another, _) = fake();
    assert!(service.start_owned(another).is_err());
    assert!(service.view(Uuid::new_v4()).is_err());
    let mut cancelled = Box::pin(service.cancel(id));
    tokio::select! { result = &mut cancelled => { result?; bail!("cleanup did not wait"); }, _ = notified(&seen.cleanup) => {} }
    drop(cancelled);
    assert_eq!(service.view(id)?.phase, Phase::Cancelling);
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 0);
    let (another, _) = fake();
    assert!(service.start_owned(another).is_err());
    seen.release_cleanup.notify_one();
    service.cancel(id).await?;
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
    assert!(service.view(id).is_err());
    let (next, _) = fake();
    let next_id = service.start_owned(next)?;
    service.cancel(next_id).await?;
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_never_rearms_and_joining_it_again_never_repolls_a_completed_task()
-> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.fail_cleanup.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    notified(&seen.advance).await?;
    assert!(service.cancel(id).await.is_err());
    assert_eq!(service.view(id)?.phase, Phase::CleanupFailed);
    assert!(service.cancel(id).await.is_err());
    assert!(service.shutdown().await.is_err());
    let (other, _) = fake();
    assert!(service.start_owned(other).is_err());
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failed_preparation_retains_original_and_cleanup_causes_without_restarting() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.fail_advance.store(true, Ordering::SeqCst);
    seen.fail_cleanup.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::CleanupFailed).await?;
    let first = format!("{:#}", service.cancel(id).await.unwrap_err());
    assert!(first.contains("ownership remains fenced"));
    assert!(first.contains("fake transfer wrapper"));
    assert!(first.contains("fake original transfer cause"));
    assert!(first.contains("fake unconfirmed cleanup"));
    assert_eq!(
        format!("{:#}", service.cancel(id).await.unwrap_err()),
        first,
        "rejoining must retain the original diagnostic, not execute again"
    );
    assert_eq!(
        format!("{:#}", service.shutdown().await.unwrap_err()),
        first
    );
    #[cfg(unix)]
    {
        let mut destination = None;
        let error = format!(
            "{:#}",
            service
                .take_archive(id, &mut destination)
                .await
                .unwrap_err()
        );
        assert!(error.contains("original ownership remains fenced"));
        assert!(error.contains("fake original transfer cause"));
        assert!(error.contains("fake unconfirmed cleanup"));
        assert!(destination.is_none(), "failure cannot hand off an archive");
    }
    let entry = service.entry(Some(id))?;
    let mut joined = entry.task.lock().await;
    let one = joined.join_cleanup().await.unwrap_err();
    let two = joined.join_cleanup().await.unwrap_err();
    assert!(Arc::ptr_eq(&one, &two), "one retained failure, no history");
    drop(joined);
    assert_eq!(seen.advance_calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    let (next, _) = fake();
    assert!(service.start_owned(next).is_err());
    let displayed = serde_json::to_string(&service.view(id)?)?;
    assert!(!displayed.contains("fake original"));
    assert!(!displayed.contains("fake unconfirmed"));
    assert!(!service.view(id)?.installation_ready);
    Ok(())
}

#[tokio::test]
async fn failed_preparation_with_confirmed_cleanup_is_not_unknown_recovery() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.fail_advance.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::Failed).await?;
    let failure = service
        .operation_failure(id)?
        .context("private original cause missing")?;
    assert!(format!("{failure:#}").contains("fake original"));
    let same = service
        .operation_failure(id)?
        .context("private original cause lost")?;
    assert!(Arc::ptr_eq(&failure, &same));
    let displayed = serde_json::to_string(&service.view(id)?)?;
    assert!(!displayed.contains("fake original"));
    assert!(service.operation_failure(Uuid::new_v4()).is_err());
    service.cancel(id).await?;
    assert_eq!(seen.advance_calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    let (next, _) = fake();
    let id = service.start_owned(next)?;
    service.cancel(id).await?;
    Ok(())
}

#[tokio::test]
async fn panicked_preparation_join_stays_failed_and_preserves_the_original_join_error() -> Result<()>
{
    let task: JoinHandle<Result<()>> = tokio::spawn(async {
        panic!("fake original preparation panic");
    });
    let mut joined = Joined::Running(task);
    let first = joined.join_cleanup().await.unwrap_err();
    let second = joined.join_cleanup().await.unwrap_err();
    assert!(Arc::ptr_eq(&first, &second));
    let diagnostic = format!("{first:#}");
    assert!(diagnostic.contains("original preparation task stopped"));
    assert!(diagnostic.contains("fake original preparation panic"));
    Ok(())
}

#[tokio::test]
async fn dropping_last_service_requests_owned_cleanup_instead_of_aborting_the_task() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    service.start_owned(work)?;
    notified(&seen.advance).await?;
    let clone = service.clone();
    drop(service);
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 0);
    drop(clone);
    notified(&seen.cleanup).await?;
    tokio::task::yield_now().await;
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn review_is_not_installation_authority_and_is_removed_on_loss_or_expiry() -> Result<()> {
    for expired in [false, true] {
        let service = Service::default();
        let (mut work, seen) = fake();
        seen.ready.store(true, Ordering::SeqCst);
        if expired {
            work.expires = Instant::now() + Duration::from_secs(1);
        }
        let id = service.start_owned(work)?;
        notified(&seen.advance).await?;
        tokio::task::yield_now().await;
        let ready = service.view(id)?;
        assert_eq!(ready.phase, Phase::AwaitingApproval);
        assert!(ready.review.is_some());
        assert!(!ready.installation_ready);
        if !expired {
            seen.authority_lost.store(true, Ordering::SeqCst);
        }
        tokio::time::timeout(Duration::from_secs(7), seen.cleanup.notified()).await?;
        let failed = service.view(id)?;
        assert_eq!(failed.phase, Phase::Failed);
        assert!(failed.review.is_none());
        service.cancel(id).await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_deferred_job_keeps_one_owner_without_spinning_or_a_hidden_waiting_queue() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.deferred.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    notified(&seen.advance).await?;
    assert_eq!(service.view(id)?.phase, Phase::WaitingForResources);
    assert_eq!(seen.advance_calls.load(Ordering::SeqCst), 1);
    let (another, _) = fake();
    assert!(service.start_owned(another).is_err());
    service.cancel(id).await?;
    assert_eq!(seen.advance_calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn poisoned_service_still_stops_owned_work_but_cannot_claim_clean_shutdown() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    let id = service.start_owned(work)?;
    notified(&seen.advance).await?;
    let owner = service.0.clone();
    assert!(
        std::thread::spawn(move || {
            let _held = owner.lock().unwrap();
            panic!("deliberate test-only poison");
        })
        .join()
        .is_err()
    );
    service.request_stop();
    notified(&seen.cleanup).await?;
    assert!(service.shutdown().await.is_err());
    assert!(service.view(id).is_err());
    Ok(())
}

async fn actor() -> Result<crate::marketplace::grant_store::Actor> {
    let database = crate::db::Database::connect(&crate::config::DatabaseConfig {
        url: "sqlite::memory:".into(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let actor = crate::marketplace::grant_store::tests::owner(&database).await?;
    database.pool.close().await;
    Ok(actor)
}
fn decision(mutation_id: Uuid) -> Result<crate::marketplace::grant_store::ApprovalSelection> {
    Ok(crate::marketplace::grant_store::ApprovalSelection {
        review_id: review()?.review_id()?,
        expected_revision: 0,
        mutation_id,
        automatic_updates: false,
    })
}
async fn visible(service: &Service, id: Uuid, phase: Phase) -> Result<View> {
    tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            let view = service.view(id)?;
            if view.phase == phase {
                return Ok(view);
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("preparation phase did not appear")?
}

#[tokio::test]
async fn exact_approval_retries_do_not_queue_or_reissue_and_never_claim_installation() -> Result<()>
{
    let service = Service::default();
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    seen.stall_approval.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    let actor = actor().await?;
    let mutation = Uuid::new_v4();
    service.approve_as(id, actor.clone(), decision(mutation)?)?;
    notified(&seen.approval).await?;
    assert_eq!(service.view(id)?.phase, Phase::Approving);
    service
        .clone()
        .approve_as(id, actor.clone(), decision(mutation)?)?;
    let mut changed = decision(mutation)?;
    changed.automatic_updates = true;
    assert!(
        service.approve_as(id, actor.clone(), changed).is_err(),
        "a retry cannot silently enable automatic updates"
    );
    assert!(
        service
            .approve_as(id, actor, decision(Uuid::new_v4())?)
            .is_err()
    );
    assert_eq!(seen.approval_calls.load(Ordering::SeqCst), 1);
    seen.release_approval.notify_one();
    let view = visible(&service, id, Phase::PermissionGranted).await?;
    assert!(view.review.is_some());
    assert!(!view.installation_ready);
    service.cancel(id).await?;
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn invalid_or_early_approval_does_not_consume_the_single_request_slot() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    let id = service.start_owned(work)?;
    notified(&seen.advance).await?;
    let actor = actor().await?;
    assert!(
        service
            .approve_as(id, actor.clone(), decision(Uuid::new_v4())?)
            .is_err()
    );
    // Restart a new owner task at review; the first unready work has no queue.
    service.cancel(id).await?;
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    let mut wrong = decision(Uuid::new_v4())?;
    wrong.review_id = Digest::of(b"different source review");
    assert!(service.approve_as(id, actor.clone(), wrong).is_err());
    assert!(
        service
            .approve_as(id, actor.clone(), decision(Uuid::nil())?)
            .is_err()
    );
    let mut wrong = decision(Uuid::new_v4())?;
    wrong.expected_revision = -1;
    assert!(service.approve_as(id, actor.clone(), wrong).is_err());
    service.approve_as(id, actor, decision(Uuid::new_v4())?)?;
    visible(&service, id, Phase::PermissionGranted).await?;
    assert_eq!(seen.approval_calls.load(Ordering::SeqCst), 1);
    service.cancel(id).await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_during_approval_keeps_owned_cleanup_join_and_cannot_late_approve()
-> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    seen.stall_approval.store(true, Ordering::SeqCst);
    seen.stall_cleanup.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    service.approve_as(id, actor().await?, decision(Uuid::new_v4())?)?;
    notified(&seen.approval).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), service.cancel(id))
            .await
            .is_err()
    );
    notified(&seen.cleanup).await?;
    assert_eq!(service.view(id)?.phase, Phase::Cancelling);
    let (replacement, _) = fake();
    assert!(service.start_owned(replacement).is_err());
    seen.release_cleanup.notify_one();
    service.cancel(id).await?;
    seen.release_approval.notify_one();
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
    let (replacement, _) = fake();
    let other = service.start_owned(replacement)?;
    service.cancel(other).await?;
    Ok(())
}

#[tokio::test]
async fn committed_permission_presentation_is_cleared_on_authority_loss() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    service.approve_as(id, actor().await?, decision(Uuid::new_v4())?)?;
    visible(&service, id, Phase::PermissionGranted).await?;
    seen.authority_lost.store(true, Ordering::SeqCst);
    let view = visible(&service, id, Phase::Failed).await?;
    assert!(view.review.is_none());
    assert!(!view.installation_ready);
    service.cancel(id).await?;
    Ok(())
}

#[tokio::test]
async fn retained_continuation_is_terminal_only_after_cleanup_and_never_reports_install_ready()
-> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    seen.retention.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    service.approve_as(id, actor().await?, decision(Uuid::new_v4())?)?;
    let view = visible(&service, id, Phase::SourceArchived).await?;
    assert!(!view.installation_ready);
    assert!(view.review.is_none());
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
    service.cancel(id).await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn archived_status_without_an_original_output_cannot_be_handed_to_installation() -> Result<()>
{
    let service = Service::default();
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    seen.retention.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    service.approve_as(id, actor().await?, decision(Uuid::new_v4())?)?;
    visible(&service, id, Phase::SourceArchived).await?;
    let mut destination = None;
    assert!(service.take_archive(id, &mut destination).await.is_err());
    assert!(destination.is_none());
    assert_eq!(service.view(id)?.phase, Phase::SourceArchived);
    service.cancel(id).await?;
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_archive_handoff_waiter_keeps_the_same_task_and_service_slot() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    seen.retention.store(true, Ordering::SeqCst);
    seen.stall_cleanup.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    service.approve_as(id, actor().await?, decision(Uuid::new_v4())?)?;
    notified(&seen.cleanup).await?;
    let mut destination = None;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            service.take_archive(id, &mut destination)
        )
        .await
        .is_err()
    );
    assert!(destination.is_none());
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 0);
    let (other, _) = fake();
    assert!(service.start_owned(other).is_err());
    assert!(matches!(
        *service.entry(Some(id))?.task.lock().await,
        Joined::Running(_)
    ));
    seen.release_cleanup.notify_one();
    assert!(
        service.take_archive(id, &mut destination).await.is_err(),
        "fake has no original archive"
    );
    service.cancel(id).await?;
    assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_preparation_cannot_transfer_an_archived_result() -> Result<()> {
    let service = Service::default();
    let (work, seen) = fake();
    seen.ready.store(true, Ordering::SeqCst);
    seen.retention.store(true, Ordering::SeqCst);
    let id = service.start_owned(work)?;
    visible(&service, id, Phase::AwaitingApproval).await?;
    service.approve_as(id, actor().await?, decision(Uuid::new_v4())?)?;
    visible(&service, id, Phase::SourceArchived).await?;
    service.entry(Some(id))?.cancel.send_replace(true);
    let mut destination = None;
    let error = service
        .take_archive(id, &mut destination)
        .await
        .err()
        .context("cancelled handoff accepted")?;
    assert!(format!("{error:#}").contains("cancelled"));
    assert!(destination.is_none());
    service.cancel(id).await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn fake_archive_completion_or_poisoned_output_cannot_create_a_handoff() -> Result<()> {
    for poison in [false, true] {
        let service = Service::default();
        let (mut work, seen) = fake();
        let output: handoff::Slot = Arc::default();
        if poison {
            let held = output.clone();
            assert!(
                std::thread::spawn(move || {
                    let _lock = held.lock().unwrap();
                    panic!("explicit handoff poison");
                })
                .join()
                .is_err()
            );
        }
        work.handoff = Some(output);
        seen.ready.store(true, Ordering::SeqCst);
        seen.retention.store(true, Ordering::SeqCst);
        let id = service.start_owned(work)?;
        visible(&service, id, Phase::AwaitingApproval).await?;
        service.approve_as(id, actor().await?, decision(Uuid::new_v4())?)?;
        visible(&service, id, Phase::CleanupFailed).await?;
        let mut destination = None;
        assert!(service.take_archive(id, &mut destination).await.is_err());
        assert!(destination.is_none());
        assert!(service.cancel(id).await.is_err());
        let (other, _) = fake();
        assert!(service.start_owned(other).is_err());
        assert_eq!(seen.cleanup_calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
