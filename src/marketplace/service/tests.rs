use super::*;

#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn installed_call_turns_share_one_queue_and_shutdown_wakes_waiters_without_networking()
-> Result<()> {
    let service = MarketplaceService::default();
    let shared = service.clone();
    let turn = service.source_provider_turn().await?;
    assert_eq!(service.snapshot().phase, RuntimePhase::NotConnected);
    assert!(shared.source_calls.try_acquire().is_err());
    let mut waiting = Box::pin(shared.source_provider_turn());
    assert!(
        timeout(Duration::from_millis(1), waiting.as_mut())
            .await
            .is_err()
    );
    service.request_stop();
    assert!(waiting.await.is_err());
    assert!(shared.source_calls.require(&turn).is_err());
    drop(turn);
    assert!(shared.source_provider_turn().await.is_err());
    assert!(shared.stop_requested());
    Ok(())
}

#[tokio::test]
async fn direct_previews_share_one_nonqueued_slot_and_never_start_a_disconnected_service()
-> Result<()> {
    let vector: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let publication = elixir_marketplace::publication::PreparedPublication::parse(
        &serde_json::to_vec(&serde_json::json!({
            "schema_version":1,"publisher":vector["publisher"],"release":vector["release"],"head":vector["head"]
        }))?,
    )?;
    let service = MarketplaceService::default();
    let permit = service.imports.clone().try_acquire_owned()?;
    let error = service
        .clone()
        .import_preview(publication.clone())
        .await
        .err()
        .context("busy preview succeeded")?;
    assert!(error.to_string().contains("busy"));
    assert_eq!(service.snapshot().phase, RuntimePhase::NotConnected);
    drop(permit);
    assert!(service.import_preview(publication).await.is_err());
    assert_eq!(service.imports.available_permits(), 1);
    assert_eq!(service.snapshot().phase, RuntimePhase::NotConnected);
    service.shutdown().await?;
    Ok(())
}
use crate::marketplace::preferences::Preferences;
use elixir_marketplace::worker_network::NetworkRoles;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, watch};

fn running(revision: i64, catalog: bool) -> ParticipationSnapshot {
    let roles = NetworkRoles {
        consumer: true,
        catalog,
        routing: false,
    };
    ParticipationSnapshot {
        phase: ParticipationPhase::Running,
        preferences_revision: Some(revision),
        desired_roles: roles,
        applied_roles: Some(roles),
        ..Default::default()
    }
}
fn saved(revision: i64, mode: ContributionMode) -> crate::marketplace::preferences::Snapshot {
    crate::marketplace::preferences::Snapshot {
        revision,
        preferences: Preferences {
            contribution: mode,
            ..Default::default()
        },
    }
}
#[derive(Default)]
struct Observed {
    stops: AtomicUsize,
    drops: AtomicUsize,
    stall: AtomicBool,
    fail: AtomicBool,
    false_success: AtomicBool,
    stopping: Notify,
    pause: AtomicBool,
    resume: Notify,
}
#[derive(Clone)]
struct Client(watch::Receiver<ParticipationSnapshot>);
impl Status for Client {
    fn snapshot(&self) -> ParticipationSnapshot {
        let mut state = self.0.borrow().clone();
        if self.0.has_changed().is_err() && state.phase != ParticipationPhase::Stopped {
            state.phase = ParticipationPhase::Failed;
            state.applied_roles = None;
        }
        state
    }
}
struct Worker {
    client: Client,
    changes: watch::Sender<ParticipationSnapshot>,
    observed: Arc<Observed>,
    finished: bool,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.observed.drops.fetch_add(1, Ordering::SeqCst);
        if !self.finished {
            let mut state = self.changes.borrow().clone();
            state.phase = ParticipationPhase::Failed;
            state.applied_roles = None;
            self.changes.send_replace(state);
        }
    }
}
#[async_trait]
impl Owned for Worker {
    type Client = Client;
    fn client(&self) -> Client {
        self.client.clone()
    }
    fn request_stop(&mut self) {
        self.observed.stops.fetch_add(1, Ordering::SeqCst);
    }
    async fn stop(mut self) -> Result<()> {
        self.observed.stopping.notify_one();
        if self.observed.pause.load(Ordering::SeqCst) {
            self.observed.resume.notified().await;
        }
        if self.observed.stall.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.observed.fail.load(Ordering::SeqCst) {
            bail!("fixture cleanup is uncertain");
        }
        self.finished = true;
        if !self.observed.false_success.load(Ordering::SeqCst) {
            let mut state = self.changes.borrow().clone();
            state.phase = ParticipationPhase::Stopped;
            state.desired_roles = NetworkRoles::default();
            state.applied_roles = Some(NetworkRoles::default());
            self.changes.send_replace(state);
        }
        Ok(())
    }
    async fn retire(self) -> Result<()> {
        self.stop().await
    }
}
fn worker() -> (Worker, watch::Sender<ParticipationSnapshot>, Arc<Observed>) {
    let (changes, observed) = watch::channel(running(1, true));
    let seen = Arc::new(Observed::default());
    (
        Worker {
            client: Client(observed),
            changes: changes.clone(),
            observed: seen.clone(),
            finished: false,
        },
        changes,
        seen,
    )
}

fn fail(changes: &watch::Sender<ParticipationSnapshot>) {
    let mut state = changes.borrow().clone();
    state.phase = ParticipationPhase::Failed;
    state.applied_roles = None;
    changes.send_replace(state);
}

#[tokio::test]
async fn poisoned_owner_state_signals_stop_without_restoring_authority_or_claiming_cleanup()
-> Result<()> {
    for query_stop in [false, true] {
        let service = Arc::new(Managed::<Worker>::default());
        let (owner, _changes, seen) = worker();
        service.attach(owner)?;
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = service.slot.lock().unwrap();
            panic!("fixture owner-state poison");
        }));
        assert!(poisoned.is_err());
        if query_stop {
            assert!(service.stop_requested());
        } else {
            service.request_stop();
        }
        assert!(seen.stops.load(Ordering::SeqCst) > 0);
        assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
        assert!(service.snapshot().participation.is_none());
        assert!(service.begin_startup().is_err());
        assert!(service.clone().shutdown().await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn failed_preparation_reconciliation_requires_actual_exclusive_leases_and_claims_no_health()
-> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    drop(service.begin_startup()?);
    assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
    assert!(service.begin_startup().is_err());
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("marketplace");
    let leases = crate::marketplace::startup::StartupLeases::acquire(&directory)?;
    let attempt = service.begin_recovery(&leases)?;
    assert_eq!(service.snapshot().phase, RuntimePhase::Starting);
    assert!(service.snapshot().participation.is_none());
    assert!(service.begin_recovery(&leases).is_err());
    drop(attempt);
    service.request_stop();
    assert!(
        service.begin_recovery(&leases).is_err(),
        "real leases cannot override shutdown"
    );
    Ok(())
}
#[tokio::test]
async fn preparation_reconciliation_cannot_displace_an_attached_failed_owner() -> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, changes, _) = worker();
    service.attach(owner)?;
    fail(&changes);
    let root = tempfile::tempdir()?;
    let leases = crate::marketplace::startup::StartupLeases::acquire(
        &root.path().canonicalize()?.join("marketplace"),
    )?;
    assert!(service.begin_recovery(&leases).is_err());
    assert!(service.clone().recover_failed().await?);
    assert!(
        service.begin_recovery(&leases).is_err(),
        "a confirmed fresh slot uses normal startup"
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn confirmed_failed_owner_retirement_rearms_one_new_generation_without_old_status()
-> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, old_changes, seen) = worker();
    service.attach(owner)?;
    let generation = service.slot.lock().unwrap().generation;
    fail(&old_changes);
    assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
    assert!(service.clone().recover_failed().await?);
    assert_eq!(service.snapshot().phase, RuntimePhase::NotConnected);
    assert!(service.snapshot().participation.is_none());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert!(service.slot.lock().unwrap().generation > generation);
    let ticket = service.begin_startup()?;
    let (next, _, _) = worker();
    ticket.attach(next)?;
    old_changes.send_replace(running(999, false));
    assert_eq!(
        service
            .snapshot()
            .participation
            .unwrap()
            .preferences_revision,
        Some(1)
    );
    assert!(
        service.clone().recover_failed().await.is_err(),
        "a healthy owner cannot be displaced"
    );
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_or_missing_retirement_ack_cannot_reset_or_retry_an_unknown_owner() -> Result<()> {
    for false_success in [false, true] {
        let service = Arc::new(Managed::<Worker>::default());
        let (owner, changes, seen) = worker();
        seen.false_success.store(false_success, Ordering::SeqCst);
        seen.fail.store(!false_success, Ordering::SeqCst);
        service.attach(owner)?;
        fail(&changes);
        assert!(service.clone().recover_failed().await.is_err());
        assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
        assert!(
            service
                .snapshot()
                .participation
                .unwrap()
                .applied_roles
                .is_none()
        );
        assert!(service.begin_startup().is_err());
        assert!(service.clone().recover_failed().await.is_err());
        assert!(service.clone().shutdown().await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_retirement_keeps_unknown_cleanup_terminal() -> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, changes, seen) = worker();
    seen.stall.store(true, Ordering::SeqCst);
    service.attach(owner)?;
    fail(&changes);
    let managed = service.clone();
    let task = tokio::spawn(async move { managed.recover_failed().await });
    seen.stopping.notified().await;
    assert_eq!(service.snapshot().phase, RuntimePhase::Stopping);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
    assert!(service.begin_startup().is_err());
    assert!(service.clone().recover_failed().await.is_err());
    Ok(())
}

#[tokio::test]
async fn owner_shutdown_during_retirement_latches_stop_and_prevents_reactivation() -> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, changes, seen) = worker();
    seen.pause.store(true, Ordering::SeqCst);
    service.attach(owner)?;
    fail(&changes);
    let managed = service.clone();
    let task = tokio::spawn(async move { managed.recover_failed().await });
    seen.stopping.notified().await;
    service.request_stop();
    seen.resume.notify_one();
    assert!(!task.await??);
    assert_eq!(service.snapshot().phase, RuntimePhase::Stopped);
    assert!(service.snapshot().participation.is_none());
    assert!(service.begin_startup().is_err());
    assert!(!service.clone().recover_failed().await?);
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn local_startup_generation_is_exclusive_and_cannot_be_stolen_by_direct_attachment()
-> Result<()> {
    let managed = Arc::new(Managed::<Worker>::default());
    let attempt = managed.begin_startup()?;
    assert_eq!(managed.snapshot().phase, RuntimePhase::Starting);
    assert!(managed.snapshot().participation.is_none());
    assert!(managed.begin_startup().is_err());
    let (foreign, _, rejected) = worker();
    assert!(managed.attach(foreign).is_err());
    assert!(rejected.stops.load(Ordering::SeqCst) > 0);
    let (owned, _, observed) = worker();
    attempt.attach(owned)?;
    assert_eq!(managed.snapshot().phase, RuntimePhase::Running);
    assert!(managed.begin_startup().is_err());
    managed.shutdown().await?;
    assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_startup_cannot_claim_cleanup_or_blindly_retry_an_uncertain_generation()
-> Result<()> {
    let managed = Arc::new(Managed::<Worker>::default());
    let attempt = managed.begin_startup()?;
    managed.request_stop();
    drop(attempt);
    assert_eq!(managed.snapshot().phase, RuntimePhase::Failed);
    assert!(managed.snapshot().participation.is_none());
    assert!(managed.clone().shutdown().await.is_err());
    assert_eq!(managed.snapshot().phase, RuntimePhase::Failed);
    assert!(managed.begin_startup().is_err());
    let (late, _, observed) = worker();
    assert!(managed.attach(late).is_err());
    assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn owner_stop_rejects_even_a_late_successful_startup_result() -> Result<()> {
    let managed = Arc::new(Managed::<Worker>::default());
    let attempt = managed.begin_startup()?;
    managed.request_stop();
    let (late, _, observed) = worker();
    assert!(attempt.attach(late).is_err());
    assert_eq!(managed.snapshot().phase, RuntimePhase::Failed);
    assert!(managed.snapshot().participation.is_none());
    assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
    assert!(observed.stops.load(Ordering::SeqCst) > 0);
    Ok(())
}

#[test]
fn saved_revision_is_not_an_applied_role_or_off_acknowledgement() {
    let mut snapshot = RuntimeSnapshot {
        phase: RuntimePhase::Running,
        participation: Some(running(1, true)),
    };
    assert!(snapshot.participation_applied(&saved(1, ContributionMode::Auto)));
    assert!(!snapshot.participation_applied(&saved(2, ContributionMode::Off)));
    assert!(!snapshot.participation_applied(&saved(1, ContributionMode::Off)));
    let state = snapshot.participation.as_mut().unwrap();
    *state = running(2, false);
    assert!(snapshot.participation_applied(&saved(2, ContributionMode::Off)));
    let zero = NetworkRoles::default();
    snapshot.participation.as_mut().unwrap().desired_roles = zero;
    snapshot.participation.as_mut().unwrap().applied_roles = Some(zero);
    assert!(
        !snapshot.participation_applied(&saved(2, ContributionMode::Off)),
        "off must preserve consumer mode"
    );
    *snapshot.participation.as_mut().unwrap() = running(2, false);
    snapshot.participation.as_mut().unwrap().phase = ParticipationPhase::Applying;
    assert!(!snapshot.participation_applied(&saved(2, ContributionMode::Off)));
    snapshot.participation.as_mut().unwrap().phase = ParticipationPhase::Running;
    snapshot
        .participation
        .as_mut()
        .unwrap()
        .preferences_revision = None;
    assert!(!snapshot.participation_applied(&saved(2, ContributionMode::Off)));
    snapshot
        .participation
        .as_mut()
        .unwrap()
        .preferences_revision = Some(2);
    snapshot.participation.as_mut().unwrap().applied_roles = None;
    assert!(!snapshot.participation_applied(&saved(2, ContributionMode::Off)));
}

#[tokio::test]
async fn one_shared_owner_slot_tracks_roles_and_stop_is_idempotent() -> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    assert_eq!(service.snapshot().phase, RuntimePhase::NotConnected);
    assert!(service.client().is_err());
    let (owner, changes, seen) = worker();
    service.attach(owner)?;
    assert_eq!(service.snapshot().phase, RuntimePhase::Running);
    let (duplicate, _, duplicate_seen) = worker();
    assert!(service.attach(duplicate).is_err());
    assert_eq!(duplicate_seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 0);
    changes.send_replace(running(2, false));
    assert!(
        service
            .snapshot()
            .participation_applied(&saved(2, ContributionMode::Off))
    );
    service.request_stop();
    assert_eq!(service.snapshot().phase, RuntimePhase::Stopping);
    assert_eq!(
        service.snapshot().participation.unwrap().applied_roles,
        None
    );
    assert!(service.client().is_err());
    service.clone().shutdown().await?;
    assert_eq!(service.snapshot().phase, RuntimePhase::Stopped);
    service.clone().shutdown().await?;
    assert!(seen.stops.load(Ordering::SeqCst) > 0);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancellation_and_failed_cleanup_cannot_publish_stopped_or_applied() -> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, _changes, seen) = worker();
    seen.stall.store(true, Ordering::SeqCst);
    service.attach(owner)?;
    let closing = service.clone();
    let task = tokio::spawn(async move { closing.shutdown().await });
    seen.stopping.notified().await;
    assert_eq!(service.snapshot().phase, RuntimePhase::Stopping);
    assert!(service.clone().shutdown().await.is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
    assert!(
        !service
            .snapshot()
            .participation_applied(&saved(1, ContributionMode::Auto))
    );
    assert!(service.client().is_err());
    assert!(service.clone().shutdown().await.is_err());
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, _changes, seen) = worker();
    seen.fail.store(true, Ordering::SeqCst);
    service.attach(owner)?;
    assert!(service.clone().shutdown().await.is_err());
    assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
    assert_eq!(
        service.snapshot().participation.unwrap().applied_roles,
        None
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_stalled_shutdown_has_a_deadline_and_drops_its_owned_lifecycle() -> Result<()> {
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, _changes, seen) = worker();
    seen.stall.store(true, Ordering::SeqCst);
    service.attach(owner)?;
    assert!(service.clone().shutdown().await.is_err());
    assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn success_without_terminal_status_and_application_drop_are_not_cleanup_proof() -> Result<()>
{
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, _changes, seen) = worker();
    seen.false_success.store(true, Ordering::SeqCst);
    service.attach(owner)?;
    assert!(service.clone().shutdown().await.is_err());
    assert_eq!(service.snapshot().phase, RuntimePhase::Failed);
    let service = Arc::new(Managed::<Worker>::default());
    let (owner, _changes, seen) = worker();
    let client = owner.client();
    service.attach(owner)?;
    let cloned = service.clone();
    drop(service);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 0);
    drop(cloned);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(client.snapshot().phase, ParticipationPhase::Failed);
    Ok(())
}
