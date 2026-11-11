use super::*;
use elixir_marketplace::catalog::SearchQuery;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

mod blob;
pub(in crate::marketplace) use blob::owner as synthetic_blob_owner;
mod mirror;
#[cfg(unix)]
pub(in crate::marketplace) mod peer_pipe;
mod retention;

fn confirmed() -> ParticipationSettings {
    ParticipationSettings {
        revision: Some(1),
        mode: ContributionMode::Auto,
        metering: Metering::Unmetered,
        accepted_disclosure_version: Some(PARTICIPATION_DISCLOSURE_VERSION),
    }
}
fn healthy() -> metrics::Facts {
    metrics::Facts {
        observed: Instant::now(),
        available_memory: Some(1024 * 1024 * 1024),
        disk_headroom: true,
        resource_pressure: false,
        foreground_resource_pressure: false,
        playback_pressure: false,
        background_allowance: true,
    }
}

#[test]
fn population_keeps_one_peer_until_scan_completion_but_rotates_on_failure_or_departure()
-> Result<()> {
    let peers: Vec<PeerKey> = ["1", "2", "3"]
        .map(|digit| PeerKey::try_from(digit.repeat(64)).unwrap())
        .to_vec();
    let mut cursor = PopulationCursor::default();
    let mut state = CatalogJobState {
        phase: CatalogJobPhase::Idle,
        kind: None,
        peer: None,
        admitted: 0,
        unchanged: 0,
        rejected: 0,
        indexed: 0,
        removed: 0,
    };
    assert_eq!(cursor.choose(&state, &[]), None);
    assert_eq!(cursor.choose(&state, &peers), Some(peers[0].clone()));
    state.kind = Some(CatalogJobKind::Population);
    state.peer = Some(peers[0].clone());
    state.phase = CatalogJobPhase::Progress;
    for _ in 0..500 {
        assert_eq!(cursor.choose(&state, &peers), Some(peers[0].clone()));
    }
    // Local maintenance and transient roster reordering must not switch scans.
    state.kind = Some(CatalogJobKind::Maintenance);
    state.peer = None;
    state.phase = CatalogJobPhase::Complete;
    assert_eq!(
        cursor.choose(
            &state,
            &[peers[2].clone(), peers[0].clone(), peers[1].clone()]
        ),
        Some(peers[0].clone())
    );
    state.kind = Some(CatalogJobKind::Population);
    state.peer = Some(peers[0].clone());
    assert_eq!(cursor.choose(&state, &peers), Some(peers[1].clone()));
    for phase in [
        CatalogJobPhase::Failed,
        CatalogJobPhase::Limited,
        CatalogJobPhase::Cancelled,
    ] {
        state.phase = phase;
        state.peer = cursor.peer.clone();
        let old = cursor.peer.clone();
        assert_ne!(cursor.choose(&state, &peers), old);
    }
    state.kind = None;
    state.peer = None;
    state.phase = CatalogJobPhase::Idle;
    let old = cursor.peer.clone().unwrap();
    let remaining: Vec<PeerKey> = peers.into_iter().filter(|p| *p != old).collect();
    assert_ne!(cursor.choose(&state, &remaining), Some(old));
    assert_eq!(cursor.choose(&state, &[]), None);
    Ok(())
}

#[tokio::test]
async fn cancelled_security_commit_fail_stops_but_read_cancellation_does_not() -> Result<()> {
    for mutating in [true, false] {
        let (mut worker, _settings, _observed) = fixture();
        let (source, _source_owner) =
            super::super::source_preflight::Client::fixture_for_security_storage()?;
        worker.client.source_preflight = Some(source);
        let client = worker.client();
        let task_client = client.clone();
        let (begun, started) = oneshot::channel();
        let task = tokio::spawn(async move {
            task_client
                .security_result(
                    async move {
                        let _ = begun.send(());
                        std::future::pending::<Result<()>>().await
                    },
                    mutating,
                )
                .await
        });
        started.await?;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(client.source_preflight.as_ref().unwrap().failed(), mutating);
        if mutating {
            assert_eq!(client.snapshot().phase, ParticipationPhase::Failed);
        }
        worker.stop().await?;
    }
    Ok(())
}

#[tokio::test]
async fn expected_security_rejections_are_local_but_storage_failure_stops_claims() -> Result<()> {
    for expected in [true, false] {
        let (mut worker, _settings, _observed) = fixture();
        let (source, _source_owner) =
            super::super::source_preflight::Client::fixture_for_security_storage()?;
        worker.client.source_preflight = Some(source);
        let client = worker.client();
        let error = if expected {
            anyhow::Error::from(crate::marketplace::security_state::Rejection::Stale)
        } else {
            anyhow::anyhow!("synthetic database integrity failure")
        };
        assert!(
            client
                .security_result(async move { Err::<(), _>(error) }, true)
                .await
                .is_err()
        );
        assert_eq!(
            client.source_preflight.as_ref().unwrap().failed(),
            !expected
        );
        worker.stop().await?;
    }
    Ok(())
}
async fn step(seconds: u64) {
    tokio::time::advance(Duration::from_secs(seconds)).await;
    for _ in 0..12 {
        tokio::task::yield_now().await;
    }
}
fn query() -> PeerOperation {
    PeerOperation::Search {
        query: SearchQuery {
            text: String::new(),
            tags: vec![],
            limit: 1,
        },
        cursor: None,
    }
}
fn fixture_peer() -> PeerKey {
    PeerKey::try_from("1".repeat(64)).unwrap()
}
fn fixture_local_head() -> ExtensionId {
    "elx:gr2q7gf5lh6pzfdnurnkvputhpqvjjfvbfhbysv7ikdgkbptzf7a/fixture-provider"
        .parse()
        .unwrap()
}

#[tokio::test]
async fn only_typed_predispatch_deferral_retains_the_original_receiver() -> Result<()> {
    let (reply, mut received) = oneshot::channel::<Result<u32>>();
    let reply = reply_or_wait(reply, Err(super::super::ProtectionCheckInProgress.into()))
        .context("the known-unsent request must remain pending")?;
    assert!(matches!(
        received.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    // Keep the SAME receiver through another observation, not a second RPC.
    let reply = reply_or_wait(reply, Err(super::super::ProtectionCheckInProgress.into()))
        .context("same pending request")?;
    assert!(reply_or_wait(reply, Ok(7)).is_none());
    assert_eq!(received.await??, 7);

    for message in [
        "marketplace protection is undergoing a routine check",
        "private transport acknowledgement outcome unknown",
    ] {
        let (reply, received) = oneshot::channel::<Result<()>>();
        assert!(reply_or_wait(reply, Err(anyhow::anyhow!(message))).is_none());
        assert_eq!(received.await?.unwrap_err().to_string(), message);
    }
    Ok(())
}

#[tokio::test]
async fn a_cancelled_predispatch_request_keeps_its_closed_original_receiver() {
    let (reply, received) = oneshot::channel::<Result<()>>();
    let reply = reply_or_wait(reply, Err(super::super::ProtectionCheckInProgress.into())).unwrap();
    drop(received);
    assert!(
        reply.is_closed(),
        "the actor must discard this request before any IPC"
    );
}

#[tokio::test(start_paused = true)]
async fn retiring_a_failed_owner_requires_joined_cleanup_not_just_failed_status() -> Result<()> {
    for unknown_cleanup in [false, true] {
        let (owner, settings, state) = fixture();
        let client = owner.client();
        warmed(&client).await;
        state.fail_stop.store(unknown_cleanup, Ordering::SeqCst);
        let mut lookup = crate::marketplace::provider_lookup::Lookup::fixture_unconfirmed(&client);
        assert!(
            format!("{:#}", lookup.wait().await.unwrap_err())
                .contains("original synthetic RPC outcome unknown")
        );
        drop(settings);
        observed(&client, |snapshot| {
            snapshot.phase == ParticipationPhase::Failed
        })
        .await;
        let result = owner.retire().await;
        assert_eq!(result.is_err(), unknown_cleanup);
        assert_eq!(lookup.finish().await.is_err(), unknown_cleanup);
        assert_eq!(lookup.finish().await.is_err(), unknown_cleanup);
        assert!(
            format!("{:#}", lookup.wait().await.unwrap_err())
                .contains("original synthetic RPC outcome unknown")
        );
        let proof = client
            .original_retirement(Instant::now() + Duration::from_secs(1))
            .await;
        assert_eq!(proof.is_err(), unknown_cleanup);
        if let Ok(proof) = proof {
            assert!(client.accepts_retirement(&proof));
            let (replacement, _replacement_settings, _) = fixture();
            assert!(!replacement.client().accepts_retirement(&proof));
            replacement.stop().await?;
        }
        assert_eq!(client.snapshot().phase, ParticipationPhase::Failed);
        assert_eq!(client.snapshot().applied_roles.is_some(), !unknown_cleanup);
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_lookup_cleanup_waits_for_the_same_original_owner_without_consuming_its_failure()
-> Result<()> {
    let (owner, _settings, _) = fixture();
    let client = owner.client();
    warmed(&client).await;
    let mut lookup = crate::marketplace::provider_lookup::Lookup::fixture_unconfirmed(&client);
    assert!(
        timeout(Duration::from_millis(10), lookup.finish())
            .await
            .is_err()
    );
    owner.retire().await?;
    lookup.finish().await?;
    lookup.finish().await?;
    assert!(
        format!("{:#}", lookup.wait().await.unwrap_err())
            .contains("original synthetic RPC outcome unknown")
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn original_protection_failure_survives_participation_stop_and_joined_retirement()
-> Result<()> {
    for parent_failed in [false, true] {
        for unknown_cleanup in [false, true] {
            for retire_only in [false, true] {
                let (owner, settings, state) = fixture();
                let client = owner.client();
                warmed(&client).await;
                state.fail_protection.store(true, Ordering::SeqCst);
                state.fail_stop.store(unknown_cleanup, Ordering::SeqCst);
                if parent_failed {
                    drop(settings);
                    observed(&client, |snapshot| {
                        snapshot.phase == ParticipationPhase::Failed
                    })
                    .await;
                }
                if retire_only {
                    assert_eq!(owner.retire().await.is_err(), unknown_cleanup);
                } else {
                    let report = format!("{:#}", owner.stop().await.unwrap_err());
                    assert!(
                        report.contains("fixture original protection failure"),
                        "{report}"
                    );
                    if parent_failed {
                        // Closing the settings watch is detected either by
                        // changed() waking or by the next loop's owner check.
                        // Both must retain the original dependency failure.
                        assert!(
                            report.contains("participation settings owner stopped")
                                || report.contains("participation observation owner stopped"),
                            "{report}"
                        );
                    }
                }
                assert_eq!(client.snapshot().phase, ParticipationPhase::Failed);
                assert_eq!(client.snapshot().applied_roles.is_some(), !unknown_cleanup);
                assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
            }
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn engine_requires_disclosure_unmetered_headroom_and_healthy_cooldown_even_for_on()
-> Result<()> {
    let limits = OrdinaryLimits::default();
    let mut engine = Engine::new();
    let mut settings = ParticipationSettings::default();
    for _ in 0..61 {
        let result = engine.decide(settings, Some(&healthy()), ProtectionState::Ready, &limits)?;
        assert!(result.desired_roles.consumer);
        assert!(!result.desired_roles.catalog);
        step(10).await;
    }
    settings.mode = ContributionMode::On;
    let result = engine.decide(settings, Some(&healthy()), ProtectionState::Ready, &limits)?;
    assert!(!result.desired_roles.catalog);
    assert!(result.blockers.contains(&Blocker::DisclosureRequired));
    settings = confirmed();
    for _ in 0..60 {
        assert!(
            !engine
                .decide(settings, Some(&healthy()), ProtectionState::Ready, &limits)?
                .desired_roles
                .catalog
        );
        step(10).await;
    }
    let result = engine.decide(settings, Some(&healthy()), ProtectionState::Ready, &limits)?;
    assert!(result.desired_roles.catalog);
    assert!(!result.desired_roles.routing);
    assert!(!result.package_sharing_active);
    assert!(!result.routing_qualified);
    settings.metering = Metering::Metered;
    assert!(
        !engine
            .decide(settings, Some(&healthy()), ProtectionState::Ready, &limits)?
            .desired_roles
            .catalog
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn missing_stale_and_pressured_host_samples_cannot_become_contribution_authority()
-> Result<()> {
    let mut engine = Engine::new();
    let limits = OrdinaryLimits::default();
    let old = healthy();
    step(31).await;
    let result = engine.decide(confirmed(), Some(&old), ProtectionState::Ready, &limits)?;
    assert!(!result.host_sample_current);
    assert!(!result.desired_roles.catalog);
    assert!(result.desired_roles.consumer);
    let result = engine.decide(confirmed(), None, ProtectionState::Ready, &limits)?;
    assert!(result.desired_roles.consumer);
    assert!(!result.desired_roles.catalog);
    let result = engine.decide(
        confirmed(),
        Some(&healthy()),
        ProtectionState::Expired,
        &limits,
    )?;
    assert!(!result.desired_roles.consumer);
    assert!(!result.desired_roles.catalog);
    Ok(())
}

#[derive(Default)]
struct Observed {
    roles: Mutex<Vec<NetworkRoles>>,
    background: Mutex<Vec<(bool, bool)>>,
    background_requests_completed: Mutex<Vec<usize>>,
    requests_completed: AtomicUsize,
    request_order: Mutex<Vec<bool>>,
    transfer_completed: AtomicUsize,
    defer_requests: AtomicBool,
    deferred: Notify,
    fail_background: AtomicBool,
    shutdowns: AtomicUsize,
    fail_catalog: AtomicBool,
    fail_stop: AtomicBool,
    fail_protection: AtomicBool,
    sharing_active: AtomicBool,
    stall_request: AtomicBool,
    checking: AtomicBool,
    playback_pressure: AtomicBool,
    resource_pressure: AtomicBool,
    low_memory: AtomicBool,
    no_disk: AtomicBool,
    missing_samples: AtomicBool,
    cancelled: AtomicUsize,
    request_started: Notify,
}
struct Fixture {
    observed: Arc<Observed>,
    protection: watch::Receiver<Snapshot>,
    applied: Option<NetworkRoles>,
}
struct Cancellation(Arc<Observed>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.0.cancelled.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Network for Fixture {
    fn package_sharing_active(&self) -> bool {
        self.observed.sharing_active.load(Ordering::SeqCst)
            && self.applied.is_some_and(|roles| roles.catalog)
    }
    fn protection(&self) -> ProtectionState {
        observed_state(&self.protection)
    }
    fn applied(&self) -> Option<NetworkRoles> {
        self.applied
    }
    async fn apply(&mut self, roles: NetworkRoles) -> Result<bool> {
        self.observed.roles.lock().unwrap().push(roles);
        self.applied = None;
        if self.observed.fail_stop.load(Ordering::SeqCst) {
            bail!("fixture cannot prove off");
        }
        self.applied = Some(NetworkRoles::default());
        if roles.catalog && self.observed.fail_catalog.swap(false, Ordering::SeqCst) {
            bail!("fixture catalog bootstrap failed cleanly");
        }
        if self.protection() == ProtectionState::Checking {
            return Ok(false);
        }
        self.applied = Some(roles);
        Ok(true)
    }
    async fn request(&mut self, request: Request) -> Option<Request> {
        self.observed
            .request_order
            .lock()
            .unwrap()
            .push(request.local_catalog_read());
        if self.observed.defer_requests.load(Ordering::SeqCst)
            && matches!(&request, Request::Peer(..))
        {
            self.observed.deferred.notify_one();
            return Some(request);
        }
        if self.observed.stall_request.load(Ordering::SeqCst) {
            let _cancelled = Cancellation(self.observed.clone());
            self.observed.request_started.notify_one();
            std::future::pending::<()>().await;
        }
        match request {
            Request::Sharing(operation, reply) => {
                use elixir_marketplace::worker_sharing::SharingState;
                let result = if !self.applied.is_some_and(|roles| roles.catalog) {
                    Err(anyhow::anyhow!("fixture contribution is disabled"))
                } else {
                    Ok(match operation {
                        WorkerControlOperation::SharingSet { inventory, .. } => {
                            SharingState::Ready {
                                generation: Some(inventory.generation),
                                blobs: inventory.blobs.len(),
                                serving: false,
                            }
                        }
                        WorkerControlOperation::SharingPoll => SharingState::Ready {
                            generation: None,
                            blobs: 0,
                            serving: false,
                        },
                        WorkerControlOperation::SharingChunk {
                            request, offset, ..
                        } => SharingState::Accepted { request, offset },
                        WorkerControlOperation::SharingAuthorize { request, .. } => {
                            SharingState::Accepted { request, offset: 0 }
                        }
                        _ => panic!("unexpected sharing fixture command"),
                    })
                };
                let _ = reply.send(result);
                self.observed
                    .transfer_completed
                    .fetch_add(1, Ordering::SeqCst);
            }
            Request::Blob(operation, reply)
                if transfer_io(&operation)
                    || matches!(operation, WorkerControlOperation::BlobCancel { .. }) =>
            {
                let result = match operation {
                    WorkerControlOperation::BlobRead { offset, .. } => {
                        BlobReadState::Pending { offset }
                    }
                    WorkerControlOperation::BlobCancel { .. } => BlobReadState::Cancelled,
                    _ => unreachable!(),
                };
                let _ = reply.send(Ok(result));
                self.observed
                    .transfer_completed
                    .fetch_add(1, Ordering::SeqCst);
            }
            Request::Cached(_, reply) => {
                let _ = reply.send(Ok(PeerResponse {
                    schema_version: 1,
                    request_id: Some(1),
                    response: elixir_marketplace::peer::PeerOutcome::Rejected {
                        code: elixir_marketplace::peer::RejectionCode::Stale,
                    },
                }));
            }
            Request::Peer(_, _, reply) | Request::Heads(_, reply) => {
                let _ = reply.send(Ok(WorkerControlResponse {
                    schema_version: 1,
                    request_id: 1,
                    control_response: WorkerControlOutcome::Rejected {
                        code: elixir_marketplace::peer::RejectionCode::Stale,
                    },
                }));
            }
            _ => bail_unreachable_request(),
        }
        self.observed
            .requests_completed
            .fetch_add(1, Ordering::SeqCst);
        None
    }
    async fn background(&mut self, population: bool, maintenance: bool) -> Result<()> {
        self.observed
            .background
            .lock()
            .unwrap()
            .push((population, maintenance));
        self.observed
            .background_requests_completed
            .lock()
            .unwrap()
            .push(self.observed.requests_completed.load(Ordering::SeqCst));
        if self.observed.fail_background.load(Ordering::SeqCst) {
            bail!("fixture private background channel failed");
        }
        Ok(())
    }
    async fn shutdown(self) -> Result<crate::marketplace::lifecycle::Exit> {
        self.observed.shutdowns.fetch_add(1, Ordering::SeqCst);
        let operation = if self.observed.fail_protection.load(Ordering::SeqCst) {
            Err(anyhow::anyhow!("fixture original protection failure"))
        } else {
            Ok(())
        };
        let cleanup = if self.observed.fail_stop.load(Ordering::SeqCst) {
            Err(anyhow::anyhow!("fixture shutdown is uncertain"))
        } else {
            Ok(())
        };
        Ok(crate::marketplace::lifecycle::Exit::new(operation, cleanup))
    }
}
fn bail_unreachable_request() {
    panic!("unexpected fixture command");
}

fn fixture() -> (
    ParticipatingWorker,
    watch::Sender<ParticipationSettings>,
    Arc<Observed>,
) {
    let limits = OrdinaryLimits::default();
    let observed = Arc::new(Observed::default());
    let (settings_send, settings) = watch::channel(confirmed());
    let (facts_send, facts) = watch::channel(Some(healthy()));
    let (protection_send, protection) = watch::channel(Snapshot::timed(
        ProtectionState::Ready,
        Duration::from_secs(15),
    ));
    let sample_state = observed.clone();
    let sampler = metrics::Sampler::fixture(
        limits.clone(),
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let mut sample = healthy();
                sample.playback_pressure = sample_state.playback_pressure.load(Ordering::SeqCst);
                sample.resource_pressure = sample_state.resource_pressure.load(Ordering::SeqCst);
                sample.foreground_resource_pressure = sample.resource_pressure;
                if sample_state.low_memory.load(Ordering::SeqCst) {
                    sample.available_memory = Some(0);
                }
                sample.disk_headroom = !sample_state.no_disk.load(Ordering::SeqCst);
                facts_send.send_replace(
                    (!sample_state.missing_samples.load(Ordering::SeqCst)).then_some(sample),
                );
                protection_send.send_replace(Snapshot::timed(
                    if sample_state.checking.load(Ordering::SeqCst) {
                        ProtectionState::Checking
                    } else {
                        ProtectionState::Ready
                    },
                    Duration::from_secs(15),
                ));
            }
        }),
    );
    let (requests, receiver) = mpsc::channel(1);
    let (transfer_requests, transfer_receiver) = mpsc::channel(1);
    let (local_reads, local_receiver) = mpsc::channel(1);
    let (status, seen) = watch::channel(ParticipationSnapshot::default());
    let (stop, stopped) = oneshot::channel();
    let (retirement_owner, retirement) = retirement::channel();
    let (credits, _) = mpsc::channel(1);
    let (faults, _) = mpsc::channel(1);
    sqlx::any::install_default_drivers();
    let client = ParticipationClient {
        blob_discovery: false,
        blob_sharing: false,
        database: Database {
            driver: crate::db::DatabaseDriver::Sqlite,
            pool: sqlx::any::AnyPoolOptions::new()
                .connect_lazy("sqlite::memory:")
                .unwrap(),
        },
        source_preflight: None,
        source_jobs: None,
        security_mirror: None,
        requests,
        transfer_requests,
        local_reads,
        status: seen,
        protection: protection.clone(),
        credits,
        faults,
        retirement,
    };
    let task = spawn_owned(
        Fixture {
            observed: observed.clone(),
            protection: protection.clone(),
            applied: Some(NetworkRoles::default()),
        },
        limits,
        settings,
        facts,
        protection,
        RequestInbox {
            regular: receiver,
            transfer: transfer_receiver,
            local_reads: local_receiver,
        },
        status,
        retirement_owner,
        stopped,
        Observers {
            _host: sampler,
            _preferences: None,
        },
    );
    (
        ParticipatingWorker {
            client,
            stop: Some(stop),
            task: Some(task),
        },
        settings_send,
        observed,
    )
}
async fn observed(
    client: &ParticipationClient,
    condition: impl Fn(&ParticipationSnapshot) -> bool,
) {
    let mut changed = client.status.clone();
    timeout(Duration::from_secs(1), async {
        loop {
            if condition(&client.snapshot()) {
                return;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
async fn warmed(client: &ParticipationClient) {
    observed(client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.consumer)
    })
    .await;
    for _ in 0..60 {
        step(10).await;
    }
}

#[tokio::test(start_paused = true)]
async fn joined_shutdown_clears_active_sharing_only_when_cleanup_is_confirmed() -> Result<()> {
    for operation_failed in [false, true] {
        for cleanup_unknown in [false, true] {
            let (owner, _settings, state) = fixture();
            let client = owner.client();
            state.sharing_active.store(true, Ordering::SeqCst);
            warmed(&client).await;
            observed(&client, |snapshot| snapshot.package_sharing_active).await;
            state
                .fail_protection
                .store(operation_failed, Ordering::SeqCst);
            state.fail_stop.store(cleanup_unknown, Ordering::SeqCst);
            assert_eq!(
                owner.stop().await.is_err(),
                operation_failed || cleanup_unknown
            );
            let snapshot = client.snapshot();
            assert_eq!(snapshot.package_sharing_active, cleanup_unknown);
            assert_eq!(
                snapshot.applied_roles,
                (!cleanup_unknown).then_some(NetworkRoles::default())
            );
            assert_eq!(
                snapshot.phase,
                if operation_failed || cleanup_unknown {
                    ParticipationPhase::Failed
                } else {
                    ParticipationPhase::Stopped
                }
            );
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn background_population_allows_unmetered_consumers_without_serving_but_respects_metering()
-> Result<()> {
    let (owner, settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    observed(&client, |s| s.applied_roles.is_some_and(|r| r.catalog)).await;
    state.background.lock().unwrap().clear();
    step(10).await;
    assert!(state.background.lock().unwrap().contains(&(true, true)));
    for metering in [Metering::Unmetered, Metering::Metered] {
        settings.send_replace(ParticipationSettings {
            mode: ContributionMode::Off,
            metering,
            ..confirmed()
        });
        observed(&client, |s| {
            s.applied_roles.is_some_and(|r| r.consumer && !r.catalog)
        })
        .await;
        state.background.lock().unwrap().clear();
        step(10).await;
        let attempts = state.background.lock().unwrap().clone();
        assert!(!attempts.is_empty());
        assert!(
            attempts
                .iter()
                .all(|attempt| *attempt == (metering == Metering::Unmetered, true))
        );
    }
    owner.stop().await?;
    let count = state.background.lock().unwrap().len();
    step(20).await;
    assert_eq!(state.background.lock().unwrap().len(), count);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unsafe_or_unknown_host_facts_and_guard_checks_do_not_start_background_work() -> Result<()>
{
    for reason in 0..6 {
        let (owner, _settings, state) = fixture();
        let client = owner.client();
        warmed(&client).await;
        match reason {
            0 => state.playback_pressure.store(true, Ordering::SeqCst),
            1 => state.resource_pressure.store(true, Ordering::SeqCst),
            2 => state.low_memory.store(true, Ordering::SeqCst),
            3 => state.no_disk.store(true, Ordering::SeqCst),
            4 => state.missing_samples.store(true, Ordering::SeqCst),
            _ => state.checking.store(true, Ordering::SeqCst),
        }
        step(10).await;
        state.background.lock().unwrap().clear();
        step(10).await;
        let attempts = state.background.lock().unwrap().clone();
        assert!(!attempts.is_empty(), "{reason}");
        assert!(
            attempts.iter().all(|attempt| *attempt == (false, false)),
            "{reason}: {attempts:?}"
        );
        assert!(client.request(query()).await.is_ok());
        owner.stop().await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn queued_foreground_work_precedes_background_ticks_and_bad_control_is_terminal() -> Result<()>
{
    let (owner, _settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    state.background.lock().unwrap().clear();
    state.background_requests_completed.lock().unwrap().clear();
    let (send, reply) = oneshot::channel();
    assert!(
        client
            .local_reads
            .try_send(Request::Cached(query(), send))
            .is_ok()
    );
    step(10).await;
    assert!(reply.await?.is_ok());
    // The request may drain before the tick. Work after that is legitimate;
    // no background admission may precede the already queued request.
    assert!(
        state
            .background_requests_completed
            .lock()
            .unwrap()
            .iter()
            .all(|count| *count >= 1)
    );
    state.fail_background.store(true, Ordering::SeqCst);
    step(10).await;
    observed(&client, |s| s.phase == ParticipationPhase::Failed).await;
    assert!(owner.stop().await.is_err());
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn queued_local_read_has_one_priority_turn_without_delaying_an_idle_read() -> Result<()> {
    let (owner, _settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;

    async fn compete(client: &ParticipationClient) -> Result<()> {
        let (regular_send, regular_reply) = oneshot::channel();
        let (local_send, local_reply) = oneshot::channel();
        // Fill both slots before yielding, with regular arriving first.
        client
            .requests
            .try_send(Request::Peer(fixture_peer(), query(), regular_send))
            .ok()
            .context("regular slot")?;
        client
            .local_reads
            .try_send(Request::Cached(query(), local_send))
            .ok()
            .context("local slot")?;
        assert!(local_reply.await??.request_id.is_some());
        assert_eq!(regular_reply.await??.request_id, 1);
        Ok(())
    }

    compete(&client).await?;
    assert_eq!(*state.request_order.lock().unwrap(), [true, false]);
    // Local can run even after using its preferred turn, if nobody else
    // is ready. A subsequent contest still owes regular work its turn.
    for _ in 0..2 {
        assert!(
            timeout(Duration::from_millis(50), client.request(query()))
                .await??
                .request_id
                .is_some()
        );
    }
    compete(&client).await?;
    assert_eq!(
        *state.request_order.lock().unwrap(),
        [true, false, true, true, false, true]
    );
    owner.stop().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deferred_foreground_requests_pause_new_background_work_without_blocking_off_or_owner_stop()
-> Result<()> {
    let (owner, settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    assert!(client.same_owner(&client.clone()));
    let (other, _other_settings, _other_state) = fixture();
    assert!(!client.same_owner(&other.client()));
    other.stop().await?;
    state.defer_requests.store(true, Ordering::SeqCst);
    let asking = client.clone();
    let task = tokio::spawn(async move { asking.request_peer(fixture_peer(), query()).await });
    state.deferred.notified().await;
    state.background.lock().unwrap().clear();
    settings.send_replace(ParticipationSettings {
        mode: ContributionMode::Off,
        ..confirmed()
    });
    observed(&client, |s| {
        s.applied_roles
            .is_some_and(|roles| roles.consumer && !roles.catalog)
    })
    .await;
    step(10).await;
    assert!(state.background.lock().unwrap().is_empty());
    assert!(!task.is_finished());
    state.defer_requests.store(false, Ordering::SeqCst);
    step(1).await;
    assert_eq!(task.await??.request_id, 1);
    owner.stop().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deferred_wan_request_and_full_regular_slot_do_not_block_local_catalog_reads() -> Result<()>
{
    let (owner, _settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    state.defer_requests.store(true, Ordering::SeqCst);
    let asking = client.clone();
    let original = tokio::spawn(async move { asking.request_peer(fixture_peer(), query()).await });
    state.deferred.notified().await;
    let (queued, mut queued_reply) = oneshot::channel();
    client
        .requests
        .try_send(Request::Peer(fixture_peer(), query(), queued))
        .ok()
        .context("one queued WAN request")?;
    let id = fixture_local_head();
    timeout(Duration::from_millis(50), async {
        assert!(client.request(query()).await?.request_id.is_some());
        let (send, reply) = oneshot::channel();
        assert!(
            client
                .enqueue_after_capacity(
                    Request::Cached(
                        PeerOperation::GetHead {
                            extension_id: id.clone()
                        },
                        send
                    ),
                    reply
                )
                .await?
                .request_id
                .is_some()
        );
        let (send, reply) = oneshot::channel();
        assert_eq!(
            client
                .enqueue_after_capacity(Request::Heads(vec![id], send), reply)
                .await?
                .request_id,
            1
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("local reads waited behind deferred WAN work")??;
    assert_eq!(state.requests_completed.load(Ordering::SeqCst), 3);
    assert!(!original.is_finished());
    assert!(matches!(
        queued_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(client.requests.capacity(), 0);
    state.defer_requests.store(false, Ordering::SeqCst);
    step(1).await;
    assert_eq!(original.await??.request_id, 1);
    assert_eq!(queued_reply.await??.request_id, 1);
    assert_eq!(state.requests_completed.load(Ordering::SeqCst), 5);
    owner.stop().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deferred_metadata_and_full_regular_slot_do_not_starve_transfer_io() -> Result<()> {
    use elixir_marketplace::{identity::Digest, worker_sharing::SharingInventory};
    let (mut owner, _settings, state) = fixture();
    owner.client.blob_sharing = true;
    let client = owner.client();
    warmed(&client).await;
    state.checking.store(true, Ordering::SeqCst);
    step(10).await;
    assert_eq!(
        observed_state(&client.protection),
        ProtectionState::Checking
    );
    state.defer_requests.store(true, Ordering::SeqCst);
    let asking = client.clone();
    let original = tokio::spawn(async move { asking.request_peer(fixture_peer(), query()).await });
    state.deferred.notified().await;
    let (queued, queued_reply) = oneshot::channel();
    client
        .requests
        .try_send(Request::Peer(fixture_peer(), query(), queued))
        .ok()
        .context("one queued metadata request")?;
    assert_eq!(client.requests.capacity(), 0);
    let generation = Digest::of(b"same installed sharing generation");
    // Keep renewing past the original fifteen-second lease, while the same
    // regular request remains known-unsent. No fresh START or discovery.
    for _ in 0..6 {
        client
            .sharing_control(WorkerControlOperation::SharingSet {
                inventory: SharingInventory {
                    generation,
                    blobs: vec![],
                },
                lease_ms: 15_000,
                resolve_installed: true,
            })
            .await?;
        client
            .sharing_control(WorkerControlOperation::SharingPoll)
            .await?;
        let slot = client
            .reserve_blob(WorkerControlOperation::BlobRead {
                job: generation,
                offset: 0,
            })
            .await?;
        assert!(matches!(
            slot.send()?.wait().await?,
            BlobReadState::Pending { offset: 0 }
        ));
        step(3).await;
        assert!(!original.is_finished());
        assert_eq!(client.requests.capacity(), 0);
    }
    assert_eq!(state.transfer_completed.load(Ordering::SeqCst), 18);
    state.defer_requests.store(false, Ordering::SeqCst);
    step(1).await;
    original.await??;
    queued_reply.await??;
    assert_eq!(state.requests_completed.load(Ordering::SeqCst), 20);
    // Cancellation remains on the regular FIFO, behind a potentially unsent
    // START; it must not acknowledge a no-op then let that START run later.
    assert!(!transfer_io(&WorkerControlOperation::BlobCancel {
        job: generation
    }));
    let slot = client
        .reserve_blob(WorkerControlOperation::BlobCancel { job: generation })
        .await?;
    assert!(matches!(
        slot.send()?.wait().await?,
        BlobReadState::Cancelled
    ));
    owner.stop().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn all_lanes_remain_bounded_and_stop_cancels_active_and_queued_calls() -> Result<()> {
    let (owner, _settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    state.stall_request.store(true, Ordering::SeqCst);
    let asking = client.clone();
    let original = tokio::spawn(async move { asking.request(query()).await });
    state.request_started.notified().await;
    let (regular, regular_reply) = oneshot::channel();
    client
        .requests
        .try_send(Request::Peer(fixture_peer(), query(), regular))
        .ok()
        .context("fill regular lane")?;
    let (transfer, transfer_reply) = oneshot::channel();
    client
        .transfer_requests
        .try_send(Request::Sharing(
            WorkerControlOperation::SharingPoll,
            transfer,
        ))
        .ok()
        .context("fill transfer lane")?;
    let (local, local_reply) = oneshot::channel();
    client
        .local_reads
        .try_send(Request::Cached(query(), local))
        .ok()
        .context("fill local lane")?;
    assert_eq!(client.requests.max_capacity(), 1);
    assert_eq!(client.transfer_requests.max_capacity(), 1);
    assert_eq!(client.local_reads.max_capacity(), 1);
    let (extra, extra_reply) = oneshot::channel();
    let error = client
        .enqueue(
            Request::Sharing(WorkerControlOperation::SharingPoll, extra),
            extra_reply,
        )
        .await
        .unwrap_err();
    assert!(ParticipationClient::queue_busy(&error));
    owner.stop().await?;
    assert!(original.await?.is_err());
    assert!(regular_reply.await.is_err());
    assert!(transfer_reply.await.is_err());
    assert!(local_reply.await.is_err());
    assert_eq!(state.cancelled.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn transfer_lane_does_not_bypass_off_while_metadata_is_deferred() -> Result<()> {
    let (mut owner, settings, state) = fixture();
    owner.client.blob_sharing = true;
    let client = owner.client();
    warmed(&client).await;
    state.defer_requests.store(true, Ordering::SeqCst);
    let asking = client.clone();
    let original = tokio::spawn(async move { asking.request_peer(fixture_peer(), query()).await });
    state.deferred.notified().await;
    settings.send_replace(ParticipationSettings {
        mode: ContributionMode::Off,
        ..confirmed()
    });
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| !roles.catalog)
    })
    .await;
    assert!(
        client
            .sharing_control(WorkerControlOperation::SharingPoll)
            .await
            .is_err()
    );
    assert_eq!(state.transfer_completed.load(Ordering::SeqCst), 0);
    assert!(!original.is_finished());
    owner.stop().await?;
    assert!(original.await?.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn automatic_catalog_contribution_and_owner_off_preserve_the_protected_consumer() -> Result<()>
{
    let (owner, settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.catalog)
    })
    .await;
    settings.send_replace(ParticipationSettings {
        mode: ContributionMode::Off,
        ..confirmed()
    });
    observed(&client, |snapshot| {
        snapshot
            .applied_roles
            .is_some_and(|roles| roles.consumer && !roles.catalog && !roles.routing)
    })
    .await;
    assert!(client.snapshot().blockers.contains(&Blocker::OwnerDisabled));
    client.request(query()).await?;
    owner.stop().await?;
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(client.snapshot().phase, ParticipationPhase::Stopped);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn clean_catalog_failure_falls_back_to_consumer_without_an_automatic_retry_storm()
-> Result<()> {
    let (owner, _settings, state) = fixture();
    state.fail_catalog.store(true, Ordering::SeqCst);
    let client = owner.client();
    warmed(&client).await;
    observed(&client, |snapshot| {
        snapshot
            .applied_roles
            .is_some_and(|roles| roles.consumer && !roles.catalog)
            && snapshot
                .recovery_seconds
                .is_some_and(|seconds| seconds > 500)
    })
    .await;
    assert_eq!(
        state
            .roles
            .lock()
            .unwrap()
            .iter()
            .filter(|roles| roles.catalog)
            .count(),
        1
    );
    for _ in 0..50 {
        step(10).await;
    }
    assert_eq!(
        state
            .roles
            .lock()
            .unwrap()
            .iter()
            .filter(|roles| roles.catalog)
            .count(),
        1
    );
    for _ in 0..10 {
        step(10).await;
    }
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.catalog)
    })
    .await;
    owner.stop().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_lost_settings_owner_is_terminal_and_cannot_leave_detached_contribution() -> Result<()> {
    let (owner, settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    drop(settings);
    observed(&client, |snapshot| {
        snapshot.phase == ParticipationPhase::Failed
    })
    .await;
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    assert!(client.request(query()).await.is_err());
    assert!(owner.stop().await.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn one_request_slot_rejects_overflow_and_owner_stop_preempts_a_stalled_catalog_caller()
-> Result<()> {
    let (owner, _settings, state) = fixture();
    let client = owner.client();
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.consumer)
    })
    .await;
    state.stall_request.store(true, Ordering::SeqCst);
    let other = client.clone();
    let active = tokio::spawn(async move { other.request(query()).await });
    state.request_started.notified().await;
    let (send, queued) = oneshot::channel();
    assert!(
        client
            .local_reads
            .try_send(Request::Cached(query(), send))
            .is_ok()
    );
    assert!(
        client
            .request(query())
            .await
            .unwrap_err()
            .to_string()
            .contains("slot")
    );
    owner.stop().await?;
    assert!(active.await?.is_err());
    assert!(queued.await.is_err());
    assert_eq!(state.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_uncertain_stop_never_acknowledges_contribution_off() -> Result<()> {
    let (owner, settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.catalog)
    })
    .await;
    state.fail_stop.store(true, Ordering::SeqCst);
    settings.send_replace(ParticipationSettings {
        mode: ContributionMode::Off,
        ..confirmed()
    });
    observed(&client, |snapshot| {
        snapshot.phase == ParticipationPhase::Failed
    })
    .await;
    assert!(client.snapshot().applied_roles.is_none());
    assert!(owner.stop().await.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn playback_and_unknown_samples_pause_contribution_and_require_healthy_recovery() -> Result<()>
{
    let (owner, _settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.catalog)
    })
    .await;
    state.playback_pressure.store(true, Ordering::SeqCst);
    step(10).await;
    observed(&client, |snapshot| {
        snapshot.blockers.contains(&Blocker::PlaybackPressure)
            && snapshot
                .applied_roles
                .is_some_and(|roles| roles.consumer && !roles.catalog)
    })
    .await;
    state.playback_pressure.store(false, Ordering::SeqCst);
    step(10).await;
    observed(&client, |snapshot| {
        snapshot.blockers.contains(&Blocker::RecoveryCooldown)
    })
    .await;
    for _ in 0..60 {
        step(10).await;
    }
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.catalog)
    })
    .await;
    state.missing_samples.store(true, Ordering::SeqCst);
    step(10).await;
    observed(&client, |snapshot| {
        !snapshot.host_sample_current
            && snapshot.blockers.contains(&Blocker::ResourcePressure)
            && snapshot
                .applied_roles
                .is_some_and(|roles| roles.consumer && !roles.catalog)
    })
    .await;
    assert!(client.request(query()).await.is_ok());
    owner.stop().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn bounded_guard_checks_do_not_flap_already_active_catalog_roles() -> Result<()> {
    let (owner, _settings, state) = fixture();
    let client = owner.client();
    warmed(&client).await;
    observed(&client, |snapshot| {
        snapshot.applied_roles.is_some_and(|roles| roles.catalog)
    })
    .await;
    let before = state.roles.lock().unwrap().len();
    state.checking.store(true, Ordering::SeqCst);
    step(10).await;
    let snapshot = client.snapshot();
    assert_eq!(snapshot.phase, ParticipationPhase::Running);
    assert!(snapshot.applied_roles.is_some_and(|roles| roles.catalog));
    assert_eq!(state.roles.lock().unwrap().len(), before);
    state.checking.store(false, Ordering::SeqCst);
    step(10).await;
    assert_eq!(state.roles.lock().unwrap().len(), before);
    owner.stop().await?;
    Ok(())
}
