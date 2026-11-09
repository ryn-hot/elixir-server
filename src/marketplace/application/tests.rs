//! Pure action and actual future-layout checks, NOT native/source proof.
use super::*;
#[test]
fn sdk_upload_start_cannot_acknowledge_a_peer_download_with_the_same_reference() -> Result<()> {
    let p = principal();
    let actor = Actor::from_principal(&p, false)?;
    let release = Digest::of(b"same signed source");
    let request = initial_request(release);
    let reference = Digest::of(b"same explicit reference");
    let peer =
        direct_start_fingerprint(&actor, request.query_id, &request.input, reference, false)?;
    let sdk = direct_start_fingerprint(&actor, request.query_id, &request.input, reference, true)?;
    assert_ne!(peer, sdk);
    Ok(())
}
use std::{
    future::Future,
    mem::{size_of, size_of_val},
};

fn principal() -> CurrentPrincipal {
    CurrentPrincipal::fixture_owner(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        1,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        crate::authz::CapabilitySet::all(),
    )
}
fn initial_request(release: Digest) -> InitialRequest {
    let review = crate::marketplace::grants::fixture_review().unwrap();
    InitialRequest::new(
        Uuid::new_v4(),
        &InstallAction {
            extension: review.scope().extension_id().clone(),
            release,
            target: installation::InstallTarget::New {
                name: "Test input DATA".into(),
            },
            expected_revision: 0,
            mutation: Uuid::new_v4(),
            controls: installation::ExpectedControls::Absent {},
        },
    )
}

#[test]
fn large_feed_execution_requires_its_original_memory_and_total_job_deadline() {
    let mut allocation = native_tests::allocation();
    allocation.execution = SourceExecutionLimits::large_feed();
    allocation.source.memory_bytes = 256 * 1024 * 1024;
    allocation.source.wall_seconds = 120;
    assert!(
        allocation.validate().is_err(),
        "standard control budget cannot cover a heavy action"
    );
    allocation.source.input_bytes = 512 * 1024 * 1024;
    allocation.source.output_bytes = 128 * 1024 * 1024;
    assert!(allocation.validate().is_ok());
    allocation.source.memory_bytes = 128 * 1024 * 1024;
    assert!(allocation.validate().is_err());
    allocation.source.memory_bytes = 256 * 1024 * 1024;
    allocation.source.wall_seconds = 89;
    assert!(allocation.validate().is_err());
}
#[test]
fn action_phases_do_not_turn_status_into_a_generic_setup_or_activation_gate() {
    let p = principal();
    let setup = Action::Setup {
        principal: p.clone(),
        mutation: Uuid::new_v4(),
        credentials: vec![],
    };
    let activate = Action::Activate {
        principal: p.clone(),
        mutation: Uuid::new_v4(),
    };
    let retry = Action::RetryStage {
        principal: p.clone(),
        mutation: Uuid::new_v4(),
    };
    let cancel = Action::Cancel {
        principal: p,
        mutation: Uuid::new_v4(),
    };
    assert!(setup.allowed(Phase::AwaitingSetup));
    assert!(!setup.allowed(Phase::Preparing));
    assert!(!setup.allowed(Phase::Installing));
    assert!(activate.allowed(Phase::AwaitingCommit));
    assert!(!activate.allowed(Phase::AwaitingSetup));
    assert!(!activate.allowed(Phase::Preparing));
    assert!(retry.allowed(Phase::RecoveryRequired));
    assert!(!retry.allowed(Phase::Installing));
    assert!(cancel.allowed(Phase::Installing));
    assert!(!cancel.allowed(Phase::Activated));
    assert!(!cancel.allowed(Phase::Cancelled));
}
#[test]
fn nil_setup_commit_and_cleanup_mutations_are_rejected_before_queuing() {
    let p = principal();
    for action in [
        Action::Setup {
            principal: p.clone(),
            mutation: Uuid::nil(),
            credentials: vec![],
        },
        Action::Activate {
            principal: p.clone(),
            mutation: Uuid::nil(),
        },
        Action::Cancel {
            principal: p,
            mutation: Uuid::nil(),
        },
    ] {
        assert!(action.validate().is_err());
    }
}
fn command_future_size<F: Future>(
    _: fn(task::Command, mpsc::Receiver<Message>, watch::Receiver<bool>, watch::Sender<View>) -> F,
) -> usize {
    size_of::<F>()
}
async fn initialize_owned(
    mut command: task::Command,
    status: watch::Sender<View>,
    stop: watch::Receiver<bool>,
) -> Result<()> {
    command.initialize(&status, &stop).await
}
async fn action_owned(
    mut command: task::Command,
    action: Action,
    status: watch::Sender<View>,
    stop: watch::Receiver<bool>,
) -> Result<()> {
    command.action(action, &status, &stop).await
}
async fn process_owned(
    mut command: task::Command,
    message: Message,
    status: watch::Sender<View>,
    stop: watch::Receiver<bool>,
) -> Result<()> {
    command.process_message(message, &status, &stop).await
}
async fn shutdown_owned(mut command: task::Command) -> Result<()> {
    command.shutdown().await
}
fn initialize_size<F: Future>(
    _: fn(task::Command, watch::Sender<View>, watch::Receiver<bool>) -> F,
) -> usize {
    size_of::<F>()
}
fn action_size<F: Future>(
    _: fn(task::Command, Action, watch::Sender<View>, watch::Receiver<bool>) -> F,
) -> usize {
    size_of::<F>()
}
fn process_size<F: Future>(
    _: fn(task::Command, Message, watch::Sender<View>, watch::Receiver<bool>) -> F,
) -> usize {
    size_of::<F>()
}
fn shutdown_size<F: Future>(_: fn(task::Command) -> F) -> usize {
    size_of::<F>()
}
#[test]
fn application_continuations_keep_the_existing_128_kib_frame_bound() {
    for (name, bytes) in [
        ("run", command_future_size(task::run)),
        ("initialize", initialize_size(initialize_owned)),
        ("action", action_size(action_owned)),
        ("process_message", process_size(process_owned)),
        ("shutdown", shutdown_size(shutdown_owned)),
    ] {
        assert!(bytes <= 128 * 1024, "{name} future grew to {bytes} bytes");
    }
    assert!(
        size_of::<task::Command>() <= 16 * 1024,
        "control owner must not embed source bytes"
    );
}
#[test]
fn setup_message_keeps_values_heap_owned_and_out_of_serialized_status() {
    let action = Action::Setup {
        principal: principal(),
        mutation: Uuid::new_v4(),
        credentials: vec![CredentialChange::set("api_key", "private-fixture", false)],
    };
    assert!(
        size_of_val(&action) < 2048,
        "credential values must stay in bounded heap inputs"
    );
    let view = View {
        operation_id: Uuid::new_v4(),
        release_id: Digest::of(b"source"),
        initial_request: initial_request(Digest::of(b"source")),
        phase: Phase::AwaitingSetup,
        preparation: None,
        review: None,
        configured_instance: None,
        revision: None,
        available_actions: vec![ActionKind::Setup],
        latest_action: None,
        activation_request_id: None,
        cancellation_request_id: None,
        installation_ready: false,
    };
    let encoded = serde_json::to_string(&view).unwrap();
    assert!(!encoded.contains("private-fixture"));
    assert!(!view.installation_ready);
}

#[test]
fn setup_payload_limits_apply_before_lookup_or_queueing() {
    let make = |credentials| Action::Setup {
        principal: principal(),
        mutation: Uuid::new_v4(),
        credentials,
    };
    let invalid = vec![
        make(
            (0..65)
                .map(|i| CredentialChange::Remove {
                    key: format!("k{i}"),
                })
                .collect(),
        ),
        make(vec![CredentialChange::set(
            "k",
            "x".repeat(64 * 1024 + 1),
            false,
        )]),
        make(
            (0..16)
                .map(|i| CredentialChange::set(format!("k{i}"), "x".repeat(64 * 1024), false))
                .collect(),
        ),
        make(vec![
            CredentialChange::set("same", "private", false),
            CredentialChange::Remove { key: "same".into() },
        ]),
        make(vec![CredentialChange::Remove {
            key: "x".repeat(65),
        }]),
        make(vec![CredentialChange::Remove {
            key: "bad\nkey".into(),
        }]),
    ];
    let service = Service::default();
    for action in invalid {
        let error = service.submit(Uuid::new_v4(), action).unwrap_err();
        assert!(
            error.to_string().contains("credential"),
            "bounds must precede even command lookup: {error}"
        );
        assert!(service.0.lock().unwrap().entry.is_none());
    }
    make(
        (0..64)
            .map(|i| CredentialChange::Remove {
                key: format!("k{i}"),
            })
            .collect(),
    )
    .validate()
    .unwrap();
    make(vec![CredentialChange::set(
        "k",
        "x".repeat(64 * 1024),
        false,
    )])
    .validate()
    .unwrap();
    make(vec![]).validate().unwrap();
}

#[tokio::test]
async fn cancelled_join_waiter_retains_the_same_original_task_and_result() -> Result<()> {
    let expected = Arc::new(19usize);
    let (send, receive) = tokio::sync::oneshot::channel();
    let mut joined = Joined::Running(tokio::spawn(async move {
        receive.await.expect("test releases original owner")
    }));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), joined.join())
            .await
            .is_err()
    );
    assert!(matches!(&joined,Joined::Running(handle) if !handle.is_finished()));
    send.send(expected.clone()).unwrap();
    assert!(Arc::ptr_eq(joined.join().await?, &expected));
    assert!(
        Arc::ptr_eq(joined.join().await?, &expected),
        "repeated observation borrows the same joined result"
    );
    Ok(())
}

#[tokio::test]
async fn failed_original_join_remains_failed_and_does_not_rearm_itself() {
    let mut joined = Joined::<()>::Running(tokio::spawn(async {
        panic!("synthetic original owner panic")
    }));
    assert!(joined.join().await.is_err());
    assert!(matches!(joined, Joined::Failed));
    assert!(joined.join().await.is_err());
    joined.poll_finished().unwrap();
    assert!(matches!(joined, Joined::Failed));
}

#[tokio::test]
async fn retained_terminal_display_is_owner_bound_data_and_cannot_mint_an_execution_owner()
-> Result<()> {
    let p = principal();
    let release = Digest::of(b"terminal display only");
    let id = Uuid::new_v4();
    let (status, seen) = watch::channel(View {
        operation_id: id,
        release_id: release,
        initial_request: initial_request(release),
        phase: Phase::Cancelled,
        preparation: None,
        review: None,
        configured_instance: None,
        revision: None,
        available_actions: vec![],
        latest_action: None,
        activation_request_id: None,
        cancellation_request_id: Some(Uuid::new_v4()),
        installation_ready: false,
    });
    drop(status);
    let (stop, _) = watch::channel(false);
    let (actions, receiver) = mpsc::channel(1);
    drop(receiver);
    let entry = Arc::new(Entry {
        id,
        user: p.user_id,
        home: p.home_id,
        start: Digest::of(b"not a source receipt"),
        stop,
        status: seen,
        actions,
        acknowledgements: Arc::default(),
        fingerprints: SecretsManager::from_key_bytes([89; 32], false),
        failures: failure::Failures::default(),
        task: AsyncMutex::new(Joined::Running(tokio::spawn(async {
            panic!("synthetic unknown owner, NOT completed native cleanup")
        }))),
    });
    // Deliberately inconsistent DATA with a failed task: no positive native
    // object is manufactured, even when the displayed phase claims cancelled.
    assert!(entry.task.lock().await.join().await.is_err());
    let service = Service::default();
    service.0.lock().unwrap().completed = Some(entry.clone());
    assert_eq!(service.current_for(&p)?, Some(id));
    assert_eq!(service.current_for(&principal())?, None);
    assert_eq!(service.view_for(&p, id)?.phase, Phase::Cancelled);
    assert!(service.view_for(&p, id)?.available_actions.is_empty());
    let allocation = fixture_allocation();
    let error = service
        .begin_installed_call(
            p.home_id,
            crate::marketplace::grants::fixture_review()?
                .scope()
                .extension_id()
                .clone(),
            Uuid::new_v4(),
            elixir_marketplace::source_execution::RuntimeJson::from_value(serde_json::json!({}))?,
            allocation.installations,
            allocation.execution,
        )
        .err()
        .context("terminal display fabricated a controller")?;
    assert!(
        error
            .to_string()
            .contains("no original installed controller")
    );
    assert!(service.0.lock().unwrap().invocation.is_none());
    assert!(service.shutdown().await.is_err());
    assert!(Arc::ptr_eq(
        service.0.lock().unwrap().completed.as_ref().unwrap(),
        &entry
    ));
    Ok(())
}

#[tokio::test]
async fn closed_owner_clears_approval_display_and_failed_join_fences_the_service_slot() -> Result<()>
{
    let p = principal();
    let review = crate::marketplace::grants::fixture_review()?;
    let id = Uuid::new_v4();
    let (status, seen) = watch::channel(View {
        operation_id: id,
        release_id: review.release_id(),
        initial_request: initial_request(review.release_id()),
        phase: Phase::AwaitingApproval,
        preparation: Some(preparation::View {
            schema_version: 1,
            preparation_id: Uuid::new_v4(),
            release_id: review.release_id(),
            phase: preparation::Phase::AwaitingApproval,
            review: Some(review.clone()),
            installation_ready: false,
            upload: None,
            authorization_lost: false,
        }),
        review: Some(review),
        configured_instance: None,
        revision: None,
        available_actions: vec![ActionKind::Approve],
        latest_action: None,
        activation_request_id: None,
        cancellation_request_id: None,
        installation_ready: false,
    });
    drop(status);
    let (stop, _) = watch::channel(false);
    let (actions, receiver) = mpsc::channel(1);
    drop(receiver);
    let entry = Arc::new(Entry {
        id,
        user: p.user_id,
        home: p.home_id,
        start: Digest::of(b"failed owner"),
        stop,
        status: seen,
        actions,
        acknowledgements: Arc::default(),
        fingerprints: SecretsManager::from_key_bytes([91; 32], false),
        failures: failure::Failures::default(),
        task: AsyncMutex::new(Joined::Running(tokio::spawn(async {
            panic!("synthetic app owner panic")
        }))),
    });
    let service = Service::default();
    assert_eq!(service.current_for(&p)?, None);
    entry
        .failures
        .original(anyhow::anyhow!("private original installation failure"));
    entry
        .failures
        .cleanup(anyhow::anyhow!("private installation cleanup failure"));
    service.0.lock().unwrap().entry = Some(entry.clone());
    assert_eq!(service.current_for(&p)?, Some(id));
    let other = principal();
    assert_eq!(service.current_for(&other)?, None);
    assert!(service.view_for(&other, id).is_err());
    let mut foreign = p.clone();
    foreign.home_id = Uuid::new_v4();
    assert_eq!(service.current_for(&foreign)?, None);
    assert!(service.view_for(&foreign, id).is_err());
    let view = service.view(id)?;
    let encoded = serde_json::to_string(&view)?;
    assert!(!encoded.contains("private original installation failure"));
    assert!(!encoded.contains("private installation cleanup failure"));
    let retained = service.fixture_current_failures()?;
    assert!(
        format!("{:#}", retained.0.context("original cause missing")?)
            .contains("private original installation failure")
    );
    assert!(
        format!("{:#}", retained.1.context("cleanup cause missing")?)
            .contains("private installation cleanup failure")
    );
    assert_eq!(view.phase, Phase::RecoveryRequired);
    assert!(view.review.is_none());
    assert!(view.preparation.is_none());
    assert!(view.available_actions.is_empty());
    assert!(!view.installation_ready);
    assert!(entry.task.lock().await.join().await.is_err());
    service.reap_finished()?;
    assert!(Arc::ptr_eq(
        service.0.lock().unwrap().entry.as_ref().unwrap(),
        &entry
    ));
    assert!(
        service.shutdown().await.is_err(),
        "a failed original join cannot claim clean shutdown"
    );
    Ok(())
}
