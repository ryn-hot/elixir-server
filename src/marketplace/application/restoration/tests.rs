//! Actual filesystem/SQL ownership plus negative owned-task checks. No
//! synthetic completion below contains a controller or native disposal proof.
use super::*;
use crate::{
    config::DatabaseConfig, marketplace::lease::ControllerLease, playback::PlaybackJobManager,
    runtime::docker::DockerRuntimeManager,
};
use std::{future::Future, mem::size_of};

async fn host() -> Result<(tempfile::TempDir, Host)> {
    let root = tempfile::tempdir()?;
    let database = Database::connect(&DatabaseConfig {
        url: format!("sqlite://{}", root.path().join("restore.sqlite").display()),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let allocation = super::super::fixture_allocation();
    let directory = root.path().canonicalize()?;
    let runtime = InstalledRuntime {
        runtime: Arc::new(DockerRuntimeManager::new(None)),
        playback: Arc::new(PlaybackJobManager::new(database.pool.clone(), None)),
        local_http: None,
        application_http: None,
        compiled_toolchains: None,
        lane: directory.join("foreground"),
        archive: directory.join("archive"),
        deployment: "restoration-unit".into(),
        image: format!("sha256:{}", "1".repeat(64)),
        source_budget: allocation.source,
        retention: allocation.retention,
    };
    Ok((
        root,
        Host::from_runtime(
            database,
            SecretsManager::from_key_bytes([57; 32], false),
            runtime,
            allocation.installations,
            allocation.execution,
        )?,
    ))
}
fn again(host: &Host) -> Result<Host> {
    Host::from_runtime(
        host.database.clone(),
        host.secrets.clone(),
        host.runtime.clone(),
        host.installations,
        host.execution.clone(),
    )
}
#[tokio::test]
async fn cold_installed_startup_can_restore_before_the_catalog_provider_owns_its_directory()
-> Result<()> {
    let (root, mut host) = host().await?;
    let worker = crate::marketplace::installed_startup::storage::prepare(root.path())?;
    host.runtime.lane = worker.join("foreground-source");
    host.runtime.archive = worker.join("source-archives");
    let saved = again(&host)?;
    let service = Service::default();
    service.begin_restoration(host)?;
    service.finish_restoration().await?;
    // Shared ancestor preparation must not steal the provider's own OS lock.
    let provider = ControllerLease::acquire_for_recovery(&worker)?;
    assert!(service.0.lock().unwrap().controller.is_some());
    service.shutdown().await?;
    drop(service);
    drop(provider);
    let reopened = InstalledController::open(saved.database, saved.secrets, saved.runtime).await?;
    reopened.require_idle()?;
    Ok(())
}
#[tokio::test]
async fn retained_controller_requires_the_same_local_http_owner_not_omission_or_replacement()
-> Result<()> {
    let (_root, mut host) = host().await?;
    host.runtime.local_http = Some(Arc::new(crate::marketplace::local_http::Client::new(
        false,
        host.runtime.playback.clone(),
    )));
    let saved = again(&host)?;
    let controller = InstalledController::open(
        host.database.clone(),
        host.secrets.clone(),
        host.runtime.clone(),
    )
    .await?;
    controller.require_configuration(&saved.runtime, &saved.secrets)?;
    let mut missing = saved.runtime.clone();
    missing.local_http = None;
    assert!(
        controller
            .require_configuration(&missing, &saved.secrets)
            .is_err()
    );
    let mut replacement = saved.runtime.clone();
    replacement.local_http = Some(Arc::new(crate::marketplace::local_http::Client::new(
        false,
        saved.runtime.playback.clone(),
    )));
    assert!(
        controller
            .require_configuration(&replacement, &saved.secrets)
            .is_err()
    );
    // Rejection cannot detach/replace the original owner or make its
    // unchanged, idle configuration unusable.
    controller.require_configuration(&saved.runtime, &saved.secrets)?;
    controller.require_idle()?;
    Ok(())
}
#[tokio::test]
async fn retained_controller_requires_the_original_compiled_toolchain_policy() -> Result<()> {
    use crate::marketplace::runtime::build::ApprovedToolchains;
    let (_root, mut host) = host().await?;
    host.runtime.compiled_toolchains = Some(Arc::new(ApprovedToolchains::default()));
    let saved = again(&host)?;
    let controller = InstalledController::open(
        host.database.clone(),
        host.secrets.clone(),
        host.runtime.clone(),
    )
    .await?;
    controller.require_configuration(&saved.runtime, &saved.secrets)?;
    let mut changed = saved.runtime.clone();
    changed.compiled_toolchains = None;
    assert!(
        controller
            .require_configuration(&changed, &saved.secrets)
            .is_err()
    );
    changed.compiled_toolchains = Some(Arc::new(ApprovedToolchains::default()));
    assert!(
        controller
            .require_configuration(&changed, &saved.secrets)
            .is_err()
    );
    controller.require_configuration(&saved.runtime, &saved.secrets)?;
    controller.require_idle()?;
    Ok(())
}
#[tokio::test]
async fn restored_controller_is_owned_before_any_call_and_clean_reopen_preserves_lineage()
-> Result<()> {
    let (_root, host) = host().await?;
    let saved = again(&host)?;
    let second = again(&host)?;
    let first = Service::default();
    first.begin_restoration(host)?;
    assert!(first.begin_restoration(again(&saved)?).is_err());
    assert!(first.0.lock().unwrap().controller.is_none());
    first.finish_restoration().await?;
    first
        .0
        .lock()
        .unwrap()
        .controller
        .as_ref()
        .unwrap()
        .require_idle()?;
    assert!(first.0.lock().unwrap().restoration.is_none());
    assert!(
        InstalledController::open(
            saved.database.clone(),
            saved.secrets.clone(),
            saved.runtime.clone()
        )
        .await
        .is_err()
    );
    let before: (String, String) = sqlx::query_as(
        "SELECT nonce,binding_sha256 FROM marketplace_installed_runtime_owner WHERE singleton=1",
    )
    .fetch_one(&saved.database.pool)
    .await?;
    first.shutdown().await?;
    drop(first);
    let next = Service::default();
    next.begin_restoration(second)?;
    next.finish_restoration().await?;
    let after: (String, String) = sqlx::query_as(
        "SELECT nonce,binding_sha256 FROM marketplace_installed_runtime_owner WHERE singleton=1",
    )
    .fetch_one(&saved.database.pool)
    .await?;
    assert_eq!(before, after);
    let installed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_installations")
        .fetch_one(&saved.database.pool)
        .await?;
    assert_eq!(installed, 0, "restoration cannot enroll a source");
    next.shutdown().await?;
    drop(next);
    assert!(ControllerLease::acquire(&saved.runtime.lane).is_ok());
    Ok(())
}
#[tokio::test]
async fn changed_lineage_fails_once_without_repair_or_a_replacement_open() -> Result<()> {
    let (_root, host) = host().await?;
    let saved = again(&host)?;
    let first = Service::default();
    first.begin_restoration(host)?;
    first.finish_restoration().await?;
    first.shutdown().await?;
    drop(first);
    let marker = saved
        .runtime
        .lane
        .join("installed-controller")
        .join("owner.binding");
    std::fs::remove_file(&marker)?; // Only this test's owned temporary marker.
    let next = Service::default();
    next.begin_restoration(again(&saved)?)?;
    assert!(next.finish_restoration().await.is_err());
    assert!(!marker.exists());
    assert!(next.0.lock().unwrap().controller.is_none());
    assert!(next.begin_restoration(again(&saved)?).is_err());
    assert!(
        next.shutdown().await.is_err(),
        "failed open is not confirmed idle cleanup"
    );
    Ok(())
}
#[tokio::test]
async fn completed_open_cannot_be_reaped_by_a_call_before_startup_handoff() -> Result<()> {
    let (_root, host) = host().await?;
    let allocation = super::super::fixture_allocation();
    let service = Service::default();
    service.begin_restoration(host)?;
    let original = service.0.lock().unwrap().restoration.clone().unwrap();
    // Actual SQL/filesystem open has joined, but its original startup waiter
    // has NOT handed off the real controller. Ordinary requests cannot steal it.
    original.task.lock().await.join().await?.require_owned()?;
    let call = service.begin_installed_call(
        Uuid::new_v4(),
        crate::marketplace::grants::fixture_review()?
            .scope()
            .extension_id()
            .clone(),
        Uuid::new_v4(),
        elixir_marketplace::source_execution::RuntimeJson::parse(b"null")?,
        allocation.installations,
        allocation.execution,
    );
    let rejected = call.is_err();
    if let Ok(mut call) = call {
        call.cancel().await?;
    } // No dangling task even if the assertion regresses.
    assert!(
        rejected,
        "a call cannot hand off restoration behind its startup waiter"
    );
    assert!(Arc::ptr_eq(
        service.0.lock().unwrap().restoration.as_ref().unwrap(),
        &original
    ));
    service.finish_restoration().await?;
    service
        .0
        .lock()
        .unwrap()
        .controller
        .as_ref()
        .unwrap()
        .require_idle()?;
    service.shutdown().await?;
    Ok(())
}
fn injected(task: JoinHandle<Completion>, stop: watch::Sender<bool>) -> (Service, Arc<Entry>) {
    let service = Service::default();
    let entry = Arc::new(Entry {
        stop,
        task: AsyncMutex::new(Joined::Running(task)),
    });
    service.0.lock().unwrap().restoration = Some(entry.clone());
    (service, entry)
}
#[tokio::test]
async fn cancelled_borrowed_wait_retains_original_open_and_shutdown_signals_before_join()
-> Result<()> {
    let (release, wait) = tokio::sync::oneshot::channel();
    let (stop, mut stopped) = watch::channel(false);
    let (service, entry) = injected(
        tokio::spawn(async move {
            let _ = wait.await;
            Completion {
                controller: None,
                result: Err(anyhow::anyhow!("synthetic failed open")),
            }
        }),
        stop,
    );
    assert!(
        timeout(Duration::from_millis(10), service.finish_restoration())
            .await
            .is_err()
    );
    assert!(matches!(&*entry.task.lock().await,Joined::Running(task) if !task.is_finished()));
    service.request_stop();
    stopped.changed().await?;
    assert!(*stopped.borrow());
    release.send(()).unwrap();
    assert!(service.shutdown().await.is_err());
    assert!(Arc::ptr_eq(
        service.0.lock().unwrap().restoration.as_ref().unwrap(),
        &entry
    ));
    assert!(service.0.lock().unwrap().controller.is_none());
    Ok(())
}
#[tokio::test]
async fn status_like_success_without_original_controller_and_panicked_open_never_release_slot()
-> Result<()> {
    for panic in [false, true] {
        let (stop, _) = watch::channel(false);
        let (service, entry) = injected(
            tokio::spawn(async move {
                assert!(!panic, "synthetic unknown original open");
                Completion {
                    controller: None,
                    result: Ok(()),
                }
            }),
            stop,
        );
        assert!(service.finish_restoration().await.is_err());
        assert!(service.reap_restoration().is_err());
        assert!(service.shutdown().await.is_err());
        assert!(service.0.lock().unwrap().controller.is_none());
        assert!(Arc::ptr_eq(
            service.0.lock().unwrap().restoration.as_ref().unwrap(),
            &entry
        ));
    }
    Ok(())
}
fn future_size<F: Future>(_: fn(Host, watch::Receiver<bool>) -> F) -> usize {
    size_of::<F>()
}
#[test]
fn restoration_future_keeps_existing_frame_bound() {
    assert!(future_size(run) <= 128 * 1024);
    assert!(size_of::<Completion>() <= 16 * 1024);
}
