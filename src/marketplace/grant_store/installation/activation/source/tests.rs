//! Private scheduling/ownership tests, not native hardware or execution proof.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn only_typed_resource_deferral_waits_and_status_retains_one_latest_reason() -> Result<()> {
    let (status, _) = watch::channel(SourceStatus {
        phase: SourcePhase::Restoring,
        last_resource_wait: None,
    });
    let polled = AtomicBool::new(false);
    let result = resource_pause(&status, anyhow::anyhow!("unknown native result"), async {
        polled.store(true, Ordering::SeqCst);
        Ok(())
    })
    .await;
    assert!(result.is_err());
    assert!(
        !polled.load(Ordering::SeqCst),
        "unknown outcomes never enter retry"
    );
    assert!(status.borrow().last_resource_wait.is_none());
    let reason = SourceResourceWait::fixture();
    let mut wait = Box::pin(resource_pause(
        &status,
        anyhow::Error::new(reason).context("bounded local admission context"),
        std::future::pending(),
    ));
    assert!(timeout(Duration::from_millis(10), &mut wait).await.is_err());
    assert_eq!(
        *status.borrow(),
        SourceStatus {
            phase: SourcePhase::Restoring,
            last_resource_wait: Some(reason),
        }
    );
    drop(wait);
    // A lost waiter does not erase the latest diagnostic or manufacture work.
    assert_eq!(status.borrow().last_resource_wait, Some(reason));
    resource_pause(&status, reason.into(), async { Ok(()) }).await?;
    assert_eq!(
        status.borrow().last_resource_wait,
        Some(reason),
        "a retry timer is not successful admission"
    );
    publish(&status, SourcePhase::CheckingSource);
    assert_eq!(
        *status.borrow(),
        SourceStatus {
            phase: SourcePhase::CheckingSource,
            last_resource_wait: None,
        }
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_stop_wait_retains_the_actual_task_until_cleanup_joins() -> Result<()> {
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("source");
    let lease = crate::marketplace::lease::ControllerLease::acquire(&lane)?;
    let (stop, mut stopped_rx) = watch::channel(false);
    let (status, _) = watch::channel(SourceStatus {
        phase: SourcePhase::Restoring,
        last_resource_wait: None,
    });
    let (release, disposed) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _held = lease;
        stopped(&mut stopped_rx).await;
        disposed.await.expect("test disposal release");
        LaunchCompletion {
            result: Err(anyhow::anyhow!("test owner completed its joined disposal")),
            disposed: None,
        }
    });
    let mut launch = SourceLaunch {
        database: Database::connect(&crate::config::DatabaseConfig {
            url: "sqlite::memory:".into(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?,
        home: Uuid::new_v4(),
        configured_instance: Some(Uuid::new_v4()),
        attempt: Uuid::new_v4(),
        stop,
        status,
        task: Some(task),
        result: None,
        disposed: None,
        join_failed: false,
        handed_off: false,
    };
    assert!(
        timeout(Duration::from_millis(10), launch.cancel())
            .await
            .is_err()
    );
    assert!(launch.task.is_some());
    assert_eq!(launch.phase(), SourcePhase::Cancelling);
    assert!(crate::marketplace::lease::ControllerLease::acquire(&lane).is_err());
    release
        .send(())
        .map_err(|_| anyhow::anyhow!("test cleanup owner was lost"))?;
    launch.cancel().await?;
    assert_eq!(launch.phase(), SourcePhase::Cancelled);
    assert!(launch.task.is_none());
    assert!(
        launch.take_disposed().is_none(),
        "a display phase and free lock cannot create a native disposal proof"
    );
    drop(crate::marketplace::lease::ControllerLease::acquire(&lane)?);
    Ok(())
}
