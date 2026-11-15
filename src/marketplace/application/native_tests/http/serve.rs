//! The SAME installed module through the long-lived executor: one verified
//! start, then calls on the running executor. A publisher error fails only
//! its call. Shutdown removes the executor.
use super::*;
use crate::marketplace::service::ServeReply;

pub(super) async fn qualify(
    state: &crate::state::AppState,
    db: &crate::db::Database,
    configuration: crate::marketplace::grant_store::installation::runtime::InstalledRuntime,
    home: Uuid,
    extension: &elixir_marketplace::identity::ExtensionId,
    instance: Uuid,
) -> Result<()> {
    state
        .marketplace
        .configure_serve(db.clone(), state.secrets.as_ref().clone(), configuration)?;
    let ordinary = || RuntimeJson::from_value(json!({}));
    let expected = json!({"cursor":1,"key":"credential-private"});
    let mut elapsed = Vec::new();
    for _ in 0..10 {
        let started = std::time::Instant::now();
        let ServeReply::Result(value) = state
            .marketplace
            .serve_call(home, extension, instance, ordinary()?)
            .await?
        else {
            bail!("the long-lived call failed");
        };
        elapsed.push(started.elapsed().as_millis());
        assert_eq!(serde_json::from_str::<Value>(value.as_str())?, expected);
    }
    // The fixture throws for an unknown contract version: only that call fails.
    let failed = state
        .marketplace
        .serve_call(
            home,
            extension,
            instance,
            RuntimeJson::from_value(json!({"capability":"live.catalog_provider","schema_version":2}))?,
        )
        .await?;
    assert!(matches!(failed, ServeReply::Failed), "a publisher error fails only its call");
    let started = std::time::Instant::now();
    let ServeReply::Result(value) = state
        .marketplace
        .serve_call(home, extension, instance, ordinary()?)
        .await?
    else {
        bail!("the executor stopped serving after a publisher error");
    };
    elapsed.push(started.elapsed().as_millis());
    assert_eq!(serde_json::from_str::<Value>(value.as_str())?, expected);
    eprintln!("native long-lived call latency: elapsed_ms={elapsed:?} (first includes start)");
    let running = tokio::process::Command::new("docker")
        .args(["ps", "-q", "--filter", "label=elixir.marketplace.isolation=serve-v1"])
        .output()
        .await?;
    assert_eq!(
        String::from_utf8_lossy(&running.stdout).lines().count(),
        1,
        "one long-lived executor serves the instance"
    );
    Ok(())
}

/// After marketplace shutdown no long-lived executor remains.
pub(super) async fn require_stopped() -> Result<()> {
    let left = tokio::process::Command::new("docker")
        .args(["ps", "-aq", "--filter", "label=elixir.marketplace.isolation=serve-v1"])
        .output()
        .await?;
    assert!(
        String::from_utf8_lossy(&left.stdout).trim().is_empty(),
        "shutdown removes every long-lived executor"
    );
    Ok(())
}
