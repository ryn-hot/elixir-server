//! Single consumer, ordinary startup/retry, approved sources and original
//! identity only. The observer must ACK before either guard handoff begins.
use super::*;

#[tokio::test]
#[ignore = "explicit consent, pinned native images, private capture socket and approved public fixtures"]
async fn initial_and_recovered_guard_handoffs_are_packet_observed() -> Result<()> {
    for flag in ["WARP_LIFECYCLE", "WAN", "PUBLIC_SOURCE"] {
        ensure!(
            std::env::var(format!("ELIXIR_MARKETPLACE_{flag}_LIVE_TEST")).as_deref() == Ok("1"),
            "explicit isolated live-test consent required"
        );
    }
    ensure!(
        std::env::var_os("ELIXIR_MARKETPLACE_TEST_HANDOFF_CAPTURE").is_some(),
        "private pre-handoff observer required"
    );
    let sources = configured_fixtures()?;
    let image = immutable_image("ELIXIR_MARKETPLACE_WARP_PROVIDER_IMAGE")?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let mut actor = Participant::new(runtime, &image).await?;
    let operation = std::panic::AssertUnwindSafe(Box::pin(async {
        let mut allocation = crate::marketplace::application::fixture_allocation();
        allocation.source.cpu_millicores = 200;
        allocation.source.wall_seconds = 600;
        allocation.validate()?;
        actor
            .start(
                &guard,
                &worker,
                Digest::of(Uuid::new_v4().as_bytes()),
                false,
                &allocation,
            )
            .await?;
        let stale = actor.client()?;
        source_ready(&stale).await?;
        let metadata = stale.verify_publication(&sources[0].publication).await?;
        ensure!(
            stale
                .preflight_fresh_publication(&metadata, 0)
                .await?
                .release_id()
                == sources[0].matched.release_id,
            "initial handoff did not preserve exact protected public source"
        );
        crate::marketplace::provider::guard::test_capture::finish().await?;
        eprintln!("{}", json!({"stage":"handoff_initial_source_verified"}));
        let label = format!("label=elixir.deployment_id={}", actor.rig.deployment);
        let ids = docker(&[
            "ps",
            "--no-trunc",
            "--quiet",
            "--filter",
            &label,
            "--filter",
            "label=elixir.marketplace.isolation=packet-v1",
        ])
        .await?;
        let ids: Vec<_> = ids.lines().filter(|v| !v.is_empty()).collect();
        ensure!(
            ids.len() == 1 && crate::runtime::packet_network::immutable_id(ids[0]),
            "original guard ambiguous"
        );
        ensure!(
            docker(&[
                "inspect",
                "--format",
                "{{.Image}} {{.State.Running}}",
                ids[0]
            ])
            .await?
            .trim()
                == format!("{guard} true"),
            "original guard image changed"
        );
        docker(&["kill", "--signal=KILL", ids[0]]).await?;
        let until = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            let state = stale.snapshot();
            if matches!(
                state.phase,
                ParticipationPhase::Failed | ParticipationPhase::Stopped
            ) {
                ensure!(
                    !state.package_sharing_active
                        && state
                            .applied_roles
                            .is_none_or(|r| !r.consumer && !r.catalog && !r.routing),
                    "stale capability retained roles"
                );
                break;
            }
            ensure!(
                tokio::time::Instant::now() < until,
                "old guard capability did not close"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(
            stale.network_status().await.is_err()
                && stale
                    .preflight_fresh_publication(&metadata, 0)
                    .await
                    .is_err(),
            "old network/source capability survived guard loss"
        );
        let until = tokio::time::Instant::now() + Duration::from_secs(600);
        let recovered = loop {
            if let Ok(client) = actor.client()
                && !client.same_owner(&stale)
                && client.snapshot().phase == ParticipationPhase::Running
            {
                break client;
            }
            ensure!(
                tokio::time::Instant::now() < until,
                "ordinary startup did not recover original identity"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        source_ready(&recovered).await?;
        let fresh = recovered
            .verify_publication(&sources[1].publication)
            .await?;
        ensure!(
            recovered
                .preflight_fresh_publication(&fresh, 0)
                .await?
                .release_id()
                == sources[1].matched.release_id,
            "recovered handoff did not preserve exact protected public source"
        );
        ensure!(
            stale.network_status().await.is_err(),
            "normal retry revived stale capability"
        );
        crate::marketplace::provider::guard::test_capture::finish().await?;
        eprintln!(
            "{}",
            json!({"stage":"handoff_recovery_verified", "same_original_identity":true,
            "normal_startup_retry":true,"stale_capability_closed":true,"fresh_public_source":true})
        );
        Ok::<_, anyhow::Error>(())
    }))
    .catch_unwind()
    .await;
    let cleanup = actor.close().await;
    match operation {
        Ok(result) => result.and(cleanup)?,
        Err(panic) => {
            cleanup?;
            std::panic::resume_unwind(panic);
        }
    }
    eprintln!("{}", json!({"stage":"handoff_original_identity_retired"}));
    Ok(())
}
