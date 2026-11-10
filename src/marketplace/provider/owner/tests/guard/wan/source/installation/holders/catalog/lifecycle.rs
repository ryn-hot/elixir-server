//! Additional effects in ONE existing cohort. No shortened update cadence,
//! manufactured readiness, replacement provider identity or production source.
use super::*;

async fn grant(actor: &Participant, source: &SourceFixture) -> Result<(i64, String, i64)> {
    sqlx::query_as("SELECT revision,review_json,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants WHERE extension_id=$1")
        .bind(source.publication.release.payload.extension_id.as_str())
        .fetch_one(&actor.rig.database.pool).await.map_err(Into::into)
}

pub(super) async fn unattended(
    actor: &Participant,
    source: &SourceFixture,
    instance: Uuid,
) -> Result<()> {
    let before = grant(actor, source).await?;
    ensure!(before.2 == 1, "normal saved owner opt-in missing");
    http(
        actor.app()?,
        actor.token()?,
        "POST",
        "/api/v1/auth/logout",
        Some(json!({})),
    )
    .await?;
    let denied = actor
        .app()?
        .clone()
        .oneshot(
            Request::get("/api/v1/marketplace/installations/current")
                .header("authorization", format!("Bearer {}", actor.token()?))
                .body(Body::empty())?,
        )
        .await?;
    ensure!(
        denied.status() == axum::http::StatusCode::UNAUTHORIZED,
        "logged-out bearer retained management access"
    );
    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_sessions")
        .fetch_one(&actor.rig.database.pool)
        .await?;
    let principal = actor
        .principal
        .as_ref()
        .context("original observation principal missing")?;
    let service = actor.state()?.marketplace.fixture_original_installation();
    let began = tokio::time::Instant::now();
    let mut report = Duration::ZERO;
    loop {
        // Observation only: no action submission, scheduler restart, search,
        // trusted head import, consent rewrite or clock manipulation.
        let view = service
            .current_for(principal)?
            .map(|id| service.view_for(principal, id))
            .transpose()?;
        if let Some(view) = &view
            && view.release_id == source.matched.release_id
        {
            ensure!(
                !matches!(
                    view.phase,
                    crate::marketplace::application::Phase::Stopped
                        | crate::marketplace::application::Phase::Cancelled
                        | crate::marketplace::application::Phase::RecoveryRequired
                ),
                "original automatic update reached {:?}",
                view.phase
            );
            if view.phase == crate::marketplace::application::Phase::Activated {
                ensure!(
                    view.configured_instance == Some(instance),
                    "automatic update replaced instance"
                );
                service.fixture_join_original(view.operation_id).await?;
                break;
            }
        }
        if began.elapsed() >= report {
            eprintln!(
                "{}",
                json!({"stage":"cross_peer_automatic_update_wait",
                "elapsed_ms":began.elapsed().as_millis(),
                "scheduler":actor.state()?.marketplace.fixture_original_update_diagnostics()?,
                "phase":view.as_ref().map(|v|v.phase)})
            );
            report = began.elapsed() + Duration::from_secs(30);
        }
        // The real scheduler may already have scanned an empty/old list and
        // entered its normal 30-minute cadence. This is a wall-clock bound,
        // not an implementation timeout or permission to start replacement work.
        ensure!(
            began.elapsed() < Duration::from_secs(2400),
            "original automatic update deadline"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ensure!(
        grant(actor, source).await? == before,
        "automatic update changed standing grant"
    );
    ensure!(
        sessions
            == sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM account_sessions")
                .fetch_one(&actor.rig.database.pool)
                .await?,
        "automatic update created a login session"
    );
    provider(
        actor.state()?,
        &actor.rig.database,
        principal.user_id,
        instance,
        "1.1.0",
    )
    .await?;
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_automatic_update_verified",
        "elapsed_ms":began.elapsed().as_millis(),"same_instance":true,
        "grant_unchanged":true,"logged_out_bearer_rejected":true,
        "scheduler_restarted":false,"consumer_new_actions":0,"consumer_source_uploads":0})
    );
    Ok(())
}

pub(super) async fn transitions(
    actors: &[Participant],
    source: &SourceFixture,
    guard: &str,
) -> Result<()> {
    ensure!(actors.len() == 2, "original two participants required");
    let holder = &actors[0];
    let consumer = &actors[1];
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_lifecycle_observation_begin"})
    );
    // Only instrumentation settling time. It grants no health, role, credit,
    // readiness or consent; all subsequent effects use actual product APIs.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let saved = http(holder.app()?, holder.token()?, "PATCH", "/api/v1/marketplace/settings",
        Some(json!({"expected_revision":2,"mutation_id":Uuid::new_v4(),"preferences":Preferences {
            contribution:ContributionMode::Off,metering:ConnectionMetering::Unmetered,..Default::default()
        }}))).await?;
    ensure!(
        saved["settings"]["revision"] == 3,
        "owner opt-out was not saved"
    );
    holder.roles(false, 3).await?;
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    while holder.client()?.snapshot().package_sharing_active {
        ensure!(
            tokio::time::Instant::now() < until,
            "opt-out left sharing active"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Contribution Off must preserve a working protected consumer, not merely
    // a saved flag. Fetch the exact already-approved public source afresh.
    let client = holder.client()?;
    source_ready(&client).await?;
    let metadata = client.verify_publication(&source.publication).await?;
    let checked = client.preflight_fresh_publication(&metadata, 0).await?;
    ensure!(
        checked.release_id() == source.matched.release_id,
        "opt-out changed source identity"
    );
    ensure!(
        !client.snapshot().package_sharing_active,
        "fresh consumer work re-enabled sharing"
    );
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_owner_opt_out_verified",
        "sharing_stopped":true,"protected_consumer_source_verified":true})
    );

    // Kill only the current cohort's exact managed guard. The kernel firewall
    // is not removed or relaxed; the application must notice the ORIGINAL
    // guard failure and revoke its old participation capability.
    let deployment = format!("label=elixir.deployment_id={}", consumer.rig.deployment);
    let ids = docker(&[
        "ps",
        "--no-trunc",
        "--quiet",
        "--filter",
        &deployment,
        "--filter",
        "label=elixir.marketplace.isolation=packet-v1",
    ])
    .await?;
    let ids: Vec<_> = ids.lines().filter(|line| !line.is_empty()).collect();
    ensure!(
        ids.len() == 1 && crate::runtime::packet_network::immutable_id(ids[0]),
        "exact original consumer guard missing or ambiguous"
    );
    let inspected = docker(&[
        "inspect",
        "--format",
        "{{.Image}} {{.State.Running}}",
        ids[0],
    ])
    .await?;
    ensure!(
        inspected.trim() == format!("{guard} true"),
        "guard identity/image changed"
    );
    let stale_client = consumer.client()?;
    source_ready(&stale_client).await?;
    let consumer_metadata = stale_client.verify_publication(&source.publication).await?;
    ensure!(
        stale_client.snapshot().phase == ParticipationPhase::Running,
        "protection-loss test did not start healthy"
    );
    eprintln!("{}", json!({"stage":"cross_peer_guard_failure_begin"}));
    docker(&["kill", "--signal=KILL", ids[0]]).await?;
    let until = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let state = stale_client.snapshot();
        if matches!(
            state.phase,
            ParticipationPhase::Failed | ParticipationPhase::Stopped
        ) {
            ensure!(
                !state.package_sharing_active
                    && state
                        .applied_roles
                        .is_none_or(|roles| !roles.consumer && !roles.catalog && !roles.routing),
                "old capability retained active roles after guard failure"
            );
            break;
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "old participation capability survived guard failure"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ensure!(
        stale_client.network_status().await.is_err(),
        "failed guard left old network RPC usable"
    );
    ensure!(
        stale_client
            .preflight_fresh_publication(&consumer_metadata, 0)
            .await
            .is_err(),
        "failed consumer retained fresh source admission"
    );
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_lifecycle_capture_complete",
        "actual_guard_killed":true,"old_capability_closed":true,"owner_opt_out":true})
    );
    // The normal startup retry owner alone must reconcile child-first and
    // reconnect using the SAME durable identity. Do not call recover/start or
    // enroll here. New transport tuples are outside the preceding capture.
    let until = tokio::time::Instant::now() + Duration::from_secs(600);
    let recovered = loop {
        if let Ok(current) = consumer.client()
            && !current.same_owner(&stale_client)
            && current.snapshot().phase == ParticipationPhase::Running
        {
            break current;
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "normal startup did not recover original identity"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    source_ready(&recovered).await?;
    let fresh = recovered.verify_publication(&source.publication).await?;
    ensure!(
        recovered
            .preflight_fresh_publication(&fresh, 0)
            .await?
            .release_id()
            == source.matched.release_id,
        "normal recovery could not verify exact public source"
    );
    ensure!(
        stale_client.network_status().await.is_err(),
        "recovery revived stale capability"
    );
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_lifecycle_transitions_verified",
        "actual_guard_killed":true,"old_capability_closed":true,"owner_opt_out":true,
        "normal_startup_recovered":true,"fresh_source_after_recovery":true,
        "excludes":"physical sleep, outer IPv6, off-site operators and full platform matrix"})
    );
    Ok(())
}
