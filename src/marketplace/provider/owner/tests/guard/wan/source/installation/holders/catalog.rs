//! One consented two-identity cohort. No known-peer sender, host-created
//! admission receipt, production credential, source publication or billing.
use super::*;
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    canonical,
    evidence::IssuerPolicy,
    records::{SignatureCache, SignedRecord},
};
mod freshness;
mod lifecycle;
mod sdk;

fn descriptor(source: &SourceFixture, weight: u8) -> Result<CandidateDescriptor> {
    // Reuse only the five public TEST credential shapes. Author signatures and
    // source bytes remain those of the already approved public snapshots.
    let fixture: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))?;
    let publication = &source.publication;
    let mut value = CandidateDescriptor::parse(&serde_json::to_vec(&json!({
        "schema_version":1,"publisher":publication.publisher,
        "release":publication.release,"head":publication.head,"revocations":[],
        "gate_evidence":fixture["evidence"]["pairs"]
    }))?)?;
    ensure!(
        value.gate_evidence.len() == 5,
        "five TEST supporters required"
    );
    let issued = now()?;
    let issuer = SigningKey::from_bytes(&[3; 32]);
    for (index, pair) in value.gate_evidence.iter_mut().enumerate() {
        let mut credential = pair.credential.payload.clone();
        credential.extension_id = publication.release.payload.extension_id.clone();
        credential.tenure_weight = weight;
        credential.issued_at = issued;
        credential.expires_at = issued + 7200;
        pair.credential = SignedRecord::sign(credential, None, &issuer)?;
        let mut endorsement = pair.endorsement.payload.clone();
        endorsement.extension_id = publication.release.payload.extension_id.clone();
        endorsement.release_id = source.matched.release_id;
        endorsement.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(
            endorsement,
            None,
            &SigningKey::from_bytes(&[10 + index as u8; 32]),
        )?;
    }
    value.verify_metadata(
        issued,
        &IssuerPolicy::new([PublicKey::from_bytes(issuer.verifying_key().to_bytes())?])?,
        &mut SignatureCache::new(1024)?,
    )?;
    Ok(value)
}

async fn offer(actor: &Participant, value: &CandidateDescriptor) -> Result<()> {
    let reply = actor
        .client()?
        .request(PeerOperation::Offer {
            descriptor: Box::new(value.clone()),
        })
        .await?;
    ensure!(
        matches!(reply.response, PeerOutcome::Offered { release_id }
        if release_id == Digest::of(canonical::to_vec(&value.release.payload)?)),
        "original TEST catalog offer rejected"
    );
    Ok(())
}

async fn discover_pair(actors: &[Participant]) -> Result<()> {
    ensure!(actors.len() == 2, "one real holder and consumer required");
    let until = tokio::time::Instant::now() + Duration::from_secs(600);
    let mut refresh = tokio::time::Instant::now();
    loop {
        let a = actors[0]
            .client()
            .context("publisher client unavailable during peer discovery")?
            .network_status()
            .await
            .context("publisher network status failed during peer discovery")?;
        let b = actors[1]
            .client()
            .context("consumer client unavailable during peer discovery")?
            .network_status()
            .await
            .context("consumer network status failed during peer discovery")?;
        a.within_limits(&NetworkLimits::default())?;
        b.within_limits(&NetworkLimits::default())?;
        if a.peers.len() == 1 && b.peers.len() == 1 && a.peers[0] != b.peers[0] {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < until,
            "original two-peer discovery deadline"
        );
        if tokio::time::Instant::now() >= refresh {
            for actor in actors {
                let _ = actor.client()?.refresh_network().await;
            }
            refresh = tokio::time::Instant::now() + Duration::from_secs(10);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn card(
    actor: &Participant,
    source: &SourceFixture,
    weight: u64,
    previous: Option<(Digest, u64)>,
) -> Result<Uuid> {
    let began = tokio::time::Instant::now();
    let started = http(
        actor.app()?,
        actor.token()?,
        "POST",
        "/api/v1/marketplace/search",
        Some(first_source_search_request(
            &source.publication.release.payload.extension_id,
        )?),
    )
    .await?;
    let mut id: Uuid = serde_json::from_value(started["query_id"].clone())?;
    let mut query_started = tokio::time::Instant::now();
    loop {
        let view = http(
            actor.app()?,
            actor.token()?,
            "GET",
            &format!("/api/v1/marketplace/search/{id}"),
            None,
        )
        .await?;
        if view["phase"] == "ready" {
            let rows = view["results"]
                .as_array()
                .context("verified card missing")?;
            ensure!(
                rows.len() == 1
                    && rows[0]["extension_id"]
                        == json!(source.publication.release.payload.extension_id)
                    && rows[0]["support"]["discovery_qualified"] == true
                    && rows[0]["support"]["verified_supporters"] == 5
                    && rows[0]["support"]["is_global_total"] == false
                    && rows[0]["metadata_only"] == true
                    && view["network_complete"] == false,
                "HTTP card lost exact release, public source or TEST rank binding: {view}"
            );
            if let Some((old_release, old_weight)) = previous
                && rows[0]["release_id"] == json!(old_release)
            {
                ensure!(
                    rows[0]["support"]["weighted_score"] == old_weight
                        && began.elapsed() < Duration::from_secs(900),
                    "known old card rank changed or bounded propagation deadline expired"
                );
                // Browsing is local-first. A completed peer delta pass has an
                // existing ten-minute cooldown. Wait for ordinary propagation,
                // never inject a descriptor/head or restart the consumer. Each
                // replacement uses the real browser endpoint and normal owner.
                tokio::time::sleep(Duration::from_secs(5)).await;
                let started = http(
                    actor.app()?,
                    actor.token()?,
                    "POST",
                    "/api/v1/marketplace/search",
                    Some(first_source_search_request(
                        &source.publication.release.payload.extension_id,
                    )?),
                )
                .await?;
                id = serde_json::from_value(started["query_id"].clone())?;
                query_started = tokio::time::Instant::now();
                continue;
            }
            ensure!(
                rows[0]["release_id"] == json!(source.matched.release_id)
                    && rows[0]["support"]["weighted_score"] == weight,
                "HTTP card returned an unknown release or incorrect TEST rank"
            );
            eprintln!(
                "{}",
                json!({"stage":"cross_peer_verified_http_card",
                "version":source.publication.release.payload.version,
                "elapsed_ms":began.elapsed().as_millis(),"simulated_support_only":true})
            );
            return Ok(id);
        }
        ensure!(
            view["phase"] == "checking"
                && query_started.elapsed() < Duration::from_secs(180)
                && began.elapsed() < Duration::from_secs(900),
            "original cross-peer HTTP search unavailable: {view}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn install_card(
    actor: &Participant,
    source: &SourceFixture,
    query: Uuid,
    existing: Option<Uuid>,
) -> Result<Uuid> {
    install_card_with_policy(actor, source, query, existing, false).await
}

async fn install_card_with_policy(
    actor: &Participant,
    source: &SourceFixture,
    query: Uuid,
    existing: Option<Uuid>,
    automatic_updates: bool,
) -> Result<Uuid> {
    let extension = &source.publication.release.payload.extension_id;
    let targets = target_choices(
        actor.app()?,
        actor.token()?,
        extension.as_str(),
        Duration::from_secs(30),
    )
    .await?;
    let (target, choice) = if let Some(instance) = existing {
        let row = targets["instances"]
            .as_array()
            .context("instance choices missing")?
            .iter()
            .find(|row| row["instance_id"] == json!(instance))
            .context("original configured target missing")?;
        (json!({"kind":"existing","instance_id":instance}), row)
    } else {
        (
            json!({"kind":"new","name":"Cross-peer qualification"}),
            &targets["new_instance"],
        )
    };
    let request = json!({"schema_version":1,"query_id":query,"extension_id":extension,
        "release_id":source.matched.release_id,"target":target,
        "expected_revision":choice["expected_revision"],"mutation_id":Uuid::new_v4(),
        "controls":choice["controls"]});
    // Normal selected-card endpoint, NOT direct-link/unqualified acceptance.
    let started = http(
        actor.app()?,
        actor.token()?,
        "POST",
        "/api/v1/marketplace/installations",
        Some(request.clone()),
    )
    .await?;
    let id: Uuid = serde_json::from_value(started["operation_id"].clone())?;
    ensure!(
        http(
            actor.app()?,
            actor.token()?,
            "POST",
            "/api/v1/marketplace/installations",
            Some(request)
        )
        .await?["operation_id"]
            == json!(id),
        "original install was replaced"
    );
    let review = phase(actor.app()?, actor.token()?, id, "awaiting_approval", None).await?;
    action(
        actor.app()?,
        actor.token()?,
        id,
        json!({"action":"approve",
        "review_id":review["review_id"],"expected_revision":review["approval_revision"],
        "mutation_id":Uuid::new_v4(),"automatic_updates":automatic_updates}),
    )
    .await?;
    phase(actor.app()?, actor.token()?, id, "awaiting_setup", None).await?;
    action(
        actor.app()?,
        actor.token()?,
        id,
        json!({"action":"setup",
        "mutation_id":Uuid::new_v4(),"credentials":[]}),
    )
    .await?;
    phase(actor.app()?, actor.token()?, id, "awaiting_commit", None).await?;
    action(
        actor.app()?,
        actor.token()?,
        id,
        json!({"action":"activate",
        "mutation_id":Uuid::new_v4()}),
    )
    .await?;
    let active = phase(actor.app()?, actor.token()?, id, "activated", None).await?;
    actor
        .state()?
        .marketplace
        .fixture_original_installation()
        .fixture_join_original(id)
        .await?;
    let instance: Uuid = serde_json::from_value(active["configured_instance"].clone())?;
    ensure!(
        existing.is_none_or(|original| instance == original),
        "update replaced configured target"
    );
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_selected_card_activated",
        "version":source.publication.release.payload.version,"source_uploads":0,
        "manual_acceptance":true,"automatic_updates_opted_in":automatic_updates,
        "simulated_support_only":true})
    );
    Ok(instance)
}

fn private_parent_diagnostics() -> Result<()> {
    // This ignored test alone owns the executable. Enable only the existing
    // fixed sharing-stage messages, after cooldown, never arbitrary errors.
    tracing_subscriber::fmt()
        .with_env_filter("off,elixir_marketplace::sharing_diagnostics=debug")
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .without_time()
        .try_init()
        .map_err(|_| anyhow::anyhow!("private sharing observer unavailable"))
}

fn failure_snapshot(
    participant: usize,
    state: &crate::marketplace::worker::ParticipationSnapshot,
) -> Value {
    // A near-failure diagnostic, NOT a liveness/cleanup/serving receipt. Avoid
    // the full snapshot: catalog fields may grow independent diagnostic data.
    json!({"stage":"cross_peer_failure_state","participant":participant,
        "phase":state.phase,"blockers":state.blockers,"applied_roles":state.applied_roles,
        "host_sample_current":state.host_sample_current,"preferences_revision":state.preferences_revision,
        "package_sharing_active":state.package_sharing_active,"recovery_seconds":state.recovery_seconds})
}

#[test]
fn private_failure_snapshot_preserves_unknown_roles_and_uses_only_closed_fields() {
    let state = crate::marketplace::worker::ParticipationSnapshot::default();
    let value = failure_snapshot(1, &state);
    let keys = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        std::collections::BTreeSet::from([
            "stage",
            "participant",
            "phase",
            "blockers",
            "applied_roles",
            "host_sample_current",
            "preferences_revision",
            "package_sharing_active",
            "recovery_seconds"
        ])
    );
    assert!(value["applied_roles"].is_null());
    assert!(value["preferences_revision"].is_null());
    assert_eq!(value["host_sample_current"], false);
}

#[test]
#[ignore = "approved local signed source fixtures only; no provider or network"]
fn approved_versioned_sources_and_test_support_match_before_enrollment() -> Result<()> {
    private_parent_diagnostics()?;
    let sources = configured_fixtures()?;
    ensure!(sources.len() == 2, "two approved snapshots required");
    descriptor(&sources[0], 2)?;
    descriptor(&sources[1], 4)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "two explicitly approved remaining identities; real protected peer catalog/source/install/update; TEST support only"]
async fn independent_catalog_selected_card_install_and_manual_update() -> Result<()> {
    catalog_workflow(false, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "standing-approved two free identities, synthetic sources; normal automatic scheduler/logout, owner opt-out and actual guard failure"]
async fn independent_catalog_automatic_update_opt_out_and_protection_loss() -> Result<()> {
    catalog_workflow(true, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "two standing-approved free identities; real publisher SDK and independently protected consumer; already-approved synthetic sources only"]
async fn independent_catalog_sdk_publish_install_and_update() -> Result<()> {
    catalog_workflow(false, true).await
}

async fn publish_and_seed(
    actor: &Participant,
    source: &SourceFixture,
    existing: Option<Uuid>,
    sdk: Option<&sdk::PublisherSdk>,
) -> Result<Uuid> {
    if let Some(sdk) = sdk {
        return sdk.install_and_seed(actor, source, existing).await;
    }
    let instance = install_with_updates(
        actor.app()?, actor.state()?, actor.token()?, source, existing, true, false,
    ).await?;
    actor.seeding(instance, source, true).await?;
    Ok(instance)
}

async fn catalog_workflow(lifecycle: bool, use_sdk: bool) -> Result<()> {
    consent()?; // No input access or enrollment before explicit consent.
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_REMAINING_TWO_IDENTITY_LIVE_TEST").as_deref() == Ok("1"),
        "explicit remaining two-identity allowance required before enrollment"
    );
    const CONSUMER: usize = 1;
    let sources = configured_fixtures()?;
    let descriptors = [descriptor(&sources[0], 2)?, descriptor(&sources[1], 4)?];
    let sdk = use_sdk.then(sdk::PublisherSdk::configured).transpose()?;
    if let Some(sdk) = &sdk {
        sdk.preflight().await?;
    }
    let provider = image()?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&worker).await?;
    ensure!(
        [
            ("elixir.marketplace.worker.blob_sharing", "2"),
            ("elixir.marketplace.worker.onion_transport", "2"),
            ("elixir.marketplace.worker.wan_query_budget", "1")
        ]
        .iter()
        .all(|(k, v)| labels.get(*k).map(String::as_str) == Some(*v))
            && !labels
                .keys()
                .any(|key| key.contains("fixture")
                    && key != "elixir.marketplace.worker.onion_fixture")
            && labels
                .get("elixir.marketplace.worker.onion_fixture")
                .map(String::as_str)
                == Some("0"),
        "ordinary pinned worker required before enrollment"
    );
    let topic = Digest::of(Uuid::new_v4().as_bytes());
    let mut allocation = crate::marketplace::application::fixture_allocation();
    allocation.source.cpu_millicores = 200;
    allocation.source.wall_seconds = 600;
    allocation.validate()?;
    let mut actors = Vec::new();
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        let mut holders = Vec::new();
        for n in 0..2 {
            actors.push(Participant::new(runtime.clone(), &provider).await?);
            eprintln!(
                "{}",
                json!({"stage":"cross_peer_participant_layout","participant":n + 1,
                "deployment":actors[n].rig.deployment,"recovery_root":actors[n].rig.root.path()})
            );
            if n == 0 && sdk.is_some() {
                // Upload/review does not need an active serving role. The
                // original SDK seeding step below still requires that role.
                actors[n].start_application(&guard, &worker, topic, true, &allocation).await?;
            } else {
                actors[n].start(&guard, &worker, topic, n == 0, &allocation).await?;
            }
            if n == 0 {
                let instance = publish_and_seed(&actors[n], &sources[0], None, sdk.as_ref()).await?;
                holders.push(instance);
                offer(&actors[n], &descriptors[0]).await?;
            }
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_grants")
            .fetch_one(&actors[CONSUMER].rig.database.pool)
            .await?;
        ensure!(count == 0, "consumer did not start empty");
        // Allow ordinary catalog refresh without permitting serving. Metered
        // mode intentionally disables this background work; foreground-only
        // metered freshness is a separate qualification case.
        let saved = http(actors[CONSUMER].app()?, actors[CONSUMER].token()?, "PATCH", "/api/v1/marketplace/settings",
            Some(json!({"expected_revision":2,"mutation_id":Uuid::new_v4(),"preferences":Preferences {
                contribution:ContributionMode::Off,metering:ConnectionMetering::Unmetered,..Default::default()
            }}))).await?;
        ensure!(saved["settings"]["revision"] == 3, "consumer refresh choice was not saved");
        actors[CONSUMER].roles(false, 3).await?;
        discover_pair(&actors).await?;
        private_parent_diagnostics()?;
        let query = card(&actors[CONSUMER], &sources[0], 10, None).await?;
        let installed = install_card_with_policy(&actors[CONSUMER], &sources[0], query, None, lifecycle).await?;
        if lifecycle && std::env::var("ELIXIR_MARKETPLACE_FULL_LIFECYCLE_CAPTURE").as_deref() == Ok("1") {
            // Observation window BEFORE publishing the already-approved next
            // version to this private cohort. No scheduler/health/role override.
            eprintln!("{}", json!({"stage":"cross_peer_automatic_capture_begin"}));
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        // Publisher updates are the exact SECOND already-public approved source.
        // No new repository content or production extension is published.
        for n in 0..1 {
            publish_and_seed(&actors[n], &sources[1], Some(holders[n]), sdk.as_ref()).await?;
            offer(&actors[n], &descriptors[1]).await?;
        }
        if lifecycle {
            // No consumer search, install request, selection, new login or timer
            // override after offering the new head: the real startup scheduler
            // must discover and authorize the update using its standing grant.
            lifecycle::unattended(&actors[CONSUMER], &sources[1], installed).await?;
            lifecycle::transitions(&actors, &sources[1], &guard).await?;
        } else {
            let query = card(
                &actors[CONSUMER],
                &sources[1],
                20,
                Some((sources[0].matched.release_id, 10)),
            )
            .await?;
            install_card(&actors[CONSUMER], &sources[1], query, Some(installed)).await?;
            actors[CONSUMER].roles(false, 3).await?;
        }
        eprintln!(
            "{}",
            json!({"stage":"cross_peer_catalog_workflow_verified",
            "independent_vpn_namespaces":2,"approved_source_versions":2,"holders":1,
            "selected_card_install_and_update":true,"automatic_update":lifecycle,
            "owner_opt_out_and_protection_loss":lifecycle,"consumer_source_uploads":0,
            "test_supporters_per_release":5,"real_paid_credentials":0,
            "consumer_only":true,"unmetered_refresh":true,"production_enabled":false})
        );
        Ok::<(), anyhow::Error>(())
    }))
    .catch_unwind()
    .await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("Original cross-peer catalog operation failed: {error:#}");
    }
    if !outcome.as_ref().is_ok_and(|value| value.is_ok()) {
        // Read the SAME retained command before shutdown drops its private
        // diagnostics. Generic HTTP recovery state is intentionally not an
        // error transcript; never require a second live run just to recover
        // the cause. These approved synthetic-only test logs remain private.
        for (index, actor) in actors.iter().enumerate() {
            if let Some(state) = &actor.state
                && let Ok((original, cleanup)) =
                    state.marketplace.fixture_original_installation_failures()
            {
                for (kind, failure) in [("original", original), ("cleanup", cleanup)] {
                    if let Some(failure) = failure {
                        let report = format!("{failure:#}")
                            .chars()
                            .take(4096)
                            .collect::<String>();
                        eprintln!(
                            "Private cross-peer participant {} installation {kind}: {report}",
                            index + 1
                        );
                    }
                }
            }
            if let Some(state) = &actor.state {
                // Read-only diagnostic even when a failed owner cannot vend a
                // live client. A missing participation snapshot is not healthy.
                let runtime = state.marketplace.snapshot();
                if let Some(participation) = runtime.participation {
                    eprintln!("{}", failure_snapshot(index + 1, &participation));
                } else {
                    eprintln!(
                        "{}",
                        json!({"stage":"cross_peer_failure_runtime","participant":index + 1,
                            "phase":runtime.phase,"participation_available":false})
                    );
                }
            }
        }
    }
    let mut cleanup = Vec::new();
    for (index, actor) in actors.iter_mut().enumerate() {
        match actor.close().await {
            Err(error) => cleanup.push(format!("participant {}: {error:#}", index + 1)),
            Ok(()) => eprintln!(
                "{}",
                json!({"stage":"cross_peer_participant_revoked",
                "participant":index + 1,"same_original_identity":true})
            ),
        }
    }
    if !cleanup.is_empty() || !outcome.as_ref().is_ok_and(|v| v.is_ok()) {
        for actor in &actors {
            actor.rig.root.preserve();
        }
    }
    match outcome {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => {
            return Err(error.context(format!("same-identity cleanup failures: {cleanup:?}")));
        }
        Ok(Ok(())) => ensure!(
            cleanup.is_empty(),
            "same-identity cleanup failures: {cleanup:?}"
        ),
    }
    // Only after both original identities and their installed application
    // tasks are confirmed retired. TempDir's implicit drop ignores a failure
    // to remove files inside an intentionally read-only source capsule.
    for (index, actor) in actors.iter_mut().enumerate() {
        if let Err(error) = actor.rig.root.close_completed_catalog() {
            cleanup.push(format!("participant {}: {error:#}", index + 1));
        }
    }
    ensure!(cleanup.is_empty(), "local catalog fixture cleanup failures: {cleanup:?}");
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_catalog_test_complete","same_identities_revoked":2,
        "excludes":"holder failover, real billing, metered foreground-only freshness, GitLab/Radicle, complete packet privacy matrix, playback, scale and release approval"})
    );
    Ok(())
}
