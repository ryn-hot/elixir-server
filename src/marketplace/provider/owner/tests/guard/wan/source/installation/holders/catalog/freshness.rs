//! Actual ordinary propagation into durable installed fences, including with
//! auto-updates OFF. Reuse approved source snapshots; no repository publication.
use super::*;
use elixir_marketplace::records::Revocation;

fn notices(sources: &[SourceFixture]) -> Result<[CandidateDescriptor; 2]> {
    let root =
        PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_PUBLIC_SOURCE_FIXTURE")?).canonicalize()?;
    let mut seed =
        crate::marketplace::startup::read_private_profile_bytes(&root.join("author/root.key"))?;
    ensure!(
        seed.len() == 32,
        "exact approved synthetic author key required"
    );
    let key = SigningKey::from_bytes(seed.as_slice().try_into()?);
    seed.fill(0);
    ensure!(
        sources.len() == 2
            && sources.iter().all(|source| source
                .publication
                .publisher
                .payload
                .root_key
                .as_bytes()
                == &key.verifying_key().to_bytes()),
        "synthetic author does not match the approved public source"
    );
    let mut old = descriptor(&sources[0], 2)?;
    let mut latest = descriptor(&sources[1], 4)?;
    let release = SignedRecord::sign(
        Revocation {
            root_key: old.publisher.payload.root_key,
            extension_id: old.release.payload.extension_id.clone(),
            release_ids: vec![sources[0].matched.release_id],
            release_keys: vec![],
            reason: "synthetic release withdrawal qualification".into(),
        },
        None,
        &key,
    )?;
    old.revocations.push(release.clone());
    latest.revocations.push(release);
    latest.revocations.push(SignedRecord::sign(
        Revocation {
            root_key: latest.publisher.payload.root_key,
            extension_id: latest.release.payload.extension_id.clone(),
            release_ids: vec![],
            release_keys: vec![latest.release.signer_key],
            reason: "synthetic signing key withdrawal qualification".into(),
        },
        None,
        &key,
    )?);
    for notice in [&old, &latest] {
        notice.inspect_security(now()?)?;
        ensure!(
            notice.to_bytes()?.len() <= elixir_marketplace::MAX_CONTROL_FRAME_BYTES,
            "bounded synthetic withdrawal required"
        );
    }
    Ok([old, latest])
}

#[test]
#[ignore = "local approved synthetic keys/sources only; no network or provider"]
fn approved_withdrawals_are_root_signed_and_do_not_change_public_source() -> Result<()> {
    let sources = configured_fixtures()?;
    let withdrawals = notices(&sources)?;
    for (index, withdrawal) in withdrawals.iter().enumerate() {
        let original = &sources[index].publication;
        ensure!(
            withdrawal.release.to_bytes()? == original.release.to_bytes()?
                && withdrawal.head.to_bytes()? == original.head.to_bytes()?
                && withdrawal.publisher.to_bytes()? == original.publisher.to_bytes()?,
            "withdrawal changed approved source/publication records"
        );
        let records = elixir_marketplace::security_job::SecurityRecords {
            schema_version: 1,
            extension_id: withdrawal.release.payload.extension_id.clone(),
            publisher: withdrawal.publisher.clone(),
            head: None,
            revocations: withdrawal.revocations.clone(),
        };
        ensure!(
            records.verify(now()?)?.revocation_ids.len() == index + 1,
            "signed restriction scope differs"
        );
    }
    Ok(())
}

async fn submit(actor: &Participant, descriptor: &CandidateDescriptor) -> Result<()> {
    let notice = descriptor
        .revocations
        .last()
        .context("synthetic withdrawal missing")?;
    let view = http(
        actor.app()?,
        actor.token()?,
        "POST",
        "/api/v1/marketplace/withdrawals",
        Some(json!({"schema_version":1,"descriptor":descriptor,
            "accepted_descriptor_id":Digest::of(descriptor.to_bytes()?),
            "revocation_id":Digest::of(notice.to_bytes()?),"accept_permanent_withdrawal":true})),
    )
    .await?;
    ensure!(
        view["signatures_verified"] == true
            && view["security_tracked"] == true
            && view["security_applied"] == true
            && view["local_catalog_retained"] == true
            && view["peer_delivery_confirmed"] == false
            && view["metadata_only"] == true,
        "authenticated owner withdrawal did not retain the exact notice"
    );
    Ok(())
}

async fn durable_notice(
    actor: &Participant,
    extension: &elixir_marketplace::identity::ExtensionId,
    field: &str,
    expected: Value,
    kind: &'static str,
) -> Result<()> {
    let began = tokio::time::Instant::now();
    loop {
        let value: Option<String> = sqlx::query_scalar(
            "SELECT SUBSTR(state_json,1,16385) FROM marketplace_extension_security WHERE extension_id=$1")
            .bind(extension.as_str()).fetch_optional(&actor.rig.database.pool).await?;
        if let Some(value) = value {
            let state = canonical::parse(value.as_bytes(), 16 * 1024)?;
            if state[field]
                .as_array()
                .is_some_and(|values| values.contains(&expected))
            {
                eprintln!(
                    "{}",
                    json!({"stage":"cross_peer_durable_restriction_observed",
                    "kind":kind,"elapsed_ms":began.elapsed().as_millis(),
                    "consumer_http_imports":0,"automatic_updates":false})
                );
                return Ok(());
            }
        }
        ensure!(
            began.elapsed() < Duration::from_secs(900),
            "ordinary peer restriction did not reach the durable installed fence"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "standing approved free temporary VPNs, synthetic-only isolated network; no new repository publications"]
async fn root_withdrawals_propagate_and_fence_manual_update_installations() -> Result<()> {
    consent()?;
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_WITHDRAWAL_LIVE_TEST").as_deref() == Ok("1"),
        "explicit synthetic withdrawal qualification required"
    );
    let sources = configured_fixtures()?;
    let withdrawals = notices(&sources)?; // Verify identity before enrollment.
    let current = descriptor(&sources[1], 4)?;
    let provider_image = image()?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&worker).await?;
    ensure!(
        labels
            .get("elixir.marketplace.worker.onion_fixture")
            .map(String::as_str)
            == Some("0")
            && labels
                .get("elixir.marketplace.worker.onion_transport")
                .map(String::as_str)
                == Some("2"),
        "ordinary pinned onion worker required"
    );
    let topic = Digest::of(Uuid::new_v4().as_bytes());
    let mut allocation = crate::marketplace::application::fixture_allocation();
    allocation.source.cpu_millicores = 200;
    allocation.source.wall_seconds = 600;
    allocation.validate()?;
    let mut actors = Vec::new();
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        for n in 0..2 {
            actors.push(Participant::new(runtime.clone(), &provider_image).await?);
            eprintln!("{}", json!({"stage":"cross_peer_participant_layout","participant":n+1,
                "deployment":actors[n].rig.deployment,"recovery_root":actors[n].rig.root.path()}));
            actors[n].start(&guard, &worker, topic, n == 0, &allocation).await?;
            if n == 0 {
                let instance = install_with_updates(actors[n].app()?, actors[n].state()?,
                    actors[n].token()?, &sources[1], None, true, false).await?;
                actors[n].seeding(instance, &sources[1], true).await?;
                offer(&actors[n], &current).await?;
            }
        }
        let consumer = &actors[1];
        let saved = http(consumer.app()?, consumer.token()?, "PATCH", "/api/v1/marketplace/settings",
            Some(json!({"expected_revision":2,"mutation_id":Uuid::new_v4(),"preferences":Preferences {
                contribution:ContributionMode::Off,metering:ConnectionMetering::Unmetered,..Default::default()
            }}))).await?;
        ensure!(saved["settings"]["revision"] == 3, "consumer refresh choice missing");
        consumer.roles(false, 3).await?;
        discover_pair(&actors).await?;
        private_parent_diagnostics()?;
        let query = card(consumer, &sources[1], 20, None).await?;
        let installed = install_card(consumer, &sources[1], query, None).await?;
        let user: String = sqlx::query_scalar("SELECT approved_by_user_id FROM marketplace_extension_grants WHERE extension_id=$1")
            .bind(current.release.payload.extension_id.as_str()).fetch_one(&consumer.rig.database.pool).await?;
        provider(consumer.state()?, &consumer.rig.database, Uuid::parse_str(&user)?, installed, "1.1.0").await?;
        let extension = &current.release.payload.extension_id;
        // The first notice withdraws OLD 1.0.0, not the running 1.1.0. It must
        // arrive without browsing/importing on the consumer or enabling updates.
        submit(&actors[0], &withdrawals[0]).await?;
        durable_notice(consumer, extension, "revoked_releases", json!(sources[0].matched.release_id), "release").await?;
        provider(consumer.state()?, &consumer.rig.database, Uuid::parse_str(&user)?, installed, "1.1.0").await?;
        eprintln!("{}", json!({"stage":"cross_peer_unrelated_release_remains_callable"}));
        // A later key withdrawal must reach durable execution fences even when
        // the catalog can no longer list the affected release.
        submit(&actors[0], &withdrawals[1]).await?;
        durable_notice(consumer, extension, "revoked_keys", json!(current.release.signer_key), "key").await?;
        ensure!(provider(consumer.state()?, &consumer.rig.database, Uuid::parse_str(&user)?, installed, "1.1.0").await.is_err(),
            "withdrawn installed source remained callable");
        let home: String = sqlx::query_scalar("SELECT home_id FROM marketplace_extension_grants WHERE extension_id=$1")
            .bind(extension.as_str()).fetch_one(&consumer.rig.database.pool).await?;
        let binding_error = crate::marketplace::grant_store::installation::standing::InstalledBinding::load_configured(
            &consumer.rig.database, Uuid::parse_str(&home)?, extension, installed, allocation.installations)
            .await.err().context("withdrawn retained binding was still admitted")?;
        ensure!(matches!(binding_error.downcast_ref::<crate::marketplace::security_state::Rejection>(),
            Some(crate::marketplace::security_state::Rejection::Withdrawn)),
            "retained source failed for an unrelated reason: {binding_error:#}");
        let omission_error = consumer.client()?.verify_publication(&sources[1].publication)
            .await.err().context("fresh verification omitting the withdrawal restored code")?;
        ensure!(matches!(omission_error.downcast_ref::<crate::marketplace::security_state::Rejection>(),
            Some(crate::marketplace::security_state::Rejection::Withdrawn)),
            "omitted notice was rejected for an unrelated reason: {omission_error:#}");
        let enabled: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants WHERE extension_id=$1")
            .bind(extension.as_str()).fetch_one(&consumer.rig.database.pool).await?;
        let automatic: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants WHERE extension_id=$1")
            .bind(extension.as_str()).fetch_one(&consumer.rig.database.pool).await?;
        ensure!(enabled == 1 && automatic == 0, "withdrawal rewrote owner choices");
        consumer.roles(false, 3).await?;
        eprintln!("{}", json!({"stage":"cross_peer_restriction_workflow_verified",
            "independent_vpn_namespaces":2,"root_notices":2,"consumer_http_imports":0,
            "automatic_updates":false,"retained_code_fenced":true,"omitted_notice_rejected":true,
            "owner_choices_preserved":true,"unrelated_release_preserved":true,"production_enabled":false}));
        Ok::<(), anyhow::Error>(())
    })).catch_unwind().await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("Original cross-peer restriction operation failed: {error:#}");
    }
    if !outcome.as_ref().is_ok_and(|value| value.is_ok()) {
        for (n, actor) in actors.iter().enumerate() {
            if let Ok(client) = actor.client() {
                eprintln!("{}", failure_snapshot(n + 1, &client.snapshot()));
            }
            if let Some(state) = &actor.state {
                if let Ok(Some(error)) = state.marketplace.fixture_original_update_failure() {
                    eprintln!(
                        "Private restriction participant {} scheduler cause: {error:#}",
                        n + 1
                    );
                }
                if let Ok((original, cleanup)) =
                    state.marketplace.fixture_original_installation_failures()
                {
                    for (kind, failure) in [("original", original), ("cleanup", cleanup)] {
                        if let Some(error) = failure {
                            eprintln!(
                                "Private restriction participant {} installation {kind}: {error:#}",
                                n + 1
                            );
                        }
                    }
                }
            }
        }
    }
    let mut cleanup = Vec::new();
    for (n, actor) in actors.iter_mut().enumerate() {
        match actor.close().await {
            Ok(()) => eprintln!(
                "{}",
                json!({"stage":"cross_peer_participant_revoked","participant":n+1,"same_original_identity":true})
            ),
            Err(error) => cleanup.push(format!("participant {}: {error:#}", n + 1)),
        }
    }
    if !cleanup.is_empty() || !outcome.as_ref().is_ok_and(|value| value.is_ok()) {
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
    eprintln!(
        "{}",
        json!({"stage":"cross_peer_restriction_test_complete","same_identities_revoked":2,
        "excludes":"off-site operators, real billing, complete packet privacy, playback, scale, platforms and release approval"})
    );
    Ok(())
}
