//! Actual source verification/native migration/SQL with synthetic discovery
//! and paid-admission observations. Not a public-network/billing/privacy pilot.
use super::*;
use installation::automatic::Authorization;

async fn update_fixture(fail_ready: bool) -> Result<serde_json::Value> {
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("../extensions/sdk/marketplace");
    let source = SOURCE.replace("===1", if fail_ready { "===99" } else { "===2" });
    let script = "import{pathToFileURL}from'node:url';const base=process.argv[1];const{createFixture}=await import(pathToFileURL(base+'/test/fixture.mjs'));const{privateKeyFromSeed,recordId,signRecord}=await import(pathToFileURL(base+'/src/protocol.mjs'));const f=createFixture({operational:true,sourceBytes:Buffer.from(process.argv[2]),transformManifest(m){m.version='1.1.0';m.runtime.security={sdk_state_limit_bytes:1048576}}});f.head=signRecord('head',{...f.head.payload,sequence:1},privateKeyFromSeed(Buffer.alloc(32,2)),recordId(f.publisher));process.stdout.write(JSON.stringify(f));";
    let output = timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("node")
            .args(["--input-type=module", "-e", script])
            .arg(sdk)
            .arg(source)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() || output.stdout.len() > 8 * MIB as usize {
        bail!("automatic-update SDK fixture failed");
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

async fn selection(
    value: &serde_json::Value,
    database: &Database,
    peer: &PeerPipeFixture,
) -> Result<(Selection, impl Fn(bool) + use<>)> {
    let (descriptor, policy) = descriptor(value)?;
    let release = Digest::of(canonical::to_vec(&descriptor.release.payload)?);
    let (metadata, metadata_owner) =
        MetadataObservation::fixture_for_verified_descriptor(descriptor, &policy)?;
    security_state::admit(database, &metadata).await?;
    let (source, source_owner) = PublicSourceObservation::fixture_for_source(
        release,
        Digest::of(b"SPDX-License-Identifier: MIT\n"),
    );
    let selected = Selection::fixture_for_native_preparation(
        peer.client.clone(),
        metadata,
        source,
        vec![peer.peer.clone()],
    )?;
    Ok((selected, move |active| {
        metadata_owner(active);
        source_owner(active);
    }))
}

async fn manual_install(
    service: &Service,
    database: &Database,
    principal: &CurrentPrincipal,
    peer: &PeerPipeFixture,
    value: &serde_json::Value,
    secrets: &SecretsManager,
    second: bool,
) -> Result<(Uuid, impl Fn(bool) + use<>)> {
    let (selected, owner) = selection(value, database, peer).await?;
    let metadata = selected.discovery_metadata()?;
    let extension = metadata.descriptor().release.payload.extension_id.clone();
    let release = Digest::of(canonical::to_vec(&metadata.descriptor().release.payload)?);
    let controls = if second {
        let extension_revision: i64 = sqlx::query_scalar(
            "SELECT revision FROM marketplace_extension_choices WHERE extension_id=$1",
        )
        .bind(extension.as_str())
        .fetch_one(&database.pool)
        .await?;
        installation::ExpectedControls::NewInstance { extension_revision }
    } else {
        installation::ExpectedControls::Absent {}
    };
    let query = Uuid::new_v4();
    let id = service.start(
        InstallContext::fixture_selected(peer.client.clone(), query, selected, false),
        Host::new(&peer.client, secrets.clone(), allocation())?,
        principal.clone(),
        query,
        InstallAction {
            extension: extension.clone(),
            release,
            target: installation::InstallTarget::New {
                name: if second {
                    "Other instance"
                } else {
                    "Auto-updated instance"
                }
                .into(),
            },
            expected_revision: 0,
            mutation: Uuid::new_v4(),
            controls,
        },
    )?;
    let review = phase(service, id, Phase::AwaitingApproval)
        .await?
        .review
        .context("original review")?;
    let revision = grant_store::load(database, &extension)
        .await?
        .map(|grant| grant.revision())
        .unwrap_or(0);
    let mutation = Uuid::new_v4();
    submit(service, id, || Action::Approve {
        principal: principal.clone(),
        selection: ApprovalSelection {
            review_id: review.review_id().unwrap(),
            expected_revision: revision,
            mutation_id: mutation,
            automatic_updates: true,
        },
    })
    .await?;
    acknowledged(service, id, mutation, ActionStatus::Acknowledged).await?;
    let configured = activate_instance(service, id, principal, database, false).await?;
    service.entry(id)?.task.lock().await.join().await?;
    service.reap_finished()?;
    Ok((configured, owner))
}

#[tokio::test]
#[ignore = "explicit pinned worker; real two-instance install/automatic native migration after logout; discovery/paid admission synthetic"]
async fn automatic_update_after_logout_preserves_consent_credentials_and_the_other_instance()
-> Result<()> {
    automatic_case(false).await
}

#[tokio::test]
#[ignore = "explicit pinned worker; real failed native readiness and mid-cutover optout/disable cleanup after logout; discovery/issuer synthetic"]
async fn automatic_failure_optout_and_disable_retire_only_the_candidate_without_a_session()
-> Result<()> {
    automatic_case(true).await
}

async fn automatic_case(recovery: bool) -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable image required")?;
    let image = docker(&["image", "inspect", "--format", "{{.Id}}", &image]).await?;
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?;
    let lane = directory.join("foreground");
    let files = directory.join("source");
    std::fs::create_dir(&files)?;
    let original = sdk_fixture().await?;
    let updated = update_fixture(recovery).await?;
    for (n, value) in [&original, &updated].into_iter().enumerate() {
        let (_, package, proof) =
            crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
        for (kind, bytes) in [("package", &package), ("proof", &proof)] {
            let file = files.join(format!("{n}-{kind}.blob"));
            std::fs::write(&file, bytes)?;
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444))?;
        }
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let database = database(&directory, None).await?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let service = Service::default();
    let mut peer = None;
    let mut host_worker = None;
    let name = format!("elixir-marketplace-auto-peer-{}", Uuid::new_v4().simple());
    let deployment = format!("auto-host-{}", Uuid::new_v4().simple());
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        let issuer = original["issuer_key"].as_str().context("issuer")?;
        peer = Some(PeerPipeFixture::start(&image, &files, &name, issuer, "providers").await?);
        check_profile(&name).await?;
        host_worker = Some(OfflineWorker::start(
            runtime.as_ref(), &directory.join("admission-host"), &deployment, &image, &[issuer.parse()?],
            crate::marketplace::packet::PacketWorkloadBudget::split(&elixir_marketplace::policy::OrdinaryLimits::default())?.worker_limits(),
        ).await?);
        let actor = grant_store::tests::owner(&database).await?;
        let principal = actor.fixture_principal();
        let preferences = String::from_utf8(canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?;
        sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
            .bind(actor.home.to_string()).bind(preferences).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&database.pool).await?;
        let peer = peer.as_mut().unwrap();
        peer.bind_security_database(database.clone());
        peer.bind_offline_source_owner(host_worker.as_ref().unwrap(), runtime.clone(), &lane)?;
        let secrets = SecretsManager::from_key_bytes([57;32], false);
        let (first, _first_owner) = manual_install(&service, &database, &principal, peer, &original, &secrets, false).await?;
        let (other, _other_owner) = manual_install(&service, &database, &principal, peer, &original, &secrets, true).await?;
        let before: (i64,String,i64) = sqlx::query_as("SELECT revision,review_json,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants")
            .fetch_one(&database.pool).await?;
        sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='automatic fixture logout'").execute(&database.pool).await?;
        let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_sessions").fetch_one(&database.pool).await?;
        if recovery {
            recovery_updates(&service, &database, peer, &secrets, actor.home, first, other, &updated, sessions).await?;
            service.shutdown().await?;
            assert!(ControllerLease::acquire(&lane).is_ok());
            return Ok::<_,anyhow::Error>(());
        }
        let (selected, _update_owner) = selection(&updated, &database, peer).await?;
        let authorization = Authorization::capture(&database, actor.home, first, selected.discovery_metadata()?, allocation().installations).await?;
        let action = authorization.action(&database).await?;
        let query = Uuid::new_v4();
        let context = InstallContext::fixture_empty(peer.client.clone(), query);
        let id = service.start_automatic(context, Host::new(&peer.client, secrets.clone(), allocation())?, authorization, selected, query, action)?;
        let complete = phase(&service, id, Phase::Activated).await?;
        assert_eq!(complete.configured_instance, Some(first));
        service.entry(id)?.task.lock().await.join().await?;
        service.reap_finished()?;
        let after: (i64,String,i64) = sqlx::query_as("SELECT revision,review_json,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants")
            .fetch_one(&database.pool).await?;
        assert_eq!(before, after, "automatic updates must not ratchet or rewrite the approved grant");
        assert_eq!(sessions, sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM account_sessions").fetch_one(&database.pool).await?);
        let mut controller = service.0.lock().unwrap().controller.take().context("original returned controller")?;
        let extension = updated["extension_id"].as_str().context("extension")?.parse()?;
        for (configured, cursor) in [(first, 2), (other, 1)] {
            let binding = installation::standing::InstalledBinding::load_configured(&database, actor.home, &extension, configured, allocation().installations).await?;
            controller.begin(binding, allocation().execution, RuntimeJson::from_value(serde_json::json!({}))?)?;
            let value = controller.finish().await?;
            assert_eq!(serde_json::from_str::<serde_json::Value>(value.as_str())?, serde_json::json!({"cursor":cursor,"key":"credential-private"}));
        }
        controller.cancel().await?;
        service.0.lock().unwrap().controller = Some(controller);
        service.shutdown().await?;
        assert!(ControllerLease::acquire(&lane).is_ok());
        Ok::<_,anyhow::Error>(())
    })).catch_unwind().await;
    let stopped = service.shutdown().await;
    let peer_stopped = match peer.take() {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let host_stopped = match host_worker.take() {
        Some(worker) => match worker.stop(runtime.as_ref()).await {
            Ok(()) => {
                crate::marketplace::preparation::remove_native_activation_host(
                    runtime.as_ref(),
                    &deployment,
                )
                .await
            }
            Err(error) => Err(error),
        },
        None => Ok(()),
    };
    let peer_removed = remove_peer(&name).await;
    database.pool.close().await;
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))?;
    if !matches!(&outcome, Ok(Ok(())))
        || stopped.is_err()
        || peer_stopped.is_err()
        || host_stopped.is_err()
        || peer_removed.is_err()
    {
        eprintln!(
            "failed automatic fixture preserved at {}",
            root.keep().display()
        );
    }
    stopped?;
    peer_stopped?;
    host_stopped?;
    peer_removed?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn candidate_claim(
    service: &Service,
    database: &Database,
    id: Uuid,
    configured: Uuid,
) -> Result<()> {
    timeout(Duration::from_secs(240), async {
        loop {
            let record: String = sqlx::query_scalar(
                "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
            )
            .bind(configured.to_string())
            .fetch_one(&database.pool)
            .await?;
            let record: serde_json::Value = serde_json::from_str(&record)?;
            if record["activation"]["phase"] == "candidate_starting" {
                return Ok::<_, anyhow::Error>(());
            }
            let phase = service.view(id)?.phase;
            if matches!(
                phase,
                Phase::Cancelled | Phase::Activated | Phase::Stopped | Phase::RecoveryRequired
            ) {
                bail!("update reached {phase:?} before its original candidate claim");
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("actual candidate-claim deadline")?
}

async fn recovery_updates(
    service: &Service,
    database: &Database,
    peer: &PeerPipeFixture,
    secrets: &SecretsManager,
    home: Uuid,
    first: Uuid,
    other: Uuid,
    updated: &serde_json::Value,
    sessions: i64,
) -> Result<()> {
    use crate::extensions::store::ExtensionStore;
    let extension = updated["extension_id"]
        .as_str()
        .context("extension")?
        .parse()?;
    let original: String = sqlx::query_scalar(
        "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
    )
    .bind(first.to_string())
    .fetch_one(&database.pool)
    .await?;
    let original: serde_json::Value = serde_json::from_str(&original)?;
    for case in 0..3 {
        if case == 2 {
            // Test datastore consent mutation, not an authenticated UI action.
            // No background task is given a session or reapproval authority.
            sqlx::query("UPDATE marketplace_extension_grants SET automatic_updates=TRUE,revision=revision+1").execute(&database.pool).await?;
        }
        let (selected, _owner) = selection(updated, database, peer).await?;
        let authorization = Authorization::capture(
            database,
            home,
            first,
            selected.discovery_metadata()?,
            allocation().installations,
        )
        .await?;
        let action = authorization.action(database).await?;
        let query = Uuid::new_v4();
        let id = service.start_automatic(
            InstallContext::fixture_empty(peer.client.clone(), query),
            Host::new(&peer.client, secrets.clone(), allocation())?,
            authorization,
            selected,
            query,
            action,
        )?;
        if case != 0 {
            candidate_claim(service, database, id, first).await?;
            if case == 1 {
                sqlx::query("UPDATE marketplace_extension_grants SET automatic_updates=FALSE,revision=revision+1").execute(&database.pool).await?;
            } else {
                ExtensionStore::new(&database.pool)
                    .set_instance_enabled(first, false)
                    .await?;
            }
        }
        phase(service, id, Phase::Cancelled).await?;
        service.entry(id)?.task.lock().await.join().await?;
        service.reap_finished()?;
        let record: String = sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(first.to_string())
        .fetch_one(&database.pool)
        .await?;
        let record: serde_json::Value = serde_json::from_str(&record)?;
        assert!(record["pending"].is_null() && record["activation"].is_null());
        assert_eq!(
            record["active"], original["active"],
            "rollback must retain the exact old capsule/state identity"
        );
        assert!(
            !record["cancelled"].is_null(),
            "actual cancellation acknowledgement required"
        );
        let enabled: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
            .bind(first.to_string()).fetch_one(&database.pool).await?;
        assert_eq!(
            enabled,
            if case == 2 { 0 } else { 1 },
            "rollback cannot enable a disabled instance"
        );
        let opted_in: i64 = sqlx::query_scalar("SELECT CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants").fetch_one(&database.pool).await?;
        assert_eq!(
            opted_in,
            if case == 1 { 0 } else { 1 },
            "rollback cannot rewrite standing consent"
        );
        assert_eq!(
            sessions,
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM account_sessions")
                .fetch_one(&database.pool)
                .await?
        );
        let mut controller = service
            .0
            .lock()
            .unwrap()
            .controller
            .take()
            .context("same returned controller")?;
        for configured in [first, other] {
            let binding = installation::standing::InstalledBinding::load_configured(
                database,
                home,
                &extension,
                configured,
                allocation().installations,
            )
            .await;
            if configured == first && case == 2 {
                assert!(
                    binding.is_err(),
                    "old code must remain unavailable while disabled"
                );
                continue;
            }
            controller.begin(
                binding?,
                allocation().execution,
                RuntimeJson::from_value(serde_json::json!({}))?,
            )?;
            let value = controller.finish().await?;
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(value.as_str())?,
                serde_json::json!({"cursor":1,"key":"credential-private"})
            );
        }
        controller.cancel().await?;
        service.0.lock().unwrap().controller = Some(controller);
    }
    Ok(())
}
