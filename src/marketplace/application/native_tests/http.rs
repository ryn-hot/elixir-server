//! Actual HTTP authentication/DTO/facade -> original native app commands/SQL.
//! Mutation routes are privately merged for qualification, not public enablement.
//! Only discovery/admission/protection observations remain synthetic.
use super::*;
use axum::{
    Router,
    body::{self, Body},
    http::Request,
};
use serde_json::{Value, json};
use tower::ServiceExt;
mod cache_tamper;
pub(in crate::marketplace) use cache_tamper::mutate as mutate_cached_fixture_bytes;
mod restart;
mod source_live;

async fn call(
    app: &Router,
    token: &str,
    method: &str,
    path: &str,
    data: Option<Value>,
) -> Result<Value> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    let body = if let Some(data) = data {
        request = request.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&data)?)
    } else {
        Body::empty()
    };
    let response = app.clone().oneshot(request.body(body)?).await?;
    anyhow::ensure!(
        response.status().is_success(),
        "HTTP management request failed: {}",
        response.status()
    );
    let bytes = body::to_bytes(response.into_body(), 128 * 1024).await?;
    anyhow::ensure!(
        !String::from_utf8_lossy(&bytes).contains("credential-private"),
        "input credentials leaked into management response"
    );
    Ok(serde_json::from_slice(&bytes)?)
}
async fn observed(app: &Router, token: &str, id: Uuid, wanted: &str) -> Result<Value> {
    observed_after_wait(app, token, id, wanted, false).await
}
async fn observed_after_wait(
    app: &Router,
    token: &str,
    id: Uuid,
    wanted: &str,
    require_wait: bool,
) -> Result<Value> {
    timeout(Duration::from_secs(240), async {
        let mut waited = false;
        loop {
            let view = call(
                app,
                token,
                "GET",
                &format!("/api/v1/marketplace/installations/{id}"),
                None,
            )
            .await?;
            waited |= view["preparation"]["phase"] == "waiting_for_resources";
            let pending = matches!(
                view["latest_action"]["status"].as_str(),
                Some("queued" | "running")
            );
            // Phase publication can precede the action acknowledgement. Do not
            // race the next input against that same still-owned action slot.
            if view["phase"] == wanted && !pending {
                anyhow::ensure!(
                    !require_wait || waited,
                    "original preparation did not report its forced admission wait"
                );
                return Ok(view);
            }
            if view["phase"] != wanted
                && matches!(
                    view["phase"].as_str(),
                    Some("recovery_required" | "stopped" | "cancelled")
                )
            {
                bail!(
                    "HTTP command reached {:?}, expected {wanted}",
                    view["phase"].as_str()
                );
            }
            sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .context("HTTP native command phase deadline")?
}
async fn action(app: &Router, token: &str, id: Uuid, data: Value) -> Result<Value> {
    call(
        app,
        token,
        "POST",
        &format!("/api/v1/marketplace/installations/{id}/actions"),
        Some(json!({"schema_version":1,"request":data})),
    )
    .await
}
type SdkRows = (
    Vec<(String, String, String, String, i64, i64, i64, String)>,
    Vec<(String, String, i64, String, i64)>,
);
async fn sdk_rows(database: &Database) -> Result<SdkRows> {
    let roots=sqlx::query_as("SELECT instance_id,extension_id,home_id,purpose,revision,key_count,encoded_bytes,CAST(updated_at AS TEXT) FROM marketplace_instance_sdk_state ORDER BY instance_id")
        .fetch_all(&database.pool).await?;
    let values=sqlx::query_as("SELECT instance_id,key,revision,value_encrypted,encoded_bytes FROM marketplace_instance_sdk_values ORDER BY instance_id,key")
        .fetch_all(&database.pool).await?;
    Ok((roots, values))
}
async fn require_idle_byte_workspace(
    runtime: &dyn RuntimeManager,
    lane: &Path,
    present: bool,
) -> Result<()> {
    let lease = ControllerLease::acquire(lane)?;
    let deployment = lease.storage_namespace()?;
    let name = format!("elixir-marketplace-source-{deployment}");
    let labels = std::collections::HashMap::from([
        (
            crate::runtime::model::ELIXIR_MANAGED_LABEL.into(),
            "true".into(),
        ),
        (
            crate::runtime::model::ELIXIR_DEPLOYMENT_ID_LABEL.into(),
            deployment.clone(),
        ),
        ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
        ("elixir.marketplace.cache_slot".into(), "source-v1".into()),
    ]);
    let state = runtime.source_cache_state(&name, &labels).await?;
    assert!(
        if present {
            matches!(
                state,
                crate::runtime::model::SourceCacheState::Present(Some(_))
            )
        } else {
            state == crate::runtime::model::SourceCacheState::Absent
        },
        "joined original call/shutdown did not leave the expected bounded byte workspace"
    );
    assert!(
        runtime
            .get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}"))
            .await?
            .is_none(),
        "idle cached bytes must not retain a native process"
    );
    Ok(())
}
#[tokio::test]
#[ignore = "explicit actual private HTTP auth/DTO/target choices/original native installation/offline cancellation+invocation; synthetic selection/protection"]
async fn native_http_authenticated_management_owns_install_and_offline_cancellation() -> Result<()>
{
    run(None, 100).await
}
#[tokio::test]
#[ignore = "exclusive actual native foreground CPU comparison, 64 MiB/200m with signed reviewed quota; synthetic initial selection/protection, not production sizing"]
async fn native_http_foreground_fifth_core_preserves_install_cancel_call_and_restart() -> Result<()>
{
    run(None, 200).await
}
#[tokio::test]
#[ignore = "exclusive actual native foreground CPU comparison, 64 MiB/500m with signed reviewed quota; synthetic initial selection/protection, not production sizing"]
async fn native_http_foreground_half_core_preserves_install_cancel_call_and_restart() -> Result<()>
{
    run(None, 500).await
}
#[tokio::test]
#[ignore = "explicit NEW empty loopback PostgreSQL with actual HTTP auth/original native app commands; not public network qualification"]
async fn postgres_native_http_authenticated_management_owns_install_and_cancellation() -> Result<()>
{
    run(
        Some(&std::env::var(
            "ELIXIR_TEST_MARKETPLACE_APPLICATION_HTTP_POSTGRES_URL",
        )?),
        100,
    )
    .await
}
#[tokio::test]
async fn foreground_comparison_requires_declared_quota_and_keeps_entrypoint_bytes() -> Result<()> {
    let mut foreground = allocation();
    foreground.source.cpu_millicores = 500;
    foreground.validate()?;
    for cpu in [100, 200, 500] {
        let value = sdk_fixture_profile_with_cpu(true, cpu).await?;
        let (release, package, proof) =
            crate::marketplace::quarantine::tests::decode_fixture(value)?;
        let mut budget = foreground.source.clone();
        budget.cpu_millicores = cpu;
        let plan = crate::marketplace::quarantine::SourceQuarantinePlan::for_release(
            &release, None, &budget,
        )?;
        let summary = plan
            .source_descriptor()
            .verify(std::io::Cursor::new(package), std::io::Cursor::new(proof))?;
        let manifest = serde_json::from_value(summary.manifest()?.value().clone())?;
        let payload = release.payload();
        let review = crate::marketplace::grants::build_review(
            &manifest,
            payload.runtime_profile,
            release.release_id(),
            payload.manifest_sha256,
            &payload.artifacts,
        )?;
        let resources = review
            .scope()
            .runtime()
            .context("fixture runtime resources")?;
        assert_eq!(resources.cpu_millicores, cpu);
        assert_eq!(
            summary.entrypoint.context("fixture entrypoint")?.sha256,
            Digest::of(LIVE_SOURCE.as_bytes())
        );
        plan.require_execution_resources(resources)?;
        if cpu < foreground.source.cpu_millicores {
            let larger = crate::marketplace::quarantine::SourceQuarantinePlan::for_release(
                &release,
                None,
                &foreground.source,
            )?;
            assert!(
                larger.require_execution_resources(resources).is_err(),
                "trusted allocation cannot exceed the approved manifest quota"
            );
        }
    }
    Ok(())
}
async fn run(postgres: Option<&str>, cpu_millicores: u32) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        // Preserve other targets at WARN; only fixed, payload-free phase
        // durations are DEBUG. Never enable blanket private-transport logging.
        .with_env_filter("warn,elixir_server::marketplace::installed_call_timing=debug")
        .with_test_writer()
        .try_init();
    let mut configured = allocation();
    configured.source.cpu_millicores = cpu_millicores;
    configured.validate()?;
    eprintln!(
        "native HTTP source profile: memory_bytes={} cpu_millicores={}",
        configured.source.memory_bytes, cpu_millicores
    );
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable image required")?;
    let image = docker(&["image", "inspect", "--format", "{{.Id}}", &image]).await?;
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?;
    let files = directory.join("source");
    let controller_root = directory.join("marketplace").join("worker");
    for path in [directory.join("marketplace"), controller_root.clone()] {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    let lane = controller_root.join("foreground-source");
    restart::write_profile(&directory, &image, &configured)?;
    std::fs::create_dir(&files)?;
    let value = sdk_fixture_profile_with_cpu(true, cpu_millicores).await?;
    let (release, package, proof) =
        crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
    for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let db = database(&directory, postgres).await?;
    let sqlite_url = format!(
        "sqlite://{}",
        directory.join("application.sqlite").display()
    );
    let (state, public, principal, token) =
        crate::http::handlers::marketplace::installation_http_fixture(
            db.clone(),
            &directory,
            postgres.unwrap_or(&sqlite_url),
        )
        .await?;
    let app = public
        .merge(crate::http::handlers::marketplace::installation_routes().with_state(state.clone()));
    state
        .marketplace
        .configure_installation(configured.clone())?;
    let original = state.marketplace.fixture_original_installation();
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let name = format!("elixir-marketplace-http-peer-{}", Uuid::new_v4().simple());
    let host_deployment = format!("http-host-{}", Uuid::new_v4().simple());
    let mut peer = None;
    let mut host = None;
    let outcome=std::panic::AssertUnwindSafe(Box::pin(async{
        let issuer=value["issuer_key"].as_str().context("fixture issuer")?;
        peer=Some(PeerPipeFixture::start(&image,&files,&name,issuer,"providers").await?);check_profile(&name).await?;
        host=Some(OfflineWorker::start(runtime.as_ref(),&directory.join("admission-host"),&host_deployment,&image,&[issuer.parse()?],
            crate::marketplace::packet::PacketWorkloadBudget::split(&elixir_marketplace::policy::OrdinaryLimits::default())?.worker_limits()).await?);
        let p=peer.as_mut().unwrap();p.bind_security_database(db.clone());
        p.bind_offline_source_owner(host.as_ref().unwrap(),runtime.clone(),&lane)?;
        // Negative denial only; successful admission still uses actual host/OS
        // measurements. The SAME original unopened source must survive it.
        p.defer_next_source_admission()?;
        let client=p.client.clone();
        let playback=client.installed_configuration(configured.source.clone(),configured.retention)?.playback;
        let(desc,policy)=descriptor(&value)?;let(metadata,_metadata_owner)=MetadataObservation::fixture_for_verified_descriptor(desc,&policy)?;
        security_state::admit(&db,&metadata).await?;
        let(source,_source_owner)=PublicSourceObservation::fixture_for_source(release.release_id(),Digest::of(b"SPDX-License-Identifier: MIT\n"));
        let selected=Selection::fixture_for_native_preparation(client.clone(),metadata,source,vec![p.peer.clone()])?;
        let query=Uuid::new_v4();state.marketplace.fixture_install_context(InstallContext::fixture_selected(client.clone(),query,selected,false))?;
        assert_eq!(call(&app,&token,"GET","/api/v1/marketplace/installations/current",None).await?,json!({"schema_version":1,"operation":null}));
        let targets=call(&app,&token,"POST","/api/v1/marketplace/install/targets",Some(json!({"schema_version":1,"extension_id":release.payload().extension_id}))).await?;
        let install=json!({"schema_version":1,"query_id":query,"extension_id":release.payload().extension_id,"release_id":release.release_id(),
            "target":{"kind":"new","name":"HTTP owner A"},"expected_revision":targets["new_instance"]["expected_revision"],"mutation_id":Uuid::new_v4(),"controls":targets["new_instance"]["controls"]});
        let started=call(&app,&token,"POST","/api/v1/marketplace/installations",Some(install.clone())).await?;
        let id:Uuid=serde_json::from_value(started["operation_id"].clone())?;
        let recovered=call(&app,&token,"GET","/api/v1/marketplace/installations/current",None).await?;
        assert_eq!(recovered["operation"]["initial_request"],install,"reconnect must preserve the original target, controls and mutation input, not guess/restamp it");
        assert_eq!(recovered["operation"]["operation_id"],started["operation_id"],
            "a reconnected client can discover the original operation without replaying start input");
        assert_eq!(call(&app,&token,"POST","/api/v1/marketplace/installations",Some(install.clone())).await?["operation_id"],started["operation_id"],"actual HTTP start retry must acknowledge without another context");
        let review=observed_after_wait(&app,&token,id,"awaiting_approval",true).await?;
        assert_eq!(review["approval_revision"],0);assert!(review["review_id"].is_string());
        let approval=Uuid::new_v4();
        action(&app,&token,id,json!({"action":"approve","review_id":review["review_id"],"expected_revision":review["approval_revision"],"mutation_id":approval,"automatic_updates":false})).await?;
        let staged=observed(&app,&token,id,"awaiting_setup").await?;
        assert_eq!(staged["revision"],1);assert_eq!(staged["installation_ready"],false);
        let setup=Uuid::new_v4();
        action(&app,&token,id,json!({"action":"setup","mutation_id":setup,"credentials":[{"operation":"set","key":"source_api","value":"credential-private","rotatable":false}]})).await?;
        let ready=observed(&app,&token,id,"awaiting_commit").await?;assert!(ready["available_actions"].as_array().unwrap().contains(&json!("activate")));
        let commit=Uuid::new_v4();action(&app,&token,id,json!({"action":"activate","mutation_id":commit})).await?;
        let active=observed(&app,&token,id,"activated").await?;let owner:Uuid=serde_json::from_value(active["configured_instance"].clone())?;
        let entry=original.entry(id)?;entry.task.lock().await.join().await?;
        assert_eq!(call(&app,&token,"POST","/api/v1/marketplace/installations",Some(install)).await?["operation_id"],started["operation_id"],
            "start acknowledgement must not depend on a new managed network client");
        let targets=call(&app,&token,"POST","/api/v1/marketplace/install/targets",Some(json!({"schema_version":1,"extension_id":release.payload().extension_id}))).await?;
        assert_eq!(targets["instances"][0]["instance_id"],json!(owner));assert!(targets["instances"][0]["controls"].is_object());
        let(next_desc,next_policy)=descriptor(&value)?;let(next_metadata,_next_meta_owner)=MetadataObservation::fixture_for_verified_descriptor(next_desc,&next_policy)?;
        security_state::admit(&db,&next_metadata).await?;
        let(next_source,_next_source_owner)=PublicSourceObservation::fixture_for_source(release.release_id(),Digest::of(b"SPDX-License-Identifier: MIT\n"));
        let next_selected=Selection::fixture_for_native_preparation(client.clone(),next_metadata,next_source,vec![peer.as_ref().unwrap().peer.clone()])?;
        let next_query=Uuid::new_v4();state.marketplace.fixture_install_context(InstallContext::fixture_selected(client,next_query,next_selected,false))?;
        let next=call(&app,&token,"POST","/api/v1/marketplace/installations",Some(json!({"schema_version":1,"query_id":next_query,"extension_id":release.payload().extension_id,"release_id":release.release_id(),
            "target":{"kind":"new","name":"HTTP cancelled B"},"expected_revision":targets["new_instance"]["expected_revision"],"mutation_id":Uuid::new_v4(),"controls":targets["new_instance"]["controls"]}))).await?;
        let next_id:Uuid=serde_json::from_value(next["operation_id"].clone())?;assert_ne!(next_id,id);
        let review=observed(&app,&token,next_id,"awaiting_approval").await?;assert_eq!(review["approval_revision"],1);
        action(&app,&token,next_id,json!({"action":"approve","review_id":review["review_id"],"expected_revision":review["approval_revision"],"mutation_id":Uuid::new_v4(),"automatic_updates":false})).await?;
        let staged=observed(&app,&token,next_id,"awaiting_setup").await?;assert_eq!(staged["revision"],1);
        peer.take().unwrap().finish().await?;
        assert_eq!(call(&app,&token,"GET","/api/v1/marketplace/installations/current",None).await?["operation"]["operation_id"],json!(next_id));
        let cancel=Uuid::new_v4();action(&app,&token,next_id,json!({"action":"cancel","mutation_id":cancel})).await?;
        let cancelled=observed(&app,&token,next_id,"cancelled").await?;
        assert_eq!(cancelled["revision"],2);assert_eq!(cancelled["cancellation_request_id"],json!(cancel));
        original.entry(next_id)?.task.lock().await.join().await?;
        playback.insert_test_job(Uuid::new_v4(),directory.join("steady-playback"),0).await;
        assert!(playback.has_latency_sensitive_work());
        assert_eq!(playback.startup_queue_len(),0,
            "steady managed playback must not become a foreground source veto");
        let mut installed=state.marketplace.begin_installed_call(principal.home_id,release.payload().extension_id.clone(),owner,RuntimeJson::from_value(json!({}))?)?;
        assert!(state.marketplace.begin_installed_call(principal.home_id,release.payload().extension_id.clone(),owner,RuntimeJson::from_value(json!({}))?).is_err(),
            "a running call or its unconsumed result must keep the original bounded slot");
        let value=installed.finish().await?;assert_eq!(serde_json::from_str::<Value>(value.as_str())?,json!({"cursor":1,"key":"credential-private"}));
        require_idle_byte_workspace(runtime.as_ref(),&lane,true).await?;
        let before_foreign=sdk_rows(&db).await?;
        let mut foreign=state.marketplace.begin_installed_call(Uuid::new_v4(),release.payload().extension_id.clone(),owner,RuntimeJson::from_value(json!({}))?)?;
        assert!(foreign.finish().await.is_err(),"an internal caller's home/instance DATA cannot replace fresh ACTIVE binding checks");
        foreign.cancel().await?;drop(foreign);
        assert_eq!(sdk_rows(&db).await?,before_foreign,"denied foreign input must preserve every SDK root, counter and encrypted value byte");
        let mut again=state.marketplace.begin_installed_call(principal.home_id,release.payload().extension_id.clone(),owner,RuntimeJson::from_value(json!({}))?)?;
        installed.cancel().await?;drop(installed);
        let value=again.finish().await?;assert_eq!(serde_json::from_str::<Value>(value.as_str())?,json!({"cursor":1,"key":"credential-private"}),
            "ordinary invoke must not rerun migration, and an old consumer cannot stop the next same-controller native call");
        again.cancel().await?;drop(again);
        source_live::qualify(&db,&state,principal.home_id,principal.user_id,owner).await?;
        require_idle_byte_workspace(runtime.as_ref(),&lane,true).await?;
        cache_tamper::qualify(&state,&db,runtime.as_ref(),(&lane,&image),principal.home_id,owner,&release.payload().extension_id).await?;
        assert_eq!(call(&app,&token,"GET","/api/v1/marketplace/installations/current",None).await?["operation"]["operation_id"],json!(next_id),
            "serving an installed extension must retain the one latest terminal installation for reconnect");
        assert_eq!(call(&app,&token,"GET",&format!("/api/v1/marketplace/installations/{next_id}"),None).await?["phase"],json!("cancelled"));
        assert_eq!(action(&app,&token,next_id,json!({"action":"cancel","mutation_id":cancel})).await?["receipt"]["request_id"],json!(cancel),
            "exact terminal acknowledgement remains data-only after original controller transfer");
        let targets=call(&app,&token,"POST","/api/v1/marketplace/install/targets",Some(json!({"schema_version":1,"extension_id":release.payload().extension_id}))).await?;
        assert_eq!(targets["instances"].as_array().unwrap().len(),2);
        assert!(targets["instances"].as_array().unwrap().iter().all(|row|row["controls"].is_object()&&!row["pending"].as_bool().unwrap()));
        state.marketplace.shutdown().await?;
        require_idle_byte_workspace(runtime.as_ref(),&lane,false).await?;
        assert!(ControllerLease::acquire(&lane).is_ok());
        println!("actual authenticated HTTP owned install A/cancel B/offline A invocation passed; target data, exact inputs and original native owners retained; synthetic discovery/protection only");
        Ok::<_,anyhow::Error>(())
    })).catch_unwind().await;
    let stopped = state.marketplace.shutdown().await;
    let peer_stopped = match peer.take() {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let host_removed = match host.take() {
        Some(worker) => match worker.stop(runtime.as_ref()).await {
            Ok(()) => {
                crate::marketplace::preparation::remove_native_activation_host(
                    runtime.as_ref(),
                    &host_deployment,
                )
                .await
            }
            Err(e) => Err(e),
        },
        None => Ok(()),
    };
    let peer_removed = remove_peer(&name).await;
    db.pool.close().await;
    // Release EVERY old facade/router/controller after their actual cleanup.
    // A separate process must restore the existing lineage, not borrow it.
    drop(app);
    drop(original);
    drop(state);
    let restarted = if outcome.as_ref().is_ok_and(|r| r.is_ok())
        && stopped.is_ok()
        && peer_stopped.is_ok()
        && host_removed.is_ok()
        && peer_removed.is_ok()
    {
        restart::child(&directory, postgres.unwrap_or(&sqlite_url), &image).await
    } else {
        Ok(())
    };
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))?;
    if outcome.as_ref().is_err()
        || outcome.as_ref().is_ok_and(|r| r.is_err())
        || stopped.is_err()
        || peer_stopped.is_err()
        || host_removed.is_err()
        || peer_removed.is_err()
        || restarted.is_err()
    {
        eprintln!(
            "failed actual HTTP/native fixture preserved at {}",
            root.keep().display()
        );
    }
    stopped?;
    peer_stopped?;
    host_removed?;
    peer_removed?;
    restarted?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
