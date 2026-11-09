//! Real server/runtime/native-private-pipe integration. The diagnostic image
//! mocks DNS/TLS; issuer claims and protection/resource scheduling are synthetic.
//! No provider enrollment, network, public eligibility or installation grant.
use super::*;
use elixir_marketplace::admission::CandidateDescriptor;
use futures_util::FutureExt;
use std::path::PathBuf;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    schema_version: u32,
    fixture_only: bool,
    simulated_paid_support: bool,
    commit: String,
    descriptors: Vec<CandidateDescriptor>,
}
async fn claims(root: &PathBuf) -> Result<Vec<CandidateDescriptor>> {
    let output = timeout(
        Duration::from_secs(10),
        tokio::process::Command::new("node")
            .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".."))
            .args([
                "crates/elixir_marketplace/worker/test/source-batch-claims.mjs",
                "--isolated-test-only",
                "--claims",
            ])
            .arg(root)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() || output.stdout.len() > 512 * 1024 || output.stderr.len() > 1024 {
        bail!("bounded isolated source fixture generation failed");
    }
    let claims: Claims = canonical::decode(&output.stdout, 512 * 1024)?;
    if claims.schema_version != 1
        || !claims.fixture_only
        || !claims.simulated_paid_support
        || claims.descriptors.len() != 20
        || claims.commit.len() != 40
        || !claims
            .commit
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        bail!("explicit isolated twenty-source fixture required");
    }
    Ok(claims.descriptors)
}

#[tokio::test]
#[ignore = "exclusive actual server/native source-batch pipe and search-only cache; diagnostic image mocks DNS/TLS, simulated support/protection, no public network/billing/install claim"]
async fn sandboxed_source_batch_populates_only_search_cache_through_real_server_owned_pipe()
-> Result<()> {
    if std::env::var("ELIXIR_MARKETPLACE_SOURCE_BATCH_BRIDGE_TEST").as_deref() != Ok("1") {
        bail!("explicit isolated bridge consent required");
    }
    let input =
        PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_SOURCE_BATCH_FIXTURE")?).canonicalize()?;
    let descriptors = claims(&input).await?;
    let image = std::env::var("ELIXIR_MARKETPLACE_SOURCE_BATCH_BRIDGE_IMAGE")?;
    if !crate::marketplace::runtime::image_is_pinned(&image) {
        bail!("immutable diagnostic image required");
    }
    let runtime: Arc<dyn RuntimeManager> =
        Arc::new(crate::runtime::docker::DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&image).await?;
    for key in [
        "elixir.marketplace.worker.source_batch_bridge_fixture",
        "elixir.marketplace.worker.source_batch",
        "elixir.marketplace.worker.metadata_batch",
    ] {
        if labels.get(key).map(String::as_str) != Some("1") {
            bail!("explicit diagnostic batch image capability required");
        }
    }
    let directory = tempfile::tempdir()?;
    let controller = directory.path().canonicalize()?.join("controller");
    let deployment = format!("source-batch-{}", uuid::Uuid::new_v4().simple());
    let issuer = descriptors[0].gate_evidence[0]
        .credential
        .payload
        .issuer_key_id;
    let worker = crate::marketplace::worker::OfflineWorker::start(
        runtime.as_ref(),
        &controller,
        &deployment,
        &image,
        &[issuer],
        crate::marketplace::packet::PacketWorkloadBudget::split(
            &elixir_marketplace::policy::OrdinaryLimits::default(),
        )?
        .worker_limits(),
    )
    .await?;
    let outcome=std::panic::AssertUnwindSafe(async {
        let namespace=runtime.exec_container_command(worker.handle(),"10004:10004",&[
            "/usr/bin/stat".into(),"-L".into(),"-c".into(),"%i".into(),"/proc/self/ns/net".into()]).await?;
        if namespace.len()>64 {bail!("native namespace observation bound");}
        let namespace:u64=namespace.trim().parse()?;
        let mut fixture=super::super::super::tests::fixture()?;
        let owner=Arc::get_mut(&mut fixture.owner).context("original isolated source owner")?;
        owner.runtime=runtime.clone();owner.handle=worker.handle().clone();owner.profile=worker.profile().clone();
        owner.binding.namespace=namespace;owner.binding.issuer_keys=vec![issuer];
        fixture.settings.send_modify(|s|{s.metering=Metering::Metered;s.accepted_disclosure_version=None;});
        fixture.facts.send_modify(|f|{let f=f.as_mut().unwrap();f.playback_pressure=true;f.background_allowance=false;});
        fixture.protection.send_modify(|s|s.until=Some(Instant::now()+Duration::from_secs(300)));
        let client=Client(fixture.owner.clone());
        assert!(Work::Background.admit(&fixture.owner,true).is_err());
        // Real native signature/adoption verification, but the issuer deliberately
        // represents isolated simulated accounts, not actual paid subscribers.
        for chunk in descriptors.chunks(10) {
            fixture.facts.send_modify(|f|f.as_mut().unwrap().observed=Instant::now());
            client.warm_metadata(chunk).await?;
        }
        let mut metadata=Vec::new();
        for d in &descriptors {
            let value=client.cached_metadata(d)?.context("native metadata input missing")?;
            assert_eq!(value.support()?.verified_supporters,5);metadata.push(value);
        }
        fixture.facts.send_modify(|f|f.as_mut().unwrap().observed=Instant::now());
        let began=std::time::Instant::now();
        client.warm_search_sources(metadata).await?;
        let elapsed=began.elapsed();
        // Observe the SAME completed task's slot release; never launch/retry work.
        let held=timeout(Duration::from_secs(2),fixture.owner.slots.clone().acquire_owned()).await??;
        fixture.owner.playback.set_test_startup_queue_len(1);
        fixture.protection.send_modify(|s|s.state=ProtectionState::Checking);
        let mut facts=Vec::new();let mut observed=None;
        for (n,d) in descriptors.iter().enumerate() {
            let metadata=client.cached_metadata(d)?.context("native metadata expired early")?;
            let plan=SourcePreflightRequest::for_release(&d.release.payload,0,Digest::of(b"scope check"),ProbeFamily::Ipv4)?.plan;
            assert!(fixture.owner.source_cache.lock().unwrap().get(plan.release_id,plan.location_id).is_none(),"warming must not mint an installation-source fact");
            if n==19 {
                assert!(client.request_search_metadata(&metadata,0).await.is_err(),"fixed unavailable source acquired a positive cache hit");
                continue;
            }
            let source=client.request_search_metadata(&metadata,0).await?;
            assert_eq!(source.release_id(),plan.release_id);
            assert!(source.valid_for()>Duration::from_secs(3600));
            let license=std::fs::read(input.join("source").join(&d.release.payload.source.subdirectory).join("LICENSE"))?;
            assert_eq!(source.license_sha256,Digest::of(license));
            if let Some(origin)=observed {assert_eq!((source.observed,source.observed_utc),origin);}
            observed=Some((source.observed,source.observed_utc));facts.push(source);
        }
        assert_eq!(facts.len(),19);
        drop(held);
        fixture.protection.send_modify(|s|s.state=ProtectionState::Ready);
        let first=client.cached_metadata(&descriptors[0])?.context("first checked metadata missing")?;
        assert!(client.request_fresh_metadata(&first,0).await.is_err(),"warm browsing must not bypass playback admission for fresh installation checks");
        fixture.owner.playback.set_test_startup_queue_len(0);
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));
        fixture.protection.send_modify(|s|{s.state=ProtectionState::Stopped;s.until=None;});
        assert!(facts.iter().all(|f|f.valid_for().is_zero()));
        eprintln!("{}",serde_json::json!({"native_source_batch_bridge":true,"inputs":20,"search_positive":19,"unavailable":1,
            "elapsed_ms":elapsed.as_millis(),"install_source_facts":0,"background_admitted":false,
            "excludes":"real DNS/TLS, VPN/credits, real paid eligibility, durable search/UI, package installation, public first-page speed or scale"}));
        Ok::<(),anyhow::Error>(())
    }).catch_unwind().await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("original isolated batch operation failed: {error:#}");
    }
    let stopped = worker.stop(runtime.as_ref()).await;
    let removed = if stopped.is_ok() {
        crate::marketplace::preparation::remove_native_activation_host(
            runtime.as_ref(),
            &deployment,
        )
        .await
    } else {
        Ok(())
    };
    if !outcome.as_ref().is_ok_and(|r| r.is_ok()) || stopped.is_err() || removed.is_err() {
        eprintln!(
            "isolated batch recovery directory retained at {}",
            directory.keep().display()
        );
    }
    stopped?;
    removed?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
