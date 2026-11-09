//! Real installed source/SDK -> separately owned Gluetun -> actual public TLS.
//! Initial ACTIVE enrollment and configured VPN consent are fixture data only.
use super::*;
use crate::{
    db::models::SecretScope,
    extensions::store::{ExtensionStore, NewSecret},
    marketplace::{application_egress::Configuration, network_lab::NetworkLab},
};
use elixir_marketplace::connectivity::ProbeFamily;
use futures_util::FutureExt;

fn cold_network_limits() -> SourceExecutionLimits {
    // Explicit existing heavy-action profile for cold Docker/VPN setup plus
    // multiple real requests. This changes TIME ONLY, not ordinary defaults,
    // source memory/CPU, SDK calls, request/response bytes or relay allowances.
    let mut selected = limits();
    selected.profile = elixir_marketplace::source_execution::SourceExecutionProfile::LargeFeed;
    selected.timeout_millis = selected.profile.maximum_timeout_millis();
    selected
}

#[test]
fn cold_network_profile_changes_only_explicit_time_not_source_or_sdk_allocations() -> Result<()> {
    let original = limits();
    let selected = cold_network_limits();
    selected.validate()?;
    assert_eq!(selected.timeout_millis, 90_000);
    assert_eq!(
        selected.minimum_memory_bytes()?,
        original.minimum_memory_bytes()?
    );
    let mut original = serde_json::to_value(original)?;
    let mut selected = serde_json::to_value(selected)?;
    for value in [&mut original, &mut selected] {
        value.as_object_mut().unwrap().remove("profile");
        value.as_object_mut().unwrap().remove("timeout_millis");
    }
    assert_eq!(original, selected);
    Ok(())
}

#[tokio::test]
#[ignore = "explicit application Gluetun/real installed VM/public DNS+TLS, separate application resources; initial ACTIVE/consent synthetic, not off-site privacy or playback qualification"]
async fn installed_application_buffered_and_streaming_http_use_the_owned_vpn_without_a_catalog()
-> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            "off,elixir_server::marketplace::quarantine::execution::native::gateway=debug,elixir_server::marketplace::installed_call_timing=debug",
        )
        .with_test_writer()
        .try_init();
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let guard_image = std::env::var("ELIXIR_MARKETPLACE_GUARD_IMAGE")?;
    let gateway_image = std::env::var("ELIXIR_TEST_APPLICATION_GATEWAY_IMAGE")?;
    let f = Fixture::with_source(Some(
        r#"import {fetch,fetchStream} from '@elixir/sdk/v1';
async function request(url,stream) {
 if(stream) {
  const r=await fetchStream(url,{maximumBodyBytes:65536});
  let body=''; for await(const chunk of r.chunks()) body+=chunk;
  return {status:r.status,body};
 }
 const r=await fetch(url); return {status:r.status,body:await r.text()};
}
export async function invoke(a) {
 if(a.single) return [await request(a.url,a.stream)];
 return [await request(a.url,a.stream),await request(a.url,!a.stream)];
}"#,
    ))
    .await?;
    let manager = secrets();
    let mut lab = NetworkLab::new(f.db.clone())?;
    let mut controller: Option<InstalledController> = None;
    let before = crate::marketplace::accounting::available_transfer(
        &f.db,
        chrono::Utc::now().timestamp().try_into()?,
        &elixir_marketplace::policy::OrdinaryLimits::default(),
    )
    .await?;
    let outcome = std::panic::AssertUnwindSafe(async {
        lab.start(&guard_image).await?;
        let (runtime, network, config) = lab.application_exit().await?;
        assert_eq!(runtime.image_labels(&image).await?.get("elixir.marketplace.worker.source_eval_verified").map(String::as_str),Some("1"),
            "use the explicitly built pre-argument-verification image for this fast-path qualification");
        sqlx::query("INSERT INTO download_network_profiles (id,name,kind,enabled,strict,scope,gateway_runtime,config_json,status,active) VALUES ('application-fixture','Fixture VPN','wireguard_config',TRUE,TRUE,'marketplace_http','gluetun_wireguard',$1,'protected',FALSE)")
            .bind(json!({"gatewayImage":gateway_image}).to_string()).execute(&f.db.pool).await?;
        ExtensionStore::new(&f.db.pool).upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(), scope: SecretScope::Global, scope_id: None,
            key: "application-fixture-config".into(), value_encrypted: manager.encrypt(&config)?, rotatable: true,
        }).await?;
        let identities = crate::network::vpn_identity::wireguard_keys(&config)?.into_iter().map(|key| (key,crate::network::vpn_identity::Purpose::MarketplaceHttp)).collect::<Vec<_>>();
        crate::network::vpn_identity::reserve(&f.db.pool,&identities).await?;
        drop(config);
        sqlx::query("INSERT INTO download_network_profile_secrets (profile_id,key,secret_ref) VALUES ('application-fixture','wireguard_config','global:application-fixture-config')")
            .execute(&f.db.pool).await?;
        let mut configured = configuration(&f, &image, &format!("app-installed-{}", Uuid::new_v4().simple()))?;
        configured.playback = lab.playback.clone();
        configured.runtime = runtime;
        configured.application_http = Some(Configuration::new(network, ProbeFamily::Ipv4)?);
        controller = Some(InstalledController::open(f.db.clone(), manager.clone(), configured).await?);
        let instance = standing_tests::enroll(&f).await?;
        for (invocations, (streaming, single)) in [(false,false),(true,false),(false,true)].into_iter().enumerate() {
            let image_lookups = lab.application_image_lookups();
            let http_starts = lab.application_http_starts();
            let modes = lab.application_http_modes();
            let source_modes = lab.application_source_modes();
            let controls = lab.application_control_times();
            let selected = if single { limits() } else { cold_network_limits() };
            let timeout_millis = selected.timeout_millis;
            controller.as_mut().unwrap().begin(
                standing_tests::binding(&f, instance).await?, selected,
                RuntimeJson::from_value(json!({"stream":streaming,"single":single,"url":"https://www.cloudflare.com/cdn-cgi/trace"}))?,
            )?;
            let began = std::time::Instant::now();
            let reply = timeout(Duration::from_secs(180), controller.as_mut().unwrap().finish()).await
                .context("original installed application HTTP call deadline")??;
            let value: serde_json::Value = serde_json::from_str(reply.as_str())?;
            let replies = value.as_array().context("explicit SDK requests must return")?;
            assert_eq!(replies.len(), if single { 1 } else { 2 });
            for reply in replies {
                assert_eq!(reply["status"], 200);
                assert!(reply["body"].as_str().is_some_and(|body| body.contains("warp=")));
            }
            assert_eq!(lab.application_gateway_starts(), 1,
                "separate original invocations must reuse the actual bounded warm tunnel, not source authority");
            assert_eq!(lab.application_image_lookups() - image_lookups, usize::from(invocations==0),
                "the retained pinned tunnel does not repeat image resolution on a new invocation");
            let helpers = lab.application_http_starts() - http_starts;
            assert!(if single { helpers == 2 } else { (3..=4).contains(&helpers) },
                "two actual requests plus an initial probe, at most one age-required re-probe; no hidden retry");
            let after_modes = lab.application_http_modes();
            let after_source_modes = lab.application_source_modes();
            assert_eq!(after_source_modes.0 - source_modes.0,0,"no redundant standalone matcher on the verified executor path");
            assert_eq!(after_source_modes.1 - source_modes.1,1,"one actual executor must verify before receiving arguments/SDK authority");
            assert_eq!(after_modes.1 - modes.1, usize::from(!single), "only explicit fetchStream uses the streaming helper");
            assert_eq!(after_modes.0 - modes.0, helpers - usize::from(!single), "the fixed probe and fetch use single-exchange buffered helpers");
            let after_controls = lab.application_control_times();
            let count = |method| after_controls.get(method).copied().unwrap_or_default().0
                .saturating_sub(controls.get(method).copied().unwrap_or_default().0);
            assert!(invocations>0 || count("observe_application_gateway") >= 1,
                "startup waits for the original tunnel before a successful full policy baseline exists");
            assert!(count("observe_unchanged_application_gateway") > 0,
                "later checks must read the fresh complete policy against its immutable baseline");
            controller.as_ref().unwrap().require_idle()?;
            eprintln!("actual installed VPN invocation {} completed in {:?}; {helpers} actual HTTP helpers, one tunnel across distinct invocations; source timeout {timeout_millis}ms, unchanged memory/CPU/SDK budgets", invocations + 1, began.elapsed());
            let mut times: Vec<_> = lab.application_control_times().into_iter().filter_map(|(method,(calls,millis))| {
                let before = controls.get(method).copied().unwrap_or_default();
                let calls = calls.saturating_sub(before.0);
                (calls > 0).then_some((method,calls,millis.saturating_sub(before.1)))
            }).collect();
            times.sort_by_key(|(_,_,millis)|std::cmp::Reverse(*millis));
            times.truncate(8);
            eprintln!("actual native control costs (method,calls,total_ms; overlapping calls do not sum to wall time): {times:?}");
        }
        lab.require_application_process_headroom().await?;
        let held=standing_tests::binding(&f,instance).await?;
        let starts=lab.application_http_starts();
        sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1").bind(instance.to_string()).execute(&f.db.pool).await?;
        controller.as_mut().unwrap().begin(held,cold_network_limits(),RuntimeJson::from_value(json!({"stream":true,"url":"https://www.cloudflare.com/cdn-cgi/trace"}))?)?;
        assert!(controller.as_mut().unwrap().finish().await.is_err(),"an idle warm tunnel cannot restore a withdrawn installed grant");
        assert_eq!(lab.application_http_starts(),starts,"withdrawn authorization must not open another HTTP child");
        controller.as_ref().unwrap().require_idle()?;
        let after = crate::marketplace::accounting::available_transfer(
            &f.db, chrono::Utc::now().timestamp().try_into()?,
            &elixir_marketplace::policy::OrdinaryLimits::default(),
        ).await?;
        assert_eq!(before, after, "application traffic must not spend relay allowances");
        eprintln!("actual installed buffered+streaming HTTP used separately owned Gluetun and real DNS/TLS without catalog/participation or relay accounting; no provider port/public activation/off-site/playback claim");
        Ok::<_, anyhow::Error>(())
    }).catch_unwind().await;
    if !matches!(&outcome, Ok(Ok(()))) {
        // Fixed compiled method names and aggregate costs only. Preserve useful
        // phase evidence even if the original gateway has already retired.
        eprintln!(
            "actual failed application native control costs: {:?}",
            lab.application_control_times()
        );
        lab.diagnose_application_exit().await;
    }
    let calls = match controller.as_mut() {
        Some(controller) => controller.shutdown_cache().await,
        None => Ok(()),
    };
    let network = lab.cleanup().await;
    if calls.is_err() || network.is_err() {
        let path = f.root.path().to_owned();
        let _ = f.root.keep();
        bail!(
            "application HTTP cleanup unconfirmed; retained fixture at {}",
            path.display()
        );
    }
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
