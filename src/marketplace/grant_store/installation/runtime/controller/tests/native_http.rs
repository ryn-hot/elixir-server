//! Actual protected owner -> SDK HTTP -> original installed controller/VM.
//! Initial account/consent/ACTIVE enrollment are explicitly synthetic fixtures,
//! not public activation, source hosting or peer-discovery qualification.
use super::*;
use crate::marketplace::{
    network_lab::NetworkLab,
    preferences::{ConnectionMetering, Preferences},
    worker::{PARTICIPATION_DISCLOSURE_VERSION, ParticipationClient, ParticipationPhase},
};
use anyhow::ensure;
use elixir_marketplace::policy::ContributionMode;
use futures_util::FutureExt;
use serde_json::json;

async fn wait_owner(client: &ParticipationClient) -> Result<()> {
    timeout(Duration::from_secs(60), async {
        loop {
            let snapshot = client.snapshot();
            ensure!(
                snapshot.phase != ParticipationPhase::Failed,
                "actual participation owner failed"
            );
            if snapshot.phase == ParticipationPhase::Running
                && snapshot.preferences_revision == Some(2)
                && snapshot.host_sample_current
            {
                let network = client.network_status().await?;
                ensure!(
                    network.active && network.consumer && !network.catalog && !network.routing,
                    "fixture must be an actual consumer, never a relay/contributor"
                );
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .context("actual preference/host sampler readiness deadline")?
}

#[tokio::test]
#[ignore = "explicit real protected HTTP/actual installed VM, synthetic initial account/consent/ACTIVE only; internet TLS and owned slow POST revocation, not public activation/off-site/aggregate qualification"]
async fn installed_fetch_uses_real_protected_owner_and_revocation_retires_original_http_without_replay()
-> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("off,elixir_server::marketplace::worker::protected::source_preflight::sdk_http=debug,elixir_server::marketplace::grant_store::installation::runtime::http=debug")
        .with_test_writer()
        .try_init();
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    let guard_image = std::env::var("ELIXIR_MARKETPLACE_GUARD_IMAGE")?;
    let f = Fixture::with_source(Some(
        r#"import {fetch,state} from '@elixir/sdk/v1';
export async function invoke(a) {
 await state.set('cursor',a.marker);
 const r=await fetch(a.url,{method:a.method,body:a.body??null});
 return {status:r.status,body:await r.text()};
}"#,
    ))
    .await?;
    // Test-only initial durable choices, not a fabricated observer/facts object.
    // The production observer/sampler must read these from the real database.
    let preferences = Preferences {
        contribution: ContributionMode::Off,
        metering: ConnectionMetering::Metered,
        contribution_disclosure_version: Some(PARTICIPATION_DISCLOSURE_VERSION),
        ..Preferences::default()
    };
    preferences.validate()?;
    let changed=sqlx::query("UPDATE marketplace_preferences SET revision=2,preferences_json=$1,mutation_id=$2 WHERE singleton=1 AND revision=1 AND home_id=$3")
        .bind(String::from_utf8(elixir_marketplace::canonical::to_vec(&preferences)?)?)
        .bind(Uuid::new_v4().to_string()).bind(f.actor.home.to_string()).execute(&f.db.pool).await?;
    ensure!(
        changed.rows_affected() == 1,
        "fixture preference lineage changed"
    );
    let mut lab = NetworkLab::new(f.db.clone())?;
    let mut owner: Option<InstalledController> = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        lab.start(&guard_image).await?;
        lab.start_slow_http().await?;
        let client = Box::pin(lab.start_participating(&image)).await?;
        wait_owner(&client).await?;
        let http = client.installed_http().context("actual protected image lacks installed HTTP")?;
        let mut config = configuration(&f,&image,&format!("http-installed-{}",Uuid::new_v4().simple()))?;
        config.runtime = lab.runtime.clone();
        config.playback = lab.playback.clone();
        // SAME global source lane as this actual protected owner; the verified
        // fixture archive stays under its original explicit retention owner.
        config.lane = lab.root.join("worker/foreground-source");
        owner = Some(Box::pin(InstalledController::open(f.db.clone(),manager(),config)).await?);
        // Establish the actual runtime lineage BEFORE installed history. Never
        // turn the no-implicit-adoption refusal into a fixture bypass.
        let instance = installed::enroll(&f).await?;
        owner.as_mut().unwrap().begin_with_http(installed::binding(&f,instance).await?,limits(),
            RuntimeJson::from_value(json!({"marker":"tls","url":"https://www.cloudflare.com/cdn-cgi/trace","method":"GET"}))?,Some(http.clone()))?;
        let reply = timeout(Duration::from_secs(180),owner.as_mut().unwrap().finish()).await
            .context("original installed TLS call deadline")??;
        let value: serde_json::Value = serde_json::from_str(reply.as_str())?;
        ensure!(value["status"]==200 && value["body"].as_str().is_some_and(|body|body.contains("warp=")),
            "installed source did not receive the real HTTPS trace");
        owner.as_ref().unwrap().require_idle()?;
        ensure!(lab.slow_posts().await?==0,"TLS test must not send a fixture POST");
        eprintln!("actual installed VM fetched public HTTPS through the original protected owner; no custom CA or native-scope factory");
        wait_owner(&client).await?;
        owner.as_mut().unwrap().begin_with_http(installed::binding(&f,instance).await?,limits(),
            RuntimeJson::from_value(json!({"marker":"revoke","url":"http://8.8.4.4:18080/slow","method":"POST","body":"owned-fixture-only"}))?,Some(http))?;
        timeout(Duration::from_secs(180),async {
            loop {
                if lab.slow_posts().await?==1 {return Ok::<_,anyhow::Error>(());}
                sleep(Duration::from_millis(200)).await;
            }
        }).await.context("actual installed POST did not reach owned fixture")??;
        // Revoke only AFTER the controlled upstream accepted ONE actual POST.
        // Cancellation cannot roll back that effect and must never replay it.
        crate::marketplace::grant_store::revoke_as(&f.db,&f.actor,
            &f.descriptor.release.payload.extension_id,f.revision,Uuid::new_v4(),Utc::now()).await?;
        ensure!(timeout(Duration::from_secs(60),owner.as_mut().unwrap().finish()).await
            .context("original revoked HTTP call retirement deadline")?.is_err(),
            "revoked installed HTTP unexpectedly returned success");
        owner.as_ref().unwrap().require_idle()?;
        ensure!(lab.slow_posts().await?==1,"revocation replayed the accepted POST");
        ensure!(client.snapshot().phase!=ParticipationPhase::Failed,"grant loss permanently faulted protected transport");
        let network=client.network_status().await?;
        ensure!(network.active && network.consumer && !network.catalog && !network.routing,
            "revocation changed transport ownership or contribution roles");
        eprintln!("in-flight installed grant revocation retired the same HTTP/native VM owners without replay; protected consumer remains live; initial enrollment synthetic; excludes public activation, off-site privacy, sustained refresh and aggregate playback");
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    // Join source/HTTP owners BEFORE stopping the worker that owns the helper.
    let calls = match owner.as_mut() {
        Some(owner) => owner.cancel().await,
        None => Ok(()),
    };
    let network = lab.cleanup().await;
    if calls.is_err() || network.is_err() {
        let path = f.root.path().to_owned();
        let _ = f.root.keep();
        bail!(
            "original installed HTTP cleanup unconfirmed; retained fixture at {}",
            path.display()
        );
    }
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
