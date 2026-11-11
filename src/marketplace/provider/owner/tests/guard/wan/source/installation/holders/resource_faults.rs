//! Actual server lifecycle, not an offline replacement harness. One approved
//! identity, real source/review/install, kernel OOM, ordinary retry and retained
//! installed invocation. Fault pressure never modifies shipping limits or code.
use super::*;
mod storage;

fn oom_event(bytes: &str, id: &str, deployment: &str, since: u64) -> Result<()> {
    ensure!(bytes.len() <= 16384, "owned OOM event bounds");
    let lines: Vec<_> = bytes.lines().filter(|line| !line.is_empty()).collect();
    ensure!(
        !lines.is_empty() && lines.len() <= 4,
        "owned kernel OOM event required"
    );
    for line in lines {
        let event: Value = serde_json::from_str(line)?;
        ensure!(
            event["Type"] == "container"
                && event["Action"] == "oom"
                && event["Actor"]["ID"] == id
                && event["Actor"]["Attributes"]["elixir.deployment_id"] == deployment
                && event["time"].as_u64().is_some_and(|time| time >= since),
            "OOM event differs from original native worker"
        );
    }
    Ok(())
}

#[test]
fn kernel_oom_evidence_cannot_be_replaced_by_an_exit_code_or_foreign_event() -> Result<()> {
    let base = json!({"Type":"container","Action":"oom","time":100,
        "Actor":{"ID":"a".repeat(64),"Attributes":{"elixir.deployment_id":"fixture"}}});
    let valid = serde_json::to_string(&base)?;
    oom_event(&valid, &"a".repeat(64), "fixture", 100)?;
    for changed in [
        json!({"ExitCode":137,"OOMKilled":false}),
        {
            let mut e = base.clone();
            e["Action"] = json!("die");
            e
        },
        {
            let mut e = base.clone();
            e["Actor"]["ID"] = json!("b".repeat(64));
            e
        },
        {
            let mut e = base.clone();
            e["Actor"]["Attributes"]["elixir.deployment_id"] = json!("foreign");
            e
        },
        {
            let mut e = base.clone();
            e["time"] = json!(99);
            e
        },
    ] {
        assert!(
            oom_event(
                &serde_json::to_string(&changed)?,
                &"a".repeat(64),
                "fixture",
                100
            )
            .is_err()
        );
    }
    assert!(oom_event("", &"a".repeat(64), "fixture", 100).is_err());
    assert!(
        oom_event(
            &std::iter::repeat_n(valid, 5).collect::<Vec<_>>().join("\n"),
            &"a".repeat(64),
            "fixture",
            100
        )
        .is_err()
    );
    Ok(())
}

async fn owned_worker(actor: &Participant, image: &str) -> Result<String> {
    let label = format!("label=elixir.deployment_id={}", actor.rig.deployment);
    let ids = docker(&[
        "ps",
        "--no-trunc",
        "--quiet",
        "--filter",
        &label,
        "--filter",
        "label=elixir.marketplace.isolation=fixed-v1",
    ])
    .await?;
    let ids: Vec<_> = ids.lines().filter(|v| !v.is_empty()).collect();
    ensure!(
        ids.len() == 1 && crate::runtime::packet_network::immutable_id(ids[0]),
        "original worker missing/ambiguous"
    );
    let values: Value = serde_json::from_str(&docker(&["inspect", ids[0]]).await?)?;
    let value = &values[0];
    ensure!(
        value["Id"] == ids[0]
            && value["Image"] == image
            && value["State"]["Running"] == true
            && value["Config"]["Labels"]["elixir.managed"] == "true"
            && value["Config"]["Labels"]["elixir.deployment_id"] == actor.rig.deployment
            && value["HostConfig"]["Memory"] == 224 * 1024 * 1024
            && value["HostConfig"]["MemorySwap"] == 224 * 1024 * 1024,
        "actual ordinary worker identity/resource allocation differs"
    );
    Ok(ids[0].to_owned())
}

async fn cause_oom(id: &str, deployment: &str) -> Result<()> {
    // A hardened PID can expose root-owned proc control. Use only its actual
    // proc-file owner inside this exact owned container; never add capabilities.
    let uid = docker(&[
        "exec",
        "--user=0:0",
        id,
        "/usr/bin/stat",
        "-c",
        "%u",
        "/proc/1/oom_score_adj",
    ])
    .await?;
    ensure!(
        matches!(uid.as_str(), "0" | "10004"),
        "unexpected original worker proc owner"
    );
    let user = format!("{uid}:{uid}");
    let since = now()?;
    let program = r#"const fs=require('node:fs');
if(process.pid===1)throw Error('fault injector cannot replace the original worker');
fs.writeFileSync('/proc/1/oom_score_adj','1000');
if(fs.readFileSync('/proc/1/oom_score_adj','utf8').trim()!=='1000')throw Error('OOM victim binding unavailable');
const allocations=[];
(async()=>{for(let n=0;n<64;n++){allocations.push(Buffer.alloc(8*1024*1024,59));await new Promise(r=>setTimeout(r,5));}
throw Error('kernel OOM did not terminate the original worker');})();"#;
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("docker")
            .args([
                "exec",
                "--user",
                &user,
                id,
                "/usr/local/bin/node",
                "--max-old-space-size=16",
                "-e",
                program,
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("original OOM injection deadline")??;
    ensure!(
        outcome.stdout.len() <= 1024
            && outcome.stderr.len() <= 4096
            && outcome.status.code() == Some(137),
        "original worker OOM injection did not terminate with kernel kill"
    );
    // The real supervisor may already have removed this container by now.
    // Require Docker's kernel OOM event, not a guessed failure/exit status.
    let until = now()?.checked_add(1).context("OOM clock bounds")?;
    let events = docker(&[
        "events",
        "--since",
        &since.to_string(),
        "--until",
        &until.to_string(),
        "--filter",
        "type=container",
        "--filter",
        &format!("container={id}"),
        "--filter",
        "event=oom",
        "--format",
        "{{json .}}",
    ])
    .await?;
    oom_event(&events, id, deployment, since)
}

async fn grant(actor: &Participant, source: &SourceFixture) -> Result<(i64, String, i64)> {
    sqlx::query_as("SELECT revision,review_json,CAST(CASE WHEN automatic_updates THEN 1 ELSE 0 END AS BIGINT) FROM marketplace_extension_grants WHERE extension_id=$1")
        .bind(source.publication.release.payload.extension_id.as_str()).fetch_one(&actor.rig.database.pool).await.map_err(Into::into)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "standing-approved one original free identity; approved dummy install, actual worker OOM and ordinary same-identity recovery"]
async fn worker_oom_recovers_original_server_owner_and_preserves_installed_source() -> Result<()> {
    for flag in ["WARP_LIFECYCLE", "WAN", "PUBLIC_SOURCE", "WORKER_OOM"] {
        ensure!(
            std::env::var(format!("ELIXIR_MARKETPLACE_{flag}_LIVE_TEST")).as_deref() == Ok("1"),
            "explicit isolated resource-fault qualification required"
        );
    }
    let sources = configured_fixtures()?;
    let image = immutable_image("ELIXIR_MARKETPLACE_WARP_PROVIDER_IMAGE")?;
    let guard = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let mut actor = Participant::new(runtime, &image).await?;
    eprintln!(
        "{}",
        json!({"stage":"resource_fault_layout","deployment":actor.rig.deployment,"recovery_root":actor.rig.root.path()})
    );
    let operation = std::panic::AssertUnwindSafe(Box::pin(async {
        let mut allocation = crate::marketplace::application::fixture_allocation();
        allocation.source.cpu_millicores=200;allocation.source.wall_seconds=600;allocation.validate()?;
        actor.start(&guard,&worker,Digest::of(Uuid::new_v4().as_bytes()),false,&allocation).await?;
        let instance = install_with_updates(actor.app()?,actor.state()?,actor.token()?,&sources[0],None,true,false).await?;
        let user = actor.principal.as_ref().context("original owner missing")?.user_id;
        provider(actor.state()?,&actor.rig.database,user,instance,"1.0.0").await?;
        let original_grant = grant(&actor,&sources[0]).await?;
        ensure!(original_grant.2==0,"resource-fault fixture must not enable automatic updates");
        let stale = actor.client()?;
        source_ready(&stale).await?;
        let old_metadata = stale.verify_publication(&sources[0].publication).await?;
        let original_worker = owned_worker(&actor,&worker).await?;
        cause_oom(&original_worker,&actor.rig.deployment).await?;
        eprintln!("{}",json!({"stage":"worker_oom_applied","kernel_oom":true,"exit_code":137,"worker_memory_bytes":224*1024*1024}));
        let until = tokio::time::Instant::now()+Duration::from_secs(120);
        loop {
            let state = stale.snapshot();
            if matches!(state.phase,ParticipationPhase::Failed|ParticipationPhase::Stopped) {
                ensure!(!state.package_sharing_active && state.applied_roles.is_none_or(|r|!r.consumer&&!r.catalog&&!r.routing),
                    "OOM left stale participation active");break;
            }
            ensure!(tokio::time::Instant::now()<until,"original worker OOM was not detected");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(stale.network_status().await.is_err() && stale.preflight_fresh_publication(&old_metadata,0).await.is_err(),
            "OOM retained old network/source authority");
        let until = tokio::time::Instant::now()+Duration::from_secs(600);
        let recovered = loop {
            if let Ok(client)=actor.client() && !client.same_owner(&stale) && client.snapshot().phase==ParticipationPhase::Running {break client;}
            ensure!(tokio::time::Instant::now()<until,"normal startup did not recover original owner after OOM");
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        source_ready(&recovered).await?;
        let fresh = recovered.verify_publication(&sources[1].publication).await?;
        ensure!(recovered.preflight_fresh_publication(&fresh,0).await?.release_id()==sources[1].matched.release_id,
            "normal recovery did not restore fresh protected source");
        ensure!(owned_worker(&actor,&worker).await? != original_worker && docker(&["ps","-a","--quiet","--no-trunc","--filter",&format!("id={original_worker}")]).await?.is_empty(),
            "normal recovery retained original OOM worker");
        ensure!(grant(&actor,&sources[0]).await?==original_grant,"OOM recovery changed the original installed grant");
        provider(actor.state()?,&actor.rig.database,user,instance,"1.0.0").await?;
        ensure!(stale.network_status().await.is_err(),"recovery revived stale capability");
        eprintln!("{}",json!({"stage":"worker_oom_recovery_verified","normal_startup_retry":true,"old_capability_closed":true,
            "fresh_source":true,"old_worker_retired":true,"same_installed_instance":true,"installed_grant_unchanged":true,
            "actual_installed_invocation_after_recovery":true,"same_original_identity":true}));
        Ok::<_,anyhow::Error>(())
    })).catch_unwind().await;
    let cleanup = actor.close().await;
    if cleanup.is_ok() {
        eprintln!(
            "{}",
            json!({"stage":"resource_fault_original_identity_retired"})
        );
    }
    cleanup?;
    match operation {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    Ok(())
}
