//! Actual offline native interpreter/private SDK pipe/SQL/AES composition.
//! Protected public-source/admission observations are explicitly synthetic.
//! No public network, installation, activation or provider qualification.
use super::*;
use crate::marketplace::{
    grant_store::tests as grants,
    lease::ControllerLease,
    quarantine::{SourceJobBudget, SourceQuarantineJob, SourceQuarantinePlan, peer_tests::docker},
    security_state,
    worker::{MetadataObservation, PeerPipeFixture},
};
use crate::runtime::{
    RuntimeManager,
    docker::DockerRuntimeManager,
    model::{ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL},
};
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor, evidence::IssuerPolicy, records::SignedRecord,
    source_execution::SourceSdkLimits,
};
use futures_util::FutureExt;
use serde_json::json;
use std::{
    collections::HashMap,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
const MIB: u64 = 1024 * 1024;

mod compiled;
pub(in crate::marketplace) use compiled::{archive_compiled, run_compiled};

struct ReadOnlyFixtures(PathBuf);
impl Drop for ReadOnlyFixtures {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
    }
}

async fn fixture() -> Result<serde_json::Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/sdk/marketplace/test/fixture.mjs");
    let script = r#"import{pathToFileURL}from'node:url';const{createFixture}=await import(pathToFileURL(process.argv[1]));
const source=`import{state,getSecret,fetch}from'@elixir/sdk/v1';
export async function invoke(a){
 if(a.loop){await state.set('started',a.marker);for(;;){}}
 await state.set('cursor',{n:a.n});
 let denied=false,networkDenied=false;
 try{await getSecret('unapproved')}catch{denied=true}
 try{await fetch('https://example.invalid/')}catch{networkDenied=true}
 return {state:await state.get('cursor'),secret:await getSecret('source_api'),denied,networkDenied,blob:a.blob};
}`;
const f=createFixture({operational:true,sourceBytes:Buffer.from(source),transformManifest(m){m.runtime.security={sdk_state_limit_bytes:1048576}}});
process.stdout.write(JSON.stringify(f));"#;
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("node")
            .args(["--input-type=module", "-e", script])
            .arg(path)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() || output.stdout.len() > 128 * 1024 {
        bail!("source SDK fixture generation failed");
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}
pub(in crate::marketplace) fn descriptor(value: &serde_json::Value) -> Result<(CandidateDescriptor, IssuerPolicy)> {
    let mut candidate = CandidateDescriptor::parse(&serde_json::to_vec(&json!({
        "schema_version":1,"publisher":value["publisher"],"release":value["release"],"head":value["head"],"revocations":[],"gate_evidence":value["evidence"]["pairs"]
    }))?)?;
    let now = Utc::now().timestamp() as u64;
    for (index, pair) in candidate.gate_evidence.iter_mut().enumerate() {
        let mut credential = pair.credential.payload.clone();
        credential.issued_at = now;
        credential.expires_at = now + 3600;
        pair.credential = SignedRecord::sign(credential, None, &SigningKey::from_bytes(&[3; 32]))?;
        let mut endorsement = pair.endorsement.payload.clone();
        endorsement.credential_id = Digest::of(pair.credential.to_bytes()?);
        pair.endorsement = SignedRecord::sign(
            endorsement,
            None,
            &SigningKey::from_bytes(&[10 + index as u8; 32]),
        )?;
    }
    Ok((
        candidate,
        IssuerPolicy::new([value["issuer_key"]
            .as_str()
            .context("fixture issuer")?
            .parse()?])?,
    ))
}
fn limits() -> SourceExecutionLimits {
    SourceExecutionLimits {
        profile: Default::default(),
        heap_bytes: 16 * MIB,
        timeout_millis: 30_000,
        max_file_bytes: 4 * MIB,
        json_bytes: MIB,
        pending_jobs: 128,
        sdk: Some(SourceSdkLimits {
            calls: 16,
            request_bytes: MIB,
            response_bytes: MIB,
        }),
    }
}
fn call(
    actor: &Actor,
    checked: &GrantCheckedQuarantine,
    arguments: serde_json::Value,
) -> Result<OwnerInvocation> {
    Ok(OwnerInvocation {
        actor: actor.clone(),
        review_id: checked.requested.review_id()?,
        grant_revision: checked.grant.revision,
        limits: limits(),
        arguments: RuntimeJson::from_value(arguments)?,
    })
}
async fn cleanup(runtime: &dyn RuntimeManager, deployment: &str) -> Result<()> {
    let volume = format!("elixir-marketplace-source-{deployment}");
    for name in [
        format!("elixir-marketplace-source-writer-{deployment}"),
        format!("{volume}-provision"),
    ] {
        if let Some(handle) = runtime.get_container_handle(&name).await? {
            let state = runtime
                .describe_container_runtime_state(&handle.id)
                .await?
                .context("fixture source ownership unavailable")?;
            if state.name != name
                || state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                || state
                    .labels
                    .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    .map(String::as_str)
                    != Some(deployment)
            {
                bail!("refusing foreign fixture source cleanup");
            }
            runtime.stop_container(&handle).await?;
            runtime.remove_container(&handle).await?;
        }
    }
    let labels = HashMap::from([
        (ELIXIR_MANAGED_LABEL.into(), "true".into()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.into()),
        ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
    ]);
    if runtime.private_file_volume_owned(&volume, &labels).await? {
        runtime.remove_private_file_volume(&volume, &labels).await?;
    }
    Ok(())
}
async fn revision(db: &Database, instance: Uuid) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(instance.to_string())
    .fetch_one(&db.pool)
    .await?)
}
async fn wait_revision(db: &Database, instance: Uuid, previous: i64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if revision(db, instance).await? > previous {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("native SDK did not commit its start marker")?
}

#[tokio::test]
#[ignore = "explicit offline native interpreter/actual local SDK database composition; not public source, installation or HTTP"]
async fn native_owner_calls_use_real_scoped_sdk_and_stop_on_revocation_or_cancellation()
-> Result<()> {
    let reference = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable worker image required")?;
    if !crate::marketplace::runtime::image_is_pinned(&reference) {
        bail!("fixture requires an immutable image");
    }
    let image = docker(&["image", "inspect", "--format", "{{.Id}}", &reference]).await?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let root = tempfile::tempdir()?;
    let private = root.path().canonicalize()?.join("source-job");
    let db = grants::connect(&root.path().join("sdk.sqlite")).await?;
    let actor = grants::owner(&db).await?;
    let prefs = String::from_utf8(canonical::to_vec(
        &crate::marketplace::preferences::Preferences::default(),
    )?)?;
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string()).bind(prefs).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&db.pool).await?;
    let value = fixture().await?;
    let (release, package, proof) =
        crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
    // The real offline peer fixture reads its public blobs during startup.
    // Mount only those read-only bytes, never the private database directory.
    let files = root.path().canonicalize()?.join("peer-files");
    std::fs::create_dir(&files)?;
    let _restore = ReadOnlyFixtures(files.clone());
    for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let deployment = format!("source-sdk-{}", Uuid::new_v4().simple());
    let peer_name = format!("elixir-marketplace-sdk-peer-{}", Uuid::new_v4().simple());
    let mut peer = None;
    let result=std::panic::AssertUnwindSafe(async {
        peer=Some(PeerPipeFixture::start(&image,&files,&peer_name,value["issuer_key"].as_str().context("issuer")?,"normal").await.context("offline peer-observation fixture startup")?);
        let peer=peer.as_mut().unwrap();peer.bind_security_database(db.clone());
        let (candidate,policy)=descriptor(&value)?;
        let (metadata,_metadata_owner)=MetadataObservation::fixture_for_verified_descriptor(candidate,&policy)?;
        security_state::admit(&db,&metadata).await?;
        let (source,_source_owner)=PublicSourceObservation::fixture_for_source(release.release_id(),Digest::of(b"SPDX-License-Identifier: MIT\n"));
        let mut selected=Selection::fixture_for_native_preparation(peer.client.clone(),metadata,source,vec![peer.peer.clone()])?;
        let budget=SourceJobBudget {memory_bytes:64*MIB,cpu_millicores:100,disk_bytes:128*MIB,input_bytes:128*MIB,output_bytes:128*MIB,wall_seconds:600};
        let plan=SourceQuarantinePlan::for_release(&release,None,&budget)?;
        let mut job=SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await?;
        for (purpose,bytes) in [(elixir_marketplace::quarantine::QuarantinePurpose::Package,package.as_slice()),(elixir_marketplace::quarantine::QuarantinePurpose::Proof,proof.as_slice())] {
            let mut input=bytes;
            job.receive(runtime.as_ref(),purpose,&mut input).await?;
        }
        let source=job.finish(runtime.as_ref()).await?.match_source(runtime.as_ref()).await?.bind_public_source(selected.public_source()?)?.check_manifest()?;
        let review=source.grant_review()?;
        let grant=approve_for_actor(&db,&actor,&source,&grants::selection(&review,0)?).await?;
        sqlx::query("INSERT INTO extensions (extension_id,name,version,kind,trust_level,manifest_json,enabled) VALUES ($1,'Native SDK fixture','1.0.0','module','community','{}',TRUE)")
            .bind(review.scope().extension_id().as_str()).execute(&db.pool).await?;
        let instance=Uuid::new_v4();let mut checked=grant.check_scope(&db,source,instance).await?;
        checked.initialize_sdk_instance(&db,"native SDK").await?;
        sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1").bind(instance.to_string()).execute(&db.pool).await?;
        let secrets=SecretsManager::from_key_bytes([93;32],false);
        grants::secret(&db,"global",None,"source_api","never-global",&secrets).await?;
        grants::secret(&db,"instance",Some(instance),"source_api","own-native-instance",&secrets).await?;
        let args=json!({"n":0.75,"blob":"x".repeat(256*1024)});
        let invocation=call(&actor,&checked,args)?;
        let result=checked.invoke_for_owner(&mut selected,&db,&secrets,invocation).await?;
        let result:serde_json::Value=serde_json::from_str(result.as_str())?;
        assert_eq!(result["state"],json!({"n":0.75}));assert_eq!(result["secret"],"own-native-instance");
        assert_eq!(result["denied"],true);assert_eq!(result["networkDenied"],true);assert_eq!(result["blob"].as_str().unwrap().len(),256*1024);
        assert!(runtime.get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}")).await?.is_none(),"completed native identity is retired before SDK handoff");
        assert!(ControllerLease::acquire(&private).is_err(),"same source lane remains owned");
        let before=revision(&db,instance).await?;
        let mut short=call(&actor,&checked,json!({"n":9,"blob":""}))?;short.limits.sdk.as_mut().unwrap().response_bytes=3;
        assert!(checked.invoke_for_owner(&mut selected,&db,&secrets,short).await.is_err());
        assert_eq!(revision(&db,instance).await?,before,"reply overflow rolls back the state mutation");
        let mut wrong=call(&actor,&checked,json!({}))?;wrong.grant_revision+=1;
        assert!(checked.invoke_for_owner(&mut selected,&db,&secrets,wrong).await.is_err());
        let before=revision(&db,instance).await?;
        let looping=call(&actor,&checked,json!({"loop":true,"marker":"revocation"}))?;
        let revoke=async {wait_revision(&db,instance,before).await?;sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='native SDK invocation fixture' WHERE id=$1").bind(actor.session.to_string()).execute(&db.pool).await?;Ok::<_,anyhow::Error>(())};
        let (invoked,revoked)=tokio::join!(checked.invoke_for_owner(&mut selected,&db,&secrets,looping),revoke);revoked?;assert!(invoked.is_err());
        assert!(runtime.get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}")).await?.is_none(),"revoked native identity is retired before cleanup completes");
        // Explicit fixture reset, never a production authorization repair.
        sqlx::query("UPDATE account_sessions SET revoked_at=NULL,revoked_reason=NULL WHERE id=$1").bind(actor.session.to_string()).execute(&db.pool).await?;
        let before=revision(&db,instance).await?;
        let looping=call(&actor,&checked,json!({"loop":true,"marker":"cancellation"}))?;
        tokio::select! {
            result=checked.invoke_for_owner(&mut selected,&db,&secrets,looping)=>bail!("loop completed before controlled cancellation: {}",result.is_ok()),
            result=wait_revision(&db,instance,before)=>result?,
        }
        assert!(ControllerLease::acquire(&private).is_err(),"cancelled caller cannot release the lane");
        checked.discard_retained().await?;
        drop(checked);let lane=ControllerLease::acquire(&private)?;drop(lane);
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    let finished = if let Some(peer) = peer {
        peer.finish().await
    } else {
        Ok(())
    };
    // Start may fail before assigning peer. Still inspect and clean only this
    // fixture's exact owned identity; attempt source cleanup even if close fails.
    let removed = crate::marketplace::quarantine::peer_tests::remove_peer(&peer_name).await;
    let source_cleanup = cleanup(runtime.as_ref(), &deployment).await;
    removed?;
    source_cleanup?;
    finished?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
