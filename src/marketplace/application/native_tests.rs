//! Real owned application command / source bytes / native execution / SQL.
//! Selection admission and protection observations are SYNTHETIC, not public
//! source/network/VPN or HTTP/client qualification. No replacement Selection.
use super::*;
use crate::{
    config::DatabaseConfig,
    marketplace::{
        lease::ControllerLease,
        quarantine::{
            SourceJobBudget,
            peer_tests::{check_profile, docker, remove_peer},
        },
        search::Selection,
        security_state,
        service::InstallContext,
        worker::{MetadataObservation, OfflineWorker, PeerPipeFixture, PublicSourceObservation},
    },
    runtime::{RuntimeManager, docker::DockerRuntimeManager},
};
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor,
    evidence::IssuerPolicy,
    records::SignedRecord,
    source_execution::{RuntimeJson, SourceSdkLimits},
};
use futures_util::FutureExt;
use std::{os::unix::fs::PermissionsExt, path::Path, time::Duration};
use tokio::time::{sleep, timeout};
mod automatic;
pub(super) mod http;

const MIB: u64 = 1024 * 1024;
const SOURCE: &str = "import{state,getSecret}from'@elixir/sdk/v1';export async function invoke(){return{cursor:await state.get('cursor'),key:await getSecret('source_api')}}export async function migrate(){await state.set('cursor',(await state.get('cursor')??0)+1)}export async function checkReady(){return(await state.get('cursor'))===1&&(await getSecret('source_api'))==='credential-private'}";
const LIVE_SOURCE: &str = r#"
import{state,getSecret}from'@elixir/sdk/v1';
export async function invoke(input){
 if(input?.capability!=='live.catalog_provider')return{cursor:await state.get('cursor'),key:await getSecret('source_api')};
 if(input.schema_version!==1)throw new Error('contract version');
 let body;const cache={maxAgeSeconds:60,staleWhileRevalidateSeconds:0,etag:'source-v1'};
 const item={id:'source-event',itemType:'event',title:'Synthetic source event',subtitle:null,description:null,status:'live',startsAt:'2026-11-02T00:00:00Z',endsAt:'2026-11-03T00:00:00Z',posterUrl:null,backgroundUrl:null,logoUrl:null,categories:[],badges:[],facts:[]};
 if(input.operation==='health')body={status:'healthy',contractVersions:[1],details:[]};
 else if(input.operation==='catalogs')body={catalogs:[{id:'events',name:'Source Events',description:'Synthetic lawful events',itemTypes:['event'],presentation:'landscape',order:1,filters:[]}],cache:{maxAgeSeconds:60,staleWhileRevalidateSeconds:0,etag:'source-v1'}};
 else if(input.operation==='catalog'&&input.request.request.catalogId==='events')body={items:[item],nextCursor:null,cache};
 else if(input.operation==='meta'&&input.request.request.itemId===item.id)body={item,streams:[{id:'primary',label:'Synthetic source',priority:1,protocolHint:'hls'}],cache};
 else if(input.operation==='resolve'&&input.request.request.itemId===item.id&&input.request.request.streamId==='primary')body={descriptor:{streamId:'primary',label:'Synthetic source',priority:1,protocol:'hls',url:'https://fixture-origin.invalid/live.m3u8',requestHeaders:{},cookies:[],origin:null,referer:null,credentialAuthorities:[],clientDisclosure:'public',expiresAt:null,refreshHandle:null,serverEgress:'not_required',privateNetwork:false,drm:{kind:'none'},timeShift:{available:false,windowSeconds:null}},alternatives:[]};
 else throw new Error('unsupported fixture operation');
 return{schema_version:1,status:200,body};
}
export async function migrate(){await state.set('cursor',(await state.get('cursor')??0)+1)}
export async function checkReady(){return(await state.get('cursor'))===1&&(await getSecret('source_api'))==='credential-private'}
"#;

pub(in crate::marketplace) fn allocation() -> Allocation {
    Allocation {
        installations: InstallBudget {
            entries: 2,
            bytes: 4 * MIB,
        },
        source: SourceJobBudget {
            memory_bytes: 64 * MIB,
            cpu_millicores: 100,
            disk_bytes: 128 * MIB,
            input_bytes: 64 * MIB,
            output_bytes: 64 * MIB,
            wall_seconds: 120,
        },
        retention: ArchiveBudget {
            bytes: 128 * MIB,
            releases: 2,
        },
        execution: SourceExecutionLimits {
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
        },
    }
}
async fn sdk_fixture() -> Result<serde_json::Value> {
    sdk_fixture_profile(false).await
}
async fn sdk_fixture_profile(live: bool) -> Result<serde_json::Value> {
    sdk_fixture_profile_with_cpu(live, 100).await
}
async fn sdk_fixture_profile_with_cpu(
    live: bool,
    cpu_millicores: u32,
) -> Result<serde_json::Value> {
    let cpu_quota = match cpu_millicores {
        100 => "0.1",
        200 => "0.2",
        500 => "0.5",
        _ => bail!("only the baseline, fifth-core and half-core comparison fixtures are supported"),
    };
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/sdk/marketplace/test/fixture.mjs");
    let script = "import{pathToFileURL}from'node:url';const{createFixture}=await import(pathToFileURL(process.argv[1]));const live=process.argv[3]==='true';const cpu=process.argv[4];const f=createFixture({operational:true,mediaTags:live?['live']:['movies'],sourceBytes:Buffer.from(process.argv[2]),transformManifest(m){m.runtime.security={sdk_state_limit_bytes:1048576};if(cpu!=='0.1')m.runtime.security.cpu_quota=cpu;if(live){m.permissions=['live.catalog.read','live.stream.resolve','live.artwork.fetch'];m.runtime.security={...m.runtime.security,run_as_non_root:true,read_only_rootfs:true,no_new_privileges:true,drop_capabilities:['ALL'],prohibit_docker_socket:true,prohibit_host_media_mounts:true};m.provides=[{capability:'live.catalog_provider',slot:'default',cardinality:'many',implementation:'source_fixture',contract_version:1,scope:{actions:['catalog','meta','resolve'],live_item_types:['event'],stream_protocols:['hls']},endpoint:{type:'source-js-v1'}}]}}});const{publisher,release,head,evidence,issuer_key,package_base64,source_proof_base64}=f;process.stdout.write(JSON.stringify({publisher,release,head,evidence,issuer_key,package_base64,source_proof_base64}));";
    let output = timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("node")
            .args(["--input-type=module", "-e", script])
            .arg(sdk)
            .arg(if live { LIVE_SOURCE } else { SOURCE })
            .arg(if live { "true" } else { "false" })
            .arg(cpu_quota)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() || output.stdout.len() > 8 * 1024 * 1024 {
        bail!("application SDK fixture generation failed")
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}
fn descriptor(value: &serde_json::Value) -> Result<(CandidateDescriptor, IssuerPolicy)> {
    let mut descriptor = CandidateDescriptor::parse(&serde_json::to_vec(
        &serde_json::json!({"schema_version":1,"publisher":value["publisher"],"release":value["release"],"head":value["head"],"revocations":[],"gate_evidence":value["evidence"]["pairs"]}),
    )?)?;
    let now = chrono::Utc::now().timestamp() as u64;
    for (index, pair) in descriptor.gate_evidence.iter_mut().enumerate() {
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
        descriptor,
        IssuerPolicy::new([value["issuer_key"]
            .as_str()
            .context("issuer key")?
            .parse()?])?,
    ))
}
async fn database(root: &Path, url: Option<&str>) -> Result<Database> {
    if let Some(url) = url {
        let parsed = reqwest::Url::parse(url)?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "postgres" | "postgresql")
                && matches!(
                    parsed.host_str(),
                    Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
                )
                && parsed.query().is_none()
                && parsed.fragment().is_none()
                && parsed
                    .path()
                    .starts_with("/elixir_marketplace_activation_app_"),
            "NEW dedicated loopback application fixture required"
        );
    }
    let db = Database::connect(&DatabaseConfig {
        url: url
            .map(str::to_owned)
            .unwrap_or_else(|| format!("sqlite://{}", root.join("application.sqlite").display())),
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    if url.is_some() {
        anyhow::ensure!(
            db.driver == crate::db::DatabaseDriver::Postgres,
            "PostgreSQL required"
        );
        let tables:i64=sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')").fetch_one(&db.pool).await?;
        anyhow::ensure!(tables == 0, "fixture database must be empty; no reset");
    }
    db.run_migrations().await?;
    Ok(db)
}
fn rejected_phase(current: Phase, wanted: Phase) -> bool {
    current != wanted
        && (matches!(
            current,
            Phase::Activated | Phase::Cancelled | Phase::Stopped
        ) || (current == Phase::RecoveryRequired && wanted != Phase::Cancelled))
}
#[test]
fn recovery_wait_accepts_the_original_recovery_state_without_accepting_it_as_cancellation() {
    assert!(!rejected_phase(Phase::RecoveryRequired, Phase::Cancelled));
    assert!(rejected_phase(
        Phase::RecoveryRequired,
        Phase::AwaitingSetup
    ));
    assert!(rejected_phase(Phase::Activated, Phase::Cancelled));
    assert!(rejected_phase(Phase::Stopped, Phase::Cancelled));
    assert!(rejected_phase(Phase::Cancelled, Phase::AwaitingSetup));
    assert!(!rejected_phase(Phase::Cancelled, Phase::Cancelled));
}
async fn phase(service: &Service, id: Uuid, wanted: Phase) -> Result<View> {
    timeout(Duration::from_secs(240), async {
        loop {
            let view = service.view(id)?;
            if view.phase == wanted {
                return Ok(view);
            }
            // A queued recovery action need not have executed yet. Its existing
            // nonterminal state is not a NEW failure; only the final Cancelled
            // phase plus the exact durable mutation below can pass this case.
            if rejected_phase(view.phase, wanted) {
                bail!(
                    "owned application reached {:?} while waiting for {:?}",
                    view.phase,
                    wanted
                )
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("application phase deadline")?
}
async fn submit(service: &Service, id: Uuid, make: impl Fn() -> Action) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            match service.submit(id, make()) {
                Ok(_) => return Ok(()),
                Err(error) => {
                    if !error.to_string().contains("slot busy") {
                        return Err(error);
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
    .await
    .context("bounded application action queue deadline")?
}
async fn acknowledged(
    service: &Service,
    id: Uuid,
    request: Uuid,
    wanted: ActionStatus,
) -> Result<ActionReceipt> {
    timeout(Duration::from_secs(240), async {
        loop {
            let view = service.view(id)?;
            let receipt = view
                .latest_action
                .context("original action receipt missing")?;
            anyhow::ensure!(
                receipt.request_id == request,
                "original action receipt was replaced"
            );
            if receipt.status == wanted {
                return Ok(receipt);
            }
            if !matches!(receipt.status, ActionStatus::Queued | ActionStatus::Running) {
                bail!("action reached {:?}, expected {:?}", receipt.status, wanted);
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("application action acknowledgement deadline")?
}
async fn activate_instance(
    service: &Service,
    id: Uuid,
    principal: &CurrentPrincipal,
    database: &Database,
    discarded_commit_ack: bool,
) -> Result<Uuid> {
    let staged = phase(service, id, Phase::AwaitingSetup).await?;
    assert_eq!(staged.revision, Some(1));
    assert!(staged.available_actions.contains(&ActionKind::Setup));
    let setup = Uuid::new_v4();
    let setup_action = || Action::Setup {
        principal: principal.clone(),
        mutation: setup,
        credentials: vec![CredentialChange::set(
            "source_api",
            "credential-private",
            false,
        )],
    };
    submit(service, id, setup_action).await?;
    acknowledged(service, id, setup, ActionStatus::Acknowledged).await?;
    assert_eq!(
        service.submit(id, setup_action())?.status,
        ActionStatus::Acknowledged,
        "lost setup reply cannot encrypt again or launch a second candidate"
    );
    assert!(
        service
            .submit(
                id,
                Action::Setup {
                    principal: principal.clone(),
                    mutation: setup,
                    credentials: vec![CredentialChange::set("source_api", "replacement", false)]
                }
            )
            .unwrap_err()
            .to_string()
            .contains("different input")
    );
    assert!(
        service
            .submit(
                id,
                Action::Setup {
                    principal: principal.clone(),
                    mutation: Uuid::new_v4(),
                    credentials: vec![CredentialChange::set("source_api", "replacement", false)]
                }
            )
            .is_err()
    );
    let ready = phase(service, id, Phase::AwaitingCommit).await?;
    assert!(ready.available_actions.contains(&ActionKind::Activate));
    assert!(!ready.available_actions.contains(&ActionKind::Setup));
    let commit = Uuid::new_v4();
    submit(service, id, || Action::Activate {
        principal: principal.clone(),
        mutation: commit,
    })
    .await?;
    if discarded_commit_ack {
        acknowledged(service, id, commit, ActionStatus::RecoveryRequired).await?;
        let recovery = phase(service, id, Phase::RecoveryRequired).await?;
        assert_eq!(recovery.activation_request_id, Some(commit));
        assert!(recovery.available_actions.contains(&ActionKind::Activate));
        assert!(
            !recovery.available_actions.contains(&ActionKind::Cancel),
            "the ACTUAL committed original result cannot be discarded"
        );
        let before: String =
            sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                .fetch_one(&database.pool)
                .await?;
        assert!(
            service
                .submit(
                    id,
                    Action::Activate {
                        principal: principal.clone(),
                        mutation: Uuid::new_v4()
                    }
                )
                .unwrap_err()
                .to_string()
                .contains("original mutation")
        );
        assert_eq!(
            service.view(id)?.latest_action.unwrap().request_id,
            commit,
            "bad replacement cannot replace the original recovery acknowledgement"
        );
        submit(service, id, || Action::Activate {
            principal: principal.clone(),
            mutation: commit,
        })
        .await?;
        acknowledged(service, id, commit, ActionStatus::Acknowledged).await?;
        let after: String = sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(
            after, before,
            "exact original commit recovery acknowledges without another journal mutation"
        );
    }
    acknowledged(service, id, commit, ActionStatus::Acknowledged).await?;
    let active = phase(service, id, Phase::Activated).await?;
    assert!(active.available_actions.is_empty());
    assert_eq!(
        service
            .submit(
                id,
                Action::Activate {
                    principal: principal.clone(),
                    mutation: commit
                }
            )?
            .status,
        ActionStatus::Acknowledged,
        "terminal retry only acknowledges the same original action"
    );
    active.configured_instance.context("configured instance")
}
async fn configuration_rejections(
    service: &Service,
    client: &ParticipationClient,
    secrets: &SecretsManager,
    principal: &CurrentPrincipal,
    query: Uuid,
    action: &InstallAction,
    database: &Database,
) -> Result<()> {
    for field in 0..14 {
        let mut host = Host::new(client, secrets.clone(), allocation())?;
        match field {
            0 => host.runtime.source_budget.memory_bytes += 1,
            1 => host.runtime.source_budget.cpu_millicores += 1,
            2 => host.runtime.source_budget.disk_bytes += 1,
            3 => host.runtime.source_budget.input_bytes += 1,
            4 => host.runtime.source_budget.output_bytes += 1,
            5 => host.runtime.source_budget.wall_seconds += 1,
            6 => host.runtime.retention.bytes += 1,
            7 => host.runtime.retention.releases += 1,
            8 => host.runtime.lane.push("foreign"),
            9 => host.runtime.archive.push("foreign"),
            10 => host.runtime.image = format!("sha256:{}", "7".repeat(64)),
            11 => host.runtime.runtime = Arc::new(DockerRuntimeManager::new(None)),
            12 => {
                host.runtime.playback = Arc::new(crate::playback::PlaybackJobManager::new(
                    database.pool.clone(),
                    None,
                ))
            }
            13 => host.secrets = SecretsManager::from_key_bytes([58; 32], false),
            _ => unreachable!(),
        }
        // Negative trusted-config DATA only. No replacement Selection or
        // native owner is constructed; the original idle controller is kept.
        let error = service
            .start(
                InstallContext::fixture_empty(client.clone(), query),
                host,
                principal.clone(),
                query,
                action.clone(),
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("controller configuration changed"),
            "field={field}: {error}"
        );
        let state = service.0.lock().unwrap();
        assert!(state.entry.is_none());
        assert!(
            state.completed.is_some(),
            "config rejection must keep the one latest completed status"
        );
        assert!(state.controller.is_some());
    }
    let instances: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(
        instances, 1,
        "configuration rejection happens before target/source/metadata effects"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit offline actual application-owned capture/select/preparation/archive/staging/encrypted setup/native activation/invocation; synthetic admission/protection"]
async fn native_owned_application_installs_and_invokes_without_reconstructing_original_handoffs()
-> Result<()> {
    run(None, false).await
}
#[tokio::test]
#[ignore = "explicit offline actual zero-paid direct owner acceptance/source/archive/staging/setup/native activation/invocation; synthetic signature/public-source/protection receipts"]
async fn native_direct_application_installs_and_invokes_without_paid_discovery_proofs() -> Result<()>
{
    run_source(None, false, true).await
}
#[tokio::test]
#[ignore = "explicit pinned worker and original native quarantine/activation; SDK stream upload, synthetic publication/public-source/protection owners"]
async fn native_sdk_upload_installs_and_invokes_without_peer_blob_downloads() -> Result<()> {
    run_input(None, false, true, true).await
}
#[tokio::test]
#[ignore = "explicit pinned worker and actual mid-input source-owner withdrawal/cleanup; synthetic public-source/protection observations"]
async fn native_source_owner_withdrawal_stops_incomplete_input_and_retires_its_original_job()
-> Result<()> {
    run_with_withdrawal(None, false, true, true, true).await
}
#[tokio::test]
#[ignore = "explicit NEW empty loopback PostgreSQL and whole actual owned application install/invocation; no public-network qualification"]
async fn postgres_native_owned_application_installs_and_invokes_without_reconstruction()
-> Result<()> {
    run(
        Some(&std::env::var(
            "ELIXIR_TEST_MARKETPLACE_APPLICATION_INSTALL_POSTGRES_URL",
        )?),
        false,
    )
    .await
}
#[tokio::test]
#[ignore = "explicit actual application task with deliberately discarded real staging receipt, source loss/revocation and fresh-owner offline cancellation; not TCP-loss injection"]
async fn native_owned_application_recovers_a_lost_staging_ack_and_cancels_offline() -> Result<()> {
    run(None, true).await
}
#[tokio::test]
#[ignore = "explicit NEW empty loopback PostgreSQL and actual owned application lost staging acknowledgement/offline cancellation"]
async fn postgres_native_owned_application_recovers_staging_ack_and_cancels_offline() -> Result<()>
{
    run(
        Some(&std::env::var(
            "ELIXIR_TEST_MARKETPLACE_APPLICATION_ACK_POSTGRES_URL",
        )?),
        true,
    )
    .await
}

async fn run(postgres: Option<&str>, lose_ack: bool) -> Result<()> {
    run_source(postgres, lose_ack, false).await
}
async fn run_source(postgres: Option<&str>, lose_ack: bool, direct: bool) -> Result<()> {
    run_input(postgres, lose_ack, direct, false).await
}
async fn run_input(
    postgres: Option<&str>,
    lose_ack: bool,
    direct: bool,
    upload: bool,
) -> Result<()> {
    run_with_withdrawal(postgres, lose_ack, direct, upload, false).await
}
async fn run_with_withdrawal(
    postgres: Option<&str>,
    lose_ack: bool,
    direct: bool,
    upload: bool,
    withdraw: bool,
) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable image required")?;
    let image = docker(&["image", "inspect", "--format", "{{.Id}}", &image]).await?;
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?;
    let lane = directory.join("foreground");
    let files = directory.join("source");
    std::fs::create_dir(&files)?;
    let value = sdk_fixture().await?;
    let (release, package, proof) =
        crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
    for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
        let file = files.join(name);
        std::fs::write(&file, bytes)?;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let db = database(&directory, postgres).await?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let service = Service::default();
    let mut peer = None;
    let mut host_worker = None;
    let name = format!("elixir-marketplace-app-peer-{}", Uuid::new_v4().simple());
    let host_deployment = format!("app-host-{}", Uuid::new_v4().simple());
    let outcome=std::panic::AssertUnwindSafe(Box::pin(async {
        let issuer=value["issuer_key"].as_str().context("fixture issuer")?;
        peer=Some(PeerPipeFixture::start(&image,&files,&name,issuer,"providers").await?);
        check_profile(&name).await?;
        host_worker=Some(OfflineWorker::start(runtime.as_ref(),&directory.join("admission-host"),&host_deployment,&image,&[issuer.parse()?],crate::marketplace::packet::PacketWorkloadBudget::split(&elixir_marketplace::policy::OrdinaryLimits::default())?.worker_limits()).await?);
        let actor=grant_store::tests::owner(&db).await?;let mut principal=actor.fixture_principal();
        let preferences=String::from_utf8(canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?;
        sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1").bind(actor.home.to_string()).bind(preferences).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&db.pool).await?;
        let peer_ref=peer.as_mut().unwrap();peer_ref.bind_security_database(db.clone());
        // Bind the ACTUAL source resource owner BEFORE making the original
        // Selection. Never patch a clone after archive or reconstruct one.
        peer_ref.bind_offline_source_owner(host_worker.as_ref().unwrap(),runtime.clone(),&lane)?;
        let (source,_source_owner)=PublicSourceObservation::fixture_for_source(release.release_id(),Digest::of(b"SPDX-License-Identifier: MIT\n"));
        let client=peer_ref.client.clone();
        let publication=elixir_marketplace::publication::PreparedPublication::parse(&serde_json::to_vec(&serde_json::json!({
            "schema_version":1,"publisher":value["publisher"],"release":value["release"],"head":value["head"],
        }))?)?;
        let reference=Digest::of(publication.to_bytes()?);
        let mut _metadata_owner:Option<Box<dyn Fn(bool)+Send+Sync>>=None;
        let mut _publication_owner:Option<Box<dyn Fn(bool)+Send+Sync>>=None;
        let selected=if direct {
            let (observation,owner)=crate::marketplace::worker::PublicationObservation::fixture_for_verified_publication(publication.clone())?;
            _publication_owner=Some(Box::new(owner));
            let accepted=crate::marketplace::selected_source::AcceptedPublication::capture(&db,observation,&principal,reference).await?;
            assert!(crate::marketplace::selected_source::SelectedSource::discovery(&accepted).is_none());
            let mutation=security_state::catalog_accept_publication(&db,&accepted,0).await?;
            assert!(mutation.tracked());assert_eq!(mutation.revision(),1);
            let selected=Selection::fixture_for_native_direct(client.clone(),accepted,source)?;
            assert!(selected.discovery_metadata().is_err(),"zero-paid owner consent is never an automatic-update discovery receipt");
            selected
        }else {
            let (descriptor,policy)=descriptor(&value)?;
            let (metadata,owner)=MetadataObservation::fixture_for_verified_descriptor(descriptor,&policy)?;
            _metadata_owner=Some(Box::new(owner));
            security_state::admit(&db,&metadata).await?;
            Selection::fixture_for_native_preparation(client.clone(),metadata,source,vec![peer_ref.peer.clone()])?
        };
        let query=Uuid::new_v4();let context=InstallContext::fixture_selected(client.clone(),query,selected,lose_ack);
        let context=if direct {context.with_direct(principal.clone(),publication.clone(),reference,query)}else{context};
        let context=if upload {context.with_upload()?} else {context};
        let context=if lose_ack {context} else {context.fixture_discard_activation_ack()};
        let preparation=context.preparation.clone();
        let secrets=SecretsManager::from_key_bytes([57;32],false);
        let host=Host::new(&client,secrets.clone(),allocation())?;
        let action=InstallAction{extension:release.payload().extension_id.clone(),release:release.release_id(),target:installation::InstallTarget::New{name:"Owned application".into()},expected_revision:0,mutation:Uuid::new_v4(),controls:installation::ExpectedControls::Absent{}};
        let retry=action.clone();
        let id=if direct {service.start_direct(context,host,principal.clone(),query,action,reference)?}
            else{service.start(context,host,principal.clone(),query,action)?};
        let duplicate_context=InstallContext::fixture_empty(client.clone(),query);
        let duplicate=if direct {let context=duplicate_context.with_direct(principal.clone(),publication.clone(),reference,query);let context=if upload {context.with_upload()?}else{context};service.start_direct(context,Host::new(&client,secrets.clone(),allocation())?,principal.clone(),query,retry.clone(),reference)?}
            else{service.start(duplicate_context,Host::new(&client,secrets.clone(),allocation())?,principal.clone(),query,retry.clone())?};
        assert_eq!(duplicate,id,"lost start reply cannot create a replacement task");
        if direct {
            assert_eq!(service.acknowledge_source_start(&principal,query,&retry,reference,upload)?,Some(id));
            if upload {assert!(service.acknowledge_direct_start(&principal,query,&retry,reference)?.is_none());}
            assert!(service.acknowledge_start(&principal,query,&retry)?.is_none());
            assert!(service.acknowledge_direct_start(&principal,query,&retry,Digest::of(b"other reference"))?.is_none());
            assert!(service.view(id)?.initial_request.direct_reference.is_some());
        }
        if upload {
            use tokio::io::AsyncWriteExt;
            let mut supplied=std::collections::BTreeSet::new();
            let deadline=tokio::time::Instant::now()+std::time::Duration::from_secs(180);
            while supplied.len()<2 {
                anyhow::ensure!(tokio::time::Instant::now()<deadline,"original SDK upload did not become ready");
                let view=service.view(id)?;
                anyhow::ensure!(!matches!(view.phase,Phase::RecoveryRequired|Phase::Cancelled|Phase::Stopped),"original upload failed before review");
                if let Some(view)=view.preparation.and_then(|p|p.upload).filter(|u|!u.claimed) {
                    if supplied.is_empty() {
                        // Normal account refresh rotates CSRF after this
                        // original job was accepted. Source input continues;
                        // later permission/setup/activation use fresh access.
                        sqlx::query("UPDATE account_sessions SET csrf_revision=csrf_revision+1 WHERE id=$1")
                            .bind(principal.account_session_id.to_string()).execute(&db.pool).await?;
                        principal.csrf_revision+=1;
                    }
                    assert!(supplied.insert(view.purpose.name().to_string()));
                    let bytes=match view.purpose {elixir_marketplace::quarantine::QuarantinePurpose::Package=>&package,elixir_marketplace::quarantine::QuarantinePurpose::Proof=>&proof,_=>bail!("unexpected upload artifact")};
                    assert_eq!(view.blob.sha256,Digest::of(bytes));assert_eq!(view.blob.size,bytes.len() as u64);
                    let current=service.view(id)?.preparation.context("original upload preparation")?;
                    let mut claim=preparation.claim_upload(current.preparation_id,view.upload_id,view.blob.size)?;
                    assert!(preparation.claim_upload(current.preparation_id,view.upload_id,view.blob.size).is_err());
                    if withdraw {
                        assert_eq!(supplied.len(),1);
                        assert!(view.blob.size>1);
                        // Leave an actual signed native input incomplete, not
                        // a completed download followed by a late status edit.
                        claim.writer.write_all(&bytes[..1]).await?;
                        assert!(ControllerLease::acquire(&lane).is_err());
                        sqlx::query("UPDATE account_sessions SET revoked_at=$1,revoked_reason='logout' WHERE id=$2")
                            .bind(chrono::Utc::now().to_rfc3339()).bind(principal.account_session_id.to_string()).execute(&db.pool).await?;
                        let failed=phase(&service,id,Phase::RecoveryRequired).await?;
                        assert!(failed.preparation.context("original withdrawn preparation")?.authorization_lost,
                            "native idle timeout alone must not masquerade as owner monitoring");
                        service.shutdown().await?;
                        assert!(!claim.completion.await.unwrap_or(false));
                        assert!(claim.writer.write_all(b"late").await.is_err());
                        let lease=ControllerLease::acquire(&lane)?;drop(lease);
                        let rows:i64=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM marketplace_extension_grants)+(SELECT COUNT(*) FROM marketplace_installations)+(SELECT COUNT(*) FROM marketplace_sdk_namespaces)")
                            .fetch_one(&db.pool).await?;
                        assert_eq!(rows,0,"withdrawal cannot grant, archive-stage, execute or allocate installed state");
                        eprintln!("actual mid-input logout stopped the original source workflow and joined native cleanup without a completed body or grant");
                        return Ok(());
                    }
                    for chunk in bytes.chunks(64*1024) {claim.writer.write_all(chunk).await?;}
                    claim.writer.shutdown().await?;
                    assert!(tokio::time::timeout(std::time::Duration::from_secs(40),claim.completion).await??);
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert_eq!(service.view(id)?.initial_request.upload_source,Some(true));
        }
        let ready=phase(&service,id,Phase::AwaitingApproval).await?;
        let review=ready.review.context("original source review")?;
        assert!(!ready.installation_ready);assert_eq!(review.release_id(),release.release_id());
        assert!(ControllerLease::acquire(&lane).is_err(),"original native source is still owned at review");
        let before:i64=sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances").fetch_one(&db.pool).await?;assert_eq!(before,0);
        let approval=Uuid::new_v4();let review_id=review.review_id()?;
        submit(&service,id,||Action::Approve{principal:principal.clone(),selection:ApprovalSelection{review_id,expected_revision:0,mutation_id:approval,automatic_updates:false}}).await?;
        acknowledged(&service,id,approval,ActionStatus::Acknowledged).await?;
        assert_eq!(service.submit(id,Action::Approve{principal:principal.clone(),selection:ApprovalSelection{review_id,expected_revision:0,mutation_id:approval,automatic_updates:false}})?.status,ActionStatus::Acknowledged);
        if lose_ack {
            phase(&service,id,Phase::RecoveryRequired).await?;
            let pending:String=sqlx::query_scalar("SELECT record_json FROM marketplace_installations").fetch_one(&db.pool).await?;
            let pending_before=pending.clone();
            let pending:serde_json::Value=serde_json::from_str(&pending)?;assert_eq!(pending["revision"],1);assert!(pending["pending"].is_object());
            grant_store::revoke(&db,&principal,&release.payload().extension_id,1,Uuid::new_v4(),chrono::Utc::now()).await?;
            sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1").bind(principal.profile_id.to_string()).execute(&db.pool).await?;
            let revision:String=sqlx::query_scalar("SELECT CAST(revision AS TEXT) FROM profile_authorization_revisions WHERE profile_id=$1").bind(principal.profile_id.to_string()).fetch_one(&db.pool).await?;
            let mut capabilities=crate::authz::CapabilitySet::all();capabilities.remove(crate::authz::Capability::SecretsManage);
            let fresh=CurrentPrincipal::fixture_owner(principal.user_id,principal.home_id,principal.profile_id,principal.account_session_id,revision.parse()?,principal.access_expires_at.min(principal.session_expires_at),capabilities);
            peer.take().unwrap().finish().await?;
            let mutation=Uuid::new_v4();submit(&service,id,||Action::Cancel{principal:principal.clone(),mutation}).await?;
            acknowledged(&service,id,mutation,ActionStatus::Rejected).await?;
            assert_eq!(service.view(id)?.phase,Phase::RecoveryRequired);
            let still_pending:String=sqlx::query_scalar("SELECT record_json FROM marketplace_installations").fetch_one(&db.pool).await?;
            assert_eq!(still_pending,pending_before,"stale action cannot touch the original journal");
            submit(&service,id,||Action::Cancel{principal:fresh.clone(),mutation}).await?;
            acknowledged(&service,id,mutation,ActionStatus::Acknowledged).await?;
            let done=phase(&service,id,Phase::Cancelled).await?;assert_eq!(done.revision,Some(2));
            let record:String=sqlx::query_scalar("SELECT record_json FROM marketplace_installations").fetch_one(&db.pool).await?;
            let record:serde_json::Value=serde_json::from_str(&record)?;assert_eq!(record["mutation"],serde_json::json!(mutation),"only the fresh owner may cancel");
            let private:i64=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_generation_secrets)").fetch_one(&db.pool).await?;assert_eq!(private,0);
            eprintln!("owned app retained an actual committed staging input after dropped acknowledgement, then fresh owner without SecretsManage cancelled offline after grant/source loss");
        } else {
            let owner=activate_instance(&service,id,&principal,&db,true).await?;
            if direct {
                let context=InstallContext::fixture_empty(client.clone(),query).with_direct(principal.clone(),publication,reference,query);
                let context=if upload {context.with_upload()?}else{context};
                assert_eq!(service.start_direct(context,Host::new(&client,secrets.clone(),allocation())?,
                    principal.clone(),query,retry,reference)?,id,"direct terminal retry retains the exact original command");
                assert!(owner != Uuid::nil());
                let record:String=sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                    .fetch_one(&db.pool).await?;
                let record:serde_json::Value=serde_json::from_str(&record)?;
                assert!(record["active"].is_object());
                assert!(record["active"]["authority"].get("gate_evidence").is_none());
                assert_eq!(service.view(id)?.initial_request.direct_reference,Some(reference));
                eprintln!("zero-paid direct source passed actual preparation/archive/grant/staging/setup/native migration/readiness/commit/invocation and lost commit ACK recovery; no discovery or auto-sharing proof");
                return Ok(());
            }
            assert_eq!(service.start(InstallContext::fixture_empty(client.clone(),query),Host::new(&client,secrets.clone(),allocation())?,principal.clone(),query,retry)?,id,"terminal start retry acknowledges SAME original operation");
            let entry=service.entry(id)?;entry.task.lock().await.join().await?;
            service.reap_finished()?;
            let parent_revision:i64=sqlx::query_scalar("SELECT revision FROM marketplace_extension_choices WHERE extension_id=$1").bind(release.payload().extension_id.as_str()).fetch_one(&db.pool).await?;
            let next_query=Uuid::new_v4();
            let next_action=InstallAction{extension:release.payload().extension_id.clone(),release:release.release_id(),target:installation::InstallTarget::New{name:"Second owned instance".into()},expected_revision:0,mutation:Uuid::new_v4(),controls:installation::ExpectedControls::NewInstance{extension_revision:parent_revision}};
            configuration_rejections(&service,&client,&secrets,&principal,next_query,&next_action,&db).await?;
            let (next_descriptor,next_policy)=self::descriptor(&value)?;
            let (next_metadata,_next_metadata_owner)=MetadataObservation::fixture_for_verified_descriptor(next_descriptor,&next_policy)?;
            security_state::admit(&db,&next_metadata).await?;
            let (next_source,_next_source_owner)=PublicSourceObservation::fixture_for_source(release.release_id(),Digest::of(b"SPDX-License-Identifier: MIT\n"));
            let next_selected=Selection::fixture_for_native_preparation(client.clone(),next_metadata,next_source,vec![peer.as_ref().unwrap().peer.clone()])?;
            let next_context=InstallContext::fixture_selected(client.clone(),next_query,next_selected,false);
            let next=service.start(next_context,Host::new(&client,secrets.clone(),allocation())?,principal.clone(),next_query,next_action)?;
            assert_ne!(next,id);
            let next_review=phase(&service,next,Phase::AwaitingApproval).await?.review.context("second original source review")?;
            let grant_revision=grant_store::load(&db,&release.payload().extension_id).await?.context("current grant")?.revision();
            let next_approval=Uuid::new_v4();let next_review_id=next_review.review_id()?;
            submit(&service,next,||Action::Approve{principal:principal.clone(),selection:ApprovalSelection{review_id:next_review_id,expected_revision:grant_revision,mutation_id:next_approval,automatic_updates:false}}).await?;
            acknowledged(&service,next,next_approval,ActionStatus::Acknowledged).await?;
            let next_owner=activate_instance(&service,next,&principal,&db,false).await?;
            assert_ne!(next_owner,owner,"second command must allocate an independent configured owner");
            service.entry(next)?.task.lock().await.join().await?;
            service.reap_finished()?;
            let mut controller=service.0.lock().unwrap().controller.take().context("original idle controller returned after actual join")?;
            peer.take().unwrap().finish().await?;
            for configured in [owner,next_owner] {
                let binding=installation::standing::InstalledBinding::load_configured(&db,actor.home,&release.payload().extension_id,configured,allocation().installations).await?;
                controller.begin(binding,allocation().execution,RuntimeJson::from_value(serde_json::json!({}))?)?;
                let value=controller.finish().await?;let result:serde_json::Value=serde_json::from_str(value.as_str())?;
                assert_eq!(result,serde_json::json!({"cursor":1,"key":"credential-private"}));
            }
            controller.cancel().await?;service.0.lock().unwrap().controller=Some(controller);
            eprintln!("two whole owned app commands reused the SAME original controller, rejected fourteen config drifts before effects, deduped exact actions without credential replacement, and invoked independent namespaces offline");
        }
        service.shutdown().await?;
        assert!(ControllerLease::acquire(&lane).is_ok(),"successful app completion must release its actual source lane");
        Ok::<_,anyhow::Error>(())
    })).catch_unwind().await;
    let stopped = service.shutdown().await;
    let peer_stopped = match peer.take() {
        Some(peer) => peer.finish().await,
        None => Ok(()),
    };
    let host_stopped_and_removed = match host_worker.take() {
        Some(worker) => match worker.stop(runtime.as_ref()).await {
            Ok(()) => {
                crate::marketplace::preparation::remove_native_activation_host(
                    runtime.as_ref(),
                    &host_deployment,
                )
                .await
            }
            // Preserve resources after an uncertain original stop, not a
            // fallback name-based removal that could conceal failed cleanup.
            Err(error) => Err(error),
        },
        None => Ok(()),
    };
    let peer_removed = remove_peer(&name).await;
    db.pool.close().await;
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))?;
    let preserve = outcome.as_ref().is_err()
        || outcome.as_ref().is_ok_and(|result| result.is_err())
        || stopped.is_err()
        || peer_stopped.is_err()
        || host_stopped_and_removed.is_err()
        || peer_removed.is_err();
    if preserve {
        eprintln!(
            "failed owned-app fixture preserved at {}",
            root.keep().display()
        );
    }
    stopped?;
    peer_stopped?;
    host_stopped_and_removed?;
    peer_removed?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
