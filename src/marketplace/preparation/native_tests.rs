//! Actual Live actor + offline Noise/private IPC/native quarantine. Admission
//! and source/protection observations are synthetic, backed by local signatures
//! and a real security database. NOT public-source/VPN/credit/install evidence.
use super::*;
use crate::{
    config::DatabaseConfig,
    db::Database,
    marketplace::{
        lease::ControllerLease,
        quarantine::{
            SourceQuarantinePlan,
            peer_tests::{check_profile, counters, docker, remove_peer},
        },
        security_state,
        worker::{MetadataObservation, PeerPipeFixture, PublicSourceObservation},
    },
    runtime::{
        RuntimeManager,
        docker::DockerRuntimeManager,
        model::{ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL},
    },
};
use ed25519_dalek::SigningKey;
use elixir_marketplace::{
    admission::CandidateDescriptor,
    evidence::IssuerPolicy,
    records::{Revocation, SignedRecord},
    security_job::SecurityRecords,
};
use futures_util::FutureExt;
use std::{collections::HashMap, os::unix::fs::PermissionsExt, path::Path};
use tokio::time::timeout;

mod configured;

async fn sdk_fixture(configuring: bool) -> Result<serde_json::Value> {
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/sdk/marketplace/test/fixture.mjs");
    let script = "import{pathToFileURL}from'node:url';const{createFixture}=await import(pathToFileURL(process.argv[1]));const source=process.argv[2]||'export const version=1;export function invoke(){throw Error(\"normal actions must not run during probe\")}\\n/*'+'x'.repeat(2*1024*1024)+'*/\\n';const f=createFixture({operational:true,sourceBytes:Buffer.from(source),transformManifest(m){if(process.argv[2])m.runtime.security={sdk_state_limit_bytes:1048576}}});const{publisher,release,head,evidence,issuer_key,package_base64,source_proof_base64}=f;process.stdout.write(JSON.stringify({publisher,release,head,evidence,issuer_key,package_base64,source_proof_base64}));";
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("node")
            .args(["--input-type=module", "-e", script])
            .arg(sdk)
            .arg(if configuring { configured::SOURCE } else { "" })
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() || output.stdout.len() > 8 * 1024 * 1024 {
        bail!("native preparation SDK fixture generation failed");
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}
fn descriptor(value: &serde_json::Value) -> Result<(CandidateDescriptor, IssuerPolicy)> {
    let mut descriptor = CandidateDescriptor::parse(&serde_json::to_vec(&serde_json::json!({
        "schema_version":1,"publisher":value["publisher"],"release":value["release"],"head":value["head"],
        "revocations":[],"gate_evidence":value["evidence"]["pairs"]
    }))?)?;
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
            .context("fixture issuer missing")?
            .parse()?])?,
    ))
}
async fn remove_source(runtime: &dyn RuntimeManager, deployment: &str) -> Result<()> {
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
            if state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                || state
                    .labels
                    .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    .map(String::as_str)
                    != Some(deployment)
            {
                bail!("refusing foreign native preparation source cleanup");
            }
            let writer = name.starts_with("elixir-marketplace-source-writer-");
            let role = if writer {
                "elixir.marketplace.isolation"
            } else {
                "elixir.marketplace.storage_job"
            };
            if state.labels.get(role).map(String::as_str) != Some("quarantine-v1")
                || (!writer && state.labels.get("elixir.marketplace.storage_name") != Some(&volume))
            {
                bail!("refusing native preparation cleanup for a different container role");
            }
            runtime.stop_container(&handle).await?;
            runtime.remove_container(&handle).await?;
        }
    }
    let labels = source_labels(deployment);
    if runtime.private_file_volume_owned(&volume, &labels).await? {
        runtime.remove_private_file_volume(&volume, &labels).await?;
    }
    Ok(())
}
fn source_labels(deployment: &str) -> HashMap<String, String> {
    HashMap::from([
        (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.to_owned()),
        (
            "elixir.marketplace.volume".to_owned(),
            "quarantine-v1".to_owned(),
        ),
    ])
}
async fn phase(service: &Service, id: Uuid, expected: Phase) -> Result<View> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let view = service.view(id)?;
            if view.phase == expected {
                return Ok(view);
            }
            if matches!(
                view.phase,
                Phase::Failed | Phase::CleanupFailed | Phase::Cancelled
            ) {
                bail!(
                    "native preparation stopped at {:?}, expected {:?}",
                    view.phase,
                    expected
                );
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("native preparation phase deadline")?
}
async fn received_prefix(runtime: &dyn RuntimeManager, deployment: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let name = format!("elixir-marketplace-source-writer-{deployment}");
        loop {
            if let Some(handle) = runtime.get_container_handle(&name).await? {
                let state = runtime
                    .describe_container_runtime_state(&handle.id)
                    .await?
                    .context("fixture source profile missing")?;
                if state
                    .labels
                    .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    .map(String::as_str)
                    != Some(deployment)
                    || state
                        .labels
                        .get("elixir.marketplace.isolation")
                        .map(String::as_str)
                        != Some("quarantine-v1")
                    || state.network_mode.as_deref() != Some("none")
                {
                    bail!("native fixture profile differs");
                }
                let argv = [
                    "/usr/bin/od",
                    "-An",
                    "-N3",
                    "-tx1",
                    "/quarantine/package.blob",
                ]
                .map(str::to_owned);
                if let Ok(prefix) = runtime
                    .exec_container_command(&handle, "10004:10004", &argv)
                    .await
                    && prefix.split_whitespace().collect::<String>() == "010101"
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("native fixture body prefix deadline")?
}

async fn archive_prefix(path: &Path, service: &Service, id: Uuid) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if std::fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0) {
                return Ok(());
            }
            let phase = service.view(id)?.phase;
            if matches!(
                phase,
                Phase::SourceArchived | Phase::Failed | Phase::CleanupFailed | Phase::Cancelled
            ) {
                bail!("archive stopped before partial-copy cancellation point: {phase:?}");
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("archive prefix deadline")?
}

async fn claimed_source_handoff(
    launch: &mut crate::marketplace::grant_store::installation::activation::SourceLaunch,
    lane: &Path,
    expected: &elixir_marketplace::source_job::SourceMatchSummary,
) -> Result<()> {
    let mut source = launch.finish().await?;
    let result = async {
        source.current().await?;
        if elixir_marketplace::canonical::to_vec(source.summary()?)?
            != elixir_marketplace::canonical::to_vec(expected)?
        {
            bail!("native restored source differs from retained source");
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    assert!(
        launch.cancel().await.is_err(),
        "old owner cannot cancel a transferred source"
    );
    assert!(ControllerLease::acquire(lane).is_err());
    // Lost/unconsumed handoff: its guard must own actual native disposal.
    drop(source);
    timeout(Duration::from_secs(45), async {
        loop {
            if let Ok(lease) = ControllerLease::acquire(lane) {
                drop(lease);
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("dropped source handoff did not join native disposal")?;
    result
}
async fn probed_source_handoff(
    launch: &mut crate::marketplace::grant_store::installation::activation::SourceLaunch,
    lane: &Path,
    expected: &elixir_marketplace::source_job::SourceMatchSummary,
) -> Result<()> {
    let source = launch.finish().await?;
    let mut probed = source.probe().await?;
    probed.current().await?;
    assert_eq!(
        elixir_marketplace::canonical::to_vec(probed.summary()?)?,
        elixir_marketplace::canonical::to_vec(expected)?
    );
    assert!(
        launch.cancel().await.is_err(),
        "old launch cannot cancel a transferred source"
    );
    assert!(
        ControllerLease::acquire(lane).is_err(),
        "probe retains actual source ownership"
    );
    probed.cancel().await?;
    assert!(
        probed.current().await.is_err(),
        "disposed source cannot report ready/current"
    );
    assert!(probed.summary().is_err());
    assert!(ControllerLease::acquire(lane).is_ok());
    Ok(())
}
async fn retained_probe_handoff(
    launch: &mut crate::marketplace::grant_store::installation::activation::SourceLaunch,
    lane: &Path,
    database: &Database,
    revoked: bool,
) -> Result<()> {
    let source = launch.finish().await?;
    let probed = source.probe().await?;
    assert!(ControllerLease::acquire(lane).is_err());
    if revoked {
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,revision=revision+1,revocation_revision=revision+1")
            .execute(&database.pool).await?;
        assert!(
            probed.retain().await.is_err(),
            "a completed probe cannot bypass fresh grant checks"
        );
        // The failed consuming handoff transfers cleanup to the same retained
        // disposal owner. Wait for actual completion, never abort its task.
        timeout(Duration::from_secs(60), async {
            loop {
                if ControllerLease::acquire(lane).is_ok() {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("failed source preparation did not join disposal")?;
    } else {
        let reference = probed.retain().await?;
        assert_eq!(reference.reference().revision(), 4);
        assert!(
            ControllerLease::acquire(lane).is_ok(),
            "retention must release the foreground lane"
        );
    }
    let json: String = sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
        .fetch_one(&database.pool)
        .await?;
    let record: serde_json::Value = serde_json::from_str(&json)?;
    assert_eq!(
        record["activation"]["phase"],
        if revoked {
            "starting"
        } else {
            "source_prepared"
        }
    );
    assert_eq!(record["revision"], if revoked { 3 } else { 4 });
    assert!(
        launch.cancel().await.is_err(),
        "old launch no longer owns the handoff"
    );
    Ok(())
}

pub(in crate::marketplace) async fn remove_activation_host(
    runtime: &dyn RuntimeManager,
    deployment: &str,
) -> Result<()> {
    let volume = format!("elixir-marketplace-{deployment}");
    for name in [
        format!("elixir-marketplace-worker-{deployment}"),
        format!("{volume}-provision"),
    ] {
        if let Some(handle) = runtime.get_container_handle(&name).await? {
            let state = runtime
                .describe_container_runtime_state(&handle.id)
                .await?
                .context("activation host fixture ownership unavailable")?;
            if state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                || state
                    .labels
                    .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    .map(String::as_str)
                    != Some(deployment)
            {
                bail!("refusing unowned activation host fixture cleanup");
            }
            runtime.remove_container(&handle).await?;
        }
    }
    runtime
        .remove_private_file_volume(
            &volume,
            &HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.to_owned()),
                (
                    "elixir.marketplace.volume".to_owned(),
                    "fixed-v1".to_owned(),
                ),
            ]),
        )
        .await
}

#[tokio::test]
#[ignore = "explicit isolated offline Live actor/Noise/native verifier/archive/disabled-staging composition; not VPN or activation"]
async fn live_preparation_checks_real_bytes_and_joins_cancelled_or_withdrawn_transfers()
-> Result<()> {
    live_preparation_cases(None, None).await
}
#[tokio::test]
#[ignore = "explicit actual packaged worker/Noise/DHT/native quarantine; empty and stale hints fall back to selected catalogs, synthetic admission/protection"]
async fn live_preparation_uses_known_catalogs_when_content_hints_are_empty_or_stale() -> Result<()>
{
    for case in ["fallback_empty", "fallback_stale"] {
        Box::pin(live_preparation_cases(Some(case), None)).await?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "explicit isolated offline native source-start cancellation with actual host resource admission"]
async fn live_source_start_cancellation_joins_its_actual_native_owner() -> Result<()> {
    live_preparation_cases(Some("retain_start_cancel"), None).await
}
#[tokio::test]
#[ignore = "explicit offline source_probe=1 real claim/restore/native no-effects probe/disposal composition"]
async fn live_source_load_probe_preserves_the_exact_owned_candidate_without_sdk_effects()
-> Result<()> {
    live_preparation_cases(Some("retain_probe"), None).await
}
#[tokio::test]
#[ignore = "explicit offline source_probe=1 actual native probe/disposal/archive retention and durable source-prepared transition"]
async fn live_probed_source_preparation_releases_its_lane_and_rechecks_revocation() -> Result<()> {
    live_preparation_cases(Some("retain_prepared"), None).await?;
    live_preparation_cases(Some("retain_prepared_revoke"), None).await
}
#[tokio::test]
#[ignore = "explicit offline actual peer/archive/claim/restore/probe/configured credential setup/candidate/commit composition; synthetic discovery/protection"]
async fn live_probed_source_continues_into_configured_activation_and_rechecks_owner() -> Result<()>
{
    live_preparation_cases(Some("retain_configured_activate"), None).await?;
    live_preparation_cases(Some("retain_configured_authority"), None).await
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL and actual offline peer/source-probe/configured candidate activation"]
async fn postgres_live_probed_source_continues_into_configured_activation() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_PROBED_ACTIVATION_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_activate"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit offline real-probe cancellation with original disposed context, offline peer, reserved writer and stable configured data"]
async fn live_probed_source_cancellation_is_offline_exact_and_preserves_configuration() -> Result<()>
{
    live_preparation_cases(Some("retain_configured_cancel"), None).await
}
#[tokio::test]
#[ignore = "explicit real peer/source/archive/cleanup and cancelled handoff waiter; ORIGINAL archive/selection pair stages once, no status reconstruction"]
async fn live_owned_archive_handoff_retains_the_original_pair_and_stages_without_reconstruction()
-> Result<()> {
    live_preparation_cases(Some("retain_handoff"), None).await
}
#[tokio::test]
#[ignore = "explicit NEW empty loopback PostgreSQL and actual original archive handoff/staging/cancellation"]
async fn postgres_owned_archive_handoff_retains_the_original_pair_and_stages_without_reconstruction()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_ARCHIVE_HANDOFF_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_handoff"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL real-probe owner-revision loss and newly authenticated offline cancellation"]
async fn postgres_live_probed_source_cancellation_uses_fresh_owner_without_restoring_stale_authority()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_PROBED_CANCEL_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_authority"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit offline real-probe owned candidate operation, lost waiter, native activation and fresh-owner atomic final-capacity cancellation"]
async fn live_owned_configured_operation_retains_lost_waiters_and_cancels_only_its_candidate()
-> Result<()> {
    live_preparation_cases(Some("retain_configured_owned"), None).await?;
    live_preparation_cases(Some("retain_configured_owned_cancel"), None).await
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL and real-probe owned candidate operation/cancellation"]
async fn postgres_owned_configured_operation_cancels_after_actual_native_retirement() -> Result<()>
{
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_OWNED_OPERATION_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_owned_cancel"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit offline original source-task ownership before probe, lost waiters, native activation and stop-before-candidate cleanup"]
async fn live_owned_source_operation_retains_preprobe_waiters_and_stops_before_candidate()
-> Result<()> {
    live_preparation_cases(Some("retain_configured_preprobe_owned"), None).await?;
    live_preparation_cases(Some("retain_configured_preprobe_stop"), None).await
}
#[tokio::test]
#[ignore = "explicit offline retained activation before prepare/claim SQL, actual native activation and stopped unlaunched original claims"]
async fn live_owned_activation_retains_sql_waiters_and_never_launches_a_stopped_claim() -> Result<()>
{
    live_preparation_cases(Some("retain_configured_preclaim_owned"), None).await?;
    live_preparation_cases(Some("retain_configured_preclaim_stop"), None).await?;
    live_preparation_cases(Some("retain_configured_preclaim_sql_stop"), None).await
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL full owned activation before prepare/claim SQL and actual native installed invocation"]
async fn postgres_owned_activation_survives_lost_waiters_from_before_prepare_sql() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_ACTIVATION_OPERATION_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_preclaim_owned"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL sealed never-launched original source claim and fresh-owner final-capacity cleanup"]
async fn postgres_owned_activation_cancels_a_stopped_original_source_claim_without_launching()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_ACTIVATION_STOP_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_preclaim_stop"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit offline actual native readiness, later real control writes and exact activation retry"]
async fn live_owned_activation_respects_later_choices_and_never_reenables_on_retry() -> Result<()> {
    for case in [
        "retain_configured_choice_instance",
        "retain_configured_choice_extension",
        "retain_configured_choice_retry",
    ] {
        Box::pin(live_preparation_cases(Some(case), None)).await?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL actual native readiness then real instance disable"]
async fn postgres_owned_activation_respects_a_later_instance_disable() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_CHOICE_INSTANCE_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_choice_instance"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL actual native readiness then real extension disable"]
async fn postgres_owned_activation_respects_a_later_extension_disable() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_CHOICE_EXTENSION_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_choice_extension"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL exact activated retry after a real disable"]
async fn postgres_owned_activation_retry_never_reenables_a_disabled_instance() -> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_CHOICE_RETRY_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_choice_retry"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit offline original source restore/probe/disposal/retention error ownership and fresh-owner final-capacity cancellation"]
async fn live_owned_source_failures_retain_actual_disposal_and_cancel_without_source_renewal()
-> Result<()> {
    for suffix in ["restore", "probe", "retention", "ack"] {
        let case = format!("retain_configured_source_failure_{suffix}");
        Box::pin(live_preparation_cases(Some(&case), None)).await?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "explicit four NEW empty loopback PostgreSQL databases and actual original native source error/disposal/retention cleanup"]
async fn postgres_owned_source_failures_retain_actual_disposal_and_cancel_without_source_renewal()
-> Result<()> {
    for suffix in ["restore", "probe", "retention", "ack"] {
        let url = std::env::var(format!(
            "ELIXIR_TEST_MARKETPLACE_SOURCE_FAILURE_{}_POSTGRES_URL",
            suffix.to_uppercase()
        ))?;
        let case = format!("retain_configured_source_failure_{suffix}");
        Box::pin(live_preparation_cases(Some(&case), Some(&url))).await?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "explicit actual native source/probe and held SQL writer; stop seals original returned candidate claim without launching migration"]
async fn live_owned_candidate_stop_joins_claim_sql_and_never_launches_the_returned_claim()
-> Result<()> {
    live_preparation_cases(Some("retain_configured_candidate_stop"), None).await
}
#[tokio::test]
#[ignore = "explicit NEW empty loopback PostgreSQL and actual native source/probe, stopped original candidate claim, offline final-capacity cleanup"]
async fn postgres_owned_candidate_stop_joins_claim_sql_and_never_launches_the_returned_claim()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_CANDIDATE_STOP_POSTGRES_URL")?;
    live_preparation_cases(Some("retain_configured_candidate_stop"), Some(&url)).await
}
#[tokio::test]
#[ignore = "explicit actual native source/probe and original acknowledged pre-claim SQL outputs through later authority failure/offline cleanup"]
async fn live_owned_preclaim_failures_retain_original_quiescence_and_copy_for_atomic_cleanup()
-> Result<()> {
    for suffix in ["quiesce", "copy"] {
        let case = format!("retain_configured_preparation_failure_{suffix}");
        Box::pin(live_preparation_cases(Some(&case), None)).await?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "explicit two NEW empty loopback PostgreSQL databases and actual native source/probe/pre-claim original-output cleanup"]
async fn postgres_owned_preclaim_failures_retain_original_quiescence_and_copy_for_atomic_cleanup()
-> Result<()> {
    for suffix in ["quiesce", "copy"] {
        let url = std::env::var(format!(
            "ELIXIR_TEST_MARKETPLACE_PRECLAIM_{}_POSTGRES_URL",
            suffix.to_uppercase()
        ))?;
        let case = format!("retain_configured_preparation_failure_{suffix}");
        Box::pin(live_preparation_cases(Some(&case), Some(&url))).await?;
    }
    Ok(())
}
async fn live_preparation_cases(only: Option<&str>, postgres: Option<&str>) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable image required")?;
    let image = docker(&["image", "inspect", "--format", "{{.Id}}", &image]).await?;
    let configuring = only.is_some_and(|case| case.starts_with("retain_configured_"));
    let value = sdk_fixture(configuring).await?;
    let (release, package, proof) =
        crate::marketplace::quarantine::tests::decode_fixture(value.clone())?;
    let issuer = value["issuer_key"].as_str().context("fixture issuer")?;
    let features = DockerRuntimeManager::new(None).image_labels(&image).await?;
    let discovery = features
        .get("elixir.marketplace.worker.blob_discovery")
        .map(String::as_str)
        == Some("1");
    let mut cases = vec!["normal", "corrupt", "cancel", "authority", "revocation"];
    if features
        .get("elixir.marketplace.worker.blob_discovery")
        .map(String::as_str)
        == Some("1")
    {
        cases.push("providers");
    } else {
        eprintln!("legacy image: provider-discovery case NOT qualified");
    }
    if features
        .get("elixir.marketplace.worker.blob_catalog_fallback")
        .map(String::as_str)
        == Some("1")
    {
        cases.extend(["fallback_empty", "fallback_stale"]);
    }
    cases.extend([
        "approve",
        "approve_revoke",
        "approve_session",
        "approve_foreign",
    ]);
    cases.extend([
        "retain",
        "retain_budget",
        "retain_cancel",
        "retain_start_cancel",
        "retain_handoff",
    ]);
    if features
        .get("elixir.marketplace.worker.source_probe")
        .map(String::as_str)
        == Some("1")
    {
        cases.extend(["retain_probe", "retain_prepared", "retain_prepared_revoke"]);
        if configuring {
            cases.extend([
                "retain_configured_activate",
                "retain_configured_authority",
                "retain_configured_cancel",
                "retain_configured_owned",
                "retain_configured_owned_cancel",
                "retain_configured_preprobe_owned",
                "retain_configured_preprobe_stop",
                "retain_configured_preclaim_owned",
                "retain_configured_preclaim_stop",
                "retain_configured_preclaim_sql_stop",
                "retain_configured_choice_instance",
                "retain_configured_choice_extension",
                "retain_configured_choice_retry",
                "retain_configured_source_failure_restore",
                "retain_configured_source_failure_probe",
                "retain_configured_source_failure_retention",
                "retain_configured_source_failure_ack",
                "retain_configured_candidate_stop",
                "retain_configured_preparation_failure_quiesce",
                "retain_configured_preparation_failure_copy",
            ]);
        }
    }
    if let Some(only) = only {
        if !cases.contains(&only) {
            bail!("unknown fixed native preparation case");
        }
        cases.retain(|case| *case == only);
    }
    for case in cases {
        eprintln!("offline Live preparation case={case}");
        let root = tempfile::tempdir()?;
        let private = root.path().canonicalize()?.join("foreground");
        let files = root.path().join("source");
        std::fs::create_dir(&files)?;
        for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
            let file = files.join(name);
            std::fs::write(&file, bytes)?;
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444))?;
        }
        // Read/search permission for the unprivileged read-only bind mount.
        std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
        let deployment = format!("preparation-{}", Uuid::new_v4().simple());
        let name = format!(
            "elixir-marketplace-preparation-peer-{}",
            Uuid::new_v4().simple()
        );
        let database = if let Some(url) = postgres {
            configured::fresh_postgres(url).await?
        } else {
            Database::connect(&DatabaseConfig {
                url: format!("sqlite://{}", root.path().join("security.sqlite").display()),
                max_connections: 4,
                connect_timeout_seconds: 5,
            })
            .await?
        };
        database.run_migrations().await?;
        let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
        let service = Service::default();
        let mut fixture = None;
        let mut restoration_deployment = None;
        let mut activation_host = None;
        let mut activation_host_deployment = None;
        let mut activation_launch = None;
        let mut configured = None;
        let mut handoff_actor = None;
        let mut handoff_target = None;
        let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
            let retaining=case.starts_with("retain");
            let mode = if matches!(case, "cancel" | "authority" | "revocation") { "stall" }
                else if retaining && discovery {"providers"}
                else if retaining || case.starts_with("approve") {"normal"} else { case };
            fixture = Some(PeerPipeFixture::start(&image, &files, &name, issuer, mode).await?);
            let peer = fixture.as_mut().unwrap();
            peer.bind_security_database(database.clone());
            check_profile(&name).await?;
            let (descriptor, policy) = descriptor(&value)?;
            let (metadata, _metadata_owner) = MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(), &policy)?;
            security_state::admit(&database, &metadata).await?;
            let (source, _source_owner) = PublicSourceObservation::fixture_for_source(release.release_id(), Digest::of(b"SPDX-License-Identifier: MIT\n"));
            if case=="retain_handoff" {
                let actor=crate::marketplace::grant_store::tests::owner(&database).await?;
                let preferences=String::from_utf8(elixir_marketplace::canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?;
                sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
                    .bind(actor.home.to_string()).bind(preferences).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&database.pool).await?;
                use crate::marketplace::grant_store::installation::{CapturedTarget,ExpectedControls,InstallAction,InstallTarget};
                handoff_target=Some(CapturedTarget::capture(&database,&actor.fixture_principal(),InstallAction {
                    extension:release.payload().extension_id.clone(),release:release.release_id(),
                    target:InstallTarget::New{name:"Original archived source".into()},expected_revision:0,
                    mutation:Uuid::new_v4(),controls:ExpectedControls::Absent{},
                }).await?);
                handoff_actor=Some(actor);
            }
            let peers = if case == "providers" || (retaining && discovery) { vec![] } else { vec![peer.peer.clone()] };
            let selected = Selection::fixture_for_native_preparation(peer.client.clone(), metadata, source, peers)?;
            let mut live = Live::new(selected, None)?;
            let retention=crate::marketplace::archive::ArchiveBudget {bytes:if case=="retain_budget" {MIB+1} else {128*MIB},releases:2};
            if retaining {live=live.with_retention(retention)?;}
            let plan = SourceQuarantinePlan::for_release(&release, None, &live.budget)?;
            let job = SourceQuarantineJob::open(runtime.clone(), &private, &deployment, &image, &plan).await?;
            let purposes = job.plan_workspace().descriptors()?.into_iter().map(|d| d.purpose).collect();
            // Native admission is explicitly preopened by this isolated fixture,
            // not a fake production resource/protection capability constructor.
            live.workspace = Workspace::Download(Box::new(Download { job: Some(job), purposes, slot: 0, peer: 0, providers: None, last_failure: None }));
            let id = service.start_owned(live)?;
            if matches!(case, "normal" | "providers" | "fallback_empty" | "fallback_stale") || case.starts_with("approve") || retaining {
                let ready = phase(&service, id, Phase::AwaitingApproval).await?;
                assert_eq!(ready.release_id, release.release_id());
                assert!(!ready.installation_ready);
                let review = ready.review.context("native permission review missing")?;
                assert_eq!(review.release_id(), release.release_id());
                assert!(review.scope().secret_keys().iter().any(|key| key == "source_api"));
                assert_eq!(counters(&name).await?["starts"], if case=="fallback_stale" {4} else {2});
                if case == "providers" { assert_eq!(counters(&name).await?["providers"], 2); }
                if matches!(case, "fallback_empty" | "fallback_stale") {
                    let observed=counters(&name).await?;
                    assert_eq!(observed["providers"],2);
                    assert_eq!(observed["opened"],2,"the known catalog supplied both signed blobs");
                    assert_eq!(observed["stale"],if case=="fallback_stale" {2} else {0});
                }
                assert!(ControllerLease::acquire(&private).is_err(), "review retains the same source lane");
                if case.starts_with("approve") || retaining {
                    let mut actor=match handoff_actor.take(){Some(actor)=>actor,None=>crate::marketplace::grant_store::tests::owner(&database).await?};
                    if case=="approve_foreign" || retaining {
                        let json=String::from_utf8(elixir_marketplace::canonical::to_vec(&crate::marketplace::preferences::Preferences::default())?)?;
                        sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
                            .bind(actor.home.to_string()).bind(json).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&database.pool).await?;
                    }
                    if case=="approve_foreign" {
                        actor=crate::marketplace::grant_store::tests::owner(&database).await?;
                    }
                    let mutation_id=Uuid::new_v4();let review_id=review.review_id()?;
                    let input=||crate::marketplace::grant_store::ApprovalSelection {
                        review_id,expected_revision:0,mutation_id,automatic_updates:false,
                    };
                    if case=="approve_session" {
                        sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='offline owner-approval fixture' WHERE user_id=$1")
                            .bind(actor.user.to_string()).execute(&database.pool).await?;
                    }
                    if case=="retain_handoff" {
                        let mut destination=None;
                        assert!(timeout(Duration::from_millis(20),service.take_archive(id,&mut destination)).await.is_err());
                        assert!(destination.is_none(),"cancelled waiter cannot extract the original output");
                        assert!(ControllerLease::acquire(&private).is_err(),"original source still owned at review");
                    }
                    service.approve_as(id,actor.clone(),input())?;
                    service.approve_as(id,actor.clone(),input())?;
                    if retaining {
                        let archive=root.path().canonicalize()?.join("source-archives");
                        if case=="retain_handoff" {
                            let archived=phase(&service,id,Phase::SourceArchived).await?;
                            assert!(!archived.installation_ready);
                            let mut original=None;
                            service.take_archive(id,&mut original).await?;
                            assert!(service.take_archive(id,&mut original).await.is_err(),"occupied destination must remain untouched");
                            let mut duplicate=None;
                            assert!(service.take_archive(id,&mut duplicate).await.is_err(),"original pair is one-use");
                            assert!(duplicate.is_none());
                            assert!(service.view(id).is_err(),"only the original joined handoff releases the service slot");
                            let (mut selected,retained,approved_review,grant_revision)=original.take().context("original archive handoff")?.into_parts();
                            assert_eq!(retained.release_id(),release.release_id());
                            assert_eq!(approved_review,review_id);
                            assert_eq!(grant_revision,1);
                            let authority=elixir_marketplace::retained_authority::AuthorityRecord::from_descriptor(&descriptor)?.verify()?;
                            let (_,actual_review)=retained.record_for(authority.release())?.review(authority.release())?;
                            assert_eq!(actual_review.review_id()?,approved_review);
                            let principal=actor.fixture_principal();
                            use crate::marketplace::grant_store::installation::{self,OwnerInstallIntent};
                            let staged=installation::stage(&mut selected,&retained,OwnerInstallIntent::new_captured(&principal,&actual_review,grant_revision,handoff_target.as_ref().context("target captured before source work")?)?,installation::InstallBudget{entries:2,bytes:4*MIB}).await?;
                            assert_eq!(staged.revision(),1);
                            let owner=staged.configured_instance().context("configured owner")?;
                            let enabled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1").bind(owner.to_string()).fetch_one(&database.pool).await?;
                            assert_eq!(enabled,0,"archive handoff and staging are not execution authority");
                            let mutation=Uuid::new_v4();
                            let cancelled=installation::cancellation::cancel(&database,installation::cancellation::OwnerCancelIntent::new(&principal,&staged,mutation)?,installation::InstallBudget{entries:2,bytes:4*MIB}).await?;
                            assert_eq!(cancelled.revision(),2);
                            assert_eq!(installation::cancellation::cancel(&database,installation::cancellation::OwnerCancelIntent::new(&principal,&staged,mutation)?,installation::InstallBudget{entries:2,bytes:4*MIB}).await?.revision(),2);
                            let remaining:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT COUNT(*) FROM marketplace_sdk_namespaces),(SELECT COUNT(*) FROM marketplace_instance_sdk_state),(SELECT COUNT(*) FROM marketplace_generation_secrets),(SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE)")
                                .fetch_one(&database.pool).await?;
                            assert_eq!(remaining,(0,0,0,0),"cancelled handoff preserves only the disabled configured owner, not its private candidate or execution authority");
                            eprintln!("original archive/selection/review/grant revision survived a lost waiter, transferred once AFTER cleanup, staged without path/status reconstruction and cancelled while disabled");
                        } else if matches!(case,"retain"|"retain_start_cancel"|"retain_probe"|"retain_prepared"|"retain_prepared_revoke") || configuring {
                            let archived=phase(&service,id,Phase::SourceArchived).await?;
                            assert!(!archived.installation_ready);assert!(archived.review.is_none());
                            assert!(ControllerLease::acquire(&private).is_ok());
                            let retained=crate::marketplace::archive::verify_retained(&archive,retention,&release,&private,16*MIB,20).await?;
                            assert_eq!(retained.release_id(),release.release_id());
                            let instances:i64=sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances").fetch_one(&database.pool).await?;
                            assert_eq!(instances,0,"permission/archive completion must not imply installation");
                            let host_deployment=format!("activation-host-{}",Uuid::new_v4().simple());
                            activation_host_deployment=Some(host_deployment.clone());
                            let worker_limits=crate::marketplace::packet::PacketWorkloadBudget::split(
                                &elixir_marketplace::policy::OrdinaryLimits::default())?.worker_limits().clone();
                            activation_host=Some(crate::marketplace::worker::OfflineWorker::start(runtime.as_ref(),
                                &root.path().canonicalize()?.join("activation-host"),&host_deployment,&image,
                                &[issuer.parse()?],&worker_limits).await?);
                            peer.bind_offline_source_owner(activation_host.as_ref().unwrap(),runtime.clone(),&private)?;
                            // Separate explicit owner action. Real retained bytes, fresh
                            // selected metadata/source, and the durable staging transaction.
                            let (metadata,_metadata_owner)=MetadataObservation::fixture_for_verified_descriptor(descriptor.clone(),&policy)?;
                            let (source,_source_owner)=PublicSourceObservation::fixture_for_source(release.release_id(),Digest::of(b"SPDX-License-Identifier: MIT\n"));
                            let mut selected=Selection::fixture_for_native_preparation(peer.client.clone(),metadata,source,vec![peer.peer.clone()])?;
                            let authority=elixir_marketplace::retained_authority::AuthorityRecord::from_descriptor(&descriptor)?.verify()?;
                            let (_,review)=retained.record_for(authority.release())?.review(authority.release())?;
                            if configuring {
                                configured = Some(configured::Configured::open(database.clone(), runtime.clone(), &private, &archive, &image, &actor).await?);
                            }
                            let intent=if let Some(configured) = &configured {
                                configured.install(&review)?
                            } else { crate::marketplace::grant_store::installation::OwnerInstallIntent::fixture_for_actor(actor.clone(),&review,1,0,Uuid::new_v4())? };
                            let staged=crate::marketplace::grant_store::installation::stage(&mut selected,&retained,intent,
                                crate::marketplace::grant_store::installation::InstallBudget{entries:2,bytes:4*MIB}).await?;
                            assert_eq!(staged.release(),release.release_id());assert_eq!(staged.revision(),1);
                            // Durable prelaunch fencing only: the fixture source
                            // is not an executable readiness/activation proof.
                            use crate::marketplace::grant_store::installation::activation::{self,OwnerActivationIntent,SourceActivationPlan,PreparedOrRecovery,StartOrRecovery,SourceLaunch,SourcePhase};
                            let recipe=SourceActivationPlan::new(authority.release(),&review,&image,
                                &crate::marketplace::quarantine::SourceJobBudget {memory_bytes:64*MIB,cpu_millicores:100,disk_bytes:128*MIB,input_bytes:64*MIB,output_bytes:64*MIB,wall_seconds:120})?;
                            let activation_mutation=Uuid::new_v4();
                            let activation_intent=|| match &configured {
                                Some(configured) => configured.intent(&staged, &review, activation_mutation),
                                None => OwnerActivationIntent::fixture_for_actor(actor.clone(),&staged,&review,1,activation_mutation),
                            };
                            let allocation=crate::marketplace::grant_store::installation::InstallBudget{entries:2,bytes:4*MIB};
                            if case.starts_with("retain_configured_preclaim_") || case.starts_with("retain_configured_choice_") {
                                let activation_intent=activation_intent()?;
                                let request=activation::ActivationRequest::new(selected,retained,staged,activation_intent,recipe,allocation,retention)?;
                                Box::pin(configured.as_mut().context("configured controller missing")?.finish_owned_activation(request,case,&mut fixture)).await?;
                            } else {
                            let prepared=match activation::prepare(&mut selected,&retained,activation_intent()?,&recipe,allocation).await? {
                                PreparedOrRecovery::Prepared(p)=>p,_=>bail!("native fixture unexpectedly requires activation recovery"),
                            };
                            let claimed=match activation::claim_start(&mut selected,&retained,activation_intent()?,&prepared,&recipe,allocation).await? {
                                StartOrRecovery::Start(s)=>s,_=>bail!("native fixture did not get its first start claim"),
                            };
                            assert_eq!(prepared.revision(),2);assert_eq!(claimed.revision(),3);assert_eq!(prepared.attempt(),claimed.attempt());
                            match activation::claim_start(&mut selected,&retained,activation_intent()?,&prepared,&recipe,allocation).await? {
                                StartOrRecovery::Recovery(r)=>assert_eq!(r.attempt(),claimed.attempt()),
                                StartOrRecovery::Start(_)=>bail!("native fixture retry minted another start claim"),
                            }
                            // Actual archive readback, native rehash and Git/package
                            // rematch. This is still quarantine, NOT execution,
                            // candidate health, activation commit or admission proof.
                            restoration_deployment=Some(claimed.deployment());
                            let expected=retained.record_for(authority.release())?.summary().clone();
                            activation_launch=Some(SourceLaunch::begin(selected,retained,claimed,recipe,allocation,retention)?);
                            if case.starts_with("retain_configured_source_failure_") {
                                Box::pin(configured.as_mut().context("configured controller missing")?.source_failure(&mut activation_launch,&staged,case,&mut fixture)).await?;
                            } else if case.starts_with("retain_configured_preparation_failure_") {
                                Box::pin(configured.as_mut().context("configured controller missing")?.finish_pre_candidate_failure(activation_launch.as_mut().context("original source launch")?,&staged,case,&mut fixture)).await?;
                            } else if case=="retain_configured_candidate_stop" {
                                Box::pin(configured.as_mut().context("configured controller missing")?.finish_unstarted(activation_launch.as_mut().context("original source launch")?,&staged,&mut fixture)).await?;
                            } else if matches!(case,"retain_configured_preprobe_owned"|"retain_configured_preprobe_stop") {
                                Box::pin(configured.as_mut().context("configured controller missing")?.finish_owned_source(&mut activation_launch,&staged,case,&mut fixture)).await?;
                            } else {
                            let launching=activation_launch.as_mut().unwrap();
                            if configuring {
                                Box::pin(configured.as_mut().context("configured controller missing")?.finish(launching, &staged, case, &mut fixture)).await?;
                            } else if case=="retain_start_cancel" {
                                timeout(Duration::from_secs(110),async {
                                    loop {
                                        match launching.phase() {
                                            SourcePhase::Restoring=>return Ok::<_,anyhow::Error>(()),
                                            SourcePhase::Failed|SourcePhase::Cancelled|SourcePhase::SourceVerified=>
                                                bail!("native source cancellation fixture missed the owned restoring phase: {:?}",launching.status()),
                                            _=>sleep(Duration::from_millis(10)).await,
                                        }
                                    }
                                }).await.with_context(||format!("native source never reached restoring: {:?}",launching.status()))??;
                                launching.cancel().await?;
                                assert_eq!(launching.phase(),SourcePhase::Cancelled);
                            } else if case=="retain_probe" {
                                probed_source_handoff(launching,&private,&expected).await?;
                            } else if matches!(case,"retain_prepared"|"retain_prepared_revoke") {
                                retained_probe_handoff(launching,&private,&database,case=="retain_prepared_revoke").await?;
                                let deployment=restoration_deployment.as_deref().context("retained preparation deployment missing")?;
                                assert!(runtime.get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}")).await?.is_none());
                                assert!(!runtime.private_file_volume_owned(&format!("elixir-marketplace-source-{deployment}"),&source_labels(deployment)).await?,"retention must delete its temporary native copy, not just release its lane");
                                let rehashed=crate::marketplace::archive::verify_retained(&archive,retention,&release,&private,16*MIB,20).await?;
                                assert_eq!(rehashed.release_id(),release.release_id(),"the bounded durable archive survives native disposal");
                            } else {
                                claimed_source_handoff(launching,&private,&expected).await?;
                            }
                            }
                            }
                            assert!(ControllerLease::acquire(&private).is_ok());
                            assert!(ControllerLease::acquire(&archive).is_ok());
                            let enabled:i64=sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM extensions WHERE enabled=TRUE)+(SELECT COUNT(*) FROM extension_instances WHERE enabled=TRUE)+(SELECT COUNT(*) FROM providers) AS BIGINT)")
                                .fetch_one(&database.pool).await?;
                            assert_eq!(enabled,if case=="retain_configured_choice_retry" {1} else if matches!(case,"retain_configured_activate"|"retain_configured_owned"|"retain_configured_preprobe_owned"|"retain_configured_preclaim_owned") {2} else {0},"only an original joined successful configured candidate can enable code");
                        } else if case=="retain_budget" {
                            assert!(phase(&service,id,Phase::Failed).await?.review.is_none());
                            assert!(!archive.join("releases/.pending").exists(),"quota failure must precede private blob allocation");
                        } else {
                            archive_prefix(&archive.join("releases/.pending/package.blob"),&service,id).await?;
                            assert!(ControllerLease::acquire(&private).is_err());
                            service.cancel(id).await?;
                            assert!(ControllerLease::acquire(&private).is_ok());
                            assert!(ControllerLease::acquire(&archive).is_ok(),"cancel acknowledgement must follow actual filesystem task/root cleanup");
                        }
                    } else if matches!(case,"approve_session"|"approve_foreign") {
                        let failed=phase(&service,id,Phase::Failed).await?;
                        assert!(failed.review.is_none());
                    } else {
                        let approved=phase(&service,id,Phase::PermissionGranted).await?;
                        assert!(!approved.installation_ready);
                        let grant=crate::marketplace::grant_store::load(&database,&release.payload().extension_id).await?.context("owned approval was not committed")?;
                        assert_eq!(grant.revision(),1);assert!(!grant.automatic_updates());
                        assert_eq!(grant.review().review_id()?,review_id);
                        assert!(ControllerLease::acquire(&private).is_err(),"permission does not discard or release quarantined source");
                        if case=="approve_revoke" {
                            sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,revision=revision+1 WHERE extension_id=$1")
                                .bind(release.payload().extension_id.as_str()).execute(&database.pool).await?;
                            assert!(phase(&service,id,Phase::Failed).await?.review.is_none());
                        }
                    }
                    let instances:i64=sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE extension_id=$1")
                        .bind(release.payload().extension_id.as_str()).fetch_one(&database.pool).await?;
                    assert_eq!(instances,if matches!(case,"retain"|"retain_start_cancel"|"retain_probe"|"retain_prepared"|"retain_prepared_revoke"|"retain_handoff") || configuring {1} else {0},"only a separate explicit disabled-staging request may allocate an instance");
                }
            } else if case == "corrupt" {
                let failed = phase(&service, id, Phase::Failed).await?;
                assert!(failed.review.is_none());
                assert_eq!(counters(&name).await?["starts"], 1);
            } else {
                received_prefix(runtime.as_ref(), &deployment).await?;
                assert!(ControllerLease::acquire(&private).is_err());
                if matches!(case, "authority" | "revocation") {
                    let mut head = descriptor.head.clone();
                    let mut revocations = vec![];
                    if case == "authority" {
                        head.payload.sequence += 1;
                        head = SignedRecord::sign(head.payload, head.authority_record_id, &SigningKey::from_bytes(&[2; 32]))?;
                    } else {
                        revocations.push(SignedRecord::sign(Revocation {
                            root_key: descriptor.publisher.payload.root_key,
                            extension_id: descriptor.release.payload.extension_id.clone(),
                            release_ids: vec![release.release_id()], release_keys: vec![],
                            reason: "offline fixture publisher withdrawal".into(),
                        }, None, &SigningKey::from_bytes(&[1; 32]))?);
                    }
                    assert!(security_state::fixture_observe_signed(&database, SecurityRecords {
                        schema_version: 1, extension_id: descriptor.release.payload.extension_id.clone(),
                        publisher: descriptor.publisher, head: Some(head), revocations,
                    }).await?);
                    let failed = phase(&service, id, Phase::Failed).await?;
                    assert!(failed.review.is_none());
                }
            }
            if !matches!(case,"retain_cancel"|"retain_handoff") {service.cancel(id).await?;}
            if matches!(case, "cancel" | "authority" | "revocation") {
                // The network acknowledgement MUST precede the completed
                // service cancellation, not arrive later by fire-and-forget.
                assert!(counters(&name).await?["cancels"].as_u64().unwrap_or(0) >= 1);
            }
            assert!(ControllerLease::acquire(&private).is_ok());
            let grants: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_extension_grants").fetch_one(&database.pool).await?;
            assert_eq!(grants, if matches!(case,"approve"|"approve_revoke") || retaining {1} else {0}, "only an explicit currently authenticated owner request may commit a grant");
            assert!(runtime.get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}")).await?.is_none());
            assert!(!runtime.private_file_volume_owned(&format!("elixir-marketplace-source-{deployment}"), &source_labels(&deployment)).await?, "actor cancellation, not fallback test cleanup, must dispose the volume");
            println!("Live case={case} passed; native bytes/ownership only, synthetic observations/protection");
            Ok::<_,anyhow::Error>(())
        })).catch_unwind().await;
        if let Some(configured) = configured.as_mut() {
            configured.cancel().await?;
        }
        let activation_stopped = match activation_launch.as_mut() {
            Some(launch) if launch.phase()!=crate::marketplace::grant_store::installation::activation::SourcePhase::SourceVerified => launch.cancel().await,
            _=>Ok(()),
        };
        drop(activation_launch);
        let activation_host_stopped = match activation_host.take() {
            Some(worker) => worker.stop(runtime.as_ref()).await,
            None => Ok(()),
        };
        let stopped = service.shutdown().await;
        let peer_stopped = if let Some(peer) = fixture {
            peer.finish().await
        } else {
            Ok(())
        };
        let peer_removed = remove_peer(&name).await;
        let source_removed = remove_source(runtime.as_ref(), &deployment).await;
        let restoration_removed = match restoration_deployment.as_deref() {
            Some(deployment) => remove_source(runtime.as_ref(), deployment).await,
            None => Ok(()),
        };
        let activation_host_removed = match activation_host_deployment.as_deref() {
            Some(deployment) => remove_activation_host(runtime.as_ref(), deployment).await,
            None => Ok(()),
        };
        database.pool.close().await;
        std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o700))?;
        stopped?;
        peer_stopped?;
        peer_removed?;
        source_removed?;
        restoration_removed?;
        activation_stopped?;
        activation_host_stopped?;
        activation_host_removed?;
        match outcome {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    Ok(())
}
