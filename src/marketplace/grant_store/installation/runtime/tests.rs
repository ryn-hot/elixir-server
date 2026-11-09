//! Actual native execution/rehash/encrypted SDK and independent host admission.
//! ACTIVE enrollment is explicitly synthetic: NOT production activation proof.
use super::super::{standing::tests as standing_tests, tests::Fixture};
use super::*;
use crate::marketplace::grant_store::tests as grant_tests;
use serde_json::json;
mod application_http;
pub(super) mod compiled;
mod local_http;

fn secrets() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
pub(in crate::marketplace::grant_store::installation) fn limits() -> SourceExecutionLimits {
    SourceExecutionLimits {
        profile: Default::default(),
        heap_bytes: 16 * 1024 * 1024,
        timeout_millis: 30_000,
        max_file_bytes: 4 * 1024 * 1024,
        json_bytes: 1024 * 1024,
        pending_jobs: 128,
        sdk: Some(elixir_marketplace::source_execution::SourceSdkLimits {
            calls: 16,
            request_bytes: 1024 * 1024,
            response_bytes: 1024 * 1024,
        }),
    }
}
async fn broker<'a>(
    f: &'a Fixture,
    binding: &Arc<InstalledBinding>,
    secrets: &'a SecretsManager,
    stop: watch::Receiver<bool>,
    http_call: &'a mut Option<http::Call>,
) -> Result<InstalledBroker<'a>> {
    Ok(InstalledBroker {
        database: &f.db,
        secrets,
        binding: binding.clone(),
        descriptor: binding.material(&f.db).await?.descriptor()?,
        native: None,
        http: None,
        local_http: None,
        application_http: None,
        application_image: "",
        application_deployment: "",
        application_admission: None,
        http_call,
        stop: RunControl::fixture(stop),
        expires: Deadline::now() + Duration::from_secs(60),
        checked: None,
    })
}
#[tokio::test]
async fn enrolled_metadata_and_live_binding_cannot_issue_sdk_effects_without_a_native_executor()
-> Result<()> {
    let f = Fixture::new().await?;
    let instance = standing_tests::enroll(&f).await?;
    let binding = Arc::new(standing_tests::binding(&f, instance).await?);
    let manager = secrets();
    let (stop, stopped) = watch::channel(false);
    let mut http_call = None;
    let mut broker = broker(&f, &binding, &manager, stopped, &mut http_call).await?;
    broker.current().await?;
    let before: i64 = sqlx::query_scalar(
        "SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(instance.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    for request in [
        json!({"operation":"state_set","key":"cursor","value":"forbidden"}),
        json!({"operation":"secret","key":"source_api"}),
        json!({"operation":"fetch","url":"https://provider.example/","method":"GET","headers":[],"body":null}),
    ] {
        assert!(
            broker
                .request(&RuntimeJson::from_value(request)?, 65536)
                .await
                .is_err()
        );
    }
    let after: i64 = sqlx::query_scalar(
        "SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(instance.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(before, after);
    stop.send_replace(true);
    assert!(broker.current().await.is_err());
    Ok(())
}
#[tokio::test]
async fn material_is_exact_enrollment_data_not_a_snapshot_that_can_follow_a_replacement()
-> Result<()> {
    let f = Fixture::new().await?;
    let instance = standing_tests::enroll(&f).await?;
    let binding = standing_tests::binding(&f, instance).await?;
    let first = binding.material(&f.db).await?;
    assert_eq!(first.instance, instance);
    standing_tests::change_journal(&f, |journal| {
        journal.active.as_mut().unwrap().grant_revision += 1;
        Ok(())
    })
    .await?;
    assert!(binding.material(&f.db).await.is_err());
    Ok(())
}

#[tokio::test]
async fn snapshot_wait_can_hold_the_actual_sqlite_writer_past_authority_poll_ticks() -> Result<()> {
    let f = Fixture::new().await?;
    let (_stop, stopped) = watch::channel(false);
    let mut stopped = RunControl::fixture(stopped);
    let result = cancellable(
        &mut stopped,
        Deadline::now() + Duration::from_secs(10),
        async {
            let mut connection = crate::marketplace::durability::connection(&f.db).await?;
            let mut tx = connection.begin().await?;
            synchronous(&mut tx, f.db.driver).await?;
            crate::marketplace::preferences::require_home_binding(
                &mut tx,
                f.actor.home,
                f.db.driver,
            )
            .await?;
            // A real held writer crosses multiple normal authority polling ticks.
            // This wrapper has no DB/binding handle with which to deadlock it
            // behind a second writer while its transaction is paused.
            sleep(3 * CHECK).await;
            let row: i64 = sqlx::query_scalar(
                "SELECT revision FROM marketplace_preferences WHERE singleton=1",
            )
            .fetch_one(&mut *tx)
            .await?;
            tx.rollback().await?;
            Ok(row)
        },
    )
    .await?;
    assert_eq!(result, 1);
    let (_stop, stopped) = watch::channel(false);
    let mut stopped = RunControl::fixture(stopped);
    assert!(
        cancellable(
            &mut stopped,
            Deadline::now() + Duration::from_millis(10),
            async {
                sleep(Duration::from_secs(1)).await;
                Ok::<_, anyhow::Error>(())
            }
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn resource_wait_is_typed_deadline_bound_and_cannot_retry_an_arbitrary_failure() -> Result<()>
{
    let f = Fixture::new().await?;
    let instance = standing_tests::enroll(&f).await?;
    let binding = standing_tests::binding(&f, instance).await?;
    let (_stop, stopped) = watch::channel(false);
    let mut stopped = RunControl::fixture(stopped);
    let error = resource_pause(
        &f.db,
        &binding,
        &mut stopped,
        Deadline::now() + Duration::from_secs(60),
        anyhow::anyhow!("not a resource wait"),
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "not a resource wait");
    let error = resource_pause(
        &f.db,
        &binding,
        &mut stopped,
        Deadline::now() + Duration::from_millis(100),
        crate::marketplace::worker::SourceResourceWait::fixture().into(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("expired"));
    binding.current(&f.db).await?;
    Ok(())
}

#[tokio::test]
async fn invalid_call_reservations_are_rejected_before_a_native_lane_is_opened() -> Result<()> {
    let f = Fixture::new().await?;
    let instance = standing_tests::enroll(&f).await?;
    let image = format!("sha256:{}", "1".repeat(64));
    let mut config = configuration(&f, &image, "preflight-fixture")?;
    let lane = config.lane.clone();
    config.source_budget.memory_bytes = 1024 * 1024;
    assert!(
        InstalledCall::begin(
            f.db.clone(),
            secrets(),
            standing_tests::binding(&f, instance).await?,
            config,
            limits(),
            RuntimeJson::parse(b"null")?
        )
        .is_err()
    );
    assert!(
        InstalledCall::begin(
            f.db.clone(),
            secrets(),
            standing_tests::binding(&f, instance).await?,
            configuration(&f, &image, "preflight-fixture")?,
            limits(),
            RuntimeJson::from_value(json!("x".repeat(MAX_ARGUMENT_BYTES + 1)))?
        )
        .is_err()
    );
    assert!(!lane.exists());
    Ok(())
}

pub(in crate::marketplace::grant_store::installation) fn configuration(
    f: &Fixture,
    image: &str,
    deployment: &str,
) -> Result<InstalledRuntime> {
    Ok(InstalledRuntime {
        runtime: Arc::new(crate::runtime::docker::DockerRuntimeManager::new(None)),
        playback: Arc::new(PlaybackJobManager::new(f.db.pool.clone(), None)),
        local_http: None,
        application_http: None,
        compiled_toolchains: None,
        lane: f.root.path().canonicalize()?.join("expensive"),
        archive: f.root.path().canonicalize()?.join("archive"),
        deployment: deployment.into(),
        image: image.into(),
        source_budget: SourceJobBudget {
            memory_bytes: 64 * 1024 * 1024,
            cpu_millicores: 100,
            disk_bytes: 128 * 1024 * 1024,
            input_bytes: 64 * 1024 * 1024,
            output_bytes: 64 * 1024 * 1024,
            wall_seconds: 300,
        },
        retention: ArchiveBudget {
            // Same explicit allocation used to create this retained fixture;
            // not an increase to any production or ordinary-worker limit.
            bytes: 128 * 1024 * 1024,
            releases: 4,
        },
    })
}
#[tokio::test]
async fn cold_controller_future_stays_bounded_without_increasing_executor_stack() -> Result<()> {
    let f = Fixture::new().await?;
    let instance = standing_tests::enroll(&f).await?;
    let binding = standing_tests::binding(&f, instance).await?;
    let (_stop, stopped) = watch::channel(false);
    let future = run(
        f.db.clone(),
        secrets(),
        binding,
        configuration(&f, &format!("sha256:{}", "1".repeat(64)), "size-fixture")?,
        limits(),
        RuntimeJson::parse(b"null")?,
        RunControl::fixture(stopped),
        None,
    );
    let bytes = std::mem::size_of_val(&future);
    eprintln!("cold installed controller future: {bytes} bytes");
    assert!(
        bytes <= 768 * 1024,
        "heap-box nested transitions instead of increasing worker stacks: {bytes}"
    );
    drop(future);
    Ok(())
}
async fn call(
    f: &Fixture,
    instance: Uuid,
    image: &str,
    deployment: &str,
    arguments: serde_json::Value,
) -> Result<InstalledCall> {
    InstalledCall::begin(
        f.db.clone(),
        secrets(),
        standing_tests::binding(f, instance).await?,
        configuration(f, image, deployment)?,
        limits(),
        RuntimeJson::from_value(arguments)?,
    )
}
async fn wait_started(f: &Fixture, instance: Uuid, before: i64) -> Result<()> {
    timeout(Duration::from_secs(180), async {
        loop {
            let revision: i64 = sqlx::query_scalar(
                "SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1",
            )
            .bind(instance.to_string())
            .fetch_one(&f.db.pool)
            .await?;
            if revision > before {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("installed native SDK start was not observed")?
}
pub(in crate::marketplace::grant_store::installation) async fn assert_disposed(
    f: &Fixture,
    image: &str,
    deployment: &str,
) -> Result<()> {
    let config = configuration(f, image, deployment)?;
    for name in [
        format!("elixir-marketplace-source-writer-{deployment}"),
        format!("elixir-marketplace-source-{deployment}-provision"),
        crate::runtime::measurement::name(deployment),
    ] {
        assert!(
            config.runtime.get_container_handle(&name).await?.is_none(),
            "native actor remains: {name}"
        );
    }
    let labels = std::collections::HashMap::from([
        (
            crate::runtime::model::ELIXIR_MANAGED_LABEL.into(),
            "true".into(),
        ),
        (
            crate::runtime::model::ELIXIR_DEPLOYMENT_ID_LABEL.into(),
            deployment.into(),
        ),
        ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
    ]);
    assert!(
        !config
            .runtime
            .private_file_volume_owned(&format!("elixir-marketplace-source-{deployment}"), &labels)
            .await?
    );
    let lease = ControllerLease::acquire(&config.lane)?;
    assert!(crate::marketplace::lease::activity::Fence::load(&lease)?.is_none());
    drop(lease);
    Ok(())
}

async fn advance_without_withdrawal(f: &Fixture) -> Result<()> {
    use ed25519_dalek::SigningKey;
    use elixir_marketplace::{records::SignedRecord, security_job::SecurityRecords};
    let mut d = f.descriptor.clone();
    d.publisher.payload.sequence += 1;
    d.publisher.payload.release_keys = vec![elixir_marketplace::identity::PublicKey::from_bytes(
        SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes(),
    )?];
    d.publisher = SignedRecord::sign(d.publisher.payload, None, &SigningKey::from_bytes(&[1; 32]))?;
    let publisher = Digest::of(d.publisher.to_bytes()?);
    d.head.payload.publisher_state_id = publisher;
    d.head.payload.sequence = 0;
    d.head.payload.stable_release_id =
        Digest::of(b"newer source, never implicitly adopted by installed runtime");
    d.head = SignedRecord::sign(
        d.head.payload,
        Some(publisher),
        &SigningKey::from_bytes(&[4; 32]),
    )?;
    assert!(
        crate::marketplace::security_state::fixture_observe_signed(
            &f.db,
            SecurityRecords {
                schema_version: 1,
                extension_id: d.release.payload.extension_id,
                publisher: d.publisher,
                head: Some(d.head),
                revocations: vec![],
            }
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit offline Docker installed-source rehash/native SDK/cancellation; synthetic active enrollment, actual independent resource admission"]
async fn native_installed_source_runs_without_discovery_or_browser_and_joins_cancellation()
-> Result<()> {
    use futures_util::FutureExt;
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable worker image required")?;
    if !crate::marketplace::runtime::image_is_pinned(&image) {
        bail!("immutable worker image required");
    }
    let f = Fixture::with_source(Some(r#"import {state,getSecret,fetch} from '@elixir/sdk/v1';
export async function invoke(a) {
  await state.set('cursor',a.value);
  if(a.loop) { for(;;){} }
  let denied=false, networkDenied=false;
  try { await getSecret('unapproved') } catch { denied=true }
  try { await fetch('https://example.invalid/') } catch { networkDenied=true }
  return {value:await state.get('cursor'),secret:await getSecret('source_api'),denied,networkDenied};
}"#)).await?;
    let instance = standing_tests::enroll(&f).await?;
    grant_tests::secret(
        &f.db,
        "global",
        None,
        "source_api",
        "not-global",
        &secrets(),
    )
    .await?;
    grant_tests::secret(
        &f.db,
        "instance",
        Some(instance),
        "source_api",
        "own-only",
        &secrets(),
    )
    .await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='installed native fixture browser logout'")
        .execute(&f.db.pool).await?;
    advance_without_withdrawal(&f).await?;
    let deployment = format!("installed-{}", Uuid::new_v4().simple());
    let mut active: Option<InstalledCall> = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut insufficient=configuration(&f,&image,&deployment)?;
        insufficient.retention.bytes=16*1024*1024;
        active=Some(InstalledCall::begin(f.db.clone(),secrets(),standing_tests::binding(&f,instance).await?,
            insufficient,limits(),RuntimeJson::parse(b"null")?)?);
        let error=active.as_mut().unwrap().finish().await.err().context("undersized archive budget unexpectedly executed source")?;
        assert!(format!("{error:#}").contains("existing source archive exceeds the configured owner budget"),"wrong budget rejection: {error:#}");
        active=None;
        assert_disposed(&f,&image,&deployment).await?;
        active=Some(call(&f, instance, &image, &deployment, json!({"value":"retained-native"})).await?);
        let result=active.as_mut().unwrap().finish().await?;
        assert_eq!(serde_json::from_str::<serde_json::Value>(result.as_str())?,
            json!({"value":"retained-native","secret":"own-only","denied":true,"networkDenied":true}));
        active=None;
        assert_disposed(&f, &image, &deployment).await?;
        let encrypted: String = sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
            .bind(instance.to_string()).fetch_one(&f.db.pool).await?;
        assert!(!encrypted.contains("retained-native"));
        let before: i64 = sqlx::query_scalar("SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1")
            .bind(instance.to_string()).fetch_one(&f.db.pool).await?;
        active=Some(call(&f, instance, &image, &deployment, json!({"value":"cancel-started","loop":true})).await?);
        wait_started(&f, instance, before).await?;
        active.as_mut().unwrap().cancel().await?;
        assert!(active.as_mut().unwrap().finish().await.is_err());
        active=None;
        assert_disposed(&f, &image, &deployment).await?;
        let before: i64=sqlx::query_scalar("SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1")
            .bind(instance.to_string()).fetch_one(&f.db.pool).await?;
        let package=f.root.path().canonicalize()?.join("archive/releases")
            .join(&f.archive.release_id().to_string()[7..]).join("package.blob");
        let original=std::fs::read(&package)?;
        let mut corrupt=original.clone();
        corrupt[0]^=1;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&package,std::fs::Permissions::from_mode(0o600))?;
        std::fs::write(&package,corrupt)?;
        std::fs::set_permissions(&package,std::fs::Permissions::from_mode(0o400))?;
        active=Some(call(&f,instance,&image,&deployment,json!({"value":"must-not-run"})).await?);
        let error=active.as_mut().unwrap().finish().await.err().context("corrupt archive unexpectedly executed source")?;
        assert!(format!("{error:#}").contains("archive replay content hash or length differs"),"wrong corruption rejection: {error:#}");
        active=None;
        assert_disposed(&f,&image,&deployment).await?;
        let after: i64=sqlx::query_scalar("SELECT revision FROM marketplace_instance_sdk_state WHERE instance_id=$1")
            .bind(instance.to_string()).fetch_one(&f.db.pool).await?;
        assert_eq!(before,after,"corrupt source must not reach SDK effects");
        // TEST-ONLY restore of the original fixture bytes, never runtime repair.
        std::fs::set_permissions(&package,std::fs::Permissions::from_mode(0o600))?;
        std::fs::write(&package,original)?;
        std::fs::set_permissions(&package,std::fs::Permissions::from_mode(0o400))?;
        active=Some(call(&f,instance,&image,&deployment,json!({"value":"revocation-started","loop":true})).await?);
        wait_started(&f,instance,after).await?;
        // Synthetic concurrent grant revocation; the actual installed broker
        // must observe and fence it, without reviving the logged-out browser.
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,revocation_revision=revision WHERE extension_id=$1")
            .bind(f.descriptor.release.payload.extension_id.as_str()).execute(&f.db.pool).await?;
        assert!(active.as_mut().unwrap().finish().await.is_err());
        active=None;
        assert_disposed(&f,&image,&deployment).await?;
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    // Always join the original task; never race a second name-based cleanup
    // against its possibly delayed native operation.
    if let Some(call) = active.as_mut() {
        call.cancel().await?;
    }
    assert_disposed(&f, &image, &deployment).await?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
