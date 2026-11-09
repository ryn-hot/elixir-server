//! Synthetic protected owner and fake private process. Original process joins
//! are real; these tests are NOT native-marker origin or protected HTTP proof.
use super::*;
use serde_json::json;
use std::process::Stdio;

struct Authority(AtomicBool);
#[async_trait::async_trait]
impl SdkHttpAuthority for Authority {
    async fn current(&self) -> Result<()> {
        if !self.0.load(Ordering::SeqCst) {
            bail!("fixture authority revoked");
        }
        Ok(())
    }
}
fn allowed() -> Arc<dyn SdkHttpAuthority> {
    Arc::new(Authority(AtomicBool::new(true)))
}

#[tokio::test]
async fn routine_check_waits_for_ready_without_launching_or_ignoring_cancellation() -> Result<()> {
    let f = super::super::tests::fixture()?;
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Checking);
    let (mut cancel, lifeline) = oneshot::channel();
    let authority = allowed();
    let ready = f.owner.http_ready(&mut cancel, authority.as_ref());
    tokio::pin!(ready);
    assert!(
        timeout(Duration::from_millis(40), &mut ready)
            .await
            .is_err()
    );
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Ready);
    assert!(timeout(Duration::from_secs(1), &mut ready).await??);
    drop(lifeline);
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));

    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Checking);
    let (mut cancel, lifeline) = oneshot::channel();
    let ready = f.owner.http_ready(&mut cancel, authority.as_ref());
    tokio::pin!(ready);
    assert!(
        timeout(Duration::from_millis(40), &mut ready)
            .await
            .is_err()
    );
    drop(lifeline);
    assert!(!timeout(Duration::from_secs(1), &mut ready).await??);
    Ok(())
}

#[tokio::test]
async fn readiness_wait_rechecks_revocation_pressure_expiry_and_closed_owner() -> Result<()> {
    for change in ["grant", "pressure", "expiry", "owner", "settings", "facts"] {
        let f = super::super::tests::fixture()?;
        f.protection
            .send_modify(|snapshot| snapshot.state = ProtectionState::Checking);
        let (mut cancel, _lifeline) = oneshot::channel();
        let authority = Arc::new(Authority(AtomicBool::new(true)));
        let ready = f.owner.http_ready(&mut cancel, authority.as_ref());
        tokio::pin!(ready);
        assert!(
            timeout(Duration::from_millis(40), &mut ready)
                .await
                .is_err()
        );
        match change {
            "grant" => authority.0.store(false, Ordering::SeqCst),
            "pressure" => f
                .facts
                .send_modify(|facts| facts.as_mut().unwrap().foreground_resource_pressure = true),
            "expiry" => f
                .protection
                .send_modify(|snapshot| snapshot.until = Some(Instant::now())),
            _ => {}
        }
        f.protection
            .send_modify(|snapshot| snapshot.state = ProtectionState::Ready);
        if change == "owner" {
            drop(f.protection);
        } else if change == "settings" {
            drop(f.settings);
        } else if change == "facts" {
            drop(f.facts);
        }
        let error = timeout(Duration::from_secs(1), &mut ready)
            .await?
            .unwrap_err()
            .to_string();
        let reason = match change {
            "grant" => "authority revoked",
            "pressure" => "resource pressure",
            "expiry" => "protection is not admitted",
            "owner" => "protection",
            _ => "observation owner has stopped",
        };
        assert!(error.contains(reason), "{change}: {error}");
        assert_eq!(f.owner.slots.available_permits(), 1);
        assert!(!f.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}

#[tokio::test]
async fn protection_changed_during_authorization_must_be_ready_again_before_input() -> Result<()> {
    struct PausedAuthority {
        entered: tokio::sync::Notify,
        resume: tokio::sync::Notify,
        first: AtomicBool,
    }
    #[async_trait::async_trait]
    impl SdkHttpAuthority for PausedAuthority {
        async fn current(&self) -> Result<()> {
            if self.first.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            Ok(())
        }
    }
    let f = super::super::tests::fixture()?;
    let authority = PausedAuthority {
        entered: Default::default(),
        resume: Default::default(),
        first: AtomicBool::new(true),
    };
    let (mut cancel, _lifeline) = oneshot::channel();
    let ready = f.owner.http_ready(&mut cancel, &authority);
    tokio::pin!(ready);
    tokio::select! {
        result = &mut ready => panic!("authorization unexpectedly completed: {result:?}"),
        _ = authority.entered.notified() => {},
    }
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Checking);
    authority.resume.notify_one();
    assert!(
        timeout(Duration::from_millis(40), &mut ready)
            .await
            .is_err()
    );
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Ready);
    assert!(timeout(Duration::from_secs(1), &mut ready).await??);
    Ok(())
}

#[tokio::test]
async fn original_http_pipe_receives_no_input_until_protection_is_ready() -> Result<()> {
    let f = super::super::tests::fixture()?;
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Checking);
    let mut call = call(f.owner.clone(), fake("good")?)?;
    assert!(
        timeout(Duration::from_millis(40), call.finish())
            .await
            .is_err()
    );
    assert!(call.task.is_some());
    assert_eq!(f.owner.slots.available_permits(), 0);
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Ready);
    assert!(matches!(
        timeout(Duration::from_secs(5), call.finish()).await??,
        SourceSdkReply::Value(_)
    ));
    call.cancel().await?;
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

impl SdkHttpCall {
    /// Negative-only cross-component fixture, never positive native proof.
    pub(in crate::marketplace) fn fixture_unconfirmed_retirement() -> Result<(Self, impl Send)> {
        let fixture = super::super::tests::fixture()?;
        let call = call(fixture.owner.clone(), fake("missing")?)?;
        Ok((call, fixture))
    }
}
#[tokio::test]
async fn denied_authority_never_launches_a_helper_or_faults_the_shared_worker() -> Result<()> {
    let f = super::super::tests::fixture()?;
    let client = Client(f.owner.clone()).sdk_http().unwrap();
    let mut call = client.start(
        &request().request.to_json()?,
        4096,
        Arc::new(Authority(AtomicBool::new(false))),
    )?;
    assert!(call.finish().await.is_err());
    call.cancel().await?;
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    assert_eq!(f.owner.slots.available_permits(), 1);
    Ok(())
}
#[tokio::test]
async fn lost_authority_in_flight_closes_the_pipe_and_joins_the_same_original_request() -> Result<()>
{
    let f = super::super::tests::fixture()?;
    let authority = Arc::new(Authority(AtomicBool::new(true)));
    let mut call = call_with_authority(f.owner.clone(), fake("wait")?, authority.clone())?;
    assert!(
        timeout(Duration::from_millis(100), call.finish())
            .await
            .is_err()
    );
    authority.0.store(false, Ordering::SeqCst);
    assert!(matches!(
        timeout(Duration::from_secs(5), call.finish()).await??,
        SourceSdkReply::Unavailable
    ));
    assert!(call.completed.as_ref().unwrap().retired);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    assert_eq!(f.owner.slots.available_permits(), 1);
    Ok(())
}

fn request() -> HttpRequest {
    HttpRequest::parse(
        &serde_json::to_vec(
            &serde_json::from_slice::<serde_json::Value>(include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../crates/elixir_marketplace/tests/fixtures/sdk-http-v1.json"
            )))
            .unwrap()["request"],
        )
        .unwrap(),
    )
    .unwrap()
}
fn fake(mode: &str) -> Result<PrivateProcessStdio> {
    let script = r#"
      const crypto=require('node:crypto'),mode=process.argv[1],kernel=JSON.parse(process.argv[2]);let bytes=Buffer.alloc(0),ready=false;
      const marker=Buffer.concat([Buffer.from('ELX_HTTP_DONE_1\n'),Buffer.from([mode==='wait'?1:0])]);
      process.stdin.on('end',()=>{if(mode==='wait')setTimeout(()=>{process.stdout.write(marker);process.exitCode=1;},150);});
      process.stdin.on('data',part=>{
        bytes=Buffer.concat([bytes,part]);if(ready||bytes.length<4||bytes.length<bytes.readUInt32BE()+4)return;ready=true;
        if(mode==='wait')return;
        const raw=bytes.subarray(4),input=JSON.parse(raw);
        const report={schema_version:1,nonce:input.nonce,request_id:'sha256:'+crypto.createHash('sha256').update(raw).digest('hex'),elapsed_millis:100,
          kernel,
          resolved_addresses:['104.16.132.229'],connected_address:'104.16.132.229',connected_port:443,source_address:'10.66.0.2',status:200,
          headers:[],body_base64:'aGVsbG8='};
        const body=Buffer.from(JSON.stringify(report)),header=Buffer.alloc(4);header.writeUInt32BE(body.length);
        process.stdout.write(Buffer.concat([header,body]));
        if(mode!=='missing')process.stdout.write(marker);
        if(mode==='trailing')process.stdout.write(Buffer.from([120]));
        if(mode==='diagnostics')process.stderr.write(Buffer.alloc(1025,120));
        process.stdin.destroy();
      });
    "#;
    let mut child = tokio::process::Command::new("node")
        .args([
            "-e",
            script,
            mode,
            &super::super::tests::kernel_fixture()?.to_string(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    Ok(PrivateProcessStdio {
        input: child.stdin.take().unwrap(),
        output: child.stdout.take().unwrap(),
        diagnostics: child.stderr.take(),
        child,
    })
}
// Exercise the same owned-call join/lifeline/slot using a fake fixed helper,
// without fabricating a production protected capability or native permission.
fn call(owner: Arc<Owner>, process: PrivateProcessStdio) -> Result<SdkHttpCall> {
    call_with_authority(owner, process, allowed())
}
fn call_with_authority(
    owner: Arc<Owner>,
    process: PrivateProcessStdio,
    authority: Arc<dyn SdkHttpAuthority>,
) -> Result<SdkHttpCall> {
    let permit = owner.slots.clone().try_acquire_owned()?;
    let (mut cancel, lifeline) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _permit = permit;
        let mut retired = false;
        let result = async {
            let request = request();
            let (bytes, diagnostic, status, cancelled) = exchange_http(
                process,
                &request.to_bytes()?,
                &owner,
                &mut cancel,
                authority.as_ref(),
            )
            .await?;
            let exit = NativeHttpExit::parse(&bytes)?;
            retired = true;
            if cancelled || !status.success() || !exit.succeeded {
                return Ok(SourceSdkReply::Unavailable);
            }
            diagnostic?;
            Ok(HttpReply::parse(
                exit.report()?.unwrap(),
                &request,
                &owner.binding.context(&owner.profile),
            )?)
        }
        .await;
        Completion {
            retired,
            result: Some(result),
        }
    });
    Ok(SdkHttpCall {
        lifeline: Some(lifeline),
        task: Some(task),
        completed: None,
        join_failed: false,
    })
}
#[tokio::test]
async fn foreground_does_not_require_background_contribution_but_keeps_resource_and_protection_gates()
-> Result<()> {
    let f = super::super::tests::fixture()?;
    f.facts
        .send_modify(|facts| facts.as_mut().unwrap().background_allowance = false);
    f.settings
        .send_modify(|settings| settings.metering = Metering::Metered);
    assert!(f.owner.admit(true).is_err());
    assert!(f.owner.admit_http(true).is_ok());
    let directory = tempfile::tempdir()?;
    f.owner
        .playback
        .insert_test_job(uuid::Uuid::new_v4(), directory.path().to_path_buf(), 0)
        .await;
    assert!(f.owner.playback.has_latency_sensitive_work());
    assert_eq!(f.owner.playback.startup_queue_len(), 0);
    f.facts
        .send_modify(|facts| facts.as_mut().unwrap().playback_pressure = true);
    assert!(f.owner.admit(true).is_err());
    assert!(
        f.owner.admit_http(true).is_ok(),
        "steady-playback background veto must not block foreground HTTP"
    );
    f.owner.playback.set_test_startup_queue_len(1);
    assert!(
        f.owner
            .admit_http(true)
            .unwrap_err()
            .to_string()
            .contains("startup or seek")
    );
    f.owner.playback.set_test_startup_queue_len(0);
    assert!(f.owner.admit_http(true).is_ok());
    for change in ["resource", "disk", "memory", "unknown_memory", "stale"] {
        let saved = f.facts.borrow().clone();
        f.facts.send_modify(|facts| {
            let facts = facts.as_mut().unwrap();
            match change {
                "resource" => facts.foreground_resource_pressure = true,
                "disk" => facts.disk_headroom = false,
                "memory" => facts.available_memory = Some(HELPER_HEADROOM - 1),
                "unknown_memory" => facts.available_memory = None,
                _ => facts.observed = Instant::now() - Duration::from_secs(31),
            }
        });
        let error = f.owner.admit_http(true).unwrap_err().to_string();
        let reason = match change {
            "resource" => "resource pressure",
            "disk" => "disk headroom",
            "memory" | "unknown_memory" => "memory headroom",
            _ => "sample is stale",
        };
        assert!(error.contains(reason), "{change}: {error}");
        f.facts.send_replace(saved);
    }
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Checking,
        until: Some(Instant::now() + Duration::from_secs(15)),
    });
    assert!(f.owner.admit_http(true).is_err());
    assert!(f.owner.admit_http(false).is_ok());
    f.settings
        .send_modify(|s| s.accepted_disclosure_version = None);
    assert!(
        f.owner.admit_http(false).is_ok(),
        "saved Off is not a contribution request"
    );
    f.settings
        .send_modify(|s| s.mode = elixir_marketplace::policy::ContributionMode::Auto);
    assert!(f.owner.admit_http(false).is_err());
    Ok(())
}
#[tokio::test]
async fn old_feature_and_shared_busy_slot_fail_before_any_helper_launch() -> Result<()> {
    let mut f = super::super::tests::fixture()?;
    Arc::get_mut(&mut f.owner).unwrap().binding.sdk_http = false;
    assert!(Client(f.owner.clone()).sdk_http().is_none());
    Arc::get_mut(&mut f.owner).unwrap().binding.sdk_http = true;
    let client = Client(f.owner.clone()).sdk_http().unwrap();
    let permit = f.owner.slots.clone().try_acquire_owned()?;
    assert!(
        client
            .start(&request().request.to_json()?, 4096, allowed())
            .is_err()
    );
    drop(permit);
    assert!(
        client
            .start(
                &RuntimeJson::from_value(json!({"operation":"secret","key":"token"}))?,
                4096,
                allowed()
            )
            .is_err()
    );
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}
#[tokio::test]
async fn cancellation_before_first_poll_does_not_launch_and_joins_original_task() -> Result<()> {
    let f = super::super::tests::fixture()?;
    f.protection
        .send_modify(|snapshot| snapshot.state = ProtectionState::Checking);
    let client = Client(f.owner.clone()).sdk_http().unwrap();
    let mut call = client.start(&request().request.to_json()?, 4096, allowed())?;
    call.cancel().await?;
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    assert!(call.task.is_none());
    assert!(call.completed.as_ref().unwrap().retired);
    Ok(())
}
#[tokio::test]
async fn full_private_report_requires_original_exit_and_has_one_consumable_result() -> Result<()> {
    let f = super::super::tests::fixture()?;
    let mut call = call(f.owner.clone(), fake("good")?)?;
    assert_eq!(f.owner.slots.available_permits(), 0);
    let SourceSdkReply::Value(value) = call.finish().await? else {
        panic!("expected HTTP reply")
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(value.as_str())?["body"],
        "hello"
    );
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(call.finish().await.is_err());
    call.cancel().await?;
    Ok(())
}
#[tokio::test]
async fn cancelled_waiter_retains_same_job_and_slot_until_actual_pipe_exit_join() -> Result<()> {
    let f = super::super::tests::fixture()?;
    let mut call = call(f.owner.clone(), fake("wait")?)?;
    // A cancelled finish future must not drop the original JoinHandle.
    assert!(
        timeout(Duration::from_millis(40), call.finish())
            .await
            .is_err()
    );
    assert!(call.task.is_some());
    assert_eq!(f.owner.slots.available_permits(), 0);
    assert!(
        timeout(Duration::from_millis(40), call.cancel())
            .await
            .is_err()
    );
    assert!(call.task.is_some());
    assert_eq!(f.owner.slots.available_permits(), 0);
    timeout(Duration::from_secs(5), call.cancel()).await??;
    assert!(call.task.is_none());
    assert!(call.completed.as_ref().unwrap().retired);
    assert_eq!(f.owner.slots.available_permits(), 1);
    Ok(())
}
#[tokio::test]
async fn protection_loss_closes_lifeline_and_waits_for_original_failure_marker() -> Result<()> {
    let f = super::super::tests::fixture()?;
    let mut call = call(f.owner.clone(), fake("wait")?)?;
    // Let the fake process receive the complete request before losing health.
    assert!(
        timeout(Duration::from_millis(100), call.finish())
            .await
            .is_err()
    );
    f.protection.send_replace(Snapshot {
        state: ProtectionState::Failed,
        until: None,
    });
    assert!(matches!(
        timeout(Duration::from_secs(5), call.finish()).await??,
        SourceSdkReply::Unavailable
    ));
    assert!(call.completed.as_ref().unwrap().retired);
    assert_eq!(f.owner.slots.available_permits(), 1);
    Ok(())
}
#[tokio::test]
async fn missing_or_trailing_native_marker_is_sticky_unconfirmed_not_an_idle_ack() -> Result<()> {
    for mode in ["missing", "trailing"] {
        let f = super::super::tests::fixture()?;
        let mut call = call(f.owner.clone(), fake(mode)?)?;
        assert!(call.finish().await.is_err());
        assert!(!call.completed.as_ref().unwrap().retired);
        assert!(call.cancel().await.is_err());
        assert!(call.finish().await.is_err());
    }
    Ok(())
}
#[tokio::test]
async fn oversized_diagnostics_reject_result_without_erasing_an_actual_retirement() -> Result<()> {
    let f = super::super::tests::fixture()?;
    let mut call = call(f.owner.clone(), fake("diagnostics")?)?;
    assert!(call.finish().await.is_err());
    assert!(call.completed.as_ref().unwrap().retired);
    call.cancel().await?;
    Ok(())
}
