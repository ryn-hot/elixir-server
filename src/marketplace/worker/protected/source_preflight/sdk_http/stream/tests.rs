//! Real private pipes/Node waits with synthetic helper reports. No native,
//! protection, installed-grant or application-data authority is fabricated.
use super::*;
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
fn allowed() -> Arc<Authority> {
    Arc::new(Authority(AtomicBool::new(true)))
}
fn request() -> Result<RuntimeJson> {
    RuntimeJson::from_value(serde_json::json!({
        "operation":"fetch_open","stream_id":7,"maximum_body_bytes":4*1024*1024,
        "request":{"operation":"fetch","url":"https://provider.example/api","method":"POST",
            "headers":[],"body":"fixture-body"}
    }))
    .map_err(Into::into)
}
fn fake(mode: &str) -> Result<PrivateProcessStdio> {
    let script = r#"
      const crypto=require('node:crypto'),mode=process.argv[1],kernel=JSON.parse(process.argv[2]);
      let buffer=Buffer.alloc(0),opened=false,ending=false;
      const frame=value=>{const bytes=Buffer.from(JSON.stringify(value)),header=Buffer.alloc(4);
        header.writeUInt32BE(bytes.length);process.stdout.write(Buffer.concat([header,bytes]));};
      const end=success=>{
        if(ending)return;ending=true;
        setTimeout(()=>{
          if(mode!=='missing')process.stdout.write(Buffer.concat([Buffer.from('ELX_HTTP_DONE_1\n'),Buffer.from([success?0:1])]));
          if(mode==='trailing')process.stdout.write('x');
          process.stdin.destroy();process.exitCode=success?0:1;
        },150);
      };
      process.stdin.on('end',()=>end(mode==='bad_sequence'||mode==='frame_length'));
      process.stdin.on('data',part=>{
        buffer=Buffer.concat([buffer,part]);
        while(buffer.length>=4&&buffer.length>=buffer.readUInt32BE()+4){
          const length=buffer.readUInt32BE(),raw=buffer.subarray(4,4+length),command=JSON.parse(raw);
          buffer=Buffer.from(buffer.subarray(4+length));
          const common={schema_version:1,sequence:command.sequence};
          if(command.operation==='open'){
            if(opened)throw new Error('second original request');opened=true;
            const http=command.http;
            frame({...common,operation:'opened',context:{schema_version:1,nonce:http.nonce,
              request_id:'sha256:'+crypto.createHash('sha256').update(JSON.stringify(http)).digest('hex'),
              kernel,resolved_addresses:['104.16.132.229'],
              connected_address:'104.16.132.229',connected_port:443,source_address:'10.66.0.2',status:200,headers:[]}});
          }else if(command.operation==='read'){
            if(mode==='held')continue;
            if(mode==='partial'){const h=Buffer.alloc(4);h.writeUInt32BE(1024);process.stdout.write(Buffer.concat([h,Buffer.from('part')]));continue;}
            if(mode==='frame_length'){process.stdout.write(Buffer.from([255,255,255,255]));continue;}
            frame({...common,sequence:command.sequence+(mode==='bad_sequence'?1:0),operation:'chunk',chunk:'é\n',done:true});
          }else{
            frame({...common,operation:'closed'});
            if(mode==='extra_output')frame({unexpected:true});
            if(mode==='diagnostics')process.stderr.write(Buffer.alloc(1025,120));
            end(true);
          }
        }
      });
    "#;
    let mut child = tokio::process::Command::new("node")
        .args([
            "-e",
            script,
            mode,
            &super::super::super::tests::kernel_fixture()?.to_string(),
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
fn call(
    owner: Arc<Owner>,
    mode: &str,
    authority: Arc<dyn SdkHttpAuthority>,
) -> Result<SdkHttpStreamCall> {
    let process = fake(mode)?;
    let permit = owner.slots.clone().try_acquire_owned()?;
    let mut protocol = StreamProtocol::new(
        &request()?,
        4096,
        Digest::of(b"stream-fixture"),
        owner.binding.family,
    )?;
    let opening = Zeroizing::new(protocol.open()?);
    let (mut cancel, lifeline) = oneshot::channel();
    let (send_headers, headers) = oneshot::channel();
    let (commands, receive) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let _permit = permit;
        let mut retired = false;
        let mut fault = FaultOnDrop {
            owner: owner.clone(),
            armed: true,
        };
        let result = async {
            let reply = StreamExchange {
                protocol: &mut protocol,
                opening: &opening,
                owner: &owner,
                cancel: &mut cancel,
                authority: authority.as_ref(),
                headers: send_headers,
                commands: receive,
            }
            .run(process, &mut retired)
            .await?;
            // TEST ONLY: the production wrapper independently inspects the
            // actual worker/grant before releasing this same deferred reply.
            acknowledge_close(reply)
        }
        .await;
        if retired {
            fault.armed = false;
        }
        StreamCompletion { retired, result }
    });
    Ok(SdkHttpStreamCall {
        id: 7,
        lifeline: Some(lifeline),
        headers: Some(headers),
        commands,
        task: Some(task),
        completed: None,
        join_failed: false,
    })
}
fn value(reply: SourceSdkReply) -> Result<serde_json::Value> {
    let SourceSdkReply::Value(value) = reply else {
        bail!("expected fixture value")
    };
    Ok(serde_json::from_str(value.as_str())?)
}

#[tokio::test]
async fn chunks_stay_on_one_original_and_close_waits_for_actual_process_retirement() -> Result<()> {
    let f = super::super::super::tests::fixture()?;
    let mut call = call(f.owner.clone(), "good", allowed())?;
    assert_eq!(call.id, 7);
    assert_eq!(value(call.headers().await?)?["stream_id"], 7);
    assert_eq!(
        value(call.read(4096).await?)?,
        serde_json::json!({"stream_id":7,"chunk":"é\n","done":true})
    );
    assert_eq!(f.owner.slots.available_permits(), 0);
    // Fake helper has sent its ACK but deliberately remains alive for 150ms.
    assert!(
        timeout(Duration::from_millis(40), call.close())
            .await
            .is_err()
    );
    assert!(call.task.is_some());
    assert_eq!(f.owner.slots.available_permits(), 0);
    // Cancel the waiter, not the original retirement task or its slot.
    timeout(Duration::from_secs(5), call.cancel()).await??;
    assert!(call.completed.as_ref().unwrap().retired);
    assert_eq!(f.owner.slots.available_permits(), 1);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}
#[tokio::test]
async fn completed_close_ack_is_one_consumable_value_after_the_original_join() -> Result<()> {
    let f = super::super::super::tests::fixture()?;
    let mut call = call(f.owner.clone(), "good", allowed())?;
    call.headers().await?;
    call.read(4096).await?;
    assert_eq!(value(call.close().await?)?, serde_json::json!(true));
    assert!(call.task.is_none());
    assert!(call.close().await.is_err());
    assert!(call.read(4096).await.is_err());
    call.cancel().await?;
    Ok(())
}
#[tokio::test]
async fn cancelled_read_and_cancel_waiters_retain_the_original_task_until_its_marker() -> Result<()>
{
    for mode in ["held", "partial"] {
        let f = super::super::super::tests::fixture()?;
        let mut call = call(f.owner.clone(), mode, allowed())?;
        call.headers().await?;
        assert!(
            timeout(Duration::from_millis(40), call.read(4096))
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(40), call.cancel())
                .await
                .is_err()
        );
        assert!(call.task.is_some());
        assert_eq!(f.owner.slots.available_permits(), 0);
        timeout(Duration::from_secs(5), call.cancel()).await??;
        assert!(call.completed.as_ref().unwrap().retired);
        assert_eq!(f.owner.slots.available_permits(), 1);
        assert!(!f.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}
#[tokio::test]
async fn protection_loss_and_revocation_retire_held_io_without_faulting_the_shared_owner()
-> Result<()> {
    for revoke in [false, true] {
        let f = super::super::super::tests::fixture()?;
        let authority = allowed();
        let mut call = call(f.owner.clone(), "held", authority.clone())?;
        call.headers().await?;
        assert!(
            timeout(Duration::from_millis(40), call.read(4096))
                .await
                .is_err()
        );
        if revoke {
            authority.0.store(false, Ordering::SeqCst);
        } else {
            f.protection.send_replace(Snapshot {
                state: ProtectionState::Failed,
                until: None,
            });
        }
        assert!(matches!(
            timeout(Duration::from_secs(5), call.read(4096)).await??,
            SourceSdkReply::Unavailable
        ));
        call.cancel().await?;
        assert!(call.completed.as_ref().unwrap().retired);
        assert!(!f.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}
#[tokio::test]
async fn absent_or_trailing_native_marker_never_acknowledges_idle_even_after_cli_exit() -> Result<()>
{
    for mode in ["missing", "trailing"] {
        let f = super::super::super::tests::fixture()?;
        let mut call = call(f.owner.clone(), mode, allowed())?;
        call.headers().await?;
        call.read(4096).await?;
        assert!(call.close().await.is_err());
        assert!(call.cancel().await.is_err());
        assert!(call.cancel().await.is_err());
        assert!(!call.completed.as_ref().unwrap().retired);
        assert!(f.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}
#[tokio::test]
async fn invalid_successful_helper_cannot_return_data_or_erase_actual_retirement() -> Result<()> {
    for mode in [
        "bad_sequence",
        "frame_length",
        "extra_output",
        "diagnostics",
    ] {
        let f = super::super::super::tests::fixture()?;
        let mut call = call(f.owner.clone(), mode, allowed())?;
        call.headers().await?;
        if matches!(mode, "bad_sequence" | "frame_length") {
            assert!(matches!(
                call.read(4096).await?,
                SourceSdkReply::Unavailable
            ));
        } else {
            call.read(4096).await?;
            assert!(matches!(call.close().await?, SourceSdkReply::Unavailable));
        }
        call.cancel().await?;
        assert!(call.completed.as_ref().unwrap().retired);
        assert!(f.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}
#[tokio::test]
async fn feature_busy_slot_and_prelaunch_cancellation_cannot_launch_another_helper() -> Result<()> {
    let mut f = super::super::super::tests::fixture()?;
    Arc::get_mut(&mut f.owner).unwrap().binding.sdk_http_stream = false;
    let client = SdkHttpClient(f.owner.clone());
    assert!(client.start_stream(&request()?, 4096, allowed()).is_err());
    drop(client);
    Arc::get_mut(&mut f.owner).unwrap().binding.sdk_http_stream = true;
    Arc::get_mut(&mut f.owner).unwrap().binding.sdk_http = false;
    let client = SdkHttpClient(f.owner.clone());
    assert!(client.start_stream(&request()?, 4096, allowed()).is_err());
    drop(client);
    Arc::get_mut(&mut f.owner).unwrap().binding.sdk_http = true;
    let permit = f.owner.slots.clone().try_acquire_owned()?;
    let client = SdkHttpClient(f.owner.clone());
    assert!(client.start_stream(&request()?, 4096, allowed()).is_err());
    drop(permit);
    let mut call = client.start_stream(&request()?, 4096, allowed())?;
    // No task poll/native launch yet; fixture runtime would fail any such I/O.
    call.cancel().await?;
    assert!(call.completed.as_ref().unwrap().retired);
    assert!(!f.owner.failed.load(Ordering::SeqCst));
    Ok(())
}
