//! Private-pipe/scheduling tests with synthetic process reports, not proof of
//! protected public retrieval. Only Binding::capture constructs production
//! contexts from the actual owned guard.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use elixir_marketplace::{
    canonical,
    policy::ContributionMode,
    records::{Publisher, SignedRecord},
    security::AuthorizedPublisher,
};
use std::process::Stdio;

// Synthetic reports must match the SAME profile used by this private fixture.
// This is not an observed kernel context or production admission evidence.
pub(super) fn kernel_fixture() -> Result<serde_json::Value> {
    let limits = FixedWorkerSpec::source_preflight_fixture()?.limits;
    Ok(serde_json::json!({
        "memory_bytes":limits.memory_bytes,"swap_bytes":0,
        "cpu_quota_us":u64::from(limits.cpu_millicores)*100,"cpu_period_us":100_000,
        "pids":64,"network_namespace_inode":12345,"seccomp_mode":2,
    }))
}

pub(super) struct Fixture {
    pub(super) owner: Arc<Owner>,
    pub(super) protection: watch::Sender<Snapshot>,
    pub(super) settings: watch::Sender<ParticipationSettings>,
    pub(super) facts: watch::Sender<Option<Facts>>,
    pub(super) faults: mpsc::Receiver<()>,
}
pub(super) fn fixture() -> Result<Fixture> {
    let (protection, p) = watch::channel(Snapshot::timed(
        ProtectionState::Ready,
        Duration::from_secs(15),
    ));
    let (settings, s) = watch::channel(ParticipationSettings {
        revision: Some(1),
        mode: ContributionMode::Off,
        metering: Metering::Unmetered,
        accepted_disclosure_version: Some(PARTICIPATION_DISCLOSURE_VERSION),
    });
    let (facts, f) = watch::channel(Some(Facts {
        observed: Instant::now(),
        available_memory: Some(1024 * 1024 * 1024),
        disk_headroom: true,
        resource_pressure: false,
        foreground_resource_pressure: false,
        playback_pressure: false,
        background_allowance: true,
    }));
    let (faults, failed) = mpsc::channel(1);
    sqlx::any::install_default_drivers();
    let playback = Arc::new(PlaybackJobManager::new(
        sqlx::any::AnyPoolOptions::new().connect_lazy("sqlite::memory:")?,
        None,
    ));
    let owner = Arc::new(Owner {
        runtime: Arc::new(crate::runtime::docker::DockerRuntimeManager::new(Some(
            "/must-not-run-source-fixture".into(),
        ))),
        handle: ContainerHandle {
            id: "a".repeat(64),
            name: "source-fixture".into(),
        },
        profile: FixedWorkerSpec::source_preflight_fixture()?,
        binding: Binding {
            radicle_source: true,
            namespace: 12345,
            addresses: vec!["10.66.0.2".parse()?],
            family: ProbeFamily::Ipv4,
            issuer_keys: vec![vector()["issuer_key"].as_str().unwrap().parse()?],
            metadata_verifier: true,
            metadata_batch: true,
            source_batch: true,
            source_batch_diagnostics: false,
            catalog_head_snapshot: false,
            catalog_head_batch: false,
            security_verifier: true,
            evidence_verifier: true,
            publication_verifier: true,
            sdk_http: true,
            sdk_http_stream: true,
        },
        protection: p,
        settings: s,
        facts: f,
        playback,
        faults,
        failed: AtomicBool::new(false),
        slots: Arc::new(Semaphore::new(1)),
        source_cache: Mutex::new(cache::SourceCache::default()),
        search_source_cache: Mutex::new(search_source::SearchSourceCache::default()),
        metadata_cache: Mutex::new(metadata::ReceiptCache::default()),
        ranking_cache: Mutex::new(evidence::ScoreCache::default()),
    });
    Ok(Fixture {
        owner,
        protection,
        settings,
        facts,
        faults: failed,
    })
}
fn vector() -> serde_json::Value {
    serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/elixir_marketplace/tests/fixtures/protocol-v1.json"
    )))
    .unwrap()
}
#[tokio::test]
async fn explicit_saved_off_preserves_foreground_reads_but_never_background_contribution()
-> Result<()> {
    let f = fixture()?;
    f.settings
        .send_modify(|s| s.accepted_disclosure_version = None);
    let client = Client(f.owner.clone());
    assert!(Work::Foreground.admit(&f.owner, true).is_ok());
    assert!(Work::Background.admit(&f.owner, true).is_err());
    assert!(client.catalog_preparation_ready()?);
    f.owner.playback.set_test_startup_queue_len(1);
    assert!(!client.catalog_preparation_ready()?);
    assert!(Work::Foreground.admit(&f.owner, true).is_err());
    f.owner.playback.set_test_startup_queue_len(0);
    f.settings.send_modify(|s| s.revision = None);
    assert!(Work::Foreground.admit(&f.owner, true).is_err());
    f.settings.send_modify(|s| {
        s.revision = Some(1);
        s.mode = ContributionMode::Auto;
    });
    assert!(Work::Foreground.admit(&f.owner, true).is_err());
    f.settings.send_modify(|s| s.mode = ContributionMode::On);
    assert!(Work::Foreground.admit(&f.owner, true).is_err());
    Ok(())
}
fn request() -> Result<(Release, SourcePreflightRequest, Vec<u8>)> {
    let mut release =
        SignedRecord::<Release>::parse(&serde_json::to_vec(&vector()["release"])?)?.payload;
    let manifest = serde_json::to_vec(
        &serde_json::json!({"id":release.extension_id,"name":release.display_name,"kind":release.kind,"version":release.version}),
    )?;
    release.manifest_sha256 = Digest::of(&manifest);
    let request = SourcePreflightRequest::for_release(
        &release,
        0,
        Digest::of(b"source-fixture-nonce"),
        ProbeFamily::Ipv4,
    )?;
    Ok((release, request, manifest))
}
fn fake(mode: &str, manifest: &[u8]) -> Result<PrivateProcessStdio> {
    let script = r#"
      const crypto=require('node:crypto'),mode=process.argv[1],manifest=process.argv[2],kernel=JSON.parse(process.argv[3]);let bytes=Buffer.alloc(0),done=false;
      process.stdin.on('end',()=>{if(mode==='cancel_wait')setTimeout(()=>{process.exitCode=1;},150);
        if(mode==='radicle_cancel'){process.stdout.write(Buffer.concat([Buffer.alloc(4),Buffer.from('ELXRADICLEEXIT1\0')]));}
        if(mode==='finite'){const header=Buffer.alloc(4);header.writeUInt32BE(2);process.stdout.write(Buffer.concat([header,Buffer.from('ok')]));}});
      process.stdin.on('data',part=>{
        bytes=Buffer.concat([bytes,part]);if(done||bytes.length<4||bytes.length<bytes.readUInt32BE()+4)return;done=true;
        if(mode==='stall'||mode==='cancel_wait'||mode==='finite')return;
        if(mode==='radicle_cancel')return;
        if(mode.startsWith('radicle_')){
          process.stdout.write(Buffer.alloc(4));
          if(mode==='radicle_known'||mode==='radicle_extra')process.stdout.write(Buffer.from('ELXRADICLEEXIT1\0'));
          if(mode==='radicle_extra')process.stdout.write(Buffer.from([0]));
          if(mode==='radicle_crash')process.exitCode=1;
          process.stdin.destroy();return;
        }
        if(mode==='unavailable'){process.exitCode=1;process.stdin.destroy();return;}
        const request=JSON.parse(bytes.subarray(4)),plan=request.plan;
        const file=(target,body)=>({url:target.url,resolved_addresses:['104.16.132.229'],connected_address:'104.16.132.229',source_address:'10.66.0.2',body_base64:body});
        let reply={schema_version:1,nonce:request.nonce,plan_id:'sha256:'+crypto.createHash('sha256').update(JSON.stringify(plan)).digest('hex'),elapsed_millis:100,
          kernel,
          manifest:file(plan.access.manifest,manifest),license:file(plan.access.license,Buffer.from('SPDX-License-Identifier: MIT\n').toString('base64'))};
        if(mode==='nonce')reply.nonce='sha256:'+'0'.repeat(64);
        if(mode==='namespace')reply.kernel.network_namespace_inode++;
        if(mode==='cpu')reply.kernel.cpu_quota_us++;
        let body=Buffer.from(JSON.stringify(reply));if(mode==='duplicate')body=Buffer.from(JSON.stringify(reply).replace('{','{"schema_version":1,'));
        const header=Buffer.alloc(4);header.writeUInt32BE(mode==='oversize'?131073:body.length);
        const frame=Buffer.concat([header,body]);process.stdout.write(mode==='partial'?frame.subarray(0,8):frame);
        if(mode==='extra')process.stdout.write(Buffer.from([0]));if(mode==='diagnostics')process.stderr.write(Buffer.alloc(1025,120));
        process.stdin.destroy();
      });
    "#;
    let mut child = tokio::process::Command::new("node")
        .args([
            "-e",
            script,
            mode,
            &STANDARD.encode(manifest),
            &kernel_fixture()?.to_string(),
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

#[tokio::test]
async fn radicle_unavailable_is_retired_only_with_the_native_cleanup_witness() -> Result<()> {
    let fixture = fixture()?;
    let (_, request, manifest) = request()?;
    let bytes = canonical::to_vec(&request)?;
    for (mode, known) in [
        ("radicle_known", true),
        ("radicle_missing", false),
        ("radicle_crash", false),
        ("radicle_extra", false),
    ] {
        let (mut completion, _response) = oneshot::channel::<Result<()>>();
        let result = timeout(
            Duration::from_secs(5),
            exchange_bytes_witness(
                fake(mode, &manifest)?,
                &bytes,
                4096,
                &fixture.owner,
                &mut completion,
                false,
                Work::Background,
                true,
            ),
        )
        .await?;
        if known {
            assert!(result?.is_err());
        } else {
            assert!(result.is_err(), "{mode}");
        }
    }
    Ok(())
}
#[tokio::test]
async fn complete_private_report_requires_clean_exit_exact_context_and_bounded_streams()
-> Result<()> {
    let fixture = fixture()?;
    let (release, request, manifest) = request()?;
    let (mut completion, _response) = oneshot::channel();
    let result = timeout(
        Duration::from_secs(5),
        exchange(
            fake("good", &manifest)?,
            &request,
            &release,
            0,
            &fixture.owner,
            &mut completion,
            Work::Background,
        ),
    )
    .await???;
    assert_eq!(
        result.license_sha256()?,
        Digest::of(b"SPDX-License-Identifier: MIT\n")
    );
    let unavailable = exchange(
        fake("unavailable", &manifest)?,
        &request,
        &release,
        0,
        &fixture.owner,
        &mut completion,
        Work::Background,
    )
    .await?;
    assert!(unavailable.is_err());
    assert!(fixture.owner.admit(true).is_ok());
    for mode in [
        "nonce",
        "namespace",
        "cpu",
        "duplicate",
        "oversize",
        "partial",
        "extra",
        "diagnostics",
    ] {
        assert!(
            timeout(
                Duration::from_secs(5),
                exchange(
                    fake(mode, &manifest)?,
                    &request,
                    &release,
                    0,
                    &fixture.owner,
                    &mut completion,
                    Work::Background,
                )
            )
            .await?
            .is_err(),
            "{mode}"
        );
    }
    Ok(())
}
#[tokio::test]
async fn rejected_publisher_content_does_not_poison_the_worker_but_bad_native_evidence_does()
-> Result<()> {
    for mode in ["good", "nonce"] {
        let mut fixture = fixture()?;
        let (mut release, _, _) = request()?;
        release.media_tags.clear();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "id":release.extension_id,"name":release.display_name,"kind":release.kind,
            "version":release.version,"provides":[{"capability":"acquisition.candidate_provider"}],
        }))?;
        release.manifest_sha256 = Digest::of(&manifest);
        let request = SourcePreflightRequest::for_release(
            &release,
            0,
            Digest::of(b"category policy fixture"),
            ProbeFamily::Ipv4,
        )?;
        let (mut completion, _response) = oneshot::channel();
        // SAME guard/disarm rule used by Owner::run after actual native exit.
        let mut cleanup = FaultOnDrop {
            owner: fixture.owner.clone(),
            armed: true,
        };
        let result = timeout(
            Duration::from_secs(5),
            exchange(
                fake(mode, &manifest)?,
                &request,
                &release,
                0,
                &fixture.owner,
                &mut completion,
                Work::Background,
            ),
        )
        .await?;
        if mode == "good" {
            assert!(
                result?.is_err(),
                "invalid publisher content cannot mint a source fact"
            );
            cleanup.armed = false;
        } else {
            assert!(
                result.is_err(),
                "bad native binding must retain the outer fail-stop path"
            );
        }
        drop(cleanup);
        assert_eq!(fixture.owner.failed.load(Ordering::SeqCst), mode == "nonce");
        assert_eq!(fixture.faults.try_recv().is_ok(), mode == "nonce");
        if mode == "good" {
            fixture.owner.admit(true)?;
            let (release, request, manifest) = self::request()?;
            assert!(
                exchange(
                    fake("good", &manifest)?,
                    &request,
                    &release,
                    0,
                    &fixture.owner,
                    &mut completion,
                    Work::Background
                )
                .await??
                .license_sha256()
                .is_ok(),
                "a later valid source can use the same healthy owner"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_a_caller_keeps_its_slot_until_actual_helper_retirement() -> Result<()> {
    let fixture = fixture()?;
    let owner = fixture.owner.clone();
    let (release, request, manifest) = request()?;
    let permit = owner.slots.clone().try_acquire_owned()?;
    let (mut completion, response) = oneshot::channel();
    let (started, begin) = oneshot::channel();
    let process = fake("cancel_wait", &manifest)?;
    let retired = tokio::spawn(async move {
        let _permit = permit;
        let _ = started.send(());
        exchange(
            process,
            &request,
            &release,
            0,
            &owner,
            &mut completion,
            Work::Background,
        )
        .await
    });
    begin.await?;
    tokio::time::sleep(Duration::from_millis(75)).await;
    drop(response);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        fixture.owner.slots.clone().try_acquire_owned().is_err(),
        "cancelled caller must not free the live helper slot"
    );
    assert!(timeout(Duration::from_secs(5), retired).await???.is_err());
    assert!(fixture.owner.slots.clone().try_acquire_owned().is_ok());
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn finite_metadata_input_is_really_closed_before_waiting_for_a_reply() -> Result<()> {
    let fixture = fixture()?;
    let (release, request, manifest) = request()?;
    let (mut completion, _response) = oneshot::channel::<Result<()>>();
    let reply = timeout(
        Duration::from_secs(5),
        exchange_bytes(
            fake("finite", &manifest)?,
            &request.to_bytes(&release, 0)?,
            4096,
            &fixture.owner,
            &mut completion,
            true,
            Work::Background,
        ),
    )
    .await???;
    assert_eq!(reply, b"ok");
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn foreground_source_pipe_preserves_purpose_when_background_work_is_blocked() -> Result<()> {
    let fixture = fixture()?;
    fixture
        .settings
        .send_modify(|settings| settings.metering = Metering::Metered);
    fixture.facts.send_modify(|facts| {
        let facts = facts.as_mut().unwrap();
        facts.playback_pressure = true;
        facts.background_allowance = false;
    });
    assert!(Work::Background.admit(&fixture.owner, true).is_err());
    Work::Foreground.admit(&fixture.owner, true)?;
    let (release, request, manifest) = request()?;
    let (mut completion, _response) = oneshot::channel();
    let result = timeout(
        Duration::from_secs(5),
        exchange(
            fake("good", &manifest)?,
            &request,
            &release,
            0,
            &fixture.owner,
            &mut completion,
            Work::Foreground,
        ),
    )
    .await???;
    assert_eq!(
        result.license_sha256()?,
        Digest::of(b"SPDX-License-Identifier: MIT\n")
    );
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

fn input_probe(witness: &std::path::Path) -> Result<PrivateProcessStdio> {
    let script = "const fs=require('node:fs');process.stdin.on('data',()=>fs.writeFileSync(process.argv[1],'received'));process.stdin.on('end',()=>{const h=Buffer.alloc(4);h.writeUInt32BE(2);process.stdout.write(Buffer.concat([h,Buffer.from('ok')]));});";
    let mut child = tokio::process::Command::new("node")
        .args(["-e", script])
        .arg(witness)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    Ok(PrivateProcessStdio {
        input: child.stdin.take().context("input")?,
        output: child.stdout.take().context("output")?,
        diagnostics: child.stderr.take(),
        child,
    })
}

#[tokio::test]
async fn foreground_input_rechecks_live_startup_before_sending_any_frame() -> Result<()> {
    let fixture = fixture()?;
    let directory = tempfile::tempdir()?;
    let witness = directory.path().join("unexpected-input");
    let process = input_probe(&witness)?;
    fixture.owner.playback.set_test_startup_queue_len(1);
    let (mut completion, _response) = oneshot::channel::<Result<()>>();
    let outcome = timeout(
        Duration::from_secs(5),
        exchange_bytes(
            process,
            b"must not be sent",
            4096,
            &fixture.owner,
            &mut completion,
            true,
            Work::Foreground,
        ),
    )
    .await??;
    fixture.owner.playback.set_test_startup_queue_len(0);
    assert!(outcome.is_err());
    assert!(
        !witness.exists(),
        "lost admission sent input to the original helper"
    );
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn foreground_launched_helper_receives_no_input_until_the_original_protection_is_ready()
-> Result<()> {
    let fixture = fixture()?;
    fixture.protection.send_replace(Snapshot::timed(
        ProtectionState::Checking,
        Duration::from_secs(15),
    ));
    let directory = tempfile::tempdir()?;
    let witness = directory.path().join("input");
    let process = input_probe(&witness)?;
    let owner = fixture.owner.clone();
    let permit = owner.slots.clone().try_acquire_owned()?;
    let (mut completion, _response) = oneshot::channel::<Result<()>>();
    let task = tokio::spawn(async move {
        let _permit = permit;
        exchange_bytes(
            process,
            b"ready only",
            4096,
            &owner,
            &mut completion,
            true,
            Work::Foreground,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished());
    assert!(!witness.exists(), "Checking sent a helper input frame");
    assert_eq!(fixture.owner.slots.available_permits(), 0);
    fixture.protection.send_replace(Snapshot::timed(
        ProtectionState::Ready,
        Duration::from_secs(15),
    ));
    assert_eq!(timeout(Duration::from_secs(5), task).await????, b"ok");
    assert!(witness.exists());
    assert_eq!(fixture.owner.slots.available_permits(), 1);
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn foreground_waiting_input_cancellation_or_protection_loss_joins_without_sending()
-> Result<()> {
    for cancel in [true, false] {
        let fixture = fixture()?;
        fixture.protection.send_replace(Snapshot::timed(
            ProtectionState::Checking,
            Duration::from_secs(15),
        ));
        let directory = tempfile::tempdir()?;
        let witness = directory.path().join("unexpected-input");
        let process = input_probe(&witness)?;
        let owner = fixture.owner.clone();
        let permit = owner.slots.clone().try_acquire_owned()?;
        let (mut completion, response) = oneshot::channel::<Result<()>>();
        let task = tokio::spawn(async move {
            let _permit = permit;
            exchange_bytes(
                process,
                b"must not send",
                4096,
                &owner,
                &mut completion,
                true,
                Work::Foreground,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(!task.is_finished());
        assert_eq!(fixture.owner.slots.available_permits(), 0);
        if cancel {
            drop(response);
        } else {
            fixture
                .protection
                .send_replace(Snapshot::inactive(ProtectionState::Failed));
        }
        assert!(timeout(Duration::from_secs(5), task).await???.is_err());
        assert!(!witness.exists());
        assert_eq!(fixture.owner.slots.available_permits(), 1);
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}

#[tokio::test]
async fn foreground_inflight_startup_or_resource_loss_joins_the_original_pipe() -> Result<()> {
    for startup in [true, false] {
        let fixture = fixture()?;
        let owner = fixture.owner.clone();
        let (release, request, manifest) = request()?;
        let permit = owner.slots.clone().try_acquire_owned()?;
        let (mut completion, _response) = oneshot::channel();
        let process = fake("cancel_wait", &manifest)?;
        let task = tokio::spawn(async move {
            let _permit = permit;
            exchange(
                process,
                &request,
                &release,
                0,
                &owner,
                &mut completion,
                Work::Foreground,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(75)).await;
        if startup {
            fixture.owner.playback.set_test_startup_queue_len(1);
        } else {
            fixture
                .facts
                .send_modify(|f| f.as_mut().unwrap().foreground_resource_pressure = true);
        }
        assert!(fixture.owner.slots.clone().try_acquire_owned().is_err());
        let joined = timeout(Duration::from_secs(5), task).await???;
        fixture.owner.playback.set_test_startup_queue_len(0);
        assert!(joined.is_err());
        assert!(fixture.owner.slots.clone().try_acquire_owned().is_ok());
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "exclusive offline actual native metadata/security/cgroup verification with synthetic protection and scheduling facts; not public source, billing or media qualification"]
async fn native_foreground_metadata_keeps_crypto_and_limits_while_background_is_blocked()
-> Result<()> {
    use futures_util::FutureExt;
    let runtime: Arc<dyn RuntimeManager> =
        Arc::new(crate::runtime::docker::DockerRuntimeManager::new(None));
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable image required")?;
    anyhow::ensure!(
        crate::marketplace::runtime::image_is_pinned(&image),
        "immutable image required"
    );
    anyhow::ensure!(
        runtime
            .image_labels(&image)
            .await?
            .get("elixir.marketplace.worker.metadata_batch")
            .map(String::as_str)
            == Some("1"),
        "native batch qualification requires its actual immutable image capability"
    );
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?;
    let deployment = format!("foreground-meta-{}", uuid::Uuid::new_v4().simple());
    let (descriptor, _, _) = evidence::tests::fixture()?;
    let issuer = descriptor.gate_evidence[0].credential.payload.issuer_key_id;
    let worker = crate::marketplace::worker::OfflineWorker::start(
        runtime.as_ref(),
        &directory.join("controller"),
        &deployment,
        &image,
        &[issuer],
        crate::marketplace::packet::PacketWorkloadBudget::split(
            &elixir_marketplace::policy::OrdinaryLimits::default(),
        )?
        .worker_limits(),
    )
    .await?;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut fixture = fixture()?;
        let owner = Arc::get_mut(&mut fixture.owner).context("original native fixture owner")?;
        owner.runtime = runtime.clone();
        owner.handle = worker.handle().clone();
        owner.profile = worker.profile().clone();
        owner.binding.issuer_keys = vec![issuer];
        fixture.settings.send_modify(|s| {
            s.metering = Metering::Metered;
            s.accepted_disclosure_version = None; // explicitly saved Off, not contribution consent
        });
        fixture.facts.send_modify(|f| {
            let f = f.as_mut().unwrap();
            f.playback_pressure = true;
            f.background_allowance = false;
        });
        // This fixture has no real guard. Keep its explicitly synthetic owner
        // alive across serial native helpers; this is not VPN qualification.
        fixture.protection.send_modify(|s| {
            *s = Snapshot::timed(s.state, Duration::from_secs(300));
        });
        assert!(Work::Background.admit(&fixture.owner, true).is_err());
        let client = Client(fixture.owner.clone());
        let cold_started = std::time::Instant::now();
        let metadata = client.verify_metadata(&descriptor).await?;
        let cold_elapsed = cold_started.elapsed();
        assert!(metadata.is_current());
        assert_eq!(
            metadata.support()?.verified_supporters,
            elixir_marketplace::MIN_DISCOVERY_SUPPORTERS
        );
        assert_eq!(metadata.descriptor().to_bytes()?, descriptor.to_bytes()?);
        let initial_valid_for = metadata.valid_for();
        let notice = SignedRecord::sign(
            elixir_marketplace::records::Revocation {
                root_key: descriptor.publisher.payload.root_key,
                extension_id: descriptor.release.payload.extension_id.clone(),
                release_ids: vec![Digest::of(canonical::to_vec(&descriptor.release.payload)?)],
                release_keys: vec![],
                reason: "offline native withdrawal qualification".into(),
            },
            None,
            &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
        )?;
        let records = elixir_marketplace::security_job::SecurityRecords {
            schema_version: 1,
            extension_id: descriptor.release.payload.extension_id.clone(),
            publisher: descriptor.publisher.clone(),
            head: None,
            revocations: vec![notice.clone()],
        };
        // A checked local cache read starts no helper and remains usable during
        // playback startup. NEW native work must still yield before launch.
        let permit = fixture.owner.slots.clone().try_acquire_owned()?;
        let warm_started = std::time::Instant::now();
        for _ in 0..20 {
            assert!(client.verify_metadata(&descriptor).await?.is_current());
        }
        eprintln!(
            "native_metadata_probe cold_ms={} warm_20_us={} scope=offline_fixed_worker_synthetic_admission",
            cold_elapsed.as_millis(),
            warm_started.elapsed().as_micros()
        );
        fixture.owner.playback.set_test_startup_queue_len(1);
        let cached = client.verify_metadata(&descriptor).await?;
        assert!(cached.valid_for() <= initial_valid_for);
        let mut cold = descriptor.clone();
        cold.head.payload.sequence += 1; // different input, not a trusted receipt
        let blocked = client.verify_metadata(&cold).await;
        let blocked_security = client.verify_owner_security(&records).await;
        fixture.owner.playback.set_test_startup_queue_len(0);
        assert!(
            blocked
                .err()
                .context("new metadata helper ignored playback startup")?
                .to_string()
                .contains("playback startup")
        );
        assert!(
            blocked_security
                .err()
                .context("new security helper ignored playback startup")?
                .to_string()
                .contains("playback startup")
        );
        drop(permit);

        // Authenticate a real root notice through the actual fixed native entry,
        // not a fixture-created SecurityObservation or a peer JSON assertion.
        assert!(client.verify_security(&records).await.is_err());
        fixture
            .facts
            .send_modify(|f| f.as_mut().unwrap().observed = Instant::now());
        let security = client.verify_owner_security(&records).await?;
        assert_eq!(
            security.summary()?.revocation_ids,
            vec![Digest::of(notice.to_bytes()?)]
        );
        assert!(security.summary()?.head.is_none());
        assert_eq!(
            canonical::to_vec(security.records())?,
            canonical::to_vec(&records)?
        );
        let mut forged = records;
        forged.revocations[0]
            .payload
            .reason
            .push_str(" changed without root signature");
        fixture
            .facts
            .send_modify(|f| f.as_mut().unwrap().observed = Instant::now());
        assert!(client.verify_owner_security(&forged).await.is_err());
        assert!(!fixture.owner.failed.load(Ordering::SeqCst));

        // SDK-generated, metadata-only public TEST keys/claims, not actual
        // public repositories or source/install eligibility. Twenty distinct
        // extension IDs compare equal cold inputs through serial vs finite
        // native helpers under the SAME actual fixed cgroup.
        let generated = timeout(Duration::from_secs(5), tokio::process::Command::new("node")
            .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".."))
            .args(["--input-type=module", "-e", "import {metadataFixture} from './crates/elixir_marketplace/worker/test/index-fixture.mjs'; process.stdout.write(JSON.stringify(Array.from({length:20},(_,i)=>metadataFixture(i))));"])
            .kill_on_drop(true).output()).await??;
        anyhow::ensure!(generated.status.success() && generated.stdout.len() <= elixir_marketplace::MAX_CONTROL_FRAME_BYTES,
            "bounded SDK metadata fixture generation failed");
        let inputs: Vec<elixir_marketplace::admission::CandidateDescriptor> =
            canonical::decode(&generated.stdout, elixir_marketplace::MAX_CONTROL_FRAME_BYTES)?;
        assert_eq!(inputs.len(),20);
        let serial_started = std::time::Instant::now();
        for input in &inputs {
            fixture.facts.send_modify(|f| f.as_mut().unwrap().observed = Instant::now());
            assert!(client.verify_metadata(input).await?.is_current());
        }
        let serial_elapsed = serial_started.elapsed();
        *fixture.owner.metadata_cache.lock().unwrap() = metadata::ReceiptCache::default();
        fixture.facts.send_modify(|f| f.as_mut().unwrap().observed = Instant::now());
        let batch_started = std::time::Instant::now();
        client.warm_metadata(&inputs).await?;
        let batch_elapsed = batch_started.elapsed();
        let _held = fixture.owner.slots.clone().try_acquire_owned()?;
        let warm_started = std::time::Instant::now();
        let mut origin = None;
        for input in &inputs {
            let value = client.verify_metadata(input).await?;
            assert!(value.is_current());
            assert_eq!(value.support()?.verified_supporters,elixir_marketplace::MIN_DISCOVERY_SUPPORTERS);
            let timestamps = value.observation_times();
            if let Some(origin) = origin { assert_eq!(timestamps,origin); }
            origin = Some(timestamps);
        }
        eprintln!("native_metadata_batch_probe serial_20_ms={} batch_20_ms={} warm_20_us={} scope=offline_fixed_worker_synthetic_admission",
            serial_elapsed.as_millis(),batch_elapsed.as_millis(),warm_started.elapsed().as_micros());
        assert!(batch_elapsed < serial_elapsed,"finite batching must reduce the measured equal-input cold work");
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    worker.stop(runtime.as_ref()).await?;
    crate::marketplace::preparation::remove_native_activation_host(runtime.as_ref(), &deployment)
        .await?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn a_current_source_cache_hit_uses_no_job_and_cannot_bypass_a_failed_owner() -> Result<()> {
    let fixture = fixture()?;
    let value = vector();
    let publisher = AuthorizedPublisher::verify(&SignedRecord::<Publisher>::parse(
        &serde_json::to_vec(&value["publisher"])?,
    )?)?;
    let release = AuthorizedRelease::verify(
        &SignedRecord::parse(&serde_json::to_vec(&value["release"])?)?,
        &publisher,
        &publisher,
    )?;
    let plan = SourcePreflightRequest::for_release(
        release.payload(),
        0,
        Digest::of(b"test cache request"),
        ProbeFamily::Ipv4,
    )?
    .plan;
    // Synthetic cached availability only, not positive public-fetch evidence.
    let observation = PublicSourceObservation {
        release_id: plan.release_id,
        location_id: plan.location_id,
        license_sha256: Digest::of(b"SPDX-License-Identifier: MIT\n"),
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection: fixture.owner.protection.clone(),
    };
    fixture
        .owner
        .source_cache
        .lock()
        .unwrap()
        .insert(&observation);
    let _permit = fixture.owner.slots.clone().try_acquire_owned()?;
    let client = Client(fixture.owner.clone());
    let cached = client.request(&release, 0).await?;
    assert_eq!(cached.observed, observation.observed);
    assert_eq!(cached.license_sha256(), observation.license_sha256());
    assert_eq!(fixture.owner.slots.available_permits(), 0);
    fixture
        .settings
        .send_modify(|s| s.metering = Metering::Metered);
    fixture.facts.send_modify(|f| {
        let f = f.as_mut().unwrap();
        f.playback_pressure = true;
        f.background_allowance = false;
    });
    assert!(
        client.request(&release, 0).await.is_err(),
        "background cache access changed purpose"
    );
    let foreground = client
        .request_release(release.payload(), 0, Work::Foreground, true)
        .await?;
    assert_eq!(
        foreground.observed, observation.observed,
        "foreground cache hit renewed its TTL"
    );
    fixture
        .settings
        .send_modify(|s| s.metering = Metering::Unmetered);
    fixture.facts.send_modify(|f| {
        let f = f.as_mut().unwrap();
        f.playback_pressure = false;
        f.background_allowance = true;
    });
    fixture.owner.failed.store(true, Ordering::SeqCst);
    assert!(client.request(&release, 0).await.is_err());
    fixture.owner.failed.store(false, Ordering::SeqCst);
    fixture
        .owner
        .source_cache
        .lock()
        .unwrap()
        .entries_for_test_clear();
    let error = client.request(&release, 0).await.err().unwrap();
    assert!(error.to_string().contains("slot is busy"));
    Ok(())
}
#[tokio::test]
async fn protection_or_resource_loss_closes_the_lifeline_but_waits_for_exit() -> Result<()> {
    for lose_protection in [true, false] {
        let fixture = fixture()?;
        let owner = fixture.owner.clone();
        let (release, request, manifest) = request()?;
        let (mut completion, _response) = oneshot::channel();
        let process = fake("cancel_wait", &manifest)?;
        let task = tokio::spawn(async move {
            exchange(
                process,
                &request,
                &release,
                0,
                &owner,
                &mut completion,
                Work::Background,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(75)).await;
        if lose_protection {
            fixture
                .protection
                .send_replace(Snapshot::inactive(ProtectionState::Expired));
        } else {
            fixture
                .facts
                .send_modify(|facts| facts.as_mut().unwrap().playback_pressure = true);
        }
        assert!(timeout(Duration::from_secs(5), task).await???.is_err());
    }
    Ok(())
}
#[tokio::test]
async fn unknown_cleanup_trips_a_shared_fail_stop_and_does_not_create_an_observation() -> Result<()>
{
    let mut fixture = fixture()?;
    let (release, request, manifest) = request()?;
    let (mut completion, _response) = oneshot::channel();
    let cleanup = FaultOnDrop {
        owner: fixture.owner.clone(),
        armed: true,
    };
    assert!(
        timeout(
            Duration::from_millis(150),
            exchange(
                fake("stall", &manifest)?,
                &request,
                &release,
                0,
                &fixture.owner,
                &mut completion,
                Work::Background,
            )
        )
        .await
        .is_err()
    );
    drop(cleanup);
    assert_eq!(fixture.faults.try_recv(), Ok(()));
    assert!(fixture.owner.admit(true).is_err());
    assert!(
        Client(fixture.owner.clone())
            .0
            .failed
            .load(Ordering::SeqCst)
    );
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn background_admission_never_treats_unknown_metering_stale_samples_or_playback_as_foreground()
-> Result<()> {
    let fixture = fixture()?;
    assert!(fixture.owner.admit(true).is_ok());
    fixture
        .settings
        .send_modify(|settings| settings.metering = Metering::Unknown);
    assert!(fixture.owner.admit(true).is_err());
    fixture
        .settings
        .send_modify(|settings| settings.metering = Metering::Unmetered);
    fixture.protection.send_replace(Snapshot::timed(
        ProtectionState::Checking,
        Duration::from_secs(100),
    ));
    assert!(fixture.owner.admit(true).is_err());
    assert!(fixture.owner.admit(false).is_ok());
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(fixture.owner.admit(false).is_err());
    Ok(())
}
#[test]
fn memory_is_reserved_inside_the_existing_worker_cap_without_growing_it() {
    let limit = 224 * 1024 * 1024;
    let snapshot = |used: u64, inactive: u64, dirty: u64| {
        format!("{used}\ninactive_file {inactive}\nfile_dirty {dirty}\nfile_writeback 0\n")
    };
    assert!(memory_headroom(&snapshot(limit - HELPER_HEADROOM, 0, 0), limit).is_ok());
    assert!(memory_headroom(&snapshot(limit, HELPER_HEADROOM, 0), limit).is_ok());
    assert!(memory_headroom(&snapshot(limit, HELPER_HEADROOM, 1), limit).is_err());
    for value in [
        "0".into(),
        "max".into(),
        "1 2".into(),
        "18446744073709551615".into(),
        snapshot(limit - HELPER_HEADROOM + 1, 0, 0),
        format!("{}inactive_file 0\n", snapshot(1, 0, 0)),
        snapshot(1, 2, 0),
        snapshot(limit + 1, HELPER_HEADROOM, 0),
    ] {
        assert!(memory_headroom(&value, limit).is_err(), "{value}");
    }
}

#[tokio::test]
async fn metadata_jobs_share_the_source_slot_and_fail_closed_without_native_feature() -> Result<()>
{
    let mut fixture = fixture()?;
    let data = vector();
    let descriptor =
        elixir_marketplace::admission::CandidateDescriptor::parse(&serde_json::to_vec(
            &serde_json::json!({"schema_version":1,"publisher":data["publisher"],
            "release":data["release"],"head":data["head"],"revocations":[],
            "gate_evidence":data["evidence"]["pairs"]}),
        )?)?;
    let client = Client(fixture.owner.clone());
    let permit = fixture.owner.slots.clone().try_acquire_owned()?;
    let error = client.verify_metadata(&descriptor).await.err().unwrap();
    assert!(error.to_string().contains("slot is busy"));
    drop(permit);
    drop(client);
    Arc::get_mut(&mut fixture.owner)
        .unwrap()
        .binding
        .metadata_verifier = false;
    assert!(
        Client(fixture.owner.clone())
            .catalog_preparation_ready()
            .is_err()
    );
    let error = Client(fixture.owner.clone())
        .verify_metadata(&descriptor)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("verification is unavailable"));
    assert!(!fixture.owner.failed.load(Ordering::SeqCst));
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn availability_tokens_expire_and_cannot_survive_their_protection_owner() -> Result<()> {
    let fixture = fixture()?;
    let value = vector();
    let publisher = AuthorizedPublisher::verify(&SignedRecord::<Publisher>::parse(
        &serde_json::to_vec(&value["publisher"])?,
    )?)?;
    let release = AuthorizedRelease::verify(
        &SignedRecord::<Release>::parse(&serde_json::to_vec(&value["release"])?)?,
        &publisher,
        &publisher,
    )?;
    let observation = PublicSourceObservation {
        release_id: release.release_id(),
        location_id: Digest::of(b"fixture"),
        license_sha256: Digest::of(b"MIT"),
        observed: Instant::now(),
        observed_utc: observation_utc()?,
        protection: fixture.owner.protection.clone(),
    };
    assert!(observation.is_current_for(&release));
    fixture
        .protection
        .send_replace(Snapshot::inactive(ProtectionState::Failed));
    assert!(!observation.is_current_for(&release));
    fixture.protection.send_replace(Snapshot::timed(
        ProtectionState::Ready,
        Duration::from_secs(600),
    ));
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(!observation.is_current_for(&release));
    Ok(())
}
