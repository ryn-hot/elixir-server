//! Actual loopback HTTP/auth + production desktop QML child. The same owned
//! provider/worker and private synthetic claims as the backend qualification;
//! no serialized owner token, public serving, billing claim or GUI shortcut.
use super::*;
use futures_util::FutureExt;
use serde_json::{Value, json};
use std::{io::Write, os::unix::fs::PermissionsExt, process::Stdio};
use tokio::{io::AsyncReadExt, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const RESULT_PREFIX: &str = "ELIXIR_MARKETPLACE_DESKTOP_RESULT ";
const LOG_BYTES: usize = 64 * 1024;
const EXCLUDES: &str = "full app navigation, physical display latency, playback, peer package transfer, real billing, 10k catalog and soak";

fn result(bytes: &[u8]) -> Result<Value> {
    ensure!(
        bytes.len() <= LOG_BYTES,
        "desktop output exceeded its bound"
    );
    let text = std::str::from_utf8(bytes)?;
    let rows: Vec<_> = text
        .lines()
        .filter_map(|line| line.strip_prefix(RESULT_PREFIX))
        .collect();
    ensure!(rows.len() == 1, "one desktop result required");
    let value: Value = serde_json::from_str(rows[0])?;
    let object = value
        .as_object()
        .context("desktop result object required")?;
    let fixed = [
        ("schema_version", json!(1)),
        ("stage", json!("protected_desktop_result")),
        ("fixture_only", json!(true)),
        ("real_http", json!(true)),
        ("normal_login", json!(true)),
        ("packaged_qml", json!(true)),
        ("software_renderer", json!(true)),
        ("catalog_rows", json!(20)),
        ("warm_samples", json!(32)),
        ("simulated_support_only", json!(true)),
        ("real_paid_credentials", json!(0)),
        ("excludes", json!(EXCLUDES)),
    ];
    let timings = [
        "setup_first_checked_data_ms",
        "setup_ready_ms",
        "setup_to_first_card_frame_ms",
        "warm_first_card_p95_ms",
        "warm_full_search_p95_ms",
        "live_filter_first_card_ms",
        "live_filter_ready_ms",
        "text_ready_ms",
        "exact_ready_ms",
        "empty_filter_ready_ms",
        "pending_replacement_ready_ms",
        "typing_frame_ms",
        "pending_typing_frame_ms",
    ];
    let expected: BTreeSet<_> = fixed
        .iter()
        .map(|(key, _)| *key)
        .chain(timings)
        .chain(["warm_first_card_1s_budget_met", "warm_200ms_budget_met"])
        .collect();
    ensure!(
        object.keys().map(String::as_str).collect::<BTreeSet<_>>() == expected,
        "desktop result fields differ"
    );
    for (key, expected) in fixed {
        ensure!(
            object.get(key) == Some(&expected),
            "desktop result scope differs"
        );
    }
    for key in timings {
        ensure!(
            object[key].as_u64().is_some_and(|ms| ms <= 180_000),
            "desktop timing invalid"
        );
    }
    ensure!(
        value["warm_first_card_1s_budget_met"]
            == json!(value["warm_first_card_p95_ms"].as_u64().unwrap() <= 1000),
        "desktop first-frame budget flag differs"
    );
    let complete = [
        "warm_full_search_p95_ms",
        "live_filter_ready_ms",
        "text_ready_ms",
        "exact_ready_ms",
        "empty_filter_ready_ms",
    ]
    .iter()
    .all(|key| value[*key].as_u64().unwrap() <= 200);
    ensure!(
        value["warm_200ms_budget_met"] == json!(complete),
        "desktop full-query budget flag differs"
    );
    Ok(value)
}

async fn log_reader<R: tokio::io::AsyncRead + Unpin>(reader: R) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(LOG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .await?;
    Ok(bytes)
}
async fn joined_log(mut task: JoinHandle<std::io::Result<Vec<u8>>>) -> Result<Vec<u8>> {
    match timeout(Duration::from_secs(10), &mut task).await {
        Ok(result) => Ok(result??),
        Err(error) => {
            task.abort();
            let _ = task.await;
            Err(error.into())
        }
    }
}
fn private_log(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join(name))?;
    file.write_all(&bytes[..bytes.len().min(LOG_BYTES)])?;
    file.sync_all()?;
    Ok(())
}
async fn run_desktop(
    executable: &Path,
    input: &[u8],
    artifacts: &Path,
    with_playback: bool,
) -> Result<Value> {
    use tokio::io::AsyncWriteExt;
    ensure!(input.len() <= 32768, "bounded desktop input required");
    let mut child = tokio::process::Command::new(executable)
        .env("QT_QPA_PLATFORM", "offscreen")
        .env("QT_QUICK_BACKEND", "software")
        .env("QT_FATAL_WARNINGS", "1")
        // The combined playback + full discovery case can legitimately exceed
        // Qt's default five minutes; stay below our unchanged 480s child bound.
        .envs(with_playback.then_some(("QTEST_FUNCTION_TIMEOUT", "450000")))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = tokio::spawn(log_reader(
        child.stdout.take().context("desktop stdout missing")?,
    ));
    let stderr = tokio::spawn(log_reader(
        child.stderr.take().context("desktop stderr missing")?,
    ));
    let operation = async {
        let mut stdin = child.stdin.take().context("desktop stdin missing")?;
        timeout(Duration::from_secs(10), stdin.write_all(input)).await??;
        stdin.shutdown().await?;
        drop(stdin); // No credential in argv, environment or public fixture files.
        let status = timeout(Duration::from_secs(480), child.wait()).await??;
        ensure!(
            status.success(),
            "original desktop process failed: {status}"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    // Retain this original Child through timeout/write failure. Never drop a
    // wait_with_output future and start a replacement process to obtain green.
    let retirement = async {
        if child.try_wait()?.is_none() {
            child.start_kill()?;
            timeout(Duration::from_secs(10), child.wait()).await??;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let out = joined_log(stdout).await;
    let err = joined_log(stderr).await;
    if let Ok(bytes) = &out {
        private_log(artifacts, "desktop-stdout.txt", bytes)?;
    }
    if let Ok(bytes) = &err {
        private_log(artifacts, "desktop-stderr.txt", bytes)?;
    }
    operation?;
    retirement?;
    let out = out?;
    let err = err?;
    ensure!(
        err.len() <= LOG_BYTES && out.len() <= LOG_BYTES,
        "desktop logs exceeded bounds"
    );
    let ui = result(&out)?;
    if with_playback {
        Ok(
            json!({"schema_version":1,"stage":"protected_playback_combined_result",
            "fixture_only":true,"ui":ui,"playback":playback_result(&out)?}),
        )
    } else {
        Ok(ui)
    }
}

fn playback_result(bytes: &[u8]) -> Result<Value> {
    const PREFIX: &str = "ELIXIR_MARKETPLACE_PLAYBACK_RESULT ";
    ensure!(bytes.len() <= LOG_BYTES, "bounded playback result required");
    let text = std::str::from_utf8(bytes)?;
    let rows: Vec<_> = text
        .lines()
        .filter_map(|line| line.strip_prefix(PREFIX))
        .collect();
    ensure!(rows.len() == 1, "one combined playback result required");
    let value: Value = serde_json::from_str(rows[0])?;
    let keys = [
        "schema_version",
        "stage",
        "fixture_only",
        "normal_playback_api",
        "actual_hls_downloads",
        "baseline_start_ms",
        "baseline_seek_ms",
        "baseline_segment_p95_ms",
        "baseline_segments",
        "loaded_start_ms",
        "loaded_seek_ms",
        "loaded_segment_p95_ms",
        "loaded_segments",
        "loaded_ui_first_card_p95_ms",
        "loaded_ui_full_search_p95_ms",
        "loaded_ui_samples",
        "loaded_filter_ms",
        "loaded_text_ms",
        "typing_frame_ms",
        "sessions_ended",
        "excludes",
    ];
    let object = value
        .as_object()
        .context("playback result object required")?;
    ensure!(
        object.keys().map(String::as_str).collect::<BTreeSet<_>>() == keys.into_iter().collect(),
        "playback result scope differs"
    );
    for (key, expected) in [
        ("schema_version", json!(1)),
        ("stage", json!("protected_playback_result")),
        ("fixture_only", json!(true)),
        ("normal_playback_api", json!(true)),
        ("actual_hls_downloads", json!(true)),
        ("loaded_ui_samples", json!(32)),
        ("sessions_ended", json!(2)),
        (
            "excludes",
            json!(
                "physical decoder/display/audio QoE, relay contribution, 10k catalog, off-site peers and full privacy/platform/soak qualification"
            ),
        ),
    ] {
        ensure!(
            value[key] == expected,
            "playback result identity or scope differs"
        );
    }
    for key in keys.into_iter().filter(|key| key.ends_with("_ms")) {
        ensure!(
            value[key].as_u64().is_some_and(|ms| ms <= 180_000),
            "bounded playback timing required"
        );
    }
    for key in ["baseline_segments", "loaded_segments"] {
        ensure!(
            value[key].as_u64().is_some_and(|n| (2..=64).contains(&n)),
            "actual distinct media deliveries required"
        );
    }
    Ok(value)
}

#[test]
fn playback_result_rejects_missing_effects_extra_fields_and_dishonest_scope() -> Result<()> {
    let mut value = json!({"schema_version":1,"stage":"protected_playback_result","fixture_only":true,
        "normal_playback_api":true,"actual_hls_downloads":true,"baseline_segments":4,"loaded_segments":8,
        "loaded_ui_samples":32,"sessions_ended":2,
        "excludes":"physical decoder/display/audio QoE, relay contribution, 10k catalog, off-site peers and full privacy/platform/soak qualification"});
    for field in [
        "baseline_start_ms",
        "baseline_seek_ms",
        "baseline_segment_p95_ms",
        "loaded_start_ms",
        "loaded_seek_ms",
        "loaded_segment_p95_ms",
        "loaded_ui_first_card_p95_ms",
        "loaded_ui_full_search_p95_ms",
        "loaded_filter_ms",
        "loaded_text_ms",
        "typing_frame_ms",
    ] {
        value[field] = json!(100);
    }
    let encoded =
        |value: &Value| format!("ELIXIR_MARKETPLACE_PLAYBACK_RESULT {value}\n").into_bytes();
    ensure!(playback_result(&encoded(&value))? == value);
    for (field, invalid) in [
        ("normal_playback_api", json!(false)),
        ("actual_hls_downloads", json!(false)),
        ("sessions_ended", json!(1)),
        ("loaded_segments", json!(0)),
        ("loaded_ui_samples", json!(31)),
        ("loaded_seek_ms", json!(-1)),
        ("loaded_start_ms", json!(180001)),
        ("excludes", json!("full privacy proven")),
    ] {
        let mut changed = value.clone();
        changed[field] = invalid;
        ensure!(playback_result(&encoded(&changed)).is_err());
    }
    let mut extra = value.clone();
    extra["grant"] = json!(true);
    ensure!(playback_result(&encoded(&extra)).is_err());
    let mut missing = value.clone();
    missing.as_object_mut().unwrap().remove("loaded_seek_ms");
    ensure!(playback_result(&encoded(&missing)).is_err());
    let bytes = encoded(&value);
    ensure!(playback_result(&[bytes.as_slice(), bytes.as_slice()].concat()).is_err());
    ensure!(playback_result(b"").is_err());
    Ok(())
}

#[test]
fn desktop_result_requires_exact_scope_and_honest_timing_flags() -> Result<()> {
    let mut value = json!({"schema_version":1,"stage":"protected_desktop_result", "fixture_only":true,
        "real_http":true,"normal_login":true,"packaged_qml":true,"software_renderer":true,
        "catalog_rows":20,"warm_samples":32,"simulated_support_only":true,"real_paid_credentials":0,
        "excludes":EXCLUDES,"warm_first_card_1s_budget_met":false,"warm_200ms_budget_met":false});
    for key in [
        "setup_first_checked_data_ms",
        "setup_ready_ms",
        "setup_to_first_card_frame_ms",
        "warm_first_card_p95_ms",
        "warm_full_search_p95_ms",
        "live_filter_first_card_ms",
        "live_filter_ready_ms",
        "text_ready_ms",
        "exact_ready_ms",
        "empty_filter_ready_ms",
        "pending_replacement_ready_ms",
        "typing_frame_ms",
        "pending_typing_frame_ms",
    ] {
        value[key] = json!(1234);
    }
    let encode = |value: &Value| format!("PASS: fixture\n{RESULT_PREFIX}{value}\n").into_bytes();
    assert!(result(&encode(&value)).is_ok());
    for (key, bad) in [
        ("real_http", json!(false)),
        ("real_paid_credentials", json!(1)),
        ("warm_samples", json!(1)),
        ("warm_200ms_budget_met", json!(true)),
        ("warm_first_card_1s_budget_met", json!(true)),
        ("typing_frame_ms", json!(-1)),
        ("unknown", json!(true)),
    ] {
        let mut changed = value.clone();
        changed[key] = bad;
        assert!(result(&encode(&changed)).is_err(), "{key}");
    }
    let twice = [encode(&value), encode(&value)].concat();
    assert!(result(&twice).is_err());
    assert!(result(&vec![b'x'; LOG_BYTES + 1]).is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit approved20 public sources and one free provider; real loopback auth/production QML child, simulated support only, no public serving or billing"]
async fn actual_protected_sources_through_real_http_and_desktop() -> Result<()> {
    actual_desktop(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit approved protected twenty-source HTTP/Qt workflow plus actual local FFmpeg/HLS baseline and loaded playback; one free identity, no new source publication or real billing"]
async fn actual_protected_desktop_during_real_playback() -> Result<()> {
    ensure!(
        std::env::var("ELIXIR_MARKETPLACE_PLAYBACK_LIVE_TEST").as_deref() == Ok("1"),
        "explicit combined real playback qualification required"
    );
    actual_desktop(true).await
}

async fn actual_desktop(with_playback: bool) -> Result<()> {
    for name in [
        "ELIXIR_MARKETPLACE_WARP_LIFECYCLE_LIVE_TEST",
        "ELIXIR_MARKETPLACE_WAN_LIVE_TEST",
        "ELIXIR_MARKETPLACE_PUBLIC_SOURCE_LIVE_TEST",
        "ELIXIR_MARKETPLACE_PUBLIC_COLD_PAGE_LIVE_TEST",
        "ELIXIR_MARKETPLACE_DESKTOP_LIVE_TEST",
    ] {
        ensure!(
            std::env::var(name).as_deref() == Ok("1"),
            "explicit desktop/provider/source consent required"
        );
    }
    let input = input().await?; // Full SDK/package/Git/license equality BEFORE enrollment.
    let executable = PathBuf::from(std::env::var("ELIXIR_MARKETPLACE_DESKTOP_EXECUTABLE")?);
    ensure!(
        executable.is_absolute(),
        "absolute desktop test executable required"
    );
    let executable = executable.canonicalize()?;
    let meta = executable.metadata()?;
    ensure!(
        meta.is_file()
            && meta.permissions().mode() & 0o111 != 0
            && executable.file_name().and_then(|name| name.to_str())
                == Some("elixir-marketplace-real-server-tests"),
        "explicit compiled desktop child required"
    );
    let provider_image = image()?;
    let guard_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_GUARD_IMAGE")?;
    let worker_image = immutable_image("ELIXIR_MARKETPLACE_PROVIDER_WORKER_IMAGE")?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(DockerRuntimeManager::new(None));
    let labels = runtime.image_labels(&worker_image).await?;
    ensure!(
        [
            "elixir.marketplace.worker.metadata_batch",
            "elixir.marketplace.worker.source_batch",
            "elixir.marketplace.worker.wan_query_budget"
        ]
        .iter()
        .all(|key| labels.get(*key).map(String::as_str) == Some("1"))
            && !labels.contains_key("elixir.marketplace.worker.source_batch_bridge_fixture"),
        "actual production-path worker required"
    );
    // Kept outside Rig's retired account root: only screenshots, test logs and
    // isolated local GUI preferences, never provider identity or private keys.
    let artifacts = tempfile::Builder::new()
        .prefix("elixir-marketplace-desktop.")
        .tempdir()?;
    // Builder's default directory mode follows the process umask. Keep every
    // retained test image/log behind an explicit owner-only directory instead.
    std::fs::set_permissions(artifacts.path(), std::fs::Permissions::from_mode(0o700))?;
    let artifacts = artifacts.keep();
    eprintln!("Desktop qualification artifacts: {}", artifacts.display());
    let mut rig = Rig::new_layout(runtime, &provider_image, true).await?;
    let stop_http = CancellationToken::new();
    let mut http = None;
    let mut media = None;
    let mut playback_manager = None;
    let observe = CancellationToken::new();
    let startup_samples = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let active_samples = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let startup_deferral = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut observation = None;
    let outcome = std::panic::AssertUnwindSafe(Box::pin(async {
        let root = rig.root.path().canonicalize()?;
        let url = format!("sqlite://{}", root.join("wan.sqlite").display());
        let (state, app, principal, token) = crate::http::handlers::marketplace::installation_http_fixture(
            rig.database.clone(), &root, &url).await?;
        let playback_ids = if with_playback {
            media = Some(super::playback::MediaFixture::prepare(&root).await?);
            let ids = media.as_ref().unwrap().seed(&state.db_pool).await?;
            playback_manager = Some(state.transcodes.clone());
            Some(ids)
        } else { None };
        super::super::installation::save_off(&app, &token).await?;
        let email: String = sqlx::query_scalar("SELECT email FROM users WHERE id=$1")
            .bind(principal.user_id.to_string()).fetch_one(&rig.database.pool).await?;
        rig.open(&guard_image, &worker_image).await.context("desktop actual protected startup")?;
        let owned = ParticipatingWorker::start(rig.worker.take().context("original protected worker missing")?,
            config(Digest::of(uuid::Uuid::new_v4().as_bytes()), false)?, rig.database.clone(), state.transcodes.clone()).await?;
        rig.application = Some(state.marketplace.clone());
        state.marketplace.attach(owned)?;
        let client = state.marketplace.client()?;
        source_ready(&client).await?;
        for (descriptor, matched) in input.descriptors.iter().zip(&input.matched) {
            let reply = client.request(PeerOperation::Offer { descriptor: Box::new(descriptor.clone()) }).await?;
            ensure!(matches!(reply.response, PeerOutcome::Offered { release_id } if release_id == matched.release_id),
                "private native test catalog differs");
        }
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let cancellation = stop_http.clone();
        http = Some(tokio::spawn(async move {
            axum::serve(listener, app).with_graceful_shutdown(cancellation.cancelled_owned()).await
        }));
        let expected: Vec<_> = input.descriptors.iter().zip(&input.matched).map(|(d, matched)| json!({
            "extension_id":d.release.payload.extension_id,"release_id":matched.release_id,"name":d.release.payload.display_name
        })).collect();
        let mut frame = json!({"schema_version":1,"fixture_only":true,
            "server_url":format!("http://{address}"),"email":email,"password":"fixture-password-only",
            "capture_directory":artifacts,"expected":expected});
        if let Some((item, file)) = playback_ids {
            frame["playback"] = json!({"media_item_id":item,"media_file_id":file});
            let manager = state.transcodes.clone();
            let stop = observe.clone();
            let starting = startup_samples.clone();
            let active = active_samples.clone();
            let deferral = startup_deferral.clone();
            let source_client = client.clone();
            let descriptor = input.descriptors[0].clone();
            observation = Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(5));
                loop { tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = interval.tick() => {
                        if manager.startup_queue_len() > 0 { starting.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                        if manager.has_latency_sensitive_work() { active.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                        if manager.startup_queue_len() > 0 && deferral.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                            // No source receipt is prewarmed before the desktop's
                            // cold page. Try once during a real start/seek, not
                            // under a manufactured positive resource watch.
                            if let Ok(metadata) = source_client.verify_metadata(&descriptor).await {
                                if manager.startup_queue_len() > 0 {
                                    match source_client.preflight_fresh_metadata(&metadata, 0).await {
                                        Err(error) => {
                                            // Public-source preflight and installed-source
                                            // quarantine have different admission errors.
                                            // Accept only the actual preflight's closed
                                            // playback reason, never an arbitrary Err.
                                            let yielded = ParticipationClient::source_verification_waiting_for_playback(&error);
                                            deferral.store(if yielded { 1 } else { 2 }, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        Ok(_) if manager.startup_queue_len() > 0 => {
                                            deferral.store(3, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        Ok(_) => {} // Startup ended during the asynchronous check; no priority claim.
                                    }
                                }
                            }
                        }
                    }
                }}
            }));
        }
        let mut result = run_desktop(&executable, &serde_json::to_vec(&frame)?, &artifacts, with_playback).await?;
        if with_playback {
            let starting = startup_samples.load(std::sync::atomic::Ordering::Relaxed);
            let active = active_samples.load(std::sync::atomic::Ordering::Relaxed);
            let (requests, bytes) = media.as_ref().context("owned media source missing")?.served();
            ensure!(startup_deferral.load(std::sync::atomic::Ordering::Relaxed) == 1,
                "fresh source verification did not demonstrably yield to actual playback startup (observation={})",
                startup_deferral.load(std::sync::atomic::Ordering::Relaxed));
            ensure!(starting > 0 && active > 0 && requests >= 4 && bytes > 64 * 1024,
                "actual playback/startup/media transfer was not observed");
            ensure!(!state.transcodes.has_latency_sensitive_work() && state.transcodes.startup_queue_len() == 0,
                "normal client did not finish its actual playback sessions");
            let ended: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM playback_sessions WHERE state='ended' AND logical_position_seconds >= 60")
                .fetch_one(&state.db_pool).await?;
            ensure!(ended == 2, "normal baseline and loaded sessions did not persist their seeks and retirement");
            let metadata = client.verify_metadata(&input.descriptors[0]).await?;
            let resumed = client.preflight_fresh_metadata(&metadata, 0).await?;
            ensure!(resumed.release_id() == input.matched[0].release_id
                && resumed.license_sha256() == input.matched[0].license.sha256
                && resumed.valid_for() <= Duration::from_secs(300),
                "mandatory fresh verification did not resume on the same owner after playback");
            result["native_observation"] = json!({"real_startup_samples":starting,"real_active_samples":active,
                "media_requests":requests,"media_bytes":bytes,"playback_retired":true,
                "fresh_source_yielded_to_actual_startup":true,"same_owner_fresh_source_resumed":true,
                "normal_sessions_ended_after_seek":ended});
        }
        let roles = client.snapshot().applied_roles.context("actual final roles missing")?;
        ensure!(roles.consumer && !roles.catalog && !roles.routing, "desktop fixture became public infrastructure");
        let settings = preferences::load(&rig.database, &principal, chrono::Utc::now()).await?;
        ensure!(settings.preferences.contribution == ContributionMode::Off && !settings.preferences.paid_support_consent,
            "desktop fixture acquired serving or paid consent");
        eprintln!("{result}");
        Ok::<(), anyhow::Error>(())
    })).catch_unwind().await;
    // The child has joined before closing its real listening socket. Always
    // finish HTTP and same-identity native recovery, including original errors.
    observe.cancel();
    let observation_retirement = if let Some(mut task) = observation.take() {
        match timeout(Duration::from_secs(10), &mut task).await {
            Ok(result) => result.map_err(anyhow::Error::from),
            Err(error) => {
                task.abort();
                let _ = task.await;
                Err(error.into())
            }
        }
    } else {
        Ok(())
    };
    if let Some(manager) = playback_manager.take() {
        manager.stop_all().await;
    }
    stop_http.cancel();
    let http_retirement = if let Some(mut task) = http {
        match timeout(Duration::from_secs(20), &mut task).await {
            Ok(result) => result
                .map_err(anyhow::Error::from)
                .and_then(|result| result.map_err(anyhow::Error::from)),
            Err(error) => {
                task.abort();
                let _ = task.await;
                Err(error.into())
            }
        }
    } else {
        Ok(())
    };
    let media_retirement = if let Some(media) = media.take() {
        media.close().await
    } else {
        Ok(())
    };
    if let Ok(Err(error)) = &outcome {
        eprintln!("Original desktop operation failed: {error:#}");
    }
    if let Err(error) = &http_retirement {
        eprintln!("Original desktop HTTP retirement failed: {error:#}");
    }
    let cleanup = rig.close().await;
    if let Err(error) = &cleanup {
        eprintln!("Original desktop same-identity cleanup failed: {error:#}");
    }
    cleanup?;
    http_retirement?;
    media_retirement?;
    observation_retirement?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
