use std::{collections::HashMap, path::Path, process::Stdio, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Path as AxumPath, State},
    http::{Response, StatusCode},
    routing::get,
};
use chrono::Utc;
use serde_json::json;
use tokio::{io::AsyncReadExt as _, net::TcpListener, process::Command};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::live::{
    config::{LiveProviderLimits, LiveRemuxLimits},
    crypto::SecretBytes,
    diagnostics::LiveRedactor,
    provider::LiveProviderClient,
    relay::LiveRelayService,
    session::{
        DeliveryMode, NewSession, SessionProtocol, StoredRecoveryState, StoredSessionDescriptor,
        StoredSource, TerminalReason, tests::sqlite_fixture,
    },
};

use super::{
    LiveRemuxPayloadBody, LiveRemuxService,
    profile::{CopyRemuxProfile, ffmpeg_args, ffprobe_args, parse_probe},
};

#[test]
fn m10_copy_profiles_are_loopback_only_and_cannot_generate_transcoding_arguments() {
    let output = std::env::temp_dir().join("elixir-live-remux-profile-test");
    let input = "http://127.0.0.1:45321/input";
    let probe = ffprobe_args(input).expect("loopback probe arguments");
    let remux = ffmpeg_args(CopyRemuxProfile::MpegTsToHls, input, &output, 4, 8, 2)
        .expect("copy-remux arguments");
    assert_eq!(probe.last().map(String::as_str), Some(input));
    assert!(remux.windows(2).any(|pair| pair == ["-codec", "copy"]));
    assert!(!remux.iter().any(|argument| {
        matches!(
            argument.as_str(),
            "-vf" | "-af" | "-filter" | "-filter_complex" | "libx264" | "libx265"
        )
    }));
    assert!(
        !remux
            .iter()
            .any(|argument| argument.contains("example.com"))
    );
    assert!(ffprobe_args("https://example.com/signed.ts?token=secret").is_err());
}

#[test]
fn m10_probe_accepts_copy_compatible_video_and_audio_only_and_rejects_transcode() {
    let video = br#"{"programs":[],"stream_groups":[],"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"audio","codec_name":"aac"}]}"#;
    let audio = br#"{"streams":[{"codec_type":"audio","codec_name":"aac"}]}"#;
    let incompatible = br#"{"streams":[{"codec_type":"video","codec_name":"vp9"}]}"#;
    assert!(!parse_probe(video).expect("compatible probe").audio_only);
    assert!(parse_probe(audio).expect("audio-only probe").audio_only);
    assert!(parse_probe(incompatible).is_err());
}

#[tokio::test]
async fn m10_real_mpeg_ts_copy_remux_is_fenced_secret_free_bounded_and_cleans_up() -> Result<()> {
    real_copy_remux(SessionProtocol::MpegTs).await
}

#[tokio::test]
async fn m10_real_dash_copy_remux_produces_hls_and_cleans_up() -> Result<()> {
    real_copy_remux(SessionProtocol::Dash).await
}

#[cfg(unix)]
#[tokio::test]
async fn m10_fault_paths_release_capacity_processes_and_temporary_output() -> Result<()> {
    for mode in [
        RemuxFixtureMode::DiskPressure,
        RemuxFixtureMode::StartupTimeout,
        RemuxFixtureMode::CrashAfterReady,
        RemuxFixtureMode::FrozenOutput,
    ] {
        real_copy_remux_mode(SessionProtocol::MpegTs, mode).await?;
    }
    Ok(())
}

#[tokio::test]
async fn m10_fence_takeover_reaps_the_old_job_and_restarts_the_current_session() -> Result<()> {
    real_copy_remux_mode(SessionProtocol::MpegTs, RemuxFixtureMode::FenceTakeover).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemuxFixtureMode {
    Normal,
    DiskPressure,
    StartupTimeout,
    CrashAfterReady,
    FrozenOutput,
    FenceTakeover,
}

async fn real_copy_remux(protocol: SessionProtocol) -> Result<()> {
    real_copy_remux_mode(protocol, RemuxFixtureMode::Normal).await
}

async fn real_copy_remux_mode(protocol: SessionProtocol, mode: RemuxFixtureMode) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let fixture = sqlite_fixture().await?;
    let repository = Arc::new(crate::live::session::tests::repository(&fixture)?);
    let temporary = tempfile::tempdir()?;
    let files = Arc::new(generate_origin_files(temporary.path(), protocol).await?);
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let origin_port = listener.local_addr()?.port();
    let origin_files = files.clone();
    let origin = Router::new().route(
        "/*path",
        get(move |AxumPath(path): AxumPath<String>| {
            serve_origin_file(State(origin_files.clone()), AxumPath(path))
        }),
    );
    let origin_shutdown = CancellationToken::new();
    let origin_shutdown_observer = origin_shutdown.clone();
    let origin_task = tokio::spawn(async move {
        axum::serve(listener, origin)
            .with_graceful_shutdown(origin_shutdown_observer.cancelled_owned())
            .await
    });

    let source_path = match protocol {
        SessionProtocol::MpegTs => "/live.ts",
        SessionProtocol::Dash => "/manifest.mpd",
        _ => anyhow::bail!("invalid M10 test protocol"),
    };
    sqlx::query(
        "INSERT INTO live_provider_destination_rules (
            id, home_id, provider_id, scheme, normalized_host, port, exact_path,
            network_scope, allow_fetch, allow_credentials, allow_client_disclosure,
            revision, created_by_user_id, created_by_actor_snapshot
         ) VALUES ($1, $2, $3, 'http', '127.0.0.1', $4, $5,
                   'public', TRUE, FALSE, FALSE, 1, $6, $7)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(fixture.owner.home_id.to_string())
    .bind(fixture.owner.provider_id.to_string())
    .bind(i64::from(origin_port))
    .bind(source_path)
    .bind(fixture.owner.user_id.to_string())
    .bind(json!({"userId": fixture.owner.user_id, "role": "owner"}).to_string())
    .execute(&fixture.database.pool)
    .await?;

    let now = Utc::now();
    let canary = "ELIXIR_M10_UPSTREAM_SECRET_CANARY";
    let (stream_id, stored_protocol, container) = match protocol {
        SessionProtocol::MpegTs => ("m10-mpegts", "mpeg_ts", "mpegts"),
        SessionProtocol::Dash => ("m10-dash", "dash", "mp4"),
        _ => anyhow::bail!("invalid M10 test protocol"),
    };
    let descriptor = StoredSessionDescriptor {
        provider_revision: "m10-provider-revision".to_string(),
        decision_reason: "client_protocol_requires_remux".to_string(),
        playback_url: None,
        selected_source_index: 0,
        sources: vec![StoredSource {
            stream_id: stream_id.to_string(),
            label: format!("M10 {stored_protocol}"),
            quality: Some("360p".to_string()),
            language: Some("en".to_string()),
            priority: 100,
            protocol: stored_protocol.to_string(),
            url: format!("http://127.0.0.1:{origin_port}{source_path}?sig={canary}"),
            request_headers: Vec::new(),
            cookies: Vec::new(),
            origin: None,
            referer: None,
            credential_authorities: Vec::new(),
            client_disclosure: "server_only".to_string(),
            expires_at: None,
            refresh_handle: None,
            server_egress: "not_required".to_string(),
            private_network: false,
            time_shift_available: false,
            time_shift_window_seconds: None,
            container: Some(container.to_string()),
            video_codec: Some("h264".to_string()),
            audio_codec: Some("aac".to_string()),
        }],
        egress: Default::default(),
        recovery: StoredRecoveryState::default(),
    };
    let grant = repository
        .create(
            NewSession {
                owner: fixture.owner,
                item_key: SecretBytes::from_utf8("m10-item".to_string()),
                stream_option_key: SecretBytes::from_utf8("m10-stream".to_string()),
                item_snapshot: SecretBytes::from_utf8("{\"title\":\"M10\"}".to_string()),
                descriptor: SecretBytes::new(serde_json::to_vec(&descriptor)?),
                delivery_mode: DeliveryMode::ServerRemux,
                protocol,
                source_index: 0,
                control_fencing_token: 1,
                now,
            },
            None,
        )
        .await?;
    let redactor = Arc::new(LiveRedactor::with_canaries([canary.to_string()])?);
    let provider = Arc::new(LiveProviderClient::new_for_test(
        fixture.database.pool.clone(),
        LiveProviderLimits::default(),
        redactor.clone(),
    )?);
    let relay = Arc::new(
        LiveRelayService::new_for_test(
            fixture.database.pool.clone(),
            repository.clone(),
            provider,
            crate::live::config::LiveRelayLimits::default(),
        )
        .map_err(|error| anyhow::anyhow!("relay build failed: {error:?}"))?,
    );
    let remux_root = temporary.path().join("remux");
    let mut limits = LiveRemuxLimits {
        max_concurrent: 1,
        temp_root: remux_root.to_string_lossy().into_owned(),
        probe_timeout_seconds: 10,
        startup_timeout_seconds: 15,
        no_output_timeout_seconds: 5,
        segment_seconds: 1,
        playlist_segments: 3,
        minimum_free_bytes: 64 * 1_024 * 1_024,
        ..LiveRemuxLimits::default()
    };
    if matches!(
        mode,
        RemuxFixtureMode::StartupTimeout
            | RemuxFixtureMode::CrashAfterReady
            | RemuxFixtureMode::FrozenOutput
    ) {
        let fake_ffmpeg = temporary.path().join("ffmpeg-m10-fault");
        write_fake_ffmpeg(&fake_ffmpeg, mode).await?;
        limits.ffmpeg_binary = fake_ffmpeg.to_string_lossy().into_owned();
        limits.startup_timeout_seconds = 1;
        limits.no_output_timeout_seconds = 2;
    }
    if mode == RemuxFixtureMode::DiskPressure {
        limits.temp_budget_bytes = 1;
    }
    let remux = Arc::new(LiveRemuxService::new(
        repository.clone(),
        relay,
        redactor,
        limits,
        std::time::Duration::from_secs(15),
    )?);
    remux.initialize().await?;
    let pressure_file = remux_root.join("pressure.bin");
    if mode == RemuxFixtureMode::DiskPressure {
        tokio::fs::write(&pressure_file, [0_u8; 2]).await?;
    }
    let admission = remux.admit_session(&grant.session).await;
    match mode {
        RemuxFixtureMode::DiskPressure => {
            assert_eq!(admission, Err(super::LiveRemuxError::DiskPressure));
            tokio::fs::remove_file(pressure_file).await?;
            assert_eq!(remux.available_capacity(), 1);
            assert_eq!(remux.snapshot().await.active_jobs, 0);
            origin_shutdown.cancel();
            origin_task.await??;
            return Ok(());
        }
        RemuxFixtureMode::StartupTimeout => {
            assert_eq!(admission, Err(super::LiveRemuxError::StartupTimeout));
            assert_eq!(remux.available_capacity(), 1);
            assert!(
                tokio::fs::read_dir(&remux_root)
                    .await?
                    .next_entry()
                    .await?
                    .is_none()
            );
            origin_shutdown.cancel();
            origin_task.await??;
            return Ok(());
        }
        RemuxFixtureMode::CrashAfterReady | RemuxFixtureMode::FrozenOutput => {
            admission?;
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(7);
            loop {
                let snapshot = remux.snapshot().await;
                if snapshot.active_jobs == 0
                    && snapshot.available_capacity == 1
                    && snapshot.jobs_failed == 1
                {
                    break;
                }
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "M10 {mode:?} cleanup exceeded its deadline"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert!(
                tokio::fs::read_dir(&remux_root)
                    .await?
                    .next_entry()
                    .await?
                    .is_none()
            );
            let cleared = repository
                .get_owned(fixture.owner, grant.session.id)
                .await?
                .context("failed remux session")?;
            assert!(cleared.remux_job_id.is_none());
            repository
                .terminate(
                    cleared.owner,
                    cleared.id,
                    cleared.revision,
                    cleared.control_fencing_token,
                    TerminalReason::ended(),
                    Utc::now(),
                )
                .await?;
            let process_list = Command::new("ps")
                .args(["-ax", "-o", "command="])
                .stdin(Stdio::null())
                .output()
                .await?;
            assert!(
                !String::from_utf8_lossy(&process_list.stdout)
                    .contains(remux_root.to_string_lossy().as_ref())
            );
            origin_shutdown.cancel();
            origin_task.await??;
            return Ok(());
        }
        RemuxFixtureMode::FenceTakeover => {
            admission?;
            let old = repository
                .get_owned(fixture.owner, grant.session.id)
                .await?
                .context("old-fence remux session")?;
            let old_job_id = old.remux_job_id.clone().context("old remux job ID")?;
            sqlx::query(
                "UPDATE live_control_server_leases
                 SET fencing_token = fencing_token + 1
                 WHERE lease_name = 'live-control-v1'",
            )
            .execute(&fixture.database.pool)
            .await?;
            let adopted = repository
                .adopt_control_fence(
                    old.owner,
                    old.id,
                    old.revision,
                    old.control_fencing_token,
                    old.control_fencing_token + 1,
                    Utc::now(),
                )
                .await?
                .session;
            assert!(adopted.remux_job_id.is_none());
            remux.reap_stale().await;
            assert_eq!(remux.available_capacity(), 1);
            assert_eq!(remux.snapshot().await.active_jobs, 0);
            assert!(remux.hls_manifest(&old).await.is_err());

            let reconciliation = remux
                .reconcile_startup(adopted.control_fencing_token)
                .await?;
            assert_eq!(reconciliation.inspected, 1);
            assert_eq!(reconciliation.restarted, 1);
            assert_eq!(reconciliation.terminated, 0);
            let restarted = repository
                .get_owned(fixture.owner, adopted.id)
                .await?
                .context("restarted remux session")?;
            assert!(
                restarted
                    .remux_job_id
                    .as_deref()
                    .is_some_and(|job_id| job_id != old_job_id)
            );
            assert_eq!(remux.available_capacity(), 0);
            repository
                .terminate(
                    restarted.owner,
                    restarted.id,
                    restarted.revision,
                    restarted.control_fencing_token,
                    TerminalReason::ended(),
                    Utc::now(),
                )
                .await?;
            remux.end_session(restarted.id).await?;
            assert_eq!(remux.available_capacity(), 1);
            assert!(
                tokio::fs::read_dir(&remux_root)
                    .await?
                    .next_entry()
                    .await?
                    .is_none()
            );
            origin_shutdown.cancel();
            origin_task.await??;
            return Ok(());
        }
        RemuxFixtureMode::Normal => admission?,
    }

    let bound = repository
        .get_owned(fixture.owner, grant.session.id)
        .await?
        .context("bound remux session")?;
    assert!(
        bound
            .remux_job_id
            .as_deref()
            .is_some_and(|id| id.starts_with("lrj1_"))
    );
    let manifest = remux.hls_manifest(&bound).await?;
    let manifest = match manifest.body {
        LiveRemuxPayloadBody::Bytes(body) => String::from_utf8(body)?,
        LiveRemuxPayloadBody::File { .. } => anyhow::bail!("manifest was not in memory"),
    };
    let resource_id = manifest
        .lines()
        .find_map(|line| {
            line.rsplit('/')
                .next()
                .filter(|value| value.starts_with("lr1_"))
        })
        .and_then(crate::live::relay::hls::HlsResourceId::parse)
        .context("rewritten remux resource")?;
    let segment = remux.hls_resource(&bound, &resource_id, None).await?;
    let segment_length = segment.headers["content-length"].to_str()?.parse::<u64>()?;
    let mut segment_body = match segment.body {
        LiveRemuxPayloadBody::File { file, length } => {
            assert_eq!(length, segment_length);
            file.take(length)
        }
        LiveRemuxPayloadBody::Bytes(_) => anyhow::bail!("segment was not a file stream"),
    };
    let mut prefix = [0_u8; 188];
    segment_body.read_exact(&mut prefix).await?;
    assert_eq!(prefix[0], 0x47);

    let job_directory = only_job_directory(&remux_root).await?;
    let process_list = Command::new("ps")
        .args(["-ax", "-o", "command="])
        .stdin(Stdio::null())
        .output()
        .await?;
    let process_list = String::from_utf8_lossy(&process_list.stdout);
    let command = process_list
        .lines()
        .find(|line| line.contains(job_directory.to_string_lossy().as_ref()))
        .context("running remux process")?;
    assert!(command.contains("ffmpeg"));
    assert!(command.contains("http://127.0.0.1:"));
    assert!(!command.contains(canary));
    assert!(!command.contains(&format!("{source_path}?")));
    let snapshot = remux.snapshot().await;
    assert_eq!(snapshot.active_jobs, 1);
    assert_eq!(snapshot.available_capacity, 0);
    assert!(snapshot.temp_bytes < 64 * 1_024 * 1_024);
    let plaintext: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM live_playback_sessions
         WHERE encrypted_descriptor LIKE '%ELIXIR_M10_UPSTREAM_SECRET_CANARY%'",
    )
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(plaintext, 0);

    repository
        .terminate(
            bound.owner,
            bound.id,
            bound.revision,
            bound.control_fencing_token,
            TerminalReason::ended(),
            Utc::now(),
        )
        .await?;
    remux.end_session(bound.id).await?;
    let snapshot = remux.snapshot().await;
    assert_eq!(snapshot.active_jobs, 0);
    assert_eq!(snapshot.available_capacity, 1);
    assert!(
        tokio::fs::read_dir(&remux_root)
            .await?
            .next_entry()
            .await?
            .is_none()
    );
    let process_list = Command::new("ps")
        .args(["-ax", "-o", "command="])
        .stdin(Stdio::null())
        .output()
        .await?;
    assert!(
        !String::from_utf8_lossy(&process_list.stdout)
            .contains(job_directory.to_string_lossy().as_ref())
    );

    origin_shutdown.cancel();
    origin_task.await??;
    Ok(())
}

async fn write_fake_ffmpeg(path: &Path, mode: RemuxFixtureMode) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let behavior = match mode {
            RemuxFixtureMode::StartupTimeout => {
                "trap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n"
            }
            RemuxFixtureMode::CrashAfterReady => "sleep 1\nexit 7\n",
            RemuxFixtureMode::FrozenOutput => "trap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
            RemuxFixtureMode::FenceTakeover => anyhow::bail!("invalid fake FFmpeg mode"),
            _ => anyhow::bail!("invalid fake FFmpeg mode"),
        };
        let output = if mode == RemuxFixtureMode::StartupTimeout {
            String::new()
        } else {
            "directory=${playlist%/*}\nprintf '\\107' > \"$directory/segment-0000000000.ts\"\ndd if=/dev/zero bs=187 count=1 >> \"$directory/segment-0000000000.ts\" 2>/dev/null\nprintf '#EXTM3U\\n#EXT-X-VERSION:3\\n#EXT-X-TARGETDURATION:1\\n#EXT-X-MEDIA-SEQUENCE:0\\n#EXTINF:1.0,\\nsegment-0000000000.ts\\n' > \"$playlist\"\n".to_string()
        };
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = \"-version\" ]; then echo 'ffmpeg version 8.0.1-m10-test'; exit 0; fi\nplaylist=''\nfor argument in \"$@\"; do playlist=$argument; done\n{output}{behavior}"
        );
        tokio::fs::write(path, script).await?;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        anyhow::bail!("fake FFmpeg fault fixtures require Unix")
    }
}

async fn generate_mpeg_ts(path: &Path) -> Result<()> {
    let status = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x180:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:sample_rate=48000",
            "-t",
            "20",
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-g",
            "25",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
        ])
        .arg(path)
        .stdin(Stdio::null())
        .status()
        .await?;
    anyhow::ensure!(status.success(), "failed to generate M10 MPEG-TS fixture");
    Ok(())
}

type OriginFiles = HashMap<String, (&'static str, Bytes)>;

async fn generate_origin_files(root: &Path, protocol: SessionProtocol) -> Result<OriginFiles> {
    match protocol {
        SessionProtocol::MpegTs => {
            let path = root.join("live.ts");
            generate_mpeg_ts(&path).await?;
            Ok(HashMap::from([(
                "live.ts".to_string(),
                ("video/mp2t", Bytes::from(tokio::fs::read(path).await?)),
            )]))
        }
        SessionProtocol::Dash => {
            let directory = root.join("dash");
            tokio::fs::create_dir(&directory).await?;
            generate_dash(&directory.join("manifest.mpd")).await?;
            let mut entries = tokio::fs::read_dir(&directory).await?;
            let mut files = HashMap::new();
            while let Some(entry) = entries.next_entry().await? {
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("non-UTF-8 DASH fixture file"))?;
                let content_type = if name.ends_with(".mpd") {
                    "application/dash+xml"
                } else if name.ends_with(".m4s") || name.ends_with(".mp4") {
                    "video/mp4"
                } else {
                    "application/octet-stream"
                };
                files.insert(
                    name,
                    (
                        content_type,
                        Bytes::from(tokio::fs::read(entry.path()).await?),
                    ),
                );
            }
            Ok(files)
        }
        _ => anyhow::bail!("invalid M10 test protocol"),
    }
}

async fn generate_dash(path: &Path) -> Result<()> {
    let status = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x180:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=660:sample_rate=48000",
            "-t",
            "20",
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-g",
            "25",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-f",
            "dash",
            "-seg_duration",
            "1",
            "-use_template",
            "1",
            "-use_timeline",
            "1",
        ])
        .arg(path)
        .stdin(Stdio::null())
        .status()
        .await?;
    anyhow::ensure!(status.success(), "failed to generate M10 DASH fixture");
    Ok(())
}

async fn serve_origin_file(
    State(files): State<Arc<OriginFiles>>,
    AxumPath(path): AxumPath<String>,
) -> Response<Body> {
    let Some((content_type, body)) = files.get(&path) else {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_FOUND;
        return response;
    };
    let mut response = Response::new(Body::from(body.clone()));
    response.headers_mut().insert(
        "content-type",
        content_type.parse().expect("fixture content type"),
    );
    response
}

async fn only_job_directory(root: &Path) -> Result<std::path::PathBuf> {
    let mut entries = tokio::fs::read_dir(root).await?;
    let path = entries
        .next_entry()
        .await?
        .context("remux job directory")?
        .path();
    anyhow::ensure!(
        entries.next_entry().await?.is_none(),
        "multiple remux job directories"
    );
    Ok(path)
}
