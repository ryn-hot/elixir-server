use std::{path::Path, time::UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::{self, Body, Bytes},
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose};
use elixir_server::{
    artwork::ArtworkService,
    auth::AuthService,
    config::{
        AuthConfig, ClassifierConfig, DatabaseConfig, LibraryConfig, RunEnvironment, SecretsConfig,
        ServerConfig, Settings, TelemetryConfig,
    },
    db::Database,
    extensions::ExtensionManager,
    http::router,
    library::LinkerService,
    metadata::MetadataService,
    secrets::SecretsManager,
    state::AppState,
};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tower::ServiceExt;
use uuid::Uuid;

struct PlaybackFixture {
    app: Router,
    state: AppState,
    token: String,
    item_id: String,
    media_file_id: String,
    _dir: TempDir,
}

struct MatrixCase {
    name: &'static str,
    file_name: &'static str,
    container: &'static str,
    video_codec: &'static str,
    audio_codec: &'static str,
    width: i32,
    height: i32,
    bitrate_bps: i64,
    subtitle: Option<SubtitleProbe>,
    network_type: &'static str,
    client_capabilities: Value,
    expected_mode: &'static str,
    expected_delivery: &'static str,
    configure: fn(&mut Settings),
}

#[derive(Clone, Copy)]
struct SubtitleProbe {
    codec: &'static str,
    kind: &'static str,
}

fn no_extra_config(_: &mut Settings) {}

fn enable_adaptive(settings: &mut Settings) {
    settings.playback.allow_adaptive_transcode = true;
    settings.playback.adaptive_quality_enabled = true;
}

fn test_settings_with_db(_root: &Path) -> Settings {
    let master_key = general_purpose::STANDARD.encode([0u8; 32]);
    Settings {
        environment: RunEnvironment::Development,
        server: ServerConfig::default(),
        database: DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        },
        library: LibraryConfig::default(),
        extensions: elixir_server::config::ExtensionsConfig::default(),
        auth: AuthConfig::default(),
        secrets: SecretsConfig {
            master_key: Some(master_key),
        },
        telemetry: TelemetryConfig::default(),
        metadata: elixir_server::config::MetadataConfig::default(),
        classifier: ClassifierConfig::default(),
        playback: elixir_server::config::PlaybackConfig::default(),
        media_interactions: elixir_server::config::MediaInteractionsConfig::default(),
        live: elixir_server::live::config::LiveConfig::default(),
        network: elixir_server::config::NetworkConfig::default(),
    }
}

async fn create_playback_test_schema(pool: &sqlx::AnyPool) -> Result<()> {
    for statement in [
        "CREATE TABLE users (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE server_instances (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            device_name TEXT NOT NULL,
            lan_addresses TEXT NOT NULL,
            wan_direct_endpoint TEXT,
            overlay_endpoint TEXT,
            last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE server_registry (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            server_id TEXT NOT NULL,
            device_name TEXT NOT NULL,
            lan_addresses TEXT NOT NULL,
            wan_direct_endpoint TEXT,
            overlay_endpoint TEXT,
            status TEXT NOT NULL DEFAULT 'online',
            last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE media_items (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            external_ids TEXT,
            title TEXT NOT NULL,
            year INTEGER,
            season INTEGER,
            episode INTEGER,
            runtime_seconds INTEGER,
            metadata_json TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE movies (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            year INTEGER,
            external_imdb TEXT,
            external_tmdb TEXT,
            metadata_json TEXT,
            runtime_seconds INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE media_files (
            id TEXT PRIMARY KEY,
            media_item_id TEXT NOT NULL,
            source_config_id TEXT,
            path TEXT NOT NULL UNIQUE,
            size_bytes BIGINT,
            container TEXT,
            video_codec TEXT,
            audio_codec TEXT,
            width INTEGER,
            height INTEGER,
            bitrate_bps BIGINT,
            hash TEXT,
            scan_state TEXT NOT NULL DEFAULT 'ok',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE movie_files (
            movie_id TEXT NOT NULL,
            media_file_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (movie_id, media_file_id)
        )",
        "CREATE TABLE external_subtitles (
            id TEXT PRIMARY KEY,
            media_file_id TEXT NOT NULL,
            path TEXT NOT NULL,
            language TEXT,
            title TEXT,
            format TEXT,
            is_default BOOLEAN NOT NULL DEFAULT 0,
            is_forced BOOLEAN NOT NULL DEFAULT 0,
            is_hearing_impaired BOOLEAN NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE media_file_probes (
            media_file_id TEXT PRIMARY KEY,
            probe_version INTEGER NOT NULL,
            ffprobe_version TEXT,
            probe_status TEXT NOT NULL,
            probed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            source_mtime_ms BIGINT,
            source_size_bytes BIGINT,
            normalized_json TEXT,
            raw_json TEXT,
            error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE playback_sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            server_id TEXT,
            media_file_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'active',
            network_type TEXT,
            logical_position_seconds REAL NOT NULL DEFAULT 0,
            duration_seconds INTEGER,
            client_capabilities TEXT,
            transcode_state TEXT,
            token TEXT,
            playback_plan_json TEXT,
            job_state_json TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

async fn setup_fixture(case: &MatrixCase) -> Result<PlaybackFixture> {
    let dir = tempdir()?;
    let mut settings = test_settings_with_db(dir.path());
    settings.auth.access_token_secret = format!("phase15-{}-secret", case.name);
    (case.configure)(&mut settings);

    let database = Database::connect(&settings.database).await?;
    create_playback_test_schema(&database.pool).await?;
    let auth_service = AuthService::new(settings.auth.clone())?;
    let metadata = MetadataService::new(elixir_server::config::MetadataConfig::default())?;
    let linkers = LinkerService::new(settings.classifier.clone())?;
    let artwork = ArtworkService::new(
        settings.library.artwork_cache_dir.clone(),
        settings.metadata.request_timeout_seconds,
    )?;
    let secrets = SecretsManager::from_settings(&settings)?;
    let state = AppState::new(
        settings,
        database,
        auth_service,
        ExtensionManager::new(),
        metadata,
        linkers,
        artwork,
        secrets,
    );
    let app = router(state.clone());

    let file_path = dir.path().join(case.file_name);
    tokio::fs::write(&file_path, b"0123456789").await?;

    let title = format!("Phase15 {}", case.name);
    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)")
        .bind(user_id.to_string())
        .bind(format!("phase15-{}@example.com", case.name))
        .bind("hashed")
        .execute(&state.db_pool)
        .await?;
    let token = state.auth_service.issue_access_token(user_id)?.token;

    let item_id = Uuid::new_v4().to_string();
    let media_file_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO media_items (id, type, external_ids, title, year, runtime_seconds)
         VALUES (?, 'movie', '{}', ?, 2026, 60)",
    )
    .bind(&item_id)
    .bind(&title)
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO movies (id, title, year, runtime_seconds)
         VALUES (?, ?, 2026, 60)",
    )
    .bind(&item_id)
    .bind(&title)
    .execute(&state.db_pool)
    .await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
         VALUES (?, ?, ?, 10, ?, ?, ?, ?, ?, ?, 'ok')",
    )
    .bind(&media_file_id)
    .bind(&item_id)
    .bind(file_path.to_string_lossy().to_string())
    .bind(case.container)
    .bind(case.video_codec)
    .bind(case.audio_codec)
    .bind(case.width)
    .bind(case.height)
    .bind(case.bitrate_bps)
    .execute(&state.db_pool)
    .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?)")
        .bind(&item_id)
        .bind(&media_file_id)
        .execute(&state.db_pool)
        .await?;

    seed_media_probe(
        &state.db_pool,
        &media_file_id,
        &file_path,
        case.container,
        case.video_codec,
        case.audio_codec,
        case.width,
        case.height,
        case.bitrate_bps,
        case.subtitle,
    )
    .await?;

    Ok(PlaybackFixture {
        app,
        state,
        token,
        item_id,
        media_file_id,
        _dir: dir,
    })
}

async fn seed_media_probe(
    pool: &sqlx::AnyPool,
    media_file_id: &str,
    path: &Path,
    container: &str,
    video_codec: &str,
    audio_codec: &str,
    width: i32,
    height: i32,
    bitrate_bps: i64,
    subtitle: Option<SubtitleProbe>,
) -> Result<()> {
    let metadata = tokio::fs::metadata(path).await?;
    let source_size_bytes = metadata.len() as i64;
    let source_mtime_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64);
    let subtitle_streams = subtitle
        .map(|subtitle| {
            vec![json!({
                "index": 2,
                "codec": subtitle.codec,
                "kind": subtitle.kind,
                "language": "eng",
                "title": "English",
                "is_default": true,
                "is_forced": false,
                "is_hearing_impaired": false,
                "external_path": null
            })]
        })
        .unwrap_or_default();
    let normalized = json!({
        "probe_version": 1,
        "ffprobe_version": "phase15-fixture",
        "probe_status": "ok",
        "probe_error": null,
        "probed_at": null,
        "path": path.to_string_lossy(),
        "container": {
            "format_names": [container],
            "canonical": container,
            "major_brand": null,
            "compatible_brands": []
        },
        "duration_seconds": 60.0,
        "size_bytes": source_size_bytes,
        "overall_bitrate_bps": bitrate_bps,
        "start_time_seconds": 0.0,
        "video_streams": [{
            "index": 0,
            "codec": video_codec,
            "profile": "High",
            "level": 41,
            "pixel_format": "yuv420p",
            "width": width,
            "height": height,
            "frame_rate": 24.0,
            "bit_depth": 8,
            "bitrate_bps": bitrate_bps,
            "color_primaries": null,
            "color_transfer": null,
            "color_matrix": null,
            "hdr10": false,
            "hdr10_plus": false,
            "dolby_vision": false,
            "mastering_metadata": false,
            "content_light_metadata": false,
            "dolby_vision_profile": null,
            "dolby_vision_has_hdr10_fallback": false,
            "is_default": true,
            "is_forced": false
        }],
        "audio_streams": [{
            "index": 1,
            "codec": audio_codec,
            "profile": null,
            "channels": 2,
            "channel_layout": "stereo",
            "sample_rate": 48000,
            "bitrate_bps": 192000,
            "language": "eng",
            "title": null,
            "is_default": true,
            "is_forced": false
        }],
        "subtitle_streams": subtitle_streams,
        "chapters_present": false,
        "attachments_present": false
    });

    sqlx::query("DELETE FROM media_file_probes WHERE media_file_id = ?")
        .bind(media_file_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO media_file_probes
            (media_file_id, probe_version, ffprobe_version, probe_status, probed_at,
             source_mtime_ms, source_size_bytes, normalized_json, raw_json, error,
             created_at, updated_at)
         VALUES (?, 1, 'phase15-fixture', 'ok', CURRENT_TIMESTAMP, ?, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(media_file_id)
    .bind(source_mtime_ms)
    .bind(source_size_bytes)
    .bind(normalized.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

fn play_body(case: &MatrixCase, fixture: &PlaybackFixture) -> Value {
    let mut body = json!({
        "media_item_id": fixture.item_id,
        "preferred_file_id": fixture.media_file_id,
        "network_type": case.network_type,
        "client_capabilities": case.client_capabilities
    });
    if case.subtitle.is_some() {
        body["subtitle_stream_index"] = json!(2);
    }
    body
}

async fn post_play(
    app: &Router,
    token: Option<&str>,
    body_value: Value,
) -> Result<(StatusCode, Value)> {
    let mut request = Request::post("/api/v1/play").header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body_value.to_string()))?)
        .await?;
    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        json!({
            "raw": String::from_utf8_lossy(&bytes)
        })
    });
    Ok((status, json))
}

fn stream_url(play_json: &Value) -> Result<&str> {
    play_json
        .get("stream_url")
        .or_else(|| play_json.get("streamUrl"))
        .and_then(Value::as_str)
        .context("stream_url")
}

fn session_id(play_json: &Value) -> Result<&str> {
    play_json
        .get("session_id")
        .or_else(|| play_json.get("sessionId"))
        .and_then(Value::as_str)
        .context("session_id")
}

fn query_param(url: &str, key: &str) -> Option<String> {
    let query_and_fragment = url.split_once('?')?.1;
    let query = query_and_fragment
        .split_once('#')
        .map_or(query_and_fragment, |(query, _)| query);
    query.split('&').find_map(|pair| {
        let (pair_key, value) = pair.split_once('=')?;
        (pair_key == key).then(|| value.to_string())
    })
}

#[tokio::test]
async fn playback_session_matrix_covers_all_planner_modes_through_play_route() -> Result<()> {
    let browser = json!({
        "profile_version": 1,
        "client_kind": "web",
        "direct_play_preferred": false,
        "supported_containers": ["mp4"],
        "supported_video_codecs": ["h264"],
        "supported_audio_codecs": ["aac"],
        "supported_subtitle_codecs": [],
        "supported_hls_segment_types": ["fmp4"],
        "quality_mode": "fixed",
        "max_bitrate_bps": 20_000_000
    });
    let cases = vec![
        MatrixCase {
            name: "direct_play",
            file_name: "Phase15.Direct.Play.2026.mkv",
            container: "mkv",
            video_codec: "h264",
            audio_codec: "aac",
            width: 1280,
            height: 720,
            bitrate_bps: 2_000_000,
            subtitle: None,
            network_type: "lan",
            client_capabilities: json!({
                "profile_version": 1,
                "client_kind": "native_mpv",
                "direct_play_preferred": true,
                "supported_containers": ["mkv"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_subtitle_codecs": ["srt", "webvtt", "ass"],
                "quality_mode": "original"
            }),
            expected_mode: "direct_play",
            expected_delivery: "direct_file",
            configure: no_extra_config,
        },
        MatrixCase {
            name: "direct_stream",
            file_name: "Phase15.Direct.Stream.2026.mkv",
            container: "mkv",
            video_codec: "h264",
            audio_codec: "aac",
            width: 1280,
            height: 720,
            bitrate_bps: 2_000_000,
            subtitle: None,
            network_type: "lan",
            client_capabilities: browser.clone(),
            expected_mode: "direct_stream",
            expected_delivery: "hls_fmp4",
            configure: no_extra_config,
        },
        MatrixCase {
            name: "audio_transcode",
            file_name: "Phase15.Audio.Transcode.2026.mkv",
            container: "mkv",
            video_codec: "h264",
            audio_codec: "dts",
            width: 1280,
            height: 720,
            bitrate_bps: 3_000_000,
            subtitle: None,
            network_type: "lan",
            client_capabilities: browser.clone(),
            expected_mode: "audio_transcode",
            expected_delivery: "hls_fmp4",
            configure: no_extra_config,
        },
        MatrixCase {
            name: "subtitle_transcode",
            file_name: "Phase15.Subtitle.Transcode.2026.mp4",
            container: "mp4",
            video_codec: "h264",
            audio_codec: "aac",
            width: 1280,
            height: 720,
            bitrate_bps: 2_000_000,
            subtitle: Some(SubtitleProbe {
                codec: "ass",
                kind: "text",
            }),
            network_type: "lan",
            client_capabilities: json!({
                "profile_version": 1,
                "client_kind": "web",
                "direct_play_preferred": false,
                "supported_containers": ["mp4"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_subtitle_codecs": ["webvtt"],
                "supported_hls_segment_types": ["fmp4"],
                "subtitle_rendering": "hls_webvtt",
                "ass_complexity_support": "simple_webvtt",
                "quality_mode": "fixed",
                "max_bitrate_bps": 20_000_000
            }),
            expected_mode: "subtitle_transcode",
            expected_delivery: "hls_fmp4",
            configure: no_extra_config,
        },
        MatrixCase {
            name: "video_transcode",
            file_name: "Phase15.Video.Transcode.2026.mkv",
            container: "mkv",
            video_codec: "h264",
            audio_codec: "aac",
            width: 1920,
            height: 1080,
            bitrate_bps: 8_000_000,
            subtitle: None,
            network_type: "wan",
            client_capabilities: json!({
                "profile_version": 1,
                "client_kind": "web",
                "direct_play_preferred": false,
                "supported_containers": ["mp4"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_hls_segment_types": ["fmp4"],
                "quality_mode": "fixed",
                "max_bitrate_bps": 3_000_000
            }),
            expected_mode: "video_transcode",
            expected_delivery: "hls_fmp4",
            configure: no_extra_config,
        },
        MatrixCase {
            name: "adaptive_transcode",
            file_name: "Phase15.Adaptive.Transcode.2026.mkv",
            container: "mkv",
            video_codec: "h264",
            audio_codec: "aac",
            width: 1920,
            height: 1080,
            bitrate_bps: 8_000_000,
            subtitle: None,
            network_type: "wan",
            client_capabilities: json!({
                "profile_version": 1,
                "client_kind": "web",
                "direct_play_preferred": false,
                "supported_containers": ["mp4"],
                "supported_video_codecs": ["h264"],
                "supported_audio_codecs": ["aac"],
                "supported_hls_segment_types": ["fmp4"],
                "quality_mode": "automatic",
                "max_bitrate_bps": 20_000_000
            }),
            expected_mode: "adaptive_transcode",
            expected_delivery: "hls_adaptive_fmp4",
            configure: enable_adaptive,
        },
    ];

    for case in &cases {
        let fixture = setup_fixture(case).await?;
        let unauthenticated = post_play(&fixture.app, None, play_body(case, &fixture)).await?;
        assert_eq!(
            unauthenticated.0,
            StatusCode::UNAUTHORIZED,
            "{} should require auth",
            case.name
        );

        let (status, play_json) = post_play(
            &fixture.app,
            Some(&fixture.token),
            play_body(case, &fixture),
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "{} play body: {play_json}",
            case.name
        );
        assert_eq!(
            play_json.get("mode").and_then(Value::as_str),
            Some(case.expected_mode),
            "{}",
            play_json
        );
        assert_eq!(
            play_json.get("delivery").and_then(Value::as_str),
            Some(case.expected_delivery),
            "{}",
            play_json
        );
        assert_eq!(
            play_json
                .pointer("/playback_plan/mode")
                .and_then(Value::as_str),
            Some(case.expected_mode),
            "{}",
            play_json
        );
        assert_eq!(
            play_json
                .pointer("/playback_plan/delivery")
                .and_then(Value::as_str),
            Some(case.expected_delivery),
            "{}",
            play_json
        );
        assert_eq!(
            play_json
                .get("server_seek_required")
                .and_then(Value::as_bool),
            Some(case.expected_mode != "direct_play")
        );

        let session_id = session_id(&play_json)?;
        let stream_url = stream_url(&play_json)?;
        let session_token = query_param(stream_url, "session").context("session token in URL")?;
        let persisted: (String, String, String, String) = sqlx::query_as(
            "SELECT
                    COALESCE(playback_plan_json, ''),
                    COALESCE(transcode_state, ''),
                    COALESCE(job_state_json, ''),
                    COALESCE(client_capabilities, '')
                 FROM playback_sessions WHERE id = ? LIMIT 1",
        )
        .bind(session_id)
        .fetch_one(&fixture.state.db_pool)
        .await?;
        for value in [&persisted.0, &persisted.1, &persisted.2, &persisted.3] {
            assert!(
                !value.contains(&session_token) && !value.contains(stream_url),
                "{} persisted state leaked tokenized URL/token: {value}",
                case.name
            );
        }

        if case.expected_mode == "direct_play" {
            assert!(
                fixture
                    .state
                    .transcodes
                    .snapshot(Uuid::parse_str(session_id)?)
                    .await
                    .is_none(),
                "direct play must not start an FFmpeg job"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn direct_play_route_enforces_ranges_session_owner_end_and_expiry() -> Result<()> {
    let case = MatrixCase {
        name: "direct_route_security",
        file_name: "Phase15.Direct.Route.Security.2026.mkv",
        container: "mkv",
        video_codec: "h264",
        audio_codec: "aac",
        width: 1280,
        height: 720,
        bitrate_bps: 2_000_000,
        subtitle: None,
        network_type: "lan",
        client_capabilities: json!({
            "profile_version": 1,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "quality_mode": "original"
        }),
        expected_mode: "direct_play",
        expected_delivery: "direct_file",
        configure: no_extra_config,
    };
    let fixture = setup_fixture(&case).await?;
    let (status, play_json) = post_play(
        &fixture.app,
        Some(&fixture.token),
        play_body(&case, &fixture),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{play_json}");
    let direct_stream_url = stream_url(&play_json)?.to_string();
    let direct_session_id = session_id(&play_json)?.to_string();
    let media_file_id = play_json
        .get("media_file_id")
        .or_else(|| play_json.get("mediaFileId"))
        .and_then(Value::as_str)
        .context("media_file_id")?
        .to_string();

    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &direct_stream_url,
        None,
        StatusCode::OK,
    )
    .await?;
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &direct_stream_url,
        Some("bytes=0-3"),
        StatusCode::PARTIAL_CONTENT,
    )
    .await?;
    let middle = direct_get(
        &fixture.app,
        &fixture.token,
        &direct_stream_url,
        Some("bytes=4-6"),
    )
    .await?;
    assert_eq!(middle.0, StatusCode::PARTIAL_CONTENT);
    assert_eq!(&middle.2[..], b"456");
    let suffix = direct_get(
        &fixture.app,
        &fixture.token,
        &direct_stream_url,
        Some("bytes=-3"),
    )
    .await?;
    assert_eq!(suffix.0, StatusCode::PARTIAL_CONTENT);
    assert_eq!(&suffix.2[..], b"789");
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &direct_stream_url,
        Some("bytes=abc"),
        StatusCode::RANGE_NOT_SATISFIABLE,
    )
    .await?;
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &direct_stream_url,
        Some("bytes=99-120"),
        StatusCode::RANGE_NOT_SATISFIABLE,
    )
    .await?;

    let head = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(&direct_stream_url)
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(head.status(), StatusCode::OK);
    assert!(
        body::to_bytes(head.into_body(), 1_048_576)
            .await?
            .is_empty()
    );

    let missing_session_url = direct_stream_url
        .split_once('?')
        .map(|(path, _)| path.to_string())
        .context("query string")?;
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &missing_session_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await?;

    let other_user = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)")
        .bind(other_user.to_string())
        .bind("phase15-other-owner@example.com")
        .bind("hashed")
        .execute(&fixture.state.db_pool)
        .await?;
    let other_token = fixture
        .state
        .auth_service
        .issue_access_token(other_user)?
        .token;
    assert_direct_status(
        &fixture.app,
        &other_token,
        &direct_stream_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await?;

    let other_file_id = Uuid::new_v4().to_string();
    let other_path = fixture._dir.path().join("Phase15.Other.File.2026.mkv");
    tokio::fs::write(&other_path, b"abcdefghij").await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
         VALUES (?, ?, ?, 10, 'mkv', 'h264', 'aac', 1280, 720, 2000000, 'ok')",
    )
    .bind(&other_file_id)
    .bind(&fixture.item_id)
    .bind(other_path.to_string_lossy().to_string())
    .execute(&fixture.state.db_pool)
    .await?;
    let tampered_url = direct_stream_url.replacen(&media_file_id, &other_file_id, 1);
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &tampered_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await?;

    let detail = request_json(
        &fixture.app,
        &fixture.token,
        Request::get(format!("/api/v1/sessions/{direct_session_id}")).body(Body::empty())?,
    )
    .await?;
    assert_eq!(detail.0, StatusCode::OK, "{}", detail.1);
    let poll = request_json(
        &fixture.app,
        &fixture.token,
        Request::get(format!("/api/v1/sessions/{direct_session_id}/poll")).body(Body::empty())?,
    )
    .await?;
    assert_eq!(poll.0, StatusCode::OK, "{}", poll.1);
    let seek = request_json(
        &fixture.app,
        &fixture.token,
        Request::post(format!("/api/v1/sessions/{direct_session_id}/seek"))
            .header("content-type", "application/json")
            .body(Body::from(json!({ "position_seconds": 5.0 }).to_string()))?,
    )
    .await?;
    assert_eq!(seek.0, StatusCode::UNAUTHORIZED, "{}", seek.1);

    let end = request_json(
        &fixture.app,
        &fixture.token,
        Request::post(format!("/api/v1/sessions/{direct_session_id}/end")).body(Body::empty())?,
    )
    .await?;
    assert_eq!(end.0, StatusCode::OK, "{}", end.1);
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &direct_stream_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await?;

    let (status, play_json) = post_play(
        &fixture.app,
        Some(&fixture.token),
        play_body(&case, &fixture),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{play_json}");
    let expired_stream_url = stream_url(&play_json)?.to_string();
    let expired_session_id = session_id(&play_json)?.to_string();
    sqlx::query("UPDATE playback_sessions SET updated_at = '2000-01-01 00:00:00' WHERE id = ?")
        .bind(&expired_session_id)
        .execute(&fixture.state.db_pool)
        .await?;
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &expired_stream_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let expired_state: String =
        sqlx::query_scalar("SELECT state FROM playback_sessions WHERE id = ? LIMIT 1")
            .bind(&expired_session_id)
            .fetch_one(&fixture.state.db_pool)
            .await?;
    assert_eq!(expired_state, "ended");

    Ok(())
}

#[tokio::test]
async fn direct_play_selected_external_subtitle_returns_scoped_sidecar_url() -> Result<()> {
    let case = MatrixCase {
        name: "direct_sidecar_subtitle",
        file_name: "Phase17.Direct.Sidecar.Subtitle.2026.mkv",
        container: "mkv",
        video_codec: "h264",
        audio_codec: "aac",
        width: 1280,
        height: 720,
        bitrate_bps: 2_000_000,
        subtitle: None,
        network_type: "lan",
        client_capabilities: json!({
            "profile_version": 4,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supported_subtitle_codecs": ["srt", "webvtt", "ass"],
            "subtitle_rendering": "sidecar",
            "ass_complexity_support": "native",
            "image_subtitle_support": "native_or_burn_in",
            "quality_mode": "original"
        }),
        expected_mode: "direct_play",
        expected_delivery: "direct_file",
        configure: no_extra_config,
    };
    let fixture = setup_fixture(&case).await?;
    let subtitle_id = Uuid::new_v4().to_string();
    let subtitle_path = fixture
        ._dir
        .path()
        .join("Phase17.Direct.Sidecar.Subtitle.2026.eng.srt");
    tokio::fs::write(
        &subtitle_path,
        "1\n00:00:00,000 --> 00:00:02,000\nsidecar subtitle\n",
    )
    .await?;
    sqlx::query(
        "INSERT INTO external_subtitles
            (id, media_file_id, path, language, title, format, is_default, is_forced, is_hearing_impaired)
         VALUES (?, ?, ?, 'eng', 'English sidecar', 'srt', 1, 0, 1)",
    )
    .bind(&subtitle_id)
    .bind(&fixture.media_file_id)
    .bind(subtitle_path.to_string_lossy().to_string())
    .execute(&fixture.state.db_pool)
    .await?;

    let mut body = play_body(&case, &fixture);
    body["subtitle_stream_index"] = json!(-100_000);
    let (status, play_json) = post_play(&fixture.app, Some(&fixture.token), body).await?;
    assert_eq!(status, StatusCode::OK, "{play_json}");
    assert_eq!(
        play_json.get("mode").and_then(Value::as_str),
        Some("direct_play")
    );
    assert_eq!(
        play_json
            .pointer("/playback_plan/subtitle_action")
            .and_then(Value::as_str),
        Some("passthrough"),
        "{play_json}"
    );
    let subtitle_url = play_json
        .get("subtitle_url")
        .or_else(|| play_json.get("subtitleUrl"))
        .and_then(Value::as_str)
        .context("subtitle_url")?;
    assert!(subtitle_url.contains(&fixture.media_file_id));
    assert!(subtitle_url.contains(&subtitle_id));

    let (subtitle_status, headers, bytes) =
        direct_get(&fixture.app, &fixture.token, subtitle_url, None).await?;
    assert_eq!(subtitle_status, StatusCode::OK);
    assert_eq!(
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/x-subrip; charset=utf-8")
    );
    assert!(String::from_utf8_lossy(&bytes).contains("sidecar subtitle"));

    let head = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(subtitle_url)
                .header("authorization", format!("Bearer {}", fixture.token))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(head.status(), StatusCode::OK);
    assert!(
        body::to_bytes(head.into_body(), 1_048_576)
            .await?
            .is_empty()
    );

    let missing_session_url = subtitle_url
        .split_once('?')
        .map(|(path, _)| path.to_string())
        .context("query string")?;
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        &missing_session_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await?;

    let other_user = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)")
        .bind(other_user.to_string())
        .bind("phase17-sidecar-other@example.com")
        .bind("hashed")
        .execute(&fixture.state.db_pool)
        .await?;
    let other_token = fixture
        .state
        .auth_service
        .issue_access_token(other_user)?
        .token;
    assert_direct_status(
        &fixture.app,
        &other_token,
        subtitle_url,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn direct_play_external_subtitle_outside_media_directory_is_forbidden() -> Result<()> {
    let case = MatrixCase {
        name: "direct_sidecar_subtitle_escape",
        file_name: "Phase17.Direct.Sidecar.Escape.2026.mkv",
        container: "mkv",
        video_codec: "h264",
        audio_codec: "aac",
        width: 1280,
        height: 720,
        bitrate_bps: 2_000_000,
        subtitle: None,
        network_type: "lan",
        client_capabilities: json!({
            "profile_version": 4,
            "client_kind": "native_mpv",
            "direct_play_preferred": true,
            "supported_containers": ["mkv"],
            "supported_video_codecs": ["h264"],
            "supported_audio_codecs": ["aac"],
            "supported_subtitle_codecs": ["srt", "webvtt", "ass"],
            "subtitle_rendering": "sidecar",
            "quality_mode": "original"
        }),
        expected_mode: "direct_play",
        expected_delivery: "direct_file",
        configure: no_extra_config,
    };
    let fixture = setup_fixture(&case).await?;
    let outside_dir = tempdir()?;
    let subtitle_id = Uuid::new_v4().to_string();
    let outside_subtitle_path = outside_dir.path().join("outside-media-dir.srt");
    tokio::fs::write(
        &outside_subtitle_path,
        "1\n00:00:00,000 --> 00:00:02,000\noutside subtitle\n",
    )
    .await?;
    sqlx::query(
        "INSERT INTO external_subtitles
            (id, media_file_id, path, language, title, format, is_default, is_forced, is_hearing_impaired)
         VALUES (?, ?, ?, 'eng', 'Outside sidecar', 'srt', 1, 0, 0)",
    )
    .bind(&subtitle_id)
    .bind(&fixture.media_file_id)
    .bind(outside_subtitle_path.to_string_lossy().to_string())
    .execute(&fixture.state.db_pool)
    .await?;

    let mut body = play_body(&case, &fixture);
    body["subtitle_stream_index"] = json!(-100_000);
    let (status, play_json) = post_play(&fixture.app, Some(&fixture.token), body).await?;
    assert_eq!(status, StatusCode::OK, "{play_json}");
    let subtitle_url = play_json
        .get("subtitle_url")
        .or_else(|| play_json.get("subtitleUrl"))
        .and_then(Value::as_str)
        .context("subtitle_url")?;
    assert!(subtitle_url.contains(&subtitle_id), "{subtitle_url}");
    assert_direct_status(
        &fixture.app,
        &fixture.token,
        subtitle_url,
        None,
        StatusCode::FORBIDDEN,
    )
    .await?;

    Ok(())
}

async fn request_json(
    app: &Router,
    token: &str,
    request: Request<Body>,
) -> Result<(StatusCode, Value)> {
    let (mut parts, body) = request.into_parts();
    parts
        .headers
        .insert("authorization", format!("Bearer {token}").parse()?);
    let response = app
        .clone()
        .oneshot(Request::from_parts(parts, body))
        .await?;
    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), 1_048_576).await?;
    let json = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        json!({
            "raw": String::from_utf8_lossy(&bytes)
        })
    });
    Ok((status, json))
}

async fn direct_get(
    app: &Router,
    token: &str,
    uri: &str,
    range: Option<&str>,
) -> Result<(StatusCode, axum::http::HeaderMap, Bytes)> {
    let mut request = Request::get(uri).header("authorization", format!("Bearer {token}"));
    if let Some(range) = range {
        request = request.header("range", range);
    }
    let response = app.clone().oneshot(request.body(Body::empty())?).await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = body::to_bytes(response.into_body(), 1_048_576).await?;
    Ok((status, headers, body))
}

async fn assert_direct_status(
    app: &Router,
    token: &str,
    uri: &str,
    range: Option<&str>,
    expected: StatusCode,
) -> Result<()> {
    let (status, _, body) = direct_get(app, token, uri, range).await?;
    assert_eq!(
        status,
        expected,
        "uri={uri} range={range:?} body={}",
        String::from_utf8_lossy(&body)
    );
    Ok(())
}
