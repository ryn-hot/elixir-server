//! Private synthetic media for combined real HTTP/Qt/playback qualification.
//! No fake playback jobs, public source publication or production endpoint.
use super::*;
use axum::{
    Router,
    body::Body,
    extract::{Path as RoutePath, State},
    http::{Response, StatusCode, header},
    routing::get,
};
use std::{
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::{sync::Semaphore, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const MAX_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_MEDIA_BYTES: u64 = 128 * 1024 * 1024;
const SEGMENT_DELAY: Duration = Duration::from_millis(750);

struct MediaState {
    manifest: String,
    segments: BTreeMap<String, PathBuf>,
    slots: Arc<Semaphore>,
    served: AtomicU64,
    served_bytes: AtomicU64,
}

async fn media_manifest(State(state): State<Arc<MediaState>>) -> Response<Body> {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")
        .body(Body::from(state.manifest.clone()))
        .unwrap()
}

/// The delay keeps the normal FFmpeg reader alive through the UI measurement.
/// It is a controlled loopback media source, not simulated codec execution.
async fn segment(
    State(state): State<Arc<MediaState>>,
    RoutePath(name): RoutePath<String>,
) -> Response<Body> {
    let Some(path) = state.segments.get(&name) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    };
    let Ok(_slot) = state.slots.clone().try_acquire_owned() else {
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Body::empty())
            .unwrap();
    };
    tokio::time::sleep(SEGMENT_DELAY).await;
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) if !bytes.is_empty() && bytes.len() as u64 <= MAX_SEGMENT_BYTES => bytes,
        _ => {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap();
        }
    };
    state.served.fetch_add(1, Ordering::Relaxed);
    state
        .served_bytes
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(Body::from(bytes))
        .unwrap()
}

pub(super) struct MediaFixture {
    input_url: String,
    state: Arc<MediaState>,
    cancel: CancellationToken,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl MediaFixture {
    pub(super) async fn prepare(root: &Path) -> Result<Self> {
        let directory = root.join("playback-source");
        std::fs::create_dir(&directory)?;
        let playlist = directory.join("source.m3u8");
        let mut child = tokio::process::Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-n",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=1280x720:rate=24:duration=240",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=48000:duration=240",
                "-c:v",
                "libx264",
                "-threads",
                "1",
                "-preset",
                "ultrafast",
                "-pix_fmt",
                "yuv420p",
                "-b:v",
                "1500k",
                "-maxrate",
                "1500k",
                "-bufsize",
                "3000k",
                "-g",
                "48",
                "-keyint_min",
                "48",
                "-sc_threshold",
                "0",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-ac",
                "2",
                "-shortest",
                "-f",
                "hls",
                "-hls_time",
                "2",
                "-hls_list_size",
                "0",
                "-hls_segment_filename",
            ])
            .arg(directory.join("segment_%05d.ts"))
            .arg(&playlist)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("start actual synthetic H264/AAC media generation")?;
        let generated = timeout(Duration::from_secs(120), child.wait()).await;
        let generation = match generated {
            Ok(status) => status.map_err(anyhow::Error::from).and_then(|status| {
                ensure!(
                    status.success(),
                    "actual synthetic media generation failed: {status}"
                );
                Ok(())
            }),
            Err(error) => Err(error.into()),
        };
        // Retain and join this exact child on timeout or an error, too.
        if child.try_wait()?.is_none() {
            child.start_kill()?;
            timeout(Duration::from_secs(10), child.wait()).await??;
        }
        generation?;

        let manifest = std::fs::read_to_string(&playlist)?;
        let names = segment_names(&manifest)?;
        let mut total = 0_u64;
        let mut segments = BTreeMap::new();
        for name in names {
            let path = directory.join(&name);
            let metadata = std::fs::symlink_metadata(&path)?;
            ensure!(
                metadata.is_file() && metadata.len() > 0 && metadata.len() <= MAX_SEGMENT_BYTES,
                "bounded generated regular media segment required"
            );
            total = total
                .checked_add(metadata.len())
                .context("fixture media byte overflow")?;
            ensure!(
                total <= MAX_MEDIA_BYTES,
                "synthetic fixture exceeded its media bound"
            );
            segments.insert(name, path);
        }
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let rewritten = manifest
            .lines()
            .map(|line| {
                if segments.contains_key(line) {
                    format!("http://{address}/segments/{line}")
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(&playlist, &rewritten)?;
        let state = Arc::new(MediaState {
            manifest: rewritten,
            segments,
            slots: Arc::new(Semaphore::new(4)),
            served: AtomicU64::new(0),
            served_bytes: AtomicU64::new(0),
        });
        let app = Router::new()
            .route("/segments/:name", get(segment))
            .route("/source.m3u8", get(media_manifest))
            .with_state(state.clone());
        let cancel = CancellationToken::new();
        let cancelled = cancel.clone();
        // No fallible preparation follows spawning the owned listener.
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(cancelled.cancelled_owned())
                .await
        });
        Ok(Self {
            input_url: format!("http://{address}/source.m3u8"),
            state,
            cancel,
            task: Some(task),
        })
    }

    pub(super) fn served(&self) -> (u64, u64) {
        (
            self.state.served.load(Ordering::Relaxed),
            self.state.served_bytes.load(Ordering::Relaxed),
        )
    }

    pub(super) async fn seed(&self, pool: &sqlx::AnyPool) -> Result<(String, String)> {
        use crate::extensions::{ExternalIds, FileDescriptor, MediaFileCandidate, MediaIdentity};
        let path = self.input_url.as_str();
        let title = format!("Marketplace Synthetic Playback {}", uuid::Uuid::new_v4());
        crate::library::run_full_scan(
            pool,
            vec![MediaFileCandidate {
                identity: MediaIdentity {
                    r#type: crate::db::models::MediaType::Movie,
                    external_ids: ExternalIds::default(),
                    title: title.clone(),
                    year: Some(2026),
                    season: None,
                    episode: None,
                },
                files: vec![FileDescriptor {
                    path: path.into(),
                    size_bytes: None,
                    hash: None,
                    container: Some("mpegts".into()),
                    video_codec: Some("h264".into()),
                    audio_codec: Some("aac".into()),
                }],
                extension_metadata: Default::default(),
                source_config_id: None,
            }],
            false,
        )
        .await?;
        let item: String = sqlx::query_scalar("SELECT id FROM movies WHERE title=$1 AND year=2026")
            .bind(title)
            .fetch_one(pool)
            .await?;
        let file: String = sqlx::query_scalar("SELECT id FROM media_files WHERE path=$1")
            .bind(path)
            .fetch_one(pool)
            .await?;
        // Normal production ffprobe and persistence, never hand-written positive
        // capabilities. Both baseline and loaded playback then use this cache.
        let capabilities =
            crate::playback::probe::ensure_media_file_probe(pool, &file, path).await?;
        ensure!(
            capabilities
                .video_streams
                .iter()
                .any(|v| v.codec.as_deref() == Some("h264")
                    && v.width == Some(1280)
                    && v.height == Some(720)),
            "actual synthetic H264/720p probe differs"
        );
        ensure!(
            capabilities
                .audio_streams
                .iter()
                .any(|a| a.codec.as_deref() == Some("aac")),
            "actual synthetic AAC probe differs"
        );
        ensure!(
            self.served().0 > 0,
            "normal ffprobe never read the actual media source"
        );
        Ok((item, file))
    }

    pub(super) async fn close(mut self) -> Result<()> {
        self.cancel.cancel();
        if let Some(mut task) = self.task.take() {
            match timeout(Duration::from_secs(10), &mut task).await {
                Ok(result) => result??,
                Err(error) => {
                    task.abort();
                    let _ = task.await;
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}

impl Drop for MediaFixture {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn segment_names(manifest: &str) -> Result<BTreeSet<String>> {
    ensure!(
        manifest.len() <= 32768
            && manifest.starts_with("#EXTM3U\n")
            && manifest.lines().any(|line| line == "#EXT-X-ENDLIST"),
        "bounded complete synthetic HLS required"
    );
    let mut names = BTreeSet::new();
    for line in manifest.lines().filter(|line| line.starts_with('#')) {
        ensure!(
            line == "#EXTM3U"
                || line == "#EXT-X-ENDLIST"
                || line.starts_with("#EXTINF:")
                || line.starts_with("#EXT-X-VERSION:")
                || line.starts_with("#EXT-X-TARGETDURATION:")
                || line.starts_with("#EXT-X-MEDIA-SEQUENCE:"),
            "unexpected generated media directive"
        );
    }
    for name in manifest
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let digits = name
            .strip_prefix("segment_")
            .and_then(|s| s.strip_suffix(".ts"));
        ensure!(
            digits.is_some_and(|s| s.len() == 5 && s.bytes().all(|b| b.is_ascii_digit())),
            "generated media name differs"
        );
        ensure!(
            names.insert(name.to_owned()) && names.len() <= 160,
            "duplicate or excessive media segments"
        );
    }
    ensure!(!names.is_empty(), "synthetic playlist has no media");
    Ok(names)
}

#[test]
fn synthetic_playlist_cannot_introduce_external_paths_duplicates_or_unbounded_input() -> Result<()>
{
    let manifest = "#EXTM3U\n#EXTINF:2,\nsegment_00000.ts\n#EXT-X-ENDLIST\n";
    ensure!(segment_names(manifest)?.len() == 1);
    for bad in [
        "../outside.ts",
        "/outside.ts",
        "http://outside.invalid/a.ts",
        "segment_0.ts",
        "segment_00000.ts\nsegment_00000.ts",
    ] {
        ensure!(segment_names(&manifest.replace("segment_00000.ts", bad)).is_err());
    }
    ensure!(segment_names(&manifest.replace("#EXT-X-ENDLIST", "")).is_err());
    ensure!(
        segment_names(&manifest.replace(
            "#EXT-X-ENDLIST",
            "#EXT-X-KEY:METHOD=AES-128,URI=\"http://outside.invalid/key\"\n#EXT-X-ENDLIST"
        ))
        .is_err()
    );
    ensure!(segment_names(&"x".repeat(32769)).is_err());
    Ok(())
}
