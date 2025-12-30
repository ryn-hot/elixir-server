use std::{
    collections::HashMap,
    fs::File as StdFile,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result};
use serde::Deserialize;
use sqlx::Row;
use tokio::{
    fs,
    process::{Child, Command},
    sync::Mutex,
    time,
};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TranscodeParams {
    pub seek_seconds: f32,
}

#[derive(Debug, Clone)]
pub(crate) struct SubtitleInfo {
    pub stream_index: usize,
    pub language: Option<String>,
    pub title: Option<String>,
    pub is_default: bool,
    pub is_forced: bool,
}

#[derive(Debug)]
pub struct TranscodeJob {
    pub temp_dir: PathBuf,
    pub media_path: String,
    pub seek_seconds: f32,
    pub log_path: PathBuf,
    pub child: Option<Child>,
    pub subtitle_delay_seconds: Option<f64>,
    pub subtitles: Vec<SubtitleInfo>,
}

#[derive(Debug, Clone)]
pub struct TranscodeHandle {
    pub playlist_path: PathBuf,
    pub log_path: PathBuf,
    pub temp_dir: PathBuf,
    pub pid: Option<u32>,
    pub subtitles: Vec<SubtitleInfo>,
}

#[derive(Clone, Default)]
pub struct TranscodeManager {
    inner: Arc<Mutex<HashMap<Uuid, TranscodeJob>>>,
}

impl TranscodeManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start_or_get(
        &self,
        session_id: Uuid,
        media_path: &str,
        params: TranscodeParams,
    ) -> Result<TranscodeHandle> {
        let mut guard = self.inner.lock().await;
        if let Some(job) = guard.get_mut(&session_id) {
            if let Some(child) = job.child.as_mut() {
                if child.try_wait()?.is_none() {
                    return Ok(TranscodeHandle {
                        playlist_path: job.temp_dir.join("master.m3u8"),
                        log_path: job.log_path.clone(),
                        temp_dir: job.temp_dir.clone(),
                        pid: child.id(),
                        subtitles: job.subtitles.clone(),
                    });
                }
            }
            // Child ended; reuse existing playlist if present.
            let playlist_path = job.temp_dir.join("master.m3u8");
            if playlist_path.exists() {
                return Ok(TranscodeHandle {
                    playlist_path,
                    log_path: job.log_path.clone(),
                    temp_dir: job.temp_dir.clone(),
                    pid: None,
                    subtitles: job.subtitles.clone(),
                });
            }
        }

        let temp_dir = self.make_temp_dir(session_id).await?;
        let playlist_path = temp_dir.join("master.m3u8");
        let variant_playlist = temp_dir.join("stream_%v.m3u8");
        let segment_template = temp_dir.join("seg_%v_%05d.ts");
        let log_path = temp_dir.join("ffmpeg.log");

        let subtitles = detect_text_subtitles(media_path).await;
        let child = spawn_ffmpeg(
            media_path,
            params.seek_seconds,
            &segment_template,
            &variant_playlist,
            &log_path,
            &temp_dir,
            &subtitles,
        )
        .await?;
        let pid = child.id();

        let job = TranscodeJob {
            temp_dir: temp_dir.clone(),
            media_path: media_path.to_string(),
            seek_seconds: params.seek_seconds,
            log_path: log_path.clone(),
            child: Some(child),
            subtitle_delay_seconds: None,
            subtitles: subtitles.clone(),
        };
        guard.insert(session_id, job);

        Ok(TranscodeHandle {
            playlist_path,
            log_path,
            temp_dir,
            pid,
            subtitles,
        })
    }

    pub async fn restart(
        &self,
        session_id: Uuid,
        media_path: &str,
        seek_seconds: f32,
    ) -> Result<TranscodeHandle> {
        let mut guard = self.inner.lock().await;
        if let Some(mut job) = guard.remove(&session_id) {
            if let Some(mut child) = job.child.take() {
                let _ = child.kill().await;
            }
            let _ = fs::remove_dir_all(&job.temp_dir).await;
        }

        let temp_dir = self.make_temp_dir(session_id).await?;
        let playlist_path = temp_dir.join("master.m3u8");
        let variant_playlist = temp_dir.join("stream_%v.m3u8");
        let segment_template = temp_dir.join("seg_%v_%05d.ts");
        let log_path = temp_dir.join("ffmpeg.log");
        let subtitles = detect_text_subtitles(media_path).await;
        let child = spawn_ffmpeg(
            media_path,
            seek_seconds,
            &segment_template,
            &variant_playlist,
            &log_path,
            &temp_dir,
            &subtitles,
        )
        .await?;
        let pid = child.id();

        let job = TranscodeJob {
            temp_dir: temp_dir.clone(),
            media_path: media_path.to_string(),
            seek_seconds,
            log_path: log_path.clone(),
            child: Some(child),
            subtitle_delay_seconds: None,
            subtitles: subtitles.clone(),
        };
        guard.insert(session_id, job);
        Ok(TranscodeHandle {
            playlist_path,
            log_path,
            temp_dir,
            pid,
            subtitles,
        })
    }

    pub async fn segment_path(&self, session_id: Uuid, name: &str) -> Option<PathBuf> {
        let guard = self.inner.lock().await;
        guard.get(&session_id).map(|j| j.temp_dir.join(name))
    }

    pub async fn subtitle_delay(&self, session_id: Uuid) -> Option<f64> {
        let guard = self.inner.lock().await;
        guard
            .get(&session_id)
            .and_then(|job| job.subtitle_delay_seconds)
    }

    pub async fn set_subtitle_delay(&self, session_id: Uuid, delay: f64) {
        let mut guard = self.inner.lock().await;
        if let Some(job) = guard.get_mut(&session_id) {
            job.subtitle_delay_seconds = Some(delay);
        }
    }

    pub async fn stop_and_remove(&self, session_id: Uuid) {
        let mut guard = self.inner.lock().await;
        if let Some(mut job) = guard.remove(&session_id) {
            if let Some(mut child) = job.child.take() {
                let _ = child.kill().await;
            }
            let _ = fs::remove_dir_all(&job.temp_dir).await;
        }
    }

    pub async fn stop_all(&self) {
        let mut guard = self.inner.lock().await;
        let keys: Vec<Uuid> = guard.keys().cloned().collect();
        for id in keys {
            if let Some(mut job) = guard.remove(&id) {
                if let Some(mut child) = job.child.take() {
                    let _ = child.kill().await;
                }
                let _ = fs::remove_dir_all(&job.temp_dir).await;
            }
        }
    }

    async fn make_temp_dir(&self, session_id: Uuid) -> Result<PathBuf> {
        let dir = std::env::temp_dir()
            .join("elixir")
            .join(session_id.to_string());
        if dir.exists() {
            fs::remove_dir_all(&dir).await.ok();
        }
        fs::create_dir_all(&dir)
            .await
            .context("creating transcode temp dir")?;
        Ok(dir)
    }
}

async fn spawn_ffmpeg(
    input: &str,
    seek_seconds: f32,
    segment_template: &Path,
    playlist_path: &Path,
    log_path: &Path,
    temp_dir: &Path,
    subtitles: &[SubtitleInfo],
) -> Result<Child> {
    let log_file = StdFile::create(log_path).context("creating ffmpeg log file")?;

    let mut command = Command::new("ffmpeg");
    command
        .arg("-y")
        .arg("-loglevel")
        .arg("warning");

    if subtitles.is_empty() {
        command
            .arg("-ss")
            .arg(format!("{seek_seconds}"))
            .arg("-i")
            .arg(input);
    } else {
        command
            .arg("-ss")
            .arg(format!("{seek_seconds}"))
            .arg("-i")
            .arg(input)
            .arg("-ss")
            .arg(format!("{seek_seconds}"))
            .arg("-i")
            .arg(input);
    }

    command
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("0:a:0");

    command
        .arg("-c:v")
        .arg("libx264")
        .arg("-preset")
        .arg("veryfast")
        .arg("-crf")
        .arg("20")
        .arg("-c:a")
        .arg("aac")
        .arg("-b:a")
        .arg("128k")
        .arg("-f")
        .arg("hls")
        .arg("-hls_time")
        .arg("4")
        .arg("-hls_playlist_type")
        .arg("event")
        .arg("-master_pl_name")
        .arg("master.m3u8")
        .arg("-var_stream_map")
        .arg("v:0,a:0")
        .arg("-hls_segment_filename")
        .arg(segment_template.to_string_lossy().to_string())
        .arg(playlist_path.to_string_lossy().to_string());

    for (idx, sub) in subtitles.iter().enumerate() {
        let playlist = temp_dir.join(format!("sub_{idx}.m3u8"));
        let segment = temp_dir.join(format!("sub_{idx}_%05d.vtt"));
        command
            .arg("-map")
            .arg(format!("1:s:{}", sub.stream_index))
            .arg("-c:s")
            .arg("webvtt")
            .arg("-f")
            .arg("segment")
            .arg("-segment_time")
            .arg("4")
            .arg("-segment_format")
            .arg("webvtt")
            .arg("-segment_list")
            .arg(playlist.to_string_lossy().to_string())
            .arg("-segment_list_type")
            .arg("m3u8")
            .arg("-segment_list_flags")
            .arg("live")
            .arg("-segment_list_size")
            .arg("0")
            .arg(segment.to_string_lossy().to_string());
    }

    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log_file))
        .spawn()
        .context("failed to spawn ffmpeg")?;

    Ok(child)
}

#[derive(Debug, Deserialize)]
struct SubtitleProbe {
    streams: Vec<SubtitleStream>,
}

#[derive(Debug, Deserialize)]
struct SubtitleStream {
    codec_name: Option<String>,
    tags: Option<HashMap<String, String>>,
    disposition: Option<SubtitleDisposition>,
}

#[derive(Debug, Deserialize)]
struct SubtitleDisposition {
    default: Option<i32>,
    forced: Option<i32>,
}

async fn detect_text_subtitles(path: &str) -> Vec<SubtitleInfo> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_streams")
        .arg("-select_streams")
        .arg("s")
        .arg(path)
        .output()
        .await
        .ok();

    let output = match output {
        Some(output) if output.status.success() => output,
        _ => return Vec::new(),
    };

    let parsed: SubtitleProbe = match serde_json::from_slice(&output.stdout) {
        Ok(parsed) => parsed,
        Err(_) => return Vec::new(),
    };
    let mut candidates = Vec::new();

    for (idx, stream) in parsed.streams.into_iter().enumerate() {
        let codec = stream.codec_name.as_deref().unwrap_or("");
        if !is_text_subtitle(codec) {
            continue;
        }
        let language = stream
            .tags
            .as_ref()
            .and_then(|tags| tags.get("language").cloned());
        let title = stream
            .tags
            .as_ref()
            .and_then(|tags| tags.get("title").cloned());
        let is_default = stream
            .disposition
            .as_ref()
            .and_then(|d| d.default)
            .unwrap_or(0)
            == 1;
        let is_forced = stream
            .disposition
            .as_ref()
            .and_then(|d| d.forced)
            .unwrap_or(0)
            == 1;
        candidates.push(SubtitleInfo {
            stream_index: idx,
            language,
            title,
            is_default,
            is_forced,
        });
    }

    candidates
}

fn is_text_subtitle(codec: &str) -> bool {
    matches!(
        codec.to_ascii_lowercase().as_str(),
        "ass" | "ssa" | "subrip" | "srt" | "webvtt" | "mov_text"
    )
}

pub async fn start_session_cleanup(
    state: crate::state::AppState,
    ttl_seconds: u64,
    interval_seconds: u64,
) {
    let mut ticker = time::interval(std::time::Duration::from_secs(interval_seconds));
    loop {
        ticker.tick().await;
        if let Err(err) = cleanup_stale_sessions(&state, ttl_seconds).await {
            tracing::warn!("playback session cleanup failed: {err}");
        }
    }
}

async fn cleanup_stale_sessions(state: &crate::state::AppState, ttl_seconds: u64) -> Result<()> {
    let rows = sqlx::query("SELECT id, updated_at FROM playback_sessions")
        .fetch_all(&state.db_pool)
        .await?;

    let now = chrono::Utc::now();
    let mut expired_ids = Vec::new();
    for row in rows {
        let id_str: String = row.get("id");
        let updated_str: String = row.try_get("updated_at")?;
        if let Some(updated_ts) = parse_timestamp(&updated_str) {
            let age = now - updated_ts;
            if age.num_seconds() as u64 > ttl_seconds {
                if let Ok(id) = Uuid::parse_str(&id_str) {
                    expired_ids.push(id);
                }
            }
        }
    }

    for id in expired_ids {
        state.transcodes.stop_and_remove(id).await;
        sqlx::query("DELETE FROM playback_sessions WHERE id = ?")
            .bind(id.to_string())
            .execute(&state.db_pool)
            .await
            .ok();
    }

    Ok(())
}

fn parse_timestamp(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            dt,
            chrono::Utc,
        ));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    None
}
