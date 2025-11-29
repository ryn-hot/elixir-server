use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result};
use tokio::{
    fs,
    process::{Child, Command},
    sync::Mutex,
};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TranscodeParams {
    pub seek_seconds: f32,
}

#[derive(Debug)]
pub struct TranscodeJob {
    pub temp_dir: PathBuf,
    pub media_path: String,
    pub seek_seconds: f32,
    pub child: Option<Child>,
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
    ) -> Result<PathBuf> {
        let mut guard = self.inner.lock().await;
        if let Some(job) = guard.get_mut(&session_id) {
            if let Some(child) = job.child.as_mut() {
                if child.try_wait()?.is_none() {
                    return Ok(job.temp_dir.join("index.m3u8"));
                }
            }
        }

        let temp_dir = self.make_temp_dir(session_id).await?;
        let playlist_path = temp_dir.join("index.m3u8");
        let segment_template = temp_dir.join("seg_%05d.ts");

        let child = spawn_ffmpeg(
            media_path,
            params.seek_seconds,
            &segment_template,
            &playlist_path,
        )
        .await?;

        let job = TranscodeJob {
            temp_dir: temp_dir.clone(),
            media_path: media_path.to_string(),
            seek_seconds: params.seek_seconds,
            child: Some(child),
        };
        guard.insert(session_id, job);

        Ok(playlist_path)
    }

    pub async fn restart(
        &self,
        session_id: Uuid,
        media_path: &str,
        seek_seconds: f32,
    ) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(mut job) = guard.remove(&session_id) {
            if let Some(mut child) = job.child.take() {
                let _ = child.kill().await;
            }
            let _ = fs::remove_dir_all(&job.temp_dir).await;
        }

        let temp_dir = self.make_temp_dir(session_id).await?;
        let playlist_path = temp_dir.join("index.m3u8");
        let segment_template = temp_dir.join("seg_%05d.ts");
        let child =
            spawn_ffmpeg(media_path, seek_seconds, &segment_template, &playlist_path).await?;

        let job = TranscodeJob {
            temp_dir,
            media_path: media_path.to_string(),
            seek_seconds,
            child: Some(child),
        };
        guard.insert(session_id, job);
        Ok(())
    }

    pub async fn segment_path(&self, session_id: Uuid, name: &str) -> Option<PathBuf> {
        let guard = self.inner.lock().await;
        guard.get(&session_id).map(|j| j.temp_dir.join(name))
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
) -> Result<Child> {
    let child = Command::new("ffmpeg")
        .arg("-y")
        .arg("-ss")
        .arg(format!("{seek_seconds}"))
        .arg("-i")
        .arg(input)
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("0:a:0")
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
        .arg("-hls_segment_filename")
        .arg(segment_template.to_string_lossy().to_string())
        .arg(playlist_path.to_string_lossy().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn ffmpeg")?;

    Ok(child)
}
