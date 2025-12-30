use std::process::Stdio;

use anyhow::Context;
use serde::Deserialize;
use tokio::process::Command;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FfprobeStreams {
    pub streams: Vec<Stream>,
    pub format: Option<Format>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Stream {
    #[serde(rename = "codec_type")]
    pub codec_type: Option<String>,
    #[serde(rename = "codec_name")]
    pub codec_name: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    #[serde(rename = "bit_rate")]
    pub bit_rate: Option<String>,
    #[serde(rename = "duration")]
    pub duration: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Format {
    pub duration: Option<String>,
    #[serde(rename = "bit_rate")]
    pub bit_rate: Option<String>,
    #[serde(rename = "format_name")]
    pub format_name: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MediaMetadata {
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub bitrate_bps: Option<i64>,
    pub duration_seconds: Option<i32>,
}

pub async fn probe(path: &str) -> anyhow::Result<MediaMetadata> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_format")
        .arg("-show_streams")
        .arg(path)
        .stdout(Stdio::piped())
        .output()
        .await
        .context("failed to spawn ffprobe")?;

    if !output.status.success() {
        anyhow::bail!(
            "ffprobe failed with code {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let parsed: FfprobeStreams =
        serde_json::from_slice(&output.stdout).context("failed to parse ffprobe json")?;

    let mut meta = MediaMetadata::default();
    let mut format_duration_seconds: Option<i32> = None;
    let mut stream_duration_seconds: Option<i32> = None;

    if let Some(fmt) = parsed.format {
        meta.container = fmt.format_name;
        meta.bitrate_bps = fmt.bit_rate.as_ref().and_then(|b| b.parse::<i64>().ok());
        format_duration_seconds = fmt
            .duration
            .as_ref()
            .and_then(|d| d.parse::<f32>().ok())
            .map(|d| d.round() as i32);
        meta.duration_seconds = format_duration_seconds;
    }

    for stream in parsed.streams {
        match stream.codec_type.as_deref() {
            Some("video") => {
                meta.video_codec = stream.codec_name.clone();
                meta.width = stream.width;
                meta.height = stream.height;
                if stream_duration_seconds.is_none() {
                    stream_duration_seconds = stream
                        .duration
                        .as_ref()
                        .and_then(|d| d.parse::<f32>().ok())
                        .map(|d| d.round() as i32);
                    if stream_duration_seconds.is_some() {
                        meta.duration_seconds = stream_duration_seconds;
                    }
                }
            }
            Some("audio") => {
                meta.audio_codec = stream.codec_name.clone();
            }
            _ => {}
        }
    }

    if stream_duration_seconds.is_none() {
        if let Ok(packet_duration) = probe_video_duration_by_packets(path).await {
            let packet_seconds = packet_duration.round() as i32;
            if packet_seconds > 0 {
                match format_duration_seconds {
                    Some(format_seconds)
                        if format_seconds as f64 > (packet_seconds as f64 * 1.1) =>
                    {
                        meta.duration_seconds = Some(packet_seconds);
                    }
                    None => {
                        meta.duration_seconds = Some(packet_seconds);
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(meta)
}

async fn probe_video_duration_by_packets(path: &str) -> anyhow::Result<f32> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("v:0")
        .arg("-show_entries")
        .arg("packet=pts_time")
        .arg("-of")
        .arg("csv=p=0")
        .arg(path)
        .stdout(Stdio::piped())
        .output()
        .await
        .context("failed to probe packet timestamps")?;

    if !output.status.success() {
        anyhow::bail!(
            "ffprobe packets failed with code {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let last = stdout.lines().rev().find(|line| !line.trim().is_empty());
    let last = last.ok_or_else(|| anyhow::anyhow!("no packet timestamps found"))?;
    let value = last
        .trim()
        .parse::<f32>()
        .context("failed to parse packet pts_time")?;
    Ok(value)
}
