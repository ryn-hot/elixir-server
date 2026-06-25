use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::process::Command;
use tokio::time::timeout;

const PACKET_DURATION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FfprobeStreams {
    pub streams: Vec<Stream>,
    pub format: Option<Format>,
    #[serde(default)]
    pub chapters: Vec<Chapter>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Stream {
    pub index: Option<i32>,
    #[serde(rename = "codec_type")]
    pub codec_type: Option<String>,
    #[serde(rename = "codec_name")]
    pub codec_name: Option<String>,
    pub profile: Option<String>,
    pub level: Option<i32>,
    #[serde(rename = "codec_tag_string")]
    pub codec_tag_string: Option<String>,
    #[serde(rename = "pix_fmt")]
    pub pix_fmt: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    #[serde(rename = "avg_frame_rate")]
    pub avg_frame_rate: Option<String>,
    #[serde(rename = "r_frame_rate")]
    pub r_frame_rate: Option<String>,
    #[serde(rename = "bits_per_raw_sample")]
    pub bits_per_raw_sample: Option<String>,
    #[serde(rename = "color_primaries")]
    pub color_primaries: Option<String>,
    #[serde(rename = "color_transfer")]
    pub color_transfer: Option<String>,
    #[serde(rename = "color_space")]
    pub color_space: Option<String>,
    pub channels: Option<i32>,
    #[serde(rename = "channel_layout")]
    pub channel_layout: Option<String>,
    #[serde(rename = "sample_rate")]
    pub sample_rate: Option<String>,
    #[serde(rename = "bit_rate")]
    pub bit_rate: Option<String>,
    #[serde(rename = "duration")]
    pub duration: Option<String>,
    #[serde(rename = "start_time")]
    pub start_time: Option<String>,
    pub tags: Option<HashMap<String, String>>,
    pub disposition: Option<Disposition>,
    #[serde(default, rename = "side_data_list")]
    pub side_data_list: Vec<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Disposition {
    #[serde(rename = "default")]
    pub default_flag: Option<i32>,
    pub forced: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Format {
    pub duration: Option<String>,
    #[serde(rename = "bit_rate")]
    pub bit_rate: Option<String>,
    #[serde(rename = "format_name")]
    pub format_name: Option<String>,
    #[serde(rename = "format_long_name")]
    pub format_long_name: Option<String>,
    #[serde(rename = "start_time")]
    pub start_time: Option<String>,
    pub size: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Chapter {
    pub id: Option<i64>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub tags: Option<HashMap<String, String>>,
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
    pub streams: Vec<Stream>,
    pub format: Option<Format>,
    pub chapters: Vec<Chapter>,
    pub raw_json: Value,
}

pub async fn probe(path: &str) -> anyhow::Result<MediaMetadata> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_format")
        .arg("-show_streams")
        .arg("-show_chapters")
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

    let raw_json: Value =
        serde_json::from_slice(&output.stdout).context("failed to parse ffprobe json")?;
    let parsed: FfprobeStreams =
        serde_json::from_value(raw_json.clone()).context("failed to parse ffprobe json")?;

    let mut meta = MediaMetadata::default();
    let mut format_duration_seconds: Option<i32> = None;
    let mut stream_duration_seconds: Option<i32> = None;

    if let Some(fmt) = parsed.format.clone() {
        meta.container = fmt.format_name;
        meta.bitrate_bps = fmt.bit_rate.as_ref().and_then(|b| b.parse::<i64>().ok());
        format_duration_seconds = fmt
            .duration
            .as_ref()
            .and_then(|d| d.parse::<f32>().ok())
            .map(|d| d.round() as i32);
        meta.duration_seconds = format_duration_seconds;
    }

    meta.streams = parsed.streams.clone();
    meta.format = parsed.format;
    meta.chapters = parsed.chapters;
    meta.raw_json = raw_json;
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
        match timeout(
            PACKET_DURATION_PROBE_TIMEOUT,
            probe_video_duration_by_packets(path),
        )
        .await
        {
            Ok(Ok(packet_duration)) => {
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
            Ok(Err(err)) => {
                tracing::debug!(
                    path,
                    error = %err,
                    "ffprobe packet duration probe failed; using format duration"
                );
            }
            Err(_) => {
                tracing::warn!(
                    path,
                    timeout_seconds = PACKET_DURATION_PROBE_TIMEOUT.as_secs(),
                    "ffprobe packet duration probe timed out; using format duration"
                );
            }
        }
    }

    Ok(meta)
}

pub async fn ffprobe_version() -> anyhow::Result<String> {
    let output = Command::new("ffprobe")
        .arg("-version")
        .stdout(Stdio::piped())
        .output()
        .await
        .context("failed to spawn ffprobe -version")?;

    if !output.status.success() {
        anyhow::bail!(
            "ffprobe -version failed with code {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .next()
        .unwrap_or("ffprobe version unknown")
        .to_string())
}

async fn probe_video_duration_by_packets(path: &str) -> anyhow::Result<f32> {
    let mut command = Command::new("ffprobe");
    command.kill_on_drop(true);
    let output = command
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_large_matroska_chapter_ids() {
        let parsed: FfprobeStreams = serde_json::from_value(serde_json::json!({
            "streams": [],
            "format": {
                "format_name": "matroska,webm",
                "duration": "8673.152000",
                "size": "13725805503"
            },
            "chapters": [{
                "id": 6562625086751577810_i64,
                "time_base": "1/1000000000",
                "start": 0,
                "start_time": "0.000000",
                "end": 867313000000_i64,
                "end_time": "867.313000",
                "tags": {
                    "title": "Chapter 01"
                }
            }]
        }))
        .expect("ffprobe json should parse");

        assert_eq!(parsed.chapters[0].id, Some(6562625086751577810_i64));
    }
}
