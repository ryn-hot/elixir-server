use std::time::UNIX_EPOCH;

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{AnyPool, Row};

use crate::media::ffprobe;

pub const MEDIA_CAPABILITIES_PROBE_VERSION: i32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeStatus {
    Ok,
    ProbeRequired,
    ProbeFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaCapabilities {
    pub probe_version: i32,
    pub ffprobe_version: Option<String>,
    pub probe_status: ProbeStatus,
    pub probe_error: Option<String>,
    pub probed_at: Option<DateTime<Utc>>,
    pub path: Option<String>,
    pub container: ContainerCapabilities,
    pub duration_seconds: Option<f64>,
    pub size_bytes: Option<i64>,
    pub overall_bitrate_bps: Option<i64>,
    pub start_time_seconds: Option<f64>,
    pub video_streams: Vec<VideoStreamCapabilities>,
    pub audio_streams: Vec<AudioStreamCapabilities>,
    pub subtitle_streams: Vec<SubtitleStreamCapabilities>,
    pub chapters_present: bool,
    pub attachments_present: bool,
}

impl MediaCapabilities {
    pub fn probe_required(media_file_id: impl Into<String>) -> Self {
        Self {
            probe_version: MEDIA_CAPABILITIES_PROBE_VERSION,
            ffprobe_version: None,
            probe_status: ProbeStatus::ProbeRequired,
            probe_error: Some(format!("probe_required:{}", media_file_id.into())),
            probed_at: None,
            path: None,
            container: ContainerCapabilities::default(),
            duration_seconds: None,
            size_bytes: None,
            overall_bitrate_bps: None,
            start_time_seconds: None,
            video_streams: Vec::new(),
            audio_streams: Vec::new(),
            subtitle_streams: Vec::new(),
            chapters_present: false,
            attachments_present: false,
        }
    }

    pub fn probe_failed(path: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            probe_version: MEDIA_CAPABILITIES_PROBE_VERSION,
            ffprobe_version: None,
            probe_status: ProbeStatus::ProbeFailed,
            probe_error: Some(error.into()),
            probed_at: Some(Utc::now()),
            path: Some(path.into()),
            container: ContainerCapabilities::default(),
            duration_seconds: None,
            size_bytes: None,
            overall_bitrate_bps: None,
            start_time_seconds: None,
            video_streams: Vec::new(),
            audio_streams: Vec::new(),
            subtitle_streams: Vec::new(),
            chapters_present: false,
            attachments_present: false,
        }
    }

    pub fn primary_video(&self) -> Option<&VideoStreamCapabilities> {
        self.video_streams
            .iter()
            .find(|stream| stream.is_default)
            .or_else(|| self.video_streams.first())
    }

    pub fn primary_audio(&self) -> Option<&AudioStreamCapabilities> {
        self.audio_streams
            .iter()
            .find(|stream| stream.is_default)
            .or_else(|| self.audio_streams.first())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerCapabilities {
    pub format_names: Vec<String>,
    pub canonical: Option<String>,
    pub major_brand: Option<String>,
    pub compatible_brands: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VideoStreamCapabilities {
    pub index: Option<i32>,
    pub codec: Option<String>,
    pub profile: Option<String>,
    pub level: Option<i32>,
    pub pixel_format: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub frame_rate: Option<f64>,
    pub bit_depth: Option<i32>,
    pub bitrate_bps: Option<i64>,
    pub color_primaries: Option<String>,
    pub color_transfer: Option<String>,
    pub color_matrix: Option<String>,
    pub hdr10: bool,
    pub hdr10_plus: bool,
    pub dolby_vision: bool,
    pub mastering_metadata: bool,
    pub content_light_metadata: bool,
    pub dolby_vision_profile: Option<i32>,
    pub dolby_vision_has_hdr10_fallback: bool,
    pub is_default: bool,
    pub is_forced: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioStreamCapabilities {
    pub index: Option<i32>,
    pub codec: Option<String>,
    pub profile: Option<String>,
    pub channels: Option<i32>,
    pub channel_layout: Option<String>,
    pub sample_rate: Option<i32>,
    pub bitrate_bps: Option<i64>,
    pub language: Option<String>,
    pub title: Option<String>,
    pub is_default: bool,
    pub is_forced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubtitleKind {
    Text,
    Image,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtitleStreamCapabilities {
    pub index: Option<i32>,
    pub external_id: Option<String>,
    pub codec: Option<String>,
    pub kind: SubtitleKind,
    pub language: Option<String>,
    pub title: Option<String>,
    pub is_default: bool,
    pub is_forced: bool,
    pub is_hearing_impaired: bool,
    pub external_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbeSignature {
    size_bytes: Option<i64>,
    mtime_ms: Option<i64>,
}

#[derive(Debug, thiserror::Error)]
pub enum MediaProbeError {
    #[error("probe_required")]
    ProbeRequired,
    #[error("probe_failed: {0}")]
    ProbeFailed(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub async fn ensure_media_file_probe(
    pool: &AnyPool,
    media_file_id: &str,
    path: &str,
) -> Result<MediaCapabilities, MediaProbeError> {
    let signature = probe_signature(path).await.ok();

    if let Some(row) = sqlx::query::<sqlx::Any>(
        "SELECT probe_version,
                probe_status,
                normalized_json,
                CAST(source_size_bytes AS TEXT) AS source_size_bytes_text,
                CAST(source_mtime_ms AS TEXT) AS source_mtime_ms_text,
                error
         FROM media_file_probes WHERE media_file_id = ? LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await?
    {
        let probe_version: i32 = row.get("probe_version");
        let status: String = row.get("probe_status");
        let source_size_bytes = optional_i64_text(&row, "source_size_bytes_text");
        let source_mtime_ms = optional_i64_text(&row, "source_mtime_ms_text");
        let source_stale = signature
            .as_ref()
            .map(|current| {
                let size_stale = match (current.size_bytes, source_size_bytes) {
                    (Some(current), Some(stored)) => current != stored,
                    _ => false,
                };
                let mtime_stale = match (current.mtime_ms, source_mtime_ms) {
                    (Some(current), Some(stored)) => current != stored,
                    _ => false,
                };
                size_stale || mtime_stale
            })
            .unwrap_or(false);
        let stale = probe_version != MEDIA_CAPABILITIES_PROBE_VERSION || source_stale;

        if !stale {
            match status.as_str() {
                "ok" => {
                    let raw: String = row.get("normalized_json");
                    let capabilities = serde_json::from_str(&raw)
                        .context("failed to decode persisted media capabilities")?;
                    return Ok(capabilities);
                }
                "probe_failed" => {
                    let error = row
                        .try_get::<String, _>("error")
                        .unwrap_or_else(|_| "ffprobe failed".to_string());
                    return Err(MediaProbeError::ProbeFailed(error));
                }
                _ => {}
            }
        }
    }

    match ffprobe::probe(path).await {
        Ok(metadata) => {
            let capabilities =
                upsert_media_file_probe_success(pool, media_file_id, path, &metadata)
                    .await
                    .map_err(MediaProbeError::from)?;
            update_media_file_probe_projection(pool, media_file_id, &metadata).await?;
            Ok(capabilities)
        }
        Err(err) => {
            let message = err.to_string();
            upsert_media_file_probe_failure(pool, media_file_id, path, &message).await?;
            Err(MediaProbeError::ProbeFailed(message))
        }
    }
}

fn optional_i64_text(row: &sqlx::any::AnyRow, column: &str) -> Option<i64> {
    row.try_get::<String, _>(column)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
}

pub async fn upsert_media_file_probe_success(
    pool: &AnyPool,
    media_file_id: &str,
    path: &str,
    metadata: &ffprobe::MediaMetadata,
) -> anyhow::Result<MediaCapabilities> {
    let signature = probe_signature(path).await.ok();
    let ffprobe_version = ffprobe::ffprobe_version().await.ok();
    let capabilities =
        normalize_ffprobe_metadata(metadata, ffprobe_version, Some(path.to_string()));
    let normalized_json = serde_json::to_string(&capabilities)?;
    let raw_json = if metadata.raw_json.is_null() {
        None
    } else {
        Some(metadata.raw_json.to_string())
    };

    sqlx::query::<sqlx::Any>("DELETE FROM media_file_probes WHERE media_file_id = ?")
        .bind(media_file_id)
        .execute(pool)
        .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_file_probes
            (media_file_id, probe_version, ffprobe_version, probe_status, probed_at,
             source_mtime_ms, source_size_bytes, normalized_json, raw_json, error,
             created_at, updated_at)
         VALUES (?, ?, ?, 'ok', CURRENT_TIMESTAMP, ?, ?, ?, ?, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(media_file_id)
    .bind(MEDIA_CAPABILITIES_PROBE_VERSION)
    .bind(capabilities.ffprobe_version.clone())
    .bind(signature.as_ref().and_then(|sig| sig.mtime_ms))
    .bind(signature.as_ref().and_then(|sig| sig.size_bytes))
    .bind(normalized_json)
    .bind(raw_json)
    .execute(pool)
    .await?;

    if let Err(err) = crate::media_interactions::ingest_chapter_segments_from_metadata(
        pool,
        media_file_id,
        metadata,
    )
    .await
    {
        tracing::warn!(
            media_file_id,
            error = %err,
            "failed to ingest chapter media segments after probe success"
        );
    }

    Ok(capabilities)
}

pub async fn upsert_media_file_probe_failure(
    pool: &AnyPool,
    media_file_id: &str,
    path: &str,
    error: &str,
) -> anyhow::Result<()> {
    let signature = probe_signature(path).await.ok();
    sqlx::query::<sqlx::Any>("DELETE FROM media_file_probes WHERE media_file_id = ?")
        .bind(media_file_id)
        .execute(pool)
        .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_file_probes
            (media_file_id, probe_version, ffprobe_version, probe_status, probed_at,
             source_mtime_ms, source_size_bytes, normalized_json, raw_json, error,
             created_at, updated_at)
         VALUES (?, ?, NULL, 'probe_failed', CURRENT_TIMESTAMP, ?, ?, NULL, NULL, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(media_file_id)
    .bind(MEDIA_CAPABILITIES_PROBE_VERSION)
    .bind(signature.as_ref().and_then(|sig| sig.mtime_ms))
    .bind(signature.as_ref().and_then(|sig| sig.size_bytes))
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_media_file_probe_projection(
    pool: &AnyPool,
    media_file_id: &str,
    metadata: &ffprobe::MediaMetadata,
) -> anyhow::Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE media_files
         SET container = COALESCE(?, container),
             video_codec = COALESCE(?, video_codec),
             audio_codec = COALESCE(?, audio_codec),
             width = COALESCE(?, width),
             height = COALESCE(?, height),
             bitrate_bps = COALESCE(?, bitrate_bps),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?",
    )
    .bind(metadata.container.as_ref())
    .bind(
        metadata
            .video_codec
            .as_ref()
            .map(|codec| canonical_video_codec(codec)),
    )
    .bind(
        metadata
            .audio_codec
            .as_ref()
            .map(|codec| canonical_audio_codec(codec)),
    )
    .bind(metadata.width)
    .bind(metadata.height)
    .bind(metadata.bitrate_bps)
    .bind(media_file_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub fn normalize_ffprobe_metadata(
    metadata: &ffprobe::MediaMetadata,
    ffprobe_version: Option<String>,
    path: Option<String>,
) -> MediaCapabilities {
    let format = metadata.format.as_ref();
    let format_names = metadata
        .container
        .as_deref()
        .map(split_format_names)
        .unwrap_or_default();
    let tags = format.and_then(|fmt| fmt.tags.as_ref());
    let container = ContainerCapabilities {
        canonical: format_names
            .iter()
            .find_map(|name| canonical_container(name).filter(|value| !value.is_empty())),
        format_names,
        major_brand: read_tag(tags, "major_brand"),
        compatible_brands: read_tag(tags, "compatible_brands")
            .map(|brands| brands.split_whitespace().map(str::to_string).collect())
            .unwrap_or_default(),
    };

    let mut video_streams = Vec::new();
    let mut audio_streams = Vec::new();
    let mut subtitle_streams = Vec::new();
    let mut attachments_present = false;

    for stream in &metadata.streams {
        match stream.codec_type.as_deref() {
            Some("video") => video_streams.push(normalize_video_stream(stream)),
            Some("audio") => audio_streams.push(normalize_audio_stream(stream)),
            Some("subtitle") => subtitle_streams.push(normalize_subtitle_stream(stream, None)),
            Some("attachment") => attachments_present = true,
            _ => {}
        }
    }

    MediaCapabilities {
        probe_version: MEDIA_CAPABILITIES_PROBE_VERSION,
        ffprobe_version,
        probe_status: ProbeStatus::Ok,
        probe_error: None,
        probed_at: Some(Utc::now()),
        path,
        container,
        duration_seconds: metadata.duration_seconds.map(|seconds| seconds as f64),
        size_bytes: format
            .and_then(|fmt| fmt.size.as_deref())
            .and_then(parse_i64),
        overall_bitrate_bps: metadata.bitrate_bps,
        start_time_seconds: format
            .and_then(|fmt| fmt.start_time.as_deref())
            .and_then(parse_f64),
        video_streams,
        audio_streams,
        subtitle_streams,
        chapters_present: !metadata.chapters.is_empty(),
        attachments_present,
    }
}

fn normalize_video_stream(stream: &ffprobe::Stream) -> VideoStreamCapabilities {
    let codec = stream.codec_name.as_deref().map(canonical_video_codec);
    let side_data_text = stream
        .side_data_list
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let bit_depth = stream
        .bits_per_raw_sample
        .as_deref()
        .and_then(parse_i32)
        .or_else(|| bit_depth_from_pixel_format(stream.pix_fmt.as_deref()));
    let color_transfer = stream.color_transfer.as_deref().unwrap_or_default();
    let color_primaries = stream.color_primaries.as_deref().unwrap_or_default();
    let hdr10 = color_transfer.eq_ignore_ascii_case("smpte2084")
        && color_primaries.eq_ignore_ascii_case("bt2020");
    let dolby_vision_profile = find_side_data_i32(&stream.side_data_list, "dv_profile");
    let dolby_vision_bl_compat =
        find_side_data_i32(&stream.side_data_list, "dv_bl_signal_compatibility_id");
    let dolby_vision = side_data_text.contains("dovi")
        || side_data_text.contains("dolby vision")
        || dolby_vision_profile.is_some()
        || stream
            .codec_tag_string
            .as_deref()
            .is_some_and(|tag| tag.to_ascii_lowercase().contains("dvh"));
    let dolby_vision_has_hdr10_fallback = dolby_vision
        && hdr10
        && (matches!(dolby_vision_profile, Some(7 | 8))
            || dolby_vision_bl_compat.is_some_and(|compat| compat > 0));

    VideoStreamCapabilities {
        index: stream.index,
        codec,
        profile: stream.profile.clone(),
        level: stream.level,
        pixel_format: stream.pix_fmt.clone(),
        width: stream.width,
        height: stream.height,
        frame_rate: stream
            .avg_frame_rate
            .as_deref()
            .and_then(parse_ratio)
            .or_else(|| stream.r_frame_rate.as_deref().and_then(parse_ratio)),
        bit_depth,
        bitrate_bps: stream.bit_rate.as_deref().and_then(parse_i64),
        color_primaries: stream.color_primaries.clone(),
        color_transfer: stream.color_transfer.clone(),
        color_matrix: stream.color_space.clone(),
        hdr10,
        hdr10_plus: side_data_text.contains("hdr10+")
            || side_data_text.contains("dynamic hdr")
            || side_data_text.contains("itu-t t.35"),
        dolby_vision,
        mastering_metadata: side_data_text.contains("mastering display metadata"),
        content_light_metadata: side_data_text.contains("content light level metadata"),
        dolby_vision_profile,
        dolby_vision_has_hdr10_fallback,
        is_default: disposition_flag(stream, |d| d.default_flag),
        is_forced: disposition_flag(stream, |d| d.forced),
    }
}

fn normalize_audio_stream(stream: &ffprobe::Stream) -> AudioStreamCapabilities {
    AudioStreamCapabilities {
        index: stream.index,
        codec: stream.codec_name.as_deref().map(canonical_audio_codec),
        profile: stream.profile.clone(),
        channels: stream.channels,
        channel_layout: stream.channel_layout.clone(),
        sample_rate: stream.sample_rate.as_deref().and_then(parse_i32),
        bitrate_bps: stream.bit_rate.as_deref().and_then(parse_i64),
        language: normalize_language_tag(read_tag(stream.tags.as_ref(), "language")),
        title: read_tag(stream.tags.as_ref(), "title"),
        is_default: disposition_flag(stream, |d| d.default_flag),
        is_forced: disposition_flag(stream, |d| d.forced),
    }
}

fn normalize_subtitle_stream(
    stream: &ffprobe::Stream,
    external_path: Option<String>,
) -> SubtitleStreamCapabilities {
    let codec = stream.codec_name.as_deref().map(canonical_subtitle_codec);
    let kind = codec
        .as_deref()
        .map(subtitle_kind)
        .unwrap_or(SubtitleKind::Unknown);
    let title = read_tag(stream.tags.as_ref(), "title");
    SubtitleStreamCapabilities {
        index: stream.index,
        external_id: None,
        codec,
        kind,
        language: normalize_language_tag(read_tag(stream.tags.as_ref(), "language")),
        is_hearing_impaired: subtitle_hearing_impaired(stream, title.as_deref()),
        title,
        is_default: disposition_flag(stream, |d| d.default_flag),
        is_forced: disposition_flag(stream, |d| d.forced),
        external_path,
    }
}

pub fn canonical_container(raw: &str) -> Option<String> {
    let lower = raw.trim().to_ascii_lowercase();
    let value = match lower.as_str() {
        "matroska" | "matroska,webm" | "mkv" => "mkv",
        "webm" => "webm",
        "mov,mp4,m4a,3gp,3g2,mj2" | "mp4" | "m4v" | "mov" => "mp4",
        "mpegts" | "mpegtsraw" | "ts" => "mpegts",
        "avi" => "avi",
        "" => return None,
        _ => lower.as_str(),
    };
    Some(value.to_string())
}

pub fn canonical_video_codec(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "avc1" | "h.264" | "h264" => "h264".to_string(),
        "h.265" | "h265" | "hevc" => "hevc".to_string(),
        "mpeg2" | "mpeg2video" => "mpeg2video".to_string(),
        "av01" | "av1" => "av1".to_string(),
        "vp09" | "vp9" => "vp9".to_string(),
        value => value.to_string(),
    }
}

pub fn canonical_audio_codec(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "mp4a" | "aac" => "aac".to_string(),
        "a52" | "ac-3" | "ac3" => "ac3".to_string(),
        "ec-3" | "eac3" | "e-ac-3" => "eac3".to_string(),
        "dca" | "dts" => "dts".to_string(),
        "true-hd" | "truehd" | "mlp" => "truehd".to_string(),
        "libopus" | "opus" => "opus".to_string(),
        "mp3" | "mp3float" => "mp3".to_string(),
        value => value.to_string(),
    }
}

pub fn canonical_subtitle_codec(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "subrip" | "srt" => "srt".to_string(),
        "webvtt" | "vtt" => "webvtt".to_string(),
        "ass" => "ass".to_string(),
        "ssa" => "ssa".to_string(),
        "mov_text" | "tx3g" => "mov_text".to_string(),
        "hdmv_pgs_subtitle" | "pgs" | "sup" => "pgs".to_string(),
        "dvd_subtitle" | "dvdsub" | "vobsub" | "idx" | "sub" => "dvd_subtitle".to_string(),
        value => value.to_string(),
    }
}

pub fn subtitle_kind(codec: &str) -> SubtitleKind {
    match codec {
        "srt" | "webvtt" | "ass" | "ssa" | "mov_text" => SubtitleKind::Text,
        "pgs" | "dvd_subtitle" | "xsub" => SubtitleKind::Image,
        _ => SubtitleKind::Unknown,
    }
}

async fn probe_signature(path: &str) -> anyhow::Result<ProbeSignature> {
    let metadata = tokio::fs::metadata(path).await?;
    let size_bytes = Some(metadata.len() as i64);
    let mtime_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64);
    Ok(ProbeSignature {
        size_bytes,
        mtime_ms,
    })
}

fn split_format_names(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn read_tag(tags: Option<&std::collections::HashMap<String, String>>, key: &str) -> Option<String> {
    tags.and_then(|tags| {
        tags.iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
            .map(|(_, value)| value.clone())
    })
}

fn normalize_language_tag(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && value != "und")
}

fn disposition_flag<F>(stream: &ffprobe::Stream, accessor: F) -> bool
where
    F: FnOnce(&ffprobe::Disposition) -> Option<i32>,
{
    stream.disposition.as_ref().and_then(accessor).unwrap_or(0) == 1
}

fn subtitle_hearing_impaired(stream: &ffprobe::Stream, title: Option<&str>) -> bool {
    disposition_flag(stream, |d| d.hearing_impaired)
        || disposition_flag(stream, |d| d.captions)
        || disposition_flag(stream, |d| d.descriptions)
        || subtitle_title_suggests_hearing_impaired(title)
}

fn subtitle_title_suggests_hearing_impaired(title: Option<&str>) -> bool {
    let Some(title) = title else {
        return false;
    };
    let lower = title.to_ascii_lowercase();
    let compact: String = lower
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    if compact.contains("hearingimpaired") || compact.contains("closedcaptions") {
        return true;
    }
    lower
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|token| matches!(token, "sdh" | "cc" | "hi"))
}

fn parse_i32(raw: &str) -> Option<i32> {
    raw.trim().parse::<i32>().ok()
}

fn parse_i64(raw: &str) -> Option<i64> {
    raw.trim().parse::<i64>().ok()
}

fn parse_f64(raw: &str) -> Option<f64> {
    raw.trim().parse::<f64>().ok()
}

fn parse_ratio(raw: &str) -> Option<f64> {
    let raw = raw.trim();
    if raw == "0/0" || raw.is_empty() {
        return None;
    }
    if let Some((left, right)) = raw.split_once('/') {
        let numerator = left.parse::<f64>().ok()?;
        let denominator = right.parse::<f64>().ok()?;
        if denominator <= 0.0 {
            return None;
        }
        Some(numerator / denominator)
    } else {
        raw.parse::<f64>().ok()
    }
}

fn bit_depth_from_pixel_format(value: Option<&str>) -> Option<i32> {
    let value = value?.to_ascii_lowercase();
    if value.contains("12") {
        Some(12)
    } else if value.contains("10") || value.contains("p010") {
        Some(10)
    } else if value.contains("16") {
        Some(16)
    } else if value.contains("8") || value.contains("yuv") {
        Some(8)
    } else {
        None
    }
}

fn find_side_data_i32(values: &[Value], key: &str) -> Option<i32> {
    values.iter().find_map(|value| find_value_i32(value, key))
}

fn find_value_i32(value: &Value, key: &str) -> Option<i32> {
    match value {
        Value::Object(map) => {
            for (candidate, value) in map {
                if candidate.eq_ignore_ascii_case(key) {
                    if let Some(number) = value.as_i64() {
                        return Some(number as i32);
                    }
                    if let Some(text) = value.as_str() {
                        if let Ok(number) = text.trim().parse::<i32>() {
                            return Some(number);
                        }
                    }
                }
                if let Some(found) = find_value_i32(value, key) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(values) => values.iter().find_map(|value| find_value_i32(value, key)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_fixture(raw: &str) -> ffprobe::MediaMetadata {
        let value: Value = serde_json::from_str(raw).unwrap();
        let parsed: ffprobe::FfprobeStreams = serde_json::from_value(value.clone()).unwrap();
        ffprobe::MediaMetadata {
            container: parsed
                .format
                .as_ref()
                .and_then(|format| format.format_name.clone()),
            video_codec: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.codec_name.clone()),
            audio_codec: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("audio"))
                .and_then(|stream| stream.codec_name.clone()),
            width: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.width),
            height: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.height),
            bitrate_bps: parsed
                .format
                .as_ref()
                .and_then(|format| format.bit_rate.as_deref())
                .and_then(parse_i64),
            duration_seconds: parsed
                .format
                .as_ref()
                .and_then(|format| format.duration.as_deref())
                .and_then(parse_f64)
                .map(|seconds| seconds.round() as i32),
            streams: parsed.streams,
            format: parsed.format,
            chapters: parsed.chapters,
            raw_json: value,
        }
    }

    #[test]
    fn normalizes_hdr_and_subtitle_facts_from_fixture() {
        let metadata = parse_fixture(include_str!("fixtures/hevc_hdr_pgs.json"));
        let normalized = normalize_ffprobe_metadata(
            &metadata,
            Some("ffprobe fixture".to_string()),
            Some("fixture.mkv".to_string()),
        );

        let video = normalized.primary_video().unwrap();
        assert_eq!(video.codec.as_deref(), Some("hevc"));
        assert!(video.hdr10);
        assert_eq!(video.bit_depth, Some(10));
        assert!(video.mastering_metadata);
        assert!(video.content_light_metadata);
        assert!(normalized.chapters_present);
        assert!(normalized.attachments_present);

        let subtitle = normalized.subtitle_streams.first().unwrap();
        assert_eq!(subtitle.codec.as_deref(), Some("pgs"));
        assert_eq!(subtitle.kind, SubtitleKind::Image);
    }

    #[test]
    fn normalizes_subtitle_hearing_impaired_metadata() {
        let metadata = parse_fixture(
            r#"{
              "streams": [{
                "index": 2,
                "codec_type": "subtitle",
                "codec_name": "subrip",
                "tags": { "language": "eng", "title": "English SDH" },
                "disposition": {
                  "default": 1,
                  "forced": 0,
                  "hearing_impaired": 1
                }
              }]
            }"#,
        );
        let normalized =
            normalize_ffprobe_metadata(&metadata, None, Some("fixture.mkv".to_string()));
        let subtitle = normalized.subtitle_streams.first().unwrap();
        assert!(subtitle.is_hearing_impaired);
        assert_eq!(subtitle.title.as_deref(), Some("English SDH"));
    }

    #[test]
    fn canonicalizes_common_probe_codecs() {
        assert_eq!(canonical_video_codec("avc1"), "h264");
        assert_eq!(canonical_video_codec("h265"), "hevc");
        assert_eq!(canonical_audio_codec("DCA"), "dts");
        assert_eq!(canonical_subtitle_codec("hdmv_pgs_subtitle"), "pgs");
        assert_eq!(canonical_subtitle_codec("sup"), "pgs");
        assert_eq!(canonical_subtitle_codec("idx"), "dvd_subtitle");
        assert_eq!(canonical_subtitle_codec("sub"), "dvd_subtitle");
        assert_eq!(canonical_subtitle_codec("dvdsub"), "dvd_subtitle");
        assert_eq!(subtitle_kind("webvtt"), SubtitleKind::Text);
        assert_eq!(subtitle_kind("pgs"), SubtitleKind::Image);
        assert_eq!(subtitle_kind("dvd_subtitle"), SubtitleKind::Image);
        assert_eq!(subtitle_kind("xsub"), SubtitleKind::Image);
    }

    #[test]
    fn normalizes_dolby_vision_profile_and_hdr10_fallback() {
        let metadata = parse_fixture(
            r#"{
              "streams": [{
                "index": 0,
                "codec_type": "video",
                "codec_name": "hevc",
                "profile": "Main 10",
                "pix_fmt": "yuv420p10le",
                "width": 1920,
                "height": 1080,
                "color_primaries": "bt2020",
                "color_transfer": "smpte2084",
                "color_space": "bt2020nc",
                "side_data_list": [{
                  "side_data_type": "DOVI configuration record",
                  "dv_profile": 8,
                  "dv_bl_signal_compatibility_id": 1
                }]
              }],
              "format": { "format_name": "mov,mp4,m4a,3gp,3g2,mj2" }
            }"#,
        );

        let normalized = normalize_ffprobe_metadata(&metadata, None, None);
        let video = normalized.primary_video().unwrap();

        assert!(video.dolby_vision);
        assert_eq!(video.dolby_vision_profile, Some(8));
        assert!(video.dolby_vision_has_hdr10_fallback);
    }

    #[test]
    fn normalizes_hdr10_plus_from_frame_side_data_merged_into_stream() {
        let metadata = parse_fixture(
            r#"{
              "streams": [{
                "index": 0,
                "codec_type": "video",
                "codec_name": "hevc",
                "profile": "Main 10",
                "pix_fmt": "yuv420p10le",
                "width": 3840,
                "height": 2160,
                "color_primaries": "bt2020",
                "color_transfer": "smpte2084",
                "color_space": "bt2020nc",
                "side_data_list": [{
                  "side_data_type": "HDR Dynamic Metadata SMPTE2094-40 (HDR10+)"
                }]
              }],
              "format": { "format_name": "matroska,webm" }
            }"#,
        );

        let normalized = normalize_ffprobe_metadata(&metadata, None, None);
        let video = normalized.primary_video().unwrap();

        assert!(video.hdr10);
        assert!(video.hdr10_plus);
    }
}
