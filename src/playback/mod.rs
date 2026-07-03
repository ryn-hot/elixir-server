use std::{
    borrow::Cow,
    collections::HashMap,
    fs::File as StdFile,
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result};
use serde::Deserialize;
use sqlx::Row;
use tokio::{
    process::{Child, Command},
    time,
};
use uuid::Uuid;

use crate::metrics::PLAYBACK_SESSION_EXPIRATIONS;
use crate::playback::plan::{
    AdaptiveLadderPlan, AudioOutputPlan, Delivery, PlaybackMode, PlaybackPlan, StreamAction,
    SubtitleBurnInMode, VideoFrameRateMode, VideoOutputPlan,
};

pub mod certification;
pub mod decision;
pub mod hardware;
pub mod jobs;
pub mod network_emulator;
pub mod performance;
pub mod plan;
pub mod probe;
pub mod profile;
pub mod range;

#[cfg(test)]
mod corpus;

pub use jobs::{PlaybackJobCapacityLimits, PlaybackJobLimits, PlaybackJobManager, PlaybackJobPlan};

pub(crate) const HLS_SEGMENT_SECONDS: f64 = 4.0;
const DEFAULT_FPS: f64 = 24.0;
const MIN_GOP: i64 = 12;
const MAX_GOP: i64 = 300;

#[derive(Debug, Clone)]
pub struct TranscodeParams {
    pub seek_seconds: f32,
    pub mode: PlaybackMode,
    pub delivery: Delivery,
}

#[derive(Debug, Clone)]
pub struct SubtitleInfo {
    pub stream_index: i32,
    pub language: Option<String>,
    pub title: Option<String>,
    pub is_default: bool,
    pub is_forced: bool,
    pub is_hearing_impaired: bool,
}

#[derive(Debug, Clone)]
pub struct TranscodeHandle {
    pub playlist_path: PathBuf,
    pub log_path: PathBuf,
    pub temp_dir: PathBuf,
    pub pid: Option<u32>,
    pub process_group_id: Option<u32>,
    pub subtitles: Vec<SubtitleInfo>,
    pub job_state: serde_json::Value,
}

pub(crate) struct SpawnedFfmpeg {
    pub child: Child,
    pub command_line: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    MasterPlaylist,
    MediaPlaylist,
    InitSegment,
    MediaSegment,
    SubtitlePlaylist,
    SubtitleSegment,
}

#[derive(Debug, Clone)]
pub struct PlaybackArtifact {
    pub kind: ArtifactKind,
    pub name: String,
    pub path: PathBuf,
    pub temp_dir: PathBuf,
}

#[derive(Debug, Clone, Default)]
struct ArtifactRegistry {
    static_artifacts: HashMap<String, ArtifactKind>,
    segment_patterns: Vec<SegmentArtifactPattern>,
    adaptive: bool,
}

#[derive(Debug, Clone)]
struct SegmentArtifactPattern {
    prefix: String,
    suffix: String,
    kind: ArtifactKind,
}

impl ArtifactRegistry {
    fn for_plan(mode: PlaybackMode, delivery: Delivery, subtitle_count: usize) -> Self {
        if mode == PlaybackMode::DirectStream {
            return Self::for_direct_stream(delivery);
        }
        if mode == PlaybackMode::AdaptiveTranscode {
            Self::for_adaptive_transcode(subtitle_count)
        } else {
            Self::for_transcode(subtitle_count)
        }
    }

    fn for_direct_stream(delivery: Delivery) -> Self {
        let mut static_artifacts = HashMap::new();
        static_artifacts.insert("master.m3u8".to_string(), ArtifactKind::MasterPlaylist);
        static_artifacts.insert("media.m3u8".to_string(), ArtifactKind::MediaPlaylist);

        if matches!(delivery, Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4) {
            static_artifacts.insert("init.mp4".to_string(), ArtifactKind::InitSegment);
        }

        let extension = match delivery {
            Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => ".m4s",
            Delivery::DirectFile | Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts => ".ts",
        };
        Self {
            static_artifacts,
            segment_patterns: vec![SegmentArtifactPattern {
                prefix: "segment_".to_string(),
                suffix: extension.to_string(),
                kind: ArtifactKind::MediaSegment,
            }],
            adaptive: false,
        }
    }

    fn for_transcode(subtitle_count: usize) -> Self {
        Self::for_transcode_kind(subtitle_count, false)
    }

    fn for_adaptive_transcode(subtitle_count: usize) -> Self {
        Self::for_transcode_kind(subtitle_count, true)
    }

    fn for_transcode_kind(subtitle_count: usize, adaptive: bool) -> Self {
        let mut static_artifacts = HashMap::new();
        static_artifacts.insert("master.m3u8".to_string(), ArtifactKind::MasterPlaylist);
        static_artifacts.insert("stream_0.m3u8".to_string(), ArtifactKind::MediaPlaylist);

        let mut segment_patterns = vec![
            SegmentArtifactPattern {
                prefix: "seg_0_".to_string(),
                suffix: ".ts".to_string(),
                kind: ArtifactKind::MediaSegment,
            },
            SegmentArtifactPattern {
                prefix: "seg_0_".to_string(),
                suffix: ".m4s".to_string(),
                kind: ArtifactKind::MediaSegment,
            },
        ];

        for idx in 0..subtitle_count {
            static_artifacts.insert(format!("sub_{idx}.m3u8"), ArtifactKind::SubtitlePlaylist);
            segment_patterns.push(SegmentArtifactPattern {
                prefix: format!("sub_{idx}_"),
                suffix: ".vtt".to_string(),
                kind: ArtifactKind::SubtitleSegment,
            });
        }

        let mut registry = Self {
            static_artifacts,
            segment_patterns,
            adaptive,
        };
        registry.register_init_segments();
        registry
    }

    fn register_init_segments(&mut self) {
        for name in ["init.mp4", "init_0.mp4", "stream_0_init.mp4"] {
            self.static_artifacts
                .insert(name.to_string(), ArtifactKind::InitSegment);
        }
    }

    fn artifact_names(&self) -> Vec<String> {
        let mut names = self.static_artifacts.keys().cloned().collect::<Vec<_>>();
        if self.adaptive {
            names.extend(
                ["stream_*.m3u8", "init_*.mp4", "seg_*_*.ts", "seg_*_*.m4s"]
                    .into_iter()
                    .map(str::to_string),
            );
        }
        names.sort();
        names
    }

    fn resolve(&self, temp_dir: &Path, raw_name: &str) -> Option<PlaybackArtifact> {
        let name = normalize_artifact_name(raw_name)?;
        if let Some(kind) = self.static_artifacts.get(&name).copied() {
            return Some(PlaybackArtifact {
                kind,
                path: temp_dir.join(&name),
                temp_dir: temp_dir.to_path_buf(),
                name,
            });
        }
        if self.adaptive {
            if let Some(kind) = adaptive_artifact_kind(&name) {
                return Some(PlaybackArtifact {
                    kind,
                    path: temp_dir.join(&name),
                    temp_dir: temp_dir.to_path_buf(),
                    name,
                });
            }
        }

        let kind = self
            .segment_patterns
            .iter()
            .find(|pattern| pattern.matches(&name))
            .map(|pattern| pattern.kind)?;

        Some(PlaybackArtifact {
            kind,
            path: temp_dir.join(&name),
            temp_dir: temp_dir.to_path_buf(),
            name,
        })
    }
}

fn adaptive_artifact_kind(name: &str) -> Option<ArtifactKind> {
    if adaptive_stream_playlist_id(name).is_some() {
        return Some(ArtifactKind::MediaPlaylist);
    }
    if adaptive_init_segment_id(name).is_some() {
        return Some(ArtifactKind::InitSegment);
    }
    if adaptive_media_segment_id(name).is_some() {
        return Some(ArtifactKind::MediaSegment);
    }
    None
}

fn adaptive_stream_playlist_id(name: &str) -> Option<String> {
    let id = name
        .strip_prefix("stream_")
        .and_then(|rest| rest.strip_suffix(".m3u8"))?;
    valid_adaptive_rung_id(id).then(|| id.to_string())
}

fn adaptive_init_segment_id(name: &str) -> Option<String> {
    let id = name
        .strip_prefix("init_")
        .and_then(|rest| rest.strip_suffix(".mp4"))?;
    valid_adaptive_rung_id(id).then(|| id.to_string())
}

fn adaptive_media_segment_id(name: &str) -> Option<String> {
    let rest = name.strip_prefix("seg_")?;
    let (id, sequence) = rest.rsplit_once('_')?;
    let sequence = sequence
        .strip_suffix(".ts")
        .or_else(|| sequence.strip_suffix(".m4s"))?;
    (valid_adaptive_rung_id(id)
        && sequence.len() == 5
        && sequence.bytes().all(|b| b.is_ascii_digit()))
    .then(|| id.to_string())
}

fn valid_adaptive_rung_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 4 && id.bytes().all(|b| b.is_ascii_digit())
}

impl SegmentArtifactPattern {
    fn matches(&self, name: &str) -> bool {
        let Some(index) = name
            .strip_prefix(&self.prefix)
            .and_then(|rest| rest.strip_suffix(&self.suffix))
        else {
            return false;
        };

        index.len() == 5 && index.bytes().all(|b| b.is_ascii_digit())
    }
}

fn normalize_artifact_name(raw_name: &str) -> Option<String> {
    let decoded = match urlencoding::decode(raw_name).ok()? {
        Cow::Borrowed(value) => value.to_string(),
        Cow::Owned(value) => value,
    };
    let name = decoded.trim();
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || Path::new(name).is_absolute()
    {
        return None;
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return None;
    }
    Some(name.to_string())
}

#[derive(Debug, Clone)]
pub(crate) struct HlsOutputLayout {
    pub master_playlist_path: PathBuf,
    pub media_playlist_path: PathBuf,
    pub segment_template_path: PathBuf,
    pub direct_stream: bool,
}

impl HlsOutputLayout {
    pub(crate) fn for_job(temp_dir: &Path, mode: PlaybackMode, delivery: Delivery) -> Self {
        let extension = match delivery {
            Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => "m4s",
            Delivery::DirectFile | Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts => "ts",
        };
        let direct_stream = mode == PlaybackMode::DirectStream;
        let media_playlist_path = if direct_stream {
            temp_dir.join("media.m3u8")
        } else {
            temp_dir.join("stream_%v.m3u8")
        };
        let segment_template_path = if direct_stream {
            temp_dir.join(format!("segment_%05d.{extension}"))
        } else {
            temp_dir.join(format!("seg_%v_%05d.{extension}"))
        };
        Self {
            master_playlist_path: temp_dir.join("master.m3u8"),
            media_playlist_path,
            segment_template_path,
            direct_stream,
        }
    }
}

async fn spawn_ffmpeg(
    input: &str,
    params: &TranscodeParams,
    playback_plan: Option<&PlaybackPlan>,
    layout: &HlsOutputLayout,
    log_path: &Path,
    temp_dir: &Path,
    subtitles: &[SubtitleInfo],
) -> Result<SpawnedFfmpeg> {
    let log_file = StdFile::create(log_path).context("creating ffmpeg log file")?;
    let mut command = Command::new("ffmpeg");

    let args = if params.mode == PlaybackMode::DirectStream {
        build_direct_stream_ffmpeg_args(input, params, playback_plan, layout)
    } else {
        let fps = probe_video_fps(input).await.unwrap_or(DEFAULT_FPS);
        build_transcode_ffmpeg_args(
            input,
            params,
            playback_plan,
            layout,
            temp_dir,
            subtitles,
            fps,
        )
    };
    command.args(&args);

    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log_file));

    #[cfg(unix)]
    {
        command.process_group(0);
    }

    let child = command.spawn().context("failed to spawn ffmpeg")?;

    let mut command_line = Vec::with_capacity(args.len() + 1);
    command_line.push("ffmpeg".to_string());
    command_line.extend(args);

    Ok(SpawnedFfmpeg {
        child,
        command_line,
    })
}

pub(crate) fn build_direct_stream_ffmpeg_args(
    input: &str,
    params: &TranscodeParams,
    playback_plan: Option<&PlaybackPlan>,
    layout: &HlsOutputLayout,
) -> Vec<String> {
    let mut args = vec![
        "-hide_banner".to_string(),
        "-nostdin".to_string(),
        "-ss".to_string(),
        format!("{}", params.seek_seconds),
        "-i".to_string(),
        input.to_string(),
        "-map".to_string(),
        selected_video_map(playback_plan),
    ];

    let audio_enabled = stream_action_enabled(
        playback_plan.map(|plan| plan.audio_action),
        StreamAction::Copy,
    );
    if audio_enabled {
        args.push("-map".to_string());
        args.push(selected_audio_map(playback_plan));
    }

    args.extend(["-sn", "-dn", "-c:v", "copy"].into_iter().map(String::from));
    if audio_enabled {
        args.extend(["-c:a", "copy"].into_iter().map(String::from));
    } else {
        args.push("-an".to_string());
    }

    if let Some(bitstream_filter) =
        direct_stream_video_bitstream_filter(playback_plan, params.delivery)
    {
        args.push("-bsf:v".to_string());
        args.push(bitstream_filter.to_string());
    }

    let segment_seconds = format!("{}", HLS_SEGMENT_SECONDS);
    args.extend(
        [
            "-f",
            "hls",
            "-hls_time",
            segment_seconds.as_str(),
            "-hls_segment_type",
            hls_segment_type(params.delivery),
            "-hls_flags",
            "independent_segments+program_date_time",
        ]
        .into_iter()
        .map(String::from),
    );

    if matches!(
        params.delivery,
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4
    ) {
        args.push("-hls_fmp4_init_filename".to_string());
        args.push("init.mp4".to_string());
    }

    args.push("-master_pl_name".to_string());
    args.push("master.m3u8".to_string());
    args.push("-hls_segment_filename".to_string());
    args.push(ffmpeg_path(&layout.segment_template_path));
    args.push(ffmpeg_path(&layout.media_playlist_path));
    args
}

fn push_transcode_seek_args(args: &mut Vec<String>, seek_seconds: f32) {
    if seek_seconds.abs() < f32::EPSILON {
        args.push("-copyts".to_string());
        args.push("-start_at_zero".to_string());
    }
    args.push("-ss".to_string());
    args.push(format!("{}", seek_seconds));
}

pub(crate) fn build_transcode_ffmpeg_args(
    input: &str,
    params: &TranscodeParams,
    playback_plan: Option<&PlaybackPlan>,
    layout: &HlsOutputLayout,
    temp_dir: &Path,
    subtitles: &[SubtitleInfo],
    fps: f64,
) -> Vec<String> {
    if params.mode == PlaybackMode::AdaptiveTranscode {
        if let Some(plan) = playback_plan {
            if let Some(ladder) = plan.adaptive_ladder.as_ref() {
                return build_adaptive_transcode_ffmpeg_args(
                    input, params, plan, ladder, layout, temp_dir, subtitles, fps,
                );
            }
        }
    }

    let gop = ((fps * HLS_SEGMENT_SECONDS).round() as i64)
        .max(MIN_GOP)
        .min(MAX_GOP);
    let force_keyframes = format!("expr:gte(t,n_forced*{})", HLS_SEGMENT_SECONDS);
    let segment_seconds = format!("{}", HLS_SEGMENT_SECONDS);
    let mut args = vec![
        "-y".to_string(),
        "-hide_banner".to_string(),
        "-nostdin".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
    ];
    push_transcode_seek_args(&mut args, params.seek_seconds);
    push_hardware_decode_args(&mut args, playback_plan);
    args.extend(["-i".to_string(), input.to_string()]);
    if let Some(path) = external_burn_in_input_path(playback_plan) {
        push_external_burn_in_input_args(&mut args, path, params.seek_seconds);
    }

    if !subtitles.is_empty() {
        args.extend(
            [
                "-itsoffset".to_string(),
                format!("-{}", params.seek_seconds),
                "-ss".to_string(),
                format!("{}", params.seek_seconds),
                "-i".to_string(),
                input.to_string(),
            ]
            .into_iter(),
        );
    }

    let video_filter = planned_video_filter(input, playback_plan);
    if let Some(filter_complex) = video_filter.filter_complex.as_ref() {
        args.push("-filter_complex".to_string());
        args.push(filter_complex.clone());
    }
    if let Some(vf) = video_filter.vf.as_ref() {
        args.push("-vf".to_string());
        args.push(vf.clone());
    }

    args.push("-map".to_string());
    args.push(video_filter.video_map.clone());
    let audio_enabled = stream_action_enabled(
        playback_plan.map(|plan| plan.audio_action),
        StreamAction::Transcode,
    );
    if audio_enabled {
        args.push("-map".to_string());
        args.push(selected_audio_map(playback_plan));
    }

    match params.mode {
        PlaybackMode::AudioTranscode => {
            args.extend(["-c:v", "copy"].into_iter().map(String::from));
            if audio_enabled {
                push_audio_transcode_args(&mut args, playback_plan);
            } else {
                args.push("-an".to_string());
            }
        }
        PlaybackMode::SubtitleTranscode => {
            args.extend(["-c:v", "copy"].into_iter().map(String::from));
            if audio_enabled {
                args.extend(["-c:a", "copy"].into_iter().map(String::from));
            } else {
                args.push("-an".to_string());
            }
        }
        PlaybackMode::DirectStream => {
            args.extend(["-c:v", "copy"].into_iter().map(String::from));
            if audio_enabled {
                args.extend(["-c:a", "copy"].into_iter().map(String::from));
            } else {
                args.push("-an".to_string());
            }
        }
        PlaybackMode::DirectPlay
        | PlaybackMode::VideoTranscode
        | PlaybackMode::AdaptiveTranscode => {
            push_video_transcode_args(&mut args, playback_plan, gop, &force_keyframes);
            if audio_enabled {
                push_audio_transcode_args(&mut args, playback_plan);
            } else {
                args.push("-an".to_string());
            }
        }
    }

    args.extend(
        [
            "-f",
            "hls",
            "-avoid_negative_ts",
            "make_zero",
            "-hls_time",
            &segment_seconds,
            "-hls_flags",
            "independent_segments",
            "-hls_playlist_type",
            "event",
        ]
        .into_iter()
        .map(String::from),
    );

    if matches!(
        params.delivery,
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4
    ) {
        args.extend(
            [
                "-hls_segment_type".to_string(),
                "fmp4".to_string(),
                "-hls_fmp4_init_filename".to_string(),
                "init_0.mp4".to_string(),
            ]
            .into_iter(),
        );
    }

    args.extend(
        [
            "-master_pl_name".to_string(),
            "master.m3u8".to_string(),
            "-var_stream_map".to_string(),
            var_stream_map(audio_enabled),
            "-hls_segment_filename".to_string(),
            ffmpeg_path(&layout.segment_template_path),
            ffmpeg_path(&layout.media_playlist_path),
        ]
        .into_iter(),
    );

    for (idx, sub) in subtitles.iter().enumerate() {
        let subtitle_input_index = text_subtitle_input_index(playback_plan);
        let playlist = temp_dir.join(format!("sub_{idx}.m3u8"));
        let segment = temp_dir.join(format!("sub_{idx}_%05d.vtt"));
        args.extend(
            [
                "-map".to_string(),
                format!("{subtitle_input_index}:{}", sub.stream_index),
                "-c:s".to_string(),
                "webvtt".to_string(),
                "-f".to_string(),
                "segment".to_string(),
                "-segment_time".to_string(),
                segment_seconds.clone(),
                "-segment_format".to_string(),
                "webvtt".to_string(),
                "-segment_list".to_string(),
                ffmpeg_path(&playlist),
                "-segment_list_type".to_string(),
                "m3u8".to_string(),
                "-segment_list_flags".to_string(),
                "live".to_string(),
                "-segment_list_size".to_string(),
                "0".to_string(),
                ffmpeg_path(&segment),
            ]
            .into_iter(),
        );
    }

    args
}

fn build_adaptive_transcode_ffmpeg_args(
    input: &str,
    params: &TranscodeParams,
    plan: &PlaybackPlan,
    ladder: &AdaptiveLadderPlan,
    layout: &HlsOutputLayout,
    temp_dir: &Path,
    subtitles: &[SubtitleInfo],
    fps: f64,
) -> Vec<String> {
    let gop = ((fps * HLS_SEGMENT_SECONDS).round() as i64)
        .max(MIN_GOP)
        .min(MAX_GOP);
    let force_keyframes = format!("expr:gte(t,n_forced*{})", HLS_SEGMENT_SECONDS);
    let segment_seconds = format!("{}", HLS_SEGMENT_SECONDS);
    let audio_enabled = stream_action_enabled(Some(plan.audio_action), StreamAction::Transcode);
    let mut args = vec![
        "-y".to_string(),
        "-hide_banner".to_string(),
        "-nostdin".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
    ];
    push_transcode_seek_args(&mut args, params.seek_seconds);
    push_hardware_decode_args(&mut args, Some(plan));
    args.extend(["-i".to_string(), input.to_string()]);
    if let Some(path) = external_burn_in_input_path(Some(plan)) {
        push_external_burn_in_input_args(&mut args, path, params.seek_seconds);
    }

    if !subtitles.is_empty() {
        args.extend(
            [
                "-itsoffset".to_string(),
                format!("-{}", params.seek_seconds),
                "-ss".to_string(),
                format!("{}", params.seek_seconds),
                "-i".to_string(),
                input.to_string(),
            ]
            .into_iter(),
        );
    }

    args.push("-filter_complex".to_string());
    args.push(adaptive_video_filter_complex(input, plan, ladder));

    for (idx, rung) in ladder.rungs.iter().enumerate() {
        args.push("-map".to_string());
        args.push(format!("[v{}]", rung.id));
        if audio_enabled {
            args.push("-map".to_string());
            args.push(selected_audio_map(Some(plan)));
        }
        push_indexed_video_transcode_args(&mut args, &rung.video, idx, gop, &force_keyframes);
        if audio_enabled {
            push_indexed_audio_transcode_args(&mut args, plan.audio_output.as_ref(), idx);
        }
    }
    if !audio_enabled {
        args.push("-an".to_string());
    }

    args.extend(
        [
            "-f",
            "hls",
            "-avoid_negative_ts",
            "make_zero",
            "-hls_time",
            &segment_seconds,
            "-hls_flags",
            "independent_segments",
            "-hls_playlist_type",
            "event",
        ]
        .into_iter()
        .map(String::from),
    );

    if matches!(
        params.delivery,
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4
    ) {
        args.extend(
            [
                "-hls_segment_type".to_string(),
                "fmp4".to_string(),
                "-hls_fmp4_init_filename".to_string(),
                "init_%v.mp4".to_string(),
            ]
            .into_iter(),
        );
    }

    args.extend(
        [
            "-master_pl_name".to_string(),
            "master.m3u8".to_string(),
            "-var_stream_map".to_string(),
            adaptive_var_stream_map(ladder, audio_enabled),
            "-hls_segment_filename".to_string(),
            ffmpeg_path(&layout.segment_template_path),
            ffmpeg_path(&layout.media_playlist_path),
        ]
        .into_iter(),
    );

    for (idx, sub) in subtitles.iter().enumerate() {
        let subtitle_input_index = text_subtitle_input_index(Some(plan));
        let playlist = temp_dir.join(format!("sub_{idx}.m3u8"));
        let segment = temp_dir.join(format!("sub_{idx}_%05d.vtt"));
        args.extend(
            [
                "-map".to_string(),
                format!("{subtitle_input_index}:{}", sub.stream_index),
                "-c:s".to_string(),
                "webvtt".to_string(),
                "-f".to_string(),
                "segment".to_string(),
                "-segment_time".to_string(),
                segment_seconds.clone(),
                "-segment_format".to_string(),
                "webvtt".to_string(),
                "-segment_list".to_string(),
                ffmpeg_path(&playlist),
                "-segment_list_type".to_string(),
                "m3u8".to_string(),
                "-segment_list_flags".to_string(),
                "live".to_string(),
                "-segment_list_size".to_string(),
                "0".to_string(),
                ffmpeg_path(&segment),
            ]
            .into_iter(),
        );
    }

    args
}

fn adaptive_video_filter_complex(
    input: &str,
    plan: &PlaybackPlan,
    ladder: &AdaptiveLadderPlan,
) -> String {
    let selected_video = selected_video_map(Some(plan));
    let split_labels = ladder
        .rungs
        .iter()
        .map(|rung| format!("[vsrc{}]", rung.id))
        .collect::<Vec<_>>()
        .join("");
    let mut parts = vec![format!(
        "[{selected_video}]split={}{}",
        ladder.rungs.len(),
        split_labels
    )];
    for rung in &ladder.rungs {
        parts.push(adaptive_video_filter_branch(
            input,
            &format!("[vsrc{}]", rung.id),
            &format!("[v{}]", rung.id),
            &rung.video,
            &rung.id,
        ));
    }
    parts.join(";")
}

fn adaptive_video_filter_branch(
    input: &str,
    source_label: &str,
    output_label: &str,
    output: &VideoOutputPlan,
    rung_id: &str,
) -> String {
    let filters = planned_video_filters(output);
    match output.burn_in.as_ref() {
        Some(burn_in) if burn_in.mode == SubtitleBurnInMode::Image => {
            let subtitle_label = image_subtitle_filter_label(burn_in);
            if filters.is_empty() {
                format!("{source_label}[{subtitle_label}]overlay{output_label}")
            } else {
                format!(
                    "{source_label}{}[vbase{rung_id}];[vbase{rung_id}][{subtitle_label}]overlay{output_label}",
                    filters.join(","),
                )
            }
        }
        Some(burn_in) if burn_in.mode == SubtitleBurnInMode::AssSsaExactStyle => {
            let mut chain = filters;
            chain.push(format!(
                "subtitles={}:si={}",
                ffmpeg_filter_path(input),
                ass_subtitle_filter_stream_index(burn_in)
            ));
            format!("{source_label}{}{output_label}", chain.join(","))
        }
        _ if filters.is_empty() => format!("{source_label}null{output_label}"),
        _ => format!("{source_label}{}{output_label}", filters.join(",")),
    }
}

fn push_indexed_video_transcode_args(
    args: &mut Vec<String>,
    output: &VideoOutputPlan,
    index: usize,
    fallback_gop: i64,
    fallback_force_keyframes: &str,
) {
    args.push(format!("-c:v:{index}"));
    args.push(ffmpeg_video_encoder(&output.encoder).to_string());
    push_indexed_hardware_encoder_options(args, &output.encoder, index);
    if let Some(preset) = ffmpeg_video_preset(&output.encoder, &output.preset) {
        args.push(format!("-preset:v:{index}"));
        args.push(preset);
    }
    if let Some(profile) = output
        .profile
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push(format!("-profile:v:{index}"));
        args.push(profile.trim().to_string());
    }
    if let Some(level) = output
        .level
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push(format!("-level:v:{index}"));
        args.push(level.trim().to_string());
    }
    if let Some(pixel_format) = output
        .pixel_format
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push(format!("-pix_fmt:v:{index}"));
        args.push(pixel_format.trim().to_string());
    }
    if let Some(tone_map) = output.tone_map.as_ref() {
        args.push(format!("-color_primaries:v:{index}"));
        args.push(tone_map.output_primaries.trim().to_string());
        args.push(format!("-color_trc:v:{index}"));
        args.push(tone_map.output_transfer.trim().to_string());
        args.push(format!("-colorspace:v:{index}"));
        args.push(tone_map.output_matrix.trim().to_string());
    }
    match output.bitrate_bps {
        Some(bitrate_bps) => {
            args.push(format!("-b:v:{index}"));
            args.push(format_video_bitrate(bitrate_bps));
            if let Some(maxrate_bps) = output.maxrate_bps {
                args.push(format!("-maxrate:v:{index}"));
                args.push(format_video_bitrate(maxrate_bps));
            }
            if let Some(bufsize_bps) = output.bufsize_bps {
                args.push(format!("-bufsize:v:{index}"));
                args.push(format_video_bitrate(bufsize_bps));
            }
        }
        None => {
            if video_encoder_supports_crf(&output.encoder) {
                args.push(format!("-crf:v:{index}"));
                args.push(output.crf.unwrap_or(20).clamp(0, 51).to_string());
            } else {
                args.push(format!("-b:v:{index}"));
                args.push("8000k".to_string());
            }
        }
    }

    let gop = output
        .gop_frames
        .unwrap_or(fallback_gop as i32)
        .clamp(12, 300);
    args.extend(
        [
            format!("-g:v:{index}"),
            gop.to_string(),
            format!("-keyint_min:v:{index}"),
            gop.to_string(),
            format!("-sc_threshold:v:{index}"),
            "0".to_string(),
            format!("-force_key_frames:v:{index}"),
            output
                .keyframe_expression
                .clone()
                .if_empty(fallback_force_keyframes),
        ]
        .into_iter(),
    );
}

fn push_indexed_audio_transcode_args(
    args: &mut Vec<String>,
    output: Option<&AudioOutputPlan>,
    index: usize,
) {
    let fallback = AudioOutputPlan {
        codec: "aac".to_string(),
        channels: Some(2),
        bitrate_bps: Some(128_000),
        language: None,
        title: None,
        reasons: Vec::new(),
    };
    let output = output.unwrap_or(&fallback);
    args.push(format!("-c:a:{index}"));
    args.push(ffmpeg_audio_encoder(&output.codec).to_string());
    if let Some(bitrate_bps) = output.bitrate_bps {
        args.push(format!("-b:a:{index}"));
        args.push(format_audio_bitrate(bitrate_bps));
    }
    if let Some(channels) = output.channels.filter(|channels| *channels > 0) {
        args.push(format!("-ac:a:{index}"));
        args.push(channels.to_string());
    }
    if let Some(language) = output
        .language
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push(format!("-metadata:s:a:{index}"));
        args.push(format!("language={}", language.trim()));
    }
    if let Some(title) = output
        .title
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push(format!("-metadata:s:a:{index}"));
        args.push(format!("title={}", title.trim()));
    }
}

fn push_indexed_hardware_encoder_options(args: &mut Vec<String>, encoder: &str, index: usize) {
    if encoder.eq_ignore_ascii_case("h264_videotoolbox")
        || encoder.eq_ignore_ascii_case("hevc_videotoolbox")
    {
        args.extend([format!("-allow_sw:v:{index}"), "0".to_string()]);
    }
}

fn adaptive_var_stream_map(ladder: &AdaptiveLadderPlan, audio_enabled: bool) -> String {
    ladder
        .rungs
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            if audio_enabled {
                format!("v:{idx},a:{idx}")
            } else {
                format!("v:{idx}")
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

trait EmptyStringFallback {
    fn if_empty(self, fallback: &str) -> String;
}

impl EmptyStringFallback for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.trim().is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

#[derive(Debug, Clone)]
struct VideoFilterArgs {
    video_map: String,
    filter_complex: Option<String>,
    vf: Option<String>,
}

fn planned_video_filter(input: &str, playback_plan: Option<&PlaybackPlan>) -> VideoFilterArgs {
    let Some(plan) = playback_plan else {
        return VideoFilterArgs {
            video_map: selected_video_map(None),
            filter_complex: None,
            vf: None,
        };
    };
    let Some(output) = plan.video_output.as_ref() else {
        return VideoFilterArgs {
            video_map: selected_video_map(playback_plan),
            filter_complex: None,
            vf: None,
        };
    };

    let filters = planned_video_filters(output);
    let selected_video = selected_video_map(playback_plan);
    match output.burn_in.as_ref() {
        Some(burn_in) if burn_in.mode == SubtitleBurnInMode::Image => {
            let subtitle_label = image_subtitle_filter_label(burn_in);
            let filter_complex = if filters.is_empty() {
                format!("[{selected_video}][{subtitle_label}]overlay[vout]")
            } else {
                format!(
                    "[{selected_video}]{}[vbase];[vbase][{subtitle_label}]overlay[vout]",
                    filters.join(","),
                )
            };
            VideoFilterArgs {
                video_map: "[vout]".to_string(),
                filter_complex: Some(filter_complex),
                vf: None,
            }
        }
        Some(burn_in) if burn_in.mode == SubtitleBurnInMode::AssSsaExactStyle => {
            let mut chain = filters;
            chain.push(format!(
                "subtitles={}:si={}",
                ffmpeg_filter_path(input),
                ass_subtitle_filter_stream_index(burn_in)
            ));
            VideoFilterArgs {
                video_map: "[vout]".to_string(),
                filter_complex: Some(format!("[{selected_video}]{}[vout]", chain.join(","))),
                vf: None,
            }
        }
        _ if !filters.is_empty() => VideoFilterArgs {
            video_map: selected_video,
            filter_complex: None,
            vf: Some(filters.join(",")),
        },
        _ => VideoFilterArgs {
            video_map: selected_video,
            filter_complex: None,
            vf: None,
        },
    }
}

fn ass_subtitle_filter_stream_index(burn_in: &crate::playback::plan::SubtitleBurnInPlan) -> i32 {
    burn_in.filter_stream_index.unwrap_or(0).max(0)
}

fn image_subtitle_filter_label(burn_in: &crate::playback::plan::SubtitleBurnInPlan) -> String {
    if burn_in.external_path.is_some() {
        "1:0".to_string()
    } else {
        format!("0:{}", burn_in.stream_index)
    }
}

fn external_burn_in_input_path(playback_plan: Option<&PlaybackPlan>) -> Option<&str> {
    playback_plan?
        .video_output
        .as_ref()?
        .burn_in
        .as_ref()?
        .external_path
        .as_deref()
}

fn push_external_burn_in_input_args(args: &mut Vec<String>, path: &str, seek_seconds: f32) {
    if seek_seconds.abs() >= f32::EPSILON {
        args.extend(
            [
                "-itsoffset".to_string(),
                format!("-{seek_seconds}"),
                "-ss".to_string(),
                format!("{seek_seconds}"),
            ]
            .into_iter(),
        );
    }
    args.extend(["-i".to_string(), path.to_string()]);
}

fn text_subtitle_input_index(playback_plan: Option<&PlaybackPlan>) -> usize {
    if external_burn_in_input_path(playback_plan).is_some() {
        2
    } else {
        1
    }
}

fn planned_video_filters(output: &VideoOutputPlan) -> Vec<String> {
    let mut filters = Vec::new();
    let mut scale_consumed_by_tone_map = false;
    if let Some(tone_map) = output.tone_map.as_ref() {
        let mut linearize = Vec::new();
        if let Some(input_primaries) = tone_map.input_primaries.as_deref() {
            linearize.push(format!("pin={}", input_primaries.trim()));
            linearize.push(format!("p={}", input_primaries.trim()));
        }
        if let Some(input_transfer) = tone_map.input_transfer.as_deref() {
            linearize.push(format!("tin={}", input_transfer.trim()));
        }
        if let Some(input_matrix) = tone_map.input_matrix.as_deref() {
            linearize.push(format!("min={}", input_matrix.trim()));
        }
        linearize.extend([
            "t=linear".to_string(),
            "m=gbr".to_string(),
            "npl=100".to_string(),
        ]);
        filters.extend([
            format!("zscale={}", linearize.join(":")),
            "format=gbrpf32le".to_string(),
            format!("tonemap=tonemap={}:desat=0", tone_map.algorithm),
        ]);
        let mut output_zscale = vec![
            format!("p={}", tone_map.output_primaries),
            format!("t={}", tone_map.output_transfer),
            format!("m={}", tone_map.output_matrix),
            "r=tv".to_string(),
        ];
        if let Some(scale) = output.scale.as_ref() {
            output_zscale.push(format!("w={}", scale.width));
            output_zscale.push(format!("h={}", scale.height));
            scale_consumed_by_tone_map = true;
        }
        filters.push(format!("zscale={}", output_zscale.join(":")));
        filters.push("format=yuv420p".to_string());
    }
    if let Some(scale) = output
        .scale
        .as_ref()
        .filter(|_| !scale_consumed_by_tone_map)
    {
        filters.push(format!("scale={}:{}", scale.width, scale.height));
    }
    if output.frame_rate.mode == VideoFrameRateMode::Convert {
        if let Some(target_fps) = output.frame_rate.target_fps.as_ref() {
            filters.push(format!("fps={target_fps}"));
        }
    }
    filters
}

fn selected_video_map(playback_plan: Option<&PlaybackPlan>) -> String {
    playback_plan
        .and_then(|plan| plan.selected_video_track)
        .map(|index| format!("0:{index}"))
        .unwrap_or_else(|| "0:v:0".to_string())
}

fn selected_audio_map(playback_plan: Option<&PlaybackPlan>) -> String {
    playback_plan
        .and_then(|plan| plan.selected_audio_track)
        .map(|index| format!("0:{index}"))
        .unwrap_or_else(|| "0:a:0?".to_string())
}

fn stream_action_enabled(action: Option<StreamAction>, fallback: StreamAction) -> bool {
    !matches!(
        action.unwrap_or(fallback),
        StreamAction::Disabled | StreamAction::Drop
    )
}

fn push_video_transcode_args(
    args: &mut Vec<String>,
    playback_plan: Option<&PlaybackPlan>,
    fallback_gop: i64,
    fallback_force_keyframes: &str,
) {
    let fallback = VideoOutputPlan {
        codec: "h264".to_string(),
        encoder: "libx264".to_string(),
        preset: "veryfast".to_string(),
        profile: Some("high".to_string()),
        level: Some("4.1".to_string()),
        crf: Some(20),
        bitrate_bps: None,
        maxrate_bps: None,
        bufsize_bps: None,
        pixel_format: Some("yuv420p".to_string()),
        scale: None,
        tone_map: None,
        frame_rate: crate::playback::plan::VideoFrameRatePlan {
            mode: VideoFrameRateMode::Source,
            source_fps: None,
            target_fps: None,
        },
        gop_frames: Some(fallback_gop as i32),
        segment_seconds: HLS_SEGMENT_SECONDS.to_string(),
        keyframe_expression: fallback_force_keyframes.to_string(),
        hls_delivery: Delivery::HlsMpegts,
        burn_in: None,
        reasons: Vec::new(),
    };
    let output = playback_plan
        .and_then(|plan| plan.video_output.as_ref())
        .unwrap_or(&fallback);

    args.push("-c:v".to_string());
    args.push(ffmpeg_video_encoder(&output.encoder).to_string());
    push_hardware_encoder_options(args, &output.encoder);
    if let Some(preset) = ffmpeg_video_preset(&output.encoder, &output.preset) {
        args.push("-preset".to_string());
        args.push(preset);
    }
    if let Some(profile) = output
        .profile
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push("-profile:v".to_string());
        args.push(profile.trim().to_string());
    }
    if let Some(level) = output
        .level
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push("-level:v".to_string());
        args.push(level.trim().to_string());
    }
    if let Some(pixel_format) = output
        .pixel_format
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push("-pix_fmt".to_string());
        args.push(pixel_format.trim().to_string());
    }
    if let Some(tone_map) = output.tone_map.as_ref() {
        args.push("-color_primaries".to_string());
        args.push(tone_map.output_primaries.trim().to_string());
        args.push("-color_trc".to_string());
        args.push(tone_map.output_transfer.trim().to_string());
        args.push("-colorspace".to_string());
        args.push(tone_map.output_matrix.trim().to_string());
    }
    match output.bitrate_bps {
        Some(bitrate_bps) => {
            args.push("-b:v".to_string());
            args.push(format_video_bitrate(bitrate_bps));
            if let Some(maxrate_bps) = output.maxrate_bps {
                args.push("-maxrate".to_string());
                args.push(format_video_bitrate(maxrate_bps));
            }
            if let Some(bufsize_bps) = output.bufsize_bps {
                args.push("-bufsize".to_string());
                args.push(format_video_bitrate(bufsize_bps));
            }
        }
        None => {
            if video_encoder_supports_crf(&output.encoder) {
                args.push("-crf".to_string());
                args.push(output.crf.unwrap_or(20).clamp(0, 51).to_string());
            } else {
                args.push("-b:v".to_string());
                args.push("8000k".to_string());
            }
        }
    }

    let gop = output
        .gop_frames
        .unwrap_or(fallback_gop as i32)
        .clamp(12, 300);
    args.extend(
        [
            "-g".to_string(),
            gop.to_string(),
            "-keyint_min".to_string(),
            gop.to_string(),
            "-sc_threshold".to_string(),
            "0".to_string(),
            "-force_key_frames".to_string(),
            output.keyframe_expression.clone(),
        ]
        .into_iter(),
    );
}

fn push_hardware_decode_args(args: &mut Vec<String>, playback_plan: Option<&PlaybackPlan>) {
    let Some(plan) = playback_plan else {
        return;
    };
    let hardware = &plan.hardware_acceleration;
    if !hardware.enabled || hardware.decoder.is_none() {
        return;
    }
    match hardware.api.as_deref().unwrap_or_default() {
        "videotoolbox" => args.extend(["-hwaccel".to_string(), "videotoolbox".to_string()]),
        "vaapi" => args.extend(["-hwaccel".to_string(), "vaapi".to_string()]),
        "qsv" => {
            args.extend(["-hwaccel".to_string(), "qsv".to_string()]);
            if let Some(decoder) = hardware.decoder.as_deref() {
                if decoder.ends_with("_qsv") {
                    args.extend(["-c:v".to_string(), decoder.to_string()]);
                }
            }
        }
        "nvenc" => args.extend(["-hwaccel".to_string(), "cuda".to_string()]),
        "amf" => match hardware.decoder.as_deref().unwrap_or_default() {
            "d3d11va" => args.extend(["-hwaccel".to_string(), "d3d11va".to_string()]),
            "dxva2" => args.extend(["-hwaccel".to_string(), "dxva2".to_string()]),
            _ => {}
        },
        _ => {}
    }
}

fn push_hardware_encoder_options(args: &mut Vec<String>, encoder: &str) {
    if encoder.eq_ignore_ascii_case("h264_videotoolbox")
        || encoder.eq_ignore_ascii_case("hevc_videotoolbox")
    {
        args.extend(["-allow_sw".to_string(), "0".to_string()]);
    }
}

fn ffmpeg_video_preset(encoder: &str, preset: &str) -> Option<String> {
    let trimmed = preset.trim();
    if trimmed.is_empty() {
        return None;
    }
    match encoder.to_ascii_lowercase().as_str() {
        "libx264" | "h264" | "x264" => Some(trimmed.to_string()),
        "h264_nvenc" | "hevc_nvenc" | "av1_nvenc" => nvenc_preset(trimmed),
        _ => None,
    }
}

fn nvenc_preset(preset: &str) -> Option<String> {
    let lower = preset.to_ascii_lowercase();
    let mapped = match lower.as_str() {
        "ultrafast" | "superfast" | "veryfast" => "p1",
        "faster" | "fast" => "p2",
        "medium" | "default" => "p4",
        "slow" => "p6",
        "slower" | "veryslow" => "p7",
        "p1" | "p2" | "p3" | "p4" | "p5" | "p6" | "p7" | "hp" | "hq" | "bd" | "ll" | "llhq"
        | "llhp" | "lossless" | "losslesshp" => lower.as_str(),
        _ => return None,
    };
    Some(mapped.to_string())
}

fn video_encoder_supports_crf(encoder: &str) -> bool {
    matches!(
        encoder.to_ascii_lowercase().as_str(),
        "libx264" | "h264" | "x264"
    )
}

fn push_audio_transcode_args(args: &mut Vec<String>, playback_plan: Option<&PlaybackPlan>) {
    let fallback = AudioOutputPlan {
        codec: "aac".to_string(),
        channels: Some(2),
        bitrate_bps: Some(128_000),
        language: None,
        title: None,
        reasons: Vec::new(),
    };
    let output = playback_plan
        .and_then(|plan| plan.audio_output.as_ref())
        .unwrap_or(&fallback);

    args.push("-c:a".to_string());
    args.push(ffmpeg_audio_encoder(&output.codec).to_string());
    if let Some(bitrate_bps) = output.bitrate_bps {
        args.push("-b:a".to_string());
        args.push(format_audio_bitrate(bitrate_bps));
    }
    if let Some(channels) = output.channels.filter(|channels| *channels > 0) {
        args.push("-ac".to_string());
        args.push(channels.to_string());
    }
    if let Some(language) = output
        .language
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push("-metadata:s:a:0".to_string());
        args.push(format!("language={}", language.trim()));
    }
    if let Some(title) = output
        .title
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.push("-metadata:s:a:0".to_string());
        args.push(format!("title={}", title.trim()));
    }
}

fn ffmpeg_audio_encoder(codec: &str) -> &'static str {
    match codec.to_ascii_lowercase().as_str() {
        "ac3" => "ac3",
        "eac3" => "eac3",
        _ => "aac",
    }
}

fn ffmpeg_video_encoder(encoder: &str) -> &'static str {
    match encoder.to_ascii_lowercase().as_str() {
        "libx264" | "h264" | "x264" => "libx264",
        "h264_videotoolbox" => "h264_videotoolbox",
        "hevc_videotoolbox" => "hevc_videotoolbox",
        "h264_vaapi" => "h264_vaapi",
        "hevc_vaapi" => "hevc_vaapi",
        "h264_qsv" => "h264_qsv",
        "hevc_qsv" => "hevc_qsv",
        "h264_nvenc" => "h264_nvenc",
        "hevc_nvenc" => "hevc_nvenc",
        "h264_amf" => "h264_amf",
        "hevc_amf" => "hevc_amf",
        _ => "libx264",
    }
}

fn format_audio_bitrate(bitrate_bps: i64) -> String {
    if bitrate_bps > 0 && bitrate_bps % 1000 == 0 {
        format!("{}k", bitrate_bps / 1000)
    } else {
        bitrate_bps.to_string()
    }
}

fn format_video_bitrate(bitrate_bps: i64) -> String {
    format_audio_bitrate(bitrate_bps)
}

fn ffmpeg_filter_path(path: &str) -> String {
    let escaped = path
        .replace('\\', "\\\\")
        .replace(':', "\\:")
        .replace('\'', "\\'");
    format!("'{escaped}'")
}

fn ffmpeg_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn var_stream_map(audio_enabled: bool) -> String {
    if audio_enabled {
        "v:0,a:0".to_string()
    } else {
        "v:0".to_string()
    }
}

fn hls_segment_type(delivery: Delivery) -> &'static str {
    match delivery {
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => "fmp4",
        Delivery::DirectFile | Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts => "mpegts",
    }
}

fn direct_stream_video_bitstream_filter(
    playback_plan: Option<&PlaybackPlan>,
    delivery: Delivery,
) -> Option<&'static str> {
    if !matches!(delivery, Delivery::HlsMpegts | Delivery::HlsAdaptiveMpegts) {
        return None;
    }
    let plan = playback_plan?;
    let codec = plan
        .compatibility_report
        .source_video_codec
        .as_deref()
        .unwrap_or_default();
    if !codec.eq_ignore_ascii_case("h264") {
        return None;
    }
    let container = plan
        .compatibility_report
        .source_container
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(
        container.as_str(),
        "mp4" | "m4v" | "mov" | "quicktime" | "isom"
    )
    .then_some("h264_mp4toannexb")
}

async fn probe_video_fps(path: &str) -> Option<f64> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("v:0")
        .arg("-show_entries")
        .arg("stream=avg_frame_rate,r_frame_rate")
        .arg("-of")
        .arg("default=nw=1:nk=1")
        .arg(path)
        .output()
        .await
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if let Some(fps) = parse_fps(line.trim()) {
            return Some(fps);
        }
    }
    None
}

fn parse_fps(raw: &str) -> Option<f64> {
    if raw.is_empty() || raw == "0/0" {
        return None;
    }
    if let Some((num, den)) = raw.split_once('/') {
        let num = num.parse::<f64>().ok()?;
        let den = den.parse::<f64>().ok()?;
        if den > 0.0 {
            let fps = num / den;
            if fps.is_finite() && fps > 0.0 {
                return Some(fps);
            }
        }
    } else if let Ok(val) = raw.parse::<f64>() {
        if val.is_finite() && val > 0.0 {
            return Some(val);
        }
    }
    None
}

#[derive(Debug, Deserialize)]
struct SubtitleProbe {
    streams: Vec<SubtitleStream>,
}

#[derive(Debug, Deserialize)]
struct SubtitleStream {
    index: Option<i32>,
    codec_name: Option<String>,
    tags: Option<HashMap<String, String>>,
    disposition: Option<SubtitleDisposition>,
}

#[derive(Debug, Deserialize)]
struct SubtitleDisposition {
    default: Option<i32>,
    forced: Option<i32>,
    hearing_impaired: Option<i32>,
    captions: Option<i32>,
    descriptions: Option<i32>,
}

async fn detect_text_subtitles(
    path: &str,
    selected_stream_index: Option<i32>,
) -> Vec<SubtitleInfo> {
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
        let stream_index = stream.index.unwrap_or(idx as i32);
        if selected_stream_index.is_some_and(|selected| selected != stream_index) {
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
        let is_hearing_impaired = subtitle_info_hearing_impaired(&stream, title.as_deref());
        candidates.push(SubtitleInfo {
            stream_index,
            language,
            title,
            is_default,
            is_forced,
            is_hearing_impaired,
        });
    }

    candidates
}

fn subtitle_info_hearing_impaired(stream: &SubtitleStream, title: Option<&str>) -> bool {
    stream
        .disposition
        .as_ref()
        .map(|d| {
            d.hearing_impaired.unwrap_or(0) == 1
                || d.captions.unwrap_or(0) == 1
                || d.descriptions.unwrap_or(0) == 1
        })
        .unwrap_or(false)
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
    let rows = sqlx::query(
        "SELECT id, COALESCE(CAST(updated_at AS TEXT), '') AS updated_at FROM playback_sessions",
    )
    .fetch_all(&state.db_pool)
    .await?;

    let now = chrono::Utc::now();
    state.transcodes.cleanup_expired(now).await;
    let mut expired_ids = Vec::new();
    for row in rows {
        let id_str: String = row.get("id");
        let updated_str: String = row.get("updated_at");
        if let Some(updated_ts) = parse_timestamp(updated_str.trim()) {
            let age = now - updated_ts;
            if age.num_seconds() as u64 > ttl_seconds {
                if let Ok(id) = Uuid::parse_str(&id_str) {
                    expired_ids.push(id);
                }
            }
        }
    }

    for id in expired_ids {
        state.transcodes.stop(id, "session_expired").await;
        PLAYBACK_SESSION_EXPIRATIONS
            .with_label_values(&["ttl"])
            .inc();
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::{Context, Result};
    use serde_json::Value;
    use tempfile::tempdir;
    use tokio::process::Command;

    use super::*;
    use crate::playback::plan::{
        AdaptiveAudioStrategy, AdaptiveLadderPlan, AdaptiveRungPlan, CompatibilityCheck,
        CompatibilityReport, HardwareAccelerationPlan, HdrAction, PLAYBACK_PLAN_VERSION,
        SeekBehavior, SubtitleBurnInPlan, VideoFrameRatePlan, VideoScalePlan, VideoToneMapPlan,
    };

    fn direct_stream_plan(
        delivery: Delivery,
        container: &str,
        video_codec: &str,
        audio_codec: Option<&str>,
    ) -> PlaybackPlan {
        let mut report = CompatibilityReport::empty("media-file");
        report.checks.push(CompatibilityCheck::pass("fixture"));
        report.source_container = Some(container.to_string());
        report.source_video_codec = Some(video_codec.to_string());
        report.source_audio_codec = audio_codec.map(str::to_string);
        report.source_bitrate_bps = Some(1_000_000);
        report.source_width = Some(160);
        report.source_height = Some(90);
        report.selected_audio_track = audio_codec.map(|_| 1);

        PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode: PlaybackMode::DirectStream,
            delivery,
            media_file_id: "media-file".to_string(),
            selected_video_track: Some(0),
            video_action: StreamAction::Copy,
            audio_action: if audio_codec.is_some() {
                StreamAction::Copy
            } else {
                StreamAction::Disabled
            },
            subtitle_action: StreamAction::Disabled,
            seek_behavior: SeekBehavior::ServerHlsRestart,
            adaptive: false,
            selected_audio_track: audio_codec.map(|_| 1),
            selected_subtitle_track: None,
            hdr_action: HdrAction::None,
            hardware_acceleration: HardwareAccelerationPlan::default(),
            audio_output: None,
            video_output: None,
            adaptive_ladder: None,
            video_transcode_reason: None,
            workload_class: None,
            feasibility: None,
            compatibility_report: report,
            reasons: vec!["direct_stream_codecs_copyable_container_changed".to_string()],
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: true,
        }
    }

    fn hls_playback_plan(
        mode: PlaybackMode,
        delivery: Delivery,
        audio_action: StreamAction,
        subtitle_action: StreamAction,
        audio_output: Option<AudioOutputPlan>,
    ) -> PlaybackPlan {
        let mut report = CompatibilityReport::empty("media-file");
        report.checks.push(CompatibilityCheck::pass("fixture"));
        report.source_container = Some("matroska".to_string());
        report.source_video_codec = Some("h264".to_string());
        report.source_audio_codec = Some("dts".to_string());
        report.source_bitrate_bps = Some(1_000_000);
        report.source_width = Some(160);
        report.source_height = Some(90);
        report.selected_audio_track = Some(1);
        report.selected_subtitle_track =
            (subtitle_action == StreamAction::ConvertTextToWebvtt).then_some(2);

        PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode,
            delivery,
            media_file_id: "media-file".to_string(),
            selected_video_track: Some(0),
            video_action: if mode == PlaybackMode::VideoTranscode {
                StreamAction::Transcode
            } else {
                StreamAction::Copy
            },
            audio_action,
            subtitle_action,
            seek_behavior: SeekBehavior::ServerHlsRestart,
            adaptive: false,
            selected_audio_track: Some(1),
            selected_subtitle_track: report.selected_subtitle_track,
            hdr_action: HdrAction::None,
            hardware_acceleration: HardwareAccelerationPlan::default(),
            audio_output,
            video_output: None,
            adaptive_ladder: None,
            video_transcode_reason: None,
            workload_class: None,
            feasibility: None,
            compatibility_report: report,
            reasons: Vec::new(),
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: true,
        }
    }

    fn assert_arg_pair(args: &[String], flag: &str, value: &str) {
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == flag && pair[1] == value),
            "{flag} {value} missing from {args:?}"
        );
    }

    fn assert_arg_absent(args: &[String], value: &str) {
        assert!(
            !args.iter().any(|arg| arg == value),
            "{value} unexpectedly present in {args:?}"
        );
    }

    fn video_output_with_subtitle_burn_in(
        stream_index: i32,
        codec: &str,
        mode: SubtitleBurnInMode,
        scale: Option<VideoScalePlan>,
    ) -> VideoOutputPlan {
        VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "libx264".to_string(),
            preset: "slow".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(8_000_000),
            maxrate_bps: Some(8_000_000),
            bufsize_bps: Some(16_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale,
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: Some(SubtitleBurnInPlan {
                stream_index,
                filter_stream_index: None,
                external_path: None,
                codec: codec.to_string(),
                mode,
                reason: "selected_subtitle_requires_video_burn_in".to_string(),
            }),
            reasons: vec!["subtitle_requires_burn_in".to_string()],
        }
    }

    #[test]
    fn ffmpeg_hls_output_paths_use_forward_slashes_for_windows_paths() {
        let layout = HlsOutputLayout::for_job(
            Path::new(r"C:\Windows\System32\actions-runner\_work\elixir\artifacts\case\hls"),
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
        );
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Disabled,
            StreamAction::Disabled,
            None,
        );

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            Path::new(r"C:\Windows\System32\actions-runner\_work\elixir\artifacts\case\hls"),
            &[],
            24.0,
        );

        assert!(args.iter().any(|arg| {
            arg == "C:/Windows/System32/actions-runner/_work/elixir/artifacts/case/hls/seg_%v_%05d.m4s"
        }));
        assert!(args.iter().any(|arg| {
            arg == "C:/Windows/System32/actions-runner/_work/elixir/artifacts/case/hls/stream_%v.m3u8"
        }));
        assert!(
            !args.iter().any(|arg| arg.contains('\\')),
            "FFmpeg HLS output paths must not contain backslashes: {args:?}"
        );
    }

    #[test]
    fn direct_stream_hls_output_paths_use_forward_slashes_for_windows_paths() {
        let layout = HlsOutputLayout::for_job(
            Path::new(r"C:\runner\_work\elixir\artifacts\direct\hls"),
            PlaybackMode::DirectStream,
            Delivery::HlsFmp4,
        );
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::DirectStream,
            delivery: Delivery::HlsFmp4,
        };
        let plan = direct_stream_plan(Delivery::HlsFmp4, "mp4", "h264", Some("aac"));

        let args =
            build_direct_stream_ffmpeg_args("/media/source.mp4", &params, Some(&plan), &layout);

        assert!(
            args.iter()
                .any(|arg| arg == "C:/runner/_work/elixir/artifacts/direct/hls/segment_%05d.m4s")
        );
        assert!(
            args.iter()
                .any(|arg| arg == "C:/runner/_work/elixir/artifacts/direct/hls/media.m3u8")
        );
        assert!(
            !args.iter().any(|arg| arg.contains('\\')),
            "FFmpeg HLS output paths must not contain backslashes: {args:?}"
        );
    }

    #[test]
    fn nvenc_presets_translate_software_policy_names_to_ffmpeg_values() {
        assert_eq!(
            ffmpeg_video_preset("h264_nvenc", "veryfast").as_deref(),
            Some("p1")
        );
        assert_eq!(
            ffmpeg_video_preset("hevc_nvenc", "slow").as_deref(),
            Some("p6")
        );
        assert_eq!(
            ffmpeg_video_preset("h264_nvenc", "p4").as_deref(),
            Some("p4")
        );
        assert_eq!(ffmpeg_video_preset("h264_nvenc", "not-a-preset"), None);
        assert_eq!(
            ffmpeg_video_preset("libx264", "veryfast").as_deref(),
            Some("veryfast")
        );
    }

    fn adaptive_rung_video(width: i32, height: i32, bitrate_bps: i64) -> VideoOutputPlan {
        VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "libx264".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(bitrate_bps),
            maxrate_bps: Some(bitrate_bps),
            bufsize_bps: Some(bitrate_bps * 2),
            pixel_format: Some("yuv420p".to_string()),
            scale: Some(VideoScalePlan {
                width,
                height,
                reason: "adaptive_ladder_rung".to_string(),
            }),
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("24".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsAdaptiveFmp4,
            burn_in: None,
            reasons: vec!["adaptive_ladder_rung".to_string()],
        }
    }

    #[test]
    fn direct_stream_ffmpeg_args_copy_selected_tracks_for_fmp4() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::DirectStream, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 42.0,
            mode: PlaybackMode::DirectStream,
            delivery: Delivery::HlsFmp4,
        };
        let plan = direct_stream_plan(Delivery::HlsFmp4, "matroska", "h264", Some("aac"));

        let args =
            build_direct_stream_ffmpeg_args("/media/source.mkv", &params, Some(&plan), &layout);

        assert_arg_pair(&args, "-ss", "42");
        assert_arg_pair(&args, "-map", "0:0");
        assert_arg_pair(&args, "-map", "0:1");
        assert_arg_pair(&args, "-c:v", "copy");
        assert_arg_pair(&args, "-c:a", "copy");
        assert_arg_pair(&args, "-hls_segment_type", "fmp4");
        assert_arg_pair(
            &args,
            "-hls_flags",
            "independent_segments+program_date_time",
        );
        assert_arg_pair(&args, "-hls_fmp4_init_filename", "init.mp4");
        assert!(args.iter().any(|arg| arg.ends_with("segment_%05d.m4s")));
        assert!(args.iter().any(|arg| arg.ends_with("media.m3u8")));
        assert!(!args.iter().any(|arg| arg == "libx264" || arg == "-b:a"));
    }

    #[test]
    fn adaptive_transcode_ffmpeg_args_build_master_variants_and_aligned_rungs() {
        let temp = tempdir().unwrap();
        let layout = HlsOutputLayout::for_job(
            temp.path(),
            PlaybackMode::AdaptiveTranscode,
            Delivery::HlsAdaptiveFmp4,
        );
        let params = TranscodeParams {
            seek_seconds: 8.0,
            mode: PlaybackMode::AdaptiveTranscode,
            delivery: Delivery::HlsAdaptiveFmp4,
        };
        let first_video = adaptive_rung_video(1280, 720, 3_000_000);
        let second_video = adaptive_rung_video(854, 480, 1_200_000);
        let mut plan = hls_playback_plan(
            PlaybackMode::AdaptiveTranscode,
            Delivery::HlsAdaptiveFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: Some("eng".to_string()),
                title: Some("Stereo".to_string()),
                reasons: Vec::new(),
            }),
        );
        plan.adaptive = true;
        plan.video_action = StreamAction::Transcode;
        plan.video_output = Some(first_video.clone());
        plan.adaptive_ladder = Some(AdaptiveLadderPlan {
            rungs: vec![
                AdaptiveRungPlan {
                    id: "0".to_string(),
                    label: "720p 3000k".to_string(),
                    bandwidth_bps: 3_000_000,
                    average_bandwidth_bps: 2_700_000,
                    width: 1280,
                    height: 720,
                    resolution: "1280x720".to_string(),
                    codecs: "avc1.640029,mp4a.40.2".to_string(),
                    frame_rate: Some("24".to_string()),
                    video: first_video,
                },
                AdaptiveRungPlan {
                    id: "1".to_string(),
                    label: "480p 1200k".to_string(),
                    bandwidth_bps: 1_200_000,
                    average_bandwidth_bps: 1_080_000,
                    width: 854,
                    height: 480,
                    resolution: "854x480".to_string(),
                    codecs: "avc1.640029,mp4a.40.2".to_string(),
                    frame_rate: Some("24".to_string()),
                    video: second_video,
                },
            ],
            starting_rung_id: "0".to_string(),
            active_rung_id: "0".to_string(),
            audio_strategy: AdaptiveAudioStrategy::PerRung,
            reasons: vec!["adaptive_ladder_source_aware".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            24.0,
        );

        assert_arg_pair(
            &args,
            "-filter_complex",
            "[0:0]split=2[vsrc0][vsrc1];[vsrc0]scale=1280:720[v0];[vsrc1]scale=854:480[v1]",
        );
        assert_arg_pair(&args, "-ss", "8");
        assert_arg_absent(&args, "-copyts");
        assert_arg_absent(&args, "-start_at_zero");
        assert_arg_pair(&args, "-map", "[v0]");
        assert_arg_pair(&args, "-map", "[v1]");
        assert_arg_pair(&args, "-c:v:0", "libx264");
        assert_arg_pair(&args, "-c:v:1", "libx264");
        assert_arg_pair(&args, "-b:v:0", "3000k");
        assert_arg_pair(&args, "-b:v:1", "1200k");
        assert_arg_pair(&args, "-g:v:0", "96");
        assert_arg_pair(&args, "-g:v:1", "96");
        assert_arg_pair(&args, "-force_key_frames:v:0", "expr:gte(t,n_forced*4)");
        assert_arg_pair(&args, "-force_key_frames:v:1", "expr:gte(t,n_forced*4)");
        assert_arg_pair(&args, "-hls_fmp4_init_filename", "init_%v.mp4");
        assert_arg_pair(&args, "-master_pl_name", "master.m3u8");
        assert_arg_pair(&args, "-var_stream_map", "v:0,a:0 v:1,a:1");
        assert!(
            args.iter().any(|arg| arg.ends_with("seg_%v_%05d.m4s")),
            "{args:?}"
        );
        assert!(
            args.iter().any(|arg| arg.ends_with("stream_%v.m3u8")),
            "{args:?}"
        );
    }

    #[test]
    fn direct_stream_mpegts_adds_h264_annexb_filter_for_mp4_sources() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::DirectStream, Delivery::HlsMpegts);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::DirectStream,
            delivery: Delivery::HlsMpegts,
        };
        let plan = direct_stream_plan(Delivery::HlsMpegts, "mp4", "h264", Some("aac"));

        let args =
            build_direct_stream_ffmpeg_args("/media/source.mp4", &params, Some(&plan), &layout);

        assert_arg_pair(&args, "-c:v", "copy");
        assert_arg_pair(&args, "-c:a", "copy");
        assert_arg_pair(&args, "-hls_segment_type", "mpegts");
        assert_arg_pair(&args, "-bsf:v", "h264_mp4toannexb");
        assert!(args.iter().any(|arg| arg.ends_with("segment_%05d.ts")));
        assert!(!args.iter().any(|arg| arg == "-hls_fmp4_init_filename"));
    }

    #[test]
    fn audio_transcode_ffmpeg_args_copy_video_and_apply_audio_output_plan() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::AudioTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::AudioTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let plan = hls_playback_plan(
            PlaybackMode::AudioTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: Some("eng".to_string()),
                title: Some("DTS 5.1".to_string()),
                reasons: vec![
                    "audio_codec_conversion_required".to_string(),
                    "audio_channel_downmix_required".to_string(),
                    "audio_bitrate_cap_applied".to_string(),
                ],
            }),
        );

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            24.0,
        );

        assert_arg_pair(&args, "-map", "0:0");
        assert_arg_pair(&args, "-map", "0:1");
        assert_arg_pair(&args, "-c:v", "copy");
        assert_arg_pair(&args, "-c:a", "aac");
        assert_arg_pair(&args, "-b:a", "128k");
        assert_arg_pair(&args, "-ac", "2");
        assert_arg_pair(&args, "-metadata:s:a:0", "language=eng");
        assert_arg_pair(&args, "-metadata:s:a:0", "title=DTS 5.1");
        assert_arg_pair(&args, "-hls_fmp4_init_filename", "init_0.mp4");
        assert!(!args.iter().any(|arg| arg == "libx264"));
    }

    #[test]
    fn subtitle_transcode_ffmpeg_args_map_selected_global_subtitle_stream() {
        let temp = tempdir().unwrap();
        let layout = HlsOutputLayout::for_job(
            temp.path(),
            PlaybackMode::SubtitleTranscode,
            Delivery::HlsFmp4,
        );
        let params = TranscodeParams {
            seek_seconds: 12.0,
            mode: PlaybackMode::SubtitleTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let plan = hls_playback_plan(
            PlaybackMode::SubtitleTranscode,
            Delivery::HlsFmp4,
            StreamAction::Copy,
            StreamAction::ConvertTextToWebvtt,
            None,
        );
        let subtitles = vec![SubtitleInfo {
            stream_index: 2,
            language: Some("eng".to_string()),
            title: Some("Dialogue".to_string()),
            is_default: true,
            is_forced: false,
            is_hearing_impaired: false,
        }];

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &subtitles,
            24.0,
        );

        assert_arg_pair(&args, "-c:v", "copy");
        assert_arg_pair(&args, "-c:a", "copy");
        assert_arg_pair(&args, "-map", "1:2");
        assert!(
            !args
                .windows(2)
                .any(|pair| pair[0] == "-map" && pair[1] == "1:s:0")
        );
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_plan_and_image_subtitle_burn_in() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::BurnIn,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: Some("eng".to_string()),
                title: None,
                reasons: vec!["audio_codec_conversion_required".to_string()],
            }),
        );
        plan.selected_subtitle_track = Some(2);
        plan.video_transcode_reason = Some("subtitle_requires_burn_in".to_string());
        plan.video_output = Some(VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "libx264".to_string(),
            preset: "slow".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(8_000_000),
            maxrate_bps: Some(8_000_000),
            bufsize_bps: Some(16_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale: Some(VideoScalePlan {
                width: 1920,
                height: 1080,
                reason: "resolution_exceeds_policy".to_string(),
            }),
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: Some(SubtitleBurnInPlan {
                stream_index: 2,
                filter_stream_index: None,
                external_path: None,
                codec: "hdmv_pgs_subtitle".to_string(),
                mode: SubtitleBurnInMode::Image,
                reason: "selected_subtitle_requires_video_burn_in".to_string(),
            }),
            reasons: vec!["subtitle_requires_burn_in".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(
            &args,
            "-filter_complex",
            "[0:0]scale=1920:1080[vbase];[vbase][0:2]overlay[vout]",
        );
        assert_arg_pair(&args, "-copyts", "-start_at_zero");
        assert_arg_pair(&args, "-ss", "0");
        assert_arg_pair(&args, "-map", "[vout]");
        assert_arg_pair(&args, "-map", "0:1");
        assert!(
            !args
                .windows(2)
                .any(|pair| pair[0] == "-map" && pair[1] == "0:0"),
            "{args:?}"
        );
        assert_arg_pair(&args, "-c:v", "libx264");
        assert_arg_pair(&args, "-preset", "slow");
        assert_arg_pair(&args, "-profile:v", "high");
        assert_arg_pair(&args, "-level:v", "4.1");
        assert_arg_pair(&args, "-pix_fmt", "yuv420p");
        assert_arg_pair(&args, "-b:v", "8000k");
        assert_arg_pair(&args, "-maxrate", "8000k");
        assert_arg_pair(&args, "-bufsize", "16000k");
        assert_arg_pair(&args, "-g", "96");
        assert_arg_pair(&args, "-keyint_min", "96");
        assert_arg_pair(&args, "-sc_threshold", "0");
        assert_arg_pair(&args, "-force_key_frames", "expr:gte(t,n_forced*4)");
        assert!(!args.iter().any(|arg| arg == "-vf"));
        assert!(!args.iter().any(|arg| arg.contains("fps=")));
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_dvd_vobsub_image_subtitle_burn_in() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Copy,
            StreamAction::BurnIn,
            None,
        );
        plan.selected_subtitle_track = Some(3);
        plan.video_transcode_reason = Some("subtitle_requires_burn_in".to_string());
        plan.video_output = Some(video_output_with_subtitle_burn_in(
            3,
            "dvd_subtitle",
            SubtitleBurnInMode::Image,
            None,
        ));

        let args = build_transcode_ffmpeg_args(
            "/media/vobsub-source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(&args, "-filter_complex", "[0:0][0:3]overlay[vout]");
        assert_arg_pair(&args, "-map", "[vout]");
        assert_arg_pair(&args, "-map", "0:1");
        assert!(
            !args
                .iter()
                .any(|arg| arg.contains("subtitles=") || arg.contains(":si=")),
            "{args:?}"
        );
        assert!(!args.iter().any(|arg| arg == "-vf"));
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_external_image_subtitle_burn_in() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 12.5,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Copy,
            StreamAction::BurnIn,
            None,
        );
        plan.selected_subtitle_track = Some(-100_000);
        plan.video_transcode_reason = Some("subtitle_requires_burn_in".to_string());
        let mut output =
            video_output_with_subtitle_burn_in(0, "idx", SubtitleBurnInMode::Image, None);
        output.burn_in.as_mut().expect("burn-in plan").external_path =
            Some("/media/Phase17.External.VobSub.idx".to_string());
        plan.video_output = Some(output);

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(&args, "-i", "/media/source.mkv");
        assert_arg_pair(&args, "-i", "/media/Phase17.External.VobSub.idx");
        assert_arg_pair(&args, "-itsoffset", "-12.5");
        assert_arg_pair(&args, "-ss", "12.5");
        assert_arg_pair(&args, "-filter_complex", "[0:0][1:0]overlay[vout]");
        assert_arg_pair(&args, "-map", "[vout]");
        assert_arg_pair(&args, "-map", "0:1");
        assert!(
            !args
                .iter()
                .any(|arg| arg.contains("[0:-100000]") || arg.contains("[0:0]overlay")),
            "{args:?}"
        );
        assert!(!args.iter().any(|arg| arg == "-vf"));
    }

    #[test]
    fn video_transcode_ffmpeg_args_do_not_seek_zero_second_external_image_subtitle() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Copy,
            StreamAction::BurnIn,
            None,
        );
        plan.selected_subtitle_track = Some(-100_000);
        plan.video_transcode_reason = Some("subtitle_requires_burn_in".to_string());
        let mut output =
            video_output_with_subtitle_burn_in(0, "pgs", SubtitleBurnInMode::Image, None);
        output.burn_in.as_mut().expect("burn-in plan").external_path =
            Some("/media/Phase17.External.PGS.sup".to_string());
        plan.video_output = Some(output);

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(&args, "-copyts", "-start_at_zero");
        assert_arg_pair(&args, "-i", "/media/source.mkv");
        assert_arg_pair(&args, "-i", "/media/Phase17.External.PGS.sup");
        assert_arg_pair(&args, "-filter_complex", "[0:0][1:0]overlay[vout]");
        assert_arg_pair(&args, "-map", "[vout]");
        assert_eq!(
            args.iter().filter(|arg| arg.as_str() == "-ss").count(),
            1,
            "{args:?}"
        );
        assert!(
            !args.windows(4).any(|pair| {
                pair[0] == "-itsoffset" && pair[1] == "-0" && pair[2] == "-ss" && pair[3] == "0"
            }),
            "{args:?}"
        );
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_ass_ssa_exact_style_burn_in() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Copy,
            StreamAction::BurnIn,
            None,
        );
        plan.selected_subtitle_track = Some(4);
        plan.video_transcode_reason = Some("subtitle_requires_burn_in".to_string());
        plan.video_output = Some(video_output_with_subtitle_burn_in(
            4,
            "ass",
            SubtitleBurnInMode::AssSsaExactStyle,
            Some(VideoScalePlan {
                width: 1280,
                height: 720,
                reason: "resolution_exceeds_policy".to_string(),
            }),
        ));
        plan.video_output
            .as_mut()
            .and_then(|output| output.burn_in.as_mut())
            .unwrap()
            .filter_stream_index = Some(2);

        let args = build_transcode_ffmpeg_args(
            "/media/Phase 17's source:final.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(
            &args,
            "-filter_complex",
            "[0:0]scale=1280:720,subtitles='/media/Phase 17\\'s source\\:final.mkv':si=2[vout]",
        );
        assert_arg_pair(&args, "-map", "[vout]");
        assert_arg_pair(&args, "-map", "0:1");
        assert!(
            !args
                .iter()
                .any(|arg| arg.contains("overlay") || arg == "-vf"),
            "{args:?}"
        );
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_hdr_tone_map_filter() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: None,
                title: None,
                reasons: Vec::new(),
            }),
        );
        plan.video_output = Some(VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "libx264".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: Some(20),
            bitrate_bps: None,
            maxrate_bps: None,
            bufsize_bps: None,
            pixel_format: Some("yuv420p".to_string()),
            scale: None,
            tone_map: Some(VideoToneMapPlan {
                algorithm: "hable".to_string(),
                input_primaries: Some("bt2020".to_string()),
                input_transfer: Some("smpte2084".to_string()),
                input_matrix: Some("bt2020nc".to_string()),
                output_primaries: "bt709".to_string(),
                output_transfer: "bt709".to_string(),
                output_matrix: "bt709".to_string(),
            }),
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: None,
            reasons: vec!["hdr_to_sdr_required".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(
            &args,
            "-vf",
            "zscale=pin=bt2020:p=bt2020:tin=smpte2084:min=bt2020nc:t=linear:m=gbr:npl=100,format=gbrpf32le,tonemap=tonemap=hable:desat=0,zscale=p=bt709:t=bt709:m=bt709:r=tv,format=yuv420p",
        );
        assert_arg_pair(&args, "-color_primaries", "bt709");
        assert_arg_pair(&args, "-color_trc", "bt709");
        assert_arg_pair(&args, "-colorspace", "bt709");
        assert_arg_pair(&args, "-map", "0:0");
    }

    #[test]
    fn video_transcode_ffmpeg_args_fold_hdr_resize_into_output_zscale() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            None,
        );
        plan.video_output = Some(VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "h264_nvenc".to_string(),
            preset: "p1".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.2".to_string()),
            crf: None,
            bitrate_bps: Some(8_000_000),
            maxrate_bps: Some(8_000_000),
            bufsize_bps: Some(16_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale: Some(VideoScalePlan {
                width: 1920,
                height: 1080,
                reason: "resolution_exceeds_policy".to_string(),
            }),
            tone_map: Some(VideoToneMapPlan {
                algorithm: "hable".to_string(),
                input_primaries: Some("bt2020".to_string()),
                input_transfer: Some("smpte2084".to_string()),
                input_matrix: Some("bt2020nc".to_string()),
                output_primaries: "bt709".to_string(),
                output_transfer: "bt709".to_string(),
                output_matrix: "bt709".to_string(),
            }),
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: None,
            reasons: vec!["hdr_to_sdr_required".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(
            &args,
            "-vf",
            "zscale=pin=bt2020:p=bt2020:tin=smpte2084:min=bt2020nc:t=linear:m=gbr:npl=100,format=gbrpf32le,tonemap=tonemap=hable:desat=0,zscale=p=bt709:t=bt709:m=bt709:r=tv:w=1920:h=1080,format=yuv420p",
        );
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_videotoolbox_hardware_plan() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: None,
                title: None,
                reasons: Vec::new(),
            }),
        );
        plan.hardware_acceleration = HardwareAccelerationPlan {
            enabled: true,
            api: Some("videotoolbox".to_string()),
            decoder: Some("videotoolbox".to_string()),
            encoder: Some("h264_videotoolbox".to_string()),
            fallback: Some("software".to_string()),
            ..HardwareAccelerationPlan::default()
        };
        plan.video_output = Some(VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "h264_videotoolbox".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(3_000_000),
            maxrate_bps: Some(3_000_000),
            bufsize_bps: Some(6_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale: None,
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: None,
            reasons: vec!["hardware_encoder_selected:h264_videotoolbox".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(&args, "-hwaccel", "videotoolbox");
        assert_arg_pair(&args, "-c:v", "h264_videotoolbox");
        assert_arg_pair(&args, "-allow_sw", "0");
        assert_arg_pair(&args, "-profile:v", "high");
        assert_arg_pair(&args, "-level:v", "4.1");
        assert_arg_pair(&args, "-b:v", "3000k");
        assert!(!args.iter().any(|arg| arg == "-crf"));
        assert!(!args.iter().any(|arg| arg == "-preset"));
    }

    #[test]
    fn video_transcode_ffmpeg_args_translate_nvenc_preset() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: None,
                title: None,
                reasons: Vec::new(),
            }),
        );
        plan.hardware_acceleration = HardwareAccelerationPlan {
            enabled: true,
            api: Some("nvenc".to_string()),
            decoder: Some("cuda".to_string()),
            encoder: Some("h264_nvenc".to_string()),
            fallback: Some("software".to_string()),
            ..HardwareAccelerationPlan::default()
        };
        plan.video_output = Some(VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "h264_nvenc".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(3_000_000),
            maxrate_bps: Some(3_000_000),
            bufsize_bps: Some(6_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale: None,
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: None,
            reasons: vec!["hardware_encoder_selected:h264_nvenc".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(&args, "-hwaccel", "cuda");
        assert_arg_pair(&args, "-c:v", "h264_nvenc");
        assert_arg_pair(&args, "-preset", "p1");
        assert!(
            !args
                .windows(2)
                .any(|pair| pair[0] == "-preset" && pair[1] == "veryfast"),
            "{args:?}"
        );
    }

    #[test]
    fn adaptive_transcode_ffmpeg_args_translate_indexed_nvenc_presets() {
        let temp = tempdir().unwrap();
        let layout = HlsOutputLayout::for_job(
            temp.path(),
            PlaybackMode::AdaptiveTranscode,
            Delivery::HlsAdaptiveFmp4,
        );
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::AdaptiveTranscode,
            delivery: Delivery::HlsAdaptiveFmp4,
        };
        let mut first_video = adaptive_rung_video(1280, 720, 3_000_000);
        first_video.encoder = "h264_nvenc".to_string();
        first_video.preset = "veryfast".to_string();
        let mut second_video = adaptive_rung_video(854, 480, 1_200_000);
        second_video.encoder = "h264_nvenc".to_string();
        second_video.preset = "slow".to_string();
        let mut plan = hls_playback_plan(
            PlaybackMode::AdaptiveTranscode,
            Delivery::HlsAdaptiveFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: None,
                title: None,
                reasons: Vec::new(),
            }),
        );
        plan.hardware_acceleration = HardwareAccelerationPlan {
            enabled: true,
            api: Some("nvenc".to_string()),
            decoder: Some("cuda".to_string()),
            encoder: Some("h264_nvenc".to_string()),
            fallback: Some("software".to_string()),
            ..HardwareAccelerationPlan::default()
        };
        plan.adaptive = true;
        plan.video_action = StreamAction::Transcode;
        plan.video_output = Some(first_video.clone());
        plan.adaptive_ladder = Some(AdaptiveLadderPlan {
            rungs: vec![
                AdaptiveRungPlan {
                    id: "0".to_string(),
                    label: "720p 3000k".to_string(),
                    bandwidth_bps: 3_000_000,
                    average_bandwidth_bps: 2_700_000,
                    width: 1280,
                    height: 720,
                    resolution: "1280x720".to_string(),
                    codecs: "avc1.640029,mp4a.40.2".to_string(),
                    frame_rate: Some("24".to_string()),
                    video: first_video,
                },
                AdaptiveRungPlan {
                    id: "1".to_string(),
                    label: "480p 1200k".to_string(),
                    bandwidth_bps: 1_200_000,
                    average_bandwidth_bps: 1_080_000,
                    width: 854,
                    height: 480,
                    resolution: "854x480".to_string(),
                    codecs: "avc1.640029,mp4a.40.2".to_string(),
                    frame_rate: Some("24".to_string()),
                    video: second_video,
                },
            ],
            starting_rung_id: "0".to_string(),
            active_rung_id: "0".to_string(),
            audio_strategy: AdaptiveAudioStrategy::PerRung,
            reasons: vec!["adaptive_ladder_source_aware".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            24.0,
        );

        assert_arg_pair(&args, "-hwaccel", "cuda");
        assert_arg_pair(&args, "-c:v:0", "h264_nvenc");
        assert_arg_pair(&args, "-c:v:1", "h264_nvenc");
        assert_arg_pair(&args, "-preset:v:0", "p1");
        assert_arg_pair(&args, "-preset:v:1", "p6");
        assert!(
            !args.iter().any(|arg| arg == "veryfast" || arg == "slow"),
            "{args:?}"
        );
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_amf_with_windows_hardware_decode() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: None,
                title: None,
                reasons: Vec::new(),
            }),
        );
        plan.hardware_acceleration = HardwareAccelerationPlan {
            enabled: true,
            api: Some("amf".to_string()),
            decoder: Some("d3d11va".to_string()),
            encoder: Some("h264_amf".to_string()),
            fallback: Some("software".to_string()),
            ..HardwareAccelerationPlan::default()
        };
        plan.video_output = Some(VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "h264_amf".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(3_000_000),
            maxrate_bps: Some(3_000_000),
            bufsize_bps: Some(6_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale: None,
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: None,
            reasons: vec!["hardware_encoder_selected:h264_amf".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(&args, "-hwaccel", "d3d11va");
        assert_arg_pair(&args, "-c:v", "h264_amf");
    }

    #[test]
    fn video_transcode_ffmpeg_args_apply_qsv_with_explicit_hardware_decoder() {
        let temp = tempdir().unwrap();
        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::VideoTranscode, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
        };
        let mut plan = hls_playback_plan(
            PlaybackMode::VideoTranscode,
            Delivery::HlsFmp4,
            StreamAction::Transcode,
            StreamAction::Disabled,
            Some(AudioOutputPlan {
                codec: "aac".to_string(),
                channels: Some(2),
                bitrate_bps: Some(128_000),
                language: None,
                title: None,
                reasons: Vec::new(),
            }),
        );
        plan.hardware_acceleration = HardwareAccelerationPlan {
            enabled: true,
            api: Some("qsv".to_string()),
            decoder: Some("h264_qsv".to_string()),
            encoder: Some("h264_qsv".to_string()),
            fallback: Some("software".to_string()),
            ..HardwareAccelerationPlan::default()
        };
        plan.video_output = Some(VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "h264_qsv".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(3_000_000),
            maxrate_bps: Some(3_000_000),
            bufsize_bps: Some(6_000_000),
            pixel_format: Some("yuv420p".to_string()),
            scale: None,
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("23.976".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsFmp4,
            burn_in: None,
            reasons: vec!["hardware_encoder_selected:h264_qsv".to_string()],
        });

        let args = build_transcode_ffmpeg_args(
            "/media/source.mkv",
            &params,
            Some(&plan),
            &layout,
            temp.path(),
            &[],
            23.976,
        );

        assert_arg_pair(&args, "-hwaccel", "qsv");
        let input_index = args.iter().position(|arg| arg == "-i").unwrap();
        assert_arg_pair(&args[..input_index], "-c:v", "h264_qsv");
        assert_arg_pair(&args[input_index..], "-c:v", "h264_qsv");
    }

    #[test]
    fn direct_stream_artifact_registry_registers_remux_outputs() {
        let temp = tempdir().unwrap();
        let registry = ArtifactRegistry::for_plan(PlaybackMode::DirectStream, Delivery::HlsFmp4, 0);

        assert_eq!(
            registry
                .resolve(temp.path(), "master.m3u8")
                .map(|artifact| artifact.kind),
            Some(ArtifactKind::MasterPlaylist)
        );
        assert_eq!(
            registry
                .resolve(temp.path(), "media.m3u8")
                .map(|artifact| artifact.kind),
            Some(ArtifactKind::MediaPlaylist)
        );
        assert_eq!(
            registry
                .resolve(temp.path(), "init.mp4")
                .map(|artifact| artifact.kind),
            Some(ArtifactKind::InitSegment)
        );
        assert_eq!(
            registry
                .resolve(temp.path(), "segment_00000.m4s")
                .map(|artifact| artifact.kind),
            Some(ArtifactKind::MediaSegment)
        );
        assert!(registry.resolve(temp.path(), "stream_0.m3u8").is_none());
        assert!(registry.resolve(temp.path(), "seg_0_00000.m4s").is_none());
    }

    #[tokio::test]
    async fn direct_stream_remux_fixture_preserves_codecs_and_resolution() -> Result<()> {
        if !tool_available("ffmpeg").await || !tool_available("ffprobe").await {
            eprintln!("skipping Direct Stream remux fixture: ffmpeg or ffprobe unavailable");
            return Ok(());
        }

        let temp = tempdir()?;
        let source = temp.path().join("source.mp4");
        let generated = Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=160x90:rate=24:duration=1",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000:sample_rate=48000:duration=1",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-shortest",
            ])
            .arg(&source)
            .output()
            .await?;
        if !generated.status.success() {
            eprintln!(
                "skipping Direct Stream remux fixture: unable to generate h264/aac fixture: {}",
                String::from_utf8_lossy(&generated.stderr)
            );
            return Ok(());
        }

        let layout =
            HlsOutputLayout::for_job(temp.path(), PlaybackMode::DirectStream, Delivery::HlsFmp4);
        let params = TranscodeParams {
            seek_seconds: 0.0,
            mode: PlaybackMode::DirectStream,
            delivery: Delivery::HlsFmp4,
        };
        let plan = direct_stream_plan(Delivery::HlsFmp4, "mp4", "h264", Some("aac"));
        let source_path = source
            .to_str()
            .context("source fixture path is not utf-8")?;
        let args = build_direct_stream_ffmpeg_args(source_path, &params, Some(&plan), &layout);

        let remuxed = Command::new("ffmpeg").args(&args).output().await?;
        assert!(
            remuxed.status.success(),
            "Direct Stream remux failed: {}",
            String::from_utf8_lossy(&remuxed.stderr)
        );

        let source_probe = probe_media(&source).await?;
        let playlist_probe = probe_media(&layout.media_playlist_path).await?;
        let source_video = first_stream(&source_probe, "video")?;
        let output_video = first_stream(&playlist_probe, "video")?;
        let source_audio = first_stream(&source_probe, "audio")?;
        let output_audio = first_stream(&playlist_probe, "audio")?;

        assert_eq!(stream_string(source_video, "codec_name"), Some("h264"));
        assert_eq!(stream_string(output_video, "codec_name"), Some("h264"));
        assert_eq!(
            stream_i64(output_video, "width"),
            stream_i64(source_video, "width")
        );
        assert_eq!(
            stream_i64(output_video, "height"),
            stream_i64(source_video, "height")
        );
        assert_eq!(stream_string(source_audio, "codec_name"), Some("aac"));
        assert_eq!(stream_string(output_audio, "codec_name"), Some("aac"));
        Ok(())
    }

    async fn tool_available(tool: &str) -> bool {
        Command::new(tool)
            .arg("-version")
            .output()
            .await
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    async fn probe_media(path: &Path) -> Result<Value> {
        let output = Command::new("ffprobe")
            .args(["-v", "error", "-show_streams", "-print_format", "json"])
            .arg(path)
            .output()
            .await?;
        assert!(
            output.status.success(),
            "ffprobe failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn first_stream<'a>(probe: &'a Value, codec_type: &str) -> Result<&'a Value> {
        probe
            .get("streams")
            .and_then(Value::as_array)
            .and_then(|streams| {
                streams.iter().find(|stream| {
                    stream
                        .get("codec_type")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == codec_type)
                })
            })
            .with_context(|| format!("{codec_type} stream missing from {probe}"))
    }

    fn stream_string<'a>(stream: &'a Value, key: &str) -> Option<&'a str> {
        stream.get(key).and_then(Value::as_str)
    }

    fn stream_i64(stream: &Value, key: &str) -> Option<i64> {
        stream.get(key).and_then(Value::as_i64)
    }
}
