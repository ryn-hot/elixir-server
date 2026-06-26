use serde::{Deserialize, Serialize};

pub const PLAYBACK_PLAN_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackMode {
    DirectPlay,
    DirectStream,
    AudioTranscode,
    SubtitleTranscode,
    VideoTranscode,
    AdaptiveTranscode,
}

impl PlaybackMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirectPlay => "direct_play",
            Self::DirectStream => "direct_stream",
            Self::AudioTranscode => "audio_transcode",
            Self::SubtitleTranscode => "subtitle_transcode",
            Self::VideoTranscode => "video_transcode",
            Self::AdaptiveTranscode => "adaptive_transcode",
        }
    }

    pub fn is_hls_producing(self) -> bool {
        !matches!(self, Self::DirectPlay)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    DirectFile,
    HlsFmp4,
    HlsMpegts,
    HlsAdaptiveFmp4,
    HlsAdaptiveMpegts,
}

impl Delivery {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirectFile => "direct_file",
            Self::HlsFmp4 => "hls_fmp4",
            Self::HlsMpegts => "hls_mpegts",
            Self::HlsAdaptiveFmp4 => "hls_adaptive_fmp4",
            Self::HlsAdaptiveMpegts => "hls_adaptive_mpegts",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamAction {
    Copy,
    Transcode,
    Drop,
    BurnIn,
    Passthrough,
    ConvertTextToWebvtt,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeekBehavior {
    ClientRange,
    ServerHlsRestart,
    HlsNative,
}

impl SeekBehavior {
    pub fn server_seek_required(self) -> bool {
        matches!(self, Self::ServerHlsRestart)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HdrAction {
    None,
    Direct,
    ToneMapToSdr,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityReport {
    pub media_file_id_valid: bool,
    pub selected_audio_track: Option<i32>,
    pub selected_subtitle_track: Option<i32>,
    pub requested_start_seconds: Option<i32>,
    pub source_container: Option<String>,
    pub source_video_codec: Option<String>,
    pub source_audio_codec: Option<String>,
    pub source_subtitle_codec: Option<String>,
    pub source_bitrate_bps: Option<i64>,
    pub source_width: Option<i32>,
    pub source_height: Option<i32>,
    pub checks: Vec<CompatibilityCheck>,
}

impl CompatibilityReport {
    pub fn empty(media_file_id: &str) -> Self {
        Self {
            media_file_id_valid: !media_file_id.trim().is_empty(),
            selected_audio_track: None,
            selected_subtitle_track: None,
            requested_start_seconds: None,
            source_container: None,
            source_video_codec: None,
            source_audio_codec: None,
            source_subtitle_codec: None,
            source_bitrate_bps: None,
            source_width: None,
            source_height: None,
            checks: Vec::new(),
        }
    }

    pub fn failed_reasons(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| !check.passed)
            .filter_map(|check| check.reason.clone())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityCheck {
    pub category: String,
    pub passed: bool,
    pub reason: Option<String>,
}

impl CompatibilityCheck {
    pub fn pass(category: impl Into<String>) -> Self {
        Self {
            category: category.into(),
            passed: true,
            reason: None,
        }
    }

    pub fn fail(category: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            category: category.into(),
            passed: false,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedOutput {
    pub name: String,
    pub kind: String,
}

impl ExpectedOutput {
    pub fn new(name: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareAccelerationPlan {
    pub enabled: bool,
    pub api: Option<String>,
    pub decoder: Option<String>,
    pub encoder: Option<String>,
    pub fallback: Option<String>,
}

impl Default for HardwareAccelerationPlan {
    fn default() -> Self {
        Self {
            enabled: false,
            api: None,
            decoder: None,
            encoder: None,
            fallback: Some("software".to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioOutputPlan {
    pub codec: String,
    pub channels: Option<i32>,
    pub bitrate_bps: Option<i64>,
    pub language: Option<String>,
    pub title: Option<String>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoFrameRateMode {
    Source,
    Convert,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoFrameRatePlan {
    pub mode: VideoFrameRateMode,
    pub source_fps: Option<String>,
    pub target_fps: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoScalePlan {
    pub width: i32,
    pub height: i32,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubtitleBurnInMode {
    Image,
    AssSsaExactStyle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtitleBurnInPlan {
    pub stream_index: i32,
    pub codec: String,
    pub mode: SubtitleBurnInMode,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoToneMapPlan {
    pub algorithm: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_primaries: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_transfer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_matrix: Option<String>,
    pub output_primaries: String,
    pub output_transfer: String,
    pub output_matrix: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoOutputPlan {
    pub codec: String,
    pub encoder: String,
    pub preset: String,
    pub profile: Option<String>,
    pub level: Option<String>,
    pub crf: Option<i32>,
    pub bitrate_bps: Option<i64>,
    pub maxrate_bps: Option<i64>,
    pub bufsize_bps: Option<i64>,
    pub pixel_format: Option<String>,
    pub scale: Option<VideoScalePlan>,
    pub tone_map: Option<VideoToneMapPlan>,
    pub frame_rate: VideoFrameRatePlan,
    pub gop_frames: Option<i32>,
    pub segment_seconds: String,
    pub keyframe_expression: String,
    pub hls_delivery: Delivery,
    pub burn_in: Option<SubtitleBurnInPlan>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveAudioStrategy {
    SharedRendition,
    PerRung,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveRungPlan {
    pub id: String,
    pub label: String,
    pub bandwidth_bps: i64,
    pub width: i32,
    pub height: i32,
    pub video: VideoOutputPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveLadderPlan {
    pub rungs: Vec<AdaptiveRungPlan>,
    pub starting_rung_id: String,
    pub active_rung_id: String,
    pub audio_strategy: AdaptiveAudioStrategy,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybackPlan {
    pub plan_version: u32,
    pub mode: PlaybackMode,
    pub delivery: Delivery,
    pub media_file_id: String,
    pub selected_video_track: Option<i32>,
    pub video_action: StreamAction,
    pub audio_action: StreamAction,
    pub subtitle_action: StreamAction,
    pub seek_behavior: SeekBehavior,
    pub adaptive: bool,
    pub selected_audio_track: Option<i32>,
    pub selected_subtitle_track: Option<i32>,
    pub hdr_action: HdrAction,
    pub hardware_acceleration: HardwareAccelerationPlan,
    #[serde(default)]
    pub audio_output: Option<AudioOutputPlan>,
    #[serde(default)]
    pub video_output: Option<VideoOutputPlan>,
    #[serde(default)]
    pub adaptive_ladder: Option<AdaptiveLadderPlan>,
    #[serde(default)]
    pub video_transcode_reason: Option<String>,
    pub compatibility_report: CompatibilityReport,
    pub reasons: Vec<String>,
    pub warnings: Vec<String>,
    pub expected_outputs: Vec<ExpectedOutput>,
    pub playable: bool,
}

impl PlaybackPlan {
    pub fn decision_reason(&self) -> String {
        self.reasons
            .first()
            .cloned()
            .unwrap_or_else(|| "playback_plan_created".to_string())
    }

    pub fn server_seek_required(&self) -> bool {
        self.seek_behavior.server_seek_required()
    }
}
