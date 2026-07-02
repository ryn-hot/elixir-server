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
    DirectDolbyVision,
    DirectHdr10Fallback,
    ToneMapToSdr,
    Unsupported,
    UnknownFailClosed,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decode_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encode_status: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl Default for HardwareAccelerationPlan {
    fn default() -> Self {
        Self {
            enabled: false,
            api: None,
            decoder: None,
            encoder: None,
            fallback: Some("software".to_string()),
            readiness_id: None,
            decode_status: None,
            encode_status: None,
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackSupportDecision {
    Supported,
    Unsupported,
    MixedFallback,
    SoftwareOnly,
    Unknown,
}

impl PlaybackSupportDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Unsupported => "unsupported",
            Self::MixedFallback => "mixed_fallback",
            Self::SoftwareOnly => "software_only",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackPerformanceDecision {
    RealtimeSafe,
    RealtimeMarginal,
    NotRealtime,
    Unknown,
}

impl PlaybackPerformanceDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RealtimeSafe => "realtime_safe",
            Self::RealtimeMarginal => "realtime_marginal",
            Self::NotRealtime => "not_realtime",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackPerformanceConfidence {
    Certified,
    LocalBenchmark,
    LiveObserved,
    StaticInferred,
    Unknown,
}

impl PlaybackPerformanceConfidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Certified => "certified",
            Self::LocalBenchmark => "local_benchmark",
            Self::LiveObserved => "live_observed",
            Self::StaticInferred => "static_inferred",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackFeasibilityAction {
    AllowDirect,
    AllowTranscode,
    AllowWithWarning,
    DowngradeQuality,
    SoftwareFallback,
    Reject,
}

impl PlaybackFeasibilityAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AllowDirect => "allow_direct",
            Self::AllowTranscode => "allow_transcode",
            Self::AllowWithWarning => "allow_with_warning",
            Self::DowngradeQuality => "downgrade_quality",
            Self::SoftwareFallback => "software_fallback",
            Self::Reject => "reject",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybackWorkloadClass {
    pub schema_version: u32,
    pub class_id: String,
    pub source_container: Option<String>,
    pub source_video_codec: Option<String>,
    pub source_video_profile: Option<String>,
    pub source_bit_depth: Option<u8>,
    pub source_pixel_format: Option<String>,
    pub source_width: Option<i32>,
    pub source_height: Option<i32>,
    pub source_frame_rate: Option<String>,
    pub source_bitrate_bps: Option<i64>,
    pub hdr_action: HdrAction,
    pub subtitle_action: StreamAction,
    pub audio_action: StreamAction,
    pub output_codec: Option<String>,
    pub output_width: Option<i32>,
    pub output_height: Option<i32>,
    pub output_pixel_format: Option<String>,
    pub delivery: Delivery,
    pub pipeline_signature: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pipeline_stages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cost_labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybackPerformanceEnvelope {
    pub id: String,
    pub host_fingerprint: String,
    pub os_family: String,
    pub os_version: Option<String>,
    pub gpu_vendor: Option<String>,
    pub gpu_model: Option<String>,
    pub gpu_driver_version: Option<String>,
    pub hardware_api: Option<String>,
    pub ffmpeg_path: Option<String>,
    pub ffmpeg_version: Option<String>,
    pub ffmpeg_sha256: Option<String>,
    pub elixir_version: Option<String>,
    pub workload_class_id: String,
    pub pipeline_signature: String,
    pub support_decision: PlaybackSupportDecision,
    pub performance_decision: PlaybackPerformanceDecision,
    pub confidence: PlaybackPerformanceConfidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_realtime_factor_millis: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_realtime_factor_millis: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup_latency_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_segment_latency_ms: Option<i64>,
    pub failure_count: i64,
    pub sample_count: i64,
    pub invalidation_fingerprint: String,
    pub last_observed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remediation_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybackFeasibilityDecision {
    pub action: PlaybackFeasibilityAction,
    pub reason: String,
    pub support_decision: PlaybackSupportDecision,
    pub performance_decision: PlaybackPerformanceDecision,
    pub confidence: PlaybackPerformanceConfidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_envelope_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_hardware_api: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_envelope_p50_realtime_factor_millis: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_envelope_p95_realtime_factor_millis: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_envelope_startup_latency_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_envelope_first_segment_latency_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_envelope_failure_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_envelope_sample_count: Option<i64>,
    pub realtime_required_millis: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remediation_codes: Vec<String>,
    pub background_probe_queued: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_stream_index: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_path: Option<String>,
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
    #[serde(default)]
    pub average_bandwidth_bps: i64,
    pub width: i32,
    pub height: i32,
    #[serde(default)]
    pub resolution: String,
    #[serde(default)]
    pub codecs: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_rate: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_class: Option<PlaybackWorkloadClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feasibility: Option<PlaybackFeasibilityDecision>,
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
