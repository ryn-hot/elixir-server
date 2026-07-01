use serde::{Deserialize, Serialize};

use super::hardware::HardwareCapabilities;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientKind {
    NativeMpv,
    Web,
    Tv,
    Mobile,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityMode {
    Original,
    Fixed,
    Automatic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubtitleBurnPolicy {
    Never,
    Automatic,
    ImageOnly,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubtitleRendering {
    Native,
    HlsWebvtt,
    Sidecar,
    BurnInOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssComplexitySupport {
    Native,
    SimpleWebvtt,
    BurnIn,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageSubtitleSupport {
    Native,
    BurnIn,
    NativeOrBurnIn,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForcedSubtitlePolicy {
    Disabled,
    MatchingAudio,
    Any,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefaultSubtitlePolicy {
    Disabled,
    MediaDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkClass {
    Lan,
    Wan,
    Unknown,
}

impl NetworkClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lan => "lan",
            Self::Wan => "wan",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPlaybackProfile {
    pub profile_version: u32,
    pub client_kind: ClientKind,
    pub direct_play_preferred: bool,
    pub max_resolution: Option<String>,
    pub max_bitrate_bps: Option<i64>,
    pub supported_containers: Vec<String>,
    pub supported_video_codecs: Vec<String>,
    pub supported_audio_codecs: Vec<String>,
    pub supported_subtitle_codecs: Vec<String>,
    pub supported_hls_segment_types: Vec<String>,
    pub max_audio_channels: Option<i32>,
    pub supports_hdr: bool,
    pub supports_hdr10_plus: bool,
    pub supports_dolby_vision: bool,
    pub supports_server_side_hls_seek: bool,
    pub supports_auth_headers_for_media: bool,
    pub subtitle_burn_policy: SubtitleBurnPolicy,
    #[serde(default = "default_subtitle_rendering")]
    pub subtitle_rendering: SubtitleRendering,
    #[serde(default = "default_ass_complexity_support")]
    pub ass_complexity_support: AssComplexitySupport,
    #[serde(default = "default_image_subtitle_support")]
    pub image_subtitle_support: ImageSubtitleSupport,
    #[serde(default = "default_forced_subtitle_policy")]
    pub forced_subtitle_policy: ForcedSubtitlePolicy,
    #[serde(default = "default_default_subtitle_policy")]
    pub default_subtitle_policy: DefaultSubtitlePolicy,
    pub quality_mode: QualityMode,
    pub app_version: Option<String>,
}

fn default_subtitle_rendering() -> SubtitleRendering {
    SubtitleRendering::HlsWebvtt
}

fn default_ass_complexity_support() -> AssComplexitySupport {
    AssComplexitySupport::BurnIn
}

fn default_image_subtitle_support() -> ImageSubtitleSupport {
    ImageSubtitleSupport::BurnIn
}

fn default_forced_subtitle_policy() -> ForcedSubtitlePolicy {
    ForcedSubtitlePolicy::MatchingAudio
}

fn default_default_subtitle_policy() -> DefaultSubtitlePolicy {
    DefaultSubtitlePolicy::MediaDefault
}

impl ClientPlaybackProfile {
    pub fn native_mpv() -> Self {
        Self {
            profile_version: 1,
            client_kind: ClientKind::NativeMpv,
            direct_play_preferred: true,
            max_resolution: None,
            max_bitrate_bps: None,
            supported_containers: vec![
                "mkv".to_string(),
                "mp4".to_string(),
                "mov".to_string(),
                "avi".to_string(),
                "mpegts".to_string(),
            ],
            supported_video_codecs: vec![
                "h264".to_string(),
                "hevc".to_string(),
                "mpeg2video".to_string(),
                "vp9".to_string(),
                "av1".to_string(),
            ],
            supported_audio_codecs: vec![
                "aac".to_string(),
                "ac3".to_string(),
                "eac3".to_string(),
                "dts".to_string(),
                "truehd".to_string(),
                "opus".to_string(),
                "mp3".to_string(),
                "flac".to_string(),
            ],
            supported_subtitle_codecs: vec![
                "srt".to_string(),
                "webvtt".to_string(),
                "ass".to_string(),
                "ssa".to_string(),
                "mov_text".to_string(),
                "pgs".to_string(),
                "dvd_subtitle".to_string(),
            ],
            supported_hls_segment_types: vec!["fmp4".to_string(), "mpegts".to_string()],
            max_audio_channels: None,
            supports_hdr: true,
            supports_hdr10_plus: true,
            supports_dolby_vision: false,
            supports_server_side_hls_seek: true,
            supports_auth_headers_for_media: true,
            subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
            subtitle_rendering: SubtitleRendering::Native,
            ass_complexity_support: AssComplexitySupport::Native,
            image_subtitle_support: ImageSubtitleSupport::NativeOrBurnIn,
            forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
            default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
            quality_mode: QualityMode::Original,
            app_version: None,
        }
    }

    pub fn browser_like() -> Self {
        Self {
            profile_version: 1,
            client_kind: ClientKind::Web,
            direct_play_preferred: false,
            max_resolution: Some("1080p".to_string()),
            max_bitrate_bps: Some(8_000_000),
            supported_containers: vec!["mp4".to_string()],
            supported_video_codecs: vec!["h264".to_string()],
            supported_audio_codecs: vec!["aac".to_string(), "ac3".to_string()],
            supported_subtitle_codecs: vec!["webvtt".to_string()],
            supported_hls_segment_types: vec!["fmp4".to_string(), "mpegts".to_string()],
            max_audio_channels: Some(2),
            supports_hdr: false,
            supports_hdr10_plus: false,
            supports_dolby_vision: false,
            supports_server_side_hls_seek: true,
            supports_auth_headers_for_media: true,
            subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
            subtitle_rendering: SubtitleRendering::HlsWebvtt,
            ass_complexity_support: AssComplexitySupport::BurnIn,
            image_subtitle_support: ImageSubtitleSupport::BurnIn,
            forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
            default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
            quality_mode: QualityMode::Fixed,
            app_version: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerPlaybackPolicy {
    pub allow_direct_play: bool,
    pub allow_direct_stream: bool,
    pub allow_audio_transcode: bool,
    pub allow_video_transcode: bool,
    pub allow_adaptive_transcode: bool,
    pub max_remote_bitrate_bps: Option<i64>,
    pub server_upload_cap_bps: Option<i64>,
    pub max_resolution: Option<String>,
    pub max_simultaneous_video_transcodes: Option<u32>,
    pub force_direct_play_for_native_mpv: bool,
    pub video_encoder_preset: String,
    pub video_encoder_profile: String,
    pub video_encoder_level: String,
    pub video_encoder_crf: i32,
    pub video_encoder_bufsize_multiplier: i32,
    pub hardware_acceleration: String,
    pub allow_hardware_decode: bool,
    pub allow_hardware_encode: bool,
    pub hardware_fallback: String,
    pub force_sdr_output: bool,
    pub hardware_capabilities: HardwareCapabilities,
}

impl Default for ServerPlaybackPolicy {
    fn default() -> Self {
        Self {
            allow_direct_play: true,
            allow_direct_stream: false,
            allow_audio_transcode: false,
            allow_video_transcode: true,
            allow_adaptive_transcode: false,
            max_remote_bitrate_bps: None,
            server_upload_cap_bps: None,
            max_resolution: None,
            max_simultaneous_video_transcodes: None,
            force_direct_play_for_native_mpv: false,
            video_encoder_preset: "veryfast".to_string(),
            video_encoder_profile: "high".to_string(),
            video_encoder_level: "4.1".to_string(),
            video_encoder_crf: 20,
            video_encoder_bufsize_multiplier: 2,
            hardware_acceleration: "auto".to_string(),
            allow_hardware_decode: true,
            allow_hardware_encode: true,
            hardware_fallback: "software".to_string(),
            force_sdr_output: false,
            hardware_capabilities: HardwareCapabilities::software_only(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPlaybackPolicy {
    pub network_class: NetworkClass,
    pub max_bitrate_bps: Option<i64>,
    pub max_remote_bitrate_bps: Option<i64>,
    pub max_resolution: Option<String>,
    pub server_upload_cap_bps: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectivePlaybackPolicy {
    pub allow_direct_play: bool,
    pub allow_direct_stream: bool,
    pub allow_audio_transcode: bool,
    pub allow_video_transcode: bool,
    pub allow_adaptive_transcode: bool,
    pub network_class: NetworkClass,
    pub max_bitrate_bps: Option<i64>,
    pub max_remote_bitrate_bps: Option<i64>,
    pub max_resolution: Option<String>,
    pub server_upload_cap_bps: Option<i64>,
    pub max_simultaneous_video_transcodes: Option<u32>,
    pub active_video_transcodes: u32,
    pub force_direct_play_for_native_mpv: bool,
    pub video_encoder_preset: String,
    pub video_encoder_profile: String,
    pub video_encoder_level: String,
    pub video_encoder_crf: i32,
    pub video_encoder_bufsize_multiplier: i32,
    pub hardware_acceleration: String,
    pub allow_hardware_decode: bool,
    pub allow_hardware_encode: bool,
    pub hardware_fallback: String,
    pub force_sdr_output: bool,
    pub hardware_capabilities: HardwareCapabilities,
}

impl Default for EffectivePlaybackPolicy {
    fn default() -> Self {
        Self {
            allow_direct_play: true,
            allow_direct_stream: false,
            allow_audio_transcode: false,
            allow_video_transcode: true,
            allow_adaptive_transcode: false,
            network_class: NetworkClass::Unknown,
            max_bitrate_bps: None,
            max_remote_bitrate_bps: None,
            max_resolution: None,
            server_upload_cap_bps: None,
            max_simultaneous_video_transcodes: None,
            active_video_transcodes: 0,
            force_direct_play_for_native_mpv: false,
            video_encoder_preset: "veryfast".to_string(),
            video_encoder_profile: "high".to_string(),
            video_encoder_level: "4.1".to_string(),
            video_encoder_crf: 20,
            video_encoder_bufsize_multiplier: 2,
            hardware_acceleration: "auto".to_string(),
            allow_hardware_decode: true,
            allow_hardware_encode: true,
            hardware_fallback: "software".to_string(),
            force_sdr_output: false,
            hardware_capabilities: HardwareCapabilities::software_only(),
        }
    }
}

pub fn derive_effective_playback_policy(
    client: &ClientPlaybackProfile,
    server: &ServerPlaybackPolicy,
    network: &NetworkPlaybackPolicy,
) -> EffectivePlaybackPolicy {
    let hls_capable = !client.supported_hls_segment_types.is_empty();
    let native_original_on_lan = client.client_kind == ClientKind::NativeMpv
        && client.direct_play_preferred
        && client.quality_mode == QualityMode::Original
        && network.network_class == NetworkClass::Lan;
    let remote_like = matches!(
        network.network_class,
        NetworkClass::Wan | NetworkClass::Unknown
    );

    let mut bitrate_caps = Vec::new();
    if !native_original_on_lan {
        push_positive_i64(&mut bitrate_caps, client.max_bitrate_bps);
        push_positive_i64(&mut bitrate_caps, network.max_bitrate_bps);
    }
    if remote_like {
        push_positive_i64(&mut bitrate_caps, server.max_remote_bitrate_bps);
        push_positive_i64(&mut bitrate_caps, network.max_remote_bitrate_bps);
        push_positive_i64(&mut bitrate_caps, server.server_upload_cap_bps);
        push_positive_i64(&mut bitrate_caps, network.server_upload_cap_bps);
    }

    let max_resolution = if native_original_on_lan {
        None
    } else {
        min_resolution_cap(
            client
                .max_resolution
                .as_deref()
                .filter(|value| !is_unlimited_resolution(value)),
            min_resolution_cap(
                server
                    .max_resolution
                    .as_deref()
                    .filter(|value| !is_unlimited_resolution(value)),
                network
                    .max_resolution
                    .as_deref()
                    .filter(|value| !is_unlimited_resolution(value)),
            )
            .as_deref(),
        )
    };

    EffectivePlaybackPolicy {
        allow_direct_play: server.allow_direct_play,
        allow_direct_stream: server.allow_direct_stream && hls_capable,
        allow_audio_transcode: server.allow_audio_transcode && hls_capable,
        allow_video_transcode: server.allow_video_transcode && hls_capable,
        allow_adaptive_transcode: server.allow_adaptive_transcode
            && hls_capable
            && client.quality_mode == QualityMode::Automatic,
        network_class: network.network_class,
        max_bitrate_bps: bitrate_caps.into_iter().min(),
        max_remote_bitrate_bps: min_positive_i64(
            server.max_remote_bitrate_bps,
            network.max_remote_bitrate_bps,
        ),
        max_resolution,
        server_upload_cap_bps: min_positive_i64(
            server.server_upload_cap_bps,
            network.server_upload_cap_bps,
        ),
        max_simultaneous_video_transcodes: server.max_simultaneous_video_transcodes,
        active_video_transcodes: 0,
        force_direct_play_for_native_mpv: server.force_direct_play_for_native_mpv,
        video_encoder_preset: server.video_encoder_preset.clone(),
        video_encoder_profile: server.video_encoder_profile.clone(),
        video_encoder_level: server.video_encoder_level.clone(),
        video_encoder_crf: server.video_encoder_crf,
        video_encoder_bufsize_multiplier: server.video_encoder_bufsize_multiplier.max(1),
        hardware_acceleration: server.hardware_acceleration.clone(),
        allow_hardware_decode: server.allow_hardware_decode,
        allow_hardware_encode: server.allow_hardware_encode,
        hardware_fallback: server.hardware_fallback.clone(),
        force_sdr_output: server.force_sdr_output,
        hardware_capabilities: server.hardware_capabilities.clone(),
    }
}

fn push_positive_i64(values: &mut Vec<i64>, value: Option<i64>) {
    if let Some(value) = value.filter(|value| *value > 0) {
        values.push(value);
    }
}

fn min_positive_i64(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a.filter(|value| *value > 0), b.filter(|value| *value > 0)) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn min_resolution_cap(a: Option<&str>, b: Option<&str>) -> Option<String> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if resolution_rank(a) <= resolution_rank(b) {
            a.to_string()
        } else {
            b.to_string()
        }),
        (Some(a), None) => Some(a.to_string()),
        (None, Some(b)) => Some(b.to_string()),
        (None, None) => None,
    }
}

fn resolution_rank(value: &str) -> i32 {
    match value.trim().to_ascii_lowercase().as_str() {
        "480p" => 0,
        "720p" => 1,
        "1080p" => 2,
        "1440p" => 3,
        "4k" | "2160p" => 4,
        "8k" | "4320p" => 5,
        _ if is_unlimited_resolution(value) => i32::MAX,
        _ => i32::MAX,
    }
}

fn is_unlimited_resolution(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "" | "any" | "none" | "unlimited" | "original" | "source" | "direct" | "native"
    )
}
