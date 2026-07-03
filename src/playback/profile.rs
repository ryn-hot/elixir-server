use serde::{Deserialize, Serialize};

use super::{hardware::HardwareCapabilities, plan::PlaybackPerformanceEnvelope};

pub const PLAYBACK_PROFILE_CATALOG_VERSION: u32 = 5;
pub const PROFILE_NATIVE_MPV_DESKTOP: &str = "native_mpv_desktop";
pub const PROFILE_WEB_CHROMIUM: &str = "web_chromium";
pub const PROFILE_WEB_SAFARI: &str = "web_safari";
pub const PROFILE_IOS_IPADOS: &str = "ios_ipados";
pub const PROFILE_ANDROID_MOBILE: &str = "android_mobile";
pub const PROFILE_ANDROID_TV: &str = "android_tv";
pub const PROFILE_APPLE_TV: &str = "apple_tv";
pub const PROFILE_ROKU_LIKE: &str = "roku_like";
pub const PROFILE_SMART_TV_CONSERVATIVE: &str = "smart_tv_conservative";
pub const PROFILE_GAME_CONSOLE_CONSERVATIVE: &str = "game_console_conservative";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientKind {
    NativeMpv,
    Web,
    Tv,
    Mobile,
    GameConsole,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityMode {
    Original,
    Fixed,
    Automatic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbrSupportType {
    NativeHls,
    HlsJs,
    Mpv,
    None,
}

impl AbrSupportType {
    pub fn supports_adaptive_hls(self) -> bool {
        !matches!(self, Self::None)
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownPerformancePolicy {
    Deny,
    AllowBestEffort,
}

impl UnknownPerformancePolicy {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "deny" | "fail" | "fail_closed" | "reject" => Self::Deny,
            "allow_best_effort" | "best_effort" | "allow" => Self::AllowBestEffort,
            _ => Self::Deny,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::AllowBestEffort => "allow_best_effort",
        }
    }
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
    #[serde(default = "default_profile_id")]
    pub profile_id: String,
    #[serde(default)]
    pub profile_name: Option<String>,
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
    #[serde(default)]
    pub supports_native_text_subtitles: bool,
    #[serde(default = "default_strict_h264_profile_limits")]
    pub strict_h264_profile_limits: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_bitrate_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_min_bitrate_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_max_bitrate_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_min_resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_max_resolution: Option<String>,
    #[serde(default = "default_abr_support_type")]
    pub abr_support_type: AbrSupportType,
    pub app_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlaybackProfileCatalogEntry {
    pub id: String,
    pub name: String,
    pub version: u32,
    pub client_kind: ClientKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlaybackProfileNegotiation {
    pub requested_profile_id: Option<String>,
    pub requested_profile_version: Option<u32>,
    pub requested_app_version: Option<String>,
    pub effective_profile_id: String,
    pub effective_profile_version: u32,
    pub latest_profile_version: u32,
    pub known_profile: bool,
    pub compatibility: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NegotiatedPlaybackProfile {
    pub profile: ClientPlaybackProfile,
    pub negotiation: PlaybackProfileNegotiation,
}

fn default_profile_id() -> String {
    PROFILE_WEB_CHROMIUM.to_string()
}

fn default_strict_h264_profile_limits() -> bool {
    true
}

fn default_abr_support_type() -> AbrSupportType {
    AbrSupportType::None
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
        native_mpv_desktop_profile()
    }

    pub fn browser_like() -> Self {
        web_chromium_profile()
    }
}

pub fn playback_profile_catalog() -> Vec<PlaybackProfileCatalogEntry> {
    [
        native_mpv_desktop_profile(),
        web_chromium_profile(),
        web_safari_profile(),
        ios_ipados_profile(),
        android_mobile_profile(),
        android_tv_profile(),
        apple_tv_profile(),
        roku_like_profile(),
        smart_tv_conservative_profile(),
        game_console_conservative_profile(),
    ]
    .into_iter()
    .map(|profile| PlaybackProfileCatalogEntry {
        id: profile.profile_id,
        name: profile
            .profile_name
            .unwrap_or_else(|| "Playback profile".to_string()),
        version: profile.profile_version,
        client_kind: profile.client_kind,
    })
    .collect()
}

pub fn negotiate_playback_profile(
    requested_profile_id: Option<&str>,
    requested_profile_version: Option<u32>,
    requested_app_version: Option<&str>,
    legacy_client_kind: Option<&str>,
) -> NegotiatedPlaybackProfile {
    let normalized_profile_id = requested_profile_id.and_then(normalize_profile_id);
    let legacy_profile_id = requested_profile_id
        .is_none()
        .then(|| legacy_profile_id_from_client_kind(legacy_client_kind))
        .flatten();
    let selected_profile_id = normalized_profile_id.or(legacy_profile_id);
    let known_profile = selected_profile_id.is_some();
    let profile_id = selected_profile_id.unwrap_or(PROFILE_WEB_CHROMIUM);
    let mut profile = profile_for_id(profile_id).unwrap_or_else(web_chromium_profile);
    let latest_profile_version = profile.profile_version;
    let effective_version = requested_profile_version
        .filter(|version| *version > 0)
        .unwrap_or(latest_profile_version)
        .min(latest_profile_version);
    profile.profile_version = effective_version;
    profile.app_version = requested_app_version.map(str::to_string);

    let mut compatibility = Vec::new();
    if !known_profile {
        compatibility.push("unknown_profile_conservative_browser_defaults".to_string());
    }
    if requested_profile_version.is_some_and(|version| version > latest_profile_version) {
        compatibility.push("requested_profile_version_capped_to_server_catalog".to_string());
    }
    if effective_version < latest_profile_version {
        compatibility.push(format!(
            "profile_version_{effective_version}_compatibility_snapshot"
        ));
        apply_profile_version_compatibility(&mut profile, effective_version);
    }

    NegotiatedPlaybackProfile {
        negotiation: PlaybackProfileNegotiation {
            requested_profile_id: requested_profile_id.map(str::to_string),
            requested_profile_version,
            requested_app_version: requested_app_version.map(str::to_string),
            effective_profile_id: profile.profile_id.clone(),
            effective_profile_version: profile.profile_version,
            latest_profile_version,
            known_profile,
            compatibility,
        },
        profile,
    }
}

pub fn profile_for_id(profile_id: &str) -> Option<ClientPlaybackProfile> {
    match normalize_profile_id(profile_id)? {
        PROFILE_NATIVE_MPV_DESKTOP => Some(native_mpv_desktop_profile()),
        PROFILE_WEB_CHROMIUM => Some(web_chromium_profile()),
        PROFILE_WEB_SAFARI => Some(web_safari_profile()),
        PROFILE_IOS_IPADOS => Some(ios_ipados_profile()),
        PROFILE_ANDROID_MOBILE => Some(android_mobile_profile()),
        PROFILE_ANDROID_TV => Some(android_tv_profile()),
        PROFILE_APPLE_TV => Some(apple_tv_profile()),
        PROFILE_ROKU_LIKE => Some(roku_like_profile()),
        PROFILE_SMART_TV_CONSERVATIVE => Some(smart_tv_conservative_profile()),
        PROFILE_GAME_CONSOLE_CONSERVATIVE => Some(game_console_conservative_profile()),
        _ => None,
    }
}

fn normalize_profile_id(raw: &str) -> Option<&'static str> {
    let value = raw.trim().to_ascii_lowercase();
    let normalized = value.replace(['-', ' '], "_");
    match normalized.as_str() {
        "native_mpv_desktop" | "native_mpv" | "mpv" | "libmpv" => Some(PROFILE_NATIVE_MPV_DESKTOP),
        "web_chromium" | "chromium" | "chrome" | "edge" | "browser_like" | "web" => {
            Some(PROFILE_WEB_CHROMIUM)
        }
        "web_safari" | "safari" => Some(PROFILE_WEB_SAFARI),
        "ios_ipados" | "ios" | "ipados" | "iphone" | "ipad" => Some(PROFILE_IOS_IPADOS),
        "android_mobile" | "android" => Some(PROFILE_ANDROID_MOBILE),
        "android_tv" | "google_tv" => Some(PROFILE_ANDROID_TV),
        "apple_tv" | "appletv" | "tvos" => Some(PROFILE_APPLE_TV),
        "roku_like" | "roku" => Some(PROFILE_ROKU_LIKE),
        "smart_tv_conservative" | "smart_tv" | "tizen" | "webos" | "vidaa" => {
            Some(PROFILE_SMART_TV_CONSERVATIVE)
        }
        "game_console_conservative" | "game_console" | "console" | "xbox" | "playstation" => {
            Some(PROFILE_GAME_CONSOLE_CONSERVATIVE)
        }
        _ => None,
    }
}

fn legacy_profile_id_from_client_kind(raw: Option<&str>) -> Option<&'static str> {
    let normalized = raw?.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "native_mpv" | "mpv" | "libmpv" | "desktop_native" => Some(PROFILE_NATIVE_MPV_DESKTOP),
        "web" | "browser" | "browser_like" => Some(PROFILE_WEB_CHROMIUM),
        "mobile" => Some(PROFILE_ANDROID_MOBILE),
        "android_tv" => Some(PROFILE_ANDROID_TV),
        "apple_tv" => Some(PROFILE_APPLE_TV),
        "tv" | "smart_tv" => Some(PROFILE_SMART_TV_CONSERVATIVE),
        "game_console" | "console" => Some(PROFILE_GAME_CONSOLE_CONSERVATIVE),
        _ => None,
    }
}

fn apply_profile_version_compatibility(profile: &mut ClientPlaybackProfile, version: u32) {
    if version < 5 {
        profile.supports_native_text_subtitles = matches!(
            profile.subtitle_rendering,
            SubtitleRendering::Native | SubtitleRendering::Sidecar
        );
        profile.strict_h264_profile_limits = profile.client_kind != ClientKind::NativeMpv;
    }
}

fn native_mpv_desktop_profile() -> ClientPlaybackProfile {
    ClientPlaybackProfile {
        profile_id: PROFILE_NATIVE_MPV_DESKTOP.to_string(),
        profile_name: Some("Native mpv desktop".to_string()),
        profile_version: PLAYBACK_PROFILE_CATALOG_VERSION,
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
        supports_native_text_subtitles: true,
        strict_h264_profile_limits: false,
        subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
        subtitle_rendering: SubtitleRendering::Native,
        ass_complexity_support: AssComplexitySupport::Native,
        image_subtitle_support: ImageSubtitleSupport::NativeOrBurnIn,
        forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
        default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
        quality_mode: QualityMode::Original,
        fixed_bitrate_bps: None,
        fixed_resolution: None,
        automatic_min_bitrate_bps: None,
        automatic_max_bitrate_bps: None,
        automatic_min_resolution: None,
        automatic_max_resolution: None,
        abr_support_type: AbrSupportType::Mpv,
        app_version: None,
    }
}

fn web_chromium_profile() -> ClientPlaybackProfile {
    ClientPlaybackProfile {
        profile_id: PROFILE_WEB_CHROMIUM.to_string(),
        profile_name: Some("Web Chromium".to_string()),
        profile_version: PLAYBACK_PROFILE_CATALOG_VERSION,
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
        supports_native_text_subtitles: false,
        strict_h264_profile_limits: true,
        subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
        subtitle_rendering: SubtitleRendering::HlsWebvtt,
        ass_complexity_support: AssComplexitySupport::BurnIn,
        image_subtitle_support: ImageSubtitleSupport::BurnIn,
        forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
        default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
        quality_mode: QualityMode::Fixed,
        fixed_bitrate_bps: Some(8_000_000),
        fixed_resolution: Some("1080p".to_string()),
        automatic_min_bitrate_bps: Some(800_000),
        automatic_max_bitrate_bps: Some(8_000_000),
        automatic_min_resolution: Some("360p".to_string()),
        automatic_max_resolution: Some("1080p".to_string()),
        abr_support_type: AbrSupportType::HlsJs,
        app_version: None,
    }
}

fn web_safari_profile() -> ClientPlaybackProfile {
    ClientPlaybackProfile {
        profile_id: PROFILE_WEB_SAFARI.to_string(),
        profile_name: Some("Web Safari".to_string()),
        profile_version: PLAYBACK_PROFILE_CATALOG_VERSION,
        client_kind: ClientKind::Web,
        direct_play_preferred: false,
        max_resolution: Some("1080p".to_string()),
        max_bitrate_bps: Some(10_000_000),
        supported_containers: vec!["mp4".to_string()],
        supported_video_codecs: vec!["h264".to_string(), "hevc".to_string()],
        supported_audio_codecs: vec!["aac".to_string(), "ac3".to_string(), "eac3".to_string()],
        supported_subtitle_codecs: vec!["webvtt".to_string()],
        supported_hls_segment_types: vec!["fmp4".to_string(), "mpegts".to_string()],
        max_audio_channels: Some(6),
        supports_hdr: true,
        supports_hdr10_plus: false,
        supports_dolby_vision: false,
        supports_server_side_hls_seek: true,
        supports_auth_headers_for_media: false,
        supports_native_text_subtitles: false,
        strict_h264_profile_limits: true,
        subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
        subtitle_rendering: SubtitleRendering::HlsWebvtt,
        ass_complexity_support: AssComplexitySupport::BurnIn,
        image_subtitle_support: ImageSubtitleSupport::BurnIn,
        forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
        default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
        quality_mode: QualityMode::Fixed,
        fixed_bitrate_bps: Some(10_000_000),
        fixed_resolution: Some("1080p".to_string()),
        automatic_min_bitrate_bps: Some(800_000),
        automatic_max_bitrate_bps: Some(10_000_000),
        automatic_min_resolution: Some("360p".to_string()),
        automatic_max_resolution: Some("1080p".to_string()),
        abr_support_type: AbrSupportType::NativeHls,
        app_version: None,
    }
}

fn ios_ipados_profile() -> ClientPlaybackProfile {
    let mut profile = web_safari_profile();
    profile.profile_id = PROFILE_IOS_IPADOS.to_string();
    profile.profile_name = Some("iOS/iPadOS".to_string());
    profile.client_kind = ClientKind::Mobile;
    profile.max_resolution = Some("4k".to_string());
    profile.max_bitrate_bps = Some(20_000_000);
    profile.fixed_bitrate_bps = Some(20_000_000);
    profile.fixed_resolution = Some("4k".to_string());
    profile.automatic_max_bitrate_bps = Some(20_000_000);
    profile.automatic_max_resolution = Some("4k".to_string());
    profile.supports_auth_headers_for_media = false;
    profile
}

fn android_mobile_profile() -> ClientPlaybackProfile {
    ClientPlaybackProfile {
        profile_id: PROFILE_ANDROID_MOBILE.to_string(),
        profile_name: Some("Android mobile".to_string()),
        profile_version: PLAYBACK_PROFILE_CATALOG_VERSION,
        client_kind: ClientKind::Mobile,
        direct_play_preferred: false,
        max_resolution: Some("1080p".to_string()),
        max_bitrate_bps: Some(12_000_000),
        supported_containers: vec!["mp4".to_string()],
        supported_video_codecs: vec!["h264".to_string(), "hevc".to_string()],
        supported_audio_codecs: vec![
            "aac".to_string(),
            "ac3".to_string(),
            "eac3".to_string(),
            "opus".to_string(),
        ],
        supported_subtitle_codecs: vec!["webvtt".to_string()],
        supported_hls_segment_types: vec!["fmp4".to_string(), "mpegts".to_string()],
        max_audio_channels: Some(6),
        supports_hdr: true,
        supports_hdr10_plus: false,
        supports_dolby_vision: false,
        supports_server_side_hls_seek: true,
        supports_auth_headers_for_media: true,
        supports_native_text_subtitles: false,
        strict_h264_profile_limits: true,
        subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
        subtitle_rendering: SubtitleRendering::HlsWebvtt,
        ass_complexity_support: AssComplexitySupport::BurnIn,
        image_subtitle_support: ImageSubtitleSupport::BurnIn,
        forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
        default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
        quality_mode: QualityMode::Fixed,
        fixed_bitrate_bps: Some(12_000_000),
        fixed_resolution: Some("1080p".to_string()),
        automatic_min_bitrate_bps: Some(800_000),
        automatic_max_bitrate_bps: Some(12_000_000),
        automatic_min_resolution: Some("360p".to_string()),
        automatic_max_resolution: Some("1080p".to_string()),
        abr_support_type: AbrSupportType::NativeHls,
        app_version: None,
    }
}

fn android_tv_profile() -> ClientPlaybackProfile {
    let mut profile = android_mobile_profile();
    profile.profile_id = PROFILE_ANDROID_TV.to_string();
    profile.profile_name = Some("Android TV".to_string());
    profile.client_kind = ClientKind::Tv;
    profile.max_resolution = Some("4k".to_string());
    profile.max_bitrate_bps = Some(30_000_000);
    profile.fixed_bitrate_bps = Some(30_000_000);
    profile.fixed_resolution = Some("4k".to_string());
    profile.automatic_max_bitrate_bps = Some(30_000_000);
    profile.automatic_max_resolution = Some("4k".to_string());
    profile
}

fn apple_tv_profile() -> ClientPlaybackProfile {
    let mut profile = ios_ipados_profile();
    profile.profile_id = PROFILE_APPLE_TV.to_string();
    profile.profile_name = Some("Apple TV".to_string());
    profile.client_kind = ClientKind::Tv;
    profile.max_bitrate_bps = Some(35_000_000);
    profile.fixed_bitrate_bps = Some(35_000_000);
    profile.automatic_max_bitrate_bps = Some(35_000_000);
    profile.supports_auth_headers_for_media = false;
    profile
}

fn roku_like_profile() -> ClientPlaybackProfile {
    ClientPlaybackProfile {
        profile_id: PROFILE_ROKU_LIKE.to_string(),
        profile_name: Some("Roku-like".to_string()),
        profile_version: PLAYBACK_PROFILE_CATALOG_VERSION,
        client_kind: ClientKind::Tv,
        direct_play_preferred: false,
        max_resolution: Some("1080p".to_string()),
        max_bitrate_bps: Some(10_000_000),
        supported_containers: vec!["mp4".to_string()],
        supported_video_codecs: vec!["h264".to_string()],
        supported_audio_codecs: vec!["aac".to_string(), "ac3".to_string()],
        supported_subtitle_codecs: vec!["webvtt".to_string()],
        supported_hls_segment_types: vec!["mpegts".to_string()],
        max_audio_channels: Some(6),
        supports_hdr: false,
        supports_hdr10_plus: false,
        supports_dolby_vision: false,
        supports_server_side_hls_seek: true,
        supports_auth_headers_for_media: false,
        supports_native_text_subtitles: false,
        strict_h264_profile_limits: true,
        subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
        subtitle_rendering: SubtitleRendering::HlsWebvtt,
        ass_complexity_support: AssComplexitySupport::BurnIn,
        image_subtitle_support: ImageSubtitleSupport::BurnIn,
        forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
        default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
        quality_mode: QualityMode::Fixed,
        fixed_bitrate_bps: Some(10_000_000),
        fixed_resolution: Some("1080p".to_string()),
        automatic_min_bitrate_bps: Some(800_000),
        automatic_max_bitrate_bps: Some(10_000_000),
        automatic_min_resolution: Some("360p".to_string()),
        automatic_max_resolution: Some("1080p".to_string()),
        abr_support_type: AbrSupportType::NativeHls,
        app_version: None,
    }
}

fn smart_tv_conservative_profile() -> ClientPlaybackProfile {
    ClientPlaybackProfile {
        profile_id: PROFILE_SMART_TV_CONSERVATIVE.to_string(),
        profile_name: Some("Smart TV conservative".to_string()),
        profile_version: PLAYBACK_PROFILE_CATALOG_VERSION,
        client_kind: ClientKind::Tv,
        direct_play_preferred: false,
        max_resolution: Some("1080p".to_string()),
        max_bitrate_bps: Some(8_000_000),
        supported_containers: vec!["mp4".to_string()],
        supported_video_codecs: vec!["h264".to_string()],
        supported_audio_codecs: vec!["aac".to_string(), "ac3".to_string()],
        supported_subtitle_codecs: vec!["webvtt".to_string()],
        supported_hls_segment_types: vec!["mpegts".to_string()],
        max_audio_channels: Some(2),
        supports_hdr: false,
        supports_hdr10_plus: false,
        supports_dolby_vision: false,
        supports_server_side_hls_seek: false,
        supports_auth_headers_for_media: false,
        supports_native_text_subtitles: false,
        strict_h264_profile_limits: true,
        subtitle_burn_policy: SubtitleBurnPolicy::Automatic,
        subtitle_rendering: SubtitleRendering::HlsWebvtt,
        ass_complexity_support: AssComplexitySupport::BurnIn,
        image_subtitle_support: ImageSubtitleSupport::BurnIn,
        forced_subtitle_policy: ForcedSubtitlePolicy::MatchingAudio,
        default_subtitle_policy: DefaultSubtitlePolicy::MediaDefault,
        quality_mode: QualityMode::Fixed,
        fixed_bitrate_bps: Some(8_000_000),
        fixed_resolution: Some("1080p".to_string()),
        automatic_min_bitrate_bps: Some(800_000),
        automatic_max_bitrate_bps: Some(8_000_000),
        automatic_min_resolution: Some("360p".to_string()),
        automatic_max_resolution: Some("1080p".to_string()),
        abr_support_type: AbrSupportType::NativeHls,
        app_version: None,
    }
}

fn game_console_conservative_profile() -> ClientPlaybackProfile {
    let mut profile = smart_tv_conservative_profile();
    profile.profile_id = PROFILE_GAME_CONSOLE_CONSERVATIVE.to_string();
    profile.profile_name = Some("Game console conservative".to_string());
    profile.client_kind = ClientKind::GameConsole;
    profile.supports_server_side_hls_seek = true;
    profile
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
    pub unknown_performance_policy: UnknownPerformancePolicy,
    pub performance_envelopes: Vec<PlaybackPerformanceEnvelope>,
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
            unknown_performance_policy: UnknownPerformancePolicy::Deny,
            performance_envelopes: Vec::new(),
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_bitrate_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_min_bitrate_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_max_bitrate_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_min_resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_max_resolution: Option<String>,
    #[serde(default = "default_abr_support_type")]
    pub abr_support_type: AbrSupportType,
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
    pub unknown_performance_policy: UnknownPerformancePolicy,
    pub performance_envelopes: Vec<PlaybackPerformanceEnvelope>,
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
            fixed_bitrate_bps: None,
            fixed_resolution: None,
            automatic_min_bitrate_bps: None,
            automatic_max_bitrate_bps: None,
            automatic_min_resolution: None,
            automatic_max_resolution: None,
            abr_support_type: AbrSupportType::None,
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
            unknown_performance_policy: UnknownPerformancePolicy::Deny,
            performance_envelopes: Vec::new(),
        }
    }
}

pub fn derive_effective_playback_policy(
    client: &ClientPlaybackProfile,
    server: &ServerPlaybackPolicy,
    network: &NetworkPlaybackPolicy,
) -> EffectivePlaybackPolicy {
    let hls_capable = !client.supported_hls_segment_types.is_empty();
    let native_original_on_lan = client.direct_play_preferred
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
        if client.quality_mode == QualityMode::Fixed {
            push_positive_i64(&mut bitrate_caps, client.fixed_bitrate_bps);
        }
        if client.quality_mode == QualityMode::Automatic {
            push_positive_i64(&mut bitrate_caps, client.automatic_max_bitrate_bps);
        }
    }
    if remote_like {
        push_positive_i64(&mut bitrate_caps, server.max_remote_bitrate_bps);
        push_positive_i64(&mut bitrate_caps, network.max_remote_bitrate_bps);
        push_positive_i64(&mut bitrate_caps, server.server_upload_cap_bps);
        push_positive_i64(&mut bitrate_caps, network.server_upload_cap_bps);
    }

    let mut max_resolution = if native_original_on_lan {
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
    if !native_original_on_lan && client.quality_mode == QualityMode::Fixed {
        max_resolution = min_resolution_cap(
            max_resolution.as_deref(),
            client
                .fixed_resolution
                .as_deref()
                .filter(|value| !is_unlimited_resolution(value)),
        );
    }
    if !native_original_on_lan && client.quality_mode == QualityMode::Automatic {
        max_resolution = min_resolution_cap(
            max_resolution.as_deref(),
            client
                .automatic_max_resolution
                .as_deref()
                .filter(|value| !is_unlimited_resolution(value)),
        );
    }

    EffectivePlaybackPolicy {
        allow_direct_play: server.allow_direct_play,
        allow_direct_stream: server.allow_direct_stream && hls_capable,
        allow_audio_transcode: server.allow_audio_transcode && hls_capable,
        allow_video_transcode: server.allow_video_transcode && hls_capable,
        allow_adaptive_transcode: server.allow_adaptive_transcode
            && hls_capable
            && client.abr_support_type.supports_adaptive_hls()
            && client.quality_mode == QualityMode::Automatic,
        network_class: network.network_class,
        max_bitrate_bps: bitrate_caps.into_iter().min(),
        max_remote_bitrate_bps: min_positive_i64(
            server.max_remote_bitrate_bps,
            network.max_remote_bitrate_bps,
        ),
        max_resolution,
        fixed_bitrate_bps: (client.quality_mode == QualityMode::Fixed)
            .then_some(client.fixed_bitrate_bps)
            .flatten(),
        fixed_resolution: (client.quality_mode == QualityMode::Fixed)
            .then_some(
                client
                    .fixed_resolution
                    .clone()
                    .filter(|value| !is_unlimited_resolution(value)),
            )
            .flatten(),
        automatic_min_bitrate_bps: (client.quality_mode == QualityMode::Automatic)
            .then_some(client.automatic_min_bitrate_bps)
            .flatten()
            .filter(|value| *value > 0),
        automatic_max_bitrate_bps: (client.quality_mode == QualityMode::Automatic)
            .then_some(client.automatic_max_bitrate_bps)
            .flatten()
            .filter(|value| *value > 0),
        automatic_min_resolution: (client.quality_mode == QualityMode::Automatic)
            .then_some(
                client
                    .automatic_min_resolution
                    .clone()
                    .filter(|value| !is_unlimited_resolution(value)),
            )
            .flatten(),
        automatic_max_resolution: (client.quality_mode == QualityMode::Automatic)
            .then_some(
                client
                    .automatic_max_resolution
                    .clone()
                    .filter(|value| !is_unlimited_resolution(value)),
            )
            .flatten(),
        abr_support_type: client.abr_support_type,
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
        unknown_performance_policy: server.unknown_performance_policy,
        performance_envelopes: server.performance_envelopes.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn phase25_catalog_contains_required_versioned_profiles() {
        let ids: BTreeSet<String> = playback_profile_catalog()
            .into_iter()
            .map(|entry| entry.id)
            .collect();

        assert_eq!(
            ids,
            BTreeSet::from([
                PROFILE_ANDROID_MOBILE.to_string(),
                PROFILE_ANDROID_TV.to_string(),
                PROFILE_APPLE_TV.to_string(),
                PROFILE_GAME_CONSOLE_CONSERVATIVE.to_string(),
                PROFILE_IOS_IPADOS.to_string(),
                PROFILE_NATIVE_MPV_DESKTOP.to_string(),
                PROFILE_ROKU_LIKE.to_string(),
                PROFILE_SMART_TV_CONSERVATIVE.to_string(),
                PROFILE_WEB_CHROMIUM.to_string(),
                PROFILE_WEB_SAFARI.to_string(),
            ])
        );

        for profile_id in ids {
            let profile = profile_for_id(&profile_id).expect("catalog profile");
            assert_eq!(profile.profile_version, PLAYBACK_PROFILE_CATALOG_VERSION);
            assert!(
                !profile.supported_containers.is_empty(),
                "{profile_id} containers"
            );
            assert!(
                !profile.supported_video_codecs.is_empty(),
                "{profile_id} video codecs"
            );
            assert!(
                !profile.supported_audio_codecs.is_empty(),
                "{profile_id} audio codecs"
            );
            assert!(
                !profile.supported_subtitle_codecs.is_empty(),
                "{profile_id} subtitle codecs"
            );
            assert!(
                !profile.supported_hls_segment_types.is_empty(),
                "{profile_id} hls segments"
            );
            assert!(profile.max_bitrate_bps.is_some() || profile.direct_play_preferred);
            assert!(profile.max_audio_channels.is_some() || profile.direct_play_preferred);
        }
    }

    #[test]
    fn phase25_unknown_profile_falls_back_to_conservative_browser_defaults() {
        let negotiated = negotiate_playback_profile(
            Some("totally-unknown-player"),
            Some(999),
            Some("99.0.0"),
            Some("native_mpv"),
        );

        assert!(!negotiated.negotiation.known_profile);
        assert_eq!(negotiated.profile.profile_id, PROFILE_WEB_CHROMIUM);
        assert_eq!(
            negotiated.profile.profile_version,
            PLAYBACK_PROFILE_CATALOG_VERSION
        );
        assert!(!negotiated.profile.direct_play_preferred);
        assert_eq!(negotiated.profile.supported_containers, vec!["mp4"]);
        assert_eq!(negotiated.profile.supported_video_codecs, vec!["h264"]);
        assert_eq!(negotiated.profile.max_audio_channels, Some(2));
        assert!(!negotiated.profile.supports_hdr);
        assert!(negotiated.profile.strict_h264_profile_limits);
        assert!(
            negotiated
                .negotiation
                .compatibility
                .iter()
                .any(|reason| reason == "unknown_profile_conservative_browser_defaults")
        );
        assert!(
            negotiated
                .negotiation
                .compatibility
                .iter()
                .any(|reason| { reason == "requested_profile_version_capped_to_server_catalog" })
        );
    }

    #[test]
    fn phase25_legacy_native_kind_maps_through_versioned_catalog() {
        let negotiated =
            negotiate_playback_profile(None, Some(4), Some("0.1.0"), Some("native_mpv"));

        assert!(negotiated.negotiation.known_profile);
        assert_eq!(negotiated.profile.profile_id, PROFILE_NATIVE_MPV_DESKTOP);
        assert_eq!(negotiated.profile.profile_version, 4);
        assert!(negotiated.profile.direct_play_preferred);
        assert!(negotiated.profile.supports_native_text_subtitles);
        assert!(!negotiated.profile.strict_h264_profile_limits);
        assert!(
            negotiated
                .negotiation
                .compatibility
                .iter()
                .any(|reason| reason == "profile_version_4_compatibility_snapshot")
        );
    }
}
