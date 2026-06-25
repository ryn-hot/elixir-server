use std::{collections::BTreeSet, process::Stdio, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{process::Command, time::timeout};

const STARTUP_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const FFMPEG_CAPABILITY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareApi {
    VideoToolbox,
    Vaapi,
    Qsv,
    Nvenc,
    Amf,
}

impl HardwareApi {
    pub const ALL: [Self; 5] = [
        Self::VideoToolbox,
        Self::Vaapi,
        Self::Qsv,
        Self::Nvenc,
        Self::Amf,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::VideoToolbox => "videotoolbox",
            Self::Vaapi => "vaapi",
            Self::Qsv => "qsv",
            Self::Nvenc => "nvenc",
            Self::Amf => "amf",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "videotoolbox" | "video_toolbox" | "vt" => Some(Self::VideoToolbox),
            "vaapi" => Some(Self::Vaapi),
            "qsv" | "quick_sync" | "quicksync" => Some(Self::Qsv),
            "nvenc" | "cuda" => Some(Self::Nvenc),
            "amf" => Some(Self::Amf),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwarePreference {
    Auto,
    Off,
    Api(HardwareApi),
}

impl HardwarePreference {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Self::Auto,
            "off" | "false" | "disabled" | "none" | "software" => Self::Off,
            value => HardwareApi::parse(value)
                .map(Self::Api)
                .unwrap_or(Self::Off),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwareFallbackPolicy {
    Software,
    Fail,
}

impl HardwareFallbackPolicy {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "fail" | "error" => Self::Fail,
            _ => Self::Software,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Software => "software",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCodecSupport {
    pub api: String,
    pub codec: String,
    pub ffmpeg_name: String,
}

impl HardwareCodecSupport {
    fn new(api: HardwareApi, codec: &str, ffmpeg_name: &str) -> Self {
        Self {
            api: api.as_str().to_string(),
            codec: codec.to_string(),
            ffmpeg_name: ffmpeg_name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareStartupProbe {
    pub api: String,
    pub operation: String,
    pub ok: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCapabilities {
    pub platform: String,
    pub ffmpeg_version: Option<String>,
    pub available_apis: Vec<String>,
    pub supported_decode_codecs: Vec<HardwareCodecSupport>,
    pub supported_encode_codecs: Vec<HardwareCodecSupport>,
    pub max_sessions: Option<u32>,
    pub hdr_tone_mapping: bool,
    pub subtitle_burn_in_limitations: Vec<String>,
    pub startup_probes: Vec<HardwareStartupProbe>,
    pub detection_errors: Vec<String>,
}

impl Default for HardwareCapabilities {
    fn default() -> Self {
        Self {
            platform: current_platform(),
            ffmpeg_version: None,
            available_apis: Vec::new(),
            supported_decode_codecs: Vec::new(),
            supported_encode_codecs: Vec::new(),
            max_sessions: None,
            hdr_tone_mapping: false,
            subtitle_burn_in_limitations: vec![
                "subtitle_burn_in_requires_software_filter".to_string(),
            ],
            startup_probes: Vec::new(),
            detection_errors: Vec::new(),
        }
    }
}

impl HardwareCapabilities {
    pub fn software_only() -> Self {
        Self::default()
    }

    pub fn is_api_available(&self, api: HardwareApi) -> bool {
        self.available_apis
            .iter()
            .any(|value| value.eq_ignore_ascii_case(api.as_str()))
    }

    pub fn encode_support(&self, api: HardwareApi, codec: &str) -> Option<&HardwareCodecSupport> {
        self.supported_encode_codecs.iter().find(|support| {
            support.api.eq_ignore_ascii_case(api.as_str())
                && support.codec.eq_ignore_ascii_case(codec)
        })
    }

    pub fn decode_support(&self, api: HardwareApi, codec: &str) -> Option<&HardwareCodecSupport> {
        self.supported_decode_codecs.iter().find(|support| {
            support.api.eq_ignore_ascii_case(api.as_str())
                && support.codec.eq_ignore_ascii_case(codec)
        })
    }

    pub fn preferred_api_for_encode(&self, codec: &str) -> Option<HardwareApi> {
        HardwareApi::ALL
            .into_iter()
            .find(|api| self.is_api_available(*api) && self.encode_support(*api, codec).is_some())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardwareDetectionConfig {
    pub preference: HardwarePreference,
}

impl Default for HardwareDetectionConfig {
    fn default() -> Self {
        Self {
            preference: HardwarePreference::Auto,
        }
    }
}

pub async fn detect_hardware_capabilities(
    config: &HardwareDetectionConfig,
) -> HardwareCapabilities {
    if config.preference == HardwarePreference::Off {
        return HardwareCapabilities::software_only();
    }

    let platform = current_platform();
    let mut detection_errors = Vec::new();
    let ffmpeg_version = run_ffmpeg_text(&["-version"], FFMPEG_CAPABILITY_TIMEOUT)
        .await
        .ok()
        .and_then(|output| output.lines().next().map(str::to_string));
    let hwaccels =
        match run_ffmpeg_text(&["-hide_banner", "-hwaccels"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_hwaccels(&output),
            Err(err) => {
                detection_errors.push(format!("hwaccels_probe_failed:{err}"));
                BTreeSet::new()
            }
        };
    let encoders =
        match run_ffmpeg_text(&["-hide_banner", "-encoders"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_ffmpeg_components(&output),
            Err(err) => {
                detection_errors.push(format!("encoders_probe_failed:{err}"));
                BTreeSet::new()
            }
        };
    let decoders =
        match run_ffmpeg_text(&["-hide_banner", "-decoders"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_ffmpeg_components(&output),
            Err(err) => {
                detection_errors.push(format!("decoders_probe_failed:{err}"));
                BTreeSet::new()
            }
        };

    let candidate_apis = match config.preference {
        HardwarePreference::Auto => HardwareApi::ALL.to_vec(),
        HardwarePreference::Api(api) => vec![api],
        HardwarePreference::Off => Vec::new(),
    };

    let mut available_apis = Vec::new();
    let mut supported_decode_codecs = Vec::new();
    let mut supported_encode_codecs = Vec::new();
    let mut startup_probes = Vec::new();

    for api in candidate_apis {
        let configured = api_configured(api, &hwaccels, &encoders, &decoders);
        if !configured {
            continue;
        }

        let encode_probe = startup_probe_encoder(api, &encoders).await;
        let encode_startup_ok = encode_probe.as_ref().map(|probe| probe.ok).unwrap_or(true);
        if let Some(probe) = encode_probe {
            if !probe.ok {
                detection_errors.push(format!(
                    "{}_startup_probe_failed:{}",
                    api.as_str(),
                    probe.detail.as_deref().unwrap_or("unknown")
                ));
            }
            startup_probes.push(probe);
        }

        if !encode_startup_ok {
            continue;
        }

        available_apis.push(api.as_str().to_string());
        supported_encode_codecs.extend(detect_encode_codecs(api, &encoders));
        supported_decode_codecs.extend(detect_decode_codecs(api, &hwaccels, &decoders));
    }

    HardwareCapabilities {
        platform,
        ffmpeg_version,
        available_apis,
        supported_decode_codecs,
        supported_encode_codecs,
        max_sessions: None,
        hdr_tone_mapping: false,
        subtitle_burn_in_limitations: vec!["subtitle_burn_in_requires_software_filter".to_string()],
        startup_probes,
        detection_errors,
    }
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn api_configured(
    api: HardwareApi,
    hwaccels: &BTreeSet<String>,
    encoders: &BTreeSet<String>,
    decoders: &BTreeSet<String>,
) -> bool {
    match api {
        HardwareApi::VideoToolbox => {
            hwaccels.contains("videotoolbox") || encoders.contains("h264_videotoolbox")
        }
        HardwareApi::Vaapi => {
            hwaccels.contains("vaapi") || encoders.iter().any(|e| e.ends_with("_vaapi"))
        }
        HardwareApi::Qsv => {
            hwaccels.contains("qsv")
                || encoders.iter().any(|e| e.ends_with("_qsv"))
                || decoders.iter().any(|d| d.ends_with("_qsv"))
        }
        HardwareApi::Nvenc => encoders.iter().any(|e| e.ends_with("_nvenc")),
        HardwareApi::Amf => encoders.iter().any(|e| e.ends_with("_amf")),
    }
}

async fn startup_probe_encoder(
    api: HardwareApi,
    encoders: &BTreeSet<String>,
) -> Option<HardwareStartupProbe> {
    let encoder = match api {
        HardwareApi::VideoToolbox if encoders.contains("h264_videotoolbox") => "h264_videotoolbox",
        HardwareApi::Vaapi if encoders.contains("h264_vaapi") => "h264_vaapi",
        HardwareApi::Qsv if encoders.contains("h264_qsv") => "h264_qsv",
        HardwareApi::Nvenc if encoders.contains("h264_nvenc") => "h264_nvenc",
        HardwareApi::Amf if encoders.contains("h264_amf") => "h264_amf",
        _ => return None,
    };

    let mut args = vec![
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=64x64:rate=1:duration=0.2",
        "-frames:v",
        "1",
        "-c:v",
        encoder,
    ];
    if api == HardwareApi::VideoToolbox {
        args.extend(["-allow_sw", "0"]);
    }
    args.extend(["-f", "null", "-"]);

    let result = run_ffmpeg_text(&args, STARTUP_PROBE_TIMEOUT).await;
    Some(match result {
        Ok(_) => HardwareStartupProbe {
            api: api.as_str().to_string(),
            operation: format!("encode:{encoder}"),
            ok: true,
            detail: None,
        },
        Err(err) => HardwareStartupProbe {
            api: api.as_str().to_string(),
            operation: format!("encode:{encoder}"),
            ok: false,
            detail: Some(err),
        },
    })
}

fn detect_encode_codecs(
    api: HardwareApi,
    encoders: &BTreeSet<String>,
) -> Vec<HardwareCodecSupport> {
    match api {
        HardwareApi::VideoToolbox => [("h264", "h264_videotoolbox"), ("hevc", "hevc_videotoolbox")]
            .into_iter()
            .filter(|(_, encoder)| encoders.contains(*encoder))
            .map(|(codec, encoder)| HardwareCodecSupport::new(api, codec, encoder))
            .collect(),
        HardwareApi::Vaapi => hardware_suffix_codecs(api, encoders, "_vaapi"),
        HardwareApi::Qsv => hardware_suffix_codecs(api, encoders, "_qsv"),
        HardwareApi::Nvenc => hardware_suffix_codecs(api, encoders, "_nvenc"),
        HardwareApi::Amf => hardware_suffix_codecs(api, encoders, "_amf"),
    }
}

fn hardware_suffix_codecs(
    api: HardwareApi,
    components: &BTreeSet<String>,
    suffix: &str,
) -> Vec<HardwareCodecSupport> {
    ["h264", "hevc", "av1", "vp9", "mpeg2video"]
        .into_iter()
        .filter_map(|codec| {
            let ffmpeg_name = format!("{codec}{suffix}");
            components
                .contains(&ffmpeg_name)
                .then(|| HardwareCodecSupport::new(api, codec, &ffmpeg_name))
        })
        .collect()
}

fn detect_decode_codecs(
    api: HardwareApi,
    hwaccels: &BTreeSet<String>,
    decoders: &BTreeSet<String>,
) -> Vec<HardwareCodecSupport> {
    match api {
        HardwareApi::VideoToolbox if hwaccels.contains("videotoolbox") => ["h264", "hevc"]
            .into_iter()
            .map(|codec| HardwareCodecSupport::new(api, codec, "videotoolbox"))
            .collect(),
        HardwareApi::Vaapi if hwaccels.contains("vaapi") => {
            ["h264", "hevc", "mpeg2video", "vp9", "av1"]
                .into_iter()
                .map(|codec| HardwareCodecSupport::new(api, codec, "vaapi"))
                .collect()
        }
        HardwareApi::Qsv => hardware_suffix_codecs(api, decoders, "_qsv"),
        HardwareApi::Nvenc if hwaccels.contains("cuda") || hwaccels.contains("nvdec") => {
            ["h264", "hevc", "av1"]
                .into_iter()
                .map(|codec| HardwareCodecSupport::new(api, codec, "cuda"))
                .collect()
        }
        HardwareApi::Amf => Vec::new(),
        _ => Vec::new(),
    }
}

pub fn parse_hwaccels(raw: &str) -> BTreeSet<String> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| !line.starts_with("Hardware acceleration methods"))
        .map(str::to_ascii_lowercase)
        .collect()
}

pub fn parse_ffmpeg_components(raw: &str) -> BTreeSet<String> {
    raw.lines()
        .filter_map(|line| line.split_whitespace().nth(1))
        .filter(|name| !name.starts_with('='))
        .map(str::to_ascii_lowercase)
        .collect()
}

async fn run_ffmpeg_text(args: &[&str], wait: Duration) -> Result<String, String> {
    let output = timeout(
        wait,
        Command::new("ffmpeg")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .map_err(|_| format!("ffmpeg timed out after {}s", wait.as_secs()))?
    .map_err(|err| format!("failed to spawn ffmpeg: {err}"))?;

    if output.status.success() {
        let mut text = String::from_utf8_lossy(&output.stdout).to_string();
        if text.trim().is_empty() {
            text = String::from_utf8_lossy(&output.stderr).to_string();
        }
        Ok(text)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!(
            "ffmpeg exited with {:?}: {}",
            output.status.code(),
            stderr.lines().take(8).collect::<Vec<_>>().join(" | ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hwaccels_and_components() {
        let hwaccels = parse_hwaccels(
            "Hardware acceleration methods:\n\
             videotoolbox\n\
             vaapi\n",
        );
        assert!(hwaccels.contains("videotoolbox"));
        assert!(hwaccels.contains("vaapi"));

        let encoders = parse_ffmpeg_components(
            " V....D h264_videotoolbox    VideoToolbox H.264 Encoder\n\
              V....D hevc_videotoolbox    VideoToolbox H.265 Encoder\n\
              V....D h264_nvenc           NVIDIA NVENC H.264 encoder\n",
        );
        assert!(encoders.contains("h264_videotoolbox"));
        assert!(encoders.contains("hevc_videotoolbox"));
        assert!(encoders.contains("h264_nvenc"));
    }

    #[test]
    fn reports_videotoolbox_codec_support_from_ffmpeg_fixtures() {
        let hwaccels = parse_hwaccels("Hardware acceleration methods:\nvideotoolbox\n");
        let encoders = parse_ffmpeg_components(
            " V....D h264_videotoolbox    VideoToolbox H.264 Encoder\n\
              V....D hevc_videotoolbox    VideoToolbox H.265 Encoder\n",
        );
        let decoders = BTreeSet::new();

        assert!(api_configured(
            HardwareApi::VideoToolbox,
            &hwaccels,
            &encoders,
            &decoders
        ));
        let encode = detect_encode_codecs(HardwareApi::VideoToolbox, &encoders);
        let decode = detect_decode_codecs(HardwareApi::VideoToolbox, &hwaccels, &decoders);

        assert!(encode.iter().any(|support| support.codec == "h264"));
        assert!(encode.iter().any(|support| support.codec == "hevc"));
        assert!(decode.iter().any(|support| support.codec == "h264"));
        assert!(decode.iter().any(|support| support.codec == "hevc"));
    }

    #[test]
    fn parses_preferences_and_fallbacks() {
        assert_eq!(HardwarePreference::parse("auto"), HardwarePreference::Auto);
        assert_eq!(HardwarePreference::parse("off"), HardwarePreference::Off);
        assert_eq!(
            HardwarePreference::parse("videotoolbox"),
            HardwarePreference::Api(HardwareApi::VideoToolbox)
        );
        assert_eq!(
            HardwareFallbackPolicy::parse("fail"),
            HardwareFallbackPolicy::Fail
        );
        assert_eq!(
            HardwareFallbackPolicy::parse("software"),
            HardwareFallbackPolicy::Software
        );
    }
}
