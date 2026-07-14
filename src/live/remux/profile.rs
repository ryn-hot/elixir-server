use std::path::Path;

use serde::Deserialize;

use crate::live::session::SessionProtocol;

const MAX_PROBE_STREAMS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyRemuxProfile {
    DashToHls,
    MpegTsToHls,
}

impl CopyRemuxProfile {
    pub fn for_protocol(protocol: SessionProtocol) -> Option<Self> {
        match protocol {
            SessionProtocol::Dash => Some(Self::DashToHls),
            SessionProtocol::MpegTs => Some(Self::MpegTsToHls),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DashToHls => "dash_to_hls_copy",
            Self::MpegTsToHls => "mpeg_ts_to_hls_copy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemuxProfileError {
    UnsupportedProtocol,
    ProbeInvalid,
    NoPlayableStream,
    UnsupportedCodec,
    InvalidLoopbackInput,
    InvalidOutputPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeSummary {
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub audio_only: bool,
}

#[derive(Deserialize)]
struct ProbeDocument {
    streams: Vec<ProbeStream>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeStream {
    codec_type: String,
    codec_name: String,
}

pub fn parse_probe(output: &[u8]) -> Result<ProbeSummary, RemuxProfileError> {
    let document: ProbeDocument =
        serde_json::from_slice(output).map_err(|_| RemuxProfileError::ProbeInvalid)?;
    if document.streams.is_empty() || document.streams.len() > MAX_PROBE_STREAMS {
        return Err(RemuxProfileError::NoPlayableStream);
    }
    let mut video_codec = None;
    let mut audio_codec = None;
    for stream in document.streams {
        let codec = stream.codec_name.trim().to_ascii_lowercase();
        if codec.is_empty()
            || codec.len() > 64
            || !codec
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(RemuxProfileError::ProbeInvalid);
        }
        match stream.codec_type.as_str() {
            "video" => {
                if !matches!(codec.as_str(), "h264" | "hevc" | "h265") {
                    return Err(RemuxProfileError::UnsupportedCodec);
                }
                video_codec.get_or_insert(codec);
            }
            "audio" => {
                if !matches!(codec.as_str(), "aac" | "ac3" | "eac3" | "mp3") {
                    return Err(RemuxProfileError::UnsupportedCodec);
                }
                audio_codec.get_or_insert(codec);
            }
            _ => {}
        }
    }
    if video_codec.is_none() && audio_codec.is_none() {
        return Err(RemuxProfileError::NoPlayableStream);
    }
    Ok(ProbeSummary {
        audio_only: video_codec.is_none(),
        video_codec,
        audio_codec,
    })
}

pub fn ffprobe_args(input_url: &str) -> Result<Vec<String>, RemuxProfileError> {
    validate_loopback_input(input_url)?;
    Ok(vec![
        "-v".to_string(),
        "error".to_string(),
        "-probesize".to_string(),
        "8388608".to_string(),
        "-analyzeduration".to_string(),
        "8000000".to_string(),
        "-show_entries".to_string(),
        "stream=codec_type,codec_name".to_string(),
        "-of".to_string(),
        "json".to_string(),
        input_url.to_string(),
    ])
}

pub fn ffmpeg_args(
    profile: CopyRemuxProfile,
    input_url: &str,
    output_dir: &Path,
    segment_seconds: u64,
    playlist_segments: u32,
    delete_threshold: u32,
) -> Result<Vec<String>, RemuxProfileError> {
    validate_loopback_input(input_url)?;
    if !output_dir.is_absolute()
        || output_dir
            .to_str()
            .is_none_or(|value| value.is_empty() || value.chars().any(char::is_control))
    {
        return Err(RemuxProfileError::InvalidOutputPath);
    }
    let playlist = output_dir.join("index.m3u8");
    let segments = output_dir.join("segment-%010d.ts");
    let playlist = playlist
        .to_str()
        .ok_or(RemuxProfileError::InvalidOutputPath)?;
    let segments = segments
        .to_str()
        .ok_or(RemuxProfileError::InvalidOutputPath)?;
    let mut arguments = vec![
        "-hide_banner".to_string(),
        "-nostdin".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
    ];
    if profile == CopyRemuxProfile::MpegTsToHls {
        arguments.extend(["-f".to_string(), "mpegts".to_string()]);
    }
    arguments.extend([
        "-re".to_string(),
        "-i".to_string(),
        input_url.to_string(),
        "-map".to_string(),
        "0:v?".to_string(),
        "-map".to_string(),
        "0:a?".to_string(),
        "-codec".to_string(),
        "copy".to_string(),
        "-max_muxing_queue_size".to_string(),
        "1024".to_string(),
        "-f".to_string(),
        "hls".to_string(),
        "-hls_time".to_string(),
        segment_seconds.to_string(),
        "-hls_list_size".to_string(),
        playlist_segments.to_string(),
        "-hls_delete_threshold".to_string(),
        delete_threshold.to_string(),
        "-hls_flags".to_string(),
        "delete_segments+omit_endlist+temp_file".to_string(),
        "-hls_segment_filename".to_string(),
        segments.to_string(),
        playlist.to_string(),
    ]);
    if arguments.iter().any(|argument| {
        matches!(
            argument.as_str(),
            "-vf" | "-af" | "-filter" | "-filter_complex" | "-c:v" | "-c:a"
        )
    }) || arguments
        .windows(2)
        .any(|pair| pair[0] == "-codec" && pair[1] != "copy")
    {
        return Err(RemuxProfileError::UnsupportedCodec);
    }
    Ok(arguments)
}

fn validate_loopback_input(input_url: &str) -> Result<(), RemuxProfileError> {
    let url =
        reqwest::Url::parse(input_url).map_err(|_| RemuxProfileError::InvalidLoopbackInput)?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(RemuxProfileError::InvalidLoopbackInput);
    }
    Ok(())
}
