use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use reqwest::{
    Client, StatusCode, Url,
    header::{HeaderMap, HeaderName, HeaderValue, RANGE, REFERER, USER_AGENT},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::http::handlers::acquisition_sources::{
    validate_safe_http_url, validate_stream_candidate_for_broker,
};

pub const FAILURE_INVALID_CANDIDATE_SHAPE: &str = "invalid_candidate_shape";
pub const FAILURE_UNSAFE_URL: &str = "unsafe_url";
pub const FAILURE_SOURCE_RETURNED_NON_MEDIA_RESPONSE: &str = "source_returned_non_media_response";
pub const FAILURE_HOSTER_RESOLVER_MISSING: &str = "hoster_resolver_missing";
pub const FAILURE_ACCOUNT_REQUIRED: &str = "account_required";
pub const FAILURE_CAPTCHA_OR_BROWSER_REQUIRED: &str = "captcha_or_browser_required";
pub const FAILURE_DRM_OR_LICENSE_REQUIRED: &str = "drm_or_license_required";
pub const FAILURE_MATERIALIZATION_PREFLIGHT_FAILED: &str = "materialization_preflight_failed";
pub const FAILURE_NETWORK_BLOCKED: &str = "network_blocked";

const DEFAULT_PREFLIGHT_TIMEOUT_SECONDS: u64 = 12;
const DEFAULT_SAMPLE_BYTES: usize = 128 * 1024;
const DEFAULT_MANIFEST_BYTES: usize = 512 * 1024;
const DEFAULT_SEGMENT_SAMPLE_BYTES: usize = 64 * 1024;
const PREFLIGHT_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Elixir-Prism-Certifier/1";

#[derive(Debug, Clone)]
pub struct StreamCandidatePreflightConfig {
    pub timeout: Duration,
    pub sample_bytes: usize,
    pub manifest_bytes: usize,
    pub segment_sample_bytes: usize,
}

impl Default for StreamCandidatePreflightConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(DEFAULT_PREFLIGHT_TIMEOUT_SECONDS),
            sample_bytes: DEFAULT_SAMPLE_BYTES,
            manifest_bytes: DEFAULT_MANIFEST_BYTES,
            segment_sample_bytes: DEFAULT_SEGMENT_SAMPLE_BYTES,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StreamCandidatePreflightReport {
    pub passed: bool,
    pub failure_class: Option<String>,
    pub summary: String,
    pub stream_type: Option<String>,
    pub url: Option<String>,
    pub final_url: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub sample_bytes: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl StreamCandidatePreflightReport {
    pub fn passed(summary: impl Into<String>) -> Self {
        Self {
            passed: true,
            failure_class: None,
            summary: summary.into(),
            stream_type: None,
            url: None,
            final_url: None,
            content_type: None,
            content_length: None,
            sample_bytes: 0,
            warnings: Vec::new(),
        }
    }

    pub fn failed(failure_class: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            passed: false,
            failure_class: Some(failure_class.into()),
            summary: summary.into(),
            stream_type: None,
            url: None,
            final_url: None,
            content_type: None,
            content_length: None,
            sample_bytes: 0,
            warnings: Vec::new(),
        }
    }

    pub fn evidence_json(&self) -> Value {
        json!(self)
    }

    fn with_candidate(mut self, candidate: &Value) -> Self {
        self.stream_type = stream_candidate_string(candidate, "/delivery/streamType")
            .map(|value| value.to_ascii_lowercase());
        self.url = stream_candidate_string(candidate, "/delivery/url");
        self
    }

    fn with_response(mut self, response: &BoundedHttpResponse, sample_bytes: usize) -> Self {
        self.final_url = Some(response.final_url.clone());
        self.content_type = response.content_type.clone();
        self.content_length = response.content_length;
        self.sample_bytes = sample_bytes;
        self
    }
}

#[derive(Debug)]
struct BoundedHttpResponse {
    final_url: String,
    content_type: Option<String>,
    content_length: Option<u64>,
    body: Vec<u8>,
}

pub async fn preflight_stream_candidate(candidate: &Value) -> StreamCandidatePreflightReport {
    preflight_stream_candidate_with_config(candidate, &StreamCandidatePreflightConfig::default())
        .await
}

pub async fn preflight_stream_candidate_with_config(
    candidate: &Value,
    config: &StreamCandidatePreflightConfig,
) -> StreamCandidatePreflightReport {
    let (candidate, validation_warnings) =
        match validate_stream_candidate_for_broker(candidate.clone()) {
            Ok((candidate, warnings)) => (candidate, warnings),
            Err(err) => {
                let message = err.to_string();
                let failure_class = if message.to_ascii_lowercase().contains("unsafe") {
                    FAILURE_UNSAFE_URL
                } else {
                    FAILURE_INVALID_CANDIDATE_SHAPE
                };
                return StreamCandidatePreflightReport::failed(failure_class, message);
            }
        };
    let stream_type = stream_candidate_string(&candidate, "/delivery/streamType")
        .unwrap_or_else(|| "direct_file".to_string())
        .to_ascii_lowercase();
    let Some(url) = stream_candidate_string(&candidate, "/delivery/url") else {
        return StreamCandidatePreflightReport::failed(
            FAILURE_MATERIALIZATION_PREFLIGHT_FAILED,
            "stream candidate requires late resolve before materialization preflight",
        )
        .with_candidate(&candidate);
    };
    let url = match validate_safe_http_url(&url) {
        Ok(url) => url,
        Err(err) => {
            return StreamCandidatePreflightReport::failed(
                FAILURE_UNSAFE_URL,
                format!("delivery URL is unsafe: {err}"),
            )
            .with_candidate(&candidate);
        }
    };

    let client = match Client::builder()
        .timeout(config.timeout)
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            return StreamCandidatePreflightReport::failed(
                FAILURE_MATERIALIZATION_PREFLIGHT_FAILED,
                format!("could not build preflight HTTP client: {err}"),
            )
            .with_candidate(&candidate);
        }
    };

    let mut report = match stream_type.as_str() {
        "hls" => preflight_hls_candidate(&client, &candidate, &url, config).await,
        "dash" => preflight_dash_candidate(&client, &candidate, &url, config).await,
        "direct_file" | "direct" | "file" => {
            preflight_direct_file_candidate(&client, &candidate, &url, config).await
        }
        other => StreamCandidatePreflightReport::failed(
            FAILURE_INVALID_CANDIDATE_SHAPE,
            format!("delivery.streamType '{other}' is unsupported for certification preflight"),
        )
        .with_candidate(&candidate),
    };
    report.warnings.extend(validation_warnings);
    report
}

async fn preflight_direct_file_candidate(
    client: &Client,
    candidate: &Value,
    url: &Url,
    config: &StreamCandidatePreflightConfig,
) -> StreamCandidatePreflightReport {
    let response = match bounded_get(client, candidate, url, config.sample_bytes).await {
        Ok(response) => response,
        Err(err) => {
            return StreamCandidatePreflightReport::failed(
                classify_network_error(&err),
                format!("direct file preflight failed: {err}"),
            )
            .with_candidate(candidate);
        }
    };
    let sample_len = response.body.len();
    if let Some((failure_class, summary)) =
        classify_non_media_response(url, response.content_type.as_deref(), &response.body)
    {
        return StreamCandidatePreflightReport::failed(failure_class, summary)
            .with_candidate(candidate)
            .with_response(&response, sample_len);
    }
    if direct_file_response_looks_media(url, response.content_type.as_deref(), &response.body) {
        return StreamCandidatePreflightReport::passed(
            "direct file candidate returned media-like response headers or bytes",
        )
        .with_candidate(candidate)
        .with_response(&response, sample_len);
    }
    StreamCandidatePreflightReport::failed(
        FAILURE_MATERIALIZATION_PREFLIGHT_FAILED,
        "direct file preflight did not observe a media-like content type, filename, or magic-byte sample",
    )
    .with_candidate(candidate)
    .with_response(&response, sample_len)
}

async fn preflight_hls_candidate(
    client: &Client,
    candidate: &Value,
    url: &Url,
    config: &StreamCandidatePreflightConfig,
) -> StreamCandidatePreflightReport {
    let response = match bounded_get(client, candidate, url, config.manifest_bytes).await {
        Ok(response) => response,
        Err(err) => {
            return StreamCandidatePreflightReport::failed(
                classify_network_error(&err),
                format!("HLS manifest preflight failed: {err}"),
            )
            .with_candidate(candidate);
        }
    };
    let sample_len = response.body.len();
    let text = String::from_utf8_lossy(&response.body);
    if let Some((failure_class, summary)) =
        classify_manifest_login_or_html(url, response.content_type.as_deref(), &text)
    {
        return StreamCandidatePreflightReport::failed(failure_class, summary)
            .with_candidate(candidate)
            .with_response(&response, sample_len);
    }
    if !text.trim_start().starts_with("#EXTM3U") {
        return StreamCandidatePreflightReport::failed(
            FAILURE_MATERIALIZATION_PREFLIGHT_FAILED,
            "HLS candidate did not return an #EXTM3U manifest",
        )
        .with_candidate(candidate)
        .with_response(&response, sample_len);
    }
    if hls_manifest_has_drm(&text) {
        return StreamCandidatePreflightReport::failed(
            FAILURE_DRM_OR_LICENSE_REQUIRED,
            "HLS manifest declares encryption or a license flow Elixir cannot materialize",
        )
        .with_candidate(candidate)
        .with_response(&response, sample_len);
    }
    if let Some(segment_url) = first_hls_media_reference(url, &text) {
        match validate_safe_http_url(segment_url.as_str()) {
            Ok(_) => {}
            Err(err) => {
                return StreamCandidatePreflightReport::failed(
                    FAILURE_UNSAFE_URL,
                    format!("HLS segment URL is unsafe: {err}"),
                )
                .with_candidate(candidate)
                .with_response(&response, sample_len);
            }
        }
        if !segment_url.path().ends_with(".m3u8") {
            match bounded_get(client, candidate, &segment_url, config.segment_sample_bytes).await {
                Ok(segment) => {
                    if let Some((failure_class, summary)) = classify_non_media_response(
                        &segment_url,
                        segment.content_type.as_deref(),
                        &segment.body,
                    ) {
                        return StreamCandidatePreflightReport::failed(failure_class, summary)
                            .with_candidate(candidate)
                            .with_response(&response, sample_len);
                    }
                }
                Err(err) => {
                    return StreamCandidatePreflightReport::failed(
                        classify_network_error(&err),
                        format!("HLS segment preflight failed: {err}"),
                    )
                    .with_candidate(candidate)
                    .with_response(&response, sample_len);
                }
            }
        }
    }
    StreamCandidatePreflightReport::passed("HLS manifest passed bounded preflight")
        .with_candidate(candidate)
        .with_response(&response, sample_len)
}

async fn preflight_dash_candidate(
    client: &Client,
    candidate: &Value,
    url: &Url,
    config: &StreamCandidatePreflightConfig,
) -> StreamCandidatePreflightReport {
    let response = match bounded_get(client, candidate, url, config.manifest_bytes).await {
        Ok(response) => response,
        Err(err) => {
            return StreamCandidatePreflightReport::failed(
                classify_network_error(&err),
                format!("DASH manifest preflight failed: {err}"),
            )
            .with_candidate(candidate);
        }
    };
    let sample_len = response.body.len();
    let text = String::from_utf8_lossy(&response.body);
    if let Some((failure_class, summary)) =
        classify_manifest_login_or_html(url, response.content_type.as_deref(), &text)
    {
        return StreamCandidatePreflightReport::failed(failure_class, summary)
            .with_candidate(candidate)
            .with_response(&response, sample_len);
    }
    let lower = text.to_ascii_lowercase();
    if !lower.contains("<mpd") {
        return StreamCandidatePreflightReport::failed(
            FAILURE_MATERIALIZATION_PREFLIGHT_FAILED,
            "DASH candidate did not return an MPD manifest",
        )
        .with_candidate(candidate)
        .with_response(&response, sample_len);
    }
    if dash_manifest_has_drm(&lower) {
        return StreamCandidatePreflightReport::failed(
            FAILURE_DRM_OR_LICENSE_REQUIRED,
            "DASH manifest declares DRM content protection Elixir cannot materialize",
        )
        .with_candidate(candidate)
        .with_response(&response, sample_len);
    }
    StreamCandidatePreflightReport::passed("DASH manifest passed bounded preflight")
        .with_candidate(candidate)
        .with_response(&response, sample_len)
}

async fn bounded_get(
    client: &Client,
    candidate: &Value,
    url: &Url,
    max_bytes: usize,
) -> Result<BoundedHttpResponse> {
    let mut headers = preflight_headers(candidate)?;
    headers.insert(
        RANGE,
        HeaderValue::from_str(&format!("bytes=0-{}", max_bytes.saturating_sub(1)))?,
    );
    if !headers.contains_key(USER_AGENT) {
        headers.insert(USER_AGENT, HeaderValue::from_static(PREFLIGHT_USER_AGENT));
    }
    let mut response = client
        .get(url.clone())
        .headers(headers)
        .send()
        .await
        .with_context(|| format!("requesting {url}"))?;
    let status = response.status();
    if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
        return Err(anyhow!("HTTP status {status}"));
    }
    let final_url = response.url().to_string();
    validate_safe_http_url(&final_url).context("final URL is unsafe")?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let content_length = response.content_length();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.context("reading preflight body")? {
        let remaining = max_bytes.saturating_sub(body.len());
        if remaining == 0 {
            break;
        }
        if chunk.len() <= remaining {
            body.extend_from_slice(&chunk);
        } else {
            body.extend_from_slice(&chunk[..remaining]);
            break;
        }
    }
    Ok(BoundedHttpResponse {
        final_url,
        content_type,
        content_length,
        body,
    })
}

fn preflight_headers(candidate: &Value) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    if let Some(values) = candidate
        .pointer("/delivery/headers")
        .and_then(Value::as_object)
    {
        for (name, value) in values {
            let value = value
                .as_str()
                .ok_or_else(|| anyhow!("delivery.headers.{name} must be a string"))?;
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())
                    .with_context(|| format!("delivery.headers.{name} has invalid name"))?,
                HeaderValue::from_str(value)
                    .with_context(|| format!("delivery.headers.{name} has invalid value"))?,
            );
        }
    }
    if let Some(referer) = stream_candidate_string(candidate, "/delivery/referer")
        && !headers.contains_key(REFERER)
    {
        validate_safe_http_url(&referer).context("delivery.referer is unsafe")?;
        headers.insert(REFERER, HeaderValue::from_str(&referer)?);
    }
    Ok(headers)
}

fn stream_candidate_string(candidate: &Value, pointer: &str) -> Option<String> {
    candidate
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn classify_network_error(err: &anyhow::Error) -> &'static str {
    let message = err.to_string().to_ascii_lowercase();
    if message.contains("dns")
        || message.contains("tls")
        || message.contains("timeout")
        || message.contains("timed out")
        || message.contains("connection")
        || message.contains("http status 403")
        || message.contains("http status 451")
    {
        FAILURE_NETWORK_BLOCKED
    } else if message.contains("http status 401") {
        FAILURE_ACCOUNT_REQUIRED
    } else {
        FAILURE_MATERIALIZATION_PREFLIGHT_FAILED
    }
}

fn classify_manifest_login_or_html(
    url: &Url,
    content_type: Option<&str>,
    text: &str,
) -> Option<(&'static str, String)> {
    let bytes = text.as_bytes();
    classify_non_media_response(url, content_type, bytes)
}

fn classify_non_media_response(
    url: &Url,
    content_type: Option<&str>,
    sample: &[u8],
) -> Option<(&'static str, String)> {
    let normalized_type = content_type.map(normalize_content_type);
    let text = String::from_utf8_lossy(sample);
    let lower = text.to_ascii_lowercase();
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if looks_like_captcha_or_browser_challenge(&lower) {
        return Some((
            FAILURE_CAPTCHA_OR_BROWSER_REQUIRED,
            "source returned a browser challenge or captcha page instead of materializable media"
                .to_string(),
        ));
    }
    if looks_like_login_or_account_page(&host, &lower) {
        return Some((
            FAILURE_ACCOUNT_REQUIRED,
            "source returned an account or login-gated page instead of anonymous media".to_string(),
        ));
    }
    if normalized_type
        .as_deref()
        .is_some_and(content_type_is_non_media)
        || sample_looks_like_non_media_document(sample)
    {
        return Some((
            FAILURE_SOURCE_RETURNED_NON_MEDIA_RESPONSE,
            format!(
                "candidate URL returned {} instead of a playable media response",
                normalized_type
                    .as_deref()
                    .unwrap_or("HTML, JSON, XML, or text")
            ),
        ));
    }
    if looks_like_known_hoster_landing_page(&host, &lower) {
        return Some((
            FAILURE_HOSTER_RESOLVER_MISSING,
            "candidate points at a hoster landing page and no resolver produced media bytes"
                .to_string(),
        ));
    }
    None
}

fn direct_file_response_looks_media(url: &Url, content_type: Option<&str>, sample: &[u8]) -> bool {
    content_type
        .map(normalize_content_type)
        .as_deref()
        .is_some_and(content_type_is_media_like)
        || url_path_has_media_extension(url.path())
        || sample_has_media_magic(sample)
}

fn normalize_content_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase()
}

fn content_type_is_non_media(content_type: &str) -> bool {
    content_type.starts_with("text/")
        || matches!(
            content_type,
            "application/json"
                | "application/problem+json"
                | "application/xml"
                | "application/xhtml+xml"
                | "application/javascript"
                | "application/x-javascript"
        )
        || content_type.ends_with("+json")
        || content_type.ends_with("+xml")
}

fn content_type_is_media_like(content_type: &str) -> bool {
    content_type.starts_with("video/")
        || content_type.starts_with("audio/")
        || matches!(
            content_type,
            "application/octet-stream"
                | "binary/octet-stream"
                | "application/mp4"
                | "application/x-matroska"
                | "application/vnd.apple.mpegurl"
                | "application/x-mpegurl"
                | "application/dash+xml"
        )
}

fn sample_looks_like_non_media_document(sample: &[u8]) -> bool {
    let text = String::from_utf8_lossy(sample);
    let trimmed = text.trim_start().to_ascii_lowercase();
    trimmed.starts_with("<!doctype html")
        || trimmed.starts_with("<html")
        || trimmed.starts_with("<?xml")
        || trimmed.starts_with('{')
        || trimmed.starts_with('[')
        || trimmed.contains("<title>")
}

fn looks_like_login_or_account_page(host: &str, lower: &str) -> bool {
    host.contains("cinecloud")
        || lower.contains("sign in with google")
        || lower.contains("login with google")
        || lower.contains("google account")
        || lower.contains("login required")
        || lower.contains("sign in")
        || lower.contains("please login")
        || lower.contains("account required")
        || lower.contains("unauthorized")
}

fn looks_like_captcha_or_browser_challenge(lower: &str) -> bool {
    lower.contains("captcha")
        || lower.contains("cf-challenge")
        || lower.contains("cloudflare")
        || lower.contains("checking your browser")
        || lower.contains("enable javascript")
}

fn looks_like_known_hoster_landing_page(host: &str, lower: &str) -> bool {
    host.contains("hubcloud")
        || host.contains("gdflix")
        || host.contains("filelions")
        || host.contains("streamtape")
        || host.contains("dood")
        || lower.contains("download now")
        || lower.contains("generate link")
}

fn url_path_has_media_extension(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    [
        ".mp4", ".mkv", ".m4v", ".mov", ".webm", ".avi", ".ts", ".m2ts", ".m3u8", ".mpd",
    ]
    .iter()
    .any(|suffix| lower.ends_with(suffix))
}

fn sample_has_media_magic(sample: &[u8]) -> bool {
    sample.windows(4).take(16).any(|window| window == b"ftyp")
        || sample.starts_with(&[0x1a, 0x45, 0xdf, 0xa3])
        || sample.starts_with(b"OggS")
        || sample.starts_with(b"RIFF")
        || sample.starts_with(b"ID3")
        || sample.first().is_some_and(|value| *value == 0x47)
}

fn hls_manifest_has_drm(text: &str) -> bool {
    text.lines().any(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("#ext-x-key")
            && !lower.contains("method=none")
            && (lower.contains("keyformat")
                || lower.contains("sample-aes")
                || lower.contains("widevine")
                || lower.contains("fairplay"))
    })
}

fn first_hls_media_reference(base_url: &Url, text: &str) -> Option<Url> {
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Ok(url) = base_url.join(trimmed) {
            return Some(url);
        }
    }
    None
}

fn dash_manifest_has_drm(lower: &str) -> bool {
    lower.contains("<contentprotection")
        || lower.contains("widevine")
        || lower.contains("playready")
        || lower.contains("com.apple.streamingkeydelivery")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_classifier_marks_cinecloud_html_as_account_required() {
        let url = Url::parse("https://new5.cinecloud.site/f/e095f16e").unwrap();
        let result = classify_non_media_response(
            &url,
            Some("text/html; charset=UTF-8"),
            b"<!doctype html><html><button>Continue with Google</button></html>",
        )
        .expect("classified");
        assert_eq!(result.0, FAILURE_ACCOUNT_REQUIRED);
    }

    #[test]
    fn preflight_classifier_accepts_mp4_magic_bytes() {
        let url = Url::parse("https://cdn.example.test/video").unwrap();
        assert!(direct_file_response_looks_media(
            &url,
            Some("application/octet-stream"),
            b"\0\0\0\x18ftypmp42"
        ));
    }

    #[test]
    fn preflight_hls_drm_detector_rejects_license_manifest() {
        let manifest =
            "#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,KEYFORMAT=\"com.apple.streamingkeydelivery\"\n";
        assert!(hls_manifest_has_drm(manifest));
    }
}
