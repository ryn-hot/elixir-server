use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{
    Client, Method, StatusCode, Url,
    header::{
        CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue,
        LOCATION, REFERER,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

const MAX_REDIRECTS: usize = 5;
const DIRECT_TIMEOUT_SECONDS: u64 = 30 * 60;
const REMUX_TIMEOUT_SECONDS: u64 = 8 * 60 * 60;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const PROGRESS_BYTES: u64 = 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 16 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerRequest {
    mode: String,
    url: String,
    #[serde(default)]
    headers: Vec<WorkerHeader>,
    #[serde(default)]
    referer: Option<String>,
    partial_path: String,
    result_path: String,
    progress_path: String,
    #[serde(default)]
    stream_type: Option<String>,
    #[serde(default)]
    duration_seconds: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerHeader {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerProgress {
    out_time_seconds: Option<f64>,
    out_time_raw: Option<u64>,
    speed: Option<String>,
    output_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerResult {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_length: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_disposition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_progress: Option<WorkerProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr_tail: Option<String>,
}

#[tokio::main]
async fn main() {
    let result = run().await;
    if let Err(err) = result {
        eprintln!("{err:?}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let request_path = parse_request_path()?;
    let request_bytes = fs::read(&request_path)
        .await
        .with_context(|| format!("reading request file '{}'", request_path.display()))?;
    let request: WorkerRequest =
        serde_json::from_slice(&request_bytes).context("parsing request file")?;
    let result_path = PathBuf::from(&request.result_path);
    let result = match request.mode.as_str() {
        "direct_file" => materialize_direct_file(&request).await,
        "remux" => remux_stream(&request).await,
        other => Err(anyhow!("unsupported worker mode '{other}'")),
    };
    let result = match result {
        Ok(result) => result,
        Err(err) => WorkerResult {
            success: false,
            error: Some(err.to_string()),
            final_url: None,
            content_length: None,
            content_type: None,
            content_disposition: None,
            output_bytes: None,
            final_progress: None,
            stderr_tail: None,
        },
    };
    write_result(&result_path, &result).await?;
    if result.success {
        Ok(())
    } else {
        bail!("worker failed")
    }
}

fn parse_request_path() -> Result<PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--request" {
            let path = args
                .next()
                .ok_or_else(|| anyhow!("--request requires a path"))?;
            return Ok(PathBuf::from(path));
        }
    }
    bail!("usage: elixir-http-stream-worker --request <path>")
}

async fn materialize_direct_file(request: &WorkerRequest) -> Result<WorkerResult> {
    let url = validate_worker_url(&request.url)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(DIRECT_TIMEOUT_SECONDS))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("building HTTP client")?;
    let headers = header_map(request)?;
    let mut response = send_following_redirects(&client, Method::GET, url, headers, None).await?;
    if !response.status().is_success() || response.status() == StatusCode::NO_CONTENT {
        bail!("direct file returned {}", response.status());
    }
    let final_url = response.url().to_string();
    let content_length = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| response.content_length());
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let content_disposition = response
        .headers()
        .get(CONTENT_DISPOSITION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let partial_path = PathBuf::from(&request.partial_path);
    ensure_parent(&partial_path).await?;
    let mut file = fs::File::create(&partial_path)
        .await
        .with_context(|| format!("creating partial '{}'", partial_path.display()))?;
    let mut downloaded = 0_u64;
    let mut last_update = Instant::now();
    let mut last_downloaded = 0_u64;
    while let Some(chunk) = response.chunk().await.context("reading response body")? {
        file.write_all(&chunk)
            .await
            .with_context(|| format!("writing partial '{}'", partial_path.display()))?;
        downloaded = downloaded.saturating_add(chunk.len() as u64);
        if downloaded.saturating_sub(last_downloaded) >= PROGRESS_BYTES
            || last_update.elapsed() >= PROGRESS_INTERVAL
        {
            write_progress(
                Path::new(&request.progress_path),
                &json!({
                    "downloadedBytes": downloaded,
                    "totalBytes": content_length,
                }),
            )
            .await?;
            last_update = Instant::now();
            last_downloaded = downloaded;
        }
    }
    file.flush().await.context("flushing partial")?;
    Ok(WorkerResult {
        success: true,
        error: None,
        final_url: Some(final_url),
        content_length,
        content_type,
        content_disposition,
        output_bytes: Some(downloaded),
        final_progress: None,
        stderr_tail: None,
    })
}

async fn remux_stream(request: &WorkerRequest) -> Result<WorkerResult> {
    validate_worker_url(&request.url)?;
    match request.stream_type.as_deref() {
        Some("hls" | "dash") => {}
        Some(other) => bail!("unsupported remux stream type '{other}'"),
        None => bail!("remux request is missing streamType"),
    }
    let partial_path = PathBuf::from(&request.partial_path);
    ensure_parent(&partial_path).await?;
    let mut args = vec![
        "-hide_banner".to_string(),
        "-nostdin".to_string(),
        "-y".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
        "-reconnect".to_string(),
        "1".to_string(),
        "-reconnect_streamed".to_string(),
        "1".to_string(),
        "-reconnect_delay_max".to_string(),
        "5".to_string(),
    ];
    let header_block = ffmpeg_header_block(&request.headers);
    if !header_block.is_empty() {
        args.push("-headers".to_string());
        args.push(header_block);
    }
    if let Some(referer) = request.referer.as_deref() {
        args.push("-referer".to_string());
        args.push(referer.to_string());
    }
    args.extend([
        "-i".to_string(),
        request.url.clone(),
        "-map".to_string(),
        "0".to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-f".to_string(),
        "matroska".to_string(),
        "-progress".to_string(),
        "pipe:1".to_string(),
        partial_path.to_string_lossy().to_string(),
    ]);
    let mut child = Command::new("ffmpeg")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning ffmpeg")?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("ffmpeg stdout not captured"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("ffmpeg stderr not captured"))?;
    let stderr_task =
        tokio::spawn(async move { read_limited_text(stderr, STDERR_TAIL_BYTES).await });
    let mut lines = BufReader::new(stdout).lines();
    let started = Instant::now();
    let timeout = remux_timeout_duration(request.duration_seconds);
    let mut progress = WorkerProgress::default();
    let mut interval = tokio::time::interval(PROGRESS_INTERVAL);
    let mut exit_status = None;
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line.context("reading ffmpeg progress")? else {
                    break;
                };
                if observe_ffmpeg_line(&mut progress, &line) {
                    progress.output_bytes = fs::metadata(&partial_path).await.ok().map(|metadata| metadata.len()).or(progress.output_bytes);
                    write_progress(Path::new(&request.progress_path), &progress).await?;
                }
            }
            _ = interval.tick() => {
                if started.elapsed() > timeout {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    bail!("ffmpeg stream-copy timed out");
                }
                if let Some(status) = child.try_wait().context("checking ffmpeg status")? {
                    exit_status = Some(status);
                    break;
                }
                progress.output_bytes = fs::metadata(&partial_path).await.ok().map(|metadata| metadata.len()).or(progress.output_bytes);
                write_progress(Path::new(&request.progress_path), &progress).await?;
            }
        }
    }
    let status = match exit_status {
        Some(status) => status,
        None => child.wait().await.context("waiting for ffmpeg")?,
    };
    let stderr_tail = stderr_task
        .await
        .unwrap_or_else(|err| Ok(format!("failed to collect stderr: {err}")))?;
    if !status.success() {
        bail!(
            "ffmpeg stream-copy failed with code {:?}: {}",
            status.code(),
            stderr_tail.trim()
        );
    }
    let output_bytes = fs::metadata(&partial_path)
        .await
        .with_context(|| format!("reading output '{}'", partial_path.display()))?
        .len();
    if output_bytes == 0 {
        bail!("ffmpeg produced an empty output file");
    }
    progress.output_bytes = Some(output_bytes);
    Ok(WorkerResult {
        success: true,
        error: None,
        final_url: Some(request.url.clone()),
        content_length: Some(output_bytes),
        content_type: Some("video/x-matroska".to_string()),
        content_disposition: None,
        output_bytes: Some(output_bytes),
        final_progress: Some(progress),
        stderr_tail: (!stderr_tail.trim().is_empty()).then_some(stderr_tail),
    })
}

async fn send_following_redirects(
    client: &Client,
    method: Method,
    initial_url: Url,
    headers: HeaderMap,
    extra_header: Option<(&str, &str)>,
) -> Result<reqwest::Response> {
    let mut next_url = initial_url;
    for redirect_count in 0..=MAX_REDIRECTS {
        validate_worker_url(next_url.as_str())?;
        let mut request = client
            .request(method.clone(), next_url.clone())
            .headers(headers.clone());
        if let Some((name, value)) = extra_header {
            request = request.header(name, value);
        }
        let response = request.send().await.context("sending HTTP request")?;
        if !response.status().is_redirection() {
            return Ok(response);
        }
        let Some(location) = response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
        else {
            return Ok(response);
        };
        if redirect_count == MAX_REDIRECTS {
            bail!("too many redirects");
        }
        next_url = next_url
            .join(location)
            .context("parsing redirect location")?;
    }
    bail!("too many redirects")
}

fn validate_worker_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).context("parsing URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("URL scheme must be http or https");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("URL must not include credentials");
    }
    if url.host_str().map(str::trim).unwrap_or_default().is_empty() {
        bail!("URL host is required");
    }
    Ok(url)
}

fn header_map(request: &WorkerRequest) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for header in &request.headers {
        headers.insert(
            HeaderName::from_bytes(header.name.as_bytes())?,
            HeaderValue::from_str(&header.value)?,
        );
    }
    if let Some(referer) = request.referer.as_deref()
        && !headers.contains_key(REFERER)
    {
        headers.insert(REFERER, HeaderValue::from_str(referer)?);
    }
    Ok(headers)
}

fn ffmpeg_header_block(headers: &[WorkerHeader]) -> String {
    headers
        .iter()
        .map(|header| format!("{}: {}\r\n", header.name.trim(), header.value.trim()))
        .collect()
}

fn observe_ffmpeg_line(progress: &mut WorkerProgress, line: &str) -> bool {
    let Some((key, value)) = line.split_once('=') else {
        return false;
    };
    let key = key.trim();
    let value = value.trim();
    match key {
        "out_time_ms" | "out_time_us" => {
            if let Ok(raw) = value.parse::<u64>() {
                progress.out_time_raw = Some(raw);
                progress.out_time_seconds = Some(raw as f64 / 1_000_000.0);
                return true;
            }
        }
        "out_time" => {
            progress.out_time_seconds = parse_ffmpeg_out_time(value);
            return progress.out_time_seconds.is_some();
        }
        "speed" if !value.is_empty() && value != "N/A" => {
            progress.speed = Some(value.to_string());
            return true;
        }
        "total_size" => {
            progress.output_bytes = value.parse::<u64>().ok();
            return progress.output_bytes.is_some();
        }
        _ => {}
    }
    false
}

fn parse_ffmpeg_out_time(value: &str) -> Option<f64> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let hours = parts[0].parse::<f64>().ok()?;
    let minutes = parts[1].parse::<f64>().ok()?;
    let seconds = parts[2].parse::<f64>().ok()?;
    Some(hours * 3600.0 + minutes * 60.0 + seconds)
}

fn remux_timeout_duration(duration_seconds: Option<f64>) -> Duration {
    let Some(duration_seconds) = duration_seconds else {
        return Duration::from_secs(REMUX_TIMEOUT_SECONDS);
    };
    if duration_seconds <= 0.0 {
        return Duration::from_secs(REMUX_TIMEOUT_SECONDS);
    }
    Duration::from_secs((duration_seconds * 4.0).ceil() as u64).clamp(
        Duration::from_secs(60 * 60),
        Duration::from_secs(REMUX_TIMEOUT_SECONDS),
    )
}

async fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating parent '{}'", parent.display()))?;
    }
    Ok(())
}

async fn write_result(path: &Path, result: &WorkerResult) -> Result<()> {
    ensure_parent(path).await?;
    let bytes = serde_json::to_vec_pretty(result).context("serializing result")?;
    fs::write(path, bytes)
        .await
        .with_context(|| format!("writing result '{}'", path.display()))
}

async fn write_progress(path: &Path, value: &impl Serialize) -> Result<()> {
    ensure_parent(path).await?;
    let bytes = serde_json::to_vec(value).context("serializing progress")?;
    fs::write(path, bytes)
        .await
        .with_context(|| format!("writing progress '{}'", path.display()))
}

async fn read_limited_text<R>(mut reader: R, limit: usize) -> Result<String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = Vec::new();
    let mut chunk = vec![0u8; 4096];
    loop {
        let read = tokio::io::AsyncReadExt::read(&mut reader, &mut chunk)
            .await
            .context("reading limited text")?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > limit {
            let start = buffer.len().saturating_sub(limit);
            buffer = buffer[start..].to_vec();
        }
    }
    Ok(String::from_utf8_lossy(&buffer).to_string())
}
