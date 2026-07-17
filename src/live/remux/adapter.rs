use std::{collections::HashMap, fmt, sync::Arc};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, Response, StatusCode, header},
    routing::get,
};
use futures_util::stream;
use quick_xml::{
    Reader, Writer,
    events::{BytesStart, BytesText, Event},
};
use reqwest::Url;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::live::{
    relay::{LiveRelayService, LiveRemuxSource},
    session::{SessionProtocol, SessionRecord},
};

const MAX_DASH_MANIFEST_BYTES: u64 = 2 * 1_024 * 1_024;
const MAX_DASH_MAPPINGS: usize = 4_096;
const MAX_DASH_URI_BYTES: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemuxAdapterError {
    Bind,
    InvalidProtocol,
    InvalidRequest,
    Upstream,
    Manifest,
    MappingLimit,
}

pub struct LiveRemuxAdapter {
    input_url: String,
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl fmt::Debug for LiveRemuxAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRemuxAdapter")
            .field("input_url", &self.input_url)
            .finish_non_exhaustive()
    }
}

impl LiveRemuxAdapter {
    pub async fn start(
        relay: Arc<LiveRelayService>,
        session: SessionRecord,
        source: Arc<LiveRemuxSource>,
        parent_cancellation: &CancellationToken,
    ) -> Result<Self, RemuxAdapterError> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| RemuxAdapterError::Bind)?;
        let address = listener.local_addr().map_err(|_| RemuxAdapterError::Bind)?;
        let origin = format!("http://{address}");
        let input_path = match source.protocol() {
            SessionProtocol::MpegTs => "/input",
            SessionProtocol::Dash => "/input/manifest.mpd",
            _ => return Err(RemuxAdapterError::InvalidProtocol),
        };
        let shutdown = parent_cancellation.child_token();
        let state = Arc::new(AdapterState {
            relay,
            session,
            source,
            origin: origin.clone(),
            dash: Mutex::new(DashState::default()),
            cancellation: shutdown.clone(),
        });
        let router = Router::new()
            .route("/*path", get(adapter_get))
            .with_state(state);
        let shutdown_observer = shutdown.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_observer.cancelled_owned())
                .await
            {
                tracing::warn!(error = %error, "Live remux loopback adapter stopped unexpectedly");
            }
        });
        Ok(Self {
            input_url: format!("{origin}{input_path}"),
            shutdown,
            task: Some(task),
        })
    }

    pub fn input_url(&self) -> &str {
        &self.input_url
    }

    pub async fn stop(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LiveRemuxAdapter {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct AdapterState {
    relay: Arc<LiveRelayService>,
    session: SessionRecord,
    source: Arc<LiveRemuxSource>,
    origin: String,
    dash: Mutex<DashState>,
    cancellation: CancellationToken,
}

impl fmt::Debug for AdapterState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdapterState")
            .field("session_id", &self.session.id)
            .field("protocol", &self.session.protocol)
            .field("source", &"[REDACTED]")
            .finish()
    }
}

#[derive(Default)]
struct DashState {
    root_base: Option<Url>,
    mappings: HashMap<String, Url>,
    reverse_mappings: HashMap<String, String>,
}

async fn adapter_get(
    State(state): State<Arc<AdapterState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response<Body> {
    match adapter_get_inner(state.clone(), uri.path(), uri.query(), &headers).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(
                session_id = %state.session.id,
                error = ?error,
                "Live remux loopback adapter request failed"
            );
            static_response(match error {
                RemuxAdapterError::InvalidRequest | RemuxAdapterError::Manifest => {
                    StatusCode::BAD_REQUEST
                }
                RemuxAdapterError::MappingLimit => StatusCode::INSUFFICIENT_STORAGE,
                _ => StatusCode::BAD_GATEWAY,
            })
        }
    }
}

async fn adapter_get_inner(
    state: Arc<AdapterState>,
    path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> Result<Response<Body>, RemuxAdapterError> {
    let range = one_header(headers, header::RANGE)?;
    let is_dash_root =
        state.session.protocol == SessionProtocol::Dash && path == "/input/manifest.mpd";
    let upstream_range = adapter_upstream_range(is_dash_root, range.as_deref())?;
    let target = resolve_target(&state, path, query).await?;
    let cancellation = state.cancellation.child_token();
    let response = state
        .relay
        .fetch_remux_source(
            &state.session,
            &state.source,
            &target,
            upstream_range,
            cancellation,
        )
        .await
        .map_err(|_| RemuxAdapterError::Upstream)?;
    if !matches!(
        response.status(),
        StatusCode::OK | StatusCode::PARTIAL_CONTENT
    ) {
        return Err(RemuxAdapterError::Upstream);
    }
    if is_dash_root {
        if response.status() != StatusCode::OK {
            tracing::warn!(stage = "status", "Live DASH adapter manifest rejection");
            return Err(RemuxAdapterError::Manifest);
        }
        let final_url = response.final_url().clone();
        let body = response
            .collect_bounded(MAX_DASH_MANIFEST_BYTES)
            .await
            .map_err(|_| {
                tracing::warn!(stage = "body", "Live DASH adapter manifest rejection");
                RemuxAdapterError::Manifest
            })?;
        let mut dash = state.dash.lock().await;
        dash.root_base = Some(final_url.join("./").map_err(|_| {
            tracing::warn!(stage = "base", "Live DASH adapter manifest rejection");
            RemuxAdapterError::Manifest
        })?);
        let rewritten = rewrite_dash_manifest(body.as_bytes(), &final_url, &state.origin, &mut dash)
            .map_err(|error| {
                tracing::warn!(stage = "rewrite", error = ?error, "Live DASH adapter manifest rejection");
                error
            })?;
        let mut result = Response::new(Body::from(rewritten.clone()));
        result.headers_mut().insert(
            header::CONTENT_TYPE,
            "application/dash+xml"
                .parse()
                .expect("static DASH content type"),
        );
        result.headers_mut().insert(
            header::CONTENT_LENGTH,
            rewritten
                .len()
                .to_string()
                .parse()
                .map_err(|_| RemuxAdapterError::Manifest)?,
        );
        return Ok(result);
    }
    Ok(stream_response(response))
}

fn adapter_upstream_range(
    is_dash_root: bool,
    range: Option<&str>,
) -> Result<Option<&str>, RemuxAdapterError> {
    if !is_dash_root {
        return Ok(range);
    }
    match range {
        None | Some("bytes=0-") => Ok(None),
        Some(_) => Err(RemuxAdapterError::InvalidRequest),
    }
}

async fn resolve_target(
    state: &AdapterState,
    path: &str,
    query: Option<&str>,
) -> Result<Url, RemuxAdapterError> {
    if state.session.protocol == SessionProtocol::MpegTs {
        return (path == "/input" && query.is_none())
            .then(|| state.source.root_url().clone())
            .ok_or(RemuxAdapterError::InvalidRequest);
    }
    if state.session.protocol != SessionProtocol::Dash {
        return Err(RemuxAdapterError::InvalidProtocol);
    }
    if path == "/input/manifest.mpd" {
        if query.is_some() {
            return Err(RemuxAdapterError::InvalidRequest);
        }
        return Ok(state.source.root_url().clone());
    }
    let dash = state.dash.lock().await;
    let (base, tail) = if let Some(tail) = path.strip_prefix("/input/") {
        (
            dash.root_base
                .as_ref()
                .ok_or(RemuxAdapterError::InvalidRequest)?,
            tail,
        )
    } else if let Some(mapped) = path.strip_prefix("/m/") {
        let (id, tail) = mapped
            .split_once('/')
            .ok_or(RemuxAdapterError::InvalidRequest)?;
        (
            dash.mappings
                .get(id)
                .ok_or(RemuxAdapterError::InvalidRequest)?,
            tail,
        )
    } else {
        return Err(RemuxAdapterError::InvalidRequest);
    };
    if tail.is_empty()
        || tail.len() > MAX_DASH_URI_BYTES
        || tail.chars().any(char::is_control)
        || tail.split('/').any(|component| component == "..")
    {
        return Err(RemuxAdapterError::InvalidRequest);
    }
    let mut target = base
        .join(tail)
        .map_err(|_| RemuxAdapterError::InvalidRequest)?;
    if let Some(query) = query {
        if query.len() > MAX_DASH_URI_BYTES || query.chars().any(char::is_control) {
            return Err(RemuxAdapterError::InvalidRequest);
        }
        target.set_query(Some(query));
    }
    Ok(target)
}

fn rewrite_dash_manifest(
    body: &[u8],
    final_url: &Url,
    origin: &str,
    state: &mut DashState,
) -> Result<Vec<u8>, RemuxAdapterError> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(false);
    let mut writer = Writer::new(Vec::with_capacity(body.len().saturating_add(512)));
    let mut buffer = Vec::new();
    let mut depth = 0usize;
    let mut bases = vec![
        final_url
            .join("./")
            .map_err(|_| RemuxAdapterError::Manifest)?,
    ];
    let mut base_url_depth = None;
    loop {
        match reader.read_event_into(&mut buffer).map_err(|_| {
            tracing::warn!(stage = "xml_read", "Live DASH manifest rewrite failed");
            RemuxAdapterError::Manifest
        })? {
            Event::Start(event) => {
                let parent_base = bases
                    .get(depth)
                    .cloned()
                    .ok_or(RemuxAdapterError::Manifest)?;
                let is_base_url = event.local_name().as_ref() == b"BaseURL";
                let rewritten = rewrite_start(event, &parent_base, origin, state).map_err(|error| {
                    tracing::warn!(stage = "xml_start", error = ?error, "Live DASH manifest rewrite failed");
                    error
                })?;
                writer
                    .write_event(Event::Start(rewritten))
                    .map_err(|_| RemuxAdapterError::Manifest)?;
                depth = depth.checked_add(1).ok_or(RemuxAdapterError::Manifest)?;
                if bases.len() <= depth {
                    bases.push(parent_base);
                } else {
                    bases[depth] = parent_base;
                }
                if is_base_url {
                    base_url_depth = Some(depth);
                }
            }
            Event::Empty(event) => {
                let base = bases.get(depth).ok_or(RemuxAdapterError::Manifest)?;
                let rewritten = rewrite_start(event, base, origin, state).map_err(|error| {
                    tracing::warn!(stage = "xml_empty", error = ?error, "Live DASH manifest rewrite failed");
                    error
                })?;
                writer
                    .write_event(Event::Empty(rewritten))
                    .map_err(|_| RemuxAdapterError::Manifest)?;
            }
            Event::Text(event) if base_url_depth == Some(depth) => {
                let decoded = event.decode().map_err(|_| RemuxAdapterError::Manifest)?;
                let value = quick_xml::escape::unescape(&decoded)
                    .map_err(|_| RemuxAdapterError::Manifest)?;
                let parent = bases
                    .get(depth.saturating_sub(1))
                    .cloned()
                    .ok_or(RemuxAdapterError::Manifest)?;
                let target = parent
                    .join(value.trim())
                    .map_err(|_| RemuxAdapterError::Manifest)?;
                let local = map_url(&target, origin, state)?;
                if let Some(base) = bases.get_mut(depth.saturating_sub(1)) {
                    *base = target;
                }
                writer
                    .write_event(Event::Text(BytesText::new(&local)))
                    .map_err(|_| RemuxAdapterError::Manifest)?;
            }
            Event::End(event) => {
                writer
                    .write_event(Event::End(event.into_owned()))
                    .map_err(|_| RemuxAdapterError::Manifest)?;
                if base_url_depth == Some(depth) {
                    base_url_depth = None;
                }
                depth = depth.checked_sub(1).ok_or(RemuxAdapterError::Manifest)?;
                bases.truncate(depth.saturating_add(1));
            }
            Event::DocType(_) => return Err(RemuxAdapterError::Manifest),
            Event::Eof => break,
            event => writer
                .write_event(event.into_owned())
                .map_err(|_| RemuxAdapterError::Manifest)?,
        }
        if writer.get_ref().len() > MAX_DASH_MANIFEST_BYTES as usize * 2 {
            return Err(RemuxAdapterError::Manifest);
        }
        buffer.clear();
    }
    Ok(writer.into_inner())
}

fn rewrite_start(
    event: BytesStart<'_>,
    base: &Url,
    origin: &str,
    state: &mut DashState,
) -> Result<BytesStart<'static>, RemuxAdapterError> {
    let mut output = event.into_owned();
    let attributes = output
        .attributes()
        .with_checks(true)
        .map(|attribute| {
            let attribute = attribute.map_err(|_| RemuxAdapterError::Manifest)?;
            let key = String::from_utf8(attribute.key.as_ref().to_vec())
                .map_err(|_| RemuxAdapterError::Manifest)?;
            let mut value = attribute
                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                .map_err(|_| RemuxAdapterError::Manifest)?
                .into_owned();
            let local_name = key.rsplit(':').next().unwrap_or(&key);
            if matches!(
                local_name,
                "media" | "initialization" | "sourceURL" | "href" | "index"
            ) && (value.starts_with('/') || Url::parse(&value).is_ok())
            {
                let target = base.join(&value).map_err(|_| RemuxAdapterError::Manifest)?;
                value = map_url(&target, origin, state)?;
            }
            Ok((key, value))
        })
        .collect::<Result<Vec<_>, RemuxAdapterError>>()?;
    output.clear_attributes();
    for (key, value) in &attributes {
        output.push_attribute((key.as_str(), value.as_str()));
    }
    Ok(output)
}

fn map_url(target: &Url, origin: &str, state: &mut DashState) -> Result<String, RemuxAdapterError> {
    if !matches!(target.scheme(), "http" | "https") || target.fragment().is_some() {
        return Err(RemuxAdapterError::Manifest);
    }
    let path = target.path();
    let (directory, tail) = match path.rsplit_once('/') {
        Some((directory, tail)) if !tail.is_empty() => (format!("{directory}/"), tail),
        _ => (path.to_string(), ""),
    };
    let mut base = target.clone();
    base.set_path(&directory);
    base.set_query(None);
    base.set_fragment(None);
    let base_key = base.as_str().to_string();
    let id = if let Some(id) = state.reverse_mappings.get(&base_key) {
        id.clone()
    } else {
        if state.mappings.len() >= MAX_DASH_MAPPINGS {
            return Err(RemuxAdapterError::MappingLimit);
        }
        let id = Uuid::new_v4().simple().to_string();
        state.mappings.insert(id.clone(), base);
        state.reverse_mappings.insert(base_key, id.clone());
        id
    };
    let mut local = format!("{origin}/m/{id}/");
    local.push_str(tail);
    if let Some(query) = target.query() {
        local.push('?');
        local.push_str(query);
    }
    Ok(local)
}

fn one_header(
    headers: &HeaderMap,
    name: header::HeaderName,
) -> Result<Option<String>, RemuxAdapterError> {
    let values = headers.get_all(name).iter().collect::<Vec<_>>();
    if values.len() > 1 {
        return Err(RemuxAdapterError::InvalidRequest);
    }
    values
        .first()
        .map(|value| {
            value
                .to_str()
                .map(str::to_string)
                .map_err(|_| RemuxAdapterError::InvalidRequest)
        })
        .transpose()
}

fn stream_response(upstream: crate::live::upstream::UpstreamResponse) -> Response<Body> {
    let status = upstream.status();
    let headers = filtered_headers(upstream.headers());
    let stream = stream::unfold(Some(upstream), |state| async move {
        let mut upstream = state?;
        match upstream.next_chunk().await {
            Ok(Some(chunk)) => Some((
                Ok::<Bytes, std::io::Error>(Bytes::from(chunk.into_bytes())),
                Some(upstream),
            )),
            Ok(None) => None,
            Err(_) => Some((
                Err(std::io::Error::other("Live remux adapter upstream failed")),
                None,
            )),
        }
    });
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

fn filtered_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::ACCEPT_RANGES,
        header::ETAG,
        header::LAST_MODIFIED,
    ] {
        if let Some(value) = upstream.get(&name) {
            headers.insert(name, value.clone());
        }
    }
    headers
}

fn static_response(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m10_dash_rewrite_uses_opaque_loopback_mappings_and_rejects_doctype() {
        let final_url = Url::parse("https://origin.example/events/main/manifest.mpd?sig=secret")
            .expect("fixture URL");
        let mut state = DashState::default();
        let body = br#"<?xml version="1.0"?><MPD><Period><BaseURL>https://cdn.example/media/</BaseURL><AdaptationSet><Representation><SegmentTemplate initialization="/init-$RepresentationID$.mp4" media="chunk-$Number$.m4s"/></Representation></AdaptationSet></Period></MPD>"#;
        let rewritten =
            rewrite_dash_manifest(body, &final_url, "http://127.0.0.1:45321", &mut state)
                .expect("safe DASH rewrite");
        let rewritten = String::from_utf8(rewritten).expect("UTF-8 DASH");
        assert!(rewritten.contains("http://127.0.0.1:45321/m/"));
        assert!(!rewritten.contains("origin.example"));
        assert!(!rewritten.contains("cdn.example"));
        assert!(!rewritten.contains("sig=secret"));
        assert!(!state.mappings.is_empty());

        let xxe = br#"<?xml version="1.0"?><!DOCTYPE MPD [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><MPD>&xxe;</MPD>"#;
        assert_eq!(
            rewrite_dash_manifest(
                xxe,
                &final_url,
                "http://127.0.0.1:45321",
                &mut DashState::default(),
            ),
            Err(RemuxAdapterError::Manifest)
        );
    }

    #[test]
    fn m10_dash_manifest_accepts_only_full_document_range_requests() {
        assert_eq!(adapter_upstream_range(true, None), Ok(None));
        assert_eq!(adapter_upstream_range(true, Some("bytes=0-")), Ok(None));
        assert_eq!(
            adapter_upstream_range(true, Some("bytes=1-")),
            Err(RemuxAdapterError::InvalidRequest)
        );
        assert_eq!(
            adapter_upstream_range(true, Some("bytes=0-1023")),
            Err(RemuxAdapterError::InvalidRequest)
        );
        assert_eq!(
            adapter_upstream_range(false, Some("bytes=4-8")),
            Ok(Some("bytes=4-8"))
        );
    }
}
