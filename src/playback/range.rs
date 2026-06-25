use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{
            ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_RANGE,
            LAST_MODIFIED,
        },
    },
};
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::ReaderStream;

use crate::metrics::{DIRECT_STREAM_BYTES, DIRECT_STREAM_RANGE_REQUESTS};

const STREAM_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectFileBody {
    Empty,
    Full,
    Range { start: u64, length: u64 },
    Error(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct DirectFileResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: DirectFileBody,
}

#[derive(Debug, Clone)]
pub struct DirectReadMetricLabels {
    pub session_id: String,
    pub user_id: String,
    pub media_file_id: String,
    pub delivery: String,
}

#[derive(Debug, Clone)]
struct FileValidators {
    etag: Option<String>,
    last_modified: Option<String>,
    modified: Option<SystemTime>,
}

pub fn build_direct_file_response(
    method: &Method,
    request_headers: &HeaderMap,
    file_len: u64,
    modified: Option<SystemTime>,
    content_type: &str,
) -> DirectFileResponse {
    let validators = FileValidators::new(file_len, modified);
    let is_head = *method == Method::HEAD;
    let mut headers = base_headers(file_len, content_type, &validators);

    let range_header = request_headers
        .get(axum::http::header::RANGE)
        .and_then(|value| value.to_str().ok())
        .filter(|_| if_range_allows_range(request_headers, &validators));

    let Some(range_header) = range_header else {
        return DirectFileResponse {
            status: StatusCode::OK,
            headers,
            body: if is_head {
                DirectFileBody::Empty
            } else {
                DirectFileBody::Full
            },
        };
    };

    let ranges = match http_range::HttpRange::parse(range_header, file_len) {
        Ok(ranges) if !ranges.is_empty() => ranges,
        _ => {
            return range_error_response(
                file_len,
                &validators,
                "requested byte range is not satisfiable",
                is_head,
            );
        }
    };

    if ranges.len() != 1 {
        return range_error_response(
            file_len,
            &validators,
            "multiple byte ranges are not supported",
            is_head,
        );
    }

    let range = ranges[0];
    let start = range.start;
    let length = range.length;
    if length == 0 || start >= file_len || start.saturating_add(length) > file_len {
        return range_error_response(
            file_len,
            &validators,
            "requested byte range is not satisfiable",
            is_head,
        );
    }
    let end = start + length - 1;

    set_header(&mut headers, CONTENT_LENGTH, &length.to_string());
    set_header(
        &mut headers,
        CONTENT_RANGE,
        &format!("bytes {start}-{end}/{file_len}"),
    );

    DirectFileResponse {
        status: StatusCode::PARTIAL_CONTENT,
        headers,
        body: if is_head {
            DirectFileBody::Empty
        } else {
            DirectFileBody::Range { start, length }
        },
    }
}

pub fn content_type_for(path: &str, container: Option<&str>) -> String {
    let ext = container
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::path::Path::new(path)
                .extension()
                .and_then(|e| e.to_str())
        });
    match ext.map(|value| value.to_ascii_lowercase()) {
        Some(ext) if ext == "mp4" => "video/mp4".to_string(),
        Some(ext) if ext == "mkv" => "video/x-matroska".to_string(),
        Some(ext) if ext == "mov" => "video/quicktime".to_string(),
        Some(ext) if ext == "avi" => "video/x-msvideo".to_string(),
        Some(ext) if ext == "webm" => "video/webm".to_string(),
        _ => "application/octet-stream".to_string(),
    }
}

pub fn direct_file_body<R>(reader: R, metrics: DirectReadMetricLabels) -> Body
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let counted = CountingReader {
        inner: reader,
        metrics,
    };
    Body::from_stream(ReaderStream::with_capacity(counted, STREAM_BUFFER_BYTES))
}

pub fn record_direct_stream_range_status(
    response: &DirectFileResponse,
    method: &Method,
    delivery: &str,
) {
    DIRECT_STREAM_RANGE_REQUESTS
        .with_label_values(&[
            direct_file_range_status(response),
            delivery,
            method.as_str(),
        ])
        .inc();
}

fn direct_file_range_status(response: &DirectFileResponse) -> &'static str {
    match response.status {
        StatusCode::PARTIAL_CONTENT => "partial",
        StatusCode::RANGE_NOT_SATISFIABLE => "not_satisfiable",
        StatusCode::OK => "full",
        _ => "other",
    }
}

fn range_error_response(
    file_len: u64,
    validators: &FileValidators,
    message: &'static str,
    is_head: bool,
) -> DirectFileResponse {
    let body = message.as_bytes().to_vec();
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    set_header(&mut headers, CONTENT_LENGTH, &body.len().to_string());
    set_header(&mut headers, CONTENT_RANGE, &format!("bytes */{file_len}"));
    insert_validators(&mut headers, validators);

    DirectFileResponse {
        status: StatusCode::RANGE_NOT_SATISFIABLE,
        headers,
        body: if is_head {
            DirectFileBody::Empty
        } else {
            DirectFileBody::Error(body)
        },
    }
}

fn base_headers(file_len: u64, content_type: &str, validators: &FileValidators) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    set_header(&mut headers, CONTENT_LENGTH, &file_len.to_string());
    set_header(&mut headers, CONTENT_TYPE, content_type);
    insert_validators(&mut headers, validators);
    headers
}

fn insert_validators(headers: &mut HeaderMap, validators: &FileValidators) {
    if let Some(etag) = validators.etag.as_deref() {
        set_header(headers, ETAG, etag);
    }
    if let Some(last_modified) = validators.last_modified.as_deref() {
        set_header(headers, LAST_MODIFIED, last_modified);
    }
}

fn if_range_allows_range(headers: &HeaderMap, validators: &FileValidators) -> bool {
    let Some(raw) = headers.get(IF_RANGE).and_then(|value| value.to_str().ok()) else {
        return true;
    };
    let raw = raw.trim();
    if raw.is_empty() || raw.starts_with("W/") {
        return false;
    }
    if validators.etag.as_deref() == Some(raw) {
        return true;
    }
    let Some(modified) = validators.modified else {
        return false;
    };
    let Ok(if_range_date) = httpdate::parse_http_date(raw) else {
        return false;
    };
    truncate_to_seconds(modified) <= if_range_date
}

fn truncate_to_seconds(time: SystemTime) -> SystemTime {
    let Ok(duration) = time.duration_since(UNIX_EPOCH) else {
        return time;
    };
    UNIX_EPOCH + std::time::Duration::from_secs(duration.as_secs())
}

fn set_header(headers: &mut HeaderMap, name: axum::http::header::HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

impl FileValidators {
    fn new(file_len: u64, modified: Option<SystemTime>) -> Self {
        let etag = modified
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| {
                format!(
                    "\"{:x}-{:x}-{:x}\"",
                    file_len,
                    duration.as_secs(),
                    duration.subsec_nanos()
                )
            });
        let last_modified = modified.map(httpdate::fmt_http_date);
        Self {
            etag,
            last_modified,
            modified,
        }
    }
}

struct CountingReader<R> {
    inner: R,
    metrics: DirectReadMetricLabels,
}

impl<R> AsyncRead for CountingReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            let read = buf.filled().len().saturating_sub(before);
            if read > 0 {
                DIRECT_STREAM_BYTES
                    .with_label_values(&[
                        &self.metrics.session_id,
                        &self.metrics.user_id,
                        &self.metrics.media_file_id,
                        &self.metrics.delivery,
                    ])
                    .inc_by(read as u64);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::RANGE;

    fn modified() -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)
    }

    fn build(method: Method, range: Option<&str>, if_range: Option<&str>) -> DirectFileResponse {
        let mut headers = HeaderMap::new();
        if let Some(range) = range {
            headers.insert(RANGE, HeaderValue::from_str(range).unwrap());
        }
        if let Some(if_range) = if_range {
            headers.insert(IF_RANGE, HeaderValue::from_str(if_range).unwrap());
        }
        build_direct_file_response(&method, &headers, 10, Some(modified()), "video/x-matroska")
    }

    fn header<'a>(
        response: &'a DirectFileResponse,
        name: axum::http::header::HeaderName,
    ) -> &'a str {
        response
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap()
    }

    #[test]
    fn full_get_response_has_direct_play_headers_and_validators() {
        let response = build(Method::GET, None, None);

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body, DirectFileBody::Full);
        assert_eq!(header(&response, ACCEPT_RANGES), "bytes");
        assert_eq!(header(&response, CONTENT_LENGTH), "10");
        assert_eq!(header(&response, CONTENT_TYPE), "video/x-matroska");
        assert!(response.headers.contains_key(ETAG));
        assert!(response.headers.contains_key(LAST_MODIFIED));
    }

    #[test]
    fn head_response_preserves_content_headers_without_body() {
        let response = build(Method::HEAD, None, None);

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body, DirectFileBody::Empty);
        assert_eq!(header(&response, CONTENT_LENGTH), "10");
    }

    #[test]
    fn closed_open_ended_and_suffix_ranges_are_supported() {
        let closed = build(Method::GET, Some("bytes=2-5"), None);
        assert_eq!(closed.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            closed.body,
            DirectFileBody::Range {
                start: 2,
                length: 4
            }
        );
        assert_eq!(header(&closed, CONTENT_RANGE), "bytes 2-5/10");
        assert_eq!(header(&closed, CONTENT_LENGTH), "4");

        let open = build(Method::GET, Some("bytes=6-"), None);
        assert_eq!(
            open.body,
            DirectFileBody::Range {
                start: 6,
                length: 4
            }
        );
        assert_eq!(header(&open, CONTENT_RANGE), "bytes 6-9/10");

        let suffix = build(Method::GET, Some("bytes=-4"), None);
        assert_eq!(
            suffix.body,
            DirectFileBody::Range {
                start: 6,
                length: 4
            }
        );
        assert_eq!(header(&suffix, CONTENT_RANGE), "bytes 6-9/10");
    }

    #[test]
    fn invalid_range_returns_416_with_unsatisfied_content_range() {
        let response = build(Method::GET, Some("bytes=20-30"), None);

        assert_eq!(response.status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(header(&response, CONTENT_RANGE), "bytes */10");
        assert_eq!(
            response.body,
            DirectFileBody::Error(b"requested byte range is not satisfiable".to_vec())
        );

        let head = build(Method::HEAD, Some("bytes=20-30"), None);
        assert_eq!(head.status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(header(&head, CONTENT_RANGE), "bytes */10");
        assert_eq!(head.body, DirectFileBody::Empty);
    }

    #[test]
    fn multi_range_is_rejected_until_multipart_support_exists() {
        let response = build(Method::GET, Some("bytes=0-1,3-4"), None);

        assert_eq!(response.status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(header(&response, CONTENT_RANGE), "bytes */10");
        assert_eq!(
            response.body,
            DirectFileBody::Error(b"multiple byte ranges are not supported".to_vec())
        );
    }

    #[test]
    fn if_range_matching_etag_allows_range_and_mismatch_returns_full_body() {
        let full = build(Method::GET, None, None);
        let etag = header(&full, ETAG).to_string();

        let matched = build(Method::GET, Some("bytes=0-3"), Some(&etag));
        assert_eq!(matched.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            matched.body,
            DirectFileBody::Range {
                start: 0,
                length: 4
            }
        );

        let mismatched = build(Method::GET, Some("bytes=0-3"), Some("\"other\""));
        assert_eq!(mismatched.status, StatusCode::OK);
        assert_eq!(mismatched.body, DirectFileBody::Full);
        assert!(!mismatched.headers.contains_key(CONTENT_RANGE));
    }

    #[test]
    fn if_range_matching_last_modified_allows_range() {
        let full = build(Method::GET, None, None);
        let last_modified = header(&full, LAST_MODIFIED).to_string();

        let response = build(Method::GET, Some("bytes=1-2"), Some(&last_modified));
        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.body,
            DirectFileBody::Range {
                start: 1,
                length: 2
            }
        );
    }

    #[test]
    fn range_metric_status_labels_are_low_cardinality() {
        assert_eq!(
            direct_file_range_status(&build(Method::GET, None, None)),
            "full"
        );
        assert_eq!(
            direct_file_range_status(&build(Method::GET, Some("bytes=1-2"), None)),
            "partial"
        );
        assert_eq!(
            direct_file_range_status(&build(Method::GET, Some("bytes=20-30"), None)),
            "not_satisfiable"
        );
    }
}
