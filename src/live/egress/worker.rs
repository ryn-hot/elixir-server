use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode},
    routing::post,
};
use chrono::{DateTime, Utc};
use futures_util::stream;
use reqwest::{Client, Url, redirect::Policy};
use tokio::{net::TcpListener, sync::Mutex, time};

use crate::live::config::is_public_egress_ip;

use super::control::{
    ControlKeys, ControlProtocolError, ControlSecretDocument, FetchControlRequest,
    ReadinessControlResponse, ResolveControlRequest, ResolveControlResponse,
    bounded_connect_timeout, open_control_request, readiness_ip_matches, response_signature,
    verify_request_signature,
};

const MAX_CONTROL_BODY_BYTES: usize = 65_536;
const MAX_SECRET_FILE_BYTES: u64 = 16_384;
const MAX_REPLAY_IDS: usize = 4_096;
const REPLAY_RETENTION_SECONDS: i64 = 90;
const AUTH_HEADER: &str = "x-elixir-live-egress-auth";
const RESPONSE_SIGNATURE_HEADER: &str = "x-elixir-live-egress-response";
const RESPONSE_REQUEST_HEADER: &str = "x-elixir-live-egress-request";
const RESPONSE_PEER_HEADER: &str = "x-elixir-live-egress-peer";
const RESPONSE_FENCE_HEADER: &str = "x-elixir-live-egress-fence";
const RESPONSE_KIND_HEADER: &str = "x-elixir-live-egress-kind";

struct WorkerState {
    secret: ControlSecretDocument,
    keys: ControlKeys,
    replay: Mutex<HashMap<uuid::Uuid, DateTime<Utc>>>,
}

impl std::fmt::Debug for WorkerState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerState")
            .field("session_id", &self.secret.session_id)
            .field("control_fencing_token", &self.secret.control_fencing_token)
            .field("expires_at", &self.secret.expires_at)
            .finish_non_exhaustive()
    }
}

pub async fn run_live_egress_worker_from_environment() -> Result<()> {
    let secret_path = std::env::var("ELIXIR_LIVE_EGRESS_SECRET_FILE")
        .context("ELIXIR_LIVE_EGRESS_SECRET_FILE is required")?;
    let control_port = std::env::var("ELIXIR_LIVE_EGRESS_CONTROL_PORT")
        .context("ELIXIR_LIVE_EGRESS_CONTROL_PORT is required")?
        .parse::<u16>()
        .context("invalid protected-egress control port")?;
    if control_port < 1_024 {
        bail!("protected-egress control port is outside its safe range");
    }
    let secret = read_secret(Path::new(&secret_path)).await?;
    let keys = secret
        .keys()
        .map_err(|_| anyhow::anyhow!("protected-egress control secret is invalid"))?;
    let state = Arc::new(WorkerState {
        secret,
        keys,
        replay: Mutex::new(HashMap::new()),
    });
    let app = Router::new()
        .route("/v1/resolve", post(resolve))
        .route("/v1/fetch", post(fetch))
        .route("/v1/readiness", post(readiness))
        .layer(DefaultBodyLimit::max(MAX_CONTROL_BODY_BYTES))
        .with_state(state);
    let listener = TcpListener::bind(("0.0.0.0", control_port))
        .await
        .context("failed to bind protected-egress control listener")?;
    axum::serve(listener, app)
        .await
        .context("protected-egress control listener failed")
}

async fn read_secret(path: &Path) -> Result<ControlSecretDocument> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .context("protected-egress control secret is unavailable")?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > MAX_SECRET_FILE_BYTES
    {
        bail!("protected-egress control secret has an invalid size");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.permissions().mode() & 0o777 != 0o600
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
        {
            bail!("protected-egress control secret has unsafe ownership or permissions");
        }
    }
    let bytes = tokio::fs::read(path)
        .await
        .context("protected-egress control secret could not be read")?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_SECRET_FILE_BYTES {
        bail!("protected-egress control secret changed while being read");
    }
    serde_json::from_slice(&bytes).context("protected-egress control secret is invalid")
}

async fn resolve(
    State(state): State<Arc<WorkerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let result = async {
        let (request_id, request): (uuid::Uuid, ResolveControlRequest) =
            authorize(&state, &headers, &body).await?;
        validate_dns_name(&request.host)?;
        if request.port == 0 {
            return Err(ControlProtocolError::Invalid);
        }
        let values = time::timeout(
            Duration::from_secs(8),
            tokio::net::lookup_host((request.host.as_str(), request.port)),
        )
        .await
        .map_err(|_| ControlProtocolError::Expired)?
        .map_err(|_| ControlProtocolError::Invalid)?;
        let mut seen = HashSet::new();
        let addresses = values
            .map(|value| value.ip())
            .filter(|address| seen.insert(*address))
            .collect::<Vec<_>>();
        if addresses.is_empty()
            || addresses.len() > 16
            || addresses
                .iter()
                .any(|address| !is_public_egress_ip(*address))
        {
            return Err(ControlProtocolError::Invalid);
        }
        let response = serde_json::to_vec(&ResolveControlResponse { addresses })
            .map_err(|_| ControlProtocolError::Invalid)?;
        signed_json_response(&state, request_id, "resolve", StatusCode::OK, "", response)
    }
    .await;
    result.unwrap_or_else(control_error_response)
}

async fn readiness(
    State(state): State<Arc<WorkerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let result = async {
        let (request_id, _): (uuid::Uuid, serde_json::Value) =
            authorize(&state, &headers, &body).await?;
        let dns = time::timeout(
            Duration::from_secs(8),
            tokio::net::lookup_host((state.secret.readiness.dns_probe_host.as_str(), 443)),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .is_some_and(|mut values| values.any(|value| is_public_egress_ip(value.ip())));
        let observed = if dns {
            external_ip(&state.secret.readiness.external_ip_url).await
        } else {
            None
        };
        let expected = readiness_ip_matches(&state.secret.readiness.expected_egress_ips, observed);
        let response = ReadinessControlResponse {
            route: observed.is_some(),
            dns,
            external_ip: expected,
            // The server independently verifies that this worker shares only the
            // selected gateway namespace before accepting this assertion.
            kill_switch: true,
            health: state.secret.expires_at > Utc::now(),
            observed_egress_ip: observed,
        };
        let response = serde_json::to_vec(&response).map_err(|_| ControlProtocolError::Invalid)?;
        signed_json_response(
            &state,
            request_id,
            "readiness",
            StatusCode::OK,
            "",
            response,
        )
    }
    .await;
    result.unwrap_or_else(control_error_response)
}

async fn fetch(
    State(state): State<Arc<WorkerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let result = async {
        let (request_id, request): (uuid::Uuid, FetchControlRequest) =
            authorize(&state, &headers, &body).await?;
        let timeout = bounded_connect_timeout(request.connect_timeout_millis)?;
        let method = match request.method.as_str() {
            "GET" => Method::GET,
            "HEAD" => Method::HEAD,
            _ => return Err(ControlProtocolError::Invalid),
        };
        let url = validate_fetch_url(&request.url)?;
        let port = url
            .port_or_known_default()
            .ok_or(ControlProtocolError::Invalid)?;
        let addresses = parse_socket_addresses(&request.socket_addresses, port)?;
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(timeout)
            .pool_max_idle_per_host(0)
            .tcp_nodelay(true)
            .referer(false);
        let host = url.host_str().ok_or(ControlProtocolError::Invalid)?;
        if host.parse::<IpAddr>().is_err() {
            builder = builder.resolve_to_addrs(host, &addresses);
        } else if !addresses
            .iter()
            .any(|address| address.ip().to_string() == host)
        {
            return Err(ControlProtocolError::Invalid);
        }
        let client = builder.build().map_err(|_| ControlProtocolError::Invalid)?;
        let request_headers = validate_request_headers(request.headers)?;
        let upstream = time::timeout(
            Duration::from_secs(35),
            client.request(method, url).headers(request_headers).send(),
        )
        .await
        .map_err(|_| ControlProtocolError::Expired)?
        .map_err(|_| ControlProtocolError::Invalid)?;
        let peer = upstream
            .remote_addr()
            .ok_or(ControlProtocolError::Invalid)?;
        if !addresses.contains(&peer) {
            return Err(ControlProtocolError::Invalid);
        }
        proxy_upstream_response(&state, request_id, peer, upstream)
    }
    .await;
    result.unwrap_or_else(control_error_response)
}

async fn authorize<T: serde::de::DeserializeOwned>(
    state: &WorkerState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(uuid::Uuid, T), ControlProtocolError> {
    if body.is_empty()
        || body.len() > MAX_CONTROL_BODY_BYTES
        || state.secret.expires_at <= Utc::now()
    {
        return Err(ControlProtocolError::Expired);
    }
    let signature = headers
        .get(AUTH_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(ControlProtocolError::Unauthenticated)?;
    verify_request_signature(&state.keys, body, signature)?;
    let (request_id, request) = open_control_request(&state.keys, body, Utc::now())?;
    let mut replay = state.replay.lock().await;
    let cutoff = Utc::now() - chrono::Duration::seconds(REPLAY_RETENTION_SECONDS);
    replay.retain(|_, inserted| *inserted >= cutoff);
    if replay.contains_key(&request_id) || replay.len() >= MAX_REPLAY_IDS {
        return Err(ControlProtocolError::Replay);
    }
    replay.insert(request_id, Utc::now());
    Ok((request_id, request))
}

fn signed_json_response(
    state: &WorkerState,
    request_id: uuid::Uuid,
    operation: &str,
    status: StatusCode,
    peer: &str,
    body: Vec<u8>,
) -> Result<Response<Body>, ControlProtocolError> {
    let signature = response_signature(
        &state.keys,
        request_id,
        operation,
        status.as_u16(),
        peer,
        state.secret.control_fencing_token,
        Some(&body),
    )?;
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    add_control_headers(
        response.headers_mut(),
        request_id,
        peer,
        state.secret.control_fencing_token,
        &signature,
        operation,
    )?;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok(response)
}

fn proxy_upstream_response(
    state: &WorkerState,
    request_id: uuid::Uuid,
    peer: SocketAddr,
    upstream: reqwest::Response,
) -> Result<Response<Body>, ControlProtocolError> {
    let status = upstream.status();
    let peer = peer.to_string();
    let signature = response_signature(
        &state.keys,
        request_id,
        "fetch",
        status.as_u16(),
        &peer,
        state.secret.control_fencing_token,
        None,
    )?;
    let mut headers = HeaderMap::new();
    let mut total = 0_usize;
    for (name, value) in upstream.headers() {
        if is_hop_by_hop(name) || name.as_str().starts_with("x-elixir-live-egress-") {
            continue;
        }
        total = total.saturating_add(name.as_str().len() + value.as_bytes().len());
        if headers.len() >= 64 || total > 32 * 1_024 {
            return Err(ControlProtocolError::Invalid);
        }
        headers.append(name.clone(), value.clone());
    }
    add_control_headers(
        &mut headers,
        request_id,
        &peer,
        state.secret.control_fencing_token,
        &signature,
        "fetch",
    )?;
    let body = Body::from_stream(stream::unfold(Some(upstream), |response| async move {
        let mut response = response?;
        match response.chunk().await {
            Ok(Some(chunk)) => Some((Ok::<Bytes, std::io::Error>(chunk), Some(response))),
            Ok(None) => None,
            Err(_) => Some((
                Err(std::io::Error::other("protected upstream stream failed")),
                None,
            )),
        }
    }));
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

fn add_control_headers(
    headers: &mut HeaderMap,
    request_id: uuid::Uuid,
    peer: &str,
    fence: i64,
    signature: &str,
    operation: &str,
) -> Result<(), ControlProtocolError> {
    for (name, value) in [
        (RESPONSE_SIGNATURE_HEADER, signature.to_string()),
        (RESPONSE_REQUEST_HEADER, request_id.to_string()),
        (RESPONSE_PEER_HEADER, peer.to_string()),
        (RESPONSE_FENCE_HEADER, fence.to_string()),
        (RESPONSE_KIND_HEADER, operation.to_string()),
    ] {
        headers.insert(
            HeaderName::from_static(name),
            HeaderValue::from_str(&value).map_err(|_| ControlProtocolError::Invalid)?,
        );
    }
    Ok(())
}

fn validate_fetch_url(value: &str) -> Result<Url, ControlProtocolError> {
    let url = Url::parse(value).map_err(|_| ControlProtocolError::Invalid)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || value.len() > 8_192
    {
        return Err(ControlProtocolError::Invalid);
    }
    Ok(url)
}

fn parse_socket_addresses(
    values: &[String],
    expected_port: u16,
) -> Result<Vec<SocketAddr>, ControlProtocolError> {
    if values.is_empty() || values.len() > 16 {
        return Err(ControlProtocolError::Invalid);
    }
    let mut addresses = Vec::with_capacity(values.len());
    for value in values {
        let address = value
            .parse::<SocketAddr>()
            .map_err(|_| ControlProtocolError::Invalid)?;
        if address.port() != expected_port || !is_public_egress_ip(address.ip()) {
            return Err(ControlProtocolError::Invalid);
        }
        if !addresses.contains(&address) {
            addresses.push(address);
        }
    }
    if addresses.is_empty() {
        return Err(ControlProtocolError::Invalid);
    }
    Ok(addresses)
}

fn validate_request_headers(
    values: Vec<(String, String)>,
) -> Result<HeaderMap, ControlProtocolError> {
    if values.len() > 64 {
        return Err(ControlProtocolError::Invalid);
    }
    let mut headers = HeaderMap::new();
    let mut total = 0_usize;
    for (name, value) in values {
        let name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| ControlProtocolError::Invalid)?;
        if is_hop_by_hop(&name)
            || matches!(
                name.as_str(),
                "host" | "forwarded" | "x-forwarded-for" | "x-forwarded-host" | "x-real-ip"
            )
            || name.as_str().starts_with("proxy-")
            || name.as_str().starts_with("x-elixir-")
        {
            return Err(ControlProtocolError::Invalid);
        }
        let value = HeaderValue::from_str(&value).map_err(|_| ControlProtocolError::Invalid)?;
        total = total.saturating_add(name.as_str().len() + value.as_bytes().len());
        if total > 32 * 1_024 {
            return Err(ControlProtocolError::Invalid);
        }
        headers.append(name, value);
    }
    Ok(headers)
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

async fn external_ip(url: &str) -> Option<IpAddr> {
    let url = Url::parse(url).ok()?;
    let host = url.host_str()?;
    let port = url.port_or_known_default()?;
    let mut addresses = if let Ok(address) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(address, port)]
    } else {
        time::timeout(
            Duration::from_secs(8),
            tokio::net::lookup_host((host, port)),
        )
        .await
        .ok()?
        .ok()?
        .collect::<Vec<_>>()
    };
    addresses.sort();
    addresses.dedup();
    if addresses.is_empty()
        || addresses.len() > 16
        || addresses
            .iter()
            .any(|address| !is_public_egress_ip(address.ip()))
    {
        return None;
    }
    let mut builder = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(12));
    if host.parse::<IpAddr>().is_err() {
        builder = builder.resolve_to_addrs(host, &addresses);
    }
    let client = builder.build().ok()?;
    let mut response = client.get(url).send().await.ok()?;
    if response.status() != StatusCode::OK {
        return None;
    }
    if !response
        .remote_addr()
        .is_some_and(|peer| addresses.contains(&peer))
    {
        return None;
    }
    if response.content_length().is_some_and(|length| length > 64) {
        return None;
    }
    let mut body = Vec::with_capacity(64);
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len().saturating_add(chunk.len()) > 64 {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    if body.is_empty() {
        return None;
    }
    std::str::from_utf8(&body).ok()?.trim().parse().ok()
}

fn validate_dns_name(value: &str) -> Result<(), ControlProtocolError> {
    if value.is_empty()
        || value.len() > 253
        || value.parse::<IpAddr>().is_ok()
        || !value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(ControlProtocolError::Invalid);
    }
    Ok(())
}

fn control_error_response(error: ControlProtocolError) -> Response<Body> {
    let status = match error {
        ControlProtocolError::Unauthenticated | ControlProtocolError::Replay => {
            StatusCode::UNAUTHORIZED
        }
        ControlProtocolError::Expired => StatusCode::GONE,
        ControlProtocolError::Invalid => StatusCode::BAD_REQUEST,
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn n11_worker_accepts_only_private_owned_single_link_secret_files() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("control.json");
        let secret = ControlSecretDocument::new(
            uuid::Uuid::new_v4(),
            1,
            Utc::now() + chrono::Duration::minutes(5),
            &ControlKeys::generate(),
            super::super::control::WorkerReadinessConfig {
                external_ip_url: "https://egress.example/ip".to_string(),
                dns_probe_host: "dns.example".to_string(),
                expected_egress_ips: vec!["1.1.1.1".parse().expect("public IP")],
            },
        )
        .expect("control secret");
        std::fs::write(&path, serde_json::to_vec(&secret).expect("secret JSON"))
            .expect("write secret");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("public permissions");
        assert!(read_secret(&path).await.is_err());

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("private permissions");
        let second_link = temporary.path().join("control-link.json");
        std::fs::hard_link(&path, &second_link).expect("hard link");
        assert!(read_secret(&path).await.is_err());
        std::fs::remove_file(second_link).expect("remove hard link");
        read_secret(&path).await.expect("private owned secret");

        let symbolic_link = temporary.path().join("control-symlink.json");
        symlink(&path, &symbolic_link).expect("symbolic link");
        assert!(read_secret(&symbolic_link).await.is_err());
    }

    #[test]
    fn n11_worker_rejects_private_targets_and_unsafe_headers() {
        assert!(parse_socket_addresses(&["127.0.0.1:443".to_string()], 443).is_err());
        assert!(parse_socket_addresses(&["169.254.169.254:80".to_string()], 80).is_err());
        assert!(parse_socket_addresses(&["[::ffff:127.0.0.1]:443".to_string()], 443).is_err());
        assert!(parse_socket_addresses(&["[2001:db8::1]:443".to_string()], 443).is_err());
        assert!(
            validate_request_headers(vec![(
                "proxy-authorization".to_string(),
                "secret".to_string()
            )])
            .is_err()
        );
        assert!(
            validate_request_headers(vec![(
                "authorization".to_string(),
                "Bearer source".to_string()
            )])
            .is_ok()
        );
    }
}
