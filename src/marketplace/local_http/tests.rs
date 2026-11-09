//! Actual local sockets plus synthetic caller authority/loopback classification.
//! These do NOT qualify native source invocation or off-site/private-LAN paths.
use super::*;
use crate::live::upstream::{ConfiguredLanEndpoint, LanEndpointAccess, PrivateLanGate};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
mod streaming;

struct Authority(AtomicBool);
#[async_trait::async_trait]
impl SdkHttpAuthority for Authority {
    async fn current(&self) -> Result<()> {
        if !self.0.load(Ordering::SeqCst) {
            bail!("synthetic original authority lost")
        }
        Ok(())
    }
}
fn authority() -> Arc<Authority> {
    Arc::new(Authority(AtomicBool::new(true)))
}
fn playback() -> Arc<PlaybackJobManager> {
    sqlx::any::install_default_drivers();
    let pool = sqlx::any::AnyPoolOptions::new()
        .connect_lazy("sqlite::memory:")
        .unwrap();
    Arc::new(PlaybackJobManager::new(pool, None))
}
struct FixedResolver;
impl Resolver for FixedResolver {
    fn start(&self, _: String, _: u16) -> Result<JoinHandle<Result<Vec<IpAddr>>>> {
        Ok(tokio::spawn(async {
            Ok(vec!["127.0.0.1".parse().unwrap()])
        }))
    }
}
struct NativeFixtureResolver;
impl Resolver for NativeFixtureResolver {
    fn start(&self, host: String, port: u16) -> Result<JoinHandle<Result<Vec<IpAddr>>>> {
        HostResolver.start(host, port)
    }
    fn fixture_loopback(&self) -> bool {
        true
    }
}
pub(super) fn fixture_client(playback: Arc<PlaybackJobManager>) -> Client {
    Client {
        enabled: true,
        playback,
        resolver: Arc::new(NativeFixtureResolver),
        slot: Arc::new(Semaphore::new(1)),
    }
}
fn client() -> Arc<Client> {
    Arc::new(Client {
        enabled: true,
        playback: playback(),
        resolver: Arc::new(FixedResolver),
        slot: Arc::new(Semaphore::new(1)),
    })
}
#[tokio::test]
async fn disabled_server_configuration_starts_no_lookup_or_request() -> Result<()> {
    let client = Arc::new(Client::new(false, playback()));
    assert!(
        client
            .start_request(request(1, "GET", None, 4096)?, authority())
            .is_err()
    );
    assert_eq!(client.slot.available_permits(), 1);
    Ok(())
}
fn request(port: u16, method: &str, body: Option<&str>, maximum: usize) -> Result<Request> {
    let url = format!("http://configured-lan.home.arpa:{port}/endpoint?token=fixture-only");
    let endpoint = ConfiguredLanEndpoint::new(&url, LanEndpointAccess::ExactUrl)?;
    let policy = endpoint
        .policy_for(
            &url,
            PrivateLanGate {
                server_enabled: true,
                provider_permission: true,
                descriptor_requested: true,
                destination_authorized: true,
            },
            LocalDestinationDenylist::new(vec!["192.168.1.10".parse()?], vec![])?,
        )?
        .allow_fixture_loopback();
    let request = SourceSdkRequest::parse(&RuntimeJson::from_value(serde_json::json!({
        "operation":"fetch","url":url,"method":method,
        "headers":[{"name":"authorization","value":"Bearer instance-fixture"}],"body":body,
    }))?)?;
    Ok(Request {
        request,
        maximum,
        policy,
    })
}
struct HeldResolver {
    reply: Mutex<Option<oneshot::Receiver<Vec<IpAddr>>>>,
    started: Mutex<Option<oneshot::Sender<()>>>,
    starts: AtomicUsize,
}
impl Resolver for HeldResolver {
    fn start(&self, _: String, _: u16) -> Result<JoinHandle<Result<Vec<IpAddr>>>> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let reply = self
            .reply
            .lock()
            .unwrap()
            .take()
            .context("original DNS reply already taken")?;
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        Ok(tokio::spawn(async { Ok(reply.await?) }))
    }
}

#[tokio::test]
async fn cancelled_waiters_and_cancel_timeouts_keep_the_original_dns_owner_and_slot() -> Result<()>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (reply, replies) = oneshot::channel();
    let (started, starts) = oneshot::channel();
    let resolver = Arc::new(HeldResolver {
        reply: Mutex::new(Some(replies)),
        started: Mutex::new(Some(started)),
        starts: AtomicUsize::new(0),
    });
    let client = Arc::new(Client {
        enabled: true,
        playback: playback(),
        resolver: resolver.clone(),
        slot: Arc::new(Semaphore::new(1)),
    });
    let mut call = client.start_request(request(port, "GET", None, 4096)?, authority())?;
    starts.await?;
    assert!(
        timeout(Duration::from_millis(20), call.finish())
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(20), call.cancel())
            .await
            .is_err()
    );
    assert!(call.task.is_some());
    assert_eq!(client.slot.available_permits(), 0);
    assert!(
        client
            .start_request(request(port, "GET", None, 4096)?, authority())
            .is_err()
    );
    assert_eq!(resolver.starts.load(Ordering::SeqCst), 1);
    reply.send(vec!["127.0.0.1".parse()?]).unwrap();
    timeout(Duration::from_secs(1), call.cancel()).await??;
    assert_eq!(client.slot.available_permits(), 1);
    assert!(
        timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err(),
        "a cancelled lookup must never dispatch after its original answer arrives"
    );
    Ok(())
}

#[tokio::test]
async fn authority_lost_as_dns_finishes_sends_no_http_bytes() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (reply, replies) = oneshot::channel();
    let (started, starts) = oneshot::channel();
    let client = Arc::new(Client {
        enabled: true,
        playback: playback(),
        resolver: Arc::new(HeldResolver {
            reply: Mutex::new(Some(replies)),
            started: Mutex::new(Some(started)),
            starts: AtomicUsize::new(0),
        }),
        slot: Arc::new(Semaphore::new(1)),
    });
    let authority = authority();
    let mut call = client.start_request(request(port, "GET", None, 4096)?, authority.clone())?;
    starts.await?;
    authority.0.store(false, Ordering::SeqCst);
    reply.send(vec!["127.0.0.1".parse()?]).unwrap();
    assert!(
        timeout(Duration::from_secs(1), call.finish())
            .await?
            .is_err()
    );
    call.cancel().await?;
    assert_eq!(client.slot.available_permits(), 1);
    assert!(
        timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn expired_lookup_retains_its_original_slot_until_real_completion() -> Result<()> {
    let (reply, replies) = oneshot::channel();
    let (started, starts) = oneshot::channel();
    let resolver = Arc::new(HeldResolver {
        reply: Mutex::new(Some(replies)),
        started: Mutex::new(Some(started)),
        starts: AtomicUsize::new(0),
    });
    let client = Arc::new(Client {
        enabled: true,
        playback: playback(),
        resolver: resolver.clone(),
        slot: Arc::new(Semaphore::new(1)),
    });
    let mut call = client.start_request(request(1, "GET", None, 4096)?, authority())?;
    starts.await?;
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(
        timeout(Duration::from_millis(10), call.finish())
            .await
            .is_err()
    );
    assert_eq!(client.slot.available_permits(), 0);
    assert_eq!(resolver.starts.load(Ordering::SeqCst), 1);
    reply.send(vec!["127.0.0.1".parse()?]).unwrap();
    let error = match call.finish().await {
        Err(error) => error,
        Ok(_) => bail!("expired original DNS must not dispatch HTTP"),
    };
    assert!(error.to_string().contains("DNS deadline"));
    call.cancel().await?;
    assert_eq!(client.slot.available_permits(), 1);
    Ok(())
}

async fn raw_origin(
    reply: Vec<u8>,
    hold: bool,
) -> Result<(
    u16,
    oneshot::Receiver<Vec<u8>>,
    oneshot::Receiver<bool>,
    JoinHandle<Result<()>>,
)> {
    origin(reply, hold, Duration::from_secs(3)).await
}
pub(in crate::marketplace) async fn native_origin(
    reply: Vec<u8>,
    hold: bool,
) -> Result<(
    u16,
    oneshot::Receiver<Vec<u8>>,
    oneshot::Receiver<bool>,
    JoinHandle<Result<()>>,
)> {
    origin(reply, hold, Duration::from_secs(180)).await
}
async fn origin(
    reply: Vec<u8>,
    hold: bool,
    accept_wait: Duration,
) -> Result<(
    u16,
    oneshot::Receiver<Vec<u8>>,
    oneshot::Receiver<bool>,
    JoinHandle<Result<()>>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (received, receiving) = oneshot::channel();
    let (closed, closing) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = timeout(accept_wait, listener.accept()).await??;
        let mut bytes = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let count = timeout(Duration::from_secs(3), stream.read(&mut buffer)).await??;
            if count == 0 || bytes.len() + count > 8192 {
                bail!("invalid local HTTP fixture request")
            }
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .map(str::parse::<usize>)
                    .transpose()?
                    .unwrap_or(0);
                if bytes.len() >= end + 4 + length {
                    break;
                }
            }
        }
        stream.write_all(&reply).await?;
        let _ = received.send(bytes);
        if hold {
            let mut byte = [0];
            let retired = match timeout(Duration::from_secs(8), stream.read(&mut byte)).await? {
                Ok(0) => true,
                Ok(_) => false,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    // Cancelling an unread HTTP body may close TCP with RST,
                    // not FIN/EOF. Both are actual terminal socket outcomes;
                    // a channel drop, timeout or other I/O error is NOT proof.
                    eprintln!("local SDK fixture socket retired: {:?}", error.kind());
                    true
                }
                Err(error) => return Err(error.into()),
            };
            let _ = closed.send(retired);
        } else {
            let _ = closed.send(true);
        }
        Ok(())
    });
    Ok((port, receiving, closing, task))
}
fn value(reply: SourceSdkReply) -> Result<serde_json::Value> {
    let SourceSdkReply::Value(value) = reply else {
        bail!("expected local HTTP value")
    };
    Ok(serde_json::from_str(value.as_str())?)
}

#[tokio::test]
async fn real_post_preserves_explicit_credentials_and_returns_redirect_without_following_it()
-> Result<()> {
    let response = b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://other.home.arpa/new\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_vec();
    let (port, received, _, server) = raw_origin(response, false).await?;
    let client = client();
    let mut call = client.start_request(
        request(port, "POST", Some("instance-body"), 4096)?,
        authority(),
    )?;
    let result = timeout(Duration::from_secs(2), call.finish()).await??;
    call.cancel().await?;
    let received = String::from_utf8(received.await?)?.to_ascii_lowercase();
    assert!(received.starts_with("post /endpoint?token=fixture-only http/1.1\r\n"));
    assert!(received.contains("authorization: bearer instance-fixture\r\n"));
    assert!(received.contains("accept-encoding: identity\r\n"));
    assert!(received.ends_with("instance-body"));
    let result = value(result)?;
    assert_eq!(result["status"], 307);
    assert_eq!(result["body"], "ok");
    server.await??;
    assert_eq!(client.slot.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn original_cancel_and_authority_loss_close_the_actual_response_socket_without_retry()
-> Result<()> {
    for revoke in [false, true] {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nprefix".to_vec();
        let (port, received, closed, server) = raw_origin(response, true).await?;
        let client = client();
        let authority = authority();
        let mut call =
            client.start_request(request(port, "GET", None, 4096)?, authority.clone())?;
        received.await?;
        if revoke {
            authority.0.store(false, Ordering::SeqCst);
            assert!(
                timeout(Duration::from_secs(2), call.finish())
                    .await?
                    .is_err()
            );
        }
        timeout(Duration::from_secs(2), call.cancel()).await??;
        assert!(
            timeout(Duration::from_secs(1), closed).await??,
            "the joined local HTTP call must not retain its response socket"
        );
        server.await??;
        assert_eq!(client.slot.available_permits(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn actual_http_rejects_oversize_compression_invalid_utf8_and_encoded_reply_overflow()
-> Result<()> {
    for response in [
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", MAX_HTTP_BODY + 1).into_bytes(),
        b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n\xff".to_vec(),
    ] {
        let (port, received, _, server) = raw_origin(response, false).await?;
        let client = client();
        let mut call = client.start_request(request(port, "GET", None, MAX_JSON_BYTES)?, authority())?;
        assert!(timeout(Duration::from_secs(2), call.finish()).await?.is_err());
        received.await?;
        call.cancel().await?;
        server.await??;
        assert_eq!(client.slot.available_permits(), 1);
    }
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\na".to_vec();
    let (port, _, _, server) = raw_origin(response, false).await?;
    let client = client();
    let mut call = client.start_request(request(port, "GET", None, 1)?, authority())?;
    assert!(
        call.finish().await.is_err(),
        "encoded JSON overhead is included in the allowance"
    );
    call.cancel().await?;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn real_head_does_not_charge_the_advertised_body_length() -> Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        MAX_HTTP_BODY + 1
    )
    .into_bytes();
    let (port, _, _, server) = raw_origin(response, false).await?;
    let client = client();
    let mut call = client.start_request(request(port, "HEAD", None, 4096)?, authority())?;
    assert_eq!(value(call.finish().await?)?["body"], "");
    call.cancel().await?;
    server.await??;
    Ok(())
}
