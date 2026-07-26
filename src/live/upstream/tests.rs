use std::{
    collections::{BTreeMap, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result as AnyResult, bail};
use async_trait::async_trait;
use axum::{Router, routing::get};
use reqwest::Certificate;
use serde_json::Value;
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, UdpSocket},
    process::Command,
};
use tokio_util::sync::CancellationToken;

use crate::live::contract::{
    ClientDisclosure, CredentialAuthority, ProviderCookie, SensitiveString, ServerEgress,
    SourceDescriptor, StreamProtocol, TimeShift,
};

use super::{
    BlockedNetwork, CredentialSet, DestinationPolicy, DestinationRule, DirectEgressConnector,
    DnsResolver, FetchRequest, HostGatewayDnsResolver, LocalDestinationDenylist, NetworkScope,
    PrivateLanGate, SafeRequestHeaders, UpstreamErrorCode, UpstreamFetcher, UpstreamLimits,
    UpstreamMethod, error::Result,
};

struct SequenceResolver {
    answers: Mutex<VecDeque<Vec<IpAddr>>>,
}

impl SequenceResolver {
    fn fixed(address: IpAddr) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(VecDeque::from([vec![address]])),
        })
    }

    fn sequence(answers: impl IntoIterator<Item = Vec<IpAddr>>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers.into_iter().collect()),
        })
    }
}

#[async_trait]
impl DnsResolver for SequenceResolver {
    async fn resolve(
        &self,
        _host: &str,
        _port: u16,
        cancellation: &CancellationToken,
    ) -> Result<Vec<IpAddr>> {
        if cancellation.is_cancelled() {
            return Err(UpstreamErrorCode::Cancelled.into());
        }
        let mut answers = self
            .answers
            .lock()
            .map_err(|_| UpstreamErrorCode::DnsFailed)?;
        if answers.len() > 1 {
            answers
                .pop_front()
                .ok_or_else(|| UpstreamErrorCode::DnsEmpty.into())
        } else {
            answers
                .front()
                .cloned()
                .ok_or_else(|| UpstreamErrorCode::DnsEmpty.into())
        }
    }
}

struct FailingResolver {
    code: UpstreamErrorCode,
}

#[async_trait]
impl DnsResolver for FailingResolver {
    async fn resolve(
        &self,
        _host: &str,
        _port: u16,
        _cancellation: &CancellationToken,
    ) -> Result<Vec<IpAddr>> {
        Err(self.code.into())
    }
}

struct PythonFixture {
    _temporary: TempDir,
    child: tokio::process::Child,
    ready: Value,
}

impl PythonFixture {
    async fn start(script: &Path, arguments: &[String]) -> AnyResult<Self> {
        let temporary = tempfile::tempdir()?;
        let ready_file = temporary.path().join("ready.json");
        let mut command = Command::new("python3");
        command
            .arg(script)
            .args(arguments)
            .arg("--ready-file")
            .arg(&ready_file);
        command
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().context("starting Live Python fixture")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let ready = loop {
            if let Ok(payload) = tokio::fs::read(&ready_file).await {
                break serde_json::from_slice(&payload)?;
            }
            if child.try_wait()?.is_some() {
                bail!("Live Python fixture exited before readiness");
            }
            if Instant::now() >= deadline {
                bail!("Live Python fixture did not become ready");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        Ok(Self {
            _temporary: temporary,
            child,
            ready,
        })
    }

    fn port(&self, name: &str) -> AnyResult<u16> {
        self.ready[name]
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .context("fixture port")
    }

    async fn stop(mut self) -> AnyResult<()> {
        self.child.kill().await?;
        let _ = self.child.wait().await;
        Ok(())
    }
}

fn origin_script() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/live/origin-suite/src/origin_server.py")
}

fn dns_script() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/live/network-suite/src/dns_server.py")
}

async fn origin(arguments: &[String]) -> AnyResult<PythonFixture> {
    let mut all = vec![
        "--host".into(),
        "127.0.0.1".into(),
        "--port".into(),
        "0".into(),
    ];
    all.extend_from_slice(arguments);
    PythonFixture::start(&origin_script(), &all).await
}

fn rule(scheme: &str, host: &str, port: u16, path: &str, scope: NetworkScope) -> DestinationRule {
    DestinationRule::new(scheme, host, port, path, scope, true).unwrap()
}

fn public_fixture_policy(scheme: &str, host: &str, port: u16, paths: &[&str]) -> DestinationPolicy {
    DestinationPolicy::new(
        paths
            .iter()
            .map(|path| rule(scheme, host, port, path, NetworkScope::Public))
            .collect(),
        PrivateLanGate::default(),
        scheme == "http",
        LocalDestinationDenylist::empty(),
    )
    .unwrap()
    .allow_fixture_loopback()
}

fn test_local_denylist() -> LocalDestinationDenylist {
    LocalDestinationDenylist::new(
        vec!["192.168.1.1".parse().unwrap()],
        vec![BlockedNetwork::new("172.18.0.0".parse().unwrap(), 16).unwrap()],
    )
    .unwrap()
}

fn test_limits() -> UpstreamLimits {
    UpstreamLimits {
        connect_timeout: Duration::from_secs(2),
        header_timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(2),
        total_timeout: Duration::from_secs(10),
        max_response_bytes: 1024 * 1024,
        max_response_headers: 64,
        max_response_header_bytes: 32 * 1024,
        max_redirects: 5,
    }
}

fn request(url: String, policy: DestinationPolicy) -> FetchRequest {
    FetchRequest::new(
        SensitiveString::new(url),
        UpstreamMethod::Get,
        policy,
        CancellationToken::new(),
    )
}

#[test]
fn r10_url_destination_and_debug_policy_is_exact_and_redacted() {
    let secret = "https://public.example/path?sig=ELIXIR_LIVE_CANARY_URL";
    let policy = DestinationPolicy::new(
        vec![rule(
            "https",
            "public.example",
            443,
            "/path",
            NetworkScope::Public,
        )],
        PrivateLanGate::default(),
        false,
        LocalDestinationDenylist::empty(),
    )
    .unwrap();
    let fetch_request = request(secret.to_string(), policy.clone());
    let debug = format!("{fetch_request:?}");
    assert!(!debug.contains("public.example"));
    assert!(!debug.contains("CANARY"));

    assert!(policy.validate_initial(secret).is_ok());
    for invalid in [
        "ftp://public.example/path",
        "https://user:password@public.example/path",
        "https://public.example/other",
        "https://public.example/path#fragment",
        "https://public.example/path?value=%0Ainjected",
        "https://localhost/path",
    ] {
        assert!(policy.validate_initial(invalid).is_err(), "{invalid}");
    }
    let error = policy
        .validate_initial("https://ELIXIR_LIVE_CANARY_SECRET@public.example/path")
        .unwrap_err();
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("CANARY"));
    assert!(!rendered.contains("public.example"));

    let redirect_policy = DestinationPolicy::new(
        vec![
            rule(
                "https",
                "public.example",
                443,
                "/path",
                NetworkScope::Public,
            ),
            rule("http", "public.example", 80, "/path", NetworkScope::Public),
        ],
        PrivateLanGate::default(),
        true,
        LocalDestinationDenylist::empty(),
    )
    .unwrap();
    let current = redirect_policy
        .validate_initial("https://public.example/path")
        .unwrap();
    assert_eq!(
        redirect_policy
            .validate_redirect(&current, "http://public.example/path")
            .unwrap_err()
            .code(),
        UpstreamErrorCode::RedirectDowngrade
    );
}

#[test]
fn r10_public_session_policy_allows_dynamic_cdns_but_never_private_addresses() {
    let policy =
        DestinationPolicy::for_public_session(Vec::new(), false, LocalDestinationDenylist::empty())
            .expect("public session policy");
    let initial = policy
        .validate_initial("https://origin.example/live/master.m3u8?token=one")
        .expect("dynamic source is admitted");
    let redirected = policy
        .validate_redirect(&initial, "https://cdn.example/events/index.m3u8?token=two")
        .expect("public CDN redirect is admitted");
    assert!(
        policy
            .resolve_target(redirected.clone(), vec!["8.8.8.8".parse().unwrap()])
            .is_ok()
    );
    assert_eq!(
        policy
            .resolve_target(redirected, vec!["10.0.0.8".parse().unwrap()])
            .unwrap_err()
            .code(),
        UpstreamErrorCode::NetworkScopeMismatch
    );
    assert_eq!(
        policy
            .validate_initial("http://cdn.example/events/index.m3u8")
            .unwrap_err()
            .code(),
        UpstreamErrorCode::SchemeForbidden
    );
}

#[test]
fn r10_ip_policy_rejects_special_mixed_and_wrong_scope_answers() {
    let public = rule(
        "https",
        "target.example",
        443,
        "/live",
        NetworkScope::Public,
    );
    let policy = DestinationPolicy::new(
        vec![public],
        PrivateLanGate::default(),
        false,
        LocalDestinationDenylist::empty(),
    )
    .unwrap();
    let target = policy
        .validate_initial("https://target.example/live")
        .unwrap();
    assert!(
        policy
            .resolve_target(target.clone(), vec!["8.8.8.8".parse().unwrap()])
            .is_ok()
    );
    let mixed = policy
        .resolve_target(
            target.clone(),
            vec!["8.8.8.8".parse().unwrap(), "10.0.0.8".parse().unwrap()],
        )
        .unwrap_err();
    assert_eq!(mixed.code(), UpstreamErrorCode::DnsMixedScope);
    let wrong_scope = policy
        .resolve_target(target.clone(), vec!["10.0.0.8".parse().unwrap()])
        .unwrap_err();
    assert_eq!(wrong_scope.code(), UpstreamErrorCode::NetworkScopeMismatch);

    for forbidden in [
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        "169.254.169.254".parse().unwrap(),
        "100.64.0.1".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        "198.18.0.1".parse().unwrap(),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        "::ffff:127.0.0.1".parse().unwrap(),
        "fe80::1".parse().unwrap(),
        "2001:db8::1".parse().unwrap(),
        "64:ff9b::7f00:1".parse().unwrap(),
    ] {
        let error = policy
            .resolve_target(target.clone(), vec![forbidden])
            .unwrap_err();
        assert_eq!(error.code(), UpstreamErrorCode::AddressForbidden);
    }
}

#[test]
fn r10_private_lan_requires_all_four_independent_approvals() {
    let private_rule = rule(
        "http",
        "nas.home.example",
        8080,
        "/stream",
        NetworkScope::PrivateLan,
    );
    let all = PrivateLanGate {
        server_enabled: true,
        provider_permission: true,
        descriptor_requested: true,
        destination_authorized: true,
    };
    assert_eq!(
        DestinationPolicy::new(
            vec![private_rule.clone()],
            all,
            true,
            LocalDestinationDenylist::empty(),
        )
        .unwrap_err()
        .code(),
        UpstreamErrorCode::DestinationForbidden
    );
    for missing in 0..4 {
        let mut gate = all;
        match missing {
            0 => gate.server_enabled = false,
            1 => gate.provider_permission = false,
            2 => gate.descriptor_requested = false,
            _ => gate.destination_authorized = false,
        }
        let policy = DestinationPolicy::new(
            vec![private_rule.clone()],
            gate,
            true,
            test_local_denylist(),
        )
        .unwrap();
        let target = policy
            .validate_initial("http://nas.home.example:8080/stream")
            .unwrap();
        assert_eq!(
            policy
                .resolve_target(target, vec!["192.168.1.12".parse().unwrap()])
                .unwrap_err()
                .code(),
            UpstreamErrorCode::PrivateLanUnauthorized
        );
    }
    let policy =
        DestinationPolicy::new(vec![private_rule], all, true, test_local_denylist()).unwrap();
    let target = policy
        .validate_initial("http://nas.home.example:8080/stream")
        .unwrap();
    assert!(
        policy
            .resolve_target(
                target.clone(),
                vec!["10.0.0.2".parse().unwrap(), "fd00::2".parse().unwrap()],
            )
            .is_ok()
    );
    assert_eq!(
        policy
            .resolve_target(target.clone(), vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],)
            .unwrap_err()
            .code(),
        UpstreamErrorCode::AddressForbidden
    );
    for local_target in ["192.168.1.1", "172.18.2.4"] {
        assert_eq!(
            policy
                .resolve_target(target.clone(), vec![local_target.parse().unwrap()])
                .unwrap_err()
                .code(),
            UpstreamErrorCode::AddressForbidden
        );
    }
}

#[test]
fn private_provider_authority_is_scoped_and_keeps_infrastructure_destinations_blocked() {
    let gateway_address: IpAddr = "192.168.65.2".parse().unwrap();
    let policy = DestinationPolicy::new(
        vec![
            DestinationRule::for_private_provider_authority("http", "host.docker.internal", 8090)
                .unwrap(),
        ],
        PrivateLanGate {
            server_enabled: true,
            provider_permission: true,
            descriptor_requested: true,
            destination_authorized: true,
        },
        true,
        LocalDestinationDenylist::new(vec![gateway_address], Vec::new()).unwrap(),
    )
    .unwrap();

    for path in ["/live/account/channel.ts", "/hls/child/playlist.m3u8"] {
        let target = policy
            .validate_initial(&format!("http://host.docker.internal:8090{path}"))
            .unwrap();
        assert!(policy.resolve_target(target, vec![gateway_address]).is_ok());
    }

    assert_eq!(
        policy
            .validate_initial("http://host.docker.internal:8091/live")
            .unwrap_err()
            .code(),
        UpstreamErrorCode::HostForbidden
    );
    assert_eq!(
        policy
            .validate_initial("http://other.private.invalid:8090/live")
            .unwrap_err()
            .code(),
        UpstreamErrorCode::DestinationForbidden
    );

    let loopback_target = policy
        .validate_initial("http://host.docker.internal:8090/live")
        .unwrap();
    assert!(
        policy
            .resolve_target(loopback_target, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .is_ok()
    );

    for forbidden in [
        "169.254.169.254".parse().unwrap(),
        "fe80::1".parse().unwrap(),
    ] {
        let target = policy
            .validate_initial("http://host.docker.internal:8090/live")
            .unwrap();
        assert_eq!(
            policy
                .resolve_target(target, vec![forbidden])
                .unwrap_err()
                .code(),
            UpstreamErrorCode::AddressForbidden
        );
    }

    let literal_loopback_policy = DestinationPolicy::new(
        vec![DestinationRule::for_private_provider_authority("http", "127.0.0.1", 8090).unwrap()],
        PrivateLanGate {
            server_enabled: true,
            provider_permission: true,
            descriptor_requested: true,
            destination_authorized: true,
        },
        true,
        LocalDestinationDenylist::new(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], Vec::new()).unwrap(),
    )
    .unwrap();
    let literal_loopback = literal_loopback_policy
        .validate_initial("http://127.0.0.1:8090/live")
        .unwrap();
    assert_eq!(
        literal_loopback_policy
            .resolve_target(literal_loopback, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .unwrap_err()
            .code(),
        UpstreamErrorCode::AddressForbidden
    );

    for host in [
        "localhost",
        "metadata.google.internal",
        "metadata.aws.internal",
        "kubernetes.default.svc",
    ] {
        assert_eq!(
            DestinationRule::for_private_provider_authority("http", host, 8090)
                .unwrap_err()
                .code(),
            UpstreamErrorCode::HostForbidden
        );
    }
}

#[tokio::test]
async fn docker_host_gateway_falls_back_only_for_dns_resolution_failures() {
    let resolver = HostGatewayDnsResolver::new(Arc::new(FailingResolver {
        code: UpstreamErrorCode::DnsFailed,
    }));
    assert_eq!(
        resolver
            .resolve("host.docker.internal", 8090, &CancellationToken::new())
            .await
            .unwrap(),
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]
    );
    assert_eq!(
        resolver
            .resolve("media.example.invalid", 443, &CancellationToken::new())
            .await
            .unwrap_err()
            .code(),
        UpstreamErrorCode::DnsFailed
    );

    let cancelled = HostGatewayDnsResolver::new(Arc::new(FailingResolver {
        code: UpstreamErrorCode::Cancelled,
    }));
    assert_eq!(
        cancelled
            .resolve("host.docker.internal", 8090, &CancellationToken::new())
            .await
            .unwrap_err()
            .code(),
        UpstreamErrorCode::Cancelled
    );
}

#[tokio::test]
async fn authorized_docker_host_gateway_fetches_through_the_host_loopback_fallback() -> AnyResult<()>
{
    let app = Router::new().route(
        "/live/channel.ts",
        get(|| async {
            (
                [(reqwest::header::CONTENT_TYPE, "video/mp2t")],
                vec![0x47_u8; 188 * 4],
            )
        }),
    );
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let policy = DestinationPolicy::new(
        vec![DestinationRule::for_private_provider_authority(
            "http",
            "host.docker.internal",
            port,
        )?],
        PrivateLanGate {
            server_enabled: true,
            provider_permission: true,
            descriptor_requested: true,
            destination_authorized: true,
        },
        true,
        LocalDestinationDenylist::new(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], Vec::new())?,
    )?;
    let resolver = HostGatewayDnsResolver::new(Arc::new(FailingResolver {
        code: UpstreamErrorCode::DnsFailed,
    }));
    let fetcher = UpstreamFetcher::new(Arc::new(resolver), test_limits())?;
    let response = fetcher
        .fetch(request(
            format!("http://host.docker.internal:{port}/live/channel.ts"),
            policy,
        ))
        .await?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.collect().await?.as_bytes(), vec![0x47_u8; 188 * 4]);
    server.abort();
    let _ = server.await;
    Ok(())
}

#[tokio::test]
async fn r10_fetch_pins_origin_follows_validated_redirects_ranges_and_accounts_bytes()
-> AnyResult<()> {
    let fixture = origin(&[]).await?;
    let port = fixture.port("port")?;
    let host = "origin.live.test";
    let paths = [
        "/adversarial/redirect/2",
        "/adversarial/redirect/1",
        "/adversarial/redirect/0",
        "/transport/progressive.mp4",
    ];
    let policy = public_fixture_policy("http", host, port, &paths);
    let resolver = SequenceResolver::fixed(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let fetcher = UpstreamFetcher::new(resolver, test_limits())?;
    let mut safe = SafeRequestHeaders::new();
    safe.insert("Accept", "video/mp4")?;
    safe.insert("Range", "bytes=10-109")?;
    let response = fetcher
        .fetch(
            request(
                format!("http://{host}:{port}/adversarial/redirect/2"),
                policy,
            )
            .with_safe_headers(safe),
        )
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.stats().redirects(), 3);
    assert!(response.headers().contains_key("content-range"));
    assert!(!response.headers().contains_key("server"));
    let stats = response.stats();
    let body = response.collect().await?.into_bytes();
    assert_eq!(body.len(), 100);
    assert_eq!(stats.bytes_received(), 100);
    assert!(stats.average_bytes_per_second() > 0);
    fixture.stop().await
}

#[tokio::test]
async fn r10_dns_is_revalidated_and_rebinding_is_rejected_before_second_connect() -> AnyResult<()> {
    let fixture = origin(&[]).await?;
    let port = fixture.port("port")?;
    let host = "rebind.live.test";
    let policy = public_fixture_policy("http", host, port, &["/health"]);
    let resolver = SequenceResolver::sequence([
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        vec!["169.254.169.254".parse().unwrap()],
    ]);
    let fetcher = UpstreamFetcher::new(resolver, test_limits())?;
    let first = fetcher
        .fetch(request(
            format!("http://{host}:{port}/health"),
            policy.clone(),
        ))
        .await?
        .collect()
        .await?
        .into_bytes();
    assert!(String::from_utf8(first)?.contains("healthy"));
    let second = fetcher
        .fetch(request(format!("http://{host}:{port}/health"), policy))
        .await
        .unwrap_err();
    assert_eq!(second.code(), UpstreamErrorCode::AddressForbidden);
    fixture.stop().await
}

#[tokio::test]
async fn r10_idle_total_body_and_cancellation_limits_fail_closed() -> AnyResult<()> {
    let fixture = origin(&[]).await?;
    let port = fixture.port("port")?;
    let host = "faults.live.test";
    let paths = ["/adversarial/slow", "/adversarial/oversized", "/health"];
    let policy = public_fixture_policy("http", host, port, &paths);
    let resolver = SequenceResolver::fixed(IpAddr::V4(Ipv4Addr::LOCALHOST));

    let control = reqwest::Client::builder().no_proxy().build()?;
    control
        .post(format!("http://127.0.0.1:{port}/control/config"))
        .json(&serde_json::json!({"delayMs": 100}))
        .send()
        .await?
        .error_for_status()?;
    let mut header_limits = test_limits();
    header_limits.header_timeout = Duration::from_millis(30);
    let header_fetcher = UpstreamFetcher::new(resolver.clone(), header_limits)?;
    assert_eq!(
        header_fetcher
            .fetch(request(
                format!("http://{host}:{port}/health"),
                policy.clone(),
            ))
            .await
            .unwrap_err()
            .code(),
        UpstreamErrorCode::HeaderTimeout
    );
    control
        .post(format!("http://127.0.0.1:{port}/control/reset"))
        .json(&serde_json::json!({}))
        .send()
        .await?
        .error_for_status()?;

    let mut total_limits = test_limits();
    total_limits.idle_timeout = Duration::from_secs(1);
    total_limits.total_timeout = Duration::from_millis(40);
    let total_fetcher = UpstreamFetcher::new(resolver.clone(), total_limits)?;
    let mut total_response = total_fetcher
        .fetch(request(
            format!("http://{host}:{port}/adversarial/slow?chunks=1&chunkBytes=64&delayMs=100"),
            policy.clone(),
        ))
        .await?;
    assert_eq!(
        total_response.next_chunk().await.unwrap_err().code(),
        UpstreamErrorCode::TotalTimeout
    );

    let mut limits = test_limits();
    limits.idle_timeout = Duration::from_millis(30);
    limits.max_response_bytes = 1024;
    let fetcher = UpstreamFetcher::new(resolver, limits)?;
    let mut slow = fetcher
        .fetch(request(
            format!("http://{host}:{port}/adversarial/slow?chunks=2&chunkBytes=64&delayMs=100"),
            policy.clone(),
        ))
        .await?;
    assert_eq!(
        slow.next_chunk().await.unwrap_err().code(),
        UpstreamErrorCode::IdleTimeout
    );
    let oversized = fetcher
        .fetch(request(
            format!("http://{host}:{port}/adversarial/oversized?bytes=2048"),
            policy.clone(),
        ))
        .await
        .unwrap_err();
    assert_eq!(oversized.code(), UpstreamErrorCode::BodyTooLarge);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_request = FetchRequest::new(
        SensitiveString::new(format!("http://{host}:{port}/health")),
        UpstreamMethod::Get,
        policy,
        cancelled,
    );
    assert_eq!(
        fetcher.fetch(cancelled_request).await.unwrap_err().code(),
        UpstreamErrorCode::Cancelled
    );
    fixture.stop().await
}

#[tokio::test]
async fn r10_credentials_cookies_and_cross_origin_redirects_are_authority_scoped() -> AnyResult<()>
{
    let fixture = origin(&[]).await?;
    let port = fixture.port("port")?;
    let primary = "credentials.live.test";
    let other = "other.live.test";
    let mut rules = Vec::new();
    for (host, path) in [
        (primary, "/cookies/set"),
        (primary, "/echo/request"),
        (primary, "/adversarial/redirect-target"),
        (primary, "/control/state"),
        (other, "/echo/request"),
    ] {
        rules.push(rule("http", host, port, path, NetworkScope::Public));
    }
    let policy = DestinationPolicy::new(
        rules,
        PrivateLanGate::default(),
        true,
        LocalDestinationDenylist::empty(),
    )?
    .allow_fixture_loopback();
    let resolver = SequenceResolver::fixed(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let fetcher = UpstreamFetcher::new(resolver, test_limits())?;
    let mut descriptor = descriptor_with_credentials(primary, port);
    descriptor.cookies.clear();
    let credentials = Arc::new(CredentialSet::from_descriptor(&descriptor)?);

    let cookie_response = fetcher
        .fetch(
            request(
                format!("http://{primary}:{port}/cookies/set?path=%2F"),
                policy.clone(),
            )
            .with_credentials(credentials.clone()),
        )
        .await?;
    assert!(!cookie_response.headers().contains_key("set-cookie"));
    cookie_response.collect().await?;
    let same_origin: Value = serde_json::from_slice(
        &fetcher
            .fetch(
                request(
                    format!("http://{primary}:{port}/echo/request"),
                    policy.clone(),
                )
                .with_credentials(credentials.clone()),
            )
            .await?
            .collect()
            .await?
            .as_bytes(),
    )?;
    assert_eq!(same_origin["authorizationPresent"], true);
    assert_eq!(same_origin["cookiePresent"], true);

    let target = format!("http://{other}:{port}/echo/request");
    let redirect_url = format!(
        "http://{primary}:{port}/adversarial/redirect-target?target={}",
        urlencoding::encode(&target)
    );
    let cross_origin: Value = serde_json::from_slice(
        &fetcher
            .fetch(request(redirect_url, policy).with_credentials(credentials))
            .await?
            .collect()
            .await?
            .as_bytes(),
    )?;
    assert_eq!(cross_origin["authorizationPresent"], false);
    assert_eq!(cross_origin["cookiePresent"], false);
    let state: Value = serde_json::from_slice(
        fetcher
            .fetch(
                request(
                    format!("http://{primary}:{port}/control/state"),
                    public_fixture_policy("http", primary, port, &["/control/state"]),
                )
                .with_credentials(Arc::new(CredentialSet::from_descriptor(&descriptor)?)),
            )
            .await?
            .collect()
            .await?
            .as_bytes(),
    )?;
    let cross_observation = state["observations"]
        .as_array()
        .and_then(|values| values.last())
        .context("cross-origin observation")?;
    assert_eq!(cross_observation["originPresent"], false);
    assert_eq!(cross_observation["refererPresent"], false);
    fixture.stop().await
}

#[test]
fn r10_request_header_and_cookie_smuggling_inputs_are_rejected() {
    let mut safe = SafeRequestHeaders::new();
    for (name, value) in [
        ("Host", "internal"),
        ("Range", "bytes=0-1,4-5"),
        ("Accept", "video/mp4\r\nX-Injected: yes"),
        ("Authorization", "secret"),
    ] {
        assert!(safe.insert(name, value).is_err());
    }

    let mut descriptor = descriptor_with_credentials("header.live.test", 443);
    descriptor.request_headers.insert(
        "Host".to_string(),
        SensitiveString::new("metadata.internal"),
    );
    assert_eq!(
        CredentialSet::from_descriptor(&descriptor)
            .unwrap_err()
            .code(),
        UpstreamErrorCode::HeaderRejected
    );
    descriptor.request_headers.remove("Host");
    descriptor.cookies[0].domain = Some("other.live.test".to_string());
    assert_eq!(
        CredentialSet::from_descriptor(&descriptor)
            .unwrap_err()
            .code(),
        UpstreamErrorCode::CookieRejected
    );
    descriptor.cookies[0].domain = None;
    descriptor
        .credential_authorities
        .push(descriptor.credential_authorities[0].clone());
    assert_eq!(
        CredentialSet::from_descriptor(&descriptor)
            .unwrap_err()
            .code(),
        UpstreamErrorCode::HeaderRejected
    );
}

#[tokio::test]
async fn r10_tls_uses_original_authority_sni_and_rejects_wrong_host() -> AnyResult<()> {
    let certificates = tempfile::tempdir()?;
    let generator = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fixtures/live/origin-suite/scripts/generate_test_certificates.sh");
    let status = Command::new("sh")
        .arg(generator)
        .arg(certificates.path())
        .status()
        .await?;
    assert!(status.success());
    let ca = Certificate::from_pem(&tokio::fs::read(certificates.path().join("ca.crt")).await?)?;
    let connector = DirectEgressConnector::with_additional_roots(vec![ca]);
    let resolver = SequenceResolver::fixed(IpAddr::V4(Ipv4Addr::LOCALHOST));

    let valid = origin(&[
        "--tls-cert".into(),
        certificates.path().join("valid.crt").display().to_string(),
        "--tls-key".into(),
        certificates.path().join("valid.key").display().to_string(),
    ])
    .await?;
    let valid_port = valid.port("port")?;
    let valid_policy = public_fixture_policy("https", "127.0.0.1", valid_port, &["/health"]);
    let fetcher =
        UpstreamFetcher::with_direct_connector(resolver.clone(), connector.clone(), test_limits())?;
    let payload = fetcher
        .fetch(request(
            format!("https://127.0.0.1:{valid_port}/health"),
            valid_policy,
        ))
        .await?
        .collect()
        .await?
        .into_bytes();
    assert!(String::from_utf8(payload)?.contains("healthy"));
    valid.stop().await?;

    let wrong = origin(&[
        "--tls-cert".into(),
        certificates
            .path()
            .join("wrong-host.crt")
            .display()
            .to_string(),
        "--tls-key".into(),
        certificates
            .path()
            .join("wrong-host.key")
            .display()
            .to_string(),
    ])
    .await?;
    let wrong_port = wrong.port("port")?;
    let wrong_policy = public_fixture_policy("https", "127.0.0.1", wrong_port, &["/health"]);
    let error = fetcher
        .fetch(request(
            format!("https://127.0.0.1:{wrong_port}/health?ELIXIR_LIVE_CANARY_TLS"),
            wrong_policy,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), UpstreamErrorCode::UpstreamConnect);
    assert!(!format!("{error:?} {error}").contains("CANARY"));
    wrong.stop().await
}

#[tokio::test]
async fn r10_sensitive_response_headers_fail_and_body_debug_is_redacted() -> AnyResult<()> {
    let app = Router::new()
        .route(
            "/sensitive",
            get(|| async {
                (
                    [(
                        reqwest::header::ETAG,
                        "\"ELIXIR_LIVE_CANARY_RESPONSE_HEADER\"",
                    )],
                    "ok",
                )
            }),
        )
        .route(
            "/body",
            get(|| async { "ELIXIR_LIVE_CANARY_INTERNAL_BODY" }),
        );
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let host = "sensitive.live.test";
    let policy = public_fixture_policy("http", host, port, &["/sensitive", "/body"]);
    let resolver = SequenceResolver::fixed(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let fetcher = UpstreamFetcher::new(resolver, test_limits())?;
    let error = fetcher
        .fetch(request(
            format!("http://{host}:{port}/sensitive"),
            policy.clone(),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), UpstreamErrorCode::SensitiveResponse);
    assert!(!format!("{error:?} {error}").contains("CANARY"));

    let body = fetcher
        .fetch(request(format!("http://{host}:{port}/body"), policy))
        .await?
        .collect()
        .await?;
    assert!(body.as_bytes().starts_with(b"ELIXIR_LIVE_CANARY_"));
    assert!(!format!("{body:?}").contains("CANARY"));
    server.abort();
    let _ = server.await;
    Ok(())
}

#[tokio::test]
async fn r10_adversarial_dns_fixture_profiles_cross_the_production_resolver_contract()
-> AnyResult<()> {
    let fixture = PythonFixture::start(
        &dns_script(),
        &[
            "--host".into(),
            "127.0.0.1".into(),
            "--dns-port".into(),
            "0".into(),
            "--control-port".into(),
            "0".into(),
        ],
    )
    .await?;
    let resolver = FixtureDnsResolver {
        server: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), fixture.port("dnsPort")?),
    };
    let hosts = [
        ("public.live.test", NetworkScope::Public),
        ("private.live.test", NetworkScope::PrivateLan),
        ("mixed.live.test", NetworkScope::Public),
        ("rebind.live.test", NetworkScope::Public),
    ];
    let rules = hosts
        .iter()
        .map(|(host, scope)| rule("https", host, 443, "/live", *scope))
        .collect();
    let policy = DestinationPolicy::new(
        rules,
        PrivateLanGate {
            server_enabled: true,
            provider_permission: true,
            descriptor_requested: true,
            destination_authorized: true,
        },
        false,
        test_local_denylist(),
    )?;
    let cancellation = CancellationToken::new();
    for host in ["public.live.test", "private.live.test"] {
        let target = policy.validate_initial(&format!("https://{host}/live"))?;
        let answers = resolver.resolve(host, 443, &cancellation).await?;
        assert!(policy.resolve_target(target, answers).is_ok());
    }
    let mixed_target = policy.validate_initial("https://mixed.live.test/live")?;
    let mixed = resolver
        .resolve("mixed.live.test", 443, &cancellation)
        .await?;
    assert_eq!(
        policy
            .resolve_target(mixed_target, mixed)
            .unwrap_err()
            .code(),
        UpstreamErrorCode::AddressForbidden
    );
    let first_target = policy.validate_initial("https://rebind.live.test/live")?;
    let first = resolver
        .resolve("rebind.live.test", 443, &cancellation)
        .await?;
    assert!(policy.resolve_target(first_target, first).is_ok());
    let second_target = policy.validate_initial("https://rebind.live.test/live")?;
    let second = resolver
        .resolve("rebind.live.test", 443, &cancellation)
        .await?;
    assert_eq!(
        policy
            .resolve_target(second_target, second)
            .unwrap_err()
            .code(),
        UpstreamErrorCode::AddressForbidden
    );
    fixture.stop().await
}

fn descriptor_with_credentials(host: &str, port: u16) -> SourceDescriptor {
    SourceDescriptor {
        stream_id: "stream".to_string(),
        label: "Fixture".to_string(),
        quality: None,
        language: None,
        priority: 0,
        protocol: StreamProtocol::Hls,
        url: SensitiveString::new(format!("http://{host}:{port}/echo/request")),
        request_headers: BTreeMap::from([(
            "authorization".to_string(),
            SensitiveString::new("Bearer ELIXIR_LIVE_CANARY_AUTHORITY"),
        )]),
        cookies: vec![ProviderCookie {
            name: "initial".to_string(),
            value: SensitiveString::new("ELIXIR_LIVE_CANARY_INITIAL_COOKIE"),
            domain: None,
            path: Some("/".to_string()),
            secure: false,
            http_only: true,
            expires_at: None,
        }],
        origin: Some(SensitiveString::new("https://client.invalid")),
        referer: Some(SensitiveString::new("https://client.invalid/live")),
        credential_authorities: vec![CredentialAuthority {
            scheme: "http".to_string(),
            host: host.to_string(),
            port,
            send_request_headers: true,
            send_cookies: true,
            send_origin: true,
            send_referer: true,
        }],
        client_disclosure: ClientDisclosure::ServerOnly,
        expires_at: None,
        refresh_handle: None,
        server_egress: ServerEgress::NotRequired,
        private_network: false,
        time_shift: TimeShift {
            available: false,
            window_seconds: None,
        },
        media: None,
    }
}

struct FixtureDnsResolver {
    server: SocketAddr,
}

#[async_trait]
impl DnsResolver for FixtureDnsResolver {
    async fn resolve(
        &self,
        host: &str,
        _port: u16,
        cancellation: &CancellationToken,
    ) -> Result<Vec<IpAddr>> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|_| UpstreamErrorCode::DnsFailed)?;
        let query = dns_query(host)?;
        socket
            .send_to(&query, self.server)
            .await
            .map_err(|_| UpstreamErrorCode::DnsFailed)?;
        let mut response = [0u8; 4096];
        let length = tokio::select! {
            _ = cancellation.cancelled() => return Err(UpstreamErrorCode::Cancelled.into()),
            result = tokio::time::timeout(Duration::from_secs(2), socket.recv(&mut response)) => {
                result
                    .map_err(|_| UpstreamErrorCode::DnsTimeout)?
                    .map_err(|_| UpstreamErrorCode::DnsFailed)?
            }
        };
        parse_dns_response(&response[..length])
    }
}

fn dns_query(host: &str) -> Result<Vec<u8>> {
    let mut output = vec![0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(UpstreamErrorCode::DnsFailed.into());
        }
        output.push(u8::try_from(label.len()).map_err(|_| UpstreamErrorCode::DnsFailed)?);
        output.extend_from_slice(label.as_bytes());
    }
    output.extend_from_slice(&[0, 0, 1, 0, 1]);
    Ok(output)
}

fn parse_dns_response(response: &[u8]) -> Result<Vec<IpAddr>> {
    if response.len() < 12 || response[0..2] != [0x12, 0x34] || response[3] & 0x0f != 0 {
        return Err(UpstreamErrorCode::DnsFailed.into());
    }
    let answers = usize::from(u16::from_be_bytes([response[6], response[7]]));
    let mut offset = 12;
    while response
        .get(offset)
        .copied()
        .ok_or(UpstreamErrorCode::DnsFailed)?
        != 0
    {
        let length = usize::from(response[offset]);
        offset = offset.saturating_add(length + 1);
        if offset >= response.len() {
            return Err(UpstreamErrorCode::DnsFailed.into());
        }
    }
    offset = offset.saturating_add(5);
    let mut values = Vec::with_capacity(answers);
    for _ in 0..answers {
        if offset + 12 > response.len() || response[offset] & 0xc0 != 0xc0 {
            return Err(UpstreamErrorCode::DnsFailed.into());
        }
        let record_type = u16::from_be_bytes([response[offset + 2], response[offset + 3]]);
        let length = usize::from(u16::from_be_bytes([
            response[offset + 10],
            response[offset + 11],
        ]));
        offset += 12;
        let payload = response
            .get(offset..offset + length)
            .ok_or(UpstreamErrorCode::DnsFailed)?;
        match record_type {
            1 if payload.len() == 4 => values.push(IpAddr::V4(Ipv4Addr::new(
                payload[0], payload[1], payload[2], payload[3],
            ))),
            28 if payload.len() == 16 => {
                let bytes: [u8; 16] = payload
                    .try_into()
                    .map_err(|_| UpstreamErrorCode::DnsFailed)?;
                values.push(IpAddr::V6(Ipv6Addr::from(bytes)));
            }
            _ => return Err(UpstreamErrorCode::DnsFailed.into()),
        }
        offset += length;
    }
    if values.is_empty() {
        return Err(UpstreamErrorCode::DnsEmpty.into());
    }
    Ok(values)
}
