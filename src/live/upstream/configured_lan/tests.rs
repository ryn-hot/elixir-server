use super::*;
use std::net::IpAddr;

fn gate() -> PrivateLanGate {
    PrivateLanGate {
        server_enabled: true,
        provider_permission: true,
        descriptor_requested: true,
        destination_authorized: true,
    }
}
fn denylist() -> LocalDestinationDenylist {
    LocalDestinationDenylist::new(
        vec![
            "127.0.0.1".parse().unwrap(),
            "192.168.1.10".parse().unwrap(),
        ],
        vec![],
    )
    .unwrap()
}

#[test]
fn exact_url_includes_query_and_never_infers_origin_access_from_a_trailing_slash() {
    let raw = "https://nas.home.arpa:8443/playlist?token=owner-secret";
    let endpoint = ConfiguredLanEndpoint::new(raw, LanEndpointAccess::ExactUrl).unwrap();
    assert!(endpoint.accepts(raw).unwrap());
    for changed in [
        "https://nas.home.arpa:8443/playlist?token=other",
        "https://nas.home.arpa:8443/playlist",
        "https://nas.home.arpa:8443/admin?token=owner-secret",
        "https://nas.home.arpa/playlist?token=owner-secret",
        "https://other.home.arpa:8443/playlist?token=owner-secret",
        "http://nas.home.arpa:8443/playlist?token=owner-secret",
    ] {
        assert!(!endpoint.accepts(changed).unwrap());
        assert!(endpoint.policy_for(changed, gate(), denylist()).is_err());
    }
    let root =
        ConfiguredLanEndpoint::new("https://nas.home.arpa/", LanEndpointAccess::ExactUrl).unwrap();
    assert!(!root.accepts("https://nas.home.arpa/admin").unwrap());
    // Canonical authority equivalence is safe; it does not broaden the path.
    assert!(root.accepts("https://NAS.home.arpa:443/").unwrap());
}

#[test]
fn explicit_origin_access_still_pins_the_complete_individual_request() {
    let endpoint =
        ConfiguredLanEndpoint::new("http://nas.home.arpa:8080/", LanEndpointAccess::Origin)
            .unwrap();
    let raw = "http://nas.home.arpa:8080/player_api.php?action=live&token=owner-secret";
    assert!(endpoint.accepts(raw).unwrap());
    let policy = endpoint.policy_for(raw, gate(), denylist()).unwrap();
    let current = policy.validate_initial(raw).unwrap();
    for redirected in [
        "/player_api.php?action=admin&token=owner-secret",
        "/admin",
        "http://other.home.arpa:8080/",
        "https://nas.home.arpa:8080/",
        "http://nas.home.arpa:8081/",
    ] {
        assert!(policy.validate_redirect(&current, redirected).is_err());
        assert!(
            policy
                .validate_initial(current.url.join(redirected).unwrap().as_str())
                .is_err()
        );
    }
    // An origin grant permits a NEW explicitly authorized request, not a
    // redirect/URL swap under the previous request's policy.
    let next = "http://nas.home.arpa:8080/player_api.php?action=vod";
    assert!(endpoint.policy_for(next, gate(), denylist()).is_ok());
}

#[test]
fn every_gate_is_required_before_constructing_a_dns_usable_policy() {
    let raw = "https://nas.home.arpa/playlist";
    let endpoint = ConfiguredLanEndpoint::new(raw, LanEndpointAccess::ExactUrl).unwrap();
    for missing in 0..4 {
        let mut denied = gate();
        match missing {
            0 => denied.server_enabled = false,
            1 => denied.provider_permission = false,
            2 => denied.descriptor_requested = false,
            _ => denied.destination_authorized = false,
        }
        assert_eq!(
            endpoint
                .policy_for(raw, denied, denylist())
                .unwrap_err()
                .code(),
            UpstreamErrorCode::PrivateLanUnauthorized
        );
    }
    assert!(
        endpoint
            .policy_for(raw, gate(), LocalDestinationDenylist::empty())
            .is_err()
    );
}

#[test]
fn private_policy_accepts_rfc1918_and_ula_but_never_rebinds_to_public_or_local_host() {
    let raw = "https://nas.home.arpa/playlist";
    let endpoint = ConfiguredLanEndpoint::new(raw, LanEndpointAccess::ExactUrl).unwrap();
    let policy = endpoint.policy_for(raw, gate(), denylist()).unwrap();
    let target = policy.validate_initial(raw).unwrap();
    for allowed in ["10.1.2.3", "172.16.0.2", "192.168.1.11", "fd00::2"] {
        let ip: IpAddr = allowed.parse().unwrap();
        let resolved = policy.resolve_target(target.clone(), vec![ip]).unwrap();
        assert_eq!(resolved.socket_addresses.len(), 1);
        assert_eq!(resolved.socket_addresses[0].ip(), ip);
        assert_eq!(resolved.socket_addresses[0].port(), 443);
    }
    for forbidden in [
        "1.1.1.1",
        "2606:4700:4700::1111",
        "127.0.0.1",
        "::1",
        "192.168.1.10",
        "169.254.169.254",
        "fe80::1",
        "100.64.0.1",
        "::ffff:10.1.2.3",
        "64:ff9b::a01:203",
    ] {
        assert!(
            policy
                .resolve_target(target.clone(), vec![forbidden.parse().unwrap()])
                .is_err()
        );
    }
    assert!(
        policy
            .resolve_target(
                target.clone(),
                vec!["10.1.2.3".parse().unwrap(), "1.1.1.1".parse().unwrap()]
            )
            .is_err()
    );
    assert!(policy.resolve_target(target, vec![]).is_err());
}

#[test]
fn private_literal_rules_keep_the_requested_address_port_path_and_query() {
    for raw in [
        "http://192.168.1.11:8080/playlist?key=owner-secret",
        "https://[fd00::2]:8443/player_api.php?key=owner-secret",
    ] {
        let endpoint = ConfiguredLanEndpoint::new(raw, LanEndpointAccess::ExactUrl).unwrap();
        let policy = endpoint.policy_for(raw, gate(), denylist()).unwrap();
        let target = policy.validate_initial(raw).unwrap();
        let address: IpAddr = target.host.parse().unwrap();
        let resolved = policy.resolve_target(target, vec![address]).unwrap();
        assert_eq!(resolved.socket_addresses[0].ip(), address);
        assert!(
            policy
                .validate_initial(&raw.replace("owner-secret", "other"))
                .is_err()
        );
        if raw.contains("playlist") {
            assert!(
                policy
                    .validate_initial(&raw.replace("playlist", "admin"))
                    .is_err()
            );
        }
    }
}

#[test]
fn invalid_or_ambiguous_configured_urls_cannot_create_an_endpoint() {
    for raw in [
        "",
        "file:///etc/passwd",
        "ftp://nas.home.arpa/playlist",
        "http://user:password@nas.home.arpa/",
        "http://nas.home.arpa/#fragment",
        "http://nas.home.arpa:0/",
        "http://nas.home.arpa/\nadmin",
        "http://nas.home.arpa/playlist%0d%0aHost:other",
        "http://nas.home.arpa/?x=%00",
        "http://nas.home.arpa\\admin",
        "http://localhost/",
        "http://a.localhost/",
        "http://metadata.google.internal/",
        "http://host.docker.internal:2375/",
        "http://gateway.docker.internal/",
    ] {
        assert!(ConfiguredLanEndpoint::new(raw, LanEndpointAccess::Origin).is_err());
    }
    for raw in [
        format!("http://nas.home.arpa/{}", "x".repeat(4096)),
        format!("http://nas.home.arpa/{}", "x".repeat(2048)),
    ] {
        assert!(ConfiguredLanEndpoint::new(&raw, LanEndpointAccess::ExactUrl).is_err());
    }
    let endpoint =
        ConfiguredLanEndpoint::new("http://nas.home.arpa/", LanEndpointAccess::Origin).unwrap();
    assert!(endpoint.accepts("http://nas.home.arpa/?x=%0a").is_err());
}

#[test]
fn debug_does_not_expose_the_configured_endpoint_or_credential_query() {
    let endpoint = ConfiguredLanEndpoint::new(
        "http://private-owner-host.home.arpa/playlist?token=private-owner-secret",
        LanEndpointAccess::ExactUrl,
    )
    .unwrap();
    let endpoint_debug = format!("{endpoint:?}");
    let policy_debug = format!(
        "{:?}",
        endpoint
            .policy_for(
                "http://private-owner-host.home.arpa/playlist?token=private-owner-secret",
                gate(),
                denylist()
            )
            .unwrap()
    );
    for text in [endpoint_debug, policy_debug] {
        assert!(!text.contains("private-owner"));
        assert!(!text.contains("token="));
    }
}

#[tokio::test]
async fn actual_connector_pins_the_request_and_query_redirect_never_reaches_a_second_request()
-> anyhow::Result<()> {
    use super::super::{
        DnsResolver, FetchRequest, UpstreamFetcher, UpstreamLimits, UpstreamMethod,
    };
    use crate::live::contract::SensitiveString;
    use axum::{Router, http::Uri, response::Redirect, routing::get};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    struct Resolver;
    #[async_trait::async_trait]
    impl DnsResolver for Resolver {
        async fn resolve(&self, _: &str, _: u16, _: &CancellationToken) -> Result<Vec<IpAddr>> {
            Ok(vec!["127.0.0.1".parse().unwrap()])
        }
    }
    let received = Arc::new(AtomicUsize::new(0));
    let counter = received.clone();
    let application = Router::new()
        .route("/playlist", get(|uri: Uri| async move { uri.to_string() }))
        .route(
            "/redirect",
            get(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Redirect::temporary("/redirect?token=other") }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let shutdown = CancellationToken::new();
    let stopped = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, application)
            .with_graceful_shutdown(stopped.cancelled_owned())
            .await
    });
    let result = async {
        let fetcher = UpstreamFetcher::new(
            Arc::new(Resolver),
            UpstreamLimits {
                connect_timeout: Duration::from_secs(1),
                header_timeout: Duration::from_secs(1),
                idle_timeout: Duration::from_secs(1),
                total_timeout: Duration::from_secs(2),
                max_response_bytes: 4096,
                max_redirects: 1,
                ..Default::default()
            },
        )?;
        let endpoint = ConfiguredLanEndpoint::new(
            &format!("http://configured-lan.home.arpa:{port}/"),
            LanEndpointAccess::Origin,
        )?;
        let request = |raw: &str, fixture: bool| -> Result<FetchRequest> {
            // Like the existing upstream connector fixtures, use a nonempty
            // denylist for a different server address. Explicit denylist
            // entries MUST win even over the test-only loopback classifier.
            let fixture_denylist =
                LocalDestinationDenylist::new(vec!["192.168.1.10".parse().unwrap()], vec![])?;
            let policy = endpoint.policy_for(raw, gate(), fixture_denylist)?;
            // ONLY a transport fixture: production policy still rejects this
            // loopback resolver answer. No actual LAN/hardware claim is made.
            let policy = if fixture {
                policy.allow_fixture_loopback()
            } else {
                policy
            };
            Ok(FetchRequest::new(
                SensitiveString::new(raw),
                UpstreamMethod::Get,
                policy,
                CancellationToken::new(),
            ))
        };
        let raw = format!("http://configured-lan.home.arpa:{port}/playlist?token=owner-secret");
        assert_eq!(
            fetcher
                .fetch(request(&raw, false)?)
                .await
                .unwrap_err()
                .code(),
            UpstreamErrorCode::AddressForbidden
        );
        let body = fetcher
            .fetch(request(&raw, true)?)
            .await?
            .collect()
            .await?
            .into_bytes();
        assert_eq!(body, b"/playlist?token=owner-secret");
        let redirect =
            format!("http://configured-lan.home.arpa:{port}/redirect?token=owner-secret");
        assert_eq!(
            fetcher
                .fetch(request(&redirect, true)?)
                .await
                .unwrap_err()
                .code(),
            UpstreamErrorCode::DestinationForbidden
        );
        assert_eq!(
            received.load(Ordering::SeqCst),
            1,
            "the query-changing redirect must be rejected before a second request"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), server).await???;
    result
}
