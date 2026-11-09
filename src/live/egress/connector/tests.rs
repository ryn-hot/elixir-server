use super::*;
use crate::live::upstream::{DestinationPolicy, LocalDestinationDenylist};
use axum::{Router, body::Bytes, http::HeaderMap, routing::post};
use reqwest::{Method, StatusCode};
use tokio::net::TcpListener;

fn target() -> ResolvedTarget {
    let policy = DestinationPolicy::for_public_session(
        Vec::new(),
        false,
        LocalDestinationDenylist::default(),
    )
    .unwrap();
    policy
        .resolve_target(
            policy
                .validate_initial("https://origin.example/feed")
                .unwrap(),
            vec!["8.8.8.8".parse().unwrap()],
        )
        .unwrap()
}

fn transport(listener: &TcpListener, keys: ControlKeys) -> ProtectedEgressTransport {
    ProtectedEgressTransport::new(
        Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap(),
        keys,
        1,
        Duration::from_secs(1),
    )
    .unwrap()
}

#[tokio::test]
async fn control_v2_rejects_bodies_and_unsupported_methods_without_opening_a_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connector = ProtectedConnector {
        client: transport(&listener, ControlKeys::generate()).client,
    };
    let target = target();
    for (method, body) in [
        (Method::GET, Some("")),
        (Method::GET, Some("must-not-be-silently-discarded")),
        (Method::HEAD, Some("")),
        (Method::POST, None),
        (Method::POST, Some("secret=value")),
        (Method::PUT, None),
        (Method::DELETE, None),
    ] {
        let error = connector
            .execute(
                &target,
                PreparedRequest {
                    method,
                    headers: HeaderMap::new(),
                    body: body.map(str::to_owned),
                },
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), UpstreamErrorCode::EgressRejected);
        assert!(!error.to_string().contains("secret"));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn control_v2_still_sends_authenticated_bodyless_get_and_head() {
    use super::super::control::{
        open_control_request, response_signature, verify_request_signature,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let keys = ControlKeys::generate();
    let connector = ProtectedConnector {
        client: transport(&listener, keys.clone()).client,
    };
    let (sent, mut received) = tokio::sync::mpsc::channel(2);
    let router = Router::new().route(
        "/v1/fetch",
        post(move |headers: HeaderMap, body: Bytes| {
            let keys = keys.clone();
            let sent = sent.clone();
            async move {
                assert!(!String::from_utf8_lossy(&body).contains("private-token"));
                verify_request_signature(&keys, &body, headers[AUTH_HEADER].to_str().unwrap())
                    .unwrap();
                let (id, request): (_, FetchControlRequest) =
                    open_control_request(&keys, &body, Utc::now()).unwrap();
                assert_eq!(request.url, "https://origin.example/feed");
                assert_eq!(request.socket_addresses, ["8.8.8.8:443"]);
                assert_eq!(
                    request.headers,
                    [("authorization".into(), "Bearer private-token".into())]
                );
                sent.send(request.method.clone()).await.unwrap();
                let signature =
                    response_signature(&keys, id, "fetch", 200, "8.8.8.8:443", 1, None).unwrap();
                let mut response = axum::http::Response::new(axum::body::Body::from(
                    if request.method == "HEAD" { "" } else { "ok" },
                ));
                for (name, value) in [
                    (RESPONSE_SIGNATURE_HEADER, signature),
                    (RESPONSE_REQUEST_HEADER, id.to_string()),
                    (RESPONSE_PEER_HEADER, "8.8.8.8:443".into()),
                    (RESPONSE_FENCE_HEADER, "1".into()),
                    (RESPONSE_KIND_HEADER, "fetch".into()),
                ] {
                    response.headers_mut().insert(name, value.parse().unwrap());
                }
                response
            }
        }),
    );
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    let result = async {
        for method in [Method::GET, Method::HEAD] {
            let response = connector
                .execute(
                    &target(),
                    PreparedRequest {
                        method: method.clone(),
                        headers: HeaderMap::from_iter([(
                            reqwest::header::AUTHORIZATION,
                            "Bearer private-token".parse().unwrap(),
                        )]),
                        body: None,
                    },
                    Duration::from_secs(1),
                    &CancellationToken::new(),
                )
                .await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.headers().get(RESPONSE_SIGNATURE_HEADER).is_none());
            assert_eq!(
                response.text().await.unwrap(),
                if method == Method::HEAD { "" } else { "ok" }
            );
            assert_eq!(received.recv().await.unwrap(), method.as_str());
        }
        Ok::<_, UpstreamError>(())
    }
    .await;
    stop.cancel();
    server.await.unwrap();
    result.unwrap();
}
