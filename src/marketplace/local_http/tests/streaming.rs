use super::*;

#[tokio::test]
async fn periodic_checks_cannot_suspend_a_lock_holder_and_deadlock_themselves() -> Result<()> {
    struct SlowAuthority(tokio::sync::Mutex<()>);
    #[async_trait::async_trait]
    impl SdkHttpAuthority for SlowAuthority {
        async fn current(&self) -> Result<()> {
            let _lock = self.0.lock().await;
            // Models an actual authority transaction outlasting the 500ms
            // refresh tick. A nested check cannot acquire its suspended lock.
            tokio::time::sleep(Duration::from_millis(600)).await;
            Ok(())
        }
    }
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_vec();
    let (port, _, _, server) = raw_origin(response, false).await?;
    let client = client();
    let mut call = client.start_stream_request(
        request(port, "GET", None, 4096)?,
        1,
        2,
        Arc::new(SlowAuthority(tokio::sync::Mutex::new(()))),
    )?;
    let answer = timeout(Duration::from_secs(4), call.headers()).await;
    // Always retire the original even if the regression assertion fails.
    call.cancel().await?;
    server.await??;
    assert_eq!(
        value(answer.context("authority check deadlocked across its own refresh tick")??)?["status"],
        200
    );
    assert_eq!(client.slot.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn one_original_response_above_two_mib_streams_with_exact_utf8_and_encoded_bounds()
-> Result<()> {
    let body = format!("abc{}", "🌍".repeat(800_000));
    let mut reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    reply.extend_from_slice(body.as_bytes());
    let (port, received, _, server) = raw_origin(reply, false).await?;
    let client = client();
    let mut call = client.start_stream_request(
        request(port, "GET", None, 65536)?,
        1,
        4 * 1024 * 1024,
        authority(),
    )?;
    assert_eq!(value(call.headers().await?)?["status"], 200);
    let mut combined = String::new();
    let mut parts = 0;
    let mut largest = 0;
    loop {
        let reply = call.read(65536).await?;
        let SourceSdkReply::Value(raw) = &reply else {
            bail!("expected stream chunk")
        };
        assert!(raw.accounted_bytes() <= 65536);
        let chunk = value(reply)?;
        assert_eq!(chunk["stream_id"], 1);
        let text = chunk["chunk"].as_str().context("text chunk")?;
        largest = largest.max(text.len());
        combined.push_str(text);
        parts += 1;
        assert!(parts <= 512);
        if chunk["done"] == true {
            break;
        }
    }
    assert_eq!(combined, body);
    assert!(parts > 1);
    assert!(
        largest > 60_000,
        "plain UTF-8 must not pay the six-byte control-character cost"
    );
    call.cancel().await?;
    assert!(
        String::from_utf8(received.await?)?
            .starts_with("GET /endpoint?token=fixture-only HTTP/1.1")
    );
    server.await??;
    assert_eq!(client.slot.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn maximally_escaped_chunks_still_fit_the_original_encoded_reply_budget() -> Result<()> {
    let body = vec![1u8; 8192];
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(&body);
    let (port, _, _, server) = raw_origin(response, false).await?;
    let client = client();
    let mut call =
        client.start_stream_request(request(port, "GET", None, 4096)?, 1, 8192, authority())?;
    call.headers().await?;
    let mut received = 0;
    loop {
        let SourceSdkReply::Value(raw) = call.read(4096).await? else {
            bail!("text chunk required")
        };
        assert!(raw.accounted_bytes() <= 4096);
        let value: serde_json::Value = serde_json::from_str(raw.as_str())?;
        let chunk = value["chunk"].as_str().context("text chunk")?;
        assert!(chunk.bytes().all(|byte| byte == 1));
        received += chunk.len();
        if value["done"] == true {
            break;
        }
    }
    assert_eq!(received, body.len());
    call.cancel().await?;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn cancelled_read_waiter_keeps_original_stream_until_socket_retirement() -> Result<()> {
    for revoke in [false, true] {
        let reply =
            b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\nprefix"
                .to_vec();
        let (port, received, closed, server) = raw_origin(reply, true).await?;
        let client = client();
        let authority = authority();
        let mut call = client.start_stream_request(
            request(port, "GET", None, 65536)?,
            1,
            2 * 1024 * 1024,
            authority.clone(),
        )?;
        call.headers().await?;
        received.await?;
        if revoke {
            authority.0.store(false, Ordering::SeqCst);
        } else {
            assert!(
                timeout(Duration::from_millis(20), call.read(65536))
                    .await
                    .is_err()
            );
            assert_eq!(client.slot.available_permits(), 0);
        }
        if revoke {
            assert!(timeout(Duration::from_secs(2), closed).await??);
            call.cancel().await?;
        } else {
            call.cancel().await?;
            assert!(timeout(Duration::from_secs(2), closed).await??);
        }
        server.await??;
        assert_eq!(client.slot.available_permits(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn stream_rejects_declared_and_chunked_overflow_and_invalid_utf8() -> Result<()> {
    for (reply,headers_ok) in [
        (b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n12345".to_vec(),false),
        (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\n12345\r\n0\r\n\r\n".to_vec(),true),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n\xff".to_vec(),true),
    ] {
        let (port,_,_,server)=raw_origin(reply,false).await?;
        let client=client();
        let mut call=client.start_stream_request(request(port,"GET",None,4096)?,1,4,authority())?;
        if headers_ok {call.headers().await?;assert!(call.read(4096).await.is_err());}
        else {assert!(call.headers().await.is_err());}
        call.cancel().await?;server.await??;assert_eq!(client.slot.available_permits(),1);
    }
    Ok(())
}
