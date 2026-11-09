//! Real pinned native/source rematch -> private SDK pipe -> bound owner secret
//! -> local socket -> retirement. Initial ACTIVE enrollment and loopback
//! classification are synthetic; not activation/off-site/hardware proof.
use super::super::controller::InstalledController;
use super::*;
use crate::marketplace::{
    grant_store::installation::standing::tests::local_http as enrolled,
    local_http::{Client, fixture_origin},
};
use futures_util::FutureExt;

#[tokio::test]
#[ignore = "exclusive actual pinned native/source/SDK/LAN socket and revocation; synthetic ACTIVE and test-only loopback classification"]
async fn native_local_fetch_uses_current_owner_endpoint_and_retires_original_socket() -> Result<()>
{
    native_local_transport(false).await
}

#[tokio::test]
#[ignore = "exclusive actual pinned native/source/SDK/chunked LAN response and revocation; synthetic ACTIVE and test-only loopback classification"]
async fn native_local_stream_reads_four_mib_and_retires_the_same_original_socket() -> Result<()> {
    native_local_transport(true).await
}

async fn native_local_transport(streaming: bool) -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    if !crate::marketplace::runtime::image_is_pinned(&image) {
        bail!("immutable worker image required")
    }
    let source = if streaming {
        r#"import {fetchStream,getSecret,state} from '@elixir/sdk/v1';
export async function invoke(a) {
 await state.set('cursor',a.marker);
 const r=await fetchStream(await getSecret('playlist_url'),{maximumBodyBytes:4194304,method:a.method,body:a.body??null});
 let bytes=0;for await(const chunk of r.chunks())bytes+=chunk.length;
 return {status:r.status,bytes};
}"#
    } else {
        r#"import {fetch,getSecret,state} from '@elixir/sdk/v1';
export async function invoke(a) {
 await state.set('cursor',a.marker);
 const r=await fetch(await getSecret('playlist_url'),{method:a.method,body:a.body??null});
 return {status:r.status,body:await r.text()};
}"#
    };
    let f = Fixture::with_lan_source(Some(source)).await?;
    let deployment = format!("native-local-{}", Uuid::new_v4().simple());
    let mut config = configuration(&f, &image, &deployment)?;
    config.local_http = Some(Arc::new(Client::fixture_loopback(config.playback.clone())));
    let mut controller = InstalledController::open(f.db.clone(), secrets(), config).await?;
    // Establish real runtime lineage BEFORE synthetic installed history. The
    // no-implicit-adoption production guard must not be bypassed by a fixture.
    let (owner, _) = enrolled::configured(&f, "local-native").await?;
    enrolled::config(&f, owner, json!({"localNetwork":true})).await?;
    let outcome = std::panic::AssertUnwindSafe(async {
        for (marker, revoke) in [(1, false), (2, true)] {
            let response = if revoke {
                b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nprefix"
                    .to_vec()
            } else if streaming {
                let mut response =
                    b"HTTP/1.1 200 OK\r\nContent-Length: 4194304\r\nConnection: close\r\n\r\n"
                        .to_vec();
                response.resize(response.len() + 4 * 1024 * 1024, b'x');
                response
            } else {
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_vec()
            };
            let (port, received, closed, server) = fixture_origin(response, revoke).await?;
            let result = async {
                let binding = InstalledBinding::load_configured(
                    &f.db,
                    f.actor.home,
                    f.material()?.review.scope().extension_id(),
                    owner,
                    standing_tests::limit(),
                )
                .await?;
                enrolled::secret(
                    &f,
                    &binding,
                    &format!("http://127.0.0.1:{port}/endpoint?token=native-fixture"),
                )
                .await?;
                let mut execution = limits();
                if streaming {
                    // Explicit test profile only. Keep the actual 64 MiB/100m
                    // native job, frame/result ceilings and HTTP deadline.
                    execution.pending_jobs = 4096;
                    let sdk = execution.sdk.as_mut().context("SDK fixture")?;
                    sdk.calls = 128;
                    sdk.response_bytes = 8 * 1024 * 1024;
                }
                controller
                    .begin_configured(
                        f.actor.home,
                        f.material()?.review.scope().extension_id(),
                        owner,
                        standing_tests::limit(),
                        execution,
                        RuntimeJson::from_value(json!({
                        "marker":marker,"method":"POST","body":"native-instance-body"}))?,
                        None,
                    )
                    .await?;
                if revoke {
                    let request = timeout(Duration::from_secs(180), received).await??;
                    assert!(String::from_utf8(request)?.ends_with("native-instance-body"));
                    crate::marketplace::grant_store::revoke_as(
                        &f.db,
                        &f.actor,
                        f.material()?.review.scope().extension_id(),
                        f.revision,
                        Uuid::new_v4(),
                        chrono::Utc::now(),
                    )
                    .await?;
                    assert!(
                        timeout(Duration::from_secs(45), controller.finish())
                            .await?
                            .is_err()
                    );
                    assert!(
                        timeout(Duration::from_secs(5), closed).await??,
                        "revocation must close the actual socket owned by this native SDK call"
                    );
                } else {
                    let reply = timeout(Duration::from_secs(180), controller.finish()).await??;
                    let reply: serde_json::Value = serde_json::from_str(reply.as_str())?;
                    if streaming {
                        assert_eq!(reply, json!({"status":200,"bytes":4194304}));
                    } else {
                        assert_eq!(reply, json!({"status":200,"body":"ok"}));
                    }
                    let request = String::from_utf8(received.await?)?;
                    assert!(
                        request.starts_with("POST /endpoint?token=native-fixture HTTP/1.1\r\n")
                    );
                    assert!(request.ends_with("native-instance-body"));
                }
                controller.cancel().await?;
                controller.require_idle()?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if result.is_err() {
                server.abort();
            }
            let server_result = server.await;
            result?;
            server_result??;
        }
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    // Owned cleanup runs even if a Rust assertion/checked operation failed.
    controller.cancel().await?;
    controller.shutdown_cache().await?;
    controller.require_idle()?;
    assert_disposed(&f, &image, &deployment).await?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
