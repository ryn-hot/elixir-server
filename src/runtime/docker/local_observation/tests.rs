use super::*;
use std::{os::unix::fs::PermissionsExt, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

fn endpoint() -> Result<(tempfile::TempDir, UnixListener, SocketBinding)> {
    let root = tempfile::tempdir()?;
    let path = root.path().canonicalize()?.join("daemon.sock");
    let listener = UnixListener::bind(&path)?;
    let m = std::fs::symlink_metadata(&path)?;
    Ok((
        root,
        listener,
        SocketBinding {
            path: path.to_str().unwrap().into(),
            device: m.dev(),
            inode: m.ino(),
        },
    ))
}
fn profile(root: &Path, binding: &SocketBinding) -> Result<crate::runtime::image_import::Profile> {
    let executable = root.join("approved-fixture");
    let bytes = b"fixture only: this observer must never execute an import companion";
    std::fs::write(&executable, bytes)?;
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
    crate::runtime::image_import::Profile::from_operator_configuration(
        executable.canonicalize()?,
        Digest::of(bytes),
        binding.path.clone().into(),
    )
}
fn response(status: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    bytes.extend_from_slice(body);
    bytes
}
async fn request(stream: &mut tokio::net::UnixStream) -> Result<String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).await?;
        bytes.push(byte[0]);
        if bytes.len() > 4096 {
            bail!("fixture request bounds");
        }
    }
    let text = String::from_utf8(bytes)?;
    if !text.starts_with("GET ") || !text.contains("host: localhost\r\n") {
        bail!("observation is not a fixed local GET");
    }
    Ok(text.lines().next().unwrap().into())
}
fn serve(
    listener: UnixListener,
    replies: Vec<Vec<u8>>,
) -> tokio::task::JoinHandle<Result<Vec<String>>> {
    tokio::spawn(async move {
        let mut requests = vec![];
        for reply in replies {
            let (mut stream, _) = listener.accept().await?;
            requests.push(request(&mut stream).await?);
            // A bounded client may intentionally close before a large body.
            let _ = stream.write_all(&reply).await;
        }
        Ok(requests)
    })
}

#[test]
fn observation_paths_are_closed_exact_and_never_accept_urls_or_queries() -> Result<()> {
    for bad in [
        "",
        "../images",
        "/a",
        "a/b",
        "a?x=1",
        "a#x",
        "-flag",
        "http://x",
        "a\r\n",
        "%2f",
        ".",
    ] {
        assert!(Query::Container(bad).path().is_err(), "{bad:?}");
    }
    assert!(Query::Container(&"a".repeat(256)).path().is_err());
    assert_eq!(
        Query::Container("elixir-marketplace-owned").path()?,
        "/v1.45/containers/elixir-marketplace-owned/json"
    );
    assert_eq!(Query::Info.path()?, "/v1.45/info");
    Ok(())
}

#[tokio::test]
async fn exact_native_state_is_requeried_and_absence_is_not_cached_or_a_cli_fallback() -> Result<()>
{
    let (root, listener, binding) = endpoint()?;
    let id = "a".repeat(64);
    let mut value = serde_json::json!({"Id":id,"Name":"/elixir-marketplace-owned","State":{"Running":true,"Status":"running"}});
    let running = serde_json::to_vec(&value)?;
    value["State"]["Running"] = false.into();
    value["State"]["Status"] = "exited".into();
    let exited = serde_json::to_vec(&value)?;
    let server = serve(
        listener,
        vec![
            response("200 OK", &running),
            response("200 OK", &exited),
            response(
                "404 Not Found",
                &serde_json::to_vec(
                    &serde_json::json!({"message":format!("No such container: {id}")}),
                )?,
            ),
            response("200 OK", &running),
        ],
    );
    let runtime = DockerRuntimeManager::new(Some("/must-not-run-observation-cli".into()))
        .with_marketplace_import_profile(profile(root.path(), &binding)?);
    // Synthetic endpoint-pairing ONLY, to isolate the observation path. The
    // actual initial CLI/daemon comparison is exercised separately below.
    runtime.local_cli_binding.set(()).unwrap();
    let handle = ContainerHandle {
        id: id.clone(),
        name: id.clone(),
    };
    assert!(runtime.inspect(&handle).await?.running);
    assert!(!runtime.inspect(&handle).await?.running);
    assert!(runtime.get_container_handle(&id).await?.is_none());
    assert_eq!(
        runtime
            .get_container_handle("elixir-marketplace-owned")
            .await?
            .unwrap()
            .id,
        id
    );
    let requests = server.await??;
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests[0],
        format!("GET /v1.45/containers/{id}/json HTTP/1.1")
    );
    assert_eq!(
        requests[3],
        "GET /v1.45/containers/elixir-marketplace-owned/json HTTP/1.1"
    );
    assert!(!runtime.use_local_observation("ordinary-legacy-name"));
    Ok(())
}

#[tokio::test]
async fn partial_or_wrong_container_id_and_name_never_satisfy_exact_lookup() -> Result<()> {
    let (root, listener, binding) = endpoint()?;
    let id = "a".repeat(64);
    let replies = [
        serde_json::json!({"Id":"b".repeat(64),"Name":"/elixir-marketplace-owned"}),
        serde_json::json!({"Id":"a".repeat(12),"Name":"/elixir-marketplace-owned"}),
        serde_json::json!({"Id":id,"Name":"/elixir-marketplace-other"}),
    ]
    .into_iter()
    .map(|v| response("200 OK", &serde_json::to_vec(&v).unwrap()))
    .collect();
    let server = serve(listener, replies);
    let runtime = DockerRuntimeManager::new(Some("/must-not-fallback".into()))
        .with_marketplace_import_profile(profile(root.path(), &binding)?);
    // Same explicit observation-only unit seam; not native daemon evidence.
    runtime.local_cli_binding.set(()).unwrap();
    assert!(runtime.get_container_handle(&id).await.is_err());
    assert!(runtime.get_container_handle(&id).await.is_err());
    assert!(
        runtime
            .get_container_handle("elixir-marketplace-owned")
            .await
            .is_err()
    );
    assert_eq!(server.await??.len(), 3);
    Ok(())
}

#[tokio::test]
async fn daemon_and_image_identity_checks_share_the_pinned_endpoint() -> Result<()> {
    let (root, listener, binding) = endpoint()?;
    let image = Digest::of(b"immutable image fixture");
    let server = serve(
        listener,
        vec![
            response("200 OK", br#"{"ID":"daemon-fixture"}"#),
            response(
                "200 OK",
                &serde_json::to_vec(&serde_json::json!({"Id":image.to_string()}))?,
            ),
            response(
                "200 OK",
                &serde_json::to_vec(&serde_json::json!({"Id":Digest::of(b"other").to_string()}))?,
            ),
            response("200 OK", br#"{"ID":"bad/daemon"}"#),
        ],
    );
    let runtime = DockerRuntimeManager::new(Some("/must-not-fallback".into()))
        .with_marketplace_import_profile(profile(root.path(), &binding)?);
    assert_eq!(runtime.image_import_daemon_id().await?, "daemon-fixture");
    assert_eq!(
        runtime.inspect_image_value(&image.to_string()).await?["Id"],
        image.to_string()
    );
    assert!(runtime.image_import_inspect(image).await.is_err());
    assert!(runtime.image_import_daemon_id().await.is_err());
    assert_eq!(server.await??.len(), 4);
    Ok(())
}

#[tokio::test]
async fn only_exact_daemon_object_absence_can_release_an_identity_lookup() -> Result<()> {
    let (_root, listener, binding) = endpoint()?;
    let id = "a".repeat(64);
    let image = Digest::of(b"missing image");
    let server = serve(
        listener,
        vec![
            response("404 Not Found", b"{}"),
            response("404 Not Found", br#"{"message":"page not found"}"#),
            response(
                "404 Not Found",
                br#"{"message":"No such container: other"}"#,
            ),
            response(
                "404 Not Found",
                &serde_json::to_vec(
                    &serde_json::json!({"message":format!("No such container: {id}"),"extra":true}),
                )?,
            ),
            response(
                "404 Not Found",
                &serde_json::to_vec(
                    &serde_json::json!({"message":format!("No such container: {id}")}),
                )?,
            ),
            response(
                "404 Not Found",
                &serde_json::to_vec(
                    &serde_json::json!({"message":format!("No such image: {image}")}),
                )?,
            ),
        ],
    );
    let observer = Observer::new(binding)?;
    for _ in 0..4 {
        assert!(observer.get(Query::Container(&id)).await.is_err());
    }
    assert!(observer.get(Query::Container(&id)).await?.is_none());
    assert!(observer.get(Query::Image(image)).await?.is_none());
    assert_eq!(server.await??.len(), 6);
    Ok(())
}

#[tokio::test]
async fn malformed_duplicate_nonobject_and_redirect_responses_fail_closed() -> Result<()> {
    let (_root, listener, binding) = endpoint()?;
    let server = serve(listener, vec![
        response("200 OK", b"{"), response("200 OK", br#"{"Id":1,"Id":2}"#),
        response("200 OK", b"[]"), response("500 Unavailable", b"{}"),
        response("404 Not Found", b"{}"),
        b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/never\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
    ]);
    let observer = Observer::new(binding)?;
    for _ in 0..6 {
        assert!(observer.get(Query::Info).await.is_err());
    }
    assert_eq!(server.await??, vec!["GET /v1.45/info HTTP/1.1"; 6]);
    Ok(())
}

#[tokio::test]
async fn declared_chunked_and_truncated_bodies_are_bounded() -> Result<()> {
    let (_root, listener, binding) = endpoint()?;
    let mut chunked = format!(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
        MAX_INSPECTION_BYTES + 1
    )
    .into_bytes();
    chunked.extend(vec![b' '; MAX_INSPECTION_BYTES + 1]);
    chunked.extend(b"\r\n0\r\n\r\n");
    let server = serve(
        listener,
        vec![
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_INSPECTION_BYTES + 1
            )
            .into_bytes(),
            chunked,
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{}".to_vec(),
        ],
    );
    let observer = Observer::new(binding)?;
    for _ in 0..3 {
        assert!(observer.get(Query::Info).await.is_err());
    }
    assert_eq!(server.await??.len(), 3);
    Ok(())
}

#[tokio::test]
async fn replaced_missing_or_symlinked_socket_never_reuses_a_connection() -> Result<()> {
    let (_root, listener, binding) = endpoint()?;
    let path = PathBuf::from(&binding.path);
    let server = serve(listener, vec![response("200 OK", b"{}")]);
    let observer = Observer::new(binding)?;
    observer.get(Query::Info).await?.context("fixture info")?;
    server.await??;
    std::fs::remove_file(&path)?;
    assert!(observer.get(Query::Info).await.is_err());
    let replacement = UnixListener::bind(&path)?;
    assert!(observer.get(Query::Info).await.is_err());
    std::fs::remove_file(&path)?;
    std::os::unix::fs::symlink(path.with_file_name("other.sock"), &path)?;
    assert!(observer.get(Query::Info).await.is_err());
    drop(replacement);
    Ok(())
}

#[tokio::test]
async fn stalled_response_expires_and_cancellation_returns_its_bounded_slot() -> Result<()> {
    let (_root, listener, binding) = endpoint()?;
    let observer = Observer::with_deadline(binding.clone(), Duration::from_millis(30))?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        request(&mut stream).await?;
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await?, 0);
        Ok::<_, anyhow::Error>(())
    });
    assert!(observer.get(Query::Info).await.is_err());
    server.await??;
    assert_eq!(observer.slots.available_permits(), MAX_CONCURRENT);
    let observer = Arc::new(observer);
    let permits = observer.slots.acquire_many(MAX_CONCURRENT as u32).await?;
    let pending = {
        let value = observer.clone();
        tokio::spawn(async move { value.get(Query::Info).await })
    };
    tokio::task::yield_now().await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(permits);
    assert_eq!(observer.slots.available_permits(), MAX_CONCURRENT);
    Ok(())
}

#[tokio::test]
async fn explicit_profile_replacement_reinitializes_the_local_client() -> Result<()> {
    let (first, listener, binding) = endpoint()?;
    let server = serve(listener, vec![response("200 OK", br#"{"ID":"first"}"#)]);
    let runtime = DockerRuntimeManager::new(Some("/must-not-fallback".into()))
        .with_marketplace_import_profile(profile(first.path(), &binding)?);
    assert_eq!(runtime.image_import_daemon_id().await?, "first");
    server.await??;
    let (second, listener, binding) = endpoint()?;
    let server = serve(listener, vec![response("200 OK", br#"{"ID":"second"}"#)]);
    let runtime = runtime.with_marketplace_import_profile(profile(second.path(), &binding)?);
    assert_eq!(runtime.image_import_daemon_id().await?, "second");
    server.await??;
    let unconfigured = DockerRuntimeManager::new(Some("/must-not-run".into()));
    assert!(!unconfigured.use_local_observation(&"a".repeat(64)));
    assert!(unconfigured.local_observer().await.is_err());
    Ok(())
}

fn identity_cli(root: &Path, id: &str) -> Result<String> {
    let path = root.join("identity-cli");
    let script = format!("#!/bin/sh\n[ \"$1\" = info ] || exit 91\nprintf '\"{id}\"\\n'\n");
    std::fs::write(&path, script)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
    Ok(path.to_str().unwrap().into())
}

#[tokio::test]
async fn cli_actions_are_pinned_only_after_independent_daemon_identity_matches() -> Result<()> {
    let (root, listener, binding) = endpoint()?;
    let server = serve(
        listener,
        vec![response("200 OK", br#"{"ID":"same-daemon"}"#)],
    );
    let runtime = DockerRuntimeManager::new(Some(identity_cli(root.path(), "same-daemon")?))
        .with_marketplace_import_profile(profile(root.path(), &binding)?);
    assert!(!runtime.use_local_observation(&"a".repeat(64)));
    for _ in 0..2 {
        let command = runtime.docker_command().await?;
        let args = command
            .as_std()
            .get_args()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(args, vec!["--host", &format!("unix://{}", binding.path)]);
        let environment = command.as_std().get_envs().collect::<Vec<_>>();
        for key in [
            "DOCKER_HOST",
            "DOCKER_CONTEXT",
            "DOCKER_TLS_VERIFY",
            "DOCKER_CERT_PATH",
        ] {
            assert!(
                environment
                    .iter()
                    .any(|(name, value)| *name == key && value.is_none())
            );
        }
    }
    assert!(runtime.local_cli_binding.get().is_some());
    assert!(runtime.use_local_observation(&"a".repeat(64)));
    assert_eq!(
        server.await??.len(),
        1,
        "only initial identity comparison is shared; runtime state is not cached"
    );
    std::fs::remove_file(&binding.path)?;
    assert!(
        runtime.docker_command().await.is_err(),
        "CLI actions also require the original socket binding"
    );
    Ok(())
}

#[tokio::test]
async fn different_cli_daemon_cannot_receive_mutations_or_be_silently_retargeted() -> Result<()> {
    let (root, listener, binding) = endpoint()?;
    let server = serve(
        listener,
        vec![response("200 OK", br#"{"ID":"pinned-daemon"}"#)],
    );
    let runtime = DockerRuntimeManager::new(Some(identity_cli(root.path(), "different-daemon")?))
        .with_marketplace_import_profile(profile(root.path(), &binding)?);
    let error = runtime
        .get_container_handle(&"a".repeat(64))
        .await
        .err()
        .context("different daemon must be rejected")?;
    assert!(error.to_string().contains("different daemons"));
    assert!(runtime.local_cli_binding.get().is_none());
    assert_eq!(
        server.await??,
        vec!["GET /v1.45/info HTTP/1.1"],
        "a mismatched initial lookup cannot query/report container absence"
    );
    Ok(())
}

#[test]
fn lazy_cli_binding_keeps_a_small_caller_future() {
    let runtime = DockerRuntimeManager::new(Some("/must-not-run".into()));
    let future = runtime.docker_command();
    let bytes = std::mem::size_of_val(&future);
    assert!(bytes <= 4096, "CLI setup future too large: {bytes}");
}
