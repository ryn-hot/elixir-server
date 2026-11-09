//! Synthetic OCI reports for lifecycle guards; actual native test is opt-in.
use super::*;
use elixir_marketplace::identity::Digest;
use serde_json::json;

fn profile() -> ApplicationHttpProfile {
    ApplicationHttpProfile::new(
        "storage-fixture",
        &format!("sha256:{}", "a".repeat(64)),
        &"b".repeat(64),
        Digest::of(b"scope"),
        Digest::of(b"invocation"),
    )
    .unwrap()
}
fn fixture(profile: &ApplicationHttpProfile) -> (ContainerHandle, Value) {
    let spec = profile.spec();
    let handle = ContainerHandle {
        id: "c".repeat(64),
        name: spec.name.clone(),
    };
    let value = json!({
        "Id":handle.id,"Name":format!("/{}",handle.name),"Image":spec.image,
        "Config":{"Image":spec.image,"User":"10005:10005","Labels":spec.labels,
            "Cmd":spec.command,"Entrypoint":[ENTRYPOINT],"Env":["PATH=/usr/bin","NODE_VERSION=24.16.0"],
            "OpenStdin":true,"Tty":false},
        "State":{"Status":"created","Running":false,"Paused":false,"Restarting":false,
            "OOMKilled":false,"Dead":false,"Pid":0,"ExitCode":0,"Error":""},
        "HostConfig":{"NetworkMode":spec.network_mode,"ReadonlyRootfs":true,
            "SecurityOpt":["no-new-privileges:true"],"Privileged":false,"CapAdd":null,"CapDrop":["ALL"],
            "Memory":96*1024*1024,"MemorySwap":96*1024*1024,"NanoCpus":90_000_000,
            "PidsLimit":64,"ShmSize":1024*1024,"LogConfig":{"Type":"none","Config":{}},
            "Ulimits":[{"Name":"core","Soft":0,"Hard":0},{"Name":"nofile","Soft":64,"Hard":64},
                {"Name":"fsize","Soft":1,"Hard":1}],
            "Init":false,"PidMode":"","IpcMode":"private","UsernsMode":"",
            "Devices":null,"DeviceRequests":null,"Binds":null,"Sysctls":null,"Tmpfs":null,
            "RestartPolicy":{"Name":"no"}},
        "Mounts":[],"NetworkSettings":{"Networks":{},"Ports":{}}
    });
    (handle, value)
}

#[test]
fn application_http_native_configuration_rejects_changed_start_intent_and_identity() -> Result<()> {
    let profile = profile();
    let (handle, good) = fixture(&profile);
    let state =
        DockerRuntimeManager::verify_application_http_configuration(&good, &handle, &profile)?;
    assert_eq!(state.status, "created");
    assert!(DockerRuntimeManager::application_http_stopped(&good));
    for (pointer, value) in [
        ("/Id", json!("d".repeat(64))),
        ("/Name", json!("/foreign")),
        ("/Image", json!(format!("sha256:{}", "d".repeat(64)))),
        ("/Config/Cmd", json!(["--sdk-http"])),
        ("/Config/Entrypoint", json!(["/bin/sh"])),
        ("/Config/Env", json!(["NODE_OPTIONS=--require=/evil"])),
        ("/Config/Labels/elixir.managed", json!("false")),
        ("/HostConfig/Init", json!(true)),
        ("/HostConfig/NetworkMode", json!("host")),
        ("/HostConfig/Memory", json!(0)),
        ("/HostConfig/NanoCpus", json!(200_000_000)),
        ("/HostConfig/RestartPolicy/Name", json!("always")),
        ("/HostConfig/PidMode", json!("host")),
        ("/HostConfig/IpcMode", json!("host")),
        ("/HostConfig/Binds", json!(["/tmp:/state:ro"])),
        ("/HostConfig/Ulimits", json!([])),
        ("/HostConfig/Devices", json!([{"PathOnHost":"/dev/tun"}])),
        (
            "/HostConfig/SecurityOpt",
            json!(["no-new-privileges:true", "seccomp:unconfined"]),
        ),
        ("/State/Running", Value::Null),
        ("/State/Status", json!("unknown")),
    ] {
        let mut bad = good.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(
            DockerRuntimeManager::verify_application_http_configuration(&bad, &handle, &profile)
                .is_err(),
            "{pointer}"
        );
    }
    Ok(())
}

#[test]
fn application_http_termination_is_positive_and_cleanup_can_stop_owned_configuration_drift()
-> Result<()> {
    let profile = profile();
    let (handle, good) = fixture(&profile);
    for status in ["created", "exited", "dead"] {
        let mut stopped = good.clone();
        stopped["State"]["Status"] = json!(status);
        DockerRuntimeManager::require_application_http_owner(&stopped, &handle, &profile)?;
        assert!(DockerRuntimeManager::application_http_stopped(&stopped));
    }
    for (pointer, value) in [
        ("/State/Pid", json!(42)),
        ("/State/Running", Value::Null),
        ("/State/Paused", json!(true)),
        ("/State/Restarting", json!(true)),
        ("/State/Status", json!("running")),
    ] {
        let mut bad = good.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(
            !DockerRuntimeManager::application_http_stopped(&bad),
            "{pointer}"
        );
    }
    let mut drift = good.clone();
    drift["HostConfig"]["ReadonlyRootfs"] = json!(false);
    assert!(
        DockerRuntimeManager::verify_application_http_configuration(&drift, &handle, &profile)
            .is_err()
    );
    // Cleanup ownership is independent of a safe-to-start configuration.
    DockerRuntimeManager::require_application_http_owner(&drift, &handle, &profile)?;
    let mut exited = good;
    exited["State"]["Status"] = json!("exited");
    DockerRuntimeManager::verify_private_native_exit(&exited, &handle, &profile.spec().labels)?;
    exited["State"]["OOMKilled"] = json!(true);
    assert!(
        DockerRuntimeManager::verify_private_native_exit(&exited, &handle, &profile.spec().labels)
            .is_err()
    );
    Ok(())
}

#[test]
fn application_http_scope_survives_main_extension_orphan_reconciliation_and_features_are_exact() {
    let profile = profile();
    assert!(
        !DockerRuntimeManager::should_prune_orphaned_managed_container(
            &profile.spec().labels,
            &HashSet::new(),
            "storage-fixture"
        )
    );
    let good = HashMap::from(
        [
            "sdk_http",
            "sdk_http_stream",
            "sdk_http_stream_large",
            "native_deadline",
        ]
        .map(|name| (format!("elixir.marketplace.worker.{name}"), "1".into())),
    );
    features(&good).unwrap();
    for key in good.keys() {
        for version in [None, Some("0"), Some("2"), Some("01")] {
            let mut bad = good.clone();
            match version {
                Some(version) => {
                    bad.insert(key.clone(), version.into());
                }
                None => {
                    bad.remove(key);
                }
            }
            assert!(features(&bad).is_err());
        }
    }
}

#[tokio::test]
async fn application_http_preflight_failure_returns_its_linear_attempt_without_create() -> Result<()>
{
    let runtime =
        DockerRuntimeManager::new(Some("/must-not-run-application-http-preflight".into()));
    let profile = profile();
    let outcome = runtime
        .create_application_http(&profile, HttpCreateAttempt::fixture(&profile))
        .await?;
    assert!(matches!(outcome, HttpCreation::NotAttempted(_)));
    Ok(())
}

#[tokio::test]
async fn application_http_mutable_or_substituted_handles_fail_before_any_cli() {
    let runtime = DockerRuntimeManager::new(Some("/must-not-run-application-http-fixture".into()));
    let profile = profile();
    let (handle, _) = fixture(&profile);
    for bad in [
        ContainerHandle {
            id: "mutable-name".into(),
            name: handle.name.clone(),
        },
        ContainerHandle {
            id: handle.id.clone(),
            name: "substituted-helper".into(),
        },
    ] {
        for result in [
            runtime.start_application_http(&bad, &profile).await,
            runtime
                .attach_application_http(&bad, &profile)
                .await
                .map(|_| ()),
            runtime
                .confirm_application_http_success(&bad, &profile)
                .await,
            runtime.stop_application_http(&bad, &profile).await,
            runtime.remove_application_http(&bad, &profile).await,
        ] {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("original immutable helper identity")
            );
        }
    }
}

/// Best-effort emergency cleanup only. Normal test cleanup below positively
/// verifies removal; a Drop attempt never counts as a successful retirement.
struct FixtureCleanup(Vec<(String, HashMap<String, String>)>);
impl Drop for FixtureCleanup {
    fn drop(&mut self) {
        for (id, expected) in self.0.iter().rev() {
            let result = std::process::Command::new("docker")
                .args(["inspect", "--format", "{{json .Config.Labels}}", id])
                .output();
            if let Ok(result) = result
                && result.status.success()
                && serde_json::from_slice::<HashMap<String, String>>(&result.stdout)
                    .is_ok_and(|actual| expected.iter().all(|(k, v)| actual.get(k) == Some(v)))
            {
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", id])
                    .output();
            }
        }
    }
}

#[tokio::test]
#[ignore = "exclusive real Docker application-helper create/start/stdio/retirement/collision; network=none gateway fixture, NOT VPN or installed-grant proof"]
async fn native_application_http_uses_closed_profile_and_preserves_foreign_name_collision()
-> Result<()> {
    use tokio::io::AsyncReadExt;
    let image = std::env::var("ELIXIR_MARKETPLACE_APPLICATION_HTTP_IMAGE")?;
    if !image.strip_prefix("sha256:").is_some_and(immutable_id) {
        bail!("exact local image required");
    }
    let runtime = DockerRuntimeManager::new(None);
    let nonce = Uuid::new_v4().simple().to_string();
    let owner = HashMap::from([(
        "elixir.marketplace.application_http.fixture".into(),
        nonce.clone(),
    )]);
    let mut cleanup = FixtureCleanup(vec![]);
    // An isolated sleep process supplies ONLY a namespace identity. No tunnel,
    // grant, account, routing capability or outbound success is manufactured.
    let sleeper = |name: String| {
        vec![
            "run".into(),
            "-d".into(),
            "--pull=never".into(),
            "--init=false".into(),
            "--name".into(),
            name,
            "--network=none".into(),
            "--read-only".into(),
            "--cap-drop=ALL".into(),
            "--security-opt=no-new-privileges".into(),
            "--memory=32m".into(),
            "--memory-swap=32m".into(),
            "--cpus=0.01".into(),
            "--pids-limit=8".into(),
            "--log-driver=none".into(),
            "--label".into(),
            format!("elixir.marketplace.application_http.fixture={nonce}"),
            "--entrypoint=/bin/sleep".into(),
            image.clone(),
            "120".into(),
        ]
    };
    let gateway = runtime
        .run_stdout(&sleeper(format!("elx-app-http-gateway-{nonce}")))
        .await?
        .trim()
        .to_owned();
    if !immutable_id(&gateway) {
        bail!("fixture gateway omitted identity");
    }
    cleanup.0.push((gateway.clone(), owner.clone()));
    let profile = ApplicationHttpProfile::new(
        "storage-native-fixture",
        &image,
        &gateway,
        Digest::of(b"no installed grant; configuration fixture only"),
        Digest::of(nonce.as_bytes()),
    )?;
    let HttpCreation::Created(handle) = runtime
        .create_application_http(&profile, HttpCreateAttempt::fixture(&profile))
        .await?
    else {
        bail!("native helper preflight unexpectedly rejected its original attempt");
    };
    cleanup
        .0
        .push((handle.id.clone(), profile.spec().labels.clone()));
    let created = runtime.inspect_container(&handle.id).await?;
    let actual =
        DockerRuntimeManager::verify_application_http_configuration(&created, &handle, &profile)?;
    if actual.running || actual.status != "created" {
        bail!("CREATE started native work");
    }
    let unstarted = runtime.inspect_unstarted_application_http(&profile).await?;
    if unstarted.id != handle.id || unstarted.name != handle.name {
        bail!("unstarted recovery substituted identity");
    }
    if runtime
        .confirm_application_http_success(&handle, &profile)
        .await
        .is_ok()
    {
        bail!("never-started helper reported natural success");
    }
    runtime.start_application_http(&handle, &profile).await?;
    if runtime
        .inspect_unstarted_application_http(&profile)
        .await
        .is_ok()
    {
        bail!("unbound recovery accepted a running native helper");
    }
    if runtime
        .start_application_http(&handle, &profile)
        .await
        .is_ok()
    {
        bail!("native helper restarted");
    }
    let process = runtime.attach_application_http(&handle, &profile).await?;
    let PrivateProcessStdio {
        mut child,
        input,
        output,
        diagnostics,
    } = process;
    drop(input); // EOF before OPEN: no DNS, socket or HTTP effect is requested.
    let mut stdout = vec![];
    let mut stderr = vec![];
    let mut output = output.take(256);
    let mut diagnostic = diagnostics
        .context("native fixture diagnostics missing")?
        .take(4096);
    let (_, _, exit) = timeout(Duration::from_secs(75), async {
        tokio::try_join!(
            output.read_to_end(&mut stdout),
            diagnostic.read_to_end(&mut stderr),
            child.wait(),
        )
    })
    .await
    .context("native original helper wait deadline")??;
    let mut expected = elixir_marketplace::sdk_http::NATIVE_EXIT_MARKER.to_vec();
    expected.push(1);
    if exit.success() || stdout != expected {
        bail!("missing original native rejection/wait marker");
    }
    if stderr.len() >= 4096 {
        bail!("unbounded native diagnostic");
    }
    if runtime
        .confirm_application_http_success(&handle, &profile)
        .await
        .is_ok()
    {
        bail!("rejected request became successful native work");
    }
    runtime.stop_application_http(&handle, &profile).await?;
    runtime.remove_application_http(&handle, &profile).await?;
    if runtime.find_container_id(&handle.id).await?.is_some() {
        bail!("owned helper remained after removal");
    }
    if runtime
        .inspect_unstarted_application_http(&profile)
        .await
        .is_ok()
    {
        bail!("unbound recovery treated name absence as confirmed creation");
    }
    // A different process with the same human name must not be replaced.
    let foreign = runtime
        .run_stdout(&sleeper(profile.spec().name.clone()))
        .await?
        .trim()
        .to_owned();
    if !immutable_id(&foreign) {
        bail!("foreign fixture omitted identity");
    }
    cleanup.0.push((foreign.clone(), owner.clone()));
    let rejected = runtime
        .create_application_http(&profile, HttpCreateAttempt::fixture(&profile))
        .await?;
    assert!(
        matches!(rejected, HttpCreation::NotAttempted(_)),
        "foreign name must be a definite pre-CREATE rejection, not replacement"
    );
    if runtime
        .inspect_unstarted_application_http(&profile)
        .await
        .is_ok()
    {
        bail!("unbound recovery adopted a foreign same-name process");
    }
    if runtime
        .find_container_id(&profile.spec().name)
        .await?
        .as_deref()
        != Some(foreign.as_str())
        || !DockerRuntimeManager::extract_state(&runtime.inspect_container(&foreign).await?)?
            .running
    {
        bail!("foreign name collision was mutated");
    }
    for id in [&foreign, &gateway] {
        let value = runtime.inspect_container(id).await?;
        if DockerRuntimeManager::extract_labels(&value)
            .get("elixir.marketplace.application_http.fixture")
            != Some(&nonce)
        {
            bail!("refusing foreign fixture cleanup");
        }
        runtime
            .run_capture_with_timeout(
                &["rm".into(), "-f".into(), id.to_owned()],
                Some(DOCKER_PROBE_COMMAND_TIMEOUT),
            )
            .await?;
        if runtime.find_container_id(id).await?.is_some() {
            bail!("fixture cleanup unconfirmed");
        }
    }
    println!(
        "application HTTP native lifecycle PASS: image={image}, no network request, actual closed UID/cgroup/profile, original parent wait, foreign collision preserved; NOT VPN/grant/application data/playback proof"
    );
    Ok(())
}
