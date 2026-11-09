use super::*;
use serde_json::json;
#[test]
fn provider_native_profile_rejects_host_state_hooks_caps_limits_and_wrong_image_protocol()
-> Result<()> {
    let profile = Profile::fixture(
        "provider-fixture",
        &format!("sha256:{}", "a".repeat(64)),
        &"b".repeat(64),
    )?;
    let (handle, good, image) = super::super::application_gateway::tests::fixture(&profile);
    container_resource_args(&profile.spec().security)?;
    DockerRuntimeManager::verify_application_gateway_configuration(
        &good, &handle, &profile, &image,
    )?;
    // Docker materializes an empty inherited WORKDIR even when the image's
    // config omits it. Only absent/null/empty are equivalent, never a new path.
    for inherited in [None, Some(Value::Null), Some(json!(""))] {
        for materialized in [None, Some(Value::Null), Some(json!(""))] {
            let mut inherited_image = image.clone();
            let config = inherited_image["Config"].as_object_mut().unwrap();
            config.remove("WorkingDir");
            if let Some(value) = inherited.clone() {
                config.insert("WorkingDir".into(), value);
            }
            let mut empty_native = good.clone();
            let config = empty_native["Config"].as_object_mut().unwrap();
            config.remove("WorkingDir");
            if let Some(value) = materialized {
                config.insert("WorkingDir".into(), value);
            }
            DockerRuntimeManager::verify_application_gateway_configuration(
                &empty_native,
                &handle,
                &profile,
                &inherited_image,
            )?;
            assert!(
                DockerRuntimeManager::verify_application_gateway_configuration(
                    &good,
                    &handle,
                    &profile,
                    &inherited_image,
                )
                .is_err(),
                "nonempty WORKDIR must not normalize to empty"
            );
        }
    }
    for bad_workdir in [
        json!(false),
        json!([]),
        json!("\0"),
        json!("x".repeat(4097)),
    ] {
        let mut bad = image.clone();
        bad["Config"]["WorkingDir"] = bad_workdir;
        assert!(
            DockerRuntimeManager::verify_application_gateway_configuration(
                &good, &handle, &profile, &bad,
            )
            .is_err()
        );
    }
    for (path, value) in [
        ("/Id", json!("c".repeat(63))),
        ("/Config/Cmd", json!(["/entrypoint.sh"])),
        ("/Config/Entrypoint", json!(["/entrypoint.sh"])),
        ("/Config/WorkingDir", json!("/tmp")),
        ("/Config/Healthcheck/Test", json!(["CMD", "sh"])),
        (
            "/Config/Env",
            json!(["PATH=/usr/bin", "WARP_LICENSE_KEY=paid"]),
        ),
        ("/HostConfig/Init", json!(false)),
        ("/HostConfig/Memory", json!(0)),
        ("/HostConfig/MemorySwap", json!(-1)),
        ("/HostConfig/NanoCpus", json!(0)),
        ("/HostConfig/PidsLimit", json!(-1)),
        ("/HostConfig/PidMode", json!("host")),
        ("/HostConfig/IpcMode", json!("host")),
        ("/HostConfig/NetworkMode", json!("host")),
        ("/HostConfig/Privileged", json!(true)),
        ("/HostConfig/CapAdd", json!(["SYS_ADMIN"])),
        ("/HostConfig/Binds", json!(["/tmp:/tmp"])),
        ("/HostConfig/Tmpfs", json!({})),
        ("/HostConfig/Devices", json!([])),
        ("/HostConfig/Ulimits", json!([])),
        (
            "/HostConfig/PortBindings",
            json!({"443/tcp":[{"HostPort":"443"}]}),
        ),
    ] {
        let mut bad = good.clone();
        *bad.pointer_mut(path).unwrap() = value;
        assert!(
            DockerRuntimeManager::verify_application_gateway_configuration(
                &bad, &handle, &profile, &image
            )
            .is_err(),
            "{path}"
        );
    }
    for (path, value) in [
        ("/Config/Entrypoint", json!(["/entrypoint.sh"])),
        ("/Config/Cmd", json!(["extra"])),
        ("/Config/Volumes", json!({"/state":{}})),
        ("/Config/Healthcheck/Test", json!(["CMD", "healthcheck"])),
        (
            "/Config/Labels/elixir.marketplace.provider.mode",
            json!("warp+doh"),
        ),
        (
            "/Config/Labels/elixir.marketplace.provider.state",
            json!("unknown"),
        ),
        (
            "/Config/Labels/elixir.marketplace.provider.masque_options",
            json!("h3-with-h2-fallback"),
        ),
        (
            "/Config/Labels/elixir.marketplace.provider.masque_options",
            Value::Null,
        ),
        (
            "/Config/Labels/elixir.marketplace.provider.connectivity_checks",
            json!("vendor"),
        ),
    ] {
        let mut bad = image.clone();
        *bad.pointer_mut(path).unwrap() = value;
        assert!(
            DockerRuntimeManager::verify_application_gateway_configuration(
                &good, &handle, &profile, &bad
            )
            .is_err(),
            "{path}"
        );
    }
    Ok(())
}
