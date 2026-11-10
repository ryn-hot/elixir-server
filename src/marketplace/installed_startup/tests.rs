use super::*;
use serde_json::{Value, json};
fn profile() -> Value {
    let a = super::super::application::fixture_allocation();
    json!({"schema_version":1,"worker_image":format!("sha256:{}","1".repeat(64)),
        "installations":{"entries":a.installations.entries,"bytes":a.installations.bytes},
        "source":{"memory_bytes":a.source.memory_bytes,"cpu_millicores":a.source.cpu_millicores,"disk_bytes":a.source.disk_bytes,"input_bytes":a.source.input_bytes,"output_bytes":a.source.output_bytes,"wall_seconds":a.source.wall_seconds},
        "retention":{"bytes":a.retention.bytes,"releases":a.retention.releases},"execution":a.execution})
}
#[test]
fn installed_profile_is_closed_pinned_and_independent_of_network_configuration() -> Result<()> {
    assert!(
        crate::config::Settings::default()
            .marketplace
            .installed_startup_file
            .is_none()
    );
    Profile::parse(&serde_json::to_vec(&profile())?)?;
    let original = Profile::parse(&serde_json::to_vec(&profile())?)?;
    assert!(original.application_http.is_none());
    assert!(original.compiled.is_none());
    assert!(compiled::select(None)?.is_none());
    for family in ["ipv4", "ipv6"] {
        let mut configured = profile();
        configured["application_http"] = json!({"family":family});
        assert!(
            Profile::parse(&serde_json::to_vec(&configured)?)?
                .application_http
                .is_some()
        );
    }
    for invalid in [
        json!({}),
        json!({"family":"direct"}),
        json!({"family":"ipv4","network_id":"host"}),
        json!({"family":"ipv4","protected":true}),
    ] {
        let mut configured = profile();
        configured["application_http"] = invalid;
        assert!(Profile::parse(&serde_json::to_vec(&configured)?).is_err());
    }
    for (key, value) in [
        ("schema_version", json!(2)),
        ("worker_image", json!("mutable:latest")),
        ("path", json!("/other")),
        ("provider", json!({})),
        ("issuer_keys", json!([])),
        ("native_ready", json!(true)),
    ] {
        let mut changed = profile();
        changed[key] = value;
        assert!(Profile::parse(&serde_json::to_vec(&changed)?).is_err());
    }
    for field in ["source", "retention", "execution", "installations"] {
        let mut changed = profile();
        changed[field]["unknown"] = json!(true);
        assert!(Profile::parse(&serde_json::to_vec(&changed)?).is_err());
    }
    let mut changed = profile();
    changed["installations"]["entries"] = json!(0);
    assert!(Profile::parse(&serde_json::to_vec(&changed)?).is_err());
    for field in [
        "memory_bytes",
        "cpu_millicores",
        "disk_bytes",
        "input_bytes",
        "output_bytes",
        "wall_seconds",
    ] {
        let mut changed = profile();
        changed["source"][field] = json!(0);
        assert!(Profile::parse(&serde_json::to_vec(&changed)?).is_err());
    }
    assert!(Profile::parse(b"{\"schema_version\":1,\"schema_version\":1}").is_err());
    Ok(())
}

fn compiled_profile() -> Value {
    json!({
        "guardian":{"executable":"/approved/guardian","sha256":elixir_marketplace::identity::Digest::of(b"guardian"),"platform":"linux/amd64"},
        "importer":{"executable":"/approved/importer","sha256":elixir_marketplace::identity::Digest::of(b"importer"),"local_socket":"/approved/docker.sock"},
        "toolchains":[elixir_marketplace::identity::Digest::of(b"reviewed-compiler")]
    })
}

#[test]
fn compiled_startup_is_an_explicit_closed_local_operator_selection() -> Result<()> {
    for platform in ["linux/amd64", "linux/arm64"] {
        for cached in [false, true] {
            let mut data = profile();
            data["compiled"] = compiled_profile();
            data["compiled"]["guardian"]["platform"] = json!(platform);
            if cached {
                data["compiled"]["retain_image_cache"] = json!(true);
            }
            let parsed = Profile::parse(&serde_json::to_vec(&data)?);
            assert_eq!(parsed.is_ok(), cfg!(unix));
            #[cfg(unix)]
            assert!(parsed?.compiled.is_some());
        }
    }
    for (pointer, value) in [
        ("/guardian/executable", json!("relative/guardian")),
        ("/guardian/platform", json!("windows/amd64")),
        ("/guardian/sha256", json!("mutable:latest")),
        ("/importer/executable", json!("https://host/importer")),
        ("/importer/local_socket", json!("tcp://127.0.0.1:2375")),
        ("/importer/local_socket", json!("/approved/socket\nother")),
        (
            "/importer/local_socket",
            json!(format!("/{}", "x".repeat(4096))),
        ),
        ("/toolchains", json!([])),
        ("/toolchains", json!(["mutable:latest"])),
        (
            "/toolchains",
            json!([
                elixir_marketplace::identity::Digest::of(b"same"),
                elixir_marketplace::identity::Digest::of(b"same")
            ]),
        ),
        (
            "/toolchains",
            json!(
                (0u8..9)
                    .map(|n| elixir_marketplace::identity::Digest::of([n]))
                    .collect::<Vec<_>>()
            ),
        ),
    ] {
        let mut data = profile();
        data["compiled"] = compiled_profile();
        *data["compiled"].pointer_mut(pointer).unwrap() = value;
        assert!(
            Profile::parse(&serde_json::to_vec(&data)?).is_err(),
            "{pointer}"
        );
    }
    for parent in ["", "/guardian", "/importer"] {
        let mut data = profile();
        data["compiled"] = compiled_profile();
        data["compiled"].pointer_mut(parent).unwrap()["untrusted_override"] = json!(true);
        assert!(
            Profile::parse(&serde_json::to_vec(&data)?).is_err(),
            "{parent}"
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn compiled_profile_parsing_cannot_approve_missing_native_helpers() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut data = profile();
    data["compiled"] = compiled_profile();
    data["compiled"]["guardian"]["executable"] = json!(root.path().join("missing-guardian"));
    data["compiled"]["importer"]["executable"] = json!(root.path().join("missing-importer"));
    data["compiled"]["importer"]["local_socket"] = json!(root.path().join("missing-socket"));
    let parsed = Profile::parse(&serde_json::to_vec(&data)?)?;
    assert!(compiled::select(parsed.compiled.as_ref()).is_err());
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}
