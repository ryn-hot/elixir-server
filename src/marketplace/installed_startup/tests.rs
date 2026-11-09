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
