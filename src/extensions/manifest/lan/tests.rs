use super::*;
use serde_json::{Value, json};

fn fields() -> Value {
    json!({"owned_settings":[
        {"id":"serviceUrl","label":"Service URL","type":"password","secret":true,"default":null,
            "ownership":"managed","storage":{"type":"instance_secret","key":"base_url"},
            "lan_endpoint":{"enabled_by":"privateNetwork","access":"origin"}},
        {"id":"privateNetwork","label":"Local network access","type":"toggle","secret":false,
            "default":false,"storage":{"type":"instance_setting","key":"privateNetwork"}}
    ]})
}
fn permissions() -> Vec<String> {
    vec![
        "network.egress".into(),
        LIVE_NETWORK_PRIVATE_PERMISSION.into(),
    ]
}
fn check(value: Value) -> Result<()> {
    let surface: ManifestControlSurface = serde_json::from_value(value)?;
    surface.validate()?;
    validate(&surface, &permissions())
}

#[test]
fn local_endpoint_contract_is_explicit_closed_and_default_off() -> Result<()> {
    check(fields())?;
    let mut exact = fields();
    exact["owned_settings"][0]["lan_endpoint"]["access"] = json!("exact_url");
    check(exact)?;
    let mut unknown = fields();
    unknown["owned_settings"][0]["lan_endpoint"]["arbitrary_private_hosts"] = json!(true);
    assert!(check(unknown).is_err());
    let mut unknown_mode = fields();
    unknown_mode["owned_settings"][0]["lan_endpoint"]["access"] = json!("any_lan");
    assert!(check(unknown_mode).is_err());
    Ok(())
}

#[test]
fn endpoint_cannot_infer_consent_from_defaults_globals_aliases_or_unrelated_values() {
    for (pointer, value) in [
        ("/owned_settings/0/default", json!("http://10.0.0.1/")),
        ("/owned_settings/0/ownership", json!("seeded")),
        ("/owned_settings/0/type", json!("toggle")),
        ("/owned_settings/0/storage/type", json!("global_secret")),
        ("/owned_settings/0/storage/type", json!("extension_setting")),
        (
            "/owned_settings/0/lan_endpoint/enabled_by",
            json!("serviceUrl"),
        ),
        (
            "/owned_settings/0/lan_endpoint/enabled_by",
            json!("undeclared"),
        ),
        ("/owned_settings/1/default", json!(true)),
        ("/owned_settings/1/default", Value::Null),
        ("/owned_settings/1/type", json!("text")),
        ("/owned_settings/1/secret", json!(true)),
        ("/owned_settings/1/storage/type", json!("extension_setting")),
    ] {
        let mut value_set = fields();
        *value_set
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("missing fixture {pointer}")) = value;
        assert!(
            check(value_set).is_err(),
            "invalid declaration at {pointer}"
        );
    }
    let mut alias = fields();
    let mut shadow = alias["owned_settings"][1].clone();
    shadow["id"] = json!("anotherToggle");
    alias["owned_settings"].as_array_mut().unwrap().push(shadow);
    assert!(check(alias).is_err());
    let mut alias = fields();
    let mut shadow = alias["owned_settings"][0].clone();
    shadow["id"] = json!("anotherUrl");
    alias["owned_settings"].as_array_mut().unwrap().push(shadow);
    assert!(check(alias).is_err());
}

#[test]
fn endpoint_count_and_both_permissions_are_bounded_independently() -> Result<()> {
    let surface: ManifestControlSurface = serde_json::from_value(fields())?;
    for permissions in [
        vec![],
        vec!["network.egress".into()],
        vec![LIVE_NETWORK_PRIVATE_PERMISSION.into()],
    ] {
        assert!(validate(&surface, &permissions).is_err());
    }
    let mut value = fields();
    for number in 1..MAX_LAN_ENDPOINTS {
        let mut endpoint = value["owned_settings"][0].clone();
        endpoint["id"] = json!(format!("url{number}"));
        endpoint["storage"]["key"] = json!(format!("url{number}"));
        value["owned_settings"]
            .as_array_mut()
            .unwrap()
            .push(endpoint);
    }
    check(value.clone())?;
    let mut ninth = value["owned_settings"][0].clone();
    ninth["id"] = json!("ninth");
    ninth["storage"]["key"] = json!("ninth");
    value["owned_settings"].as_array_mut().unwrap().push(ninth);
    assert!(check(value).is_err());
    Ok(())
}

#[test]
fn ordinary_controls_keep_their_previous_serialized_contract() -> Result<()> {
    let mut ordinary = fields();
    ordinary["owned_settings"][0]
        .as_object_mut()
        .unwrap()
        .remove("lan_endpoint");
    let surface: ManifestControlSurface = serde_json::from_value(ordinary)?;
    validate(&surface, &[])?;
    let serialized = serde_json::to_value(&surface)?;
    assert!(
        !serialized["owned_settings"][0]
            .as_object()
            .unwrap()
            .contains_key("lan_endpoint")
    );
    assert!(
        !serialized["owned_settings"][1]
            .as_object()
            .unwrap()
            .contains_key("lan_endpoint")
    );
    Ok(())
}
