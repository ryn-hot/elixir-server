//! Test-only route fault. No host route or firewall command is issued.
use super::*;
use std::net::Ipv4Addr;

fn gateway(routes: &str, expected: &str) -> Result<Ipv4Addr> {
    ensure!(routes.len() <= 4096, "owned route response bounds");
    let routes: Vec<Value> = serde_json::from_str(routes)?;
    ensure!(routes.len() == 1, "one original default route required");
    let route = &routes[0];
    ensure!(
        route["dst"] == "default"
            && route["dev"] == "eth0"
            && route.get("multipath").is_none()
            && route.get("nhid").is_none()
            && route["gateway"].as_str() == Some(expected),
        "owned uplink default route differs"
    );
    let value: Ipv4Addr = expected.parse()?;
    ensure!(
        !value.is_unspecified()
            && !value.is_loopback()
            && !value.is_multicast()
            && !value.is_broadcast(),
        "invalid original uplink gateway"
    );
    Ok(value)
}

#[test]
fn route_fault_requires_one_exact_original_ipv4_uplink_gateway() -> Result<()> {
    let valid = json!([{"dst":"default","dev":"eth0","gateway":"172.18.0.1","flags":[]}]);
    assert_eq!(
        gateway(&valid.to_string(), "172.18.0.1")?,
        "172.18.0.1".parse::<Ipv4Addr>()?
    );
    for changed in [
        json!([]),
        json!([valid[0], valid[0]]),
        json!([{"dst":"default","dev":"lo","gateway":"172.18.0.1"}]),
        json!([{"dst":"default","dev":"eth0","gateway":"172.18.0.2"}]),
        json!([{"dst":"default","dev":"eth0","gateway":"172.18.0.1","nhid":1}]),
        json!([{"dst":"default","dev":"eth0","gateway":"172.18.0.1","multipath":[]}]),
    ] {
        assert!(gateway(&changed.to_string(), "172.18.0.1").is_err());
    }
    for address in [
        "0.0.0.0",
        "127.0.0.1",
        "224.0.0.1",
        "255.255.255.255",
        "::1",
        "not-an-address",
    ] {
        assert!(
            gateway(
                &json!([{"dst":"default","dev":"eth0","gateway":address}]).to_string(),
                address
            )
            .is_err()
        );
    }
    Ok(())
}

pub(super) async fn apply(actor: &Participant, image: &str) -> Result<()> {
    let ids = docker(&[
        "ps",
        "--quiet",
        "--no-trunc",
        "--filter",
        &format!("label=elixir.deployment_id={}", actor.rig.deployment),
        "--filter",
        "label=elixir.marketplace.isolation=provider-v1",
    ])
    .await?;
    let ids: Vec<_> = ids.lines().filter(|v| !v.is_empty()).collect();
    ensure!(
        ids.len() == 1 && crate::runtime::packet_network::immutable_id(ids[0]),
        "original route-fault provider missing or ambiguous"
    );
    let id = ids[0];
    let inspected: Value = serde_json::from_str(&docker(&["inspect", id]).await?)?;
    let value = &inspected[0];
    ensure!(
        value["Id"] == id
            && value["Image"] == image
            && value["State"]["Running"] == true
            && value["Config"]["Labels"]["elixir.managed"] == "true"
            && value["Config"]["Labels"]["elixir.deployment_id"] == actor.rig.deployment,
        "route fault cannot select another actor"
    );
    let networks = value["NetworkSettings"]["Networks"]
        .as_object()
        .context("original uplink missing")?;
    ensure!(
        networks.len() == 1,
        "route fault requires one original uplink"
    );
    let expected = networks.values().next().unwrap()["Gateway"]
        .as_str()
        .context("original uplink gateway missing")?;
    let original = docker(&[
        "exec",
        "--user=0:0",
        id,
        "/usr/sbin/ip",
        "-j",
        "-4",
        "route",
        "show",
        "table",
        "main",
        "default",
    ])
    .await?;
    let gateway = gateway(&original, expected)?.to_string();
    docker(&[
        "exec",
        "--user=0:0",
        id,
        "/usr/sbin/ip",
        "-4",
        "route",
        "del",
        "default",
        "via",
        &gateway,
        "dev",
        "eth0",
    ])
    .await?;
    let after = docker(&[
        "exec",
        "--user=0:0",
        id,
        "/usr/sbin/ip",
        "-j",
        "-4",
        "route",
        "show",
        "table",
        "main",
        "default",
    ])
    .await?;
    ensure!(
        serde_json::from_str::<Vec<Value>>(&after)?.is_empty(),
        "original default route was not removed"
    );
    Ok(())
}
