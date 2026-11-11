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

fn table(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .or_else(|| value.as_str().and_then(|v| v.parse().ok()))
}

fn tunnel_default(bytes: &str) -> Result<u32> {
    ensure!(bytes.len() <= 16384, "owned tunnel-route response bounds");
    let rows: Vec<Value> = serde_json::from_str(bytes)?;
    ensure!(rows.len() <= 64, "owned default-route count bounds");
    let selected: Vec<_> = rows
        .iter()
        .filter(|row| row["dst"] == "default" && row["dev"] == "CloudflareWARP")
        .collect();
    ensure!(
        selected.len() == 1,
        "one exact original VPN default route required"
    );
    let route = selected[0];
    let table = table(&route["table"]).context("numeric original VPN table required")?;
    ensure!(
        table != 0
            && !matches!(table, 253..=255)
            && route.get("gateway").is_none()
            && route.get("multipath").is_none()
            && route.get("nhid").is_none(),
        "unsupported original VPN default route"
    );
    Ok(table)
}

fn worker_route(
    rows: &Value,
    interface: &str,
    expected_table: u32,
    gateway: Option<&str>,
) -> Result<()> {
    let rows = rows.as_array().context("actual worker route missing")?;
    ensure!(
        rows.len() == 1
            && rows[0]["dev"] == interface
            && rows[0]["dst"] == "1.1.1.1"
            && table(&rows[0]["uid"]) == Some(10004),
        "actual worker route differs"
    );
    let row = &rows[0];
    let observed_table = match row.get("table") {
        None => Some(254),
        Some(Value::String(name)) if name == "main" => Some(254),
        Some(value) => table(value),
    };
    ensure!(
        observed_table == Some(expected_table) && row["gateway"].as_str() == gateway,
        "actual worker route table/gateway differs"
    );
    Ok(())
}

#[test]
fn vpn_route_fault_requires_exact_tunnel_and_observed_physical_fallthrough() -> Result<()> {
    let route = json!({"dst":"default","dev":"CloudflareWARP","table":65743,"flags":[]});
    assert_eq!(tunnel_default(&json!([route]).to_string())?, 65743);
    let mut numeric_string = route.clone();
    numeric_string["table"] = json!("65743");
    assert_eq!(tunnel_default(&json!([numeric_string]).to_string())?, 65743);
    for bad in [
        json!([]),
        json!([route, route]),
        json!([{"dst":"default","dev":"eth0","table":65743}]),
        json!([{"dst":"default","dev":"CloudflareWARP","table":"main"}]),
        json!([{"dst":"default","dev":"CloudflareWARP","table":254}]),
        json!([{"dst":"default","dev":"CloudflareWARP","table":0}]),
        json!([{"dst":"default","dev":"CloudflareWARP","table":65743,"multipath":[]}]),
    ] {
        assert!(tunnel_default(&bad.to_string()).is_err());
    }
    let physical = json!([{"dst":"1.1.1.1","dev":"eth0","gateway":"172.18.0.1","uid":10004}]);
    worker_route(&physical, "eth0", 254, Some("172.18.0.1"))?;
    let protected = json!([{"dst":"1.1.1.1","dev":"CloudflareWARP","table":65743,"uid":10004}]);
    worker_route(&protected, "CloudflareWARP", 65743, None)?;
    assert!(worker_route(&protected, "eth0", 254, Some("172.18.0.1")).is_err());
    assert!(worker_route(&physical, "eth0", 254, Some("172.18.0.2")).is_err());
    let mut wrong_uid = physical.clone();
    wrong_uid[0]["uid"] = json!(0);
    assert!(worker_route(&wrong_uid, "eth0", 254, Some("172.18.0.1")).is_err());
    assert!(
        worker_route(
            &json!([physical[0], physical[0]]),
            "eth0",
            254,
            Some("172.18.0.1")
        )
        .is_err()
    );
    Ok(())
}

async fn vpn_fault(id: &str, original: &str, expected: &str) -> Result<()> {
    let defaults = docker(&[
        "exec",
        "--user=0:0",
        id,
        "/usr/sbin/ip",
        "-N",
        "-j",
        "-4",
        "route",
        "show",
        "table",
        "all",
        "default",
    ])
    .await?;
    let selected = tunnel_default(&defaults)?;
    let port = elixir_marketplace::network_probe::ROUTE_PROBE_SOURCE_PORT.to_string();
    let before = docker(&[
        "exec",
        "--user=0:0",
        id,
        "/usr/sbin/ip",
        "-N",
        "-j",
        "-4",
        "route",
        "get",
        "1.1.1.1",
        "mark",
        "0",
        "uid",
        "10004",
        "ipproto",
        "tcp",
        "sport",
        &port,
        "dport",
        "443",
    ])
    .await?;
    worker_route(
        &serde_json::from_str::<Value>(&before)?,
        "CloudflareWARP",
        selected,
        None,
    )?;
    // One exact-container exec observes the fault and fallthrough together,
    // before ordinary shutdown can retire the original provider. No files or
    // firewall rules are changed. Private addresses stay in bounded memory.
    let script = r#"set -euo pipefail
task_table="$1"
task_port="$2"
/usr/sbin/ip -4 route del default dev CloudflareWARP table "$task_table"
task_physical="$(/usr/sbin/ip -N -j -4 route show table main default)"
task_worker="$(/usr/sbin/ip -N -j -4 route get 1.1.1.1 mark 0 uid 10004 ipproto tcp sport "$task_port" dport 443)"
printf '{"physical":%s,"worker":%s}\n' "$task_physical" "$task_worker"
"#;
    let proof = docker(&[
        "exec",
        "--user=0:0",
        id,
        "/bin/bash",
        "-c",
        script,
        "owned-vpn-route-fault",
        &selected.to_string(),
        &port,
    ])
    .await?;
    ensure!(proof.len() <= 8192, "owned route-fallthrough proof bounds");
    let proof: Value = serde_json::from_str(&proof)?;
    ensure!(
        proof["physical"] == serde_json::from_str::<Value>(original)?,
        "VPN fault changed physical default route"
    );
    worker_route(&proof["worker"], "eth0", 254, Some(expected))?;
    Ok(())
}

pub(super) async fn apply(actor: &Participant, image: &str, vpn_route: bool) -> Result<()> {
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
        "-N",
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
    if vpn_route {
        vpn_fault(id, &original, &gateway).await?;
        return Ok(());
    }
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
