//! OCI/configuration observations only; no VPN or native-source authority.
use super::*;
use crate::{
    live::{config::LiveEgressProfileKind, egress::prepare_gateway_material_from_secret_values},
    network::protection::DownloadNetworkProfileKind,
};
use serde_json::json;

async fn profile() -> Result<ApplicationGatewayProfile> {
    let files=prepare_gateway_material_from_secret_values(LiveEgressProfileKind::Wireguard,
        b"[Interface]\nPrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=\nAddress = 10.64.0.2/32\n[Peer]\nPublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=\nAllowedIPs = 0.0.0.0/0\nEndpoint = 1.1.1.1:51820\n",None,None).await?;
    ApplicationGatewayProfile::fixture(
        "fixture",
        &"b".repeat(64),
        &format!("sha256:{}", "a".repeat(64)),
        &DownloadNetworkProfileKind::WireguardConfig,
        &files,
    )
}
pub(in crate::runtime::docker) fn fixture(p: &impl TunnelActor) -> (ContainerHandle, Value, Value) {
    let s = p.spec();
    let h = ContainerHandle {
        id: "c".repeat(64),
        name: s.name.clone(),
    };
    let mut image = json!({"Id":s.image,"Config":{"Entrypoint":[p.image_entrypoint()],"Cmd":null,
        "Volumes":null,"Env":["PATH=/usr/bin"],"WorkingDir":"/"}});
    if p.provider() {
        image["Config"]["Healthcheck"] = json!({"Test":["NONE"]});
        image["Config"]["Labels"] = json!({
        "elixir.marketplace.provider.schema":"1","elixir.marketplace.provider.state":"opaque-private-v1",
        "elixir.marketplace.provider.mode":"tunnel_only","elixir.marketplace.provider.protocol":"MASQUE",
        "elixir.marketplace.provider.masque_options":"h3-only",
        "elixir.marketplace.provider.connectivity_checks":"supervisor"});
    }
    let env: Vec<_> = std::iter::once("PATH=/usr/bin".to_owned())
        .chain(s.env.iter().map(|v| format!("{}={}", v.name, v.value)))
        .collect();
    let tmpfs: serde_json::Map<_, _> = s
        .security
        .tmpfs
        .iter()
        .map(|m| {
            let text = format_tmpfs(m);
            let (path, options) = text.split_once(':').unwrap();
            (path.to_owned(), Value::String(options.into()))
        })
        .collect();
    let mut value = json!({"Id":h.id,"Name":format!("/{}",h.name),"Image":s.image,
        "Config":{"Image":s.image,"Cmd":s.command,"Entrypoint":[p.entrypoint()],"User":"0:0",
            "Env":env,"OpenStdin":true,"Tty":false,"WorkingDir":"/","Labels":s.labels},
        "State":{"Status":"created","Running":false,"Paused":false,"Restarting":false,"Pid":0},
        "HostConfig":{"NetworkMode":s.network,"ReadonlyRootfs":true,"Privileged":false,
            "SecurityOpt":["no-new-privileges:true"],"CapAdd":s.cap_add,"CapDrop":["ALL"],
            "Tmpfs":tmpfs,"Memory":s.security.memory_limit_mb.unwrap()*1024*1024,"MemorySwap":s.security.memory_swap_limit_mb.unwrap()*1024*1024,
            "NanoCpus":if p.provider(){200_000_000}else{100_000_000},"PidsLimit":64,"ShmSize":1024*1024,
            "Ulimits":[{"Name":"core","Soft":0,"Hard":0},{"Name":"nofile","Soft":256,"Hard":256},
                {"Name":"fsize","Soft":s.security.file_size_limit_bytes.unwrap(),"Hard":s.security.file_size_limit_bytes.unwrap()}],
            "Init":s.security.init_process,"PidMode":"","IpcMode":"private","UsernsMode":"","RestartPolicy":{"Name":"no"},
            "PublishAllPorts":false,"LogConfig":{"Type":"none","Config":{}},"Sysctls":s.sysctls,
            "Devices":[{"PathOnHost":"/dev/net/tun","PathInContainer":"/dev/net/tun","CgroupPermissions":"rwm"}],
            "Binds":null,"Mounts":null,"DeviceRequests":null,"PortBindings":{},"Links":null,
            "ExtraHosts":null,"Dns":null,"DnsSearch":null,"DnsOptions":null},
        "Mounts":[],"NetworkSettings":{"Networks":{"fixture":{"NetworkID":s.network,"EndpointID":"d".repeat(64)}},"Ports":{}}});
    if p.provider() {
        value["Config"]["Healthcheck"] = json!({"Test":["NONE"]});
        value["Config"]["Cmd"] = Value::Null;
    }
    (h, value, image)
}

#[tokio::test]
async fn gateway_native_configuration_rejects_identity_resource_mount_command_and_environment_drift()
-> Result<()> {
    let p = profile().await?;
    let (h, good, image) = fixture(&p);
    DockerRuntimeManager::verify_application_gateway_configuration(&good, &h, &p, &image)?;
    for (path, value) in [
        ("/Id", json!("e".repeat(64))),
        ("/Name", json!("/elsewhere")),
        ("/Image", json!("foreign:tag")),
        ("/Config/Cmd", json!(["sh"])),
        ("/Config/Env", json!(["LD_PRELOAD=/evil"])),
        ("/Config/Entrypoint", json!(["/bin/sh"])),
        ("/Config/User", json!("1000")),
        ("/HostConfig/NetworkMode", json!("host")),
        ("/HostConfig/ReadonlyRootfs", json!(false)),
        ("/HostConfig/Privileged", json!(true)),
        ("/HostConfig/CapAdd", json!(["SYS_ADMIN"])),
        ("/HostConfig/Tmpfs", json!({})),
        ("/HostConfig/Memory", json!(0)),
        ("/HostConfig/MemorySwap", json!(-1)),
        ("/HostConfig/NanoCpus", json!(0)),
        ("/HostConfig/PidsLimit", json!(-1)),
        ("/HostConfig/Ulimits", json!([])),
        ("/HostConfig/Init", json!(false)),
        ("/HostConfig/LogConfig/Type", json!("json-file")),
        ("/HostConfig/Binds", json!(["/tmp:/tmp"])),
        ("/HostConfig/DeviceRequests", json!({})),
        ("/HostConfig/Devices", json!([])),
        ("/HostConfig/Sysctls", json!({})),
        ("/HostConfig/RestartPolicy/Name", json!("always")),
        ("/HostConfig/PublishAllPorts", json!(true)),
        (
            "/Mounts",
            json!([{"Type":"volume","Destination":"/gluetun","RW":true}]),
        ),
    ] {
        let mut bad = good.clone();
        *bad.pointer_mut(path).unwrap() = value;
        assert!(
            DockerRuntimeManager::verify_application_gateway_configuration(&bad, &h, &p, &image)
                .is_err(),
            "{path}"
        );
    }
    Ok(())
}
#[tokio::test]
async fn gateway_created_empty_endpoint_is_not_running_network_or_readiness_proof() -> Result<()> {
    let p = profile().await?;
    let (h, mut value, image) = fixture(&p);
    value["NetworkSettings"]["Networks"]["fixture"]["NetworkID"] = json!("");
    value["NetworkSettings"]["Networks"]["fixture"]["EndpointID"] = json!("");
    DockerRuntimeManager::verify_application_gateway_configuration(&value, &h, &p, &image)?;
    value["State"]["Status"] = json!("running");
    value["State"]["Running"] = json!(true);
    assert!(
        DockerRuntimeManager::verify_application_gateway_configuration(&value, &h, &p, &image)
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn gateway_image_and_actual_environment_cannot_add_hooks_credentials_or_anonymous_state()
-> Result<()> {
    let p = profile().await?;
    let (h, good, image) = fixture(&p);
    for (path, value) in [
        ("/Id", json!("wrong")),
        ("/Config/Entrypoint", json!(["elsewhere"])),
        ("/Config/Volumes", json!({"/state":{}})),
        ("/Config/Cmd", json!(["extra"])),
        (
            "/Config/Env",
            json!(["PATH=/usr/bin", "OPENVPN_PASSWORD=embedded"]),
        ),
        ("/Config/Env", json!(["PATH=/usr/bin", "LD_PRELOAD=/hook"])),
        ("/Config/Env", json!(["PATH=/usr/bin", "PATH=/elsewhere"])),
    ] {
        let mut bad = image.clone();
        *bad.pointer_mut(path).unwrap() = value;
        assert!(
            DockerRuntimeManager::verify_application_gateway_configuration(&good, &h, &p, &bad)
                .is_err(),
            "{path}"
        );
    }
    Ok(())
}
#[tokio::test]
async fn gateway_stop_can_retire_configuration_drift_but_requires_positive_native_state()
-> Result<()> {
    let p = profile().await?;
    let (h, mut good, image) = fixture(&p);
    good["HostConfig"]["Memory"] = json!(0);
    assert!(
        DockerRuntimeManager::verify_application_gateway_configuration(&good, &h, &p, &image)
            .is_err()
    );
    DockerRuntimeManager::require_application_gateway_owner(&good, &h, &p)?;
    assert!(DockerRuntimeManager::application_gateway_stopped(&good));
    for (path, value) in [
        ("/State/Pid", json!(1)),
        ("/State/Running", Value::Null),
        ("/State/Paused", json!(true)),
        ("/State/Restarting", json!(true)),
    ] {
        let mut bad = good.clone();
        *bad.pointer_mut(path).unwrap() = value;
        assert!(!DockerRuntimeManager::application_gateway_stopped(&bad));
    }
    Ok(())
}
