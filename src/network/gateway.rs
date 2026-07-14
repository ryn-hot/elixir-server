use std::collections::HashMap;

use anyhow::{Result, bail};

use crate::runtime::model::{
    ContainerSpec, EnvVar, VolumeMount, VolumeMountSourceKind, apply_container_spec_fingerprint,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayRuntime {
    None,
    GluetunWireguard(GluetunWireguardGatewayRuntime),
    GluetunOpenvpn(GluetunOpenvpnGatewayRuntime),
    CloudflareWarp(CloudflareWarpGatewayRuntime),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GluetunWireguardGatewayRuntime {
    pub image: String,
    pub config_host_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GluetunOpenvpnGatewayRuntime {
    pub image: String,
    pub config_host_path: String,
    pub auth_host_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudflareWarpGatewayRuntime {
    pub image: String,
    pub state_volume_name: String,
    pub enrollment_id: String,
    pub identity_secret_ref: String,
}

#[derive(Debug, Clone, Copy)]
pub struct GatewayTopologyProfile<'a> {
    pub id: &'a str,
    pub kind: &'a str,
    pub runtime: &'a GatewayRuntime,
}

#[derive(Debug, Clone, Copy)]
pub struct GatewayTopologyLabels<'a> {
    pub role: &'a str,
    pub profile_id: &'a str,
    pub profile_kind: &'a str,
    pub runtime_kind: &'a str,
    pub exposed_ports: &'a str,
}

#[derive(Debug, Clone)]
pub struct GatewayTopologyCompileInput<'a> {
    pub app_container_name: &'a str,
    pub app_spec: &'a ContainerSpec,
    pub base_labels: &'a HashMap<String, String>,
    pub labels: GatewayTopologyLabels<'a>,
    pub error_subject: &'a str,
}

#[derive(Debug, Clone)]
pub struct CompiledGatewayTopology {
    pub gateway_spec: Option<ContainerSpec>,
    pub protected_app_spec: ContainerSpec,
}

pub fn compile_gateway_topology(
    profile: GatewayTopologyProfile<'_>,
    input: GatewayTopologyCompileInput<'_>,
) -> Result<CompiledGatewayTopology> {
    match profile.runtime {
        GatewayRuntime::None => {
            let mut protected_app_spec = input.app_spec.clone();
            stamp_topology_labels(
                &mut protected_app_spec,
                profile,
                "direct",
                input.app_spec,
                input.labels,
            );
            Ok(CompiledGatewayTopology {
                gateway_spec: None,
                protected_app_spec,
            })
        }
        GatewayRuntime::GluetunWireguard(runtime) => compile_wireguard(runtime, profile, input),
        GatewayRuntime::GluetunOpenvpn(runtime) => compile_openvpn(runtime, profile, input),
        GatewayRuntime::CloudflareWarp(runtime) => compile_warp(runtime, profile, input),
    }
}

pub(crate) fn exposed_container_ports_label(spec: &ContainerSpec) -> String {
    let mut ports = spec
        .ports
        .iter()
        .map(|port| {
            format!(
                "{}/{}",
                port.container_port,
                port.protocol.as_deref().unwrap_or("tcp")
            )
        })
        .collect::<Vec<_>>();
    ports.sort();
    ports.join(",")
}

fn stamp_topology_labels(
    spec: &mut ContainerSpec,
    profile: GatewayTopologyProfile<'_>,
    runtime_kind: &str,
    source_app_spec: &ContainerSpec,
    labels: GatewayTopologyLabels<'_>,
) {
    spec.labels
        .insert(labels.profile_id.to_string(), profile.id.to_string());
    spec.labels
        .insert(labels.profile_kind.to_string(), profile.kind.to_string());
    spec.labels
        .insert(labels.runtime_kind.to_string(), runtime_kind.to_string());
    spec.labels.insert(
        labels.exposed_ports.to_string(),
        exposed_container_ports_label(source_app_spec),
    );
    apply_container_spec_fingerprint(spec);
}

fn compile_wireguard(
    runtime: &GluetunWireguardGatewayRuntime,
    profile: GatewayTopologyProfile<'_>,
    input: GatewayTopologyCompileInput<'_>,
) -> Result<CompiledGatewayTopology> {
    let image = required(
        runtime.image.as_str(),
        input.error_subject,
        profile.id,
        "empty gateway image",
    )?;
    let config_host_path = required(
        runtime.config_host_path.as_str(),
        input.error_subject,
        profile.id,
        "empty WireGuard config path",
    )?;
    let app_container_name = required(
        input.app_container_name,
        input.error_subject,
        profile.id,
        "empty app container name",
    )?;
    let gateway_name = format!("{app_container_name}-vpn");
    let mut labels = input.base_labels.clone();
    labels.insert(
        input.labels.role.to_string(),
        "wireguard_gateway".to_string(),
    );

    let mut sysctls = HashMap::new();
    sysctls.insert(
        "net.ipv4.conf.all.src_valid_mark".to_string(),
        "1".to_string(),
    );

    let mut gateway_env = vec![
        EnvVar {
            name: "VPN_SERVICE_PROVIDER".to_string(),
            value: "custom".to_string(),
        },
        EnvVar {
            name: "VPN_TYPE".to_string(),
            value: "wireguard".to_string(),
        },
        EnvVar {
            name: "WIREGUARD_CONF_FILE".to_string(),
            value: "wg0.conf".to_string(),
        },
        EnvVar {
            name: "FIREWALL_OUTBOUND_SUBNETS".to_string(),
            value: "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16".to_string(),
        },
    ];
    push_input_ports(&mut gateway_env, input.app_spec);

    let mut gateway_spec = ContainerSpec {
        name: gateway_name.clone(),
        image: image.to_string(),
        network: input.app_spec.network.clone(),
        network_mode: None,
        aliases: input.app_spec.aliases.clone(),
        env: gateway_env,
        volumes: vec![VolumeMount {
            source_kind: VolumeMountSourceKind::Bind,
            host_path: config_host_path.to_string(),
            container_path: "/gluetun/wireguard/wg0.conf".to_string(),
            read_only: true,
        }],
        ports: input.app_spec.ports.clone(),
        labels,
        command: Vec::new(),
        cap_add: vec!["NET_ADMIN".to_string()],
        cap_drop: Vec::new(),
        devices: vec!["/dev/net/tun:/dev/net/tun".to_string()],
        sysctls,
        security: Default::default(),
    };
    let mut protected_app_spec = namespace_app(input.app_spec, &gateway_name);
    stamp_topology_labels(
        &mut gateway_spec,
        profile,
        "gluetun_wireguard",
        input.app_spec,
        input.labels,
    );
    stamp_topology_labels(
        &mut protected_app_spec,
        profile,
        "gluetun_wireguard",
        input.app_spec,
        input.labels,
    );
    Ok(CompiledGatewayTopology {
        gateway_spec: Some(gateway_spec),
        protected_app_spec,
    })
}

fn compile_openvpn(
    runtime: &GluetunOpenvpnGatewayRuntime,
    profile: GatewayTopologyProfile<'_>,
    input: GatewayTopologyCompileInput<'_>,
) -> Result<CompiledGatewayTopology> {
    let image = required(
        runtime.image.as_str(),
        input.error_subject,
        profile.id,
        "empty OpenVPN gateway image",
    )?;
    let config_host_path = required(
        runtime.config_host_path.as_str(),
        input.error_subject,
        profile.id,
        "empty OpenVPN config path",
    )?;
    let app_container_name = required(
        input.app_container_name,
        input.error_subject,
        profile.id,
        "empty app container name",
    )?;
    let gateway_name = format!("{app_container_name}-vpn");
    let mut labels = input.base_labels.clone();
    labels.insert(input.labels.role.to_string(), "openvpn_gateway".to_string());

    let mut gateway_env = vec![
        EnvVar {
            name: "VPN_SERVICE_PROVIDER".to_string(),
            value: "custom".to_string(),
        },
        EnvVar {
            name: "VPN_TYPE".to_string(),
            value: "openvpn".to_string(),
        },
        EnvVar {
            name: "OPENVPN_CUSTOM_CONFIG".to_string(),
            value: "/gluetun/custom.conf".to_string(),
        },
        EnvVar {
            name: "FIREWALL_OUTBOUND_SUBNETS".to_string(),
            value: "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16".to_string(),
        },
    ];
    push_input_ports(&mut gateway_env, input.app_spec);

    let mut volumes = vec![VolumeMount {
        source_kind: VolumeMountSourceKind::Bind,
        host_path: config_host_path.to_string(),
        container_path: "/gluetun/custom.conf".to_string(),
        read_only: true,
    }];
    if let Some(auth_host_path) = runtime
        .auth_host_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        volumes.push(VolumeMount {
            source_kind: VolumeMountSourceKind::Bind,
            host_path: auth_host_path.to_string(),
            container_path: "/gluetun/auth.txt".to_string(),
            read_only: true,
        });
    }

    let mut gateway_spec = ContainerSpec {
        name: gateway_name.clone(),
        image: image.to_string(),
        network: input.app_spec.network.clone(),
        network_mode: None,
        aliases: input.app_spec.aliases.clone(),
        env: gateway_env,
        volumes,
        ports: input.app_spec.ports.clone(),
        labels,
        command: Vec::new(),
        cap_add: vec!["NET_ADMIN".to_string()],
        cap_drop: Vec::new(),
        devices: vec!["/dev/net/tun:/dev/net/tun".to_string()],
        sysctls: HashMap::new(),
        security: Default::default(),
    };
    let mut protected_app_spec = namespace_app(input.app_spec, &gateway_name);
    stamp_topology_labels(
        &mut gateway_spec,
        profile,
        "gluetun_openvpn",
        input.app_spec,
        input.labels,
    );
    stamp_topology_labels(
        &mut protected_app_spec,
        profile,
        "gluetun_openvpn",
        input.app_spec,
        input.labels,
    );
    Ok(CompiledGatewayTopology {
        gateway_spec: Some(gateway_spec),
        protected_app_spec,
    })
}

fn compile_warp(
    runtime: &CloudflareWarpGatewayRuntime,
    profile: GatewayTopologyProfile<'_>,
    input: GatewayTopologyCompileInput<'_>,
) -> Result<CompiledGatewayTopology> {
    let image = required(
        runtime.image.as_str(),
        input.error_subject,
        profile.id,
        "empty WARP gateway image",
    )?;
    let state_volume_name = required(
        runtime.state_volume_name.as_str(),
        input.error_subject,
        profile.id,
        "empty WARP state volume",
    )?;
    let enrollment_id = required(
        runtime.enrollment_id.as_str(),
        input.error_subject,
        profile.id,
        "empty WARP enrollment id",
    )?;
    let identity_secret_ref = required(
        runtime.identity_secret_ref.as_str(),
        input.error_subject,
        profile.id,
        "empty WARP identity secret reference",
    )?;
    let app_container_name = required(
        input.app_container_name,
        input.error_subject,
        profile.id,
        "empty app container name",
    )?;
    let gateway_name = format!("{app_container_name}-vpn");
    let mut labels = input.base_labels.clone();
    labels.insert(input.labels.role.to_string(), "warp_gateway".to_string());
    labels.insert("elixir.warp.profile_id".to_string(), profile.id.to_string());
    labels.insert(
        "elixir.warp.enrollment_id".to_string(),
        enrollment_id.to_string(),
    );

    let mut sysctls = HashMap::new();
    sysctls.insert(
        "net.ipv6.conf.all.disable_ipv6".to_string(),
        "0".to_string(),
    );
    sysctls.insert(
        "net.ipv4.conf.all.src_valid_mark".to_string(),
        "1".to_string(),
    );
    sysctls.insert("net.ipv4.ip_forward".to_string(), "1".to_string());
    sysctls.insert("net.ipv6.conf.all.forwarding".to_string(), "1".to_string());
    sysctls.insert("net.ipv6.conf.all.accept_ra".to_string(), "2".to_string());

    let mut gateway_spec = ContainerSpec {
        name: gateway_name.clone(),
        image: image.to_string(),
        network: input.app_spec.network.clone(),
        network_mode: None,
        aliases: input.app_spec.aliases.clone(),
        env: vec![
            EnvVar {
                name: "WARP_SLEEP".to_string(),
                value: "2".to_string(),
            },
            EnvVar {
                name: "WARP_ENABLE_NAT".to_string(),
                value: "1".to_string(),
            },
            EnvVar {
                name: "ELIXIR_WARP_ENROLLMENT_ID".to_string(),
                value: enrollment_id.to_string(),
            },
            EnvVar {
                name: "ELIXIR_WARP_IDENTITY_SECRET_REF".to_string(),
                value: identity_secret_ref.to_string(),
            },
        ],
        volumes: vec![VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: state_volume_name.to_string(),
            container_path: "/var/lib/cloudflare-warp".to_string(),
            read_only: false,
        }],
        ports: input.app_spec.ports.clone(),
        labels,
        command: Vec::new(),
        cap_add: vec![
            "NET_ADMIN".to_string(),
            "MKNOD".to_string(),
            "AUDIT_WRITE".to_string(),
        ],
        cap_drop: Vec::new(),
        devices: vec!["/dev/net/tun:/dev/net/tun".to_string()],
        sysctls,
        security: Default::default(),
    };
    let mut protected_app_spec = namespace_app(input.app_spec, &gateway_name);
    stamp_topology_labels(
        &mut gateway_spec,
        profile,
        "warp_gateway",
        input.app_spec,
        input.labels,
    );
    stamp_topology_labels(
        &mut protected_app_spec,
        profile,
        "warp_gateway",
        input.app_spec,
        input.labels,
    );
    Ok(CompiledGatewayTopology {
        gateway_spec: Some(gateway_spec),
        protected_app_spec,
    })
}

fn namespace_app(app_spec: &ContainerSpec, gateway_name: &str) -> ContainerSpec {
    let mut protected_app_spec = app_spec.clone();
    protected_app_spec.network_mode = Some(format!("container:{gateway_name}"));
    protected_app_spec.aliases.clear();
    protected_app_spec.ports.clear();
    protected_app_spec
}

fn push_input_ports(env: &mut Vec<EnvVar>, app_spec: &ContainerSpec) {
    let input_ports = app_spec
        .ports
        .iter()
        .map(|port| port.container_port.to_string())
        .collect::<Vec<_>>()
        .join(",");
    if !input_ports.is_empty() {
        env.push(EnvVar {
            name: "FIREWALL_INPUT_PORTS".to_string(),
            value: input_ports,
        });
    }
}

fn required<'a>(value: &'a str, subject: &str, profile_id: &str, error: &str) -> Result<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{subject} '{profile_id}' has an {error}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::model::PortMapping;

    const TEST_LABELS: GatewayTopologyLabels<'static> = GatewayTopologyLabels {
        role: "elixir.network_role",
        profile_id: "elixir.live_egress.profile_id",
        profile_kind: "elixir.live_egress.profile_kind",
        runtime_kind: "elixir.live_egress.runtime_kind",
        exposed_ports: "elixir.live_egress.exposed_ports",
    };

    #[test]
    fn n10_neutral_compiler_uses_caller_owned_labels_and_namespace() -> Result<()> {
        let app = test_app_spec();
        let runtime = GatewayRuntime::GluetunWireguard(GluetunWireguardGatewayRuntime {
            image: "example/gateway:1".to_string(),
            config_host_path: "/run/elixir/live/wg0.conf".to_string(),
        });
        let topology = compile_gateway_topology(
            GatewayTopologyProfile {
                id: "live-profile-1",
                kind: "wireguard_config",
                runtime: &runtime,
            },
            GatewayTopologyCompileInput {
                app_container_name: "elx-live-worker-1",
                app_spec: &app,
                base_labels: &HashMap::new(),
                labels: TEST_LABELS,
                error_subject: "live egress profile",
            },
        )?;

        let gateway = topology.gateway_spec.expect("gateway topology");
        assert_eq!(gateway.name, "elx-live-worker-1-vpn");
        assert_eq!(
            gateway
                .labels
                .get(TEST_LABELS.profile_id)
                .map(String::as_str),
            Some("live-profile-1")
        );
        assert_eq!(
            gateway
                .labels
                .get(TEST_LABELS.runtime_kind)
                .map(String::as_str),
            Some("gluetun_wireguard")
        );
        assert!(
            gateway
                .labels
                .keys()
                .all(|key| !key.starts_with("elixir.download_network."))
        );
        assert_eq!(
            topology.protected_app_spec.network_mode.as_deref(),
            Some("container:elx-live-worker-1-vpn")
        );
        assert!(topology.protected_app_spec.ports.is_empty());
        assert!(topology.protected_app_spec.aliases.is_empty());
        Ok(())
    }

    #[test]
    fn n10_neutral_compiler_preserves_validation_subject_and_input() {
        let app = test_app_spec();
        let runtime = GatewayRuntime::CloudflareWarp(CloudflareWarpGatewayRuntime {
            image: " ".to_string(),
            state_volume_name: "state".to_string(),
            enrollment_id: "enrollment".to_string(),
            identity_secret_ref: "secret-ref".to_string(),
        });
        let error = compile_gateway_topology(
            GatewayTopologyProfile {
                id: "live-profile-2",
                kind: "cloudflare_warp",
                runtime: &runtime,
            },
            GatewayTopologyCompileInput {
                app_container_name: "elx-live-worker-2",
                app_spec: &app,
                base_labels: &HashMap::new(),
                labels: TEST_LABELS,
                error_subject: "live egress profile",
            },
        )
        .expect_err("blank image must fail closed");

        assert_eq!(
            error.to_string(),
            "live egress profile 'live-profile-2' has an empty WARP gateway image"
        );
        assert_eq!(app.network_mode, None);
        assert_eq!(app.ports.len(), 2);
    }

    fn test_app_spec() -> ContainerSpec {
        ContainerSpec {
            name: "elx-live-worker".to_string(),
            image: "example/live-worker:1".to_string(),
            network: "elixir_live".to_string(),
            network_mode: None,
            aliases: vec!["live-worker".to_string()],
            env: Vec::new(),
            volumes: Vec::new(),
            ports: vec![
                PortMapping {
                    container_port: 8080,
                    host_port: None,
                    host_ip: None,
                    protocol: None,
                },
                PortMapping {
                    container_port: 9090,
                    host_port: None,
                    host_ip: None,
                    protocol: Some("udp".to_string()),
                },
            ],
            labels: HashMap::new(),
            command: Vec::new(),
            cap_add: Vec::new(),
            cap_drop: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
            security: Default::default(),
        }
    }
}
