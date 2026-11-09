//! Closed marketplace underlay contract. Docker observations are not protection
//! capabilities; the controller must bind the actual network and actor IDs.
use super::model::{ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
};

pub const PACKET_NETWORK_LABEL: &str = "elixir.marketplace.uplink";
pub const PACKET_NETWORK_SCHEMA: &str = "encrypted-vpn-v1";
pub fn immutable_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn deployment_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

#[derive(Debug, Clone)]
pub struct PacketNetworkSpec {
    name: String,
    deployment: String,
    ipv6: bool,
    internal: bool,
}
impl PacketNetworkSpec {
    pub fn internet(deployment: &str, ipv6: bool) -> Result<Self> {
        Self::new(deployment, ipv6, false)
    }
    pub(super) fn new(deployment: &str, ipv6: bool, internal: bool) -> Result<Self> {
        if !deployment_valid(deployment) {
            bail!("invalid marketplace network deployment");
        }
        Ok(Self {
            name: format!("elixir-marketplace-uplink-{deployment}"),
            deployment: deployment.to_owned(),
            ipv6,
            internal,
        })
    }
    #[cfg(test)]
    pub fn isolated_fixture(deployment: &str, ipv6: bool) -> Result<Self> {
        Self::new(deployment, ipv6, true)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn ipv6(&self) -> bool {
        self.ipv6
    }
    pub fn internal(&self) -> bool {
        self.internal
    }
    pub fn labels(&self) -> HashMap<String, String> {
        HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (
                ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(),
                self.deployment.clone(),
            ),
            (
                PACKET_NETWORK_LABEL.to_owned(),
                PACKET_NETWORK_SCHEMA.to_owned(),
            ),
        ])
    }
    pub fn options(&self) -> BTreeMap<String, String> {
        let mut options = BTreeMap::from([
            (
                "com.docker.network.bridge.enable_icc".to_owned(),
                "false".to_owned(),
            ),
            (
                "com.docker.network.bridge.enable_ip_masquerade".to_owned(),
                "true".to_owned(),
            ),
            (
                "com.docker.network.bridge.host_binding_ipv4".to_owned(),
                "127.0.0.1".to_owned(),
            ),
            (
                "com.docker.network.bridge.gateway_mode_ipv4".to_owned(),
                "nat".to_owned(),
            ),
        ]);
        if self.ipv6 {
            options.insert(
                "com.docker.network.bridge.gateway_mode_ipv6".to_owned(),
                "nat".to_owned(),
            );
        }
        options
    }
    pub fn verify(&self, actual: &PacketNetworkInspection) -> Result<()> {
        if !immutable_id(&actual.id)
            || actual.name != self.name
            || actual.driver != "bridge"
            || actual.scope != "local"
            || actual.internal != self.internal
            || !actual.ipv4
            || actual.ipv6 != self.ipv6
            || actual.attachable
            || actual.ingress
            || actual.config_only
            || !actual.config_from.is_empty()
            || actual.options != self.options()
            || actual.ipam_driver != "default"
            || !actual.ipam_options.is_empty()
            || self
                .labels()
                .iter()
                .any(|(key, value)| actual.labels.get(key) != Some(value))
            || actual.subnets.len() != if self.ipv6 { 2 } else { 1 }
        {
            bail!("marketplace uplink differs from its owned network profile");
        }
        let mut v4 = 0;
        let mut v6 = 0;
        for subnet in &actual.subnets {
            subnet.validate()?;
            match subnet.gateway {
                IpAddr::V4(_) => v4 += 1,
                IpAddr::V6(_) => v6 += 1,
            }
        }
        if v4 != 1 || v6 != usize::from(self.ipv6) {
            bail!("marketplace uplink address families differ");
        }
        for (id, member) in &actual.members {
            if !immutable_id(id) || !immutable_id(&member.endpoint_id) || member.name.len() > 128 {
                bail!("invalid marketplace network member");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PacketNetworkMember {
    pub name: String,
    pub endpoint_id: String,
}
#[derive(Debug, Clone)]
pub struct PacketNetworkSubnet {
    pub subnet: String,
    pub gateway: IpAddr,
}
impl PacketNetworkSubnet {
    fn validate(&self) -> Result<()> {
        let (address, prefix) = self
            .subnet
            .split_once('/')
            .context("missing uplink subnet prefix")?;
        let address: IpAddr = address.parse()?;
        let prefix: u8 = prefix.parse()?;
        let valid = match (address, self.gateway) {
            (IpAddr::V4(network), IpAddr::V4(gateway)) if (8..=30).contains(&prefix) => {
                let mask = u32::MAX << (32 - prefix);
                let network = u32::from(network);
                let gateway = u32::from(gateway);
                gateway != network
                    && gateway != (network | !mask)
                    && network & !mask == 0
                    && gateway & mask == network
                    && std::net::Ipv4Addr::from(network).is_private()
            }
            (IpAddr::V6(network), IpAddr::V6(gateway)) if (7..=126).contains(&prefix) => {
                let mask = u128::MAX << (128 - prefix);
                let network = u128::from(network);
                let gateway = u128::from(gateway);
                network >> 121 == 0x7e
                    && network & !mask == 0
                    && gateway & mask == network
                    && gateway != network
            }
            _ => false,
        };
        if !valid {
            bail!("invalid private marketplace uplink subnet/gateway");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PacketNetworkInspection {
    pub id: String,
    pub name: String,
    pub driver: String,
    pub scope: String,
    pub internal: bool,
    pub ipv4: bool,
    pub ipv6: bool,
    pub attachable: bool,
    pub ingress: bool,
    pub config_only: bool,
    pub config_from: String,
    pub labels: HashMap<String, String>,
    pub options: BTreeMap<String, String>,
    pub ipam_driver: String,
    pub ipam_options: BTreeMap<String, String>,
    pub subnets: Vec<PacketNetworkSubnet>,
    pub members: BTreeMap<String, PacketNetworkMember>,
}
impl PacketNetworkInspection {
    pub fn parse(value: &Value) -> Result<Self> {
        let text = |path: &str| {
            value
                .pointer(path)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("missing network string")
        };
        let boolean = |path: &str| {
            value
                .pointer(path)
                .and_then(Value::as_bool)
                .context("missing network boolean")
        };
        fn map(value: &Value, path: &str) -> Result<BTreeMap<String, String>> {
            match value.pointer(path).context("missing network map")? {
                Value::Null => Ok(BTreeMap::new()),
                Value::Object(map) if map.len() <= 32 => map
                    .iter()
                    .map(|(key, value)| {
                        Ok((
                            key.clone(),
                            value
                                .as_str()
                                .context("invalid network map value")?
                                .to_owned(),
                        ))
                    })
                    .collect(),
                _ => bail!("invalid or oversized network map"),
            }
        }
        let rows = value
            .pointer("/IPAM/Config")
            .and_then(Value::as_array)
            .context("missing network address pools")?;
        if rows.is_empty() || rows.len() > 2 {
            bail!("invalid network address pool count");
        }
        let mut subnets = vec![];
        for row in rows {
            if row
                .get("IPRange")
                .is_some_and(|value| !value.is_null() && value.as_str() != Some(""))
                || row.get("AuxiliaryAddresses").is_some_and(|value| {
                    !value.is_null() && !value.as_object().is_some_and(|map| map.is_empty())
                })
            {
                bail!("unexpected custom network allocation");
            }
            subnets.push(PacketNetworkSubnet {
                subnet: row
                    .get("Subnet")
                    .and_then(Value::as_str)
                    .context("missing network subnet")?
                    .to_owned(),
                gateway: row
                    .get("Gateway")
                    .and_then(Value::as_str)
                    .context("missing network gateway")?
                    .parse()?,
            });
        }
        let rows = value
            .get("Containers")
            .and_then(Value::as_object)
            .context("missing network members")?;
        if rows.len() > 8 {
            bail!("unexpected marketplace network membership");
        }
        let members = rows
            .iter()
            .map(|(id, row)| {
                Ok((
                    id.clone(),
                    PacketNetworkMember {
                        name: row
                            .get("Name")
                            .and_then(Value::as_str)
                            .context("missing network member name")?
                            .to_owned(),
                        endpoint_id: row
                            .get("EndpointID")
                            .and_then(Value::as_str)
                            .context("missing network endpoint ID")?
                            .to_owned(),
                    },
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            id: text("/Id")?,
            name: text("/Name")?,
            driver: text("/Driver")?,
            scope: text("/Scope")?,
            internal: boolean("/Internal")?,
            ipv4: boolean("/EnableIPv4")?,
            ipv6: boolean("/EnableIPv6")?,
            attachable: boolean("/Attachable")?,
            ingress: boolean("/Ingress")?,
            config_only: boolean("/ConfigOnly")?,
            config_from: text("/ConfigFrom/Network")?,
            labels: map(value, "/Labels")?.into_iter().collect(),
            options: map(value, "/Options")?,
            ipam_driver: text("/IPAM/Driver")?,
            ipam_options: map(value, "/IPAM/Options")?,
            subnets,
            members,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn fixture(spec: &PacketNetworkSpec) -> Value {
        let mut pools = vec![serde_json::json!({"Subnet":"172.29.0.0/16","Gateway":"172.29.0.1"})];
        if spec.ipv6() {
            pools.push(serde_json::json!({"Subnet":"fd77:12::/64","Gateway":"fd77:12::1"}));
        }
        serde_json::json!({"Id":"a".repeat(64),"Name":spec.name(),"Driver":"bridge","Scope":"local","Internal":spec.internal(),"EnableIPv4":true,"EnableIPv6":spec.ipv6(),"Attachable":false,"Ingress":false,"ConfigOnly":false,"ConfigFrom":{"Network":""},"Labels":spec.labels(),"Options":spec.options(),"IPAM":{"Driver":"default","Options":null,"Config":pools},"Containers":{}})
    }
    #[test]
    fn actual_network_configuration_is_required_not_names_or_labels() -> Result<()> {
        let spec = PacketNetworkSpec::internet("unit", true)?;
        let value = fixture(&spec);
        spec.verify(&PacketNetworkInspection::parse(&value)?)?;
        for (path, replacement) in [
            ("/Id", Value::String("short".to_owned())),
            ("/Name", Value::String("other".to_owned())),
            ("/Driver", Value::String("host".to_owned())),
            ("/Scope", Value::String("swarm".to_owned())),
            ("/Internal", Value::Bool(true)),
            ("/EnableIPv4", Value::Null),
            ("/EnableIPv6", Value::Bool(false)),
            ("/Attachable", Value::Bool(true)),
            ("/Ingress", Value::Bool(true)),
            ("/ConfigOnly", Value::Bool(true)),
            ("/ConfigFrom/Network", Value::String("foreign".to_owned())),
            ("/Labels", serde_json::json!({})),
            ("/Options", serde_json::json!({})),
            ("/IPAM/Driver", Value::String("foreign".to_owned())),
            ("/IPAM/Options", serde_json::json!({"unexpected":"true"})),
            ("/IPAM/Config", serde_json::json!([])),
            (
                "/IPAM/Config/0/Gateway",
                Value::String("172.30.0.1".to_owned()),
            ),
            (
                "/IPAM/Config/0/Subnet",
                Value::String("8.8.8.0/24".to_owned()),
            ),
            (
                "/IPAM/Config/1/Subnet",
                Value::String("2001:db8::/64".to_owned()),
            ),
        ] {
            let mut bad = value.clone();
            *bad.pointer_mut(path).unwrap() = replacement;
            assert!(
                PacketNetworkInspection::parse(&bad)
                    .and_then(|raw| spec.verify(&raw))
                    .is_err(),
                "{path}"
            );
        }
        let mut bad = value.clone();
        bad["Options"]["com.docker.network.bridge.enable_icc"] = serde_json::json!("true");
        assert!(spec.verify(&PacketNetworkInspection::parse(&bad)?).is_err());
        let mut bad = value.clone();
        bad["IPAM"]["Config"][0]["AuxiliaryAddresses"] =
            serde_json::json!({"reserved":"172.29.0.12"});
        assert!(PacketNetworkInspection::parse(&bad).is_err());
        let mut bad = value;
        bad["Containers"] =
            serde_json::json!({"short":{"Name":"foreign","EndpointID":"b".repeat(64)}});
        assert!(spec.verify(&PacketNetworkInspection::parse(&bad)?).is_err());
        assert!(PacketNetworkSpec::internet("../foreign", false).is_err());
        Ok(())
    }
    #[test]
    fn ipv4_only_and_isolated_fixture_modes_are_distinct_profiles() -> Result<()> {
        let spec = PacketNetworkSpec::internet("unit", false)?;
        let actual = PacketNetworkInspection::parse(&fixture(&spec))?;
        spec.verify(&actual)?;
        assert!(
            PacketNetworkSpec::isolated_fixture("unit", false)?
                .verify(&actual)
                .is_err()
        );
        assert!(
            PacketNetworkSpec::internet("unit", true)?
                .verify(&actual)
                .is_err()
        );
        for (subnet, gateway) in [
            ("172.29.0.0/16", "172.29.255.255"),
            ("172.29.0.1/16", "172.29.0.2"),
            ("0.0.0.0/0", "8.8.8.8"),
            ("fd77:12::/64", "fd77:13::1"),
            ("fe80::/64", "fe80::1"),
        ] {
            assert!(
                PacketNetworkSubnet {
                    subnet: subnet.to_owned(),
                    gateway: gateway.parse()?
                }
                .validate()
                .is_err()
            );
        }
        Ok(())
    }
}
