use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CapabilitySlot {
    pub capability: String,
    #[serde(default = "default_slot")]
    pub slot_id: String,
}

impl CapabilitySlot {
    pub fn new(capability: String, slot_id: Option<String>) -> Result<Self> {
        let slot_id = slot_id.unwrap_or_else(default_slot);
        let value = Self {
            capability,
            slot_id,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn key(&self) -> String {
        format!("{}/{}", self.capability, self.slot_id)
    }

    pub fn validate(&self) -> Result<()> {
        if self.capability.trim().is_empty() {
            bail!("capability is required");
        }
        if self.slot_id.trim().is_empty() {
            bail!("slot_id is required");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SlotSelectionPolicy {
    Manual,
    AutoPrefer,
    AutoHighestTrust,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    Prompt,
    AutoReplace,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderEndpoint {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    #[serde(default = "default_base_path")]
    pub base_path: String,
    #[serde(default)]
    pub network: Option<String>,
}

pub const HOST_RUNTIME_NETWORK: &str = "elixir_host";

impl ProviderEndpoint {
    pub fn new(
        scheme: String,
        host: String,
        port: u16,
        base_path: Option<String>,
        network: Option<String>,
    ) -> Result<Self> {
        let base_path = normalize_base_path(base_path);
        let endpoint = Self {
            scheme,
            host,
            port,
            base_path,
            network,
        };
        endpoint.validate()?;
        Ok(endpoint)
    }

    pub fn canonical_url(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "{}://{}:{}{}",
            self.scheme, self.host, self.port, self.base_path
        ))
    }

    pub fn validate(&self) -> Result<()> {
        if self.scheme.trim().is_empty() {
            bail!("endpoint scheme is required");
        }
        if self.port == 0 {
            bail!("endpoint port must be non-zero");
        }
        validate_host(
            &self.host,
            self.network.as_deref() == Some(HOST_RUNTIME_NETWORK),
        )?;
        Ok(())
    }
}

fn default_slot() -> String {
    "default".to_string()
}

fn default_base_path() -> String {
    "/".to_string()
}

fn normalize_base_path(value: Option<String>) -> String {
    let value = value.unwrap_or_else(default_base_path);
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return default_base_path();
    }
    if trimmed.starts_with('/') {
        return trimmed.to_string();
    }
    format!("/{}", trimmed)
}

fn validate_host(host: &str, allow_host_runtime: bool) -> Result<()> {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        bail!("endpoint host is required");
    }
    if trimmed.contains("://") {
        bail!("endpoint host must not include a scheme");
    }
    let lowered = trimmed.to_ascii_lowercase();
    let address = lowered
        .trim_matches(|character| matches!(character, '[' | ']'))
        .parse::<std::net::IpAddr>()
        .ok();
    if matches!(lowered.as_str(), "localhost" | "host.docker.internal")
        || address.is_some_and(|address| address.is_unspecified())
        || (address.is_some_and(|address| address.is_loopback()) && !allow_host_runtime)
    {
        bail!("endpoint host '{}' is not allowed", trimmed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_slot_requires_fields() {
        assert!(CapabilitySlot::new("".to_string(), None).is_err());
        assert!(CapabilitySlot::new("media.manager".to_string(), Some("".to_string())).is_err());
        let slot = CapabilitySlot::new("media.manager".to_string(), None).expect("slot");
        assert_eq!(slot.slot_id, "default");
        assert_eq!(slot.key(), "media.manager/default");
    }

    #[test]
    fn endpoint_rejects_localhost() {
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "localhost".to_string(),
            8080,
            None,
            None,
        );
        assert!(endpoint.is_err());
    }

    #[test]
    fn endpoint_allows_only_marked_host_runtime_loopback() {
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "127.0.0.1".to_string(),
            32_932,
            None,
            Some(HOST_RUNTIME_NETWORK.to_string()),
        )
        .expect("host runtime endpoint");
        assert_eq!(endpoint.network.as_deref(), Some(HOST_RUNTIME_NETWORK));

        assert!(
            ProviderEndpoint::new(
                "http".to_string(),
                "0.0.0.0".to_string(),
                32_932,
                None,
                Some(HOST_RUNTIME_NETWORK.to_string()),
            )
            .is_err()
        );
    }

    #[test]
    fn endpoint_normalizes_base_path() {
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-indexer".to_string(),
            9696,
            Some("health".to_string()),
            Some("elixir_net".to_string()),
        )
        .expect("endpoint");
        assert_eq!(endpoint.base_path, "/health");
        assert_eq!(
            endpoint.canonical_url().expect("url"),
            "http://svc-indexer:9696/health"
        );
    }
}
