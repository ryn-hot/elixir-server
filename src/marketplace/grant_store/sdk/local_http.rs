//! Current owner-configured endpoint DATA. This is not native execution or
//! transport authority. The original installed broker must retain/recheck both
//! independently, including while a request is in flight. No secret fallback,
//! publisher defaults, write, DNS, network I/O or transaction across I/O.
use super::*;
use crate::{
    extensions::manifest::ManifestLanEndpointAccess,
    live::upstream::{
        ConfiguredLanEndpoint, DestinationPolicy, LanEndpointAccess, LocalDestinationDenylist,
        PrivateLanGate,
    },
};

pub(in crate::marketplace) struct LocalEndpoint {
    endpoint: ConfiguredLanEndpoint,
    revision: Digest,
}
impl LocalEndpoint {
    pub(in crate::marketplace) fn revision(&self) -> Digest {
        self.revision
    }
    pub(in crate::marketplace) fn policy_for(
        &self,
        requested: &str,
        server_enabled: bool,
        denylist: LocalDestinationDenylist,
    ) -> Result<DestinationPolicy> {
        // The other restrictions were read from the current installed scope
        // and explicit owner toggle, NOT accepted from the SDK request.
        Ok(self.endpoint.policy_for(
            requested,
            PrivateLanGate {
                server_enabled,
                provider_permission: true,
                descriptor_requested: true,
                destination_authorized: true,
            },
            denylist,
        )?)
    }
}

pub(in crate::marketplace::grant_store) async fn load(
    database: &Database,
    secrets: &SecretsManager,
    binding: &super::super::installation::standing::InstalledBinding,
    requested: &str,
) -> Result<Option<LocalEndpoint>> {
    let coordinates = binding.coordinates();
    // No legacy identity store or cross-instance endpoint lookup. A configured
    // owner and its separate installed-generation namespace are mandatory.
    if coordinates.instance == coordinates.namespace {
        return Ok(None);
    }
    let permissions = binding.review().scope().permissions();
    if !permissions.iter().any(|p| p == "network.egress")
        || !permissions.iter().any(|p| p == "live.network.private")
    {
        // Immutable ORIGINAL scope can rule out local access without another
        // manifest/SQL pass. None is no endpoint, never public/native authority;
        // the dispatcher's independent public-authority checks still apply.
        return Ok(None);
    }
    let mut connection = crate::marketplace::durability::connection(database).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, database.driver).await?;
    let manifest = binding.current_manifest(&mut tx, database.driver).await?;
    let Some(control) = manifest.control_surface else {
        tx.rollback().await?;
        return Ok(None);
    };
    let fields: Vec<_> = control
        .owned_settings
        .iter()
        .filter(|field| field.lan_endpoint.is_some())
        .collect();
    if fields.len() > 8 {
        bail!("installed local endpoint declarations exceed their bound");
    }
    if fields.is_empty() {
        tx.rollback().await?;
        return Ok(None);
    }
    let config: Option<String> = sqlx::query_scalar(
        "SELECT SUBSTR(config_json,1,65537) FROM extension_instances WHERE instance_id=$1 AND extension_id=$2",
    ).bind(coordinates.instance.to_string()).bind(coordinates.extension.as_str())
        .fetch_one(&mut *tx).await?;
    let config = match config {
        Some(value) if value.len() <= 65536 => serde_json::from_str::<serde_json::Value>(&value)?,
        None => serde_json::json!({}),
        _ => bail!("local endpoint configuration exceeds its bound"),
    };
    let config = config
        .as_object()
        .context("local endpoint configuration must be an object")?;
    let mut selected = None;
    for field in fields {
        let declaration = field
            .lan_endpoint
            .as_ref()
            .expect("selected endpoint declaration");
        let enable = control
            .owned_settings
            .iter()
            .find(|setting| setting.id == declaration.enabled_by)
            .context("local endpoint enable control is missing")?;
        // No declared default, coercion or extension/global setting fallback.
        if config.get(&enable.storage.key) != Some(&serde_json::Value::Bool(true)) {
            continue;
        }
        let value = match field.storage.r#type.as_str() {
            "instance_secret" => {
                generation_secrets::read(&mut tx, secrets, coordinates, &field.storage.key).await?
            }
            "instance_setting" => match config.get(&field.storage.key) {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(value)) => {
                    Some(zeroize::Zeroizing::new(value.clone()))
                }
                _ => bail!("configured local endpoint must be text"),
            },
            _ => bail!("local endpoint is not an instance-owned setting"),
        };
        let Some(value) = value.filter(|value| !value.is_empty()) else {
            continue;
        };
        let access = match declaration.access {
            ManifestLanEndpointAccess::ExactUrl => LanEndpointAccess::ExactUrl,
            ManifestLanEndpointAccess::Origin => LanEndpointAccess::Origin,
        };
        let endpoint = ConfiguredLanEndpoint::new(&value, access)?;
        if endpoint.accepts(requested)? {
            // Only an in-memory comparison of current restrictions; this is
            // never persisted/published or treated as a native/grant permit.
            let bytes = zeroize::Zeroizing::new(canonical::to_vec(&serde_json::json!({
                "domain":"elixir-marketplace/configured-lan-endpoint/v1",
                "home":coordinates.home,"extension":coordinates.extension,
                "instance":coordinates.instance,"namespace":coordinates.namespace,
                "field":field.id,"declaration":declaration,"endpoint":value.as_str(),
            }))?);
            let found = LocalEndpoint {
                endpoint,
                revision: Digest::of(bytes.as_slice()),
            };
            if selected.is_some() {
                bail!("multiple configured local endpoint declarations match this request");
            }
            selected = Some(found);
        }
    }
    tx.rollback().await?;
    Ok(selected)
}
