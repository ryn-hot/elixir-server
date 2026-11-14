use std::{collections::BTreeSet, fmt};

use reqwest::Url;
use serde_json::{Value, json};
use sqlx::AnyPool;
use uuid::Uuid;

use crate::{
    db::models::ExtensionTrustLevel,
    extensions::{
        manifest::{
            LIVE_CATALOG_READ_PERMISSION, LIVE_NETWORK_PRIVATE_PERMISSION,
            LIVE_STREAM_RESOLVE_PERMISSION,
        },
        store::{ExtensionStore, ReadyLiveCatalogProvider},
    },
    orchestrator::model::{HOST_RUNTIME_NETWORK, ProviderEndpoint},
};

use super::super::contract::{
    ContractError, LiveItemType, ProviderContract, ProviderOperation, StreamProtocol,
    validate_provider_config,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderRevision([u8; 32]);

impl ProviderRevision {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ProviderRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&blake3::Hash::from_bytes(self.0).to_hex())
    }
}

#[derive(Clone)]
pub struct LiveProviderSnapshot {
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub extension_id: String,
    pub extension_version: String,
    pub implementation: String,
    pub trust_level: ExtensionTrustLevel,
    pub contract: ProviderContract,
    pub revision: ProviderRevision,
    actions: BTreeSet<ProviderOperation>,
    permissions: BTreeSet<String>,
    config: Value,
    endpoint: ProviderTransport,
}
#[derive(Clone)]
enum ProviderTransport {
    Http(ProviderRuntimeEndpoint),
    #[cfg(unix)]
    Source(crate::marketplace::service::SourceProviderDescriptor),
}

impl LiveProviderSnapshot {
    pub fn permits(&self, operation: ProviderOperation) -> bool {
        match operation {
            ProviderOperation::Health => true,
            ProviderOperation::Catalogs | ProviderOperation::Catalog => {
                self.actions.contains(&ProviderOperation::Catalog)
                    && self.permissions.contains(LIVE_CATALOG_READ_PERMISSION)
            }
            ProviderOperation::Meta => {
                self.actions.contains(&ProviderOperation::Meta)
                    && self.permissions.contains(LIVE_CATALOG_READ_PERMISSION)
            }
            ProviderOperation::Resolve => {
                self.actions.contains(&ProviderOperation::Resolve)
                    && self.permissions.contains(LIVE_STREAM_RESOLVE_PERMISSION)
            }
            ProviderOperation::Refresh => {
                self.actions.contains(&ProviderOperation::Refresh)
                    && self.permissions.contains(LIVE_STREAM_RESOLVE_PERMISSION)
            }
        }
    }

    pub fn permits_private_network(&self) -> bool {
        self.permissions.contains(LIVE_NETWORK_PRIVATE_PERMISSION)
    }

    pub(crate) fn config(&self) -> &Value {
        &self.config
    }

    pub(crate) fn operation_url(
        &self,
        operation: ProviderOperation,
    ) -> Result<Url, ProviderDirectoryError> {
        match &self.endpoint {
            ProviderTransport::Http(endpoint) => endpoint.operation_url(operation),
            #[cfg(unix)]
            ProviderTransport::Source(_) => Err(invalid_snapshot()),
        }
    }
    #[cfg(unix)]
    pub(super) fn source(&self) -> Option<&crate::marketplace::service::SourceProviderDescriptor> {
        match &self.endpoint {
            ProviderTransport::Source(source) => Some(source),
            _ => None,
        }
    }
}

impl fmt::Debug for LiveProviderSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveProviderSnapshot")
            .field("provider_id", &self.provider_id)
            .field("instance_id", &self.instance_id)
            .field("extension_id", &self.extension_id)
            .field("extension_version", &self.extension_version)
            .field("implementation", &self.implementation)
            .field("trust_level", &self.trust_level)
            .field("contract", &self.contract)
            .field("revision", &self.revision)
            .field("actions", &self.actions)
            .field("permissions", &self.permissions)
            .field("config", &"<bounded-provider-config>")
            .field("endpoint", &"<private-runtime-endpoint>")
            .finish()
    }
}

#[derive(Debug, Clone)]
struct ProviderRuntimeEndpoint {
    origin: Url,
    base_path: String,
    health_path: String,
}

impl ProviderRuntimeEndpoint {
    fn operation_url(&self, operation: ProviderOperation) -> Result<Url, ProviderDirectoryError> {
        let suffix = if operation == ProviderOperation::Health {
            self.health_path.as_str()
        } else {
            operation.path()
        };
        let path = join_paths(&self.base_path, suffix);
        let mut url = self.origin.clone();
        url.set_path(&path);
        url.set_query(None);
        url.set_fragment(None);
        Ok(url)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderDirectoryErrorCode {
    StoreUnavailable,
    NotReady,
    InvalidSnapshot,
    RevisionChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderDirectoryError {
    code: ProviderDirectoryErrorCode,
}

impl ProviderDirectoryError {
    const fn new(code: ProviderDirectoryErrorCode) -> Self {
        Self { code }
    }

    pub const fn code(self) -> ProviderDirectoryErrorCode {
        self.code
    }
}

impl fmt::Display for ProviderDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Live provider directory failed ({:?})",
            self.code
        )
    }
}

impl std::error::Error for ProviderDirectoryError {}

#[derive(Clone)]
pub struct LiveProviderDirectory {
    pool: AnyPool,
    allow_loopback: bool,
    #[cfg(unix)]
    source: Option<(
        crate::db::Database,
        crate::marketplace::service::MarketplaceService,
    )>,
}

impl LiveProviderDirectory {
    pub fn new(pool: AnyPool) -> Self {
        Self {
            pool,
            allow_loopback: false,
            #[cfg(unix)]
            source: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(pool: AnyPool) -> Self {
        Self {
            pool,
            allow_loopback: true,
            #[cfg(unix)]
            source: None,
        }
    }
    #[cfg(unix)]
    pub(crate) fn with_marketplace(
        database: crate::db::Database,
        marketplace: crate::marketplace::service::MarketplaceService,
    ) -> Self {
        Self {
            pool: database.pool.clone(),
            allow_loopback: false,
            source: Some((database, marketplace)),
        }
    }
    #[cfg(unix)]
    pub(super) fn source_runtime(
        &self,
    ) -> Option<&(
        crate::db::Database,
        crate::marketplace::service::MarketplaceService,
    )> {
        self.source.as_ref()
    }

    pub async fn discover(&self) -> Result<Vec<LiveProviderSnapshot>, ProviderDirectoryError> {
        let ready = ExtensionStore::new(&self.pool)
            .list_ready_live_catalog_providers()
            .await
            .map_err(|_| {
                ProviderDirectoryError::new(ProviderDirectoryErrorCode::StoreUnavailable)
            })?;
        let mut providers = Vec::with_capacity(ready.len());
        for provider in ready {
            let provider_id = provider.provider.provider_id;
            match snapshot_from_ready(provider, self.allow_loopback) {
                Ok(provider) => providers.push(provider),
                Err(error) => tracing::warn!(
                    provider_id = %provider_id,
                    error_code = ?error.code(),
                    "excluding invalid Live provider snapshot"
                ),
            }
        }
        #[cfg(unix)]
        if let Some((database, marketplace)) = &self.source {
            let source = marketplace
                .source_providers(
                    database,
                    crate::extensions::manifest::LIVE_CATALOG_PROVIDER_CAPABILITY,
                )
                .await
                .map_err(|_| {
                ProviderDirectoryError::new(ProviderDirectoryErrorCode::StoreUnavailable)
            })?;
            for descriptor in source {
                let provider_id = descriptor.provider;
                match snapshot_from_source(descriptor) {
                    Ok(snapshot) => providers.push(snapshot),
                    Err(error) => {
                        tracing::warn!(%provider_id, error_code=?error.code(), "excluding invalid source Live provider snapshot")
                    }
                }
            }
        }
        Ok(providers)
    }

    pub async fn get(
        &self,
        provider_id: Uuid,
    ) -> Result<LiveProviderSnapshot, ProviderDirectoryError> {
        #[cfg(unix)]
        if let Some((database, marketplace)) = &self.source {
            if let Some(descriptor) = marketplace
                .source_provider(
                    database,
                    crate::extensions::manifest::LIVE_CATALOG_PROVIDER_CAPABILITY,
                    provider_id,
                )
                .await
                .map_err(|_| ProviderDirectoryError::new(ProviderDirectoryErrorCode::NotReady))?
            {
                return snapshot_from_source(descriptor);
            }
        }
        let ready = ExtensionStore::new(&self.pool)
            .get_ready_live_catalog_provider(provider_id)
            .await
            .map_err(|_| ProviderDirectoryError::new(ProviderDirectoryErrorCode::StoreUnavailable))?
            .ok_or_else(|| ProviderDirectoryError::new(ProviderDirectoryErrorCode::NotReady))?;
        snapshot_from_ready(ready, self.allow_loopback)
    }

    pub async fn verify(
        &self,
        snapshot: &LiveProviderSnapshot,
    ) -> Result<(), ProviderDirectoryError> {
        let current = self.get(snapshot.provider_id).await?;
        if current.revision == snapshot.revision {
            Ok(())
        } else {
            Err(ProviderDirectoryError::new(
                ProviderDirectoryErrorCode::RevisionChanged,
            ))
        }
    }
}

fn snapshot_from_ready(
    ready: ReadyLiveCatalogProvider,
    allow_loopback: bool,
) -> Result<LiveProviderSnapshot, ProviderDirectoryError> {
    // Public identities never gain an HTTP transport from legacy labels,
    // provider flags or a database URL. Their installed pipeline is separate.
    if ready.extension_id.starts_with("elx:") {
        return Err(invalid_snapshot());
    }
    let implementation = ready
        .provider
        .implementation
        .clone()
        .ok_or_else(invalid_snapshot)?;
    let endpoint_value = ready
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(invalid_snapshot)?;
    let persisted: ProviderEndpoint =
        serde_json::from_value(endpoint_value.clone()).map_err(|_| invalid_snapshot())?;
    let declared_scheme = ready
        .declared_endpoint
        .scheme
        .as_deref()
        .ok_or_else(invalid_snapshot)?;
    let declared_port = ready.declared_endpoint.port.ok_or_else(invalid_snapshot)?;
    let declared_base_path = ready
        .declared_endpoint
        .base_path
        .as_deref()
        .ok_or_else(invalid_snapshot)?;
    let host_runtime = is_host_runtime_endpoint(&persisted);
    if persisted.scheme != declared_scheme
        || (!host_runtime && persisted.port != declared_port)
        || normalize_path(&persisted.base_path)? != normalize_path(declared_base_path)?
    {
        return Err(invalid_snapshot());
    }
    let health_path = ready
        .healthcheck
        .path
        .as_deref()
        .ok_or_else(invalid_snapshot)?;
    let endpoint = validate_runtime_endpoint(&persisted, health_path, allow_loopback)?;

    let mut actions = BTreeSet::new();
    for action in &ready.declared_scope.actions {
        let operation = match action.as_str() {
            "catalog" => ProviderOperation::Catalog,
            "meta" => ProviderOperation::Meta,
            "resolve" => ProviderOperation::Resolve,
            "refresh" => ProviderOperation::Refresh,
            _ => return Err(invalid_snapshot()),
        };
        if !actions.insert(operation) {
            return Err(invalid_snapshot());
        }
    }
    if !actions.contains(&ProviderOperation::Catalog)
        || !actions.contains(&ProviderOperation::Meta)
        || !actions.contains(&ProviderOperation::Resolve)
    {
        return Err(invalid_snapshot());
    }
    let item_types = ready
        .declared_scope
        .live_item_types
        .iter()
        .map(|value| match value.as_str() {
            "event" => Ok(LiveItemType::Event),
            "channel" => Ok(LiveItemType::Channel),
            _ => Err(invalid_snapshot()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let protocols = ready
        .declared_scope
        .stream_protocols
        .iter()
        .map(|value| match value.as_str() {
            "hls" => Ok(StreamProtocol::Hls),
            "dash" => Ok(StreamProtocol::Dash),
            "http_progressive" => Ok(StreamProtocol::HttpProgressive),
            "mpeg_ts" => Ok(StreamProtocol::MpegTs),
            "rtmp" => Ok(StreamProtocol::Rtmp),
            "srt" => Ok(StreamProtocol::Srt),
            _ => Err(invalid_snapshot()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let contract = ProviderContract::new(item_types, protocols).map_err(map_contract_error)?;
    let permissions = ready.permissions.iter().cloned().collect::<BTreeSet<_>>();
    if !permissions.contains(LIVE_CATALOG_READ_PERMISSION)
        || !permissions.contains(LIVE_STREAM_RESOLVE_PERMISSION)
    {
        return Err(invalid_snapshot());
    }
    let mut config = ready.instance_config.clone().unwrap_or_else(|| json!({}));
    if let Some(config) = config.as_object_mut() {
        config.remove("runtime");
    }
    validate_provider_config(&config).map_err(map_contract_error)?;

    let revision_value = json!({
        "providerId": ready.provider.provider_id,
        "instanceId": ready.provider.instance_id,
        "extensionId": ready.extension_id,
        "extensionVersion": ready.extension_version,
        "extensionInstalledAt": ready.extension_installed_at,
        "instanceUpdatedAt": ready.instance_updated_at,
        "implementation": implementation,
        "trust": ready.trust_level.as_str(),
        "contractVersion": ready.contract_version,
        "permissions": permissions,
        "scope": ready.provider.scope_json,
        "endpoint": endpoint_value,
        "config": config,
    });
    let revision_bytes = serde_json::to_vec(&revision_value).map_err(|_| invalid_snapshot())?;
    let revision = ProviderRevision(*blake3::hash(&revision_bytes).as_bytes());

    Ok(LiveProviderSnapshot {
        provider_id: ready.provider.provider_id,
        instance_id: ready.provider.instance_id,
        extension_id: ready.extension_id,
        extension_version: ready.extension_version,
        implementation,
        trust_level: ready.trust_level,
        contract,
        revision,
        actions,
        permissions,
        config,
        endpoint: ProviderTransport::Http(endpoint),
    })
}

#[cfg(unix)]
fn snapshot_from_source(
    source: crate::marketplace::service::SourceProviderDescriptor,
) -> Result<LiveProviderSnapshot, ProviderDirectoryError> {
    let mut actions = BTreeSet::new();
    for action in &source.scope.actions {
        let operation = match action.as_str() {
            "catalog" => ProviderOperation::Catalog,
            "meta" => ProviderOperation::Meta,
            "resolve" => ProviderOperation::Resolve,
            "refresh" => ProviderOperation::Refresh,
            _ => return Err(invalid_snapshot()),
        };
        if !actions.insert(operation) {
            return Err(invalid_snapshot());
        }
    }
    for required in [
        ProviderOperation::Catalog,
        ProviderOperation::Meta,
        ProviderOperation::Resolve,
    ] {
        if !actions.contains(&required) {
            return Err(invalid_snapshot());
        }
    }
    let types: Vec<LiveItemType> = serde_json::from_value(json!(source.scope.live_item_types))
        .map_err(|_| invalid_snapshot())?;
    let protocols: Vec<StreamProtocol> =
        serde_json::from_value(json!(source.scope.stream_protocols))
            .map_err(|_| invalid_snapshot())?;
    let contract = ProviderContract::new(types, protocols).map_err(map_contract_error)?;
    let permissions = source.permissions.iter().cloned().collect::<BTreeSet<_>>();
    if !permissions.contains(LIVE_CATALOG_READ_PERMISSION)
        || !permissions.contains(LIVE_STREAM_RESOLVE_PERMISSION)
    {
        return Err(invalid_snapshot());
    }
    validate_provider_config(&source.config).map_err(map_contract_error)?;
    Ok(LiveProviderSnapshot {
        provider_id: source.provider,
        instance_id: source.instance,
        extension_id: source.extension.to_string(),
        extension_version: source.version.clone(),
        implementation: source.implementation.clone(),
        trust_level: ExtensionTrustLevel::Community,
        contract,
        revision: ProviderRevision(*source.revision.as_bytes()),
        actions,
        permissions,
        config: source.config.clone(),
        endpoint: ProviderTransport::Source(source),
    })
}

fn validate_runtime_endpoint(
    endpoint: &ProviderEndpoint,
    health_path: &str,
    allow_loopback: bool,
) -> Result<ProviderRuntimeEndpoint, ProviderDirectoryError> {
    if !matches!(endpoint.scheme.as_str(), "http" | "https")
        || endpoint.host.is_empty()
        || endpoint.host.len() > 253
        || endpoint.host.chars().any(char::is_control)
        || endpoint.host.bytes().any(|byte| {
            byte.is_ascii_whitespace() || matches!(byte, b'/' | b'\\' | b'@' | b'?' | b'#')
        })
    {
        return Err(invalid_snapshot());
    }
    let host_runtime = is_host_runtime_endpoint(endpoint);
    if !allow_loopback && !host_runtime && forbidden_runtime_host(&endpoint.host) {
        return Err(invalid_snapshot());
    }
    let base_path = normalize_path(&endpoint.base_path)?;
    let health_path = normalize_path(health_path)?;
    let host = if endpoint.host.contains(':') && !endpoint.host.starts_with('[') {
        format!("[{}]", endpoint.host)
    } else {
        endpoint.host.clone()
    };
    let origin = Url::parse(&format!(
        "{}://{}:{}/",
        endpoint.scheme, host, endpoint.port
    ))
    .map_err(|_| invalid_snapshot())?;
    if !origin.username().is_empty() || origin.password().is_some() || origin.host_str().is_none() {
        return Err(invalid_snapshot());
    }
    Ok(ProviderRuntimeEndpoint {
        origin,
        base_path,
        health_path,
    })
}

fn forbidden_runtime_host(host: &str) -> bool {
    let normalized = host
        .trim_matches(|character| matches!(character, '[' | ']'))
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "localhost"
            | "host.docker.internal"
            | "host.containers.internal"
            | "gateway.docker.internal"
    ) || normalized.ends_with(".localhost")
    {
        return true;
    }
    normalized
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_loopback() || address.is_unspecified())
}

fn is_host_runtime_endpoint(endpoint: &ProviderEndpoint) -> bool {
    endpoint.network.as_deref() == Some(HOST_RUNTIME_NETWORK)
        && endpoint
            .host
            .trim_matches(|character| matches!(character, '[' | ']'))
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn normalize_path(value: &str) -> Result<String, ProviderDirectoryError> {
    if value.is_empty()
        || value.len() > 256
        || !value.starts_with('/')
        || value.chars().any(char::is_control)
        || value.contains(['?', '#', '\\'])
    {
        return Err(invalid_snapshot());
    }
    let lowered = value.to_ascii_lowercase();
    if ["%00", "%2e", "%2f", "%5c"]
        .iter()
        .any(|encoded| lowered.contains(encoded))
        || value
            .split('/')
            .any(|segment| matches!(segment, "." | ".."))
    {
        return Err(invalid_snapshot());
    }
    let normalized = if value == "/" {
        "/".to_string()
    } else {
        value.trim_end_matches('/').to_string()
    };
    Ok(normalized)
}

fn join_paths(base: &str, suffix: &str) -> String {
    if base == "/" {
        suffix.to_string()
    } else {
        format!("{}{}", base.trim_end_matches('/'), suffix)
    }
}

fn invalid_snapshot() -> ProviderDirectoryError {
    ProviderDirectoryError::new(ProviderDirectoryErrorCode::InvalidSnapshot)
}

fn map_contract_error(_: ContractError) -> ProviderDirectoryError {
    invalid_snapshot()
}

#[cfg(all(test, unix))]
mod source_tests {
    use super::*;
    #[tokio::test]
    async fn public_identity_cannot_acquire_http_transport_from_legacy_readiness()
    -> anyhow::Result<()> {
        let database = super::super::tests::test_database().await?;
        let (_, id) = super::super::tests::seed_provider(&database, 8110, json!({})).await?;
        let mut ready = ExtensionStore::new(&database.pool)
            .get_ready_live_catalog_provider(id)
            .await?
            .unwrap();
        snapshot_from_ready(ready.clone(), true)?;
        ready.extension_id = "elx:forged-public".into();
        assert_eq!(
            snapshot_from_ready(ready, true).unwrap_err().code(),
            ProviderDirectoryErrorCode::InvalidSnapshot
        );
        database.pool.close().await;
        Ok(())
    }
}
