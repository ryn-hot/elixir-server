use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::db::models::ProviderHealthState;
use crate::extensions::auto_managed::{is_nzbget_extension_id, is_qbittorrent_extension_id};
use crate::extensions::manifest::ExtensionManifest;
use crate::extensions::store::{ExtensionStore, ProviderDetails};
use crate::orchestrator::model::ProviderEndpoint;

pub const TORRENT_DEFAULT_LOGICAL_ID: &str = "downloaders.torrent.default";
pub const USENET_DEFAULT_LOGICAL_ID: &str = "downloaders.usenet.default";
pub const DEBRID_DEFAULT_LOGICAL_ID: &str = "acquisition.debrid.default";
pub const DEFAULT_ROUTE_OWNER_ID: &str = "default";
pub const DEBRID_ACCOUNT_MISSING_MESSAGE: &str = "Add debrid account";
pub const DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE: &str = "Active debrid service is not configured";
pub const DEBRID_SERVICE_UNAVAILABLE_MESSAGE: &str = "Active debrid service is unavailable";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadBrokerRole {
    Torrent,
    Usenet,
    DebridResolver,
}

impl DownloadBrokerRole {
    pub fn from_logical_id(logical_id: &str) -> Result<Self> {
        match logical_id.trim() {
            TORRENT_DEFAULT_LOGICAL_ID => Ok(Self::Torrent),
            USENET_DEFAULT_LOGICAL_ID => Ok(Self::Usenet),
            DEBRID_DEFAULT_LOGICAL_ID => Ok(Self::DebridResolver),
            other => bail!("unknown downloader logical id '{other}'"),
        }
    }

    pub fn logical_id(self) -> &'static str {
        match self {
            Self::Torrent => TORRENT_DEFAULT_LOGICAL_ID,
            Self::Usenet => USENET_DEFAULT_LOGICAL_ID,
            Self::DebridResolver => DEBRID_DEFAULT_LOGICAL_ID,
        }
    }

    fn from_capability(capability: &str) -> Option<Self> {
        match capability {
            "downloader.torrent" => Some(Self::Torrent),
            "downloader.nzb" => Some(Self::Usenet),
            "debrid.resolver" => Some(Self::DebridResolver),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadBrokerProviderKind {
    Managed,
    External,
    Debrid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadBrokerBindingKind {
    Auto,
    ManagedProtected,
    ManagedDirect,
    External,
    Debrid,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerInventory {
    pub downloaders: Vec<DownloadBrokerProviderRecord>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerProviderRecord {
    pub logical_id: String,
    pub broker_path: String,
    pub endpoints: DownloadBrokerEndpointContract,
    pub role: DownloadBrokerRole,
    pub provider_kind: DownloadBrokerProviderKind,
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub extension_id: String,
    pub capability: String,
    pub implementation: Option<String>,
    pub health_state: ProviderHealthState,
    pub selected_for_default: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerEndpointContract {
    pub base_path: String,
    pub submit_path: String,
    pub progress_path: String,
    pub cancel_path_template: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerRouteInventory {
    pub routes: Vec<DownloadBrokerRouteRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerRouteRecord {
    pub logical_id: String,
    pub owner_id: String,
    pub owner_label: String,
    pub role: DownloadBrokerRole,
    pub binding_kind: DownloadBrokerBindingKind,
    pub provider_id: Option<Uuid>,
    pub profile_id: Option<String>,
    pub status: String,
    pub inherited: bool,
    pub selected_provider_id: Option<Uuid>,
    pub selected_provider_kind: Option<DownloadBrokerProviderKind>,
    pub selected_extension_id: Option<String>,
    pub category: Option<String>,
    pub download_path: Option<String>,
    pub allow_shared_path: bool,
    pub candidates: Vec<DownloadBrokerRouteCandidate>,
    pub checks: Vec<DownloadBrokerRouteCheck>,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerRouteUpdate {
    #[serde(default = "default_binding_kind")]
    pub binding_kind: DownloadBrokerBindingKind,
    #[serde(default)]
    pub owner_id: Option<String>,
    #[serde(default)]
    pub provider_id: Option<Uuid>,
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub download_path: Option<String>,
    #[serde(default)]
    pub allow_shared_path: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerRouteCandidate {
    pub provider_id: Uuid,
    pub provider_kind: DownloadBrokerProviderKind,
    pub extension_id: String,
    pub implementation: Option<String>,
    pub health_state: ProviderHealthState,
    pub selected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadBrokerRouteCheckStatus {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerRouteCheck {
    pub code: String,
    pub status: DownloadBrokerRouteCheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedDownloadBrokerProvider {
    pub record: DownloadBrokerProviderRecord,
    pub binding_kind: DownloadBrokerBindingKind,
    pub category: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ProviderBrokerScopeDocument {
    #[serde(default)]
    download_broker: ProviderBrokerScope,
    #[serde(default)]
    broker: ProviderBrokerScope,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ProviderBrokerScope {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    provider_kind: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    logical_id: Option<String>,
}

pub async fn list_logical_downloaders(
    store: &ExtensionStore<'_>,
) -> Result<DownloadBrokerInventory> {
    let details = store.list_provider_details().await?;
    let instances = store
        .list_instances(None)
        .await?
        .into_iter()
        .filter(|instance| instance.enabled)
        .map(|instance| (instance.instance_id, instance))
        .collect::<HashMap<_, _>>();
    let enabled_extensions = store
        .list_extensions()
        .await?
        .into_iter()
        .filter(|extension| extension.enabled)
        .map(|extension| extension.extension_id)
        .collect::<HashSet<_>>();
    let mut records = Vec::new();

    for detail in details {
        let Some(instance) = instances.get(&detail.provider.instance_id) else {
            continue;
        };
        if instance.extension_id != detail.extension_id
            || !enabled_extensions.contains(&detail.extension_id)
        {
            continue;
        }
        let Some(role) = DownloadBrokerRole::from_capability(&detail.provider.capability) else {
            continue;
        };
        let scope = parse_broker_scope(detail.provider.scope_json.as_ref());
        if !broker_scope_enabled(&scope) {
            continue;
        }
        let Some(endpoint_json) = detail.provider.endpoint_json.as_ref() else {
            continue;
        };
        let _: ProviderEndpoint = serde_json::from_value(endpoint_json.clone())
            .context("parsing downloader provider endpoint")?;

        let logical_id = broker_scope_logical_id(&scope).unwrap_or_else(|| role.logical_id());
        if logical_id != role.logical_id() {
            continue;
        }
        let broker_path = broker_path(logical_id);

        records.push(DownloadBrokerProviderRecord {
            logical_id: logical_id.to_string(),
            broker_path: broker_path.clone(),
            endpoints: broker_endpoint_contract(logical_id),
            role,
            provider_kind: broker_provider_kind(&detail, &scope, role),
            provider_id: detail.provider.provider_id,
            instance_id: detail.provider.instance_id,
            extension_id: detail.extension_id,
            capability: detail.provider.capability,
            implementation: detail.provider.implementation,
            health_state: detail.provider.health_state,
            selected_for_default: false,
        });
    }

    mark_selected_defaults(&mut records);
    Ok(DownloadBrokerInventory {
        downloaders: records,
    })
}

pub async fn list_acquisition_routes(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
) -> Result<DownloadBrokerRouteInventory> {
    let inventory = list_logical_downloaders(store).await?;
    let mut routes = Vec::new();
    for role in [
        DownloadBrokerRole::Torrent,
        DownloadBrokerRole::Usenet,
        DownloadBrokerRole::DebridResolver,
    ] {
        let binding = load_route_binding(pool, role.logical_id(), DEFAULT_ROUTE_OWNER_ID).await?;
        routes.push(route_record_for_binding(
            role.logical_id(),
            DEFAULT_ROUTE_OWNER_ID,
            "Default",
            role,
            binding,
            None,
            &inventory.downloaders,
        ));
    }
    for spec in extension_acquisition_route_specs(store).await? {
        let binding = load_route_binding(pool, &spec.logical_id, &spec.owner_id).await?;
        let fallback = load_route_binding(pool, &spec.logical_id, DEFAULT_ROUTE_OWNER_ID).await?;
        routes.push(route_record_for_binding(
            &spec.logical_id,
            &spec.owner_id,
            &spec.owner_label,
            spec.role,
            binding,
            fallback,
            &inventory.downloaders,
        ));
    }
    annotate_route_collisions(&mut routes);
    Ok(DownloadBrokerRouteInventory { routes })
}

#[allow(dead_code)]
pub async fn resolve_logical_downloader(
    store: &ExtensionStore<'_>,
    logical_id: &str,
) -> Result<ResolvedDownloadBrokerProvider> {
    let role = DownloadBrokerRole::from_logical_id(logical_id)?;
    let inventory = list_logical_downloaders(store).await?;
    let candidates = inventory
        .downloaders
        .into_iter()
        .filter(|record| record.role == role && record.logical_id == role.logical_id())
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        bail!("no downloader provider is registered for '{logical_id}'");
    }

    if let Some(selected) = candidates
        .iter()
        .find(|record| record.selected_for_default)
        .cloned()
    {
        return Ok(ResolvedDownloadBrokerProvider {
            record: selected,
            binding_kind: DownloadBrokerBindingKind::Auto,
            category: None,
        });
    }

    Err(anyhow!(
        "multiple downloader providers are registered for '{logical_id}' and none is selected"
    ))
}

#[allow(dead_code)]
pub async fn resolve_logical_downloader_with_bindings(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    logical_id: &str,
) -> Result<ResolvedDownloadBrokerProvider> {
    resolve_logical_downloader_for_owner(pool, store, logical_id, DEFAULT_ROUTE_OWNER_ID).await
}

pub async fn resolve_logical_downloader_for_owner(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    logical_id: &str,
    owner_id: &str,
) -> Result<ResolvedDownloadBrokerProvider> {
    let role = DownloadBrokerRole::from_logical_id(logical_id)?;
    let owner_id = route_owner_id(Some(owner_id));
    let routes = list_acquisition_routes(pool, store).await?;
    let route = routes
        .routes
        .into_iter()
        .find(|route| route.logical_id == logical_id && route.owner_id == owner_id)
        .ok_or_else(|| {
            anyhow!(
                "no acquisition route '{}' is registered for owner '{}'",
                logical_id,
                owner_id
            )
        })?;
    if let Some(blocker) = route.blocker.as_ref() {
        bail!("{blocker}");
    }
    let Some(provider_id) = route.selected_provider_id else {
        bail!(
            "no downloader provider matches route binding '{}' for '{logical_id}'",
            route.binding_kind.as_str()
        );
    };

    let inventory = list_logical_downloaders(store).await?;
    let record = inventory
        .downloaders
        .into_iter()
        .find(|candidate| {
            candidate.provider_id == provider_id
                && candidate.role == role
                && candidate.logical_id == role.logical_id()
        })
        .ok_or_else(|| anyhow!("selected provider '{provider_id}' is not a broker candidate"))?;

    Ok(ResolvedDownloadBrokerProvider {
        record,
        binding_kind: route.binding_kind,
        category: route.category,
    })
}

pub async fn upsert_acquisition_route(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    logical_id: &str,
    update: DownloadBrokerRouteUpdate,
) -> Result<DownloadBrokerRouteRecord> {
    let role = DownloadBrokerRole::from_logical_id(logical_id)?;
    validate_route_update(role, &update)?;
    let owner_id = route_owner_id(update.owner_id.as_deref());
    let category = normalize_optional_route_category(update.category.as_deref())?;
    let download_path = normalize_optional_download_path(update.download_path.as_deref())?;
    let allow_shared_path = if update.allow_shared_path.unwrap_or(false) {
        1_i64
    } else {
        0_i64
    };
    if let Some(provider_id) = update.provider_id {
        let inventory = list_logical_downloaders(store).await?;
        let provider = inventory
            .downloaders
            .iter()
            .find(|record| record.provider_id == provider_id)
            .ok_or_else(|| anyhow!("provider '{provider_id}' is not a broker candidate"))?;
        if provider.role != role {
            bail!(
                "provider '{}' is for role '{:?}', not '{:?}'",
                provider_id,
                provider.role,
                role
            );
        }
        if !binding_kind_accepts_provider(update.binding_kind, provider.provider_kind) {
            bail!(
                "provider '{}' does not match binding kind '{}'",
                provider_id,
                update.binding_kind.as_str()
            );
        }
    }

    sqlx::query::<sqlx::Any>(
        "DELETE FROM download_provider_bindings WHERE logical_role = ? AND owner_id = ?",
    )
    .bind(logical_id)
    .bind(&owner_id)
    .execute(pool)
    .await
    .context("clearing previous acquisition route binding")?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO download_provider_bindings (id, logical_role, owner_id, binding_kind, provider_id, profile_id, category, download_path, allow_shared_path, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(logical_id)
    .bind(&owner_id)
    .bind(update.binding_kind.as_str())
    .bind(update.provider_id.map(|value| value.to_string()))
    .bind(update.profile_id.as_deref().map(str::trim).filter(|value| !value.is_empty()))
    .bind(category.as_deref())
    .bind(download_path.as_deref())
    .bind(allow_shared_path)
    .bind(update.status.as_deref().unwrap_or("selected"))
    .execute(pool)
    .await
    .context("storing acquisition route binding")?;

    let routes = list_acquisition_routes(pool, store).await?;
    routes
        .routes
        .into_iter()
        .find(|route| route.logical_id == logical_id && route.owner_id == owner_id)
        .ok_or_else(|| anyhow!("stored acquisition route was not readable"))
}

pub fn broker_path(logical_id: &str) -> String {
    format!("/api/v1/download-broker/{logical_id}")
}

fn broker_endpoint_contract(logical_id: &str) -> DownloadBrokerEndpointContract {
    let base_path = broker_path(logical_id);
    DownloadBrokerEndpointContract {
        submit_path: format!("{base_path}/submit"),
        progress_path: format!("{base_path}/progress"),
        cancel_path_template: format!("{base_path}/items/{{downloadId}}"),
        base_path,
    }
}

#[derive(Debug, Clone)]
struct StoredRouteBinding {
    binding_kind: DownloadBrokerBindingKind,
    provider_id: Option<Uuid>,
    profile_id: Option<String>,
    category: Option<String>,
    download_path: Option<String>,
    allow_shared_path: bool,
    status: String,
}

async fn load_route_binding(
    pool: &sqlx::AnyPool,
    logical_id: &str,
    owner_id: &str,
) -> Result<Option<StoredRouteBinding>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT binding_kind, COALESCE(CAST(provider_id AS TEXT), '') as provider_id, COALESCE(CAST(profile_id AS TEXT), '') as profile_id, COALESCE(CAST(category AS TEXT), '') as category, COALESCE(CAST(download_path AS TEXT), '') as download_path, CASE WHEN LOWER(CAST(allow_shared_path AS TEXT)) IN ('1', 'true', 't') THEN 1 ELSE 0 END as allow_shared_path, status FROM download_provider_bindings WHERE logical_role = ? AND owner_id = ? ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(logical_id)
    .bind(owner_id)
    .fetch_optional(pool)
    .await
    .context("loading acquisition route binding")?;
    let Some(row) = row else {
        return Ok(None);
    };
    let binding_kind_raw: String = row.try_get("binding_kind")?;
    let provider_id_raw: String = row.try_get("provider_id")?;
    let provider_id = if provider_id_raw.trim().is_empty() {
        None
    } else {
        Some(
            Uuid::parse_str(provider_id_raw.trim())
                .context("download provider binding provider_id is not a valid UUID")?,
        )
    };
    let profile_id_raw: String = row.try_get("profile_id")?;
    let profile_id = if profile_id_raw.trim().is_empty() {
        None
    } else {
        Some(profile_id_raw.trim().to_string())
    };
    let category_raw: String = row.try_get("category")?;
    let category = category_raw
        .trim()
        .is_empty()
        .then_some(None)
        .unwrap_or_else(|| Some(category_raw.trim().to_string()));
    let download_path_raw: String = row.try_get("download_path")?;
    let download_path = download_path_raw
        .trim()
        .is_empty()
        .then_some(None)
        .unwrap_or_else(|| Some(download_path_raw.trim().to_string()));
    let allow_shared_path_raw: i64 = row.try_get("allow_shared_path")?;
    Ok(Some(StoredRouteBinding {
        binding_kind: parse_binding_kind(&binding_kind_raw)?,
        provider_id,
        profile_id,
        category,
        download_path,
        allow_shared_path: allow_shared_path_raw != 0,
        status: row.try_get("status")?,
    }))
}

fn route_record_for_binding(
    logical_id: &str,
    owner_id: &str,
    owner_label: &str,
    role: DownloadBrokerRole,
    binding: Option<StoredRouteBinding>,
    fallback_binding: Option<StoredRouteBinding>,
    providers: &[DownloadBrokerProviderRecord],
) -> DownloadBrokerRouteRecord {
    let inherited = binding.is_none() && fallback_binding.is_some();
    let effective_binding = binding.as_ref().or(fallback_binding.as_ref());
    let binding_kind = effective_binding
        .as_ref()
        .map(|binding| binding.binding_kind)
        .unwrap_or(DownloadBrokerBindingKind::Auto);
    let candidates = providers
        .iter()
        .filter(|record| record.role == role && record.logical_id == role.logical_id())
        .cloned()
        .collect::<Vec<_>>();
    let selected = select_provider_for_binding(role, &candidates, effective_binding);
    let mut checks = Vec::new();
    let debrid_default_ambiguous = role == DownloadBrokerRole::DebridResolver
        && selected.is_none()
        && candidates.len() > 1
        && effective_binding
            .and_then(|binding| binding.provider_id)
            .is_none();
    let debrid_selected_unhealthy = selected.as_ref().is_some_and(|record| {
        role == DownloadBrokerRole::DebridResolver
            && record.health_state == ProviderHealthState::Unhealthy
    });
    let blocker = if selected.is_some() && !debrid_selected_unhealthy {
        checks.push(route_check(
            "download_route_provider_selected",
            DownloadBrokerRouteCheckStatus::Pass,
            "The route resolves to a concrete acquisition provider.",
        ));
        None
    } else if debrid_selected_unhealthy {
        checks.push(route_check(
            "download_route_debrid_service_unavailable",
            DownloadBrokerRouteCheckStatus::Fail,
            DEBRID_SERVICE_UNAVAILABLE_MESSAGE,
        ));
        Some(DEBRID_SERVICE_UNAVAILABLE_MESSAGE.to_string())
    } else if debrid_default_ambiguous {
        let detail = if role == DownloadBrokerRole::DebridResolver {
            DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE
        } else {
            "Multiple debrid resolver providers are registered. Select one default debrid provider explicitly."
        };
        checks.push(route_check(
            "download_route_debrid_default_ambiguous",
            DownloadBrokerRouteCheckStatus::Fail,
            detail,
        ));
        Some(detail.to_string())
    } else if candidates.is_empty() {
        let detail = if role == DownloadBrokerRole::DebridResolver {
            DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE
        } else {
            "No provider is registered for this acquisition route."
        };
        checks.push(route_check(
            "download_route_provider_missing",
            DownloadBrokerRouteCheckStatus::Fail,
            detail,
        ));
        Some(detail.to_string())
    } else {
        let detail = if role == DownloadBrokerRole::DebridResolver {
            DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE.to_string()
        } else {
            format!(
                "No provider matches binding kind '{}'.",
                binding_kind.as_str()
            )
        };
        checks.push(route_check(
            "download_route_provider_binding_unmatched",
            DownloadBrokerRouteCheckStatus::Fail,
            &detail,
        ));
        Some(detail)
    };
    let owner_id = route_owner_id(Some(owner_id));
    let category = effective_binding
        .and_then(|binding| binding.category.clone())
        .or_else(|| default_route_category(&owner_id, role));
    let download_path = effective_binding
        .and_then(|binding| binding.download_path.clone())
        .or_else(|| {
            category
                .as_ref()
                .map(|category| format!("/downloads/{category}"))
        });
    let allow_shared_path = effective_binding
        .map(|binding| binding.allow_shared_path)
        .unwrap_or(false);
    if owner_id != DEFAULT_ROUTE_OWNER_ID {
        checks.push(route_check(
            "download_route_namespace",
            DownloadBrokerRouteCheckStatus::Pass,
            &format!(
                "Acquisition route '{}' uses category '{}' for owner '{}'.",
                logical_id,
                category.as_deref().unwrap_or(""),
                owner_id
            ),
        ));
    }
    if inherited {
        checks.push(route_check(
            "download_route_inherits_default",
            DownloadBrokerRouteCheckStatus::Warn,
            "This extension route inherits the default acquisition provider until an override is selected.",
        ));
    }
    let selected_provider_id = selected.as_ref().map(|record| record.provider_id);
    let candidates = candidates
        .iter()
        .map(|record| DownloadBrokerRouteCandidate {
            provider_id: record.provider_id,
            provider_kind: record.provider_kind,
            extension_id: record.extension_id.clone(),
            implementation: record.implementation.clone(),
            health_state: record.health_state,
            selected: selected_provider_id == Some(record.provider_id),
        })
        .collect::<Vec<_>>();

    DownloadBrokerRouteRecord {
        logical_id: logical_id.to_string(),
        owner_id,
        owner_label: owner_label.to_string(),
        role,
        binding_kind,
        provider_id: binding.as_ref().and_then(|binding| binding.provider_id),
        profile_id: binding
            .as_ref()
            .and_then(|binding| binding.profile_id.clone()),
        status: if inherited {
            "inherited".to_string()
        } else {
            binding
                .as_ref()
                .map(|binding| binding.status.clone())
                .unwrap_or_else(|| "auto".to_string())
        },
        inherited,
        selected_provider_id,
        selected_provider_kind: selected.as_ref().map(|record| record.provider_kind),
        selected_extension_id: selected.as_ref().map(|record| record.extension_id.clone()),
        category,
        download_path,
        allow_shared_path,
        candidates,
        checks,
        blocker,
    }
}

fn select_provider_for_binding(
    role: DownloadBrokerRole,
    candidates: &[DownloadBrokerProviderRecord],
    binding: Option<&StoredRouteBinding>,
) -> Option<DownloadBrokerProviderRecord> {
    if let Some(provider_id) = binding.and_then(|binding| binding.provider_id) {
        return candidates
            .iter()
            .find(|record| record.provider_id == provider_id)
            .cloned();
    }
    let binding_kind = binding
        .map(|binding| binding.binding_kind)
        .unwrap_or(DownloadBrokerBindingKind::Auto);
    let mut filtered = candidates
        .iter()
        .filter(|record| binding_kind_accepts_provider(binding_kind, record.provider_kind))
        .cloned()
        .collect::<Vec<_>>();
    if role == DownloadBrokerRole::DebridResolver && filtered.len() > 1 {
        return None;
    }
    filtered.sort_by_key(|record| {
        (
            health_rank(record.health_state),
            provider_kind_rank(record.provider_kind),
            record.extension_id.clone(),
            record.provider_id,
        )
    });
    filtered.first().cloned()
}

fn validate_route_update(
    role: DownloadBrokerRole,
    update: &DownloadBrokerRouteUpdate,
) -> Result<()> {
    match update.binding_kind {
        DownloadBrokerBindingKind::Debrid if role != DownloadBrokerRole::DebridResolver => {
            bail!("debrid binding can only be used with the debrid resolver route")
        }
        DownloadBrokerBindingKind::ManagedProtected | DownloadBrokerBindingKind::ManagedDirect
            if role == DownloadBrokerRole::DebridResolver =>
        {
            bail!("managed downloader bindings cannot be used with the debrid resolver route")
        }
        DownloadBrokerBindingKind::External
        | DownloadBrokerBindingKind::Auto
        | DownloadBrokerBindingKind::ManagedProtected
        | DownloadBrokerBindingKind::ManagedDirect
        | DownloadBrokerBindingKind::Debrid => Ok(()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcquisitionRouteSpec {
    owner_id: String,
    owner_label: String,
    logical_id: String,
    role: DownloadBrokerRole,
}

async fn extension_acquisition_route_specs(
    store: &ExtensionStore<'_>,
) -> Result<Vec<AcquisitionRouteSpec>> {
    let mut specs = Vec::new();
    for extension in store
        .list_extensions()
        .await?
        .into_iter()
        .filter(|extension| extension.enabled)
    {
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        else {
            continue;
        };
        for download in &manifest.requires.downloads {
            let logical_id = download.resolved_logical_id().to_string();
            let role = DownloadBrokerRole::from_logical_id(&logical_id)?;
            specs.push(AcquisitionRouteSpec {
                owner_id: route_owner_id(Some(&extension.extension_id)),
                owner_label: extension.name.clone(),
                logical_id,
                role,
            });
        }
    }
    specs.sort_by(|left, right| {
        left.owner_id
            .cmp(&right.owner_id)
            .then(left.logical_id.cmp(&right.logical_id))
    });
    specs.dedup_by(|left, right| {
        left.owner_id == right.owner_id && left.logical_id == right.logical_id
    });
    Ok(specs)
}

fn annotate_route_collisions(routes: &mut [DownloadBrokerRouteRecord]) {
    for route in routes.iter_mut() {
        if route.owner_id != DEFAULT_ROUTE_OWNER_ID && route.selected_provider_id.is_some() {
            route.checks.push(route_check(
                "download_route_collision_clear",
                DownloadBrokerRouteCheckStatus::Pass,
                "No category or download path collision has been detected for this route.",
            ));
        }
    }

    let mut failures: Vec<(usize, DownloadBrokerRouteCheck)> = Vec::new();
    for left_idx in 0..routes.len() {
        for right_idx in (left_idx + 1)..routes.len() {
            let left = &routes[left_idx];
            let right = &routes[right_idx];
            if left.owner_id == DEFAULT_ROUTE_OWNER_ID
                || right.owner_id == DEFAULT_ROUTE_OWNER_ID
                || left.selected_provider_id.is_none()
                || left.selected_provider_id != right.selected_provider_id
            {
                continue;
            }
            if let (Some(left_category), Some(right_category)) =
                (left.category.as_deref(), right.category.as_deref())
            {
                if left_category.eq_ignore_ascii_case(right_category) {
                    let detail = format!(
                        "Routes '{}' and '{}' both use provider '{}' category '{}'. Categories must be unique per owner unless the route is intentionally shared.",
                        left.owner_label,
                        right.owner_label,
                        left.selected_provider_id.expect("selected provider"),
                        left_category
                    );
                    failures.push((
                        left_idx,
                        route_check(
                            "download_route_category_collision",
                            DownloadBrokerRouteCheckStatus::Fail,
                            &detail,
                        ),
                    ));
                    failures.push((
                        right_idx,
                        route_check(
                            "download_route_category_collision",
                            DownloadBrokerRouteCheckStatus::Fail,
                            &detail,
                        ),
                    ));
                }
            }
            if left.allow_shared_path || right.allow_shared_path {
                continue;
            }
            if let (Some(left_path), Some(right_path)) = (
                left.download_path.as_deref(),
                right.download_path.as_deref(),
            ) {
                if normalize_path_for_compare(left_path) == normalize_path_for_compare(right_path) {
                    let detail = format!(
                        "Routes '{}' and '{}' both import from '{}'. Shared import paths must be explicitly allowed.",
                        left.owner_label, right.owner_label, left_path
                    );
                    failures.push((
                        left_idx,
                        route_check(
                            "download_route_path_collision",
                            DownloadBrokerRouteCheckStatus::Fail,
                            &detail,
                        ),
                    ));
                    failures.push((
                        right_idx,
                        route_check(
                            "download_route_path_collision",
                            DownloadBrokerRouteCheckStatus::Fail,
                            &detail,
                        ),
                    ));
                }
            }
        }
    }

    for (idx, failure) in failures {
        let route = &mut routes[idx];
        route.checks.retain(|check| {
            check.code != "download_route_collision_clear"
                || check.status != DownloadBrokerRouteCheckStatus::Pass
        });
        if route.blocker.is_none() {
            route.blocker = Some(failure.detail.clone());
        }
        route.checks.push(failure);
    }
}

fn route_check(
    code: &str,
    status: DownloadBrokerRouteCheckStatus,
    detail: &str,
) -> DownloadBrokerRouteCheck {
    DownloadBrokerRouteCheck {
        code: code.to_string(),
        status,
        detail: detail.to_string(),
    }
}

fn route_owner_id(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_ROUTE_OWNER_ID)
        .to_string()
}

fn default_route_category(owner_id: &str, role: DownloadBrokerRole) -> Option<String> {
    if owner_id == DEFAULT_ROUTE_OWNER_ID {
        return None;
    }
    Some(format!(
        "{}-{}",
        route_owner_slug(owner_id),
        match role {
            DownloadBrokerRole::Torrent => "torrent",
            DownloadBrokerRole::Usenet => "usenet",
            DownloadBrokerRole::DebridResolver => "debrid",
        }
    ))
}

fn route_owner_slug(owner_id: &str) -> String {
    let mut slug = String::new();
    for ch in owner_id.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '.' | '_' | '-' | ' ') && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        "extension".to_string()
    } else {
        slug.to_string()
    }
}

fn normalize_optional_route_category(value: Option<&str>) -> Result<Option<String>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.contains('/') || value.contains('\\') || value.contains("..") {
        bail!("route category must be a single namespaced category segment");
    }
    Ok(Some(value.to_string()))
}

fn normalize_optional_download_path(value: Option<&str>) -> Result<Option<String>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if !value.starts_with('/') || value == "/" || value.contains("..") {
        bail!("download path must be an absolute non-root path without '..'");
    }
    Ok(Some(value.trim_end_matches('/').to_string()))
}

fn normalize_path_for_compare(value: &str) -> String {
    value.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn binding_kind_accepts_provider(
    binding_kind: DownloadBrokerBindingKind,
    provider_kind: DownloadBrokerProviderKind,
) -> bool {
    match binding_kind {
        DownloadBrokerBindingKind::Auto => true,
        DownloadBrokerBindingKind::ManagedProtected | DownloadBrokerBindingKind::ManagedDirect => {
            provider_kind == DownloadBrokerProviderKind::Managed
        }
        DownloadBrokerBindingKind::External => {
            provider_kind == DownloadBrokerProviderKind::External
        }
        DownloadBrokerBindingKind::Debrid => provider_kind == DownloadBrokerProviderKind::Debrid,
    }
}

fn default_binding_kind() -> DownloadBrokerBindingKind {
    DownloadBrokerBindingKind::Auto
}

fn parse_binding_kind(value: &str) -> Result<DownloadBrokerBindingKind> {
    match value.trim() {
        "auto" => Ok(DownloadBrokerBindingKind::Auto),
        "managed_protected" => Ok(DownloadBrokerBindingKind::ManagedProtected),
        "managed_direct" => Ok(DownloadBrokerBindingKind::ManagedDirect),
        "external" => Ok(DownloadBrokerBindingKind::External),
        "debrid" => Ok(DownloadBrokerBindingKind::Debrid),
        other => bail!("invalid download provider binding kind '{other}'"),
    }
}

impl DownloadBrokerBindingKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::ManagedProtected => "managed_protected",
            Self::ManagedDirect => "managed_direct",
            Self::External => "external",
            Self::Debrid => "debrid",
        }
    }
}

fn parse_broker_scope(scope_json: Option<&Value>) -> ProviderBrokerScopeDocument {
    scope_json
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

fn broker_scope_enabled(scope: &ProviderBrokerScopeDocument) -> bool {
    scope
        .download_broker
        .enabled
        .or(scope.broker.enabled)
        .unwrap_or(true)
}

fn broker_scope_logical_id(scope: &ProviderBrokerScopeDocument) -> Option<&str> {
    scope
        .download_broker
        .logical_id
        .as_deref()
        .or(scope.broker.logical_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn broker_provider_kind(
    detail: &ProviderDetails,
    scope: &ProviderBrokerScopeDocument,
    role: DownloadBrokerRole,
) -> DownloadBrokerProviderKind {
    let explicit = scope
        .download_broker
        .provider_kind
        .as_deref()
        .or(scope.download_broker.kind.as_deref())
        .or(scope.broker.provider_kind.as_deref())
        .or(scope.broker.kind.as_deref())
        .map(|value| value.trim().to_ascii_lowercase());
    match explicit.as_deref() {
        Some("managed") => return DownloadBrokerProviderKind::Managed,
        Some("external") => return DownloadBrokerProviderKind::External,
        Some("debrid") => return DownloadBrokerProviderKind::Debrid,
        _ => {}
    }

    match role {
        DownloadBrokerRole::Torrent if is_qbittorrent_extension_id(&detail.extension_id) => {
            DownloadBrokerProviderKind::Managed
        }
        DownloadBrokerRole::Usenet if is_nzbget_extension_id(&detail.extension_id) => {
            DownloadBrokerProviderKind::Managed
        }
        DownloadBrokerRole::DebridResolver => DownloadBrokerProviderKind::Debrid,
        _ => DownloadBrokerProviderKind::External,
    }
}

fn mark_selected_defaults(records: &mut [DownloadBrokerProviderRecord]) {
    for role in [
        DownloadBrokerRole::Torrent,
        DownloadBrokerRole::Usenet,
        DownloadBrokerRole::DebridResolver,
    ] {
        if let Some(provider_id) = select_default_provider_id(records, role) {
            for record in records.iter_mut().filter(|record| record.role == role) {
                record.selected_for_default = record.provider_id == provider_id;
            }
        }
    }
}

fn select_default_provider_id(
    records: &[DownloadBrokerProviderRecord],
    role: DownloadBrokerRole,
) -> Option<Uuid> {
    let mut candidates = records
        .iter()
        .filter(|record| record.role == role)
        .collect::<Vec<_>>();
    if role == DownloadBrokerRole::DebridResolver && candidates.len() != 1 {
        return None;
    }
    candidates.sort_by_key(|record| {
        (
            provider_kind_rank(record.provider_kind),
            health_rank(record.health_state),
            record.extension_id.clone(),
            record.provider_id,
        )
    });
    candidates.first().map(|record| record.provider_id)
}

fn provider_kind_rank(kind: DownloadBrokerProviderKind) -> u8 {
    match kind {
        DownloadBrokerProviderKind::Managed => 0,
        DownloadBrokerProviderKind::External => 1,
        DownloadBrokerProviderKind::Debrid => 2,
    }
}

fn health_rank(state: ProviderHealthState) -> u8 {
    match state {
        ProviderHealthState::Healthy => 0,
        ProviderHealthState::Degraded => 1,
        ProviderHealthState::Unknown => 2,
        ProviderHealthState::Unhealthy => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Result;
    use serde_json::json;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality};
    use crate::extensions::store::{
        ExtensionStore, NewExtension, NewExtensionInstance, NewProvider,
    };

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn insert_extension_with_enabled(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        enabled: bool,
    ) -> Result<()> {
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: extension_id.to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({ "id": extension_id, "version": "1.0.0" }),
                package_hash: None,
                enabled,
            })
            .await
    }

    async fn insert_downloader_provider(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        capability: &str,
        implementation: &str,
        host: &str,
        scope_json: Option<Value>,
        health_state: ProviderHealthState,
    ) -> Result<Uuid> {
        insert_downloader_provider_with_enabled(
            store,
            extension_id,
            capability,
            implementation,
            host,
            scope_json,
            health_state,
            true,
            true,
        )
        .await
    }

    async fn insert_downloader_provider_with_enabled(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        capability: &str,
        implementation: &str,
        host: &str,
        scope_json: Option<Value>,
        health_state: ProviderHealthState,
        extension_enabled: bool,
        instance_enabled: bool,
    ) -> Result<Uuid> {
        insert_extension_with_enabled(store, extension_id, extension_enabled).await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: instance_enabled,
            })
            .await?;
        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            host.to_string(),
            if capability == "downloader.nzb" {
                6789
            } else {
                8080
            },
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: capability.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some(implementation.to_string()),
                scope_json,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state,
            })
            .await?;
        Ok(provider_id)
    }

    async fn insert_acquisition_extension(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        name: &str,
        downloads: &[&str],
    ) -> Result<()> {
        let download_requires = downloads
            .iter()
            .map(|kind| {
                json!({
                    "kind": *kind,
                    "mode": "broker"
                })
            })
            .collect::<Vec<_>>();
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: name.to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": name,
                    "requires": {
                        "downloads": download_requires
                    }
                }),
                package_hash: None,
                enabled: true,
            })
            .await
    }

    #[tokio::test]
    async fn broker_inventory_lists_managed_and_external_torrent_providers() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let managed_id = insert_downloader_provider(
            &store,
            "elixir.modules.qbittorrent",
            "downloader.torrent",
            "qbittorrent",
            "svc-qbittorrent",
            None,
            ProviderHealthState::Unknown,
        )
        .await?;
        let external_id = insert_downloader_provider(
            &store,
            "external.stack.qbit",
            "downloader.torrent",
            "qbittorrent",
            "external-qbit",
            Some(json!({ "download_broker": { "provider_kind": "external" } })),
            ProviderHealthState::Healthy,
        )
        .await?;

        let inventory = list_logical_downloaders(&store).await?;
        assert_eq!(inventory.downloaders.len(), 2);
        let managed = inventory
            .downloaders
            .iter()
            .find(|record| record.provider_id == managed_id)
            .expect("managed provider");
        assert_eq!(managed.provider_kind, DownloadBrokerProviderKind::Managed);
        assert!(managed.selected_for_default);
        assert_eq!(managed.logical_id, TORRENT_DEFAULT_LOGICAL_ID);
        assert_eq!(
            managed.endpoints.submit_path,
            "/api/v1/download-broker/downloaders.torrent.default/submit"
        );

        let external = inventory
            .downloaders
            .iter()
            .find(|record| record.provider_id == external_id)
            .expect("external provider");
        assert_eq!(external.provider_kind, DownloadBrokerProviderKind::External);
        assert!(!external.selected_for_default);
        assert_eq!(external.logical_id, TORRENT_DEFAULT_LOGICAL_ID);

        let serialized = serde_json::to_value(&inventory)?;
        let first = serialized
            .get("downloaders")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .expect("serialized downloader");
        assert!(first.get("endpoint").is_none());
        assert!(first.get("endpoints").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn broker_resolver_prefers_managed_when_external_coexists() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let managed_id = insert_downloader_provider(
            &store,
            "elixir.modules.nzbget",
            "downloader.nzb",
            "nzbget",
            "svc-nzbget",
            None,
            ProviderHealthState::Unknown,
        )
        .await?;
        insert_downloader_provider(
            &store,
            "external.stack.nzb",
            "downloader.nzb",
            "nzbget",
            "external-nzbget",
            Some(json!({ "download_broker": { "provider_kind": "external" } })),
            ProviderHealthState::Healthy,
        )
        .await?;

        let resolved = resolve_logical_downloader(&store, USENET_DEFAULT_LOGICAL_ID).await?;
        assert_eq!(resolved.record.provider_id, managed_id);
        assert_eq!(
            resolved.record.provider_kind,
            DownloadBrokerProviderKind::Managed
        );
        Ok(())
    }

    #[tokio::test]
    async fn route_binding_can_select_external_torrent_provider() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_downloader_provider(
            &store,
            "elixir.modules.qbittorrent",
            "downloader.torrent",
            "qbittorrent",
            "svc-qbittorrent",
            None,
            ProviderHealthState::Healthy,
        )
        .await?;
        let external_id = insert_downloader_provider(
            &store,
            "external.stack.qbit",
            "downloader.torrent",
            "qbittorrent",
            "external-qbit",
            Some(json!({ "download_broker": { "provider_kind": "external" } })),
            ProviderHealthState::Healthy,
        )
        .await?;

        let route = upsert_acquisition_route(
            &database.pool,
            &store,
            TORRENT_DEFAULT_LOGICAL_ID,
            DownloadBrokerRouteUpdate {
                binding_kind: DownloadBrokerBindingKind::External,
                owner_id: None,
                provider_id: None,
                profile_id: None,
                category: None,
                download_path: None,
                allow_shared_path: None,
                status: None,
            },
        )
        .await?;
        assert_eq!(route.binding_kind, DownloadBrokerBindingKind::External);
        assert_eq!(route.selected_provider_id, Some(external_id));
        assert!(route.blocker.is_none());

        let resolved = resolve_logical_downloader_with_bindings(
            &database.pool,
            &store,
            TORRENT_DEFAULT_LOGICAL_ID,
        )
        .await?;
        assert_eq!(resolved.record.provider_id, external_id);
        assert_eq!(resolved.binding_kind, DownloadBrokerBindingKind::External);
        Ok(())
    }

    #[tokio::test]
    async fn debrid_resolver_route_is_visible_and_selectable() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let debrid_id = insert_downloader_provider(
            &store,
            "external.real_debrid",
            "debrid.resolver",
            "real_debrid",
            "real-debrid",
            Some(json!({ "download_broker": { "provider_kind": "debrid" } })),
            ProviderHealthState::Healthy,
        )
        .await?;

        let routes = list_acquisition_routes(&database.pool, &store).await?;
        let debrid = routes
            .routes
            .iter()
            .find(|route| route.logical_id == DEBRID_DEFAULT_LOGICAL_ID)
            .expect("debrid route");
        assert_eq!(debrid.role, DownloadBrokerRole::DebridResolver);
        assert_eq!(debrid.selected_provider_id, Some(debrid_id));

        let route = upsert_acquisition_route(
            &database.pool,
            &store,
            DEBRID_DEFAULT_LOGICAL_ID,
            DownloadBrokerRouteUpdate {
                binding_kind: DownloadBrokerBindingKind::Debrid,
                owner_id: None,
                provider_id: Some(debrid_id),
                profile_id: None,
                category: None,
                download_path: None,
                allow_shared_path: None,
                status: Some("selected".to_string()),
            },
        )
        .await?;
        assert_eq!(route.provider_id, Some(debrid_id));
        assert_eq!(route.binding_kind, DownloadBrokerBindingKind::Debrid);
        assert!(route.blocker.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn future_debrid_provider_scope_binds_to_default_debrid_route() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let provider_id = insert_downloader_provider(
            &store,
            "community.premiumize",
            "debrid.resolver",
            "premiumize",
            "premiumize-extension",
            Some(json!({
                "download_broker": {
                    "enabled": true,
                    "provider_kind": "debrid",
                    "logical_id": DEBRID_DEFAULT_LOGICAL_ID,
                    "capabilities": {
                        "magnetSubmit": true,
                        "fileListing": true,
                        "fileSelection": true,
                        "fileSelectionMode": "before_transfer"
                    }
                }
            })),
            ProviderHealthState::Healthy,
        )
        .await?;

        let inventory = list_logical_downloaders(&store).await?;
        let provider = inventory
            .downloaders
            .iter()
            .find(|record| record.provider_id == provider_id)
            .expect("future debrid provider");
        assert_eq!(provider.logical_id, DEBRID_DEFAULT_LOGICAL_ID);
        assert_eq!(provider.role, DownloadBrokerRole::DebridResolver);
        assert_eq!(provider.provider_kind, DownloadBrokerProviderKind::Debrid);
        assert_eq!(provider.implementation.as_deref(), Some("premiumize"));
        assert!(provider.selected_for_default);

        let resolved = resolve_logical_downloader_for_owner(
            &database.pool,
            &store,
            DEBRID_DEFAULT_LOGICAL_ID,
            DEFAULT_ROUTE_OWNER_ID,
        )
        .await?;
        assert_eq!(resolved.record.provider_id, provider_id);
        assert_eq!(
            resolved.record.implementation.as_deref(),
            Some("premiumize")
        );
        assert_eq!(resolved.binding_kind, DownloadBrokerBindingKind::Auto);
        Ok(())
    }

    #[tokio::test]
    async fn debrid_default_requires_explicit_binding_when_multiple_providers_exist() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let real_debrid_id = insert_downloader_provider(
            &store,
            "elixir.modules.real_debrid",
            "debrid.resolver",
            "real_debrid",
            "real-debrid",
            Some(json!({ "download_broker": { "provider_kind": "debrid", "logical_id": DEBRID_DEFAULT_LOGICAL_ID } })),
            ProviderHealthState::Healthy,
        )
        .await?;
        let premiumize_id = insert_downloader_provider(
            &store,
            "community.premiumize",
            "debrid.resolver",
            "premiumize",
            "premiumize-extension",
            Some(json!({ "download_broker": { "provider_kind": "debrid", "logical_id": DEBRID_DEFAULT_LOGICAL_ID } })),
            ProviderHealthState::Healthy,
        )
        .await?;

        let inventory = list_logical_downloaders(&store).await?;
        assert!(inventory.downloaders.iter().any(|record| {
            record.provider_id == real_debrid_id && !record.selected_for_default
        }));
        assert!(
            inventory.downloaders.iter().any(|record| {
                record.provider_id == premiumize_id && !record.selected_for_default
            })
        );

        let routes = list_acquisition_routes(&database.pool, &store).await?;
        let debrid = routes
            .routes
            .iter()
            .find(|route| route.logical_id == DEBRID_DEFAULT_LOGICAL_ID)
            .expect("debrid route");
        assert_eq!(debrid.candidates.len(), 2);
        assert_eq!(debrid.selected_provider_id, None);
        assert!(
            debrid
                .blocker
                .as_deref()
                .unwrap_or_default()
                .contains(DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE)
        );
        assert!(debrid.checks.iter().any(|check| {
            check.code == "download_route_debrid_default_ambiguous"
                && check.status == DownloadBrokerRouteCheckStatus::Fail
        }));

        let err = resolve_logical_downloader_for_owner(
            &database.pool,
            &store,
            DEBRID_DEFAULT_LOGICAL_ID,
            DEFAULT_ROUTE_OWNER_ID,
        )
        .await
        .expect_err("ambiguous debrid route should not resolve");
        assert!(
            err.to_string()
                .contains(DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE)
        );

        let route = upsert_acquisition_route(
            &database.pool,
            &store,
            DEBRID_DEFAULT_LOGICAL_ID,
            DownloadBrokerRouteUpdate {
                binding_kind: DownloadBrokerBindingKind::Debrid,
                owner_id: None,
                provider_id: Some(premiumize_id),
                profile_id: None,
                category: None,
                download_path: None,
                allow_shared_path: None,
                status: Some("selected".to_string()),
            },
        )
        .await?;
        assert_eq!(route.selected_provider_id, Some(premiumize_id));
        assert!(route.blocker.is_none());

        let resolved = resolve_logical_downloader_for_owner(
            &database.pool,
            &store,
            DEBRID_DEFAULT_LOGICAL_ID,
            DEFAULT_ROUTE_OWNER_ID,
        )
        .await?;
        assert_eq!(resolved.record.provider_id, premiumize_id);
        assert_eq!(
            resolved.record.implementation.as_deref(),
            Some("premiumize")
        );
        assert_eq!(resolved.binding_kind, DownloadBrokerBindingKind::Debrid);
        Ok(())
    }

    #[tokio::test]
    async fn extension_route_can_override_default_without_rewiring_external_stack() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let managed_id = insert_downloader_provider(
            &store,
            "elixir.modules.qbittorrent",
            "downloader.torrent",
            "qbittorrent",
            "svc-qbittorrent",
            None,
            ProviderHealthState::Healthy,
        )
        .await?;
        let external_id = insert_downloader_provider(
            &store,
            "external.stack.qbit",
            "downloader.torrent",
            "qbittorrent",
            "external-qbit",
            Some(json!({ "download_broker": { "provider_kind": "external" } })),
            ProviderHealthState::Healthy,
        )
        .await?;
        insert_acquisition_extension(
            &store,
            "elixir.extensions.test_source",
            "Test Source",
            &["torrent"],
        )
        .await?;

        upsert_acquisition_route(
            &database.pool,
            &store,
            TORRENT_DEFAULT_LOGICAL_ID,
            DownloadBrokerRouteUpdate {
                binding_kind: DownloadBrokerBindingKind::External,
                owner_id: None,
                provider_id: Some(external_id),
                profile_id: None,
                category: None,
                download_path: None,
                allow_shared_path: None,
                status: None,
            },
        )
        .await?;
        let inherited_routes = list_acquisition_routes(&database.pool, &store).await?;
        let inherited = inherited_routes
            .routes
            .iter()
            .find(|route| {
                route.logical_id == TORRENT_DEFAULT_LOGICAL_ID
                    && route.owner_id == "elixir.extensions.test_source"
            })
            .expect("test source route");
        assert!(inherited.inherited);
        assert_eq!(inherited.selected_provider_id, Some(external_id));

        let override_route = upsert_acquisition_route(
            &database.pool,
            &store,
            TORRENT_DEFAULT_LOGICAL_ID,
            DownloadBrokerRouteUpdate {
                binding_kind: DownloadBrokerBindingKind::ManagedProtected,
                owner_id: Some("elixir.extensions.test_source".to_string()),
                provider_id: Some(managed_id),
                profile_id: None,
                category: None,
                download_path: None,
                allow_shared_path: None,
                status: None,
            },
        )
        .await?;
        assert!(!override_route.inherited);
        assert_eq!(override_route.selected_provider_id, Some(managed_id));
        assert_eq!(
            override_route.category.as_deref(),
            Some("elixir-extensions-test-source-torrent")
        );

        let default_resolved = resolve_logical_downloader_with_bindings(
            &database.pool,
            &store,
            TORRENT_DEFAULT_LOGICAL_ID,
        )
        .await?;
        assert_eq!(default_resolved.record.provider_id, external_id);
        let extension_resolved = resolve_logical_downloader_for_owner(
            &database.pool,
            &store,
            TORRENT_DEFAULT_LOGICAL_ID,
            "elixir.extensions.test_source",
        )
        .await?;
        assert_eq!(extension_resolved.record.provider_id, managed_id);
        assert_eq!(
            extension_resolved.category.as_deref(),
            Some("elixir-extensions-test-source-torrent")
        );
        Ok(())
    }

    #[tokio::test]
    async fn extension_routes_report_category_and_path_collisions() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        let managed_id = insert_downloader_provider(
            &store,
            "elixir.modules.qbittorrent",
            "downloader.torrent",
            "qbittorrent",
            "svc-qbittorrent",
            None,
            ProviderHealthState::Healthy,
        )
        .await?;
        insert_acquisition_extension(&store, "ext.alpha", "Alpha", &["torrent"]).await?;
        insert_acquisition_extension(&store, "ext.beta", "Beta", &["torrent"]).await?;

        for owner_id in ["ext.alpha", "ext.beta"] {
            upsert_acquisition_route(
                &database.pool,
                &store,
                TORRENT_DEFAULT_LOGICAL_ID,
                DownloadBrokerRouteUpdate {
                    binding_kind: DownloadBrokerBindingKind::ManagedProtected,
                    owner_id: Some(owner_id.to_string()),
                    provider_id: Some(managed_id),
                    profile_id: None,
                    category: Some("shared".to_string()),
                    download_path: Some("/downloads/shared".to_string()),
                    allow_shared_path: None,
                    status: None,
                },
            )
            .await?;
        }

        let routes = list_acquisition_routes(&database.pool, &store).await?;
        for owner_id in ["ext.alpha", "ext.beta"] {
            let route = routes
                .routes
                .iter()
                .find(|route| {
                    route.logical_id == TORRENT_DEFAULT_LOGICAL_ID && route.owner_id == owner_id
                })
                .expect("extension route");
            assert!(
                route
                    .blocker
                    .as_deref()
                    .unwrap_or_default()
                    .contains("Shared import paths")
                    || route
                        .blocker
                        .as_deref()
                        .unwrap_or_default()
                        .contains("category")
            );
            assert!(route.checks.iter().any(|check| {
                check.code == "download_route_category_collision"
                    && check.status == DownloadBrokerRouteCheckStatus::Fail
            }));
            assert!(route.checks.iter().any(|check| {
                check.code == "download_route_path_collision"
                    && check.status == DownloadBrokerRouteCheckStatus::Fail
            }));
        }

        let err = resolve_logical_downloader_for_owner(
            &database.pool,
            &store,
            TORRENT_DEFAULT_LOGICAL_ID,
            "ext.alpha",
        )
        .await
        .expect_err("colliding route should be blocked");
        assert!(
            err.to_string().contains("category") || err.to_string().contains("Shared import paths")
        );
        Ok(())
    }

    #[tokio::test]
    async fn broker_scope_can_disable_provider_candidate() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_downloader_provider(
            &store,
            "external.stack.disabled",
            "downloader.torrent",
            "qbittorrent",
            "external-disabled-qbit",
            Some(json!({ "download_broker": { "enabled": false } })),
            ProviderHealthState::Healthy,
        )
        .await?;

        let inventory = list_logical_downloaders(&store).await?;
        assert!(inventory.downloaders.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn broker_inventory_skips_disabled_extensions_and_instances() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_downloader_provider_with_enabled(
            &store,
            "external.stack.disabled.extension",
            "downloader.torrent",
            "qbittorrent",
            "disabled-extension-qbit",
            None,
            ProviderHealthState::Healthy,
            false,
            true,
        )
        .await?;
        insert_downloader_provider_with_enabled(
            &store,
            "external.stack.disabled.instance",
            "downloader.torrent",
            "qbittorrent",
            "disabled-instance-qbit",
            None,
            ProviderHealthState::Healthy,
            true,
            false,
        )
        .await?;
        let enabled_id = insert_downloader_provider(
            &store,
            "external.stack.enabled",
            "downloader.torrent",
            "qbittorrent",
            "enabled-qbit",
            None,
            ProviderHealthState::Healthy,
        )
        .await?;

        let inventory = list_logical_downloaders(&store).await?;
        assert_eq!(inventory.downloaders.len(), 1);
        assert_eq!(inventory.downloaders[0].provider_id, enabled_id);
        Ok(())
    }
}
