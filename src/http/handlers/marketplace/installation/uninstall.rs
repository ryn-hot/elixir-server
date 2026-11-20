//! `POST /api/v1/marketplace/installations/uninstall`: the owner's Uninstall
//! for an extension installed from the marketplace (`elx:` IDs), including a
//! container stack and all of its apps. See `marketplace::service::uninstall`.
use super::*;
use crate::{
    db::models::ExtensionKind,
    drivers::IndexerRegistryPatch,
    extensions::{manifest::ExtensionManifest, store::ExtensionStore},
    marketplace::{
        service::{UninstallError, UninstallOptions, Uninstalled},
        stack,
    },
};
use std::collections::HashMap;

pub(super) const MAX_UNINSTALL_BYTES: usize = 1024;
/// How long the request waits for a running setup run, reconcile pass or
/// extension call before answering 409.
const WAIT: Duration = Duration::from_secs(20);

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    extension_id: String,
}

pub(super) async fn uninstall(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    headers: HeaderMap,
    bytes: Bytes,
) -> ApiResult<Json<Uninstalled>> {
    mutation(&state, &principal, &headers)?;
    authorize(&state, &principal).await?;
    let request: Request = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::bad_request("expected {\"extension_id\": \"elx:...\"}"))?;
    let proxies = proxy_cleanups(&state, &request.extension_id).await;
    let options = UninstallOptions::new(
        WAIT,
        Duration::from_secs(state.settings.extensions.apply_lock_ttl_seconds.max(1)),
    );
    let database = database(&state);
    let result = Box::pin(state.marketplace.uninstall_extension(
        &database,
        &principal,
        &request.extension_id,
        state.orchestrator.as_ref(),
        options,
    ))
    .await;
    match result {
        Ok(done) => {
            remove_proxies(proxies).await;
            Ok(Json(done))
        }
        Err(UninstallError::NotFound) => Err(ApiError::structured(
            StatusCode::NOT_FOUND,
            "marketplace_extension_not_installed",
            "This extension is not installed from the marketplace.",
            None,
        )),
        Err(UninstallError::Forbidden) => Err(ApiError::forbidden(
            "current server owner authorization is required",
        )),
        Err(UninstallError::Busy(reason)) => Err(ApiError::structured(
            StatusCode::CONFLICT,
            "marketplace_uninstall_busy",
            format!(
                "The extension cannot be uninstalled right now: {reason}. Nothing was removed; try again shortly."
            ),
            None,
        )),
        Err(UninstallError::Failed(error)) => {
            tracing::warn!(
                extension = request.extension_id.as_str(),
                "marketplace uninstall failed: {error:#}"
            );
            Err(ApiError::structured(
                StatusCode::INTERNAL_SERVER_ERROR,
                "marketplace_uninstall_failed",
                "Uninstall failed and nothing was removed. The extension still works.",
                None,
            ))
        }
    }
}

/// Prowlarr indexer proxies a stack connector registered, when the Prowlarr
/// that holds them is NOT part of the stack being removed (the cleanup the
/// generic uninstall does for a connector). A stack's own Prowlarr goes away
/// with the stack, so this is normally empty.
struct ProxyCleanup {
    target_extension: String,
    capability: String,
    names: Vec<String>,
    tags: Vec<String>,
}

async fn proxy_cleanups(state: &AppState, extension_id: &str) -> Vec<ProxyCleanup> {
    if !extension_id.starts_with("elx:") {
        return Vec::new();
    }
    let prefix = stack::member_prefix(extension_id);
    let store = ExtensionStore::new(&state.db_pool);
    let (Ok(extensions), Ok(providers), Ok(instances)) = (
        store.list_extensions().await,
        store.list_providers(None).await,
        store.list_instances(None).await,
    ) else {
        return Vec::new();
    };
    let owners: HashMap<Uuid, String> = instances
        .into_iter()
        .map(|instance| (instance.instance_id, instance.extension_id))
        .collect();
    let mut cleanups = Vec::new();
    for connector in extensions
        .iter()
        .filter(|row| row.kind == ExtensionKind::Connector && row.extension_id.starts_with(&prefix))
    {
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(connector.manifest_json.clone())
        else {
            continue;
        };
        for action in &manifest.actions {
            let (Some(target), Some(patch)) = (action.target.as_ref(), action.patch.as_ref())
            else {
                continue;
            };
            if action.r#type != "driver_patch" || target.capability != "indexer.registry" {
                continue;
            }
            let Ok(IndexerRegistryPatch::RegisterIndexerProxies { proxies }) =
                serde_json::from_value::<IndexerRegistryPatch>(patch.clone())
            else {
                continue;
            };
            // The same resolution as the generic uninstall: exactly one
            // instance provides that service slot.
            let mut holders: Vec<Uuid> = providers
                .iter()
                .filter(|provider| {
                    provider.capability == target.capability && provider.slot_id == target.slot
                })
                .map(|provider| provider.instance_id)
                .collect();
            holders.sort();
            holders.dedup();
            let [holder] = holders[..] else { continue };
            let Some(owner) = owners.get(&holder) else {
                continue;
            };
            if owner.starts_with(&prefix) {
                continue;
            }
            cleanups.push(ProxyCleanup {
                target_extension: owner.clone(),
                capability: target.capability.clone(),
                names: proxies.iter().map(|proxy| proxy.name.clone()).collect(),
                tags: proxies
                    .iter()
                    .flat_map(|proxy| proxy.tags.iter().cloned())
                    .collect(),
            });
        }
    }
    cleanups
}

/// Best effort, after the uninstall committed: the records are already gone.
async fn remove_proxies(cleanups: Vec<ProxyCleanup>) {
    for cleanup in cleanups {
        let indexers =
            match crate::http::handlers::discovery::manager::ExtensionIndexerList::for_member(
                &cleanup.target_extension,
                &cleanup.capability,
            ) {
                Ok(Some(indexers)) => indexers,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!("uninstall could not reach an indexer list: {error:#}");
                    continue;
                }
            };
        if let Err(error) = indexers
            .call::<serde_json::Value>(
                "remove_proxies",
                serde_json::json!({ "names": cleanup.names, "tags": cleanup.tags }),
            )
            .await
        {
            tracing::warn!(
                target = cleanup.target_extension.as_str(),
                "uninstall could not remove a connector's indexer proxies: {error:#}"
            );
        }
    }
}
