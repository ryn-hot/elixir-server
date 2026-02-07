use std::collections::HashSet;

use anyhow::Result;
use uuid::Uuid;

use crate::drivers::DriverPatch;
use crate::drivers::{
    DownloaderSpec, IndexerRegistryPatch, MediaManagerMoviesPatch, MediaManagerTvPatch,
};
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_runtime,
};
use crate::extensions::store::ExtensionStore;
use crate::orchestrator::planner::PlanAction;

pub async fn missing_required_secrets_for_plan(
    store: &ExtensionStore<'_>,
    actions: &[PlanAction],
) -> Result<Vec<String>> {
    let mut missing = HashSet::new();
    for action in actions {
        match action {
            PlanAction::EnsureRuntimeRunning { runtime, .. } => {
                let required = required_secrets_from_runtime(&runtime.runtime.env)?;
                if required.is_empty() {
                    continue;
                }
                let mut missing_for_instance =
                    missing_required_secrets_for_instance(store, runtime.instance_id, &required)
                        .await?;
                if is_qbittorrent_extension_id(&runtime.extension_id) {
                    missing_for_instance = filter_qbittorrent_missing(missing_for_instance);
                }
                missing.extend(missing_for_instance);
            }
            PlanAction::ApplyDriverPatch { patch } => {
                let provider = store
                    .get_provider(patch.target_provider_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("provider {} not found", patch.target_provider_id))?;
                let driver_patch = DriverPatch::from_manifest(
                    &patch.target_capability,
                    patch.patch.clone(),
                )?;
                let mut missing_for_patch =
                    missing_indexer_secrets_for_patch(store, provider.instance_id, &driver_patch)
                        .await?;
                missing_for_patch.extend(
                    missing_downloader_secrets_for_patch(store, &driver_patch).await?,
                );
                missing.extend(missing_for_patch);
            }
            _ => {}
        }
    }
    let mut missing: Vec<_> = missing.into_iter().collect();
    missing.sort();
    Ok(missing)
}

pub fn has_unresolved_conflicts(conflicts: &[serde_json::Value]) -> bool {
    conflicts.iter().any(|conflict| {
        let code = conflict.get("code").and_then(|value| value.as_str());
        match code {
            Some("missing_required_secrets") => false,
            Some("slot_conflict") => {
                let policy = conflict
                    .get("policy")
                    .and_then(|value| value.as_str())
                    .unwrap_or("prompt");
                let resolved = conflict
                    .get("resolved")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                match policy {
                    "auto_replace" => false,
                    _ => !resolved,
                }
            }
            _ => true,
        }
    })
}

async fn missing_indexer_secrets_for_patch(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    patch: &DriverPatch,
) -> Result<Vec<String>> {
    let mut missing = HashSet::new();
    let indexers: Vec<_> = match patch {
        DriverPatch::IndexerRegistry(IndexerRegistryPatch::RegisterIndexers { indexers }) => {
            indexers.iter().collect()
        }
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetIndexerRegistry { indexers }) => {
            indexers.iter().collect()
        }
        _ => Vec::new(),
    };
    for indexer in indexers {
        let fields = indexer.credential_fields()?;
        for field in fields {
            let key = indexer.credential_secret_key(field);
            let exists = store
                .get_secret(crate::db::models::SecretScope::Instance, Some(instance_id), &key)
                .await?
                .is_some();
            if !exists {
                missing.insert(format!("instance:{}:{}", instance_id, key));
            }
        }
    }
    Ok(missing.into_iter().collect())
}

async fn missing_downloader_secrets_for_patch(
    _store: &ExtensionStore<'_>,
    patch: &DriverPatch,
) -> Result<Vec<String>> {
    let downloaders: Vec<_> = match patch {
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetDownloaders { downloaders }) => {
            downloaders.iter().collect()
        }
        DriverPatch::MediaManagerMovies(MediaManagerMoviesPatch::SetDownloaders { downloaders }) => {
            downloaders.iter().collect()
        }
        _ => Vec::new(),
    };
    for downloader in downloaders {
        if !is_qbittorrent_downloader(&downloader.r#type) {
            continue;
        }
        if downloader_has_credentials(downloader) {
            continue;
        }
        // qBittorrent credentials are auto-generated on first run.
        continue;
    }
    Ok(Vec::new())
}

fn filter_qbittorrent_missing(missing: Vec<String>) -> Vec<String> {
    missing
        .into_iter()
        .filter(|value| {
            !value.ends_with(":qbittorrent_username")
                && !value.ends_with(":qbittorrent_password")
        })
        .collect()
}

fn is_qbittorrent_extension_id(extension_id: &str) -> bool {
    extension_id
        .to_ascii_lowercase()
        .contains("qbittorrent")
}

fn is_qbittorrent_downloader(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("qbittorrent")
}

fn downloader_has_credentials(downloader: &DownloaderSpec) -> bool {
    !downloader_setting_missing(&downloader.settings, "username")
        && !downloader_setting_missing(&downloader.settings, "password")
}

fn downloader_setting_missing(
    settings: &std::collections::HashMap<String, serde_json::Value>,
    key: &str,
) -> bool {
    match settings.get(key) {
        None => true,
        Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(value)) => value.trim().is_empty(),
        Some(_) => false,
    }
}
