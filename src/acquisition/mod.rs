pub mod automation;
pub mod release_resolution;
pub mod subscriptions;

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::{Context, Result as AnyResult, bail};
use chrono::{DateTime, Utc};
use reqwest::Method as ReqwestMethod;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    db::models::ProviderHealthState,
    extensions::store::{ExtensionStore, ManagedIngestIntent},
    http::handlers::{
        discovery::{
            ProviderContext, extract_arr_queue_records, load_provider_contexts,
            manager_queue_paths, queue_entry_download_id, queue_entry_downloader_label,
            queue_entry_matches_manager_item, remove_nzbget_download_by_download_id,
            remove_qbittorrent_download_by_download_id, request_arr_json_with_query,
            request_arr_search_item, request_arr_write, resolve_arr_api_key,
            resolve_provider_transport_base_url, select_nzbget_provider,
            select_qbittorrent_provider,
        },
        extensions::request_instance_service_json,
    },
    orchestrator::model::ProviderEndpoint,
    state::AppState,
};

const AUTO_RECOVERY_POLL_INTERVAL_SECONDS: u64 = 30;
const AUTO_RECOVERY_MAX_RETRIES: u32 = 2;
pub(crate) const AUTO_RECOVERY_COOLDOWN_SECONDS: i64 = 180;
const AUTO_RECOVERY_STATE_KEY: &str = "acquisition.recovery.auto_retry_state";
const NZBGET_DRONE_DOWNLOAD_ID_PARAM: &str = "drone";
const AUTO_RECOVERY_EARLY_DEAD_MIN_DOWNLOAD_TIME_SECONDS: u64 = 90;
const AUTO_RECOVERY_EARLY_DEAD_MIN_ARTICLE_ATTEMPTS: u64 = 200;
const AUTO_RECOVERY_EARLY_DEAD_MAX_DOWNLOADED_BYTES: u64 = 16 * 1024 * 1024;
const AUTO_RECOVERY_EARLY_DEAD_MAX_SUCCESS_RATIO: f64 = 0.0;
const AUTO_RECOVERY_EARLY_DEAD_LOG_WINDOW_ENTRIES: u64 = 200;
const AUTO_RECOVERY_EARLY_DEAD_LOG_CONFIRMATION_HITS: usize = 3;
const AUTO_RECOVERY_TORRENT_METADATA_TIMEOUT_SECONDS: i64 = 10 * 60;
const AUTO_RECOVERY_TORRENT_ZERO_PROGRESS_TIMEOUT_SECONDS: i64 = 15 * 60;

#[derive(Debug, Clone, Default)]
pub(crate) struct IntentRecoveryView {
    pub last_attempted_download_id: Option<String>,
    pub last_attempted_at: Option<DateTime<Utc>>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub last_attempt_succeeded: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AcquisitionRecoveryStateDocument {
    #[serde(default)]
    intents: BTreeMap<String, IntentRecoveryState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct IntentRecoveryState {
    #[serde(default)]
    manager_item_id: Option<String>,
    #[serde(default)]
    auto_retry_attempts: u32,
    #[serde(default)]
    last_attempted_download_id: Option<String>,
    #[serde(default)]
    last_attempted_at: Option<DateTime<Utc>>,
    #[serde(default)]
    cooldown_until: Option<DateTime<Utc>>,
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct ArrHistoryEntry {
    history_id: i64,
}

#[derive(Debug, Clone)]
struct ReleaseRecoveryTarget {
    history_id: i64,
    download_id: String,
    downloader: ReleaseRecoveryDownloader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseRecoveryDownloader {
    Nzbget,
    Qbittorrent,
}

#[derive(Debug, Clone, Default)]
struct NzbgetRecoveryGroup {
    nzb_id: i64,
    status: String,
    success_articles: u64,
    failed_articles: u64,
    health: Option<i64>,
    critical_health: Option<i64>,
    downloaded_size_bytes: Option<u64>,
    download_time_sec: Option<u64>,
    message_count: u64,
}

#[derive(Debug, Clone, Default)]
struct QbittorrentRecoveryTorrent {
    state: String,
    progress: Option<f64>,
    downloaded: Option<u64>,
    amount_left: Option<u64>,
    download_rate_bps: Option<u64>,
    connected_seeds: Option<u64>,
    connected_peers: Option<u64>,
    completion_on: Option<DateTime<Utc>>,
    added_on: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct NzbgetRpcGroup {
    #[serde(rename = "NZBID", default)]
    nzb_id: Option<i64>,
    #[serde(rename = "Status", default)]
    status: Option<String>,
    #[serde(rename = "SuccessArticles", default)]
    success_articles: Option<u64>,
    #[serde(rename = "FailedArticles", default)]
    failed_articles: Option<u64>,
    #[serde(rename = "Health", default)]
    health: Option<i64>,
    #[serde(rename = "CriticalHealth", default)]
    critical_health: Option<i64>,
    #[serde(rename = "DownloadedSizeLo", default)]
    downloaded_size_lo: Option<u64>,
    #[serde(rename = "DownloadedSizeHi", default)]
    downloaded_size_hi: Option<u64>,
    #[serde(rename = "DownloadedSizeMB", default)]
    downloaded_size_mb: Option<u64>,
    #[serde(rename = "DownloadTimeSec", default)]
    download_time_sec: Option<u64>,
    #[serde(rename = "MessageCount", default)]
    message_count: Option<u64>,
    #[serde(rename = "Parameters", default)]
    parameters: Vec<NzbgetRpcGroupParameter>,
}

#[derive(Debug, Deserialize)]
struct NzbgetRpcGroupParameter {
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "Value", default)]
    value: String,
}

#[derive(Debug, Deserialize)]
struct NzbgetRpcLogEntry {
    #[serde(rename = "Text", default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QbittorrentRecoveryTorrentInfo {
    #[serde(default)]
    hash: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    progress: Option<f64>,
    #[serde(default)]
    downloaded: Option<u64>,
    #[serde(default)]
    amount_left: Option<u64>,
    #[serde(default)]
    dlspeed: Option<u64>,
    #[serde(default)]
    num_seeds: Option<u64>,
    #[serde(default)]
    num_leechs: Option<u64>,
    #[serde(default)]
    completion_on: Option<i64>,
    #[serde(default)]
    added_on: Option<i64>,
}

pub async fn start_acquisition_recovery_loop(state: AppState) {
    if let Err(err) = validate_acquisition_recovery_boot(&state).await {
        warn!("acquisition recovery worker disabled at boot: {err}");
        return;
    }

    let mut interval =
        tokio::time::interval(Duration::from_secs(AUTO_RECOVERY_POLL_INTERVAL_SECONDS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        if let Err(err) = run_acquisition_recovery_iteration(&state).await {
            warn!("acquisition recovery worker disabled after failure: {err}");
            break;
        }
    }
}

pub async fn execute_find_another_release(state: &AppState, intent_id: Uuid) -> AnyResult<String> {
    let store = ExtensionStore::new(&state.db_pool);
    let intent = store
        .list_active_managed_ingest_intents()
        .await?
        .into_iter()
        .find(|intent| intent.intent_id == intent_id)
        .ok_or_else(|| anyhow::anyhow!("managed acquisition item is no longer available"))?;
    let contexts = load_provider_contexts(&store).await?;
    let message = execute_release_recovery(state, &store, &contexts, &intent).await?;
    clear_intent_recovery_state(&store, intent.intent_id).await?;
    Ok(message)
}

pub(crate) async fn load_intent_recovery_views(
    store: &ExtensionStore<'_>,
) -> AnyResult<HashMap<Uuid, IntentRecoveryView>> {
    let state = load_recovery_state(store).await?;
    let mut views = HashMap::new();
    for (intent_id, entry) in state.intents {
        let Ok(intent_id) = Uuid::parse_str(&intent_id) else {
            continue;
        };
        views.insert(
            intent_id,
            IntentRecoveryView {
                last_attempted_download_id: entry.last_attempted_download_id,
                last_attempted_at: entry.last_attempted_at,
                cooldown_until: entry.cooldown_until,
                last_attempt_succeeded: entry.last_attempted_at.is_some()
                    && entry.last_error.is_none(),
            },
        );
    }
    Ok(views)
}

async fn validate_acquisition_recovery_boot(state: &AppState) -> AnyResult<()> {
    let store = ExtensionStore::new(&state.db_pool);
    let _ = load_provider_contexts(&store).await?;
    Ok(())
}

async fn run_acquisition_recovery_iteration(state: &AppState) -> AnyResult<()> {
    let store = ExtensionStore::new(&state.db_pool);
    let intents = store.list_active_managed_ingest_intents().await?;
    if intents.is_empty() {
        return Ok(());
    }

    let contexts = load_provider_contexts(&store).await?;
    let provider_map: HashMap<Uuid, ProviderContext> = contexts
        .iter()
        .cloned()
        .map(|provider| (provider.detail.provider.provider_id, provider))
        .collect();
    let nzbget_provider = select_nzbget_provider(&contexts);
    let qbittorrent_provider = select_qbittorrent_provider(&contexts);

    let mut queue_cache = HashMap::<Uuid, Value>::new();
    let mut history_cache = HashMap::<Uuid, Value>::new();
    let mut nzbget_groups_cache = None::<HashMap<String, NzbgetRecoveryGroup>>;
    let mut qbittorrent_torrents_cache = None::<HashMap<String, QbittorrentRecoveryTorrent>>;
    let mut state_doc = load_recovery_state(&store).await?;
    let mut changed = false;
    let now = Utc::now();

    let active_ids: BTreeMap<String, String> = intents
        .iter()
        .map(|intent| {
            (
                intent.intent_id.to_string(),
                intent.manager_item_id.clone().unwrap_or_default(),
            )
        })
        .collect();

    state_doc.intents.retain(|intent_id, entry| {
        active_ids
            .get(intent_id)
            .map(|manager_item_id| {
                entry.manager_item_id.as_deref().unwrap_or_default() == manager_item_id
            })
            .unwrap_or(false)
    });

    for intent in intents {
        let intent_key = intent.intent_id.to_string();
        let entry = state_doc.intents.entry(intent_key).or_default();
        if entry.manager_item_id != intent.manager_item_id {
            *entry = IntentRecoveryState {
                manager_item_id: intent.manager_item_id.clone(),
                ..Default::default()
            };
            changed = true;
        }

        if entry.auto_retry_attempts >= AUTO_RECOVERY_MAX_RETRIES {
            continue;
        }
        if entry
            .cooldown_until
            .map(|until| until > now)
            .unwrap_or(false)
        {
            continue;
        }

        let Some(provider) = provider_map.get(&intent.manager_provider_id) else {
            continue;
        };
        if provider.detail.provider.health_state == ProviderHealthState::Unhealthy {
            continue;
        }
        let implementation = provider
            .detail
            .provider
            .implementation
            .as_deref()
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_default();
        if !matches!(implementation.as_str(), "sonarr" | "radarr") {
            continue;
        }

        let transport = match resolve_arr_transport(state, &store, provider, &implementation).await
        {
            Ok(value) => value,
            Err(err) => {
                warn!(
                    intent_id = %intent.intent_id,
                    manager_provider_id = %intent.manager_provider_id,
                    "skipping acquisition recovery transport resolution: {err}"
                );
                continue;
            }
        };

        let target = match find_nzbget_recovery_target(
            state,
            &store,
            provider,
            &implementation,
            &intent,
            nzbget_provider,
            &mut queue_cache,
            &mut history_cache,
            &mut nzbget_groups_cache,
            true,
        )
        .await
        {
            Ok(Some(value)) => Some(value),
            Ok(None) => match find_qbittorrent_recovery_target(
                state,
                &store,
                provider,
                &implementation,
                &intent,
                qbittorrent_provider,
                &mut queue_cache,
                &mut history_cache,
                &mut qbittorrent_torrents_cache,
                true,
            )
            .await
            {
                Ok(value) => value,
                Err(err) => {
                    warn!(
                        intent_id = %intent.intent_id,
                        manager_provider_id = %intent.manager_provider_id,
                        "skipping acquisition recovery target detection: {err}"
                    );
                    continue;
                }
            },
            Err(err) => {
                warn!(
                    intent_id = %intent.intent_id,
                    manager_provider_id = %intent.manager_provider_id,
                    "skipping acquisition recovery target detection: {err}"
                );
                continue;
            }
        };
        let Some(target) = target else {
            continue;
        };

        if entry.last_attempted_download_id.as_deref() == Some(target.download_id.as_str()) {
            continue;
        }

        let result = match request_arr_mark_history_failed(
            &transport.base_url,
            &transport.api_key,
            target.history_id,
        )
        .await
        {
            Ok(()) => Ok(cleanup_failed_release_download(
                state,
                &store,
                &intent,
                nzbget_provider,
                qbittorrent_provider,
                &target,
                true,
            )
            .await),
            Err(err) => Err(err),
        };

        entry.last_attempted_download_id = Some(target.download_id.clone());
        entry.last_attempted_at = Some(now);
        entry.cooldown_until =
            Some(now + chrono::Duration::seconds(AUTO_RECOVERY_COOLDOWN_SECONDS.max(1)));
        entry.auto_retry_attempts = entry.auto_retry_attempts.saturating_add(1);
        entry.last_error = result.as_ref().err().map(|err| err.to_string());
        changed = true;

        match result {
            Ok(message) => info!(
                intent_id = %intent.intent_id,
                manager_provider_id = %intent.manager_provider_id,
                manager_item_id = ?intent.manager_item_id,
                history_id = target.history_id,
                download_id = %target.download_id,
                "{message}"
            ),
            Err(err) => warn!(
                intent_id = %intent.intent_id,
                manager_provider_id = %intent.manager_provider_id,
                manager_item_id = ?intent.manager_item_id,
                history_id = target.history_id,
                download_id = %target.download_id,
                "auto recovery attempt recorded as failed: {err}"
            ),
        }
    }

    if changed {
        save_recovery_state(&store, &state_doc).await?;
    }

    Ok(())
}

async fn execute_release_recovery(
    state: &AppState,
    store: &ExtensionStore<'_>,
    contexts: &[ProviderContext],
    intent: &ManagedIngestIntent,
) -> AnyResult<String> {
    let provider_map: HashMap<Uuid, &ProviderContext> = contexts
        .iter()
        .map(|provider| (provider.detail.provider.provider_id, provider))
        .collect();
    let provider = provider_map
        .get(&intent.manager_provider_id)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("manager provider is no longer available"))?;

    let implementation = provider
        .detail
        .provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if !matches!(implementation.as_str(), "sonarr" | "radarr") {
        bail!("manager retry is not supported for implementation '{implementation}'");
    }

    let manager_item_id = intent
        .manager_item_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("manager item id is not available yet"))?;
    let manager_item_id_numeric = manager_item_id
        .parse::<i64>()
        .context("parsing manager item id")?;

    let transport = resolve_arr_transport(state, store, provider, &implementation).await?;
    let mut queue_cache = HashMap::<Uuid, Value>::new();
    let mut history_cache = HashMap::<Uuid, Value>::new();
    let mut nzbget_groups_cache = None::<HashMap<String, NzbgetRecoveryGroup>>;
    let mut qbittorrent_torrents_cache = None::<HashMap<String, QbittorrentRecoveryTorrent>>;
    let nzbget_provider = select_nzbget_provider(contexts);
    let qbittorrent_provider = select_qbittorrent_provider(contexts);

    if let Some(target) = find_nzbget_recovery_target(
        state,
        store,
        provider,
        &implementation,
        intent,
        nzbget_provider,
        &mut queue_cache,
        &mut history_cache,
        &mut nzbget_groups_cache,
        true,
    )
    .await?
    {
        request_arr_mark_history_failed(&transport.base_url, &transport.api_key, target.history_id)
            .await?;
        return Ok(cleanup_failed_release_download(
            state,
            store,
            intent,
            nzbget_provider,
            qbittorrent_provider,
            &target,
            true,
        )
        .await);
    }

    if let Some(target) = find_qbittorrent_recovery_target(
        state,
        store,
        provider,
        &implementation,
        intent,
        qbittorrent_provider,
        &mut queue_cache,
        &mut history_cache,
        &mut qbittorrent_torrents_cache,
        true,
    )
    .await?
    {
        request_arr_mark_history_failed(&transport.base_url, &transport.api_key, target.history_id)
            .await?;
        return Ok(cleanup_failed_release_download(
            state,
            store,
            intent,
            nzbget_provider,
            qbittorrent_provider,
            &target,
            true,
        )
        .await);
    }

    if let Some(target) = find_nzbget_recovery_target(
        state,
        store,
        provider,
        &implementation,
        intent,
        nzbget_provider,
        &mut queue_cache,
        &mut history_cache,
        &mut nzbget_groups_cache,
        false,
    )
    .await?
    {
        request_arr_mark_history_failed(&transport.base_url, &transport.api_key, target.history_id)
            .await?;
        return Ok(cleanup_failed_release_download(
            state,
            store,
            intent,
            nzbget_provider,
            qbittorrent_provider,
            &target,
            false,
        )
        .await);
    }

    if let Some(target) = find_qbittorrent_recovery_target(
        state,
        store,
        provider,
        &implementation,
        intent,
        qbittorrent_provider,
        &mut queue_cache,
        &mut history_cache,
        &mut qbittorrent_torrents_cache,
        false,
    )
    .await?
    {
        request_arr_mark_history_failed(&transport.base_url, &transport.api_key, target.history_id)
            .await?;
        return Ok(cleanup_failed_release_download(
            state,
            store,
            intent,
            nzbget_provider,
            qbittorrent_provider,
            &target,
            false,
        )
        .await);
    }

    let history_value = load_arr_history_page(state, store, provider, &implementation).await?;
    if let Some(history_entry) = find_latest_history_entry_for_item(
        &history_value,
        &implementation,
        manager_item_id,
        &["downloadfailed"],
    ) {
        request_arr_mark_history_failed(
            &transport.base_url,
            &transport.api_key,
            history_entry.history_id,
        )
        .await?;
        return Ok(
            "Marked the failed release in the manager and requested another release.".to_string(),
        );
    }

    let queue_value = request_arr_json_with_query(
        &transport.base_url,
        &transport.api_key,
        &manager_queue_paths(&implementation),
        &[("page", "1".to_string()), ("pageSize", "250".to_string())],
    )
    .await
    .ok();
    let queue_entry = queue_value
        .as_ref()
        .map(extract_arr_queue_records)
        .unwrap_or_default()
        .into_iter()
        .find(|entry| queue_entry_matches_manager_item(entry, &implementation, manager_item_id));

    let mut removed_failed_download = false;
    let mut removed_downloader_label = None::<&'static str>;
    if let Some(entry) = queue_entry.as_ref()
        && let Some(download_id) = queue_entry_download_id(entry)
        && queue_entry_downloader_label(entry)
            .map(|value| value.to_ascii_lowercase().contains("nzbget"))
            .unwrap_or(false)
        && let Some(nzbget_provider) = nzbget_provider
    {
        removed_failed_download =
            remove_nzbget_download_by_download_id(state, store, nzbget_provider, &download_id)
                .await?;
        removed_downloader_label = Some("NZBGet job");
    } else if let Some(entry) = queue_entry.as_ref()
        && let Some(download_id) = queue_entry_download_id(entry)
        && queue_entry_downloader_label(entry)
            .map(|value| value.to_ascii_lowercase().contains("qbittorrent"))
            .unwrap_or(false)
        && let Some(qbittorrent_provider) = qbittorrent_provider
    {
        if qbittorrent_torrents_cache.is_none() {
            qbittorrent_torrents_cache =
                Some(load_qbittorrent_torrent_index(state, store, qbittorrent_provider).await?);
        }
        if qbittorrent_torrent_index_has_local_payload(&qbittorrent_torrents_cache, &download_id) {
            return Ok(
                "qBittorrent reports local payload data for the current torrent. Elixir left it alone; import the recovered files or clear the torrent manually before searching again."
                    .to_string(),
            );
        }
        removed_failed_download = remove_qbittorrent_download_by_download_id(
            state,
            store,
            qbittorrent_provider,
            &download_id,
        )
        .await?;
        removed_downloader_label = Some("qBittorrent torrent");
    }

    request_arr_search_item(
        &implementation,
        &transport.base_url,
        &transport.api_key,
        manager_item_id_numeric,
    )
    .await?;

    Ok(if removed_failed_download {
        format!(
            "Removed the failed {} and started a fresh search for another release.",
            removed_downloader_label.unwrap_or("download")
        )
    } else {
        "Started a fresh search for another release.".to_string()
    })
}

async fn cleanup_failed_release_download(
    state: &AppState,
    store: &ExtensionStore<'_>,
    intent: &ManagedIngestIntent,
    nzbget_provider: Option<&ProviderContext>,
    qbittorrent_provider: Option<&ProviderContext>,
    target: &ReleaseRecoveryTarget,
    auto_recovery: bool,
) -> String {
    let removed_failed_download = match target.downloader {
        ReleaseRecoveryDownloader::Nzbget => {
            if let Some(nzbget_provider) = nzbget_provider {
                match remove_nzbget_download_by_download_id(
                    state,
                    store,
                    nzbget_provider,
                    &target.download_id,
                )
                .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(
                            intent_id = %intent.intent_id,
                            manager_provider_id = %intent.manager_provider_id,
                            history_id = target.history_id,
                            download_id = %target.download_id,
                            "failed to remove dead NZBGet job after manager failover: {err}"
                        );
                        false
                    }
                }
            } else {
                false
            }
        }
        ReleaseRecoveryDownloader::Qbittorrent => {
            if let Some(qbittorrent_provider) = qbittorrent_provider {
                match remove_qbittorrent_download_by_download_id(
                    state,
                    store,
                    qbittorrent_provider,
                    &target.download_id,
                )
                .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(
                            intent_id = %intent.intent_id,
                            manager_provider_id = %intent.manager_provider_id,
                            history_id = target.history_id,
                            download_id = %target.download_id,
                            "failed to remove dead qBittorrent torrent after manager failover: {err}"
                        );
                        false
                    }
                }
            } else {
                false
            }
        }
    };

    let prefix = match (auto_recovery, target.downloader) {
        (true, ReleaseRecoveryDownloader::Nzbget) => {
            "Auto recovery blacklisted the dead NZBGet release in the manager"
        }
        (true, ReleaseRecoveryDownloader::Qbittorrent) => {
            "Auto recovery blacklisted the dead torrent release in the manager"
        }
        (false, ReleaseRecoveryDownloader::Nzbget) => {
            "Marked the current manager release as failed"
        }
        (false, ReleaseRecoveryDownloader::Qbittorrent) => {
            "Marked the current torrent release as failed"
        }
    };

    let removed_phrase = match target.downloader {
        ReleaseRecoveryDownloader::Nzbget => "removed the dead NZBGet job",
        ReleaseRecoveryDownloader::Qbittorrent => "removed the dead qBittorrent torrent",
    };

    if removed_failed_download {
        format!("{prefix}, {removed_phrase}, and requested another release.")
    } else {
        format!("{prefix} and requested another release.")
    }
}

struct ArrTransport {
    base_url: String,
    api_key: String,
}

async fn resolve_arr_transport(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    implementation: &str,
) -> AnyResult<ArrTransport> {
    let endpoint_json = provider
        .detail
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
    let base_url =
        resolve_provider_transport_base_url(provider.detail.provider.instance_id, &endpoint)
            .await?;
    let api_key = resolve_arr_api_key(state, store, provider, implementation).await?;
    Ok(ArrTransport { base_url, api_key })
}

async fn find_nzbget_recovery_target(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    implementation: &str,
    intent: &ManagedIngestIntent,
    nzbget_provider: Option<&ProviderContext>,
    queue_cache: &mut HashMap<Uuid, Value>,
    history_cache: &mut HashMap<Uuid, Value>,
    nzbget_groups_cache: &mut Option<HashMap<String, NzbgetRecoveryGroup>>,
    require_dead_nzbget: bool,
) -> AnyResult<Option<ReleaseRecoveryTarget>> {
    let manager_item_id = match intent.manager_item_id.as_deref() {
        Some(value) if !value.trim().is_empty() => value.trim(),
        _ => return Ok(None),
    };

    let transport = resolve_arr_transport(state, store, provider, implementation).await?;
    let queue_value = match queue_cache.get(&intent.manager_provider_id) {
        Some(value) => value.clone(),
        None => {
            let value =
                load_arr_queue_page(&transport.base_url, &transport.api_key, implementation)
                    .await?;
            queue_cache.insert(intent.manager_provider_id, value.clone());
            value
        }
    };

    let queue_entry = extract_arr_queue_records(&queue_value)
        .into_iter()
        .find(|entry| {
            queue_entry_matches_manager_item(entry, implementation, manager_item_id)
                && queue_entry_downloader_label(entry)
                    .map(|value| value.to_ascii_lowercase().contains("nzbget"))
                    .unwrap_or(false)
                && queue_entry_download_id(entry).is_some()
        });
    let Some(queue_entry) = queue_entry else {
        return Ok(None);
    };
    let Some(download_id) = queue_entry_download_id(&queue_entry)
        .as_deref()
        .map(normalize_download_id)
    else {
        return Ok(None);
    };

    if require_dead_nzbget {
        let Some(nzbget_provider) = nzbget_provider else {
            return Ok(None);
        };
        if nzbget_groups_cache.is_none() {
            *nzbget_groups_cache =
                Some(load_nzbget_group_index(state, store, nzbget_provider).await?);
        }
        let is_dead = if let Some(group) = nzbget_groups_cache
            .as_ref()
            .and_then(|groups| groups.get(&download_id))
        {
            nzbget_group_is_auto_recovery_candidate(state, store, nzbget_provider, group).await?
        } else {
            false
        };
        if !is_dead {
            return Ok(None);
        }
    }

    let history_value = match history_cache.get(&intent.manager_provider_id) {
        Some(value) => value.clone(),
        None => {
            let value = load_arr_history_page(state, store, provider, implementation).await?;
            history_cache.insert(intent.manager_provider_id, value.clone());
            value
        }
    };
    let Some(history_entry) = find_latest_history_entry_for_download(
        &history_value,
        implementation,
        manager_item_id,
        &download_id,
        &["grabbed", "downloadfailed"],
    ) else {
        return Ok(None);
    };

    Ok(Some(ReleaseRecoveryTarget {
        history_id: history_entry.history_id,
        download_id,
        downloader: ReleaseRecoveryDownloader::Nzbget,
    }))
}

async fn find_qbittorrent_recovery_target(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    implementation: &str,
    intent: &ManagedIngestIntent,
    qbittorrent_provider: Option<&ProviderContext>,
    queue_cache: &mut HashMap<Uuid, Value>,
    history_cache: &mut HashMap<Uuid, Value>,
    qbittorrent_torrents_cache: &mut Option<HashMap<String, QbittorrentRecoveryTorrent>>,
    require_dead_qbittorrent: bool,
) -> AnyResult<Option<ReleaseRecoveryTarget>> {
    let manager_item_id = match intent.manager_item_id.as_deref() {
        Some(value) if !value.trim().is_empty() => value.trim(),
        _ => return Ok(None),
    };

    let transport = resolve_arr_transport(state, store, provider, implementation).await?;
    let queue_value = match queue_cache.get(&intent.manager_provider_id) {
        Some(value) => value.clone(),
        None => {
            let value =
                load_arr_queue_page(&transport.base_url, &transport.api_key, implementation)
                    .await?;
            queue_cache.insert(intent.manager_provider_id, value.clone());
            value
        }
    };

    let queue_entry = extract_arr_queue_records(&queue_value)
        .into_iter()
        .find(|entry| {
            queue_entry_matches_manager_item(entry, implementation, manager_item_id)
                && queue_entry_downloader_label(entry)
                    .map(|value| value.to_ascii_lowercase().contains("qbittorrent"))
                    .unwrap_or(false)
                && queue_entry_download_id(entry).is_some()
        });
    let Some(queue_entry) = queue_entry else {
        return Ok(None);
    };
    let Some(download_id) = queue_entry_download_id(&queue_entry)
        .as_deref()
        .map(normalize_download_id)
    else {
        return Ok(None);
    };

    if let Some(qbittorrent_provider) = qbittorrent_provider {
        if qbittorrent_torrents_cache.is_none() {
            *qbittorrent_torrents_cache =
                Some(load_qbittorrent_torrent_index(state, store, qbittorrent_provider).await?);
        }

        let Some(torrent) = qbittorrent_torrents_cache
            .as_ref()
            .and_then(|torrents| torrents.get(&download_id))
        else {
            if require_dead_qbittorrent {
                return Ok(None);
            }
            return Ok(None);
        };

        if qbittorrent_torrent_has_local_payload(torrent) {
            return Ok(None);
        }

        if require_dead_qbittorrent && !qbittorrent_torrent_is_auto_recovery_candidate(torrent) {
            return Ok(None);
        }
    } else {
        return Ok(None);
    }

    let history_value = match history_cache.get(&intent.manager_provider_id) {
        Some(value) => value.clone(),
        None => {
            let value = load_arr_history_page(state, store, provider, implementation).await?;
            history_cache.insert(intent.manager_provider_id, value.clone());
            value
        }
    };
    let Some(history_entry) = find_latest_history_entry_for_download(
        &history_value,
        implementation,
        manager_item_id,
        &download_id,
        &["grabbed", "downloadfailed"],
    ) else {
        return Ok(None);
    };

    Ok(Some(ReleaseRecoveryTarget {
        history_id: history_entry.history_id,
        download_id,
        downloader: ReleaseRecoveryDownloader::Qbittorrent,
    }))
}

async fn load_arr_queue_page(
    base_url: &str,
    api_key: &str,
    implementation: &str,
) -> AnyResult<Value> {
    request_arr_json_with_query(
        base_url,
        api_key,
        &manager_queue_paths(implementation),
        &[("page", "1".to_string()), ("pageSize", "250".to_string())],
    )
    .await
}

async fn load_arr_history_page(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    implementation: &str,
) -> AnyResult<Value> {
    let transport = resolve_arr_transport(state, store, provider, implementation).await?;
    request_arr_json_with_query(
        &transport.base_url,
        &transport.api_key,
        &["api/v3/history", "api/v4/history"],
        &[
            ("page", "1".to_string()),
            ("pageSize", "100".to_string()),
            ("sortDirection", "descending".to_string()),
            ("sortKey", "date".to_string()),
        ],
    )
    .await
}

async fn load_qbittorrent_torrent_index(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
) -> AnyResult<HashMap<String, QbittorrentRecoveryTorrent>> {
    let value = request_instance_service_json(
        state,
        store,
        provider.detail.provider.instance_id,
        ReqwestMethod::GET,
        "api/v2/torrents/info",
        None,
    )
    .await?;
    let torrents: Vec<QbittorrentRecoveryTorrentInfo> =
        serde_json::from_value(value).context("parsing qbittorrent recovery queue")?;

    let mut index = HashMap::new();
    for torrent in torrents {
        let hash = normalize_download_id(&torrent.hash);
        if hash.is_empty() {
            continue;
        }

        index.insert(
            hash,
            QbittorrentRecoveryTorrent {
                state: torrent
                    .state
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                progress: torrent.progress,
                downloaded: torrent.downloaded,
                amount_left: torrent.amount_left,
                download_rate_bps: torrent.dlspeed,
                connected_seeds: torrent.num_seeds,
                connected_peers: torrent.num_leechs,
                completion_on: torrent
                    .completion_on
                    .filter(|timestamp| *timestamp > 0)
                    .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0)),
                added_on: torrent
                    .added_on
                    .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0)),
            },
        );
    }

    Ok(index)
}

async fn load_nzbget_group_index(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
) -> AnyResult<HashMap<String, NzbgetRecoveryGroup>> {
    let groups: Vec<NzbgetRpcGroup> = serde_json::from_value(
        request_nzbget_rpc_result(state, store, provider, "listgroups", json!([0])).await?,
    )
    .context("parsing nzbget recovery groups")?;

    let mut index = HashMap::new();
    for group in groups {
        let Some(download_id) = group
            .parameters
            .iter()
            .find_map(|parameter| {
                parameter
                    .name
                    .trim()
                    .eq_ignore_ascii_case(NZBGET_DRONE_DOWNLOAD_ID_PARAM)
                    .then_some(parameter.value.trim())
                    .filter(|value| !value.is_empty())
            })
            .map(normalize_download_id)
        else {
            continue;
        };

        index
            .entry(download_id.clone())
            .or_insert(NzbgetRecoveryGroup {
                nzb_id: group.nzb_id.unwrap_or_default(),
                status: group
                    .status
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                success_articles: group.success_articles.unwrap_or(0),
                failed_articles: group.failed_articles.unwrap_or(0),
                health: group.health,
                critical_health: group.critical_health,
                downloaded_size_bytes: combine_size_parts(
                    group.downloaded_size_hi,
                    group.downloaded_size_lo,
                )
                .or_else(|| {
                    group
                        .downloaded_size_mb
                        .map(|value| value.saturating_mul(1024 * 1024))
                }),
                download_time_sec: group.download_time_sec,
                message_count: group.message_count.unwrap_or(0),
            });
    }

    Ok(index)
}

async fn request_nzbget_rpc_result(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    method: &str,
    params: Value,
) -> AnyResult<Value> {
    let payload = request_instance_service_json(
        state,
        store,
        provider.detail.provider.instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": method,
            "params": params,
            "id": 1
        })),
    )
    .await?;

    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        bail!("nzbget {method} returned error: {error}");
    }

    payload
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("nzbget {method} response missing result"))
}

async fn load_nzbget_group_log_entries(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    group: &NzbgetRecoveryGroup,
) -> AnyResult<Vec<NzbgetRpcLogEntry>> {
    if group.nzb_id <= 0 || group.message_count == 0 {
        return Ok(Vec::new());
    }

    let entries = AUTO_RECOVERY_EARLY_DEAD_LOG_WINDOW_ENTRIES.min(group.message_count);
    let value = request_nzbget_rpc_result(
        state,
        store,
        provider,
        "loadlog",
        json!([group.nzb_id, 0, entries]),
    )
    .await?;
    serde_json::from_value(value).context("parsing nzbget item log")
}

async fn nzbget_group_is_auto_recovery_candidate(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    group: &NzbgetRecoveryGroup,
) -> AnyResult<bool> {
    if nzbget_group_matches_hard_failure_gate(group) {
        return Ok(true);
    }
    if !nzbget_group_matches_early_dead_gate(group) {
        return Ok(false);
    }

    let log_entries = load_nzbget_group_log_entries(state, store, provider, group).await?;
    Ok(nzbget_log_confirms_dead_release(&log_entries))
}

fn nzbget_group_matches_hard_failure_gate(group: &NzbgetRecoveryGroup) -> bool {
    if group.status.contains("failure") {
        return true;
    }

    if let (Some(health), Some(critical_health)) = (group.health, group.critical_health) {
        if health <= critical_health {
            return true;
        }
    }

    false
}

fn nzbget_group_matches_early_dead_gate(group: &NzbgetRecoveryGroup) -> bool {
    if !group.status.contains("downloading") && !group.status.contains("warning") {
        return false;
    }

    let article_attempts = group.success_articles.saturating_add(group.failed_articles);
    if article_attempts < AUTO_RECOVERY_EARLY_DEAD_MIN_ARTICLE_ATTEMPTS {
        return false;
    }

    let Some(download_time_sec) = group.download_time_sec else {
        return false;
    };
    if download_time_sec < AUTO_RECOVERY_EARLY_DEAD_MIN_DOWNLOAD_TIME_SECONDS {
        return false;
    }

    let Some(success_ratio) = nzbget_group_success_ratio(group) else {
        return false;
    };
    if success_ratio > AUTO_RECOVERY_EARLY_DEAD_MAX_SUCCESS_RATIO {
        return false;
    }

    group
        .downloaded_size_bytes
        .map(|value| value <= AUTO_RECOVERY_EARLY_DEAD_MAX_DOWNLOADED_BYTES)
        .unwrap_or(true)
}

fn nzbget_group_success_ratio(group: &NzbgetRecoveryGroup) -> Option<f64> {
    let total = group.success_articles.saturating_add(group.failed_articles);
    (total > 0).then_some(group.success_articles as f64 / total as f64)
}

fn nzbget_log_confirms_dead_release(entries: &[NzbgetRpcLogEntry]) -> bool {
    let mut confirmation_hits = 0usize;

    for entry in entries {
        let Some(text) = entry.text.as_deref() else {
            continue;
        };
        if !nzbget_log_entry_confirms_dead_release(text) {
            continue;
        }
        confirmation_hits += 1;
        if confirmation_hits >= AUTO_RECOVERY_EARLY_DEAD_LOG_CONFIRMATION_HITS {
            return true;
        }
    }

    false
}

fn nzbget_log_entry_confirms_dead_release(text: &str) -> bool {
    let normalized = text.trim().to_ascii_lowercase();
    if normalized.contains("430 no such article")
        || normalized.contains("article or group not found")
        || normalized.contains("all servers failed")
    {
        return true;
    }

    nzbget_log_entry_reports_full_file_failure(&normalized)
}

fn nzbget_log_entry_reports_full_file_failure(text: &str) -> bool {
    let Some(prefix) = text.split(" article downloads failed").next() else {
        return false;
    };
    let Some((failed, total)) = prefix.split_once(" of ") else {
        return false;
    };
    let Ok(failed) = failed.trim().parse::<u64>() else {
        return false;
    };
    let Ok(total) = total.trim().parse::<u64>() else {
        return false;
    };

    total > 0 && failed == total
}

fn qbittorrent_torrent_is_auto_recovery_candidate(torrent: &QbittorrentRecoveryTorrent) -> bool {
    if qbittorrent_torrent_has_local_payload(torrent) {
        return false;
    }

    if matches!(torrent.state.as_str(), "error" | "missingfiles") {
        return true;
    }

    let Some(added_on) = torrent.added_on else {
        return false;
    };
    let age_seconds = (Utc::now() - added_on).num_seconds();
    let connected_seeds = torrent.connected_seeds.unwrap_or(0);
    let connected_peers = torrent.connected_peers.unwrap_or(0);
    let no_connections = connected_seeds == 0 && connected_peers == 0;
    let download_rate = torrent.download_rate_bps.unwrap_or(0);

    if matches!(torrent.state.as_str(), "metadl" | "forcedmetadl")
        && age_seconds >= AUTO_RECOVERY_TORRENT_METADATA_TIMEOUT_SECONDS
        && no_connections
        && download_rate == 0
    {
        return true;
    }

    matches!(
        torrent.state.as_str(),
        "downloading" | "stalleddl" | "forceddl"
    ) && age_seconds >= AUTO_RECOVERY_TORRENT_ZERO_PROGRESS_TIMEOUT_SECONDS
        && no_connections
        && download_rate == 0
}

fn qbittorrent_torrent_index_has_local_payload(
    torrents: &Option<HashMap<String, QbittorrentRecoveryTorrent>>,
    download_id: &str,
) -> bool {
    let download_id = normalize_download_id(download_id);
    torrents
        .as_ref()
        .and_then(|torrents| torrents.get(&download_id))
        .map(qbittorrent_torrent_has_local_payload)
        .unwrap_or(false)
}

fn qbittorrent_torrent_has_local_payload(torrent: &QbittorrentRecoveryTorrent) -> bool {
    torrent.progress.unwrap_or(0.0) > 0.0
        || torrent.downloaded.unwrap_or(0) > 0
        || torrent.amount_left == Some(0)
        || torrent.completion_on.is_some()
}

fn normalize_download_id(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn combine_size_parts(hi: Option<u64>, lo: Option<u64>) -> Option<u64> {
    match (hi, lo) {
        (Some(hi), Some(lo)) => Some((hi << 32) | lo),
        (Some(hi), None) => Some(hi << 32),
        (None, Some(lo)) => Some(lo),
        (None, None) => None,
    }
}

fn extract_arr_history_records(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    value
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn find_latest_history_entry_for_download(
    history_value: &Value,
    implementation: &str,
    manager_item_id: &str,
    download_id: &str,
    accepted_events: &[&str],
) -> Option<ArrHistoryEntry> {
    let normalized_download_id = normalize_download_id(download_id);
    extract_arr_history_records(history_value)
        .into_iter()
        .find_map(|entry| {
            let item_id = match implementation {
                "sonarr" => entry.get("seriesId"),
                "radarr" => entry.get("movieId"),
                _ => None,
            }?;
            let matches_item = item_id
                .as_i64()
                .map(|value| value.to_string() == manager_item_id)
                .or_else(|| {
                    item_id
                        .as_str()
                        .map(|value| value.trim() == manager_item_id)
                })
                .unwrap_or(false);
            if !matches_item {
                return None;
            }

            let event_type = entry
                .get("eventType")
                .and_then(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase())
                .unwrap_or_default();
            if !accepted_events
                .iter()
                .any(|candidate| event_type == candidate.to_ascii_lowercase())
            {
                return None;
            }

            let entry_download_id = entry
                .get("downloadId")
                .and_then(Value::as_str)
                .map(normalize_download_id);
            if entry_download_id.as_deref() != Some(normalized_download_id.as_str()) {
                return None;
            }

            Some(ArrHistoryEntry {
                history_id: entry.get("id").and_then(Value::as_i64)?,
            })
        })
}

fn find_latest_history_entry_for_item(
    history_value: &Value,
    implementation: &str,
    manager_item_id: &str,
    accepted_events: &[&str],
) -> Option<ArrHistoryEntry> {
    extract_arr_history_records(history_value)
        .into_iter()
        .find_map(|entry| {
            let item_id = match implementation {
                "sonarr" => entry.get("seriesId"),
                "radarr" => entry.get("movieId"),
                _ => None,
            }?;
            let matches_item = item_id
                .as_i64()
                .map(|value| value.to_string() == manager_item_id)
                .or_else(|| {
                    item_id
                        .as_str()
                        .map(|value| value.trim() == manager_item_id)
                })
                .unwrap_or(false);
            if !matches_item {
                return None;
            }

            let event_type = entry
                .get("eventType")
                .and_then(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase())
                .unwrap_or_default();
            if !accepted_events
                .iter()
                .any(|candidate| event_type == candidate.to_ascii_lowercase())
            {
                return None;
            }

            Some(ArrHistoryEntry {
                history_id: entry.get("id").and_then(Value::as_i64)?,
            })
        })
}

async fn request_arr_mark_history_failed(
    base_url: &str,
    api_key: &str,
    history_id: i64,
) -> AnyResult<()> {
    let history_id = history_id.to_string();
    let v3 = format!("api/v3/history/failed/{history_id}");
    let v4 = format!("api/v4/history/failed/{history_id}");
    request_arr_write(
        base_url,
        api_key,
        &[v3.as_str(), v4.as_str()],
        ReqwestMethod::POST,
        &json!({}),
    )
    .await
}

async fn load_recovery_state(
    store: &ExtensionStore<'_>,
) -> AnyResult<AcquisitionRecoveryStateDocument> {
    let Some(value) = store.get_extension_setting(AUTO_RECOVERY_STATE_KEY).await? else {
        return Ok(AcquisitionRecoveryStateDocument::default());
    };
    serde_json::from_value(value).context("parsing acquisition recovery state")
}

async fn save_recovery_state(
    store: &ExtensionStore<'_>,
    state: &AcquisitionRecoveryStateDocument,
) -> AnyResult<()> {
    store
        .upsert_extension_setting(
            AUTO_RECOVERY_STATE_KEY,
            &serde_json::to_value(state).context("serializing acquisition recovery state")?,
        )
        .await?;
    Ok(())
}

async fn clear_intent_recovery_state(store: &ExtensionStore<'_>, intent_id: Uuid) -> AnyResult<()> {
    let mut state = load_recovery_state(store).await?;
    if state.intents.remove(&intent_id.to_string()).is_some() {
        save_recovery_state(store, &state).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn find_latest_history_entry_for_download_matches_grabbed_release_by_download_id() {
        let history = json!({
            "records": [
                {"id": 21, "eventType": "grabbed", "movieId": 6, "downloadId": "aaa"},
                {"id": 22, "eventType": "downloadFailed", "movieId": 6, "downloadId": "bbb"},
                {"id": 23, "eventType": "grabbed", "movieId": 6, "downloadId": "bbb"},
                {"id": 24, "eventType": "grabbed", "movieId": 7, "downloadId": "bbb"}
            ]
        });

        let entry = find_latest_history_entry_for_download(
            &history,
            "radarr",
            "6",
            "bbb",
            &["grabbed", "downloadfailed"],
        )
        .expect("matching history entry");

        assert_eq!(entry.history_id, 22);
    }

    #[test]
    fn nzbget_group_matches_hard_failure_gate_requires_failure_or_critical_health() {
        let warning_only = NzbgetRecoveryGroup {
            status: "warning".to_string(),
            health: Some(2000),
            critical_health: Some(1000),
            ..Default::default()
        };
        assert!(!nzbget_group_matches_hard_failure_gate(&warning_only));

        let failed = NzbgetRecoveryGroup {
            status: "failure".to_string(),
            ..Default::default()
        };
        assert!(nzbget_group_matches_hard_failure_gate(&failed));

        let unrecoverable = NzbgetRecoveryGroup {
            status: "downloading".to_string(),
            health: Some(800),
            critical_health: Some(1000),
            ..Default::default()
        };
        assert!(nzbget_group_matches_hard_failure_gate(&unrecoverable));
    }

    #[test]
    fn nzbget_group_matches_early_dead_gate_for_zero_success_failure_heavy_sample() {
        let group = NzbgetRecoveryGroup {
            status: "downloading".to_string(),
            success_articles: 0,
            failed_articles: 400,
            downloaded_size_bytes: Some(64 * 1024),
            download_time_sec: Some(120),
            ..Default::default()
        };

        assert_eq!(nzbget_group_success_ratio(&group), Some(0.0));
        assert!(nzbget_group_matches_early_dead_gate(&group));
    }

    #[test]
    fn nzbget_group_matches_early_dead_gate_rejects_groups_with_any_success() {
        let group = NzbgetRecoveryGroup {
            status: "downloading".to_string(),
            success_articles: 1,
            failed_articles: 400,
            downloaded_size_bytes: Some(64 * 1024),
            download_time_sec: Some(120),
            ..Default::default()
        };

        assert!(nzbget_group_success_ratio(&group).unwrap_or_default() > 0.0);
        assert!(!nzbget_group_matches_early_dead_gate(&group));
    }

    #[test]
    fn nzbget_group_matches_early_dead_gate_rejects_samples_with_real_progress() {
        let group = NzbgetRecoveryGroup {
            status: "downloading".to_string(),
            success_articles: 0,
            failed_articles: 400,
            downloaded_size_bytes: Some(AUTO_RECOVERY_EARLY_DEAD_MAX_DOWNLOADED_BYTES + 1),
            download_time_sec: Some(120),
            ..Default::default()
        };

        assert!(!nzbget_group_matches_early_dead_gate(&group));
    }

    #[test]
    fn nzbget_log_confirms_dead_release_from_repeated_full_file_failures() {
        let entries = vec![
            NzbgetRpcLogEntry {
                text: Some("137 of 137 article downloads failed for \"one\"".to_string()),
            },
            NzbgetRpcLogEntry {
                text: Some("137 of 137 article downloads failed for \"two\"".to_string()),
            },
            NzbgetRpcLogEntry {
                text: Some("137 of 137 article downloads failed for \"three\"".to_string()),
            },
        ];

        assert!(nzbget_log_confirms_dead_release(&entries));
    }

    #[test]
    fn nzbget_log_confirms_dead_release_rejects_partial_file_failures() {
        let entries = vec![
            NzbgetRpcLogEntry {
                text: Some("1 of 137 article downloads failed for \"one\"".to_string()),
            },
            NzbgetRpcLogEntry {
                text: Some("2 of 137 article downloads failed for \"two\"".to_string()),
            },
            NzbgetRpcLogEntry {
                text: Some("3 of 137 article downloads failed for \"three\"".to_string()),
            },
        ];

        assert!(!nzbget_log_confirms_dead_release(&entries));
    }

    #[test]
    fn qbittorrent_torrent_is_auto_recovery_candidate_for_metadata_stall() {
        let torrent = QbittorrentRecoveryTorrent {
            state: "metadl".to_string(),
            download_rate_bps: Some(0),
            connected_seeds: Some(0),
            connected_peers: Some(0),
            added_on: Some(
                Utc::now()
                    - chrono::Duration::seconds(AUTO_RECOVERY_TORRENT_METADATA_TIMEOUT_SECONDS),
            ),
            ..Default::default()
        };

        assert!(qbittorrent_torrent_is_auto_recovery_candidate(&torrent));
    }

    #[test]
    fn qbittorrent_torrent_is_auto_recovery_candidate_rejects_slow_but_connected_swarm() {
        let torrent = QbittorrentRecoveryTorrent {
            state: "metadl".to_string(),
            download_rate_bps: Some(0),
            connected_seeds: Some(0),
            connected_peers: Some(1),
            added_on: Some(
                Utc::now()
                    - chrono::Duration::seconds(AUTO_RECOVERY_TORRENT_METADATA_TIMEOUT_SECONDS),
            ),
            ..Default::default()
        };

        assert!(!qbittorrent_torrent_is_auto_recovery_candidate(&torrent));
    }

    #[test]
    fn qbittorrent_torrent_is_auto_recovery_candidate_for_hard_failure() {
        let torrent = QbittorrentRecoveryTorrent {
            state: "error".to_string(),
            ..Default::default()
        };

        assert!(qbittorrent_torrent_is_auto_recovery_candidate(&torrent));
    }

    #[test]
    fn qbittorrent_torrent_is_auto_recovery_candidate_rejects_hard_failure_with_payload() {
        let torrent = QbittorrentRecoveryTorrent {
            state: "missingfiles".to_string(),
            downloaded: Some(1),
            ..Default::default()
        };

        assert!(!qbittorrent_torrent_is_auto_recovery_candidate(&torrent));
    }

    #[test]
    fn qbittorrent_torrent_is_auto_recovery_candidate_rejects_completed_payload() {
        let torrent = QbittorrentRecoveryTorrent {
            state: "error".to_string(),
            progress: Some(1.0),
            amount_left: Some(0),
            completion_on: Some(Utc::now()),
            ..Default::default()
        };

        assert!(!qbittorrent_torrent_is_auto_recovery_candidate(&torrent));
    }
}
