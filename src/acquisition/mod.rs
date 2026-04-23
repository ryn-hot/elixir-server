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
            request_arr_json_with_query, request_arr_search_item, request_arr_write,
            resolve_arr_api_key, resolve_provider_transport_base_url, select_nzbget_provider,
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

    let mut queue_cache = HashMap::<Uuid, Value>::new();
    let mut history_cache = HashMap::<Uuid, Value>::new();
    let mut nzbget_groups_cache = None::<HashMap<String, NzbgetRecoveryGroup>>;
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
            Ok(value) => value,
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
            Ok(()) => {
                let removed_dead_job = if let Some(nzbget_provider) = nzbget_provider {
                    match remove_nzbget_download_by_download_id(
                        state,
                        &store,
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
                                "auto recovery failed to remove dead NZBGet job after manager failover: {err}"
                            );
                            false
                        }
                    }
                } else {
                    false
                };
                Ok(if removed_dead_job {
                    "Auto recovery blacklisted the dead NZBGet release in the manager, removed the dead NZBGet job, and requested another release."
                        .to_string()
                } else {
                    "Auto recovery blacklisted the dead NZBGet release in the manager and requested another release."
                        .to_string()
                })
            }
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

    if let Some(target) = find_nzbget_recovery_target(
        state,
        store,
        provider,
        &implementation,
        intent,
        select_nzbget_provider(contexts),
        &mut queue_cache,
        &mut history_cache,
        &mut nzbget_groups_cache,
        true,
    )
    .await?
    {
        request_arr_mark_history_failed(&transport.base_url, &transport.api_key, target.history_id)
            .await?;
        let removed_failed_download =
            if let Some(nzbget_provider) = select_nzbget_provider(contexts) {
                remove_nzbget_download_by_download_id(
                    state,
                    store,
                    nzbget_provider,
                    &target.download_id,
                )
                .await?
            } else {
                false
            };
        return Ok(if removed_failed_download {
            "Blacklisted the dead NZBGet release in the manager, removed the dead NZBGet job, and requested another release."
                    .to_string()
        } else {
            "Blacklisted the dead NZBGet release in the manager and requested another release."
                .to_string()
        });
    }

    if let Some(target) = find_nzbget_recovery_target(
        state,
        store,
        provider,
        &implementation,
        intent,
        select_nzbget_provider(contexts),
        &mut queue_cache,
        &mut history_cache,
        &mut nzbget_groups_cache,
        false,
    )
    .await?
    {
        request_arr_mark_history_failed(&transport.base_url, &transport.api_key, target.history_id)
            .await?;
        let removed_failed_download =
            if let Some(nzbget_provider) = select_nzbget_provider(contexts) {
                remove_nzbget_download_by_download_id(
                    state,
                    store,
                    nzbget_provider,
                    &target.download_id,
                )
                .await?
            } else {
                false
            };
        return Ok(if removed_failed_download {
            "Marked the current manager release as failed, removed the dead NZBGet job, and requested another release."
                    .to_string()
        } else {
            "Marked the current manager release as failed and requested another release."
                .to_string()
        });
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
    if let Some(entry) = queue_entry.as_ref()
        && let Some(download_id) = queue_entry_download_id(entry)
        && queue_entry_downloader_label(entry)
            .map(|value| value.to_ascii_lowercase().contains("nzbget"))
            .unwrap_or(false)
        && let Some(nzbget_provider) = select_nzbget_provider(contexts)
    {
        removed_failed_download =
            remove_nzbget_download_by_download_id(state, store, nzbget_provider, &download_id)
                .await?;
    }

    request_arr_search_item(
        &implementation,
        &transport.base_url,
        &transport.api_key,
        manager_item_id_numeric,
    )
    .await?;

    Ok(if removed_failed_download {
        "Removed the failed NZBGet job and started a fresh search for another release.".to_string()
    } else {
        "Started a fresh search for another release.".to_string()
    })
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
}
