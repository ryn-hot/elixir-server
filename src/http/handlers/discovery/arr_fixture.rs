//! Test-only: Sonarr and Radarr replies converted to Find Media's
//! general-purpose shapes (the conversion the server's own Arr code used before
//! Stage 3 removed it), so the acquisition-state tests keep checking the
//! general logic with realistic data. Nothing here is compiled into the server.
#![allow(dead_code)]
use super::manager::*;
use super::*;

#[derive(Debug, Clone, Default)]
pub(crate) struct SonarrEpisodeDescriptor {
    pub(crate) season_number: i64,
    pub(crate) episode_number: i64,
    pub(crate) title: String,
}

/// Sonarr/Radarr reads, converted to the general-purpose shapes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn derive_arr_acquisition_state(
    implementation: &str,
    manager_item_id: &str,
    item_value: &Value,
    queue_value: Option<&Value>,
    library_matched: bool,
    recovery_view: Option<&IntentRecoveryView>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
    sonarr_episode_index: Option<&HashMap<i64, SonarrEpisodeDescriptor>>,
) -> AcquisitionItemState {
    let item = legacy::item_status(implementation, item_value);
    let queue = legacy::queue(implementation, queue_value);
    let episodes = sonarr_episode_index.map(legacy::episodes);
    manager::derive_manager_acquisition_state(
        manager::ManagerAcquisitionInput {
            kind: legacy::kind(implementation),
            manager_label: legacy::label(implementation),
            item_id: manager_item_id,
            item: &item,
            queue: &queue,
            episodes: episodes.as_ref(),
        },
        library_matched,
        recovery_view,
        downloader_progress,
    )
}

pub(crate) fn extract_arr_queue_records(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    value
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn as_id_string(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        let value = value.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
        return None;
    }
    if let Some(value) = value.as_i64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_u64() {
        return Some(value.to_string());
    }
    None
}

pub(crate) fn json_nonempty_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn queue_entry_download_id(entry: &Value) -> Option<String> {
    entry
        .get("downloadId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn queue_entry_downloader_label(entry: &Value) -> Option<String> {
    entry
        .get("downloadClient")
        .or_else(|| entry.get("downloadClientName"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn queue_entry_error_message(entry: &Value) -> Option<String> {
    let error_message = entry
        .get("errorMessage")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let status = entry
        .get("status")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if status.contains("downloadclientunavailable") {
        return Some(error_message.unwrap_or_else(|| {
            "Manager could not hand this release to a download client.".to_string()
        }));
    }
    let tracked_status = entry
        .get("trackedDownloadStatus")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if tracked_status.contains("warning")
        || tracked_status.contains("error")
        || tracked_status.contains("unavailable")
    {
        return Some(
            error_message
                .unwrap_or_else(|| "Downloader reported a problem for this item.".to_string()),
        );
    }
    None
}

pub(crate) fn queue_entry_protocol(entry: &Value) -> Option<String> {
    entry
        .get("protocol")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn queue_entry_state(entry: &Value) -> String {
    let mut parts = Vec::new();
    for key in ["trackedDownloadState", "trackedDownloadStatus", "status"] {
        if let Some(value) = entry.get(key).and_then(Value::as_str) {
            let value = value.trim().to_ascii_lowercase();
            if !value.is_empty() {
                parts.push(value);
            }
        }
    }
    parts.join(" ")
}

pub(crate) fn queue_entry_status_message(entry: &Value) -> Option<String> {
    let message = entry
        .get("errorMessage")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    let tracked_status = entry
        .get("trackedDownloadStatus")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();

    tracked_status.eq("ok").then(|| message.to_string())
}

pub(crate) fn radarr_item_has_file(value: &Value) -> bool {
    value
        .get("hasFile")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || value
            .get("movieFileId")
            .and_then(Value::as_i64)
            .map(|id| id > 0)
            .unwrap_or(false)
        || value
            .get("sizeOnDisk")
            .and_then(Value::as_u64)
            .map(|size| size > 0)
            .unwrap_or(false)
}

pub(crate) fn sonarr_item_has_files(value: &Value) -> bool {
    value
        .get("statistics")
        .and_then(|statistics| statistics.get("episodeFileCount"))
        .and_then(Value::as_i64)
        .map(|count| count > 0)
        .unwrap_or(false)
        || value
            .get("statistics")
            .and_then(|statistics| statistics.get("sizeOnDisk"))
            .and_then(Value::as_u64)
            .map(|size| size > 0)
            .unwrap_or(false)
}

pub(crate) fn sonarr_series_stats(value: &Value) -> SonarrSeriesStats {
    let statistics = value.get("statistics");
    SonarrSeriesStats {
        episode_count: statistics
            .and_then(|item| item.get("episodeCount"))
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(0),
        episode_file_count: statistics
            .and_then(|item| item.get("episodeFileCount"))
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(0),
    }
}

/// Sonarr/Radarr JSON converted to the general-purpose shapes.
pub(crate) mod legacy {
    use super::*;

    pub(crate) fn kind(implementation: &str) -> ManagerItemKind {
        if implementation == "sonarr" {
            ManagerItemKind::Series
        } else {
            ManagerItemKind::Movie
        }
    }
    pub(crate) fn label(implementation: &str) -> &'static str {
        match implementation {
            "sonarr" => "Sonarr",
            "radarr" => "Radarr",
            _ => "The manager",
        }
    }
    pub(crate) fn item_status(implementation: &str, value: &Value) -> ManagerItemStatus {
        match implementation {
            "sonarr" => {
                let stats = sonarr_series_stats(value);
                ManagerItemStatus {
                    has_files: sonarr_item_has_files(value),
                    file_count: stats.episode_file_count as u64,
                    total_count: stats.episode_count as u64,
                }
            }
            "radarr" => ManagerItemStatus {
                has_files: radarr_item_has_file(value),
                ..Default::default()
            },
            _ => ManagerItemStatus::default(),
        }
    }
    pub(crate) fn queue_entry(implementation: &str, entry: &Value) -> ManagerQueueEntry {
        let direct_id = match implementation {
            "sonarr" => entry
                .get("seriesId")
                .or_else(|| entry.get("series").and_then(|value| value.get("id"))),
            "radarr" => entry
                .get("movieId")
                .or_else(|| entry.get("movie").and_then(|value| value.get("id"))),
            _ => None,
        };
        ManagerQueueEntry {
            item_id: as_id_string(direct_id.unwrap_or(&Value::Null)),
            episode_id: as_id_string(entry.get("episodeId").unwrap_or(&Value::Null)),
            entry_id: as_id_string(entry.get("id").unwrap_or(&Value::Null)),
            download_id: queue_entry_download_id(entry),
            downloader: queue_entry_downloader_label(entry),
            protocol: queue_entry_protocol(entry),
            state: queue_entry_state(entry),
            status_message: queue_entry_status_message(entry),
            error_message: queue_entry_error_message(entry),
            title: json_nonempty_string(entry.get("title")),
        }
    }
    pub(crate) fn queue(
        implementation: &str,
        queue_value: Option<&Value>,
    ) -> Vec<ManagerQueueEntry> {
        queue_value
            .map(extract_arr_queue_records)
            .unwrap_or_default()
            .iter()
            .map(|entry| queue_entry(implementation, entry))
            .collect()
    }
    /// Arr history records; entries without a numeric id are skipped.
    pub(crate) fn history(implementation: &str, value: &Value) -> Vec<ManagerHistoryEntry> {
        let records = if let Some(items) = value.as_array() {
            items.clone()
        } else {
            value
                .get("records")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        records
            .iter()
            .filter_map(|entry| {
                let history_id = entry.get("id").and_then(Value::as_i64)?.to_string();
                let item = match implementation {
                    "sonarr" => entry.get("seriesId"),
                    "radarr" => entry.get("movieId"),
                    _ => None,
                };
                Some(ManagerHistoryEntry {
                    history_id,
                    item_id: item.and_then(|value| {
                        value
                            .as_i64()
                            .map(|id| id.to_string())
                            .or_else(|| value.as_str().map(|id| id.trim().to_string()))
                    }),
                    event_type: entry
                        .get("eventType")
                        .and_then(Value::as_str)
                        .map(|value| value.trim().to_ascii_lowercase())
                        .unwrap_or_default(),
                    download_id: entry
                        .get("downloadId")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                })
            })
            .collect()
    }
    pub(crate) fn episodes(index: &HashMap<i64, SonarrEpisodeDescriptor>) -> ManagerEpisodeIndex {
        index
            .iter()
            .map(|(id, episode)| {
                (
                    id.to_string(),
                    ManagerEpisode {
                        season: episode.season_number,
                        episode: episode.episode_number,
                        title: episode.title.clone(),
                    },
                )
            })
            .collect()
    }
}
