//! General-purpose media-manager status for Find Media. A request's
//! acquisition state is derived from these shapes whichever manager answers:
//! an installed extension returns them directly; until Stage 3, the server's
//! legacy Sonarr/Radarr reads are converted by [`legacy`]. See
//! docs/architecture/arr-stack-marketplace-extension.md.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManagerItemKind {
    Series,
    Movie,
}

/// One managed item. For a series, `file_count` of `total_count` episodes
/// have files; a movie reports only `has_files`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagerItemStatus {
    #[serde(default)]
    pub has_files: bool,
    #[serde(default)]
    pub file_count: u64,
    #[serde(default)]
    pub total_count: u64,
}

/// One entry of the manager's download queue.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagerQueueEntry {
    #[serde(default)]
    pub item_id: Option<String>,
    #[serde(default)]
    pub episode_id: Option<String>,
    #[serde(default)]
    pub entry_id: Option<String>,
    #[serde(default)]
    pub download_id: Option<String>,
    #[serde(default)]
    pub downloader: Option<String>,
    #[serde(default)]
    pub protocol: Option<String>,
    /// Lowercase words describing the entry. Words containing `import`,
    /// `post`, `extract`, `verif` or `download` set the phase.
    #[serde(default)]
    pub state: String,
    /// An informational message while the entry is healthy.
    #[serde(default)]
    pub status_message: Option<String>,
    /// Set when the manager reports a problem with this entry.
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagerEpisode {
    pub season: i64,
    pub episode: i64,
    #[serde(default)]
    pub title: String,
}
/// Episodes by the manager's episode ID.
pub(crate) type ManagerEpisodeIndex = HashMap<String, ManagerEpisode>;

/// One entry of the manager's recent history (grabs, failures, imports).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagerHistoryEntry {
    pub history_id: String,
    #[serde(default)]
    pub item_id: Option<String>,
    /// Lowercase, e.g. `grabbed`, `downloadfailed`.
    #[serde(default)]
    pub event_type: String,
    #[serde(default)]
    pub download_id: Option<String>,
}

/// A media manager Find Media and release recovery talk to: an installed
/// stack's module, or (until Stage 3) the server's own Sonarr/Radarr code.
pub(crate) enum ManagerClient {
    Legacy {
        implementation: String,
        base_url: String,
        api_key: String,
    },
    #[cfg(unix)]
    Extension(ExtensionManager),
}

impl ManagerClient {
    pub(crate) async fn for_provider(
        state: &AppState,
        store: &ExtensionStore<'_>,
        provider: &ProviderContext,
    ) -> AnyResult<Self> {
        #[cfg(unix)]
        if let Some(manager) = ExtensionManager::for_provider(state, provider).await? {
            return Ok(Self::Extension(manager));
        }
        let implementation = provider
            .detail
            .provider
            .implementation
            .as_deref()
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_default();
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
        let api_key = resolve_arr_api_key(state, store, provider, &implementation).await?;
        Ok(Self::Legacy {
            implementation,
            base_url,
            api_key,
        })
    }

    /// The first queue page, every item's entries.
    pub(crate) async fn queue(&self) -> AnyResult<Vec<ManagerQueueEntry>> {
        match self {
            Self::Legacy {
                implementation,
                base_url,
                api_key,
            } => {
                let value = request_arr_json_with_query(
                    base_url,
                    api_key,
                    &manager_queue_paths(implementation),
                    &[("page", "1".to_string()), ("pageSize", "250".to_string())],
                )
                .await?;
                Ok(legacy::queue(implementation, Some(&value)))
            }
            #[cfg(unix)]
            Self::Extension(manager) => {
                #[derive(Deserialize)]
                struct Reply {
                    entries: Vec<ManagerQueueEntry>,
                }
                Ok(manager.call::<Reply>("queue", json!({})).await?.entries)
            }
        }
    }

    /// The newest 100 history entries.
    pub(crate) async fn history(&self) -> AnyResult<Vec<ManagerHistoryEntry>> {
        match self {
            Self::Legacy {
                implementation,
                base_url,
                api_key,
            } => {
                let value = request_arr_json_with_query(
                    base_url,
                    api_key,
                    &["api/v3/history", "api/v4/history"],
                    &[
                        ("page", "1".to_string()),
                        ("pageSize", "100".to_string()),
                        ("sortDirection", "descending".to_string()),
                        ("sortKey", "date".to_string()),
                    ],
                )
                .await?;
                Ok(legacy::history(implementation, &value))
            }
            #[cfg(unix)]
            Self::Extension(manager) => {
                #[derive(Deserialize)]
                struct Reply {
                    entries: Vec<ManagerHistoryEntry>,
                }
                Ok(manager.call::<Reply>("history", json!({})).await?.entries)
            }
        }
    }

    /// Marks a grabbed release failed; the manager blocklists it and searches.
    pub(crate) async fn mark_failed(&self, history_id: &str) -> AnyResult<()> {
        match self {
            Self::Legacy {
                base_url, api_key, ..
            } => {
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
            #[cfg(unix)]
            Self::Extension(manager) => {
                manager
                    .call::<Value>("mark_failed", json!({ "historyId": history_id }))
                    .await?;
                Ok(())
            }
        }
    }

    /// Starts a fresh search for one item.
    pub(crate) async fn search_item(&self, item_id: &str) -> AnyResult<()> {
        match self {
            Self::Legacy {
                implementation,
                base_url,
                api_key,
            } => {
                let item_id = item_id.parse::<i64>().context("parsing manager item id")?;
                request_arr_search_item(implementation, base_url, api_key, item_id).await
            }
            #[cfg(unix)]
            Self::Extension(manager) => {
                manager
                    .call::<Value>("search_item", json!({ "itemId": item_id }))
                    .await?;
                Ok(())
            }
        }
    }
}

/// What Find Media needs to know about one managed item and its downloads.
pub(crate) struct ManagerAcquisitionInput<'a> {
    pub kind: ManagerItemKind,
    /// Shown in text, e.g. "Sonarr is handling this series …".
    pub manager_label: &'a str,
    pub item_id: &'a str,
    pub item: &'a ManagerItemStatus,
    /// The whole queue; entries for other items are ignored.
    pub queue: &'a [ManagerQueueEntry],
    pub episodes: Option<&'a ManagerEpisodeIndex>,
}

pub(super) fn derive_manager_acquisition_state(
    input: ManagerAcquisitionInput<'_>,
    library_matched: bool,
    recovery_view: Option<&IntentRecoveryView>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
) -> AcquisitionItemState {
    let queue_entries: Vec<&ManagerQueueEntry> = input
        .queue
        .iter()
        .filter(|entry| entry.item_id.as_deref() == Some(input.item_id))
        .collect();
    let queue_entry_count = queue_entries.len();
    let has_file = input.item.has_files;

    if input.kind == ManagerItemKind::Series && !queue_entries.is_empty() {
        return derive_series_batch_state(
            input.manager_label,
            input.item,
            &queue_entries,
            input.episodes,
            downloader_progress,
        );
    }

    if let Some(entry) = queue_entries.first() {
        let mut state = derive_queue_entry_state(entry, queue_entry_count, downloader_progress);
        state.children = vec![build_download_attempt_child(
            entry,
            input.episodes,
            downloader_progress,
        )];
        return state;
    }

    if acquisition_is_completed(input.kind, input.item, library_matched) {
        return completed_acquisition_state();
    }

    if has_file && library_matched {
        let mut evidence = base_acquisition_evidence(true, false, true, queue_entry_count, None, None);
        evidence.push(acquisition_evidence(
            "Elixir linked",
            "Yes",
            Some("success"),
        ));
        return AcquisitionItemState {
            phase: AcquisitionPhase::Importing,
            headline: "Imported files linked in Elixir.".to_string(),
            detail: Some(
                "The manager has imported files and Elixir linked them to the managed request. Waiting for remaining monitored media."
                    .to_string(),
            ),
            blocker: None,
            evidence,
            actions: Vec::new(),
            progress_percent: Some(100.0),
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
            children: Vec::new(),
        };
    }

    if has_file {
        let mut evidence = base_acquisition_evidence(true, false, true, queue_entry_count, None, None);
        evidence.push(acquisition_evidence("Elixir linked", "No", Some("warning")));
        return AcquisitionItemState {
            phase: AcquisitionPhase::Importing,
            headline: "Imported in manager, not linked in Elixir.".to_string(),
            detail: Some(
                "The manager reports imported files, but Elixir has not linked the import to the managed request yet.".to_string(),
            ),
            blocker: None,
            evidence,
            actions: Vec::new(),
            progress_percent: Some(100.0),
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
            children: Vec::new(),
        };
    }

    if acquisition_recovery_is_transitioning(recovery_view) {
        return AcquisitionItemState {
            phase: AcquisitionPhase::FindingAnotherRelease,
            headline: "Finding another release.".to_string(),
            detail: Some(
                "Elixir cleared the dead release and is waiting for the manager to provide another one."
                    .to_string(),
            ),
            blocker: None,
            evidence: base_acquisition_evidence(true, false, false, 0, None, None),
            actions: Vec::new(),
            progress_percent: None,
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
            children: Vec::new(),
        };
    }

    AcquisitionItemState {
        phase: AcquisitionPhase::AcceptedByManager,
        headline: "Accepted by manager.".to_string(),
        detail: Some(
            "The manager accepted the item and is still looking for a valid release. No downloader has accepted it yet."
                .to_string(),
        ),
        blocker: None,
        evidence: base_acquisition_evidence(true, false, false, 0, None, None),
        actions: Vec::new(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label: None,
        protocol: None,
        children: Vec::new(),
    }
}

fn acquisition_is_completed(
    kind: ManagerItemKind,
    item: &ManagerItemStatus,
    library_matched: bool,
) -> bool {
    if !library_matched {
        return false;
    }
    match kind {
        ManagerItemKind::Series => item.total_count > 0 && item.file_count >= item.total_count,
        ManagerItemKind::Movie => true,
    }
}

fn derive_queue_entry_state(
    entry: &ManagerQueueEntry,
    queue_entry_count: usize,
    downloader_progress_index: &AcquisitionDownloaderProgressIndex,
) -> AcquisitionItemState {
    let downloader_progress = entry
        .download_id
        .as_deref()
        .and_then(|download_id| downloader_progress_index.get(download_id));
    let progress_percent = downloader_progress.and_then(|item| item.progress_percent);
    let eta_seconds = downloader_progress.and_then(|item| item.eta_seconds);
    let downloader_label = entry.downloader.clone();
    let protocol = entry.protocol.clone();
    let status_message = entry.status_message.clone();

    if let Some(message) = entry.error_message.clone() {
        return acquisition_attention(
            "manager_queue_error",
            "Manager reported a problem",
            message,
            base_acquisition_evidence(
                true,
                false,
                false,
                queue_entry_count,
                downloader_label.as_deref(),
                protocol.as_deref(),
            ),
        );
    }

    if let Some(issue) = downloader_progress.and_then(|item| item.issue.clone()) {
        if issue.code.starts_with("nzbget_release_")
            || issue.code.starts_with("qbittorrent_release_")
        {
            let protected_payload = issue.code == "qbittorrent_release_failed_with_payload";
            let headline = if protected_payload {
                "Torrent needs manual recovery."
            } else {
                "Dead release detected."
            };
            let title = if protected_payload {
                issue.title.clone()
            } else {
                "Dead release detected".to_string()
            };
            return AcquisitionItemState {
                phase: AcquisitionPhase::NeedsAttention,
                headline: headline.to_string(),
                detail: Some(issue.detail.clone()),
                blocker: Some(FindMediaAcquisitionBlocker {
                    code: issue.code,
                    title,
                    detail: issue.detail,
                    severity: "warning".to_string(),
                }),
                evidence: base_acquisition_evidence(
                    true,
                    true,
                    false,
                    queue_entry_count,
                    downloader_label.as_deref(),
                    protocol.as_deref(),
                ),
                actions: Vec::new(),
                progress_percent: None,
                eta_seconds: None,
                downloader_label,
                protocol,
                children: Vec::new(),
            };
        }

        return AcquisitionItemState {
            phase: AcquisitionPhase::NeedsAttention,
            headline: issue.title.clone(),
            detail: Some(issue.detail.clone()),
            blocker: Some(FindMediaAcquisitionBlocker {
                code: issue.code,
                title: issue.title,
                detail: issue.detail,
                severity: "warning".to_string(),
            }),
            evidence: base_acquisition_evidence(
                true,
                true,
                false,
                queue_entry_count,
                downloader_label.as_deref(),
                protocol.as_deref(),
            ),
            actions: Vec::new(),
            progress_percent,
            eta_seconds,
            downloader_label,
            protocol,
            children: Vec::new(),
        };
    }

    let tracked_state = entry.state.as_str();
    let phase = if tracked_state.contains("import") {
        AcquisitionPhase::Importing
    } else if tracked_state.contains("post")
        || tracked_state.contains("extract")
        || tracked_state.contains("verif")
        || progress_percent.map(|value| value >= 99.5).unwrap_or(false)
    {
        AcquisitionPhase::PostProcessing
    } else if tracked_state.contains("download")
        || progress_percent.map(|value| value > 0.0).unwrap_or(false)
    {
        AcquisitionPhase::Downloading
    } else {
        AcquisitionPhase::QueuedInDownloader
    };

    let (headline, detail) = match phase {
        AcquisitionPhase::Downloading => {
            if let Some(label) = downloader_label.as_deref() {
                (
                    format!("Downloading via {label}."),
                    Some(status_message.clone().unwrap_or_else(|| {
                        "Transfer is active in the downloader.".to_string()
                    })),
                )
            } else {
                (
                    "Download in progress.".to_string(),
                    Some(status_message.clone().unwrap_or_else(|| {
                        "Transfer is active in the downloader.".to_string()
                    })),
                )
            }
        }
        AcquisitionPhase::PostProcessing => (
            "Download finished.".to_string(),
            Some("Waiting for verification, extraction, or downloader cleanup.".to_string()),
        ),
        AcquisitionPhase::Importing => (
            "Manager is importing the completed download.".to_string(),
            Some("The downloader finished and the manager is finishing the import.".to_string()),
        ),
        _ => {
            if let Some(label) = downloader_label.as_deref() {
                (
                    format!("Queued with {label}."),
                    Some(status_message.clone().unwrap_or_else(|| {
                        "Manager handed the item to the downloader. Waiting for transfer to start."
                            .to_string()
                    })),
                )
            } else {
                (
                    "Waiting in the download queue.".to_string(),
                    Some(status_message.unwrap_or_else(|| {
                        "Manager handed the item to the downloader. Waiting for transfer to start."
                            .to_string()
                    })),
                )
            }
        }
    };

    AcquisitionItemState {
        phase,
        headline,
        detail,
        blocker: None,
        evidence: base_acquisition_evidence(
            true,
            true,
            false,
            queue_entry_count,
            downloader_label.as_deref(),
            protocol.as_deref(),
        ),
        actions: Vec::new(),
        progress_percent,
        eta_seconds,
        downloader_label,
        protocol,
        children: Vec::new(),
    }
}

fn derive_series_batch_state(
    manager_label: &str,
    item: &ManagerItemStatus,
    queue_entries: &[&ManagerQueueEntry],
    episodes: Option<&ManagerEpisodeIndex>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
) -> AcquisitionItemState {
    let mut children = queue_entries
        .iter()
        .map(|entry| build_download_attempt_child(entry, episodes, downloader_progress))
        .collect::<Vec<_>>();
    let counts = summarize_sonarr_batch_children(&children);
    let stats = SonarrSeriesStats {
        episode_count: item.total_count as usize,
        episode_file_count: item.file_count as usize,
    };
    let downloader_label = queue_entries.first().and_then(|entry| entry.downloader.clone());
    let protocol = queue_entries.first().and_then(|entry| entry.protocol.clone());
    let phase = summarize_sonarr_batch_phase(counts);

    let mut evidence = base_acquisition_evidence(
        true,
        true,
        false,
        queue_entries.len(),
        downloader_label.as_deref(),
        protocol.as_deref(),
    );
    evidence.push(acquisition_evidence(
        "Transfers queued",
        counts.queued.to_string(),
        Some("neutral"),
    ));
    if counts.downloading > 0 {
        evidence.push(acquisition_evidence(
            "Transfers downloading",
            counts.downloading.to_string(),
            Some("success"),
        ));
    }
    if counts.post_processing > 0 {
        evidence.push(acquisition_evidence(
            "Transfers post-processing",
            counts.post_processing.to_string(),
            Some("neutral"),
        ));
    }
    if counts.importing > 0 {
        evidence.push(acquisition_evidence(
            "Transfers importing",
            counts.importing.to_string(),
            Some("neutral"),
        ));
    }
    if counts.needs_attention + counts.failed > 0 {
        evidence.push(acquisition_evidence(
            "Transfers needing attention",
            (counts.needs_attention + counts.failed).to_string(),
            Some("warning"),
        ));
    }
    if stats.episode_count > 0 {
        evidence.push(acquisition_evidence(
            "Files imported",
            format!("{} / {}", stats.episode_file_count, stats.episode_count),
            Some(if stats.episode_file_count > 0 {
                "success"
            } else {
                "neutral"
            }),
        ));
    } else if stats.episode_file_count > 0 {
        evidence.push(acquisition_evidence(
            "Files imported",
            stats.episode_file_count.to_string(),
            Some("success"),
        ));
    }

    children.sort_by(|left, right| {
        let left_phase = acquisition_phase_from_str(&left.phase);
        let right_phase = acquisition_phase_from_str(&right.phase);
        left_phase
            .sort_priority()
            .cmp(&right_phase.sort_priority())
            .then_with(|| {
                left.title
                    .to_ascii_lowercase()
                    .cmp(&right.title.to_ascii_lowercase())
            })
    });

    AcquisitionItemState {
        phase,
        headline: format_sonarr_batch_headline(counts),
        detail: Some(format_series_batch_detail(manager_label, stats)),
        blocker: build_sonarr_batch_blocker(counts),
        evidence,
        actions: Vec::new(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label,
        protocol,
        children,
    }
}

fn format_series_batch_detail(manager_label: &str, stats: SonarrSeriesStats) -> String {
    if stats.episode_count > 0 {
        format!(
            "{manager_label} is handling this series as release downloads. {} of {} episode files are imported so far.",
            stats.episode_file_count, stats.episode_count
        )
    } else if stats.episode_file_count > 0 {
        format!(
            "{manager_label} is handling this series as release downloads. {} episode files are already imported.",
            stats.episode_file_count
        )
    } else {
        format!("{manager_label} is handling this series as one or more release downloads.")
    }
}

fn build_download_attempt_child(
    entry: &ManagerQueueEntry,
    episodes: Option<&ManagerEpisodeIndex>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
) -> FindMediaAcquisitionChildItem {
    let state = derive_queue_entry_state(entry, 1, downloader_progress);
    let download_id = entry.download_id.clone();
    let progress = download_id
        .as_deref()
        .and_then(|download_id| downloader_progress.get(download_id));
    let (title, subtitle) = download_attempt_title(entry, progress, episodes);
    let id = download_id
        .clone()
        .or_else(|| entry.episode_id.clone())
        .or_else(|| entry.entry_id.clone())
        .unwrap_or_else(|| title.clone());

    FindMediaAcquisitionChildItem {
        id,
        title,
        release_id: None,
        source_provider_id: None,
        source_provider_label: None,
        source_suite: None,
        route_provider_id: None,
        route_provider_label: None,
        route_logical_id: None,
        subtitle,
        download_id,
        status: progress
            .and_then(|item| item.status.clone())
            .or_else(|| Some(entry.state.clone()).filter(|value| !value.trim().is_empty())),
        category: progress.and_then(|item| item.category.clone()),
        phase: state.phase.as_str().to_string(),
        phase_label: state.phase.label().to_string(),
        headline: state.headline,
        detail: state.detail,
        blocker: state.blocker,
        progress_percent: state.progress_percent,
        eta_seconds: state.eta_seconds,
        downloader_label: state.downloader_label,
        protocol: state.protocol,
        size_bytes: progress.and_then(|item| item.size_bytes),
        downloaded_bytes: progress.and_then(|item| item.downloaded_bytes),
        remaining_bytes: progress.and_then(|item| item.remaining_bytes),
        download_rate_bps: progress.and_then(|item| item.download_rate_bps),
        upload_rate_bps: progress.and_then(|item| item.upload_rate_bps),
        connected_seeds: progress.and_then(|item| item.connected_seeds),
        connected_peers: progress.and_then(|item| item.connected_peers),
        known_seeds: progress.and_then(|item| item.known_seeds),
        known_peers: progress.and_then(|item| item.known_peers),
        availability: progress.and_then(|item| item.availability),
        seen_complete_at: progress.and_then(|item| item.seen_complete_at),
    }
}

fn download_attempt_title(
    entry: &ManagerQueueEntry,
    downloader_progress: Option<&AcquisitionDownloaderProgress>,
    episodes: Option<&ManagerEpisodeIndex>,
) -> (String, Option<String>) {
    let release_title = entry
        .title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let title = downloader_progress
        .and_then(|item| item.release_title.clone())
        .or(release_title)
        .unwrap_or_else(|| "Download".to_string());

    let Some(episode) = entry
        .episode_id
        .as_deref()
        .and_then(|id| episodes.and_then(|index| index.get(id)))
    else {
        return (title, None);
    };

    let code = format!("S{:02}E{:02}", episode.season, episode.episode);
    let destination = if episode.title.trim().is_empty() {
        code
    } else {
        format!("{code} • {}", episode.title.trim())
    };
    (title, Some(destination))
}

/// Legacy Sonarr/Radarr JSON, converted to the general-purpose shapes. Used
/// only for the server's own Arr code until Stage 3 removes it.
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
    pub(crate) fn queue(implementation: &str, queue_value: Option<&Value>) -> Vec<ManagerQueueEntry> {
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
    pub(in crate::http::handlers::discovery) fn episodes(
        index: &HashMap<i64, SonarrEpisodeDescriptor>,
    ) -> ManagerEpisodeIndex {
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

/// One member of an installed marketplace stack, answered by the stack's own
/// code in its long-lived executor.
#[cfg(unix)]
pub(crate) struct StackMemberCall {
    marketplace: crate::marketplace::service::MarketplaceService,
    home: Uuid,
    extension: elixir_marketplace::identity::ExtensionId,
    instance: Uuid,
    member: String,
}

#[cfg(unix)]
#[derive(Deserialize)]
struct ExtensionReply {
    schema_version: u32,
    status: u16,
    #[serde(default)]
    body: Value,
}

#[cfg(unix)]
impl StackMemberCall {
    /// `None` for any extension that is not a marketplace stack member.
    async fn resolve(state: &AppState, member_id: &str) -> AnyResult<Option<Self>> {
        Self::resolve_with(&state.db_pool, &state.marketplace, member_id).await
    }

    async fn resolve_with(
        pool: &sqlx::AnyPool,
        marketplace: &crate::marketplace::service::MarketplaceService,
        member_id: &str,
    ) -> AnyResult<Option<Self>> {
        if !crate::marketplace::stack::is_stack_member_id(member_id) {
            return Ok(None);
        }
        let (stack, member) = crate::marketplace::stack::owner_of(pool, member_id)
            .await?
            .context("this member's stack extension is not installed or enabled")?;
        let (home, instance) = crate::marketplace::stack::installed_instance(pool, &stack)
            .await?
            .context("this member's stack extension has no enabled installation")?;
        Ok(Some(Self {
            marketplace: marketplace.clone(),
            home,
            extension: stack.parse().context("stack extension id")?,
            instance,
            member,
        }))
    }

    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        capability: &str,
        operation: &str,
        request: Value,
    ) -> AnyResult<T> {
        let arguments = elixir_marketplace::source_execution::RuntimeJson::from_value(json!({
            "schema_version": 1,
            "capability": capability,
            "member": self.member,
            "operation": operation,
            "request": request,
        }))
        .context("stack member request exceeds runtime bounds")?;
        let reply = match self
            .marketplace
            .serve_call(self.home, &self.extension, self.instance, arguments)
            .await?
        {
            crate::marketplace::service::ServeReply::Result(value) => value,
            crate::marketplace::service::ServeReply::Failed => {
                bail!("the stack extension failed this request")
            }
        };
        let reply: ExtensionReply =
            serde_json::from_str(reply.as_str()).context("stack extension reply shape")?;
        if reply.schema_version != 1 {
            bail!("stack extension reply schema is unsupported");
        }
        if reply.status != 200 {
            let message = reply
                .body
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| reply.body.to_string().chars().take(300).collect());
            bail!("stack extension returned {}: {message}", reply.status);
        }
        serde_json::from_value(reply.body).context("stack extension reply body")
    }
}

/// A Sonarr/Radarr member: the general-purpose media-manager interface.
#[cfg(unix)]
pub(crate) struct ExtensionManager {
    target: StackMemberCall,
    pub(crate) kind: ManagerItemKind,
}

#[cfg(unix)]
impl ExtensionManager {
    /// `None` for any provider that is not a marketplace stack member.
    pub(crate) async fn for_provider(
        state: &AppState,
        provider: &ProviderContext,
    ) -> AnyResult<Option<Self>> {
        Self::for_member(
            state,
            &provider.detail.extension_id,
            provider.detail.provider.implementation.as_deref(),
        )
        .await
    }

    /// `None` for any extension that is not a marketplace stack member.
    pub(crate) async fn for_member(
        state: &AppState,
        member_id: &str,
        implementation: Option<&str>,
    ) -> AnyResult<Option<Self>> {
        if !crate::marketplace::stack::is_stack_member_id(member_id) {
            return Ok(None);
        }
        let kind = match implementation
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("sonarr") => ManagerItemKind::Series,
            Some("radarr") => ManagerItemKind::Movie,
            other => bail!("unsupported stack media manager {other:?}"),
        };
        Ok(StackMemberCall::resolve(state, member_id)
            .await?
            .map(|target| Self { target, kind }))
    }

    /// One operation of the general-purpose interface; `kind` is added.
    pub(crate) async fn call<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        mut request: Value,
    ) -> AnyResult<T> {
        if let Some(object) = request.as_object_mut() {
            object.insert("kind".to_string(), serde_json::to_value(self.kind)?);
        }
        self.target.call("media.manager", operation, request).await
    }
}

/// A Prowlarr member: the general-purpose indexer-list interface.
#[cfg(unix)]
pub(crate) struct ExtensionIndexerList {
    target: StackMemberCall,
}

#[cfg(unix)]
impl ExtensionIndexerList {
    /// `None` unless this is a marketplace stack's indexer list.
    pub(crate) async fn for_member(
        state: &AppState,
        member_id: &str,
        implementation: Option<&str>,
    ) -> AnyResult<Option<Self>> {
        if !crate::marketplace::stack::is_stack_member_id(member_id)
            || implementation.map(|value| value.trim().to_ascii_lowercase()).as_deref()
                != Some("prowlarr")
        {
            return Ok(None);
        }
        Ok(StackMemberCall::resolve(state, member_id)
            .await?
            .map(|target| Self { target }))
    }

    pub(crate) async fn call<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        request: Value,
    ) -> AnyResult<T> {
        self.target.call("indexer.list", operation, request).await
    }

    pub(crate) async fn names(&self, operation: &str) -> AnyResult<Vec<String>> {
        #[derive(Deserialize)]
        struct Names {
            names: Vec<String>,
        }
        Ok(self.call::<Names>(operation, json!({})).await?.names)
    }
}

/// The orchestrator's setup of a marketplace stack's members, answered by the
/// stack's own code in its long-lived executor.
#[cfg(unix)]
pub(crate) struct ServeStackSetup {
    pool: sqlx::AnyPool,
    marketplace: crate::marketplace::service::MarketplaceService,
}

#[cfg(unix)]
impl ServeStackSetup {
    pub(crate) fn new(
        pool: sqlx::AnyPool,
        marketplace: crate::marketplace::service::MarketplaceService,
    ) -> Self {
        Self { pool, marketplace }
    }

    async fn target(
        &self,
        member_id: &str,
        implementation: &str,
    ) -> AnyResult<(StackMemberCall, &'static str)> {
        let capability = match implementation.trim().to_ascii_lowercase().as_str() {
            "sonarr" | "radarr" => "media.manager",
            "prowlarr" => "indexer.list",
            other => bail!("no stack setup interface for {other:?}"),
        };
        let target = StackMemberCall::resolve_with(&self.pool, &self.marketplace, member_id)
            .await?
            .context("stack member is not served")?;
        Ok((target, capability))
    }
}

#[cfg(unix)]
#[async_trait::async_trait]
impl crate::orchestrator::stack_setup::StackMemberSetup for ServeStackSetup {
    async fn evaluate_patch(
        &self,
        member_id: &str,
        implementation: &str,
        patch: Value,
        provider_url: &str,
    ) -> AnyResult<crate::drivers::DriftEvaluation> {
        let (target, capability) = self.target(member_id, implementation).await?;
        let value: Value = target
            .call(
                capability,
                "evaluate_patch",
                json!({ "patch": patch, "providerUrl": provider_url }),
            )
            .await?;
        crate::orchestrator::stack_setup::drift_from_json(value)
    }
    async fn apply_patch(
        &self,
        member_id: &str,
        implementation: &str,
        patch: Value,
        provider_url: &str,
    ) -> AnyResult<crate::drivers::ApplyResult> {
        let (target, capability) = self.target(member_id, implementation).await?;
        let value: Value = target
            .call(
                capability,
                "apply_patch",
                json!({ "patch": patch, "providerUrl": provider_url }),
            )
            .await?;
        crate::orchestrator::stack_setup::apply_from_json(value)
    }
    async fn bootstrap_files(&self, member_id: &str, implementation: &str) -> AnyResult<Vec<String>> {
        #[derive(Deserialize)]
        struct Reply {
            files: Vec<String>,
        }
        let (target, capability) = self.target(member_id, implementation).await?;
        Ok(target
            .call::<Reply>(capability, "bootstrap_files", json!({}))
            .await?
            .files)
    }
    async fn bootstrap(
        &self,
        member_id: &str,
        implementation: &str,
        files: std::collections::BTreeMap<String, String>,
        (username, password): (String, String),
    ) -> AnyResult<Option<String>> {
        #[derive(Deserialize)]
        struct Reply {
            #[serde(rename = "apiKey", default)]
            api_key: Option<String>,
        }
        let (target, capability) = self.target(member_id, implementation).await?;
        Ok(target
            .call::<Reply>(
                capability,
                "bootstrap",
                json!({ "files": files, "login": { "username": username, "password": password } }),
            )
            .await?
            .api_key)
    }
    async fn read_state(
        &self,
        member_id: &str,
        implementation: &str,
    ) -> AnyResult<crate::drivers::StateSnapshot> {
        #[derive(Deserialize)]
        struct Reply {
            #[serde(default)]
            summary: Option<String>,
        }
        let (target, capability) = self.target(member_id, implementation).await?;
        let reply: Reply = target.call(capability, "read_state", json!({})).await?;
        Ok(crate::drivers::StateSnapshot {
            summary: reply.summary,
            activity: None,
        })
    }
}

/// One lookup result of the general-purpose interface.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagerLookupItem {
    pub title: String,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default)]
    pub overview: Option<String>,
    #[serde(default)]
    pub external_ids: ManagerExternalIds,
    #[serde(default)]
    pub poster_url: Option<String>,
    #[serde(default)]
    pub poster_path: Option<String>,
    #[serde(default)]
    pub popularity: Option<f64>,
}
#[derive(Debug, Default, Deserialize)]
pub(crate) struct ManagerExternalIds {
    #[serde(default)]
    pub tvdb: Option<String>,
    #[serde(default)]
    pub tmdb: Option<String>,
    #[serde(default)]
    pub imdb: Option<String>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct ManagerLookupReply {
    pub items: Vec<ManagerLookupItem>,
}

/// The general-purpose status of one item: the item, its queue entries and,
/// for a series with downloads, its episode titles.
#[derive(Debug, Deserialize)]
pub(crate) struct ManagerStatusReply {
    pub item: ManagerItemStatus,
    #[serde(default)]
    pub queue: Vec<ManagerQueueEntry>,
    #[serde(default)]
    pub episodes: Option<ManagerEpisodeIndex>,
}

/// Files the manager imported for one item.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagerImportedFiles {
    #[serde(default)]
    pub external_ids: ManagerExternalIds,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub files: Vec<ManagerImportedFile>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagerImportedFile {
    pub manager_path: String,
    #[serde(default)]
    pub size_bytes: Option<i64>,
    #[serde(default)]
    pub season: Option<i32>,
    #[serde(default)]
    pub episode: Option<i32>,
    #[serde(default)]
    pub absolute_episode: Option<i32>,
    #[serde(default)]
    pub episode_title: Option<String>,
    #[serde(default)]
    pub video_codec: Option<String>,
    #[serde(default)]
    pub audio_codec: Option<String>,
    #[serde(default)]
    pub raw: Value,
}

/// Find Media results from a lookup reply. Relative poster paths are joined
/// to the member's own address, as the server did for its own lookups.
pub(crate) fn lookup_results(
    kind: ManagerItemKind,
    media_type: MediaType,
    base_url: Option<&str>,
    reply: ManagerLookupReply,
) -> Vec<DiscoveryResult> {
    reply
        .items
        .into_iter()
        .filter(|item| !item.title.trim().is_empty())
        .map(|item| {
            let ids = item.external_ids;
            let poster_url = item.poster_url.or_else(|| {
                let root = Url::parse(base_url?).ok()?;
                let path = item.poster_path?;
                Some(root.join(path.trim().trim_start_matches('/')).ok()?.to_string())
            });
            let (r#type, external_ids) = match kind {
                ManagerItemKind::Series => (
                    if media_type == MediaType::Anime {
                        MediaType::Anime
                    } else {
                        MediaType::Series
                    },
                    ExternalIds {
                        imdb: ids.imdb,
                        tmdb: ids.tmdb,
                        tvdb_series: ids.tvdb,
                        ..Default::default()
                    },
                ),
                ManagerItemKind::Movie => (
                    MediaType::Movie,
                    ExternalIds {
                        imdb: ids.imdb,
                        tmdb: ids.tmdb,
                        tvdb_movie: ids.tvdb,
                        ..Default::default()
                    },
                ),
            };
            DiscoveryResult {
                title: item.title.trim().to_string(),
                r#type,
                year: item.year,
                external_ids: Some(external_ids),
                description: item.overview,
                poster_url,
                popularity_score: item.popularity,
            }
        })
        .collect()
}

/// The add request of the general-purpose interface.
pub(crate) fn add_request(request: &crate::drivers::AddMediaRequest) -> Value {
    let ids = request.external_ids.clone().unwrap_or_default();
    json!({
        "mediaType": match request.media_type {
            MediaType::Movie => "movie",
            MediaType::Anime => "anime",
            _ => "series",
        },
        "title": request.title,
        "year": request.year,
        "externalIds": {
            "imdb": ids.imdb,
            "tmdb": ids.tmdb,
            "tvdb": ids.tvdb,
            "tvdbSeries": ids.tvdb_series,
            "tvdbMovie": ids.tvdb_movie,
            "anilist": ids.anilist,
        },
        "monitor": request.options.monitor,
        "search": request.options.search,
        "rootFolderPath": request.options.root_folder_path,
        "qualityProfileId": request.options.quality_profile_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reply the Arr Stack module produced in the real engine for one
    /// series with a downloading episode (QuickJS port harness).
    const STATUS: &str = r#"{"episodes":{"100":{"episode":3,"season":1,"title":"T"}},"item":{"fileCount":2,"hasFiles":true,"totalCount":10},"queue":[{"downloadId":"a","downloader":null,"entryId":null,"episodeId":"100","errorMessage":null,"itemId":"42","protocol":null,"state":"downloading","statusMessage":null,"title":null}]}"#;

    #[test]
    fn extension_status_derives_the_same_series_batch_state() {
        let status: ManagerStatusReply = serde_json::from_str(STATUS).unwrap();
        let state = derive_manager_acquisition_state(
            ManagerAcquisitionInput {
                kind: ManagerItemKind::Series,
                manager_label: "Sonarr",
                item_id: "42",
                item: &status.item,
                queue: &status.queue,
                episodes: status.episodes.as_ref(),
            },
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
        );
        assert_eq!(state.phase, AcquisitionPhase::Downloading);
        assert_eq!(state.headline, "1 transfer downloading.");
        assert_eq!(
            state.detail.as_deref(),
            Some("Sonarr is handling this series as release downloads. 2 of 10 episode files are imported so far.")
        );
        assert_eq!(state.children.len(), 1);
        assert_eq!(state.children[0].subtitle.as_deref(), Some("S01E03 • T"));
        assert_eq!(state.children[0].id, "a");
    }

    #[test]
    fn legacy_and_extension_shapes_agree_for_a_queued_movie() {
        let item = json!({"hasFile": false, "movieFileId": 0});
        let queue = json!({"records": [{"movieId": 5, "downloadId": "x", "downloadClient": "qBittorrent",
            "protocol": "torrent", "trackedDownloadState": "downloading", "trackedDownloadStatus": "ok", "status": "queued"}]});
        let legacy = derive_arr_acquisition_state(
            "radarr", "5", &item, Some(&queue), false, None,
            &AcquisitionDownloaderProgressIndex::default(), None,
        );
        let extension: ManagerStatusReply = serde_json::from_value(json!({
            "item": {"hasFiles": false, "fileCount": 0, "totalCount": 0},
            "queue": [{"itemId": "5", "downloadId": "x", "downloader": "qBittorrent", "protocol": "torrent",
                "state": "downloading ok queued", "statusMessage": null, "errorMessage": null}],
            "episodes": null
        }))
        .unwrap();
        let derived = derive_manager_acquisition_state(
            ManagerAcquisitionInput {
                kind: ManagerItemKind::Movie,
                manager_label: "Radarr",
                item_id: "5",
                item: &extension.item,
                queue: &extension.queue,
                episodes: None,
            },
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
        );
        assert_eq!(
            serde_json::to_value(&legacy.headline).unwrap(),
            serde_json::to_value(&derived.headline).unwrap()
        );
        assert_eq!(legacy.phase, derived.phase);
        assert_eq!(legacy.detail, derived.detail);
        assert_eq!(legacy.downloader_label, derived.downloader_label);
        assert_eq!(legacy.children.len(), derived.children.len());
    }

    #[test]
    fn lookup_results_join_relative_posters_to_the_member() {
        let reply: ManagerLookupReply = serde_json::from_value(json!({"items": [
            {"title": "The Expanse", "year": 2015, "externalIds": {"tvdb": "280619", "imdb": "tt3230854"},
             "posterPath": "/MediaCover/1/poster.jpg", "popularity": 8.4},
            {"title": "Heat", "externalIds": {}, "posterUrl": "https://image.example/p.jpg"},
            {"title": "  "}
        ]}))
        .unwrap();
        let results = lookup_results(
            ManagerItemKind::Series,
            MediaType::Anime,
            Some("http://127.0.0.1:8989/"),
            reply,
        );
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].r#type, MediaType::Anime);
        assert_eq!(
            results[0].poster_url.as_deref(),
            Some("http://127.0.0.1:8989/MediaCover/1/poster.jpg")
        );
        let ids = results[0].external_ids.as_ref().unwrap();
        assert_eq!(ids.tvdb_series.as_deref(), Some("280619"));
        assert_eq!(results[1].poster_url.as_deref(), Some("https://image.example/p.jpg"));
    }
}
