use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{AnyPool, Row, any::AnyRow};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    db::models::{MediaType, ProviderHealthState, ProviderReadinessPhase, SlotCardinality},
    debrid::REAL_DEBRID_EXTENSION_ID,
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, DownloadBrokerBindingKind, DownloadBrokerRouteRecord,
        TORRENT_DEFAULT_LOGICAL_ID, list_acquisition_routes,
    },
    extensions::{
        ExternalIds, MediaIdentity,
        store::{ExtensionStore, ManagedIngestIntent, NewExtensionInstance, NewProvider},
    },
    http::handlers::download_broker::submit_download_broker_source,
    library::{AniZipEpisodeRecord, AniZipMapping, resolve_anilist_season_chain},
    network::protection::observed_download_protection_status,
    orchestrator::{model::ProviderEndpoint, planner::stable_provider_id},
    state::AppState,
};

pub const TORRENTIO_EXTENSION_ID: &str = "elixir.extensions.torrentio";
pub const TORRENTIO_IMPLEMENTATION: &str = "torrentio_stremio";
pub const TORRENTIO_CANDIDATE_CAPABILITY: &str = "acquisition.candidate_provider";

const DEFAULT_TORRENTIO_BASE_URL: &str = "https://torrentio.strem.fun";
const DEFAULT_RESULT_LIMIT: usize = 40;
const MAX_RESULT_LIMIT: usize = 100;
const TORRENTIO_HTTP_TIMEOUT_SECONDS: u64 = 20;
const TORRENTIO_ACQUISITION_WORKER_TICK_SECONDS: u64 = 60;
const TORRENTIO_ACQUISITION_MAX_SUBMISSIONS_PER_ITERATION: usize = 8;
const TORRENTIO_ACQUISITION_RELEASE_DELAY_SECONDS: i64 = 0;
const TORRENTIO_METADATA_REFRESH_MIN_SECONDS: i64 = 6 * 60 * 60;
const TORRENTIO_METADATA_REFRESH_JITTER_SECONDS: i64 = 6 * 60 * 60;
const TORRENTIO_METADATA_AIR_WINDOW_JITTER_SECONDS: i64 = 5 * 60;
const TORRENTIO_CANDIDATE_INITIAL_JITTER_SECONDS: i64 = 5 * 60;
const TORRENTIO_CANDIDATE_DISCOVERY_MIN_SECONDS: i64 = 10 * 60;
const TORRENTIO_CANDIDATE_DISCOVERY_JITTER_SECONDS: i64 = 5 * 60;
const TORRENTIO_CANDIDATE_HOT_WINDOW_SECONDS: i64 = 2 * 60 * 60;
const TORRENTIO_CANDIDATE_WARM_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const TORRENTIO_CANDIDATE_HOT_RETRY_SECONDS: i64 = 15 * 60;
const TORRENTIO_CANDIDATE_WARM_RETRY_SECONDS: i64 = 60 * 60;
const TORRENTIO_CANDIDATE_COLD_RETRY_SECONDS: i64 = 24 * 60 * 60;

const TORRENTIO_JOB_PENDING: &str = "pending";
const TORRENTIO_JOB_SUBMITTED: &str = "submitted";
const TORRENTIO_JOB_BLOCKED: &str = "blocked";
const TORRENTIO_JOB_FAILED: &str = "failed";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentioCandidateSearchRequest {
    #[serde(alias = "media_type")]
    pub media_type: String,
    #[serde(default, alias = "external_ids")]
    pub external_ids: Option<ExternalIds>,
    #[serde(default)]
    pub season: Option<i32>,
    #[serde(default)]
    pub episode: Option<i32>,
    #[serde(default)]
    pub stremio_id: Option<String>,
    #[serde(default, alias = "provider_id")]
    pub provider_id: Option<Uuid>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentioCandidateSearchResponse {
    pub extension_id: String,
    pub owner_id: String,
    pub provider_id: Uuid,
    pub media_type: String,
    pub stremio_type: String,
    pub stremio_id: String,
    pub route_policy: String,
    pub route_options: Vec<TorrentioRouteOption>,
    pub candidates: Vec<TorrentioCandidate>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentioRouteOption {
    pub logical_id: String,
    pub label: String,
    pub description: String,
    pub binding_kind: DownloadBrokerBindingKind,
    pub selected_provider_id: Option<Uuid>,
    pub selected_extension_id: Option<String>,
    pub available: bool,
    pub needs_account: bool,
    pub account_extension_id: Option<String>,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentioCandidate {
    pub id: String,
    pub source_provider: String,
    pub title: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub source: String,
    pub source_kind: String,
    pub info_hash: Option<String>,
    pub file_index: Option<i64>,
    pub quality: Option<String>,
    pub size_bytes: Option<u64>,
    pub seeders: Option<u32>,
    pub language: Option<String>,
    pub cached_debrid: Option<bool>,
    pub route_logical_ids: Vec<String>,
    pub default_route_logical_id: Option<String>,
    pub score: i64,
    pub score_badges: Vec<TorrentioScoreBadge>,
    pub rank: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentioScoreBadge {
    pub label: String,
    pub value: i64,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct TorrentioAcquisitionJobView {
    pub job_id: Uuid,
    pub title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub status: String,
    pub route_logical_id: Option<String>,
    pub candidate_id: Option<String>,
    pub candidate_title: Option<String>,
    pub candidate_source: Option<String>,
    pub candidate_source_kind: Option<String>,
    pub candidate_info_hash: Option<String>,
    pub candidate_file_index: Option<i64>,
    pub candidate_quality: Option<String>,
    pub candidate_size_bytes: Option<u64>,
    pub candidate_seeders: Option<u32>,
    pub candidate_language: Option<String>,
    pub candidate_cached_debrid: Option<bool>,
    pub candidate_score: Option<i64>,
    pub candidate_score_badges: Vec<TorrentioScoreBadge>,
    pub candidate_rank: Option<i64>,
    pub download_id: Option<String>,
    pub last_error: Option<String>,
    pub import_event_id: Option<Uuid>,
    pub imported_at: Option<DateTime<Utc>>,
    pub import_error: Option<String>,
}

#[derive(Debug, Clone)]
struct TorrentioAcquisitionJob {
    job_id: Uuid,
    intent_id: Uuid,
    search_media_type: MediaType,
    target_key: String,
    title: String,
    external_ids: ExternalIds,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    aired_at: Option<DateTime<Utc>>,
    status: String,
    route_logical_id: Option<String>,
    candidate_id: Option<String>,
    candidate_title: Option<String>,
    candidate_source: Option<String>,
    candidate_source_kind: Option<String>,
    candidate_info_hash: Option<String>,
    candidate_file_index: Option<i64>,
    candidate_quality: Option<String>,
    candidate_size_bytes: Option<u64>,
    candidate_seeders: Option<u32>,
    candidate_language: Option<String>,
    candidate_cached_debrid: Option<bool>,
    candidate_score: Option<i64>,
    candidate_score_badges: Vec<TorrentioScoreBadge>,
    candidate_rank: Option<i64>,
    download_id: Option<String>,
    last_error: Option<String>,
    import_event_id: Option<Uuid>,
    imported_at: Option<DateTime<Utc>>,
    import_error: Option<String>,
    next_search_at: Option<DateTime<Utc>>,
    search_attempts: i64,
}

#[derive(Debug, Clone)]
struct TorrentioAcquisitionTarget {
    target_key: String,
    title: String,
    external_ids: ExternalIds,
    search_media_type: MediaType,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    aired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct TorrentioTargetExpansion {
    targets: Vec<TorrentioAcquisitionTarget>,
    next_air_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct TorrentioSubscriptionSchedule {
    next_metadata_refresh_at: Option<DateTime<Utc>>,
}

impl TorrentioTargetExpansion {
    fn extend(&mut self, other: TorrentioTargetExpansion) {
        self.targets.extend(other.targets);
        if let Some(next_air_at) = other.next_air_at {
            self.note_future_air(next_air_at);
        }
    }

    fn note_future_air(&mut self, aired_at: DateTime<Utc>) {
        if self
            .next_air_at
            .map(|current| aired_at < current)
            .unwrap_or(true)
        {
            self.next_air_at = Some(aired_at);
        }
    }
}

#[derive(Debug, Clone)]
struct TorrentioSubmissionChoice {
    candidate: TorrentioCandidate,
    route: TorrentioRouteOption,
    score: i64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TorrentioAcquisitionIterationSummary {
    pub scanned_intents: usize,
    pub pending_targets: usize,
    pub submitted_targets: usize,
    pub blocked_targets: usize,
    pub failed_targets: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StremioStreamResponse {
    #[serde(default)]
    streams: Vec<StremioStream>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StremioStream {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    info_hash: Option<String>,
    #[serde(default)]
    file_idx: Option<i64>,
    #[serde(default)]
    sources: Vec<String>,
    #[serde(default)]
    behavior_hints: Option<StremioBehaviorHints>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StremioBehaviorHints {
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    video_size: Option<u64>,
}

#[derive(Debug, Clone)]
struct TorrentioProviderSelection {
    provider_id: Uuid,
    config: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TorrentioRoutePolicy {
    DebridFirst,
    TorrentFirst,
    AskEachTime,
}

#[derive(Debug, Clone)]
struct TorrentioSourcePreferences {
    route_policy: TorrentioRoutePolicy,
    allowed_qualities: Vec<String>,
    required_languages: Vec<String>,
    max_size_bytes: Option<u64>,
}

pub async fn ensure_torrentio_installed_provider(state: &AppState) -> Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    ensure_torrentio_installed_provider_in_store(&store).await
}

async fn ensure_torrentio_installed_provider_in_store(store: &ExtensionStore<'_>) -> Result<()> {
    let Some(extension) = store.get_extension(TORRENTIO_EXTENSION_ID).await? else {
        return Ok(());
    };
    if !extension.enabled {
        return Ok(());
    }

    let mut instances = store.list_instances(Some(TORRENTIO_EXTENSION_ID)).await?;
    if instances.is_empty() {
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: TORRENTIO_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(default_torrentio_config()),
                enabled: true,
            })
            .await?;
        instances = store.list_instances(Some(TORRENTIO_EXTENSION_ID)).await?;
    }

    let Some(instance) = instances
        .into_iter()
        .filter(|instance| instance.enabled)
        .min_by_key(|instance| {
            (
                !instance.instance_name.eq_ignore_ascii_case("default"),
                instance.instance_name.clone(),
            )
        })
    else {
        return Ok(());
    };

    let provider_id = stable_provider_id(
        instance.instance_id,
        TORRENTIO_CANDIDATE_CAPABILITY,
        "default",
    );
    let endpoint = provider_endpoint_for_url(DEFAULT_TORRENTIO_BASE_URL)?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id: instance.instance_id,
            capability: TORRENTIO_CANDIDATE_CAPABILITY.to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some(TORRENTIO_IMPLEMENTATION.to_string()),
            scope_json: Some(json!({
                "media_types": ["movies", "tv", "anime"],
                "actions": ["search"]
            })),
            endpoint_json: Some(serde_json::to_value(endpoint)?),
            health_state: ProviderHealthState::Healthy,
        })
        .await?;
    store
        .upsert_provider_readiness(
            provider_id,
            ProviderReadinessPhase::DriverReady,
            Some("Torrentio-compatible source discovery is available through the installed extension package."),
        )
        .await?;
    Ok(())
}

pub async fn search_torrentio_candidates(
    state: &AppState,
    request: TorrentioCandidateSearchRequest,
) -> Result<TorrentioCandidateSearchResponse> {
    let media_type = parse_candidate_media_type(&request.media_type)?;
    let (stremio_type, stremio_id) = resolve_stremio_stream_target(&request, media_type)?;
    let store = ExtensionStore::new(&state.db_pool);
    let provider = select_torrentio_provider(&store, request.provider_id).await?;
    let config = merge_torrentio_config(provider.config.as_object());
    let preferences = torrentio_source_preferences(&config);
    let limit = request
        .limit
        .unwrap_or_else(|| config_result_limit(&config))
        .clamp(1, MAX_RESULT_LIMIT);
    let stream_url = build_stream_url(&config, &stremio_type, &stremio_id)?;
    let route_options = torrentio_route_options(state, &store).await?;

    let response = Client::builder()
        .timeout(Duration::from_secs(TORRENTIO_HTTP_TIMEOUT_SECONDS))
        .user_agent("Elixir/0.1 Torrentio Source")
        .build()
        .context("building Torrentio source HTTP client")?
        .get(stream_url)
        .send()
        .await
        .context("calling Torrentio-compatible source")?;
    let status = response.status();
    let body = response.text().await.context("reading source response")?;
    if !status.is_success() {
        bail!("Torrentio-compatible source returned {status}");
    }
    let streams: StremioStreamResponse =
        serde_json::from_str(&body).context("parsing Stremio stream response")?;

    let mut candidates = Vec::new();
    for (idx, stream) in streams.streams.iter().enumerate() {
        let Some(mut candidate) = candidate_from_stream(
            stream,
            idx,
            &stremio_type,
            &stremio_id,
            &route_options,
            preferences.route_policy,
        ) else {
            continue;
        };
        if !torrentio_candidate_matches_preferences(&candidate, &preferences) {
            continue;
        }
        annotate_torrentio_candidate_score(
            &mut candidate,
            &route_options,
            preferences.route_policy,
        );
        candidates.push(candidate);
    }
    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.rank.cmp(&right.rank))
            .then_with(|| left.id.cmp(&right.id))
    });
    candidates.truncate(limit);

    Ok(TorrentioCandidateSearchResponse {
        extension_id: TORRENTIO_EXTENSION_ID.to_string(),
        owner_id: TORRENTIO_EXTENSION_ID.to_string(),
        provider_id: provider.provider_id,
        media_type: media_type_api_name(media_type).to_string(),
        stremio_type,
        stremio_id,
        route_policy: preferences.route_policy.as_str().to_string(),
        route_options,
        candidates,
    })
}

pub async fn start_torrentio_acquisition_loop(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(
        TORRENTIO_ACQUISITION_WORKER_TICK_SECONDS,
    ));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        match run_torrentio_acquisition_iteration(&state).await {
            Ok(summary) => {
                if summary.submitted_targets > 0
                    || summary.blocked_targets > 0
                    || summary.failed_targets > 0
                {
                    info!(
                        scanned_intents = summary.scanned_intents,
                        pending_targets = summary.pending_targets,
                        submitted_targets = summary.submitted_targets,
                        blocked_targets = summary.blocked_targets,
                        failed_targets = summary.failed_targets,
                        "Torrentio monitored acquisition iteration completed"
                    );
                }
            }
            Err(err) => warn!("Torrentio monitored acquisition iteration failed: {err}"),
        }
    }
}

pub async fn run_torrentio_acquisition_iteration(
    state: &AppState,
) -> Result<TorrentioAcquisitionIterationSummary> {
    let store = ExtensionStore::new(&state.db_pool);
    let intents = store.list_active_managed_ingest_intents().await?;
    if intents.is_empty() {
        return Ok(TorrentioAcquisitionIterationSummary::default());
    }

    let mut summary = TorrentioAcquisitionIterationSummary {
        scanned_intents: intents.len(),
        ..Default::default()
    };
    let mut submissions_remaining = TORRENTIO_ACQUISITION_MAX_SUBMISSIONS_PER_ITERATION;
    let now = Utc::now();

    for intent in intents {
        if intent.media_type == MediaType::Movie && intent.last_matched_at.is_some() {
            continue;
        }

        let schedule =
            upsert_torrentio_acquisition_subscription(&state.db_pool, &intent, now).await?;
        if torrentio_metadata_refresh_due(&schedule, now) {
            let expansion = match torrentio_targets_for_intent(state, &intent).await {
                Ok(expansion) => expansion,
                Err(err) => {
                    warn!(
                        intent_id = %intent.intent_id,
                        media_type = intent.media_type.as_str(),
                        title = %intent.title,
                        "Torrentio target expansion failed: {err}"
                    );
                    TorrentioTargetExpansion::default()
                }
            };
            summary.pending_targets += expansion.targets.len();
            let expanded_target_count = expansion.targets.len();
            for target in expansion.targets {
                upsert_torrentio_acquisition_job(&state.db_pool, &intent, &target, now).await?;
            }
            schedule_next_torrentio_metadata_refresh(
                &state.db_pool,
                intent.intent_id,
                now,
                expansion.next_air_at,
                expanded_target_count,
            )
            .await?;
        }

        if submissions_remaining == 0 {
            continue;
        }

        let jobs = list_ready_torrentio_acquisition_jobs_for_intent(
            &state.db_pool,
            intent.intent_id,
            now,
            submissions_remaining,
        )
        .await?;
        summary.pending_targets += jobs.len();

        for job in jobs {
            let target = torrentio_target_from_job(&job);
            if !torrentio_job_is_ready_for_submission(&job, now) || submissions_remaining == 0 {
                continue;
            }

            match submit_torrentio_target(state, &intent, &target, &job).await {
                Ok(()) => {
                    summary.submitted_targets += 1;
                    submissions_remaining = submissions_remaining.saturating_sub(1);
                }
                Err(TorrentioTargetSubmitError::Blocked(message)) => {
                    let next_search_at = next_torrentio_candidate_search_at(
                        &target,
                        job.search_attempts + 1,
                        Utc::now(),
                    );
                    mark_torrentio_acquisition_job_blocked(
                        &state.db_pool,
                        job.job_id,
                        &message,
                        next_search_at,
                    )
                    .await?;
                    summary.blocked_targets += 1;
                }
                Err(TorrentioTargetSubmitError::Failed(message)) => {
                    let next_search_at = next_torrentio_candidate_search_at(
                        &target,
                        job.search_attempts + 1,
                        Utc::now(),
                    );
                    mark_torrentio_acquisition_job_failed(
                        &state.db_pool,
                        job.job_id,
                        &message,
                        next_search_at,
                    )
                    .await?;
                    summary.failed_targets += 1;
                }
            }
        }
    }

    Ok(summary)
}

pub async fn list_torrentio_acquisition_jobs_for_intents(
    pool: &AnyPool,
    intent_ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<TorrentioAcquisitionJobView>>> {
    if intent_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let wanted: std::collections::HashSet<Uuid> = intent_ids.iter().copied().collect();
    let rows = sqlx::query(
        "SELECT
            job_id,
            intent_id,
            media_type,
            COALESCE(search_media_type, media_type) AS search_media_type,
            target_key,
            title,
            year,
            CAST(external_ids_json AS TEXT) AS external_ids_json,
            season_number,
            episode_number,
            absolute_episode_number,
            CAST(aired_at AS TEXT) AS aired_at,
            status,
            CAST(route_logical_id AS TEXT) AS route_logical_id,
            CAST(candidate_id AS TEXT) AS candidate_id,
            CAST(candidate_title AS TEXT) AS candidate_title,
            CAST(candidate_source AS TEXT) AS candidate_source,
            CAST(candidate_source_kind AS TEXT) AS candidate_source_kind,
            CAST(candidate_info_hash AS TEXT) AS candidate_info_hash,
            CAST(candidate_file_index AS INTEGER) AS candidate_file_index,
            CAST(candidate_quality AS TEXT) AS candidate_quality,
            CAST(candidate_size_bytes AS INTEGER) AS candidate_size_bytes,
            CAST(candidate_seeders AS INTEGER) AS candidate_seeders,
            CAST(candidate_language AS TEXT) AS candidate_language,
            CAST(candidate_cached_debrid AS INTEGER) AS candidate_cached_debrid,
            CAST(candidate_score AS INTEGER) AS candidate_score,
            CAST(candidate_score_badges_json AS TEXT) AS candidate_score_badges_json,
            CAST(candidate_rank AS INTEGER) AS candidate_rank,
            CAST(download_id AS TEXT) AS download_id,
            CAST(last_error AS TEXT) AS last_error,
            CAST(import_event_id AS TEXT) AS import_event_id,
            CAST(imported_at AS TEXT) AS imported_at,
            CAST(import_error AS TEXT) AS import_error,
            CAST(next_search_at AS TEXT) AS next_search_at,
            CAST(search_attempts AS INTEGER) AS search_attempts,
            CAST(updated_at AS TEXT) AS updated_at
         FROM torrentio_acquisition_jobs
         ORDER BY created_at DESC",
    )
    .fetch_all(pool)
    .await?;

    let mut items: HashMap<Uuid, Vec<TorrentioAcquisitionJobView>> = HashMap::new();
    for row in rows {
        let job = map_torrentio_acquisition_job(&row)?;
        if !wanted.contains(&job.intent_id) {
            continue;
        }
        items
            .entry(job.intent_id)
            .or_default()
            .push(TorrentioAcquisitionJobView {
                job_id: job.job_id,
                title: job.title,
                season_number: job.season_number,
                episode_number: job.episode_number,
                absolute_episode_number: job.absolute_episode_number,
                status: job.status,
                route_logical_id: job.route_logical_id,
                candidate_id: job.candidate_id,
                candidate_title: job.candidate_title,
                candidate_source: job.candidate_source,
                candidate_source_kind: job.candidate_source_kind,
                candidate_info_hash: job.candidate_info_hash,
                candidate_file_index: job.candidate_file_index,
                candidate_quality: job.candidate_quality,
                candidate_size_bytes: job.candidate_size_bytes,
                candidate_seeders: job.candidate_seeders,
                candidate_language: job.candidate_language,
                candidate_cached_debrid: job.candidate_cached_debrid,
                candidate_score: job.candidate_score,
                candidate_score_badges: job.candidate_score_badges,
                candidate_rank: job.candidate_rank,
                download_id: job.download_id,
                last_error: job.last_error,
                import_event_id: job.import_event_id,
                imported_at: job.imported_at,
                import_error: job.import_error,
            });
    }
    Ok(items)
}

#[derive(Debug)]
enum TorrentioTargetSubmitError {
    Blocked(String),
    Failed(String),
}

async fn submit_torrentio_target(
    state: &AppState,
    intent: &ManagedIngestIntent,
    target: &TorrentioAcquisitionTarget,
    job: &TorrentioAcquisitionJob,
) -> std::result::Result<(), TorrentioTargetSubmitError> {
    let request = TorrentioCandidateSearchRequest {
        media_type: media_type_api_name(target.search_media_type).to_string(),
        external_ids: Some(target.external_ids.clone()),
        season: target.season_number,
        episode: target.episode_number,
        stremio_id: None,
        provider_id: None,
        limit: Some(DEFAULT_RESULT_LIMIT),
    };

    let response = search_torrentio_candidates(state, request)
        .await
        .map_err(|err| TorrentioTargetSubmitError::Failed(err.to_string()))?;
    mark_torrentio_acquisition_job_searched(&state.db_pool, job.job_id, response.provider_id)
        .await
        .map_err(|err| TorrentioTargetSubmitError::Failed(err.to_string()))?;

    let Some(choice) = choose_torrentio_submission(&response) else {
        let message = torrentio_route_blocker_message(&response).unwrap_or_else(|| {
            "No downloadable Torrentio candidate is currently available.".to_string()
        });
        return Err(TorrentioTargetSubmitError::Blocked(message));
    };

    let submit_result = submit_download_broker_source(
        state,
        &choice.route.logical_id,
        Some(TORRENTIO_EXTENSION_ID),
        &choice.candidate.source,
        Some(&choice.candidate.title),
    )
    .await
    .map_err(|err| TorrentioTargetSubmitError::Blocked(format_api_error(&err)))?;

    let download_id = submit_result
        .download_id
        .or_else(|| choice.candidate.info_hash.clone());
    mark_torrentio_acquisition_job_submitted(
        &state.db_pool,
        job.job_id,
        &choice,
        download_id.as_deref(),
    )
    .await
    .map_err(|err| TorrentioTargetSubmitError::Failed(err.to_string()))?;

    info!(
        intent_id = %intent.intent_id,
        job_id = %job.job_id,
        target_key = %target.target_key,
        route = %choice.route.logical_id,
        candidate_id = %choice.candidate.id,
        score = choice.score,
        "Torrentio monitored acquisition submitted source candidate"
    );
    Ok(())
}

async fn torrentio_targets_for_intent(
    state: &AppState,
    intent: &ManagedIngestIntent,
) -> Result<TorrentioTargetExpansion> {
    let external_ids = intent.external_ids.clone().unwrap_or_default();
    match intent.media_type {
        MediaType::Movie => Ok(TorrentioTargetExpansion {
            targets: vec![TorrentioAcquisitionTarget {
                target_key: "movie".to_string(),
                title: intent.title.clone(),
                external_ids,
                search_media_type: MediaType::Movie,
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                aired_at: None,
            }],
            next_air_at: None,
        }),
        MediaType::Series => {
            let expansion = torrentio_series_targets_from_elixir_metadata(state, intent).await?;
            if expansion.targets.is_empty() {
                bail!(
                    "series episode target expansion requires Elixir metadata with aired episode identities"
                );
            }
            Ok(expansion)
        }
        MediaType::Anime => {
            let expansion = torrentio_anime_targets_from_elixir_metadata(state, intent).await?;
            if expansion.targets.is_empty() {
                bail!(
                    "anime episode target expansion requires AniList/AniZip or IMDb-backed Elixir metadata with aired episode identities"
                );
            }
            Ok(expansion)
        }
    }
}

async fn torrentio_series_targets_from_elixir_metadata(
    state: &AppState,
    intent: &ManagedIngestIntent,
) -> Result<TorrentioTargetExpansion> {
    let mut expansion = torrentio_episode_targets_from_library(state, intent, MediaType::Series)
        .await
        .unwrap_or_else(|err| {
            warn!(
                intent_id = %intent.intent_id,
                title = %intent.title,
                "Elixir library episode expansion failed: {err}"
            );
            TorrentioTargetExpansion::default()
        });
    expansion.extend(torrentio_episode_targets_from_tvdb(state, intent).await?);
    expansion
        .extend(torrentio_episode_targets_from_cinemeta(state, intent, MediaType::Series).await?);
    expansion.targets = dedupe_torrentio_targets(expansion.targets);
    Ok(expansion)
}

async fn torrentio_anime_targets_from_elixir_metadata(
    state: &AppState,
    intent: &ManagedIngestIntent,
) -> Result<TorrentioTargetExpansion> {
    let mut expansion = torrentio_episode_targets_from_library(state, intent, MediaType::Series)
        .await
        .unwrap_or_else(|err| {
            warn!(
                intent_id = %intent.intent_id,
                title = %intent.title,
                "Elixir anime library episode expansion failed: {err}"
            );
            TorrentioTargetExpansion::default()
        });
    expansion.extend(torrentio_episode_targets_from_anizip(state, intent).await?);
    expansion
        .extend(torrentio_episode_targets_from_cinemeta(state, intent, MediaType::Series).await?);
    expansion.targets = dedupe_torrentio_targets(expansion.targets);
    Ok(expansion)
}

async fn torrentio_episode_targets_from_library(
    state: &AppState,
    intent: &ManagedIngestIntent,
    search_media_type: MediaType,
) -> Result<TorrentioTargetExpansion> {
    let external_ids = intent.external_ids.clone().unwrap_or_default();
    if external_ids
        .imdb
        .as_deref()
        .and_then(non_empty_str)
        .is_none()
    {
        return Ok(TorrentioTargetExpansion::default());
    }

    let series_ids = find_library_series_ids(&state.db_pool, intent, &external_ids).await?;
    if series_ids.is_empty() {
        return Ok(TorrentioTargetExpansion::default());
    }

    let release_cutoff = torrentio_release_cutoff();
    let mut expansion = TorrentioTargetExpansion::default();
    for series_id in series_ids {
        let rows = sqlx::query(
            "SELECT
                season_number,
                episode_number,
                absolute_episode_number,
                CAST(title AS TEXT) AS title,
                CAST(metadata_json AS TEXT) AS metadata_json,
                CAST(has_file AS INTEGER) AS has_file
             FROM episodes
             WHERE series_id = ?
               AND season_number > 0
               AND episode_number > 0
             ORDER BY season_number ASC, episode_number ASC",
        )
        .bind(series_id)
        .fetch_all(&state.db_pool)
        .await?;

        for row in rows {
            let has_file = row.try_get::<i64, _>("has_file").unwrap_or_default() != 0;
            if has_file {
                continue;
            }
            let season_number = row.try_get::<i32, _>("season_number")?;
            let episode_number = row.try_get::<i32, _>("episode_number")?;
            let metadata_json: Option<String> = row.try_get("metadata_json").ok().flatten();
            let metadata = metadata_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
            let Some(aired_at) = metadata.as_ref().and_then(episode_air_date) else {
                continue;
            };
            if aired_at > release_cutoff {
                expansion.note_future_air(aired_at);
                continue;
            }
            let title = row.try_get::<Option<String>, _>("title").ok().flatten();
            let absolute_episode_number = row
                .try_get::<Option<i32>, _>("absolute_episode_number")
                .ok()
                .flatten();
            expansion.targets.push(build_episode_target(
                intent,
                search_media_type,
                external_ids.clone(),
                season_number,
                episode_number,
                absolute_episode_number,
                Some(aired_at),
                title.as_deref(),
            ));
        }
    }
    Ok(expansion)
}

async fn find_library_series_ids(
    pool: &AnyPool,
    intent: &ManagedIngestIntent,
    external_ids: &ExternalIds,
) -> Result<Vec<String>> {
    let tvdb_series = external_ids
        .tvdb_series
        .as_deref()
        .and_then(non_empty_str)
        .or_else(|| external_ids.tvdb.as_deref().and_then(non_empty_str))
        .map(str::to_string);
    let rows = sqlx::query(
        "SELECT id
         FROM series
         WHERE (? IS NOT NULL AND external_imdb = ?)
            OR (? IS NOT NULL AND external_tvdb_series = ?)
            OR (? IS NOT NULL AND external_anilist = ?)
            OR (
                lower(title) = lower(?)
                AND (? IS NULL OR year IS NULL OR year = ?)
            )
         ORDER BY updated_at DESC
         LIMIT 5",
    )
    .bind(external_ids.imdb.as_deref())
    .bind(external_ids.imdb.as_deref())
    .bind(tvdb_series.as_deref())
    .bind(tvdb_series.as_deref())
    .bind(external_ids.anilist.as_deref())
    .bind(external_ids.anilist.as_deref())
    .bind(&intent.title)
    .bind(intent.year)
    .bind(intent.year)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("id").ok())
        .collect())
}

async fn torrentio_episode_targets_from_tvdb(
    state: &AppState,
    intent: &ManagedIngestIntent,
) -> Result<TorrentioTargetExpansion> {
    let mut external_ids = intent.external_ids.clone().unwrap_or_default();
    let Some(imdb_id) = external_ids.imdb.as_deref().and_then(non_empty_str) else {
        return Ok(TorrentioTargetExpansion::default());
    };

    let tvdb_series_id = external_ids
        .tvdb_series
        .as_deref()
        .and_then(non_empty_str)
        .or_else(|| external_ids.tvdb.as_deref().and_then(non_empty_str))
        .map(str::to_string)
        .or_else(|| None);
    let tvdb_series_id = match tvdb_series_id {
        Some(value) => Some(value),
        None => state.linkers.link_tvdb_series_by_imdb(imdb_id).await?,
    };
    let Some(tvdb_series_id) = tvdb_series_id else {
        return Ok(TorrentioTargetExpansion::default());
    };
    if external_ids.tvdb_series.is_none() {
        external_ids.tvdb_series = Some(tvdb_series_id.clone());
    }

    let release_cutoff = torrentio_release_cutoff();
    let seasons = state
        .linkers
        .fetch_tvdb_series_seasons(&tvdb_series_id)
        .await?;
    let mut expansion = TorrentioTargetExpansion::default();
    for season in seasons {
        let Some(season_number) = tvdb_season_number(&season).filter(|value| *value > 0) else {
            continue;
        };
        for episode in state
            .linkers
            .fetch_tvdb_season_episodes(&tvdb_series_id, season_number)
            .await?
        {
            let Some(episode_number) = episode.episode_number.filter(|value| *value > 0) else {
                continue;
            };
            let Some(aired_at) = episode_air_date(&episode.raw) else {
                continue;
            };
            if aired_at > release_cutoff {
                expansion.note_future_air(aired_at);
                continue;
            }
            expansion.targets.push(build_episode_target(
                intent,
                MediaType::Series,
                external_ids.clone(),
                season_number,
                episode_number,
                episode.absolute_number,
                Some(aired_at),
                episode.title.as_deref(),
            ));
        }
    }
    Ok(expansion)
}

async fn torrentio_episode_targets_from_cinemeta(
    state: &AppState,
    intent: &ManagedIngestIntent,
    search_media_type: MediaType,
) -> Result<TorrentioTargetExpansion> {
    let external_ids = intent.external_ids.clone().unwrap_or_default();
    if external_ids
        .imdb
        .as_deref()
        .and_then(non_empty_str)
        .is_none()
    {
        return Ok(TorrentioTargetExpansion::default());
    }
    let identity = MediaIdentity {
        r#type: MediaType::Series,
        external_ids: external_ids.clone(),
        title: intent.title.clone(),
        year: intent.year,
        season: None,
        episode: None,
    };
    let Some(metadata) = state.metadata.fetch_metadata(&identity).await? else {
        return Ok(TorrentioTargetExpansion::default());
    };
    let Some(videos) = metadata
        .metadata_json
        .get("videos")
        .and_then(Value::as_array)
    else {
        return Ok(TorrentioTargetExpansion::default());
    };
    let release_cutoff = torrentio_release_cutoff();
    let mut expansion = TorrentioTargetExpansion::default();
    for video in videos {
        let Some(season_number) = json_i32(
            video
                .get("season")
                .or_else(|| video.get("seasonNumber"))
                .or_else(|| video.get("season_number")),
        )
        .filter(|value| *value > 0) else {
            continue;
        };
        let Some(episode_number) = json_i32(
            video
                .get("episode")
                .or_else(|| video.get("episodeNumber"))
                .or_else(|| video.get("episode_number")),
        )
        .filter(|value| *value > 0) else {
            continue;
        };
        let Some(aired_at) = episode_air_date(video) else {
            continue;
        };
        if aired_at > release_cutoff {
            expansion.note_future_air(aired_at);
            continue;
        }
        let title = video.get("title").and_then(Value::as_str);
        expansion.targets.push(build_episode_target(
            intent,
            search_media_type,
            external_ids.clone(),
            season_number,
            episode_number,
            json_i32(
                video
                    .get("absoluteEpisodeNumber")
                    .or_else(|| video.get("absolute_number"))
                    .or_else(|| video.get("absoluteNumber")),
            ),
            Some(aired_at),
            title,
        ));
    }
    Ok(expansion)
}

async fn torrentio_episode_targets_from_anizip(
    state: &AppState,
    intent: &ManagedIngestIntent,
) -> Result<TorrentioTargetExpansion> {
    let base_ids = intent.external_ids.clone().unwrap_or_default();
    let Some(anilist_id) = base_ids.anilist.as_deref().and_then(non_empty_str) else {
        return Ok(TorrentioTargetExpansion::default());
    };
    let Some(seed_mapping) = state.linkers.fetch_anizip_mapping(anilist_id).await? else {
        return Ok(TorrentioTargetExpansion::default());
    };

    let mut mappings = vec![(anilist_id.to_string(), seed_mapping)];
    if let Some(seed_season) = anizip_mapping_seed_season(&mappings[0].1) {
        match resolve_anilist_season_chain(
            Some(&state.settings.classifier),
            seed_season,
            anilist_id,
            1.0,
        )
        .await
        {
            Ok(chain) => {
                for entry in chain {
                    if mappings
                        .iter()
                        .any(|(known_id, _)| known_id == &entry.anilist_id)
                    {
                        continue;
                    }
                    match state.linkers.fetch_anizip_mapping(&entry.anilist_id).await {
                        Ok(Some(mapping)) => mappings.push((entry.anilist_id, mapping)),
                        Ok(None) => {}
                        Err(err) => warn!(
                            intent_id = %intent.intent_id,
                            anilist_id = %entry.anilist_id,
                            season = entry.season_number,
                            "ani.zip season-chain mapping fetch failed: {err}"
                        ),
                    }
                }
            }
            Err(err) => warn!(
                intent_id = %intent.intent_id,
                anilist_id,
                "AniList season-chain expansion failed: {err}"
            ),
        }
    }

    let release_cutoff = torrentio_release_cutoff();
    let mut expansion = TorrentioTargetExpansion::default();
    for (season_anilist_id, mapping) in mappings {
        let mut season_ids = base_ids.clone();
        season_ids.anilist = Some(season_anilist_id.clone());
        let anime_state = match load_anilist_target_state(state, intent, &season_ids).await {
            Ok(state) => state,
            Err(err) => {
                warn!(
                    intent_id = %intent.intent_id,
                    anilist_id = %season_anilist_id,
                    "AniList season state fetch failed: {err}"
                );
                None
            }
        };
        append_torrentio_episode_targets_from_anizip_mapping(
            &mut expansion,
            intent,
            &base_ids,
            &season_anilist_id,
            mapping,
            anime_state.as_ref(),
            release_cutoff,
        );
    }
    Ok(expansion)
}

fn append_torrentio_episode_targets_from_anizip_mapping(
    expansion: &mut TorrentioTargetExpansion,
    intent: &ManagedIngestIntent,
    base_ids: &ExternalIds,
    season_anilist_id: &str,
    mapping: AniZipMapping,
    anime_state: Option<&AniListTargetState>,
    release_cutoff: DateTime<Utc>,
) {
    let multi_season_mapping = anizip_mapping_has_multiple_seasons(&mapping);
    let mut external_ids = merge_external_ids(base_ids.clone(), mapping.ids);
    external_ids.anilist = Some(season_anilist_id.to_string());
    if external_ids
        .imdb
        .as_deref()
        .and_then(non_empty_str)
        .is_none()
    {
        return;
    }
    for episode in mapping.episodes {
        let Some(season_number) = episode.season_number.filter(|value| *value > 0) else {
            continue;
        };
        let Some(episode_number) = episode.episode_number.filter(|value| *value > 0) else {
            continue;
        };
        let anilist_episode_number =
            anizip_episode_number_for_anilist_state(&episode, multi_season_mapping);
        let release_at =
            anime_episode_release_at(&episode.raw, anilist_episode_number, anime_state);
        if release_at
            .map(|aired_at| aired_at > release_cutoff)
            .unwrap_or(false)
        {
            if let Some(aired_at) = release_at {
                expansion.note_future_air(aired_at);
            }
            continue;
        }
        if release_at.is_none()
            && !anime_episode_is_released(
                &episode.raw,
                anilist_episode_number,
                anime_state,
                release_cutoff,
            )
        {
            continue;
        }
        expansion.targets.push(build_episode_target(
            intent,
            MediaType::Series,
            external_ids.clone(),
            season_number,
            episode_number,
            episode.absolute_episode_number,
            release_at,
            episode.title.as_deref(),
        ));
    }
}

#[derive(Debug, Clone, Default)]
struct AniListTargetState {
    status: Option<String>,
    episodes: Option<i32>,
    next_airing_episode: Option<i32>,
    next_airing_at: Option<DateTime<Utc>>,
}

async fn load_anilist_target_state(
    state: &AppState,
    intent: &ManagedIngestIntent,
    external_ids: &ExternalIds,
) -> Result<Option<AniListTargetState>> {
    let identity = MediaIdentity {
        r#type: MediaType::Anime,
        external_ids: external_ids.clone(),
        title: intent.title.clone(),
        year: intent.year,
        season: None,
        episode: None,
    };
    let Some(metadata) = state.metadata.fetch_metadata(&identity).await? else {
        return Ok(None);
    };
    Ok(Some(AniListTargetState {
        status: metadata
            .metadata_json
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_string),
        episodes: json_i32(metadata.metadata_json.get("episodes")),
        next_airing_episode: metadata
            .metadata_json
            .get("nextAiringEpisode")
            .and_then(|value| json_i32(value.get("episode"))),
        next_airing_at: metadata
            .metadata_json
            .get("nextAiringEpisode")
            .and_then(episode_air_date),
    }))
}

fn anime_episode_release_at(
    raw: &Value,
    absolute_episode_number: Option<i32>,
    state: Option<&AniListTargetState>,
) -> Option<DateTime<Utc>> {
    episode_air_date(raw).or_else(|| {
        let state = state?;
        let absolute_episode_number = absolute_episode_number.filter(|value| *value > 0)?;
        let next_airing_episode = state.next_airing_episode?;
        (absolute_episode_number == next_airing_episode)
            .then_some(())
            .and(state.next_airing_at)
    })
}

fn anime_episode_is_released(
    raw: &Value,
    absolute_episode_number: Option<i32>,
    state: Option<&AniListTargetState>,
    release_cutoff: DateTime<Utc>,
) -> bool {
    if let Some(aired_at) = episode_air_date(raw) {
        return aired_at <= release_cutoff;
    }

    let Some(state) = state else {
        return false;
    };
    let status = state
        .status
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();
    let Some(absolute_episode_number) = absolute_episode_number.filter(|value| *value > 0) else {
        return false;
    };
    if matches!(status.as_str(), "FINISHED" | "COMPLETED") {
        return state
            .episodes
            .map(|count| absolute_episode_number <= count)
            .unwrap_or(true);
    }
    if status == "RELEASING" {
        return state
            .next_airing_episode
            .map(|next| absolute_episode_number < next)
            .unwrap_or(false);
    }
    false
}

fn build_episode_target(
    intent: &ManagedIngestIntent,
    search_media_type: MediaType,
    external_ids: ExternalIds,
    season_number: i32,
    episode_number: i32,
    absolute_episode_number: Option<i32>,
    aired_at: Option<DateTime<Utc>>,
    episode_title: Option<&str>,
) -> TorrentioAcquisitionTarget {
    let episode_title = episode_title.and_then(non_empty_str).unwrap_or("Episode");
    TorrentioAcquisitionTarget {
        target_key: format!("s{season_number:02}e{episode_number:02}"),
        title: format!(
            "{} S{:02}E{:02} - {}",
            intent.title, season_number, episode_number, episode_title
        ),
        external_ids,
        search_media_type,
        season_number: Some(season_number),
        episode_number: Some(episode_number),
        absolute_episode_number,
        aired_at,
    }
}

fn dedupe_torrentio_targets(
    targets: Vec<TorrentioAcquisitionTarget>,
) -> Vec<TorrentioAcquisitionTarget> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for target in targets {
        if seen.insert(target.target_key.clone()) {
            out.push(target);
        }
    }
    out.sort_by(|left, right| {
        left.season_number
            .cmp(&right.season_number)
            .then_with(|| left.episode_number.cmp(&right.episode_number))
            .then_with(|| left.target_key.cmp(&right.target_key))
    });
    out
}

fn anizip_mapping_seed_season(mapping: &AniZipMapping) -> Option<i32> {
    let mut counts: HashMap<i32, usize> = HashMap::new();
    for episode in &mapping.episodes {
        let Some(season_number) = episode.season_number.filter(|value| *value > 0) else {
            continue;
        };
        *counts.entry(season_number).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by(|(left_season, left_count), (right_season, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_season.cmp(left_season))
        })
        .map(|(season_number, _)| season_number)
}

fn anizip_mapping_has_multiple_seasons(mapping: &AniZipMapping) -> bool {
    let mut seasons = HashSet::new();
    for episode in &mapping.episodes {
        let Some(season_number) = episode.season_number.filter(|value| *value > 0) else {
            continue;
        };
        seasons.insert(season_number);
        if seasons.len() > 1 {
            return true;
        }
    }
    false
}

fn anizip_episode_number_for_anilist_state(
    episode: &AniZipEpisodeRecord,
    multi_season_mapping: bool,
) -> Option<i32> {
    if multi_season_mapping {
        episode
            .absolute_episode_number
            .filter(|value| *value > 0)
            .or_else(|| episode.episode_number.filter(|value| *value > 0))
    } else {
        episode
            .episode_number
            .filter(|value| *value > 0)
            .or_else(|| episode.absolute_episode_number.filter(|value| *value > 0))
    }
}

fn torrentio_release_cutoff() -> DateTime<Utc> {
    Utc::now() - chrono::Duration::seconds(TORRENTIO_ACQUISITION_RELEASE_DELAY_SECONDS.max(0))
}

fn tvdb_season_number(value: &Value) -> Option<i32> {
    json_i32(
        value
            .get("number")
            .or_else(|| value.get("seasonNumber"))
            .or_else(|| value.get("season_number")),
    )
}

fn episode_air_date(value: &Value) -> Option<DateTime<Utc>> {
    for key in [
        "aired",
        "airDate",
        "air_date",
        "firstAired",
        "first_aired",
        "released",
        "releaseDate",
        "release_date",
        "airingAt",
        "airing_at",
        "startDate",
        "start_date",
    ] {
        let Some(raw) = value.get(key) else {
            continue;
        };
        if let Some(parsed) = parse_air_date_value(raw) {
            return Some(parsed);
        }
    }
    None
}

fn parse_air_date_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        return parse_air_date_text(text);
    }
    if let Some(timestamp) = value.as_i64() {
        return DateTime::<Utc>::from_timestamp(timestamp, 0);
    }
    if let Some(object) = value.as_object() {
        let year = object
            .get("year")
            .and_then(Value::as_i64)
            .map(|value| value as i32)?;
        let month = object
            .get("month")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(1);
        let day = object
            .get("day")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(1);
        return NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|date| DateTime::<Utc>::from_naive_utc_and_offset(date, Utc));
    }
    None
}

fn parse_air_date_text(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Some(parsed.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc));
        }
    }
    for format in ["%Y-%m-%d", "%Y/%m/%d"] {
        if let Ok(parsed) = NaiveDate::parse_from_str(value, format) {
            return parsed
                .and_hms_opt(0, 0, 0)
                .map(|date| DateTime::<Utc>::from_naive_utc_and_offset(date, Utc));
        }
    }
    None
}

fn json_i32(value: Option<&Value>) -> Option<i32> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return i32::try_from(number).ok();
    }
    if let Some(number) = value.as_u64() {
        return i32::try_from(number).ok();
    }
    value.as_str()?.trim().parse::<i32>().ok()
}

fn merge_external_ids(mut base: ExternalIds, incoming: ExternalIds) -> ExternalIds {
    base.imdb = base.imdb.or(incoming.imdb);
    base.tmdb = base.tmdb.or(incoming.tmdb);
    base.tvdb = base.tvdb.or(incoming.tvdb);
    base.tvdb_series = base.tvdb_series.or(incoming.tvdb_series);
    base.tvdb_movie = base.tvdb_movie.or(incoming.tvdb_movie);
    base.anilist = base.anilist.or(incoming.anilist);
    base.anidb = base.anidb.or(incoming.anidb);
    base.mal = base.mal.or(incoming.mal);
    base.kitsu = base.kitsu.or(incoming.kitsu);
    base
}

impl TorrentioRoutePolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::DebridFirst => "debrid_first",
            Self::TorrentFirst => "torrent_first",
            Self::AskEachTime => "ask_each_time",
        }
    }

    fn route_order(self) -> &'static [&'static str] {
        match self {
            Self::DebridFirst | Self::AskEachTime => {
                &[DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID]
            }
            Self::TorrentFirst => &[TORRENT_DEFAULT_LOGICAL_ID, DEBRID_DEFAULT_LOGICAL_ID],
        }
    }
}

fn choose_torrentio_submission(
    response: &TorrentioCandidateSearchResponse,
) -> Option<TorrentioSubmissionChoice> {
    let route_policy = parse_torrentio_route_policy(&response.route_policy);
    if route_policy == TorrentioRoutePolicy::AskEachTime {
        return None;
    }
    let mut best = None::<TorrentioSubmissionChoice>;
    for candidate in &response.candidates {
        let Some(route) = choose_torrentio_route(candidate, &response.route_options, route_policy)
        else {
            continue;
        };
        let score = if candidate.score == 0 {
            torrentio_candidate_score(candidate, Some(&route)).0
        } else {
            candidate.score
        };
        let choice = TorrentioSubmissionChoice {
            candidate: candidate.clone(),
            route,
            score,
        };
        if best
            .as_ref()
            .map(|current| choice.score > current.score)
            .unwrap_or(true)
        {
            best = Some(choice);
        }
    }
    best
}

fn choose_torrentio_route(
    candidate: &TorrentioCandidate,
    route_options: &[TorrentioRouteOption],
    route_policy: TorrentioRoutePolicy,
) -> Option<TorrentioRouteOption> {
    route_policy.route_order().iter().find_map(|logical_id| {
        let supports_route = candidate
            .route_logical_ids
            .iter()
            .any(|route_id| route_id == logical_id);
        if !supports_route {
            return None;
        }
        route_options
            .iter()
            .find(|route| {
                route.logical_id == *logical_id && route.available && route.blocker.is_none()
            })
            .cloned()
    })
}

fn annotate_torrentio_candidate_score(
    candidate: &mut TorrentioCandidate,
    route_options: &[TorrentioRouteOption],
    route_policy: TorrentioRoutePolicy,
) {
    let route = choose_torrentio_route(candidate, route_options, route_policy);
    let (score, badges) = torrentio_candidate_score(candidate, route.as_ref());
    candidate.score = score;
    candidate.score_badges = badges;
}

fn torrentio_candidate_score(
    candidate: &TorrentioCandidate,
    route: Option<&TorrentioRouteOption>,
) -> (i64, Vec<TorrentioScoreBadge>) {
    let mut score = 0;
    let mut badges = Vec::new();

    if let Some(route) = route {
        let value = match route.logical_id.as_str() {
            DEBRID_DEFAULT_LOGICAL_ID => 1_000_000,
            TORRENT_DEFAULT_LOGICAL_ID => 500_000,
            _ => 0,
        };
        if value > 0 {
            badges.push(score_badge(route_label(&route.logical_id), value, "route"));
            score += value;
        }
    }

    if let Some(quality) = candidate.quality.as_deref() {
        let value = quality_score(quality);
        if value > 0 {
            badges.push(score_badge(quality, value, "quality"));
            score += value;
        }
    }

    if candidate.cached_debrid == Some(true) {
        let value = 50_000;
        badges.push(score_badge("Cached debrid", value, "cache"));
        score += value;
    }

    let seed_score = candidate.seeders.unwrap_or(0).min(10_000) as i64;
    if seed_score > 0 {
        badges.push(score_badge(
            &format!("{} seeders", candidate.seeders.unwrap_or_default()),
            seed_score,
            "seeders",
        ));
        score += seed_score;
    }

    if let Some(language) = candidate.language.as_deref() {
        let value = 1_000;
        badges.push(score_badge(language, value, "language"));
        score += value;
    }

    let rank_penalty = candidate.rank as i64;
    if rank_penalty > 0 {
        badges.push(score_badge(
            &format!("Source rank {}", candidate.rank),
            -rank_penalty,
            "rank",
        ));
        score -= rank_penalty;
    }

    (score, badges)
}

fn score_badge(label: &str, value: i64, kind: &str) -> TorrentioScoreBadge {
    TorrentioScoreBadge {
        label: label.to_string(),
        value,
        kind: kind.to_string(),
    }
}

fn quality_score(quality: &str) -> i64 {
    match normalize_quality(quality).as_deref() {
        Some("2160p") => 30_000,
        Some("1080p") => 20_000,
        Some("720p") => 10_000,
        _ => 0,
    }
}

fn torrentio_route_blocker_message(response: &TorrentioCandidateSearchResponse) -> Option<String> {
    if parse_torrentio_route_policy(&response.route_policy) == TorrentioRoutePolicy::AskEachTime {
        return Some(
            "Torrentio route policy is set to ask each time; monitored acquisition needs an automatic route policy."
                .to_string(),
        );
    }
    response
        .route_options
        .iter()
        .find(|route| route.logical_id == DEBRID_DEFAULT_LOGICAL_ID && route.blocker.is_some())
        .and_then(|route| route.blocker.clone())
        .or_else(|| {
            response
                .route_options
                .iter()
                .find(|route| route.blocker.is_some())
                .and_then(|route| route.blocker.clone())
        })
}

fn torrentio_job_is_ready_for_submission(
    job: &TorrentioAcquisitionJob,
    now: DateTime<Utc>,
) -> bool {
    if job.status == TORRENTIO_JOB_SUBMITTED {
        return false;
    }
    if !matches!(
        job.status.as_str(),
        TORRENTIO_JOB_PENDING | TORRENTIO_JOB_BLOCKED | TORRENTIO_JOB_FAILED
    ) {
        return false;
    }
    job.next_search_at
        .map(|next_search_at| next_search_at <= now)
        .unwrap_or(true)
}

fn initial_torrentio_candidate_search_at(
    target: &TorrentioAcquisitionTarget,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    let Some(release_at) = torrentio_target_release_at(target) else {
        return now;
    };
    if release_at <= now {
        return now;
    }
    release_at
        + chrono::Duration::seconds(deterministic_jitter_seconds(
            &target.target_key,
            TORRENTIO_CANDIDATE_INITIAL_JITTER_SECONDS,
        ))
}

fn next_torrentio_candidate_search_at(
    target: &TorrentioAcquisitionTarget,
    completed_search_attempts: i64,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    let seed = format!("{}:{completed_search_attempts}", target.target_key);
    if let Some(release_at) = torrentio_target_release_at(target) {
        if release_at > now {
            return release_at
                + chrono::Duration::seconds(deterministic_jitter_seconds(
                    &seed,
                    TORRENTIO_CANDIDATE_INITIAL_JITTER_SECONDS,
                ));
        }
        let age_seconds = now.signed_duration_since(release_at).num_seconds();
        let interval_seconds = if age_seconds <= TORRENTIO_CANDIDATE_HOT_WINDOW_SECONDS {
            TORRENTIO_CANDIDATE_HOT_RETRY_SECONDS
        } else if age_seconds <= TORRENTIO_CANDIDATE_WARM_WINDOW_SECONDS {
            TORRENTIO_CANDIDATE_WARM_RETRY_SECONDS
        } else if completed_search_attempts < 4 {
            TORRENTIO_CANDIDATE_DISCOVERY_MIN_SECONDS
        } else {
            TORRENTIO_CANDIDATE_COLD_RETRY_SECONDS
        };
        return now
            + chrono::Duration::seconds(interval_seconds)
            + chrono::Duration::seconds(deterministic_jitter_seconds(
                &seed,
                TORRENTIO_CANDIDATE_DISCOVERY_JITTER_SECONDS,
            ));
    }

    now + chrono::Duration::seconds(TORRENTIO_CANDIDATE_DISCOVERY_MIN_SECONDS)
        + chrono::Duration::seconds(deterministic_jitter_seconds(
            &seed,
            TORRENTIO_CANDIDATE_DISCOVERY_JITTER_SECONDS,
        ))
}

fn torrentio_target_release_at(target: &TorrentioAcquisitionTarget) -> Option<DateTime<Utc>> {
    target.aired_at.map(|aired_at| {
        aired_at + chrono::Duration::seconds(TORRENTIO_ACQUISITION_RELEASE_DELAY_SECONDS.max(0))
    })
}

fn next_torrentio_metadata_refresh_at(
    intent_id: Uuid,
    now: DateTime<Utc>,
    next_air_at: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    let seed = intent_id.to_string();
    let periodic = now
        + chrono::Duration::seconds(TORRENTIO_METADATA_REFRESH_MIN_SECONDS)
        + chrono::Duration::seconds(deterministic_jitter_seconds(
            &seed,
            TORRENTIO_METADATA_REFRESH_JITTER_SECONDS,
        ));
    let Some(next_air_at) = next_air_at else {
        return periodic;
    };
    let air_window = next_air_at
        + chrono::Duration::seconds(TORRENTIO_ACQUISITION_RELEASE_DELAY_SECONDS.max(0))
        + chrono::Duration::seconds(deterministic_jitter_seconds(
            &format!("{seed}:air"),
            TORRENTIO_METADATA_AIR_WINDOW_JITTER_SECONDS,
        ));
    if air_window < periodic {
        air_window.max(now)
    } else {
        periodic
    }
}

fn deterministic_jitter_seconds(seed: &str, max_seconds: i64) -> i64 {
    if max_seconds <= 0 {
        return 0;
    }
    let mut hash = 14_695_981_039_346_656_037u64;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    (hash % (max_seconds as u64 + 1)) as i64
}

fn datetime_to_db(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

async fn upsert_torrentio_acquisition_subscription(
    pool: &AnyPool,
    intent: &ManagedIngestIntent,
    now: DateTime<Utc>,
) -> Result<TorrentioSubscriptionSchedule> {
    let next_refresh_at = datetime_to_db(now);
    sqlx::query::<sqlx::Any>(
        "INSERT INTO torrentio_acquisition_subscriptions (
            intent_id,
            media_type,
            next_metadata_refresh_at
        ) VALUES (?, ?, ?)
         ON CONFLICT(intent_id) DO UPDATE
         SET media_type = excluded.media_type,
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(intent.intent_id.to_string())
    .bind(intent.media_type.as_str())
    .bind(next_refresh_at)
    .execute(pool)
    .await?;

    get_torrentio_acquisition_subscription(pool, intent.intent_id).await
}

async fn get_torrentio_acquisition_subscription(
    pool: &AnyPool,
    intent_id: Uuid,
) -> Result<TorrentioSubscriptionSchedule> {
    let row = sqlx::query(
        "SELECT
            CAST(next_metadata_refresh_at AS TEXT) AS next_metadata_refresh_at
         FROM torrentio_acquisition_subscriptions
         WHERE intent_id = ?
         LIMIT 1",
    )
    .bind(intent_id.to_string())
    .fetch_one(pool)
    .await?;
    Ok(TorrentioSubscriptionSchedule {
        next_metadata_refresh_at: row_get_optional_datetime(&row, "next_metadata_refresh_at")?,
    })
}

fn torrentio_metadata_refresh_due(
    schedule: &TorrentioSubscriptionSchedule,
    now: DateTime<Utc>,
) -> bool {
    schedule
        .next_metadata_refresh_at
        .map(|next_refresh_at| next_refresh_at <= now)
        .unwrap_or(true)
}

async fn schedule_next_torrentio_metadata_refresh(
    pool: &AnyPool,
    intent_id: Uuid,
    now: DateTime<Utc>,
    next_air_at: Option<DateTime<Utc>>,
    expanded_target_count: usize,
) -> Result<()> {
    let next_refresh_at = next_torrentio_metadata_refresh_at(intent_id, now, next_air_at);
    sqlx::query::<sqlx::Any>(
        "UPDATE torrentio_acquisition_subscriptions
         SET last_metadata_refresh_at = ?,
             next_metadata_refresh_at = ?,
             last_expanded_target_count = ?,
             next_air_at = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE intent_id = ?",
    )
    .bind(datetime_to_db(now))
    .bind(datetime_to_db(next_refresh_at))
    .bind(expanded_target_count as i64)
    .bind(next_air_at.map(datetime_to_db))
    .bind(intent_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

fn torrentio_target_from_job(job: &TorrentioAcquisitionJob) -> TorrentioAcquisitionTarget {
    TorrentioAcquisitionTarget {
        target_key: job.target_key.clone(),
        title: job.title.clone(),
        external_ids: job.external_ids.clone(),
        search_media_type: job.search_media_type,
        season_number: job.season_number,
        episode_number: job.episode_number,
        absolute_episode_number: job.absolute_episode_number,
        aired_at: job.aired_at,
    }
}

async fn upsert_torrentio_acquisition_job(
    pool: &AnyPool,
    intent: &ManagedIngestIntent,
    target: &TorrentioAcquisitionTarget,
    now: DateTime<Utc>,
) -> Result<TorrentioAcquisitionJob> {
    let external_ids_json =
        Some(serde_json::to_string(&target.external_ids).context("serializing external ids")?);
    let initial_next_search_at = initial_torrentio_candidate_search_at(target, now);
    sqlx::query::<sqlx::Any>(
        "INSERT INTO torrentio_acquisition_jobs (
            job_id,
            intent_id,
            media_type,
            search_media_type,
            target_key,
            title,
            year,
            external_ids_json,
            season_number,
            episode_number,
            absolute_episode_number,
            aired_at,
            route_policy,
            status,
            next_search_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'debrid_first', 'pending', ?)
         ON CONFLICT(intent_id, target_key) DO UPDATE
         SET search_media_type = excluded.search_media_type,
             title = excluded.title,
             year = excluded.year,
             external_ids_json = excluded.external_ids_json,
             season_number = excluded.season_number,
             episode_number = excluded.episode_number,
             absolute_episode_number = excluded.absolute_episode_number,
             aired_at = excluded.aired_at,
             updated_at = CURRENT_TIMESTAMP
         WHERE torrentio_acquisition_jobs.status != 'submitted'",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(intent.intent_id.to_string())
    .bind(intent.media_type.as_str())
    .bind(target.search_media_type.as_str())
    .bind(&target.target_key)
    .bind(&target.title)
    .bind(intent.year)
    .bind(external_ids_json.as_deref())
    .bind(target.season_number)
    .bind(target.episode_number)
    .bind(target.absolute_episode_number)
    .bind(target.aired_at.map(datetime_to_db))
    .bind(datetime_to_db(initial_next_search_at))
    .execute(pool)
    .await?;

    get_torrentio_acquisition_job(pool, intent.intent_id, &target.target_key)
        .await?
        .ok_or_else(|| anyhow!("Torrentio acquisition job was not persisted"))
}

async fn list_ready_torrentio_acquisition_jobs_for_intent(
    pool: &AnyPool,
    intent_id: Uuid,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<Vec<TorrentioAcquisitionJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "SELECT
            job_id,
            intent_id,
            media_type,
            COALESCE(search_media_type, media_type) AS search_media_type,
            target_key,
            title,
            year,
            CAST(external_ids_json AS TEXT) AS external_ids_json,
            season_number,
            episode_number,
            absolute_episode_number,
            CAST(aired_at AS TEXT) AS aired_at,
            status,
            CAST(route_logical_id AS TEXT) AS route_logical_id,
            CAST(candidate_id AS TEXT) AS candidate_id,
            CAST(candidate_title AS TEXT) AS candidate_title,
            CAST(candidate_source AS TEXT) AS candidate_source,
            CAST(candidate_source_kind AS TEXT) AS candidate_source_kind,
            CAST(candidate_info_hash AS TEXT) AS candidate_info_hash,
            CAST(candidate_file_index AS INTEGER) AS candidate_file_index,
            CAST(candidate_quality AS TEXT) AS candidate_quality,
            CAST(candidate_size_bytes AS INTEGER) AS candidate_size_bytes,
            CAST(candidate_seeders AS INTEGER) AS candidate_seeders,
            CAST(candidate_language AS TEXT) AS candidate_language,
            CAST(candidate_cached_debrid AS INTEGER) AS candidate_cached_debrid,
            CAST(candidate_score AS INTEGER) AS candidate_score,
            CAST(candidate_score_badges_json AS TEXT) AS candidate_score_badges_json,
            CAST(candidate_rank AS INTEGER) AS candidate_rank,
            CAST(download_id AS TEXT) AS download_id,
            CAST(last_error AS TEXT) AS last_error,
            CAST(import_event_id AS TEXT) AS import_event_id,
            CAST(imported_at AS TEXT) AS imported_at,
            CAST(import_error AS TEXT) AS import_error,
            CAST(next_search_at AS TEXT) AS next_search_at,
            CAST(search_attempts AS INTEGER) AS search_attempts,
            CAST(updated_at AS TEXT) AS updated_at
         FROM torrentio_acquisition_jobs
         WHERE intent_id = ?
           AND status IN ('pending', 'blocked', 'failed')
           AND (next_search_at IS NULL OR next_search_at <= ?)
         ORDER BY COALESCE(next_search_at, created_at) ASC, created_at ASC
         LIMIT ?",
    )
    .bind(intent_id.to_string())
    .bind(datetime_to_db(now))
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| map_torrentio_acquisition_job(&row))
        .collect()
}

async fn get_torrentio_acquisition_job(
    pool: &AnyPool,
    intent_id: Uuid,
    target_key: &str,
) -> Result<Option<TorrentioAcquisitionJob>> {
    let row = sqlx::query(
        "SELECT
            job_id,
            intent_id,
            media_type,
            COALESCE(search_media_type, media_type) AS search_media_type,
            target_key,
            title,
            year,
            CAST(external_ids_json AS TEXT) AS external_ids_json,
            season_number,
            episode_number,
            absolute_episode_number,
            CAST(aired_at AS TEXT) AS aired_at,
            status,
            CAST(route_logical_id AS TEXT) AS route_logical_id,
            CAST(candidate_id AS TEXT) AS candidate_id,
            CAST(candidate_title AS TEXT) AS candidate_title,
            CAST(candidate_source AS TEXT) AS candidate_source,
            CAST(candidate_source_kind AS TEXT) AS candidate_source_kind,
            CAST(candidate_info_hash AS TEXT) AS candidate_info_hash,
            CAST(candidate_file_index AS INTEGER) AS candidate_file_index,
            CAST(candidate_quality AS TEXT) AS candidate_quality,
            CAST(candidate_size_bytes AS INTEGER) AS candidate_size_bytes,
            CAST(candidate_seeders AS INTEGER) AS candidate_seeders,
            CAST(candidate_language AS TEXT) AS candidate_language,
            CAST(candidate_cached_debrid AS INTEGER) AS candidate_cached_debrid,
            CAST(candidate_score AS INTEGER) AS candidate_score,
            CAST(candidate_score_badges_json AS TEXT) AS candidate_score_badges_json,
            CAST(candidate_rank AS INTEGER) AS candidate_rank,
            CAST(download_id AS TEXT) AS download_id,
            CAST(last_error AS TEXT) AS last_error,
            CAST(import_event_id AS TEXT) AS import_event_id,
            CAST(imported_at AS TEXT) AS imported_at,
            CAST(import_error AS TEXT) AS import_error,
            CAST(next_search_at AS TEXT) AS next_search_at,
            CAST(search_attempts AS INTEGER) AS search_attempts,
            CAST(updated_at AS TEXT) AS updated_at
         FROM torrentio_acquisition_jobs
         WHERE intent_id = ? AND target_key = ?
         LIMIT 1",
    )
    .bind(intent_id.to_string())
    .bind(target_key)
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_torrentio_acquisition_job(&row))
        .transpose()
}

async fn mark_torrentio_acquisition_job_searched(
    pool: &AnyPool,
    job_id: Uuid,
    provider_id: Uuid,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE torrentio_acquisition_jobs
         SET source_provider_id = ?,
             last_search_at = CURRENT_TIMESTAMP,
             search_attempts = search_attempts + 1,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(provider_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_torrentio_acquisition_job_submitted(
    pool: &AnyPool,
    job_id: Uuid,
    choice: &TorrentioSubmissionChoice,
    download_id: Option<&str>,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE torrentio_acquisition_jobs
         SET status = ?,
             route_logical_id = ?,
             candidate_id = ?,
             candidate_title = ?,
             candidate_source = ?,
             candidate_source_kind = ?,
             candidate_info_hash = ?,
             candidate_file_index = ?,
             candidate_quality = ?,
             candidate_size_bytes = ?,
             candidate_seeders = ?,
             candidate_language = ?,
             candidate_cached_debrid = ?,
             candidate_score = ?,
             candidate_score_badges_json = ?,
             candidate_rank = ?,
             download_id = ?,
             last_error = NULL,
             import_error = NULL,
             last_submitted_at = CURRENT_TIMESTAMP,
             next_search_at = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(TORRENTIO_JOB_SUBMITTED)
    .bind(&choice.route.logical_id)
    .bind(&choice.candidate.id)
    .bind(&choice.candidate.title)
    .bind(&choice.candidate.source)
    .bind(&choice.candidate.source_kind)
    .bind(choice.candidate.info_hash.as_deref())
    .bind(choice.candidate.file_index)
    .bind(choice.candidate.quality.as_deref())
    .bind(choice.candidate.size_bytes.and_then(u64_to_i64))
    .bind(choice.candidate.seeders.map(i64::from))
    .bind(choice.candidate.language.as_deref())
    .bind(choice.candidate.cached_debrid.map(bool_to_i64))
    .bind(choice.score)
    .bind(serde_json::to_string(&choice.candidate.score_badges)?)
    .bind(choice.candidate.rank as i64)
    .bind(download_id)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_torrentio_acquisition_job_blocked(
    pool: &AnyPool,
    job_id: Uuid,
    message: &str,
    next_search_at: DateTime<Utc>,
) -> Result<()> {
    mark_torrentio_acquisition_job_status(
        pool,
        job_id,
        TORRENTIO_JOB_BLOCKED,
        message,
        next_search_at,
    )
    .await
}

async fn mark_torrentio_acquisition_job_failed(
    pool: &AnyPool,
    job_id: Uuid,
    message: &str,
    next_search_at: DateTime<Utc>,
) -> Result<()> {
    mark_torrentio_acquisition_job_status(
        pool,
        job_id,
        TORRENTIO_JOB_FAILED,
        message,
        next_search_at,
    )
    .await
}

async fn mark_torrentio_acquisition_job_status(
    pool: &AnyPool,
    job_id: Uuid,
    status: &str,
    message: &str,
    next_search_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE torrentio_acquisition_jobs
         SET status = ?,
             last_error = ?,
             next_search_at = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(status)
    .bind(message)
    .bind(datetime_to_db(next_search_at))
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_torrentio_acquisition_job_imported(
    pool: &AnyPool,
    job_id: Uuid,
    event_id: Uuid,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE torrentio_acquisition_jobs
         SET import_event_id = ?,
             imported_at = CURRENT_TIMESTAMP,
             import_error = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(event_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_torrentio_acquisition_job_import_failed(
    pool: &AnyPool,
    job_id: Uuid,
    message: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE torrentio_acquisition_jobs
         SET import_error = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(message)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

fn map_torrentio_acquisition_job(row: &AnyRow) -> Result<TorrentioAcquisitionJob> {
    let job_id_raw: String = row.try_get("job_id")?;
    let intent_id_raw: String = row.try_get("intent_id")?;
    let search_media_type_raw: String = row.try_get("search_media_type")?;
    let external_ids_json = row_get_optional_string(row, "external_ids_json")?;
    let external_ids = external_ids_json
        .as_deref()
        .map(serde_json::from_str::<ExternalIds>)
        .transpose()
        .context("parsing torrentio_acquisition_jobs.external_ids_json")?
        .unwrap_or_default();
    let score_badges_json = row_get_optional_string(row, "candidate_score_badges_json")?;
    let candidate_score_badges = score_badges_json
        .as_deref()
        .map(serde_json::from_str::<Vec<TorrentioScoreBadge>>)
        .transpose()
        .context("parsing torrentio_acquisition_jobs.candidate_score_badges_json")?
        .unwrap_or_default();
    let import_event_id = row_get_optional_string(row, "import_event_id")?
        .as_deref()
        .map(|value| {
            Uuid::parse_str(value).context("parsing torrentio_acquisition_jobs.import_event_id")
        })
        .transpose()?;
    Ok(TorrentioAcquisitionJob {
        job_id: Uuid::parse_str(&job_id_raw)
            .context("parsing torrentio_acquisition_jobs.job_id")?,
        intent_id: Uuid::parse_str(&intent_id_raw)
            .context("parsing torrentio_acquisition_jobs.intent_id")?,
        search_media_type: parse_torrentio_media_type(&search_media_type_raw)?,
        target_key: row.try_get("target_key")?,
        title: row.try_get("title")?,
        external_ids,
        season_number: row.try_get("season_number").ok(),
        episode_number: row.try_get("episode_number").ok(),
        absolute_episode_number: row.try_get("absolute_episode_number").ok(),
        aired_at: row_get_optional_datetime(row, "aired_at")?,
        status: row.try_get("status")?,
        route_logical_id: row_get_optional_string(row, "route_logical_id")?,
        candidate_id: row_get_optional_string(row, "candidate_id")?,
        candidate_title: row_get_optional_string(row, "candidate_title")?,
        candidate_source: row_get_optional_string(row, "candidate_source")?,
        candidate_source_kind: row_get_optional_string(row, "candidate_source_kind")?,
        candidate_info_hash: row_get_optional_string(row, "candidate_info_hash")?,
        candidate_file_index: row_get_optional_i64(row, "candidate_file_index")?,
        candidate_quality: row_get_optional_string(row, "candidate_quality")?,
        candidate_size_bytes: row_get_optional_i64(row, "candidate_size_bytes")?
            .and_then(i64_to_u64),
        candidate_seeders: row_get_optional_i64(row, "candidate_seeders")?.and_then(i64_to_u32),
        candidate_language: row_get_optional_string(row, "candidate_language")?,
        candidate_cached_debrid: row_get_optional_i64(row, "candidate_cached_debrid")?
            .map(|value| value != 0),
        candidate_score: row_get_optional_i64(row, "candidate_score")?,
        candidate_score_badges,
        candidate_rank: row_get_optional_i64(row, "candidate_rank")?,
        download_id: row_get_optional_string(row, "download_id")?,
        last_error: row_get_optional_string(row, "last_error")?,
        import_event_id,
        imported_at: row_get_optional_datetime(row, "imported_at")?,
        import_error: row_get_optional_string(row, "import_error")?,
        next_search_at: row_get_optional_datetime(row, "next_search_at")?,
        search_attempts: row
            .try_get::<Option<i64>, _>("search_attempts")
            .ok()
            .flatten()
            .unwrap_or_default(),
    })
}

fn parse_torrentio_media_type(value: &str) -> Result<MediaType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Ok(MediaType::Movie),
        "series" | "tv" => Ok(MediaType::Series),
        "anime" => Ok(MediaType::Anime),
        _ => bail!("unknown Torrentio media type '{}'", value.trim()),
    }
}

fn row_get_optional_string(row: &AnyRow, column: &str) -> Result<Option<String>> {
    Ok(row.try_get::<Option<String>, _>(column).ok().flatten())
}

fn row_get_optional_i64(row: &AnyRow, column: &str) -> Result<Option<i64>> {
    Ok(row.try_get::<Option<i64>, _>(column).ok().flatten())
}

fn row_get_optional_datetime(row: &AnyRow, column: &str) -> Result<Option<DateTime<Utc>>> {
    Ok(row_get_optional_string(row, column)?
        .as_deref()
        .and_then(parse_db_datetime))
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn i64_to_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

fn i64_to_u32(value: i64) -> Option<u32> {
    u32::try_from(value).ok()
}

fn parse_db_datetime(value: &str) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed)
        .map(|value| value.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|value| value.and_utc())
        })
        .or_else(|| {
            NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
                .ok()
                .and_then(|value| value.and_hms_opt(0, 0, 0))
                .map(|value| value.and_utc())
        })
}

fn format_api_error(error: &crate::http::error::ApiError) -> String {
    match error {
        crate::http::error::ApiError::BadRequest(message)
        | crate::http::error::ApiError::Unauthorized(message)
        | crate::http::error::ApiError::Forbidden(message)
        | crate::http::error::ApiError::NotFound(message)
        | crate::http::error::ApiError::Conflict(message)
        | crate::http::error::ApiError::Internal(message) => message.clone(),
    }
}

async fn select_torrentio_provider(
    store: &ExtensionStore<'_>,
    provider_id: Option<Uuid>,
) -> Result<TorrentioProviderSelection> {
    let extension = store
        .get_extension(TORRENTIO_EXTENSION_ID)
        .await?
        .ok_or_else(|| anyhow!("Torrentio source extension is not installed"))?;
    if !extension.enabled {
        bail!("Torrentio source extension is disabled");
    }

    let providers = store.list_provider_details().await?;
    let mut matches = Vec::new();
    for detail in providers {
        if detail.extension_id != TORRENTIO_EXTENSION_ID {
            continue;
        }
        if detail.provider.capability != TORRENTIO_CANDIDATE_CAPABILITY {
            continue;
        }
        if detail.provider.implementation.as_deref() != Some(TORRENTIO_IMPLEMENTATION) {
            continue;
        }
        let Some(instance) = store.get_instance(detail.provider.instance_id).await? else {
            continue;
        };
        if !instance.enabled {
            continue;
        }
        matches.push(TorrentioProviderSelection {
            provider_id: detail.provider.provider_id,
            config: instance
                .config_json
                .unwrap_or_else(default_torrentio_config),
        });
    }
    matches.sort_by_key(|item| item.provider_id);

    if let Some(provider_id) = provider_id {
        return matches
            .into_iter()
            .find(|item| item.provider_id == provider_id)
            .ok_or_else(|| anyhow!("Torrentio source provider '{provider_id}' is not available"));
    }

    matches
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no Torrentio source provider is available"))
}

async fn torrentio_route_options(
    state: &AppState,
    store: &ExtensionStore<'_>,
) -> Result<Vec<TorrentioRouteOption>> {
    let routes = list_acquisition_routes(&state.db_pool, store).await?;
    let protection_status =
        observed_download_protection_status(&state.settings, &state.db_pool, &state.secrets)
            .await
            .ok();
    Ok(routes
        .routes
        .iter()
        .filter(|route| route.owner_id == TORRENTIO_EXTENSION_ID)
        .filter(|route| {
            route.logical_id == DEBRID_DEFAULT_LOGICAL_ID
                || route.logical_id == TORRENT_DEFAULT_LOGICAL_ID
        })
        .map(|route| route_option_from_record(route, protection_status.as_ref()))
        .collect())
}

fn route_option_from_record(
    route: &DownloadBrokerRouteRecord,
    protection_status: Option<&crate::network::protection::DownloadProtectionStatus>,
) -> TorrentioRouteOption {
    let selected_candidate = route.candidates.iter().find(|candidate| candidate.selected);
    let selected_health = selected_candidate.map(|candidate| candidate.health_state);
    let mut blocker = route.blocker.clone();
    let mut needs_account = false;
    let mut account_extension_id = None;

    if route.logical_id == DEBRID_DEFAULT_LOGICAL_ID
        && route.selected_extension_id.as_deref() == Some(REAL_DEBRID_EXTENSION_ID)
        && selected_health != Some(ProviderHealthState::Healthy)
    {
        needs_account = true;
        account_extension_id = Some(REAL_DEBRID_EXTENSION_ID.to_string());
        blocker =
            Some("Add a Real-Debrid account to use direct HTTPS debrid downloads.".to_string());
    }

    if route.logical_id == TORRENT_DEFAULT_LOGICAL_ID
        && route.binding_kind == DownloadBrokerBindingKind::ManagedProtected
    {
        if let Some(status) = protection_status {
            if let Some(status_blocker) = status.blocker.as_ref() {
                blocker = Some(format!(
                    "Protected local acquisition is blocked by '{}': {}",
                    status_blocker.code, status_blocker.detail
                ));
            } else if !status
                .protected_apps
                .iter()
                .any(|app| app.eq_ignore_ascii_case("qbittorrent"))
            {
                blocker = Some(
                    "Protected local acquisition requires qBittorrent in the active download protection profile."
                        .to_string(),
                );
            }
        }
    }

    TorrentioRouteOption {
        logical_id: route.logical_id.clone(),
        label: route_label(&route.logical_id).to_string(),
        description: route_description(&route.logical_id).to_string(),
        binding_kind: route.binding_kind,
        selected_provider_id: route.selected_provider_id,
        selected_extension_id: route.selected_extension_id.clone(),
        available: blocker.is_none() && route.selected_provider_id.is_some(),
        needs_account,
        account_extension_id,
        blocker,
    }
}

fn candidate_from_stream(
    stream: &StremioStream,
    idx: usize,
    stremio_type: &str,
    stremio_id: &str,
    route_options: &[TorrentioRouteOption],
    route_policy: TorrentioRoutePolicy,
) -> Option<TorrentioCandidate> {
    let display_title = stream_display_title(stream)?;
    let (source, source_kind) = source_for_stream(stream, &display_title)?;
    let route_logical_ids = route_ids_for_source_kind(&source_kind);
    let detail_text = stream_detail_text(stream);
    let default_route_logical_id =
        choose_default_route(&route_logical_ids, route_options, route_policy);
    let info_hash = stream
        .info_hash
        .as_ref()
        .and_then(normalize_info_hash)
        .or_else(|| info_hash_from_magnet(&source));
    let hash = blake3::hash(source.as_bytes()).to_hex().to_string();
    let id = format!(
        "torrentio:{}:{}:{}:{}",
        stremio_type,
        stremio_id,
        idx,
        &hash[..16]
    );

    Some(TorrentioCandidate {
        id,
        source_provider: "Torrentio".to_string(),
        title: display_title,
        name: stream.name.as_ref().and_then(non_empty_string),
        description: stream.title.as_ref().and_then(non_empty_string),
        source,
        source_kind,
        info_hash,
        file_index: stream.file_idx,
        quality: detect_quality(&detail_text),
        size_bytes: stream
            .behavior_hints
            .as_ref()
            .and_then(|hints| hints.video_size)
            .or_else(|| parse_size_bytes(&detail_text)),
        seeders: parse_seeders(&detail_text),
        language: detect_language(&detail_text),
        cached_debrid: detect_cached_debrid(&detail_text),
        route_logical_ids,
        default_route_logical_id,
        score: 0,
        score_badges: Vec::new(),
        rank: idx + 1,
    })
}

fn source_for_stream(stream: &StremioStream, display_title: &str) -> Option<(String, String)> {
    if let Some(url) = stream.url.as_ref().and_then(non_empty_string) {
        let lowered = url.to_ascii_lowercase();
        if lowered.starts_with("magnet:") {
            return Some((url, "magnet".to_string()));
        }
        if lowered.starts_with("http://") || lowered.starts_with("https://") {
            return Some((url, "http".to_string()));
        }
    }

    let info_hash = stream.info_hash.as_ref().and_then(normalize_info_hash)?;
    Some((
        magnet_from_info_hash(&info_hash, display_title, &stream.sources),
        "magnet".to_string(),
    ))
}

fn magnet_from_info_hash(info_hash: &str, display_title: &str, sources: &[String]) -> String {
    let mut magnet = format!("magnet:?xt=urn:btih:{info_hash}");
    if !display_title.trim().is_empty() {
        magnet.push_str("&dn=");
        magnet.push_str(&urlencoding::encode(display_title));
    }
    for tracker in sources
        .iter()
        .filter_map(|source| tracker_from_source(source))
        .take(10)
    {
        magnet.push_str("&tr=");
        magnet.push_str(&urlencoding::encode(&tracker));
    }
    magnet
}

fn info_hash_from_magnet(source: &str) -> Option<String> {
    let trimmed = source.trim();
    let prefix = "magnet:?";
    if trimmed.len() < prefix.len() || !trimmed[..prefix.len()].eq_ignore_ascii_case(prefix) {
        return None;
    }
    let query = &trimmed[prefix.len()..];
    for part in query.split('&') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        if !key.eq_ignore_ascii_case("xt") {
            continue;
        }
        let decoded = urlencoding::decode(value).ok()?;
        let decoded = decoded.trim();
        let urn_prefix = "urn:btih:";
        if decoded.len() < urn_prefix.len()
            || !decoded[..urn_prefix.len()].eq_ignore_ascii_case(urn_prefix)
        {
            continue;
        }
        let hash = &decoded[urn_prefix.len()..];
        if let Some(info_hash) = normalize_info_hash_str(hash) {
            return Some(info_hash);
        }
    }
    None
}

fn tracker_from_source(source: &str) -> Option<String> {
    let trimmed = source.trim();
    let tracker = trimmed.strip_prefix("tracker:").unwrap_or(trimmed).trim();
    let lowered = tracker.to_ascii_lowercase();
    if lowered.starts_with("udp://")
        || lowered.starts_with("http://")
        || lowered.starts_with("https://")
    {
        Some(tracker.to_string())
    } else {
        None
    }
}

fn route_ids_for_source_kind(source_kind: &str) -> Vec<String> {
    if source_kind.eq_ignore_ascii_case("magnet") {
        vec![
            DEBRID_DEFAULT_LOGICAL_ID.to_string(),
            TORRENT_DEFAULT_LOGICAL_ID.to_string(),
        ]
    } else {
        vec![DEBRID_DEFAULT_LOGICAL_ID.to_string()]
    }
}

fn choose_default_route(
    candidate_routes: &[String],
    route_options: &[TorrentioRouteOption],
    route_policy: TorrentioRoutePolicy,
) -> Option<String> {
    if route_policy == TorrentioRoutePolicy::AskEachTime {
        return None;
    }
    let available = |logical_id: &str| {
        candidate_routes.iter().any(|item| item == logical_id)
            && route_options
                .iter()
                .any(|route| route.logical_id == logical_id && route.available)
    };
    for logical_id in route_policy.route_order() {
        if available(logical_id) {
            return Some(logical_id.to_string());
        }
    }
    candidate_routes
        .iter()
        .find(|logical_id| {
            route_policy
                .route_order()
                .iter()
                .any(|preferred| logical_id.as_str() == *preferred)
        })
        .cloned()
        .or_else(|| candidate_routes.first().cloned())
}

fn resolve_stremio_stream_target(
    request: &TorrentioCandidateSearchRequest,
    media_type: MediaType,
) -> Result<(String, String)> {
    if let Some(stremio_id) = request
        .stremio_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        validate_stremio_id(stremio_id)?;
        return Ok((stremio_type(media_type).to_string(), stremio_id.to_string()));
    }

    let ids = request
        .external_ids
        .as_ref()
        .ok_or_else(|| anyhow!("external_ids are required for Torrentio candidate search"))?;
    match media_type {
        MediaType::Movie => {
            let imdb = ids
                .imdb
                .as_deref()
                .and_then(non_empty_str)
                .ok_or_else(|| anyhow!("movie Torrentio search requires an IMDb id"))?;
            validate_stremio_id(imdb)?;
            Ok(("movie".to_string(), imdb.to_string()))
        }
        MediaType::Series => {
            let imdb = ids
                .imdb
                .as_deref()
                .and_then(non_empty_str)
                .ok_or_else(|| anyhow!("TV Torrentio search requires an IMDb id"))?;
            let season = request
                .season
                .filter(|value| *value > 0)
                .ok_or_else(|| anyhow!("TV Torrentio search requires season"))?;
            let episode = request
                .episode
                .filter(|value| *value > 0)
                .ok_or_else(|| anyhow!("TV Torrentio search requires episode"))?;
            let stremio_id = format!("{imdb}:{season}:{episode}");
            validate_stremio_id(&stremio_id)?;
            Ok(("series".to_string(), stremio_id))
        }
        MediaType::Anime => {
            if let Some(imdb) = ids.imdb.as_deref().and_then(non_empty_str) {
                validate_stremio_id(imdb)?;
                return Ok(("anime".to_string(), imdb.to_string()));
            }
            let kitsu = ids
                .kitsu
                .as_deref()
                .and_then(non_empty_str)
                .ok_or_else(|| anyhow!("anime Torrentio search requires an IMDb or Kitsu id"))?;
            let stremio_id = if kitsu.starts_with("kitsu:") {
                kitsu.to_string()
            } else {
                format!("kitsu:{kitsu}")
            };
            validate_stremio_id(&stremio_id)?;
            Ok(("anime".to_string(), stremio_id))
        }
    }
}

fn build_stream_url(config: &Value, stremio_type: &str, stremio_id: &str) -> Result<Url> {
    let base_url = config
        .get("base_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_TORRENTIO_BASE_URL);
    let mut url = Url::parse(base_url).context("parsing Torrentio source base URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("Torrentio source base URL must be http or https");
    }
    let addon_path = config
        .get("addon_path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let addon_path = normalize_addon_path(addon_path)?;
    let mut path_parts = Vec::new();
    let base_path = url.path().trim_matches('/');
    if !base_path.is_empty() {
        path_parts.push(base_path.to_string());
    }
    if !addon_path.is_empty() {
        path_parts.push(addon_path);
    }
    path_parts.push("stream".to_string());
    path_parts.push(stremio_type.to_string());
    path_parts.push(format!("{stremio_id}.json"));
    url.set_path(&path_parts.join("/"));
    url.set_query(None);
    Ok(url)
}

fn normalize_addon_path(raw: &str) -> Result<String> {
    let mut value = raw.trim().trim_matches('/').to_string();
    if value.ends_with("/manifest.json") {
        value.truncate(value.len() - "/manifest.json".len());
        value = value.trim_matches('/').to_string();
    } else if value == "manifest.json" {
        value.clear();
    }
    let lowered = value.to_ascii_lowercase();
    for forbidden in [
        "realdebrid=",
        "premiumize=",
        "alldebrid=",
        "debridlink=",
        "easydebrid=",
        "offcloud=",
        "torbox=",
        "putio=",
    ] {
        if lowered.contains(forbidden) {
            bail!(
                "debrid account tokens must be configured on Elixir's debrid provider, not the Torrentio source"
            );
        }
    }
    if value.contains("..") || value.contains('?') || value.contains('#') {
        bail!("Torrentio addon path must be a relative path segment without query or fragment");
    }
    Ok(value)
}

fn validate_stremio_id(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("Stremio stream id is required");
    }
    if trimmed
        .chars()
        .any(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, ':' | '_' | '-' | '.')))
    {
        bail!("Stremio stream id contains unsupported characters");
    }
    Ok(())
}

fn parse_candidate_media_type(value: &str) -> Result<MediaType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Ok(MediaType::Movie),
        "series" | "tv" => Ok(MediaType::Series),
        "anime" => Ok(MediaType::Anime),
        _ => bail!("unknown media type '{}'", value.trim()),
    }
}

fn stremio_type(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movie",
        MediaType::Series => "series",
        MediaType::Anime => "anime",
    }
}

fn media_type_api_name(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movie",
        MediaType::Series => "series",
        MediaType::Anime => "anime",
    }
}

fn route_label(logical_id: &str) -> &'static str {
    match logical_id {
        DEBRID_DEFAULT_LOGICAL_ID => "Direct HTTPS debrid download",
        TORRENT_DEFAULT_LOGICAL_ID => "Torrent downloader",
        _ => "Acquisition route",
    }
}

fn route_description(logical_id: &str) -> &'static str {
    match logical_id {
        DEBRID_DEFAULT_LOGICAL_ID => {
            "Submit the magnet to Elixir's debrid route for native Real-Debrid materialization."
        }
        TORRENT_DEFAULT_LOGICAL_ID => {
            "Submit the magnet to the selected torrent broker route, usually qBittorrent."
        }
        _ => "Submit through an Elixir acquisition route.",
    }
}

fn default_torrentio_config() -> Value {
    json!({
        "base_url": DEFAULT_TORRENTIO_BASE_URL,
        "addon_path": "",
        "route_policy": "debrid_first",
        "allowed_qualities": "",
        "max_size_gb": 0,
        "required_languages": "",
        "result_limit": DEFAULT_RESULT_LIMIT
    })
}

fn merge_torrentio_config(config: Option<&serde_json::Map<String, Value>>) -> Value {
    let mut merged = default_torrentio_config();
    if let (Some(target), Some(source)) = (merged.as_object_mut(), config) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
    merged
}

fn config_result_limit(config: &Value) -> usize {
    config
        .get("result_limit")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(DEFAULT_RESULT_LIMIT)
}

fn torrentio_source_preferences(config: &Value) -> TorrentioSourcePreferences {
    TorrentioSourcePreferences {
        route_policy: config_route_policy(config),
        allowed_qualities: config_string_list(config.get("allowed_qualities"))
            .into_iter()
            .filter_map(|value| normalize_quality(&value))
            .collect(),
        required_languages: config_string_list(config.get("required_languages"))
            .into_iter()
            .map(|value| normalize_language(&value))
            .filter(|value| !value.is_empty())
            .collect(),
        max_size_bytes: config_max_size_bytes(config),
    }
}

fn config_route_policy(config: &Value) -> TorrentioRoutePolicy {
    let legacy_default_route = config.get("default_route").and_then(Value::as_str);
    if let Some(value) = config.get("route_policy").and_then(Value::as_str) {
        if parse_torrentio_route_policy(value) == TorrentioRoutePolicy::DebridFirst
            && legacy_default_route == Some(TORRENT_DEFAULT_LOGICAL_ID)
        {
            return TorrentioRoutePolicy::TorrentFirst;
        }
        return parse_torrentio_route_policy(value);
    }
    match legacy_default_route.unwrap_or(DEBRID_DEFAULT_LOGICAL_ID) {
        TORRENT_DEFAULT_LOGICAL_ID => TorrentioRoutePolicy::TorrentFirst,
        _ => TorrentioRoutePolicy::DebridFirst,
    }
}

fn parse_torrentio_route_policy(value: &str) -> TorrentioRoutePolicy {
    match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "torrent_first" | "torrent" | "downloaders.torrent.default" => {
            TorrentioRoutePolicy::TorrentFirst
        }
        "ask_each_time" | "ask" | "manual" => TorrentioRoutePolicy::AskEachTime,
        _ => TorrentioRoutePolicy::DebridFirst,
    }
}

fn config_string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .flat_map(split_config_list)
            .collect(),
        Some(Value::String(value)) => split_config_list(value).collect(),
        _ => Vec::new(),
    }
}

fn split_config_list(value: &str) -> impl Iterator<Item = String> + '_ {
    value
        .split([',', '\n', '|'])
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
}

fn config_max_size_bytes(config: &Value) -> Option<u64> {
    let gb = config.get("max_size_gb").and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
    })?;
    (gb > 0.0).then_some((gb * 1024_f64 * 1024_f64 * 1024_f64) as u64)
}

fn torrentio_candidate_matches_preferences(
    candidate: &TorrentioCandidate,
    preferences: &TorrentioSourcePreferences,
) -> bool {
    if let Some(max_size_bytes) = preferences.max_size_bytes {
        if candidate
            .size_bytes
            .map(|size| size > max_size_bytes)
            .unwrap_or(false)
        {
            return false;
        }
    }

    if !preferences.allowed_qualities.is_empty() {
        let Some(quality) = candidate.quality.as_deref().and_then(normalize_quality) else {
            return false;
        };
        if !preferences
            .allowed_qualities
            .iter()
            .any(|allowed| allowed == &quality)
        {
            return false;
        }
    }

    if !preferences.required_languages.is_empty() {
        let Some(language) = candidate.language.as_deref().map(normalize_language) else {
            return false;
        };
        if !candidate_language_matches(&language, &preferences.required_languages) {
            return false;
        }
    }

    true
}

fn candidate_language_matches(language: &str, required: &[String]) -> bool {
    language == "multi" || language == "dual audio" || required.iter().any(|item| item == language)
}

fn provider_endpoint_for_url(raw: &str) -> Result<ProviderEndpoint> {
    let url = Url::parse(raw).context("parsing provider endpoint URL")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("provider endpoint URL host is missing"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("provider endpoint URL port is missing"))?;
    let base_path = url
        .path()
        .trim()
        .trim_end_matches('/')
        .is_empty()
        .then_some(None)
        .unwrap_or_else(|| Some(url.path().trim_end_matches('/').to_string()));
    ProviderEndpoint::new(
        url.scheme().to_string(),
        host.to_string(),
        port,
        base_path,
        None,
    )
}

fn stream_display_title(stream: &StremioStream) -> Option<String> {
    stream
        .name
        .as_ref()
        .and_then(non_empty_string)
        .or_else(|| {
            stream
                .title
                .as_deref()
                .and_then(|title| title.lines().next())
                .and_then(non_empty_str)
                .map(str::to_string)
        })
        .or_else(|| stream.info_hash.as_ref().and_then(normalize_info_hash))
}

fn stream_detail_text(stream: &StremioStream) -> String {
    [
        stream.name.as_deref(),
        stream.title.as_deref(),
        stream
            .behavior_hints
            .as_ref()
            .and_then(|hints| hints.filename.as_deref()),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n")
}

fn detect_quality(text: &str) -> Option<String> {
    let lower = text.to_ascii_lowercase();
    for quality in ["2160p", "4k", "1080p", "720p", "576p", "480p"] {
        if lower.contains(quality) {
            return normalize_quality(quality);
        }
    }
    None
}

fn normalize_quality(value: &str) -> Option<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "4k" | "2160" | "2160p" | "uhd" => Some("2160p".to_string()),
        "1080" | "1080p" | "fhd" => Some("1080p".to_string()),
        "720" | "720p" | "hd" => Some("720p".to_string()),
        "576" | "576p" => Some("576p".to_string()),
        "480" | "480p" | "sd" => Some("480p".to_string()),
        _ => None,
    }
}

fn detect_language(text: &str) -> Option<String> {
    let tokens = normalized_word_tokens(text);
    if tokens.is_empty() {
        return None;
    }
    if contains_language_token(&tokens, &["multi", "multiaudio", "multi audio"]) {
        return Some("multi".to_string());
    }
    if contains_language_token(&tokens, &["dual", "dual audio", "dualaudio"]) {
        return Some("dual audio".to_string());
    }
    for (language, aliases) in [
        ("english", &["english", "eng", "en"][..]),
        ("japanese", &["japanese", "jpn", "jp", "ja"][..]),
        (
            "spanish",
            &["spanish", "espanol", "latino", "spa", "es"][..],
        ),
        ("french", &["french", "fre", "fra", "fr"][..]),
        ("german", &["german", "ger", "deu", "de"][..]),
        ("italian", &["italian", "ita", "it"][..]),
        ("korean", &["korean", "kor", "ko"][..]),
        ("chinese", &["chinese", "mandarin", "chi", "zho", "zh"][..]),
    ] {
        if contains_language_token(&tokens, aliases) {
            return Some(language.to_string());
        }
    }
    None
}

fn normalize_language(value: &str) -> String {
    match value.trim().to_ascii_lowercase().replace('_', " ").as_str() {
        "eng" | "en" => "english".to_string(),
        "jpn" | "jp" | "ja" => "japanese".to_string(),
        "spa" | "es" | "espanol" | "latino" => "spanish".to_string(),
        "fre" | "fra" | "fr" => "french".to_string(),
        "ger" | "deu" | "de" => "german".to_string(),
        "ita" | "it" => "italian".to_string(),
        "kor" | "ko" => "korean".to_string(),
        "chi" | "zho" | "zh" => "chinese".to_string(),
        "dual" | "dual audio" | "dualaudio" => "dual audio".to_string(),
        "multi" | "multi audio" | "multiaudio" => "multi".to_string(),
        other => other.trim().to_string(),
    }
}

fn contains_language_token(tokens: &[String], aliases: &[&str]) -> bool {
    aliases.iter().any(|alias| {
        let alias_tokens = normalized_word_tokens(alias);
        if alias_tokens.is_empty() {
            return false;
        }
        if alias_tokens.len() == 1 {
            return tokens.iter().any(|token| token == &alias_tokens[0]);
        }
        tokens
            .windows(alias_tokens.len())
            .any(|window| window == alias_tokens.as_slice())
    })
}

fn normalized_word_tokens(text: &str) -> Vec<String> {
    text.to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect()
}

fn detect_cached_debrid(text: &str) -> Option<bool> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("uncached") || lower.contains("not cached") {
        return Some(false);
    }
    if lower.contains("cached")
        || lower.contains("real-debrid")
        || lower.contains("real debrid")
        || lower.contains("rd+")
        || lower.contains("[rd]")
        || lower.contains("premiumize")
        || lower.contains("pm+")
        || lower.contains("alldebrid")
        || lower.contains("ad+")
        || lower.contains("instant")
    {
        return Some(true);
    }
    None
}

fn parse_size_bytes(text: &str) -> Option<u64> {
    let normalized = text
        .replace(['\n', '\r', '|', '[', ']', '(', ')'], " ")
        .replace(',', ".");
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    for token in &tokens {
        if let Some(bytes) = parse_compact_size_token(token) {
            return Some(bytes);
        }
    }
    for pair in tokens.windows(2) {
        let Some(number) = clean_number_token(pair[0]).and_then(|value| value.parse::<f64>().ok())
        else {
            continue;
        };
        let Some(multiplier) = size_multiplier(&clean_unit_token(pair[1])) else {
            continue;
        };
        return Some((number * multiplier) as u64);
    }
    None
}

fn parse_compact_size_token(value: &str) -> Option<u64> {
    let token = value.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '.');
    let split = token
        .char_indices()
        .find(|(_, ch)| ch.is_ascii_alphabetic())
        .map(|(idx, _)| idx)?;
    let number = token[..split].parse::<f64>().ok()?;
    let multiplier = size_multiplier(&clean_unit_token(&token[split..]))?;
    Some((number * multiplier) as u64)
}

fn size_multiplier(unit: &str) -> Option<f64> {
    match unit {
        "kb" | "kib" => Some(1024_f64),
        "mb" | "mib" => Some(1024_f64 * 1024_f64),
        "gb" | "gib" => Some(1024_f64 * 1024_f64 * 1024_f64),
        "tb" | "tib" => Some(1024_f64 * 1024_f64 * 1024_f64 * 1024_f64),
        _ => None,
    }
}

fn parse_seeders(text: &str) -> Option<u32> {
    parse_seeders_after_icon(text).or_else(|| parse_seeders_near_label(text))
}

fn parse_seeders_after_icon(text: &str) -> Option<u32> {
    for (idx, ch) in text.char_indices() {
        if ch != '\u{1f464}' && ch != '\u{1f465}' {
            continue;
        }
        let after = &text[idx + ch.len_utf8()..];
        if let Some(value) = parse_first_count(after) {
            return Some(value);
        }
    }
    None
}

fn parse_seeders_near_label(text: &str) -> Option<u32> {
    let normalized = text.replace(
        ['\n', '\r', '|', '[', ']', '(', ')', ':', '=', '/', '\\'],
        " ",
    );
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    for pair in tokens.windows(2) {
        if is_seed_label(pair[0]) {
            if let Some(value) = parse_count_token(pair[1]) {
                return Some(value);
            }
        }
        if is_seed_label(pair[1]) {
            if let Some(value) = parse_count_token(pair[0]) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_first_count(value: &str) -> Option<u32> {
    value.split_whitespace().find_map(parse_count_token)
}

fn parse_count_token(value: &str) -> Option<u32> {
    let token = value
        .trim_matches(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '.' || ch == ','))
        .to_ascii_lowercase();
    if token.is_empty() {
        return None;
    }
    let (number, multiplier) = if let Some(number) = token.strip_suffix('k') {
        (number, 1_000_f64)
    } else if let Some(number) = token.strip_suffix('m') {
        (number, 1_000_000_f64)
    } else {
        (token.as_str(), 1_f64)
    };
    let number = number.replace(',', "");
    let parsed = number.parse::<f64>().ok()?;
    u32::try_from((parsed * multiplier).round() as u64).ok()
}

fn is_seed_label(value: &str) -> bool {
    matches!(
        value
            .trim_matches(|ch: char| !ch.is_ascii_alphabetic())
            .to_ascii_lowercase()
            .as_str(),
        "seed" | "seeds" | "seeder" | "seeders"
    )
}

fn clean_number_token(value: &str) -> Option<String> {
    let cleaned = value
        .trim_matches(|ch: char| !ch.is_ascii_digit() && ch != '.')
        .to_string();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn clean_unit_token(value: &str) -> String {
    value
        .trim_matches(|ch: char| !ch.is_ascii_alphabetic())
        .to_ascii_lowercase()
}

fn normalize_info_hash(value: &String) -> Option<String> {
    normalize_info_hash_str(value)
}

fn normalize_info_hash_str(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if (32..=64).contains(&trimmed.len()) && trimmed.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        Some(trimmed.to_string())
    } else {
        None
    }
}

fn non_empty_string(value: &String) -> Option<String> {
    non_empty_str(value).map(str::to_string)
}

fn non_empty_str(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg(test)]
fn torrentio_manifest_json() -> Value {
    json!({
        "id": TORRENTIO_EXTENSION_ID,
        "version": "0.1.0",
        "kind": "module",
        "name": "Torrentio Source",
        "description": "Torrentio-compatible source discovery for brokered download-first acquisition.",
        "publisher": { "name": "Elixir" },
        "trust": "community",
        "permissions": ["network.egress"],
        "provides": [{
            "capability": TORRENTIO_CANDIDATE_CAPABILITY,
            "slot": "default",
            "cardinality": "one",
            "implementation": TORRENTIO_IMPLEMENTATION,
            "scope": {
                "media_types": ["movies", "tv", "anime"],
                "actions": ["search"]
            }
        }],
        "requires": {
            "downloads": [
                { "kind": "torrent", "mode": "broker", "logical_id": TORRENT_DEFAULT_LOGICAL_ID },
                { "kind": "debrid", "mode": "broker", "logical_id": DEBRID_DEFAULT_LOGICAL_ID }
            ]
        },
        "runtime": { "type": "internal" },
        "control_surface": {
            "adapter": "generic_v1",
            "owned_settings": [
                {
                    "id": "baseUrl",
                    "label": "Source base URL",
                    "description": "HTTP base URL for a Torrentio-compatible Stremio stream source.",
                    "type": "text",
                    "required": true,
                    "ownership": "managed",
                    "storage": { "type": "instance_setting", "key": "base_url" }
                },
                {
                    "id": "addonPath",
                    "label": "Addon path",
                    "description": "Optional non-secret Torrentio configuration path before /stream.",
                    "type": "text",
                    "required": false,
                    "ownership": "managed",
                    "storage": { "type": "instance_setting", "key": "addon_path" }
                },
                {
                    "id": "routePolicy",
                    "label": "Route policy",
                    "description": "Preferred Elixir route priority for ranked source candidates.",
                    "type": "select",
                    "required": true,
                    "ownership": "managed",
                    "storage": { "type": "instance_setting", "key": "route_policy" },
                    "options": [
                        { "value": "debrid_first", "label": "Debrid first" },
                        { "value": "torrent_first", "label": "Torrent first" },
                        { "value": "ask_each_time", "label": "Ask each time" }
                    ]
                },
                {
                    "id": "allowedQualities",
                    "label": "Allowed qualities",
                    "description": "Optional comma-separated quality allowlist, such as 2160p,1080p.",
                    "type": "text",
                    "required": false,
                    "ownership": "managed",
                    "storage": { "type": "instance_setting", "key": "allowed_qualities" }
                },
                {
                    "id": "maxSizeGb",
                    "label": "Max size GB",
                    "description": "Optional maximum candidate size in GB. Use 0 for no size limit.",
                    "type": "number",
                    "required": false,
                    "ownership": "managed",
                    "storage": { "type": "instance_setting", "key": "max_size_gb" }
                },
                {
                    "id": "requiredLanguages",
                    "label": "Required languages",
                    "description": "Optional comma-separated language filter. Empty allows all languages.",
                    "type": "text",
                    "required": false,
                    "ownership": "managed",
                    "storage": { "type": "instance_setting", "key": "required_languages" }
                },
                {
                    "id": "resultLimit",
                    "label": "Result limit",
                    "description": "Maximum candidates to return per search.",
                    "type": "number",
                    "required": true,
                    "ownership": "managed",
                    "storage": { "type": "instance_setting", "key": "result_limit" }
                }
            ],
            "native_only": [{
                "id": "streaming",
                "title": "Streaming",
                "description": "This extension only discovers download candidates. Playback URL resolution remains reserved for a future streaming integration."
            }]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{
            Database,
            models::{ExtensionKind, ExtensionTrustLevel},
        },
        download_broker::{
            DownloadBrokerProviderKind, DownloadBrokerRole, DownloadBrokerRouteCandidate,
            list_acquisition_routes,
        },
        extensions::{manifest::ExtensionManifest, store::NewExtension},
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

    async fn install_torrentio_extension_record(store: &ExtensionStore<'_>) -> Result<()> {
        store
            .upsert_extension(&NewExtension {
                extension_id: TORRENTIO_EXTENSION_ID.to_string(),
                name: "Torrentio Source".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: torrentio_manifest_json(),
                package_hash: Some("test-package".to_string()),
                enabled: true,
            })
            .await?;
        Ok(())
    }

    #[test]
    fn torrentio_manifest_declares_te0_contract() -> Result<()> {
        let manifest: ExtensionManifest = serde_json::from_value(torrentio_manifest_json())?;
        manifest.validate()?;
        assert_eq!(manifest.id, TORRENTIO_EXTENSION_ID);
        assert_eq!(manifest.kind, ExtensionKind::Module);
        assert_eq!(
            manifest
                .runtime
                .as_ref()
                .map(|runtime| runtime.r#type.as_str()),
            Some("internal")
        );
        assert_eq!(
            manifest.provides[0].capability,
            TORRENTIO_CANDIDATE_CAPABILITY
        );
        let logical_ids = manifest
            .requires
            .downloads
            .iter()
            .map(|item| item.resolved_logical_id().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            logical_ids,
            vec![TORRENT_DEFAULT_LOGICAL_ID, DEBRID_DEFAULT_LOGICAL_ID]
        );
        let control = manifest
            .control_surface
            .as_ref()
            .expect("generic control surface");
        assert_eq!(control.adapter, "generic_v1");
        assert!(
            control
                .native_only
                .iter()
                .any(|item| item.id == "streaming")
        );
        let setting_ids = control
            .owned_settings
            .iter()
            .map(|setting| setting.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            setting_ids,
            vec![
                "baseUrl",
                "addonPath",
                "routePolicy",
                "allowedQualities",
                "maxSizeGb",
                "requiredLanguages",
                "resultLimit"
            ]
        );
        assert!(control.owned_settings.iter().all(|setting| !setting.secret));
        Ok(())
    }

    #[tokio::test]
    async fn ensure_torrentio_provider_does_not_auto_install_extension() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        ensure_torrentio_installed_provider_in_store(&store).await?;

        assert!(store.get_extension(TORRENTIO_EXTENSION_ID).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn ensure_torrentio_installed_provider_creates_instance_provider_and_routes() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        install_torrentio_extension_record(&store).await?;
        ensure_torrentio_installed_provider_in_store(&store).await?;

        let extension = store
            .get_extension(TORRENTIO_EXTENSION_ID)
            .await?
            .expect("extension installed");
        assert_eq!(extension.name, "Torrentio Source");
        assert!(extension.enabled);

        let instances = store.list_instances(Some(TORRENTIO_EXTENSION_ID)).await?;
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].instance_name, "default");
        assert_eq!(
            instances[0]
                .config_json
                .as_ref()
                .and_then(|value| value.get("route_policy"))
                .and_then(Value::as_str),
            Some("debrid_first")
        );

        let providers = store.list_providers(Some(instances[0].instance_id)).await?;
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].capability, TORRENTIO_CANDIDATE_CAPABILITY);
        assert_eq!(
            providers[0].implementation.as_deref(),
            Some(TORRENTIO_IMPLEMENTATION)
        );
        assert_eq!(providers[0].health_state, ProviderHealthState::Healthy);
        let readiness = store
            .get_provider_readiness(providers[0].provider_id)
            .await?
            .expect("provider readiness");
        assert_eq!(
            readiness.readiness_phase,
            ProviderReadinessPhase::DriverReady
        );
        assert!(
            readiness
                .readiness_detail
                .as_deref()
                .unwrap_or_default()
                .contains("source discovery")
        );

        let routes = list_acquisition_routes(&database.pool, &store).await?;
        let owner_routes = routes
            .routes
            .iter()
            .filter(|route| route.owner_id == TORRENTIO_EXTENSION_ID)
            .collect::<Vec<_>>();
        assert_eq!(owner_routes.len(), 2);
        assert_eq!(
            owner_routes
                .iter()
                .map(|route| route.logical_id.as_str())
                .collect::<Vec<_>>(),
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID]
        );
        for route in owner_routes {
            assert_eq!(route.owner_label, "Torrentio Source");
            let suffix = if route.logical_id == DEBRID_DEFAULT_LOGICAL_ID {
                "debrid"
            } else {
                "torrent"
            };
            let expected_category = format!("elixir-extensions-torrentio-{suffix}");
            let expected_download_path = format!("/downloads/{expected_category}");
            assert_eq!(route.category.as_deref(), Some(expected_category.as_str()));
            assert_eq!(
                route.download_path.as_deref(),
                Some(expected_download_path.as_str())
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn ensure_torrentio_installed_provider_preserves_disabled_extension_state() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        install_torrentio_extension_record(&store).await?;
        ensure_torrentio_installed_provider_in_store(&store).await?;
        store
            .set_extension_enabled(TORRENTIO_EXTENSION_ID, false)
            .await?;

        ensure_torrentio_installed_provider_in_store(&store).await?;

        let extension = store
            .get_extension(TORRENTIO_EXTENSION_ID)
            .await?
            .expect("extension installed");
        assert!(!extension.enabled);
        Ok(())
    }

    #[test]
    fn stremio_target_for_tv_requires_episode_specific_id() -> Result<()> {
        let request = TorrentioCandidateSearchRequest {
            media_type: "series".to_string(),
            external_ids: Some(ExternalIds {
                imdb: Some("tt0903747".to_string()),
                ..Default::default()
            }),
            season: Some(1),
            episode: Some(2),
            stremio_id: None,
            provider_id: None,
            limit: None,
        };
        let (kind, id) = resolve_stremio_stream_target(&request, MediaType::Series)?;
        assert_eq!(kind, "series");
        assert_eq!(id, "tt0903747:1:2");
        Ok(())
    }

    #[test]
    fn stremio_target_for_movie_uses_imdb_id() -> Result<()> {
        let request = TorrentioCandidateSearchRequest {
            media_type: "movie".to_string(),
            external_ids: Some(ExternalIds {
                imdb: Some("tt0111161".to_string()),
                ..Default::default()
            }),
            season: None,
            episode: None,
            stremio_id: None,
            provider_id: None,
            limit: None,
        };
        let (kind, id) = resolve_stremio_stream_target(&request, MediaType::Movie)?;
        assert_eq!(kind, "movie");
        assert_eq!(id, "tt0111161");
        Ok(())
    }

    #[test]
    fn stremio_target_for_anime_prefers_imdb_and_falls_back_to_kitsu() -> Result<()> {
        let imdb_request = TorrentioCandidateSearchRequest {
            media_type: "anime".to_string(),
            external_ids: Some(ExternalIds {
                imdb: Some("tt9335498".to_string()),
                kitsu: Some("43056".to_string()),
                ..Default::default()
            }),
            season: None,
            episode: None,
            stremio_id: None,
            provider_id: None,
            limit: None,
        };
        let (kind, id) = resolve_stremio_stream_target(&imdb_request, MediaType::Anime)?;
        assert_eq!(kind, "anime");
        assert_eq!(id, "tt9335498");

        let kitsu_request = TorrentioCandidateSearchRequest {
            media_type: "anime".to_string(),
            external_ids: Some(ExternalIds {
                kitsu: Some("43056".to_string()),
                ..Default::default()
            }),
            season: None,
            episode: None,
            stremio_id: None,
            provider_id: None,
            limit: None,
        };
        let (kind, id) = resolve_stremio_stream_target(&kitsu_request, MediaType::Anime)?;
        assert_eq!(kind, "anime");
        assert_eq!(id, "kitsu:43056");
        Ok(())
    }

    #[test]
    fn build_stream_url_uses_base_path_addon_path_and_stream_target() -> Result<()> {
        let config = json!({
            "base_url": "https://source.example/root?ignored=true",
            "addon_path": "/providers=yts/manifest.json"
        });
        let url = build_stream_url(&config, "movie", "tt0111161")?;
        assert_eq!(
            url.as_str(),
            "https://source.example/root/providers=yts/stream/movie/tt0111161.json"
        );
        Ok(())
    }

    #[test]
    fn route_option_marks_missing_real_debrid_account() {
        let provider_id = Uuid::new_v4();
        let route = DownloadBrokerRouteRecord {
            logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
            owner_id: TORRENTIO_EXTENSION_ID.to_string(),
            owner_label: "Torrentio Source".to_string(),
            role: DownloadBrokerRole::DebridResolver,
            binding_kind: DownloadBrokerBindingKind::Debrid,
            provider_id: None,
            profile_id: None,
            status: "auto".to_string(),
            inherited: false,
            selected_provider_id: Some(provider_id),
            selected_provider_kind: Some(DownloadBrokerProviderKind::Debrid),
            selected_extension_id: Some(REAL_DEBRID_EXTENSION_ID.to_string()),
            category: Some("elixir-extensions-torrentio-debrid".to_string()),
            download_path: Some("/downloads/elixir-extensions-torrentio-debrid".to_string()),
            allow_shared_path: false,
            candidates: vec![DownloadBrokerRouteCandidate {
                provider_id,
                provider_kind: DownloadBrokerProviderKind::Debrid,
                extension_id: REAL_DEBRID_EXTENSION_ID.to_string(),
                implementation: Some("real_debrid".to_string()),
                health_state: ProviderHealthState::Unhealthy,
                selected: true,
            }],
            checks: Vec::new(),
            blocker: None,
        };

        let option = route_option_from_record(&route, None);

        assert!(!option.available);
        assert!(option.needs_account);
        assert_eq!(
            option.account_extension_id.as_deref(),
            Some(REAL_DEBRID_EXTENSION_ID)
        );
        assert!(
            option
                .blocker
                .as_deref()
                .unwrap_or_default()
                .contains("Add a Real-Debrid account")
        );
    }

    #[test]
    fn stream_candidate_builds_magnet_from_info_hash() {
        let route_options = vec![TorrentioRouteOption {
            logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
            label: "Debrid".to_string(),
            description: String::new(),
            binding_kind: DownloadBrokerBindingKind::Debrid,
            selected_provider_id: Some(Uuid::new_v4()),
            selected_extension_id: Some(REAL_DEBRID_EXTENSION_ID.to_string()),
            available: true,
            needs_account: false,
            account_extension_id: None,
            blocker: None,
        }];
        let stream = StremioStream {
            name: Some("Example 1080p".to_string()),
            title: Some("Example 1080p\nSeeders: 1,234".to_string()),
            url: None,
            info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            file_idx: Some(2),
            sources: vec!["tracker:udp://tracker.example/announce".to_string()],
            behavior_hints: Some(StremioBehaviorHints {
                filename: Some("Example.2024.1080p.WEB-DL.mkv".to_string()),
                video_size: Some(1_610_612_736),
            }),
        };
        let candidate = candidate_from_stream(
            &stream,
            0,
            "movie",
            "tt1234567",
            &route_options,
            TorrentioRoutePolicy::DebridFirst,
        )
        .expect("candidate");
        assert!(candidate.source.starts_with("magnet:?xt=urn:btih:"));
        assert!(candidate.source.contains("&tr=udp%3A%2F%2Ftracker.example"));
        assert_eq!(candidate.quality.as_deref(), Some("1080p"));
        assert_eq!(candidate.size_bytes, Some(1_610_612_736));
        assert_eq!(candidate.seeders, Some(1_234));
        assert_eq!(
            candidate.info_hash.as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );
        assert_eq!(candidate.file_index, Some(2));
        assert_eq!(
            candidate.route_logical_ids,
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID]
        );
        assert_eq!(
            candidate.default_route_logical_id.as_deref(),
            Some(DEBRID_DEFAULT_LOGICAL_ID)
        );
    }

    #[test]
    fn stream_candidate_extracts_info_hash_from_magnet_url() {
        let route_options = vec![TorrentioRouteOption {
            logical_id: TORRENT_DEFAULT_LOGICAL_ID.to_string(),
            label: "Torrent".to_string(),
            description: String::new(),
            binding_kind: DownloadBrokerBindingKind::ManagedProtected,
            selected_provider_id: Some(Uuid::new_v4()),
            selected_extension_id: Some("elixir.modules.qbittorrent".to_string()),
            available: true,
            needs_account: false,
            account_extension_id: None,
            blocker: None,
        }];
        let stream = StremioStream {
            name: Some("Example 2160p".to_string()),
            title: Some(format!("Example 2160p 2.5GB \u{1f464} 42")),
            url: Some(
                "magnet:?xt=urn:btih:abcdefabcdefabcdefabcdefabcdefabcdefabcd&dn=Example"
                    .to_string(),
            ),
            info_hash: None,
            file_idx: None,
            sources: Vec::new(),
            behavior_hints: None,
        };

        let candidate = candidate_from_stream(
            &stream,
            1,
            "movie",
            "tt1234567",
            &route_options,
            TorrentioRoutePolicy::DebridFirst,
        )
        .expect("candidate");

        assert_eq!(candidate.source_kind, "magnet");
        assert_eq!(
            candidate.info_hash.as_deref(),
            Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd")
        );
        assert_eq!(candidate.quality.as_deref(), Some("2160p"));
        assert_eq!(candidate.size_bytes, Some(2_684_354_560));
        assert_eq!(candidate.seeders, Some(42));
        assert_eq!(
            candidate.default_route_logical_id.as_deref(),
            Some(TORRENT_DEFAULT_LOGICAL_ID)
        );
    }

    #[test]
    fn http_stream_candidates_only_support_debrid_route() {
        let route_options = vec![
            TorrentioRouteOption {
                logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                label: "Debrid".to_string(),
                description: String::new(),
                binding_kind: DownloadBrokerBindingKind::Debrid,
                selected_provider_id: Some(Uuid::new_v4()),
                selected_extension_id: Some(REAL_DEBRID_EXTENSION_ID.to_string()),
                available: true,
                needs_account: false,
                account_extension_id: None,
                blocker: None,
            },
            TorrentioRouteOption {
                logical_id: TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                label: "Torrent".to_string(),
                description: String::new(),
                binding_kind: DownloadBrokerBindingKind::ManagedProtected,
                selected_provider_id: Some(Uuid::new_v4()),
                selected_extension_id: Some("elixir.modules.qbittorrent".to_string()),
                available: true,
                needs_account: false,
                account_extension_id: None,
                blocker: None,
            },
        ];
        let stream = StremioStream {
            name: Some("Hoster 720p".to_string()),
            title: Some("Hoster 720p 900 MB seeds 12".to_string()),
            url: Some("https://cdn.example/media/file.mkv".to_string()),
            info_hash: None,
            file_idx: None,
            sources: Vec::new(),
            behavior_hints: None,
        };

        let candidate = candidate_from_stream(
            &stream,
            0,
            "movie",
            "tt1234567",
            &route_options,
            TorrentioRoutePolicy::TorrentFirst,
        )
        .expect("candidate");

        assert_eq!(candidate.source_kind, "http");
        assert_eq!(candidate.route_logical_ids, vec![DEBRID_DEFAULT_LOGICAL_ID]);
        assert_eq!(
            candidate.default_route_logical_id.as_deref(),
            Some(DEBRID_DEFAULT_LOGICAL_ID)
        );
        assert_eq!(candidate.seeders, Some(12));
    }

    #[test]
    fn monitored_choice_prefers_available_debrid_over_better_seeded_torrent() {
        let response = TorrentioCandidateSearchResponse {
            extension_id: TORRENTIO_EXTENSION_ID.to_string(),
            owner_id: TORRENTIO_EXTENSION_ID.to_string(),
            provider_id: Uuid::new_v4(),
            media_type: "movie".to_string(),
            stremio_type: "movie".to_string(),
            stremio_id: "tt1234567".to_string(),
            route_policy: "debrid_first".to_string(),
            route_options: vec![
                test_route_option(DEBRID_DEFAULT_LOGICAL_ID, true, None),
                test_route_option(TORRENT_DEFAULT_LOGICAL_ID, true, None),
            ],
            candidates: vec![
                test_candidate(
                    "torrent",
                    vec![TORRENT_DEFAULT_LOGICAL_ID],
                    Some(10_000),
                    Some("2160p"),
                    1,
                ),
                test_candidate(
                    "debrid",
                    vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
                    Some(1),
                    Some("1080p"),
                    2,
                ),
            ],
        };

        let choice = choose_torrentio_submission(&response).expect("choice");

        assert_eq!(choice.candidate.id, "debrid");
        assert_eq!(choice.route.logical_id, DEBRID_DEFAULT_LOGICAL_ID);
    }

    #[test]
    fn monitored_choice_falls_back_to_torrent_when_debrid_blocked() {
        let response = TorrentioCandidateSearchResponse {
            extension_id: TORRENTIO_EXTENSION_ID.to_string(),
            owner_id: TORRENTIO_EXTENSION_ID.to_string(),
            provider_id: Uuid::new_v4(),
            media_type: "movie".to_string(),
            stremio_type: "movie".to_string(),
            stremio_id: "tt1234567".to_string(),
            route_policy: "debrid_first".to_string(),
            route_options: vec![
                test_route_option(
                    DEBRID_DEFAULT_LOGICAL_ID,
                    false,
                    Some("Add a Real-Debrid account."),
                ),
                test_route_option(TORRENT_DEFAULT_LOGICAL_ID, true, None),
            ],
            candidates: vec![test_candidate(
                "magnet",
                vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
                Some(100),
                Some("1080p"),
                1,
            )],
        };

        let choice = choose_torrentio_submission(&response).expect("choice");

        assert_eq!(choice.candidate.id, "magnet");
        assert_eq!(choice.route.logical_id, TORRENT_DEFAULT_LOGICAL_ID);
    }

    #[test]
    fn monitored_choice_respects_torrent_first_route_policy() {
        let response = TorrentioCandidateSearchResponse {
            extension_id: TORRENTIO_EXTENSION_ID.to_string(),
            owner_id: TORRENTIO_EXTENSION_ID.to_string(),
            provider_id: Uuid::new_v4(),
            media_type: "movie".to_string(),
            stremio_type: "movie".to_string(),
            stremio_id: "tt1234567".to_string(),
            route_policy: "torrent_first".to_string(),
            route_options: vec![
                test_route_option(DEBRID_DEFAULT_LOGICAL_ID, true, None),
                test_route_option(TORRENT_DEFAULT_LOGICAL_ID, true, None),
            ],
            candidates: vec![test_candidate(
                "magnet",
                vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
                Some(100),
                Some("1080p"),
                1,
            )],
        };

        let choice = choose_torrentio_submission(&response).expect("choice");

        assert_eq!(choice.route.logical_id, TORRENT_DEFAULT_LOGICAL_ID);
    }

    #[test]
    fn ask_each_time_route_policy_blocks_automatic_submission() {
        let response = TorrentioCandidateSearchResponse {
            extension_id: TORRENTIO_EXTENSION_ID.to_string(),
            owner_id: TORRENTIO_EXTENSION_ID.to_string(),
            provider_id: Uuid::new_v4(),
            media_type: "movie".to_string(),
            stremio_type: "movie".to_string(),
            stremio_id: "tt1234567".to_string(),
            route_policy: "ask_each_time".to_string(),
            route_options: vec![test_route_option(DEBRID_DEFAULT_LOGICAL_ID, true, None)],
            candidates: vec![test_candidate(
                "magnet",
                vec![DEBRID_DEFAULT_LOGICAL_ID],
                Some(100),
                Some("1080p"),
                1,
            )],
        };

        assert!(choose_torrentio_submission(&response).is_none());
        assert!(
            torrentio_route_blocker_message(&response)
                .unwrap_or_default()
                .contains("ask each time")
        );
    }

    #[test]
    fn candidate_preferences_filter_quality_size_and_language() {
        let preferences = torrentio_source_preferences(&json!({
            "route_policy": "debrid_first",
            "allowed_qualities": "1080p",
            "max_size_gb": 2,
            "required_languages": "english"
        }));
        let mut candidate = test_candidate(
            "ok",
            vec![DEBRID_DEFAULT_LOGICAL_ID],
            Some(10),
            Some("1080p"),
            1,
        );
        candidate.size_bytes = Some(1_500_000_000);
        candidate.language = Some("english".to_string());
        assert!(torrentio_candidate_matches_preferences(
            &candidate,
            &preferences
        ));

        candidate.quality = Some("2160p".to_string());
        assert!(!torrentio_candidate_matches_preferences(
            &candidate,
            &preferences
        ));

        candidate.quality = Some("1080p".to_string());
        candidate.size_bytes = Some(3_000_000_000);
        assert!(!torrentio_candidate_matches_preferences(
            &candidate,
            &preferences
        ));

        candidate.size_bytes = Some(1_500_000_000);
        candidate.language = Some("japanese".to_string());
        assert!(!torrentio_candidate_matches_preferences(
            &candidate,
            &preferences
        ));
    }

    #[test]
    fn candidate_scoring_exposes_cached_debrid_and_language_badges() {
        let route_options = vec![test_route_option(DEBRID_DEFAULT_LOGICAL_ID, true, None)];
        let stream = StremioStream {
            name: Some("Example 1080p [RD+] English".to_string()),
            title: Some("Example 1080p cached 2 GB Seeders: 20".to_string()),
            url: None,
            info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            file_idx: None,
            sources: Vec::new(),
            behavior_hints: None,
        };
        let mut candidate = candidate_from_stream(
            &stream,
            0,
            "movie",
            "tt1234567",
            &route_options,
            TorrentioRoutePolicy::DebridFirst,
        )
        .expect("candidate");

        annotate_torrentio_candidate_score(
            &mut candidate,
            &route_options,
            TorrentioRoutePolicy::DebridFirst,
        );

        assert_eq!(candidate.language.as_deref(), Some("english"));
        assert_eq!(candidate.cached_debrid, Some(true));
        assert!(candidate.score > 1_000_000);
        assert!(
            candidate
                .score_badges
                .iter()
                .any(|badge| badge.kind == "cache")
        );
    }

    #[test]
    fn episode_air_date_accepts_cinemeta_tvdb_and_anilist_shapes() {
        assert!(episode_air_date(&json!({ "released": "2024-02-03T00:00:00.000Z" })).is_some());
        assert!(episode_air_date(&json!({ "aired": "2024-02-03" })).is_some());
        assert!(
            episode_air_date(&json!({ "startDate": { "year": 2024, "month": 2, "day": 3 } }))
                .is_some()
        );
        assert!(episode_air_date(&json!({ "airingAt": 1_707_523_200 })).is_some());
    }

    #[test]
    fn anizip_mapping_seed_season_uses_dominant_real_season() {
        let mapping = AniZipMapping {
            ids: ExternalIds::default(),
            episodes: vec![
                crate::library::AniZipEpisodeRecord {
                    season_number: None,
                    episode_number: None,
                    absolute_episode_number: None,
                    title: None,
                    overview: None,
                    runtime_minutes: None,
                    image: None,
                    tvdb_id: None,
                    anidb_eid: None,
                    raw: json!({}),
                },
                crate::library::AniZipEpisodeRecord {
                    season_number: Some(2),
                    episode_number: Some(1),
                    absolute_episode_number: Some(13),
                    title: None,
                    overview: None,
                    runtime_minutes: None,
                    image: None,
                    tvdb_id: None,
                    anidb_eid: None,
                    raw: json!({}),
                },
                crate::library::AniZipEpisodeRecord {
                    season_number: Some(2),
                    episode_number: Some(2),
                    absolute_episode_number: Some(14),
                    title: None,
                    overview: None,
                    runtime_minutes: None,
                    image: None,
                    tvdb_id: None,
                    anidb_eid: None,
                    raw: json!({}),
                },
            ],
            images: Vec::new(),
            titles: HashMap::new(),
        };

        assert_eq!(anizip_mapping_seed_season(&mapping), Some(2));
    }

    #[test]
    fn anizip_mapping_seed_season_tie_prefers_lowest_real_season() {
        let mapping = test_anizip_mapping(
            ExternalIds::default(),
            &[(Some(3), Some(1), Some(25)), (Some(2), Some(1), Some(13))],
        );

        assert_eq!(anizip_mapping_seed_season(&mapping), Some(2));
    }

    #[test]
    fn anizip_targets_use_season_local_episode_for_per_season_anilist_state() {
        let cutoff = parse_air_date_text("2026-05-05T12:00:00Z").expect("cutoff");
        let base_ids = ExternalIds {
            anilist: Some("113415".to_string()),
            ..Default::default()
        };
        let intent = test_managed_ingest_intent(MediaType::Anime, base_ids.clone());
        let mapping = test_anizip_mapping(
            ExternalIds {
                imdb: Some("tt1234567".to_string()),
                anilist: Some("145064".to_string()),
                ..Default::default()
            },
            &[(Some(2), Some(1), Some(13)), (Some(2), Some(2), Some(14))],
        );
        let releasing = AniListTargetState {
            status: Some("RELEASING".to_string()),
            episodes: None,
            next_airing_episode: Some(2),
            next_airing_at: Some(cutoff + chrono::Duration::days(7)),
        };
        let mut expansion = TorrentioTargetExpansion::default();

        append_torrentio_episode_targets_from_anizip_mapping(
            &mut expansion,
            &intent,
            &base_ids,
            "145064",
            mapping,
            Some(&releasing),
            cutoff,
        );

        assert_eq!(expansion.targets.len(), 1);
        let target = &expansion.targets[0];
        assert_eq!(target.target_key, "s02e01");
        assert_eq!(target.absolute_episode_number, Some(13));
        assert_eq!(target.external_ids.anilist.as_deref(), Some("145064"));
        assert_eq!(target.external_ids.imdb.as_deref(), Some("tt1234567"));
    }

    #[test]
    fn anime_release_state_never_guesses_future_releasing_episodes() {
        let cutoff = parse_air_date_text("2024-02-03").expect("cutoff");
        let releasing = AniListTargetState {
            status: Some("RELEASING".to_string()),
            episodes: None,
            next_airing_episode: Some(5),
            next_airing_at: None,
        };
        assert!(anime_episode_is_released(
            &json!({}),
            Some(4),
            Some(&releasing),
            cutoff
        ));
        assert!(!anime_episode_is_released(
            &json!({}),
            Some(5),
            Some(&releasing),
            cutoff
        ));
        assert!(!anime_episode_is_released(
            &json!({}),
            Some(6),
            Some(&releasing),
            cutoff
        ));
    }

    #[test]
    fn anime_finished_state_allows_known_mapped_episodes() {
        let cutoff = parse_air_date_text("2024-02-03").expect("cutoff");
        let finished = AniListTargetState {
            status: Some("FINISHED".to_string()),
            episodes: Some(12),
            next_airing_episode: None,
            next_airing_at: None,
        };
        assert!(anime_episode_is_released(
            &json!({}),
            Some(12),
            Some(&finished),
            cutoff
        ));
        assert!(!anime_episode_is_released(
            &json!({}),
            Some(13),
            Some(&finished),
            cutoff
        ));
    }

    #[test]
    fn metadata_refresh_uses_six_to_twelve_hour_periodic_window() {
        let now = parse_air_date_text("2026-05-05T12:00:00Z").expect("date");
        let next = next_torrentio_metadata_refresh_at(Uuid::from_u128(42), now, None);
        let seconds = next.signed_duration_since(now).num_seconds();

        assert!(seconds >= TORRENTIO_METADATA_REFRESH_MIN_SECONDS);
        assert!(
            seconds
                <= TORRENTIO_METADATA_REFRESH_MIN_SECONDS
                    + TORRENTIO_METADATA_REFRESH_JITTER_SECONDS
        );
    }

    #[test]
    fn metadata_refresh_moves_to_known_air_window_when_sooner() {
        let now = parse_air_date_text("2026-05-05T12:00:00Z").expect("date");
        let next_air_at = now + chrono::Duration::minutes(30);
        let next = next_torrentio_metadata_refresh_at(Uuid::from_u128(43), now, Some(next_air_at));
        let seconds_after_air = next.signed_duration_since(next_air_at).num_seconds();

        assert!(next >= next_air_at);
        assert!(seconds_after_air <= TORRENTIO_METADATA_AIR_WINDOW_JITTER_SECONDS);
    }

    #[test]
    fn candidate_retry_schedule_uses_hot_warm_and_cold_windows() {
        let now = parse_air_date_text("2026-05-05T12:00:00Z").expect("date");
        let hot = test_target_with_air(now - chrono::Duration::minutes(30));
        let warm = test_target_with_air(now - chrono::Duration::hours(3));
        let cold = test_target_with_air(now - chrono::Duration::days(2));

        let hot_seconds = next_torrentio_candidate_search_at(&hot, 1, now)
            .signed_duration_since(now)
            .num_seconds();
        assert!(hot_seconds >= TORRENTIO_CANDIDATE_HOT_RETRY_SECONDS);
        assert!(
            hot_seconds
                <= TORRENTIO_CANDIDATE_HOT_RETRY_SECONDS
                    + TORRENTIO_CANDIDATE_DISCOVERY_JITTER_SECONDS
        );

        let warm_seconds = next_torrentio_candidate_search_at(&warm, 1, now)
            .signed_duration_since(now)
            .num_seconds();
        assert!(warm_seconds >= TORRENTIO_CANDIDATE_WARM_RETRY_SECONDS);
        assert!(
            warm_seconds
                <= TORRENTIO_CANDIDATE_WARM_RETRY_SECONDS
                    + TORRENTIO_CANDIDATE_DISCOVERY_JITTER_SECONDS
        );

        let first_old_retry_seconds = next_torrentio_candidate_search_at(&cold, 1, now)
            .signed_duration_since(now)
            .num_seconds();
        assert!(first_old_retry_seconds >= TORRENTIO_CANDIDATE_DISCOVERY_MIN_SECONDS);
        assert!(
            first_old_retry_seconds
                <= TORRENTIO_CANDIDATE_DISCOVERY_MIN_SECONDS
                    + TORRENTIO_CANDIDATE_DISCOVERY_JITTER_SECONDS
        );

        let cold_seconds = next_torrentio_candidate_search_at(&cold, 4, now)
            .signed_duration_since(now)
            .num_seconds();
        assert!(cold_seconds >= TORRENTIO_CANDIDATE_COLD_RETRY_SECONDS);
        assert!(
            cold_seconds
                <= TORRENTIO_CANDIDATE_COLD_RETRY_SECONDS
                    + TORRENTIO_CANDIDATE_DISCOVERY_JITTER_SECONDS
        );
    }

    #[test]
    fn candidate_due_check_respects_next_search_at() {
        let now = parse_air_date_text("2026-05-05T12:00:00Z").expect("date");
        let mut job = test_acquisition_job(Some(now + chrono::Duration::minutes(1)));
        assert!(!torrentio_job_is_ready_for_submission(&job, now));

        job.next_search_at = Some(now);
        assert!(torrentio_job_is_ready_for_submission(&job, now));

        job.status = TORRENTIO_JOB_SUBMITTED.to_string();
        assert!(!torrentio_job_is_ready_for_submission(&job, now));
    }

    fn test_target_with_air(aired_at: DateTime<Utc>) -> TorrentioAcquisitionTarget {
        TorrentioAcquisitionTarget {
            target_key: "s01e01".to_string(),
            title: "Example S01E01".to_string(),
            external_ids: ExternalIds {
                imdb: Some("tt1234567".to_string()),
                ..Default::default()
            },
            search_media_type: MediaType::Series,
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            aired_at: Some(aired_at),
        }
    }

    fn test_acquisition_job(next_search_at: Option<DateTime<Utc>>) -> TorrentioAcquisitionJob {
        TorrentioAcquisitionJob {
            job_id: Uuid::new_v4(),
            intent_id: Uuid::new_v4(),
            search_media_type: MediaType::Series,
            target_key: "s01e01".to_string(),
            title: "Example S01E01".to_string(),
            external_ids: ExternalIds {
                imdb: Some("tt1234567".to_string()),
                ..Default::default()
            },
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            aired_at: None,
            status: TORRENTIO_JOB_PENDING.to_string(),
            route_logical_id: None,
            candidate_id: None,
            candidate_title: None,
            candidate_source: None,
            candidate_source_kind: None,
            candidate_info_hash: None,
            candidate_file_index: None,
            candidate_quality: None,
            candidate_size_bytes: None,
            candidate_seeders: None,
            candidate_language: None,
            candidate_cached_debrid: None,
            candidate_score: None,
            candidate_score_badges: Vec::new(),
            candidate_rank: None,
            download_id: None,
            last_error: None,
            import_event_id: None,
            imported_at: None,
            import_error: None,
            next_search_at,
            search_attempts: 0,
        }
    }

    fn test_managed_ingest_intent(
        media_type: MediaType,
        external_ids: ExternalIds,
    ) -> ManagedIngestIntent {
        let now = Utc::now();
        ManagedIngestIntent {
            intent_id: Uuid::new_v4(),
            media_type,
            title: "Example".to_string(),
            normalized_title: "example".to_string(),
            year: Some(2026),
            external_ids: Some(external_ids),
            manager_provider_id: Uuid::new_v4(),
            manager_item_id: None,
            manager_label: None,
            source: "test".to_string(),
            active: true,
            last_matched_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn test_anizip_mapping(
        ids: ExternalIds,
        episodes: &[(Option<i32>, Option<i32>, Option<i32>)],
    ) -> AniZipMapping {
        AniZipMapping {
            ids,
            episodes: episodes
                .iter()
                .map(|(season_number, episode_number, absolute_episode_number)| {
                    AniZipEpisodeRecord {
                        season_number: *season_number,
                        episode_number: *episode_number,
                        absolute_episode_number: *absolute_episode_number,
                        title: None,
                        overview: None,
                        runtime_minutes: None,
                        image: None,
                        tvdb_id: None,
                        anidb_eid: None,
                        raw: json!({
                            "seasonNumber": season_number,
                            "episodeNumber": episode_number,
                            "absoluteEpisodeNumber": absolute_episode_number,
                        }),
                    }
                })
                .collect(),
            images: Vec::new(),
            titles: HashMap::new(),
        }
    }

    fn test_route_option(
        logical_id: &str,
        available: bool,
        blocker: Option<&str>,
    ) -> TorrentioRouteOption {
        TorrentioRouteOption {
            logical_id: logical_id.to_string(),
            label: logical_id.to_string(),
            description: String::new(),
            binding_kind: if logical_id == DEBRID_DEFAULT_LOGICAL_ID {
                DownloadBrokerBindingKind::Debrid
            } else {
                DownloadBrokerBindingKind::ManagedProtected
            },
            selected_provider_id: available.then(Uuid::new_v4),
            selected_extension_id: None,
            available,
            needs_account: false,
            account_extension_id: None,
            blocker: blocker.map(str::to_string),
        }
    }

    fn test_candidate(
        id: &str,
        route_ids: Vec<&str>,
        seeders: Option<u32>,
        quality: Option<&str>,
        rank: usize,
    ) -> TorrentioCandidate {
        TorrentioCandidate {
            id: id.to_string(),
            source_provider: "test".to_string(),
            title: id.to_string(),
            name: None,
            description: None,
            source: "magnet:?xt=urn:btih:abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string()),
            file_index: None,
            quality: quality.map(str::to_string),
            size_bytes: None,
            seeders,
            language: None,
            cached_debrid: None,
            route_logical_ids: route_ids.into_iter().map(str::to_string).collect(),
            default_route_logical_id: None,
            score: 0,
            score_badges: Vec::new(),
            rank,
        }
    }

    #[test]
    fn addon_path_rejects_debrid_tokens() {
        let err = normalize_addon_path("providers=yts|realdebrid=secret").unwrap_err();
        assert!(err.to_string().contains("debrid account tokens"));
    }
}
