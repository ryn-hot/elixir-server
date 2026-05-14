use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use serde_json::{Value as JsonValue, json};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    acquisition::release_resolution::{
        anime::{
            AnimeCandidateInput, AnimeCandidateScore, AnimeCandidateScoringContext,
            AnimeCandidateTarget, AnimeFileCoveragePlan, AnimeMetadataGraphInput,
            AnimeReleaseFileInput, AnimeSeasonMapping, build_anime_metadata_graph,
            infer_anizip_season_number, plan_anime_file_coverage, score_anime_candidate,
        },
        fingerprint::candidate_release_fingerprint,
        models::{
            AcquisitionReleaseState, NewAcquisitionAnimeCandidateParse,
            NewAcquisitionAnimeGraphSnapshot, NewAcquisitionRelease, NewAcquisitionReleaseCoverage,
            NewAcquisitionReleaseFile, NewAcquisitionReleaseJob, ReleaseConfidence,
            ReleaseCoverageState, ReleaseJobState, ReleaseKind, ReleaseResolverKind,
        },
        store::{
            count_active_release_jobs_by_route, count_active_release_jobs_by_subscription_route,
            get_release_by_fingerprint, list_release_coverage, upsert_anime_candidate_parse,
            upsert_anime_graph_snapshot, upsert_release, upsert_release_coverage,
            upsert_release_file, upsert_release_job,
        },
    },
    acquisition::subscriptions::{
        AcquisitionRoutePolicy, AcquisitionSubscription, AcquisitionSubscriptionFilter,
        AcquisitionTarget, AcquisitionTargetState, AcquisitionTargetStateUpdate,
        NewAcquisitionTarget, get_target, list_due_candidate_targets,
        list_due_metadata_subscriptions, list_submitted_debrid_targets, list_subscription_targets,
        list_subscriptions, record_metadata_refresh, update_subscription_external_ids,
        update_target_state, upsert_subscription_targets,
    },
    db::models::{MediaType, ProviderHealthState},
    debrid::get_debrid_job_status,
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, DEFAULT_ROUTE_OWNER_ID, TORRENT_DEFAULT_LOGICAL_ID,
    },
    extensions::{ExternalIds, store::ExtensionStore},
    http::{
        error::ApiError,
        handlers::{
            acquisition_sources::{
                ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY, AcquisitionCandidate,
                CandidateScoreBadge, CandidateSearchIntent, CandidateSearchPreferences,
                CandidateSearchRequest, CandidateSearchTarget, search_candidates_with_store,
            },
            download_broker::{DownloadBrokerSubmitRequest, submit_to_broker},
        },
    },
    library::{AniListSeasonChainEntry, resolve_anilist_season_chain},
    state::AppState,
};

const AUTOMATION_POLL_INTERVAL_SECONDS: u64 = 60;
const METADATA_REFRESH_BASE_SECONDS: i64 = 6 * 60 * 60;
const METADATA_REFRESH_JITTER_SECONDS: i64 = 6 * 60 * 60;
const CANDIDATE_SEARCH_BASE_SECONDS: i64 = 10 * 60;
const CANDIDATE_SEARCH_JITTER_SECONDS: i64 = 5 * 60;
const HOT_RETRY_INTERVAL_SECONDS: i64 = 15 * 60;
const HOT_RETRY_WINDOW_SECONDS: i64 = 2 * 60 * 60;
const WARM_RETRY_INTERVAL_SECONDS: i64 = 60 * 60;
const WARM_RETRY_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const COLD_RETRY_INTERVAL_SECONDS: i64 = 24 * 60 * 60;
const METADATA_BATCH_LIMIT: i64 = 5;
const SEARCH_BATCH_LIMIT: i64 = 20;
const FALLBACK_BATCH_LIMIT: i64 = 50;
const DEFAULT_CANDIDATE_LIMIT: u32 = 25;
const DEFAULT_GLOBAL_DEBRID_RELEASE_JOB_CAP: i64 = 10;
const DEFAULT_SUBSCRIPTION_DEBRID_RELEASE_JOB_CAP: i64 = 3;
const DEFAULT_GLOBAL_TORRENT_RELEASE_JOB_CAP: i64 = 5;
const DEFAULT_SUBSCRIPTION_TORRENT_RELEASE_JOB_CAP: i64 = 2;
const DEFAULT_STAGED_INSPECTION_JOB_CAP: i64 = 10;
const PACK_BACKFILL_TARGET_THRESHOLD: usize = 3;
const QUEUE_CAPACITY_RETRY_SECONDS: i64 = 5 * 60;

#[derive(Debug, Clone)]
struct CandidateSubmission {
    provider_id: Uuid,
    source_extension_id: String,
    candidate: AcquisitionCandidate,
    anime_coverage_plan: Option<AnimeFileCoveragePlan>,
}

#[derive(Debug, Clone)]
struct CandidateSelection {
    candidate: AcquisitionCandidate,
    anime_coverage_plan: Option<AnimeFileCoveragePlan>,
}

#[derive(Debug, Clone)]
struct TargetSearchGroup {
    representative: AcquisitionTarget,
    targets: Vec<AcquisitionTarget>,
    search_intent: Option<CandidateSearchIntent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetryBucket {
    NoAirTime,
    Hot,
    Warm,
    Cold,
}

impl RetryBucket {
    fn as_str(self) -> &'static str {
        match self {
            Self::NoAirTime => "no_air_time",
            Self::Hot => "hot",
            Self::Warm => "warm",
            Self::Cold => "cold",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AnimeTargetGroupKey {
    subscription_id: Uuid,
    season_number: Option<i32>,
    retry_bucket: RetryBucket,
}

#[derive(Debug, Clone, Copy)]
struct SelectionPreference {
    prefer_packs_for_backfill: bool,
}

impl SelectionPreference {
    fn for_group(group: &TargetSearchGroup) -> Self {
        Self {
            prefer_packs_for_backfill: group.targets.len() >= PACK_BACKFILL_TARGET_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct QueueGovernorCaps {
    global_debrid: i64,
    subscription_debrid: i64,
    global_torrent: i64,
    subscription_torrent: i64,
    #[allow(dead_code)]
    staged_inspection: i64,
}

impl Default for QueueGovernorCaps {
    fn default() -> Self {
        Self {
            global_debrid: DEFAULT_GLOBAL_DEBRID_RELEASE_JOB_CAP,
            subscription_debrid: DEFAULT_SUBSCRIPTION_DEBRID_RELEASE_JOB_CAP,
            global_torrent: DEFAULT_GLOBAL_TORRENT_RELEASE_JOB_CAP,
            subscription_torrent: DEFAULT_SUBSCRIPTION_TORRENT_RELEASE_JOB_CAP,
            staged_inspection: DEFAULT_STAGED_INSPECTION_JOB_CAP,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueCapacityBlock {
    route_logical_id: String,
    global_active: i64,
    global_limit: i64,
    subscription_active: i64,
    subscription_limit: i64,
}

#[derive(Debug, Clone)]
enum CandidateSubmitOutcome {
    Submitted,
    CapacityBlocked(QueueCapacityBlock),
}

#[derive(Debug, Clone)]
struct QueueGovernor {
    caps: QueueGovernorCaps,
    active_by_route: HashMap<String, i64>,
    active_by_subscription_route: HashMap<(Uuid, String), i64>,
}

impl QueueGovernor {
    async fn load(pool: &sqlx::AnyPool) -> Result<Self> {
        let caps = QueueGovernorCaps::default();
        let mut active_by_route = HashMap::new();
        let active_by_subscription_route = HashMap::new();
        for route in [DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID] {
            active_by_route.insert(
                route.to_string(),
                count_active_release_jobs_by_route(pool, route).await?,
            );
        }
        Ok(Self {
            caps,
            active_by_route,
            active_by_subscription_route,
        })
    }

    async fn hydrate_subscription_route(
        &mut self,
        pool: &sqlx::AnyPool,
        subscription_id: Uuid,
        route_logical_id: &str,
    ) -> Result<()> {
        let key = (subscription_id, route_logical_id.to_string());
        if self.active_by_subscription_route.contains_key(&key) {
            return Ok(());
        }
        let count = count_active_release_jobs_by_subscription_route(
            pool,
            subscription_id,
            route_logical_id,
        )
        .await?;
        self.active_by_subscription_route.insert(key, count);
        Ok(())
    }

    async fn try_reserve(
        &mut self,
        pool: &sqlx::AnyPool,
        subscription_id: Uuid,
        route_logical_id: &str,
    ) -> Result<std::result::Result<(), QueueCapacityBlock>> {
        if self.route_limits(route_logical_id).is_none() {
            return Ok(Ok(()));
        }
        self.hydrate_subscription_route(pool, subscription_id, route_logical_id)
            .await?;
        Ok(self.try_reserve_loaded(subscription_id, route_logical_id))
    }

    fn try_reserve_loaded(
        &mut self,
        subscription_id: Uuid,
        route_logical_id: &str,
    ) -> std::result::Result<(), QueueCapacityBlock> {
        let route_key = route_logical_id.to_string();
        let sub_key = (subscription_id, route_key.clone());
        let Some((global_limit, subscription_limit)) = self.route_limits(route_logical_id) else {
            return Ok(());
        };
        let global_active = *self.active_by_route.get(&route_key).unwrap_or(&0);
        let subscription_active = *self
            .active_by_subscription_route
            .get(&sub_key)
            .unwrap_or(&0);
        if global_active >= global_limit || subscription_active >= subscription_limit {
            return Err(QueueCapacityBlock {
                route_logical_id: route_key,
                global_active,
                global_limit,
                subscription_active,
                subscription_limit,
            });
        }
        *self.active_by_route.entry(route_key.clone()).or_default() += 1;
        *self
            .active_by_subscription_route
            .entry(sub_key)
            .or_default() += 1;
        Ok(())
    }

    fn release_reservation(&mut self, subscription_id: Uuid, route_logical_id: &str) {
        let route_key = route_logical_id.to_string();
        if let Some(value) = self.active_by_route.get_mut(&route_key) {
            *value = (*value).saturating_sub(1);
        }
        let sub_key = (subscription_id, route_key);
        if let Some(value) = self.active_by_subscription_route.get_mut(&sub_key) {
            *value = (*value).saturating_sub(1);
        }
    }

    fn route_limits(&self, route_logical_id: &str) -> Option<(i64, i64)> {
        match route_logical_id {
            DEBRID_DEFAULT_LOGICAL_ID => {
                Some((self.caps.global_debrid, self.caps.subscription_debrid))
            }
            TORRENT_DEFAULT_LOGICAL_ID => {
                Some((self.caps.global_torrent, self.caps.subscription_torrent))
            }
            _ => None,
        }
    }
}

pub async fn start_acquisition_automation_loop(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(AUTOMATION_POLL_INTERVAL_SECONDS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        if let Err(err) = run_acquisition_automation_iteration(&state).await {
            warn!("acquisition automation pass failed: {err}");
        }
    }
}

pub(crate) async fn run_acquisition_automation_iteration(state: &AppState) -> Result<()> {
    refresh_due_metadata(state).await?;
    search_due_targets(state).await?;
    retry_failed_debrid_targets_with_torrent(state).await?;
    Ok(())
}

async fn refresh_due_metadata(state: &AppState) -> Result<()> {
    let now = Utc::now();
    let subscriptions =
        list_due_metadata_subscriptions(&state.db_pool, now, METADATA_BATCH_LIMIT).await?;
    for subscription in subscriptions {
        match expand_subscription_targets(state, &subscription, now).await {
            Ok(expansion) => {
                if !expansion.external_ids_changed.is_default() {
                    update_subscription_external_ids(
                        &state.db_pool,
                        subscription.subscription_id,
                        &expansion.external_ids_changed,
                    )
                    .await?;
                }
                if !expansion.targets.is_empty() {
                    upsert_subscription_targets(
                        &state.db_pool,
                        subscription.subscription_id,
                        expansion.targets,
                    )
                    .await?;
                }
                if let Some(graph_snapshot) = expansion.anime_graph_snapshot {
                    upsert_anime_graph_snapshot(&state.db_pool, graph_snapshot).await?;
                }
                let next_after =
                    next_metadata_refresh_after(&subscription, &expansion.season_chain, now);
                record_metadata_refresh(&state.db_pool, subscription.subscription_id, next_after)
                    .await?;
                debug!(
                    subscription_id = %subscription.subscription_id,
                    targets = expansion.target_count,
                    next_after = %next_after,
                    "refreshed acquisition metadata"
                );
            }
            Err(err) => {
                let next_after = now
                    + ChronoDuration::minutes(30)
                    + jitter_duration(&subscription.subscription_id, 15 * 60);
                record_metadata_refresh(&state.db_pool, subscription.subscription_id, next_after)
                    .await?;
                warn!(
                    subscription_id = %subscription.subscription_id,
                    "acquisition metadata refresh failed: {err}"
                );
            }
        }
    }
    Ok(())
}

async fn search_due_targets(state: &AppState) -> Result<()> {
    let now = Utc::now();
    let targets = list_due_candidate_targets(&state.db_pool, now, SEARCH_BATCH_LIMIT).await?;
    if targets.is_empty() {
        return Ok(());
    }

    let subscriptions = list_subscriptions(
        &state.db_pool,
        AcquisitionSubscriptionFilter { active: Some(true) },
    )
    .await?
    .into_iter()
    .map(|item| (item.subscription_id, item))
    .collect::<HashMap<_, _>>();

    let groups = build_due_target_search_groups(state, &subscriptions, targets, now).await?;
    let mut governor = QueueGovernor::load(&state.db_pool).await?;

    for group in groups {
        let Some(subscription) = subscriptions.get(&group.representative.subscription_id) else {
            continue;
        };
        if let Err(err) =
            search_and_submit_group(state, subscription, &group, now, &mut governor).await
        {
            let next_after = next_candidate_retry_after(subscription, &group.representative, now);
            let retry_targets = if group.targets.is_empty() {
                vec![group.representative.clone()]
            } else {
                group.targets.clone()
            };
            update_group_targets_for_retry(
                state,
                subscription,
                &retry_targets,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Pending,
                    state_reason: Some(format!("Candidate automation failed: {err}")),
                    next_search_after: Some(next_after),
                    increment_search_attempts: true,
                    ..Default::default()
                },
            )
            .await?;
            warn!(
                target_id = %group.representative.target_id,
                subscription_id = %group.representative.subscription_id,
                next_after = %next_after,
                "candidate automation failed: {err}"
            );
        }
    }
    Ok(())
}

async fn build_due_target_search_groups(
    state: &AppState,
    subscriptions: &HashMap<Uuid, AcquisitionSubscription>,
    due_targets: Vec<AcquisitionTarget>,
    now: DateTime<Utc>,
) -> Result<Vec<TargetSearchGroup>> {
    let anime_subscription_ids = due_targets
        .iter()
        .filter(|target| target.media_type == MediaType::Anime)
        .map(|target| target.subscription_id)
        .collect::<BTreeSet<_>>();
    let mut targets_by_subscription = HashMap::<Uuid, Vec<AcquisitionTarget>>::new();
    for subscription_id in anime_subscription_ids {
        targets_by_subscription.insert(
            subscription_id,
            list_subscription_targets(&state.db_pool, subscription_id).await?,
        );
    }
    Ok(build_target_search_groups(
        subscriptions,
        due_targets,
        &targets_by_subscription,
        now,
    ))
}

fn build_target_search_groups(
    subscriptions: &HashMap<Uuid, AcquisitionSubscription>,
    due_targets: Vec<AcquisitionTarget>,
    targets_by_subscription: &HashMap<Uuid, Vec<AcquisitionTarget>>,
    now: DateTime<Utc>,
) -> Vec<TargetSearchGroup> {
    let mut groups = Vec::new();
    let mut anime_due_keys = BTreeSet::<AnimeTargetGroupKey>::new();

    for target in due_targets {
        let Some(subscription) = subscriptions.get(&target.subscription_id) else {
            continue;
        };
        if target.media_type != MediaType::Anime || subscription.media_type != MediaType::Anime {
            groups.push(TargetSearchGroup {
                representative: target,
                targets: Vec::new(),
                search_intent: None,
            });
            continue;
        }
        anime_due_keys.insert(anime_target_group_key(subscription, &target, now));
    }

    for key in anime_due_keys {
        let Some(subscription) = subscriptions.get(&key.subscription_id) else {
            continue;
        };
        let source_targets = targets_by_subscription
            .get(&key.subscription_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let mut targets = source_targets
            .iter()
            .filter(|target| target_is_due_for_group(subscription, target, now))
            .filter(|target| anime_target_group_key(subscription, target, now) == key)
            .cloned()
            .collect::<Vec<_>>();
        if targets.is_empty() {
            continue;
        }
        sort_targets_for_group(&mut targets);
        let representative = targets[0].clone();
        groups.push(TargetSearchGroup {
            representative,
            search_intent: Some(search_intent_for_targets(&targets, key.retry_bucket)),
            targets,
        });
    }

    groups.sort_by(|left, right| compare_search_groups(left, right));
    groups
}

async fn search_and_submit_group(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    group: &TargetSearchGroup,
    now: DateTime<Utc>,
    governor: &mut QueueGovernor,
) -> Result<()> {
    let target = &group.representative;
    let Some(target) = get_target(&state.db_pool, target.target_id).await? else {
        return Ok(());
    };
    if !matches!(
        target.state,
        AcquisitionTargetState::Pending
            | AcquisitionTargetState::Searching
            | AcquisitionTargetState::Blocked
    ) {
        return Ok(());
    }
    update_target_state(
        &state.db_pool,
        target.target_id,
        AcquisitionTargetStateUpdate {
            state: AcquisitionTargetState::Searching,
            state_reason: Some("Searching acquisition source provider.".to_string()),
            ..Default::default()
        },
    )
    .await?;

    let request =
        candidate_search_request_for_group(subscription, &target, group.search_intent.clone());
    let response = search_candidates_with_store(&state.db_pool, request).await?;
    let group_targets = if subscription.media_type == MediaType::Anime && !group.targets.is_empty()
    {
        group.targets.clone()
    } else if subscription.media_type == MediaType::Anime {
        vec![target.clone()]
    } else {
        Vec::new()
    };
    let anime_context = anime_candidate_scoring_context(subscription, &target, &group_targets);
    let preference = SelectionPreference::for_group(group);
    let selection = select_best_candidate_with_preference(
        &response.candidates,
        subscription.route_policy,
        anime_context.as_ref(),
        preference,
    )
    .or_else(|| {
        if group_targets.len() <= 1 {
            return None;
        }
        let single_context =
            anime_candidate_scoring_context(subscription, &target, std::slice::from_ref(&target));
        select_best_candidate_with_preference(
            &response.candidates,
            subscription.route_policy,
            single_context.as_ref(),
            SelectionPreference {
                prefer_packs_for_backfill: false,
            },
        )
    });
    let Some(selection) = selection else {
        let next_after = next_candidate_retry_after(subscription, &target, now);
        let retry_targets = if group_targets.is_empty() {
            vec![target.clone()]
        } else {
            group_targets.clone()
        };
        update_group_targets_for_retry(
            state,
            subscription,
            &retry_targets,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Pending,
                state_reason: Some(format!(
                    "No matching acquisition candidates were found for {}.",
                    group
                        .search_intent
                        .as_ref()
                        .map(|intent| intent.kind.as_str())
                        .unwrap_or("target")
                )),
                selected_provider_id: Some(response.provider.provider_id),
                next_search_after: Some(next_after),
                increment_search_attempts: true,
                ..Default::default()
            },
        )
        .await?;
        return Ok(());
    };

    let submission = CandidateSubmission {
        provider_id: response.provider.provider_id,
        source_extension_id: response.provider.extension_id.clone(),
        candidate: selection.candidate,
        anime_coverage_plan: selection.anime_coverage_plan,
    };
    match submit_selected_candidate(
        state,
        subscription,
        &target,
        submission,
        None,
        Some(governor),
    )
    .await?
    {
        CandidateSubmitOutcome::Submitted => {}
        CandidateSubmitOutcome::CapacityBlocked(block) => {
            defer_group_for_queue_capacity(state, subscription, group, block, now).await?;
        }
    }
    Ok(())
}

fn anime_target_group_key(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    now: DateTime<Utc>,
) -> AnimeTargetGroupKey {
    AnimeTargetGroupKey {
        subscription_id: target.subscription_id,
        season_number: target.season_number,
        retry_bucket: retry_bucket_for_target(subscription, target, now),
    }
}

fn retry_bucket_for_target(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    now: DateTime<Utc>,
) -> RetryBucket {
    let Some(air_time) = target.air_time else {
        return RetryBucket::NoAirTime;
    };
    let release_time = air_time + ChronoDuration::seconds(subscription.release_delay_seconds);
    let elapsed = now - release_time;
    if elapsed < ChronoDuration::seconds(HOT_RETRY_WINDOW_SECONDS) {
        RetryBucket::Hot
    } else if elapsed < ChronoDuration::seconds(WARM_RETRY_WINDOW_SECONDS) {
        RetryBucket::Warm
    } else {
        RetryBucket::Cold
    }
}

fn target_is_due_for_group(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    now: DateTime<Utc>,
) -> bool {
    if !matches!(
        target.state,
        AcquisitionTargetState::Pending
            | AcquisitionTargetState::Searching
            | AcquisitionTargetState::Blocked
    ) {
        return false;
    }
    if target.air_time.is_some_and(|air_time| air_time > now) {
        return false;
    }
    target
        .next_search_after
        .or(Some(subscription.candidate_search_after))
        .is_none_or(|next_after| next_after <= now)
}

fn sort_targets_for_group(targets: &mut [AcquisitionTarget]) {
    targets.sort_by_key(|target| {
        (
            target.season_number.unwrap_or(i32::MAX),
            target.episode_number.unwrap_or(i32::MAX),
            target.absolute_episode_number.unwrap_or(i32::MAX),
            target.target_key.clone(),
        )
    });
}

fn compare_search_groups(left: &TargetSearchGroup, right: &TargetSearchGroup) -> Ordering {
    left.representative
        .next_search_after
        .cmp(&right.representative.next_search_after)
        .then_with(|| {
            left.representative
                .subscription_id
                .cmp(&right.representative.subscription_id)
        })
        .then_with(|| {
            left.representative
                .season_number
                .unwrap_or(i32::MAX)
                .cmp(&right.representative.season_number.unwrap_or(i32::MAX))
        })
        .then_with(|| {
            left.representative
                .episode_number
                .unwrap_or(i32::MAX)
                .cmp(&right.representative.episode_number.unwrap_or(i32::MAX))
        })
        .then_with(|| {
            left.representative
                .target_key
                .cmp(&right.representative.target_key)
        })
}

fn search_intent_for_targets(
    targets: &[AcquisitionTarget],
    retry_bucket: RetryBucket,
) -> CandidateSearchIntent {
    let season_number = common_season_number(targets);
    let mut sorted_targets = targets.iter().collect::<Vec<_>>();
    sorted_targets.sort_by_key(|target| {
        (
            target.season_number.unwrap_or(i32::MAX),
            target.episode_number.unwrap_or(i32::MAX),
            target.absolute_episode_number.unwrap_or(i32::MAX),
            target.target_key.clone(),
        )
    });
    let mut target_keys = sorted_targets
        .into_iter()
        .map(|target| target.target_key.clone())
        .collect::<Vec<_>>();
    target_keys.dedup();
    CandidateSearchIntent {
        kind: search_intent_kind(targets, season_number).to_string(),
        season_number,
        episode_start: targets
            .iter()
            .filter_map(|target| target.episode_number)
            .min(),
        episode_end: targets
            .iter()
            .filter_map(|target| target.episode_number)
            .max(),
        absolute_episode_start: targets
            .iter()
            .filter_map(|target| target.absolute_episode_number)
            .min(),
        absolute_episode_end: targets
            .iter()
            .filter_map(|target| target.absolute_episode_number)
            .max(),
        target_count: u32::try_from(targets.len()).unwrap_or(u32::MAX),
        target_keys,
        retry_bucket: Some(retry_bucket.as_str().to_string()),
    }
}

fn search_intent_kind(targets: &[AcquisitionTarget], season_number: Option<i32>) -> &'static str {
    if targets.len() <= 1 {
        return "episode";
    }
    if season_number.is_some() && targets.len() >= PACK_BACKFILL_TARGET_THRESHOLD {
        return "season_pack";
    }
    if targets.len() >= PACK_BACKFILL_TARGET_THRESHOLD {
        "anime_batch"
    } else {
        "multi_episode"
    }
}

fn common_season_number(targets: &[AcquisitionTarget]) -> Option<i32> {
    let mut seasons = targets
        .iter()
        .filter_map(|target| target.season_number)
        .collect::<BTreeSet<_>>();
    if seasons.len() == 1 {
        seasons.pop_first()
    } else {
        None
    }
}

async fn update_group_targets_for_retry(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    targets: &[AcquisitionTarget],
    update: AcquisitionTargetStateUpdate,
) -> Result<()> {
    let Some(first) = targets.first() else {
        return Ok(());
    };
    for target in targets {
        let next_search_after = update
            .next_search_after
            .clone()
            .or_else(|| Some(next_candidate_retry_after(subscription, target, Utc::now())));
        update_target_state(
            &state.db_pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                next_search_after,
                increment_search_attempts: target.target_id == first.target_id
                    && update.increment_search_attempts,
                ..update.clone()
            },
        )
        .await?;
    }
    Ok(())
}

async fn defer_group_for_queue_capacity(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    group: &TargetSearchGroup,
    block: QueueCapacityBlock,
    now: DateTime<Utc>,
) -> Result<()> {
    let next_after = now
        + jittered_seconds(
            &group.representative.target_id,
            QUEUE_CAPACITY_RETRY_SECONDS,
            120,
        );
    let targets = if group.targets.is_empty() {
        vec![group.representative.clone()]
    } else {
        group.targets.clone()
    };
    update_group_targets_for_retry(
        state,
        subscription,
        &targets,
        AcquisitionTargetStateUpdate {
            state: AcquisitionTargetState::Pending,
            state_reason: Some(queue_capacity_reason(&block)),
            next_search_after: Some(next_after),
            increment_search_attempts: false,
            ..Default::default()
        },
    )
    .await
}

fn queue_capacity_reason(block: &QueueCapacityBlock) -> String {
    format!(
        "Queue capacity reached for {}: global {}/{}, subscription {}/{}.",
        block.route_logical_id,
        block.global_active,
        block.global_limit,
        block.subscription_active,
        block.subscription_limit
    )
}

async fn retry_failed_debrid_targets_with_torrent(state: &AppState) -> Result<()> {
    let targets = list_submitted_debrid_targets(&state.db_pool, FALLBACK_BATCH_LIMIT).await?;
    if targets.is_empty() {
        return Ok(());
    }
    let mut governor = QueueGovernor::load(&state.db_pool).await?;
    let subscriptions = list_subscriptions(
        &state.db_pool,
        AcquisitionSubscriptionFilter { active: Some(true) },
    )
    .await?
    .into_iter()
    .map(|item| (item.subscription_id, item))
    .collect::<HashMap<_, _>>();

    for target in targets {
        let Some(download_id) = target.download_id.as_deref() else {
            continue;
        };
        let Ok(job_id) = Uuid::parse_str(download_id) else {
            continue;
        };
        let Some(status) = get_debrid_job_status(&state.db_pool, job_id).await? else {
            continue;
        };
        if !debrid_status_failed(&status.status) {
            continue;
        }
        let Some(subscription) = subscriptions.get(&target.subscription_id) else {
            continue;
        };
        if subscription.route_policy != AcquisitionRoutePolicy::DebridFirst {
            continue;
        }
        let Some(candidate_value) = target.selected_candidate.clone() else {
            continue;
        };
        let candidate: AcquisitionCandidate = serde_json::from_value(candidate_value)
            .context("parsing selected acquisition candidate for debrid fallback")?;
        if !candidate_supports_route(&candidate, TORRENT_DEFAULT_LOGICAL_ID) {
            update_target_state(
                &state.db_pool,
                target.target_id,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Blocked,
                    state_reason: Some(format!(
                        "Real-Debrid failed and the selected candidate has no torrent fallback: {}",
                        status.last_error.unwrap_or_else(|| status.status.clone())
                    )),
                    next_search_after: Some(
                        Utc::now()
                            + jittered_seconds(&target.target_id, WARM_RETRY_INTERVAL_SECONDS, 300),
                    ),
                    ..Default::default()
                },
            )
            .await?;
            continue;
        }
        let provider_id = target
            .selected_provider_id
            .or(subscription.source_provider_id)
            .ok_or_else(|| anyhow!("source provider is missing for torrent fallback"))?;
        let source_extension_id = source_extension_id_for_candidate_provider(
            &ExtensionStore::new(&state.db_pool),
            provider_id,
        )
        .await?;
        let submission = CandidateSubmission {
            provider_id,
            source_extension_id,
            candidate,
            anime_coverage_plan: None,
        };
        match submit_selected_candidate(
            state,
            subscription,
            &target,
            submission,
            Some(TORRENT_DEFAULT_LOGICAL_ID),
            Some(&mut governor),
        )
        .await
        {
            Ok(CandidateSubmitOutcome::Submitted) => info!(
                target_id = %target.target_id,
                debrid_job_id = %status.job_id,
                "submitted torrent fallback after Real-Debrid failure"
            ),
            Ok(CandidateSubmitOutcome::CapacityBlocked(block)) => {
                let next_after = Utc::now()
                    + jittered_seconds(&target.target_id, QUEUE_CAPACITY_RETRY_SECONDS, 120);
                update_target_state(
                    &state.db_pool,
                    target.target_id,
                    AcquisitionTargetStateUpdate {
                        state: AcquisitionTargetState::Blocked,
                        state_reason: Some(queue_capacity_reason(&block)),
                        next_search_after: Some(next_after),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Err(err) => {
                let next_after = Utc::now()
                    + jittered_seconds(&target.target_id, WARM_RETRY_INTERVAL_SECONDS, 300);
                update_target_state(
                    &state.db_pool,
                    target.target_id,
                    AcquisitionTargetStateUpdate {
                        state: AcquisitionTargetState::Blocked,
                        state_reason: Some(format!(
                            "Real-Debrid failed and torrent fallback is blocked: {err}"
                        )),
                        next_search_after: Some(next_after),
                        ..Default::default()
                    },
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn submit_selected_candidate(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    submission: CandidateSubmission,
    route_override: Option<&str>,
    mut governor: Option<&mut QueueGovernor>,
) -> Result<CandidateSubmitOutcome> {
    let route_logical_id = select_candidate_route(
        route_override,
        subscription.route_policy,
        &submission.candidate,
    )?;
    if submission.anime_coverage_plan.is_some()
        && let Some(download_id) = existing_anime_release_download_id(state, &submission).await?
    {
        persist_anime_release_submission(
            state,
            subscription,
            target,
            &submission,
            &route_logical_id,
            Some(download_id),
            "Reused existing pack-aware acquisition release.",
        )
        .await?;
        return Ok(CandidateSubmitOutcome::Submitted);
    }
    let governed_submission = submission.anime_coverage_plan.is_some() || route_override.is_some();
    let mut reserved_route = None::<String>;
    if governed_submission && let Some(governor) = governor.as_deref_mut() {
        match governor
            .try_reserve(
                &state.db_pool,
                subscription.subscription_id,
                &route_logical_id,
            )
            .await?
        {
            Ok(()) => reserved_route = Some(route_logical_id.clone()),
            Err(block) => return Ok(CandidateSubmitOutcome::CapacityBlocked(block)),
        }
    }
    match submit_candidate_to_route(state, target, &submission, &route_logical_id).await {
        Ok(download_id) => {
            let reason = if route_override == Some(TORRENT_DEFAULT_LOGICAL_ID) {
                "Submitted through torrent fallback."
            } else {
                "Submitted through acquisition route."
            };
            if submission.anime_coverage_plan.is_some() {
                persist_anime_release_submission(
                    state,
                    subscription,
                    target,
                    &submission,
                    &route_logical_id,
                    download_id,
                    reason,
                )
                .await?;
            } else {
                mark_target_submitted(
                    state,
                    target,
                    &submission,
                    &route_logical_id,
                    download_id,
                    reason,
                )
                .await?;
            }
            Ok(CandidateSubmitOutcome::Submitted)
        }
        Err(err)
            if route_logical_id == DEBRID_DEFAULT_LOGICAL_ID
                && subscription.route_policy == AcquisitionRoutePolicy::DebridFirst
                && candidate_supports_route(&submission.candidate, TORRENT_DEFAULT_LOGICAL_ID) =>
        {
            if let Some(route) = reserved_route.take() {
                if let Some(governor) = governor.as_deref_mut() {
                    governor.release_reservation(subscription.subscription_id, &route);
                }
            }
            if governed_submission && let Some(governor) = governor.as_deref_mut() {
                match governor
                    .try_reserve(
                        &state.db_pool,
                        subscription.subscription_id,
                        TORRENT_DEFAULT_LOGICAL_ID,
                    )
                    .await?
                {
                    Ok(()) => reserved_route = Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
                    Err(block) => return Ok(CandidateSubmitOutcome::CapacityBlocked(block)),
                }
            }
            warn!(
                target_id = %target.target_id,
                "debrid submission failed, trying torrent fallback: {err}"
            );
            let torrent_download_id = match submit_candidate_to_route(
                state,
                target,
                &submission,
                TORRENT_DEFAULT_LOGICAL_ID,
            )
            .await
            {
                Ok(download_id) => download_id,
                Err(fallback_err) => {
                    if let Some(route) = reserved_route.take() {
                        if let Some(governor) = governor.as_deref_mut() {
                            governor.release_reservation(subscription.subscription_id, &route);
                        }
                    }
                    let next_after = next_candidate_retry_after(subscription, target, Utc::now());
                    update_target_state(
                        &state.db_pool,
                        target.target_id,
                        AcquisitionTargetStateUpdate {
                            state: AcquisitionTargetState::Blocked,
                            state_reason: Some(format!(
                                "Real-Debrid route failed: {err}; torrent fallback failed: {fallback_err}"
                            )),
                            selected_provider_id: Some(submission.provider_id),
                            selected_route_logical_id: Some(
                                TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                            ),
                            selected_candidate: Some(selected_candidate_provenance(&submission)?),
                            next_search_after: Some(next_after),
                            increment_search_attempts: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                    return Ok(CandidateSubmitOutcome::Submitted);
                }
            };
            if submission.anime_coverage_plan.is_some() {
                persist_anime_release_submission(
                    state,
                    subscription,
                    target,
                    &submission,
                    TORRENT_DEFAULT_LOGICAL_ID,
                    torrent_download_id,
                    "Real-Debrid rejected the candidate; submitted torrent fallback.",
                )
                .await?;
            } else {
                mark_target_submitted(
                    state,
                    target,
                    &submission,
                    TORRENT_DEFAULT_LOGICAL_ID,
                    torrent_download_id,
                    "Real-Debrid rejected the candidate; submitted torrent fallback.",
                )
                .await?;
            }
            Ok(CandidateSubmitOutcome::Submitted)
        }
        Err(err) => {
            if let Some(route) = reserved_route.take() {
                if let Some(governor) = governor.as_deref_mut() {
                    governor.release_reservation(subscription.subscription_id, &route);
                }
            }
            let next_after = next_candidate_retry_after(subscription, target, Utc::now());
            update_target_state(
                &state.db_pool,
                target.target_id,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Blocked,
                    state_reason: Some(format!("Acquisition route blocked: {err}")),
                    selected_provider_id: Some(submission.provider_id),
                    selected_route_logical_id: Some(route_logical_id),
                    selected_candidate: Some(selected_candidate_provenance(&submission)?),
                    next_search_after: Some(next_after),
                    increment_search_attempts: true,
                    ..Default::default()
                },
            )
            .await?;
            Ok(CandidateSubmitOutcome::Submitted)
        }
    }
}

async fn submit_candidate_to_route(
    state: &AppState,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
) -> Result<Option<String>> {
    let store = ExtensionStore::new(&state.db_pool);
    let request = DownloadBrokerSubmitRequest {
        source: submission.candidate.source.clone(),
        category: None,
        paused: Some(false),
        name: Some(download_display_name(target, &submission.candidate)),
        priority: None,
        add_to_top: None,
    };
    let response = submit_to_broker(
        state,
        &store,
        route_logical_id,
        Some(&submission.source_extension_id),
        request,
    )
    .await
    .map_err(api_error_to_anyhow)?;
    Ok(response
        .download_id
        .or_else(|| submission.candidate.info_hash.clone()))
}

async fn mark_target_submitted(
    state: &AppState,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
    download_id: Option<String>,
    reason: &str,
) -> Result<()> {
    update_target_state(
        &state.db_pool,
        target.target_id,
        AcquisitionTargetStateUpdate {
            state: AcquisitionTargetState::Submitted,
            state_reason: Some(format!("{reason} {route_logical_id}")),
            selected_provider_id: Some(submission.provider_id),
            selected_route_logical_id: Some(route_logical_id.to_string()),
            selected_candidate: Some(selected_candidate_provenance(submission)?),
            download_id,
            next_search_after: None,
            increment_search_attempts: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn existing_anime_release_download_id(
    state: &AppState,
    submission: &CandidateSubmission,
) -> Result<Option<String>> {
    let fingerprint =
        candidate_release_fingerprint(&submission.candidate, Some(submission.provider_id));
    let Some(release) = get_release_by_fingerprint(
        &state.db_pool,
        DEFAULT_ROUTE_OWNER_ID,
        &submission.source_extension_id,
        &fingerprint,
    )
    .await?
    else {
        return Ok(None);
    };
    if matches!(
        release.state,
        AcquisitionReleaseState::Failed | AcquisitionReleaseState::Cancelled
    ) {
        return Ok(None);
    }
    Ok(release.download_id)
}

async fn persist_anime_release_submission(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
    download_id: Option<String>,
    reason: &str,
) -> Result<()> {
    let Some(plan) = submission.anime_coverage_plan.as_ref() else {
        mark_target_submitted(
            state,
            target,
            submission,
            route_logical_id,
            download_id,
            reason,
        )
        .await?;
        return Ok(());
    };
    let fingerprint =
        candidate_release_fingerprint(&submission.candidate, Some(submission.provider_id));
    let selected_candidate = selected_candidate_provenance(submission)?;
    let release = upsert_release(
        &state.db_pool,
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: Some(subscription.subscription_id),
            source_provider_id: Some(submission.provider_id),
            source_extension_id: submission.source_extension_id.clone(),
            owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
            media_type: target.media_type,
            title: subscription.title.clone(),
            release_title: submission.candidate.title.clone(),
            source: submission.candidate.source.clone(),
            source_kind: submission.candidate.source_kind.clone(),
            info_hash: submission.candidate.info_hash.clone(),
            fingerprint: fingerprint.clone(),
            release_kind: plan.release_kind,
            resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
            resolver_version: plan.resolver_version.clone(),
            confidence: plan.confidence,
            score: submission.candidate.score,
            selected_route_logical_id: Some(route_logical_id.to_string()),
            selected_provider_id: Some(submission.provider_id),
            download_id: download_id.clone(),
            remote_release_id: None,
            state: AcquisitionReleaseState::Submitted,
            state_reason: Some(format!(
                "{reason} pack-aware coverage entries: {}",
                plan.entries.len()
            )),
            selected_candidate: Some(selected_candidate.clone()),
            coverage_plan: Some(serde_json::to_value(plan)?),
        },
    )
    .await?;

    let parsed = crate::acquisition::release_resolution::anime::parse_anime_release_title(
        &submission.candidate.title,
    );
    upsert_anime_candidate_parse(
        &state.db_pool,
        NewAcquisitionAnimeCandidateParse {
            candidate_parse_id: None,
            release_id: release.release_id,
            source_provider_id: Some(submission.provider_id),
            source_candidate_id: submission.candidate.id.clone(),
            release_title: submission.candidate.title.clone(),
            normalized_title: parsed.normalized_title.clone(),
            parsed: serde_json::to_value(&parsed)?,
            confidence: plan.confidence,
            review_reasons: json!(plan.review_reasons),
        },
    )
    .await?;

    let mut file_ids_by_key = HashMap::new();
    for file in anime_release_file_inputs(&submission.candidate) {
        let parsed =
            crate::acquisition::release_resolution::anime::parse_anime_release_title(&file.path);
        let release_file = upsert_release_file(
            &state.db_pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: file.file_index,
                file_id: file.file_id.clone(),
                path: file.path.clone(),
                basename: None,
                size_bytes: file.size_bytes,
                selectable: file.selectable,
                parsed_title: parsed.series_title.clone(),
                parsed_season_number: parsed.season_number,
                parsed_episode_number: parsed.episode_start_number,
                parsed_episode_end_number: parsed.episode_end_number,
                parsed_absolute_episode_number: parsed.absolute_episode_numbers.first().copied(),
                parsed_absolute_episode_end_number: parsed.absolute_episode_numbers.last().copied(),
                parsed_air_date: None,
                parsed_quality: parsed.quality.resolution.clone(),
                parsed_language: parsed
                    .subtitle_languages
                    .first()
                    .cloned()
                    .or_else(|| parsed.audio_languages.first().cloned()),
                parsed_release_group: parsed.release_group.clone(),
                parser_confidence: parsed.confidence,
                parser_reason: (!parsed.review_reasons.is_empty())
                    .then(|| parsed.review_reasons.join(",")),
                raw: Some(json!({
                    "fileKey": file.file_key.clone(),
                    "parsed": parsed,
                })),
            },
        )
        .await?;
        file_ids_by_key.insert(file.file_key, release_file.release_file_id);
    }

    let targets = list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
    let targets_by_key = targets
        .into_iter()
        .map(|target| (target.target_key.clone(), target))
        .collect::<HashMap<_, _>>();
    let mut submitted_target_ids = BTreeSet::new();
    for entry in &plan.entries {
        let Some(covered_target) = targets_by_key.get(&entry.target_key) else {
            continue;
        };
        let release_file_id = entry
            .release_file_key
            .as_ref()
            .and_then(|key| file_ids_by_key.get(key))
            .copied();
        upsert_release_coverage(
            &state.db_pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id,
                target_id: covered_target.target_id,
                coverage_kind: entry.coverage_kind,
                confidence: entry.confidence,
                score: entry.score,
                reason: Some(entry.reason.clone()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("rr3f_file_list".to_string()),
            },
        )
        .await?;
        submitted_target_ids.insert(covered_target.target_id);
        update_target_state(
            &state.db_pool,
            covered_target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some(format!("{reason} {route_logical_id}")),
                selected_provider_id: Some(submission.provider_id),
                selected_route_logical_id: Some(route_logical_id.to_string()),
                selected_candidate: Some(selected_candidate.clone()),
                download_id: download_id.clone(),
                next_search_after: None,
                increment_search_attempts: covered_target.target_id == target.target_id,
                ..Default::default()
            },
        )
        .await?;
    }

    if submitted_target_ids.is_empty() {
        mark_target_submitted(
            state,
            target,
            submission,
            route_logical_id,
            download_id.clone(),
            reason,
        )
        .await?;
    }

    upsert_release_job(
        &state.db_pool,
        NewAcquisitionReleaseJob {
            release_job_id: None,
            release_id: release.release_id,
            route_logical_id: route_logical_id.to_string(),
            provider_id: Some(submission.provider_id),
            download_id,
            remote_release_id: None,
            state: ReleaseJobState::Submitted,
            state_reason: Some(reason.to_string()),
            active: true,
            started_at: Some(Utc::now()),
            completed_at: None,
        },
    )
    .await?;
    let coverages = list_release_coverage(&state.db_pool, release.release_id).await?;
    debug!(
        release_id = %release.release_id,
        coverage = coverages.len(),
        "persisted RR-3F anime release coverage"
    );
    Ok(())
}

#[derive(Debug, Default)]
struct SubscriptionExpansion {
    targets: Vec<NewAcquisitionTarget>,
    target_count: usize,
    external_ids_changed: ExternalIds,
    season_chain: Vec<AniListSeasonChainEntry>,
    anime_graph_snapshot: Option<NewAcquisitionAnimeGraphSnapshot>,
}

async fn expand_subscription_targets(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    now: DateTime<Utc>,
) -> Result<SubscriptionExpansion> {
    if subscription.monitor_policy
        == crate::acquisition::subscriptions::AcquisitionMonitorPolicy::SelectedTargets
    {
        return Ok(SubscriptionExpansion::default());
    }

    let original_ids = subscription.external_ids.clone().unwrap_or_default();
    let mut ids = original_ids.clone();
    let (mut targets, anime_graph_snapshot) = match subscription.media_type {
        MediaType::Movie => (expand_movie_subscription(subscription, &ids, now), None),
        MediaType::Series => (
            expand_series_subscription(state, subscription, &mut ids, now).await?,
            None,
        ),
        MediaType::Anime => expand_anime_subscription(state, subscription, &mut ids, now).await?,
    };
    let season_chain = targets
        .iter()
        .filter_map(|target| {
            target
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("anilistSeason"))
                .and_then(|value| {
                    serde_json::from_value::<AniListSeasonChainEntryEnvelope>(value.clone()).ok()
                })
                .map(Into::into)
        })
        .collect::<Vec<_>>();
    targets.retain(|target| target_allowed(subscription, target, now));
    let target_count = targets.len();
    Ok(SubscriptionExpansion {
        targets,
        target_count,
        external_ids_changed: if ids != original_ids {
            ids
        } else {
            ExternalIds::default()
        },
        season_chain,
        anime_graph_snapshot,
    })
}

fn expand_movie_subscription(
    subscription: &AcquisitionSubscription,
    ids: &ExternalIds,
    now: DateTime<Utc>,
) -> Vec<NewAcquisitionTarget> {
    vec![NewAcquisitionTarget {
        target_key: Some("movie".to_string()),
        media_type: Some(MediaType::Movie),
        title: Some(subscription.title.clone()),
        season_number: None,
        episode_number: None,
        absolute_episode_number: None,
        air_date: None,
        air_time: None,
        metadata: Some(json!({
            "source": "subscription",
            "externalIds": ids,
        })),
        state: Some(AcquisitionTargetState::Pending),
        next_search_after: Some(now),
    }]
}

async fn expand_series_subscription(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    ids: &mut ExternalIds,
    now: DateTime<Utc>,
) -> Result<Vec<NewAcquisitionTarget>> {
    let tvdb_series_id = resolve_tvdb_series_id(state, ids).await?;
    let Some(tvdb_series_id) = tvdb_series_id else {
        return Ok(Vec::new());
    };
    ids.tvdb_series = Some(tvdb_series_id.clone());
    if ids.tvdb.is_none() {
        ids.tvdb = Some(tvdb_series_id.clone());
    }

    let seasons = state
        .linkers
        .fetch_tvdb_series_seasons(&tvdb_series_id)
        .await
        .unwrap_or_default();
    let mut season_numbers = seasons
        .iter()
        .filter_map(extract_season_number)
        .filter(|season| *season > 0)
        .collect::<Vec<_>>();
    season_numbers.sort_unstable();
    season_numbers.dedup();

    let mut targets = Vec::new();
    for season_number in season_numbers {
        let episodes = state
            .linkers
            .fetch_tvdb_season_episodes(&tvdb_series_id, season_number)
            .await
            .unwrap_or_default();
        for episode in episodes {
            let Some(episode_number) = episode.episode_number else {
                continue;
            };
            if episode_number <= 0 {
                continue;
            }
            let air_date = extract_air_date(&episode.raw);
            let air_time = air_date
                .as_deref()
                .and_then(|value| parse_air_time(value).or_else(|| parse_air_date(value)));
            targets.push(NewAcquisitionTarget {
                target_key: None,
                media_type: Some(MediaType::Series),
                title: episode
                    .title
                    .clone()
                    .or_else(|| Some(subscription.title.clone())),
                season_number: Some(season_number),
                episode_number: Some(episode_number),
                absolute_episode_number: episode.absolute_number,
                air_date,
                air_time,
                metadata: Some(json!({
                    "source": "tvdb",
                    "externalIds": ids,
                    "tvdbEpisodeId": episode.tvdb_episode_id,
                    "raw": episode.raw,
                })),
                state: Some(AcquisitionTargetState::Pending),
                next_search_after: Some(next_search_after_for_air_time(
                    air_time,
                    subscription.release_delay_seconds,
                    now,
                )),
            });
        }
    }
    Ok(targets)
}

async fn expand_anime_subscription(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    ids: &mut ExternalIds,
    now: DateTime<Utc>,
) -> Result<(
    Vec<NewAcquisitionTarget>,
    Option<NewAcquisitionAnimeGraphSnapshot>,
)> {
    let Some(seed_anilist_id) = ids.anilist.clone() else {
        return Ok((
            expand_series_subscription(state, subscription, ids, now).await?,
            None,
        ));
    };

    let seed_mapping = state
        .linkers
        .fetch_anizip_mapping(&seed_anilist_id)
        .await
        .unwrap_or(None);
    let seed_season = seed_mapping
        .as_ref()
        .and_then(infer_anizip_season_number)
        .unwrap_or(1);
    let mut season_chain = resolve_anilist_season_chain(
        Some(&state.settings.classifier),
        seed_season,
        &seed_anilist_id,
        1.0,
    )
    .await
    .unwrap_or_default();
    if season_chain.is_empty() {
        season_chain.push(AniListSeasonChainEntry {
            season_number: seed_season,
            anilist_id: seed_anilist_id.clone(),
            title: subscription.title.clone(),
            format: None,
            season_year: subscription.year,
            start_year: subscription.year,
            status: None,
            episodes: None,
            next_airing_episode: None,
            next_airing_at: None,
            confidence: 1.0,
        });
    }

    let mut season_mappings = Vec::new();
    let mut seen_anilist_ids = HashSet::new();
    for season in &season_chain {
        if !seen_anilist_ids.insert(season.anilist_id.clone()) {
            continue;
        }
        let mapping = if season.anilist_id == seed_anilist_id {
            seed_mapping.clone()
        } else {
            state
                .linkers
                .fetch_anizip_mapping(&season.anilist_id)
                .await
                .unwrap_or(None)
        };
        season_mappings.push(AnimeSeasonMapping {
            season: season.clone(),
            mapping,
        });
    }

    let graph = build_anime_metadata_graph(AnimeMetadataGraphInput {
        title: subscription.title.clone(),
        year: subscription.year,
        seed_anilist_id,
        seed_season_number: seed_season,
        external_ids: ids.clone(),
        seasons: season_mappings,
    });
    *ids = graph.external_ids.clone();
    let targets = graph.to_new_acquisition_targets(subscription.release_delay_seconds, now);
    let snapshot = graph.to_graph_snapshot_input(
        Some(subscription.subscription_id),
        DEFAULT_ROUTE_OWNER_ID.to_string(),
    );
    Ok((targets, Some(snapshot)))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AniListSeasonChainEntryEnvelope {
    season_number: i32,
    anilist_id: String,
    title: String,
    format: Option<String>,
    season_year: Option<i32>,
    start_year: Option<i32>,
    status: Option<String>,
    episodes: Option<i32>,
    next_airing_episode: Option<i32>,
    next_airing_at: Option<i64>,
    confidence: f32,
}

impl From<&AniListSeasonChainEntry> for AniListSeasonChainEntryEnvelope {
    fn from(value: &AniListSeasonChainEntry) -> Self {
        Self {
            season_number: value.season_number,
            anilist_id: value.anilist_id.clone(),
            title: value.title.clone(),
            format: value.format.clone(),
            season_year: value.season_year,
            start_year: value.start_year,
            status: value.status.clone(),
            episodes: value.episodes,
            next_airing_episode: value.next_airing_episode,
            next_airing_at: value.next_airing_at,
            confidence: value.confidence,
        }
    }
}

impl From<AniListSeasonChainEntryEnvelope> for AniListSeasonChainEntry {
    fn from(value: AniListSeasonChainEntryEnvelope) -> Self {
        Self {
            season_number: value.season_number,
            anilist_id: value.anilist_id,
            title: value.title,
            format: value.format,
            season_year: value.season_year,
            start_year: value.start_year,
            status: value.status,
            episodes: value.episodes,
            next_airing_episode: value.next_airing_episode,
            next_airing_at: value.next_airing_at,
            confidence: value.confidence,
        }
    }
}

async fn resolve_tvdb_series_id(state: &AppState, ids: &ExternalIds) -> Result<Option<String>> {
    if let Some(value) = ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()) {
        return Ok(Some(value.clone()));
    }
    if let Some(imdb) = ids.imdb.as_deref() {
        return state.linkers.link_tvdb_series_by_imdb(imdb).await;
    }
    Ok(None)
}

fn candidate_search_request_for_group(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    search_intent: Option<CandidateSearchIntent>,
) -> CandidateSearchRequest {
    let external_ids = merged_target_external_ids(subscription.external_ids.clone(), target);
    CandidateSearchRequest {
        provider_id: target
            .selected_provider_id
            .or(subscription.source_provider_id),
        media_type: media_type_name(target.media_type).to_string(),
        title: target.title.clone(),
        year: subscription.year,
        external_ids: Some(external_ids),
        target: Some(CandidateSearchTarget {
            season_number: target.season_number,
            episode_number: target.episode_number,
            absolute_episode_number: target.absolute_episode_number,
            air_date: target.air_date.clone(),
        }),
        search_intent,
        preferences: preferences_from_subscription(subscription),
        limit: Some(DEFAULT_CANDIDATE_LIMIT),
    }
}

fn preferences_from_subscription(
    subscription: &AcquisitionSubscription,
) -> CandidateSearchPreferences {
    let profile = subscription.quality_profile.as_ref();
    CandidateSearchPreferences {
        route_policy: Some(subscription.route_policy.as_str().to_string()),
        allowed_qualities: json_string_array(profile, &["allowedQualities", "qualities"]),
        max_size_bytes: json_u64(profile, &["maxSizeBytes", "max_size_bytes"]),
        required_languages: json_string_array(profile, &["requiredLanguages", "languages"]),
    }
}

fn anime_candidate_scoring_context(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    subscription_targets: &[AcquisitionTarget],
) -> Option<AnimeCandidateScoringContext> {
    if target.media_type != MediaType::Anime {
        return None;
    }

    let metadata = target.metadata.as_ref();
    let mut aliases = BTreeSet::new();
    insert_candidate_alias(&mut aliases, &subscription.title);
    let targets = if subscription_targets.is_empty() {
        vec![target.clone()]
    } else {
        subscription_targets.to_vec()
    };
    for item in &targets {
        insert_candidate_alias(&mut aliases, &item.title);
        if let Some(values) = item
            .metadata
            .as_ref()
            .and_then(|value| value.get("aliases"))
            .and_then(JsonValue::as_array)
        {
            for value in values {
                if let Some(alias) = value.as_str() {
                    insert_candidate_alias(&mut aliases, alias);
                }
            }
        }
    }

    Some(AnimeCandidateScoringContext {
        graph_fingerprint: metadata_string(metadata, &["graphFingerprint"]),
        aliases: aliases.into_iter().collect(),
        targets: targets
            .iter()
            .map(|target| {
                let metadata = target.metadata.as_ref();
                AnimeCandidateTarget {
                    target_key: target.target_key.clone(),
                    canonical_key: metadata_string(metadata, &["targetCanonicalKey"]),
                    title: target.title.clone(),
                    season_number: target.season_number,
                    episode_number: target.episode_number,
                    absolute_episode_number: target.absolute_episode_number,
                    tvdb_episode_id: metadata_string(metadata, &["tvdbEpisodeId"]),
                    anidb_episode_id: metadata_string(metadata, &["anidbEpisodeId"]),
                }
            })
            .collect(),
    })
}

fn insert_candidate_alias(aliases: &mut BTreeSet<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        aliases.insert(trimmed.to_string());
    }
}

fn metadata_string(metadata: Option<&JsonValue>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        metadata
            .and_then(|value| value.get(*key))
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
    })
}

#[cfg(test)]
fn select_best_candidate(
    candidates: &[AcquisitionCandidate],
    route_policy: AcquisitionRoutePolicy,
    anime_context: Option<&AnimeCandidateScoringContext>,
) -> Option<CandidateSelection> {
    select_best_candidate_with_preference(
        candidates,
        route_policy,
        anime_context,
        SelectionPreference {
            prefer_packs_for_backfill: false,
        },
    )
}

fn select_best_candidate_with_preference(
    candidates: &[AcquisitionCandidate],
    route_policy: AcquisitionRoutePolicy,
    anime_context: Option<&AnimeCandidateScoringContext>,
    preference: SelectionPreference,
) -> Option<CandidateSelection> {
    let mut best = None;
    for candidate in candidates
        .iter()
        .filter(|candidate| candidate_allowed_by_policy(candidate, route_policy))
    {
        let selection = match anime_context {
            Some(context) => {
                let input = anime_candidate_input(candidate);
                let plan = plan_anime_file_coverage(
                    context,
                    &input,
                    &anime_release_file_inputs(candidate),
                );
                if !plan.rejection_reasons.is_empty()
                    || plan.confidence == ReleaseConfidence::ReviewRequired
                {
                    continue;
                }
                let score = score_anime_candidate(context, &input);
                anime_scored_candidate(candidate, score, Some(plan))
            }
            None => CandidateSelection {
                candidate: candidate.clone(),
                anime_coverage_plan: None,
            },
        };
        let replace = best
            .as_ref()
            .map(|current: &CandidateSelection| {
                compare_candidate_selections(&selection, current, route_policy, preference)
                    == Ordering::Greater
            })
            .unwrap_or(true);
        if replace {
            best = Some(selection);
        }
    }
    best
}

fn compare_candidate_selections(
    left: &CandidateSelection,
    right: &CandidateSelection,
    route_policy: AcquisitionRoutePolicy,
    preference: SelectionPreference,
) -> Ordering {
    if preference.prefer_packs_for_backfill {
        let left_pack = anime_release_plan_rank(left.anime_coverage_plan.as_ref());
        let right_pack = anime_release_plan_rank(right.anime_coverage_plan.as_ref());
        let left_coverage = anime_release_plan_coverage(left.anime_coverage_plan.as_ref());
        let right_coverage = anime_release_plan_coverage(right.anime_coverage_plan.as_ref());
        let pack_order = (left_pack, left_coverage).cmp(&(right_pack, right_coverage));
        if pack_order != Ordering::Equal {
            return pack_order;
        }
    }
    compare_candidates(&left.candidate, &right.candidate, route_policy)
}

fn anime_release_plan_rank(plan: Option<&AnimeFileCoveragePlan>) -> i32 {
    let Some(plan) = plan else {
        return 0;
    };
    match plan.release_kind {
        ReleaseKind::SeriesPack => 5,
        ReleaseKind::MultiSeasonPack => 4,
        ReleaseKind::SeasonPack => 3,
        ReleaseKind::MultiEpisode => 2,
        ReleaseKind::Single => 1,
        ReleaseKind::Unknown => 0,
    }
}

fn anime_release_plan_coverage(plan: Option<&AnimeFileCoveragePlan>) -> usize {
    plan.map(|plan| plan.entries.len()).unwrap_or_default()
}

fn anime_candidate_input(candidate: &AcquisitionCandidate) -> AnimeCandidateInput {
    AnimeCandidateInput {
        title: candidate.title.clone(),
        source_kind: candidate.source_kind.clone(),
        quality: candidate.quality.clone(),
        size_bytes: candidate.size_bytes,
        seeders: candidate.seeders,
        cached_debrid: candidate.cached_debrid,
        rank: candidate.rank,
        source_score: candidate.score,
        supported_routes: candidate.supported_routes.clone(),
        default_route: candidate.default_route.clone(),
    }
}

fn anime_release_file_inputs(candidate: &AcquisitionCandidate) -> Vec<AnimeReleaseFileInput> {
    candidate
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let fallback_index = file
                .file_index
                .or_else(|| i64::try_from(index).ok().map(|value| value + 1));
            let file_key = file
                .file_id
                .clone()
                .or_else(|| fallback_index.map(|value| value.to_string()))
                .unwrap_or_else(|| file.path.clone());
            AnimeReleaseFileInput {
                file_key,
                file_id: file.file_id.clone(),
                file_index: fallback_index,
                path: file.path.clone(),
                size_bytes: file.size_bytes.and_then(|value| i64::try_from(value).ok()),
                selectable: file.selectable.unwrap_or(true),
            }
        })
        .collect()
}

fn anime_scored_candidate(
    candidate: &AcquisitionCandidate,
    match_score: AnimeCandidateScore,
    anime_coverage_plan: Option<AnimeFileCoveragePlan>,
) -> CandidateSelection {
    let mut candidate = candidate.clone();
    candidate.score = Some(match_score.score);
    candidate.score_badges.push(CandidateScoreBadge {
        label: "Anime match".to_string(),
        detail: Some(anime_match_detail(&match_score)),
        score: Some(match_score.score),
    });
    CandidateSelection {
        candidate,
        anime_coverage_plan,
    }
}

fn anime_match_detail(match_score: &AnimeCandidateScore) -> String {
    let alias = match_score
        .alias_matches
        .first()
        .map(|item| format!("{} {}", item.kind.as_str(), item.display))
        .unwrap_or_else(|| "no alias".to_string());
    let target = match_score
        .target_matches
        .first()
        .map(|item| format!("{} {}", item.target_key, item.match_reason))
        .unwrap_or_else(|| "no target".to_string());
    format!("{alias}; {target}; {:?}", match_score.confidence)
}

fn compare_candidates(
    left: &AcquisitionCandidate,
    right: &AcquisitionCandidate,
    route_policy: AcquisitionRoutePolicy,
) -> Ordering {
    candidate_score_tuple(left, route_policy).cmp(&candidate_score_tuple(right, route_policy))
}

fn candidate_score_tuple(
    candidate: &AcquisitionCandidate,
    route_policy: AcquisitionRoutePolicy,
) -> (i32, i32, i32, i32, i64, i32, i32) {
    let route_score = match route_policy {
        AcquisitionRoutePolicy::TorrentOnly => {
            if candidate_supports_route(candidate, TORRENT_DEFAULT_LOGICAL_ID) {
                2
            } else {
                0
            }
        }
        AcquisitionRoutePolicy::DebridOnly => {
            if candidate_supports_route(candidate, DEBRID_DEFAULT_LOGICAL_ID) {
                2
            } else {
                0
            }
        }
        AcquisitionRoutePolicy::DebridFirst | AcquisitionRoutePolicy::Manual => {
            if candidate_supports_route(candidate, DEBRID_DEFAULT_LOGICAL_ID) {
                2
            } else {
                1
            }
        }
    };
    let cached_score = match candidate.cached_debrid {
        Some(true) => 2,
        Some(false) => 0,
        None => 1,
    };
    let score = (candidate.score.unwrap_or(0.0) * 1000.0).round() as i32;
    let quality = quality_score(candidate.quality.as_deref());
    let seeders = candidate.seeders.unwrap_or_default() as i64;
    let rank = candidate.rank.map(|rank| 10_000 - rank as i32).unwrap_or(0);
    let size = candidate
        .size_bytes
        .map(|bytes| (bytes / (1024 * 1024 * 1024)) as i32)
        .unwrap_or(0);
    (
        route_score,
        cached_score,
        score,
        quality,
        seeders,
        rank,
        size,
    )
}

fn candidate_allowed_by_policy(
    candidate: &AcquisitionCandidate,
    route_policy: AcquisitionRoutePolicy,
) -> bool {
    match route_policy {
        AcquisitionRoutePolicy::DebridOnly => {
            candidate_supports_route(candidate, DEBRID_DEFAULT_LOGICAL_ID)
        }
        AcquisitionRoutePolicy::TorrentOnly => {
            candidate_supports_route(candidate, TORRENT_DEFAULT_LOGICAL_ID)
        }
        AcquisitionRoutePolicy::DebridFirst | AcquisitionRoutePolicy::Manual => {
            candidate_supports_route(candidate, DEBRID_DEFAULT_LOGICAL_ID)
                || candidate_supports_route(candidate, TORRENT_DEFAULT_LOGICAL_ID)
        }
    }
}

fn select_candidate_route(
    requested_route: Option<&str>,
    route_policy: AcquisitionRoutePolicy,
    candidate: &AcquisitionCandidate,
) -> Result<String> {
    if let Some(route) = requested_route.and_then(non_empty) {
        validate_selected_candidate_route(route, candidate)?;
        return Ok(route.to_string());
    }
    let selected = match route_policy {
        AcquisitionRoutePolicy::DebridFirst => {
            if candidate_supports_route(candidate, DEBRID_DEFAULT_LOGICAL_ID) {
                Some(DEBRID_DEFAULT_LOGICAL_ID)
            } else if candidate_supports_route(candidate, TORRENT_DEFAULT_LOGICAL_ID) {
                Some(TORRENT_DEFAULT_LOGICAL_ID)
            } else {
                candidate.default_route.as_deref()
            }
        }
        AcquisitionRoutePolicy::DebridOnly => Some(DEBRID_DEFAULT_LOGICAL_ID),
        AcquisitionRoutePolicy::TorrentOnly => Some(TORRENT_DEFAULT_LOGICAL_ID),
        AcquisitionRoutePolicy::Manual => candidate.default_route.as_deref(),
    };
    let route = selected.ok_or_else(|| anyhow!("selectedRouteLogicalId is required"))?;
    validate_selected_candidate_route(route, candidate)?;
    Ok(route.to_string())
}

fn validate_selected_candidate_route(route: &str, candidate: &AcquisitionCandidate) -> Result<()> {
    if route != DEBRID_DEFAULT_LOGICAL_ID && route != TORRENT_DEFAULT_LOGICAL_ID {
        bail!("unsupported selected route '{route}'");
    }
    if !candidate_supports_route(candidate, route) {
        bail!("candidate does not support route '{route}'");
    }
    Ok(())
}

fn candidate_supports_route(candidate: &AcquisitionCandidate, route: &str) -> bool {
    if !candidate.supported_routes.is_empty() {
        return candidate
            .supported_routes
            .iter()
            .any(|item| item.eq_ignore_ascii_case(route));
    }
    match (candidate.source_kind.as_str(), route) {
        ("magnet", DEBRID_DEFAULT_LOGICAL_ID | TORRENT_DEFAULT_LOGICAL_ID) => true,
        ("http" | "hoster", DEBRID_DEFAULT_LOGICAL_ID) => true,
        ("torrent", TORRENT_DEFAULT_LOGICAL_ID) => true,
        _ => false,
    }
}

async fn source_extension_id_for_candidate_provider(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
) -> Result<String> {
    let provider = store
        .get_provider(provider_id)
        .await?
        .ok_or_else(|| anyhow!("candidate provider '{provider_id}' was not found"))?;
    if provider.capability != ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY {
        bail!(
            "provider '{}' is '{}', not '{}'",
            provider_id,
            provider.capability,
            ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY
        );
    }
    if provider.health_state != ProviderHealthState::Healthy {
        bail!("candidate provider '{}' is not healthy", provider_id);
    }
    let instance = store
        .get_instance(provider.instance_id)
        .await?
        .ok_or_else(|| anyhow!("candidate provider instance was not found"))?;
    if !instance.enabled {
        bail!("candidate provider instance is disabled");
    }
    let extension = store
        .get_extension(&instance.extension_id)
        .await?
        .ok_or_else(|| anyhow!("candidate provider extension was not found"))?;
    if !extension.enabled {
        bail!("candidate provider extension is disabled");
    }
    Ok(instance.extension_id)
}

fn selected_candidate_provenance(submission: &CandidateSubmission) -> Result<JsonValue> {
    let mut value = serde_json::to_value(&submission.candidate)?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "sourceProviderId".to_string(),
            json!(submission.provider_id.to_string()),
        );
        object.insert(
            "sourceExtensionId".to_string(),
            json!(submission.source_extension_id),
        );
        if let Some(plan) = submission.anime_coverage_plan.as_ref() {
            object.insert("animeCoveragePlan".to_string(), serde_json::to_value(plan)?);
        }
    }
    Ok(value)
}

fn merged_target_external_ids(
    base: Option<ExternalIds>,
    target: &AcquisitionTarget,
) -> ExternalIds {
    let mut ids = base.unwrap_or_default();
    if let Some(metadata_ids) = target
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("externalIds"))
        .and_then(|value| serde_json::from_value::<ExternalIds>(value.clone()).ok())
    {
        merge_external_ids(&mut ids, &metadata_ids);
    }
    ids
}

fn merge_external_ids(target: &mut ExternalIds, source: &ExternalIds) {
    if target.imdb.is_none() {
        target.imdb = source.imdb.clone();
    }
    if target.tmdb.is_none() {
        target.tmdb = source.tmdb.clone();
    }
    if target.tvdb.is_none() {
        target.tvdb = source.tvdb.clone();
    }
    if target.tvdb_series.is_none() {
        target.tvdb_series = source.tvdb_series.clone();
    }
    if target.tvdb_movie.is_none() {
        target.tvdb_movie = source.tvdb_movie.clone();
    }
    if target.anilist.is_none() {
        target.anilist = source.anilist.clone();
    }
    if target.anidb.is_none() {
        target.anidb = source.anidb.clone();
    }
    if target.mal.is_none() {
        target.mal = source.mal.clone();
    }
    if target.kitsu.is_none() {
        target.kitsu = source.kitsu.clone();
    }
}

trait ExternalIdsDefaultCheck {
    fn is_default(&self) -> bool;
}

impl ExternalIdsDefaultCheck for ExternalIds {
    fn is_default(&self) -> bool {
        self == &ExternalIds::default()
    }
}

fn next_metadata_refresh_after(
    subscription: &AcquisitionSubscription,
    chain: &[AniListSeasonChainEntry],
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    let regular = now
        + ChronoDuration::seconds(METADATA_REFRESH_BASE_SECONDS)
        + jitter_duration(
            &subscription.subscription_id,
            METADATA_REFRESH_JITTER_SECONDS,
        );
    let around_airing = chain
        .iter()
        .filter_map(|entry| entry.next_airing_at)
        .filter_map(|timestamp| Utc.timestamp_opt(timestamp, 0).single())
        .filter(|airing| *airing > now)
        .map(|airing| airing - ChronoDuration::minutes(30))
        .filter(|refresh| *refresh > now)
        .min();
    around_airing
        .map(|value| value.min(regular))
        .unwrap_or(regular)
}

fn next_candidate_retry_after(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    if let Some(air_time) = target.air_time {
        let release_time = air_time + ChronoDuration::seconds(subscription.release_delay_seconds);
        if now < release_time {
            return release_time;
        }
        let elapsed = now - release_time;
        if elapsed < ChronoDuration::seconds(HOT_RETRY_WINDOW_SECONDS) {
            return now + jittered_seconds(&target.target_id, HOT_RETRY_INTERVAL_SECONDS, 120);
        }
        if elapsed < ChronoDuration::seconds(WARM_RETRY_WINDOW_SECONDS) {
            return now + jittered_seconds(&target.target_id, WARM_RETRY_INTERVAL_SECONDS, 300);
        }
        return now + jittered_seconds(&target.target_id, COLD_RETRY_INTERVAL_SECONDS, 900);
    }
    now + jittered_seconds(
        &target.target_id,
        CANDIDATE_SEARCH_BASE_SECONDS,
        CANDIDATE_SEARCH_JITTER_SECONDS,
    )
}

fn next_search_after_for_air_time(
    air_time: Option<DateTime<Utc>>,
    release_delay_seconds: i64,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    match air_time {
        Some(air_time) if air_time + ChronoDuration::seconds(release_delay_seconds) > now => {
            air_time + ChronoDuration::seconds(release_delay_seconds)
        }
        _ => now,
    }
}

fn target_allowed(
    subscription: &AcquisitionSubscription,
    target: &NewAcquisitionTarget,
    now: DateTime<Utc>,
) -> bool {
    use crate::acquisition::subscriptions::AcquisitionMonitorPolicy;
    match subscription.monitor_policy {
        AcquisitionMonitorPolicy::AllMissing => true,
        AcquisitionMonitorPolicy::FutureOnly => target
            .air_time
            .map(|air_time| air_time >= now)
            .unwrap_or(true),
        AcquisitionMonitorPolicy::SelectedTargets => false,
        AcquisitionMonitorPolicy::SelectedSeasons => {
            let seasons = selected_seasons(subscription.quality_profile.as_ref());
            target
                .season_number
                .map(|season| seasons.contains(&season))
                .unwrap_or(false)
        }
    }
}

fn selected_seasons(profile: Option<&JsonValue>) -> HashSet<i32> {
    let mut seasons = HashSet::new();
    for key in ["selectedSeasons", "seasons"] {
        if let Some(values) = profile
            .and_then(|value| value.get(key))
            .and_then(JsonValue::as_array)
        {
            for value in values {
                if let Some(season) = value.as_i64() {
                    seasons.insert(season as i32);
                }
            }
        }
    }
    seasons
}

fn debrid_status_failed(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "failed" | "error" | "dead" | "virus" | "magnet_error"
    )
}

fn api_error_to_anyhow(err: ApiError) -> anyhow::Error {
    let message = match err {
        ApiError::BadRequest(message)
        | ApiError::Unauthorized(message)
        | ApiError::Forbidden(message)
        | ApiError::NotFound(message)
        | ApiError::Conflict(message)
        | ApiError::Internal(message) => message,
    };
    anyhow!(message)
}

fn extract_season_number(value: &JsonValue) -> Option<i32> {
    value
        .get("number")
        .or_else(|| value.get("seasonNumber"))
        .or_else(|| value.get("season_number"))
        .and_then(JsonValue::as_i64)
        .map(|value| value as i32)
}

fn extract_air_date(raw: &JsonValue) -> Option<String> {
    for key in [
        "airDate",
        "air_date",
        "firstAired",
        "first_aired",
        "aired",
        "releaseDate",
    ] {
        if let Some(value) = raw.get(key).and_then(JsonValue::as_str) {
            if let Some(date) = normalize_air_date(value) {
                return Some(date);
            }
        }
    }
    None
}

fn normalize_air_date(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.len() >= 10 {
        return Some(trimmed[..10].to_string());
    }
    None
}

fn parse_air_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn parse_air_date(value: &str) -> Option<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(value.get(0..10)?, "%Y-%m-%d").ok()?;
    Some(Utc.from_utc_datetime(&date.and_hms_opt(12, 0, 0)?))
}

fn json_string_array(profile: Option<&JsonValue>, keys: &[&str]) -> Vec<String> {
    for key in keys {
        if let Some(values) = profile
            .and_then(|value| value.get(*key))
            .and_then(JsonValue::as_array)
        {
            return values
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect();
        }
    }
    Vec::new()
}

fn json_u64(profile: Option<&JsonValue>, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(value) = profile.and_then(|value| value.get(*key)) {
            if let Some(number) = value.as_u64() {
                return Some(number);
            }
        }
    }
    None
}

fn quality_score(value: Option<&str>) -> i32 {
    let Some(value) = value else {
        return 0;
    };
    let lower = value.to_ascii_lowercase();
    if lower.contains("2160") || lower.contains("4k") {
        4
    } else if lower.contains("1080") {
        3
    } else if lower.contains("720") {
        2
    } else if lower.contains("480") {
        1
    } else {
        0
    }
}

fn download_display_name(target: &AcquisitionTarget, candidate: &AcquisitionCandidate) -> String {
    if let (Some(season), Some(episode)) = (target.season_number, target.episode_number) {
        return format!("{} S{season:02}E{episode:02}", target.title);
    }
    candidate.title.clone()
}

fn media_type_name(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movie",
        MediaType::Series => "series",
        MediaType::Anime => "anime",
    }
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn jitter_duration(seed: &Uuid, max_seconds: i64) -> ChronoDuration {
    ChronoDuration::seconds(jitter_seconds(seed, max_seconds))
}

fn jittered_seconds(seed: &Uuid, base_seconds: i64, max_jitter_seconds: i64) -> ChronoDuration {
    ChronoDuration::seconds(base_seconds + jitter_seconds(seed, max_jitter_seconds))
}

fn jitter_seconds(seed: &Uuid, max_seconds: i64) -> i64 {
    if max_seconds <= 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    (hasher.finish() % max_seconds as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::handlers::acquisition_sources::AcquisitionCandidateFile;
    use serde_json::json;

    fn candidate(
        title: &str,
        routes: Vec<&str>,
        cached_debrid: Option<bool>,
        seeders: Option<u32>,
    ) -> AcquisitionCandidate {
        AcquisitionCandidate {
            id: None,
            title: title.to_string(),
            source: format!("magnet:?xt=urn:btih:{title}"),
            source_kind: "magnet".to_string(),
            info_hash: None,
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: None,
            seeders,
            language: None,
            cached_debrid,
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: routes.into_iter().map(ToString::to_string).collect(),
            default_route: None,
            raw: None,
        }
    }

    fn test_subscription() -> AcquisitionSubscription {
        AcquisitionSubscription {
            subscription_id: Uuid::new_v4(),
            media_type: MediaType::Series,
            title: "Show".to_string(),
            normalized_title: "show".to_string(),
            year: Some(2026),
            external_ids: None,
            monitor_policy: Default::default(),
            route_policy: AcquisitionRoutePolicy::DebridFirst,
            source_provider_id: None,
            release_delay_seconds: 0,
            quality_profile: None,
            metadata_refresh_after: Utc::now(),
            candidate_search_after: Utc::now(),
            last_metadata_refresh_at: None,
            last_candidate_search_at: None,
            status: Default::default(),
            active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn anime_subscription() -> AcquisitionSubscription {
        AcquisitionSubscription {
            media_type: MediaType::Anime,
            title: "Example Title".to_string(),
            normalized_title: "example title".to_string(),
            ..test_subscription()
        }
    }

    fn episode_target(
        subscription: &AcquisitionSubscription,
        season_number: i32,
        episode_number: i32,
    ) -> AcquisitionTarget {
        let now = Utc::now();
        AcquisitionTarget {
            target_id: Uuid::new_v4(),
            subscription_id: subscription.subscription_id,
            target_key: format!("S{season_number:02}E{episode_number:02}"),
            media_type: MediaType::Series,
            title: "Show".to_string(),
            season_number: Some(season_number),
            episode_number: Some(episode_number),
            absolute_episode_number: None,
            air_date: None,
            air_time: Some(now - ChronoDuration::days(1)),
            metadata: None,
            state: AcquisitionTargetState::Pending,
            state_reason: None,
            selected_provider_id: None,
            selected_route_logical_id: None,
            selected_candidate: None,
            download_id: None,
            import_event_id: None,
            search_attempts: 0,
            last_search_at: None,
            next_search_after: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn anime_episode_target(
        subscription: &AcquisitionSubscription,
        season_number: i32,
        episode_number: i32,
    ) -> AcquisitionTarget {
        AcquisitionTarget {
            media_type: MediaType::Anime,
            title: "Example Title".to_string(),
            absolute_episode_number: Some(episode_number),
            ..episode_target(subscription, season_number, episode_number)
        }
    }

    fn anime_scoring_context(target_count: i32) -> AnimeCandidateScoringContext {
        AnimeCandidateScoringContext {
            graph_fingerprint: Some("automation-rr3k".to_string()),
            aliases: vec!["Example Title".to_string()],
            targets: (1..=target_count)
                .map(|episode| AnimeCandidateTarget {
                    target_key: format!("S01E{episode:02}"),
                    canonical_key: Some(format!("tvdb:100:S01E{episode:02}")),
                    title: format!("Episode {episode}"),
                    season_number: Some(1),
                    episode_number: Some(episode),
                    absolute_episode_number: Some(episode),
                    tvdb_episode_id: Some(format!("10{episode:02}")),
                    anidb_episode_id: Some(format!("20{episode:02}")),
                })
                .collect(),
        }
    }

    #[test]
    fn best_candidate_prefers_cached_debrid_for_debrid_first() {
        let torrent = candidate("torrent", vec![TORRENT_DEFAULT_LOGICAL_ID], None, Some(500));
        let cached = candidate(
            "cached",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(1),
        );
        let best = select_best_candidate(
            &[torrent, cached],
            AcquisitionRoutePolicy::DebridFirst,
            None,
        )
        .expect("best candidate");
        assert_eq!(best.candidate.title, "cached");
    }

    #[test]
    fn anime_best_candidate_requires_graph_alias_and_target_match() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("automation-rr3e".to_string()),
            aliases: vec!["Example Title".to_string()],
            targets: vec![AnimeCandidateTarget {
                target_key: "S01E01".to_string(),
                canonical_key: Some("tvdb:100:S01E01".to_string()),
                title: "Episode One".to_string(),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                tvdb_episode_id: Some("1001".to_string()),
                anidb_episode_id: Some("2001".to_string()),
            }],
        };
        let wrong = candidate(
            "[SubsPlease] Different Title - 01 [1080p]",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(500),
        );
        let valid = candidate(
            "[SubsPlease] Example Title - 01 [1080p]",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(10),
        );

        let best = select_best_candidate(
            &[wrong, valid],
            AcquisitionRoutePolicy::DebridFirst,
            Some(&context),
        )
        .expect("best anime candidate");

        assert_eq!(
            best.candidate.title,
            "[SubsPlease] Example Title - 01 [1080p]"
        );
        assert!(best.candidate.score.is_some());
        assert!(
            best.candidate
                .score_badges
                .iter()
                .any(|badge| badge.label == "Anime match")
        );
    }

    #[test]
    fn anime_pack_candidate_with_file_list_covers_many_targets_once() {
        let context = AnimeCandidateScoringContext {
            graph_fingerprint: Some("automation-rr3f".to_string()),
            aliases: vec!["Example Title".to_string()],
            targets: vec![
                AnimeCandidateTarget {
                    target_key: "S01E01".to_string(),
                    canonical_key: Some("tvdb:100:S01E01".to_string()),
                    title: "Episode One".to_string(),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    tvdb_episode_id: Some("1001".to_string()),
                    anidb_episode_id: Some("2001".to_string()),
                },
                AnimeCandidateTarget {
                    target_key: "S01E02".to_string(),
                    canonical_key: Some("tvdb:100:S01E02".to_string()),
                    title: "Episode Two".to_string(),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: Some(2),
                    tvdb_episode_id: Some("1002".to_string()),
                    anidb_episode_id: Some("2002".to_string()),
                },
            ],
        };
        let mut pack = candidate(
            "[SubsPlease] Example Title S01 Batch [1080p]",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(200),
        );
        pack.files = vec![
            AcquisitionCandidateFile {
                file_id: Some("1".to_string()),
                file_index: Some(1),
                path: "Example Title - 01 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
            AcquisitionCandidateFile {
                file_id: Some("2".to_string()),
                file_index: Some(2),
                path: "Example Title - 02 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
        ];

        let best =
            select_best_candidate(&[pack], AcquisitionRoutePolicy::DebridFirst, Some(&context))
                .expect("pack candidate selected");
        let plan = best
            .anime_coverage_plan
            .expect("anime coverage plan should be attached");

        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(plan.entries.len(), 2);
        assert_eq!(plan.selected_file_keys, vec!["1", "2"]);
    }

    #[test]
    fn candidate_search_request_includes_grouped_pack_search_intent() {
        let subscription = anime_subscription();
        let targets = vec![
            anime_episode_target(&subscription, 1, 1),
            anime_episode_target(&subscription, 1, 2),
            anime_episode_target(&subscription, 1, 3),
        ];
        let request = candidate_search_request_for_group(
            &subscription,
            &targets[0],
            Some(search_intent_for_targets(&targets, RetryBucket::Cold)),
        );
        let serialized = serde_json::to_value(&request).expect("serialized request");

        assert_eq!(request.media_type, "anime");
        assert_eq!(request.title, "Example Title");
        assert_eq!(
            request
                .target
                .as_ref()
                .and_then(|target| target.season_number),
            Some(1)
        );
        assert_eq!(
            request
                .target
                .as_ref()
                .and_then(|target| target.episode_number),
            Some(1)
        );
        assert_eq!(serialized["target"]["seasonNumber"], 1);
        assert_eq!(serialized["target"]["episodeNumber"], 1);
        assert_eq!(serialized["searchIntent"]["kind"], "season_pack");
        assert_eq!(serialized["searchIntent"]["seasonNumber"], 1);
        assert_eq!(serialized["searchIntent"]["targetCount"], 3);
        assert_eq!(
            serialized["searchIntent"]["targetKeys"],
            json!(["S01E01", "S01E02", "S01E03"])
        );
        assert_eq!(serialized["searchIntent"]["retryBucket"], "cold");
    }

    #[test]
    fn anime_backfill_grouping_collapses_large_season_into_one_search() {
        let subscription = anime_subscription();
        let now = Utc::now();
        let targets = (1..=1000)
            .map(|episode| AcquisitionTarget {
                air_time: Some(now - ChronoDuration::days(30)),
                next_search_after: Some(now - ChronoDuration::minutes(1)),
                ..anime_episode_target(&subscription, 1, episode)
            })
            .collect::<Vec<_>>();
        let subscriptions = HashMap::from([(subscription.subscription_id, subscription.clone())]);
        let targets_by_subscription =
            HashMap::from([(subscription.subscription_id, targets.clone())]);

        let groups =
            build_target_search_groups(&subscriptions, targets, &targets_by_subscription, now);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].targets.len(), 1000);
        let intent = groups[0].search_intent.as_ref().expect("search intent");
        assert_eq!(intent.kind, "season_pack");
        assert_eq!(intent.season_number, Some(1));
        assert_eq!(intent.episode_start, Some(1));
        assert_eq!(intent.episode_end, Some(1000));
        assert_eq!(intent.target_count, 1000);
        assert_eq!(
            intent.target_keys.first().map(String::as_str),
            Some("S01E01")
        );
        assert_eq!(
            intent.target_keys.last().map(String::as_str),
            Some("S01E1000")
        );
        assert_eq!(intent.retry_bucket.as_deref(), Some("cold"));
    }

    #[test]
    fn large_backfill_selection_prefers_high_coverage_pack_over_single() {
        let context = anime_scoring_context(3);
        let single = candidate(
            "[SubsPlease] Example Title - 01 [1080p]",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(1000),
        );
        let mut pack = candidate(
            "[SubsPlease] Example Title S01 Batch [1080p]",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(1),
        );
        pack.files = vec![
            AcquisitionCandidateFile {
                file_id: Some("1".to_string()),
                file_index: Some(1),
                path: "Example Title - 01 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
            AcquisitionCandidateFile {
                file_id: Some("2".to_string()),
                file_index: Some(2),
                path: "Example Title - 02 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
            AcquisitionCandidateFile {
                file_id: Some("3".to_string()),
                file_index: Some(3),
                path: "Example Title - 03 [1080p].mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
        ];

        let best = select_best_candidate_with_preference(
            &[single, pack],
            AcquisitionRoutePolicy::DebridFirst,
            Some(&context),
            SelectionPreference {
                prefer_packs_for_backfill: true,
            },
        )
        .expect("best anime pack candidate");
        let plan = best
            .anime_coverage_plan
            .expect("anime coverage plan should be attached");

        assert_eq!(
            best.candidate.title,
            "[SubsPlease] Example Title S01 Batch [1080p]"
        );
        assert_eq!(plan.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(plan.entries.len(), 3);
        assert_eq!(plan.selected_file_keys, vec!["1", "2", "3"]);
    }

    #[test]
    fn queue_governor_enforces_subscription_route_caps() {
        let subscription_id = Uuid::new_v4();
        let mut governor = QueueGovernor {
            caps: QueueGovernorCaps::default(),
            active_by_route: HashMap::new(),
            active_by_subscription_route: HashMap::new(),
        };

        assert!(
            governor
                .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
                .is_ok()
        );
        assert!(
            governor
                .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
                .is_ok()
        );
        assert!(
            governor
                .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
                .is_ok()
        );
        let block = governor
            .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
            .expect_err("subscription route cap");

        assert_eq!(block.route_logical_id, DEBRID_DEFAULT_LOGICAL_ID);
        assert_eq!(
            block.subscription_active,
            DEFAULT_SUBSCRIPTION_DEBRID_RELEASE_JOB_CAP
        );
        assert_eq!(
            block.subscription_limit,
            DEFAULT_SUBSCRIPTION_DEBRID_RELEASE_JOB_CAP
        );
    }

    #[test]
    fn retry_after_uses_hot_window_after_air_time() {
        let subscription = AcquisitionSubscription {
            subscription_id: Uuid::new_v4(),
            media_type: MediaType::Series,
            title: "Show".to_string(),
            normalized_title: "show".to_string(),
            year: None,
            external_ids: None,
            monitor_policy: Default::default(),
            route_policy: AcquisitionRoutePolicy::DebridFirst,
            source_provider_id: None,
            release_delay_seconds: 30 * 60,
            quality_profile: None,
            metadata_refresh_after: Utc::now(),
            candidate_search_after: Utc::now(),
            last_metadata_refresh_at: None,
            last_candidate_search_at: None,
            status: Default::default(),
            active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let now = Utc::now();
        let target = AcquisitionTarget {
            target_id: Uuid::new_v4(),
            subscription_id: subscription.subscription_id,
            target_key: "S01E01".to_string(),
            media_type: MediaType::Series,
            title: "Pilot".to_string(),
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: None,
            air_date: None,
            air_time: Some(now - ChronoDuration::minutes(10)),
            metadata: None,
            state: AcquisitionTargetState::Pending,
            state_reason: None,
            selected_provider_id: None,
            selected_route_logical_id: None,
            selected_candidate: None,
            download_id: None,
            import_event_id: None,
            search_attempts: 0,
            last_search_at: None,
            next_search_after: None,
            created_at: now,
            updated_at: now,
        };
        let target = AcquisitionTarget {
            air_time: Some(now - ChronoDuration::minutes(40)),
            ..target
        };
        let retry = next_candidate_retry_after(&subscription, &target, now);
        assert!(retry >= now + ChronoDuration::minutes(14));
        assert!(retry <= now + ChronoDuration::minutes(17));
    }
}
