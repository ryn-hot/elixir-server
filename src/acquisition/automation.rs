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
    acquisition::audit::{
        EVENT_ACQUISITION_SEARCH_SCHEDULED, NewAcquisitionAuditEvent,
        record_acquisition_audit_event,
    },
    acquisition::release_resolution::{
        anime::{
            ANIME_SHOKO_STYLE_RESOLVER_VERSION, AnimeCandidateInput, AnimeCandidateScore,
            AnimeCandidateScoringContext, AnimeCandidateTarget, AnimeFileCoveragePlan,
            AnimeMetadataGraphInput, AnimeReleaseFileInput, AnimeSeasonMapping,
            build_anime_metadata_graph, infer_anizip_season_number, plan_anime_file_coverage,
            score_anime_candidate,
        },
        fingerprint::candidate_release_fingerprint,
        models::{
            AcquisitionReleaseState, NewAcquisitionAnimeCandidateParse,
            NewAcquisitionAnimeGraphSnapshot, NewAcquisitionRelease, NewAcquisitionReleaseCoverage,
            NewAcquisitionReleaseFile, NewAcquisitionReleaseJob, ReleaseConfidence,
            ReleaseCoverageState, ReleaseJobState, ReleaseKind, ReleaseResolverKind,
        },
        review_candidates::{
            ManualReviewResolverEvidence, ManualReviewRoutePolicyEvidence, ManualReviewTargetScope,
            NewManualReviewCandidateRelease, upsert_manual_review_candidate_release,
        },
        store::{
            count_active_release_jobs, count_active_release_jobs_by_route,
            count_active_release_jobs_by_subscription,
            count_active_release_jobs_by_subscription_route, count_stale_active_release_jobs,
            get_release_by_fingerprint, list_release_coverage, upsert_anime_candidate_parse,
            upsert_anime_graph_snapshot, upsert_release, upsert_release_coverage,
            upsert_release_file, upsert_release_job,
        },
        tv::{
            TV_SONARR_STYLE_RESOLVER_VERSION, TvCoverageOptions, TvCoveragePlan,
            TvReleaseFileInput, TvSonarrStyleResolver, TvTarget,
        },
    },
    acquisition::subscriptions::{
        AcquisitionCompletionPolicy, AcquisitionRequestMode, AcquisitionRequestScope,
        AcquisitionRoutePolicy, AcquisitionSubscription, AcquisitionSubscriptionFilter,
        AcquisitionTarget, AcquisitionTargetState, AcquisitionTargetStateUpdate,
        NewAcquisitionTarget, complete_terminal_acquisition_requests, get_target,
        list_due_candidate_targets, list_due_metadata_subscriptions, list_submitted_debrid_targets,
        list_subscription_targets, list_subscriptions, record_metadata_refresh,
        reset_target_for_candidate_retry, start_subscription_tracking_if_initial_download_complete,
        update_subscription_external_ids, update_target_state, upsert_subscription_targets,
    },
    db::models::{MediaType, ProviderHealthState},
    debrid::{
        DebridFailureClass, DebridFailureResponsePolicy, active_debrid_concurrent_downloads,
        get_debrid_job_status,
    },
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, DEFAULT_ROUTE_OWNER_ID, TORRENT_DEFAULT_LOGICAL_ID,
    },
    extensions::{ExternalIds, store::ExtensionStore},
    http::{
        error::ApiError,
        handlers::{
            acquisition_sources::{
                ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY, AcquisitionCandidate,
                CandidateRouteOption, CandidateScoreBadge, CandidateSearchIntent,
                CandidateSearchPreferences, CandidateSearchRequest, CandidateSearchResponse,
                CandidateSearchTarget, acquisition_candidate_tracker_count,
                search_candidates_with_store,
            },
            download_broker::{
                DownloadBrokerSubmitRequest, DownloadBrokerSubmitResponse,
                process_stale_qbittorrent_acquisition_releases, submit_to_broker,
            },
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
const COMPLETION_RECONCILIATION_BATCH_LIMIT: i64 = 50;
const DEFAULT_CANDIDATE_LIMIT: u32 = 25;
const DEFAULT_GLOBAL_DEBRID_RELEASE_JOB_CAP: i64 = 1;
const DEFAULT_SUBSCRIPTION_DEBRID_RELEASE_JOB_CAP: i64 = 1;
const DEFAULT_GLOBAL_TORRENT_RELEASE_JOB_CAP: i64 = 5;
const DEFAULT_SUBSCRIPTION_TORRENT_RELEASE_JOB_CAP: i64 = 2;
const DEFAULT_GLOBAL_RELEASE_JOB_CAP: i64 = 12;
const DEFAULT_SUBSCRIPTION_RELEASE_JOB_CAP: i64 = 5;
const DEFAULT_MAX_CANDIDATE_SEARCHES_PER_TICK: usize = SEARCH_BATCH_LIMIT as usize;
const INITIAL_BACKFILL_NO_CANDIDATE_TERMINAL_ATTEMPTS: i64 = 3;
const DEFAULT_MAX_SUBMISSIONS_PER_TICK: usize = 5;
const DEFAULT_STAGED_INSPECTION_JOB_CAP: i64 = 10;
const DEFAULT_STALE_ACTIVE_JOB_SECONDS: i64 = 6 * 60 * 60;
const PACK_BACKFILL_TARGET_THRESHOLD: usize = 3;
const QUEUE_CAPACITY_RETRY_SECONDS: i64 = 5 * 60;
const FALLBACK_NEXT_CANDIDATE_RETRY_SECONDS: i64 = 30;
const MAX_MANUAL_REVIEW_CANDIDATES_PER_GROUP: usize = 5;
const ONE_SHOT_METADATA_BLOCKER_TARGET_KEY: &str = "METADATA_BLOCKED";

#[derive(Debug, Clone)]
struct CandidateSubmission {
    provider_id: Uuid,
    source_extension_id: String,
    candidate: AcquisitionCandidate,
    provider_warnings: Vec<String>,
    anime_coverage_plan: Option<AnimeFileCoveragePlan>,
    tv_coverage_plan: Option<TvCoveragePlan>,
    request_scope_evidence: Option<JsonValue>,
    dispatch: Option<SchedulerDispatchEvidence>,
}

#[derive(Debug, Clone)]
struct ExistingReleaseReuse {
    download_id: String,
    selected_provider_id: Option<Uuid>,
}

impl CandidateSubmission {
    fn has_release_coverage_plan(&self) -> bool {
        self.anime_coverage_plan.is_some() || self.tv_coverage_plan.is_some()
    }
}

#[derive(Debug, Clone)]
struct CandidateSelection {
    candidate: AcquisitionCandidate,
    anime_coverage_plan: Option<AnimeFileCoveragePlan>,
    tv_coverage_plan: Option<TvCoveragePlan>,
}

#[derive(Debug, Clone)]
struct CandidateReleasePlan {
    provider_id: Uuid,
    source_extension_id: String,
    provider_warnings: Vec<String>,
    route_logical_id: String,
    fingerprint: String,
    selection: CandidateSelection,
    release_kind: ReleaseKind,
    resolver_kind: ReleaseResolverKind,
    resolver_version: String,
    confidence: ReleaseConfidence,
    covered_target_ids: BTreeSet<Uuid>,
    covered_target_keys: BTreeSet<String>,
    overfetch_count: usize,
    request_scope_evidence: Option<JsonValue>,
}

#[derive(Debug, Clone)]
struct CandidateReviewPlan {
    candidate: AcquisitionCandidate,
    release_kind: ReleaseKind,
    resolver_kind: ReleaseResolverKind,
    resolver_version: String,
    confidence: ReleaseConfidence,
    rejection_codes: Vec<String>,
    parsed_release: Option<JsonValue>,
    score: Option<f64>,
    reason: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SchedulerDispatchEvidence {
    scheduler_phase: &'static str,
    group_key: String,
    search_intent: Option<CandidateSearchIntent>,
    selected_plan_score: SchedulerPlanScoreEvidence,
    capacity_snapshot: QueueCapacitySnapshot,
    route_decision: SchedulerRouteDecisionEvidence,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SchedulerPlanScoreEvidence {
    confidence: String,
    covered_target_count: usize,
    release_kind: String,
    resolver_kind: String,
    resolver_version: String,
    route_preference_score: i32,
    cached_debrid_score: i32,
    freshness_score: i32,
    tracker_count: usize,
    quality_score: i32,
    seeders: Option<u32>,
    overfetch_count: usize,
    source_rank: Option<u32>,
    source_score: Option<f64>,
    score_tuple: (i32, usize, i32, i32, i32, i32, i32, i64, i32, i32, i32),
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SchedulerRouteDecisionEvidence {
    route_policy: String,
    selected_route_logical_id: String,
    default_route: Option<String>,
    supported_routes: Vec<String>,
    reason: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct QueueCapacitySnapshot {
    global_active: i64,
    global_limit: i64,
    subscription_active: i64,
    subscription_limit: i64,
    route_active: i64,
    route_limit: Option<i64>,
    subscription_route_active: i64,
    subscription_route_limit: Option<i64>,
    searches_this_tick: usize,
    search_tick_limit: usize,
    submissions_this_tick: usize,
    submission_tick_limit: usize,
    stale_active_jobs: i64,
}

#[derive(Debug, Default)]
struct CandidateReleasePlanBatch {
    plans: Vec<CandidateReleasePlan>,
    review_candidates: Vec<CandidateReviewPlan>,
    capacity_block: Option<QueueCapacityBlock>,
    candidate_count: usize,
    policy_rejected_count: usize,
    preference_rejected_count: usize,
    already_claimed_count: usize,
    resolver_rejected_count: usize,
    route_unavailable_count: usize,
}

impl CandidateReleasePlan {
    fn into_submission(self, dispatch: SchedulerDispatchEvidence) -> CandidateSubmission {
        CandidateSubmission {
            provider_id: self.provider_id,
            source_extension_id: self.source_extension_id,
            candidate: self.selection.candidate,
            provider_warnings: self.provider_warnings,
            anime_coverage_plan: self.selection.anime_coverage_plan,
            tv_coverage_plan: self.selection.tv_coverage_plan,
            request_scope_evidence: self.request_scope_evidence,
            dispatch: Some(dispatch),
        }
    }
}

#[derive(Debug, Clone)]
struct TargetSearchGroup {
    group_key: String,
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
struct TargetSearchGroupKey {
    subscription_id: Uuid,
    media_type: &'static str,
    route_policy: &'static str,
    grouping_kind: &'static str,
    season_number: Option<i32>,
    air_date: Option<String>,
    target_key: Option<String>,
    retry_bucket: RetryBucket,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct SelectionPreference {
    prefer_packs_for_backfill: bool,
}

#[derive(Debug, Clone, Copy)]
struct QueueGovernorCaps {
    global: i64,
    subscription: i64,
    global_debrid: i64,
    subscription_debrid: i64,
    global_torrent: i64,
    subscription_torrent: i64,
    max_candidate_searches_per_tick: usize,
    max_submissions_per_tick: usize,
    stale_active_job_seconds: i64,
    #[allow(dead_code)]
    staged_inspection: i64,
}

impl Default for QueueGovernorCaps {
    fn default() -> Self {
        Self {
            global: DEFAULT_GLOBAL_RELEASE_JOB_CAP,
            subscription: DEFAULT_SUBSCRIPTION_RELEASE_JOB_CAP,
            global_debrid: DEFAULT_GLOBAL_DEBRID_RELEASE_JOB_CAP,
            subscription_debrid: DEFAULT_SUBSCRIPTION_DEBRID_RELEASE_JOB_CAP,
            global_torrent: DEFAULT_GLOBAL_TORRENT_RELEASE_JOB_CAP,
            subscription_torrent: DEFAULT_SUBSCRIPTION_TORRENT_RELEASE_JOB_CAP,
            max_candidate_searches_per_tick: DEFAULT_MAX_CANDIDATE_SEARCHES_PER_TICK,
            max_submissions_per_tick: DEFAULT_MAX_SUBMISSIONS_PER_TICK,
            stale_active_job_seconds: DEFAULT_STALE_ACTIVE_JOB_SECONDS,
            staged_inspection: DEFAULT_STAGED_INSPECTION_JOB_CAP,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueCapacityLimitKind {
    Global,
    Subscription,
    Route,
    SubscriptionRoute,
    SearchTick,
    SubmissionTick,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueCapacityBlock {
    kind: QueueCapacityLimitKind,
    route_logical_id: Option<String>,
    global_active: i64,
    global_limit: i64,
    subscription_active: i64,
    subscription_limit: i64,
    tick_active: usize,
    tick_limit: usize,
}

#[derive(Debug, Clone)]
enum CandidateSubmitOutcome {
    Submitted,
    CapacityBlocked(QueueCapacityBlock),
}

#[derive(Debug, Clone)]
struct QueueGovernor {
    caps: QueueGovernorCaps,
    global_active: i64,
    active_by_route: HashMap<String, i64>,
    active_by_subscription: HashMap<Uuid, i64>,
    active_by_subscription_route: HashMap<(Uuid, String), i64>,
    stale_active_jobs: i64,
    searches_this_tick: usize,
    submissions_this_tick: usize,
}

impl QueueGovernor {
    async fn load(pool: &sqlx::AnyPool) -> Result<Self> {
        let mut caps = QueueGovernorCaps::default();
        let debrid_cap = active_debrid_concurrent_downloads(pool).await?;
        caps.global_debrid = debrid_cap;
        caps.subscription_debrid = debrid_cap;
        let stale_before = Utc::now() - ChronoDuration::seconds(caps.stale_active_job_seconds);
        let mut active_by_route = HashMap::new();
        for route in [DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID] {
            active_by_route.insert(
                route.to_string(),
                count_active_release_jobs_by_route(pool, route).await?,
            );
        }
        Ok(Self {
            caps,
            global_active: count_active_release_jobs(pool).await?,
            active_by_route,
            active_by_subscription: HashMap::new(),
            active_by_subscription_route: HashMap::new(),
            stale_active_jobs: count_stale_active_release_jobs(pool, stale_before).await?,
            searches_this_tick: 0,
            submissions_this_tick: 0,
        })
    }

    async fn hydrate_subscription(
        &mut self,
        pool: &sqlx::AnyPool,
        subscription_id: Uuid,
    ) -> Result<()> {
        if self.active_by_subscription.contains_key(&subscription_id) {
            return Ok(());
        }
        let count = count_active_release_jobs_by_subscription(pool, subscription_id).await?;
        self.active_by_subscription.insert(subscription_id, count);
        Ok(())
    }

    async fn try_start_search(
        &mut self,
        pool: &sqlx::AnyPool,
        subscription: &AcquisitionSubscription,
    ) -> Result<std::result::Result<(), QueueCapacityBlock>> {
        if self.searches_this_tick >= self.caps.max_candidate_searches_per_tick {
            return Ok(Err(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::SearchTick,
                route_logical_id: None,
                global_active: self.global_active,
                global_limit: self.caps.global,
                subscription_active: 0,
                subscription_limit: self.caps.subscription,
                tick_active: self.searches_this_tick,
                tick_limit: self.caps.max_candidate_searches_per_tick,
            }));
        }

        self.hydrate_subscription(pool, subscription.subscription_id)
            .await?;
        let subscription_active = *self
            .active_by_subscription
            .get(&subscription.subscription_id)
            .unwrap_or(&0);

        if self.global_active >= self.caps.global {
            return Ok(Err(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::Global,
                route_logical_id: None,
                global_active: self.global_active,
                global_limit: self.caps.global,
                subscription_active,
                subscription_limit: self.caps.subscription,
                tick_active: self.searches_this_tick,
                tick_limit: self.caps.max_candidate_searches_per_tick,
            }));
        }
        if subscription_active >= self.caps.subscription {
            return Ok(Err(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::Subscription,
                route_logical_id: None,
                global_active: self.global_active,
                global_limit: self.caps.global,
                subscription_active,
                subscription_limit: self.caps.subscription,
                tick_active: self.searches_this_tick,
                tick_limit: self.caps.max_candidate_searches_per_tick,
            }));
        }
        let allowed_routes = route_preference_order(subscription.route_policy, None);
        if !allowed_routes.is_empty() {
            let mut first_block = None;
            let mut has_route_capacity = false;
            for route in allowed_routes {
                self.hydrate_subscription_route(pool, subscription.subscription_id, route)
                    .await?;
                if let Some(block) = self.route_capacity_block(subscription.subscription_id, route)
                {
                    first_block.get_or_insert(block);
                } else {
                    has_route_capacity = true;
                    break;
                }
            }
            if !has_route_capacity && let Some(block) = first_block {
                return Ok(Err(block));
            }
        }

        self.searches_this_tick += 1;
        Ok(Ok(()))
    }

    fn stale_active_jobs(&self) -> i64 {
        self.stale_active_jobs
    }

    async fn refresh_dispatch_capacity(
        &mut self,
        pool: &sqlx::AnyPool,
        subscription_id: Uuid,
        route_logical_id: &str,
    ) -> Result<()> {
        self.global_active = count_active_release_jobs(pool).await?;
        self.active_by_route.insert(
            route_logical_id.to_string(),
            count_active_release_jobs_by_route(pool, route_logical_id).await?,
        );
        self.active_by_subscription.insert(
            subscription_id,
            count_active_release_jobs_by_subscription(pool, subscription_id).await?,
        );
        self.active_by_subscription_route.insert(
            (subscription_id, route_logical_id.to_string()),
            count_active_release_jobs_by_subscription_route(
                pool,
                subscription_id,
                route_logical_id,
            )
            .await?,
        );
        Ok(())
    }

    fn capacity_snapshot(
        &self,
        subscription_id: Uuid,
        route_logical_id: &str,
    ) -> QueueCapacitySnapshot {
        let route_key = route_logical_id.to_string();
        let sub_key = (subscription_id, route_key.clone());
        let (route_limit, subscription_route_limit) = self
            .route_limits(route_logical_id)
            .map(|(route, subscription)| (Some(route), Some(subscription)))
            .unwrap_or((None, None));
        QueueCapacitySnapshot {
            global_active: self.global_active,
            global_limit: self.caps.global,
            subscription_active: *self
                .active_by_subscription
                .get(&subscription_id)
                .unwrap_or(&0),
            subscription_limit: self.caps.subscription,
            route_active: *self.active_by_route.get(&route_key).unwrap_or(&0),
            route_limit,
            subscription_route_active: *self
                .active_by_subscription_route
                .get(&sub_key)
                .unwrap_or(&0),
            subscription_route_limit,
            searches_this_tick: self.searches_this_tick,
            search_tick_limit: self.caps.max_candidate_searches_per_tick,
            submissions_this_tick: self.submissions_this_tick,
            submission_tick_limit: self.caps.max_submissions_per_tick,
            stale_active_jobs: self.stale_active_jobs,
        }
    }

    fn remaining_submission_slots(&self) -> usize {
        self.caps
            .max_submissions_per_tick
            .saturating_sub(self.submissions_this_tick)
    }

    fn route_capacity_block(
        &self,
        subscription_id: Uuid,
        route_logical_id: &str,
    ) -> Option<QueueCapacityBlock> {
        let route_key = route_logical_id.to_string();
        let sub_key = (subscription_id, route_key.clone());
        let (global_limit, subscription_limit) = self.route_limits(route_logical_id)?;
        let global_active = *self.active_by_route.get(&route_key).unwrap_or(&0);
        let subscription_active = *self
            .active_by_subscription_route
            .get(&sub_key)
            .unwrap_or(&0);
        if global_active >= global_limit {
            return Some(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::Route,
                route_logical_id: Some(route_key),
                global_active,
                global_limit,
                subscription_active,
                subscription_limit,
                tick_active: self.submissions_this_tick,
                tick_limit: self.caps.max_submissions_per_tick,
            });
        }
        if subscription_active >= subscription_limit {
            return Some(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::SubscriptionRoute,
                route_logical_id: Some(route_key),
                global_active,
                global_limit,
                subscription_active,
                subscription_limit,
                tick_active: self.submissions_this_tick,
                tick_limit: self.caps.max_submissions_per_tick,
            });
        }
        None
    }

    fn tick_submission_capacity_block(
        &self,
        subscription_id: Uuid,
        route_logical_id: &str,
    ) -> Option<QueueCapacityBlock> {
        (self.submissions_this_tick >= self.caps.max_submissions_per_tick).then(|| {
            let route_key = route_logical_id.to_string();
            let sub_key = (subscription_id, route_key.clone());
            let (global_limit, subscription_limit) =
                self.route_limits(route_logical_id).unwrap_or((0, 0));
            QueueCapacityBlock {
                kind: QueueCapacityLimitKind::SubmissionTick,
                route_logical_id: Some(route_key.clone()),
                global_active: *self.active_by_route.get(&route_key).unwrap_or(&0),
                global_limit,
                subscription_active: *self
                    .active_by_subscription_route
                    .get(&sub_key)
                    .unwrap_or(&0),
                subscription_limit,
                tick_active: self.submissions_this_tick,
                tick_limit: self.caps.max_submissions_per_tick,
            }
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
        self.refresh_dispatch_capacity(pool, subscription_id, route_logical_id)
            .await?;
        if let Some(block) = self.tick_submission_capacity_block(subscription_id, route_logical_id)
        {
            return Ok(Err(block));
        }
        self.hydrate_subscription(pool, subscription_id).await?;
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
        if self.global_active >= self.caps.global {
            return Err(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::Global,
                route_logical_id: Some(route_key),
                global_active: self.global_active,
                global_limit: self.caps.global,
                subscription_active: *self
                    .active_by_subscription
                    .get(&subscription_id)
                    .unwrap_or(&0),
                subscription_limit: self.caps.subscription,
                tick_active: self.submissions_this_tick,
                tick_limit: self.caps.max_submissions_per_tick,
            });
        }
        let total_subscription_active = *self
            .active_by_subscription
            .get(&subscription_id)
            .unwrap_or(&0);
        if total_subscription_active >= self.caps.subscription {
            return Err(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::Subscription,
                route_logical_id: Some(route_key),
                global_active: self.global_active,
                global_limit: self.caps.global,
                subscription_active: total_subscription_active,
                subscription_limit: self.caps.subscription,
                tick_active: self.submissions_this_tick,
                tick_limit: self.caps.max_submissions_per_tick,
            });
        }
        if let Some(block) = self.tick_submission_capacity_block(subscription_id, route_logical_id)
        {
            return Err(block);
        }
        let global_active = *self.active_by_route.get(&route_key).unwrap_or(&0);
        let subscription_active = *self
            .active_by_subscription_route
            .get(&sub_key)
            .unwrap_or(&0);
        if global_active >= global_limit {
            return Err(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::Route,
                route_logical_id: Some(route_key),
                global_active,
                global_limit,
                subscription_active,
                subscription_limit,
                tick_active: self.submissions_this_tick,
                tick_limit: self.caps.max_submissions_per_tick,
            });
        }
        if subscription_active >= subscription_limit {
            return Err(QueueCapacityBlock {
                kind: QueueCapacityLimitKind::SubscriptionRoute,
                route_logical_id: Some(route_key),
                global_active,
                global_limit,
                subscription_active,
                subscription_limit,
                tick_active: self.submissions_this_tick,
                tick_limit: self.caps.max_submissions_per_tick,
            });
        }
        *self.active_by_route.entry(route_key.clone()).or_default() += 1;
        *self
            .active_by_subscription_route
            .entry(sub_key)
            .or_default() += 1;
        *self
            .active_by_subscription
            .entry(subscription_id)
            .or_default() += 1;
        self.global_active += 1;
        self.submissions_this_tick += 1;
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
        if let Some(value) = self.active_by_subscription.get_mut(&subscription_id) {
            *value = (*value).saturating_sub(1);
        }
        self.global_active = self.global_active.saturating_sub(1);
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
    process_stale_qbittorrent_acquisition_releases(state, FALLBACK_BATCH_LIMIT).await?;
    search_due_targets(state).await?;
    retry_failed_debrid_targets_with_torrent(state).await?;
    reconcile_terminal_acquisition_requests(state).await?;
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
                    if subscription.request_mode == AcquisitionRequestMode::OneShot {
                        clear_one_shot_metadata_blocker(state, &subscription).await?;
                    }
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
                if subscription.request_mode == AcquisitionRequestMode::OneShot {
                    persist_one_shot_metadata_blocker(state, &subscription, &err.to_string())
                        .await?;
                }
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

    let groups = fair_order_search_groups(
        build_due_target_search_groups(state, &subscriptions, targets, now).await?,
    );
    let mut governor = QueueGovernor::load(&state.db_pool).await?;
    if governor.stale_active_jobs() > 0 {
        debug!(
            stale_active_jobs = governor.stale_active_jobs(),
            "acquisition queue governor found stale active jobs"
        );
    }

    for group in groups {
        let Some(subscription) = subscriptions.get(&group.representative.subscription_id) else {
            continue;
        };
        match governor
            .try_start_search(&state.db_pool, subscription)
            .await?
        {
            Ok(()) => {}
            Err(block) => {
                defer_group_for_queue_capacity(state, subscription, &group, block, now).await?;
                continue;
            }
        }
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

async fn reconcile_terminal_acquisition_requests(state: &AppState) -> Result<()> {
    let completed = complete_terminal_acquisition_requests(
        &state.db_pool,
        COMPLETION_RECONCILIATION_BATCH_LIMIT,
        "All scoped acquisition targets reached a terminal state.",
    )
    .await?;
    for item in completed {
        debug!(
            subscription_id = %item.subscription_id,
            request_mode = item.request_mode.as_str(),
            request_scope = item.request_scope.as_str(),
            target_count = item.target_count,
            imported_count = item.imported_count,
            excluded_count = item.excluded_count,
            "completed terminal acquisition request"
        );
    }
    Ok(())
}

async fn build_due_target_search_groups(
    state: &AppState,
    subscriptions: &HashMap<Uuid, AcquisitionSubscription>,
    due_targets: Vec<AcquisitionTarget>,
    now: DateTime<Utc>,
) -> Result<Vec<TargetSearchGroup>> {
    let grouped_subscription_ids = due_targets
        .iter()
        .filter(|target| target.media_type != MediaType::Movie)
        .map(|target| target.subscription_id)
        .collect::<BTreeSet<_>>();
    let mut targets_by_subscription = HashMap::<Uuid, Vec<AcquisitionTarget>>::new();
    for subscription_id in grouped_subscription_ids {
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
    let mut singleton_groups = Vec::new();
    let mut due_group_keys = BTreeSet::<TargetSearchGroupKey>::new();

    for target in due_targets {
        let Some(subscription) = subscriptions.get(&target.subscription_id) else {
            continue;
        };
        let key = target_search_group_key(subscription, &target, now);
        if key.grouping_kind == "target" || key.grouping_kind == "movie" {
            singleton_groups.push(TargetSearchGroup {
                group_key: key.as_stable_key(),
                representative: target,
                targets: Vec::new(),
                search_intent: None,
            });
            continue;
        }
        due_group_keys.insert(key);
    }

    let mut groups = singleton_groups;
    for key in due_group_keys {
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
            .filter(|target| target_search_group_key(subscription, target, now) == key)
            .cloned()
            .collect::<Vec<_>>();
        if targets.is_empty() {
            continue;
        }
        sort_targets_for_group(&mut targets);
        let representative = targets[0].clone();
        groups.push(TargetSearchGroup {
            group_key: key.as_stable_key(),
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
    let searching_targets = if group.targets.is_empty() {
        vec![target.clone()]
    } else {
        group.targets.clone()
    };
    for grouped_target in &searching_targets {
        if matches!(
            grouped_target.state,
            AcquisitionTargetState::Pending
                | AcquisitionTargetState::Searching
                | AcquisitionTargetState::Blocked
        ) {
            update_target_state(
                &state.db_pool,
                grouped_target.target_id,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Searching,
                    state_reason: Some("Searching acquisition source provider.".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        }
    }
    record_scheduler_search_audit_event(state, subscription, group, &target).await?;

    let request =
        candidate_search_request_for_group(subscription, &target, group.search_intent.clone());
    let response = search_candidates_with_store(&state.db_pool, request).await?;
    process_candidate_search_response_for_group(
        state,
        subscription,
        group,
        &response,
        &target,
        now,
        governor,
    )
    .await
}

async fn process_candidate_search_response_for_group(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    group: &TargetSearchGroup,
    response: &CandidateSearchResponse,
    target: &AcquisitionTarget,
    now: DateTime<Utc>,
    governor: &mut QueueGovernor,
) -> Result<()> {
    let grouped_targets = if !group.targets.is_empty() {
        group.targets.clone()
    } else {
        vec![target.clone()]
    };
    let mut batch = build_candidate_release_plans(
        state,
        subscription,
        response,
        target,
        &grouped_targets,
        governor,
    )
    .await?;
    let selected_plans = select_bounded_release_plans(
        std::mem::take(&mut batch.plans),
        subscription.route_policy,
        &grouped_targets,
        governor.remaining_submission_slots(),
    );

    if selected_plans.is_empty() {
        if let Some(block) = batch.capacity_block {
            defer_group_for_queue_capacity(state, subscription, group, block, now).await?;
            return Ok(());
        }
        if !batch.review_candidates.is_empty() {
            persist_group_manual_review_candidates(
                state,
                subscription,
                group,
                response,
                &grouped_targets,
                batch.review_candidates,
                now,
            )
            .await?;
            return Ok(());
        }
        let state_reason = no_matching_candidates_reason(&batch, group);
        if should_mark_no_candidate_group_terminal(subscription, target) {
            update_group_targets_terminal_no_results(
                state,
                &grouped_targets,
                response.provider.provider_id,
                state_reason,
            )
            .await?;
            start_subscription_tracking_if_initial_download_complete(
                &state.db_pool,
                subscription.subscription_id,
                now,
            )
            .await?;
            return Ok(());
        }
        let next_after = next_candidate_retry_after(subscription, &target, now);
        update_group_targets_for_retry(
            state,
            subscription,
            &grouped_targets,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Pending,
                state_reason: Some(state_reason),
                selected_provider_id: Some(response.provider.provider_id),
                next_search_after: Some(next_after),
                increment_search_attempts: true,
                ..Default::default()
            },
        )
        .await?;
        return Ok(());
    }

    for plan in selected_plans {
        debug!(
            target_id = %target.target_id,
            route = plan.route_logical_id.as_str(),
            release_kind = plan.release_kind.as_str(),
            resolver_kind = plan.resolver_kind.as_str(),
            resolver_version = plan.resolver_version.as_str(),
            covered_targets = plan.covered_target_keys.len(),
            "submitting RR-6C acquisition release plan"
        );
        let route_logical_id = plan.route_logical_id.clone();
        let dispatch = scheduler_dispatch_evidence(
            subscription,
            group,
            &plan,
            governor.capacity_snapshot(subscription.subscription_id, &route_logical_id),
        );
        let submission = plan.into_submission(dispatch);
        match submit_selected_candidate(
            state,
            subscription,
            &target,
            submission,
            Some(&route_logical_id),
            Some(governor),
        )
        .await?
        {
            CandidateSubmitOutcome::Submitted => {}
            CandidateSubmitOutcome::CapacityBlocked(block) => {
                defer_group_for_queue_capacity(state, subscription, group, block, now).await?;
                break;
            }
        }
    }
    Ok(())
}

async fn build_candidate_release_plans(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    response: &CandidateSearchResponse,
    representative: &AcquisitionTarget,
    grouped_targets: &[AcquisitionTarget],
    governor: &mut QueueGovernor,
) -> Result<CandidateReleasePlanBatch> {
    let mut batch = CandidateReleasePlanBatch::default();
    for candidate in &response.candidates {
        batch.candidate_count += 1;
        if !candidate_allowed_by_policy(candidate, subscription.route_policy) {
            batch.policy_rejected_count += 1;
            continue;
        }
        if !candidate_allowed_by_subscription_preferences(candidate, subscription) {
            batch.preference_rejected_count += 1;
            continue;
        }
        if !candidate_has_available_route_for_policy(
            candidate,
            &response.route_options,
            subscription.route_policy,
        ) {
            batch.route_unavailable_count += 1;
            continue;
        }
        let fingerprint =
            candidate_release_fingerprint(candidate, Some(response.provider.provider_id));
        if release_fingerprint_already_claimed(
            &state.db_pool,
            &response.provider.extension_id,
            &fingerprint,
        )
        .await?
        {
            batch.already_claimed_count += 1;
            continue;
        }
        let Some(coverage) =
            analyze_candidate_coverage(subscription, representative, grouped_targets, candidate)
        else {
            batch.resolver_rejected_count += 1;
            if let Some(plan) = build_manual_review_candidate_plan(
                subscription,
                representative,
                grouped_targets,
                candidate,
            )? {
                batch.review_candidates.push(plan);
            }
            continue;
        };
        if coverage.confidence == ReleaseConfidence::ReviewRequired
            || coverage.confidence == ReleaseConfidence::Low
            || coverage.covered_target_ids.is_empty()
        {
            batch.resolver_rejected_count += 1;
            if let Some(plan) = build_manual_review_candidate_plan(
                subscription,
                representative,
                grouped_targets,
                candidate,
            )? {
                batch.review_candidates.push(plan);
            }
            continue;
        }
        let route_selection = match select_candidate_route_for_plan(
            &state.db_pool,
            subscription,
            candidate,
            &response.route_options,
            governor,
        )
        .await
        {
            Ok(selection) => selection,
            Err(err) => {
                debug!(
                    candidate_title = candidate.title.as_str(),
                    subscription_id = %subscription.subscription_id,
                    "candidate route unavailable during RR-6C planning: {err}"
                );
                batch.route_unavailable_count += 1;
                continue;
            }
        };
        match route_selection {
            Ok(route_logical_id) => batch.plans.push(CandidateReleasePlan {
                provider_id: response.provider.provider_id,
                source_extension_id: response.provider.extension_id.clone(),
                provider_warnings: response.warnings.clone(),
                route_logical_id,
                fingerprint,
                selection: coverage.selection,
                release_kind: coverage.release_kind,
                resolver_kind: coverage.resolver_kind,
                resolver_version: coverage.resolver_version,
                confidence: coverage.confidence,
                covered_target_ids: coverage.covered_target_ids,
                covered_target_keys: coverage.covered_target_keys,
                overfetch_count: coverage.overfetch_count,
                request_scope_evidence: Some(request_scope_resolution_evidence(
                    subscription,
                    grouped_targets,
                )),
            }),
            Err(block) => {
                batch.capacity_block.get_or_insert(block);
            }
        }
    }
    batch
        .plans
        .sort_by(|left, right| compare_release_plans(right, left, subscription.route_policy));
    batch.review_candidates.sort_by(|left, right| {
        compare_review_candidate_plans(right, left, subscription.route_policy)
    });
    Ok(batch)
}

fn no_matching_candidates_reason(
    batch: &CandidateReleasePlanBatch,
    group: &TargetSearchGroup,
) -> String {
    let intent = group
        .search_intent
        .as_ref()
        .map(|intent| intent.kind.as_str())
        .unwrap_or("target");
    if batch.candidate_count == 0 {
        return format!("No acquisition candidates were returned for {intent}.");
    }
    if batch.policy_rejected_count == batch.candidate_count {
        return format!(
            "Candidates were returned for {intent}, but none matched the configured route policy."
        );
    }
    if batch.preference_rejected_count == batch.candidate_count {
        return format!(
            "Candidates were returned for {intent}, but none matched the subscription preferences."
        );
    }
    if batch.already_claimed_count == batch.candidate_count {
        return format!(
            "Candidates were returned for {intent}, but every candidate has already been tried or suppressed."
        );
    }
    if batch.route_unavailable_count > 0 && batch.route_unavailable_count >= batch.candidate_count {
        return format!(
            "Candidates were returned for {intent}, but no allowed acquisition route is currently available."
        );
    }
    if batch.resolver_rejected_count > 0 {
        return format!(
            "Candidates were returned for {intent}, but Elixir could not safely match them."
        );
    }
    format!("No matching acquisition candidates were found for {intent}.")
}

async fn persist_group_manual_review_candidates(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    group: &TargetSearchGroup,
    response: &CandidateSearchResponse,
    grouped_targets: &[AcquisitionTarget],
    review_candidates: Vec<CandidateReviewPlan>,
    now: DateTime<Utc>,
) -> Result<()> {
    let target_scope = manual_review_target_scope(subscription, grouped_targets);
    let selected = select_bounded_review_candidates(review_candidates);
    let mut release_count = 0usize;
    for plan in selected {
        let route_policy =
            manual_review_route_policy_evidence(subscription, response, &plan.candidate);
        upsert_manual_review_candidate_release(
            &state.db_pool,
            NewManualReviewCandidateRelease {
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: Some(response.provider.provider_id),
                source_extension_id: response.provider.extension_id.clone(),
                owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
                media_type: subscription.media_type,
                title: subscription.title.clone(),
                candidate: plan.candidate,
                target_scope: target_scope.clone(),
                resolver_evidence: ManualReviewResolverEvidence {
                    resolver_kind: plan.resolver_kind,
                    resolver_version: plan.resolver_version,
                    parsed_release: plan.parsed_release,
                    rejection_codes: plan.rejection_codes,
                    candidate_score: plan.score,
                    reason: Some(plan.reason.clone()),
                },
                route_policy,
                release_kind: plan.release_kind,
                score: plan.score,
                state_reason: Some(plan.reason),
            },
        )
        .await?;
        release_count += 1;
    }

    let next_after = next_candidate_retry_after(subscription, &group.representative, now);
    update_group_targets_for_retry(
        state,
        subscription,
        grouped_targets,
        AcquisitionTargetStateUpdate {
            state: AcquisitionTargetState::Pending,
            state_reason: Some(format!(
                "Candidates found; awaiting manual release selection ({release_count} review item{}).",
                if release_count == 1 { "" } else { "s" }
            )),
            selected_provider_id: Some(response.provider.provider_id),
            next_search_after: Some(next_after),
            increment_search_attempts: true,
            ..Default::default()
        },
    )
    .await?;
    debug!(
        subscription_id = %subscription.subscription_id,
        provider_id = %response.provider.provider_id,
        group_key = group.group_key.as_str(),
        release_count,
        "persisted manual review acquisition candidates"
    );
    Ok(())
}

fn select_bounded_review_candidates(
    mut candidates: Vec<CandidateReviewPlan>,
) -> Vec<CandidateReviewPlan> {
    candidates.truncate(MAX_MANUAL_REVIEW_CANDIDATES_PER_GROUP);
    candidates
}

fn build_manual_review_candidate_plan(
    subscription: &AcquisitionSubscription,
    representative: &AcquisitionTarget,
    grouped_targets: &[AcquisitionTarget],
    candidate: &AcquisitionCandidate,
) -> Result<Option<CandidateReviewPlan>> {
    let targets = if grouped_targets.is_empty() {
        vec![representative.clone()]
    } else {
        grouped_targets.to_vec()
    };
    match subscription.media_type {
        MediaType::Movie => Ok(None),
        MediaType::Series => Ok(Some(build_tv_manual_review_candidate(candidate, &targets)?)),
        MediaType::Anime => Ok(Some(build_anime_manual_review_candidate(
            subscription,
            representative,
            &targets,
            candidate,
        )?)),
    }
}

fn build_tv_manual_review_candidate(
    candidate: &AcquisitionCandidate,
    targets: &[AcquisitionTarget],
) -> Result<CandidateReviewPlan> {
    let tv_targets = tv_targets_for_acquisition_targets(targets);
    let resolver = TvSonarrStyleResolver;
    let parsed = resolver.parse_title(&candidate.title);
    let files = tv_release_file_inputs(candidate);
    let plan = resolver.plan_coverage(
        &parsed,
        &tv_targets,
        &files,
        TvCoverageOptions {
            allow_partial_pack: false,
            file_selection_supported: candidate_file_selection_supported(candidate),
        },
    );
    let mut rejection_codes = plan
        .rejection_reasons
        .iter()
        .map(|reason| reason.as_str().to_string())
        .collect::<Vec<_>>();
    if tv_targets.is_empty() {
        rejection_codes.push("missing_tv_target_scope".to_string());
    }
    if plan.confidence == ReleaseConfidence::ReviewRequired {
        rejection_codes.push("review_required_confidence".to_string());
    }
    if plan.confidence == ReleaseConfidence::Low {
        rejection_codes.push("low_confidence".to_string());
    }
    if plan.entries.is_empty() {
        rejection_codes.push("no_safe_target_coverage".to_string());
    }
    rejection_codes.sort();
    rejection_codes.dedup();
    let release_kind = if plan.release_kind == ReleaseKind::Unknown {
        parsed.release_kind
    } else {
        plan.release_kind
    };
    let reason = manual_review_reason(&rejection_codes, "TV");
    Ok(CandidateReviewPlan {
        candidate: candidate.clone(),
        release_kind,
        resolver_kind: ReleaseResolverKind::TvSonarrStyle,
        resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
        confidence: plan.confidence,
        rejection_codes,
        parsed_release: Some(json!({
            "parsed": parsed,
            "coveragePlan": plan,
        })),
        score: candidate.score,
        reason,
    })
}

fn build_anime_manual_review_candidate(
    subscription: &AcquisitionSubscription,
    representative: &AcquisitionTarget,
    targets: &[AcquisitionTarget],
    candidate: &AcquisitionCandidate,
) -> Result<CandidateReviewPlan> {
    let Some(context) = anime_candidate_scoring_context(subscription, representative, targets)
    else {
        return Ok(CandidateReviewPlan {
            candidate: candidate.clone(),
            release_kind: ReleaseKind::Unknown,
            resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
            resolver_version: ANIME_SHOKO_STYLE_RESOLVER_VERSION.to_string(),
            confidence: ReleaseConfidence::ReviewRequired,
            rejection_codes: vec!["missing_anime_graph_context".to_string()],
            parsed_release: Some(json!({
                "candidateTitle": candidate.title,
                "reason": "anime metadata graph context was unavailable"
            })),
            score: candidate.score,
            reason: "Anime candidate needs review because metadata graph context is unavailable."
                .to_string(),
        });
    };
    let input = anime_candidate_input(candidate);
    let files = anime_release_file_inputs(candidate);
    let plan = plan_anime_file_coverage(&context, &input, &files);
    let mut rejection_codes = plan.rejection_reasons.clone();
    rejection_codes.extend(plan.review_reasons.iter().cloned());
    if plan.confidence == ReleaseConfidence::ReviewRequired {
        rejection_codes.push("review_required_confidence".to_string());
    }
    if plan.confidence == ReleaseConfidence::Low {
        rejection_codes.push("low_confidence".to_string());
    }
    if plan.entries.is_empty() {
        rejection_codes.push("no_safe_target_coverage".to_string());
    }
    rejection_codes.sort();
    rejection_codes.dedup();
    let score = score_anime_candidate(&context, &input);
    let reason = manual_review_reason(&rejection_codes, "Anime");
    Ok(CandidateReviewPlan {
        candidate: candidate.clone(),
        release_kind: plan.release_kind,
        resolver_kind: plan.resolver_kind,
        resolver_version: plan.resolver_version.clone(),
        confidence: plan.confidence,
        rejection_codes,
        parsed_release: Some(json!({
            "coveragePlan": plan,
            "graphFingerprint": context.graph_fingerprint,
            "aliasCount": context.aliases.len(),
            "targetCount": context.targets.len(),
        })),
        score: Some(score.score),
        reason,
    })
}

fn manual_review_reason(rejection_codes: &[String], label: &str) -> String {
    if rejection_codes.is_empty() {
        return format!(
            "{label} candidate needs manual review before Elixir can safely download it."
        );
    }
    format!(
        "{label} candidate needs manual review before download: {}.",
        rejection_codes.join(", ")
    )
}

fn manual_review_target_scope(
    subscription: &AcquisitionSubscription,
    targets: &[AcquisitionTarget],
) -> ManualReviewTargetScope {
    ManualReviewTargetScope {
        subscription_id: Some(subscription.subscription_id),
        media_type: subscription.media_type,
        targets: targets.iter().map(|target| target.target_id).collect(),
        target_keys: targets
            .iter()
            .map(|target| target.target_key.clone())
            .collect(),
        season_number: common_season_number(targets),
        episode_numbers: targets
            .iter()
            .filter_map(|target| target.episode_number)
            .collect(),
        absolute_episode_numbers: targets
            .iter()
            .filter_map(|target| target.absolute_episode_number)
            .collect(),
    }
}

fn request_scope_resolution_evidence(
    subscription: &AcquisitionSubscription,
    targets: &[AcquisitionTarget],
) -> JsonValue {
    json!({
        "requestMode": subscription.request_mode.as_str(),
        "requestScope": subscription.request_scope.as_str(),
        "metadataPolicy": subscription.metadata_policy.as_str(),
        "completionPolicy": subscription.completion_policy.as_str(),
        "monitorPolicy": subscription.monitor_policy.as_str(),
        "scope": subscription.scope.clone(),
        "targetCount": targets.len(),
        "targetIds": targets
            .iter()
            .map(|target| target.target_id.to_string())
            .collect::<Vec<_>>(),
        "targetKeys": targets
            .iter()
            .map(|target| target.target_key.clone())
            .collect::<Vec<_>>(),
        "seasonNumbers": targets
            .iter()
            .filter_map(|target| target.season_number)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
        "episodeNumbers": targets
            .iter()
            .filter_map(|target| target.episode_number)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
        "absoluteEpisodeNumbers": targets
            .iter()
            .filter_map(|target| target.absolute_episode_number)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
    })
}

fn coverage_plan_with_request_scope(
    plan: JsonValue,
    request_scope_evidence: Option<&JsonValue>,
) -> JsonValue {
    let Some(evidence) = request_scope_evidence else {
        return plan;
    };
    match plan {
        JsonValue::Object(mut object) => {
            object.insert("requestScopeEvidence".to_string(), evidence.clone());
            JsonValue::Object(object)
        }
        other => json!({
            "coveragePlan": other,
            "requestScopeEvidence": evidence,
        }),
    }
}

fn manual_review_route_policy_evidence(
    subscription: &AcquisitionSubscription,
    response: &CandidateSearchResponse,
    candidate: &AcquisitionCandidate,
) -> ManualReviewRoutePolicyEvidence {
    let mut allowed_routes = route_preference_order(
        subscription.route_policy,
        candidate.default_route.as_deref(),
    )
    .into_iter()
    .filter(|route| candidate_supports_route(candidate, route))
    .filter(|route| {
        response
            .route_options
            .iter()
            .any(|option| option.logical_id == *route)
    })
    .map(ToString::to_string)
    .collect::<Vec<_>>();
    if allowed_routes.is_empty() {
        allowed_routes = candidate.supported_routes.clone();
    }
    ManualReviewRoutePolicyEvidence {
        preferred: Some(subscription.route_policy.as_str().to_string()),
        allowed_routes,
    }
}

fn candidate_has_available_route_for_policy(
    candidate: &AcquisitionCandidate,
    route_options: &[CandidateRouteOption],
    route_policy: AcquisitionRoutePolicy,
) -> bool {
    route_preference_order(route_policy, candidate.default_route.as_deref())
        .into_iter()
        .any(|route| {
            candidate_supports_route(candidate, route)
                && route_option_available(route_options, route)
        })
}

fn scheduler_dispatch_evidence(
    subscription: &AcquisitionSubscription,
    group: &TargetSearchGroup,
    plan: &CandidateReleasePlan,
    capacity_snapshot: QueueCapacitySnapshot,
) -> SchedulerDispatchEvidence {
    let candidate = &plan.selection.candidate;
    SchedulerDispatchEvidence {
        scheduler_phase: "rr6c",
        group_key: group.group_key.clone(),
        search_intent: group.search_intent.clone(),
        selected_plan_score: SchedulerPlanScoreEvidence {
            confidence: plan.confidence.as_str().to_string(),
            covered_target_count: plan.covered_target_ids.len(),
            release_kind: plan.release_kind.as_str().to_string(),
            resolver_kind: plan.resolver_kind.as_str().to_string(),
            resolver_version: plan.resolver_version.clone(),
            route_preference_score: route_preference_score(
                &plan.route_logical_id,
                subscription.route_policy,
            ),
            cached_debrid_score: cached_debrid_score(candidate.cached_debrid),
            freshness_score: candidate_freshness_score(candidate),
            tracker_count: candidate_tracker_count(candidate),
            quality_score: quality_score(candidate.quality.as_deref()),
            seeders: candidate.seeders,
            overfetch_count: plan.overfetch_count,
            source_rank: candidate.rank,
            source_score: candidate.score,
            score_tuple: release_plan_score_tuple(plan, subscription.route_policy),
        },
        capacity_snapshot,
        route_decision: SchedulerRouteDecisionEvidence {
            route_policy: subscription.route_policy.as_str().to_string(),
            selected_route_logical_id: plan.route_logical_id.clone(),
            default_route: candidate.default_route.clone(),
            supported_routes: candidate.supported_routes.clone(),
            reason: route_decision_reason(subscription.route_policy, &plan.route_logical_id),
        },
    }
}

async fn record_scheduler_search_audit_event(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    group: &TargetSearchGroup,
    target: &AcquisitionTarget,
) -> Result<()> {
    let grouped_targets = if group.targets.is_empty() {
        vec![target.clone()]
    } else {
        group.targets.clone()
    };
    record_acquisition_audit_event(
        &state.db_pool,
        NewAcquisitionAuditEvent {
            event_type: EVENT_ACQUISITION_SEARCH_SCHEDULED.to_string(),
            subscription_id: Some(subscription.subscription_id),
            target_id: Some(target.target_id),
            state: Some(AcquisitionTargetState::Searching.as_str().to_string()),
            reason: Some(
                "Scheduler selected scoped acquisition targets for provider search.".to_string(),
            ),
            evidence: Some(json!({
                "requestMode": subscription.request_mode.as_str(),
                "requestScope": subscription.request_scope.as_str(),
                "metadataPolicy": subscription.metadata_policy.as_str(),
                "completionPolicy": subscription.completion_policy.as_str(),
                "routePolicy": subscription.route_policy.as_str(),
                "groupKey": group.group_key.as_str(),
                "searchIntent": group.search_intent.as_ref(),
                "targetCount": grouped_targets.len(),
                "targetKeys": grouped_targets
                    .iter()
                    .map(|item| item.target_key.clone())
                    .collect::<Vec<_>>(),
            })),
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

fn compare_review_candidate_plans(
    left: &CandidateReviewPlan,
    right: &CandidateReviewPlan,
    route_policy: AcquisitionRoutePolicy,
) -> Ordering {
    review_candidate_score_tuple(left, route_policy)
        .cmp(&review_candidate_score_tuple(right, route_policy))
        .then_with(|| right.candidate.title.cmp(&left.candidate.title))
}

fn review_candidate_score_tuple(
    plan: &CandidateReviewPlan,
    route_policy: AcquisitionRoutePolicy,
) -> (i32, i32, i32, i32, i64, i32, i32) {
    (
        confidence_rank(plan.confidence),
        release_kind_rank(plan.release_kind),
        route_preference_score(
            plan.candidate.default_route.as_deref().unwrap_or_default(),
            route_policy,
        ),
        candidate_freshness_score(&plan.candidate),
        plan.candidate.seeders.unwrap_or_default() as i64,
        (plan
            .score
            .unwrap_or_else(|| plan.candidate.score.unwrap_or(0.0))
            * 1000.0)
            .round() as i32,
        source_rank_score(plan.candidate.rank),
    )
}

fn route_decision_reason(route_policy: AcquisitionRoutePolicy, route_logical_id: &str) -> String {
    match (route_policy, route_logical_id) {
        (AcquisitionRoutePolicy::DebridFirst, DEBRID_DEFAULT_LOGICAL_ID) => {
            "debrid-first policy selected available debrid route".to_string()
        }
        (AcquisitionRoutePolicy::DebridFirst, TORRENT_DEFAULT_LOGICAL_ID) => {
            "debrid-first policy selected torrent fallback due to debrid capacity or availability"
                .to_string()
        }
        (AcquisitionRoutePolicy::DebridOnly, _) => {
            "debrid-only policy selected debrid route".to_string()
        }
        (AcquisitionRoutePolicy::TorrentOnly, _) => {
            "torrent-only policy selected torrent route".to_string()
        }
        (AcquisitionRoutePolicy::Manual, _) => {
            "manual route policy selected candidate/default route".to_string()
        }
        (_, _) => "route policy selected supported candidate route".to_string(),
    }
}

#[derive(Debug)]
struct CandidateCoverageAnalysis {
    selection: CandidateSelection,
    release_kind: ReleaseKind,
    resolver_kind: ReleaseResolverKind,
    resolver_version: String,
    confidence: ReleaseConfidence,
    covered_target_ids: BTreeSet<Uuid>,
    covered_target_keys: BTreeSet<String>,
    overfetch_count: usize,
}

fn analyze_candidate_coverage(
    subscription: &AcquisitionSubscription,
    representative: &AcquisitionTarget,
    grouped_targets: &[AcquisitionTarget],
    candidate: &AcquisitionCandidate,
) -> Option<CandidateCoverageAnalysis> {
    let targets = if grouped_targets.is_empty() {
        vec![representative.clone()]
    } else {
        grouped_targets.to_vec()
    };
    match subscription.media_type {
        MediaType::Anime => {
            analyze_anime_candidate_coverage(subscription, representative, &targets, candidate)
        }
        MediaType::Series => analyze_tv_candidate_coverage(candidate, &targets),
        MediaType::Movie => Some(analyze_movie_candidate_coverage(candidate, representative)),
    }
}

fn analyze_anime_candidate_coverage(
    subscription: &AcquisitionSubscription,
    representative: &AcquisitionTarget,
    targets: &[AcquisitionTarget],
    candidate: &AcquisitionCandidate,
) -> Option<CandidateCoverageAnalysis> {
    let context = anime_candidate_scoring_context(subscription, representative, targets)?;
    let input = anime_candidate_input(candidate);
    let plan = plan_anime_file_coverage(&context, &input, &anime_release_file_inputs(candidate));
    if !plan.rejection_reasons.is_empty()
        || plan.confidence == ReleaseConfidence::ReviewRequired
        || plan.entries.is_empty()
    {
        return None;
    }
    let score = score_anime_candidate(&context, &input);
    let target_by_key = targets
        .iter()
        .map(|target| (target.target_key.clone(), target))
        .collect::<HashMap<_, _>>();
    let mut covered_target_ids = BTreeSet::new();
    let mut covered_target_keys = BTreeSet::new();
    for entry in &plan.entries {
        if let Some(target) = target_by_key.get(&entry.target_key) {
            covered_target_ids.insert(target.target_id);
            covered_target_keys.insert(target.target_key.clone());
        }
    }
    let selected_files = plan.selected_file_keys.len();
    Some(CandidateCoverageAnalysis {
        selection: anime_scored_candidate(candidate, score, Some(plan.clone())),
        release_kind: plan.release_kind,
        resolver_kind: plan.resolver_kind,
        resolver_version: plan.resolver_version.clone(),
        confidence: plan.confidence,
        covered_target_ids,
        covered_target_keys,
        overfetch_count: candidate_media_file_count(&candidate).saturating_sub(selected_files),
    })
}

fn analyze_tv_candidate_coverage(
    candidate: &AcquisitionCandidate,
    targets: &[AcquisitionTarget],
) -> Option<CandidateCoverageAnalysis> {
    let tv_targets = tv_targets_for_acquisition_targets(targets);
    if tv_targets.is_empty() {
        return None;
    }
    let resolver = TvSonarrStyleResolver;
    let parsed = resolver.parse_title(&candidate.title);
    let files = tv_release_file_inputs(candidate);
    let plan = resolver.plan_coverage(
        &parsed,
        &tv_targets,
        &files,
        TvCoverageOptions {
            allow_partial_pack: false,
            file_selection_supported: candidate_file_selection_supported(candidate),
        },
    );
    if plan.confidence == ReleaseConfidence::ReviewRequired || plan.entries.is_empty() {
        return None;
    }
    let mut candidate = candidate.clone();
    candidate.score_badges.push(CandidateScoreBadge {
        label: "TV match".to_string(),
        detail: Some(format!(
            "{} coverage entries via {}",
            plan.entries.len(),
            TV_SONARR_STYLE_RESOLVER_VERSION
        )),
        score: Some(plan.entries.len() as f64),
    });
    let covered_target_ids = plan
        .entries
        .iter()
        .map(|entry| entry.target_id)
        .collect::<BTreeSet<_>>();
    let covered_target_keys = plan
        .entries
        .iter()
        .map(|entry| entry.target_key.clone())
        .collect::<BTreeSet<_>>();
    let selected_files = plan
        .entries
        .iter()
        .filter_map(|entry| entry.release_file_id.as_ref())
        .collect::<BTreeSet<_>>()
        .len();
    let overfetch_count = candidate_media_file_count(&candidate).saturating_sub(selected_files);
    Some(CandidateCoverageAnalysis {
        selection: CandidateSelection {
            candidate,
            anime_coverage_plan: None,
            tv_coverage_plan: Some(plan.clone()),
        },
        release_kind: plan.release_kind,
        resolver_kind: plan.resolver_kind,
        resolver_version: plan.resolver_version.clone(),
        confidence: plan.confidence,
        covered_target_ids,
        covered_target_keys,
        overfetch_count,
    })
}

fn analyze_movie_candidate_coverage(
    candidate: &AcquisitionCandidate,
    target: &AcquisitionTarget,
) -> CandidateCoverageAnalysis {
    CandidateCoverageAnalysis {
        selection: CandidateSelection {
            candidate: candidate.clone(),
            anime_coverage_plan: None,
            tv_coverage_plan: None,
        },
        release_kind: ReleaseKind::Single,
        resolver_kind: ReleaseResolverKind::MovieSingle,
        resolver_version: "rr6-movie-single-v0".to_string(),
        confidence: ReleaseConfidence::High,
        covered_target_ids: BTreeSet::from([target.target_id]),
        covered_target_keys: BTreeSet::from([target.target_key.clone()]),
        overfetch_count: 0,
    }
}

async fn release_fingerprint_already_claimed(
    pool: &sqlx::AnyPool,
    source_extension_id: &str,
    fingerprint: &str,
) -> Result<bool> {
    let Some(release) = get_release_by_fingerprint(
        pool,
        DEFAULT_ROUTE_OWNER_ID,
        source_extension_id,
        fingerprint,
    )
    .await?
    else {
        return Ok(false);
    };
    Ok(release_suppresses_automatic_rediscovery(&release)
        || !matches!(
            release.state,
            AcquisitionReleaseState::Failed | AcquisitionReleaseState::Cancelled
        ))
}

fn release_suppresses_automatic_rediscovery(
    release: &crate::acquisition::release_resolution::models::AcquisitionRelease,
) -> bool {
    release.coverage_plan.as_ref().is_some_and(|plan| {
        json_status(plan, &["retrySuppression", "status"]) == Some("rejected")
            || json_status(plan, &["manualReview", "status"]) == Some("rejected")
            || json_bool(plan, &["retrySuppression", "suppressAutomaticRediscovery"]) == Some(true)
    })
}

fn json_status<'a>(value: &'a JsonValue, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str()
}

fn json_bool(value: &JsonValue, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_bool()
}

async fn select_candidate_route_for_plan(
    pool: &sqlx::AnyPool,
    subscription: &AcquisitionSubscription,
    candidate: &AcquisitionCandidate,
    route_options: &[CandidateRouteOption],
    governor: &mut QueueGovernor,
) -> Result<std::result::Result<String, QueueCapacityBlock>> {
    let mut first_capacity_block = None;
    for route in route_preference_order(
        subscription.route_policy,
        candidate.default_route.as_deref(),
    ) {
        if !candidate_supports_route(candidate, route)
            || !route_option_available(route_options, route)
        {
            continue;
        }
        governor
            .hydrate_subscription_route(pool, subscription.subscription_id, route)
            .await?;
        if let Some(block) = governor.route_capacity_block(subscription.subscription_id, route) {
            first_capacity_block.get_or_insert(block);
            continue;
        }
        return Ok(Ok(route.to_string()));
    }
    if let Some(block) = first_capacity_block {
        return Ok(Err(block));
    }
    bail!(
        "candidate has no available route for policy '{}'",
        subscription.route_policy.as_str()
    )
}

fn select_bounded_release_plans(
    plans: Vec<CandidateReleasePlan>,
    route_policy: AcquisitionRoutePolicy,
    targets: &[AcquisitionTarget],
    limit: usize,
) -> Vec<CandidateReleasePlan> {
    if limit == 0 {
        return Vec::new();
    }
    let wanted_target_ids = targets
        .iter()
        .map(|target| target.target_id)
        .collect::<BTreeSet<_>>();
    let mut sorted = plans;
    sorted.sort_by(|left, right| compare_release_plans(right, left, route_policy));
    let mut selected = Vec::new();
    let mut covered = BTreeSet::new();
    for plan in sorted {
        if selected.len() >= limit {
            break;
        }
        if !wanted_target_ids.is_empty()
            && plan
                .covered_target_ids
                .iter()
                .all(|target_id| !wanted_target_ids.contains(target_id))
        {
            continue;
        }
        if plan
            .covered_target_ids
            .iter()
            .all(|target_id| covered.contains(target_id))
        {
            continue;
        }
        covered.extend(plan.covered_target_ids.iter().copied());
        selected.push(plan);
        if !wanted_target_ids.is_empty() && wanted_target_ids.iter().all(|id| covered.contains(id))
        {
            break;
        }
    }
    selected
}

fn compare_release_plans(
    left: &CandidateReleasePlan,
    right: &CandidateReleasePlan,
    route_policy: AcquisitionRoutePolicy,
) -> Ordering {
    release_plan_score_tuple(left, route_policy)
        .cmp(&release_plan_score_tuple(right, route_policy))
        .then_with(|| right.fingerprint.cmp(&left.fingerprint))
}

fn release_plan_score_tuple(
    plan: &CandidateReleasePlan,
    route_policy: AcquisitionRoutePolicy,
) -> (i32, usize, i32, i32, i32, i32, i32, i64, i32, i32, i32) {
    let candidate = &plan.selection.candidate;
    (
        confidence_rank(plan.confidence),
        plan.covered_target_ids.len(),
        route_preference_score(&plan.route_logical_id, route_policy),
        cached_debrid_score(candidate.cached_debrid),
        candidate_freshness_score(candidate),
        quality_score(candidate.quality.as_deref()),
        release_kind_rank(plan.release_kind),
        candidate.seeders.unwrap_or_default() as i64,
        -(plan.overfetch_count as i32),
        (candidate.score.unwrap_or(0.0) * 1000.0).round() as i32,
        source_rank_score(candidate.rank),
    )
}

fn confidence_rank(confidence: ReleaseConfidence) -> i32 {
    match confidence {
        ReleaseConfidence::High => 4,
        ReleaseConfidence::Medium => 3,
        ReleaseConfidence::Low => 1,
        ReleaseConfidence::ReviewRequired => 0,
    }
}

fn release_kind_rank(kind: ReleaseKind) -> i32 {
    match kind {
        ReleaseKind::SeriesPack => 5,
        ReleaseKind::MultiSeasonPack => 4,
        ReleaseKind::SeasonPack => 3,
        ReleaseKind::MultiEpisode => 2,
        ReleaseKind::Single => 1,
        ReleaseKind::Unknown => 0,
    }
}

fn route_preference_score(route: &str, route_policy: AcquisitionRoutePolicy) -> i32 {
    match route_policy {
        AcquisitionRoutePolicy::DebridFirst => {
            if route == DEBRID_DEFAULT_LOGICAL_ID {
                2
            } else if route == TORRENT_DEFAULT_LOGICAL_ID {
                1
            } else {
                0
            }
        }
        AcquisitionRoutePolicy::DebridOnly => (route == DEBRID_DEFAULT_LOGICAL_ID) as i32,
        AcquisitionRoutePolicy::TorrentOnly => (route == TORRENT_DEFAULT_LOGICAL_ID) as i32,
        AcquisitionRoutePolicy::Manual => 1,
    }
}

fn cached_debrid_score(value: Option<bool>) -> i32 {
    match value {
        Some(true) => 2,
        None => 1,
        Some(false) => 0,
    }
}

fn source_rank_score(rank: Option<u32>) -> i32 {
    rank.map(|rank| 10_000 - rank as i32).unwrap_or_default()
}

fn candidate_freshness_score(candidate: &AcquisitionCandidate) -> i32 {
    if candidate.source_kind.eq_ignore_ascii_case("http") {
        return 4;
    }
    let mut score = match candidate.cached_debrid {
        Some(true) => 8,
        Some(false) => -2,
        None => 0,
    };
    if !candidate.source_kind.eq_ignore_ascii_case("magnet")
        || candidate.cached_debrid == Some(true)
    {
        return score;
    }
    score += match candidate.seeders {
        Some(100..) => 5,
        Some(50..=99) => 4,
        Some(15..=49) => 1,
        Some(5..=14) => -1,
        Some(1..=4) => -4,
        Some(0) => -7,
        None => -2,
    };
    score += match candidate_tracker_count(candidate) {
        3.. => 3,
        1..=2 => 1,
        _ => -5,
    };
    score
}

fn candidate_tracker_count(candidate: &AcquisitionCandidate) -> usize {
    acquisition_candidate_tracker_count(candidate)
}

impl TargetSearchGroupKey {
    fn as_stable_key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}",
            self.subscription_id,
            self.media_type,
            self.route_policy,
            self.grouping_kind,
            self.season_number
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            self.air_date.as_deref().unwrap_or("-"),
            self.target_key.as_deref().unwrap_or("-")
        )
    }
}

fn target_search_group_key(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    now: DateTime<Utc>,
) -> TargetSearchGroupKey {
    let retry_bucket = retry_bucket_for_target(subscription, target, now);
    let target_key = target.target_key.clone();
    let (grouping_kind, season_number, air_date, target_key) = match subscription.media_type {
        MediaType::Movie => ("movie", None, None, Some(target_key)),
        MediaType::Series | MediaType::Anime
            if target.air_date.is_some()
                && (target.season_number.is_none()
                    || target.target_key.to_ascii_uppercase().starts_with("DATE:")) =>
        {
            ("daily", None, target.air_date.clone(), None)
        }
        MediaType::Series | MediaType::Anime if target.season_number.is_some() => {
            ("season", target.season_number, None, None)
        }
        MediaType::Anime if target.absolute_episode_number.is_some() => {
            ("absolute", None, None, None)
        }
        _ => ("target", None, None, Some(target_key)),
    };
    TargetSearchGroupKey {
        subscription_id: target.subscription_id,
        media_type: media_type_key(subscription.media_type),
        route_policy: subscription.route_policy.as_str(),
        grouping_kind,
        season_number,
        air_date,
        target_key,
        retry_bucket,
    }
}

fn media_type_key(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movie",
        MediaType::Series => "series",
        MediaType::Anime => "anime",
    }
}

fn route_preference_order(
    route_policy: AcquisitionRoutePolicy,
    default_route: Option<&str>,
) -> Vec<&'static str> {
    match route_policy {
        AcquisitionRoutePolicy::DebridFirst => {
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID]
        }
        AcquisitionRoutePolicy::DebridOnly => vec![DEBRID_DEFAULT_LOGICAL_ID],
        AcquisitionRoutePolicy::TorrentOnly => vec![TORRENT_DEFAULT_LOGICAL_ID],
        AcquisitionRoutePolicy::Manual => {
            match default_route.and_then(|route| match route {
                DEBRID_DEFAULT_LOGICAL_ID => Some(DEBRID_DEFAULT_LOGICAL_ID),
                TORRENT_DEFAULT_LOGICAL_ID => Some(TORRENT_DEFAULT_LOGICAL_ID),
                _ => None,
            }) {
                Some(route) => vec![route],
                None => vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            }
        }
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
        .then_with(|| left.group_key.cmp(&right.group_key))
}

fn fair_order_search_groups(groups: Vec<TargetSearchGroup>) -> Vec<TargetSearchGroup> {
    let mut by_subscription = HashMap::<Uuid, Vec<TargetSearchGroup>>::new();
    for group in groups {
        by_subscription
            .entry(group.representative.subscription_id)
            .or_default()
            .push(group);
    }
    let mut lanes = by_subscription.into_iter().collect::<Vec<_>>();
    for (_, lane) in &mut lanes {
        lane.sort_by(compare_search_groups);
    }
    lanes.sort_by(|(left_id, left_lane), (right_id, right_lane)| {
        let left = left_lane.first();
        let right = right_lane.first();
        match (left, right) {
            (Some(left), Some(right)) => {
                compare_search_groups(left, right).then_with(|| left_id.cmp(right_id))
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left_id.cmp(right_id),
        }
    });

    let mut ordered = Vec::new();
    loop {
        let mut emitted = false;
        for (_, lane) in &mut lanes {
            if lane.is_empty() {
                continue;
            }
            ordered.push(lane.remove(0));
            emitted = true;
        }
        if !emitted {
            break;
        }
    }
    ordered
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
        return if targets
            .first()
            .and_then(|target| target.air_date.as_ref())
            .is_some()
        {
            "daily"
        } else {
            "episode"
        };
    }
    let air_dates = targets
        .iter()
        .filter_map(|target| target.air_date.as_deref())
        .collect::<BTreeSet<_>>();
    if air_dates.len() == 1 {
        return "daily";
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

fn should_mark_no_candidate_group_terminal(
    subscription: &AcquisitionSubscription,
    representative: &AcquisitionTarget,
) -> bool {
    if subscription.request_mode == AcquisitionRequestMode::OneShot
        && subscription.completion_policy == AcquisitionCompletionPolicy::TerminalSelectedTargets
    {
        return true;
    }

    subscription.request_mode == AcquisitionRequestMode::Monitored
        && subscription.tracking_started_at.is_none()
        && representative.search_attempts + 1 >= INITIAL_BACKFILL_NO_CANDIDATE_TERMINAL_ATTEMPTS
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

async fn update_group_targets_terminal_no_results(
    state: &AppState,
    targets: &[AcquisitionTarget],
    provider_id: Uuid,
    state_reason: String,
) -> Result<()> {
    let Some(first) = targets.first() else {
        return Ok(());
    };
    for target in targets {
        update_target_state(
            &state.db_pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Excluded,
                state_reason: Some(state_reason.clone()),
                selected_provider_id: Some(provider_id),
                next_search_after: None,
                increment_search_attempts: target.target_id == first.target_id,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
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
    match block.kind {
        QueueCapacityLimitKind::Global => format!(
            "Queue capacity reached: global active jobs {}/{}.",
            block.global_active, block.global_limit
        ),
        QueueCapacityLimitKind::Subscription => format!(
            "Queue capacity reached: subscription active jobs {}/{}.",
            block.subscription_active, block.subscription_limit
        ),
        QueueCapacityLimitKind::Route => format!(
            "Queue capacity reached for {}: route active jobs {}/{}.",
            block.route_logical_id.as_deref().unwrap_or("route"),
            block.global_active,
            block.global_limit
        ),
        QueueCapacityLimitKind::SubscriptionRoute => format!(
            "Queue capacity reached for {}: subscription route active jobs {}/{}.",
            block.route_logical_id.as_deref().unwrap_or("route"),
            block.subscription_active,
            block.subscription_limit
        ),
        QueueCapacityLimitKind::SearchTick => format!(
            "Queue capacity reached: candidate searches this tick {}/{}.",
            block.tick_active, block.tick_limit
        ),
        QueueCapacityLimitKind::SubmissionTick => format!(
            "Queue capacity reached: submissions this tick {}/{}.",
            block.tick_active, block.tick_limit
        ),
    }
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
        if !status.is_failed() {
            continue;
        }
        if status.source_kind != "magnet" {
            continue;
        }
        let Some(subscription) = subscriptions.get(&target.subscription_id) else {
            continue;
        };
        let Some(candidate_value) = target.selected_candidate.clone() else {
            continue;
        };
        let candidate: AcquisitionCandidate = serde_json::from_value(candidate_value)
            .context("parsing selected acquisition candidate for debrid fallback")?;
        match debrid_failure_fallback_action(subscription, &candidate, &status) {
            DebridFailureFallbackAction::SubmitTorrent { route_logical_id } => {
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
                    provider_warnings: Vec::new(),
                    anime_coverage_plan: None,
                    tv_coverage_plan: None,
                    request_scope_evidence: Some(request_scope_resolution_evidence(
                        subscription,
                        &[target.clone()],
                    )),
                    dispatch: None,
                };
                match submit_selected_candidate(
                    state,
                    subscription,
                    &target,
                    submission,
                    Some(route_logical_id),
                    Some(&mut governor),
                )
                .await
                {
                    Ok(CandidateSubmitOutcome::Submitted) => info!(
                        target_id = %target.target_id,
                        debrid_job_id = %status.job_id,
                        debrid_release_id = ?status.release_id,
                        remote_status = status.remote_status.as_deref().unwrap_or("unknown"),
                        failure_class = status.failure_class.as_deref().unwrap_or("unknown"),
                        fallback_route = route_logical_id,
                        "submitted torrent fallback after debrid failure"
                    ),
                    Ok(CandidateSubmitOutcome::CapacityBlocked(block)) => {
                        let next_after = Utc::now()
                            + jittered_seconds(
                                &target.target_id,
                                QUEUE_CAPACITY_RETRY_SECONDS,
                                120,
                            );
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
                        warn!(
                            target_id = %target.target_id,
                            debrid_job_id = %status.job_id,
                            fallback_route = route_logical_id,
                            error = %err,
                            "torrent fallback after debrid failure could not be submitted"
                        );
                        let next_after = Utc::now()
                            + jittered_seconds(&target.target_id, WARM_RETRY_INTERVAL_SECONDS, 300);
                        update_target_state(
                            &state.db_pool,
                            target.target_id,
                            AcquisitionTargetStateUpdate {
                                state: AcquisitionTargetState::Blocked,
                                state_reason: Some(
                                    "Debrid could not complete this release, and qBittorrent fallback could not be started. Check route/provider status or try another release."
                                        .to_string(),
                                ),
                                next_search_after: Some(next_after),
                                ..Default::default()
                            },
                        )
                        .await?;
                    }
                }
            }
            DebridFailureFallbackAction::RetryNextCandidate { reason } => {
                reset_target_for_candidate_retry(
                    &state.db_pool,
                    target.target_id,
                    reason,
                    Utc::now()
                        + jittered_seconds(
                            &target.target_id,
                            FALLBACK_NEXT_CANDIDATE_RETRY_SECONDS,
                            15,
                        ),
                )
                .await?;
            }
            DebridFailureFallbackAction::NoAutomaticFallback { reason } => {
                update_target_state(
                    &state.db_pool,
                    target.target_id,
                    AcquisitionTargetStateUpdate {
                        state: AcquisitionTargetState::Blocked,
                        state_reason: Some(reason),
                        next_search_after: Some(
                            Utc::now()
                                + jittered_seconds(
                                    &target.target_id,
                                    WARM_RETRY_INTERVAL_SECONDS,
                                    300,
                                ),
                        ),
                        ..Default::default()
                    },
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DebridFailureFallbackAction {
    SubmitTorrent { route_logical_id: &'static str },
    RetryNextCandidate { reason: String },
    NoAutomaticFallback { reason: String },
}

fn debrid_failure_fallback_action(
    subscription: &AcquisitionSubscription,
    candidate: &AcquisitionCandidate,
    status: &crate::debrid::DebridJobStatus,
) -> DebridFailureFallbackAction {
    let summary = debrid_failure_user_summary(status);
    let failure_class = status
        .failure_class
        .as_deref()
        .and_then(DebridFailureClass::from_str);
    let response_policy = failure_class.map(DebridFailureClass::response_policy);
    if matches!(
        response_policy,
        Some(DebridFailureResponsePolicy::AccountActionRequired)
    ) {
        return DebridFailureFallbackAction::NoAutomaticFallback {
            reason: format!(
                "Debrid could not complete this release. {summary} Check the active debrid account before submitting more debrid work."
            ),
        };
    }
    if matches!(
        response_policy,
        Some(DebridFailureResponsePolicy::RetryProviderLater)
    ) {
        return DebridFailureFallbackAction::NoAutomaticFallback {
            reason: format!(
                "Debrid could not complete this release. {summary} Elixir will retry provider-backed acquisition later instead of immediately submitting more work."
            ),
        };
    }
    if subscription.route_policy != AcquisitionRoutePolicy::DebridFirst {
        return DebridFailureFallbackAction::NoAutomaticFallback {
            reason: format!(
                "Debrid could not complete this release. Route policy '{}' does not allow automatic qBittorrent fallback. {summary}",
                subscription.route_policy.as_str()
            ),
        };
    }
    if candidate_supports_route(candidate, TORRENT_DEFAULT_LOGICAL_ID) {
        return DebridFailureFallbackAction::SubmitTorrent {
            route_logical_id: TORRENT_DEFAULT_LOGICAL_ID,
        };
    }
    DebridFailureFallbackAction::RetryNextCandidate {
        reason: format!(
            "Debrid could not complete this release and the selected candidate has no qBittorrent fallback. {summary} Trying the next ranked release."
        ),
    }
}

fn debrid_failure_user_summary(status: &crate::debrid::DebridJobStatus) -> String {
    let message = status
        .last_error
        .as_deref()
        .or(status.selection_error.as_deref())
        .unwrap_or_default()
        .trim();
    let normalized_message = message.to_ascii_lowercase();
    if normalized_message.contains("torbox accepted this torrent") {
        return ensure_sentence(message);
    }
    match status.failure_class.as_deref() {
        Some("no_seeds") => {
            if status
                .remote_status
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("torbox")
            {
                "TorBox accepted this torrent, but it is not cached and has no seeds.".to_string()
            } else {
                "The debrid provider accepted this torrent, but it has no seeds.".to_string()
            }
        }
        Some("provider_stalled") => {
            "The debrid provider accepted this torrent, but the provider transfer is stalled."
                .to_string()
        }
        Some("file_list_unavailable") => {
            "The debrid provider accepted this torrent, but no file list is available yet."
                .to_string()
        }
        Some("magnet_rejected") => "The debrid provider rejected this magnet.".to_string(),
        Some("invalid_source") => {
            "The debrid provider rejected this source as invalid or unsupported.".to_string()
        }
        Some("content_blocked") => {
            "The debrid provider blocked or filtered this source.".to_string()
        }
        Some("not_found_or_expired") => {
            "The debrid provider no longer has this source available.".to_string()
        }
        Some("rate_limited") => {
            "The debrid provider is rate limiting requests. Try again later.".to_string()
        }
        Some("too_many_active_downloads") => {
            "The debrid provider has reached its active download limit.".to_string()
        }
        Some("provider_account_limit_reached") => {
            "The active debrid account has reached a provider account or service limit.".to_string()
        }
        Some("quota_exhausted") => {
            "The active debrid account has exhausted traffic, quota, or fair-use allowance."
                .to_string()
        }
        Some("provider_account_restricted") => {
            "The active debrid account is restricted or not allowed to use this provider feature."
                .to_string()
        }
        Some("provider_unavailable") => {
            "The debrid provider is temporarily unavailable. Try again later.".to_string()
        }
        Some("unauthorized") => {
            "The active debrid account rejected the request. Check the account token.".to_string()
        }
        Some("provider_auth_missing") => {
            "The active debrid account rejected the request. Check the account token.".to_string()
        }
        Some("selection_failed") => {
            "Elixir could not safely select files from the debrid release.".to_string()
        }
        Some(_) if !message.is_empty() && !looks_internal_debrid_message(message) => {
            ensure_sentence(message)
        }
        _ => "The debrid provider reported a failed release.".to_string(),
    }
}

fn ensure_sentence(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.ends_with(['.', '!', '?']) {
        trimmed.to_string()
    } else {
        format!("{trimmed}.")
    }
}

fn looks_internal_debrid_message(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    normalized.contains("parsing ")
        || normalized.contains("serde")
        || normalized.contains("json")
        || normalized.contains("sql")
        || normalized.contains("stack backtrace")
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
    if submission.has_release_coverage_plan()
        && let Some(reuse) = existing_release_reuse(state, &submission).await?
    {
        persist_release_submission(
            state,
            subscription,
            target,
            &submission,
            &route_logical_id,
            Some(reuse.download_id),
            reuse.selected_provider_id.unwrap_or(submission.provider_id),
            "Reused existing pack-aware acquisition release.",
        )
        .await?;
        return Ok(CandidateSubmitOutcome::Submitted);
    }
    let governed_submission = governor.is_some();
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
    match submit_candidate_to_route(state, subscription, target, &submission, &route_logical_id)
        .await
    {
        Ok(response) => {
            let reason = if route_override == Some(TORRENT_DEFAULT_LOGICAL_ID) {
                "Submitted through torrent fallback."
            } else {
                "Submitted through acquisition route."
            };
            if submission.has_release_coverage_plan() {
                persist_release_submission(
                    state,
                    subscription,
                    target,
                    &submission,
                    &route_logical_id,
                    response.download_id,
                    response.provider_id,
                    reason,
                )
                .await?;
            } else {
                mark_target_submitted(
                    state,
                    target,
                    &submission,
                    &route_logical_id,
                    response.download_id,
                    response.provider_id,
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
            let torrent_response = match submit_candidate_to_route(
                state,
                subscription,
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
                                "Debrid route failed: {err}; torrent fallback failed: {fallback_err}"
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
            if submission.has_release_coverage_plan() {
                persist_release_submission(
                    state,
                    subscription,
                    target,
                    &submission,
                    TORRENT_DEFAULT_LOGICAL_ID,
                    torrent_response.download_id,
                    torrent_response.provider_id,
                    "Debrid rejected the candidate; submitted torrent fallback.",
                )
                .await?;
            } else {
                mark_target_submitted(
                    state,
                    target,
                    &submission,
                    TORRENT_DEFAULT_LOGICAL_ID,
                    torrent_response.download_id,
                    torrent_response.provider_id,
                    "Debrid rejected the candidate; submitted torrent fallback.",
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
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
) -> Result<DownloadBrokerSubmitResponse> {
    let store = ExtensionStore::new(&state.db_pool);
    let request = DownloadBrokerSubmitRequest {
        source: submission.candidate.source.clone(),
        category: None,
        paused: Some(false),
        name: Some(download_display_name(target, &submission.candidate)),
        priority: None,
        add_to_top: None,
        subscription_id: Some(subscription.subscription_id),
        source_provider_id: Some(submission.provider_id),
        source_extension_id: Some(submission.source_extension_id.clone()),
        media_type: Some(subscription.media_type),
        media_title: Some(subscription.title.clone()),
        selected_candidate: Some(submission.candidate.clone()),
        release_fingerprint: None,
    };
    submit_to_broker(
        state,
        &store,
        route_logical_id,
        Some(&submission.source_extension_id),
        request,
    )
    .await
    .map_err(api_error_to_anyhow)
}

async fn mark_target_submitted(
    state: &AppState,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
    download_id: Option<String>,
    route_provider_id: Uuid,
    reason: &str,
) -> Result<()> {
    let selected_candidate = selected_candidate_provenance_with_submission(
        submission,
        route_logical_id,
        &download_id,
        route_provider_id,
        reason,
    )?;
    update_target_state(
        &state.db_pool,
        target.target_id,
        AcquisitionTargetStateUpdate {
            state: AcquisitionTargetState::Submitted,
            state_reason: Some(format!("{reason} {route_logical_id}")),
            selected_provider_id: Some(submission.provider_id),
            selected_route_logical_id: Some(route_logical_id.to_string()),
            selected_candidate: Some(selected_candidate),
            download_id,
            next_search_after: None,
            increment_search_attempts: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn existing_release_reuse(
    state: &AppState,
    submission: &CandidateSubmission,
) -> Result<Option<ExistingReleaseReuse>> {
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
    let Some(download_id) = release.download_id else {
        return Ok(None);
    };
    Ok(Some(ExistingReleaseReuse {
        download_id,
        selected_provider_id: release.selected_provider_id,
    }))
}

async fn persist_release_submission(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
    download_id: Option<String>,
    route_provider_id: Uuid,
    reason: &str,
) -> Result<()> {
    if submission.anime_coverage_plan.is_some() {
        persist_anime_release_submission(
            state,
            subscription,
            target,
            submission,
            route_logical_id,
            download_id,
            route_provider_id,
            reason,
        )
        .await
    } else if submission.tv_coverage_plan.is_some() {
        persist_tv_release_submission(
            state,
            subscription,
            target,
            submission,
            route_logical_id,
            download_id,
            route_provider_id,
            reason,
        )
        .await
    } else {
        mark_target_submitted(
            state,
            target,
            submission,
            route_logical_id,
            download_id,
            route_provider_id,
            reason,
        )
        .await
    }
}

async fn persist_anime_release_submission(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
    download_id: Option<String>,
    route_provider_id: Uuid,
    reason: &str,
) -> Result<()> {
    let Some(plan) = submission.anime_coverage_plan.as_ref() else {
        return mark_target_submitted(
            state,
            target,
            submission,
            route_logical_id,
            download_id,
            route_provider_id,
            reason,
        )
        .await;
    };
    let fingerprint =
        candidate_release_fingerprint(&submission.candidate, Some(submission.provider_id));
    let selected_candidate = selected_candidate_provenance_with_submission(
        submission,
        route_logical_id,
        &download_id,
        route_provider_id,
        reason,
    )?;
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
            selected_provider_id: Some(route_provider_id),
            download_id: download_id.clone(),
            remote_release_id: None,
            state: AcquisitionReleaseState::Submitted,
            state_reason: Some(format!(
                "{reason} pack-aware coverage entries: {}",
                plan.entries.len()
            )),
            selected_candidate: Some(selected_candidate.clone()),
            coverage_plan: Some(coverage_plan_with_request_scope(
                serde_json::to_value(plan)?,
                submission.request_scope_evidence.as_ref(),
            )),
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
                provider_file_id: file.file_id.clone(),
                path: file.path.clone(),
                basename: None,
                size_bytes: file.size_bytes,
                selectable: file.selectable,
                selected: None,
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
                provider_metadata: Some(json!({
                    "fileKey": file.file_key.clone(),
                    "fileId": file.file_id.clone(),
                    "selectable": file.selectable,
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
            route_provider_id,
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
            provider_id: Some(route_provider_id),
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

async fn persist_tv_release_submission(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    submission: &CandidateSubmission,
    route_logical_id: &str,
    download_id: Option<String>,
    route_provider_id: Uuid,
    reason: &str,
) -> Result<()> {
    let Some(plan) = submission.tv_coverage_plan.as_ref() else {
        return mark_target_submitted(
            state,
            target,
            submission,
            route_logical_id,
            download_id,
            route_provider_id,
            reason,
        )
        .await;
    };
    let fingerprint =
        candidate_release_fingerprint(&submission.candidate, Some(submission.provider_id));
    let selected_candidate = selected_candidate_provenance_with_submission(
        submission,
        route_logical_id,
        &download_id,
        route_provider_id,
        reason,
    )?;
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
            fingerprint,
            release_kind: plan.release_kind,
            resolver_kind: plan.resolver_kind,
            resolver_version: plan.resolver_version.clone(),
            confidence: plan.confidence,
            score: submission.candidate.score,
            selected_route_logical_id: Some(route_logical_id.to_string()),
            selected_provider_id: Some(route_provider_id),
            download_id: download_id.clone(),
            remote_release_id: None,
            state: AcquisitionReleaseState::Submitted,
            state_reason: Some(format!(
                "{reason} TV coverage entries: {}",
                plan.entries.len()
            )),
            selected_candidate: Some(selected_candidate.clone()),
            coverage_plan: Some(coverage_plan_with_request_scope(
                serde_json::to_value(plan)?,
                submission.request_scope_evidence.as_ref(),
            )),
        },
    )
    .await?;

    let mut file_ids_by_key = HashMap::new();
    for file in tv_release_file_inputs(&submission.candidate) {
        let parsed = TvSonarrStyleResolver.parse_file(&file.path);
        let release_file = upsert_release_file(
            &state.db_pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: None,
                file_id: Some(file.file_id.clone()),
                provider_file_id: Some(file.file_id.clone()),
                path: file.path.clone(),
                basename: None,
                size_bytes: file.size_bytes,
                selectable: file.selectable,
                selected: plan
                    .entries
                    .iter()
                    .any(|entry| entry.release_file_id.as_deref() == Some(file.file_id.as_str()))
                    .then_some(true),
                parsed_title: parsed.normalized_series_title.clone(),
                parsed_season_number: parsed.season_number,
                parsed_episode_number: parsed.episode_numbers.first().copied(),
                parsed_episode_end_number: parsed.episode_numbers.last().copied(),
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: parsed.air_date.clone(),
                parsed_quality: parsed.quality.resolution.map(|value| format!("{value:?}")),
                parsed_language: parsed.modifiers.languages.first().cloned(),
                parsed_release_group: parsed.release_group.clone(),
                parser_confidence: ReleaseConfidence::High,
                parser_reason: None,
                raw: Some(serde_json::to_value(&parsed)?),
                provider_metadata: Some(json!({
                    "fileId": file.file_id,
                    "selectable": file.selectable,
                })),
            },
        )
        .await?;
        file_ids_by_key.insert(file.file_id, release_file.release_file_id);
    }

    let targets = list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
    let targets_by_id = targets
        .into_iter()
        .map(|target| (target.target_id, target))
        .collect::<HashMap<_, _>>();
    let mut submitted_target_ids = BTreeSet::new();
    for entry in &plan.entries {
        let Some(covered_target) = targets_by_id.get(&entry.target_id) else {
            continue;
        };
        let release_file_id = entry
            .release_file_id
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
                confidence: plan.confidence,
                score: submission.candidate.score,
                reason: Some(format!("TV Sonarr-style {}", entry.target_key)),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("rr2_tv_sonarr_style".to_string()),
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
            route_provider_id,
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
            provider_id: Some(route_provider_id),
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
    debug!(
        release_id = %release.release_id,
        coverage = plan.entries.len(),
        "persisted RR-6B TV release coverage"
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
    if subscription.request_mode == AcquisitionRequestMode::OneShot {
        return expand_one_shot_subscription_targets(state, subscription, now).await;
    }

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

async fn expand_one_shot_subscription_targets(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    now: DateTime<Utc>,
) -> Result<SubscriptionExpansion> {
    let existing_targets =
        list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
    if existing_targets
        .iter()
        .any(|target| !is_metadata_snapshot_blocker_key(&target.target_key))
    {
        return Ok(SubscriptionExpansion::default());
    }

    validate_one_shot_snapshot_scope(subscription)?;

    let original_ids = subscription.external_ids.clone().unwrap_or_default();
    let mut ids = original_ids.clone();
    let (mut targets, anime_graph_snapshot) = match subscription.media_type {
        MediaType::Movie => (expand_movie_subscription(subscription, &ids, now), None),
        MediaType::Series => (
            expand_series_subscription(state, subscription, &mut ids, now).await?,
            None,
        ),
        MediaType::Anime => {
            expand_anime_subscription_with_options(
                state,
                subscription,
                &mut ids,
                now,
                AnimeExpansionOptions { strict: true },
            )
            .await?
        }
    };

    targets.retain(|target| one_shot_scope_allows_target(subscription, target));
    annotate_one_shot_snapshot_targets(subscription, &mut targets);
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
    let target_count = targets.len();
    if target_count == 0 {
        bail!(
            "one-shot metadata snapshot produced no acquisition targets for {} scope '{}'; check metadata provider configuration and external IDs",
            media_type_name(subscription.media_type),
            subscription.request_scope.as_str()
        );
    }

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

async fn persist_one_shot_metadata_blocker(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    message: &str,
) -> Result<()> {
    let reason = format!("One-shot metadata snapshot failed: {message}");
    let targets = upsert_subscription_targets(
        &state.db_pool,
        subscription.subscription_id,
        vec![NewAcquisitionTarget {
            target_key: Some(ONE_SHOT_METADATA_BLOCKER_TARGET_KEY.to_string()),
            media_type: Some(subscription.media_type),
            title: Some(format!("{} metadata snapshot", subscription.title)),
            season_number: None,
            episode_number: None,
            absolute_episode_number: None,
            air_date: None,
            air_time: None,
            metadata: Some(json!({
                "source": "one_shot_metadata_snapshot",
                "blocker": "metadata_snapshot_failed",
                "requestMode": subscription.request_mode.as_str(),
                "requestScope": subscription.request_scope.as_str(),
                "metadataPolicy": subscription.metadata_policy.as_str(),
                "scope": subscription.scope.clone(),
                "error": message,
            })),
            state: Some(AcquisitionTargetState::Blocked),
            next_search_after: None,
        }],
    )
    .await?;

    if let Some(target) = targets
        .iter()
        .find(|target| is_metadata_snapshot_blocker_key(&target.target_key))
    {
        update_target_state(
            &state.db_pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Blocked,
                state_reason: Some(reason),
                next_search_after: None,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

async fn clear_one_shot_metadata_blocker(
    state: &AppState,
    subscription: &AcquisitionSubscription,
) -> Result<()> {
    let targets = list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
    for target in targets {
        if is_metadata_snapshot_blocker_key(&target.target_key)
            && target.state == AcquisitionTargetState::Blocked
        {
            update_target_state(
                &state.db_pool,
                target.target_id,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Excluded,
                    state_reason: Some(
                        "Metadata snapshot recovered and created scoped targets".to_string(),
                    ),
                    next_search_after: None,
                    ..Default::default()
                },
            )
            .await?;
        }
    }
    Ok(())
}

fn validate_one_shot_snapshot_scope(subscription: &AcquisitionSubscription) -> Result<()> {
    let scope = subscription.scope.as_ref();
    match subscription.request_scope {
        AcquisitionRequestScope::Subscription | AcquisitionRequestScope::Movie => Ok(()),
        AcquisitionRequestScope::Episode => {
            if !scope_target_keys(scope).is_empty()
                || (scope_i32(scope, &["seasonNumber", "season_number"]).is_some()
                    && scope_i32(scope, &["episodeNumber", "episode_number"]).is_some())
                || scope_i32(scope, &["absoluteEpisodeNumber", "absolute_episode_number"]).is_some()
            {
                Ok(())
            } else {
                bail!(
                    "one-shot episode request requires targetKey, targetKeys, seasonNumber plus episodeNumber, or absoluteEpisodeNumber"
                )
            }
        }
        AcquisitionRequestScope::Season => {
            if scope_i32(scope, &["seasonNumber", "season_number"]).is_some() {
                Ok(())
            } else {
                bail!("one-shot season request requires seasonNumber in scope")
            }
        }
        AcquisitionRequestScope::Range => {
            if !scope_target_keys(scope).is_empty()
                || (scope_i32(scope, &["seasonNumber", "season_number"]).is_some()
                    && (scope_i32(scope, &["episodeStart", "episode_start"]).is_some()
                        || scope_i32(scope, &["episodeEnd", "episode_end"]).is_some()))
                || scope_i32(scope, &["absoluteEpisodeStart", "absolute_episode_start"]).is_some()
                || scope_i32(scope, &["absoluteEpisodeEnd", "absolute_episode_end"]).is_some()
            {
                Ok(())
            } else {
                bail!(
                    "one-shot range request requires targetKeys, seasonNumber plus episodeStart/episodeEnd, or absoluteEpisodeStart/absoluteEpisodeEnd"
                )
            }
        }
        AcquisitionRequestScope::Missing => Ok(()),
        AcquisitionRequestScope::SelectedTargets => {
            if !scope_target_keys(scope).is_empty() {
                Ok(())
            } else {
                bail!("one-shot selected-target request requires targetKey or targetKeys in scope")
            }
        }
    }
}

fn one_shot_scope_allows_target(
    subscription: &AcquisitionSubscription,
    target: &NewAcquisitionTarget,
) -> bool {
    let scope = subscription.scope.as_ref();
    let target_keys = scope_target_keys(scope);
    if !target_keys.is_empty() {
        let target_key = generated_one_shot_target_key(subscription.media_type, target);
        return target_keys.contains(&target_key);
    }

    match subscription.request_scope {
        AcquisitionRequestScope::Subscription | AcquisitionRequestScope::Movie => true,
        AcquisitionRequestScope::Missing => true,
        AcquisitionRequestScope::SelectedTargets => false,
        AcquisitionRequestScope::Season => scope_i32(scope, &["seasonNumber", "season_number"])
            .map(|season| target.season_number == Some(season))
            .unwrap_or(false),
        AcquisitionRequestScope::Episode => {
            if let Some(absolute) =
                scope_i32(scope, &["absoluteEpisodeNumber", "absolute_episode_number"])
            {
                return target.absolute_episode_number == Some(absolute);
            }
            let season = scope_i32(scope, &["seasonNumber", "season_number"]);
            let episode = scope_i32(scope, &["episodeNumber", "episode_number"]);
            season.is_some()
                && episode.is_some()
                && target.season_number == season
                && target.episode_number == episode
        }
        AcquisitionRequestScope::Range => {
            if let Some(absolute) = target.absolute_episode_number
                && let Some((start, end)) = scope_range(
                    scope,
                    &["absoluteEpisodeStart", "absolute_episode_start"],
                    &["absoluteEpisodeEnd", "absolute_episode_end"],
                )
            {
                return number_in_range(absolute, start, end);
            }
            let Some(season) = scope_i32(scope, &["seasonNumber", "season_number"]) else {
                return false;
            };
            let Some(episode) = target.episode_number else {
                return false;
            };
            if target.season_number != Some(season) {
                return false;
            }
            scope_range(
                scope,
                &["episodeStart", "episode_start"],
                &["episodeEnd", "episode_end"],
            )
            .map(|(start, end)| number_in_range(episode, start, end))
            .unwrap_or(false)
        }
    }
}

fn annotate_one_shot_snapshot_targets(
    subscription: &AcquisitionSubscription,
    targets: &mut [NewAcquisitionTarget],
) {
    for target in targets {
        let mut metadata = match target.metadata.take() {
            Some(JsonValue::Object(object)) => object,
            Some(value) => {
                let mut object = serde_json::Map::new();
                object.insert("sourceMetadata".to_string(), value);
                object
            }
            None => serde_json::Map::new(),
        };
        metadata.insert(
            "acquisitionRequest".to_string(),
            json!({
                "mode": subscription.request_mode.as_str(),
                "scope": subscription.request_scope.as_str(),
                "metadataPolicy": subscription.metadata_policy.as_str(),
                "completionPolicy": subscription.completion_policy.as_str(),
                "requestedScope": subscription.scope.clone(),
            }),
        );
        target.metadata = Some(JsonValue::Object(metadata));
    }
}

fn scope_target_keys(scope: Option<&JsonValue>) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let Some(scope) = scope else {
        return keys;
    };
    if let Some(value) = scope.get("targetKey").and_then(JsonValue::as_str) {
        if let Some(key) = normalize_scope_target_key(value) {
            keys.insert(key);
        }
    }
    for key in ["targetKeys", "targets"] {
        if let Some(values) = scope.get(key).and_then(JsonValue::as_array) {
            for value in values {
                if let Some(value) = value.as_str() {
                    if let Some(key) = normalize_scope_target_key(value) {
                        keys.insert(key);
                    }
                } else if let Some(value) = value.get("targetKey").and_then(JsonValue::as_str)
                    && let Some(key) = normalize_scope_target_key(value)
                {
                    keys.insert(key);
                }
            }
        }
    }
    keys
}

fn scope_i32(scope: Option<&JsonValue>, keys: &[&str]) -> Option<i32> {
    let scope = scope?;
    keys.iter().find_map(|key| {
        let value = scope.get(*key)?;
        value
            .as_i64()
            .map(|value| value as i32)
            .or_else(|| value.as_str()?.trim().parse::<i32>().ok())
    })
}

fn scope_range(
    scope: Option<&JsonValue>,
    start_keys: &[&str],
    end_keys: &[&str],
) -> Option<(i32, i32)> {
    let start = scope_i32(scope, start_keys);
    let end = scope_i32(scope, end_keys);
    match (start, end) {
        (Some(start), Some(end)) => Some((start, end)),
        (Some(start), None) => Some((start, start)),
        (None, Some(end)) => Some((end, end)),
        (None, None) => None,
    }
}

fn number_in_range(value: i32, start: i32, end: i32) -> bool {
    value >= start.min(end) && value <= start.max(end)
}

fn generated_one_shot_target_key(media_type: MediaType, target: &NewAcquisitionTarget) -> String {
    target
        .target_key
        .as_deref()
        .and_then(normalize_scope_target_key)
        .unwrap_or_else(|| {
            if let (Some(season), Some(episode)) = (target.season_number, target.episode_number) {
                format!("S{season:02}E{episode:02}")
            } else if let Some(absolute) = target.absolute_episode_number {
                format!("A{absolute:04}")
            } else if let Some(air_date) = target.air_date.as_deref() {
                format!("DATE:{}", air_date.trim().to_ascii_uppercase())
            } else if media_type == MediaType::Movie {
                "MOVIE".to_string()
            } else {
                "SERIES".to_string()
            }
        })
}

fn normalize_scope_target_key(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_ascii_uppercase())
}

fn is_metadata_snapshot_blocker_key(value: &str) -> bool {
    value.eq_ignore_ascii_case(ONE_SHOT_METADATA_BLOCKER_TARGET_KEY)
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
    if state
        .settings
        .classifier
        .tvdb_api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        bail!("TVDB API key is required to expand TV series acquisition targets");
    }
    ids.tvdb_series = Some(tvdb_series_id.clone());
    if ids.tvdb.is_none() {
        ids.tvdb = Some(tvdb_series_id.clone());
    }

    let seasons = state
        .linkers
        .fetch_tvdb_series_seasons(&tvdb_series_id)
        .await
        .with_context(|| {
            format!("fetching TVDB seasons for acquisition series {tvdb_series_id}")
        })?;
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
            .with_context(|| {
                format!(
                    "fetching TVDB season {season_number} episodes for acquisition series {tvdb_series_id}"
                )
            })?;
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
    expand_anime_subscription_with_options(
        state,
        subscription,
        ids,
        now,
        AnimeExpansionOptions { strict: false },
    )
    .await
}

#[derive(Debug, Clone, Copy)]
struct AnimeExpansionOptions {
    strict: bool,
}

async fn expand_anime_subscription_with_options(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    ids: &mut ExternalIds,
    now: DateTime<Utc>,
    options: AnimeExpansionOptions,
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

    let seed_mapping = fetch_anizip_mapping_for_expansion(state, &seed_anilist_id, options).await?;
    let seed_season = seed_mapping
        .as_ref()
        .and_then(infer_anizip_season_number)
        .unwrap_or(1);
    let mut season_chain =
        resolve_anilist_season_chain_for_expansion(&seed_anilist_id, seed_season, state, options)
            .await?;
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
            fetch_anizip_mapping_for_expansion(state, &season.anilist_id, options).await?
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

async fn fetch_anizip_mapping_for_expansion(
    state: &AppState,
    anilist_id: &str,
    options: AnimeExpansionOptions,
) -> Result<Option<crate::library::AniZipMapping>> {
    let result = state.linkers.fetch_anizip_mapping(anilist_id).await;
    if options.strict {
        result.with_context(|| format!("fetching ani.zip mapping for AniList id {anilist_id}"))
    } else {
        Ok(result.unwrap_or(None))
    }
}

async fn resolve_anilist_season_chain_for_expansion(
    seed_anilist_id: &str,
    seed_season: i32,
    state: &AppState,
    options: AnimeExpansionOptions,
) -> Result<Vec<AniListSeasonChainEntry>> {
    let result = resolve_anilist_season_chain(
        Some(&state.settings.classifier),
        seed_season,
        seed_anilist_id,
        1.0,
    )
    .await;
    if options.strict {
        result.with_context(|| {
            format!("resolving AniList season chain for AniList id {seed_anilist_id}")
        })
    } else {
        Ok(result.unwrap_or_default())
    }
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
            target_key: Some(target.target_key.clone()),
            title: Some(target.title.clone()),
            season_number: target.season_number,
            episode_number: target.episode_number,
            absolute_episode_number: target.absolute_episode_number,
            air_date: target.air_date.clone(),
            metadata: target.metadata.clone(),
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

#[cfg(test)]
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
                tv_coverage_plan: None,
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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
        tv_coverage_plan: None,
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

#[cfg(test)]
fn compare_candidates(
    left: &AcquisitionCandidate,
    right: &AcquisitionCandidate,
    route_policy: AcquisitionRoutePolicy,
) -> Ordering {
    candidate_score_tuple(left, route_policy).cmp(&candidate_score_tuple(right, route_policy))
}

#[cfg(test)]
fn candidate_score_tuple(
    candidate: &AcquisitionCandidate,
    route_policy: AcquisitionRoutePolicy,
) -> (i32, i32, i32, i32, i32, i64, i32, i32) {
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
    let freshness = candidate_freshness_score(candidate);
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
        freshness,
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

fn route_option_available(route_options: &[CandidateRouteOption], route: &str) -> bool {
    route_options
        .iter()
        .find(|option| option.logical_id == route)
        .map(|option| option.available && option.blocker.is_none())
        .unwrap_or(true)
}

fn candidate_allowed_by_subscription_preferences(
    candidate: &AcquisitionCandidate,
    subscription: &AcquisitionSubscription,
) -> bool {
    let profile = subscription.quality_profile.as_ref();
    if let Some(max_size) = json_u64(profile, &["maxSizeBytes", "max_size_bytes"]) {
        if candidate
            .size_bytes
            .is_some_and(|size_bytes| size_bytes > max_size)
        {
            return false;
        }
    }
    let allowed_qualities = json_string_array(profile, &["allowedQualities", "qualities"]);
    if !allowed_qualities.is_empty()
        && !quality_matches_any(candidate.quality.as_deref(), &allowed_qualities)
    {
        return false;
    }
    let required_languages = json_string_array(profile, &["requiredLanguages", "languages"]);
    if !required_languages.is_empty()
        && !language_matches_any(candidate.language.as_deref(), &required_languages)
    {
        return false;
    }
    true
}

fn quality_matches_any(value: Option<&str>, allowed: &[String]) -> bool {
    let Some(value) = value else {
        return false;
    };
    let lower = value.to_ascii_lowercase();
    allowed
        .iter()
        .map(|item| item.to_ascii_lowercase())
        .any(|item| lower.contains(&item))
}

fn language_matches_any(value: Option<&str>, required: &[String]) -> bool {
    let Some(value) = value else {
        return false;
    };
    let lower = value.to_ascii_lowercase();
    required
        .iter()
        .map(|item| item.to_ascii_lowercase())
        .any(|item| lower.contains(&item))
}

fn tv_targets_for_acquisition_targets(targets: &[AcquisitionTarget]) -> Vec<TvTarget> {
    targets
        .iter()
        .filter_map(|target| {
            if target.media_type != MediaType::Series {
                return None;
            }
            Some(TvTarget {
                target_id: target.target_id,
                target_key: target.target_key.clone(),
                season_number: target.season_number.unwrap_or_default(),
                episode_number: target.episode_number.unwrap_or_default(),
                air_date: target.air_date.clone(),
            })
        })
        .collect()
}

fn tv_release_file_inputs(candidate: &AcquisitionCandidate) -> Vec<TvReleaseFileInput> {
    candidate
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let fallback_id = file
                .file_index
                .or_else(|| i64::try_from(index).ok().map(|value| value + 1))
                .map(|value| value.to_string())
                .unwrap_or_else(|| file.path.clone());
            TvReleaseFileInput {
                file_id: file.file_id.clone().unwrap_or(fallback_id),
                path: file.path.clone(),
                size_bytes: file.size_bytes.and_then(|value| i64::try_from(value).ok()),
                selectable: file.selectable.unwrap_or(true),
            }
        })
        .collect()
}

fn candidate_file_selection_supported(candidate: &AcquisitionCandidate) -> bool {
    !candidate.files.is_empty()
        && candidate
            .files
            .iter()
            .any(|file| file.selectable.unwrap_or(true))
}

fn candidate_media_file_count(candidate: &AcquisitionCandidate) -> usize {
    candidate
        .files
        .iter()
        .filter(|file| looks_like_media_file(&file.path))
        .count()
}

fn looks_like_media_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    matches!(
        lower.rsplit('.').next(),
        Some("mkv" | "mp4" | "m4v" | "avi" | "mov" | "wmv" | "ts" | "m2ts" | "webm")
    )
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
    selected_candidate_provenance_inner(submission, None)
}

fn selected_candidate_provenance_with_submission(
    submission: &CandidateSubmission,
    route_logical_id: &str,
    download_id: &Option<String>,
    route_provider_id: Uuid,
    reason: &str,
) -> Result<JsonValue> {
    selected_candidate_provenance_inner(
        submission,
        Some(json!({
            "routeLogicalId": route_logical_id,
            "routeProviderId": route_provider_id,
            "downloadId": download_id,
            "reason": reason,
            "recordedAt": Utc::now().to_rfc3339(),
        })),
    )
}

fn selected_candidate_provenance_inner(
    submission: &CandidateSubmission,
    submission_result: Option<JsonValue>,
) -> Result<JsonValue> {
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
        if let Some(plan) = submission.tv_coverage_plan.as_ref() {
            object.insert("tvCoveragePlan".to_string(), serde_json::to_value(plan)?);
        }
        if let Some(evidence) = submission.request_scope_evidence.as_ref() {
            object.insert("requestScopeEvidence".to_string(), evidence.clone());
        }
        if let Some(dispatch) = submission.dispatch.as_ref() {
            object.insert(
                "schedulerDispatch".to_string(),
                serde_json::to_value(dispatch)?,
            );
        }
        if !submission.provider_warnings.is_empty() {
            object.insert(
                "providerWarnings".to_string(),
                json!(submission.provider_warnings),
            );
        }
        if let Some(submission_result) = submission_result {
            object.insert("submissionResult".to_string(), submission_result);
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
    use crate::acquisition::{
        audit::{
            EVENT_ACQUISITION_SEARCH_SCHEDULED, count_acquisition_audit_events_for_subscription,
        },
        release_resolution::store::{ReleaseListFilter, list_releases},
        subscriptions::{
            AcquisitionCompletionPolicy, AcquisitionMetadataPolicy, AcquisitionRequestMode,
            AcquisitionRequestScope, NewAcquisitionSubscription, NewAcquisitionTarget,
            create_subscription, upsert_subscription_targets,
        },
    };
    use crate::http::handlers::acquisition_sources::{
        AcquisitionCandidateFile, CandidateProviderSummary, normalize_acquisition_candidate,
    };
    use crate::{
        artwork::ArtworkService,
        auth::AuthService,
        config::Settings,
        extensions::{
            ExtensionManager,
            store::{ExtensionStore, NewExtension, NewExtensionInstance, NewProvider},
        },
        metadata::MetadataService,
        orchestrator::planner::stable_provider_id,
        secrets::SecretsManager,
    };
    use crate::{
        config::DatabaseConfig,
        db::{
            Database,
            models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality},
        },
        library::LinkerService,
    };
    use axum::{
        Json, Router,
        extract::{Path as AxumPath, Query, State},
        routing::{get, post},
    };
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    use tokio::{net::TcpListener, task::JoinHandle};

    async fn setup_test_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn setup_test_state() -> Result<AppState> {
        let mut settings = Settings::default();
        settings.database = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        setup_test_state_with_settings(settings).await
    }

    async fn setup_test_state_with_settings(settings: Settings) -> Result<AppState> {
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let auth_service = AuthService::new(settings.auth.clone())?;
        let metadata = MetadataService::new(settings.metadata.clone())?;
        let linkers = LinkerService::new(settings.classifier.clone())?;
        let artwork = ArtworkService::new(
            settings.library.artwork_cache_dir.clone(),
            settings.metadata.request_timeout_seconds,
        )?;
        let secrets = SecretsManager::from_key_bytes([0u8; 32], true);
        Ok(AppState::new(
            settings,
            database,
            auth_service,
            ExtensionManager::new(),
            metadata,
            linkers,
            artwork,
            secrets,
        ))
    }

    async fn setup_test_state_with_tvdb_fixture() -> Result<(AppState, JoinHandle<()>)> {
        let (base_url, handle) = start_osr1_tvdb_fixture().await?;
        let mut settings = Settings::default();
        settings.database = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        settings.classifier.tvdb_base_url = base_url;
        settings.classifier.tvdb_api_key = Some("test-tvdb-key".to_string());
        setup_test_state_with_settings(settings)
            .await
            .map(|state| (state, handle))
    }

    async fn start_osr1_tvdb_fixture() -> Result<(String, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        let app = Router::new()
            .route(
                "/login",
                post(|| async { Json(json!({ "data": { "token": "test-token" } })) }),
            )
            .route(
                "/series/:series_id/extended",
                get(|AxumPath(series_id): AxumPath<String>| async move {
                    Json(json!({
                        "data": {
                            "id": series_id.parse::<i64>().unwrap_or_default(),
                            "seasons": [
                                { "id": 10, "number": 0 },
                                { "id": 11, "number": 1 },
                                { "id": 12, "number": 2 }
                            ]
                        }
                    }))
                }),
            )
            .route(
                "/series/:series_id/episodes/default",
                get(
                    |AxumPath(series_id): AxumPath<String>,
                     Query(query): Query<std::collections::HashMap<String, String>>| async move {
                        let season = query
                            .get("season")
                            .and_then(|value| value.parse::<i32>().ok())
                            .unwrap_or(1);
                        let episodes = match season {
                            1 => vec![
                                json!({
                                    "id": 101,
                                    "seasonNumber": 1,
                                    "number": 1,
                                    "absoluteNumber": 1,
                                    "name": format!("Fixture {series_id} S01E01"),
                                    "aired": "2026-01-01"
                                }),
                                json!({
                                    "id": 102,
                                    "seasonNumber": 1,
                                    "number": 2,
                                    "absoluteNumber": 2,
                                    "name": format!("Fixture {series_id} S01E02"),
                                    "aired": "2026-01-08"
                                }),
                            ],
                            2 => vec![
                                json!({
                                    "id": 201,
                                    "seasonNumber": 2,
                                    "number": 1,
                                    "absoluteNumber": 3,
                                    "name": format!("Fixture {series_id} S02E01"),
                                    "aired": "2026-02-01"
                                }),
                                json!({
                                    "id": 202,
                                    "seasonNumber": 2,
                                    "number": 2,
                                    "absoluteNumber": 4,
                                    "name": format!("Fixture {series_id} S02E02"),
                                    "aired": "2026-02-08"
                                }),
                                json!({
                                    "id": 203,
                                    "seasonNumber": 2,
                                    "number": 3,
                                    "absoluteNumber": 5,
                                    "name": format!("Fixture {series_id} S02E03"),
                                    "aired": "2026-02-15"
                                }),
                            ],
                            _ => Vec::new(),
                        };
                        Json(json!({ "data": { "episodes": episodes } }))
                    },
                ),
            );
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("tvdb fixture");
        });
        Ok((base_url, handle))
    }

    #[derive(Clone)]
    struct MockCandidateProviderState {
        requests: Arc<Mutex<Vec<Value>>>,
    }

    async fn start_te10b_candidate_provider_server()
    -> Result<(u16, Arc<Mutex<Vec<Value>>>, JoinHandle<()>)> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = MockCandidateProviderState {
            requests: requests.clone(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let app = Router::new()
            .route("/candidate-provider/search", post(te10b_candidate_provider))
            .with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server");
        });
        Ok((port, requests, handle))
    }

    async fn te10b_candidate_provider(
        State(state): State<MockCandidateProviderState>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        state
            .requests
            .lock()
            .expect("requests lock")
            .push(payload.clone());
        let media_type = payload
            .pointer("/request/mediaType")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let title = payload
            .pointer("/request/title")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let intent_kind = payload
            .pointer("/request/searchIntent/kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let candidates = match (media_type, title, intent_kind) {
            ("series", "Empty Source Show", _) => Vec::new(),
            ("series", "Ambiguous Review Show", _) => vec![candidate_json(
                "Ambiguous.Review.Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
                "te10btvreview",
                &[],
            )],
            ("anime", "Ambiguous Anime", _) => vec![candidate_json(
                "[SubsPlease] Different Anime - 01 [1080p]",
                "te10banimereview",
                &[],
            )],
            ("movie", _, _) => vec![candidate_json(
                "Movie.2026.1080p.WEB-DL-GROUP",
                "te10bmovie",
                &[],
            )],
            ("series", _, "season_pack") => vec![candidate_json(
                "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
                "te10bseasonpack",
                &[
                    ("1", "Show.S01.COMPLETE/Show.S01E01.1080p.mkv"),
                    ("2", "Show.S01.COMPLETE/Show.S01E02.1080p.mkv"),
                    ("3", "Show.S01.COMPLETE/Show.S01E03.1080p.mkv"),
                ],
            )],
            ("series", _, "series_pack") => vec![candidate_json(
                "Show Complete Series 1080p BluRay x265-GRP",
                "te10bseriespack",
                &[
                    ("1", "Show Complete Series/Show.S01E01.1080p.mkv"),
                    ("2", "Show Complete Series/Show.S02E01.1080p.mkv"),
                ],
            )],
            ("series", _, _) => vec![candidate_json(
                "Show.S02E01.1080p.WEB-DL-GROUP",
                "te10btvepisode",
                &[],
            )],
            ("anime", "Translated Anime", _) => vec![candidate_json(
                "[SubsPlease] Translated Anime S04E01 [1080p]",
                "te10banimetvdb",
                &[],
            )],
            ("anime", _, _) => vec![candidate_json(
                "[SubsPlease] Example Title - 1000 [1080p]",
                "te10banimeabsolute",
                &[],
            )],
            _ => Vec::new(),
        };
        let mut rows = candidates;
        rows.push(json!({
            "title": "Malformed Candidate",
            "sourceKind": "magnet"
        }));
        Json(json!({
            "candidates": rows,
            "warnings": ["provider fixture warning"]
        }))
    }

    fn candidate_json(title: &str, hash: &str, files: &[(&str, &str)]) -> Value {
        json!({
            "title": title,
            "source": format!("magnet:?xt=urn:btih:{hash}"),
            "sourceKind": "magnet",
            "infoHash": hash,
            "quality": "1080p",
            "seeders": 100,
            "cachedDebrid": true,
            "supportedRoutes": [
                DEBRID_DEFAULT_LOGICAL_ID,
                TORRENT_DEFAULT_LOGICAL_ID
            ],
            "defaultRoute": DEBRID_DEFAULT_LOGICAL_ID,
            "files": files.iter().enumerate().map(|(index, (id, path))| json!({
                "fileId": id,
                "fileIndex": index + 1,
                "path": path,
                "sizeBytes": 1_000_000u64,
                "selectable": true
            })).collect::<Vec<_>>()
        })
    }

    fn candidate_search_response_for_test(
        provider_id: Uuid,
        extension_id: &str,
        media_types: Vec<&str>,
        candidates: Vec<AcquisitionCandidate>,
    ) -> CandidateSearchResponse {
        CandidateSearchResponse {
            schema_version: 1,
            provider: CandidateProviderSummary {
                provider_id,
                extension_id: extension_id.to_string(),
                extension_name: "Test Candidate Source".to_string(),
                instance_id: Uuid::new_v4(),
                instance_name: "Default".to_string(),
                capability: ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                implementation: Some("test_candidate_source".to_string()),
                health_state: ProviderHealthState::Healthy,
                media_types: media_types.into_iter().map(ToString::to_string).collect(),
                actions: vec!["search".to_string()],
            },
            route_options: available_route_options(),
            candidates,
            warnings: Vec::new(),
        }
    }

    async fn seed_te10b_candidate_provider(state: &AppState, port: u16) -> Result<Uuid> {
        let store = ExtensionStore::new(&state.db_pool);
        seed_te10b_broker_provider(
            &store,
            "elixir.modules.real_debrid.test",
            "debrid.resolver",
            "real_debrid",
            "debrid",
        )
        .await?;
        seed_te10b_broker_provider(
            &store,
            "elixir.modules.qbittorrent.test",
            "downloader.torrent",
            "qbittorrent",
            "managed",
        )
        .await?;

        let extension_id = "elixir.sources.te10b.fixture";
        let instance_id = Uuid::new_v4();
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "TE-10B Fixture Source".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "TE-10B Fixture Source",
                    "provides": [{
                        "capability": ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
                        "slot": "default",
                        "cardinality": "one",
                        "implementation": "te10b_fixture",
                        "scope": {
                            "media_types": ["movie", "series", "tv", "anime"],
                            "actions": ["search"]
                        }
                    }],
                    "requires": {
                        "downloads": [
                            { "kind": "debrid", "mode": "broker" },
                            { "kind": "torrent", "mode": "broker" }
                        ]
                    },
                    "runtime": {
                        "type": "container",
                        "image": "example/te10b-fixture:1"
                    }
                }),
                package_hash: Some("te10b".to_string()),
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({ "resultLimit": 25 })),
                enabled: true,
            })
            .await?;
        let provider_id = stable_provider_id(
            instance_id,
            ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
            "default",
        );
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("te10b_fixture".to_string()),
                scope_json: Some(json!({
                    "media_types": ["movie", "series", "tv", "anime"],
                    "actions": ["search"]
                })),
                endpoint_json: Some(json!({
                    "scheme": "http",
                    "host": "te10b-provider.internal",
                    "port": port,
                    "base_path": "/candidate-provider",
                    "network": null
                })),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok(provider_id)
    }

    async fn seed_te10b_broker_provider(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        capability: &str,
        implementation: &str,
        provider_kind: &str,
    ) -> Result<()> {
        let instance_id = Uuid::new_v4();
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: extension_id.to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": extension_id
                }),
                package_hash: Some(format!("{extension_id}:te10b")),
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id: Uuid::new_v4(),
                instance_id,
                capability: capability.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some(implementation.to_string()),
                scope_json: Some(json!({
                    "download_broker": {
                        "provider_kind": provider_kind
                    }
                })),
                endpoint_json: Some(json!({
                    "scheme": "http",
                    "host": "127.0.0.1",
                    "port": 9,
                    "base_path": "/",
                    "network": null
                })),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn series_expansion_fails_when_tvdb_key_is_missing() -> Result<()> {
        let state = setup_test_state().await?;
        let subscription = AcquisitionSubscription {
            media_type: MediaType::Series,
            external_ids: Some(ExternalIds {
                tvdb: Some("338186".to_string()),
                tvdb_series: Some("338186".to_string()),
                ..ExternalIds::default()
            }),
            ..test_subscription()
        };
        let mut ids = subscription.external_ids.clone().unwrap_or_default();

        let err = expand_series_subscription(&state, &subscription, &mut ids, Utc::now())
            .await
            .expect_err("series expansion should fail without TVDB config");

        assert!(
            err.to_string().contains("TVDB API key is required"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn osr1_one_shot_season_snapshot_hydrates_tvdb_targets_once() -> Result<()> {
        let (state, fixture) = setup_test_state_with_tvdb_fixture().await?;
        let now = Utc::now();
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "OSR One Shot Season".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("338186".to_string()),
                    tvdb: Some("338186".to_string()),
                    ..ExternalIds::default()
                }),
                idempotency_key: None,
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Season),
                scope: Some(json!({ "kind": "season", "seasonNumber": 1 })),
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: Default::default(),
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(now - ChronoDuration::minutes(1)),
                candidate_search_after: Some(now),
            },
        )
        .await?;

        refresh_due_metadata(&state).await?;
        fixture.abort();

        let targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert_eq!(
            targets
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S01E01", "S01E02"]
        );
        assert!(targets.iter().all(|target| {
            target
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.pointer("/acquisitionRequest/mode"))
                .and_then(JsonValue::as_str)
                == Some("one_shot")
        }));

        let due = list_due_metadata_subscriptions(&state.db_pool, Utc::now(), 10).await?;
        assert!(
            due.iter()
                .all(|item| item.subscription_id != subscription.subscription_id),
            "one-shot season snapshots must not be scheduled for recurring refresh"
        );
        Ok(())
    }

    #[tokio::test]
    async fn osr1_one_shot_range_snapshot_filters_tvdb_targets() -> Result<()> {
        let (state, fixture) = setup_test_state_with_tvdb_fixture().await?;
        let now = Utc::now();
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "OSR One Shot Range".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("338186".to_string()),
                    tvdb: Some("338186".to_string()),
                    ..ExternalIds::default()
                }),
                idempotency_key: None,
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Range),
                scope: Some(json!({
                    "kind": "range",
                    "seasonNumber": 2,
                    "episodeStart": 2,
                    "episodeEnd": 3
                })),
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: Default::default(),
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(now - ChronoDuration::minutes(1)),
                candidate_search_after: Some(now),
            },
        )
        .await?;

        refresh_due_metadata(&state).await?;
        fixture.abort();

        let targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert_eq!(
            targets
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E02", "S02E03"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn osr1_one_shot_missing_snapshot_uses_provider_scope_without_recurring_refresh()
    -> Result<()> {
        let (state, fixture) = setup_test_state_with_tvdb_fixture().await?;
        let now = Utc::now();
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "OSR Missing Scope".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("338186".to_string()),
                    tvdb: Some("338186".to_string()),
                    ..ExternalIds::default()
                }),
                idempotency_key: None,
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Missing),
                scope: Some(json!({ "kind": "missing" })),
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: Default::default(),
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(now - ChronoDuration::minutes(1)),
                candidate_search_after: Some(now),
            },
        )
        .await?;

        refresh_due_metadata(&state).await?;
        fixture.abort();

        let targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert_eq!(targets.len(), 5);
        assert!(targets.iter().any(|target| target.target_key == "S01E01"));
        assert!(targets.iter().any(|target| target.target_key == "S02E03"));
        let subscription = crate::acquisition::subscriptions::get_subscription(
            &state.db_pool,
            subscription.subscription_id,
        )
        .await?
        .expect("subscription");
        assert_eq!(
            subscription.metadata_policy,
            AcquisitionMetadataPolicy::InitialOnly
        );
        assert!(subscription.last_metadata_refresh_at.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn osr1_one_shot_metadata_failure_persists_blocker_target() -> Result<()> {
        let state = setup_test_state().await?;
        let now = Utc::now();
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "OSR Blocked Metadata".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("338186".to_string()),
                    tvdb: Some("338186".to_string()),
                    ..ExternalIds::default()
                }),
                idempotency_key: None,
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Season),
                scope: Some(json!({ "kind": "season", "seasonNumber": 1 })),
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: Default::default(),
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(now - ChronoDuration::minutes(1)),
                candidate_search_after: Some(now),
            },
        )
        .await?;

        refresh_due_metadata(&state).await?;

        let targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target_key, ONE_SHOT_METADATA_BLOCKER_TARGET_KEY);
        assert_eq!(targets[0].state, AcquisitionTargetState::Blocked);
        assert!(
            targets[0]
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .contains("TVDB API key is required"),
            "unexpected blocker reason: {:?}",
            targets[0].state_reason
        );
        assert_eq!(
            targets[0]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("blocker"))
                .and_then(JsonValue::as_str),
            Some("metadata_snapshot_failed")
        );
        Ok(())
    }

    #[test]
    fn osr1_one_shot_anime_mapped_scope_uses_canonical_target_keys() -> Result<()> {
        let subscription = AcquisitionSubscription {
            media_type: MediaType::Anime,
            request_mode: AcquisitionRequestMode::OneShot,
            request_scope: AcquisitionRequestScope::SelectedTargets,
            scope: Some(json!({
                "kind": "selected_targets",
                "targetKeys": ["S03E02"]
            })),
            ..test_subscription()
        };
        let wanted = NewAcquisitionTarget {
            target_key: None,
            media_type: Some(MediaType::Anime),
            title: Some("Mapped Anime Episode".to_string()),
            season_number: Some(3),
            episode_number: Some(2),
            absolute_episode_number: Some(24),
            air_date: None,
            air_time: None,
            metadata: Some(json!({
                "source": "anizip",
                "tvdbSeason": 3,
                "tvdbEpisode": 2,
                "anilistSeason": {
                    "seasonNumber": 3,
                    "anilistId": "1003",
                    "title": "Mapped Anime S3",
                    "confidence": 1.0
                }
            })),
            state: Some(AcquisitionTargetState::Pending),
            next_search_after: None,
        };
        let extra = NewAcquisitionTarget {
            season_number: Some(3),
            episode_number: Some(3),
            absolute_episode_number: Some(25),
            ..wanted.clone()
        };

        assert!(one_shot_scope_allows_target(&subscription, &wanted));
        assert!(!one_shot_scope_allows_target(&subscription, &extra));
        Ok(())
    }

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

    #[test]
    fn asr6_selected_candidate_provenance_records_route_provider_id() -> Result<()> {
        let source_provider_id = Uuid::new_v4();
        let route_provider_id = Uuid::new_v4();
        let submission = CandidateSubmission {
            provider_id: source_provider_id,
            source_extension_id: "elixir.sources.torrentio".to_string(),
            candidate: candidate(
                "Example.Series.S01E01.1080p-GROUP",
                vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
                Some(false),
                Some(12),
            ),
            provider_warnings: Vec::new(),
            anime_coverage_plan: None,
            tv_coverage_plan: None,
            request_scope_evidence: None,
            dispatch: None,
        };

        let provenance = selected_candidate_provenance_with_submission(
            &submission,
            TORRENT_DEFAULT_LOGICAL_ID,
            &Some("qb-fallback".to_string()),
            route_provider_id,
            "Submitted through torrent fallback.",
        )?;

        assert_eq!(
            provenance
                .get("sourceProviderId")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            Some(source_provider_id.to_string())
        );
        assert_eq!(
            provenance
                .pointer("/submissionResult/routeProviderId")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            Some(route_provider_id.to_string())
        );
        assert_eq!(
            provenance
                .pointer("/submissionResult/routeLogicalId")
                .and_then(JsonValue::as_str),
            Some(TORRENT_DEFAULT_LOGICAL_ID)
        );
        Ok(())
    }

    fn failed_debrid_status(failure_class: &str) -> crate::debrid::DebridJobStatus {
        crate::debrid::DebridJobStatus {
            job_id: Uuid::new_v4(),
            status: "failed".to_string(),
            remote_status: Some("stalled".to_string()),
            source_kind: "magnet".to_string(),
            release_id: Some(Uuid::new_v4()),
            failure_class: Some(failure_class.to_string()),
            last_error: None,
            selection_error: None,
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
            idempotency_key: None,
            request_mode: AcquisitionRequestMode::Monitored,
            request_scope: AcquisitionRequestScope::Subscription,
            scope: None,
            metadata_policy: AcquisitionMetadataPolicy::Recurring,
            completion_policy: AcquisitionCompletionPolicy::Manual,
            monitor_policy: Default::default(),
            route_policy: AcquisitionRoutePolicy::DebridFirst,
            source_provider_id: None,
            release_delay_seconds: 0,
            quality_profile: None,
            metadata_refresh_after: Utc::now(),
            candidate_search_after: Utc::now(),
            last_metadata_refresh_at: None,
            last_candidate_search_at: None,
            tracking_started_at: None,
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

    fn movie_target(subscription: &AcquisitionSubscription) -> AcquisitionTarget {
        let now = Utc::now();
        AcquisitionTarget {
            target_id: Uuid::new_v4(),
            subscription_id: subscription.subscription_id,
            target_key: "MOVIE".to_string(),
            media_type: MediaType::Movie,
            title: subscription.title.clone(),
            season_number: None,
            episode_number: None,
            absolute_episode_number: None,
            air_date: None,
            air_time: None,
            metadata: Some(json!({
                "source": "te10b_test",
                "externalIds": subscription.external_ids.clone().unwrap_or_default()
            })),
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

    fn anime_absolute_target(
        subscription: &AcquisitionSubscription,
        absolute_episode_number: i32,
    ) -> AcquisitionTarget {
        let mut target = anime_episode_target(subscription, 1, absolute_episode_number);
        target.target_key = format!("A{absolute_episode_number:04}");
        target.season_number = None;
        target.episode_number = None;
        target.absolute_episode_number = Some(absolute_episode_number);
        target
    }

    fn new_series_episode_target(
        title: &str,
        season_number: i32,
        episode_number: i32,
    ) -> NewAcquisitionTarget {
        NewAcquisitionTarget {
            target_key: Some(format!("S{season_number:02}E{episode_number:02}")),
            media_type: Some(MediaType::Series),
            title: Some(title.to_string()),
            season_number: Some(season_number),
            episode_number: Some(episode_number),
            absolute_episode_number: None,
            air_date: None,
            air_time: Some(Utc::now() - ChronoDuration::days(1)),
            metadata: None,
            state: Some(AcquisitionTargetState::Pending),
            next_search_after: Some(Utc::now()),
        }
    }

    fn new_anime_episode_target(
        title: &str,
        season_number: i32,
        episode_number: i32,
        absolute_episode_number: i32,
    ) -> NewAcquisitionTarget {
        NewAcquisitionTarget {
            target_key: Some(format!("S{season_number:02}E{episode_number:02}")),
            media_type: Some(MediaType::Anime),
            title: Some(title.to_string()),
            season_number: Some(season_number),
            episode_number: Some(episode_number),
            absolute_episode_number: Some(absolute_episode_number),
            air_date: None,
            air_time: Some(Utc::now() - ChronoDuration::days(1)),
            metadata: Some(json!({
                "source": "amr1_test",
                "aliases": [title],
                "graphFingerprint": format!("{title}:graph:v1"),
                "targetCanonicalKey": format!("anime:{title}:S{season_number:02}E{episode_number:02}"),
                "tvdbEpisodeId": format!("tvdb-{season_number}-{episode_number}"),
                "anidbEpisodeId": format!("anidb-{absolute_episode_number}")
            })),
            state: Some(AcquisitionTargetState::Pending),
            next_search_after: Some(Utc::now()),
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

    fn available_route_options() -> Vec<CandidateRouteOption> {
        vec![
            CandidateRouteOption {
                logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                label: "Debrid".to_string(),
                available: true,
                selected_provider_id: Some(Uuid::new_v4()),
                selected_extension_id: Some("test.debrid".to_string()),
                blocker: None,
            },
            CandidateRouteOption {
                logical_id: TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                label: "Torrent".to_string(),
                available: true,
                selected_provider_id: Some(Uuid::new_v4()),
                selected_extension_id: Some("test.torrent".to_string()),
                blocker: None,
            },
        ]
    }

    fn release_plan_for_test(
        candidate: AcquisitionCandidate,
        route_logical_id: &str,
        release_kind: ReleaseKind,
        confidence: ReleaseConfidence,
        covered_targets: &[AcquisitionTarget],
    ) -> CandidateReleasePlan {
        CandidateReleasePlan {
            provider_id: Uuid::new_v4(),
            source_extension_id: "test.source".to_string(),
            provider_warnings: Vec::new(),
            route_logical_id: route_logical_id.to_string(),
            fingerprint: candidate_release_fingerprint(&candidate, None),
            selection: CandidateSelection {
                candidate,
                anime_coverage_plan: None,
                tv_coverage_plan: None,
            },
            release_kind,
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
            confidence,
            covered_target_ids: covered_targets
                .iter()
                .map(|target| target.target_id)
                .collect(),
            covered_target_keys: covered_targets
                .iter()
                .map(|target| target.target_key.clone())
                .collect(),
            overfetch_count: 0,
            request_scope_evidence: None,
        }
    }

    fn release_for_test(
        subscription: &AcquisitionSubscription,
        candidate: &AcquisitionCandidate,
        route_logical_id: &str,
        state: AcquisitionReleaseState,
    ) -> NewAcquisitionRelease {
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: Some(subscription.subscription_id),
            source_provider_id: None,
            source_extension_id: "test.source".to_string(),
            owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
            media_type: subscription.media_type,
            title: subscription.title.clone(),
            release_title: candidate.title.clone(),
            source: candidate.source.clone(),
            source_kind: candidate.source_kind.clone(),
            info_hash: candidate.info_hash.clone(),
            fingerprint: candidate_release_fingerprint(candidate, None),
            release_kind: ReleaseKind::Single,
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
            confidence: ReleaseConfidence::High,
            score: candidate.score,
            selected_route_logical_id: Some(route_logical_id.to_string()),
            selected_provider_id: None,
            download_id: Some(format!("download-{}", candidate.title)),
            remote_release_id: None,
            state,
            state_reason: Some("test release".to_string()),
            selected_candidate: None,
            coverage_plan: None,
        }
    }

    fn empty_queue_governor() -> QueueGovernor {
        QueueGovernor {
            caps: QueueGovernorCaps::default(),
            global_active: 0,
            active_by_route: HashMap::new(),
            active_by_subscription: HashMap::new(),
            active_by_subscription_route: HashMap::new(),
            stale_active_jobs: 0,
            searches_this_tick: 0,
            submissions_this_tick: 0,
        }
    }

    #[tokio::test]
    async fn te10b_due_provider_invocation_feeds_rr_decisions_for_required_media_shapes()
    -> Result<()> {
        let state = setup_test_state().await?;
        let (port, requests, server) = start_te10b_candidate_provider_server().await?;
        let provider_id = seed_te10b_candidate_provider(&state, port).await?;

        #[derive(Clone)]
        struct Fixture {
            subscription: AcquisitionSubscription,
            targets: Vec<AcquisitionTarget>,
            search_intent: Option<CandidateSearchIntent>,
            release_kind: ReleaseKind,
            resolver_kind: ReleaseResolverKind,
            confidence: ReleaseConfidence,
            covered: usize,
        }

        let mut movie_subscription = AcquisitionSubscription {
            media_type: MediaType::Movie,
            title: "Movie".to_string(),
            normalized_title: "movie".to_string(),
            external_ids: Some(ExternalIds {
                imdb: Some("tt1000001".to_string()),
                ..Default::default()
            }),
            source_provider_id: Some(provider_id),
            ..test_subscription()
        };
        movie_subscription.subscription_id = Uuid::new_v4();
        let movie_targets = vec![movie_target(&movie_subscription)];

        let mut episode_subscription = AcquisitionSubscription {
            source_provider_id: Some(provider_id),
            external_ids: Some(ExternalIds {
                tvdb_series: Some("100".to_string()),
                ..Default::default()
            }),
            ..test_subscription()
        };
        episode_subscription.subscription_id = Uuid::new_v4();
        let episode_targets = vec![episode_target(&episode_subscription, 2, 1)];

        let mut season_subscription = AcquisitionSubscription {
            source_provider_id: Some(provider_id),
            external_ids: Some(ExternalIds {
                tvdb_series: Some("101".to_string()),
                ..Default::default()
            }),
            ..test_subscription()
        };
        season_subscription.subscription_id = Uuid::new_v4();
        let season_targets = vec![
            episode_target(&season_subscription, 1, 1),
            episode_target(&season_subscription, 1, 2),
            episode_target(&season_subscription, 1, 3),
        ];

        let mut series_subscription = AcquisitionSubscription {
            source_provider_id: Some(provider_id),
            external_ids: Some(ExternalIds {
                tvdb_series: Some("102".to_string()),
                ..Default::default()
            }),
            ..test_subscription()
        };
        series_subscription.subscription_id = Uuid::new_v4();
        let series_targets = vec![
            episode_target(&series_subscription, 1, 1),
            episode_target(&series_subscription, 2, 1),
        ];

        let mut anime_absolute_subscription = AcquisitionSubscription {
            source_provider_id: Some(provider_id),
            external_ids: Some(ExternalIds {
                anilist: Some("200".to_string()),
                ..Default::default()
            }),
            ..anime_subscription()
        };
        anime_absolute_subscription.subscription_id = Uuid::new_v4();
        let anime_absolute_targets =
            vec![anime_absolute_target(&anime_absolute_subscription, 1000)];

        let mut anime_tvdb_subscription = AcquisitionSubscription {
            title: "Translated Anime".to_string(),
            normalized_title: "translated anime".to_string(),
            source_provider_id: Some(provider_id),
            external_ids: Some(ExternalIds {
                anilist: Some("201".to_string()),
                tvdb_series: Some("301".to_string()),
                ..Default::default()
            }),
            ..anime_subscription()
        };
        anime_tvdb_subscription.subscription_id = Uuid::new_v4();
        let mut anime_tvdb_target = anime_episode_target(&anime_tvdb_subscription, 4, 1);
        anime_tvdb_target.title = "Translated Anime".to_string();
        anime_tvdb_target.absolute_episode_number = Some(49);
        anime_tvdb_target.metadata = Some(json!({
            "source": "anilist_anizip_tvdb",
            "aliases": ["Translated Anime"],
            "targetCanonicalKey": "tvdb:301:S04E01",
            "tvdbEpisodeId": "3010401",
            "externalIds": anime_tvdb_subscription.external_ids.clone().unwrap_or_default()
        }));
        let anime_tvdb_targets = vec![anime_tvdb_target];

        let fixtures = vec![
            Fixture {
                subscription: movie_subscription,
                targets: movie_targets,
                search_intent: None,
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::MovieSingle,
                confidence: ReleaseConfidence::High,
                covered: 1,
            },
            Fixture {
                subscription: episode_subscription,
                targets: episode_targets,
                search_intent: None,
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                confidence: ReleaseConfidence::High,
                covered: 1,
            },
            Fixture {
                subscription: season_subscription,
                targets: season_targets.clone(),
                search_intent: Some(search_intent_for_targets(
                    &season_targets,
                    RetryBucket::Cold,
                )),
                release_kind: ReleaseKind::SeasonPack,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                confidence: ReleaseConfidence::High,
                covered: 3,
            },
            Fixture {
                subscription: series_subscription,
                targets: series_targets.clone(),
                search_intent: Some(CandidateSearchIntent {
                    kind: "series_pack".to_string(),
                    target_count: 2,
                    target_keys: series_targets
                        .iter()
                        .map(|target| target.target_key.clone())
                        .collect(),
                    ..Default::default()
                }),
                release_kind: ReleaseKind::SeriesPack,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                confidence: ReleaseConfidence::High,
                covered: 2,
            },
            Fixture {
                subscription: anime_absolute_subscription,
                targets: anime_absolute_targets,
                search_intent: None,
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
                confidence: ReleaseConfidence::High,
                covered: 1,
            },
            Fixture {
                subscription: anime_tvdb_subscription,
                targets: anime_tvdb_targets,
                search_intent: None,
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
                confidence: ReleaseConfidence::Medium,
                covered: 1,
            },
        ];

        for fixture in fixtures {
            let target = fixture.targets[0].clone();
            let request = candidate_search_request_for_group(
                &fixture.subscription,
                &target,
                fixture.search_intent.clone(),
            );
            let response =
                crate::http::handlers::acquisition_sources::search_candidates_with_store_at_base_url(
                    &state.db_pool,
                    request,
                    &format!("http://127.0.0.1:{port}/candidate-provider"),
                )
                .await?;
            assert_eq!(
                response.provider.provider_id, provider_id,
                "provider selection should use the subscription provider"
            );
            assert_eq!(
                response.candidates.len(),
                1,
                "{} should keep the valid candidate and drop malformed rows",
                fixture.subscription.title
            );
            assert!(
                response
                    .warnings
                    .iter()
                    .any(|warning| warning == "provider fixture warning")
            );
            assert!(
                response
                    .warnings
                    .iter()
                    .any(|warning| warning.contains("candidate[1] rejected"))
            );

            let mut governor = QueueGovernor::load(&state.db_pool).await?;
            let batch = build_candidate_release_plans(
                &state,
                &fixture.subscription,
                &response,
                &target,
                &fixture.targets,
                &mut governor,
            )
            .await?;
            assert_eq!(
                batch.plans.len(),
                1,
                "{} should produce one RR plan",
                fixture.subscription.title
            );
            let plan = &batch.plans[0];
            assert_eq!(plan.release_kind, fixture.release_kind);
            assert_eq!(plan.resolver_kind, fixture.resolver_kind);
            assert_eq!(plan.covered_target_ids.len(), fixture.covered);
            assert_eq!(plan.confidence, fixture.confidence);
            assert!(
                plan.provider_warnings
                    .iter()
                    .any(|warning| warning.contains("candidate[1] rejected"))
            );

            let group = TargetSearchGroup {
                group_key: "te10b-group".to_string(),
                representative: target.clone(),
                targets: fixture.targets.clone(),
                search_intent: fixture.search_intent.clone(),
            };
            let dispatch = scheduler_dispatch_evidence(
                &fixture.subscription,
                &group,
                plan,
                governor.capacity_snapshot(
                    fixture.subscription.subscription_id,
                    &plan.route_logical_id,
                ),
            );
            let provenance =
                selected_candidate_provenance(&plan.clone().into_submission(dispatch))?;
            assert_eq!(
                provenance
                    .pointer("/schedulerDispatch/selectedPlanScore/resolverKind")
                    .and_then(Value::as_str),
                Some(fixture.resolver_kind.as_str())
            );
            assert_eq!(
                provenance
                    .pointer("/schedulerDispatch/selectedPlanScore/coveredTargetCount")
                    .and_then(Value::as_u64),
                Some(fixture.covered as u64)
            );
            assert!(
                provenance
                    .get("providerWarnings")
                    .and_then(Value::as_array)
                    .is_some_and(|warnings| !warnings.is_empty())
            );
        }

        let captured = requests.lock().expect("requests lock").clone();
        assert_eq!(captured.len(), 6);
        assert!(captured.iter().all(|payload| {
            payload
                .pointer("/request/preferences/routePolicy")
                .and_then(Value::as_str)
                == Some("debrid_first")
        }));
        assert!(captured.iter().all(|payload| {
            payload
                .pointer("/request/target/targetKey")
                .and_then(Value::as_str)
                .is_some()
        }));
        assert!(captured.iter().any(|payload| {
            payload
                .pointer("/request/target/metadata/source")
                .and_then(Value::as_str)
                == Some("anilist_anizip_tvdb")
        }));

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn amr1_tv_resolver_rejection_persists_manual_review_candidate() -> Result<()> {
        let state = setup_test_state().await?;
        let provider_id = seed_te10b_candidate_provider(&state, 49152).await?;
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Ambiguous Review Show".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("9001".to_string()),
                    ..Default::default()
                }),
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: Some(provider_id),
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(Utc::now()),
                candidate_search_after: Some(Utc::now()),
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &state.db_pool,
            subscription.subscription_id,
            vec![
                new_series_episode_target("Ambiguous Review Show", 1, 1),
                new_series_episode_target("Ambiguous Review Show", 1, 2),
            ],
        )
        .await?;
        let group = TargetSearchGroup {
            group_key: "amr1-tv-review".to_string(),
            representative: targets[0].clone(),
            targets: targets.clone(),
            search_intent: Some(search_intent_for_targets(&targets, RetryBucket::Cold)),
        };
        let response = candidate_search_response_for_test(
            provider_id,
            "elixir.sources.amr1.tv",
            vec!["series"],
            vec![candidate(
                "Ambiguous.Review.Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
                vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
                Some(true),
                Some(100),
            )],
        );
        let mut governor = QueueGovernor::load(&state.db_pool).await?;

        process_candidate_search_response_for_group(
            &state,
            &subscription,
            &group,
            &response,
            &targets[0],
            Utc::now(),
            &mut governor,
        )
        .await?;

        let releases = list_releases(
            &state.db_pool,
            ReleaseListFilter {
                subscription_id: Some(subscription.subscription_id),
                state: Some(AcquisitionReleaseState::ReviewRequired),
                limit: Some(10),
            },
        )
        .await?;
        assert_eq!(releases.len(), 1);
        let release = &releases[0];
        assert_eq!(release.source_provider_id, Some(provider_id));
        assert_eq!(release.selected_provider_id, None);
        assert_eq!(release.download_id, None);
        assert_eq!(release.resolver_kind, ReleaseResolverKind::TvSonarrStyle);
        assert_eq!(release.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|value| value.pointer("/resolverEvidence/rejectionCodes"))
                .and_then(Value::as_array)
                .is_some_and(|codes| !codes.is_empty())
        );

        let coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
        assert_eq!(coverage.len(), 2);
        assert!(
            coverage
                .iter()
                .all(|item| item.state == ReleaseCoverageState::ReviewRequired)
        );

        let updated_targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert!(updated_targets.iter().all(|target| {
            target.state == AcquisitionTargetState::Pending
                && target.state_reason.as_deref().is_some_and(|reason| {
                    reason.contains("awaiting manual release selection")
                        && !reason.contains("review required")
                })
                && target.next_search_after.is_some()
        }));
        Ok(())
    }

    #[tokio::test]
    async fn amr1_anime_ambiguity_persists_manual_review_candidate() -> Result<()> {
        let state = setup_test_state().await?;
        let provider_id = seed_te10b_candidate_provider(&state, 49153).await?;
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Ambiguous Anime".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    anilist: Some("9101".to_string()),
                    tvdb_series: Some("9102".to_string()),
                    ..Default::default()
                }),
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: Some(provider_id),
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(Utc::now()),
                candidate_search_after: Some(Utc::now()),
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &state.db_pool,
            subscription.subscription_id,
            vec![new_anime_episode_target("Ambiguous Anime", 1, 1, 1)],
        )
        .await?;
        let group = TargetSearchGroup {
            group_key: "amr1-anime-review".to_string(),
            representative: targets[0].clone(),
            targets: targets.clone(),
            search_intent: None,
        };
        let response = candidate_search_response_for_test(
            provider_id,
            "elixir.sources.amr1.anime",
            vec!["anime"],
            vec![candidate(
                "[SubsPlease] Different Anime - 01 [1080p]",
                vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
                Some(true),
                Some(100),
            )],
        );
        let mut governor = QueueGovernor::load(&state.db_pool).await?;

        process_candidate_search_response_for_group(
            &state,
            &subscription,
            &group,
            &response,
            &targets[0],
            Utc::now(),
            &mut governor,
        )
        .await?;

        let releases = list_releases(
            &state.db_pool,
            ReleaseListFilter {
                subscription_id: Some(subscription.subscription_id),
                state: Some(AcquisitionReleaseState::ReviewRequired),
                limit: Some(10),
            },
        )
        .await?;
        assert_eq!(releases.len(), 1);
        let release = &releases[0];
        assert_eq!(release.source_provider_id, Some(provider_id));
        assert_eq!(release.resolver_kind, ReleaseResolverKind::AnimeShokoStyle);
        assert_eq!(release.selected_provider_id, None);
        assert_eq!(release.download_id, None);
        assert!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|value| value.pointer("/resolverEvidence/rejectionCodes"))
                .and_then(Value::as_array)
                .is_some_and(|codes| !codes.is_empty())
        );
        let coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, ReleaseCoverageState::ReviewRequired);
        Ok(())
    }

    #[tokio::test]
    async fn amr1_no_candidates_remains_pending_retry_without_review_release() -> Result<()> {
        let state = setup_test_state().await?;
        let provider_id = seed_te10b_candidate_provider(&state, 49154).await?;
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Empty Source Show".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("9201".to_string()),
                    ..Default::default()
                }),
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: Some(provider_id),
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(Utc::now()),
                candidate_search_after: Some(Utc::now()),
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &state.db_pool,
            subscription.subscription_id,
            vec![new_series_episode_target("Empty Source Show", 1, 1)],
        )
        .await?;
        let group = TargetSearchGroup {
            group_key: "amr1-empty-source".to_string(),
            representative: targets[0].clone(),
            targets: targets.clone(),
            search_intent: None,
        };
        let response = candidate_search_response_for_test(
            provider_id,
            "elixir.sources.amr1.empty",
            vec!["series"],
            Vec::new(),
        );
        let mut governor = QueueGovernor::load(&state.db_pool).await?;

        process_candidate_search_response_for_group(
            &state,
            &subscription,
            &group,
            &response,
            &targets[0],
            Utc::now(),
            &mut governor,
        )
        .await?;

        let releases = list_releases(
            &state.db_pool,
            ReleaseListFilter {
                subscription_id: Some(subscription.subscription_id),
                state: Some(AcquisitionReleaseState::ReviewRequired),
                limit: Some(10),
            },
        )
        .await?;
        assert!(releases.is_empty());
        let updated_targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert_eq!(updated_targets.len(), 1);
        assert_eq!(updated_targets[0].state, AcquisitionTargetState::Pending);
        assert_eq!(updated_targets[0].selected_provider_id, Some(provider_id));
        assert!(
            updated_targets[0]
                .state_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("No acquisition candidates were returned"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn mmr1_one_shot_no_candidates_marks_targets_terminal_no_results() -> Result<()> {
        let state = setup_test_state().await?;
        let provider_id = seed_te10b_candidate_provider(&state, 49155).await?;
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "One Shot Empty Source".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("9301".to_string()),
                    ..Default::default()
                }),
                idempotency_key: None,
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Season),
                scope: Some(json!({
                    "kind": "season",
                    "seasonNumber": 1
                })),
                metadata_policy: Some(AcquisitionMetadataPolicy::InitialOnly),
                completion_policy: Some(AcquisitionCompletionPolicy::TerminalSelectedTargets),
                monitor_policy:
                    crate::acquisition::subscriptions::AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: Some(provider_id),
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(Utc::now()),
                candidate_search_after: Some(Utc::now()),
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &state.db_pool,
            subscription.subscription_id,
            vec![
                new_series_episode_target("One Shot Empty Source", 1, 1),
                new_series_episode_target("One Shot Empty Source", 1, 2),
            ],
        )
        .await?;
        let group = TargetSearchGroup {
            group_key: "mmr1-one-shot-empty-source".to_string(),
            representative: targets[0].clone(),
            targets: targets.clone(),
            search_intent: Some(search_intent_for_targets(&targets, RetryBucket::Cold)),
        };
        let response = candidate_search_response_for_test(
            provider_id,
            "elixir.sources.mmr1.empty",
            vec!["series"],
            Vec::new(),
        );
        let mut governor = QueueGovernor::load(&state.db_pool).await?;

        process_candidate_search_response_for_group(
            &state,
            &subscription,
            &group,
            &response,
            &targets[0],
            Utc::now(),
            &mut governor,
        )
        .await?;

        let updated_targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert_eq!(updated_targets.len(), 2);
        assert!(updated_targets.iter().all(|target| {
            target.state == AcquisitionTargetState::Excluded
                && target.selected_provider_id == Some(provider_id)
                && target.state_reason.as_deref().is_some_and(|reason| {
                    reason.contains("No acquisition candidates were returned")
                })
        }));
        assert_eq!(updated_targets[0].search_attempts, 1);
        assert_eq!(updated_targets[1].search_attempts, 0);

        let completed = complete_terminal_acquisition_requests(
            &state.db_pool,
            10,
            "All scoped acquisition targets reached a terminal state.",
        )
        .await?;
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].target_count, 2);
        assert_eq!(completed[0].excluded_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn mmr_live_initial_backfill_no_candidates_terminalizes_after_retry_budget() -> Result<()>
    {
        let state = setup_test_state().await?;
        let provider_id = seed_te10b_candidate_provider(&state, 49156).await?;
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Initial Empty Source".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("9401".to_string()),
                    ..Default::default()
                }),
                idempotency_key: None,
                request_mode: Some(AcquisitionRequestMode::Monitored),
                request_scope: Some(AcquisitionRequestScope::Subscription),
                scope: None,
                metadata_policy: Some(AcquisitionMetadataPolicy::Recurring),
                completion_policy: Some(AcquisitionCompletionPolicy::Manual),
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: Some(provider_id),
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: Some(Utc::now()),
                candidate_search_after: Some(Utc::now()),
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &state.db_pool,
            subscription.subscription_id,
            vec![
                new_series_episode_target("Initial Empty Source", 1, 1),
                new_series_episode_target("Initial Empty Source", 1, 2),
            ],
        )
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_targets SET search_attempts = 2 WHERE target_id = ?",
        )
        .bind(targets[0].target_id.to_string())
        .execute(&state.db_pool)
        .await?;
        let refreshed_targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        let group = TargetSearchGroup {
            group_key: "mmr-live-initial-empty-source".to_string(),
            representative: refreshed_targets[0].clone(),
            targets: refreshed_targets.clone(),
            search_intent: Some(search_intent_for_targets(
                &refreshed_targets,
                RetryBucket::Cold,
            )),
        };
        let response = candidate_search_response_for_test(
            provider_id,
            "elixir.sources.mmr-live.empty",
            vec!["series"],
            Vec::new(),
        );
        let mut governor = QueueGovernor::load(&state.db_pool).await?;

        process_candidate_search_response_for_group(
            &state,
            &subscription,
            &group,
            &response,
            &refreshed_targets[0],
            Utc::now(),
            &mut governor,
        )
        .await?;

        let updated_targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        assert!(updated_targets.iter().all(|target| {
            target.state == AcquisitionTargetState::Excluded
                && target.selected_provider_id == Some(provider_id)
                && target.next_search_after.is_none()
                && target.state_reason.as_deref().is_some_and(|reason| {
                    reason.contains("No acquisition candidates were returned")
                })
        }));
        let tracking_started_at: Option<String> = sqlx::query_scalar(
            "SELECT CAST(tracking_started_at AS TEXT)
             FROM acquisition_subscriptions
             WHERE subscription_id = ?",
        )
        .bind(subscription.subscription_id.to_string())
        .fetch_one(&state.db_pool)
        .await?;
        assert!(tracking_started_at.is_some());
        Ok(())
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
    fn best_candidate_demotes_trackerless_uncached_magnets() {
        let trackerless = candidate(
            "trackerless",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            None,
            Some(80),
        );
        let mut tracked = candidate(
            "tracked",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            None,
            Some(20),
        );
        tracked.source = "magnet:?xt=urn:btih:tracked&tr=udp%3A%2F%2Ftracker-a.example%2Fannounce&tr=udp%3A%2F%2Ftracker-b.example%2Fannounce&tr=https%3A%2F%2Ftracker-c.example%2Fannounce".to_string();

        let best = select_best_candidate(
            &[trackerless, tracked],
            AcquisitionRoutePolicy::DebridFirst,
            None,
        )
        .expect("best candidate");
        assert_eq!(best.candidate.title, "tracked");
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
    fn series_backfill_grouping_collapses_large_season_into_one_search() {
        let subscription = test_subscription();
        let now = Utc::now();
        let targets = (1..=1000)
            .map(|episode| AcquisitionTarget {
                air_time: Some(now - ChronoDuration::days(30)),
                next_search_after: Some(now - ChronoDuration::minutes(1)),
                ..episode_target(&subscription, 1, episode)
            })
            .collect::<Vec<_>>();
        let subscriptions = HashMap::from([(subscription.subscription_id, subscription.clone())]);
        let targets_by_subscription =
            HashMap::from([(subscription.subscription_id, targets.clone())]);

        let groups =
            build_target_search_groups(&subscriptions, targets, &targets_by_subscription, now);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].targets.len(), 1000);
        assert!(groups[0].group_key.contains(&format!(
            "{}:series:debrid_first:season:1",
            subscription.subscription_id
        )));
        let intent = groups[0].search_intent.as_ref().expect("search intent");
        assert_eq!(intent.kind, "season_pack");
        assert_eq!(intent.season_number, Some(1));
        assert_eq!(intent.episode_start, Some(1));
        assert_eq!(intent.episode_end, Some(1000));
    }

    #[test]
    fn daily_tv_groups_by_air_date_window() {
        let subscription = AcquisitionSubscription {
            title: "Daily Show".to_string(),
            normalized_title: "dailyshow".to_string(),
            ..test_subscription()
        };
        let now = Utc::now();
        let targets = vec![
            AcquisitionTarget {
                target_key: "DATE:2026-05-01".to_string(),
                season_number: None,
                episode_number: None,
                air_date: Some("2026-05-01".to_string()),
                air_time: Some(now - ChronoDuration::days(1)),
                ..episode_target(&subscription, 1, 1)
            },
            AcquisitionTarget {
                target_key: "DATE:2026-05-02".to_string(),
                season_number: None,
                episode_number: None,
                air_date: Some("2026-05-02".to_string()),
                air_time: Some(now - ChronoDuration::days(1)),
                ..episode_target(&subscription, 1, 2)
            },
        ];
        let subscriptions = HashMap::from([(subscription.subscription_id, subscription.clone())]);
        let targets_by_subscription =
            HashMap::from([(subscription.subscription_id, targets.clone())]);

        let groups =
            build_target_search_groups(&subscriptions, targets, &targets_by_subscription, now);

        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| {
            group
                .search_intent
                .as_ref()
                .is_some_and(|intent| intent.kind == "daily")
        }));
        assert_ne!(groups[0].group_key, groups[1].group_key);
    }

    #[test]
    fn movie_targets_remain_single_target_groups() {
        let subscription = AcquisitionSubscription {
            media_type: MediaType::Movie,
            title: "Movie".to_string(),
            normalized_title: "movie".to_string(),
            ..test_subscription()
        };
        let now = Utc::now();
        let targets = vec![
            AcquisitionTarget {
                target_key: "movie".to_string(),
                media_type: MediaType::Movie,
                title: "Movie".to_string(),
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                ..episode_target(&subscription, 1, 1)
            },
            AcquisitionTarget {
                target_key: "movie-alt".to_string(),
                media_type: MediaType::Movie,
                title: "Movie Alt".to_string(),
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                ..episode_target(&subscription, 1, 2)
            },
        ];
        let subscriptions = HashMap::from([(subscription.subscription_id, subscription.clone())]);
        let groups = build_target_search_groups(&subscriptions, targets, &HashMap::new(), now);

        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| group.targets.is_empty()));
        assert!(groups.iter().all(|group| group.search_intent.is_none()));
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
    fn rr6b_tv_season_pack_plan_beats_single_episode_candidates() {
        let subscription = test_subscription();
        let targets = vec![
            episode_target(&subscription, 1, 1),
            episode_target(&subscription, 1, 2),
            episode_target(&subscription, 1, 3),
        ];
        let mut pack = candidate(
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(10),
        );
        pack.files = vec![
            AcquisitionCandidateFile {
                file_id: Some("1".to_string()),
                file_index: Some(1),
                path: "Show.S01.COMPLETE/Show.S01E01.1080p.mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
            AcquisitionCandidateFile {
                file_id: Some("2".to_string()),
                file_index: Some(2),
                path: "Show.S01.COMPLETE/Show.S01E02.1080p.mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
            AcquisitionCandidateFile {
                file_id: Some("3".to_string()),
                file_index: Some(3),
                path: "Show.S01.COMPLETE/Show.S01E03.1080p.mkv".to_string(),
                size_bytes: Some(1_000_000),
                selectable: Some(true),
            },
        ];
        let pack_plan = analyze_candidate_coverage(&subscription, &targets[0], &targets, &pack)
            .expect("pack coverage");
        let single = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(500),
        );
        let single_plan = analyze_candidate_coverage(&subscription, &targets[0], &targets, &single)
            .expect("single coverage");

        let selected = select_bounded_release_plans(
            vec![
                release_plan_for_test(
                    pack,
                    DEBRID_DEFAULT_LOGICAL_ID,
                    pack_plan.release_kind,
                    pack_plan.confidence,
                    &targets,
                ),
                release_plan_for_test(
                    single,
                    DEBRID_DEFAULT_LOGICAL_ID,
                    single_plan.release_kind,
                    single_plan.confidence,
                    &targets[0..1],
                ),
            ],
            AcquisitionRoutePolicy::DebridFirst,
            &targets,
            5,
        );

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].release_kind, ReleaseKind::SeasonPack);
        assert_eq!(selected[0].covered_target_ids.len(), 3);
    }

    #[test]
    fn rr6b_singles_fill_only_uncovered_gaps_after_pack_selection() {
        let subscription = test_subscription();
        let targets = vec![
            episode_target(&subscription, 1, 1),
            episode_target(&subscription, 1, 2),
            episode_target(&subscription, 1, 3),
        ];
        let pack = release_plan_for_test(
            candidate(
                "Show.S01E01-E02.1080p.WEB-DL-GROUP",
                vec![DEBRID_DEFAULT_LOGICAL_ID],
                Some(true),
                Some(10),
            ),
            DEBRID_DEFAULT_LOGICAL_ID,
            ReleaseKind::MultiEpisode,
            ReleaseConfidence::High,
            &targets[0..2],
        );
        let duplicate_single = release_plan_for_test(
            candidate(
                "Show.S01E01.1080p.WEB-DL-GROUP",
                vec![DEBRID_DEFAULT_LOGICAL_ID],
                Some(true),
                Some(1000),
            ),
            DEBRID_DEFAULT_LOGICAL_ID,
            ReleaseKind::Single,
            ReleaseConfidence::High,
            &targets[0..1],
        );
        let gap_single = release_plan_for_test(
            candidate(
                "Show.S01E03.1080p.WEB-DL-GROUP",
                vec![DEBRID_DEFAULT_LOGICAL_ID],
                Some(true),
                Some(100),
            ),
            DEBRID_DEFAULT_LOGICAL_ID,
            ReleaseKind::Single,
            ReleaseConfidence::High,
            &targets[2..3],
        );

        let selected = select_bounded_release_plans(
            vec![duplicate_single, gap_single, pack],
            AcquisitionRoutePolicy::DebridFirst,
            &targets,
            5,
        );

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].release_kind, ReleaseKind::MultiEpisode);
        assert_eq!(
            selected[1].covered_target_keys,
            BTreeSet::from(["S01E03".to_string()])
        );
    }

    #[tokio::test]
    async fn rr6b_existing_active_or_completed_fingerprint_is_not_resubmitted() -> Result<()> {
        let database = setup_test_db().await?;
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(10),
        );
        let fingerprint = candidate_release_fingerprint(&candidate, None);
        upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: None,
                source_provider_id: None,
                source_extension_id: "test.source".to_string(),
                owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
                media_type: MediaType::Series,
                title: "Show".to_string(),
                release_title: candidate.title.clone(),
                source: candidate.source.clone(),
                source_kind: candidate.source_kind.clone(),
                info_hash: candidate.info_hash.clone(),
                fingerprint: fingerprint.clone(),
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
                confidence: ReleaseConfidence::High,
                score: None,
                selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                selected_provider_id: None,
                download_id: Some("download-1".to_string()),
                remote_release_id: None,
                state: AcquisitionReleaseState::Submitted,
                state_reason: Some("submitted".to_string()),
                selected_candidate: None,
                coverage_plan: None,
            },
        )
        .await?;

        assert!(
            release_fingerprint_already_claimed(&database.pool, "test.source", &fingerprint)
                .await?
        );
        upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                state: AcquisitionReleaseState::Failed,
                fingerprint: "failed-fingerprint".to_string(),
                release_title: "Failed".to_string(),
                source: "magnet:?xt=urn:btih:failed".to_string(),
                ..NewAcquisitionRelease {
                    release_id: None,
                    subscription_id: None,
                    source_provider_id: None,
                    source_extension_id: "test.source".to_string(),
                    owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: candidate.title,
                    source: candidate.source,
                    source_kind: candidate.source_kind,
                    info_hash: None,
                    fingerprint: "failed-fingerprint".to_string(),
                    release_kind: ReleaseKind::Single,
                    resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                    resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
                    confidence: ReleaseConfidence::High,
                    score: None,
                    selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                    selected_provider_id: None,
                    download_id: None,
                    remote_release_id: None,
                    state: AcquisitionReleaseState::Failed,
                    state_reason: Some("failed".to_string()),
                    selected_candidate: None,
                    coverage_plan: None,
                }
            },
        )
        .await?;
        assert!(
            !release_fingerprint_already_claimed(
                &database.pool,
                "test.source",
                "failed-fingerprint"
            )
            .await?
        );
        upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                state: AcquisitionReleaseState::Cancelled,
                fingerprint: "rejected-fingerprint".to_string(),
                release_title: "Rejected".to_string(),
                source: "magnet:?xt=urn:btih:rejected".to_string(),
                coverage_plan: Some(json!({
                    "manualReview": {
                        "status": "rejected",
                        "reason": "wrong pack"
                    },
                    "retrySuppression": {
                        "status": "rejected",
                        "suppressAutomaticRediscovery": true
                    }
                })),
                ..NewAcquisitionRelease {
                    release_id: None,
                    subscription_id: None,
                    source_provider_id: None,
                    source_extension_id: "test.source".to_string(),
                    owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Rejected".to_string(),
                    source: "magnet:?xt=urn:btih:rejected".to_string(),
                    source_kind: "magnet".to_string(),
                    info_hash: None,
                    fingerprint: "rejected-fingerprint".to_string(),
                    release_kind: ReleaseKind::Single,
                    resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                    resolver_version: TV_SONARR_STYLE_RESOLVER_VERSION.to_string(),
                    confidence: ReleaseConfidence::High,
                    score: None,
                    selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                    selected_provider_id: None,
                    download_id: None,
                    remote_release_id: None,
                    state: AcquisitionReleaseState::Cancelled,
                    state_reason: Some("rejected".to_string()),
                    selected_candidate: None,
                    coverage_plan: None,
                }
            },
        )
        .await?;
        assert!(
            release_fingerprint_already_claimed(
                &database.pool,
                "test.source",
                "rejected-fingerprint"
            )
            .await?,
            "rejected fingerprint should be suppressed until explicit alternate search policy"
        );
        Ok(())
    }

    #[test]
    fn asr8_uncached_trackerless_candidate_ranks_below_healthier_tracked_candidate() {
        let target = episode_target(&test_subscription(), 1, 1);
        let mut trackerless = candidate(
            "Show.S01E01.1080p.Trackerless-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(100),
        );
        trackerless.score = Some(1.0);
        trackerless.source =
            "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();

        let mut tracked = candidate(
            "Show.S01E01.1080p.Tracked-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(20),
        );
        tracked.score = Some(0.1);
        tracked.source = concat!(
            "magnet:?xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "&tr=udp%3A%2F%2Ftracker.example%3A1337%2Fannounce",
            "&tr=udp%3A%2F%2Ftracker2.example%3A1337%2Fannounce"
        )
        .to_string();

        let trackerless_plan = release_plan_for_test(
            trackerless,
            DEBRID_DEFAULT_LOGICAL_ID,
            ReleaseKind::Single,
            ReleaseConfidence::High,
            std::slice::from_ref(&target),
        );
        let tracked_plan = release_plan_for_test(
            tracked,
            DEBRID_DEFAULT_LOGICAL_ID,
            ReleaseKind::Single,
            ReleaseConfidence::High,
            &[target],
        );

        assert!(
            compare_release_plans(
                &tracked_plan,
                &trackerless_plan,
                AcquisitionRoutePolicy::DebridFirst,
            ) == Ordering::Greater,
            "server freshness scoring should prefer healthier tracker evidence over stale source seeder hints"
        );
    }

    #[test]
    fn asr8_selected_candidate_provenance_exposes_tracker_and_weak_swarm_evidence() -> Result<()> {
        let provider_id = Uuid::new_v4();
        let candidate = normalize_acquisition_candidate(AcquisitionCandidate {
            id: None,
            title: "Show.S01E01.1080p.Trackerless-GROUP".to_string(),
            source: "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: None,
            seeders: None,
            language: None,
            cached_debrid: Some(false),
            rank: None,
            score: Some(0.99),
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: vec![DEBRID_DEFAULT_LOGICAL_ID.to_string()],
            default_route: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            raw: None,
        })?;
        let submission = CandidateSubmission {
            provider_id,
            source_extension_id: "elixir.sources.torrentio".to_string(),
            candidate,
            provider_warnings: Vec::new(),
            anime_coverage_plan: None,
            tv_coverage_plan: None,
            request_scope_evidence: None,
            dispatch: None,
        };

        let provenance = selected_candidate_provenance(&submission)?;
        assert_eq!(
            provenance
                .pointer("/raw/serverEvidence/torrentHealth/trackerCount")
                .and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            provenance
                .pointer(
                    "/raw/serverEvidence/torrentHealth/liveDownloaderEvidenceOverridesSourceHints"
                )
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            provenance
                .get("scoreBadges")
                .and_then(Value::as_array)
                .is_some_and(|badges| badges.iter().any(|badge| {
                    badge.get("label").and_then(Value::as_str) == Some("Weak swarm")
                        && badge
                            .get("detail")
                            .and_then(Value::as_str)
                            .is_some_and(|detail| detail.contains("no tracker URLs"))
                }))
        );
        Ok(())
    }

    #[tokio::test]
    async fn asr8_live_zero_seed_retry_suppression_skips_dead_fingerprint() -> Result<()> {
        let state = setup_test_state().await?;
        let provider_id = Uuid::new_v4();
        let mut subscription = test_subscription();
        subscription.route_policy = AcquisitionRoutePolicy::TorrentOnly;
        let target = episode_target(&subscription, 1, 1);

        let mut dead = candidate(
            "Show.S01E01.1080p.Dead-GROUP",
            vec![TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(0),
        );
        dead.source = "magnet:?xt=urn:btih:dddddddddddddddddddddddddddddddddddddddd".to_string();
        let mut healthy = candidate(
            "Show.S01E01.1080p.Healthier-GROUP",
            vec![TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(25),
        );
        healthy.source = concat!(
            "magnet:?xt=urn:btih:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "&tr=udp%3A%2F%2Ftracker.example%3A1337%2Fannounce"
        )
        .to_string();

        let dead_fingerprint = candidate_release_fingerprint(&dead, Some(provider_id));
        let mut failed_release = release_for_test(
            &subscription,
            &dead,
            TORRENT_DEFAULT_LOGICAL_ID,
            AcquisitionReleaseState::Failed,
        );
        failed_release.subscription_id = None;
        failed_release.source_extension_id = "elixir.sources.torrentio".to_string();
        failed_release.fingerprint = dead_fingerprint;
        failed_release.coverage_plan = Some(json!({
            "torrentRuntime": {
                "runtimeState": "failed",
                "failureState": "no_seeds",
                "connectedSeeds": 0,
                "completeSeeds": 0,
                "availability": 0.0
            },
            "retrySuppression": {
                "status": "rejected",
                "suppressAutomaticRediscovery": true,
                "reason": "no_seeds"
            }
        }));
        upsert_release(&state.db_pool, failed_release).await?;

        let response = CandidateSearchResponse {
            schema_version: 1,
            provider: CandidateProviderSummary {
                provider_id,
                extension_id: "elixir.sources.torrentio".to_string(),
                extension_name: "Torrentio".to_string(),
                instance_id: Uuid::new_v4(),
                instance_name: "Default".to_string(),
                capability: ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                implementation: Some("torrentio".to_string()),
                health_state: ProviderHealthState::Healthy,
                media_types: vec!["series".to_string()],
                actions: Vec::new(),
            },
            route_options: available_route_options(),
            candidates: vec![dead, healthy.clone()],
            warnings: Vec::new(),
        };
        let mut governor = QueueGovernor::load(&state.db_pool).await?;
        let batch = build_candidate_release_plans(
            &state,
            &subscription,
            &response,
            &target,
            std::slice::from_ref(&target),
            &mut governor,
        )
        .await?;

        assert_eq!(batch.plans.len(), 1);
        assert_eq!(batch.plans[0].selection.candidate.title, healthy.title);
        Ok(())
    }

    #[tokio::test]
    async fn rr6b_debrid_first_uses_torrent_when_debrid_capacity_is_full() -> Result<()> {
        let database = setup_test_db().await?;
        let subscription = test_subscription();
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(10),
        );
        let mut governor = empty_queue_governor();
        governor.caps.global_debrid = 0;

        let route = select_candidate_route_for_plan(
            &database.pool,
            &subscription,
            &candidate,
            &available_route_options(),
            &mut governor,
        )
        .await?
        .expect("torrent route should be selected");

        assert_eq!(route, TORRENT_DEFAULT_LOGICAL_ID);
        Ok(())
    }

    #[tokio::test]
    async fn rr6b_torrent_only_rejects_blocked_torrent_route() -> Result<()> {
        let database = setup_test_db().await?;
        let subscription = AcquisitionSubscription {
            route_policy: AcquisitionRoutePolicy::TorrentOnly,
            ..test_subscription()
        };
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![TORRENT_DEFAULT_LOGICAL_ID],
            None,
            Some(10),
        );
        let mut routes = available_route_options();
        if let Some(route) = routes
            .iter_mut()
            .find(|route| route.logical_id == TORRENT_DEFAULT_LOGICAL_ID)
        {
            route.available = false;
            route.blocker = Some("protected downloader route is blocked".to_string());
        }
        let mut governor = empty_queue_governor();

        let result = select_candidate_route_for_plan(
            &database.pool,
            &subscription,
            &candidate,
            &routes,
            &mut governor,
        )
        .await;

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn asr3_debrid_failure_fallback_uses_only_qbittorrent_route() {
        let subscription = test_subscription();
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![
                DEBRID_DEFAULT_LOGICAL_ID,
                "acquisition.debrid.provider.premiumize",
                TORRENT_DEFAULT_LOGICAL_ID,
            ],
            Some(false),
            Some(0),
        );
        let status = failed_debrid_status("no_seeds");

        let action = debrid_failure_fallback_action(&subscription, &candidate, &status);

        assert_eq!(
            action,
            DebridFailureFallbackAction::SubmitTorrent {
                route_logical_id: TORRENT_DEFAULT_LOGICAL_ID
            }
        );
    }

    #[test]
    fn debrid_rate_limit_retries_provider_later_without_qbittorrent_fallback() {
        let subscription = test_subscription();
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(0),
        );
        let status = failed_debrid_status("rate_limited");

        let action = debrid_failure_fallback_action(&subscription, &candidate, &status);

        match action {
            DebridFailureFallbackAction::NoAutomaticFallback { reason } => {
                assert!(reason.contains("retry provider-backed acquisition later"));
                assert!(reason.contains("rate limiting"));
            }
            other => panic!("expected retry-later action, got {other:?}"),
        }
    }

    #[test]
    fn debrid_account_or_quota_failures_block_automatic_fallback() {
        let subscription = test_subscription();
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(0),
        );

        for failure_class in ["provider_auth_missing", "quota_exhausted"] {
            let status = failed_debrid_status(failure_class);
            let action = debrid_failure_fallback_action(&subscription, &candidate, &status);
            match action {
                DebridFailureFallbackAction::NoAutomaticFallback { reason } => {
                    assert!(reason.contains("Check the active debrid account"));
                }
                other => panic!("expected account-action block, got {other:?}"),
            }
        }
    }

    #[test]
    fn debrid_source_rejections_still_use_allowed_torrent_fallback() {
        let subscription = test_subscription();
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(0),
        );
        let status = failed_debrid_status("content_blocked");

        let action = debrid_failure_fallback_action(&subscription, &candidate, &status);

        assert_eq!(
            action,
            DebridFailureFallbackAction::SubmitTorrent {
                route_logical_id: TORRENT_DEFAULT_LOGICAL_ID
            }
        );
    }

    #[test]
    fn asr3_debrid_failure_without_torrent_route_retries_source_candidate() {
        let subscription = test_subscription();
        let candidate = AcquisitionCandidate {
            source: "https://hoster.example/file".to_string(),
            source_kind: "hoster".to_string(),
            supported_routes: vec![DEBRID_DEFAULT_LOGICAL_ID.to_string()],
            ..candidate(
                "Show.S01E01.1080p.WEB-DL-GROUP",
                vec![DEBRID_DEFAULT_LOGICAL_ID],
                Some(false),
                None,
            )
        };
        let status = failed_debrid_status("provider_stalled");

        let action = debrid_failure_fallback_action(&subscription, &candidate, &status);

        match action {
            DebridFailureFallbackAction::RetryNextCandidate { reason } => {
                assert!(reason.contains("no qBittorrent fallback"));
                assert!(reason.contains("provider transfer is stalled"));
            }
            other => panic!("expected source retry, got {other:?}"),
        }
    }

    #[test]
    fn asr3_debrid_only_policy_does_not_use_torrent_fallback() {
        let subscription = AcquisitionSubscription {
            route_policy: AcquisitionRoutePolicy::DebridOnly,
            ..test_subscription()
        };
        let candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
            Some(false),
            Some(0),
        );
        let status = failed_debrid_status("no_seeds");

        let action = debrid_failure_fallback_action(&subscription, &candidate, &status);

        match action {
            DebridFailureFallbackAction::NoAutomaticFallback { reason } => {
                assert!(reason.contains("debrid_only"));
                assert!(reason.contains("has no seeds"));
            }
            other => panic!("expected blocked fallback, got {other:?}"),
        }
    }

    #[test]
    fn rr6b_review_required_tv_pack_is_not_auto_planned() {
        let subscription = test_subscription();
        let targets = vec![
            episode_target(&subscription, 1, 1),
            episode_target(&subscription, 1, 2),
        ];
        let pack_without_files = candidate(
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(10),
        );

        assert!(
            analyze_candidate_coverage(&subscription, &targets[0], &targets, &pack_without_files)
                .is_none()
        );
    }

    #[test]
    fn rr6c_fair_order_rotates_due_groups_between_subscriptions() {
        let sub_a = test_subscription();
        let sub_b = AcquisitionSubscription {
            subscription_id: Uuid::new_v4(),
            title: "Other".to_string(),
            normalized_title: "other".to_string(),
            ..test_subscription()
        };
        let group = |subscription: &AcquisitionSubscription, episode: i32| TargetSearchGroup {
            group_key: format!(
                "{}:series:debrid_first:season:1:{episode}",
                subscription.subscription_id
            ),
            representative: episode_target(subscription, 1, episode),
            targets: vec![episode_target(subscription, 1, episode)],
            search_intent: None,
        };

        let ordered = fair_order_search_groups(vec![
            group(&sub_a, 1),
            group(&sub_a, 2),
            group(&sub_a, 3),
            group(&sub_b, 1),
            group(&sub_b, 2),
        ]);
        let ids = ordered
            .iter()
            .map(|group| group.representative.subscription_id)
            .collect::<Vec<_>>();

        assert_ne!(ids[0], ids[1]);
        assert_eq!(ids[0], ids[2]);
        assert_eq!(ids[1], ids[3]);
        assert!(ids[4] == sub_a.subscription_id || ids[4] == sub_b.subscription_id);
        assert_eq!(
            ids.iter()
                .filter(|id| **id == sub_a.subscription_id)
                .count(),
            3
        );
        assert_eq!(
            ids.iter()
                .filter(|id| **id == sub_b.subscription_id)
                .count(),
            2
        );
    }

    #[test]
    fn osr3_large_one_shot_groups_are_interleaved_with_other_requests() {
        let one_shot = AcquisitionSubscription {
            request_mode: AcquisitionRequestMode::OneShot,
            request_scope: AcquisitionRequestScope::Season,
            metadata_policy: AcquisitionMetadataPolicy::InitialOnly,
            completion_policy: AcquisitionCompletionPolicy::TerminalSelectedTargets,
            monitor_policy:
                crate::acquisition::subscriptions::AcquisitionMonitorPolicy::SelectedTargets,
            scope: Some(json!({ "kind": "season", "seasonNumber": 1 })),
            ..test_subscription()
        };
        let monitored = AcquisitionSubscription {
            subscription_id: Uuid::new_v4(),
            title: "Monitored".to_string(),
            normalized_title: "monitored".to_string(),
            ..test_subscription()
        };
        let group = |subscription: &AcquisitionSubscription, episode: i32| TargetSearchGroup {
            group_key: format!(
                "{}:series:{}:episode:{episode}",
                subscription.subscription_id,
                subscription.request_mode.as_str()
            ),
            representative: episode_target(subscription, 1, episode),
            targets: vec![episode_target(subscription, 1, episode)],
            search_intent: Some(search_intent_for_targets(
                &[episode_target(subscription, 1, episode)],
                RetryBucket::Cold,
            )),
        };

        let ordered = fair_order_search_groups(vec![
            group(&one_shot, 1),
            group(&one_shot, 2),
            group(&one_shot, 3),
            group(&one_shot, 4),
            group(&monitored, 1),
            group(&monitored, 2),
        ]);
        let ids = ordered
            .iter()
            .map(|group| group.representative.subscription_id)
            .collect::<Vec<_>>();

        assert_eq!(ids.len(), 6);
        assert_ne!(
            ids[0], ids[1],
            "large one-shot scopes must not monopolize the scheduler lane"
        );
        assert_eq!(
            ids.iter()
                .filter(|id| **id == one_shot.subscription_id)
                .count(),
            4
        );
        assert_eq!(
            ids.iter()
                .filter(|id| **id == monitored.subscription_id)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn osr3_scheduler_audit_records_one_shot_mode_scope_and_targets() -> Result<()> {
        let state = setup_test_state().await?;
        let subscription = create_subscription(
            &state.db_pool,
            NewAcquisitionSubscription {
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Season),
                scope: Some(json!({ "kind": "season", "seasonNumber": 1 })),
                metadata_policy: Some(AcquisitionMetadataPolicy::InitialOnly),
                completion_policy: Some(AcquisitionCompletionPolicy::TerminalSelectedTargets),
                ..NewAcquisitionSubscription {
                    media_type: MediaType::Series,
                    title: "Audit Show".to_string(),
                    year: Some(2026),
                    external_ids: None,
                    idempotency_key: None,
                    request_mode: None,
                    request_scope: None,
                    scope: None,
                    metadata_policy: None,
                    completion_policy: None,
                    monitor_policy: Default::default(),
                    route_policy: AcquisitionRoutePolicy::DebridFirst,
                    source_provider_id: None,
                    release_delay_seconds: Some(0),
                    quality_profile: None,
                    metadata_refresh_after: Some(Utc::now()),
                    candidate_search_after: Some(Utc::now()),
                }
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &state.db_pool,
            subscription.subscription_id,
            vec![
                new_series_episode_target("Audit Show", 1, 1),
                new_series_episode_target("Audit Show", 1, 2),
            ],
        )
        .await?;
        let group = TargetSearchGroup {
            group_key: "osr3-audit-group".to_string(),
            representative: targets[0].clone(),
            targets: targets.clone(),
            search_intent: Some(search_intent_for_targets(&targets, RetryBucket::Cold)),
        };

        record_scheduler_search_audit_event(&state, &subscription, &group, &targets[0]).await?;

        assert_eq!(
            count_acquisition_audit_events_for_subscription(
                &state.db_pool,
                subscription.subscription_id,
                EVENT_ACQUISITION_SEARCH_SCHEDULED,
            )
            .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn osr4_one_shot_episode_accepts_season_pack_with_scoped_file_selection() -> Result<()> {
        let state = setup_test_state().await?;
        let subscription = AcquisitionSubscription {
            request_mode: AcquisitionRequestMode::OneShot,
            request_scope: AcquisitionRequestScope::Episode,
            scope: Some(json!({
                "kind": "episode",
                "seasonNumber": 1,
                "episodeNumber": 1,
                "targetKey": "S01E01"
            })),
            metadata_policy:
                crate::acquisition::subscriptions::AcquisitionMetadataPolicy::InitialOnly,
            completion_policy:
                crate::acquisition::subscriptions::AcquisitionCompletionPolicy::TerminalSelectedTargets,
            monitor_policy:
                crate::acquisition::subscriptions::AcquisitionMonitorPolicy::SelectedTargets,
            source_provider_id: Some(Uuid::new_v4()),
            ..test_subscription()
        };
        let target = episode_target(&subscription, 1, 1);
        let candidate: AcquisitionCandidate = serde_json::from_value(candidate_json(
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
            "osr4seasonpack",
            &[
                ("1", "Show.S01.COMPLETE/Show.S01E01.1080p.mkv"),
                ("2", "Show.S01.COMPLETE/Show.S01E02.1080p.mkv"),
            ],
        ))?;
        let response = candidate_search_response_for_test(
            subscription.source_provider_id.expect("provider id"),
            "test.source",
            vec!["series"],
            vec![candidate],
        );
        let mut governor = QueueGovernor::load(&state.db_pool).await?;

        let batch = build_candidate_release_plans(
            &state,
            &subscription,
            &response,
            &target,
            &[target.clone()],
            &mut governor,
        )
        .await?;

        assert_eq!(batch.plans.len(), 1);
        assert!(batch.review_candidates.is_empty());
        let plan = &batch.plans[0];
        assert_eq!(plan.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(plan.confidence, ReleaseConfidence::High);
        assert_eq!(
            plan.covered_target_keys,
            BTreeSet::from(["S01E01".to_string()])
        );
        assert_eq!(plan.overfetch_count, 1);
        assert_eq!(
            plan.request_scope_evidence
                .as_ref()
                .and_then(|value| value.get("requestMode"))
                .and_then(JsonValue::as_str),
            Some("one_shot")
        );

        let group = TargetSearchGroup {
            group_key: "osr4-one-shot-episode".to_string(),
            representative: target.clone(),
            targets: vec![target],
            search_intent: Some(CandidateSearchIntent {
                kind: "episode".to_string(),
                target_count: 1,
                target_keys: vec!["S01E01".to_string()],
                season_number: Some(1),
                episode_start: Some(1),
                episode_end: Some(1),
                ..Default::default()
            }),
        };
        let dispatch = scheduler_dispatch_evidence(
            &subscription,
            &group,
            plan,
            governor.capacity_snapshot(subscription.subscription_id, &plan.route_logical_id),
        );
        let provenance = selected_candidate_provenance(&plan.clone().into_submission(dispatch))?;
        assert_eq!(
            provenance
                .pointer("/requestScopeEvidence/requestScope")
                .and_then(JsonValue::as_str),
            Some("episode")
        );
        assert_eq!(
            provenance
                .pointer("/requestScopeEvidence/targetKeys/0")
                .and_then(JsonValue::as_str),
            Some("S01E01")
        );
        Ok(())
    }

    #[tokio::test]
    async fn osr4_one_shot_season_accepts_multiseason_pack_by_selecting_requested_files()
    -> Result<()> {
        let state = setup_test_state().await?;
        let subscription = AcquisitionSubscription {
            request_mode: AcquisitionRequestMode::OneShot,
            request_scope: AcquisitionRequestScope::Season,
            scope: Some(json!({ "kind": "season", "seasonNumber": 2 })),
            metadata_policy:
                crate::acquisition::subscriptions::AcquisitionMetadataPolicy::InitialOnly,
            completion_policy:
                crate::acquisition::subscriptions::AcquisitionCompletionPolicy::TerminalSelectedTargets,
            monitor_policy:
                crate::acquisition::subscriptions::AcquisitionMonitorPolicy::SelectedTargets,
            source_provider_id: Some(Uuid::new_v4()),
            ..test_subscription()
        };
        let targets = vec![
            episode_target(&subscription, 2, 1),
            episode_target(&subscription, 2, 2),
        ];
        let candidate: AcquisitionCandidate = serde_json::from_value(candidate_json(
            "Show.S01-S03.1080p.BluRay-GROUP",
            "osr4multiseason",
            &[
                ("s1e1", "Show.S01-S03/Show.S01E01.1080p.mkv"),
                ("s2e1", "Show.S01-S03/Show.S02E01.1080p.mkv"),
                ("s2e2", "Show.S01-S03/Show.S02E02.1080p.mkv"),
                ("s3e1", "Show.S01-S03/Show.S03E01.1080p.mkv"),
            ],
        ))?;
        let response = candidate_search_response_for_test(
            subscription.source_provider_id.expect("provider id"),
            "test.source",
            vec!["series"],
            vec![candidate],
        );
        let mut governor = QueueGovernor::load(&state.db_pool).await?;

        let batch = build_candidate_release_plans(
            &state,
            &subscription,
            &response,
            &targets[0],
            &targets,
            &mut governor,
        )
        .await?;

        assert_eq!(batch.plans.len(), 1);
        assert!(batch.review_candidates.is_empty());
        let plan = &batch.plans[0];
        assert_eq!(plan.release_kind, ReleaseKind::MultiSeasonPack);
        assert_eq!(plan.covered_target_keys.len(), 2);
        assert_eq!(plan.overfetch_count, 2);
        assert_eq!(
            plan.request_scope_evidence
                .as_ref()
                .and_then(|value| value.get("requestScope"))
                .and_then(JsonValue::as_str),
            Some("season")
        );
        Ok(())
    }

    #[test]
    fn rr6c_dispatch_evidence_records_plan_route_capacity_and_search_context() {
        let subscription = test_subscription();
        let targets = vec![
            episode_target(&subscription, 1, 1),
            episode_target(&subscription, 1, 2),
        ];
        let plan = release_plan_for_test(
            candidate(
                "Show.S01E01-E02.1080p.WEB-DL-GROUP",
                vec![DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID],
                Some(true),
                Some(42),
            ),
            DEBRID_DEFAULT_LOGICAL_ID,
            ReleaseKind::MultiEpisode,
            ReleaseConfidence::High,
            &targets,
        );
        let group = TargetSearchGroup {
            group_key: "group-key".to_string(),
            representative: targets[0].clone(),
            targets: targets.clone(),
            search_intent: Some(search_intent_for_targets(&targets, RetryBucket::Cold)),
        };
        let evidence = scheduler_dispatch_evidence(
            &subscription,
            &group,
            &plan,
            QueueCapacitySnapshot {
                global_active: 1,
                global_limit: 12,
                subscription_active: 1,
                subscription_limit: 5,
                route_active: 0,
                route_limit: Some(1),
                subscription_route_active: 0,
                subscription_route_limit: Some(1),
                searches_this_tick: 1,
                search_tick_limit: 20,
                submissions_this_tick: 0,
                submission_tick_limit: 5,
                stale_active_jobs: 0,
            },
        );
        let value = serde_json::to_value(&evidence).expect("dispatch evidence serializes");

        assert_eq!(value["schedulerPhase"], "rr6c");
        assert_eq!(value["groupKey"], "group-key");
        assert_eq!(value["searchIntent"]["kind"], "multi_episode");
        assert_eq!(
            value["routeDecision"]["selectedRouteLogicalId"],
            DEBRID_DEFAULT_LOGICAL_ID
        );
        assert_eq!(value["selectedPlanScore"]["coveredTargetCount"], 2);
        assert_eq!(value["capacitySnapshot"]["globalActive"], 1);
    }

    #[tokio::test]
    async fn rr6c_dispatch_reservation_rechecks_database_capacity_for_races() -> Result<()> {
        let database = setup_test_db().await?;
        let subscription = test_subscription();
        let active_candidate = candidate(
            "Show.S01E01.1080p.WEB-DL-GROUP",
            vec![DEBRID_DEFAULT_LOGICAL_ID],
            Some(true),
            Some(10),
        );
        let mut release_input = release_for_test(
            &subscription,
            &active_candidate,
            DEBRID_DEFAULT_LOGICAL_ID,
            AcquisitionReleaseState::Submitted,
        );
        release_input.subscription_id = None;
        let release = upsert_release(&database.pool, release_input).await?;
        upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                provider_id: None,
                download_id: Some("race-download".to_string()),
                remote_release_id: None,
                state: ReleaseJobState::Submitted,
                state_reason: Some("active race job".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;

        let mut governor = empty_queue_governor();
        governor.caps.global = 1;
        let block = governor
            .try_reserve(
                &database.pool,
                subscription.subscription_id,
                DEBRID_DEFAULT_LOGICAL_ID,
            )
            .await?
            .expect_err("db active job should fill global capacity");

        assert_eq!(block.kind, QueueCapacityLimitKind::Global);
        assert_eq!(block.global_active, 1);
        assert_eq!(governor.global_active, 1);
        Ok(())
    }

    #[test]
    fn queue_governor_enforces_debrid_single_lane_route_cap() {
        let subscription_id = Uuid::new_v4();
        let mut governor = empty_queue_governor();

        assert!(
            governor
                .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
                .is_ok()
        );
        let block = governor
            .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
            .expect_err("debrid route cap");

        assert_eq!(block.kind, QueueCapacityLimitKind::Route);
        assert_eq!(
            block.route_logical_id.as_deref(),
            Some(DEBRID_DEFAULT_LOGICAL_ID)
        );
        assert_eq!(block.global_active, DEFAULT_GLOBAL_DEBRID_RELEASE_JOB_CAP);
        assert_eq!(block.global_limit, DEFAULT_GLOBAL_DEBRID_RELEASE_JOB_CAP);
    }

    #[test]
    fn queue_governor_enforces_global_subscription_search_and_submission_caps() {
        let subscription_id = Uuid::new_v4();
        let mut governor = empty_queue_governor();
        governor.caps.global = 1;
        governor.global_active = 1;
        let global = governor
            .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
            .expect_err("global cap");
        assert_eq!(global.kind, QueueCapacityLimitKind::Global);

        let mut governor = empty_queue_governor();
        governor.caps.subscription = 1;
        governor.active_by_subscription.insert(subscription_id, 1);
        let subscription = governor
            .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
            .expect_err("subscription cap");
        assert_eq!(subscription.kind, QueueCapacityLimitKind::Subscription);

        let mut governor = empty_queue_governor();
        governor.caps.max_submissions_per_tick = 1;
        assert!(
            governor
                .try_reserve_loaded(subscription_id, DEBRID_DEFAULT_LOGICAL_ID)
                .is_ok()
        );
        let tick = governor
            .try_reserve_loaded(Uuid::new_v4(), DEBRID_DEFAULT_LOGICAL_ID)
            .expect_err("submission tick cap");
        assert_eq!(tick.kind, QueueCapacityLimitKind::SubmissionTick);

        let mut governor = empty_queue_governor();
        governor.caps.max_candidate_searches_per_tick = 0;
        assert_eq!(governor.searches_this_tick, 0);
        let reason = queue_capacity_reason(&QueueCapacityBlock {
            kind: QueueCapacityLimitKind::SearchTick,
            route_logical_id: None,
            global_active: 0,
            global_limit: governor.caps.global,
            subscription_active: 0,
            subscription_limit: governor.caps.subscription,
            tick_active: 0,
            tick_limit: 0,
        });
        assert!(reason.contains("candidate searches this tick"));
    }

    #[tokio::test]
    async fn queue_governor_blocks_search_before_source_call_when_tick_cap_is_full() -> Result<()> {
        let database = setup_test_db().await?;
        let subscription = test_subscription();
        let mut governor = empty_queue_governor();
        governor.caps.max_candidate_searches_per_tick = 0;

        let block = governor
            .try_start_search(&database.pool, &subscription)
            .await?
            .expect_err("search tick cap");

        assert_eq!(block.kind, QueueCapacityLimitKind::SearchTick);
        assert_eq!(governor.searches_this_tick, 0);
        Ok(())
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
            idempotency_key: None,
            request_mode: AcquisitionRequestMode::Monitored,
            request_scope: AcquisitionRequestScope::Subscription,
            scope: None,
            metadata_policy: AcquisitionMetadataPolicy::Recurring,
            completion_policy: AcquisitionCompletionPolicy::Manual,
            monitor_policy: Default::default(),
            route_policy: AcquisitionRoutePolicy::DebridFirst,
            source_provider_id: None,
            release_delay_seconds: 30 * 60,
            quality_profile: None,
            metadata_refresh_after: Utc::now(),
            candidate_search_after: Utc::now(),
            last_metadata_refresh_at: None,
            last_candidate_search_at: None,
            tracking_started_at: None,
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
