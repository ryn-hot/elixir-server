use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
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
    acquisition::subscriptions::{
        AcquisitionRoutePolicy, AcquisitionSubscription, AcquisitionSubscriptionFilter,
        AcquisitionTarget, AcquisitionTargetState, AcquisitionTargetStateUpdate,
        NewAcquisitionTarget, list_due_candidate_targets, list_due_metadata_subscriptions,
        list_submitted_debrid_targets, list_subscriptions, record_metadata_refresh,
        update_subscription_external_ids, update_target_state, upsert_subscription_targets,
    },
    db::models::{MediaType, ProviderHealthState},
    debrid::get_debrid_job_status,
    download_broker::{DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID},
    extensions::{ExternalIds, store::ExtensionStore},
    http::{
        error::ApiError,
        handlers::{
            acquisition_sources::{
                ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY, AcquisitionCandidate,
                CandidateSearchPreferences, CandidateSearchRequest, CandidateSearchTarget,
                search_candidates_with_store,
            },
            download_broker::{DownloadBrokerSubmitRequest, submit_to_broker},
        },
    },
    library::{AniListSeasonChainEntry, AniZipMapping, resolve_anilist_season_chain},
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

#[derive(Debug, Clone)]
struct CandidateSubmission {
    provider_id: Uuid,
    source_extension_id: String,
    candidate: AcquisitionCandidate,
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

    for target in targets {
        let Some(subscription) = subscriptions.get(&target.subscription_id) else {
            continue;
        };
        if let Err(err) = search_and_submit_target(state, subscription, &target, now).await {
            let next_after = next_candidate_retry_after(subscription, &target, now);
            update_target_state(
                &state.db_pool,
                target.target_id,
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
                target_id = %target.target_id,
                subscription_id = %target.subscription_id,
                next_after = %next_after,
                "candidate automation failed: {err}"
            );
        }
    }
    Ok(())
}

async fn search_and_submit_target(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    now: DateTime<Utc>,
) -> Result<()> {
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

    let request = candidate_search_request(subscription, target);
    let response = search_candidates_with_store(&state.db_pool, request).await?;
    let Some(candidate) = select_best_candidate(&response.candidates, subscription.route_policy)
    else {
        let next_after = next_candidate_retry_after(subscription, target, now);
        update_target_state(
            &state.db_pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Pending,
                state_reason: Some("No matching acquisition candidates were found.".to_string()),
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
        candidate,
    };
    submit_selected_candidate(state, subscription, target, submission, None).await?;
    Ok(())
}

async fn retry_failed_debrid_targets_with_torrent(state: &AppState) -> Result<()> {
    let targets = list_submitted_debrid_targets(&state.db_pool, FALLBACK_BATCH_LIMIT).await?;
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
        };
        match submit_selected_candidate(
            state,
            subscription,
            &target,
            submission,
            Some(TORRENT_DEFAULT_LOGICAL_ID),
        )
        .await
        {
            Ok(()) => info!(
                target_id = %target.target_id,
                debrid_job_id = %status.job_id,
                "submitted torrent fallback after Real-Debrid failure"
            ),
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
) -> Result<()> {
    let route_logical_id = select_candidate_route(
        route_override,
        subscription.route_policy,
        &submission.candidate,
    )?;
    match submit_candidate_to_route(state, target, &submission, &route_logical_id).await {
        Ok(download_id) => {
            mark_target_submitted(
                state,
                target,
                &submission,
                &route_logical_id,
                download_id,
                if route_override == Some(TORRENT_DEFAULT_LOGICAL_ID) {
                    "Submitted through torrent fallback."
                } else {
                    "Submitted through acquisition route."
                },
            )
            .await?;
            Ok(())
        }
        Err(err)
            if route_logical_id == DEBRID_DEFAULT_LOGICAL_ID
                && subscription.route_policy == AcquisitionRoutePolicy::DebridFirst
                && candidate_supports_route(&submission.candidate, TORRENT_DEFAULT_LOGICAL_ID) =>
        {
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
                    return Ok(());
                }
            };
            mark_target_submitted(
                state,
                target,
                &submission,
                TORRENT_DEFAULT_LOGICAL_ID,
                torrent_download_id,
                "Real-Debrid rejected the candidate; submitted torrent fallback.",
            )
            .await?;
            Ok(())
        }
        Err(err) => {
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
            Ok(())
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

#[derive(Debug, Default)]
struct SubscriptionExpansion {
    targets: Vec<NewAcquisitionTarget>,
    target_count: usize,
    external_ids_changed: ExternalIds,
    season_chain: Vec<AniListSeasonChainEntry>,
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
    let mut targets = match subscription.media_type {
        MediaType::Movie => expand_movie_subscription(subscription, &ids, now),
        MediaType::Series => expand_series_subscription(state, subscription, &mut ids, now).await?,
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
) -> Result<Vec<NewAcquisitionTarget>> {
    let Some(seed_anilist_id) = ids.anilist.clone() else {
        return expand_series_subscription(state, subscription, ids, now).await;
    };

    let seed_mapping = state
        .linkers
        .fetch_anizip_mapping(&seed_anilist_id)
        .await
        .unwrap_or(None);
    if let Some(mapping) = seed_mapping.as_ref() {
        merge_external_ids(ids, &mapping.ids);
    }
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

    let mut targets_by_key: HashMap<String, NewAcquisitionTarget> = HashMap::new();
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
        if let Some(mapping) = mapping {
            merge_external_ids(ids, &mapping.ids);
            merge_anizip_targets(
                subscription,
                ids,
                season,
                &mapping,
                now,
                &mut targets_by_key,
            );
        } else if let (Some(next_episode), Some(next_airing_at)) =
            (season.next_airing_episode, season.next_airing_at)
        {
            let air_time = Utc.timestamp_opt(next_airing_at, 0).single();
            let target = NewAcquisitionTarget {
                target_key: Some(format!("A{next_episode:04}")),
                media_type: Some(MediaType::Anime),
                title: Some(season.title.clone()),
                season_number: Some(season.season_number),
                episode_number: None,
                absolute_episode_number: Some(next_episode),
                air_date: air_time.map(|value| value.date_naive().to_string()),
                air_time,
                metadata: Some(json!({
                    "source": "anilist",
                    "externalIds": ids,
                    "anilistSeasonId": season.anilist_id,
                    "anilistSeason": AniListSeasonChainEntryEnvelope::from(season),
                })),
                state: Some(AcquisitionTargetState::Pending),
                next_search_after: Some(next_search_after_for_air_time(
                    air_time,
                    subscription.release_delay_seconds,
                    now,
                )),
            };
            targets_by_key.insert(format!("A{next_episode:04}"), target);
        }
    }

    let mut targets = targets_by_key.into_values().collect::<Vec<_>>();
    targets.sort_by_key(|target| {
        (
            target.season_number.unwrap_or(i32::MAX),
            target.episode_number.unwrap_or(i32::MAX),
            target.absolute_episode_number.unwrap_or(i32::MAX),
        )
    });
    Ok(targets)
}

fn merge_anizip_targets(
    subscription: &AcquisitionSubscription,
    ids: &ExternalIds,
    season: &AniListSeasonChainEntry,
    mapping: &AniZipMapping,
    now: DateTime<Utc>,
    targets_by_key: &mut HashMap<String, NewAcquisitionTarget>,
) {
    for episode in &mapping.episodes {
        let Some(season_number) = episode.season_number.or(Some(season.season_number)) else {
            continue;
        };
        let Some(episode_number) = episode.episode_number else {
            continue;
        };
        if season_number <= 0 || episode_number <= 0 {
            continue;
        }
        let key = format!("S{season_number:02}E{episode_number:02}");
        let air_date = extract_air_date(&episode.raw);
        let air_time = air_date
            .as_deref()
            .and_then(|value| parse_air_time(value).or_else(|| parse_air_date(value)));
        targets_by_key
            .entry(key)
            .or_insert_with(|| NewAcquisitionTarget {
                target_key: None,
                media_type: Some(MediaType::Anime),
                title: episode.title.clone().or_else(|| {
                    Some(format!(
                        "{} S{season_number:02}E{episode_number:02}",
                        subscription.title
                    ))
                }),
                season_number: Some(season_number),
                episode_number: Some(episode_number),
                absolute_episode_number: episode.absolute_episode_number,
                air_date,
                air_time,
                metadata: Some(json!({
                    "source": "anizip",
                    "externalIds": ids,
                    "anilistSeasonId": season.anilist_id,
                    "anilistSeason": AniListSeasonChainEntryEnvelope::from(season),
                    "tvdbEpisodeId": episode.tvdb_id,
                    "anidbEpisodeId": episode.anidb_eid,
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

fn candidate_search_request(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
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

fn select_best_candidate(
    candidates: &[AcquisitionCandidate],
    route_policy: AcquisitionRoutePolicy,
) -> Option<AcquisitionCandidate> {
    candidates
        .iter()
        .filter(|candidate| candidate_allowed_by_policy(candidate, route_policy))
        .max_by(|left, right| compare_candidates(left, right, route_policy))
        .cloned()
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

fn infer_anizip_season_number(mapping: &AniZipMapping) -> Option<i32> {
    let mut counts: HashMap<i32, usize> = HashMap::new();
    for season in mapping
        .episodes
        .iter()
        .filter_map(|episode| episode.season_number)
        .filter(|season| *season > 0)
    {
        *counts.entry(season).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(season, _)| season)
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
            supported_routes: routes.into_iter().map(ToString::to_string).collect(),
            default_route: None,
            raw: None,
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
        let best = select_best_candidate(&[torrent, cached], AcquisitionRoutePolicy::DebridFirst)
            .expect("best candidate");
        assert_eq!(best.title, "cached");
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
