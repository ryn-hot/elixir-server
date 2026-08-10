use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{AnyPool, Row};
use uuid::Uuid;

use crate::{
    anime_matching::AnimeMatchingService,
    db::models::MediaType,
    extensions::{ExternalIds, FileDescriptor, MediaIdentity},
    state::AppState,
};

use super::{
    AggregatedCandidate, AggregatedFile, CanonicalEpisodeNumberMap, ClassificationDisposition,
    ClassificationOutcome, ClassifierPipeline, LinkerService, ResolvedEpisodeNumbers,
    SeasonAnilistSeed, anizip_mapping_for_scan, build_classifier_pipeline,
    classify_candidate_files, cleanup_orphan_series_stub, derive_override_key,
    infer_anizip_mapping_season, insert_anizip_episode_numbers, insert_season_anilist_seed,
    library_provisional_anilist_ids, load_persisted_episode_number_map,
    merge_authoritative_anizip_numbers, resolve_difficult_library_anime_files,
    resolve_episode_numbers,
};

pub const ANIME_LIBRARY_REPAIR_VERSION: i32 = 1;
const ANIME_LIBRARY_REPAIR_BATCH_SIZE: i64 = 32;
const ANIME_LIBRARY_REPAIR_CLAIM_MINUTES: i64 = 15;
const ANIME_LIBRARY_REPAIR_CLAIM_HEARTBEAT_SECONDS: u64 = 60;
// Keep repair-produced applied evidence on the same monotonic schema version
// as forward classification. Its canonical repair envelope is intentionally
// distinct, and its empty causal rows make clear that canonical
// `anime_match_repair` identities are not provisional classifier ownership.
const APPLIED_IDENTITY_EVIDENCE_VERSION: i32 = 2;

static REPAIR_ITERATION_ACTIVE: AtomicBool = AtomicBool::new(false);
static REPAIR_PROCESS_TOKEN: LazyLock<String> =
    LazyLock::new(|| format!("{}-{}", std::process::id(), Uuid::new_v4()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnimeLibraryRepairTrigger {
    Startup,
    LibraryScan,
    MetadataRefresh,
    ModelActivated,
    ProviderCorrection,
}

impl AnimeLibraryRepairTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::LibraryScan => "library_scan",
            Self::MetadataRefresh => "metadata_refresh",
            Self::ModelActivated => "model_activated",
            Self::ProviderCorrection => "provider_correction",
        }
    }

    fn force_metadata(self) -> bool {
        matches!(self, Self::MetadataRefresh)
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnimeLibraryRepairSnapshot {
    pub repair_version: i32,
    pub status: String,
    pub scanned_count: i64,
    pub claimed_count: i64,
    pub retryable_count: i64,
    pub completed_count: i64,
    pub protected_count: i64,
    pub repaired_link_count: i64,
    pub repaired_identity_count: i64,
    pub failure_count: i64,
    pub pending_files: i64,
    pub running_files: i64,
    pub retryable_files: i64,
    pub last_error: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RepairIterationCounts {
    scanned: i64,
    claimed: i64,
    retryable: i64,
    completed: i64,
    protected: i64,
    repaired_links: i64,
    repaired_identities: i64,
    failures: i64,
}

#[derive(Debug, Clone)]
struct RepairCandidate {
    media_file_id: Uuid,
    series_id: Uuid,
    path: String,
    title: String,
    year: Option<i32>,
    descriptor: FileDescriptor,
    reason: String,
    evidence_snapshot: Value,
}

#[derive(Debug, Clone)]
struct ClaimedRepair {
    candidate: RepairCandidate,
    claim_token: String,
}

#[derive(Debug, Clone)]
struct CanonicalIdentityEvidence {
    series_ids: ExternalIds,
    season_anilist_id: Option<String>,
    tvdb_episode_id: Option<String>,
    anidb_episode_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClassifierSeriesIdentityRow {
    provider: String,
    external_id: String,
    source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClassifierSeasonIdentityRow {
    season_number: i32,
    provider: String,
    external_id: String,
    source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClassifierEpisodeIdentityRow {
    episode_id: String,
    provider: String,
    external_id: String,
    source: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClassifierIdentityManifest {
    series_rows: BTreeSet<ClassifierSeriesIdentityRow>,
    season_rows: BTreeSet<ClassifierSeasonIdentityRow>,
    episode_rows: BTreeSet<ClassifierEpisodeIdentityRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    legacy_exclusive_owner: Option<LegacyExclusiveIdentityOwner>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyExclusiveIdentityOwner {
    media_file_id: String,
    series_id: String,
}

#[derive(Debug, Clone)]
struct CanonicalRepairPlan {
    target_series_id: Uuid,
    numbers: ResolvedEpisodeNumbers,
    classification: ClassificationOutcome,
    identity: CanonicalIdentityEvidence,
    classifier_identity: ClassifierIdentityManifest,
    assist_json: Option<String>,
    identity_evidence_json: String,
}

enum RepairPlanDecision {
    Apply(CanonicalRepairPlan),
    Retryable { assist_json: Option<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyRepairOutcome {
    Applied { links: i64, identities: i64 },
    Protected,
}

struct RepairIterationGuard;

impl Drop for RepairIterationGuard {
    fn drop(&mut self) {
        REPAIR_ITERATION_ACTIVE.store(false, Ordering::Release);
    }
}

struct RepairClaimHeartbeat {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl RepairClaimHeartbeat {
    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

pub async fn run_anime_library_repair_for_state(
    state: &AppState,
    trigger: AnimeLibraryRepairTrigger,
) -> Result<AnimeLibraryRepairSnapshot> {
    if REPAIR_ITERATION_ACTIVE
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return anime_library_repair_snapshot(&state.db_pool).await;
    }
    let _guard = RepairIterationGuard;
    let _identity_mutation_guard = super::LIBRARY_IDENTITY_MUTATION_LOCK.lock().await;
    let _database_identity_guard =
        super::acquire_library_identity_database_guard(&state.db_pool).await?;
    let classifier = build_classifier_pipeline(Some(&state.settings.classifier));
    let anilist_bridge = super::build_anilist_identifier(Some(&state.settings.classifier));
    let matching_service = state.anime_inference.matching_service();
    run_anime_library_repair_iteration_with_anilist(
        &state.db_pool,
        Some(&state.linkers),
        &classifier,
        &anilist_bridge,
        &matching_service,
        state.metadata.ttl_seconds(),
        trigger,
    )
    .await?;
    anime_library_repair_snapshot(&state.db_pool).await
}

async fn run_anime_library_repair_iteration(
    pool: &AnyPool,
    linkers: Option<&LinkerService>,
    classifier: &ClassifierPipeline,
    matching_service: &AnimeMatchingService,
    mapping_ttl_seconds: u64,
    trigger: AnimeLibraryRepairTrigger,
) -> Result<RepairIterationCounts> {
    let anilist_bridge = super::build_anilist_identifier(None);
    run_anime_library_repair_iteration_with_anilist(
        pool,
        linkers,
        classifier,
        &anilist_bridge,
        matching_service,
        mapping_ttl_seconds,
        trigger,
    )
    .await
}

async fn run_anime_library_repair_iteration_with_anilist(
    pool: &AnyPool,
    linkers: Option<&LinkerService>,
    classifier: &ClassifierPipeline,
    anilist_bridge: &elixir_classifier::identify::anilist::AniListIdentifier,
    matching_service: &AnimeMatchingService,
    mapping_ttl_seconds: u64,
    trigger: AnimeLibraryRepairTrigger,
) -> Result<RepairIterationCounts> {
    let run_token = format!("{}-run-{}", REPAIR_PROCESS_TOKEN.as_str(), Uuid::new_v4());
    if !claim_repair_run(pool, &run_token).await? {
        return Ok(RepairIterationCounts::default());
    }

    let result = run_claimed_repair_iteration(
        pool,
        linkers,
        classifier,
        anilist_bridge,
        matching_service,
        mapping_ttl_seconds,
        trigger,
        &run_token,
    )
    .await;
    match result {
        Ok(counts) => {
            finish_repair_run(pool, &run_token, &counts, None).await?;
            Ok(counts)
        }
        Err(error) => {
            let detail = bounded_repair_text(&format!("{error:#}"));
            let mut counts = RepairIterationCounts::default();
            counts.failures = 1;
            finish_repair_run(pool, &run_token, &counts, Some(&detail)).await?;
            Err(error)
        }
    }
}

async fn run_claimed_repair_iteration(
    pool: &AnyPool,
    linkers: Option<&LinkerService>,
    classifier: &ClassifierPipeline,
    anilist_bridge: &elixir_classifier::identify::anilist::AniListIdentifier,
    matching_service: &AnimeMatchingService,
    mapping_ttl_seconds: u64,
    trigger: AnimeLibraryRepairTrigger,
    run_token: &str,
) -> Result<RepairIterationCounts> {
    // Freeze the retry wave by promoting every row that was retryable when
    // this trigger began. Rows that fail during this pass return to
    // `retryable` and are therefore excluded from the same wave, while the
    // original set drains across as many bounded batches as necessary.
    prepare_retryable_wave(pool).await?;
    let mut counts = RepairIterationCounts::default();
    loop {
        renew_repair_run_claim(pool, run_token).await?;
        let candidates = discover_repair_candidates(pool).await?;
        if candidates.is_empty() {
            break;
        }
        counts.scanned = counts
            .scanned
            .saturating_add(i64::try_from(candidates.len()).unwrap_or(i64::MAX));
        persist_repair_run_progress(
            pool,
            run_token,
            &RepairIterationCounts {
                scanned: i64::try_from(candidates.len()).unwrap_or(i64::MAX),
                ..Default::default()
            },
        )
        .await?;

        for candidate in candidates {
            renew_repair_run_claim(pool, run_token).await?;
            enroll_repair_candidate(pool, &candidate).await?;
            if repair_candidate_is_protected(pool, &candidate).await? {
                mark_repair_protected_for_run(pool, candidate.media_file_id, Some(run_token))
                    .await?;
                counts.protected += 1;
                continue;
            }
            let Some(claimed) =
                claim_repair_candidate_for_run(pool, candidate, Some(run_token)).await?
            else {
                continue;
            };
            counts.claimed += 1;
            ensure!(
                renew_repair_candidate_claim(pool, &claimed).await?,
                "anime library repair claim changed before canonical planning"
            );
            let heartbeat = start_repair_claim_heartbeat(pool.clone(), &claimed, run_token);
            let decision = build_canonical_repair_plan(
                pool,
                linkers,
                classifier,
                anilist_bridge,
                matching_service,
                mapping_ttl_seconds,
                trigger.force_metadata(),
                &claimed,
            )
            .await;
            heartbeat.stop().await;
            if decision.is_ok() {
                ensure!(
                    renew_repair_candidate_claim(pool, &claimed).await?,
                    "anime library repair claim changed during canonical planning"
                );
            }
            match decision {
                Ok(RepairPlanDecision::Apply(plan)) => {
                    match apply_canonical_repair_plan_for_run(
                        pool,
                        &claimed,
                        &plan,
                        Some(run_token),
                    )
                    .await
                    {
                        Ok(ApplyRepairOutcome::Applied { links, identities }) => {
                            counts.completed += 1;
                            counts.repaired_links += links;
                            counts.repaired_identities += identities;
                        }
                        Ok(ApplyRepairOutcome::Protected) => {
                            counts.protected += 1;
                        }
                        Err(error) => {
                            counts.failures += 1;
                            counts.retryable += 1;
                            mark_repair_retryable_for_run(
                                pool,
                                &claimed,
                                Some(&bounded_repair_text(&format!("{error:#}"))),
                                plan.assist_json.as_deref(),
                                Some(run_token),
                            )
                            .await?;
                        }
                    }
                }
                Ok(RepairPlanDecision::Retryable { assist_json }) => {
                    counts.retryable += 1;
                    mark_repair_retryable_for_run(
                        pool,
                        &claimed,
                        None,
                        assist_json.as_deref(),
                        Some(run_token),
                    )
                    .await?;
                }
                Err(error) => {
                    counts.failures += 1;
                    counts.retryable += 1;
                    mark_repair_retryable_for_run(
                        pool,
                        &claimed,
                        Some(&bounded_repair_text(&format!("{error:#}"))),
                        None,
                        Some(run_token),
                    )
                    .await?;
                }
            }
        }
        tokio::task::yield_now().await;
    }
    tracing::info!(
        repair_version = ANIME_LIBRARY_REPAIR_VERSION,
        trigger = trigger.as_str(),
        scanned = counts.scanned,
        claimed = counts.claimed,
        completed = counts.completed,
        retryable = counts.retryable,
        protected = counts.protected,
        failures = counts.failures,
        "automatic anime library repair iteration completed"
    );
    Ok(counts)
}

async fn prepare_retryable_wave(pool: &AnyPool) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repairs SET status = 'pending', updated_at = CURRENT_TIMESTAMP \
         WHERE repair_version = $1 AND status = 'retryable'",
    )
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .execute(pool)
    .await?;
    Ok(())
}

async fn claim_repair_run(pool: &AnyPool, claim_token: &str) -> Result<bool> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO library_anime_repair_runs (repair_version, status, created_at, updated_at) \
         VALUES ($1, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(repair_version) DO NOTHING",
    )
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .execute(pool)
    .await?;
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repair_runs SET status = 'running', claim_token = $1, \
         claim_expires_at = $2, \
         started_at = CURRENT_TIMESTAMP, finished_at = NULL, last_error = NULL, \
         updated_at = CURRENT_TIMESTAMP \
         WHERE repair_version = $3 AND (status != 'running' \
           OR claim_expires_at IS NULL OR claim_expires_at <= $4 \
           OR claim_token LIKE $5)",
    )
    .bind(claim_token)
    .bind(repair_claim_expiry())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(repair_claim_now())
    .bind(format!("{}%", REPAIR_PROCESS_TOKEN.as_str()))
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() == 1)
}

async fn renew_repair_run_claim(pool: &AnyPool, claim_token: &str) -> Result<()> {
    let renewed = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repair_runs SET claim_expires_at = $1, \
         updated_at = CURRENT_TIMESTAMP WHERE repair_version = $2 \
         AND status = 'running' AND claim_token = $3",
    )
    .bind(repair_claim_expiry())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(claim_token)
    .execute(pool)
    .await?;
    ensure!(
        renewed.rows_affected() == 1,
        "anime library repair run ownership changed"
    );
    Ok(())
}

async fn persist_repair_run_progress(
    pool: &AnyPool,
    claim_token: &str,
    delta: &RepairIterationCounts,
) -> Result<()> {
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repair_runs SET \
         scanned_count = scanned_count + $1, claimed_count = claimed_count + $2, \
         retryable_count = retryable_count + $3, completed_count = completed_count + $4, \
         protected_count = protected_count + $5, \
         repaired_link_count = repaired_link_count + $6, \
         repaired_identity_count = repaired_identity_count + $7, \
         failure_count = failure_count + $8, updated_at = CURRENT_TIMESTAMP \
         WHERE repair_version = $9 AND status = 'running' AND claim_token = $10",
    )
    .bind(delta.scanned)
    .bind(delta.claimed)
    .bind(delta.retryable)
    .bind(delta.completed)
    .bind(delta.protected)
    .bind(delta.repaired_links)
    .bind(delta.repaired_identities)
    .bind(delta.failures)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(claim_token)
    .execute(pool)
    .await?;
    ensure!(
        updated.rows_affected() == 1,
        "anime library repair run ownership changed"
    );
    Ok(())
}

async fn persist_repair_run_progress_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    claim_token: &str,
    delta: &RepairIterationCounts,
) -> Result<()> {
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repair_runs SET \
         scanned_count = scanned_count + $1, claimed_count = claimed_count + $2, \
         retryable_count = retryable_count + $3, completed_count = completed_count + $4, \
         protected_count = protected_count + $5, \
         repaired_link_count = repaired_link_count + $6, \
         repaired_identity_count = repaired_identity_count + $7, \
         failure_count = failure_count + $8, updated_at = CURRENT_TIMESTAMP \
         WHERE repair_version = $9 AND status = 'running' AND claim_token = $10",
    )
    .bind(delta.scanned)
    .bind(delta.claimed)
    .bind(delta.retryable)
    .bind(delta.completed)
    .bind(delta.protected)
    .bind(delta.repaired_links)
    .bind(delta.repaired_identities)
    .bind(delta.failures)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(claim_token)
    .execute(&mut **transaction)
    .await?;
    ensure!(
        updated.rows_affected() == 1,
        "anime library repair run ownership changed"
    );
    Ok(())
}

async fn finish_repair_run(
    pool: &AnyPool,
    claim_token: &str,
    _counts: &RepairIterationCounts,
    error: Option<&str>,
) -> Result<()> {
    let status = if error.is_some() {
        "failed"
    } else {
        "completed"
    };
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repair_runs SET status = $1, claim_token = NULL, \
         claim_expires_at = NULL, \
         failure_count = failure_count + $2, last_error = $3, \
         finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP \
         WHERE repair_version = $4 AND status = 'running' AND claim_token = $5",
    )
    .bind(status)
    .bind(if error.is_some() { 1_i64 } else { 0_i64 })
    .bind(error)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(claim_token)
    .execute(pool)
    .await?;
    ensure!(
        updated.rows_affected() == 1,
        "anime library repair run ownership changed"
    );
    Ok(())
}

async fn discover_repair_candidates(pool: &AnyPool) -> Result<Vec<RepairCandidate>> {
    let rows = sqlx::query(
        "SELECT mf.id AS media_file_id, mf.media_item_id AS series_id, mf.path, mf.size_bytes, \
                mf.container, mf.video_codec, mf.audio_codec, mf.hash, \
                s.title, s.year, s.library_type, crs.disposition, crs.hint_json, \
                crs.candidates_json, crs.applied_identity_evidence_json, \
                lr.status AS repair_status, lr.evidence_snapshot_json AS enrolled_evidence_snapshot_json, \
                (SELECT COUNT(*) FROM episode_files ef WHERE ef.media_file_id = mf.id) AS link_count, \
                (SELECT COUNT(*) FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
                 WHERE ef.media_file_id = mf.id AND e.season_number = 1 AND e.episode_number = 1) AS s01e01_count, \
                (SELECT COUNT(*) FROM review_queue rq \
                 WHERE rq.media_file_id = mf.id AND rq.status = 'pending') AS pending_review_count \
         FROM media_files mf \
         JOIN series s ON s.id = mf.media_item_id \
         LEFT JOIN classifier_resolution_state crs ON crs.media_file_id = mf.id \
         LEFT JOIN library_anime_repairs lr ON lr.media_file_id = mf.id \
              AND lr.repair_version = $1 \
         WHERE mf.scan_state != 'missing' \
           AND (s.library_type = 'anime' OR s.external_anilist IS NOT NULL \
                OR EXISTS (SELECT 1 FROM season_external_ids sei JOIN seasons sx ON sx.id = sei.season_id \
                           WHERE sx.series_id = s.id AND sei.provider = 'anilist') \
                OR LOWER(COALESCE(crs.hint_json, '')) LIKE '%\"librarytype\":\"anime\"%' \
                OR LOWER(COALESCE(crs.candidates_json, '')) LIKE '%anilist%' \
                OR EXISTS (SELECT 1 FROM review_queue anime_rq \
                           WHERE anime_rq.media_file_id = mf.id AND anime_rq.status = 'pending' \
                             AND (LOWER(COALESCE(anime_rq.hint_json, '')) \
                                  LIKE '%\"librarytype\":\"anime\"%' \
                                  OR LOWER(COALESCE(anime_rq.candidates_json, '')) \
                                  LIKE '%anilist%'))) \
           AND (lr.status IS NULL OR lr.status = 'pending' \
                OR (lr.status = 'running' AND (lr.claim_expires_at IS NULL \
                    OR lr.claim_expires_at <= $4 OR lr.claim_token LIKE $3))) \
           AND (lr.status IS NOT NULL \
                OR (SELECT COUNT(*) FROM episode_files ef WHERE ef.media_file_id = mf.id) > 1 \
                OR (SELECT COUNT(*) FROM review_queue rq \
                    WHERE rq.media_file_id = mf.id AND rq.status = 'pending') > 0 \
                OR ((SELECT COUNT(*) FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
                     WHERE ef.media_file_id = mf.id AND e.season_number = 1 AND e.episode_number = 1) > 0 \
                    AND (crs.disposition IS NULL OR crs.disposition = 'unresolved')) \
                OR crs.disposition = 'unresolved') \
         ORDER BY CASE lr.status WHEN 'retryable' THEN 1 ELSE 0 END, \
                  lr.updated_at ASC, \
                  CASE \
                    WHEN (SELECT COUNT(*) FROM episode_files ef \
                          WHERE ef.media_file_id = mf.id) > 1 THEN 0 \
                    WHEN (SELECT COUNT(*) FROM episode_files ef JOIN episodes e \
                          ON e.id = ef.episode_id WHERE ef.media_file_id = mf.id \
                          AND e.season_number = 1 AND e.episode_number = 1) > 0 THEN 1 \
                    WHEN (SELECT COUNT(*) FROM review_queue rq \
                          WHERE rq.media_file_id = mf.id AND rq.status = 'pending') > 0 THEN 2 \
                    ELSE 3 END, mf.id LIMIT $2",
    )
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(ANIME_LIBRARY_REPAIR_BATCH_SIZE)
    .bind(format!("{}%", REPAIR_PROCESS_TOKEN.as_str()))
    .bind(repair_claim_now())
    .fetch_all(pool)
    .await?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let media_file_id = Uuid::parse_str(&row.try_get::<String, _>("media_file_id")?)?;
        let series_id = Uuid::parse_str(&row.try_get::<String, _>("series_id")?)?;
        let link_count = row.try_get::<i64, _>("link_count").unwrap_or(0);
        let s01e01_count = row.try_get::<i64, _>("s01e01_count").unwrap_or(0);
        let pending_review_count = row.try_get::<i64, _>("pending_review_count").unwrap_or(0);
        let disposition = row.try_get::<String, _>("disposition").ok();
        let repair_status = row.try_get::<String, _>("repair_status").ok();
        let mut reasons = Vec::new();
        if link_count > 1 {
            reasons.push("multiple_episode_links");
        }
        if s01e01_count > 0 && disposition.as_deref() != Some("applied") {
            reasons.push("silent_s01e01");
        }
        if pending_review_count > 0 {
            reasons.push("legacy_pending_classification");
        }
        if disposition.as_deref() == Some("unresolved") {
            reasons.push("unresolved_classification");
        }
        if reasons.is_empty() && repair_status.is_some() {
            reasons.push("interrupted_repair");
        }
        let path: String = row.try_get("path")?;
        // Once enrolled, the discovery evidence is immutable. In particular,
        // retrying a repair must not expand a legacy identity manifest from
        // whatever rows happen to exist at retry time.
        let evidence_snapshot = match row
            .try_get::<String, _>("enrolled_evidence_snapshot_json")
            .ok()
        {
            Some(enrolled) => serde_json::from_str::<Value>(&enrolled)?,
            None => {
                repair_evidence_snapshot(
                    pool,
                    media_file_id,
                    series_id,
                    &path,
                    row.try_get::<String, _>("hint_json").ok(),
                    row.try_get::<String, _>("candidates_json").ok(),
                    row.try_get::<String, _>("applied_identity_evidence_json")
                        .ok(),
                )
                .await?
            }
        };
        candidates.push(RepairCandidate {
            media_file_id,
            series_id,
            path: path.clone(),
            title: row.try_get("title")?,
            year: row.try_get::<i64, _>("year").ok().map(|year| year as i32),
            descriptor: FileDescriptor {
                path,
                size_bytes: row.try_get("size_bytes").ok(),
                hash: row.try_get("hash").ok(),
                container: row.try_get("container").ok(),
                video_codec: row.try_get("video_codec").ok(),
                audio_codec: row.try_get("audio_codec").ok(),
            },
            reason: reasons.join(","),
            evidence_snapshot,
        });
    }
    Ok(candidates)
}

async fn repair_evidence_snapshot(
    pool: &AnyPool,
    media_file_id: Uuid,
    series_id: Uuid,
    path: &str,
    hint_json: Option<String>,
    candidates_json: Option<String>,
    applied_identity_evidence_json: Option<String>,
) -> Result<Value> {
    let links = sqlx::query(
        "SELECT e.id, e.season_number, e.episode_number, e.absolute_episode_number \
         FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
         WHERE ef.media_file_id = $1 ORDER BY e.season_number, e.episode_number, e.id",
    )
    .bind(media_file_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "episodeId": row.try_get::<String, _>("id").ok(),
            "seasonNumber": row.try_get::<i64, _>("season_number").ok(),
            "episodeNumber": row.try_get::<i64, _>("episode_number").ok(),
            "absoluteEpisodeNumber": row.try_get::<i64, _>("absolute_episode_number").ok(),
        })
    })
    .collect::<Vec<_>>();
    let series_ids = sqlx::query(
        "SELECT provider, external_id, source FROM series_external_ids \
         WHERE series_id = $1 ORDER BY provider, external_id, source",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "provider": row.try_get::<String, _>("provider").ok(),
            "externalId": row.try_get::<String, _>("external_id").ok(),
            "source": row.try_get::<String, _>("source").ok(),
        })
    })
    .collect::<Vec<_>>();
    let direct_series_ids = sqlx::query(
        "SELECT external_imdb, external_tvdb_series, external_anilist \
         FROM series WHERE id = $1 LIMIT 1",
    )
    .bind(series_id.to_string())
    .fetch_one(pool)
    .await?;
    let season_ids = sqlx::query(
        "SELECT s.season_number, sei.provider, sei.external_id, sei.source \
         FROM season_external_ids sei JOIN seasons s ON s.id = sei.season_id \
         WHERE s.series_id = $1 ORDER BY s.season_number, sei.provider, sei.external_id",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "seasonNumber": row.try_get::<i64, _>("season_number").ok(),
            "provider": row.try_get::<String, _>("provider").ok(),
            "externalId": row.try_get::<String, _>("external_id").ok(),
            "source": row.try_get::<String, _>("source").ok(),
        })
    })
    .collect::<Vec<_>>();
    let episode_ids = sqlx::query(
        "SELECT e.id AS episode_id, e.season_number, e.episode_number, \
                eei.provider, eei.external_id, eei.source \
         FROM episode_external_ids eei JOIN episodes e ON e.id = eei.episode_id \
         WHERE e.series_id = $1 \
         ORDER BY e.season_number, e.episode_number, e.id, eei.provider, eei.external_id",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "episodeId": row.try_get::<String, _>("episode_id").ok(),
            "seasonNumber": row.try_get::<i64, _>("season_number").ok(),
            "episodeNumber": row.try_get::<i64, _>("episode_number").ok(),
            "provider": row.try_get::<String, _>("provider").ok(),
            "externalId": row.try_get::<String, _>("external_id").ok(),
            "source": row.try_get::<String, _>("source").ok(),
        })
    })
    .collect::<Vec<_>>();
    let pending_review = sqlx::query(
        "SELECT id, confidence, hint_json, candidates_json, \
                CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at \
         FROM review_queue WHERE media_file_id = $1 AND status = 'pending' \
         ORDER BY created_at, id",
    )
    .bind(media_file_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "id": row.try_get::<String, _>("id").ok(),
            "confidence": row.try_get::<f32, _>("confidence").ok(),
            "hint": row
                .try_get::<String, _>("hint_json")
                .ok()
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok()),
            "candidates": row
                .try_get::<String, _>("candidates_json")
                .ok()
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok()),
            "createdAt": row.try_get::<String, _>("created_at").ok(),
            "updatedAt": row.try_get::<String, _>("updated_at").ok(),
        })
    })
    .collect::<Vec<_>>();
    let legacy_exclusive_classifier_identity_evidence =
        legacy_exclusive_classifier_identity_evidence(
            pool,
            media_file_id,
            series_id,
            path,
            applied_identity_evidence_json.is_none(),
            &series_ids,
            &season_ids,
            &episode_ids,
        )
        .await?;
    let applied_identity_evidence = applied_identity_evidence_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    Ok(json!({
        "schemaVersion": 1,
        "mediaFileId": media_file_id,
        "seriesId": series_id,
        "path": path,
        "episodeLinks": links,
        "seriesExternalIds": series_ids,
        "seriesDirectIds": {
            "imdb": direct_series_ids.try_get::<String, _>("external_imdb").ok(),
            "tvdb": direct_series_ids.try_get::<String, _>("external_tvdb_series").ok(),
            "anilist": direct_series_ids.try_get::<String, _>("external_anilist").ok(),
        },
        "seasonExternalIds": season_ids,
        "episodeExternalIds": episode_ids,
        "legacyPendingReview": pending_review,
        "classifierHint": hint_json.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()),
        "classifierCandidates": candidates_json.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()),
        "appliedIdentityEvidence": applied_identity_evidence,
        "legacyExclusiveClassifierIdentityEvidence": legacy_exclusive_classifier_identity_evidence,
    }))
}

async fn legacy_exclusive_classifier_identity_evidence(
    pool: &AnyPool,
    media_file_id: Uuid,
    series_id: Uuid,
    path: &str,
    legacy_row: bool,
    series_rows: &[Value],
    season_rows: &[Value],
    episode_rows: &[Value],
) -> Result<Value> {
    if !legacy_row {
        return Ok(Value::Null);
    }

    let series_media_file_ids = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM media_files WHERE media_item_id = $1 ORDER BY id",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?;
    let episode_link_media_file_ids = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT DISTINCT ef.media_file_id FROM episode_files ef \
         JOIN episodes e ON e.id = ef.episode_id WHERE e.series_id = $1 \
         ORDER BY ef.media_file_id",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?;
    let expected_media_file_id = media_file_id.to_string();
    let structurally_exclusive = series_media_file_ids.len() == 1
        && series_media_file_ids.first() == Some(&expected_media_file_id)
        && episode_link_media_file_ids
            .iter()
            .all(|id| id == &expected_media_file_id);
    let unprotected_at_discovery =
        !repair_subject_is_protected(pool, media_file_id, series_id, path).await?;
    let eligible = structurally_exclusive && unprotected_at_discovery;
    let exact_classifier_rows = |rows: &[Value]| {
        if !eligible {
            return Vec::new();
        }
        rows.iter()
            .filter(|row| {
                row.get("source")
                    .and_then(Value::as_str)
                    .is_some_and(|source| matches!(source, "classifier" | "anilist_chain"))
            })
            .cloned()
            .collect::<Vec<_>>()
    };

    Ok(json!({
        "schemaVersion": 1,
        "mediaFileId": media_file_id,
        "seriesId": series_id,
        "structurallyExclusive": structurally_exclusive,
        "unprotectedAtDiscovery": unprotected_at_discovery,
        "seriesMediaFileIds": series_media_file_ids,
        "episodeLinkMediaFileIds": episode_link_media_file_ids,
        "causalIdentityRows": {
            "series": exact_classifier_rows(series_rows),
            "seasons": exact_classifier_rows(season_rows),
            "episodes": exact_classifier_rows(episode_rows),
        },
    }))
}

async fn enroll_repair_candidate(pool: &AnyPool, candidate: &RepairCandidate) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO library_anime_repairs \
         (media_file_id, repair_version, status, reason, evidence_snapshot_json, created_at, updated_at) \
         VALUES ($1, $2, 'pending', $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(media_file_id, repair_version) DO UPDATE SET \
         reason = excluded.reason, updated_at = CURRENT_TIMESTAMP \
         WHERE library_anime_repairs.status IN ('pending', 'running', 'retryable')",
    )
    .bind(candidate.media_file_id.to_string())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(&candidate.reason)
    .bind(serde_json::to_string(&candidate.evidence_snapshot)?)
    .execute(pool)
    .await?;
    Ok(())
}

fn classifier_identity_manifest(evidence: &Value) -> ClassifierIdentityManifest {
    fn provider_name(key: &str) -> Option<&'static str> {
        match key.to_ascii_lowercase().as_str() {
            "imdb" => Some("imdb"),
            "tmdb" => Some("tmdb"),
            "tvdb" | "tvdbseries" | "tvdb_series" => Some("tvdb"),
            "anilist" | "anilistid" | "anilist_id" => Some("anilist"),
            "anidb" | "anidbid" | "anidb_id" => Some("anidb"),
            "mal" | "malid" | "mal_id" => Some("mal"),
            "kitsu" | "kitsuid" | "kitsu_id" => Some("kitsu"),
            "tvdbepisode" | "tvdb_episode" => Some("tvdb_episode"),
            "anidbepisode" | "anidb_episode" => Some("anidb_episode"),
            _ => None,
        }
    }
    fn classifier_source(value: &str) -> bool {
        matches!(value, "classifier" | "anilist_chain")
    }
    fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
    fn validated_legacy_exact_rows(
        evidence: &Value,
    ) -> Option<(&Value, LegacyExclusiveIdentityOwner)> {
        if evidence.get("schemaVersion").and_then(Value::as_i64) != Some(1) {
            return None;
        }
        let root_media_file_id = string_field(evidence, "mediaFileId")?;
        let root_series_id = string_field(evidence, "seriesId")?;
        let legacy = evidence
            .get("legacyExclusiveClassifierIdentityEvidence")?
            .as_object()?;
        if legacy.get("schemaVersion").and_then(Value::as_i64) != Some(1)
            || legacy.get("structurallyExclusive").and_then(Value::as_bool) != Some(true)
            || legacy
                .get("unprotectedAtDiscovery")
                .and_then(Value::as_bool)
                != Some(true)
            || legacy.get("mediaFileId").and_then(Value::as_str) != Some(root_media_file_id)
            || legacy.get("seriesId").and_then(Value::as_str) != Some(root_series_id)
        {
            return None;
        }
        let series_media_file_ids = legacy.get("seriesMediaFileIds")?.as_array()?;
        if series_media_file_ids.len() != 1
            || series_media_file_ids[0].as_str() != Some(root_media_file_id)
        {
            return None;
        }
        let linked_media_file_ids = legacy.get("episodeLinkMediaFileIds")?.as_array()?;
        if !linked_media_file_ids
            .iter()
            .all(|value| value.as_str() == Some(root_media_file_id))
        {
            return None;
        }
        Some((
            legacy.get("causalIdentityRows")?,
            LegacyExclusiveIdentityOwner {
                media_file_id: root_media_file_id.to_string(),
                series_id: root_series_id.to_string(),
            },
        ))
    }

    let available_series = evidence
        .get("seriesExternalIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let provider = provider_name(string_field(row, "provider")?)?;
            let external_id = string_field(row, "externalId")?;
            let source = string_field(row, "source")?;
            classifier_source(source).then(|| ClassifierSeriesIdentityRow {
                provider: provider.to_string(),
                external_id: external_id.to_string(),
                source: source.to_string(),
            })
        })
        .collect::<BTreeSet<_>>();
    let available_seasons = evidence
        .get("seasonExternalIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let season_number = row
                .get("seasonNumber")
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value >= 0)?;
            let provider = provider_name(string_field(row, "provider")?)?;
            let external_id = string_field(row, "externalId")?;
            let source = string_field(row, "source")?;
            classifier_source(source).then(|| ClassifierSeasonIdentityRow {
                season_number,
                provider: provider.to_string(),
                external_id: external_id.to_string(),
                source: source.to_string(),
            })
        })
        .collect::<BTreeSet<_>>();
    let available_episodes = evidence
        .get("episodeExternalIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let episode_id = string_field(row, "episodeId")?;
            let provider = provider_name(string_field(row, "provider")?)?;
            let external_id = string_field(row, "externalId")?;
            let source = string_field(row, "source")?;
            classifier_source(source).then(|| ClassifierEpisodeIdentityRow {
                episode_id: episode_id.to_string(),
                provider: provider.to_string(),
                external_id: external_id.to_string(),
                source: source.to_string(),
            })
        })
        .collect::<BTreeSet<_>>();

    let applied_value = evidence.get("appliedIdentityEvidence");
    let applied = applied_value.filter(|value| value.is_object());
    let applied_schema_version = applied
        .and_then(|value| value.get("schemaVersion"))
        .and_then(Value::as_i64);
    let legacy_exact = if applied_value.is_none() || applied_value.is_some_and(Value::is_null) {
        validated_legacy_exact_rows(evidence)
    } else {
        None
    };
    let mut manifest = ClassifierIdentityManifest::default();

    // Schema v2 can name each row written because of this particular applied
    // classifier result. Never trust the envelope alone: intersect it with the
    // discovery snapshot so a stale or malformed manifest cannot delete a row
    // that did not exist when this file was enrolled.
    if let Some(exact) = applied
        .filter(|_| applied_schema_version == Some(2))
        .and_then(|value| value.get("causalIdentityRows"))
        .or_else(|| legacy_exact.as_ref().map(|(rows, _)| *rows))
    {
        for key in ["series", "seriesExternalIds"] {
            for row in exact
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(provider) = string_field(row, "provider").and_then(provider_name) else {
                    continue;
                };
                let Some(external_id) = string_field(row, "externalId") else {
                    continue;
                };
                let Some(source) = string_field(row, "source").filter(|v| classifier_source(v))
                else {
                    continue;
                };
                let exact_row = ClassifierSeriesIdentityRow {
                    provider: provider.to_string(),
                    external_id: external_id.to_string(),
                    source: source.to_string(),
                };
                if available_series.contains(&exact_row) {
                    manifest.series_rows.insert(exact_row);
                }
            }
        }
        for key in ["seasons", "seasonExternalIds"] {
            for row in exact
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(season_number) = row
                    .get("seasonNumber")
                    .and_then(Value::as_i64)
                    .and_then(|value| i32::try_from(value).ok())
                    .filter(|value| *value >= 0)
                else {
                    continue;
                };
                let Some(provider) = string_field(row, "provider").and_then(provider_name) else {
                    continue;
                };
                let Some(external_id) = string_field(row, "externalId") else {
                    continue;
                };
                let Some(source) = string_field(row, "source").filter(|v| classifier_source(v))
                else {
                    continue;
                };
                let exact_row = ClassifierSeasonIdentityRow {
                    season_number,
                    provider: provider.to_string(),
                    external_id: external_id.to_string(),
                    source: source.to_string(),
                };
                if available_seasons.contains(&exact_row) {
                    manifest.season_rows.insert(exact_row);
                }
            }
        }
        for key in ["episodes", "episodeExternalIds"] {
            for row in exact
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(episode_id) = string_field(row, "episodeId") else {
                    continue;
                };
                let Some(provider) = string_field(row, "provider").and_then(provider_name) else {
                    continue;
                };
                let Some(external_id) = string_field(row, "externalId") else {
                    continue;
                };
                let Some(source) = string_field(row, "source").filter(|v| classifier_source(v))
                else {
                    continue;
                };
                let exact_row = ClassifierEpisodeIdentityRow {
                    episode_id: episode_id.to_string(),
                    provider: provider.to_string(),
                    external_id: external_id.to_string(),
                    source: source.to_string(),
                };
                if available_episodes.contains(&exact_row) {
                    manifest.episode_rows.insert(exact_row);
                }
            }
        }
        if !manifest.series_rows.is_empty()
            || !manifest.season_rows.is_empty()
            || !manifest.episode_rows.is_empty()
        {
            if let Some((_, owner)) = legacy_exact {
                manifest.legacy_exclusive_owner = Some(owner);
            }
        }
        return manifest;
    }

    // A malformed or future evidence version must fail closed. In particular,
    // never reinterpret a schema-v2 envelope without exact causal rows through
    // the broader schema-v1 compatibility path below.
    if applied.is_some() && applied_schema_version != Some(1) {
        return manifest;
    }

    // Schema v1 named causal external IDs and accepted numbering, but not
    // physical rows. Intersect those claims with the exact captured rows. This
    // safely recovers series rows and the accepted relation-season row; it does
    // not guess that arbitrary classifier-sourced episode IDs were caused by
    // this file.
    let Some(applied) = applied else {
        return manifest;
    };
    let causal_ids = applied
        .get("causalExternalIds")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|ids| ids.iter())
        .filter_map(|(provider, value)| {
            Some((
                provider_name(provider)?.to_string(),
                value.as_str()?.trim().to_string(),
            ))
        })
        .filter(|(_, id)| !id.is_empty())
        .collect::<BTreeSet<_>>();
    manifest.series_rows.extend(
        available_series
            .into_iter()
            .filter(|row| causal_ids.contains(&(row.provider.clone(), row.external_id.clone()))),
    );
    if let Some(season) = applied
        .get("acceptedNumbers")
        .and_then(|value| value.get("seasonNumber"))
        .and_then(Value::as_i64)
        .and_then(|season| i32::try_from(season).ok())
        .filter(|season| *season >= 0)
    {
        manifest
            .season_rows
            .extend(available_seasons.into_iter().filter(|row| {
                row.season_number == season
                    && causal_ids.contains(&(row.provider.clone(), row.external_id.clone()))
            }));
    }
    manifest
}

async fn repair_candidate_is_protected(
    pool: &AnyPool,
    candidate: &RepairCandidate,
) -> Result<bool> {
    repair_subject_is_protected(
        pool,
        candidate.media_file_id,
        candidate.series_id,
        &candidate.path,
    )
    .await
}

async fn repair_subject_is_protected(
    pool: &AnyPool,
    media_file_id: Uuid,
    series_id: Uuid,
    path: &str,
) -> Result<bool> {
    let protected: Option<i64> = sqlx::query_scalar(
        "SELECT 1 WHERE \
         EXISTS (SELECT 1 FROM acquisition_import_file_links \
                 WHERE media_file_id = $1 AND state = 'imported') \
         OR EXISTS (SELECT 1 FROM managed_library_provenance WHERE media_item_id = $2) \
         OR EXISTS (SELECT 1 FROM media_ownerships \
                    WHERE media_item_id = $2 AND active = 1 \
                      AND owner_type IN ('acquisition', 'extension')) \
         LIMIT 1",
    )
    .bind(media_file_id.to_string())
    .bind(series_id.to_string())
    .fetch_optional(pool)
    .await?;
    if protected.is_some() {
        return Ok(true);
    }
    // A movie override is just as authoritative as an anime/series override.
    // Historical repair is explicitly correcting cross-type mistakes, so the
    // currently attached series type cannot safely narrow this lookup.
    for library_type in ["anime", "series", "movie"] {
        let Some(key) = derive_override_key(library_type, path) else {
            continue;
        };
        let override_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM classifier_overrides \
             WHERE library_type = $1 AND normalized_key = $2 LIMIT 1",
        )
        .bind(library_type)
        .bind(key)
        .fetch_optional(pool)
        .await?;
        if override_exists.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn mark_repair_protected_for_run(
    pool: &AnyPool,
    media_file_id: Uuid,
    run_token: Option<&str>,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repairs SET status = 'protected', claim_token = NULL, \
         claim_expires_at = NULL, \
         claimed_at = NULL, completed_at = CURRENT_TIMESTAMP, last_error = NULL, \
         updated_at = CURRENT_TIMESTAMP \
         WHERE media_file_id = $1 AND repair_version = $2 \
           AND status IN ('pending', 'running', 'retryable')",
    )
    .bind(media_file_id.to_string())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .execute(&mut *transaction)
    .await?;
    ensure!(
        updated.rows_affected() == 1,
        "anime library repair protection state changed"
    );
    if let Some(run_token) = run_token {
        persist_repair_run_progress_in_transaction(
            &mut transaction,
            run_token,
            &RepairIterationCounts {
                protected: 1,
                ..Default::default()
            },
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn claim_repair_candidate(
    pool: &AnyPool,
    candidate: RepairCandidate,
) -> Result<Option<ClaimedRepair>> {
    claim_repair_candidate_for_run(pool, candidate, None).await
}

async fn claim_repair_candidate_for_run(
    pool: &AnyPool,
    candidate: RepairCandidate,
    run_token: Option<&str>,
) -> Result<Option<ClaimedRepair>> {
    let claim_token = format!("{}-file-{}", REPAIR_PROCESS_TOKEN.as_str(), Uuid::new_v4());
    let mut transaction = pool.begin().await?;
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repairs SET status = 'running', claim_token = $1, \
         claim_expires_at = $2, attempt_count = attempt_count + 1, claimed_at = CURRENT_TIMESTAMP, \
         completed_at = NULL, last_error = NULL, updated_at = CURRENT_TIMESTAMP \
         WHERE media_file_id = $3 AND repair_version = $4 \
           AND (status IN ('pending', 'retryable') \
                OR (status = 'running' AND (claim_expires_at IS NULL \
                    OR claim_expires_at <= $6 OR claim_token LIKE $5)))",
    )
    .bind(&claim_token)
    .bind(repair_claim_expiry())
    .bind(candidate.media_file_id.to_string())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(format!("{}%", REPAIR_PROCESS_TOKEN.as_str()))
    .bind(repair_claim_now())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 0 {
        transaction.rollback().await?;
        return Ok(None);
    }
    if let Some(run_token) = run_token {
        persist_repair_run_progress_in_transaction(
            &mut transaction,
            run_token,
            &RepairIterationCounts {
                claimed: 1,
                ..Default::default()
            },
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(Some(ClaimedRepair {
        candidate,
        claim_token,
    }))
}

fn bounded_repair_text(value: &str) -> String {
    value.chars().take(2_000).collect()
}

fn repair_claim_now() -> i64 {
    Utc::now().timestamp()
}

fn repair_claim_expiry() -> i64 {
    repair_claim_now().saturating_add(ANIME_LIBRARY_REPAIR_CLAIM_MINUTES.saturating_mul(60))
}

async fn renew_repair_candidate_claim(pool: &AnyPool, claimed: &ClaimedRepair) -> Result<bool> {
    renew_repair_candidate_claim_by_owner(
        pool,
        claimed.candidate.media_file_id,
        &claimed.claim_token,
    )
    .await
}

async fn renew_repair_candidate_claim_by_owner(
    pool: &AnyPool,
    media_file_id: Uuid,
    claim_token: &str,
) -> Result<bool> {
    let renewed = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repairs SET claim_expires_at = $1, updated_at = CURRENT_TIMESTAMP \
         WHERE media_file_id = $2 AND repair_version = $3 \
           AND status = 'running' AND claim_token = $4",
    )
    .bind(repair_claim_expiry())
    .bind(media_file_id.to_string())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(claim_token)
    .execute(pool)
    .await?;
    Ok(renewed.rows_affected() == 1)
}

fn start_repair_claim_heartbeat(
    pool: AnyPool,
    claimed: &ClaimedRepair,
    run_claim_token: &str,
) -> RepairClaimHeartbeat {
    start_repair_claim_heartbeat_with_interval(
        pool,
        claimed,
        run_claim_token,
        Duration::from_secs(ANIME_LIBRARY_REPAIR_CLAIM_HEARTBEAT_SECONDS),
    )
}

fn start_repair_claim_heartbeat_with_interval(
    pool: AnyPool,
    claimed: &ClaimedRepair,
    run_claim_token: &str,
    heartbeat_interval: Duration,
) -> RepairClaimHeartbeat {
    let media_file_id = claimed.candidate.media_file_id;
    let file_claim_token = claimed.claim_token.clone();
    let run_claim_token = run_claim_token.to_string();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(heartbeat_interval);
        // `interval` ticks immediately once. The worker renews synchronously
        // before starting this task, so consume that tick and wait for the
        // first real heartbeat interval.
        interval.tick().await;
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                _ = interval.tick() => {
                    if let Err(error) = renew_repair_run_claim(&pool, &run_claim_token).await {
                        tracing::warn!(
                            media_file_id = %media_file_id,
                            error = %error,
                            "anime repair run claim changed while canonical planning was active"
                        );
                        break;
                    }
                    match renew_repair_candidate_claim_by_owner(
                        &pool,
                        media_file_id,
                        &file_claim_token,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            tracing::warn!(
                                media_file_id = %media_file_id,
                                "anime repair file claim changed while canonical planning was active"
                            );
                            break;
                        }
                        Err(error) => {
                            tracing::warn!(
                                media_file_id = %media_file_id,
                                error = %error,
                                "anime repair file claim heartbeat failed"
                            );
                        }
                    }
                }
            }
        }
    });
    RepairClaimHeartbeat {
        shutdown: Some(shutdown_tx),
        task,
    }
}

async fn build_canonical_repair_plan(
    pool: &AnyPool,
    linkers: Option<&LinkerService>,
    classifier: &ClassifierPipeline,
    anilist_bridge: &elixir_classifier::identify::anilist::AniListIdentifier,
    matching_service: &AnimeMatchingService,
    mapping_ttl_seconds: u64,
    force_metadata: bool,
    claimed: &ClaimedRepair,
) -> Result<RepairPlanDecision> {
    let classifier_identity = classifier_identity_manifest(&claimed.candidate.evidence_snapshot);
    let trusted_ids = load_trusted_series_ids(pool, claimed.candidate.series_id).await?;
    let candidate = AggregatedCandidate {
        identity: MediaIdentity {
            r#type: MediaType::Anime,
            external_ids: trusted_ids.clone(),
            title: claimed.candidate.title.clone(),
            year: claimed.candidate.year,
            season: None,
            episode: None,
        },
        files: vec![AggregatedFile {
            descriptor: claimed.candidate.descriptor.clone(),
            source_config_id: None,
            extension_metadata: HashMap::new(),
            // Historical links are evidence to replace, not parser inputs.
            // Feeding their old numbers back into classification would
            // self-authenticate a silent S01E01 default.
            season: None,
            episode: None,
            absolute_episode: None,
        }],
    };
    let (mut corrected_ids, mut outcomes, _, tvdb_seeds, mut season_seeds) =
        classify_candidate_files(
            pool,
            classifier,
            &candidate,
            &trusted_ids,
            true,
            false,
            true,
        )
        .await?;
    let trusted_season_ids =
        load_trusted_season_anilist_ids(pool, claimed.candidate.series_id).await?;
    for (season_number, anilist_id) in &trusted_season_ids {
        insert_season_anilist_seed(
            &mut season_seeds,
            *season_number,
            SeasonAnilistSeed {
                anilist_id: anilist_id.clone(),
                confidence: 1.0,
                causal_paths: BTreeSet::new(),
            },
        );
    }
    let expanded_chain = build_repair_relation_context(
        linkers,
        anilist_bridge,
        &candidate,
        &tvdb_seeds,
        &mut corrected_ids,
        &mut outcomes,
        &mut season_seeds,
    )
    .await?;
    let mut classification = outcomes
        .get(&claimed.candidate.path)
        .cloned()
        .unwrap_or_else(unresolved_repair_classification);

    let mut mapping_targets = BTreeSet::new();
    if let Some(anilist_id) = corrected_ids.anilist.as_deref() {
        let id = anilist_id.trim();
        if !id.is_empty() {
            mapping_targets.insert(id.to_string());
        }
    }
    mapping_targets.extend(library_provisional_anilist_ids(&outcomes));
    mapping_targets
        .extend(load_legacy_provisional_anilist_ids(pool, claimed.candidate.series_id).await?);
    for (_, anilist_id) in trusted_season_ids {
        mapping_targets.insert(anilist_id.clone());
    }
    mapping_targets.extend(
        season_seeds
            .values()
            .map(|seed| seed.anilist_id.trim().to_string())
            .filter(|id| !id.is_empty()),
    );
    mapping_targets.extend(
        expanded_chain
            .iter()
            .map(|season| season.anilist_id.trim().to_string())
            .filter(|id| !id.is_empty()),
    );

    let mut scan_cache = HashMap::new();
    let mut mappings_by_anilist_id = HashMap::<String, Arc<super::AniZipMapping>>::new();
    let mut current_number_map = CanonicalEpisodeNumberMap::new();
    for anilist_id in mapping_targets {
        if let Some(mapping) = anizip_mapping_for_scan(
            pool,
            linkers,
            &anilist_id,
            mapping_ttl_seconds,
            force_metadata,
            &mut scan_cache,
        )
        .await?
        {
            insert_anizip_episode_numbers(&mut current_number_map, &mapping);
            if let Some(season_number) = infer_anizip_mapping_season(&mapping) {
                let mapped_id = mapping
                    .ids
                    .anilist
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(anilist_id.as_str())
                    .to_string();
                insert_season_anilist_seed(
                    &mut season_seeds,
                    season_number,
                    SeasonAnilistSeed {
                        anilist_id: mapped_id,
                        confidence: 0.5,
                        causal_paths: BTreeSet::new(),
                    },
                );
            }
            mappings_by_anilist_id.insert(anilist_id, Arc::new(mapping));
        }
    }
    let persisted_number_map =
        load_persisted_episode_number_map(pool, claimed.candidate.series_id).await?;
    let number_map = merge_authoritative_anizip_numbers(persisted_number_map, current_number_map);
    let deterministic = resolve_episode_numbers(
        &candidate.files[0],
        Some(&classification),
        MediaType::Anime,
        &number_map,
    );
    let mut resolved_numbers = HashMap::from([(claimed.candidate.path.clone(), deterministic)]);

    if deterministic.season.is_none() || deterministic.episode.is_none() {
        resolve_difficult_library_anime_files(
            matching_service,
            &candidate,
            &expanded_chain,
            &mut season_seeds,
            &mappings_by_anilist_id,
            &mut corrected_ids,
            &mut resolved_numbers,
            &mut outcomes,
        )
        .await;
        classification = outcomes
            .get(&claimed.candidate.path)
            .cloned()
            .unwrap_or_else(unresolved_repair_classification);
    }

    let resolved = resolved_numbers
        .get(&claimed.candidate.path)
        .copied()
        .unwrap_or(deterministic);
    let (Some(season_number), Some(episode_number)) = (resolved.season, resolved.episode) else {
        let assist_json = classification
            .candidates_json
            .as_deref()
            .and_then(extract_anime_match_assist_json);
        return Ok(RepairPlanDecision::Retryable { assist_json });
    };
    ensure!(
        season_number >= 0 && episode_number > 0,
        "repair produced invalid canonical episode numbering"
    );

    let mapping_identity = canonical_mapping_identity(&mappings_by_anilist_id, resolved)?;
    let series_ids = canonical_repair_series_ids(
        &trusted_ids,
        &corrected_ids,
        &expanded_chain,
        &season_seeds,
        &mappings_by_anilist_id,
    );
    let identity = CanonicalIdentityEvidence {
        series_ids,
        season_anilist_id: mapping_identity
            .as_ref()
            .and_then(|identity| identity.season_anilist_id.clone())
            .or_else(|| {
                season_seeds
                    .get(&season_number)
                    .map(|seed| seed.anilist_id.clone())
                    .filter(|id| !id.trim().is_empty())
            }),
        tvdb_episode_id: mapping_identity
            .as_ref()
            .and_then(|identity| identity.tvdb_episode_id.clone()),
        anidb_episode_id: mapping_identity
            .as_ref()
            .and_then(|identity| identity.anidb_episode_id.clone()),
    };
    let target_series_id =
        canonical_series_owner(pool, claimed.candidate.series_id, &identity.series_ids).await?;
    classification.disposition = ClassificationDisposition::Applied;
    classification.accepted_numbers = Some(resolved);
    classification.retry_supersedes_applied = true;
    classification.preserve_authoritative_episode_links = false;
    let assist_json = classification
        .candidates_json
        .as_deref()
        .and_then(extract_anime_match_assist_json);
    let identity_evidence_json = serde_json::to_string(&json!({
        "schemaVersion": APPLIED_IDENTITY_EVIDENCE_VERSION,
        "origin": if assist_json.is_some() { "anime_match_repair" } else { "deterministic_repair" },
        "envelope": "canonical_repair",
        "acceptedNumbers": {
            "seasonNumber": season_number,
            "episodeNumber": episode_number,
            "absoluteEpisodeNumber": resolved.absolute_episode,
        },
        "canonicalIdentity": {
            "targetSeriesId": target_series_id,
            "seriesExternalIds": identity.series_ids,
            "season": {
                "seasonNumber": season_number,
                "anilistId": identity.season_anilist_id,
            },
            "episode": {
                "seasonNumber": season_number,
                "episodeNumber": episode_number,
                "absoluteEpisodeNumber": resolved.absolute_episode,
                "tvdbEpisodeId": identity.tvdb_episode_id,
                "anidbEpisodeId": identity.anidb_episode_id,
            },
        },
        "causalIdentityRows": {
            "series": [],
            "seasons": [],
            "episodes": [],
        },
        "replacedClassifierEvidence": classifier_identity,
    }))?;
    Ok(RepairPlanDecision::Apply(CanonicalRepairPlan {
        target_series_id,
        numbers: resolved,
        classification,
        identity,
        classifier_identity,
        assist_json,
        identity_evidence_json,
    }))
}

async fn build_repair_relation_context(
    linkers: Option<&LinkerService>,
    anilist_bridge: &elixir_classifier::identify::anilist::AniListIdentifier,
    candidate: &AggregatedCandidate,
    tvdb_seeds: &HashMap<i32, super::TvdbBridgeSeed>,
    corrected_ids: &mut ExternalIds,
    outcomes: &mut HashMap<String, ClassificationOutcome>,
    season_seeds: &mut HashMap<i32, SeasonAnilistSeed>,
) -> Result<Vec<super::AniListSeasonChainEntry>> {
    if let Some(linker) = linkers {
        if corrected_ids.tvdb_series.is_none()
            && let Some(imdb) = corrected_ids.imdb.as_ref()
            && let Ok(Some(tvdb_id)) = linker.link_tvdb_series_by_imdb(imdb).await
        {
            corrected_ids.tvdb_series = Some(tvdb_id);
        }

        if corrected_ids.anilist.is_none()
            && !tvdb_seeds.is_empty()
            && let Some(tvdb_id) = corrected_ids
                .tvdb_series
                .as_ref()
                .or(corrected_ids.tvdb.as_ref())
                .cloned()
        {
            let series_meta_result = linker.fetch_tvdb_series(&tvdb_id).await;
            let prerequisite_failure = match &series_meta_result {
                Ok(None) => Some("TVDB series metadata was unavailable".to_string()),
                Err(error) => Some(format!("TVDB series metadata lookup failed: {error}")),
                Ok(Some(_)) => None,
            };
            if let Some(failure) = prerequisite_failure {
                super::mark_tvdb_anime_bridge_prerequisite_unresolved(
                    outcomes, tvdb_seeds, &failure,
                )?;
            }

            if let Ok(Some(series_meta)) = series_meta_result
                && super::tvdb_indicates_anime(&series_meta)
            {
                let mut seeds = tvdb_seeds.values().cloned().collect::<Vec<_>>();
                seeds.sort_by(|left, right| {
                    right
                        .confidence
                        .total_cmp(&left.confidence)
                        .then_with(|| left.season_number.cmp(&right.season_number))
                });
                let mut season_years = HashMap::<i32, i32>::new();
                if let Ok(seasons) = linker.fetch_tvdb_series_seasons(&tvdb_id).await {
                    for season_meta in seasons {
                        let Some(season_number) = super::extract_tvdb_season_number(&season_meta)
                        else {
                            continue;
                        };
                        if let Some(year) = super::extract_tvdb_season_year(&season_meta) {
                            season_years.insert(season_number, year);
                        }
                    }
                }
                let scorer = super::DefaultScorer::default();
                for seed in seeds {
                    let bridged = super::apply_tvdb_anime_bridge(
                        &series_meta,
                        anilist_bridge,
                        &scorer,
                        corrected_ids,
                        outcomes,
                        &seed,
                        season_years.get(&seed.season_number).copied(),
                    )
                    .await?;
                    for (season_number, seed) in bridged.season_anilist_ids {
                        insert_season_anilist_seed(season_seeds, season_number, seed);
                    }
                }
            }
        }
    }

    let strongest_seed = season_seeds
        .iter()
        .filter(|(_, seed)| super::season_anilist_seed_is_usable(seed))
        .min_by(|left, right| {
            right
                .1
                .confidence
                .total_cmp(&left.1.confidence)
                .then_with(|| left.0.cmp(right.0))
                .then_with(|| left.1.anilist_id.trim().cmp(right.1.anilist_id.trim()))
        })
        .map(|(season_number, seed)| (*season_number, seed.clone()));
    let expanded = if let Some((seed_season, seed)) = strongest_seed {
        match super::expand_anilist_season_chain(anilist_bridge, seed_season, &seed).await {
            Ok(expanded) => expanded,
            Err(error) => {
                tracing::warn!(
                    media_file = %candidate.files[0].descriptor.path,
                    anilist_id = %seed.anilist_id,
                    seed_season,
                    error = %error,
                    "historical anime repair relation-chain expansion failed; retaining bounded known context"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    super::apply_anilist_relation_chain_seeds(season_seeds, &expanded, &BTreeSet::new());
    if let Some(root_id) = super::select_root_anilist_id(&expanded, season_seeds) {
        corrected_ids.anilist = Some(root_id);
    }
    Ok(expanded)
}

fn canonical_repair_series_ids(
    trusted_ids: &ExternalIds,
    corrected_ids: &ExternalIds,
    expanded_chain: &[super::AniListSeasonChainEntry],
    season_seeds: &HashMap<i32, SeasonAnilistSeed>,
    mappings: &HashMap<String, Arc<super::AniZipMapping>>,
) -> ExternalIds {
    let expanded_root = expanded_chain
        .iter()
        .min_by_key(|entry| entry.season_number)
        .map(|entry| entry.anilist_id.trim().to_string())
        .filter(|id| !id.is_empty());
    // A selected episode mapping proves the season identity, not the franchise
    // root. Promote AniList to series scope only when the relation graph found
    // the root or when independent context identifies season one. A season-2
    // mapping remains available below for season/episode identity without
    // becoming a permanent series owner.
    let root_anilist_id = expanded_root.or_else(|| {
        season_seeds
            .get(&1)
            .filter(|seed| super::season_anilist_seed_is_usable(seed))
            .map(|seed| seed.anilist_id.trim().to_string())
    });

    let root_mapping_ids = root_anilist_id.as_deref().and_then(|root_id| {
        mappings
            .get(root_id)
            .or_else(|| {
                mappings.values().find(|mapping| {
                    mapping
                        .ids
                        .anilist
                        .as_deref()
                        .is_some_and(|id| id.trim().eq_ignore_ascii_case(root_id))
                })
            })
            .map(|mapping| mapping.ids.clone())
    });
    let validated_ids = root_mapping_ids;
    // The selected canonical mapping outranks provisional classifier fields,
    // while genuinely trusted historical provider IDs retain their authority.
    let mapped_first = validated_ids
        .as_ref()
        .map(|ids| super::merge_external_ids(ids, Some(corrected_ids.clone())))
        .unwrap_or_else(|| corrected_ids.clone());
    let mut series_ids = super::merge_external_ids(trusted_ids, Some(mapped_first));
    if trusted_ids.anilist.is_none() {
        series_ids.anilist = root_anilist_id;
    }
    super::strip_anime_ids(&series_ids)
}

fn unresolved_repair_classification() -> ClassificationOutcome {
    ClassificationOutcome {
        disposition: ClassificationDisposition::Unresolved,
        confidence: None,
        hint_json: None,
        candidates_json: None,
        season_scope: None,
        retry_supersedes_applied: true,
        bridge_protected: false,
        parsed_hint: None,
        accepted_numbers: None,
        preserve_authoritative_episode_links: false,
        applied_identity_rows: Default::default(),
    }
}

async fn load_trusted_series_ids(pool: &AnyPool, series_id: Uuid) -> Result<ExternalIds> {
    let row = sqlx::query(
        "SELECT external_imdb, external_tvdb_series, external_anilist FROM series WHERE id = $1",
    )
    .bind(series_id.to_string())
    .fetch_one(pool)
    .await?;
    let mut ids = ExternalIds::default();
    for (provider, external_id) in sqlx::query_as::<_, (String, String)>(
        "SELECT provider, external_id FROM series_external_ids \
         WHERE series_id = $1 AND COALESCE(source, '') NOT IN ('classifier', 'anilist_chain') \
         ORDER BY provider, created_at DESC, external_id",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?
    {
        set_external_id_if_empty(&mut ids, &provider, external_id);
    }
    for (provider, direct) in [
        ("imdb", row.try_get::<String, _>("external_imdb").ok()),
        (
            "tvdb",
            row.try_get::<String, _>("external_tvdb_series").ok(),
        ),
        ("anilist", row.try_get::<String, _>("external_anilist").ok()),
    ] {
        let Some(direct) = direct
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let classifier_only: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM series_external_ids WHERE series_id = $1 AND provider = $2 \
             AND external_id = $3 AND source IN ('classifier', 'anilist_chain') LIMIT 1",
        )
        .bind(series_id.to_string())
        .bind(provider)
        .bind(&direct)
        .fetch_optional(pool)
        .await?;
        if classifier_only.is_none() {
            set_external_id_if_empty(&mut ids, provider, direct);
        }
    }
    Ok(ids)
}

fn set_external_id_if_empty(ids: &mut ExternalIds, provider: &str, value: String) {
    match provider {
        "imdb" if ids.imdb.is_none() => ids.imdb = Some(value),
        "tmdb" if ids.tmdb.is_none() => ids.tmdb = Some(value),
        "tvdb" if ids.tvdb_series.is_none() => ids.tvdb_series = Some(value),
        "anilist" if ids.anilist.is_none() => ids.anilist = Some(value),
        "anidb" if ids.anidb.is_none() => ids.anidb = Some(value),
        "mal" if ids.mal.is_none() => ids.mal = Some(value),
        "kitsu" if ids.kitsu.is_none() => ids.kitsu = Some(value),
        _ => {}
    }
}

async fn load_trusted_season_anilist_ids(
    pool: &AnyPool,
    series_id: Uuid,
) -> Result<Vec<(i32, String)>> {
    Ok(sqlx::query(
        "SELECT s.season_number, sei.external_id FROM season_external_ids sei \
         JOIN seasons s ON s.id = sei.season_id \
         WHERE s.series_id = $1 AND sei.provider = 'anilist' \
           AND COALESCE(sei.source, '') NOT IN ('classifier', 'anilist_chain') \
         ORDER BY s.season_number, sei.created_at DESC, sei.external_id",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .filter_map(|row| {
        Some((
            row.try_get::<i64, _>("season_number").ok()? as i32,
            row.try_get::<String, _>("external_id").ok()?,
        ))
    })
    .collect())
}

async fn load_legacy_provisional_anilist_ids(
    pool: &AnyPool,
    series_id: Uuid,
) -> Result<BTreeSet<String>> {
    let mut ids = sqlx::query_scalar::<_, String>(
        "SELECT external_id FROM series_external_ids WHERE series_id = $1 \
         AND provider = 'anilist' AND source IN ('classifier', 'anilist_chain') \
         UNION SELECT sei.external_id FROM season_external_ids sei \
         JOIN seasons s ON s.id = sei.season_id WHERE s.series_id = $1 \
         AND sei.provider = 'anilist' AND sei.source IN ('classifier', 'anilist_chain')",
    )
    .bind(series_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|id| id.trim().to_string())
    .filter(|id| !id.is_empty())
    .collect::<BTreeSet<_>>();
    if let Some(direct) = sqlx::query_scalar::<_, String>(
        "SELECT external_anilist FROM series WHERE id = $1 AND external_anilist IS NOT NULL",
    )
    .bind(series_id.to_string())
    .fetch_optional(pool)
    .await?
    {
        if !direct.trim().is_empty() {
            ids.insert(direct.trim().to_string());
        }
    }
    Ok(ids.into_iter().take(8).collect())
}

fn canonical_mapping_identity(
    mappings: &HashMap<String, Arc<super::AniZipMapping>>,
    numbers: ResolvedEpisodeNumbers,
) -> Result<Option<CanonicalIdentityEvidence>> {
    let mut identities = BTreeMap::<String, CanonicalIdentityEvidence>::new();
    for mapping in mappings.values() {
        for episode in &mapping.episodes {
            if episode.season_number != numbers.season
                || episode.episode_number != numbers.episode
                || (numbers.absolute_episode.is_some()
                    && episode.absolute_episode_number != numbers.absolute_episode)
            {
                continue;
            }
            let identity = CanonicalIdentityEvidence {
                series_ids: mapping.ids.clone(),
                season_anilist_id: mapping.ids.anilist.clone(),
                tvdb_episode_id: episode.tvdb_id.clone(),
                anidb_episode_id: episode.anidb_eid.clone(),
            };
            let key = serde_json::to_string(&json!({
                "series": identity.series_ids,
                "season": identity.season_anilist_id,
                "tvdbEpisode": identity.tvdb_episode_id,
                "anidbEpisode": identity.anidb_episode_id,
            }))?;
            identities.entry(key).or_insert(identity);
        }
    }
    ensure!(
        identities.len() <= 1,
        "canonical repair mapping is ambiguous across ani.zip identities"
    );
    Ok(identities.into_values().next())
}

fn extract_anime_match_assist_json(raw: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(raw).ok()?;
    let assist = value.get("animeMatchAssist")?.clone();
    serde_json::to_string(&json!({ "animeMatchAssist": assist })).ok()
}

async fn apply_canonical_repair_plan(
    pool: &AnyPool,
    claimed: &ClaimedRepair,
    plan: &CanonicalRepairPlan,
) -> Result<ApplyRepairOutcome> {
    apply_canonical_repair_plan_inner_with_run(pool, claimed, plan, false, None).await
}

async fn apply_canonical_repair_plan_for_run(
    pool: &AnyPool,
    claimed: &ClaimedRepair,
    plan: &CanonicalRepairPlan,
    run_token: Option<&str>,
) -> Result<ApplyRepairOutcome> {
    apply_canonical_repair_plan_inner_with_run(pool, claimed, plan, false, run_token).await
}

async fn apply_canonical_repair_plan_inner(
    pool: &AnyPool,
    claimed: &ClaimedRepair,
    plan: &CanonicalRepairPlan,
    fail_before_commit: bool,
) -> Result<ApplyRepairOutcome> {
    apply_canonical_repair_plan_inner_with_run(pool, claimed, plan, fail_before_commit, None).await
}

async fn apply_canonical_repair_plan_inner_with_run(
    pool: &AnyPool,
    claimed: &ClaimedRepair,
    plan: &CanonicalRepairPlan,
    fail_before_commit: bool,
    run_token: Option<&str>,
) -> Result<ApplyRepairOutcome> {
    let media_file_id = claimed.candidate.media_file_id.to_string();
    let source_series_id = claimed.candidate.series_id.to_string();
    let series_id = plan.target_series_id.to_string();
    let mut transaction = pool.begin().await?;

    lock_repair_authority_tables(&mut transaction).await?;

    // The file row and the versioned ledger claim are the ownership boundary.
    // PostgreSQL receives a row lock here; SQLite serializes the write
    // transaction. Every later check and mutation is protected by that lock.
    let locked = sqlx::query::<sqlx::Any>(
        "UPDATE media_files SET id = id WHERE id = $1 AND media_item_id = $2 AND path = $3 \
         AND scan_state != 'missing'",
    )
    .bind(&media_file_id)
    .bind(&source_series_id)
    .bind(&claimed.candidate.path)
    .execute(&mut *transaction)
    .await?;
    ensure!(
        locked.rows_affected() == 1,
        "anime repair target changed or disappeared"
    );
    let owns_claim: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM library_anime_repairs WHERE media_file_id = $1 AND repair_version = $2 \
         AND status = 'running' AND claim_token = $3 LIMIT 1",
    )
    .bind(&media_file_id)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(&claimed.claim_token)
    .fetch_optional(&mut *transaction)
    .await?;
    ensure!(owns_claim.is_some(), "anime library repair claim changed");

    for locked_series_id in [&source_series_id, &series_id]
        .into_iter()
        .collect::<BTreeSet<_>>()
    {
        let locked_series = sqlx::query::<sqlx::Any>("UPDATE series SET id = id WHERE id = $1")
            .bind(locked_series_id)
            .execute(&mut *transaction)
            .await?;
        ensure!(
            locked_series.rows_affected() == 1,
            "anime repair canonical series changed or disappeared"
        );
    }

    if repair_candidate_is_protected_in_transaction(&mut transaction, claimed).await? {
        let protected = sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repairs SET status = 'protected', claim_token = NULL, \
             claim_expires_at = NULL, \
             claimed_at = NULL, completed_at = CURRENT_TIMESTAMP, last_error = NULL, \
             last_assist_json = $1, updated_at = CURRENT_TIMESTAMP \
             WHERE media_file_id = $2 AND repair_version = $3 \
               AND status = 'running' AND claim_token = $4",
        )
        .bind(plan.assist_json.as_ref())
        .bind(&media_file_id)
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .bind(&claimed.claim_token)
        .execute(&mut *transaction)
        .await?;
        ensure!(
            protected.rows_affected() == 1,
            "anime library repair claim changed"
        );
        if let Some(run_token) = run_token {
            persist_repair_run_progress_in_transaction(
                &mut transaction,
                run_token,
                &RepairIterationCounts {
                    protected: 1,
                    ..Default::default()
                },
            )
            .await?;
        }
        transaction.commit().await?;
        return Ok(ApplyRepairOutcome::Protected);
    }

    // Legacy rows have no per-classification write manifest. Discovery may
    // authorize their exact classifier rows only for a one-file provisional
    // series; recheck that structural premise under the repair transaction
    // before any ownership or link mutation occurs.
    let legacy_identity_manifest_authorized = legacy_identity_manifest_is_still_exclusive(
        &mut transaction,
        &plan.classifier_identity,
        &media_file_id,
        &source_series_id,
    )
    .await?;
    ensure!(
        plan.classifier_identity.legacy_exclusive_owner.is_none()
            || legacy_identity_manifest_authorized,
        "legacy anime repair identity ownership changed after enrollment"
    );

    let season_number = plan
        .numbers
        .season
        .ok_or_else(|| anyhow::anyhow!("canonical repair plan is missing its season"))?;
    let episode_number = plan
        .numbers
        .episode
        .ok_or_else(|| anyhow::anyhow!("canonical repair plan is missing its episode"))?;
    ensure!(
        season_number >= 0,
        "canonical repair season must not be negative"
    );
    ensure!(
        episode_number > 0,
        "canonical repair episode must be positive"
    );

    let proposed_season_id = Uuid::new_v4().to_string();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO seasons (id, series_id, season_number, created_at, updated_at) \
         VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(series_id, season_number) DO UPDATE SET updated_at = CURRENT_TIMESTAMP",
    )
    .bind(&proposed_season_id)
    .bind(&series_id)
    .bind(season_number)
    .execute(&mut *transaction)
    .await?;
    let season_id: String = sqlx::query_scalar(
        "SELECT id FROM seasons WHERE series_id = $1 AND season_number = $2 LIMIT 1",
    )
    .bind(&series_id)
    .bind(season_number)
    .fetch_one(&mut *transaction)
    .await?;

    let proposed_episode_id = Uuid::new_v4().to_string();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episodes \
         (id, series_id, season_id, season_number, episode_number, absolute_episode_number, \
          has_file, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(series_id, season_number, episode_number) DO UPDATE SET \
         absolute_episode_number = COALESCE( \
             excluded.absolute_episode_number, episodes.absolute_episode_number \
         ), updated_at = CURRENT_TIMESTAMP",
    )
    .bind(&proposed_episode_id)
    .bind(&series_id)
    .bind(&season_id)
    .bind(season_number)
    .bind(episode_number)
    .bind(plan.numbers.absolute_episode)
    .execute(&mut *transaction)
    .await?;
    let episode_id: String = sqlx::query_scalar(
        "SELECT id FROM episodes WHERE series_id = $1 AND season_number = $2 \
         AND episode_number = $3 LIMIT 1",
    )
    .bind(&series_id)
    .bind(season_number)
    .bind(episode_number)
    .fetch_one(&mut *transaction)
    .await?;

    let previous_episode_ids = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT episode_id FROM episode_files WHERE media_file_id = $1 ORDER BY episode_id",
    )
    .bind(&media_file_id)
    .fetch_all(&mut *transaction)
    .await?;
    let previous_movie_ids = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = $1 ORDER BY movie_id",
    )
    .bind(&media_file_id)
    .fetch_all(&mut *transaction)
    .await?;
    let discovered_episode_ids = claimed
        .candidate
        .evidence_snapshot
        .get("episodeLinks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|link| link.get("episodeId").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let current_episode_ids = previous_episode_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let already_canonical = current_episode_ids.len() == 1
        && current_episode_ids
            .first()
            .is_some_and(|id| id == &episode_id);
    ensure!(
        current_episode_ids == discovered_episode_ids || already_canonical,
        "anime repair episode links changed after discovery"
    );
    let link_changed = previous_episode_ids.len() != 1
        || previous_episode_ids.first() != Some(&episode_id)
        || !previous_movie_ids.is_empty();
    if source_series_id != series_id {
        let moved = sqlx::query::<sqlx::Any>(
            "UPDATE media_files SET media_item_id = $1, updated_at = CURRENT_TIMESTAMP \
             WHERE id = $2 AND media_item_id = $3",
        )
        .bind(&series_id)
        .bind(&media_file_id)
        .bind(&source_series_id)
        .execute(&mut *transaction)
        .await?;
        ensure!(
            moved.rows_affected() == 1,
            "anime repair file ownership changed"
        );
    }
    sqlx::query::<sqlx::Any>("DELETE FROM episode_files WHERE media_file_id = $1")
        .bind(&media_file_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
    )
    .bind(&episode_id)
    .bind(&media_file_id)
    .execute(&mut *transaction)
    .await?;
    sqlx::query::<sqlx::Any>("DELETE FROM movie_files WHERE media_file_id = $1")
        .bind(&media_file_id)
        .execute(&mut *transaction)
        .await?;

    let mut affected_episode_ids = previous_episode_ids.into_iter().collect::<BTreeSet<_>>();
    affected_episode_ids.insert(episode_id.clone());
    for affected_episode_id in affected_episode_ids {
        refresh_episode_has_file(&mut transaction, &affected_episode_id).await?;
    }
    for previous_movie_id in previous_movie_ids {
        cleanup_orphan_movie_placeholder_in_transaction(&mut transaction, &previous_movie_id)
            .await?;
    }

    let empty_classifier_identity = ClassifierIdentityManifest::default();
    let classifier_identity = if legacy_identity_manifest_authorized {
        &plan.classifier_identity
    } else {
        &empty_classifier_identity
    };
    let mut repaired_identities = remove_classifier_identity_manifest(
        &mut transaction,
        &source_series_id,
        classifier_identity,
    )
    .await?;
    repaired_identities +=
        replace_classifier_series_identity(&mut transaction, &series_id, &plan.identity.series_ids)
            .await?;
    repaired_identities += replace_classifier_season_identity(
        &mut transaction,
        &season_id,
        plan.identity.season_anilist_id.as_deref(),
    )
    .await?;
    repaired_identities += replace_classifier_episode_identity(
        &mut transaction,
        &episode_id,
        "tvdb_episode",
        plan.identity.tvdb_episode_id.as_deref(),
    )
    .await?;
    repaired_identities += replace_classifier_episode_identity(
        &mut transaction,
        &episode_id,
        "anidb_episode",
        plan.identity.anidb_episode_id.as_deref(),
    )
    .await?;
    repaired_identities += repair_target_legacy_identity_cache(
        &mut transaction,
        &series_id,
        &plan.identity.series_ids,
    )
    .await?;

    if fail_before_commit {
        anyhow::bail!("injected anime repair failure before commit");
    }

    sqlx::query::<sqlx::Any>(
        "INSERT INTO classifier_resolution_state \
         (media_file_id, disposition, confidence, hint_json, candidates_json, \
          applied_identity_version, applied_identity_evidence_json, anime_match_assist_json, \
          created_at, updated_at) \
         VALUES ($1, 'applied', $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT(media_file_id) DO UPDATE SET disposition = 'applied', \
         confidence = COALESCE(excluded.confidence, classifier_resolution_state.confidence), \
         hint_json = COALESCE(excluded.hint_json, classifier_resolution_state.hint_json), \
         candidates_json = COALESCE( \
             excluded.candidates_json, classifier_resolution_state.candidates_json \
         ), applied_identity_version = excluded.applied_identity_version, \
         applied_identity_evidence_json = excluded.applied_identity_evidence_json, \
         anime_match_assist_json = COALESCE( \
             excluded.anime_match_assist_json, classifier_resolution_state.anime_match_assist_json \
         ), \
         updated_at = CURRENT_TIMESTAMP",
    )
    .bind(&media_file_id)
    .bind(plan.classification.confidence)
    .bind(plan.classification.hint_json.as_ref())
    .bind(plan.classification.candidates_json.as_ref())
    .bind(APPLIED_IDENTITY_EVIDENCE_VERSION)
    .bind(&plan.identity_evidence_json)
    .bind(plan.assist_json.as_ref())
    .execute(&mut *transaction)
    .await?;

    mark_captured_review_rows_applied(&mut transaction, claimed).await?;

    let repaired_links = i64::from(link_changed);
    let completed = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repairs SET status = 'completed', claim_token = NULL, \
         claim_expires_at = NULL, \
         claimed_at = NULL, completed_at = CURRENT_TIMESTAMP, repaired_link_count = $1, \
         repaired_identity_count = $2, last_error = NULL, last_assist_json = $3, \
         updated_at = CURRENT_TIMESTAMP \
         WHERE media_file_id = $4 AND repair_version = $5 \
           AND status = 'running' AND claim_token = $6",
    )
    .bind(repaired_links)
    .bind(repaired_identities)
    .bind(plan.assist_json.as_ref())
    .bind(&media_file_id)
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(&claimed.claim_token)
    .execute(&mut *transaction)
    .await?;
    ensure!(
        completed.rows_affected() == 1,
        "anime library repair claim changed"
    );
    if let Some(run_token) = run_token {
        persist_repair_run_progress_in_transaction(
            &mut transaction,
            run_token,
            &RepairIterationCounts {
                completed: 1,
                repaired_links,
                repaired_identities,
                ..Default::default()
            },
        )
        .await?;
    }

    transaction.commit().await?;
    if let Err(error) = cleanup_orphan_series_stub(pool, &source_series_id, &series_id).await {
        tracing::warn!(
            media_file_id = %media_file_id,
            previous_series_id = %source_series_id,
            current_series_id = %series_id,
            error = %error,
            "anime repair committed but placeholder-series cleanup failed"
        );
    }
    Ok(ApplyRepairOutcome::Applied {
        links: repaired_links,
        identities: repaired_identities,
    })
}

async fn lock_repair_authority_tables(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
) -> Result<()> {
    if transaction.backend_name() != "PostgreSQL" {
        return Ok(());
    }

    // A row-level check cannot lock a row that does not exist yet. Lock all
    // authority sources before taking any repair row locks so a
    // concurrent import, managed-library claim, or manual override is ordered
    // wholly before or wholly after this repair transaction. SHARE still
    // permits ordinary reads while conflicting with writer ROW EXCLUSIVE
    // locks. Keeping this first also avoids a writer/repair lock inversion.
    sqlx::query::<sqlx::Any>(
        "LOCK TABLE acquisition_import_file_links, managed_library_provenance, \
                    classifier_overrides, media_ownerships IN SHARE MODE",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn legacy_identity_manifest_is_still_exclusive(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    manifest: &ClassifierIdentityManifest,
    media_file_id: &str,
    series_id: &str,
) -> Result<bool> {
    let Some(owner) = manifest.legacy_exclusive_owner.as_ref() else {
        return Ok(true);
    };
    if owner.media_file_id != media_file_id || owner.series_id != series_id {
        return Ok(false);
    }
    let exclusive: Option<i64> = sqlx::query_scalar(
        "SELECT 1 WHERE \
         EXISTS (SELECT 1 FROM media_files WHERE id = $1 AND media_item_id = $2) \
         AND NOT EXISTS (SELECT 1 FROM media_files \
                         WHERE media_item_id = $2 AND id != $1) \
         AND NOT EXISTS (SELECT 1 FROM episode_files ef \
                         JOIN episodes e ON e.id = ef.episode_id \
                         WHERE e.series_id = $2 AND ef.media_file_id != $1) \
         LIMIT 1",
    )
    .bind(media_file_id)
    .bind(series_id)
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(exclusive.is_some())
}

async fn mark_captured_review_rows_applied(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    claimed: &ClaimedRepair,
) -> Result<()> {
    let review_ids = claimed
        .candidate
        .evidence_snapshot
        .get("legacyPendingReview")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|review| review.get("id").and_then(Value::as_str))
        .filter(|id| !id.trim().is_empty())
        .collect::<BTreeSet<_>>();
    for review_id in review_ids {
        sqlx::query::<sqlx::Any>(
            "UPDATE review_queue SET status = 'applied', updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND media_file_id = $2 AND status = 'pending'",
        )
        .bind(review_id)
        .bind(claimed.candidate.media_file_id.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn repair_candidate_is_protected_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    claimed: &ClaimedRepair,
) -> Result<bool> {
    let protected: Option<i64> = sqlx::query_scalar(
        "SELECT 1 WHERE \
         EXISTS (SELECT 1 FROM acquisition_import_file_links \
                 WHERE media_file_id = $1 AND state = 'imported') \
         OR EXISTS (SELECT 1 FROM managed_library_provenance WHERE media_item_id = $2) \
         OR EXISTS (SELECT 1 FROM media_ownerships \
                    WHERE media_item_id = $2 AND active = 1 \
                      AND owner_type IN ('acquisition', 'extension')) \
         LIMIT 1",
    )
    .bind(claimed.candidate.media_file_id.to_string())
    .bind(claimed.candidate.series_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if protected.is_some() {
        return Ok(true);
    }
    for library_type in ["anime", "series", "movie"] {
        let Some(key) = derive_override_key(library_type, &claimed.candidate.path) else {
            continue;
        };
        let override_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM classifier_overrides WHERE library_type = $1 \
             AND normalized_key = $2 LIMIT 1",
        )
        .bind(library_type)
        .bind(key)
        .fetch_optional(&mut **transaction)
        .await?;
        if override_exists.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn refresh_episode_has_file(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    episode_id: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes SET has_file = CASE WHEN EXISTS (\
            SELECT 1 FROM episode_files ef JOIN media_files mf ON mf.id = ef.media_file_id \
            WHERE ef.episode_id = episodes.id AND mf.scan_state = 'ok'\
         ) THEN TRUE ELSE FALSE END, updated_at = CURRENT_TIMESTAMP WHERE id = $1",
    )
    .bind(episode_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn cleanup_orphan_movie_placeholder_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    movie_id: &str,
) -> Result<()> {
    // A stale cross-link is evidence only about the link. The movie itself may
    // be a real library item, a managed import, or an item with independent
    // metadata. Delete it only when both backing rows carry the exact internal
    // placeholder marker and every ownership/identity signal is absent.
    let removed = sqlx::query::<sqlx::Any>(
        "DELETE FROM movies WHERE id = $1 \
         AND external_imdb IS NULL AND external_tmdb IS NULL \
         AND metadata_json = '{\"classifierPlaceholder\":true}' \
         AND NOT EXISTS (SELECT 1 FROM movie_files WHERE movie_id = $1) \
         AND NOT EXISTS (SELECT 1 FROM movie_external_ids WHERE movie_id = $1) \
         AND NOT EXISTS (SELECT 1 FROM managed_library_provenance WHERE media_item_id = $1) \
         AND NOT EXISTS (SELECT 1 FROM artwork_refs \
                         WHERE owner_type = 'movie' AND owner_id = $1) \
         AND EXISTS (SELECT 1 FROM media_items mi WHERE mi.id = $1 AND mi.type = 'movie' \
                     AND mi.metadata_json = '{\"classifierPlaceholder\":true}' \
                     AND COALESCE(NULLIF(TRIM(mi.external_ids), ''), '{}') = '{}')",
    )
    .bind(movie_id)
    .execute(&mut **transaction)
    .await?;
    if removed.rows_affected() == 0 {
        return Ok(());
    }

    sqlx::query::<sqlx::Any>(
        "DELETE FROM media_items WHERE id = $1 AND type = 'movie' \
         AND metadata_json = '{\"classifierPlaceholder\":true}' \
         AND COALESCE(NULLIF(TRIM(external_ids), ''), '{}') = '{}' \
         AND NOT EXISTS (SELECT 1 FROM media_files WHERE media_item_id = $1) \
         AND NOT EXISTS (SELECT 1 FROM managed_library_provenance WHERE media_item_id = $1) \
         AND NOT EXISTS (SELECT 1 FROM movies WHERE id = $1) \
         AND NOT EXISTS (SELECT 1 FROM series WHERE id = $1)",
    )
    .bind(movie_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn canonical_series_ids(ids: &ExternalIds) -> Vec<(&'static str, &str)> {
    let mut values = Vec::new();
    if let Some(value) = ids.imdb.as_deref().filter(|value| !value.trim().is_empty()) {
        values.push(("imdb", value));
    }
    if let Some(value) = ids.tmdb.as_deref().filter(|value| !value.trim().is_empty()) {
        values.push(("tmdb", value));
    }
    if let Some(value) = ids
        .tvdb_series
        .as_deref()
        .or(ids.tvdb.as_deref())
        .filter(|value| !value.trim().is_empty())
    {
        values.push(("tvdb", value));
    }
    if let Some(value) = ids
        .anilist
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        values.push(("anilist", value));
    }
    if let Some(value) = ids
        .anidb
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        values.push(("anidb", value));
    }
    if let Some(value) = ids.mal.as_deref().filter(|value| !value.trim().is_empty()) {
        values.push(("mal", value));
    }
    if let Some(value) = ids
        .kitsu
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        values.push(("kitsu", value));
    }
    values
}

async fn canonical_series_owner(
    pool: &AnyPool,
    current_series_id: Uuid,
    ids: &ExternalIds,
) -> Result<Uuid> {
    let mut owners = BTreeSet::new();
    for (provider, external_id) in canonical_series_ids(ids) {
        owners.extend(
            sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT series_id FROM series_external_ids \
                 WHERE provider = $1 AND external_id = $2 \
                   AND COALESCE(source, '') NOT IN ('classifier', 'anilist_chain')",
            )
            .bind(provider)
            .bind(external_id)
            .fetch_all(pool)
            .await?,
        );
        let direct_query = match provider {
            "imdb" => Some("SELECT id FROM series WHERE external_imdb = $1"),
            "tvdb" => Some("SELECT id FROM series WHERE external_tvdb_series = $1"),
            "anilist" => Some("SELECT id FROM series WHERE external_anilist = $1"),
            _ => None,
        };
        if let Some(query) = direct_query {
            let query = format!(
                "{query} AND (\
                   EXISTS (SELECT 1 FROM series_external_ids trusted \
                           WHERE trusted.series_id = series.id \
                             AND trusted.provider = $2 AND trusted.external_id = $1 \
                             AND COALESCE(trusted.source, '') \
                                 NOT IN ('classifier', 'anilist_chain')) \
                   OR NOT EXISTS (SELECT 1 FROM series_external_ids provisional \
                                  WHERE provisional.series_id = series.id \
                                    AND provisional.provider = $2 \
                                    AND provisional.external_id = $1 \
                                    AND provisional.source \
                                        IN ('classifier', 'anilist_chain')))"
            );
            owners.extend(
                sqlx::query_scalar::<sqlx::Any, String>(&query)
                    .bind(external_id)
                    .bind(provider)
                    .fetch_all(pool)
                    .await?,
            );
        }
    }
    ensure!(
        owners.len() <= 1,
        "canonical anime identity is already split across multiple series"
    );
    owners
        .into_iter()
        .next()
        .map(|owner| Uuid::parse_str(&owner).map_err(Into::into))
        .transpose()
        .map(|owner| owner.unwrap_or(current_series_id))
}

async fn remove_classifier_identity_manifest(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    series_id: &str,
    manifest: &ClassifierIdentityManifest,
) -> Result<i64> {
    let mut changed = 0_i64;
    for row in &manifest.series_rows {
        let direct_column = match row.provider.as_str() {
            "imdb" => Some("external_imdb"),
            "tvdb" => Some("external_tvdb_series"),
            "anilist" => Some("external_anilist"),
            _ => None,
        };
        if let Some(column) = direct_column {
            let select = format!("SELECT {column} FROM series WHERE id = $1 LIMIT 1");
            let current = sqlx::query_scalar::<sqlx::Any, Option<String>>(&select)
                .bind(series_id)
                .fetch_one(&mut **transaction)
                .await?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            if current.as_deref() == Some(row.external_id.as_str()) {
                let exact_row_exists = sqlx::query_scalar::<sqlx::Any, i64>(
                    "SELECT 1 FROM series_external_ids WHERE series_id = $1 AND provider = $2 \
                     AND external_id = $3 AND source = $4 LIMIT 1",
                )
                .bind(series_id)
                .bind(&row.provider)
                .bind(&row.external_id)
                .bind(&row.source)
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
                if exact_row_exists {
                    let update = format!(
                        "UPDATE series SET {column} = NULL, updated_at = CURRENT_TIMESTAMP \
                         WHERE id = $1 AND {column} = $2"
                    );
                    changed += sqlx::query::<sqlx::Any>(&update)
                        .bind(series_id)
                        .bind(&row.external_id)
                        .execute(&mut **transaction)
                        .await?
                        .rows_affected() as i64;
                }
            }
        }
        changed += sqlx::query::<sqlx::Any>(
            "DELETE FROM series_external_ids WHERE series_id = $1 AND provider = $2 \
             AND external_id = $3 AND source = $4",
        )
        .bind(series_id)
        .bind(&row.provider)
        .bind(&row.external_id)
        .bind(&row.source)
        .execute(&mut **transaction)
        .await?
        .rows_affected() as i64;
    }

    for row in &manifest.season_rows {
        let season_ids = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT s.id FROM seasons s JOIN season_external_ids sei ON sei.season_id = s.id \
             WHERE s.series_id = $1 AND s.season_number = $2 AND sei.provider = $3 \
               AND sei.external_id = $4 AND sei.source = $5",
        )
        .bind(series_id)
        .bind(row.season_number)
        .bind(&row.provider)
        .bind(&row.external_id)
        .bind(&row.source)
        .fetch_all(&mut **transaction)
        .await?;
        for season_id in season_ids {
            if row.provider == "anilist" {
                changed += sqlx::query::<sqlx::Any>(
                    "UPDATE seasons SET external_anilist = NULL, updated_at = CURRENT_TIMESTAMP \
                     WHERE id = $1 AND external_anilist = $2",
                )
                .bind(&season_id)
                .bind(&row.external_id)
                .execute(&mut **transaction)
                .await?
                .rows_affected() as i64;
            }
            changed += sqlx::query::<sqlx::Any>(
                "DELETE FROM season_external_ids WHERE season_id = $1 \
                 AND provider = $2 AND external_id = $3 AND source = $4",
            )
            .bind(&season_id)
            .bind(&row.provider)
            .bind(&row.external_id)
            .bind(&row.source)
            .execute(&mut **transaction)
            .await?
            .rows_affected() as i64;
        }
    }

    for row in &manifest.episode_rows {
        let belongs_to_series: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM episodes WHERE id = $1 AND series_id = $2 LIMIT 1")
                .bind(&row.episode_id)
                .bind(series_id)
                .fetch_optional(&mut **transaction)
                .await?;
        if belongs_to_series.is_none() {
            continue;
        }
        changed += sqlx::query::<sqlx::Any>(
            "DELETE FROM episode_external_ids WHERE episode_id = $1 AND provider = $2 \
             AND external_id = $3 AND source = $4",
        )
        .bind(&row.episode_id)
        .bind(&row.provider)
        .bind(&row.external_id)
        .bind(&row.source)
        .execute(&mut **transaction)
        .await?
        .rows_affected() as i64;
    }
    Ok(changed)
}

async fn replace_classifier_series_identity(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    series_id: &str,
    ids: &ExternalIds,
) -> Result<i64> {
    let mut changed = 0_i64;
    for (provider, canonical) in canonical_series_ids(ids) {
        let direct_column = match provider {
            "imdb" => Some("external_imdb"),
            "tvdb" => Some("external_tvdb_series"),
            "anilist" => Some("external_anilist"),
            _ => None,
        };
        if let Some(column) = direct_column {
            let clear_provisional_collision = format!(
                "UPDATE series SET {column} = NULL, updated_at = CURRENT_TIMESTAMP \
                 WHERE id != $1 AND {column} = $2 \
                   AND EXISTS (SELECT 1 FROM series_external_ids provisional \
                               WHERE provisional.series_id = series.id \
                                 AND provisional.provider = $3 \
                                 AND provisional.external_id = $2 \
                                 AND provisional.source IN ('classifier', 'anilist_chain')) \
                   AND NOT EXISTS (SELECT 1 FROM series_external_ids trusted \
                                   WHERE trusted.series_id = series.id \
                                     AND trusted.provider = $3 \
                                     AND trusted.external_id = $2 \
                                     AND COALESCE(trusted.source, '') \
                                         NOT IN ('classifier', 'anilist_chain'))"
            );
            changed += sqlx::query::<sqlx::Any>(&clear_provisional_collision)
                .bind(series_id)
                .bind(canonical)
                .bind(provider)
                .execute(&mut **transaction)
                .await?
                .rows_affected() as i64;

            let query = format!("SELECT {column} FROM series WHERE id = $1 LIMIT 1");
            let current = sqlx::query_scalar::<sqlx::Any, Option<String>>(&query)
                .bind(series_id)
                .fetch_one(&mut **transaction)
                .await?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            if current.is_none() {
                let update = format!(
                    "UPDATE series SET {column} = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2"
                );
                changed += sqlx::query::<sqlx::Any>(&update)
                    .bind(canonical)
                    .bind(series_id)
                    .execute(&mut **transaction)
                    .await?
                    .rows_affected() as i64;
            }
        }

        changed += sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids \
             (id, series_id, provider, external_id, confidence, source) \
             VALUES ($1, $2, $3, $4, 1.0, 'anime_match_repair') ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(series_id)
        .bind(provider)
        .bind(canonical)
        .execute(&mut **transaction)
        .await?
        .rows_affected() as i64;
    }
    Ok(changed)
}

async fn replace_classifier_season_identity(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    season_id: &str,
    canonical_anilist: Option<&str>,
) -> Result<i64> {
    let Some(canonical) = canonical_anilist
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(0);
    };
    let mut changed = 0_i64;

    let current = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT external_anilist FROM seasons WHERE id = $1 AND external_anilist IS NOT NULL",
    )
    .bind(season_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty());
    if current.is_none() {
        changed += sqlx::query::<sqlx::Any>(
            "UPDATE seasons SET external_anilist = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
        )
        .bind(canonical)
        .bind(season_id)
        .execute(&mut **transaction)
        .await?
        .rows_affected() as i64;
    }
    changed += sqlx::query::<sqlx::Any>(
        "INSERT INTO season_external_ids \
         (id, season_id, provider, external_id, confidence, source) \
         VALUES ($1, $2, 'anilist', $3, 1.0, 'anime_match_repair') ON CONFLICT DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(season_id)
    .bind(canonical)
    .execute(&mut **transaction)
    .await?
    .rows_affected() as i64;
    Ok(changed)
}

async fn replace_classifier_episode_identity(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    episode_id: &str,
    provider: &str,
    canonical_id: Option<&str>,
) -> Result<i64> {
    let Some(canonical) = canonical_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(0);
    };
    let changed = sqlx::query::<sqlx::Any>(
        "INSERT INTO episode_external_ids \
         (id, episode_id, provider, external_id, confidence, source) \
         VALUES ($1, $2, $3, $4, 1.0, 'anime_match_repair') ON CONFLICT DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(episode_id)
    .bind(provider)
    .bind(canonical)
    .execute(&mut **transaction)
    .await?
    .rows_affected() as i64;
    Ok(changed)
}

fn merge_canonical_media_item_external_ids(existing: Option<&Value>, ids: &ExternalIds) -> Value {
    let mut merged = existing
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (provider, external_id) in canonical_series_ids(ids) {
        // Replace aliases for providers proven by the canonical result, but
        // retain unrelated provider and extension-owned keys. The legacy JSON
        // cache has no row-level provenance, so deleting anything else would
        // exceed the repair's classifier-only removal authority.
        let (key, aliases): (&str, &[&str]) = match provider {
            "imdb" => ("imdb", &["imdb", "imdbId", "imdb_id"]),
            "tmdb" => ("tmdb", &["tmdb", "tmdbId", "tmdb_id"]),
            "tvdb" => (
                "tvdbSeries",
                &["tvdb", "tvdbSeries", "tvdb_series", "tvdbId", "tvdb_id"],
            ),
            "anilist" => (
                "anilist",
                &["anilist", "aniList", "anilistId", "anilist_id"],
            ),
            "anidb" => ("anidb", &["anidb", "aniDb", "anidbId", "anidb_id"]),
            "mal" => ("mal", &["mal", "malId", "mal_id"]),
            "kitsu" => ("kitsu", &["kitsu", "kitsuId", "kitsu_id"]),
            _ => continue,
        };
        for alias in aliases {
            merged.remove(*alias);
        }
        merged.insert(key.to_string(), Value::String(external_id.to_string()));
    }
    Value::Object(merged)
}

async fn repair_target_legacy_identity_cache(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    target_series_id: &str,
    canonical_ids: &ExternalIds,
) -> Result<i64> {
    // Managed and acquisition owners remain the identity authority even when
    // this repair corrects the file's episode link. Legacy compatibility rows
    // are rewritten only for locally owned/external items.
    let authoritative: Option<i64> = sqlx::query_scalar(
        "SELECT 1 WHERE \
         EXISTS (SELECT 1 FROM managed_library_provenance WHERE media_item_id = $1) \
         OR EXISTS (SELECT 1 FROM media_ownerships \
                    WHERE media_item_id = $1 AND active = 1 \
                      AND owner_type IN ('extension', 'acquisition')) \
         OR EXISTS (SELECT 1 FROM acquisition_import_file_links ail \
                    LEFT JOIN media_files mf ON mf.id = ail.media_file_id \
                    LEFT JOIN episodes e ON e.id = ail.episode_id \
                    WHERE ail.state = 'imported' \
                      AND (mf.media_item_id = $1 OR e.series_id = $1 OR ail.movie_id = $1)) \
         LIMIT 1",
    )
    .bind(target_series_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if authoritative.is_some() {
        return Ok(0);
    }

    let row = sqlx::query(
        "SELECT type, external_ids, title, year FROM media_items WHERE id = $1 LIMIT 1",
    )
    .bind(target_series_id)
    .fetch_one(&mut **transaction)
    .await?;
    let current_type: String = row.try_get("type")?;
    let current_external_ids = row
        .try_get::<String, _>("external_ids")
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
    let title: String = row.try_get("title")?;
    let year = row.try_get::<i64, _>("year").ok();
    let canonical_external_ids =
        merge_canonical_media_item_external_ids(current_external_ids.as_ref(), canonical_ids);
    let canonical_external_ids_json = serde_json::to_string(&canonical_external_ids)?;
    let mut changed = 0_i64;
    if current_type != "anime" || current_external_ids.as_ref() != Some(&canonical_external_ids) {
        changed += sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET type = 'anime', external_ids = $1, \
             updated_at = CURRENT_TIMESTAMP WHERE id = $2",
        )
        .bind(&canonical_external_ids_json)
        .bind(target_series_id)
        .execute(&mut **transaction)
        .await?
        .rows_affected() as i64;
    }

    let owner_rows = sqlx::query(
        "SELECT ownership_id, metadata_json FROM media_ownerships \
         WHERE media_item_id = $1 AND owner_type = 'external' AND active = 1 \
         ORDER BY ownership_id",
    )
    .bind(target_series_id)
    .fetch_all(&mut **transaction)
    .await?;
    for owner in owner_rows {
        let ownership_id: String = owner.try_get("ownership_id")?;
        let original = owner
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
        let mut metadata = original
            .clone()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        metadata.insert("mediaType".to_string(), Value::String("anime".to_string()));
        metadata.insert("title".to_string(), Value::String(title.clone()));
        metadata.insert(
            "year".to_string(),
            year.map_or(Value::Null, |value| Value::Number(value.into())),
        );
        let repaired_owner_external_ids =
            merge_canonical_media_item_external_ids(metadata.get("externalIds"), canonical_ids);
        metadata.insert("externalIds".to_string(), repaired_owner_external_ids);
        let repaired = Value::Object(metadata);
        if original.as_ref() == Some(&repaired) {
            continue;
        }
        changed += sqlx::query::<sqlx::Any>(
            "UPDATE media_ownerships SET metadata_json = $1, updated_at = CURRENT_TIMESTAMP \
             WHERE ownership_id = $2 AND media_item_id = $3 \
               AND owner_type = 'external' AND active = 1",
        )
        .bind(serde_json::to_string(&repaired)?)
        .bind(ownership_id)
        .bind(target_series_id)
        .execute(&mut **transaction)
        .await?
        .rows_affected() as i64;
    }
    Ok(changed)
}

async fn mark_repair_retryable_for_run(
    pool: &AnyPool,
    claimed: &ClaimedRepair,
    error: Option<&str>,
    assist_json: Option<&str>,
    run_token: Option<&str>,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE library_anime_repairs SET status = 'retryable', claim_token = NULL, \
         claim_expires_at = NULL, \
         claimed_at = NULL, completed_at = NULL, repaired_link_count = 0, \
         repaired_identity_count = 0, last_error = $1, last_assist_json = $2, \
         updated_at = CURRENT_TIMESTAMP \
         WHERE media_file_id = $3 AND repair_version = $4 \
           AND status = 'running' AND claim_token = $5",
    )
    .bind(error)
    .bind(assist_json)
    .bind(claimed.candidate.media_file_id.to_string())
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .bind(&claimed.claim_token)
    .execute(&mut *transaction)
    .await?;
    ensure!(
        updated.rows_affected() == 1,
        "anime library repair claim changed"
    );
    if let Some(run_token) = run_token {
        persist_repair_run_progress_in_transaction(
            &mut transaction,
            run_token,
            &RepairIterationCounts {
                retryable: 1,
                failures: if error.is_some() { 1 } else { 0 },
                ..Default::default()
            },
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

pub async fn anime_library_repair_snapshot(pool: &AnyPool) -> Result<AnimeLibraryRepairSnapshot> {
    let mut snapshot = AnimeLibraryRepairSnapshot {
        repair_version: ANIME_LIBRARY_REPAIR_VERSION,
        status: "pending".to_string(),
        ..Default::default()
    };
    if let Some(row) = sqlx::query(
        "SELECT status, scanned_count, claimed_count, retryable_count, completed_count, \
                protected_count, repaired_link_count, repaired_identity_count, failure_count, \
                last_error, CAST(updated_at AS TEXT) AS updated_at \
         FROM library_anime_repair_runs WHERE repair_version = $1 LIMIT 1",
    )
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .fetch_optional(pool)
    .await?
    {
        snapshot.status = row.try_get("status")?;
        snapshot.scanned_count = row.try_get("scanned_count")?;
        snapshot.claimed_count = row.try_get("claimed_count")?;
        snapshot.retryable_count = row.try_get("retryable_count")?;
        snapshot.completed_count = row.try_get("completed_count")?;
        snapshot.protected_count = row.try_get("protected_count")?;
        snapshot.repaired_link_count = row.try_get("repaired_link_count")?;
        snapshot.repaired_identity_count = row.try_get("repaired_identity_count")?;
        snapshot.failure_count = row.try_get("failure_count")?;
        snapshot.last_error = row.try_get("last_error").ok();
        snapshot.updated_at = row.try_get("updated_at").ok();
    }
    if let Some(row) = sqlx::query(
        "SELECT \
           SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_files, \
           SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_files, \
           SUM(CASE WHEN status = 'retryable' THEN 1 ELSE 0 END) AS retryable_files \
         FROM library_anime_repairs WHERE repair_version = $1",
    )
    .bind(ANIME_LIBRARY_REPAIR_VERSION)
    .fetch_optional(pool)
    .await?
    {
        snapshot.pending_files = row.try_get::<i64, _>("pending_files").unwrap_or(0);
        snapshot.running_files = row.try_get::<i64, _>("running_files").unwrap_or(0);
        snapshot.retryable_files = row.try_get::<i64, _>("retryable_files").unwrap_or(0);
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use elixir_classifier::{
        hint::{
            ClassificationHint as TestClassifierHint, FileInput as TestClassifierInput, HintParser,
            LibraryType as TestClassifierLibraryType,
        },
        identify::{
            CandidateMatch as TestCandidateMatch, ExternalIds as TestClassifierExternalIds,
            IdentifierProvider, KindHint as TestKindHint, MatchFeatures as TestMatchFeatures,
        },
    };
    use tokio::{net::TcpListener, sync::oneshot};

    use super::*;

    use crate::{
        anime_matching::{
            ANIME_MATCH_SCHEMA_VERSION, AnimeCandidateMatch, AnimeMatchAudioProfile,
            AnimeMatchEngine, AnimeMatchRequest, AnimeMatchResponse,
        },
        config::DatabaseConfig,
        db::{Database, DatabaseDriver},
    };

    #[derive(Debug, Clone)]
    struct RepairFixture {
        candidate: RepairCandidate,
        original_episode_ids: Vec<Uuid>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct IdentityLinkSnapshot {
        legacy_media_item: (String, Option<String>),
        external_owner_metadata: Vec<(String, Option<String>)>,
        direct_series_ids: (Option<String>, Option<String>, Option<String>),
        links: Vec<(String, i64, i64, Option<i64>)>,
        series_ids: Vec<(String, String, Option<String>)>,
        seasons: Vec<(String, i64, Option<String>)>,
        season_ids: Vec<(String, String, String, Option<String>)>,
        episode_ids: Vec<(String, String, String, Option<String>)>,
        classifier_state: Option<(String, Option<String>, Option<String>, i64, Option<String>)>,
    }

    #[derive(Clone)]
    struct RepairMatchEngine {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AnimeMatchEngine for RepairMatchEngine {
        async fn match_candidates(&self, request: AnimeMatchRequest) -> Result<AnimeMatchResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let target_key = request
                .target
                .wanted_target_keys
                .first()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("repair fixture received no wanted target"))?;
            Ok(AnimeMatchResponse {
                schema_version: ANIME_MATCH_SCHEMA_VERSION,
                matches: vec![AnimeCandidateMatch {
                    candidate_key: "candidate-0".to_string(),
                    matched_target_keys: vec![target_key],
                    audio_profile: AnimeMatchAudioProfile::Unknown,
                    selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
                }],
            })
        }
    }

    #[derive(Clone)]
    struct TvdbOnlyRepairHintParser {
        path: String,
    }

    impl HintParser for TvdbOnlyRepairHintParser {
        fn name(&self) -> &'static str {
            "alm8_tvdb_only_repair_hint"
        }

        fn parse(&self, input: &TestClassifierInput) -> Vec<TestClassifierHint> {
            if input.path != self.path {
                return Vec::new();
            }
            vec![TestClassifierHint {
                library_type: TestClassifierLibraryType::Anime,
                title: "Tokyo Ghoul".to_string(),
                alt_titles: vec!["Tokyo Ghoul Root A".to_string()],
                year: Some(2014),
                season: Some(2),
                episode: None,
                absolute_episode: None,
                duration_seconds: None,
                embedded_ids: TestClassifierExternalIds::default(),
                parser: "alm8_tvdb_only_repair_hint",
                parser_confidence: 1.0,
                source_path: Some(input.path.clone()),
            }]
        }
    }

    struct TvdbOnlyRepairIdentifier;

    #[async_trait]
    impl IdentifierProvider for TvdbOnlyRepairIdentifier {
        fn name(&self) -> &'static str {
            "alm8_tvdb_only_repair_identifier"
        }

        fn supports(&self, _library_type: TestClassifierLibraryType) -> bool {
            true
        }

        async fn identify(&self, hint: &TestClassifierHint) -> Result<Vec<TestCandidateMatch>> {
            Ok(vec![TestCandidateMatch {
                provider: "tvdb",
                kind: TestKindHint::Anime,
                ids: TestClassifierExternalIds {
                    tvdb_series: Some("305014".to_string()),
                    ..Default::default()
                },
                input_echo: false,
                title: "Tokyo Ghoul".to_string(),
                alt_titles: vec!["Tokyo Ghoul Root A".to_string()],
                year: Some(2014),
                season: hint.season,
                episode: None,
                absolute_episode: None,
                duration_seconds: None,
                provider_confidence: 1.0,
                score: 0.0,
                features: TestMatchFeatures::default(),
            }])
        }
    }

    #[derive(Clone)]
    struct RelationContextRepairMatchEngine {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AnimeMatchEngine for RelationContextRepairMatchEngine {
        async fn match_candidates(&self, request: AnimeMatchRequest) -> Result<AnimeMatchResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let root = request
                .context
                .seasons
                .iter()
                .find(|season| season.season_number == 1)
                .ok_or_else(|| anyhow::anyhow!("repair request omitted relation-chain season 1"))?;
            ensure!(
                root.anilist_id == "1001",
                "repair relation root has the wrong AniList identity"
            );
            let sequel = request
                .context
                .seasons
                .iter()
                .find(|season| season.season_number == 2)
                .ok_or_else(|| anyhow::anyhow!("repair request omitted relation-chain season 2"))?;
            ensure!(
                sequel.anilist_id == "1002"
                    && sequel
                        .aliases
                        .iter()
                        .any(|alias| alias.value == "Tokyo Ghoul Root A"),
                "repair sequel context omitted the canonical relation alias"
            );
            let target = sequel
                .targets
                .iter()
                .find(|target| {
                    target.episode_number == Some(1) && target.absolute_episode_number == Some(13)
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("repair sequel context omitted S02E01/absolute 13")
                })?;
            ensure!(
                request
                    .target
                    .wanted_target_keys
                    .contains(&target.target_key),
                "repair request did not expose the canonical sequel target to the model"
            );
            Ok(AnimeMatchResponse {
                schema_version: ANIME_MATCH_SCHEMA_VERSION,
                matches: vec![AnimeCandidateMatch {
                    candidate_key: "candidate-0".to_string(),
                    matched_target_keys: vec![target.target_key.clone()],
                    audio_profile: AnimeMatchAudioProfile::Unknown,
                    selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
                }],
            })
        }
    }

    async fn start_tvdb_repair_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let app = Router::new()
            .route(
                "/login",
                post(|| async { Json(json!({ "data": { "token": "repair-token" } })) }),
            )
            .route(
                "/series/:series_id",
                get(|| async {
                    Json(json!({
                        "data": {
                            "id": 305014,
                            "name": "Tokyo Ghoul",
                            "year": "2014",
                            "country": "jpn",
                            "genres": [{ "name": "Anime" }]
                        }
                    }))
                }),
            )
            .route(
                "/series/:series_id/extended",
                get(|| async {
                    Json(json!({
                        "data": {
                            "seasons": [
                                { "number": 1, "year": 2014 },
                                { "number": 2, "year": 2015 }
                            ]
                        }
                    }))
                }),
            );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}"), shutdown_tx))
    }

    fn anilist_repair_node(id: i32) -> Value {
        match id {
            1001 => json!({
                "id": 1001,
                "title": {
                    "romaji": "Tokyo Ghoul",
                    "english": "Tokyo Ghoul",
                    "native": "東京喰種"
                },
                "synonyms": [],
                "format": "TV",
                "seasonYear": 2014,
                "startDate": { "year": 2014 },
                "status": "FINISHED",
                "episodes": 12,
                "nextAiringEpisode": null,
                "relations": {
                    "edges": [{
                        "relationType": "SEQUEL",
                        "node": {
                            "id": 1002,
                            "title": {
                                "romaji": "Tokyo Ghoul Root A",
                                "english": "Tokyo Ghoul Root A",
                                "native": "東京喰種√A"
                            },
                            "synonyms": ["Tokyo Ghoul Season 2"],
                            "format": "TV",
                            "seasonYear": 2015,
                            "startDate": { "year": 2015 }
                        }
                    }]
                }
            }),
            1002 => json!({
                "id": 1002,
                "title": {
                    "romaji": "Tokyo Ghoul Root A",
                    "english": "Tokyo Ghoul Root A",
                    "native": "東京喰種√A"
                },
                "synonyms": ["Tokyo Ghoul Season 2"],
                "format": "TV",
                "seasonYear": 2015,
                "startDate": { "year": 2015 },
                "status": "FINISHED",
                "episodes": 12,
                "nextAiringEpisode": null,
                "relations": {
                    "edges": [{
                        "relationType": "PREQUEL",
                        "node": {
                            "id": 1001,
                            "title": {
                                "romaji": "Tokyo Ghoul",
                                "english": "Tokyo Ghoul",
                                "native": "東京喰種"
                            },
                            "synonyms": [],
                            "format": "TV",
                            "seasonYear": 2014,
                            "startDate": { "year": 2014 }
                        }
                    }]
                }
            }),
            _ => Value::Null,
        }
    }

    async fn start_anilist_repair_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let app = Router::new().route(
            "/",
            post(|Json(payload): Json<Value>| async move {
                if let Some(id) = payload
                    .get("variables")
                    .and_then(|variables| variables.get("id"))
                    .and_then(Value::as_i64)
                {
                    Json(json!({ "data": { "Media": anilist_repair_node(id as i32) } }))
                } else {
                    Json(json!({
                        "data": { "Page": { "media": [anilist_repair_node(1001)] } }
                    }))
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}"), shutdown_tx))
    }

    #[test]
    fn alm8_classifier_identity_manifest_contains_only_exact_captured_causal_rows() {
        let episode_id = Uuid::new_v4().to_string();
        let evidence = json!({
            "seriesExternalIds": [
                { "provider": "tvdb", "externalId": "wrong-tvdb", "source": "classifier" },
                { "provider": "tvdb", "externalId": "unrelated-tvdb", "source": "classifier" }
            ],
            "seasonExternalIds": [
                { "seasonNumber": 1, "provider": "anilist", "externalId": "wrong-anilist", "source": "anilist_chain" },
                { "seasonNumber": 1, "provider": "anilist", "externalId": "unrelated-season", "source": "classifier" }
            ],
            "episodeExternalIds": [
                { "episodeId": episode_id, "provider": "tvdb_episode", "externalId": "wrong-episode", "source": "classifier" },
                { "episodeId": episode_id, "provider": "tvdb_episode", "externalId": "unrelated-episode", "source": "classifier" }
            ],
            "classifierCandidates": {
                "hypotheses": [{ "candidate": { "ids": { "tvdb": "unrelated-tvdb" } } }]
            },
            "appliedIdentityEvidence": {
                "schemaVersion": 2,
                "causalIdentityRows": {
                    "series": [
                        { "provider": "tvdb", "externalId": "wrong-tvdb", "source": "classifier" },
                        { "provider": "tvdb", "externalId": "not-in-snapshot", "source": "classifier" }
                    ],
                    "seasons": [
                        { "seasonNumber": 1, "provider": "anilist", "externalId": "wrong-anilist", "source": "anilist_chain" }
                    ],
                    "episodes": [
                        { "episodeId": episode_id, "provider": "tvdb_episode", "externalId": "wrong-episode", "source": "classifier" }
                    ]
                }
            }
        });

        let manifest = classifier_identity_manifest(&evidence);
        assert_eq!(
            manifest.series_rows,
            BTreeSet::from([ClassifierSeriesIdentityRow {
                provider: "tvdb".to_string(),
                external_id: "wrong-tvdb".to_string(),
                source: "classifier".to_string(),
            }])
        );
        assert_eq!(
            manifest.season_rows,
            BTreeSet::from([ClassifierSeasonIdentityRow {
                season_number: 1,
                provider: "anilist".to_string(),
                external_id: "wrong-anilist".to_string(),
                source: "anilist_chain".to_string(),
            }])
        );
        assert_eq!(
            manifest.episode_rows,
            BTreeSet::from([ClassifierEpisodeIdentityRow {
                episode_id,
                provider: "tvdb_episode".to_string(),
                external_id: "wrong-episode".to_string(),
                source: "classifier".to_string(),
            }])
        );

        let mut future_or_malformed = evidence;
        future_or_malformed["appliedIdentityEvidence"]["schemaVersion"] = json!(3);
        assert_eq!(
            classifier_identity_manifest(&future_or_malformed),
            ClassifierIdentityManifest::default(),
            "unknown evidence schemas must never authorize identity deletion"
        );
    }

    #[test]
    fn alm8_classifier_identity_manifest_does_not_infer_causality_from_candidates() {
        let evidence = json!({
            "seriesExternalIds": [
                { "provider": "tvdb", "externalId": "candidate-tvdb", "source": "classifier" }
            ],
            "seasonExternalIds": [
                { "seasonNumber": 1, "provider": "anilist", "externalId": "candidate-anilist", "source": "anilist_chain" }
            ],
            "classifierHint": { "season": 1 },
            "classifierCandidates": {
                "hypotheses": [{
                    "candidate": {
                        "ids": { "tvdb": "candidate-tvdb", "anilist": "candidate-anilist" }
                    }
                }]
            },
            "appliedIdentityEvidence": null
        });

        assert_eq!(
            classifier_identity_manifest(&evidence),
            ClassifierIdentityManifest::default()
        );
    }

    async fn test_database() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn seed_legacy_source_identity_rows(
        pool: &AnyPool,
        fixture: &RepairFixture,
    ) -> Result<()> {
        let series_id = fixture.candidate.series_id.to_string();
        let episode_id = fixture.original_episode_ids[0].to_string();
        sqlx::query::<sqlx::Any>(
            "UPDATE series SET external_anilist = 'legacy-wrong-series' WHERE id = $1",
        )
        .bind(&series_id)
        .execute(pool)
        .await?;
        for (external_id, source) in [
            ("legacy-wrong-series", "classifier"),
            ("managed-series-keep", "managed_import"),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO series_external_ids \
                 (id, series_id, provider, external_id, confidence, source) \
                 VALUES ($1, $2, 'anilist', $3, 1.0, $4)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(&series_id)
            .bind(external_id)
            .bind(source)
            .execute(pool)
            .await?;
        }

        let season_id: String =
            sqlx::query_scalar("SELECT id FROM seasons WHERE series_id = $1 AND season_number = 1")
                .bind(&series_id)
                .fetch_one(pool)
                .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE seasons SET external_anilist = 'legacy-wrong-season' WHERE id = $1",
        )
        .bind(&season_id)
        .execute(pool)
        .await?;
        for (external_id, source) in [
            ("legacy-wrong-season", "anilist_chain"),
            ("managed-season-keep", "managed_import"),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO season_external_ids \
                 (id, season_id, provider, external_id, confidence, source) \
                 VALUES ($1, $2, 'anilist', $3, 1.0, $4)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(&season_id)
            .bind(external_id)
            .bind(source)
            .execute(pool)
            .await?;
        }
        for (external_id, source) in [
            ("legacy-wrong-episode", "classifier"),
            ("managed-episode-keep", "managed_import"),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO episode_external_ids \
                 (id, episode_id, provider, external_id, confidence, source) \
                 VALUES ($1, $2, 'tvdb_episode', $3, 1.0, $4)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(&episode_id)
            .bind(external_id)
            .bind(source)
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn alm8_legacy_manifest_captures_exact_rows_for_an_exclusive_source_series() -> Result<()>
    {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("exclusive-legacy.mkv");
        std::fs::write(&path, b"exclusive")?;
        let path = path.to_string_lossy().to_string();
        let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;
        seed_legacy_source_identity_rows(&database.pool, &fixture).await?;

        let unrelated_series_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, title) VALUES ($1, 'anime', 'Unrelated')",
        )
        .bind(unrelated_series_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series (id, title, library_type) VALUES ($1, 'Unrelated', 'anime')",
        )
        .bind(unrelated_series_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids \
             (id, series_id, provider, external_id, confidence, source) \
             VALUES ($1, $2, 'anilist', 'unrelated-classifier-keep', 1.0, 'classifier')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(unrelated_series_id.to_string())
        .execute(&database.pool)
        .await?;

        let evidence = repair_evidence_snapshot(
            &database.pool,
            fixture.candidate.media_file_id,
            fixture.candidate.series_id,
            &path,
            None,
            None,
            None,
        )
        .await?;
        let legacy = &evidence["legacyExclusiveClassifierIdentityEvidence"];
        assert_eq!(legacy["structurallyExclusive"], true);
        assert_eq!(legacy["unprotectedAtDiscovery"], true);
        assert_eq!(
            legacy["seriesMediaFileIds"],
            json!([fixture.candidate.media_file_id])
        );
        assert_eq!(
            legacy["causalIdentityRows"]["series"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            legacy["causalIdentityRows"]["seasons"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            legacy["causalIdentityRows"]["episodes"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );

        let manifest = classifier_identity_manifest(&evidence);
        assert_eq!(manifest.series_rows.len(), 1);
        assert_eq!(manifest.season_rows.len(), 1);
        assert_eq!(manifest.episode_rows.len(), 1);
        assert_eq!(
            manifest.legacy_exclusive_owner,
            Some(LegacyExclusiveIdentityOwner {
                media_file_id: fixture.candidate.media_file_id.to_string(),
                series_id: fixture.candidate.series_id.to_string(),
            })
        );

        let mut transaction = database.pool.begin().await?;
        assert!(
            legacy_identity_manifest_is_still_exclusive(
                &mut transaction,
                &manifest,
                &fixture.candidate.media_file_id.to_string(),
                &fixture.candidate.series_id.to_string(),
            )
            .await?
        );
        assert_eq!(
            remove_classifier_identity_manifest(
                &mut transaction,
                &fixture.candidate.series_id.to_string(),
                &manifest,
            )
            .await?,
            5
        );
        transaction.commit().await?;

        let source_rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT external_id, source FROM series_external_ids WHERE series_id = $1 \
             UNION ALL \
             SELECT sei.external_id, sei.source FROM season_external_ids sei \
             JOIN seasons s ON s.id = sei.season_id WHERE s.series_id = $1 \
             UNION ALL \
             SELECT eei.external_id, eei.source FROM episode_external_ids eei \
             JOIN episodes e ON e.id = eei.episode_id WHERE e.series_id = $1 \
             ORDER BY external_id",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(
            source_rows,
            vec![
                (
                    "managed-episode-keep".to_string(),
                    "managed_import".to_string()
                ),
                (
                    "managed-season-keep".to_string(),
                    "managed_import".to_string()
                ),
                (
                    "managed-series-keep".to_string(),
                    "managed_import".to_string()
                ),
            ]
        );
        let unrelated_survives: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM series_external_ids \
             WHERE series_id = $1 AND external_id = 'unrelated-classifier-keep' \
               AND source = 'classifier'",
        )
        .bind(unrelated_series_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(unrelated_survives, 1);
        Ok(())
    }

    #[tokio::test]
    async fn alm8_legacy_manifest_rejects_shared_series_and_stale_exclusivity() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("shared-legacy-a.mkv");
        std::fs::write(&path, b"shared-a")?;
        let path = path.to_string_lossy().to_string();
        let mut fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;
        seed_legacy_source_identity_rows(&database.pool, &fixture).await?;
        fixture.candidate.evidence_snapshot = repair_evidence_snapshot(
            &database.pool,
            fixture.candidate.media_file_id,
            fixture.candidate.series_id,
            &path,
            None,
            None,
            None,
        )
        .await?;
        let formerly_exclusive = classifier_identity_manifest(&fixture.candidate.evidence_snapshot);
        assert!(formerly_exclusive.legacy_exclusive_owner.is_some());
        enroll_repair_candidate(&database.pool, &fixture.candidate).await?;

        let shared_media_file_id = Uuid::new_v4();
        let shared_path = temp.path().join("shared-legacy-b.mkv");
        std::fs::write(&shared_path, b"shared-b")?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_files (id, media_item_id, path, scan_state) \
             VALUES ($1, $2, $3, 'ok')",
        )
        .bind(shared_media_file_id.to_string())
        .bind(fixture.candidate.series_id.to_string())
        .bind(shared_path.to_string_lossy().to_string())
        .execute(&database.pool)
        .await?;

        let shared_evidence = repair_evidence_snapshot(
            &database.pool,
            fixture.candidate.media_file_id,
            fixture.candidate.series_id,
            &path,
            None,
            None,
            None,
        )
        .await?;
        assert_eq!(
            shared_evidence["legacyExclusiveClassifierIdentityEvidence"]["structurallyExclusive"],
            false
        );
        assert_eq!(
            classifier_identity_manifest(&shared_evidence),
            ClassifierIdentityManifest::default()
        );
        let mut transaction = database.pool.begin().await?;
        assert!(
            !legacy_identity_manifest_is_still_exclusive(
                &mut transaction,
                &formerly_exclusive,
                &fixture.candidate.media_file_id.to_string(),
                &fixture.candidate.series_id.to_string(),
            )
            .await?
        );
        transaction.rollback().await?;
        let claimed = claim_repair_candidate(&database.pool, fixture.candidate.clone())
            .await?
            .expect("enrolled legacy repair must be claimable");
        let before = identity_link_snapshot(&database.pool, &fixture).await?;
        let mut plan = canonical_plan(fixture.candidate.series_id);
        plan.classifier_identity = formerly_exclusive;
        let error = apply_canonical_repair_plan(&database.pool, &claimed, &plan)
            .await
            .expect_err("stale exclusive ownership must not publish a canonical repair");
        assert!(format!("{error:#}").contains("identity ownership changed after enrollment"));
        assert_eq!(
            identity_link_snapshot(&database.pool, &fixture).await?,
            before
        );
        let error_text = error.to_string();
        mark_repair_retryable_for_run(&database.pool, &claimed, Some(&error_text), None, None)
            .await?;
        let retry_status: String = sqlx::query_scalar(
            "SELECT status FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(retry_status, "retryable");
        let classifier_rows: i64 = sqlx::query_scalar(
            "SELECT \
                (SELECT COUNT(*) FROM series_external_ids \
                 WHERE series_id = $1 AND source IN ('classifier', 'anilist_chain')) + \
                (SELECT COUNT(*) FROM season_external_ids sei JOIN seasons s ON s.id = sei.season_id \
                 WHERE s.series_id = $1 AND sei.source IN ('classifier', 'anilist_chain')) + \
                (SELECT COUNT(*) FROM episode_external_ids eei JOIN episodes e ON e.id = eei.episode_id \
                 WHERE e.series_id = $1 AND eei.source IN ('classifier', 'anilist_chain'))",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(classifier_rows, 3);
        Ok(())
    }

    #[tokio::test]
    async fn alm8_legacy_manifest_rejects_protected_exclusive_series() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("protected-exclusive.mkv");
        std::fs::write(&path, b"protected")?;
        let path = path.to_string_lossy().to_string();
        let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;
        seed_legacy_source_identity_rows(&database.pool, &fixture).await?;
        let override_key =
            derive_override_key("anime", &path).expect("fixture path must produce an override key");
        sqlx::query::<sqlx::Any>(
            "INSERT INTO classifier_overrides \
             (id, library_type, normalized_key, anilist_id) \
             VALUES ($1, 'anime', $2, 'manual-authority')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(override_key)
        .execute(&database.pool)
        .await?;

        let evidence = repair_evidence_snapshot(
            &database.pool,
            fixture.candidate.media_file_id,
            fixture.candidate.series_id,
            &path,
            None,
            None,
            None,
        )
        .await?;
        assert_eq!(
            evidence["legacyExclusiveClassifierIdentityEvidence"]["structurallyExclusive"],
            true
        );
        assert_eq!(
            evidence["legacyExclusiveClassifierIdentityEvidence"]["unprotectedAtDiscovery"],
            false
        );
        assert_eq!(
            classifier_identity_manifest(&evidence),
            ClassifierIdentityManifest::default()
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm8_retry_discovery_reuses_the_enrolled_identity_snapshot() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("immutable-evidence.mkv");
        std::fs::write(&path, b"immutable")?;
        let path = path.to_string_lossy().to_string();
        let mut fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;
        seed_legacy_source_identity_rows(&database.pool, &fixture).await?;
        fixture.candidate.evidence_snapshot = repair_evidence_snapshot(
            &database.pool,
            fixture.candidate.media_file_id,
            fixture.candidate.series_id,
            &path,
            None,
            None,
            None,
        )
        .await?;
        enroll_repair_candidate(&database.pool, &fixture.candidate).await?;

        sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids \
             (id, series_id, provider, external_id, confidence, source) \
             VALUES ($1, $2, 'anilist', 'post-enrollment-row', 1.0, 'classifier')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        let discovered = discover_repair_candidates(&database.pool).await?;
        let rediscovered = discovered
            .iter()
            .find(|candidate| candidate.media_file_id == fixture.candidate.media_file_id)
            .expect("enrolled candidate must be rediscovered");
        assert_eq!(
            rediscovered.evidence_snapshot,
            fixture.candidate.evidence_snapshot
        );
        assert!(
            !rediscovered
                .evidence_snapshot
                .to_string()
                .contains("post-enrollment-row")
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm8_target_legacy_identity_preserves_managed_acquisition_and_extension_authority()
    -> Result<()> {
        let database = test_database().await?;
        let canonical_ids = ExternalIds {
            anilist: Some("canonical-anilist".to_string()),
            tvdb_series: Some("canonical-tvdb".to_string()),
            ..Default::default()
        };
        for authority in ["managed", "acquisition", "extension"] {
            let media_item_id = Uuid::new_v4();
            let old_external_ids = format!("{{\"anilist\":\"{authority}-owned\"}}");
            sqlx::query::<sqlx::Any>(
                "INSERT INTO media_items (id, type, external_ids, title, year) \
                 VALUES ($1, 'tv', $2, $3, 2014)",
            )
            .bind(media_item_id.to_string())
            .bind(&old_external_ids)
            .bind(format!("{authority} target"))
            .execute(&database.pool)
            .await?;
            match authority {
                "managed" => {
                    sqlx::query::<sqlx::Any>(
                        "INSERT INTO managed_library_provenance \
                         (media_item_id, media_type, title, normalized_title, manager_provider_id) \
                         VALUES ($1, 'series', 'Managed target', 'managedtarget', \
                                 'fixture-manager')",
                    )
                    .bind(media_item_id.to_string())
                    .execute(&database.pool)
                    .await?;
                }
                owner_type => {
                    sqlx::query::<sqlx::Any>(
                        "INSERT INTO media_ownerships \
                         (ownership_id, media_item_id, owner_type, owner_role, metadata_json, active) \
                         VALUES ($1, $2, $3, 'primary', '{\"authority\":\"keep\"}', 1)",
                    )
                    .bind(Uuid::new_v4().to_string())
                    .bind(media_item_id.to_string())
                    .bind(owner_type)
                    .execute(&database.pool)
                    .await?;
                }
            }

            let mut transaction = database.pool.begin().await?;
            assert_eq!(
                repair_target_legacy_identity_cache(
                    &mut transaction,
                    &media_item_id.to_string(),
                    &canonical_ids,
                )
                .await?,
                0
            );
            transaction.commit().await?;
            let preserved: (String, String) =
                sqlx::query_as("SELECT type, external_ids FROM media_items WHERE id = $1")
                    .bind(media_item_id.to_string())
                    .fetch_one(&database.pool)
                    .await?;
            assert_eq!(preserved, ("tv".to_string(), old_external_ids));
        }
        Ok(())
    }

    async fn insert_repair_fixture(
        pool: &AnyPool,
        path: &str,
        linked_numbers: &[(i32, i32, Option<i32>)],
    ) -> Result<RepairFixture> {
        let series_id = Uuid::new_v4();
        let media_file_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, title, year) VALUES ($1, 'anime', $2, 2014)",
        )
        .bind(series_id.to_string())
        .bind("Tokyo Ghoul")
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series (id, title, year, library_type) VALUES ($1, $2, 2014, 'anime')",
        )
        .bind(series_id.to_string())
        .bind("Tokyo Ghoul")
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_files \
             (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, scan_state) \
             VALUES ($1, $2, $3, 19, 'mkv', 'h264', 'aac', 'ok')",
        )
        .bind(media_file_id.to_string())
        .bind(series_id.to_string())
        .bind(path)
        .execute(pool)
        .await?;

        let mut seasons = HashMap::<i32, Uuid>::new();
        let mut original_episode_ids = Vec::new();
        for &(season_number, episode_number, absolute_episode_number) in linked_numbers {
            let season_id = if let Some(season_id) = seasons.get(&season_number) {
                *season_id
            } else {
                let season_id = Uuid::new_v4();
                sqlx::query::<sqlx::Any>(
                    "INSERT INTO seasons (id, series_id, season_number) VALUES ($1, $2, $3)",
                )
                .bind(season_id.to_string())
                .bind(series_id.to_string())
                .bind(season_number)
                .execute(pool)
                .await?;
                seasons.insert(season_number, season_id);
                season_id
            };
            let episode_id = Uuid::new_v4();
            sqlx::query::<sqlx::Any>(
                "INSERT INTO episodes \
                 (id, series_id, season_id, season_number, episode_number, \
                  absolute_episode_number, has_file) \
                 VALUES ($1, $2, $3, $4, $5, $6, TRUE)",
            )
            .bind(episode_id.to_string())
            .bind(series_id.to_string())
            .bind(season_id.to_string())
            .bind(season_number)
            .bind(episode_number)
            .bind(absolute_episode_number)
            .execute(pool)
            .await?;
            sqlx::query::<sqlx::Any>(
                "INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)",
            )
            .bind(episode_id.to_string())
            .bind(media_file_id.to_string())
            .execute(pool)
            .await?;
            original_episode_ids.push(episode_id);
        }
        sqlx::query::<sqlx::Any>(
            "INSERT INTO classifier_resolution_state \
             (media_file_id, disposition, hint_json, candidates_json) \
             VALUES ($1, 'unresolved', $2, $3)",
        )
        .bind(media_file_id.to_string())
        .bind(serde_json::to_string(&json!({
            "libraryType": "anime",
            "title": "Tokyo Ghoul Root A"
        }))?)
        .bind(serde_json::to_string(&json!({
            "hypotheses": [{
                "candidate": {
                    "ids": { "tvdb": "wrong-tvdb", "anilist": "wrong-anilist" }
                }
            }]
        }))?)
        .execute(pool)
        .await?;

        let evidence_snapshot = json!({
            "schemaVersion": 1,
            "mediaFileId": media_file_id,
            "seriesId": series_id,
            "path": path,
            "episodeLinks": linked_numbers
                .iter()
                .zip(&original_episode_ids)
                .map(|(&(season, episode, absolute), id)| json!({
                    "episodeId": id,
                    "seasonNumber": season,
                    "episodeNumber": episode,
                    "absoluteEpisodeNumber": absolute,
                }))
                .collect::<Vec<_>>(),
            "seriesExternalIds": [],
            "seriesDirectIds": {},
            "seasonExternalIds": [],
            "episodeExternalIds": [],
            "legacyPendingReview": [],
            "classifierHint": {
                "libraryType": "anime",
                "title": "Tokyo Ghoul Root A"
            },
            "classifierCandidates": {
                "hypotheses": [{
                    "candidate": {
                        "ids": { "tvdb": "wrong-tvdb", "anilist": "wrong-anilist" }
                    }
                }]
            },
            "appliedIdentityEvidence": null,
        });
        Ok(RepairFixture {
            candidate: RepairCandidate {
                media_file_id,
                series_id,
                path: path.to_string(),
                title: "Tokyo Ghoul".to_string(),
                year: Some(2014),
                descriptor: FileDescriptor {
                    path: path.to_string(),
                    size_bytes: Some(19),
                    hash: None,
                    container: Some("mkv".to_string()),
                    video_codec: Some("h264".to_string()),
                    audio_codec: Some("aac".to_string()),
                },
                reason: "silent_s01e01,multiple_episode_links".to_string(),
                evidence_snapshot,
            },
            original_episode_ids,
        })
    }

    async fn ensure_episode(
        pool: &AnyPool,
        series_id: Uuid,
        season_number: i32,
        episode_number: i32,
        absolute_episode_number: Option<i32>,
    ) -> Result<Uuid> {
        let proposed_season_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO seasons (id, series_id, season_number) VALUES ($1, $2, $3) \
             ON CONFLICT(series_id, season_number) DO NOTHING",
        )
        .bind(proposed_season_id.to_string())
        .bind(series_id.to_string())
        .bind(season_number)
        .execute(pool)
        .await?;
        let season_id: String = sqlx::query_scalar(
            "SELECT id FROM seasons WHERE series_id = $1 AND season_number = $2",
        )
        .bind(series_id.to_string())
        .bind(season_number)
        .fetch_one(pool)
        .await?;
        let episode_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO episodes \
             (id, series_id, season_id, season_number, episode_number, \
              absolute_episode_number, has_file) \
             VALUES ($1, $2, $3, $4, $5, $6, FALSE) \
             ON CONFLICT(series_id, season_number, episode_number) DO NOTHING",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(&season_id)
        .bind(season_number)
        .bind(episode_number)
        .bind(absolute_episode_number)
        .execute(pool)
        .await?;
        let episode_id: String = sqlx::query_scalar(
            "SELECT id FROM episodes WHERE series_id = $1 AND season_number = $2 \
             AND episode_number = $3",
        )
        .bind(series_id.to_string())
        .bind(season_number)
        .bind(episode_number)
        .fetch_one(pool)
        .await?;
        Ok(Uuid::parse_str(&episode_id)?)
    }

    async fn insert_running_claim(
        pool: &AnyPool,
        candidate: RepairCandidate,
        claim_token: &str,
    ) -> Result<ClaimedRepair> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO library_anime_repairs \
             (media_file_id, repair_version, status, claim_token, attempt_count, reason, \
              evidence_snapshot_json, claimed_at, claim_expires_at) \
             VALUES ($1, $2, 'running', $3, 1, $4, $5, CURRENT_TIMESTAMP, $6)",
        )
        .bind(candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .bind(claim_token)
        .bind(&candidate.reason)
        .bind(serde_json::to_string(&candidate.evidence_snapshot)?)
        .bind(repair_claim_expiry())
        .execute(pool)
        .await?;
        Ok(ClaimedRepair {
            candidate,
            claim_token: claim_token.to_string(),
        })
    }

    fn canonical_plan(series_id: Uuid) -> CanonicalRepairPlan {
        let numbers = ResolvedEpisodeNumbers {
            season: Some(2),
            episode: Some(1),
            absolute_episode: Some(13),
        };
        let mut classification = unresolved_repair_classification();
        classification.disposition = ClassificationDisposition::Applied;
        classification.confidence = Some(1.0);
        classification.accepted_numbers = Some(numbers);
        let mut classifier_identity = ClassifierIdentityManifest::default();
        classifier_identity
            .series_rows
            .insert(ClassifierSeriesIdentityRow {
                provider: "tvdb".to_string(),
                external_id: "wrong-tvdb".to_string(),
                source: "classifier".to_string(),
            });
        classifier_identity
            .series_rows
            .insert(ClassifierSeriesIdentityRow {
                provider: "anilist".to_string(),
                external_id: "wrong-anilist".to_string(),
                source: "anilist_chain".to_string(),
            });
        classifier_identity
            .season_rows
            .insert(ClassifierSeasonIdentityRow {
                season_number: 1,
                provider: "anilist".to_string(),
                external_id: "wrong-anilist".to_string(),
                source: "anilist_chain".to_string(),
            });
        CanonicalRepairPlan {
            target_series_id: series_id,
            numbers,
            classification,
            identity: CanonicalIdentityEvidence {
                series_ids: ExternalIds {
                    tvdb_series: Some("305014".to_string()),
                    anilist: Some("1002".to_string()),
                    ..Default::default()
                },
                season_anilist_id: Some("1002".to_string()),
                tvdb_episode_id: Some("2013".to_string()),
                anidb_episode_id: Some("3013".to_string()),
            },
            classifier_identity,
            assist_json: Some(
                serde_json::to_string(&json!({
                    "animeMatchAssist": { "modelId": "fixture-qwen" }
                }))
                .expect("fixture assist must serialize"),
            ),
            identity_evidence_json: serde_json::to_string(&json!({
                "schemaVersion": APPLIED_IDENTITY_EVIDENCE_VERSION,
                "origin": "anime_match_repair",
                "envelope": "canonical_repair",
                "acceptedNumbers": {
                    "seasonNumber": 2,
                    "episodeNumber": 1,
                    "absoluteEpisodeNumber": 13,
                },
                "canonicalIdentity": {
                    "targetSeriesId": series_id,
                    "seriesExternalIds": {
                        "tvdbSeries": "305014",
                        "anilist": "1002",
                    },
                    "season": {
                        "seasonNumber": 2,
                        "anilistId": "1002",
                    },
                    "episode": {
                        "seasonNumber": 2,
                        "episodeNumber": 1,
                        "absoluteEpisodeNumber": 13,
                        "tvdbEpisodeId": "2013",
                        "anidbEpisodeId": "3013",
                    },
                },
                "causalIdentityRows": {
                    "series": [],
                    "seasons": [],
                    "episodes": [],
                }
            }))
            .expect("fixture evidence must serialize"),
        }
    }

    fn repair_model_mapping() -> super::super::AniZipMapping {
        super::super::AniZipMapping {
            ids: ExternalIds {
                anilist: Some("1002".to_string()),
                tvdb_series: Some("305014".to_string()),
                ..Default::default()
            },
            episodes: vec![super::super::AniZipEpisodeRecord {
                season_number: Some(2),
                episode_number: Some(1),
                absolute_episode_number: Some(13),
                episode_label: Some("13".to_string()),
                mainline_episode_number: Some(13),
                title: Some("New Surge".to_string()),
                overview: None,
                runtime_minutes: Some(24),
                image: None,
                tvdb_id: Some("2013".to_string()),
                anidb_eid: Some("3013".to_string()),
                raw: json!({
                    "episode": "13",
                    "seasonNumber": 2,
                    "episodeNumber": 1,
                    "absoluteEpisodeNumber": 13
                }),
            }],
            images: Vec::new(),
            titles: HashMap::from([
                ("en".to_string(), "Tokyo Ghoul Root A".to_string()),
                ("x-jat".to_string(), "Tokyo Ghoul √A".to_string()),
                ("ja".to_string(), "東京喰種トーキョーグール√A".to_string()),
            ]),
        }
    }

    fn repair_relation_root_mapping() -> super::super::AniZipMapping {
        super::super::AniZipMapping {
            ids: ExternalIds {
                imdb: Some("tt3741634".to_string()),
                tvdb_series: Some("305014".to_string()),
                anilist: Some("1001".to_string()),
                anidb: Some("root-anidb-must-not-be-series-owned".to_string()),
                mal: Some("root-mal-must-not-be-series-owned".to_string()),
                kitsu: Some("root-kitsu-must-not-be-series-owned".to_string()),
                ..Default::default()
            },
            episodes: vec![super::super::AniZipEpisodeRecord {
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                episode_label: Some("1".to_string()),
                mainline_episode_number: Some(1),
                title: Some("Tragedy".to_string()),
                overview: None,
                runtime_minutes: Some(24),
                image: None,
                tvdb_id: Some("10001".to_string()),
                anidb_eid: Some("20001".to_string()),
                raw: json!({
                    "episode": "1",
                    "seasonNumber": 1,
                    "episodeNumber": 1,
                    "absoluteEpisodeNumber": 1
                }),
            }],
            images: Vec::new(),
            titles: HashMap::from([
                ("en".to_string(), "Tokyo Ghoul".to_string()),
                ("x-jat".to_string(), "Tokyo Ghoul".to_string()),
                ("ja".to_string(), "東京喰種".to_string()),
            ]),
        }
    }

    fn repair_relation_sequel_mapping() -> super::super::AniZipMapping {
        super::super::AniZipMapping {
            ids: ExternalIds {
                tvdb_series: Some("season-tvdb-must-not-own-series".to_string()),
                anilist: Some("1002".to_string()),
                anidb: Some("season-anidb-must-not-be-series-owned".to_string()),
                mal: Some("season-mal-must-not-be-series-owned".to_string()),
                kitsu: Some("season-kitsu-must-not-be-series-owned".to_string()),
                ..Default::default()
            },
            episodes: vec![super::super::AniZipEpisodeRecord {
                season_number: Some(2),
                episode_number: Some(1),
                absolute_episode_number: Some(13),
                episode_label: Some("13".to_string()),
                mainline_episode_number: Some(13),
                title: Some("New Surge".to_string()),
                overview: None,
                runtime_minutes: Some(24),
                image: None,
                tvdb_id: Some("2013".to_string()),
                anidb_eid: Some("3013".to_string()),
                raw: json!({
                    "episode": "13",
                    "seasonNumber": 2,
                    "episodeNumber": 1,
                    "absoluteEpisodeNumber": 13
                }),
            }],
            images: Vec::new(),
            titles: HashMap::from([
                ("en".to_string(), "Tokyo Ghoul Root A".to_string()),
                ("x-jat".to_string(), "Tokyo Ghoul √A".to_string()),
                ("ja".to_string(), "東京喰種√A".to_string()),
            ]),
        }
    }

    async fn identity_link_snapshot(
        pool: &AnyPool,
        fixture: &RepairFixture,
    ) -> Result<IdentityLinkSnapshot> {
        let legacy_media_item = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT type, external_ids FROM media_items WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_one(pool)
        .await?;
        let external_owner_metadata = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT ownership_id, metadata_json FROM media_ownerships \
                 WHERE media_item_id = $1 AND owner_type = 'external' AND active = 1 \
                 ORDER BY ownership_id",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(pool)
        .await?;
        let direct_series_ids =
            sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
                "SELECT external_imdb, external_tvdb_series, external_anilist \
             FROM series WHERE id = $1",
            )
            .bind(fixture.candidate.series_id.to_string())
            .fetch_one(pool)
            .await?;
        let links = sqlx::query_as::<_, (String, i64, i64, Option<i64>)>(
            "SELECT e.id, e.season_number, e.episode_number, e.absolute_episode_number \
             FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
             WHERE ef.media_file_id = $1 \
             ORDER BY e.season_number, e.episode_number, e.id",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .fetch_all(pool)
        .await?;
        let series_ids = sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT provider, external_id, source FROM series_external_ids \
             WHERE series_id = $1 ORDER BY provider, external_id, source",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(pool)
        .await?;
        let seasons = sqlx::query_as::<_, (String, i64, Option<String>)>(
            "SELECT id, season_number, external_anilist FROM seasons WHERE series_id = $1 \
             ORDER BY season_number, id",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(pool)
        .await?;
        let season_ids = sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT sei.season_id, sei.provider, sei.external_id, sei.source \
             FROM season_external_ids sei JOIN seasons s ON s.id = sei.season_id \
             WHERE s.series_id = $1 ORDER BY sei.season_id, sei.provider, sei.external_id, sei.source",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(pool)
        .await?;
        let episode_ids = sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT eei.episode_id, eei.provider, eei.external_id, eei.source \
             FROM episode_external_ids eei JOIN episodes e ON e.id = eei.episode_id \
             WHERE e.series_id = $1 ORDER BY eei.episode_id, eei.provider, eei.external_id, eei.source",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(pool)
        .await?;
        let classifier_state =
            sqlx::query_as::<_, (String, Option<String>, Option<String>, i64, Option<String>)>(
                "SELECT disposition, hint_json, candidates_json, applied_identity_version, \
                    applied_identity_evidence_json \
             FROM classifier_resolution_state WHERE media_file_id = $1",
            )
            .bind(fixture.candidate.media_file_id.to_string())
            .fetch_optional(pool)
            .await?;
        Ok(IdentityLinkSnapshot {
            legacy_media_item,
            external_owner_metadata,
            direct_series_ids,
            links,
            series_ids,
            seasons,
            season_ids,
            episode_ids,
            classifier_state,
        })
    }

    async fn seed_mixed_identity_provenance(
        pool: &AnyPool,
        fixture: &RepairFixture,
        target_episode_id: Uuid,
    ) -> Result<()> {
        let series_id = fixture.candidate.series_id.to_string();
        sqlx::query::<sqlx::Any>(
            "UPDATE series SET external_tvdb_series = 'wrong-tvdb', \
             external_anilist = 'wrong-anilist' WHERE id = $1",
        )
        .bind(&series_id)
        .execute(pool)
        .await?;
        for (provider, external_id, source) in [
            ("tvdb", "wrong-tvdb", "classifier"),
            ("anilist", "wrong-anilist", "anilist_chain"),
            ("imdb", "tt-managed", "managed_import"),
            ("tvdb", "tvdb-override-keep", "override"),
            ("anilist", "anilist-anizip-keep", "anizip"),
            ("tmdb", "tmdb-classifier-not-causal", "classifier"),
            ("tvdb", "tvdb-classifier-not-causal", "classifier"),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO series_external_ids \
                 (id, series_id, provider, external_id, confidence, source) \
                 VALUES ($1, $2, $3, $4, 1.0, $5)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(&series_id)
            .bind(provider)
            .bind(external_id)
            .bind(source)
            .execute(pool)
            .await?;
        }

        let season_one_id: String =
            sqlx::query_scalar("SELECT id FROM seasons WHERE series_id = $1 AND season_number = 1")
                .bind(&series_id)
                .fetch_one(pool)
                .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE seasons SET external_anilist = 'wrong-anilist' WHERE id = $1",
        )
        .bind(&season_one_id)
        .execute(pool)
        .await?;
        for (external_id, source) in [
            ("wrong-anilist", "anilist_chain"),
            ("season-managed-keep", "managed_import"),
            ("season-override-keep", "override"),
            ("season-anizip-keep", "anizip"),
            ("season-classifier-not-causal", "classifier"),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO season_external_ids \
                 (id, season_id, provider, external_id, confidence, source) \
                 VALUES ($1, $2, 'anilist', $3, 1.0, $4)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(&season_one_id)
            .bind(external_id)
            .bind(source)
            .execute(pool)
            .await?;
        }
        for (provider, external_id, source) in [
            ("tvdb_episode", "wrong-tvdb-episode", "classifier"),
            ("tvdb_episode", "managed-tvdb-episode", "managed_import"),
            ("anidb_episode", "wrong-anidb-episode", "anilist_chain"),
            ("anidb_episode", "anizip-anidb-episode", "anizip"),
            (
                "tvdb_episode",
                "tvdb-episode-classifier-not-causal",
                "classifier",
            ),
            (
                "anidb_episode",
                "anidb-episode-classifier-not-causal",
                "anilist_chain",
            ),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO episode_external_ids \
                 (id, episode_id, provider, external_id, confidence, source) \
                 VALUES ($1, $2, $3, $4, 1.0, $5)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(target_episode_id.to_string())
            .bind(provider)
            .bind(external_id)
            .bind(source)
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn alm8_repair_replaces_s01e01_and_multiple_links_once_without_touching_file()
    -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("Tokyo Ghoul Root A - 13.mkv");
        let original_bytes = b"fixture-video-bytes";
        std::fs::write(&path, original_bytes)?;
        let path = path.to_string_lossy().to_string();
        let mut fixture =
            insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1)), (1, 2, Some(2))])
                .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET type = 'tv', external_ids = \
             '{\"anilist\":\"legacy-wrong\",\"imdb\":\"tt-keep\",\"legacyCatalog\":\"keep-42\"}' \
             WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        let external_owner_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_ownerships \
             (ownership_id, media_item_id, owner_type, owner_role, metadata_json, active) \
             VALUES ($1, $2, 'external', 'primary', \
                     '{\"mediaType\":\"tv\",\"externalIds\":{\"anilist\":\"wrong\",\"tmdb\":\"keep-tmdb\",\"fanIndex\":\"keep-fan\"},\"custom\":\"keep\"}', 1)",
        )
        .bind(external_owner_id.to_string())
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        let review_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO review_queue \
             (id, media_file_id, status, confidence, hint_json, candidates_json) \
             VALUES ($1, $2, 'pending', 0.5, '{}', '{}')",
        )
        .bind(review_id.to_string())
        .bind(fixture.candidate.media_file_id.to_string())
        .execute(&database.pool)
        .await?;
        fixture.candidate.evidence_snapshot["legacyPendingReview"] = json!([{ "id": review_id }]);

        let stale_movie_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, external_ids, title, year, metadata_json) \
             VALUES ($1, 'movie', '{}', 'Stale Placeholder', 2014, \
                     '{\"classifierPlaceholder\":true}')",
        )
        .bind(stale_movie_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movies (id, title, year, metadata_json) \
             VALUES ($1, 'Stale Placeholder', 2014, '{\"classifierPlaceholder\":true}')",
        )
        .bind(stale_movie_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(stale_movie_id.to_string())
        .bind(fixture.candidate.media_file_id.to_string())
        .execute(&database.pool)
        .await?;

        let real_movie_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, external_ids, title, year) \
             VALUES ($1, 'movie', '{\"tmdb\":\"real-42\"}', 'Real Movie', 2014)",
        )
        .bind(real_movie_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movies (id, title, year, external_tmdb) \
             VALUES ($1, 'Real Movie', 2014, 'real-42')",
        )
        .bind(real_movie_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(real_movie_id.to_string())
        .bind(fixture.candidate.media_file_id.to_string())
        .execute(&database.pool)
        .await?;

        let managed_movie_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, external_ids, title, year, metadata_json) \
             VALUES ($1, 'movie', '{}', 'Managed Movie', 2014, \
                     '{\"classifierPlaceholder\":true}')",
        )
        .bind(managed_movie_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movies (id, title, year, metadata_json) \
             VALUES ($1, 'Managed Movie', 2014, '{\"classifierPlaceholder\":true}')",
        )
        .bind(managed_movie_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_library_provenance \
             (media_item_id, media_type, title, normalized_title, manager_provider_id) \
             VALUES ($1, 'movie', 'Managed Movie', 'managedmovie', 'fixture-manager')",
        )
        .bind(managed_movie_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)",
        )
        .bind(managed_movie_id.to_string())
        .bind(fixture.candidate.media_file_id.to_string())
        .execute(&database.pool)
        .await?;
        let target_episode_id =
            ensure_episode(&database.pool, fixture.candidate.series_id, 2, 1, Some(13)).await?;
        let claimed = insert_running_claim(
            &database.pool,
            fixture.candidate.clone(),
            "replacement-claim",
        )
        .await?;
        let plan = canonical_plan(fixture.candidate.series_id);

        assert_eq!(
            apply_canonical_repair_plan(&database.pool, &claimed, &plan).await?,
            ApplyRepairOutcome::Applied {
                links: 1,
                identities: 10,
            }
        );

        let legacy_identity: (String, String, String) = sqlx::query_as(
            "SELECT mi.type, mi.external_ids, mo.metadata_json FROM media_items mi \
             JOIN media_ownerships mo ON mo.media_item_id = mi.id \
             WHERE mi.id = $1 AND mo.ownership_id = $2",
        )
        .bind(fixture.candidate.series_id.to_string())
        .bind(external_owner_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(legacy_identity.0, "anime");
        assert_eq!(
            serde_json::from_str::<Value>(&legacy_identity.1)?,
            json!({
                "anilist": "1002",
                "imdb": "tt-keep",
                "legacyCatalog": "keep-42",
                "tvdbSeries": "305014"
            })
        );
        let owner_metadata: Value = serde_json::from_str(&legacy_identity.2)?;
        assert_eq!(owner_metadata["mediaType"], "anime");
        assert_eq!(owner_metadata["title"], "Tokyo Ghoul");
        assert_eq!(owner_metadata["year"], 2014);
        assert_eq!(owner_metadata["custom"], "keep");
        assert_eq!(
            owner_metadata["externalIds"],
            json!({
                "anilist": "1002",
                "fanIndex": "keep-fan",
                "tmdb": "keep-tmdb",
                "tvdbSeries": "305014"
            })
        );

        let linked = sqlx::query_as::<_, (String, i64, i64, Option<i64>)>(
            "SELECT e.id, e.season_number, e.episode_number, e.absolute_episode_number \
             FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
             WHERE ef.media_file_id = $1",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(
            linked,
            vec![(target_episode_id.to_string(), 2, 1, Some(13))]
        );
        for old_episode_id in &fixture.original_episode_ids {
            let has_file: i64 =
                sqlx::query_scalar("SELECT CAST(has_file AS INTEGER) FROM episodes WHERE id = $1")
                    .bind(old_episode_id.to_string())
                    .fetch_one(&database.pool)
                    .await?;
            assert_eq!(has_file, 0);
        }
        let target_has_file: i64 =
            sqlx::query_scalar("SELECT CAST(has_file AS INTEGER) FROM episodes WHERE id = $1")
                .bind(target_episode_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(target_has_file, 1);
        let review_status: String =
            sqlx::query_scalar("SELECT status FROM review_queue WHERE id = $1")
                .bind(review_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(review_status, "applied");
        let stale_movie_links: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM movie_files WHERE media_file_id = $1")
                .bind(fixture.candidate.media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(stale_movie_links, 0);
        let stale_movie_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM movies WHERE id = $1")
            .bind(stale_movie_id.to_string())
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(stale_movie_rows, 0);
        let stale_media_item_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM media_items WHERE id = $1")
                .bind(stale_movie_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(stale_media_item_rows, 0);
        let surviving_movies: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM movies WHERE id IN ($1, $2)")
                .bind(real_movie_id.to_string())
                .bind(managed_movie_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(surviving_movies, 2);
        let surviving_media_items: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM media_items WHERE id IN ($1, $2)")
                .bind(real_movie_id.to_string())
                .bind(managed_movie_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(surviving_media_items, 2);
        assert_eq!(std::fs::read(&path)?, original_bytes);

        let ledger = sqlx::query_as::<_, (String, i64, i64, Option<String>, Option<String>)>(
            "SELECT status, repaired_link_count, repaired_identity_count, claim_token, \
                    CAST(completed_at AS TEXT) \
             FROM library_anime_repairs WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(ledger.0, "completed");
        assert_eq!(ledger.1, 1);
        assert_eq!(ledger.2, 10);
        assert_eq!(ledger.3, None);
        assert!(ledger.4.is_some());

        let first_completion = identity_link_snapshot(&database.pool, &fixture).await?;
        let counts = run_anime_library_repair_iteration(
            &database.pool,
            None,
            &ClassifierPipeline::new(),
            &AnimeMatchingService::disabled(),
            86_400,
            AnimeLibraryRepairTrigger::Startup,
        )
        .await?;
        assert_eq!(counts, RepairIterationCounts::default());
        assert_eq!(
            identity_link_snapshot(&database.pool, &fixture).await?,
            first_completion
        );
        assert_eq!(std::fs::read(&path)?, original_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn alm8_full_worker_discovers_models_and_applies_canonical_repair() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("[Group] Tokyo Ghoul Root A - 13.mkv");
        let original_bytes = b"full-worker-repair";
        std::fs::write(&path, original_bytes)?;
        let path = path.to_string_lossy().to_string();
        let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;

        // Historical classifier evidence supplies a provisional lookup key only. The cached
        // canonical mapping and validated one-target model response must drive the repair.
        sqlx::query::<sqlx::Any>(
            "UPDATE series SET external_anilist = 'wrong-anilist' WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids \
             (id, series_id, provider, external_id, confidence, source) \
             VALUES ($1, $2, 'anilist', 'wrong-anilist', 0.4, 'classifier')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        super::super::persist_cached_anizip_mapping(
            &database.pool,
            "wrong-anilist",
            &repair_model_mapping(),
        )
        .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let matcher = AnimeMatchingService::with_engine(Arc::new(RepairMatchEngine {
            calls: calls.clone(),
        }));

        let counts = run_anime_library_repair_iteration(
            &database.pool,
            None,
            &ClassifierPipeline::new(),
            &matcher,
            86_400,
            AnimeLibraryRepairTrigger::ModelActivated,
        )
        .await?;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.scanned, 1);
        assert_eq!(counts.claimed, 1);
        assert_eq!(counts.completed, 1);
        assert_eq!(counts.retryable, 0);
        let links: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(
            "SELECT e.season_number, e.episode_number, e.absolute_episode_number \
             FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
             WHERE ef.media_file_id = $1",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(links, vec![(2, 1, Some(13))]);
        let state: (String, String, String) = sqlx::query_as(
            "SELECT crs.disposition, crs.anime_match_assist_json, lr.status \
             FROM classifier_resolution_state crs \
             JOIN library_anime_repairs lr ON lr.media_file_id = crs.media_file_id \
             WHERE crs.media_file_id = $1 AND lr.repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(state.0, "applied");
        assert_eq!(state.2, "completed");
        let assist: Value = serde_json::from_str(&state.1)?;
        assert_eq!(assist["animeMatchAssist"]["source"], "local_model");
        assert_eq!(assist["animeMatchAssist"]["result"], "matched");
        let repaired_series_identity: (Option<String>, i64, i64, i64) = sqlx::query_as(
            "SELECT s.external_anilist, \
                    (SELECT COUNT(*) FROM series_external_ids sei \
                     WHERE sei.series_id = s.id AND sei.provider = 'anilist' \
                       AND sei.external_id = 'wrong-anilist' \
                       AND sei.source = 'classifier'), \
                    (SELECT COUNT(*) FROM series_external_ids sei \
                     WHERE sei.series_id = s.id AND sei.provider = 'anilist' \
                       AND sei.external_id = '1002' \
                       AND sei.source = 'anime_match_repair'), \
                    (SELECT COUNT(*) FROM season_external_ids sei \
                     JOIN seasons season ON season.id = sei.season_id \
                     WHERE season.series_id = s.id AND season.season_number = 2 \
                       AND sei.provider = 'anilist' AND sei.external_id = '1002' \
                       AND sei.source = 'anime_match_repair') \
             FROM series s WHERE s.id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(repaired_series_identity, (None, 0, 0, 1));
        assert_eq!(std::fs::read(&path)?, original_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn alm8_model_readiness_dispatch_retries_preexisting_ledger_row() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp
            .path()
            .join("[Group] Tokyo Ghoul Root A - 13 readiness retry.mkv");
        std::fs::write(&path, b"model-readiness-retry")?;
        let path = path.to_string_lossy().to_string();
        let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;

        sqlx::query::<sqlx::Any>(
            "UPDATE series SET external_anilist = 'wrong-anilist' WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids \
             (id, series_id, provider, external_id, confidence, source) \
             VALUES ($1, $2, 'anilist', 'wrong-anilist', 0.4, 'classifier')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        super::super::persist_cached_anizip_mapping(
            &database.pool,
            "wrong-anilist",
            &repair_model_mapping(),
        )
        .await?;
        enroll_repair_candidate(&database.pool, &fixture.candidate).await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repairs SET status = 'retryable' \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;

        let calls = Arc::new(AtomicUsize::new(0));
        let matcher = AnimeMatchingService::with_engine(Arc::new(RepairMatchEngine {
            calls: calls.clone(),
        }));
        let classifier = ClassifierPipeline::new();
        let mut observed_activation_generation = 0;
        let counts = super::super::anime_repair_loop::dispatch_published_model_activation_for_test(
            &mut observed_activation_generation,
            1,
            |trigger| {
                run_anime_library_repair_iteration(
                    &database.pool,
                    None,
                    &classifier,
                    &matcher,
                    86_400,
                    trigger,
                )
            },
        )
        .await
        .expect("a newly published model generation must dispatch repair")?;

        assert_eq!(observed_activation_generation, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.scanned, 1);
        assert_eq!(counts.claimed, 1);
        assert_eq!(counts.completed, 1);
        assert_eq!(counts.retryable, 0);
        let ledger: (String, i64) = sqlx::query_as(
            "SELECT status, attempt_count FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(ledger, ("completed".to_string(), 1));
        Ok(())
    }

    #[tokio::test]
    async fn alm8_tvdb_only_historical_repair_uses_bridge_relation_context_and_root_identity()
    -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp
            .path()
            .join("[Group] Tokyo Ghoul Root A unresolved release.mkv");
        let original_bytes = b"tvdb-bridge-repair";
        std::fs::write(&path, original_bytes)?;
        let path = path.to_string_lossy().to_string();
        let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;

        // These are the historical classifier-owned identities being repaired.
        // The fresh classifier provides only TVDB evidence; AniList identity and
        // sequel context must come from the automatic bridge and relation graph.
        sqlx::query::<sqlx::Any>(
            "UPDATE series SET external_tvdb_series = 'stale-tvdb', \
             external_anilist = 'stale-anilist' WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        for (provider, external_id, source) in [
            ("tvdb", "stale-tvdb", "classifier"),
            ("anilist", "stale-anilist", "anilist_chain"),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO series_external_ids \
                 (id, series_id, provider, external_id, confidence, source) \
                 VALUES ($1, $2, $3, $4, 0.4, $5)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(fixture.candidate.series_id.to_string())
            .bind(provider)
            .bind(external_id)
            .bind(source)
            .execute(&database.pool)
            .await?;
        }
        let provisional_collision_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, title, year) \
             VALUES ($1, 'anime', 'Provisional Collision', 2014)",
        )
        .bind(provisional_collision_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series \
             (id, title, year, library_type, external_anilist) \
             VALUES ($1, 'Provisional Collision', 2014, 'anime', '1001')",
        )
        .bind(provisional_collision_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids \
             (id, series_id, provider, external_id, confidence, source) \
             VALUES ($1, $2, 'anilist', '1001', 0.4, 'classifier')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(provisional_collision_id.to_string())
        .execute(&database.pool)
        .await?;
        super::super::persist_cached_anizip_mapping(
            &database.pool,
            "1001",
            &repair_relation_root_mapping(),
        )
        .await?;
        super::super::persist_cached_anizip_mapping(
            &database.pool,
            "1002",
            &repair_relation_sequel_mapping(),
        )
        .await?;

        let (tvdb_url, tvdb_shutdown) = start_tvdb_repair_server().await?;
        let (anilist_url, anilist_shutdown) = start_anilist_repair_server().await?;
        let linker = LinkerService::new(crate::config::ClassifierConfig {
            tvdb_base_url: tvdb_url,
            tvdb_api_key: Some("repair-api-key".to_string()),
            anizip_base_url: String::new(),
            request_timeout_seconds: 5,
        })?;
        let anilist_bridge =
            elixir_classifier::identify::anilist::AniListIdentifier::new(anilist_url, 5);
        let classifier = ClassifierPipeline::new()
            .register_hint_parser(Arc::new(TvdbOnlyRepairHintParser { path: path.clone() }))
            .register_identifier_provider(Arc::new(TvdbOnlyRepairIdentifier));
        let model_calls = Arc::new(AtomicUsize::new(0));
        let matcher =
            AnimeMatchingService::with_engine(Arc::new(RelationContextRepairMatchEngine {
                calls: model_calls.clone(),
            }));

        let counts = run_anime_library_repair_iteration_with_anilist(
            &database.pool,
            Some(&linker),
            &classifier,
            &anilist_bridge,
            &matcher,
            86_400,
            AnimeLibraryRepairTrigger::ModelActivated,
        )
        .await?;
        let _ = tvdb_shutdown.send(());
        let _ = anilist_shutdown.send(());

        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.completed, 1);
        assert_eq!(counts.retryable, 0);
        let links: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(
            "SELECT e.season_number, e.episode_number, e.absolute_episode_number \
             FROM episode_files ef JOIN episodes e ON e.id = ef.episode_id \
             WHERE ef.media_file_id = $1",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert_eq!(links, vec![(2, 1, Some(13))]);
        let applied_evidence: (i64, String) = sqlx::query_as(
            "SELECT applied_identity_version, applied_identity_evidence_json \
             FROM classifier_resolution_state WHERE media_file_id = $1",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(applied_evidence.0, 2);
        let applied_evidence: Value = serde_json::from_str(&applied_evidence.1)?;
        assert_eq!(applied_evidence["schemaVersion"], 2);
        assert_eq!(applied_evidence["envelope"], "canonical_repair");
        assert_eq!(
            applied_evidence["canonicalIdentity"]["seriesExternalIds"]["anilist"],
            "1001"
        );
        assert_eq!(
            applied_evidence["causalIdentityRows"],
            json!({ "series": [], "seasons": [], "episodes": [] })
        );

        let direct_ids: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT external_imdb, external_tvdb_series, external_anilist \
             FROM series WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            direct_ids,
            (
                Some("tt3741634".to_string()),
                Some("305014".to_string()),
                Some("1001".to_string()),
            )
        );
        let repaired_owner: String =
            sqlx::query_scalar("SELECT media_item_id FROM media_files WHERE id = $1")
                .bind(fixture.candidate.media_file_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(repaired_owner, fixture.candidate.series_id.to_string());
        let provisional_collision: (Option<String>, i64, i64) = sqlx::query_as(
            "SELECT s.external_anilist, \
                    (SELECT COUNT(*) FROM media_files mf WHERE mf.media_item_id = s.id), \
                    (SELECT COUNT(*) FROM series_external_ids sei \
                     WHERE sei.series_id = s.id AND sei.provider = 'anilist' \
                       AND sei.external_id = '1001' AND sei.source = 'classifier') \
             FROM series s WHERE s.id = $1",
        )
        .bind(provisional_collision_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(provisional_collision, (None, 0, 1));
        let series_ids: Vec<(String, String)> = sqlx::query_as(
            "SELECT provider, external_id FROM series_external_ids \
             WHERE series_id = $1 ORDER BY provider, external_id",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert!(series_ids.contains(&("anilist".to_string(), "1001".to_string())));
        assert!(series_ids.contains(&("tvdb".to_string(), "305014".to_string())));
        assert!(!series_ids.iter().any(|(provider, external_id)| {
            matches!(provider.as_str(), "anidb" | "mal" | "kitsu")
                || matches!(
                    external_id.as_str(),
                    "stale-tvdb" | "stale-anilist" | "season-tvdb-must-not-own-series"
                )
        }));
        let season_anilist: Option<String> = sqlx::query_scalar(
            "SELECT external_anilist FROM seasons \
             WHERE series_id = $1 AND season_number = 2",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(season_anilist.as_deref(), Some("1002"));
        let episode_ids: Vec<(String, String)> = sqlx::query_as(
            "SELECT eei.provider, eei.external_id FROM episode_external_ids eei \
             JOIN episodes e ON e.id = eei.episode_id \
             WHERE e.series_id = $1 AND e.season_number = 2 AND e.episode_number = 1 \
             ORDER BY eei.provider",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert!(episode_ids.contains(&("tvdb_episode".to_string(), "2013".to_string())));
        assert!(episode_ids.contains(&("anidb_episode".to_string(), "3013".to_string())));
        assert_eq!(std::fs::read(&path)?, original_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn alm8_repair_replaces_only_causal_classifier_identity_evidence() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("Tokyo Ghoul Root A 13.mkv");
        std::fs::write(&path, b"identity-fixture")?;
        let path = path.to_string_lossy().to_string();
        let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;
        let target_episode_id =
            ensure_episode(&database.pool, fixture.candidate.series_id, 2, 1, Some(13)).await?;
        seed_mixed_identity_provenance(&database.pool, &fixture, target_episode_id).await?;
        let claimed = insert_running_claim(
            &database.pool,
            fixture.candidate.clone(),
            "identity-cleanup-claim",
        )
        .await?;

        let mut plan = canonical_plan(fixture.candidate.series_id);
        for (provider, external_id, source) in [
            ("tvdb_episode", "wrong-tvdb-episode", "classifier"),
            ("anidb_episode", "wrong-anidb-episode", "anilist_chain"),
        ] {
            plan.classifier_identity
                .episode_rows
                .insert(ClassifierEpisodeIdentityRow {
                    episode_id: target_episode_id.to_string(),
                    provider: provider.to_string(),
                    external_id: external_id.to_string(),
                    source: source.to_string(),
                });
        }
        let outcome = apply_canonical_repair_plan(&database.pool, &claimed, &plan).await?;
        assert!(matches!(outcome, ApplyRepairOutcome::Applied { .. }));

        let direct = sqlx::query_as::<_, (Option<String>, Option<String>)>(
            "SELECT external_tvdb_series, external_anilist FROM series WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            direct,
            (Some("305014".to_string()), Some("1002".to_string()))
        );

        let series_rows = sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT provider, external_id, source FROM series_external_ids WHERE series_id = $1 \
             ORDER BY provider, external_id, source",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert!(!series_rows.iter().any(|(_, id, source)| {
            matches!(id.as_str(), "wrong-tvdb" | "wrong-anilist")
                && matches!(source.as_deref(), Some("classifier" | "anilist_chain"))
        }));
        for survivor in [
            ("imdb", "tt-managed", "managed_import"),
            ("tvdb", "tvdb-override-keep", "override"),
            ("anilist", "anilist-anizip-keep", "anizip"),
            ("tmdb", "tmdb-classifier-not-causal", "classifier"),
            ("tvdb", "tvdb-classifier-not-causal", "classifier"),
        ] {
            assert!(series_rows.iter().any(|(provider, id, source)| {
                provider == survivor.0 && id == survivor.1 && source.as_deref() == Some(survivor.2)
            }));
        }
        for canonical in [
            ("tvdb", "305014", "anime_match_repair"),
            ("anilist", "1002", "anime_match_repair"),
        ] {
            assert!(series_rows.iter().any(|(provider, id, source)| {
                provider == canonical.0
                    && id == canonical.1
                    && source.as_deref() == Some(canonical.2)
            }));
        }

        let season_rows = sqlx::query_as::<_, (i64, Option<String>, String, Option<String>)>(
            "SELECT s.season_number, s.external_anilist, sei.external_id, sei.source \
             FROM seasons s JOIN season_external_ids sei ON sei.season_id = s.id \
             WHERE s.series_id = $1 ORDER BY s.season_number, sei.external_id, sei.source",
        )
        .bind(fixture.candidate.series_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert!(!season_rows.iter().any(|(_, _, id, source)| {
            id == "wrong-anilist" && source.as_deref() == Some("anilist_chain")
        }));
        for survivor in [
            ("season-managed-keep", "managed_import"),
            ("season-override-keep", "override"),
            ("season-anizip-keep", "anizip"),
            ("season-classifier-not-causal", "classifier"),
        ] {
            assert!(season_rows.iter().any(|(_, _, id, source)| {
                id == survivor.0 && source.as_deref() == Some(survivor.1)
            }));
        }
        assert!(season_rows.iter().any(|(season, direct, id, source)| {
            *season == 2
                && direct.as_deref() == Some("1002")
                && id == "1002"
                && source.as_deref() == Some("anime_match_repair")
        }));

        let episode_rows = sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT provider, external_id, source FROM episode_external_ids \
             WHERE episode_id = $1 ORDER BY provider, external_id, source",
        )
        .bind(target_episode_id.to_string())
        .fetch_all(&database.pool)
        .await?;
        assert!(!episode_rows.iter().any(|(_, id, source)| {
            matches!(id.as_str(), "wrong-tvdb-episode" | "wrong-anidb-episode")
                && matches!(source.as_deref(), Some("classifier" | "anilist_chain"))
        }));
        for survivor in [
            ("tvdb_episode", "managed-tvdb-episode", "managed_import"),
            ("anidb_episode", "anizip-anidb-episode", "anizip"),
            (
                "tvdb_episode",
                "tvdb-episode-classifier-not-causal",
                "classifier",
            ),
            (
                "anidb_episode",
                "anidb-episode-classifier-not-causal",
                "anilist_chain",
            ),
        ] {
            assert!(episode_rows.iter().any(|(provider, id, source)| {
                provider == survivor.0 && id == survivor.1 && source.as_deref() == Some(survivor.2)
            }));
        }
        for canonical in [("tvdb_episode", "2013"), ("anidb_episode", "3013")] {
            assert!(episode_rows.iter().any(|(provider, id, source)| {
                provider == canonical.0
                    && id == canonical.1
                    && source.as_deref() == Some("anime_match_repair")
            }));
        }
        Ok(())
    }

    #[tokio::test]
    async fn alm8_model_disabled_retry_is_byte_for_byte_non_destructive() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("ambiguous-release-without-numbers.mkv");
        let original_bytes = b"ambiguous-video";
        std::fs::write(&path, original_bytes)?;
        let path = path.to_string_lossy().to_string();
        let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;
        let unlinked_target =
            ensure_episode(&database.pool, fixture.candidate.series_id, 2, 1, Some(13)).await?;
        seed_mixed_identity_provenance(&database.pool, &fixture, unlinked_target).await?;
        let before = identity_link_snapshot(&database.pool, &fixture).await?;

        let counts = run_anime_library_repair_iteration(
            &database.pool,
            None,
            &ClassifierPipeline::new(),
            &AnimeMatchingService::disabled(),
            86_400,
            AnimeLibraryRepairTrigger::Startup,
        )
        .await?;
        assert_eq!(counts.claimed, 1);
        assert_eq!(counts.retryable, 1);
        assert_eq!(counts.completed, 0);
        assert_eq!(counts.repaired_links, 0);
        assert_eq!(counts.repaired_identities, 0);
        assert_eq!(
            identity_link_snapshot(&database.pool, &fixture).await?,
            before
        );
        assert_eq!(std::fs::read(&path)?, original_bytes);
        let ledger = sqlx::query_as::<_, (String, i64, i64, i64, Option<String>)>(
            "SELECT status, attempt_count, repaired_link_count, repaired_identity_count, claim_token \
             FROM library_anime_repairs WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(ledger, ("retryable".to_string(), 1, 0, 0, None));
        Ok(())
    }

    #[tokio::test]
    async fn alm8_repair_transaction_rolls_back_every_mutation_on_failure() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("rollback-release.mkv");
        let original_bytes = b"rollback-video";
        std::fs::write(&path, original_bytes)?;
        let path = path.to_string_lossy().to_string();
        let mut fixture =
            insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1)), (1, 2, Some(2))])
                .await?;
        let review_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO review_queue (id, media_file_id, status, hint_json, candidates_json) \
             VALUES ($1, $2, 'pending', '{}', '{}')",
        )
        .bind(review_id.to_string())
        .bind(fixture.candidate.media_file_id.to_string())
        .execute(&database.pool)
        .await?;
        fixture.candidate.evidence_snapshot["legacyPendingReview"] = json!([{ "id": review_id }]);
        sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET type = 'tv', external_ids = \
             '{\"anilist\":\"rollback-old\"}' WHERE id = $1",
        )
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_ownerships \
             (ownership_id, media_item_id, owner_type, owner_role, metadata_json, active) \
             VALUES ($1, $2, 'external', 'primary', \
                     '{\"mediaType\":\"tv\",\"externalIds\":{\"anilist\":\"rollback-old\"}}', 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        let target_episode =
            ensure_episode(&database.pool, fixture.candidate.series_id, 2, 1, Some(13)).await?;
        seed_mixed_identity_provenance(&database.pool, &fixture, target_episode).await?;
        let claimed =
            insert_running_claim(&database.pool, fixture.candidate.clone(), "rollback-claim")
                .await?;
        let before = identity_link_snapshot(&database.pool, &fixture).await?;

        let error = apply_canonical_repair_plan_inner(
            &database.pool,
            &claimed,
            &canonical_plan(fixture.candidate.series_id),
            true,
        )
        .await
        .expect_err("injected pre-commit failure must abort repair");
        assert!(format!("{error:#}").contains("injected anime repair failure"));
        assert_eq!(
            identity_link_snapshot(&database.pool, &fixture).await?,
            before
        );
        assert_eq!(std::fs::read(&path)?, original_bytes);
        let review_status: String =
            sqlx::query_scalar("SELECT status FROM review_queue WHERE id = $1")
                .bind(review_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(review_status, "pending");
        let ledger = sqlx::query_as::<_, (String, Option<String>, i64, i64)>(
            "SELECT status, claim_token, repaired_link_count, repaired_identity_count \
             FROM library_anime_repairs WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            ledger,
            (
                "running".to_string(),
                Some("rollback-claim".to_string()),
                0,
                0,
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm8_repair_protects_override_managed_owned_and_imported_files() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let override_path = temp.path().join("override.mkv");
        let movie_override_path = temp.path().join("movie-override.mkv");
        let managed_path = temp.path().join("managed.mkv");
        let acquisition_owned_path = temp.path().join("acquisition-owned.mkv");
        let imported_path = temp.path().join("imported.mkv");
        for path in [
            &override_path,
            &movie_override_path,
            &managed_path,
            &acquisition_owned_path,
            &imported_path,
        ] {
            std::fs::write(path, b"protected-video")?;
        }
        let override_fixture = insert_repair_fixture(
            &database.pool,
            &override_path.to_string_lossy(),
            &[(1, 1, Some(1))],
        )
        .await?;
        let movie_override_fixture = insert_repair_fixture(
            &database.pool,
            &movie_override_path.to_string_lossy(),
            &[(1, 1, Some(1))],
        )
        .await?;
        let managed_fixture = insert_repair_fixture(
            &database.pool,
            &managed_path.to_string_lossy(),
            &[(1, 1, Some(1))],
        )
        .await?;
        let acquisition_owned_fixture = insert_repair_fixture(
            &database.pool,
            &acquisition_owned_path.to_string_lossy(),
            &[(1, 1, Some(1))],
        )
        .await?;
        let imported_fixture = insert_repair_fixture(
            &database.pool,
            &imported_path.to_string_lossy(),
            &[(1, 1, Some(1))],
        )
        .await?;

        let override_key = derive_override_key("anime", &override_fixture.candidate.path)
            .expect("fixture path must derive an override key");
        sqlx::query::<sqlx::Any>(
            "INSERT INTO classifier_overrides (id, library_type, normalized_key, anilist_id) \
             VALUES ($1, 'anime', $2, 'manual-anilist')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(override_key)
        .execute(&database.pool)
        .await?;
        let movie_override_key =
            derive_override_key("movie", &movie_override_fixture.candidate.path)
                .expect("fixture path must derive a movie override key");
        sqlx::query::<sqlx::Any>(
            "INSERT INTO classifier_overrides (id, library_type, normalized_key, imdb_id) \
             VALUES ($1, 'movie', $2, 'tt-manual-movie')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(movie_override_key)
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_library_provenance \
             (media_item_id, media_type, title, normalized_title, manager_provider_id) \
             VALUES ($1, 'anime', 'Tokyo Ghoul', 'tokyoghoul', 'fixture-manager')",
        )
        .bind(managed_fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;
        // This is the crash/interleaving state after acquisition ownership is
        // durable but before the separate import-link bookkeeping reaches its
        // final `imported` state. Ownership alone must fence repair.
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_ownerships \
             (ownership_id, media_item_id, owner_type, owner_role, active) \
             VALUES ($1, $2, 'acquisition', 'primary', 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(acquisition_owned_fixture.candidate.series_id.to_string())
        .execute(&database.pool)
        .await?;

        let release_id = Uuid::new_v4();
        let release_job_id = Uuid::new_v4();
        let import_run_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_releases \
             (release_id, source_extension_id, media_type, title, release_title, source, \
              source_kind, fingerprint, release_kind, resolver_kind, resolver_version, confidence) \
             VALUES ($1, 'fixture.source', 'anime', 'Tokyo Ghoul', 'Tokyo Ghoul 13', \
                     'fixture', 'torrent', $2, 'episode', 'deterministic', '1', 'verified')",
        )
        .bind(release_id.to_string())
        .bind(format!("fixture-{release_id}"))
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_release_jobs \
             (release_job_id, release_id, route_logical_id, state) \
             VALUES ($1, $2, 'fixture.route', 'completed')",
        )
        .bind(release_job_id.to_string())
        .bind(release_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_import_runs \
             (import_run_id, release_id, release_job_id, route_logical_id, state) \
             VALUES ($1, $2, $3, 'fixture.route', 'completed')",
        )
        .bind(import_run_id.to_string())
        .bind(release_id.to_string())
        .bind(release_job_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_import_file_links \
             (import_link_id, import_run_id, release_id, local_path, media_file_id, \
              episode_id, state, verification_state) \
             VALUES ($1, $2, $3, $4, $5, $6, 'imported', 'verified')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(import_run_id.to_string())
        .bind(release_id.to_string())
        .bind(&imported_fixture.candidate.path)
        .bind(imported_fixture.candidate.media_file_id.to_string())
        .bind(imported_fixture.original_episode_ids[0].to_string())
        .execute(&database.pool)
        .await?;

        let before = [
            identity_link_snapshot(&database.pool, &override_fixture).await?,
            identity_link_snapshot(&database.pool, &movie_override_fixture).await?,
            identity_link_snapshot(&database.pool, &managed_fixture).await?,
            identity_link_snapshot(&database.pool, &acquisition_owned_fixture).await?,
            identity_link_snapshot(&database.pool, &imported_fixture).await?,
        ];
        let counts = run_anime_library_repair_iteration(
            &database.pool,
            None,
            &ClassifierPipeline::new(),
            &AnimeMatchingService::disabled(),
            86_400,
            AnimeLibraryRepairTrigger::Startup,
        )
        .await?;
        assert_eq!(counts.protected, 5);
        assert_eq!(counts.claimed, 0);
        assert_eq!(counts.completed, 0);
        for (index, fixture) in [
            &override_fixture,
            &movie_override_fixture,
            &managed_fixture,
            &acquisition_owned_fixture,
            &imported_fixture,
        ]
        .into_iter()
        .enumerate()
        {
            let status: String = sqlx::query_scalar(
                "SELECT status FROM library_anime_repairs \
                 WHERE media_file_id = $1 AND repair_version = $2",
            )
            .bind(fixture.candidate.media_file_id.to_string())
            .bind(ANIME_LIBRARY_REPAIR_VERSION)
            .fetch_one(&database.pool)
            .await?;
            assert_eq!(status, "protected");
            assert_eq!(
                identity_link_snapshot(&database.pool, fixture).await?,
                before[index]
            );
            assert_eq!(std::fs::read(&fixture.candidate.path)?, b"protected-video");
        }
        Ok(())
    }

    #[tokio::test]
    async fn alm8_retry_trigger_drains_every_preexisting_batch_exactly_once() -> Result<()> {
        let database = test_database().await?;
        for index in 0_u8..33 {
            let first = char::from(b'a' + (index / 26));
            let second = char::from(b'a' + (index % 26));
            let path = format!("/library/ambiguous-release-{first}{second}.mkv");
            let fixture = insert_repair_fixture(&database.pool, &path, &[(1, 1, Some(1))]).await?;
            enroll_repair_candidate(&database.pool, &fixture.candidate).await?;
            sqlx::query::<sqlx::Any>(
                "UPDATE library_anime_repairs SET status = 'retryable' \
                 WHERE media_file_id = $1 AND repair_version = $2",
            )
            .bind(fixture.candidate.media_file_id.to_string())
            .bind(ANIME_LIBRARY_REPAIR_VERSION)
            .execute(&database.pool)
            .await?;
        }

        let counts = run_anime_library_repair_iteration(
            &database.pool,
            None,
            &ClassifierPipeline::new(),
            &AnimeMatchingService::disabled(),
            86_400,
            AnimeLibraryRepairTrigger::ModelActivated,
        )
        .await?;
        assert_eq!(counts.scanned, 33);
        assert_eq!(counts.claimed, 33);
        assert_eq!(counts.retryable, 33);
        assert_eq!(counts.completed, 0);

        let ledger = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT COUNT(*), MIN(attempt_count), MAX(attempt_count) \
             FROM library_anime_repairs WHERE repair_version = $1 AND status = 'retryable'",
        )
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(ledger, (33, 1, 1));
        Ok(())
    }

    #[tokio::test]
    async fn alm8_run_progress_is_exact_after_interruption_and_resume() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("interrupted-progress.mkv");
        std::fs::write(&path, b"interrupted-progress")?;
        let fixture =
            insert_repair_fixture(&database.pool, &path.to_string_lossy(), &[(1, 1, Some(1))])
                .await?;

        let first_run = "foreign-interrupted-run";
        assert!(claim_repair_run(&database.pool, first_run).await?);
        persist_repair_run_progress(
            &database.pool,
            first_run,
            &RepairIterationCounts {
                scanned: 1,
                ..Default::default()
            },
        )
        .await?;
        enroll_repair_candidate(&database.pool, &fixture.candidate).await?;
        let first_claim = claim_repair_candidate_for_run(
            &database.pool,
            fixture.candidate.clone(),
            Some(first_run),
        )
        .await?
        .expect("first run must claim the fixture");
        mark_repair_retryable_for_run(
            &database.pool,
            &first_claim,
            Some("injected interruption"),
            None,
            Some(first_run),
        )
        .await?;

        // Simulate a process interruption: the run never reaches its finish
        // update, but all completed event counters are already durable.
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repair_runs SET claim_expires_at = 946684800 \
             WHERE repair_version = $1 AND claim_token = $2",
        )
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .bind(first_run)
        .execute(&database.pool)
        .await?;

        let resumed_run = "resumed-progress-run";
        assert!(claim_repair_run(&database.pool, resumed_run).await?);
        let interrupted_snapshot = anime_library_repair_snapshot(&database.pool).await?;
        assert_eq!(interrupted_snapshot.scanned_count, 1);
        assert_eq!(interrupted_snapshot.claimed_count, 1);
        assert_eq!(interrupted_snapshot.retryable_count, 1);
        assert_eq!(interrupted_snapshot.failure_count, 1);

        prepare_retryable_wave(&database.pool).await?;
        let mut resumed_candidates = discover_repair_candidates(&database.pool).await?;
        assert_eq!(resumed_candidates.len(), 1);
        persist_repair_run_progress(
            &database.pool,
            resumed_run,
            &RepairIterationCounts {
                scanned: 1,
                ..Default::default()
            },
        )
        .await?;
        let resumed_claim = claim_repair_candidate_for_run(
            &database.pool,
            resumed_candidates.pop().expect("resumed candidate"),
            Some(resumed_run),
        )
        .await?
        .expect("resumed run must reclaim the fixture");
        assert!(matches!(
            apply_canonical_repair_plan_for_run(
                &database.pool,
                &resumed_claim,
                &canonical_plan(fixture.candidate.series_id),
                Some(resumed_run),
            )
            .await?,
            ApplyRepairOutcome::Applied { .. }
        ));
        finish_repair_run(
            &database.pool,
            resumed_run,
            &RepairIterationCounts::default(),
            None,
        )
        .await?;

        let completed = anime_library_repair_snapshot(&database.pool).await?;
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.scanned_count, 2);
        assert_eq!(completed.claimed_count, 2);
        assert_eq!(completed.retryable_count, 1);
        assert_eq!(completed.completed_count, 1);
        assert_eq!(completed.protected_count, 0);
        assert_eq!(completed.repaired_link_count, 1);
        assert_eq!(completed.repaired_identity_count, 9);
        assert_eq!(completed.failure_count, 1);
        assert_eq!(completed.pending_files, 0);
        assert_eq!(completed.running_files, 0);
        assert_eq!(completed.retryable_files, 0);
        Ok(())
    }

    #[tokio::test]
    async fn alm8_run_and_file_claim_heartbeat_renew_owners_and_stale_claims_resume() -> Result<()>
    {
        let database = test_database().await?;
        let fixture = insert_repair_fixture(
            &database.pool,
            "/library/repair-heartbeat-fixture.mkv",
            &[(1, 1, Some(1))],
        )
        .await?;
        enroll_repair_candidate(&database.pool, &fixture.candidate).await?;
        let claimed = claim_repair_candidate(&database.pool, fixture.candidate.clone())
            .await?
            .expect("repair heartbeat fixture must be claimable");
        let run_claim_token = "repair-heartbeat-run";
        assert!(claim_repair_run(&database.pool, run_claim_token).await?);
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repairs SET claim_expires_at = $1 \
             WHERE media_file_id = $2 AND repair_version = $3",
        )
        .bind(repair_claim_now().saturating_sub(1))
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;

        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repair_runs SET claim_expires_at = $1 \
             WHERE repair_version = $2 AND claim_token = $3",
        )
        .bind(repair_claim_now().saturating_sub(1))
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .bind(run_claim_token)
        .execute(&database.pool)
        .await?;
        let heartbeat = start_repair_claim_heartbeat_with_interval(
            database.pool.clone(),
            &claimed,
            run_claim_token,
            Duration::from_millis(10),
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        heartbeat.stop().await;
        let run_expiry: i64 = sqlx::query_scalar(
            "SELECT claim_expires_at FROM library_anime_repair_runs \
             WHERE repair_version = $1 AND claim_token = $2",
        )
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .bind(run_claim_token)
        .fetch_one(&database.pool)
        .await?;
        assert!(run_expiry > repair_claim_now());

        assert!(renew_repair_candidate_claim(&database.pool, &claimed).await?);
        let renewed_expiry: i64 = sqlx::query_scalar(
            "SELECT claim_expires_at FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert!(renewed_expiry > repair_claim_now());

        let foreign = ClaimedRepair {
            candidate: fixture.candidate.clone(),
            claim_token: "foreign-repair-owner".to_string(),
        };
        assert!(!renew_repair_candidate_claim(&database.pool, &foreign).await?);
        let unchanged_expiry: i64 = sqlx::query_scalar(
            "SELECT claim_expires_at FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(unchanged_expiry, renewed_expiry);

        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repairs SET claim_token = 'foreign-repair-owner', \
             claim_expires_at = $1 \
             WHERE media_file_id = $2 AND repair_version = $3",
        )
        .bind(repair_claim_expiry())
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
        assert!(!renew_repair_candidate_claim(&database.pool, &claimed).await?);
        assert!(
            claim_repair_candidate(&database.pool, fixture.candidate.clone())
                .await?
                .is_none(),
            "an active foreign file lease must not be reclaimed"
        );
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repairs SET claim_expires_at = $1 \
             WHERE media_file_id = $2 AND repair_version = $3",
        )
        .bind(repair_claim_now().saturating_sub(1))
        .bind(fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
        let resumed = claim_repair_candidate(&database.pool, fixture.candidate.clone())
            .await?
            .expect("an expired file lease must be reclaimable");
        assert_ne!(resumed.claim_token, claimed.claim_token);
        Ok(())
    }

    #[tokio::test]
    async fn alm8_expired_claims_resume_while_active_foreign_leases_are_untouched() -> Result<()> {
        let database = test_database().await?;
        let temp = tempfile::tempdir()?;
        let expired_path = temp.path().join("expired.mkv");
        let active_path = temp.path().join("active.mkv");
        std::fs::write(&expired_path, b"expired")?;
        std::fs::write(&active_path, b"active")?;
        let expired_fixture = insert_repair_fixture(
            &database.pool,
            &expired_path.to_string_lossy(),
            &[(1, 1, Some(1))],
        )
        .await?;
        let active_fixture = insert_repair_fixture(
            &database.pool,
            &active_path.to_string_lossy(),
            &[(1, 1, Some(1))],
        )
        .await?;
        for (fixture, token, expiry) in [
            (&expired_fixture, "foreign-expired-file", 946684800_i64),
            (&active_fixture, "foreign-active-file", 32503680000_i64),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO library_anime_repairs \
                 (media_file_id, repair_version, status, claim_token, attempt_count, reason, \
                  evidence_snapshot_json, claimed_at, claim_expires_at) \
                 VALUES ($1, $2, 'running', $3, 1, $4, $5, CURRENT_TIMESTAMP, $6)",
            )
            .bind(fixture.candidate.media_file_id.to_string())
            .bind(ANIME_LIBRARY_REPAIR_VERSION)
            .bind(token)
            .bind(&fixture.candidate.reason)
            .bind(serde_json::to_string(&fixture.candidate.evidence_snapshot)?)
            .bind(expiry)
            .execute(&database.pool)
            .await?;
        }

        let discovered = discover_repair_candidates(&database.pool).await?;
        assert!(discovered.iter().any(|candidate| {
            candidate.media_file_id == expired_fixture.candidate.media_file_id
        }));
        assert!(!discovered.iter().any(|candidate| {
            candidate.media_file_id == active_fixture.candidate.media_file_id
        }));
        let reclaimed = claim_repair_candidate(&database.pool, expired_fixture.candidate.clone())
            .await?
            .expect("expired file claim must be resumable");
        assert!(
            reclaimed
                .claim_token
                .starts_with(REPAIR_PROCESS_TOKEN.as_str())
        );
        assert!(
            claim_repair_candidate(&database.pool, active_fixture.candidate.clone())
                .await?
                .is_none()
        );
        let expired_ledger = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT status, claim_token, attempt_count FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(expired_fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(expired_ledger.0, "running");
        assert_eq!(expired_ledger.1, reclaimed.claim_token);
        assert_eq!(expired_ledger.2, 2);
        let active_ledger = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT status, claim_token, attempt_count FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(active_fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            active_ledger,
            ("running".to_string(), "foreign-active-file".to_string(), 1)
        );

        sqlx::query::<sqlx::Any>(
            "INSERT INTO library_anime_repair_runs \
             (repair_version, status, claim_token, claim_expires_at, started_at) \
             VALUES ($1, 'running', 'foreign-expired-run', 946684800, \
                     CURRENT_TIMESTAMP)",
        )
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
        assert!(claim_repair_run(&database.pool, "resumed-run").await?);
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repair_runs SET claim_token = 'foreign-active-run', \
             claim_expires_at = 32503680000 WHERE repair_version = $1",
        )
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
        assert!(!claim_repair_run(&database.pool, "cannot-steal-run").await?);
        let active_run: (String, String) = sqlx::query_as(
            "SELECT status, claim_token FROM library_anime_repair_runs WHERE repair_version = $1",
        )
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            active_run,
            ("running".to_string(), "foreign-active-run".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm8_postgres_claim_apply_rollback_and_authority_serialization_when_configured()
    -> Result<()> {
        let Ok(url) = std::env::var("ELIXIR_TEST_POSTGRES_EMPTY_DATABASE_URL") else {
            return Ok(());
        };
        let database = Database::connect(&DatabaseConfig {
            url,
            max_connections: 6,
            connect_timeout_seconds: 5,
        })
        .await?;
        assert_eq!(database.driver, DatabaseDriver::Postgres);
        database.run_migrations().await?;

        let first_identity_guard =
            super::super::acquire_library_identity_database_guard(&database.pool)
                .await?
                .expect("PostgreSQL identity coordinator must hold a transaction guard");
        let coordinator_pool = database.pool.clone();
        let mut second_identity_guard = tokio::spawn(async move {
            super::super::acquire_library_identity_database_guard(&coordinator_pool).await
        });
        let mut observed_advisory_wait = false;
        for _ in 0..100 {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM pg_stat_activity \
                 WHERE datname = CURRENT_DATABASE() AND wait_event_type = 'Lock' \
                   AND query LIKE 'SELECT pg_advisory_xact_lock%'",
            )
            .fetch_one(&database.pool)
            .await?;
            if waiting > 0 {
                observed_advisory_wait = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            observed_advisory_wait && !second_identity_guard.is_finished(),
            "the second PostgreSQL identity coordinator must wait on the advisory transaction lock"
        );
        drop(first_identity_guard);
        let acquired_second_guard =
            tokio::time::timeout(Duration::from_secs(5), &mut second_identity_guard).await???;
        assert!(acquired_second_guard.is_some());
        drop(acquired_second_guard);

        assert!(claim_repair_run(&database.pool, "postgres-active-run").await?);
        let run_expiry: i64 = sqlx::query_scalar(
            "SELECT claim_expires_at FROM library_anime_repair_runs \
             WHERE repair_version = $1",
        )
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert!(run_expiry > repair_claim_now());
        assert!(!claim_repair_run(&database.pool, "postgres-blocked-run").await?);
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repair_runs SET claim_expires_at = $1 \
             WHERE repair_version = $2",
        )
        .bind(repair_claim_now().saturating_sub(1))
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
        assert!(claim_repair_run(&database.pool, "postgres-reclaimed-run").await?);
        finish_repair_run(
            &database.pool,
            "postgres-reclaimed-run",
            &RepairIterationCounts::default(),
            None,
        )
        .await?;

        let success_fixture = insert_repair_fixture(
            &database.pool,
            &format!("/library/postgres-success-{}.mkv", Uuid::new_v4()),
            &[(1, 1, Some(1)), (1, 2, Some(2))],
        )
        .await?;
        enroll_repair_candidate(&database.pool, &success_fixture.candidate).await?;
        let success_claim =
            claim_repair_candidate(&database.pool, success_fixture.candidate.clone())
                .await?
                .expect("pending PostgreSQL file must be claimable");
        let file_expiry: i64 = sqlx::query_scalar(
            "SELECT claim_expires_at FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(success_fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert!(file_expiry > repair_claim_now());
        let success_outcome = apply_canonical_repair_plan(
            &database.pool,
            &success_claim,
            &canonical_plan(success_fixture.candidate.series_id),
        )
        .await?;
        assert!(
            matches!(
                success_outcome,
                ApplyRepairOutcome::Applied {
                    links: 1,
                    identities
                } if identities > 0
            ),
            "PostgreSQL canonical apply returned {success_outcome:?}"
        );
        let completed_status: String = sqlx::query_scalar(
            "SELECT status FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(success_fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(completed_status, "completed");

        let rollback_fixture = insert_repair_fixture(
            &database.pool,
            &format!("/library/postgres-rollback-{}.mkv", Uuid::new_v4()),
            &[(1, 1, Some(1)), (1, 2, Some(2))],
        )
        .await?;
        enroll_repair_candidate(&database.pool, &rollback_fixture.candidate).await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repairs SET status = 'running', \
             claim_token = 'postgres-foreign-file', claim_expires_at = $1, \
             attempt_count = 1, claimed_at = CURRENT_TIMESTAMP \
             WHERE media_file_id = $2 AND repair_version = $3",
        )
        .bind(repair_claim_expiry())
        .bind(rollback_fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
        assert!(
            claim_repair_candidate(&database.pool, rollback_fixture.candidate.clone())
                .await?
                .is_none()
        );
        sqlx::query::<sqlx::Any>(
            "UPDATE library_anime_repairs SET claim_expires_at = $1 \
             WHERE media_file_id = $2 AND repair_version = $3",
        )
        .bind(repair_claim_now().saturating_sub(1))
        .bind(rollback_fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .execute(&database.pool)
        .await?;
        let rollback_claim =
            claim_repair_candidate(&database.pool, rollback_fixture.candidate.clone())
                .await?
                .expect("expired PostgreSQL file lease must be reclaimable");
        let before_rollback = identity_link_snapshot(&database.pool, &rollback_fixture).await?;
        // The successful apply above established the canonical owner for this
        // identity. Production planning resolves the later file to that owner
        // instead of attempting to publish the same unique IDs on its
        // provisional source series.
        let rollback_plan = canonical_plan(success_fixture.candidate.series_id);
        let rollback_error = apply_canonical_repair_plan_inner(
            &database.pool,
            &rollback_claim,
            &rollback_plan,
            true,
        )
        .await
        .expect_err("injected PostgreSQL failure must roll back");
        assert!(
            format!("{rollback_error:#}").contains("injected anime repair failure"),
            "unexpected PostgreSQL rollback error: {rollback_error:#}"
        );
        assert_eq!(
            identity_link_snapshot(&database.pool, &rollback_fixture).await?,
            before_rollback
        );

        let authority_fixture = insert_repair_fixture(
            &database.pool,
            &format!("/library/postgres-authority-{}.mkv", Uuid::new_v4()),
            &[(1, 1, Some(1))],
        )
        .await?;
        enroll_repair_candidate(&database.pool, &authority_fixture.candidate).await?;
        let authority_claim =
            claim_repair_candidate(&database.pool, authority_fixture.candidate.clone())
                .await?
                .expect("authority fixture must be claimable");
        let authority_before = identity_link_snapshot(&database.pool, &authority_fixture).await?;
        let override_key = derive_override_key("anime", &authority_fixture.candidate.path)
            .expect("fixture path must produce an override key");
        let mut authority_writer = database.pool.begin().await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO classifier_overrides \
             (id, library_type, normalized_key, anilist_id) \
             VALUES ($1, 'anime', $2, 'postgres-manual-authority')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(override_key)
        .execute(&mut *authority_writer)
        .await?;

        let apply_pool = database.pool.clone();
        let authority_plan = canonical_plan(authority_fixture.candidate.series_id);
        let apply_task = tokio::spawn(async move {
            apply_canonical_repair_plan(&apply_pool, &authority_claim, &authority_plan).await
        });
        let mut observed_authority_lock_wait = false;
        for _ in 0..100 {
            let blocked_lock_queries: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM pg_stat_activity \
                 WHERE datname = CURRENT_DATABASE() AND wait_event_type = 'Lock' \
                   AND query LIKE 'LOCK TABLE acquisition_import_file_links%'",
            )
            .fetch_one(&database.pool)
            .await?;
            if blocked_lock_queries > 0 {
                observed_authority_lock_wait = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            observed_authority_lock_wait && !apply_task.is_finished(),
            "repair must be observed waiting for the in-flight authority writer"
        );
        authority_writer.commit().await?;
        let authority_outcome =
            tokio::time::timeout(Duration::from_secs(5), apply_task).await???;
        assert_eq!(authority_outcome, ApplyRepairOutcome::Protected);
        assert_eq!(
            identity_link_snapshot(&database.pool, &authority_fixture).await?,
            authority_before
        );
        let protected_status: String = sqlx::query_scalar(
            "SELECT status FROM library_anime_repairs \
             WHERE media_file_id = $1 AND repair_version = $2",
        )
        .bind(authority_fixture.candidate.media_file_id.to_string())
        .bind(ANIME_LIBRARY_REPAIR_VERSION)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(protected_status, "protected");
        Ok(())
    }
}
